use std::marker::PhantomData;

use aead::Buffer;

use crate::capability::EncryptionState;
use crate::error::{ErrKind, RpcError, RpcResult};
use crate::io::{AsyncIORead, AsyncIOWrite};
use crate::message::{Message, MessageBuffer};
use crate::opt::branch_prediction::unlikely;
use crate::private::Private;

// RPC FRAME
// +-----------------------+---------------------+
// | Header (unencrypted?) | Payload (encrypted) |
// +-----------------------+---------------------+

const HEADER_LEN: usize = 4;

const MAX_MESSAGE_SIZE: u32 = 4 * 1024 * 1024;

// Definitely not for bulk throughput or streams, but streams are a different story.
const FRAMING_CAPACITY: usize = 1024;

pub trait AsyncRpcReceiver: Private {
    fn receive(&mut self) -> impl Future<Output = RpcResult<Message>> + Send;
}

pub trait AsyncRpcSender: Private {
    fn send(&mut self, message: &Message) -> impl Future<Output = RpcResult<()>> + Send;
    fn close(&mut self) -> impl Future<Output = RpcResult<()>> + Send;
}

impl<T> Private for RpcSender<T> {}
impl<T> Private for EncryptedRpcSender<T> {}
impl<T> Private for RpcReceiver<T> {}
impl<T> Private for EncryptedRpcReceiver<T> {}

struct ExtBufferView<'a> {
    pub buf: &'a mut Vec<u8>,
    pub offset: usize,
}

impl<'a> AsRef<[u8]> for ExtBufferView<'a> {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        &self.buf[self.offset..]
    }
}

impl<'a> AsMut<[u8]> for ExtBufferView<'a> {
    #[inline(always)]
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buf[self.offset..]
    }
}

impl<'a> Buffer for ExtBufferView<'a> {
    #[inline(always)]
    fn extend_from_slice(&mut self, other: &[u8]) -> aead::Result<()> {
        self.buf.extend_from_slice(other);
        Ok(())
    }

    #[inline(always)]
    fn truncate(&mut self, len: usize) {
        self.buf.truncate(self.offset + len);
    }
}

struct FixedBufferView<'a> {
    ptr: *mut u8,
    len: usize,
    _marker: std::marker::PhantomData<&'a mut [u8]>,
}

unsafe impl<'a> Send for FixedBufferView<'a> {}

impl<'a> FixedBufferView<'a> {
    const fn new(ptr: *mut u8, len: usize) -> Self {
        Self {
            ptr,
            len,
            _marker: PhantomData,
        }
    }

    const fn as_slice(&self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    const fn as_slice_mut(&self) -> &'a mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl<'a> AsRef<[u8]> for FixedBufferView<'a> {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'a> AsMut<[u8]> for FixedBufferView<'a> {
    #[inline(always)]
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}

impl<'a> Buffer for FixedBufferView<'a> {
    #[inline(always)]
    fn extend_from_slice(&mut self, _other: &[u8]) -> aead::Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn truncate(&mut self, len: usize) {
        self.len = len
    }
}

pub struct RpcSender<T> {
    writer: T,
    buffer: MessageBuffer,
}

impl<T> RpcSender<T> {
    #[inline(always)]
    pub fn new(writer: T) -> Self {
        Self {
            writer,
            buffer: MessageBuffer {
                buf: Vec::with_capacity(FRAMING_CAPACITY),
            },
        }
    }

    #[inline(always)]
    pub fn with_capacity(writer: T, framing_cap: usize) -> Self {
        Self {
            writer,
            buffer: MessageBuffer {
                buf: Vec::with_capacity(framing_cap.max(HEADER_LEN)),
            },
        }
    }
}

impl<T> AsyncRpcSender for RpcSender<T>
where
    T: AsyncIOWrite + Send + Unpin,
{
    /// Encodes a message as RPC frame and writes it into the I/O stream.
    async fn send(&mut self, message: &Message) -> RpcResult<()> {
        unsafe { self.buffer.buf.set_len(HEADER_LEN) };

        Message::encode_into_writer(message, &mut self.buffer)?;

        let len = (self.buffer.buf.len() - HEADER_LEN) as u32;

        self.buffer.buf[0..HEADER_LEN].copy_from_slice(&len.to_le_bytes());

        self.writer.write_all(&self.buffer.buf).await?;
        Ok(())
    }

    #[inline(always)]
    async fn close(&mut self) -> RpcResult<()> {
        self.writer.shutdown().await.map_err(Into::into)
    }
}

pub struct EncryptedRpcSender<T> {
    writer: T,
    buffer: MessageBuffer,
    state: EncryptionState,
}

impl<T> EncryptedRpcSender<T> {
    #[inline(always)]
    pub fn new(writer: T, state: EncryptionState) -> Self {
        Self {
            writer,
            buffer: MessageBuffer {
                buf: Vec::with_capacity(FRAMING_CAPACITY),
            },
            state,
        }
    }

    #[inline(always)]
    pub fn with_capacity(writer: T, state: EncryptionState, framing_cap: usize) -> Self {
        Self {
            writer,
            buffer: MessageBuffer {
                buf: Vec::with_capacity(framing_cap.max(HEADER_LEN)),
            },
            state,
        }
    }
}

impl<T> AsyncRpcSender for EncryptedRpcSender<T>
where
    T: AsyncIOWrite + Send + Unpin,
{
    /// Encodes a message as RPC frame and writes it into the I/O stream.
    async fn send(&mut self, message: &Message) -> RpcResult<()> {
        unsafe { self.buffer.buf.set_len(HEADER_LEN) };

        Message::encode_into_writer(message, &mut self.buffer)?;

        let mut enc_buf = ExtBufferView {
            buf: &mut self.buffer.buf,
            offset: HEADER_LEN,
        };

        self.state.encrypt(&mut enc_buf, b"")?;

        let len = (self.buffer.buf.len() - HEADER_LEN) as u32;

        self.buffer.buf[0..HEADER_LEN].copy_from_slice(&len.to_le_bytes());

        self.writer.write_all(&self.buffer.buf).await?;
        Ok(())
    }

    #[inline(always)]
    async fn close(&mut self) -> RpcResult<()> {
        self.writer.shutdown().await.map_err(Into::into)
    }
}

pub struct RpcReceiver<T> {
    reader: T,
    buffer: Vec<u8>,
}

impl<T> RpcReceiver<T> {
    #[inline(always)]
    pub fn new(reader: T) -> Self {
        Self {
            reader,
            buffer: Vec::with_capacity(FRAMING_CAPACITY),
        }
    }

    #[inline(always)]
    pub fn with_capacity(reader: T, framing_cap: usize) -> Self {
        Self {
            reader,
            buffer: Vec::with_capacity(framing_cap),
        }
    }
}

impl<T> AsyncRpcReceiver for RpcReceiver<T>
where
    T: AsyncIORead + Send + Unpin,
{
    /// Reads and validates the RPC frame and decodes its data as a message.
    /// This method is not cancellation-safe, and it should not be awaited as part of selection race.
    /// On error, receiving must be terminated and the stream must be closed.
    async fn receive(&mut self) -> RpcResult<Message> {
        debug_assert_eq!(self.buffer.len(), 0);

        let mut len_bytes = [0u8; 4];

        let read = self.reader.read_exact(&mut len_bytes).await?;
        debug_assert_eq!(read, 4);

        let len = u32::from_le_bytes(len_bytes);

        if unlikely(len > MAX_MESSAGE_SIZE) {
            return Err(RpcError::error(ErrKind::LargeMessage));
        }

        let len = len as usize;

        // Safety: capacity must be ensured before segmentation.
        self.buffer.reserve_exact(len);
        let segment = unsafe { std::slice::from_raw_parts_mut(self.buffer.as_mut_ptr(), len) };

        // Safety: This call must initialize the provided segment or it must fail and return.
        let read = self.reader.read_exact(segment).await?;
        debug_assert_eq!(read, segment.len());

        // Safety: Reading is assumed to be done on initialized bytes at this stage.
        Message::decode_from_slice(segment)
    }
}

pub struct EncryptedRpcReceiver<T> {
    reader: T,
    state: EncryptionState,
    buffer: Vec<u8>,
}

impl<T> EncryptedRpcReceiver<T> {
    #[inline(always)]
    pub fn new(reader: T, state: EncryptionState) -> Self {
        Self {
            reader,
            state,
            buffer: Vec::with_capacity(FRAMING_CAPACITY),
        }
    }

    #[inline(always)]
    pub fn with_capacity(reader: T, state: EncryptionState, framing_cap: usize) -> Self {
        Self {
            reader,
            state,
            buffer: Vec::with_capacity(framing_cap),
        }
    }
}

impl<T> AsyncRpcReceiver for EncryptedRpcReceiver<T>
where
    T: AsyncIORead + Send + Unpin,
{
    /// Reads and validates the RPC frame and decodes its data as a message.
    /// This method is not cancellation-safe, and it should not be awaited as part of selection race.
    /// On error, receiving must be terminated and the stream must be closed.
    async fn receive(&mut self) -> RpcResult<Message> {
        debug_assert_eq!(self.buffer.len(), 0);

        let mut len_bytes = [0u8; 4];

        let read = self.reader.read_exact(&mut len_bytes).await?;
        debug_assert_eq!(read, 4);

        let len = u32::from_le_bytes(len_bytes);

        if unlikely(len > MAX_MESSAGE_SIZE) {
            return Err(RpcError::error(ErrKind::LargeMessage));
        }

        let len = len as usize;

        // Safety: capacity must be ensured before segmentation.
        self.buffer.reserve_exact(len);
        let mut segment = FixedBufferView::new(self.buffer.as_mut_ptr(), len);

        // Safety: This call must initialize the provided segment or it must fail and return.
        let read = self.reader.read_exact(segment.as_slice_mut()).await?;
        debug_assert_eq!(read, segment.len);

        // Safety: Reading is assumed to be done on initialized bytes at this stage.
        self.state.decrypt(&mut segment, b"")?;
        Message::decode_from_slice(segment.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use tokio::net::{TcpStream, UnixListener, UnixStream};

    use crate::message::{Call, MessageType};

    #[tokio::test]
    async fn test_read_write_tcp_rpc() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let (transport, _) = listener.accept().await.unwrap();

            let (t_reader, t_writer) = transport.into_split();
            let mut rpc_reader = RpcReceiver::new(t_reader);
            let mut rpc_writer = RpcSender::new(t_writer);

            loop {
                match rpc_reader.receive().await {
                    Ok(msg) => {
                        if let Err(e) = rpc_writer.send(&msg).await {
                            println!("Server error: {:?}", e);
                        }
                    }
                    Err(e) => {
                        println!("Server error: {:?}", e);
                        break;
                    }
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let transport = TcpStream::connect(addr).await.unwrap();
        let (io_reader, io_writer) = transport.into_split();
        let mut rpc_reader = RpcReceiver::new(io_reader);
        let mut rpc_writer = RpcSender::new(io_writer);

        let call_msg = Message::call(Call::new(1, b"hi there".to_vec()));
        rpc_writer.send(&call_msg).await.unwrap();

        let reply_msg = rpc_reader.receive().await.unwrap();
        match (&call_msg.kind, &reply_msg.kind) {
            (MessageType::Call(sent), MessageType::Call(got)) => {
                assert_eq!(sent.method, got.method);
                assert_eq!(sent.data, got.data);
            }
            _ => panic!("Expected call"),
        }

        rpc_writer.close().await.unwrap();
        handle.await.unwrap()
    }

    #[tokio::test]
    async fn test_read_write_unix_rpc() {
        let path = "unix_transport_test.sock";

        let listener = UnixListener::bind(&path).unwrap();

        let handle = tokio::spawn(async move {
            let (transport, _) = listener.accept().await.unwrap();
            let (t_reader, t_writer) = transport.into_split();

            let mut rpc_reader = RpcReceiver::new(t_reader);
            let mut rpc_writer = RpcSender::new(t_writer);

            loop {
                match rpc_reader.receive().await {
                    Ok(msg) => {
                        if let Err(e) = rpc_writer.send(&msg).await {
                            println!("Server error: {:?}", e);
                        }
                    }
                    Err(e) => {
                        println!("Server error: {:?}", e);
                        break;
                    }
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let transport = UnixStream::connect(&path).await.unwrap();

        let (t_reader, t_writer) = transport.into_split();
        let mut rpc_reader = RpcReceiver::new(t_reader);
        let mut rpc_writer = RpcSender::new(t_writer);

        let call_msg = Message::call(Call::new(1, b"hi there".to_vec()));
        rpc_writer.send(&call_msg).await.unwrap();

        let reply_msg = rpc_reader.receive().await.unwrap();
        match (&call_msg.kind, &reply_msg.kind) {
            (MessageType::Call(sent), MessageType::Call(got)) => {
                assert_eq!(sent.method, got.method);
                assert_eq!(sent.data, got.data);
            }
            _ => panic!("Expected call"),
        }

        rpc_writer.close().await.unwrap();
        handle.await.unwrap();

        std::fs::remove_file(&path).unwrap();
    }
}
