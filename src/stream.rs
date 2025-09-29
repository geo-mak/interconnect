use bincode::enc::write::Writer;
use bincode::error::EncodeError;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::capability::{BufferView, EncryptionState};
use crate::error::{ErrKind, RpcError, RpcResult};
use crate::message::Message;
use crate::sealed::Sealed;

// RPC FRAME
// +-----------------------+---------------------+
// | Header (unencrypted?) | Payload (encrypted) |
// +-----------------------+---------------------+

const HEADER_LEN: usize = 4;

const MAX_MESSAGE_SIZE: u32 = 4 * 1024 * 1024;

// Definitely not for bulk throughput or streams, but streams are a different story.
const FRAMING_CAPACITY: usize = 1024;

pub trait RpcAsyncReceiver: Sealed {
    fn receive(&mut self) -> impl Future<Output = RpcResult<Message>> + Send;
}

pub trait RpcAsyncSender: Sealed {
    fn send(&mut self, message: &Message) -> impl Future<Output = RpcResult<()>> + Send;
    fn close(&mut self) -> impl Future<Output = RpcResult<()>> + Send;
}

impl<T> Sealed for RpcSender<T> {}
impl<T> Sealed for EncryptedRpcSender<T> {}
impl<T> Sealed for RpcReceiver<T> {}
impl<T> Sealed for EncryptedRpcReceiver<T> {}

pub struct BytesWriter {
    pub buf: Vec<u8>,
}

impl Writer for BytesWriter {
    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        self.buf.extend_from_slice(bytes);
        Ok(())
    }
}

pub struct RpcSender<T> {
    writer: T,
    bytes: BytesWriter,
}

impl<T> RpcSender<T> {
    #[inline(always)]
    pub fn new(writer: T) -> Self {
        Self {
            writer,
            bytes: BytesWriter {
                buf: Vec::with_capacity(FRAMING_CAPACITY),
            },
        }
    }

    #[inline(always)]
    pub fn with_capacity(writer: T, framing_cap: usize) -> Self {
        Self {
            writer,
            bytes: BytesWriter {
                buf: Vec::with_capacity(framing_cap.max(HEADER_LEN)),
            },
        }
    }
}

impl<T> RpcAsyncSender for RpcSender<T>
where
    T: AsyncWriteExt + Send + Sync + Unpin,
{
    /// Encodes a message as RPC frame and writes it into the I/O stream.
    async fn send(&mut self, message: &Message) -> RpcResult<()> {
        unsafe { self.bytes.buf.set_len(HEADER_LEN) };

        message.encode_into_writer(&mut self.bytes)?;

        let len = (self.bytes.buf.len() - HEADER_LEN) as u32;

        self.bytes.buf[0..HEADER_LEN].copy_from_slice(&len.to_le_bytes());

        self.writer.write_all(&self.bytes.buf).await?;
        Ok(())
    }

    #[inline(always)]
    async fn close(&mut self) -> RpcResult<()> {
        self.writer.shutdown().await.map_err(Into::into)
    }
}

pub struct EncryptedRpcSender<T> {
    writer: T,
    state: EncryptionState,
    bytes: BytesWriter,
}

impl<T> EncryptedRpcSender<T> {
    #[inline(always)]
    pub fn new(writer: T, state: EncryptionState) -> Self {
        Self {
            writer,
            state,
            bytes: BytesWriter {
                buf: Vec::with_capacity(FRAMING_CAPACITY),
            },
        }
    }

    #[inline(always)]
    pub fn with_capacity(writer: T, state: EncryptionState, framing_cap: usize) -> Self {
        Self {
            writer,
            state,
            bytes: BytesWriter {
                buf: Vec::with_capacity(framing_cap.max(HEADER_LEN)),
            },
        }
    }
}

impl<T> RpcAsyncSender for EncryptedRpcSender<T>
where
    T: AsyncWriteExt + Send + Sync + Unpin,
{
    /// Encodes a message as RPC frame and writes it into the I/O stream.
    async fn send(&mut self, message: &Message) -> RpcResult<()> {
        unsafe { self.bytes.buf.set_len(HEADER_LEN) };

        message.encode_into_writer(&mut self.bytes)?;

        let mut enc_buf = BufferView {
            buf: &mut self.bytes.buf,
            offset: HEADER_LEN,
        };

        self.state.encrypt(&mut enc_buf, b"")?;

        let len = (self.bytes.buf.len() - HEADER_LEN) as u32;

        self.bytes.buf[0..HEADER_LEN].copy_from_slice(&len.to_le_bytes());

        self.writer.write_all(&self.bytes.buf).await?;
        Ok(())
    }

    #[inline(always)]
    async fn close(&mut self) -> RpcResult<()> {
        self.writer.shutdown().await.map_err(Into::into)
    }
}

pub struct RpcReceiver<T> {
    reader: T,
    bytes: Vec<u8>,
}

impl<T> RpcReceiver<T> {
    #[inline(always)]
    pub fn new(reader: T) -> Self {
        Self {
            reader,
            bytes: Vec::with_capacity(FRAMING_CAPACITY),
        }
    }

    #[inline(always)]
    pub fn with_capacity(reader: T, framing_cap: usize) -> Self {
        Self {
            reader,
            bytes: Vec::with_capacity(framing_cap),
        }
    }
}

impl<T> RpcAsyncReceiver for RpcReceiver<T>
where
    T: AsyncReadExt + Send + Sync + Unpin,
{
    /// Reads and validates the RPC frame and decodes its data as a message.
    /// This method is not cancellation-safe, and it should not be awaited as part of selection race.
    /// **Currently**, if an error has been returned, receiving must be terminated and the stream must be closed.
    #[allow(clippy::uninit_vec)]
    async fn receive(&mut self) -> RpcResult<Message> {
        unsafe { self.bytes.set_len(0) }

        // Read 4 bytes size prefix.
        let len = self.reader.read_u32_le().await?;

        if len > MAX_MESSAGE_SIZE {
            return Err(RpcError::error(ErrKind::LargeMessage));
        }

        // Len is 0 => If len > cap => reserve, no-op otherwise.
        self.bytes.reserve_exact(len as usize);

        // Safety: This should be safe because:
        //
        // 1 - Buffer has allocated memory for `len` bytes.
        //
        // 2 - `len` matches the expected decoding size.
        //
        // 3 - In Rust, any operation (Add, Sub, Mul, etc.) with uninitialized memory is UB,
        //     because the bit-pattern of that memory will not be interpreted in consistent manner (random),
        //     but we are not doing any operation except copying/overwriting FROM the stream TO the buffer,
        //     and only on success, or otherwise, an error will be returned.
        unsafe { self.bytes.set_len(len as usize) }
        self.reader.read_exact(&mut self.bytes).await?;

        Message::decode(&self.bytes)
    }
}

pub struct EncryptedRpcReceiver<T> {
    reader: T,
    state: EncryptionState,
    bytes: Vec<u8>,
}

impl<T> EncryptedRpcReceiver<T> {
    #[inline(always)]
    pub fn new(reader: T, state: EncryptionState) -> Self {
        Self {
            reader,
            state,
            bytes: Vec::with_capacity(FRAMING_CAPACITY),
        }
    }

    #[inline(always)]
    pub fn with_capacity(reader: T, state: EncryptionState, framing_cap: usize) -> Self {
        Self {
            reader,
            state,
            bytes: Vec::with_capacity(framing_cap),
        }
    }
}

impl<T> RpcAsyncReceiver for EncryptedRpcReceiver<T>
where
    T: AsyncReadExt + Send + Sync + Unpin,
{
    /// Reads and validates the RPC frame and decodes its data as a message.
    /// This method is not cancellation-safe, and it should not be awaited as part of selection race.
    /// **Currently**, if an error has been returned, receiving must be terminated and the stream must be closed.
    #[allow(clippy::uninit_vec)]
    async fn receive(&mut self) -> RpcResult<Message> {
        unsafe { self.bytes.set_len(0) }

        // Read 4 bytes size prefix.
        let len = self.reader.read_u32_le().await?;

        if len > MAX_MESSAGE_SIZE {
            return Err(RpcError::error(ErrKind::LargeMessage));
        }

        // Len is 0 => If len > cap => reserve, no-op otherwise.
        self.bytes.reserve_exact(len as usize);

        // Safety: This should be safe because:
        //
        // 1 - Buffer has allocated memory for `len` bytes.
        //
        // 2 - `len` matches the expected decoding size.
        //
        // 3 - In Rust, any operation (Add, Sub, Mul, etc.) with uninitialized memory is UB,
        //     because the bit-pattern of that memory will not be interpreted in consistent manner (random),
        //     but we are not doing any operation except copying/overwriting FROM the stream TO the buffer,
        //     and only on success, or otherwise, an error will be returned.
        unsafe { self.bytes.set_len(len as usize) }
        self.reader.read_exact(&mut self.bytes).await?;

        self.state.decrypt(&mut self.bytes, b"")?;

        Message::decode(&self.bytes)
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
