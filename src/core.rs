use std::ptr;

use bincode::config::{Configuration, standard};
use bincode::enc::write::Writer;
use bincode::error::EncodeError;
use serde::{Deserialize, Serialize};

use aead::Buffer;

use uuid::Uuid;

use crate::capability::EncryptionState;
use crate::error::ErrKind;
use crate::opt::branch_prediction::unlikely;
use crate::{
    RpcError, RpcResult,
    io::{AsyncIORead, AsyncIOWrite},
};

use crate::private::Private;

// RPC FRAME
//   Header (unencrypted?)   Payload (encrypted)
// +----------------------+---------------------------------+
// |  Total len           | Header (ID, Type) + Data (Maybe)
// +----------------------+---------------------------------+

const CONFIG: Configuration = standard();

const STD_MESSAGE_LEN: usize = 4;

const STD_MAX_MESSAGE_SIZE: u32 = 4 * 1024 * 1024;

const STD_FRAMING_MIN_ALLOC: usize = STD_MESSAGE_LEN + Header::BYTES + RpcError::BYTES + 2;

// Definitely not for bulk throughput or streams, but streams are a different story.
const STD_FRAMING_ALLOC: usize = 1024;

/// Message types of the RPC protocol.
#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(u8)]
pub enum MessageType {
    /// A call message that may or may not have reply.
    ///
    /// This call targets methods that take extra parameters.
    Call = 0,

    /// A call message that may or may not have reply.
    ///
    /// This call targets methods that don't take any extra parameters.
    NullaryCall = 1,

    /// A response message (can be returned by either side).
    Reply = 2,

    /// Error message is a lightweight structure for operational errors.
    /// Rich and detailed errors are considered part of the service design,
    /// and subject to the documented reply type of a particular method.
    Error = 3,

    /// A heartbeat/ping message.
    Ping = 4,

    /// A heartbeat/pong response.
    Pong = 5,
}

impl MessageType {
    #[inline]
    pub const fn from_le_byte(byte: u8) -> Option<Self> {
        use MessageType::*;
        Some(match byte {
            0 => Call,
            1 => NullaryCall,
            2 => Reply,
            3 => Error,
            4 => Ping,
            5 => Pong,
            _ => return None,
        })
    }
}

impl core::fmt::Display for MessageType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Call => f.write_str("Variadic Call"),
            Self::NullaryCall => f.write_str("Niladic Call"),
            Self::Reply => f.write_str("Reply"),
            Self::Error => f.write_str("Error"),
            Self::Ping => f.write_str("Ping"),
            Self::Pong => f.write_str("Pong"),
        }
    }
}

pub type MessageID = Uuid;

#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    /// A 128-bit (16 byte) unique identifier for the message
    pub id: MessageID,

    /// The payload of the message.
    pub kind: MessageType,
}

impl Header {
    pub const BYTES: usize = 17;

    #[inline]
    pub fn new(id: Uuid, kind: MessageType) -> Self {
        Self { id, kind }
    }

    #[inline]
    pub fn auto(kind: MessageType) -> Self {
        Self {
            id: Uuid::new_v4(),
            kind,
        }
    }
}

/// A Type that can be a destination to encode messages.
pub trait MessageWriter {
    /// Write data to the underlying writer.
    ///
    /// The entire data must be written, or an error shall be returned.
    fn write(&mut self, data: &[u8]) -> RpcResult<()>;
}

/// The ribosome.
pub struct Message;

impl Message {
    #[inline]
    pub fn encode_header(id: &MessageID, kind: MessageType) -> [u8; 17] {
        let mut buf = [0u8; 17];
        buf[..16].copy_from_slice(&id.to_bytes_le());
        buf[16] = kind as u8;
        buf
    }

    #[inline]
    pub fn encode_header_into(
        id: &MessageID,
        kind: MessageType,
        output: &mut [u8],
    ) -> RpcResult<()> {
        if unlikely(output.len() < 17) {
            return Err(RpcError::error(ErrKind::Encoding));
        }
        output[..16].copy_from_slice(&id.to_bytes_le());
        output[16] = kind as u8;
        Ok(())
    }

    #[inline]
    pub fn decode_header(message: &[u8]) -> RpcResult<Header> {
        if unlikely(message.len() < 17) {
            return Err(RpcError::error(ErrKind::Decoding));
        }
        let mut id_bytes = [0u8; 16];
        id_bytes.copy_from_slice(&message[..16]);
        let id = Uuid::from_bytes_le(id_bytes);
        let kind =
            MessageType::from_le_byte(message[16]).ok_or(RpcError::error(ErrKind::Decoding))?;
        Ok(Header { id, kind })
    }

    #[inline]
    pub fn encode_error(err: RpcError) -> [u8; 5] {
        let mut buf = [0u8; 5];
        buf[0] = err.kind as u8;
        buf[1..5].copy_from_slice(&err.refer.to_le_bytes());
        buf
    }

    #[inline]
    pub fn encode_error_into(err: RpcError, output: &mut [u8]) -> RpcResult<()> {
        if unlikely(output.len() < 5) {
            return Err(RpcError::error(ErrKind::Encoding));
        }
        output[0] = err.kind as u8;
        output[1..5].copy_from_slice(&err.refer.to_le_bytes());
        Ok(())
    }

    #[inline]
    pub fn decode_error(message: &[u8]) -> RpcResult<RpcError> {
        let seg_err = &message[17..];
        if unlikely(seg_err.len() < 5) {
            return Err(RpcError::error(ErrKind::Decoding));
        }

        let kind = ErrKind::from_le_byte(seg_err[0]).ok_or(RpcError::error(ErrKind::Decoding))?;

        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&seg_err[1..5]);
        let refer = i32::from_le_bytes(bytes);

        Ok(RpcError { kind, refer })
    }

    pub fn decode_method(message: &[u8]) -> RpcResult<u16> {
        let mut method = [0u8; 2];
        method.copy_from_slice(&message[17..19]);
        Ok(u16::from_le_bytes(method))
    }

    pub fn param_data(message: &[u8]) -> &[u8] {
        &message[19..]
    }

    pub fn decode_params<R>(message: &[u8]) -> RpcResult<R>
    where
        R: for<'de> Deserialize<'de>,
    {
        Self::decode_from_slice::<R>(&message[19..])
    }

    pub fn reply_data(message: &[u8]) -> &[u8] {
        &message[17..]
    }

    pub fn decode_reply<R>(message: &[u8]) -> RpcResult<R>
    where
        R: for<'de> Deserialize<'de>,
    {
        Self::decode_from_slice::<R>(&message[17..])
    }

    /// Encodes a value to binary format.
    pub fn encode_to_vec<T: Serialize>(value: &T) -> RpcResult<Vec<u8>> {
        bincode::serde::encode_to_vec(value, CONFIG).map_err(Into::into)
    }

    /// Encodes a value to binary format into writer.
    pub fn encode_into_writer<T, W>(value: &T, dst: &mut W) -> RpcResult<()>
    where
        T: Serialize,
        W: MessageWriter,
    {
        bincode::serde::encode_into_writer(value, ImplWriter(dst), CONFIG).map_err(Into::into)
    }

    /// Decodes data from slice of bytes into a value.
    pub fn decode_from_slice<T>(data: &[u8]) -> RpcResult<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        match bincode::serde::borrow_decode_from_slice(data, CONFIG) {
            Ok((value, _)) => Ok(value),
            Err(err) => Err(RpcError::from(err)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageBuffer {
    pub data: Vec<u8>,
}

impl MessageBuffer {
    #[inline]
    pub const fn new() -> Self {
        Self { data: Vec::new() }
    }

    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            data: Vec::with_capacity(cap),
        }
    }

    /// # Safety
    /// - The buffer must have enough capacity to accommodate the the copying data.
    /// - The buffer memory is valid for writing/overwriting withing the range from
    ///   `0` to the length of copying data.
    #[inline]
    pub unsafe fn copy_from(&mut self, src: &[u8]) {
        let len = src.len();
        debug_assert!(len <= self.data.capacity());
        unsafe {
            ptr::copy_nonoverlapping(src.as_ptr(), self.data.as_mut_ptr(), len);
            self.data.set_len(len);
        }
    }
}

impl MessageWriter for MessageBuffer {
    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) -> RpcResult<()> {
        self.data.extend_from_slice(bytes);
        Ok(())
    }
}

struct ImplWriter<'a, W: MessageWriter>(&'a mut W);
impl<'a, W: MessageWriter> Writer for ImplWriter<'a, W> {
    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        self.0.write(bytes).map_err(|_| EncodeError::Other(""))
    }
}

pub trait AsyncSender: Private {
    fn call<P: Serialize + Sync>(
        &mut self,
        id: &MessageID,
        method: u16,
        params: &P,
    ) -> impl Future<Output = RpcResult<()>> + Send;
    fn call_nullary(
        &mut self,
        id: &MessageID,
        method: u16,
    ) -> impl Future<Output = RpcResult<()>> + Send;
    fn reply<R: Serialize + Sync>(
        &mut self,
        id: &MessageID,
        reply: &R,
    ) -> impl Future<Output = RpcResult<()>> + Send;
    fn error(
        &mut self,
        id: &MessageID,
        err: RpcError,
    ) -> impl Future<Output = RpcResult<()>> + Send;
    fn ping(&mut self, id: &MessageID) -> impl Future<Output = RpcResult<()>> + Send;
    fn pong(&mut self, id: &MessageID) -> impl Future<Output = RpcResult<()>> + Send;
    fn close(&mut self) -> impl Future<Output = RpcResult<()>> + Send;
}

pub trait AsyncReceiver: Private {
    fn receive(&mut self) -> impl Future<Output = RpcResult<()>> + Send;
    fn message(&self) -> &[u8];
}

impl<T> Private for MessageSender<T> {}
impl<T> Private for MessageReceiver<T> {}
impl<T> Private for EncMessageSender<T> {}
impl<T> Private for EncMessageReceiver<T> {}

pub struct MessageSender<T> {
    writer: T,
    buffer: MessageBuffer,
}

impl<T> MessageSender<T> {
    #[inline]
    pub fn new(writer: T) -> Self {
        Self {
            writer,
            buffer: MessageBuffer {
                data: Vec::with_capacity(STD_FRAMING_ALLOC),
            },
        }
    }

    #[inline]
    pub fn with_capacity(writer: T, framing_cap: usize) -> Self {
        Self {
            writer,
            buffer: MessageBuffer {
                data: Vec::with_capacity(framing_cap.max(STD_FRAMING_MIN_ALLOC)),
            },
        }
    }
}

impl<T: AsyncIOWrite + Send + Unpin> MessageSender<T> {
    #[inline]
    async fn write_all(&mut self) -> RpcResult<()> {
        let len = (self.buffer.data.len() - STD_MESSAGE_LEN) as u32;

        self.buffer.data[0..STD_MESSAGE_LEN].copy_from_slice(&len.to_le_bytes());

        self.writer.write_all(&self.buffer.data).await?;
        Ok(())
    }
}

impl<T: AsyncIOWrite + Send + Unpin> AsyncSender for MessageSender<T> {
    async fn call<P: Serialize + Sync>(
        &mut self,
        id: &MessageID,
        method: u16,
        params: &P,
    ) -> RpcResult<()> {
        unsafe { self.buffer.data.set_len(STD_MESSAGE_LEN) };
        self.buffer
            .write(&Message::encode_header(id, MessageType::Call))?;
        self.buffer.write(&method.to_le_bytes())?;
        Message::encode_into_writer(params, &mut self.buffer)?;
        self.write_all().await
    }

    async fn call_nullary(&mut self, id: &MessageID, method: u16) -> RpcResult<()> {
        unsafe { self.buffer.data.set_len(STD_MESSAGE_LEN) };
        self.buffer
            .write(&Message::encode_header(id, MessageType::NullaryCall))?;
        self.buffer.write(&method.to_le_bytes())?;
        self.write_all().await
    }

    async fn reply<R: Serialize + Sync>(&mut self, id: &MessageID, reply: &R) -> RpcResult<()> {
        unsafe { self.buffer.data.set_len(STD_MESSAGE_LEN) };
        self.buffer
            .write(&Message::encode_header(id, MessageType::Reply))?;
        Message::encode_into_writer(reply, &mut self.buffer)?;
        self.write_all().await
    }

    async fn error(&mut self, id: &MessageID, err: RpcError) -> RpcResult<()> {
        unsafe { self.buffer.data.set_len(STD_MESSAGE_LEN) };
        self.buffer
            .write(&Message::encode_header(id, MessageType::Error))?;
        self.buffer.write(&Message::encode_error(err))?;
        self.write_all().await
    }

    async fn ping(&mut self, id: &MessageID) -> RpcResult<()> {
        unsafe { self.buffer.data.set_len(STD_MESSAGE_LEN) };
        self.buffer
            .write(&Message::encode_header(id, MessageType::Ping))?;
        self.write_all().await
    }

    async fn pong(&mut self, id: &MessageID) -> RpcResult<()> {
        unsafe { self.buffer.data.set_len(STD_MESSAGE_LEN) };
        self.buffer
            .write(&Message::encode_header(id, MessageType::Pong))?;
        self.write_all().await
    }

    #[inline(always)]
    async fn close(&mut self) -> RpcResult<()> {
        self.writer.shutdown().await.map_err(Into::into)
    }
}

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
    buf: &'a mut Vec<u8>,
    len: usize,
}

impl<'a> FixedBufferView<'a> {
    const fn new(buf: &'a mut Vec<u8>, len: usize) -> Self {
        Self { buf, len }
    }

    const fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buf.as_ptr(), self.len) }
    }

    const fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.buf.as_mut_ptr(), self.len) }
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
        unsafe { self.buf.set_len(len) };
    }
}

pub struct EncMessageSender<T> {
    writer: T,
    state: EncryptionState,
    buffer: MessageBuffer,
}

impl<T> EncMessageSender<T> {
    #[inline]
    pub fn new(writer: T, state: EncryptionState) -> Self {
        Self {
            writer,
            state,
            buffer: MessageBuffer::with_capacity(STD_FRAMING_ALLOC),
        }
    }

    #[inline]
    pub fn with_capacity(writer: T, state: EncryptionState, framing_cap: usize) -> Self {
        Self {
            writer,
            state,
            buffer: MessageBuffer::with_capacity(framing_cap.max(STD_FRAMING_MIN_ALLOC)),
        }
    }
}

impl<T: AsyncIOWrite + Send + Unpin> EncMessageSender<T> {
    async fn write_all(&mut self) -> RpcResult<()> {
        let mut enc_buf = ExtBufferView {
            buf: &mut self.buffer.data,
            offset: STD_MESSAGE_LEN,
        };

        self.state.encrypt(&mut enc_buf, b"")?;

        let len = (self.buffer.data.len() - STD_MESSAGE_LEN) as u32;

        self.buffer.data[0..STD_MESSAGE_LEN].copy_from_slice(&len.to_le_bytes());

        self.writer.write_all(&self.buffer.data).await?;
        Ok(())
    }
}

impl<T: AsyncIOWrite + Send + Unpin> AsyncSender for EncMessageSender<T> {
    async fn call<P: Serialize>(
        &mut self,
        id: &MessageID,
        method: u16,
        params: &P,
    ) -> RpcResult<()> {
        unsafe { self.buffer.data.set_len(STD_MESSAGE_LEN) };
        self.buffer
            .write(&Message::encode_header(id, MessageType::Call))?;
        self.buffer.write(&method.to_le_bytes())?;
        Message::encode_into_writer(params, &mut self.buffer)?;
        self.write_all().await
    }

    async fn call_nullary(&mut self, id: &MessageID, method: u16) -> RpcResult<()> {
        unsafe { self.buffer.data.set_len(STD_MESSAGE_LEN) };
        self.buffer
            .write(&Message::encode_header(id, MessageType::NullaryCall))?;
        self.buffer.write(&method.to_le_bytes())?;
        self.write_all().await
    }

    async fn reply<R: Serialize>(&mut self, id: &MessageID, reply: &R) -> RpcResult<()> {
        unsafe { self.buffer.data.set_len(STD_MESSAGE_LEN) };
        self.buffer
            .write(&Message::encode_header(id, MessageType::Reply))?;
        Message::encode_into_writer(reply, &mut self.buffer)?;
        self.write_all().await
    }

    async fn error(&mut self, id: &MessageID, err: RpcError) -> RpcResult<()> {
        unsafe { self.buffer.data.set_len(STD_MESSAGE_LEN) };
        self.buffer
            .write(&Message::encode_header(id, MessageType::Error))?;
        self.buffer.write(&Message::encode_error(err))?;
        self.write_all().await
    }

    async fn ping(&mut self, id: &MessageID) -> RpcResult<()> {
        unsafe { self.buffer.data.set_len(STD_MESSAGE_LEN) };
        self.buffer
            .write(&Message::encode_header(id, MessageType::Ping))?;
        self.write_all().await
    }

    async fn pong(&mut self, id: &MessageID) -> RpcResult<()> {
        unsafe { self.buffer.data.set_len(STD_MESSAGE_LEN) };
        self.buffer
            .write(&Message::encode_header(id, MessageType::Pong))?;
        self.write_all().await
    }

    #[inline(always)]
    async fn close(&mut self) -> RpcResult<()> {
        self.writer.shutdown().await.map_err(Into::into)
    }
}

pub struct MessageReceiver<T> {
    reader: T,
    buffer: MessageBuffer,
}

impl<T> MessageReceiver<T> {
    #[inline]
    pub fn new(reader: T) -> Self {
        Self {
            reader,
            buffer: MessageBuffer::with_capacity(STD_FRAMING_ALLOC),
        }
    }

    #[inline]
    pub fn with_capacity(reader: T, framing_cap: usize) -> Self {
        Self {
            reader,
            buffer: MessageBuffer::with_capacity(framing_cap),
        }
    }
}

impl<T: AsyncIORead + Send + Unpin> AsyncReceiver for MessageReceiver<T> {
    async fn receive(&mut self) -> RpcResult<()> {
        let mut len_bytes = [0u8; 4];
        let read = self.reader.read_exact(&mut len_bytes).await?;
        debug_assert_eq!(read, 4);

        let len = u32::from_le_bytes(len_bytes);

        if unlikely(len > STD_MAX_MESSAGE_SIZE) {
            return Err(RpcError::error(ErrKind::LargeMessage));
        }

        let len = len as usize;

        // Safety: capacity must be ensured before segmentation.
        unsafe { self.buffer.data.set_len(0) }
        self.buffer.data.reserve_exact(len);
        let segment = unsafe { std::slice::from_raw_parts_mut(self.buffer.data.as_mut_ptr(), len) };

        // Safety: This call must initialize the provided segment or it must fail and return.
        let read = self.reader.read_exact(segment).await?;
        debug_assert_eq!(read, len);

        unsafe { self.buffer.data.set_len(len) }
        Ok(())
    }

    #[inline(always)]
    fn message(&self) -> &[u8] {
        &self.buffer.data
    }
}

pub struct EncMessageReceiver<T> {
    reader: T,
    state: EncryptionState,
    buffer: MessageBuffer,
}

impl<T> EncMessageReceiver<T> {
    #[inline]
    pub fn new(reader: T, state: EncryptionState) -> Self {
        Self {
            reader,
            state,
            buffer: MessageBuffer::with_capacity(STD_FRAMING_ALLOC),
        }
    }

    #[inline]
    pub fn with_capacity(reader: T, state: EncryptionState, framing_cap: usize) -> Self {
        Self {
            reader,
            state,
            buffer: MessageBuffer::with_capacity(framing_cap),
        }
    }
}

impl<T: AsyncIORead + Send + Unpin> AsyncReceiver for EncMessageReceiver<T> {
    async fn receive(&mut self) -> RpcResult<()> {
        let mut len_bytes = [0u8; 4];
        let read = self.reader.read_exact(&mut len_bytes).await?;
        debug_assert_eq!(read, 4);

        let len = u32::from_le_bytes(len_bytes);

        if unlikely(len > STD_MAX_MESSAGE_SIZE) {
            return Err(RpcError::error(ErrKind::LargeMessage));
        }

        let len = len as usize;

        // Safety: capacity must be ensured before segmentation.
        unsafe { self.buffer.data.set_len(0) }
        self.buffer.data.reserve_exact(len);
        let mut segment = FixedBufferView::new(&mut self.buffer.data, len);

        // Safety: This call must initialize the provided segment or it must fail and return.
        let read = self.reader.read_exact(segment.as_slice_mut()).await?;
        debug_assert_eq!(read, len);

        // Safety: Reading is assumed to be done on initialized bytes at this stage.
        self.state.decrypt(&mut segment, b"")
    }

    #[inline(always)]
    fn message(&self) -> &[u8] {
        &self.buffer.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use tokio::net::{TcpStream, UnixListener, UnixStream};

    #[tokio::test]
    async fn test_read_write_tcp_rpc() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let (transport, _) = listener.accept().await.unwrap();

            let (t_reader, t_writer) = transport.into_split();

            let mut msg_sender = MessageSender::new(t_writer);
            let mut msg_receiver = MessageReceiver::new(t_reader);

            loop {
                match msg_receiver.receive().await {
                    Ok(_) => {
                        let header = Message::decode_header(msg_receiver.message()).unwrap();
                        assert!(header.kind == MessageType::Call);

                        let method = Message::decode_method(msg_receiver.message()).unwrap();
                        assert!(method == 1);

                        let data: String = Message::decode_params(msg_receiver.message()).unwrap();
                        assert_eq!(&data, &"hi there");

                        if let Err(e) = msg_sender.reply(&header.id, &"Reply: hi there").await {
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

        let mut msg_sender = MessageSender::new(io_writer);
        let mut msg_receiver = MessageReceiver::new(io_reader);

        msg_sender
            .call(&MessageID::new_v4(), 1, &"hi there")
            .await
            .unwrap();

        msg_receiver.receive().await.unwrap();

        let header = Message::decode_header(msg_receiver.message()).unwrap();

        assert!(header.kind == MessageType::Reply);

        let reply: String = Message::decode_reply(msg_receiver.message()).unwrap();
        assert_eq!(reply, "Reply: hi there");

        msg_sender.close().await.unwrap();
        handle.await.unwrap()
    }

    #[tokio::test]
    async fn test_read_write_unix_rpc() {
        let path = "unix_transport_test_core.sock";

        let listener = UnixListener::bind(&path).unwrap();

        let handle = tokio::spawn(async move {
            let (transport, _) = listener.accept().await.unwrap();

            let (t_reader, t_writer) = transport.into_split();

            let mut msg_sender = MessageSender::new(t_writer);
            let mut msg_receiver = MessageReceiver::new(t_reader);

            loop {
                match msg_receiver.receive().await {
                    Ok(_) => {
                        let header = Message::decode_header(msg_receiver.message()).unwrap();
                        assert!(header.kind == MessageType::Call);

                        let method = Message::decode_method(msg_receiver.message()).unwrap();
                        assert!(method == 1);

                        let data: String = Message::decode_params(msg_receiver.message()).unwrap();
                        assert_eq!(data, "hi there");

                        if let Err(e) = msg_sender.reply(&header.id, &"Reply: hi there").await {
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

        let transport = UnixStream::connect(path).await.unwrap();
        let (io_reader, io_writer) = transport.into_split();

        let mut msg_sender = MessageSender::new(io_writer);
        let mut msg_receiver = MessageReceiver::new(io_reader);

        msg_sender
            .call(&MessageID::new_v4(), 1, &"hi there")
            .await
            .unwrap();

        msg_receiver.receive().await.unwrap();

        let header = Message::decode_header(msg_receiver.message()).unwrap();

        assert!(header.kind == MessageType::Reply);

        assert_eq!(
            Message::decode_reply::<String>(msg_receiver.message()).unwrap(),
            "Reply: hi there"
        );

        msg_sender.close().await.unwrap();
        handle.await.unwrap();

        std::fs::remove_file(&path).unwrap();
    }
}
