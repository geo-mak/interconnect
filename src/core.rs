use std::ptr::copy_nonoverlapping;
use std::slice::{from_raw_parts, from_raw_parts_mut};

use bincode::config::{Configuration, standard};
use bincode::enc::write::Writer;
use bincode::error::EncodeError;

use serde::{Deserialize, Serialize};

use aead::Buffer;

use crate::error::{ErrKind, RpcError, RpcResult};
use crate::io::{AsyncIORead, AsyncIOWrite, IOSegment};
use crate::opt::branch_hints::{likely, unlikely};
use crate::specs::EncryptionState;

use crate::private::Private;

// RPC FRAME
//   Header (unencrypted?)   Message (encrypted)
// +----------------------+--------------------------------------+
// |  Total len           | Header (ID, Directive) + Data (Maybe)
// +----------------------+--------------------------------------+

const CONFIG: Configuration = standard();

const STD_MAX_MSG_SIZE: u32 = 4 * 1024 * 1024;

const STD_FRAME_MIN_ALLOC: usize = 4 + Header::BYTES + RpcError::BYTES;

// Definitely not for bulk throughput or streams, but streams are a different story.
const STD_FRAME_ALLOC: usize = 1024;

/// Message directives of the RPC protocol.
#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(u8)]
pub enum Directive {
    /// A directive that instructs executing an operation.
    ///
    /// This directive targets operations that take parameters (data dependencies).
    ///
    /// Depending on the implementation, the target operation may not return a value.
    Call = 0,

    /// A directive that instructs executing an operation.
    ///
    /// This directive targets operations that don't take parameters (data dependencies).
    ///
    /// Depending on the implementation, the target operation may not return a value.
    NullaryCall = 1,

    /// A directive that instructs processing data returned by an operation.
    Return = 2,

    /// Error directive as a response to a call.
    ///
    /// This directive instructs decoding `RpcError` from the message.
    ///
    /// `RpcError` is a lightweight data structure for communicating errors.
    ///
    /// Rich and detailed errors are application specific, and subject to the documented
    /// return type of a particular operation.
    ///
    /// However, it is strongly recommended to limit communicating errors to `RpcError`
    /// if possible, because it has an integrated and optimized decoding mechanism.
    Error = 3,

    /// A heartbeat/ping directive.
    Ping = 4,

    /// A heartbeat/pong directive.
    Pong = 5,
}

impl Directive {
    #[inline]
    pub const fn from_byte(byte: u8) -> Option<Self> {
        use Directive::*;
        Some(match byte {
            0 => Call,
            1 => NullaryCall,
            2 => Return,
            3 => Error,
            4 => Ping,
            5 => Pong,
            _ => return None,
        })
    }
}

impl core::fmt::Display for Directive {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Call => f.write_str("Variadic Call"),
            Self::NullaryCall => f.write_str("Niladic Call"),
            Self::Return => f.write_str("Return"),
            Self::Error => f.write_str("Error"),
            Self::Ping => f.write_str("Ping"),
            Self::Pong => f.write_str("Pong"),
        }
    }
}

pub type MessageID = u64;

#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    /// A 64-bit (8 bytes) identifier of the message
    pub id: MessageID,

    /// The directive of the message.
    pub directive: Directive,
}

impl Header {
    pub const BYTES: usize = 9;

    #[inline]
    pub fn new(id: MessageID, directive: Directive) -> Self {
        Self { id, directive }
    }
}

/// A Type that can be a destination to encode messages.
pub trait MessageWriter {
    /// Writes data to the underlying writer in checked-mode.
    ///
    /// The entire data must be written, or an error shall be returned.
    fn write(&mut self, data: &[u8]) -> RpcResult<()>;
}

/// The ribosome.
pub mod message {
    use serde::de::DeserializeOwned;

    use super::*;

    #[inline]
    pub fn encode_header(id: &MessageID, directive: Directive) -> [u8; 9] {
        let mut header_bytes = [0u8; 9];

        let id_bytes = id.to_le_bytes();

        header_bytes[..8].copy_from_slice(&id_bytes);

        header_bytes[8] = directive as u8;

        header_bytes
    }

    #[inline]
    pub fn encode_header_into(
        id: &MessageID,
        directive: Directive,
        output: &mut [u8],
    ) -> RpcResult<()> {
        if unlikely(output.len() < 9) {
            return Err(RpcError::error(ErrKind::Encoding));
        }

        output[..8].copy_from_slice(&id.to_le_bytes());

        output[8] = directive as u8;

        Ok(())
    }

    #[inline]
    pub fn decode_header(message: &[u8]) -> RpcResult<Header> {
        if unlikely(message.len() < 9) {
            return Err(RpcError::error(ErrKind::Decoding));
        }

        let mut id_bytes = [0u8; 8];

        id_bytes.copy_from_slice(&message[..8]);

        let id = MessageID::from_le_bytes(id_bytes);

        let directive =
            Directive::from_byte(message[8]).ok_or(RpcError::error(ErrKind::Decoding))?;

        Ok(Header { id, directive })
    }

    #[inline]
    pub fn encode_error(err: RpcError) -> [u8; 5] {
        let mut error_bytes = [0u8; 5];

        error_bytes[0] = err.kind as u8;

        let refer_bytes = err.refer.to_le_bytes();

        error_bytes[1..5].copy_from_slice(&refer_bytes);

        error_bytes
    }

    #[inline]
    pub fn encode_error_into(err: RpcError, output: &mut [u8]) -> RpcResult<()> {
        if unlikely(output.len() < 5) {
            return Err(RpcError::error(ErrKind::Encoding));
        }

        output[0] = err.kind as u8;

        let refer_bytes = err.refer.to_le_bytes();

        output[1..5].copy_from_slice(&refer_bytes);

        Ok(())
    }

    #[inline]
    pub fn decode_error(message: &[u8]) -> RpcResult<RpcError> {
        // Header + error = 14.
        if likely(message.len() == 14) {
            let error_segment = &message[9..14];
            let refer_segment = &error_segment[1..5];

            let kind =
                ErrKind::from_byte(error_segment[0]).ok_or(RpcError::error(ErrKind::Decoding))?;

            let mut refer_bytes = [0u8; 4];

            refer_bytes.copy_from_slice(refer_segment);

            let refer = i32::from_le_bytes(refer_bytes);

            return Ok(RpcError { kind, refer });
        }

        Err(RpcError::error(ErrKind::Decoding))
    }

    #[inline]
    pub fn decode_op(message: &[u8]) -> RpcResult<u16> {
        if unlikely(message.len() < 11) {
            return Err(RpcError::error(ErrKind::Encoding));
        }

        let op_segment = &message[9..11];

        let mut op_bytes = [0u8; 2];

        op_bytes.copy_from_slice(op_segment);

        Ok(u16::from_le_bytes(op_bytes))
    }

    #[inline]
    pub fn params_data(message: &[u8]) -> RpcResult<&[u8]> {
        if likely(message.len() > 11) {
            return Ok(&message[11..]);
        }

        Err(RpcError::error(ErrKind::Decoding))
    }

    #[inline]
    pub fn decode_op_return_params(message: &[u8]) -> RpcResult<(u16, &[u8])> {
        if likely(message.len() > 11) {
            let op_segment = &message[9..11];

            let mut op_bytes = [0u8; 2];

            op_bytes.copy_from_slice(op_segment);

            let op = u16::from_le_bytes(op_bytes);

            return Ok((op, &message[11..]));
        }

        Err(RpcError::error(ErrKind::Decoding))
    }

    #[inline]
    pub fn returned_data(message: &[u8]) -> RpcResult<&[u8]> {
        if likely(message.len() > 9) {
            return Ok(&message[9..]);
        }

        Err(RpcError::error(ErrKind::Decoding))
    }

    /// Encodes a value to binary format.
    #[inline]
    pub fn encode_to_vec<T: Serialize>(value: &T) -> RpcResult<Vec<u8>> {
        bincode::serde::encode_to_vec(value, CONFIG).map_err(Into::into)
    }

    /// Encodes a value to binary format into the provided slice.
    #[inline]
    pub fn encode_into_slice<T, W>(value: &T, dst: &mut [u8]) -> RpcResult<usize>
    where
        T: Serialize,
    {
        bincode::serde::encode_into_slice(value, dst, CONFIG).map_err(Into::into)
    }

    /// Encodes a value to binary format into I/O segment.
    #[inline]
    pub fn encode_into_segment<T, S>(value: &T, dst: &mut S) -> RpcResult<()>
    where
        T: Serialize,
        S: IOSegment,
    {
        bincode::serde::encode_into_writer(value, ImplWriter(dst), CONFIG).map_err(Into::into)
    }

    /// Decodes data from slice of bytes into a value by borrowing the data from the provided store.
    pub fn decode_borrowed_from_slice<'de, T>(data: &'de [u8]) -> RpcResult<T>
    where
        T: Deserialize<'de>,
    {
        match bincode::serde::borrow_decode_from_slice(data, CONFIG) {
            Ok((value, _)) => Ok(value),
            Err(err) => Err(RpcError::from(err)),
        }
    }

    /// Decodes data from slice of bytes into a value.
    #[inline]
    pub fn decode_owned_from_slice<T>(data: &[u8]) -> RpcResult<T>
    where
        T: DeserializeOwned,
    {
        match bincode::serde::decode_from_slice(data, CONFIG) {
            Ok((value, _)) => Ok(value),
            Err(err) => Err(RpcError::from(err)),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MessageStore {
    pub(crate) data: Vec<u8>,
}

impl MessageStore {
    #[allow(dead_code)]
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { data: Vec::new() }
    }

    #[inline]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    /// # Safety
    /// - `src` must consist of fully initialized bytes.
    /// - `src` must be a non-overlapping (disjoint) memory-segment.
    /// - The buffer must have enough capacity to accommodate the the copying data.
    /// - The buffer memory is valid for writing/overwriting withing the range from
    ///   `0` to the length of copying data.
    #[inline]
    pub(crate) unsafe fn copy_from(&mut self, src: &[u8]) {
        let count = src.len();
        debug_assert!(count <= self.data.capacity());
        unsafe {
            copy_nonoverlapping(src.as_ptr(), self.data.as_mut_ptr(), count);
            self.data.set_len(count);
        }
    }
}

unsafe impl IOSegment for MessageStore {
    #[inline]
    fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    fn as_slice_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    #[inline]
    fn clear(&mut self) {
        self.data.clear();
    }

    #[inline]
    unsafe fn set_len(&mut self, new_len: usize) {
        unsafe { self.data.set_len(new_len) }
    }

    #[inline]
    fn write(&mut self, src: &[u8]) -> bool {
        let src_len = src.len();
        let current_len = self.data.len();

        // Sufficiency of capacity is checked inside.
        if let Ok(()) = self.data.try_reserve(src_len) {
            unsafe {
                let dst_ptr = self.data.as_mut_ptr().add(current_len);
                copy_nonoverlapping(src.as_ptr(), dst_ptr, src_len);
                self.data.set_len(current_len + src_len);
            };

            return true;
        }

        false
    }

    #[inline]
    unsafe fn write_at(&mut self, offset: usize, src: &[u8]) {
        let count = src.len();
        debug_assert!(offset + count <= self.data.capacity());
        unsafe { copy_nonoverlapping(src.as_ptr(), self.data.as_mut_ptr().add(offset), count) };
    }
}

struct ImplWriter<'a, S: IOSegment>(&'a mut S);
impl<'a, S: IOSegment> Writer for ImplWriter<'a, S> {
    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        if !self.0.write(bytes) {
            return Err(EncodeError::UnexpectedEnd);
        };
        Ok(())
    }
}

pub trait AsyncSender: Private {
    fn call<P: Serialize + Sync>(
        &mut self,
        id: &MessageID,
        op: u16,
        params: &P,
    ) -> impl Future<Output = RpcResult<()>> + Send;
    fn call_nullary(
        &mut self,
        id: &MessageID,
        op: u16,
    ) -> impl Future<Output = RpcResult<()>> + Send;
    fn return_data<R: Serialize + Sync>(
        &mut self,
        id: &MessageID,
        data: &R,
    ) -> impl Future<Output = RpcResult<()>> + Send;
    fn return_error(
        &mut self,
        id: &MessageID,
        error: RpcError,
    ) -> impl Future<Output = RpcResult<()>> + Send;
    fn ping(&mut self, id: &MessageID) -> impl Future<Output = RpcResult<()>> + Send;
    fn pong(&mut self, id: &MessageID) -> impl Future<Output = RpcResult<()>> + Send;
    fn terminate(&mut self) -> impl Future<Output = RpcResult<()>> + Send;
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
    transport: T,
    store: MessageStore,
}

impl<T> MessageSender<T> {
    #[inline]
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            store: MessageStore {
                data: Vec::with_capacity(STD_FRAME_ALLOC),
            },
        }
    }

    #[inline]
    pub fn with_capacity(transport: T, framing_cap: usize) -> Self {
        Self {
            transport,
            store: MessageStore {
                data: Vec::with_capacity(framing_cap.max(STD_FRAME_MIN_ALLOC)),
            },
        }
    }
}

impl<T: AsyncIOWrite + Send + Unpin> AsyncSender for MessageSender<T> {
    async fn call<P: Serialize + Sync>(
        &mut self,
        id: &MessageID,
        op: u16,
        params: &P,
    ) -> RpcResult<()> {
        let header_bytes = message::encode_header(id, Directive::Call);
        let op_bytes = op.to_le_bytes();

        // Safety:
        // - Capacity is ensured up to 18 bytes.
        // - On early return, no data is produced.
        unsafe {
            self.store.write_at(4, &header_bytes);
            self.store.write_at(13, &op_bytes);
            self.store.data.set_len(15);
            message::encode_into_segment(params, &mut self.store)?;
            let len = (self.store.data.len() - 4) as u32;
            self.store.write_at(0, &len.to_le_bytes());
        }

        self.transport.write_all(&self.store.data).await?;
        Ok(())
    }

    async fn call_nullary(&mut self, id: &MessageID, op: u16) -> RpcResult<()> {
        let header_bytes = message::encode_header(id, Directive::NullaryCall);
        let op_bytes = op.to_le_bytes();

        // Safety: Capacity is ensured up to 18 bytes.
        unsafe {
            self.store.write_at(0, &11u32.to_le_bytes());
            self.store.write_at(4, &header_bytes);
            self.store.write_at(13, &op_bytes);
            self.store.data.set_len(15);
        }

        self.transport.write_all(&self.store.data).await?;
        Ok(())
    }

    async fn return_data<R: Serialize + Sync>(
        &mut self,
        id: &MessageID,
        reply: &R,
    ) -> RpcResult<()> {
        let header_bytes = message::encode_header(id, Directive::Return);

        // Safety:
        // - Capacity is ensured up to 18 bytes.
        // - On early return, no data is produced.
        unsafe {
            self.store.write_at(4, &header_bytes);
            self.store.data.set_len(13);
            message::encode_into_segment(reply, &mut self.store)?;
            let len = (self.store.data.len() - 4) as u32;
            self.store.write_at(0, &len.to_le_bytes());
        }

        self.transport.write_all(&self.store.data).await?;
        Ok(())
    }

    async fn return_error(&mut self, id: &MessageID, error: RpcError) -> RpcResult<()> {
        let header_bytes = message::encode_header(id, Directive::Error);
        let error_bytes = message::encode_error(error);

        // Safety: Capacity is ensured up to 18 bytes.
        unsafe {
            self.store.write_at(0, &14u32.to_le_bytes());
            self.store.write_at(4, &header_bytes);
            self.store.write_at(13, &error_bytes);
            self.store.data.set_len(18);
        }

        self.transport.write_all(&self.store.data).await?;
        Ok(())
    }

    async fn ping(&mut self, id: &MessageID) -> RpcResult<()> {
        let header_bytes = message::encode_header(id, Directive::Ping);

        // Safety: Capacity is ensured up to 18 bytes.
        unsafe {
            self.store.write_at(0, &9u32.to_le_bytes());
            self.store.write_at(4, &header_bytes);
            self.store.data.set_len(13);
        }

        self.transport.write_all(&self.store.data).await?;
        Ok(())
    }

    async fn pong(&mut self, id: &MessageID) -> RpcResult<()> {
        let header_bytes = message::encode_header(id, Directive::Pong);

        // Safety: Capacity is ensured up to 18 bytes.
        unsafe {
            self.store.write_at(0, &9u32.to_le_bytes());
            self.store.write_at(4, &header_bytes);
            self.store.data.set_len(13);
        }

        self.transport.write_all(&self.store.data).await?;
        Ok(())
    }

    #[inline(always)]
    async fn terminate(&mut self) -> RpcResult<()> {
        self.transport.terminate().await.map_err(Into::into)
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
        // RT_DYN_ALLOC
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
        unsafe { from_raw_parts(self.buf.as_ptr(), self.len) }
    }

    const fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { from_raw_parts_mut(self.buf.as_mut_ptr(), self.len) }
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
    transport: T,
    state: EncryptionState,
    store: MessageStore,
}

impl<T> EncMessageSender<T> {
    #[inline]
    pub fn new(transport: T, state: EncryptionState) -> Self {
        Self {
            transport,
            state,
            store: MessageStore {
                data: Vec::with_capacity(STD_FRAME_ALLOC),
            },
        }
    }

    #[inline]
    pub fn with_capacity(transport: T, state: EncryptionState, framing_cap: usize) -> Self {
        Self {
            transport,
            state,
            store: MessageStore {
                data: Vec::with_capacity(framing_cap.max(STD_FRAME_MIN_ALLOC)),
            },
        }
    }
}

impl<T: AsyncIOWrite + Send + Unpin> EncMessageSender<T> {
    #[inline]
    async fn update_len_write_all(&mut self) -> RpcResult<()> {
        let mut encryption_buf = ExtBufferView {
            buf: &mut self.store.data,
            offset: 4,
        };

        self.state.encrypt(&mut encryption_buf, b"")?;

        unsafe {
            let len = (self.store.data.len() - 4) as u32;
            self.store.write_at(0, &len.to_le_bytes());
        }

        self.transport.write_all(&self.store.data).await?;
        Ok(())
    }
}

impl<T: AsyncIOWrite + Send + Unpin> AsyncSender for EncMessageSender<T> {
    async fn call<P: Serialize>(&mut self, id: &MessageID, op: u16, params: &P) -> RpcResult<()> {
        let header_bytes = message::encode_header(id, Directive::Call);
        let op_bytes = op.to_le_bytes();

        // Safety:
        // - Capacity is ensured up to 18 bytes.
        // - On early return, no data is produced.
        unsafe {
            self.store.write_at(4, &header_bytes);
            self.store.write_at(13, &op_bytes);
            self.store.data.set_len(15);
            message::encode_into_segment(params, &mut self.store)?;
        }

        self.update_len_write_all().await
    }

    async fn call_nullary(&mut self, id: &MessageID, op: u16) -> RpcResult<()> {
        let header_bytes = message::encode_header(id, Directive::NullaryCall);
        let op_bytes = op.to_le_bytes();

        // Safety: Capacity is ensured up to 18 bytes.
        unsafe {
            self.store.write_at(4, &header_bytes);
            self.store.write_at(13, &op_bytes);
            self.store.data.set_len(15);
        }

        self.update_len_write_all().await
    }

    async fn return_data<R: Serialize>(&mut self, id: &MessageID, reply: &R) -> RpcResult<()> {
        let header_bytes = message::encode_header(id, Directive::Return);

        // Safety:
        // - Capacity is ensured up to 18 bytes.
        // - On early return, no data is produced.
        unsafe {
            self.store.write_at(4, &header_bytes);
            self.store.data.set_len(13);
            message::encode_into_segment(reply, &mut self.store)?;
        }

        self.update_len_write_all().await
    }

    async fn return_error(&mut self, id: &MessageID, error: RpcError) -> RpcResult<()> {
        let header_bytes = message::encode_header(id, Directive::Error);
        let error_bytes = message::encode_error(error);

        // Safety: Capacity is ensured up to 18 bytes.
        unsafe {
            self.store.write_at(4, &header_bytes);
            self.store.write_at(13, &error_bytes);
            self.store.data.set_len(18);
        }

        self.update_len_write_all().await
    }

    async fn ping(&mut self, id: &MessageID) -> RpcResult<()> {
        let header_bytes = message::encode_header(id, Directive::Ping);

        // Safety: Capacity is ensured up to 18 bytes.
        unsafe {
            self.store.write_at(4, &header_bytes);
            self.store.data.set_len(13);
        }

        self.update_len_write_all().await
    }

    async fn pong(&mut self, id: &MessageID) -> RpcResult<()> {
        let header_bytes = message::encode_header(id, Directive::Pong);

        // Safety: Capacity is ensured up to 18 bytes.
        unsafe {
            self.store.write_at(4, &header_bytes);
            self.store.data.set_len(13);
        }

        self.update_len_write_all().await
    }

    #[inline(always)]
    async fn terminate(&mut self) -> RpcResult<()> {
        self.transport.terminate().await.map_err(Into::into)
    }
}

pub struct MessageReceiver<T> {
    transport: T,
    buffer: MessageStore,
}

impl<T> MessageReceiver<T> {
    #[inline]
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            buffer: MessageStore::with_capacity(STD_FRAME_ALLOC),
        }
    }

    #[inline]
    pub fn with_capacity(transport: T, framing_cap: usize) -> Self {
        Self {
            transport,
            buffer: MessageStore::with_capacity(framing_cap),
        }
    }
}

impl<T: AsyncIORead + Send + Unpin> AsyncReceiver for MessageReceiver<T> {
    async fn receive(&mut self) -> RpcResult<()> {
        let mut len_bytes = [0u8; 4];
        let read = self.transport.read_exact(&mut len_bytes).await?;
        debug_assert_eq!(read, 4);

        let len = u32::from_le_bytes(len_bytes);

        if unlikely(len > STD_MAX_MSG_SIZE) {
            return Err(RpcError::error(ErrKind::RecvSizeLimit));
        }

        let len = len as usize;

        // Safety:
        // - Capacity must be ensured before segmentation.
        // - Len remains `0` until reading full message as announced.
        unsafe { self.buffer.data.set_len(0) }
        self.buffer.data.try_reserve(len)?;
        let segment = unsafe { std::slice::from_raw_parts_mut(self.buffer.data.as_mut_ptr(), len) };

        // Safety: This call must initialize the provided segment or it must fail and return.
        let read = self.transport.read_exact(segment).await?;
        debug_assert_eq!(read, len);

        // Safety: `len` bytes are assumed to have been initialized.
        unsafe { self.buffer.data.set_len(len) }
        Ok(())
    }

    #[inline(always)]
    fn message(&self) -> &[u8] {
        &self.buffer.data
    }
}

pub struct EncMessageReceiver<T> {
    transport: T,
    state: EncryptionState,
    buffer: MessageStore,
}

impl<T> EncMessageReceiver<T> {
    #[inline]
    pub fn new(transport: T, state: EncryptionState) -> Self {
        Self {
            transport,
            state,
            buffer: MessageStore::with_capacity(STD_FRAME_ALLOC),
        }
    }

    #[inline]
    pub fn with_capacity(transport: T, state: EncryptionState, framing_cap: usize) -> Self {
        Self {
            transport,
            state,
            buffer: MessageStore::with_capacity(framing_cap),
        }
    }
}

impl<T: AsyncIORead + Send + Unpin> AsyncReceiver for EncMessageReceiver<T> {
    async fn receive(&mut self) -> RpcResult<()> {
        let mut len_bytes = [0u8; 4];
        let read = self.transport.read_exact(&mut len_bytes).await?;
        debug_assert_eq!(read, 4);

        let len = u32::from_le_bytes(len_bytes);

        if unlikely(len > STD_MAX_MSG_SIZE) {
            return Err(RpcError::error(ErrKind::RecvSizeLimit));
        }

        let len = len as usize;

        // Safety:
        // - Capacity must be ensured before segmentation.
        // - Len remains `0` until reading full message as announced.
        unsafe { self.buffer.data.set_len(0) }
        self.buffer.data.try_reserve(len)?;
        let mut segment = FixedBufferView::new(&mut self.buffer.data, len);

        // Safety: This call must initialize the provided segment or it must fail and return.
        let read = self.transport.read_exact(segment.as_slice_mut()).await?;
        debug_assert_eq!(read, len);

        // Safety:
        // - Reading is assumed to be done on initialized bytes at this stage.
        // - len is updated after decryption by calling FixedBufferView::truncate.
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
                        let message = msg_receiver.message();

                        let header = message::decode_header(message).unwrap();
                        assert!(header.directive == Directive::Call);

                        let op = message::decode_op(message).unwrap();
                        assert!(op == 1);

                        let params_data = message::params_data(message).unwrap();

                        let data: &str = message::decode_borrowed_from_slice(params_data).unwrap();
                        assert_eq!(&data, &"hi there");

                        if let Err(e) = msg_sender.return_data(&header.id, &"Reply: hi there").await
                        {
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

        msg_sender.call(&1, 1, &"hi there").await.unwrap();

        msg_receiver.receive().await.unwrap();

        let message = msg_receiver.message();

        let header = message::decode_header(message).unwrap();

        assert!(header.directive == Directive::Return);

        let returned = message::returned_data(message).unwrap();

        let reply: &str = message::decode_borrowed_from_slice(returned).unwrap();
        assert_eq!(reply, "Reply: hi there");

        msg_sender.terminate().await.unwrap();
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
                        let message = msg_receiver.message();

                        let header = message::decode_header(message).unwrap();
                        assert!(header.directive == Directive::Call);

                        let op = message::decode_op(message).unwrap();
                        assert!(op == 1);

                        let params_data = message::params_data(message).unwrap();

                        let params: &str =
                            message::decode_borrowed_from_slice(params_data).unwrap();
                        assert_eq!(params, "hi there");

                        if let Err(e) = msg_sender.return_data(&header.id, &"Reply: hi there").await
                        {
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

        msg_sender.call(&1, 1, &"hi there").await.unwrap();

        msg_receiver.receive().await.unwrap();

        let message = msg_receiver.message();

        let header = message::decode_header(message).unwrap();

        assert!(header.directive == Directive::Return);

        let returned = message::returned_data(message).unwrap();

        assert_eq!(
            message::decode_borrowed_from_slice::<&str>(returned).unwrap(),
            "Reply: hi there"
        );

        msg_sender.terminate().await.unwrap();
        handle.await.unwrap();

        std::fs::remove_file(&path).unwrap();
    }
}
