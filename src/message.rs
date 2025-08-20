use bincode::config::{Configuration, standard};
use bincode::de::read::Reader;
use bincode::enc::write::Writer;

use serde::{Deserialize, Serialize};

use uuid::Uuid;

use crate::error::{RpcError, RpcResult};

const CONFIG: Configuration = standard();

/// RPC call message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Call {
    pub method: u16,
    pub data: Vec<u8>,
}

impl Call {
    #[inline(always)]
    pub fn decode_as<T: for<'de> Deserialize<'de>>(&self) -> RpcResult<T> {
        Message::decode_as(&self.data)
    }
}

/// RPC notification message.
pub type Notification = Call;

/// RPC reply message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Reply {
    pub data: Vec<u8>,
}

impl Reply {
    #[inline(always)]
    pub fn with<T: Serialize>(value: &T) -> RpcResult<Self> {
        let instance = Self {
            data: Message::encode_to_bytes(value)?,
        };
        Ok(instance)
    }
}

/// Message types of the RPC protocol.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MessageType {
    /// A bidirectional call (can be initiated by either side).
    Call(Call),

    /// A response message (can be returned by either side).
    Reply(Reply),

    /// Error message is a lightweight structure for operational errors.
    /// Rich and detailed errors are considered part of the service design,
    /// and subject to the documented reply type of a particular method.
    Error(RpcError),

    /// A notification message (no response expected).
    Notification(Notification),

    /// A heartbeat/ping message.
    Ping,

    /// A heartbeat/pong response.
    Pong,
}

/// High-level message structure of the RPC protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// A 128-bit (16 byte) unique identifier for the message.
    // TODO: Use integer with tracker later, to reduce message size.
    pub id: Uuid,

    /// The payload of the message.
    pub kind: MessageType,
}

impl Message {
    /// Creates a new message.
    pub fn new(id: Uuid, kind: MessageType) -> Self {
        Self { id, kind }
    }

    /// Creates a new message with auto-generated id.
    pub fn new_with_id(kind: MessageType) -> Self {
        Self {
            id: Uuid::new_v4(),
            kind,
        }
    }

    /// Creates a call message (bidirectional) with auto-generated id.
    pub fn call(method: u16, data: Vec<u8>) -> Self {
        Self::new_with_id(MessageType::Call(Call { method, data }))
    }

    /// Creates a response message.
    pub fn reply(id: Uuid, reply: Reply) -> Self {
        Self::new(id, MessageType::Reply(reply))
    }

    /// Creates an error message.
    pub fn error(id: Uuid, err: RpcError) -> Self {
        Self::new(id, MessageType::Error(err))
    }

    /// Creates a notification message.
    pub fn notification(method: u16, data: Vec<u8>) -> Self {
        Self::new_with_id(MessageType::Notification(Call { method, data }))
    }

    /// Creates a ping message with auto-generated id.
    pub fn ping() -> Self {
        Self::new_with_id(MessageType::Ping)
    }

    /// Creates a pong message.
    pub fn pong(id: Uuid) -> Self {
        Self::new(id, MessageType::Pong)
    }

    /// Encodes the message into binary format.
    pub fn encode(&self) -> RpcResult<Vec<u8>> {
        bincode::serde::encode_to_vec(self, CONFIG).map_err(Into::into)
    }

    /// Decodes a message from binary format.
    pub fn decode(message: &[u8]) -> RpcResult<Self> {
        bincode::serde::borrow_decode_from_slice(message, CONFIG)
            .map(|(msg, _)| msg)
            .map_err(Into::into)
    }

    /// Encodes and writes the message into a writer.
    pub fn encode_into_writer<W: Writer>(&self, dst: &mut W) -> RpcResult<()> {
        bincode::serde::encode_into_writer(self, dst, CONFIG).map_err(Into::into)
    }

    /// Decodes a message from the given reader.
    pub fn decode_from_reader<R: Reader>(src: &mut R) -> RpcResult<Self> {
        bincode::serde::decode_from_reader(src, CONFIG).map_err(Into::into)
    }

    /// Encodes a value into binary format.
    pub fn encode_to_bytes<T: Serialize>(value: &T) -> RpcResult<Vec<u8>> {
        bincode::serde::encode_to_vec(value, CONFIG).map_err(Into::into)
    }

    /// Decodes data from bytes into a value.
    pub fn decode_as<T: for<'de> Deserialize<'de>>(data: &[u8]) -> RpcResult<T> {
        bincode::serde::borrow_decode_from_slice(data, CONFIG)
            .map(|(value, _)| value)
            .map_err(Into::into)
    }

    /// Creates a request message with typed parameters.
    pub fn call_with<P: Serialize>(method: u16, params: P) -> RpcResult<Self> {
        let data = Self::encode_to_bytes(&params)?;
        Ok(Self::call(method, data))
    }

    /// Creates a response message with typed value.
    pub fn reply_with<R: Serialize>(id: Uuid, value: R) -> RpcResult<Self> {
        let data = Self::encode_to_bytes(&value)?;
        Ok(Self::reply(id, Reply { data }))
    }

    /// Creates a notification message with typed parameters.
    pub fn notification_with<P: Serialize>(method: u16, params: P) -> RpcResult<Self> {
        let data = Self::encode_to_bytes(&params)?;
        Ok(Self::notification(method, data))
    }

    /// Checks if this message expects a response.
    pub fn expects_response(&self) -> bool {
        matches!(self.kind, MessageType::Call(_) | MessageType::Ping)
    }

    /// Checks if this message is a response.
    pub fn is_response(&self) -> bool {
        matches!(
            self.kind,
            MessageType::Reply(_) | MessageType::Error(_) | MessageType::Pong
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::error::ErrKind;

    use super::*;

    #[test]
    fn test_message_encoding_decoding() {
        let original_msg = Message::call(1, vec![10, 20, 30]);

        let serialized = original_msg.encode().unwrap();
        let deserialized_msg = Message::decode(&serialized).unwrap();

        assert_eq!(original_msg.id, original_msg.id);

        match (&original_msg.kind, &deserialized_msg.kind) {
            (MessageType::Call(orig_call), MessageType::Call(decoded_call)) => {
                assert_eq!(orig_call.method, decoded_call.method);
                assert_eq!(orig_call.data, decoded_call.data);
            }
            _ => panic!("Expected call"),
        }
    }

    #[test]
    fn test_call_with() {
        let message = Message::call_with(1, (5u8, 3u8)).unwrap();
        match &message.kind {
            MessageType::Call(call) => {
                assert_eq!(call.method, 1);
                let params: (u8, u8) = Message::decode_as(&call.data).unwrap();
                assert_eq!(params, (5, 3));
                assert!(message.expects_response());
            }
            _ => panic!("Expected call"),
        }
    }

    #[test]
    fn test_reply_with() {
        let id = Uuid::new_v4();
        let message = Message::reply_with(id, 8u8).unwrap();
        match &message.kind {
            MessageType::Reply(reply) => {
                let value: u8 = Message::decode_as(&reply.data).unwrap();
                assert_eq!(value, 8);
                assert!(!message.expects_response());
            }
            _ => panic!("Expected reply"),
        }
    }

    #[test]
    fn test_notification_with() {
        let message = Message::notification_with(1, (22u8, 1u8)).unwrap();
        match &message.kind {
            MessageType::Notification(notify) => {
                assert_eq!(notify.method, 1);
                let params: (u8, u8) = Message::decode_as(&notify.data).unwrap();
                assert_eq!(params, (22, 1));
                assert!(!message.expects_response());
            }
            _ => panic!("Expected notification"),
        }
    }

    #[test]
    fn test_error() {
        let id = Uuid::new_v4();
        let err = RpcError::error(ErrKind::Timeout);
        let message = Message::error(id, err);
        match &message.kind {
            MessageType::Error(e) => {
                assert_eq!(e.kind, ErrKind::Timeout);
                assert!(!message.expects_response());
            }
            _ => panic!("Expected error"),
        }
    }

    #[test]
    fn test_ping() {
        let message = Message::ping();
        match &message.kind {
            MessageType::Ping => {
                assert!(message.expects_response());
                assert!(!message.is_response());
            }
            _ => panic!("Expected ping"),
        }
    }

    #[test]
    fn test_pong() {
        let id = Uuid::new_v4();
        let message = Message::pong(id);
        match &message.kind {
            MessageType::Pong => {
                assert!(!message.expects_response());
                assert!(message.is_response());
                assert_eq!(message.id, id);
            }
            _ => panic!("Expected pong"),
        }
    }
}
