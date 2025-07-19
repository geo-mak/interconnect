use bincode::config::{self, Configuration};
use serde::{Deserialize, Serialize};

use uuid::Uuid;

use crate::{RpcError, error::RpcResult};

const CONFIG: Configuration = config::standard();

/// RPC call message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Call {
    pub method: String,
    pub data: Vec<u8>,
}

/// RPC reply message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Reply {
    pub data: Vec<u8>,
}

/// Message types of the RPC protocol.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MessageType {
    /// A bidirectional call (can be initiated by either side).
    Call(Call),

    /// A response message (can be returned by either side).
    Reply(Reply),

    Error(RpcError),

    /// A notification message (no response expected).
    Notification(Call),

    /// A heartbeat/ping message.
    Ping,

    /// A heartbeat/pong response.
    Pong,
}

/// Core message structure for the RPC protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Protocol version.
    pub version: u8,

    /// A 128-bit (16 byte) unique identifier for the message.
    // TODO: Use integer with tracker later, to reduce message size.
    pub id: Uuid,

    /// The payload of the message.
    pub kind: MessageType,
}

impl Message {
    /// Creates a new message.
    pub fn new(id: Uuid, kind: MessageType) -> Self {
        Self {
            version: 1,
            id,
            kind,
        }
    }

    /// Creates a new message with auto-generated id.
    pub fn new_with_id(kind: MessageType) -> Self {
        Self {
            version: 1,
            id: Uuid::new_v4(),
            kind,
        }
    }

    /// Creates a call message (bidirectional) with auto-generated id.
    pub fn call(method: String, data: Vec<u8>) -> Self {
        Self::new_with_id(MessageType::Call(Call { method, data }))
    }

    /// Creates a response message.
    pub fn reply(id: Uuid, data: Vec<u8>) -> Self {
        Self::new(id, MessageType::Reply(Reply { data }))
    }

    /// Creates an error message.
    pub fn error(id: Uuid, err: RpcError) -> Self {
        Self::new(id, MessageType::Error(err))
    }

    /// Creates a notification message.
    pub fn notification(method: String, data: Vec<u8>) -> Self {
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
    pub fn encode(&self) -> crate::RpcResult<Vec<u8>> {
        bincode::serde::encode_to_vec(self, CONFIG).map_err(Into::into)
    }

    /// Decodes a message from binary format.
    pub fn decode(message: &[u8]) -> crate::RpcResult<Self> {
        bincode::serde::borrow_decode_from_slice(message, CONFIG)
            .map(|(msg, _)| msg)
            .map_err(Into::into)
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
    pub fn call_with<P: Serialize>(method: String, params: P) -> RpcResult<Self> {
        let data = Self::encode_to_bytes(&params)?;
        Ok(Self::call(method, data))
    }

    /// Creates a response message with typed value.
    pub fn reply_with<R: Serialize>(id: Uuid, value: R) -> RpcResult<Self> {
        let data = Self::encode_to_bytes(&value)?;
        Ok(Self::reply(id, data))
    }

    /// Creates a notification message with typed parameters.
    pub fn notification_with<P: Serialize>(method: String, params: P) -> RpcResult<Self> {
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
        let original_msg = Message::call("test_method".to_string(), vec![10, 20, 30]);

        let serialized = original_msg.encode().unwrap();
        let deserialized_msg = Message::decode(&serialized).unwrap();

        assert_eq!(original_msg.version, deserialized_msg.version);
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
        let message = Message::call_with("add".to_string(), (5, 3)).unwrap();
        match &message.kind {
            MessageType::Call(call) => {
                assert_eq!(call.method, "add");
                let params_add: (i32, i32) = Message::decode_as(&call.data).unwrap();
                assert_eq!(params_add, (5, 3));
                assert!(message.expects_response());
            }
            _ => panic!("Expected call"),
        }
    }

    #[test]
    fn test_reply_with() {
        let id = Uuid::new_v4();
        let message = Message::reply_with(id, 8).unwrap();
        match &message.kind {
            MessageType::Reply(reply) => {
                let value: i32 = Message::decode_as(&reply.data).unwrap();
                assert_eq!(value, 8);
                assert!(!message.expects_response());
            }
            _ => panic!("Expected reply"),
        }
    }

    #[test]
    fn test_notification_with() {
        let message =
            Message::notification_with("notify".to_string(), (42, "hello".to_string())).unwrap();
        match &message.kind {
            MessageType::Notification(notify) => {
                assert_eq!(notify.method, "notify");
                let params: (i32, String) = Message::decode_as(&notify.data).unwrap();
                assert_eq!(params, (42, "hello".to_string()));
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
