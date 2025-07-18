use bincode::config::{self, Configuration};
use serde::{Deserialize, Serialize};

use uuid::Uuid;

use crate::error::RpcResult;

const CONFIG: Configuration = config::standard();

/// Message types in the RPC protocol.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MessageType {
    /// A request message from client to server.
    Request,

    /// A response message from server to client.
    Response,

    /// A bidirectional call (can be initiated by either side).
    Call,

    /// A notification message (no response expected).
    Notification,

    /// An error response.
    Error,

    /// A heartbeat/ping message.
    Ping,

    /// A heartbeat/pong response.
    Pong,
}

/// Core message structure for the RPC protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// A 128-bit (16 byte) unique identifier for the message.
    // TODO: Use integer with tracker later, to reduce message size.
    pub id: Uuid,

    /// Type of the message.
    pub kind: MessageType,

    /// Method name being called.
    pub method: String,

    /// Serialized parameters/payload.
    pub data: Vec<u8>,

    /// Message timestamp (Unix timestamp in milliseconds).
    // TODO: Rarely useful.
    pub timestamp: u64,

    /// Protocol version.
    pub version: u8,
}

impl Message {
    /// Creates a new message.
    pub fn new(id: Uuid, message_type: MessageType, method: String, data: Vec<u8>) -> Self {
        Self {
            id,
            kind: message_type,
            method,
            data,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            version: 1,
        }
    }

    /// Creates a new message with auto-generated id.
    pub fn new_with_id(message_type: MessageType, method: String, data: Vec<u8>) -> Self {
        Self::new(Uuid::new_v4(), message_type, method, data)
    }

    /// Creates a request message.
    pub fn request(method: String, data: Vec<u8>) -> Self {
        Self::new_with_id(MessageType::Request, method, data)
    }

    /// Creates a response message.
    pub fn response(id: Uuid, method: String, data: Vec<u8>) -> Self {
        Self::new(id, MessageType::Response, method, data)
    }

    /// Creates a notification message.
    pub fn notification(method: String, data: Vec<u8>) -> Self {
        Self::new_with_id(MessageType::Notification, method, data)
    }

    /// Creates a call message (bidirectional).
    pub fn call(method: String, data: Vec<u8>) -> Self {
        Self::new_with_id(MessageType::Call, method, data)
    }

    /// Creates an error message.
    pub fn error(id: Uuid, error_msg: String) -> Self {
        let data = Message::encode_data(&error_msg).unwrap_or_default();
        Self::new(id, MessageType::Error, "error".to_string(), data)
    }

    /// Creates a ping message.
    pub fn ping() -> Self {
        Self::new_with_id(MessageType::Ping, "ping".to_string(), Vec::new())
    }

    /// Creates a pong message.
    pub fn pong(id: Uuid) -> Self {
        Self::new(id, MessageType::Pong, "pong".to_string(), Vec::new())
    }

    /// Encodes the message into binary format.
    pub fn encode(&self) -> crate::RpcResult<Vec<u8>> {
        bincode::serde::encode_to_vec(self, CONFIG).map_err(Into::into)
    }

    /// Decodes a message from binary format.
    pub fn decode(data: &[u8]) -> crate::RpcResult<Self> {
        bincode::serde::borrow_decode_from_slice(data, CONFIG)
            .map(|(msg, _)| msg)
            .map_err(Into::into)
    }

    /// Encodes a value into a data.
    pub fn encode_data<T: Serialize>(value: &T) -> RpcResult<Vec<u8>> {
        bincode::serde::encode_to_vec(value, CONFIG).map_err(Into::into)
    }

    /// Decodes the message data into a value.
    pub fn decode_data<T: for<'de> Deserialize<'de>>(&self) -> RpcResult<T> {
        bincode::serde::borrow_decode_from_slice(&self.data, CONFIG)
            .map(|(value, _)| value)
            .map_err(Into::into)
    }

    /// Decodes a data from bytes into a value.
    pub fn decode_data_from<T: for<'de> Deserialize<'de>>(data: &[u8]) -> RpcResult<T> {
        bincode::serde::borrow_decode_from_slice(data, CONFIG)
            .map(|(value, _)| value)
            .map_err(Into::into)
    }

    /// Creates a request message with typed parameters.
    pub fn request_with_params<P: Serialize>(method: String, params: P) -> RpcResult<Self> {
        let data = Self::encode_data(&params)?;
        Ok(Self::request(method, data))
    }

    /// Creates a response message with typed result.
    pub fn response_with_result<R: Serialize>(
        id: Uuid,
        method: String,
        result: R,
    ) -> RpcResult<Self> {
        let data = Self::encode_data(&result)?;
        Ok(Self::response(id, method, data))
    }

    /// Creates a notification message with typed parameters.
    pub fn notification_with_params<P: Serialize>(method: String, params: P) -> RpcResult<Self> {
        let data = Self::encode_data(&params)?;
        Ok(Self::notification(method, data))
    }

    /// Creates a call message with typed parameters.
    pub fn call_with_params<P: Serialize>(method: String, params: P) -> RpcResult<Self> {
        let data = Self::encode_data(&params)?;
        Ok(Self::call(method, data))
    }

    /// Creates an error message with a typed error.
    pub fn error_with_details<E: Serialize>(id: Uuid, error: E) -> RpcResult<Self> {
        let data = Self::encode_data(&error)?;
        Ok(Self::new(id, MessageType::Error, "error".to_string(), data))
    }

    /// Checks if this message expects a response.
    pub fn expects_response(&self) -> bool {
        matches!(
            self.kind,
            MessageType::Request | MessageType::Call | MessageType::Ping
        )
    }

    /// Checks if this message is a response.
    pub fn is_response(&self) -> bool {
        matches!(
            self.kind,
            MessageType::Response | MessageType::Error | MessageType::Pong
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_encoding_decoding() {
        let original_msg = Message::notification("notify".to_string(), vec![10, 20, 30]);
        let serialized = original_msg.encode().unwrap();

        let deserialized_msg = Message::decode(&serialized).unwrap();

        assert_eq!(original_msg.id, deserialized_msg.id);
        assert_eq!(original_msg.method, deserialized_msg.method);
        assert_eq!(original_msg.kind, deserialized_msg.kind);
        assert_eq!(original_msg.data, deserialized_msg.data);
        assert_eq!(original_msg.version, deserialized_msg.version);
    }

    #[test]
    fn test_request_response_and_params() {
        // Test direct message creation with typed parameters for 'add'
        let req_add = Message::request_with_params("add".to_string(), (5, 3)).unwrap();
        assert_eq!(req_add.method, "add");
        assert_eq!(req_add.kind, MessageType::Request);
        assert!(req_add.expects_response());
        let params_add: (i32, i32) = req_add.decode_data().unwrap();
        assert_eq!(params_add, (5, 3));

        let resp = Message::response_with_result(req_add.id, "add".to_string(), 8).unwrap();
        assert_eq!(resp.kind, MessageType::Response);
        let value: i32 = resp.decode_data().unwrap();
        assert_eq!(value, 8);
    }

    #[test]
    fn test_error_with_details() {
        let id = Uuid::new_v4();
        let err_msg = "Division by zero";
        let error_msg = Message::error_with_details(id, err_msg);
        assert!(error_msg.is_ok());
        let error_msg = error_msg.unwrap();
        assert_eq!(error_msg.kind, MessageType::Error);
    }

    #[test]
    fn test_request_response_semantics() {
        // Request MUST have a response.
        let request = Message::request("get_data".to_string(), vec![]);
        assert!(request.expects_response());
        assert_eq!(request.kind, MessageType::Request);

        // Response for successful request.
        let response = Message::response(request.id, "get_data".to_string(), vec![1, 2, 3]);
        assert!(response.is_response());
        assert_eq!(response.kind, MessageType::Response);

        // Error response for failed request.
        let error = Message::error(request.id, "Database error".to_string());
        assert!(error.is_response());
        assert_eq!(error.kind, MessageType::Error);

        // Notification expects NO response.
        let notification = Message::notification("state_changed".to_string(), vec![]);
        assert!(!notification.expects_response());
        assert_eq!(notification.kind, MessageType::Notification);
    }
}
