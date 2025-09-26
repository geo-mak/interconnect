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
    /// A call message that may or may not have reply.
    ///
    /// This call targets methods that take extra parameters.
    Call(Call),

    /// A call message that may or may not have reply.
    ///
    /// This call targets methods that don't take any extra parameters.
    NullaryCall(u16),

    /// A response message (can be returned by either side).
    Reply(Reply),

    /// Error message is a lightweight structure for operational errors.
    /// Rich and detailed errors are considered part of the service design,
    /// and subject to the documented reply type of a particular method.
    Error(RpcError),

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

    /// Creates a call message with auto-generated id.
    pub fn call(method: u16, data: Vec<u8>) -> Self {
        Self::new_with_id(MessageType::Call(Call { method, data }))
    }

    /// Creates a nullary call message with auto-generated id.
    pub fn nullary_call(method: u16) -> Self {
        Self::new_with_id(MessageType::NullaryCall(method))
    }

    /// Creates a response message.
    pub fn reply(id: Uuid, reply: Reply) -> Self {
        Self::new(id, MessageType::Reply(reply))
    }

    /// Creates an error message.
    pub fn error(id: Uuid, err: RpcError) -> Self {
        Self::new(id, MessageType::Error(err))
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
        let id = Uuid::new_v4();
        let data = vec![10, 20, 30];

        let call = Message::call(1, data.clone());
        let enc_call = call.encode().unwrap();
        let dec_call = Message::decode(&enc_call).unwrap();
        assert_eq!(call.id, dec_call.id);
        match (&call.kind, &dec_call.kind) {
            (MessageType::Call(orig_call), MessageType::Call(decoded_call)) => {
                assert_eq!(orig_call.method, decoded_call.method);
                assert_eq!(orig_call.data, decoded_call.data);
            }
            _ => panic!("Expected call"),
        }

        let nullary_call = Message::nullary_call(1);
        let enc_nullary_call = nullary_call.encode().unwrap();
        let dec_nullary_call = Message::decode(&enc_nullary_call).unwrap();
        assert_eq!(nullary_call.id, dec_nullary_call.id);
        match (&nullary_call.kind, &dec_nullary_call.kind) {
            (MessageType::NullaryCall(orig_method), MessageType::NullaryCall(decoded_method)) => {
                assert_eq!(orig_method, decoded_method);
            }
            _ => panic!("Expected nullary call"),
        }

        let reply_msg = Message::reply(id, Reply { data });
        let enc_reply = reply_msg.encode().unwrap();
        let dec_reply = Message::decode(&enc_reply).unwrap();
        assert_eq!(reply_msg.id, dec_reply.id);
        match (&reply_msg.kind, &dec_reply.kind) {
            (MessageType::Reply(orig_reply), MessageType::Reply(decoded_reply)) => {
                assert_eq!(orig_reply.data, decoded_reply.data);
            }
            _ => panic!("Expected reply"),
        }

        let err_msg = Message::error(id, RpcError::error(ErrKind::Timeout));
        let enc_err = err_msg.encode().unwrap();
        let dec_err = Message::decode(&enc_err).unwrap();
        assert_eq!(err_msg.id, dec_err.id);
        match (&err_msg.kind, &dec_err.kind) {
            (MessageType::Error(orig_err), MessageType::Error(decoded_err)) => {
                assert_eq!(orig_err, decoded_err);
            }
            _ => panic!("Expected error"),
        }

        let ping_msg = Message::ping();
        let enc_ping = ping_msg.encode().unwrap();
        let dec_ping = Message::decode(&enc_ping).unwrap();
        assert_eq!(ping_msg.id, dec_ping.id);
        assert_eq!(dec_ping.kind, MessageType::Ping, "Expected ping");

        let pong_msg = Message::pong(id);
        let enc_pong = pong_msg.encode().unwrap();
        let dec_pong = Message::decode(&enc_pong).unwrap();
        assert_eq!(pong_msg.id, dec_pong.id);
        assert_eq!(dec_pong.kind, MessageType::Pong, "Expected pong");
    }

    #[test]
    fn test_call_with() {
        let message = Message::call_with(1, "parameters").unwrap();
        match &message.kind {
            MessageType::Call(call) => {
                assert_eq!(call.method, 1);
                let params: String = Message::decode_as(&call.data).unwrap();
                assert_eq!(params, "parameters");
            }
            _ => panic!("Expected call"),
        }
    }

    #[test]
    fn test_reply_with() {
        let id = Uuid::new_v4();
        let message = Message::reply_with(id, "some reply").unwrap();
        match &message.kind {
            MessageType::Reply(reply) => {
                let value: String = Message::decode_as(&reply.data).unwrap();
                assert_eq!(value, "some reply");
            }
            _ => panic!("Expected reply"),
        }
    }
}
