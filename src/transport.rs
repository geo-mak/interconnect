use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp;
use tokio::net::unix;
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::Mutex;

use crate::error::{ErrKind, RpcError, RpcResult};
use crate::message::Message;

const MAX_MESSAGE_SIZE: u32 = 16_777_216;

pub trait SocketAddress {}

pub trait AsyncIOStream: Sized + Send {
    type ReadingHalf: AsyncReadExt + Send + Sync + Unpin;
    type WritingHalf: AsyncWriteExt + Send + Sync + Unpin;

    fn into_split(self) -> (Self::ReadingHalf, Self::WritingHalf);
}

impl AsyncIOStream for TcpStream {
    type ReadingHalf = tcp::OwnedReadHalf;
    type WritingHalf = tcp::OwnedWriteHalf;

    #[inline(always)]
    fn into_split(self) -> (tcp::OwnedReadHalf, tcp::OwnedWriteHalf) {
        TcpStream::into_split(self)
    }
}

impl AsyncIOStream for UnixStream {
    type ReadingHalf = unix::OwnedReadHalf;
    type WritingHalf = unix::OwnedWriteHalf;

    #[inline(always)]
    fn into_split(self) -> (unix::OwnedReadHalf, unix::OwnedWriteHalf) {
        UnixStream::into_split(self)
    }
}

struct StreamState<S>
where
    S: AsyncIOStream,
{
    reader: Mutex<S::ReadingHalf>,
    writer: Mutex<S::WritingHalf>,
}

/// RPC transport layer.
/// It handles framed reads and writes to the underlying I/O stream.
/// It is full-duplex. Writing and reading can be done in parallel.
///
/// The concurrency control is managed internally,
/// to simplify the implementation of the connection layers.
pub struct RpcStream<S>
where
    S: AsyncIOStream,
{
    state: StreamState<S>,
}

impl<S> RpcStream<S>
where
    S: AsyncIOStream,
{
    /// Creates a new RPC stream from an established I/O stream.
    pub fn new(stream: S) -> Self {
        let (reader, writer) = stream.into_split();
        Self {
            state: StreamState {
                reader: Mutex::new(reader),
                writer: Mutex::new(writer),
            },
        }
    }

    /// Reads and validates the raw message and decodes it as user-level message.
    pub async fn read(&self) -> RpcResult<Message> {
        let mut reader = self.state.reader.lock().await;

        // Read 4 bytes size-metadata.
        let len = reader.read_u32().await?;

        if len > MAX_MESSAGE_SIZE {
            return Err(RpcError::error(ErrKind::LargeMessage));
        }

        // TODO: Decode from stream directly. No new buffers.
        let mut bytes = vec![0u8; len as usize];

        reader.read_exact(&mut bytes).await?;

        Message::decode(&bytes)
    }

    /// Encodes the message and writes it into the I/O stream.
    pub async fn write(&self, message: &Message) -> RpcResult<()> {
        // TODO: Encode to stream directly. No new buffers.
        let bytes = message.encode()?;

        let mut writer = self.state.writer.lock().await;

        // Write 4 bytes metadata for message size.
        writer.write_u32(bytes.len() as u32).await?;

        // Write data.
        writer.write_all(&bytes).await?;
        writer.flush().await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> RpcResult<()> {
        let mut writer = self.state.writer.lock().await;
        writer.shutdown().await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::message::MessageType;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn test_read_write_tcp_rpc() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let transport = RpcStream::new(stream);
            loop {
                match transport.read().await {
                    Ok(msg) => {
                        if let Err(e) = transport.write(&msg).await {
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

        let io_stream = TcpStream::connect(addr).await.unwrap();
        let rpc_stream = RpcStream::new(io_stream);

        let call_msg = Message::call(1, b"hi there".to_vec());
        rpc_stream.write(&call_msg).await.unwrap();

        let reply_msg = rpc_stream.read().await.unwrap();
        match (&call_msg.kind, &reply_msg.kind) {
            (MessageType::Call(sent), MessageType::Call(got)) => {
                assert_eq!(sent.method, got.method);
                assert_eq!(sent.data, got.data);
            }
            _ => panic!("Expected call"),
        }

        rpc_stream.shutdown().await.unwrap();
        handle.await.unwrap()
    }

    #[tokio::test]
    async fn test_read_write_unix_rpc() {
        let path = "unix_transport_test.sock";

        let listener = UnixListener::bind(&path).unwrap();

        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let transport = RpcStream::new(stream);
            loop {
                match transport.read().await {
                    Ok(msg) => {
                        if let Err(e) = transport.write(&msg).await {
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

        let io_stream = UnixStream::connect(&path).await.unwrap();

        let rpc_stream = RpcStream::new(io_stream);

        let call_msg = Message::call(1, b"hi there".to_vec());
        rpc_stream.write(&call_msg).await.unwrap();

        let reply_msg = rpc_stream.read().await.unwrap();
        match (&call_msg.kind, &reply_msg.kind) {
            (MessageType::Call(sent), MessageType::Call(got)) => {
                assert_eq!(sent.method, got.method);
                assert_eq!(sent.data, got.data);
            }
            _ => panic!("Expected call"),
        }

        rpc_stream.shutdown().await.unwrap();
        handle.await.unwrap();

        std::fs::remove_file(&path).unwrap();
    }
}
