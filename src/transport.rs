use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp;
use tokio::net::unix;
use tokio::net::{TcpStream, UnixStream};

use crate::error::{ErrKind, RpcError, RpcResult};
use crate::message::Message;

const MAX_MESSAGE_SIZE: u32 = 16_777_216;

pub struct RpcStreamReader<T>
where
    T: AsyncReadExt + Send + Sync + Unpin,
{
    reader: T,
}

impl<T> RpcStreamReader<T>
where
    T: AsyncReadExt + Send + Sync + Unpin,
{
    #[inline(always)]
    pub const fn new(reader: T) -> Self {
        Self { reader }
    }

    /// Reads and validates the raw message and decodes it as user-level message.
    pub async fn read(&mut self) -> RpcResult<Message> {
        // Read 4 bytes size-metadata.
        let len = self.reader.read_u32().await?;

        if len > MAX_MESSAGE_SIZE {
            return Err(RpcError::error(ErrKind::LargeMessage));
        }

        // TODO: Decode from stream directly. No new buffers.
        let mut bytes = vec![0u8; len as usize];

        self.reader.read_exact(&mut bytes).await?;

        Message::decode(&bytes)
    }
}

pub struct RpcStreamWriter<T>
where
    T: AsyncWriteExt + Send + Sync + Unpin,
{
    writer: T,
}

impl<T> RpcStreamWriter<T>
where
    T: AsyncWriteExt + Send + Sync + Unpin,
{
    #[inline(always)]
    pub const fn new(writer: T) -> Self {
        Self { writer }
    }

    /// Encodes the message and writes it into the I/O stream.
    pub async fn write(&mut self, message: &Message) -> RpcResult<()> {
        // TODO: Encode to stream directly. No new buffers.
        let bytes = message.encode()?;

        // Write 4 bytes metadata for message size.
        self.writer.write_u32(bytes.len() as u32).await?;

        // Write data.
        self.writer.write_all(&bytes).await?;
        self.writer.flush().await?;
        Ok(())
    }

    #[inline(always)]
    pub async fn shutdown(&mut self) -> RpcResult<()> {
        self.writer.shutdown().await.map_err(Into::into)
    }
}

pub trait SplitOwnedStream: Sized + Send {
    type OwnedReadHalf: AsyncReadExt + Send + Sync + Unpin;
    type OwnedWriteHalf: AsyncWriteExt + Send + Sync + Unpin;
    fn split_owned(
        self,
    ) -> (
        RpcStreamReader<Self::OwnedReadHalf>,
        RpcStreamWriter<Self::OwnedWriteHalf>,
    );
}

impl SplitOwnedStream for TcpStream {
    type OwnedReadHalf = tcp::OwnedReadHalf;
    type OwnedWriteHalf = tcp::OwnedWriteHalf;

    #[inline(always)]
    fn split_owned(
        self,
    ) -> (
        RpcStreamReader<Self::OwnedReadHalf>,
        RpcStreamWriter<Self::OwnedWriteHalf>,
    ) {
        let (reader, writer) = TcpStream::into_split(self);
        (RpcStreamReader { reader }, RpcStreamWriter { writer })
    }
}

impl SplitOwnedStream for UnixStream {
    type OwnedReadHalf = unix::OwnedReadHalf;
    type OwnedWriteHalf = unix::OwnedWriteHalf;

    #[inline(always)]
    fn split_owned(
        self,
    ) -> (
        RpcStreamReader<Self::OwnedReadHalf>,
        RpcStreamWriter<Self::OwnedWriteHalf>,
    ) {
        let (reader, writer) = UnixStream::into_split(self);
        (RpcStreamReader { reader }, RpcStreamWriter { writer })
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
            let (mut rpc_reader, mut rpc_writer) = stream.split_owned();

            loop {
                match rpc_reader.read().await {
                    Ok(msg) => {
                        if let Err(e) = rpc_writer.write(&msg).await {
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
        let (mut rpc_reader, mut rpc_writer) = io_stream.split_owned();

        let call_msg = Message::call(1, b"hi there".to_vec());
        rpc_writer.write(&call_msg).await.unwrap();

        let reply_msg = rpc_reader.read().await.unwrap();
        match (&call_msg.kind, &reply_msg.kind) {
            (MessageType::Call(sent), MessageType::Call(got)) => {
                assert_eq!(sent.method, got.method);
                assert_eq!(sent.data, got.data);
            }
            _ => panic!("Expected call"),
        }

        rpc_writer.shutdown().await.unwrap();
        handle.await.unwrap()
    }

    #[tokio::test]
    async fn test_read_write_unix_rpc() {
        let path = "unix_transport_test.sock";

        let listener = UnixListener::bind(&path).unwrap();

        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut rpc_reader, mut rpc_writer) = stream.split_owned();

            loop {
                match rpc_reader.read().await {
                    Ok(msg) => {
                        if let Err(e) = rpc_writer.write(&msg).await {
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

        let (mut rpc_reader, mut rpc_writer) = io_stream.split_owned();

        let call_msg = Message::call(1, b"hi there".to_vec());
        rpc_writer.write(&call_msg).await.unwrap();

        let reply_msg = rpc_reader.read().await.unwrap();
        match (&call_msg.kind, &reply_msg.kind) {
            (MessageType::Call(sent), MessageType::Call(got)) => {
                assert_eq!(sent.method, got.method);
                assert_eq!(sent.data, got.data);
            }
            _ => panic!("Expected call"),
        }

        rpc_writer.shutdown().await.unwrap();
        handle.await.unwrap();

        std::fs::remove_file(&path).unwrap();
    }
}
