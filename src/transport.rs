use bincode::enc::write::Writer;
use bincode::error::EncodeError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::{ErrKind, RpcError, RpcResult};
use crate::message::Message;

const MAX_DATA_SIZE: u32 = 16_777_216;

pub struct AsyncRpcReceiver<T>
where
    T: AsyncReadExt + Send + Sync + Unpin,
{
    reader: T,
    bytes: Vec<u8>,
}

impl<T> AsyncRpcReceiver<T>
where
    T: AsyncReadExt + Send + Sync + Unpin,
{
    #[inline(always)]
    pub fn new(reader: T) -> Self {
        Self {
            reader,
            bytes: Vec::new(),
        }
    }

    /// Reads and validates the RPC frame and decodes its data as a message.
    /// This method is not cancellation-safe, and it should not be awaited as part of selection race.
    pub async fn receive(&mut self) -> RpcResult<Message> {
        // Read 4 bytes size-metadata.
        let len = self.reader.read_u32().await?;

        if len > MAX_DATA_SIZE {
            return Err(RpcError::error(ErrKind::LargeMessage));
        }

        self.bytes.reserve(len as usize);

        // Safety: This is safe.
        // Len is within the allocated range and matches the decoding size.
        unsafe { self.bytes.set_len(len as usize) }

        self.reader.read_exact(&mut self.bytes).await?;

        Message::decode(&self.bytes)
    }
}

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

pub struct AsyncRpcSender<T>
where
    T: AsyncWriteExt + Send + Sync + Unpin,
{
    writer: T,
    bytes: BytesWriter,
}

impl<T> AsyncRpcSender<T>
where
    T: AsyncWriteExt + Send + Sync + Unpin,
{
    #[inline(always)]
    pub fn new(writer: T) -> Self {
        Self {
            writer,
            bytes: BytesWriter { buf: Vec::new() },
        }
    }

    /// Encodes a message as RPC frame and writes it into the I/O stream.
    pub async fn send(&mut self, message: &Message) -> RpcResult<()> {
        // Safety: This is safe. I named the buffer bytes for safety reasons.
        unsafe { self.bytes.buf.set_len(0) }
        message.encode_into_writer(&mut self.bytes)?;
        self.writer.write_u32(self.bytes.buf.len() as u32).await?;
        self.writer.write_all(&self.bytes.buf).await?;
        self.writer.flush().await?;
        Ok(())
    }

    #[inline(always)]
    pub async fn close(&mut self) -> RpcResult<()> {
        self.writer.shutdown().await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{OwnedSplitStream, message::MessageType};
    use tokio::net::{TcpStream, UnixListener, UnixStream};

    #[tokio::test]
    async fn test_read_write_tcp_rpc() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let (io_stream, _) = listener.accept().await.unwrap();

            let (reader, writer) = io_stream.owned_split();
            let mut rpc_reader = AsyncRpcReceiver::new(reader);
            let mut rpc_writer = AsyncRpcSender::new(writer);

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

        let io_stream = TcpStream::connect(addr).await.unwrap();
        let (io_reader, io_writer) = io_stream.owned_split();

        let mut rpc_reader = AsyncRpcReceiver::new(io_reader);
        let mut rpc_writer = AsyncRpcSender::new(io_writer);

        let call_msg = Message::call(1, b"hi there".to_vec());
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
            let (io_stream, _) = listener.accept().await.unwrap();
            let (io_reader, io_writer) = io_stream.owned_split();

            let mut rpc_reader = AsyncRpcReceiver::new(io_reader);
            let mut rpc_writer = AsyncRpcSender::new(io_writer);

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

        let io_stream = UnixStream::connect(&path).await.unwrap();

        let (io_reader, io_writer) = io_stream.owned_split();
        let mut rpc_reader = AsyncRpcReceiver::new(io_reader);
        let mut rpc_writer = AsyncRpcSender::new(io_writer);

        let call_msg = Message::call(1, b"hi there".to_vec());
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
