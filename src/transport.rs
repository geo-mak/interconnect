use bincode::enc::write::Writer;
use bincode::error::EncodeError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::{ErrKind, RpcError, RpcResult};
use crate::message::Message;

const MAX_DATA_SIZE: u32 = 16 * 1024 * 1024;

// Definitely not for bulk throughput or streams, but streams are a different story.
const FRAMING_CAPACITY: usize = 1024;

// TODO: TLS support.
pub struct AsyncRpcReceiver<T> {
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

    /// Reads and validates the RPC frame and decodes its data as a message.
    /// This method is not cancellation-safe, and it should not be awaited as part of selection race.
    /// **Currently**, if an error has been returned, receiving must be terminated and the stream must be closed.
    #[allow(clippy::uninit_vec)]
    pub async fn receive(&mut self) -> RpcResult<Message> {
        // Read 4 bytes size prefix.
        let len = self.reader.read_u32_le().await?;

        if len > MAX_DATA_SIZE {
            return Err(RpcError::error(ErrKind::LargeMessage));
        }

        // Reserve uninitialized memory.
        self.bytes.reserve(len as usize);

        // Safety: This should be safe because:
        //
        // 1 - Buffer has allocated memory for `len` bytes.
        //
        // 2 - `len` matches the expected decoding size.
        //
        // 3 - In Rust, any operation (Add, Sub, Mul, etc.) with uninitialized memory is UB,
        //     because the byte-pattern of that memory will not be interpreted in consistent manner (random),
        //     but we are not doing any operation except copying/overwriting FROM the stream TO the buffer,
        //     and only on success, or otherwise, an error will be returned.
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

// TODO: TLS support.
pub struct AsyncRpcSender<T> {
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
                buf: Vec::with_capacity(framing_cap),
            },
        }
    }

    /// Encodes a message as RPC frame and writes it into the I/O stream.
    pub async fn send(&mut self, message: &Message) -> RpcResult<()> {
        unsafe { self.bytes.buf.set_len(0) }

        message.encode_into_writer(&mut self.bytes)?;

        // Write 4-bytes size prefix.
        self.writer
            .write_u32_le(self.bytes.buf.len() as u32)
            .await?;

        self.writer.write_all(&self.bytes.buf).await?;
        self.writer.flush().await?;
        Ok(())
    }

    #[inline(always)]
    pub async fn close(&mut self) -> RpcResult<()> {
        // Why only writer exposes shutdown?
        self.writer.shutdown().await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use tokio::net::{TcpStream, UnixListener, UnixStream};

    use crate::connection::OwnedSplitStream;
    use crate::message::MessageType;

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
