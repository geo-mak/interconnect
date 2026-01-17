use core::marker::PhantomData;

use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

use crate::next::error::ProtocolResult;
use crate::next::mem::IOSegment;
use crate::next::transport::stream;
use crate::next::transport::stream::specs::{ConnectionSpecs, negotiation};
use crate::next::transport::traits::{Transport, TransportReceiver, TransportSender};

pub struct UnixLinkSender<T> {
    writer: OwnedWriteHalf,
    _t: PhantomData<T>,
}

impl<T> UnixLinkSender<T> {
    pub const fn new(writer: OwnedWriteHalf) -> Self {
        Self {
            writer,
            _t: PhantomData,
        }
    }
}

impl<T> TransportSender<T> for UnixLinkSender<T>
where
    T: IOSegment + Send + Sync,
{
    async fn send(&mut self, source: &mut T) -> ProtocolResult<()> {
        stream::core::send(&mut self.writer, source).await
    }

    async fn terminate(&mut self) -> ProtocolResult<()> {
        self.writer.shutdown().await?;
        Ok(())
    }
}

pub struct UnixLinkReceiver<T> {
    reader: OwnedReadHalf,
    _t: PhantomData<T>,
}

impl<T> UnixLinkReceiver<T> {
    pub const fn new(reader: OwnedReadHalf) -> Self {
        Self {
            reader,
            _t: PhantomData,
        }
    }
}

impl<T> TransportReceiver<T> for UnixLinkReceiver<T>
where
    T: IOSegment + Send + Sync,
{
    async fn receive(&mut self, destination: &mut T) -> ProtocolResult<()> {
        stream::core::receive(&mut self.reader, destination).await
    }
}

pub struct UnixLink<T> {
    stream: UnixStream,
    _t: PhantomData<T>,
}

impl<T> Transport<T> for UnixLink<T>
where
    T: IOSegment + Send + Sync,
{
    type Parameters = &'static str;

    type Sender = UnixLinkSender<T>;

    type Receiver = UnixLinkReceiver<T>;

    async fn connect(parameters: &Self::Parameters) -> ProtocolResult<Self> {
        let mut stream = UnixStream::connect(parameters).await?;

        negotiation::initiate(&mut stream, ConnectionSpecs::new(1, false)).await?;

        Ok(Self {
            stream,
            _t: PhantomData,
        })
    }

    async fn send(&mut self, source: &mut T) -> ProtocolResult<()> {
        stream::core::send(&mut self.stream, source).await
    }

    async fn receive(&mut self, destination: &mut T) -> ProtocolResult<()> {
        stream::core::receive(&mut self.stream, destination).await
    }

    async fn terminate(&mut self) -> ProtocolResult<()> {
        self.stream.shutdown().await?;
        Ok(())
    }

    fn split(self) -> (Self::Sender, Self::Receiver) {
        let (r, w) = self.stream.into_split();
        (UnixLinkSender::new(w), UnixLinkReceiver::new(r))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;

    use crate::next::mem::IOPool;
    use crate::next::mem::IOPoolSegment;

    #[tokio::test]
    async fn test_unix_link_send_receive() {
        let path = "/tmp/test_interconnect_uds.sock";
        let _ = std::fs::remove_file(path);
        let listener = UnixListener::bind(path).unwrap();

        // Server side.
        let server_handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            // Negotiation.
            let specs = negotiation::read_frame(&mut socket).await.unwrap();
            assert_eq!(specs.abi, 1);
            assert!(!specs.encryption);
            negotiation::confirm(&mut socket).await.unwrap();

            // Receive message.
            let mut len_buf = [0u8; 4];
            socket.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut data = vec![0u8; len];
            socket.read_exact(&mut data).await.unwrap();
            assert_eq!(data, b"hello from client");

            // Send response.
            let response = b"hello from server";
            let total_len = (response.len() as u32).to_le_bytes();
            socket.write_all(&total_len).await.unwrap();
            socket.write_all(response).await.unwrap();
        });

        // Client side.
        let mut link = UnixLink::<IOPoolSegment>::connect(&path).await.unwrap();

        // Send.
        let pool = IOPool::new(2, 1024);
        let mut segment = pool.acquire().unwrap();
        segment.write(b"hello from client");
        link.send(&mut segment).await.unwrap();

        // Receive.
        let mut recv_segment = pool.acquire().unwrap();
        link.receive(&mut recv_segment).await.unwrap();
        assert_eq!(recv_segment.as_slice(), b"hello from server");

        server_handle.await.unwrap();
        let _ = std::fs::remove_file(path);
    }
}
