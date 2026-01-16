use core::marker::PhantomData;
use core::net::SocketAddr;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::next::error::ProtocolResult;
use crate::next::transport::io::IOSegment;
use crate::next::transport::stream;
use crate::next::transport::stream::specs::{ConnectionSpecs, EncryptionState, negotiation};
use crate::next::transport::traits::{Transport, TransportReceiver, TransportSender};

pub struct IPLinkSender<T> {
    writer: OwnedWriteHalf,
    _t: PhantomData<T>,
}

impl<T> IPLinkSender<T> {
    pub const fn new(writer: OwnedWriteHalf) -> Self {
        Self {
            writer,
            _t: PhantomData,
        }
    }
}

impl<T> TransportSender<T> for IPLinkSender<T>
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

pub struct IPLinkReceiver<T> {
    reader: OwnedReadHalf,
    _t: PhantomData<T>,
}

impl<T> IPLinkReceiver<T> {
    pub const fn new(reader: OwnedReadHalf) -> Self {
        Self {
            reader,
            _t: PhantomData,
        }
    }
}

impl<T> TransportReceiver<T> for IPLinkReceiver<T>
where
    T: IOSegment + Send + Sync,
{
    async fn receive(&mut self, destination: &mut T) -> ProtocolResult<()> {
        stream::core::receive(&mut self.reader, destination).await
    }
}

pub struct IPLink<T> {
    stream: TcpStream,
    _t: PhantomData<T>,
}

impl<T> Transport<T> for IPLink<T>
where
    T: IOSegment + Send + Sync,
{
    type Parameters = SocketAddr;

    type Sender = IPLinkSender<T>;

    type Receiver = IPLinkReceiver<T>;

    async fn connect(parameters: &Self::Parameters) -> ProtocolResult<Self> {
        let mut stream = TcpStream::connect(parameters).await?;

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
        (IPLinkSender::new(w), IPLinkReceiver::new(r))
    }
}

pub struct IPLinkSecureSender<T> {
    writer: OwnedWriteHalf,
    state: EncryptionState,
    _t: PhantomData<T>,
}

impl<T> IPLinkSecureSender<T> {
    pub const fn new(writer: OwnedWriteHalf, state: EncryptionState) -> Self {
        Self {
            writer,
            state,
            _t: PhantomData,
        }
    }
}

impl<T> TransportSender<T> for IPLinkSecureSender<T>
where
    T: IOSegment + Send + Sync,
{
    async fn send(&mut self, source: &mut T) -> ProtocolResult<()> {
        stream::core::send_encrypted(&mut self.writer, source, &mut self.state).await
    }

    async fn terminate(&mut self) -> ProtocolResult<()> {
        self.writer.shutdown().await?;
        Ok(())
    }
}

pub struct IPLinkSecureReceiver<T> {
    reader: OwnedReadHalf,
    state: EncryptionState,
    _t: PhantomData<T>,
}

impl<T> IPLinkSecureReceiver<T> {
    pub const fn new(reader: OwnedReadHalf, state: EncryptionState) -> Self {
        Self {
            reader,
            state,
            _t: PhantomData,
        }
    }
}

impl<T> TransportReceiver<T> for IPLinkSecureReceiver<T>
where
    T: IOSegment + Send + Sync,
{
    async fn receive(&mut self, destination: &mut T) -> ProtocolResult<()> {
        stream::core::receive_encrypted(&mut self.reader, destination, &mut self.state).await
    }
}

pub struct IPLinkSecure<T> {
    stream: TcpStream,
    r_key: EncryptionState,
    w_key: EncryptionState,
    _t: PhantomData<T>,
}

impl<T> Transport<T> for IPLinkSecure<T>
where
    T: IOSegment + Send + Sync,
{
    type Parameters = SocketAddr;

    type Sender = IPLinkSecureSender<T>;

    type Receiver = IPLinkSecureReceiver<T>;

    async fn connect(parameters: &Self::Parameters) -> ProtocolResult<Self> {
        let mut stream = TcpStream::connect(parameters).await?;

        negotiation::initiate(&mut stream, ConnectionSpecs::new(1, true)).await?;

        let (r_key, w_key) = negotiation::initiate_key_exchange(&mut stream).await?;

        Ok(Self {
            stream,
            r_key,
            w_key,
            _t: PhantomData,
        })
    }

    async fn send(&mut self, source: &mut T) -> ProtocolResult<()> {
        stream::core::send_encrypted(&mut self.stream, source, &mut self.w_key).await
    }

    async fn receive(&mut self, destination: &mut T) -> ProtocolResult<()> {
        stream::core::receive_encrypted(&mut self.stream, destination, &mut self.r_key).await
    }

    async fn terminate(&mut self) -> ProtocolResult<()> {
        self.stream.shutdown().await?;
        Ok(())
    }

    fn split(self) -> (Self::Sender, Self::Receiver) {
        let (r, w) = self.stream.into_split();
        (
            IPLinkSecureSender::new(w, self.w_key),
            IPLinkSecureReceiver::new(r, self.r_key),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::next::transport::io::IOPool;
    use crate::next::transport::io::IOPoolSegment;

    #[tokio::test]
    async fn test_ip_link_send_receive() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

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
        let mut link = IPLink::<IOPoolSegment>::connect(&addr).await.unwrap();

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
    }

    #[tokio::test]
    async fn test_ip_link_secure_send_receive() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Server side.
        let server_handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            // Negotiation.
            let specs = negotiation::read_frame(&mut socket).await.unwrap();
            assert_eq!(specs.abi, 1);
            assert!(specs.encryption);
            negotiation::confirm(&mut socket).await.unwrap();

            // Key exchange.
            let (mut r_state, mut w_state) =
                negotiation::accept_key_exchange(&mut socket).await.unwrap();

            // Receive encrypted message.
            let mut len_buf = [0u8; 4];
            socket.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut data = vec![0u8; len];
            socket.read_exact(&mut data).await.unwrap();

            let mut buffer = data;
            r_state.decrypt(&mut buffer, b"").unwrap();
            assert_eq!(buffer, b"hello secure client");

            // Send encrypted response.
            let mut response_data = b"hello secure server".to_vec();
            w_state.encrypt(&mut response_data, b"").unwrap();
            let total_len = (response_data.len() as u32).to_le_bytes();
            socket.write_all(&total_len).await.unwrap();
            socket.write_all(&response_data).await.unwrap();
        });

        // Client side.
        let mut link = IPLinkSecure::<IOPoolSegment>::connect(&addr).await.unwrap();

        let pool = IOPool::new(2, 1024);

        // Send.
        let mut segment = pool.acquire().unwrap();
        segment.write(b"hello secure client");
        link.send(&mut segment).await.unwrap();

        // Receive.
        let mut recv_segment = pool.acquire().unwrap();
        link.receive(&mut recv_segment).await.unwrap();
        assert_eq!(recv_segment.as_slice(), b"hello secure server");

        server_handle.await.unwrap();
    }
}
