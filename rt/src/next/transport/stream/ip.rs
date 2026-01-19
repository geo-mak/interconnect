use core::marker::PhantomData;
use core::net::SocketAddr;

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

use crate::next::error::{ErrKind, ProtocolError, ProtocolResult};
use crate::next::mem::IOSegment;
use crate::next::transport::stream;
use crate::next::transport::stream::specs::{ConnectionSpecs, EncryptionState, negotiation};
use crate::next::transport::traits::{
    Transport, TransportInitiator, TransportReceiver, TransportSender, TransportServer,
};

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

pub struct IPLink<S, R> {
    stream: TcpStream,
    _s: PhantomData<S>,
    _r: PhantomData<R>,
}

impl<S, R> IPLink<S, R> {
    #[inline]
    const fn from(stream: TcpStream) -> Self {
        Self {
            stream,
            _s: PhantomData,
            _r: PhantomData,
        }
    }
}

impl<S, R> Transport<S, R> for IPLink<S, R>
where
    S: IOSegment + Send + Sync,
    R: IOSegment + Send + Sync,
{
    type Parameters = SocketAddr;

    type Sender = IPLinkSender<S>;

    type Receiver = IPLinkReceiver<R>;

    async fn connect(parameters: &Self::Parameters) -> ProtocolResult<Self> {
        let mut stream = TcpStream::connect(parameters).await?;

        negotiation::initiate(&mut stream, ConnectionSpecs::new(1, false)).await?;

        Ok(Self {
            stream,
            _s: PhantomData,
            _r: PhantomData,
        })
    }

    async fn send(&mut self, source: &mut S) -> ProtocolResult<()> {
        stream::core::send(&mut self.stream, source).await
    }

    async fn receive(&mut self, destination: &mut R) -> ProtocolResult<()> {
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

pub struct IPLinkSecure<S, R> {
    stream: TcpStream,
    send_state: EncryptionState,
    recv_state: EncryptionState,
    _s: PhantomData<S>,
    _r: PhantomData<R>,
}

impl<S, R> IPLinkSecure<S, R> {
    #[inline]
    const fn from(
        stream: TcpStream,
        send_state: EncryptionState,
        recv_state: EncryptionState,
    ) -> Self {
        Self {
            stream,
            send_state,
            recv_state,
            _s: PhantomData,
            _r: PhantomData,
        }
    }
}

impl<S, R> Transport<S, R> for IPLinkSecure<S, R>
where
    S: IOSegment + Send + Sync,
    R: IOSegment + Send + Sync,
{
    type Parameters = SocketAddr;

    type Sender = IPLinkSecureSender<S>;

    type Receiver = IPLinkSecureReceiver<R>;

    async fn connect(parameters: &Self::Parameters) -> ProtocolResult<Self> {
        let mut stream = TcpStream::connect(parameters).await?;

        negotiation::initiate(&mut stream, ConnectionSpecs::new(1, true)).await?;

        let (send_state, recv_state) = negotiation::initiate_key_exchange(&mut stream).await?;

        Ok(Self::from(stream, send_state, recv_state))
    }

    async fn send(&mut self, source: &mut S) -> ProtocolResult<()> {
        stream::core::send_encrypted(&mut self.stream, source, &mut self.send_state).await
    }

    async fn receive(&mut self, destination: &mut R) -> ProtocolResult<()> {
        stream::core::receive_encrypted(&mut self.stream, destination, &mut self.recv_state).await
    }

    async fn terminate(&mut self) -> ProtocolResult<()> {
        self.stream.shutdown().await?;
        Ok(())
    }

    fn split(self) -> (Self::Sender, Self::Receiver) {
        let (r, w) = self.stream.into_split();
        (
            IPLinkSecureSender::new(w, self.send_state),
            IPLinkSecureReceiver::new(r, self.recv_state),
        )
    }
}

pub struct IPLinkInitiator<S, R> {
    stream: TcpStream,
    _s: PhantomData<S>,
    _r: PhantomData<R>,
}

impl<S, R> IPLinkInitiator<S, R> {
    #[inline]
    const fn from(stream: TcpStream) -> Self {
        Self {
            stream,
            _s: PhantomData,
            _r: PhantomData,
        }
    }
}

impl<S, R> TransportInitiator<S, R> for IPLinkInitiator<S, R>
where
    S: IOSegment + Send + Sync,
    R: IOSegment + Send + Sync,
{
    type Transport = IPLink<S, R>;

    async fn initiate(mut self) -> ProtocolResult<IPLink<S, R>> {
        let specs = negotiation::read_frame(&mut self.stream).await?;

        // TODO: Hardcoded because config are not accepted currently.
        if specs.abi != 1 {
            negotiation::reject(&mut self.stream).await?;
            return Err(ProtocolError::error(ErrKind::SpecsMismatch));
        }

        negotiation::confirm(&mut self.stream).await?;

        Ok(IPLink::from(self.stream))
    }
}

pub struct IPLinkServer<S, R> {
    listener: TcpListener,
    _s: PhantomData<S>,
    _r: PhantomData<R>,
}

impl<S, R> TransportServer<S, R> for IPLinkServer<S, R>
where
    S: IOSegment + Send + Sync,
    R: IOSegment + Send + Sync,
{
    type Transport = IPLink<S, R>;

    type Initiator = IPLinkInitiator<S, R>;

    type Parameter = SocketAddr;

    type ID = SocketAddr;

    async fn create(id: &SocketAddr) -> ProtocolResult<Self>
    where
        Self: Sized,
    {
        let listener = TcpListener::bind(id).await?;

        Ok(Self {
            listener,
            _s: PhantomData,
            _r: PhantomData,
        })
    }

    async fn accept(&self) -> ProtocolResult<(IPLinkInitiator<S, R>, SocketAddr)> {
        let (stream, addr) = self.listener.accept().await?;
        Ok((IPLinkInitiator::from(stream), addr))
    }

    fn id(&self) -> ProtocolResult<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    async fn terminate(&mut self) -> ProtocolResult<()> {
        Err(ProtocolError::error(ErrKind::Unimplemented))
    }
}

pub struct IPLinkSecureInitiator<S, R> {
    stream: TcpStream,
    _s: PhantomData<S>,
    _r: PhantomData<R>,
}

impl<S, R> IPLinkSecureInitiator<S, R> {
    #[inline]
    const fn from(stream: TcpStream) -> Self {
        Self {
            stream,
            _s: PhantomData,
            _r: PhantomData,
        }
    }
}

impl<S, R> TransportInitiator<S, R> for IPLinkSecureInitiator<S, R>
where
    S: IOSegment + Send + Sync,
    R: IOSegment + Send + Sync,
{
    type Transport = IPLinkSecure<S, R>;

    async fn initiate(mut self) -> ProtocolResult<IPLinkSecure<S, R>> {
        let specs = negotiation::read_frame(&mut self.stream).await?;

        // TODO: Hardcoded because config are not accepted currently.
        if specs.abi != 1 {
            negotiation::reject(&mut self.stream).await?;
            return Err(ProtocolError::error(ErrKind::SpecsMismatch));
        }

        negotiation::confirm(&mut self.stream).await?;

        let (send_state, recv_state) = negotiation::accept_key_exchange(&mut self.stream).await?;

        Ok(IPLinkSecure::from(self.stream, send_state, recv_state))
    }
}

pub struct IPLinkSecureServer<S, R> {
    listener: TcpListener,
    _s: PhantomData<S>,
    _r: PhantomData<R>,
}

impl<S, R> TransportServer<S, R> for IPLinkSecureServer<S, R>
where
    S: IOSegment + Send + Sync,
    R: IOSegment + Send + Sync,
{
    type Transport = IPLinkSecure<S, R>;

    type Initiator = IPLinkSecureInitiator<S, R>;

    type Parameter = SocketAddr;

    type ID = SocketAddr;

    async fn create(id: &SocketAddr) -> ProtocolResult<Self>
    where
        Self: Sized,
    {
        let instance = TcpListener::bind(id).await?;

        Ok(Self {
            listener: instance,
            _s: PhantomData,
            _r: PhantomData,
        })
    }

    async fn accept(&self) -> ProtocolResult<(IPLinkSecureInitiator<S, R>, SocketAddr)> {
        let (stream, addr) = self.listener.accept().await?;
        Ok((IPLinkSecureInitiator::from(stream), addr))
    }

    fn id(&self) -> ProtocolResult<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    async fn terminate(&mut self) -> ProtocolResult<()> {
        Err(ProtocolError::error(ErrKind::Unimplemented))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::next::mem::IOPool;
    use crate::next::mem::IOPoolSegment;

    #[tokio::test]
    async fn test_ip_link_send_receive() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let server = IPLinkServer::<IOPoolSegment, IOPoolSegment>::create(&addr)
            .await
            .unwrap();

        let server_addr = server.listener.local_addr().unwrap();
        let pool = IOPool::new(4, 1024);

        // Server side.
        let server_pool = pool.clone();
        let server_handle = tokio::spawn(async move {
            let (initiator, _) = server.accept().await.unwrap();
            let mut link = initiator.initiate().await.unwrap();

            // Receive message.
            let mut data = server_pool.acquire().unwrap();
            link.receive(&mut data).await.unwrap();
            assert_eq!(data.as_slice(), b"hello from client");

            // Send response.
            let mut response = server_pool.acquire().unwrap();
            response.write(b"hello from server");
            link.send(&mut response).await.unwrap();
        });

        // Client side.
        let mut link = IPLink::<IOPoolSegment, IOPoolSegment>::connect(&server_addr)
            .await
            .unwrap();

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
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = IPLinkSecureServer::<IOPoolSegment, IOPoolSegment>::create(&addr)
            .await
            .unwrap();

        let server_addr = server.listener.local_addr().unwrap();

        let pool = IOPool::new(4, 1024);

        // Server side.
        let server_pool = pool.clone();
        let server_handle = tokio::spawn(async move {
            let (initiator, _) = server.accept().await.unwrap();
            let mut link = initiator.initiate().await.unwrap();

            // Receive message.
            let mut data = server_pool.acquire().unwrap();
            link.receive(&mut data).await.unwrap();
            assert_eq!(data.as_slice(), b"hello secure client");

            // Send response.
            let mut response = server_pool.acquire().unwrap();
            response.write(b"hello secure server");
            link.send(&mut response).await.unwrap();
        });

        // Client side.
        let mut link = IPLinkSecure::<IOPoolSegment, IOPoolSegment>::connect(&server_addr)
            .await
            .unwrap();

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
