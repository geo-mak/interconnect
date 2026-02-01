use core::marker::PhantomData;

use tokio::io::AsyncWriteExt;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf, SocketAddr};
use tokio::net::{UnixListener, UnixStream};

use crate::next::error::{ErrKind, ProtocolError, ProtocolResult};
use crate::next::mem::IOSegment;
use crate::next::transport::stream;
use crate::next::transport::stream::specs::{ConnectionSpecs, negotiation};
use crate::next::transport::traits::{
    Transport, TransportInitiator, TransportReceiver, TransportSender, TransportServer,
};

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

impl<T> TransportSender for UnixLinkSender<T>
where
    T: IOSegment + Send + Sync,
{
    type SendSegment = T;

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

impl<T> TransportReceiver for UnixLinkReceiver<T>
where
    T: IOSegment + Send + Sync,
{
    type ReceiveSegment = T;

    async fn receive(&mut self, destination: &mut T) -> ProtocolResult<()> {
        stream::core::receive(&mut self.reader, destination).await
    }
}

pub struct UnixLink<S, R> {
    stream: UnixStream,
    _s: PhantomData<S>,
    _r: PhantomData<R>,
}

impl<S, R> UnixLink<S, R> {
    #[inline]
    const fn from(stream: UnixStream) -> Self {
        Self {
            stream,
            _s: PhantomData,
            _r: PhantomData,
        }
    }
}

impl<S, R> Transport for UnixLink<S, R>
where
    S: IOSegment + Send + Sync,
    R: IOSegment + Send + Sync,
{
    type Parameters = &'static str;

    type SendSegment = S;

    type Sender = UnixLinkSender<S>;

    type ReceiveSegment = R;

    type Receiver = UnixLinkReceiver<R>;

    async fn connect(parameters: &Self::Parameters) -> ProtocolResult<Self> {
        let mut stream = UnixStream::connect(parameters).await?;

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
        (UnixLinkSender::new(w), UnixLinkReceiver::new(r))
    }
}

pub struct UnixLinkInitiator<S, R> {
    stream: UnixStream,
    _s: PhantomData<S>,
    _r: PhantomData<R>,
}

impl<S, R> UnixLinkInitiator<S, R> {
    #[inline]
    pub const fn from(stream: UnixStream) -> Self {
        Self {
            stream,
            _s: PhantomData,
            _r: PhantomData,
        }
    }
}

impl<S, R> TransportInitiator for UnixLinkInitiator<S, R>
where
    S: IOSegment + Send + Sync,
    R: IOSegment + Send + Sync,
{
    type Transport = UnixLink<S, R>;

    async fn initiate(mut self) -> ProtocolResult<UnixLink<S, R>> {
        let specs = negotiation::read_frame(&mut self.stream).await?;
        // TODO: Hardcoded because config are not accepted currently.
        if specs.abi != 1 {
            negotiation::reject(&mut self.stream).await?;
            return Err(ProtocolError::error(ErrKind::SpecsMismatch));
        }
        negotiation::confirm(&mut self.stream).await?;

        Ok(UnixLink::from(self.stream))
    }
}

pub struct UnixLinkServer<S, R> {
    listener: UnixListener,
    _s: PhantomData<S>,
    _r: PhantomData<R>,
}

impl<S, R> TransportServer for UnixLinkServer<S, R>
where
    S: IOSegment + Send + Sync,
    R: IOSegment + Send + Sync,
{
    type Transport = UnixLink<S, R>;

    type Initiator = UnixLinkInitiator<S, R>;

    type Parameter = &'static str;

    type ID = SocketAddr;

    async fn create(parameters: &Self::Parameter) -> ProtocolResult<Self>
    where
        Self: Sized,
    {
        let listener = UnixListener::bind(parameters)?;
        Ok(Self {
            listener,
            _s: PhantomData,
            _r: PhantomData,
        })
    }

    async fn accept(&self) -> ProtocolResult<(UnixLinkInitiator<S, R>, Self::ID)> {
        let (stream, addr) = self.listener.accept().await?;
        Ok((UnixLinkInitiator::from(stream), addr))
    }

    fn id(&self) -> ProtocolResult<Self::ID> {
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
    async fn test_unix_link_send_receive() {
        let path = "/tmp/test_interconnect_uds.sock";
        let _ = std::fs::remove_file(path);
        let server = UnixLinkServer::<IOPoolSegment, IOPoolSegment>::create(&path)
            .await
            .unwrap();

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
        let mut link = UnixLink::<IOPoolSegment, IOPoolSegment>::connect(&path)
            .await
            .unwrap();

        // Send.
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
