use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::next::error::ProtocolResult;
use crate::next::mem::IOSegment;

/// The sender of the transport-component.
pub trait TransportSender<S: IOSegment> {
    fn send(&mut self, source: &mut S) -> impl Future<Output = ProtocolResult<()>> + Send;
    fn terminate(&mut self) -> impl Future<Output = ProtocolResult<()>> + Send;
}

/// The receiver of the transport-component.
pub trait TransportReceiver<R: IOSegment> {
    fn receive(&mut self, destination: &mut R) -> impl Future<Output = ProtocolResult<()>> + Send;
}

/// A type that can act as transport layer.
///
/// This trait is a continuity to the effort of isolating memory from the actual logic of transferring data.
///
/// Types that implement this trait shall ask for their memory from a specified allocator or `provider`.
///
/// Memory providers shall provide properly aligned memory as types conforming to `IOSegment` trait.
pub trait Transport<S: IOSegment, R: IOSegment>: Sized {
    /// TODO: Add associated error-type?

    type Parameters;

    type Sender: TransportSender<S>;

    type Receiver: TransportReceiver<R>;

    fn connect(parameters: &Self::Parameters) -> impl Future<Output = ProtocolResult<Self>> + Send;

    fn send(&mut self, source: &mut S) -> impl Future<Output = ProtocolResult<()>> + Send;

    fn receive(&mut self, destination: &mut R) -> impl Future<Output = ProtocolResult<()>> + Send;

    fn terminate(&mut self) -> impl Future<Output = ProtocolResult<()>> + Send;

    fn split(self) -> (Self::Sender, Self::Receiver);
}

/// A type that establishes a connection after being accepted by the transport-server.
pub trait TransportInitiator<S: IOSegment, R: IOSegment>: Sized {
    type Transport: Transport<S, R>;

    fn initiate(self) -> impl Future<Output = ProtocolResult<Self::Transport>> + Send;
}

/// A type that serves transport-components.
///
/// This trait can be implemented by transport-components that support multi-endpoint connections.
///
/// Types implementing this trait are used by multi-client server-implementations.
pub trait TransportServer<S: IOSegment, R: IOSegment>: Sized {
    type Transport: Transport<S, R>;

    type Initiator: TransportInitiator<S, R, Transport = Self::Transport>;

    type Parameter;

    type ID;

    fn create(parameter: &Self::Parameter) -> impl Future<Output = ProtocolResult<Self>>;

    fn accept(&self) -> impl Future<Output = ProtocolResult<(Self::Initiator, Self::ID)>> + Send;

    fn id(&self) -> ProtocolResult<Self::ID>;

    fn terminate(&mut self) -> impl Future<Output = ProtocolResult<()>> + Send;
}

pub trait BytesSender {
    fn send_bytes(&mut self, source: &[u8]) -> impl Future<Output = ProtocolResult<()>> + Send;
}

impl<T> BytesSender for T
where
    T: AsyncWriteExt + Send + Unpin,
{
    async fn send_bytes(&mut self, source: &[u8]) -> ProtocolResult<()> {
        self.write_all(source).await?;
        Ok(())
    }
}

pub trait BytesReceiver {
    fn receive_bytes(
        &mut self,
        destination: &mut [u8],
    ) -> impl Future<Output = ProtocolResult<()>> + Send;
}

impl<T> BytesReceiver for T
where
    T: AsyncReadExt + Send + Unpin,
{
    async fn receive_bytes(&mut self, destination: &mut [u8]) -> ProtocolResult<()> {
        // TODO:
        // Passing "MaybeUninit" makes it safer, clear and more efficient.
        // Problem: "MaybeUninit" in arrays will transform the codebase into "casting-spaghetti".
        self.read_exact(destination).await?;
        Ok(())
    }
}

pub trait BytesTransport: BytesSender + BytesReceiver {}
impl<T> BytesTransport for T where T: BytesSender + BytesReceiver {}
