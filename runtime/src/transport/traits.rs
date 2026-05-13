use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::ICResult;

/// The sender of the transport-component.
pub trait TransportSender {
    type SendSegment;

    fn send(&mut self, source: &mut Self::SendSegment)
    -> impl Future<Output = ICResult<()>> + Send;
    fn terminate(&mut self) -> impl Future<Output = ICResult<()>> + Send;
}

/// The receiver of the transport-component.
pub trait TransportReceiver {
    type ReceiveSegment;

    fn receive(
        &mut self,
        destination: &mut Self::ReceiveSegment,
    ) -> impl Future<Output = ICResult<()>> + Send;
}

/// A type that can act as transport layer.
///
/// This trait is a continuity to the effort of isolating memory from the actual logic of transferring data.
///
/// Types that implement this trait shall ask for their memory from a specified allocator or `provider`.
pub trait Transport: Sized {
    // TODO: Add associated error-type?

    type Parameters;

    type SendSegment;

    type Sender: TransportSender<SendSegment = Self::SendSegment>;

    type ReceiveSegment;

    type Receiver: TransportReceiver<ReceiveSegment = Self::ReceiveSegment>;

    fn connect(parameters: &Self::Parameters) -> impl Future<Output = ICResult<Self>> + Send;

    fn send(&mut self, source: &mut Self::SendSegment)
    -> impl Future<Output = ICResult<()>> + Send;

    fn receive(
        &mut self,
        destination: &mut Self::ReceiveSegment,
    ) -> impl Future<Output = ICResult<()>> + Send;

    fn terminate(&mut self) -> impl Future<Output = ICResult<()>> + Send;

    fn split(self) -> (Self::Sender, Self::Receiver);
}

/// A type that establishes a connection after being accepted by the transport-server.
pub trait TransportInitiator: Sized {
    type Transport: Transport;

    fn initiate(self) -> impl Future<Output = ICResult<Self::Transport>> + Send;
}

/// A type that serves transport-components.
///
/// This trait can be implemented by transport-components that support multi-endpoint connections.
///
/// Types implementing this trait are used by multi-client server-implementations.
pub trait TransportServer: Sized {
    type Transport: Transport;

    type Initiator: TransportInitiator<Transport = Self::Transport>;

    type Parameters;

    type Info;

    fn create(parameters: &Self::Parameters) -> impl Future<Output = ICResult<Self>>;

    fn accept(&self) -> impl Future<Output = ICResult<(Self::Initiator, Self::Info)>> + Send;

    fn info(&self) -> ICResult<Self::Info>;

    fn terminate(&mut self) -> impl Future<Output = ICResult<()>> + Send;
}

pub trait BytesSender {
    fn send(&mut self, source: &[u8]) -> impl Future<Output = ICResult<()>> + Send;
}

impl<T> BytesSender for T
where
    T: AsyncWriteExt + Send + Unpin,
{
    async fn send(&mut self, source: &[u8]) -> ICResult<()> {
        self.write_all(source).await?;
        Ok(())
    }
}

pub trait BytesReceiver {
    fn receive(&mut self, destination: &mut [u8]) -> impl Future<Output = ICResult<()>> + Send;
}

impl<T> BytesReceiver for T
where
    T: AsyncReadExt + Send + Unpin,
{
    async fn receive(&mut self, destination: &mut [u8]) -> ICResult<()> {
        // TODO:
        // Passing "MaybeUninit" makes it safer, clear and more efficient.
        // Problem: "MaybeUninit" in arrays will transform the codebase into "casting-spaghetti".
        self.read_exact(destination).await?;
        Ok(())
    }
}

pub trait BytesTransport: BytesSender + BytesReceiver {}
impl<T> BytesTransport for T where T: BytesSender + BytesReceiver {}
