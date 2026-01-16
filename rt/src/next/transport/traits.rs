use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::next::error::ProtocolResult;
use crate::next::transport::io::IOSegment;

/// The send of a particular transport component.
pub trait TransportSender<S: IOSegment> {
    fn send(&mut self, source: &mut S) -> impl Future<Output = ProtocolResult<()>> + Send;
    fn terminate(&mut self) -> impl Future<Output = ProtocolResult<()>> + Send;
}

/// The receiver of a particular transport component.
pub trait TransportReceiver<S: IOSegment> {
    fn receive(&mut self, destination: &mut S) -> impl Future<Output = ProtocolResult<()>> + Send;
}

/// A type that can act as transport layer.
///
/// This trait is a continuity to the effort of isolating memory from the actual logic of transferring data.
///
/// Types that implement this trait shall ask for their memory from a specified allocator or `provider`.
///
/// Memory providers shall provide properly aligned memory as types conforming to `IOSegment` trait.
pub trait Transport<S: IOSegment> {
    /// TODO: Add associated error-type?

    type Parameters;

    type Sender: TransportSender<S>;

    type Receiver: TransportReceiver<S>;

    fn connect(parameters: &Self::Parameters) -> impl Future<Output = ProtocolResult<Self>> + Send
    where
        Self: Sized;

    fn send(&mut self, source: &mut S) -> impl Future<Output = ProtocolResult<()>> + Send;

    fn receive(&mut self, destination: &mut S) -> impl Future<Output = ProtocolResult<()>> + Send;

    fn terminate(&mut self) -> impl Future<Output = ProtocolResult<()>> + Send;

    fn split(self) -> (Self::Sender, Self::Receiver);
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
        self.read_exact(destination).await?;
        Ok(())
    }
}

pub trait BytesTransport: BytesSender + BytesReceiver {}
impl<T> BytesTransport for T where T: BytesSender + BytesReceiver {}
