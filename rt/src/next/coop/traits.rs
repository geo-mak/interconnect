use core::future::Future;
use core::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::next::error::{ProtocolError, ProtocolResult};

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

pub trait TimeInstant: Copy {
    fn now() -> Self;
    fn duration_since(&self, earlier: Self) -> Duration;
}

pub trait Timer {
    type TimeInstant: TimeInstant;

    fn sleep(duration: Duration) -> impl Future<Output = ()> + Send;

    fn timeout<F, T>(duration: Duration, future: F) -> impl Future<Output = Result<T, ProtocolError>> + Send
    where F: Future<Output = T> + Send;

    fn timeout_at<F, T>(deadline: Self::TimeInstant, future: F) -> impl Future<Output = Result<T, ProtocolError>> + Send
    where F: Future<Output = T> + Send;
}

pub trait ControlHandle<T: Send> {
    type Error;
    
    fn abort(&self);
    fn result(self) -> impl Future<Output = Result<T, Self::Error>> + Send;
}

pub trait Executor {
    type ControlHandle<T: Send + 'static>: ControlHandle<T>;
    type Timer: Timer;

    fn spawn<F>(&self, future: F) -> Self::ControlHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
}
