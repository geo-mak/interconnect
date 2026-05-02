use core::time::Duration;

use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;

use crate::coop::traits::{ControlHandle, TaskServer, TimeInstant, Timer};
use crate::error::ProtocolError;

impl TimeInstant for tokio::time::Instant {
    #[inline]
    fn now() -> Self {
        Instant::now()
    }

    #[inline]
    fn duration_since(&self, earlier: Self) -> Duration {
        self.duration_since(earlier)
    }
}

pub struct TokioTimer;

impl Timer for TokioTimer {
    type TimeInstant = tokio::time::Instant;

    #[inline]
    async fn sleep(duration: Duration) {
        tokio::time::sleep(duration).await
    }

    #[inline]
    async fn timeout<F, T>(duration: std::time::Duration, future: F) -> Result<T, ProtocolError>
    where
        F: Future<Output = T> + Send,
    {
        Ok(tokio::time::timeout(duration, future).await?)
    }

    #[inline]
    async fn timeout_at<F, T>(deadline: Self::TimeInstant, future: F) -> Result<T, ProtocolError>
    where
        F: Future<Output = T>,
    {
        Ok(tokio::time::timeout_at(deadline, future).await?)
    }
}

impl<T> ControlHandle<T> for JoinHandle<T>
where
    T: Send,
{
    type Error = JoinError;

    #[inline]
    fn abort(&self) {
        self.abort();
    }

    #[inline]
    async fn result(self) -> Result<T, JoinError> {
        self.await
    }
}

#[derive(Clone)]
pub struct TokioServer;

impl TaskServer for TokioServer {
    type ControlHandle<T: Send + 'static> = JoinHandle<T>;

    type Timer = TokioTimer;

    #[inline]
    fn create<F>(&self, future: F) -> Self::ControlHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(future)
    }
}
