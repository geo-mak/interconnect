use core::time::Duration;

use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;

use crate::concurrency::server::traits::{Task, TaskServer, TimeInstant, Timer};
use crate::error::ICError;

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
    async fn timeout<F, T>(duration: std::time::Duration, future: F) -> Result<T, ICError>
    where
        F: Future<Output = T> + Send,
    {
        Ok(tokio::time::timeout(duration, future).await?)
    }

    #[inline]
    async fn timeout_at<F, T>(deadline: Self::TimeInstant, future: F) -> Result<T, ICError>
    where
        F: Future<Output = T>,
    {
        Ok(tokio::time::timeout_at(deadline, future).await?)
    }
}

pub struct TokioTask<T>(JoinHandle<T>);

impl<T> Task<T> for TokioTask<T> {
    type Error = JoinError;

    #[inline]
    fn cancel(&self) {
        self.0.abort();
    }

    #[inline]
    async fn result(self) -> Result<T, JoinError> {
        self.0.await
    }
}

#[derive(Clone)]
pub struct TokioServer;

impl TaskServer for TokioServer {
    type Task<T> = TokioTask<T>;

    type Timer = TokioTimer;

    #[inline]
    fn create<F>(&self, future: F) -> Self::Task<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        TokioTask(tokio::task::spawn(future))
    }

    #[inline]
    fn create_dedicated<F, R>(&self, f: F) -> Self::Task<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        TokioTask(tokio::task::spawn_blocking(f))
    }
}
