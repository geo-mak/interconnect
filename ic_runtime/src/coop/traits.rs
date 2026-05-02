use core::future::Future;
use core::time::Duration;

use crate::error::ProtocolError;

pub trait TimeInstant: Copy {
    fn now() -> Self;
    fn duration_since(&self, earlier: Self) -> Duration;
}

pub trait Timer {
    type TimeInstant: TimeInstant;

    fn sleep(duration: Duration) -> impl Future<Output = ()> + Send;

    fn timeout<F, T>(
        duration: Duration,
        future: F,
    ) -> impl Future<Output = Result<T, ProtocolError>> + Send
    where
        F: Future<Output = T> + Send;

    fn timeout_at<F, T>(
        deadline: Self::TimeInstant,
        future: F,
    ) -> impl Future<Output = Result<T, ProtocolError>> + Send
    where
        F: Future<Output = T> + Send;
}

pub trait ControlHandle<T: Send> {
    type Error;

    fn abort(&self);
    fn result(self) -> impl Future<Output = Result<T, Self::Error>> + Send;
}

pub trait TaskServer {
    type ControlHandle<T: Send + 'static>: ControlHandle<T>;
    type Timer: Timer;

    fn create<F>(&self, future: F) -> Self::ControlHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
}
