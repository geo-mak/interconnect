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

    /// Sends abort signal to scheduler.
    ///
    /// Abort might not have an immediate effect and
    /// may take effect after the task yield control to the scheduler.
    fn abort(&self);

    /// Returns the result of the task **after** it finishes
    /// either normally or as a consequence of aborting.
    fn result(self) -> impl Future<Output = Result<T, Self::Error>> + Send;
}

pub trait TaskServer {
    type ControlHandle<T: Send + 'static>: ControlHandle<T>;
    type Timer: Timer;

    /// Creates a new task and **schedules** it immediately.
    ///
    /// The task might end up being running immediately.
    fn create<F>(&self, future: F) -> Self::ControlHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
}
