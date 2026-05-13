use core::future::Future;
use core::time::Duration;

use crate::error::ICError;

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
    ) -> impl Future<Output = Result<T, ICError>> + Send
    where
        F: Future<Output = T> + Send;

    fn timeout_at<F, T>(
        deadline: Self::TimeInstant,
        future: F,
    ) -> impl Future<Output = Result<T, ICError>> + Send
    where
        F: Future<Output = T> + Send;
}

pub trait Task<T> {
    type Error;

    /// Sends cancellation signal to scheduler.
    ///
    /// Cancellation might not have an immediate effect, it may take effect after the task
    /// yields control to the scheduler.
    ///
    /// **Note**: Tasks created with dedicated thread **can't** be canceled by the scheduler.
    fn cancel(&self);

    /// Returns the result of the task **after** it finishes
    /// either normally or as a consequence of aborting.
    fn result(self) -> impl Future<Output = Result<T, Self::Error>>;
}

pub trait TaskServer {
    type Task<T>: Task<T>;

    type Timer: Timer;

    /// Creates a new task and **schedules** it immediately.
    ///
    /// The task might end up being running immediately.
    fn create<F>(&self, future: F) -> Self::Task<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    /// Creates a new non-cooperative task and **schedules** it immediately.
    ///
    /// The task will run on a dedicated thread.
    fn create_dedicated<F, R>(&self, f: F) -> Self::Task<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;
}
