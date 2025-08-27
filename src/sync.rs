use std::cell::UnsafeCell;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{
    AtomicBool, AtomicUsize, Ordering,
    Ordering::{AcqRel, Acquire, Release},
};
use std::task::{Context, Poll, Waker};

const SET: usize = 0b01;
const WAIT: usize = 0b00;
const WAKE: usize = 0b10;

/// A concurrent version of task's waker, protected via atomic operations.
///
/// This implementation tracks changes with 3-state:
/// - Set
/// - Wait
/// - Wake
///
/// Inter-thread transitions are tracked with proper memory ordering,
/// ensuring observability and proper responses within internal methods.
///
/// Consider reading methods' documentation carefully,
/// as each provides more information about its safety requirements and tradeoffs.
///
/// The current implementation doesn't track panic state.
pub(crate) struct AtomicWaker {
    state: AtomicUsize,
    waker: UnsafeCell<Option<Waker>>,
}

unsafe impl Send for AtomicWaker {}
unsafe impl Sync for AtomicWaker {}

impl AtomicWaker {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self {
            state: AtomicUsize::new(WAIT),
            waker: UnsafeCell::new(None),
        }
    }

    /// Sets the waker to be notified on calls to `wake`.
    ///
    /// If a waker has been set already and the new one is not identical,
    /// the waker will be overwritten by the new waker.
    ///
    /// If a waker is already set and currently being woken,
    /// the provided waker will be woken as well without storing it.
    ///
    /// In case of race condition to set waker at the same time, the first thread to change the state
    /// will set its waker, other threads will not set.
    pub(crate) fn set(&self, waker: &Waker) {
        let observed = match self.state.compare_exchange(WAIT, SET, Acquire, Acquire) {
            Ok(prev) => prev,
            Err(current) => current,
        };

        match observed {
            WAIT => unsafe {
                match &*self.waker.get() {
                    // Current waker == stored waker => do nothing.
                    Some(existing) if existing.will_wake(waker) => (),
                    // Set or overwrite.
                    _ => *self.waker.get() = Some(waker.clone()),
                }

                // State has been changed concurrently.
                if let Err(current) = self.state.compare_exchange(SET, WAIT, AcqRel, Acquire) {
                    debug_assert_eq!(current, SET | WAKE);
                    let waker = (*self.waker.get()).take().unwrap();
                    self.state.swap(WAIT, AcqRel);
                    waker.wake();
                }
            },
            WAKE => {
                // Just wake this one also please.
                waker.wake_by_ref();
            }
            other => {
                // Do nothing.
                debug_assert!(other == SET || other == SET | WAKE);
            }
        }
    }

    /// Returns the last set waker if any.
    pub(crate) fn take(&self) -> Option<Waker> {
        match self.state.fetch_or(WAKE, AcqRel) {
            WAIT => {
                let waker = unsafe { (*self.waker.get()).take() };
                self.state.fetch_and(!WAKE, Release);
                waker
            }
            other => {
                debug_assert!(other == SET || other == SET | WAKE || other == WAKE);
                None
            }
        }
    }

    /// Wake the last stored waker if any.
    ///
    /// Safe to call concurrently with `set()`.
    #[inline]
    pub(crate) fn wake(&self) {
        if let Some(waker) = self.take() {
            waker.wake();
        }
    }
}

impl Default for AtomicWaker {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for AtomicWaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AtomicWaker")
    }
}

/// A reference-counted "release" barrier.
///
/// This synchronization primitive works according to the concept of `passive consensus`,
/// and has four operations:
///
/// - Attest (`A` operation): Checks if `R` operation has been started, signaling void protection if it `true`.
///
/// - Increment (`I` operation): increments the reference count of active locks.
///
/// - Decrement (`D` operation): decrements the reference count of active locks.
///
/// - Release (`R` operation): Sets the release flag to `true`.
///   This operation is irreversible per instance.
///
/// Each call to `lock` performs `A` operation. On success, `I` operation is performed,
/// and a lock is returned.
///
/// Once a lock is dropped, a `D` operation is performed automatically.
///
/// All attempts to create locks after `release` has been started will fail,
/// and the scheduled `release` future will resolve automatically when the count has reached `0`.
pub struct ReleaseBarrier {
    release: AtomicBool,
    active: AtomicUsize,
    waiter: AtomicWaker,
}

impl ReleaseBarrier {
    pub const fn new() -> Self {
        Self {
            release: AtomicBool::new(false),
            active: AtomicUsize::new(0),
            waiter: AtomicWaker::new(),
        }
    }

    /// Creates a lock valid for the scope.
    ///
    /// Returns `None` if `release` has been started, signaling void protection.
    #[inline]
    pub fn lock(&self) -> Option<ReleaseLock<'_>> {
        // Initial `A` operation.
        if self.release.load(Ordering::Acquire) {
            return None;
        }

        // All atomic operations on a single variable participate in a single total modification order.
        // If that is held true by the implementation, then `Relaxed` ordering here is fine.
        // So if this call succeeds, next call to release() must observe the modification.
        self.active.fetch_add(1, Ordering::Relaxed);

        // If `R` operation has been started concurrently we don't create lock.
        if self.release.load(Ordering::Acquire) {
            self.discard();
            return None;
        }

        // All set.
        Some(ReleaseLock { barrier: self })
    }

    #[inline]
    fn discard(&self) {
        if self.active.fetch_sub(1, Ordering::Release) == 1 && self.release.load(Ordering::Acquire)
        {
            self.waiter.wake();
        }
    }

    /// Triggers release and registers the calling context.
    ///
    /// Creating new locks after this call will fail,
    /// and protected scopes will be awaited until they finish.
    ///
    /// **Note**:
    /// This method is safe for concurrent access in terms of memory safety,
    /// but it is not what it is designed for.
    ///
    /// If releasing is not yet feasible, a new call will reset the internal waker to the waker
    /// of the last **observed** caller, and the previous returned futures will not resolve.
    ///
    /// In case of race condition, only one thread will be successful in registering its waker,
    /// calls from other threads will not be registered at all.
    ///
    /// The `XOR` mutability rule is not enforced to give more flexibility, 
    /// and avoid synchronization overhead of internal APIs, but exposing it
    /// in public APIs requires enforcing `XOR` mutability rule.
    pub fn release(&self) -> ReleaseFuture<'_> {
        self.release.store(true, Ordering::Release);
        ReleaseFuture { barrier: self }
    }
}

pub struct ReleaseFuture<'a> {
    barrier: &'a ReleaseBarrier,
}

impl<'a> Future for ReleaseFuture<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.barrier.active.load(Ordering::Acquire) == 0 {
            return Poll::Ready(());
        }
        self.barrier.waiter.set(cx.waker());
        Poll::Pending
    }
}

/// A lock for a protected scope.
///
/// The lock is released on drop (RAII effect).
pub struct ReleaseLock<'a> {
    barrier: &'a ReleaseBarrier,
}

impl Drop for ReleaseLock<'_> {
    fn drop(&mut self) {
        self.barrier.discard();
    }
}

impl Default for ReleaseBarrier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn test_barrier_core_ops() {
        let barrier = ReleaseBarrier::new();

        let lock1 = barrier.lock().expect("Should acquire lock 1");
        let lock2 = barrier.lock().expect("Should acquire lock 2");
        let lock3 = barrier.lock().expect("Should acquire lock 3");

        assert_eq!(barrier.active.load(Ordering::Acquire), 3);

        // Release should not complete while any guard is held.
        let res = timeout(Duration::from_millis(100), barrier.release()).await;
        assert!(
            res.is_err(),
            "release() should not complete while locks are held"
        );

        // After release is called, no new locks can be acquired.
        assert!(
            barrier.lock().is_none(),
            "No new locks should be acquired after release"
        );

        drop(lock1);
        assert_eq!(barrier.active.load(Ordering::Acquire), 2);

        drop(lock2);
        assert_eq!(barrier.active.load(Ordering::Acquire), 1);

        drop(lock3);
        assert_eq!(barrier.active.load(Ordering::Acquire), 0);

        let res = timeout(Duration::from_millis(100), barrier.release()).await;
        assert!(res.is_ok(), "release() should complete, no locks are held");
    }

    #[tokio::test]
    async fn test_barrier_concurrent_lock_and_release_race() {
        use std::sync::Arc;
        use tokio::sync::Barrier;
        use tokio::time::{Duration, sleep, timeout};

        let r_barrier = Arc::new(ReleaseBarrier::new());
        let barrier = Arc::new(Barrier::new(2));

        let rb_1 = r_barrier.clone();
        let barrier1 = barrier.clone();

        // Task 1: Acquires the lock for a while.
        let t1 = tokio::spawn(async move {
            let guard = rb_1.lock().expect("Task 1 should acquire lock");

            // Synchronize with t2 before sleeping.
            barrier1.wait().await;

            sleep(Duration::from_millis(150)).await;
            drop(guard);
        });

        let rb_2 = r_barrier.clone();
        let barrier2 = barrier.clone();

        // Task 2: Attempts to release while lock is held.
        let t2 = tokio::spawn(async move {
            // Wait for t1 to acquire lock.
            barrier2.wait().await;

            // Must fail, lock must still be held.
            let res = timeout(Duration::from_millis(100), rb_2.release()).await;
            assert!(
                res.is_err(),
                "release() should not complete while lock is held"
            );

            // Must complete, lock must have been dropped.
            let res = timeout(Duration::from_millis(100), rb_2.release()).await;
            assert!(res.is_ok(), "release() should complete, no locks are held");

            // No new locks after release.
            assert!(
                rb_2.lock().is_none(),
                "No new locks should be acquired after release"
            );
        });

        let ((), ()) = tokio::try_join!(t1, t2).expect("Tasks should not panic");
    }
}
