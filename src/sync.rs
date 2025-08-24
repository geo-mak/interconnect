use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::Poll,
};

use futures::task::AtomicWaker;

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
    active: AtomicUsize,
    release: AtomicBool,
    waker: AtomicWaker,
}

impl ReleaseBarrier {
    pub const fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            release: AtomicBool::new(false),
            waker: AtomicWaker::new(),
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

    fn discard(&self) {
        if self.active.fetch_sub(1, Ordering::Release) == 1 && self.release.load(Ordering::Acquire)
        {
            self.waker.wake();
        }
    }

    /// Triggers release and registers the calling context.
    ///
    /// Internal waker is overwritten by new calls,
    /// so only the last caller will be registered.
    ///
    /// Creating new locks after this call will fail,
    /// and protected scopes will be awaited until they finish.
    pub async fn release(&self) {
        self.release.store(true, Ordering::Release);
        futures::future::poll_fn(|cx| {
            if self.active.load(Ordering::Acquire) == 0 {
                return Poll::Ready(());
            }
            self.waker.register(cx.waker());
            Poll::Pending
        })
        .await;
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
