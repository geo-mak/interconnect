use std::cell::UnsafeCell;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{
    AtomicUsize,
    Ordering::{AcqRel, Acquire, Release},
};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::{fmt, ptr};

pub const NOOP_WAKER: Waker = {
    const NOOP_RAW_WAKER: RawWaker = {
        const VTABLE: RawWakerVTable =
            RawWakerVTable::new(|_| NOOP_RAW_WAKER, |_| {}, |_| {}, |_| {});
        RawWaker::new(ptr::null(), &VTABLE)
    };
    unsafe { Waker::from_raw(NOOP_RAW_WAKER) }
};

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

const SET: usize = 0b01;
const WAIT: usize = 0b00;
const WAKE: usize = 0b10;

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
        // Whatever.
        let observed = match self.state.compare_exchange(WAIT, SET, Acquire, Acquire) {
            Ok(prev) => prev,
            Err(current) => current,
        };

        match observed {
            WAIT => unsafe {
                match &*self.waker.get() {
                    Some(prev) if prev.will_wake(waker) => (),
                    _ => *self.waker.get() = Some(waker.clone()),
                }

                // If the state transitioned to include the `WAKE` flag,
                // this means that `self.wake()` has been called concurrently,
                if let Err(current) = self.state.compare_exchange(SET, WAIT, AcqRel, Acquire) {
                    // Only reachable if `current` == `SET | WAKE`.
                    debug_assert_eq!(current, SET | WAKE);

                    // Remove this thing and call wake on it as requested.
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

    /// Removes and wakes the stored waker if any.
    ///
    /// Safe to call concurrently with `set()`.
    #[inline]
    pub(crate) fn wake(&self) {
        match self.state.fetch_or(WAKE, AcqRel) {
            WAIT => {
                let data = unsafe { (*self.waker.get()).take() };
                self.state.fetch_and(!WAKE, Release);
                if let Some(waker) = data {
                    waker.wake();
                }
            }
            other => {
                // A concurrent thread is doing `SET` currently.
                // `WAKE` flag has been set, `set()` method will deal with it.
                debug_assert!(other == SET || other == SET | WAKE || other == WAKE);
            }
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

/// A manually triggered "latch" with dynamic count of locks.
///
/// This synchronization primitive works according to the concept of `passive consensus`,
/// and has four operations:
///
/// - Attest (`A` operation): Checks if `O` operation has been started, signaling void protection if it `true`.
///
/// - Increment (`I` operation): increments the reference count of active locks.
///
/// - Decrement (`D` operation): decrements the reference count of active locks.
///
/// - Open (`O` operation): Sets the open flag.
///   This operation is irreversible per instance.
///
/// Each call to `acquire` performs `A` operation. On success, `I` operation is performed,
/// and a lock is returned.
///
/// Once a lock is dropped, a `D` operation is performed automatically.
///
/// All attempts to create locks after `open` has been set will fail,
/// and the scheduled `wait` future will resolve automatically,
/// when the count of active locks has reached `0`.
pub struct DynamicLatch {
    /// Bits array:
    /// Lower bit: 1 is open, 0 is closed.
    /// Higher bits: locks count as usize value.
    state: AtomicUsize,
    waiter: AtomicWaker,
}

impl DynamicLatch {
    pub const fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            waiter: AtomicWaker::new(),
        }
    }

    /// Tries to acquire a lock for the current scope.
    ///
    /// Returns `None` if `open` has been started, signaling void protection.
    #[inline]
    pub fn acquire(&self) -> Option<LatchLock<'_>> {
        // Open is only done once, so we optimize for the likely case.
        // This will make the failure case more expensive.
        let current = self.state.fetch_add(2, Acquire);
        if (current & 1) != 0 {
            self.release();
            return None;
        }
        // All set.
        Some(LatchLock { latch: self })
    }

    /// Tries to acquire an owned lock.
    ///
    /// Returns `None` if `open` has been started.
    pub fn acquire_owned(self: &Arc<Self>) -> Option<OwnedLatchLock> {
        let current = self.state.fetch_add(2, Acquire);
        if (current & 1) != 0 {
            self.release();
            return None;
        }

        Some(OwnedLatchLock {
            latch: self.clone(),
        })
    }

    #[inline]
    pub(crate) fn acquire_manual(&self) -> bool {
        let current = self.state.fetch_add(2, Acquire);
        if (current & 1) != 0 {
            self.release();
            return false;
        }
        true
    }

    #[inline(always)]
    pub(crate) fn release(&self) {
        // If last state was open and has exactly one last lock.
        if self.state.fetch_sub(2, Release) == 3 {
            self.waiter.wake();
        }
    }

    /// Sets the open flag, preventing new locks from being created.
    #[inline(always)]
    pub fn open(&self) {
        self.state.fetch_or(1, AcqRel);
    }

    /// Returns a future that resolves when all locks are released.
    ///
    /// **Note**:
    /// This method is safe for concurrent access in terms of memory safety,
    /// but it is not what it is designed for.
    ///
    /// If opening is not yet feasible, a new call will reset the internal waker to the waker
    /// of the last **observed** caller, and the previous returned futures will not resolve.
    ///
    /// In case of race condition, only one thread will be successful in registering its waker,
    /// calls from other threads will not be registered at all.
    ///
    /// The `XOR` mutability rule is not enforced to give more flexibility,
    /// and avoid synchronization overhead of internal APIs, but exposing it
    /// in public APIs requires enforcing `XOR` mutability rule.
    #[inline(always)]
    pub fn wait(&self) -> WaitFuture<'_> {
        WaitFuture { latch: self }
    }

    /// Returns `true` if the latch is currently open.
    #[inline(always)]
    pub fn is_open(&self) -> bool {
        (self.state.load(Acquire) & 1) != 0
    }

    /// Returns the current count of held locks.
    #[inline(always)]
    pub fn count(&self) -> usize {
        self.state.load(Acquire) >> 1
    }
}

impl Default for DynamicLatch {
    fn default() -> Self {
        Self::new()
    }
}

pub struct WaitFuture<'a> {
    latch: &'a DynamicLatch,
}

impl<'a> Future for WaitFuture<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.latch.state.load(Acquire) == 1 {
            return Poll::Ready(());
        }
        self.latch.waiter.set(cx.waker());
        Poll::Pending
    }
}

/// A lock for a protected scope.
///
/// The lock is released on drop (RAII effect).
pub struct LatchLock<'a> {
    latch: &'a DynamicLatch,
}

impl Drop for LatchLock<'_> {
    fn drop(&mut self) {
        self.latch.release();
    }
}

/// An owned lock for a protected scope, not tied to a lifetime.
///
/// This can be moved freely between threads and tasks.
pub struct OwnedLatchLock {
    latch: Arc<DynamicLatch>,
}

impl Drop for OwnedLatchLock {
    fn drop(&mut self) {
        self.latch.release();
    }
}

/// An intrusive list node.
///
/// Node must have stable address during its lifetime.
#[derive(Debug)]
pub(crate) struct INode<T> {
    prev: Option<NonNull<INode<T>>>,
    next: Option<NonNull<INode<T>>>,
    data: T,
    _pin: PhantomPinned,
}

impl<T> INode<T> {
    pub(crate) const fn new(data: T) -> INode<T> {
        INode::<T> {
            prev: None,
            next: None,
            data,
            _pin: PhantomPinned,
        }
    }
}

impl<T> std::ops::Deref for INode<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}

impl<T> std::ops::DerefMut for INode<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

/// An intrusive linked list.
#[derive(Debug)]
pub(crate) struct IList<T> {
    first: Option<NonNull<INode<T>>>,
    last: Option<NonNull<INode<T>>>,
}

impl<T> IList<T> {
    pub(crate) const fn new() -> Self {
        IList::<T> {
            first: None,
            last: None,
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    /// Attaches the node to the list as the last node (i=n-1).
    #[allow(dead_code)]
    pub(crate) unsafe fn attach_last(&mut self, node: &mut INode<T>) {
        node.prev = self.last;
        node.next = None;

        match self.last {
            Some(mut last) => unsafe { last.as_mut().next = Some(node.into()) },
            None => {
                // list is empty, so first is now this node also.
                self.first = Some(node.into());
            }
        }

        self.last = Some(node.into());
    }

    /// Attaches the node to the list as the first node (i=0).
    pub(crate) unsafe fn attach_first(&mut self, node: &mut INode<T>) {
        node.next = self.first;
        node.prev = None;
        if let Some(mut first) = self.first {
            unsafe { first.as_mut().prev = Some(node.into()) }
        }
        self.first = Some(node.into());
        if self.last.is_none() {
            self.last = Some(node.into());
        }
    }

    /// Detaches the node from the list.
    pub(crate) unsafe fn detach(&mut self, node: &mut INode<T>) -> bool {
        match node.prev {
            None => {
                if self.first != Some(node.into()) {
                    debug_assert!(node.next.is_none());
                    return false;
                }
                self.first = node.next;
            }
            Some(mut prev) => unsafe {
                debug_assert_eq!(prev.as_ref().next, Some(node.into()));
                prev.as_mut().next = node.next;
            },
        }

        match node.next {
            None => {
                debug_assert_eq!(self.last, Some(node.into()));
                self.last = node.prev;
            }
            Some(mut next) => unsafe {
                debug_assert_eq!(next.as_mut().prev, Some(node.into()));
                next.as_mut().prev = node.prev;
            },
        }

        node.next = None;
        node.prev = None;

        true
    }

    /// Detaches the first node (i=0) from the list.
    ///
    /// Returns the detached node if any.
    #[allow(dead_code)]
    pub(crate) fn detach_first(&mut self) -> Option<&mut INode<T>> {
        unsafe {
            let mut current_first = self.first?;
            self.first = current_first.as_mut().next;

            let first = current_first.as_mut();
            match first.next {
                None => {
                    debug_assert_eq!(Some(first.into()), self.last);
                    self.last = None;
                }
                Some(mut next) => {
                    next.as_mut().prev = None;
                }
            }

            first.prev = None;
            first.next = None;
            Some(&mut *(first as *mut INode<T>))
        }
    }

    /// Detaches the last node (i=n-1) from the list.
    ///
    /// Returns the detached node if any.
    #[allow(dead_code)]
    pub(crate) fn detach_last(&mut self) -> Option<&mut INode<T>> {
        unsafe {
            let mut current_last = self.last?;
            self.last = current_last.as_mut().prev;

            let last = current_last.as_mut();
            match last.prev {
                None => {
                    debug_assert_eq!(Some(last.into()), self.first);
                    self.first = None;
                }
                Some(mut prev) => {
                    prev.as_mut().next = None;
                }
            }

            last.prev = None;
            last.next = None;
            Some(&mut *(last as *mut INode<T>))
        }
    }

    /// Iterates over the nodes from first to last (0 -> n-1),
    /// and applies `f` function to each of them after **detaching**.
    pub(crate) fn drain<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut INode<T>),
    {
        let mut current = self.first;
        self.first = None;
        self.last = None;

        while let Some(mut node) = current {
            unsafe {
                let node_ref = node.as_mut();
                current = node_ref.next;

                node_ref.next = None;
                node_ref.prev = None;

                f(node_ref);
            }
        }
    }

    /// Iterates over the nodes from last to first (n-1 -> 0),
    /// and applies `f` function to each of them after **detaching**.
    #[allow(dead_code)]
    pub(crate) fn drain_rev<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut INode<T>),
    {
        let mut current = self.last;
        self.first = None;
        self.last = None;

        while let Some(mut node) = current {
            unsafe {
                let node_ref = node.as_mut();
                current = node_ref.prev;

                node_ref.next = None;
                node_ref.prev = None;

                f(node_ref);
            }
        }
    }
}

pub type ThreadNotifyLock<'a> = parking_lot::lock_api::MutexGuard<'a, parking_lot::RawMutex, ()>;

/// A thread notification construct that can be used as signaling mechanism between threads.
///
/// This implementation is very lightweight, with user-space synchronization.
pub struct ThreadNotify {
    sync: parking_lot::Mutex<()>,
    parker: parking_lot::Condvar,
}

impl ThreadNotify {
    #[inline]
    pub fn new() -> Self {
        Self {
            sync: parking_lot::Mutex::new(()),
            parker: parking_lot::Condvar::new(),
        }
    }

    #[inline]
    pub fn lock(&self) -> ThreadNotifyLock<'_> {
        self.sync.lock()
    }

    #[inline]
    pub fn is_locked(&self) -> bool {
        self.sync.is_locked()
    }

    /// Waits with a pre-acquired lock until a notification is received.
    #[inline]
    pub fn wait(&self, lock: &mut ThreadNotifyLock<'_>) {
        self.parker.wait(lock)
    }

    /// Waits with a pre-acquired lock until a notification is received or timeout occurs.
    ///
    /// Returns `true` in the case of timeout, and `false` otherwise´.
    #[inline]
    pub fn wait_with_timeout(
        &self,
        lock: &mut ThreadNotifyLock<'_>,
        timeout: std::time::Duration,
    ) -> bool {
        self.parker.wait_for(lock, timeout).timed_out()
    }

    /// Notifies a **waiting** thread to wake.
    ///
    /// If there is no waiter, this call does nothing.
    #[inline]
    pub fn notify_one(&self) {
        self.parker.notify_one();
    }

    /// Notifies all **waiting** threads to wake.
    ///
    /// If there is no waiter, this call does nothing.
    #[inline]
    pub fn notify_all(&self) {
        self.parker.notify_all();
    }
}

#[cfg(test)]
mod tests_dynamic_latch {
    use super::*;

    use std::sync::Arc;

    use tokio::sync::Barrier;
    use tokio::time::{Duration, sleep, timeout};

    #[tokio::test]
    async fn test_latch_core_ops() {
        let latch = DynamicLatch::new();

        let lock1 = latch.acquire().expect("Should acquire lock 1");
        let lock2 = latch.acquire().expect("Should acquire lock 2");
        let lock3 = latch.acquire().expect("Should acquire lock 3");

        assert_eq!((latch.count()), 3);

        latch.open();

        // Open should not complete while any guard is held.
        let res = timeout(Duration::from_millis(100), latch.wait()).await;
        assert!(
            res.is_err(),
            "wait() should not complete while locks are held"
        );

        // After open is called, no new locks can be acquired.
        assert!(
            latch.acquire().is_none(),
            "No new locks should be acquired after release"
        );

        drop(lock1);
        assert_eq!((latch.count()), 2);

        drop(lock2);
        assert_eq!((latch.count()), 1);

        drop(lock3);
        assert_eq!((latch.count()), 0);

        let res = timeout(Duration::from_millis(100), latch.wait()).await;
        assert!(res.is_ok(), "open() should complete, no locks are held");
    }

    #[tokio::test]
    async fn test_latch_acquire_release_open() {
        let latch = Arc::new(DynamicLatch::new());
        let barrier = Arc::new(Barrier::new(2));

        let latch_1 = latch.clone();
        let barrier1 = barrier.clone();

        // Task 1: Acquires the lock for a while.
        let t1 = tokio::spawn(async move {
            let guard = latch_1.acquire().expect("Task 1 should acquire lock");

            // Synchronize with t2 before sleeping.
            barrier1.wait().await;

            sleep(Duration::from_millis(150)).await;
            drop(guard);
        });

        let latch_2 = latch.clone();
        let barrier2 = barrier.clone();

        // Task 2: Attempts to open while lock is held.
        let t2 = tokio::spawn(async move {
            // Wait for t1 to acquire lock.
            barrier2.wait().await;

            latch.open();

            // Must fail, lock must still be held.
            let res = timeout(Duration::from_millis(100), latch_2.wait()).await;
            assert!(
                res.is_err(),
                "open() should not complete while lock is held"
            );

            // Must complete, lock must have been dropped.
            let res = timeout(Duration::from_millis(50), latch_2.wait()).await;
            assert!(res.is_ok(), "open() should complete, no locks are held");

            // No new locks after release.
            assert!(
                latch_2.acquire().is_none(),
                "No new locks should be acquired after open"
            );
        });

        let ((), ()) = tokio::try_join!(t1, t2).expect("Tasks should not panic");
    }
}
