use std::cell::UnsafeCell;
use std::io::Write;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::{
    AtomicUsize,
    Ordering::{AcqRel, Acquire, Relaxed, Release},
};
use std::task::{Context, Poll, Waker};
use std::{fmt, io};

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

    /// Detaches the first node (i=0) from the list if the list.
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
/// This implementation is very lightweight, with user-space synchronization as first option.
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

struct IORingSegment {
    data: UnsafeCell<Vec<u8>>,
    state: AtomicU8,
}

const SEG_NONE: u8 = 0;
const SEG_PUBLISHED: u8 = 1;
const SEG_DISCARDED: u8 = 2;

impl IORingSegment {
    #[inline]
    fn with_capacity(cap: usize) -> Self {
        IORingSegment {
            data: UnsafeCell::new(Vec::with_capacity(cap)),
            state: AtomicU8::new(SEG_NONE),
        }
    }

    #[inline(always)]
    unsafe fn buffer(&self) -> &mut Vec<u8> {
        unsafe { &mut *self.data.get() }
    }

    #[inline(always)]
    unsafe fn set_none(&self) {
        self.state.store(SEG_NONE, Release);
    }

    #[inline(always)]
    unsafe fn set_published(&self) {
        self.state.store(SEG_PUBLISHED, Release);
    }

    #[inline(always)]
    unsafe fn set_discarded(&self) {
        self.state.store(SEG_DISCARDED, Release);
    }

    #[inline(always)]
    unsafe fn pub_ref(&self) -> IOSegment<'_> {
        IOSegment { seg: self }
    }
}

/// An exclusive segment that implements `io::Write`.
///
/// `publish` method must be called to save the data written to the segment.
///
/// If dropped before calling `publish`, the written data will be discarded.
pub(crate) struct IOSegment<'a> {
    seg: &'a IORingSegment,
}

impl<'a> IOSegment<'a> {
    #[inline(always)]
    pub(crate) fn publish(self) {
        unsafe {
            self.seg.set_published();
        }
        std::mem::forget(self);
    }
}

impl<'a> io::Write for IOSegment<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        unsafe { self.seg.buffer().write(buf) }
    }

    /// This call in no-op.
    ///
    /// Call `publish` to save written data.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> Drop for IOSegment<'a> {
    fn drop(&mut self) {
        // If the user never called publish(),
        // it must be discarded to prevent deadlocking the ring.
        unsafe { self.seg.set_discarded() }
    }
}

/// A multi-producer **single-consumer** ring buffer for variable-length byte-messages.
///
/// This buffer acts as a bounded communication channel.
///
/// The bounded implementation means that it will block writing when it reaches its full capacity.
///
/// This structure is a core component, which doesn't included notification component
/// and doesn't observe panic events on both sides, with very basic protection mechanism
/// aimed at ensuring data integrity.
pub(crate) struct IORing {
    segments: Box<[IORingSegment]>,
    mask: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
}

unsafe impl Sync for IORing {}

impl IORing {
    /// Creates new `IORing` with the specified count of dynamic segments.
    ///
    /// Parameters:
    /// - `count`: The count of concurrent segments. Count must be power of 2.
    /// - `seg_size`: The size of each segment in bytes.
    pub(crate) fn new(count: usize, seg_size: usize) -> Self {
        assert!(count.is_power_of_two(), "Count must be power of 2");

        let segments = (0..count)
            .map(|_| IORingSegment::with_capacity(seg_size))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            segments,
            mask: count - 1,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    /// Tries to acquire a segment for writing.
    ///
    /// Returns `None` if there is no segment is free currently.
    pub(crate) fn acquire(&self) -> Option<IOSegment<'_>> {
        loop {
            let head = self.head.load(Relaxed);
            let tail = self.tail.load(Acquire);
            let used = head.wrapping_sub(tail);

            // TODO: Cycles policy.
            if used >= self.segments.len() {
                let tail_idx = tail & self.mask;
                let tail_seg = &self.segments[tail_idx];

                let state = tail_seg.state.load(Acquire);
                if state == SEG_DISCARDED {
                    if tail_seg
                        .state
                        .compare_exchange_weak(SEG_DISCARDED, SEG_NONE, AcqRel, Acquire)
                        .is_ok()
                    {
                        self.tail.store(tail.wrapping_add(1), Release);
                        let seg_ref = unsafe {
                            tail_seg.buffer().clear();
                            tail_seg.pub_ref()
                        };
                        return Some(seg_ref);
                    }
                    continue;
                }
                // Just full.
                return None;
            }

            let seg_idx = head & self.mask;
            let segment = &self.segments[seg_idx];

            // Acquire exclusive segment.
            if self
                .head
                .compare_exchange_weak(head, head.wrapping_add(1), AcqRel, Relaxed)
                .is_ok()
            {
                let seg_ref = unsafe {
                    segment.buffer().clear();
                    segment.pub_ref()
                };
                return Some(seg_ref);
            }
        }
    }

    /// Tries to receive a published message.
    ///
    /// Safety:
    ///
    /// This method must have single consumer at a time, no concurrent calls.
    ///
    /// The `XOR` mutability rule is not enforced to keep the type `mutex-free` at the type level.
    ///
    /// User must guarantee that only one consumer at a time can call this method.
    ///
    /// Returns:
    ///
    /// - None: When there is no published message currently.
    ///
    /// - Some(message size): When a published message has been written successfully to `dst`.
    ///
    /// - Some(I/O error): When writing a published message into `dst` has failed.
    ///   Next call will try to write the same message.
    pub(crate) fn receive_into<W: Write>(&self, dst: &mut W) -> Option<io::Result<usize>> {
        loop {
            let tail = self.tail.load(Relaxed);
            let seg_idx = tail & self.mask;
            let tail_seg = &self.segments[seg_idx];

            match tail_seg.state.load(Acquire) {
                SEG_PUBLISHED => {
                    let buf = unsafe { tail_seg.buffer() };
                    if let Err(e) = dst.write_all(buf) {
                        return Some(Err(e));
                    }
                    unsafe { tail_seg.set_none() };
                    self.tail.store(tail.wrapping_add(1), Release);
                    return Some(Ok(buf.len()));
                }
                SEG_NONE => return None,
                SEG_DISCARDED => {
                    if tail_seg
                        .state
                        .compare_exchange_weak(SEG_DISCARDED, SEG_NONE, AcqRel, Acquire)
                        .is_ok()
                    {
                        self.tail.store(tail.wrapping_add(1), Release);
                    }
                }
                _ => unreachable!(),
            }
        }
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

#[cfg(test)]
mod tests_io_ring {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn test_io_ring_acquire_publish() {
        let ring = IORing::new(2, 16);
        let mut seg = ring.acquire().expect("must get a segment");

        let _ = seg.write(b"ring ");
        let _ = seg.write(b"my ");
        let _ = seg.write(b"bells");

        let mut dst = [0u8; 13];

        let res = ring.receive_into(&mut dst.as_mut());
        assert!(res.is_none());

        seg.publish();

        let res = ring.receive_into(&mut dst.as_mut()).unwrap().unwrap();
        assert_eq!(res, 13);
        assert_eq!(&dst, b"ring my bells");
    }

    #[test]
    fn test_io_ring_read_empty() {
        let ring = IORing::new(1, 0);
        let mut dst = Vec::new();
        let n = ring.receive_into(&mut dst);
        assert!(n.is_none());
        assert!(dst.is_empty());
    }

    #[test]
    fn test_io_ring_no_space() {
        let ring = IORing::new(1, 8);
        let msg = b"12345678";

        let mut seg = ring.acquire().expect("expected a free segment");
        seg.write_all(msg).unwrap();
        seg.publish();

        assert!(ring.acquire().is_none());
    }

    #[test]
    fn test_io_ring_read_write_seq() {
        let ring = IORing::new(4, 5);
        let msgs = [b"Alpha", b"Betaa", b"Gamma"];

        for m in msgs {
            let mut seg = ring.acquire().expect("expected free segment");
            seg.write_all(m).unwrap();
            seg.publish();
        }

        let mut dst = [0u8; 15];
        let mut pos = 0;
        for _ in 0..3 {
            let n = ring
                .receive_into(&mut dst[pos..].as_mut())
                .unwrap()
                .unwrap();
            assert_eq!(n, 5);
            pos += 5;
        }
        assert_eq!(&dst, b"AlphaBetaaGamma");
    }

    #[test]
    fn test_io_ring_wrapping_cycles() {
        let ring = IORing::new(8, 2);
        let mut dst = [0u8; 2];

        for i in 0u16..4096 {
            let mut seg = ring.acquire().expect("acquire failed");
            seg.write_all(&(i + 1).to_le_bytes()).unwrap();
            seg.publish();

            let n = ring.receive_into(&mut dst.as_mut()).unwrap().unwrap();
            assert_eq!(n, 2);

            let num = u16::from_le_bytes(dst);
            assert_eq!(num, i + 1);
        }
    }

    #[test]
    fn test_io_ring_data_race() {
        let ring = Arc::new(IORing::new(128, 7));
        let barrier = Arc::new(Barrier::new(4));

        let ring_1 = ring.clone();
        let barrier_1 = barrier.clone();
        let t1 = thread::spawn(move || {
            barrier_1.wait();
            for _ in 0..30 {
                let mut seg = ring_1.acquire().unwrap();
                seg.write_all("thread1".as_bytes()).unwrap();
                seg.publish();
            }
        });

        let ring_2 = ring.clone();
        let barrier_2 = barrier.clone();
        let t2 = thread::spawn(move || {
            barrier_2.wait();
            for _ in 0..30 {
                let mut seg = ring_2.acquire().unwrap();
                seg.write_all("thread2".as_bytes()).unwrap();
                seg.publish();
            }
        });

        let ring_3 = ring.clone();
        let barrier_3 = barrier.clone();
        let t3 = thread::spawn(move || {
            barrier_3.wait();
            for _ in 0..30 {
                let mut seg = ring_3.acquire().unwrap();
                seg.write_all("thread3".as_bytes()).unwrap();
                seg.publish();
            }
        });

        let ring_4 = ring.clone();
        let barrier_4 = barrier.clone();
        let t4 = thread::spawn(move || {
            barrier_4.wait();
            for _ in 0..30 {
                let mut seg = ring_4.acquire().unwrap();
                seg.write_all("thread4".as_bytes()).unwrap();
                seg.publish();
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();
        t4.join().unwrap();

        let mut counts = [0usize; 4];
        let mut dst = [0u8; 7];

        while let Some(Ok(_)) = ring.receive_into(&mut dst.as_mut()) {
            match std::str::from_utf8(&dst).expect("Unreadable data in the ring") {
                "thread1" => counts[0] += 1,
                "thread2" => counts[1] += 1,
                "thread3" => counts[2] += 1,
                "thread4" => counts[3] += 1,
                other => panic!("Unexpected data: {other}"),
            }
        }

        assert_eq!(counts, [30, 30, 30, 30]);
    }

    #[test]
    fn test_io_ring_discarded_segment() {
        let ring = IORing::new(2, 16);

        {
            let _ = ring.acquire().unwrap();
        }

        let mut seg_2 = ring.acquire().unwrap();
        write!(seg_2, "ok").unwrap();
        seg_2.publish();

        let mut seg_3 = ring.acquire().unwrap();
        write!(seg_3, "was discarded").unwrap();
        seg_3.publish();

        let mut dst = [0u8; 2];
        let res = ring.receive_into(&mut dst.as_mut()).unwrap().unwrap();
        assert_eq!(res, 2);
        assert_eq!(&dst, b"ok");

        let mut dst = [0u8; 13];
        let res = ring.receive_into(&mut dst.as_mut()).unwrap().unwrap();
        assert_eq!(res, 13);
        assert_eq!(&dst, b"was discarded");
    }
}
