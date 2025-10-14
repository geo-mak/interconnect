use std::cell::UnsafeCell;
use std::io::Write;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{
    AtomicU64, AtomicUsize, Ordering,
    Ordering::{AcqRel, Acquire, Release},
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
        let current = self.state.fetch_add(2, Ordering::Acquire);
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
        let current = self.state.fetch_add(2, Ordering::Acquire);
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
        let current = self.state.fetch_add(2, Ordering::Acquire);
        if (current & 1) != 0 {
            self.release();
            return false;
        }
        true
    }

    #[inline(always)]
    pub(crate) fn release(&self) {
        // If last state was open and has exactly one last lock.
        if self.state.fetch_sub(2, Ordering::Release) == 3 {
            self.waiter.wake();
        }
    }

    /// Sets the open flag, preventing new locks from being created.
    #[inline(always)]
    pub fn open(&self) {
        self.state.fetch_or(1, Ordering::AcqRel);
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
        (self.state.load(Ordering::Acquire) & 1) != 0
    }

    /// Returns the current count of held locks.
    #[inline(always)]
    pub fn count(&self) -> usize {
        self.state.load(Ordering::Acquire) >> 1
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
        if self.latch.state.load(Ordering::Acquire) == 1 {
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

/// A multi-producer **single-consumer** ring buffer for variable-length byte-messages.
///
/// This buffer acts as a bounded communication channel, where each message is written framed.
///
/// The bounded implementation means that it will block writing when it reaches its full capacity.
pub struct IORing {
    data: Box<[u8]>,
    published: Box<[AtomicU64]>,
    cap: usize,
    mask: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
}

impl IORing {
    /// Creates new `IORing` with the specified capacity.
    ///
    /// Capacity must be power of 2 and >= 8.
    ///
    /// Each message is written after 4-bytes (u32 little-endian) length-prefix.
    pub fn new(cap: usize) -> Self {
        assert!(cap.is_power_of_two());
        assert!(cap >= 8);

        let data = vec![0u8; cap].into_boxed_slice();

        // 1 bit per message.
        let pub_len = (cap + 63) / 64;
        let published = (0..pub_len)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            data,
            published,
            cap,
            mask: cap - 1,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    /// Tries to write a message.
    ///
    /// Each message is written after 4-bytes (u32 little-endian) length-prefix.
    ///
    /// Returns `true` if writing was successful.
    ///
    /// Returns `false` if the frame is larger than the available capacity.
    pub fn try_write(&self, msg: &[u8]) -> bool {
        let frame = 4 + msg.len();
        if frame > self.cap {
            return false;
        }

        loop {
            let head = self.head.load(Ordering::Relaxed);
            let tail = self.tail.load(Ordering::Acquire);
            let used = head.wrapping_sub(tail);
            if used + frame > self.cap {
                return false;
            }

            let new_head = head.wrapping_add(frame);
            if self
                .head
                .compare_exchange_weak(head, new_head, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                unsafe { self.write_publish(head, msg) };
                return true;
            }
        }
    }

    unsafe fn write_publish(&self, pos: usize, msg: &[u8]) {
        let mask = self.mask;
        let len = msg.len();
        let ptr = self.data.as_ptr().cast_mut();

        // Write.
        unsafe {
            // Payload.
            let msg_pos = pos + 4;
            for i in 0..len {
                ptr.add((msg_pos + i) & mask).write(msg[i]);
            }
            // Header.
            let msg_len = (len as u32).to_le_bytes();
            for i in 0..4 {
                ptr.add((pos + i) & mask).write(msg_len[i]);
            }
        }

        // Publish.
        let msg_idx = (pos >> 6) & (self.published.len() - 1);
        let msg_flag = 1u64 << (pos & 63);
        self.published[msg_idx].fetch_or(msg_flag, Ordering::Release);
    }

    /// Tries to read a published message.
    ///
    /// Returns the message size on success.
    /// Returning `0` means there is no published message currently.
    ///
    /// Returns I/O error on failure to read into `dst`.
    pub fn read_next<W: Write>(&self, dst: &mut W) -> io::Result<usize> {
        let tail = self.tail.load(Ordering::Relaxed);

        // Check flag.
        let msg_idx = (tail >> 6) & (self.published.len() - 1);
        let msg_flag = 1u64 << (tail & 63);
        if self.published[msg_idx].load(Ordering::Acquire) & msg_flag == 0 {
            return Ok(0);
        }

        let buf = &*self.data;
        let cap = self.cap;
        let mask = self.mask;
        let pos = tail & mask;

        // Read header.
        let mut len_bytes = [0u8; 4];
        for i in 0..4 {
            len_bytes[i] = buf[(pos + i) & mask];
        }

        let msg_len = u32::from_le_bytes(len_bytes) as usize;
        if msg_len == 0 {
            return Ok(0);
        }

        // Read payload (may wrap).
        let payload_pos = (pos + 4) & mask;
        let some = std::cmp::min(msg_len, cap - payload_pos);
        dst.write_all(&buf[payload_pos..payload_pos + some])?;
        if some < msg_len {
            let rem = msg_len - some;
            dst.write_all(&buf[0..rem])?;
        }

        // Clear flag.
        self.published[msg_idx].fetch_and(!msg_flag, Ordering::Release);

        // Advance tail.
        self.tail
            .store(tail.wrapping_add(4 + msg_len), Ordering::Release);

        Ok(msg_len)
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
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn test_io_ring_read_empty() {
        let ring = IORing::new(16);
        let mut dst = Vec::new();
        let n = ring.read_next(&mut dst).unwrap();
        assert_eq!(n, 0);
        assert!(dst.is_empty());
    }

    #[test]
    fn test_io_ring_no_space() {
        let ring = IORing::new(8);
        let msg = b"12345678";
        assert!(!ring.try_write(msg));
    }

    #[test]
    fn test_io_ring_read_write() {
        let ring = IORing::new(32);
        let msgs = [b"Alpha", b"Betaa", b"Gamma"];
        for m in msgs {
            assert!(ring.try_write(m));
        }
        let mut dst = [0u8; 15];
        let mut pos = 0;
        for _ in 0..3 {
            let n = ring.read_next(&mut dst[pos..].as_mut()).unwrap();
            assert_eq!(n, 5);
            pos += 5;
        }
        assert_eq!(&dst, b"AlphaBetaaGamma");
    }

    #[test]
    fn test_io_ring_wrapping_cycles() {
        let ring = IORing::new(64);
        let mut dst = Vec::new();

        let frame_size = 4 + 2;
        let mut must_head = 0;
        let mut must_tail = 0;

        for i in 0u16..4096 {
            let head = ring.head.load(Ordering::Relaxed);
            let tail = ring.tail.load(Ordering::Relaxed);

            assert_eq!(
                head & ring.mask,
                must_head & ring.mask,
                "Head mismatch at iter {i}"
            );
            assert_eq!(
                tail & ring.mask,
                must_tail & ring.mask,
                "Tail mismatch at iter {i}"
            );

            let success = ring.try_write(&(i + 1).to_le_bytes());
            assert!(success, "Failed to write iter {i}");

            let n = ring.read_next(&mut dst).unwrap();
            assert_eq!(n, 2, "Expected 2 bytes, got {n}");

            must_head = must_head.wrapping_add(frame_size);
            must_tail = must_tail.wrapping_add(frame_size);
        }

        let mut iter = 0;
        for msg in dst.chunks(2) {
            iter += 1;
            let mut bytes = [0u8; 2];
            bytes.copy_from_slice(msg);
            let num = u16::from_le_bytes(bytes);
            assert_eq!(num, iter, "Publishing out of order at iter {iter}");
        }
    }

    #[test]
    fn test_io_ring_data_race() {
        let ring = Arc::new(IORing::new(1024));
        let barrier = Arc::new(Barrier::new(3));

        let ring_1 = ring.clone();
        let barrier_1 = barrier.clone();
        let t1 = thread::spawn(move || {
            barrier_1.wait();
            for _ in 0..30 {
                assert!(ring_1.try_write("thread1".as_bytes()));
            }
        });

        let ring_2 = ring.clone();
        let barrier_2 = barrier.clone();
        let t2 = thread::spawn(move || {
            barrier_2.wait();
            for _ in 0..30 {
                assert!(ring_2.try_write("thread2".as_bytes()))
            }
        });

        let ring_3 = ring.clone();
        let barrier_3 = barrier.clone();
        let t3 = thread::spawn(move || {
            barrier_3.wait();
            for _ in 0..30 {
                assert!(ring_3.try_write("thread3".as_bytes()))
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();

        let mut dst = [0u8; 630];
        let mut pos = 0;
        for _ in 0..90 {
            ring.read_next(&mut dst[pos..].as_mut()).unwrap();
            pos += 7;
        }

        for chunk in dst.chunks(7) {
            let s = std::str::from_utf8(chunk).unwrap();
            assert!(s == "thread1" || s == "thread2" || s == "thread3");
        }
    }
}
