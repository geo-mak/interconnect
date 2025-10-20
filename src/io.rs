use std::cell::UnsafeCell;
use std::io::{self, Write};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU8, AtomicUsize};

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
    unsafe fn pub_ref(&self) -> IOSegment<'_> {
        IOSegment { seg: self }
    }

    #[inline(always)]
    unsafe fn buffer(&self) -> &mut Vec<u8> {
        unsafe { &mut *self.data.get() }
    }

    #[inline(always)]
    unsafe fn set_published(&self) {
        self.state.store(SEG_PUBLISHED, Release);
    }

    #[inline(always)]
    unsafe fn set_discarded(&self) {
        self.state.store(SEG_DISCARDED, Release);
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
        // If publish is never called, it must be discarded
        // to prevent deadlocking the ring.
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
    /// Returns `None` if all segments are currently in use.
    pub(crate) fn acquire(&self) -> Option<IOSegment<'_>> {
        loop {
            let head = self.head.load(Relaxed);
            let tail = self.tail.load(Acquire);

            // TODO: Cycles policy. Right now the two pointers are monotonic unbounded.
            let used = head.wrapping_sub(tail);
            if used >= self.segments.len() {
                // Discarded is treated as published.
                // For faster dynamics, consumer must be invoked to make segments free.
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
                    tail_seg.state.store(SEG_NONE, Relaxed);
                    self.tail.store(tail.wrapping_add(1), Release);
                    return Some(Ok(buf.len()));
                }
                SEG_NONE => return None,
                SEG_DISCARDED => {
                    tail_seg.state.store(SEG_NONE, Relaxed);
                    self.tail.store(tail.wrapping_add(1), Release);
                }
                _ => unreachable!(),
            }
        }
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
        let ring = IORing::new(2, 0);
        // Total: 2

        // Unpublished. Left 1.
        {
            let _ = ring.acquire().unwrap();
        }

        // Published. Left 0.
        let seg_2 = ring.acquire().unwrap();
        seg_2.publish();

        // Must fail.
        assert!(ring.acquire().is_none());

        // Receiving + recycling.
        // Published consumed. Discarded recycled.
        while let Some(Ok(_)) = ring.receive_into(&mut [0u8; 0].as_mut()) {}

        // All clear.
        let seg_4 = ring.acquire();
        assert!(seg_4.is_some());

        let seg_5 = ring.acquire();
        assert!(seg_5.is_some());
    }
}
