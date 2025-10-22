use std::cell::UnsafeCell;
use std::io;
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
/// `flush` method does nothing.
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

/// A published segment that contains data.
///
/// Data can be accessed via `data` method.
///
/// After handling the data, `recycle` method must be called to drive the ring
/// and get the next published segment.
///
/// Segment must be freed as soon as possible.
/// Not calling `recycle` will never set the segment free, and a subsequent call to `receive`
/// will return the same segment.
///
/// The segment will panic when `recycle` is called for the second time on the same segment.
pub(crate) struct PublishedSegment<'a> {
    io_ring: &'a IORing,
    seg: &'a IORingSegment,
    read: usize,
}

impl<'a> PublishedSegment<'a> {
    const fn new(io_ring: &'a IORing, seg: &'a IORingSegment, read: usize) -> Self {
        Self { io_ring, seg, read }
    }

    #[inline(always)]
    pub(crate) fn data(&self) -> &mut Vec<u8> {
        unsafe { self.seg.buffer() }
    }

    /// Frees the segment to be used by producers.
    ///
    /// This method will panic if it is called on a recycled segment.
    #[inline(always)]
    pub(crate) fn recycle(self) {
        // Once all pathways get proper shape, it can be made in debug mode only.
        assert!(
            self.io_ring.read.load(Acquire) == self.read,
            "Invalid recycling"
        );
        self.seg.state.store(SEG_NONE, Relaxed);
        self.io_ring.read.store(self.read.wrapping_add(1), Release);
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
    write: AtomicUsize,
    read: AtomicUsize,
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
            write: AtomicUsize::new(0),
            read: AtomicUsize::new(0),
        }
    }

    /// Tries to acquire a segment for writing.
    ///
    /// Returns `None` if all segments are currently in use.
    pub(crate) fn acquire(&self) -> Option<IOSegment<'_>> {
        loop {
            let write = self.write.load(Relaxed);
            let read = self.read.load(Acquire);

            // TODO: Cycles policy. Right now the two pointers are monotonic unbounded.
            let used = write.wrapping_sub(read);
            if used >= self.segments.len() {
                // Doing cleanups here will make things complicated and slow,
                // because this path is entered on full capacity regardless of the reason.
                //
                // For faster dynamics, consumer must be invoked to make segments free,
                // because it detects both, published and discarded segments, and doesn't have to
                // do synchronized state-store with this thread, and this thread doesn't
                // have to update the read cursor also.
                //
                // So this setup is leaner and cleaner and faster, just poke the consumer to drive the "noria",
                // and the wheel will spin again.
                return None;
            }

            let seg_idx = write & self.mask;
            let segment = &self.segments[seg_idx];

            // Acquire exclusive segment.
            if self
                .write
                .compare_exchange_weak(write, write.wrapping_add(1), AcqRel, Relaxed)
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
    /// Returns a reference to the current published segment if any, or `None` otherwise.
    pub(crate) fn receive(&self) -> Option<PublishedSegment<'_>> {
        loop {
            let read = self.read.load(Relaxed);
            let seg_idx = read & self.mask;
            let segment = &self.segments[seg_idx];

            match segment.state.load(Acquire) {
                SEG_PUBLISHED => {
                    return Some(PublishedSegment::new(self, segment, read));
                }
                SEG_NONE => return None,
                SEG_DISCARDED => {
                    segment.state.store(SEG_NONE, Relaxed);
                    self.read.store(read.wrapping_add(1), Release);
                }
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod tests_io_ring {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn test_io_ring_acquire_publish_receive() {
        let ring = IORing::new(1, 43);

        let mut segment = ring.acquire().expect("must get a segment");

        let _ = segment.write(b"The quick brown fox ");
        let _ = segment.write(b"jumps over the lazy dog");

        let res = ring.receive();
        assert!(res.is_none());

        segment.publish();

        let published = ring.receive().expect("Must get published segment");
        assert_eq!(
            published.data().as_slice(),
            b"The quick brown fox jumps over the lazy dog"
        );

        let segment = ring.acquire();
        assert!(segment.is_none());

        published.recycle();

        let segment = ring.acquire();
        assert!(segment.is_some());
    }

    #[test]
    #[should_panic = "Invalid recycling"]
    fn test_io_ring_recycling_twice() {
        let ring = IORing::new(1, 0);

        let segment = ring.acquire().expect("must get a segment");
        segment.publish();

        let published = ring.receive().expect("Must get published segment");
        let same_published = ring.receive().expect("Must get published segment again");

        // Set none.
        published.recycle();

        // Fire in the hole...
        same_published.recycle();
    }

    #[test]
    fn test_io_ring_read_empty() {
        let ring = IORing::new(1, 0);
        let n = ring.receive();
        assert!(n.is_none());
    }

    #[test]
    fn test_io_ring_no_space() {
        let ring = IORing::new(1, 0);
        let _ = ring.acquire().expect("expected a free segment");
        assert!(ring.acquire().is_none());
    }

    #[test]
    fn test_io_ring_publishing_order() {
        let ring = IORing::new(4, 5);
        let msgs = [b"Alpha", b"Betaa", b"Gamma"];

        for m in msgs {
            let mut seg = ring.acquire().expect("expected free segment");
            seg.write(m).unwrap();
            seg.publish();
        }

        let mut dst = [0u8; 15];
        let mut pos = 0;

        while let Some(published) = ring.receive() {
            dst[pos..pos + 5].copy_from_slice(&published.data());
            published.recycle();
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

            seg.write(&(i + 1).to_le_bytes()).unwrap();

            seg.publish();

            let pub_seg = ring.receive().unwrap();
            dst.copy_from_slice(&pub_seg.data());

            let num = u16::from_le_bytes(dst);
            assert_eq!(num, i + 1);

            pub_seg.recycle();
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
                seg.write("thread1".as_bytes()).unwrap();
                seg.publish();
            }
        });

        let ring_2 = ring.clone();
        let barrier_2 = barrier.clone();
        let t2 = thread::spawn(move || {
            barrier_2.wait();
            for _ in 0..30 {
                let mut seg = ring_2.acquire().unwrap();
                seg.write("thread2".as_bytes()).unwrap();
                seg.publish();
            }
        });

        let ring_3 = ring.clone();
        let barrier_3 = barrier.clone();
        let t3 = thread::spawn(move || {
            barrier_3.wait();
            for _ in 0..30 {
                let mut seg = ring_3.acquire().unwrap();
                seg.write("thread3".as_bytes()).unwrap();
                seg.publish();
            }
        });

        let ring_4 = ring.clone();
        let barrier_4 = barrier.clone();
        let t4 = thread::spawn(move || {
            barrier_4.wait();
            for _ in 0..30 {
                let mut seg = ring_4.acquire().unwrap();
                seg.write("thread4".as_bytes()).unwrap();
                seg.publish();
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();
        t4.join().unwrap();

        let mut counts = [0usize; 4];

        while let Some(published) = ring.receive() {
            match std::str::from_utf8(&published.data()).expect("Unreadable data in the ring") {
                "thread1" => counts[0] += 1,
                "thread2" => counts[1] += 1,
                "thread3" => counts[2] += 1,
                "thread4" => counts[3] += 1,
                other => panic!("Unexpected data: {other}"),
            }
            published.recycle();
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
        while let Some(published) = ring.receive() {
            published.recycle();
        }

        // All clear.
        let seg_4 = ring.acquire();
        assert!(seg_4.is_some());

        let seg_5 = ring.acquire();
        assert!(seg_5.is_some());
    }
}
