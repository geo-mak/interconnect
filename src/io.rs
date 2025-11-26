use std::cell::Cell;
use std::io;
use std::mem::ManuallyDrop;
use std::ptr::copy_nonoverlapping;
use std::sync::Arc;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU8, AtomicUsize};

use crate::opt::branch_prediction::unlikely;

pub trait AsyncIORead {
    /// Tries to write available data into the provided output-segment from the underlying I/O source.
    ///
    /// Note:
    /// - The provided output-segment must be assumed to be consisting of **uninitialized** data.
    ///
    /// - Writing into the output-segment has **initialization** semantics.
    ///
    /// - Reading from the output-segment at a position where no data has been written yet is undefined behavior.
    ///
    /// Returns the number of bytes where **initialized** by being successfully written to the output-segment.
    fn read(&mut self, output: &mut [u8]) -> impl Future<Output = io::Result<usize>> + Send
    where
        Self: Unpin;

    /// Tries to write data into the provided output-segment from the underlying I/O source.
    ///
    /// This method will try to fill the provided output-segment, or it will fail otherwise.
    ///
    /// Note:
    /// - The provided output-segment must be assumed to be consisting of **uninitialized** data.
    ///
    /// - Writing into the output-segment has **initialization** semantics.
    ///
    /// - Reading from the output-segment at a position where no data has been written yet is undefined behavior.
    ///
    /// Returns the number of bytes where **initialized** by being successfully written to the output-segment.
    fn read_exact(&mut self, output: &mut [u8]) -> impl Future<Output = io::Result<usize>> + Send
    where
        Self: Unpin;
}

pub trait AsyncIOWrite {
    fn write(&mut self, input: &[u8]) -> impl Future<Output = io::Result<usize>> + Send
    where
        Self: Unpin;

    fn write_all(&mut self, input: &[u8]) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Unpin;

    fn terminate(&mut self) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Unpin;
}

pub(crate) struct IOPoolSegment {
    pool: Arc<IOSegmentsPool>,
    segment_ptr: *mut u8,
    len: usize,
}

unsafe impl Send for IOPoolSegment {}
unsafe impl Sync for IOPoolSegment {}

impl IOPoolSegment {
    #[inline]
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.pool.seg_size
    }

    #[inline(always)]
    pub(crate) const fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts_mut(self.segment_ptr, self.len) }
    }

    #[inline(always)]
    pub(crate) const fn data_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.segment_ptr, self.len) }
    }

    #[inline]
    pub(crate) fn clear(&mut self) {
        self.len = 0;
    }

    #[inline]
    pub(crate) unsafe fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.pool.seg_size);
        self.len = len;
    }

    #[inline]
    fn recycle(&mut self) {
        let offset = self.segment_ptr as usize - self.pool.data.as_ptr() as usize;
        let index = offset >> self.pool.offset_shift;

        let mut free_list = self.pool.free_list.lock();
        free_list.recycle(index);
    }
}

impl Drop for IOPoolSegment {
    fn drop(&mut self) {
        self.recycle();
    }
}

impl<'a> io::Write for IOPoolSegment {
    /// Tries to write the provided input data into the segment.
    ///
    /// Note:
    /// This call doesn't do partial writes, either the entire input data is written,
    /// or an `UnexpectedEof` error is returned as a result of lack of capacity.
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        let capacity = self.pool.seg_size;
        let current_len = self.len;

        let free = capacity - current_len;
        let src_len = input.len();

        if unlikely(free < src_len) {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        unsafe {
            copy_nonoverlapping(input.as_ptr(), self.segment_ptr.add(current_len), src_len);
        }

        self.len += src_len;
        Ok(src_len)
    }

    /// This call in no-op.
    ///
    /// Call `publish` to save written data.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct IOPoolFreeList {
    slots: Box<[usize]>,
    free: usize,
}

impl IOPoolFreeList {
    #[inline]
    pub(crate) fn new(capacity: usize) -> Self {
        IOPoolFreeList {
            slots: (0..capacity).collect(),
            free: capacity,
        }
    }

    const fn acquire(&mut self) -> Option<usize> {
        if self.free == 0 {
            None
        } else {
            self.free -= 1;
            Some(self.slots[self.free])
        }
    }

    const fn recycle(&mut self, index: usize) {
        self.slots[self.free] = index;
        self.free += 1;
    }
}

struct IOSegmentsPool {
    data: Vec<u8>,
    free_list: parking_lot::Mutex<IOPoolFreeList>,
    seg_size: usize,
    offset_shift: u32,
}

#[derive(Clone)]
pub(crate) struct IOPool {
    pool: Arc<IOSegmentsPool>,
}

impl IOPool {
    pub(crate) fn new(count: usize, seg_size: usize) -> Self {
        assert!(count > 0, "Count must be greater than 0");
        assert!(
            seg_size.is_power_of_two(),
            "Segment's size must be power of 2"
        );

        let capacity = count.checked_mul(seg_size).expect("Allocation overflow");

        let data = Vec::with_capacity(capacity);
        let free_list = IOPoolFreeList::new(count);

        IOPool {
            pool: Arc::new(IOSegmentsPool {
                data,
                seg_size,
                offset_shift: seg_size.trailing_zeros(),
                free_list: parking_lot::Mutex::new(free_list),
            }),
        }
    }

    #[inline]
    pub(crate) fn segment_size(&self) -> usize {
        self.pool.seg_size
    }

    #[inline]
    pub(crate) fn acquire(&self) -> Option<IOPoolSegment> {
        let segment_index = self.pool.free_list.lock().acquire()?;

        let offset = segment_index << self.pool.offset_shift;
        let segment_ptr = unsafe { self.pool.data.as_ptr().add(offset) as *mut u8 };

        Some(IOPoolSegment {
            segment_ptr,
            len: 0,
            pool: Arc::clone(&self.pool),
        })
    }
}

/// An exclusive segment that implements `io::Write`.
///
/// `publish` method must be called to save the data written to the segment.
/// `flush` method does nothing.
///
/// If dropped before calling `publish`, the written data will be discarded.
pub(crate) struct IORingSegment<'a> {
    data: &'a mut [u8],
    metadata: &'a IOSegmentMetadata,
}

impl<'a> IORingSegment<'a> {
    const fn new(data: &'a mut [u8], metadata: &'a IOSegmentMetadata) -> Self {
        Self { data, metadata }
    }

    /// Returns the maximum capacity of the segment.
    #[inline(always)]
    pub(crate) const fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Returns the amount of bytes have been written to the segment.
    #[inline(always)]
    pub(crate) const fn len(&self) -> u32 {
        self.metadata.written.get()
    }

    /// Returns a slice to the written data.
    #[inline(always)]
    pub(crate) fn data(&self) -> &[u8] {
        let written_len = self.metadata.written.get() as usize;
        &self.data[..written_len]
    }

    /// Returns a mutable slice to the written data.
    #[inline(always)]
    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        let written_len = self.metadata.written.get() as usize;
        &mut self.data[..written_len]
    }

    #[inline(always)]
    pub(crate) fn publish(self) {
        self.metadata.state.store(SEG_PUBLISHED, Release);
        let _ = ManuallyDrop::new(self);
    }

    #[inline(always)]
    fn set_discarded(&self) {
        self.metadata.written.set(0);
        self.metadata.state.store(SEG_DISCARDED, Release);
    }
}

impl<'a> io::Write for IORingSegment<'a> {
    /// Tries to write the provided input data into the segment.
    ///
    /// Note:
    /// This call doesn't do partial writes, either the entire input data is written,
    /// or an `UnexpectedEof` error is returned as a result of lack of capacity.
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        let current_len = self.metadata.written.get() as usize;
        let free = self.data.len() - current_len;
        let src_len = input.len();

        if unlikely(free < src_len) {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        unsafe {
            copy_nonoverlapping(
                input.as_ptr(),
                self.data.as_mut_ptr().add(current_len),
                src_len,
            );
        }

        self.metadata.written.set((current_len + src_len) as u32);
        Ok(src_len)
    }

    /// This call in no-op.
    ///
    /// Call `publish` to save written data.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> Drop for IORingSegment<'a> {
    fn drop(&mut self) {
        // If publish is never called, it must be discarded
        // to prevent deadlocking the ring.
        self.set_discarded()
    }
}

/// A published segment that contains data.
///
/// Data can be accessed via `data` methods.
///
/// After handling the data, `recycle` method must be called to drive the ring
/// and get the next published segment.
///
/// Segment must be freed as soon as possible.
/// Not calling `recycle` will never set the segment free, and a subsequent call to `receive`
/// will return the same segment.
///
/// The segment will panic when `recycle` is called on the same segment more than once.
pub(crate) struct IORingPubSegment<'a> {
    ring: &'a IORing,
    data: &'a mut [u8],
    metadata: &'a IOSegmentMetadata,
    read: usize,
}

impl<'a> IORingPubSegment<'a> {
    #[inline(always)]
    const fn new(
        ring: &'a IORing,
        data: &'a mut [u8],
        metadata: &'a IOSegmentMetadata,
        read: usize,
    ) -> Self {
        Self {
            ring,
            data,
            metadata,
            read,
        }
    }

    /// Returns the amount of bytes have been published via the segment.
    #[inline(always)]
    pub(crate) const fn len(&self) -> u32 {
        self.metadata.written.get()
    }

    /// Returns a slice to the published data.
    #[inline(always)]
    fn data(&self) -> &[u8] {
        let written_len = self.metadata.written.get() as usize;
        &self.data[..written_len]
    }

    /// Returns a mutable slice to the published data.
    #[inline(always)]
    fn data_mut(&mut self) -> &mut [u8] {
        let written_len = self.metadata.written.get() as usize;
        &mut self.data[..written_len]
    }

    /// Frees the segment to be reused.
    ///
    /// This method will panic if it is called on the same segment more than once.
    #[inline(always)]
    pub(crate) fn recycle(self) {
        // RT_ASSERT.
        assert!(
            self.ring.read.load(Acquire) == self.read,
            "Recycling the same segment more than once"
        );
        self.metadata.written.set(0);
        self.metadata.state.store(SEG_NONE, Relaxed);
        self.ring.read.store(self.read.wrapping_add(1), Release);
    }
}

struct IOSegmentMetadata {
    state: AtomicU8,
    /// Safety: Non-synchronized store. No concurrent access.
    written: Cell<u32>,
}

impl IOSegmentMetadata {
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(SEG_NONE),
            written: Cell::new(0),
        }
    }
}

/// A multi-producer **single-consumer** ring buffer for variable-length byte-messages.
///
/// This buffer acts as a bounded communication channel.
///
/// The bounded implementation means that it will block writing when it reaches its full capacity.
///
/// This structure is a core component, which doesn't included queueing and notification components
/// and doesn't observe panic events on both sides, with very basic protection mechanism
/// aimed at ensuring data integrity.
pub(crate) struct IORing {
    data: Vec<u8>,
    metadata: Vec<IOSegmentMetadata>,
    seg_count: usize,
    seg_size: u32,
    offset_shift: u32,
    index_mask: usize,
    // TODO: The two pointers are monotonic with wrapping arithmetic.
    write: AtomicUsize,
    read: AtomicUsize,
}

const SEG_NONE: u8 = 0;
const SEG_PUBLISHED: u8 = 1;
const SEG_DISCARDED: u8 = 2;

unsafe impl Sync for IORing {}

impl IORing {
    /// Creates new `IORing` with the specified count of fixed-size segments.
    ///
    /// Parameters:
    /// - `count`: The count of concurrent segments. Count must be power of 2.
    /// - `seg_size`: The size of each segment in bytes. Size must be power of 2 and <= u32::MAX;
    pub(crate) fn new(count: usize, seg_size: usize) -> Self {
        assert!(count.is_power_of_two(), "Count must be power of 2");
        assert!(
            seg_size.is_power_of_two() && seg_size <= u32::MAX as usize,
            "Segment's size must be power of 2 and <= u32::MAX"
        );

        let capacity = count.checked_mul(seg_size).expect("Allocation overflow");

        let data = Vec::with_capacity(capacity);
        let metadata = (0..count).map(|_| IOSegmentMetadata::new()).collect();

        Self {
            data,
            metadata,
            seg_count: count,
            seg_size: seg_size as u32,
            offset_shift: seg_size.trailing_zeros(),
            index_mask: count - 1,
            write: AtomicUsize::new(0),
            read: AtomicUsize::new(0),
        }
    }

    pub(crate) const fn capacity(&self) -> usize {
        self.seg_count
    }

    pub(crate) const fn segment_size(&self) -> u32 {
        self.seg_size
    }

    const fn segment_data_of(&self, segment_index: usize) -> &mut [u8] {
        let offset = segment_index << self.offset_shift;
        unsafe {
            std::slice::from_raw_parts_mut(
                self.data.as_ptr().add(offset) as *mut u8,
                self.seg_size as usize,
            )
        }
    }

    /// Tries to acquire a segment for writing.
    ///
    /// Returns `None` if all segments are currently in use.
    pub(crate) fn acquire(&self) -> Option<IORingSegment<'_>> {
        loop {
            let write = self.write.load(Relaxed);
            let read = self.read.load(Acquire);

            if write.wrapping_sub(read) >= self.seg_count {
                return None;
            }

            let segment_index = write & self.index_mask;

            // Acquire exclusive segment.
            if self
                .write
                .compare_exchange_weak(write, write.wrapping_add(1), AcqRel, Relaxed)
                .is_ok()
            {
                let metadata = &self.metadata[segment_index];
                let segment_data = self.segment_data_of(segment_index);

                debug_assert!(metadata.written.get() == 0);

                return Some(IORingSegment {
                    data: segment_data,
                    metadata,
                });
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
    pub(crate) fn receive(&self) -> Option<IORingPubSegment<'_>> {
        loop {
            let read = self.read.load(Relaxed);
            let segment_index = read & self.index_mask;
            let metadata = &self.metadata[segment_index];

            match metadata.state.load(Acquire) {
                SEG_PUBLISHED => {
                    let segment_data = self.segment_data_of(segment_index);
                    return Some(IORingPubSegment::new(self, segment_data, metadata, read));
                }
                SEG_NONE => return None,
                SEG_DISCARDED => {
                    metadata.state.store(SEG_NONE, Relaxed);
                    self.read.store(read.wrapping_add(1), Release);
                }
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod tests_io_pool {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_io_pool_acquire_write_recycle() {
        let pool = IOPool::new(1, 64);

        let mut segment = pool.acquire().expect("must get a segment");
        assert!(segment.capacity() == 64);
        assert!(segment.len() == 0);
        assert_eq!(segment.data(), b"");

        let written = segment.write(b"The quick brown fox ").unwrap();
        assert!(written == 20);
        assert!(segment.len() == 20);
        assert_eq!(segment.data(), b"The quick brown fox ");

        let written = segment.write(b"jumps over the lazy dog").unwrap();
        assert!(written == 23);
        assert!(segment.len() == 43);
        assert_eq!(
            segment.data(),
            b"The quick brown fox jumps over the lazy dog"
        );

        let another_segment = pool.acquire();
        assert!(another_segment.is_none());

        drop(segment);

        let segment = pool.acquire();
        assert!(segment.is_some());
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
        let ring = IORing::new(1, 64);

        let mut segment = ring.acquire().expect("must get a segment");
        assert!(segment.capacity() == 64);
        assert!(segment.len() == 0);
        assert_eq!(segment.data(), b"");

        let written = segment.write(b"The quick brown fox ").unwrap();
        assert!(written == 20);
        assert!(segment.len() == 20);
        assert_eq!(segment.data(), b"The quick brown fox ");

        let written = segment.write(b"jumps over the lazy dog").unwrap();
        assert!(written == 23);
        assert!(segment.len() == 43);
        assert_eq!(
            segment.data(),
            b"The quick brown fox jumps over the lazy dog"
        );

        let res = ring.receive();
        assert!(res.is_none());

        segment.publish();

        let published = ring.receive().expect("Must get published segment");
        assert!(published.len() == 43);
        assert_eq!(
            published.data(),
            b"The quick brown fox jumps over the lazy dog"
        );

        let segment = ring.acquire();
        assert!(segment.is_none());

        published.recycle();

        let segment = ring.acquire();
        assert!(segment.is_some());
    }

    #[test]
    #[should_panic = "Recycling the same segment more than once"]
    fn test_io_ring_recycling_twice() {
        let ring = IORing::new(1, 1);

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
    fn test_io_ring_discarded_segment() {
        let ring = IORing::new(2, 1);
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

    #[test]
    fn test_io_ring_wrapping_cycles() {
        let ring = IORing::new(8, 2);

        let mut dst = [0u8; 2];
        for i in 0u16..4096 {
            let mut seg = ring.acquire().expect("acquire failed");

            seg.write(&(i + 1).to_le_bytes()).unwrap();

            seg.publish();

            let published = ring.receive().unwrap();
            dst.copy_from_slice(published.data());

            let num = u16::from_le_bytes(dst);
            assert_eq!(num, i + 1);

            published.recycle();
        }
    }

    #[test]
    fn test_io_ring_data_race() {
        let ring = Arc::new(IORing::new(128, 8));
        let barrier = Arc::new(Barrier::new(4));
        let mut threads = Vec::with_capacity(4);

        for i in 0..4 {
            let ring_clone = ring.clone();
            let barrier_clone = barrier.clone();
            threads.push(thread::spawn(move || {
                barrier_clone.wait();
                for _ in 0..30 {
                    let mut segment = ring_clone.acquire().unwrap();
                    segment.write(format!("thread{}", i).as_bytes()).unwrap();
                    segment.publish();
                }
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        let mut counts = [0usize; 4];

        while let Some(published) = ring.receive() {
            match std::str::from_utf8(published.data()).expect("Unreadable data in the ring") {
                "thread0" => counts[0] += 1,
                "thread1" => counts[1] += 1,
                "thread2" => counts[2] += 1,
                "thread3" => counts[3] += 1,
                other => panic!("Unexpected data: {other}"),
            }
            published.recycle();
        }

        assert_eq!(counts, [30, 30, 30, 30]);
    }
}
