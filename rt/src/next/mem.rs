//! Next generation memory allocators for sending and receiving.

use core::cell::Cell;
use core::mem::ManuallyDrop;
use core::ptr::copy_nonoverlapping;
use core::slice::{from_raw_parts, from_raw_parts_mut};
use core::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use core::sync::atomic::{AtomicU8, AtomicUsize};

use std::sync::Arc;

use crate::next::types::core::TypeU64;

pub const ALLOC_MEM_ALIGN: usize = 8;

/// Slice of eight bytes aligned to an 8-byte boundary.
pub type BasicBlock = TypeU64;

/// A unified interface for types that perform untyped reads from and writes to a memory-region directly.
pub unsafe trait IOSegment {
    /// Returns the number of the **initialized** bytes in the segment.
    fn len(&self) -> usize;

    /// Returns the current max capacity of the segment in bytes.
    fn capacity(&self) -> usize;

    /// Checks the current capacity if it is sufficient enough for the `required` capacity in total.
    ///
    /// The `required` capacity is compared against total capacity, **not** the remaining capacity,
    /// which might be less if data has been written already.
    ///
    /// Tries to allocates more if the segment allows dynamic allocation.
    ///
    /// Returns `false` if the `required` capacity can't be satisfied, either because the segment is fixed,
    /// or attempting to allocate more has failed.
    fn ensure_capacity(&self, required: usize) -> bool;

    /// Returns the base pointer of the allocated segment.
    fn as_ptr(&self) -> *const u8;

    /// Returns the base pointer of the allocated segment.
    fn as_ptr_mut(&mut self) -> *mut u8;

    /// Returns an immutable view to the **initialized** data.
    #[inline]
    fn as_slice(&self) -> &[u8] {
        unsafe { from_raw_parts(self.as_ptr(), self.len()) }
    }

    /// Returns a mutable view to the **initialized** data.
    #[inline]
    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { from_raw_parts_mut(self.as_ptr_mut(), self.len()) }
    }

    /// Constructs a view as slice of `count` bytes.
    ///
    /// Safety:
    /// - The count must be within the bounds of the allocated memory.
    /// - The bytes of the slice might be uninitialized.
    ///
    /// Note:
    /// There is currently no point of storing "Maybeuninit<u8>" if the call chain until
    /// the point of writing is not conforming to the parameter.
    ///
    /// "Maybeuninit<u8>" in arrays and slices will dramatically increase the code complexity
    /// and transforms the codebase into "spaghetti of casting and transmuting".
    #[inline]
    unsafe fn view(&self, count: usize) -> &[u8] {
        debug_assert!(count <= self.capacity());
        unsafe { core::slice::from_raw_parts(self.as_ptr(), count) }
    }

    /// Constructs a mutable view as mutable slice of `count` bytes.
    ///
    /// # Safety
    /// - The count must be within the bounds of the allocated memory.
    /// - The bytes of the slice might be uninitialized.
    ///
    /// Note:
    /// There is currently no point of storing "Maybeuninit<u8>" if the call chain until
    /// the point of writing is not conforming to the parameter.
    ///
    /// "Maybeuninit<u8>" in arrays and slices will dramatically increase the code complexity
    /// and transforms the codebase into "spaghetti of casting and transmuting".
    #[inline]
    unsafe fn view_mut(&mut self, count: usize) -> &mut [u8] {
        debug_assert!(count <= self.capacity());
        unsafe { core::slice::from_raw_parts_mut(self.as_ptr_mut(), count) }
    }

    /// Sets the current length to `0`.
    fn clear(&mut self);

    /// Sets the length of the segment.
    ///
    /// Safety:
    /// - The new length must be within the bound of segment's capacity.
    /// - No data shall be produced before fully initializing the updated length.
    unsafe fn set_len(&mut self, new_len: usize);

    /// Tries to writes the provided data to the segment in checked-mode.
    ///
    /// If the implementation enables dynamic allocation, this call may allocate more capacity if needed.
    ///
    /// Returns `true` on success or `false` in case of not enough capacity or failure to allocate more.
    ///
    /// Length is advanced after successful writing.
    ///
    /// Safety:
    /// - The source slice must consist of fully initialized bytes.
    /// - The source slice must be a non-overlapping (disjoint) memory-region.
    fn write(&mut self, src: &[u8]) -> bool;

    /// Writes data to the segment in unchecked-mode.
    ///
    /// Safety:
    /// - The source slice must consist of fully initialized bytes.
    /// - The source slice must be a non-overlapping (disjoint) memory-region.
    /// - The segment must have enough capacity to accommodate the the source data.
    /// - The segment is valid for writing/overwriting withing the range [`offset`: source length - 1].
    /// - Length is **not** advanced after writing.
    unsafe fn write_at(&mut self, offset: usize, src: &[u8]);
}

pub(crate) struct IOPoolSegment {
    pool: Arc<IOSegmentsPool>,
    segment_ptr: *mut u8,
    len: usize,
}

unsafe impl Send for IOPoolSegment {}
unsafe impl Sync for IOPoolSegment {}

impl IOPoolSegment {
    /// Returns the maximum capacity of the segment.
    #[inline]
    pub(crate) fn max_capacity(&self) -> usize {
        self.pool.seg_size
    }

    #[inline]
    fn recycle(&mut self) {
        let offset = self.segment_ptr as usize - self.pool.data.as_ptr() as usize;
        let index = offset >> self.pool.offset_shift;
        self.pool.free_list.lock().recycle(index);
    }
}

impl Drop for IOPoolSegment {
    fn drop(&mut self) {
        self.recycle();
    }
}

unsafe impl IOSegment for IOPoolSegment {
    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.max_capacity()
    }

    #[inline]
    fn ensure_capacity(&self, required: usize) -> bool {
        self.max_capacity() >= required
    }

    #[inline]
    fn as_ptr(&self) -> *const u8 {
        self.segment_ptr
    }

    #[inline]
    fn as_ptr_mut(&mut self) -> *mut u8 {
        self.segment_ptr
    }

    #[inline]
    fn clear(&mut self) {
        self.len = 0;
    }

    #[inline]
    unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.pool.seg_size);
        self.len = new_len;
    }

    #[inline]
    fn write(&mut self, src: &[u8]) -> bool {
        let capacity = self.pool.seg_size;
        let current_len = self.len;

        let free = capacity - current_len;
        let src_len = src.len();

        if free < src_len {
            return false;
        }

        unsafe {
            copy_nonoverlapping(src.as_ptr(), self.segment_ptr.add(current_len), src_len);
        }

        self.len += src_len;

        true
    }

    #[inline]
    unsafe fn write_at(&mut self, offset: usize, src: &[u8]) {
        let count = src.len();
        debug_assert!(offset + count <= self.pool.seg_size);
        unsafe { copy_nonoverlapping(src.as_ptr(), self.segment_ptr.add(offset), count) };
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

/// An exclusive segment that implements `IOSegment`.
///
/// `publish` method must be called to save the data written to the segment.
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
    pub(crate) const fn max_capacity(&self) -> usize {
        self.data.len()
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

impl<'a> Drop for IORingSegment<'a> {
    fn drop(&mut self) {
        // If publish is never called, it must be discarded
        // to prevent deadlocking the ring.
        self.set_discarded()
    }
}

unsafe impl<'a> IOSegment for IORingSegment<'a> {
    #[inline]
    fn len(&self) -> usize {
        self.metadata.written.get() as usize
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.max_capacity()
    }

    #[inline]
    fn ensure_capacity(&self, required: usize) -> bool {
        self.max_capacity() >= required
    }

    #[inline]
    fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    #[inline]
    fn as_ptr_mut(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    #[inline]
    fn clear(&mut self) {
        self.metadata.written.set(0);
    }

    #[inline]
    unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.data.len());
        self.metadata.written.set(new_len as u32)
    }

    #[inline]
    fn write(&mut self, src: &[u8]) -> bool {
        let current_len = self.metadata.written.get() as usize;
        let free = self.data.len() - current_len;
        let src_len = src.len();

        if free < src_len {
            return false;
        }

        unsafe {
            copy_nonoverlapping(
                src.as_ptr(),
                self.data.as_mut_ptr().add(current_len),
                src_len,
            );
        }

        self.metadata.written.set((current_len + src_len) as u32);
        true
    }

    #[inline]
    unsafe fn write_at(&mut self, offset: usize, src: &[u8]) {
        let count = src.len();
        debug_assert!(offset + count <= self.max_capacity());
        unsafe { copy_nonoverlapping(src.as_ptr(), self.data.as_mut_ptr().add(offset), count) };
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

    #[test]
    fn test_io_pool_acquire_write_recycle() {
        let pool = IOPool::new(1, 64);

        let mut segment = pool.acquire().expect("must get a segment");
        assert!(segment.max_capacity() == 64);
        assert!(segment.len() == 0);
        assert_eq!(segment.as_slice(), b"");

        assert!(segment.write(b"The quick brown fox "));
        assert!(segment.len() == 20);
        assert_eq!(segment.as_slice(), b"The quick brown fox ");

        assert!(segment.write(b"jumps over the lazy dog"));
        assert!(segment.len() == 43);
        assert_eq!(
            segment.as_slice(),
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
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn test_io_ring_acquire_publish_receive() {
        let ring = IORing::new(1, 64);
        assert!(ring.capacity() == 1);
        assert!(ring.segment_size() == 64);

        let mut segment = ring.acquire().expect("must get a segment");
        assert!(segment.max_capacity() == 64);
        assert!(segment.len() == 0);
        assert_eq!(segment.as_slice(), b"");

        assert!(segment.write(b"The quick brown fox "));
        assert!(segment.len() == 20);
        assert_eq!(segment.as_slice(), b"The quick brown fox ");

        assert!(segment.write(b"jumps over the lazy dog"));
        assert!(segment.len() == 43);
        assert_eq!(
            segment.as_slice(),
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

            assert!(seg.write(&(i + 1).to_le_bytes()));

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
                    assert!(segment.write(format!("thread{}", i).as_bytes()));
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
