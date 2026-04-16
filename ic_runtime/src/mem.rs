use core::cell::Cell;
use core::mem::ManuallyDrop;
use core::ptr::NonNull;
use core::ptr::copy_nonoverlapping;
use core::slice::{from_raw_parts, from_raw_parts_mut};
use core::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use core::sync::atomic::{AtomicU8, AtomicUsize};

use std::sync::Arc;

use crate::codec::decoder::Decoder;
use crate::codec::encoder::Encoder;
use crate::codec::types::core::TypeU64;
use crate::error::{ErrKind, ProtocolError, ProtocolResult};

/// A memory segment with 8 bytes in size and alignment.
pub type BasicBlock = TypeU64;
pub const BASIC_BLOCK_SIZE: usize = 8;
pub const BASIC_BLOCK_SHIFT: usize = 3;
pub const BASIC_BLOCK_MASK: usize = BASIC_BLOCK_SIZE - 1;

/// A unified interface for types that perform untyped reads from and writes to a memory-region directly.
pub unsafe trait IOSegment {
    /// Returns the number of the **initialized** bytes in the segment.
    fn len(&self) -> usize;

    /// Returns the current **total** capacity of the segment in **bytes**.
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

    /// Makes the segment reuseable as new segment with it's length set to `0`.
    ///
    /// The concrete operations performed to make the segment reuseable are implementation-dependent.
    fn clear(&mut self);

    /// Sets the length of the segment.
    ///
    /// Safety:
    /// - The new length must be within the bound of segment's capacity.
    /// - No data shall be produced before fully initializing the updated length.
    unsafe fn set_len(&mut self, new_len: usize);

    /// Returns an immutable view to the **initialized** data.
    #[inline]
    fn as_slice(&self) -> &[u8] {
        unsafe { from_raw_parts(self.as_ptr(), self.len()) }
    }

    /// Constructs a slice of `count` bytes from offset `0` to offset `count - 1`.
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
    /// and transforms the codebase into "spaghetti" of casting and transmuting.
    #[inline]
    unsafe fn as_slice_of(&self, count: usize) -> &[u8] {
        debug_assert!(count <= self.capacity());
        unsafe { from_raw_parts(self.as_ptr(), count) }
    }

    /// Returns a mutable view to the **initialized** data.
    #[inline]
    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { from_raw_parts_mut(self.as_ptr_mut(), self.len()) }
    }

    /// Constructs a mutable slice of `count` bytes from offset `0` to offset `count - 1`.
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
    /// and transforms the codebase into "spaghetti" of casting and transmuting.
    #[inline]
    unsafe fn as_slice_mut_of(&mut self, count: usize) -> &mut [u8] {
        debug_assert!(count <= self.capacity());
        unsafe { from_raw_parts_mut(self.as_ptr_mut(), count) }
    }

    /// Tries to append the provided data to the segment in checked-mode.
    ///
    /// If the implementation enables dynamic allocation, this call may allocate more capacity if needed.
    ///
    /// Returns `true` on success or `false` in case of not enough capacity or failure to allocate more.
    ///
    /// Length is **advanced** after successful storage.
    ///
    /// Safety:
    /// - The source slice must consist of fully initialized bytes.
    /// - The source slice must be a non-overlapping (disjoint) memory-region.
    #[inline]
    fn store(&mut self, source: &[u8]) -> bool {
        let segment_len = self.len();
        let source_len = source.len();

        let new_len = segment_len + source_len;

        if self.ensure_capacity(new_len) {
            unsafe {
                copy_nonoverlapping(
                    source.as_ptr(),
                    self.as_ptr_mut().add(segment_len),
                    source_len,
                );

                self.set_len(new_len);
            };

            return true;
        };

        false
    }

    /// Stores the provided source data in the segment at the provided `offset` in unchecked-mode.
    ///
    /// This function enables storing data anywhere within the capacity of the segment,
    /// regardless of current segment's length.
    ///
    /// Safety:
    /// - The source slice must consist of fully initialized bytes.
    /// - The source slice must be a non-overlapping (disjoint) memory-region.
    /// - The segment must have enough capacity to accommodate the the source data.
    /// - The segment is valid for writing/overwriting withing the range [`offset`: source length - 1].
    /// - The current length remains **unchanged** after writing.
    #[inline]
    unsafe fn store_at(&mut self, offset: usize, source: &[u8]) {
        let count = source.len();
        debug_assert!(offset + count <= self.capacity());
        unsafe { copy_nonoverlapping(source.as_ptr(), self.as_ptr_mut().add(offset), count) };
    }
}

pub trait MemoryProvider {
    type SendSegment;
    type ReceiveSegment;

    fn acquire_send(&self) -> Option<Self::SendSegment>;
    fn acquire_receive(&self) -> Option<Self::ReceiveSegment>;
}

pub(crate) struct IOPoolSegment {
    pool: Arc<IOSegmentsPool>,
    segment_ptr: *mut u8,
    len: usize,
    read_offset: usize,
}

unsafe impl Send for IOPoolSegment {}
unsafe impl Sync for IOPoolSegment {}

impl Drop for IOPoolSegment {
    fn drop(&mut self) {
        self.release();
    }
}

impl IOPoolSegment {
    /// Returns the maximum capacity of the segment.
    #[inline]
    pub(crate) fn max_capacity(&self) -> usize {
        self.pool.seg_size
    }

    #[inline]
    pub fn remaining_blocks(&self) -> usize {
        (self.len - self.read_offset) >> BASIC_BLOCK_SHIFT
    }

    #[inline]
    fn release(&mut self) {
        let offset = self.segment_ptr as usize - self.pool.data.as_ptr() as usize;
        let index = offset >> self.pool.offset_shift;
        self.pool.index.lock().release(index);
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
        self.read_offset = 0;
    }

    #[inline]
    unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.pool.seg_size);
        self.len = new_len;
    }
}

impl Encoder for IOPoolSegment {
    #[inline]
    fn len_bytes(&self) -> usize {
        self.len
    }

    #[inline]
    fn memset_zero(&mut self, count: usize) -> bool {
        let zeroing_len_aligned = (count + BASIC_BLOCK_MASK) & !BASIC_BLOCK_MASK;

        let segment_len = self.len;

        // Assuming the current len is padded.
        debug_assert!(segment_len % BASIC_BLOCK_SIZE == 0);
        let new_len_aligned = segment_len + zeroing_len_aligned;

        // Safety: Capacity must be enured for "extra" aligned bytes.
        if !self.ensure_capacity(new_len_aligned) {
            return false;
        }

        unsafe {
            let zeroing_ptr = self.as_ptr_mut().add(segment_len);

            // Zeroing aligned.
            zeroing_ptr.write_bytes(0, zeroing_len_aligned);

            // Advance aligned.
            self.set_len(new_len_aligned);

            return true;
        }
    }

    #[inline]
    fn write_encoded(&mut self, source: &[u8]) -> bool {
        let source_len = source.len();

        if source_len == 0 {
            return true;
        }

        let source_len_aligned = (source_len + BASIC_BLOCK_MASK) & !BASIC_BLOCK_MASK;

        let segment_len = self.len;

        // Assuming the current len is padded.
        debug_assert!(segment_len % BASIC_BLOCK_SIZE == 0);
        let new_len_aligned = segment_len + source_len_aligned;

        // Safety: Capacity must be enured for "extra" aligned bytes.
        if !self.ensure_capacity(new_len_aligned) {
            return false;
        }

        unsafe {
            // Zero out the last block.
            self.as_ptr_mut()
                .add(new_len_aligned - BASIC_BLOCK_SIZE)
                .cast::<BasicBlock>()
                .write(TypeU64(0));

            let copying_ptr = self.as_ptr_mut().add(segment_len);

            // Copy source exact.
            copying_ptr.copy_from_nonoverlapping(source.as_ptr(), source_len);

            // Advance aligned.
            self.set_len(new_len_aligned);

            return true;
        }
    }

    #[inline]
    fn write_encoded_at(&mut self, offset: usize, source: &[u8]) {
        // RT_ASSERT.
        assert!(offset + source.len() <= self.len);
        unsafe {
            let ptr = self.as_ptr_mut().add(offset);
            ptr.copy_from_nonoverlapping(source.as_ptr(), source.len());
        }
    }
}

unsafe impl Decoder for IOPoolSegment {
    #[inline]
    fn get_blocks_pointer(&mut self, count: usize) -> ProtocolResult<NonNull<BasicBlock>> {
        let segment_len = self.len;
        let read_offset = self.read_offset;

        let remaining_bytes = segment_len.saturating_sub(read_offset);

        let required_bytes = count << BASIC_BLOCK_SHIFT;

        if remaining_bytes < required_bytes {
            return Err(ProtocolError::error(ErrKind::NotEnoughData));
        }

        unsafe {
            let blocks_ptr = self.segment_ptr.add(read_offset);

            // Advance the read offset.
            self.read_offset += required_bytes;

            Ok(NonNull::new_unchecked(blocks_ptr.cast()))
        }
    }
}

struct IOPoolIndex {
    indices: Box<[usize]>,
    free: usize,
}

impl IOPoolIndex {
    #[inline]
    pub(crate) fn new(capacity: usize) -> Self {
        IOPoolIndex {
            indices: (0..capacity).collect(),
            free: capacity,
        }
    }

    const fn acquire(&mut self) -> Option<usize> {
        if self.free == 0 {
            None
        } else {
            self.free -= 1;
            Some(self.indices[self.free])
        }
    }

    const fn release(&mut self, index: usize) {
        self.indices[self.free] = index;
        self.free += 1;
    }
}

struct IOSegmentsPool {
    data: Vec<BasicBlock>,
    index: parking_lot::Mutex<IOPoolIndex>,
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
        assert!(
            seg_size >= BASIC_BLOCK_SIZE,
            "Segment size must be at least `BasicBlock` size"
        );

        let capacity = count.checked_mul(seg_size).expect("Allocation overflow");
        let blocks_count = capacity >> BASIC_BLOCK_SHIFT;

        let data = Vec::with_capacity(blocks_count);
        let index = IOPoolIndex::new(count);

        IOPool {
            pool: Arc::new(IOSegmentsPool {
                data,
                seg_size,
                offset_shift: seg_size.trailing_zeros(),
                index: parking_lot::Mutex::new(index),
            }),
        }
    }

    #[inline]
    pub(crate) fn segment_size(&self) -> usize {
        self.pool.seg_size
    }

    #[inline]
    pub(crate) fn acquire(&self) -> Option<IOPoolSegment> {
        let segment_index = self.pool.index.lock().acquire()?;

        let offset = segment_index << self.pool.offset_shift;
        let segment_ptr = unsafe { (self.pool.data.as_ptr() as *mut u8).add(offset) };

        let segment = IOPoolSegment {
            pool: Arc::clone(&self.pool),
            segment_ptr,
            len: 0,
            read_offset: 0,
        };

        Some(segment)
    }
}

impl MemoryProvider for IOPool {
    type SendSegment = IOPoolSegment;

    type ReceiveSegment = IOPoolSegment;

    #[inline(always)]
    fn acquire_send(&self) -> Option<IOPoolSegment> {
        self.acquire()
    }

    #[inline(always)]
    fn acquire_receive(&self) -> Option<IOPoolSegment> {
        self.acquire()
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
}

/// A published segment that contains data.
///
/// Data can be accessed via `data` methods.
///
/// After handling the data, `release` method must be called to drive the ring
/// and get the next published segment.
///
/// Segment must be freed as soon as possible.
///
/// Subsequent calls to `receive` will always return `None` as long the same received
/// segment has not been released yet.
pub(crate) struct IORingPubSegment<'a> {
    ring: &'a IORing,
    data: &'a mut [u8],
    metadata: &'a IOSegmentMetadata,
}

impl<'a> IORingPubSegment<'a> {
    #[inline(always)]
    const fn new(ring: &'a IORing, data: &'a mut [u8], metadata: &'a IOSegmentMetadata) -> Self {
        Self {
            ring,
            data,
            metadata,
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

    /// Releases the segment to be reused.
    #[inline(always)]
    fn release(&mut self) {
        self.metadata.written.set(0);
        self.ring.rcv_pos.fetch_add(1, Release);
    }
}

impl<'a> Drop for IORingPubSegment<'a> {
    fn drop(&mut self) {
        self.release()
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
    send_pos: AtomicUsize,
    rcv_pos: AtomicUsize,
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
            send_pos: AtomicUsize::new(0),
            rcv_pos: AtomicUsize::new(0),
        }
    }

    pub(crate) const fn capacity(&self) -> usize {
        self.seg_count
    }

    pub(crate) const fn segment_size(&self) -> u32 {
        self.seg_size
    }

    const fn slice_at(&self, segment_index: usize) -> &mut [u8] {
        let offset = segment_index << self.offset_shift;
        unsafe {
            from_raw_parts_mut(
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
            let send_pos = self.send_pos.load(Relaxed);
            let rcv_pos = self.rcv_pos.load(Acquire);

            if send_pos.wrapping_sub(rcv_pos) >= self.seg_count {
                return None;
            }

            let segment_index = send_pos & self.index_mask;

            // Acquire exclusive segment.
            if self
                .send_pos
                .compare_exchange_weak(send_pos, send_pos.wrapping_add(1), AcqRel, Relaxed)
                .is_ok()
            {
                let metadata = &self.metadata[segment_index];

                debug_assert!(metadata.written.get() == 0);

                let segment = IORingSegment {
                    data: self.slice_at(segment_index),
                    metadata,
                };

                return Some(segment);
            }
        }
    }

    /// Tries to receive a published message.
    ///
    /// Safety:
    ///
    /// This method must have single consumer at a time, no concurrent calls.
    ///
    /// User must guarantee that only one consumer at a time can call this method.
    ///
    /// Returns:
    ///
    /// Returns a reference to the current published segment if any, or `None` otherwise.
    pub(crate) fn receive(&self) -> Option<IORingPubSegment<'_>> {
        loop {
            let rcv_pos = self.rcv_pos.load(Relaxed);
            let segment_index = rcv_pos & self.index_mask;
            let metadata = &self.metadata[segment_index];

            match metadata.state.load(Acquire) {
                SEG_PUBLISHED => {
                    metadata.state.store(SEG_NONE, Relaxed);
                    let segment_data = self.slice_at(segment_index);
                    return Some(IORingPubSegment::new(self, segment_data, metadata));
                }
                SEG_NONE => return None,
                SEG_DISCARDED => {
                    metadata.state.store(SEG_NONE, Relaxed);
                    self.rcv_pos.store(rcv_pos.wrapping_add(1), Release);
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

        assert!(segment.store(b"The quick brown fox "));
        assert!(segment.len() == 20);
        assert_eq!(segment.as_slice(), b"The quick brown fox ");

        assert!(segment.store(b"jumps over the lazy dog"));
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

    #[test]
    fn test_io_pool_segment_msg_ops() {
        let pool = IOPool::new(1, 64);
        let mut segment = pool.acquire().expect("must get a segment");

        segment.memset_zero(5);
        assert_eq!(segment.len_bytes(), 8);

        segment.write_encoded(b"there!");
        assert_eq!(segment.len_bytes(), 16);

        segment.write_encoded_at(0, b"hi");

        assert_eq!(segment.remaining_blocks(), 2);

        let block_0 = segment.get_blocks_pointer(1).expect("must get blocks");
        assert_eq!(segment.remaining_blocks(), 1);

        let block_1 = segment.get_blocks_pointer(1).expect("must get blocks");
        assert_eq!(segment.remaining_blocks(), 0);

        assert!(segment.get_blocks_pointer(1).is_err());

        unsafe {
            let block_0_slice = from_raw_parts(block_0.as_ptr().cast::<u8>(), 2);
            assert_eq!(block_0_slice, b"hi");

            let block_1_slice = from_raw_parts(block_1.as_ptr().cast::<u8>(), 6);
            assert_eq!(block_1_slice, b"there!");
        }

        let data = segment.as_slice();

        assert_eq!(data[14], 0);
        assert_eq!(data[15], 0);
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

        assert!(segment.store(b"The quick brown fox "));
        assert!(segment.len() == 20);
        assert_eq!(segment.as_slice(), b"The quick brown fox ");

        assert!(segment.store(b"jumps over the lazy dog"));
        assert!(segment.len() == 43);
        assert_eq!(
            segment.as_slice(),
            b"The quick brown fox jumps over the lazy dog"
        );

        let seg_none = ring.receive();
        assert!(seg_none.is_none());

        segment.publish();

        let published = ring.receive().expect("Must get published segment");

        assert!(published.len() == 43);
        assert_eq!(
            published.data(),
            b"The quick brown fox jumps over the lazy dog"
        );

        // Receiving again without releasing.
        assert!(ring.receive().is_none());

        // Acquiring without releasing.
        assert!(ring.acquire().is_none());

        // Release.
        drop(published);

        {
            assert!(ring.acquire().is_some());
            // Set discarded.
        }

        // Discarded.
        assert!(ring.acquire().is_none());

        // Clear.
        assert!(ring.receive().is_none());

        assert!(ring.acquire().is_some());
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
                    assert!(segment.store(format!("thread{}", i).as_bytes()));
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
        }

        assert_eq!(counts, [30, 30, 30, 30]);
    }
}
