use core::mem;
use core::ptr::NonNull;
use core::slice;

use crate::codec::decode::{Decode, Decoded};
use crate::codec::reference::TypeRef;
use crate::codec::types::limits::TypeLimits;
use crate::error::{ErrKind, ProtocolError, ProtocolResult};
use crate::mem::{BASIC_BLOCK_SIZE, BasicBlock};

#[inline]
const fn assert_conform_to_alignment<T>() {
    assert!(
        align_of::<T>() <= BASIC_BLOCK_SIZE,
        "Type has higher alignment than the alignment of the basic block",
    );
}

/// A type that can decode Interconnect's messages.
///
/// Note:
/// The current type-system has no explicit support for sharing OS-handles,
/// and the decoder also doesn't support that either.
///
/// Supporting OS-handles might get added later.
pub unsafe trait Decoder {
    /// Returns a pointer to a slice of blocks backed by decoder's memory.
    ///
    /// Safety:
    /// - The returned pointer must point to `count` initialized blocks.
    /// - The returned pointer must be valid for reads and writes.
    /// - The returned pointer must not outlive the decoder.
    fn get_blocks_pointer(&mut self, count: usize) -> ProtocolResult<NonNull<BasicBlock>>;

    fn get_blocks<'de>(
        self: &mut &'de mut Self,
        count: usize,
    ) -> Result<&'de mut [BasicBlock], ProtocolError> {
        let blocks_ptr = self.get_blocks_pointer(count)?;

        Ok(unsafe { slice::from_raw_parts_mut(blocks_ptr.as_ptr(), count) })
    }

    fn ref_as<'de, T>(self: &mut &'de mut Self) -> ProtocolResult<TypeRef<'de, T>> {
        assert_conform_to_alignment::<T>();

        let count = size_of::<T>().div_ceil(BASIC_BLOCK_SIZE);

        let blocks = self.get_blocks(count)?;

        unsafe { Ok(TypeRef::from_ptr_assume_init(blocks.as_mut_ptr().cast())) }
    }

    fn slice_of_ref_as<'de, T>(
        self: &mut &'de mut Self,
        len: usize,
    ) -> ProtocolResult<TypeRef<'de, [T]>> {
        assert_conform_to_alignment::<T>();

        let items_bytes = size_of::<T>() * len;

        let blocks_count = items_bytes.div_ceil(BASIC_BLOCK_SIZE);

        let blocks_bytes = BASIC_BLOCK_SIZE * blocks_count;

        let padding_len = blocks_bytes - items_bytes;

        let blocks_ptr = self.get_blocks(blocks_count)?.as_mut_ptr();

        let padding_bytes: &[u8] =
            unsafe { slice::from_raw_parts(blocks_ptr.cast::<u8>().add(items_bytes), padding_len) };

        // RT_ASSERT.
        // Padding bytes must be zeros.
        if padding_bytes.iter().any(|byte| *byte != 0) {
            return Err(ProtocolError::error(ErrKind::InvalidPadding));
        }

        unsafe { Ok(TypeRef::new_slice_assume_init(blocks_ptr.cast(), len)) }
    }

    fn decode<T>(mut self, limits: T::Limits) -> ProtocolResult<Decoded<T, Self>>
    where
        T: Decode<Self> + TypeLimits,
        Self: Sized,
    {
        let mut decoder = &mut self;

        let mut view = decoder.ref_as::<T>()?;

        T::decode(view.borrow_mut(), decoder, limits)?;

        unsafe { Ok(Decoded::new_assume_valid(view.as_ptr_mut(), self)) }
    }

    fn decode_associated_type<'de, T>(
        self: &mut &'de mut Self,
        limits: T::Limits,
    ) -> Result<T::Type<'de>, ProtocolError>
    where
        T: Decode<Self> + TypeLimits,
    {
        let mut view = self.ref_as::<T>()?;

        T::decode(view.borrow_mut(), self, limits)?;

        unsafe { Ok(view.as_ptr_mut().cast::<T::Type<'de>>().read()) }
    }
}

unsafe impl Decoder for &mut [BasicBlock] {
    #[inline]
    fn get_blocks_pointer(&mut self, count: usize) -> ProtocolResult<NonNull<BasicBlock>> {
        if count > self.len() {
            return Err(ProtocolError::error(ErrKind::NotEnoughData));
        }

        let blocks = mem::take(self);

        let (first, rest) = unsafe { blocks.split_at_mut_unchecked(count) };

        *self = rest;

        unsafe { Ok(NonNull::new_unchecked(first.as_mut_ptr())) }
    }
}
