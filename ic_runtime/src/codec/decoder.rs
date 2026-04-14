use core::fmt;
use core::mem;
use core::mem::ManuallyDrop;
use core::ops::Deref;
use core::ptr::NonNull;
use core::slice::{from_raw_parts, from_raw_parts_mut};

use crate::codec::convert::from::FromProtocolType;
use crate::codec::convert::into::IntoNative;
use crate::codec::decode::Decode;
use crate::codec::reference::TypeRef;
use crate::codec::types::core::ProtocolType;
use crate::error::{ErrKind, ProtocolError, ProtocolResult};
use crate::mem::{BASIC_BLOCK_SIZE, BasicBlock};

#[inline]
const fn assert_conform_to_alignment<T>() {
    assert!(
        align_of::<T>() <= BASIC_BLOCK_SIZE,
        "Type has higher alignment than the alignment of the basic block",
    );
}

/// Reference to a decoded value and its backing decoder.
///
/// Note:
/// The current type-system has no explicit support for sharing OS-handles,
/// and the decoder also doesn't support that either.
///
/// Supporting OS-handles might get added later.
pub struct Decoded<T: ?Sized, D> {
    value_ptr: NonNull<T>,
    decoder: ManuallyDrop<D>,
}

unsafe impl<T: Send + ?Sized, D: Send> Send for Decoded<T, D> {}
unsafe impl<T: Sync + ?Sized, D: Sync> Sync for Decoded<T, D> {}

impl<T: ?Sized, D> Deref for Decoded<T, D> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.value_ptr.as_ref() }
    }
}

impl<T: ?Sized, D> Drop for Decoded<T, D> {
    fn drop(&mut self) {
        unsafe {
            // This might frees the decoder for reuse, or deallocates it.
            ManuallyDrop::drop(&mut self.decoder);
        }
    }
}

impl<T: ?Sized, D> Decoded<T, D> {
    /// Creates new instance from the provided pointer and decoder.
    ///
    /// Safety:
    /// Pointer must be valid, aligned to the alignment of `T`
    /// and references value in the passed decoder.
    pub unsafe fn new_assume_valid(ptr: *mut T, decoder: D) -> Self {
        Self {
            value_ptr: unsafe { NonNull::new_unchecked(ptr) },
            decoder: ManuallyDrop::new(decoder),
        }
    }

    /// Returns the pointer and decoder of this instance.
    pub fn into_parts(mut self) -> (*mut T, D) {
        let ptr = self.value_ptr.as_ptr();
        let decoder = unsafe { ManuallyDrop::take(&mut self.decoder) };
        let _ = ManuallyDrop::new(self);
        (ptr, decoder)
    }

    /// Applies transformation on the value using the passed function, and consumes the current instance.
    pub fn map_into<U>(self, f: impl FnOnce(T::Type<'_>) -> U) -> U
    where
        T: ProtocolType,
    {
        // Safety: destructor is now off.
        let (ptr, decoder) = self.into_parts();
        // Make copy to stack.
        let value = unsafe { ptr.cast::<T::Type<'_>>().read() };
        let fn_return = f(value);
        drop(decoder);
        fn_return
    }

    /// Transforms the value into type that can be constructed from protocol type and consumes the current instance.
    pub fn into<U>(self) -> U
    where
        T: ProtocolType,
        U: for<'de> FromProtocolType<T::Type<'de>>,
    {
        self.map_into(|protocol_type| U::from_protocol_type(protocol_type))
    }

    /// Transforms the value into native type and consumes the current instance.
    pub fn into_native(self) -> T::Native
    where
        T: ProtocolType + IntoNative,
        T::Native: for<'de> FromProtocolType<T::Type<'de>>,
    {
        self.into::<T::Native>()
    }
}

impl<T, D> fmt::Debug for Decoded<T, D>
where
    T: fmt::Debug + ?Sized,
    D: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(f)
    }
}

/// A type that can decode Interconnect's messages.
///
/// Note:
/// The current type-system has no explicit support for sharing OS-handles,
/// and the decoder also doesn't support that either.
///
/// Supporting OS-handles might get added later.
pub unsafe trait Decoder {
    /// Returns a pointer to a group of blocks backed by decoder's memory.
    ///
    /// Safety: The returned pointer must not outlive the decoder.
    fn get_blocks_pointer(&mut self, blocks_count: usize) -> ProtocolResult<NonNull<BasicBlock>>;

    fn get_blocks<'de>(
        self: &mut &'de mut Self,
        blocks_count: usize,
    ) -> Result<&'de mut [BasicBlock], ProtocolError> {
        let blocks_ptr = self.get_blocks_pointer(blocks_count)?;

        let blocks_slice = unsafe { from_raw_parts_mut(blocks_ptr.as_ptr(), blocks_count) };

        Ok(blocks_slice)
    }

    fn ref_as<'de, T>(self: &mut &'de mut Self) -> ProtocolResult<TypeRef<'de, T>> {
        assert_conform_to_alignment::<T>();

        let blocks_count = size_of::<T>().div_ceil(BASIC_BLOCK_SIZE);

        let blocks_slice = self.get_blocks(blocks_count)?;

        let type_ref = unsafe { TypeRef::from_ptr_assume_init(blocks_slice.as_mut_ptr().cast()) };

        Ok(type_ref)
    }

    fn slice_ref_as<'de, T>(
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
            unsafe { from_raw_parts(blocks_ptr.cast::<u8>().add(items_bytes), padding_len) };

        // RT_ASSERT.
        // Padding bytes must be zeros.
        if padding_bytes.iter().any(|byte| *byte != 0) {
            return Err(ProtocolError::error(ErrKind::InvalidPadding));
        }

        let type_ref = unsafe { TypeRef::new_slice_assume_init(blocks_ptr.cast(), len) };

        Ok(type_ref)
    }

    /// Attempts to decode the available data as a value of type `T::Type`.
    ///
    /// Returns a reference to the value after **advancing** the decoder.
    ///
    /// Advancing the decoder means that the bytes needed to construct `T::Type` will be skipped after this call.
    fn decode_ref<'de, T>(
        self: &mut &'de mut Self,
        limits: T::Limits,
    ) -> ProtocolResult<T::Type<'de>>
    where
        T: Decode<Self>,
    {
        let mut view = self.ref_as::<T>()?;

        T::decode(view.borrow_mut(), self, limits)?;

        let decoded_ref = unsafe { view.as_ptr_mut().cast::<T::Type<'de>>().read() };

        Ok(decoded_ref)
    }

    /// Attempts to decode the available data as a value of type `T`.
    ///
    /// Consumes the instance and returns a decoded value as owned value,
    /// which can be accessed as value of type `T`.
    fn decode<T>(mut self, limits: T::Limits) -> ProtocolResult<Decoded<T, Self>>
    where
        T: Decode<Self>,
        Self: Sized,
    {
        let mut decoder = &mut self;

        let mut view = decoder.ref_as::<T>()?;

        T::decode(view.borrow_mut(), decoder, limits)?;

        let decoded = unsafe { Decoded::new_assume_valid(view.as_ptr_mut(), self) };

        Ok(decoded)
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

        let blocks_ptr = unsafe { NonNull::new_unchecked(first.as_mut_ptr()) };

        Ok(blocks_ptr)
    }
}
