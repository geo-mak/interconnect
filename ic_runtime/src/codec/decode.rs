use core::fmt;
use core::mem::ManuallyDrop;
use core::ops::Deref;
use core::ptr::NonNull;

use crate::codec::reference::TypeRef;
use crate::error::{ErrKind, ProtocolError, ProtocolResult};
use crate::types::convert::{FromProtocolType, IntoNativeType};
use crate::types::core::{
    ProtocolType, TypeF32, TypeF64, TypeI8, TypeI16, TypeI32, TypeI64, TypeU8, TypeU16, TypeU32,
    TypeU64,
};
use crate::types::limits::TypeLimits;

/// A type that can construct itself as protocol type from bytes.
pub unsafe trait Decode<D: ?Sized>: ProtocolType + TypeLimits {
    /// Decodes a the type into the provided value.
    ///
    /// This call checks for valid memory-representation and conformance to the defined limits.
    fn decode(
        value: TypeRef<'_, Self>,
        decoder: &mut D,
        limits: Self::Limits,
    ) -> ProtocolResult<()>;
}

macro_rules! impl_decode_for {
    ($ty:ty) => {
        unsafe impl<D: ?Sized> Decode<D> for $ty {
            #[inline]
            fn decode(_: TypeRef<'_, Self>, _: &mut D, _: ()) -> ProtocolResult<()> {
                Ok(())
            }
        }
    };
}

impl_decode_for!(());

unsafe impl<D: ?Sized> Decode<D> for bool {
    #[inline]
    fn decode(value: TypeRef<'_, Self>, _: &mut D, _: ()) -> ProtocolResult<()> {
        let value = unsafe { value.as_ptr().cast::<u8>().read() };
        match value {
            0 | 1 => Ok(()),
            _ => Err(ProtocolError::error(ErrKind::Decoding)),
        }
    }
}

impl_decode_for!(TypeI8);
impl_decode_for!(TypeI16);
impl_decode_for!(TypeI32);
impl_decode_for!(TypeI64);
impl_decode_for!(TypeU8);
impl_decode_for!(TypeU16);
impl_decode_for!(TypeU32);
impl_decode_for!(TypeU64);
impl_decode_for!(TypeF32);
impl_decode_for!(TypeF64);

unsafe impl<D: ?Sized, T: Decode<D>, const N: usize> Decode<D> for [T; N] {
    fn decode(
        mut value: TypeRef<'_, Self>,
        decoder: &mut D,
        limits: T::Limits,
    ) -> ProtocolResult<()> {
        for i in 0..N {
            T::decode(value.index(i), decoder, limits)?;
        }
        Ok(())
    }
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

// Safety:
// - `Send` if `T` and `D` are `Send`.
// - `Sync` if `T` and `D` are `Sync`.
unsafe impl<T: Send + ?Sized, D: Send> Send for Decoded<T, D> {}
unsafe impl<T: Sync + ?Sized, D: Sync> Sync for Decoded<T, D> {}

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

    /// Transforms the value into type that can be converted to native type and consumes the current instance.
    pub fn into_native_as<U>(self) -> U
    where
        T: ProtocolType,
        U: for<'de> FromProtocolType<T::Type<'de>>,
    {
        self.map_into(|protocol_type| U::from_protocol_type(protocol_type))
    }

    /// Transforms the value into native type and consumes the current instance.
    pub fn into_native(self) -> T::NativeType
    where
        T: ProtocolType + IntoNativeType,
        T::NativeType: for<'de> FromProtocolType<T::Type<'de>>,
    {
        self.into_native_as::<T::NativeType>()
    }
}

impl<T: ?Sized, D> Deref for Decoded<T, D> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.value_ptr.as_ref() }
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
