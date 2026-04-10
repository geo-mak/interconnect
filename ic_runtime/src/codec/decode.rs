use crate::codec::reference::TypeRef;
use crate::codec::types::core::{
    ProtocolType, TypeF32, TypeF64, TypeI8, TypeI16, TypeI32, TypeI64, TypeU8, TypeU16, TypeU32,
    TypeU64,
};
use crate::error::{ErrKind, ProtocolError, ProtocolResult};

/// A type that can construct itself as protocol type from bytes.
pub unsafe trait Decode<D: ?Sized>: ProtocolType {
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
