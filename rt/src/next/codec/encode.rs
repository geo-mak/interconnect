use core::mem::MaybeUninit;
use core::ptr::copy_nonoverlapping;

use crate::next::error::ProtocolResult;
use crate::next::types::convert::CopyConversion;
use crate::next::types::core::{
    TypeF32, TypeF64, TypeI8, TypeI16, TypeI32, TypeI64, TypeU8, TypeU16, TypeU32, TypeU64,
};
use crate::next::types::limits::TypeLimits;

pub unsafe trait Encode<P: TypeLimits, E: ?Sized>: Sized {
    /// Hint for encoders that enables fast conversion if the type can be copied bitwise.
    const COPY_CONVERSION: CopyConversion<Self, P> = CopyConversion::disable();
    /// Encodes the value in two stages.
    ///
    /// Inlined values are stored on the stack as `MaybeUninit<P>`
    /// and written back after the out-of-line payload, if any.
    fn encode(
        self,
        encoder: &mut E,
        inline_value: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()>;
}

/// Encodes an optional value.
pub unsafe trait EncodeOption<P: TypeLimits, E: ?Sized>: Sized {
    /// Encodes this optional in two stages.
    ///
    /// Inlined values are stored on the stack as `MaybeUninit<P>`
    /// and written back after the out-of-line payload, if any.
    fn encode_option(
        instance: Option<Self>,
        encoder: &mut E,
        inline_value: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()>;
}

unsafe impl<P: TypeLimits, E: ?Sized, T: Encode<P, E>> Encode<P, E> for Box<T> {
    fn encode(
        self,
        encoder: &mut E,
        inline_value: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        T::encode(*self, encoder, inline_value, limits)
    }
}

unsafe impl<'a, P, E, T> Encode<P, E> for &'a Box<T>
where
    P: TypeLimits,
    E: ?Sized,
    &'a T: Encode<P, E>,
{
    fn encode(
        self,
        encoder: &mut E,
        inline_value: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        <&'a T>::encode(self, encoder, inline_value, limits)
    }
}

unsafe impl<P, E, T> EncodeOption<P, E> for Box<T>
where
    P: TypeLimits,
    E: ?Sized,
    T: EncodeOption<P, E>,
{
    fn encode_option(
        instance: Option<Self>,
        encoder: &mut E,
        inline_value: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        T::encode_option(instance.map(|value| *value), encoder, inline_value, limits)
    }
}

unsafe impl<'a, P, E, T> EncodeOption<P, E> for &'a Box<T>
where
    P: TypeLimits,
    E: ?Sized,
    &'a T: EncodeOption<P, E>,
{
    fn encode_option(
        instance: Option<Self>,
        encoder: &mut E,
        inline_value: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        <&'a T>::encode_option(
            instance.map(|value| &**value),
            encoder,
            inline_value,
            limits,
        )
    }
}

macro_rules! impl_encode_for {
    ($ty:ty) => {
        impl_encode_for!($ty, $ty);
    };
    ($ty:ty, $enc:ty) => {
        unsafe impl<E: ?Sized> Encode<$enc, E> for $ty {
            #[inline]
            fn encode(
                self,
                encoder: &mut E,
                storage: &mut MaybeUninit<$enc>,
                limits: <$enc as TypeLimits>::Limits,
            ) -> ProtocolResult<()> {
                Encode::encode(&self, encoder, storage, limits)
            }
        }

        unsafe impl<'a, E: ?Sized> Encode<$enc, E> for &'a $ty {
            #[inline]
            fn encode(
                self,
                _: &mut E,
                storage: &mut MaybeUninit<$enc>,
                _limits: <$enc as TypeLimits>::Limits,
            ) -> ProtocolResult<()> {
                storage.write(<$enc>::from(*self));
                Ok(())
            }
        }
    };
}

impl_encode_for!(());
impl_encode_for!(bool);
impl_encode_for!(TypeI8);
impl_encode_for!(TypeI16);
impl_encode_for!(TypeI32);
impl_encode_for!(TypeI64);
impl_encode_for!(TypeU8);
impl_encode_for!(TypeU16);
impl_encode_for!(TypeU32);
impl_encode_for!(TypeU64);
impl_encode_for!(TypeF32);
impl_encode_for!(TypeF64);

fn encode_to_array<V, P, E, T, const N: usize>(
    value: V,
    encoder: &mut E,
    inline_value: &mut MaybeUninit<[P; N]>,
    limits: P::Limits,
) -> ProtocolResult<()>
where
    V: AsRef<[T]> + IntoIterator,
    V::Item: Encode<P, E>,
    P: TypeLimits,
    E: ?Sized,
    T: Encode<P, E>,
{
    if T::COPY_CONVERSION.is_enabled() {
        unsafe {
            copy_nonoverlapping(value.as_ref().as_ptr().cast(), inline_value.as_mut_ptr(), 1);
        }
    } else {
        for (i, item) in value.into_iter().enumerate() {
            let value_i =
                unsafe { &mut *inline_value.as_mut_ptr().cast::<MaybeUninit<P>>().add(i) };
            item.encode(encoder, value_i, limits)?;
        }
    }
    Ok(())
}

unsafe impl<P, E, T, const N: usize> Encode<[P; N], E> for [T; N]
where
    P: TypeLimits,
    E: ?Sized,
    T: Encode<P, E>,
{
    fn encode(
        self,
        encoder: &mut E,
        inline_value: &mut MaybeUninit<[P; N]>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        encode_to_array(self, encoder, inline_value, limits)
    }
}

unsafe impl<'a, P, E, T, const N: usize> Encode<[P; N], E> for &'a [T; N]
where
    P: TypeLimits,
    E: ?Sized,
    T: Encode<P, E>,
    &'a T: Encode<P, E>,
{
    fn encode(
        self,
        encoder: &mut E,
        inline_value: &mut MaybeUninit<[P; N]>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        encode_to_array(self, encoder, inline_value, limits)
    }
}

unsafe impl<P, E, T> Encode<P, E> for Option<T>
where
    P: TypeLimits,
    E: ?Sized,
    T: EncodeOption<P, E>,
{
    fn encode(
        self,
        encoder: &mut E,
        inline_value: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        T::encode_option(self, encoder, inline_value, limits)
    }
}

unsafe impl<'a, P, E, T> Encode<P, E> for &'a Option<T>
where
    P: TypeLimits,
    E: ?Sized,
    Option<&'a T>: Encode<P, E>,
{
    fn encode(
        self,
        encoder: &mut E,
        inline_value: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        self.as_ref().encode(encoder, inline_value, limits)
    }
}
