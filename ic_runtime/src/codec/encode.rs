use core::mem::MaybeUninit;
use core::ptr::copy_nonoverlapping;

use crate::codec::convert::opt::CopyConversion;
use crate::codec::types::core::{
    ProtocolType, TypeF32, TypeF64, TypeI8, TypeI16, TypeI32, TypeI64, TypeU8, TypeU16, TypeU32,
    TypeU64,
};
use crate::codec::types::limits::TypeLimits;
use crate::error::ProtocolResult;

pub unsafe trait Encode<P, E>: Sized
where
    P: ProtocolType,
    E: ?Sized,
{
    /// Hint for encoders that enables fast conversion if the type can be copied bitwise.
    const COPY_CONVERSION: CopyConversion<Self, P> = CopyConversion::disable();

    /// Encodes the value into the provided encoder and storage.
    fn encode(
        self,
        encoder: &mut E,
        storage: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()>;
}

macro_rules! impl_encode_for {
    ($ty:ty) => {
        impl_encode_for!($ty, $ty);
    };
    ($p_type:ty, $encodable:ty) => {
        unsafe impl<E: ?Sized> Encode<$p_type, E> for $encodable {
            #[inline]
            fn encode(
                self,
                encoder: &mut E,
                storage: &mut MaybeUninit<$p_type>,
                limits: <$p_type as TypeLimits>::Limits,
            ) -> ProtocolResult<()> {
                Encode::encode(&self, encoder, storage, limits)
            }
        }

        unsafe impl<'a, E: ?Sized> Encode<$p_type, E> for &'a $encodable {
            #[inline]
            fn encode(
                self,
                _: &mut E,
                storage: &mut MaybeUninit<$p_type>,
                _: <$p_type as TypeLimits>::Limits,
            ) -> ProtocolResult<()> {
                storage.write(<$p_type>::from(*self));
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

unsafe impl<P, E, T> Encode<P, E> for Box<T>
where
    P: ProtocolType,
    E: ?Sized,
    T: Encode<P, E>,
{
    fn encode(
        self,
        encoder: &mut E,
        storage: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        T::encode(*self, encoder, storage, limits)
    }
}

unsafe impl<'a, P, E, T> Encode<P, E> for &'a Box<T>
where
    P: ProtocolType,
    E: ?Sized,
    &'a T: Encode<P, E>,
{
    fn encode(
        self,
        encoder: &mut E,
        storage: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        <&'a T>::encode(self, encoder, storage, limits)
    }
}

fn encode_into_array<A, P, E, T, const N: usize>(
    value: A,
    encoder: &mut E,
    storage: &mut MaybeUninit<[P; N]>,
    limits: P::Limits,
) -> ProtocolResult<()>
where
    A: AsRef<[T]> + IntoIterator,
    P: ProtocolType,
    E: ?Sized,
    A::Item: Encode<P, E>,
    T: Encode<P, E>,
{
    if T::COPY_CONVERSION.is_enabled() {
        unsafe {
            copy_nonoverlapping(value.as_ref().as_ptr().cast(), storage.as_mut_ptr(), 1);
        }
    } else {
        for (i, item) in value.into_iter().enumerate() {
            let value_i = unsafe { &mut *storage.as_mut_ptr().cast::<MaybeUninit<P>>().add(i) };
            item.encode(encoder, value_i, limits)?;
        }
    }
    Ok(())
}

unsafe impl<P, E, T, const N: usize> Encode<[P; N], E> for [T; N]
where
    P: ProtocolType,
    E: ?Sized,
    T: Encode<P, E>,
{
    fn encode(
        self,
        encoder: &mut E,
        storage: &mut MaybeUninit<[P; N]>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        encode_into_array(self, encoder, storage, limits)
    }
}

unsafe impl<'a, P, E, T, const N: usize> Encode<[P; N], E> for &'a [T; N]
where
    P: ProtocolType,
    E: ?Sized,
    T: Encode<P, E>,
    &'a T: Encode<P, E>,
{
    fn encode(
        self,
        encoder: &mut E,
        storage: &mut MaybeUninit<[P; N]>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        encode_into_array(self, encoder, storage, limits)
    }
}

unsafe impl<P, E, T> Encode<P, E> for Option<T>
where
    P: ProtocolType,
    E: ?Sized,
    T: EncodeOption<P, E>,
{
    fn encode(
        self,
        encoder: &mut E,
        storage: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        T::encode_option(self, encoder, storage, limits)
    }
}

unsafe impl<'a, P, E, T> Encode<P, E> for &'a Option<T>
where
    P: ProtocolType,
    E: ?Sized,
    Option<&'a T>: Encode<P, E>,
{
    fn encode(
        self,
        encoder: &mut E,
        storage: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        self.as_ref().encode(encoder, storage, limits)
    }
}

pub unsafe trait EncodeOption<P, E>: Sized
where
    P: ProtocolType,
    E: ?Sized,
{
    /// Encodes the optional value into the provided encoder and storage.
    fn encode_option(
        instance: Option<Self>,
        encoder: &mut E,
        storage: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()>;
}

unsafe impl<P, E, T> EncodeOption<P, E> for Box<T>
where
    P: ProtocolType,
    E: ?Sized,
    T: EncodeOption<P, E>,
{
    fn encode_option(
        instance: Option<Self>,
        encoder: &mut E,
        storage: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        T::encode_option(instance.map(|value| *value), encoder, storage, limits)
    }
}

unsafe impl<'a, P, E, T> EncodeOption<P, E> for &'a Box<T>
where
    P: ProtocolType,
    E: ?Sized,
    &'a T: EncodeOption<P, E>,
{
    fn encode_option(
        instance: Option<Self>,
        encoder: &mut E,
        storage: &mut MaybeUninit<P>,
        limits: P::Limits,
    ) -> ProtocolResult<()> {
        <&'a T>::encode_option(instance.map(|value| &**value), encoder, storage, limits)
    }
}
