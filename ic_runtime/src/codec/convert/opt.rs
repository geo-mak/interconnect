use core::marker::PhantomData;

use crate::codec::types::core::{
    TypeF32, TypeF64, TypeI8, TypeI16, TypeI32, TypeI64, TypeU8, TypeU16, TypeU32, TypeU64,
};

/// Conversion hint, that tells if conversion from `T` to `U` can be achieved by copying bitwise bytes of `T`.
pub struct CopyConversion<T: ?Sized, U: ?Sized>(bool, PhantomData<(*mut T, *mut U)>);

impl<T: ?Sized, U: ?Sized> CopyConversion<T, U> {
    /// Returns an instance with conversion enabled.
    ///
    /// Safety: `T` and `U` must be the same size and without their padding bytes.
    pub const unsafe fn enable() -> Self {
        Self(true, PhantomData)
    }

    /// Returns an instance with conversion disabled.
    pub const fn disable() -> Self {
        Self(false, PhantomData)
    }

    /// Checks if conversion by copying is enabled.
    pub const fn is_enabled(&self) -> bool {
        self.0
    }

    /// Returns an instance with conversion-hint matching the passed predicate.
    ///
    /// Safety: `T` and `U` must be the same size and without their padding bytes.
    pub const unsafe fn from_predicate(predicate: bool) -> Self {
        Self(predicate, PhantomData)
    }

    /// Returns an enabled instance, if conversion from `T` to `U` is already enabled.
    pub const fn eval_array<const N: usize>(&self) -> CopyConversion<[T; N], [U; N]>
    where
        T: Sized,
        U: Sized,
    {
        unsafe { CopyConversion::from_predicate(self.is_enabled()) }
    }

    /// Returns an enabled instance, if conversion from `T` to `U` is already enabled.
    pub const fn eval_slice(&self) -> CopyConversion<[T], [U]>
    where
        T: Sized,
        U: Sized,
    {
        unsafe { CopyConversion::from_predicate(self.is_enabled()) }
    }
}

macro_rules! impl_copy_conversion_between {
    ($ty:ty) => {
        impl CopyConversion<$ty, $ty> {
            pub const PRIMITIVE: Self = unsafe { Self::enable() };
        }
    };
    ($native:ty, $protocol:ty) => {
        impl_copy_conversion_between!($protocol);

        impl CopyConversion<$native, $protocol> {
            pub const PRIMITIVE: Self = unsafe {
                CopyConversion::from_predicate(
                    size_of::<Self>() <= 1 || cfg!(target_endian = "little"),
                )
            };
        }

        impl CopyConversion<$protocol, $native> {
            pub const PRIMITIVE: Self = unsafe {
                CopyConversion::from_predicate(
                    CopyConversion::<$native, $protocol>::PRIMITIVE.is_enabled(),
                )
            };
        }
    };
}

impl_copy_conversion_between! {()}
impl_copy_conversion_between! {bool}
impl_copy_conversion_between! { i8, TypeI8 }
impl_copy_conversion_between! { i16, TypeI16 }
impl_copy_conversion_between! { i32, TypeI32 }
impl_copy_conversion_between! { i64, TypeI64 }
impl_copy_conversion_between! { u8, TypeU8 }
impl_copy_conversion_between! { u16, TypeU16 }
impl_copy_conversion_between! { u32, TypeU32 }
impl_copy_conversion_between! { u64, TypeU64 }
impl_copy_conversion_between! { f32, TypeF32 }
impl_copy_conversion_between! { f64, TypeF64 }
