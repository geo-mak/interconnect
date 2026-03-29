use core::marker::PhantomData;
use core::mem::{ManuallyDrop, MaybeUninit};
use core::ptr::copy_nonoverlapping;

use crate::types::core::{
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

    /// Returns an enabled instance, if conversion from `[T; N]` to `[U; N]` is already enabled.
    pub const fn eval_array<const N: usize>(&self) -> CopyConversion<[T; N], [U; N]>
    where
        T: Sized,
        U: Sized,
    {
        unsafe { CopyConversion::from_predicate(self.is_enabled()) }
    }

    /// Returns an enabled instance, if conversion from `[T]` to `[U]` is already enabled.
    pub const fn eval_slice(&self) -> CopyConversion<[T], [U]>
    where
        T: Sized,
        U: Sized,
    {
        unsafe { CopyConversion::from_predicate(self.is_enabled()) }
    }
}

impl<T: ?Sized> CopyConversion<T, T> {
    /// Returns an instance with conversion enabled.
    pub const fn identical() -> Self {
        unsafe { Self::enable() }
    }
}

macro_rules! impl_copy_conversion_between {
    ($ty:ty) => {
        impl CopyConversion<$ty, $ty> {
            pub const PRIMITIVE: Self = Self::identical();
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

/// A type which is convertible from a protocol type.
pub trait FromProtocolType<P>: Sized {
    /// Checks if conversion by bitwise-copying is enabled.
    const COPY_CONVERSION: CopyConversion<P, Self> = CopyConversion::disable();

    /// Converts the given `protocol_type` to this type.
    fn from_protocol_type(protocol_type: P) -> Self;
}

/// A type which is convertible from a reference to a protocol type.
pub trait FromProtocolTypeRef<P>: FromProtocolType<P> {
    /// Converts the given `protocol_type` reference to this type.
    fn from_protocol_type_ref(protocol_type: &P) -> Self;
}

/// A convertible type from option of protocol type.
pub trait FromOptionProtocolType<P>: Sized {
    /// Converts the given `protocol_type` to an option of this type.
    fn from_option_protocol_type(protocol_type: P) -> Option<Self>;
}

/// A convertible type from a reference to an option of protocol type.
pub trait FromOptionProtocolTypeRef<P>: FromOptionProtocolType<P> {
    /// Converts the given `protocol_type` reference to an option of this type.
    fn from_option_protocol_type_ref(protocol_type: &P) -> Option<Self>;
}

macro_rules! impl_from_protocol_type_for {
    ($ty:ty) => {
        impl_from_protocol_type_for!($ty, from $ty);
    };
    ($native:ty, from $protocol:ty) => {
        impl FromProtocolType<$protocol> for $native {
            const COPY_CONVERSION: CopyConversion<$protocol, $native> =
                CopyConversion::<$protocol, $native>::PRIMITIVE;

            #[inline]
            fn from_protocol_type(protocol_type: $protocol) -> Self {
                protocol_type.into()
            }
        }

        impl FromProtocolTypeRef<$protocol> for $native {
            #[inline]
            fn from_protocol_type_ref(protocol_type: &$protocol) -> Self {
                (*protocol_type).into()
            }
        }
    };
}

impl_from_protocol_type_for! {()}
impl_from_protocol_type_for! {bool}
impl_from_protocol_type_for! { i8, from TypeI8 }
impl_from_protocol_type_for! { i16, from TypeI16 }
impl_from_protocol_type_for! { i32, from TypeI32 }
impl_from_protocol_type_for! { i64, from TypeI64 }
impl_from_protocol_type_for! { u8, from TypeU8 }
impl_from_protocol_type_for! { u16, from TypeU16 }
impl_from_protocol_type_for! { u32, from TypeU32 }
impl_from_protocol_type_for! { u64, from TypeU64 }
impl_from_protocol_type_for! { f32, from TypeF32 }
impl_from_protocol_type_for! { f64, from TypeF64 }

impl<T: FromProtocolType<P>, P, const N: usize> FromProtocolType<[P; N]> for [T; N] {
    fn from_protocol_type(protocol_type: [P; N]) -> Self {
        let mut value = MaybeUninit::<[T; N]>::uninit();
        if T::COPY_CONVERSION.is_enabled() {
            unsafe {
                copy_nonoverlapping(protocol_type.as_ptr().cast(), value.as_mut_ptr(), 1);
            }
            let _ = ManuallyDrop::new(protocol_type);
        } else {
            for (i, item) in protocol_type.into_iter().enumerate() {
                unsafe {
                    value
                        .as_mut_ptr()
                        .cast::<T>()
                        .add(i)
                        .write(T::from_protocol_type(item));
                }
            }
        }
        unsafe { value.assume_init() }
    }
}

impl<T: FromProtocolTypeRef<P>, P, const N: usize> FromProtocolTypeRef<[P; N]> for [T; N] {
    fn from_protocol_type_ref(protocol_type: &[P; N]) -> Self {
        let mut value = MaybeUninit::<[T; N]>::uninit();
        if T::COPY_CONVERSION.is_enabled() {
            unsafe {
                copy_nonoverlapping(protocol_type.as_ptr().cast(), value.as_mut_ptr(), 1);
            }
        } else {
            for (i, item) in protocol_type.iter().enumerate() {
                unsafe {
                    value
                        .as_mut_ptr()
                        .cast::<T>()
                        .add(i)
                        .write(T::from_protocol_type_ref(item));
                }
            }
        }
        unsafe { value.assume_init() }
    }
}

impl<T: FromProtocolType<P>, P> FromProtocolType<P> for Box<T> {
    fn from_protocol_type(protocol_type: P) -> Self {
        Box::new(T::from_protocol_type(protocol_type))
    }
}

impl<T: FromProtocolTypeRef<P>, P> FromProtocolTypeRef<P> for Box<T> {
    fn from_protocol_type_ref(protocol_type: &P) -> Self {
        Box::new(T::from_protocol_type_ref(protocol_type))
    }
}

impl<T: FromOptionProtocolType<P>, P> FromProtocolType<P> for Option<T> {
    fn from_protocol_type(protocol_type: P) -> Self {
        T::from_option_protocol_type(protocol_type)
    }
}

impl<T: FromOptionProtocolTypeRef<P>, P> FromProtocolTypeRef<P> for Option<T> {
    fn from_protocol_type_ref(protocol_type: &P) -> Self {
        T::from_option_protocol_type_ref(protocol_type)
    }
}

/// Type that is convertible to native type.
pub trait IntoNativeType: Sized {
    type NativeType: FromProtocolType<Self>;

    /// Converts this type into its native equivalent.
    fn into_native_type(self) -> Self::NativeType {
        Self::NativeType::from_protocol_type(self)
    }
}

macro_rules! impl_into_native_for {
    ($ty:ty) => {
        impl_into_native_for!($ty, into $ty);
    };
    ($protocol:ty, into $native:ty) => {
        impl IntoNativeType for $protocol {
            type NativeType = $native;
        }
    };
}

impl_into_native_for! {()}
impl_into_native_for! {bool}
impl_into_native_for! { TypeI8, into i8 }
impl_into_native_for! { TypeI16, into i16 }
impl_into_native_for! { TypeI32, into i32 }
impl_into_native_for! { TypeI64, into i64 }
impl_into_native_for! { TypeU8, into u8 }
impl_into_native_for! { TypeU16, into u16 }
impl_into_native_for! { TypeU32, into u32 }
impl_into_native_for! { TypeU64, into u64 }
impl_into_native_for! { TypeF32, into f32 }
impl_into_native_for! { TypeF64, into f64 }

impl<P: IntoNativeType, const N: usize> IntoNativeType for [P; N] {
    type NativeType = [P::NativeType; N];
}
