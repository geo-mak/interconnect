use core::mem::{ManuallyDrop, MaybeUninit};
use core::ptr::copy_nonoverlapping;

use crate::codec::convert::opt::CopyConversion;
use crate::codec::types::core::{
    TypeF32, TypeF64, TypeI8, TypeI16, TypeI32, TypeI64, TypeU8, TypeU16, TypeU32, TypeU64,
};

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
