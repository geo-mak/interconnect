use crate::codec::convert::from::FromProtocolType;
use crate::codec::types::core::{
    TypeF32, TypeF64, TypeI16, TypeI32, TypeI64, TypeU16, TypeU32, TypeU64,
};

/// Type that is convertible to native type.
pub trait IntoNative: Sized {
    type Native: FromProtocolType<Self>;

    /// Converts this type into its native equivalent.
    fn into_native(self) -> Self::Native {
        Self::Native::from_protocol_type(self)
    }
}

macro_rules! impl_into_native_for {
    ($ty:ty) => {
        impl_into_native_for!($ty, into $ty);
    };
    ($protocol:ty, into $native:ty) => {
        impl IntoNative for $protocol {
            type Native = $native;
        }
    };
}

impl_into_native_for! {()}
impl_into_native_for! {bool}
impl_into_native_for! { i8 }
impl_into_native_for! { TypeI16, into i16 }
impl_into_native_for! { TypeI32, into i32 }
impl_into_native_for! { TypeI64, into i64 }
impl_into_native_for! { u8 }
impl_into_native_for! { TypeU16, into u16 }
impl_into_native_for! { TypeU32, into u32 }
impl_into_native_for! { TypeU64, into u64 }
impl_into_native_for! { TypeF32, into f32 }
impl_into_native_for! { TypeF64, into f64 }

impl<P: IntoNative, const N: usize> IntoNative for [P; N] {
    type Native = [P::Native; N];
}
