#[cfg(not(target_endian = "little"))]
compile_error!("only little-endian targets are supported by Interconnect");

use core::mem::MaybeUninit;

use crate::codec::types::limits::TypeLimits;

/// Interconnect's protocol type.
pub unsafe trait ProtocolType: 'static + Sized + TypeLimits {
    /// The referenced inner type.
    type Type<'de>: TypeLimits<Limits = Self::Limits>;

    /// Writes zeroes to the padding for this type if applicable.
    fn write_zero_padding(to: &mut MaybeUninit<Self>);
}

macro_rules! impl_protocol_type_for {
    ($ty:ty) => {
        unsafe impl ProtocolType for $ty {
            type Type<'de> = Self;

            #[inline]
            fn write_zero_padding(_: &mut MaybeUninit<Self>) {}
        }
    };
}

macro_rules! impl_core_op_unary {
    ($trait:ident::$fn:ident, $name:ident : $inner:ty) => {
        impl core::ops::$trait for $name {
            type Output = <$inner as core::ops::$trait>::Output;
            #[inline]
            fn $fn(self) -> Self::Output {
                self.0.$fn()
            }
        }
    };
}

macro_rules! impl_core_op_binary {
    ($trait:ident::$fn:ident, $name:ident : $inner:ty) => {
        impl core::ops::$trait<$inner> for $name {
            type Output = $inner;
            #[inline]
            fn $fn(self, rhs: $inner) -> Self::Output {
                self.0.$fn(rhs)
            }
        }

        impl core::ops::$trait<&$inner> for $name {
            type Output = $inner;
            #[inline]
            fn $fn(self, rhs: &$inner) -> Self::Output {
                self.0.$fn(*rhs)
            }
        }

        impl core::ops::$trait<$name> for $name {
            type Output = $inner;
            #[inline]
            fn $fn(self, rhs: $name) -> Self::Output {
                self.0.$fn(rhs.0)
            }
        }

        impl core::ops::$trait<&$name> for $name {
            type Output = $inner;
            #[inline]
            fn $fn(self, rhs: &$name) -> Self::Output {
                self.0.$fn(rhs.0)
            }
        }

        impl core::ops::$trait<$inner> for &$name {
            type Output = $inner;
            #[inline]
            fn $fn(self, rhs: $inner) -> Self::Output {
                self.0.$fn(rhs)
            }
        }

        impl core::ops::$trait<&$inner> for &$name {
            type Output = $inner;
            #[inline]
            fn $fn(self, rhs: &$inner) -> Self::Output {
                self.0.$fn(*rhs)
            }
        }

        impl core::ops::$trait<$name> for &$name {
            type Output = $inner;
            #[inline]
            fn $fn(self, rhs: $name) -> Self::Output {
                self.0.$fn(rhs.0)
            }
        }

        impl core::ops::$trait<&$name> for &$name {
            type Output = $inner;
            #[inline]
            fn $fn(self, rhs: &$name) -> Self::Output {
                self.0.$fn(rhs.0)
            }
        }
    };
}

macro_rules! impl_core_op_assign {
    ($trait:ident::$fn:ident, $name:ident : $inner:ty) => {
        impl core::ops::$trait<$inner> for $name {
            #[inline]
            fn $fn(&mut self, rhs: $inner) {
                self.0.$fn(rhs);
            }
        }

        impl core::ops::$trait<&$inner> for $name {
            #[inline]
            fn $fn(&mut self, rhs: &$inner) {
                self.0.$fn(*rhs);
            }
        }

        impl core::ops::$trait<$name> for $name {
            #[inline]
            fn $fn(&mut self, rhs: $name) {
                self.0.$fn(rhs.0);
            }
        }

        impl core::ops::$trait<&$name> for $name {
            #[inline]
            fn $fn(&mut self, rhs: &$name) {
                self.0.$fn(rhs.0);
            }
        }
    };
}

macro_rules! impl_clone_copy {
    ($name:ident) => {
        impl Copy for $name {}
        impl Clone for $name {
            #[inline]
            fn clone(&self) -> Self {
                *self
            }
        }
    };
}

macro_rules! impl_default {
    ($name:ident : $inner:ty) => {
        impl Default for $name {
            #[inline]
            fn default() -> Self {
                Self(<$inner as Default>::default())
            }
        }
    };
}

macro_rules! impl_fmt {
    ($trait:ident, $name:ident) => {
        impl core::fmt::$trait for $name {
            #[inline]
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                ::core::fmt::$trait::fmt(&self.0, f)
            }
        }
    };
}

macro_rules! impl_from {
    ($name:ident : $inner:ty) => {
        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }

        impl<'a> From<&'a $inner> for $name {
            fn from(value: &'a $inner) -> Self {
                Self(*value)
            }
        }

        impl From<$name> for $inner {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl<'a> From<&'a $name> for $inner {
            fn from(value: &'a $name) -> Self {
                value.0
            }
        }
    };
}

macro_rules! impl_hash {
    ($name:ident) => {
        impl core::hash::Hash for $name {
            fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
                self.0.hash(state);
            }
        }
    };
}

macro_rules! impl_partial_ord_and_ord {
    ($name:ident : $inner:ty) => {
        impl PartialOrd for $name {
            #[inline]
            fn partial_cmp(&self, other: &Self) -> Option<::core::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl PartialOrd<$inner> for $name {
            #[inline]
            fn partial_cmp(&self, other: &$inner) -> Option<::core::cmp::Ordering> {
                self.0.partial_cmp(other)
            }
        }

        impl Ord for $name {
            #[inline]
            fn cmp(&self, other: &Self) -> ::core::cmp::Ordering {
                self.0.cmp(&other.0)
            }
        }
    };
}

macro_rules! impl_partial_eq_and_eq {
    ($name:ident : $inner:ty) => {
        impl PartialEq for $name {
            #[inline]
            fn eq(&self, other: &Self) -> bool {
                let lhs = self.0;
                let rhs = other.0;
                lhs.eq(&rhs)
            }
        }

        impl PartialEq<$inner> for $name {
            #[inline]
            fn eq(&self, other: &$inner) -> bool {
                self.0.eq(other)
            }
        }

        impl Eq for $name {}
    };
}

macro_rules! impl_partial_ord {
    ($name:ident : $inner:ty) => {
        impl PartialOrd for $name {
            #[inline]
            fn partial_cmp(&self, other: &Self) -> Option<::core::cmp::Ordering> {
                self.0.partial_cmp(&other.0)
            }
        }

        impl PartialOrd<$inner> for $name {
            #[inline]
            fn partial_cmp(&self, other: &$inner) -> Option<::core::cmp::Ordering> {
                self.0.partial_cmp(other)
            }
        }
    };
}

macro_rules! impl_product_and_sum {
    ($name:ident) => {
        impl core::iter::Product for $name {
            #[inline]
            fn product<I: Iterator<Item = Self>>(iter: I) -> Self {
                Self(iter.map(|x| x.0).product())
            }
        }

        impl core::iter::Sum for $name {
            #[inline]
            fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
                Self(iter.map(|x| x.0).sum())
            }
        }
    };
}

macro_rules! impl_int_traits {
    ($name:ident: $inner:ident) => {
        impl_core_op_unary!(Neg::neg, $name: $inner);
        impl_core_op_unary!(Not::not, $name: $inner);

        impl_core_op_binary!(Add::add, $name: $inner);
        impl_core_op_binary!(Div::div, $name: $inner);
        impl_core_op_binary!(BitAnd::bitand, $name: $inner);
        impl_core_op_binary!(BitOr::bitor, $name: $inner);
        impl_core_op_binary!(BitXor::bitxor, $name: $inner);
        impl_core_op_binary!(Mul::mul, $name: $inner);
        impl_core_op_binary!(Rem::rem, $name: $inner);
        impl_core_op_binary!(Shl::shl, $name: $inner);
        impl_core_op_binary!(Shr::shr, $name: $inner);
        impl_core_op_binary!(Sub::sub, $name: $inner);

        impl_core_op_assign!(AddAssign::add_assign, $name: $inner);
        impl_core_op_assign!(BitXorAssign::bitxor_assign, $name: $inner);
        impl_core_op_assign!(BitOrAssign::bitor_assign, $name: $inner);
        impl_core_op_assign!(BitAndAssign::bitand_assign, $name: $inner);
        impl_core_op_assign!(DivAssign::div_assign, $name: $inner);
        impl_core_op_assign!(RemAssign::rem_assign, $name: $inner);
        impl_core_op_assign!(ShlAssign::shl_assign, $name: $inner);
        impl_core_op_assign!(ShrAssign::shr_assign, $name: $inner);
        impl_core_op_assign!(SubAssign::sub_assign, $name: $inner);
        impl_core_op_assign!(MulAssign::mul_assign, $name: $inner);

        impl_partial_eq_and_eq!($name: $inner);

        impl_partial_ord_and_ord!($name: $inner);

        impl_product_and_sum!($name);

        impl_fmt!(Debug, $name);
        impl_fmt!(Display, $name);
        impl_fmt!(LowerExp, $name);
        impl_fmt!(LowerHex, $name);
        impl_fmt!(UpperExp, $name);
        impl_fmt!(UpperHex, $name);
        impl_fmt!(Binary, $name);
        impl_fmt!(Octal, $name);

        impl_clone_copy!($name);

        impl_default!($name: $inner);

        impl_from!($name: $inner);

        impl_hash!($name);
    };
}

macro_rules! impl_uint_traits {
    ($name:ident: $inner:ident) => {
        impl_core_op_unary!(Not::not, $name: $inner);

        impl_core_op_binary!(Add::add, $name: $inner);
        impl_core_op_binary!(Div::div, $name: $inner);
        impl_core_op_binary!(BitAnd::bitand, $name: $inner);
        impl_core_op_binary!(BitOr::bitor, $name: $inner);
        impl_core_op_binary!(BitXor::bitxor, $name: $inner);
        impl_core_op_binary!(Mul::mul, $name: $inner);
        impl_core_op_binary!(Rem::rem, $name: $inner);
        impl_core_op_binary!(Shl::shl, $name: $inner);
        impl_core_op_binary!(Shr::shr, $name: $inner);
        impl_core_op_binary!(Sub::sub, $name: $inner);

        impl_core_op_assign!(AddAssign::add_assign, $name: $inner);
        impl_core_op_assign!(BitXorAssign::bitxor_assign, $name: $inner);
        impl_core_op_assign!(BitOrAssign::bitor_assign, $name: $inner);
        impl_core_op_assign!(BitAndAssign::bitand_assign, $name: $inner);
        impl_core_op_assign!(DivAssign::div_assign, $name: $inner);
        impl_core_op_assign!(RemAssign::rem_assign, $name: $inner);
        impl_core_op_assign!(ShlAssign::shl_assign, $name: $inner);
        impl_core_op_assign!(ShrAssign::shr_assign, $name: $inner);
        impl_core_op_assign!(SubAssign::sub_assign, $name: $inner);
        impl_core_op_assign!(MulAssign::mul_assign, $name: $inner);

        impl_partial_eq_and_eq!($name: $inner);

        impl_partial_ord_and_ord!($name: $inner);

        impl_product_and_sum!($name);

        impl_fmt!(Debug, $name);
        impl_fmt!(Display, $name);
        impl_fmt!(LowerExp, $name);
        impl_fmt!(LowerHex, $name);
        impl_fmt!(UpperExp, $name);
        impl_fmt!(UpperHex, $name);
        impl_fmt!(Binary, $name);
        impl_fmt!(Octal, $name);

        impl_clone_copy!($name);

        impl_default!($name: $inner);

        impl_from!($name: $inner);

        impl_hash!($name);
    };
}

macro_rules! impl_float_traits {
    ($name:ident: $inner:ty) => {
        impl_core_op_unary!(Neg::neg, $name: $inner);

        impl_core_op_binary!(Add::add, $name: $inner);
        impl_core_op_binary!(Div::div, $name: $inner);
        impl_core_op_binary!(Mul::mul, $name: $inner);
        impl_core_op_binary!(Rem::rem, $name: $inner);
        impl_core_op_binary!(Sub::sub, $name: $inner);

        impl_core_op_assign!(AddAssign::add_assign, $name: $inner);
        impl_core_op_assign!(DivAssign::div_assign, $name: $inner);
        impl_core_op_assign!(MulAssign::mul_assign, $name: $inner);
        impl_core_op_assign!(RemAssign::rem_assign, $name: $inner);
        impl_core_op_assign!(SubAssign::sub_assign, $name: $inner);

        impl_partial_eq_and_eq!($name: $inner);

        impl_partial_ord!($name: $inner);

        impl_product_and_sum!($name);

        impl_fmt!(Debug, $name);
        impl_fmt!(Display, $name);
        impl_fmt!(LowerExp, $name);
        impl_fmt!(UpperExp, $name);

        impl_clone_copy!($name);

        impl_default!($name: $inner);

        impl_from!($name: $inner);
    };
}

macro_rules! define_primitive {
    ($name:ident: $inner:ty, $align:expr) => {
        #[repr(C, align($align))]
        #[derive(zerocopy::FromBytes, zerocopy::IntoBytes)]
        pub struct $name(pub $inner);

        impl core::ops::Deref for $name {
            type Target = $inner;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl core::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}

macro_rules! define_int {
    ($name:ident: $inner:ident, $align:expr) => {
        define_primitive!($name: $inner, $align);
        impl_int_traits!($name: $inner);
    }
}

macro_rules! define_uint {
    ($name:ident: $inner:ident, $align:expr) => {
        define_primitive!($name: $inner, $align);
        impl_uint_traits!($name: $inner);
    }
}

macro_rules! define_float {
    ($name:ident: $inner:ident, $align:expr) => {
        define_primitive!($name: $inner, $align);
        impl_float_traits!($name: $inner);
    }
}

define_int!(TypeI8: i8, 1);
define_int!(TypeI16: i16, 2);
define_int!(TypeI32: i32, 4);
define_int!(TypeI64: i64, 8);
define_uint!(TypeU8: u8, 1);
define_uint!(TypeU16: u16, 2);
define_uint!(TypeU32: u32, 4);
define_uint!(TypeU64: u64, 8);
define_float!(TypeF32: f32, 4);
define_float!(TypeF64: f64, 8);

impl_protocol_type_for!(());
impl_protocol_type_for!(bool);
impl_protocol_type_for!(TypeI8);
impl_protocol_type_for!(TypeI16);
impl_protocol_type_for!(TypeI32);
impl_protocol_type_for!(TypeI64);
impl_protocol_type_for!(TypeU8);
impl_protocol_type_for!(TypeU16);
impl_protocol_type_for!(TypeU32);
impl_protocol_type_for!(TypeU64);
impl_protocol_type_for!(TypeF32);
impl_protocol_type_for!(TypeF64);

unsafe impl<T: ProtocolType, const N: usize> ProtocolType for [T; N] {
    type Type<'de> = [T::Type<'de>; N];

    #[inline]
    fn write_zero_padding(storage: &mut MaybeUninit<Self>) {
        for i in 0..N {
            let item = unsafe { &mut *storage.as_mut_ptr().cast::<MaybeUninit<T>>().add(i) };
            T::write_zero_padding(item);
        }
    }
}
