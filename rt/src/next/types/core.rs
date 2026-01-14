#[cfg(not(target_endian = "little"))]
compile_error!("only little-endian targets are supported by Interconnect");

use core::mem::MaybeUninit;

macro_rules! impl_core_op_unary {
    (trait = $trait:ident, fn = $fn:ident, for $name:ident : $inner:ty) => {
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
    (trait = $trait:ident, fn = $fn:ident, for $name:ident : $inner:ty) => {
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
    (trait = $trait:ident, fn = $fn:ident, for $name:ident : $inner:ty) => {
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
    (for $name:ident) => {
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
    (for $name:ident : $inner:ty) => {
        impl Default for $name {
            #[inline]
            fn default() -> Self {
                Self(<$inner as Default>::default())
            }
        }
    };
}

macro_rules! impl_fmt {
    ($trait:ident for $name:ident) => {
        impl core::fmt::$trait for $name {
            #[inline]
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                ::core::fmt::$trait::fmt(&self.0, f)
            }
        }
    };
}

macro_rules! impl_from {
    (for $name:ident : $inner:ty) => {
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
    (for $name:ident) => {
        impl core::hash::Hash for $name {
            fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
                self.0.hash(state);
            }
        }
    };
}

macro_rules! impl_partial_ord_and_ord {
    (for $name:ident : $inner:ty) => {
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
    (for $name:ident : $inner:ty) => {
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
    (for $name:ident : $inner:ty) => {
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
    (for $name:ident) => {
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
        impl_core_op_unary!(trait = Neg, fn = neg, for $name: $inner);
        impl_core_op_unary!(trait = Not, fn = not, for $name: $inner);

        impl_core_op_binary!(trait = Add, fn = add, for $name: $inner);
        impl_core_op_binary!(trait = Div, fn = div, for $name: $inner);
        impl_core_op_binary!(trait = BitAnd, fn = bitand, for $name: $inner);
        impl_core_op_binary!(trait = BitOr, fn = bitor, for $name: $inner);
        impl_core_op_binary!(trait = BitXor, fn = bitxor, for $name: $inner);
        impl_core_op_binary!(trait = Mul, fn = mul, for $name: $inner);
        impl_core_op_binary!(trait = Rem, fn = rem, for $name: $inner);
        impl_core_op_binary!(trait = Shl, fn = shl, for $name: $inner);
        impl_core_op_binary!(trait = Shr, fn = shr, for $name: $inner);
        impl_core_op_binary!(trait = Sub, fn = sub, for $name: $inner);

        impl_core_op_assign!(trait = AddAssign, fn = add_assign, for $name: $inner);
        impl_core_op_assign!(trait = BitXorAssign, fn = bitxor_assign, for $name: $inner);
        impl_core_op_assign!(trait = BitOrAssign, fn = bitor_assign, for $name: $inner);
        impl_core_op_assign!(trait = BitAndAssign, fn = bitand_assign, for $name: $inner);
        impl_core_op_assign!(trait = DivAssign, fn = div_assign, for $name: $inner);
        impl_core_op_assign!(trait = RemAssign, fn = rem_assign, for $name: $inner);
        impl_core_op_assign!(trait = ShlAssign, fn = shl_assign, for $name: $inner);
        impl_core_op_assign!(trait = ShrAssign, fn = shr_assign, for $name: $inner);
        impl_core_op_assign!(trait = SubAssign, fn = sub_assign, for $name: $inner);
        impl_core_op_assign!(trait = MulAssign, fn = mul_assign, for $name: $inner);

        impl_partial_eq_and_eq!(for $name: $inner);
        impl_partial_ord_and_ord!(for $name: $inner);
        impl_product_and_sum!(for $name);

        impl_fmt!(LowerExp for $name);
        impl_fmt!(LowerHex for $name);
        impl_fmt!(UpperExp for $name);
        impl_fmt!(UpperHex for $name);
        impl_fmt!(Octal for $name);
        impl_fmt!(Display for $name);
        impl_fmt!(Binary for $name);
        impl_fmt!(Debug for $name);

        impl_clone_copy!(for $name);
        impl_default!(for $name: $inner);
        impl_hash!(for $name);
        impl_from!(for $name: $inner);
    };
}

macro_rules! impl_uint_traits {
    ($name:ident: $inner:ident) => {
        impl_core_op_unary!(trait = Not, fn = not, for $name: $inner);

        impl_core_op_binary!(trait = Add, fn = add, for $name: $inner);
        impl_core_op_binary!(trait = Div, fn = div, for $name: $inner);
        impl_core_op_binary!(trait = BitAnd, fn = bitand, for $name: $inner);
        impl_core_op_binary!(trait = BitOr, fn = bitor, for $name: $inner);
        impl_core_op_binary!(trait = BitXor, fn = bitxor, for $name: $inner);
        impl_core_op_binary!(trait = Mul, fn = mul, for $name: $inner);
        impl_core_op_binary!(trait = Rem, fn = rem, for $name: $inner);
        impl_core_op_binary!(trait = Shl, fn = shl, for $name: $inner);
        impl_core_op_binary!(trait = Shr, fn = shr, for $name: $inner);
        impl_core_op_binary!(trait = Sub, fn = sub, for $name: $inner);

        impl_core_op_assign!(trait = AddAssign, fn = add_assign, for $name: $inner);
        impl_core_op_assign!(trait = BitXorAssign, fn = bitxor_assign, for $name: $inner);
        impl_core_op_assign!(trait = BitOrAssign, fn = bitor_assign, for $name: $inner);
        impl_core_op_assign!(trait = BitAndAssign, fn = bitand_assign, for $name: $inner);
        impl_core_op_assign!(trait = DivAssign, fn = div_assign, for $name: $inner);
        impl_core_op_assign!(trait = RemAssign, fn = rem_assign, for $name: $inner);
        impl_core_op_assign!(trait = ShlAssign, fn = shl_assign, for $name: $inner);
        impl_core_op_assign!(trait = ShrAssign, fn = shr_assign, for $name: $inner);
        impl_core_op_assign!(trait = SubAssign, fn = sub_assign, for $name: $inner);
        impl_core_op_assign!(trait = MulAssign, fn = mul_assign, for $name: $inner);

        impl_partial_eq_and_eq!(for $name: $inner);
        impl_partial_ord_and_ord!(for $name: $inner);

        impl_product_and_sum!(for $name);

        impl_fmt!(LowerExp for $name);
        impl_fmt!(LowerHex for $name);
        impl_fmt!(UpperExp for $name);
        impl_fmt!(UpperHex for $name);
        impl_fmt!(Octal for $name);
        impl_fmt!(Display for $name);
        impl_fmt!(Binary for $name);
        impl_fmt!(Debug for $name);

        impl_clone_copy!(for $name);
        impl_default!(for $name: $inner);
        impl_hash!(for $name);
        impl_from!(for $name: $inner);
    };
}

macro_rules! impl_float_traits {
    ($name:ident: $inner:ty) => {
        impl_core_op_unary!(trait = Neg, fn = neg, for $name: $inner);

        impl_core_op_binary!(trait = Add, fn = add, for $name: $inner);
        impl_core_op_binary!(trait = Div, fn = div, for $name: $inner);
        impl_core_op_binary!(trait = Mul, fn = mul, for $name: $inner);
        impl_core_op_binary!(trait = Rem, fn = rem, for $name: $inner);
        impl_core_op_binary!(trait = Sub, fn = sub, for $name: $inner);

        impl_core_op_assign!(trait = AddAssign, fn = add_assign, for $name: $inner);
        impl_core_op_assign!(trait = DivAssign, fn = div_assign, for $name: $inner);
        impl_core_op_assign!(trait = MulAssign, fn = mul_assign, for $name: $inner);
        impl_core_op_assign!(trait = RemAssign, fn = rem_assign, for $name: $inner);
        impl_core_op_assign!(trait = SubAssign, fn = sub_assign, for $name: $inner);

        impl_partial_eq_and_eq!(for $name: $inner);
        impl_partial_ord!(for $name: $inner);

        impl_product_and_sum!(for $name);

        impl_fmt!(LowerExp for $name);
        impl_fmt!(UpperExp for $name);
        impl_fmt!(Display for $name);
        impl_fmt!(Debug for $name);

        impl_clone_copy!(for $name);
        impl_default!(for $name: $inner);
        impl_from!(for $name: $inner);
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

/// Interconnect type.
pub unsafe trait ProtocolType: 'static + Sized {
    /// The referenced inner type.
    type Type<'de>: 'de;

    /// Writes zeroes to the padding for this type, if any.
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
    fn write_zero_padding(to: &mut MaybeUninit<Self>) {
        for i in 0..N {
            let item = unsafe { &mut *to.as_mut_ptr().cast::<MaybeUninit<T>>().add(i) };
            T::write_zero_padding(item);
        }
    }
}
