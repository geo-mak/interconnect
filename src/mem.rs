use std::marker::PhantomData;

pub const SIZE_OF_U8: usize = size_of::<u8>();
pub const ALIGN_OF_U8: usize = align_of::<u8>();

pub const SIZE_OF_I8: usize = size_of::<i8>();
pub const ALIGN_OF_I8: usize = align_of::<i8>();

pub const SIZE_OF_U16: usize = size_of::<u16>();
pub const ALIGN_OF_U16: usize = align_of::<u16>();

pub const SIZE_OF_I16: usize = size_of::<i16>();
pub const ALIGN_OF_I16: usize = align_of::<i16>();

pub const SIZE_OF_U32: usize = size_of::<u32>();
pub const ALIGN_OF_U32: usize = align_of::<u32>();

pub const SIZE_OF_I32: usize = size_of::<i32>();
pub const ALIGN_OF_I32: usize = align_of::<i32>();

pub const SIZE_OF_U64: usize = size_of::<u64>();
pub const ALIGN_OF_U64: usize = align_of::<u64>();

pub const SIZE_OF_I64: usize = size_of::<i64>();
pub const ALIGN_OF_I64: usize = align_of::<i64>();

pub const SIZE_OF_F32: usize = size_of::<f32>();
pub const ALIGN_OF_F32: usize = align_of::<f32>();

pub const SIZE_OF_F64: usize = size_of::<f64>();
pub const ALIGN_OF_F64: usize = align_of::<f64>();

pub const SIZE_OF_S_OFFSET: usize = SIZE_OF_I32;
pub const SIZE_OF_U_OFFSET: usize = SIZE_OF_U32;

mod private {
    /// A type where any combination of its bits represents a valid value of it.
    pub trait UnconstrainedTransmute {}

    impl UnconstrainedTransmute for u8 {}
    impl UnconstrainedTransmute for i8 {}

    impl UnconstrainedTransmute for u16 {}
    impl UnconstrainedTransmute for i16 {}

    impl UnconstrainedTransmute for u32 {}
    impl UnconstrainedTransmute for i32 {}

    impl UnconstrainedTransmute for u64 {}
    impl UnconstrainedTransmute for i64 {}
}

pub trait Primitive: Sized + PartialEq + Copy + Clone {
    type Value: private::UnconstrainedTransmute;

    fn to_le(self) -> Self::Value;
    fn from_le(value: Self::Value) -> Self;
}

/// This trait enables offset-based access to data in the linear memory.
pub trait ReadAligned<'a> {
    type Target;

    /// Reads a value at the provided offset in the provided source data.
    ///
    /// Safety:
    /// The provided data at the provided offset must contain a valid value of `Self::Target`.
    /// This applies also to everything it transitively points to by offset.
    unsafe fn read_aligned_at(offset: usize, src: &'a [u8]) -> Self::Target;
}

/// Trait for types that write properly-aligned values values into slice.
pub trait WriteAligned: Sized {
    type Output;

    const OUTPUT_SIZE: usize = size_of::<Self::Output>();
    const OUTPUT_ALIGN: usize = align_of::<Self::Output>();

    /// Writes `Self::Output` to the provided slice with proper alignment.
    ///
    /// Safety:
    /// - `aligned_dst` must have length greater than or equal to `Self::OUTPUT_SIZE`.
    /// - `aligned_dst` must be aligned to `Self::OUTPUT_ALIGN`.
    unsafe fn write_aligned_into(&self, aligned_dst: &mut [u8], current_len: usize);
}

impl<'a, T: WriteAligned> WriteAligned for &'a T {
    type Output = T::Output;

    unsafe fn write_aligned_into(&self, aligned_output: &mut [u8], current_len: usize) {
        unsafe { T::write_aligned_into(self, aligned_output, current_len) }
    }
}

/// Reads a value from the provided slice at the specified `offset`.
///
/// Performs endian conversion, if necessary.
///
/// Safety:
/// Length of the source slice must be greater or equal to the `offset` + size of `T`.
#[inline]
pub unsafe fn read_aligned_at<T: Primitive>(offset: usize, src: &[u8]) -> T {
    unsafe { read_aligned_from(&src[offset..]) }
}

/// Reads a value from the provided byte slice.
///
/// Performs endian conversion, if necessary.
///
/// Safety:
/// Length of the source slice must be greater or equal to the size of `T`.
#[inline]
pub unsafe fn read_aligned_from<T: Primitive>(src: &[u8]) -> T {
    let size = size_of::<T::Value>();

    debug_assert!(
        src.len() >= size,
        "Capacity mismatch for the requested read, needed {} got {}",
        size,
        src.len()
    );

    let mut mem = core::mem::MaybeUninit::<T::Value>::uninit();

    unsafe {
        core::ptr::copy_nonoverlapping(src.as_ptr(), mem.as_mut_ptr() as *mut u8, size);
        T::from_le(mem.assume_init())
    }
}

/// Writes the provided value into the provided slice.
///
/// Performs endian conversion if required.
///
/// Safety:
/// Length of the output slice must be greater or equal to the size of `T`.
#[inline]
pub unsafe fn write_aligned_into<T: Primitive>(output: &mut [u8], value: T) {
    let size = size_of::<T::Value>();

    debug_assert!(
        output.len() >= size,
        "insufficient capacity, needed {} got {}",
        size,
        output.len()
    );

    let value_le = value.to_le();

    unsafe {
        core::ptr::copy_nonoverlapping(
            &value_le as *const T::Value as *const u8,
            output.as_mut_ptr() as *mut u8,
            size,
        );
    }
}

macro_rules! impl_primitive {
    ($ty:ident) => {
        impl Primitive for $ty {
            type Value = Self;

            #[inline(always)]
            fn to_le(self) -> Self::Value {
                Self::to_le(self)
            }

            #[inline(always)]
            fn from_le(v: Self::Value) -> Self {
                Self::from_le(v)
            }
        }
    };
}

impl Primitive for bool {
    type Value = u8;

    #[inline(always)]
    fn to_le(self) -> Self::Value {
        self as u8
    }

    #[inline(always)]
    fn from_le(value: Self::Value) -> Self {
        value != 0
    }
}

impl Primitive for f32 {
    type Value = u32;

    #[inline(always)]
    fn to_le(self) -> u32 {
        self.to_bits().to_le()
    }

    #[inline(always)]
    fn from_le(value: u32) -> Self {
        f32::from_bits(u32::from_le(value))
    }
}

impl Primitive for f64 {
    type Value = u64;

    #[inline(always)]
    fn to_le(self) -> u64 {
        self.to_bits().to_le()
    }

    #[inline(always)]
    fn from_le(value: u64) -> Self {
        f64::from_bits(u64::from_le(value))
    }
}

impl_primitive!(u8);
impl_primitive!(i8);
impl_primitive!(u16);
impl_primitive!(i16);
impl_primitive!(u32);
impl_primitive!(i32);
impl_primitive!(u64);
impl_primitive!(i64);

macro_rules! impl_read_aligned_for_primitive {
    ($ty:ident) => {
        impl<'a> ReadAligned<'a> for $ty {
            type Target = $ty;

            #[inline(always)]
            unsafe fn read_aligned_at(offset: usize, src: &'a [u8]) -> Self::Target {
                unsafe { read_aligned_at::<$ty>(offset, src) }
            }
        }
    };
}

impl<'a> ReadAligned<'a> for bool {
    type Target = bool;

    #[inline(always)]
    unsafe fn read_aligned_at(offset: usize, src: &'a [u8]) -> Self::Target {
        (unsafe { read_aligned_at::<u8>(offset, src) }) != 0
    }
}

impl_read_aligned_for_primitive!(u8);
impl_read_aligned_for_primitive!(i8);
impl_read_aligned_for_primitive!(u16);
impl_read_aligned_for_primitive!(i16);
impl_read_aligned_for_primitive!(u32);
impl_read_aligned_for_primitive!(i32);
impl_read_aligned_for_primitive!(u64);
impl_read_aligned_for_primitive!(i64);
impl_read_aligned_for_primitive!(f32);
impl_read_aligned_for_primitive!(f64);

macro_rules! impl_write_aligned_for_primitive {
    ($ty:ident) => {
        impl WriteAligned for $ty {
            type Output = $ty;

            #[inline]
            unsafe fn write_aligned_into(&self, aligned_dst: &mut [u8], _current_len: usize) {
                unsafe { write_aligned_into::<$ty>(aligned_dst, *self) }
            }
        }
    };
}

impl_write_aligned_for_primitive!(bool);
impl_write_aligned_for_primitive!(u8);
impl_write_aligned_for_primitive!(i8);
impl_write_aligned_for_primitive!(u16);
impl_write_aligned_for_primitive!(i16);
impl_write_aligned_for_primitive!(u32);
impl_write_aligned_for_primitive!(i32);
impl_write_aligned_for_primitive!(u64);
impl_write_aligned_for_primitive!(i64);
impl_write_aligned_for_primitive!(f32);
impl_write_aligned_for_primitive!(f64);

/// A `signed` relative pointer.
pub type SOffset = i32;

/// An `unsigned` relative pointer.
///
/// Used to represent both the relative pointers and lengths of vectors and strings.
pub type UOffset = u32;

/// A `SOffset` that supports typed read/write via `ReadAligned` and `WriteAligned` respectively.
#[derive(Debug)]
pub struct TypedSOffset<T>(SOffset, PhantomData<T>);

impl<T> TypedSOffset<T> {
    #[inline(always)]
    pub fn value(&self) -> SOffset {
        self.0
    }
}

impl<'a, T: ReadAligned<'a>> ReadAligned<'a> for TypedSOffset<T> {
    type Target = T::Target;

    #[inline(always)]
    unsafe fn read_aligned_at(offset: usize, src: &'a [u8]) -> Self::Target {
        let slice = &src[offset..offset + SIZE_OF_S_OFFSET];
        unsafe {
            let skip_offset = read_aligned_from::<SOffset>(slice);
            T::read_aligned_at((offset as SOffset - skip_offset) as usize, src)
        }
    }
}

impl<T> WriteAligned for TypedSOffset<T> {
    type Output = Self;

    #[inline]
    unsafe fn write_aligned_into(&self, dst: &mut [u8], written_len: usize) {
        unsafe { self.value().write_aligned_into(dst, written_len) };
    }
}

/// An `UOffset` that supports typed read/write via `ReadAligned` and `WriteAligned` respectively.
#[derive(Debug)]
pub struct TypedUOffset<T>(UOffset, PhantomData<T>);

impl<T> Copy for TypedUOffset<T> {}

impl<T> Clone for TypedUOffset<T> {
    #[inline(always)]
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> TypedUOffset<T> {
    #[inline(always)]
    pub fn value(self) -> UOffset {
        self.0
    }
}

impl<'a, T: ReadAligned<'a>> ReadAligned<'a> for TypedUOffset<T> {
    type Target = T::Target;

    #[inline(always)]
    unsafe fn read_aligned_at(offset: usize, src: &'a [u8]) -> Self::Target {
        let slice = &src[offset..offset + SIZE_OF_U_OFFSET];

        unsafe {
            let skip_offset = read_aligned_from::<u32>(slice) as usize;
            T::read_aligned_at(offset + skip_offset, src)
        }
    }
}

impl<T> WriteAligned for TypedUOffset<T> {
    type Output = Self;

    #[inline(always)]
    unsafe fn write_aligned_into(&self, dst: &mut [u8], current_len: usize) {
        unsafe { self.value().write_aligned_into(dst, current_len) };
    }
}
