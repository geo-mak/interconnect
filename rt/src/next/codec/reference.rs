use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::ops::{Deref, DerefMut};
use core::ptr::slice_from_raw_parts_mut;
use core::slice::from_raw_parts;

use munge::{Destructure, Move, Restructure};
use zerocopy::{FromBytes, IntoBytes};

/// A reference to a value.
///
/// The referenced value is **not** guaranteed to have correct representation of type `T`.
#[repr(transparent)]
pub struct TypeRef<'de, T: ?Sized> {
    ptr: *mut T,
    _t: PhantomData<&'de mut [u8]>,
}

unsafe impl<T: Send> Send for TypeRef<'_, T> {}
unsafe impl<T: Sync> Sync for TypeRef<'_, T> {}

impl<'de, T> TypeRef<'de, T>
where
    T: ?Sized,
{
    /// Returns a new instance backed by the given `storage`.
    ///
    /// This method shall be called when referencing new value.
    pub const fn new_assume_uninit(storage: &'de mut MaybeUninit<T>) -> Self
    where
        T: Sized,
    {
        unsafe {
            storage.as_mut_ptr().write_bytes(0, 1);
            Self::from_ptr(storage.as_mut_ptr())
        }
    }

    /// Creates a new instance from raw pointer.
    ///
    /// Safety:
    /// - The memory-location pointed to by the pointer must be fully initialized.
    /// - The pointer must be at offset aligned to the alignment of `T`.
    pub const unsafe fn from_ptr(ptr: *mut T) -> Self {
        Self {
            ptr,
            _t: PhantomData,
        }
    }

    /// Creates an a copy by borrowing `self` mutably.
    pub const fn as_mut(&mut self) -> TypeRef<'_, T> {
        Self {
            ptr: self.ptr,
            _t: PhantomData,
        }
    }

    /// Returns a pointer to the underlying value.
    ///
    /// Value pointed to by the pointer could be **invalid**.
    pub const fn as_ptr(&self) -> *const T {
        self.ptr
    }

    /// Returns a mutable pointer to the underlying potentially-invalid value.
    pub const fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr
    }

    /// Returns a reference to the underlying value.
    ///
    /// Safety: The referenced value must be ensured to be a valid value of type `T`.
    pub const unsafe fn deref_unchecked(&self) -> &T {
        unsafe { &*self.as_ptr() }
    }

    /// Returns a mutable reference to the underlying value.
    ///
    /// Safety: The referenced value must be ensured to be a valid value of type `T`.
    pub const unsafe fn deref_mut_unchecked(&mut self) -> &mut T {
        unsafe { &mut *self.as_mut_ptr() }
    }

    /// Writes a value to the memory location.
    pub const fn write(&mut self, value: T)
    where
        T: IntoBytes + Sized,
    {
        unsafe {
            self.as_mut_ptr().write(value);
        }
    }
}

impl<'de, T> TypeRef<'de, T>
where
    T: Sized,
{
    /// Creates a new instance from the given `storage`.
    ///
    /// Safety: The `storage` must fully **initialized** with valid value of type `T`.
    pub const unsafe fn new_assume_init(storage: &mut MaybeUninit<T>) -> Self {
        Self {
            ptr: storage.as_mut_ptr(),
            _t: PhantomData,
        }
    }
}

impl<T> TypeRef<'_, T> {
    /// Returns a slice of the underlying bytes.
    pub const fn as_bytes(&self) -> &[u8] {
        unsafe { from_raw_parts(self.ptr.cast::<u8>(), size_of::<T>()) }
    }
}

impl<T, const N: usize> TypeRef<'_, [T; N]> {
    /// Returns a instance referencing the element at the given `index`.
    pub const fn index(&mut self, index: usize) -> TypeRef<'_, T> {
        assert!(index < N, "Referencing out-of-bounds");
        TypeRef {
            ptr: unsafe { self.as_mut_ptr().cast::<T>().add(index) },
            _t: PhantomData,
        }
    }
}

impl<T> TypeRef<'_, [T]> {
    /// Creates new instance referencing slice from the given pointer.
    ///
    /// Safety:
    /// - The memory-location pointed to by the pointer must be fully initialized.
    /// - The pointer must be at offset aligned to the alignment of `T`.
    pub const unsafe fn new_slice_unchecked(ptr: *mut T, len: usize) -> Self {
        Self {
            ptr: slice_from_raw_parts_mut(ptr, len),
            _t: PhantomData,
        }
    }

    /// Returns a instance referencing the element at the given `index`.
    pub const fn index(&mut self, index: usize) -> TypeRef<'_, T> {
        assert!(index < self.ptr.len(), "Referencing out-of-bounds");
        TypeRef {
            ptr: unsafe { self.as_mut_ptr().cast::<T>().add(index) },
            _t: PhantomData,
        }
    }
}

impl<'de, T> Iterator for TypeRef<'de, [T]> {
    type Item = TypeRef<'de, T>;

    fn next(&mut self) -> Option<Self::Item> {
        let len = self.ptr.len();

        if len == 0 {
            return None;
        }

        unsafe {
            let slice = &mut *self.ptr;
            let first_ptr = slice.as_mut_ptr();

            // Advance the slice pointer.
            self.ptr = slice_from_raw_parts_mut(first_ptr.add(1), len - 1);

            Some(TypeRef {
                ptr: first_ptr,
                _t: PhantomData,
            })
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.ptr.len();
        (len, Some(len))
    }
}

impl<'de, T> ExactSizeIterator for TypeRef<'de, [T]> {
    fn len(&self) -> usize {
        self.ptr.len()
    }
}

impl<T: FromBytes> Deref for TypeRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.as_ptr() }
    }
}

impl<T: FromBytes> DerefMut for TypeRef<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.as_mut_ptr() }
    }
}

unsafe impl<T> Destructure for TypeRef<'_, T> {
    type Underlying = T;
    type Destructuring = Move;

    fn underlying(&mut self) -> *mut Self::Underlying {
        self.as_mut_ptr()
    }
}

unsafe impl<'de, T, U: 'de> Restructure<U> for TypeRef<'de, T> {
    type Restructured = TypeRef<'de, U>;

    unsafe fn restructure(&self, ptr: *mut U) -> Self::Restructured {
        TypeRef {
            ptr,
            _t: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::MaybeUninit;

    #[test]
    fn test_type_ref_write() {
        let mut storage = MaybeUninit::<u32>::uninit();
        let mut type_ref = TypeRef::new_assume_uninit(&mut storage);

        type_ref.write(42);
        assert_eq!(unsafe { *type_ref.deref_unchecked() }, 42);
        assert_eq!(type_ref.as_bytes(), 42u32.to_le_bytes());
    }

    #[test]
    fn test_type_ref_index() {
        // Test array indexing
        let mut storage = MaybeUninit::<[u32; 3]>::uninit();
        let mut type_ref = TypeRef::new_assume_uninit(&mut storage);
        type_ref.write([10, 20, 30]);

        assert_eq!(unsafe { *type_ref.index(0).deref_unchecked() }, 10);
        assert_eq!(unsafe { *type_ref.index(1).deref_unchecked() }, 20);
        assert_eq!(unsafe { *type_ref.index(2).deref_unchecked() }, 30);
    }

    #[test]
    fn test_type_ref_iterator() {
        let mut data = [10i32, 20i32, 30i32];
        let slice_ref = unsafe { TypeRef::new_slice_unchecked(data.as_mut_ptr(), 3) };

        let mut iter = slice_ref;
        assert_eq!(iter.len(), 3);
        assert_eq!(iter.size_hint(), (3, Some(3)));

        let first = iter.next().unwrap();
        assert_eq!(unsafe { *first.deref_unchecked() }, 10);
        assert_eq!(iter.len(), 2);

        let second = iter.next().unwrap();
        assert_eq!(unsafe { *second.deref_unchecked() }, 20);
        assert_eq!(iter.len(), 1);

        let third = iter.next().unwrap();
        assert_eq!(unsafe { *third.deref_unchecked() }, 30);
        assert_eq!(iter.len(), 0);

        assert!(iter.next().is_none());
    }
}
