use core::marker::PhantomData;
use core::mem::MaybeUninit;

use munge::munge;

use crate::next::codec::reference::TypeRef;
use crate::next::error::{ErrKind, ProtocolError};
use crate::next::mem::BasicBlock;
use crate::next::types::core::TypeU64;

pub const PTR_TAG_NULL: u64 = 0;
pub const PTR_TAG_SET: u64 = u64::MAX;

/// The canonical pointer-type of Interconnect.
///
/// This type is used in fields to reference out-of-line data-structures.
///
/// The pointer is tagged and can be either `null` or `set`.
///
/// This type is implemented as union with two members:
/// - The pointer-tag: Either "PTR_TAG_NULL" or "PTR_TAG_SET".
/// - The resolved pointer to the value in the decoding-segment during decoding.
///
/// Both are `8-bytes` in size.
///
/// Since this is a union type, tracking the current active member is manual.
///
/// # Safety
/// Dereferencing the pointer in wrong union-state will return either `null` pointer or an invalid pointer `u64::MAX`.
/// Either way this should lead to `segmentation/access violation`, but it is `UB` at the compiler-level.
#[repr(C, align(8))]
pub union TypePointer<'de, T> {
    ptr_tag: TypeU64,
    ptr: *mut T,
    _seg_lf: PhantomData<&'de mut [BasicBlock]>,
}

unsafe impl<T: Send> Send for TypePointer<'_, T> {}
unsafe impl<T: Sync> Sync for TypePointer<'_, T> {}

impl<T> TypePointer<'_, T> {
    pub fn is_null(storage: TypeRef<'_, Self>) -> Result<bool, ProtocolError> {
        unsafe {
            munge!(let Self { ptr_tag } = storage);
            match **ptr_tag {
                PTR_TAG_NULL => Ok(true),
                PTR_TAG_SET => Ok(false),
                _ => Err(ProtocolError::error(ErrKind::InvalidPtrTag)),
            }
        }
    }

    pub const fn as_ptr(&self) -> *const T {
        unsafe { self.ptr }
    }

    pub const fn as_ptr_mut(&mut self) -> *mut T {
        unsafe { self.ptr }
    }

    pub fn encode_as_set(storage: &mut MaybeUninit<Self>) {
        unsafe {
            munge!(let Self { ptr_tag } = storage);
            ptr_tag.write(TypeU64(PTR_TAG_SET));
        }
    }

    pub fn encode_as_null(storage: &mut MaybeUninit<Self>) {
        unsafe {
            munge!(let Self { ptr_tag } = storage);
            ptr_tag.write(TypeU64(PTR_TAG_NULL));
        }
    }

    pub fn set_pointer(storage: TypeRef<'_, Self>, resolved_ptr: *mut T) {
        unsafe {
            munge!(let Self { mut ptr } = storage);
            *ptr.as_ptr_mut() = resolved_ptr;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_as_ptr() {
        let mut value = 100u32;
        let value_ptr: *mut u32 = &mut value;

        let mut pointer_union = TypePointer { ptr: value_ptr };

        assert_eq!(pointer_union.as_ptr(), value_ptr);
        assert_eq!(pointer_union.as_ptr_mut(), value_ptr);
    }

    #[test]
    fn test_encode_set() {
        let mut storage = MaybeUninit::<TypePointer<u32>>::uninit();
        TypePointer::encode_as_set(&mut storage);

        unsafe {
            let ptr = storage.as_ptr();
            assert_eq!(*(*ptr).ptr_tag, PTR_TAG_SET);
        }
    }

    #[test]
    fn test_encode_null() {
        let mut storage = MaybeUninit::<TypePointer<u32>>::uninit();
        TypePointer::encode_as_null(&mut storage);

        unsafe {
            let ptr = storage.as_ptr();
            assert_eq!(*(*ptr).ptr_tag, PTR_TAG_NULL);
        }
    }

    #[test]
    fn test_set_resolved_ptr() {
        let mut value = 100u32;
        let resolved_ptr: *mut u32 = &mut value;

        let mut storage = MaybeUninit::<TypePointer<u32>>::uninit();
        TypePointer::encode_as_null(&mut storage);

        let type_ref = unsafe { TypeRef::new_assume_init(&mut storage) };
        TypePointer::set_pointer(type_ref, resolved_ptr);

        unsafe {
            let ptr = storage.as_ptr();
            assert_eq!((*ptr).ptr, resolved_ptr);
        }
    }

    #[test]
    fn test_is_null() {
        let mut storage_set = MaybeUninit::<TypePointer<u32>>::uninit();
        TypePointer::encode_as_set(&mut storage_set);

        let type_ref_set = unsafe { TypeRef::new_assume_init(&mut storage_set) };
        assert_eq!(TypePointer::is_null(type_ref_set), Ok(false));

        let mut storage_null = MaybeUninit::<TypePointer<u32>>::uninit();
        TypePointer::encode_as_null(&mut storage_null);
        let type_ref_null = unsafe { TypeRef::new_assume_init(&mut storage_null) };
        assert_eq!(TypePointer::is_null(type_ref_null), Ok(true));

        let mut storage_invalid = MaybeUninit::<TypePointer<u32>>::uninit();
        unsafe {
            (*storage_invalid.as_mut_ptr()).ptr_tag = TypeU64(12345);
        }
        let type_ref_invalid = unsafe { TypeRef::new_assume_init(&mut storage_invalid) };
        assert!(matches!(
            TypePointer::is_null(type_ref_invalid),
            Err(ProtocolError {
                kind: ErrKind::InvalidPtrTag,
                ..
            })
        ));
    }
}
