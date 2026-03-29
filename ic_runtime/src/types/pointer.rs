use core::marker::PhantomData;
use core::mem::MaybeUninit;

use munge::munge;

use crate::codec::reference::TypeRef;
use crate::error::{ErrKind, ProtocolError};
use crate::mem::BasicBlock;
use crate::types::core::TypeU64;

pub const PTR_TAG_NULL: u64 = 0;
pub const PTR_TAG_SET: u64 = u64::MAX;

/// The canonical tagged-pointer of Interconnect.
///
/// The pointer can be tagged as either `null` or `set`.
///
/// This type is used in fields to reference out-of-line data-structures that can be optional.
///
/// The optionality of types and their rules are still in the design-phase, but the key idea is that types can be
/// declared as optional, and the way to achieve this with minimum storage-cost, is to transform
/// any optional type into inlined tagged-pointer, and the actual data/payload shall be stored out-of-line.
/// This means that an optional field has a fixed "small" cost, in addition to variable cost when the pointer is `set`.
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
pub union TypeTaggedPtr<'de, T> {
    ptr_tag: TypeU64,
    ptr: *mut T,
    _seg_lf: PhantomData<&'de mut [BasicBlock]>,
}

unsafe impl<T: Send> Send for TypeTaggedPtr<'_, T> {}
unsafe impl<T: Sync> Sync for TypeTaggedPtr<'_, T> {}

impl<T> TypeTaggedPtr<'_, T> {
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
mod tests_tagged_ptr {
    use super::*;

    #[test]
    fn test_tagged_ptr_set_null_resolved() {
        let mut tagged_ptr = MaybeUninit::<TypeTaggedPtr<u8>>::uninit();

        // Tag as set.
        TypeTaggedPtr::encode_as_set(&mut tagged_ptr);
        let ptr_ref_set = unsafe { TypeRef::new_assume_init(&mut tagged_ptr) };
        assert_eq!(TypeTaggedPtr::is_null(ptr_ref_set), Ok(false));

        // Tag as null.
        TypeTaggedPtr::encode_as_null(&mut tagged_ptr);
        let ptr_ref_null = unsafe { TypeRef::new_assume_init(&mut tagged_ptr) };
        assert_eq!(TypeTaggedPtr::is_null(ptr_ref_null), Ok(true));

        // Invalid tag.
        unsafe {
            (*tagged_ptr.as_mut_ptr()).ptr_tag = TypeU64(12345);
            let ptr_ref_invalid = TypeRef::new_assume_init(&mut tagged_ptr);
            assert!(matches!(
                TypeTaggedPtr::is_null(ptr_ref_invalid),
                Err(ProtocolError {
                    kind: ErrKind::InvalidPtrTag,
                    ..
                })
            ));
        }

        // Ptr as resolved.
        let mut value = 10u8;
        let value_ptr: *mut u8 = &mut value;

        let ptr_ref_resolved = unsafe { TypeRef::new_assume_init(&mut tagged_ptr) };
        TypeTaggedPtr::set_pointer(ptr_ref_resolved, value_ptr);

        unsafe {
            assert_eq!(tagged_ptr.assume_init().ptr, value_ptr);
        }
    }
}
