use crate::next::codec::reference::TypeRef;
use crate::next::error::ProtocolResult;
use crate::next::types::core::{
    TypeF32, TypeF64, TypeI8, TypeI16, TypeI32, TypeI64, TypeU8, TypeU16, TypeU32, TypeU64,
};

/// Type that doesn't have limits.
pub trait Unlimited {}

macro_rules! impl_unlimited_for {
    ($ty:ty) => {
        impl Unlimited for $ty {}
    };
}

impl_unlimited_for!(());
impl_unlimited_for!(bool);

impl_unlimited_for!(TypeI8);
impl_unlimited_for!(TypeI16);
impl_unlimited_for!(TypeI32);
impl_unlimited_for!(TypeI64);

impl_unlimited_for!(TypeU8);
impl_unlimited_for!(TypeU16);
impl_unlimited_for!(TypeU32);
impl_unlimited_for!(TypeU64);

impl_unlimited_for!(TypeF32);
impl_unlimited_for!(TypeF64);

impl<T> TypeLimits for T
where
    T: Unlimited,
{
    type Limits = ();

    #[inline]
    fn check_limits(_: TypeRef<'_, Self>, _: ()) -> ProtocolResult<()> {
        Ok(())
    }
}

/// Types implementing this trait can be checked against satisfying their limits.
///
/// This trait adds extra checking to the type, but it doesn't influence the verification of its representation.
pub trait TypeLimits {
    type Limits: Copy;

    /// Checks the type for satisfying its limits.
    fn check_limits(value: TypeRef<'_, Self>, limits: Self::Limits) -> ProtocolResult<()>;
}

impl<T, const N: usize> TypeLimits for [T; N]
where
    T: TypeLimits,
{
    type Limits = T::Limits;

    fn check_limits(mut value: TypeRef<'_, Self>, limits: Self::Limits) -> ProtocolResult<()> {
        // RT_ASSERT.
        // Safety: All values must have been initialized.
        let slice = unsafe { (value.as_ptr_mut() as *mut [T]).as_mut() }.unwrap();

        for i in slice {
            let value_i = unsafe { TypeRef::from_ptr_assume_init(i) };
            T::check_limits(value_i, limits)?;
        }

        Ok(())
    }
}
