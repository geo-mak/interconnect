use core::mem::MaybeUninit;

use zerocopy::IntoBytes;

use crate::next::codec::{decode::Decode, encode::Encode};
use crate::next::error::ProtocolResult;
use crate::next::types::core::{ProtocolType, TypeU64};
use crate::next::types::limits::Unlimited;

pub const STD_MAX_MSG_SIZE: u32 = 4 * 1024 * 1024;

pub type MessageID = TypeU64;

pub type MessageDirective = TypeU64;

/// 16-bytes header of Interconnect's message.
///
/// Header consists of two fields, each is 8-bytes in size and alignment.
///
/// TODO: Both have embedded bit-flags.
#[derive(Debug, Clone, Copy, IntoBytes)]
#[repr(C)]
pub struct TypeMessageHeader {
    /// 8-bytes identifier of the message.
    ///
    /// TODO: Not all bytes are usable.
    pub id: MessageID,

    /// 8-bytes directive of the message.
    ///
    /// TODO: Not all bytes are usable.
    pub directive: MessageDirective,
}

impl TypeMessageHeader {
    #[inline]
    pub const fn new(id: MessageID, directive: MessageDirective) -> Self {
        Self { id, directive }
    }
}

unsafe impl ProtocolType for TypeMessageHeader {
    type Type<'de> = Self;

    #[inline]
    fn write_zero_padding(_to: &mut MaybeUninit<Self>) {}
}

impl Unlimited for TypeMessageHeader {}

unsafe impl<E: ?Sized> Encode<TypeMessageHeader, E> for TypeMessageHeader {
    fn encode(
        self,
        _encoder: &mut E,
        inline_value: &mut MaybeUninit<TypeMessageHeader>,
        _limits: <TypeMessageHeader as super::limits::TypeLimits>::Limits,
    ) -> ProtocolResult<()> {
        inline_value.write(self);
        Ok(())
    }
}

unsafe impl<E: ?Sized> Encode<TypeMessageHeader, E> for &TypeMessageHeader {
    fn encode(
        self,
        encoder: &mut E,
        inline_value: &mut MaybeUninit<TypeMessageHeader>,
        limits: <TypeMessageHeader as super::limits::TypeLimits>::Limits,
    ) -> ProtocolResult<()> {
        Encode::encode(*self, encoder, inline_value, limits)
    }
}

unsafe impl<D: ?Sized> Decode<D> for TypeMessageHeader {
    fn decode(
        _value: crate::next::codec::reference::TypeRef<'_, Self>,
        _decoder: &mut D,
        _limits: Self::Limits,
    ) -> ProtocolResult<()> {
        Ok(())
    }
}
