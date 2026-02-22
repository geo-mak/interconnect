use core::mem::MaybeUninit;

use zerocopy::IntoBytes;

use crate::codec::decoder::Decoder;
use crate::codec::encoder::Encoder;
use crate::codec::reference::TypeRef;
use crate::codec::{decode::Decode, encode::Encode};
use crate::error::ProtocolResult;
use crate::types::core::{ProtocolType, TypeU64};
use crate::types::limits::{TypeLimits, Unlimited};

pub type MessageID = TypeU64;

pub type MessageDirective = TypeU64;

// TODO: Define bit-flags and their semantics.
// TODO: Should bit-falgs have their own fields?
/// 16-bytes header of Interconnect's message.
///
/// Header consists of two fields, each is 8-bytes in size and alignment.
#[derive(Debug, Clone, Copy, IntoBytes)]
#[repr(C)]
pub struct TypeMessageHeader {
    // TODO: Define id-rules.
    /// 8-bytes identifier of the message.
    pub id: MessageID,

    // TODO: Define directive-rules.
    /// 8-bytes directive of the message.
    pub directive: MessageDirective,
}

impl TypeMessageHeader {
    #[inline]
    pub const fn new(id: MessageID, directive: MessageDirective) -> Self {
        Self { id, directive }
    }

    pub fn encode_header<E: Encoder>(
        message_id: u64,
        directive: u64,
        encoder: &mut E,
    ) -> ProtocolResult<()> {
        let header = Self::new(TypeU64(message_id), TypeU64(directive));
        encoder.encode_next(header, ())
    }

    pub fn decode_header<D: Decoder>(mut decoder: &mut D) -> ProtocolResult<(u64, u64)> {
        let header = decoder.decode_associated_type::<Self>(())?;
        Ok((*header.id, *header.directive))
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
        _limits: <TypeMessageHeader as TypeLimits>::Limits,
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
        limits: <TypeMessageHeader as TypeLimits>::Limits,
    ) -> ProtocolResult<()> {
        Encode::encode(*self, encoder, inline_value, limits)
    }
}

unsafe impl<D: ?Sized> Decode<D> for TypeMessageHeader {
    fn decode(
        _value: TypeRef<'_, Self>,
        _decoder: &mut D,
        _limits: Self::Limits,
    ) -> ProtocolResult<()> {
        Ok(())
    }
}
