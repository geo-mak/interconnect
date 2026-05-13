use core::mem::MaybeUninit;

use zerocopy::IntoBytes;

use crate::codec::decoder::Decoder;
use crate::codec::encoder::Encoder;
use crate::codec::reference::TypeRef;
use crate::codec::types::core::{ProtocolType, TypeU64};
use crate::codec::types::limits::TypeLimits;
use crate::codec::{decode::Decode, encode::Encode};
use crate::error::ICResult;

pub type MessageID = TypeU64;
pub type MessageDirective = TypeU64;

// TODO: Define bit-flags and their semantics.
// TODO: Should bit-flags have their own fields?
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
    ) -> ICResult<()> {
        let header = Self::new(TypeU64(message_id), TypeU64(directive));
        encoder.encode_next(header, ())
    }

    pub fn decode_header<D: Decoder>(mut decoder: &mut D) -> ICResult<(u64, u64)> {
        let header = decoder.decode_ref::<Self>(())?;
        Ok((*header.id, *header.directive))
    }
}

impl TypeLimits for TypeMessageHeader {
    type Limits = ();

    #[inline]
    fn check_limits(_: TypeRef<'_, Self>, _: ()) -> ICResult<()> {
        Ok(())
    }
}

unsafe impl ProtocolType for TypeMessageHeader {
    type Type<'de> = Self;

    #[inline]
    fn set_padding_zeros(_: &mut MaybeUninit<Self>) {}
}

unsafe impl<E: ?Sized> Encode<TypeMessageHeader, E> for TypeMessageHeader {
    fn encode(
        self,
        _: &mut E,
        storage: &mut MaybeUninit<TypeMessageHeader>,
        _: <TypeMessageHeader as TypeLimits>::Limits,
    ) -> ICResult<()> {
        storage.write(self);
        Ok(())
    }
}

unsafe impl<E: ?Sized> Encode<TypeMessageHeader, E> for &TypeMessageHeader {
    fn encode(
        self,
        encoder: &mut E,
        storage: &mut MaybeUninit<TypeMessageHeader>,
        limits: <TypeMessageHeader as TypeLimits>::Limits,
    ) -> ICResult<()> {
        Encode::encode(*self, encoder, storage, limits)
    }
}

unsafe impl<D: ?Sized> Decode<D> for TypeMessageHeader {
    fn decode(_: TypeRef<'_, Self>, _: &mut D, _: Self::Limits) -> ICResult<()> {
        Ok(())
    }
}
