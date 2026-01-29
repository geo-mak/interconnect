use core::future::Future;

use crate::next::codec::decoder::Decoder;
use crate::next::codec::encode::Encode;
use crate::next::codec::encoder::Encoder;
use crate::next::error::{ErrKind, ProtocolError, ProtocolResult};
use crate::next::types::core::ProtocolType;
use crate::next::types::limits::TypeLimits;

// TODO: Support unsolicited errors?
pub trait CallContext<E: Encoder> {
    type CallID: Copy;

    fn call_id(&self) -> &Self::CallID;

    fn respond_with<'c, I, M>(
        &mut self,
        op: u64,
        message: &'c M,
    ) -> impl Future<Output = ProtocolResult<()>> + Send
    where
        I: ProtocolType + TypeLimits<Limits = ()>,
        M: Sync,
        &'c M: Encode<I, E>;
}

pub trait Application {
    fn call<E, M, C>(
        &self,
        op: u64,
        message: M,
        context: &mut C,
    ) -> impl Future<Output = ProtocolResult<()>> + Send
    where
        E: Encoder,
        M: Decoder + Send,
        C: CallContext<E> + Send;

    fn call_nullary<E, C>(
        &self,
        op: u64,
        context: &mut C,
    ) -> impl Future<Output = ProtocolResult<()>> + Send
    where
        E: Encoder,
        C: CallContext<E> + Send;

    /// Informs the application to terminate its state machines and waits for completion.
    /// By default, it returns `Unimplemented` error immediately.
    fn terminate(&self) -> impl Future<Output = ProtocolResult<()>> + Send {
        core::future::ready(Err(ProtocolError::error(ErrKind::Unimplemented)))
    }
}
