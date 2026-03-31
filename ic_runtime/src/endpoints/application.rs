use core::future::Future;

use crate::codec::decoder::Decoder;
use crate::codec::encode::Encode;
use crate::codec::encoder::Encoder;
use crate::codec::types::core::ProtocolType;
use crate::codec::types::limits::TypeLimits;
use crate::error::ProtocolResult;

// TODO: Support unsolicited server events?
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
    fn terminate(&self) -> impl Future<Output = ProtocolResult<()>> + Send;
}
