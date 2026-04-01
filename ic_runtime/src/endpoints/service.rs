use core::future::Future;

use crate::codec::decoder::Decoder;
use crate::codec::encode::Encode;
use crate::codec::encoder::Encoder;
use crate::codec::types::core::ProtocolType;
use crate::error::ProtocolResult;

// TODO: Support unsolicited server events?
pub trait CallContext<E: Encoder> {
    type CallID: Copy;

    fn call_id(&self) -> &Self::CallID;

    fn respond_with<'c, P, M>(
        &mut self,
        op: u64,
        message: &'c M,
    ) -> impl Future<Output = ProtocolResult<()>> + Send
    where
        P: ProtocolType<Limits = ()>,
        M: Sync,
        &'c M: Encode<P, E>;
}

pub trait Service {
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

    /// Informs the service to terminate its state machines and waits for completion.
    fn terminate(&self) -> impl Future<Output = ProtocolResult<()>> + Send;
}
