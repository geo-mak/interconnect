use core::future::Future;

use crate::codec::decoder::Decoder;
use crate::codec::encode::Encode;
use crate::codec::encoder::Encoder;
use crate::codec::types::core::ProtocolType;
use crate::error::ICResult;

// TODO: Support unsolicited server events?
pub trait CallContext<E: Encoder> {
    type CallID: Copy;

    fn call_id(&self) -> &Self::CallID;

    fn respond_with<'c, P, M>(
        &mut self,
        op: u64,
        message: &'c M,
    ) -> impl Future<Output = ICResult<()>> + Send
    where
        P: ProtocolType<Limits = ()>,
        M: Sync,
        &'c M: Encode<P, E>;
}

pub trait Session<'a> {
    fn call<E, M, C>(
        &self,
        op: u64,
        message: M,
        context: &mut C,
    ) -> impl Future<Output = ICResult<()>> + Send
    where
        E: Encoder,
        M: Decoder + Send,
        C: CallContext<E> + Send;

    fn call_nullary<E, C>(
        &self,
        op: u64,
        context: &mut C,
    ) -> impl Future<Output = ICResult<()>> + Send
    where
        E: Encoder,
        C: CallContext<E> + Send;
}

// TODO: Solving the identity-problem will solve the last puzzle in the chain.
/// A type that stores the components of the service implementation and creates service's sessions.
pub trait SessionServer {
    type Parameters;

    type Session<'a>: Session<'a>
    where
        Self: 'a;

    // Creates new session with the provided parameters.
    fn create<'a>(&'a self, parameters: Self::Parameters) -> Self::Session<'a>;

    /// Informs the service to terminate its state machines and waits for completion.
    fn terminate(&self) -> impl Future<Output = ICResult<()>> + Send;
}
