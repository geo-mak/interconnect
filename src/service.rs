use std::future::Future;

use serde::{Deserialize, Serialize};

use crate::{
    Message,
    error::{ErrKind, RpcError, RpcResult},
};

#[derive(Debug, PartialEq)]
pub struct Call<'a> {
    pub method: u16,
    pub params: &'a [u8],
}

impl<'a> Call<'a> {
    /// Tries to decode the parameters as `P`.
    #[inline(always)]
    pub fn decode_as<P: for<'de> Deserialize<'de>>(&self) -> RpcResult<P> {
        Message::decode_from_slice(self.params)
    }
}

pub trait CallContext {
    type ID: Copy;

    /// The identifier of the call.
    fn id(&self) -> &Self::ID;

    /// Sends a reply message back to the caller.
    ///
    /// Unless noted otherwise by the implementation, this method is **not** safe to be canceled.
    fn send_reply<R: Serialize + Sync>(
        &mut self,
        reply: &R,
    ) -> impl Future<Output = RpcResult<()>> + Send;

    /// Sends an error message back to the caller.
    ///
    /// Unless noted otherwise by the implementation, this method is **not** safe to be canceled.
    fn send_error(&mut self, err: RpcError) -> impl Future<Output = RpcResult<()>> + Send;

    /// Sends a one-way call back to the caller.
    ///
    /// Unless noted otherwise by the implementation, this method is **not** safe to be canceled.
    ///
    /// By default, it returns `Unimplemented` error.
    fn call<P: Serialize + Sync>(
        &mut self,
        _method: u16,
        _params: &P,
    ) -> impl Future<Output = RpcResult<()>> + Send {
        std::future::ready(Err(RpcError::error(ErrKind::Unimplemented)))
    }

    /// Sends a one-way nullary call back to the caller.
    ///
    /// Unless noted otherwise by the implementation, this method is **not** safe to be canceled.
    ///
    /// By default, it returns `Unimplemented` error.
    fn call_nullary(&mut self, _method: u16) -> impl Future<Output = RpcResult<()>> + Send {
        std::future::ready(Err(RpcError::error(ErrKind::Unimplemented)))
    }
}

/// Trait for implementing RPC service handler.
///
/// The host may impose restrictions on the service implementation according to its needs.
pub trait RpcService {
    /// Handles a call to a method with parameters.
    ///
    /// Response to the call is optional and can be done via the context's methods.
    ///
    /// By default, it sends `Unimplemented` error to the caller.
    fn call<C>(
        &self,
        _call: Call<'_>,
        context: &mut C,
    ) -> impl Future<Output = RpcResult<()>> + Send
    where
        C: CallContext + Send,
    {
        context.send_error(RpcError::error(ErrKind::Unimplemented))
    }

    /// Handles a call to a nullary method.
    ///
    /// Response to the call is optional and can be done via the context's methods.
    ///
    /// By default, it sends `Unimplemented` error to the caller.
    fn call_nullary<C>(
        &self,
        _method: u16,
        context: &mut C,
    ) -> impl Future<Output = RpcResult<()>> + Send
    where
        C: CallContext + Send,
    {
        context.send_error(RpcError::error(ErrKind::Unimplemented))
    }

    /// Informs the service to terminate its state machines and waits for completion.
    /// By default, it returns immediately.
    fn shutdown(&self) -> impl Future<Output = RpcResult<()>> + Send {
        std::future::ready(Ok(()))
    }
}

impl RpcService for () {}
