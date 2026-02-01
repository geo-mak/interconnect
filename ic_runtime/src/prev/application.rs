use std::future::Future;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::prev::error::{ErrKind, RpcError, RpcResult};
use crate::prev::message;

#[derive(Debug, PartialEq)]
pub struct Call<'a> {
    pub op: u16,
    pub params: &'a [u8],
}

impl<'a> Call<'a> {
    /// Tries to decode the parameters as `P` by borrowing data
    /// from the underlying store for type construction.
    #[inline(always)]
    pub fn decode_as<'de, P>(&'de self) -> RpcResult<P>
    where
        P: Deserialize<'de>,
    {
        message::decode_borrowed_from_slice(self.params)
    }

    /// Tries to decode the parameters as `P`.
    #[inline(always)]
    pub fn decode_owned_as<P>(&self) -> RpcResult<P>
    where
        P: DeserializeOwned,
    {
        message::decode_owned_from_slice(self.params)
    }
}

pub trait CallContext {
    type ID: Copy;

    /// The identifier of the call.
    fn id(&self) -> &Self::ID;

    /// Sends data as returning message back to the caller.
    ///
    /// Unless noted otherwise by the implementation, this method is **not** safe to be canceled.
    fn return_data<R: Serialize + Sync>(
        &mut self,
        data: &R,
    ) -> impl Future<Output = RpcResult<()>> + Send;

    /// Sends an error message back to the caller.
    ///
    /// Unless noted otherwise by the implementation, this method is **not** safe to be canceled.
    fn return_error(&mut self, error: RpcError) -> impl Future<Output = RpcResult<()>> + Send;

    /// Sends a one-way call back to the caller.
    ///
    /// Unless noted otherwise by the implementation, this method is **not** safe to be canceled.
    ///
    /// By default, it returns `Unimplemented` error.
    fn call<P: Serialize + Sync>(
        &mut self,
        _op: u16,
        _params: &P,
    ) -> impl Future<Output = RpcResult<()>> + Send {
        std::future::ready(Err(RpcError::error(ErrKind::Unimplemented)))
    }

    /// Sends a one-way nullary call back to the caller.
    ///
    /// Unless noted otherwise by the implementation, this method is **not** safe to be canceled.
    ///
    /// By default, it returns `Unimplemented` error.
    fn call_nullary(&mut self, _op: u16) -> impl Future<Output = RpcResult<()>> + Send {
        std::future::ready(Err(RpcError::error(ErrKind::Unimplemented)))
    }
}

/// Trait for implementing RPC applications.
///
/// RPC applications match and execute received calls.
///
/// The host may impose restrictions on the implementation of the application.
pub trait RpcApplication {
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
        context.return_error(RpcError::error(ErrKind::Unimplemented))
    }

    /// Handles a call to a nullary operation.
    ///
    /// Response to the call is optional and can be done via the context's methods.
    ///
    /// By default, it sends `Unimplemented` error to the caller.
    fn call_nullary<C>(
        &self,
        _op: u16,
        context: &mut C,
    ) -> impl Future<Output = RpcResult<()>> + Send
    where
        C: CallContext + Send,
    {
        context.return_error(RpcError::error(ErrKind::Unimplemented))
    }

    /// Informs the application to terminate its state machines and waits for completion.
    /// By default, it returns immediately.
    fn terminate(&self) -> impl Future<Output = RpcResult<()>> + Send {
        std::future::ready(Ok(()))
    }
}

impl RpcApplication for () {}
