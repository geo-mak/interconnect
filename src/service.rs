use std::future::Future;

use crate::error::{ErrKind, RpcError, RpcResult};
use crate::message::{Call, Notification, Reply};

/// Trait for implementing RPC service handler.
///
/// The host may impose restrictions on the service implementation according to its needs.
pub trait RpcService {
    /// Handles a call to a method with parameters.
    ///
    /// By default, it returns `NotImplemented` error.
    fn call(&self, _call: &Call) -> impl Future<Output = RpcResult<Reply>> + Send {
        std::future::ready(Err(RpcError::error(ErrKind::Unimplemented)))
    }

    /// Handles a call to a nullary method.
    ///
    /// By default, it returns `NotImplemented` error.
    fn nullary_call(&self, _method: u16) -> impl Future<Output = RpcResult<Reply>> + Send {
        std::future::ready(Err(RpcError::error(ErrKind::Unimplemented)))
    }

    /// Handles a notification message.
    ///
    /// By default, it returns `NotImplemented` error.
    fn notify(&self, _notification: &Notification) -> impl Future<Output = RpcResult<()>> + Send {
        std::future::ready(Err(RpcError::error(ErrKind::Unimplemented)))
    }

    /// Informs the service to terminate its state machines and waits for completion.
    /// By default, it returns immediately.
    fn shutdown(&self) -> impl Future<Output = RpcResult<()>> + Send {
        std::future::ready(Ok(()))
    }
}

impl RpcService for () {}
