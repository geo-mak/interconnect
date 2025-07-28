use std::future::Future;

use crate::error::{ErrKind, RpcError, RpcResult};
use crate::message::{Call, Notification, Reply};

/// Trait for implementing RPC service handler.
/// Implementations must assume responsibility for managing access to shared resources.
pub trait RpcService: Send + Sync + Clone + 'static {
    /// Handles a method call and returns the result.
    /// By default, it returns `NotImplemented` error.
    fn call(&self, _call: &Call) -> impl Future<Output = RpcResult<Reply>> + Send {
        std::future::ready(Err(RpcError::error(ErrKind::NotImplemented)))
    }

    /// Handles a notification message.
    /// By default, it returns `NotImplemented` error.
    fn notify(&self, _notification: &Notification) -> impl Future<Output = RpcResult<()>> + Send {
        std::future::ready(Err(RpcError::error(ErrKind::NotImplemented)))
    }
}

impl RpcService for () {}
