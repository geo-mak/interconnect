use std::future::Future;

use crate::error::{ErrKind, RpcError, RpcResult};

/// Trait for implementing RPC service handler.
/// Implementations must assume responsibility for managing access to shared resources.
pub trait RpcService: Send + Sync {
    /// Handles a method call and returns the result.
    /// By default, it returns `NotImplemented` error.
    fn handle_call(
        &self,
        _method: u16,
        _data: &[u8],
    ) -> impl Future<Output = RpcResult<Vec<u8>>> + Send {
        std::future::ready(Err(RpcError::error(ErrKind::NotImplemented)))
    }

    /// Handles a notification message.
    /// By default, it returns `NotImplemented` error.
    fn handle_notification(
        &self,
        _method: u16,
        _data: &[u8],
    ) -> impl Future<Output = RpcResult<()>> + Send {
        std::future::ready(Err(RpcError::error(ErrKind::NotImplemented)))
    }
}

impl RpcService for () {}
