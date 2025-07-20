use std::borrow::Cow;
use std::fmt;
use std::io;

use serde::{Deserialize, Serialize};
use tokio::time::error::Elapsed;

/// Result type alias for RPC operations.
pub type RpcResult<T> = Result<T, RpcError>;

/// RPC error variant.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[repr(u8)]
pub enum ErrKind {
    Undefined,

    /// IO errors (network, file system, etc.).
    IO,

    ConnectionFailed,

    ConnectionClosed,

    Timeout,

    /// Encoding errors.
    Encoding,

    /// Decoding errors.
    Decoding,

    /// Violation of the protocol-specific invariants.
    Protocol,

    /// Unexpected message has been received/sent.
    UnexpectedMsg,

    /// Message size exceeds the allowed size by the protocol.
    LargeMessage,

    /// Errors originated from the service implementation.
    Service,

    /// Method not implemented/not found.
    NotImplemented,

    /// Errors originated from the server implementation.
    Server,

    /// Errors originated from the client implementation.
    Client,

    /// Client was not found.
    NoClient,
}

impl From<&'static str> for ErrCtx {
    #[inline]
    fn from(value: &'static str) -> Self {
        Self::Str(Cow::Borrowed(value))
    }
}

impl From<String> for ErrCtx {
    #[inline]
    fn from(value: String) -> Self {
        Self::Str(Cow::Owned(value))
    }
}

/// RPC error context.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ErrCtx {
    None,
    Code(i32),
    Str(Cow<'static, str>),
}

impl fmt::Display for ErrCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrCtx::None => write!(f, ""),
            ErrCtx::Code(code) => write!(f, "code={code}"),
            ErrCtx::Str(s) => write!(f, "{s}"),
        }
    }
}

/// Error type of RPC operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RpcError {
    pub kind: ErrKind,
    pub ctx: ErrCtx,
}

impl RpcError {
    #[inline(always)]
    pub const fn error(kind: ErrKind) -> Self {
        Self {
            kind,
            ctx: ErrCtx::None,
        }
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error: {:?}, Context: {}", self.kind, self.ctx)
    }
}

impl From<io::Error> for RpcError {
    #[inline]
    fn from(err: io::Error) -> Self {
        RpcError {
            kind: ErrKind::IO,
            ctx: ErrCtx::Str(Cow::Owned(err.to_string())),
        }
    }
}

impl From<bincode::error::EncodeError> for RpcError {
    fn from(err: bincode::error::EncodeError) -> Self {
        RpcError {
            kind: ErrKind::Encoding,
            ctx: ErrCtx::Str(Cow::Owned(err.to_string())),
        }
    }
}

impl From<bincode::error::DecodeError> for RpcError {
    #[inline]
    fn from(err: bincode::error::DecodeError) -> Self {
        RpcError {
            kind: ErrKind::Decoding,
            ctx: ErrCtx::Str(Cow::Owned(err.to_string())),
        }
    }
}

impl From<Elapsed> for RpcError {
    #[inline]
    fn from(_: Elapsed) -> Self {
        RpcError {
            kind: ErrKind::Timeout,
            ctx: ErrCtx::None,
        }
    }
}

impl Default for RpcError {
    #[inline]
    /// Creates undefined error without context.
    fn default() -> Self {
        Self {
            kind: ErrKind::Undefined,
            ctx: ErrCtx::None,
        }
    }
}
