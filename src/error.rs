use std::fmt;
use std::io;

use serde::{Deserialize, Serialize};
use tokio::time::error::Elapsed;

/// Result type alias for RPC operations.
pub type RpcResult<T> = Result<T, RpcError>;

/// RPC error variant.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
#[repr(u8)]
pub enum ErrKind {
    /// Service-defined error.
    Service,

    /// Unmapped I/O error.
    IO,

    Disconnected,

    Canceled,

    InvalidNegotiation,

    CapabilityMismatch,

    KeyDerivation,

    InvalidKey,

    Encryption,

    Decryption,

    Encoding,

    Decoding,

    MaxLimit,

    Timeout,

    LargeMessage,

    UnexpectedMsg,

    DroppedMessage,

    Unidentified,

    Unimplemented,
}

/// Error type of RPC operations.
///
/// This type is designed to be very lightweight with the following scheme:
///
/// - Error: A representative error that can be direct or indirect/categorical.
/// - Reference: An extra context to the error as reference. `0` as value means `N/A` or `None`.
///
/// This scheme allows efficient matching of errors, at the same time it keeps
/// the error type simple and small to be used internally and over the wire.
///
/// For example, for reporting service-specific error, the kind can be set to `Service`
/// as category, and the actual error can be provided as reference to service-specific error's member.
///
/// For text-formatted error messages, helper functions can be used to provide formatted string
/// representation, in similar fashion to POSIX error-handling.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct RpcError {
    pub kind: ErrKind,
    // errno is i32.
    pub refer: i32,
}

impl RpcError {
    #[inline(always)]
    pub const fn new(kind: ErrKind, refer: i32) -> Self {
        Self { kind, refer }
    }

    #[inline(always)]
    pub const fn error(kind: ErrKind) -> Self {
        Self::new(kind, 0)
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error: {:?}. Reference: {}", self.kind, self.refer)
    }
}

impl From<io::Error> for RpcError {
    #[inline]
    fn from(err: io::Error) -> Self {
        if err.kind() == std::io::ErrorKind::UnexpectedEof {
            RpcError {
                kind: ErrKind::Disconnected,
                refer: 0,
            }
        } else {
            RpcError {
                kind: ErrKind::IO,
                refer: err.raw_os_error().unwrap_or(0),
            }
        }
    }
}

impl From<bincode::error::EncodeError> for RpcError {
    fn from(_: bincode::error::EncodeError) -> Self {
        RpcError {
            kind: ErrKind::Encoding,
            refer: 0,
        }
    }
}

impl From<bincode::error::DecodeError> for RpcError {
    #[inline]
    fn from(_: bincode::error::DecodeError) -> Self {
        RpcError {
            kind: ErrKind::Decoding,
            refer: 0,
        }
    }
}

impl From<Elapsed> for RpcError {
    #[inline]
    fn from(_: Elapsed) -> Self {
        RpcError {
            kind: ErrKind::Timeout,
            refer: 0,
        }
    }
}
