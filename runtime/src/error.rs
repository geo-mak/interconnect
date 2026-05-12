use core::fmt;

use std::collections::TryReserveError;
use std::io;

use tokio::time::error::Elapsed;

/// Result type alias for protocol's operations.
pub type ProtocolResult<T> = Result<T, ProtocolError>;

/// The variant of protocol's error.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum ErrKind {
    /// Unmapped transport-error.
    Transport,

    PeerClosed,

    Canceled,

    InvalidNegotiation,

    SpecsMismatch,

    KeyDerivation,

    InvalidKey,

    Encryption,

    Decryption,

    Encoding,

    Decoding,

    MemoryAllocation,

    RoundLimit,

    CapacityLimit,

    SendSizeLimit,

    RecvSizeLimit,

    Timeout,

    UnexpectedMsg,

    DroppedMessage,

    Unidentified,

    Unimplemented,

    Validation,

    NotEnoughData,

    InvalidPadding,

    InvalidPtrTag,
}

/// Error type of common runtime operations.
///
/// This type is designed to be very lightweight with the following scheme:
///
/// - Error: A representative error that can be direct or indirect/categorical.
/// - Reference: An extra context to the error as reference. `0` as value means `N/A` or `None`.
///
/// This design allows efficient matching of errors, at the same time it keeps the error type simple
/// and small.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProtocolError {
    pub kind: ErrKind,
    // errno is i32.
    pub refer: i32,
}

impl ProtocolError {
    #[inline(always)]
    pub const fn new(kind: ErrKind, refer: i32) -> Self {
        Self { kind, refer }
    }

    #[inline(always)]
    pub const fn error(kind: ErrKind) -> Self {
        Self::new(kind, 0)
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error: {:?}. Reference: {}", self.kind, self.refer)
    }
}

impl From<io::Error> for ProtocolError {
    #[inline]
    fn from(err: io::Error) -> Self {
        if err.kind() == std::io::ErrorKind::UnexpectedEof {
            ProtocolError {
                kind: ErrKind::PeerClosed,
                refer: 0,
            }
        } else {
            ProtocolError {
                kind: ErrKind::Transport,
                refer: err.raw_os_error().unwrap_or(0),
            }
        }
    }
}

impl From<Elapsed> for ProtocolError {
    #[inline]
    fn from(_: Elapsed) -> Self {
        ProtocolError {
            kind: ErrKind::Timeout,
            refer: 0,
        }
    }
}

impl From<TryReserveError> for ProtocolError {
    #[inline]
    fn from(_: TryReserveError) -> Self {
        ProtocolError {
            kind: ErrKind::MemoryAllocation,
            refer: 0,
        }
    }
}
