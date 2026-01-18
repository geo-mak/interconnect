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
    /// Application-defined error.
    Application = 0,

    /// Unmapped transport-error.
    Transport = 1,

    Disconnected = 2,

    Canceled = 3,

    InvalidNegotiation = 4,

    SpecsMismatch = 5,

    KeyDerivation = 6,

    InvalidEncryptionKey = 7,

    Encryption = 8,

    Decryption = 9,

    Encoding = 10,

    Decoding = 11,

    MemoryAllocation = 12,

    RoundLimit = 13,

    CapacityLimit = 14,

    SendSizeLimit = 15,

    RecvSizeLimit = 16,

    Timeout = 17,

    UnexpectedMsg = 18,

    DroppedMessage = 19,

    Unidentified = 20,

    Unimplemented = 21,

    Validation = 22,

    NotEnoughData = 23,

    InvalidPadding = 24,
}

impl ErrKind {
    #[inline]
    pub fn from_byte(byte: u8) -> Option<Self> {
        use ErrKind::*;
        Some(match byte {
            0 => Application,
            1 => Transport,
            2 => Disconnected,
            3 => Canceled,
            4 => InvalidNegotiation,
            5 => SpecsMismatch,
            6 => KeyDerivation,
            7 => InvalidEncryptionKey,
            8 => Encryption,
            9 => Decryption,
            10 => Encoding,
            11 => Decoding,
            12 => MemoryAllocation,
            13 => RoundLimit,
            14 => CapacityLimit,
            15 => SendSizeLimit,
            16 => RecvSizeLimit,
            17 => Timeout,
            18 => UnexpectedMsg,
            19 => DroppedMessage,
            20 => Unidentified,
            21 => Unimplemented,
            22 => Validation,
            23 => NotEnoughData,
            24 => InvalidPadding,
            _ => return None,
        })
    }
}

/// Error type of protocol operations.
///
/// This type is designed to be very lightweight with the following scheme:
///
/// - Error: A representative error that can be direct or indirect/categorical.
/// - Reference: An extra context to the error as reference. `0` as value means `N/A` or `None`.
///
/// This design allows efficient matching of errors, at the same time it keeps
/// the error type simple and small to be used internally and over the wire.
///
/// For example, for returning an application-specific error, the kind can be set to `Application`
/// as category, and the actual error can be provided as reference to application-specific error's member.
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
                kind: ErrKind::Disconnected,
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

impl From<bincode::error::EncodeError> for ProtocolError {
    fn from(_: bincode::error::EncodeError) -> Self {
        ProtocolError {
            kind: ErrKind::Encoding,
            refer: 0,
        }
    }
}

impl From<bincode::error::DecodeError> for ProtocolError {
    #[inline]
    fn from(_: bincode::error::DecodeError) -> Self {
        ProtocolError {
            kind: ErrKind::Decoding,
            refer: 0,
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
