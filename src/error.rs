use std::error::Error;
use std::fmt;
use std::io;

use tokio::time::error::Elapsed;

/// Result type alias for RPC operations.
pub type RpcResult<T> = Result<T, RpcError>;

/// Error type of RPC operations.
#[derive(Debug)]
pub enum RpcError {
    /// IO errors (network, file system, etc.).
    Io(io::Error),

    /// Encoding errors.
    Encoding(bincode::error::EncodeError),

    /// Decoding errors.
    Decoding(bincode::error::DecodeError),

    /// Timeout errors from tokio.
    Elapsed(Elapsed),

    /// General connection-related errors.
    Connection(String),

    /// Errors that violates the protocol-specific invariants.
    Protocol(String),

    /// Errors originated from the server implementation.
    Server(String),

    /// Errors originated from the client implementation.
    Client(String),

    InvalidRequest(String),

    Timeout,

    ConnectionClosed,

    /// Method not implemented/not found.
    NotImplemented(String),

    /// Method-originated error.
    Method(String),
}

impl RpcError {
    /// Creates a new connection error.
    pub fn connection<S: Into<String>>(msg: S) -> Self {
        Self::Connection(msg.into())
    }

    /// Creates a new protocol error.
    pub fn protocol<S: Into<String>>(msg: S) -> Self {
        Self::Protocol(msg.into())
    }

    /// Creates a new server error.
    pub fn server<S: Into<String>>(msg: S) -> Self {
        Self::Server(msg.into())
    }

    /// Creates a new client error.
    pub fn client<S: Into<String>>(msg: S) -> Self {
        Self::Client(msg.into())
    }

    /// Creates a method not implemented error.
    pub fn not_implemented<S: Into<String>>(method: S) -> Self {
        Self::NotImplemented(method.into())
    }

    /// Creates a new method error.
    pub fn method<S: Into<String>>(msg: S) -> Self {
        Self::Method(msg.into())
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RpcError::Io(err) => write!(f, "IO error: {}", err),
            RpcError::Encoding(err) => write!(f, "Encoding error: {}", err),
            RpcError::Decoding(err) => write!(f, "Decoding error: {}", err),
            RpcError::Elapsed(err) => write!(f, "Timeout elapsed: {}", err),
            RpcError::Connection(msg) => write!(f, "Connection error: {}", msg),
            RpcError::Protocol(msg) => write!(f, "Protocol error: {}", msg),
            RpcError::Server(msg) => write!(f, "Server error: {}", msg),
            RpcError::Client(msg) => write!(f, "Client error: {}", msg),
            RpcError::InvalidRequest(msg) => write!(f, "Invalid request: {}", msg),
            RpcError::Timeout => write!(f, "Timeout"),
            RpcError::ConnectionClosed => write!(f, "Connection closed"),
            RpcError::NotImplemented(msg) => write!(f, "Method not implemented: {}", msg),
            RpcError::Method(msg) => write!(f, "Method error: {}", msg),
        }
    }
}

impl Error for RpcError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RpcError::Io(err) => Some(err),
            RpcError::Encoding(err) => Some(err),
            RpcError::Decoding(err) => Some(err),
            RpcError::Elapsed(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for RpcError {
    fn from(err: io::Error) -> Self {
        RpcError::Io(err)
    }
}

impl From<bincode::error::EncodeError> for RpcError {
    fn from(err: bincode::error::EncodeError) -> Self {
        RpcError::Encoding(err)
    }
}

impl From<bincode::error::DecodeError> for RpcError {
    fn from(err: bincode::error::DecodeError) -> Self {
        RpcError::Decoding(err)
    }
}

impl From<Elapsed> for RpcError {
    fn from(err: Elapsed) -> Self {
        RpcError::Elapsed(err)
    }
}
