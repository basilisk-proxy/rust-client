use thiserror::Error;

/// Standard result type used by service-bus client operations.
pub type ClientResult<T> = Result<T, ClientError>;

/// Error type returned by low-level bus operations.
#[derive(Debug, Error)]
pub enum ClientError {
    /// I/O failure while reading from or writing to the TCP socket.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization or deserialization failure for protocol frames.
    #[error("JSON serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    /// Protocol-level error frame returned by the service bus.
    #[error("protocol error {code}: {message}")]
    Protocol { code: String, message: String },
    /// A required field was missing from a received frame.
    #[error("protocol message missing expected field: {0}")]
    MissingField(&'static str),
    /// Internal request/response channel closed unexpectedly.
    #[error("background channel closed")]
    ChannelClosed,
    /// Received a frame type that does not match the expected response.
    #[error("unexpected protocol message type: {0}")]
    UnexpectedMessage(String),
}
