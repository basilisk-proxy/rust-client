use thiserror::Error;

pub type ClientResult<T> = Result<T, ClientError>;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("protocol error {code}: {message}")]
    Protocol { code: String, message: String },
    #[error("protocol message missing expected field: {0}")]
    MissingField(&'static str),
    #[error("background channel closed")]
    ChannelClosed,
    #[error("unexpected protocol message type: {0}")]
    UnexpectedMessage(String),
}
