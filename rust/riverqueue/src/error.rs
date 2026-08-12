//! Public errors.

use thiserror::Error;

/// Error returned by River operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Job arguments or options are invalid.
    #[error("invalid job: {0}")]
    InvalidJob(String),

    /// A running job cannot be deleted.
    #[error("running jobs cannot be deleted")]
    JobRunning,

    /// The requested record does not exist.
    #[error("not found")]
    NotFound,

    /// Background runtime task failed.
    #[error("runtime: {0}")]
    Runtime(String),

    /// JSON encoding or decoding failed.
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),

    /// PostgreSQL operation failed.
    #[error("PostgreSQL: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// A client with workers cannot insert an unregistered kind by default.
    #[error("job kind is not registered in the client's Workers bundle: {0}")]
    UnknownJobKind(String),
}

impl Error {
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn from_join(error: tokio::task::JoinError) -> Self {
        Self::Runtime(error.to_string())
    }
}
