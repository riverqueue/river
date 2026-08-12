//! Public errors.

use thiserror::Error;

/// A thread-safe error source whose concrete type can be inspected by callers.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

macro_rules! context_error {
    ($name:ident, $description:literal) => {
        #[doc = concat!("Structured ", $description, " error.")]
        #[derive(Debug, Error)]
        #[error("{context}: {message}")]
        pub struct $name {
            context: &'static str,
            message: String,
            source: Option<BoxError>,
        }

        impl $name {
            pub(crate) fn new(context: &'static str, message: impl Into<String>) -> Self {
                Self {
                    context,
                    message: message.into(),
                    source: None,
                }
            }

            /// Returns the operation or field being validated.
            #[must_use]
            pub const fn context(&self) -> &'static str {
                self.context
            }

            /// Returns the specific failure message.
            #[must_use]
            pub fn message(&self) -> &str {
                &self.message
            }
        }
    };
}

context_error!(ConfigurationError, "configuration");
context_error!(JobValidationError, "job validation");
context_error!(RuntimeError, "runtime");

/// Error returned by River operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Client, queue, subscription, or runtime configuration is invalid.
    #[error("invalid configuration: {0}")]
    Configuration(#[source] ConfigurationError),

    /// Job arguments or options are invalid.
    #[error("invalid job: {0}")]
    InvalidJob(#[source] JobValidationError),

    /// A database operation failed.
    #[error("database: {0}")]
    Database(#[source] BoxError),

    /// A transactional executor belongs to another database backend.
    #[error(transparent)]
    DatabaseMismatch(#[from] crate::database::DatabaseMismatch),

    /// A hook, middleware, or exact-version extension failed.
    #[error("extension {phase}: {source}")]
    Extension {
        /// Lifecycle phase in which the extension failed.
        phase: &'static str,
        /// Original extension error.
        #[source]
        source: BoxError,
    },

    /// A running job cannot be deleted.
    #[error("running jobs cannot be deleted")]
    JobRunning,

    /// The requested record does not exist.
    #[error("not found")]
    NotFound,

    /// Background runtime task failed.
    #[error("runtime: {0}")]
    Runtime(#[source] RuntimeError),

    /// An operation that spawns tasks was called outside Tokio.
    #[error("{operation} requires an active Tokio runtime")]
    RuntimeUnavailable {
        /// Operation that requires Tokio task spawning.
        operation: &'static str,
    },

    /// A spawned runtime task panicked or was cancelled.
    #[error("runtime task failed: {0}")]
    RuntimeTask(#[source] tokio::task::JoinError),

    /// A user-provided resumable step returned an error.
    #[error("resumable step {name:?}: {source}")]
    ResumableStep {
        /// Name of the step that failed.
        name: String,
        /// Original step error.
        #[source]
        source: BoxError,
    },

    /// JSON encoding or decoding failed.
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),

    /// A client with workers cannot insert an unregistered kind by default.
    #[error("job kind is not registered in the client's Workers bundle: {0}")]
    UnknownJobKind(String),
}

impl Error {
    pub(crate) fn configuration_context(context: &'static str, message: impl Into<String>) -> Self {
        Self::Configuration(ConfigurationError::new(context, message))
    }

    /// Creates an invalid client/configuration error.
    #[must_use]
    pub fn configuration(message: impl Into<String>) -> Self {
        Self::configuration_context("client configuration", message)
    }

    /// Wraps an error raised by a hook, middleware, or companion extension.
    pub fn extension(
        phase: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Extension {
            phase,
            source: Box::new(source),
        }
    }

    /// Creates an invalid job arguments/options error.
    #[must_use]
    pub fn invalid_job(message: impl Into<String>) -> Self {
        Self::invalid_job_context("job", message)
    }

    pub(crate) fn invalid_job_context(context: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidJob(JobValidationError::new(context, message))
    }

    /// Creates a runtime state error without an underlying source.
    #[must_use]
    pub fn runtime(message: impl Into<String>) -> Self {
        Self::runtime_context("runtime operation", message)
    }

    pub(crate) fn runtime_context(context: &'static str, message: impl Into<String>) -> Self {
        Self::Runtime(RuntimeError::new(context, message))
    }

    pub(crate) fn runtime_source(
        context: &'static str,
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Runtime(RuntimeError {
            context,
            message: message.into(),
            source: Some(Box::new(source)),
        })
    }

    pub(crate) const fn from_join(error: tokio::task::JoinError) -> Self {
        Self::RuntimeTask(error)
    }
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(Box::new(error))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn extension_preserves_concrete_source() {
        let error = Error::extension("test hook", std::io::Error::other("failed"));
        let source = error.source().unwrap();

        assert_eq!(source.to_string(), "failed");
        assert!(source.downcast_ref::<std::io::Error>().is_some());
    }

    #[test]
    fn sqlx_conversion_preserves_concrete_source() {
        let error = Error::from(sqlx::Error::RowNotFound);
        let source = error.source().unwrap();

        assert!(source.downcast_ref::<sqlx::Error>().is_some());
    }

    #[test]
    fn structured_runtime_error_preserves_context_and_source() {
        let error = Error::runtime_source(
            "resumable cursor",
            "cannot decode cursor",
            std::io::Error::other("bad JSON"),
        );
        let Error::Runtime(runtime) = &error else {
            panic!("expected runtime error");
        };

        assert_eq!(runtime.context(), "resumable cursor");
        assert_eq!(runtime.message(), "cannot decode cursor");
        let source = error.source().unwrap().source().unwrap();
        assert!(source.downcast_ref::<std::io::Error>().is_some());
    }
}
