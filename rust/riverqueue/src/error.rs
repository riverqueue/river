//! Public errors.
//!
//! Every error in this crate either describes itself and exposes what caused
//! it through [`std::error::Error::source`], or is transparent and delegates
//! both its message and its source to the error it wraps. None does both, so
//! a report that prints the whole chain, such as `anyhow`'s `{:#}`, never
//! repeats a message.

use std::fmt;

use thiserror::Error;

use crate::JobState;

/// A thread-safe error source whose concrete type can be inspected by callers.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

macro_rules! context_error {
    ($(#[$meta:meta])* $name:ident, $format:literal) => {
        $(#[$meta])*
        #[derive(Debug, Error)]
        #[error($format)]
        pub struct $name {
            context: &'static str,
            message: String,
            #[source]
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

            /// Returns the operation or field the error is about.
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

context_error!(
    /// Invalid client, queue, subscription, or maintenance configuration.
    ConfigurationError,
    "invalid {context} configuration: {message}"
);
context_error!(
    /// Invalid job arguments or insertion options.
    JobValidationError,
    "invalid {context}: {message}"
);
context_error!(
    /// A failure of River's runtime, with the operation it happened in.
    RuntimeError,
    "{context}: {message}"
);

/// The kind of record an [`Error::NotFound`] refers to, with the key that
/// was looked up.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Record {
    /// A job, by ID.
    Job(i64),
    /// A queue, by name.
    Queue(String),
}

impl fmt::Display for Record {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Job(id) => write!(formatter, "job {id}"),
            Self::Queue(name) => write!(formatter, "queue {name:?}"),
        }
    }
}

/// Where an extension that failed was running.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ExtensionPhase {
    /// An add-on crate's service running on every started client.
    AddOnRuntimeService,
    /// An add-on crate claiming jobs for a fetch.
    AddOnFetchClaim,
    /// An add-on crate selecting jobs for River to claim in a fetch.
    AddOnFetchSelection,
    /// An add-on crate's insertion step.
    AddOnInsertion,
    /// An add-on crate's step after a job is cancelled.
    AddOnJobCancel,
    /// An add-on crate's step after a job is retried.
    AddOnJobRetry,
    /// An add-on crate's step after job state changes are persisted.
    AddOnJobSetState,
    /// An add-on crate's rescue of stuck jobs.
    AddOnRescue,
    /// An add-on crate selecting stuck jobs to rescue.
    AddOnRescueSelection,
    /// An [`ErrorHandler::handle_error`](crate::ErrorHandler::handle_error)
    /// call.
    ErrorHandler,
    /// A [`Hook::insert_begin`](crate::Hook::insert_begin) hook.
    InsertBeginHook,
    /// An [`InsertMiddleware`](crate::InsertMiddleware).
    InsertMiddleware,
    /// A [`Hook::decode_insert_result`](crate::Hook::decode_insert_result)
    /// hook.
    InsertResultDecodeHook,
    /// A [`Hook::metric_emit`](crate::Hook::metric_emit) hook.
    MetricEmitHook,
    /// A [`Hook::periodic_jobs_start`](crate::Hook::periodic_jobs_start)
    /// hook.
    PeriodicJobsStartHook,
    /// An [`ErrorHandler::handle_stuck`](crate::ErrorHandler::handle_stuck)
    /// call.
    StuckJobHandler,
}

impl fmt::Display for ExtensionPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AddOnRuntimeService => "add-on runtime service",
            Self::AddOnFetchClaim => "add-on fetch claim",
            Self::AddOnFetchSelection => "add-on fetch selection",
            Self::AddOnInsertion => "add-on job insertion",
            Self::AddOnJobCancel => "add-on job cancel",
            Self::AddOnJobRetry => "add-on job retry",
            Self::AddOnJobSetState => "add-on job set state",
            Self::AddOnRescue => "add-on rescue",
            Self::AddOnRescueSelection => "add-on rescue selection",
            Self::ErrorHandler => "error handler",
            Self::InsertBeginHook => "insert begin hook",
            Self::InsertMiddleware => "insert middleware",
            Self::InsertResultDecodeHook => "insert result decode hook",
            Self::MetricEmitHook => "metric hook",
            Self::PeriodicJobsStartHook => "periodic jobs start hook",
            Self::StuckJobHandler => "stuck job handler",
        })
    }
}

/// Error returned by River operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The client is already running; a client runs once at a time.
    #[error("client is already running")]
    AlreadyRunning,

    /// The client stopped before reaching the state being waited for, such as
    /// readiness in [`RunHandle::wait_ready`](crate::RunHandle::wait_ready).
    #[error("client stopped")]
    ClientStopped,

    /// Client, queue, subscription, or maintenance configuration is invalid.
    #[error(transparent)]
    Configuration(ConfigurationError),

    /// A database operation failed.
    ///
    /// The payload is SQLx's error on either backend, so a caller can match a
    /// failure such as [`sqlx::Error::PoolTimedOut`] or inspect
    /// [`sqlx::Error::Database`] for a constraint violation. A stored row
    /// River can't decode is reported as [`sqlx::Error::Decode`] or
    /// [`sqlx::Error::ColumnDecode`].
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    /// A transactional executor belongs to another database backend.
    #[error(transparent)]
    DatabaseMismatch(#[from] crate::database::DatabaseMismatch),

    /// A hook, middleware, or add-on crate failed.
    #[error("{phase} failed")]
    Extension {
        /// Where the extension was running.
        phase: ExtensionPhase,
        /// Original extension error.
        #[source]
        source: BoxError,
    },

    /// Job arguments or options are invalid.
    #[error(transparent)]
    InvalidJob(JobValidationError),

    /// JSON encoding or decoding failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// The operation needs a running job, such as completing it in a
    /// transaction, but the job is in another state.
    #[error("job is {state}, not running")]
    JobNotRunning {
        /// The job's current state.
        state: JobState,
    },

    /// A running job cannot be deleted.
    #[error("running jobs cannot be deleted")]
    JobRunning,

    /// The requested record does not exist.
    #[error("{0} not found")]
    NotFound(Record),

    /// A user-provided resumable step returned an error.
    #[error("resumable step {name:?} failed")]
    ResumableStep {
        /// Name of the step that failed.
        name: String,
        /// Original step error.
        #[source]
        source: BoxError,
    },

    /// River's runtime failed.
    #[error(transparent)]
    Runtime(RuntimeError),

    /// An operation that spawns tasks was called outside Tokio.
    #[error("{operation} requires an active Tokio runtime")]
    RuntimeUnavailable {
        /// Operation that requires Tokio task spawning.
        operation: &'static str,
    },

    /// A spawned runtime task panicked or was cancelled.
    #[error(transparent)]
    RuntimeTask(tokio::task::JoinError),

    /// A client with workers cannot insert an unregistered kind by default.
    #[error("job kind is not registered in the client's Workers bundle: {0}")]
    UnknownJobKind(String),
}

impl Error {
    pub(crate) fn configuration(message: impl Into<String>) -> Self {
        Self::configuration_context("client", message)
    }

    pub(crate) fn configuration_context(context: &'static str, message: impl Into<String>) -> Self {
        Self::Configuration(ConfigurationError::new(context, message))
    }

    /// Wraps an error raised by a hook, middleware, or add-on crate.
    ///
    /// Insert middleware uses this to fail an insertion with its own error:
    ///
    /// ```
    /// use riverqueue::{Error, ExtensionPhase};
    ///
    /// let error = Error::extension(ExtensionPhase::InsertMiddleware, "quota exceeded");
    /// assert_eq!(error.to_string(), "insert middleware failed");
    /// ```
    pub fn extension(phase: ExtensionPhase, source: impl Into<BoxError>) -> Self {
        Self::Extension {
            phase,
            source: source.into(),
        }
    }

    pub(crate) fn invalid_job(message: impl Into<String>) -> Self {
        Self::invalid_job_context("job", message)
    }

    pub(crate) fn invalid_job_context(context: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidJob(JobValidationError::new(context, message))
    }

    pub(crate) fn runtime_context(context: &'static str, message: impl Into<String>) -> Self {
        Self::Runtime(RuntimeError::new(context, message))
    }

    pub(crate) fn runtime_source(
        context: &'static str,
        message: impl Into<String>,
        source: impl Into<BoxError>,
    ) -> Self {
        Self::Runtime(RuntimeError {
            context,
            message: message.into(),
            source: Some(source.into()),
        })
    }

    pub(crate) const fn from_join(error: tokio::task::JoinError) -> Self {
        Self::RuntimeTask(error)
    }
}

/// Formats an error with its whole source chain, `outer: inner: innermost`,
/// for River's own log lines.
pub(crate) struct Chain<'a>(pub(crate) &'a (dyn std::error::Error + 'static));

impl fmt::Display for Chain<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)?;
        let mut source = self.0.source();
        while let Some(error) = source {
            write!(formatter, ": {error}")?;
            source = error.source();
        }
        Ok(())
    }
}

/// Returns a caught panic's message, when it has one.
pub(crate) fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    /// Renders an error the way `anyhow`'s `{:#}` does.
    fn report(error: &(dyn std::error::Error + 'static)) -> String {
        Chain(error).to_string()
    }

    #[test]
    fn database_errors_are_transparent() {
        let error = Error::from(sqlx::Error::RowNotFound);
        assert!(matches!(error, Error::Database(sqlx::Error::RowNotFound)));
        assert_eq!(report(&error), sqlx::Error::RowNotFound.to_string());
    }

    #[test]
    fn extension_preserves_concrete_source() {
        let error = Error::extension(
            ExtensionPhase::InsertMiddleware,
            std::io::Error::other("failed"),
        );
        let source = error.source().unwrap();

        assert!(source.downcast_ref::<std::io::Error>().is_some());
        assert_eq!(report(&error), "insert middleware failed: failed");
    }

    #[test]
    fn reports_never_repeat_a_message() {
        let cases = [
            (
                Error::configuration("bad"),
                "invalid client configuration: bad",
            ),
            (Error::invalid_job("bad kind"), "invalid job: bad kind"),
            (Error::NotFound(Record::Job(42)), "job 42 not found"),
            (
                Error::NotFound(Record::Queue("default".to_owned())),
                r#"queue "default" not found"#,
            ),
            (
                Error::JobNotRunning {
                    state: JobState::Completed,
                },
                "job is completed, not running",
            ),
            (
                Error::ResumableStep {
                    name: "second".to_owned(),
                    source: "step failed".into(),
                },
                r#"resumable step "second" failed: step failed"#,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(report(&error), expected);
        }
    }

    #[test]
    fn work_errors_are_transparent() {
        let inner = Error::extension(ExtensionPhase::ErrorHandler, "handler failed");
        let error = crate::WorkError::new(inner);

        assert_eq!(report(&error), "error handler failed: handler failed");
        assert!(matches!(
            error.downcast_ref::<Error>(),
            Some(Error::Extension {
                phase: ExtensionPhase::ErrorHandler,
                ..
            })
        ));
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
        assert!(
            error
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .is_some()
        );
        assert_eq!(
            report(&error),
            "resumable cursor: cannot decode cursor: bad JSON"
        );
    }
}
