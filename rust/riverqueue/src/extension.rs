//! Ordered hooks, middleware, and plugin registration.

use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::{Error, InsertParams, JobRow, PeriodicJobs, WorkContext};

/// Cloneable worker error passed to hooks and error handlers.
#[derive(Clone)]
pub struct WorkError {
    message: String,
    source: Arc<dyn std::error::Error + Send + Sync>,
}

impl WorkError {
    pub(crate) fn new(error: Box<dyn std::error::Error + Send + Sync>) -> Self {
        let message = error.to_string();
        Self {
            message,
            source: error.into(),
        }
    }

    /// Returns the concrete worker error for inspection or downcasting.
    #[must_use]
    pub fn source_ref(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self.source.as_ref()
    }
}

impl fmt::Debug for WorkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkError")
            .field("message", &self.message)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for WorkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for WorkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Name of an internal runtime metric emitted to hooks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MetricName {
    /// Duration of one successful available-job fetch.
    JobGetAvailableDuration,
    /// Number of rows claimed by one successful available-job fetch.
    JobGetAvailableCount,
}

/// Strongly typed metric emitted by River without installing a recorder.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum Metric {
    /// Duration of one successful available-job fetch.
    JobGetAvailableDuration(Duration),
    /// Number of rows claimed by one successful available-job fetch.
    JobGetAvailableCount(u64),
}

impl Metric {
    /// Stable metric name.
    #[must_use]
    pub const fn name(self) -> MetricName {
        match self {
            Self::JobGetAvailableDuration(_) => MetricName::JobGetAvailableDuration,
            Self::JobGetAvailableCount(_) => MetricName::JobGetAvailableCount,
        }
    }
}

/// Mutable type-erased insertion data visible to extensions.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct InsertContext {
    /// Serialized arguments that will be persisted and hashed.
    pub encoded_args: Value,
    /// Stable job kind.
    pub kind: String,
    /// Insertion options.
    pub opts: InsertParams,
}

/// Public summary of a worker result passed to extensions.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum WorkResult {
    /// Worker requested cancellation.
    Cancelled,
    /// Worker completed successfully.
    Completed,
    /// Worker requested terminal discard.
    Discarded,
    /// Worker returned an error.
    Failed(WorkError),
    /// Worker panicked.
    Panicked(String),
    /// Worker was aborted after ignoring cancellation.
    Aborted,
    /// Worker returned because its client was shutting down.
    Interrupted,
    /// Worker requested a snooze.
    Snoozed(Duration),
}

/// Lifecycle observations. Hooks run in registration order.
#[async_trait]
pub trait Hook: Send + Sync + 'static {
    /// Decodes a persisted row before River constructs a typed insertion
    /// result. This is the inverse of any storage transformation performed by
    /// [`Hook::insert_begin`]. Decode hooks run in reverse registration order
    /// so nested transformations compose correctly.
    async fn decode_insert_result(&self, _job: &mut JobRow) -> Result<(), Error> {
        Ok(())
    }

    /// Runs before insert middleware and SQL execution.
    async fn insert_begin(&self, _insert: &mut InsertContext) -> Result<(), Error> {
        Ok(())
    }

    /// Runs after successful insertion or unique lookup.
    async fn insert_end(
        &self,
        _job: &JobRow,
        _unique_skipped_as_duplicate: bool,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Runs after a successful no-returning `COPY` insertion.
    async fn insert_many_fast_end(&self, _inserted_count: u64) -> Result<(), Error> {
        Ok(())
    }

    /// Observes a runtime metric. Hook failures are logged and do not fail the
    /// operation that produced the metric.
    async fn metric_emit(&self, _metric: Metric) -> Result<(), Error> {
        Ok(())
    }

    /// Runs when the client's dynamic periodic-job service starts.
    async fn periodic_jobs_start(&self, _jobs: &PeriodicJobs) -> Result<(), Error> {
        Ok(())
    }

    /// Runs before work middleware and worker execution.
    async fn work_begin(&self, _context: &WorkContext, _job: &mut JobRow) -> Result<(), Error> {
        Ok(())
    }

    /// Runs after worker execution and before result persistence.
    async fn work_end(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        _result: &WorkResult,
    ) -> Result<(), Error> {
        Ok(())
    }
}

/// Ordered insertion middleware.
#[async_trait]
pub trait InsertMiddleware: Send + Sync + 'static {
    /// Mutates or validates an insertion before SQL execution.
    async fn before_insert(&self, insert: &mut InsertContext) -> Result<(), Error>;

    /// Observes a successful insertion in reverse registration order.
    async fn after_insert(
        &self,
        _job: &JobRow,
        _unique_skipped_as_duplicate: bool,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Observes a successful no-returning `COPY` insertion in reverse
    /// registration order.
    async fn after_insert_many_fast(&self, _inserted_count: u64) -> Result<(), Error> {
        Ok(())
    }
}

/// Ordered worker middleware.
#[async_trait]
pub trait WorkMiddleware: Send + Sync + 'static {
    /// Runs before the typed worker.
    async fn before_work(&self, _context: &WorkContext, _job: &mut JobRow) -> Result<(), Error> {
        Ok(())
    }

    /// Runs after the typed worker in reverse registration order.
    async fn after_work(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        _result: &WorkResult,
    ) -> Result<(), Error> {
        Ok(())
    }
}

/// Retry scheduling policy for ordinary worker errors and panics.
pub trait RetryPolicy: Send + Sync + 'static {
    /// Returns the delay before another attempt.
    fn next_retry(&self, job: &JobRow, error: &str, now: DateTime<Utc>) -> Duration;
}

/// River's quartic retry policy with compatibility jitter.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultRetryPolicy {
    seed: u64,
}

impl DefaultRetryPolicy {
    /// Uses a deterministic jitter seed, primarily for reproducible tests.
    #[must_use]
    pub const fn with_seed(seed: u64) -> Self {
        Self { seed }
    }
}

impl RetryPolicy for DefaultRetryPolicy {
    fn next_retry(&self, job: &JobRow, _error: &str, now: DateTime<Utc>) -> Duration {
        crate::client::default_retry_delay(job, now, self.seed)
    }
}

/// Result override returned by an error handler.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorHandlerDecision {
    /// Continue normal retry or discard handling.
    #[default]
    Continue,
    /// Cancel immediately regardless of remaining attempts.
    Cancel,
}

/// Handler invoked for worker errors, panics, and forced task abortion.
#[async_trait]
pub trait ErrorHandler: Send + Sync + 'static {
    /// Optionally overrides default retry handling.
    async fn handle_error(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        _result: &WorkResult,
    ) -> Result<ErrorHandlerDecision, Error> {
        Ok(ErrorHandlerDecision::default())
    }

    /// Observes a job that exceeded its cancellation grace period.
    async fn handle_stuck(&self, _job: &JobRow) -> Result<(), Error> {
        Ok(())
    }
}

/// Collection of extension components installed as one unit.
pub trait Plugin: Send + Sync + 'static {
    /// Hooks contributed by this plugin.
    fn hooks(&self) -> Vec<Arc<dyn Hook>> {
        Vec::new()
    }

    /// Insertion middleware contributed by this plugin.
    fn insert_middleware(&self) -> Vec<Arc<dyn InsertMiddleware>> {
        Vec::new()
    }

    /// Worker middleware contributed by this plugin.
    fn work_middleware(&self) -> Vec<Arc<dyn WorkMiddleware>> {
        Vec::new()
    }
}
