//! Ordered hooks, middleware, and plugin registration.

use std::{fmt, future::Future, pin::Pin, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};

use crate::{BoxError, Error, InsertParams, JobRow, PeriodicJobs, WorkContext, WorkOutcome};

/// Cloneable worker error passed to hooks and error handlers.
#[derive(Clone)]
pub struct WorkError {
    message: String,
    source: Arc<dyn std::error::Error + Send + Sync>,
}

impl WorkError {
    /// Wraps an error, for example one a [`WorkMiddleware`] or
    /// [`Hook::work_end`] returns in place of the worker's result.
    pub fn new(error: impl Into<BoxError>) -> Self {
        let error = error.into();
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

/// Error a worker returns when it stops because its
/// [`WorkContext::cancellation_token`](crate::WorkContext::cancellation_token)
/// was cancelled.
///
/// This is River's equivalent of Go's `context.Canceled`. When a client's hard
/// shutdown cancels a job, a worker that returns this error (directly or
/// anywhere in its error's source chain) is treated as interrupted: the job
/// becomes available again with the attempt refunded and no error recorded.
/// Any other error returned during shutdown is recorded and retried like an
/// ordinary failure, so a job that genuinely fails while the client stops
/// still consumes its attempt.
///
/// ```
/// use riverqueue::{WorkCancelled, WorkContext, WorkOutcome};
///
/// async fn work(context: WorkContext) -> Result<WorkOutcome, WorkCancelled> {
///     tokio::select! {
///         () = context.cancellation_token().cancelled() => Err(WorkCancelled),
///         () = tokio::time::sleep(std::time::Duration::from_secs(1)) => Ok(WorkOutcome::Complete),
///     }
/// }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, thiserror::Error)]
#[error("job work cancelled")]
pub struct WorkCancelled;

impl WorkCancelled {
    /// Whether `error` or any error in its source chain is [`WorkCancelled`].
    ///
    /// Errors wrapped by `std::io::Error::other` are inspected as well,
    /// because `io::Error` does not expose its payload as a source.
    #[must_use]
    pub fn is_in_chain(error: &(dyn std::error::Error + 'static)) -> bool {
        let mut current = Some(error);
        while let Some(error) = current {
            if error.is::<Self>() {
                return true;
            }
            if let Some(payload) = error
                .downcast_ref::<std::io::Error>()
                .and_then(std::io::Error::get_ref)
                && Self::is_in_chain(payload)
            {
                return true;
            }
            current = error.source();
        }
        false
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

/// A job about to be inserted, as seen by hooks and insertion middleware.
///
/// This is the Rust counterpart of River Go's `rivertype.JobInsertParams`.
/// River resolves options, validates them, and computes the unique key before
/// any extension runs, so changing the arguments, queue, or schedule here
/// doesn't change the job's uniqueness, just as in River Go.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct InsertContext {
    /// Serialized arguments that will be persisted, as exact JSON text.
    /// Replace them with [`encode_args`](crate::encoding::encode_args) to keep
    /// the encoding River Go would produce.
    pub encoded_args: Box<serde_json::value::RawValue>,
    /// Stable job kind.
    pub kind: String,
    /// Resolved insertion options.
    pub opts: InsertParams,
    /// State the job is inserted in: available, pending, or scheduled.
    pub state: crate::JobState,
    /// Creation time to persist instead of the database's current time.
    pub(crate) created_at: Option<DateTime<Utc>>,
    /// Unique key hash computed from the original insertion.
    pub(crate) unique_key: Option<Vec<u8>>,
    /// Bitmask of states in which the unique key is enforced.
    pub(crate) unique_states: Option<u8>,
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

/// A thread-safe boxed future, used where River erases extension types.
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Lifecycle hooks. Hooks run in registration order and observe or adjust
/// jobs without wrapping River's operations; use [`InsertMiddleware`] or
/// [`WorkMiddleware`] to wrap them.
///
/// Every method has a default no-op implementation, so implement only the
/// ones you need. Methods are ordinary `async fn`s:
///
/// ```
/// use riverqueue::{BoxError, Hook, InsertContext};
///
/// struct TagEverything;
///
/// impl Hook for TagEverything {
///     async fn insert_begin(&self, insert: &mut InsertContext) -> Result<(), BoxError> {
///         insert.opts.tags.push("tagged".to_owned());
///         Ok(())
///     }
/// }
/// ```
///
/// An error returned from a hook fails the operation it observes and is
/// reported as [`Error::Extension`] with the hook's error as its source.
pub trait Hook: Send + Sync + 'static {
    /// Decodes a persisted row before River returns it from an insertion.
    ///
    /// This is the inverse of any storage transformation performed by
    /// [`Hook::insert_begin`]. Decode hooks run in reverse registration order
    /// so that nested transformations compose.
    fn decode_insert_result(
        &self,
        job: &mut JobRow,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        let _ = job;
        std::future::ready(Ok(()))
    }

    /// Runs for each job inside insertion middleware, before the job is
    /// written. It may change the job's arguments, options, or initial
    /// state.
    fn insert_begin(
        &self,
        insert: &mut InsertContext,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        let _ = insert;
        std::future::ready(Ok(()))
    }

    /// Observes a runtime metric. Failures are logged and don't affect the
    /// operation that produced the metric.
    fn metric_emit(&self, metric: Metric) -> impl Future<Output = Result<(), BoxError>> + Send {
        let _ = metric;
        std::future::ready(Ok(()))
    }

    /// Runs when this client's periodic job enqueuer starts, which happens
    /// each time the client is elected leader.
    fn periodic_jobs_start(
        &self,
        jobs: &PeriodicJobs,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        let _ = jobs;
        std::future::ready(Ok(()))
    }

    /// Runs inside work middleware, before the job's arguments are decoded
    /// and the worker runs. It may change the job, for example to decode
    /// arguments another hook or middleware transformed on insertion.
    ///
    /// An error fails the attempt with that error; the worker and
    /// [`Hook::work_end`] don't run.
    fn work_begin(
        &self,
        context: &WorkContext,
        job: &mut JobRow,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        let _ = (context, job);
        std::future::ready(Ok(()))
    }

    /// Runs inside work middleware, after the worker returns, and returns
    /// the attempt's result.
    ///
    /// Like River Go's `HookWorkEnd`, the returned result replaces the
    /// worker's, so a hook should return `result` unchanged unless it means
    /// to change the outcome, for example to turn a specific error into a
    /// snooze. Hooks run in registration order, each receiving the previous
    /// hook's result. It doesn't run when the worker panics.
    fn work_end(
        &self,
        context: &WorkContext,
        job: &JobRow,
        result: Result<WorkOutcome, WorkError>,
    ) -> impl Future<Output = Result<WorkOutcome, WorkError>> + Send {
        let _ = (context, job);
        std::future::ready(result)
    }
}

/// Object-safe form of [`Hook`] that River stores after registration.
pub(crate) trait DynHook: Send + Sync + 'static {
    fn decode_insert_result<'a>(&'a self, job: &'a mut JobRow) -> BoxFuture<'a, Result<(), Error>>;
    fn insert_begin<'a>(
        &'a self,
        insert: &'a mut InsertContext,
    ) -> BoxFuture<'a, Result<(), Error>>;
    fn metric_emit(&self, metric: Metric) -> BoxFuture<'_, Result<(), Error>>;
    fn periodic_jobs_start<'a>(
        &'a self,
        jobs: &'a PeriodicJobs,
    ) -> BoxFuture<'a, Result<(), Error>>;
    fn work_begin<'a>(
        &'a self,
        context: &'a WorkContext,
        job: &'a mut JobRow,
    ) -> BoxFuture<'a, Result<(), BoxError>>;
    fn work_end<'a>(
        &'a self,
        context: &'a WorkContext,
        job: &'a JobRow,
        result: Result<WorkOutcome, WorkError>,
    ) -> BoxFuture<'a, Result<WorkOutcome, WorkError>>;
}

fn hook_error(phase: &'static str) -> impl FnOnce(BoxError) -> Error {
    move |source| Error::Extension { phase, source }
}

impl<H: Hook> DynHook for H {
    fn decode_insert_result<'a>(&'a self, job: &'a mut JobRow) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            Hook::decode_insert_result(self, job)
                .await
                .map_err(hook_error("insert result decode hook"))
        })
    }

    fn insert_begin<'a>(
        &'a self,
        insert: &'a mut InsertContext,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            Hook::insert_begin(self, insert)
                .await
                .map_err(hook_error("insert begin hook"))
        })
    }

    fn metric_emit(&self, metric: Metric) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            Hook::metric_emit(self, metric)
                .await
                .map_err(hook_error("metric hook"))
        })
    }

    fn periodic_jobs_start<'a>(
        &'a self,
        jobs: &'a PeriodicJobs,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            Hook::periodic_jobs_start(self, jobs)
                .await
                .map_err(hook_error("periodic jobs start hook"))
        })
    }

    fn work_begin<'a>(
        &'a self,
        context: &'a WorkContext,
        job: &'a mut JobRow,
    ) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(Hook::work_begin(self, context, job))
    }

    fn work_end<'a>(
        &'a self,
        context: &'a WorkContext,
        job: &'a JobRow,
        result: Result<WorkOutcome, WorkError>,
    ) -> BoxFuture<'a, Result<WorkOutcome, WorkError>> {
        Box::pin(Hook::work_end(self, context, job, result))
    }
}

/// Middleware wrapping each insertion, including batches.
///
/// Like River Go's `JobInsertMiddleware`, middleware sees every job in an
/// insertion at once and decides whether and how to continue by calling
/// [`InsertNext::run`]. Middleware registered first is outermost. It can
/// change jobs before passing them on, observe or change the results, wrap
/// the insertion in a span or timer, or return early without inserting.
///
/// ```
/// use riverqueue::{Error, InsertContext, InsertMiddleware, InsertNext, InsertedJobs};
///
/// struct CountInserts;
///
/// impl InsertMiddleware for CountInserts {
///     async fn insert_many(
///         &self,
///         jobs: Vec<InsertContext>,
///         next: InsertNext<'_>,
///     ) -> Result<InsertedJobs, Error> {
///         let count = jobs.len();
///         let inserted = next.run(jobs).await?;
///         println!("inserted {count} jobs");
///         Ok(inserted)
///     }
/// }
/// ```
pub trait InsertMiddleware: Send + Sync + 'static {
    /// Wraps the insertion of `jobs`.
    fn insert_many(
        &self,
        jobs: Vec<InsertContext>,
        next: InsertNext<'_>,
    ) -> impl Future<Output = Result<InsertedJobs, Error>> + Send;
}

/// Object-safe form of [`InsertMiddleware`].
pub(crate) trait DynInsertMiddleware: Send + Sync + 'static {
    fn insert_many<'a>(
        &'a self,
        jobs: Vec<InsertContext>,
        next: InsertNext<'a>,
    ) -> BoxFuture<'a, Result<InsertedJobs, Error>>;
}

impl<M: InsertMiddleware> DynInsertMiddleware for M {
    fn insert_many<'a>(
        &'a self,
        jobs: Vec<InsertContext>,
        next: InsertNext<'a>,
    ) -> BoxFuture<'a, Result<InsertedJobs, Error>> {
        Box::pin(InsertMiddleware::insert_many(self, jobs, next))
    }
}

pub(crate) type InsertEndpoint<'a> =
    Box<dyn FnOnce(Vec<InsertContext>) -> BoxFuture<'a, Result<InsertedJobs, Error>> + Send + 'a>;

/// The remainder of an insertion: any inner middleware followed by River's
/// persistence of the jobs.
pub struct InsertNext<'a> {
    endpoint: InsertEndpoint<'a>,
    remaining: &'a [Arc<dyn DynInsertMiddleware>],
}

impl<'a> InsertNext<'a> {
    pub(crate) fn new(
        middleware: &'a [Arc<dyn DynInsertMiddleware>],
        endpoint: InsertEndpoint<'a>,
    ) -> Self {
        Self {
            endpoint,
            remaining: middleware,
        }
    }

    /// Continues the insertion with `jobs`.
    ///
    /// # Errors
    ///
    /// Returns the error of any inner middleware, hook, or extension, or of
    /// the database insertion.
    pub async fn run(self, jobs: Vec<InsertContext>) -> Result<InsertedJobs, Error> {
        match self.remaining.split_first() {
            Some((middleware, remaining)) => {
                middleware
                    .insert_many(
                        jobs,
                        InsertNext {
                            endpoint: self.endpoint,
                            remaining,
                        },
                    )
                    .await
            }
            None => (self.endpoint)(jobs).await,
        }
    }
}

impl fmt::Debug for InsertNext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InsertNext")
            .field("remaining_middleware", &self.remaining.len())
            .finish_non_exhaustive()
    }
}

/// Jobs written by an insertion, as seen by [`InsertMiddleware`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum InsertedJobs {
    /// Rows returned by an insertion, in input order.
    Rows(Vec<InsertedJob>),
    /// Number of rows written by a fast insertion that doesn't return rows.
    Count(u64),
}

impl InsertedJobs {
    /// Returns the number of jobs inserted, including unique jobs whose
    /// insertion was skipped as a duplicate.
    #[must_use]
    pub fn len(&self) -> u64 {
        match self {
            Self::Rows(rows) => u64::try_from(rows.len()).unwrap_or(u64::MAX),
            Self::Count(count) => *count,
        }
    }

    /// Returns whether no jobs were inserted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One row returned by an insertion.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct InsertedJob {
    /// The inserted job, or the existing job a unique insertion matched.
    pub job: JobRow,
    /// Whether the insertion was skipped because a matching unique job
    /// already existed.
    pub unique_skipped_as_duplicate: bool,
}

impl InsertedJob {
    pub(crate) const fn new(job: JobRow, unique_skipped_as_duplicate: bool) -> Self {
        Self {
            job,
            unique_skipped_as_duplicate,
        }
    }
}

/// Middleware wrapping each job attempt.
///
/// Like River Go's `WorkerMiddleware`, middleware decides whether and how to
/// continue by calling [`WorkNext::run`], and returns the attempt's result.
/// Middleware registered first is outermost. [`Hook::work_begin`], argument
/// decoding, the worker, and [`Hook::work_end`] all run inside the innermost
/// middleware, so middleware can change the job before it's decoded, wrap
/// the attempt in a span or timer, or change its result. A job whose kind has
/// no registered worker fails before any middleware runs.
///
/// When the worker panics, the panic unwinds through middleware as it does in
/// River Go; River records it as a failed attempt.
///
/// ```
/// use riverqueue::{JobRow, WorkContext, WorkError, WorkMiddleware, WorkNext, WorkOutcome};
///
/// struct TimeJobs;
///
/// impl WorkMiddleware for TimeJobs {
///     async fn work(
///         &self,
///         _context: &WorkContext,
///         job: JobRow,
///         next: WorkNext<'_>,
///     ) -> Result<WorkOutcome, WorkError> {
///         let kind = job.kind.clone();
///         let started = std::time::Instant::now();
///         let result = next.run(job).await;
///         println!("{kind} took {:?}", started.elapsed());
///         result
///     }
/// }
/// ```
pub trait WorkMiddleware: Send + Sync + 'static {
    /// Wraps one attempt of `job`.
    fn work(
        &self,
        context: &WorkContext,
        job: JobRow,
        next: WorkNext<'_>,
    ) -> impl Future<Output = Result<WorkOutcome, WorkError>> + Send;
}

/// Object-safe form of [`WorkMiddleware`].
pub(crate) trait DynWorkMiddleware: Send + Sync + 'static {
    fn work<'a>(
        &'a self,
        context: &'a WorkContext,
        job: JobRow,
        next: WorkNext<'a>,
    ) -> BoxFuture<'a, Result<WorkOutcome, WorkError>>;
}

impl<M: WorkMiddleware> DynWorkMiddleware for M {
    fn work<'a>(
        &'a self,
        context: &'a WorkContext,
        job: JobRow,
        next: WorkNext<'a>,
    ) -> BoxFuture<'a, Result<WorkOutcome, WorkError>> {
        Box::pin(WorkMiddleware::work(self, context, job, next))
    }
}

pub(crate) type WorkEndpoint<'a> =
    Box<dyn FnOnce(JobRow) -> BoxFuture<'a, Result<WorkOutcome, WorkError>> + Send + 'a>;

/// The remainder of a job attempt: any inner middleware followed by River's
/// work hooks and the worker.
pub struct WorkNext<'a> {
    context: &'a WorkContext,
    endpoint: WorkEndpoint<'a>,
    remaining: &'a [Arc<dyn DynWorkMiddleware>],
}

impl<'a> WorkNext<'a> {
    pub(crate) fn new(
        middleware: &'a [Arc<dyn DynWorkMiddleware>],
        context: &'a WorkContext,
        endpoint: WorkEndpoint<'a>,
    ) -> Self {
        Self {
            context,
            endpoint,
            remaining: middleware,
        }
    }

    /// Continues the attempt with `job`.
    ///
    /// # Errors
    ///
    /// Returns the worker's error, or the error of an inner middleware, a
    /// work hook, or argument decoding.
    pub async fn run(self, job: JobRow) -> Result<WorkOutcome, WorkError> {
        match self.remaining.split_first() {
            Some((middleware, remaining)) => {
                middleware
                    .work(
                        self.context,
                        job,
                        WorkNext {
                            context: self.context,
                            endpoint: self.endpoint,
                            remaining,
                        },
                    )
                    .await
            }
            None => (self.endpoint)(job).await,
        }
    }
}

impl fmt::Debug for WorkNext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkNext")
            .field("remaining_middleware", &self.remaining.len())
            .finish_non_exhaustive()
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

/// Handler invoked for worker errors, panics, and stuck jobs.
///
/// Both methods have default implementations. Handler errors and panics are
/// logged and don't change how River handles the job.
pub trait ErrorHandler: Send + Sync + 'static {
    /// Called when a worker returns an error, panics, or is aborted. Returning
    /// [`ErrorHandlerDecision::Cancel`] cancels the job regardless of its
    /// remaining attempts.
    fn handle_error(
        &self,
        context: &WorkContext,
        job: &JobRow,
        result: &WorkResult,
    ) -> impl Future<Output = Result<ErrorHandlerDecision, BoxError>> + Send {
        let _ = (context, job, result);
        std::future::ready(Ok(ErrorHandlerDecision::default()))
    }

    /// Called when a job keeps running past its cancellation grace period.
    fn handle_stuck(&self, job: &JobRow) -> impl Future<Output = Result<(), BoxError>> + Send {
        let _ = job;
        std::future::ready(Ok(()))
    }
}

/// Object-safe form of [`ErrorHandler`].
pub(crate) trait DynErrorHandler: Send + Sync + 'static {
    fn handle_error<'a>(
        &'a self,
        context: &'a WorkContext,
        job: &'a JobRow,
        result: &'a WorkResult,
    ) -> BoxFuture<'a, Result<ErrorHandlerDecision, Error>>;
    fn handle_stuck<'a>(&'a self, job: &'a JobRow) -> BoxFuture<'a, Result<(), Error>>;
}

impl<H: ErrorHandler> DynErrorHandler for H {
    fn handle_error<'a>(
        &'a self,
        context: &'a WorkContext,
        job: &'a JobRow,
        result: &'a WorkResult,
    ) -> BoxFuture<'a, Result<ErrorHandlerDecision, Error>> {
        Box::pin(recover_handler_panic("error handler", async move {
            ErrorHandler::handle_error(self, context, job, result).await
        }))
    }

    fn handle_stuck<'a>(&'a self, job: &'a JobRow) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(recover_handler_panic("stuck job handler", async move {
            ErrorHandler::handle_stuck(self, job).await
        }))
    }
}

/// Awaits an error handler, treating a panic like a returned error, as Go's
/// `invokeErrorHandler` recovers one. The job's result is still persisted,
/// rather than the panic unwinding the executor and leaving the job running.
async fn recover_handler_panic<T>(
    phase: &'static str,
    handler: impl Future<Output = Result<T, BoxError>>,
) -> Result<T, Error> {
    use futures_util::FutureExt as _;

    match std::panic::AssertUnwindSafe(handler).catch_unwind().await {
        Ok(result) => result.map_err(hook_error(phase)),
        Err(panic) => Err(hook_error(phase)(
            format!("panicked: {}", crate::error::panic_message(&panic)).into(),
        )),
    }
}

/// A set of extensions installed together, such as a tracing integration
/// that needs a hook and middleware.
///
/// ```
/// use riverqueue::{BoxError, Extensions, Hook, InsertContext, Plugin};
///
/// struct AuditHook;
///
/// impl Hook for AuditHook {
///     async fn insert_begin(&self, insert: &mut InsertContext) -> Result<(), BoxError> {
///         println!("inserting {}", insert.kind);
///         Ok(())
///     }
/// }
///
/// struct Audit;
///
/// impl Plugin for Audit {
///     fn install(&self, extensions: &mut Extensions) {
///         extensions.hook(AuditHook);
///     }
/// }
/// ```
pub trait Plugin: Send + Sync + 'static {
    /// Registers the plugin's hooks and middleware.
    fn install(&self, extensions: &mut Extensions);
}

/// Registrar through which a [`Plugin`] adds hooks and middleware.
///
/// Extensions are appended after any registered earlier, in the order the
/// plugin adds them.
#[derive(Default)]
pub struct Extensions {
    pub(crate) hooks: Vec<Arc<dyn DynHook>>,
    pub(crate) insert_middleware: Vec<Arc<dyn DynInsertMiddleware>>,
    pub(crate) work_middleware: Vec<Arc<dyn DynWorkMiddleware>>,
}

impl Extensions {
    /// Adds a lifecycle hook.
    pub fn hook<H: Hook>(&mut self, hook: H) -> &mut Self {
        self.hooks.push(Arc::new(hook));
        self
    }

    /// Adds insertion middleware.
    pub fn insert_middleware<M: InsertMiddleware>(&mut self, middleware: M) -> &mut Self {
        self.insert_middleware.push(Arc::new(middleware));
        self
    }

    /// Adds worker middleware.
    pub fn work_middleware<M: WorkMiddleware>(&mut self, middleware: M) -> &mut Self {
        self.work_middleware.push(Arc::new(middleware));
        self
    }
}

impl fmt::Debug for Extensions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Extensions")
            .field("hooks", &self.hooks.len())
            .field("insert_middleware", &self.insert_middleware.len())
            .field("work_middleware", &self.work_middleware.len())
            .finish()
    }
}
