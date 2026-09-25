//! Typed worker interfaces and registration.

use std::{
    collections::{HashMap, HashSet},
    error::Error as StdError,
    future::Future,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::{
    BoxError, Client, Error, Job, JobArgs, JobMetadata, JobRow, JobUpdateParams, WorkError,
    database::DatabaseTransactionExecutor,
};

/// Context available while a job is running.
#[derive(Clone)]
pub struct WorkContext {
    cancellation: CancellationToken,
    client: Option<Client>,
    job_id: Option<i64>,
    metadata_updates: Arc<Mutex<Map<String, Value>>>,
    resumable: Arc<Mutex<ResumableState>>,
}

impl WorkContext {
    /// Creates a detached context with no client.
    #[must_use]
    pub(crate) fn new(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            client: None,
            job_id: None,
            metadata_updates: Arc::new(Mutex::new(Map::new())),
            resumable: Arc::new(Mutex::new(ResumableState::default())),
        }
    }

    /// Cancellation token triggered by timeout, remote cancellation, or stop.
    #[must_use]
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Returns the River client supervising this job. Contexts constructed by
    /// test helpers such as `riverqueue-test` are detached and return `None`.
    #[must_use]
    pub fn client(&self) -> Option<&Client> {
        self.client.as_ref()
    }

    /// Completes the running job in a caller-managed transaction, for example
    /// alongside business writes the job performed, like River Go's
    /// `JobCompleteTx`.
    ///
    /// Metadata recorded on this context, including output, is merged into
    /// the job. The job becomes completed only when the transaction commits,
    /// and River then leaves the completed row unchanged when the worker
    /// returns. [`Jobs::complete`](crate::Jobs::complete) completes a running
    /// job by ID outside a worker.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Runtime`] when this context doesn't belong to a
    /// running worker, [`Error::InvalidJob`] when the job is no longer
    /// running, [`Error::DatabaseMismatch`] for a transaction from another
    /// backend, [`Error::Extension`] when an extension's completion hook
    /// fails, and [`Error::Database`] when the database operation fails.
    pub async fn job_complete_tx<'executor, E>(&self, connection: E) -> Result<JobRow, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        let (client, job_id) = self.current_job()?;
        client
            .job_complete_tx_with_metadata(connection, job_id, self.metadata_updates())
            .await
    }

    /// Sets a metadata key that River merges into the job's metadata when it
    /// records the attempt's result. Setting a key again replaces its value.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` can't be serialized to JSON.
    pub fn metadata_set(
        &self,
        key: impl Into<String>,
        value: impl Serialize,
    ) -> Result<(), serde_json::Error> {
        let value = serde_json::to_value(value)?;
        self.insert_metadata(key.into(), value);
        Ok(())
    }

    /// Records the job's output under River's reserved output metadata key,
    /// where [`JobRow::output`] and River UI read it. Like River Go, output
    /// is limited to 32 MB of JSON, but should be kept much smaller.
    ///
    /// # Errors
    ///
    /// Returns an error when `output` can't be serialized to JSON or its JSON
    /// is larger than 32 MB.
    pub fn record_output(&self, output: impl Serialize) -> Result<(), serde_json::Error> {
        let output = serde_json::to_value(output)?;
        check_output_size(&output).map_err(<serde_json::Error as serde::ser::Error>::custom)?;
        self.insert_metadata(crate::METADATA_KEY_OUTPUT.to_owned(), output);
        Ok(())
    }

    pub(crate) fn insert_metadata(&self, key: String, value: Value) {
        self.lock_metadata().insert(key, value);
    }

    /// Metadata updates are never held across an await, and each update
    /// leaves the map consistent, so a poisoned lock is still usable.
    fn lock_metadata(&self) -> MutexGuard<'_, Map<String, Value>> {
        self.metadata_updates
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Like metadata, resumable state is never locked across an await, and
    /// each update leaves it consistent, so a poisoned lock is still usable.
    fn lock_resumable(&self) -> MutexGuard<'_, ResumableState> {
        self.resumable
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs a named resumable step, skipping work completed by an earlier
    /// failed attempt.
    ///
    /// Await steps sequentially. Nested steps are supported, but concurrent
    /// steps do not define a checkpoint order. A step may fail with any error
    /// convertible into [`BoxError`], including `anyhow::Error`; it is
    /// returned as the source of [`Error::ResumableStep`].
    pub async fn resumable_step<F, Fut, E>(&self, name: &str, step: F) -> Result<(), Error>
    where
        E: Into<BoxError>,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let previous_step_name = match self.begin_resumable_step(name, false)? {
            StepAction::Run(previous) => previous,
            StepAction::Skip => return Ok(()),
        };

        let result = step().await;
        let mut state = self.lock_resumable();
        state.step_name = previous_step_name;
        match result {
            Ok(()) => {
                state.completed_step = Some(name.to_owned());
                Ok(())
            }
            Err(error) => Err(state.fail_step(name, error.into())),
        }
    }

    /// Runs a named resumable step with the last cursor recorded for that step.
    ///
    /// Errors are handled as in [`WorkContext::resumable_step`].
    pub async fn resumable_step_with_cursor<T, F, Fut, E>(
        &self,
        name: &str,
        step: F,
    ) -> Result<(), Error>
    where
        E: Into<BoxError>,
        F: FnOnce(T) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        T: Default + DeserializeOwned,
    {
        let previous_step_name = match self.begin_resumable_step(name, true)? {
            StepAction::Run(previous) => previous,
            StepAction::Skip => return Ok(()),
        };

        let cursor = {
            let state = self.lock_resumable();
            state
                .cursors
                .get(name)
                .cloned()
                .map(serde_json::from_value)
                .transpose()
        };
        let cursor = match cursor {
            Ok(cursor) => cursor.unwrap_or_default(),
            Err(error) => {
                let mut state = self.lock_resumable();
                state.step_name = previous_step_name;
                return Err(state.fail_step(name, Box::new(error)));
            }
        };
        let result = step(cursor).await;
        let mut state = self.lock_resumable();
        state.step_name = previous_step_name;
        match result {
            Ok(()) => {
                state.completed_step = Some(name.to_owned());
                state.cursors.remove(name);
                Ok(())
            }
            Err(error) => Err(state.fail_step(name, error.into())),
        }
    }

    /// Records progress for the currently running resumable cursor step.
    ///
    /// # Errors
    ///
    /// Returns an error when called outside a resumable step or when `cursor`
    /// can't be serialized to JSON.
    pub fn resumable_set_cursor<T: Serialize>(&self, cursor: &T) -> Result<(), Error> {
        let cursor = serde_json::to_value(cursor)?;
        let mut state = self.lock_resumable();
        let step_name = state.step_name.clone().ok_or_else(|| {
            Error::runtime_context(
                "worker context",
                "resumable cursor can only be set inside a resumable cursor step".to_owned(),
            )
        })?;
        state.cursors.insert(step_name, cursor);
        Ok(())
    }

    /// Persists the current resumable step in a caller-managed transaction.
    pub async fn resumable_set_step_tx<'executor, E>(&self, connection: E) -> Result<JobRow, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        self.resumable_checkpoint_tx::<Value, _>(connection, None)
            .await
    }

    /// Persists the current resumable step and cursor in a caller-managed
    /// transaction.
    pub async fn resumable_set_step_cursor_tx<'executor, T, E>(
        &self,
        connection: E,
        cursor: &T,
    ) -> Result<JobRow, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
        T: Serialize,
    {
        self.resumable_checkpoint_tx(connection, Some(cursor)).await
    }

    async fn resumable_checkpoint_tx<'executor, T, E>(
        &self,
        connection: E,
        cursor: Option<&T>,
    ) -> Result<JobRow, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
        T: Serialize,
    {
        let metadata = self.resumable_checkpoint(cursor)?;
        let (client, job_id) = self.current_job()?;
        client
            .job_update_tx(
                connection,
                job_id,
                JobUpdateParams {
                    metadata,
                    output: None,
                },
            )
            .await
    }

    /// Marks the current step complete, records `cursor` for it, and returns
    /// the checkpoint metadata to persist.
    fn resumable_checkpoint<T: Serialize>(
        &self,
        cursor: Option<&T>,
    ) -> Result<Map<String, Value>, Error> {
        let mut state = self.lock_resumable();
        let step_name = state.step_name.clone().ok_or_else(|| {
            Error::runtime_context(
                "worker context",
                "resumable checkpoint must be set inside a resumable step".to_owned(),
            )
        })?;
        state.completed_step = Some(step_name.clone());
        if let Some(cursor) = cursor {
            state
                .cursors
                .insert(step_name.clone(), serde_json::to_value(cursor)?);
        }
        let mut metadata = Map::new();
        metadata.insert(
            crate::METADATA_KEY_RESUMABLE_STEP.to_owned(),
            step_name.into(),
        );
        if !state.cursors.is_empty() {
            metadata.insert(
                crate::METADATA_KEY_RESUMABLE_CURSOR.to_owned(),
                Value::Object(state.cursors.clone()),
            );
        }
        Ok(metadata)
    }

    fn begin_resumable_step(&self, name: &str, cursor_step: bool) -> Result<StepAction, Error> {
        if name.is_empty() {
            return Err(Error::runtime_context(
                "worker context",
                "resumable step name cannot be empty".to_owned(),
            ));
        }
        let mut state = self.lock_resumable();
        if let Some(failure) = &state.failure {
            return Err(Error::runtime_source(
                "worker context",
                failure.to_string(),
                failure.clone(),
            ));
        }
        if !state.all_step_names.insert(name.to_owned()) {
            let message = format!("duplicate resumable step name {name:?}");
            state.failure = Some(WorkError::new(Box::new(Error::runtime_context(
                "worker context",
                message.clone(),
            ))));
            return Err(Error::runtime_context("worker context", message));
        }
        if !state.resume_matched {
            if state.resume_step.as_deref() == Some(name) {
                state.completed_step = Some(name.to_owned());
                state.resume_matched = true;
                if !cursor_step || !state.cursors.contains_key(name) {
                    return Ok(StepAction::Skip);
                }
            } else {
                return Ok(StepAction::Skip);
            }
        }
        let previous = state.step_name.replace(name.to_owned());
        Ok(StepAction::Run(previous))
    }

    fn current_job(&self) -> Result<(&Client, i64), Error> {
        self.client
            .as_ref()
            .zip(self.job_id)
            .ok_or_else(|| {
                Error::runtime_context(
                    "worker context",
                    "transactional context operation requires a WorkContext supplied to a running River worker"
                        .to_owned(),
                )
            })
    }

    /// Returns a snapshot of metadata recorded during this attempt.
    pub(crate) fn metadata_updates(&self) -> Map<String, Value> {
        self.lock_metadata().clone()
    }

    pub(crate) fn for_job(
        client: Client,
        cancellation: CancellationToken,
        job_id: i64,
        metadata: &JobMetadata,
    ) -> Self {
        let state = ResumableState::from_metadata(metadata);
        Self {
            cancellation,
            client: Some(client),
            job_id: Some(job_id),
            metadata_updates: Arc::new(Mutex::new(Map::new())),
            resumable: Arc::new(Mutex::new(state)),
        }
    }

    /// Creates a detached attempt context using persisted resumable metadata.
    #[must_use]
    pub(crate) fn for_test_job(job: &JobRow) -> Self {
        let mut context = Self::new(CancellationToken::new());
        context.resumable = Arc::new(Mutex::new(ResumableState::from_metadata(&job.metadata)));
        context
    }

    /// Validates checkpoint metadata before invoking user work.
    pub(crate) fn resumable_validate(&self) -> Result<(), WorkError> {
        match &self.lock_resumable().failure {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    /// Resolves attempt-scoped resumable errors and metadata for runtime/test parity.
    pub(crate) fn resumable_finish(&self, worker_failed: bool) -> Option<WorkError> {
        let state = self.lock_resumable();
        let failure = state.failure.clone().or_else(|| {
            (!worker_failed && !state.resume_matched).then(|| {
                WorkError::new(Box::new(Error::runtime_context(
                    "worker context",
                    format!(
                        "resumable step {:?} not found in worker",
                        state.resume_step.as_deref().unwrap_or_default()
                    ),
                )))
            })
        });
        if (worker_failed || failure.is_some())
            && let Some(completed_step) = &state.completed_step
        {
            let mut updates = self.lock_metadata();
            updates.insert(
                crate::METADATA_KEY_RESUMABLE_STEP.to_owned(),
                completed_step.clone().into(),
            );
            if state.cursors.is_empty() {
                if state.had_cursors {
                    updates.insert(crate::METADATA_KEY_RESUMABLE_CURSOR.to_owned(), Value::Null);
                }
            } else {
                updates.insert(
                    crate::METADATA_KEY_RESUMABLE_CURSOR.to_owned(),
                    Value::Object(state.cursors.clone()),
                );
            }
        }
        failure
    }
}

impl std::fmt::Debug for WorkContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkContext")
            .field("job_id", &self.job_id)
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("metadata_updates", &*self.lock_metadata())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct ResumableState {
    all_step_names: HashSet<String>,
    completed_step: Option<String>,
    cursors: Map<String, Value>,
    failure: Option<WorkError>,
    had_cursors: bool,
    resume_matched: bool,
    resume_step: Option<String>,
    step_name: Option<String>,
}

impl Default for ResumableState {
    fn default() -> Self {
        Self {
            all_step_names: HashSet::new(),
            completed_step: None,
            cursors: Map::new(),
            failure: None,
            had_cursors: false,
            resume_matched: true,
            resume_step: None,
            step_name: None,
        }
    }
}

impl ResumableState {
    fn from_metadata(metadata: &JobMetadata) -> Self {
        let mut state = Self::default();
        state.resume_step = metadata
            .get::<String>(crate::METADATA_KEY_RESUMABLE_STEP)
            .ok()
            .flatten()
            .filter(|step| !step.is_empty());
        state.resume_matched = state.resume_step.is_none();
        match metadata.get_raw(crate::METADATA_KEY_RESUMABLE_CURSOR) {
            Some(raw) if raw.get().starts_with('{') => {
                if let Ok(cursors) = serde_json::from_str::<Map<String, Value>>(raw.get()) {
                    state.had_cursors = !cursors.is_empty();
                    state.cursors = cursors;
                } else {
                    state.failure = Some(WorkError::new(Box::new(Error::invalid_job(
                        "river:resumable_cursor cannot be decoded",
                    ))));
                }
            }
            Some(raw) if raw.get().starts_with('[') => {
                state.failure = Some(WorkError::new(Box::new(Error::invalid_job(
                    "river:resumable_cursor must be an object when present",
                ))));
            }
            _ => {}
        }
        state
    }

    fn fail_step(&mut self, name: &str, error: BoxError) -> Error {
        let source = WorkError::new(error);
        self.failure = Some(WorkError::new(Box::new(Error::ResumableStep {
            name: name.to_owned(),
            source: Box::new(source.clone()),
        })));
        Error::ResumableStep {
            name: name.to_owned(),
            source: Box::new(source),
        }
    }
}

enum StepAction {
    Run(Option<String>),
    Skip,
}

/// Successful control outcome returned by a worker.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkOutcome {
    /// Mark the job cancelled.
    Cancel,
    /// Mark the job complete.
    #[default]
    Complete,
    /// Discard without another attempt.
    Discard,
    /// Reschedule without consuming an attempt.
    Snooze(Duration),
}

/// Per-worker timeout selection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkerTimeout {
    /// Use the client-wide timeout.
    #[default]
    ClientDefault,
    /// Do not time out this kind of job.
    Disabled,
    /// Cancel the job after this duration.
    After(Duration),
}

/// A typed asynchronous job worker.
pub trait Worker<A>: Send + Sync + 'static
where
    A: JobArgs,
{
    /// Worker-specific error type. Errors use River's retry policy.
    ///
    /// Any error convertible into [`BoxError`] works, including concrete
    /// error types, `Box<dyn Error + Send + Sync>`, and report types such as
    /// `anyhow::Error` or `eyre::Report`. Hooks and error handlers receive it
    /// as a [`WorkError`], whose [`source_ref`](WorkError::source_ref) can be
    /// downcast to a concrete error type. A report type converts into its own
    /// wrapper, which keeps its message and source chain but can't be
    /// downcast to the type it wraps; return a concrete error type when an
    /// extension needs to downcast it.
    type Error: Into<BoxError>;

    /// Overrides the client retry delay for this job. Returning `None` uses the
    /// client policy.
    fn next_retry(
        &self,
        _job: &Job<A>,
        _error: &WorkError,
        _now: DateTime<Utc>,
    ) -> Option<Duration> {
        None
    }

    /// Overrides the client timeout for this job.
    fn timeout(&self, _job: &Job<A>) -> WorkerTimeout {
        WorkerTimeout::ClientDefault
    }

    /// Executes a job.
    ///
    /// Implementations can use `async fn`; the explicit return type guarantees
    /// that the resulting future can run on River's multithreaded Tokio
    /// runtime without requiring each implementation to box its future.
    fn work(
        &self,
        context: WorkContext,
        job: Job<A>,
    ) -> impl Future<Output = Result<WorkOutcome, Self::Error>> + Send;
}

/// Type-erased adapter from persisted rows to a typed [`Worker`].
#[async_trait]
trait ErasedWorker: Send + Sync {
    fn next_retry(
        &self,
        row: &JobRow,
        error: &WorkError,
        now: DateTime<Utc>,
    ) -> Result<Option<Duration>, Box<dyn StdError + Send + Sync>>;

    fn timeout(&self, row: &JobRow) -> Result<WorkerTimeout, Box<dyn StdError + Send + Sync>>;

    /// Runs one attempt. Arguments are decoded once, and the worker's
    /// timeout for the decoded job is reported through `timeout` before
    /// work starts. The outer error reports arguments that couldn't be
    /// decoded, in which case the worker didn't run.
    async fn work(
        &self,
        context: WorkContext,
        row: &JobRow,
        timeout: oneshot::Sender<WorkerTimeout>,
    ) -> Result<Result<WorkOutcome, WorkError>, WorkError>;
}

struct FunctionWorker<F> {
    function: F,
}

impl<A, E, F, Fut> Worker<A> for FunctionWorker<F>
where
    A: JobArgs,
    E: Into<BoxError>,
    F: Fn(WorkContext, Job<A>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<WorkOutcome, E>> + Send,
{
    type Error = E;

    fn work(
        &self,
        context: WorkContext,
        job: Job<A>,
    ) -> impl Future<Output = Result<WorkOutcome, Self::Error>> + Send {
        (self.function)(context, job)
    }
}

struct RegisteredWorker<A, W> {
    worker: W,
    _args: std::marker::PhantomData<A>,
}

#[async_trait]
impl<A, W> ErasedWorker for RegisteredWorker<A, W>
where
    A: JobArgs,
    W: Worker<A>,
{
    fn next_retry(
        &self,
        row: &JobRow,
        error: &WorkError,
        now: DateTime<Utc>,
    ) -> Result<Option<Duration>, Box<dyn StdError + Send + Sync>> {
        // `work` consumes its job, so a failed attempt decodes once more to
        // consult the worker's retry override.
        let job = Job {
            args: row.decode_args()?,
            row: row.clone(),
        };
        Ok(Worker::<A>::next_retry(&self.worker, &job, error, now))
    }

    fn timeout(&self, row: &JobRow) -> Result<WorkerTimeout, Box<dyn StdError + Send + Sync>> {
        let job = Job {
            args: row.decode_args()?,
            row: row.clone(),
        };
        Ok(Worker::<A>::timeout(&self.worker, &job))
    }

    async fn work(
        &self,
        context: WorkContext,
        row: &JobRow,
        timeout: oneshot::Sender<WorkerTimeout>,
    ) -> Result<Result<WorkOutcome, WorkError>, WorkError> {
        let job = Job {
            args: row.decode_args().map_err(WorkError::new)?,
            row: row.clone(),
        };
        // The supervisor may have stopped waiting for a timeout; that is not
        // an error for the attempt.
        let _ = timeout.send(Worker::<A>::timeout(&self.worker, &job));
        // Sending wakes the supervisor on this worker thread, where Tokio
        // may hold it in a slot other threads can't steal. Yield once so it
        // starts the timeout before a worker that blocks the thread (which
        // the supervisor exists to detect) can delay it.
        tokio::task::yield_now().await;
        Ok(self
            .worker
            .work(context, job)
            .await
            .map_err(|error| WorkError::new(error.into())))
    }
}

/// Type-erased collection of workers keyed by job kind.
#[derive(Clone, Default)]
pub struct WorkerRegistry {
    workers: HashMap<&'static str, Arc<dyn ErasedWorker>>,
}

impl std::fmt::Debug for WorkerRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerRegistry")
            .field("kinds", &self.kinds())
            .finish_non_exhaustive()
    }
}

impl WorkerRegistry {
    /// Creates an empty worker registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns registered kinds in stable order.
    #[must_use]
    pub fn kinds(&self) -> Vec<&'static str> {
        let mut kinds = self.workers.keys().copied().collect::<Vec<_>>();
        kinds.sort_unstable();
        kinds
    }

    pub(crate) fn contains_kind(&self, kind: &str) -> bool {
        self.workers.contains_key(kind)
    }

    pub(crate) fn next_retry(
        &self,
        row: &JobRow,
        error: &WorkError,
        now: DateTime<Utc>,
    ) -> Result<Option<Duration>, Box<dyn StdError + Send + Sync>> {
        self.worker_for(row)?.next_retry(row, error, now)
    }

    /// Evaluates the worker timeout for a persisted row outside an attempt,
    /// such as when rescuing stuck jobs.
    pub(crate) fn timeout(
        &self,
        row: &JobRow,
    ) -> Result<WorkerTimeout, Box<dyn StdError + Send + Sync>> {
        self.worker_for(row)?.timeout(row)
    }

    /// Registers one worker, rejecting duplicate kinds.
    pub fn register<A, W>(&mut self, worker: W) -> Result<&mut Self, Error>
    where
        A: JobArgs,
        W: Worker<A>,
    {
        if A::KIND.is_empty() || A::KIND.len() >= 128 {
            return Err(Error::invalid_job_context(
                "worker registration",
                format!(
                    "job kind must contain between 1 and 127 bytes: {:?}",
                    A::KIND
                ),
            ));
        }
        let mut kinds = vec![A::KIND];
        for alias in A::kind_aliases() {
            if alias.is_empty() || alias.len() >= 128 {
                return Err(Error::invalid_job_context(
                    "worker registration",
                    format!("job kind alias must contain between 1 and 127 bytes: {alias:?}"),
                ));
            }
            if kinds.contains(alias) || self.workers.contains_key(alias) {
                return Err(Error::invalid_job_context(
                    "worker registration",
                    format!("worker already registered for kind {alias:?}"),
                ));
            }
            kinds.push(alias);
        }
        if self.workers.contains_key(A::KIND) {
            return Err(Error::invalid_job_context(
                "worker registration",
                format!("worker already registered for kind {:?}", A::KIND),
            ));
        }
        let worker: Arc<dyn ErasedWorker> = Arc::new(RegisteredWorker::<A, W> {
            worker,
            _args: std::marker::PhantomData,
        });
        for kind in kinds {
            self.workers.insert(kind, Arc::clone(&worker));
        }
        Ok(self)
    }

    /// Registers an asynchronous function or closure as a worker.
    ///
    /// The function may return any error convertible into [`BoxError`], such
    /// as `anyhow::Result<WorkOutcome>`; see [`Worker::Error`]. Use [`Worker`]
    /// instead when a job kind needs to override its timeout or retry
    /// schedule.
    ///
    /// # Errors
    ///
    /// Returns an error when the job kind or one of its aliases is invalid or
    /// already registered.
    pub fn register_fn<A, E, F, Fut>(&mut self, function: F) -> Result<&mut Self, Error>
    where
        A: JobArgs,
        E: Into<BoxError>,
        F: Fn(WorkContext, Job<A>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<WorkOutcome, E>> + Send,
    {
        self.register::<A, _>(FunctionWorker { function })
    }

    /// Returns an error for a row whose kind has no registered worker, which
    /// River fails before running any hook or middleware, as River Go does.
    pub(crate) fn check_kind(&self, row: &JobRow) -> Result<(), WorkError> {
        self.worker_for(row).map(|_| ()).map_err(WorkError::new)
    }

    /// Runs one attempt of `row`, decoding its arguments once. The worker's
    /// timeout for the job is sent on `timeout` before work starts; the
    /// sender is dropped without a value when the attempt fails first. The
    /// outer error reports an unknown kind or arguments that couldn't be
    /// decoded, in which case the worker didn't run.
    pub(crate) async fn work(
        &self,
        context: WorkContext,
        row: &JobRow,
        timeout: oneshot::Sender<WorkerTimeout>,
    ) -> Result<Result<WorkOutcome, WorkError>, WorkError> {
        let worker = self.worker_for(row).map_err(WorkError::new)?;
        worker.work(context, row, timeout).await
    }

    fn worker_for(
        &self,
        row: &JobRow,
    ) -> Result<&Arc<dyn ErasedWorker>, Box<dyn StdError + Send + Sync>> {
        self.workers.get(row.kind.as_str()).ok_or_else(|| {
            Box::new(Error::UnknownJobKind(row.kind.clone())) as Box<dyn StdError + Send + Sync>
        })
    }
}

/// Maximum encoded size of recorded output (Go `maxOutputSizeBytes`).
const MAX_OUTPUT_BYTES: usize = 32 * 1024 * 1024;

/// Rejects output whose JSON is larger than River Go allows.
pub(crate) fn check_output_size(output: &Value) -> Result<(), String> {
    let size = crate::encoding::to_go_string(output)
        .map_err(|error| error.to_string())?
        .len();
    if size > MAX_OUTPUT_BYTES {
        return Err(format!(
            "output is too large: {size} bytes (max {} MB)",
            MAX_OUTPUT_BYTES / 1024 / 1024
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fmt,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use chrono::Utc;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;
    use crate::JobState;

    #[derive(Debug)]
    struct FunctionError;

    impl fmt::Display for FunctionError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("function worker failed")
        }
    }

    impl StdError for FunctionError {}

    #[derive(Debug, Deserialize, Serialize)]
    struct FunctionJobArgs {
        fail: bool,
    }

    impl JobArgs for FunctionJobArgs {
        const KIND: &'static str = "function_worker";

        fn kind_aliases() -> &'static [&'static str] {
            &["function_worker_v1"]
        }
    }

    async fn function_worker(
        context: WorkContext,
        job: Job<FunctionJobArgs>,
    ) -> Result<WorkOutcome, FunctionError> {
        if job.args.fail {
            return Err(FunctionError);
        }
        context.record_output(json!({"function": true})).unwrap();
        Ok(WorkOutcome::Complete)
    }

    fn job_row(kind: &str, fail: bool) -> JobRow {
        let now = Utc::now();
        JobRow {
            attempt: 1,
            attempted_at: Some(now),
            attempted_by: vec!["test".to_owned()],
            created_at: now,
            encoded_args: serde_json::value::to_raw_value(&json!({"fail": fail})).unwrap(),
            errors: Vec::new(),
            finalized_at: None,
            id: 1,
            kind: kind.to_owned(),
            max_attempts: 25,
            metadata: JobMetadata::default(),
            priority: 1,
            queue: "default".to_owned(),
            scheduled_at: now,
            state: JobState::Running,
            tags: Vec::new(),
            unique_key: None,
            unique_states: None,
        }
    }

    #[tokio::test]
    async fn resumable_context_runs_without_a_checkpoint() {
        for metadata in [json!({}), json!({"river:resumable_step": ""})] {
            let mut row = job_row(FunctionJobArgs::KIND, false);
            row.metadata = metadata.as_object().unwrap().clone().into();
            let context = WorkContext::for_test_job(&row);
            let mut ran = false;
            context
                .resumable_step("first", || async {
                    ran = true;
                    Ok::<_, FunctionError>(())
                })
                .await
                .unwrap();
            assert!(ran);
            assert!(context.resumable_finish(false).is_none());
        }
    }

    #[tokio::test]
    async fn resumable_cursor_decode_failure_is_sticky() {
        let mut row = job_row(FunctionJobArgs::KIND, false);
        row.metadata = json!({
            "river:resumable_step": "first",
            "river:resumable_cursor": { "second": "not a number" }
        })
        .as_object()
        .unwrap()
        .clone()
        .into();
        let context = WorkContext::for_test_job(&row);
        context
            .resumable_step("first", || async {
                panic!("already completed");
                #[allow(unreachable_code)]
                Ok::<_, FunctionError>(())
            })
            .await
            .unwrap();
        let error = context
            .resumable_step_with_cursor("second", |_: i64| async {
                panic!("invalid cursor must not reach worker");
                #[allow(unreachable_code)]
                Ok::<_, FunctionError>(())
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        assert!(context.resumable_set_cursor(&1).is_err());
        assert!(context.resumable_finish(false).is_some());
        assert_eq!(
            context.metadata_updates()[crate::METADATA_KEY_RESUMABLE_STEP],
            "first"
        );
    }

    #[tokio::test]
    async fn resumable_nested_steps_restore_parent_and_error_sources() {
        let context = WorkContext::new(CancellationToken::new());
        let error = context
            .resumable_step_with_cursor("outer", |_: i64| async {
                context
                    .resumable_step("inner", || async { Ok::<_, Error>(()) })
                    .await?;
                context.resumable_set_cursor(&7)?;
                Err::<(), _>(Error::ResumableStep {
                    name: "source".to_owned(),
                    source: Box::new(FunctionError),
                })
            })
            .await
            .unwrap_err();
        let mut source: &(dyn StdError + 'static) = &error;
        while !source.is::<FunctionError>() {
            source = source.source().expect("preserved source");
        }
        let finished = context.resumable_finish(false).unwrap();
        let mut source: &(dyn StdError + 'static) = &finished;
        while !source.is::<FunctionError>() {
            source = source.source().expect("preserved suppressed source");
        }
        assert_eq!(
            context.metadata_updates(),
            json!({
                "river:resumable_step": "inner",
                "river:resumable_cursor": {"outer": 7}
            })
            .as_object()
            .unwrap()
            .clone()
        );
    }

    #[tokio::test]
    async fn register_fn_accepts_capturing_closure() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_worker = Arc::clone(&calls);
        let mut workers = WorkerRegistry::new();
        workers
            .register_fn(move |_context: WorkContext, _job: Job<FunctionJobArgs>| {
                let calls = Arc::clone(&calls_for_worker);
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, FunctionError>(WorkOutcome::Snooze(Duration::from_secs(1)))
                }
            })
            .unwrap();

        let (timeout_sender, _timeout_receiver) = oneshot::channel();
        let outcome = workers
            .work(
                WorkContext::new(CancellationToken::new()),
                &job_row(FunctionJobArgs::KIND, false),
                timeout_sender,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(outcome, WorkOutcome::Snooze(Duration::from_secs(1)));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn register_fn_handles_aliases_and_typed_errors() {
        let mut workers = WorkerRegistry::new();
        workers.register_fn(function_worker).unwrap();

        assert_eq!(
            workers.kinds(),
            [FunctionJobArgs::KIND, "function_worker_v1"]
        );

        let (timeout_sender, _timeout_receiver) = oneshot::channel();
        let error = workers
            .work(
                WorkContext::new(CancellationToken::new()),
                &job_row("function_worker_v1", true),
                timeout_sender,
            )
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.source_ref().downcast_ref::<FunctionError>().is_some());
    }

    static COUNTED_DECODES: AtomicUsize = AtomicUsize::new(0);

    #[derive(Serialize)]
    struct CountedArgs {
        timeout_ms: u64,
    }

    impl<'de> Deserialize<'de> for CountedArgs {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            #[derive(Deserialize)]
            struct Fields {
                timeout_ms: u64,
            }
            COUNTED_DECODES.fetch_add(1, Ordering::SeqCst);
            let fields = Fields::deserialize(deserializer)?;
            Ok(Self {
                timeout_ms: fields.timeout_ms,
            })
        }
    }

    impl JobArgs for CountedArgs {
        const KIND: &'static str = "counted_args";
    }

    struct CountedWorker;

    impl Worker<CountedArgs> for CountedWorker {
        type Error = FunctionError;

        fn timeout(&self, job: &Job<CountedArgs>) -> WorkerTimeout {
            WorkerTimeout::After(Duration::from_millis(job.args.timeout_ms))
        }

        fn work(
            &self,
            _context: WorkContext,
            job: Job<CountedArgs>,
        ) -> impl Future<Output = Result<WorkOutcome, Self::Error>> + Send {
            assert_eq!(job.row.kind, CountedArgs::KIND);
            std::future::ready(Ok(WorkOutcome::Complete))
        }
    }

    #[tokio::test]
    async fn work_decodes_args_once_and_reports_timeout_first() {
        let mut workers = WorkerRegistry::new();
        workers.register(CountedWorker).unwrap();
        let mut row = job_row(CountedArgs::KIND, false);
        row.encoded_args = serde_json::value::to_raw_value(&json!({"timeout_ms": 1234})).unwrap();

        let (timeout_sender, timeout_receiver) = oneshot::channel();
        let outcome = workers
            .work(
                WorkContext::new(CancellationToken::new()),
                &row,
                timeout_sender,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(outcome, WorkOutcome::Complete);
        assert_eq!(
            timeout_receiver.await.unwrap(),
            WorkerTimeout::After(Duration::from_millis(1234))
        );
        assert_eq!(COUNTED_DECODES.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn work_with_undecodable_args_fails_without_reporting_timeout() {
        let mut workers = WorkerRegistry::new();
        workers.register_fn(function_worker).unwrap();
        let mut row = job_row(FunctionJobArgs::KIND, false);
        row.encoded_args = serde_json::value::to_raw_value(&json!({"fail": "no"})).unwrap();

        let (timeout_sender, timeout_receiver) = oneshot::channel();
        let error = workers
            .work(
                WorkContext::new(CancellationToken::new()),
                &row,
                timeout_sender,
            )
            .await
            .unwrap_err();

        assert!(error.source_ref().is::<serde_json::Error>());
        assert!(timeout_receiver.await.is_err());
    }

    async fn run_once(workers: &WorkerRegistry, row: &JobRow) -> Result<WorkOutcome, WorkError> {
        let (timeout_sender, _timeout_receiver) = oneshot::channel();
        workers
            .work(
                WorkContext::new(CancellationToken::new()),
                row,
                timeout_sender,
            )
            .await
            .and_then(|result| result)
    }

    async fn anyhow_function_worker(
        _context: WorkContext,
        job: Job<FunctionJobArgs>,
    ) -> anyhow::Result<WorkOutcome> {
        use anyhow::Context as _;

        if job.args.fail {
            return Err(std::io::Error::other("disk full")).context("writing report");
        }
        Ok(WorkOutcome::Complete)
    }

    #[tokio::test]
    async fn register_fn_accepts_anyhow_results() {
        let mut workers = WorkerRegistry::new();
        workers.register_fn(anyhow_function_worker).unwrap();

        assert_eq!(
            run_once(&workers, &job_row(FunctionJobArgs::KIND, false))
                .await
                .unwrap(),
            WorkOutcome::Complete
        );
        let error = run_once(&workers, &job_row(FunctionJobArgs::KIND, true))
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "writing report");
        // The report's source chain is preserved for inspection.
        let root = error
            .source_ref()
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .expect("anyhow context source");
        assert_eq!(root.to_string(), "disk full");
    }

    struct BoxedErrorWorker;

    impl Worker<FunctionJobArgs> for BoxedErrorWorker {
        type Error = BoxError;

        async fn work(
            &self,
            _context: WorkContext,
            _job: Job<FunctionJobArgs>,
        ) -> Result<WorkOutcome, Self::Error> {
            tokio::task::yield_now().await;
            Err(Box::new(FunctionError))
        }
    }

    #[tokio::test]
    async fn boxed_worker_errors_remain_downcastable() {
        let mut workers = WorkerRegistry::new();
        workers.register(BoxedErrorWorker).unwrap();

        let error = run_once(&workers, &job_row(FunctionJobArgs::KIND, false))
            .await
            .unwrap_err();
        assert!(error.source_ref().downcast_ref::<FunctionError>().is_some());
    }

    #[tokio::test]
    async fn resumable_steps_accept_anyhow_errors() {
        let context = WorkContext::new(CancellationToken::new());
        let error = context
            .resumable_step("first", || async {
                Err::<(), _>(anyhow::anyhow!("step failed"))
            })
            .await
            .unwrap_err();

        assert!(matches!(&error, Error::ResumableStep { name, .. } if name == "first"));
        assert_eq!(error.source().unwrap().to_string(), "step failed");

        let context = WorkContext::new(CancellationToken::new());
        let error = context
            .resumable_step_with_cursor("second", |_: i64| async {
                Err::<(), _>(anyhow::anyhow!("cursor step failed"))
            })
            .await
            .unwrap_err();
        assert!(matches!(&error, Error::ResumableStep { name, .. } if name == "second"));
        assert_eq!(error.source().unwrap().to_string(), "cursor step failed");
    }

    #[test]
    fn metadata_set_and_record_output_serialize_values() {
        #[derive(Serialize)]
        struct Receipt {
            delivered: bool,
        }

        let context = WorkContext::new(CancellationToken::new());
        context.metadata_set("attempts", 3).unwrap();
        context.metadata_set("attempts", 4).unwrap();
        context.record_output(Receipt { delivered: true }).unwrap();

        let bad_output = std::collections::BTreeMap::from([((1, 2), true)]);
        assert!(context.record_output(&bad_output).is_err());
        assert_eq!(
            context.metadata_updates(),
            json!({"attempts": 4, "output": {"delivered": true}})
                .as_object()
                .unwrap()
                .clone()
        );
    }

    #[test]
    fn work_context_debug_shows_attempt_state() {
        let cancellation = CancellationToken::new();
        let context = WorkContext::new(cancellation.clone());
        context.metadata_set("attempts", 3).unwrap();
        cancellation.cancel();

        assert_eq!(
            format!("{context:?}"),
            r#"WorkContext { job_id: None, cancelled: true, metadata_updates: {"attempts": Number(3)}, .. }"#
        );
    }

    #[test]
    fn register_fn_rejects_duplicate_kinds() {
        let mut workers = WorkerRegistry::new();
        workers.register_fn(function_worker).unwrap();

        let Err(error) = workers.register_fn(function_worker) else {
            panic!("duplicate registration should fail");
        };

        assert!(error.to_string().contains("already registered"));
    }

    #[test]
    fn registry_debug_lists_kinds_without_worker_internals() {
        let mut registry = WorkerRegistry::new();
        registry
            .register_fn(
                |_context: WorkContext, _job: Job<FunctionJobArgs>| async move {
                    Ok::<_, std::io::Error>(WorkOutcome::Complete)
                },
            )
            .unwrap();

        let debug = format!("{registry:?}");
        assert!(debug.contains(FunctionJobArgs::KIND));
        assert!(!debug.contains("dyn ErasedWorker"));
    }

    #[test]
    fn recorded_output_is_limited_like_go() {
        let context = crate::__private::work_context(tokio_util::sync::CancellationToken::new());
        let limit = super::MAX_OUTPUT_BYTES;
        // A JSON string's two quotes count toward the limit.
        context.record_output("x".repeat(limit - 2)).unwrap();
        let error = context.record_output("x".repeat(limit - 1)).unwrap_err();
        assert!(error.to_string().contains("output is too large"), "{error}");
        assert_eq!(
            context.metadata_updates()[crate::METADATA_KEY_OUTPUT]
                .as_str()
                .map(str::len),
            Some(limit - 2)
        );
    }
}
