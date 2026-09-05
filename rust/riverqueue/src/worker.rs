//! Typed worker interfaces and registration.

use std::{
    collections::{HashMap, HashSet},
    error::Error as StdError,
    future::Future,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    Client, Error, Job, JobArgs, JobRow, JobUpdateParams, WorkError,
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
    /// Creates a context supervised by River.
    #[doc(hidden)]
    #[must_use]
    pub fn new(cancellation: CancellationToken) -> Self {
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
    /// test helpers with [`WorkContext::new`] are detached and return `None`.
    #[must_use]
    pub fn client(&self) -> Option<&Client> {
        self.client.as_ref()
    }

    /// Completes the running job inside a caller-managed transaction, merging
    /// metadata recorded on this context and invoking the exact-version
    /// completion extension seam.
    pub async fn job_complete_tx<'executor, E>(&self, connection: E) -> Result<JobRow, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        let (client, job_id) = self.current_job()?;
        client
            .job_complete_tx_with_metadata(connection, job_id, self.metadata_updates().await)
            .await
    }

    /// Sets a metadata key to be persisted with the job result.
    pub async fn metadata_set(&self, key: impl Into<String>, value: Value) {
        self.metadata_updates.lock().await.insert(key.into(), value);
    }

    /// Records typed JSON output under River's reserved output key.
    pub async fn record_output<T: Serialize>(&self, output: &T) -> Result<(), Error> {
        let output = serde_json::to_value(output)?;
        self.metadata_set(crate::METADATA_KEY_OUTPUT, output).await;
        Ok(())
    }

    /// Runs a named resumable step, skipping work completed by an earlier
    /// failed attempt.
    ///
    /// Await steps sequentially. Nested steps are supported, but concurrent
    /// steps do not define a checkpoint order.
    pub async fn resumable_step<F, Fut, E>(&self, name: &str, step: F) -> Result<(), Error>
    where
        E: StdError + Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let previous_step_name = match self.begin_resumable_step(name, false).await? {
            StepAction::Run(previous) => previous,
            StepAction::Skip => return Ok(()),
        };

        let result = step().await;
        let mut state = self.resumable.lock().await;
        state.step_name = previous_step_name;
        match result {
            Ok(()) => {
                state.completed_step = Some(name.to_owned());
                Ok(())
            }
            Err(error) => Err(state.fail_step(name, error)),
        }
    }

    /// Runs a named resumable step with the last cursor recorded for that step.
    pub async fn resumable_step_with_cursor<T, F, Fut, E>(
        &self,
        name: &str,
        step: F,
    ) -> Result<(), Error>
    where
        E: StdError + Send + Sync + 'static,
        F: FnOnce(T) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        T: Default + DeserializeOwned,
    {
        let previous_step_name = match self.begin_resumable_step(name, true).await? {
            StepAction::Run(previous) => previous,
            StepAction::Skip => return Ok(()),
        };

        let cursor = {
            let state = self.resumable.lock().await;
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
                let mut state = self.resumable.lock().await;
                state.step_name = previous_step_name;
                return Err(state.fail_step(name, error));
            }
        };
        let result = step(cursor).await;
        let mut state = self.resumable.lock().await;
        state.step_name = previous_step_name;
        match result {
            Ok(()) => {
                state.completed_step = Some(name.to_owned());
                state.cursors.remove(name);
                Ok(())
            }
            Err(error) => Err(state.fail_step(name, error)),
        }
    }

    /// Records progress for the currently running resumable cursor step.
    pub async fn resumable_set_cursor<T: Serialize>(&self, cursor: &T) -> Result<(), Error> {
        let cursor = serde_json::to_value(cursor)?;
        let mut state = self.resumable.lock().await;
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
        let mut state = self.resumable.lock().await;
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
        drop(state);
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

    async fn begin_resumable_step(
        &self,
        name: &str,
        cursor_step: bool,
    ) -> Result<StepAction, Error> {
        if name.is_empty() {
            return Err(Error::runtime_context(
                "worker context",
                "resumable step name cannot be empty".to_owned(),
            ));
        }
        let mut state = self.resumable.lock().await;
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
    ///
    /// This exact-version seam is used by `riverqueue-test` to return an
    /// immutable result after invoking a worker directly.
    #[doc(hidden)]
    pub async fn metadata_updates(&self) -> Map<String, Value> {
        self.metadata_updates.lock().await.clone()
    }

    pub(crate) fn for_job(
        client: Client,
        cancellation: CancellationToken,
        job_id: i64,
        metadata: &Map<String, Value>,
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
    #[doc(hidden)]
    #[must_use]
    pub fn for_test_job(job: &JobRow) -> Self {
        let mut context = Self::new(CancellationToken::new());
        context.resumable = Arc::new(Mutex::new(ResumableState::from_metadata(&job.metadata)));
        context
    }

    /// Validates checkpoint metadata before invoking user work.
    #[doc(hidden)]
    pub async fn resumable_validate(&self) -> Result<(), WorkError> {
        match &self.resumable.lock().await.failure {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    /// Resolves attempt-scoped resumable errors and metadata for runtime/test parity.
    #[doc(hidden)]
    pub async fn resumable_finish(&self, worker_failed: bool) -> Option<WorkError> {
        let state = self.resumable.lock().await;
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
            let mut updates = self.metadata_updates.lock().await;
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
    fn from_metadata(metadata: &Map<String, Value>) -> Self {
        let mut state = Self::default();
        state.resume_step = metadata
            .get(crate::METADATA_KEY_RESUMABLE_STEP)
            .and_then(Value::as_str)
            .filter(|step| !step.is_empty())
            .map(str::to_owned);
        state.resume_matched = state.resume_step.is_none();
        match metadata.get(crate::METADATA_KEY_RESUMABLE_CURSOR) {
            Some(Value::Object(cursors)) => {
                state.cursors.clone_from(cursors);
                state.had_cursors = !cursors.is_empty();
            }
            Some(Value::Array(_)) => {
                state.failure = Some(WorkError::new(Box::new(Error::invalid_job(
                    "river:resumable_cursor must be an object when present",
                ))));
            }
            _ => {}
        }
        state
    }

    fn fail_step(&mut self, name: &str, error: impl StdError + Send + Sync + 'static) -> Error {
        let source = WorkError::new(Box::new(error));
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
    type Error: StdError + Send + Sync + 'static;

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

#[async_trait]
trait ErasedWorker: Send + Sync {
    fn next_retry(
        &self,
        row: &JobRow,
        error: &WorkError,
        now: DateTime<Utc>,
    ) -> Result<Option<Duration>, Box<dyn StdError + Send + Sync>>;

    fn timeout(&self, row: &JobRow) -> Result<WorkerTimeout, Box<dyn StdError + Send + Sync>>;

    async fn work(&self, context: WorkContext, row: &JobRow) -> Result<WorkOutcome, WorkError>;
}

struct FunctionWorker<F> {
    function: F,
}

impl<A, E, F, Fut> Worker<A> for FunctionWorker<F>
where
    A: JobArgs,
    E: StdError + Send + Sync + 'static,
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
        let args = serde_json::from_value(row.encoded_args.clone())?;
        Ok(Worker::<A>::next_retry(
            &self.worker,
            &Job {
                args,
                row: row.clone(),
            },
            error,
            now,
        ))
    }

    fn timeout(&self, row: &JobRow) -> Result<WorkerTimeout, Box<dyn StdError + Send + Sync>> {
        let args = serde_json::from_value(row.encoded_args.clone())?;
        Ok(Worker::<A>::timeout(
            &self.worker,
            &Job {
                args,
                row: row.clone(),
            },
        ))
    }

    async fn work(&self, context: WorkContext, row: &JobRow) -> Result<WorkOutcome, WorkError> {
        let args = serde_json::from_value(row.encoded_args.clone())
            .map_err(|error| WorkError::new(Box::new(error)))?;
        self.worker
            .work(
                context,
                Job {
                    args,
                    row: row.clone(),
                },
            )
            .await
            .map_err(|error| WorkError::new(Box::new(error)))
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
    /// Use [`Worker`] instead when a job kind needs to override its timeout or
    /// retry schedule.
    ///
    /// # Errors
    ///
    /// Returns an error when the job kind or one of its aliases is invalid or
    /// already registered.
    pub fn register_fn<A, E, F, Fut>(&mut self, function: F) -> Result<&mut Self, Error>
    where
        A: JobArgs,
        E: StdError + Send + Sync + 'static,
        F: Fn(WorkContext, Job<A>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<WorkOutcome, E>> + Send,
    {
        self.register::<A, _>(FunctionWorker { function })
    }

    pub(crate) async fn work(
        &self,
        context: WorkContext,
        row: &JobRow,
    ) -> Result<WorkOutcome, WorkError> {
        let worker = self.worker_for(row).map_err(WorkError::new)?;
        worker.work(context, row).await
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
    use serde_json::{Map, json};

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
        context
            .record_output(&json!({"function": true}))
            .await
            .unwrap();
        Ok(WorkOutcome::Complete)
    }

    fn job_row(kind: &str, fail: bool) -> JobRow {
        let now = Utc::now();
        JobRow {
            attempt: 1,
            attempted_at: Some(now),
            attempted_by: vec!["test".to_owned()],
            created_at: now,
            encoded_args: json!({"fail": fail}),
            errors: Vec::new(),
            finalized_at: None,
            id: 1,
            kind: kind.to_owned(),
            max_attempts: 25,
            metadata: Map::new(),
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
            row.metadata = metadata.as_object().unwrap().clone();
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
            assert!(context.resumable_finish(false).await.is_none());
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
        .clone();
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
        assert!(context.resumable_set_cursor(&1).await.is_err());
        assert!(context.resumable_finish(false).await.is_some());
        assert_eq!(
            context.metadata_updates().await[crate::METADATA_KEY_RESUMABLE_STEP],
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
                context.resumable_set_cursor(&7).await?;
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
        let finished = context.resumable_finish(false).await.unwrap();
        let mut source: &(dyn StdError + 'static) = &finished;
        while !source.is::<FunctionError>() {
            source = source.source().expect("preserved suppressed source");
        }
        assert_eq!(
            context.metadata_updates().await,
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

        let outcome = workers
            .work(
                WorkContext::new(CancellationToken::new()),
                &job_row(FunctionJobArgs::KIND, false),
            )
            .await
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

        let error = workers
            .work(
                WorkContext::new(CancellationToken::new()),
                &job_row("function_worker_v1", true),
            )
            .await
            .unwrap_err();
        assert!(error.source_ref().downcast_ref::<FunctionError>().is_some());
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
}
