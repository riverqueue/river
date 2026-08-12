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
use sqlx::PgConnection;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{Client, Error, Job, JobArgs, JobRow, JobUpdateParams};

/// Context available while a job is running.
#[derive(Clone)]
pub struct WorkContext {
    cancellation: CancellationToken,
    client: Option<Client>,
    metadata_updates: Arc<Mutex<Map<String, Value>>>,
    resumable: Arc<Mutex<ResumableState>>,
}

impl WorkContext {
    /// Creates a context supervised by River.
    #[must_use]
    pub fn new(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            client: None,
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
    pub async fn job_complete_tx(
        &self,
        connection: &mut PgConnection,
        job_id: i64,
    ) -> Result<JobRow, Error> {
        let client = self.client.as_ref().ok_or_else(|| {
            Error::Runtime(
                "transactional completion requires a WorkContext supplied to a running River worker"
                    .to_owned(),
            )
        })?;
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
    pub async fn resumable_step<F, Fut, E>(&self, name: &str, step: F) -> Result<(), Error>
    where
        E: StdError,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        match self.begin_resumable_step(name, false).await? {
            StepAction::Run => {}
            StepAction::Skip => return Ok(()),
        }

        let result = step().await;
        let mut state = self.resumable.lock().await;
        state.step_name = None;
        match result {
            Ok(()) => {
                state.completed_step = Some(name.to_owned());
                Ok(())
            }
            Err(error) => {
                let message = format!("resumable step {name:?}: {error}");
                state.failure = Some(message.clone());
                Err(Error::Runtime(message))
            }
        }
    }

    /// Runs a named resumable step with the last cursor recorded for that step.
    pub async fn resumable_step_with_cursor<T, F, Fut, E>(
        &self,
        name: &str,
        step: F,
    ) -> Result<(), Error>
    where
        E: StdError,
        F: FnOnce(T) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        T: Default + DeserializeOwned,
    {
        match self.begin_resumable_step(name, true).await? {
            StepAction::Run => {}
            StepAction::Skip => return Ok(()),
        }

        let cursor = {
            let state = self.resumable.lock().await;
            state
                .cursors
                .get(name)
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|error| {
                    Error::Runtime(format!(
                        "unmarshal resumable cursor for step {name:?}: {error}"
                    ))
                })?
                .unwrap_or_default()
        };
        let result = step(cursor).await;
        let mut state = self.resumable.lock().await;
        state.step_name = None;
        match result {
            Ok(()) => {
                state.completed_step = Some(name.to_owned());
                state.cursors.remove(name);
                Ok(())
            }
            Err(error) => {
                let message = format!("resumable step {name:?}: {error}");
                state.failure = Some(message.clone());
                Err(Error::Runtime(message))
            }
        }
    }

    /// Records progress for the currently running resumable cursor step.
    pub async fn resumable_set_cursor<T: Serialize>(&self, cursor: &T) -> Result<(), Error> {
        let cursor = serde_json::to_value(cursor)?;
        let mut state = self.resumable.lock().await;
        let step_name = state.step_name.clone().ok_or_else(|| {
            Error::Runtime(
                "resumable cursor can only be set inside a resumable cursor step".to_owned(),
            )
        })?;
        state.cursors.insert(step_name, cursor);
        Ok(())
    }

    /// Persists the current resumable step in a caller-managed transaction.
    pub async fn resumable_set_step_tx(
        &self,
        client: &Client,
        connection: &mut PgConnection,
        job_id: i64,
    ) -> Result<JobRow, Error> {
        self.resumable_checkpoint_tx::<Value>(client, connection, job_id, None)
            .await
    }

    /// Persists the current resumable step and cursor in a caller-managed
    /// transaction.
    pub async fn resumable_set_step_cursor_tx<T: Serialize>(
        &self,
        client: &Client,
        connection: &mut PgConnection,
        job_id: i64,
        cursor: &T,
    ) -> Result<JobRow, Error> {
        self.resumable_checkpoint_tx(client, connection, job_id, Some(cursor))
            .await
    }

    async fn resumable_checkpoint_tx<T: Serialize>(
        &self,
        client: &Client,
        connection: &mut PgConnection,
        job_id: i64,
        cursor: Option<&T>,
    ) -> Result<JobRow, Error> {
        let mut state = self.resumable.lock().await;
        let step_name = state.step_name.clone().ok_or_else(|| {
            Error::Runtime("resumable checkpoint must be set inside a resumable step".to_owned())
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
            return Err(Error::Runtime(
                "resumable step name cannot be empty".to_owned(),
            ));
        }
        let mut state = self.resumable.lock().await;
        if let Some(failure) = &state.failure {
            return Err(Error::Runtime(failure.clone()));
        }
        if !state.all_step_names.insert(name.to_owned()) {
            let message = format!("duplicate resumable step name {name:?}");
            state.failure = Some(message.clone());
            return Err(Error::Runtime(message));
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
        state.step_name = Some(name.to_owned());
        Ok(StepAction::Run)
    }

    pub(crate) async fn metadata_updates(&self) -> Map<String, Value> {
        self.metadata_updates.lock().await.clone()
    }

    pub(crate) fn for_job(
        client: Client,
        cancellation: CancellationToken,
        metadata: &Map<String, Value>,
    ) -> Self {
        let mut state = ResumableState::default();
        state.resume_step = metadata
            .get(crate::METADATA_KEY_RESUMABLE_STEP)
            .and_then(Value::as_str)
            .map(str::to_owned);
        state.resume_matched = state.resume_step.is_none();
        if let Some(Value::Object(cursors)) = metadata.get(crate::METADATA_KEY_RESUMABLE_CURSOR) {
            state.cursors.clone_from(cursors);
            state.had_cursors = !cursors.is_empty();
        }
        Self {
            cancellation,
            client: Some(client),
            metadata_updates: Arc::new(Mutex::new(Map::new())),
            resumable: Arc::new(Mutex::new(state)),
        }
    }

    pub(crate) async fn resumable_finish(&self, worker_failed: bool) -> Option<String> {
        let state = self.resumable.lock().await;
        if worker_failed && let Some(completed_step) = &state.completed_step {
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
        state.failure.clone().or_else(|| {
            (!worker_failed && !state.resume_matched).then(|| {
                format!(
                    "resumable step {:?} not found in worker",
                    state.resume_step.as_deref().unwrap_or_default()
                )
            })
        })
    }
}

#[derive(Debug, Default)]
struct ResumableState {
    all_step_names: HashSet<String>,
    completed_step: Option<String>,
    cursors: Map<String, Value>,
    failure: Option<String>,
    had_cursors: bool,
    resume_matched: bool,
    resume_step: Option<String>,
    step_name: Option<String>,
}

enum StepAction {
    Run,
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
#[async_trait]
pub trait Worker<A>: Send + Sync + 'static
where
    A: JobArgs,
{
    /// Worker-specific error type. Errors use River's retry policy.
    type Error: StdError + Send + Sync + 'static;

    /// Overrides the client retry delay for this job. Returning `None` uses the
    /// client policy.
    fn next_retry(&self, _job: &Job<A>, _error: &str, _now: DateTime<Utc>) -> Option<Duration> {
        None
    }

    /// Overrides the client timeout for this job.
    fn timeout(&self, _job: &Job<A>) -> WorkerTimeout {
        WorkerTimeout::ClientDefault
    }

    /// Executes a job.
    async fn work(&self, context: WorkContext, job: Job<A>) -> Result<WorkOutcome, Self::Error>;
}

#[async_trait]
trait ErasedWorker: Send + Sync {
    fn next_retry(
        &self,
        row: &JobRow,
        error: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<Duration>, Box<dyn StdError + Send + Sync>>;

    fn timeout(&self, row: &JobRow) -> Result<WorkerTimeout, Box<dyn StdError + Send + Sync>>;

    async fn work(
        &self,
        context: WorkContext,
        row: &JobRow,
    ) -> Result<WorkOutcome, Box<dyn StdError + Send + Sync>>;
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
        error: &str,
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

    async fn work(
        &self,
        context: WorkContext,
        row: &JobRow,
    ) -> Result<WorkOutcome, Box<dyn StdError + Send + Sync>> {
        let args = serde_json::from_value(row.encoded_args.clone())?;
        self.worker
            .work(
                context,
                Job {
                    args,
                    row: row.clone(),
                },
            )
            .await
            .map_err(|error| Box::new(error) as Box<dyn StdError + Send + Sync>)
    }
}

/// Type-erased collection of workers keyed by job kind.
#[derive(Clone, Default)]
pub struct WorkerRegistry {
    workers: HashMap<&'static str, Arc<dyn ErasedWorker>>,
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
        error: &str,
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
            return Err(Error::InvalidJob(format!(
                "job kind must contain between 1 and 127 bytes: {:?}",
                A::KIND
            )));
        }
        let mut kinds = vec![A::KIND];
        for alias in A::kind_aliases() {
            if alias.is_empty() || alias.len() >= 128 {
                return Err(Error::InvalidJob(format!(
                    "job kind alias must contain between 1 and 127 bytes: {alias:?}"
                )));
            }
            if kinds.contains(alias) || self.workers.contains_key(alias) {
                return Err(Error::InvalidJob(format!(
                    "worker already registered for kind {alias:?}"
                )));
            }
            kinds.push(alias);
        }
        if self.workers.contains_key(A::KIND) {
            return Err(Error::InvalidJob(format!(
                "worker already registered for kind {:?}",
                A::KIND
            )));
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

    pub(crate) async fn work(
        &self,
        context: WorkContext,
        row: &JobRow,
    ) -> Result<WorkOutcome, Box<dyn StdError + Send + Sync>> {
        let worker = self.worker_for(row)?;
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
