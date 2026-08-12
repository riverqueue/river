//! Test helpers for River applications.

#![forbid(unsafe_code)]

use chrono::Utc;
use riverqueue::{Error, Job, JobArgs, JobRow, JobState, WorkContext, WorkOutcome, Worker};
use serde_json::Map;
use tokio_util::sync::CancellationToken;

/// Protocol revision understood by the Rust test helpers.
pub const PROTOCOL_REVISION: u32 = 1;

/// Builder for a realistic persisted job value usable in worker unit tests.
pub struct TestJobBuilder<A: JobArgs> {
    args: A,
    attempt: i16,
    id: i64,
    metadata: Map<String, serde_json::Value>,
    state: JobState,
}

impl<A: JobArgs> TestJobBuilder<A> {
    /// Starts a test job using River's defaults.
    #[must_use]
    pub fn new(args: A) -> Self {
        Self {
            args,
            attempt: 1,
            id: 1,
            metadata: Map::new(),
            state: JobState::Running,
        }
    }

    /// Sets the current attempt.
    #[must_use]
    pub const fn attempt(mut self, attempt: i16) -> Self {
        self.attempt = attempt;
        self
    }

    /// Builds the typed test job.
    pub fn build(self) -> Result<Job<A>, Error> {
        let now = Utc::now();
        let encoded_args = serde_json::to_value(&self.args)?;
        Ok(Job {
            args: self.args,
            row: JobRow {
                attempt: self.attempt,
                attempted_at: Some(now),
                attempted_by: vec!["riverqueue-test".to_owned()],
                created_at: now,
                encoded_args,
                errors: Vec::new(),
                finalized_at: None,
                id: self.id,
                kind: A::KIND.to_owned(),
                max_attempts: A::default_insert_opts().max_attempts,
                metadata: self.metadata,
                priority: A::default_insert_opts().priority,
                queue: A::default_insert_opts().queue,
                scheduled_at: now,
                state: self.state,
                tags: Vec::new(),
                unique_key: None,
                unique_states: None,
            },
        })
    }

    /// Sets the database ID.
    #[must_use]
    pub const fn id(mut self, id: i64) -> Self {
        self.id = id;
        self
    }

    /// Replaces metadata.
    #[must_use]
    pub fn metadata(mut self, metadata: Map<String, serde_json::Value>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Sets the persisted state.
    #[must_use]
    pub const fn state(mut self, state: JobState) -> Self {
        self.state = state;
        self
    }
}

/// Result of running one worker directly in a unit test.
pub struct TestWorkResult<E> {
    /// Context used for the invocation, including output and metadata updates.
    pub context: WorkContext,
    /// Worker outcome or typed error.
    pub result: Result<WorkOutcome, E>,
}

/// Runs a typed worker once without a database or background runtime.
pub async fn work_once<A, W>(worker: &W, job: Job<A>) -> TestWorkResult<W::Error>
where
    A: JobArgs,
    W: Worker<A>,
{
    let context = WorkContext::new(CancellationToken::new());
    let result = worker.work(context.clone(), job).await;
    TestWorkResult { context, result }
}
