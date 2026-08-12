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

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, time::Duration};

    use async_trait::async_trait;
    use riverqueue::{InsertOpts, WorkContext};
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct TestArgs {
        message: String,
    }

    impl JobArgs for TestArgs {
        const KIND: &'static str = "riverqueue_test_helper";

        fn default_insert_opts() -> InsertOpts {
            InsertOpts {
                max_attempts: 7,
                priority: 3,
                queue: "testing".to_owned(),
                ..InsertOpts::default()
            }
        }
    }

    struct TestWorker;

    #[async_trait]
    impl Worker<TestArgs> for TestWorker {
        type Error = Infallible;

        async fn work(
            &self,
            context: WorkContext,
            job: Job<TestArgs>,
        ) -> Result<WorkOutcome, Self::Error> {
            assert_eq!(job.args.message, "work once");
            assert_eq!(job.row.id, 42);
            context
                .record_output(&serde_json::json!({"worked": true}))
                .await
                .unwrap();
            context
                .metadata_set("worker_metadata", serde_json::json!("set"))
                .await;
            Ok(WorkOutcome::Snooze(Duration::from_secs(30)))
        }
    }

    #[test]
    fn test_job_builder_applies_overrides_and_argument_defaults() {
        let metadata = Map::from_iter([("test".to_owned(), serde_json::json!(true))]);
        let job = TestJobBuilder::new(TestArgs {
            message: "builder".to_owned(),
        })
        .attempt(4)
        .id(99)
        .metadata(metadata)
        .state(JobState::Retryable)
        .build()
        .unwrap();

        assert_eq!(job.args.message, "builder");
        assert_eq!(job.row.attempt, 4);
        assert_eq!(job.row.attempted_by, ["riverqueue-test"]);
        assert_eq!(
            job.row.encoded_args,
            serde_json::json!({"message": "builder"})
        );
        assert_eq!(job.row.id, 99);
        assert_eq!(job.row.kind, TestArgs::KIND);
        assert_eq!(job.row.max_attempts, 7);
        assert_eq!(job.row.metadata["test"], true);
        assert_eq!(job.row.priority, 3);
        assert_eq!(job.row.queue, "testing");
        assert_eq!(job.row.state, JobState::Retryable);
        assert!(job.row.attempted_at.is_some());
        assert!(job.row.finalized_at.is_none());
    }

    #[tokio::test]
    async fn work_once_runs_worker_with_detached_context() {
        let job = TestJobBuilder::new(TestArgs {
            message: "work once".to_owned(),
        })
        .id(42)
        .build()
        .unwrap();

        let worked = work_once(&TestWorker, job).await;

        assert_eq!(
            worked.result.unwrap(),
            WorkOutcome::Snooze(Duration::from_secs(30))
        );
        assert!(worked.context.client().is_none());
        assert!(!worked.context.cancellation_token().is_cancelled());
    }
}
