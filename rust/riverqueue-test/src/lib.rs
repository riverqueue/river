#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use chrono::Utc;
use riverqueue::{
    Error, Job, JobArgs, JobRow, JobState, MAX_ATTEMPTS_DEFAULT, PRIORITY_DEFAULT, QUEUE_DEFAULT,
    WorkContext, WorkError, WorkOutcome, Worker,
};
use serde_json::{Map, Value};

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
        let defaults = A::default_insert_opts();
        let max_attempts = defaults.max_attempts().unwrap_or(MAX_ATTEMPTS_DEFAULT);
        let priority = defaults.priority().unwrap_or(PRIORITY_DEFAULT);
        let queue = defaults.queue().unwrap_or(QUEUE_DEFAULT).to_owned();
        let mut row = JobRow::new(self.id, A::KIND, encoded_args, now);
        row.attempt = self.attempt;
        row.attempted_at = Some(now);
        row.attempted_by = vec!["riverqueue-test".to_owned()];
        row.max_attempts = max_attempts;
        row.metadata = self.metadata;
        row.priority = priority;
        row.queue = queue;
        row.state = self.state;
        Ok(Job::new(self.args, row))
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
#[non_exhaustive]
pub struct TestWorkResult<E> {
    /// Context used for the invocation, including output and metadata updates.
    pub context: WorkContext,
    metadata_updates: Map<String, Value>,
    /// Worker outcome, typed worker error, or runtime checkpoint error.
    pub result: Result<WorkOutcome, TestWorkError<E>>,
}

/// Failure from a direct worker invocation or River's resumable coordinator.
#[derive(Debug)]
#[non_exhaustive]
pub enum TestWorkError<E> {
    /// Invalid checkpoint metadata or a step error the worker suppresses.
    Resumable(WorkError),
    /// Original, unerased error returned by the worker.
    Worker(E),
}

impl<E: std::fmt::Display> std::fmt::Display for TestWorkError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Resumable(error) => error.fmt(formatter),
            Self::Worker(error) => error.fmt(formatter),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for TestWorkError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Resumable(error) => Some(error),
            Self::Worker(error) => Some(error),
        }
    }
}

impl<E> TestWorkResult<E> {
    /// Returns a snapshot of metadata recorded by the worker.
    #[must_use]
    pub const fn metadata_updates(&self) -> &Map<String, Value> {
        &self.metadata_updates
    }

    /// Returns output recorded by the worker, if any.
    #[must_use]
    pub fn output(&self) -> Option<&Value> {
        self.metadata_updates.get(riverqueue::METADATA_KEY_OUTPUT)
    }
}

/// Runs a typed worker once without a database or background runtime.
///
/// Initializes resumable steps from the job's metadata and records checkpoints
/// on failure, including step errors caught by the worker. This helper does not
/// simulate queue scheduling, timeouts, middleware, or database transactions.
pub async fn work_once<A, W>(worker: &W, job: Job<A>) -> TestWorkResult<W::Error>
where
    A: JobArgs,
    W: Worker<A>,
{
    let context = WorkContext::for_test_job(&job.row);
    let mut result = match context.resumable_validate().await {
        Ok(()) => worker
            .work(context.clone(), job)
            .await
            .map_err(TestWorkError::Worker),
        Err(error) => Err(TestWorkError::Resumable(error)),
    };
    if let Some(error) = context.resumable_finish(result.is_err()).await
        && result.is_ok()
    {
        result = Err(TestWorkError::Resumable(error));
    }
    let metadata_updates = context.metadata_updates().await;
    TestWorkResult {
        context,
        metadata_updates,
        result,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use riverqueue::{InsertOpts, WorkContext};
    use serde::{Deserialize, Serialize};

    use super::*;

    struct ResumableWorker {
        calls: AtomicUsize,
    }

    impl Worker<TestArgs> for ResumableWorker {
        type Error = Infallible;

        async fn work(
            &self,
            context: WorkContext,
            job: Job<TestArgs>,
        ) -> Result<WorkOutcome, Self::Error> {
            let _ = context
                .resumable_step("first", || async {
                    self.calls.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, std::io::Error>(())
                })
                .await;
            let _ = context
                .resumable_step("second", || async {
                    if job.row.attempt == 1 {
                        Err(std::io::Error::other("try again"))
                    } else {
                        Ok(())
                    }
                })
                .await;
            Ok(WorkOutcome::Complete)
        }
    }

    #[tokio::test]
    async fn work_once_checkpoints_suppressed_step_errors_and_resumes() {
        let worker = ResumableWorker {
            calls: AtomicUsize::new(0),
        };
        let args = TestArgs {
            message: "resume".to_owned(),
        };
        let first = work_once(&worker, TestJobBuilder::new(args.clone()).build().unwrap()).await;
        assert!(matches!(first.result, Err(TestWorkError::Resumable(_))));
        assert_eq!(
            first.metadata_updates()[riverqueue::METADATA_KEY_RESUMABLE_STEP],
            "first"
        );
        let second = work_once(
            &worker,
            TestJobBuilder::new(args)
                .attempt(2)
                .metadata(first.metadata_updates().clone())
                .build()
                .unwrap(),
        )
        .await;
        assert!(matches!(second.result, Ok(WorkOutcome::Complete)));
        assert_eq!(worker.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn work_once_rejects_invalid_checkpoint_before_work() {
        let worker = ResumableWorker {
            calls: AtomicUsize::new(0),
        };
        let job = TestJobBuilder::new(TestArgs {
            message: "invalid".to_owned(),
        })
        .metadata(
            serde_json::json!({ "river:resumable_cursor": [] })
                .as_object()
                .unwrap()
                .clone(),
        )
        .build()
        .unwrap();
        let outcome = work_once(&worker, job).await;
        assert!(matches!(outcome.result, Err(TestWorkError::Resumable(_))));
        assert_eq!(worker.calls.load(Ordering::Relaxed), 0);
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct TestArgs {
        message: String,
    }

    impl JobArgs for TestArgs {
        const KIND: &'static str = "riverqueue_test_helper";

        fn default_insert_opts() -> InsertOpts {
            InsertOpts::default()
                .with_max_attempts(7)
                .with_priority(3)
                .with_queue("testing")
        }
    }

    static DEFAULT_CALLS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, Deserialize, Serialize)]
    struct DefaultsOnceArgs {}

    impl JobArgs for DefaultsOnceArgs {
        const KIND: &'static str = "riverqueue_test_defaults_once";

        fn default_insert_opts() -> InsertOpts {
            DEFAULT_CALLS.fetch_add(1, Ordering::Relaxed);
            InsertOpts::default()
        }
    }

    struct TestWorker;

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

    #[test]
    fn test_job_builder_evaluates_argument_defaults_once() {
        DEFAULT_CALLS.store(0, Ordering::Relaxed);

        TestJobBuilder::new(DefaultsOnceArgs {}).build().unwrap();

        assert_eq!(DEFAULT_CALLS.load(Ordering::Relaxed), 1);
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

        assert_eq!(worked.output(), Some(&serde_json::json!({"worked": true})));
        assert_eq!(
            worked.metadata_updates()["worker_metadata"],
            serde_json::json!("set")
        );
        assert_eq!(
            worked.result.unwrap(),
            WorkOutcome::Snooze(Duration::from_secs(30))
        );
        assert!(worked.context.client().is_none());
        assert!(!worked.context.cancellation_token().is_cancelled());
    }
}
