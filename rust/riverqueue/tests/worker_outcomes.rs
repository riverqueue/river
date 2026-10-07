//! What a worked job's row records for each way an attempt ends: worker
//! outcomes, errors, panics, timeouts, resumable step validation, and remote
//! cancellation of an attempt fetched again after a snooze.
//!
//! Outcome handling doesn't depend on the backend, so these tests use
//! temporary SQLite databases and need no external services.

#![cfg(feature = "sqlite")]

mod support;

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use riverqueue::{
    BoxError, Client, EventKind, InsertOpts, Job, JobArgs, JobCancelError, JobRow, JobState,
    QueueConfig, WorkCancelled, WorkContext, WorkOutcome, Worker, WorkerTimeout, Workers,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::SqlitePool;
use tokio::sync::Semaphore;

use crate::support::{sqlite_cleanup, sqlite_file_pool};

/// Every wait in these tests is bounded by this timeout. It covers a few of
/// SQLite's two-second polls for cancellation requests.
const WAIT: Duration = Duration::from_secs(10);

/// The timeout of jobs whose behavior is [`Behavior::Timeout`].
const JOB_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Behavior {
    Cancel,
    CancelWithReason,
    Discard,
    Error,
    Output,
    Panic,
    SnoozeOnceThenWaitForCancel,
    Timeout,
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_worker_outcomes")]
struct OutcomeArgs {
    behavior: Behavior,
}

/// One worker implementation for every outcome, following each job's
/// behavior.
#[derive(Clone)]
struct OutcomeWorker {
    /// Gains a permit when a snoozed job's second attempt starts.
    refetched: Arc<Semaphore>,
}

impl Worker<OutcomeArgs> for OutcomeWorker {
    type Error = BoxError;

    fn timeout(&self, job: &Job<OutcomeArgs>) -> WorkerTimeout {
        match job.args.behavior {
            Behavior::Timeout => WorkerTimeout::After(JOB_TIMEOUT),
            _ => WorkerTimeout::ClientDefault,
        }
    }

    async fn work(
        &self,
        context: WorkContext,
        job: Job<OutcomeArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        match job.args.behavior {
            Behavior::Cancel => Ok(WorkOutcome::Cancel),
            Behavior::CancelWithReason => Err(JobCancelError::new("card expired").into()),
            Behavior::Discard => Ok(WorkOutcome::Discard),
            Behavior::Error => Err("intentional failure".into()),
            Behavior::Output => {
                context.record_output(json!({"message": "recorded"}))?;
                Ok(WorkOutcome::Complete)
            }
            Behavior::Panic => panic!("worker panicked on purpose"),
            Behavior::SnoozeOnceThenWaitForCancel => {
                if job.row.metadata.get::<i64>("snoozes")?.is_none() {
                    return Ok(WorkOutcome::Snooze(Duration::ZERO));
                }
                self.refetched.add_permits(1);
                context.cancellation_token().cancelled().await;
                Err(WorkCancelled.into())
            }
            // Cooperates with the cancellation from its timeout.
            Behavior::Timeout => {
                context.cancellation_token().cancelled().await;
                Err(WorkCancelled.into())
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_worker_outcomes_resumable")]
struct ResumableArgs {
    /// Runs the first step twice.
    duplicate: bool,
}

/// Records the resumable steps that ran, and whether the worker was called.
#[derive(Clone, Default)]
struct ResumableWorker(Arc<Mutex<Vec<&'static str>>>);

impl Worker<ResumableArgs> for ResumableWorker {
    type Error = BoxError;

    async fn work(
        &self,
        context: WorkContext,
        job: Job<ResumableArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        self.0.lock().unwrap().push("worker");
        let step = |name: &'static str| {
            let ran = Arc::clone(&self.0);
            move || async move {
                ran.lock().unwrap().push(name);
                Ok::<_, BoxError>(())
            }
        };
        context.resumable_step("first", step("first")).await?;
        if job.args.duplicate {
            // Like Go's `ResumableStep`, a duplicate name fails the job even
            // when the worker ignores the step's error.
            let _ = context.resumable_step("first", step("first again")).await;
        }
        context.resumable_step("second", step("second")).await?;
        Ok(WorkOutcome::Complete)
    }
}

fn workers(refetched: &Arc<Semaphore>, resumable: &ResumableWorker) -> Workers {
    let mut workers = Workers::new();
    workers
        .add(OutcomeWorker {
            refetched: Arc::clone(refetched),
        })
        .unwrap()
        .add(resumable.clone())
        .unwrap();
    workers
}

fn client(pool: &SqlitePool, workers: Workers) -> Client {
    Client::builder(pool.clone())
        .queue(
            "default",
            QueueConfig::new(4)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .workers(workers)
        .build()
        .unwrap()
}

fn single_attempt() -> InsertOpts {
    InsertOpts::default().with_max_attempts(1)
}

/// Works jobs until each of `ids` is finalized, then returns their rows in
/// the same order.
async fn work_until_finalized(client: &Client, ids: &[i64]) -> Vec<JobRow> {
    // Subscribe before starting, so no event can be missed.
    let mut events = client
        .subscribe(&[
            EventKind::JobCancelled,
            EventKind::JobCompleted,
            EventKind::JobFailed,
        ])
        .unwrap();
    let mut run = client.start().unwrap();
    let mut pending: HashSet<i64> = ids.iter().copied().collect();
    tokio::time::timeout(WAIT, async {
        while !pending.is_empty() {
            let event = events.recv().await.unwrap();
            if let Some(event) = event.as_job()
                && event.job.finalized_at.is_some()
            {
                pending.remove(&event.job.id);
            }
        }
    })
    .await
    .expect("every job is finalized");
    run.stop().await.unwrap();

    let mut rows = Vec::new();
    for id in ids {
        rows.push(client.jobs().get(*id).await.unwrap());
    }
    rows
}

/// Asserts that `row` finalized after one attempt with one error from it.
#[track_caller]
fn assert_one_failed_attempt(row: &JobRow, state: JobState) {
    assert_eq!(row.state, state, "{row:?}");
    assert_eq!(row.attempt, 1, "{row:?}");
    assert!(row.finalized_at.is_some(), "{row:?}");
    assert_eq!(row.errors.len(), 1, "{row:?}");
    assert_eq!(row.errors[0].attempt, 1, "{row:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn each_worker_outcome_finalizes_the_job_like_go() {
    let (pool, path) = sqlite_file_pool(4).await;
    let client = client(
        &pool,
        workers(&Arc::new(Semaphore::new(0)), &ResumableWorker::default()),
    );
    let mut ids = Vec::new();
    for behavior in [
        Behavior::Cancel,
        Behavior::Discard,
        Behavior::Error,
        Behavior::Output,
    ] {
        let inserted = client
            .insert(OutcomeArgs { behavior })
            .opts(single_attempt())
            .await
            .unwrap();
        ids.push(inserted.id());
    }
    // Cancelling with a reason, like Go's `JobCancel(err)`, cancels a job
    // with attempts left and records the reason.
    let cancelled_with_reason = client
        .insert(OutcomeArgs {
            behavior: Behavior::CancelWithReason,
        })
        .opts(InsertOpts::default().with_max_attempts(3))
        .await
        .unwrap();
    ids.push(cancelled_with_reason.id());
    let rows = work_until_finalized(&client, &ids).await;

    assert_one_failed_attempt(&rows[0], JobState::Cancelled);
    assert_one_failed_attempt(&rows[1], JobState::Discarded);
    assert_one_failed_attempt(&rows[2], JobState::Discarded);
    assert_eq!(rows[2].errors[0].error, "intentional failure");
    assert_one_failed_attempt(&rows[4], JobState::Cancelled);
    assert_eq!(rows[4].errors[0].error, "JobCancelError: card expired");

    let completed = &rows[3];
    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(completed.attempt, 1);
    assert!(completed.finalized_at.is_some());
    assert_eq!(completed.errors, []);
    assert_eq!(
        completed.decode_output::<serde_json::Value>().unwrap(),
        Some(json!({"message": "recorded"}))
    );
    sqlite_cleanup(pool, path).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn panics_discard_the_last_attempt_with_value_and_trace() {
    let (pool, path) = sqlite_file_pool(4).await;
    let client = client(
        &pool,
        workers(&Arc::new(Semaphore::new(0)), &ResumableWorker::default()),
    );
    let inserted = client
        .insert(OutcomeArgs {
            behavior: Behavior::Panic,
        })
        .opts(single_attempt())
        .await
        .unwrap();
    let rows = work_until_finalized(&client, &[inserted.id()]).await;

    let discarded = &rows[0];
    assert_one_failed_attempt(discarded, JobState::Discarded);
    assert!(
        discarded.errors[0]
            .error
            .contains("worker panicked on purpose"),
        "{:?}",
        discarded.errors
    );
    assert_ne!(discarded.errors[0].trace, "");
    sqlite_cleanup(pool, path).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_cancellation_reaches_an_attempt_refetched_after_a_snooze() {
    let (pool, path) = sqlite_file_pool(4).await;
    let refetched = Arc::new(Semaphore::new(0));
    let client = client(&pool, workers(&refetched, &ResumableWorker::default()));
    let other = Client::builder(pool.clone()).build().unwrap();
    let mut events = client.subscribe(&[EventKind::JobCancelled]).unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    let id = other
        .insert(OutcomeArgs {
            behavior: Behavior::SnoozeOnceThenWaitForCancel,
        })
        .await
        .unwrap()
        .id();
    tokio::time::timeout(WAIT, refetched.acquire())
        .await
        .expect("the snoozed job runs again")
        .unwrap()
        .forget();
    other.jobs().cancel(id).await.unwrap();

    let event = tokio::time::timeout(WAIT, events.recv())
        .await
        .expect("the second attempt is cancelled")
        .unwrap();
    assert_eq!(event.as_job().map(|event| event.job.id), Some(id));
    run.stop().await.unwrap();

    let cancelled = client.jobs().get(id).await.unwrap();
    // The snooze gave its attempt back, so the second attempt is attempt 1.
    assert_one_failed_attempt(&cancelled, JobState::Cancelled);
    assert_eq!(cancelled.metadata.get::<i64>("snoozes").unwrap(), Some(1));
    sqlite_cleanup(pool, path).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn resumable_steps_validate_names_and_cursor_metadata() {
    let (pool, path) = sqlite_file_pool(4).await;
    let resumable = ResumableWorker::default();
    let client = client(&pool, workers(&Arc::new(Semaphore::new(0)), &resumable));
    let metadata = |metadata: serde_json::Value| {
        InsertOpts::default()
            .with_max_attempts(1)
            .with_metadata(metadata.as_object().unwrap().clone())
    };
    let cases = [
        (false, json!({"river:resumable_cursor": [1]})),
        (true, json!({})),
        (true, json!({"river:resumable_step": "second"})),
        (false, json!({"river:resumable_step": ""})),
    ];

    // One case at a time, so each case's steps are recorded separately.
    let mut results = Vec::new();
    for (duplicate, case) in cases {
        let id = client
            .insert(ResumableArgs { duplicate })
            .opts(metadata(case))
            .await
            .unwrap()
            .id();
        let row = work_until_finalized(&client, &[id]).await.remove(0);
        results.push((row, std::mem::take(&mut *resumable.0.lock().unwrap())));
    }

    // Cursor metadata that isn't an object fails the attempt before the
    // worker runs.
    let (row, ran) = &results[0];
    assert_one_failed_attempt(row, JobState::Discarded);
    assert!(
        row.errors[0].error.contains("resumable_cursor"),
        "{:?}",
        row.errors
    );
    assert_eq!(*ran, [] as [&str; 0]);

    // A duplicate step name fails the job, both when the first step runs
    // and when it's skipped because a later step is recorded.
    let (row, ran) = &results[1];
    assert_one_failed_attempt(row, JobState::Discarded);
    assert!(
        row.errors[0]
            .error
            .contains(r#"duplicate resumable step name "first""#),
        "{:?}",
        row.errors
    );
    assert_eq!(*ran, ["worker", "first"]);
    let (row, ran) = &results[2];
    assert_one_failed_attempt(row, JobState::Discarded);
    assert!(
        row.errors[0]
            .error
            .contains(r#"duplicate resumable step name "first""#),
        "{:?}",
        row.errors
    );
    assert_eq!(*ran, ["worker"]);

    // An empty recorded step restarts from the first step.
    let (row, ran) = &results[3];
    assert_eq!(row.state, JobState::Completed, "{row:?}");
    assert_eq!(*ran, ["worker", "first", "second"]);
    sqlite_cleanup(pool, path).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn timeouts_cancel_cooperative_workers() {
    let (pool, path) = sqlite_file_pool(4).await;
    let client = client(
        &pool,
        workers(&Arc::new(Semaphore::new(0)), &ResumableWorker::default()),
    );
    let inserted = client
        .insert(OutcomeArgs {
            behavior: Behavior::Timeout,
        })
        .opts(single_attempt())
        .await
        .unwrap();
    let rows = work_until_finalized(&client, &[inserted.id()]).await;

    let discarded = &rows[0];
    assert_one_failed_attempt(discarded, JobState::Discarded);
    let ran_for = discarded.finalized_at.unwrap() - discarded.attempted_at.unwrap();
    assert!(
        ran_for >= riverqueue::chrono::Duration::from_std(JOB_TIMEOUT).unwrap(),
        "finalized {ran_for} after the attempt started"
    );
    sqlite_cleanup(pool, path).await;
}
