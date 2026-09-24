//! SQLite runtime behavior under malformed rows and database faults.

#![cfg(feature = "sqlite")]

use std::{
    convert::Infallible,
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use riverqueue::{
    AttemptError, BoxError, Client, ErrorHandler, ErrorHandlerDecision, EventKind, InsertOpts, Job,
    JobArgs, JobEventKind, JobRow, JobState, MaintenanceConfig, QueueConfig, RetryPolicy,
    UniqueOpts, WorkCancelled, WorkContext, WorkOutcome, WorkResult, WorkerRegistry,
};
use riverqueue_migrate::SqliteMigrator;
use serde::{Deserialize, Serialize};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use tokio::sync::Semaphore;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_sqlite_resilience")]
struct ResilienceArgs {}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_sqlite_resilience_gated")]
struct GatedArgs {
    fail: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("gated job failed")]
struct GatedError;

/// A job that waits for client shutdown, then stops cooperatively or fails.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_sqlite_resilience_shutdown")]
struct ShutdownArgs {
    cooperative: bool,
}

#[derive(Debug, thiserror::Error)]
enum ShutdownError {
    #[error("stopped for shutdown")]
    Cancelled(#[source] WorkCancelled),
    #[error("real failure during shutdown")]
    Real,
}

/// Lets a test hold a gated job inside its worker until released.
#[derive(Clone)]
struct Gate {
    release: Arc<Semaphore>,
    started: Arc<Semaphore>,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            release: Arc::new(Semaphore::new(0)),
            started: Arc::new(Semaphore::new(0)),
        }
    }
}

impl Gate {
    async fn wait_started(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.started.acquire())
            .await
            .expect("gated job did not start")
            .unwrap()
            .forget();
    }

    fn release(&self) {
        self.release.add_permits(1);
    }
}

/// A migrated WAL database file that is removed when the test finishes.
struct TestDatabase {
    path: PathBuf,
    pool: SqlitePool,
}

impl TestDatabase {
    async fn new(busy_timeout: Duration) -> Self {
        static DATABASE_NONCE: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "river-sqlite-resilience-{}-{}.sqlite",
            std::process::id(),
            DATABASE_NONCE.fetch_add(1, Ordering::Relaxed)
        ));
        let pool = Self::connect(&path, busy_timeout, 4).await;
        SqliteMigrator::new(pool.clone())
            .migrate_up()
            .await
            .unwrap();
        Self { path, pool }
    }

    async fn connect(path: &Path, busy_timeout: Duration, connections: u32) -> SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(connections)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(path)
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(busy_timeout),
            )
            .await
            .unwrap()
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        for suffix in ["-shm", "-wal"] {
            let mut path = self.path.as_os_str().to_owned();
            path.push(suffix);
            let _ = std::fs::remove_file(path);
        }
    }
}

fn completing_workers() -> WorkerRegistry {
    gated_workers(&Gate::default())
}

fn gated_workers(gate: &Gate) -> WorkerRegistry {
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(|_context: WorkContext, _job: Job<ResilienceArgs>| async {
            Ok::<_, Infallible>(WorkOutcome::Complete)
        })
        .unwrap();
    let shutdown_gate = gate.clone();
    workers
        .register_fn(move |context: WorkContext, job: Job<ShutdownArgs>| {
            let gate = shutdown_gate.clone();
            async move {
                gate.started.add_permits(1);
                context.cancellation_token().cancelled().await;
                if job.args.cooperative {
                    Err::<WorkOutcome, _>(ShutdownError::Cancelled(WorkCancelled))
                } else {
                    Err(ShutdownError::Real)
                }
            }
        })
        .unwrap();
    let gate = gate.clone();
    workers
        .register_fn(move |_context: WorkContext, job: Job<GatedArgs>| {
            let gate = gate.clone();
            async move {
                gate.started.add_permits(1);
                gate.release.acquire().await.unwrap().forget();
                if job.args.fail {
                    Err(GatedError)
                } else {
                    Ok(WorkOutcome::Complete)
                }
            }
        })
        .unwrap();
    workers
}

fn fast_queue() -> QueueConfig {
    QueueConfig::new(4)
        .with_fetch_cooldown(Duration::from_millis(1))
        .with_fetch_poll_interval(Duration::from_millis(20))
}

/// Polls `condition` until it holds, failing the test after `timeout`.
async fn wait_until<F, Fut>(timeout: Duration, description: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while !condition().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn job_state(pool: &SqlitePool, id: i64) -> String {
    sqlx::query_scalar("SELECT state FROM river_job WHERE id = ?")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Records every failed attempt the error handler sees.
#[derive(Clone, Default)]
struct RecordingErrorHandler(Arc<std::sync::Mutex<Vec<(JobRow, String)>>>);

#[allow(
    clippy::unused_async_trait_impl,
    reason = "these extensions only record state synchronously"
)]
impl ErrorHandler for RecordingErrorHandler {
    async fn handle_error(
        &self,
        _context: &WorkContext,
        job: &JobRow,
        result: &WorkResult,
    ) -> Result<ErrorHandlerDecision, BoxError> {
        let error = match result {
            WorkResult::Failed(error) => error.to_string(),
            other => format!("{other:?}"),
        };
        self.0.lock().unwrap().push((job.clone(), error));
        Ok(ErrorHandlerDecision::Continue)
    }
}

/// Schedules every retry an hour out so a failed job stays `retryable`.
struct RetryAnHourLater;

impl RetryPolicy for RetryAnHourLater {
    fn next_retry(&self, _job: &JobRow, _error: &str, _now: DateTime<Utc>) -> Duration {
        Duration::from_hours(1)
    }
}

async fn set_json_column(pool: &SqlitePool, id: i64, column: &str, json: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE river_job SET {column} = jsonb(?) WHERE id = ?"
    )))
    .bind(json)
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn claimed_rows_decode_individually_and_accept_go_integer_ranges() {
    let database = TestDatabase::new(Duration::from_secs(5)).await;
    let error_handler = RecordingErrorHandler::default();
    let client = Client::builder(database.pool.clone())
        .id("sqlite-resilience-decode")
        .maintenance(
            MaintenanceConfig::default().with_scheduler_interval(Duration::from_millis(50)),
        )
        .error_handler(error_handler.clone())
        .retry_policy(RetryAnHourLater)
        .workers(completing_workers())
        .queue("default", fast_queue())
        .build()
        .unwrap();
    let mut events = client
        .subscribe(&[EventKind::JobCompleted, EventKind::JobFailed])
        .unwrap();

    // River Go stores native integers on SQLite, so `max_attempts` can exceed
    // Rust's `i16`. Such a job must still be worked.
    let wide = client.insert(ResilienceArgs {}).await.unwrap();
    sqlx::query("UPDATE river_job SET max_attempts = 40000 WHERE id = ?")
        .bind(wide.job.row.id)
        .execute(&database.pool)
        .await
        .unwrap();
    // Attempt errors in a shape River doesn't write decode leniently like
    // River Go's, so the job is still worked.
    let odd_errors = client.insert(ResilienceArgs {}).await.unwrap();
    set_json_column(
        &database.pool,
        odd_errors.job.row.id,
        "errors",
        r#"[{"attempt": "1", "error": {"message": "boom"}}, 42]"#,
    )
    .await;
    // A row whose tags aren't an array can't become a `JobRow`. Claimed with
    // the others, such a job isn't worked, and its attempt fails like any
    // other: retried with the client's retry policy, or discarded at its
    // maximum attempts. An `errors` value that isn't an array is wrapped in
    // one so the attempt error can still be appended.
    let malformed_retried = client.insert(ResilienceArgs {}).await.unwrap();
    set_json_column(
        &database.pool,
        malformed_retried.job.row.id,
        "errors",
        r#"{"not":"an array"}"#,
    )
    .await;
    let malformed_discarded = client
        .insert(ResilienceArgs {})
        .opts(InsertOpts::default().with_max_attempts(1))
        .await
        .unwrap();
    let malformed_ids = [malformed_retried.job.row.id, malformed_discarded.job.row.id];
    for id in malformed_ids {
        set_json_column(&database.pool, id, "tags", r#"{"not":"an array"}"#).await;
    }
    let ordinary = client.insert(ResilienceArgs {}).await.unwrap();
    let decodable_ids = [wide.job.row.id, odd_errors.job.row.id, ordinary.job.row.id];

    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();
    let mut events_by_id = std::collections::HashMap::new();
    while events_by_id.len() < decodable_ids.len() + malformed_ids.len() {
        let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("job events")
            .unwrap();
        let event = event.as_job().unwrap().clone();
        events_by_id.insert(event.job.id, event);
    }
    run.shutdown().await.unwrap();

    for id in decodable_ids {
        assert_eq!(events_by_id[&id].kind, JobEventKind::Completed);
    }
    let (max_attempts, errors): (i64, Option<String>) =
        sqlx::query_as("SELECT max_attempts, json(errors) FROM river_job WHERE id = ?")
            .bind(wide.job.row.id)
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!(
        max_attempts, 40_000,
        "decoding must not rewrite the stored value"
    );
    assert!(errors.is_none());
    let wide = client.job_get(wide.job.row.id).await.unwrap();
    assert_eq!(wide.max_attempts, i16::MAX);

    let odd_errors = client.job_get(odd_errors.job.row.id).await.unwrap();
    let zero_time = "0001-01-01T00:00:00Z".parse().unwrap();
    assert_eq!(
        odd_errors.errors,
        [
            AttemptError::new(zero_time, 1, r#"{"message":"boom"}"#),
            AttemptError::new(zero_time, 0, "42"),
        ]
    );

    // Failed events and the error handler carry the fields that could be
    // decoded, with the others left empty.
    let handled = error_handler.0.lock().unwrap().clone();
    assert_eq!(handled.len(), malformed_ids.len());
    for (id, state) in [
        (malformed_retried.job.row.id, JobState::Retryable),
        (malformed_discarded.job.row.id, JobState::Discarded),
    ] {
        let event = &events_by_id[&id];
        assert_eq!(event.kind, JobEventKind::Failed);
        assert_eq!(event.job.state, state);
        assert_eq!(event.job.kind, ResilienceArgs::KIND);
        assert!(event.job.tags.is_empty());

        let (job, error) = handled.iter().find(|(job, _)| job.id == id).unwrap();
        assert_eq!(job.attempt, 1);
        assert!(job.tags.is_empty());
        assert!(
            error.starts_with("job row couldn't be decoded: "),
            "{error}"
        );
        assert!(error.contains("error unmarshaling `tags`: "), "{error}");

        // The attempt error is appended without rewriting the undecodable
        // tags, and the undecodable row still can't be read.
        assert!(client.job_get(id).await.is_err());
        let (stored_state, attempt, errors, tags, scheduled_at): (
            String,
            i64,
            String,
            String,
            DateTime<Utc>,
        ) = sqlx::query_as(
            "SELECT state, attempt, json(errors), json(tags), scheduled_at FROM river_job WHERE id = ?",
        )
        .bind(id)
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(stored_state, state.as_str());
        assert_eq!(attempt, 1);
        assert_eq!(tags, r#"{"not":"an array"}"#);
        let errors: Vec<serde_json::Value> = serde_json::from_str(&errors).unwrap();
        let appended = errors.last().unwrap();
        assert_eq!(appended["attempt"], 1);
        assert_eq!(appended["error"], error.as_str());
        if state == JobState::Retryable {
            assert_eq!(errors[0], serde_json::json!({"not": "an array"}));
            assert!(
                scheduled_at > Utc::now() + chrono::Duration::minutes(50),
                "the client retry policy wasn't used: {scheduled_at}"
            );
        } else {
            assert_eq!(errors.len(), 1);
        }
    }
}

// Like River Go's `JobGetStuck`, the rescuer reads a stuck job whose row
// can't be fully decoded, so it can recover it along with the others.
#[tokio::test(flavor = "multi_thread")]
async fn rescuer_recovers_undecodable_stuck_jobs() {
    let database = TestDatabase::new(Duration::from_secs(5)).await;
    let client = Client::builder(database.pool.clone())
        .id("sqlite-resilience-rescue")
        .job_timeout(Some(Duration::from_millis(100)))
        .maintenance(
            MaintenanceConfig::default()
                .with_elect_interval(Duration::from_millis(20))
                .with_rescue_after(Duration::from_millis(100))
                .with_rescuer_interval(Duration::from_millis(20)),
        )
        .retry_policy(RetryAnHourLater)
        .workers(completing_workers())
        .queue("default", fast_queue())
        .build()
        .unwrap();
    let mut stuck = Vec::new();
    for _ in 0..2 {
        let job = client.insert(ResilienceArgs {}).await.unwrap();
        sqlx::query(
            "UPDATE river_job SET state = 'running', attempt = 1, \
             attempted_at = datetime('now', '-1 hour') WHERE id = ?",
        )
        .bind(job.job.row.id)
        .execute(&database.pool)
        .await
        .unwrap();
        stuck.push(job.job.row.id);
    }
    set_json_column(&database.pool, stuck[0], "tags", r#"{"not":"an array"}"#).await;

    let mut run = client.start().unwrap();
    for id in &stuck {
        wait_until(Duration::from_secs(10), "stuck job rescue", || async {
            job_state(&database.pool, *id).await == "retryable"
        })
        .await;
    }
    run.shutdown().await.unwrap();

    let tags: String = sqlx::query_scalar("SELECT json(tags) FROM river_job WHERE id = ?")
        .bind(stuck[0])
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(tags, r#"{"not":"an array"}"#);
}

#[tokio::test(flavor = "multi_thread")]
async fn completion_retries_while_a_foreign_writer_holds_the_lock() {
    // The client gives up on a busy database after 50 ms, far less than the
    // foreign transaction below holds the write lock.
    let database = TestDatabase::new(Duration::from_millis(50)).await;
    let gate = Gate::default();
    let client = Client::builder(database.pool.clone())
        .id("sqlite-resilience-writer-lock")
        .without_notifications()
        .workers(gated_workers(&gate))
        .queue("default", fast_queue())
        .build()
        .unwrap();
    let job = client.insert(GatedArgs { fail: false }).await.unwrap();

    let mut run = client.start().unwrap();
    gate.wait_started().await;
    let foreign = TestDatabase::connect(&database.path, Duration::from_secs(5), 1).await;
    let writer = foreign.begin_with("BEGIN IMMEDIATE").await.unwrap();
    gate.release();
    // Hold the lock across several busy timeouts and the first retry.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    writer.rollback().await.unwrap();
    wait_until(
        Duration::from_secs(15),
        "completion after unlock",
        || async { job_state(&database.pool, job.job.row.id).await == "completed" },
    )
    .await;
    run.shutdown().await.unwrap();
    foreign.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn completion_cancels_on_a_null_cancel_attempted_at_key() {
    let database = TestDatabase::new(Duration::from_secs(5)).await;
    let gate = Gate::default();
    let client = Client::builder(database.pool.clone())
        .id("sqlite-resilience-cancel-key")
        .without_notifications()
        .workers(gated_workers(&gate))
        .queue("default", fast_queue())
        .build()
        .unwrap();
    let job = client.insert(GatedArgs { fail: true }).await.unwrap();

    let mut run = client.start().unwrap();
    gate.wait_started().await;
    // River Go treats the key's presence, not its value, as a cancellation.
    sqlx::query(
        "UPDATE river_job SET metadata = jsonb_set(metadata, '$.cancel_attempted_at', json('null')) \
         WHERE id = ?",
    )
    .bind(job.job.row.id)
    .execute(&database.pool)
    .await
    .unwrap();
    gate.release();
    wait_until(Duration::from_secs(10), "cancellation", || async {
        job_state(&database.pool, job.job.row.id).await == "cancelled"
    })
    .await;
    run.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn hard_shutdown_interrupts_only_cooperative_cancellations() {
    let database = TestDatabase::new(Duration::from_secs(5)).await;
    let gate = Gate::default();
    let client = Client::builder(database.pool.clone())
        .id("sqlite-resilience-shutdown")
        .without_notifications()
        .workers(gated_workers(&gate))
        .queue("default", fast_queue())
        .build()
        .unwrap();
    let cooperative = client
        .insert(ShutdownArgs { cooperative: true })
        .await
        .unwrap();
    let cancel_attempted = client
        .insert(ShutdownArgs { cooperative: true })
        .await
        .unwrap();
    let real_error = client
        .insert(ShutdownArgs { cooperative: false })
        .await
        .unwrap();

    let mut run = client.start().unwrap();
    for _ in 0..3 {
        gate.wait_started().await;
    }
    let notifications_before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM river_notification WHERE topic = 'river_insert'")
            .fetch_one(&database.pool)
            .await
            .unwrap();
    sqlx::query(
        "UPDATE river_job SET metadata = jsonb_set(metadata, '$.cancel_attempted_at', \
         '2026-01-02T03:04:05Z') WHERE id = ?",
    )
    .bind(cancel_attempted.job.row.id)
    .execute(&database.pool)
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), run.shutdown_now())
        .await
        .unwrap()
        .unwrap();

    let cooperative = client.job_get(cooperative.job.row.id).await.unwrap();
    assert_eq!(cooperative.state, JobState::Available);
    assert_eq!(cooperative.attempt, 0);
    assert!(cooperative.attempted_at.is_some(), "attempted_at is kept");
    assert!(cooperative.errors.is_empty());
    let notifications_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM river_notification WHERE topic = 'river_insert'")
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert!(notifications_after > notifications_before);

    let cancel_attempted = client.job_get(cancel_attempted.job.row.id).await.unwrap();
    assert_eq!(cancel_attempted.state, JobState::Cancelled);
    assert!(cancel_attempted.finalized_at.is_some());

    let real_error = client.job_get(real_error.job.row.id).await.unwrap();
    assert!(matches!(
        real_error.state,
        JobState::Available | JobState::Retryable
    ));
    assert_eq!(real_error.attempt, 1);
    assert_eq!(real_error.errors.len(), 1);
    assert_eq!(real_error.errors[0].error, "real failure during shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_attempted_at_matches_go_time_json() {
    let database = TestDatabase::new(Duration::from_secs(5)).await;
    let client = Client::builder(database.pool.clone())
        .id("sqlite-resilience-cancel-time")
        .build()
        .unwrap();
    let job = client.insert(ResilienceArgs {}).await.unwrap();
    let cancelled = client.job_cancel(job.job.row.id).await.unwrap();
    let cancel_attempted_at = cancelled.metadata["cancel_attempted_at"].as_str().unwrap();
    assert!(cancel_attempted_at.ends_with('Z'), "{cancel_attempted_at}");
    if let Some((_, fraction)) = cancel_attempted_at.trim_end_matches('Z').split_once('.') {
        assert!(
            !fraction.ends_with('0'),
            "trailing zeros are trimmed: {cancel_attempted_at}"
        );
    }
    chrono::DateTime::parse_from_rfc3339(cancel_attempted_at).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn notification_poll_failures_do_not_stop_the_client() {
    // In rollback-journal mode an exclusive writer blocks readers, so the
    // outbox poll fails with `database is locked` while the lock is held.
    static DATABASE_NONCE: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "river-sqlite-resilience-journal-{}-{}.sqlite",
        std::process::id(),
        DATABASE_NONCE.fetch_add(1, Ordering::Relaxed)
    ));
    let connect = |busy_timeout| {
        SqlitePoolOptions::new().max_connections(2).connect_with(
            SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Delete)
                .busy_timeout(busy_timeout),
        )
    };
    let pool = connect(Duration::from_millis(20)).await.unwrap();
    SqliteMigrator::new(pool.clone())
        .migrate_up()
        .await
        .unwrap();
    let database = TestDatabase {
        path: path.clone(),
        pool,
    };
    let client = Client::builder(database.pool.clone())
        .id("sqlite-resilience-outbox")
        .workers(completing_workers())
        .queue("default", fast_queue())
        .build()
        .unwrap();

    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();
    let foreign = connect(Duration::from_secs(5)).await.unwrap();
    let writer = foreign.begin_with("BEGIN EXCLUSIVE").await.unwrap();
    // Hold the lock across several 100 ms outbox polls.
    tokio::time::sleep(Duration::from_millis(500)).await;
    writer.rollback().await.unwrap();
    foreign.close().await;

    let job = client.insert(ResilienceArgs {}).await.unwrap();
    wait_until(Duration::from_secs(15), "work after the lock", || async {
        job_state(&database.pool, job.job.row.id).await == "completed"
    })
    .await;
    run.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn unique_duplicates_are_detected_across_clients_with_the_same_id() {
    let database = TestDatabase::new(Duration::from_secs(5)).await;
    // Default client IDs repeat across restarted containers, so duplicate
    // detection must not depend on them.
    let first = Client::builder(database.pool.clone())
        .id("sqlite-resilience-same-id")
        .build()
        .unwrap();
    let second = Client::builder(database.pool.clone())
        .id("sqlite-resilience-same-id")
        .build()
        .unwrap();
    let opts = InsertOpts::default().with_unique(UniqueOpts::new().by_args());

    let inserted = first
        .insert(ResilienceArgs {})
        .opts(opts.clone())
        .await
        .unwrap();
    assert!(!inserted.unique_skipped_as_duplicate);
    let duplicate = second.insert(ResilienceArgs {}).opts(opts).await.unwrap();
    assert!(duplicate.unique_skipped_as_duplicate);
    assert_eq!(duplicate.job.row.id, inserted.job.row.id);
}
