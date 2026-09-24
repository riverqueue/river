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

use riverqueue::{
    Client, InsertOpts, Job, JobArgs, JobState, MaintenanceConfig, QueueConfig, WorkContext,
    WorkOutcome, WorkerRegistry,
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

#[tokio::test(flavor = "multi_thread")]
async fn claimed_rows_decode_individually_and_accept_go_integer_ranges() {
    let database = TestDatabase::new(Duration::from_secs(5)).await;
    let client = Client::builder(database.pool.clone())
        .id("sqlite-resilience-decode")
        .maintenance(
            MaintenanceConfig::default().with_scheduler_interval(Duration::from_millis(50)),
        )
        .workers(completing_workers())
        .queue("default", fast_queue())
        .build()
        .unwrap();

    // River Go stores native integers on SQLite, so `max_attempts` can exceed
    // Rust's `i16`. Such a job must still be worked.
    let wide = client.insert(ResilienceArgs {}).await.unwrap();
    sqlx::query("UPDATE river_job SET max_attempts = 40000 WHERE id = ?")
        .bind(wide.job.row.id)
        .execute(&database.pool)
        .await
        .unwrap();
    // A row whose tags are not an array cannot become a `JobRow`. Claiming it
    // with the others must record a failure for it alone.
    let malformed = client
        .insert_with(
            ResilienceArgs {},
            InsertOpts::default().with_max_attempts(1),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE river_job SET tags = jsonb('{}') WHERE id = ?")
        .bind(malformed.job.row.id)
        .execute(&database.pool)
        .await
        .unwrap();
    let ordinary = client.insert(ResilienceArgs {}).await.unwrap();

    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();
    for id in [wide.job.row.id, ordinary.job.row.id] {
        wait_until(
            Duration::from_secs(10),
            "decodable job completion",
            || async { job_state(&database.pool, id).await == "completed" },
        )
        .await;
    }
    wait_until(Duration::from_secs(10), "malformed job failure", || async {
        job_state(&database.pool, malformed.job.row.id).await == "discarded"
    })
    .await;
    run.shutdown().await.unwrap();

    let (max_attempts, errors): (i64, String) =
        sqlx::query_as("SELECT max_attempts, json(errors) FROM river_job WHERE id = ?")
            .bind(wide.job.row.id)
            .fetch_one(&database.pool)
            .await
            .map(|(max_attempts, errors): (i64, Option<String>)| {
                (max_attempts, errors.unwrap_or_default())
            })
            .unwrap();
    assert_eq!(
        max_attempts, 40_000,
        "decoding must not rewrite the stored value"
    );
    assert!(errors.is_empty());
    let wide = client.job_get(wide.job.row.id).await.unwrap();
    assert_eq!(wide.state, JobState::Completed);
    assert_eq!(wide.max_attempts, i16::MAX);

    let (attempt, errors): (i64, String) =
        sqlx::query_as("SELECT attempt, json(errors) FROM river_job WHERE id = ?")
            .bind(malformed.job.row.id)
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!(attempt, 1);
    let errors: Vec<serde_json::Value> = serde_json::from_str(&errors).unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["attempt"], 1);
    assert!(
        errors[0]["error"]
            .as_str()
            .unwrap()
            .starts_with("River could not decode the job row"),
        "{errors:?}"
    );
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

    let run = client.start().unwrap();
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

    let run = client.start().unwrap();
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
