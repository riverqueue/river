//! PostgreSQL runtime behavior under malformed rows and database faults.
//!
//! These tests require `RIVER_RUST_DATABASE_URL` and fail when it is missing.
//! Each test migrates a uniquely named schema so concurrent runs against one
//! database cannot interfere.

#![cfg(feature = "postgres-tests")]

use std::{
    convert::Infallible,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use riverqueue::{
    Client, InsertOpts, Job, JobArgs, JobState, QueueConfig, WorkCancelled, WorkContext,
    WorkOutcome, WorkerRegistry,
    database::{PostgresDatabase, SchemaName},
};
use riverqueue_migrate::PostgresMigrator;
use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, PgPool};
use tokio::sync::Semaphore;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_postgres_resilience")]
struct ResilienceArgs {}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_postgres_resilience_gated")]
struct GatedArgs {}

/// A job that waits for client shutdown and then stops in the given way.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_postgres_resilience_shutdown")]
struct ShutdownArgs {
    behavior: String,
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

fn database_url() -> String {
    std::env::var("RIVER_RUST_DATABASE_URL")
        .expect("RIVER_RUST_DATABASE_URL must point at a disposable PostgreSQL test database")
}

/// A migrated schema with a unique name, dropped by [`TestSchema::drop`].
struct TestSchema {
    name: String,
    pool: PgPool,
    schema: SchemaName,
}

impl TestSchema {
    async fn new(label: &str) -> Self {
        static SCHEMA_NONCE: AtomicUsize = AtomicUsize::new(0);
        let name = format!(
            "river_res_{label}_{}_{}",
            std::process::id(),
            SCHEMA_NONCE.fetch_add(1, Ordering::Relaxed)
        );
        let pool = PgPool::connect(&database_url()).await.unwrap();
        sqlx::raw_sql(AssertSqlSafe(format!("CREATE SCHEMA {name}")))
            .execute(&pool)
            .await
            .unwrap();
        let schema = SchemaName::new(name.clone()).unwrap();
        PostgresMigrator::new(pool.clone())
            .with_schema(schema.clone())
            .migrate_up()
            .await
            .unwrap();
        Self { name, pool, schema }
    }

    fn database(&self) -> PostgresDatabase {
        PostgresDatabase::new(self.pool.clone()).schema(self.schema.clone())
    }

    fn table(&self) -> String {
        format!("{}.river_job", self.name)
    }

    async fn execute(&self, sql: String) {
        sqlx::raw_sql(AssertSqlSafe(sql))
            .execute(&self.pool)
            .await
            .unwrap();
    }

    async fn job_attempt(&self, id: i64) -> i16 {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT attempt FROM {} WHERE id = $1",
            self.table()
        )))
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn job_state(&self, id: i64) -> String {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT state::text FROM {} WHERE id = $1",
            self.table()
        )))
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn drop(self) {
        self.execute(format!("DROP SCHEMA {} CASCADE", self.name))
            .await;
        self.pool.close().await;
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
                match job.args.behavior.as_str() {
                    "cooperative" => Err(ShutdownError::Cancelled(WorkCancelled)),
                    "error" => Err(ShutdownError::Real),
                    "panic" => panic!("panic during shutdown"),
                    behavior => unreachable!("unknown shutdown behavior {behavior}"),
                }
            }
        })
        .unwrap();
    let gate = gate.clone();
    workers
        .register_fn(move |_context: WorkContext, _job: Job<GatedArgs>| {
            let gate = gate.clone();
            async move {
                gate.started.add_permits(1);
                gate.release.acquire().await.unwrap().forget();
                Ok::<_, Infallible>(WorkOutcome::Complete)
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

#[tokio::test(flavor = "multi_thread")]
async fn claimed_rows_decode_individually_and_leniently() {
    let schema = TestSchema::new("decode").await;
    let client = Client::builder(schema.database())
        .id("postgres-resilience-decode")
        .without_notifications()
        .workers(completing_workers())
        .queue("default", fast_queue())
        .build()
        .unwrap();

    // River Go decodes attempt errors with `encoding/json`, which tolerates
    // missing and unknown fields.
    let sparse_errors = client.insert(ResilienceArgs {}).await.unwrap();
    schema
        .execute(format!(
            "UPDATE {} SET errors = ARRAY['{{\"error\": \"go\", \"extra\": 1}}'::jsonb] \
             WHERE id = {}",
            schema.table(),
            sparse_errors.job.row.id
        ))
        .await;
    // Array metadata cannot become a `JobRow`. Claiming it with the others
    // must record a failure for it alone.
    let malformed = client
        .insert_with(
            ResilienceArgs {},
            InsertOpts::default().with_max_attempts(1),
        )
        .await
        .unwrap();
    schema
        .execute(format!(
            "UPDATE {} SET metadata = '[1]'::jsonb WHERE id = {}",
            schema.table(),
            malformed.job.row.id
        ))
        .await;
    let ordinary = client.insert(ResilienceArgs {}).await.unwrap();

    let run = client.start().unwrap();
    for id in [sparse_errors.job.row.id, ordinary.job.row.id] {
        wait_until(
            Duration::from_secs(10),
            "decodable job completion",
            || async { schema.job_state(id).await == "completed" },
        )
        .await;
    }
    wait_until(Duration::from_secs(10), "malformed job failure", || async {
        schema.job_state(malformed.job.row.id).await == "discarded"
    })
    .await;
    run.shutdown().await.unwrap();

    let sparse_errors = client.job_get(sparse_errors.job.row.id).await.unwrap();
    assert_eq!(sparse_errors.state, JobState::Completed);
    assert_eq!(sparse_errors.errors.len(), 1);
    assert_eq!(sparse_errors.errors[0].error, "go");
    assert_eq!(sparse_errors.errors[0].attempt, 0);

    let (attempt, errors): (i16, Vec<serde_json::Value>) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT attempt, errors FROM {} WHERE id = $1",
        schema.table()
    )))
    .bind(malformed.job.row.id)
    .fetch_one(&schema.pool)
    .await
    .unwrap();
    assert_eq!(attempt, 1);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["attempt"], 1);
    assert!(
        errors[0]["error"]
            .as_str()
            .unwrap()
            .starts_with("River could not decode the job row"),
        "{errors:?}"
    );

    schema.drop().await;
}

fn gated_client(schema: &TestSchema, id: &str, gate: &Gate) -> Client {
    Client::builder(schema.database())
        .id(id)
        .without_notifications()
        .workers(gated_workers(gate))
        .queue("default", fast_queue())
        .build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn completion_retries_a_transient_database_error() {
    let schema = TestSchema::new("retry").await;
    // Fail the first running-to-completed transition with a serialization
    // failure. A sequence records the injection outside the aborted statement.
    schema
        .execute(format!(
            "CREATE SEQUENCE {name}.completion_fault; \
             CREATE FUNCTION {name}.fail_completion_once() RETURNS trigger \
             LANGUAGE plpgsql AS $$ BEGIN \
                 IF OLD.state = 'running' AND NEW.state = 'completed' \
                    AND nextval('{name}.completion_fault') = 1 THEN \
                     RAISE EXCEPTION 'injected completion failure' USING ERRCODE = '40001'; \
                 END IF; \
                 RETURN NEW; \
             END $$; \
             CREATE TRIGGER fail_completion_once BEFORE UPDATE ON {name}.river_job \
             FOR EACH ROW EXECUTE FUNCTION {name}.fail_completion_once()",
            name = schema.name
        ))
        .await;
    let client = gated_client(&schema, "postgres-resilience-retry", &Gate::default());
    let job = client.insert(ResilienceArgs {}).await.unwrap();

    let run = client.start().unwrap();
    wait_until(
        Duration::from_secs(10),
        "completion after retry",
        || async { schema.job_state(job.job.row.id).await == "completed" },
    )
    .await;
    run.shutdown().await.unwrap();

    let injected: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT last_value FROM {}.completion_fault",
        schema.name
    )))
    .fetch_one(&schema.pool)
    .await
    .unwrap();
    assert!(injected >= 2, "the injected failure never fired");
    let job = client.job_get(job.job.row.id).await.unwrap();
    assert_eq!(job.attempt, 1);
    assert!(job.errors.is_empty());

    schema.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn completion_waits_for_a_row_lock() {
    let schema = TestSchema::new("lock").await;
    let gate = Gate::default();
    let client = gated_client(&schema, "postgres-resilience-lock", &gate);
    let job = client.insert(GatedArgs {}).await.unwrap();

    let run = client.start().unwrap();
    gate.wait_started().await;
    let mut locker = schema.pool.begin().await.unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "SELECT 1 FROM {} WHERE id = $1 FOR UPDATE",
        schema.table()
    )))
    .bind(job.job.row.id)
    .execute(&mut *locker)
    .await
    .unwrap();
    gate.release();
    wait_until(
        Duration::from_secs(10),
        "completion to wait on the lock",
        || async {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype = 'transactionid'",
            )
            .fetch_one(&schema.pool)
            .await
            .unwrap();
            waiting > 0
        },
    )
    .await;
    locker.commit().await.unwrap();
    wait_until(
        Duration::from_secs(10),
        "completion after unlock",
        || async { schema.job_state(job.job.row.id).await == "completed" },
    )
    .await;
    run.shutdown().await.unwrap();

    schema.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn completion_leaves_rows_moved_out_of_running_and_keeps_working() {
    let schema = TestSchema::new("moved").await;
    let gate = Gate::default();
    let client = gated_client(&schema, "postgres-resilience-moved", &gate);
    let pending = client.insert(GatedArgs {}).await.unwrap();

    let run = client.start().unwrap();
    gate.wait_started().await;
    // An operator or extension moves the running job back to `pending`.
    schema
        .execute(format!(
            "UPDATE {} SET state = 'pending' WHERE id = {}",
            schema.table(),
            pending.job.row.id
        ))
        .await;
    gate.release();
    let later = client.insert(ResilienceArgs {}).await.unwrap();
    wait_until(
        Duration::from_secs(10),
        "a later job to complete",
        || async { schema.job_state(later.job.row.id).await == "completed" },
    )
    .await;
    run.shutdown().await.unwrap();

    assert_eq!(schema.job_state(pending.job.row.id).await, "pending");

    schema.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn completion_does_not_rewrite_a_newer_attempt_number() {
    let schema = TestSchema::new("attempt").await;
    let gate = Gate::default();
    let client = gated_client(&schema, "postgres-resilience-attempt", &gate);
    let job = client.insert(GatedArgs {}).await.unwrap();

    let run = client.start().unwrap();
    gate.wait_started().await;
    // Another client rescued and refetched the job while this attempt ran.
    schema
        .execute(format!(
            "UPDATE {} SET attempt = 5 WHERE id = {}",
            schema.table(),
            job.job.row.id
        ))
        .await;
    gate.release();
    wait_until(Duration::from_secs(10), "stale completion", || async {
        schema.job_state(job.job.row.id).await == "completed"
    })
    .await;
    run.shutdown().await.unwrap();

    assert_eq!(schema.job_attempt(job.job.row.id).await, 5);

    schema.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn hard_shutdown_interrupts_only_cooperative_cancellations() {
    let schema = TestSchema::new("shutdown").await;
    let gate = Gate::default();
    let client = gated_client(&schema, "postgres-resilience-shutdown", &gate);
    let insert = |behavior: &str| {
        client.insert(ShutdownArgs {
            behavior: behavior.to_owned(),
        })
    };
    let cooperative = insert("cooperative").await.unwrap();
    let cancel_attempted = insert("cooperative").await.unwrap();
    let real_error = insert("error").await.unwrap();
    let panicked = insert("panic").await.unwrap();

    let mut listener = sqlx::postgres::PgListener::connect_with(&schema.pool)
        .await
        .unwrap();
    listener
        .listen(&format!("{}.river_insert", schema.name))
        .await
        .unwrap();
    let run = client.start().unwrap();
    for _ in 0..4 {
        gate.wait_started().await;
    }
    // A cancellation whose notification never reached this client.
    schema
        .execute(format!(
            "UPDATE {} SET metadata = jsonb_set(metadata, '{{cancel_attempted_at}}', \
             to_jsonb('2026-01-02T03:04:05Z'::text)) WHERE id = {}",
            schema.table(),
            cancel_attempted.job.row.id
        ))
        .await;
    tokio::time::timeout(Duration::from_secs(10), run.shutdown_now())
        .await
        .unwrap()
        .unwrap();

    let cooperative = client.job_get(cooperative.job.row.id).await.unwrap();
    assert_eq!(cooperative.state, JobState::Available);
    assert_eq!(cooperative.attempt, 0);
    assert!(cooperative.attempted_at.is_some(), "attempted_at is kept");
    assert!(cooperative.errors.is_empty());
    let notification = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("interrupted job did not notify peers")
        .unwrap();
    assert_eq!(notification.payload(), r#"{"queue" : "default"}"#);
    drop(listener);

    let cancel_attempted = client.job_get(cancel_attempted.job.row.id).await.unwrap();
    assert_eq!(cancel_attempted.state, JobState::Cancelled);
    assert!(cancel_attempted.finalized_at.is_some());

    for (job, error) in [
        (real_error, "real failure during shutdown"),
        (panicked, "panic during shutdown"),
    ] {
        let job = client.job_get(job.job.row.id).await.unwrap();
        assert!(
            matches!(job.state, JobState::Available | JobState::Retryable),
            "{:?}",
            job.state
        );
        assert_eq!(job.attempt, 1, "a genuine failure consumes its attempt");
        assert_eq!(job.errors.len(), 1);
        assert!(job.errors[0].error.contains(error), "{:?}", job.errors);
    }

    schema.drop().await;
}
