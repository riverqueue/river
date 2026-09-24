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

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use riverqueue::__private::ClientBuilderExt;
use riverqueue::{
    __private::{DatabaseConnection, JobSetStateParams, Pilot, PilotError},
    AttemptError, BoxError, Client, ErrorHandler, ErrorHandlerDecision, EventKind, InsertOpts, Job,
    JobArgs, JobEventKind, JobRow, JobState, QueueConfig, RetryPolicy, WorkCancelled, WorkContext,
    WorkOutcome, WorkResult, WorkerRegistry,
    database::{PostgresDatabase, SchemaName},
};
use riverqueue_migrate::PostgresMigrator;
use serde::{Deserialize, Serialize};
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{net::TcpListener, sync::Semaphore, task::AbortHandle};

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

/// A job that blocks its thread without yielding, so Tokio cannot abort it
/// until the blocking section ends.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_postgres_resilience_blocking")]
struct BlockingArgs {
    block_ms: u64,
}

/// Records when blocking work finished and when later work started.
#[derive(Default)]
struct BlockingTimeline {
    blocking_finished: std::sync::Mutex<Option<std::time::Instant>>,
    later_started: std::sync::Mutex<Option<std::time::Instant>>,
}

/// Signals every stuck job.
#[derive(Clone)]
struct StuckSignal(Arc<Semaphore>);

#[allow(
    clippy::unused_async_trait_impl,
    reason = "these extensions only record state synchronously"
)]
impl ErrorHandler for StuckSignal {
    async fn handle_stuck(&self, _job: &JobRow) -> Result<(), BoxError> {
        self.0.add_permits(1);
        Ok(())
    }
}

fn blocking_workers(gate: &Gate, timeline: &Arc<BlockingTimeline>) -> WorkerRegistry {
    let mut workers = WorkerRegistry::new();
    let blocking_gate = gate.clone();
    let blocking_timeline = Arc::clone(timeline);
    workers
        .register_fn(move |_context: WorkContext, job: Job<BlockingArgs>| {
            let gate = blocking_gate.clone();
            let timeline = Arc::clone(&blocking_timeline);
            async move {
                gate.started.add_permits(1);
                // Deliberately ignore cancellation without an await point.
                std::thread::sleep(Duration::from_millis(job.args.block_ms));
                *timeline.blocking_finished.lock().unwrap() = Some(std::time::Instant::now());
                tokio::task::yield_now().await;
                Ok::<_, Infallible>(WorkOutcome::Complete)
            }
        })
        .unwrap();
    let later_timeline = Arc::clone(timeline);
    workers
        .register_fn(move |_context: WorkContext, _job: Job<ResilienceArgs>| {
            let timeline = Arc::clone(&later_timeline);
            async move {
                timeline
                    .later_started
                    .lock()
                    .unwrap()
                    .get_or_insert_with(std::time::Instant::now);
                Ok::<_, Infallible>(WorkOutcome::Complete)
            }
        })
        .unwrap();
    workers
}

/// A job that snoozes for longer than any representable schedule.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_postgres_resilience_snooze")]
struct SnoozeForeverArgs {}

/// Lets a test hold a gated job inside its worker until released.
#[derive(Clone)]
struct Gate {
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
    started: Arc<Semaphore>,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
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

/// A TCP proxy between a client and PostgreSQL that can make the database
/// unavailable: it resets open connections and refuses new ones until
/// restored. Unlike terminating backends, this keeps the database down for
/// the client while other connections still work.
struct FaultProxy {
    accept_task: AbortHandle,
    connections: Arc<std::sync::Mutex<Vec<AbortHandle>>>,
    options: PgConnectOptions,
    rejected: Arc<AtomicUsize>,
    up: Arc<std::sync::atomic::AtomicBool>,
}

impl FaultProxy {
    async fn start() -> Self {
        let upstream: PgConnectOptions = database_url().parse().unwrap();
        let upstream_host = upstream.get_host().to_owned();
        let upstream_port = upstream.get_port();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connections = Arc::new(std::sync::Mutex::new(Vec::<AbortHandle>::new()));
        let rejected = Arc::new(AtomicUsize::new(0));
        let up = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let accept_connections = Arc::clone(&connections);
        let accept_rejected = Arc::clone(&rejected);
        let accept_up = Arc::clone(&up);
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok((mut client, _)) = listener.accept().await else {
                    continue;
                };
                if !accept_up.load(Ordering::SeqCst) {
                    accept_rejected.fetch_add(1, Ordering::SeqCst);
                    drop(client);
                    continue;
                }
                let host = upstream_host.clone();
                let connection = tokio::spawn(async move {
                    if host.starts_with('/') {
                        let path = format!("{host}/.s.PGSQL.{upstream_port}");
                        if let Ok(mut server) = tokio::net::UnixStream::connect(path).await {
                            let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                        }
                    } else if let Ok(mut server) =
                        tokio::net::TcpStream::connect((host.as_str(), upstream_port)).await
                    {
                        let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                    }
                });
                accept_connections
                    .lock()
                    .unwrap()
                    .push(connection.abort_handle());
            }
        })
        .abort_handle();
        let options = upstream.host("127.0.0.1").port(address.port());
        Self {
            accept_task,
            connections,
            options,
            rejected,
            up,
        }
    }

    /// A pool that reaches PostgreSQL only through the proxy. A short acquire
    /// timeout keeps operations failing quickly while the database is down.
    fn pool(&self, max_connections: u32) -> PgPool {
        PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(500))
            .max_connections(max_connections)
            .connect_lazy_with(self.options.clone())
    }

    fn take_down(&self) {
        self.up.store(false, Ordering::SeqCst);
        for connection in self.connections.lock().unwrap().drain(..) {
            connection.abort();
        }
    }

    fn restore(&self) {
        self.up.store(true, Ordering::SeqCst);
    }

    async fn wait_for_rejections(&self, count: usize) {
        wait_until(Duration::from_secs(30), "reconnection attempts", || async {
            self.rejected.load(Ordering::SeqCst) >= count
        })
        .await;
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.accept_task.abort();
        for connection in self.connections.lock().unwrap().drain(..) {
            connection.abort();
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
    workers
        .register_fn(
            |_context: WorkContext, _job: Job<SnoozeForeverArgs>| async {
                Ok::<_, Infallible>(WorkOutcome::Snooze(Duration::MAX))
            },
        )
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
                let active = gate.active.fetch_add(1, Ordering::SeqCst) + 1;
                gate.max_active.fetch_max(active, Ordering::SeqCst);
                gate.started.add_permits(1);
                gate.release.acquire().await.unwrap().forget();
                gate.active.fetch_sub(1, Ordering::SeqCst);
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

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn claimed_rows_decode_individually_and_leniently() {
    let schema = TestSchema::new("decode").await;
    let error_handler = RecordingErrorHandler::default();
    let client = Client::builder(schema.database())
        .id("postgres-resilience-decode")
        .without_notifications()
        .error_handler(error_handler.clone())
        .retry_policy(RetryAnHourLater)
        .workers(completing_workers())
        .queue("default", fast_queue())
        .build()
        .unwrap();
    let mut events = client
        .subscribe(&[EventKind::JobCompleted, EventKind::JobFailed])
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
    // Attempt errors in a shape River doesn't write decode leniently like
    // River Go's, so the job is still worked.
    let odd_errors = client.insert(ResilienceArgs {}).await.unwrap();
    schema
        .execute(format!(
            "UPDATE {} SET errors = ARRAY[\
                '{{\"at\": \"2024-01-02 03:04:05+00\", \"attempt\": \"1\", \
                  \"error\": {{\"message\": \"boom\"}}, \"trace\": [\"frame\"]}}'::jsonb, \
                '42'::jsonb] \
             WHERE id = {}",
            schema.table(),
            odd_errors.job.row.id
        ))
        .await;
    // Array metadata can't become a `JobRow`. Claimed with the others, such a
    // job isn't worked, and its attempt fails like any other: retried with
    // the client's retry policy, or discarded at its maximum attempts.
    let malformed_retried = client.insert(ResilienceArgs {}).await.unwrap();
    let malformed_discarded = client
        .insert(ResilienceArgs {})
        .opts(InsertOpts::default().with_max_attempts(1))
        .await
        .unwrap();
    let malformed_ids = [malformed_retried.job.row.id, malformed_discarded.job.row.id];
    for id in malformed_ids {
        schema
            .execute(format!(
                "UPDATE {} SET metadata = '[1]'::jsonb WHERE id = {id}",
                schema.table()
            ))
            .await;
    }
    let ordinary = client.insert(ResilienceArgs {}).await.unwrap();
    let decodable_ids = [
        sparse_errors.job.row.id,
        odd_errors.job.row.id,
        ordinary.job.row.id,
    ];

    let mut run = client.start().unwrap();
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
    let sparse_errors = client.job_get(sparse_errors.job.row.id).await.unwrap();
    assert_eq!(sparse_errors.errors.len(), 1);
    assert_eq!(sparse_errors.errors[0].error, "go");
    assert_eq!(sparse_errors.errors[0].attempt, 0);
    let odd_errors = client.job_get(odd_errors.job.row.id).await.unwrap();
    assert_eq!(
        odd_errors.errors,
        [
            AttemptError::new(
                "2024-01-02T03:04:05Z".parse().unwrap(),
                1,
                r#"{"message":"boom"}"#
            )
            .with_trace(r#"["frame"]"#),
            AttemptError::new("0001-01-01T00:00:00Z".parse().unwrap(), 0, "42"),
        ]
    );

    // Failed events and the error handler carry the fields that could be
    // decoded, with the metadata left empty.
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
        assert!(event.job.metadata.is_empty());
        assert!(
            event.job.errors[0]
                .error
                .starts_with("job row couldn't be decoded: error unmarshaling `metadata`: "),
            "{:?}",
            event.job.errors
        );

        let (job, error) = handled.iter().find(|(job, _)| job.id == id).unwrap();
        assert_eq!(job.attempt, 1);
        assert!(job.metadata.is_empty());
        assert_eq!(error, &event.job.errors[0].error);

        // The attempt error is appended without rewriting the undecodable
        // metadata.
        let (state, attempt, errors, metadata, scheduled_at): (
            String,
            i16,
            Vec<serde_json::Value>,
            serde_json::Value,
            DateTime<Utc>,
        ) = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT state::text, attempt, errors, metadata, scheduled_at FROM {} WHERE id = $1",
            schema.table()
        )))
        .bind(id)
        .fetch_one(&schema.pool)
        .await
        .unwrap();
        assert_eq!(state, event.job.state.as_str());
        assert_eq!(attempt, 1);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0]["attempt"], 1);
        assert_eq!(errors[0]["error"], event.job.errors[0].error.as_str());
        assert_eq!(metadata, serde_json::json!([1]));
        if state == "retryable" {
            assert!(
                scheduled_at > Utc::now() + chrono::Duration::minutes(50),
                "the client retry policy wasn't used: {scheduled_at}"
            );
        }
    }

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

    let mut run = client.start().unwrap();
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

    let mut run = client.start().unwrap();
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

    let mut run = client.start().unwrap();
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

    let mut run = client.start().unwrap();
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
    let mut run = client.start().unwrap();
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

#[tokio::test(flavor = "multi_thread")]
async fn stuck_job_keeps_its_worker_slot_until_it_ends() {
    let schema = TestSchema::new("stuck").await;
    let gate = Gate::default();
    let timeline = Arc::new(BlockingTimeline::default());
    let stuck = StuckSignal(Arc::new(Semaphore::new(0)));
    let client = Client::builder(schema.database())
        .id("postgres-resilience-stuck")
        .error_handler(stuck.clone())
        .job_stuck_threshold(Duration::from_millis(50))
        .job_timeout(Some(Duration::from_millis(100)))
        .without_notifications()
        .workers(blocking_workers(&gate, &timeline))
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(20)),
        )
        .build()
        .unwrap();
    let blocked = client
        .insert(BlockingArgs { block_ms: 1_500 })
        .await
        .unwrap();
    let later = client
        .insert(ResilienceArgs {})
        .opts(InsertOpts::default().with_priority(2))
        .await
        .unwrap();

    let mut run = client.start().unwrap();
    gate.wait_started().await;
    tokio::time::timeout(Duration::from_secs(5), stuck.0.acquire())
        .await
        .expect("stuck handler was not invoked")
        .unwrap()
        .forget();
    // The aborted task is still blocking its thread, so its row must stay
    // `running` rather than becoming retryable while the original runs.
    assert_eq!(schema.job_state(blocked.job.row.id).await, "running");
    wait_until(Duration::from_secs(10), "the later job", || async {
        schema.job_state(later.job.row.id).await == "completed"
    })
    .await;
    run.shutdown().await.unwrap();

    let blocking_finished = timeline.blocking_finished.lock().unwrap().unwrap();
    let later_started = timeline.later_started.lock().unwrap().unwrap();
    assert!(
        later_started >= blocking_finished,
        "the stuck job's worker slot was released while it still ran"
    );
    let blocked = client.job_get(blocked.job.row.id).await.unwrap();
    assert_eq!(blocked.attempt, 1);
    assert_eq!(blocked.errors.len(), 1);
    assert_eq!(
        blocked.errors[0].error,
        "job aborted after ignoring cancellation"
    );

    schema.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_leaves_a_job_still_stuck_after_abort_running() {
    let schema = TestSchema::new("abandon").await;
    let gate = Gate::default();
    let timeline = Arc::new(BlockingTimeline::default());
    let client = Client::builder(schema.database())
        .id("postgres-resilience-abandon")
        .job_stuck_threshold(Duration::from_millis(50))
        .without_notifications()
        .workers(blocking_workers(&gate, &timeline))
        .queue("default", fast_queue())
        .build()
        .unwrap();
    let blocked = client
        .insert(BlockingArgs { block_ms: 1_500 })
        .await
        .unwrap();

    let mut run = client.start().unwrap();
    gate.wait_started().await;
    let started = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(1), run.shutdown_now())
        .await
        .expect("shutdown waited for a task that cannot be aborted")
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(timeline.blocking_finished.lock().unwrap().is_none());
    assert_eq!(schema.job_state(blocked.job.row.id).await, "running");

    schema.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn out_of_range_snooze_is_clamped_and_cancel_time_matches_go() {
    let schema = TestSchema::new("wire").await;
    let client = gated_client(&schema, "postgres-resilience-wire", &Gate::default());
    let snoozed = client.insert(SnoozeForeverArgs {}).await.unwrap();

    let mut run = client.start().unwrap();
    wait_until(Duration::from_secs(10), "the snooze", || async {
        schema.job_state(snoozed.job.row.id).await == "scheduled"
    })
    .await;
    run.shutdown().await.unwrap();
    let snoozed = client.job_get(snoozed.job.row.id).await.unwrap();
    assert_eq!(snoozed.attempt, 0);
    assert!(snoozed.scheduled_at > chrono::Utc::now() + chrono::Duration::days(365 * 200));

    // River Go writes `cancel_attempted_at` as `time.Time` JSON.
    let cancelled = client.job_cancel(snoozed.id).await.unwrap();
    let cancel_attempted_at = cancelled.metadata["cancel_attempted_at"].as_str().unwrap();
    assert!(cancel_attempted_at.ends_with('Z'), "{cancel_attempted_at}");
    if let Some((_, fraction)) = cancel_attempted_at.trim_end_matches('Z').split_once('.') {
        assert!(
            !fraction.ends_with('0'),
            "trailing zeros are trimmed: {cancel_attempted_at}"
        );
    }
    chrono::DateTime::parse_from_rfc3339(cancel_attempted_at).unwrap();

    schema.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn client_survives_database_outage_and_catches_up() {
    let schema = TestSchema::new("outage").await;
    let proxy = FaultProxy::start().await;
    let gate = Gate::default();
    let client =
        Client::builder(PostgresDatabase::new(proxy.pool(4)).schema(schema.schema.clone()))
            .id("postgres-resilience-outage")
            .workers(gated_workers(&gate))
            // Notifications, not polling, must deliver work inserted during the
            // outage once the listener reconnects.
            .queue(
                "default",
                QueueConfig::new(4)
                    .with_fetch_cooldown(Duration::from_millis(1))
                    .with_fetch_poll_interval(Duration::from_secs(60)),
            )
            .build()
            .unwrap();
    // A second client inserts directly while the first is cut off.
    let inserter = Client::builder(schema.database()).build().unwrap();

    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();
    let before = inserter.insert(ResilienceArgs {}).await.unwrap();
    wait_until(
        Duration::from_secs(10),
        "work before the outage",
        || async { schema.job_state(before.job.row.id).await == "completed" },
    )
    .await;
    let in_flight = inserter.insert(GatedArgs {}).await.unwrap();
    gate.wait_started().await;

    proxy.take_down();
    // The in-flight job finishes while its completion cannot be written.
    gate.release();
    let during = inserter.insert(ResilienceArgs {}).await.unwrap();
    proxy.wait_for_rejections(3).await;
    proxy.restore();

    for (id, description) in [
        (in_flight.job.row.id, "in-flight job completion"),
        (during.job.row.id, "job inserted during the outage"),
    ] {
        wait_until(Duration::from_secs(30), description, || async {
            schema.job_state(id).await == "completed"
        })
        .await;
    }
    tokio::time::timeout(Duration::from_secs(10), run.shutdown())
        .await
        .unwrap()
        .unwrap();

    let in_flight = inserter.job_get(in_flight.job.row.id).await.unwrap();
    assert_eq!(in_flight.attempt, 1, "the in-flight job was not rescued");
    assert!(in_flight.errors.is_empty());

    drop(proxy);
    schema.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn client_started_during_an_outage_becomes_ready_after_recovery() {
    let schema = TestSchema::new("startdown").await;
    let proxy = FaultProxy::start().await;
    proxy.take_down();
    let client =
        Client::builder(PostgresDatabase::new(proxy.pool(2)).schema(schema.schema.clone()))
            .id("postgres-resilience-start-outage")
            .workers(completing_workers())
            .queue("default", fast_queue())
            .build()
            .unwrap();
    let job = Client::builder(schema.database())
        .build()
        .unwrap()
        .insert(ResilienceArgs {})
        .await
        .unwrap();

    let mut run = client.start().unwrap();
    // Outlast the producer's fast startup retries, after which it backs off
    // instead of stopping the client.
    tokio::time::sleep(Duration::from_secs(11)).await;
    proxy.restore();
    tokio::time::timeout(Duration::from_secs(30), run.wait_ready())
        .await
        .expect("listener did not recover")
        .unwrap();
    wait_until(Duration::from_secs(30), "work after recovery", || async {
        schema.job_state(job.job.row.id).await == "completed"
    })
    .await;
    run.shutdown().await.unwrap();

    drop(proxy);
    schema.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn listener_does_not_occupy_a_pool_connection() {
    let schema = TestSchema::new("listener").await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url())
        .await
        .unwrap();
    let client = Client::builder(PostgresDatabase::new(pool.clone()).schema(schema.schema.clone()))
        .id("postgres-resilience-listener-pool")
        .workers(completing_workers())
        .queue("default", fast_queue())
        .build()
        .unwrap();

    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();
    let job = client.insert(ResilienceArgs {}).await.unwrap();
    wait_until(
        Duration::from_secs(10),
        "work through a one-connection pool",
        || async { schema.job_state(job.job.row.id).await == "completed" },
    )
    .await;
    run.shutdown().await.unwrap();
    pool.close().await;

    schema.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_reconfiguration_waits_for_the_previous_producer() {
    let schema = TestSchema::new("reconfig").await;
    let gate = Gate::default();
    let client = gated_client(&schema, "postgres-resilience-reconfigure", &gate);
    let first = client.insert(GatedArgs {}).await.unwrap();

    let mut run = client.start().unwrap();
    gate.wait_started().await;
    client
        .queue_add(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .unwrap();
    let second = client.insert(GatedArgs {}).await.unwrap();
    // Hold the first job long enough that a replacement producer started
    // too early would take the second job alongside it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    gate.release();
    gate.wait_started().await;
    gate.release();
    for id in [first.job.row.id, second.job.row.id] {
        wait_until(Duration::from_secs(10), "both jobs", || async {
            schema.job_state(id).await == "completed"
        })
        .await;
    }
    run.shutdown().await.unwrap();
    assert_eq!(
        gate.max_active.load(Ordering::SeqCst),
        1,
        "reconfiguration exceeded max_workers"
    );

    schema.drop().await;
}

/// An extension that observes completions inside River's transaction,
/// failing its first call and deleting jobs marked for deletion.
#[derive(Clone, Default)]
struct SetStatePilot {
    calls: Arc<AtomicUsize>,
    seen: Arc<std::sync::Mutex<Vec<(i64, String)>>>,
}

#[async_trait]
impl Pilot for SetStatePilot {
    fn intercepts_job_set_state(&self) -> bool {
        true
    }

    async fn after_jobs_set_state(
        &self,
        connection: DatabaseConnection<'_>,
        params: &JobSetStateParams,
    ) -> Result<(), PilotError> {
        let connection = connection.into_postgres().unwrap();
        let schema = params.database.postgres_schema().unwrap();
        let ids = params.jobs.iter().map(|job| job.id).collect::<Vec<_>>();
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {} (job_id) SELECT unnest($1::bigint[])",
            schema.qualify("hook_effect")
        )))
        .bind(&ids)
        .execute(&mut *connection)
        .await?;
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(std::io::Error::other("first set-state hook call fails").into());
        }
        self.seen.lock().unwrap().extend(
            params
                .jobs
                .iter()
                .map(|job| (job.id, job.state.as_str().to_owned())),
        );
        let deleted = params
            .jobs
            .iter()
            .filter(|job| job.metadata.contains_key("delete_me"))
            .map(|job| job.id)
            .collect::<Vec<_>>();
        sqlx::query(AssertSqlSafe(format!(
            "DELETE FROM {} WHERE id = ANY($1)",
            schema.qualify("river_job")
        )))
        .bind(deleted)
        .execute(&mut *connection)
        .await?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn extension_set_state_hook_runs_in_the_completion_transaction() {
    let schema = TestSchema::new("hook").await;
    schema
        .execute(format!(
            "CREATE TABLE {}.hook_effect (job_id bigint NOT NULL)",
            schema.name
        ))
        .await;
    let pilot = SetStatePilot::default();
    let gate = Gate::default();
    let client = Client::builder(schema.database())
        .id("postgres-resilience-hook")
        .pilot(pilot.clone())
        .without_notifications()
        .workers(gated_workers(&gate))
        .queue("default", fast_queue())
        .build()
        .unwrap();
    let mut events = client.subscribe(&[EventKind::JobCompleted]).unwrap();
    let kept = client.insert(GatedArgs {}).await.unwrap();
    let deleted = client
        .insert(GatedArgs {})
        .opts(
            InsertOpts::default().with_metadata(serde_json::Map::from_iter([(
                "delete_me".to_owned(),
                serde_json::json!(true),
            )])),
        )
        .await
        .unwrap();

    let mut run = client.start().unwrap();
    gate.wait_started().await;
    gate.wait_started().await;
    // Release both together so they share one batch.
    gate.release();
    gate.release();
    let mut completed = Vec::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("completion events")
            .unwrap();
        completed.push(event.as_job().unwrap().job.id);
    }
    run.shutdown().await.unwrap();

    completed.sort_unstable();
    let mut expected = vec![kept.job.row.id, deleted.job.row.id];
    expected.sort_unstable();
    assert_eq!(
        completed, expected,
        "events come from rows River already holds"
    );
    assert_eq!(schema.job_state(kept.job.row.id).await, "completed");
    assert!(matches!(
        client.job_get(deleted.job.row.id).await,
        Err(riverqueue::Error::NotFound)
    ));
    let mut seen = pilot.seen.lock().unwrap().clone();
    seen.sort_unstable();
    assert_eq!(
        seen,
        expected
            .iter()
            .map(|id| (*id, "completed".to_owned()))
            .collect::<Vec<_>>()
    );
    // The failed first call's writes rolled back with its batch.
    let effects: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {}.hook_effect",
        schema.name
    )))
    .fetch_one(&schema.pool)
    .await
    .unwrap();
    assert_eq!(effects, 2);

    schema.drop().await;
}
