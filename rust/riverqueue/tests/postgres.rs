#![cfg(feature = "postgres-tests")]

use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use riverqueue::internal::{
    CompletionAction, CompletionParams, DatabaseConfig, DatabaseConnection, DatabasePool,
    FetchParams, MaintenanceService, Pilot, PilotError, RuntimeService, SchemaName,
};
use riverqueue::{
    BoxError, Client, EventKind, ExtensionClaimParams, InsertBatch, InsertOpts, IntervalSchedule,
    Job, JobArgs, JobListOrderBy, JobListParams, JobRow, JobState, JobUpdateParams,
    MaintenanceConfig, PeriodicJob, PeriodicJobOpts, QueueConfig, QueueListParams, RetryPolicy,
    UniqueOpts, WorkContext, WorkError, WorkOutcome, Worker, WorkerRegistry, WorkerTimeout,
    database::{PostgresDatabase, PostgresReindexConfig, PostgresReindexSchedule},
};
use riverqueue_migrate::{Direction, MigrateOpts};
use riverqueue_migrate::{MIGRATION_VERSION_LATEST, PostgresMigrator};
use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, PgPool};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_conformance_echo")]
struct EchoArgs {
    message: String,
}

struct EchoWorker;

impl Worker<EchoArgs> for EchoWorker {
    type Error = Infallible;

    async fn work(
        &self,
        context: WorkContext,
        job: Job<EchoArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        assert!(!job.args.message.is_empty());
        context
            .record_output(&serde_json::json!({"message": job.args.message}))
            .await
            .unwrap();
        Ok(WorkOutcome::Complete)
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_conformance_cancel")]
struct CancelArgs {}

struct CancelWorker;

impl Worker<CancelArgs> for CancelWorker {
    type Error = Infallible;

    async fn work(
        &self,
        context: WorkContext,
        _job: Job<CancelArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        context.cancellation_token().cancelled().await;
        Ok(WorkOutcome::Cancel)
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_conformance_fail")]
struct FailArgs {}

struct FailWorker;

impl Worker<FailArgs> for FailWorker {
    type Error = std::io::Error;

    async fn work(
        &self,
        _context: WorkContext,
        _job: Job<FailArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        Err(std::io::Error::other("intentional failure"))
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_conformance_ignores_cancel")]
struct IgnoresCancelArgs {}

struct IgnoresCancelWorker;

impl Worker<IgnoresCancelArgs> for IgnoresCancelWorker {
    type Error = Infallible;

    async fn work(
        &self,
        _context: WorkContext,
        _job: Job<IgnoresCancelArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        std::future::pending().await
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_rescue_default_timeout")]
struct RescueDefaultTimeoutArgs {}

struct RescueDefaultTimeoutWorker;

impl Worker<RescueDefaultTimeoutArgs> for RescueDefaultTimeoutWorker {
    type Error = Infallible;

    async fn work(
        &self,
        _context: WorkContext,
        _job: Job<RescueDefaultTimeoutArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        Ok(WorkOutcome::Complete)
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_rescue_disabled_timeout")]
struct RescueDisabledTimeoutArgs {}

struct RescueDisabledTimeoutWorker;

impl Worker<RescueDisabledTimeoutArgs> for RescueDisabledTimeoutWorker {
    type Error = Infallible;

    fn timeout(&self, _job: &Job<RescueDisabledTimeoutArgs>) -> WorkerTimeout {
        WorkerTimeout::Disabled
    }

    async fn work(
        &self,
        _context: WorkContext,
        _job: Job<RescueDisabledTimeoutArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        Ok(WorkOutcome::Complete)
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_rescue_long_timeout")]
struct RescueLongTimeoutArgs {}

struct RescueLongTimeoutWorker;

impl Worker<RescueLongTimeoutArgs> for RescueLongTimeoutWorker {
    type Error = Infallible;

    fn timeout(&self, _job: &Job<RescueLongTimeoutArgs>) -> WorkerTimeout {
        WorkerTimeout::After(Duration::from_hours(1))
    }

    async fn work(
        &self,
        _context: WorkContext,
        _job: Job<RescueLongTimeoutArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        Ok(WorkOutcome::Complete)
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_rescue_retry_override")]
struct RescueRetryOverrideArgs {}

struct RescueRetryOverrideWorker;

impl Worker<RescueRetryOverrideArgs> for RescueRetryOverrideWorker {
    type Error = Infallible;

    fn next_retry(
        &self,
        _job: &Job<RescueRetryOverrideArgs>,
        _error: &WorkError,
        _now: chrono::DateTime<chrono::Utc>,
    ) -> Option<Duration> {
        Some(Duration::from_hours(2))
    }

    async fn work(
        &self,
        _context: WorkContext,
        _job: Job<RescueRetryOverrideArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        Ok(WorkOutcome::Complete)
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_resumable_checkpoint")]
struct ResumableCheckpointArgs {
    mode: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct ResumableCursor {
    offset: i64,
}

struct ResumableCheckpointWorker {
    cursor_values: Arc<Mutex<Vec<ResumableCursor>>>,
    pool: PgPool,
    validate_runs: Arc<AtomicUsize>,
}

impl Worker<ResumableCheckpointArgs> for ResumableCheckpointWorker {
    type Error = riverqueue::Error;

    fn next_retry(
        &self,
        job: &Job<ResumableCheckpointArgs>,
        _error: &WorkError,
        _now: chrono::DateTime<chrono::Utc>,
    ) -> Option<Duration> {
        (job.args.mode == "cursor_retry").then_some(Duration::from_millis(500))
    }

    async fn work(
        &self,
        context: WorkContext,
        job: Job<ResumableCheckpointArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        match job.args.mode.as_str() {
            "cursor_retry" => {
                context
                    .resumable_step("validate", || async {
                        self.validate_runs.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, riverqueue::Error>(())
                    })
                    .await?;
                let cursor_context = context.clone();
                let cursor_values = Arc::clone(&self.cursor_values);
                let attempt = job.row.attempt;
                context
                    .resumable_step_with_cursor(
                        "process",
                        move |cursor: ResumableCursor| async move {
                            cursor_values.lock().unwrap().push(cursor.clone());
                            if attempt == 1 {
                                cursor_context
                                    .resumable_set_cursor(&ResumableCursor { offset: 42 })
                                    .await?;
                                return Err(riverqueue::Error::runtime(
                                    "intentional resumable cursor failure".to_owned(),
                                ));
                            }
                            Ok(())
                        },
                    )
                    .await?;
            }
            "commit_cursor" | "rollback_cursor" => {
                let checkpoint_context = context.clone();
                let mode = job.args.mode.clone();
                let pool = self.pool.clone();
                context
                    .resumable_step_with_cursor("tx_cursor", move |_: ResumableCursor| async move {
                        let mut transaction = pool.begin().await?;
                        checkpoint_context
                            .resumable_set_step_cursor_tx(
                                &mut transaction,
                                &ResumableCursor { offset: 7 },
                            )
                            .await?;
                        if mode == "commit_cursor" {
                            transaction.commit().await?;
                        } else {
                            transaction.rollback().await?;
                        }
                        Ok::<_, riverqueue::Error>(())
                    })
                    .await?;
            }
            "commit_step" | "rollback_step" => {
                let checkpoint_context = context.clone();
                let mode = job.args.mode.clone();
                let pool = self.pool.clone();
                context
                    .resumable_step("tx_step", move || async move {
                        let mut transaction = pool.begin().await?;
                        checkpoint_context
                            .resumable_set_step_tx(&mut transaction)
                            .await?;
                        if mode == "commit_step" {
                            transaction.commit().await?;
                        } else {
                            transaction.rollback().await?;
                        }
                        Ok::<_, riverqueue::Error>(())
                    })
                    .await?;
            }
            mode => {
                return Err(riverqueue::Error::runtime(format!(
                    "unknown test mode {mode}"
                )));
            }
        }
        Ok(WorkOutcome::Complete)
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_conformance_resumable")]
struct ResumableArgs {}

#[derive(Clone, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_transactional")]
struct TransactionalArgs {}

struct ResumableWorker {
    first_runs: Arc<AtomicUsize>,
    second_runs: Arc<AtomicUsize>,
}

struct TransactionalWorker {
    pool: PgPool,
}

#[derive(Clone)]
struct TestPilot {
    completions: Arc<AtomicUsize>,
    fetches: Arc<AtomicUsize>,
    maintenance_starts: Arc<AtomicUsize>,
    maintenance_stops: Arc<AtomicUsize>,
    runtime_starts: Arc<AtomicUsize>,
    runtime_stops: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct ContinueCompletionPilot {
    completions: Arc<AtomicUsize>,
}

#[async_trait]
impl Pilot for ContinueCompletionPilot {
    fn intercepts_completion(&self) -> bool {
        true
    }

    async fn before_job_completion(
        &self,
        _connection: DatabaseConnection<'_>,
        _params: &CompletionParams,
    ) -> Result<CompletionAction, PilotError> {
        self.completions.fetch_add(1, Ordering::SeqCst);
        Ok(CompletionAction::Continue)
    }
}

struct LongRetryPolicy;

impl RetryPolicy for LongRetryPolicy {
    fn next_retry(
        &self,
        _job: &JobRow,
        _error: &str,
        _now: chrono::DateTime<chrono::Utc>,
    ) -> Duration {
        Duration::from_hours(1)
    }
}

#[async_trait]
impl Pilot for TestPilot {
    fn intercepts_completion(&self) -> bool {
        true
    }

    fn intercepts_fetch(&self) -> bool {
        true
    }

    async fn select_job_ids(
        &self,
        connection: DatabaseConnection<'_>,
        params: &FetchParams,
    ) -> Result<Option<Vec<i64>>, PilotError> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let schema = params.database.postgres_schema().unwrap();
        let table = schema.qualify("river_job");
        let queue_table = schema.qualify("river_queue");
        let sql = format!(
            "SELECT id FROM {table} WHERE state = 'available' AND queue = $1 \
             AND scheduled_at <= now() AND kind = ANY($2::text[]) \
             AND NOT EXISTS (SELECT 1 FROM {queue_table} WHERE name = $1 AND paused_at IS NOT NULL) \
             ORDER BY priority, scheduled_at, id LIMIT $3 FOR UPDATE SKIP LOCKED"
        );
        Ok(Some(
            sqlx::query_scalar(AssertSqlSafe(sql))
                .bind(&params.queue)
                .bind(&params.kinds)
                .bind(params.maximum)
                .fetch_all(connection.into_postgres().unwrap())
                .await?,
        ))
    }

    async fn before_job_completion(
        &self,
        connection: DatabaseConnection<'_>,
        params: &CompletionParams,
    ) -> Result<CompletionAction, PilotError> {
        self.completions.fetch_add(1, Ordering::SeqCst);
        let table = params
            .database
            .postgres_schema()
            .unwrap()
            .qualify("river_job");
        let sql = format!(
            "UPDATE {table} SET state = 'completed', finalized_at = now(), \
             metadata = metadata || $2::jsonb || '{{\"extension_handled\": true}}'::jsonb \
             WHERE id = $1"
        );
        sqlx::query(AssertSqlSafe(sql))
            .bind(params.job_id)
            .bind(sqlx::types::Json(&params.metadata_updates))
            .execute(connection.into_postgres().unwrap())
            .await?;
        Ok(CompletionAction::Handled)
    }

    fn maintenance_services(&self) -> Vec<Arc<dyn MaintenanceService>> {
        vec![Arc::new(TestMaintenance {
            starts: Arc::clone(&self.maintenance_starts),
            stops: Arc::clone(&self.maintenance_stops),
        })]
    }

    fn runtime_services(&self) -> Vec<Arc<dyn RuntimeService>> {
        vec![Arc::new(TestRuntime {
            starts: Arc::clone(&self.runtime_starts),
            stops: Arc::clone(&self.runtime_stops),
        })]
    }
}

struct TestMaintenance {
    starts: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
}

#[async_trait]
impl MaintenanceService for TestMaintenance {
    async fn run(
        &self,
        _pool: DatabasePool,
        _database: DatabaseConfig,
        cancellation: CancellationToken,
    ) -> Result<(), PilotError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        cancellation.cancelled().await;
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct TestRuntime {
    starts: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
}

#[async_trait]
impl RuntimeService for TestRuntime {
    async fn run(
        &self,
        _pool: DatabasePool,
        _database: DatabaseConfig,
        cancellation: CancellationToken,
    ) -> Result<(), PilotError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        cancellation.cancelled().await;
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl Worker<ResumableArgs> for ResumableWorker {
    type Error = riverqueue::Error;

    async fn work(
        &self,
        context: WorkContext,
        job: Job<ResumableArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        context
            .resumable_step("first", || async {
                self.first_runs.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::io::Error>(())
            })
            .await?;
        context
            .resumable_step("second", || async {
                self.second_runs.fetch_add(1, Ordering::SeqCst);
                if job.row.attempt == 1 {
                    Err(std::io::Error::other("fail second step once"))
                } else {
                    Ok(())
                }
            })
            .await?;
        Ok(WorkOutcome::Complete)
    }
}

impl Worker<TransactionalArgs> for TransactionalWorker {
    type Error = riverqueue::Error;

    async fn work(
        &self,
        context: WorkContext,
        _job: Job<TransactionalArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        assert_eq!(context.client().unwrap().id(), "rust-maintenance-client");
        context
            .metadata_set("transactional_completion", serde_json::json!(true))
            .await;
        let mut transaction = self.pool.begin().await?;
        let completed = context.job_complete_tx(&mut transaction).await?;
        assert_eq!(completed.state, JobState::Completed);
        assert_eq!(completed.metadata["transactional_completion"], true);
        assert_eq!(completed.metadata["extension_handled"], true);
        transaction.commit().await?;
        Ok(WorkOutcome::Complete)
    }
}

#[tokio::test]
async fn cancellation_wins_over_rescheduling_completion_updates() {
    let database_url = std::env::var("RIVER_RUST_DATABASE_URL")
        .expect("RIVER_RUST_DATABASE_URL must point at a disposable test database");
    let pool = PgPool::connect(&database_url).await.unwrap();
    let schema = SchemaName::new("rust_cancellation_winner_test").unwrap();
    sqlx::raw_sql(
        "DROP SCHEMA IF EXISTS rust_cancellation_winner_test CASCADE; \
         CREATE SCHEMA rust_cancellation_winner_test",
    )
    .execute(&pool)
    .await
    .unwrap();
    PostgresMigrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await
        .unwrap();

    for direct_completion in [false, true] {
        assert_cancellation_wins(&pool, &schema, direct_completion).await;
    }

    sqlx::raw_sql("DROP SCHEMA rust_cancellation_winner_test CASCADE")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn concurrent_unique_inserts_return_the_conflicting_job() {
    const INSERT_COUNT: usize = 32;

    let Ok(database_url) = std::env::var("RIVER_RUST_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL runtime test without RIVER_RUST_DATABASE_URL");
        return;
    };
    let pool = PgPool::connect(&database_url).await.unwrap();
    let schema = SchemaName::new("rust_unique_concurrency_test").unwrap();
    sqlx::raw_sql(
        "DROP SCHEMA IF EXISTS rust_unique_concurrency_test CASCADE; \
         CREATE SCHEMA rust_unique_concurrency_test",
    )
    .execute(&pool)
    .await
    .unwrap();
    PostgresMigrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await
        .unwrap();
    let client = Client::builder(PostgresDatabase::new(pool.clone()).schema(schema))
        .build()
        .unwrap();

    let all_states = vec![
        JobState::Available,
        JobState::Cancelled,
        JobState::Completed,
        JobState::Discarded,
        JobState::Pending,
        JobState::Retryable,
        JobState::Running,
        JobState::Scheduled,
    ];
    let fixed_scheduled_at = chrono::Utc::now() - chrono::Duration::minutes(1);
    let cases = [
        (
            "by_args",
            InsertOpts::default().with_unique(UniqueOpts::new().by_args()),
        ),
        (
            "by_args_and_queue",
            InsertOpts::default()
                .with_queue("unique_queue")
                .with_unique(UniqueOpts::new().by_args().by_queue()),
        ),
        (
            "by_args_and_states",
            InsertOpts::default().with_unique(UniqueOpts::new().by_args().by_states(all_states)),
        ),
        (
            "by_args_and_period",
            InsertOpts::default()
                .with_scheduled_at(fixed_scheduled_at)
                .with_unique(
                    UniqueOpts::new()
                        .by_args()
                        .by_period(Duration::from_mins(1)),
                ),
        ),
    ];

    for (message, opts) in cases {
        let barrier = Arc::new(tokio::sync::Barrier::new(INSERT_COUNT));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..INSERT_COUNT {
            let barrier = Arc::clone(&barrier);
            let client = client.clone();
            let message = message.to_owned();
            let opts = opts.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                client.insert_with(EchoArgs { message }, opts).await
            });
        }

        let mut results = Vec::with_capacity(INSERT_COUNT);
        while let Some(result) = tasks.join_next().await {
            results.push(result.unwrap().unwrap());
        }
        let job_id = results[0].job.row.id;
        assert!(results.iter().all(|result| result.job.row.id == job_id));
        assert_eq!(
            results
                .iter()
                .filter(|result| !result.unique_skipped_as_duplicate)
                .count(),
            1,
            "unique case {message} should insert exactly one job"
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| result.unique_skipped_as_duplicate)
                .count(),
            INSERT_COUNT - 1,
            "unique case {message} should return the winner to every conflicting insert"
        );
    }

    sqlx::raw_sql("DROP SCHEMA rust_unique_concurrency_test CASCADE")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one backend regression verifies atomic selection, ordering, row updates, and decode rollback"
)]
async fn extension_claim_returns_ordered_rows_and_rolls_back_decode_errors() {
    let Ok(database_url) = std::env::var("RIVER_RUST_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL claim test without RIVER_RUST_DATABASE_URL");
        return;
    };
    let pool = PgPool::connect(&database_url).await.unwrap();
    let schema = SchemaName::new("rust_extension_claim_test").unwrap();
    sqlx::raw_sql(
        "DROP SCHEMA IF EXISTS rust_extension_claim_test CASCADE; \
         CREATE SCHEMA rust_extension_claim_test",
    )
    .execute(&pool)
    .await
    .unwrap();
    PostgresMigrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await
        .unwrap();
    let client = Client::builder(PostgresDatabase::new(pool.clone()).schema(schema.clone()))
        .id("postgres-extension-claimer")
        .build()
        .unwrap();
    let table = schema.qualify("river_job");
    let now = chrono::Utc::now();
    let matches = serde_json::Map::from_iter([
        ("group".to_owned(), serde_json::json!("shared")),
        ("mode".to_owned(), serde_json::json!("open")),
    ]);
    let leader = client
        .insert_with(
            EchoArgs {
                message: "leader".to_owned(),
            },
            InsertOpts::default().with_metadata(matches.clone()),
        )
        .await
        .unwrap();
    let mut expected = Vec::new();
    for (message, priority, scheduled_at) in [
        ("third", 2, now - chrono::Duration::minutes(3)),
        ("second", 1, now - chrono::Duration::minutes(1)),
        ("first", 1, now - chrono::Duration::minutes(2)),
    ] {
        let inserted = client
            .insert_with(
                EchoArgs {
                    message: message.to_owned(),
                },
                InsertOpts::default()
                    .with_metadata(matches.clone())
                    .with_priority(priority),
            )
            .await
            .unwrap();
        let update_sql = format!("UPDATE {table} SET scheduled_at = $1 WHERE id = $2");
        sqlx::query(AssertSqlSafe(update_sql))
            .bind(scheduled_at)
            .bind(inserted.job.row.id)
            .execute(&pool)
            .await
            .unwrap();
        expected.push(inserted);
    }
    client
        .insert_with(
            EchoArgs {
                message: "wrong metadata".to_owned(),
            },
            InsertOpts::default().with_metadata(serde_json::Map::from_iter([
                ("group".to_owned(), serde_json::json!("shared")),
                ("mode".to_owned(), serde_json::json!("closed")),
            ])),
        )
        .await
        .unwrap();

    let rows = client
        .extension_claim_jobs(ExtensionClaimParams {
            excluded_job_id: leader.job.row.id,
            kind: EchoArgs::KIND.to_owned(),
            maximum: 3,
            metadata_matches: matches,
            metadata_updates: serde_json::Map::from_iter([(
                "claim".to_owned(),
                serde_json::json!("leader"),
            )]),
            queue: "default".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        [
            expected[2].job.row.id,
            expected[1].job.row.id,
            expected[0].job.row.id,
        ]
    );
    for row in &rows {
        assert_eq!(row.state, JobState::Running);
        assert_eq!(row.attempt, 1);
        assert_eq!(row.attempted_by, ["postgres-extension-claimer"]);
        assert_eq!(row.metadata["claim"], "leader");
    }

    let invalid = client.insert(FailArgs {}).await.unwrap();
    let corrupt_sql = format!("UPDATE {table} SET errors = ARRAY['{{}}'::jsonb] WHERE id = $1");
    sqlx::query(AssertSqlSafe(corrupt_sql))
        .bind(invalid.job.row.id)
        .execute(&pool)
        .await
        .unwrap();
    let error = client
        .extension_claim_jobs(ExtensionClaimParams {
            excluded_job_id: 0,
            kind: FailArgs::KIND.to_owned(),
            maximum: 1,
            metadata_matches: serde_json::Map::new(),
            metadata_updates: serde_json::Map::new(),
            queue: "default".to_owned(),
        })
        .await
        .unwrap_err();
    assert!(matches!(error, riverqueue::Error::Database(_)));
    let state_sql = format!("SELECT state::text, attempt FROM {table} WHERE id = $1");
    let (state, attempt): (String, i16) = sqlx::query_as(AssertSqlSafe(state_sql))
        .bind(invalid.job.row.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "available");
    assert_eq!(attempt, 0);

    sqlx::raw_sql("DROP SCHEMA rust_extension_claim_test CASCADE")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn insert_many_variants_preserve_order_and_transactionality() {
    let database_url = std::env::var("RIVER_RUST_DATABASE_URL")
        .expect("RIVER_RUST_DATABASE_URL must point at a disposable test database");
    let pool = PgPool::connect(&database_url).await.unwrap();
    let schema = SchemaName::new("rust_insert_many_test").unwrap();
    sqlx::raw_sql(
        "DROP SCHEMA IF EXISTS rust_insert_many_test CASCADE; \
         CREATE SCHEMA rust_insert_many_test",
    )
    .execute(&pool)
    .await
    .unwrap();
    PostgresMigrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await
        .unwrap();
    let client = Client::builder(PostgresDatabase::new(pool.clone()).schema(schema.clone()))
        .build()
        .unwrap();
    let table = schema.qualify("river_job");

    let empty_many = client
        .insert_many(Vec::<EchoArgs>::new())
        .await
        .unwrap_err();
    assert_eq!(
        empty_many.to_string(),
        "invalid job: job: no jobs to insert"
    );
    let empty_batch = client.insert_batch(InsertBatch::new()).await.unwrap_err();
    assert_eq!(
        empty_batch.to_string(),
        "invalid job: job: no jobs to insert"
    );
    let mut empty_transaction = pool.begin().await.unwrap();
    let empty_many_tx = client
        .insert_many_tx(&mut empty_transaction, Vec::<EchoArgs>::new())
        .await
        .unwrap_err();
    assert_eq!(
        empty_many_tx.to_string(),
        "invalid job: job: no jobs to insert"
    );
    let empty_batch_tx = client
        .insert_batch_tx(&mut empty_transaction, InsertBatch::new())
        .await
        .unwrap_err();
    assert_eq!(
        empty_batch_tx.to_string(),
        "invalid job: job: no jobs to insert"
    );
    empty_transaction.commit().await.unwrap();

    let past_scheduled_at = chrono::Utc::now() - chrono::Duration::minutes(1);
    let ordered = client
        .insert_many_with([
            (
                EchoArgs {
                    message: "ordered-one".to_owned(),
                },
                InsertOpts::default(),
            ),
            (
                EchoArgs {
                    message: "ordered-two".to_owned(),
                },
                InsertOpts::default(),
            ),
            (
                EchoArgs {
                    message: "ordered-past-scheduled".to_owned(),
                },
                InsertOpts::default().with_scheduled_at(past_scheduled_at),
            ),
        ])
        .await
        .unwrap();
    assert_eq!(
        ordered
            .iter()
            .map(|result| result.job.args.message.as_str())
            .collect::<Vec<_>>(),
        ["ordered-one", "ordered-two", "ordered-past-scheduled"]
    );
    assert!(
        ordered
            .windows(2)
            .all(|pair| pair[0].job.row.id < pair[1].job.row.id)
    );
    assert_eq!(ordered[2].job.row.state, JobState::Scheduled);

    let defaults = client
        .insert_many([
            EchoArgs {
                message: "default-one".to_owned(),
            },
            EchoArgs {
                message: "default-two".to_owned(),
            },
        ])
        .await
        .unwrap();
    assert_eq!(defaults.len(), 2);

    let mut heterogeneous = InsertBatch::new();
    heterogeneous
        .push(EchoArgs {
            message: "heterogeneous".to_owned(),
        })
        .unwrap()
        .push_with(
            CancelArgs {},
            InsertOpts::default().with_queue("heterogeneous-queue"),
        )
        .unwrap();
    let heterogeneous = client.insert_batch(heterogeneous).await.unwrap();
    assert_eq!(heterogeneous.len(), 2);
    assert_eq!(heterogeneous[0].job.kind, EchoArgs::KIND);
    assert_eq!(heterogeneous[1].job.kind, CancelArgs::KIND);
    assert_eq!(heterogeneous[1].job.queue, "heterogeneous-queue");

    let time_without_states = client
        .job_list(
            &JobListParams::default()
                .with_ids(ordered.iter().map(|result| result.job.row.id))
                .with_order_by(JobListOrderBy::Time),
        )
        .await
        .unwrap();
    assert_eq!(
        time_without_states
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        ordered
            .iter()
            .map(|result| result.job.row.id)
            .collect::<Vec<_>>()
    );
    let finalized_without_states = client
        .job_list(&JobListParams::default().with_order_by(JobListOrderBy::FinalizedAt))
        .await;
    assert!(matches!(
        finalized_without_states,
        Err(riverqueue::Error::InvalidJob(_))
    ));

    let unique_opts = InsertOpts::default().with_unique(UniqueOpts::new().by_args());
    let unique = client
        .insert_many_with([
            (
                EchoArgs {
                    message: "unique-batch".to_owned(),
                },
                unique_opts.clone(),
            ),
            (
                EchoArgs {
                    message: "unique-batch".to_owned(),
                },
                unique_opts.clone(),
            ),
        ])
        .await
        .unwrap();
    assert_eq!(unique[0].job.row.id, unique[1].job.row.id);
    assert!(!unique[0].unique_skipped_as_duplicate);
    assert!(unique[1].unique_skipped_as_duplicate);

    let mut transaction = pool.begin().await.unwrap();
    let rolled_back = client
        .insert_many_tx_with(
            &mut transaction,
            ["tx-rollback-one", "tx-rollback-two"].map(|message| {
                (
                    EchoArgs {
                        message: message.to_owned(),
                    },
                    InsertOpts::default().with_tags(["tx-rollback"]),
                )
            }),
        )
        .await
        .unwrap();
    assert_eq!(rolled_back[0].job.args.message, "tx-rollback-one");
    assert_eq!(rolled_back[1].job.args.message, "tx-rollback-two");
    transaction.rollback().await.unwrap();
    let rolled_back_count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {table} WHERE 'tx-rollback' = ANY(tags)"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rolled_back_count, 0);

    let mut transaction = pool.begin().await.unwrap();
    client
        .insert_many_tx_with(
            &mut transaction,
            ["tx-commit-one", "tx-commit-two"].map(|message| {
                (
                    EchoArgs {
                        message: message.to_owned(),
                    },
                    InsertOpts::default().with_tags(["tx-commit"]),
                )
            }),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let committed_messages: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT args ->> 'message' FROM {table} WHERE 'tx-commit' = ANY(tags) ORDER BY id"
    )))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(committed_messages, ["tx-commit-one", "tx-commit-two"]);

    let invalid_batch = client
        .insert_many_with([
            (
                EchoArgs {
                    message: "atomic-valid".to_owned(),
                },
                InsertOpts::default().with_tags(["atomic-ordinary"]),
            ),
            (
                EchoArgs {
                    message: "atomic-invalid".to_owned(),
                },
                InsertOpts::default().with_priority(0),
            ),
        ])
        .await;
    assert!(invalid_batch.is_err());
    let atomic_ordinary_count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {table} WHERE 'atomic-ordinary' = ANY(tags)"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(atomic_ordinary_count, 0);

    let mut transaction = pool.begin().await.unwrap();
    client
        .insert_tx(
            &mut transaction,
            EchoArgs {
                message: "ordinary-savepoint-control".to_owned(),
            },
        )
        .await
        .unwrap();
    let ordinary_savepoint = client
        .insert_many_tx_with(
            &mut transaction,
            [
                (
                    EchoArgs {
                        message: "ordinary-savepoint-prefix".to_owned(),
                    },
                    InsertOpts::default().with_tags(["ordinary-savepoint-batch"]),
                ),
                (
                    EchoArgs {
                        message: "ordinary-savepoint-invalid".to_owned(),
                    },
                    InsertOpts::default().with_priority(0),
                ),
            ],
        )
        .await;
    assert!(ordinary_savepoint.is_err());
    transaction.commit().await.unwrap();
    let ordinary_control_count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {table} WHERE args ->> 'message' = 'ordinary-savepoint-control'"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    let ordinary_batch_count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {table} WHERE 'ordinary-savepoint-batch' = ANY(tags)"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(ordinary_control_count, 1);
    assert_eq!(ordinary_batch_count, 0);

    let mut transaction = pool.begin().await.unwrap();
    assert_eq!(
        client
            .insert_many_fast_tx_with(
                &mut transaction,
                ["fast-rollback-one", "fast-rollback-two"].map(|message| {
                    (
                        EchoArgs {
                            message: message.to_owned(),
                        },
                        InsertOpts::default().with_tags(["fast-rollback"]),
                    )
                }),
            )
            .await
            .unwrap(),
        2
    );
    transaction.rollback().await.unwrap();
    let fast_rollback_count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {table} WHERE 'fast-rollback' = ANY(tags)"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(fast_rollback_count, 0);

    let mut transaction = pool.begin().await.unwrap();
    assert_eq!(
        client
            .insert_many_fast_tx_with(
                &mut transaction,
                ["fast-commit-one", "fast-commit-two"].map(|message| {
                    (
                        EchoArgs {
                            message: message.to_owned(),
                        },
                        InsertOpts::default().with_tags(["fast-commit"]),
                    )
                }),
            )
            .await
            .unwrap(),
        2
    );
    transaction.commit().await.unwrap();
    let fast_committed_messages: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT args ->> 'message' FROM {table} WHERE 'fast-commit' = ANY(tags) ORDER BY id"
    )))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        fast_committed_messages,
        ["fast-commit-one", "fast-commit-two"]
    );

    client
        .insert_with(
            EchoArgs {
                message: "fast-unique-conflict".to_owned(),
            },
            unique_opts,
        )
        .await
        .unwrap();
    let mut transaction = pool.begin().await.unwrap();
    client
        .insert_tx(
            &mut transaction,
            EchoArgs {
                message: "fast-savepoint-control".to_owned(),
            },
        )
        .await
        .unwrap();
    let fast_atomic = client
        .insert_many_fast_tx_with(
            &mut transaction,
            [
                (
                    EchoArgs {
                        message: "fast-atomic-valid".to_owned(),
                    },
                    InsertOpts::default().with_tags(["fast-atomic"]),
                ),
                (
                    EchoArgs {
                        message: "fast-unique-conflict".to_owned(),
                    },
                    InsertOpts::default().with_unique(UniqueOpts::new().by_args()),
                ),
            ],
        )
        .await;
    assert!(fast_atomic.is_err());
    transaction.commit().await.unwrap();
    let fast_control_count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {table} WHERE args ->> 'message' = 'fast-savepoint-control'"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    let fast_atomic_count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {table} WHERE 'fast-atomic' = ANY(tags)"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(fast_control_count, 1);
    assert_eq!(fast_atomic_count, 0);

    sqlx::raw_sql("DROP SCHEMA rust_insert_many_test CASCADE")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn migrates_inserts_and_works_a_job() {
    let database_url = std::env::var("RIVER_RUST_DATABASE_URL")
        .expect("RIVER_RUST_DATABASE_URL must point at a disposable test database");
    let pool = PgPool::connect(&database_url).await.unwrap();
    let migrator = PostgresMigrator::new(pool.clone());
    migrator.migrate_up().await.unwrap();
    assert_eq!(
        migrator.existing_versions().await.unwrap(),
        (1..=MIGRATION_VERSION_LATEST).collect::<Vec<_>>()
    );

    sqlx::raw_sql(
        "DROP SCHEMA IF EXISTS rust_migration_test CASCADE; CREATE SCHEMA rust_migration_test",
    )
    .execute(&pool)
    .await
    .unwrap();
    let custom_migrator = PostgresMigrator::new(pool.clone())
        .with_schema(riverqueue::database::SchemaName::new("rust_migration_test").unwrap());
    let first_up = custom_migrator
        .migrate(Direction::Up, MigrateOpts::new().with_target_version(4))
        .await
        .unwrap();
    assert_eq!(
        first_up
            .versions
            .iter()
            .map(|version| version.version)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert!(!custom_migrator.validate(None).await.unwrap().ok);
    custom_migrator.migrate_up().await.unwrap();
    assert!(custom_migrator.validate(None).await.unwrap().ok);
    custom_migrator
        .migrate(Direction::Down, MigrateOpts::new().with_target_version(3))
        .await
        .unwrap();
    assert_eq!(
        custom_migrator.existing_versions().await.unwrap(),
        vec![1, 2, 3]
    );
    custom_migrator.migrate_up().await.unwrap();
    let dry_run = custom_migrator
        .migrate(
            Direction::Down,
            MigrateOpts::new().with_dry_run(true).with_max_steps(2),
        )
        .await
        .unwrap();
    assert_eq!(dry_run.versions.len(), 2);
    assert_eq!(
        custom_migrator.existing_versions().await.unwrap(),
        (1..=MIGRATION_VERSION_LATEST).collect::<Vec<_>>()
    );
    custom_migrator
        .migrate(Direction::Down, MigrateOpts::new().with_target_version(-1))
        .await
        .unwrap();
    assert!(
        custom_migrator
            .existing_versions()
            .await
            .unwrap()
            .is_empty()
    );
    sqlx::raw_sql("DROP SCHEMA rust_migration_test CASCADE")
        .execute(&pool)
        .await
        .unwrap();

    sqlx::raw_sql("TRUNCATE river_job, river_notification, river_queue RESTART IDENTITY CASCADE")
        .execute(&pool)
        .await
        .unwrap();

    let mut workers = WorkerRegistry::new();
    workers.register::<CancelArgs, _>(CancelWorker).unwrap();
    workers.register::<EchoArgs, _>(EchoWorker).unwrap();
    workers.register::<FailArgs, _>(FailWorker).unwrap();
    let resumable_first_runs = Arc::new(AtomicUsize::new(0));
    let resumable_second_runs = Arc::new(AtomicUsize::new(0));
    workers
        .register::<ResumableArgs, _>(ResumableWorker {
            first_runs: Arc::clone(&resumable_first_runs),
            second_runs: Arc::clone(&resumable_second_runs),
        })
        .unwrap();
    let client = Client::builder(pool.clone())
        .id("rust-conformance-client")
        .workers(workers)
        .queue("default", QueueConfig::new(2))
        .build()
        .unwrap();

    let fast_count = client
        .insert_many_fast_with([
            (
                EchoArgs {
                    message: "fast one".to_owned(),
                },
                InsertOpts::default()
                    .with_metadata(serde_json::Map::from_iter([(
                        "source".to_owned(),
                        serde_json::json!("copy"),
                    )]))
                    .with_tags(["fast-one"]),
            ),
            (
                EchoArgs {
                    message: "fast two".to_owned(),
                },
                InsertOpts::default()
                    .with_pending(true)
                    .with_tags(["fast-two"]),
            ),
        ])
        .await
        .unwrap();
    assert_eq!(fast_count, 2);
    let fast_rows = client
        .job_list(&JobListParams::default().with_tags_any(["fast-one", "fast-two"]))
        .await
        .unwrap();
    assert_eq!(fast_rows.len(), 2);
    assert!(fast_rows.iter().any(|row| row.state == JobState::Pending));
    assert!(
        fast_rows
            .iter()
            .any(|row| row.metadata.get("source") == Some(&serde_json::json!("copy")))
    );

    let inserted = client
        .insert(EchoArgs {
            message: "from Rust".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(inserted.job.row.state, JobState::Available);

    let non_running = client
        .insert(EchoArgs {
            message: "not running".to_owned(),
        })
        .await
        .unwrap();
    let mut transaction = pool.begin().await.unwrap();
    let error = client
        .job_complete_tx(&mut transaction, non_running.job.row.id)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("job must be running"));
    assert!(matches!(
        client.job_complete_tx(&mut transaction, i64::MAX).await,
        Err(riverqueue::Error::NotFound)
    ));
    transaction.rollback().await.unwrap();

    sqlx::query("UPDATE river_job SET state = 'running' WHERE id = $1")
        .bind(non_running.job.row.id)
        .execute(&pool)
        .await
        .unwrap();
    let mut transaction = pool.begin().await.unwrap();
    let completed_then_rolled_back = client
        .job_complete_tx(&mut transaction, non_running.job.row.id)
        .await
        .unwrap();
    assert_eq!(completed_then_rolled_back.state, JobState::Completed);
    transaction.rollback().await.unwrap();
    assert_eq!(
        client.job_get(non_running.job.row.id).await.unwrap().state,
        JobState::Running
    );
    sqlx::query("UPDATE river_job SET state = 'available' WHERE id = $1")
        .bind(non_running.job.row.id)
        .execute(&pool)
        .await
        .unwrap();

    let mut completed_events = client.subscribe(&[EventKind::JobCompleted]).unwrap();
    let run_handle = client.start().unwrap();
    let row = wait_for_state(&client, inserted.job.row.id, JobState::Completed).await;
    assert_eq!(row.attempt, 1);
    assert_eq!(row.attempted_by, ["rust-conformance-client"]);
    assert_eq!(
        row.output(),
        Some(&serde_json::json!({"message": "from Rust"}))
    );
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), completed_events.recv())
            .await
            .unwrap()
            .unwrap();
        if event.as_job().unwrap().job.id == inserted.job.row.id {
            break;
        }
    }

    let failed = client
        .insert_with(FailArgs {}, InsertOpts::default().with_max_attempts(1))
        .await
        .unwrap();
    let failed = wait_for_state(&client, failed.job.row.id, JobState::Discarded).await;
    assert_eq!(failed.errors.len(), 1);
    assert_eq!(failed.errors[0].error, "intentional failure");

    let resumable = client
        .insert_with(ResumableArgs {}, InsertOpts::default().with_max_attempts(2))
        .await
        .unwrap();
    let resumable = wait_for_state(&client, resumable.job.row.id, JobState::Completed).await;
    assert_eq!(resumable.metadata["river:resumable_step"], "first");
    assert_eq!(resumable_first_runs.load(Ordering::SeqCst), 1);
    assert_eq!(resumable_second_runs.load(Ordering::SeqCst), 2);

    let cancelling = client.insert(CancelArgs {}).await.unwrap();
    wait_for_state(&client, cancelling.job.row.id, JobState::Running).await;
    client.job_cancel(cancelling.job.row.id).await.unwrap();
    let cancelled = wait_for_state(&client, cancelling.job.row.id, JobState::Cancelled).await;
    assert!(cancelled.finalized_at.is_some());
    assert!(cancelled.metadata.contains_key("cancel_attempted_at"));

    client
        .queue_add(
            "dynamic",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .unwrap();
    let dynamic = client
        .insert_with(
            EchoArgs {
                message: "dynamic queue".to_owned(),
            },
            InsertOpts::default().with_queue("dynamic"),
        )
        .await
        .unwrap();
    wait_for_state(&client, dynamic.job.row.id, JobState::Completed).await;
    assert!(client.queue_remove("dynamic").unwrap().is_some());

    let unique_options = InsertOpts::default().with_unique(UniqueOpts::new().by_args());
    let unique_first = client
        .insert_with(
            EchoArgs {
                message: "unique".to_owned(),
            },
            unique_options.clone(),
        )
        .await
        .unwrap();
    let unique_second = client
        .insert_with(
            EchoArgs {
                message: "unique".to_owned(),
            },
            unique_options,
        )
        .await
        .unwrap();
    assert_eq!(unique_first.job.row.id, unique_second.job.row.id);
    assert!(unique_second.unique_skipped_as_duplicate);

    let unknown_kind_id: i64 = sqlx::query_scalar(
        "INSERT INTO river_job (args, kind, max_attempts) \
         VALUES ('{}'::jsonb, 'rust_unregistered_kind', 1) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let unknown_kind = wait_for_state(&client, unknown_kind_id, JobState::Discarded).await;
    assert_eq!(unknown_kind.attempt, 1);
    assert_eq!(unknown_kind.errors.len(), 1);
    assert_eq!(
        unknown_kind.errors[0].error,
        "job kind is not registered in the client's Workers bundle: rust_unregistered_kind"
    );

    run_handle.shutdown().await.unwrap();
    let mut restarted_handle = client.start().unwrap();
    restarted_handle.wait_ready().await.unwrap();
    restarted_handle.shutdown().await.unwrap();

    let mut interrupt_workers = WorkerRegistry::new();
    interrupt_workers
        .register::<IgnoresCancelArgs, _>(IgnoresCancelWorker)
        .unwrap();
    let interrupt_client = Client::builder(pool.clone())
        .id("rust-interrupt-client")
        .job_stuck_threshold(Duration::from_millis(10))
        .workers(interrupt_workers)
        .queue("interrupt", QueueConfig::new(1))
        .build()
        .unwrap();
    let interrupted = interrupt_client
        .insert_with(
            IgnoresCancelArgs {},
            InsertOpts::default().with_queue("interrupt"),
        )
        .await
        .unwrap();
    let mut interrupted_events = interrupt_client
        .subscribe(&[EventKind::JobInterrupted])
        .unwrap();
    let interrupt_handle = interrupt_client.start().unwrap();
    wait_for_state(&interrupt_client, interrupted.job.row.id, JobState::Running).await;
    interrupt_handle.shutdown_now().await.unwrap();
    let interrupted_row = interrupt_client
        .job_get(interrupted.job.row.id)
        .await
        .unwrap();
    assert_eq!(interrupted_row.attempt, 0);
    assert_eq!(interrupted_row.state, JobState::Available);
    assert!(interrupted_row.errors.is_empty());
    let event = tokio::time::timeout(Duration::from_secs(1), interrupted_events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.as_job().unwrap().job.id, interrupted.job.row.id);

    let queue = client.queue_get("default").await.unwrap();
    assert_eq!(queue.name, "default");
    assert!(queue.paused_at.is_none());
    client.queue_pause("default").await.unwrap();
    assert!(
        client
            .queue_get("default")
            .await
            .unwrap()
            .paused_at
            .is_some()
    );
    client.queue_resume("default").await.unwrap();
    assert!(
        client
            .queue_get("default")
            .await
            .unwrap()
            .paused_at
            .is_none()
    );
    let queue = client
        .queue_update(
            "default",
            serde_json::Map::from_iter([("owner".to_owned(), serde_json::json!("rust"))]),
        )
        .await
        .unwrap();
    assert_eq!(queue.metadata["owner"], "rust");
    let queues = client
        .queue_list(&QueueListParams::default())
        .await
        .unwrap();
    assert_eq!(queues.len(), 3);
    assert!(queues.iter().any(|queue| queue.name == "dynamic"));

    let listed = client
        .job_list(&JobListParams::default().with_kinds([EchoArgs::KIND]))
        .await
        .unwrap();
    assert!(listed.iter().any(|row| row.id == inserted.job.row.id));
    let updated = client
        .job_update(
            inserted.job.row.id,
            JobUpdateParams::default().with_output(serde_json::json!({"ok": true})),
        )
        .await
        .unwrap();
    assert_eq!(updated.output(), Some(&serde_json::json!({"ok": true})));

    let retried = client.job_retry(failed.id).await.unwrap();
    assert_eq!(retried.state, JobState::Available);
    assert_eq!(retried.max_attempts, 2);
    let deleted = client.job_delete(retried.id).await.unwrap();
    assert_eq!(deleted.id, retried.id);
    assert!(matches!(
        client.job_get(retried.id).await,
        Err(riverqueue::Error::NotFound)
    ));

    let mut transaction = pool.begin().await.unwrap();
    let transaction_insert = client
        .insert_tx(
            &mut transaction,
            EchoArgs {
                message: "from Rust".to_owned(),
            },
        )
        .await
        .unwrap();
    let raw_transaction_insert = client
        .insert_raw_tx(
            &mut transaction,
            EchoArgs::KIND,
            &[],
            serde_json::json!({"message": "raw from Rust"}),
            InsertOpts::default(),
        )
        .await
        .unwrap();
    assert!(matches!(
        client.job_get(transaction_insert.job.row.id).await,
        Err(riverqueue::Error::NotFound)
    ));
    assert!(matches!(
        client.job_get(raw_transaction_insert.job.id).await,
        Err(riverqueue::Error::NotFound)
    ));
    transaction.commit().await.unwrap();
    assert_eq!(
        client
            .job_get(transaction_insert.job.row.id)
            .await
            .unwrap()
            .state,
        JobState::Available
    );
    assert_eq!(
        client
            .job_get(raw_transaction_insert.job.id)
            .await
            .unwrap()
            .encoded_args["message"],
        "raw from Rust"
    );
    let _pool_connection = client
        .postgres_pool()
        .expect("client is configured for PostgreSQL")
        .acquire()
        .await
        .unwrap();

    let mut transaction = pool.begin().await.unwrap();
    let tx_row = client
        .job_get_tx(&mut transaction, transaction_insert.job.row.id)
        .await
        .unwrap();
    assert_eq!(tx_row.id, transaction_insert.job.row.id);
    client
        .job_update_tx(
            &mut transaction,
            tx_row.id,
            JobUpdateParams::default().with_output(serde_json::json!("transactional")),
        )
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    assert!(
        client
            .job_get(transaction_insert.job.row.id)
            .await
            .unwrap()
            .output()
            .is_none()
    );

    let mut maintenance_workers = WorkerRegistry::new();
    maintenance_workers
        .register::<EchoArgs, _>(EchoWorker)
        .unwrap();
    maintenance_workers
        .register::<TransactionalArgs, _>(TransactionalWorker { pool: pool.clone() })
        .unwrap();
    let pilot_completions = Arc::new(AtomicUsize::new(0));
    let pilot_fetches = Arc::new(AtomicUsize::new(0));
    let pilot_maintenance_starts = Arc::new(AtomicUsize::new(0));
    let pilot_maintenance_stops = Arc::new(AtomicUsize::new(0));
    let pilot_runtime_starts = Arc::new(AtomicUsize::new(0));
    let pilot_runtime_stops = Arc::new(AtomicUsize::new(0));
    let maintenance_client = Client::builder(pool.clone())
        .id("rust-maintenance-client")
        .maintenance(
            MaintenanceConfig::default()
                .with_elect_interval(Duration::from_millis(20))
                .with_rescue_after(Duration::from_millis(20))
                .with_rescuer_interval(Duration::from_millis(20))
                .with_scheduler_interval(Duration::from_millis(20)),
        )
        .periodic_job(PeriodicJob::with_options(
            IntervalSchedule::new(Duration::from_mins(1)).unwrap(),
            || EchoArgs {
                message: "periodic run on start".to_owned(),
            },
            PeriodicJobOpts::new()
                .with_id("rust-periodic")
                .run_on_start(),
        ))
        .pilot(TestPilot {
            completions: Arc::clone(&pilot_completions),
            fetches: Arc::clone(&pilot_fetches),
            maintenance_starts: Arc::clone(&pilot_maintenance_starts),
            maintenance_stops: Arc::clone(&pilot_maintenance_stops),
            runtime_starts: Arc::clone(&pilot_runtime_starts),
            runtime_stops: Arc::clone(&pilot_runtime_stops),
        })
        .workers(maintenance_workers)
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    let scheduled = maintenance_client
        .insert_with(
            EchoArgs {
                message: "scheduled by leader".to_owned(),
            },
            InsertOpts::default()
                .with_scheduled_at(chrono::Utc::now() + chrono::Duration::milliseconds(100)),
        )
        .await
        .unwrap();
    let transactional = maintenance_client
        .insert(TransactionalArgs {})
        .await
        .unwrap();
    let stuck_id: i64 = sqlx::query_scalar(
        "INSERT INTO river_job (args, attempt, attempted_at, attempted_by, kind, max_attempts, state) \
         VALUES ('{}'::jsonb, 1, now() - interval '1 second', ARRAY['dead-client'], \
                 'unregistered_stuck_kind', 2, 'running') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let maintenance_handle = maintenance_client.start().unwrap();
    let periodic = wait_for_job_matching(&maintenance_client, |row| {
        row.metadata.get("river:periodic_job_id") == Some(&serde_json::json!("rust-periodic"))
    })
    .await;
    assert_eq!(periodic.metadata["periodic"], true);
    wait_for_state(
        &maintenance_client,
        scheduled.job.row.id,
        JobState::Completed,
    )
    .await;
    let transactional = wait_for_state(
        &maintenance_client,
        transactional.job.row.id,
        JobState::Completed,
    )
    .await;
    assert_eq!(transactional.metadata["transactional_completion"], true);
    assert_eq!(transactional.metadata["extension_handled"], true);
    assert!(pilot_fetches.load(Ordering::SeqCst) > 0);
    assert!(pilot_completions.load(Ordering::SeqCst) > 0);
    assert_eq!(
        maintenance_client
            .job_get(scheduled.job.row.id)
            .await
            .unwrap()
            .metadata["extension_handled"],
        true
    );
    assert_eq!(pilot_maintenance_starts.load(Ordering::SeqCst), 1);
    assert_eq!(pilot_runtime_starts.load(Ordering::SeqCst), 1);
    let rescued = wait_for_state(&maintenance_client, stuck_id, JobState::Discarded).await;
    assert_eq!(rescued.metadata["river:rescue_count"], 1);
    assert_eq!(
        rescued.errors.last().unwrap().error,
        "Stuck job rescued by JobRescuer"
    );
    let leader_id: String = sqlx::query_scalar("SELECT leader_id FROM river_leader")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(leader_id, "rust-maintenance-client");
    maintenance_handle.shutdown().await.unwrap();
    assert_eq!(pilot_maintenance_stops.load(Ordering::SeqCst), 1);
    assert_eq!(pilot_runtime_stops.load(Ordering::SeqCst), 1);
    let leader_count: i64 = sqlx::query_scalar("SELECT count(*) FROM river_leader")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(leader_count, 0);

    sqlx::raw_sql(
        "TRUNCATE river_job, river_notification, river_queue, river_leader RESTART IDENTITY CASCADE; \
         DROP INDEX IF EXISTS rust_maintenance_reindex_idx; \
         CREATE INDEX rust_maintenance_reindex_idx ON river_job (id)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let cleanup_job_ids = sqlx::query_scalar::<_, i64>(
        "INSERT INTO river_job (args, finalized_at, kind, state) VALUES \
         ('{}'::jsonb, now() - interval '1 hour', 'cleanup_cancelled', 'cancelled'), \
         ('{}'::jsonb, now() - interval '1 hour', 'cleanup_completed', 'completed'), \
         ('{}'::jsonb, now() - interval '1 hour', 'cleanup_discarded', 'discarded') \
         RETURNING id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO river_queue (name, created_at, updated_at) \
         VALUES ('stale_cleanup_queue', now() - interval '2 hours', now() - interval '1 hour')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let reindex_file_node_before: i64 = sqlx::query_scalar(
        "SELECT pg_relation_filenode('rust_maintenance_reindex_idx'::regclass)::bigint",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let mut cleanup_workers = WorkerRegistry::new();
    cleanup_workers.register::<EchoArgs, _>(EchoWorker).unwrap();
    let cleanup_client = Client::builder(
        PostgresDatabase::new(pool.clone()).reindex(
            PostgresReindexConfig::default()
                .with_index_names(["rust_maintenance_reindex_idx"])
                .with_schedule(PostgresReindexSchedule::Interval(Duration::from_millis(50))),
        ),
    )
    .id("rust-cleanup-client")
    .maintenance(
        MaintenanceConfig::default()
            .with_cancelled_job_retention(Some(Duration::from_millis(1)))
            .with_completed_job_retention(Some(Duration::from_millis(1)))
            .with_discarded_job_retention(Some(Duration::from_millis(1)))
            .with_elect_interval(Duration::from_millis(20))
            .with_job_cleaner_interval(Duration::from_millis(20))
            .with_queue_cleaner_interval(Duration::from_millis(20))
            .with_queue_retention(Duration::from_millis(1)),
    )
    .workers(cleanup_workers)
    .queue("cleanup_active", QueueConfig::new(1))
    .build()
    .unwrap();
    let cleanup_handle = cleanup_client.start().unwrap();
    let cleanup_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let old_job_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM river_job WHERE id = ANY($1::bigint[])")
                .bind(&cleanup_job_ids)
                .fetch_one(&pool)
                .await
                .unwrap();
        let stale_queue_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM river_queue WHERE name = 'stale_cleanup_queue'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let reindex_file_node_after: i64 = sqlx::query_scalar(
            "SELECT pg_relation_filenode('rust_maintenance_reindex_idx'::regclass)::bigint",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if old_job_count == 0
            && stale_queue_count == 0
            && reindex_file_node_after != reindex_file_node_before
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < cleanup_deadline,
            "maintenance did not clean old jobs/queues and reindex in time: \
             old_job_count={old_job_count}, stale_queue_count={stale_queue_count}, \
             reindex_file_node_before={reindex_file_node_before}, \
             reindex_file_node_after={reindex_file_node_after}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cleanup_handle.shutdown().await.unwrap();
    sqlx::query("DROP INDEX rust_maintenance_reindex_idx")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end rescuer scenario compares all worker timeout and retry overrides"
)]
async fn rescuer_honors_worker_timeout_and_retry_overrides() {
    let database_url = std::env::var("RIVER_RUST_DATABASE_URL")
        .expect("RIVER_RUST_DATABASE_URL must point at a disposable test database");
    let pool = PgPool::connect(&database_url).await.unwrap();
    let schema = SchemaName::new("rust_rescuer_timeout_test").unwrap();
    sqlx::raw_sql(
        "DROP SCHEMA IF EXISTS rust_rescuer_timeout_test CASCADE; \
         CREATE SCHEMA rust_rescuer_timeout_test",
    )
    .execute(&pool)
    .await
    .unwrap();
    PostgresMigrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await
        .unwrap();

    let table = schema.qualify("river_job");
    let insert_sql = format!(
        "INSERT INTO {table} \
            (args, attempt, attempted_at, attempted_by, kind, max_attempts, state) \
         VALUES ('{{}}'::jsonb, 1, now() - interval '1 second', ARRAY['dead-client'], $1, $2, 'running') \
         RETURNING id"
    );
    let default_timeout_id: i64 = sqlx::query_scalar(AssertSqlSafe(insert_sql.clone()))
        .bind(RescueDefaultTimeoutArgs::KIND)
        .bind(1_i16)
        .fetch_one(&pool)
        .await
        .unwrap();
    let disabled_timeout_id: i64 = sqlx::query_scalar(AssertSqlSafe(insert_sql.clone()))
        .bind(RescueDisabledTimeoutArgs::KIND)
        .bind(1_i16)
        .fetch_one(&pool)
        .await
        .unwrap();
    let long_timeout_id: i64 = sqlx::query_scalar(AssertSqlSafe(insert_sql.clone()))
        .bind(RescueLongTimeoutArgs::KIND)
        .bind(1_i16)
        .fetch_one(&pool)
        .await
        .unwrap();
    let retry_override_id: i64 = sqlx::query_scalar(AssertSqlSafe(insert_sql))
        .bind(RescueRetryOverrideArgs::KIND)
        .bind(2_i16)
        .fetch_one(&pool)
        .await
        .unwrap();

    let mut workers = WorkerRegistry::new();
    workers
        .register::<RescueDefaultTimeoutArgs, _>(RescueDefaultTimeoutWorker)
        .unwrap();
    workers
        .register::<RescueDisabledTimeoutArgs, _>(RescueDisabledTimeoutWorker)
        .unwrap();
    workers
        .register::<RescueLongTimeoutArgs, _>(RescueLongTimeoutWorker)
        .unwrap();
    workers
        .register::<RescueRetryOverrideArgs, _>(RescueRetryOverrideWorker)
        .unwrap();
    let client = Client::builder(
        PostgresDatabase::new(pool.clone())
            .schema(schema)
            .reindex(PostgresReindexConfig::default().with_index_names([] as [&str; 0])),
    )
    .id("rust-rescuer-timeout-client")
    .job_timeout(Some(Duration::from_millis(100)))
    .maintenance(
        MaintenanceConfig::default()
            .with_elect_interval(Duration::from_millis(20))
            .with_rescue_after(Duration::from_millis(20))
            .with_rescuer_interval(Duration::from_millis(20)),
    )
    .queue("default", QueueConfig::new(1))
    .workers(workers)
    .build()
    .unwrap();
    let handle = client.start().unwrap();

    let default_timeout = wait_for_state(&client, default_timeout_id, JobState::Discarded).await;
    assert_eq!(default_timeout.metadata["river:rescue_count"], 1);
    assert_eq!(default_timeout.errors.len(), 1);
    let retry_override = wait_for_state(&client, retry_override_id, JobState::Retryable).await;
    assert_eq!(retry_override.metadata["river:rescue_count"], 1);
    assert_eq!(retry_override.errors.len(), 1);
    assert!(
        retry_override.scheduled_at > chrono::Utc::now() + chrono::Duration::minutes(90),
        "worker retry override was not applied: {:?}",
        retry_override.scheduled_at
    );

    tokio::time::sleep(Duration::from_millis(100)).await;
    for id in [disabled_timeout_id, long_timeout_id] {
        let row = client.job_get(id).await.unwrap();
        assert_eq!(row.state, JobState::Running);
        assert!(row.errors.is_empty());
        assert!(!row.metadata.contains_key("river:rescue_count"));
    }

    handle.shutdown().await.unwrap();
    sqlx::raw_sql("DROP SCHEMA rust_rescuer_timeout_test CASCADE")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn resumable_cursor_and_transactional_checkpoints() {
    let detached_context = WorkContext::new(CancellationToken::new());
    let cursor_error = detached_context
        .resumable_set_cursor(&ResumableCursor { offset: 1 })
        .await
        .unwrap_err();
    assert!(
        cursor_error
            .to_string()
            .contains("resumable cursor can only be set inside a resumable cursor step")
    );

    let database_url = std::env::var("RIVER_RUST_DATABASE_URL")
        .expect("RIVER_RUST_DATABASE_URL must point at a disposable test database");
    let pool = PgPool::connect(&database_url).await.unwrap();
    let schema = SchemaName::new("rust_resumable_checkpoint_test").unwrap();
    sqlx::raw_sql(
        "DROP SCHEMA IF EXISTS rust_resumable_checkpoint_test CASCADE; \
         CREATE SCHEMA rust_resumable_checkpoint_test",
    )
    .execute(&pool)
    .await
    .unwrap();
    PostgresMigrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await
        .unwrap();

    let cursor_values = Arc::new(Mutex::new(Vec::new()));
    let validate_runs = Arc::new(AtomicUsize::new(0));
    let mut workers = WorkerRegistry::new();
    workers
        .register::<ResumableCheckpointArgs, _>(ResumableCheckpointWorker {
            cursor_values: Arc::clone(&cursor_values),
            pool: pool.clone(),
            validate_runs: Arc::clone(&validate_runs),
        })
        .unwrap();
    let client = Client::builder(
        PostgresDatabase::new(pool.clone())
            .schema(schema)
            .reindex(PostgresReindexConfig::default().with_index_names([] as [&str; 0])),
    )
    .id("rust-resumable-checkpoint-test")
    .maintenance(
        MaintenanceConfig::default()
            .with_elect_interval(Duration::from_millis(20))
            .with_scheduler_interval(Duration::from_millis(20)),
    )
    .without_notifications()
    .queue(
        "default",
        QueueConfig::new(5)
            .with_fetch_cooldown(Duration::from_millis(1))
            .with_fetch_poll_interval(Duration::from_millis(10)),
    )
    .workers(workers)
    .build()
    .unwrap();
    let mut job_ids = std::collections::HashMap::new();
    for mode in [
        "commit_cursor",
        "commit_step",
        "cursor_retry",
        "rollback_cursor",
        "rollback_step",
    ] {
        let inserted = client
            .insert_with(
                ResumableCheckpointArgs {
                    mode: mode.to_owned(),
                },
                InsertOpts::default().with_max_attempts(2),
            )
            .await
            .unwrap();
        job_ids.insert(mode, inserted.job.row.id);
    }
    let handle = client.start().unwrap();

    let first_failure = wait_for_state(&client, job_ids["cursor_retry"], JobState::Retryable).await;
    assert_eq!(first_failure.metadata["river:resumable_step"], "validate");
    assert_eq!(
        first_failure.metadata["river:resumable_cursor"]["process"],
        serde_json::json!({"offset": 42})
    );
    assert_eq!(first_failure.errors.len(), 1);

    let resumed = wait_for_state(&client, job_ids["cursor_retry"], JobState::Completed).await;
    assert_eq!(resumed.attempt, 2);
    assert_eq!(validate_runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        *cursor_values.lock().unwrap(),
        [ResumableCursor::default(), ResumableCursor { offset: 42 }]
    );

    let committed_cursor =
        wait_for_state(&client, job_ids["commit_cursor"], JobState::Completed).await;
    assert_eq!(
        committed_cursor.metadata["river:resumable_step"],
        "tx_cursor"
    );
    assert_eq!(
        committed_cursor.metadata["river:resumable_cursor"]["tx_cursor"],
        serde_json::json!({"offset": 7})
    );
    let committed_step = wait_for_state(&client, job_ids["commit_step"], JobState::Completed).await;
    assert_eq!(committed_step.metadata["river:resumable_step"], "tx_step");
    assert!(
        !committed_step
            .metadata
            .contains_key("river:resumable_cursor")
    );

    for mode in ["rollback_cursor", "rollback_step"] {
        let rolled_back = wait_for_state(&client, job_ids[mode], JobState::Completed).await;
        assert!(!rolled_back.metadata.contains_key("river:resumable_step"));
        assert!(!rolled_back.metadata.contains_key("river:resumable_cursor"));
    }

    handle.shutdown().await.unwrap();
    sqlx::raw_sql("DROP SCHEMA rust_resumable_checkpoint_test CASCADE")
        .execute(&pool)
        .await
        .unwrap();
}

async fn assert_cancellation_wins(pool: &PgPool, schema: &SchemaName, direct_completion: bool) {
    const ATTEMPT: i16 = 7;

    let pilot_completions = Arc::new(AtomicUsize::new(0));
    let mut workers = WorkerRegistry::new();
    workers.register::<EchoArgs, _>(EchoWorker).unwrap();
    let builder = Client::builder(PostgresDatabase::new(pool.clone()).schema(schema.clone()))
        .id(if direct_completion {
            "rust-cancellation-direct"
        } else {
            "rust-cancellation-batch"
        })
        .retry_policy(LongRetryPolicy)
        .workers(workers)
        .queue(
            "default",
            QueueConfig::new(1).with_fetch_poll_interval(Duration::from_mins(1)),
        )
        .without_notifications();
    let client = if direct_completion {
        builder
            .pilot(ContinueCompletionPilot {
                completions: Arc::clone(&pilot_completions),
            })
            .build()
            .unwrap()
    } else {
        builder.build().unwrap()
    };

    let original_scheduled_at = chrono::Utc::now() + chrono::Duration::hours(2);
    let table = schema.qualify("river_job");
    let mut claimed_rows = Vec::new();
    let mut expected = Vec::new();
    for message in ["long snooze", "short snooze", "retryable error"] {
        let inserted = client
            .insert_with(
                EchoArgs {
                    message: message.to_owned(),
                },
                InsertOpts::default()
                    .with_max_attempts(20)
                    .with_scheduled_at(original_scheduled_at),
            )
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {table} SET attempt = $2, attempted_at = now(), state = 'running' WHERE id = $1"
        )))
        .bind(inserted.job.row.id)
        .bind(ATTEMPT)
        .execute(pool)
        .await
        .unwrap();
        let marked = client.job_cancel(inserted.job.row.id).await.unwrap();
        assert_eq!(marked.state, JobState::Running);
        assert!(marked.metadata.contains_key("cancel_attempted_at"));
        expected.push((marked.id, marked.attempt, marked.scheduled_at));
        claimed_rows.push(marked);
    }

    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();
    let results: Vec<Result<WorkOutcome, BoxError>> = vec![
        Ok(WorkOutcome::Snooze(Duration::from_hours(1))),
        Ok(WorkOutcome::Snooze(Duration::ZERO)),
        Err(Box::new(std::io::Error::other("retryable failure"))),
    ];
    client
        .extension_persist_claimed_outcomes(
            &WorkContext::new(CancellationToken::new()),
            claimed_rows.into_iter().zip(results).collect(),
        )
        .await
        .unwrap();

    for (id, attempt, scheduled_at) in expected {
        let cancelled = wait_for_state(&client, id, JobState::Cancelled).await;
        assert_eq!(cancelled.attempt, attempt);
        assert_eq!(cancelled.scheduled_at, scheduled_at);
    }
    assert_eq!(
        pilot_completions.load(Ordering::SeqCst),
        usize::from(direct_completion) * 3
    );
    run.shutdown().await.unwrap();
}

async fn wait_for_job_matching(client: &Client, predicate: impl Fn(&JobRow) -> bool) -> JobRow {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let rows = client
            .job_list(&JobListParams::default().with_limit(10_000))
            .await
            .unwrap();
        if let Some(row) = rows.into_iter().find(&predicate) {
            return row;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "matching job was not inserted"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_state(client: &Client, id: i64, expected: JobState) -> JobRow {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = client.job_get(id).await.unwrap();
        if row.state == expected {
            return row;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job did not reach {expected:?}; last state: {:?}",
            row.state
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
