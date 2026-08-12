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
    CompletionAction, CompletionParams, FetchParams, MaintenanceService, Pilot, PilotError,
    RuntimeService, SchemaName,
};
use riverqueue::{
    Client, EventKind, InsertOpts, IntervalSchedule, Job, JobArgs, JobListParams, JobRow, JobState,
    JobUpdateParams, MaintenanceConfig, PeriodicJob, PeriodicJobOpts, QueueConfig, QueueListParams,
    ReindexerSchedule, UniqueOpts, WorkContext, WorkOutcome, Worker, WorkerRegistry, WorkerTimeout,
};
use riverqueue_migrate::{Direction, MigrateOpts};
use riverqueue_migrate::{MIGRATION_VERSION_LATEST, Migrator};
use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, PgConnection, PgPool};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_conformance_echo")]
struct EchoArgs {
    message: String,
}

struct EchoWorker;

#[async_trait]
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

#[async_trait]
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

#[async_trait]
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

#[async_trait]
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

#[async_trait]
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

#[async_trait]
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

#[async_trait]
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

#[async_trait]
impl Worker<RescueRetryOverrideArgs> for RescueRetryOverrideWorker {
    type Error = Infallible;

    fn next_retry(
        &self,
        _job: &Job<RescueRetryOverrideArgs>,
        _error: &str,
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

#[async_trait]
impl Worker<ResumableCheckpointArgs> for ResumableCheckpointWorker {
    type Error = riverqueue::Error;

    fn next_retry(
        &self,
        job: &Job<ResumableCheckpointArgs>,
        _error: &str,
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
                                return Err(riverqueue::Error::Runtime(
                                    "intentional resumable cursor failure".to_owned(),
                                ));
                            }
                            Ok(())
                        },
                    )
                    .await?;
            }
            "commit_cursor" | "rollback_cursor" => {
                let checkpoint_client = context.client().unwrap().clone();
                let checkpoint_context = context.clone();
                let job_id = job.row.id;
                let mode = job.args.mode.clone();
                let pool = self.pool.clone();
                context
                    .resumable_step_with_cursor("tx_cursor", move |_: ResumableCursor| async move {
                        let mut transaction = pool.begin().await?;
                        checkpoint_context
                            .resumable_set_step_cursor_tx(
                                &checkpoint_client,
                                &mut transaction,
                                job_id,
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
                let checkpoint_client = context.client().unwrap().clone();
                let checkpoint_context = context.clone();
                let job_id = job.row.id;
                let mode = job.args.mode.clone();
                let pool = self.pool.clone();
                context
                    .resumable_step("tx_step", move || async move {
                        let mut transaction = pool.begin().await?;
                        checkpoint_context
                            .resumable_set_step_tx(&checkpoint_client, &mut transaction, job_id)
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
                return Err(riverqueue::Error::Runtime(format!(
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
        connection: &mut PgConnection,
        params: &FetchParams,
    ) -> Result<Option<Vec<i64>>, PilotError> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let table = params.schema.qualify("river_job");
        let queue_table = params.schema.qualify("river_queue");
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
                .fetch_all(connection)
                .await?,
        ))
    }

    async fn before_job_completion(
        &self,
        connection: &mut PgConnection,
        params: &CompletionParams,
    ) -> Result<CompletionAction, PilotError> {
        self.completions.fetch_add(1, Ordering::SeqCst);
        let table = params.schema.qualify("river_job");
        let sql = format!(
            "UPDATE {table} SET state = 'completed', finalized_at = now(), \
             metadata = metadata || $2::jsonb || '{{\"extension_handled\": true}}'::jsonb \
             WHERE id = $1"
        );
        sqlx::query(AssertSqlSafe(sql))
            .bind(params.job_id)
            .bind(sqlx::types::Json(&params.metadata_updates))
            .execute(connection)
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
        _pool: PgPool,
        _schema: SchemaName,
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
        _pool: PgPool,
        _schema: SchemaName,
        cancellation: CancellationToken,
    ) -> Result<(), PilotError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        cancellation.cancelled().await;
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
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

#[async_trait]
impl Worker<TransactionalArgs> for TransactionalWorker {
    type Error = riverqueue::Error;

    async fn work(
        &self,
        context: WorkContext,
        job: Job<TransactionalArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        assert_eq!(context.client().unwrap().id(), "rust-maintenance-client");
        context
            .metadata_set("transactional_completion", serde_json::json!(true))
            .await;
        let mut transaction = self.pool.begin().await?;
        let completed = context
            .job_complete_tx(&mut transaction, job.row.id)
            .await?;
        assert_eq!(completed.state, JobState::Completed);
        assert_eq!(completed.metadata["transactional_completion"], true);
        assert_eq!(completed.metadata["extension_handled"], true);
        transaction.commit().await?;
        Ok(WorkOutcome::Complete)
    }
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
    Migrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await
        .unwrap();
    let client = Client::builder(pool.clone())
        .schema(schema)
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
            InsertOpts {
                unique: UniqueOpts {
                    by_args: true,
                    ..UniqueOpts::default()
                },
                ..InsertOpts::default()
            },
        ),
        (
            "by_args_and_queue",
            InsertOpts {
                queue: "unique_queue".to_owned(),
                unique: UniqueOpts {
                    by_args: true,
                    by_queue: true,
                    ..UniqueOpts::default()
                },
                ..InsertOpts::default()
            },
        ),
        (
            "by_args_and_states",
            InsertOpts {
                unique: UniqueOpts {
                    by_args: true,
                    by_state: Some(all_states),
                    ..UniqueOpts::default()
                },
                ..InsertOpts::default()
            },
        ),
        (
            "by_args_and_period",
            InsertOpts {
                scheduled_at: Some(fixed_scheduled_at),
                unique: UniqueOpts {
                    by_args: true,
                    by_period: Some(Duration::from_mins(1)),
                    ..UniqueOpts::default()
                },
                ..InsertOpts::default()
            },
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
                client.insert(EchoArgs { message }, opts).await
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
    Migrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await
        .unwrap();
    let client = Client::builder(pool.clone())
        .schema(schema.clone())
        .build()
        .unwrap();
    let table = schema.qualify("river_job");

    let past_scheduled_at = chrono::Utc::now() - chrono::Duration::minutes(1);
    let ordered = client
        .insert_many([
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
                InsertOpts {
                    scheduled_at: Some(past_scheduled_at),
                    ..InsertOpts::default()
                },
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

    let unique_opts = InsertOpts {
        unique: UniqueOpts {
            by_args: true,
            ..UniqueOpts::default()
        },
        ..InsertOpts::default()
    };
    let unique = client
        .insert_many([
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
        .insert_many_tx(
            &mut transaction,
            ["tx-rollback-one", "tx-rollback-two"].map(|message| {
                (
                    EchoArgs {
                        message: message.to_owned(),
                    },
                    InsertOpts {
                        tags: vec!["tx-rollback".to_owned()],
                        ..InsertOpts::default()
                    },
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
        .insert_many_tx(
            &mut transaction,
            ["tx-commit-one", "tx-commit-two"].map(|message| {
                (
                    EchoArgs {
                        message: message.to_owned(),
                    },
                    InsertOpts {
                        tags: vec!["tx-commit".to_owned()],
                        ..InsertOpts::default()
                    },
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
        .insert_many([
            (
                EchoArgs {
                    message: "atomic-valid".to_owned(),
                },
                InsertOpts {
                    tags: vec!["atomic-ordinary".to_owned()],
                    ..InsertOpts::default()
                },
            ),
            (
                EchoArgs {
                    message: "atomic-invalid".to_owned(),
                },
                InsertOpts {
                    priority: 0,
                    ..InsertOpts::default()
                },
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
    assert_eq!(
        client
            .insert_many_fast_tx(
                &mut transaction,
                ["fast-rollback-one", "fast-rollback-two"].map(|message| {
                    (
                        EchoArgs {
                            message: message.to_owned(),
                        },
                        InsertOpts {
                            tags: vec!["fast-rollback".to_owned()],
                            ..InsertOpts::default()
                        },
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
            .insert_many_fast_tx(
                &mut transaction,
                ["fast-commit-one", "fast-commit-two"].map(|message| {
                    (
                        EchoArgs {
                            message: message.to_owned(),
                        },
                        InsertOpts {
                            tags: vec!["fast-commit".to_owned()],
                            ..InsertOpts::default()
                        },
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
        .insert(
            EchoArgs {
                message: "fast-unique-conflict".to_owned(),
            },
            unique_opts,
        )
        .await
        .unwrap();
    let mut transaction = pool.begin().await.unwrap();
    let fast_atomic = client
        .insert_many_fast_tx(
            &mut transaction,
            [
                (
                    EchoArgs {
                        message: "fast-atomic-valid".to_owned(),
                    },
                    InsertOpts {
                        tags: vec!["fast-atomic".to_owned()],
                        ..InsertOpts::default()
                    },
                ),
                (
                    EchoArgs {
                        message: "fast-unique-conflict".to_owned(),
                    },
                    InsertOpts {
                        unique: UniqueOpts {
                            by_args: true,
                            ..UniqueOpts::default()
                        },
                        ..InsertOpts::default()
                    },
                ),
            ],
        )
        .await;
    assert!(fast_atomic.is_err());
    transaction.rollback().await.unwrap();
    let fast_atomic_count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {table} WHERE 'fast-atomic' = ANY(tags)"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
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
    let migrator = Migrator::new(pool.clone());
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
    let custom_migrator = Migrator::new(pool.clone())
        .with_schema(riverqueue::SchemaName::new("rust_migration_test").unwrap());
    let first_up = custom_migrator
        .migrate(
            Direction::Up,
            MigrateOpts {
                target_version: Some(4),
                ..MigrateOpts::default()
            },
        )
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
        .migrate(
            Direction::Down,
            MigrateOpts {
                target_version: Some(3),
                ..MigrateOpts::default()
            },
        )
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
            MigrateOpts {
                dry_run: true,
                max_steps: Some(2),
                ..MigrateOpts::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(dry_run.versions.len(), 2);
    assert_eq!(
        custom_migrator.existing_versions().await.unwrap(),
        (1..=MIGRATION_VERSION_LATEST).collect::<Vec<_>>()
    );
    custom_migrator
        .migrate(
            Direction::Down,
            MigrateOpts {
                target_version: Some(-1),
                ..MigrateOpts::default()
            },
        )
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
        .insert_many_fast([
            (
                EchoArgs {
                    message: "fast one".to_owned(),
                },
                InsertOpts {
                    metadata: serde_json::Map::from_iter([(
                        "source".to_owned(),
                        serde_json::json!("copy"),
                    )]),
                    tags: vec!["fast-one".to_owned()],
                    ..InsertOpts::default()
                },
            ),
            (
                EchoArgs {
                    message: "fast two".to_owned(),
                },
                InsertOpts {
                    pending: true,
                    tags: vec!["fast-two".to_owned()],
                    ..InsertOpts::default()
                },
            ),
        ])
        .await
        .unwrap();
    assert_eq!(fast_count, 2);
    let fast_rows = client
        .job_list(&JobListParams {
            tags_any: vec!["fast-one".to_owned(), "fast-two".to_owned()],
            ..JobListParams::default()
        })
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
        .insert(
            EchoArgs {
                message: "from Rust".to_owned(),
            },
            InsertOpts::default(),
        )
        .await
        .unwrap();
    assert_eq!(inserted.job.row.state, JobState::Available);

    let non_running = client
        .insert(
            EchoArgs {
                message: "not running".to_owned(),
            },
            InsertOpts::default(),
        )
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
        if event.job.unwrap().id == inserted.job.row.id {
            break;
        }
    }

    let failed = client
        .insert(
            FailArgs {},
            InsertOpts {
                max_attempts: 1,
                ..InsertOpts::default()
            },
        )
        .await
        .unwrap();
    let failed = wait_for_state(&client, failed.job.row.id, JobState::Discarded).await;
    assert_eq!(failed.errors.len(), 1);
    assert_eq!(failed.errors[0].error, "intentional failure");

    let resumable = client
        .insert(
            ResumableArgs {},
            InsertOpts {
                max_attempts: 2,
                ..InsertOpts::default()
            },
        )
        .await
        .unwrap();
    let resumable = wait_for_state(&client, resumable.job.row.id, JobState::Completed).await;
    assert_eq!(resumable.metadata["river:resumable_step"], "first");
    assert_eq!(resumable_first_runs.load(Ordering::SeqCst), 1);
    assert_eq!(resumable_second_runs.load(Ordering::SeqCst), 2);

    let cancelling = client
        .insert(CancelArgs {}, InsertOpts::default())
        .await
        .unwrap();
    wait_for_state(&client, cancelling.job.row.id, JobState::Running).await;
    client.job_cancel(cancelling.job.row.id).await.unwrap();
    let cancelled = wait_for_state(&client, cancelling.job.row.id, JobState::Cancelled).await;
    assert!(cancelled.finalized_at.is_some());
    assert!(cancelled.metadata.contains_key("cancel_attempted_at"));

    client
        .queue_add(
            "dynamic",
            QueueConfig {
                fetch_cooldown: Duration::from_millis(1),
                fetch_poll_interval: Duration::from_millis(10),
                max_workers: 1,
            },
        )
        .unwrap();
    let dynamic = client
        .insert(
            EchoArgs {
                message: "dynamic queue".to_owned(),
            },
            InsertOpts {
                queue: "dynamic".to_owned(),
                ..InsertOpts::default()
            },
        )
        .await
        .unwrap();
    wait_for_state(&client, dynamic.job.row.id, JobState::Completed).await;
    assert!(client.queue_remove("dynamic").unwrap().is_some());

    let unique_options = InsertOpts {
        unique: UniqueOpts {
            by_args: true,
            ..UniqueOpts::default()
        },
        ..InsertOpts::default()
    };
    let unique_first = client
        .insert(
            EchoArgs {
                message: "unique".to_owned(),
            },
            unique_options.clone(),
        )
        .await
        .unwrap();
    let unique_second = client
        .insert(
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
        .insert(
            IgnoresCancelArgs {},
            InsertOpts {
                queue: "interrupt".to_owned(),
                ..InsertOpts::default()
            },
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
    assert_eq!(event.job.unwrap().id, interrupted.job.row.id);

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
        .job_list(&JobListParams {
            kinds: vec![EchoArgs::KIND.to_owned()],
            ..JobListParams::default()
        })
        .await
        .unwrap();
    assert!(listed.iter().any(|row| row.id == inserted.job.row.id));
    let updated = client
        .job_update(
            inserted.job.row.id,
            JobUpdateParams {
                output: Some(serde_json::json!({"ok": true})),
                ..JobUpdateParams::default()
            },
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
            InsertOpts::default(),
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
    let _pool_connection = client.pool().acquire().await.unwrap();

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
            JobUpdateParams {
                output: Some(serde_json::json!("transactional")),
                ..JobUpdateParams::default()
            },
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
        .maintenance(MaintenanceConfig {
            elect_interval: Duration::from_millis(20),
            rescue_after: Duration::from_millis(20),
            rescuer_interval: Duration::from_millis(20),
            scheduler_interval: Duration::from_millis(20),
            ..MaintenanceConfig::default()
        })
        .periodic_job(PeriodicJob::with_defaults(
            IntervalSchedule::new(Duration::from_mins(1)).unwrap(),
            || EchoArgs {
                message: "periodic run on start".to_owned(),
            },
            PeriodicJobOpts {
                id: Some("rust-periodic".to_owned()),
                run_on_start: true,
            },
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
            QueueConfig {
                fetch_cooldown: Duration::from_millis(1),
                fetch_poll_interval: Duration::from_millis(10),
                max_workers: 1,
            },
        )
        .build()
        .unwrap();
    let scheduled = maintenance_client
        .insert(
            EchoArgs {
                message: "scheduled by leader".to_owned(),
            },
            InsertOpts {
                scheduled_at: Some(chrono::Utc::now() + chrono::Duration::milliseconds(100)),
                ..InsertOpts::default()
            },
        )
        .await
        .unwrap();
    let transactional = maintenance_client
        .insert(TransactionalArgs {}, InsertOpts::default())
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
    let cleanup_client = Client::builder(pool.clone())
        .id("rust-cleanup-client")
        .maintenance(MaintenanceConfig {
            cancelled_job_retention: Some(Duration::from_millis(1)),
            completed_job_retention: Some(Duration::from_millis(1)),
            discarded_job_retention: Some(Duration::from_millis(1)),
            elect_interval: Duration::from_millis(20),
            job_cleaner_interval: Duration::from_millis(20),
            queue_cleaner_interval: Duration::from_millis(20),
            queue_retention: Duration::from_millis(1),
            reindexer_index_names: vec!["rust_maintenance_reindex_idx".to_owned()],
            reindexer_schedule: ReindexerSchedule::Interval(Duration::from_millis(50)),
            ..MaintenanceConfig::default()
        })
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
    Migrator::new(pool.clone())
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
    let client = Client::builder(pool.clone())
        .id("rust-rescuer-timeout-client")
        .job_timeout(Some(Duration::from_millis(100)))
        .maintenance(MaintenanceConfig {
            elect_interval: Duration::from_millis(20),
            rescue_after: Duration::from_millis(20),
            rescuer_interval: Duration::from_millis(20),
            reindexer_index_names: Vec::new(),
            ..MaintenanceConfig::default()
        })
        .queue("default", QueueConfig::new(1))
        .schema(schema)
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
    Migrator::new(pool.clone())
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
    let client = Client::builder(pool.clone())
        .id("rust-resumable-checkpoint-test")
        .maintenance(MaintenanceConfig {
            elect_interval: Duration::from_millis(20),
            reindexer_index_names: Vec::new(),
            scheduler_interval: Duration::from_millis(20),
            ..MaintenanceConfig::default()
        })
        .poll_only(true)
        .queue(
            "default",
            QueueConfig {
                fetch_cooldown: Duration::from_millis(1),
                fetch_poll_interval: Duration::from_millis(10),
                max_workers: 5,
            },
        )
        .schema(schema)
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
            .insert(
                ResumableCheckpointArgs {
                    mode: mode.to_owned(),
                },
                InsertOpts {
                    max_attempts: 2,
                    ..InsertOpts::default()
                },
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

async fn wait_for_job_matching(client: &Client, predicate: impl Fn(&JobRow) -> bool) -> JobRow {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let rows = client
            .job_list(&JobListParams {
                limit: 10_000,
                ..JobListParams::default()
            })
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
