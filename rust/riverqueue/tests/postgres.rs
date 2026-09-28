#![cfg(all(feature = "postgres", river_postgres_tests))]

mod support;

use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use riverqueue::__private::ClientBuilderExt;
use riverqueue::__private::{
    ClaimedJob, DatabaseConnection, JobSetStateParams, MaintenanceService,
    MaintenanceServiceContext, Pilot, PilotError, PilotProducer, ProducerClaimContext,
    ProducerClaimNext, ProducerStartContext, RuntimeService, RuntimeServiceContext,
};
use riverqueue::{
    Client, EventKind, InsertBatch, InsertOpts, IntervalSchedule, Job, JobArgs, JobListOrderBy,
    JobListParams, JobRow, JobState, JobUpdateParams, MaintenanceConfig, PeriodicJob,
    PeriodicJobOpts, QueueConfig, QueueListParams, UniqueOpts, WorkContext, WorkError, WorkOutcome,
    Worker, WorkerRegistry, WorkerTimeout,
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

    fn work(
        &self,
        context: WorkContext,
        job: Job<EchoArgs>,
    ) -> impl Future<Output = Result<WorkOutcome, Self::Error>> + Send {
        assert!(!job.args.message.is_empty());
        context
            .record_output(serde_json::json!({"message": job.args.message}))
            .unwrap();
        std::future::ready(Ok(WorkOutcome::Complete))
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

    fn work(
        &self,
        _context: WorkContext,
        _job: Job<FailArgs>,
    ) -> impl std::future::Future<Output = Result<WorkOutcome, Self::Error>> + Send {
        std::future::ready(Err(std::io::Error::other("intentional failure")))
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

    fn work(
        &self,
        _context: WorkContext,
        _job: Job<RescueDefaultTimeoutArgs>,
    ) -> impl std::future::Future<Output = Result<WorkOutcome, Self::Error>> + Send {
        std::future::ready(Ok(WorkOutcome::Complete))
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

    fn work(
        &self,
        _context: WorkContext,
        _job: Job<RescueDisabledTimeoutArgs>,
    ) -> impl std::future::Future<Output = Result<WorkOutcome, Self::Error>> + Send {
        std::future::ready(Ok(WorkOutcome::Complete))
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

    fn work(
        &self,
        _context: WorkContext,
        _job: Job<RescueLongTimeoutArgs>,
    ) -> impl std::future::Future<Output = Result<WorkOutcome, Self::Error>> + Send {
        std::future::ready(Ok(WorkOutcome::Complete))
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

    fn work(
        &self,
        _context: WorkContext,
        _job: Job<RescueRetryOverrideArgs>,
    ) -> impl std::future::Future<Output = Result<WorkOutcome, Self::Error>> + Send {
        std::future::ready(Ok(WorkOutcome::Complete))
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
    type Error = riverqueue::BoxError;

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
                                    .resumable_set_cursor(&ResumableCursor { offset: 42 })?;
                                return Err("intentional resumable cursor failure".into());
                            }
                            Ok::<(), riverqueue::BoxError>(())
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
                return Err(format!("unknown test mode {mode}").into());
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

#[derive(Default)]
struct ResumableWorker {
    first_runs: Arc<AtomicUsize>,
    second_runs: Arc<AtomicUsize>,
}

struct TransactionalWorker {
    pool: PgPool,
}

#[derive(Clone, Default)]
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
    fn intercepts_job_set_state(&self) -> bool {
        true
    }

    async fn start_producer(
        &self,
        _context: ProducerStartContext,
    ) -> Result<Option<Box<dyn PilotProducer>>, PilotError> {
        Ok(Some(Box::new(CountingProducer {
            fetches: Arc::clone(&self.fetches),
        })))
    }

    async fn after_jobs_set_state(
        &self,
        connection: DatabaseConnection<'_>,
        params: &JobSetStateParams,
    ) -> Result<(), PilotError> {
        self.completions
            .fetch_add(params.jobs.len(), Ordering::SeqCst);
        let table = params
            .database
            .postgres_schema()
            .unwrap()
            .qualify("river_job");
        let completed = params
            .jobs
            .iter()
            .filter(|job| job.state == JobState::Completed)
            .map(|job| job.id)
            .collect::<Vec<_>>();
        let sql = format!(
            "UPDATE {table} SET metadata = metadata || '{{\"extension_handled\": true}}'::jsonb \
             WHERE id = ANY($1)"
        );
        sqlx::query(AssertSqlSafe(sql))
            .bind(completed)
            .execute(connection.into_postgres().unwrap())
            .await?;
        Ok(())
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

/// Claims with River's standard claim in its own transaction and counts
/// claims.
struct CountingProducer {
    fetches: Arc<AtomicUsize>,
}

#[async_trait]
impl PilotProducer for CountingProducer {
    fn intercepts_claim(&self) -> bool {
        true
    }

    async fn claim(
        &self,
        context: ProducerClaimContext<'_>,
        next: ProducerClaimNext<'_>,
    ) -> Result<Vec<ClaimedJob>, PilotError> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let mut transaction = context.database.begin().await?;
        let jobs = next.claim(transaction.connection()).await?;
        transaction.commit().await?;
        Ok(jobs)
    }
}

struct TestMaintenance {
    starts: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
}

#[async_trait]
impl MaintenanceService for TestMaintenance {
    async fn run(&self, context: MaintenanceServiceContext) -> Result<(), PilotError> {
        let cancellation = context.term.token;
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
    async fn run(&self, context: RuntimeServiceContext) -> Result<(), PilotError> {
        let cancellation = context.cancellation;
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
            .metadata_set("transactional_completion", true)
            .unwrap();
        let mut transaction = self.pool.begin().await?;
        let completed = context.job_complete_tx(&mut transaction).await?;
        assert_eq!(completed.state, JobState::Completed);
        assert_eq!(
            completed
                .metadata
                .get::<bool>("transactional_completion")
                .unwrap(),
            Some(true)
        );
        transaction.commit().await?;
        Ok(WorkOutcome::Complete)
    }
}

#[tokio::test]
async fn client_cancels_a_running_job() {
    let database = support::PostgresSchema::current("rs_cancel_job").await;
    let client = worker_client(&database.pool, ResumableWorker::default());
    let mut run_handle = client.start().unwrap();
    // Remote cancellation arrives by notification, so the listener must be
    // subscribed before the job is cancelled.
    run_handle.wait_ready().await.unwrap();

    let cancelling = client.insert(CancelArgs {}).await.unwrap();
    wait_for_state(&client, cancelling.job.row.id, JobState::Running).await;
    client.jobs().cancel(cancelling.job.row.id).await.unwrap();
    let cancelled = wait_for_state(&client, cancelling.job.row.id, JobState::Cancelled).await;
    assert!(cancelled.finalized_at.is_some());
    assert!(cancelled.metadata.contains_key("cancel_attempted_at"));

    run_handle.shutdown().await.unwrap();
    database.cleanup().await;
}

#[tokio::test]
async fn client_completes_a_job_with_output_and_event() {
    let database = support::PostgresSchema::current("rs_complete_job").await;
    let client = worker_client(&database.pool, ResumableWorker::default());

    let inserted = client
        .insert(EchoArgs {
            message: "from Rust".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(inserted.job.row.state, JobState::Available);

    let mut completed_events = client.subscribe(&[EventKind::JobCompleted]).unwrap();
    let mut run_handle = client.start().unwrap();
    let row = wait_for_state(&client, inserted.job.row.id, JobState::Completed).await;
    assert_eq!(row.attempt, 1);
    assert_eq!(row.attempted_by, ["rust-conformance-client"]);
    assert_eq!(
        row.decode_output::<serde_json::Value>().unwrap(),
        Some(serde_json::json!({"message": "from Rust"}))
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

    run_handle.shutdown().await.unwrap();
    database.cleanup().await;
}

#[tokio::test]
async fn client_discards_a_failing_job_after_max_attempts() {
    let database = support::PostgresSchema::current("rs_discard_fail").await;
    let client = worker_client(&database.pool, ResumableWorker::default());
    let mut run_handle = client.start().unwrap();

    let failed = client
        .insert(FailArgs {})
        .opts(InsertOpts::default().with_max_attempts(1))
        .await
        .unwrap();
    let failed = wait_for_state(&client, failed.job.row.id, JobState::Discarded).await;
    assert_eq!(failed.errors.len(), 1);
    assert_eq!(failed.errors[0].error, "intentional failure");

    run_handle.shutdown().await.unwrap();
    database.cleanup().await;
}

#[tokio::test]
async fn client_discards_a_job_of_an_unregistered_kind() {
    let database = support::PostgresSchema::current("rs_unknown_kind").await;
    let client = worker_client(&database.pool, ResumableWorker::default());
    let mut run_handle = client.start().unwrap();

    let unknown_kind_id: i64 = sqlx::query_scalar(
        "INSERT INTO river_job (args, kind, max_attempts) \
         VALUES ('{}'::jsonb, 'rust_unregistered_kind', 1) RETURNING id",
    )
    .fetch_one(&database.pool)
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
    database.cleanup().await;
}

#[tokio::test]
async fn client_restarts_after_shutdown() {
    let database = support::PostgresSchema::current("rs_restart").await;
    let client = worker_client(&database.pool, ResumableWorker::default());

    let mut run_handle = client.start().unwrap();
    run_handle.wait_ready().await.unwrap();
    run_handle.shutdown().await.unwrap();
    let mut restarted_handle = client.start().unwrap();
    restarted_handle.wait_ready().await.unwrap();
    restarted_handle.shutdown().await.unwrap();

    database.cleanup().await;
}

#[tokio::test]
async fn client_resumes_resumable_steps_on_retry() {
    let database = support::PostgresSchema::current("rs_resumable").await;
    let resumable_worker = ResumableWorker::default();
    let resumable_first_runs = Arc::clone(&resumable_worker.first_runs);
    let resumable_second_runs = Arc::clone(&resumable_worker.second_runs);
    let client = worker_client(&database.pool, resumable_worker);
    let mut run_handle = client.start().unwrap();

    let resumable = client
        .insert(ResumableArgs {})
        .opts(InsertOpts::default().with_max_attempts(2))
        .await
        .unwrap();
    let resumable = wait_for_state(&client, resumable.job.row.id, JobState::Completed).await;
    assert_eq!(
        resumable
            .metadata
            .get::<String>("river:resumable_step")
            .unwrap()
            .as_deref(),
        Some("first")
    );
    assert_eq!(resumable_first_runs.load(Ordering::SeqCst), 1);
    assert_eq!(resumable_second_runs.load(Ordering::SeqCst), 2);

    run_handle.shutdown().await.unwrap();
    database.cleanup().await;
}

#[tokio::test]
async fn complete_tx_requires_a_running_job_and_rolls_back() {
    let database = support::PostgresSchema::current("rs_complete_tx").await;
    let pool = database.pool.clone();
    let client = worker_client(&pool, ResumableWorker::default());

    let non_running = client
        .insert(EchoArgs {
            message: "not running".to_owned(),
        })
        .await
        .unwrap();
    let mut transaction = pool.begin().await.unwrap();
    let error = client
        .jobs()
        .complete(non_running.job.row.id)
        .tx(&mut transaction)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            riverqueue::Error::JobNotRunning {
                state: JobState::Available
            }
        ),
        "{error}"
    );
    assert!(matches!(
        client.jobs().complete(i64::MAX).tx(&mut transaction).await,
        Err(riverqueue::Error::NotFound(riverqueue::Record::Job(
            i64::MAX
        )))
    ));
    transaction.rollback().await.unwrap();

    sqlx::query("UPDATE river_job SET state = 'running' WHERE id = $1")
        .bind(non_running.job.row.id)
        .execute(&pool)
        .await
        .unwrap();
    let mut transaction = pool.begin().await.unwrap();
    let completed_then_rolled_back = client
        .jobs()
        .complete(non_running.job.row.id)
        .tx(&mut transaction)
        .await
        .unwrap();
    assert_eq!(completed_then_rolled_back.state, JobState::Completed);
    transaction.rollback().await.unwrap();
    assert_eq!(
        client
            .jobs()
            .get(non_running.job.row.id)
            .await
            .unwrap()
            .state,
        JobState::Running
    );

    database.cleanup().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn concurrent_unique_inserts_return_the_conflicting_job() {
    const INSERT_COUNT: usize = 32;

    let database = support::PostgresSchema::new("rs_unique_conc").await;
    let pool = database.pool.clone();
    let schema = database.schema.clone();
    let client = Client::builder(PostgresDatabase::new(pool.clone()).with_schema(schema))
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
            InsertOpts::default().with_unique(UniqueOpts::new().with_by_args(true)),
        ),
        (
            "by_args_and_queue",
            InsertOpts::default()
                .with_queue("unique_queue")
                .with_unique(UniqueOpts::new().with_by_args(true).with_by_queue(true)),
        ),
        (
            "by_args_and_states",
            InsertOpts::default().with_unique(
                UniqueOpts::new()
                    .with_by_args(true)
                    .with_by_state(all_states),
            ),
        ),
        (
            "by_args_and_period",
            InsertOpts::default()
                .with_scheduled_at(fixed_scheduled_at)
                .with_unique(
                    UniqueOpts::new()
                        .with_by_args(true)
                        .with_by_period(Duration::from_mins(1)),
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
                client.insert(EchoArgs { message }).opts(opts).await
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

    database.cleanup().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn insert_many_variants_preserve_order_and_transactionality() {
    let database = support::PostgresSchema::new("rs_insert_many").await;
    let pool = database.pool.clone();
    let schema = database.schema.clone();
    let client = Client::builder(PostgresDatabase::new(pool.clone()).with_schema(schema.clone()))
        .build()
        .unwrap();
    let table = schema.qualify("river_job");

    let empty_many = client
        .insert_many(Vec::<EchoArgs>::new())
        .await
        .unwrap_err();
    assert_eq!(empty_many.to_string(), "invalid job: no jobs to insert");
    let empty_batch = client.insert_batch(InsertBatch::new()).await.unwrap_err();
    assert_eq!(empty_batch.to_string(), "invalid job: no jobs to insert");
    let mut empty_transaction = pool.begin().await.unwrap();
    let empty_many_tx = client
        .insert_many(Vec::<EchoArgs>::new())
        .tx(&mut empty_transaction)
        .await
        .unwrap_err();
    assert_eq!(empty_many_tx.to_string(), "invalid job: no jobs to insert");
    let empty_batch_tx = client
        .insert_batch(InsertBatch::new())
        .tx(&mut empty_transaction)
        .await
        .unwrap_err();
    assert_eq!(empty_batch_tx.to_string(), "invalid job: no jobs to insert");
    empty_transaction.commit().await.unwrap();

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
        .push_with(
            CancelArgs {},
            InsertOpts::default().with_queue("heterogeneous-queue"),
        );
    let heterogeneous = client.insert_batch(heterogeneous).await.unwrap();
    assert_eq!(heterogeneous.len(), 2);
    assert_eq!(heterogeneous[0].job.kind, EchoArgs::KIND);
    assert_eq!(heterogeneous[1].job.kind, CancelArgs::KIND);
    assert_eq!(heterogeneous[1].job.queue, "heterogeneous-queue");

    let time_without_states = client
        .jobs()
        .list(
            JobListParams::default()
                .ids(ordered.iter().map(|result| result.job.row.id))
                .order_by(JobListOrderBy::Time),
        )
        .await
        .unwrap()
        .jobs;
    assert_eq!(
        time_without_states
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        [
            ordered[2].job.row.id,
            ordered[0].job.row.id,
            ordered[1].job.row.id,
        ]
    );
    let finalized_without_states = client
        .jobs()
        .list(JobListParams::default().order_by(JobListOrderBy::FinalizedAt))
        .await;
    assert!(matches!(
        finalized_without_states,
        Err(riverqueue::Error::InvalidJob(_))
    ));

    let unique_opts = InsertOpts::default().with_unique(UniqueOpts::new().with_by_args(true));
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
        .insert_many(["tx-rollback-one", "tx-rollback-two"].map(|message| {
            (
                EchoArgs {
                    message: message.to_owned(),
                },
                InsertOpts::default().with_tags(["tx-rollback"]),
            )
        }))
        .tx(&mut transaction)
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
        .insert_many(["tx-commit-one", "tx-commit-two"].map(|message| {
            (
                EchoArgs {
                    message: message.to_owned(),
                },
                InsertOpts::default().with_tags(["tx-commit"]),
            )
        }))
        .tx(&mut transaction)
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
        .insert(EchoArgs {
            message: "ordinary-savepoint-control".to_owned(),
        })
        .tx(&mut transaction)
        .await
        .unwrap();
    let ordinary_savepoint = client
        .insert_many([
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
        ])
        .tx(&mut transaction)
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

    database.cleanup().await;
}

#[tokio::test]
async fn insert_unique_by_args_returns_the_existing_job() {
    let database = support::PostgresSchema::current("rs_unique_insert").await;
    let client = worker_client(&database.pool, ResumableWorker::default());

    let unique_options = InsertOpts::default().with_unique(UniqueOpts::new().with_by_args(true));
    let unique_first = client
        .insert(EchoArgs {
            message: "unique".to_owned(),
        })
        .opts(unique_options.clone())
        .await
        .unwrap();
    let unique_second = client
        .insert(EchoArgs {
            message: "unique".to_owned(),
        })
        .opts(unique_options)
        .await
        .unwrap();
    assert_eq!(unique_first.job.row.id, unique_second.job.row.id);
    assert!(unique_second.unique_skipped_as_duplicate);

    database.cleanup().await;
}

#[tokio::test]
async fn job_admin_lists_updates_retries_and_deletes() {
    let database = support::PostgresSchema::current("rs_job_admin").await;
    let client = worker_client(&database.pool, ResumableWorker::default());
    let mut run_handle = client.start().unwrap();

    let inserted = client
        .insert(EchoArgs {
            message: "from Rust".to_owned(),
        })
        .await
        .unwrap();
    let failed = client
        .insert(FailArgs {})
        .opts(InsertOpts::default().with_max_attempts(1))
        .await
        .unwrap();
    wait_for_state(&client, inserted.job.row.id, JobState::Completed).await;
    let failed = wait_for_state(&client, failed.job.row.id, JobState::Discarded).await;
    run_handle.shutdown().await.unwrap();

    let listed = client
        .jobs()
        .list(JobListParams::default().kinds([EchoArgs::KIND]))
        .await
        .unwrap()
        .jobs;
    assert!(listed.iter().any(|row| row.id == inserted.job.row.id));
    let updated = client
        .jobs()
        .update(
            inserted.job.row.id,
            JobUpdateParams::default().output(serde_json::json!({"ok": true})),
        )
        .await
        .unwrap();
    assert_eq!(
        updated.decode_output::<serde_json::Value>().unwrap(),
        Some(serde_json::json!({"ok": true}))
    );

    let retried = client.jobs().retry(failed.id).await.unwrap();
    assert_eq!(retried.state, JobState::Available);
    assert_eq!(retried.max_attempts, 2);
    let deleted = client.jobs().delete(retried.id).await.unwrap();
    assert_eq!(deleted.id, retried.id);
    assert!(matches!(
        client.jobs().get(retried.id).await,
        Err(riverqueue::Error::NotFound(_))
    ));

    database.cleanup().await;
}

#[tokio::test]
async fn local_queue_added_at_runtime_works_jobs() {
    let database = support::PostgresSchema::current("rs_dynamic_queue").await;
    let client = worker_client(&database.pool, ResumableWorker::default());
    let mut run_handle = client.start().unwrap();
    run_handle.wait_ready().await.unwrap();

    client
        .local_queues()
        .add(
            "dynamic",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .unwrap();
    let dynamic = client
        .insert(EchoArgs {
            message: "dynamic queue".to_owned(),
        })
        .opts(InsertOpts::default().with_queue("dynamic"))
        .await
        .unwrap();
    wait_for_state(&client, dynamic.job.row.id, JobState::Completed).await;
    client.local_queues().remove("dynamic").await.unwrap();
    run_handle.shutdown().await.unwrap();

    // The removed queue's row stays behind alongside the configured one.
    let queues = client
        .queues()
        .list(QueueListParams::default())
        .await
        .unwrap();
    assert_eq!(queues.len(), 2);
    assert!(queues.iter().any(|queue| queue.name == "dynamic"));

    database.cleanup().await;
}

#[tokio::test]
async fn maintenance_cleans_old_jobs_and_queues_and_reindexes() {
    let database = support::PostgresSchema::current("rs_cleanup").await;
    let pool = database.pool.clone();

    sqlx::raw_sql("CREATE INDEX rust_maintenance_reindex_idx ON river_job (id)")
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
        PostgresDatabase::new(pool.clone()).with_reindex(
            PostgresReindexConfig::default()
                .with_index_names(["rust_maintenance_reindex_idx"])
                .with_schedule(PostgresReindexSchedule::Interval(Duration::from_millis(50))),
        ),
    )
    .id("rust-cleanup-client")
    .maintenance(
        MaintenanceConfig::default()
            .with_cancelled_job_retention(riverqueue::Retention::DeleteAfter(
                Duration::from_millis(1),
            ))
            .with_completed_job_retention(riverqueue::Retention::DeleteAfter(
                Duration::from_millis(1),
            ))
            .with_discarded_job_retention(riverqueue::Retention::DeleteAfter(
                Duration::from_millis(1),
            ))
            .with_elect_interval(Duration::from_millis(20))
            .with_job_cleaner_interval(Duration::from_millis(20))
            .with_queue_cleaner_interval(Duration::from_millis(20))
            .with_queue_retention(Duration::from_millis(1)),
    )
    .workers(cleanup_workers)
    .queue("cleanup_active", QueueConfig::new(1))
    .build()
    .unwrap();
    let mut cleanup_handle = cleanup_client.start().unwrap();
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

    database.cleanup().await;
}

#[tokio::test]
async fn maintenance_client_runs_pilot_periodic_scheduled_and_transactional_jobs() {
    let database = support::PostgresSchema::current("rs_pilot").await;
    let pilot = TestPilot::default();
    let maintenance_client = maintenance_client(&database.pool, pilot.clone());

    let scheduled = maintenance_client
        .insert(EchoArgs {
            message: "scheduled by leader".to_owned(),
        })
        .opts(
            InsertOpts::default()
                .with_scheduled_at(chrono::Utc::now() + chrono::Duration::milliseconds(100)),
        )
        .await
        .unwrap();
    let transactional = maintenance_client
        .insert(TransactionalArgs {})
        .await
        .unwrap();
    let mut maintenance_handle = maintenance_client.start().unwrap();
    let periodic = wait_for_job_matching(&maintenance_client, |row| {
        row.metadata
            .get::<String>("river:periodic_job_id")
            .ok()
            .flatten()
            .as_deref()
            == Some("rust-periodic")
    })
    .await;
    assert_eq!(
        periodic.metadata.get::<bool>("periodic").unwrap(),
        Some(true)
    );
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
    assert_eq!(
        transactional
            .metadata
            .get::<bool>("transactional_completion")
            .unwrap(),
        Some(true)
    );
    assert_eq!(
        transactional
            .metadata
            .get::<bool>("extension_handled")
            .unwrap(),
        Some(true)
    );
    assert!(pilot.fetches.load(Ordering::SeqCst) > 0);
    assert!(pilot.completions.load(Ordering::SeqCst) > 0);
    assert_eq!(
        maintenance_client
            .jobs()
            .get(scheduled.job.row.id)
            .await
            .unwrap()
            .metadata
            .get::<bool>("extension_handled")
            .unwrap(),
        Some(true)
    );
    assert_eq!(pilot.maintenance_starts.load(Ordering::SeqCst), 1);
    assert_eq!(pilot.runtime_starts.load(Ordering::SeqCst), 1);
    maintenance_handle.shutdown().await.unwrap();
    assert_eq!(pilot.maintenance_stops.load(Ordering::SeqCst), 1);
    assert_eq!(pilot.runtime_stops.load(Ordering::SeqCst), 1);

    database.cleanup().await;
}

#[tokio::test]
async fn maintenance_leader_rescues_stuck_jobs_and_resigns_on_shutdown() {
    let database = support::PostgresSchema::current("rs_rescue_leader").await;
    let pool = database.pool.clone();
    let maintenance_client = maintenance_client(&pool, TestPilot::default());

    let stuck_id: i64 = sqlx::query_scalar(
        "INSERT INTO river_job (args, attempt, attempted_at, attempted_by, kind, max_attempts, state) \
         VALUES ('{}'::jsonb, 1, now() - interval '2 hours', ARRAY['dead-client'], \
                 'unregistered_stuck_kind', 2, 'running') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut maintenance_handle = maintenance_client.start().unwrap();
    let rescued = wait_for_state(&maintenance_client, stuck_id, JobState::Discarded).await;
    assert_eq!(
        rescued.metadata.get::<i64>("river:rescue_count").unwrap(),
        Some(1)
    );
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
    let leader_count: i64 = sqlx::query_scalar("SELECT count(*) FROM river_leader")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(leader_count, 0);

    database.cleanup().await;
}

#[tokio::test]
async fn migrator_migrates_the_current_schema() {
    // The pool's `search_path` points at a fresh schema, so the default
    // migrator targets the connection's current schema without touching
    // `public`.
    let database = support::PostgresSchema::current_unmigrated("rs_migrate_current").await;

    let migrator = PostgresMigrator::new(database.pool.clone());
    migrator.migrate_up().await.unwrap();
    assert_eq!(
        migrator.existing_versions().await.unwrap(),
        (1..=MIGRATION_VERSION_LATEST).collect::<Vec<_>>()
    );

    database.cleanup().await;
}

#[tokio::test]
async fn migrator_steps_a_custom_schema_up_and_down() {
    let database = support::PostgresSchema::unmigrated("rs_migrate_custom").await;

    let custom_migrator =
        PostgresMigrator::new(database.pool.clone()).with_schema(database.schema.clone());
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
    assert!(!custom_migrator.validate(None).await.unwrap().is_valid());
    custom_migrator.migrate_up().await.unwrap();
    assert!(custom_migrator.validate(None).await.unwrap().is_valid());
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

    database.cleanup().await;
}

#[tokio::test]
async fn queue_admin_gets_pauses_resumes_and_updates() {
    let database = support::PostgresSchema::current("rs_queue_admin").await;
    let client = worker_client(&database.pool, ResumableWorker::default());
    // Starting the client records its configured queue.
    let mut run_handle = client.start().unwrap();
    run_handle.wait_ready().await.unwrap();
    run_handle.shutdown().await.unwrap();

    let queue = client.queues().get("default").await.unwrap();
    assert_eq!(queue.name, "default");
    assert!(queue.paused_at.is_none());
    client.queues().pause("default").await.unwrap();
    assert!(
        client
            .queues()
            .get("default")
            .await
            .unwrap()
            .paused_at
            .is_some()
    );
    client.queues().resume("default").await.unwrap();
    assert!(
        client
            .queues()
            .get("default")
            .await
            .unwrap()
            .paused_at
            .is_none()
    );
    let queue = client
        .queues()
        .update(
            "default",
            riverqueue::QueueUpdateParams::new().metadata(serde_json::Map::from_iter([(
                "owner".to_owned(),
                serde_json::json!("rust"),
            )])),
        )
        .await
        .unwrap();
    assert_eq!(queue.metadata["owner"], "rust");

    database.cleanup().await;
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end rescuer scenario compares all worker timeout and retry overrides"
)]
async fn rescuer_honors_worker_timeout_and_retry_overrides() {
    let database = support::PostgresSchema::new("rs_rescue_timeout").await;
    let pool = database.pool.clone();
    let schema = database.schema.clone();

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
            .with_schema(schema)
            .with_reindex(PostgresReindexConfig::default().with_index_names([] as [&str; 0])),
    )
    .id("rust-rescuer-timeout-client")
    .job_timeout(Duration::from_millis(100))
    .maintenance(
        MaintenanceConfig::default()
            .with_elect_interval(Duration::from_millis(20))
            .with_rescue_after(Duration::from_millis(100))
            .with_rescuer_interval(Duration::from_millis(20)),
    )
    .queue("default", QueueConfig::new(1))
    .workers(workers)
    .build()
    .unwrap();
    let mut handle = client.start().unwrap();

    let default_timeout = wait_for_state(&client, default_timeout_id, JobState::Discarded).await;
    assert_eq!(
        default_timeout
            .metadata
            .get::<i64>("river:rescue_count")
            .unwrap(),
        Some(1)
    );
    assert_eq!(default_timeout.errors.len(), 1);
    let retry_override = wait_for_state(&client, retry_override_id, JobState::Retryable).await;
    assert_eq!(
        retry_override
            .metadata
            .get::<i64>("river:rescue_count")
            .unwrap(),
        Some(1)
    );
    assert_eq!(retry_override.errors.len(), 1);
    assert!(
        retry_override.scheduled_at > chrono::Utc::now() + chrono::Duration::minutes(90),
        "worker retry override was not applied: {:?}",
        retry_override.scheduled_at
    );

    // Every job was stuck from the start, so the rescuer pass that rescued
    // the job with the highest ID also looked at these two and left them
    // running.
    for id in [disabled_timeout_id, long_timeout_id] {
        let row = client.jobs().get(id).await.unwrap();
        assert_eq!(row.state, JobState::Running);
        assert!(row.errors.is_empty());
        assert!(!row.metadata.contains_key("river:rescue_count"));
    }

    handle.shutdown().await.unwrap();
    database.cleanup().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn resumable_cursor_and_transactional_checkpoints() {
    let detached_context = riverqueue::__private::work_context(CancellationToken::new());
    let cursor_error = detached_context
        .resumable_set_cursor(&ResumableCursor { offset: 1 })
        .unwrap_err();
    assert!(
        cursor_error
            .to_string()
            .contains("resumable cursor can only be set inside a resumable cursor step")
    );

    let database = support::PostgresSchema::new("rs_resumable_ckpt").await;
    let pool = database.pool.clone();
    let schema = database.schema.clone();

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
            .with_schema(schema)
            .with_reindex(PostgresReindexConfig::default().with_index_names([] as [&str; 0])),
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
            .insert(ResumableCheckpointArgs {
                mode: mode.to_owned(),
            })
            .opts(InsertOpts::default().with_max_attempts(2))
            .await
            .unwrap();
        job_ids.insert(mode, inserted.job.row.id);
    }
    let mut handle = client.start().unwrap();

    let first_failure = wait_for_state(&client, job_ids["cursor_retry"], JobState::Retryable).await;
    assert_eq!(
        first_failure
            .metadata
            .get::<String>("river:resumable_step")
            .unwrap()
            .as_deref(),
        Some("validate")
    );
    assert_eq!(
        first_failure
            .metadata
            .get::<serde_json::Value>("river:resumable_cursor")
            .unwrap()
            .unwrap()["process"],
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
        committed_cursor
            .metadata
            .get::<String>("river:resumable_step")
            .unwrap()
            .as_deref(),
        Some("tx_cursor")
    );
    assert_eq!(
        committed_cursor
            .metadata
            .get::<serde_json::Value>("river:resumable_cursor")
            .unwrap()
            .unwrap()["tx_cursor"],
        serde_json::json!({"offset": 7})
    );
    let committed_step = wait_for_state(&client, job_ids["commit_step"], JobState::Completed).await;
    assert_eq!(
        committed_step
            .metadata
            .get::<String>("river:resumable_step")
            .unwrap()
            .as_deref(),
        Some("tx_step")
    );
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
    database.cleanup().await;
}

#[tokio::test]
async fn shutdown_now_interrupts_a_job_ignoring_cancellation() {
    let database = support::PostgresSchema::current("rs_interrupt").await;

    let mut interrupt_workers = WorkerRegistry::new();
    interrupt_workers
        .register::<IgnoresCancelArgs, _>(IgnoresCancelWorker)
        .unwrap();
    let interrupt_client = Client::builder(database.pool.clone())
        .id("rust-interrupt-client")
        .job_stuck_threshold(Duration::from_millis(10))
        .workers(interrupt_workers)
        .queue("interrupt", QueueConfig::new(1))
        .build()
        .unwrap();
    let interrupted = interrupt_client
        .insert(IgnoresCancelArgs {})
        .opts(InsertOpts::default().with_queue("interrupt"))
        .await
        .unwrap();
    let mut interrupted_events = interrupt_client
        .subscribe(&[EventKind::JobInterrupted])
        .unwrap();
    let mut interrupt_handle = interrupt_client.start().unwrap();
    wait_for_state(&interrupt_client, interrupted.job.row.id, JobState::Running).await;
    interrupt_handle.shutdown_now().await.unwrap();
    let interrupted_row = interrupt_client
        .jobs()
        .get(interrupted.job.row.id)
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

    database.cleanup().await;
}

#[tokio::test]
async fn transactional_get_and_update_roll_back() {
    let database = support::PostgresSchema::current("rs_tx_update").await;
    let pool = database.pool.clone();
    let client = worker_client(&pool, ResumableWorker::default());
    let inserted = client
        .insert(EchoArgs {
            message: "from Rust".to_owned(),
        })
        .await
        .unwrap();

    let mut transaction = pool.begin().await.unwrap();
    let tx_row = client
        .jobs()
        .get(inserted.job.row.id)
        .tx(&mut transaction)
        .await
        .unwrap();
    assert_eq!(tx_row.id, inserted.job.row.id);
    client
        .jobs()
        .update(
            tx_row.id,
            JobUpdateParams::default().output(serde_json::json!("transactional")),
        )
        .tx(&mut transaction)
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    assert!(
        client
            .jobs()
            .get(inserted.job.row.id)
            .await
            .unwrap()
            .output()
            .is_none()
    );

    database.cleanup().await;
}

#[tokio::test]
async fn transactional_inserts_become_visible_on_commit() {
    let database = support::PostgresSchema::current("rs_tx_insert").await;
    let pool = database.pool.clone();
    let client = worker_client(&pool, ResumableWorker::default());

    let mut transaction = pool.begin().await.unwrap();
    let transaction_insert = client
        .insert(EchoArgs {
            message: "from Rust".to_owned(),
        })
        .tx(&mut transaction)
        .await
        .unwrap();
    let raw_transaction_insert = riverqueue::__private::ExtensionClient::new(&client)
        .insert_raw(
            EchoArgs::KIND,
            &[],
            serde_json::value::to_raw_value(&serde_json::json!({"message": "raw from Rust"}))
                .unwrap(),
            InsertOpts::default(),
        )
        .tx(&mut transaction)
        .await
        .unwrap();
    assert!(matches!(
        client.jobs().get(transaction_insert.job.row.id).await,
        Err(riverqueue::Error::NotFound(_))
    ));
    assert!(matches!(
        client.jobs().get(raw_transaction_insert.job.id).await,
        Err(riverqueue::Error::NotFound(_))
    ));
    transaction.commit().await.unwrap();
    assert_eq!(
        client
            .jobs()
            .get(transaction_insert.job.row.id)
            .await
            .unwrap()
            .state,
        JobState::Available
    );
    assert_eq!(
        client
            .jobs()
            .get(raw_transaction_insert.job.id)
            .await
            .unwrap()
            .decode_args::<serde_json::Value>()
            .unwrap()["message"],
        "raw from Rust"
    );
    let pool_connection = client
        .postgres_pool()
        .expect("client is configured for PostgreSQL")
        .acquire()
        .await
        .unwrap();
    // Return the connection so closing the pool at cleanup doesn't wait on it.
    drop(pool_connection);

    database.cleanup().await;
}

/// Builds the maintenance client shared by the pilot and rescuer tests.
fn maintenance_client(pool: &PgPool, pilot: TestPilot) -> Client {
    let mut maintenance_workers = WorkerRegistry::new();
    maintenance_workers
        .register::<EchoArgs, _>(EchoWorker)
        .unwrap();
    maintenance_workers
        .register::<TransactionalArgs, _>(TransactionalWorker { pool: pool.clone() })
        .unwrap();
    Client::builder(pool.clone())
        .id("rust-maintenance-client")
        .maintenance(
            MaintenanceConfig::default()
                .with_elect_interval(Duration::from_millis(20))
                // Like Go, the rescue age cannot be shorter than the default
                // one-minute job timeout.
                .with_rescue_after(Duration::from_mins(1))
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
                .with_run_on_start(true),
        ))
        .pilot(pilot)
        .workers(maintenance_workers)
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap()
}

async fn wait_for_job_matching(client: &Client, predicate: impl Fn(&JobRow) -> bool) -> JobRow {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let rows = client
            .jobs()
            .list(JobListParams::default().limit(10_000))
            .await
            .unwrap()
            .jobs;
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
        let row = client.jobs().get(id).await.unwrap();
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

/// Builds a client on the pool's current schema that works the basic
/// conformance job kinds.
fn worker_client(pool: &PgPool, resumable: ResumableWorker) -> Client {
    let mut workers = WorkerRegistry::new();
    workers.register::<CancelArgs, _>(CancelWorker).unwrap();
    workers.register::<EchoArgs, _>(EchoWorker).unwrap();
    workers.register::<FailArgs, _>(FailWorker).unwrap();
    workers.register::<ResumableArgs, _>(resumable).unwrap();
    Client::builder(pool.clone())
        .id("rust-conformance-client")
        .workers(workers)
        .queue("default", QueueConfig::new(2))
        .build()
        .unwrap()
}
