#![cfg(feature = "postgres-tests")]

mod support;

use std::{
    collections::HashSet,
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use riverqueue::{
    BoxError, Client, EventKind, EventReceiver, EventRecvError, Extensions, Hook, InsertContext,
    InsertMiddleware, InsertNext, InsertedJobs, Job, JobArgs, JobRow, JobState, Metric,
    PeriodicJobs, Plugin, QueueConfig, SubscribeConfig, WorkContext, WorkError, WorkMiddleware,
    WorkNext, WorkOutcome, Worker, WorkerRegistry, database::PostgresDatabase,
};
use serde::{Deserialize, Serialize};
use sqlx::AssertSqlSafe;
use tokio::sync::Semaphore;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_runtime_config")]
struct RuntimeArgs {}

struct RuntimeWorker;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_runtime_burst")]
struct BurstArgs {}

struct BurstWorker;

impl Worker<BurstArgs> for BurstWorker {
    type Error = Infallible;

    fn work(
        &self,
        _context: WorkContext,
        _job: Job<BurstArgs>,
    ) -> impl std::future::Future<Output = Result<WorkOutcome, Self::Error>> + Send {
        std::future::ready(Ok(WorkOutcome::Complete))
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_runtime_cancel_snooze")]
struct CancelSnoozeArgs {}

struct CancelSnoozeWorker {
    started: Arc<Semaphore>,
}

impl Worker<CancelSnoozeArgs> for CancelSnoozeWorker {
    type Error = Infallible;

    async fn work(
        &self,
        context: WorkContext,
        _job: Job<CancelSnoozeArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        self.started.add_permits(1);
        context.cancellation_token().cancelled().await;
        Ok(WorkOutcome::Snooze(Duration::from_hours(1)))
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_runtime_terminal_race")]
struct TerminalRaceArgs {}

struct TerminalRaceWorker {
    finish: Arc<Semaphore>,
    started: Arc<Semaphore>,
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_runtime_shutdown")]
struct ShutdownArgs {
    ignore_cancellation: bool,
}

struct ShutdownWorker {
    finish: Arc<Semaphore>,
    started: Arc<Semaphore>,
}

impl Worker<ShutdownArgs> for ShutdownWorker {
    type Error = Infallible;

    async fn work(
        &self,
        _context: WorkContext,
        job: Job<ShutdownArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        self.started.add_permits(1);
        if job.args.ignore_cancellation {
            std::future::pending::<()>().await;
        } else {
            self.finish.acquire().await.unwrap().forget();
        }
        Ok(WorkOutcome::Complete)
    }
}

impl Worker<TerminalRaceArgs> for TerminalRaceWorker {
    type Error = Infallible;

    async fn work(
        &self,
        context: WorkContext,
        _job: Job<TerminalRaceArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        self.started.add_permits(1);
        self.finish.acquire().await.unwrap().forget();
        context.metadata_set("worker_completion", true).unwrap();
        Ok(WorkOutcome::Complete)
    }
}

#[derive(Clone)]
struct RuntimeHook {
    counts: Arc<RuntimeCounts>,
}

#[derive(Default)]
struct RuntimeCounts {
    insert_after: AtomicUsize,
    insert_before: AtomicUsize,
    metrics: AtomicUsize,
    periodic_starts: AtomicUsize,
    work_after: AtomicUsize,
    work_before: AtomicUsize,
}

#[allow(
    clippy::unused_async_trait_impl,
    reason = "these extensions only record state synchronously"
)]
impl Hook for RuntimeHook {
    async fn insert_begin(&self, _insert: &mut InsertContext) -> Result<(), BoxError> {
        self.counts.insert_before.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn metric_emit(&self, _metric: Metric) -> Result<(), BoxError> {
        self.counts.metrics.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn periodic_jobs_start(&self, _jobs: &PeriodicJobs) -> Result<(), BoxError> {
        self.counts.periodic_starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn work_begin(&self, _context: &WorkContext, job: &mut JobRow) -> Result<(), BoxError> {
        self.counts.work_before.fetch_add(1, Ordering::SeqCst);
        let mut args: serde_json::Value = job.decode_args()?;
        args["hook_decrypted"] = true.into();
        job.encoded_args = riverqueue::encoding::encode_args(&args)?;
        Ok(())
    }

    async fn work_end(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        result: Result<WorkOutcome, WorkError>,
    ) -> Result<WorkOutcome, WorkError> {
        self.counts.work_after.fetch_add(1, Ordering::SeqCst);
        result
    }
}

#[derive(Clone)]
struct RuntimeInsertMiddleware(Arc<RuntimeCounts>);

impl InsertMiddleware for RuntimeInsertMiddleware {
    async fn insert_many(
        &self,
        mut jobs: Vec<InsertContext>,
        next: InsertNext<'_>,
    ) -> Result<InsertedJobs, riverqueue::Error> {
        for job in &mut jobs {
            job.opts
                .metadata
                .insert("middleware", true)
                .expect("boolean metadata serializes");
        }
        let inserted = next.run(jobs).await?;
        let InsertedJobs::Rows(rows) = &inserted else {
            unreachable!("River returns rows")
        };
        self.0.insert_after.fetch_add(rows.len(), Ordering::SeqCst);
        Ok(inserted)
    }
}

struct RuntimePlugin {
    counts: Arc<RuntimeCounts>,
}

impl Plugin for RuntimePlugin {
    fn install(&self, extensions: &mut Extensions) {
        extensions
            .hook(RuntimeHook {
                counts: Arc::clone(&self.counts),
            })
            .insert_middleware(RuntimeInsertMiddleware(Arc::clone(&self.counts)))
            .work_middleware(RuntimeWorkMiddleware(Arc::clone(&self.counts)));
    }
}

#[derive(Clone)]
struct RuntimeWorkMiddleware(Arc<RuntimeCounts>);

impl WorkMiddleware for RuntimeWorkMiddleware {
    async fn work(
        &self,
        _context: &WorkContext,
        job: JobRow,
        next: WorkNext<'_>,
    ) -> Result<WorkOutcome, WorkError> {
        // Like River Go, work hooks run inside middleware, so the hook
        // hasn't transformed the arguments yet.
        let args = job
            .decode_args::<serde_json::Value>()
            .map_err(WorkError::new)?;
        assert!(args.get("hook_decrypted").is_none());
        self.0.work_before.fetch_add(1, Ordering::SeqCst);
        let result = next.run(job).await;
        self.0.work_after.fetch_add(1, Ordering::SeqCst);
        result
    }
}

impl Worker<RuntimeArgs> for RuntimeWorker {
    type Error = Infallible;

    async fn work(
        &self,
        _context: WorkContext,
        job: Job<RuntimeArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        assert_eq!(
            job.row.decode_args::<serde_json::Value>().unwrap()["hook_decrypted"],
            true
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok(WorkOutcome::Complete)
    }
}

async fn setup_runtime() -> (Client, Arc<RuntimeCounts>, support::PostgresSchema) {
    let database = support::PostgresSchema::new("rt_config").await;
    let pool = database.pool.clone();
    let schema = database.schema.clone();

    let mut workers = WorkerRegistry::new();
    workers.register::<RuntimeArgs, _>(RuntimeWorker).unwrap();
    let counts = Arc::new(RuntimeCounts::default());
    let client = Client::builder(PostgresDatabase::new(pool).schema(schema))
        .default_max_attempts(7)
        .plugin(RuntimePlugin {
            counts: Arc::clone(&counts),
        })
        .id("rust-runtime-config-test")
        .without_notifications()
        .workers(workers)
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    (client, counts, database)
}

#[tokio::test]
async fn completion_burst_does_not_lag_large_subscription() {
    const JOB_COUNT: usize = 6_000;

    let database = support::PostgresSchema::new("rt_burst").await;
    let pool = database.pool.clone();
    let schema = database.schema.clone();

    let mut workers = WorkerRegistry::new();
    workers.register::<BurstArgs, _>(BurstWorker).unwrap();
    let client = Client::builder(PostgresDatabase::new(pool.clone()).schema(schema.clone()))
        .id("rust-runtime-burst-test")
        .without_notifications()
        .workers(workers)
        .queue(
            "default",
            QueueConfig::new(1_000)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    let mut completed = client
        .subscribe_config(
            SubscribeConfig::new([EventKind::JobCompleted])
                .unwrap()
                .with_buffer_capacity(std::num::NonZeroUsize::new(JOB_COUNT).unwrap()),
        )
        .unwrap();
    let jobs = (0..JOB_COUNT).map(|_| (BurstArgs {}, riverqueue::InsertOpts::default()));
    assert_eq!(client.insert_many(jobs).await.unwrap().len(), JOB_COUNT);
    let expected_ids = sqlx::query_scalar::<_, i64>(AssertSqlSafe(format!(
        "SELECT id FROM {}",
        schema.qualify("river_job")
    )))
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .collect::<HashSet<_>>();
    assert_eq!(expected_ids.len(), JOB_COUNT);

    let mut run_handle = client.start().unwrap();
    run_handle.wait_ready().await.unwrap();
    let received_ids = tokio::time::timeout(Duration::from_secs(10), async {
        let mut received_ids = HashSet::with_capacity(JOB_COUNT);
        for _ in 0..JOB_COUNT {
            let event = completed.recv().await.unwrap();
            assert_eq!(event.kind(), EventKind::JobCompleted);
            let id = event.as_job().expect("completion event has a job").job.id;
            assert!(
                received_ids.insert(id),
                "duplicate completion event for job {id}"
            );
        }
        received_ids
    })
    .await
    .unwrap();
    assert_eq!(received_ids, expected_ids);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), completed.recv())
            .await
            .is_err(),
        "unexpected extra completion event"
    );
    run_handle.shutdown().await.unwrap();

    let completed_count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {} WHERE state = 'completed'",
        schema.qualify("river_job")
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(completed_count, i64::try_from(JOB_COUNT).unwrap());
    database.cleanup().await;
}

#[tokio::test]
async fn external_terminal_state_wins_worker_completion_race() {
    let database = support::PostgresSchema::new("rt_terminal_race").await;
    let pool = database.pool.clone();
    let schema = database.schema.clone();

    let finish = Arc::new(Semaphore::new(0));
    let started = Arc::new(Semaphore::new(0));
    let mut workers = WorkerRegistry::new();
    workers
        .register::<TerminalRaceArgs, _>(TerminalRaceWorker {
            finish: Arc::clone(&finish),
            started: Arc::clone(&started),
        })
        .unwrap();
    let client = Client::builder(PostgresDatabase::new(pool.clone()).schema(schema.clone()))
        .id("rust-runtime-terminal-race-test")
        .without_notifications()
        .workers(workers)
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    let mut terminal_events = client
        .subscribe(&[
            EventKind::JobCancelled,
            EventKind::JobCompleted,
            EventKind::JobFailed,
        ])
        .unwrap();
    let mut run_handle = client.start().unwrap();
    let table = schema.qualify("river_job");
    let state_type = schema.qualify("river_job_state");

    for external_state in [
        JobState::Cancelled,
        JobState::Completed,
        JobState::Discarded,
    ] {
        let inserted = client.insert(TerminalRaceArgs {}).await.unwrap();
        started.acquire().await.unwrap().forget();
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {table} SET finalized_at = now(), \
                metadata = metadata || '{{\"external_terminal\":true}}'::jsonb, \
                state = $2::text::{state_type} \
             WHERE id = $1 AND state = 'running'"
        )))
        .bind(inserted.job.row.id)
        .bind(external_state.as_str())
        .execute(&pool)
        .await
        .unwrap();
        finish.add_permits(1);
        let event = tokio::time::timeout(Duration::from_secs(5), terminal_events.recv())
            .await
            .unwrap()
            .unwrap();
        let expected_event = match external_state {
            JobState::Cancelled => EventKind::JobCancelled,
            JobState::Completed => EventKind::JobCompleted,
            JobState::Discarded => EventKind::JobFailed,
            _ => unreachable!("test uses terminal external states"),
        };
        assert_eq!(event.kind(), expected_event);
        let event = event.as_job().unwrap();
        assert_eq!(event.job.id, inserted.job.row.id);
        assert_eq!(event.job.state, external_state);
        assert_eq!(
            event.job.metadata.get::<bool>("worker_completion").unwrap(),
            Some(true)
        );

        let row = client.jobs().get(inserted.job.row.id).await.unwrap();
        assert_eq!(row.state, external_state);
        assert_eq!(
            row.metadata.get::<bool>("external_terminal").unwrap(),
            Some(true)
        );
        assert_eq!(
            row.metadata.get::<bool>("worker_completion").unwrap(),
            Some(true)
        );
    }

    run_handle.shutdown().await.unwrap();
    database.cleanup().await;
}

#[tokio::test]
async fn remote_cancellation_overrides_worker_snooze() {
    let database = support::PostgresSchema::new("rt_cancel_snooze").await;
    let pool = database.pool.clone();
    let schema = database.schema.clone();

    let started = Arc::new(Semaphore::new(0));
    let mut workers = WorkerRegistry::new();
    workers
        .register::<CancelSnoozeArgs, _>(CancelSnoozeWorker {
            started: Arc::clone(&started),
        })
        .unwrap();
    let client = Client::builder(PostgresDatabase::new(pool.clone()).schema(schema.clone()))
        .id("rust-runtime-cancel-snooze-test")
        .workers(workers)
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    let mut cancelled_events = client.subscribe(&[EventKind::JobCancelled]).unwrap();
    let mut run_handle = client.start().unwrap();
    run_handle.wait_ready().await.unwrap();
    let inserted = client.insert(CancelSnoozeArgs {}).await.unwrap();
    started.acquire().await.unwrap().forget();
    client.jobs().cancel(inserted.job.row.id).await.unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), cancelled_events.recv())
        .await
        .unwrap()
        .unwrap();
    let row = &event.as_job().expect("cancellation event has a job").job;
    assert_eq!(row.id, inserted.job.row.id);
    assert_eq!(row.state, JobState::Cancelled);
    assert_eq!(
        row.errors.last().unwrap().error,
        "JobCancelError: job cancelled remotely"
    );
    assert_eq!(row.attempt, 1);

    run_handle.shutdown().await.unwrap();
    database.cleanup().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn shutdown_waits_for_active_work_and_soft_stop_escalates() {
    let database = support::PostgresSchema::new("rt_shutdown").await;
    let pool = database.pool.clone();
    let schema = database.schema.clone();

    let graceful_finish = Arc::new(Semaphore::new(0));
    let graceful_started = Arc::new(Semaphore::new(0));
    let mut graceful_workers = WorkerRegistry::new();
    graceful_workers
        .register::<ShutdownArgs, _>(ShutdownWorker {
            finish: Arc::clone(&graceful_finish),
            started: Arc::clone(&graceful_started),
        })
        .unwrap();
    let graceful_client =
        Client::builder(PostgresDatabase::new(pool.clone()).schema(schema.clone()))
            .id("rust-runtime-graceful-shutdown-test")
            .without_notifications()
            .workers(graceful_workers)
            .queue(
                "graceful",
                QueueConfig::new(1)
                    .with_fetch_cooldown(Duration::from_millis(1))
                    .with_fetch_poll_interval(Duration::from_millis(10)),
            )
            .build()
            .unwrap();
    let active = graceful_client
        .insert(ShutdownArgs {
            ignore_cancellation: false,
        })
        .opts(riverqueue::InsertOpts::default().with_queue("graceful"))
        .await
        .unwrap();
    let mut graceful_handle = graceful_client.start().unwrap();
    graceful_handle.wait_ready().await.unwrap();
    graceful_started.acquire().await.unwrap().forget();
    let unfetched = graceful_client
        .insert(ShutdownArgs {
            ignore_cancellation: false,
        })
        .opts(riverqueue::InsertOpts::default().with_queue("graceful"))
        .await
        .unwrap();
    // Request the stop before releasing the worker, then check when the
    // shutdown returns that the worker had taken its release: a shutdown that
    // didn't wait for active work would return with the permit unclaimed.
    graceful_handle.stopper().stop();
    let finish = Arc::clone(&graceful_finish);
    let graceful_shutdown = tokio::spawn(async move {
        graceful_handle.shutdown().await.unwrap();
        finish.available_permits()
    });
    graceful_finish.add_permits(1);
    let unclaimed = tokio::time::timeout(Duration::from_secs(2), graceful_shutdown)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        unclaimed, 0,
        "graceful shutdown returned while barrier work was active"
    );
    assert_eq!(
        graceful_client
            .jobs()
            .get(active.job.row.id)
            .await
            .unwrap()
            .state,
        JobState::Completed
    );
    assert_eq!(
        graceful_client
            .jobs()
            .get(unfetched.job.row.id)
            .await
            .unwrap()
            .state,
        JobState::Available
    );

    let escalation_started = Arc::new(Semaphore::new(0));
    let mut escalation_workers = WorkerRegistry::new();
    escalation_workers
        .register::<ShutdownArgs, _>(ShutdownWorker {
            finish: Arc::new(Semaphore::new(0)),
            started: Arc::clone(&escalation_started),
        })
        .unwrap();
    let escalation_client =
        Client::builder(PostgresDatabase::new(pool.clone()).schema(schema.clone()))
            .id("rust-runtime-soft-stop-escalation-test")
            .job_stuck_threshold(Duration::from_millis(10))
            .without_notifications()
            .soft_stop_timeout(Duration::from_millis(50))
            .workers(escalation_workers)
            .queue(
                "escalation",
                QueueConfig::new(1)
                    .with_fetch_cooldown(Duration::from_millis(1))
                    .with_fetch_poll_interval(Duration::from_millis(10)),
            )
            .build()
            .unwrap();
    let mut interrupted_events = escalation_client
        .subscribe(&[EventKind::JobInterrupted])
        .unwrap();
    let stuck = escalation_client
        .insert(ShutdownArgs {
            ignore_cancellation: true,
        })
        .opts(riverqueue::InsertOpts::default().with_queue("escalation"))
        .await
        .unwrap();
    let mut escalation_handle = escalation_client.start().unwrap();
    escalation_handle.wait_ready().await.unwrap();
    escalation_started.acquire().await.unwrap().forget();
    let shutdown_started = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(2), escalation_handle.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(shutdown_started.elapsed() >= Duration::from_millis(50));
    let interrupted = escalation_client
        .jobs()
        .get(stuck.job.row.id)
        .await
        .unwrap();
    assert_eq!(interrupted.attempt, 0);
    assert_eq!(interrupted.state, JobState::Available);
    assert!(interrupted.errors.is_empty());
    let event = tokio::time::timeout(Duration::from_secs(1), interrupted_events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.as_job().unwrap().job.id, stuck.job.row.id);

    database.cleanup().await;
}

async fn next_queue_event(receiver: &mut EventReceiver) -> EventKind {
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap()
        .kind()
}

#[tokio::test]
async fn poll_only_and_subscription_configuration() {
    let (client, counts, database) = setup_runtime().await;
    let mut completed = client
        .subscribe_config(
            SubscribeConfig::new([EventKind::JobCompleted])
                .unwrap()
                .with_buffer_capacity(std::num::NonZeroUsize::new(4).unwrap()),
        )
        .unwrap();
    let mut run_handle = client.start().unwrap();
    run_handle.wait_ready().await.unwrap();
    assert_eq!(
        client
            .insert_many([
                (
                    RuntimeArgs {},
                    riverqueue::InsertOpts::default().with_pending(true),
                ),
                (
                    RuntimeArgs {},
                    riverqueue::InsertOpts::default().with_pending(true),
                ),
            ])
            .await
            .unwrap()
            .len(),
        2
    );
    let inserted = client.insert(RuntimeArgs {}).await.unwrap();
    assert_eq!(inserted.job.row.max_attempts, 7);
    assert!(
        inserted
            .job
            .row
            .metadata
            .get::<bool>("middleware")
            .unwrap()
            .unwrap()
    );
    let event = tokio::time::timeout(Duration::from_secs(2), completed.recv())
        .await
        .unwrap()
        .unwrap();
    let job_event = event.as_job().unwrap();
    assert_eq!(job_event.job.id, inserted.job.row.id);
    let statistics = job_event.statistics.unwrap();
    assert!(statistics.run_duration >= Duration::from_millis(5));
    assert!(statistics.complete_duration > Duration::ZERO);
    assert!(counts.metrics.load(Ordering::SeqCst) >= 2);
    assert_eq!(counts.periodic_starts.load(Ordering::SeqCst), 1);
    assert_eq!(counts.insert_before.load(Ordering::SeqCst), 3);
    assert_eq!(counts.insert_after.load(Ordering::SeqCst), 3);
    assert_eq!(counts.work_before.load(Ordering::SeqCst), 2);
    assert_eq!(counts.work_after.load(Ordering::SeqCst), 2);

    let mut lagged = client
        .subscribe_config(
            SubscribeConfig::new([EventKind::QueuePaused, EventKind::QueueResumed])
                .unwrap()
                .with_buffer_capacity(std::num::NonZeroUsize::new(1).unwrap()),
        )
        .unwrap();
    let mut transitions = client
        .subscribe(&[EventKind::QueuePaused, EventKind::QueueResumed])
        .unwrap();
    client.queues().pause("default").await.unwrap();
    assert_eq!(
        next_queue_event(&mut transitions).await,
        EventKind::QueuePaused
    );
    client.queues().resume("default").await.unwrap();
    assert_eq!(
        next_queue_event(&mut transitions).await,
        EventKind::QueueResumed
    );
    client.queues().pause("default").await.unwrap();
    assert_eq!(
        next_queue_event(&mut transitions).await,
        EventKind::QueuePaused
    );
    assert!(matches!(
        lagged.recv().await,
        Err(EventRecvError::Lagged(2))
    ));
    assert_eq!(lagged.recv().await.unwrap().kind(), EventKind::QueuePaused);

    run_handle.shutdown().await.unwrap();
    assert_eq!(
        client.jobs().get(inserted.job.row.id).await.unwrap().state,
        JobState::Completed
    );
    database.cleanup().await;
}

#[test]
fn start_without_runtime_returns_error_and_is_restartable() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let database = runtime.block_on(support::PostgresSchema::new("rt_missing"));
    let mut workers = WorkerRegistry::new();
    workers.register::<BurstArgs, _>(BurstWorker).unwrap();
    let client = Client::builder(
        PostgresDatabase::new(database.pool.clone()).schema(database.schema.clone()),
    )
    .queue("default", QueueConfig::new(1))
    .workers(workers)
    .build()
    .unwrap();

    let Err(error) = client.start() else {
        panic!("start should require Tokio");
    };
    assert!(error.to_string().contains("active Tokio runtime"));

    runtime.block_on(async {
        let mut run = client
            .start()
            .expect("failed start must not poison the client");
        run.wait_ready().await.unwrap();
        run.shutdown_now().await.unwrap();
        database.cleanup().await;
    });
    drop(client);
    runtime.shutdown_background();
}
