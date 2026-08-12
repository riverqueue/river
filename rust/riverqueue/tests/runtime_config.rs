#![cfg(feature = "postgres-tests")]

use std::{
    collections::HashSet,
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use riverqueue::{
    Client, EventKind, EventRecvError, Hook, InsertContext, InsertMiddleware, Job, JobArgs, JobRow,
    JobState, Metric, PeriodicJobs, Plugin, QueueConfig, SubscribeConfig, WorkContext,
    WorkMiddleware, WorkOutcome, WorkResult, Worker, WorkerRegistry,
};
use riverqueue_migrate::Migrator;
use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, PgPool};
use tokio::sync::Semaphore;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_runtime_config")]
struct RuntimeArgs {}

struct RuntimeWorker;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_runtime_burst")]
struct BurstArgs {}

struct BurstWorker;

#[async_trait]
impl Worker<BurstArgs> for BurstWorker {
    type Error = Infallible;

    async fn work(
        &self,
        _context: WorkContext,
        _job: Job<BurstArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        Ok(WorkOutcome::Complete)
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_runtime_cancel_snooze")]
struct CancelSnoozeArgs {}

struct CancelSnoozeWorker {
    started: Arc<Semaphore>,
}

#[async_trait]
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

#[async_trait]
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

#[async_trait]
impl Worker<TerminalRaceArgs> for TerminalRaceWorker {
    type Error = Infallible;

    async fn work(
        &self,
        context: WorkContext,
        _job: Job<TerminalRaceArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        self.started.add_permits(1);
        self.finish.acquire().await.unwrap().forget();
        context
            .metadata_set("worker_completion", serde_json::json!(true))
            .await;
        Ok(WorkOutcome::Complete)
    }
}

#[derive(Clone)]
struct RuntimeHook {
    counts: Arc<RuntimeCounts>,
}

#[derive(Default)]
struct RuntimeCounts {
    fast_insert_after: AtomicUsize,
    insert_after: AtomicUsize,
    insert_before: AtomicUsize,
    metrics: AtomicUsize,
    periodic_starts: AtomicUsize,
    work_after: AtomicUsize,
    work_before: AtomicUsize,
}

#[async_trait]
impl Hook for RuntimeHook {
    async fn insert_begin(&self, _insert: &mut InsertContext) -> Result<(), riverqueue::Error> {
        self.counts.insert_before.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn insert_end(
        &self,
        _job: &JobRow,
        _unique_skipped_as_duplicate: bool,
    ) -> Result<(), riverqueue::Error> {
        self.counts.insert_after.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn insert_many_fast_end(&self, inserted_count: u64) -> Result<(), riverqueue::Error> {
        self.counts
            .fast_insert_after
            .fetch_add(usize::try_from(inserted_count).unwrap(), Ordering::SeqCst);
        Ok(())
    }

    async fn metric_emit(&self, _metric: Metric) -> Result<(), riverqueue::Error> {
        self.counts.metrics.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn periodic_jobs_start(&self, _jobs: &PeriodicJobs) -> Result<(), riverqueue::Error> {
        self.counts.periodic_starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn work_begin(
        &self,
        _context: &WorkContext,
        job: &mut JobRow,
    ) -> Result<(), riverqueue::Error> {
        self.counts.work_before.fetch_add(1, Ordering::SeqCst);
        job.encoded_args["hook_decrypted"] = true.into();
        Ok(())
    }

    async fn work_end(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        _result: &WorkResult,
    ) -> Result<(), riverqueue::Error> {
        self.counts.work_after.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Clone)]
struct RuntimeInsertMiddleware;

#[async_trait]
impl InsertMiddleware for RuntimeInsertMiddleware {
    async fn before_insert(&self, insert: &mut InsertContext) -> Result<(), riverqueue::Error> {
        insert
            .opts
            .metadata
            .insert("middleware".to_owned(), true.into());
        Ok(())
    }

    async fn after_insert_many_fast(&self, inserted_count: u64) -> Result<(), riverqueue::Error> {
        assert_eq!(inserted_count, 2);
        Ok(())
    }
}

struct RuntimePlugin {
    counts: Arc<RuntimeCounts>,
}

impl Plugin for RuntimePlugin {
    fn hooks(&self) -> Vec<Arc<dyn Hook>> {
        vec![Arc::new(RuntimeHook {
            counts: Arc::clone(&self.counts),
        })]
    }

    fn insert_middleware(&self) -> Vec<Arc<dyn InsertMiddleware>> {
        vec![Arc::new(RuntimeInsertMiddleware)]
    }

    fn work_middleware(&self) -> Vec<Arc<dyn WorkMiddleware>> {
        vec![Arc::new(RuntimeWorkMiddleware(Arc::clone(&self.counts)))]
    }
}

#[derive(Clone)]
struct RuntimeWorkMiddleware(Arc<RuntimeCounts>);

#[async_trait]
impl WorkMiddleware for RuntimeWorkMiddleware {
    async fn after_work(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        _result: &WorkResult,
    ) -> Result<(), riverqueue::Error> {
        self.0.work_after.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn before_work(
        &self,
        _context: &WorkContext,
        job: &mut JobRow,
    ) -> Result<(), riverqueue::Error> {
        assert_eq!(job.encoded_args["hook_decrypted"], true);
        self.0.work_before.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl Worker<RuntimeArgs> for RuntimeWorker {
    type Error = Infallible;

    async fn work(
        &self,
        _context: WorkContext,
        job: Job<RuntimeArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        assert_eq!(job.row.encoded_args["hook_decrypted"], true);
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok(WorkOutcome::Complete)
    }
}

async fn setup_runtime(database_url: &str) -> (Client, Arc<RuntimeCounts>) {
    let pool = PgPool::connect(database_url).await.unwrap();
    let schema = riverqueue::SchemaName::new("rust_runtime_config_test").unwrap();
    sqlx::raw_sql(AssertSqlSafe(
        "DROP SCHEMA IF EXISTS rust_runtime_config_test CASCADE; \
         CREATE SCHEMA rust_runtime_config_test",
    ))
    .execute(&pool)
    .await
    .unwrap();
    Migrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await
        .unwrap();

    let mut workers = WorkerRegistry::new();
    workers.register::<RuntimeArgs, _>(RuntimeWorker).unwrap();
    let counts = Arc::new(RuntimeCounts::default());
    let client = Client::builder(pool)
        .default_max_attempts(7)
        .plugin(RuntimePlugin {
            counts: Arc::clone(&counts),
        })
        .id("rust-runtime-config-test")
        .poll_only(true)
        .schema(schema)
        .workers(workers)
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
    (client, counts)
}

#[tokio::test]
async fn completion_burst_does_not_lag_large_subscription() {
    const JOB_COUNT: usize = 6_000;

    let Ok(database_url) = std::env::var("RIVER_RUST_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL runtime test without RIVER_RUST_DATABASE_URL");
        return;
    };

    let pool = PgPool::connect(&database_url).await.unwrap();
    let schema = riverqueue::SchemaName::new("rust_runtime_burst_test").unwrap();
    sqlx::raw_sql(AssertSqlSafe(
        "DROP SCHEMA IF EXISTS rust_runtime_burst_test CASCADE; \
         CREATE SCHEMA rust_runtime_burst_test",
    ))
    .execute(&pool)
    .await
    .unwrap();
    Migrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await
        .unwrap();

    let mut workers = WorkerRegistry::new();
    workers.register::<BurstArgs, _>(BurstWorker).unwrap();
    let client = Client::builder(pool.clone())
        .id("rust-runtime-burst-test")
        .poll_only(true)
        .schema(schema.clone())
        .workers(workers)
        .queue(
            "default",
            QueueConfig {
                fetch_cooldown: Duration::from_millis(1),
                fetch_poll_interval: Duration::from_millis(10),
                max_workers: 1_000,
            },
        )
        .build()
        .unwrap();
    let mut completed = client
        .subscribe_config(SubscribeConfig {
            buffer_capacity: JOB_COUNT,
            kinds: vec![EventKind::JobCompleted],
        })
        .unwrap();
    let jobs = (0..JOB_COUNT).map(|_| (BurstArgs {}, riverqueue::InsertOpts::default()));
    assert_eq!(
        client.insert_many_fast(jobs).await.unwrap(),
        u64::try_from(JOB_COUNT).unwrap()
    );
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
            assert_eq!(event.kind, EventKind::JobCompleted);
            let id = event.job.expect("completion event has a job").id;
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
    sqlx::raw_sql("DROP SCHEMA rust_runtime_burst_test CASCADE")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn external_terminal_state_wins_worker_completion_race() {
    let database_url = std::env::var("RIVER_RUST_DATABASE_URL")
        .expect("RIVER_RUST_DATABASE_URL must point at a disposable test database");
    let pool = PgPool::connect(&database_url).await.unwrap();
    let schema = riverqueue::SchemaName::new("rust_runtime_terminal_race_test").unwrap();
    sqlx::raw_sql(AssertSqlSafe(
        "DROP SCHEMA IF EXISTS rust_runtime_terminal_race_test CASCADE; \
         CREATE SCHEMA rust_runtime_terminal_race_test",
    ))
    .execute(&pool)
    .await
    .unwrap();
    Migrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await
        .unwrap();

    let finish = Arc::new(Semaphore::new(0));
    let started = Arc::new(Semaphore::new(0));
    let mut workers = WorkerRegistry::new();
    workers
        .register::<TerminalRaceArgs, _>(TerminalRaceWorker {
            finish: Arc::clone(&finish),
            started: Arc::clone(&started),
        })
        .unwrap();
    let client = Client::builder(pool.clone())
        .id("rust-runtime-terminal-race-test")
        .poll_only(true)
        .schema(schema.clone())
        .workers(workers)
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
    let mut terminal_events = client
        .subscribe(&[EventKind::JobCompleted, EventKind::JobFailed])
        .unwrap();
    let run_handle = client.start().unwrap();
    let table = schema.qualify("river_job");
    let state_type = schema.qualify("river_job_state");

    for external_state in [JobState::Completed, JobState::Discarded] {
        let inserted = client
            .insert(TerminalRaceArgs {}, riverqueue::InsertOpts::default())
            .await
            .unwrap();
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
        tokio::time::sleep(Duration::from_millis(100)).await;

        let row = client.job_get(inserted.job.row.id).await.unwrap();
        assert_eq!(row.state, external_state);
        assert_eq!(row.metadata["external_terminal"], true);
        assert!(!row.metadata.contains_key("worker_completion"));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), terminal_events.recv())
            .await
            .is_err(),
        "worker emitted an event for an externally finalized job"
    );

    run_handle.shutdown().await.unwrap();
    sqlx::raw_sql("DROP SCHEMA rust_runtime_terminal_race_test CASCADE")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn remote_cancellation_overrides_worker_snooze() {
    let database_url = std::env::var("RIVER_RUST_DATABASE_URL")
        .expect("RIVER_RUST_DATABASE_URL must point at a disposable test database");
    let pool = PgPool::connect(&database_url).await.unwrap();
    let schema = riverqueue::SchemaName::new("rust_runtime_cancel_snooze_test").unwrap();
    sqlx::raw_sql(AssertSqlSafe(
        "DROP SCHEMA IF EXISTS rust_runtime_cancel_snooze_test CASCADE; \
         CREATE SCHEMA rust_runtime_cancel_snooze_test",
    ))
    .execute(&pool)
    .await
    .unwrap();
    Migrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await
        .unwrap();

    let started = Arc::new(Semaphore::new(0));
    let mut workers = WorkerRegistry::new();
    workers
        .register::<CancelSnoozeArgs, _>(CancelSnoozeWorker {
            started: Arc::clone(&started),
        })
        .unwrap();
    let client = Client::builder(pool.clone())
        .id("rust-runtime-cancel-snooze-test")
        .schema(schema.clone())
        .workers(workers)
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
    let mut cancelled_events = client.subscribe(&[EventKind::JobCancelled]).unwrap();
    let mut run_handle = client.start().unwrap();
    run_handle.wait_ready().await.unwrap();
    let inserted = client
        .insert(CancelSnoozeArgs {}, riverqueue::InsertOpts::default())
        .await
        .unwrap();
    started.acquire().await.unwrap().forget();
    client.job_cancel(inserted.job.row.id).await.unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), cancelled_events.recv())
        .await
        .unwrap()
        .unwrap();
    let row = event.job.expect("cancellation event has a job");
    assert_eq!(row.id, inserted.job.row.id);
    assert_eq!(row.state, JobState::Cancelled);
    assert_eq!(
        row.errors.last().unwrap().error,
        "JobCancelError: job cancelled remotely"
    );
    assert_eq!(row.attempt, 1);

    run_handle.shutdown().await.unwrap();
    sqlx::raw_sql("DROP SCHEMA rust_runtime_cancel_snooze_test CASCADE")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn shutdown_waits_for_active_work_and_soft_stop_escalates() {
    let database_url = std::env::var("RIVER_RUST_DATABASE_URL")
        .expect("RIVER_RUST_DATABASE_URL must point at a disposable test database");
    let pool = PgPool::connect(&database_url).await.unwrap();
    let schema = riverqueue::SchemaName::new("rust_runtime_shutdown_test").unwrap();
    sqlx::raw_sql(AssertSqlSafe(
        "DROP SCHEMA IF EXISTS rust_runtime_shutdown_test CASCADE; \
         CREATE SCHEMA rust_runtime_shutdown_test",
    ))
    .execute(&pool)
    .await
    .unwrap();
    Migrator::new(pool.clone())
        .with_schema(schema.clone())
        .migrate_up()
        .await
        .unwrap();

    let graceful_finish = Arc::new(Semaphore::new(0));
    let graceful_started = Arc::new(Semaphore::new(0));
    let mut graceful_workers = WorkerRegistry::new();
    graceful_workers
        .register::<ShutdownArgs, _>(ShutdownWorker {
            finish: Arc::clone(&graceful_finish),
            started: Arc::clone(&graceful_started),
        })
        .unwrap();
    let graceful_client = Client::builder(pool.clone())
        .id("rust-runtime-graceful-shutdown-test")
        .poll_only(true)
        .schema(schema.clone())
        .workers(graceful_workers)
        .queue(
            "graceful",
            QueueConfig {
                fetch_cooldown: Duration::from_millis(1),
                fetch_poll_interval: Duration::from_millis(10),
                max_workers: 1,
            },
        )
        .build()
        .unwrap();
    let active = graceful_client
        .insert(
            ShutdownArgs {
                ignore_cancellation: false,
            },
            riverqueue::InsertOpts {
                queue: "graceful".to_owned(),
                ..riverqueue::InsertOpts::default()
            },
        )
        .await
        .unwrap();
    let mut graceful_handle = graceful_client.start().unwrap();
    graceful_handle.wait_ready().await.unwrap();
    graceful_started.acquire().await.unwrap().forget();
    let unfetched = graceful_client
        .insert(
            ShutdownArgs {
                ignore_cancellation: false,
            },
            riverqueue::InsertOpts {
                queue: "graceful".to_owned(),
                ..riverqueue::InsertOpts::default()
            },
        )
        .await
        .unwrap();
    let graceful_shutdown = tokio::spawn(graceful_handle.shutdown());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !graceful_shutdown.is_finished(),
        "graceful shutdown returned while barrier work was active"
    );
    graceful_finish.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), graceful_shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        graceful_client
            .job_get(active.job.row.id)
            .await
            .unwrap()
            .state,
        JobState::Completed
    );
    assert_eq!(
        graceful_client
            .job_get(unfetched.job.row.id)
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
    let escalation_client = Client::builder(pool.clone())
        .id("rust-runtime-soft-stop-escalation-test")
        .job_stuck_threshold(Duration::from_millis(10))
        .poll_only(true)
        .schema(schema.clone())
        .soft_stop_timeout(Some(Duration::from_millis(50)))
        .workers(escalation_workers)
        .queue(
            "escalation",
            QueueConfig {
                fetch_cooldown: Duration::from_millis(1),
                fetch_poll_interval: Duration::from_millis(10),
                max_workers: 1,
            },
        )
        .build()
        .unwrap();
    let mut interrupted_events = escalation_client
        .subscribe(&[EventKind::JobInterrupted])
        .unwrap();
    let stuck = escalation_client
        .insert(
            ShutdownArgs {
                ignore_cancellation: true,
            },
            riverqueue::InsertOpts {
                queue: "escalation".to_owned(),
                ..riverqueue::InsertOpts::default()
            },
        )
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
    let interrupted = escalation_client.job_get(stuck.job.row.id).await.unwrap();
    assert_eq!(interrupted.attempt, 0);
    assert_eq!(interrupted.state, JobState::Available);
    assert!(interrupted.errors.is_empty());
    let event = tokio::time::timeout(Duration::from_secs(1), interrupted_events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.job.unwrap().id, stuck.job.row.id);

    sqlx::raw_sql("DROP SCHEMA rust_runtime_shutdown_test CASCADE")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn poll_only_and_subscription_configuration() {
    let Ok(database_url) = std::env::var("RIVER_RUST_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL runtime test without RIVER_RUST_DATABASE_URL");
        return;
    };
    let (client, counts) = setup_runtime(&database_url).await;
    let mut completed = client
        .subscribe_config(SubscribeConfig {
            buffer_capacity: 4,
            kinds: vec![EventKind::JobCompleted],
        })
        .unwrap();
    let mut run_handle = client.start().unwrap();
    run_handle.wait_ready().await.unwrap();
    assert_eq!(
        client
            .insert_many_fast([
                (
                    RuntimeArgs {},
                    riverqueue::InsertOpts {
                        pending: true,
                        ..riverqueue::InsertOpts::default()
                    },
                ),
                (
                    RuntimeArgs {},
                    riverqueue::InsertOpts {
                        pending: true,
                        ..riverqueue::InsertOpts::default()
                    },
                ),
            ])
            .await
            .unwrap(),
        2
    );
    let inserted = client.insert_default(RuntimeArgs {}).await.unwrap();
    assert_eq!(inserted.job.row.max_attempts, 7);
    assert_eq!(inserted.job.row.metadata["middleware"], true);
    let event = tokio::time::timeout(Duration::from_secs(2), completed.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.job.unwrap().id, inserted.job.row.id);
    let statistics = event.job_statistics.unwrap();
    assert!(statistics.run_duration >= Duration::from_millis(5));
    assert!(statistics.complete_duration > Duration::ZERO);
    assert!(counts.metrics.load(Ordering::SeqCst) >= 2);
    assert_eq!(counts.periodic_starts.load(Ordering::SeqCst), 1);
    assert_eq!(counts.fast_insert_after.load(Ordering::SeqCst), 2);
    assert_eq!(counts.insert_before.load(Ordering::SeqCst), 3);
    assert_eq!(counts.insert_after.load(Ordering::SeqCst), 1);
    assert_eq!(counts.work_before.load(Ordering::SeqCst), 2);
    assert_eq!(counts.work_after.load(Ordering::SeqCst), 2);

    let mut lagged = client
        .subscribe_config(SubscribeConfig {
            buffer_capacity: 1,
            kinds: vec![EventKind::QueuePaused, EventKind::QueueResumed],
        })
        .unwrap();
    client.queue_pause("default").await.unwrap();
    client.queue_resume("default").await.unwrap();
    client.queue_pause("default").await.unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(matches!(
        lagged.recv().await,
        Err(EventRecvError::Lagged(2))
    ));
    assert_eq!(lagged.recv().await.unwrap().kind, EventKind::QueuePaused);

    run_handle.shutdown().await.unwrap();
    assert_eq!(
        client.job_get(inserted.job.row.id).await.unwrap().state,
        JobState::Completed
    );
}
