#![cfg(feature = "postgres-tests")]

use std::{
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

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_runtime_config")]
struct RuntimeArgs {}

struct RuntimeWorker;

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
