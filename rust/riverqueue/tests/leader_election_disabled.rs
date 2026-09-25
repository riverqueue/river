//! Clients built with `without_leader_election`, on every backend.
//!
//! PostgreSQL tests run in a unique schema and fail rather than skip when
//! `RIVER_RUST_DATABASE_URL` is unset; SQLite tests use a temporary file.

#![cfg(any(feature = "postgres-tests", feature = "sqlite"))]

mod support;

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
    __private::{
        ClientBuilderExt, Database, DatabaseConfig, DatabasePool, MaintenanceService, Pilot,
        PilotError, RuntimeService,
    },
    Client, ClientBuilder, EventKind, EventReceiver, Job, JobArgs, JobRow, JobState,
    MaintenanceConfig, NeverSchedule, PeriodicJob, PeriodicJobOpts, QueueConfig, WorkContext,
    WorkOutcome, WorkerRegistry,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

/// Every wait in these tests is bounded by this timeout.
const TIMEOUT: Duration = Duration::from_secs(10);

/// A short election interval, so that a client wrongly taking part in
/// elections would become leader well within these tests.
const ELECT_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_leader_election_disabled_noop")]
struct NoopArgs {}

fn noop_workers() -> WorkerRegistry {
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(|_context: WorkContext, _job: Job<NoopArgs>| async {
            Ok::<_, Infallible>(WorkOutcome::Complete)
        })
        .unwrap();
    workers
}

fn run_on_start_job() -> PeriodicJob {
    PeriodicJob::with_options(
        NeverSchedule,
        || NoopArgs {},
        PeriodicJobOpts::new().run_on_start(),
    )
}

/// Counts starts of an extension's leader-owned and per-client services.
#[derive(Clone, Default)]
struct ServicePilot {
    maintenance_services_calls: Arc<AtomicUsize>,
    maintenance_starts: Arc<AtomicUsize>,
    runtime_starts: Arc<AtomicUsize>,
}

#[async_trait]
impl Pilot for ServicePilot {
    fn maintenance_services(&self) -> Vec<Arc<dyn MaintenanceService>> {
        self.maintenance_services_calls
            .fetch_add(1, Ordering::SeqCst);
        vec![Arc::new(CountingService(Arc::clone(
            &self.maintenance_starts,
        )))]
    }

    fn runtime_services(&self) -> Vec<Arc<dyn RuntimeService>> {
        vec![Arc::new(CountingService(Arc::clone(&self.runtime_starts)))]
    }
}

struct CountingService(Arc<AtomicUsize>);

impl CountingService {
    async fn run_until_cancelled(&self, cancellation: CancellationToken) {
        self.0.fetch_add(1, Ordering::SeqCst);
        cancellation.cancelled().await;
    }
}

#[async_trait]
impl MaintenanceService for CountingService {
    async fn run(
        &self,
        _pool: DatabasePool,
        _database: DatabaseConfig,
        cancellation: CancellationToken,
    ) -> Result<(), PilotError> {
        self.run_until_cancelled(cancellation).await;
        Ok(())
    }
}

#[async_trait]
impl RuntimeService for CountingService {
    async fn run(
        &self,
        _pool: DatabasePool,
        _database: DatabaseConfig,
        cancellation: CancellationToken,
    ) -> Result<(), PilotError> {
        self.run_until_cancelled(cancellation).await;
        Ok(())
    }
}

/// A migrated database on one backend.
enum Backend {
    #[cfg(feature = "postgres-tests")]
    Postgres(support::PostgresSchema),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool, std::path::PathBuf),
}

impl Backend {
    #[cfg(feature = "postgres-tests")]
    async fn postgres() -> Self {
        Self::Postgres(support::PostgresSchema::new("river_no_election").await)
    }

    #[cfg(feature = "sqlite")]
    async fn sqlite() -> Self {
        let (pool, path) = support::sqlite_file_pool(4).await;
        Self::Sqlite(pool, path)
    }

    fn database(&self) -> Database {
        match self {
            #[cfg(feature = "postgres-tests")]
            Self::Postgres(schema) => Database::from_source(
                riverqueue::database::PostgresDatabase::new(schema.pool.clone())
                    .schema(schema.schema.clone()),
            ),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool, _) => Database::from_source(pool.clone()),
        }
    }

    /// Returns a builder for a worker client with a short election interval.
    fn builder(&self, id: &str, queue: &str) -> ClientBuilder {
        Client::builder(self.database())
            .id(id)
            .maintenance(MaintenanceConfig::default().with_elect_interval(ELECT_INTERVAL))
            .queue(queue, QueueConfig::new(1))
            .workers(noop_workers())
    }

    /// Returns the elected leader's client ID, if any.
    async fn leader_id(&self) -> Option<String> {
        match self {
            #[cfg(feature = "postgres-tests")]
            Self::Postgres(schema) => sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT leader_id FROM {}",
                schema.table("river_leader")
            )))
            .fetch_optional(&schema.pool)
            .await
            .unwrap(),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool, _) => sqlx::query_scalar("SELECT leader_id FROM river_leader")
                .fetch_optional(pool)
                .await
                .unwrap(),
        }
    }

    async fn wait_for_leader(&self, id: &str) {
        tokio::time::timeout(TIMEOUT, async {
            while self.leader_id().await.as_deref() != Some(id) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{id} was not elected leader"));
    }

    async fn cleanup(self) {
        match self {
            #[cfg(feature = "postgres-tests")]
            Self::Postgres(schema) => schema.cleanup().await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool, path) => support::sqlite_cleanup(pool, path).await,
        }
    }
}

async fn next_completed(events: &mut EventReceiver) -> JobRow {
    let event = tokio::time::timeout(TIMEOUT, events.recv())
        .await
        .expect("a job should complete")
        .unwrap();
    let job = event.as_job().expect("a job event").clone();
    assert_eq!(job.job.state, JobState::Completed);
    job.job
}

/// Works jobs over two runs of the same client without ever electing a
/// leader, including stopping with no leadership to resign.
async fn works_jobs_without_electing(backend: Backend, poll_only: bool) {
    let mut builder = backend
        .builder("no_election", "default")
        .without_leader_election();
    if poll_only {
        builder = builder.without_notifications();
    }
    let client = builder.build().unwrap();
    for _ in 0..2 {
        let mut events = client.subscribe(&[EventKind::JobCompleted]).unwrap();
        let mut run = client.start().unwrap();
        run.wait_ready().await.unwrap();

        let inserted = client.insert(NoopArgs {}).await.unwrap();
        assert_eq!(next_completed(&mut events).await.id, inserted.job.row.id);
        assert_eq!(backend.leader_id().await, None);

        tokio::time::timeout(Duration::from_secs(5), run.shutdown())
            .await
            .expect("the client should stop")
            .unwrap();
    }
    backend.cleanup().await;
}

/// Works a periodic job another client enqueues as leader, runs no
/// leader-owned extension services, and stays ineligible once that leader
/// stops.
async fn stays_ineligible_after_leader_stops(backend: Backend) {
    let pilot = ServicePilot::default();
    let client = backend
        .builder("no_election", "default")
        .without_leader_election()
        .pilot(pilot.clone())
        .build()
        .unwrap();
    let mut events = client.subscribe(&[EventKind::JobCompleted]).unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    // The leader enqueues a periodic job on the default queue, which only
    // the client without leader election works.
    let leader = backend
        .builder("eligible_leader", "leader")
        .periodic_job(run_on_start_job())
        .build()
        .unwrap();
    let mut leader_run = leader.start().unwrap();
    leader_run.wait_ready().await.unwrap();
    backend.wait_for_leader("eligible_leader").await;

    let periodic = next_completed(&mut events).await;
    assert_eq!(periodic.attempted_by, ["no_election"]);
    assert_eq!(
        backend.leader_id().await.as_deref(),
        Some("eligible_leader")
    );

    tokio::time::timeout(Duration::from_secs(5), leader_run.shutdown())
        .await
        .expect("the leader should stop")
        .unwrap();

    let inserted = client.insert(NoopArgs {}).await.unwrap();
    let worked = next_completed(&mut events).await;
    assert_eq!(worked.id, inserted.job.row.id);
    assert_eq!(worked.attempted_by, ["no_election"]);
    assert_eq!(backend.leader_id().await, None);

    assert_eq!(pilot.maintenance_services_calls.load(Ordering::SeqCst), 0);
    assert_eq!(pilot.maintenance_starts.load(Ordering::SeqCst), 0);
    assert_eq!(pilot.runtime_starts.load(Ordering::SeqCst), 1);

    run.shutdown().await.unwrap();
    backend.cleanup().await;
}

#[cfg(feature = "sqlite")]
mod configuration {
    use riverqueue::IntervalSchedule;

    use super::*;

    #[tokio::test]
    async fn build_rejects_periodic_jobs() {
        let backend = Backend::sqlite().await;
        let error = backend
            .builder("no_election", "default")
            .without_leader_election()
            .periodic_job(run_on_start_job())
            .build()
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("periodic jobs must be empty when leader election is disabled"),
            "{error}"
        );
        backend.cleanup().await;
    }

    #[tokio::test]
    async fn builder_reports_leader_election_disabled() {
        let backend = Backend::sqlite().await;
        let builder = backend.builder("default_client", "default");
        assert!(!builder.leader_election_disabled());
        assert!(builder.without_leader_election().leader_election_disabled());
        backend.cleanup().await;
    }

    #[tokio::test]
    async fn periodic_jobs_reject_additions() {
        let backend = Backend::sqlite().await;
        let client = backend
            .builder("no_election", "default")
            .without_leader_election()
            .build()
            .unwrap();
        let periodic = client.periodic_jobs();
        let job = || {
            PeriodicJob::new(
                IntervalSchedule::new(Duration::from_mins(1)).unwrap(),
                || NoopArgs {},
            )
        };
        for error in [
            periodic.add(job()).unwrap_err(),
            periodic.add_many(vec![job()]).unwrap_err(),
        ] {
            assert!(
                error
                    .to_string()
                    .contains("periodic jobs can't be added when leader election is disabled"),
                "{error}"
            );
        }
        // Removing jobs from the always-empty bundle is harmless.
        periodic.clear();
        assert!(!periodic.remove_by_id("missing"));
        backend.cleanup().await;
    }
}

#[cfg(feature = "postgres-tests")]
mod postgres {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn stays_ineligible_after_leader_stops() {
        super::stays_ineligible_after_leader_stops(Backend::postgres().await).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn works_jobs_without_electing() {
        super::works_jobs_without_electing(Backend::postgres().await, false).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn works_jobs_without_electing_poll_only() {
        super::works_jobs_without_electing(Backend::postgres().await, true).await;
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn stays_ineligible_after_leader_stops() {
        super::stays_ineligible_after_leader_stops(Backend::sqlite().await).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn works_jobs_without_electing() {
        super::works_jobs_without_electing(Backend::sqlite().await, false).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn works_jobs_without_electing_poll_only() {
        super::works_jobs_without_electing(Backend::sqlite().await, true).await;
    }
}
