//! Extension services and the client's stop order.
//!
//! Maintenance services get their leadership term and are supervised within
//! it. On a graceful stop, claims, leadership with its maintenance services,
//! and runtime services end at once, like River Go's services started on its
//! fetch context, while each producer keeps reporting to its extension
//! session until its running jobs finish.
//!
//! PostgreSQL scenarios run in a unique schema and fail rather than skip when
//! `RIVER_RUST_DATABASE_URL` is unset; SQLite scenarios use temporary files.

#![cfg(any(feature = "postgres-tests", feature = "sqlite"))]

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
use chrono::{DateTime, Utc};
use riverqueue::__private::{
    ClientBuilderExt, MaintenanceService, MaintenanceServiceContext, Pilot, PilotError,
    PilotProducer, ProducerKeepAliveContext, ProducerShutdownContext, ProducerStartContext,
    RuntimeService, RuntimeServiceContext,
};
use riverqueue::{
    Client, Job, JobArgs, JobRow, JobState, MaintenanceConfig, QueueConfig, WorkContext,
    WorkOutcome, WorkerRegistry,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, Semaphore};

const WAIT: Duration = Duration::from_secs(10);

/// Bounds a whole scenario, so a stop that never finishes fails the test
/// instead of hanging it.
const SCENARIO: Duration = Duration::from_mins(1);

/// A job whose worker holds its slot until the test releases it.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "extension_services_gated")]
struct GatedArgs {}

/// Everything River told the extension, in order.
#[derive(Clone, Debug, PartialEq)]
enum Call {
    Finished(i64),
    KeepAlive,
    MaintenanceStarted(DateTime<Utc>),
    MaintenanceStopped,
    RuntimeStarted,
    RuntimeStopped,
    Shutdown,
}

#[derive(Default)]
struct Calls {
    changed: Notify,
    calls: Mutex<Vec<Call>>,
}

impl Calls {
    fn push(&self, call: Call) {
        self.calls.lock().unwrap().push(call);
        self.changed.notify_waiters();
    }

    fn snapshot(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    fn position(&self, call: &Call) -> Option<usize> {
        self.snapshot().iter().position(|recorded| recorded == call)
    }

    async fn wait_until(&self, what: &str, done: impl Fn(&[Call]) -> bool) {
        tokio::time::timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                if done(&self.calls.lock().unwrap()) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }
}

/// How the maintenance service's runs end.
#[derive(Clone, Copy, Debug)]
enum Runs {
    /// Every run waits for its term to end.
    UntilTermEnds,
    /// The first run fails, the second panics, and later runs wait for the
    /// term to end.
    FailThenPanic,
}

#[derive(Clone)]
struct ServicePilot {
    calls: Arc<Calls>,
    maintenance_runs: Arc<AtomicUsize>,
    runs: Runs,
}

impl ServicePilot {
    fn new(runs: Runs) -> Self {
        Self {
            calls: Arc::default(),
            maintenance_runs: Arc::default(),
            runs,
        }
    }
}

#[async_trait]
impl Pilot for ServicePilot {
    fn maintenance_services(&self) -> Vec<Arc<dyn MaintenanceService>> {
        vec![Arc::new(self.clone())]
    }

    fn runtime_services(&self) -> Vec<Arc<dyn RuntimeService>> {
        vec![Arc::new(self.clone())]
    }

    async fn start_producer(
        &self,
        _context: ProducerStartContext,
    ) -> Result<Option<Box<dyn PilotProducer>>, PilotError> {
        Ok(Some(Box::new(self.clone())))
    }
}

#[async_trait]
impl MaintenanceService for ServicePilot {
    fn name(&self) -> &'static str {
        "test maintenance"
    }

    async fn run(&self, context: MaintenanceServiceContext) -> Result<(), PilotError> {
        let run = self.maintenance_runs.fetch_add(1, Ordering::SeqCst);
        self.calls
            .push(Call::MaintenanceStarted(context.term.elected_at));
        match (self.runs, run) {
            (Runs::FailThenPanic, 0) => return Err("maintenance failed on purpose".into()),
            (Runs::FailThenPanic, 1) => panic!("maintenance panicked on purpose"),
            _ => {}
        }
        // The service can use the client's database.
        context.database.begin().await?.commit().await?;
        context.term.token.cancelled().await;
        self.calls.push(Call::MaintenanceStopped);
        Ok(())
    }
}

#[async_trait]
impl RuntimeService for ServicePilot {
    async fn run(&self, context: RuntimeServiceContext) -> Result<(), PilotError> {
        self.calls.push(Call::RuntimeStarted);
        context.database.begin().await?.commit().await?;
        context.cancellation.cancelled().await;
        self.calls.push(Call::RuntimeStopped);
        Ok(())
    }
}

#[async_trait]
impl PilotProducer for ServicePilot {
    fn job_finished(&self, job: &JobRow) {
        self.calls.push(Call::Finished(job.id));
    }

    async fn keep_alive(&self, _context: ProducerKeepAliveContext) -> Result<(), PilotError> {
        self.calls.push(Call::KeepAlive);
        Ok(())
    }

    async fn shutdown(&self, _context: ProducerShutdownContext) -> Result<(), PilotError> {
        self.calls.push(Call::Shutdown);
        Ok(())
    }
}

fn client(
    builder: riverqueue::ClientBuilder,
    pilot: &ServicePilot,
    release: &Arc<Semaphore>,
) -> Client {
    let mut workers = WorkerRegistry::new();
    let release = Arc::clone(release);
    workers
        .register_fn(move |_context: WorkContext, _job: Job<GatedArgs>| {
            let release = Arc::clone(&release);
            async move {
                release.acquire().await.unwrap().forget();
                Ok::<_, Infallible>(WorkOutcome::Complete)
            }
        })
        .unwrap();
    builder
        .pilot(pilot.clone())
        .producer_report_interval(Duration::from_millis(20))
        .maintenance(MaintenanceConfig::default().with_elect_interval(Duration::from_millis(100)))
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .workers(workers)
        .build()
        .unwrap()
}

/// A graceful stop ends leadership, maintenance services, and runtime
/// services at once, while the producer keeps reporting until its running
/// job finishes and only then shuts its session down.
async fn assert_stop_order_matches_go(builder: riverqueue::ClientBuilder) {
    let pilot = ServicePilot::new(Runs::UntilTermEnds);
    let release = Arc::new(Semaphore::new(0));
    let client = client(builder, &pilot, &release);
    let id = client.insert(GatedArgs {}).await.unwrap().id();
    let mut run = client.start().unwrap();
    pilot
        .calls
        .wait_until("leadership and a running job", |calls| {
            calls
                .iter()
                .any(|call| matches!(call, Call::MaintenanceStarted(_)))
                && calls.contains(&Call::RuntimeStarted)
        })
        .await;
    tokio::time::timeout(WAIT, async {
        while client.jobs().get(id).await.unwrap().state != JobState::Running {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("job starts");

    run.stopper().stop();
    pilot
        .calls
        .wait_until("services to stop", |calls| {
            calls.contains(&Call::MaintenanceStopped) && calls.contains(&Call::RuntimeStopped)
        })
        .await;
    let stopped = pilot.calls.snapshot().len();
    pilot
        .calls
        .wait_until("reports after the services stopped", |calls| {
            calls[stopped..]
                .iter()
                .filter(|call| **call == Call::KeepAlive)
                .count()
                >= 2
        })
        .await;
    assert_eq!(pilot.calls.position(&Call::Shutdown), None);
    assert_eq!(
        client.jobs().get(id).await.unwrap().state,
        JobState::Running
    );

    release.add_permits(1);
    tokio::time::timeout(WAIT, run.wait())
        .await
        .expect("client stops")
        .unwrap();
    let finished = pilot.calls.position(&Call::Finished(id)).unwrap();
    let shutdown = pilot.calls.position(&Call::Shutdown).unwrap();
    assert!(finished < shutdown);
    assert_eq!(
        client.jobs().get(id).await.unwrap().state,
        JobState::Completed
    );
}

/// A maintenance service that fails or panics is restarted within its
/// leadership term, and every run gets the same term.
async fn assert_maintenance_services_restart_within_their_term(builder: riverqueue::ClientBuilder) {
    let pilot = ServicePilot::new(Runs::FailThenPanic);
    let release = Arc::new(Semaphore::new(0));
    let client = client(builder, &pilot, &release);
    let mut run = client.start().unwrap();
    pilot
        .calls
        .wait_until("the third maintenance run", |calls| {
            calls
                .iter()
                .filter(|call| matches!(call, Call::MaintenanceStarted(_)))
                .count()
                >= 3
        })
        .await;
    run.shutdown().await.unwrap();

    let terms = pilot
        .calls
        .snapshot()
        .into_iter()
        .filter_map(|call| match call {
            Call::MaintenanceStarted(elected_at) => Some(elected_at),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(terms.len(), 3, "{terms:?}");
    assert!(terms.iter().all(|term| *term == terms[0]), "{terms:?}");
    assert!(pilot.calls.snapshot().contains(&Call::MaintenanceStopped));
}

#[cfg(feature = "postgres-tests")]
mod postgres {
    use riverqueue::database::PostgresDatabase;

    use super::*;
    use crate::support::PostgresSchema;

    fn builder(schema: &PostgresSchema) -> riverqueue::ClientBuilder {
        Client::builder(
            PostgresDatabase::new(schema.pool.clone()).with_schema(schema.schema.clone()),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn maintenance_services_restart_within_their_term() {
        let schema = PostgresSchema::new("services_restart").await;
        tokio::time::timeout(
            SCENARIO,
            assert_maintenance_services_restart_within_their_term(builder(&schema)),
        )
        .await
        .expect("scenario finishes");
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stop_order_matches_go() {
        let schema = PostgresSchema::new("services_stop_order").await;
        tokio::time::timeout(SCENARIO, assert_stop_order_matches_go(builder(&schema)))
            .await
            .expect("scenario finishes");
        schema.cleanup().await;
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    use crate::support::{sqlite_cleanup, sqlite_file_pool};

    #[tokio::test(flavor = "multi_thread")]
    async fn maintenance_services_restart_within_their_term() {
        let (pool, path) = sqlite_file_pool(4).await;
        tokio::time::timeout(
            SCENARIO,
            assert_maintenance_services_restart_within_their_term(Client::builder(pool.clone())),
        )
        .await
        .expect("scenario finishes");
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stop_order_matches_go() {
        let (pool, path) = sqlite_file_pool(4).await;
        tokio::time::timeout(
            SCENARIO,
            assert_stop_order_matches_go(Client::builder(pool.clone())),
        )
        .await
        .expect("scenario finishes");
        sqlite_cleanup(pool, path).await;
    }
}
