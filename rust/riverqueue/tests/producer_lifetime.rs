//! A producer's lifetime as an extension session sees it: reports that
//! continue while the producer drains, the serial shutdown after its last
//! attempt, queue removal that waits for that shutdown and keeps the name
//! reserved, live reconfiguration, and extension queue settings.
//!
//! Postgres scenarios run in a unique schema and fail rather than skip when
//! `RIVER_RUST_DATABASE_URL` is unset; SQLite scenarios use temporary files.

#![cfg(any(all(feature = "postgres", river_postgres_tests), feature = "sqlite"))]

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
use riverqueue::__private::{
    ClientBuilderExt, Pilot, PilotError, PilotProducer, ProducerConfiguration,
    ProducerKeepAliveContext, ProducerShutdownContext, ProducerStartContext, QueueConfigExt,
};
use riverqueue::{
    Client, Error, ExtensionPhase, InsertOpts, Job, JobArgs, JobRow, JobState, QueueConfig,
    WorkContext, WorkOutcome, Workers,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::sync::{Notify, Semaphore};

const WAIT: Duration = Duration::from_secs(10);

/// A job whose worker holds its slot until the test releases it.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "producer_lifetime_gated")]
struct GatedArgs {}

/// Starts and releases [`GatedArgs`] jobs.
#[derive(Clone)]
struct Gate {
    release: Arc<Semaphore>,
    started: Arc<Recorder<i64>>,
}

impl Gate {
    fn new() -> Self {
        Self {
            release: Arc::new(Semaphore::new(0)),
            started: Arc::default(),
        }
    }

    fn release(&self, jobs: usize) {
        self.release.add_permits(jobs);
    }

    fn workers(&self) -> Workers {
        let gate = self.clone();
        let mut workers = Workers::new();
        workers
            .add_fn(move |_context: WorkContext, job: Job<GatedArgs>| {
                let gate = gate.clone();
                async move {
                    gate.started.push(job.id());
                    gate.release.acquire().await.unwrap().forget();
                    Ok::<_, Infallible>(WorkOutcome::Complete)
                }
            })
            .unwrap();
        workers
    }
}

fn fast_queue(max_workers: usize) -> QueueConfig {
    QueueConfig::new(max_workers)
        .with_fetch_cooldown(Duration::from_millis(1))
        .with_fetch_poll_interval(Duration::from_millis(10))
}

/// Values recorded by a session, with a notification on every change.
struct Recorder<T> {
    changed: Notify,
    values: Mutex<Vec<T>>,
}

impl<T> Default for Recorder<T> {
    fn default() -> Self {
        Self {
            changed: Notify::new(),
            values: Mutex::new(Vec::new()),
        }
    }
}

impl<T: Clone> Recorder<T> {
    fn push(&self, value: T) {
        self.values.lock().unwrap().push(value);
        self.changed.notify_waiters();
    }

    fn snapshot(&self) -> Vec<T> {
        self.values.lock().unwrap().clone()
    }

    /// Waits until `done` holds for the recorded values.
    async fn wait_until(&self, what: &str, done: impl Fn(&[T]) -> bool) {
        tokio::time::timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                if done(&self.values.lock().unwrap()) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }
}

/// What a session was told, in order.
#[derive(Clone, Debug, PartialEq)]
enum Call {
    Configured(ProducerConfiguration),
    Finished(i64),
    KeepAlive,
    Shutdown { attempt: u32, timeout: Duration },
    Started(ProducerConfiguration),
}

/// How a session's shutdown attempts behave.
#[derive(Clone, Copy, Debug)]
enum ShutdownBehavior {
    /// Every attempt succeeds.
    Succeed,
    /// The first attempt never finishes, the second fails, and the third
    /// succeeds.
    HangThenFail,
}

/// Records every call River makes to its sessions. Accepts extension
/// settings that are JSON objects.
#[derive(Clone)]
struct LifetimePilot {
    calls: Arc<Recorder<Call>>,
    /// The first keep-alive never finishes.
    hang_first_keep_alive: bool,
    /// The session callback that panics, if any.
    panic_in: Option<&'static str>,
    shutdown: ShutdownBehavior,
    starts: Arc<AtomicUsize>,
}

impl LifetimePilot {
    fn new(shutdown: ShutdownBehavior) -> Self {
        Self {
            calls: Arc::default(),
            hang_first_keep_alive: false,
            panic_in: None,
            shutdown,
            starts: Arc::default(),
        }
    }

    fn keep_alives(&self) -> usize {
        self.calls
            .snapshot()
            .iter()
            .filter(|call| matches!(call, Call::KeepAlive))
            .count()
    }

    fn shutdowns(calls: &[Call]) -> Vec<(u32, Duration)> {
        calls
            .iter()
            .filter_map(|call| match call {
                Call::Shutdown { attempt, timeout } => Some((*attempt, *timeout)),
                _ => None,
            })
            .collect()
    }
}

#[async_trait]
impl Pilot for LifetimePilot {
    fn validate_queue_settings(
        &self,
        _queue: &str,
        settings: &Map<String, Value>,
    ) -> Result<(), PilotError> {
        match settings.get("limit") {
            None | Some(Value::Number(_)) => Ok(()),
            Some(other) => Err(format!("limit must be a number, not {other}").into()),
        }
    }

    async fn start_producer(
        &self,
        context: ProducerStartContext,
    ) -> Result<Option<Box<dyn PilotProducer>>, PilotError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.calls.push(Call::Started(context.configuration));
        Ok(Some(Box::new(self.clone())))
    }
}

#[async_trait]
impl PilotProducer for LifetimePilot {
    fn configuration_changed(&self, configuration: &ProducerConfiguration) {
        self.calls.push(Call::Configured(configuration.clone()));
        assert_ne!(
            self.panic_in,
            Some("configuration_changed"),
            "panicked on purpose"
        );
    }

    fn job_finished(&self, job: &JobRow) {
        self.calls.push(Call::Finished(job.id));
        assert_ne!(self.panic_in, Some("job_finished"), "panicked on purpose");
    }

    async fn keep_alive(&self, context: ProducerKeepAliveContext) -> Result<(), PilotError> {
        assert!(context.stale_before < chrono::Utc::now());
        self.calls.push(Call::KeepAlive);
        if self.hang_first_keep_alive && self.keep_alives() == 1 {
            std::future::pending::<()>().await;
        }
        Ok(())
    }

    async fn shutdown(&self, context: ProducerShutdownContext) -> Result<(), PilotError> {
        self.calls.push(Call::Shutdown {
            attempt: context.attempt,
            timeout: context.timeout,
        });
        match (self.shutdown, context.attempt) {
            (ShutdownBehavior::HangThenFail, 1) => std::future::pending().await,
            (ShutdownBehavior::HangThenFail, 2) => Err("shutdown failed on purpose".into()),
            _ => Ok(()),
        }
    }
}

fn builder_with(
    builder: riverqueue::ClientBuilder,
    pilot: &LifetimePilot,
    gate: &Gate,
) -> riverqueue::ClientBuilder {
    builder
        .pilot(pilot.clone())
        .producer_report_interval(Duration::from_millis(20))
        .workers(gate.workers())
}

/// A stopping producer keeps reporting while its attempts drain, so peers
/// keep counting them, and shuts its session down only after the last one
/// finished. Nothing reaches the session after shutdown.
async fn assert_reports_continue_through_the_drain(builder: riverqueue::ClientBuilder) {
    let pilot = LifetimePilot::new(ShutdownBehavior::Succeed);
    let gate = Gate::new();
    let client = builder_with(builder, &pilot, &gate)
        .queue("default", fast_queue(1))
        .build()
        .unwrap();
    let id = client.insert(GatedArgs {}).await.unwrap().id();
    let mut run = client.start().unwrap();
    gate.started
        .wait_until("the job to start", |started| started.contains(&id))
        .await;

    run.stopper().stop();
    let before = pilot.keep_alives();
    pilot
        .calls
        .wait_until("two reports during the drain", |calls| {
            calls
                .iter()
                .filter(|call| matches!(call, Call::KeepAlive))
                .count()
                >= before + 2
        })
        .await;
    assert_eq!(LifetimePilot::shutdowns(&pilot.calls.snapshot()), []);
    assert_eq!(
        client.jobs().get(id).await.unwrap().state,
        JobState::Running
    );

    gate.release(1);
    tokio::time::timeout(WAIT, run.wait())
        .await
        .expect("client stops")
        .unwrap();
    let calls = pilot.calls.snapshot();
    let finished = calls
        .iter()
        .position(|call| *call == Call::Finished(id))
        .expect("job finished");
    let shutdown = calls
        .iter()
        .position(|call| matches!(call, Call::Shutdown { .. }))
        .expect("session shut down");
    assert!(finished < shutdown, "{calls:?}");
    assert_eq!(shutdown, calls.len() - 1, "{calls:?}");
    assert_eq!(
        LifetimePilot::shutdowns(&calls),
        [(1, Duration::from_millis(100))]
    );
    assert_eq!(
        client.jobs().get(id).await.unwrap().state,
        JobState::Completed
    );
}

/// Shutdown attempts run one at a time with Go's growing deadlines: an
/// attempt that doesn't finish in time is dropped, and a failed one is
/// retried.
async fn assert_shutdown_retries_with_growing_deadlines(builder: riverqueue::ClientBuilder) {
    let pilot = LifetimePilot::new(ShutdownBehavior::HangThenFail);
    let gate = Gate::new();
    let client = builder_with(builder, &pilot, &gate)
        .queue("default", fast_queue(1))
        .build()
        .unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();
    tokio::time::timeout(WAIT, run.stop())
        .await
        .expect("client stops")
        .unwrap();

    assert_eq!(
        LifetimePilot::shutdowns(&pilot.calls.snapshot()),
        [
            (1, Duration::from_millis(100)),
            (2, Duration::from_millis(500)),
            (3, Duration::from_millis(2_500)),
        ]
    );
}

/// A keep-alive that never finishes is dropped after Go's ten seconds, and
/// the next report follows.
async fn assert_stuck_keep_alives_time_out(builder: riverqueue::ClientBuilder) {
    let pilot = LifetimePilot {
        hang_first_keep_alive: true,
        ..LifetimePilot::new(ShutdownBehavior::Succeed)
    };
    let gate = Gate::new();
    let client = builder_with(builder, &pilot, &gate)
        .queue("default", fast_queue(1))
        .build()
        .unwrap();
    let mut run = client.start().unwrap();
    pilot
        .calls
        .wait_until("the first report", |calls| calls.contains(&Call::KeepAlive))
        .await;
    let started = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(20), async {
        while pilot.keep_alives() < 2 {
            pilot.calls.changed.notified().await;
        }
    })
    .await
    .expect("a report after the stuck one");
    let waited = started.elapsed();
    assert!(
        (Duration::from_secs(9)..Duration::from_secs(15)).contains(&waited),
        "{waited:?}"
    );
    run.stop().await.unwrap();
}

/// A panic in `job_finished` or `configuration_changed` stops the client
/// with an extension error, after the producer drains and shuts the session
/// down.
async fn assert_callback_panics_stop_the_client_in_order(
    builder: impl Fn() -> riverqueue::ClientBuilder,
    callback: &'static str,
) {
    let pilot = LifetimePilot {
        panic_in: Some(callback),
        ..LifetimePilot::new(ShutdownBehavior::Succeed)
    };
    let gate = Gate::new();
    let client = builder_with(builder(), &pilot, &gate)
        .queue("default", fast_queue(1))
        .build()
        .unwrap();
    let id = client.insert(GatedArgs {}).await.unwrap().id();
    let mut run = client.start().unwrap();
    gate.started
        .wait_until("the job to start", |started| started.contains(&id))
        .await;
    if callback == "configuration_changed" {
        client
            .local_queues()
            .update("default", fast_queue(2))
            .unwrap();
        pilot
            .calls
            .wait_until("the configuration change", |calls| {
                calls.iter().any(|call| matches!(call, Call::Configured(_)))
            })
            .await;
    }
    // The job ignores cancellation, so the drain waits for it.
    gate.release(1);
    let error = tokio::time::timeout(WAIT, run.wait())
        .await
        .expect("client stops")
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::Extension {
                phase: ExtensionPhase::AddOn {
                    operation: "producer"
                },
                ..
            }
        ),
        "{callback}: {error}"
    );
    let calls = pilot.calls.snapshot();
    assert!(calls.contains(&Call::Finished(id)), "{callback}: {calls:?}");
    assert!(
        matches!(calls.last(), Some(Call::Shutdown { attempt: 1, .. })),
        "{callback}: {calls:?}"
    );
}

/// Like Go's `QueueBundle.Remove`, removing a queue waits for its producer
/// to drain and shut its session down, and the name stays reserved until
/// then. Adding the queue again starts a new session.
async fn assert_removal_waits_for_the_drain(builder: riverqueue::ClientBuilder) {
    let pilot = LifetimePilot::new(ShutdownBehavior::Succeed);
    let gate = Gate::new();
    let client = builder_with(builder, &pilot, &gate)
        .queue("default", fast_queue(1))
        .build()
        .unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();
    client.local_queues().add("gated", fast_queue(1)).unwrap();
    let id = client
        .insert(GatedArgs {})
        .opts(InsertOpts::default().with_queue("gated"))
        .await
        .unwrap()
        .id();
    gate.started
        .wait_until("the job to start", |started| started.contains(&id))
        .await;

    let removing = tokio::spawn({
        let client = client.clone();
        async move { client.local_queues().remove("gated").await }
    });
    // Reports keep arriving while the removed queue drains, and the removal
    // stays pending until the job finishes.
    let before = pilot.keep_alives();
    pilot
        .calls
        .wait_until("reports during the removal", |calls| {
            calls
                .iter()
                .filter(|call| matches!(call, Call::KeepAlive))
                .count()
                >= before + 4
        })
        .await;
    assert!(!removing.is_finished());
    assert!(!client.local_queues().configs().contains_key("gated"));
    assert!(matches!(
        client.local_queues().add("gated", fast_queue(1)),
        Err(Error::QueueAlreadyAdded { name }) if name == "gated"
    ));

    gate.release(1);
    let removed = tokio::time::timeout(WAIT, removing)
        .await
        .expect("removal finishes")
        .unwrap()
        .unwrap();
    assert_eq!(removed, fast_queue(1));
    // Both the default queue's session and the removed one's may report, but
    // the removed session shut down before the removal returned.
    assert_eq!(LifetimePilot::shutdowns(&pilot.calls.snapshot()).len(), 1);

    let starts = pilot.starts.load(Ordering::SeqCst);
    client.local_queues().add("gated", fast_queue(1)).unwrap();
    pilot
        .calls
        .wait_until("a new session", |_| {
            pilot.starts.load(Ordering::SeqCst) > starts
        })
        .await;
    run.stop().await.unwrap();
}

/// A running producer applies an updated configuration without restarting:
/// more workers start another job at once, and fewer workers never cancel
/// running jobs. The session sees each configuration.
async fn assert_updates_apply_while_running(builder: riverqueue::ClientBuilder) {
    let pilot = LifetimePilot::new(ShutdownBehavior::Succeed);
    let gate = Gate::new();
    let client = builder_with(builder, &pilot, &gate)
        .queue("default", fast_queue(1))
        .build()
        .unwrap();
    let first = client.insert(GatedArgs {}).await.unwrap().id();
    let second = client.insert(GatedArgs {}).await.unwrap().id();
    let mut run = client.start().unwrap();
    gate.started
        .wait_until("the first job", |started| started == [first])
        .await;

    client
        .local_queues()
        .update("default", fast_queue(2))
        .unwrap();
    gate.started
        .wait_until("the second job", |started| started.contains(&second))
        .await;
    client
        .local_queues()
        .update("default", fast_queue(1))
        .unwrap();
    pilot
        .calls
        .wait_until("both configurations", |calls| {
            calls
                .iter()
                .filter_map(|call| match call {
                    Call::Configured(configuration) => Some(configuration.max_workers),
                    _ => None,
                })
                .eq([2, 1])
        })
        .await;

    gate.release(2);
    for id in [first, second] {
        tokio::time::timeout(WAIT, async {
            while client.jobs().get(id).await.unwrap().state != JobState::Completed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("jobs complete");
    }
    run.stop().await.unwrap();
    assert_eq!(pilot.starts.load(Ordering::SeqCst), 1, "no restart");
}

/// Extension queue settings reach the session and are validated by the
/// extension when the client is built and when a queue is added or
/// updated.
async fn assert_queue_settings_reach_the_session(builder: impl Fn() -> riverqueue::ClientBuilder) {
    let gate = Gate::new();
    let error = builder()
        .workers(gate.workers())
        .queue(
            "default",
            fast_queue(1).with_extension_setting("limit", json!(1)),
        )
        .build()
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::Extension {
                phase: ExtensionPhase::AddOn {
                    operation: "queue settings"
                },
                ..
            }
        ),
        "{error}"
    );

    let pilot = LifetimePilot::new(ShutdownBehavior::Succeed);
    let client = builder_with(builder(), &pilot, &gate)
        .queue(
            "default",
            fast_queue(1).with_extension_setting("limit", json!(1)),
        )
        .build()
        .unwrap();
    for rejected in [
        client.local_queues().add(
            "other",
            fast_queue(1).with_extension_setting("limit", json!("x")),
        ),
        client.local_queues().update(
            "default",
            fast_queue(1).with_extension_setting("limit", json!("x")),
        ),
    ] {
        assert!(
            matches!(
                rejected,
                Err(Error::Extension {
                    phase: ExtensionPhase::AddOn {
                        operation: "queue settings"
                    },
                    ..
                })
            ),
            "{rejected:?}"
        );
    }
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();
    client
        .local_queues()
        .update(
            "default",
            fast_queue(1).with_extension_setting("limit", json!(2)),
        )
        .unwrap();
    pilot
        .calls
        .wait_until("the updated settings", |calls| {
            calls.iter().any(|call| {
                matches!(call, Call::Configured(configuration)
                    if configuration.settings.get("limit") == Some(&json!(2)))
            })
        })
        .await;
    run.stop().await.unwrap();
    let Some(Call::Started(started)) = pilot.calls.snapshot().first().cloned() else {
        panic!("session never started");
    };
    assert_eq!(started.settings.get("limit"), Some(&json!(1)));
}

#[cfg(all(feature = "postgres", river_postgres_tests))]
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
    async fn callback_panics_stop_the_client_in_order() {
        for callback in ["configuration_changed", "job_finished"] {
            let schema = PostgresSchema::new("lifetime_panic").await;
            assert_callback_panics_stop_the_client_in_order(|| builder(&schema), callback).await;
            schema.cleanup().await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stuck_keep_alives_time_out() {
        let schema = PostgresSchema::new("lifetime_stuck_report").await;
        assert_stuck_keep_alives_time_out(builder(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn queue_settings_reach_the_session() {
        let schema = PostgresSchema::new("lifetime_settings").await;
        assert_queue_settings_reach_the_session(|| builder(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn removal_waits_for_the_drain() {
        let schema = PostgresSchema::new("lifetime_removal").await;
        assert_removal_waits_for_the_drain(builder(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reports_continue_through_the_drain() {
        let schema = PostgresSchema::new("lifetime_drain").await;
        assert_reports_continue_through_the_drain(builder(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_retries_with_growing_deadlines() {
        let schema = PostgresSchema::new("lifetime_shutdown").await;
        assert_shutdown_retries_with_growing_deadlines(builder(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn updates_apply_while_running() {
        let schema = PostgresSchema::new("lifetime_update").await;
        assert_updates_apply_while_running(builder(&schema)).await;
        schema.cleanup().await;
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    use crate::support::{sqlite_cleanup, sqlite_file_pool};

    #[tokio::test(flavor = "multi_thread")]
    async fn callback_panics_stop_the_client_in_order() {
        for callback in ["configuration_changed", "job_finished"] {
            let (pool, path) = sqlite_file_pool(4).await;
            assert_callback_panics_stop_the_client_in_order(
                || Client::builder(pool.clone()),
                callback,
            )
            .await;
            sqlite_cleanup(pool, path).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stuck_keep_alives_time_out() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_stuck_keep_alives_time_out(Client::builder(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn queue_settings_reach_the_session() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_queue_settings_reach_the_session(|| Client::builder(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn removal_waits_for_the_drain() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_removal_waits_for_the_drain(Client::builder(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reports_continue_through_the_drain() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_reports_continue_through_the_drain(Client::builder(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_retries_with_growing_deadlines() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_shutdown_retries_with_growing_deadlines(Client::builder(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn updates_apply_while_running() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_updates_apply_while_running(Client::builder(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }
}
