//! Extension producer sessions: claims the session owns, the protocol checks
//! River applies to what it returns, per-attempt accounting through
//! `job_finished`, and configuration changes.
//!
//! PostgreSQL scenarios run in a unique schema and fail rather than skip when
//! `RIVER_RUST_DATABASE_URL` is unset; SQLite scenarios use temporary files.

#![cfg(any(feature = "postgres-tests", feature = "sqlite"))]

mod support;

use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use riverqueue::__private::{
    ClaimedJob, ClientBuilderExt, DatabaseConnection, Pilot, PilotError, PilotProducer,
    ProducerClaimContext, ProducerClaimNext, ProducerConfiguration, ProducerStartContext,
};
use riverqueue::{
    Client, Error, ExtensionPhase, InsertOpts, Job, JobArgs, JobRow, JobState, QueueConfig,
    QueueUpdateParams, WorkContext, WorkOutcome, WorkerRegistry,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "producer_session")]
struct SessionArgs {
    fail: bool,
}

/// A job whose worker blocks its thread until released, ignoring
/// cancellation.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "producer_session_blocking")]
struct BlockingArgs {}

fn fast_queue(max_workers: usize) -> QueueConfig {
    QueueConfig::new(max_workers)
        .with_fetch_cooldown(Duration::from_millis(1))
        .with_fetch_poll_interval(Duration::from_millis(10))
}

fn workers() -> WorkerRegistry {
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(|_context: WorkContext, job: Job<SessionArgs>| async move {
            if job.args.fail {
                return Err(std::io::Error::other("failed on purpose"));
            }
            Ok(WorkOutcome::Complete)
        })
        .unwrap();
    workers
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

/// How [`SessionPilot`]'s sessions claim.
#[derive(Clone, Copy, Debug)]
enum Claim {
    /// River's claim in the session's own transaction.
    Standard,
    /// River's claim, returning its first job twice.
    Duplicate,
    /// River's claim, reporting its first job as still available.
    NotRunning,
    /// River's claim, reporting its first job in another queue.
    WrongQueue,
    /// River's claim, reporting its first job as last attempted by another
    /// client.
    ForeignClient,
    /// River's claim, padded with made-up jobs past the claim's limit.
    OverLimit,
    /// Available rows selected with River's projection but never claimed,
    /// decoded as far as possible.
    Unclaimed,
    /// A row without River's columns, which can't be identified.
    Unidentifiable,
}

/// Starts a session for every producer generation and records what River
/// tells it.
#[derive(Clone)]
struct SessionPilot {
    claim: Claim,
    configurations: Arc<Recorder<ProducerConfiguration>>,
    finished: Arc<Recorder<i64>>,
}

impl SessionPilot {
    fn new(claim: Claim) -> Self {
        Self {
            claim,
            configurations: Arc::default(),
            finished: Arc::default(),
        }
    }
}

#[async_trait]
impl Pilot for SessionPilot {
    async fn start_producer(
        &self,
        context: ProducerStartContext,
    ) -> Result<Option<Box<dyn PilotProducer>>, PilotError> {
        self.configurations.push(context.configuration);
        Ok(Some(Box::new(self.clone())))
    }
}

#[async_trait]
impl PilotProducer for SessionPilot {
    fn intercepts_claim(&self) -> bool {
        true
    }

    async fn claim(
        &self,
        context: ProducerClaimContext<'_>,
        next: ProducerClaimNext<'_>,
    ) -> Result<Vec<ClaimedJob>, PilotError> {
        let mut transaction = context.database.begin().await?;
        if matches!(self.claim, Claim::Unclaimed | Claim::Unidentifiable) {
            let jobs = raw_claim(self.claim, &mut transaction).await?;
            transaction.commit().await?;
            return Ok(jobs);
        }
        let mut jobs = next.claim(transaction.connection()).await?;
        transaction.commit().await?;
        let first = jobs.first().and_then(ClaimedJob::job).cloned();
        match (self.claim, first) {
            (Claim::Duplicate, Some(first)) => jobs.push(first.into()),
            (Claim::NotRunning, Some(mut first)) => {
                first.state = JobState::Available;
                jobs[0] = first.into();
            }
            (Claim::WrongQueue, Some(mut first)) => {
                "elsewhere".clone_into(&mut first.queue);
                jobs[0] = first.into();
            }
            (Claim::ForeignClient, Some(mut first)) => {
                first.attempted_by.push("another-client".to_owned());
                jobs[0] = first.into();
            }
            (Claim::OverLimit, Some(first)) => {
                for offset in 1..=i64::try_from(context.limit)? {
                    let mut extra = first.clone();
                    extra.id += offset * 1_000_000;
                    jobs.push(extra.into());
                }
            }
            _ => {}
        }
        Ok(jobs)
    }

    fn configuration_changed(&self, configuration: &ProducerConfiguration) {
        self.configurations.push(configuration.clone());
    }

    fn job_finished(&self, job: &JobRow) {
        self.finished.push(job.id);
    }
}

/// Selects rows for [`Claim::Unclaimed`] or [`Claim::Unidentifiable`]
/// without claiming anything.
async fn raw_claim(
    claim: Claim,
    transaction: &mut riverqueue::__private::PilotTransaction,
) -> Result<Vec<ClaimedJob>, PilotError> {
    let unclaimed = matches!(claim, Claim::Unclaimed);
    match transaction.connection() {
        #[cfg(feature = "postgres")]
        DatabaseConnection::Postgres(connection) => {
            let sql = if unclaimed {
                format!(
                    "SELECT {}, false AS unique_skipped_as_duplicate FROM river_job AS job \
                     WHERE state = 'available'",
                    riverqueue::__private::postgres_job_projection("job")
                )
            } else {
                "SELECT 1 AS id".to_owned()
            };
            let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
                .fetch_all(connection)
                .await?;
            Ok(rows
                .iter()
                .map(riverqueue::__private::claimed_postgres_job)
                .collect())
        }
        #[cfg(feature = "sqlite")]
        DatabaseConnection::Sqlite(connection) => {
            let sql = if unclaimed {
                format!(
                    "SELECT {} FROM river_job WHERE state = 'available'",
                    riverqueue::__private::SQLITE_JOB_COLUMNS
                )
            } else {
                "SELECT 1 AS id".to_owned()
            };
            let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
                .fetch_all(connection)
                .await?;
            Ok(rows
                .iter()
                .map(riverqueue::__private::claimed_sqlite_job)
                .collect())
        }
        #[allow(unreachable_patterns)]
        _ => unreachable!("built-in backends only"),
    }
}

fn counts(ids: &[i64]) -> HashMap<i64, usize> {
    let mut counts = HashMap::new();
    for id in ids {
        *counts.entry(*id).or_default() += 1;
    }
    counts
}

/// Every accepted claimed row reaches `job_finished` exactly once, whatever
/// its attempt's outcome.
async fn assert_every_attempt_finishes_once(builder: riverqueue::ClientBuilder) {
    let pilot = SessionPilot::new(Claim::Standard);
    let client = builder
        .pilot(pilot.clone())
        .queue("default", fast_queue(3))
        .workers(workers())
        .build()
        .unwrap();
    let mut ids = Vec::new();
    for fail in [false, true, false, false, true] {
        // A failed job is discarded, so it can't run twice.
        ids.push(
            client
                .insert(SessionArgs { fail })
                .opts(InsertOpts::default().with_max_attempts(1))
                .await
                .unwrap()
                .id(),
        );
    }
    let mut run = client.start().unwrap();
    pilot
        .finished
        .wait_until("every attempt to finish", |finished| {
            ids.iter().all(|id| finished.contains(id))
        })
        .await;
    run.shutdown().await.unwrap();

    let finished = counts(&pilot.finished.snapshot());
    assert_eq!(finished.len(), ids.len(), "{finished:?}");
    assert!(finished.values().all(|count| *count == 1), "{finished:?}");
    for (index, id) in ids.iter().enumerate() {
        let expected = if matches!(index, 1 | 4) {
            JobState::Discarded
        } else {
            JobState::Completed
        };
        assert_eq!(client.jobs().get(*id).await.unwrap().state, expected);
    }
}

/// An attempt whose worker outlives its abort during shutdown leaves its job
/// running for the rescuer, but still finishes in the session, so the
/// extension doesn't count it against the queue for the rest of the run.
async fn assert_abandoned_attempts_finish(builder: riverqueue::ClientBuilder) {
    let pilot = SessionPilot::new(Claim::Standard);
    let (release, blocked) = std::sync::mpsc::channel::<()>();
    let blocked = Arc::new(Mutex::new(blocked));
    let started = Arc::new(Notify::new());
    let mut workers = WorkerRegistry::new();
    let worker_started = Arc::clone(&started);
    workers
        .register_fn(move |_context: WorkContext, _job: Job<BlockingArgs>| {
            let blocked = Arc::clone(&blocked);
            let started = Arc::clone(&worker_started);
            async move {
                started.notify_one();
                // Blocks the worker's thread, so neither cancellation nor
                // an abort can end it.
                let _ = blocked.lock().unwrap().recv();
                Ok::<_, Infallible>(WorkOutcome::Complete)
            }
        })
        .unwrap();
    let client = builder
        .pilot(pilot.clone())
        .job_stuck_threshold(Duration::from_millis(50))
        .queue("default", fast_queue(1))
        .workers(workers)
        .build()
        .unwrap();
    let id = client.insert(BlockingArgs {}).await.unwrap().id();
    let mut run = client.start().unwrap();
    tokio::time::timeout(WAIT, started.notified())
        .await
        .expect("worker starts");

    tokio::time::timeout(WAIT, run.shutdown_now())
        .await
        .expect("client stops")
        .unwrap();
    assert_eq!(pilot.finished.snapshot(), [id]);
    assert_eq!(
        client.jobs().get(id).await.unwrap().state,
        JobState::Running
    );
    release.send(()).unwrap();
}

/// Panics while handling a failed job, which ends its attempt's task.
struct PanickingErrorHandler;

impl riverqueue::ErrorHandler for PanickingErrorHandler {
    async fn handle_error(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        _result: &riverqueue::WorkResult,
    ) -> Result<riverqueue::ErrorHandlerDecision, riverqueue::BoxError> {
        tokio::task::yield_now().await;
        panic!("error handler panicked on purpose")
    }
}

/// An attempt whose task panics, here in the client's error handler, still
/// finishes in the session.
async fn assert_panicked_attempts_finish(builder: riverqueue::ClientBuilder) {
    let pilot = SessionPilot::new(Claim::Standard);
    let client = builder
        .pilot(pilot.clone())
        .error_handler(PanickingErrorHandler)
        .queue("default", fast_queue(1))
        .workers(workers())
        .build()
        .unwrap();
    let id = client
        .insert(SessionArgs { fail: true })
        .await
        .unwrap()
        .id();
    let mut run = client.start().unwrap();
    pilot
        .finished
        .wait_until("the panicked attempt to finish", |finished| {
            finished.contains(&id)
        })
        .await;
    run.shutdown().await.unwrap();
    assert_eq!(pilot.finished.snapshot(), [id]);
}

/// A session result River can't accept stops the client with a protocol
/// error, and its rows never reach `job_finished`.
///
/// `corrupt` runs on the inserted job before the client starts, to make it
/// undecodable for [`Claim::Unclaimed`].
async fn assert_broken_claims_stop_the_client<F, Fut>(
    builder: impl Fn() -> riverqueue::ClientBuilder,
    claim: Claim,
    corrupt: F,
) where
    F: FnOnce(i64) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let pilot = SessionPilot::new(claim);
    let client = builder()
        .pilot(pilot.clone())
        .queue("default", fast_queue(2))
        .workers(workers())
        .build()
        .unwrap();
    let id = client
        .insert(SessionArgs { fail: false })
        .await
        .unwrap()
        .id();
    if matches!(claim, Claim::Unclaimed) {
        corrupt(id).await;
    }
    let mut run = client.start().unwrap();
    let error = tokio::time::timeout(WAIT, run.wait())
        .await
        .expect("client stops")
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::Extension {
                phase: ExtensionPhase::AddOnFetchClaim,
                ..
            }
        ),
        "{claim:?}: {error}"
    );
    assert!(pilot.finished.snapshot().is_empty(), "{claim:?}");
    // A committed claim is left for the rescuer. (River can't read a
    // corrupted row back.)
    if matches!(claim, Claim::Unclaimed) {
        return;
    }
    let expected = if matches!(claim, Claim::Unidentifiable) {
        JobState::Available
    } else {
        JobState::Running
    };
    assert_eq!(
        client.jobs().get(id).await.unwrap().state,
        expected,
        "{claim:?}"
    );
}

/// Every way a claim result can break the protocol.
const BROKEN_CLAIMS: [Claim; 7] = [
    Claim::Duplicate,
    Claim::NotRunning,
    Claim::WrongQueue,
    Claim::ForeignClient,
    Claim::OverLimit,
    Claim::Unclaimed,
    Claim::Unidentifiable,
];

/// Queue record changes reach the session at once, as Go's producer handles
/// `metadata_changed`, rather than at the next queue poll two seconds
/// later. A listening client learns of another client's update through the
/// control notification; a poll-only client learns of its own update through
/// a local signal.
///
/// Two consecutive updates must each arrive within 1.5 seconds. A poll could
/// catch the first by chance, but the second is made right after that poll,
/// so it would wait almost the whole interval.
async fn assert_queue_changes_reach_the_session(
    builder: impl Fn() -> riverqueue::ClientBuilder,
    poll_only: bool,
) {
    let pilot = SessionPilot::new(Claim::Standard);
    let queue = if poll_only { "poll_only" } else { "listening" };
    let mut client_builder = builder()
        .pilot(pilot.clone())
        .queue(queue, fast_queue(4))
        .workers(workers());
    if poll_only {
        client_builder = client_builder.without_notifications();
    }
    let client = client_builder.build().unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();
    let started = pilot.configurations.snapshot();
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].max_workers, 4);
    assert_eq!(started[0].queue.name, queue);

    let updater = if poll_only {
        client.clone()
    } else {
        builder().build().unwrap()
    };
    for value in 1..=2 {
        let metadata = json!({"value": value});
        let Value::Object(map) = metadata.clone() else {
            unreachable!()
        };
        updater
            .queues()
            .update(queue, QueueUpdateParams::new().metadata(map.clone()))
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_millis(1500),
            pilot
                .configurations
                .wait_until("the metadata change", |configurations| {
                    configurations
                        .last()
                        .is_some_and(|configuration| configuration.queue.metadata == map)
                }),
        )
        .await
        .unwrap_or_else(|_| panic!("metadata {metadata} not reported in time"));
    }
    run.shutdown().await.unwrap();
}

#[cfg(feature = "postgres-tests")]
mod postgres {
    use riverqueue::database::PostgresDatabase;

    use super::*;
    use crate::support::PostgresSchema;

    fn builder(schema: &PostgresSchema) -> riverqueue::ClientBuilder {
        Client::builder(PostgresDatabase::new(schema.pool.clone()).schema(schema.schema.clone()))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn panicked_attempts_finish() {
        let schema = PostgresSchema::new("session_panicked").await;
        assert_panicked_attempts_finish(builder(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn abandoned_attempts_finish() {
        let schema = PostgresSchema::new("session_abandoned").await;
        assert_abandoned_attempts_finish(builder(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn broken_claims_stop_the_client() {
        for claim in BROKEN_CLAIMS {
            // The current schema, so the unqualified raw claims find it.
            let schema = PostgresSchema::current("session_broken").await;
            // Metadata that isn't an object can't be decoded.
            let (pool, table) = (schema.pool.clone(), schema.table("river_job"));
            assert_broken_claims_stop_the_client(
                || builder(&schema),
                claim,
                |id| async move {
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "UPDATE {table} SET metadata = '[1]'::jsonb WHERE id = $1"
                    )))
                    .bind(id)
                    .execute(&pool)
                    .await
                    .unwrap();
                },
            )
            .await;
            schema.cleanup().await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_attempt_finishes_once() {
        let schema = PostgresSchema::new("session_finished").await;
        assert_every_attempt_finishes_once(builder(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn queue_changes_reach_the_session() {
        let schema = PostgresSchema::new("session_config").await;
        assert_queue_changes_reach_the_session(|| builder(&schema), false).await;
        assert_queue_changes_reach_the_session(|| builder(&schema), true).await;
        schema.cleanup().await;
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    use crate::support::{sqlite_cleanup, sqlite_file_pool};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn abandoned_attempts_finish() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_abandoned_attempts_finish(Client::builder(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn broken_claims_stop_the_client() {
        for claim in BROKEN_CLAIMS {
            let (pool, path) = sqlite_file_pool(4).await;
            // Tags that aren't an array can't be decoded.
            let corrupt_pool = pool.clone();
            assert_broken_claims_stop_the_client(
                || Client::builder(pool.clone()),
                claim,
                |id| async move {
                    sqlx::query(
                        "UPDATE river_job SET tags = jsonb('{\"not\":\"an array\"}') WHERE id = ?",
                    )
                    .bind(id)
                    .execute(&corrupt_pool)
                    .await
                    .unwrap();
                },
            )
            .await;
            sqlite_cleanup(pool, path).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn panicked_attempts_finish() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_panicked_attempts_finish(Client::builder(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_attempt_finishes_once() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_every_attempt_finishes_once(Client::builder(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn queue_changes_reach_the_session() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_queue_changes_reach_the_session(|| Client::builder(pool.clone()), false).await;
        assert_queue_changes_reach_the_session(|| Client::builder(pool.clone()), true).await;
        sqlite_cleanup(pool, path).await;
    }
}
