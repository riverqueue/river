//! PostgreSQL tests of individual maintenance services, ported from Go's
//! `internal/maintenance` and `internal/leadership` suites. Each test uses its
//! own freshly migrated schema and fails when `RIVER_RUST_DATABASE_URL` is
//! unset.

use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, PgPool, postgres::PgPoolOptions};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::__private::{
    DatabaseConnection, JobUpdatedParams, Pilot, PilotError, RescueAction, RescueManyParams,
};
use riverqueue_migrate::PostgresMigrator;

use super::{
    BatchSizes, Breakers, cleaner,
    elector::{DatabaseLeaderStore, Elector, ElectorEvent},
    maintainer::ServiceContext,
    rescuer, scheduler,
};
use crate::{
    Client, Job, JobArgs, JobState, MaintenanceConfig, QueueConfig, SchemaName, UniqueOpts,
    WorkContext, WorkOutcome, Worker, WorkerRegistry, WorkerTimeout,
    database::{PostgresDatabase, PostgresReindexConfig},
};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "maintenance_no_timeout")]
struct NoTimeoutArgs {}

struct NoTimeoutWorker;

impl Worker<NoTimeoutArgs> for NoTimeoutWorker {
    type Error = Infallible;

    fn work(
        &self,
        _context: WorkContext,
        _job: Job<NoTimeoutArgs>,
    ) -> impl Future<Output = Result<WorkOutcome, Self::Error>> + Send {
        std::future::ready(Ok(WorkOutcome::Complete))
    }

    fn timeout(&self, _job: &Job<NoTimeoutArgs>) -> WorkerTimeout {
        WorkerTimeout::Disabled
    }
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "maintenance_short_timeout")]
struct ShortTimeoutArgs {}

struct ShortTimeoutWorker;

impl Worker<ShortTimeoutArgs> for ShortTimeoutWorker {
    type Error = Infallible;

    fn work(
        &self,
        _context: WorkContext,
        _job: Job<ShortTimeoutArgs>,
    ) -> impl Future<Output = Result<WorkOutcome, Self::Error>> + Send {
        std::future::ready(Ok(WorkOutcome::Complete))
    }

    fn timeout(&self, _job: &Job<ShortTimeoutArgs>) -> WorkerTimeout {
        WorkerTimeout::After(Duration::from_millis(1))
    }
}

fn workers() -> WorkerRegistry {
    let mut workers = WorkerRegistry::new();
    workers
        .register::<NoTimeoutArgs, _>(NoTimeoutWorker)
        .unwrap();
    workers
        .register::<ShortTimeoutArgs, _>(ShortTimeoutWorker)
        .unwrap();
    workers
}

/// A migrated schema owned by one test.
struct TestDatabase {
    pool: PgPool,
    schema: SchemaName,
    name: String,
}

impl TestDatabase {
    async fn new(prefix: &str) -> Self {
        static NONCE: AtomicUsize = AtomicUsize::new(0);
        let url = std::env::var("RIVER_RUST_DATABASE_URL")
            .expect("RIVER_RUST_DATABASE_URL must point at a disposable test database");
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(&url)
            .await
            .expect("connect to RIVER_RUST_DATABASE_URL");
        let mut name = format!(
            "{prefix}_{:x}_{:x}_{:x}",
            std::process::id(),
            NONCE.fetch_add(1, Ordering::Relaxed),
            Utc::now().timestamp_subsec_nanos()
        );
        name.truncate(riverqueue_migrate::SCHEMA_MAX_LEN);
        sqlx::raw_sql(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .unwrap();
        let schema = SchemaName::new(name.clone()).unwrap();
        PostgresMigrator::new(pool.clone())
            .with_schema(schema.clone())
            .migrate_up()
            .await
            .unwrap();
        Self { pool, schema, name }
    }

    fn table(&self, table: &str) -> String {
        self.schema.qualify(table)
    }

    fn client(&self) -> crate::ClientBuilder {
        Client::builder(
            PostgresDatabase::new(self.pool.clone())
                .schema(self.schema.clone())
                .reindex(PostgresReindexConfig::default().with_index_names([] as [&str; 0])),
        )
        .workers(workers())
    }

    /// Inserts a raw job row and returns its ID.
    async fn insert_job(&self, job: RawJob<'_>) -> i64 {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "INSERT INTO {} (args, attempt, attempted_at, finalized_at, kind, max_attempts, \
                metadata, queue, scheduled_at, state, unique_key, unique_states) \
             VALUES ('{{}}', $1, $2, $3, $4, $5, $6, $7, coalesce($8, now()), $9::text::{}, $10, \
                CASE WHEN $10 IS NULL THEN NULL ELSE $11::int::bit(8) END) RETURNING id",
            self.table("river_job"),
            self.schema.qualify("river_job_state"),
        )))
        .bind(job.attempt)
        .bind(job.attempted_at)
        .bind(job.finalized_at)
        .bind(job.kind)
        .bind(job.max_attempts)
        .bind(sqlx::types::Json(job.metadata))
        .bind(job.queue)
        .bind(job.scheduled_at)
        .bind(job.state)
        .bind(job.unique_key)
        .bind(job.unique_states)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn job(&self, id: i64) -> Option<(String, i32, serde_json::Value)> {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT state::text, coalesce(array_length(errors, 1), 0), metadata FROM {} WHERE id = $1",
            self.table("river_job")
        )))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .unwrap()
    }

    async fn state(&self, id: i64) -> Option<String> {
        self.job(id).await.map(|(state, _, _)| state)
    }

    async fn cleanup(self) {
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.name
        )))
        .execute(&self.pool)
        .await
        .unwrap();
        self.pool.close().await;
    }
}

struct RawJob<'a> {
    attempt: i16,
    attempted_at: Option<DateTime<Utc>>,
    finalized_at: Option<DateTime<Utc>>,
    kind: &'a str,
    max_attempts: i16,
    metadata: serde_json::Value,
    queue: &'a str,
    scheduled_at: Option<DateTime<Utc>>,
    state: &'a str,
    unique_key: Option<Vec<u8>>,
    /// Unique-state bitmask; defaults to every state when a key is set.
    unique_states: i32,
}

impl Default for RawJob<'_> {
    fn default() -> Self {
        Self {
            attempt: 0,
            attempted_at: None,
            finalized_at: None,
            kind: NoTimeoutArgs::KIND,
            max_attempts: 25,
            metadata: serde_json::json!({}),
            queue: "default",
            scheduled_at: None,
            state: "available",
            unique_key: None,
            unique_states: 0xFF,
        }
    }
}

fn hours_ago(hours: i64) -> DateTime<Utc> {
    Utc::now() - chrono::Duration::hours(hours)
}

fn stuck(kind: &str) -> RawJob<'_> {
    RawJob {
        attempt: 1,
        attempted_at: Some(hours_ago(3)),
        kind,
        state: "running",
        ..RawJob::default()
    }
}

fn context(client: &Client, batch: i64) -> ServiceContext {
    ServiceContext {
        breakers: Arc::new(Breakers::new(BatchSizes {
            default: batch,
            reduced: 1,
        })),
        cancel: CancellationToken::new(),
        inner: Arc::clone(&client.inner),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rescuer_rescues_past_full_batch_of_jobs_with_no_timeout() {
    let database = TestDatabase::new("rmt_rescue_batch").await;
    let client = database.client().build().unwrap();

    // A full batch of stuck jobs whose timeout is disabled must not stop the
    // rescuer from reaching later eligible jobs (Go
    // `RescuesPastFullBatchOfJobsWithNoTimeout`).
    let mut ignored = Vec::new();
    for _ in 0..6 {
        ignored.push(database.insert_job(stuck(NoTimeoutArgs::KIND)).await);
    }
    let eligible = database.insert_job(stuck(ShortTimeoutArgs::KIND)).await;

    tokio::time::timeout(
        Duration::from_secs(20),
        rescuer::run_once(&context(&client, 3)),
    )
    .await
    .expect("rescuer must not livelock on a full batch of ignored jobs")
    .unwrap();

    assert_eq!(database.state(eligible).await.as_deref(), Some("retryable"));
    for id in ignored {
        let (state, errors, metadata) = database.job(id).await.unwrap();
        assert_eq!(state, "running");
        assert_eq!(errors, 0);
        assert!(metadata.get("river:rescue_count").is_none());
    }
    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rescuer_rescues_undecodable_stuck_jobs() {
    let database = TestDatabase::new("rmt_rescue_undecodable").await;
    let client = database.client().build().unwrap();

    // Like River Go's `JobGetStuck`, a stuck job whose row can't be fully
    // decoded is still read, so neither it nor the jobs read with it are
    // stranded.
    let undecodable = database
        .insert_job(RawJob {
            metadata: serde_json::json!([1]),
            ..stuck(ShortTimeoutArgs::KIND)
        })
        .await;
    let decodable = database.insert_job(stuck(ShortTimeoutArgs::KIND)).await;

    rescuer::run_once(&context(&client, 100)).await.unwrap();

    for id in [undecodable, decodable] {
        let (state, errors, _) = database.job(id).await.unwrap();
        assert_eq!((state.as_str(), errors), ("retryable", 1), "job {id}");
    }
    database.cleanup().await;
}

/// Mutates selected jobs inside the rescue transaction to simulate workers
/// that finish or re-claim jobs after selection, then lets OSS continue.
struct StaleSnapshotPilot {
    completed: Arc<std::sync::Mutex<Vec<i64>>>,
    handled: bool,
    reclaimed: Arc<std::sync::Mutex<Vec<i64>>>,
    schema: SchemaName,
}

#[async_trait]
impl Pilot for StaleSnapshotPilot {
    fn intercepts_rescue(&self) -> bool {
        true
    }

    async fn rescue_jobs(
        &self,
        connection: DatabaseConnection<'_>,
        params: &RescueManyParams,
    ) -> Result<RescueAction, PilotError> {
        let connection = connection.into_postgres().expect("PostgreSQL connection");
        let completed = self.completed.lock().unwrap().clone();
        let reclaimed = self.reclaimed.lock().unwrap().clone();
        assert!(params.jobs.iter().any(|job| completed.contains(&job.id)));
        let table = self.schema.qualify("river_job");
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {table} SET state = 'completed', finalized_at = now() WHERE id = ANY($1)"
        )))
        .bind(&completed)
        .execute(&mut *connection)
        .await?;
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {table} SET attempt = attempt + 1, attempted_at = now() WHERE id = ANY($1)"
        )))
        .bind(&reclaimed)
        .execute(&mut *connection)
        .await?;
        Ok(if self.handled {
            RescueAction::Handled
        } else {
            RescueAction::Continue
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rescuer_update_is_guarded_against_stale_selection() {
    for handled in [false, true] {
        let database = TestDatabase::new("rmt_rescue_stale").await;
        let completed = database.insert_job(stuck(ShortTimeoutArgs::KIND)).await;
        let reclaimed = database.insert_job(stuck(ShortTimeoutArgs::KIND)).await;
        let eligible = database.insert_job(stuck(ShortTimeoutArgs::KIND)).await;
        let client = database
            .client()
            .with_pilot(StaleSnapshotPilot {
                completed: Arc::new(std::sync::Mutex::new(vec![completed])),
                handled,
                reclaimed: Arc::new(std::sync::Mutex::new(vec![reclaimed])),
                schema: database.schema.clone(),
            })
            .build()
            .unwrap();

        rescuer::run_once(&context(&client, 100)).await.unwrap();

        // Jobs completed or claimed again after selection keep their new
        // state, errors, and metadata.
        let (state, errors, metadata) = database.job(completed).await.unwrap();
        assert_eq!((state.as_str(), errors), ("completed", 0));
        assert!(metadata.get("river:rescue_count").is_none());
        let (state, errors, _) = database.job(reclaimed).await.unwrap();
        assert_eq!((state.as_str(), errors), ("running", 0));
        // An extension that handles the rescue suppresses the OSS update.
        let expected = if handled { "running" } else { "retryable" };
        assert_eq!(database.state(eligible).await.as_deref(), Some(expected));
        database.cleanup().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn scheduler_look_ahead_and_unique_conflicts() {
    let database = TestDatabase::new("rmt_scheduler").await;
    let client = database
        .client()
        .maintenance(MaintenanceConfig::default().with_scheduler_interval(Duration::from_secs(5)))
        .build()
        .unwrap();

    let due = database
        .insert_job(RawJob {
            scheduled_at: Some(hours_ago(1)),
            state: "scheduled",
            ..RawJob::default()
        })
        .await;
    let look_ahead = database
        .insert_job(RawJob {
            scheduled_at: Some(Utc::now() + chrono::Duration::seconds(2)),
            state: "retryable",
            ..RawJob::default()
        })
        .await;
    let later = database
        .insert_job(RawJob {
            scheduled_at: Some(Utc::now() + chrono::Duration::minutes(5)),
            state: "scheduled",
            ..RawJob::default()
        })
        .await;
    // Retryable jobs whose unique key is held by another job in a unique
    // state are discarded instead of made available (Go
    // `MovesUniqueKeyConflictingJobsToDiscarded`). Like Go's test, unique
    // states are the defaults without `retryable` (available, completed,
    // pending, running, scheduled) so the duplicates can exist while waiting.
    let without_retryable = 0b1101_0101;
    let mut non_conflicting = Vec::new();
    for key in [1_u8, 2] {
        non_conflicting.push(
            database
                .insert_job(RawJob {
                    scheduled_at: Some(hours_ago(1)),
                    state: "retryable",
                    unique_key: Some(vec![key; 32]),
                    unique_states: without_retryable,
                    ..RawJob::default()
                })
                .await,
        );
    }
    let mut conflicting = Vec::new();
    for (key, holder_state) in [
        (3_u8, "available"),
        (4, "completed"),
        (5, "pending"),
        (6, "running"),
        (7, "scheduled"),
    ] {
        conflicting.push(
            database
                .insert_job(RawJob {
                    scheduled_at: Some(hours_ago(1)),
                    state: "retryable",
                    unique_key: Some(vec![key; 32]),
                    unique_states: without_retryable,
                    ..RawJob::default()
                })
                .await,
        );
        database
            .insert_job(RawJob {
                attempt: i16::from(holder_state == "running"),
                attempted_at: (holder_state == "running").then(Utc::now),
                finalized_at: (holder_state == "completed").then(Utc::now),
                scheduled_at: (holder_state == "scheduled")
                    .then(|| Utc::now() + chrono::Duration::hours(1)),
                state: holder_state,
                unique_key: Some(vec![key; 32]),
                unique_states: without_retryable,
                ..RawJob::default()
            })
            .await;
    }

    scheduler::run_once(&context(&client, 100)).await.unwrap();

    assert_eq!(database.state(due).await.as_deref(), Some("available"));
    assert_eq!(
        database.state(look_ahead).await.as_deref(),
        Some("available")
    );
    assert_eq!(database.state(later).await.as_deref(), Some("scheduled"));
    for id in non_conflicting {
        assert_eq!(database.state(id).await.as_deref(), Some("available"));
    }
    for id in conflicting {
        let (state, _, metadata) = database.job(id).await.unwrap();
        assert_eq!(state, "discarded");
        assert_eq!(metadata["unique_key_conflict"], "scheduler_discarded");
    }
    database.cleanup().await;
}

struct ExcludingPilot;

impl Pilot for ExcludingPilot {
    fn job_cleaner_queue_exclusions(&self) -> Vec<String> {
        vec!["extension_owned".to_owned()]
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn job_cleaner_retention_exclusions_and_batches() {
    let retentions = [
        (None, None, None),
        (None, Some(1), Some(1)),
        (Some(1), None, Some(1)),
        (Some(1), Some(1), None),
    ];
    for (cancelled, completed, discarded) in retentions {
        let database = TestDatabase::new("rmt_job_cleaner").await;
        let hours = |retention: Option<u64>| retention.map(Duration::from_hours);
        let client = database
            .client()
            .with_pilot(ExcludingPilot)
            .maintenance(
                MaintenanceConfig::default()
                    .with_cancelled_job_retention(hours(cancelled))
                    .with_completed_job_retention(hours(completed))
                    .with_discarded_job_retention(hours(discarded)),
            )
            .build()
            .unwrap();
        let mut expired = Vec::new();
        for state in ["cancelled", "completed", "discarded"] {
            for _ in 0..3 {
                expired.push((
                    state,
                    database
                        .insert_job(RawJob {
                            finalized_at: Some(hours_ago(2)),
                            state,
                            ..RawJob::default()
                        })
                        .await,
                ));
            }
        }
        let recent = database
            .insert_job(RawJob {
                finalized_at: Some(Utc::now()),
                state: "completed",
                ..RawJob::default()
            })
            .await;
        let excluded = database
            .insert_job(RawJob {
                finalized_at: Some(hours_ago(2)),
                queue: "extension_owned",
                state: "completed",
                ..RawJob::default()
            })
            .await;
        let running = database
            .insert_job(RawJob {
                attempt: 1,
                attempted_at: Some(hours_ago(2)),
                state: "running",
                ..RawJob::default()
            })
            .await;

        // A batch size of two forces several batches.
        cleaner::clean_jobs(&context(&client, 2)).await.unwrap();

        for (state, id) in expired {
            let retention = match state {
                "cancelled" => cancelled,
                "completed" => completed,
                _ => discarded,
            };
            assert_eq!(
                database.state(id).await.is_none(),
                retention.is_some(),
                "{state} job with retention {retention:?}"
            );
        }
        assert!(database.state(recent).await.is_some());
        assert!(database.state(excluded).await.is_some());
        assert!(database.state(running).await.is_some());
        database.cleanup().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_cleaner_keeps_active_queues() {
    let database = TestDatabase::new("rmt_queue_cleaner").await;
    let client = database.client().build().unwrap();
    for (name, age_hours) in [
        ("stale_a", 25),
        ("stale_b", 30),
        ("stale_c", 48),
        ("active", 0),
    ] {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {} (name, created_at, metadata, updated_at) \
             VALUES ($1, now(), '{{}}', now() - make_interval(hours => $2))",
            database.table("river_queue")
        )))
        .bind(name)
        .bind(age_hours)
        .execute(&database.pool)
        .await
        .unwrap();
    }

    cleaner::clean_queues(&context(&client, 2)).await.unwrap();

    let remaining: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT name FROM {} ORDER BY name",
        database.table("river_queue")
    )))
    .fetch_all(&database.pool)
    .await
    .unwrap();
    assert_eq!(remaining, ["active"]);
    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn elector_loses_leadership_when_same_id_term_is_replaced() {
    let database = TestDatabase::new("rmt_term_replaced").await;
    let client = database
        .client()
        .id("shared-leader-id")
        .maintenance(MaintenanceConfig::default().with_elect_interval(Duration::from_millis(25)))
        .build()
        .unwrap();
    let (events_sender, mut events) = mpsc::unbounded_channel();
    let (_wakeups_sender, wakeups) = mpsc::unbounded_channel();
    let (terms_sender, mut terms) = mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let elector = Elector::new(
        Arc::new(DatabaseLeaderStore::new(Arc::clone(&client.inner))),
        client.id().to_owned(),
        Duration::from_millis(25),
    )
    .with_events(events_sender);
    let run = tokio::spawn(elector.run(cancel.clone(), wakeups, terms_sender));
    let term = terms.recv().await.unwrap();

    // Another instance with the same ID replaces the term: same leader ID,
    // newer `elected_at`.
    let table = database.table("river_leader");
    let replaced_elected_at: DateTime<Utc> = sqlx::query_scalar(AssertSqlSafe(format!(
        "WITH removed AS (DELETE FROM {table} RETURNING leader_id, elected_at) \
         INSERT INTO {table} (leader_id, elected_at, expires_at) \
         SELECT leader_id, elected_at + interval '1 second', now() + interval '1 hour' FROM removed \
         RETURNING elected_at"
    )))
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert!(replaced_elected_at > term.elected_at);

    tokio::time::timeout(Duration::from_secs(10), term.token.cancelled())
        .await
        .expect("the replaced term must be given up");
    let mut observed = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            observed.push(event);
            if event == ElectorEvent::Denied {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(observed.contains(&ElectorEvent::Lost));
    assert!(!observed.contains(&ElectorEvent::Resigned));

    // The replacement term was neither renewed nor deleted by this client.
    let (elected_at, expires_in_minutes): (DateTime<Utc>, f64) =
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT elected_at, (extract(epoch FROM expires_at - now()) / 60)::float8 FROM {table}"
        )))
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(elected_at, replaced_elected_at);
    assert!(expires_in_minutes > 50.0);

    cancel.cancel();
    run.await.unwrap();
    database.cleanup().await;
}

#[derive(Clone, Default)]
struct HookPilot {
    cancels: Arc<std::sync::Mutex<Vec<(i64, String)>>>,
    retries: Arc<std::sync::Mutex<Vec<(i64, String)>>>,
    fail: bool,
}

#[async_trait]
impl Pilot for HookPilot {
    fn intercepts_job_cancel_retry(&self) -> bool {
        true
    }

    async fn after_job_cancel(
        &self,
        _connection: DatabaseConnection<'_>,
        job: &JobUpdatedParams,
    ) -> Result<(), PilotError> {
        self.cancels
            .lock()
            .unwrap()
            .push((job.job.id, job.job.state.as_str().to_owned()));
        if self.fail {
            return Err(std::io::Error::other("cancel hook failed").into());
        }
        Ok(())
    }

    async fn after_job_retry(
        &self,
        _connection: DatabaseConnection<'_>,
        job: &JobUpdatedParams,
    ) -> Result<(), PilotError> {
        self.retries
            .lock()
            .unwrap()
            .push((job.job.id, job.job.state.as_str().to_owned()));
        if self.fail {
            return Err(std::io::Error::other("retry hook failed").into());
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_and_retry_post_hooks_share_the_transaction() {
    let database = TestDatabase::new("rmt_cancel_retry").await;
    let pilot = HookPilot::default();
    let client = database.client().with_pilot(pilot.clone()).build().unwrap();
    let id = database.insert_job(RawJob::default()).await;

    let cancelled = client.job_cancel(id).await.unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert_eq!(
        *pilot.cancels.lock().unwrap(),
        [(id, "cancelled".to_owned())]
    );
    let retried = client.job_retry(id).await.unwrap();
    assert_eq!(retried.state, JobState::Available);
    assert_eq!(
        *pilot.retries.lock().unwrap(),
        [(id, "available".to_owned())]
    );

    // A failing hook rolls back the operation it follows.
    let failing = database
        .client()
        .with_pilot(HookPilot {
            fail: true,
            ..HookPilot::default()
        })
        .build()
        .unwrap();
    assert!(failing.job_cancel(id).await.is_err());
    assert_eq!(database.state(id).await.as_deref(), Some("available"));
    let mut transaction = database.pool.begin().await.unwrap();
    assert!(client.job_cancel_tx(&mut transaction, id).await.is_ok());
    transaction.rollback().await.unwrap();
    assert_eq!(database.state(id).await.as_deref(), Some("available"));
    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn periodic_jobs_use_expected_run_time_and_uniqueness() {
    use crate::{InsertOpts, IntervalSchedule, PeriodicJob};

    let database = TestDatabase::new("rmt_periodic").await;
    let client = database.client().build().unwrap();
    let periodic = client.periodic_jobs();
    periodic
        .add(PeriodicJob::conditional(
            IntervalSchedule::new(Duration::from_secs(60)).unwrap(),
            || {
                Some((
                    NoTimeoutArgs {},
                    InsertOpts::default().with_unique(UniqueOpts::new().by_args()),
                ))
            },
        ))
        .unwrap();

    let start = Utc::now();
    periodic.reset_for_leadership();
    periodic.run_due(&client, start).await;
    let target = periodic.next_run_at().unwrap();
    // Running a little before the target still inserts it, scheduled at the
    // expected run time rather than when the enqueuer woke up (Go
    // `SetsScheduledAtAccordingToExpectedNextRunAt`).
    periodic
        .run_due(&client, target - chrono::Duration::milliseconds(50))
        .await;
    let rows: Vec<DateTime<Utc>> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT scheduled_at FROM {}",
        database.table("river_job")
    )))
    .fetch_all(&database.pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].timestamp_micros(),
        target.timestamp_micros(),
        "periodic job scheduled at its expected run time"
    );

    // The next occurrence is unique by args with the first and is skipped
    // (Go `RespectsJobUniqueness`).
    let next = periodic.next_run_at().unwrap();
    assert!(next > target);
    periodic.run_due(&client, next).await;
    let count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {}",
        database.table("river_job")
    )))
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reindexer_skips_artifacts_and_drops_artifacts_when_cancelled() {
    let database = TestDatabase::new("rmt_reindexer").await;
    let table = database.table("river_job");
    for index in ["maint_reindex_artifact_idx", "maint_reindex_cancel_idx"] {
        sqlx::raw_sql(AssertSqlSafe(format!(
            "CREATE INDEX \"{index}\" ON {table} (kind)"
        )))
        .execute(&database.pool)
        .await
        .unwrap();
    }
    // A leftover of an earlier interrupted rebuild causes a skip.
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE INDEX \"maint_reindex_artifact_idx_ccnew1\" ON {table} (kind)"
    )))
    .execute(&database.pool)
    .await
    .unwrap();
    let filenode = |index: &'static str| {
        let pool = database.pool.clone();
        let schema = database.name.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT c.relfilenode::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2",
            )
            .bind(schema)
            .bind(index)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let artifact_before = filenode("maint_reindex_artifact_idx").await;
    let cancel = CancellationToken::new();
    assert!(
        !super::reindexer::reindex_one_for_test(
            &database.pool,
            &database.schema,
            &cancel,
            "maint_reindex_artifact_idx",
        )
        .await
        .unwrap()
    );
    assert_eq!(
        filenode("maint_reindex_artifact_idx").await,
        artifact_before
    );

    // A rebuild blocked behind an old snapshot is cancelled when the term
    // ends, and the concurrent-build artifact it created is dropped (Go
    // `ReindexDeletesArtifactsWhenCancelledWithStop`).
    let mut snapshot = database.pool.begin().await.unwrap();
    sqlx::raw_sql(AssertSqlSafe(format!(
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM {table}"
    )))
    .execute(&mut *snapshot)
    .await
    .unwrap();
    let rebuild = tokio::spawn({
        let pool = database.pool.clone();
        let schema = database.schema.clone();
        let cancel = cancel.clone();
        async move {
            super::reindexer::reindex_one_for_test(
                &pool,
                &schema,
                &cancel,
                "maint_reindex_cancel_idx",
            )
            .await
        }
    });
    let artifact_count = || {
        let pool = database.pool.clone();
        let schema = database.name.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname LIKE 'maint\\_reindex\\_cancel\\_idx\\_cc%'",
            )
            .bind(schema)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        while artifact_count().await == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the rebuild should create its concurrent artifact");
    cancel.cancel();
    snapshot.rollback().await.unwrap();
    assert!(rebuild.await.unwrap().is_err());
    assert_eq!(artifact_count().await, 0);
    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn periodic_start_hooks_and_run_on_start_follow_each_leadership_gain() {
    use crate::{Hook, IntervalSchedule, PeriodicJob, PeriodicJobOpts, PeriodicJobs};

    struct CountingHook(Arc<AtomicUsize>);

    #[allow(
        clippy::unused_async_trait_impl,
        reason = "the hook only counts starts"
    )]
    impl Hook for CountingHook {
        async fn periodic_jobs_start(&self, _jobs: &PeriodicJobs) -> Result<(), crate::BoxError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let database = TestDatabase::new("rmt_periodic_gain").await;
    let starts = Arc::new(AtomicUsize::new(0));
    let client = database
        .client()
        .hook(CountingHook(Arc::clone(&starts)))
        .maintenance(MaintenanceConfig::default().with_elect_interval(Duration::from_millis(50)))
        .periodic_job(PeriodicJob::with_options(
            IntervalSchedule::new(Duration::from_hours(1)).unwrap(),
            || NoTimeoutArgs {},
            PeriodicJobOpts::new().with_id("gain").run_on_start(),
        ))
        .queue("default", QueueConfig::new(1))
        .build()
        .unwrap();
    let periodic_count = || {
        let pool = database.pool.clone();
        let table = database.table("river_job");
        async move {
            sqlx::query_scalar::<_, i64>(AssertSqlSafe(format!(
                "SELECT count(*) FROM {table} WHERE metadata ->> 'river:periodic_job_id' = 'gain'"
            )))
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let wait_for = |expected: usize| {
        let starts = Arc::clone(&starts);
        async move {
            tokio::time::timeout(Duration::from_secs(10), async {
                while starts.load(Ordering::SeqCst) < expected
                    || periodic_count().await < i64::try_from(expected).unwrap()
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("expected {expected} leadership gains"));
        }
    };
    let mut handle = client.start().unwrap();
    wait_for(1).await;
    client.request_resign().await.unwrap();
    wait_for(2).await;
    handle.shutdown().await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(periodic_count().await, 2);
    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn maintenance_start_retries_then_requests_resignation() {
    use crate::{Hook, PeriodicJobs};

    // Fails the first three start attempts of every client, like Go's
    // `QueueMaintainerStartRetriesAndResigns`.
    struct FlakyHook(Arc<AtomicUsize>);

    #[allow(
        clippy::unused_async_trait_impl,
        reason = "the hook only counts starts"
    )]
    impl Hook for FlakyHook {
        async fn periodic_jobs_start(&self, _jobs: &PeriodicJobs) -> Result<(), crate::BoxError> {
            if self.0.fetch_add(1, Ordering::SeqCst) < 3 {
                return Err("start failed".into());
            }
            Ok(())
        }
    }

    let database = TestDatabase::new("rmt_start_retry").await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let client = database
        .client()
        .hook(FlakyHook(Arc::clone(&attempts)))
        .maintenance(MaintenanceConfig::default().with_elect_interval(Duration::from_millis(50)))
        .queue("default", QueueConfig::new(1))
        .build()
        .unwrap();
    let elected_at = || {
        let pool = database.pool.clone();
        let table = database.table("river_leader");
        async move {
            sqlx::query_scalar::<_, DateTime<Utc>>(AssertSqlSafe(format!(
                "SELECT elected_at FROM {table}"
            )))
            .fetch_optional(&pool)
            .await
            .unwrap()
        }
    };
    let mut handle = client.start().unwrap();
    let first_term = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(elected_at) = elected_at().await {
                return elected_at;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    // After three failed attempts the leader asks to resign, and the next
    // term's start succeeds.
    tokio::time::timeout(Duration::from_secs(20), async {
        while attempts.load(Ordering::SeqCst) < 4 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("maintenance start should be retried in a new term");
    let second_term = elected_at().await.unwrap();
    assert_ne!(second_term, first_term);
    handle.shutdown().await.unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 4);
    database.cleanup().await;
}
