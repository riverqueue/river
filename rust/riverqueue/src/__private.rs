//! Unstable extension points for River's own companion crates.
//!
//! Nothing in this module is part of River's public API. It changes without
//! notice between any two versions, so only crates released in lockstep with
//! `riverqueue` may use it.

#![allow(missing_docs)]

use std::{fmt, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Map, Value, value::RawValue};

#[cfg(feature = "postgres")]
use crate::database::SchemaName;
use crate::{AttemptError, InsertResult, Job, JobRow, JobState};
#[cfg(feature = "postgres")]
use sqlx::{PgConnection, PgPool};
#[cfg(feature = "sqlite")]
use sqlx::{SqliteConnection, SqlitePool};
use tokio_util::sync::CancellationToken;

pub use crate::client::{ExtensionClient, WeakClient};
pub use crate::database::erased::{Database, ErasedExecutor, ErasedTransaction};

/// Builder operations reserved for River's own companion crates.
pub trait ClientBuilderExt: Sized {
    /// Returns whether the client will stay out of leader election, as set
    /// by `ClientBuilder::without_leader_election`.
    ///
    /// Such a client never runs [`Pilot::maintenance_services`], and River
    /// rejects its own periodic jobs when it's built. A companion crate that
    /// configures leader-owned work of its own, such as additional periodic
    /// jobs, should reject that configuration the same way.
    fn leader_election_disabled(&self) -> bool;

    /// Installs a pilot from a companion crate.
    #[must_use]
    fn pilot<P: Pilot>(self, pilot: P) -> Self;
}

impl ClientBuilderExt for crate::ClientBuilder {
    fn leader_election_disabled(&self) -> bool {
        self.leader_election_disabled
    }

    fn pilot<P: Pilot>(self, pilot: P) -> Self {
        self.with_pilot(pilot)
    }
}

/// Encodes a UTC timestamp in River's canonical SQLite wire format.
///
/// This keeps companion crates aligned with River and Go's
/// millisecond-rounded, timezone-free SQLite representation.
#[cfg(feature = "sqlite")]
#[must_use]
pub fn sqlite_timestamp(time: DateTime<Utc>) -> String {
    crate::database::sqlite::sqlite_time(time)
}

/// Adds an add-on crate's indexes to PostgreSQL's default reindexer list.
///
/// Names already in the list are skipped. A caller who chose index names
/// explicitly with `PostgresReindexConfig::with_index_names`, including an
/// empty list that disables the reindexer, keeps exactly that list. A
/// custom schedule or timeout alone still receives add-on indexes. SQLite
/// sources are returned unchanged.
#[cfg(feature = "postgres")]
#[must_use]
pub fn database_with_default_postgres_reindex_names(
    mut database: Database,
    names: impl IntoIterator<Item = impl Into<String>>,
) -> Database {
    database.extend_default_postgres_reindex_names(names);
    database
}

/// Creates a detached work context with no client.
#[must_use]
pub fn work_context(cancellation: CancellationToken) -> crate::WorkContext {
    crate::WorkContext::new(cancellation)
}

/// Creates a detached work context for a job, restoring its persisted
/// resumable metadata.
#[must_use]
pub fn work_context_for_job(job: &JobRow) -> crate::WorkContext {
    crate::WorkContext::for_test_job(job)
}

/// Returns a snapshot of metadata recorded during an attempt.
#[must_use]
pub fn work_context_metadata_updates(context: &crate::WorkContext) -> Map<String, Value> {
    context.metadata_updates()
}

/// Validates resumable checkpoint metadata before invoking user work.
///
/// # Errors
///
/// Returns the resumable metadata failure recorded for the attempt.
pub fn work_context_resumable_validate(
    context: &crate::WorkContext,
) -> Result<(), crate::WorkError> {
    context.resumable_validate()
}

/// Resolves attempt-scoped resumable errors and metadata after user work.
pub fn work_context_resumable_finish(
    context: &crate::WorkContext,
    worker_failed: bool,
) -> Option<crate::WorkError> {
    context.resumable_finish(worker_failed)
}

/// Notification topic for queue and job control messages.
pub const NOTIFICATION_TOPIC_CONTROL: &str = crate::protocol::NOTIFICATION_TOPIC_CONTROL;

/// Notification topic for newly available jobs.
pub const NOTIFICATION_TOPIC_INSERT: &str = crate::protocol::NOTIFICATION_TOPIC_INSERT;

/// Notification topic for leadership changes.
pub const NOTIFICATION_TOPIC_LEADERSHIP: &str = crate::protocol::NOTIFICATION_TOPIC_LEADERSHIP;

/// A River notification topic, mirroring Go's `notifier.NotificationTopic`.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NotificationTopic {
    /// Queue and job control messages.
    Control,
    /// Newly available jobs.
    Insert,
    /// Leadership changes.
    Leadership,
}

impl NotificationTopic {
    /// Returns the unqualified topic name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Control => NOTIFICATION_TOPIC_CONTROL,
            Self::Insert => NOTIFICATION_TOPIC_INSERT,
            Self::Leadership => NOTIFICATION_TOPIC_LEADERSHIP,
        }
    }
}

impl fmt::Display for NotificationTopic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Sends notifications on a caller's transaction connection, like Go's
/// `riverdriver.Executor.NotifyMany`.
///
/// PostgreSQL issues `pg_notify` on the schema-qualified channel
/// (`<schema>.<topic>`, using `current_schema()` when no schema is
/// configured), so delivery happens only when the transaction commits. SQLite
/// appends rows to the durable `river_notification` outbox that River clients
/// poll. An empty payload list does nothing.
///
/// # Errors
///
/// Returns an error when the connection and configuration name different
/// backends or when the database rejects the statement.
#[doc(hidden)]
pub async fn notify_many(
    connection: DatabaseConnection<'_>,
    database: &DatabaseConfig,
    topic: NotificationTopic,
    payloads: &[String],
) -> Result<(), PilotError> {
    if payloads.is_empty() {
        return Ok(());
    }
    match (connection, database) {
        #[cfg(feature = "postgres")]
        (DatabaseConnection::Postgres(connection), DatabaseConfig::Postgres { schema }) => {
            sqlx::query(
                "SELECT pg_notify(concat(coalesce($1::text, current_schema()), '.', $2::text), payload) \
                 FROM unnest($3::text[]) AS payload",
            )
            .bind(schema.as_deref())
            .bind(topic.as_str())
            .bind(payloads)
            .execute(connection)
            .await?;
            Ok(())
        }
        #[cfg(feature = "sqlite")]
        (DatabaseConnection::Sqlite(connection), DatabaseConfig::Sqlite) => {
            let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "INSERT INTO river_notification (payload, topic) ",
            );
            query.push_values(payloads, |mut row, payload| {
                row.push_bind(payload).push_bind(topic.as_str());
            });
            query.build().execute(connection).await?;
            Ok(())
        }
        #[allow(unreachable_patterns)]
        (connection, database) => Err(format!(
            "notification connection {:?} does not match database {:?}",
            connection.kind(),
            database.kind()
        )
        .into()),
    }
}

/// Error type used across the exact-version internal pilot seam.
pub type PilotError = Box<dyn std::error::Error + Send + Sync>;

pub use crate::database::DatabaseKind;

/// Backend configuration passed through River's exact-version extension seam.
#[doc(hidden)]
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum DatabaseConfig {
    /// PostgreSQL backend configuration.
    #[cfg(feature = "postgres")]
    Postgres { schema: SchemaName },
    /// SQLite backend configuration.
    #[cfg(feature = "sqlite")]
    Sqlite,
}

impl DatabaseConfig {
    /// Returns the selected backend.
    #[must_use]
    pub const fn kind(&self) -> DatabaseKind {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres { .. } => DatabaseKind::Postgres,
            #[cfg(feature = "sqlite")]
            Self::Sqlite => DatabaseKind::Sqlite,
        }
    }

    /// Returns PostgreSQL's configured schema, if selected.
    #[must_use]
    #[cfg(feature = "postgres")]
    pub const fn postgres_schema(&self) -> Option<&SchemaName> {
        match self {
            Self::Postgres { schema } => Some(schema),
            #[cfg(feature = "sqlite")]
            Self::Sqlite => None,
        }
    }
}

/// Borrowed transaction connection passed to an exact-version extension.
#[doc(hidden)]
#[non_exhaustive]
pub enum DatabaseConnection<'connection> {
    /// PostgreSQL transaction connection.
    #[cfg(feature = "postgres")]
    Postgres(&'connection mut PgConnection),
    /// SQLite transaction connection.
    #[cfg(feature = "sqlite")]
    Sqlite(&'connection mut SqliteConnection),
}

impl<'connection> DatabaseConnection<'connection> {
    /// Returns the selected backend.
    #[must_use]
    pub const fn kind(&self) -> DatabaseKind {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => DatabaseKind::Postgres,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(_) => DatabaseKind::Sqlite,
        }
    }

    /// Reborrows the connection for one operation, leaving this value
    /// usable afterwards.
    pub(crate) fn reborrow(&mut self) -> DatabaseConnection<'_> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(connection) => DatabaseConnection::Postgres(connection),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(connection) => DatabaseConnection::Sqlite(connection),
        }
    }

    /// Returns the PostgreSQL connection, if selected.
    #[must_use]
    #[cfg(feature = "postgres")]
    pub fn into_postgres(self) -> Option<&'connection mut PgConnection> {
        match self {
            Self::Postgres(connection) => Some(connection),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(_) => None,
        }
    }

    /// Returns the SQLite connection, if selected.
    #[must_use]
    #[cfg(feature = "sqlite")]
    pub fn into_sqlite(self) -> Option<&'connection mut SqliteConnection> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => None,
            Self::Sqlite(connection) => Some(connection),
        }
    }
}

impl fmt::Debug for DatabaseConnection<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatabaseConnection")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

/// Caller-owned pool passed to an exact-version background service.
#[doc(hidden)]
#[derive(Clone)]
#[non_exhaustive]
pub enum DatabasePool {
    /// PostgreSQL pool.
    #[cfg(feature = "postgres")]
    Postgres(PgPool),
    /// SQLite pool.
    #[cfg(feature = "sqlite")]
    Sqlite(SqlitePool),
}

impl DatabasePool {
    /// Returns the selected backend.
    #[must_use]
    pub const fn kind(&self) -> DatabaseKind {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => DatabaseKind::Postgres,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(_) => DatabaseKind::Sqlite,
        }
    }

    /// Returns the caller-owned PostgreSQL pool, if selected.
    #[must_use]
    #[cfg(feature = "postgres")]
    pub const fn postgres(&self) -> Option<&PgPool> {
        match self {
            Self::Postgres(pool) => Some(pool),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(_) => None,
        }
    }

    /// Returns the caller-owned SQLite pool, if selected.
    #[must_use]
    #[cfg(feature = "sqlite")]
    pub const fn sqlite(&self) -> Option<&SqlitePool> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => None,
            Self::Sqlite(pool) => Some(pool),
        }
    }
}

impl fmt::Debug for DatabasePool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatabasePool")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

/// Inputs available while selecting jobs under a fetch transaction.
#[derive(Clone, Debug)]
pub struct FetchParams {
    /// Stable client identifier.
    pub client_id: String,
    /// Registered job kinds, including aliases.
    pub kinds: Vec<String>,
    /// Maximum rows to lock.
    pub maximum: i32,
    /// Queue being fetched.
    pub queue: String,
    /// Selected database backend configuration.
    pub database: DatabaseConfig,
}

/// Inputs available while selecting stuck jobs under a rescue transaction.
///
/// Mirrors Go's `JobGetStuckParams`: selections page by ID after `after_id`
/// and consider only jobs attempted before `stuck_horizon`.
#[derive(Clone, Debug)]
pub struct RescueParams {
    /// Only jobs with a greater ID belong to this batch.
    pub after_id: i64,
    /// Selected database backend configuration.
    pub database: DatabaseConfig,
    /// Maximum rows to select.
    pub maximum: i64,
    /// Age at which the OSS runtime considers a running job stuck.
    pub rescue_after: Duration,
    /// Jobs attempted at or after this time are not stuck. Computed once per
    /// rescuer pass.
    pub stuck_horizon: DateTime<Utc>,
}

/// One stuck job's transition exactly as the OSS rescuer would persist it.
#[derive(Clone, Debug)]
pub struct RescueJob {
    /// Attempt error JSON appended to the job's `errors`.
    pub attempt_error: Value,
    /// Finalization time, set for `cancelled` and `discarded`.
    pub finalized_at: Option<DateTime<Utc>>,
    /// Job ID.
    pub id: i64,
    /// Next scheduled time.
    pub scheduled_at: DateTime<Utc>,
    /// Target River state string.
    pub state: JobState,
}

/// Inputs of a batched rescue, mirroring Go's `JobRescueManyParams`.
///
/// OSS only applies each transition to jobs still `running` with
/// `attempted_at` before `stuck_horizon`, so a job completed or claimed again
/// after selection is left untouched. Implementations that handle the rescue
/// themselves should apply the same guard.
#[derive(Clone, Debug)]
pub struct RescueManyParams {
    /// Selected database backend configuration.
    pub database: DatabaseConfig,
    /// Transitions OSS would write, in ID order.
    pub jobs: Vec<RescueJob>,
    /// Horizon the batch was selected with.
    pub stuck_horizon: DateTime<Utc>,
}

/// Whether the OSS rescuer should perform its normal guarded update.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RescueAction {
    /// Continue through the OSS rescue update.
    #[default]
    Continue,
    /// The extension persisted the rescue itself.
    Handled,
}

/// A job River just cancelled or retried, passed to extension post-hooks in
/// the same transaction as the update.
#[derive(Clone, Debug)]
pub struct JobUpdatedParams {
    /// Selected database backend configuration.
    pub database: DatabaseConfig,
    /// The job after the update.
    pub job: JobRow,
}

/// Rows passed to [`Pilot::after_jobs_set_state`].
#[derive(Clone, Debug)]
pub struct JobSetStateParams<'a> {
    /// Selected database backend configuration.
    pub database: DatabaseConfig,
    /// The ID of every job in the batch, including jobs deleted while their
    /// workers ran, which have no row in `jobs`. Like River Go's pilot
    /// `JobFinish`, this lets an extension release per-job resources for
    /// every attempt that ended.
    pub job_ids: &'a [i64],
    /// Every job in the batch that still exists, as returned by the update,
    /// including jobs that were no longer running and so kept their state.
    pub jobs: &'a [JobRow],
}

/// Rows passed to [`Pilot::after_jobs_inserted`].
#[derive(Clone, Debug)]
pub struct JobsInsertedParams<'a> {
    /// Selected database backend configuration.
    pub database: DatabaseConfig,
    /// Jobs the insertion wrote, excluding unique insertions skipped as
    /// duplicates, in input order.
    pub jobs: &'a [JobRow],
}

/// Queue metadata passed to [`Pilot::queue_metadata_changed`].
#[derive(Clone, Debug)]
pub struct QueueMetadataChangedParams {
    /// Selected database backend configuration.
    pub database: DatabaseConfig,
    /// The queue's current metadata.
    pub metadata: Map<String, Value>,
    /// Caller-owned pool.
    pub pool: DatabasePool,
    /// Queue name.
    pub queue: String,
}

/// Mutable job insertion fields exposed to an exact-version extension.
///
/// The references point into River's resolved insertion context. Changes are
/// validated and persisted by the ordinary insertion pipeline after the
/// extension returns.
#[doc(hidden)]
pub struct JobInsertParams<'insert> {
    /// Serialized job arguments as exact JSON text.
    pub encoded_args: &'insert mut Box<serde_json::value::RawValue>,
    /// Stable job kind.
    pub kind: &'insert mut String,
    /// Arbitrary job metadata.
    pub metadata: &'insert mut crate::JobMetadata,
    /// Queue in which the job will run.
    pub queue: &'insert mut String,
    /// Initial state: available, pending, or scheduled. An extension may
    /// insert a job as pending, like River Go's insert hooks setting
    /// `JobInsertParams.State`.
    pub state: &'insert mut JobState,
}

impl fmt::Debug for JobInsertParams<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobInsertParams")
            .field("kind", self.kind)
            .field("queue", self.queue)
            .finish_non_exhaustive()
    }
}

/// A leader-owned service supplied by an exact-version extension.
#[async_trait]
pub trait MaintenanceService: Send + Sync + 'static {
    /// Runs until cancellation and returns if the service fails.
    async fn run(
        &self,
        pool: DatabasePool,
        database: DatabaseConfig,
        cancellation: CancellationToken,
    ) -> Result<(), PilotError>;
}

/// Per-client service supplied by an exact-version extension.
///
/// Unlike [`MaintenanceService`], a runtime service runs on every started
/// client rather than only while that client holds River leadership.
#[async_trait]
pub trait RuntimeService: Send + Sync + 'static {
    /// Runs until cancellation and returns if the service fails.
    async fn run(
        &self,
        pool: DatabasePool,
        database: DatabaseConfig,
        cancellation: CancellationToken,
    ) -> Result<(), PilotError>;
}

/// Exact-version extension seam for matched companion crates.
///
/// This trait is intentionally not a stable River API. The internal crate is
/// version-locked to `riverqueue`, allowing the SPI to evolve with both
/// implementations.
#[async_trait]
pub trait Pilot: Send + Sync + 'static {
    /// Queues whose finalized jobs are owned by an extension-specific cleaner
    /// and skipped by River's job cleaner, like Go's
    /// `Pilot.JobCleanerQueuesExcluded`. Read on every cleaner pass.
    fn job_cleaner_queue_exclusions(&self) -> Vec<String> {
        Vec::new()
    }

    /// Whether job cancellation and retry must run
    /// [`Pilot::after_job_cancel`] and [`Pilot::after_job_retry`]. Returning
    /// `true` also makes pool-based cancel and retry use a transaction.
    fn intercepts_job_cancel_retry(&self) -> bool {
        false
    }

    /// Whether fetches must enter the exact-version interception transaction.
    /// Returning `false` lets OSS claim jobs with one PostgreSQL statement;
    /// implementations that override `select_job_ids` return `true`.
    fn intercepts_fetch(&self) -> bool {
        false
    }

    /// Whether stuck-job candidate selection must enter the exact-version
    /// interception transaction.
    fn intercepts_rescue(&self) -> bool {
        false
    }

    /// Whether job state transitions must run in a transaction that also
    /// calls [`Pilot::after_jobs_set_state`], like River Go's
    /// `Pilot.JobSetStateIfRunningMany`.
    ///
    /// Returning `false` keeps River's one-statement completion path.
    /// Implementations that override `after_jobs_set_state` return `true`.
    fn intercepts_job_set_state(&self) -> bool {
        false
    }

    /// How many intercepted completion batches may run concurrently, like
    /// River Go's `PilotJobCompletionConcurrency`.
    ///
    /// River never exceeds its backend's own limit (two on PostgreSQL, one on
    /// SQLite) and starts a second concurrent batch only when a full batch of
    /// completions is waiting. The default allows one batch at a time.
    fn job_set_state_concurrency(&self) -> usize {
        1
    }

    /// Whether inserts must enter the exact-version interception transaction.
    ///
    /// Returning `true` makes pool-based insertion acquire a transaction so
    /// [`Pilot::before_job_insert`] can observe backend state on the same
    /// connection as the eventual insert.
    fn intercepts_insert(&self) -> bool {
        false
    }

    /// Mutates or validates a resolved insertion using its transaction
    /// connection.
    ///
    /// River invokes ordinary begin hooks first, then this method, then insert
    /// middleware. The insert and its backend notification remain in the same
    /// transaction.
    async fn before_job_insert(
        &self,
        _connection: DatabaseConnection<'_>,
        _params: &mut JobInsertParams<'_>,
    ) -> Result<(), PilotError> {
        Ok(())
    }

    /// Optionally claims jobs itself, like River Go's `Pilot.JobGetAvailable`.
    ///
    /// Called in the fetch transaction when [`Pilot::intercepts_fetch`]
    /// returns `true`, before [`Pilot::select_job_ids`]. Returning jobs skips
    /// River's claim: the extension must have moved them to `running` with
    /// the attempt incremented, `attempted_at` set, and this client appended
    /// to `attempted_by`, exactly as River's claim does. Build each entry
    /// from a row selected with [`postgres_job_projection`] using
    /// [`claimed_postgres_job`] (or their SQLite equivalents), so a row that
    /// can't be fully decoded fails its attempt exactly as it would through
    /// River's own claim. `None` continues with [`Pilot::select_job_ids`] and
    /// River's claim.
    async fn claim_jobs(
        &self,
        _connection: DatabaseConnection<'_>,
        _params: &FetchParams,
    ) -> Result<Option<Vec<ClaimedJob>>, PilotError> {
        Ok(None)
    }

    /// Optionally selects and locks fetch candidates using the provided
    /// transaction connection. Returned IDs are claimed by the OSS runtime in
    /// the same transaction. `None` delegates selection to River OSS.
    async fn select_job_ids(
        &self,
        _connection: DatabaseConnection<'_>,
        _params: &FetchParams,
    ) -> Result<Option<Vec<i64>>, PilotError> {
        Ok(None)
    }

    /// Optionally selects stuck-job candidates, honoring the params' cursor
    /// and horizon. Returned IDs are evaluated by the OSS rescuer in the same
    /// transaction; returning `maximum` IDs asks for another batch.
    async fn select_rescue_job_ids(
        &self,
        _connection: DatabaseConnection<'_>,
        _params: &RescueParams,
    ) -> Result<Option<Vec<i64>>, PilotError> {
        Ok(None)
    }

    /// Optionally persists a batch of rescues in the rescuer's transaction.
    ///
    /// Called only when [`Pilot::intercepts_rescue`] returns `true`, after
    /// OSS decided each selected job's transition. Returning
    /// [`RescueAction::Continue`] lets OSS apply its guarded update.
    async fn rescue_jobs(
        &self,
        _connection: DatabaseConnection<'_>,
        _params: &RescueManyParams,
    ) -> Result<RescueAction, PilotError> {
        Ok(RescueAction::Continue)
    }

    /// Runs after River cancels a job, in the same transaction and with the
    /// updated row. Called only when [`Pilot::intercepts_job_cancel_retry`]
    /// returns `true`; an error rolls back the cancellation.
    async fn after_job_cancel(
        &self,
        _connection: DatabaseConnection<'_>,
        _job: &JobUpdatedParams,
    ) -> Result<(), PilotError> {
        Ok(())
    }

    /// Runs after River retries a job, in the same transaction and with the
    /// updated row. Called only when [`Pilot::intercepts_job_cancel_retry`]
    /// returns `true`; an error rolls back the retry.
    async fn after_job_retry(
        &self,
        _connection: DatabaseConnection<'_>,
        _job: &JobUpdatedParams,
    ) -> Result<(), PilotError> {
        Ok(())
    }

    /// Observes a batch of job state transitions inside River's transaction.
    ///
    /// Called only when [`Pilot::intercepts_job_set_state`] returns `true`.
    /// River keeps batching completions: each batch runs `BEGIN`, River's
    /// set-state-if-running update (which returns full rows), this hook with
    /// those rows, then `COMMIT`. The transactional `job_complete_tx` path
    /// calls it with its one row inside the caller's transaction.
    ///
    /// The hook may write further state with the connection, including
    /// deleting returned rows; River still reports events from the rows it
    /// already holds. Returning an error rolls the batch back, and River
    /// retries it like any other failed completion write.
    async fn after_jobs_set_state(
        &self,
        _connection: DatabaseConnection<'_>,
        _params: &JobSetStateParams,
    ) -> Result<(), PilotError> {
        Ok(())
    }

    /// Runs after River writes a batch of inserted jobs, inside the insertion
    /// transaction, like the post-insert work in River Go's
    /// `Pilot.JobInsertMany`.
    ///
    /// Called only when [`Pilot::intercepts_insert`] returns `true`, on every
    /// insertion path, including batches, fast inserts (which then write rows
    /// individually rather than with `COPY`), caller-managed transactions,
    /// and periodic jobs. Unique insertions skipped as duplicates aren't
    /// included. Returning an error rolls back the insertion.
    async fn after_jobs_inserted(
        &self,
        _connection: DatabaseConnection<'_>,
        _params: &JobsInsertedParams<'_>,
    ) -> Result<(), PilotError> {
        Ok(())
    }

    /// Observes a queue's metadata when a producer starts and whenever it's
    /// changed at runtime, like River Go's `Pilot.QueueMetadataChanged`.
    /// Errors are logged.
    async fn queue_metadata_changed(
        &self,
        _params: &QueueMetadataChangedParams,
    ) -> Result<(), PilotError> {
        Ok(())
    }

    /// Leader-owned services contributed by the extension. River runs them
    /// only while the client is leader, and never on a client built with
    /// `ClientBuilder::without_leader_election`.
    fn maintenance_services(&self) -> Vec<std::sync::Arc<dyn MaintenanceService>> {
        Vec::new()
    }

    /// Per-client services contributed by the extension. River runs them on
    /// every started client, including one without leader election.
    fn runtime_services(&self) -> Vec<std::sync::Arc<dyn RuntimeService>> {
        Vec::new()
    }
}

/// No-op pilot used by River OSS.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopPilot;

impl Pilot for NoopPilot {}

/// Columns River selects to decode a PostgreSQL job row, qualified by
/// `alias`, for use with [`decode_postgres_job_row`].
#[cfg(feature = "postgres")]
#[must_use]
pub fn postgres_job_projection(alias: &str) -> String {
    crate::client::job_projection(alias)
}

/// Decodes a row selected with [`postgres_job_projection`] exactly as River
/// decodes its own rows.
///
/// # Errors
///
/// Returns an error when the row can't be decoded.
#[cfg(feature = "postgres")]
pub fn decode_postgres_job_row(row: &sqlx::postgres::PgRow) -> Result<JobRow, PilotError> {
    crate::client::decode_job_row(row).map_err(|undecodable| undecodable.error.into())
}

/// Decodes a claimed row selected with [`postgres_job_projection`] as far
/// as River can, for [`Pilot::claim_jobs`].
#[cfg(feature = "postgres")]
#[must_use]
pub fn claimed_postgres_job(row: &sqlx::postgres::PgRow) -> ClaimedJob {
    ClaimedJob::from_decoded(crate::client::decode_job_row(row))
}

/// Columns River selects to decode a SQLite job row, for use with
/// [`decode_sqlite_job_row`].
#[cfg(feature = "sqlite")]
pub const SQLITE_JOB_COLUMNS: &str = crate::database::sqlite::JOB_COLUMNS;

/// Decodes a row selected with [`SQLITE_JOB_COLUMNS`] exactly as River
/// decodes its own rows.
///
/// # Errors
///
/// Returns an error when the row can't be decoded.
#[cfg(feature = "sqlite")]
pub fn decode_sqlite_job_row(row: &sqlx::sqlite::SqliteRow) -> Result<JobRow, PilotError> {
    crate::database::sqlite::decode_job_row(row).map_err(|undecodable| undecodable.error.into())
}

/// Decodes a claimed row selected with [`SQLITE_JOB_COLUMNS`] as far as
/// River can, for [`Pilot::claim_jobs`].
#[cfg(feature = "sqlite")]
#[must_use]
pub fn claimed_sqlite_job(row: &sqlx::sqlite::SqliteRow) -> ClaimedJob {
    ClaimedJob::from_decoded(crate::database::sqlite::decode_job_row(row))
}

/// A job claimed by [`Pilot::claim_jobs`].
///
/// River works a decoded job normally. Like a row River claims itself, an
/// undecodable one isn't worked: its attempt fails with an error describing
/// the decode failure, before hooks or middleware run, and it's retried or
/// discarded through ordinary error handling.
#[derive(Debug)]
pub struct ClaimedJob(crate::client::DecodedJob);

impl ClaimedJob {
    fn from_decoded(decoded: crate::client::DecodedJob) -> Self {
        Self(decoded)
    }

    /// Returns the decoded row, or `None` when some field couldn't be
    /// decoded.
    #[must_use]
    pub fn job(&self) -> Option<&JobRow> {
        self.0.as_ref().ok()
    }

    /// Returns why the row couldn't be fully decoded, if it couldn't.
    #[must_use]
    pub fn decode_error(&self) -> Option<&str> {
        self.0
            .as_ref()
            .err()
            .map(|undecodable| undecodable.error.as_str())
    }

    pub(crate) fn into_decoded(self) -> crate::client::DecodedJob {
        self.0
    }
}

impl From<JobRow> for ClaimedJob {
    fn from(job: JobRow) -> Self {
        Self(Ok(job))
    }
}

/// Type-erased result returned by River's exact-version insertion seam.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RawInsertResult {
    /// Inserted job or the existing matching unique job.
    pub job: JobRow,
    /// Whether insertion was skipped because a unique job already existed.
    pub unique_skipped_as_duplicate: bool,
}

/// Persisted insertion fields accepted by River's exact-version extension
/// seam.
///
/// River resets execution fields and lets the backend allocate the live-row
/// ID rather than explicitly retaining a source ID. The supplied creation
/// time, schedule, and uniqueness wire values are retained while the ordinary
/// hook, middleware, insertion-interception, and notification pipeline runs.
#[derive(Clone, Debug)]
pub struct ExtensionInsertParams {
    /// Original creation time.
    pub created_at: DateTime<Utc>,
    /// Serialized job arguments.
    pub encoded_args: Box<RawValue>,
    /// Stable job kind.
    pub kind: String,
    /// Maximum attempts, including the first.
    pub max_attempts: i16,
    /// Arbitrary job metadata.
    pub metadata: crate::JobMetadata,
    /// Priority from one through four.
    pub priority: i16,
    /// Queue in which the job runs.
    pub queue: String,
    /// Earliest time at which the reinserted job may run.
    pub scheduled_at: DateTime<Utc>,
    /// Searchable tags.
    pub tags: Vec<String>,
    /// Existing unique hash, if any.
    pub unique_key: Option<Vec<u8>>,
    /// Existing states in which the key is enforced, if any.
    pub unique_states: Option<Vec<JobState>>,
}

/// Eligibility and metadata changes for an exact-version atomic job claim.
///
/// River claims available, due jobs matching the kind, queue, and top-level
/// metadata values. It excludes one coordinating job, records the claiming
/// client and attempt, applies `metadata_updates`, and returns complete rows in
/// priority, scheduled-time, and ID order.
#[derive(Clone, Debug)]
pub struct ExtensionClaimParams {
    /// Job ID excluded from the claim.
    pub excluded_job_id: i64,
    /// Stable job kind to claim.
    pub kind: String,
    /// Maximum number of jobs to claim.
    pub maximum: i32,
    /// Top-level metadata values that must match exactly.
    pub metadata_matches: Map<String, Value>,
    /// Top-level metadata values merged into every claimed job.
    pub metadata_updates: Map<String, Value>,
    /// Queue from which jobs are claimed.
    pub queue: String,
}

impl RawInsertResult {
    /// Converts an exact-version raw result after its arguments are decoded.
    #[must_use]
    pub fn into_typed<A>(self, args: A) -> InsertResult<A> {
        InsertResult {
            job: Job::new(args, self.job),
            unique_skipped_as_duplicate: self.unique_skipped_as_duplicate,
        }
    }
}

/// Complete persisted job fields for exact-version record conversion.
#[derive(Debug)]
pub struct JobRowParts {
    pub id: i64,
    pub attempt: i16,
    pub attempted_at: Option<DateTime<Utc>>,
    pub attempted_by: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub encoded_args: Box<RawValue>,
    pub errors: Vec<AttemptError>,
    pub finalized_at: Option<DateTime<Utc>>,
    pub kind: String,
    pub max_attempts: i16,
    pub metadata: crate::JobMetadata,
    pub priority: i16,
    pub queue: String,
    pub scheduled_at: DateTime<Utc>,
    pub state: JobState,
    pub tags: Vec<String>,
    pub unique_key: Option<Vec<u8>>,
    pub unique_states: Option<Vec<JobState>>,
}

impl JobRowParts {
    /// Converts complete fields from an exact-version database record.
    #[must_use]
    pub fn into_row(self) -> JobRow {
        let parts = self;
        JobRow {
            attempt: parts.attempt,
            attempted_at: parts.attempted_at,
            attempted_by: parts.attempted_by,
            created_at: parts.created_at,
            encoded_args: parts.encoded_args,
            errors: parts.errors,
            finalized_at: parts.finalized_at,
            id: parts.id,
            kind: parts.kind,
            max_attempts: parts.max_attempts,
            metadata: parts.metadata,
            priority: parts.priority,
            queue: parts.queue,
            scheduled_at: parts.scheduled_at,
            state: parts.state,
            tags: parts.tags,
            unique_key: parts.unique_key,
            unique_states: parts.unique_states,
        }
    }
}
