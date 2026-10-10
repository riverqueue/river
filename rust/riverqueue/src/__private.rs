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

pub use crate::client::{ExtensionClient, PreparedInsertRequest, RawInsertRequest, WeakClient};
pub use crate::database::erased::{Database, ErasedExecutor, ErasedTransaction};
pub use crate::pilot::{
    PilotDatabase, PilotProducer, PilotTransaction, ProducerClaimContext, ProducerClaimNext,
    ProducerConfiguration, ProducerKeepAliveContext, ProducerShutdownContext, ProducerStartContext,
};

/// Insertion options reserved for River's own companion crates.
///
/// Extension options travel beside a job's ordinary options, from a job
/// type's `JobArgs::default_insert_opts` or a call's options, to the
/// extension's [`Pilot::before_jobs_insert`] hook as
/// [`JobInsertParams::extension_options`]. River doesn't persist them. They
/// resolve key by key: a call's option replaces the job type's option with
/// the same key, and the job type's other options are kept, so an extension
/// can declare options for a job type that per-call options such as metadata
/// don't disturb.
pub trait InsertOptsExt: Sized {
    /// Returns the extension options set on these insertion options.
    fn extension_options(&self) -> &Map<String, Value>;

    /// Sets the extension option `key`, replacing any earlier value.
    #[must_use]
    fn with_extension_option(self, key: impl Into<String>, value: Value) -> Self;
}

impl InsertOptsExt for crate::InsertOpts {
    fn extension_options(&self) -> &Map<String, Value> {
        &self.extension_options
    }

    fn with_extension_option(mut self, key: impl Into<String>, value: Value) -> Self {
        self.extension_options.insert(key.into(), value);
        self
    }
}

/// Queue configuration reserved for River's own companion crates.
///
/// Extension settings travel with a queue's configuration, through
/// `ClientBuilder::queue` or [`LocalQueues`](crate::LocalQueues), to the
/// extension's [`Pilot::validate_queue_settings`] and then its producer
/// session as [`ProducerConfiguration::settings`]. River doesn't persist
/// them.
pub trait QueueConfigExt: Sized {
    /// Returns the extension settings on this configuration.
    fn extension_settings(&self) -> &Map<String, Value>;

    /// Sets the extension setting `key`, replacing any earlier value.
    #[must_use]
    fn with_extension_setting(self, key: impl Into<String>, value: Value) -> Self;
}

impl QueueConfigExt for crate::QueueConfig {
    fn extension_settings(&self) -> &Map<String, Value> {
        &self.extension_settings
    }

    fn with_extension_setting(mut self, key: impl Into<String>, value: Value) -> Self {
        self.extension_settings.insert(key.into(), value);
        self
    }
}

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

    /// Sets how often producers call [`PilotProducer::keep_alive`], 30
    /// seconds by default like River Go's `ProducerReportInterval`. It's a
    /// control for tests, not a tuning option.
    #[must_use]
    fn producer_report_interval(self, interval: Duration) -> Self;
}

impl ClientBuilderExt for crate::ClientBuilder {
    fn leader_election_disabled(&self) -> bool {
        self.leader_election_disabled
    }

    fn pilot<P: Pilot>(self, pilot: P) -> Self {
        self.with_pilot(pilot)
    }

    fn producer_report_interval(mut self, interval: Duration) -> Self {
        self.producer_report_interval = interval;
        self
    }
}

/// Formats an error and its sources the way River records a job's error,
/// `outer: inner`, with a message that repeats its source's shortened.
///
/// Add-on crates use it for error text they persist themselves, so it reads
/// the same as the errors River records.
#[must_use]
pub fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    crate::error::Chain(error).to_string()
}

/// Decodes one persisted attempt error leniently, like River Go's driver
/// reads, so an element in a shape River doesn't write can't make its row
/// unreadable. [`AttemptError`]'s `Deserialize` is strict like Go's
/// `encoding/json`, so add-on crates reading `errors` from the database use
/// this instead. Only text that isn't valid JSON is an error.
pub fn attempt_error_from_json(json: &str) -> Result<AttemptError, serde_json::Error> {
    AttemptError::from_json_lenient(json)
}

/// Decodes a persisted JSON array of attempt errors leniently, decoding each
/// element like [`attempt_error_from_json`]. `null` is empty, and anything
/// other than an array is an error.
pub fn attempt_errors_from_json(json: &str) -> Result<Vec<AttemptError>, serde_json::Error> {
    AttemptError::from_json_array_lenient(json)
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

/// Adds an add-on crate's indexes to Postgres's default reindexer list.
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

/// Claims an available job for `client` as a fetch would, marking it
/// running with a new attempt, and returns a work context for that attempt
/// whose [`WorkContext::client`](crate::WorkContext::client) is `client`.
///
/// # Errors
///
/// Returns [`Error::NotFound`](crate::Error::NotFound) for a missing job, an
/// invalid-job error when the job isn't available, and a database error when
/// the claim fails.
pub async fn claim_job_for_test(
    client: &crate::Client,
    id: i64,
) -> Result<(JobRow, crate::WorkContext), crate::Error> {
    let inner = &client.inner;
    let mut session =
        crate::storage::Session::begin(&inner.database, crate::storage::Access::Transaction)
            .await?;
    let row = session.storage(inner).job_claim(id).await?;
    session.commit().await?;
    let context = crate::WorkContext::for_job(
        client.clone(),
        CancellationToken::new(),
        row.id,
        &row.metadata,
    );
    Ok((row, context))
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
/// Postgres issues `pg_notify` on the schema-qualified channel
/// (`<schema>.<topic>`, using `current_schema()` when no schema is
/// configured), so delivery happens only when the transaction commits. A
/// server without `LISTEN`/`NOTIFY`, like YugabyteDB by default, gets no
/// notifications; the configuration carries no detected capabilities, so
/// each call checks the server. SQLite appends rows to the durable
/// `river_notification` outbox that River clients poll. An empty payload list
/// does nothing.
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
            if !crate::database::postgres_capabilities::PostgresCapabilities::detect(
                &mut *connection,
            )
            .await?
            .supports_listen_notify
            {
                return Ok(());
            }
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

/// Filters for [`delete_finalized_jobs`], mirroring the job cleaner's query.
///
/// Each horizon deletes jobs in that state finalized before it; `None` keeps
/// jobs in that state, however old.
#[doc(hidden)]
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct FinalizedJobDeleteParams {
    /// Delete cancelled jobs finalized before this time.
    pub cancelled_before: Option<DateTime<Utc>>,
    /// Delete completed jobs finalized before this time.
    pub completed_before: Option<DateTime<Utc>>,
    /// Delete discarded jobs finalized before this time.
    pub discarded_before: Option<DateTime<Utc>>,
    /// Maximum jobs to delete, lowest IDs first.
    pub limit: i64,
    /// Queues whose jobs are kept.
    pub queues_excluded: Vec<String>,
    /// When set, only jobs in these queues are deleted.
    pub queues_included: Option<Vec<String>>,
}

impl FinalizedJobDeleteParams {
    /// Creates filters that delete nothing until a horizon is set.
    #[must_use]
    pub fn new(limit: i64) -> Self {
        Self {
            limit,
            ..Self::default()
        }
    }
}

/// Deletes finalized jobs with River's job cleaner query, on a caller's
/// connection, and returns how many were deleted.
///
/// Add-on crates use it for cleaner passes of their own, such as per-queue
/// retention, so their deletions match River's exactly, including keeping a
/// state whose horizon is `None` on every backend. It runs no timeout or
/// cancellation of its own.
///
/// # Errors
///
/// Returns an error when the connection and configuration name different
/// backends or when the database rejects the statement.
#[doc(hidden)]
pub async fn delete_finalized_jobs(
    connection: DatabaseConnection<'_>,
    database: &DatabaseConfig,
    params: &FinalizedJobDeleteParams,
) -> Result<u64, PilotError> {
    match (connection, database) {
        #[cfg(feature = "postgres")]
        (DatabaseConnection::Postgres(connection), DatabaseConfig::Postgres { schema }) => Ok(
            crate::maintenance::postgres_delete_finalized_jobs(connection, schema, params).await?,
        ),
        #[cfg(feature = "sqlite")]
        (DatabaseConnection::Sqlite(connection), DatabaseConfig::Sqlite) => {
            Ok(crate::maintenance::sqlite_delete_finalized_jobs(connection, params).await?)
        }
        #[allow(unreachable_patterns)]
        (connection, database) => Err(format!(
            "deletion connection {:?} does not match database {:?}",
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
    /// Postgres backend configuration.
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

    /// Returns Postgres's configured schema, if selected.
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
    /// Postgres transaction connection.
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

    /// Returns the Postgres connection, if selected.
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
    /// Postgres pool.
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

    /// Returns the caller-owned Postgres pool, if selected.
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
    /// The limit on the rescuer transaction the selection runs in. An
    /// extension that reads through a connection of its own should bound
    /// that read the same way.
    pub timeout: Duration,
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
    /// workers ran, which have no row in `jobs`. Per-attempt resources, such
    /// as running counts, are released by [`PilotProducer::job_finished`]
    /// instead, which doesn't wait for persistence.
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

/// Mutable job insertion fields exposed to an exact-version extension.
///
/// The references point into River's resolved insertion context. Changes are
/// validated and persisted by the ordinary insertion pipeline after the
/// extension returns.
#[doc(hidden)]
pub struct JobInsertParams<'insert> {
    /// Serialized job arguments as exact JSON text.
    pub encoded_args: &'insert mut Box<serde_json::value::RawValue>,
    /// Extension options resolved from the job type's and the call's
    /// [`InsertOptsExt`] options, keyed by extension. River doesn't persist
    /// them.
    pub extension_options: &'insert Map<String, Value>,
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

/// A leadership term, handed to [`MaintenanceService::run`].
///
/// `token` is cancelled the moment this client stops trusting its
/// leadership: when it resigns, when a renewal fails, or when the trust
/// deadline passes without a renewal. Cancellation is local: it can't fence
/// statements already sent to the database.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct LeaderTerm {
    /// When the database recorded this client's election, which identifies
    /// the term.
    pub elected_at: DateTime<Utc>,
    /// Cancelled when the term ends.
    pub token: CancellationToken,
}

/// Inputs to [`MaintenanceService::run`].
#[derive(Debug)]
#[non_exhaustive]
pub struct MaintenanceServiceContext {
    /// The client running the service, without keeping it alive.
    pub client: WeakClient,
    /// The client's database.
    pub database: PilotDatabase,
    /// The leadership term the service runs in.
    pub term: LeaderTerm,
}

/// Inputs to [`RuntimeService::run`].
#[derive(Debug)]
#[non_exhaustive]
pub struct RuntimeServiceContext {
    /// Cancelled when the service should stop, which happens as soon as the
    /// client starts stopping.
    pub cancellation: CancellationToken,
    /// The client running the service, without keeping it alive.
    pub client: WeakClient,
    /// The client's database.
    pub database: PilotDatabase,
}

/// A leader-owned service supplied by an exact-version extension.
///
/// River runs each service for every leadership term this client holds and
/// supervises it within the term: a service that returns an error, panics,
/// or returns before its term ends is logged and started again after River's
/// service backoff, which starts over after two minutes of healthy running.
/// A term's services all return before the next term's start.
#[async_trait]
pub trait MaintenanceService: Send + Sync + 'static {
    /// A name for the service in River's logs.
    fn name(&self) -> &'static str {
        "extension maintenance service"
    }

    /// Runs until the term's token is cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error when the service failed; River restarts it.
    async fn run(&self, context: MaintenanceServiceContext) -> Result<(), PilotError>;
}

/// Per-client service supplied by an exact-version extension.
///
/// Unlike [`MaintenanceService`], a runtime service runs on every started
/// client rather than only while that client holds River leadership. River
/// starts runtime services before the client's producers, and restarts one
/// that fails, panics, or returns early after its service backoff.
#[async_trait]
pub trait RuntimeService: Send + Sync + 'static {
    /// A name for the service in River's logs.
    fn name(&self) -> &'static str {
        "extension runtime service"
    }

    /// Runs until the context's cancellation.
    ///
    /// # Errors
    ///
    /// Returns an error when the service failed; River restarts it.
    async fn run(&self, context: RuntimeServiceContext) -> Result<(), PilotError>;
}

/// What a pilot gets when its client is built, from
/// [`Pilot::install`].
#[derive(Debug)]
#[non_exhaustive]
pub struct PilotInstallContext {
    /// The client, without keeping it alive.
    pub client: WeakClient,
    /// The client's database, for the pilot's own statements.
    pub database: PilotDatabase,
    /// How often producers report to their sessions, which peers use to tell
    /// when a producer has gone stale.
    pub producer_report_interval: Duration,
}

/// Exact-version extension seam for matched companion crates.
///
/// This trait is intentionally not a stable River API. The internal crate is
/// version-locked to `riverqueue`, allowing the SPI to evolve with both
/// implementations.
#[async_trait]
pub trait Pilot: std::any::Any + Send + Sync + 'static {
    /// Binds the pilot to the client being built, like River Go's
    /// `PilotInit`. River calls it once per client, before the builder
    /// returns the client; a pilot installed on several clients is called
    /// once for each. The pilot keeps what it needs from `context` rather
    /// than reading the client's public database accessors.
    fn install(&self, _context: PilotInstallContext) {}

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
    /// River never exceeds its backend's own limit (two on Postgres, one on
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

    /// Mutates or validates every job of one insertion call at once, using
    /// its transaction connection, like River Go's `Pilot.JobInsertMany`
    /// receiving the whole batch.
    ///
    /// River invokes it once per insertion when it intercepts inserts, after
    /// every job's begin hooks and before writing any job. Implementations
    /// can share work across the batch, such as reading each distinct queue's
    /// configuration once. The default calls [`Pilot::before_job_insert`]
    /// for each job in order.
    ///
    /// `connection` is always the insertion's transaction: River's own, or
    /// the caller's. Its owner commits or rolls it back, so implementations
    /// must not end it, and must make related writes on it.
    async fn before_jobs_insert(
        &self,
        mut connection: DatabaseConnection<'_>,
        jobs: &mut [JobInsertParams<'_>],
    ) -> Result<(), PilotError> {
        for job in jobs {
            self.before_job_insert(connection.reborrow(), job).await?;
        }
        Ok(())
    }

    /// Mutates or validates a resolved insertion using its transaction
    /// connection. Called for each job by the default
    /// [`Pilot::before_jobs_insert`].
    ///
    /// Insert middleware wraps the whole step: River runs middleware, then
    /// ordinary begin hooks, then this method, then the write. The insert and
    /// its backend notification remain in the same transaction. In a
    /// caller's transaction they all run directly in it without a savepoint,
    /// so the caller rolls back when any of them fails.
    async fn before_job_insert(
        &self,
        _connection: DatabaseConnection<'_>,
        _params: &mut JobInsertParams<'_>,
    ) -> Result<(), PilotError> {
        Ok(())
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
    /// insertion path, including batches, caller-managed transactions, and
    /// periodic jobs. Unique insertions skipped as duplicates aren't
    /// included. Returning an error rolls back the insertion. As in
    /// [`Pilot::before_jobs_insert`], `connection` is the insertion's
    /// transaction, which the extension must not end.
    async fn after_jobs_inserted(
        &self,
        _connection: DatabaseConnection<'_>,
        _params: &JobsInsertedParams<'_>,
    ) -> Result<(), PilotError> {
        Ok(())
    }

    /// Validates the extension settings of a queue's configuration, set with
    /// [`QueueConfigExt::with_extension_setting`].
    ///
    /// River calls it when a client is built and when a queue is added or
    /// updated through [`LocalQueues`](crate::LocalQueues), before the
    /// configuration takes effect. The default accepts only a configuration
    /// without extension settings.
    ///
    /// # Errors
    ///
    /// Returns an error describing settings the extension doesn't accept.
    fn validate_queue_settings(
        &self,
        queue: &str,
        settings: &Map<String, Value>,
    ) -> Result<(), PilotError> {
        if settings.is_empty() {
            return Ok(());
        }
        Err(format!("queue {queue:?} has extension settings, but no extension accepts them").into())
    }

    /// Starts the extension's session for a new generation of a queue's
    /// producer, like River Go's pilot `ProducerInit`, or returns `None` when
    /// the extension doesn't take part in this queue's claims.
    ///
    /// River calls it once the queue's record exists and before the
    /// producer's first claim. When it fails, River logs the error and
    /// retries the producer's start with backoff.
    ///
    /// When the producer stops while this call is still running, River drops
    /// its future. An extension that already created shared state by then,
    /// such as a producer row, gets no session and so no
    /// [`PilotProducer::shutdown`] for it; the same race exists between River
    /// Go's `ProducerInit` and a stop. Peers must treat such state like that
    /// of a client that exited, for example by letting it go stale.
    async fn start_producer(
        &self,
        _context: ProducerStartContext,
    ) -> Result<Option<Box<dyn PilotProducer>>, PilotError> {
        Ok(None)
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

/// Columns River selects to decode a Postgres job row, qualified by
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
/// as River can, for [`PilotProducer::claim`].
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
/// River can, for [`PilotProducer::claim`].
#[cfg(feature = "sqlite")]
#[must_use]
pub fn claimed_sqlite_job(row: &sqlx::sqlite::SqliteRow) -> ClaimedJob {
    ClaimedJob::from_decoded(crate::database::sqlite::decode_job_row(row))
}

/// A job claimed by [`PilotProducer::claim`].
///
/// River works a decoded job normally. Like a row River claims itself, an
/// undecodable one isn't worked: its attempt fails with an error describing
/// the decode failure, before hooks or middleware run, and it's retried or
/// discarded through ordinary error handling.
#[derive(Debug)]
pub struct ClaimedJob(crate::client::DecodedJob);

impl ClaimedJob {
    pub(crate) const fn from_decoded(decoded: crate::client::DecodedJob) -> Self {
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

    /// Whether `column` of a partly decoded row couldn't be decoded.
    pub(crate) fn column_undecodable(&self, column: &str) -> bool {
        self.0
            .as_ref()
            .err()
            .is_some_and(|undecodable| undecodable.columns.iter().any(|name| name == column))
    }

    /// Returns the claimed row, with any field that couldn't be decoded left
    /// empty, unless not even the row's identity could be decoded.
    pub(crate) fn row(&self) -> Option<&JobRow> {
        match &self.0 {
            Ok(job) => Some(job),
            Err(undecodable) => undecodable.row.as_deref(),
        }
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

/// A stored job to insert again, such as one set aside and retried later,
/// with [`ExtensionClient::insert_prepared`].
///
/// River inserts it like any other job: insert middleware, begin hooks, the
/// extension's insertion step, and notifications all run once, and they see
/// the stored arguments and metadata. What they return is stored, so the
/// job keeps its identity only when every step leaves a stored job alone.
/// Unique-key calculation doesn't run: the job keeps its unique key and
/// states, creation time, schedule, and metadata, and gets a new ID.
/// `encoded_args` may be any JSON value, including an array or `null`.
#[derive(Clone, Debug)]
pub struct PreparedInsertParams {
    /// Original creation time.
    pub created_at: DateTime<Utc>,
    /// Serialized job arguments.
    pub encoded_args: Box<RawValue>,
    /// Stable job kind.
    pub kind: String,
    /// Maximum attempts, including the first.
    pub max_attempts: i32,
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

/// Inputs to a peer claim's callback, from [`PeerAttempts::claim`].
#[derive(Debug)]
#[non_exhaustive]
pub struct PeerClaimContext<'c> {
    /// The coordinating attempt's cancellation token, cancelled by a hard
    /// stop or a remote cancellation of the coordinator's job. A soft stop
    /// leaves it alone.
    pub cancellation: &'c CancellationToken,
    /// This client's identifier, which claimed rows' `attempted_by` must
    /// end with.
    pub client_id: &'c str,
    /// The claim's transaction, which River commits once the callback
    /// returns and its rows pass River's checks.
    pub connection: DatabaseConnection<'c>,
    /// The client's database.
    pub database: &'c PilotDatabase,
}

/// An outcome for one peer, for [`PeerAttempts::complete`].
#[derive(Debug)]
#[non_exhaustive]
pub struct PeerOutcome {
    /// The peer, as [`PeerAttempts::claim`] returned it.
    pub job: JobRow,
    /// The peer's result, as a worker would return it.
    pub result: Result<crate::WorkOutcome, crate::BoxError>,
}

impl PeerOutcome {
    /// Creates an outcome for `job`.
    #[must_use]
    pub const fn new(job: JobRow, result: Result<crate::WorkOutcome, crate::BoxError>) -> Self {
        Self { job, result }
    }
}

/// The peers of a running attempt: jobs the attempt, their coordinator,
/// claims and completes alongside its own job, such as a group of related
/// jobs it works together.
///
/// River owns each peer from the commit of the claim that took it until its
/// outcome persists. Peers take no producer slots and never reach
/// [`PilotProducer::job_finished`]. When the coordinator's attempt ends,
/// River refuses new peer operations, waits for those it accepted, and gives
/// every peer still without an outcome one before the coordinator's own: an
/// interruption when River stopped the coordinator, and a failure otherwise,
/// including when the coordinator's job was cancelled remotely. A peer stops
/// being owned when its outcome persists, before its event, so it can be
/// claimed again at once.
///
/// A soft stop doesn't end peer operations. A coordinator keeps claiming and
/// completing peers after its producer stops fetching new jobs, until its
/// attempt ends, and the client's stop waits for the attempt and so for
/// every peer it claimed. A hard stop cancels the attempt, which ends its
/// claims.
#[derive(Clone, Copy, Debug)]
pub struct PeerAttempts<'a> {
    context: &'a crate::WorkContext,
}

impl<'a> PeerAttempts<'a> {
    /// Returns the peers of the attempt `context` belongs to.
    #[must_use]
    pub const fn new(context: &'a crate::WorkContext) -> Self {
        Self { context }
    }

    fn attempt(
        self,
    ) -> Result<
        (
            &'a crate::Client,
            &'a std::sync::Arc<crate::client::PeerLedger>,
        ),
        crate::Error,
    > {
        match (self.context.client(), self.context.peers()) {
            (Some(client), Some(peers)) => Ok((client, peers)),
            _ => Err(crate::Error::Extension {
                phase: crate::ExtensionPhase::AddOn {
                    operation: "peer attempts",
                },
                source: "peer operations require a running attempt".into(),
            }),
        }
    }

    /// Claims peers with `run` in a transaction River opens and commits.
    ///
    /// `run` must claim on the context's connection, like a producer claim:
    /// it moves rows to `running`, increments their attempt, and appends this
    /// client to `attempted_by`, and builds each result with
    /// [`claimed_postgres_job`] or [`claimed_sqlite_job`]. Before commit,
    /// River rejects the whole claim when a row can't be identified, appears
    /// twice, is the coordinator's own job, is already owned by an attempt
    /// or worked by this client, is at an attempt this coordinator already
    /// saw end, or isn't running under this client. A claim whose coordinator
    /// is cancelled before commit, by a hard stop or a remote cancellation,
    /// rolls back. A soft stop doesn't stop claims: the coordinator may keep
    /// claiming until its attempt ends. River doesn't retry a failed claim.
    ///
    /// Returns the decoded rows River now tracks. A row that couldn't be
    /// fully decoded is completed as a failure instead and not returned.
    ///
    /// # Errors
    ///
    /// Returns an [`Error::Extension`](crate::Error::Extension) error for a
    /// claim River rejected, from `run`, once the coordinator's attempt was
    /// cancelled, or once it ended, and a database error when the transaction
    /// fails.
    pub async fn claim<F>(self, run: F) -> Result<Vec<JobRow>, crate::Error>
    where
        F: for<'c> FnOnce(
                PeerClaimContext<'c>,
            ) -> futures_util::future::BoxFuture<
                'c,
                Result<Vec<ClaimedJob>, PilotError>,
            > + Send,
    {
        let (client, peers) = self.attempt()?;
        peers.claim(&client.inner, self.context, run).await
    }

    /// Completes peers through River's ordinary completion pipeline: the
    /// error handler, the coordinator's recorded metadata, retry selection,
    /// the extension's set-state step, events, and fenced persistence.
    /// Returns once every outcome persisted.
    ///
    /// Outcomes are accepted all or none: each job must be a peer of this
    /// attempt at the attempt it was claimed at, appear once, and have no
    /// outcome yet.
    ///
    /// # Errors
    ///
    /// Returns an [`Error::Extension`](crate::Error::Extension) error for
    /// outcomes River rejected or once the coordinator ended, and a runtime
    /// error when an outcome couldn't be handed to the completer or wasn't
    /// persisted. An outcome not handed over leaves its peer without one, so
    /// River supplies one when the coordinator ends.
    pub async fn complete(self, outcomes: Vec<PeerOutcome>) -> Result<(), crate::Error> {
        let (client, peers) = self.attempt()?;
        peers.complete(&client.inner, self.context, outcomes).await
    }
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
    pub attempt: i32,
    pub attempted_at: Option<DateTime<Utc>>,
    pub attempted_by: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub encoded_args: Box<RawValue>,
    pub errors: Vec<AttemptError>,
    pub finalized_at: Option<DateTime<Utc>>,
    pub kind: String,
    pub max_attempts: i32,
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
