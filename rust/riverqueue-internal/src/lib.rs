#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

#[cfg(not(any(feature = "postgres", feature = "sqlite")))]
compile_error!(
    "riverqueue-internal requires at least one database feature: `postgres` or `sqlite`"
);

use std::{fmt, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
#[cfg(feature = "postgres")]
use sqlx::{PgConnection, PgPool};
#[cfg(feature = "sqlite")]
use sqlx::{SqliteConnection, SqlitePool};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// PostgreSQL's maximum identifier length.
pub const POSTGRES_IDENTIFIER_MAX: usize = 63;

/// Longest River notification topic.
pub const NOTIFICATION_TOPIC_LONGEST: &str = "river_leadership";

/// Maximum schema length after reserving `<schema>.river_leadership`.
pub const SCHEMA_MAX_LEN: usize = POSTGRES_IDENTIFIER_MAX - NOTIFICATION_TOPIC_LONGEST.len() - 1;

/// A validated PostgreSQL schema used by River.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaName(Option<String>);

impl SchemaName {
    /// Uses PostgreSQL's current schema.
    #[must_use]
    pub const fn current() -> Self {
        Self(None)
    }

    /// Validates an optional explicit schema.
    ///
    /// Like Go's `SafeIdentifier` quoting, any name is accepted and quoted
    /// when rendered, including mixed case and punctuation such as
    /// `river-prod`. Names containing NUL are rejected, as are names too long
    /// to prefix River's notification topics within PostgreSQL's identifier
    /// limit.
    ///
    /// # Errors
    ///
    /// Returns an error when the schema is too long or contains NUL.
    pub fn new(schema: impl Into<String>) -> Result<Self, SchemaNameError> {
        let schema = schema.into();
        if schema.is_empty() {
            return Ok(Self::current());
        }
        if schema.len() > SCHEMA_MAX_LEN {
            return Err(SchemaNameError::TooLong {
                length: schema.len(),
                maximum: SCHEMA_MAX_LEN,
            });
        }
        if schema.contains('\0') {
            return Err(SchemaNameError::Invalid(schema));
        }

        Ok(Self(Some(schema)))
    }

    /// Returns the unquoted explicit schema, if configured.
    #[must_use]
    pub fn as_deref(&self) -> Option<&str> {
        self.0.as_deref()
    }

    /// Qualifies and safely quotes a database object name.
    #[must_use]
    pub fn qualify(&self, object: &str) -> String {
        match &self.0 {
            Some(schema) => format!("{}.{}", quote_identifier(schema), quote_identifier(object)),
            None => quote_identifier(object),
        }
    }

    /// Prefix used by River's canonical migration templates.
    #[must_use]
    pub fn migration_prefix(&self) -> String {
        self.0.as_ref().map_or_else(String::new, |schema| {
            format!("{}.", quote_identifier(schema))
        })
    }

    /// Fully qualified PostgreSQL notification channel.
    #[must_use]
    pub fn notification_topic(&self, topic: &str) -> String {
        match &self.0 {
            Some(schema) => format!("{schema}.{topic}"),
            None => format!("public.{topic}"),
        }
    }
}

/// Quotes a PostgreSQL identifier, doubling embedded quotes like Go's
/// `dbutil.SafeIdentifier`.
fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

impl Default for SchemaName {
    fn default() -> Self {
        Self::current()
    }
}

impl fmt::Display for SchemaName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_deref().unwrap_or("<current>"))
    }
}

/// Error type used across the exact-version internal pilot seam.
pub type PilotError = Box<dyn std::error::Error + Send + Sync>;

/// Built-in backend selected for an exact-version extension call.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseKind {
    /// PostgreSQL.
    #[cfg(feature = "postgres")]
    Postgres,
    /// SQLite.
    #[cfg(feature = "sqlite")]
    Sqlite,
}

/// Backend configuration passed through River's exact-version extension seam.
#[doc(hidden)]
#[derive(Clone, Debug)]
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
    pub state: String,
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
    /// Job ID.
    pub id: i64,
    /// Job kind.
    pub kind: String,
    /// Job metadata after the update.
    pub metadata: Map<String, Value>,
    /// Job queue.
    pub queue: String,
    /// River state string after the update.
    pub state: String,
}

/// A job row as persisted by River's set-state-if-running update.
///
/// The fields match `riverqueue::JobRow`, which this crate cannot name;
/// `riverqueue::JobRow::from_parts` rebuilds one when an extension needs it.
/// States use River's wire strings (for example `"completed"`), and attempt
/// errors keep their persisted JSON form.
#[derive(Clone, Debug)]
pub struct JobSetStateRow {
    /// Database-generated ID.
    pub id: i64,
    /// Current attempt number.
    pub attempt: i16,
    /// Last attempt time.
    pub attempted_at: Option<DateTime<Utc>>,
    /// IDs of clients that attempted the job.
    pub attempted_by: Vec<String>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Encoded job arguments.
    pub encoded_args: Box<serde_json::value::RawValue>,
    /// Persisted attempt errors in chronological order.
    pub errors: Vec<Value>,
    /// Terminal-state time.
    pub finalized_at: Option<DateTime<Utc>>,
    /// Stable job kind.
    pub kind: String,
    /// Maximum attempts.
    pub max_attempts: i16,
    /// Arbitrary and River-reserved metadata.
    pub metadata: Map<String, Value>,
    /// Priority from one through four.
    pub priority: i16,
    /// Queue name.
    pub queue: String,
    /// Earliest run time.
    pub scheduled_at: DateTime<Utc>,
    /// Current state as River's wire string.
    pub state: String,
    /// Searchable tags.
    pub tags: Vec<String>,
    /// Unique hash, if any.
    pub unique_key: Option<Vec<u8>>,
    /// States, as wire strings, in which the unique key is enforced.
    pub unique_states: Option<Vec<String>>,
}

/// Rows passed to [`Pilot::after_jobs_set_state`].
#[derive(Clone, Debug)]
pub struct JobSetStateParams {
    /// Selected database backend configuration.
    pub database: DatabaseConfig,
    /// Every job in the batch that still exists, as returned by the update,
    /// including jobs that were no longer running and so kept their state.
    pub jobs: Vec<JobSetStateRow>,
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
    pub metadata: &'insert mut Map<String, Value>,
    /// Queue in which the job will run.
    pub queue: &'insert mut String,
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

    /// Leader-owned services contributed by the extension.
    fn maintenance_services(&self) -> Vec<std::sync::Arc<dyn MaintenanceService>> {
        Vec::new()
    }

    /// Per-client services contributed by the extension.
    fn runtime_services(&self) -> Vec<std::sync::Arc<dyn RuntimeService>> {
        Vec::new()
    }
}

/// No-op pilot used by River OSS.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopPilot;

impl Pilot for NoopPilot {}

/// Invalid River schema name.
#[derive(Debug, Error)]
pub enum SchemaNameError {
    /// Schema contains a NUL character, which PostgreSQL identifiers cannot.
    #[error("schema name cannot contain NUL: {0:?}")]
    Invalid(String),

    /// Schema is too long to prefix River's notification topics.
    #[error("schema length {length} exceeds maximum {maximum}")]
    TooLong {
        /// Observed byte length.
        length: usize,
        /// Maximum byte length.
        maximum: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_name_validates_and_qualifies() {
        let schema = SchemaName::new("river_test").unwrap();
        assert_eq!(schema.qualify("river_job"), "\"river_test\".\"river_job\"");
        assert_eq!(
            schema.notification_topic("river_insert"),
            "river_test.river_insert"
        );

        // Go quotes any schema with `SafeIdentifier`, so Rust accepts the
        // same names and escapes embedded quotes.
        let hyphenated = SchemaName::new("river-prod").unwrap();
        assert_eq!(
            hyphenated.qualify("river_job"),
            "\"river-prod\".\"river_job\""
        );
        assert_eq!(
            hyphenated.notification_topic("river_insert"),
            "river-prod.river_insert"
        );
        assert_eq!(
            SchemaName::new("MyRiver").unwrap().migration_prefix(),
            "\"MyRiver\"."
        );
        assert_eq!(
            SchemaName::new("odd\"name").unwrap().qualify("river_job"),
            "\"odd\"\"name\".\"river_job\""
        );
        assert!(SchemaName::new("1leading_digit").is_ok());
        assert!(SchemaName::new("nul\0byte").is_err());
        assert!(SchemaName::new("a".repeat(SCHEMA_MAX_LEN + 1)).is_err());
    }
}
