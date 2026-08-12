#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

#[cfg(not(any(feature = "postgres", feature = "sqlite")))]
compile_error!(
    "riverqueue-internal requires at least one database feature: `postgres` or `sqlite`"
);

use std::{fmt, time::Duration};

use async_trait::async_trait;
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
    /// # Errors
    ///
    /// Returns an error when the schema is too long or is not a safe
    /// PostgreSQL identifier.
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

        let mut chars = schema.chars();
        let starts_validly = chars
            .next()
            .is_some_and(|character| character == '_' || character.is_ascii_alphabetic());
        let remainder_valid =
            chars.all(|character| character == '_' || character.is_ascii_alphanumeric());
        if !starts_validly || !remainder_valid {
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
            Some(schema) => format!("\"{schema}\".\"{object}\""),
            None => format!("\"{object}\""),
        }
    }

    /// Prefix used by River's canonical migration templates.
    #[must_use]
    pub fn migration_prefix(&self) -> String {
        self.0
            .as_ref()
            .map_or_else(String::new, |schema| format!("\"{schema}\"."))
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
#[derive(Clone, Debug)]
pub struct RescueParams {
    /// Maximum rows to lock.
    pub maximum: i64,
    /// Age at which the OSS runtime considers a running job stuck.
    pub rescue_after: Duration,
    /// Selected database backend configuration.
    pub database: DatabaseConfig,
}

/// Data available before River persists a worker result.
#[derive(Clone, Debug)]
pub struct CompletionParams {
    /// Selected database backend configuration.
    pub database: DatabaseConfig,
    /// Job ID being finalized or rescheduled.
    pub job_id: i64,
    /// Metadata additions produced by work.
    pub metadata_updates: Map<String, Value>,
    /// Proposed River state string.
    pub state: String,
}

/// Mutable job insertion fields exposed to an exact-version extension.
///
/// The references point into River's resolved insertion context. Changes are
/// validated and persisted by the ordinary insertion pipeline after the
/// extension returns.
#[doc(hidden)]
pub struct JobInsertParams<'insert> {
    /// Serialized job arguments.
    pub encoded_args: &'insert mut Value,
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

/// Whether the OSS completer should perform its normal row update.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CompletionAction {
    /// Continue through the OSS completion query.
    #[default]
    Continue,
    /// The pilot handled completion transactionally.
    Handled,
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
    /// Metadata keys whose jobs are owned by an extension-specific cleaner.
    fn job_cleaner_metadata_exclusions(&self) -> &'static [&'static str] {
        &[]
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

    /// Whether completion must enter the exact-version interception
    /// transaction. Returning `false` lets OSS use its one-statement fast
    /// path; implementations that override `before_job_completion` return
    /// `true`.
    fn intercepts_completion(&self) -> bool {
        false
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

    /// Optionally selects and locks stuck-job candidates. Returned IDs are
    /// rescued by the OSS runtime in the same transaction.
    async fn select_rescue_job_ids(
        &self,
        _connection: DatabaseConnection<'_>,
        _params: &RescueParams,
    ) -> Result<Option<Vec<i64>>, PilotError> {
        Ok(None)
    }

    /// Intercepts completion within River's completion transaction.
    async fn before_job_completion(
        &self,
        _connection: DatabaseConnection<'_>,
        _params: &CompletionParams,
    ) -> Result<CompletionAction, PilotError> {
        Ok(CompletionAction::Continue)
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
    /// Schema contains unsupported characters.
    #[error(
        "schema name can only contain letters, numbers, and underscores, and must start with a letter or underscore: {0:?}"
    )]
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

        assert!(SchemaName::new("1bad").is_err());
        assert!(SchemaName::new("bad-name").is_err());
    }
}
