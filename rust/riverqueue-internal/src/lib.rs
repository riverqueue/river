//! Unstable implementation details shared by River's Rust crates.
//!
//! Nothing in this crate is a public compatibility promise. Applications
//! should depend on `riverqueue` instead.

#![forbid(unsafe_code)]

use std::{fmt, time::Duration};

use async_trait::async_trait;
use serde_json::{Map, Value};
use sqlx::{PgConnection, PgPool};
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
    /// River schema.
    pub schema: SchemaName,
}

/// Inputs available while selecting stuck jobs under a rescue transaction.
#[derive(Clone, Debug)]
pub struct RescueParams {
    /// Maximum rows to lock.
    pub maximum: i64,
    /// Age at which the OSS runtime considers a running job stuck.
    pub rescue_after: Duration,
    /// River schema.
    pub schema: SchemaName,
}

/// Data available before River persists a worker result.
#[derive(Clone, Debug)]
pub struct CompletionParams {
    /// Job ID being finalized or rescheduled.
    pub job_id: i64,
    /// Metadata additions produced by work.
    pub metadata_updates: Map<String, Value>,
    /// River schema.
    pub schema: SchemaName,
    /// Proposed River state string.
    pub state: String,
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
        pool: PgPool,
        schema: SchemaName,
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
        pool: PgPool,
        schema: SchemaName,
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
    /// Whether the OSS per-job cleaner must leave workflow jobs for an
    /// extension-owned workflow-aware cleaner.
    fn excludes_workflow_jobs_from_cleaner(&self) -> bool {
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

    /// Whether completion must enter the exact-version interception
    /// transaction. Returning `false` lets OSS use its one-statement fast
    /// path; implementations that override `before_job_completion` return
    /// `true`.
    fn intercepts_completion(&self) -> bool {
        false
    }

    /// Optionally selects and locks fetch candidates using the provided
    /// transaction connection. Returned IDs are claimed by the OSS runtime in
    /// the same transaction. `None` delegates selection to River OSS.
    async fn select_job_ids(
        &self,
        _connection: &mut PgConnection,
        _params: &FetchParams,
    ) -> Result<Option<Vec<i64>>, PilotError> {
        Ok(None)
    }

    /// Optionally selects and locks stuck-job candidates. Returned IDs are
    /// rescued by the OSS runtime in the same transaction.
    async fn select_rescue_job_ids(
        &self,
        _connection: &mut PgConnection,
        _params: &RescueParams,
    ) -> Result<Option<Vec<i64>>, PilotError> {
        Ok(None)
    }

    /// Intercepts completion within River's completion transaction.
    async fn before_job_completion(
        &self,
        _connection: &mut PgConnection,
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
