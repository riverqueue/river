//! Persisted and typed job values.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};

use crate::{MAX_ATTEMPTS_DEFAULT, PRIORITY_DEFAULT, QUEUE_DEFAULT};

/// Arguments for a typed River job.
pub trait JobArgs: DeserializeOwned + Send + Serialize + Sync + 'static {
    /// Stable job kind stored in PostgreSQL.
    const KIND: &'static str;

    /// Former kind names handled by the same worker during safe renames.
    fn kind_aliases() -> &'static [&'static str] {
        &[]
    }

    /// Job-type insertion defaults.
    fn default_insert_opts() -> InsertOpts {
        InsertOpts::default()
    }

    /// JSON paths selected for argument-scoped uniqueness.
    fn unique_fields() -> &'static [&'static str] {
        &[]
    }
}

/// A failed job attempt persisted in `river_job.errors`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AttemptError {
    /// Time at which the error occurred.
    pub at: DateTime<Utc>,
    /// Attempt number on which the error occurred.
    pub attempt: i16,
    /// Stringified worker error or panic value.
    pub error: String,
    /// Backtrace for a panic, otherwise empty.
    pub trace: String,
}

/// Options applied while inserting a job.
#[derive(Clone, Debug)]
pub struct InsertOpts {
    /// Maximum number of attempts, including the first.
    pub max_attempts: i16,
    /// Arbitrary JSON object metadata.
    pub metadata: Map<String, Value>,
    /// Insert in the pending state.
    pub pending: bool,
    /// Priority from one (highest) through four (lowest).
    pub priority: i16,
    /// Queue in which the job runs.
    pub queue: String,
    /// Earliest time the job may run.
    pub scheduled_at: Option<DateTime<Utc>>,
    /// Searchable tags.
    pub tags: Vec<String>,
    /// Unique job options.
    pub unique: UniqueOpts,
}

impl Default for InsertOpts {
    fn default() -> Self {
        Self {
            max_attempts: MAX_ATTEMPTS_DEFAULT,
            metadata: Map::new(),
            pending: false,
            priority: PRIORITY_DEFAULT,
            queue: QUEUE_DEFAULT.to_owned(),
            scheduled_at: None,
            tags: Vec::new(),
            unique: UniqueOpts::default(),
        }
    }
}

/// Result of inserting a job.
#[derive(Clone, Debug)]
pub struct InsertResult<A> {
    /// Inserted job or the existing matching unique job.
    pub job: Job<A>,
    /// Whether insertion was skipped because a unique job already existed.
    pub unique_skipped_as_duplicate: bool,
}

/// Type-erased result returned by River's exact-version insertion seam.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct RawInsertResult {
    /// Inserted job or the existing matching unique job.
    pub job: JobRow,
    /// Whether insertion was skipped because a unique job already existed.
    pub unique_skipped_as_duplicate: bool,
}

/// Typed job passed to a worker.
#[derive(Clone, Debug)]
pub struct Job<A> {
    /// Decoded arguments.
    pub args: A,
    /// Persisted job fields.
    pub row: JobRow,
}

/// Persisted River job fields.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobRow {
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
    pub encoded_args: Value,
    /// Failed attempts in chronological order.
    pub errors: Vec<AttemptError>,
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
    /// Current state.
    pub state: JobState,
    /// Searchable tags.
    pub tags: Vec<String>,
    /// Unique hash, if any.
    pub unique_key: Option<Vec<u8>>,
    /// States in which this job's unique key is enforced, if any.
    pub unique_states: Option<Vec<JobState>>,
}

impl JobRow {
    /// Returns recorded output from metadata.
    #[must_use]
    pub fn output(&self) -> Option<&Value> {
        self.metadata.get(crate::METADATA_KEY_OUTPUT)
    }
}

/// Persisted River job state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    /// Eligible to run now.
    Available,
    /// Manually cancelled.
    Cancelled,
    /// Successfully completed.
    Completed,
    /// Exhausted retries.
    Discarded,
    /// Parked pending external action.
    Pending,
    /// Failed and scheduled for retry.
    Retryable,
    /// Actively running.
    Running,
    /// Scheduled for the future.
    Scheduled,
}

impl JobState {
    /// All states in River's canonical bit order.
    pub const ALL: [Self; 8] = [
        Self::Available,
        Self::Cancelled,
        Self::Completed,
        Self::Discarded,
        Self::Pending,
        Self::Retryable,
        Self::Running,
        Self::Scheduled,
    ];

    /// States required for a custom unique-state set.
    pub const UNIQUE_REQUIRED: [Self; 4] = [
        Self::Available,
        Self::Pending,
        Self::Running,
        Self::Scheduled,
    ];

    /// Default states that enforce uniqueness.
    pub const UNIQUE_DEFAULT: [Self; 6] = [
        Self::Available,
        Self::Completed,
        Self::Pending,
        Self::Retryable,
        Self::Running,
        Self::Scheduled,
    ];

    /// PostgreSQL string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Cancelled => "cancelled",
            Self::Completed => "completed",
            Self::Discarded => "discarded",
            Self::Pending => "pending",
            Self::Retryable => "retryable",
            Self::Running => "running",
            Self::Scheduled => "scheduled",
        }
    }

    /// Bit used by `river_job.unique_states`.
    #[must_use]
    pub const fn unique_bit(self) -> u8 {
        match self {
            Self::Available => 0b0000_0001,
            Self::Cancelled => 0b0000_0010,
            Self::Completed => 0b0000_0100,
            Self::Discarded => 0b0000_1000,
            Self::Pending => 0b0001_0000,
            Self::Retryable => 0b0010_0000,
            Self::Running => 0b0100_0000,
            Self::Scheduled => 0b1000_0000,
        }
    }
}

impl TryFrom<&str> for JobState {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "available" => Ok(Self::Available),
            "cancelled" => Ok(Self::Cancelled),
            "completed" => Ok(Self::Completed),
            "discarded" => Ok(Self::Discarded),
            "pending" => Ok(Self::Pending),
            "retryable" => Ok(Self::Retryable),
            "running" => Ok(Self::Running),
            "scheduled" => Ok(Self::Scheduled),
            _ => Err(format!("unknown River job state {value:?}")),
        }
    }
}

/// Dimensions used to deduplicate a job.
#[derive(Clone, Debug, Default)]
pub struct UniqueOpts {
    /// Include encoded arguments.
    pub by_args: bool,
    /// Include the lower bound of this period.
    pub by_period: Option<Duration>,
    /// Include the queue.
    pub by_queue: bool,
    /// States in which the key is unique.
    pub by_state: Option<Vec<JobState>>,
    /// Exclude the job kind.
    pub exclude_kind: bool,
}

impl UniqueOpts {
    /// Whether no uniqueness dimension is enabled.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        !self.by_args
            && self.by_period.is_none()
            && !self.by_queue
            && self.by_state.is_none()
            && !self.exclude_kind
    }

    /// PostgreSQL bitmask for the configured states.
    #[must_use]
    pub fn state_bitmask(&self) -> u8 {
        self.by_state
            .as_deref()
            .unwrap_or(&JobState::UNIQUE_DEFAULT)
            .iter()
            .fold(0, |mask, state| mask | state.unique_bit())
    }

    /// Validates River's uniqueness invariants.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(period) = self.by_period
            && period < Duration::from_secs(1)
        {
            return Err("unique period must be at least one second".to_owned());
        }
        if let Some(states) = &self.by_state {
            let missing = JobState::UNIQUE_REQUIRED
                .iter()
                .filter(|state| !states.contains(state))
                .map(|state| state.as_str())
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                return Err(format!(
                    "unique states must contain required states: {}",
                    missing.join(", ")
                ));
            }
        }
        Ok(())
    }
}
