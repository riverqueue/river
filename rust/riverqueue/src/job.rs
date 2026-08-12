//! Persisted and typed job values.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};

use crate::{PRIORITY_DEFAULT, QUEUE_DEFAULT};

/// Arguments for a typed River job.
pub trait JobArgs: DeserializeOwned + Send + Serialize + Sync + 'static {
    /// Stable job kind stored with each job.
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

/// An atomic insertion batch that can contain multiple job argument types.
///
/// Items retain their [`JobArgs`] insertion defaults and may additionally set
/// per-item [`InsertOpts`]. Results are returned in the same order.
#[derive(Debug, Default)]
pub struct InsertBatch {
    pub(crate) items: Vec<InsertBatchItem>,
}

impl InsertBatch {
    /// Creates an empty batch.
    #[must_use]
    pub const fn new() -> Self {
        Self { items: Vec::new() }
    }

    /// Returns whether the batch contains no jobs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the number of jobs in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Appends a job using its job-type defaults.
    ///
    /// # Errors
    ///
    /// Returns an error when the arguments cannot be encoded as JSON.
    pub fn push<A: JobArgs>(&mut self, args: A) -> Result<&mut Self, serde_json::Error> {
        self.push_with(args, InsertOpts::default())
    }

    /// Appends a job with options overlaid on its job-type defaults.
    ///
    /// # Errors
    ///
    /// Returns an error when the arguments cannot be encoded as JSON.
    pub fn push_with<A: JobArgs>(
        &mut self,
        args: A,
        opts: InsertOpts,
    ) -> Result<&mut Self, serde_json::Error> {
        self.items.push(InsertBatchItem {
            defaults: A::default_insert_opts(),
            encoded_args: serde_json::to_value(args)?,
            kind: A::KIND,
            opts,
            unique_fields: A::unique_fields(),
        });
        Ok(self)
    }

    /// Appends a job using its job-type defaults and returns the batch for
    /// chaining.
    ///
    /// # Errors
    ///
    /// Returns an error when the arguments cannot be encoded as JSON.
    pub fn with<A: JobArgs>(mut self, args: A) -> Result<Self, serde_json::Error> {
        self.push(args)?;
        Ok(self)
    }

    /// Appends a job with insertion options and returns the batch for chaining.
    ///
    /// # Errors
    ///
    /// Returns an error when the arguments cannot be encoded as JSON.
    pub fn with_options<A: JobArgs>(
        mut self,
        args: A,
        opts: InsertOpts,
    ) -> Result<Self, serde_json::Error> {
        self.push_with(args, opts)?;
        Ok(self)
    }
}

#[derive(Debug)]
pub(crate) struct InsertBatchItem {
    pub(crate) defaults: InsertOpts,
    pub(crate) encoded_args: Value,
    pub(crate) kind: &'static str,
    pub(crate) opts: InsertOpts,
    pub(crate) unique_fields: &'static [&'static str],
}

/// A failed job attempt persisted in `river_job.errors`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
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

impl AttemptError {
    /// Creates a persisted attempt error without a panic trace.
    #[must_use]
    pub fn new(at: DateTime<Utc>, attempt: i16, error: impl Into<String>) -> Self {
        Self {
            at,
            attempt,
            error: error.into(),
            trace: String::new(),
        }
    }

    /// Sets the captured panic trace.
    #[must_use]
    pub fn with_trace(mut self, trace: impl Into<String>) -> Self {
        self.trace = trace.into();
        self
    }
}

/// Partial options applied while inserting a job.
///
/// Options declared by [`JobArgs::default_insert_opts`] are overlaid on River
/// and client defaults. Options supplied to an insertion call are then overlaid
/// on the job-type options. A value is therefore never treated as "unset"
/// merely because it happens to equal River's default.
#[derive(Clone, Debug, Default)]
pub struct InsertOpts {
    max_attempts: Option<i16>,
    metadata: Option<Map<String, Value>>,
    pending: Option<bool>,
    priority: Option<i16>,
    queue: Option<String>,
    scheduled_at: ScheduleOverride,
    tags: Option<Vec<String>>,
    unique: Option<UniqueOpts>,
}

#[derive(Clone, Copy, Debug, Default)]
enum ScheduleOverride {
    At(DateTime<Utc>),
    Immediate,
    #[default]
    Inherit,
}

impl InsertOpts {
    /// Returns the configured maximum attempts override.
    #[must_use]
    pub const fn max_attempts(&self) -> Option<i16> {
        self.max_attempts
    }

    /// Returns the configured metadata replacement.
    #[must_use]
    pub fn metadata(&self) -> Option<&Map<String, Value>> {
        self.metadata.as_ref()
    }

    /// Returns the configured pending-state override.
    #[must_use]
    pub const fn pending(&self) -> Option<bool> {
        self.pending
    }

    /// Returns the configured priority override.
    #[must_use]
    pub const fn priority(&self) -> Option<i16> {
        self.priority
    }

    /// Returns the configured queue override.
    #[must_use]
    pub fn queue(&self) -> Option<&str> {
        self.queue.as_deref()
    }

    /// Returns the schedule override. The outer option indicates whether an
    /// override was supplied; the inner option selects scheduled versus
    /// immediately eligible work.
    #[must_use]
    pub const fn scheduled_at(&self) -> Option<Option<DateTime<Utc>>> {
        match self.scheduled_at {
            ScheduleOverride::At(scheduled_at) => Some(Some(scheduled_at)),
            ScheduleOverride::Immediate => Some(None),
            ScheduleOverride::Inherit => None,
        }
    }

    /// Returns the configured tags replacement.
    #[must_use]
    pub fn tags(&self) -> Option<&[String]> {
        self.tags.as_deref()
    }

    /// Returns the configured uniqueness replacement.
    #[must_use]
    pub const fn unique(&self) -> Option<&UniqueOpts> {
        self.unique.as_ref()
    }

    /// Overrides the maximum number of attempts, including the first.
    #[must_use]
    pub const fn with_max_attempts(mut self, maximum: i16) -> Self {
        self.max_attempts = Some(maximum);
        self
    }

    /// Replaces arbitrary JSON object metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: Map<String, Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Selects whether to insert in the pending state.
    #[must_use]
    pub const fn with_pending(mut self, pending: bool) -> Self {
        self.pending = Some(pending);
        self
    }

    /// Overrides priority from one (highest) through four (lowest).
    #[must_use]
    pub const fn with_priority(mut self, priority: i16) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Overrides the queue in which the job runs.
    #[must_use]
    pub fn with_queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Schedules the job no earlier than `scheduled_at`.
    #[must_use]
    pub const fn with_scheduled_at(mut self, scheduled_at: DateTime<Utc>) -> Self {
        self.scheduled_at = ScheduleOverride::At(scheduled_at);
        self
    }

    /// Explicitly overrides a job-type schedule to make the job immediately
    /// eligible.
    #[must_use]
    pub const fn without_schedule(mut self) -> Self {
        self.scheduled_at = ScheduleOverride::Immediate;
        self
    }

    /// Replaces searchable tags.
    #[must_use]
    pub fn with_tags(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tags = Some(tags.into_iter().map(Into::into).collect());
        self
    }

    /// Replaces unique-job options.
    #[must_use]
    pub fn with_unique(mut self, unique: UniqueOpts) -> Self {
        self.unique = Some(unique);
        self
    }

    pub(crate) fn resolve(
        client_max_attempts: i16,
        job_defaults: Self,
        call_overrides: Self,
    ) -> InsertParams {
        let mut resolved = InsertParams {
            max_attempts: client_max_attempts,
            metadata: Map::new(),
            pending: false,
            priority: PRIORITY_DEFAULT,
            queue: QUEUE_DEFAULT.to_owned(),
            scheduled_at: None,
            tags: Vec::new(),
            unique: UniqueOpts::default(),
        };
        resolved.apply(job_defaults);
        resolved.apply(call_overrides);
        resolved
    }
}

/// Fully resolved insertion parameters visible to insertion extensions.
///
/// River resolves these from call, job-type, client, and library defaults
/// before invoking hooks or middleware. Extensions may mutate them before
/// validation and persistence.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct InsertParams {
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

impl InsertParams {
    fn apply(&mut self, options: InsertOpts) {
        if let Some(value) = options.max_attempts {
            self.max_attempts = value;
        }
        if let Some(value) = options.metadata {
            self.metadata = value;
        }
        if let Some(value) = options.pending {
            self.pending = value;
        }
        if let Some(value) = options.priority {
            self.priority = value;
        }
        if let Some(value) = options.queue {
            self.queue = value;
        }
        match options.scheduled_at {
            ScheduleOverride::At(value) => self.scheduled_at = Some(value),
            ScheduleOverride::Immediate => self.scheduled_at = None,
            ScheduleOverride::Inherit => {}
        }
        if let Some(value) = options.tags {
            self.tags = value;
        }
        if let Some(value) = options.unique {
            self.unique = value;
        }
    }
}

/// Result of inserting a job.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct InsertResult<A> {
    /// Inserted job or the existing matching unique job.
    pub job: Job<A>,
    /// Whether insertion was skipped because a unique job already existed.
    pub unique_skipped_as_duplicate: bool,
}

/// Type-erased result from inserting an item in an [`InsertBatch`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct InsertBatchResult {
    /// Inserted job or the existing matching unique job.
    pub job: JobRow,
    /// Whether insertion was skipped because a unique job already existed.
    pub unique_skipped_as_duplicate: bool,
}

/// Type-erased result returned by River's exact-version insertion seam.
#[doc(hidden)]
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
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ExtensionInsertParams {
    /// Original creation time.
    pub created_at: DateTime<Utc>,
    /// Serialized job arguments.
    pub encoded_args: Value,
    /// Stable job kind.
    pub kind: String,
    /// Maximum attempts, including the first.
    pub max_attempts: i16,
    /// Arbitrary job metadata.
    pub metadata: Map<String, Value>,
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
#[doc(hidden)]
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
    #[doc(hidden)]
    #[must_use]
    pub fn into_typed<A>(self, args: A) -> InsertResult<A> {
        InsertResult {
            job: Job::new(args, self.job),
            unique_skipped_as_duplicate: self.unique_skipped_as_duplicate,
        }
    }
}

/// Typed job passed to a worker.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Job<A> {
    /// Decoded arguments.
    pub args: A,
    /// Persisted job fields.
    pub row: JobRow,
}

impl<A> Job<A> {
    /// Creates a typed job from decoded arguments and a persisted row.
    /// This is primarily useful for worker unit tests.
    #[must_use]
    pub const fn new(args: A, row: JobRow) -> Self {
        Self { args, row }
    }
}

/// Persisted River job fields.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[non_exhaustive]
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

/// Complete persisted job fields for exact-version record conversion.
#[doc(hidden)]
pub struct JobRowParts {
    pub id: i64,
    pub attempt: i16,
    pub attempted_at: Option<DateTime<Utc>>,
    pub attempted_by: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub encoded_args: Value,
    pub errors: Vec<AttemptError>,
    pub finalized_at: Option<DateTime<Utc>>,
    pub kind: String,
    pub max_attempts: i16,
    pub metadata: Map<String, Value>,
    pub priority: i16,
    pub queue: String,
    pub scheduled_at: DateTime<Utc>,
    pub state: JobState,
    pub tags: Vec<String>,
    pub unique_key: Option<Vec<u8>>,
    pub unique_states: Option<Vec<JobState>>,
}

impl JobRow {
    /// Converts complete fields from an exact-version database record.
    #[doc(hidden)]
    #[must_use]
    pub fn from_parts(parts: JobRowParts) -> Self {
        Self {
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
    /// Creates a minimal persisted row suitable for tests and adapters.
    #[must_use]
    pub fn new(id: i64, kind: impl Into<String>, encoded_args: Value, now: DateTime<Utc>) -> Self {
        Self {
            attempt: 0,
            attempted_at: None,
            attempted_by: Vec::new(),
            created_at: now,
            encoded_args,
            errors: Vec::new(),
            finalized_at: None,
            id,
            kind: kind.into(),
            max_attempts: crate::MAX_ATTEMPTS_DEFAULT,
            metadata: Map::new(),
            priority: crate::PRIORITY_DEFAULT,
            queue: crate::QUEUE_DEFAULT.to_owned(),
            scheduled_at: now,
            state: JobState::Available,
            tags: Vec::new(),
            unique_key: None,
            unique_states: None,
        }
    }

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

    /// Canonical database string representation.
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

/// Failure to parse a River job state.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown River job state {value:?}")]
pub struct JobStateParseError {
    value: String,
}

impl JobStateParseError {
    /// Returns the unrecognized state value.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl std::str::FromStr for JobState {
    type Err = JobStateParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "available" => Ok(Self::Available),
            "cancelled" => Ok(Self::Cancelled),
            "completed" => Ok(Self::Completed),
            "discarded" => Ok(Self::Discarded),
            "pending" => Ok(Self::Pending),
            "retryable" => Ok(Self::Retryable),
            "running" => Ok(Self::Running),
            "scheduled" => Ok(Self::Scheduled),
            _ => Err(JobStateParseError {
                value: value.to_owned(),
            }),
        }
    }
}

impl TryFrom<&str> for JobState {
    type Error = JobStateParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

/// Dimensions used to deduplicate a job.
#[derive(Clone, Debug, Default)]
pub struct UniqueOpts {
    /// Include encoded arguments.
    pub(crate) by_args: bool,
    /// Include the lower bound of this period.
    pub(crate) by_period: Option<Duration>,
    /// Include the queue.
    pub(crate) by_queue: bool,
    /// States in which the key is unique.
    pub(crate) by_state: Option<Vec<JobState>>,
    /// Exclude the job kind.
    pub(crate) exclude_kind: bool,
}

impl UniqueOpts {
    /// Creates disabled uniqueness options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            by_args: false,
            by_period: None,
            by_queue: false,
            by_state: None,
            exclude_kind: false,
        }
    }

    /// Includes encoded arguments in the unique key.
    #[must_use]
    pub const fn by_args(mut self) -> Self {
        self.by_args = true;
        self
    }

    /// Includes the lower bound of a period in the unique key.
    #[must_use]
    pub const fn by_period(mut self, period: Duration) -> Self {
        self.by_period = Some(period);
        self
    }

    /// Includes the queue in the unique key.
    #[must_use]
    pub const fn by_queue(mut self) -> Self {
        self.by_queue = true;
        self
    }

    /// Uses a custom set of states in which the key is unique.
    #[must_use]
    pub fn by_states(mut self, states: impl IntoIterator<Item = JobState>) -> Self {
        self.by_state = Some(states.into_iter().collect());
        self
    }

    /// Returns the configured period component.
    #[must_use]
    pub const fn period(&self) -> Option<Duration> {
        self.by_period
    }

    /// Returns the configured custom state set.
    #[must_use]
    pub fn states(&self) -> Option<&[JobState]> {
        self.by_state.as_deref()
    }

    /// Returns whether encoded arguments are included.
    #[must_use]
    pub const fn uses_args(&self) -> bool {
        self.by_args
    }

    /// Returns whether the job kind is excluded.
    #[must_use]
    pub const fn excludes_kind(&self) -> bool {
        self.exclude_kind
    }

    /// Returns whether the queue is included.
    #[must_use]
    pub const fn uses_queue(&self) -> bool {
        self.by_queue
    }

    /// Excludes the job kind from the unique key.
    #[must_use]
    pub const fn without_kind(mut self) -> Self {
        self.exclude_kind = true;
        self
    }

    /// Whether no uniqueness dimension is enabled.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        !self.by_args
            && self.by_period.is_none()
            && !self.by_queue
            && self.by_state.is_none()
            && !self.exclude_kind
    }

    /// Canonical persisted bitmask for the configured states.
    #[must_use]
    pub fn state_bitmask(&self) -> u8 {
        self.by_state
            .as_deref()
            .unwrap_or(&JobState::UNIQUE_DEFAULT)
            .iter()
            .fold(0, |mask, state| mask | state.unique_bit())
    }

    /// Validates River's uniqueness invariants.
    pub(crate) fn validate(&self) -> Result<(), String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MAX_ATTEMPTS_DEFAULT;

    #[test]
    fn insertion_options_resolve_by_layer_without_sentinels() {
        let job_defaults = InsertOpts::default()
            .with_max_attempts(9)
            .with_priority(3)
            .with_queue("job_queue")
            .with_scheduled_at(Utc::now());
        let resolved = InsertOpts::resolve(
            7,
            job_defaults,
            InsertOpts::default().with_priority(2).without_schedule(),
        );

        assert_eq!(resolved.max_attempts, 9);
        assert_eq!(resolved.priority, 2);
        assert_eq!(resolved.queue, "job_queue");
        assert_eq!(resolved.scheduled_at, None);
    }

    #[test]
    fn explicit_river_default_overrides_a_job_default() {
        let resolved = InsertOpts::resolve(
            7,
            InsertOpts::default()
                .with_max_attempts(9)
                .with_priority(3)
                .with_queue("job_queue"),
            InsertOpts::default()
                .with_max_attempts(MAX_ATTEMPTS_DEFAULT)
                .with_priority(PRIORITY_DEFAULT)
                .with_queue(QUEUE_DEFAULT),
        );

        assert_eq!(resolved.max_attempts, MAX_ATTEMPTS_DEFAULT);
        assert_eq!(resolved.priority, PRIORITY_DEFAULT);
        assert_eq!(resolved.queue, QUEUE_DEFAULT);
    }
}
