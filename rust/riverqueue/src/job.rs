//! Persisted and typed job values.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, value::RawValue};

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
    /// Arguments are encoded immediately. If encoding fails, the error is
    /// returned when the batch is inserted and no job in it is inserted.
    pub fn push<A: JobArgs>(&mut self, args: A) -> &mut Self {
        self.push_with(args, InsertOpts::default())
    }

    /// Appends a job with options overlaid on its job-type defaults.
    ///
    /// Arguments are encoded immediately. If encoding fails, the error is
    /// returned when the batch is inserted and no job in it is inserted.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "the batch takes ownership of its jobs"
    )]
    pub fn push_with<A: JobArgs>(&mut self, args: A, opts: InsertOpts) -> &mut Self {
        self.items.push(InsertBatchItem {
            defaults: A::default_insert_opts(),
            encoded_args: crate::encoding::encode_args(&args),
            kind: A::KIND,
            opts,
            unique_fields: A::unique_fields(),
        });
        self
    }
}

#[derive(Debug)]
pub(crate) struct InsertBatchItem {
    pub(crate) defaults: InsertOpts,
    pub(crate) encoded_args: Result<Box<RawValue>, serde_json::Error>,
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

/// How an [`InsertOpts`] layer affects a job's schedule.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ScheduleOverride {
    /// Schedule the job no earlier than this time.
    At(DateTime<Utc>),
    /// Make the job immediately eligible, replacing any schedule from a lower
    /// layer.
    Immediate,
    /// Keep the schedule from a lower layer, or run immediately when none
    /// sets one.
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

    /// Returns how these options affect the job's schedule.
    #[must_use]
    pub const fn scheduled_at(&self) -> ScheduleOverride {
        self.scheduled_at
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

    /// Returns these options with every option set in `overrides` replacing
    /// the corresponding option here. Options `overrides` leaves unset are
    /// kept.
    ///
    /// This is how River layers call-site options over job-type defaults, and
    /// how `#[river(insert_opts = ...)]` layers a function's options over the
    /// derive's attribute defaults.
    #[must_use]
    pub fn overlay(mut self, overrides: Self) -> Self {
        let Self {
            max_attempts,
            metadata,
            pending,
            priority,
            queue,
            scheduled_at,
            tags,
            unique,
        } = overrides;
        self.max_attempts = max_attempts.or(self.max_attempts);
        self.metadata = metadata.or(self.metadata);
        self.pending = pending.or(self.pending);
        self.priority = priority.or(self.priority);
        self.queue = queue.or(self.queue);
        if scheduled_at != ScheduleOverride::Inherit {
            self.scheduled_at = scheduled_at;
        }
        self.tags = tags.or(self.tags);
        self.unique = unique.or(self.unique);
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

impl<A> InsertResult<A> {
    /// Returns the ID of the inserted job, or of the existing job when a
    /// unique insertion was skipped.
    #[must_use]
    pub const fn id(&self) -> i64 {
        self.job.id()
    }
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

impl InsertBatchResult {
    /// Returns the ID of the inserted job, or of the existing job when a
    /// unique insertion was skipped.
    #[must_use]
    pub const fn id(&self) -> i64 {
        self.job.id
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

    /// Returns the job's database ID.
    #[must_use]
    pub const fn id(&self) -> i64 {
        self.row.id
    }
}

/// Persisted River job fields.
///
/// Arguments are kept as the exact JSON text stored with the job, so values
/// written by other River clients (including numbers beyond `f64` precision
/// and member order) are preserved when a row is read and passed along. Use
/// [`JobRow::decode_args`] to decode them into a typed value.
///
/// Metadata is decoded into a [`serde_json::Map`]. It is exact for strings,
/// booleans, and integers within the `i64`/`u64` range; other numbers are
/// approximated as `f64` in this view. River merges metadata updates in the
/// database, so values it does not change are never rewritten.
#[derive(Clone, Debug, Deserialize, Serialize)]
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
    /// Encoded job arguments as the exact JSON text stored with the job.
    pub encoded_args: Box<RawValue>,
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
    /// Creates a minimal persisted row suitable for tests and adapters.
    ///
    /// Use [`encode_args`](crate::encoding::encode_args) to encode typed
    /// arguments the same way River does when inserting them.
    #[must_use]
    pub fn new(
        id: i64,
        kind: impl Into<String>,
        encoded_args: Box<RawValue>,
        now: DateTime<Utc>,
    ) -> Self {
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

    /// Decodes the job's arguments into `T`.
    ///
    /// # Errors
    ///
    /// Returns an error when the stored arguments do not deserialize as `T`.
    pub fn decode_args<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(self.encoded_args.get())
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
    pub(crate) const fn unique_bit(self) -> u8 {
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
    pub(crate) fn state_bitmask(&self) -> u8 {
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
    fn overlay_replaces_only_options_set_in_overrides() {
        let scheduled_at = Utc::now();
        let base = InsertOpts::default()
            .with_max_attempts(9)
            .with_priority(3)
            .with_queue("base_queue")
            .with_scheduled_at(scheduled_at)
            .with_tags(["base"]);

        let kept = base.clone().overlay(InsertOpts::default());
        assert_eq!(kept.max_attempts(), Some(9));
        assert_eq!(kept.queue(), Some("base_queue"));
        assert_eq!(kept.scheduled_at(), ScheduleOverride::At(scheduled_at));
        assert_eq!(kept.tags(), Some(&["base".to_owned()][..]));

        let overlaid = base.overlay(
            InsertOpts::default()
                .with_priority(2)
                .with_tags(Vec::<String>::new())
                .with_unique(UniqueOpts::new().by_queue())
                .without_schedule(),
        );
        assert_eq!(overlaid.max_attempts(), Some(9));
        assert_eq!(overlaid.priority(), Some(2));
        assert_eq!(overlaid.queue(), Some("base_queue"));
        assert_eq!(overlaid.scheduled_at(), ScheduleOverride::Immediate);
        assert_eq!(overlaid.tags(), Some(&[][..]));
        assert!(overlaid.unique().is_some_and(UniqueOpts::uses_queue));
    }

    #[test]
    fn job_and_insert_result_expose_ids() {
        let row = JobRow::new(
            42,
            "id_test",
            crate::encoding::encode_args(&serde_json::json!({})).unwrap(),
            Utc::now(),
        );
        let job = Job::new((), row.clone());
        assert_eq!(job.id(), 42);
        assert_eq!(
            InsertResult {
                job,
                unique_skipped_as_duplicate: false,
            }
            .id(),
            42
        );
        assert_eq!(
            InsertBatchResult {
                job: row,
                unique_skipped_as_duplicate: true,
            }
            .id(),
            42
        );
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
