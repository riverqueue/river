//! Job querying and update parameters.

use std::{fmt, str::FromStr};

use base64::{
    Engine as _, alphabet,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig, general_purpose},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::{JobRow, JobState};

/// Stable keyset cursor for job-list pagination.
#[derive(Clone, Debug)]
pub struct JobListCursor {
    pub(crate) id: i64,
    kind: String,
    order_by: JobListOrderBy,
    queue: String,
    /// Value of the time field the list is ordered by, or `None` when the
    /// list is ordered by ID alone or the job's value is null.
    pub(crate) sort_time: Option<DateTime<Utc>>,
}

impl JobListCursor {
    /// Builds a cursor from a returned row and the parameters used to list it.
    ///
    /// [`JobListResult::last_cursor`] already holds the cursor after a page's
    /// last job.
    ///
    /// # Errors
    ///
    /// Returns [`JobListCursorError::InvalidListParams`] when `params` are
    /// invalid.
    pub fn from_job(job: &JobRow, params: &JobListParams) -> Result<Self, JobListCursorError> {
        params
            .validate()
            .map_err(JobListCursorError::InvalidListParams)?;
        Ok(Self::after_job(job, params))
    }

    /// Builds the cursor after `job` for already validated parameters.
    pub(crate) fn after_job(job: &JobRow, params: &JobListParams) -> Self {
        // The time comes from the field the list is ordered by. For time
        // ordering over several states, that's the first state's field for
        // every job, which may differ from the field of the job's own state.
        Self {
            id: job.id,
            kind: job.kind.clone(),
            order_by: params.order_by,
            queue: job.queue.clone(),
            sort_time: params.time_field().and_then(|field| field.value(job)),
        }
    }

    /// Decodes an opaque cursor emitted by either matched implementation.
    pub fn decode(encoded: &str) -> Result<Self, JobListCursorError> {
        encoded.parse()
    }

    /// Encodes this cursor for storage in an API pagination token.
    ///
    /// The text is byte-for-byte what River Go's `JobListCursor.MarshalText`
    /// produces: padded URL-safe Base64 of Go's `encoding/json` encoding,
    /// including its string escaping and shortest fractional seconds. A
    /// cursor without a time, because its list is ordered by ID or its job's
    /// time field is null, carries Go's zero time.
    ///
    /// # Panics
    ///
    /// Panics only if Serde cannot serialize River's fixed, internally
    /// constructed cursor representation. Its fields have no fallible custom
    /// serializers, so this indicates a River implementation bug.
    pub fn encode(&self) -> String {
        let value = JobListCursorValue {
            id: self.id,
            kind: self.kind.clone(),
            queue: self.queue.clone(),
            sort_field: self.order_by.as_str().to_owned(),
            time: self.sort_time.unwrap_or_else(go_zero_time),
        };
        let json = crate::encoding::encode_args(&value)
            .expect("fixed job-list cursor value always serializes");
        general_purpose::URL_SAFE.encode(json.get())
    }
}

impl fmt::Display for JobListCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.encode())
    }
}

impl FromStr for JobListCursor {
    type Err = JobListCursorError;

    /// Accepts URL-safe or standard Base64, with or without padding.
    fn from_str(encoded: &str) -> Result<Self, Self::Err> {
        const TOLERANT: GeneralPurposeConfig =
            GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent);
        const URL_SAFE: GeneralPurpose = GeneralPurpose::new(&alphabet::URL_SAFE, TOLERANT);
        const STANDARD: GeneralPurpose = GeneralPurpose::new(&alphabet::STANDARD, TOLERANT);

        let bytes = URL_SAFE
            .decode(encoded)
            .or_else(|_| STANDARD.decode(encoded))
            .map_err(JobListCursorError::Base64)?;
        let value: JobListCursorValue =
            serde_json::from_slice(&bytes).map_err(JobListCursorError::Json)?;
        let order_by = value.sort_field.parse()?;
        Ok(Self {
            id: value.id,
            kind: value.kind,
            order_by,
            queue: value.queue,
            // The zero time stands for no time. No job's time field holds it.
            sort_time: (order_by != JobListOrderBy::Id && value.time != go_zero_time())
                .then_some(value.time),
        })
    }
}

/// Failure to build or decode a job-list cursor.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum JobListCursorError {
    /// Cursor text is neither URL-safe nor standard Base64.
    #[error("invalid job-list cursor base64: {0}")]
    Base64(#[source] base64::DecodeError),
    /// List parameters cannot produce a valid cursor.
    #[error("invalid job-list parameters: {0}")]
    InvalidListParams(String),
    /// Cursor contents are not valid JSON.
    #[error("invalid job-list cursor JSON: {0}")]
    Json(#[source] serde_json::Error),
    /// Cursor names an unsupported ordering field.
    #[error("unknown job-list cursor sort field {0:?}")]
    UnknownSortField(String),
}

#[derive(Deserialize, Serialize)]
struct JobListCursorValue {
    id: i64,
    kind: String,
    queue: String,
    sort_field: String,
    #[serde(with = "crate::encoding::go_time")]
    time: DateTime<Utc>,
}

/// Field used for stable job-list ordering.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum JobListOrderBy {
    /// Finalization time followed by ID.
    FinalizedAt,
    /// Database ID only.
    #[default]
    Id,
    /// Scheduled time followed by ID.
    ScheduledAt,
    /// State-appropriate time followed by ID.
    ///
    /// The time field is chosen by the first state given to
    /// [`JobListParams::states`], and jobs in every listed state are ordered
    /// by that field:
    ///
    /// * `available`, `pending`, `retryable`, and `scheduled` use
    ///   `scheduled_at`.
    /// * `running` uses `attempted_at`.
    /// * `cancelled`, `completed`, and `discarded` use `finalized_at`.
    ///
    /// Without a state filter, jobs are ordered by `scheduled_at`. Jobs whose
    /// field is null, like `finalized_at` for jobs that haven't finalized,
    /// come after all others in ascending order and before all others in
    /// descending order.
    Time,
}

impl JobListOrderBy {
    /// Returns the cross-language wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FinalizedAt => "finalized_at",
            Self::Id => "id",
            Self::ScheduledAt => "scheduled_at",
            Self::Time => "time",
        }
    }
}

impl fmt::Display for JobListOrderBy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for JobListOrderBy {
    type Err = JobListCursorError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "finalized_at" => Ok(Self::FinalizedAt),
            "id" => Ok(Self::Id),
            "scheduled_at" => Ok(Self::ScheduledAt),
            "time" => Ok(Self::Time),
            _ => Err(JobListCursorError::UnknownSortField(value.to_owned())),
        }
    }
}

/// Direction used for job-list ordering and cursor comparison.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SortDirection {
    /// Ascending order.
    #[default]
    Ascending,
    /// Descending order.
    Descending,
}

/// Safe filters for bulk job deletion.
#[derive(Clone, Debug)]
pub struct JobDeleteManyParams {
    pub(crate) all: bool,
    pub(crate) filter: JobListParams,
}

impl JobDeleteManyParams {
    /// Explicitly selects every non-running job, subject to the filter limit.
    #[must_use]
    pub fn all() -> Self {
        Self {
            all: true,
            filter: JobListParams::default(),
        }
    }

    /// Selects jobs matching a nonempty filter.
    #[must_use]
    pub const fn matching(filter: JobListParams) -> Self {
        Self { all: false, filter }
    }

    /// Returns whether this operation explicitly selects every job.
    #[must_use]
    pub const fn deletes_all(&self) -> bool {
        self.all
    }

    /// Returns the row filter and deletion limit.
    #[must_use]
    pub const fn filter(&self) -> &JobListParams {
        &self.filter
    }
}

/// A page of jobs returned by [`Jobs::list`](crate::Jobs::list).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct JobListResult {
    /// Jobs in the requested order.
    pub jobs: Vec<JobRow>,
    /// Cursor after the last job, to request the next page by passing it
    /// to [`JobListParams::after`] with otherwise identical parameters.
    /// `None` when the page is empty.
    pub last_cursor: Option<JobListCursor>,
}

/// Filters and pagination for listing jobs.
///
/// Filters combine with AND, and each list-valued filter matches any of its
/// values. Unset filters match every job:
///
/// ```
/// # use riverqueue::{JobListOrderBy, JobListParams, JobState, SortDirection};
/// let params = JobListParams::default()
///     .queues(["email"])
///     .states([JobState::Completed])
///     .order_by(JobListOrderBy::Time)
///     .direction(SortDirection::Descending)
///     .limit(50);
/// ```
#[derive(Clone, Debug)]
pub struct JobListParams {
    pub(crate) direction: SortDirection,
    pub(crate) ids: Vec<i64>,
    pub(crate) kinds: Vec<String>,
    pub(crate) limit: u32,
    pub(crate) metadata: Option<Map<String, Value>>,
    pub(crate) order_by: JobListOrderBy,
    pub(crate) priorities: Vec<i16>,
    pub(crate) queues: Vec<String>,
    pub(crate) start: Option<JobListStart>,
    pub(crate) states: Vec<JobState>,
    pub(crate) tags_all: Vec<String>,
    pub(crate) tags_any: Vec<String>,
}

/// Where a job listing starts. A listing continues either from a keyset
/// cursor or after an ID, never both.
#[derive(Clone, Debug)]
pub(crate) enum JobListStart {
    Cursor(JobListCursor),
    Id(i64),
}

impl Default for JobListParams {
    fn default() -> Self {
        Self {
            direction: SortDirection::Ascending,
            ids: Vec::new(),
            kinds: Vec::new(),
            limit: 100,
            metadata: None,
            order_by: JobListOrderBy::Id,
            priorities: Vec::new(),
            queues: Vec::new(),
            start: None,
            states: Vec::new(),
            tags_all: Vec::new(),
            tags_any: Vec::new(),
        }
    }
}

impl JobListParams {
    /// Returns jobs after a cursor from a previous page, usually that page's
    /// [`JobListResult::last_cursor`]. The cursor must come from a listing
    /// with the same ordering. Replaces any [`after_id`](Self::after_id).
    #[must_use]
    pub fn after(mut self, cursor: JobListCursor) -> Self {
        self.start = Some(JobListStart::Cursor(cursor));
        self
    }

    /// Returns jobs whose ID comes after `id` in the sort direction.
    /// Replaces any [`after`](Self::after) cursor.
    #[must_use]
    pub fn after_id(mut self, id: i64) -> Self {
        self.start = Some(JobListStart::Id(id));
        self
    }

    /// Sets the sort and cursor direction. Defaults to ascending.
    #[must_use]
    pub const fn direction(mut self, direction: SortDirection) -> Self {
        self.direction = direction;
        self
    }

    /// Matches jobs with any of these IDs.
    #[must_use]
    pub fn ids(mut self, ids: impl IntoIterator<Item = i64>) -> Self {
        self.ids = ids.into_iter().collect();
        self
    }

    /// Matches jobs of any of these kinds.
    #[must_use]
    pub fn kinds(mut self, kinds: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.kinds = kinds.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the maximum number of jobs returned, from one through 10,000.
    /// Defaults to 100. Listing fails with a limit outside that range.
    #[must_use]
    pub const fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Matches jobs whose metadata contains this JSON object, like
    /// PostgreSQL's `@>` operator.
    #[must_use]
    pub fn metadata(mut self, metadata: Map<String, Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Sets the field jobs are ordered by, with ID breaking ties. Defaults
    /// to [`JobListOrderBy::Id`].
    #[must_use]
    pub const fn order_by(mut self, order_by: JobListOrderBy) -> Self {
        self.order_by = order_by;
        self
    }

    /// Matches jobs with any of these priorities.
    #[must_use]
    pub fn priorities(mut self, priorities: impl IntoIterator<Item = i16>) -> Self {
        self.priorities = priorities.into_iter().collect();
        self
    }

    /// Matches jobs in any of these queues.
    #[must_use]
    pub fn queues(mut self, queues: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.queues = queues.into_iter().map(Into::into).collect();
        self
    }

    /// Matches jobs in any of these states.
    #[must_use]
    pub fn states(mut self, states: impl IntoIterator<Item = JobState>) -> Self {
        self.states = states.into_iter().collect();
        self
    }

    /// Matches jobs that have every one of these tags.
    #[must_use]
    pub fn tags_all(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tags_all = tags.into_iter().map(Into::into).collect();
        self
    }

    /// Matches jobs that have at least one of these tags.
    #[must_use]
    pub fn tags_any(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tags_any = tags.into_iter().map(Into::into).collect();
        self
    }

    /// Whether at least one narrowing predicate was supplied.
    #[must_use]
    pub fn has_filter(&self) -> bool {
        self.start.is_some()
            || !self.ids.is_empty()
            || !self.kinds.is_empty()
            || self.metadata.is_some()
            || !self.priorities.is_empty()
            || !self.queues.is_empty()
            || !self.states.is_empty()
            || !self.tags_all.is_empty()
            || !self.tags_any.is_empty()
    }

    /// Returns the keyset cursor to continue after, if any.
    pub(crate) const fn cursor(&self) -> Option<&JobListCursor> {
        match &self.start {
            Some(JobListStart::Cursor(cursor)) => Some(cursor),
            Some(JobListStart::Id(_)) | None => None,
        }
    }

    /// Returns how the list is ordered and where it resumes, for the storage
    /// backends to render as SQL.
    pub(crate) fn keyset(&self) -> JobListKeyset {
        let time_field = self.time_field();
        let nullable = time_field.is_some_and(|field| self.time_field_nullable(field));
        let after = self.start.as_ref().map(|start| match start {
            JobListStart::Id(id) => JobListAfter::Id(*id),
            JobListStart::Cursor(cursor) => match (time_field, cursor.sort_time) {
                (Some(_), Some(time)) => JobListAfter::Time {
                    id: cursor.id,
                    time,
                },
                (Some(_), None) if nullable => JobListAfter::NullTime { id: cursor.id },
                // Like Go, a cursor without a time for a field that can't be
                // null resumes by ID.
                _ => JobListAfter::Id(cursor.id),
            },
        });
        JobListKeyset {
            after,
            direction: self.direction,
            nullable,
            time_field,
        }
    }

    /// Returns the time field jobs are ordered by before ID, or `None` when
    /// ordering by ID alone.
    fn time_field(&self) -> Option<JobListTimeField> {
        match self.order_by {
            JobListOrderBy::FinalizedAt => Some(JobListTimeField::Finalized),
            JobListOrderBy::Id => None,
            JobListOrderBy::ScheduledAt => Some(JobListTimeField::Scheduled),
            // No state filter lists every state. Like Go, whose default
            // states start with `available`, use that state's field.
            JobListOrderBy::Time => Some(
                self.states
                    .first()
                    .map_or(JobListTimeField::Scheduled, |state| {
                        JobListTimeField::for_state(*state)
                    }),
            ),
        }
    }

    /// Returns whether `field` may be null for listed jobs.
    fn time_field_nullable(&self, field: JobListTimeField) -> bool {
        match field {
            JobListTimeField::Attempted => true,
            // The schema requires `finalized_at` for exactly the finalized
            // states. Unlike Go's custom SQL conditions, no Rust filter can
            // widen the state filter, so other filters don't matter here.
            JobListTimeField::Finalized => {
                self.states.is_empty()
                    || self.states.iter().any(|state| {
                        !matches!(
                            state,
                            JobState::Cancelled | JobState::Completed | JobState::Discarded
                        )
                    })
            }
            JobListTimeField::Scheduled => false,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if !(1..=10_000).contains(&self.limit) {
            return Err("job list limit must be between 1 and 10000".to_owned());
        }
        if let Some(cursor) = self.cursor()
            && cursor.order_by != self.order_by
        {
            return Err("job list cursor sort field does not match list ordering".to_owned());
        }
        if self.order_by == JobListOrderBy::FinalizedAt
            && (self.states.is_empty()
                || self.states.iter().any(|state| {
                    !matches!(
                        state,
                        JobState::Cancelled | JobState::Completed | JobState::Discarded
                    )
                }))
        {
            return Err(
                "finalized_at ordering requires only cancelled, completed, or discarded states"
                    .to_owned(),
            );
        }
        Ok(())
    }
}

/// Where a job list resumes, relative to its ordering.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum JobListAfter {
    /// Jobs whose ID follows this one in the list direction.
    Id(i64),
    /// Jobs following a cursor job whose time field is null. Nulls sort
    /// last ascending and first descending.
    NullTime { id: i64 },
    /// Jobs following a cursor job with this time.
    Time { id: i64, time: DateTime<Utc> },
}

/// How a validated job list is ordered and where it resumes. Each storage
/// backend renders it as SQL, so they order and page identically.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct JobListKeyset {
    pub(crate) after: Option<JobListAfter>,
    pub(crate) direction: SortDirection,
    /// Whether the time field may be null for listed jobs. Nulls then sort
    /// explicitly last ascending and first descending, the PostgreSQL
    /// default, so that every backend agrees and cursors can match them.
    pub(crate) nullable: bool,
    /// Time field ordered before ID, or `None` to order by ID alone.
    pub(crate) time_field: Option<JobListTimeField>,
}

impl JobListKeyset {
    /// Returns the ID of the job the list resumes after.
    pub(crate) const fn after_id(&self) -> Option<i64> {
        match self.after {
            Some(
                JobListAfter::Id(id)
                | JobListAfter::NullTime { id }
                | JobListAfter::Time { id, .. },
            ) => Some(id),
            None => None,
        }
    }

    /// Returns the time of the job the list resumes after, if that job has
    /// one.
    pub(crate) const fn after_time(&self) -> Option<DateTime<Utc>> {
        match self.after {
            Some(JobListAfter::Time { time, .. }) => Some(time),
            Some(JobListAfter::Id(_) | JobListAfter::NullTime { .. }) | None => None,
        }
    }

    /// Returns the condition selecting rows after the cursor, or `None` to
    /// start from the beginning. Backends replace the placeholders with
    /// their parameters for [`after_time`](Self::after_time) and
    /// [`after_id`](Self::after_id).
    pub(crate) fn after_sql(&self) -> Option<Vec<JobListSqlPart>> {
        use JobListSqlPart::{AfterId, AfterTime, Sql};

        let comparison = match self.direction {
            SortDirection::Ascending => ">",
            SortDirection::Descending => "<",
        };
        let id_only = || vec![Sql(format!("id {comparison} ")), AfterId];
        let after = self.after?;
        let Some(field) = self.time_field.map(JobListTimeField::column) else {
            return Some(id_only());
        };
        Some(match (after, self.direction) {
            (JobListAfter::Id(_), _) => id_only(),
            // After a null time, only nulls with a later ID follow ascending,
            // and every non-null time also follows descending.
            (JobListAfter::NullTime { .. }, SortDirection::Ascending) => {
                vec![
                    Sql(format!("({field} IS NULL AND id > ")),
                    AfterId,
                    Sql(")".to_owned()),
                ]
            }
            (JobListAfter::NullTime { .. }, SortDirection::Descending) => vec![
                Sql(format!("({field} IS NOT NULL OR id < ")),
                AfterId,
                Sql(")".to_owned()),
            ],
            (JobListAfter::Time { .. }, direction) => {
                // Nulls follow every time ascending and precede every time
                // descending.
                let or_null = if self.nullable && direction == SortDirection::Ascending {
                    format!(" OR {field} IS NULL")
                } else {
                    String::new()
                };
                vec![
                    Sql(format!("({field} {comparison} ")),
                    AfterTime,
                    Sql(format!(" OR ({field} = ")),
                    AfterTime,
                    Sql(format!(" AND id {comparison} ")),
                    AfterId,
                    Sql(format!("){or_null})")),
                ]
            }
        })
    }

    /// Returns the `ORDER BY` clause, without the keyword.
    pub(crate) fn order_sql(&self) -> String {
        let direction = match self.direction {
            SortDirection::Ascending => "ASC",
            SortDirection::Descending => "DESC",
        };
        let Some(field) = self.time_field else {
            return format!("id {direction}");
        };
        let nulls = match (self.nullable, self.direction) {
            (false, _) => "",
            (true, SortDirection::Ascending) => " NULLS LAST",
            (true, SortDirection::Descending) => " NULLS FIRST",
        };
        format!("{} {direction}{nulls}, id {direction}", field.column())
    }
}

/// A piece of a job list cursor condition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JobListSqlPart {
    /// Placeholder for the ID of the job the list resumes after.
    AfterId,
    /// Placeholder for the time of the job the list resumes after.
    AfterTime,
    /// SQL text.
    Sql(String),
}

/// Time field a job list orders by before its ID tiebreaker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JobListTimeField {
    Attempted,
    Finalized,
    Scheduled,
}

impl JobListTimeField {
    /// Returns the field that [`JobListOrderBy::Time`] uses for `state`.
    const fn for_state(state: JobState) -> Self {
        match state {
            JobState::Available | JobState::Pending | JobState::Retryable | JobState::Scheduled => {
                Self::Scheduled
            }
            JobState::Running => Self::Attempted,
            JobState::Cancelled | JobState::Completed | JobState::Discarded => Self::Finalized,
        }
    }

    /// Returns the job table column.
    pub(crate) const fn column(self) -> &'static str {
        match self {
            Self::Attempted => "attempted_at",
            Self::Finalized => "finalized_at",
            Self::Scheduled => "scheduled_at",
        }
    }

    /// Returns the field's value on `job`, or `None` if it's null.
    const fn value(self, job: &JobRow) -> Option<DateTime<Utc>> {
        match self {
            Self::Attempted => job.attempted_at,
            Self::Finalized => job.finalized_at,
            Self::Scheduled => Some(job.scheduled_at),
        }
    }
}

fn go_zero_time() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("0001-01-01T00:00:00Z")
        .expect("Go zero time is valid RFC 3339")
        .with_timezone(&Utc)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Texts from River Go's `JobListCursor.MarshalText`.
    const GO_CURSORS: [&str; 3] = [
        "eyJpZCI6NDIsImtpbmQiOiJzZW5kX2VtYWlsIiwicXVldWUiOiJwcmlvcml0eSIsInNvcnRfZmllbGQiOiJzY2hlZHVsZWRfYXQiLCJ0aW1lIjoiMjAyNi0wMS0wMlQwMzowNDowNS42Nzg5WiJ9",
        "eyJpZCI6Nywia2luZCI6ImFcdTAwM2NiXHUwMDNlXHUwMDI2Y1x1MjAyOCIsInF1ZXVlIjoiZGVmYXVsdCIsInNvcnRfZmllbGQiOiJpZCIsInRpbWUiOiIwMDAxLTAxLTAxVDAwOjAwOjAwWiJ9",
        "eyJpZCI6OTAwNzE5OTI1NDc0MDk5Mywia2luZCI6ImNvbmZvcm1hbmNlX2N1cnNvcn5-fiIsInF1ZXVlIjoiZGVmYXVsdCIsInNvcnRfZmllbGQiOiJmaW5hbGl6ZWRfYXQiLCJ0aW1lIjoiMjAyNi0wMS0wMlQwMzowNDowNS4xMloifQ==",
    ];

    fn utc(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn cursor_encodes_go_text_byte_for_byte() {
        let cursors = [
            JobListCursor {
                id: 42,
                kind: "send_email".to_owned(),
                order_by: JobListOrderBy::ScheduledAt,
                queue: "priority".to_owned(),
                sort_time: Some(utc("2026-01-02T03:04:05.6789Z")),
            },
            JobListCursor {
                id: 7,
                kind: "a<b>&c\u{2028}".to_owned(),
                order_by: JobListOrderBy::Id,
                queue: "default".to_owned(),
                sort_time: None,
            },
            JobListCursor {
                id: 9_007_199_254_740_993,
                kind: "conformance_cursor~~~".to_owned(),
                order_by: JobListOrderBy::FinalizedAt,
                queue: "default".to_owned(),
                sort_time: Some(utc("2026-01-02T03:04:05.12Z")),
            },
        ];
        for (cursor, go_text) in cursors.iter().zip(GO_CURSORS) {
            assert_eq!(cursor.encode(), go_text);
            let decoded = JobListCursor::decode(go_text).unwrap();
            assert_eq!(decoded.encode(), go_text);
            assert_eq!(decoded.kind, cursor.kind);
            assert_eq!(decoded.sort_time, cursor.sort_time);
        }
    }

    #[test]
    fn cursor_decodes_either_alphabet_with_or_without_padding() {
        let go_text = GO_CURSORS[2];
        assert!(go_text.contains('-') && go_text.ends_with("=="));
        let standard = go_text.replace('-', "+").replace('_', "/");
        for text in [
            go_text,
            go_text.trim_end_matches('='),
            &standard,
            standard.trim_end_matches('='),
        ] {
            let decoded = JobListCursor::decode(text).unwrap();
            assert_eq!(decoded.encode(), go_text, "{text}");
        }
        assert!(matches!(
            JobListCursor::decode("not base64!"),
            Err(JobListCursorError::Base64(_))
        ));
    }

    #[test]
    fn cursor_round_trips_go_compatible_text() {
        let cursor = JobListCursor {
            id: 42,
            kind: "send_email".to_owned(),
            order_by: JobListOrderBy::ScheduledAt,
            queue: "priority".to_owned(),
            sort_time: Some(
                DateTime::parse_from_rfc3339("2026-01-02T03:04:05.6789Z")
                    .unwrap()
                    .with_timezone(&Utc),
            ),
        };

        let encoded = cursor.encode();
        let decoded = JobListCursor::decode(&encoded).unwrap();
        assert_eq!(decoded.id, cursor.id);
        assert_eq!(decoded.kind, cursor.kind);
        assert_eq!(decoded.order_by, cursor.order_by);
        assert_eq!(decoded.queue, cursor.queue);
        assert_eq!(decoded.sort_time, cursor.sort_time);
    }

    #[test]
    fn zero_time_cursor_keeps_nullable_time_ordering() {
        let cursor = JobListCursor {
            id: 42,
            kind: "send_email".to_owned(),
            order_by: JobListOrderBy::Time,
            queue: "priority".to_owned(),
            sort_time: None,
        };
        let decoded = JobListCursor::decode(&cursor.encode()).unwrap();
        assert_eq!(decoded.sort_time, None);

        for (direction, expected_order) in [
            (
                SortDirection::Ascending,
                "attempted_at ASC NULLS LAST, id ASC",
            ),
            (
                SortDirection::Descending,
                "attempted_at DESC NULLS FIRST, id DESC",
            ),
        ] {
            let keyset = JobListParams::default()
                .states([JobState::Running, JobState::Available])
                .order_by(JobListOrderBy::Time)
                .direction(direction)
                .after(decoded.clone())
                .keyset();
            assert_eq!(keyset.after, Some(JobListAfter::NullTime { id: 42 }));
            assert_eq!(keyset.order_sql(), expected_order);
            assert!(keyset.after_sql().is_some());
        }
    }

    #[test]
    fn a_later_start_replaces_an_earlier_one() {
        let cursor = JobListCursor::decode(
            &JobListCursor {
                id: 7,
                kind: "kind".to_owned(),
                order_by: JobListOrderBy::Id,
                queue: "default".to_owned(),
                sort_time: None,
            }
            .encode(),
        )
        .unwrap();

        let params = JobListParams::default().after(cursor.clone()).after_id(3);
        assert_eq!(params.keyset().after_id(), Some(3));
        assert!(params.cursor().is_none());
        assert!(params.validate().is_ok());

        let params = JobListParams::default().after_id(3).after(cursor);
        assert_eq!(params.keyset().after_id(), Some(7));
        assert!(params.cursor().is_some());
        assert!(params.has_filter());
    }

    #[test]
    fn limits_outside_the_supported_range_are_rejected() {
        for limit in [0, 10_001, u32::MAX] {
            assert_eq!(
                JobListParams::default()
                    .limit(limit)
                    .validate()
                    .unwrap_err(),
                "job list limit must be between 1 and 10000"
            );
        }
        assert!(JobListParams::default().limit(10_000).validate().is_ok());
    }

    #[test]
    fn defaults_do_not_filter_states() {
        let params = JobListParams::default();

        assert!(params.states.is_empty());
        assert!(!params.has_filter());
    }

    #[test]
    fn finalized_order_requires_terminal_states() {
        let params = JobListParams::default().order_by(JobListOrderBy::FinalizedAt);

        assert_eq!(
            params.validate().unwrap_err(),
            "finalized_at ordering requires only cancelled, completed, or discarded states"
        );
    }
}

/// Mutable public job fields.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct JobUpdateParams {
    /// Merge these metadata keys into the existing object.
    pub metadata: Map<String, Value>,
    /// Set or replace the reserved job output value.
    pub output: Option<Value>,
}

impl JobUpdateParams {
    /// Merges metadata keys into the existing object.
    #[must_use]
    pub fn with_metadata(mut self, metadata: Map<String, Value>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Sets or replaces the reserved output value.
    #[must_use]
    pub fn with_output(mut self, output: Value) -> Self {
        self.output = Some(output);
        self
    }
}
