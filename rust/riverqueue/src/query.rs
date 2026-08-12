//! Job querying and update parameters.

use std::{fmt, str::FromStr};

use base64::{Engine as _, engine::general_purpose};
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
    pub(crate) sort_time: Option<DateTime<Utc>>,
}

impl JobListCursor {
    /// Builds a cursor from a returned row and the parameters used to list it.
    pub fn from_job(job: &JobRow, params: &JobListParams) -> Result<Self, JobListCursorError> {
        params
            .validate()
            .map_err(JobListCursorError::InvalidListParams)?;
        let sort_time = match params.order_by {
            JobListOrderBy::Id => None,
            JobListOrderBy::FinalizedAt => job.finalized_at,
            JobListOrderBy::ScheduledAt => Some(job.scheduled_at),
            JobListOrderBy::Time if params.states.is_empty() => None,
            JobListOrderBy::Time => match job.state {
                JobState::Available
                | JobState::Pending
                | JobState::Retryable
                | JobState::Scheduled => Some(job.scheduled_at),
                JobState::Running => job.attempted_at.or(Some(job.created_at)),
                JobState::Cancelled | JobState::Completed | JobState::Discarded => {
                    job.finalized_at.or(Some(job.created_at))
                }
            },
        };
        Ok(Self {
            id: job.id,
            kind: job.kind.clone(),
            order_by: params.order_by,
            queue: job.queue.clone(),
            sort_time,
        })
    }

    /// Decodes an opaque cursor emitted by either matched implementation.
    pub fn decode(encoded: &str) -> Result<Self, JobListCursorError> {
        encoded.parse()
    }

    /// Encodes this cursor for storage in an API pagination token.
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
        let json =
            serde_json::to_vec(&value).expect("fixed job-list cursor value always serializes");
        general_purpose::URL_SAFE.encode(json)
    }
}

impl fmt::Display for JobListCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.encode())
    }
}

impl FromStr for JobListCursor {
    type Err = JobListCursorError;

    fn from_str(encoded: &str) -> Result<Self, Self::Err> {
        let bytes = general_purpose::URL_SAFE
            .decode(encoded)
            .or_else(|_| general_purpose::STANDARD.decode(encoded))
            .map_err(JobListCursorError::Base64)?;
        let value: JobListCursorValue =
            serde_json::from_slice(&bytes).map_err(JobListCursorError::Json)?;
        let order_by = value.sort_field.parse()?;
        Ok(Self {
            id: value.id,
            kind: value.kind,
            order_by,
            queue: value.queue,
            sort_time: (order_by != JobListOrderBy::Id).then_some(value.time),
        })
    }
}

/// Failure to build or decode a job-list cursor.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum JobListCursorError {
    /// Cursor text is not URL-safe or standard Base64.
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

/// Filters and pagination for listing jobs.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct JobListParams {
    /// Return rows after this stable keyset cursor.
    pub after: Option<JobListCursor>,
    /// Return IDs greater than this cursor.
    pub after_id: Option<i64>,
    /// Match any of these IDs.
    pub ids: Vec<i64>,
    /// Match any of these kinds.
    pub kinds: Vec<String>,
    /// Maximum rows, from one through 10,000.
    pub limit: i32,
    /// Require metadata to contain this JSON object.
    pub metadata: Option<Map<String, Value>>,
    /// Stable primary sort field.
    pub order_by: JobListOrderBy,
    /// Match any of these priorities.
    pub priorities: Vec<i16>,
    /// Match any of these queues.
    pub queues: Vec<String>,
    /// Match any of these states.
    pub states: Vec<JobState>,
    /// Sort and cursor direction.
    pub direction: SortDirection,
    /// Require every tag.
    pub tags_all: Vec<String>,
    /// Require at least one tag.
    pub tags_any: Vec<String>,
}

impl Default for JobListParams {
    fn default() -> Self {
        Self {
            after: None,
            after_id: None,
            ids: Vec::new(),
            kinds: Vec::new(),
            limit: 100,
            metadata: None,
            order_by: JobListOrderBy::Id,
            priorities: Vec::new(),
            queues: Vec::new(),
            states: Vec::new(),
            direction: SortDirection::Ascending,
            tags_all: Vec::new(),
            tags_any: Vec::new(),
        }
    }
}

impl JobListParams {
    /// Sets the keyset cursor after which rows are returned.
    #[must_use]
    pub fn with_after(mut self, cursor: JobListCursor) -> Self {
        self.after = Some(cursor);
        self
    }

    /// Restricts results to explicit IDs.
    #[must_use]
    pub fn with_ids(mut self, ids: impl IntoIterator<Item = i64>) -> Self {
        self.ids = ids.into_iter().collect();
        self
    }

    /// Restricts results to job kinds.
    #[must_use]
    pub fn with_kinds(mut self, kinds: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.kinds = kinds.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the maximum returned rows.
    #[must_use]
    pub const fn with_limit(mut self, limit: i32) -> Self {
        self.limit = limit;
        self
    }

    /// Sets the stable primary ordering.
    #[must_use]
    pub const fn with_order_by(mut self, order_by: JobListOrderBy) -> Self {
        self.order_by = order_by;
        self
    }

    /// Requires at least one of the supplied tags.
    #[must_use]
    pub fn with_tags_any(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tags_any = tags.into_iter().map(Into::into).collect();
        self
    }

    /// Whether at least one narrowing predicate was supplied.
    #[must_use]
    pub fn has_filter(&self) -> bool {
        self.after_id.is_some()
            || self.after.is_some()
            || !self.ids.is_empty()
            || !self.kinds.is_empty()
            || self.metadata.is_some()
            || !self.priorities.is_empty()
            || !self.queues.is_empty()
            || !self.states.is_empty()
            || !self.tags_all.is_empty()
            || !self.tags_any.is_empty()
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if !(1..=10_000).contains(&self.limit) {
            return Err("job list limit must be between 1 and 10000".to_owned());
        }
        if self.after.is_some() && self.after_id.is_some() {
            return Err("job list cannot specify both after and after_id".to_owned());
        }
        if let Some(cursor) = &self.after
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
        if self.after.as_ref().is_some_and(|cursor| {
            self.order_by != JobListOrderBy::Id
                && !(self.order_by == JobListOrderBy::Time && self.states.is_empty())
                && cursor.sort_time.is_none()
        }) {
            return Err("job list cursor does not contain its sort time".to_owned());
        }
        Ok(())
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
    fn defaults_do_not_filter_states() {
        let params = JobListParams::default();

        assert!(params.states.is_empty());
        assert!(!params.has_filter());
    }

    #[test]
    fn finalized_order_requires_terminal_states() {
        let params = JobListParams {
            order_by: JobListOrderBy::FinalizedAt,
            ..JobListParams::default()
        };

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
