//! Decoding of persisted job rows.
//!
//! River Go decodes rows with `encoding/json` and native integers, so a row
//! written by Go (or edited by an operator) can contain values that a strict
//! Rust decoder rejects, like SQLite integers outside `i16`, and on SQLite,
//! JSON columns can be changed to any shape.
//!
//! Like River Go, reads that return a job to a caller decode strictly, while
//! the runtime decodes each row it has claimed, completed, or found stuck on
//! its own and tolerates fields that can't be decoded. Such a row keeps the
//! fields that could be decoded, leaves the others empty, and carries the
//! decode error, so one bad row can't fail or strand the rows read with it.

use std::fmt::Display;

#[cfg(feature = "postgres")]
use chrono::{DateTime, Utc};
#[cfg(feature = "postgres")]
use serde_json::{Value, value::RawValue};
#[cfg(feature = "postgres")]
use sqlx::{FromRow, Row, postgres::PgRow, types::Json};
use tracing::error;

use crate::JobRow;
#[cfg(feature = "postgres")]
use crate::{AttemptError, Error, JobState};

/// A row with fields that River couldn't decode.
#[derive(Debug)]
pub(crate) struct UndecodableJob {
    /// Why the row couldn't be decoded, with a line for each field that
    /// couldn't be, like River Go's joined decode errors.
    pub(crate) error: String,
    /// The row with every field that could be decoded and the others left
    /// empty, or `None` when not even the columns that identify the job could
    /// be.
    pub(crate) row: Option<Box<JobRow>>,
}

/// A row decoded on its own, with any undecodable fields reported
/// separately.
pub(crate) type DecodedJob = Result<JobRow, UndecodableJob>;

/// Returns a row for a runtime path that tolerates undecodable fields, like
/// River Go's set-state and stuck-job reads. Only a row that can't be
/// identified at all is dropped, with a log.
pub(crate) fn tolerant_row(decoded: DecodedJob) -> Option<JobRow> {
    match decoded {
        Ok(row) => Some(row),
        Err(UndecodableJob { error, row: None }) => {
            error!(%error, "River job row couldn't be identified; skipping it");
            None
        }
        Err(UndecodableJob { row, .. }) => row.map(|row| *row),
    }
}

/// Collects why fields of one row couldn't be decoded.
#[derive(Default)]
pub(crate) struct FieldErrors(Vec<String>);

impl FieldErrors {
    /// Returns a decoded field, or records why it couldn't be decoded and
    /// leaves it empty.
    pub(crate) fn field<T: Default>(
        &mut self,
        column: &str,
        decoded: Result<T, impl Display>,
    ) -> T {
        decoded.unwrap_or_else(|error| {
            self.0
                .push(format!("error unmarshaling `{column}`: {error}"));
            T::default()
        })
    }

    /// Finishes decoding `row`, reporting it as undecodable if any of its
    /// fields couldn't be decoded.
    pub(crate) fn finish(self, row: JobRow) -> DecodedJob {
        if self.0.is_empty() {
            Ok(row)
        } else {
            Err(UndecodableJob {
                error: self.0.join("\n"),
                row: Some(Box::new(row)),
            })
        }
    }
}

/// Converts a persisted integer to `i16`, saturating at the type bounds.
///
/// River Go stores `attempt`, `max_attempts`, and `priority` as native
/// integers on SQLite. Values beyond `i16` are only reachable through
/// `max_attempts` in practice; saturating keeps such a job workable with
/// identical retry decisions until its 32,767th attempt.
pub(crate) fn saturating_i16(value: i64) -> i16 {
    i16::try_from(value).unwrap_or(if value < 0 { i16::MIN } else { i16::MAX })
}

/// A PostgreSQL job row. Columns the database constrains decode strictly, while
/// those that can hold values River can't represent are kept as their decode
/// results.
#[cfg(feature = "postgres")]
pub(crate) struct JobRecord {
    attempt: i16,
    attempted_at: Option<DateTime<Utc>>,
    attempted_by: Result<Option<Vec<String>>, sqlx::Error>,
    created_at: DateTime<Utc>,
    encoded_args: Json<Box<RawValue>>,
    errors: Result<Vec<Option<Json<Box<RawValue>>>>, sqlx::Error>,
    finalized_at: Option<DateTime<Utc>>,
    id: i64,
    kind: String,
    max_attempts: i16,
    metadata: Json<Value>,
    priority: i16,
    queue: String,
    scheduled_at: DateTime<Utc>,
    state: String,
    tags: Result<Vec<String>, sqlx::Error>,
    unique_key: Option<Vec<u8>>,
    pub(super) unique_skipped_as_duplicate: bool,
    unique_states: Option<String>,
}

#[cfg(feature = "postgres")]
impl<'row> FromRow<'row, PgRow> for JobRecord {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx::Error> {
        // `job_projection` fixes the first 18 columns in this order, and every
        // JobRecord query appends the insert-only duplicate flag at index 18.
        // Positional decoding avoids repeated column-name lookups on hot fetch
        // and completion paths.
        Ok(Self {
            attempt: row.try_get(1)?,
            attempted_at: row.try_get(2)?,
            attempted_by: row.try_get(3),
            created_at: row.try_get(4)?,
            encoded_args: row.try_get(5)?,
            errors: row.try_get(6),
            finalized_at: row.try_get(7)?,
            id: row.try_get(0)?,
            kind: row.try_get(8)?,
            max_attempts: row.try_get(9)?,
            metadata: row.try_get(10)?,
            priority: row.try_get(11)?,
            queue: row.try_get(12)?,
            scheduled_at: row.try_get(13)?,
            state: row.try_get(14)?,
            tags: row.try_get(15),
            unique_key: row.try_get(16)?,
            unique_skipped_as_duplicate: row.try_get(18)?,
            unique_states: row.try_get(17)?,
        })
    }
}

#[cfg(feature = "postgres")]
impl JobRecord {
    /// Decodes the row, failing if any field can't be decoded.
    pub(crate) fn into_job_row(self) -> Result<JobRow, Error> {
        let id = self.id;
        self.decode()
            .map_err(|job| Error::invalid_job(format!("job {id}: {}", job.error)))
    }

    /// Decodes the row, keeping the fields that can be decoded when others
    /// can't. River Go reads the metadata as raw JSON, but a [`JobRow`] can
    /// only represent an object.
    pub(crate) fn decode(self) -> DecodedJob {
        let state = JobState::try_from(self.state.as_str()).map_err(|error| UndecodableJob {
            error: format!("job {}: {error}", self.id),
            row: None,
        })?;
        let mut errors = FieldErrors::default();
        let attempted_by = errors.field("attempted_by", self.attempted_by);
        let attempt_errors = errors.field(
            "errors",
            self.errors
                .map_err(|error| error.to_string())
                .and_then(|errors| {
                    errors
                        .iter()
                        .map(|error| match error {
                            Some(error) => AttemptError::from_json_lenient(error.0.get())
                                .map_err(|error| error.to_string()),
                            None => Err("unexpected SQL NULL element".to_owned()),
                        })
                        .collect()
                }),
        );
        let metadata = errors.field(
            "metadata",
            match self.metadata.0 {
                Value::Object(metadata) => Ok(metadata),
                _ => Err("not a JSON object"),
            },
        );
        let tags = errors.field("tags", self.tags);
        let unique_states = errors.field(
            "unique_states",
            self.unique_states
                .map(|bits| {
                    u8::from_str_radix(&bits, 2).map(|bitmask| {
                        JobState::ALL
                            .into_iter()
                            .filter(|state| bitmask & state.unique_bit() != 0)
                            .collect()
                    })
                })
                .transpose(),
        );
        errors.finish(JobRow {
            attempt: self.attempt,
            attempted_at: self.attempted_at,
            attempted_by: attempted_by.unwrap_or_default(),
            created_at: self.created_at,
            encoded_args: self.encoded_args.0,
            errors: attempt_errors,
            finalized_at: self.finalized_at,
            id: self.id,
            kind: self.kind,
            max_attempts: self.max_attempts,
            metadata,
            priority: self.priority,
            queue: self.queue,
            scheduled_at: self.scheduled_at,
            state,
            tags,
            unique_key: self.unique_key,
            unique_states,
        })
    }
}

/// Decodes a row selected with [`job_projection`] on its own.
#[cfg(feature = "postgres")]
pub(crate) fn decode_job_row(row: &PgRow) -> DecodedJob {
    JobRecord::from_row(row)
        .map_err(|error| UndecodableJob {
            error: error.to_string(),
            row: None,
        })?
        .decode()
}

#[cfg(feature = "postgres")]
pub(crate) fn job_projection(alias: &str) -> String {
    format!(
        "{alias}.id, {alias}.attempt, {alias}.attempted_at, {alias}.attempted_by, \
         {alias}.created_at, {alias}.args AS encoded_args, \
         coalesce({alias}.errors, '{{}}'::jsonb[]) AS errors, \
         {alias}.finalized_at, {alias}.kind, {alias}.max_attempts, {alias}.metadata, \
         {alias}.priority, {alias}.queue, {alias}.scheduled_at, {alias}.state::text AS state, \
         {alias}.tags::text[] AS tags, {alias}.unique_key, {alias}.unique_states::text AS unique_states"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_saturate_to_i16() {
        assert_eq!(saturating_i16(25), 25);
        assert_eq!(saturating_i16(40_000), i16::MAX);
        assert_eq!(saturating_i16(-40_000), i16::MIN);
    }
}
