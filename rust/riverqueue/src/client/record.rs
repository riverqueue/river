//! Lenient decoding of persisted job rows.
//!
//! River Go decodes rows with `encoding/json` and native integers, so a row
//! written by Go (or edited by an operator) can contain values that a strict
//! Rust decoder rejects: attempt errors with missing or extra fields, or
//! SQLite integers outside `i16`. Runtime paths decode each row on its own so
//! one such row cannot fail a whole claimed or completed batch.

#[allow(clippy::wildcard_imports)]
use super::*;

/// A row that River could not decode into a [`JobRow`].
///
/// The runtime keeps enough lenient fields to record a failed attempt for a
/// claimed row, the way River Go's executor records an argument decode error.
#[derive(Debug)]
pub(crate) struct UndecodableJob {
    pub(crate) attempt: i64,
    pub(crate) error: String,
    pub(crate) error_count: usize,
    pub(crate) id: Option<i64>,
    pub(crate) max_attempts: i64,
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

/// Decodes one persisted attempt error the way Go's `json.Unmarshal` does:
/// missing, `null`, and unknown fields are accepted, while a non-object or a
/// field of the wrong JSON type is an error.
pub(crate) fn decode_attempt_error(value: &Value) -> Result<AttemptError, String> {
    let Value::Object(fields) = value else {
        return Err(format!("attempt error is not a JSON object: {value}"));
    };
    let field = |key| fields.get(key).filter(|value| !value.is_null());
    let at = match field("at") {
        None => go_zero_time(),
        Some(Value::String(at)) => DateTime::parse_from_rfc3339(at)
            .map_err(|error| format!("attempt error time {at:?} is invalid: {error}"))?
            .with_timezone(&Utc),
        Some(at) => return Err(format!("attempt error time is not a string: {at}")),
    };
    let attempt = match field("attempt") {
        None => 0,
        Some(attempt) => attempt
            .as_i64()
            .map(saturating_i16)
            .ok_or_else(|| format!("attempt error attempt is not an integer: {attempt}"))?,
    };
    let text = |key| match field(key) {
        None => Ok(String::new()),
        Some(Value::String(text)) => Ok(text.clone()),
        Some(text) => Err(format!("attempt error {key} is not a string: {text}")),
    };
    Ok(AttemptError {
        at,
        attempt,
        error: text("error")?,
        trace: text("trace")?,
    })
}

/// Go's zero `time.Time`, which `encoding/json` leaves in a missing field.
fn go_zero_time() -> DateTime<Utc> {
    DateTime::<Utc>::from_naive_utc_and_offset(
        chrono::NaiveDate::from_ymd_opt(1, 1, 1)
            .and_then(|date| date.and_hms_opt(0, 0, 0))
            .unwrap_or_default(),
        Utc,
    )
}

#[cfg(feature = "postgres")]
pub(crate) struct JobRecord {
    attempt: i16,
    attempted_at: Option<DateTime<Utc>>,
    attempted_by: Option<Vec<String>>,
    created_at: DateTime<Utc>,
    encoded_args: Json<Box<RawValue>>,
    errors: Vec<Json<Value>>,
    finalized_at: Option<DateTime<Utc>>,
    id: i64,
    kind: String,
    max_attempts: i16,
    metadata: Json<Value>,
    priority: i16,
    queue: String,
    scheduled_at: DateTime<Utc>,
    state: String,
    tags: Vec<String>,
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
            attempted_by: row.try_get(3)?,
            created_at: row.try_get(4)?,
            encoded_args: row.try_get(5)?,
            errors: row.try_get(6)?,
            finalized_at: row.try_get(7)?,
            id: row.try_get(0)?,
            kind: row.try_get(8)?,
            max_attempts: row.try_get(9)?,
            metadata: row.try_get(10)?,
            priority: row.try_get(11)?,
            queue: row.try_get(12)?,
            scheduled_at: row.try_get(13)?,
            state: row.try_get(14)?,
            tags: row.try_get(15)?,
            unique_key: row.try_get(16)?,
            unique_skipped_as_duplicate: row.try_get(18)?,
            unique_states: row.try_get(17)?,
        })
    }
}

#[cfg(feature = "postgres")]
impl JobRecord {
    pub(crate) fn into_job_row(self) -> Result<JobRow, Error> {
        let Value::Object(metadata) = self.metadata.0 else {
            return Err(Error::invalid_job(format!(
                "job {} metadata is not an object",
                self.id
            )));
        };
        let unique_states = self
            .unique_states
            .map(|bits| {
                let bitmask = u8::from_str_radix(&bits, 2).map_err(|error| {
                    Error::invalid_job(format!(
                        "job {} has invalid unique states {bits:?}: {error}",
                        self.id
                    ))
                })?;
                Ok::<_, Error>(
                    JobState::ALL
                        .into_iter()
                        .filter(|state| bitmask & state.unique_bit() != 0)
                        .collect(),
                )
            })
            .transpose()?;
        Ok(JobRow {
            attempt: self.attempt,
            attempted_at: self.attempted_at,
            attempted_by: self.attempted_by.unwrap_or_default(),
            created_at: self.created_at,
            encoded_args: self.encoded_args.0,
            errors: self
                .errors
                .iter()
                .map(|error| decode_attempt_error(&error.0))
                .collect::<Result<_, _>>()
                .map_err(|error| Error::invalid_job(format!("job {}: {error}", self.id)))?,
            finalized_at: self.finalized_at,
            id: self.id,
            kind: self.kind,
            max_attempts: self.max_attempts,
            metadata,
            priority: self.priority,
            queue: self.queue,
            scheduled_at: self.scheduled_at,
            state: JobState::try_from(self.state.as_str())
                .map_err(|error| Error::invalid_job(error.to_string()))?,
            tags: self.tags,
            unique_key: self.unique_key,
            unique_states,
        })
    }
}

/// Decodes a row selected with [`job_projection`] on its own, keeping enough
/// of an undecodable row to report it.
#[cfg(feature = "postgres")]
pub(crate) fn decode_job_row(row: &PgRow) -> Result<JobRow, UndecodableJob> {
    let decoded = JobRecord::from_row(row)
        .map_err(Error::from)
        .and_then(JobRecord::into_job_row);
    decoded.map_err(|error| UndecodableJob {
        attempt: row.try_get::<i16, _>(1).map_or(0, i64::from),
        error: error.to_string(),
        error_count: row
            .try_get::<Vec<Json<Value>>, _>(6)
            .map_or(0, |errors| errors.len()),
        id: row.try_get(0).ok(),
        max_attempts: row.try_get::<i16, _>(9).map_or(0, i64::from),
    })
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
    use serde_json::json;

    use super::*;

    #[test]
    fn attempt_errors_decode_like_go_json() {
        let full = decode_attempt_error(&json!({
            "at": "2026-01-02T03:04:05.123456789Z",
            "attempt": 3,
            "error": "boom",
            "extra": {"ignored": true},
            "trace": "trace",
        }))
        .unwrap();
        assert_eq!(full.attempt, 3);
        assert_eq!(full.at.to_rfc3339(), "2026-01-02T03:04:05.123456789+00:00");
        assert_eq!(full.error, "boom");
        assert_eq!(full.trace, "trace");

        let sparse = decode_attempt_error(&json!({"attempt": 40_000, "trace": null})).unwrap();
        assert_eq!(sparse.at, go_zero_time());
        assert_eq!(sparse.attempt, i16::MAX);
        assert_eq!(sparse.error, "");
        assert_eq!(sparse.trace, "");

        for invalid in [
            json!([]),
            json!({"attempt": "1"}),
            json!({"attempt": 1.5}),
            json!({"at": "yesterday"}),
            json!({"error": 7}),
        ] {
            assert!(decode_attempt_error(&invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn integers_saturate_to_i16() {
        assert_eq!(saturating_i16(25), 25);
        assert_eq!(saturating_i16(40_000), i16::MAX);
        assert_eq!(saturating_i16(-40_000), i16::MIN);
    }
}
