//! MySQL operation primitives.
//!
//! MySQL deliberately stays behind River's sealed database boundary. These
//! operations mirror River's storage semantics without exposing SQLx executor
//! types through the public API. Multi-step operations are expressed as small
//! primitives so their caller can compose them in one MySQL transaction.

#![allow(
    clippy::needless_raw_string_hashes,
    reason = "consistent SQL delimiters make large dialect-specific statements easier to audit"
)]
#![allow(
    clippy::struct_field_names,
    reason = "leader_id is the cross-language River protocol field name"
)]

use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde_json::{Map, Value, value::RawValue};
use sqlx::{AssertSqlSafe, Connection, FromRow, MySql, MySqlConnection, QueryBuilder, Row};

use sqlx::mysql::MySqlRow;

use crate::{
    AttemptError, JobMetadata, JobRow, JobState, METADATA_KEY_UNIQUE_NONCE, Queue,
    client::{DecodedJob, FieldErrors, UndecodableJob, go_time_json, tolerant_row},
    query::{JobListKeyset, JobListSqlPart},
};

/// MySQL JSON values are projected as bytes so decoding preserves large integers.
pub(crate) const JOB_COLUMNS: &str = r#"
    id, attempt, attempted_at, CAST(attempted_by AS BINARY) AS attempted_by,
    created_at, CAST(args AS BINARY) AS encoded_args, CAST(errors AS BINARY) AS errors,
    finalized_at, kind, max_attempts, CAST(metadata AS BINARY) AS metadata,
    priority, queue, scheduled_at, state, CAST(COALESCE(tags, 'null') AS BINARY) AS tags,
    unique_key, unique_states
"#;
const QUEUE_COLUMNS: &str =
    "created_at, CAST(metadata AS CHAR) AS metadata, name, paused_at, updated_at";
const ERRORS_APPENDED: &str = "JSON_ARRAY_APPEND(CASE WHEN errors IS NULL THEN JSON_ARRAY() WHEN JSON_TYPE(errors) = 'ARRAY' THEN errors ELSE JSON_ARRAY(errors) END, '$', CAST(? AS JSON))";

/// A short poll interval keeps local wakeups responsive while queue fetch
/// polling remains the durable recovery path.
pub(crate) const DEFAULT_NOTIFICATION_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Maximum number of outbox rows a notification listener reads at once,
/// matching River Go's MySQL listener.
pub(crate) const NOTIFICATION_BATCH_SIZE: i64 = 256;

#[derive(Debug, thiserror::Error)]
pub(crate) enum BackendError {
    #[error("invalid MySQL River row: {0}")]
    InvalidRow(String),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error("invalid MySQL River JSON")]
    Json(#[from] serde_json::Error),
}

/// Reports MySQL failures the way Postgres's reach callers: as SQLx's own
/// error, with stored values River can't decode as [`sqlx::Error::Decode`].
impl From<BackendError> for sqlx::Error {
    fn from(error: BackendError) -> Self {
        match error {
            BackendError::Sqlx(error) => error,
            BackendError::InvalidRow(message) => {
                Self::Decode(format!("invalid MySQL River row: {message}").into())
            }
            BackendError::Json(error) => Self::Decode(Box::new(error)),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct InsertJob<'a> {
    pub attempted_at: Option<DateTime<Utc>>,
    pub attempted_by: &'a [String],
    pub attempt: i32,
    /// When unset, MySQL's `UTC_TIMESTAMP(6)`, like Go's driver.
    pub created_at: Option<DateTime<Utc>>,
    pub encoded_args: &'a serde_json::value::RawValue,
    pub errors: &'a [AttemptError],
    pub finalized_at: Option<DateTime<Utc>>,
    pub id: Option<i64>,
    pub kind: &'a str,
    pub max_attempts: i32,
    pub metadata: &'a JobMetadata,
    pub priority: i16,
    pub queue: &'a str,
    /// When unset, MySQL's `UTC_TIMESTAMP(6)`, like Go's driver.
    pub scheduled_at: Option<DateTime<Utc>>,
    pub state: JobState,
    pub tags: &'a [String],
    pub unique_key: Option<&'a [u8]>,
    pub unique_nonce: Option<&'a str>,
    pub unique_states: Option<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct InsertedJob {
    pub job: JobRow,
    pub unique_skipped_as_duplicate: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ClaimJobs<'a> {
    pub client_id: &'a str,
    /// Kinds to claim, or `None` for every kind. Ignored when claiming
    /// selected IDs.
    pub kinds: Option<&'a [String]>,
    pub limit: i32,
    pub max_attempted_by: i32,
    pub now: DateTime<Utc>,
    pub queue: &'a str,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ListJobs<'a> {
    /// Excludes running jobs before applying the limit, as bulk deletion does.
    pub exclude_running: bool,
    pub ids: &'a [i64],
    pub keyset: JobListKeyset,
    pub kinds: &'a [&'a str],
    pub limit: i32,
    pub metadata: Option<&'a Map<String, Value>>,
    pub priorities: &'a [i16],
    pub queues: &'a [&'a str],
    pub states: &'a [JobState],
    pub tags_all: &'a [&'a str],
    pub tags_any: &'a [&'a str],
}

#[derive(Clone, Debug)]
pub(crate) struct CompleteJob<'a> {
    pub attempt: Option<i32>,
    pub error: Option<&'a AttemptError>,
    pub finalized_at: Option<DateTime<Utc>>,
    pub id: i64,
    pub metadata_updates: Option<&'a Map<String, Value>>,
    pub now: DateTime<Utc>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub state: JobState,
}

#[derive(Clone, Debug)]
pub(crate) struct RescueJob<'a> {
    pub error: &'a AttemptError,
    pub finalized_at: Option<DateTime<Utc>>,
    pub id: i64,
    pub scheduled_at: DateTime<Utc>,
    pub state: JobState,
    /// The rescue applies only to a job still running from before this time.
    pub stuck_horizon: DateTime<Utc>,
}

/// Job cleaner deletion horizons; `None` keeps that state indefinitely.
#[derive(Clone, Debug)]
pub(crate) struct CleanupJobs<'a> {
    pub cancelled_before: Option<DateTime<Utc>>,
    pub completed_before: Option<DateTime<Utc>>,
    pub discarded_before: Option<DateTime<Utc>>,
    pub limit: i32,
    pub queues_excluded: &'a [&'a str],
    pub queues_included: Option<&'a [&'a str]>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Leader {
    pub elected_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub leader_id: String,
}

#[derive(Clone, Debug, FromRow, PartialEq)]
pub(crate) struct Notification {
    pub id: i64,
    pub payload: String,
    pub topic: String,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NotificationInput<'a> {
    pub payload: &'a str,
    pub topic: &'a str,
}

// River Go stores `attempt`, `max_attempts`, and `priority` as native
// integers, so they decode as `i64`. A value `JobRow`'s field can't hold
// makes the row undecodable, like other columns River can't represent.
// JSON columns are read as bytes, since a value changed out of band might not
// even be UTF-8.
#[derive(Clone, Debug, FromRow)]
struct JobRecord {
    attempt: i64,
    attempted_at: Option<DateTime<Utc>>,
    attempted_by: Option<Vec<u8>>,
    created_at: DateTime<Utc>,
    encoded_args: Vec<u8>,
    errors: Option<Vec<u8>>,
    finalized_at: Option<DateTime<Utc>>,
    id: i64,
    kind: String,
    max_attempts: i64,
    metadata: Vec<u8>,
    priority: i64,
    queue: String,
    scheduled_at: DateTime<Utc>,
    state: String,
    tags: Vec<u8>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<i64>,
}

impl JobRecord {
    /// Decodes the row, failing if any field can't be decoded.
    fn into_job(self) -> Result<JobRow, BackendError> {
        let id = self.id;
        self.decode()
            .map_err(|job| BackendError::InvalidRow(format!("job {id}: {}", job.error)))
    }

    /// Decodes the row, keeping the fields that can be decoded when others
    /// can't, like River Go. JSON columns can be changed to any shape, so
    /// each decodes on its own. River Go reads the metadata as raw JSON, but a
    /// [`JobRow`] can only represent an object.
    fn decode(self) -> DecodedJob {
        let unidentifiable = |error: String| UndecodableJob {
            columns: Vec::new(),
            error: format!("job {}: {error}", self.id),
            row: None,
        };
        let state = JobState::try_from(self.state.as_str())
            .map_err(|error| unidentifiable(error.to_string()))?;

        let mut errors = FieldErrors::default();
        // Like River Go, args that aren't valid JSON are left empty, as
        // `null`, so the job isn't worked with them.
        let encoded_args = errors.field_or_else(
            "args",
            json_text(self.encoded_args)
                .and_then(|text| RawValue::from_string(text).map_err(|error| error.to_string())),
            || RawValue::from_string("null".to_owned()).expect("null is valid JSON"),
        );
        let attempted_by = errors.field(
            "attempted_by",
            self.attempted_by
                .map(json_text)
                .transpose()
                .and_then(|text| {
                    decode_json_strings(text.as_deref()).map_err(|error| error.to_string())
                }),
        );
        let attempt_errors = errors.field(
            "errors",
            self.errors.map(json_text).transpose().and_then(|text| {
                text.as_deref()
                    .map_or_else(|| Ok(Vec::new()), AttemptError::from_json_array_lenient)
                    .map_err(|error| error.to_string())
            }),
        );
        let metadata = errors.field(
            "metadata",
            json_text(self.metadata).and_then(|text| {
                text.parse::<JobMetadata>()
                    .map_err(|error| error.to_string())
            }),
        );
        let tags = errors.field(
            "tags",
            json_text(self.tags).and_then(|text| {
                decode_json_strings(Some(&text)).map_err(|error| error.to_string())
            }),
        );
        let unique_states = errors.field(
            "unique_states",
            self.unique_states.map(decode_unique_states).transpose(),
        );
        let attempt = errors.field("attempt", i32::try_from(self.attempt));
        let max_attempts = errors.field("max_attempts", i32::try_from(self.max_attempts));
        let priority = errors.field("priority", i16::try_from(self.priority));
        errors.finish(JobRow {
            id: self.id,
            attempt,
            attempted_at: self.attempted_at,
            attempted_by,
            created_at: self.created_at,
            encoded_args,
            errors: attempt_errors,
            finalized_at: self.finalized_at,
            kind: self.kind,
            max_attempts,
            metadata,
            priority,
            queue: self.queue,
            scheduled_at: self.scheduled_at,
            state,
            tags,
            unique_key: self.unique_key,
            unique_states,
        })
    }
}

/// Decodes a row selected with [`JOB_COLUMNS`] on its own.
pub(crate) fn decode_job_row(row: &MySqlRow) -> DecodedJob {
    JobRecord::from_row(row)
        .map_err(|error| UndecodableJob {
            columns: Vec::new(),
            error: error.to_string(),
            row: None,
        })?
        .decode()
}

#[derive(Clone, Debug, FromRow)]
struct QueueRecord {
    created_at: DateTime<Utc>,
    metadata: String,
    name: String,
    paused_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
}

impl QueueRecord {
    fn into_queue(self) -> Result<Queue, BackendError> {
        let metadata: Value = serde_json::from_str(&self.metadata)?;
        let metadata = metadata.as_object().cloned().ok_or_else(|| {
            BackendError::InvalidRow(format!("queue {:?} metadata is not an object", self.name))
        })?;
        Ok(Queue {
            created_at: self.created_at,
            metadata,
            metadata_text: self.metadata,
            name: self.name,
            paused_at: self.paused_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(Clone, Debug, FromRow)]
struct LeaderRecord {
    elected_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    leader_id: String,
}

impl From<LeaderRecord> for Leader {
    fn from(record: LeaderRecord) -> Self {
        Self {
            elected_at: record.elected_at,
            expires_at: record.expires_at,
            leader_id: record.leader_id,
        }
    }
}

/// Converts a JSON column read as bytes to text.
fn json_text(bytes: Vec<u8>) -> Result<String, String> {
    String::from_utf8(bytes).map_err(|error| error.to_string())
}

fn decode_json_or_default<T>(encoded: Option<&str>) -> Result<T, serde_json::Error>
where
    T: serde::de::DeserializeOwned + Default,
{
    encoded.map_or_else(|| Ok(T::default()), serde_json::from_str)
}

/// Decodes a JSON array of strings like Go's `encoding/json` decodes a
/// `[]string`: `null` is empty, as is a `null` element.
fn decode_json_strings(encoded: Option<&str>) -> Result<Vec<String>, serde_json::Error> {
    Ok(
        decode_json_or_default::<Option<Vec<Option<String>>>>(encoded)?
            .unwrap_or_default()
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect(),
    )
}

fn decode_unique_states(bits: i64) -> Result<Vec<JobState>, String> {
    let bits = u8::try_from(bits).map_err(|_| format!("value out of range for byte: {bits}"))?;
    Ok(JobState::ALL
        .iter()
        .copied()
        .filter(|state| bits & state.unique_bit() != 0)
        .collect())
}

pub(crate) fn mysql_time(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

fn mysql_time_optional(time: Option<DateTime<Utc>>) -> Option<String> {
    time.map(mysql_time)
}

pub(crate) async fn insert(
    connection: &mut MySqlConnection,
    params: &InsertJob<'_>,
) -> Result<InsertedJob, BackendError> {
    let mut metadata = params.metadata.clone();
    if let Some(nonce) = params.unique_nonce {
        metadata.insert(METADATA_KEY_UNIQUE_NONCE, Value::String(nonce.to_owned()))?;
    }
    let result = sqlx::query(
        "INSERT INTO river_job (id, args, attempt, attempted_at, attempted_by, created_at, errors, \
         finalized_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states) \
         VALUES (?, CAST(? AS JSON), ?, ?, CAST(? AS JSON), COALESCE(?, UTC_TIMESTAMP(6)), CAST(? AS JSON), \
         ?, ?, ?, CAST(? AS JSON), ?, ?, COALESCE(?, UTC_TIMESTAMP(6)), ?, CAST(? AS JSON), ?, ?) \
         ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)",
    )
    .bind(params.id).bind(params.encoded_args.get()).bind(params.attempt)
    .bind(mysql_time_optional(params.attempted_at)).bind(serde_json::to_string(params.attempted_by)?)
    .bind(mysql_time_optional(params.created_at)).bind(serde_json::to_string(params.errors)?)
    .bind(mysql_time_optional(params.finalized_at)).bind(params.kind).bind(params.max_attempts)
    .bind(serde_json::to_string(&metadata)?).bind(params.priority).bind(params.queue)
    .bind(mysql_time_optional(params.scheduled_at)).bind(params.state.as_str())
    .bind(serde_json::to_string(params.tags)?).bind(params.unique_key).bind(params.unique_states.map(i16::from))
    .execute(&mut *connection).await?;
    let id = i64::try_from(result.last_insert_id())
        .map_err(|error| BackendError::InvalidRow(error.to_string()))?;
    let job = get_locked(connection, id)
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;
    let unique_skipped_as_duplicate = params.unique_nonce.is_some_and(|nonce| {
        job.metadata
            .get::<String>(METADATA_KEY_UNIQUE_NONCE)
            .ok()
            .flatten()
            .as_deref()
            != Some(nonce)
    });
    if unique_skipped_as_duplicate
        && (params.unique_key.is_none()
            || params.unique_key != job.unique_key.as_deref()
            || params
                .unique_states
                .is_none_or(|bits| bits & params.state.unique_bit() == 0)
            || !job
                .unique_states
                .as_ref()
                .is_some_and(|states| states.contains(&job.state)))
    {
        return Err(BackendError::InvalidRow(
            "insert conflicts with a constraint other than River job uniqueness".into(),
        ));
    }
    Ok(InsertedJob {
        job,
        unique_skipped_as_duplicate,
    })
}

pub(crate) async fn claim(
    connection: &mut MySqlConnection,
    params: &ClaimJobs<'_>,
) -> Result<Vec<DecodedJob>, BackendError> {
    if params.limit <= 0 {
        return Ok(Vec::new());
    }
    let mut query = QueryBuilder::<MySql>::new("SELECT id FROM river_job WHERE queue = ");
    query.push_bind(params.queue).push(" AND CAST(queue AS BINARY) = ").push_bind(params.queue)
        .push(" AND state = 'available' AND scheduled_at <= ").push_bind(mysql_time(params.now))
        .push(" AND NOT EXISTS (SELECT 1 FROM river_queue WHERE name = river_job.queue AND CAST(name AS BINARY) = CAST(river_job.queue AS BINARY) AND paused_at IS NOT NULL)");
    if let Some(kinds) = params.kinds {
        if kinds.is_empty() {
            return Ok(Vec::new());
        }
        query.push(" AND CAST(kind AS BINARY) IN (");
        let mut separated = query.separated(", ");
        for kind in kinds {
            separated.push_bind(kind);
        }
        separated.push_unseparated(")");
    }
    query
        .push(" ORDER BY priority, scheduled_at, id LIMIT ")
        .push_bind(params.limit)
        .push(" FOR UPDATE SKIP LOCKED");
    let ids = query
        .build_query_scalar::<i64>()
        .fetch_all(&mut *connection)
        .await?;
    claim_selected(connection, params, &ids).await
}

/// Updates the selected rows while the caller holds their transaction locks.
pub(crate) async fn claim_selected(
    connection: &mut MySqlConnection,
    params: &ClaimJobs<'_>,
    ids: &[i64],
) -> Result<Vec<DecodedJob>, BackendError> {
    if params.limit <= 0 {
        return Ok(Vec::new());
    }
    let mut rows = Vec::with_capacity(ids.len());
    for id in ids {
        let sql = format!(
            "SELECT {JOB_COLUMNS} FROM river_job WHERE id = ? AND state = 'available' FOR UPDATE"
        );
        let Some(row) = sqlx::query(AssertSqlSafe(sql))
            .bind(id)
            .fetch_optional(&mut *connection)
            .await?
        else {
            continue;
        };
        // A malformed attempted_by is retained so the worker fails that row alone.
        let attempted_by = row
            .try_get::<Option<Vec<u8>>, _>("attempted_by")?
            .as_deref()
            .map(serde_json::from_slice::<Option<Vec<Option<String>>>>)
            .transpose();
        let attempted_by = attempted_by
            .ok()
            .map(|values| {
                let mut values = values.flatten().unwrap_or_default();
                let keep =
                    usize::try_from(params.max_attempted_by.saturating_sub(1)).unwrap_or_default();
                values.drain(..values.len().saturating_sub(keep));
                values.push(Some(params.client_id.to_owned()));
                serde_json::to_string(&values)
            })
            .transpose()?;
        sqlx::query("UPDATE river_job SET attempt = attempt + 1, attempted_at = ?, attempted_by = COALESCE(CAST(? AS JSON), attempted_by), state = 'running' WHERE id = ?")
            .bind(mysql_time(params.now)).bind(attempted_by).bind(id).execute(&mut *connection).await?;
        if let Some(row) = get_raw(connection, *id, false).await? {
            rows.push(decode_job_row(&row));
        }
    }
    Ok(rows)
}

async fn get_raw(
    connection: &mut MySqlConnection,
    id: i64,
    lock: bool,
) -> Result<Option<MySqlRow>, BackendError> {
    let sql = format!(
        "SELECT {JOB_COLUMNS} FROM river_job WHERE id = ?{}",
        if lock { " FOR UPDATE" } else { "" }
    );
    Ok(sqlx::query(AssertSqlSafe(sql))
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?)
}

pub(crate) async fn get(
    connection: &mut MySqlConnection,
    id: i64,
) -> Result<Option<JobRow>, BackendError> {
    get_raw(connection, id, false)
        .await?
        .as_ref()
        .map(|row| JobRecord::from_row(row)?.into_job())
        .transpose()
}

pub(crate) async fn get_locked(
    connection: &mut MySqlConnection,
    id: i64,
) -> Result<Option<JobRow>, BackendError> {
    get_raw(connection, id, true)
        .await?
        .as_ref()
        .map(|row| JobRecord::from_row(row)?.into_job())
        .transpose()
}

/// Merge objects shallowly, retaining JSON null values as Postgres does.
fn push_metadata(
    query: &mut QueryBuilder<MySql>,
    updates: &Map<String, Value>,
) -> Result<(), BackendError> {
    if updates.is_empty() {
        query.push("metadata");
        return Ok(());
    }
    query.push("JSON_SET(metadata");
    for (key, value) in updates {
        query
            .push(", ")
            .push_bind(format!("$.{}", serde_json::to_string(key)?))
            .push(", CAST(")
            .push_bind(serde_json::to_string(value)?)
            .push(" AS JSON)");
    }
    query.push(")");
    Ok(())
}

pub(crate) async fn cancel(
    connection: &mut MySqlConnection,
    id: i64,
    now: DateTime<Utc>,
) -> Result<Option<JobRow>, BackendError> {
    let Some(row) = get_locked(connection, id).await? else {
        return Ok(None);
    };
    if row.finalized_at.is_some()
        || matches!(
            row.state,
            JobState::Cancelled | JobState::Completed | JobState::Discarded
        )
    {
        return Ok(None);
    }
    sqlx::query("UPDATE river_job SET finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE ? END, metadata = JSON_SET(metadata, '$.cancel_attempted_at', ?), state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END WHERE id = ?")
        .bind(mysql_time(now)).bind(go_time_json(now)).bind(id).execute(&mut *connection).await?;
    get(connection, id).await
}

pub(crate) async fn delete(
    connection: &mut MySqlConnection,
    id: i64,
) -> Result<Option<JobRow>, BackendError> {
    let Some(row) = get_locked(connection, id).await? else {
        return Ok(None);
    };
    if row.state == JobState::Running {
        return Ok(None);
    }
    sqlx::query("DELETE FROM river_job WHERE id = ?")
        .bind(id)
        .execute(connection)
        .await?;
    Ok(Some(row))
}

pub(crate) async fn retry(
    connection: &mut MySqlConnection,
    id: i64,
    now: DateTime<Utc>,
) -> Result<Option<JobRow>, BackendError> {
    let Some(row) = get_locked(connection, id).await? else {
        return Ok(None);
    };
    if row.state == JobState::Running
        || (row.state == JobState::Available && row.scheduled_at <= now)
    {
        return Ok(None);
    }
    sqlx::query("UPDATE river_job SET state = 'available', finalized_at = NULL, scheduled_at = ?, max_attempts = CASE WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END WHERE id = ?")
        .bind(mysql_time(now)).bind(id).execute(&mut *connection).await?;
    get(connection, id).await
}

pub(crate) async fn complete(
    connection: &mut MySqlConnection,
    params: &CompleteJob<'_>,
) -> Result<Option<JobRow>, BackendError> {
    complete_decoded(connection, params)
        .await?
        .map(|row| row.map_err(|job| BackendError::InvalidRow(job.error)))
        .transpose()
}

pub(crate) async fn complete_decoded(
    connection: &mut MySqlConnection,
    params: &CompleteJob<'_>,
) -> Result<Option<DecodedJob>, BackendError> {
    let Some(row) = get_raw(connection, params.id, true).await? else {
        return Ok(None);
    };
    if row.try_get::<String, _>("state")? != "running" {
        return Ok(None);
    }
    let metadata: Value = serde_json::from_slice(&row.try_get::<Vec<u8>, _>("metadata")?)?;
    let cancel = matches!(
        params.state,
        JobState::Available | JobState::Retryable | JobState::Scheduled
    ) && metadata
        .as_object()
        .is_some_and(|metadata| metadata.contains_key("cancel_attempted_at"));
    let mut query = QueryBuilder::<MySql>::new("UPDATE river_job SET state = ");
    query.push_bind(if cancel {
        "cancelled"
    } else {
        params.state.as_str()
    });
    if cancel {
        query
            .push(", finalized_at = ")
            .push_bind(mysql_time(params.now));
    } else {
        if let Some(attempt) = params.attempt {
            query.push(", attempt = ").push_bind(attempt);
        }
        if let Some(time) = params.finalized_at {
            query.push(", finalized_at = ").push_bind(mysql_time(time));
        }
        if let Some(time) = params.scheduled_at {
            query.push(", scheduled_at = ").push_bind(mysql_time(time));
        }
    }
    if let Some(error) = params.error {
        // This expression has one placeholder; bind through the query builder.
        let (before, after) = ERRORS_APPENDED
            .split_once('?')
            .expect("one error parameter");
        query
            .push(", errors = ")
            .push(before)
            .push_bind(serde_json::to_string(error)?)
            .push(after);
    }
    if let Some(metadata) = params.metadata_updates {
        query.push(", metadata = ");
        push_metadata(&mut query, metadata)?;
    }
    query.push(" WHERE id = ").push_bind(params.id);
    query.build().execute(&mut *connection).await?;
    Ok(get_raw(connection, params.id, false)
        .await?
        .as_ref()
        .map(decode_job_row))
}

pub(crate) async fn merge_metadata_if_not_running(
    connection: &mut MySqlConnection,
    id: i64,
    updates: &Map<String, Value>,
) -> Result<Option<DecodedJob>, BackendError> {
    let Some(row) = get_raw(connection, id, true).await? else {
        return Ok(None);
    };
    if row.try_get::<String, _>("state")? == "running" {
        return Ok(None);
    }
    let mut query = QueryBuilder::<MySql>::new("UPDATE river_job SET metadata = ");
    push_metadata(&mut query, updates)?;
    query.push(" WHERE id = ").push_bind(id);
    query.build().execute(&mut *connection).await?;
    Ok(get_raw(connection, id, false)
        .await?
        .as_ref()
        .map(decode_job_row))
}

pub(crate) async fn update(
    connection: &mut MySqlConnection,
    id: i64,
    updates: &Map<String, Value>,
) -> Result<Option<JobRow>, BackendError> {
    if get_raw(connection, id, true).await?.is_none() {
        return Ok(None);
    }
    let mut query = QueryBuilder::<MySql>::new("UPDATE river_job SET metadata = ");
    push_metadata(&mut query, updates)?;
    query.push(" WHERE id = ").push_bind(id);
    query.build().execute(&mut *connection).await?;
    get(connection, id).await
}

#[allow(
    clippy::too_many_lines,
    reason = "the bound-only query builder keeps all list filters and keyset ordering auditable"
)]
pub(crate) async fn list(
    connection: &mut MySqlConnection,
    params: &ListJobs<'_>,
) -> Result<Vec<JobRow>, BackendError> {
    if params.limit <= 0 {
        return Ok(Vec::new());
    }

    let mut query =
        QueryBuilder::<MySql>::new(format!("SELECT {JOB_COLUMNS} FROM river_job WHERE true"));
    if let Some(after) = params.keyset.after_sql() {
        query.push(" AND ");
        for part in after {
            match part {
                JobListSqlPart::AfterId => {
                    query.push_bind(params.keyset.after_id().expect("cursor has an ID"));
                }
                JobListSqlPart::AfterTime => {
                    query.push_bind(mysql_time(
                        params.keyset.after_time().expect("cursor has a time"),
                    ));
                }
                JobListSqlPart::Sql(sql) => {
                    query.push(sql);
                }
            }
        }
    }
    if params.exclude_running {
        query.push(" AND state != 'running'");
    }
    if !params.ids.is_empty() {
        query.push(" AND id IN (");
        let mut separated = query.separated(", ");
        for id in params.ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(")");
    }
    if !params.kinds.is_empty() {
        query.push(" AND CAST(kind AS BINARY) IN (");
        let mut separated = query.separated(", ");
        for kind in params.kinds {
            separated.push_bind(kind);
        }
        separated.push_unseparated(")");
    }
    if !params.priorities.is_empty() {
        query.push(" AND priority IN (");
        let mut separated = query.separated(", ");
        for priority in params.priorities {
            separated.push_bind(priority);
        }
        separated.push_unseparated(")");
    }
    if !params.queues.is_empty() {
        query.push(" AND CAST(queue AS BINARY) IN (");
        let mut separated = query.separated(", ");
        for queue in params.queues {
            separated.push_bind(queue);
        }
        separated.push_unseparated(")");
    }
    if !params.states.is_empty() {
        query.push(" AND state IN (");
        let mut separated = query.separated(", ");
        for state in params.states {
            separated.push_bind(state.as_str());
        }
        separated.push_unseparated(")");
    }
    if let Some(metadata) = params.metadata {
        query.push(" AND ");
        push_contains_value(
            &mut query,
            "metadata",
            &Value::Object(metadata.clone()),
            &mut 0,
        )?;
    }
    for tag in params.tags_all {
        query
            .push(" AND JSON_CONTAINS(tags, JSON_QUOTE(")
            .push_bind(tag)
            .push("))");
    }
    if !params.tags_any.is_empty() {
        query.push(" AND EXISTS (SELECT 1 FROM JSON_TABLE(tags, '$[*]' COLUMNS(value VARCHAR(255) PATH '$')) AS tag WHERE CAST(value AS BINARY) IN (");
        let mut separated = query.separated(", ");
        for tag in params.tags_any {
            separated.push_bind(tag);
        }
        separated.push_unseparated("))");
    }
    query
        .push(" ORDER BY ")
        .push(mysql_order(&params.keyset.order_sql()));
    query.push(" LIMIT ").push_bind(params.limit);

    let records = query
        .build_query_as::<JobRecord>()
        .fetch_all(&mut *connection)
        .await?;
    records.into_iter().map(JobRecord::into_job).collect()
}

fn mysql_order(order: &str) -> String {
    let field = order.split_whitespace().next().expect("order has a field");
    if order.contains("NULLS LAST") {
        format!("{field} IS NULL ASC, {}", order.replace(" NULLS LAST", ""))
    } else if order.contains("NULLS FIRST") {
        format!(
            "{field} IS NULL DESC, {}",
            order.replace(" NULLS FIRST", "")
        )
    } else {
        order.to_owned()
    }
}

// JSON_CONTAINS flattens nested arrays; Postgres containment preserves each
// level. JSON_TABLE traverses each level explicitly. Scalar subqueries avoid
// MySQL 8 semijoin rewrites losing nested correlations.
fn push_contains_value(
    query: &mut QueryBuilder<MySql>,
    source: &str,
    fragment: &Value,
    aliases: &mut usize,
) -> Result<(), BackendError> {
    match fragment {
        Value::Object(object) => {
            query.push(format!("(JSON_TYPE({source}) = 'OBJECT'"));
            for (key, value) in object {
                let alias = format!("contains_{aliases}");
                *aliases += 1;
                query
                    .push(format!(
                        " AND (SELECT 1 FROM JSON_TABLE(JSON_EXTRACT({source}, "
                    ))
                    .push_bind(format!("$.{}", serde_json::to_string(key)?))
                    .push(format!(
                        "), '$' COLUMNS(value JSON PATH '$')) AS {alias} WHERE "
                    ));
                push_contains_value(query, &format!("{alias}.value"), value, aliases)?;
                query.push(" LIMIT 1) IS NOT NULL");
            }
            query.push(")");
        }
        Value::Array(array) => {
            query.push(format!("(JSON_TYPE({source}) = 'ARRAY'"));
            for value in array {
                let alias = format!("contains_{aliases}");
                *aliases += 1;
                query.push(format!(" AND (SELECT 1 FROM JSON_TABLE({source}, '$[*]' COLUMNS(value JSON PATH '$')) AS {alias} WHERE "));
                push_contains_value(query, &format!("{alias}.value"), value, aliases)?;
                query.push(" LIMIT 1) IS NOT NULL");
            }
            query.push(")");
        }
        Value::Null => {
            query.push(format!(
                "({source} IS NULL OR JSON_TYPE({source}) = 'NULL')"
            ));
        }
        value => {
            query.push(format!("(JSON_TYPE({source}) NOT IN ('ARRAY', 'OBJECT') AND JSON_CONTAINS({source}, CAST("))
                .push_bind(serde_json::to_string(value)?).push(" AS JSON)))");
        }
    }
    Ok(())
}

pub(crate) async fn queue_upsert(
    connection: &mut MySqlConnection,
    name: &str,
    metadata: &Map<String, Value>,
    paused_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<Queue, BackendError> {
    sqlx::query("INSERT INTO river_queue (created_at, metadata, name, paused_at, updated_at) VALUES (?, CAST(? AS JSON), ?, ?, ?) ON DUPLICATE KEY UPDATE updated_at = ?")
        .bind(mysql_time(now)).bind(serde_json::to_string(metadata)?).bind(name)
        .bind(mysql_time_optional(paused_at)).bind(mysql_time(now)).bind(mysql_time(now))
        .execute(&mut *connection).await?;
    queue_get(connection, name)
        .await?
        .ok_or_else(|| sqlx::Error::RowNotFound.into())
}

pub(crate) async fn queue_get(
    connection: &mut MySqlConnection,
    name: &str,
) -> Result<Option<Queue>, BackendError> {
    sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(format!(
        "SELECT {QUEUE_COLUMNS} FROM river_queue WHERE name = ? AND CAST(name AS BINARY) = ?"
    )))
    .bind(name)
    .bind(name)
    .fetch_optional(connection)
    .await?
    .map(QueueRecord::into_queue)
    .transpose()
}

pub(crate) async fn queue_list(
    connection: &mut MySqlConnection,
    limit: i32,
) -> Result<Vec<Queue>, BackendError> {
    if limit <= 0 {
        return Ok(Vec::new());
    }
    sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(format!(
        "SELECT {QUEUE_COLUMNS} FROM river_queue ORDER BY name LIMIT ?"
    )))
    .bind(limit)
    .fetch_all(connection)
    .await?
    .into_iter()
    .map(QueueRecord::into_queue)
    .collect()
}

pub(crate) async fn queue_pause(
    connection: &mut MySqlConnection,
    name: &str,
    now: DateTime<Utc>,
) -> Result<Vec<Queue>, BackendError> {
    queue_set_paused(connection, name, Some(now), now).await
}

pub(crate) async fn queue_resume(
    connection: &mut MySqlConnection,
    name: &str,
    now: DateTime<Utc>,
) -> Result<Vec<Queue>, BackendError> {
    queue_set_paused(connection, name, None, now).await
}

async fn queue_set_paused(
    connection: &mut MySqlConnection,
    name: &str,
    paused_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<Vec<Queue>, BackendError> {
    let names = sqlx::query_scalar::<_, String>("SELECT name FROM river_queue WHERE (? = '*' OR CAST(name AS BINARY) = ?) ORDER BY name FOR UPDATE")
        .bind(name).bind(name).fetch_all(&mut *connection).await?;
    let mut queues = Vec::with_capacity(names.len());
    for name in names {
        sqlx::query("UPDATE river_queue SET updated_at = CASE WHEN (? AND paused_at IS NULL) OR (NOT ? AND paused_at IS NOT NULL) THEN ? ELSE updated_at END, paused_at = CASE WHEN ? THEN COALESCE(paused_at, ?) ELSE NULL END WHERE name = ?")
            .bind(paused_at.is_some()).bind(paused_at.is_some()).bind(mysql_time(now))
            .bind(paused_at.is_some()).bind(mysql_time_optional(paused_at)).bind(&name).execute(&mut *connection).await?;
        if let Some(queue) = queue_get(connection, &name).await? {
            queues.push(queue);
        }
    }
    Ok(queues)
}

pub(crate) async fn queue_update(
    connection: &mut MySqlConnection,
    name: &str,
    metadata: Option<&Map<String, Value>>,
    now: DateTime<Utc>,
) -> Result<Option<Queue>, BackendError> {
    sqlx::query("UPDATE river_queue SET metadata = COALESCE(CAST(? AS JSON), metadata), updated_at = ? WHERE name = ? AND CAST(name AS BINARY) = ?")
        .bind(metadata.map(serde_json::to_string).transpose()?).bind(mysql_time(now)).bind(name).bind(name).execute(&mut *connection).await?;
    queue_get(connection, name).await
}

pub(crate) async fn queue_delete_expired(
    connection: &mut MySqlConnection,
    updated_before: DateTime<Utc>,
    limit: i32,
) -> Result<Vec<String>, BackendError> {
    if limit <= 0 {
        return Ok(Vec::new());
    }
    let names = sqlx::query_scalar::<_, String>("SELECT name FROM river_queue WHERE updated_at < ? ORDER BY name LIMIT ? FOR UPDATE SKIP LOCKED")
        .bind(mysql_time(updated_before)).bind(limit).fetch_all(&mut *connection).await?;
    for name in &names {
        sqlx::query("DELETE FROM river_queue WHERE name = ?")
            .bind(name)
            .execute(&mut *connection)
            .await?;
    }
    Ok(names)
}

pub(crate) async fn leader_elect(
    connection: &mut MySqlConnection,
    leader_id: &str,
    now: DateTime<Utc>,
    ttl: Duration,
) -> Result<Option<Leader>, BackendError> {
    let expires_at = now
        + chrono::Duration::from_std(ttl)
            .map_err(|error| BackendError::InvalidRow(error.to_string()))?;
    let result = sqlx::query("INSERT INTO river_leader (name, leader_id, elected_at, expires_at) VALUES ('default', ?, ?, ?)")
        .bind(leader_id).bind(mysql_time(now)).bind(mysql_time(expires_at)).execute(&mut *connection).await;
    match result {
        Ok(_) => leader_get(connection).await,
        Err(error)
            if error
                .as_database_error()
                .is_some_and(sqlx::error::DatabaseError::is_unique_violation) =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

async fn leader_get(connection: &mut MySqlConnection) -> Result<Option<Leader>, BackendError> {
    Ok(sqlx::query_as::<_, LeaderRecord>("SELECT elected_at, expires_at, leader_id FROM river_leader WHERE name = 'default' FOR UPDATE")
        .fetch_optional(connection).await?.map(Leader::from))
}

pub(crate) async fn leader_reelect(
    connection: &mut MySqlConnection,
    leader_id: &str,
    elected_at: DateTime<Utc>,
    now: DateTime<Utc>,
    ttl: Duration,
) -> Result<Option<Leader>, BackendError> {
    let expires_at = now
        + chrono::Duration::from_std(ttl)
            .map_err(|error| BackendError::InvalidRow(error.to_string()))?;
    let result = sqlx::query("UPDATE river_leader SET expires_at = ? WHERE name = 'default' AND CAST(leader_id AS BINARY) = ? AND elected_at = ? AND expires_at >= ?")
        .bind(mysql_time(expires_at)).bind(leader_id).bind(mysql_time(elected_at)).bind(mysql_time(now)).execute(&mut *connection).await?;
    if result.rows_affected() == 0 {
        return Ok(None);
    }
    leader_get(connection).await
}

pub(crate) async fn leader_resign(
    connection: &mut MySqlConnection,
    leader_id: &str,
    elected_at: DateTime<Utc>,
) -> Result<bool, BackendError> {
    Ok(sqlx::query("DELETE FROM river_leader WHERE name = 'default' AND CAST(leader_id AS BINARY) = ? AND elected_at = ?")
        .bind(leader_id).bind(mysql_time(elected_at)).execute(connection).await?.rows_affected() > 0)
}

pub(crate) async fn leader_delete_expired(
    connection: &mut MySqlConnection,
    now: DateTime<Utc>,
) -> Result<u64, BackendError> {
    Ok(sqlx::query("DELETE FROM river_leader WHERE expires_at < ?")
        .bind(mysql_time(now))
        .execute(connection)
        .await?
        .rows_affected())
}

pub(crate) async fn notification_insert(
    connection: &mut MySqlConnection,
    notifications: &[NotificationInput<'_>],
) -> Result<u64, BackendError> {
    if notifications.is_empty() {
        return Ok(0);
    }
    let mut query = QueryBuilder::<MySql>::new("INSERT INTO river_notification (payload, topic) ");
    query.push_values(notifications, |mut row, notification| {
        row.push_bind(notification.payload)
            .push_bind(notification.topic);
    });
    Ok(query.build().execute(connection).await?.rows_affected())
}

// Ordered locking reads wait for earlier uncommitted IDs. MAX(id), snapshot
// reads and SKIP LOCKED can advance the cursor past a later commit forever.
pub(crate) async fn notification_get_after(
    connection: &mut MySqlConnection,
    after_id: i64,
    topics: &[&str],
    limit: i64,
) -> Result<Vec<Notification>, BackendError> {
    if limit <= 0 || topics.is_empty() {
        return Ok(Vec::new());
    }
    Ok(sqlx::query_as::<_, Notification>("SELECT id, payload, topic FROM river_notification FORCE INDEX (PRIMARY) WHERE id > ? ORDER BY id LIMIT ? FOR SHARE")
        .bind(after_id).bind(limit).fetch_all(connection).await?)
}

pub(crate) async fn notification_last_id(
    connection: &mut MySqlConnection,
) -> Result<i64, BackendError> {
    // Read every ID to lock lower outstanding inserts before establishing the cursor.
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT id FROM river_notification FORCE INDEX (PRIMARY) ORDER BY id FOR UPDATE",
    )
    .fetch_all(connection)
    .await?
    .last()
    .copied()
    .unwrap_or_default())
}

#[derive(Debug, Default)]
pub(crate) struct NotificationListener {
    /// ID of the last row read, or `None` before connecting.
    last_id: Option<i64>,
    /// Rows read but not yet delivered.
    pending: VecDeque<Notification>,
    /// Subscribed topics and the ID after which each one's rows are
    /// delivered.
    topics: HashMap<String, i64>,
}

impl NotificationListener {
    /// Starts reading after the outbox's current maximum ID.
    pub(crate) async fn connect(
        &mut self,
        connection: &mut MySqlConnection,
    ) -> Result<(), BackendError> {
        let mut tx = connection.begin().await?;
        let last_id = notification_last_id(&mut tx).await?;
        tx.commit().await?;
        self.last_id = Some(last_id);
        Ok(())
    }

    /// Whether the initial outbox cursor has been established.
    pub(crate) const fn is_connected(&self) -> bool {
        self.last_id.is_some()
    }

    /// Subscribes to `topic`, delivering only rows written from now on.
    /// Subscribing to a topic that's already subscribed changes nothing.
    pub(crate) async fn listen(
        &mut self,
        connection: &mut MySqlConnection,
        topic: &str,
    ) -> Result<(), BackendError> {
        if self.topics.contains_key(topic) {
            return Ok(());
        }
        // Only this topic's starting ID advances. Advancing the shared cursor
        // could skip undelivered rows of topics that are already subscribed.
        let mut tx = connection.begin().await?;
        let last_id = notification_last_id(&mut tx).await?;
        tx.commit().await?;
        // Cleanup may have deleted rows that are still buffered locally.
        let start_id = last_id.max(self.last_id.unwrap_or_default());
        self.topics.insert(topic.to_owned(), start_id);
        Ok(())
    }

    /// Returns the next notification on a subscribed topic, or `None` once
    /// the outbox has no newer rows on subscribed topics.
    pub(crate) async fn next(
        &mut self,
        connection: &mut MySqlConnection,
    ) -> Result<Option<Notification>, BackendError> {
        loop {
            if self.pending.is_empty() {
                let Some(after_id) = self.last_id else {
                    return Ok(None);
                };
                let topics = self.topics.keys().map(String::as_str).collect::<Vec<_>>();
                // Lock each bounded scan in its own transaction, then release
                // its locks before dispatching notifications. Cursor and buffer
                // changes become visible only after a successful commit.
                let mut tx = connection.begin().await?;
                let notifications =
                    notification_get_after(&mut tx, after_id, &topics, NOTIFICATION_BATCH_SIZE)
                        .await?;
                tx.commit().await?;
                let Some(last) = notifications.last() else {
                    return Ok(None);
                };
                self.last_id = Some(last.id);
                self.pending.extend(notifications);
            }
            // Subscriptions can change between reading a batch and delivering
            // it.
            while let Some(notification) = self.pending.pop_front() {
                if self
                    .topics
                    .get(&notification.topic)
                    .is_some_and(|start_id| notification.id > *start_id)
                {
                    return Ok(Some(notification));
                }
            }
        }
    }
}

pub(crate) async fn notification_cleanup(
    connection: &mut MySqlConnection,
    created_before: DateTime<Utc>,
    limit: i64,
) -> Result<u64, BackendError> {
    if limit <= 0 {
        return Ok(0);
    }
    Ok(sqlx::query(
        "DELETE FROM river_notification WHERE created_at < ? ORDER BY created_at, id LIMIT ?",
    )
    .bind(mysql_time(created_before))
    .bind(limit)
    .execute(&mut *connection)
    .await?
    .rows_affected())
}

pub(crate) async fn stuck_jobs(
    connection: &mut MySqlConnection,
    after_id: i64,
    attempted_before: DateTime<Utc>,
    limit: i32,
) -> Result<Vec<JobRow>, BackendError> {
    if limit <= 0 {
        return Ok(Vec::new());
    }
    let sql = format!(
        r#"
        SELECT {JOB_COLUMNS}
        FROM river_job
        WHERE state = 'running'
          AND id > ?
          AND attempted_at < ?
        ORDER BY id ASC
        LIMIT ?
        "#
    );
    let records = sqlx::query(AssertSqlSafe(sql))
        .bind(after_id)
        .bind(mysql_time(attempted_before))
        .bind(limit)
        .fetch_all(&mut *connection)
        .await?;
    Ok(tolerant_rows(&records))
}

/// Loads exactly the rescue candidates selected by an exact-version
/// extension. The enclosing transaction protects the subsequent state
/// transition.
pub(crate) async fn jobs_by_ids(
    connection: &mut MySqlConnection,
    ids: &[i64],
) -> Result<Vec<JobRow>, BackendError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query =
        QueryBuilder::<MySql>::new(format!("SELECT {JOB_COLUMNS} FROM river_job WHERE id IN ("));
    {
        let mut separated = query.separated(", ");
        for id in ids {
            separated.push_bind(id);
        }
    }
    query.push(") ORDER BY id ASC");
    let records = query.build().fetch_all(&mut *connection).await?;
    Ok(tolerant_rows(&records))
}

/// Decodes rows for the rescuer, which like River Go's `JobGetStuck`
/// tolerates undecodable fields so a job stranded by one can be rescued.
fn tolerant_rows(records: &[MySqlRow]) -> Vec<JobRow> {
    records
        .iter()
        .filter_map(|record| tolerant_row(decode_job_row(record)))
        .collect()
}

pub(crate) async fn cleanup_jobs(
    connection: &mut MySqlConnection,
    params: &CleanupJobs<'_>,
) -> Result<u64, BackendError> {
    let horizons = [
        ("cancelled", params.cancelled_before),
        ("completed", params.completed_before),
        ("discarded", params.discarded_before),
    ];
    if params.limit <= 0 || horizons.iter().all(|(_, before)| before.is_none()) {
        return Ok(0);
    }
    let mut query = QueryBuilder::<MySql>::new("DELETE FROM river_job WHERE (");
    let mut first = true;
    for (state, before) in horizons {
        let Some(before) = before else {
            continue;
        };
        if !first {
            query.push(" OR ");
        }
        first = false;
        query
            .push("(state = ")
            .push_bind(state)
            .push(" AND finalized_at < ")
            .push_bind(mysql_time(before))
            .push(")");
    }
    query.push(")");
    if !params.queues_excluded.is_empty() {
        query.push(" AND CAST(queue AS BINARY) NOT IN (");
        let mut separated = query.separated(", ");
        for queue in params.queues_excluded {
            separated.push_bind(queue);
        }
        separated.push_unseparated(")");
    }
    if let Some(queues_included) = params.queues_included {
        if queues_included.is_empty() {
            return Ok(0);
        }
        query.push(" AND CAST(queue AS BINARY) IN (");
        let mut separated = query.separated(", ");
        for queue in queues_included {
            separated.push_bind(queue);
        }
        separated.push_unseparated(")");
    }
    query
        .push(" ORDER BY id ASC LIMIT ")
        .push_bind(params.limit);
    Ok(query
        .build()
        .execute(&mut *connection)
        .await?
        .rows_affected())
}

/// A due job the scheduler may make available.
#[derive(Clone, Debug, FromRow)]
pub(crate) struct ScheduleCandidate {
    pub id: i64,
    pub unique_key: Option<Vec<u8>>,
}

/// Selects due retryable/scheduled jobs in scheduler order. Scheduling is a
/// multi-step MySQL operation: callers keep a write transaction open while
/// checking unique collisions and applying the transitions below.
///
/// Like River Go, only the columns scheduling needs are selected, so a job
/// whose row can't be decoded, like one whose attempt failed because a JSON
/// column isn't valid JSON, doesn't fail scheduling for every other job. The
/// transitions below return such a job's row with the undecodable fields left
/// empty.
pub(crate) async fn schedule_candidates(
    connection: &mut MySqlConnection,
    now: DateTime<Utc>,
    limit: i32,
) -> Result<Vec<ScheduleCandidate>, BackendError> {
    if limit <= 0 {
        return Ok(Vec::new());
    }
    Ok(sqlx::query_as::<_, ScheduleCandidate>(
        r#"
        SELECT id, unique_key
        FROM river_job
        WHERE state IN ('retryable', 'scheduled') AND scheduled_at <= ?
        ORDER BY priority ASC, scheduled_at ASC, id ASC
        LIMIT ? FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(mysql_time(now))
    .bind(limit)
    .fetch_all(&mut *connection)
    .await?)
}

pub(crate) async fn schedule_has_unique_collision(
    connection: &mut MySqlConnection,
    id: i64,
    unique_key: &[u8],
) -> Result<bool, BackendError> {
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM river_job
            WHERE id != ?
              AND unique_key = ?
              AND unique_states IS NOT NULL
              AND CASE state
                  WHEN 'available' THEN unique_states & (1 << 0)
                  WHEN 'cancelled' THEN unique_states & (1 << 1)
                  WHEN 'completed' THEN unique_states & (1 << 2)
                  WHEN 'discarded' THEN unique_states & (1 << 3)
                  WHEN 'pending' THEN unique_states & (1 << 4)
                  WHEN 'retryable' THEN unique_states & (1 << 5)
                  WHEN 'running' THEN unique_states & (1 << 6)
                  WHEN 'scheduled' THEN unique_states & (1 << 7)
                  ELSE 0
              END >= 1
        )
        "#,
    )
    .bind(id)
    .bind(unique_key)
    .fetch_one(&mut *connection)
    .await?;
    Ok(exists)
}

pub(crate) async fn rescue(
    connection: &mut MySqlConnection,
    params: &RescueJob<'_>,
) -> Result<Option<JobRow>, BackendError> {
    let Some(row) = get_raw(connection, params.id, true).await? else {
        return Ok(None);
    };
    if row.try_get::<String, _>("state")? != "running"
        || row
            .try_get::<Option<DateTime<Utc>>, _>("attempted_at")?
            .is_none_or(|time| time >= params.stuck_horizon)
    {
        return Ok(None);
    }
    let sql = format!(
        "UPDATE river_job SET errors = {ERRORS_APPENDED}, finalized_at = ?, scheduled_at = ?, \
        metadata = JSON_SET(metadata, '$.\"river:rescue_count\"', CASE WHEN JSON_TYPE(JSON_EXTRACT(metadata, '$.\"river:rescue_count\"')) IN ('INTEGER', 'DOUBLE', 'DECIMAL') THEN JSON_EXTRACT(metadata, '$.\"river:rescue_count\"') + 1 ELSE 1 END), state = ? WHERE id = ?"
    );
    sqlx::query(AssertSqlSafe(sql))
        .bind(serde_json::to_string(params.error)?)
        .bind(mysql_time_optional(params.finalized_at))
        .bind(mysql_time(params.scheduled_at))
        .bind(params.state.as_str())
        .bind(params.id)
        .execute(&mut *connection)
        .await?;
    Ok(get_raw(connection, params.id, false)
        .await?
        .as_ref()
        .and_then(|row| tolerant_row(decode_job_row(row))))
}

pub(crate) async fn schedule_set_available(
    connection: &mut MySqlConnection,
    ids: &[i64],
) -> Result<Vec<JobRow>, BackendError> {
    let mut available = Vec::with_capacity(ids.len());
    for id in ids {
        match sqlx::query("UPDATE river_job SET state = 'available' WHERE id = ? AND state IN ('scheduled', 'retryable')").bind(id).execute(&mut *connection).await {
            Ok(result) if result.rows_affected() > 0 => {
                if let Some(row) = get_raw(connection, *id, false).await?.as_ref().and_then(|row| tolerant_row(decode_job_row(row))) { available.push(row); }
            }
            Ok(_) => {}
            Err(error) if error.as_database_error().is_some_and(sqlx::error::DatabaseError::is_unique_violation) => {
                // A concurrent unique insert may win after the scheduler's
                // collision check. InnoDB rolls back this statement only.
                schedule_discard_conflicts(connection, &[*id], Utc::now()).await?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(available)
}

pub(crate) async fn schedule_discard_conflicts(
    connection: &mut MySqlConnection,
    ids: &[i64],
    now: DateTime<Utc>,
) -> Result<Vec<JobRow>, BackendError> {
    for id in ids {
        sqlx::query("UPDATE river_job SET metadata = JSON_SET(metadata, '$.unique_key_conflict', 'scheduler_discarded'), finalized_at = ?, state = 'discarded' WHERE id = ?")
            .bind(mysql_time(now)).bind(id).execute(&mut *connection).await?;
    }
    jobs_by_ids(connection, ids).await
}

#[cfg(all(test, river_mysql_tests))]
mod tests;
