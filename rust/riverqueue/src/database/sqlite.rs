//! SQLite operation primitives.
//!
//! SQLite deliberately stays behind River's sealed database boundary. These
//! operations mirror River's storage semantics without exposing SQLx executor
//! types through the public API. Multi-step operations are expressed as small
//! primitives so their caller can compose them in one SQLite transaction.

#![allow(
    clippy::needless_raw_string_hashes,
    reason = "consistent SQL delimiters make large dialect-specific statements easier to audit"
)]
#![allow(
    clippy::struct_field_names,
    reason = "leader_id is the cross-language River protocol field name"
)]

use std::time::Duration;

use chrono::{DateTime, SubsecRound, Utc};
use serde_json::{Map, Value};
use sqlx::{AssertSqlSafe, FromRow, QueryBuilder, Sqlite, SqliteConnection};

use sqlx::sqlite::SqliteRow;

use crate::{
    AttemptError, JobRow, JobState, METADATA_KEY_UNIQUE_NONCE, Queue,
    client::{DecodedJob, FieldErrors, UndecodableJob, go_time_json, saturating_i16, tolerant_row},
    query::{JobListKeyset, JobListSqlPart},
};

pub(crate) const JOB_COLUMNS: &str = r#"
    id,
    attempt,
    attempted_at,
    CASE WHEN attempted_by IS NULL THEN NULL ELSE json(attempted_by) END AS attempted_by,
    created_at,
    json(args) AS encoded_args,
    CASE WHEN errors IS NULL THEN NULL ELSE json(errors) END AS errors,
    finalized_at,
    kind,
    max_attempts,
    json(metadata) AS metadata,
    priority,
    queue,
    scheduled_at,
    state,
    json(tags) AS tags,
    unique_key,
    unique_states
"#;

const QUEUE_COLUMNS: &str = r#"
    created_at,
    json(metadata) AS metadata,
    name,
    paused_at,
    updated_at
"#;

/// A short poll interval keeps local wakeups responsive while queue fetch
/// polling remains the durable recovery path.
pub(crate) const DEFAULT_NOTIFICATION_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, thiserror::Error)]
pub(crate) enum BackendError {
    #[error("invalid SQLite River row: {0}")]
    InvalidRow(String),
    #[error("SQLite query failed: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("SQLite River JSON failed: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Clone, Debug)]
pub(crate) struct InsertJob<'a> {
    pub attempted_at: Option<DateTime<Utc>>,
    pub attempted_by: &'a [String],
    pub attempt: i16,
    pub created_at: DateTime<Utc>,
    pub encoded_args: &'a serde_json::value::RawValue,
    pub errors: &'a [AttemptError],
    pub finalized_at: Option<DateTime<Utc>>,
    pub id: Option<i64>,
    pub kind: &'a str,
    pub max_attempts: i16,
    pub metadata: &'a Map<String, Value>,
    pub priority: i16,
    pub queue: &'a str,
    pub scheduled_at: DateTime<Utc>,
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
    pub limit: i32,
    pub max_attempted_by: i32,
    pub now: DateTime<Utc>,
    pub queue: &'a str,
}

#[derive(Clone, Debug)]
pub(crate) struct ClaimFilteredJobs<'a> {
    pub client_id: &'a str,
    pub excluded_job_id: i64,
    pub kind: &'a str,
    pub limit: i32,
    pub max_attempted_by: i32,
    pub metadata_matches: &'a Map<String, Value>,
    pub metadata_updates: &'a Map<String, Value>,
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
    pub attempt: Option<i16>,
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
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Leader {
    pub elected_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub leader_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Notification {
    pub created_at: DateTime<Utc>,
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
// integers, so they decode as `i64` and saturate into `JobRow`'s fields.
#[derive(Clone, Debug, FromRow)]
struct JobRecord {
    attempt: i64,
    attempted_at: Option<DateTime<Utc>>,
    attempted_by: Option<String>,
    created_at: DateTime<Utc>,
    encoded_args: String,
    errors: Option<String>,
    finalized_at: Option<DateTime<Utc>>,
    id: i64,
    kind: String,
    max_attempts: i64,
    metadata: String,
    priority: i64,
    queue: String,
    scheduled_at: DateTime<Utc>,
    state: String,
    tags: String,
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
            error: format!("job {}: {error}", self.id),
            row: None,
        };
        let state = JobState::try_from(self.state.as_str())
            .map_err(|error| unidentifiable(error.to_string()))?;
        let encoded_args = serde_json::value::RawValue::from_string(self.encoded_args)
            .map_err(|error| unidentifiable(format!("error decoding `args`: {error}")))?;

        let mut errors = FieldErrors::default();
        let attempted_by = errors.field(
            "attempted_by",
            decode_json_strings(self.attempted_by.as_deref()),
        );
        let attempt_errors = errors.field(
            "errors",
            decode_json_or_default::<Option<Vec<AttemptError>>>(self.errors.as_deref()),
        );
        let metadata = errors.field(
            "metadata",
            serde_json::from_str::<Value>(&self.metadata)
                .map_err(|error| error.to_string())
                .and_then(|metadata| match metadata {
                    Value::Object(metadata) => Ok(metadata),
                    _ => Err("not a JSON object".to_owned()),
                }),
        );
        let tags = errors.field("tags", decode_json_strings(Some(&self.tags)));
        let unique_states = errors.field(
            "unique_states",
            self.unique_states.map(decode_unique_states).transpose(),
        );
        errors.finish(JobRow {
            id: self.id,
            attempt: saturating_i16(self.attempt),
            attempted_at: self.attempted_at,
            attempted_by,
            created_at: self.created_at,
            encoded_args,
            errors: attempt_errors.unwrap_or_default(),
            finalized_at: self.finalized_at,
            kind: self.kind,
            max_attempts: saturating_i16(self.max_attempts),
            metadata,
            priority: saturating_i16(self.priority),
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
pub(crate) fn decode_job_row(row: &SqliteRow) -> DecodedJob {
    JobRecord::from_row(row)
        .map_err(|error| UndecodableJob {
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

#[derive(Clone, Debug, FromRow)]
struct NotificationRecord {
    created_at: DateTime<Utc>,
    id: i64,
    payload: String,
    topic: String,
}

impl From<NotificationRecord> for Notification {
    fn from(record: NotificationRecord) -> Self {
        Self {
            created_at: record.created_at,
            id: record.id,
            payload: record.payload,
            topic: record.topic,
        }
    }
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
        .into_iter()
        .filter(|state| bits & state.unique_bit() != 0)
        .collect())
}

fn json_text(value: &(impl serde::Serialize + ?Sized)) -> Result<String, serde_json::Error> {
    serde_json::to_string(value)
}

pub(crate) fn sqlite_time(time: DateTime<Utc>) -> String {
    time.round_subsecs(3)
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

fn sqlite_time_optional(time: Option<DateTime<Utc>>) -> Option<String> {
    time.map(sqlite_time)
}

fn sqlite_ttl(ttl: Duration) -> String {
    format!("{:.3} seconds", ttl.as_secs_f64())
}

pub(crate) async fn insert(
    connection: &mut SqliteConnection,
    params: &InsertJob<'_>,
) -> Result<InsertedJob, BackendError> {
    let mut metadata = params.metadata.clone();
    if let Some(nonce) = params.unique_nonce {
        metadata.insert(
            METADATA_KEY_UNIQUE_NONCE.to_owned(),
            Value::String(nonce.to_owned()),
        );
    }
    let attempted_by = json_text(params.attempted_by)?;
    let encoded_args = json_text(params.encoded_args)?;
    let errors = json_text(params.errors)?;
    let metadata = json_text(&metadata)?;
    let tags = json_text(params.tags)?;
    let sql = format!(
        r#"
        INSERT INTO river_job (
            id, args, attempt, attempted_at, attempted_by, created_at, errors,
            finalized_at, kind, max_attempts, metadata, priority, queue,
            scheduled_at, state, tags, unique_key, unique_states
        ) VALUES (
            ?, jsonb(?), ?, ?, CASE WHEN ? = '[]' THEN NULL ELSE jsonb(?) END,
            ?, CASE WHEN ? = '[]' THEN NULL ELSE jsonb(?) END, ?, ?, ?,
            jsonb(?), ?, ?, ?, ?, jsonb(?), ?, ?
        )
        ON CONFLICT (unique_key)
            WHERE unique_key IS NOT NULL
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
            DO UPDATE SET kind = excluded.kind
        RETURNING {JOB_COLUMNS}
        "#
    );
    let record = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(params.id)
        .bind(encoded_args)
        .bind(params.attempt)
        .bind(sqlite_time_optional(params.attempted_at))
        .bind(&attempted_by)
        .bind(&attempted_by)
        .bind(sqlite_time(params.created_at))
        .bind(&errors)
        .bind(&errors)
        .bind(sqlite_time_optional(params.finalized_at))
        .bind(params.kind)
        .bind(params.max_attempts)
        .bind(metadata)
        .bind(params.priority)
        .bind(params.queue)
        .bind(sqlite_time(params.scheduled_at))
        .bind(params.state.as_str())
        .bind(tags)
        .bind(params.unique_key)
        .bind(params.unique_states.map(i64::from))
        .fetch_one(&mut *connection)
        .await?;
    let job = record.into_job()?;
    let unique_skipped_as_duplicate = params.unique_nonce.is_some_and(|nonce| {
        job.metadata
            .get(METADATA_KEY_UNIQUE_NONCE)
            .and_then(Value::as_str)
            != Some(nonce)
    });
    Ok(InsertedJob {
        job,
        unique_skipped_as_duplicate,
    })
}

/// Claims due jobs. The claim commits even when a row cannot be decoded, so
/// each row is decoded separately and the caller records a failed attempt for
/// any undecodable row instead of stranding the whole batch as running.
pub(crate) async fn claim(
    connection: &mut SqliteConnection,
    params: &ClaimJobs<'_>,
) -> Result<Vec<DecodedJob>, BackendError> {
    if params.limit <= 0 {
        return Ok(Vec::new());
    }

    let now = sqlite_time(params.now);
    let mut query = QueryBuilder::<Sqlite>::new(
        r#"
        UPDATE river_job
        SET
            attempt = attempt + 1,
            attempted_at = "#,
    );
    query.push_bind(&now);
    query.push(
        r#",
            attempted_by = jsonb(json_insert(
                (
                    SELECT jsonb_group_array(value)
                    FROM (
                        SELECT value FROM (
                            SELECT key, value
                            FROM json_each(coalesce(attempted_by, jsonb('[]')))
                            ORDER BY key DESC
                            LIMIT "#,
    );
    query.push_bind(params.max_attempted_by.saturating_sub(1));
    query.push(
        r#"
                        ) ORDER BY key ASC
                    )
                ),
                '$[#]',
                "#,
    );
    query.push_bind(params.client_id);
    query.push(
        r#"
            )),
            state = 'running'
        WHERE id IN (
            SELECT river_job.id
            FROM river_job
            WHERE queue = "#,
    );
    query.push_bind(params.queue);
    query.push(" AND scheduled_at <= ");
    query.push_bind(&now);
    query.push(
        r#"
              AND state = 'available'
              AND NOT EXISTS (
                  SELECT 1
                  FROM river_queue
                  WHERE river_queue.name = river_job.queue
                    AND river_queue.paused_at IS NOT NULL
              )
            ORDER BY priority ASC, scheduled_at ASC, id ASC
            LIMIT "#,
    );
    query.push_bind(params.limit);
    query.push(format!(") RETURNING {JOB_COLUMNS}"));

    let rows = query.build().fetch_all(&mut *connection).await?;
    Ok(rows.iter().map(decode_job_row).collect())
}

/// Atomically claims due jobs matching exact-version extension filters.
pub(crate) async fn claim_filtered(
    connection: &mut SqliteConnection,
    params: &ClaimFilteredJobs<'_>,
) -> Result<Vec<JobRow>, BackendError> {
    if params.limit <= 0 {
        return Ok(Vec::new());
    }

    let now = sqlite_time(params.now);
    let metadata_updates = json_text(params.metadata_updates)?;
    let mut query = QueryBuilder::<Sqlite>::new(
        r#"
        UPDATE river_job
        SET
            attempt = attempt + 1,
            attempted_at = "#,
    );
    query.push_bind(&now);
    query.push(
        r#",
            attempted_by = jsonb(json_insert(
                (
                    SELECT jsonb_group_array(value)
                    FROM (
                        SELECT value FROM (
                            SELECT key, value
                            FROM json_each(coalesce(attempted_by, jsonb('[]')))
                            ORDER BY key DESC
                            LIMIT "#,
    );
    query.push_bind(params.max_attempted_by.saturating_sub(1));
    query.push(
        r#"
                        ) ORDER BY key ASC
                    )
                ),
                '$[#]',
                "#,
    );
    query.push_bind(params.client_id);
    query.push(
        r#"
            )),
            metadata = jsonb(json_patch(json(metadata), json("#,
    );
    query.push_bind(metadata_updates);
    query.push(
        r#"))),
            state = 'running'
        WHERE id IN (
            SELECT river_job.id
            FROM river_job
            WHERE state = 'available'
              AND queue = "#,
    );
    query.push_bind(params.queue);
    query.push(" AND kind = ");
    query.push_bind(params.kind);
    query.push(" AND id != ");
    query.push_bind(params.excluded_job_id);
    query.push(" AND scheduled_at <= ");
    query.push_bind(&now);
    for (key, value) in params.metadata_matches {
        let path = format!("$.{}", serde_json::to_string(key)?);
        query.push(" AND json_extract(json(metadata), ");
        query.push_bind(path);
        query.push(") IS json_extract(");
        query.push_bind(serde_json::to_string(value)?);
        query.push(", '$')");
    }
    query.push(" ORDER BY priority ASC, scheduled_at ASC, id ASC LIMIT ");
    query.push_bind(params.limit);
    query.push(format!(") RETURNING {JOB_COLUMNS}"));

    let records = query
        .build_query_as::<JobRecord>()
        .fetch_all(&mut *connection)
        .await?;
    records.into_iter().map(JobRecord::into_job).collect()
}

/// Claims exactly the IDs selected by an exact-version extension.
///
/// The caller keeps selection and this update in one transaction. Eligibility
/// beyond the final running-state guard is deliberately the selector's
/// responsibility, matching the PostgreSQL interception path.
pub(crate) async fn claim_selected(
    connection: &mut SqliteConnection,
    params: &ClaimJobs<'_>,
    ids: &[i64],
) -> Result<Vec<DecodedJob>, BackendError> {
    if params.limit <= 0 || ids.is_empty() {
        return Ok(Vec::new());
    }

    let now = sqlite_time(params.now);
    let mut query = QueryBuilder::<Sqlite>::new(
        r#"
        UPDATE river_job
        SET
            attempt = attempt + 1,
            attempted_at = "#,
    );
    query.push_bind(&now);
    query.push(
        r#",
            attempted_by = jsonb(json_insert(
                (
                    SELECT jsonb_group_array(value)
                    FROM (
                        SELECT value FROM (
                            SELECT key, value
                            FROM json_each(coalesce(attempted_by, jsonb('[]')))
                            ORDER BY key DESC
                            LIMIT "#,
    );
    query.push_bind(params.max_attempted_by.saturating_sub(1));
    query.push(
        r#"
                        ) ORDER BY key ASC
                    )
                ),
                '$[#]',
                "#,
    );
    query.push_bind(params.client_id);
    query.push(
        r#"
            )),
            state = 'running'
        WHERE state = 'available' AND id IN ("#,
    );
    {
        let mut separated = query.separated(", ");
        for id in ids {
            separated.push_bind(id);
        }
    }
    query.push(format!(") RETURNING {JOB_COLUMNS}"));

    let rows = query.build().fetch_all(&mut *connection).await?;
    Ok(rows.iter().map(decode_job_row).collect())
}

pub(crate) async fn get(
    connection: &mut SqliteConnection,
    id: i64,
) -> Result<Option<JobRow>, BackendError> {
    let sql = format!("SELECT {JOB_COLUMNS} FROM river_job WHERE id = ? LIMIT 1");
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?
        .map(JobRecord::into_job)
        .transpose()
}

#[allow(
    clippy::too_many_lines,
    reason = "the bound-only query builder keeps all list filters and keyset ordering auditable"
)]
pub(crate) async fn list(
    connection: &mut SqliteConnection,
    params: &ListJobs<'_>,
) -> Result<Vec<JobRow>, BackendError> {
    if params.limit <= 0 {
        return Ok(Vec::new());
    }

    let mut query =
        QueryBuilder::<Sqlite>::new(format!("SELECT {JOB_COLUMNS} FROM river_job WHERE true"));
    if let Some(after) = params.keyset.after_sql() {
        query.push(" AND ");
        for part in after {
            match part {
                JobListSqlPart::AfterId => {
                    query.push_bind(params.keyset.after_id().expect("cursor has an ID"));
                }
                JobListSqlPart::AfterTime => {
                    query.push_bind(sqlite_time(
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
        query.push(" AND kind IN (");
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
        query.push(" AND queue IN (");
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
        let metadata = json_text(metadata)?;
        query
            .push(" AND json_patch(json(metadata), json(")
            .push_bind(metadata)
            .push(")) = json(metadata)");
    }
    for tag in params.tags_all {
        query
            .push(" AND EXISTS (SELECT 1 FROM json_each(json(tags)) WHERE value = ")
            .push_bind(tag)
            .push(")");
    }
    if !params.tags_any.is_empty() {
        query.push(" AND EXISTS (SELECT 1 FROM json_each(json(tags)) WHERE value IN (");
        let mut separated = query.separated(", ");
        for tag in params.tags_any {
            separated.push_bind(tag);
        }
        separated.push_unseparated("))");
    }
    query.push(" ORDER BY ").push(params.keyset.order_sql());
    query.push(" LIMIT ").push_bind(params.limit);

    let records = query
        .build_query_as::<JobRecord>()
        .fetch_all(&mut *connection)
        .await?;
    records.into_iter().map(JobRecord::into_job).collect()
}

/// Marks cancellation intent. A running job stays running so its worker can
/// observe cancellation; any other non-finalized job is finalized immediately.
pub(crate) async fn cancel(
    connection: &mut SqliteConnection,
    id: i64,
    now: DateTime<Utc>,
) -> Result<Option<JobRow>, BackendError> {
    let cancel_attempted_at = json_text(&go_time_json(now))?;
    let sql = format!(
        r#"
        UPDATE river_job
        SET
            state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END,
            finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE ? END,
            metadata = jsonb_set(metadata, '$.cancel_attempted_at', jsonb(?))
        WHERE id = ?
          AND state NOT IN ('cancelled', 'completed', 'discarded')
          AND finalized_at IS NULL
        RETURNING {JOB_COLUMNS}
        "#
    );
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(sqlite_time(now))
        .bind(cancel_attempted_at)
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?
        .map(JobRecord::into_job)
        .transpose()
}

/// Deletes a non-running job. A missing result is intentionally ambiguous;
/// callers distinguish not-found from running inside the same transaction.
pub(crate) async fn delete(
    connection: &mut SqliteConnection,
    id: i64,
) -> Result<Option<JobRow>, BackendError> {
    let sql = format!(
        "DELETE FROM river_job WHERE id = ? AND state != 'running' RETURNING {JOB_COLUMNS}"
    );
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?
        .map(JobRecord::into_job)
        .transpose()
}

/// Makes a non-running job immediately available. A missing result is
/// intentionally ambiguous; callers fetch in the same transaction to
/// distinguish running, already-available, and not-found jobs.
pub(crate) async fn retry(
    connection: &mut SqliteConnection,
    id: i64,
    now: DateTime<Utc>,
) -> Result<Option<JobRow>, BackendError> {
    let sql = format!(
        r#"
        UPDATE river_job
        SET
            state = 'available',
            max_attempts = CASE
                WHEN attempt = max_attempts THEN max_attempts + 1
                ELSE max_attempts
            END,
            finalized_at = NULL,
            scheduled_at = ?
        WHERE id = ?
          AND state != 'running'
          AND (state != 'available' OR scheduled_at > ?)
        RETURNING {JOB_COLUMNS}
        "#
    );
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(sqlite_time(now))
        .bind(id)
        .bind(sqlite_time(now))
        .fetch_optional(&mut *connection)
        .await?
        .map(JobRecord::into_job)
        .transpose()
}

pub(crate) async fn complete(
    connection: &mut SqliteConnection,
    params: &CompleteJob<'_>,
) -> Result<Option<JobRow>, BackendError> {
    complete_decoded(connection, params)
        .await?
        .map(|row| row.map_err(|job| BackendError::InvalidRow(job.error)))
        .transpose()
}

/// Sets a running job's state, decoding the returned row separately so a
/// malformed row cannot fail the surrounding completion transaction.
pub(crate) async fn complete_decoded(
    connection: &mut SqliteConnection,
    params: &CompleteJob<'_>,
) -> Result<Option<DecodedJob>, BackendError> {
    let error = params
        .error
        .map(json_text)
        .transpose()?
        .unwrap_or_else(|| "{}".to_owned());
    let metadata = params
        .metadata_updates
        .map(json_text)
        .transpose()?
        .unwrap_or_else(|| "{}".to_owned());
    // Like River Go, a `cancel_attempted_at` key cancels the job even when its
    // value is JSON `null`; `->` distinguishes that from a missing key.
    let should_cancel = r#"(
        (? IN ('available', 'retryable', 'scheduled'))
        AND (metadata -> 'cancel_attempted_at') IS NOT NULL
    )"#;
    let sql = format!(
        r#"
        UPDATE river_job
        SET
            attempt = CASE
                WHEN NOT {should_cancel} AND ? THEN ?
                ELSE attempt
            END,
            -- `errors` is always an array unless it's been changed out of
            -- band. Like River Go, wrap any other value in an array so the
            -- new error is still appended without losing it.
            errors = CASE
                WHEN ? AND coalesce(json_type(errors), 'array') <> 'array'
                    THEN jsonb(json_array(json(errors), json(?)))
                WHEN ? THEN jsonb(json_insert(
                    json(coalesce(errors, jsonb('[]'))), '$[#]', json(?)
                ))
                ELSE errors
            END,
            finalized_at = CASE
                WHEN {should_cancel} THEN ?
                WHEN ? THEN ?
                ELSE finalized_at
            END,
            metadata = CASE
                WHEN ? THEN jsonb_patch(json(metadata), json(?))
                ELSE metadata
            END,
            scheduled_at = CASE
                WHEN NOT {should_cancel} AND ? THEN ?
                ELSE scheduled_at
            END,
            state = CASE WHEN {should_cancel} THEN 'cancelled' ELSE ? END
        WHERE id = ? AND state = 'running'
        RETURNING {JOB_COLUMNS}
        "#
    );
    let state = params.state.as_str();
    let row = sqlx::query(AssertSqlSafe(sql))
        .bind(state)
        .bind(params.attempt.is_some())
        .bind(params.attempt.unwrap_or_default())
        .bind(params.error.is_some())
        .bind(&error)
        .bind(params.error.is_some())
        .bind(&error)
        .bind(state)
        .bind(sqlite_time(params.now))
        .bind(params.finalized_at.is_some())
        .bind(sqlite_time_optional(params.finalized_at))
        .bind(params.metadata_updates.is_some())
        .bind(metadata)
        .bind(state)
        .bind(params.scheduled_at.is_some())
        .bind(sqlite_time_optional(params.scheduled_at))
        .bind(state)
        .bind(state)
        .bind(params.id)
        .fetch_optional(&mut *connection)
        .await?;
    Ok(row.as_ref().map(decode_job_row))
}

/// Applies completion metadata after another actor has already moved a job
/// out of `running`, preserving the winning terminal state.
pub(crate) async fn merge_metadata_if_not_running(
    connection: &mut SqliteConnection,
    id: i64,
    metadata_updates: &Map<String, Value>,
) -> Result<Option<DecodedJob>, BackendError> {
    let metadata = json_text(metadata_updates)?;
    let sql = format!(
        r#"
        UPDATE river_job
        SET metadata = jsonb_patch(json(metadata), json(?))
        WHERE id = ? AND state != 'running'
        RETURNING {JOB_COLUMNS}
        "#
    );
    let row = sqlx::query(AssertSqlSafe(sql))
        .bind(metadata)
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?;
    Ok(row.as_ref().map(decode_job_row))
}

pub(crate) async fn update(
    connection: &mut SqliteConnection,
    id: i64,
    metadata_updates: &Map<String, Value>,
) -> Result<Option<JobRow>, BackendError> {
    let metadata = json_text(metadata_updates)?;
    let sql = format!(
        r#"
        UPDATE river_job
        SET metadata = jsonb_patch(json(metadata), json(?))
        WHERE id = ?
        RETURNING {JOB_COLUMNS}
        "#
    );
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(metadata)
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?
        .map(JobRecord::into_job)
        .transpose()
}

pub(crate) async fn queue_upsert(
    connection: &mut SqliteConnection,
    name: &str,
    metadata: &Map<String, Value>,
    paused_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<Queue, BackendError> {
    let metadata = json_text(metadata)?;
    let sql = format!(
        r#"
        INSERT INTO river_queue (created_at, metadata, name, paused_at, updated_at)
        VALUES (?, jsonb(?), ?, ?, ?)
        ON CONFLICT (name) DO UPDATE SET updated_at = excluded.updated_at
        RETURNING {QUEUE_COLUMNS}
        "#
    );
    sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql))
        .bind(sqlite_time(now))
        .bind(metadata)
        .bind(name)
        .bind(sqlite_time_optional(paused_at))
        .bind(sqlite_time(now))
        .fetch_one(&mut *connection)
        .await?
        .into_queue()
}

pub(crate) async fn queue_get(
    connection: &mut SqliteConnection,
    name: &str,
) -> Result<Option<Queue>, BackendError> {
    let sql = format!("SELECT {QUEUE_COLUMNS} FROM river_queue WHERE name = ?");
    sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql))
        .bind(name)
        .fetch_optional(&mut *connection)
        .await?
        .map(QueueRecord::into_queue)
        .transpose()
}

pub(crate) async fn queue_list(
    connection: &mut SqliteConnection,
    limit: i32,
) -> Result<Vec<Queue>, BackendError> {
    if limit <= 0 {
        return Ok(Vec::new());
    }
    let sql = format!("SELECT {QUEUE_COLUMNS} FROM river_queue ORDER BY name ASC LIMIT ?");
    let records = sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql))
        .bind(limit)
        .fetch_all(&mut *connection)
        .await?;
    records.into_iter().map(QueueRecord::into_queue).collect()
}

pub(crate) async fn queue_pause(
    connection: &mut SqliteConnection,
    name: &str,
    now: DateTime<Utc>,
) -> Result<Vec<Queue>, BackendError> {
    queue_set_paused(connection, name, Some(now), now).await
}

pub(crate) async fn queue_resume(
    connection: &mut SqliteConnection,
    name: &str,
    now: DateTime<Utc>,
) -> Result<Vec<Queue>, BackendError> {
    queue_set_paused(connection, name, None, now).await
}

async fn queue_set_paused(
    connection: &mut SqliteConnection,
    name: &str,
    paused_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<Vec<Queue>, BackendError> {
    let (paused_at_sql, changed_sql) = if paused_at.is_some() {
        ("coalesce(paused_at, ?)", "paused_at IS NULL")
    } else {
        ("NULL", "paused_at IS NOT NULL")
    };
    let sql = format!(
        r#"
        UPDATE river_queue
        SET
            paused_at = {paused_at_sql},
            updated_at = CASE WHEN {changed_sql} THEN ? ELSE updated_at END
        WHERE (? = '*' OR name = ?)
        RETURNING {QUEUE_COLUMNS}
        "#
    );
    let mut query = sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql));
    if let Some(paused_at) = paused_at {
        query = query.bind(sqlite_time(paused_at));
    }
    let records = query
        .bind(sqlite_time(now))
        .bind(name)
        .bind(name)
        .fetch_all(&mut *connection)
        .await?;
    records.into_iter().map(QueueRecord::into_queue).collect()
}

pub(crate) async fn queue_update(
    connection: &mut SqliteConnection,
    name: &str,
    metadata: &Map<String, Value>,
    now: DateTime<Utc>,
) -> Result<Option<Queue>, BackendError> {
    let metadata = json_text(metadata)?;
    let sql = format!(
        r#"
        UPDATE river_queue
        SET metadata = jsonb(?), updated_at = ?
        WHERE name = ?
        RETURNING {QUEUE_COLUMNS}
        "#
    );
    sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql))
        .bind(metadata)
        .bind(sqlite_time(now))
        .bind(name)
        .fetch_optional(&mut *connection)
        .await?
        .map(QueueRecord::into_queue)
        .transpose()
}

pub(crate) async fn queue_delete_expired(
    connection: &mut SqliteConnection,
    updated_before: DateTime<Utc>,
    limit: i32,
) -> Result<Vec<String>, BackendError> {
    if limit <= 0 {
        return Ok(Vec::new());
    }
    let records = sqlx::query_scalar::<_, String>(
        r#"
        DELETE FROM river_queue
        WHERE name IN (
            SELECT name
            FROM river_queue
            WHERE updated_at < ?
            ORDER BY name ASC
            LIMIT ?
        )
        RETURNING name
        "#,
    )
    .bind(sqlite_time(updated_before))
    .bind(limit)
    .fetch_all(&mut *connection)
    .await?;
    Ok(records)
}

pub(crate) async fn leader_elect(
    connection: &mut SqliteConnection,
    leader_id: &str,
    now: DateTime<Utc>,
    ttl: Duration,
) -> Result<Option<Leader>, BackendError> {
    sqlx::query_as::<_, LeaderRecord>(
        r#"
        INSERT INTO river_leader (leader_id, elected_at, expires_at)
        VALUES (?, ?, datetime(?, 'subsec', ?))
        ON CONFLICT (name) DO NOTHING
        RETURNING elected_at, expires_at, leader_id
        "#,
    )
    .bind(leader_id)
    .bind(sqlite_time(now))
    .bind(sqlite_time(now))
    .bind(sqlite_ttl(ttl))
    .fetch_optional(&mut *connection)
    .await
    .map(|record| record.map(Leader::from))
    .map_err(BackendError::from)
}

/// Extends a lease only for the same leader *and* term (`elected_at`), like
/// Go's `LeaderAttemptReelect`.
pub(crate) async fn leader_reelect(
    connection: &mut SqliteConnection,
    leader_id: &str,
    elected_at: DateTime<Utc>,
    now: DateTime<Utc>,
    ttl: Duration,
) -> Result<Option<Leader>, BackendError> {
    sqlx::query_as::<_, LeaderRecord>(
        r#"
        UPDATE river_leader
        SET expires_at = datetime(?, 'subsec', ?)
        WHERE unixepoch(elected_at, 'subsec') = unixepoch(?, 'subsec')
          AND unixepoch(expires_at, 'subsec') >= unixepoch(?, 'subsec')
          AND leader_id = ?
        RETURNING elected_at, expires_at, leader_id
        "#,
    )
    .bind(sqlite_time(now))
    .bind(sqlite_ttl(ttl))
    .bind(sqlite_time(elected_at))
    .bind(sqlite_time(now))
    .bind(leader_id)
    .fetch_optional(&mut *connection)
    .await
    .map(|record| record.map(Leader::from))
    .map_err(BackendError::from)
}

/// Deletes a lease only for the same leader *and* term, like Go's
/// `LeaderResign`.
pub(crate) async fn leader_resign(
    connection: &mut SqliteConnection,
    leader_id: &str,
    elected_at: DateTime<Utc>,
) -> Result<bool, BackendError> {
    let result = sqlx::query(
        r#"
        DELETE FROM river_leader
        WHERE unixepoch(elected_at, 'subsec') = unixepoch(?, 'subsec')
          AND leader_id = ?
        "#,
    )
    .bind(sqlite_time(elected_at))
    .bind(leader_id)
    .execute(&mut *connection)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub(crate) async fn leader_delete_expired(
    connection: &mut SqliteConnection,
    now: DateTime<Utc>,
) -> Result<u64, BackendError> {
    let result = sqlx::query("DELETE FROM river_leader WHERE expires_at < ?")
        .bind(sqlite_time(now))
        .execute(&mut *connection)
        .await?;
    Ok(result.rows_affected())
}

pub(crate) async fn notification_insert(
    connection: &mut SqliteConnection,
    notifications: &[NotificationInput<'_>],
) -> Result<u64, BackendError> {
    if notifications.is_empty() {
        return Ok(0);
    }
    let mut query = QueryBuilder::<Sqlite>::new("INSERT INTO river_notification (payload, topic) ");
    query.push_values(notifications, |mut row, notification| {
        row.push_bind(notification.payload)
            .push_bind(notification.topic);
    });
    Ok(query
        .build()
        .execute(&mut *connection)
        .await?
        .rows_affected())
}

pub(crate) async fn notification_poll(
    connection: &mut SqliteConnection,
    after_id: i64,
    limit: i32,
) -> Result<Vec<Notification>, BackendError> {
    if limit <= 0 {
        return Ok(Vec::new());
    }
    let records = sqlx::query_as::<_, NotificationRecord>(
        r#"
        SELECT created_at, id, payload, topic
        FROM river_notification
        WHERE id > ?
        ORDER BY id ASC
        LIMIT ?
        "#,
    )
    .bind(after_id)
    .bind(limit)
    .fetch_all(&mut *connection)
    .await?;
    Ok(records.into_iter().map(Notification::from).collect())
}

pub(crate) async fn notification_last_id(
    connection: &mut SqliteConnection,
) -> Result<i64, BackendError> {
    sqlx::query_scalar("SELECT coalesce(max(id), 0) FROM river_notification")
        .fetch_one(&mut *connection)
        .await
        .map_err(BackendError::from)
}

pub(crate) async fn notification_cleanup(
    connection: &mut SqliteConnection,
    created_before: DateTime<Utc>,
    limit: i64,
) -> Result<u64, BackendError> {
    if limit <= 0 {
        return Ok(0);
    }
    Ok(sqlx::query(
        "DELETE FROM river_notification WHERE id IN (\
         SELECT id FROM river_notification WHERE created_at < ? \
         ORDER BY id ASC LIMIT ?)",
    )
    .bind(sqlite_time(created_before))
    .bind(limit)
    .execute(&mut *connection)
    .await?
    .rows_affected())
}

pub(crate) async fn stuck_jobs(
    connection: &mut SqliteConnection,
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
        .bind(sqlite_time(attempted_before))
        .bind(limit)
        .fetch_all(&mut *connection)
        .await?;
    Ok(tolerant_rows(&records))
}

/// Loads exactly the rescue candidates selected by an exact-version
/// extension. The enclosing transaction protects the subsequent state
/// transition.
pub(crate) async fn jobs_by_ids(
    connection: &mut SqliteConnection,
    ids: &[i64],
) -> Result<Vec<JobRow>, BackendError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query =
        QueryBuilder::<Sqlite>::new(format!("SELECT {JOB_COLUMNS} FROM river_job WHERE id IN ("));
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
fn tolerant_rows(records: &[SqliteRow]) -> Vec<JobRow> {
    records
        .iter()
        .filter_map(|record| tolerant_row(decode_job_row(record)))
        .collect()
}

pub(crate) async fn rescue(
    connection: &mut SqliteConnection,
    params: &RescueJob<'_>,
) -> Result<Option<JobRow>, BackendError> {
    let error = json_text(params.error)?;
    let sql = format!(
        r#"
        UPDATE river_job
        SET
            errors = jsonb(json_insert(
                json(coalesce(errors, jsonb('[]'))), '$[#]', json(?)
            )),
            finalized_at = ?,
            scheduled_at = ?,
            metadata = jsonb_set(
                metadata,
                '$."river:rescue_count"',
                coalesce(
                    CASE json_type(metadata, '$."river:rescue_count"')
                        WHEN 'integer' THEN json_extract(metadata, '$."river:rescue_count"')
                        WHEN 'real' THEN json_extract(metadata, '$."river:rescue_count"')
                    END,
                    0
                ) + 1
            ),
            state = ?
        WHERE id = ? AND state = 'running' AND attempted_at < ?
        RETURNING {JOB_COLUMNS}
        "#
    );
    let row = sqlx::query(AssertSqlSafe(sql))
        .bind(error)
        .bind(sqlite_time_optional(params.finalized_at))
        .bind(sqlite_time(params.scheduled_at))
        .bind(params.state.as_str())
        .bind(params.id)
        .bind(sqlite_time(params.stuck_horizon))
        .fetch_optional(&mut *connection)
        .await?;
    // Like River Go, a rescued job whose row can't be fully decoded is still
    // rescued rather than failing the rescuer's transaction.
    Ok(row
        .as_ref()
        .and_then(|row| tolerant_row(decode_job_row(row))))
}

pub(crate) async fn cleanup_jobs(
    connection: &mut SqliteConnection,
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
    let mut query = QueryBuilder::<Sqlite>::new(
        "DELETE FROM river_job WHERE id IN (SELECT id FROM river_job WHERE (",
    );
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
            .push_bind(sqlite_time(before))
            .push(")");
    }
    query.push(")");
    if !params.queues_excluded.is_empty() {
        query.push(" AND queue NOT IN (");
        let mut separated = query.separated(", ");
        for queue in params.queues_excluded {
            separated.push_bind(queue);
        }
        separated.push_unseparated(")");
    }
    query
        .push(" ORDER BY id ASC LIMIT ")
        .push_bind(params.limit)
        .push(")");
    Ok(query
        .build()
        .execute(&mut *connection)
        .await?
        .rows_affected())
}

/// Selects due retryable/scheduled jobs in scheduler order. Scheduling is a
/// multi-step SQLite operation: callers keep a write transaction open while
/// checking unique collisions and applying the transitions below.
pub(crate) async fn schedule_candidates(
    connection: &mut SqliteConnection,
    now: DateTime<Utc>,
    limit: i32,
) -> Result<Vec<JobRow>, BackendError> {
    if limit <= 0 {
        return Ok(Vec::new());
    }
    let sql = format!(
        r#"
        SELECT {JOB_COLUMNS}
        FROM river_job
        WHERE state IN ('retryable', 'scheduled') AND scheduled_at <= ?
        ORDER BY priority ASC, scheduled_at ASC, id ASC
        LIMIT ?
        "#
    );
    let records = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(sqlite_time(now))
        .bind(limit)
        .fetch_all(&mut *connection)
        .await?;
    records.into_iter().map(JobRecord::into_job).collect()
}

pub(crate) async fn schedule_has_unique_collision(
    connection: &mut SqliteConnection,
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

pub(crate) async fn schedule_set_available(
    connection: &mut SqliteConnection,
    ids: &[i64],
) -> Result<Vec<JobRow>, BackendError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query =
        QueryBuilder::<Sqlite>::new("UPDATE river_job SET state = 'available' WHERE id IN (");
    {
        let mut separated = query.separated(", ");
        for id in ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(")");
    }
    query.push(format!(" RETURNING {JOB_COLUMNS}"));
    let records = query
        .build_query_as::<JobRecord>()
        .fetch_all(&mut *connection)
        .await?;
    records.into_iter().map(JobRecord::into_job).collect()
}

pub(crate) async fn schedule_discard_conflicts(
    connection: &mut SqliteConnection,
    ids: &[i64],
    now: DateTime<Utc>,
) -> Result<Vec<JobRow>, BackendError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = QueryBuilder::<Sqlite>::new(
        r#"
        UPDATE river_job
        SET
            metadata = jsonb_patch(
                json(metadata),
                json('{"unique_key_conflict":"scheduler_discarded"}')
            ),
            finalized_at = "#,
    );
    query
        .push_bind(sqlite_time(now))
        .push(", state = 'discarded' WHERE id IN (");
    {
        let mut separated = query.separated(", ");
        for id in ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(")");
    }
    query.push(format!(" RETURNING {JOB_COLUMNS}"));
    let records = query
        .build_query_as::<JobRecord>()
        .fetch_all(&mut *connection)
        .await?;
    records.into_iter().map(JobRecord::into_job).collect()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeDelta, TimeZone, Timelike};
    use serde_json::json;
    use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};

    use super::*;

    const SCHEMA: &str = r#"
        CREATE TABLE river_job (
            id integer PRIMARY KEY,
            args jsonb NOT NULL DEFAULT (jsonb('{}')),
            attempt integer NOT NULL DEFAULT 0,
            attempted_at timestamp,
            attempted_by jsonb,
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            errors jsonb,
            finalized_at timestamp,
            kind text NOT NULL,
            max_attempts integer NOT NULL DEFAULT 25,
            metadata jsonb NOT NULL DEFAULT (jsonb('{}')),
            priority integer NOT NULL DEFAULT 1,
            queue text NOT NULL DEFAULT 'default',
            state text NOT NULL DEFAULT 'available',
            scheduled_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            tags jsonb NOT NULL DEFAULT (jsonb('[]')),
            unique_key blob,
            unique_states integer
        );
        CREATE UNIQUE INDEX river_job_unique_idx ON river_job (unique_key)
        WHERE unique_key IS NOT NULL
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
          END >= 1;
        CREATE TABLE river_queue (
            name text PRIMARY KEY NOT NULL,
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            metadata jsonb NOT NULL DEFAULT (jsonb('{}')),
            paused_at timestamp,
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE river_leader (
            elected_at timestamp NOT NULL,
            expires_at timestamp NOT NULL,
            leader_id text NOT NULL,
            name text PRIMARY KEY NOT NULL DEFAULT 'default' CHECK (name = 'default')
        );
        CREATE TABLE river_notification (
            id integer PRIMARY KEY AUTOINCREMENT,
            created_at timestamp NOT NULL DEFAULT (datetime('now', 'subsec')),
            payload text NOT NULL,
            topic text NOT NULL
        );
    "#;

    async fn setup() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(SCHEMA).execute(&pool).await.unwrap();
        pool
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one scenario verifies the complete persisted SQLite job lifecycle"
    )]
    #[tokio::test]
    async fn job_lifecycle_and_unique_insert() {
        let pool = setup().await;
        let mut connection = pool.acquire().await.unwrap();
        let now = Utc
            .with_ymd_and_hms(2026, 1, 2, 3, 4, 5)
            .unwrap()
            .with_nanosecond(123_800_000)
            .unwrap();
        let args = serde_json::value::to_raw_value(&json!({"message": "hello"})).unwrap();
        let metadata = Map::new();
        let tags = vec!["mail".to_owned()];
        let unique_key = [7_u8; 32];
        let insert_params = InsertJob {
            attempt: 0,
            attempted_at: None,
            attempted_by: &[],
            created_at: now,
            encoded_args: &args,
            errors: &[],
            finalized_at: None,
            id: None,
            kind: "send_mail",
            max_attempts: 25,
            metadata: &metadata,
            priority: 1,
            queue: "default",
            scheduled_at: now,
            state: JobState::Available,
            tags: &tags,
            unique_key: Some(&unique_key),
            unique_nonce: Some("first"),
            unique_states: Some(
                JobState::UNIQUE_DEFAULT
                    .into_iter()
                    .fold(0, |bits, state| bits | state.unique_bit()),
            ),
        };

        let inserted = insert(&mut connection, &insert_params).await.unwrap();
        assert!(!inserted.unique_skipped_as_duplicate);
        assert_eq!(inserted.job.encoded_args.get(), args.get());
        assert_eq!(inserted.job.tags, tags);
        let (created_at, scheduled_at): (String, String) =
            sqlx::query_as("SELECT created_at, scheduled_at FROM river_job WHERE id = ?")
                .bind(inserted.job.id)
                .fetch_one(&mut *connection)
                .await
                .unwrap();
        assert_eq!(created_at, "2026-01-02 03:04:05.124");
        assert_eq!(scheduled_at, "2026-01-02 03:04:05.124");

        let duplicate = InsertJob {
            unique_nonce: Some("second"),
            ..insert_params
        };
        let duplicate = insert(&mut connection, &duplicate).await.unwrap();
        assert!(duplicate.unique_skipped_as_duplicate);
        assert_eq!(duplicate.job.id, inserted.job.id);

        queue_upsert(&mut connection, "default", &Map::new(), None, now)
            .await
            .unwrap();
        queue_pause(&mut connection, "default", now).await.unwrap();
        let claim_params = ClaimJobs {
            client_id: "client-1",
            limit: 10,
            max_attempted_by: 100,
            now,
            queue: "default",
        };
        assert!(
            claim(&mut connection, &claim_params)
                .await
                .unwrap()
                .is_empty()
        );

        queue_resume(&mut connection, "default", now).await.unwrap();
        let claimed = claim(&mut connection, &claim_params)
            .await
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].attempt, 1);
        assert_eq!(claimed[0].attempted_by, ["client-1"]);
        assert_eq!(claimed[0].state, JobState::Running);

        let mut output = Map::new();
        output.insert(crate::METADATA_KEY_OUTPUT.to_owned(), json!({"sent": true}));
        let completed = complete(
            &mut connection,
            &CompleteJob {
                attempt: None,
                error: None,
                finalized_at: Some(now),
                id: claimed[0].id,
                metadata_updates: Some(&output),
                now,
                scheduled_at: None,
                state: JobState::Completed,
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(completed.state, JobState::Completed);
        assert_eq!(completed.output(), Some(&json!({"sent": true})));

        let listed = list(
            &mut connection,
            &ListJobs {
                states: &[JobState::Completed],
                tags_all: &["mail"],
                limit: 10,
                ..ListJobs::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(listed.len(), 1);
        assert!(
            delete(&mut connection, completed.id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(get(&mut connection, completed.id).await.unwrap().is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one scenario walks every maintenance query in order"
    )]
    async fn maintenance_leadership_and_wakeup_outbox() {
        let pool = setup().await;
        let mut connection = pool.acquire().await.unwrap();
        let now = Utc::now();
        let args = serde_json::value::to_raw_value(&json!({})).unwrap();
        let metadata = Map::new();
        let scheduled = insert(
            &mut connection,
            &InsertJob {
                attempt: 0,
                attempted_at: None,
                attempted_by: &[],
                created_at: now,
                encoded_args: &args,
                errors: &[],
                finalized_at: None,
                id: None,
                kind: "scheduled",
                max_attempts: 3,
                metadata: &metadata,
                priority: 1,
                queue: "default",
                scheduled_at: now,
                state: JobState::Scheduled,
                tags: &[],
                unique_key: None,
                unique_nonce: None,
                unique_states: None,
            },
        )
        .await
        .unwrap()
        .job;

        let candidates = schedule_candidates(&mut connection, now, 10).await.unwrap();
        assert_eq!(candidates.len(), 1);
        let available = schedule_set_available(&mut connection, &[scheduled.id])
            .await
            .unwrap();
        assert_eq!(available[0].state, JobState::Available);

        let leader = leader_elect(&mut connection, "leader-1", now, Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert!(
            leader_elect(&mut connection, "leader-2", now, Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );
        // A same-ID lease from another term is neither renewed nor resigned.
        let other_term = leader.elected_at - TimeDelta::seconds(1);
        assert!(
            leader_reelect(
                &mut connection,
                "leader-1",
                other_term,
                now + TimeDelta::seconds(1),
                Duration::from_secs(30),
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            !leader_resign(&mut connection, "leader-1", other_term)
                .await
                .unwrap()
        );
        let renewed = leader_reelect(
            &mut connection,
            "leader-1",
            leader.elected_at,
            now + TimeDelta::seconds(1),
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(renewed.expires_at > leader.expires_at);
        assert!(
            leader_resign(&mut connection, "leader-1", renewed.elected_at)
                .await
                .unwrap()
        );

        let inserted = notification_insert(
            &mut connection,
            &[
                NotificationInput {
                    payload: "1",
                    topic: "insert_many",
                },
                NotificationInput {
                    payload: "default",
                    topic: "queue_pause",
                },
            ],
        )
        .await
        .unwrap();
        assert_eq!(inserted, 2);
        let notifications = notification_poll(&mut connection, 0, 10).await.unwrap();
        assert_eq!(notifications.len(), 2);
        assert_eq!(notifications[0].topic, "insert_many");
        assert_eq!(notification_last_id(&mut connection).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn notification_cleanup_is_bounded_and_preserves_recent_rows() {
        let pool = setup().await;
        let mut connection = pool.acquire().await.unwrap();
        let now = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        sqlx::query(
            "INSERT INTO river_notification (created_at, payload, topic) VALUES \
             (?, 'old-1', 'test'), (?, 'old-2', 'test'), (?, 'recent', 'test')",
        )
        .bind(sqlite_time(now - TimeDelta::minutes(6)))
        .bind(sqlite_time(
            now - TimeDelta::minutes(5) - TimeDelta::seconds(1),
        ))
        .bind(sqlite_time(now - TimeDelta::minutes(4)))
        .execute(&mut *connection)
        .await
        .unwrap();

        assert_eq!(
            notification_cleanup(&mut connection, now - TimeDelta::minutes(5), 1)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            notification_cleanup(&mut connection, now - TimeDelta::minutes(5), 10)
                .await
                .unwrap(),
            1
        );
        let remaining: Vec<String> =
            sqlx::query_scalar("SELECT payload FROM river_notification ORDER BY id")
                .fetch_all(&mut *connection)
                .await
                .unwrap();
        assert_eq!(remaining, ["recent"]);
    }

    #[tokio::test]
    async fn claim_caps_attempted_by_without_reversing_history() {
        let pool = setup().await;
        let mut connection = pool.acquire().await.unwrap();
        let now = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let inserted = insert(
            &mut connection,
            &InsertJob {
                attempt: 0,
                attempted_at: None,
                attempted_by: &[],
                created_at: now,
                encoded_args: &serde_json::value::to_raw_value(&json!({})).unwrap(),
                errors: &[],
                finalized_at: None,
                id: None,
                kind: "attempt_history",
                max_attempts: 25,
                metadata: &Map::new(),
                priority: 1,
                queue: "default",
                scheduled_at: now,
                state: JobState::Available,
                tags: &[],
                unique_key: None,
                unique_nonce: None,
                unique_states: None,
            },
        )
        .await
        .unwrap()
        .job;
        for client_id in ["one", "two", "three", "four", "five"] {
            let claimed = claim(
                &mut connection,
                &ClaimJobs {
                    client_id,
                    limit: 1,
                    max_attempted_by: 3,
                    now,
                    queue: "default",
                },
            )
            .await
            .unwrap();
            assert_eq!(claimed.len(), 1);
            sqlx::query("UPDATE river_job SET state = 'available' WHERE id = ?")
                .bind(inserted.id)
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        let row = get(&mut connection, inserted.id).await.unwrap().unwrap();
        assert_eq!(row.attempted_by, ["three", "four", "five"]);
    }

    /// Inserts a job for the undecodable row tests.
    async fn insert_test_job(
        connection: &mut SqliteConnection,
        state: JobState,
        now: DateTime<Utc>,
    ) -> JobRow {
        let running = state == JobState::Running;
        insert(
            connection,
            &InsertJob {
                attempt: i16::from(running),
                attempted_at: running.then_some(now - TimeDelta::hours(2)),
                attempted_by: &[],
                created_at: now,
                encoded_args: &serde_json::value::to_raw_value(&json!({})).unwrap(),
                errors: &[],
                finalized_at: None,
                id: None,
                kind: "undecodable",
                max_attempts: 25,
                metadata: &Map::new(),
                priority: 1,
                queue: "default",
                scheduled_at: now,
                state,
                tags: &["tag".to_owned()],
                unique_key: None,
                unique_nonce: None,
                unique_states: None,
            },
        )
        .await
        .unwrap()
        .job
    }

    /// Overwrites a JSON column the way a row changed out of band would be,
    /// into a shape River can't decode.
    async fn set_json_column(connection: &mut SqliteConnection, id: i64, column: &str, json: &str) {
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE river_job SET {column} = jsonb(?) WHERE id = ?"
        )))
        .bind(json)
        .bind(id)
        .execute(&mut *connection)
        .await
        .unwrap();
    }

    fn retryable_completion(id: i64, error: &AttemptError, now: DateTime<Utc>) -> CompleteJob<'_> {
        CompleteJob {
            attempt: None,
            error: Some(error),
            finalized_at: None,
            id,
            metadata_updates: None,
            now,
            scheduled_at: Some(now + TimeDelta::hours(1)),
            state: JobState::Retryable,
        }
    }

    // A claimed job whose row can't be decoded is returned separately with the
    // fields that could be decoded, without failing the others.
    #[tokio::test]
    async fn claim_returns_undecodable_rows_separately() {
        let pool = setup().await;
        let mut connection = pool.acquire().await.unwrap();
        let now = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let job1 = insert_test_job(&mut connection, JobState::Available, now).await;
        let job2 = insert_test_job(&mut connection, JobState::Available, now).await;
        let job3 = insert_test_job(&mut connection, JobState::Available, now).await;
        set_json_column(&mut connection, job2.id, "errors", r#"{"not":"an array"}"#).await;
        set_json_column(&mut connection, job2.id, "tags", r#"{"not":"an array"}"#).await;

        let claimed = claim(
            &mut connection,
            &ClaimJobs {
                client_id: "client",
                limit: 10,
                max_attempted_by: 100,
                now,
                queue: "default",
            },
        )
        .await
        .unwrap();
        assert_eq!(claimed.len(), 3);
        let decoded = claimed
            .iter()
            .filter_map(|job| job.as_ref().ok().map(|job| job.id))
            .collect::<Vec<_>>();
        assert_eq!(decoded, [job1.id, job3.id]);

        let undecodable = claimed
            .into_iter()
            .find_map(Result::err)
            .expect("undecodable job");
        assert!(
            undecodable.error.contains("error unmarshaling `errors`"),
            "{}",
            undecodable.error
        );
        assert!(
            undecodable.error.contains("error unmarshaling `tags`"),
            "{}",
            undecodable.error
        );
        let row = undecodable.row.expect("partially decoded row");
        assert_eq!(row.id, job2.id);
        assert_eq!(row.attempt, 1);
        assert_eq!(row.attempted_by, ["client"]);
        assert_eq!(row.kind, "undecodable");
        assert_eq!(row.state, JobState::Running);
        assert!(row.errors.is_empty());
        assert!(row.tags.is_empty());
    }

    // A job whose row can't be fully decoded still has its state set and is
    // returned with the fields that could be decoded, so it doesn't fail the
    // other jobs completed in the same transaction. The undecodable value is
    // left as it was.
    #[tokio::test]
    async fn complete_returns_undecodable_rows_partially() {
        let pool = setup().await;
        let mut connection = pool.acquire().await.unwrap();
        let now = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let job1 = insert_test_job(&mut connection, JobState::Running, now).await;
        let job2 = insert_test_job(&mut connection, JobState::Running, now).await;
        set_json_column(&mut connection, job2.id, "tags", r#"{"not":"an array"}"#).await;
        let error = AttemptError::new(now, 1, "fake error");

        let row1 = complete_decoded(&mut connection, &retryable_completion(job1.id, &error, now))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(row1.state, JobState::Retryable);
        let undecodable =
            complete_decoded(&mut connection, &retryable_completion(job2.id, &error, now))
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
        let row2 = undecodable.row.expect("partially decoded row");
        assert_eq!(row2.id, job2.id);
        assert_eq!(row2.state, JobState::Retryable);
        assert_eq!(row2.errors, [error]);
        assert!(row2.tags.is_empty());

        let strict = get(&mut connection, job2.id).await.unwrap_err();
        assert!(
            strict.to_string().contains("error unmarshaling `tags`"),
            "{strict}"
        );
        let tags: String = sqlx::query_scalar("SELECT json(tags) FROM river_job WHERE id = ?")
            .bind(job2.id)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(tags, r#"{"not":"an array"}"#);
    }

    // `errors` that isn't an array is wrapped in one so the new error can be
    // appended without losing the existing value, like River Go.
    #[tokio::test]
    async fn complete_wraps_a_non_array_errors_value() {
        let pool = setup().await;
        let mut connection = pool.acquire().await.unwrap();
        let now = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let job = insert_test_job(&mut connection, JobState::Running, now).await;
        set_json_column(
            &mut connection,
            job.id,
            "errors",
            r#"{"error":"existing value"}"#,
        )
        .await;
        let error = AttemptError::new(now, 1, "fake error");

        let row = complete_decoded(&mut connection, &retryable_completion(job.id, &error, now))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(row.state, JobState::Retryable);
        assert_eq!(
            row.errors
                .iter()
                .map(|error| error.error.as_str())
                .collect::<Vec<_>>(),
            ["existing value", "fake error"]
        );
    }

    // A stuck job whose row can't be fully decoded is still returned so that
    // it can be rescued.
    #[tokio::test]
    async fn stuck_jobs_include_undecodable_rows() {
        let pool = setup().await;
        let mut connection = pool.acquire().await.unwrap();
        let now = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let job1 = insert_test_job(&mut connection, JobState::Running, now).await;
        let job2 = insert_test_job(&mut connection, JobState::Running, now).await;
        set_json_column(&mut connection, job1.id, "tags", r#"{"not":"an array"}"#).await;

        let stuck = stuck_jobs(&mut connection, 0, now, 10).await.unwrap();
        assert_eq!(
            stuck.iter().map(|job| job.id).collect::<Vec<_>>(),
            [job1.id, job2.id]
        );
        assert!(stuck[0].tags.is_empty());
        assert_eq!(stuck[1].tags, ["tag"]);
    }

    #[tokio::test]
    async fn late_completion_merges_metadata_without_changing_terminal_state() {
        let pool = setup().await;
        let mut connection = pool.acquire().await.unwrap();
        let now = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let inserted = insert(
            &mut connection,
            &InsertJob {
                attempt: 1,
                attempted_at: Some(now),
                attempted_by: &["client".to_owned()],
                created_at: now,
                encoded_args: &serde_json::value::to_raw_value(&json!({})).unwrap(),
                errors: &[],
                finalized_at: None,
                id: None,
                kind: "late_completion",
                max_attempts: 25,
                metadata: &Map::from_iter([("winner".to_owned(), json!(true))]),
                priority: 1,
                queue: "default",
                scheduled_at: now,
                state: JobState::Running,
                tags: &[],
                unique_key: None,
                unique_nonce: None,
                unique_states: None,
            },
        )
        .await
        .unwrap()
        .job;
        for terminal in [JobState::Completed, JobState::Discarded] {
            sqlx::query("UPDATE river_job SET state = ?, metadata = jsonb('{\"winner\":true}') WHERE id = ?")
                .bind(terminal.as_str())
                .bind(inserted.id)
                .execute(&mut *connection)
                .await
                .unwrap();
            let completion = complete(
                &mut connection,
                &CompleteJob {
                    attempt: Some(1),
                    error: None,
                    finalized_at: Some(now),
                    id: inserted.id,
                    metadata_updates: Some(&Map::from_iter([("stale".to_owned(), json!(true))])),
                    now,
                    scheduled_at: None,
                    state: JobState::Completed,
                },
            )
            .await
            .unwrap();
            assert!(completion.is_none());
            let completion = merge_metadata_if_not_running(
                &mut connection,
                inserted.id,
                &Map::from_iter([("stale".to_owned(), json!(true))]),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();
            assert_eq!(completion.state, terminal);
            let row = get(&mut connection, inserted.id).await.unwrap().unwrap();
            assert_eq!(row.state, terminal);
            assert_eq!(row.metadata["winner"], true);
            assert_eq!(row.metadata["stale"], true);
        }
    }

    #[test]
    fn wire_encoders_match_go_sqlite() {
        let base = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        assert_eq!(
            sqlite_time(base.with_nanosecond(123_400_000).unwrap()),
            "2026-01-02 03:04:05.123"
        );
        assert_eq!(
            sqlite_time(base.with_nanosecond(123_800_000).unwrap()),
            "2026-01-02 03:04:05.124"
        );
        assert_eq!(
            sqlite_time(base.with_nanosecond(999_800_000).unwrap()),
            "2026-01-02 03:04:06.000"
        );
        assert_eq!(sqlite_ttl(Duration::from_millis(3_255)), "3.255 seconds");
    }
}
