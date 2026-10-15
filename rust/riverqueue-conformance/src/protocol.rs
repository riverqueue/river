//! The contract's messages, mirroring the Go types in `conformance/protocol`,
//! which define it.

use chrono::{DateTime, Utc};
use riverqueue::{AttemptError, JobRow, JobState};
use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};

pub const KIND_ECHO: &str = "conformance_echo";
pub const KIND_ECHO_PEER: &str = "conformance_echo_peer";
pub const KIND_ECHO_RENAMED: &str = "conformance_echo_renamed";
pub const PERIODIC_JOB_ID: &str = "conformance-periodic";
pub const PERIODIC_MARKER_JOB_ID: &str = "conformance-periodic-marker";

/// A JSON-RPC 2.0 request.
#[derive(Debug, Deserialize)]
pub struct Request {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub jsonrpc: String,
    #[serde(default)]
    pub method: String,
    pub params: Option<Box<RawValue>>,
}

/// A JSON-RPC 2.0 response.
#[derive(Debug, Serialize)]
pub struct Response {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
    pub id: i64,
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Box<RawValue>>,
}

/// A JSON-RPC 2.0 error, with the contract's codes.
#[derive(Debug, Serialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

impl RpcError {
    pub const PARSE_ERROR: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL: i32 = -32603;
    pub const NOT_FOUND: i32 = -32001;
    pub const REJECTED: i32 = -32002;

    #[allow(
        clippy::needless_pass_by_value,
        reason = "errors are passed by value, as `map_err` hands them over"
    )]
    pub fn new(code: i32, message: impl ToString) -> Self {
        Self {
            code,
            message: message.to_string(),
        }
    }

    pub fn invalid_params(message: impl ToString) -> Self {
        Self::new(Self::INVALID_PARAMS, message)
    }

    pub fn not_found(message: impl ToString) -> Self {
        Self::new(Self::NOT_FOUND, message)
    }

    pub fn rejected(message: impl ToString) -> Self {
        Self::new(Self::REJECTED, message)
    }
}

/// River's errors reject the request, except those naming a missing job or
/// queue.
impl From<riverqueue::Error> for RpcError {
    fn from(error: riverqueue::Error) -> Self {
        let code = match error {
            riverqueue::Error::NotFound(_) => Self::NOT_FOUND,
            _ => Self::REJECTED,
        };
        Self::new(code, riverqueue::__private::error_chain(&error))
    }
}

/// Declares params, whose fields are all optional and take their zero values,
/// which mean River's defaults, and which reject unknown fields.
macro_rules! params {
    ($($(#[$attr:meta])* $name:ident { $($field:ident: $type:ty),* $(,)? })*) => {$(
        $(#[$attr])*
        #[derive(Debug, Default, Deserialize)]
        #[serde(default, deny_unknown_fields)]
        pub struct $name { $(pub $field: $type),* }
    )*};
}

params! {
    /// Params of the methods that take none.
    Empty {}
    InsertParams { jobs: Vec<InsertJob>, schema: String, tx: String }
    /// One job to insert: the echo job's args, which Go embeds, and options.
    InsertJob { behavior: String, duration_ms: i64, message: String, opts: Option<InsertOpts> }
    InsertOpts {
        max_attempts: i64,
        metadata: Option<Box<RawValue>>,
        pending: bool,
        priority: i64,
        queue: String,
        scheduled_at: Option<DateTime<Utc>>,
        tags: Vec<String>,
        unique: Option<UniqueOpts>,
    }
    UniqueOpts {
        by_args: bool,
        by_period_ms: u64,
        by_queue: bool,
        by_state: Vec<String>,
        exclude_kind: bool,
    }
    JobParams { id: i64, schema: String, tx: String }
    ListParams {
        after: String,
        direction: String,
        ids: Vec<i64>,
        kinds: Vec<String>,
        limit: u32,
        metadata: Option<Value>,
        order_by: String,
        priorities: Vec<i16>,
        queues: Vec<String>,
        schema: String,
        states: Vec<String>,
        tags_all: Vec<String>,
        tx: String,
    }
    MigrateParams { direction: String, schema: String, target_version: Option<i64> }
    QueueParams { action: String, metadata: Option<Value>, name: String, schema: String, tx: String }
    ReleaseParams { name: String }
    RequestResignParams { schema: String, tx: String }
    #[allow(clippy::struct_excessive_bools, reason = "the contract's independent options")]
    StartParams {
        claim_barrier: String,
        client_id: String,
        error_handler_cancel: bool,
        fetch_only_known_kinds: bool,
        fetch_poll_interval_ms: u64,
        job_timeout_ms: i64,
        leader_election_disabled: bool,
        max_workers: usize,
        periodic_run_on_start: bool,
        periodic_unique: bool,
        poll_only: bool,
        queues: Vec<String>,
        rescue_after_ms: u64,
        retry_delay_ms: u64,
        schema: String,
        tuning: Option<Tuning>,
        worker_kinds: Vec<String>,
    }
    #[allow(clippy::struct_field_names, reason = "the contract's field names")]
    Tuning { elect_interval_ms: u64, rescuer_interval_ms: u64, scheduler_interval_ms: u64 }
    StopParams { cancel: bool }
    TxParams { tx: String }
    TxEndParams { commit: bool, tx: String }
}

#[derive(Debug, Serialize)]
pub struct HandshakeResult {
    pub driver: &'static str,
    pub implementation: &'static str,
    pub version: &'static str,
}

#[derive(Debug, Serialize)]
pub struct InsertResult {
    pub results: Vec<JobInsertResult>,
}

#[derive(Debug, Serialize)]
pub struct JobInsertResult {
    pub job: Job,
    pub unique_skipped_as_duplicate: bool,
}

#[derive(Debug, Serialize)]
pub struct ListResult {
    pub cursor: Option<String>,
    pub jobs: Vec<Job>,
}

#[derive(Debug, Serialize)]
pub struct MigrateResult {
    pub versions: Vec<i64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StatsResult {
    pub cancelled_at_start: u64,
    pub error_handler_calls: u64,
    pub events: Vec<&'static str>,
    pub periodic_starts: u64,
}

/// A job as the contract reports it: every column, `river:unique_nonce` left
/// out of metadata, the unique key in hex, and unique states sorted. Args and
/// metadata are reported as stored, so numbers no float can hold keep their
/// exact text.
#[derive(Debug, Serialize)]
pub struct Job {
    args: Box<RawValue>,
    attempt: i32,
    attempted_at: Option<DateTime<Utc>>,
    attempted_by: Vec<String>,
    created_at: DateTime<Utc>,
    errors: Vec<AttemptError>,
    finalized_at: Option<DateTime<Utc>>,
    id: i64,
    kind: String,
    max_attempts: i32,
    metadata: Box<RawValue>,
    priority: i16,
    queue: String,
    scheduled_at: DateTime<Utc>,
    state: &'static str,
    tags: Vec<String>,
    unique_key: Option<String>,
    unique_states: Option<Vec<&'static str>>,
}

impl From<JobRow> for Job {
    fn from(row: JobRow) -> Self {
        let mut metadata = row.metadata;
        metadata.remove(riverqueue::METADATA_KEY_UNIQUE_NONCE);
        let unique_states = row.unique_states.map(|states| {
            let mut names: Vec<_> = states.into_iter().map(JobState::as_str).collect();
            names.sort_unstable();
            names
        });
        Self {
            args: row.encoded_args,
            attempt: row.attempt,
            attempted_at: row.attempted_at,
            attempted_by: row.attempted_by,
            created_at: row.created_at,
            errors: row.errors,
            finalized_at: row.finalized_at,
            id: row.id,
            kind: row.kind,
            max_attempts: row.max_attempts,
            metadata: metadata.into_raw(),
            priority: row.priority,
            queue: row.queue,
            scheduled_at: row.scheduled_at,
            state: row.state.as_str(),
            tags: row.tags,
            unique_key: row.unique_key.map(|key| {
                key.iter()
                    .fold(String::new(), |hex, byte| hex + &format!("{byte:02x}"))
            }),
            unique_states,
        }
    }
}
