//! Newline-delimited JSON-RPC adapter for River's black-box conformance suite.

#![forbid(unsafe_code)]

use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use riverqueue::database::SchemaName;
use riverqueue::{
    AttemptError, BoxError, Client, CronSchedule, DefaultRetryPolicy, ErrorHandler,
    ErrorHandlerDecision, EventKind, EventReceiver, Extensions, Hook, InsertContext,
    InsertMiddleware, InsertNext, InsertOpts, InsertResult, InsertedJobs, IntervalSchedule, Job,
    JobArgs, JobDeleteManyParams, JobListCursor, JobListParams, JobListResult, JobRow, JobState,
    JobUpdateParams, MaintenanceConfig, PeriodicJob, PeriodicJobOpts, PeriodicJobs, Plugin, Queue,
    QueueConfig, QueueListParams, RetryPolicy, RunHandle, SortDirection, SubscribeConfig,
    UniqueOpts, WorkCancelled, WorkContext, WorkMiddleware, WorkOutcome, WorkResult, Worker,
    WorkerRegistry,
    database::{PostgresDatabase, PostgresReindexConfig, PostgresReindexSchedule, SqliteDatabase},
    encoding::encode_args,
    protocol::{UniqueKeyInput, unique_key, unique_states_bitmask},
};
use riverqueue_migrate::{
    Direction, MIGRATION_LINE_MAIN, MIGRATION_VERSION_LATEST, MigrateOpts, PostgresMigrator,
    SqliteMigrator,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json, value::RawValue};
use sqlx::{
    AssertSqlSafe, PgPool, Postgres, Sqlite, SqlitePool, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions},
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use tokio::sync::watch;

const ADAPTER_VERSION: u32 = 13;
const PROTOCOL_REVISION: u32 = 1;

const ADAPTER_METHODS: &[&str] = &[
    "barrier_create",
    "barrier_release",
    "benchmark_enqueue",
    "cancel",
    "clock_set",
    "connection_count",
    "cron_next",
    "delete",
    "delete_many",
    "fault_disconnect_application",
    "fault_disconnect_listeners",
    "fault_expire_leader",
    "get",
    "handshake",
    "insert",
    "insert_many",
    "insert_many_fast",
    "leader",
    "list",
    "listener_count",
    "migrate",
    "queue_add",
    "queue_get",
    "queue_list",
    "queue_pause",
    "queue_remove",
    "queue_resume",
    "queue_update",
    "raw_finalize",
    "raw_insert_exact_json",
    "raw_insert_full_row",
    "raw_insert_no_notify",
    "raw_job_exact_json",
    "raw_job_timestamps",
    "request_resign",
    "reset",
    "retry",
    "retry_delay",
    "rng_seed",
    "runtime_stats",
    "start",
    "stop",
    "tx_begin",
    "tx_cancel",
    "tx_commit",
    "tx_delete",
    "tx_delete_many",
    "tx_fail",
    "tx_get",
    "tx_insert",
    "tx_insert_many",
    "tx_insert_many_fast",
    "tx_list",
    "tx_queue_get",
    "tx_queue_list",
    "tx_queue_pause",
    "tx_queue_resume",
    "tx_queue_update",
    "tx_retry",
    "tx_rollback",
    "tx_update",
    "unique_key",
    "update",
    "wait",
    "work",
];

const INSERT_ONLY_CAPABILITIES: &[&str] = &["insert", "lifecycle", "transactions", "unique_jobs"];

const INSERT_ONLY_METHODS: &[&str] = &[
    "handshake",
    "insert",
    "insert_many",
    "tx_begin",
    "tx_commit",
    "tx_insert",
    "tx_insert_many",
    "tx_rollback",
    "unique_key",
];

const CAPABILITIES: &[&str] = &[
    "barriers",
    "cancel",
    "custom_schema",
    "deterministic_controls",
    "extensions",
    "fast_insert",
    "fault_injection",
    "get",
    "insert",
    "job_crud",
    "leadership",
    "lifecycle",
    "maintenance",
    "migrate",
    "notifications",
    "periodic_jobs",
    "poll_only",
    "queues",
    "reset",
    "resumable_jobs",
    "retry",
    "scheduler",
    "subscriptions",
    "transactions",
    "unique_jobs",
    "work",
];

const SQLITE_ADAPTER_METHODS: &[&str] = &[
    "cancel",
    "clock_set",
    "cron_next",
    "delete",
    "delete_many",
    "get",
    "handshake",
    "insert",
    "insert_many",
    "insert_many_fast",
    "list",
    "migrate",
    "raw_insert_exact_json",
    "raw_job_exact_json",
    "raw_job_timestamps",
    "reset",
    "retry",
    "retry_delay",
    "rng_seed",
    "tx_begin",
    "tx_cancel",
    "tx_commit",
    "tx_delete",
    "tx_delete_many",
    "tx_get",
    "tx_insert",
    "tx_insert_many",
    "tx_insert_many_fast",
    "tx_list",
    "tx_retry",
    "tx_rollback",
    "tx_update",
    "unique_key",
    "update",
];

const SQLITE_CAPABILITIES: &[&str] = &[
    "cancel",
    "deterministic_controls",
    "fast_insert",
    "get",
    "insert",
    "job_crud",
    "lifecycle",
    "migrate",
    "reset",
    "retry",
    "transactions",
    "unique_jobs",
];

const SQLITE_RUNTIME_METHODS: &[&str] = &[
    "barrier_create",
    "barrier_release",
    "cancel",
    "clock_set",
    "cron_next",
    "delete",
    "delete_many",
    "get",
    "handshake",
    "insert",
    "insert_many",
    "insert_many_fast",
    "leader",
    "list",
    "migrate",
    "queue_add",
    "queue_get",
    "queue_list",
    "queue_pause",
    "queue_remove",
    "queue_resume",
    "queue_update",
    "raw_finalize",
    "raw_insert_exact_json",
    "raw_insert_no_notify",
    "raw_job_exact_json",
    "raw_job_timestamps",
    "request_resign",
    "reset",
    "retry",
    "retry_delay",
    "rng_seed",
    "runtime_stats",
    "start",
    "stop",
    "tx_begin",
    "tx_cancel",
    "tx_commit",
    "tx_delete",
    "tx_delete_many",
    "tx_get",
    "tx_insert",
    "tx_insert_many",
    "tx_insert_many_fast",
    "tx_list",
    "tx_queue_get",
    "tx_queue_list",
    "tx_queue_pause",
    "tx_queue_resume",
    "tx_queue_update",
    "tx_retry",
    "tx_rollback",
    "tx_update",
    "unique_key",
    "update",
    "wait",
    "work",
];

const SQLITE_RUNTIME_CAPABILITIES: &[&str] = &[
    "barriers",
    "cancel",
    "deterministic_controls",
    "extensions",
    "fast_insert",
    "get",
    "insert",
    "job_crud",
    "leadership",
    "lifecycle",
    "migrate",
    "notifications",
    "periodic_jobs",
    "poll_only",
    "queues",
    "reset",
    "resumable_jobs",
    "retry",
    "scheduler",
    "subscriptions",
    "transactions",
    "unique_jobs",
    "work",
];

/// Stable JSON-RPC error codes from `conformance/adapter/contract.json`.
mod error_code {
    pub const DATABASE: i32 = -32_003;
    pub const INVALID_PARAMS: i32 = -32_602;
    pub const INVALID_REQUEST: i32 = -32_600;
    pub const METHOD_NOT_FOUND: i32 = -32_601;
    pub const NOT_FOUND: i32 = -32_001;
    pub const PARSE: i32 = -32_700;
    pub const REJECTED: i32 = -32_002;
    pub const UNSUPPORTED: i32 = -32_004;
}

/// A failure the adapter classifies with a contract error code itself.
#[derive(Debug)]
struct AdapterError {
    code: i32,
    message: String,
}

impl AdapterError {
    fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: error_code::INVALID_PARAMS,
            message: message.into(),
        }
    }

    fn method_not_found(method: &str) -> Self {
        Self {
            code: error_code::METHOD_NOT_FOUND,
            message: format!("method not found: {method}"),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: error_code::NOT_FOUND,
            message: message.into(),
        }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            code: error_code::UNSUPPORTED,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AdapterError {}

/// Maps a failure to its contract error code. River reports missing rows as
/// `Error::NotFound` and database failures as `Error::Database`; every other
/// River failure is a rejection of the request.
fn error_code(error: &(dyn std::error::Error + Send + Sync + 'static)) -> i32 {
    if let Some(error) = error.downcast_ref::<AdapterError>() {
        return error.code;
    }
    if let Some(error) = error.downcast_ref::<riverqueue::Error>() {
        return match error {
            riverqueue::Error::NotFound => error_code::NOT_FOUND,
            riverqueue::Error::Database(_) => error_code::DATABASE,
            _ => error_code::REJECTED,
        };
    }
    if error.downcast_ref::<sqlx::Error>().is_some() {
        return error_code::DATABASE;
    }
    if let Some(error) = error.downcast_ref::<io::Error>()
        && error.kind() == io::ErrorKind::NotFound
    {
        return error_code::NOT_FOUND;
    }
    error_code::REJECTED
}

/// Parameter schemas from the adapter contract, used to reject parameters
/// the contract does not define instead of silently ignoring them.
struct ContractParams {
    definitions: Map<String, Value>,
    methods: HashMap<String, Value>,
}

impl ContractParams {
    fn load() -> Result<Self, serde_json::Error> {
        let contract: Value =
            serde_json::from_str(include_str!("../../../conformance/adapter/contract.json"))?;
        let definitions = contract["$defs"].as_object().cloned().unwrap_or_default();
        let methods = contract["methods"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|method| {
                Some((
                    method["name"].as_str()?.to_owned(),
                    method["params"].clone(),
                ))
            })
            .collect();
        Ok(Self {
            definitions,
            methods,
        })
    }

    /// Rejects object keys that a closed contract schema does not declare.
    fn check(&self, method: &str, params: &Value) -> Result<(), AdapterError> {
        match self.methods.get(method) {
            Some(schema) => self.check_value(schema, params, "params"),
            None => Ok(()),
        }
    }

    fn check_value(
        &self,
        schema: &Value,
        value: &Value,
        location: &str,
    ) -> Result<(), AdapterError> {
        if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
            return match reference
                .strip_prefix("#/$defs/")
                .and_then(|name| self.definitions.get(name))
            {
                Some(definition) => self.check_value(definition, value, location),
                // References to other schema files describe results, which
                // the adapter produces rather than receives.
                None => Ok(()),
            };
        }
        match value {
            Value::Object(object) => {
                let properties = schema.get("properties").and_then(Value::as_object);
                for (key, child) in object {
                    match properties.and_then(|properties| properties.get(key)) {
                        Some(child_schema) => {
                            self.check_value(child_schema, child, &format!("{location}.{key}"))?;
                        }
                        None if schema.get("additionalProperties") == Some(&Value::Bool(false)) => {
                            return Err(AdapterError::invalid_params(format!(
                                "unknown parameter {location}.{key}"
                            )));
                        }
                        None => {}
                    }
                }
                Ok(())
            }
            Value::Array(items) => match schema.get("items") {
                Some(item_schema) => items.iter().enumerate().try_for_each(|(index, item)| {
                    self.check_value(item_schema, item, &format!("{location}[{index}]"))
                }),
                None => Ok(()),
            },
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct Request {
    id: Value,
    jsonrpc: String,
    method: String,
    #[serde(default)]
    params: Option<Box<RawValue>>,
}

#[derive(Serialize)]
struct Response {
    id: Value,
    jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ResponseError>,
}

#[derive(Serialize)]
struct ResponseError {
    code: i32,
    message: String,
}

#[derive(Deserialize)]
struct UniqueKeyParams {
    /// Exact argument bytes; unique keys hash them without reinterpretation.
    args: Box<RawValue>,
    kind: String,
    now: DateTime<Utc>,
    options: UniqueKeyOptions,
    queue: String,
    scheduled_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize)]
struct UniqueKeyOptions {
    by_args: bool,
    by_period_nanos: u64,
    by_queue: bool,
    by_state: Option<Vec<JobState>>,
    exclude_kind: bool,
}

impl UniqueKeyOptions {
    fn to_unique_opts(&self) -> UniqueOpts {
        build_unique_opts(
            self.by_args,
            (self.by_period_nanos > 0).then(|| Duration::from_nanos(self.by_period_nanos)),
            self.by_queue,
            self.by_state.clone(),
            self.exclude_kind,
        )
    }
}

/// Answers `unique_key` from the raw request so fixture arguments are hashed
/// byte for byte, including number tokens such as `-0` and `1e+100`.
fn respond_unique_key(request: &Request) -> Response {
    let result = request
        .params
        .as_deref()
        .ok_or_else(|| "unique_key requires params".to_owned())
        .and_then(|params| {
            serde_json::from_str::<UniqueKeyParams>(params.get()).map_err(|error| error.to_string())
        })
        .and_then(|params| {
            let opts = params.options.to_unique_opts();
            let key = match params.kind.as_str() {
                "conformance_all_args" => unique_key_for_args::<UniqueAllArgs>(&params, &opts),
                "conformance_numeric_boundaries" => {
                    unique_key_for_args::<UniqueNumericArgs>(&params, &opts)
                }
                "conformance_selected_args" => {
                    unique_key_for_args::<UniqueSelectedArgs>(&params, &opts)
                }
                "conformance_simple" => unique_key_for_args::<UniqueSimpleArgs>(&params, &opts),
                kind => Err(format!("unsupported unique fixture kind {kind:?}")),
            }?;
            Ok(json!({"sha256": hex(&key), "state_mask": unique_states_bitmask(&opts)}))
        });
    match result {
        Ok(result) => Response::success(request.id.clone(), result),
        Err(error) => Response::error(request.id.clone(), -32_000, error),
    }
}

fn unique_key_for_args<A>(params: &UniqueKeyParams, opts: &UniqueOpts) -> Result<[u8; 32], String>
where
    A: JobArgs + serde::de::DeserializeOwned,
{
    // Decode to confirm the fixture matches the job type, as River Go does
    // when it resolves unique struct tags.
    serde_json::from_str::<A>(params.args.get()).map_err(|error| error.to_string())?;
    unique_key(&UniqueKeyInput {
        encoded_args: &params.args,
        kind: A::KIND,
        now: params.now,
        opts,
        queue: &params.queue,
        scheduled_at: params.scheduled_at,
        unique_fields: A::unique_fields(),
    })
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "unique fixture options produced no key".to_owned())
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "conformance_echo")]
struct ConformanceArgs {
    #[serde(default)]
    behavior: String,
    #[serde(default)]
    duration_ms: u64,
    message: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(transparent)]
struct UniqueAllArgs(Value);

impl JobArgs for UniqueAllArgs {
    const KIND: &'static str = "conformance_all_args";
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(transparent)]
struct UniqueNumericArgs(Value);

impl JobArgs for UniqueNumericArgs {
    const KIND: &'static str = "conformance_numeric_boundaries";
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct UniqueSelectedAccount {
    id: String,
    ignored: String,
}

#[derive(Debug, Deserialize, JobArgs, Serialize)]
#[river(
    kind = "conformance_selected_args",
    unique(by_args("account.id", "account.region", "label", "path/key"))
)]
struct UniqueSelectedArgs {
    #[serde(default)]
    account: UniqueSelectedAccount,
    #[serde(default)]
    ignored: bool,
    #[serde(default)]
    label: Option<String>,
    #[serde(default, rename = "path/key")]
    path_key: String,
}

#[derive(Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "conformance_simple")]
struct UniqueSimpleArgs {
    id: i64,
}

struct ConformanceWorker {
    barriers: Arc<BarrierRegistry>,
    pool: Option<PgPool>,
    probe: Arc<RuntimeProbe>,
}

async fn work_resumable_cursor(context: &WorkContext, attempt: i16) {
    // Intentionally suppress errors: attempt finalization must still retain
    // the failed step and its cursor, like Go's resumable coordinator.
    let _ = context
        .resumable_step("first", || async {
            context
                .metadata_set("first_attempt", attempt)
                .map_err(io::Error::other)
        })
        .await;
    let _ = context
        .resumable_step_with_cursor("second", |cursor: i64| async move {
            if attempt == 1 {
                context.resumable_set_cursor(&7).map_err(io::Error::other)?;
                return Err(io::Error::other("retry with cursor"));
            }
            if cursor != 7 {
                return Err(io::Error::other(format!("expected cursor 7, got {cursor}")));
            }
            context
                .metadata_set("cursor_observed", cursor)
                .map_err(io::Error::other)
        })
        .await;
    let _ = context
        .resumable_step("third", || async {
            if attempt == 2 {
                Err(io::Error::other("retry after consuming cursor"))
            } else {
                Ok(())
            }
        })
        .await;
}

#[allow(clippy::match_same_arms)]
impl Worker<ConformanceArgs> for ConformanceWorker {
    type Error = io::Error;

    #[allow(
        clippy::too_many_lines,
        reason = "one match maps every shared conformance behavior"
    )]
    async fn work(
        &self,
        context: WorkContext,
        job: Job<ConformanceArgs>,
    ) -> Result<WorkOutcome, Self::Error> {
        match job.args.behavior.as_str() {
            "barrier_output" | "barrier_wait" => {
                self.barriers.wait(&job.args.message).await?;
                if job.args.behavior == "barrier_output" {
                    context
                        .record_output(json!({"race": "worker"}))
                        .map_err(io::Error::other)?;
                }
                Ok(WorkOutcome::Complete)
            }
            "cancel" => Ok(WorkOutcome::Cancel),
            "cancel_error" => {
                context.cancellation_token().cancelled().await;
                Err(io::Error::other("conformance failure after cancellation"))
            }
            "cancel_panic" => {
                context.cancellation_token().cancelled().await;
                panic!("conformance panic after cancellation")
            }
            "cooperative_cancel" => {
                context.cancellation_token().cancelled().await;
                Err(io::Error::other(WorkCancelled))
            }
            "discard" => Ok(WorkOutcome::Discard),
            "error" => Err(io::Error::other("conformance retryable error")),
            "ignored_cancel" => std::future::pending().await,
            "output" => {
                context
                    .record_output(json!({"message": job.args.message}))
                    .map_err(io::Error::other)?;
                Ok(WorkOutcome::Complete)
            }
            "panic" => panic!("conformance worker panic"),
            "sleep" => {
                tokio::time::sleep(Duration::from_millis(job.args.duration_ms)).await;
                Ok(WorkOutcome::Complete)
            }
            "snooze_once" | "snooze_then_cancel" if !job.row.metadata.contains_key("snoozes") => {
                Ok(WorkOutcome::Snooze(Duration::from_millis(
                    job.args.duration_ms.max(1),
                )))
            }
            "snooze_then_cancel" => {
                context.cancellation_token().cancelled().await;
                Err(io::Error::other(WorkCancelled))
            }
            "resumable_cursor" => {
                work_resumable_cursor(&context, job.row.attempt).await;
                Ok(WorkOutcome::Complete)
            }
            "resumable" | "resumable_duplicate" => {
                let first_probe = Arc::clone(&self.probe);
                context
                    .resumable_step("first", move || async move {
                        first_probe.increment_resumable_first()?;
                        Ok::<_, io::Error>(())
                    })
                    .await
                    .map_err(io::Error::other)?;
                let second_probe = Arc::clone(&self.probe);
                context
                    .resumable_step(
                        if job.args.behavior == "resumable_duplicate" {
                            "first"
                        } else {
                            "second"
                        },
                        move || async move {
                            second_probe.increment_resumable_second()?;
                            if job.row.attempt == 1 {
                                Err(io::Error::other("fail second resumable step once"))
                            } else {
                                Ok(())
                            }
                        },
                    )
                    .await
                    .map_err(io::Error::other)?;
                Ok(WorkOutcome::Complete)
            }
            "transactional_complete" => {
                context
                    .metadata_set("transactional_completion", true)
                    .map_err(io::Error::other)?;
                let pool = self.pool.as_ref().ok_or_else(|| {
                    io::Error::other("transactional completion requires PostgreSQL")
                })?;
                let mut transaction = pool.begin().await.map_err(io::Error::other)?;
                context
                    .job_complete_tx(&mut transaction)
                    .await
                    .map_err(io::Error::other)?;
                transaction.commit().await.map_err(io::Error::other)?;
                Ok(WorkOutcome::Complete)
            }
            _ => Ok(WorkOutcome::Complete),
        }
    }
}

#[derive(Default)]
struct RuntimeProbe {
    state: Mutex<RuntimeProbeState>,
}

#[derive(Default)]
struct RuntimeProbeState {
    error_handler_calls: usize,
    events: Vec<String>,
    periodic_starts: usize,
    resumable_first_runs: usize,
    resumable_second_runs: usize,
    stuck_jobs: usize,
    trace: Vec<String>,
}

impl RuntimeProbe {
    fn add_event(&self, kind: EventKind) -> io::Result<()> {
        self.state
            .lock()
            .map_err(|_| io::Error::other("runtime probe lock poisoned"))?
            .events
            .push(event_kind_name(kind).to_owned());
        Ok(())
    }

    fn add_trace(&self, entry: &str) -> io::Result<()> {
        self.state
            .lock()
            .map_err(|_| io::Error::other("runtime probe lock poisoned"))?
            .trace
            .push(entry.to_owned());
        Ok(())
    }

    fn increment_periodic_starts(&self) -> io::Result<()> {
        self.state
            .lock()
            .map_err(|_| io::Error::other("runtime probe lock poisoned"))?
            .periodic_starts += 1;
        Ok(())
    }

    fn increment_error_handler_calls(&self) -> io::Result<()> {
        self.state
            .lock()
            .map_err(|_| io::Error::other("runtime probe lock poisoned"))?
            .error_handler_calls += 1;
        Ok(())
    }

    fn increment_resumable_first(&self) -> io::Result<()> {
        self.state
            .lock()
            .map_err(|_| io::Error::other("runtime probe lock poisoned"))?
            .resumable_first_runs += 1;
        Ok(())
    }

    fn increment_resumable_second(&self) -> io::Result<()> {
        self.state
            .lock()
            .map_err(|_| io::Error::other("runtime probe lock poisoned"))?
            .resumable_second_runs += 1;
        Ok(())
    }

    fn increment_stuck_jobs(&self) -> io::Result<()> {
        self.state
            .lock()
            .map_err(|_| io::Error::other("runtime probe lock poisoned"))?
            .stuck_jobs += 1;
        Ok(())
    }

    fn snapshot(&self) -> io::Result<Value> {
        let state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("runtime probe lock poisoned"))?;
        Ok(json!({
            "error_handler_calls": state.error_handler_calls,
            "events": state.events,
            "periodic_starts": state.periodic_starts,
            "resumable_first_runs": state.resumable_first_runs,
            "resumable_second_runs": state.resumable_second_runs,
            "stuck_jobs": state.stuck_jobs,
            "trace": state.trace,
        }))
    }
}

/// Records stuck jobs and, when `cancel` is set, counts worker errors and
/// cancels the failed job.
struct ConformanceErrorHandler {
    cancel: bool,
    probe: Arc<RuntimeProbe>,
}

#[allow(
    clippy::unused_async_trait_impl,
    reason = "these extensions only record state synchronously"
)]
impl ErrorHandler for ConformanceErrorHandler {
    async fn handle_error(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        _result: &WorkResult,
    ) -> Result<ErrorHandlerDecision, BoxError> {
        if !self.cancel {
            return Ok(ErrorHandlerDecision::Continue);
        }
        self.probe
            .increment_error_handler_calls()
            .map_err(|error| BoxError::from(error.to_string()))?;
        Ok(ErrorHandlerDecision::Cancel)
    }

    async fn handle_stuck(&self, _job: &JobRow) -> Result<(), BoxError> {
        self.probe
            .increment_stuck_jobs()
            .map_err(|error| BoxError::from(error.to_string()))
    }
}

struct ProbeHook(Arc<RuntimeProbe>);

#[allow(
    clippy::unused_async_trait_impl,
    reason = "these extensions only record state synchronously"
)]
impl Hook for ProbeHook {
    async fn insert_begin(&self, _insert: &mut InsertContext) -> Result<(), BoxError> {
        self.0
            .add_trace("hook:insert_begin")
            .map_err(|error| BoxError::from(error.to_string()))
    }

    async fn periodic_jobs_start(&self, _jobs: &PeriodicJobs) -> Result<(), BoxError> {
        self.0
            .increment_periodic_starts()
            .map_err(|error| BoxError::from(error.to_string()))?;
        self.0
            .add_trace("hook:periodic_start")
            .map_err(|error| BoxError::from(error.to_string()))
    }

    async fn work_begin(&self, _context: &WorkContext, _job: &mut JobRow) -> Result<(), BoxError> {
        self.0
            .add_trace("hook:work_begin")
            .map_err(|error| BoxError::from(error.to_string()))
    }

    async fn work_end(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        _result: &WorkResult,
    ) -> Result<(), BoxError> {
        self.0
            .add_trace("hook:work_end")
            .map_err(|error| BoxError::from(error.to_string()))
    }
}

struct ProbeInsertMiddleware(Arc<RuntimeProbe>);

impl InsertMiddleware for ProbeInsertMiddleware {
    async fn insert_many(
        &self,
        jobs: Vec<InsertContext>,
        next: InsertNext<'_>,
    ) -> Result<InsertedJobs, riverqueue::Error> {
        self.0
            .add_trace("middleware:insert_before")
            .map_err(|error| riverqueue::Error::runtime(error.to_string()))?;
        let inserted = next.run(jobs).await;
        self.0
            .add_trace("middleware:insert_after")
            .map_err(|error| riverqueue::Error::runtime(error.to_string()))?;
        inserted
    }
}

struct ProbeWorkMiddleware(Arc<RuntimeProbe>);

#[async_trait]
impl WorkMiddleware for ProbeWorkMiddleware {
    async fn before_work(
        &self,
        _context: &WorkContext,
        _job: &mut JobRow,
    ) -> Result<(), riverqueue::Error> {
        self.0
            .add_trace("middleware:work_before")
            .map_err(|error| riverqueue::Error::runtime(error.to_string()))
    }

    async fn after_work(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        _result: &WorkResult,
    ) -> Result<(), riverqueue::Error> {
        self.0
            .add_trace("middleware:work_after")
            .map_err(|error| riverqueue::Error::runtime(error.to_string()))
    }
}

struct ConformancePlugin(Arc<RuntimeProbe>);

impl Plugin for ConformancePlugin {
    fn install(&self, extensions: &mut Extensions) {
        extensions
            .hook(ProbeHook(Arc::clone(&self.0)))
            .insert_middleware(ProbeInsertMiddleware(Arc::clone(&self.0)))
            .work_middleware(ProbeWorkMiddleware(Arc::clone(&self.0)));
    }
}

struct FixedRetryPolicy(Duration);

impl RetryPolicy for FixedRetryPolicy {
    fn next_retry(&self, _job: &JobRow, _error: &str, now: DateTime<Utc>) -> Duration {
        let _ = now;
        self.0
    }
}

#[derive(Default)]
struct BarrierRegistry {
    senders: Mutex<HashMap<String, watch::Sender<bool>>>,
}

impl BarrierRegistry {
    fn clear(&self) -> io::Result<()> {
        self.senders
            .lock()
            .map_err(|_| io::Error::other("barrier registry lock poisoned"))?
            .clear();
        Ok(())
    }

    fn create(&self, name: &str) -> io::Result<()> {
        if name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "barrier name is required",
            ));
        }
        let mut senders = self
            .senders
            .lock()
            .map_err(|_| io::Error::other("barrier registry lock poisoned"))?;
        if senders.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("barrier {name:?} already exists"),
            ));
        }
        let (sender, _) = watch::channel(false);
        senders.insert(name.to_owned(), sender);
        Ok(())
    }

    fn release(&self, name: &str) -> io::Result<()> {
        let sender = self
            .senders
            .lock()
            .map_err(|_| io::Error::other("barrier registry lock poisoned"))?
            .remove(name)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("barrier {name:?} not found"),
                )
            })?;
        sender
            .send(true)
            .map_err(|_| io::Error::other(format!("barrier {name:?} has no waiter")))
    }

    async fn wait(&self, name: &str) -> io::Result<()> {
        let mut receiver = self
            .senders
            .lock()
            .map_err(|_| io::Error::other("barrier registry lock poisoned"))?
            .get(name)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("barrier {name:?} not found"),
                )
            })?
            .subscribe();
        if !*receiver.borrow() {
            receiver
                .changed()
                .await
                .map_err(|_| io::Error::other(format!("barrier {name:?} was removed")))?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Deserialize)]
struct InsertParams {
    #[serde(default)]
    behavior: String,
    #[serde(default)]
    duration_ms: u64,
    #[serde(default)]
    kind: String,
    message: String,
    #[serde(default)]
    opts: InsertOptsParams,
    #[serde(default)]
    schema: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct InsertOptsParams {
    max_attempts: Option<i16>,
    #[serde(default)]
    metadata: Map<String, Value>,
    #[serde(default)]
    pending: bool,
    priority: Option<i16>,
    queue: Option<String>,
    scheduled_at: Option<DateTime<Utc>>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    unique: UniqueOptsParams,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct UniqueOptsParams {
    #[serde(default)]
    by_args: bool,
    by_period_ms: Option<u64>,
    #[serde(default)]
    by_queue: bool,
    by_state: Option<Vec<JobState>>,
    #[serde(default)]
    exclude_kind: bool,
}

impl UniqueOptsParams {
    fn to_unique_opts(&self) -> UniqueOpts {
        build_unique_opts(
            self.by_args,
            self.by_period_ms.map(Duration::from_millis),
            self.by_queue,
            self.by_state.clone(),
            self.exclude_kind,
        )
    }
}

struct RunningClient {
    client: Client,
    events: EventReceiver,
    handle: RunHandle,
    probe: Arc<RuntimeProbe>,
}

fn build_unique_opts(
    by_args: bool,
    by_period: Option<Duration>,
    by_queue: bool,
    by_state: Option<Vec<JobState>>,
    exclude_kind: bool,
) -> UniqueOpts {
    let mut opts = UniqueOpts::new();
    if by_args {
        opts = opts.by_args();
    }
    if let Some(period) = by_period {
        opts = opts.by_period(period);
    }
    if by_queue {
        opts = opts.by_queue();
    }
    if let Some(states) = by_state {
        opts = opts.by_states(states);
    }
    if exclude_kind {
        opts = opts.without_kind();
    }
    opts
}

struct Adapter {
    barriers: Arc<BarrierRegistry>,
    clock: Option<DateTime<Utc>>,
    pool: PgPool,
    profile: String,
    rng_seed: u64,
    running: Option<RunningClient>,
    transactions: HashMap<String, Transaction<'static, Postgres>>,
}

enum AdapterBackend {
    Postgres(Adapter),
    Sqlite(SqliteAdapter),
}

struct SqliteAdapter {
    barriers: Arc<BarrierRegistry>,
    clock: Option<DateTime<Utc>>,
    pool: SqlitePool,
    profile: String,
    rng_seed: u64,
    running: Option<RunningClient>,
    transactions: HashMap<String, Transaction<'static, Sqlite>>,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("River Rust conformance adapter: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("RIVER_CONFORMANCE_DATABASE_URL")?;
    let contract = ContractParams::load()?;
    let mut adapter = match std::env::var("RIVER_CONFORMANCE_DATABASE_KIND")
        .as_deref()
        .unwrap_or("postgres")
    {
        "postgres" => {
            let profile = std::env::var("RIVER_CONFORMANCE_PROFILE")
                .unwrap_or_else(|_| "postgres-full-v1".to_owned());
            if !matches!(profile.as_str(), "insert-only-v1" | "postgres-full-v1") {
                return Err(
                    format!("unsupported PostgreSQL conformance profile {profile:?}").into(),
                );
            }
            let options =
                postgres_connect_options(&database_url)?.application_name("river-conformance-rust");
            AdapterBackend::Postgres(Adapter {
                barriers: Arc::new(BarrierRegistry::default()),
                clock: None,
                pool: PgPoolOptions::new().connect_with(options).await?,
                profile,
                rng_seed: 0,
                running: None,
                transactions: HashMap::new(),
            })
        }
        "sqlite" => {
            let profile = std::env::var("RIVER_CONFORMANCE_PROFILE")
                .unwrap_or_else(|_| "portable-storage-v1".to_owned());
            if !matches!(
                profile.as_str(),
                "portable-storage-v1" | "sqlite-runtime-v1"
            ) {
                return Err(format!("unsupported SQLite conformance profile {profile:?}").into());
            }
            let options = SqliteConnectOptions::new()
                .filename(database_url)
                .create_if_missing(true)
                .foreign_keys(true)
                .busy_timeout(Duration::from_secs(5))
                .journal_mode(SqliteJournalMode::Wal);
            AdapterBackend::Sqlite(SqliteAdapter {
                barriers: Arc::new(BarrierRegistry::default()),
                clock: None,
                pool: SqlitePoolOptions::new()
                    .max_connections(5)
                    .connect_with(options)
                    .await?,
                profile,
                rng_seed: 0,
                running: None,
                transactions: HashMap::new(),
            })
        }
        kind => return Err(format!("unsupported RIVER_CONFORMANCE_DATABASE_KIND {kind:?}").into()),
    };
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<Request>(&line) {
            Ok(request) if request.jsonrpc == "2.0" => adapter.respond(request, &contract).await,
            Ok(request) => Response::error(
                request.id,
                error_code::INVALID_REQUEST,
                "jsonrpc must be 2.0".to_owned(),
            ),
            Err(error) => Response::error(Value::Null, error_code::PARSE, error.to_string()),
        };
        serde_json::to_writer(&mut stdout, &response)?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }
    match adapter {
        AdapterBackend::Postgres(mut adapter) => {
            if let Some(mut running) = adapter.running.take() {
                running.handle.shutdown_now().await?;
            }
        }
        AdapterBackend::Sqlite(mut adapter) => {
            if let Some(mut running) = adapter.running.take() {
                running.handle.shutdown_now().await?;
            }
        }
    }
    Ok(())
}

fn postgres_connect_options(database_url: &str) -> Result<PgConnectOptions, sqlx::Error> {
    let mut options = PgConnectOptions::from_str(database_url)?;
    if !database_url_has_userinfo(database_url)
        && let Some(username) = ["PGUSER", "USER", "LOGNAME"]
            .into_iter()
            .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
    {
        options = options.username(&username);
    }
    Ok(options)
}

fn database_url_has_userinfo(database_url: &str) -> bool {
    database_url
        .split_once("://")
        .and_then(|(_, remainder)| remainder.split('/').next())
        .is_some_and(|authority| authority.contains('@'))
}

impl AdapterBackend {
    async fn respond(&mut self, request: Request, contract: &ContractParams) -> Response {
        match self {
            Self::Postgres(adapter) => adapter.respond(request, contract).await,
            Self::Sqlite(adapter) => adapter.respond(request, contract).await,
        }
    }
}

impl Adapter {
    fn profile_methods(&self) -> (&'static [&'static str], &'static [&'static str]) {
        if self.profile == "insert-only-v1" {
            (INSERT_ONLY_METHODS, INSERT_ONLY_CAPABILITIES)
        } else {
            (ADAPTER_METHODS, CAPABILITIES)
        }
    }

    async fn respond(&mut self, request: Request, contract: &ContractParams) -> Response {
        let params = match decode_request_params(request.params.as_deref()) {
            Ok(params) => params,
            Err(error) => {
                return Response::error(request.id, error_code::INVALID_PARAMS, error.to_string());
            }
        };
        if !self.profile_methods().0.contains(&request.method.as_str()) {
            let error = AdapterError::method_not_found(&request.method);
            return Response::error(request.id, error.code, error.message);
        }
        if let Err(error) = contract.check(&request.method, &params) {
            return Response::error(request.id, error.code, error.message);
        }
        if request.method == "unique_key" {
            // Hash the exact request bytes so numbers keep their encoding.
            return respond_unique_key(&request);
        }
        let result = self.handle(&request.method, params).await;
        match result {
            Ok(result) => Response::success(request.id, result),
            Err(error) => {
                Response::error(request.id, error_code(error.as_ref()), error.to_string())
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn handle(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        match method {
            "handshake" => {
                let (methods, capabilities) = self.profile_methods();
                Ok(json!({
                    "adapter_version": ADAPTER_VERSION,
                    "backend": "postgres",
                    "capabilities": capabilities,
                    "implementation": "rust",
                    "implementation_version": env!("CARGO_PKG_VERSION"),
                    "methods": methods,
                    "migration_lines": {MIGRATION_LINE_MAIN: MIGRATION_VERSION_LATEST},
                    "profile": self.profile,
                    "protocol_revision": PROTOCOL_REVISION,
                }))
            }
            "migrate" => {
                let schema = schema_name(params.get("schema").and_then(Value::as_str))?;
                if let Some(name) = schema.as_deref() {
                    sqlx::query(AssertSqlSafe(format!(
                        "CREATE SCHEMA IF NOT EXISTS \"{name}\""
                    )))
                    .execute(&self.pool)
                    .await?;
                }
                let migrator = PostgresMigrator::new(self.pool.clone()).with_schema(schema);
                let direction = match params
                    .get("direction")
                    .and_then(Value::as_str)
                    .unwrap_or("up")
                {
                    "down" => Direction::Down,
                    "up" => Direction::Up,
                    value => return Err(format!("unknown migration direction {value:?}").into()),
                };
                let result = migrator.migrate(direction, migrate_opts(&params)?).await?;
                let applied = result
                    .versions
                    .iter()
                    .map(|version| version.version)
                    .collect::<Vec<_>>();
                let existing = migrator.existing_versions().await?;
                let valid = migrator.validate(None).await?.ok;
                Ok(json!({"applied": applied, "existing": existing, "valid": valid}))
            }
            "reset" => {
                if self.running.is_some() || !self.transactions.is_empty() {
                    return Err("reset requires no running client or open transaction".into());
                }
                let schema = schema_name(params.get("schema").and_then(Value::as_str))?;
                let sql = format!(
                    "TRUNCATE {}, {}, {}, {} RESTART IDENTITY CASCADE",
                    schema.qualify("river_job"),
                    schema.qualify("river_notification"),
                    schema.qualify("river_queue"),
                    schema.qualify("river_leader"),
                );
                sqlx::raw_sql(AssertSqlSafe(sql))
                    .execute(&self.pool)
                    .await?;
                self.barriers.clear()?;
                Ok(json!({}))
            }
            "clock_set" => {
                self.clock =
                    Some(DateTime::parse_from_rfc3339(&required_string(&params, "now")?)?.to_utc());
                Ok(json!({}))
            }
            "rng_seed" => {
                self.rng_seed = params
                    .get("seed")
                    .and_then(Value::as_u64)
                    .ok_or("seed must be an unsigned integer")?;
                Ok(json!({}))
            }
            "cron_next" => cron_next(&params),
            "retry_delay" => {
                let now = self
                    .clock
                    .ok_or("clock_set is required before retry_delay")?;
                let error_count = usize::try_from(required_i64(&params, "error_count")?)?;
                if error_count == 0 {
                    return Err("error_count must be positive".into());
                }
                let row = retry_row(required_i64(&params, "job_id")?, now, error_count - 1)?;
                let delay = DefaultRetryPolicy::with_seed(self.rng_seed).next_retry(
                    &row,
                    "conformance retry",
                    now,
                );
                Ok(json!({"delay_ns": u64::try_from(delay.as_nanos())?}))
            }
            "barrier_create" => {
                let name = required_string(&params, "name")?;
                self.barriers.create(&name)?;
                Ok(json!({}))
            }
            "barrier_release" => {
                let name = required_string(&params, "name")?;
                self.barriers.release(&name)?;
                Ok(json!({}))
            }
            "insert" => {
                let params: InsertParams = serde_json::from_value(params)?;
                let client = self.client_for_schema(&params.schema)?;
                let result = client
                    .insert(params.args())
                    .opts(params.opts.into_opts())
                    .await?;
                Ok(normalize_job(&result.job.row))
            }
            "insert_many" => {
                let jobs = insert_many_params(&params)?;
                let results = self.client()?.insert_many(jobs).await?;
                Ok(normalize_insert_many_results(&results))
            }
            "benchmark_enqueue" => {
                let jobs = usize::try_from(required_i64(&params, "jobs")?)?;
                if jobs == 0 {
                    return Err("jobs must be positive".into());
                }
                let client = self.client()?;
                let mut latencies = Vec::with_capacity(jobs);
                let started_at = std::time::Instant::now();
                for index in 0..jobs {
                    let inserted_at = std::time::Instant::now();
                    client
                        .insert(ConformanceArgs {
                            behavior: String::new(),
                            duration_ms: 0,
                            message: format!("benchmark-enqueue-{index}"),
                        })
                        .opts(InsertOpts::default())
                        .await?;
                    latencies.push(inserted_at.elapsed());
                }
                let duration = started_at.elapsed();
                latencies.sort_unstable();
                let p95 = latencies[(latencies.len() * 95).div_ceil(100) - 1];
                Ok(json!({
                    "duration_ns": u64::try_from(duration.as_nanos())?,
                    "p95_ns": u64::try_from(p95.as_nanos())?,
                }))
            }
            "insert_many_fast" => {
                let params = params.get("jobs").cloned().ok_or("missing jobs")?;
                let params: Vec<InsertParams> = serde_json::from_value(params)?;
                let jobs = params
                    .into_iter()
                    .map(|params| (params.args(), params.opts.into_opts()))
                    .collect::<Vec<_>>();
                let count = self.client()?.insert_many(jobs).fast().await?;
                Ok(json!({"count": count}))
            }
            "get" => {
                let client = self.client_for_schema(
                    params
                        .get("schema")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                )?;
                let row = client.jobs().get(required_i64(&params, "id")?).await?;
                Ok(normalize_job(&row))
            }
            "list" => {
                let list = list_params(&params)?;
                let rows = self.client()?.jobs().list(list).await?;
                Ok(normalize_job_list(&rows))
            }
            "cancel" => {
                let row = self
                    .client()?
                    .jobs()
                    .cancel(required_i64(&params, "id")?)
                    .await?;
                Ok(normalize_job(&row))
            }
            "delete" => {
                let row = self
                    .client()?
                    .jobs()
                    .delete(required_i64(&params, "id")?)
                    .await?;
                Ok(normalize_job(&row))
            }
            "delete_many" => {
                let list = list_params(&params)?;
                let delete = if params.get("all").and_then(Value::as_bool).unwrap_or(false) {
                    JobDeleteManyParams::all()
                } else {
                    JobDeleteManyParams::matching(list)
                };
                let rows = self.client()?.jobs().delete_many(delete).await?;
                Ok(json!({"jobs": rows.iter().map(normalize_job).collect::<Vec<_>>() }))
            }
            "retry" => {
                let row = self
                    .client()?
                    .jobs()
                    .retry(required_i64(&params, "id")?)
                    .await?;
                Ok(normalize_job(&row))
            }
            "update" => {
                let id = required_i64(&params, "id")?;
                let metadata = params
                    .get("metadata")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?
                    .unwrap_or_default();
                let output = params.get("output").cloned();
                let row = self
                    .client()?
                    .jobs()
                    .update(id, job_update_params(metadata, output))
                    .await?;
                Ok(normalize_job(&row))
            }
            "queue_add" => {
                let running = self
                    .running
                    .as_ref()
                    .ok_or("queue_add requires a running client")?;
                let max_workers = optional_i64(&params, "max_workers").unwrap_or(1);
                running.client.queue_add(
                    required_string(&params, "name")?,
                    QueueConfig::new(usize::try_from(max_workers)?)
                        .with_fetch_cooldown(Duration::from_millis(1))
                        .with_fetch_poll_interval(Duration::from_millis(10)),
                )?;
                Ok(json!({}))
            }
            "queue_get" => {
                let queue = self
                    .client()?
                    .queue_get(&required_string(&params, "name")?)
                    .await?;
                Ok(normalize_queue(&queue))
            }
            "queue_list" => {
                let limit = optional_i64(&params, "limit").unwrap_or(100);
                let queues = self
                    .client()?
                    .queue_list(&queue_list_params(i32::try_from(limit)?))
                    .await?;
                Ok(json!({
                    "queues": queues.iter().map(normalize_queue).collect::<Vec<_>>()
                }))
            }
            "queue_pause" | "queue_resume" => {
                let name = required_string(&params, "name")?;
                let client = self.client()?;
                if method == "queue_pause" {
                    client.queue_pause(&name).await?;
                } else {
                    client.queue_resume(&name).await?;
                }
                Ok(json!({}))
            }
            "queue_remove" => {
                let running = self
                    .running
                    .as_ref()
                    .ok_or("queue_remove requires a running client")?;
                let name = required_string(&params, "name")?;
                if running.client.queue_remove(&name)?.is_none() {
                    return Err(format!("queue {name:?} is not configured").into());
                }
                Ok(json!({}))
            }
            "queue_update" => {
                let name = required_string(&params, "name")?;
                let metadata = params
                    .get("metadata")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?
                    .unwrap_or_default();
                let queue = self.client()?.queue_update(&name, metadata).await?;
                Ok(normalize_queue(&queue))
            }
            "request_resign" => {
                if let Some(handle) = params.get("handle").and_then(Value::as_str) {
                    let client = self.client()?.clone();
                    let transaction = self.transactions.get_mut(handle).ok_or_else(|| {
                        AdapterError::not_found(format!("transaction {handle:?} not found"))
                    })?;
                    client.request_resign_tx(transaction).await?;
                } else {
                    self.client()?.request_resign().await?;
                }
                Ok(json!({}))
            }
            "leader" => {
                let leader = sqlx::query_as::<_, (String, DateTime<Utc>)>(
                    "SELECT leader_id, elected_at FROM river_leader WHERE name = 'default' AND expires_at >= now()",
                )
                .fetch_optional(&self.pool)
                .await?;
                Ok(match leader {
                    Some((leader_id, elected_at)) => json!({
                        "elected_at": format_time(elected_at),
                        "leader_id": leader_id,
                    }),
                    None => json!({"elected_at": null, "leader_id": null}),
                })
            }
            "listener_count" => {
                let count = sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND application_name = 'river-conformance-rust' AND query LIKE 'LISTEN %'",
                )
                .fetch_one(&self.pool)
                .await?;
                Ok(json!({"count": count}))
            }
            "connection_count" => {
                let count = sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND application_name = 'river-conformance-rust'",
                )
                .fetch_one(&self.pool)
                .await?;
                Ok(json!({"count": count}))
            }
            "fault_disconnect_listeners" => {
                let count = sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND application_name = 'river-conformance-rust' AND query LIKE 'LISTEN %' AND pid != pg_backend_pid()) AS terminated",
                )
                .fetch_one(&self.pool)
                .await?;
                Ok(json!({"count": count}))
            }
            "fault_disconnect_application" => {
                let application_name = required_string(&params, "application_name")?;
                // Only conformance adapters may be disconnected. Every
                // descriptor's application name carries this prefix, so the
                // check stays candidate-neutral.
                if !application_name.starts_with("river-conformance-")
                    || application_name == "river-conformance-harness"
                {
                    return Err("application_name must name a conformance adapter".into());
                }
                let count = sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND application_name = $1 AND pid != pg_backend_pid()) AS terminated",
                )
                .bind(application_name)
                .fetch_one(&self.pool)
                .await?;
                Ok(json!({"count": count}))
            }
            "fault_expire_leader" => {
                sqlx::query("UPDATE river_leader SET expires_at = now() - interval '1 second'")
                    .execute(&self.pool)
                    .await?;
                Ok(json!({}))
            }
            "raw_finalize" => {
                let id = required_i64(&params, "id")?;
                let state = required_string(&params, "state")?;
                if !matches!(state.as_str(), "completed" | "discarded") {
                    return Err(AdapterError::invalid_params(
                        "state must be completed or discarded",
                    )
                    .into());
                }
                let metadata = params.get("metadata").cloned().unwrap_or_else(|| json!({}));
                let result = sqlx::query(
                    r#"UPDATE river_job
                       SET errors = CASE WHEN $2 = 'discarded'
                               THEN array_append(errors, '{"at":"2026-02-03T04:05:06.789Z","attempt":1,"error":"external discard","trace":"external trace"}'::jsonb)
                               ELSE errors END,
                           finalized_at = now(),
                           metadata = metadata || $3::jsonb,
                           state = $2::river_job_state
                       WHERE id = $1 AND state = 'running'"#,
                )
                .bind(id)
                .bind(state)
                .bind(sqlx::types::Json(metadata))
                .execute(&self.pool)
                .await?;
                if result.rows_affected() != 1 {
                    return Err(AdapterError::not_found("running job not found").into());
                }
                Ok(normalize_job(&self.client()?.jobs().get(id).await?))
            }
            "raw_insert_no_notify" => {
                let params: InsertParams = serde_json::from_value(params)?;
                let kind = if params.kind.is_empty() {
                    "conformance_echo"
                } else {
                    &params.kind
                };
                let max_attempts = params.opts.max_attempts.unwrap_or(25);
                let id = sqlx::query_scalar::<_, i64>(
                    "INSERT INTO river_job (args, kind, max_attempts) VALUES ($1, $2, $3) RETURNING id",
                )
                .bind(sqlx::types::Json(params.args()))
                .bind(kind)
                .bind(max_attempts)
                .fetch_one(&self.pool)
                .await?;
                Ok(normalize_job(&self.client()?.jobs().get(id).await?))
            }
            "raw_insert_exact_json" => {
                let id = sqlx::query_scalar::<_, i64>(
                    r#"INSERT INTO river_job (id, args, kind, max_attempts, metadata)
                       VALUES (
                           COALESCE($1, nextval(pg_get_serial_sequence('river_job', 'id'))),
                           '{"decimal":0.12345678901234567890123456789,"integer":9223372036854775807}'::jsonb,
                           'conformance_exact_json', 25,
                           '{"negative":-9223372036854775808}'::jsonb
                       ) RETURNING id"#,
                )
				.bind(optional_i64(&params, "id"))
                .fetch_one(&self.pool)
                .await?;
                Ok(json!({"id": id}))
            }
            "raw_insert_full_row" => {
                let id = sqlx::query_scalar::<_, i64>(
                    r#"INSERT INTO river_job (
                        args, attempt, attempted_at, attempted_by, created_at, errors,
                        finalized_at, kind, max_attempts, metadata, priority, queue,
                        scheduled_at, state, tags, unique_key, unique_states
                    ) VALUES (
                        '{"nested":{"enabled":true},"values":[1,"two",null]}'::jsonb,
                        3, '2026-01-02T03:04:06.123456Z', ARRAY['go-client','candidate-client'],
                        '2026-01-02T03:04:05.6789Z',
                        ARRAY['{"at":"2026-01-02T03:04:06.123456Z","attempt":3,"error":"worker failed: escaped \"detail\"","trace":"frame one\nframe two"}'::jsonb],
                        '2026-01-02T03:04:07.000001Z', 'conformance_full_row', 4,
                        '{"output":{"ok":true},"river:rescue_count":2,"user":"metadata"}'::jsonb,
                        2, 'priority_jobs', '2026-01-02T03:04:05.999999Z', 'discarded',
                        ARRAY['alpha_tag','beta_tag'], decode(repeat('ab', 32), 'hex'), B'11110101'
                    ) RETURNING id"#,
                )
                .fetch_one(&self.pool)
                .await?;
                Ok(normalize_job(&self.client()?.jobs().get(id).await?))
            }
            "raw_job_exact_json" => {
                let row = self
                    .client()?
                    .jobs()
                    .get(required_i64(&params, "id")?)
                    .await?;
                exact_json_tokens(&row)
            }
            "raw_job_timestamps" => {
                let id = required_i64(&params, "id")?;
                let (created_at, scheduled_at) = sqlx::query_as::<_, (String, String)>(
                    "SELECT created_at::text, scheduled_at::text FROM river_job WHERE id = $1",
                )
                .bind(id)
                .fetch_one(&self.pool)
                .await?;
                Ok(json!({"created_at": created_at, "scheduled_at": scheduled_at}))
            }
            "start" => {
                if self.running.is_some() {
                    return Err("client already running".into());
                }
                let client_id = required_string(&params, "client_id")?;
                let error_handler_cancel = params
                    .get("error_handler_cancel")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let fetch_poll_interval = optional_i64(&params, "fetch_poll_interval_ms")
                    .map(duration_millis)
                    .transpose()?
                    .unwrap_or(Duration::from_millis(10));
                let queue = params
                    .get("queue")
                    .and_then(Value::as_str)
                    .unwrap_or("default")
                    .to_owned();
                let max_workers = optional_i64(&params, "max_workers").unwrap_or(4);
                let poll_only = params
                    .get("poll_only")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let schema = schema_name(params.get("schema").and_then(Value::as_str))?;
                let probe = Arc::new(RuntimeProbe::default());
                let mut workers = WorkerRegistry::new();
                workers.register::<ConformanceArgs, _>(ConformanceWorker {
                    barriers: Arc::clone(&self.barriers),
                    pool: Some(self.pool.clone()),
                    probe: Arc::clone(&probe),
                })?;
                let maintenance = maintenance_config(&params)?;
                let mut builder = Client::builder(
                    PostgresDatabase::new(self.pool.clone())
                        .schema(schema)
                        .reindex(reindex_config(&params)?),
                )
                .id(client_id)
                .job_stuck_threshold(Duration::from_millis(100))
                .maintenance(maintenance)
                .workers(workers)
                .queue(
                    queue,
                    QueueConfig::new(usize::try_from(max_workers)?)
                        .with_fetch_cooldown(Duration::from_millis(1))
                        .with_fetch_poll_interval(fetch_poll_interval),
                );
                if poll_only {
                    builder = builder.without_notifications();
                }
                if params
                    .get("instrumented")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    builder = builder.plugin(ConformancePlugin(Arc::clone(&probe)));
                }
                builder = builder.error_handler(ConformanceErrorHandler {
                    cancel: error_handler_cancel,
                    probe: Arc::clone(&probe),
                });
                if let Some(milliseconds) = optional_i64(&params, "job_stuck_threshold_ms") {
                    builder = builder.job_stuck_threshold(duration_millis(milliseconds)?);
                }
                if let Some(milliseconds) = optional_i64(&params, "job_timeout_ms") {
                    builder = builder.job_timeout(Some(duration_millis(milliseconds)?));
                }
                if params
                    .get("job_timeout_disabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    builder = builder.job_timeout(None);
                }
                if params
                    .get("periodic_run_on_start")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    builder = builder.periodic_job(PeriodicJob::with_options(
                        IntervalSchedule::new(Duration::from_hours(1))?,
                        || ConformanceArgs {
                            behavior: String::new(),
                            duration_ms: 0,
                            message: "periodic run on start".to_owned(),
                        },
                        PeriodicJobOpts::new()
                            .with_id("conformance-periodic")
                            .run_on_start(),
                    ));
                }
                if let Some(milliseconds) = optional_i64(&params, "retry_delay_ms") {
                    builder =
                        builder.retry_policy(FixedRetryPolicy(duration_millis(milliseconds)?));
                }
                let client = builder.build()?;
                let events = client.subscribe_config(SubscribeConfig::new([
                    EventKind::JobCancelled,
                    EventKind::JobCompleted,
                    EventKind::JobFailed,
                    EventKind::JobInterrupted,
                    EventKind::JobSnoozed,
                    EventKind::QueuePaused,
                    EventKind::QueueResumed,
                ])?)?;
                let mut handle = client.start()?;
                handle.wait_ready().await?;
                self.running = Some(RunningClient {
                    client,
                    events,
                    handle,
                    probe,
                });
                Ok(json!({}))
            }
            "stop" => {
                let mut running = self.running.take().ok_or("client is not running")?;
                if params
                    .get("cancel")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    running.handle.shutdown_now().await?;
                } else {
                    running.handle.shutdown().await?;
                }
                Ok(json!({}))
            }
            "runtime_stats" => {
                let running = self
                    .running
                    .as_mut()
                    .ok_or("runtime_stats requires a running client")?;
                while let Ok(event) =
                    tokio::time::timeout(Duration::from_millis(1), running.events.recv()).await
                {
                    running.probe.add_event(event?.kind())?;
                }
                Ok(running.probe.snapshot()?)
            }
            "wait" => {
                let id = required_i64(&params, "id")?;
                let row = if let Some(running) = &self.running {
                    wait_for_state(&running.client, id, params.get("states")).await?
                } else {
                    wait_for_state(&self.client()?, id, params.get("states")).await?
                };
                Ok(normalize_job(&row))
            }
            "work" => {
                let id = required_i64(&params, "id")?;
                if self.running.is_some() {
                    return Err("work requires no already-running client".into());
                }
                let mut workers = WorkerRegistry::new();
                workers.register::<ConformanceArgs, _>(ConformanceWorker {
                    barriers: Arc::clone(&self.barriers),
                    pool: Some(self.pool.clone()),
                    probe: Arc::new(RuntimeProbe::default()),
                })?;
                let client = Client::builder(
                    PostgresDatabase::new(self.pool.clone())
                        .schema(schema_name(params.get("schema").and_then(Value::as_str))?),
                )
                .id(params
                    .get("client_id")
                    .and_then(Value::as_str)
                    .unwrap_or("rust-conformance-adapter"))
                .workers(workers)
                .queue(
                    "default",
                    QueueConfig::new(1)
                        .with_fetch_cooldown(Duration::from_millis(1))
                        .with_fetch_poll_interval(Duration::from_millis(10)),
                )
                .build()?;
                let mut handle = client.start()?;
                let row = wait_for_state(&client, id, None).await;
                let stop = handle.shutdown().await;
                stop?;
                Ok(normalize_job(&row?))
            }
            "tx_begin" => {
                let handle = required_string(&params, "handle")?;
                if self.transactions.contains_key(&handle) {
                    return Err(format!("transaction {handle:?} already exists").into());
                }
                self.transactions.insert(handle, self.pool.begin().await?);
                Ok(json!({}))
            }
            "tx_insert" => {
                let handle = required_string(&params, "handle")?;
                let insert: InsertParams =
                    serde_json::from_value(params.get("job").cloned().ok_or("missing job")?)?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                let row = client
                    .insert(insert.args())
                    .opts(insert.opts.into_opts())
                    .tx(transaction)
                    .await?;
                Ok(normalize_job(&row.job.row))
            }
            "tx_insert_many" | "tx_insert_many_fast" => {
                let handle = required_string(&params, "handle")?;
                let jobs = insert_many_params(params.get("jobs").ok_or("missing jobs")?)?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                if method == "tx_insert_many_fast" {
                    let count = client.insert_many(jobs).fast().tx(transaction).await?;
                    Ok(json!({"count": count}))
                } else {
                    let results = client.insert_many(jobs).tx(transaction).await?;
                    Ok(normalize_insert_many_results(&results))
                }
            }
            "tx_get" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                Ok(normalize_job(&client.jobs().get(id).tx(transaction).await?))
            }
            "tx_cancel" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                Ok(normalize_job(
                    &client.jobs().cancel(id).tx(transaction).await?,
                ))
            }
            "tx_delete" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                Ok(normalize_job(
                    &client.jobs().delete(id).tx(transaction).await?,
                ))
            }
            "tx_retry" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                Ok(normalize_job(
                    &client.jobs().retry(id).tx(transaction).await?,
                ))
            }
            "tx_update" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let metadata = params
                    .get("metadata")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?
                    .unwrap_or_default();
                let output = params.get("output").cloned();
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                let row = client
                    .jobs()
                    .update(id, job_update_params(metadata, output))
                    .tx(transaction)
                    .await?;
                Ok(normalize_job(&row))
            }
            "tx_list" => {
                let handle = required_string(&params, "handle")?;
                let list = list_params(&params)?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                let rows = client.jobs().list(list).tx(transaction).await?;
                Ok(normalize_job_list(&rows))
            }
            "tx_delete_many" => {
                let handle = required_string(&params, "handle")?;
                let filter = list_params(&params)?;
                let all = params.get("all").and_then(Value::as_bool).unwrap_or(false);
                let delete = if all {
                    JobDeleteManyParams::all()
                } else {
                    JobDeleteManyParams::matching(filter)
                };
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                let rows = client.jobs().delete_many(delete).tx(transaction).await?;
                Ok(json!({"jobs": rows.iter().map(normalize_job).collect::<Vec<_>>() }))
            }
            "tx_queue_get" => {
                let handle = required_string(&params, "handle")?;
                let name = required_string(&params, "name")?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                Ok(normalize_queue(
                    &client.queue_get_tx(transaction, &name).await?,
                ))
            }
            "tx_queue_list" => {
                let handle = required_string(&params, "handle")?;
                let limit = optional_i64(&params, "limit").unwrap_or(100);
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                let queues = client
                    .queue_list_tx(transaction, &queue_list_params(i32::try_from(limit)?))
                    .await?;
                Ok(json!({
                    "queues": queues.iter().map(normalize_queue).collect::<Vec<_>>()
                }))
            }
            "tx_queue_pause" | "tx_queue_resume" => {
                let handle = required_string(&params, "handle")?;
                let name = required_string(&params, "name")?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                if method == "tx_queue_pause" {
                    client.queue_pause_tx(transaction, &name).await?;
                } else {
                    client.queue_resume_tx(transaction, &name).await?;
                }
                Ok(json!({}))
            }
            "tx_queue_update" => {
                let handle = required_string(&params, "handle")?;
                let name = required_string(&params, "name")?;
                let metadata = params
                    .get("metadata")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?
                    .unwrap_or_default();
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                Ok(normalize_queue(
                    &client.queue_update_tx(transaction, &name, metadata).await?,
                ))
            }
            "tx_fail" => {
                let handle = required_string(&params, "handle")?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                sqlx::query("SELECT 1 / 0")
                    .execute(&mut **transaction)
                    .await?;
                Ok(json!({}))
            }
            "tx_commit" | "tx_rollback" => {
                let handle = required_string(&params, "handle")?;
                let transaction = self.transactions.remove(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                if method == "tx_commit" {
                    transaction.commit().await?;
                } else {
                    transaction.rollback().await?;
                }
                Ok(json!({}))
            }
            _ => Err(AdapterError::method_not_found(method).into()),
        }
    }

    fn client(&self) -> Result<Client, riverqueue::Error> {
        self.client_for_schema("")
    }

    fn client_for_schema(&self, schema: &str) -> Result<Client, riverqueue::Error> {
        let schema = SchemaName::new(schema)
            .map_err(|error| riverqueue::Error::invalid_job(error.to_string()))?;
        if let Some(running) = &self.running {
            if running.client.postgres_schema() != Some(&schema) {
                return Err(riverqueue::Error::invalid_job(format!(
                    "running client schema {:?} does not match requested schema {schema}",
                    running.client.postgres_schema()
                )));
            }
            return Ok(running.client.clone());
        }
        Client::builder(PostgresDatabase::new(self.pool.clone()).schema(schema)).build()
    }
}

impl SqliteAdapter {
    fn profile_methods(&self) -> (&'static [&'static str], &'static [&'static str]) {
        if self.profile == "sqlite-runtime-v1" {
            (SQLITE_RUNTIME_METHODS, SQLITE_RUNTIME_CAPABILITIES)
        } else {
            (SQLITE_ADAPTER_METHODS, SQLITE_CAPABILITIES)
        }
    }

    async fn respond(&mut self, request: Request, contract: &ContractParams) -> Response {
        let params = match decode_request_params(request.params.as_deref()) {
            Ok(params) => params,
            Err(error) => {
                return Response::error(request.id, error_code::INVALID_PARAMS, error.to_string());
            }
        };
        if !self.profile_methods().0.contains(&request.method.as_str()) {
            let error = AdapterError::method_not_found(&request.method);
            return Response::error(request.id, error.code, error.message);
        }
        if let Err(error) = contract.check(&request.method, &params) {
            return Response::error(request.id, error.code, error.message);
        }
        if request.method == "unique_key" {
            // Hash the exact request bytes so numbers keep their encoding.
            return respond_unique_key(&request);
        }
        let result = self.handle(&request.method, params).await;
        match result {
            Ok(result) => Response::success(request.id, result),
            Err(error) => {
                Response::error(request.id, error_code(error.as_ref()), error.to_string())
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn handle(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        match method {
            "handshake" => {
                let (methods, capabilities) = self.profile_methods();
                Ok(json!({
                    "adapter_version": ADAPTER_VERSION,
                    "backend": "sqlite",
                    "capabilities": capabilities,
                    "implementation": "rust",
                    "implementation_version": env!("CARGO_PKG_VERSION"),
                    "methods": methods,
                    "migration_lines": {MIGRATION_LINE_MAIN: MIGRATION_VERSION_LATEST},
                    "profile": self.profile,
                    "protocol_revision": PROTOCOL_REVISION,
                }))
            }
            "migrate" => {
                if params
                    .get("schema")
                    .and_then(Value::as_str)
                    .is_some_and(|schema| !schema.is_empty())
                {
                    return Err(AdapterError::unsupported(
                        "SQLite conformance does not support custom schemas",
                    )
                    .into());
                }
                let migrator = SqliteMigrator::new(self.pool.clone());
                let direction = match params
                    .get("direction")
                    .and_then(Value::as_str)
                    .unwrap_or("up")
                {
                    "down" => Direction::Down,
                    "up" => Direction::Up,
                    value => return Err(format!("unknown migration direction {value:?}").into()),
                };
                let result = migrator.migrate(direction, migrate_opts(&params)?).await?;
                let applied = result
                    .versions
                    .iter()
                    .map(|version| version.version)
                    .collect::<Vec<_>>();
                let existing = migrator.existing_versions().await?;
                let valid = migrator.validate(None).await?.ok;
                Ok(json!({"applied": applied, "existing": existing, "valid": valid}))
            }
            "reset" => {
                if !self.transactions.is_empty() {
                    return Err("reset requires no open transaction".into());
                }
                for table in [
                    "river_notification",
                    "river_job",
                    "river_queue",
                    "river_leader",
                ] {
                    sqlx::query(AssertSqlSafe(format!("DELETE FROM {table}")))
                        .execute(&self.pool)
                        .await?;
                }
                Ok(json!({}))
            }
            "clock_set" => {
                self.clock =
                    Some(DateTime::parse_from_rfc3339(&required_string(&params, "now")?)?.to_utc());
                Ok(json!({}))
            }
            "rng_seed" => {
                self.rng_seed = params
                    .get("seed")
                    .and_then(Value::as_u64)
                    .ok_or("seed must be an unsigned integer")?;
                Ok(json!({}))
            }
            "cron_next" => cron_next(&params),
            "retry_delay" => {
                let now = self
                    .clock
                    .ok_or("clock_set is required before retry_delay")?;
                let error_count = usize::try_from(required_i64(&params, "error_count")?)?;
                if error_count == 0 {
                    return Err("error_count must be positive".into());
                }
                let row = retry_row(required_i64(&params, "job_id")?, now, error_count - 1)?;
                let delay = DefaultRetryPolicy::with_seed(self.rng_seed).next_retry(
                    &row,
                    "conformance retry",
                    now,
                );
                Ok(json!({"delay_ns": u64::try_from(delay.as_nanos())?}))
            }
            "barrier_create" => {
                let name = required_string(&params, "name")?;
                self.barriers.create(&name)?;
                Ok(json!({}))
            }
            "barrier_release" => {
                let name = required_string(&params, "name")?;
                self.barriers.release(&name)?;
                Ok(json!({}))
            }
            "insert" => {
                let params: InsertParams = serde_json::from_value(params)?;
                if !params.schema.is_empty() {
                    return Err(AdapterError::unsupported(
                        "SQLite conformance does not support custom schemas",
                    )
                    .into());
                }
                let result = self
                    .client()?
                    .insert(params.args())
                    .opts(params.opts.into_opts())
                    .await?;
                Ok(normalize_job(&result.job.row))
            }
            "insert_many" => {
                let jobs = insert_many_params(&params)?;
                let results = self.client()?.insert_many(jobs).await?;
                Ok(normalize_insert_many_results(&results))
            }
            "insert_many_fast" => {
                let params = params.get("jobs").cloned().ok_or("missing jobs")?;
                let params: Vec<InsertParams> = serde_json::from_value(params)?;
                let jobs = params
                    .into_iter()
                    .map(|params| (params.args(), params.opts.into_opts()))
                    .collect::<Vec<_>>();
                let count = self.client()?.insert_many(jobs).fast().await?;
                Ok(json!({"count": count}))
            }
            "raw_insert_no_notify" => {
                let params: InsertParams = serde_json::from_value(params)?;
                let encoded_args = serde_json::to_string(&params.args())?;
                let kind = if params.kind.is_empty() {
                    "conformance_echo".to_owned()
                } else {
                    params.kind
                };
                let max_attempts = params.opts.max_attempts.unwrap_or(25);
                let id = sqlx::query_scalar::<_, i64>(
                    "INSERT INTO river_job (args, kind, max_attempts) VALUES (jsonb(?), ?, ?) RETURNING id",
                )
                .bind(encoded_args)
                .bind(kind)
                .bind(max_attempts)
                .fetch_one(&self.pool)
                .await?;
                Ok(normalize_job(&self.client()?.jobs().get(id).await?))
            }
            "raw_insert_exact_json" => {
                let id = sqlx::query_scalar::<_, i64>(
                    r#"INSERT INTO river_job (id, args, kind, max_attempts, metadata)
                       VALUES (
                           ?1,
                           jsonb('{"decimal":0.12345678901234567890123456789,"integer":9223372036854775807}'),
                           'conformance_exact_json', 25,
                           jsonb('{"negative":-9223372036854775808}')
                       ) RETURNING id"#,
                )
				.bind(optional_i64(&params, "id"))
                .fetch_one(&self.pool)
                .await?;
                Ok(json!({"id": id}))
            }
            "get" => {
                if params
                    .get("schema")
                    .and_then(Value::as_str)
                    .is_some_and(|schema| !schema.is_empty())
                {
                    return Err(AdapterError::unsupported(
                        "SQLite conformance does not support custom schemas",
                    )
                    .into());
                }
                let row = self
                    .client()?
                    .jobs()
                    .get(required_i64(&params, "id")?)
                    .await?;
                Ok(normalize_job(&row))
            }
            "list" => {
                let list = list_params(&params)?;
                let rows = self.client()?.jobs().list(list).await?;
                Ok(normalize_job_list(&rows))
            }
            "cancel" => {
                let row = self
                    .client()?
                    .jobs()
                    .cancel(required_i64(&params, "id")?)
                    .await?;
                Ok(normalize_job(&row))
            }
            "delete" => {
                let row = self
                    .client()?
                    .jobs()
                    .delete(required_i64(&params, "id")?)
                    .await?;
                Ok(normalize_job(&row))
            }
            "delete_many" => {
                let list = list_params(&params)?;
                let delete = if params.get("all").and_then(Value::as_bool).unwrap_or(false) {
                    JobDeleteManyParams::all()
                } else {
                    JobDeleteManyParams::matching(list)
                };
                let rows = self.client()?.jobs().delete_many(delete).await?;
                Ok(json!({"jobs": rows.iter().map(normalize_job).collect::<Vec<_>>() }))
            }
            "retry" => {
                let row = self
                    .client()?
                    .jobs()
                    .retry(required_i64(&params, "id")?)
                    .await?;
                Ok(normalize_job(&row))
            }
            "update" => {
                let id = required_i64(&params, "id")?;
                let metadata = params
                    .get("metadata")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?
                    .unwrap_or_default();
                let output = params.get("output").cloned();
                let row = self
                    .client()?
                    .jobs()
                    .update(id, job_update_params(metadata, output))
                    .await?;
                Ok(normalize_job(&row))
            }
            "raw_finalize" => {
                let id = required_i64(&params, "id")?;
                let state = required_string(&params, "state")?;
                if !matches!(state.as_str(), "completed" | "discarded") {
                    return Err(AdapterError::invalid_params(
                        "state must be completed or discarded",
                    )
                    .into());
                }
                let metadata = params.get("metadata").cloned().unwrap_or_else(|| json!({}));
                let result = sqlx::query(
                    r#"UPDATE river_job
                       SET errors = CASE WHEN ?2 = 'discarded'
                               THEN jsonb(json_insert(json(coalesce(errors, jsonb('[]'))), '$[#]', json('{"at":"2026-02-03T04:05:06.789Z","attempt":1,"error":"external discard","trace":"external trace"}')))
                               ELSE errors END,
                           finalized_at = strftime('%Y-%m-%d %H:%M:%f', 'now'),
                           metadata = jsonb_patch(json(metadata), json(?3)),
                           state = ?2
                       WHERE id = ?1 AND state = 'running'"#,
                )
                .bind(id)
                .bind(state)
                .bind(sqlx::types::Json(metadata))
                .execute(&self.pool)
                .await?;
                if result.rows_affected() != 1 {
                    return Err(AdapterError::not_found("running job not found").into());
                }
                Ok(normalize_job(&self.client()?.jobs().get(id).await?))
            }
            "raw_job_timestamps" => {
                let id = required_i64(&params, "id")?;
                let (created_at, scheduled_at) = sqlx::query_as::<_, (String, String)>(
                    "SELECT CAST(created_at AS TEXT), CAST(scheduled_at AS TEXT) FROM river_job WHERE id = ?",
                )
                .bind(id)
                .fetch_one(&self.pool)
                .await?;
                Ok(json!({"created_at": created_at, "scheduled_at": scheduled_at}))
            }
            "raw_job_exact_json" => {
                let row = self
                    .client()?
                    .jobs()
                    .get(required_i64(&params, "id")?)
                    .await?;
                exact_json_tokens(&row)
            }
            "queue_add" => {
                let running = self
                    .running
                    .as_ref()
                    .ok_or("queue_add requires a running client")?;
                let max_workers = optional_i64(&params, "max_workers").unwrap_or(1);
                running.client.queue_add(
                    required_string(&params, "name")?,
                    QueueConfig::new(usize::try_from(max_workers)?)
                        .with_fetch_cooldown(Duration::from_millis(1))
                        .with_fetch_poll_interval(Duration::from_millis(10)),
                )?;
                Ok(json!({}))
            }
            "queue_get" => {
                let queue = self
                    .client()?
                    .queue_get(&required_string(&params, "name")?)
                    .await?;
                Ok(normalize_queue(&queue))
            }
            "queue_list" => {
                let limit = optional_i64(&params, "limit").unwrap_or(100);
                let queues = self
                    .client()?
                    .queue_list(&queue_list_params(i32::try_from(limit)?))
                    .await?;
                Ok(json!({
                    "queues": queues.iter().map(normalize_queue).collect::<Vec<_>>()
                }))
            }
            "queue_pause" | "queue_resume" => {
                let name = required_string(&params, "name")?;
                let client = self.client()?;
                if method == "queue_pause" {
                    client.queue_pause(&name).await?;
                } else {
                    client.queue_resume(&name).await?;
                }
                Ok(json!({}))
            }
            "queue_remove" => {
                let running = self
                    .running
                    .as_ref()
                    .ok_or("queue_remove requires a running client")?;
                let name = required_string(&params, "name")?;
                if running.client.queue_remove(&name)?.is_none() {
                    return Err(format!("queue {name:?} is not configured").into());
                }
                Ok(json!({}))
            }
            "queue_update" => {
                let name = required_string(&params, "name")?;
                let metadata = params
                    .get("metadata")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?
                    .unwrap_or_default();
                let queue = self.client()?.queue_update(&name, metadata).await?;
                Ok(normalize_queue(&queue))
            }
            "leader" => {
                let leader = sqlx::query_as::<_, (String, String)>(
                    "SELECT leader_id, elected_at FROM river_leader WHERE name = 'default' AND expires_at >= strftime('%Y-%m-%d %H:%M:%f', 'now')",
                )
                .fetch_optional(&self.pool)
                .await?;
                Ok(match leader {
                    Some((leader_id, elected_at)) => json!({
                        "elected_at": format_time(parse_sqlite_time(&elected_at)?),
                        "leader_id": leader_id,
                    }),
                    None => json!({"elected_at": null, "leader_id": null}),
                })
            }
            "request_resign" => {
                if let Some(handle) = params.get("handle").and_then(Value::as_str) {
                    let client = self.client()?.clone();
                    let transaction = self.transactions.get_mut(handle).ok_or_else(|| {
                        AdapterError::not_found(format!("transaction {handle:?} not found"))
                    })?;
                    client.request_resign_tx(transaction).await?;
                } else {
                    self.client()?.request_resign().await?;
                }
                Ok(json!({}))
            }
            "start" => {
                if self.running.is_some() {
                    return Err("client already running".into());
                }
                let client_id = required_string(&params, "client_id")?;
                let error_handler_cancel = params
                    .get("error_handler_cancel")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let fetch_poll_interval = optional_i64(&params, "fetch_poll_interval_ms")
                    .map(duration_millis)
                    .transpose()?
                    .unwrap_or(Duration::from_millis(10));
                let queue = params
                    .get("queue")
                    .and_then(Value::as_str)
                    .unwrap_or("default")
                    .to_owned();
                let max_workers = optional_i64(&params, "max_workers").unwrap_or(4);
                let poll_only = params
                    .get("poll_only")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let probe = Arc::new(RuntimeProbe::default());
                let mut workers = WorkerRegistry::new();
                workers.register::<ConformanceArgs, _>(ConformanceWorker {
                    barriers: Arc::clone(&self.barriers),
                    pool: None,
                    probe: Arc::clone(&probe),
                })?;
                let maintenance = maintenance_config(&params)?;
                let mut builder = Client::builder(SqliteDatabase::new(self.pool.clone()))
                    .id(client_id)
                    .job_stuck_threshold(Duration::from_millis(100))
                    .maintenance(maintenance)
                    .workers(workers)
                    .queue(
                        queue,
                        QueueConfig::new(usize::try_from(max_workers)?)
                            .with_fetch_cooldown(Duration::from_millis(1))
                            .with_fetch_poll_interval(fetch_poll_interval),
                    );
                if poll_only {
                    builder = builder.without_notifications();
                }
                if params
                    .get("instrumented")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    builder = builder.plugin(ConformancePlugin(Arc::clone(&probe)));
                }
                builder = builder.error_handler(ConformanceErrorHandler {
                    cancel: error_handler_cancel,
                    probe: Arc::clone(&probe),
                });
                if let Some(milliseconds) = optional_i64(&params, "job_stuck_threshold_ms") {
                    builder = builder.job_stuck_threshold(duration_millis(milliseconds)?);
                }
                if let Some(milliseconds) = optional_i64(&params, "job_timeout_ms") {
                    builder = builder.job_timeout(Some(duration_millis(milliseconds)?));
                }
                if params
                    .get("job_timeout_disabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    builder = builder.job_timeout(None);
                }
                if params
                    .get("periodic_run_on_start")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    builder = builder.periodic_job(PeriodicJob::with_options(
                        IntervalSchedule::new(Duration::from_hours(1))?,
                        || ConformanceArgs {
                            behavior: String::new(),
                            duration_ms: 0,
                            message: "periodic run on start".to_owned(),
                        },
                        PeriodicJobOpts::new()
                            .with_id("conformance-periodic")
                            .run_on_start(),
                    ));
                }
                if let Some(milliseconds) = optional_i64(&params, "retry_delay_ms") {
                    builder =
                        builder.retry_policy(FixedRetryPolicy(duration_millis(milliseconds)?));
                }
                let client = builder.build()?;
                let events = client.subscribe_config(SubscribeConfig::new([
                    EventKind::JobCancelled,
                    EventKind::JobCompleted,
                    EventKind::JobFailed,
                    EventKind::JobInterrupted,
                    EventKind::JobSnoozed,
                    EventKind::QueuePaused,
                    EventKind::QueueResumed,
                ])?)?;
                let mut handle = client.start()?;
                handle.wait_ready().await?;
                self.running = Some(RunningClient {
                    client,
                    events,
                    handle,
                    probe,
                });
                Ok(json!({}))
            }
            "stop" => {
                let mut running = self.running.take().ok_or("client is not running")?;
                if params
                    .get("cancel")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    running.handle.shutdown_now().await?;
                } else {
                    running.handle.shutdown().await?;
                }
                Ok(json!({}))
            }
            "runtime_stats" => {
                let running = self
                    .running
                    .as_mut()
                    .ok_or("runtime_stats requires a running client")?;
                while let Ok(event) =
                    tokio::time::timeout(Duration::from_millis(1), running.events.recv()).await
                {
                    running.probe.add_event(event?.kind())?;
                }
                Ok(running.probe.snapshot()?)
            }
            "wait" => {
                let id = required_i64(&params, "id")?;
                let row = if let Some(running) = &self.running {
                    wait_for_state(&running.client, id, params.get("states")).await?
                } else {
                    wait_for_state(&self.client()?, id, params.get("states")).await?
                };
                Ok(normalize_job(&row))
            }
            "work" => {
                let id = required_i64(&params, "id")?;
                if self.running.is_some() {
                    return Err("work requires no already-running client".into());
                }
                let mut workers = WorkerRegistry::new();
                workers.register::<ConformanceArgs, _>(ConformanceWorker {
                    barriers: Arc::clone(&self.barriers),
                    pool: None,
                    probe: Arc::new(RuntimeProbe::default()),
                })?;
                let client = Client::builder(SqliteDatabase::new(self.pool.clone()))
                    .id(params
                        .get("client_id")
                        .and_then(Value::as_str)
                        .unwrap_or("rust-conformance-adapter"))
                    .workers(workers)
                    .queue(
                        "default",
                        QueueConfig::new(1)
                            .with_fetch_cooldown(Duration::from_millis(1))
                            .with_fetch_poll_interval(Duration::from_millis(10)),
                    )
                    .build()?;
                let mut handle = client.start()?;
                let row = wait_for_state(&client, id, None).await;
                let stop = handle.shutdown().await;
                stop?;
                Ok(normalize_job(&row?))
            }
            "tx_begin" => {
                let handle = required_string(&params, "handle")?;
                if self.transactions.contains_key(&handle) {
                    return Err(format!("transaction {handle:?} already exists").into());
                }
                self.transactions
                    .insert(handle, self.pool.begin_with("BEGIN IMMEDIATE").await?);
                Ok(json!({}))
            }
            "tx_insert" => {
                let handle = required_string(&params, "handle")?;
                let insert: InsertParams =
                    serde_json::from_value(params.get("job").cloned().ok_or("missing job")?)?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                let row = client
                    .insert(insert.args())
                    .opts(insert.opts.into_opts())
                    .tx(transaction)
                    .await?;
                Ok(normalize_job(&row.job.row))
            }
            "tx_insert_many" | "tx_insert_many_fast" => {
                let handle = required_string(&params, "handle")?;
                let jobs = insert_many_params(params.get("jobs").ok_or("missing jobs")?)?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                if method == "tx_insert_many_fast" {
                    let count = client.insert_many(jobs).fast().tx(transaction).await?;
                    Ok(json!({"count": count}))
                } else {
                    let results = client.insert_many(jobs).tx(transaction).await?;
                    Ok(normalize_insert_many_results(&results))
                }
            }
            "tx_get" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                Ok(normalize_job(&client.jobs().get(id).tx(transaction).await?))
            }
            "tx_cancel" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                Ok(normalize_job(
                    &client.jobs().cancel(id).tx(transaction).await?,
                ))
            }
            "tx_delete" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                Ok(normalize_job(
                    &client.jobs().delete(id).tx(transaction).await?,
                ))
            }
            "tx_retry" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                Ok(normalize_job(
                    &client.jobs().retry(id).tx(transaction).await?,
                ))
            }
            "tx_update" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let metadata = params
                    .get("metadata")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?
                    .unwrap_or_default();
                let output = params.get("output").cloned();
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                let row = client
                    .jobs()
                    .update(id, job_update_params(metadata, output))
                    .tx(transaction)
                    .await?;
                Ok(normalize_job(&row))
            }
            "tx_list" => {
                let handle = required_string(&params, "handle")?;
                let list = list_params(&params)?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                let rows = client.jobs().list(list).tx(transaction).await?;
                Ok(normalize_job_list(&rows))
            }
            "tx_delete_many" => {
                let handle = required_string(&params, "handle")?;
                let filter = list_params(&params)?;
                let all = params.get("all").and_then(Value::as_bool).unwrap_or(false);
                let delete = if all {
                    JobDeleteManyParams::all()
                } else {
                    JobDeleteManyParams::matching(filter)
                };
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                let rows = client.jobs().delete_many(delete).tx(transaction).await?;
                Ok(json!({"jobs": rows.iter().map(normalize_job).collect::<Vec<_>>() }))
            }
            "tx_queue_get" => {
                let handle = required_string(&params, "handle")?;
                let name = required_string(&params, "name")?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                Ok(normalize_queue(
                    &client.queue_get_tx(transaction, &name).await?,
                ))
            }
            "tx_queue_list" => {
                let handle = required_string(&params, "handle")?;
                let limit = optional_i64(&params, "limit").unwrap_or(100);
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                let queues = client
                    .queue_list_tx(transaction, &queue_list_params(i32::try_from(limit)?))
                    .await?;
                Ok(json!({
                    "queues": queues.iter().map(normalize_queue).collect::<Vec<_>>()
                }))
            }
            "tx_queue_pause" | "tx_queue_resume" => {
                let handle = required_string(&params, "handle")?;
                let name = required_string(&params, "name")?;
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                if method == "tx_queue_pause" {
                    client.queue_pause_tx(transaction, &name).await?;
                } else {
                    client.queue_resume_tx(transaction, &name).await?;
                }
                Ok(json!({}))
            }
            "tx_queue_update" => {
                let handle = required_string(&params, "handle")?;
                let name = required_string(&params, "name")?;
                let metadata = params
                    .get("metadata")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?
                    .unwrap_or_default();
                let client = self.client()?;
                let transaction = self.transactions.get_mut(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                Ok(normalize_queue(
                    &client.queue_update_tx(transaction, &name, metadata).await?,
                ))
            }
            "tx_commit" | "tx_rollback" => {
                let handle = required_string(&params, "handle")?;
                let transaction = self.transactions.remove(&handle).ok_or_else(|| {
                    AdapterError::not_found(format!("transaction {handle:?} not found"))
                })?;
                if method == "tx_commit" {
                    transaction.commit().await?;
                } else {
                    transaction.rollback().await?;
                }
                Ok(json!({}))
            }
            _ => Err(AdapterError::method_not_found(method).into()),
        }
    }

    fn client(&self) -> Result<Client, riverqueue::Error> {
        if let Some(running) = &self.running {
            return Ok(running.client.clone());
        }
        Client::builder(SqliteDatabase::new(self.pool.clone())).build()
    }
}

fn decode_request_params(raw: Option<&RawValue>) -> Result<Value, serde_json::Error> {
    raw.map_or(Ok(Value::Null), |raw| serde_json::from_str(raw.get()))
}

fn parse_sqlite_time(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .map(|value| value.and_utc())
}

fn schema_name(schema: Option<&str>) -> Result<SchemaName, riverqueue::Error> {
    SchemaName::new(schema.unwrap_or_default())
        .map_err(|error| riverqueue::Error::invalid_job(error.to_string()))
}

fn retry_row(
    id: i64,
    now: DateTime<Utc>,
    previous_errors: usize,
) -> Result<JobRow, serde_json::Error> {
    let mut row = JobRow::new(id, "conformance_echo", encode_args(&json!({}))?, now);
    row.attempt = i16::try_from(previous_errors.saturating_add(1)).unwrap_or(i16::MAX);
    row.attempted_at = Some(now);
    row.attempted_by = vec!["conformance".to_owned()];
    row.errors = vec![AttemptError::new(now, 1, "previous failure"); previous_errors];
    row.max_attempts = 1_000;
    row.metadata = Map::new();
    row.state = JobState::Retryable;
    Ok(row)
}

impl InsertParams {
    fn args(&self) -> ConformanceArgs {
        ConformanceArgs {
            behavior: self.behavior.clone(),
            duration_ms: self.duration_ms,
            message: self.message.clone(),
        }
    }
}

impl InsertOptsParams {
    fn into_opts(self) -> InsertOpts {
        let scheduled_at = self.scheduled_at;
        let unique = self.unique.to_unique_opts();
        let mut opts = InsertOpts::default()
            .with_metadata(self.metadata)
            .with_pending(self.pending)
            .with_tags(self.tags)
            .with_unique(unique);
        opts = match scheduled_at {
            Some(scheduled_at) => opts.with_scheduled_at(scheduled_at),
            None => opts.without_schedule(),
        };
        if let Some(max_attempts) = self.max_attempts {
            opts = opts.with_max_attempts(max_attempts);
        }
        if let Some(priority) = self.priority {
            opts = opts.with_priority(priority);
        }
        if let Some(queue) = self.queue {
            opts = opts.with_queue(queue);
        }
        opts
    }
}

fn list_params(params: &Value) -> Result<JobListParams, Box<dyn std::error::Error + Send + Sync>> {
    let mut list = JobListParams::default();
    if let Some(limit) = optional_i64(params, "limit") {
        list.limit = i32::try_from(limit)?;
    }
    list.ids = string_or_number_array::<i64>(params, "ids")?;
    list.kinds = string_array(params, "kinds")?;
    list.metadata = params
        .get("metadata")
        .cloned()
        .map(serde_json::from_value)
        .transpose()?;
    if let Some(order_by) = params.get("order_by").and_then(Value::as_str) {
        list.order_by = order_by.parse()?;
    }
    list.priorities = string_or_number_array::<i16>(params, "priorities")?;
    list.queues = string_array(params, "queues")?;
    list.tags_all = string_array(params, "tags_all")?;
    list.tags_any = string_array(params, "tags_any")?;
    if let Some(states) = params.get("states") {
        list.states = serde_json::from_value(states.clone())?;
    }
    if let Some(direction) = params.get("direction").and_then(Value::as_str) {
        list.direction = match direction {
            "asc" => SortDirection::Ascending,
            "desc" => SortDirection::Descending,
            _ => {
                return Err(
                    io::Error::other(format!("unsupported direction {direction:?}")).into(),
                );
            }
        };
    }
    if let Some(after) = params.get("after").and_then(Value::as_str) {
        list.after = Some(JobListCursor::decode(after).map_err(io::Error::other)?);
    }
    Ok(list)
}

async fn wait_for_state(
    client: &Client,
    id: i64,
    states: Option<&Value>,
) -> Result<JobRow, Box<dyn std::error::Error + Send + Sync>> {
    let states = states
        .cloned()
        .map(serde_json::from_value::<Vec<JobState>>)
        .transpose()?
        .unwrap_or_else(|| {
            vec![
                JobState::Cancelled,
                JobState::Completed,
                JobState::Discarded,
            ]
        });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let row = client.jobs().get(id).await?;
        if states.contains(&row.state) {
            return Ok(row);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("job {id} did not reach {states:?}; state={:?}", row.state).into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn job_update_params(metadata: Map<String, Value>, output: Option<Value>) -> JobUpdateParams {
    let params = JobUpdateParams::default().with_metadata(metadata);
    match output {
        Some(output) => params.with_output(output),
        None => params,
    }
}

fn queue_list_params(limit: i32) -> QueueListParams {
    QueueListParams::default().with_limit(limit)
}

fn exact_json_tokens(row: &JobRow) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let args: HashMap<String, Box<RawValue>> = row.decode_args()?;
    let arg_token = |key: &str| {
        args.get(key)
            .map(|value| value.get().to_owned())
            .ok_or_else(|| io::Error::other(format!("exact JSON key {key:?} not found")))
    };
    let negative = row
        .metadata
        .get("negative")
        .map(Value::to_string)
        .ok_or_else(|| io::Error::other("exact JSON key \"negative\" not found"))?;
    Ok(json!({
        "decimal": arg_token("decimal")?,
        "integer": arg_token("integer")?,
        "negative": negative,
    }))
}

fn normalize_job(row: &JobRow) -> Value {
    let mut metadata = row.metadata.clone();
    metadata.remove(riverqueue::METADATA_KEY_UNIQUE_NONCE);
    json!({
        "args": row.encoded_args,
        "attempt": row.attempt,
        "attempted_at": row.attempted_at.map(format_time),
        "attempted_by": row.attempted_by,
        "created_at": format_time(row.created_at),
        "errors": row.errors.iter().map(|error| json!({
            "at": format_time(error.at),
            "attempt": error.attempt,
            "error": error.error,
            "trace": error.trace,
        })).collect::<Vec<_>>(),
        "finalized_at": row.finalized_at.map(format_time),
        "id": row.id,
        "kind": row.kind,
        "max_attempts": row.max_attempts,
        "metadata": metadata,
        "priority": row.priority,
        "queue": row.queue,
        "scheduled_at": format_time(row.scheduled_at),
        "state": row.state,
        "tags": row.tags,
        "unique_key": row.unique_key.as_deref().map(hex),
        "unique_states": row.unique_states,
    })
}

fn insert_many_params(
    params: &Value,
) -> Result<Vec<(ConformanceArgs, InsertOpts)>, Box<dyn std::error::Error + Send + Sync>> {
    let jobs = if params.is_array() {
        params.clone()
    } else {
        params.get("jobs").cloned().ok_or("missing jobs")?
    };
    let jobs: Vec<InsertParams> = serde_json::from_value(jobs)?;
    Ok(jobs
        .into_iter()
        .map(|params| (params.args(), params.opts.into_opts()))
        .collect())
}

fn normalize_insert_many_results<A: JobArgs>(results: &[InsertResult<A>]) -> Value {
    json!({
        "results": results.iter().map(|result| json!({
            "job": normalize_job(&result.job.row),
            "unique_skipped_as_duplicate": result.unique_skipped_as_duplicate,
        })).collect::<Vec<_>>(),
    })
}

fn normalize_job_list(result: &JobListResult) -> Value {
    json!({
        "cursor": result.last_cursor.as_ref().map(JobListCursor::encode),
        "jobs": result.jobs.iter().map(normalize_job).collect::<Vec<_>>(),
    })
}

fn normalize_queue(queue: &Queue) -> Value {
    json!({
        "created_at": format_time(queue.created_at),
        "metadata": queue.metadata,
        "name": queue.name,
        "paused_at": queue.paused_at.map(format_time),
        "updated_at": format_time(queue.updated_at),
    })
}

fn event_kind_name(kind: EventKind) -> &'static str {
    match kind {
        EventKind::JobCancelled => "job_cancelled",
        EventKind::JobCompleted => "job_completed",
        EventKind::JobFailed => "job_failed",
        EventKind::JobInterrupted => "job_interrupted",
        EventKind::JobSnoozed => "job_snoozed",
        EventKind::QueuePaused => "queue_paused",
        EventKind::QueueResumed => "queue_resumed",
        _ => "unknown",
    }
}

fn format_time(time: DateTime<Utc>) -> String {
    let formatted = time.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let Some(without_zone) = formatted.strip_suffix('Z') else {
        return formatted;
    };
    let without_zeroes = without_zone.trim_end_matches('0');
    let normalized = without_zeroes.strip_suffix('.').unwrap_or(without_zeroes);
    format!("{normalized}Z")
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn optional_i64(params: &Value, name: &str) -> Option<i64> {
    params.get(name).and_then(Value::as_i64)
}

fn migrate_opts(params: &Value) -> Result<MigrateOpts, Box<dyn std::error::Error + Send + Sync>> {
    let mut opts = MigrateOpts::new().with_dry_run(
        params
            .get("dry_run")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    );
    if let Some(max_steps) = optional_i64(params, "max_steps") {
        opts = opts.with_max_steps(usize::try_from(max_steps)?);
    }
    if let Some(target_version) = optional_i64(params, "target_version") {
        opts = opts.with_target_version(target_version);
    }
    Ok(opts)
}

/// Applies optional maintenance tuning from `start` parameters. Job
/// retentions of `-1` keep that state forever, like Go.
fn maintenance_config(
    params: &Value,
) -> Result<MaintenanceConfig, Box<dyn std::error::Error + Send + Sync>> {
    let mut maintenance = MaintenanceConfig::default();
    let retention = |name: &str| -> Result<
        Option<Option<Duration>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        match optional_i64(params, name) {
            None => Ok(None),
            Some(-1) => Ok(Some(None)),
            Some(milliseconds) => Ok(Some(Some(duration_millis(milliseconds)?))),
        }
    };
    if let Some(retention) = retention("cancelled_job_retention_ms")? {
        maintenance = maintenance.with_cancelled_job_retention(retention);
    }
    if let Some(retention) = retention("completed_job_retention_ms")? {
        maintenance = maintenance.with_completed_job_retention(retention);
    }
    if let Some(retention) = retention("discarded_job_retention_ms")? {
        maintenance = maintenance.with_discarded_job_retention(retention);
    }
    if let Some(milliseconds) = optional_i64(params, "elect_interval_ms") {
        maintenance = maintenance.with_elect_interval(duration_millis(milliseconds)?);
    }
    if let Some(milliseconds) = optional_i64(params, "job_cleaner_interval_ms") {
        maintenance = maintenance.with_job_cleaner_interval(duration_millis(milliseconds)?);
    }
    if let Some(milliseconds) = optional_i64(params, "queue_cleaner_interval_ms") {
        maintenance = maintenance.with_queue_cleaner_interval(duration_millis(milliseconds)?);
    }
    if let Some(milliseconds) = optional_i64(params, "rescue_after_ms") {
        maintenance = maintenance.with_rescue_after(duration_millis(milliseconds)?);
    }
    if let Some(milliseconds) = optional_i64(params, "rescuer_interval_ms") {
        maintenance = maintenance.with_rescuer_interval(duration_millis(milliseconds)?);
    }
    if let Some(milliseconds) = optional_i64(params, "scheduler_interval_ms") {
        maintenance = maintenance.with_scheduler_interval(duration_millis(milliseconds)?);
    }
    Ok(maintenance)
}

/// Builds the PostgreSQL reindexer configuration from optional `start`
/// parameters.
fn reindex_config(
    params: &Value,
) -> Result<PostgresReindexConfig, Box<dyn std::error::Error + Send + Sync>> {
    let mut config = PostgresReindexConfig::default();
    if let Some(names) = params.get("reindexer_index_names") {
        let names = names
            .as_array()
            .ok_or("reindexer_index_names must be an array")?
            .iter()
            .map(|name| name.as_str().ok_or("reindexer index names must be strings"))
            .collect::<Result<Vec<_>, _>>()?;
        config = config.with_index_names(names);
    }
    if let Some(milliseconds) = optional_i64(params, "reindexer_interval_ms") {
        config = config.with_schedule(PostgresReindexSchedule::Interval(duration_millis(
            milliseconds,
        )?));
    }
    Ok(config)
}

/// Evaluates River Go's standard cron syntax from a reference time, returning
/// successive occurrences in that time's offset.
fn cron_next(params: &Value) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let expression = required_string(params, "expression")?;
    let from = DateTime::parse_from_rfc3339(&required_string(params, "from")?)?;
    let count = usize::try_from(required_i64(params, "count")?)?;
    if count == 0 {
        return Err("count must be positive".into());
    }
    let schedule = CronSchedule::parse(&expression)?;
    let mut next = Vec::with_capacity(count);
    let mut current = from;
    while next.len() < count {
        let Some(occurrence) = schedule.next_after(&current) else {
            break;
        };
        next.push(occurrence.to_rfc3339_opts(SecondsFormat::AutoSi, true));
        current = occurrence;
    }
    Ok(json!({"next": next}))
}

fn duration_millis(
    milliseconds: i64,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    Ok(Duration::from_millis(u64::try_from(milliseconds)?))
}

fn required_i64(
    params: &Value,
    name: &str,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    optional_i64(params, name).ok_or_else(|| format!("missing integer parameter {name:?}").into())
}

fn required_string(
    params: &Value,
    name: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    params
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("missing string parameter {name:?}").into())
}

fn string_array(
    params: &Value,
    name: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    params
        .get(name)
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
        .map(Option::unwrap_or_default)
}

fn string_or_number_array<T>(
    params: &Value,
    name: &str,
) -> Result<Vec<T>, Box<dyn std::error::Error + Send + Sync>>
where
    T: serde::de::DeserializeOwned,
{
    params
        .get(name)
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
        .map(Option::unwrap_or_default)
}

impl Response {
    fn error(id: Value, code: i32, message: String) -> Self {
        Self {
            error: Some(ResponseError { code, message }),
            id,
            jsonrpc: "2.0",
            result: None,
        }
    }

    fn success(id: Value, result: Value) -> Self {
        Self {
            error: None,
            id,
            jsonrpc: "2.0",
            result: Some(result),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_explicit_postgres_userinfo() {
        assert!(!database_url_has_userinfo(
            "postgres://localhost/river_conformance"
        ));
        assert!(database_url_has_userinfo(
            "postgres://river@localhost/river_conformance"
        ));
        assert!(database_url_has_userinfo(
            "postgres://river:secret@localhost/river_conformance"
        ));
    }

    #[test]
    fn unique_key_hashes_exact_request_argument_tokens() {
        // Go-generated golden `map_order_and_negative_zero`: the `-0` token
        // and member order must reach the hash unchanged.
        let request: Request = serde_json::from_str(concat!(
            r#"{"id":1,"jsonrpc":"2.0","method":"unique_key","params":{"#,
            "\"args\":{\"2\":2,\"10\":10,\"zero\":-0,\"😀\":1,\"\u{e000}\":2},",
            r#""kind":"conformance_all_args","now":"2026-01-02T03:04:05.6789Z","#,
            r#""options":{"by_args":true,"by_period_nanos":0,"by_queue":false,"exclude_kind":false},"#,
            r#""queue":"default","scheduled_at":null}}"#,
        ))
        .unwrap();
        let response = serde_json::to_value(respond_unique_key(&request)).unwrap();

        assert_eq!(
            response["result"]["sha256"],
            "fcdf33e0c39c1fc7e956876345a985f2418bd69c6e4d6a5c794abf1e78cdfdb6"
        );
    }

    #[test]
    fn timestamp_format_matches_go_rfc3339_nano() {
        let timestamp = DateTime::parse_from_rfc3339("2026-08-11T17:20:27.425860Z")
            .unwrap()
            .to_utc();
        assert_eq!(format_time(timestamp), "2026-08-11T17:20:27.42586Z");

        let whole_second = DateTime::parse_from_rfc3339("2026-08-11T17:20:27Z")
            .unwrap()
            .to_utc();
        assert_eq!(format_time(whole_second), "2026-08-11T17:20:27Z");
    }
}
