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
    AttemptError, Client, DefaultRetryPolicy, ErrorHandler, ErrorHandlerDecision, EventKind,
    EventReceiver, Hook, InsertContext, InsertMiddleware, InsertOpts, InsertResult,
    IntervalSchedule, Job, JobArgs, JobDeleteManyParams, JobListCursor, JobListParams, JobRow,
    JobState, JobUpdateParams, MaintenanceConfig, PeriodicJob, PeriodicJobOpts, PeriodicJobs,
    Plugin, Queue, QueueConfig, QueueListParams, RetryPolicy, RunHandle, SortDirection,
    SubscribeConfig, UniqueKeyInput, UniqueOpts, WorkContext, WorkMiddleware, WorkOutcome,
    WorkResult, Worker, WorkerRegistry, build_unique_key,
    database::{PostgresDatabase, SqliteDatabase},
};
use riverqueue_migrate::{
    Direction, MIGRATION_LINE_MAIN, MIGRATION_VERSION_LATEST, MigrateOpts, PostgresMigrator,
    SqliteMigrator,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::{
    AssertSqlSafe, PgPool, Postgres, Sqlite, SqlitePool, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions},
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use tokio::sync::watch;

const ADAPTER_VERSION: u32 = 11;
const PROTOCOL_REVISION: u32 = 1;

const ADAPTER_METHODS: &[&str] = &[
    "barrier_create",
    "barrier_release",
    "benchmark_enqueue",
    "cancel",
    "clock_set",
    "connection_count",
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
    "raw_insert_full_row",
    "raw_insert_no_notify",
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
    "subscriptions",
    "transactions",
    "unique_jobs",
    "work",
];

const SQLITE_ADAPTER_METHODS: &[&str] = &[
    "cancel",
    "clock_set",
    "delete",
    "delete_many",
    "get",
    "handshake",
    "insert",
    "insert_many",
    "insert_many_fast",
    "list",
    "migrate",
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
    "raw_insert_no_notify",
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

#[derive(Debug, Deserialize)]
struct Request {
    id: Value,
    jsonrpc: String,
    method: String,
    #[serde(default)]
    params: Value,
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
    args: Value,
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

fn unique_key_for_args<A>(params: &UniqueKeyParams, opts: &UniqueOpts) -> Result<[u8; 32], String>
where
    A: JobArgs + serde::de::DeserializeOwned,
{
    let args =
        serde_json::from_value::<A>(params.args.clone()).map_err(|error| error.to_string())?;
    build_unique_key(&UniqueKeyInput {
        args: &args,
        encoded_args: &params.args,
        now: params.now,
        opts,
        queue: &params.queue,
        scheduled_at: params.scheduled_at,
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

#[derive(Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "conformance_all_args")]
struct UniqueAllArgs {
    alpha: String,
    maximum: i64,
    zeta: String,
}

#[derive(Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "conformance_numeric_boundaries")]
struct UniqueNumericArgs {
    exponent: f64,
    fraction: f64,
    maximum: i64,
    minimum: i64,
    unsigned_maximum: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct UniqueSelectedAccount {
    id: String,
    ignored: String,
}

#[derive(Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "conformance_selected_args", unique("account.id", "label"))]
struct UniqueSelectedArgs {
    account: UniqueSelectedAccount,
    ignored: bool,
    label: String,
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

#[allow(clippy::match_same_arms)]
impl Worker<ConformanceArgs> for ConformanceWorker {
    type Error = io::Error;

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
                        .record_output(&json!({"race": "worker"}))
                        .await
                        .map_err(io::Error::other)?;
                }
                Ok(WorkOutcome::Complete)
            }
            "cancel" => Ok(WorkOutcome::Cancel),
            "cooperative_cancel" => {
                context.cancellation_token().cancelled().await;
                Err(io::Error::other("work cancelled"))
            }
            "discard" => Ok(WorkOutcome::Discard),
            "error" => Err(io::Error::other("conformance retryable error")),
            "ignored_cancel" => std::future::pending().await,
            "output" => {
                context
                    .record_output(&json!({"message": job.args.message}))
                    .await
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
                Err(io::Error::other("work cancelled"))
            }
            "resumable" => {
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
                    .resumable_step("second", move || async move {
                        second_probe.increment_resumable_second()?;
                        if job.row.attempt == 1 {
                            Err(io::Error::other("fail second resumable step once"))
                        } else {
                            Ok(())
                        }
                    })
                    .await
                    .map_err(io::Error::other)?;
                Ok(WorkOutcome::Complete)
            }
            "transactional_complete" => {
                context
                    .metadata_set("transactional_completion", json!(true))
                    .await;
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
            "trace": state.trace,
        }))
    }
}

struct ConformanceErrorHandler(Arc<RuntimeProbe>);

#[async_trait]
impl ErrorHandler for ConformanceErrorHandler {
    async fn handle_error(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        _result: &WorkResult,
    ) -> Result<ErrorHandlerDecision, riverqueue::Error> {
        self.0
            .increment_error_handler_calls()
            .map_err(|error| riverqueue::Error::runtime(error.to_string()))?;
        Ok(ErrorHandlerDecision::Cancel)
    }
}

struct ProbeHook(Arc<RuntimeProbe>);

#[async_trait]
impl Hook for ProbeHook {
    async fn insert_begin(&self, _insert: &mut InsertContext) -> Result<(), riverqueue::Error> {
        self.0
            .add_trace("hook:insert_begin")
            .map_err(|error| riverqueue::Error::runtime(error.to_string()))
    }

    async fn periodic_jobs_start(&self, _jobs: &PeriodicJobs) -> Result<(), riverqueue::Error> {
        self.0
            .increment_periodic_starts()
            .map_err(|error| riverqueue::Error::runtime(error.to_string()))?;
        self.0
            .add_trace("hook:periodic_start")
            .map_err(|error| riverqueue::Error::runtime(error.to_string()))
    }

    async fn work_begin(
        &self,
        _context: &WorkContext,
        _job: &mut JobRow,
    ) -> Result<(), riverqueue::Error> {
        self.0
            .add_trace("hook:work_begin")
            .map_err(|error| riverqueue::Error::runtime(error.to_string()))
    }

    async fn work_end(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        _result: &WorkResult,
    ) -> Result<(), riverqueue::Error> {
        self.0
            .add_trace("hook:work_end")
            .map_err(|error| riverqueue::Error::runtime(error.to_string()))
    }
}

struct ProbeInsertMiddleware(Arc<RuntimeProbe>);

#[async_trait]
impl InsertMiddleware for ProbeInsertMiddleware {
    async fn before_insert(&self, _insert: &mut InsertContext) -> Result<(), riverqueue::Error> {
        self.0
            .add_trace("middleware:insert_before")
            .map_err(|error| riverqueue::Error::runtime(error.to_string()))
    }

    async fn after_insert(
        &self,
        _job: &JobRow,
        _unique_skipped_as_duplicate: bool,
    ) -> Result<(), riverqueue::Error> {
        self.0
            .add_trace("middleware:insert_after")
            .map_err(|error| riverqueue::Error::runtime(error.to_string()))
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
    fn hooks(&self) -> Vec<Arc<dyn Hook>> {
        vec![Arc::new(ProbeHook(Arc::clone(&self.0)))]
    }

    fn insert_middleware(&self) -> Vec<Arc<dyn InsertMiddleware>> {
        vec![Arc::new(ProbeInsertMiddleware(Arc::clone(&self.0)))]
    }

    fn work_middleware(&self) -> Vec<Arc<dyn WorkMiddleware>> {
        vec![Arc::new(ProbeWorkMiddleware(Arc::clone(&self.0)))]
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
    let mut adapter = match std::env::var("RIVER_CONFORMANCE_DATABASE_KIND")
        .as_deref()
        .unwrap_or("postgres")
    {
        "postgres" => {
            let options = PgConnectOptions::from_str(&database_url)?
                .application_name("river-conformance-rust");
            AdapterBackend::Postgres(Adapter {
                barriers: Arc::new(BarrierRegistry::default()),
                clock: None,
                pool: PgPoolOptions::new().connect_with(options).await?,
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
            Ok(request) if request.jsonrpc == "2.0" => adapter.respond(request).await,
            Ok(request) => Response::error(request.id, -32_600, "jsonrpc must be 2.0".to_owned()),
            Err(error) => Response::error(Value::Null, -32_700, error.to_string()),
        };
        serde_json::to_writer(&mut stdout, &response)?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }
    match adapter {
        AdapterBackend::Postgres(mut adapter) => {
            if let Some(running) = adapter.running.take() {
                running.handle.shutdown_now().await?;
            }
        }
        AdapterBackend::Sqlite(mut adapter) => {
            if let Some(running) = adapter.running.take() {
                running.handle.shutdown_now().await?;
            }
        }
    }
    Ok(())
}

impl AdapterBackend {
    async fn respond(&mut self, request: Request) -> Response {
        match self {
            Self::Postgres(adapter) => adapter.respond(request).await,
            Self::Sqlite(adapter) => adapter.respond(request).await,
        }
    }
}

impl Adapter {
    async fn respond(&mut self, request: Request) -> Response {
        let result = self.handle(&request.method, request.params).await;
        match result {
            Ok(result) => Response::success(request.id, result),
            Err(error) => Response::error(request.id, -32_000, error.to_string()),
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn handle(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        match method {
            "handshake" => Ok(json!({
                "adapter_version": ADAPTER_VERSION,
                "backend": "postgres",
                "capabilities": CAPABILITIES,
                "implementation": "rust",
                "implementation_version": env!("CARGO_PKG_VERSION"),
                "methods": ADAPTER_METHODS,
                "migration_lines": {MIGRATION_LINE_MAIN: MIGRATION_VERSION_LATEST},
                "profile": "postgres-full-v1",
                "protocol_revision": PROTOCOL_REVISION,
            })),
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
            "retry_delay" => {
                let now = self
                    .clock
                    .ok_or("clock_set is required before retry_delay")?;
                let error_count = usize::try_from(required_i64(&params, "error_count")?)?;
                if error_count == 0 {
                    return Err("error_count must be positive".into());
                }
                let row = retry_row(required_i64(&params, "job_id")?, now, error_count - 1);
                let delay = DefaultRetryPolicy::with_seed(self.rng_seed).next_retry(
                    &row,
                    "conformance retry",
                    now,
                );
                Ok(json!({"delay_ns": u64::try_from(delay.as_nanos())?}))
            }
            "unique_key" => {
                let params: UniqueKeyParams = serde_json::from_value(params)?;
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
                Ok(json!({"sha256": hex(&key), "state_mask": opts.state_bitmask()}))
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
                    .insert_with(params.args(), params.opts.into_opts())
                    .await?;
                Ok(normalize_job(&result.job.row))
            }
            "insert_many" => {
                let jobs = insert_many_params(&params)?;
                let results = self.client()?.insert_many_with(jobs).await?;
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
                        .insert_with(
                            ConformanceArgs {
                                behavior: String::new(),
                                duration_ms: 0,
                                message: format!("benchmark-enqueue-{index}"),
                            },
                            InsertOpts::default(),
                        )
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
                let count = self.client()?.insert_many_fast_with(jobs).await?;
                Ok(json!({"count": count}))
            }
            "get" => {
                let client = self.client_for_schema(
                    params
                        .get("schema")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                )?;
                let row = client.job_get(required_i64(&params, "id")?).await?;
                Ok(normalize_job(&row))
            }
            "list" => {
                let list = list_params(&params)?;
                let rows = self.client()?.job_list(&list).await?;
                normalize_job_list(&rows, &list)
            }
            "cancel" => {
                let row = self
                    .client()?
                    .job_cancel(required_i64(&params, "id")?)
                    .await?;
                Ok(normalize_job(&row))
            }
            "delete" => {
                let row = self
                    .client()?
                    .job_delete(required_i64(&params, "id")?)
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
                let rows = self.client()?.job_delete_many(&delete).await?;
                Ok(json!({"jobs": rows.iter().map(normalize_job).collect::<Vec<_>>() }))
            }
            "retry" => {
                let row = self
                    .client()?
                    .job_retry(required_i64(&params, "id")?)
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
                    .job_update(id, job_update_params(metadata, output))
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
                self.client()?.request_resign().await?;
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
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'river-conformance-rust' AND query LIKE 'LISTEN %'",
                )
                .fetch_one(&self.pool)
                .await?;
                Ok(json!({"count": count}))
            }
            "connection_count" => {
                let count = sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'river-conformance-rust'",
                )
                .fetch_one(&self.pool)
                .await?;
                Ok(json!({"count": count}))
            }
            "fault_disconnect_listeners" => {
                let count = sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'river-conformance-rust' AND query LIKE 'LISTEN %' AND pid != pg_backend_pid()) AS terminated",
                )
                .fetch_one(&self.pool)
                .await?;
                Ok(json!({"count": count}))
            }
            "fault_disconnect_application" => {
                let application_name = required_string(&params, "application_name")?;
                if !matches!(
                    application_name.as_str(),
                    "river-conformance-go" | "river-conformance-rust"
                ) {
                    return Err("unsupported conformance application_name".into());
                }
                let count = sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1 AND pid != pg_backend_pid()) AS terminated",
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
                    return Err("state must be completed or discarded".into());
                }
                let metadata = params.get("metadata").cloned().unwrap_or_else(|| json!({}));
                let result = sqlx::query(
                    r#"UPDATE river_job
                       SET errors = CASE WHEN $2 = 'discarded'
                               THEN array_append(errors, '{"at":"2026-02-03T04:05:06.789Z","attempt":1,"error":"external discard","trace":"external trace"}'::jsonb)
                               ELSE errors END,
                           finalized_at = '2026-02-03T04:05:06.789Z'::timestamptz,
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
                    return Err("running job not found".into());
                }
                Ok(normalize_job(&self.client()?.job_get(id).await?))
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
                Ok(normalize_job(&self.client()?.job_get(id).await?))
            }
            "raw_insert_full_row" => {
                let id = sqlx::query_scalar::<_, i64>(
                    r#"INSERT INTO river_job (
                        args, attempt, attempted_at, attempted_by, created_at, errors,
                        finalized_at, kind, max_attempts, metadata, priority, queue,
                        scheduled_at, state, tags, unique_key, unique_states
                    ) VALUES (
                        '{"nested":{"enabled":true},"values":[1,"two",null]}'::jsonb,
                        3, '2026-01-02T03:04:06.123456Z', ARRAY['go-client','rust-client'],
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
                Ok(normalize_job(&self.client()?.job_get(id).await?))
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
                let mut maintenance = MaintenanceConfig::default();
                if let Some(milliseconds) = optional_i64(&params, "elect_interval_ms") {
                    maintenance = maintenance.with_elect_interval(duration_millis(milliseconds)?);
                }
                if let Some(milliseconds) = optional_i64(&params, "rescue_after_ms") {
                    maintenance = maintenance.with_rescue_after(duration_millis(milliseconds)?);
                }
                if let Some(milliseconds) = optional_i64(&params, "rescuer_interval_ms") {
                    maintenance = maintenance.with_rescuer_interval(duration_millis(milliseconds)?);
                }
                if let Some(milliseconds) = optional_i64(&params, "scheduler_interval_ms") {
                    maintenance =
                        maintenance.with_scheduler_interval(duration_millis(milliseconds)?);
                }
                let mut builder =
                    Client::builder(PostgresDatabase::new(self.pool.clone()).schema(schema))
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
                if error_handler_cancel {
                    builder = builder.error_handler(ConformanceErrorHandler(Arc::clone(&probe)));
                }
                if let Some(milliseconds) = optional_i64(&params, "job_stuck_threshold_ms") {
                    builder = builder.job_stuck_threshold(duration_millis(milliseconds)?);
                }
                if let Some(milliseconds) = optional_i64(&params, "job_timeout_ms") {
                    builder = builder.job_timeout(Some(duration_millis(milliseconds)?));
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
                let running = self.running.take().ok_or("client is not running")?;
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
                let handle = client.start()?;
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
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                let row = client
                    .insert_tx_with(transaction, insert.args(), insert.opts.into_opts())
                    .await?;
                Ok(normalize_job(&row.job.row))
            }
            "tx_insert_many" | "tx_insert_many_fast" => {
                let handle = required_string(&params, "handle")?;
                let jobs = insert_many_params(params.get("jobs").ok_or("missing jobs")?)?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                if method == "tx_insert_many_fast" {
                    let count = client.insert_many_fast_tx_with(transaction, jobs).await?;
                    Ok(json!({"count": count}))
                } else {
                    let results = client.insert_many_tx_with(transaction, jobs).await?;
                    Ok(normalize_insert_many_results(&results))
                }
            }
            "tx_get" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                Ok(normalize_job(&client.job_get_tx(transaction, id).await?))
            }
            "tx_cancel" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                Ok(normalize_job(&client.job_cancel_tx(transaction, id).await?))
            }
            "tx_delete" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                Ok(normalize_job(&client.job_delete_tx(transaction, id).await?))
            }
            "tx_retry" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                Ok(normalize_job(&client.job_retry_tx(transaction, id).await?))
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
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                let row = client
                    .job_update_tx(transaction, id, job_update_params(metadata, output))
                    .await?;
                Ok(normalize_job(&row))
            }
            "tx_list" => {
                let handle = required_string(&params, "handle")?;
                let list = list_params(&params)?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                let rows = client.job_list_tx(transaction, &list).await?;
                normalize_job_list(&rows, &list)
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
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                let rows = client.job_delete_many_tx(transaction, &delete).await?;
                Ok(json!({"jobs": rows.iter().map(normalize_job).collect::<Vec<_>>() }))
            }
            "tx_queue_get" => {
                let handle = required_string(&params, "handle")?;
                let name = required_string(&params, "name")?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                Ok(normalize_queue(
                    &client.queue_get_tx(transaction, &name).await?,
                ))
            }
            "tx_queue_list" => {
                let handle = required_string(&params, "handle")?;
                let limit = optional_i64(&params, "limit").unwrap_or(100);
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
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
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
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
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                Ok(normalize_queue(
                    &client.queue_update_tx(transaction, &name, metadata).await?,
                ))
            }
            "tx_fail" => {
                let handle = required_string(&params, "handle")?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                sqlx::query("SELECT 1 / 0")
                    .execute(&mut **transaction)
                    .await?;
                Ok(json!({}))
            }
            "tx_commit" | "tx_rollback" => {
                let handle = required_string(&params, "handle")?;
                let transaction = self
                    .transactions
                    .remove(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                if method == "tx_commit" {
                    transaction.commit().await?;
                } else {
                    transaction.rollback().await?;
                }
                Ok(json!({}))
            }
            _ => Err(format!("method not found: {method}").into()),
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
    async fn respond(&mut self, request: Request) -> Response {
        let result = self.handle(&request.method, request.params).await;
        match result {
            Ok(result) => Response::success(request.id, result),
            Err(error) => Response::error(request.id, -32_000, error.to_string()),
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
                let (methods, capabilities) = if self.profile == "sqlite-runtime-v1" {
                    (SQLITE_RUNTIME_METHODS, SQLITE_RUNTIME_CAPABILITIES)
                } else {
                    (SQLITE_ADAPTER_METHODS, SQLITE_CAPABILITIES)
                };
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
                    return Err("SQLite conformance does not support custom schemas".into());
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
            "retry_delay" => {
                let now = self
                    .clock
                    .ok_or("clock_set is required before retry_delay")?;
                let error_count = usize::try_from(required_i64(&params, "error_count")?)?;
                if error_count == 0 {
                    return Err("error_count must be positive".into());
                }
                let row = retry_row(required_i64(&params, "job_id")?, now, error_count - 1);
                let delay = DefaultRetryPolicy::with_seed(self.rng_seed).next_retry(
                    &row,
                    "conformance retry",
                    now,
                );
                Ok(json!({"delay_ns": u64::try_from(delay.as_nanos())?}))
            }
            "unique_key" => {
                let params: UniqueKeyParams = serde_json::from_value(params)?;
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
                Ok(json!({"sha256": hex(&key), "state_mask": opts.state_bitmask()}))
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
                    return Err("SQLite conformance does not support custom schemas".into());
                }
                let result = self
                    .client()?
                    .insert_with(params.args(), params.opts.into_opts())
                    .await?;
                Ok(normalize_job(&result.job.row))
            }
            "insert_many" => {
                let jobs = insert_many_params(&params)?;
                let results = self.client()?.insert_many_with(jobs).await?;
                Ok(normalize_insert_many_results(&results))
            }
            "insert_many_fast" => {
                let params = params.get("jobs").cloned().ok_or("missing jobs")?;
                let params: Vec<InsertParams> = serde_json::from_value(params)?;
                let jobs = params
                    .into_iter()
                    .map(|params| (params.args(), params.opts.into_opts()))
                    .collect::<Vec<_>>();
                let count = self.client()?.insert_many_fast_with(jobs).await?;
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
                Ok(normalize_job(&self.client()?.job_get(id).await?))
            }
            "get" => {
                if params
                    .get("schema")
                    .and_then(Value::as_str)
                    .is_some_and(|schema| !schema.is_empty())
                {
                    return Err("SQLite conformance does not support custom schemas".into());
                }
                let row = self.client()?.job_get(required_i64(&params, "id")?).await?;
                Ok(normalize_job(&row))
            }
            "list" => {
                let list = list_params(&params)?;
                let rows = self.client()?.job_list(&list).await?;
                normalize_job_list(&rows, &list)
            }
            "cancel" => {
                let row = self
                    .client()?
                    .job_cancel(required_i64(&params, "id")?)
                    .await?;
                Ok(normalize_job(&row))
            }
            "delete" => {
                let row = self
                    .client()?
                    .job_delete(required_i64(&params, "id")?)
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
                let rows = self.client()?.job_delete_many(&delete).await?;
                Ok(json!({"jobs": rows.iter().map(normalize_job).collect::<Vec<_>>() }))
            }
            "retry" => {
                let row = self
                    .client()?
                    .job_retry(required_i64(&params, "id")?)
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
                    .job_update(id, job_update_params(metadata, output))
                    .await?;
                Ok(normalize_job(&row))
            }
            "raw_finalize" => {
                let id = required_i64(&params, "id")?;
                let state = required_string(&params, "state")?;
                if !matches!(state.as_str(), "completed" | "discarded") {
                    return Err("state must be completed or discarded".into());
                }
                let metadata = params.get("metadata").cloned().unwrap_or_else(|| json!({}));
                let result = sqlx::query(
                    r#"UPDATE river_job
                       SET errors = CASE WHEN ?2 = 'discarded'
                               THEN jsonb(json_insert(json(coalesce(errors, jsonb('[]'))), '$[#]', json('{"at":"2026-02-03T04:05:06.789Z","attempt":1,"error":"external discard","trace":"external trace"}')))
                               ELSE errors END,
                           finalized_at = '2026-02-03 04:05:06.789',
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
                    return Err("running job not found".into());
                }
                Ok(normalize_job(&self.client()?.job_get(id).await?))
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
                self.client()?.request_resign().await?;
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
                let mut maintenance = MaintenanceConfig::default();
                if let Some(milliseconds) = optional_i64(&params, "elect_interval_ms") {
                    maintenance = maintenance.with_elect_interval(duration_millis(milliseconds)?);
                }
                if let Some(milliseconds) = optional_i64(&params, "rescue_after_ms") {
                    maintenance = maintenance.with_rescue_after(duration_millis(milliseconds)?);
                }
                if let Some(milliseconds) = optional_i64(&params, "rescuer_interval_ms") {
                    maintenance = maintenance.with_rescuer_interval(duration_millis(milliseconds)?);
                }
                if let Some(milliseconds) = optional_i64(&params, "scheduler_interval_ms") {
                    maintenance =
                        maintenance.with_scheduler_interval(duration_millis(milliseconds)?);
                }
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
                if error_handler_cancel {
                    builder = builder.error_handler(ConformanceErrorHandler(Arc::clone(&probe)));
                }
                if let Some(milliseconds) = optional_i64(&params, "job_stuck_threshold_ms") {
                    builder = builder.job_stuck_threshold(duration_millis(milliseconds)?);
                }
                if let Some(milliseconds) = optional_i64(&params, "job_timeout_ms") {
                    builder = builder.job_timeout(Some(duration_millis(milliseconds)?));
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
                let running = self.running.take().ok_or("client is not running")?;
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
                let handle = client.start()?;
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
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                let row = client
                    .insert_tx_with(transaction, insert.args(), insert.opts.into_opts())
                    .await?;
                Ok(normalize_job(&row.job.row))
            }
            "tx_insert_many" | "tx_insert_many_fast" => {
                let handle = required_string(&params, "handle")?;
                let jobs = insert_many_params(params.get("jobs").ok_or("missing jobs")?)?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                if method == "tx_insert_many_fast" {
                    let count = client.insert_many_fast_tx_with(transaction, jobs).await?;
                    Ok(json!({"count": count}))
                } else {
                    let results = client.insert_many_tx_with(transaction, jobs).await?;
                    Ok(normalize_insert_many_results(&results))
                }
            }
            "tx_get" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                Ok(normalize_job(&client.job_get_tx(transaction, id).await?))
            }
            "tx_cancel" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                Ok(normalize_job(&client.job_cancel_tx(transaction, id).await?))
            }
            "tx_delete" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                Ok(normalize_job(&client.job_delete_tx(transaction, id).await?))
            }
            "tx_retry" => {
                let handle = required_string(&params, "handle")?;
                let id = required_i64(&params, "id")?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                Ok(normalize_job(&client.job_retry_tx(transaction, id).await?))
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
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                let row = client
                    .job_update_tx(transaction, id, job_update_params(metadata, output))
                    .await?;
                Ok(normalize_job(&row))
            }
            "tx_list" => {
                let handle = required_string(&params, "handle")?;
                let list = list_params(&params)?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                let rows = client.job_list_tx(transaction, &list).await?;
                normalize_job_list(&rows, &list)
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
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                let rows = client.job_delete_many_tx(transaction, &delete).await?;
                Ok(json!({"jobs": rows.iter().map(normalize_job).collect::<Vec<_>>() }))
            }
            "tx_queue_get" => {
                let handle = required_string(&params, "handle")?;
                let name = required_string(&params, "name")?;
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                Ok(normalize_queue(
                    &client.queue_get_tx(transaction, &name).await?,
                ))
            }
            "tx_queue_list" => {
                let handle = required_string(&params, "handle")?;
                let limit = optional_i64(&params, "limit").unwrap_or(100);
                let client = self.client()?;
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
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
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
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
                let transaction = self
                    .transactions
                    .get_mut(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                Ok(normalize_queue(
                    &client.queue_update_tx(transaction, &name, metadata).await?,
                ))
            }
            "tx_commit" | "tx_rollback" => {
                let handle = required_string(&params, "handle")?;
                let transaction = self
                    .transactions
                    .remove(&handle)
                    .ok_or_else(|| format!("transaction {handle:?} not found"))?;
                if method == "tx_commit" {
                    transaction.commit().await?;
                } else {
                    transaction.rollback().await?;
                }
                Ok(json!({}))
            }
            _ => Err(format!("method not found for SQLite profile: {method}").into()),
        }
    }

    fn client(&self) -> Result<Client, riverqueue::Error> {
        if let Some(running) = &self.running {
            return Ok(running.client.clone());
        }
        Client::builder(SqliteDatabase::new(self.pool.clone())).build()
    }
}

fn parse_sqlite_time(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .map(|value| value.and_utc())
}

fn schema_name(schema: Option<&str>) -> Result<SchemaName, riverqueue::Error> {
    SchemaName::new(schema.unwrap_or_default())
        .map_err(|error| riverqueue::Error::invalid_job(error.to_string()))
}

fn retry_row(id: i64, now: DateTime<Utc>, previous_errors: usize) -> JobRow {
    let mut row = JobRow::new(id, "conformance_echo", json!({}), now);
    row.attempt = i16::try_from(previous_errors.saturating_add(1)).unwrap_or(i16::MAX);
    row.attempted_at = Some(now);
    row.attempted_by = vec!["conformance".to_owned()];
    row.errors = vec![AttemptError::new(now, 1, "previous failure"); previous_errors];
    row.max_attempts = 1_000;
    row.metadata = Map::new();
    row.state = JobState::Retryable;
    row
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
        let row = client.job_get(id).await?;
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

fn normalize_job_list(
    rows: &[JobRow],
    params: &JobListParams,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let cursor = rows
        .last()
        .map(|row| JobListCursor::from_job(row, params))
        .transpose()
        .map_err(io::Error::other)?
        .map(|cursor| cursor.encode());
    Ok(json!({
        "cursor": cursor,
        "jobs": rows.iter().map(normalize_job).collect::<Vec<_>>(),
    }))
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
