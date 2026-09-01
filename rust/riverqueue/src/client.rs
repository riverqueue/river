//! Database-backed client, insertion, and worker runtime.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

#[cfg(feature = "postgres")]
use chrono::SecondsFormat;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::AssertSqlSafe;
#[cfg(feature = "sqlite")]
use sqlx::SqlitePool;
#[cfg(feature = "postgres")]
use sqlx::{
    Executor, FromRow, PgConnection, PgPool, Postgres, Row,
    postgres::{PgListener, PgRow},
    types::Json,
};
#[cfg(feature = "postgres")]
use std::fmt::Write as _;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot, watch},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info_span, warn};

use riverqueue_internal::{
    CompletionAction, CompletionParams, DatabaseConnection as PilotDatabaseConnection, FetchParams,
    JobInsertParams as PilotJobInsertParams,
};
use riverqueue_internal::{
    DatabaseConfig as PilotDatabaseConfig, DatabasePool as PilotDatabasePool, NoopPilot, Pilot,
};

use crate::{
    AttemptError, BoxError, DefaultRetryPolicy, Error, ErrorHandler, ErrorHandlerDecision, Event,
    EventKind, EventReceiver, ExtensionClaimParams, ExtensionInsertParams, FETCH_COOLDOWN_DEFAULT,
    FETCH_COOLDOWN_MIN, FETCH_POLL_INTERVAL_DEFAULT, Hook, InsertBatch, InsertBatchResult,
    InsertContext, InsertMiddleware, InsertOpts, InsertParams, InsertResult,
    JOB_STUCK_THRESHOLD_DEFAULT, JOB_TIMEOUT_DEFAULT, Job, JobArgs, JobEventKind, JobRow, JobState,
    JobStatistics, MAX_ATTEMPTS_DEFAULT, Metric, Plugin, QUEUE_NUM_WORKERS_MAX, QueueEventKind,
    RawInsertResult, RetryPolicy, SchemaName, SubscribeConfig, WorkContext, WorkError,
    WorkMiddleware, WorkOutcome, WorkResult, WorkerRegistry, WorkerTimeout,
    database::{
        Database, DatabaseExecutor, DatabaseKind, DatabasePool, DatabaseTransactionExecutor,
        ErasedExecutor, ExecutorInner, IntoDatabase,
    },
    periodic::{PeriodicInsert, PeriodicJob, PeriodicJobs},
    unique::build_unique_key_parts,
};

const ATTEMPTED_BY_MAX: i32 = 100;
const EVENT_BUFFER_CAPACITY: usize = 10_000;
const PENDING_CANCELLATION_LIMIT: usize = 10_000;
const PENDING_CANCELLATION_RETENTION: Duration = Duration::from_mins(1);
// Large queues otherwise become limited by a single PostgreSQL claim round trip.
// Concurrent `SKIP LOCKED` claims safely divide the available worker slots.
const PARALLEL_FETCH_MINIMUM: usize = 1_000;
const QUEUE_CONFIG_POLL_INTERVAL: Duration = Duration::from_secs(2);
const QUEUE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Leader-owned maintenance timing and retention settings.
#[derive(Clone, Debug)]
pub struct MaintenanceConfig {
    /// Retention for cancelled jobs; `None` disables deletion.
    pub(crate) cancelled_job_retention: Option<Duration>,
    /// Retention for completed jobs; `None` disables deletion.
    pub(crate) completed_job_retention: Option<Duration>,
    /// Retention for discarded jobs; `None` disables deletion.
    pub(crate) discarded_job_retention: Option<Duration>,
    /// Leader election and renewal interval.
    pub(crate) elect_interval: Duration,
    /// Job cleaner interval.
    pub(crate) job_cleaner_interval: Duration,
    /// Timeout for each job-cleaner deletion statement.
    pub(crate) job_cleaner_timeout: Duration,
    /// Age at which running jobs may be rescued.
    pub(crate) rescue_after: Duration,
    /// Stuck-job rescuer interval.
    pub(crate) rescuer_interval: Duration,
    /// Retention for inactive queue records.
    pub(crate) queue_retention: Duration,
    /// Inactive queue cleaner interval.
    pub(crate) queue_cleaner_interval: Duration,
    /// Due-job scheduler interval.
    pub(crate) scheduler_interval: Duration,
}

macro_rules! maintenance_option {
    ($getter:ident, $setter:ident, $field:ident) => {
        #[doc = concat!("Returns `", stringify!($field), "`.")]
        #[must_use]
        pub const fn $getter(&self) -> Option<Duration> {
            self.$field
        }

        #[doc = concat!("Sets `", stringify!($field), "`.")]
        #[must_use]
        pub const fn $setter(mut self, value: Option<Duration>) -> Self {
            self.$field = value;
            self
        }
    };
}

macro_rules! maintenance_duration {
    ($getter:ident, $setter:ident, $field:ident) => {
        #[doc = concat!("Returns `", stringify!($field), "`.")]
        #[must_use]
        pub const fn $getter(&self) -> Duration {
            self.$field
        }

        #[doc = concat!("Sets `", stringify!($field), "`.")]
        #[must_use]
        pub const fn $setter(mut self, value: Duration) -> Self {
            self.$field = value;
            self
        }
    };
}

impl MaintenanceConfig {
    maintenance_option!(
        cancelled_job_retention,
        with_cancelled_job_retention,
        cancelled_job_retention
    );
    maintenance_option!(
        completed_job_retention,
        with_completed_job_retention,
        completed_job_retention
    );
    maintenance_option!(
        discarded_job_retention,
        with_discarded_job_retention,
        discarded_job_retention
    );
    maintenance_duration!(elect_interval, with_elect_interval, elect_interval);
    maintenance_duration!(
        job_cleaner_interval,
        with_job_cleaner_interval,
        job_cleaner_interval
    );
    maintenance_duration!(
        job_cleaner_timeout,
        with_job_cleaner_timeout,
        job_cleaner_timeout
    );
    maintenance_duration!(rescue_after, with_rescue_after, rescue_after);
    maintenance_duration!(rescuer_interval, with_rescuer_interval, rescuer_interval);
    maintenance_duration!(queue_retention, with_queue_retention, queue_retention);
    maintenance_duration!(
        queue_cleaner_interval,
        with_queue_cleaner_interval,
        queue_cleaner_interval
    );
    maintenance_duration!(
        scheduler_interval,
        with_scheduler_interval,
        scheduler_interval
    );
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            cancelled_job_retention: Some(Duration::from_hours(24)),
            completed_job_retention: Some(Duration::from_hours(24)),
            discarded_job_retention: Some(Duration::from_hours(168)),
            elect_interval: Duration::from_secs(5),
            job_cleaner_interval: Duration::from_secs(30),
            job_cleaner_timeout: Duration::from_secs(30),
            rescue_after: Duration::from_hours(1),
            rescuer_interval: Duration::from_secs(30),
            queue_retention: Duration::from_hours(24),
            queue_cleaner_interval: Duration::from_hours(1),
            scheduler_interval: Duration::from_secs(5),
        }
    }
}

/// Queue-specific worker settings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueConfig {
    /// Minimum delay between fetches.
    pub(crate) fetch_cooldown: Duration,
    /// Fallback polling interval.
    pub(crate) fetch_poll_interval: Duration,
    /// Maximum jobs run concurrently by this client.
    pub(crate) max_workers: usize,
}

impl QueueConfig {
    /// Creates queue configuration with River's timing defaults.
    #[must_use]
    pub const fn new(max_workers: usize) -> Self {
        Self {
            fetch_cooldown: FETCH_COOLDOWN_DEFAULT,
            fetch_poll_interval: FETCH_POLL_INTERVAL_DEFAULT,
            max_workers,
        }
    }

    /// Returns the minimum delay between fetches.
    #[must_use]
    pub const fn fetch_cooldown(&self) -> Duration {
        self.fetch_cooldown
    }

    /// Returns the fallback polling interval.
    #[must_use]
    pub const fn fetch_poll_interval(&self) -> Duration {
        self.fetch_poll_interval
    }

    /// Returns the maximum jobs run concurrently.
    #[must_use]
    pub const fn max_workers(&self) -> usize {
        self.max_workers
    }

    /// Sets the minimum delay between fetches.
    #[must_use]
    pub const fn with_fetch_cooldown(mut self, interval: Duration) -> Self {
        self.fetch_cooldown = interval;
        self
    }

    /// Sets the fallback polling interval.
    #[must_use]
    pub const fn with_fetch_poll_interval(mut self, interval: Duration) -> Self {
        self.fetch_poll_interval = interval;
        self
    }

    /// Sets the maximum jobs run concurrently.
    #[must_use]
    pub const fn with_max_workers(mut self, maximum: usize) -> Self {
        self.max_workers = maximum;
        self
    }

    fn validate(&self, name: &str) -> Result<(), Error> {
        validate_queue(name)?;
        if !(1..=QUEUE_NUM_WORKERS_MAX).contains(&self.max_workers) {
            return Err(Error::configuration(format!(
                "queue {name:?} max_workers must be between 1 and {QUEUE_NUM_WORKERS_MAX}"
            )));
        }
        if self.fetch_cooldown < FETCH_COOLDOWN_MIN {
            return Err(Error::configuration(
                "fetch cooldown must be at least one millisecond".to_owned(),
            ));
        }
        if self.fetch_poll_interval < self.fetch_cooldown {
            return Err(Error::configuration(
                "fetch poll interval cannot be shorter than fetch cooldown".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Builder for a River client.
pub struct ClientBuilder {
    database: Database,
    default_max_attempts: i16,
    error_handler: Option<Arc<dyn ErrorHandler>>,
    hooks: Vec<Arc<dyn Hook>>,
    id: String,
    job_stuck_threshold: Duration,
    job_timeout: Option<Duration>,
    maintenance: MaintenanceConfig,
    insert_middleware: Vec<Arc<dyn InsertMiddleware>>,
    periodic_jobs: Vec<PeriodicJob>,
    pilot: Arc<dyn Pilot>,
    poll_only: bool,
    queues: HashMap<String, QueueConfig>,
    retry_policy: Arc<dyn RetryPolicy>,
    allow_legacy_job_kinds: bool,
    allow_unregistered_job_kinds: bool,
    soft_stop_timeout: Option<Duration>,
    pub(crate) workers: WorkerRegistry,
    work_middleware: Vec<Arc<dyn WorkMiddleware>>,
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientBuilder")
            .field("database_kind", &self.database.kind())
            .field("id", &self.id)
            .field("queue_count", &self.queues.len())
            .field("worker_kinds", &self.workers.kinds())
            .field("hook_count", &self.hooks.len())
            .field("periodic_job_count", &self.periodic_jobs.len())
            .finish_non_exhaustive()
    }
}

impl ClientBuilder {
    /// Sets the maximum attempts used by [`Client::insert`] when the job type
    /// does not override it.
    #[must_use]
    pub fn default_max_attempts(mut self, maximum: i16) -> Self {
        self.default_max_attempts = maximum;
        self
    }

    /// Installs a worker error and stuck-task handler.
    #[must_use]
    pub fn error_handler<H: ErrorHandler>(mut self, handler: H) -> Self {
        self.error_handler = Some(Arc::new(handler));
        self
    }

    /// Adds an ordered lifecycle hook.
    #[must_use]
    pub fn hook<H: Hook>(mut self, hook: H) -> Self {
        self.hooks.push(Arc::new(hook));
        self
    }

    /// Sets a stable client identifier.
    #[must_use]
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// Sets the delay between cancellation and stuck classification.
    #[must_use]
    pub fn job_stuck_threshold(mut self, threshold: Duration) -> Self {
        self.job_stuck_threshold = threshold;
        self
    }

    /// Sets the per-job timeout. `None` disables timeouts.
    #[must_use]
    pub fn job_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.job_timeout = timeout;
        self
    }

    /// Configures leader-owned maintenance services.
    #[must_use]
    pub fn maintenance(mut self, maintenance: MaintenanceConfig) -> Self {
        self.maintenance = maintenance;
        self
    }

    /// Adds ordered insertion middleware.
    #[must_use]
    pub fn insert_middleware<M: InsertMiddleware>(mut self, middleware: M) -> Self {
        self.insert_middleware.push(Arc::new(middleware));
        self
    }

    /// Adds a periodic job to the initial client configuration.
    #[must_use]
    pub fn periodic_job(mut self, job: PeriodicJob) -> Self {
        self.periodic_jobs.push(job);
        self
    }

    /// Installs an exact-version internal pilot from a matched companion crate.
    #[doc(hidden)]
    #[must_use]
    pub fn pilot<P: Pilot>(mut self, pilot: P) -> Self {
        self.pilot = Arc::new(pilot);
        self
    }

    /// Disables the backend notification channel or outbox poller while
    /// retaining queue fetch polling.
    #[must_use]
    pub fn without_notifications(mut self) -> Self {
        self.poll_only = true;
        self
    }

    /// Installs all extension points contributed by a plugin.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn plugin<P: Plugin>(mut self, plugin: P) -> Self {
        self.hooks.extend(plugin.hooks());
        self.insert_middleware.extend(plugin.insert_middleware());
        self.work_middleware.extend(plugin.work_middleware());
        self
    }

    /// Adds or replaces a queue.
    #[must_use]
    pub fn queue(mut self, name: impl Into<String>, config: QueueConfig) -> Self {
        self.queues.insert(name.into(), config);
        self
    }

    /// Replaces River's default retry policy.
    #[must_use]
    pub fn retry_policy<P: RetryPolicy>(mut self, retry_policy: P) -> Self {
        self.retry_policy = Arc::new(retry_policy);
        self
    }

    /// Temporarily permits legacy job kinds that do not match River's format.
    #[must_use]
    pub fn allow_legacy_job_kinds(mut self) -> Self {
        self.allow_legacy_job_kinds = true;
        self
    }

    /// Allows inserting kinds with no worker in this client's registry.
    /// Insert-only clients already permit every kind.
    #[must_use]
    pub fn allow_unregistered_job_kinds(mut self) -> Self {
        self.allow_unregistered_job_kinds = true;
        self
    }

    /// Escalates graceful shutdown to job cancellation after this duration.
    /// `None` waits indefinitely.
    #[must_use]
    pub fn soft_stop_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.soft_stop_timeout = timeout;
        self
    }

    /// Installs a typed worker registry.
    #[must_use]
    pub fn workers(mut self, workers: WorkerRegistry) -> Self {
        self.workers = workers;
        self
    }

    /// Adds ordered worker middleware.
    #[must_use]
    pub fn work_middleware<M: WorkMiddleware>(mut self, middleware: M) -> Self {
        self.work_middleware.push(Arc::new(middleware));
        self
    }

    /// Validates configuration and builds the client.
    #[allow(
        clippy::too_many_lines,
        reason = "central validation keeps builder failures deterministic before allocating runtime state"
    )]
    pub fn build(self) -> Result<Client, Error> {
        if self.default_max_attempts < 1 {
            return Err(Error::configuration(
                "default max attempts must be greater than zero".to_owned(),
            ));
        }
        if self.id.is_empty() || self.id.len() > 100 {
            return Err(Error::configuration(
                "client ID must contain between 1 and 100 bytes".to_owned(),
            ));
        }
        if self
            .soft_stop_timeout
            .is_some_and(|timeout| timeout.is_zero())
        {
            return Err(Error::configuration(
                "soft stop timeout must be positive when configured".to_owned(),
            ));
        }
        for (name, config) in &self.queues {
            config.validate(name)?;
        }
        for (name, interval) in [
            ("elect interval", self.maintenance.elect_interval),
            (
                "job cleaner interval",
                self.maintenance.job_cleaner_interval,
            ),
            ("job cleaner timeout", self.maintenance.job_cleaner_timeout),
            ("rescue after", self.maintenance.rescue_after),
            ("rescuer interval", self.maintenance.rescuer_interval),
            (
                "queue cleaner interval",
                self.maintenance.queue_cleaner_interval,
            ),
            ("queue retention", self.maintenance.queue_retention),
            ("scheduler interval", self.maintenance.scheduler_interval),
        ] {
            if interval.is_zero() {
                return Err(Error::configuration(format!("{name} must be positive")));
            }
        }
        #[cfg(feature = "postgres")]
        let reindex = self.database.postgres_reindex();
        #[cfg(feature = "postgres")]
        if reindex.is_some_and(|config| config.timeout().is_zero()) {
            return Err(Error::configuration(
                "reindexer timeout must be positive".to_owned(),
            ));
        }
        #[cfg(feature = "postgres")]
        if matches!(
            reindex.map(crate::database::PostgresReindexConfig::schedule),
            Some(crate::database::PostgresReindexSchedule::Interval(interval)) if interval.is_zero()
        ) {
            return Err(Error::configuration(
                "reindexer interval must be positive".to_owned(),
            ));
        }
        #[cfg(feature = "postgres")]
        for index_name in reindex
            .into_iter()
            .flat_map(crate::database::PostgresReindexConfig::index_names)
        {
            validate_identifier(index_name, "reindexer index")?;
        }
        if !self.queues.is_empty() && self.workers.kinds().is_empty() {
            return Err(Error::configuration(
                "workers must be configured when queues are configured".to_owned(),
            ));
        }
        for metadata_key in self.pilot.job_cleaner_metadata_exclusions() {
            validate_metadata_key(metadata_key)?;
        }

        let periodic_jobs = PeriodicJobs::from_jobs(self.periodic_jobs)?;
        #[cfg(feature = "postgres")]
        let schema = self
            .database
            .postgres_schema()
            .cloned()
            .unwrap_or_else(SchemaName::current);
        let (events, _) = broadcast::channel(EVENT_BUFFER_CAPACITY);
        let (queue_changes, _) = watch::channel(0_u64);
        let (queue_notifications, _) = broadcast::channel(1_024);
        Ok(Client {
            inner: Arc::new(ClientInner {
                completion_sender: Mutex::new(None),
                database: self.database,
                default_max_attempts: self.default_max_attempts,
                error_handler: self.error_handler,
                events,
                fetch_registration_windows: AtomicU64::new(0),
                hooks: self.hooks,
                id: self.id,
                job_stuck_threshold: self.job_stuck_threshold,
                job_timeout: self.job_timeout,
                maintenance: self.maintenance,
                insert_middleware: self.insert_middleware,
                periodic_jobs,
                pending_cancellations: Mutex::new(HashMap::new()),
                pilot: self.pilot,
                poll_only: self.poll_only,
                queue_changes,
                queue_notifications,
                queues: RwLock::new(self.queues),
                retry_policy: self.retry_policy,
                running: Mutex::new(HashMap::new()),
                #[cfg(feature = "postgres")]
                schema,
                allow_legacy_job_kinds: self.allow_legacy_job_kinds,
                allow_unregistered_job_kinds: self.allow_unregistered_job_kinds,
                soft_stop_timeout: self.soft_stop_timeout,
                started: AtomicBool::new(false),
                unique_nonce: AtomicU64::new(0),
                workers: self.workers,
                work_middleware: self.work_middleware,
            }),
        })
    }
}

pub(crate) struct ClientInner {
    completion_sender: Mutex<Option<mpsc::WeakSender<CompletionUpdate>>>,
    pub(crate) database: Database,
    default_max_attempts: i16,
    error_handler: Option<Arc<dyn ErrorHandler>>,
    pub(crate) events: broadcast::Sender<Event>,
    fetch_registration_windows: AtomicU64,
    pub(crate) hooks: Vec<Arc<dyn Hook>>,
    pub(crate) id: String,
    job_stuck_threshold: Duration,
    pub(crate) job_timeout: Option<Duration>,
    pub(crate) maintenance: MaintenanceConfig,
    insert_middleware: Vec<Arc<dyn InsertMiddleware>>,
    pub(crate) periodic_jobs: PeriodicJobs,
    pending_cancellations: Mutex<HashMap<i64, std::time::Instant>>,
    pub(crate) pilot: Arc<dyn Pilot>,
    poll_only: bool,
    queue_changes: watch::Sender<u64>,
    queue_notifications: broadcast::Sender<RuntimeNotification>,
    queues: RwLock<HashMap<String, QueueConfig>>,
    pub(crate) retry_policy: Arc<dyn RetryPolicy>,
    running: Mutex<HashMap<i64, CancellationToken>>,
    #[cfg(feature = "postgres")]
    pub(crate) schema: SchemaName,
    allow_legacy_job_kinds: bool,
    allow_unregistered_job_kinds: bool,
    soft_stop_timeout: Option<Duration>,
    started: AtomicBool,
    unique_nonce: AtomicU64,
    pub(crate) workers: WorkerRegistry,
    work_middleware: Vec<Arc<dyn WorkMiddleware>>,
}

#[cfg(feature = "sqlite")]
fn sqlite_backend_error(error: crate::database::sqlite::BackendError) -> Error {
    Error::Database(Box::new(error))
}

fn transaction_pool_error(operation: &'static str) -> Error {
    Error::configuration(format!(
        "{operation} requires a caller-managed transaction, not a pool or bare connection"
    ))
}

#[cfg(feature = "postgres")]
async fn begin_postgres_savepoint(connection: &mut PgConnection, name: &str) -> Result<(), Error> {
    sqlx::query(AssertSqlSafe(format!("SAVEPOINT {name}")))
        .execute(connection)
        .await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
async fn begin_sqlite_savepoint(
    connection: &mut sqlx::SqliteConnection,
    name: &str,
) -> Result<(), Error> {
    sqlx::query(AssertSqlSafe(format!("SAVEPOINT {name}")))
        .execute(connection)
        .await?;
    Ok(())
}

#[cfg(feature = "postgres")]
async fn finish_postgres_savepoint<T>(
    connection: &mut PgConnection,
    name: &str,
    result: Result<T, Error>,
) -> Result<T, Error> {
    if result.is_err() {
        sqlx::query(AssertSqlSafe(format!("ROLLBACK TO SAVEPOINT {name}")))
            .execute(&mut *connection)
            .await?;
    }
    sqlx::query(AssertSqlSafe(format!("RELEASE SAVEPOINT {name}")))
        .execute(connection)
        .await?;
    result
}

#[cfg(feature = "sqlite")]
async fn finish_sqlite_savepoint<T>(
    connection: &mut sqlx::SqliteConnection,
    name: &str,
    result: Result<T, Error>,
) -> Result<T, Error> {
    if result.is_err() {
        sqlx::query(AssertSqlSafe(format!("ROLLBACK TO SAVEPOINT {name}")))
            .execute(&mut *connection)
            .await?;
    }
    sqlx::query(AssertSqlSafe(format!("RELEASE SAVEPOINT {name}")))
        .execute(connection)
        .await?;
    result
}

impl ClientInner {
    pub(crate) fn erase_executor<'executor, E>(
        &self,
        executor: E,
    ) -> Result<ErasedExecutor<'executor>, crate::database::DatabaseMismatch>
    where
        E: DatabaseExecutor<'executor>,
    {
        self.database.executor(executor)
    }

    #[cfg(feature = "postgres")]
    pub(crate) const fn database(&self) -> &Database {
        &self.database
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn postgres_pool(&self) -> Option<&PgPool> {
        match self.database.pool() {
            DatabasePool::Postgres(pool) => Some(pool),
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(_) => None,
        }
    }

    #[cfg(feature = "sqlite")]
    pub(crate) fn sqlite_pool(&self) -> Option<&SqlitePool> {
        match self.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(_) => None,
            DatabasePool::Sqlite(pool) => Some(pool),
        }
    }

    pub(crate) fn pilot_database_config(&self) -> PilotDatabaseConfig {
        match self.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(_) => PilotDatabaseConfig::Postgres {
                #[cfg(feature = "postgres")]
                schema: self.schema.clone(),
            },
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(_) => PilotDatabaseConfig::Sqlite,
        }
    }

    pub(crate) fn pilot_database_pool(&self) -> PilotDatabasePool {
        match self.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => PilotDatabasePool::Postgres(pool.clone()),
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => PilotDatabasePool::Sqlite(pool.clone()),
        }
    }
}

/// A River client backed by a caller-owned pool for a built-in database.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Client")
            .field("database_kind", &self.database_kind())
            .field("id", &self.id())
            .field("started", &self.inner.started.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

/// Non-owning handle used by exact-version extension services.
#[doc(hidden)]
#[derive(Clone)]
pub struct WeakClient {
    inner: Weak<ClientInner>,
}

impl std::fmt::Debug for WeakClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WeakClient")
            .field("alive", &(self.inner.strong_count() > 0))
            .finish_non_exhaustive()
    }
}

impl WeakClient {
    /// Upgrades the handle while its originating client remains alive.
    #[must_use]
    pub fn upgrade(&self) -> Option<Client> {
        self.inner.upgrade().map(|inner| Client { inner })
    }
}

impl Client {
    /// Creates a builder for an insert-only client.
    #[must_use]
    pub fn builder<D>(database: D) -> ClientBuilder
    where
        D: IntoDatabase,
    {
        let database = Database::from_source(database);
        ClientBuilder {
            database,
            default_max_attempts: MAX_ATTEMPTS_DEFAULT,
            error_handler: None,
            hooks: Vec::new(),
            id: default_client_id(),
            job_stuck_threshold: JOB_STUCK_THRESHOLD_DEFAULT,
            job_timeout: Some(JOB_TIMEOUT_DEFAULT),
            maintenance: MaintenanceConfig::default(),
            insert_middleware: Vec::new(),
            periodic_jobs: Vec::new(),
            pilot: Arc::new(NoopPilot),
            poll_only: false,
            queues: HashMap::new(),
            retry_policy: Arc::new(DefaultRetryPolicy::default()),
            allow_legacy_job_kinds: false,
            allow_unregistered_job_kinds: false,
            soft_stop_timeout: None,
            workers: WorkerRegistry::new(),
            work_middleware: Vec::new(),
        }
    }

    /// Creates a non-owning handle for an exact-version extension service.
    #[doc(hidden)]
    #[must_use]
    pub fn downgrade(&self) -> WeakClient {
        WeakClient {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Stable identifier recorded in `attempted_by`.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    /// Atomically claims complete job rows for an exact-version extension.
    ///
    /// Eligible jobs are available, due, and match the supplied kind, queue,
    /// and top-level metadata values. River records the attempt and client ID,
    /// applies the metadata updates in the same transaction, and returns rows
    /// ordered by priority, scheduled time, and ID.
    #[doc(hidden)]
    pub async fn extension_claim_jobs(
        &self,
        params: ExtensionClaimParams,
    ) -> Result<Vec<JobRow>, Error> {
        if params.maximum <= 0 {
            return Ok(Vec::new());
        }
        #[cfg(feature = "sqlite")]
        if let Some(pool) = self.inner.sqlite_pool() {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let rows = crate::database::sqlite::claim_filtered(
                &mut transaction,
                &crate::database::sqlite::ClaimFilteredJobs {
                    client_id: &self.inner.id,
                    excluded_job_id: params.excluded_job_id,
                    kind: &params.kind,
                    limit: params.maximum,
                    max_attempted_by: ATTEMPTED_BY_MAX,
                    metadata_matches: &params.metadata_matches,
                    metadata_updates: &params.metadata_updates,
                    now: Utc::now(),
                    queue: &params.queue,
                },
            )
            .await;
            return match rows {
                Ok(mut rows) => {
                    sort_claimed_jobs(&mut rows);
                    transaction.commit().await?;
                    Ok(rows)
                }
                Err(error) => {
                    transaction.rollback().await?;
                    Err(sqlite_backend_error(error))
                }
            };
        }
        #[cfg(feature = "postgres")]
        {
            let pool = self
                .inner
                .postgres_pool()
                .expect("PostgreSQL claim path requires a PostgreSQL pool");
            let table = self.inner.schema.qualify("river_job");
            let sql = format!(
                "WITH locked AS (\
                    SELECT id FROM {table} \
                    WHERE state = 'available' AND queue = $1 AND kind = $2 \
                      AND id != $3 AND scheduled_at <= now() \
                      AND metadata @> $4::jsonb \
                    ORDER BY priority ASC, scheduled_at ASC, id ASC \
                    LIMIT $5 FOR UPDATE SKIP LOCKED\
                 ) UPDATE {table} AS job \
                    SET state = 'running', attempt = job.attempt + 1, \
                        attempted_at = now(), attempted_by = array_append(\
                            CASE WHEN array_length(job.attempted_by, 1) >= $7 \
                                 THEN job.attempted_by[array_length(job.attempted_by, 1) + 2 - $7:] \
                                 ELSE job.attempted_by END, $6), \
                        metadata = job.metadata || $8::jsonb \
                    FROM locked WHERE job.id = locked.id \
                    RETURNING {}, false AS unique_skipped_as_duplicate",
                job_projection("job")
            );
            let mut transaction = pool.begin().await?;
            let records = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
                .bind(&params.queue)
                .bind(&params.kind)
                .bind(params.excluded_job_id)
                .bind(Json(&params.metadata_matches))
                .bind(params.maximum)
                .bind(&self.inner.id)
                .bind(ATTEMPTED_BY_MAX)
                .bind(Json(&params.metadata_updates))
                .fetch_all(&mut *transaction)
                .await;
            let records = match records {
                Ok(records) => records,
                Err(error) => {
                    transaction.rollback().await?;
                    return Err(error.into());
                }
            };
            let rows = records
                .into_iter()
                .map(JobRecord::into_job_row)
                .collect::<Result<Vec<_>, _>>();
            return match rows {
                Ok(mut rows) => {
                    sort_claimed_jobs(&mut rows);
                    transaction.commit().await?;
                    Ok(rows)
                }
                Err(error) => {
                    transaction.rollback().await?;
                    Err(error)
                }
            };
        }
        #[allow(unreachable_code)]
        Err(Error::runtime(
            "database dispatch selected no supported backend".to_owned(),
        ))
    }

    /// Computes the configured retry delay for an exact-version extension.
    #[doc(hidden)]
    #[must_use]
    pub fn extension_retry_delay(&self, row: &JobRow, error: &str, now: DateTime<Utc>) -> Duration {
        self.inner.retry_policy.next_retry(row, error, now)
    }

    /// Returns the scheduler horizon used by exact-version completion helpers.
    #[doc(hidden)]
    #[must_use]
    pub fn extension_scheduler_interval(&self) -> Duration {
        self.inner.maintenance.scheduler_interval
    }

    /// Reports outcomes for jobs claimed and executed by an exact-version
    /// extension through River's canonical completion pipeline.
    ///
    /// The extension's handler context supplies metadata updates shared by the
    /// execution. Each failed outcome runs the error handler for its own job,
    /// then every outcome uses ordinary retry selection, completion
    /// interception, persistence batching, event delivery, and statistics.
    /// Work middleware and work hooks are deliberately not invoked again: they
    /// surround the extension's handler once, before it reports these results.
    /// Outcomes racing an external terminal transition preserve the persisted
    /// state while retaining the submitted event reason.
    ///
    /// # Errors
    ///
    /// Returns an error when the client runtime is not accepting completions,
    /// or when synchronous extension interception or result normalization
    /// fails. Ordinary batched persistence failures are reported by the
    /// running completion service, matching regular worker behavior.
    #[doc(hidden)]
    pub async fn extension_persist_claimed_outcomes(
        &self,
        execution_context: &WorkContext,
        outcomes: Vec<(JobRow, Result<WorkOutcome, BoxError>)>,
    ) -> Result<(), Error> {
        if outcomes.is_empty() {
            return Ok(());
        }
        let metadata_updates = execution_context.metadata_updates().await;
        let completion_sender = self
            .inner
            .completion_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(mpsc::WeakSender::upgrade)
            .ok_or_else(|| {
                Error::runtime_context(
                    "completion pipeline",
                    "client runtime is not accepting job completions".to_owned(),
                )
            })?;
        let mut first_error = None;
        for (row, result) in outcomes {
            if let Err(error) = self
                .extension_persist_claimed_outcome(
                    row,
                    result,
                    execution_context.cancellation_token(),
                    &metadata_updates,
                    &completion_sender,
                )
                .await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    async fn extension_persist_claimed_outcome(
        &self,
        row: JobRow,
        result: Result<WorkOutcome, BoxError>,
        execution_cancellation: &CancellationToken,
        metadata_updates: &Map<String, Value>,
        completion_sender: &mpsc::Sender<CompletionUpdate>,
    ) -> Result<(), Error> {
        let cancellation = CancellationToken::new();
        let context = WorkContext::for_job(
            self.clone(),
            execution_cancellation.clone(),
            row.id,
            &row.metadata,
        );
        for (key, value) in metadata_updates {
            context.metadata_set(key.clone(), value.clone()).await;
        }
        let result = result.map_err(worker_failure_from_source);
        let work_result = public_work_result(&result);
        let mut error_handler_result = ErrorHandlerDecision::default();
        if let Some(error_handler) = &self.inner.error_handler
            && matches!(work_result, WorkResult::Failed(_))
        {
            match error_handler
                .handle_error(&context, &row, &work_result)
                .await
            {
                Ok(decision) => error_handler_result = decision,
                Err(error) => error!(error = %error, "River error handler failed"),
            }
        }
        let queue_wait_duration = row
            .attempted_at
            .and_then(|attempted_at| {
                (attempted_at - row.scheduled_at.max(row.created_at))
                    .to_std()
                    .ok()
            })
            .unwrap_or_default();
        let completion = CompletionAttempt {
            cancellation,
            timing: CompletionTiming {
                completion_started: std::time::Instant::now(),
                queue_wait_duration,
                run_duration: Duration::ZERO,
            },
        };
        match persist_result(
            &self.inner,
            &row,
            &completion,
            result,
            context.metadata_updates().await,
            error_handler_result,
            completion_sender,
        )
        .await?
        {
            PersistResult::Finished(Some(event)) => {
                let Event::Job(event) = *event else {
                    unreachable!("job persistence returns only job events")
                };
                let event = Event::job_with_statistics(
                    event.kind,
                    event.job,
                    JobStatistics {
                        complete_duration: completion.timing.completion_started.elapsed(),
                        queue_wait_duration,
                        run_duration: Duration::ZERO,
                    },
                );
                let _ = self.inner.events.send(event);
            }
            PersistResult::Enqueued | PersistResult::Finished(None) => {}
        }
        Ok(())
    }

    /// Returns the dynamic periodic-job bundle for this client.
    #[must_use]
    pub fn periodic_jobs(&self) -> PeriodicJobs {
        self.inner.periodic_jobs.clone()
    }

    /// Returns the selected database backend.
    #[must_use]
    pub fn database_kind(&self) -> DatabaseKind {
        self.inner.database.kind()
    }

    /// Returns the caller-owned PostgreSQL pool, if this is a PostgreSQL
    /// client.
    #[must_use]
    #[cfg(feature = "postgres")]
    pub fn postgres_pool(&self) -> Option<&PgPool> {
        self.inner.postgres_pool()
    }

    /// Returns the validated PostgreSQL schema selection, or `None` for a
    /// backend without PostgreSQL schemas.
    #[must_use]
    pub fn postgres_schema(&self) -> Option<&SchemaName> {
        self.inner.database.postgres_schema()
    }

    /// Returns the caller-owned SQLite pool, if this is a SQLite client.
    #[must_use]
    #[cfg(feature = "sqlite")]
    pub fn sqlite_pool(&self) -> Option<&SqlitePool> {
        self.inner.sqlite_pool()
    }

    /// Adds or reconfigures a queue. A running client starts or restarts the
    /// queue without restarting other queues.
    pub fn queue_add(
        &self,
        name: impl Into<String>,
        config: QueueConfig,
    ) -> Result<Option<QueueConfig>, Error> {
        let name = name.into();
        config.validate(&name)?;
        if self.inner.workers.kinds().is_empty() {
            return Err(Error::configuration(
                "workers must be configured when queues are configured".to_owned(),
            ));
        }
        let previous = self
            .inner
            .queues
            .write()
            .map_err(|_| Error::runtime("queue configuration lock poisoned".to_owned()))?
            .insert(name, config);
        self.inner.queue_changes.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
        Ok(previous)
    }

    /// Returns a stable snapshot of configured queues.
    pub fn queue_configs(&self) -> Result<HashMap<String, QueueConfig>, Error> {
        self.inner
            .queues
            .read()
            .map(|queues| queues.clone())
            .map_err(|_| Error::runtime("queue configuration lock poisoned".to_owned()))
    }

    /// Stops and removes a configured queue. Persisted jobs and queue rows are
    /// left untouched for other clients.
    pub fn queue_remove(&self, name: &str) -> Result<Option<QueueConfig>, Error> {
        let previous = self
            .inner
            .queues
            .write()
            .map_err(|_| Error::runtime("queue configuration lock poisoned".to_owned()))?
            .remove(name);
        if previous.is_some() {
            self.inner.queue_changes.send_modify(|generation| {
                *generation = generation.wrapping_add(1);
            });
        }
        Ok(previous)
    }

    /// Subscribes to selected local client events with a bounded buffer.
    pub fn subscribe(&self, kinds: &[EventKind]) -> Result<EventReceiver, Error> {
        self.subscribe_config(SubscribeConfig::new(kinds.iter().copied())?)
    }

    /// Subscribes with an explicit bounded-buffer capacity. When the receiver
    /// falls behind, the next receive reports how many events were dropped.
    pub fn subscribe_config(&self, config: SubscribeConfig) -> Result<EventReceiver, Error> {
        let (buffer_capacity, kinds) = config.into_parts();
        if self
            .inner
            .queues
            .read()
            .map_err(|_| Error::runtime("queue configuration lock poisoned".to_owned()))?
            .is_empty()
        {
            return Err(Error::configuration(
                "event subscriptions require a client configured to work queues".to_owned(),
            ));
        }
        if buffer_capacity == 0 {
            return Err(Error::configuration(
                "event subscription buffer capacity must be positive".to_owned(),
            ));
        }
        let kinds = crate::event::validate_kinds(&kinds)?;
        let mut source = self.inner.events.subscribe();
        let (sender, receiver) = mpsc::channel(buffer_capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let dropped_for_task = Arc::clone(&dropped);
        tokio::runtime::Handle::try_current().map_err(|_| Error::RuntimeUnavailable {
            operation: "event subscriptions",
        })?;
        tokio::spawn(async move {
            loop {
                match source.recv().await {
                    Ok(event) if kinds.contains(&event.kind()) => match sender.try_send(event) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            dropped_for_task.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    },
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        dropped_for_task.fetch_add(count, Ordering::Relaxed);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Ok(EventReceiver::new(dropped, receiver))
    }

    /// Inserts a typed job using its job-type, client, and River defaults.
    pub async fn insert<A: JobArgs>(&self, args: A) -> Result<InsertResult<A>, Error> {
        self.insert_with(args, InsertOpts::default()).await
    }

    /// Inserts a typed job with options overlaid on its job-type defaults.
    pub async fn insert_with<A: JobArgs>(
        &self,
        args: A,
        opts: InsertOpts,
    ) -> Result<InsertResult<A>, Error> {
        let result = match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => self.insert_on(pool, args, opts).await?,
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => self.insert_on(pool, args, opts).await?,
        };
        self.signal_insert(&result.job.row, result.unique_skipped_as_duplicate);
        Ok(result)
    }

    /// Atomically inserts a batch containing one or more job argument types.
    ///
    /// Results correspond positionally to the batch items. Each item retains
    /// its own [`JobArgs`] defaults and uniqueness definition.
    pub async fn insert_batch(&self, batch: InsertBatch) -> Result<Vec<InsertBatchResult>, Error> {
        validate_nonempty_batch(&batch)?;
        let results = match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await?;
                let results = self.insert_batch_tx(&mut transaction, batch).await?;
                transaction.commit().await?;
                results
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                let mut transaction = crate::database::begin_sqlite_write(pool).await?;
                let results = self.insert_batch_tx(&mut transaction, batch).await?;
                transaction.commit().await?;
                results
            }
        };
        for result in &results {
            self.signal_insert(&result.job, result.unique_skipped_as_duplicate);
        }
        Ok(results)
    }

    /// Atomically inserts a heterogeneous batch in a caller-managed
    /// transaction.
    pub async fn insert_batch_tx<'executor, E>(
        &self,
        executor: E,
        batch: InsertBatch,
    ) -> Result<Vec<InsertBatchResult>, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        validate_nonempty_batch(&batch)?;
        match self
            .inner
            .erase_executor(executor)
            .map_err(Error::from)?
            .into_inner()
        {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                let savepoint = self.batch_savepoint("heterogeneous");
                begin_postgres_savepoint(connection, &savepoint).await?;
                let result = self.insert_batch_postgres(connection, batch).await;
                finish_postgres_savepoint(connection, &savepoint, result).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                let savepoint = self.batch_savepoint("heterogeneous");
                begin_sqlite_savepoint(connection, &savepoint).await?;
                let result = self.insert_batch_sqlite(connection, batch).await;
                finish_sqlite_savepoint(connection, &savepoint, result).await
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => Err(transaction_pool_error("insert_batch_tx")),
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => Err(transaction_pool_error("insert_batch_tx")),
        }
    }

    #[cfg(feature = "postgres")]
    async fn insert_batch_postgres(
        &self,
        connection: &mut PgConnection,
        batch: InsertBatch,
    ) -> Result<Vec<InsertBatchResult>, Error> {
        self.insert_batch_on_postgres(connection, batch).await
    }

    #[cfg(feature = "postgres")]
    async fn insert_batch_on_postgres(
        &self,
        connection: &mut PgConnection,
        batch: InsertBatch,
    ) -> Result<Vec<InsertBatchResult>, Error> {
        let mut results = Vec::with_capacity(batch.len());
        for item in batch.items {
            self.validate_known_kind(item.kind)?;
            let opts =
                InsertOpts::resolve(self.inner.default_max_attempts, item.defaults, item.opts);
            let (mut job, unique_skipped_as_duplicate) = self
                .insert_encoded_on(
                    &mut *connection,
                    item.kind,
                    item.unique_fields,
                    &item.encoded_args,
                    opts,
                )
                .await?;
            for hook in self.inner.hooks.iter().rev() {
                hook.decode_insert_result(&mut job).await?;
            }
            results.push(InsertBatchResult {
                job,
                unique_skipped_as_duplicate,
            });
        }
        Ok(results)
    }

    #[cfg(feature = "sqlite")]
    async fn insert_batch_sqlite(
        &self,
        connection: &mut sqlx::SqliteConnection,
        batch: InsertBatch,
    ) -> Result<Vec<InsertBatchResult>, Error> {
        let mut results = Vec::with_capacity(batch.len());
        for item in batch.items {
            self.validate_known_kind(item.kind)?;
            let opts =
                InsertOpts::resolve(self.inner.default_max_attempts, item.defaults, item.opts);
            let (mut job, unique_skipped_as_duplicate) = self
                .insert_encoded_on(
                    &mut *connection,
                    item.kind,
                    item.unique_fields,
                    &item.encoded_args,
                    opts,
                )
                .await?;
            for hook in self.inner.hooks.iter().rev() {
                hook.decode_insert_result(&mut job).await?;
            }
            results.push(InsertBatchResult {
                job,
                unique_skipped_as_duplicate,
            });
        }
        Ok(results)
    }

    /// Inserts a homogeneous typed batch atomically using each job type's
    /// insertion defaults.
    pub async fn insert_many<A, I>(&self, jobs: I) -> Result<Vec<InsertResult<A>>, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = A>,
    {
        self.insert_many_with(jobs.into_iter().map(|args| (args, InsertOpts::default())))
            .await
    }

    /// Inserts a homogeneous typed batch atomically with per-job insertion
    /// options.
    pub async fn insert_many_with<A, I>(&self, jobs: I) -> Result<Vec<InsertResult<A>>, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let jobs = collect_nonempty_jobs(jobs)?;
        let results = match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await?;
                let results = self.insert_many_tx_with(&mut transaction, jobs).await?;
                transaction.commit().await?;
                results
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                let mut transaction = crate::database::begin_sqlite_write(pool).await?;
                let results = self.insert_many_tx_with(&mut transaction, jobs).await?;
                transaction.commit().await?;
                results
            }
        };
        for result in &results {
            self.signal_insert(&result.job.row, result.unique_skipped_as_duplicate);
        }
        Ok(results)
    }

    /// Inserts a typed batch using the backend's optimized atomic path and
    /// returns only the inserted row count. PostgreSQL uses COPY; SQLite uses
    /// a transactional fallback. A unique conflict fails the whole operation.
    /// Per-job begin hooks and insertion middleware run before persistence;
    /// successful completion uses the fast-insert callbacks because no rows
    /// are returned.
    pub async fn insert_many_fast<A, I>(&self, jobs: I) -> Result<u64, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = A>,
    {
        self.insert_many_fast_with(jobs.into_iter().map(|args| (args, InsertOpts::default())))
            .await
    }

    /// Inserts a typed batch with per-job options using the backend's
    /// optimized insertion path.
    pub async fn insert_many_fast_with<A, I>(&self, jobs: I) -> Result<u64, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let count = match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await?;
                let count = self
                    .insert_many_fast_tx_with(&mut transaction, jobs)
                    .await?;
                transaction.commit().await?;
                count
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                let mut transaction = crate::database::begin_sqlite_write(pool).await?;
                let count = self
                    .insert_many_fast_tx_with(&mut transaction, jobs)
                    .await?;
                transaction.commit().await?;
                count
            }
        };
        if count > 0 {
            let _ = self
                .inner
                .queue_notifications
                .send(RuntimeNotification::Insert("*".to_owned()));
        }
        Ok(count)
    }

    #[cfg(feature = "sqlite")]
    #[allow(
        clippy::too_many_lines,
        reason = "keeps fast-insert hooks, interception, persistence, and callbacks in one ordered path"
    )]
    async fn insert_many_fast_sqlite<A, I>(
        &self,
        connection: &mut sqlx::SqliteConnection,
        jobs: I,
    ) -> Result<u64, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let now = Utc::now();
        let mut prepared = Vec::new();
        for (args, opts) in jobs {
            self.validate_known_kind(A::KIND)?;
            let mut insert = InsertContext {
                encoded_args: serde_json::to_value(args)?,
                kind: A::KIND.to_owned(),
                opts: InsertOpts::resolve(
                    self.inner.default_max_attempts,
                    A::default_insert_opts(),
                    opts,
                ),
            };
            for hook in &self.inner.hooks {
                hook.insert_begin(&mut insert).await?;
            }
            if self.inner.pilot.intercepts_insert() {
                let InsertContext {
                    encoded_args,
                    kind,
                    opts,
                } = &mut insert;
                self.inner
                    .pilot
                    .before_job_insert(
                        PilotDatabaseConnection::Sqlite(&mut *connection),
                        &mut PilotJobInsertParams {
                            encoded_args,
                            kind,
                            metadata: &mut opts.metadata,
                            queue: &mut opts.queue,
                        },
                    )
                    .await
                    .map_err(|source| Error::Extension {
                        phase: "job insertion",
                        source,
                    })?;
            }
            for middleware in &self.inner.insert_middleware {
                middleware.before_insert(&mut insert).await?;
            }
            validate_insert_parts(
                &insert.kind,
                &insert.opts,
                self.inner.allow_legacy_job_kinds,
            )?;
            prepared.push(PreparedFastInsert::new(insert, A::unique_fields(), now)?);
        }
        if prepared.is_empty() {
            return Err(Error::invalid_job("no jobs to insert".to_owned()));
        }

        let mut queues = std::collections::BTreeSet::new();
        for job in &prepared {
            let nonce = job.unique_key.map(|_| {
                format!(
                    "{}-{}",
                    self.inner.id,
                    self.inner.unique_nonce.fetch_add(1, Ordering::Relaxed)
                )
            });
            let inserted = crate::database::sqlite::insert(
                &mut *connection,
                &crate::database::sqlite::InsertJob {
                    attempt: 0,
                    attempted_at: None,
                    attempted_by: &[],
                    created_at: job.now,
                    encoded_args: &job.encoded_args,
                    errors: &[],
                    finalized_at: None,
                    id: None,
                    kind: &job.kind,
                    max_attempts: job.max_attempts,
                    metadata: &job.metadata,
                    priority: job.priority,
                    queue: &job.queue,
                    scheduled_at: job.scheduled_at,
                    state: job.state,
                    tags: &job.tags,
                    unique_key: job.unique_key.as_ref().map(<[u8; 32]>::as_slice),
                    unique_nonce: nonce.as_deref(),
                    unique_states: job.unique_states,
                },
            )
            .await
            .map_err(sqlite_backend_error)?;
            if inserted.unique_skipped_as_duplicate {
                return Err(Error::invalid_job(
                    "fast insertion encountered a unique conflict".to_owned(),
                ));
            }
            if inserted.job.state == JobState::Available {
                queues.insert(inserted.job.queue);
            }
        }
        for queue in queues {
            let payload = serde_json::json!({"queue": queue}).to_string();
            crate::database::sqlite::notification_insert(
                &mut *connection,
                &[crate::database::sqlite::NotificationInput {
                    payload: &payload,
                    topic: crate::NOTIFICATION_TOPIC_INSERT,
                }],
            )
            .await
            .map_err(sqlite_backend_error)?;
        }
        let count = u64::try_from(prepared.len()).unwrap_or(u64::MAX);
        for middleware in self.inner.insert_middleware.iter().rev() {
            middleware.after_insert_many_fast(count).await?;
        }
        for hook in &self.inner.hooks {
            hook.insert_many_fast_end(count).await?;
        }
        Ok(count)
    }

    /// Inserts a typed batch using the backend's optimized path inside a
    /// caller-managed transaction.
    pub async fn insert_many_fast_tx<'executor, A, I, E>(
        &self,
        executor: E,
        jobs: I,
    ) -> Result<u64, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = A>,
        E: DatabaseTransactionExecutor<'executor>,
    {
        self.insert_many_fast_tx_with(
            executor,
            jobs.into_iter().map(|args| (args, InsertOpts::default())),
        )
        .await
    }

    /// Inserts a typed batch with per-job options using the backend's
    /// optimized path inside a caller-managed transaction.
    pub async fn insert_many_fast_tx_with<'executor, A, I, E>(
        &self,
        executor: E,
        jobs: I,
    ) -> Result<u64, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
        E: DatabaseTransactionExecutor<'executor>,
    {
        match self
            .inner
            .erase_executor(executor)
            .map_err(Error::from)?
            .into_inner()
        {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                let savepoint = self.batch_savepoint("fast");
                begin_postgres_savepoint(connection, &savepoint).await?;
                let result = self.insert_many_fast_postgres(connection, jobs).await;
                finish_postgres_savepoint(connection, &savepoint, result).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                let savepoint = self.batch_savepoint("fast");
                begin_sqlite_savepoint(connection, &savepoint).await?;
                let result = self.insert_many_fast_sqlite(connection, jobs).await;
                finish_sqlite_savepoint(connection, &savepoint, result).await
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => Err(transaction_pool_error("insert_many_fast_tx")),
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => Err(transaction_pool_error("insert_many_fast_tx")),
        }
    }

    #[cfg(feature = "postgres")]
    async fn insert_many_fast_postgres<A, I>(
        &self,
        connection: &mut PgConnection,
        jobs: I,
    ) -> Result<u64, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let now = Utc::now();
        let mut prepared = Vec::new();
        for (args, opts) in jobs {
            if !self.inner.allow_unregistered_job_kinds
                && !self.inner.workers.kinds().is_empty()
                && !self.inner.workers.contains_kind(A::KIND)
            {
                return Err(Error::UnknownJobKind(A::KIND.to_owned()));
            }
            let mut insert = InsertContext {
                encoded_args: serde_json::to_value(args)?,
                kind: A::KIND.to_owned(),
                opts: InsertOpts::resolve(
                    self.inner.default_max_attempts,
                    A::default_insert_opts(),
                    opts,
                ),
            };
            for hook in &self.inner.hooks {
                hook.insert_begin(&mut insert).await?;
            }
            if self.inner.pilot.intercepts_insert() {
                let InsertContext {
                    encoded_args,
                    kind,
                    opts,
                } = &mut insert;
                self.inner
                    .pilot
                    .before_job_insert(
                        PilotDatabaseConnection::Postgres(&mut *connection),
                        &mut PilotJobInsertParams {
                            encoded_args,
                            kind,
                            metadata: &mut opts.metadata,
                            queue: &mut opts.queue,
                        },
                    )
                    .await
                    .map_err(|source| Error::Extension {
                        phase: "job insertion",
                        source,
                    })?;
            }
            for middleware in &self.inner.insert_middleware {
                middleware.before_insert(&mut insert).await?;
            }
            validate_insert_parts(
                &insert.kind,
                &insert.opts,
                self.inner.allow_legacy_job_kinds,
            )?;
            prepared.push(PreparedFastInsert::new(insert, A::unique_fields(), now)?);
        }
        if prepared.is_empty() {
            return Err(Error::invalid_job("no jobs to insert".to_owned()));
        }

        let table = self.inner.schema.qualify("river_job");
        let copy_sql = format!(
            "COPY {table} (args, created_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states) FROM STDIN WITH (FORMAT csv, NULL '\\N')"
        );
        let data = encode_fast_copy(&prepared);
        let mut copy = connection.copy_in_raw(&copy_sql).await?;
        if let Err(copy_error) = copy.send(data).await {
            let _ = copy.abort("River fast insertion failed").await;
            return Err(copy_error.into());
        }
        let count = copy.finish().await?;

        let queues = prepared
            .iter()
            .filter(|job| job.state == JobState::Available)
            .map(|job| job.queue.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        for queue in queues {
            sqlx::query(
                "SELECT pg_notify(concat(coalesce($1::text, current_schema()), '.', $2::text), json_build_object('queue', $3::text)::text)",
            )
            .bind(self.inner.schema.as_deref())
            .bind(crate::NOTIFICATION_TOPIC_INSERT)
            .bind(queue)
            .execute(&mut *connection)
            .await?;
        }
        for middleware in self.inner.insert_middleware.iter().rev() {
            middleware.after_insert_many_fast(count).await?;
        }
        for hook in &self.inner.hooks {
            hook.insert_many_fast_end(count).await?;
        }
        Ok(count)
    }

    /// Inserts a homogeneous typed batch on a caller-managed transaction using
    /// each job type's insertion defaults.
    pub async fn insert_many_tx<'executor, A, I, E>(
        &self,
        executor: E,
        jobs: I,
    ) -> Result<Vec<InsertResult<A>>, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = A>,
        E: DatabaseTransactionExecutor<'executor>,
    {
        self.insert_many_tx_with(
            executor,
            jobs.into_iter().map(|args| (args, InsertOpts::default())),
        )
        .await
    }

    /// Inserts a homogeneous typed batch with per-job options on a
    /// caller-managed transaction. The caller chooses commit or rollback
    /// visibility.
    pub async fn insert_many_tx_with<'executor, A, I, E>(
        &self,
        executor: E,
        jobs: I,
    ) -> Result<Vec<InsertResult<A>>, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
        E: DatabaseTransactionExecutor<'executor>,
    {
        let jobs = collect_nonempty_jobs(jobs)?;
        match self
            .inner
            .erase_executor(executor)
            .map_err(Error::from)?
            .into_inner()
        {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                let savepoint = self.batch_savepoint("regular");
                begin_postgres_savepoint(connection, &savepoint).await?;
                let result = self.insert_many_postgres(connection, jobs).await;
                finish_postgres_savepoint(connection, &savepoint, result).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                let savepoint = self.batch_savepoint("regular");
                begin_sqlite_savepoint(connection, &savepoint).await?;
                let result = self.insert_many_sqlite(connection, jobs).await;
                finish_sqlite_savepoint(connection, &savepoint, result).await
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => Err(transaction_pool_error("insert_many_tx")),
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => Err(transaction_pool_error("insert_many_tx")),
        }
    }

    fn batch_savepoint(&self, operation: &str) -> String {
        let nonce = self.inner.unique_nonce.fetch_add(1, Ordering::Relaxed);
        format!("river_{operation}_insert_many_{nonce}")
    }

    #[cfg(feature = "postgres")]
    async fn insert_many_postgres<A, I>(
        &self,
        connection: &mut PgConnection,
        jobs: I,
    ) -> Result<Vec<InsertResult<A>>, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let jobs = jobs.into_iter();
        let (lower, _) = jobs.size_hint();
        let mut results = Vec::with_capacity(lower);
        for (args, opts) in jobs {
            results.push(self.insert_on(&mut *connection, args, opts).await?);
        }
        Ok(results)
    }

    #[cfg(feature = "sqlite")]
    async fn insert_many_sqlite<A, I>(
        &self,
        connection: &mut sqlx::SqliteConnection,
        jobs: I,
    ) -> Result<Vec<InsertResult<A>>, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let jobs = jobs.into_iter();
        let (lower, _) = jobs.size_hint();
        let mut results = Vec::with_capacity(lower);
        for (args, opts) in jobs {
            results.push(self.insert_on(&mut *connection, args, opts).await?);
        }
        Ok(results)
    }

    /// Inserts an encoded job through River's exact-version extension seam.
    #[doc(hidden)]
    pub async fn insert_raw(
        &self,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: Value,
        opts: InsertOpts,
    ) -> Result<RawInsertResult, Error> {
        let opts =
            InsertOpts::resolve(self.inner.default_max_attempts, InsertOpts::default(), opts);
        self.insert_raw_params(kind, unique_fields, encoded_args, opts)
            .await
    }

    /// Inserts an encoded job with already-resolved parameters through River's
    /// exact-version extension seam.
    #[doc(hidden)]
    pub async fn insert_raw_params(
        &self,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: Value,
        opts: InsertParams,
    ) -> Result<RawInsertResult, Error> {
        self.validate_known_kind(kind)?;
        let (mut job, unique_skipped_as_duplicate) = match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => {
                self.insert_encoded_on(pool, kind, unique_fields, &encoded_args, opts)
                    .await?
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                self.insert_encoded_on(pool, kind, unique_fields, &encoded_args, opts)
                    .await?
            }
        };
        for hook in self.inner.hooks.iter().rev() {
            hook.decode_insert_result(&mut job).await?;
        }
        self.signal_insert(&job, unique_skipped_as_duplicate);
        Ok(RawInsertResult {
            job,
            unique_skipped_as_duplicate,
        })
    }

    /// Inserts an encoded job inside a caller-managed transaction.
    #[doc(hidden)]
    pub async fn insert_raw_tx<'executor, E>(
        &self,
        connection: E,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: Value,
        opts: InsertOpts,
    ) -> Result<RawInsertResult, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        let opts =
            InsertOpts::resolve(self.inner.default_max_attempts, InsertOpts::default(), opts);
        self.insert_raw_params_tx(connection, kind, unique_fields, encoded_args, opts)
            .await
    }

    /// Inserts an encoded job with already-resolved parameters inside a
    /// caller-managed transaction.
    #[doc(hidden)]
    pub async fn insert_raw_params_tx<'executor, E>(
        &self,
        connection: E,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: Value,
        opts: InsertParams,
    ) -> Result<RawInsertResult, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        self.validate_known_kind(kind)?;
        let (mut job, unique_skipped_as_duplicate) = self
            .insert_encoded_on(connection, kind, unique_fields, &encoded_args, opts)
            .await?;
        for hook in self.inner.hooks.iter().rev() {
            hook.decode_insert_result(&mut job).await?;
        }
        Ok(RawInsertResult {
            job,
            unique_skipped_as_duplicate,
        })
    }

    /// Reinserts persisted fields through River's canonical insertion
    /// pipeline inside a caller-managed transaction.
    ///
    /// This exact-version operation lets the backend allocate the ID rather
    /// than explicitly retaining a source ID, and resets execution state while
    /// retaining the supplied creation, schedule, and uniqueness wire values.
    /// Begin hooks, insertion interception, middleware, end callbacks, and the
    /// backend notification all run exactly once.
    #[doc(hidden)]
    pub async fn extension_insert_tx<'executor, E>(
        &self,
        transaction: E,
        params: ExtensionInsertParams,
    ) -> Result<RawInsertResult, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        let mut source_row = JobRow {
            attempt: 0,
            attempted_at: None,
            attempted_by: Vec::new(),
            created_at: params.created_at,
            encoded_args: params.encoded_args,
            errors: Vec::new(),
            finalized_at: None,
            id: 0,
            kind: params.kind,
            max_attempts: params.max_attempts,
            metadata: params.metadata,
            priority: params.priority,
            queue: params.queue,
            scheduled_at: params.scheduled_at,
            state: JobState::Available,
            tags: params.tags,
            unique_key: params.unique_key,
            unique_states: params.unique_states,
        };
        // Exact-version callers supply persisted wire fields. Normalize them
        // before the ordinary begin pipeline so storage transforms are not
        // applied twice, then decode the newly persisted result below just as
        // a typed insertion does.
        for hook in self.inner.hooks.iter().rev() {
            hook.decode_insert_result(&mut source_row).await?;
        }
        let unique_states = match (&source_row.unique_key, &source_row.unique_states) {
            (None, None) => None,
            (Some(_), Some(states)) => Some(
                states
                    .iter()
                    .fold(0, |bitmask, state| bitmask | state.unique_bit()),
            ),
            _ => {
                return Err(Error::invalid_job_context(
                    "exact-version insertion",
                    "unique_key and unique_states must either both be set or both be absent"
                        .to_owned(),
                ));
            }
        };
        let opts = InsertParams {
            max_attempts: source_row.max_attempts,
            metadata: source_row.metadata,
            pending: false,
            priority: source_row.priority,
            queue: source_row.queue,
            scheduled_at: Some(source_row.scheduled_at),
            tags: source_row.tags,
            unique: crate::UniqueOpts::default(),
        };
        let executor = self
            .inner
            .erase_executor(transaction)
            .map_err(Error::from)?
            .into_inner();
        let (mut job, unique_skipped_as_duplicate) = self
            .insert_encoded_inner(
                executor,
                &source_row.kind,
                &[],
                &source_row.encoded_args,
                opts,
                Some(ExtensionInsertWire {
                    created_at: source_row.created_at,
                    unique_key: source_row.unique_key,
                    unique_states,
                }),
            )
            .await?;
        for hook in self.inner.hooks.iter().rev() {
            hook.decode_insert_result(&mut job).await?;
        }
        Ok(RawInsertResult {
            job,
            unique_skipped_as_duplicate,
        })
    }

    /// Inserts a typed job on a caller-managed transaction using
    /// its job-type, client, and River defaults.
    pub async fn insert_tx<'executor, A, E>(
        &self,
        connection: E,
        args: A,
    ) -> Result<InsertResult<A>, Error>
    where
        A: JobArgs,
        E: DatabaseTransactionExecutor<'executor>,
    {
        self.insert_tx_with(connection, args, InsertOpts::default())
            .await
    }

    /// Inserts a typed job with options on a caller-managed transaction.
    pub async fn insert_tx_with<'executor, A, E>(
        &self,
        connection: E,
        args: A,
        opts: InsertOpts,
    ) -> Result<InsertResult<A>, Error>
    where
        A: JobArgs,
        E: DatabaseTransactionExecutor<'executor>,
    {
        self.insert_on(connection, args, opts).await
    }

    /// Resolves typed insertion options for an exact-version extension.
    #[doc(hidden)]
    #[must_use]
    pub fn resolve_insert_opts<A: JobArgs>(&self, opts: InsertOpts) -> InsertParams {
        InsertOpts::resolve(
            self.inner.default_max_attempts,
            A::default_insert_opts(),
            opts,
        )
    }

    /// Gets one job by ID.
    pub async fn job_get(&self, id: i64) -> Result<JobRow, Error> {
        #[cfg(feature = "sqlite")]
        if let Some(pool) = self.inner.sqlite_pool() {
            let mut connection = pool.acquire().await?;
            return crate::database::sqlite::get(&mut connection, id)
                .await
                .map_err(sqlite_backend_error)?
                .ok_or(Error::NotFound);
        }
        #[cfg(feature = "postgres")]
        let Some(pool) = self.inner.postgres_pool() else {
            return Err(Error::runtime(
                "database dispatch selected no supported backend".to_owned(),
            ));
        };
        #[cfg(feature = "postgres")]
        let table = self.inner.schema.qualify("river_job");
        #[cfg(feature = "postgres")]
        let sql = format!(
            "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job WHERE id = $1 LIMIT 1",
            job_projection("job")
        );
        #[cfg(feature = "postgres")]
        let record = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(id)
            .fetch_optional(pool)
            .await?
            .ok_or(Error::NotFound)?;
        #[cfg(feature = "postgres")]
        return record.into_job_row();
        #[allow(unreachable_code)]
        Err(Error::runtime(
            "database dispatch selected no supported backend".to_owned(),
        ))
    }

    /// Cancels a job and returns its current row.
    pub async fn job_cancel(&self, id: i64) -> Result<JobRow, Error> {
        match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await?;
                let row = self.job_cancel_tx(&mut transaction, id).await?;
                transaction.commit().await?;
                Ok(row)
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                let mut transaction = crate::database::begin_sqlite_write(pool).await?;
                let now = Utc::now();
                let updated = crate::database::sqlite::cancel(&mut transaction, id, now)
                    .await
                    .map_err(sqlite_backend_error)?;
                let was_updated = updated.is_some();
                let row = match updated {
                    Some(row) => row,
                    None => crate::database::sqlite::get(&mut transaction, id)
                        .await
                        .map_err(sqlite_backend_error)?
                        .ok_or(Error::NotFound)?,
                };
                if was_updated {
                    let payload = serde_json::json!({
                        "action": "cancel",
                        "job_id": id,
                        "queue": row.queue,
                    })
                    .to_string();
                    crate::database::sqlite::notification_insert(
                        &mut transaction,
                        &[crate::database::sqlite::NotificationInput {
                            payload: &payload,
                            topic: crate::NOTIFICATION_TOPIC_CONTROL,
                        }],
                    )
                    .await
                    .map_err(sqlite_backend_error)?;
                }
                transaction.commit().await?;
                signal_running_attempt(
                    &self.inner.running,
                    &self.inner.pending_cancellations,
                    &self.inner.fetch_registration_windows,
                    id,
                );
                Ok(row)
            }
        }
    }

    /// Cancels a job inside a caller-managed transaction. The notification is
    /// delivered only if the caller commits.
    pub async fn job_cancel_tx<'executor, E>(&self, connection: E, id: i64) -> Result<JobRow, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        #[cfg(feature = "postgres")]
        let table = self.inner.schema.qualify("river_job");
        #[cfg(feature = "postgres")]
        let sql = format!(
            "WITH locked AS (\
                SELECT id, queue, state, finalized_at FROM {table} WHERE id = $1 FOR UPDATE\
             ), notified AS (\
                SELECT id, pg_notify(concat(coalesce($2::text, current_schema()), '.', $3::text), json_build_object('action', 'cancel', 'job_id', id, 'queue', queue)::text)\
                FROM locked WHERE state NOT IN ('cancelled', 'completed', 'discarded') AND finalized_at IS NULL\
             ), updated AS (\
                UPDATE {table} AS job SET \
                    state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END, \
                    finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE now() END, \
                    metadata = jsonb_set(metadata, '{{cancel_attempted_at}}'::text[], to_jsonb(now()), true) \
                FROM notified WHERE job.id = notified.id RETURNING job.*\
             ) \
             SELECT {}, false AS unique_skipped_as_duplicate FROM updated AS job \
             UNION ALL \
             SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
             WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM updated) LIMIT 1",
            job_projection("job"),
            job_projection("job")
        );
        #[cfg(feature = "postgres")]
        let postgres_query = || {
            sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql.clone()))
                .bind(id)
                .bind(self.inner.schema.as_deref())
                .bind(crate::NOTIFICATION_TOPIC_CONTROL)
        };
        match self
            .inner
            .erase_executor(connection)
            .map_err(Error::from)?
            .into_inner()
        {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => postgres_query()
                .fetch_optional(connection)
                .await?
                .ok_or(Error::NotFound)?
                .into_job_row(),
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                let updated = crate::database::sqlite::cancel(connection, id, Utc::now())
                    .await
                    .map_err(sqlite_backend_error)?;
                let was_updated = updated.is_some();
                let row = match updated {
                    Some(row) => row,
                    None => crate::database::sqlite::get(connection, id)
                        .await
                        .map_err(sqlite_backend_error)?
                        .ok_or(Error::NotFound)?,
                };
                if was_updated {
                    let payload = serde_json::json!({
                        "action": "cancel",
                        "job_id": id,
                        "queue": row.queue,
                    })
                    .to_string();
                    crate::database::sqlite::notification_insert(
                        connection,
                        &[crate::database::sqlite::NotificationInput {
                            payload: &payload,
                            topic: crate::NOTIFICATION_TOPIC_CONTROL,
                        }],
                    )
                    .await
                    .map_err(sqlite_backend_error)?;
                }
                Ok(row)
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => Err(transaction_pool_error("job_cancel_tx")),
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => Err(transaction_pool_error("job_cancel_tx")),
        }
    }

    /// Requests that the current leader resign after committing an internal
    /// transaction.
    pub async fn request_resign(&self) -> Result<(), Error> {
        match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await?;
                self.request_resign_tx(&mut transaction).await?;
                transaction.commit().await?;
                Ok(())
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                let mut transaction = crate::database::begin_sqlite_write(pool).await?;
                crate::database::sqlite::notification_insert(
                    &mut transaction,
                    &[crate::database::sqlite::NotificationInput {
                        payload: r#"{"action":"request_resign"}"#,
                        topic: crate::NOTIFICATION_TOPIC_LEADERSHIP,
                    }],
                )
                .await
                .map_err(sqlite_backend_error)?;
                transaction.commit().await?;
                let _ = self
                    .inner
                    .queue_notifications
                    .send(RuntimeNotification::LeadershipRequestResign);
                Ok(())
            }
        }
    }

    /// Requests leader resignation in a caller-managed transaction.
    pub async fn request_resign_tx<'executor, E>(&self, connection: E) -> Result<(), Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        match self
            .inner
            .erase_executor(connection)
            .map_err(Error::from)?
            .into_inner()
        {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                sqlx::query(
                    "SELECT pg_notify(concat(coalesce($1::text, current_schema()), '.', $2::text), $3::text)",
                )
                .bind(self.inner.schema.as_deref())
                .bind(crate::NOTIFICATION_TOPIC_LEADERSHIP)
                .bind(r#"{"action":"request_resign"}"#)
                .execute(connection)
                .await?;
                Ok(())
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                crate::database::sqlite::notification_insert(
                    connection,
                    &[crate::database::sqlite::NotificationInput {
                        payload: r#"{"action":"request_resign"}"#,
                        topic: crate::NOTIFICATION_TOPIC_LEADERSHIP,
                    }],
                )
                .await
                .map_err(sqlite_backend_error)?;
                Ok(())
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => Err(transaction_pool_error("request_resign_tx")),
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => Err(transaction_pool_error("request_resign_tx")),
        }
    }

    /// Starts configured queues and returns a lifecycle handle.
    #[allow(
        clippy::too_many_lines,
        reason = "keeps startup ordering and ownership visible"
    )]
    pub fn start(&self) -> Result<RunHandle, Error> {
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| Error::RuntimeUnavailable {
                operation: "starting a client",
            })?;
        if self
            .inner
            .queues
            .read()
            .map_err(|_| Error::runtime("queue configuration lock poisoned".to_owned()))?
            .is_empty()
        {
            return Err(Error::configuration(
                "at least one queue is required to start a client".to_owned(),
            ));
        }
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::runtime("client is already running".to_owned()));
        }
        let fetch_cancel = CancellationToken::new();
        let work_cancel = CancellationToken::new();
        let inner = Arc::clone(&self.inner);
        let fetch_for_task = fetch_cancel.clone();
        let work_for_task = work_cancel.clone();
        let (ready_sender, ready) = oneshot::channel();
        let join = runtime.spawn(async move {
            let result = async {
                let notifications = inner.queue_notifications.clone();
                let (completion_sender, completion_receiver) = mpsc::channel(10_000);
                *inner
                    .completion_sender
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some(completion_sender.downgrade());
                let mut queues = JoinSet::new();
                queues.spawn(run_dynamic_queues(
                    Arc::clone(&inner),
                    completion_sender,
                    fetch_for_task.child_token(),
                    work_for_task.child_token(),
                    notifications.clone(),
                    inner.queue_changes.subscribe(),
                ));
                if inner.poll_only {
                    let _ = ready_sender.send(Ok(()));
                } else {
                    match inner.database.kind() {
                        #[cfg(feature = "postgres")]
                        DatabaseKind::Postgres => {
                            queues.spawn(run_notifications(
                                Arc::clone(&inner),
                                fetch_for_task.child_token(),
                                notifications.clone(),
                                ready_sender,
                            ));
                        }
                        #[cfg(feature = "sqlite")]
                        DatabaseKind::Sqlite => {
                            queues.spawn(run_sqlite_notifications(
                                Arc::clone(&inner),
                                fetch_for_task.child_token(),
                                notifications.clone(),
                                ready_sender,
                            ));
                        }
                    }
                }
                queues.spawn(crate::maintenance::run_maintenance(
                    Arc::clone(&inner),
                    fetch_for_task.child_token(),
                    notifications.subscribe(),
                ));
                for service in inner.pilot.runtime_services() {
                    let pool = inner.pilot_database_pool();
                    let database = inner.pilot_database_config();
                    let service_cancel = fetch_for_task.child_token();
                    queues.spawn(async move {
                        service
                            .run(pool, database, service_cancel)
                            .await
                            .map_err(|service_error| Error::Extension {
                                phase: "runtime service",
                                source: service_error,
                            })
                    });
                }
                queues.spawn(run_completion_batcher(
                    Arc::clone(&inner),
                    completion_receiver,
                ));
                while let Some(result) = queues.join_next().await {
                    result.map_err(Error::from_join)??;
                }
                Ok(())
            }
            .await;
            inner.started.store(false, Ordering::Release);
            result
        });
        Ok(RunHandle {
            fetch_cancel: Some(fetch_cancel),
            join: Some(join),
            ready: Some(ready),
            soft_stop_timeout: self.inner.soft_stop_timeout,
            work_cancel: Some(work_cancel),
        })
    }

    async fn insert_on<'executor, A, E>(
        &self,
        executor: E,
        args: A,
        opts: InsertOpts,
    ) -> Result<InsertResult<A>, Error>
    where
        A: JobArgs,
        E: DatabaseExecutor<'executor>,
    {
        self.validate_known_kind(A::KIND)?;
        let encoded_args = serde_json::to_value(&args)?;
        let opts = InsertOpts::resolve(
            self.inner.default_max_attempts,
            A::default_insert_opts(),
            opts,
        );
        let (mut row, unique_skipped_as_duplicate) = self
            .insert_encoded_on(executor, A::KIND, A::unique_fields(), &encoded_args, opts)
            .await?;
        for hook in self.inner.hooks.iter().rev() {
            hook.decode_insert_result(&mut row).await?;
        }
        let args = serde_json::from_value(row.encoded_args.clone())?;
        Ok(InsertResult {
            job: Job { args, row },
            unique_skipped_as_duplicate,
        })
    }

    pub(crate) async fn insert_periodic(
        &self,
        insert: PeriodicInsert,
        opts: InsertParams,
    ) -> Result<JobRow, Error> {
        validate_insert_parts(insert.kind, &opts, self.inner.allow_legacy_job_kinds)?;
        let (mut row, unique_skipped_as_duplicate) = match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => {
                self.insert_encoded_on(
                    pool,
                    insert.kind,
                    insert.unique_fields,
                    &insert.encoded_args,
                    opts,
                )
                .await?
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                self.insert_encoded_on(
                    pool,
                    insert.kind,
                    insert.unique_fields,
                    &insert.encoded_args,
                    opts,
                )
                .await?
            }
        };
        for hook in self.inner.hooks.iter().rev() {
            hook.decode_insert_result(&mut row).await?;
        }
        self.signal_insert(&row, unique_skipped_as_duplicate);
        Ok(row)
    }

    pub(crate) fn default_max_attempts(&self) -> i16 {
        self.inner.default_max_attempts
    }

    fn signal_insert(&self, row: &JobRow, unique_skipped_as_duplicate: bool) {
        if row.state == JobState::Available && !unique_skipped_as_duplicate {
            let _ = self
                .inner
                .queue_notifications
                .send(RuntimeNotification::Insert(row.queue.clone()));
        }
    }

    pub(crate) fn signal_queue_control(&self, queue: &str) {
        let _ = self
            .inner
            .queue_notifications
            .send(RuntimeNotification::QueueControl(queue.to_owned()));
    }

    #[allow(
        clippy::too_many_lines,
        reason = "keeps backend insert hook ordering identical across dispatch branches"
    )]
    async fn insert_encoded_on<'executor, E>(
        &self,
        executor: E,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: &Value,
        opts: InsertParams,
    ) -> Result<(JobRow, bool), Error>
    where
        E: DatabaseExecutor<'executor>,
    {
        // Resolve and validate the backend before invoking user hooks or
        // middleware so a mismatched executor cannot cause side effects.
        let executor = self
            .inner
            .erase_executor(executor)
            .map_err(Error::from)?
            .into_inner();
        if self.inner.pilot.intercepts_insert() {
            match executor {
                #[cfg(feature = "postgres")]
                ExecutorInner::PostgresPool(pool) => {
                    let mut transaction = pool.begin().await?;
                    let result = self
                        .insert_encoded_inner(
                            ExecutorInner::PostgresConnection(&mut transaction),
                            kind,
                            unique_fields,
                            encoded_args,
                            opts,
                            None,
                        )
                        .await?;
                    transaction.commit().await?;
                    return Ok(result);
                }
                #[cfg(feature = "sqlite")]
                ExecutorInner::SqlitePool(pool) => {
                    let mut transaction = crate::database::begin_sqlite_write(pool).await?;
                    let result = self
                        .insert_encoded_inner(
                            ExecutorInner::SqliteConnection(&mut transaction),
                            kind,
                            unique_fields,
                            encoded_args,
                            opts,
                            None,
                        )
                        .await?;
                    transaction.commit().await?;
                    return Ok(result);
                }
                executor => {
                    return self
                        .insert_encoded_inner(
                            executor,
                            kind,
                            unique_fields,
                            encoded_args,
                            opts,
                            None,
                        )
                        .await;
                }
            }
        }
        self.insert_encoded_inner(executor, kind, unique_fields, encoded_args, opts, None)
            .await
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "keeps backend insertion and exact-version wire semantics aligned"
    )]
    async fn insert_encoded_inner(
        &self,
        mut executor: ExecutorInner<'_>,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: &Value,
        opts: InsertParams,
        wire: Option<ExtensionInsertWire>,
    ) -> Result<(JobRow, bool), Error> {
        let mut insert = InsertContext {
            encoded_args: encoded_args.clone(),
            kind: kind.to_owned(),
            opts,
        };
        for hook in &self.inner.hooks {
            hook.insert_begin(&mut insert).await?;
        }
        if self.inner.pilot.intercepts_insert() {
            let connection = match &mut executor {
                #[cfg(feature = "postgres")]
                ExecutorInner::PostgresConnection(connection) => {
                    PilotDatabaseConnection::Postgres(connection)
                }
                #[cfg(feature = "sqlite")]
                ExecutorInner::SqliteConnection(connection) => {
                    PilotDatabaseConnection::Sqlite(connection)
                }
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                _ => {
                    return Err(Error::runtime_context(
                        "job insertion interception",
                        "insertion pilot requires a transaction connection".to_owned(),
                    ));
                }
            };
            let InsertContext {
                encoded_args,
                kind,
                opts,
            } = &mut insert;
            self.inner
                .pilot
                .before_job_insert(
                    connection,
                    &mut PilotJobInsertParams {
                        encoded_args,
                        kind,
                        metadata: &mut opts.metadata,
                        queue: &mut opts.queue,
                    },
                )
                .await
                .map_err(|source| Error::Extension {
                    phase: "job insertion",
                    source,
                })?;
        }
        for middleware in &self.inner.insert_middleware {
            middleware.before_insert(&mut insert).await?;
        }
        if wire.is_none() {
            validate_insert_parts(
                &insert.kind,
                &insert.opts,
                self.inner.allow_legacy_job_kinds,
            )?;
        }
        let InsertContext {
            encoded_args,
            kind,
            opts,
        } = insert;
        let now = Utc::now();
        let (created_at, state, unique_key, unique_states) = if let Some(wire) = wire {
            (
                Some(wire.created_at),
                JobState::Available,
                wire.unique_key,
                wire.unique_states,
            )
        } else {
            let unique_key = build_unique_key_parts(
                &kind,
                unique_fields,
                &encoded_args,
                now,
                &opts.unique,
                &opts.queue,
                opts.scheduled_at,
            )?
            .map(|key| key.to_vec());
            let unique_states = unique_key.as_ref().map(|_| opts.unique.state_bitmask());
            let state = if opts.pending {
                JobState::Pending
            } else if opts.scheduled_at.is_some() {
                JobState::Scheduled
            } else {
                JobState::Available
            };
            (None, state, unique_key, unique_states)
        };
        #[cfg(feature = "postgres")]
        let table = self.inner.schema.qualify("river_job");
        #[cfg(feature = "postgres")]
        let state_type = self.inner.schema.qualify("river_job_state");
        #[cfg(feature = "postgres")]
        let state_function = self.inner.schema.qualify("river_job_state_in_bitmask");
        // The no-op update is intentional and matches River Go. `DO NOTHING`
        // followed by a select in this CTE cannot see a conflicting row that
        // committed after the statement snapshot was taken.
        #[cfg(feature = "postgres")]
        let sql = format!(
            "WITH inserted AS (\
                INSERT INTO {table} (args, created_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states) \
                VALUES ($1, coalesce($2, now()), $3, $4, $5, $6, $7, coalesce($8, now()), $9::text::{state_type}, $10, $11, $12::integer::bit(8)) \
                ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND unique_states IS NOT NULL AND {state_function}(unique_states, state) \
                DO UPDATE SET kind = EXCLUDED.kind \
                RETURNING *, (xmax != 0) AS unique_skipped_as_duplicate\
             ), notified AS (\
                SELECT pg_notify(concat(coalesce($13::text, current_schema()), '.', $14::text), json_build_object('queue', queue)::text) \
                FROM inserted WHERE state = 'available' AND NOT unique_skipped_as_duplicate\
             ) \
             SELECT {}, job.unique_skipped_as_duplicate \
             FROM inserted AS job LEFT JOIN notified ON true",
            job_projection("job")
        );
        #[cfg(feature = "postgres")]
        let postgres_query = || {
            sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql.clone()))
                .bind(Json(&encoded_args))
                .bind(created_at)
                .bind(&kind)
                .bind(opts.max_attempts)
                .bind(Json(&opts.metadata))
                .bind(opts.priority)
                .bind(&opts.queue)
                .bind(opts.scheduled_at)
                .bind(state.as_str())
                .bind(&opts.tags)
                .bind(unique_key.clone())
                .bind(unique_states.map(i32::from))
                .bind(self.inner.schema.as_deref())
                .bind(crate::NOTIFICATION_TOPIC_INSERT)
        };
        let (row, unique_skipped_as_duplicate) = match executor {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                let record = postgres_query()
                    .fetch_optional(connection)
                    .await?
                    .ok_or_else(|| {
                        Error::invalid_job("unique insert found no conflicting row".to_owned())
                    })?;
                let duplicate = record.unique_skipped_as_duplicate;
                (record.into_job_row()?, duplicate)
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(pool) => {
                let record = postgres_query()
                    .fetch_optional(pool)
                    .await?
                    .ok_or_else(|| {
                        Error::invalid_job("unique insert found no conflicting row".to_owned())
                    })?;
                let duplicate = record.unique_skipped_as_duplicate;
                (record.into_job_row()?, duplicate)
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                let nonce = unique_key.as_ref().map(|_| {
                    format!(
                        "{}-{}",
                        self.inner.id,
                        self.inner.unique_nonce.fetch_add(1, Ordering::Relaxed)
                    )
                });
                let inserted = crate::database::sqlite::insert(
                    connection,
                    &crate::database::sqlite::InsertJob {
                        attempt: 0,
                        attempted_at: None,
                        attempted_by: &[],
                        created_at: created_at.unwrap_or(now),
                        encoded_args: &encoded_args,
                        errors: &[],
                        finalized_at: None,
                        id: None,
                        kind: &kind,
                        max_attempts: opts.max_attempts,
                        metadata: &opts.metadata,
                        priority: opts.priority,
                        queue: &opts.queue,
                        scheduled_at: opts.scheduled_at.unwrap_or(now),
                        state,
                        tags: &opts.tags,
                        unique_key: unique_key.as_deref(),
                        unique_nonce: nonce.as_deref(),
                        unique_states,
                    },
                )
                .await
                .map_err(sqlite_backend_error)?;
                if inserted.job.state == JobState::Available
                    && !inserted.unique_skipped_as_duplicate
                {
                    let payload = serde_json::json!({"queue": inserted.job.queue}).to_string();
                    crate::database::sqlite::notification_insert(
                        connection,
                        &[crate::database::sqlite::NotificationInput {
                            payload: &payload,
                            topic: crate::NOTIFICATION_TOPIC_INSERT,
                        }],
                    )
                    .await
                    .map_err(sqlite_backend_error)?;
                }
                (inserted.job, inserted.unique_skipped_as_duplicate)
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(pool) => {
                let mut transaction = crate::database::begin_sqlite_write(pool).await?;
                let nonce = unique_key.as_ref().map(|_| {
                    format!(
                        "{}-{}",
                        self.inner.id,
                        self.inner.unique_nonce.fetch_add(1, Ordering::Relaxed)
                    )
                });
                let inserted = crate::database::sqlite::insert(
                    &mut transaction,
                    &crate::database::sqlite::InsertJob {
                        attempt: 0,
                        attempted_at: None,
                        attempted_by: &[],
                        created_at: created_at.unwrap_or(now),
                        encoded_args: &encoded_args,
                        errors: &[],
                        finalized_at: None,
                        id: None,
                        kind: &kind,
                        max_attempts: opts.max_attempts,
                        metadata: &opts.metadata,
                        priority: opts.priority,
                        queue: &opts.queue,
                        scheduled_at: opts.scheduled_at.unwrap_or(now),
                        state,
                        tags: &opts.tags,
                        unique_key: unique_key.as_deref(),
                        unique_nonce: nonce.as_deref(),
                        unique_states,
                    },
                )
                .await
                .map_err(sqlite_backend_error)?;
                if inserted.job.state == JobState::Available
                    && !inserted.unique_skipped_as_duplicate
                {
                    let payload = serde_json::json!({"queue": inserted.job.queue}).to_string();
                    crate::database::sqlite::notification_insert(
                        &mut transaction,
                        &[crate::database::sqlite::NotificationInput {
                            payload: &payload,
                            topic: crate::NOTIFICATION_TOPIC_INSERT,
                        }],
                    )
                    .await
                    .map_err(sqlite_backend_error)?;
                }
                transaction.commit().await?;
                (inserted.job, inserted.unique_skipped_as_duplicate)
            }
        };
        for middleware in self.inner.insert_middleware.iter().rev() {
            middleware
                .after_insert(&row, unique_skipped_as_duplicate)
                .await?;
        }
        for hook in &self.inner.hooks {
            hook.insert_end(&row, unique_skipped_as_duplicate).await?;
        }
        Ok((row, unique_skipped_as_duplicate))
    }

    fn validate_known_kind(&self, kind: &str) -> Result<(), Error> {
        if !self.inner.allow_unregistered_job_kinds
            && !self.inner.workers.kinds().is_empty()
            && !self.inner.workers.contains_kind(kind)
        {
            return Err(Error::UnknownJobKind(kind.to_owned()));
        }
        Ok(())
    }
}

/// Controls one running client instance.
///
/// Dropping the handle requests immediate cancellation but cannot wait for
/// in-flight database work to finish. Use [`RunHandle::shutdown`] or
/// [`RunHandle::shutdown_now`] when shutdown must be observed before returning,
/// or [`RunHandle::detach`] to deliberately leave the client running.
#[must_use = "dropping the handle requests immediate client shutdown; call detach to run it independently"]
pub struct RunHandle {
    fetch_cancel: Option<CancellationToken>,
    join: Option<tokio::task::JoinHandle<Result<(), Error>>>,
    ready: Option<oneshot::Receiver<Result<(), String>>>,
    soft_stop_timeout: Option<Duration>,
    work_cancel: Option<CancellationToken>,
}

impl std::fmt::Debug for RunHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RunHandle")
            .field("attached", &self.join.is_some())
            .field("ready_observed", &self.ready.is_none())
            .finish_non_exhaustive()
    }
}

impl RunHandle {
    /// Leaves the client running independently of this handle.
    ///
    /// This permanently relinquishes lifecycle control. The runtime then ends
    /// only on an internal error or process shutdown. Most applications should
    /// retain the handle and use an awaited shutdown method instead.
    pub fn detach(mut self) {
        self.fetch_cancel.take();
        self.work_cancel.take();
        self.join.take();
    }

    /// Waits until the selected backend's notification path is active.
    ///
    /// Poll-only clients are ready immediately. Calling this more than once is
    /// harmless.
    pub async fn wait_ready(&mut self) -> Result<(), Error> {
        let Some(ready) = self.ready.take() else {
            return Ok(());
        };
        ready
            .await
            .map_err(|_| Error::runtime("client stopped before becoming ready".to_owned()))?
            .map_err(Error::runtime)
    }

    /// Stops fetching and waits indefinitely for active jobs.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        if let Some(cancellation) = self.fetch_cancel.take() {
            cancellation.cancel();
        }
        let Some(mut join) = self.join.take() else {
            return Ok(());
        };
        if let Some(timeout) = self.soft_stop_timeout {
            tokio::select! {
                result = &mut join => return join_client_result(result),
                () = tokio::time::sleep(timeout) => {
                    if let Some(cancellation) = self.work_cancel.take() {
                        cancellation.cancel();
                    }
                },
            }
        }
        join_client_result(join.await)
    }

    /// Stops fetching and cancels active job contexts.
    pub async fn shutdown_now(mut self) -> Result<(), Error> {
        if let Some(cancellation) = self.fetch_cancel.take() {
            cancellation.cancel();
        }
        if let Some(cancellation) = self.work_cancel.take() {
            cancellation.cancel();
        }
        match self.join.take() {
            Some(join) => join_client_result(join.await),
            None => Ok(()),
        }
    }

    /// Waits for the client to stop.
    pub async fn wait(mut self) -> Result<(), Error> {
        match self.join.take() {
            Some(join) => join_client_result(join.await),
            None => Ok(()),
        }
    }
}

impl Drop for RunHandle {
    fn drop(&mut self) {
        if let Some(cancellation) = &self.fetch_cancel {
            cancellation.cancel();
        }
        if let Some(cancellation) = &self.work_cancel {
            cancellation.cancel();
        }
    }
}

fn join_client_result(
    result: Result<Result<(), Error>, tokio::task::JoinError>,
) -> Result<(), Error> {
    result.map_err(Error::from_join)??;
    Ok(())
}

fn collect_nonempty_jobs<T>(jobs: impl IntoIterator<Item = T>) -> Result<Vec<T>, Error> {
    let jobs = jobs.into_iter().collect::<Vec<_>>();
    if jobs.is_empty() {
        return Err(Error::invalid_job("no jobs to insert".to_owned()));
    }
    Ok(jobs)
}

fn validate_nonempty_batch(batch: &InsertBatch) -> Result<(), Error> {
    if batch.is_empty() {
        return Err(Error::invalid_job("no jobs to insert".to_owned()));
    }
    Ok(())
}

#[cfg(feature = "postgres")]
pub(crate) struct JobRecord {
    attempt: i16,
    attempted_at: Option<DateTime<Utc>>,
    attempted_by: Option<Vec<String>>,
    created_at: DateTime<Utc>,
    encoded_args: Json<Value>,
    errors: Vec<Json<AttemptError>>,
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
    unique_skipped_as_duplicate: bool,
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
            errors: self.errors.into_iter().map(|error| error.0).collect(),
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

struct CompletionUpdate {
    attempt: i16,
    cancellation: CancellationToken,
    error_json: Option<Value>,
    event_kind: JobEventKind,
    finalized_at: Option<DateTime<Utc>>,
    job_id: i64,
    metadata: Map<String, Value>,
    scheduled_at: Option<DateTime<Utc>>,
    state: JobState,
    timing: CompletionTiming,
}

fn persisted_completion_event_kind(state: JobState, requested: JobEventKind) -> JobEventKind {
    match state {
        JobState::Available => match requested {
            JobEventKind::Failed | JobEventKind::Interrupted | JobEventKind::Snoozed => requested,
            JobEventKind::Cancelled | JobEventKind::Completed => JobEventKind::Failed,
        },
        JobState::Cancelled => JobEventKind::Cancelled,
        JobState::Completed => JobEventKind::Completed,
        JobState::Discarded | JobState::Retryable => JobEventKind::Failed,
        JobState::Scheduled => JobEventKind::Snoozed,
        JobState::Pending | JobState::Running => {
            panic!("completion event received a job that was not finalized")
        }
    }
}

struct CompletionAttempt {
    cancellation: CancellationToken,
    timing: CompletionTiming,
}

#[derive(Clone, Copy)]
struct CompletionTiming {
    completion_started: std::time::Instant,
    queue_wait_duration: Duration,
    run_duration: Duration,
}

enum PersistResult {
    Enqueued,
    Finished(Option<Box<Event>>),
}

async fn run_completion_batcher(
    inner: Arc<ClientInner>,
    mut receiver: mpsc::Receiver<CompletionUpdate>,
) -> Result<(), Error> {
    const COMPLETION_BATCH_DELAY: Duration = Duration::from_millis(10);
    #[cfg(feature = "postgres")]
    const COMPLETION_BATCH_CONCURRENCY: usize = 2;
    const COMPLETION_BATCH_SIZE: usize = 5_000;
    const COMPLETION_BATCH_THRESHOLD: usize = COMPLETION_BATCH_SIZE;
    let mut batches = JoinSet::new();
    let batch_concurrency = match inner.database.kind() {
        #[cfg(feature = "postgres")]
        DatabaseKind::Postgres => COMPLETION_BATCH_CONCURRENCY,
        #[cfg(feature = "sqlite")]
        DatabaseKind::Sqlite => 1,
    };

    loop {
        // Never build a third coordinator-owned batch while both persistence
        // slots are occupied. The bounded receiver applies backpressure until
        // one of the two database writes finishes.
        while batches.len() >= batch_concurrency {
            let result = batches
                .join_next()
                .await
                .expect("completion batch task is present")
                .map_err(Error::from_join)?;
            finish_completion_batch(&inner, result);
        }
        let first = if batches.is_empty() {
            receiver.recv().await
        } else {
            tokio::select! {
                update = receiver.recv() => update,
                result = batches.join_next() => {
                    let result = result
                        .expect("completion batch task is present")
                        .map_err(Error::from_join)?;
                    finish_completion_batch(&inner, result);
                    continue;
                }
            }
        };
        let Some(first) = first else {
            break;
        };
        let mut batch = Vec::with_capacity(COMPLETION_BATCH_SIZE);
        batch.push(first);
        let delay = tokio::time::sleep(COMPLETION_BATCH_DELAY);
        tokio::pin!(delay);
        while batch.len() < COMPLETION_BATCH_THRESHOLD {
            tokio::select! {
                () = &mut delay => break,
                update = receiver.recv() => match update {
                    Some(update) => batch.push(update),
                    None => break,
                },
            }
        }
        while batch.len() < COMPLETION_BATCH_SIZE {
            match receiver.try_recv() {
                Ok(update) => batch.push(update),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }

        // A second database write is worthwhile only when work has already
        // filled a complete batch. Partial batches wait for the active writer,
        // retaining the low-query behavior of the serial architecture.
        while !batches.is_empty() && batch.len() < COMPLETION_BATCH_THRESHOLD {
            let result = batches
                .join_next()
                .await
                .expect("completion batch task is present")
                .map_err(Error::from_join)?;
            finish_completion_batch(&inner, result);
        }
        let batch_inner = Arc::clone(&inner);
        batches.spawn(async move {
            let records = persist_completion_batch(&batch_inner, &batch).await;
            (batch, records)
        });
        while let Some(result) = batches.try_join_next() {
            finish_completion_batch(&inner, result.map_err(Error::from_join)?);
        }
    }
    while let Some(result) = batches.join_next().await {
        finish_completion_batch(&inner, result.map_err(Error::from_join)?);
    }
    Ok(())
}

fn finish_completion_batch(
    inner: &ClientInner,
    (batch, records): (Vec<CompletionUpdate>, Result<Vec<JobRow>, Error>),
) {
    match records {
        Ok(records) => {
            let mut rows = HashMap::with_capacity(records.len());
            for record in records {
                let id = record.id;
                rows.insert(id, record);
            }
            for update in &batch {
                let job_id = update.job_id;
                finish_batched_completion(inner, update, rows.remove(&job_id));
            }
        }
        Err(error) => {
            error!(
                error = %error,
                count = batch.len(),
                "failed to persist River job completion batch"
            );
            for update in &batch {
                remove_running_attempt(&inner.running, update.job_id, &update.cancellation);
            }
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "keeps PostgreSQL batch and transactionally equivalent SQLite completion together"
)]
async fn persist_completion_batch(
    inner: &ClientInner,
    batch: &[CompletionUpdate],
) -> Result<Vec<JobRow>, Error> {
    #[cfg(feature = "sqlite")]
    if let Some(pool) = inner.sqlite_pool() {
        let mut transaction = crate::database::begin_sqlite_write(pool).await?;
        let mut rows = Vec::with_capacity(batch.len());
        for update in batch {
            let attempt_error = update
                .error_json
                .clone()
                .map(serde_json::from_value::<AttemptError>)
                .transpose()?;
            let row = crate::database::sqlite::complete(
                &mut transaction,
                &crate::database::sqlite::CompleteJob {
                    attempt: Some(update.attempt),
                    error: attempt_error.as_ref(),
                    finalized_at: update.finalized_at,
                    id: update.job_id,
                    metadata_updates: Some(&update.metadata),
                    now: Utc::now(),
                    scheduled_at: update.scheduled_at,
                    state: update.state,
                },
            )
            .await
            .map_err(sqlite_backend_error)?;
            if let Some(row) = row {
                rows.push(row);
            } else if let Some(row) = crate::database::sqlite::merge_metadata_if_not_running(
                &mut transaction,
                update.job_id,
                &update.metadata,
            )
            .await
            .map_err(sqlite_backend_error)?
            {
                rows.push(row);
            }
        }
        transaction.commit().await?;
        return Ok(rows);
    }
    #[cfg(feature = "postgres")]
    {
        let attempts = batch
            .iter()
            .map(|update| update.attempt)
            .collect::<Vec<_>>();
        let errors = batch
            .iter()
            .map(|update| update.error_json.clone().map(Json))
            .collect::<Vec<_>>();
        let finalized_at = batch
            .iter()
            .map(|update| update.finalized_at)
            .collect::<Vec<_>>();
        let ids = batch.iter().map(|update| update.job_id).collect::<Vec<_>>();
        let metadata = batch
            .iter()
            .map(|update| Json(Value::Object(update.metadata.clone())))
            .collect::<Vec<_>>();
        let scheduled_at = batch
            .iter()
            .map(|update| update.scheduled_at)
            .collect::<Vec<_>>();
        let states = batch
            .iter()
            .map(|update| update.state.as_str())
            .collect::<Vec<_>>();
        let table = inner.schema.qualify("river_job");
        let state_type = inner.schema.qualify("river_job_state");
        let sql = format!(
            "WITH updates AS (\
            SELECT * FROM unnest(\
                $1::bigint[], $2::smallint[], $3::jsonb[], $4::timestamptz[], \
                $5::jsonb[], $6::timestamptz[], $7::text[]\
            ) AS update_params(\
                id, attempt, attempt_error, finalized_at, metadata, scheduled_at, state)\
         ) \
         UPDATE {table} AS job SET \
            attempt = CASE WHEN job.state = 'running' \
                AND NOT (updates.state IN ('available', 'retryable', 'scheduled') \
                    AND job.metadata ? 'cancel_attempted_at') \
                THEN updates.attempt ELSE job.attempt END, \
            errors = CASE WHEN job.state != 'running' OR updates.attempt_error IS NULL THEN job.errors \
                ELSE array_append(coalesce(job.errors, '{{}}'), updates.attempt_error) END, \
            finalized_at = CASE WHEN job.state != 'running' THEN job.finalized_at \
                WHEN updates.state IN ('available', 'retryable', 'scheduled') \
                AND job.metadata ? 'cancel_attempted_at' \
                THEN coalesce(updates.finalized_at, now()) ELSE updates.finalized_at END, \
            metadata = job.metadata || updates.metadata, \
            scheduled_at = CASE WHEN job.state = 'running' \
                AND NOT (updates.state IN ('available', 'retryable', 'scheduled') \
                    AND job.metadata ? 'cancel_attempted_at') \
                THEN coalesce(updates.scheduled_at, job.scheduled_at) ELSE job.scheduled_at END, \
            state = CASE WHEN job.state != 'running' THEN job.state \
                WHEN updates.state IN ('available', 'retryable', 'scheduled') \
                AND job.metadata ? 'cancel_attempted_at' \
                THEN 'cancelled'::{state_type} ELSE updates.state::{state_type} END \
         FROM updates WHERE job.id = updates.id \
         RETURNING {}, false AS unique_skipped_as_duplicate",
            job_projection("job")
        );
        let records = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(ids)
            .bind(attempts)
            .bind(errors)
            .bind(finalized_at)
            .bind(metadata)
            .bind(scheduled_at)
            .bind(states)
            .fetch_all(
                inner
                    .postgres_pool()
                    .expect("PostgreSQL completion path requires a PostgreSQL pool"),
            )
            .await?;
        return records.into_iter().map(JobRecord::into_job_row).collect();
    }
    #[allow(unreachable_code)]
    Err(Error::runtime(
        "database dispatch selected no supported backend".to_owned(),
    ))
}

fn finish_batched_completion(
    inner: &ClientInner,
    update: &CompletionUpdate,
    record: Option<JobRow>,
) {
    if let Some(row) = record {
        let event_kind = persisted_completion_event_kind(row.state, update.event_kind);
        let event = Event::job_with_statistics(
            event_kind,
            row,
            JobStatistics {
                complete_duration: update.timing.completion_started.elapsed(),
                queue_wait_duration: update.timing.queue_wait_duration,
                run_duration: update.timing.run_duration,
            },
        );
        let _ = inner.events.send(event);
    } else {
        debug!(
            job_id = update.job_id,
            "job result ignored because job is no longer running"
        );
    }
    remove_running_attempt(&inner.running, update.job_id, &update.cancellation);
}

fn remove_running_attempt(
    running: &Mutex<HashMap<i64, CancellationToken>>,
    job_id: i64,
    cancellation: &CancellationToken,
) {
    let mut running = running
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if running
        .get(&job_id)
        .is_some_and(|active| active == cancellation)
    {
        running.remove(&job_id);
    }
}

fn register_running_attempt(
    running: &Mutex<HashMap<i64, CancellationToken>>,
    pending_cancellations: &Mutex<HashMap<i64, std::time::Instant>>,
    job_id: i64,
    cancellation: &CancellationToken,
) {
    // Keep the locks in this order here and in `signal_running_attempt` so a
    // cancellation cannot fall between checking the active map and recording
    // a just-fetched attempt.
    let mut running = running
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    running.insert(job_id, cancellation.clone());
    let should_cancel = pending_cancellations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&job_id)
        .is_some();
    drop(running);
    if should_cancel {
        cancellation.cancel();
    }
}

fn signal_running_attempt(
    running: &Mutex<HashMap<i64, CancellationToken>>,
    pending_cancellations: &Mutex<HashMap<i64, std::time::Instant>>,
    fetch_registration_windows: &AtomicU64,
    job_id: i64,
) {
    let running = running
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(cancellation) = running.get(&job_id).cloned() {
        drop(running);
        cancellation.cancel();
        return;
    }
    if fetch_registration_windows.load(Ordering::SeqCst) == 0 {
        return;
    }

    let now = std::time::Instant::now();
    let mut pending = pending_cancellations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    pending.retain(|_, received_at| {
        now.saturating_duration_since(*received_at) <= PENDING_CANCELLATION_RETENTION
    });
    if pending.len() >= PENDING_CANCELLATION_LIMIT
        && let Some(oldest_job_id) = pending
            .iter()
            .min_by_key(|(_, received_at)| **received_at)
            .map(|(job_id, _)| *job_id)
    {
        pending.remove(&oldest_job_id);
    }
    pending.insert(job_id, now);
}

struct FetchRegistrationGuard<'a> {
    inner: &'a ClientInner,
}

impl<'a> FetchRegistrationGuard<'a> {
    fn new(inner: &'a ClientInner) -> Self {
        inner
            .fetch_registration_windows
            .fetch_add(1, Ordering::SeqCst);
        Self { inner }
    }
}

impl Drop for FetchRegistrationGuard<'_> {
    fn drop(&mut self) {
        // Synchronize the last-window transition with
        // `signal_running_attempt`, which holds this lock while deciding
        // whether to retain an unmatched cancellation.
        let _running = self
            .inner
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self
            .inner
            .fetch_registration_windows
            .fetch_sub(1, Ordering::SeqCst)
            == 1
        {
            self.inner
                .pending_cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
        }
    }
}

#[derive(Deserialize)]
struct ControlNotification {
    action: String,
    job_id: Option<i64>,
    queue: Option<String>,
}

#[derive(Deserialize)]
struct InsertNotification {
    queue: String,
}

#[derive(Deserialize)]
struct LeadershipNotification {
    action: String,
    leader_id: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) enum RuntimeNotification {
    Insert(String),
    LeadershipChanged,
    LeadershipRequestResign,
    QueueControl(String),
}

#[cfg(feature = "postgres")]
async fn run_notifications(
    inner: Arc<ClientInner>,
    cancel: CancellationToken,
    queue_notifications: broadcast::Sender<RuntimeNotification>,
    ready: oneshot::Sender<Result<(), String>>,
) -> Result<(), Error> {
    let schema = match inner.schema.as_deref() {
        Some(schema) => schema.to_owned(),
        None => sqlx::query_scalar::<_, Option<String>>("SELECT current_schema()")
            .fetch_one(
                inner
                    .postgres_pool()
                    .expect("PostgreSQL notifications require a PostgreSQL pool"),
            )
            .await?
            .ok_or_else(|| Error::invalid_job("PostgreSQL current_schema() is null".to_owned()))?,
    };
    let control_topic = format!("{schema}.{}", crate::NOTIFICATION_TOPIC_CONTROL);
    let insert_topic = format!("{schema}.{}", crate::NOTIFICATION_TOPIC_INSERT);
    let leadership_topic = format!("{schema}.{}", crate::NOTIFICATION_TOPIC_LEADERSHIP);
    let listener_result = async {
        let mut listener = PgListener::connect_with(
            inner
                .postgres_pool()
                .expect("PostgreSQL notifications require a PostgreSQL pool"),
        )
        .await?;
        listener.listen(&control_topic).await?;
        listener.listen(&insert_topic).await?;
        listener.listen(&leadership_topic).await?;
        Ok::<_, sqlx::Error>(listener)
    }
    .await;
    let mut listener = match listener_result {
        Ok(listener) => {
            let _ = ready.send(Ok(()));
            listener
        }
        Err(listener_error) => {
            let _ = ready.send(Err(listener_error.to_string()));
            return Err(listener_error.into());
        }
    };

    loop {
        let notification = tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            notification = listener.recv() => notification?,
        };
        if notification.channel() == insert_topic {
            if let Ok(payload) = serde_json::from_str::<InsertNotification>(notification.payload())
            {
                let _ = queue_notifications.send(RuntimeNotification::Insert(payload.queue));
            }
            continue;
        }
        if notification.channel() == leadership_topic {
            if let Ok(payload) =
                serde_json::from_str::<LeadershipNotification>(notification.payload())
            {
                if payload.action == "resigned"
                    && payload.leader_id.as_deref() == Some(inner.id.as_str())
                {
                    continue;
                }
                let notification = if payload.action == "request_resign" {
                    RuntimeNotification::LeadershipRequestResign
                } else {
                    RuntimeNotification::LeadershipChanged
                };
                let _ = queue_notifications.send(notification);
            }
            continue;
        }

        let Ok(payload) = serde_json::from_str::<ControlNotification>(notification.payload())
        else {
            warn!(
                payload = notification.payload(),
                "ignored invalid River control notification"
            );
            continue;
        };
        match payload.action.as_str() {
            "cancel" => {
                if let Some(job_id) = payload.job_id {
                    signal_running_attempt(
                        &inner.running,
                        &inner.pending_cancellations,
                        &inner.fetch_registration_windows,
                        job_id,
                    );
                }
            }
            "pause" | "resume" => {
                if let Some(queue) = payload.queue {
                    let _ = queue_notifications.send(RuntimeNotification::QueueControl(queue));
                }
            }
            _ => debug!(
                action = payload.action,
                "ignored unknown River control action"
            ),
        }
    }
}

#[cfg(feature = "sqlite")]
#[allow(
    clippy::too_many_lines,
    reason = "keeps SQLite outbox topic decoding and dispatch in one ordered polling loop"
)]
async fn run_sqlite_notifications(
    inner: Arc<ClientInner>,
    cancel: CancellationToken,
    queue_notifications: broadcast::Sender<RuntimeNotification>,
    ready: oneshot::Sender<Result<(), String>>,
) -> Result<(), Error> {
    let pool = inner
        .sqlite_pool()
        .expect("SQLite notifications require a SQLite pool");
    let initial = async {
        let mut connection = pool.acquire().await?;
        crate::database::sqlite::notification_last_id(&mut connection)
            .await
            .map_err(sqlite_backend_error)
    }
    .await;
    let mut after_id = match initial {
        Ok(last_id) => {
            let _ = ready.send(Ok(()));
            last_id
        }
        Err(error) => {
            let _ = ready.send(Err(error.to_string()));
            return Err(error);
        }
    };
    let mut notification_tick =
        tokio::time::interval(crate::database::sqlite::DEFAULT_NOTIFICATION_POLL_INTERVAL);
    notification_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            _ = notification_tick.tick() => {}
        }
        let notifications = {
            let mut connection = pool.acquire().await?;
            crate::database::sqlite::notification_poll(&mut connection, after_id, 1_000)
                .await
                .map_err(sqlite_backend_error)?
        };
        for notification in notifications {
            after_id = notification.id;
            match notification.topic.as_str() {
                crate::NOTIFICATION_TOPIC_INSERT => {
                    if let Ok(payload) =
                        serde_json::from_str::<InsertNotification>(&notification.payload)
                    {
                        let _ =
                            queue_notifications.send(RuntimeNotification::Insert(payload.queue));
                    }
                }
                crate::NOTIFICATION_TOPIC_LEADERSHIP => {
                    if let Ok(payload) =
                        serde_json::from_str::<LeadershipNotification>(&notification.payload)
                    {
                        if payload.action == "resigned"
                            && payload.leader_id.as_deref() == Some(inner.id.as_str())
                        {
                            continue;
                        }
                        let notification = if payload.action == "request_resign" {
                            RuntimeNotification::LeadershipRequestResign
                        } else {
                            RuntimeNotification::LeadershipChanged
                        };
                        let _ = queue_notifications.send(notification);
                    }
                }
                crate::NOTIFICATION_TOPIC_CONTROL => {
                    let Ok(payload) =
                        serde_json::from_str::<ControlNotification>(&notification.payload)
                    else {
                        warn!(
                            payload = notification.payload,
                            "ignored invalid River control notification"
                        );
                        continue;
                    };
                    match payload.action.as_str() {
                        "cancel" => {
                            if let Some(job_id) = payload.job_id {
                                signal_running_attempt(
                                    &inner.running,
                                    &inner.pending_cancellations,
                                    &inner.fetch_registration_windows,
                                    job_id,
                                );
                            }
                        }
                        "pause" | "resume" => {
                            if let Some(queue) = payload.queue {
                                let _ = queue_notifications
                                    .send(RuntimeNotification::QueueControl(queue));
                            }
                        }
                        _ => debug!(
                            action = payload.action,
                            "ignored unknown River control action"
                        ),
                    }
                }
                _ => {}
            }
        }
    }
}

async fn run_dynamic_queues(
    inner: Arc<ClientInner>,
    completion_sender: mpsc::Sender<CompletionUpdate>,
    fetch_cancel: CancellationToken,
    work_cancel: CancellationToken,
    notifications: broadcast::Sender<RuntimeNotification>,
    mut changes: watch::Receiver<u64>,
) -> Result<(), Error> {
    let mut active = HashMap::<String, (QueueConfig, CancellationToken, u64)>::new();
    let mut next_generation = 0_u64;
    let mut tasks = JoinSet::new();
    reconcile_queues(
        &inner,
        &completion_sender,
        &fetch_cancel,
        &work_cancel,
        &notifications,
        &mut active,
        &mut tasks,
        &mut next_generation,
    )?;

    loop {
        tokio::select! {
            () = fetch_cancel.cancelled() => break,
            change_result = changes.changed() => {
                if change_result.is_err() {
                    break;
                }
                reconcile_queues(
                    &inner,
                    &completion_sender,
                    &fetch_cancel,
                    &work_cancel,
                    &notifications,
                    &mut active,
                    &mut tasks,
                    &mut next_generation,
                )?;
            }
            result = tasks.join_next(), if !tasks.is_empty() => {
                let (name, generation, queue_cancel, result) = result
                    .ok_or_else(|| Error::runtime("dynamic queue task set closed".to_owned()))?
                    .map_err(Error::from_join)?;
                if active
                    .get(&name)
                    .is_some_and(|(_, _, current_generation)| *current_generation == generation)
                {
                    active.remove(&name);
                }
                if let Err(queue_error) = result
                    && !queue_cancel.is_cancelled()
                {
                    return Err(queue_error);
                }
            }
        }
    }

    for (_, queue_cancel, _) in active.values() {
        queue_cancel.cancel();
    }
    while let Some(result) = tasks.join_next().await {
        let (_, _, _, queue_result) = result.map_err(Error::from_join)?;
        if let Err(queue_error) = queue_result {
            debug!(error = %queue_error, "dynamic queue stopped with an error during shutdown");
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn reconcile_queues(
    inner: &Arc<ClientInner>,
    completion_sender: &mpsc::Sender<CompletionUpdate>,
    fetch_cancel: &CancellationToken,
    work_cancel: &CancellationToken,
    notifications: &broadcast::Sender<RuntimeNotification>,
    active: &mut HashMap<String, (QueueConfig, CancellationToken, u64)>,
    tasks: &mut JoinSet<(String, u64, CancellationToken, Result<(), Error>)>,
    next_generation: &mut u64,
) -> Result<(), Error> {
    let configured = inner
        .queues
        .read()
        .map_err(|_| Error::runtime("queue configuration lock poisoned".to_owned()))?
        .clone();
    for (name, (running_config, queue_cancel, _)) in &*active {
        if configured.get(name) != Some(running_config) {
            queue_cancel.cancel();
        }
    }
    active.retain(|name, (running_config, _, _)| configured.get(name) == Some(running_config));

    for (name, config) in configured {
        if active.contains_key(&name) {
            continue;
        }
        let queue_cancel = fetch_cancel.child_token();
        *next_generation = next_generation.wrapping_add(1);
        let generation = *next_generation;
        active.insert(
            name.clone(),
            (config.clone(), queue_cancel.clone(), generation),
        );
        let inner = Arc::clone(inner);
        let completion_sender = completion_sender.clone();
        let notifications = notifications.subscribe();
        let task_cancel = queue_cancel.clone();
        let task_name = name.clone();
        let work_cancel = work_cancel.child_token();
        tasks.spawn(async move {
            let result = run_queue(
                inner,
                completion_sender,
                task_name.clone(),
                config,
                task_cancel.clone(),
                work_cancel,
                notifications,
            )
            .await;
            (task_name, generation, task_cancel, result)
        });
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_queue(
    inner: Arc<ClientInner>,
    completion_sender: mpsc::Sender<CompletionUpdate>,
    queue: String,
    config: QueueConfig,
    fetch_cancel: CancellationToken,
    work_cancel: CancellationToken,
    mut notifications: broadcast::Receiver<RuntimeNotification>,
) -> Result<(), Error> {
    const START_RETRY_INTERVAL: Duration = Duration::from_millis(10);
    const START_TIMEOUT: Duration = Duration::from_secs(10);

    let start_time = tokio::time::Instant::now();
    let initial_queue = loop {
        match crate::storage::touch_queue(&inner, &queue).await {
            Ok(queue_row) => break queue_row,
            Err(queue_error) if start_time.elapsed() < START_TIMEOUT => {
                debug!(error = %queue_error, "River queue startup failed; retrying");
                tokio::select! {
                    () = fetch_cancel.cancelled() => return Ok(()),
                    () = tokio::time::sleep(START_RETRY_INTERVAL) => {}
                }
            }
            Err(queue_error) => return Err(queue_error),
        }
    };
    let mut paused = initial_queue.paused_at.is_some();
    let permits = Arc::new(Semaphore::new(config.max_workers));
    let mut jobs = JoinSet::new();
    let mut last_fetch = tokio::time::Instant::now() - config.fetch_cooldown;
    let mut heartbeat = tokio::time::interval(QUEUE_HEARTBEAT_INTERVAL);
    let mut poll = tokio::time::interval(config.fetch_poll_interval);
    let mut queue_config_poll = tokio::time::interval(QUEUE_CONFIG_POLL_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    queue_config_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        let (mut should_fetch, refresh_queue_state) = tokio::select! {
            () = fetch_cancel.cancelled() => break,
            _ = heartbeat.tick() => {
                if let Err(queue_error) = crate::storage::touch_queue(&inner, &queue).await {
                    error!(error = %queue_error, "River queue heartbeat failed; retrying");
                }
                (false, false)
            },
            _ = queue_config_poll.tick() => (false, true),
            _ = poll.tick() => (true, false),
            result = jobs.join_next(), if !jobs.is_empty() => {
                if let Some(Err(join_error)) = result {
                    error!(error = %join_error, "River queue task failed");
                }
                (true, false)
            },
            notification = notifications.recv() => match notification {
                Ok(RuntimeNotification::Insert(notification_queue)) => (
                    notification_queue == "*" || notification_queue == queue,
                    false,
                ),
                Ok(RuntimeNotification::QueueControl(notification_queue)) => (
                    false,
                    notification_queue == "*" || notification_queue == queue,
                ),
                Ok(
                    RuntimeNotification::LeadershipChanged
                        | RuntimeNotification::LeadershipRequestResign,
                )
                | Err(broadcast::error::RecvError::Closed) => (false, false),
                Err(broadcast::error::RecvError::Lagged(_)) => (true, true),
            },
        };

        if refresh_queue_state {
            match crate::storage::load_queue(&inner, &queue).await {
                Ok(Some(queue_row)) => {
                    let next_paused = queue_row.paused_at.is_some();
                    if next_paused != paused {
                        paused = next_paused;
                        let event_kind = if paused {
                            QueueEventKind::Paused
                        } else {
                            QueueEventKind::Resumed
                        };
                        let _ = inner.events.send(Event::queue(event_kind, queue_row));
                        should_fetch |= !paused;
                    }
                }
                Ok(None) => {}
                Err(queue_error) => {
                    error!(error = %queue_error, "River queue state refresh failed; retrying");
                    continue;
                }
            }
        }

        if !should_fetch || paused {
            continue;
        }
        let since_fetch = last_fetch.elapsed();
        if let Some(remaining) = config.fetch_cooldown.checked_sub(since_fetch) {
            tokio::time::sleep(remaining).await;
        }
        let available = permits.available_permits();
        if available == 0 {
            continue;
        }
        let registration_guard = FetchRegistrationGuard::new(&inner);
        let use_parallel_fetch = match inner.database.kind() {
            #[cfg(feature = "postgres")]
            DatabaseKind::Postgres => true,
            #[cfg(feature = "sqlite")]
            DatabaseKind::Sqlite => false,
        };
        let rows = if use_parallel_fetch
            && available >= PARALLEL_FETCH_MINIMUM
            && !inner.pilot.intercepts_fetch()
        {
            let first_maximum = available / 2;
            let second_maximum = available - first_maximum;
            let (first, second) = tokio::join!(
                fetch_jobs(&inner, &queue, first_maximum),
                fetch_jobs(&inner, &queue, second_maximum),
            );
            match (first, second) {
                (Ok(mut first), Ok(second)) => {
                    first.extend(second);
                    first
                }
                (Ok(rows), Err(fetch_error)) | (Err(fetch_error), Ok(rows)) => {
                    error!(
                        error = %fetch_error,
                        "one parallel River job fetch failed; working the successfully fetched jobs"
                    );
                    rows
                }
                (Err(fetch_error), Err(second_fetch_error)) => {
                    last_fetch = tokio::time::Instant::now();
                    error!(
                        error = %fetch_error,
                        secondary_error = %second_fetch_error,
                        "River job fetch failed; retrying"
                    );
                    continue;
                }
            }
        } else {
            match fetch_jobs(&inner, &queue, available).await {
                Ok(rows) => rows,
                Err(fetch_error) => {
                    last_fetch = tokio::time::Instant::now();
                    error!(error = %fetch_error, "River job fetch failed; retrying");
                    continue;
                }
            }
        };
        last_fetch = tokio::time::Instant::now();
        for row in rows {
            let permit = Arc::clone(&permits)
                .acquire_owned()
                .await
                .map_err(|_| Error::invalid_job("queue worker semaphore closed".to_owned()))?;
            let hard_cancel = work_cancel.child_token();
            let cancellation = hard_cancel.child_token();
            register_running_attempt(
                &inner.running,
                &inner.pending_cancellations,
                row.id,
                &cancellation,
            );
            let inner = Arc::clone(&inner);
            let completion_sender = completion_sender.clone();
            jobs.spawn(async move {
                execute_job(
                    inner,
                    row,
                    hard_cancel,
                    cancellation,
                    completion_sender,
                    permit,
                )
                .await;
            });
        }
        drop(registration_guard);
        while let Some(result) = jobs.try_join_next() {
            if let Err(join_error) = result {
                error!(error = %join_error, "River queue task failed");
            }
        }
    }

    while let Some(result) = jobs.join_next().await {
        if let Err(join_error) = result {
            error!(error = %join_error, "River queue task failed during shutdown");
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "keeps hooks and metrics identical across backend fetch paths"
)]
async fn fetch_jobs(
    inner: &ClientInner,
    queue: &str,
    maximum: usize,
) -> Result<Vec<JobRow>, Error> {
    let fetch_started = (!inner.hooks.is_empty()).then(std::time::Instant::now);
    let maximum = i32::try_from(maximum)
        .map_err(|_| Error::invalid_job("fetch maximum exceeds i32".to_owned()))?;
    #[cfg(feature = "sqlite")]
    if let Some(pool) = inner.sqlite_pool() {
        let params = crate::database::sqlite::ClaimJobs {
            client_id: &inner.id,
            limit: maximum,
            max_attempted_by: ATTEMPTED_BY_MAX,
            now: Utc::now(),
            queue,
        };
        let rows = if inner.pilot.intercepts_fetch() {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let selected_ids = inner
                .pilot
                .select_job_ids(
                    PilotDatabaseConnection::Sqlite(&mut transaction),
                    &FetchParams {
                        client_id: inner.id.clone(),
                        database: inner.pilot_database_config(),
                        kinds: inner
                            .workers
                            .kinds()
                            .into_iter()
                            .map(str::to_owned)
                            .collect(),
                        maximum,
                        queue: queue.to_owned(),
                    },
                )
                .await
                .map_err(|source| Error::Extension {
                    phase: "fetch selection",
                    source,
                })?;
            let rows = match selected_ids {
                Some(ids) => {
                    crate::database::sqlite::claim_selected(&mut transaction, &params, &ids).await
                }
                None => crate::database::sqlite::claim(&mut transaction, &params).await,
            }
            .map_err(sqlite_backend_error)?;
            transaction.commit().await?;
            rows
        } else {
            let mut connection = pool.acquire().await?;
            crate::database::sqlite::claim(&mut connection, &params)
                .await
                .map_err(sqlite_backend_error)?
        };
        if let Some(fetch_started) = fetch_started {
            for metric in [
                Metric::JobGetAvailableDuration(fetch_started.elapsed()),
                Metric::JobGetAvailableCount(u64::try_from(rows.len()).unwrap_or(u64::MAX)),
            ] {
                for hook in &inner.hooks {
                    if let Err(hook_error) = hook.metric_emit(metric).await {
                        error!(error = %hook_error, "River metric hook failed");
                    }
                }
            }
        }
        return Ok(rows);
    }
    #[cfg(feature = "postgres")]
    {
        let table = inner.schema.qualify("river_job");
        let queue_table = inner.schema.qualify("river_queue");
        let oss_sql = format!(
            "WITH locked AS (\
            SELECT id FROM {table} WHERE state = 'available' AND queue = $1 AND scheduled_at <= now() \
                AND NOT EXISTS (SELECT 1 FROM {queue_table} WHERE name = $1 AND paused_at IS NOT NULL) \
            ORDER BY priority, scheduled_at, id LIMIT $2 FOR UPDATE SKIP LOCKED\
         ) UPDATE {table} AS job \
            SET state = 'running', attempt = job.attempt + 1, attempted_at = now(), \
                attempted_by = array_append(\
                    CASE WHEN array_length(job.attempted_by, 1) >= $4 \
                         THEN job.attempted_by[array_length(job.attempted_by, 1) + 2 - $4:] \
                         ELSE job.attempted_by END, $3) \
            FROM locked WHERE job.id = locked.id \
            RETURNING {}, false AS unique_skipped_as_duplicate",
            job_projection("job")
        );
        let records = if inner.pilot.intercepts_fetch() {
            let kinds = inner
                .workers
                .kinds()
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            let mut transaction = inner
                .postgres_pool()
                .expect("PostgreSQL fetch extension requires a PostgreSQL pool")
                .begin()
                .await?;
            let selected_ids = inner
                .pilot
                .select_job_ids(
                    PilotDatabaseConnection::Postgres(&mut transaction),
                    &FetchParams {
                        client_id: inner.id.clone(),
                        database: inner.pilot_database_config(),
                        kinds: kinds.clone(),
                        maximum,
                        queue: queue.to_owned(),
                    },
                )
                .await
                .map_err(|source| Error::Extension {
                    phase: "fetch selection",
                    source,
                })?;
            let records = if let Some(selected_ids) = selected_ids {
                let sql = format!(
                    "UPDATE {table} AS job SET state = 'running', attempt = job.attempt + 1, \
                    attempted_at = now(), attempted_by = array_append(\
                        CASE WHEN array_length(job.attempted_by, 1) >= $3 \
                             THEN job.attempted_by[array_length(job.attempted_by, 1) + 2 - $3:] \
                             ELSE job.attempted_by END, $2) \
                WHERE id = ANY($1::bigint[]) AND state = 'available' \
                RETURNING {}, false AS unique_skipped_as_duplicate",
                    job_projection("job")
                );
                sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
                    .bind(selected_ids)
                    .bind(&inner.id)
                    .bind(ATTEMPTED_BY_MAX)
                    .fetch_all(&mut *transaction)
                    .await?
            } else {
                fetch_oss_records(&mut *transaction, oss_sql, queue, maximum, &inner.id).await?
            };
            transaction.commit().await?;
            records
        } else {
            fetch_oss_records(
                inner
                    .postgres_pool()
                    .expect("PostgreSQL fetch path requires a PostgreSQL pool"),
                oss_sql,
                queue,
                maximum,
                &inner.id,
            )
            .await?
        };
        let rows = records
            .into_iter()
            .map(JobRecord::into_job_row)
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(fetch_started) = fetch_started {
            for metric in [
                Metric::JobGetAvailableDuration(fetch_started.elapsed()),
                Metric::JobGetAvailableCount(u64::try_from(rows.len()).unwrap_or(u64::MAX)),
            ] {
                for hook in &inner.hooks {
                    if let Err(hook_error) = hook.metric_emit(metric).await {
                        error!(error = %hook_error, "River metric hook failed");
                    }
                }
            }
        }
        return Ok(rows);
    }
    #[allow(unreachable_code)]
    Err(Error::runtime(
        "database dispatch selected no supported backend".to_owned(),
    ))
}

#[cfg(feature = "postgres")]
async fn fetch_oss_records<'executor, E>(
    executor: E,
    sql: String,
    queue: &str,
    maximum: i32,
    client_id: &str,
) -> Result<Vec<JobRecord>, sqlx::Error>
where
    E: Executor<'executor, Database = Postgres>,
{
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(queue)
        .bind(maximum)
        .bind(client_id)
        .bind(ATTEMPTED_BY_MAX)
        .fetch_all(executor)
        .await
}

#[allow(clippy::too_many_lines)]
async fn execute_job(
    inner: Arc<ClientInner>,
    row: JobRow,
    hard_cancel: CancellationToken,
    cancellation: CancellationToken,
    completion_sender: mpsc::Sender<CompletionUpdate>,
    worker_permit: OwnedSemaphorePermit,
) {
    let span = info_span!("river_job", job_id = row.id, job_kind = %row.kind, queue = %row.queue);
    async move {
        let queue_wait_duration = row
            .attempted_at
            .and_then(|attempted_at| {
                (attempted_at - row.scheduled_at.max(row.created_at))
                    .to_std()
                    .ok()
            })
            .unwrap_or_default();
        let context = WorkContext::for_job(
            Client {
                inner: Arc::clone(&inner),
            },
            cancellation.clone(),
            row.id,
            &row.metadata,
        );
        let mut worker_row = row.clone();
        let worker_context = context.clone();
        let worker_inner = Arc::clone(&inner);
        let mut worker_task = tokio::spawn(async move {
            for hook in &worker_inner.hooks {
                hook.work_begin(&worker_context, &mut worker_row)
                    .await
                    .map_err(boxed_extension_error)?;
            }
            for middleware in &worker_inner.work_middleware {
                middleware
                    .before_work(&worker_context, &mut worker_row)
                    .await
                    .map_err(boxed_extension_error)?;
            }
            let result = worker_inner
                .workers
                .work(worker_context.clone(), &worker_row)
                .await;
            let public_result = erased_work_result(&result);
            for middleware in worker_inner.work_middleware.iter().rev() {
                middleware
                    .after_work(&worker_context, &worker_row, &public_result)
                    .await
                    .map_err(boxed_extension_error)?;
            }
            for hook in &worker_inner.hooks {
                hook.work_end(&worker_context, &worker_row, &public_result)
                    .await
                    .map_err(boxed_extension_error)?;
            }
            result
        });

        let timeout = match inner.workers.timeout(&row) {
            Ok(WorkerTimeout::After(timeout)) => Some(timeout),
            Ok(WorkerTimeout::ClientDefault) => inner.job_timeout,
            Ok(WorkerTimeout::Disabled) => None,
            Err(timeout_error) => {
                debug!(error = %timeout_error, "could not evaluate worker timeout; using client default");
                inner.job_timeout
            }
        };
        let mut cancellation_cause = None;
        let work_started = std::time::Instant::now();
        let result = if let Some(timeout) = timeout {
            tokio::select! {
                result = &mut worker_task => worker_join_result(result),
                () = cancellation.cancelled() => {
                    cancellation_cause = Some(if hard_cancel.is_cancelled() {
                        CancellationCause::Shutdown
                    } else {
                        CancellationCause::Remote
                    });
                    cancellation.cancel();
                    finish_cancelled_task(&mut worker_task, inner.job_stuck_threshold).await
                }
                () = tokio::time::sleep(timeout) => {
                    cancellation_cause = Some(CancellationCause::Timeout);
                    cancellation.cancel();
                    finish_cancelled_task(&mut worker_task, inner.job_stuck_threshold).await
                }
            }
        } else {
            tokio::select! {
                result = &mut worker_task => worker_join_result(result),
                () = cancellation.cancelled() => {
                    cancellation_cause = Some(if hard_cancel.is_cancelled() {
                        CancellationCause::Shutdown
                    } else {
                        CancellationCause::Remote
                    });
                    cancellation.cancel();
                    finish_cancelled_task(&mut worker_task, inner.job_stuck_threshold).await
                }
            }
        };

        // A cooperative worker can observe cancellation and return before this
        // select polls the cancellation branch. Preserve the cancellation cause
        // in that race so remote cancellation still gets its canonical outcome.
        if cancellation_cause.is_none() && cancellation.is_cancelled() {
            cancellation_cause = Some(if hard_cancel.is_cancelled() {
                CancellationCause::Shutdown
            } else {
                CancellationCause::Remote
            });
        }

        let run_duration = work_started.elapsed();
        let mut result = result;
        let was_aborted = result
            .as_ref()
            .is_err_and(|failure| matches!(failure.kind, WorkerFailureKind::Aborted));
        if let Some(resumable_failure) = context.resumable_finish(result.is_err()).await {
            result = Err(WorkerFailure {
                error: resumable_failure,
                kind: WorkerFailureKind::Error,
                source: None,
                trace: String::new(),
            });
        }
        if cancellation_cause == Some(CancellationCause::Shutdown)
            && let Err(failure) = &mut result
        {
            failure.error.clear();
            failure.error.push_str("job interrupted by client shutdown");
            failure.kind = WorkerFailureKind::Interrupted;
            failure.source = None;
            failure.trace.clear();
        }
        if cancellation_cause == Some(CancellationCause::Remote)
            && !matches!(result, Ok(WorkOutcome::Complete))
        {
            result = Err(WorkerFailure {
                error: "JobCancelError: job cancelled remotely".to_owned(),
                kind: WorkerFailureKind::Cancelled,
                source: None,
                trace: String::new(),
            });
        }
        let work_result = public_work_result(&result);
        let mut error_handler_result = ErrorHandlerDecision::default();
        if let Some(error_handler) = &inner.error_handler
            && matches!(
                work_result,
                WorkResult::Aborted | WorkResult::Failed(_) | WorkResult::Panicked(_)
            )
        {
            match error_handler
                .handle_error(&context, &row, &work_result)
                .await
            {
                Ok(handler_result) => error_handler_result = handler_result,
                Err(handler_error) => {
                    error!(error = %handler_error, "River error handler failed");
                }
            }
        }
        if was_aborted
            && let Some(error_handler) = &inner.error_handler
            && let Err(handler_error) = error_handler.handle_stuck(&row).await
        {
            error!(error = %handler_error, "River stuck handler failed");
        }
        let metadata_updates = context.metadata_updates().await;
        let completion = CompletionAttempt {
            cancellation: cancellation.clone(),
            timing: CompletionTiming {
                completion_started: std::time::Instant::now(),
                queue_wait_duration,
                run_duration,
            },
        };
        let completion_enqueued = match persist_result(
            &inner,
            &row,
            &completion,
            result,
            metadata_updates,
            error_handler_result,
            &completion_sender,
        )
        .await
        {
            Ok(PersistResult::Finished(Some(event))) => {
                let Event::Job(job_event) = *event else {
                    unreachable!("job persistence returns only job events")
                };
                let event = Event::job_with_statistics(job_event.kind, job_event.job, JobStatistics {
                    complete_duration: completion.timing.completion_started.elapsed(),
                    queue_wait_duration,
                    run_duration,
                });
                let _ = inner.events.send(event);
                false
            }
            Ok(PersistResult::Enqueued) => true,
            Ok(PersistResult::Finished(None)) => false,
            Err(operation_error) => {
                error!(error = %operation_error, "failed to persist River job result");
                false
            }
        };
        drop(worker_permit);
        if completion_enqueued {
            return;
        }
        remove_running_attempt(&inner.running, row.id, &cancellation);
    }
    .instrument(span)
    .await;
}

type WorkerResult = Result<WorkOutcome, WorkerFailure>;

#[derive(Debug)]
struct WorkerFailure {
    error: String,
    kind: WorkerFailureKind,
    source: Option<WorkError>,
    trace: String,
}

#[derive(Debug)]
enum WorkerFailureKind {
    Aborted,
    Cancelled,
    Error,
    Interrupted,
    Panic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CancellationCause {
    Remote,
    Shutdown,
    Timeout,
}

async fn finish_cancelled_task(
    worker_task: &mut tokio::task::JoinHandle<Result<WorkOutcome, WorkError>>,
    stuck_threshold: Duration,
) -> WorkerResult {
    if let Ok(result) = tokio::time::timeout(stuck_threshold, &mut *worker_task).await {
        return worker_join_result(result);
    }
    warn!(
        ?stuck_threshold,
        "River job remained active after cancellation; aborting task"
    );
    worker_task.abort();
    match tokio::time::timeout(Duration::from_millis(100), &mut *worker_task).await {
        Ok(Err(join_error)) if join_error.is_cancelled() => Err(WorkerFailure {
            error: "job aborted after ignoring cancellation".to_owned(),
            kind: WorkerFailureKind::Aborted,
            source: None,
            trace: String::new(),
        }),
        Ok(result) => worker_join_result(result),
        Err(_) => Err(WorkerFailure {
            error: "job remained stuck after Tokio task abort".to_owned(),
            kind: WorkerFailureKind::Aborted,
            source: None,
            trace: String::new(),
        }),
    }
}

fn worker_join_result(
    result: Result<Result<WorkOutcome, WorkError>, tokio::task::JoinError>,
) -> WorkerResult {
    match result {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(worker_error)) => Err(WorkerFailure {
            error: worker_error.to_string(),
            kind: WorkerFailureKind::Error,
            source: Some(worker_error),
            trace: String::new(),
        }),
        Err(join_error) => Err(WorkerFailure {
            error: if join_error.is_panic() {
                format!("job panicked: {join_error}")
            } else {
                format!("job task cancelled: {join_error}")
            },
            kind: if join_error.is_panic() {
                WorkerFailureKind::Panic
            } else {
                WorkerFailureKind::Aborted
            },
            source: None,
            trace: format!("{join_error:?}"),
        }),
    }
}

fn worker_failure_from_source(error: BoxError) -> WorkerFailure {
    let error = WorkError::new(error);
    WorkerFailure {
        error: error.to_string(),
        kind: WorkerFailureKind::Error,
        source: Some(error),
        trace: String::new(),
    }
}

fn boxed_extension_error(error: Error) -> WorkError {
    WorkError::new(Box::new(error))
}

fn erased_work_result(result: &Result<WorkOutcome, WorkError>) -> WorkResult {
    match result {
        Ok(WorkOutcome::Cancel) => WorkResult::Cancelled,
        Ok(WorkOutcome::Complete) => WorkResult::Completed,
        Ok(WorkOutcome::Discard) => WorkResult::Discarded,
        Ok(WorkOutcome::Snooze(duration)) => WorkResult::Snoozed(*duration),
        Err(error) => WorkResult::Failed(error.clone()),
    }
}

fn public_work_result(result: &WorkerResult) -> WorkResult {
    match result {
        Ok(WorkOutcome::Cancel) => WorkResult::Cancelled,
        Ok(WorkOutcome::Complete) => WorkResult::Completed,
        Ok(WorkOutcome::Discard) => WorkResult::Discarded,
        Ok(WorkOutcome::Snooze(duration)) => WorkResult::Snoozed(*duration),
        Err(failure) => match failure.kind {
            WorkerFailureKind::Aborted => WorkResult::Aborted,
            WorkerFailureKind::Cancelled => WorkResult::Cancelled,
            WorkerFailureKind::Error => {
                WorkResult::Failed(failure.source.clone().unwrap_or_else(|| {
                    WorkError::new(Box::new(std::io::Error::other(failure.error.clone())))
                }))
            }
            WorkerFailureKind::Interrupted => WorkResult::Interrupted,
            WorkerFailureKind::Panic => WorkResult::Panicked(failure.error.clone()),
        },
    }
}

#[allow(clippy::too_many_lines)]
async fn persist_result(
    inner: &ClientInner,
    row: &JobRow,
    completion: &CompletionAttempt,
    result: WorkerResult,
    metadata_updates: Map<String, Value>,
    error_handler_result: ErrorHandlerDecision,
    completion_sender: &mpsc::Sender<CompletionUpdate>,
) -> Result<PersistResult, Error> {
    let now = Utc::now();
    let (state, finalized_at, scheduled_at, attempt, attempt_error, metadata, event_kind) =
        match result {
            Ok(WorkOutcome::Complete) => (
                JobState::Completed,
                Some(now),
                None,
                row.attempt,
                None,
                metadata_updates,
                JobEventKind::Completed,
            ),
            Ok(WorkOutcome::Cancel) => (
                JobState::Cancelled,
                Some(now),
                None,
                row.attempt,
                Some(AttemptError {
                    at: row.attempted_at.unwrap_or(now),
                    attempt: row.attempt,
                    error: "job cancelled by worker".to_owned(),
                    trace: String::new(),
                }),
                metadata_updates,
                JobEventKind::Cancelled,
            ),
            Ok(WorkOutcome::Discard) => (
                JobState::Discarded,
                Some(now),
                None,
                row.attempt,
                Some(AttemptError {
                    at: row.attempted_at.unwrap_or(now),
                    attempt: row.attempt,
                    error: "job discarded by worker".to_owned(),
                    trace: String::new(),
                }),
                metadata_updates,
                JobEventKind::Failed,
            ),
            Ok(WorkOutcome::Snooze(duration)) => {
                let scheduled_at = now
                    + chrono::Duration::from_std(duration)
                        .map_err(|error| Error::invalid_job(error.to_string()))?;
                let state = if duration <= inner.maintenance.scheduler_interval {
                    JobState::Available
                } else {
                    JobState::Scheduled
                };
                let mut metadata = metadata_updates;
                let snoozes = row
                    .metadata
                    .get("snoozes")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    + 1;
                metadata.insert("snoozes".to_owned(), Value::from(snoozes));
                (
                    state,
                    None,
                    Some(scheduled_at),
                    row.attempt - 1,
                    None,
                    metadata,
                    JobEventKind::Snoozed,
                )
            }
            Err(failure) => {
                if matches!(failure.kind, WorkerFailureKind::Interrupted) {
                    return persist_interrupted(inner, row, metadata_updates)
                        .await
                        .map(|event| PersistResult::Finished(event.map(Box::new)));
                }
                let retry_error = failure.source.clone().unwrap_or_else(|| {
                    WorkError::new(Box::new(std::io::Error::other(failure.error.clone())))
                });
                let attempt_error = AttemptError {
                    at: row.attempted_at.unwrap_or(now),
                    attempt: row.attempt,
                    error: failure.error,
                    trace: failure.trace,
                };
                if matches!(failure.kind, WorkerFailureKind::Cancelled)
                    || error_handler_result == ErrorHandlerDecision::Cancel
                {
                    (
                        JobState::Cancelled,
                        Some(now),
                        None,
                        row.attempt,
                        Some(attempt_error),
                        metadata_updates,
                        JobEventKind::Cancelled,
                    )
                } else if row.attempt >= row.max_attempts {
                    (
                        JobState::Discarded,
                        Some(now),
                        None,
                        row.attempt,
                        Some(attempt_error),
                        metadata_updates,
                        JobEventKind::Failed,
                    )
                } else {
                    let worker_retry_after = inner
                        .workers
                        .next_retry(row, &retry_error, now)
                        .unwrap_or_else(|retry_error| {
                            debug!(error = %retry_error, "could not evaluate worker retry override");
                            None
                        });
                    let delay = worker_retry_after.unwrap_or_else(|| {
                        inner
                            .retry_policy
                            .next_retry(row, &attempt_error.error, now)
                    });
                    let scheduled_at = now
                        + chrono::Duration::from_std(delay)
                            .map_err(|error| Error::invalid_job(error.to_string()))?;
                    let state = if delay <= inner.maintenance.scheduler_interval {
                        JobState::Available
                    } else {
                        JobState::Retryable
                    };
                    (
                        state,
                        None,
                        Some(scheduled_at),
                        row.attempt,
                        Some(attempt_error),
                        metadata_updates,
                        JobEventKind::Failed,
                    )
                }
            }
        };

    #[cfg(feature = "postgres")]
    let table = inner.schema.qualify("river_job");
    #[cfg(feature = "postgres")]
    let state_type = inner.schema.qualify("river_job_state");
    #[cfg(feature = "postgres")]
    let sql = format!(
        "UPDATE {table} AS job SET \
            attempt = CASE WHEN state = 'running' \
                                AND NOT ($7::text IN ('available', 'retryable', 'scheduled') \
                                    AND metadata ? 'cancel_attempted_at') \
                           THEN $2 ELSE attempt END, \
            errors = CASE WHEN state != 'running' OR $3::jsonb IS NULL THEN errors ELSE array_append(coalesce(errors, '{{}}'), $3::jsonb) END, \
            finalized_at = CASE WHEN state != 'running' THEN finalized_at \
                                WHEN $7::text IN ('available', 'retryable', 'scheduled') AND metadata ? 'cancel_attempted_at' \
                                THEN coalesce($4, now()) ELSE $4 END, \
            metadata = metadata || $5::jsonb, \
            scheduled_at = CASE WHEN state = 'running' \
                                     AND NOT ($7::text IN ('available', 'retryable', 'scheduled') \
                                         AND metadata ? 'cancel_attempted_at') \
                                THEN coalesce($6, scheduled_at) ELSE scheduled_at END, \
            state = CASE WHEN state != 'running' THEN state \
                         WHEN $7::text IN ('available', 'retryable', 'scheduled') AND metadata ? 'cancel_attempted_at' \
                         THEN 'cancelled'::{state_type} ELSE $7::text::{state_type} END \
         WHERE id = $1 \
         RETURNING {}, false AS unique_skipped_as_duplicate",
        job_projection("job")
    );
    let error_json = attempt_error
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;
    if !inner.pilot.intercepts_completion() {
        completion_sender
            .send(CompletionUpdate {
                attempt,
                cancellation: completion.cancellation.clone(),
                error_json,
                event_kind,
                finalized_at,
                job_id: row.id,
                metadata,
                scheduled_at,
                state,
                timing: completion.timing,
            })
            .await
            .map_err(|_| Error::runtime("completion batcher stopped".to_owned()))?;
        return Ok(PersistResult::Enqueued);
    }

    #[cfg(feature = "sqlite")]
    if let Some(pool) = inner.sqlite_pool() {
        let record = {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let completion_action = inner
                .pilot
                .before_job_completion(
                    PilotDatabaseConnection::Sqlite(&mut transaction),
                    &CompletionParams {
                        database: inner.pilot_database_config(),
                        job_id: row.id,
                        metadata_updates: metadata.clone(),
                        state: state.as_str().to_owned(),
                    },
                )
                .await
                .map_err(|source| Error::Extension {
                    phase: "job completion",
                    source,
                })?;
            let record = match completion_action {
                CompletionAction::Continue => {
                    let updated = crate::database::sqlite::complete(
                        &mut transaction,
                        &crate::database::sqlite::CompleteJob {
                            attempt: Some(attempt),
                            error: attempt_error.as_ref(),
                            finalized_at,
                            id: row.id,
                            metadata_updates: Some(&metadata),
                            now,
                            scheduled_at,
                            state,
                        },
                    )
                    .await
                    .map_err(sqlite_backend_error)?;
                    match updated {
                        Some(row) => Some(row),
                        None => crate::database::sqlite::merge_metadata_if_not_running(
                            &mut transaction,
                            row.id,
                            &metadata,
                        )
                        .await
                        .map_err(sqlite_backend_error)?,
                    }
                }
                CompletionAction::Handled => crate::database::sqlite::get(&mut transaction, row.id)
                    .await
                    .map_err(sqlite_backend_error)?,
            };
            transaction.commit().await?;
            record
        };
        let Some(row) = record else {
            debug!(
                job_id = row.id,
                "job result ignored because the job no longer exists"
            );
            return Ok(PersistResult::Finished(None));
        };
        let event_kind = persisted_completion_event_kind(row.state, event_kind);
        return Ok(PersistResult::Finished(Some(Box::new(Event::job(
            event_kind, row,
        )))));
    }
    #[cfg(feature = "postgres")]
    if let Some(pool) = inner.postgres_pool() {
        let record = {
            let mut transaction = pool.begin().await?;
            let completion_action = inner
                .pilot
                .before_job_completion(
                    PilotDatabaseConnection::Postgres(&mut transaction),
                    &CompletionParams {
                        database: inner.pilot_database_config(),
                        job_id: row.id,
                        metadata_updates: metadata.clone(),
                        state: state.as_str().to_owned(),
                    },
                )
                .await
                .map_err(|source| Error::Extension {
                    phase: "job completion",
                    source,
                })?;
            let record = match completion_action {
                CompletionAction::Continue => {
                    persist_completion_update(
                        &mut *transaction,
                        &sql,
                        row.id,
                        attempt,
                        error_json.as_ref(),
                        finalized_at,
                        &metadata,
                        scheduled_at,
                        state,
                    )
                    .await?
                }
                CompletionAction::Handled => {
                    let sql = format!(
                        "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
                 WHERE id = $1 LIMIT 1",
                        job_projection("job")
                    );
                    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
                        .bind(row.id)
                        .fetch_optional(&mut *transaction)
                        .await?
                }
            };
            transaction.commit().await?;
            record
        };
        let Some(record) = record else {
            debug!(
                job_id = row.id,
                "job result ignored because the job no longer exists"
            );
            return Ok(PersistResult::Finished(None));
        };
        let row = record.into_job_row()?;
        let event_kind = persisted_completion_event_kind(row.state, event_kind);
        return Ok(PersistResult::Finished(Some(Box::new(Event::job(
            event_kind, row,
        )))));
    }
    #[allow(unreachable_code)]
    Err(Error::runtime(
        "database dispatch selected no supported backend".to_owned(),
    ))
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "postgres")]
async fn persist_completion_update<'executor, E>(
    executor: E,
    sql: &str,
    job_id: i64,
    attempt: i16,
    error_json: Option<&Value>,
    finalized_at: Option<DateTime<Utc>>,
    metadata: &Map<String, Value>,
    scheduled_at: Option<DateTime<Utc>>,
    state: JobState,
) -> Result<Option<JobRecord>, Error>
where
    E: Executor<'executor, Database = Postgres>,
{
    Ok(
        sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql.to_owned()))
            .bind(job_id)
            .bind(attempt)
            .bind(error_json.map(Json))
            .bind(finalized_at)
            .bind(Json(metadata))
            .bind(scheduled_at)
            .bind(state.as_str())
            .fetch_optional(executor)
            .await?,
    )
}

async fn persist_interrupted(
    inner: &ClientInner,
    row: &JobRow,
    metadata_updates: Map<String, Value>,
) -> Result<Option<Event>, Error> {
    #[cfg(feature = "sqlite")]
    if let Some(pool) = inner.sqlite_pool() {
        let mut transaction = crate::database::begin_sqlite_write(pool).await?;
        let updated = crate::database::sqlite::interrupt(
            &mut transaction,
            row.id,
            &metadata_updates,
            Utc::now(),
        )
        .await
        .map_err(sqlite_backend_error)?;
        let updated = match updated {
            Some(row) => Some(row),
            None => crate::database::sqlite::merge_metadata_if_not_running(
                &mut transaction,
                row.id,
                &metadata_updates,
            )
            .await
            .map_err(sqlite_backend_error)?,
        };
        if let Some(updated) = &updated
            && updated.state == JobState::Available
        {
            let payload = serde_json::json!({"queue": updated.queue}).to_string();
            crate::database::sqlite::notification_insert(
                &mut transaction,
                &[crate::database::sqlite::NotificationInput {
                    payload: &payload,
                    topic: crate::NOTIFICATION_TOPIC_INSERT,
                }],
            )
            .await
            .map_err(sqlite_backend_error)?;
        }
        transaction.commit().await?;
        return Ok(updated.map(|row| {
            Event::job(
                persisted_completion_event_kind(row.state, JobEventKind::Interrupted),
                row,
            )
        }));
    }
    #[cfg(feature = "postgres")]
    {
        let table = inner.schema.qualify("river_job");
        let sql = format!(
            "UPDATE {table} AS job SET \
         attempt = CASE WHEN state = 'running' THEN greatest(job.attempt - 1, 0) ELSE attempt END, \
         attempted_at = CASE WHEN state = 'running' THEN NULL ELSE attempted_at END, \
         finalized_at = CASE WHEN state = 'running' THEN NULL ELSE finalized_at END, \
         metadata = metadata || $2::jsonb, \
         scheduled_at = CASE WHEN state = 'running' THEN now() ELSE scheduled_at END, \
         state = CASE WHEN state = 'running' THEN 'available'::{state_type} ELSE state END \
         WHERE id = $1 \
         RETURNING {}, false AS unique_skipped_as_duplicate",
            job_projection("job"),
            state_type = inner.schema.qualify("river_job_state")
        );
        let record = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(row.id)
            .bind(Json(metadata_updates))
            .fetch_optional(
                inner
                    .postgres_pool()
                    .expect("PostgreSQL completion path requires a PostgreSQL pool"),
            )
            .await?;
        return Ok(record.map(JobRecord::into_job_row).transpose()?.map(|row| {
            Event::job(
                persisted_completion_event_kind(row.state, JobEventKind::Interrupted),
                row,
            )
        }));
    }
    #[allow(unreachable_code)]
    Err(Error::runtime(
        "database dispatch selected no supported backend".to_owned(),
    ))
}

fn default_client_id() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_owned());
    format!("{host}-{}", std::process::id())
}

fn sort_claimed_jobs(rows: &mut [JobRow]) {
    rows.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| left.scheduled_at.cmp(&right.scheduled_at))
            .then_with(|| left.id.cmp(&right.id))
    });
}

struct ExtensionInsertWire {
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<u8>,
}

struct PreparedFastInsert {
    encoded_args: Value,
    kind: String,
    max_attempts: i16,
    metadata: Map<String, Value>,
    now: DateTime<Utc>,
    priority: i16,
    queue: String,
    scheduled_at: DateTime<Utc>,
    state: JobState,
    tags: Vec<String>,
    unique_key: Option<[u8; 32]>,
    unique_states: Option<u8>,
}

impl PreparedFastInsert {
    fn new(
        insert: InsertContext,
        unique_fields: &[&str],
        now: DateTime<Utc>,
    ) -> Result<Self, Error> {
        let InsertContext {
            encoded_args,
            kind,
            opts,
        } = insert;
        let unique_key = build_unique_key_parts(
            &kind,
            unique_fields,
            &encoded_args,
            now,
            &opts.unique,
            &opts.queue,
            opts.scheduled_at,
        )?;
        let unique_states = unique_key.map(|_| opts.unique.state_bitmask());
        let scheduled_at = opts.scheduled_at.unwrap_or(now);
        let state = if opts.pending {
            JobState::Pending
        } else if opts.scheduled_at.is_some() {
            JobState::Scheduled
        } else {
            JobState::Available
        };
        Ok(Self {
            encoded_args,
            kind,
            max_attempts: opts.max_attempts,
            metadata: opts.metadata,
            now,
            priority: opts.priority,
            queue: opts.queue,
            scheduled_at,
            state,
            tags: opts.tags,
            unique_key,
            unique_states,
        })
    }
}

#[cfg(feature = "postgres")]
fn encode_fast_copy(jobs: &[PreparedFastInsert]) -> Vec<u8> {
    let mut output = String::new();
    for job in jobs {
        let unique_key = job.unique_key.map(|key| {
            let mut value = String::from("\\x");
            for byte in key {
                write!(value, "{byte:02x}").expect("writing to a string cannot fail");
            }
            value
        });
        let unique_states = job.unique_states.map(|states| format!("{states:08b}"));
        let fields = [
            Some(job.encoded_args.to_string()),
            Some(job.now.to_rfc3339_opts(SecondsFormat::Micros, true)),
            Some(job.kind.clone()),
            Some(job.max_attempts.to_string()),
            Some(Value::Object(job.metadata.clone()).to_string()),
            Some(job.priority.to_string()),
            Some(job.queue.clone()),
            Some(
                job.scheduled_at
                    .to_rfc3339_opts(SecondsFormat::Micros, true),
            ),
            Some(job.state.as_str().to_owned()),
            Some(postgres_array(&job.tags)),
            unique_key,
            unique_states,
        ];
        for (index, field) in fields.iter().enumerate() {
            if index > 0 {
                output.push(',');
            }
            match field {
                Some(field) => {
                    output.push('"');
                    output.push_str(&field.replace('"', "\"\""));
                    output.push('"');
                }
                None => output.push_str("\\N"),
            }
        }
        output.push('\n');
    }
    output.into_bytes()
}

#[cfg(feature = "postgres")]
fn postgres_array(values: &[String]) -> String {
    let values = values
        .iter()
        .map(|value| format!(r#""{}""#, value.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{values}}}")
}

pub(crate) fn default_retry_delay(row: &JobRow, now: DateTime<Utc>, seed: u64) -> Duration {
    const MAX_RETRY_NANOS: u64 = i64::MAX as u64;

    let error_count = u32::try_from(row.errors.len().saturating_add(1)).unwrap_or(u32::MAX);
    let base_seconds = u128::from(error_count).pow(4);
    if base_seconds.saturating_mul(1_000_000_000) >= u128::from(MAX_RETRY_NANOS) {
        return Duration::from_nanos(MAX_RETRY_NANOS);
    }
    let base_seconds = u64::try_from(base_seconds).expect("capped retry seconds fit u64");
    let base = Duration::from_secs(base_seconds);

    let mut hasher = Sha256::new();
    hasher.update(seed.to_be_bytes());
    hasher.update(row.id.to_be_bytes());
    hasher.update(error_count.to_be_bytes());
    hasher.update(now.timestamp_nanos_opt().unwrap_or_default().to_be_bytes());
    let hash = hasher.finalize();
    let sample = u32::from_be_bytes(hash[..4].try_into().unwrap());
    let ratio = f64::from(sample) / f64::from(u32::MAX);
    base.mul_f64(0.9 + ratio * 0.2)
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

fn validate_insert_parts(
    kind: &str,
    opts: &InsertParams,
    allow_legacy_job_kinds: bool,
) -> Result<(), Error> {
    let mut kind_characters = kind.chars();
    if !allow_legacy_job_kinds
        && (kind.len() < 2
            || kind.len() >= 128
            || !kind_characters.next().is_some_and(is_word)
            || !kind_characters.all(valid_kind_character))
    {
        return Err(Error::invalid_job(format!("invalid job kind {kind:?}")));
    }
    if opts.max_attempts < 1 {
        return Err(Error::invalid_job(
            "max_attempts must be greater than zero".to_owned(),
        ));
    }
    if !(1..=4).contains(&opts.priority) {
        return Err(Error::invalid_job(
            "priority must be between one and four".to_owned(),
        ));
    }
    validate_queue(&opts.queue)?;
    for tag in &opts.tags {
        if tag.len() > 255 || tag.len() < 3 {
            return Err(Error::invalid_job(
                "tags must contain between 3 and 255 bytes".to_owned(),
            ));
        }
        let mut characters = tag.chars();
        let first = characters.next().unwrap();
        let last = tag.chars().next_back().unwrap();
        if !is_word(first)
            || !is_word(last)
            || !characters.all(|character| is_word(character) || character == '-')
        {
            return Err(Error::invalid_job(format!("invalid tag {tag:?}")));
        }
    }
    opts.unique.validate().map_err(Error::invalid_job)
}

fn valid_kind_character(character: char) -> bool {
    character.is_ascii_alphanumeric()
        || matches!(
            character,
            '_' | '-' | '[' | ']' | '<' | '>' | '/' | '.' | '·' | ':' | '+'
        )
}

pub(crate) fn validate_queue(queue: &str) -> Result<(), Error> {
    if queue.is_empty() || queue.len() > 64 {
        return Err(Error::invalid_job(
            "queue name must contain between 1 and 64 bytes".to_owned(),
        ));
    }
    if !queue
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
    {
        return Err(Error::invalid_job(format!("invalid queue name {queue:?}")));
    }
    let mut previous_separator = false;
    for character in queue.chars() {
        let separator = matches!(character, '_' | '-');
        if !(character.is_ascii_lowercase() || character.is_ascii_digit() || separator)
            || (separator && previous_separator)
        {
            return Err(Error::invalid_job(format!("invalid queue name {queue:?}")));
        }
        previous_separator = separator;
    }
    if previous_separator {
        return Err(Error::invalid_job(format!("invalid queue name {queue:?}")));
    }
    Ok(())
}

#[cfg(feature = "postgres")]
fn validate_identifier(identifier: &str, description: &str) -> Result<(), Error> {
    let mut characters = identifier.chars();
    if identifier.is_empty()
        || identifier.len() > 63
        || !characters
            .next()
            .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(Error::invalid_job(format!(
            "invalid PostgreSQL {description} identifier {identifier:?}"
        )));
    }
    Ok(())
}

fn is_word(character: char) -> bool {
    character == '_' || character.is_ascii_alphanumeric()
}

fn validate_metadata_key(key: &str) -> Result<(), Error> {
    let mut characters = key.chars();
    if key.is_empty()
        || !characters
            .next()
            .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(Error::configuration(format!(
            "invalid job cleaner metadata exclusion key {key:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_events_follow_persisted_state() {
        let cases = [
            (
                JobState::Available,
                JobEventKind::Failed,
                JobEventKind::Failed,
            ),
            (
                JobState::Available,
                JobEventKind::Interrupted,
                JobEventKind::Interrupted,
            ),
            (
                JobState::Available,
                JobEventKind::Cancelled,
                JobEventKind::Failed,
            ),
            (
                JobState::Available,
                JobEventKind::Completed,
                JobEventKind::Failed,
            ),
            (
                JobState::Available,
                JobEventKind::Snoozed,
                JobEventKind::Snoozed,
            ),
            (
                JobState::Cancelled,
                JobEventKind::Failed,
                JobEventKind::Cancelled,
            ),
            (
                JobState::Completed,
                JobEventKind::Failed,
                JobEventKind::Completed,
            ),
            (
                JobState::Discarded,
                JobEventKind::Completed,
                JobEventKind::Failed,
            ),
            (
                JobState::Retryable,
                JobEventKind::Completed,
                JobEventKind::Failed,
            ),
            (
                JobState::Scheduled,
                JobEventKind::Failed,
                JobEventKind::Snoozed,
            ),
        ];

        for (state, requested, expected) in cases {
            assert_eq!(persisted_completion_event_kind(state, requested), expected);
        }
    }

    #[test]
    fn completion_cleanup_preserves_newer_attempt() {
        let job_id = 42;
        let first = CancellationToken::new();
        let second = CancellationToken::new();
        let running = Mutex::new(HashMap::from([(job_id, first.clone())]));

        running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(job_id, second.clone());

        remove_running_attempt(&running, job_id, &first);
        assert_eq!(
            running
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&job_id),
            Some(&second)
        );

        remove_running_attempt(&running, job_id, &second);
        assert!(
            running
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&job_id)
                .is_none()
        );
    }

    #[test]
    fn pending_cancellation_reaches_fetched_attempt() {
        let job_id = 42;
        let cancellation = CancellationToken::new();
        let fetch_registration_windows = AtomicU64::new(1);
        let pending_cancellations = Mutex::new(HashMap::new());
        let running = Mutex::new(HashMap::new());

        signal_running_attempt(
            &running,
            &pending_cancellations,
            &fetch_registration_windows,
            job_id,
        );
        register_running_attempt(&running, &pending_cancellations, job_id, &cancellation);

        assert!(cancellation.is_cancelled());
        assert!(
            pending_cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
    }

    #[test]
    fn unmatched_cancellation_is_not_retained_without_fetch() {
        let fetch_registration_windows = AtomicU64::new(0);
        let pending_cancellations = Mutex::new(HashMap::new());
        let running = Mutex::new(HashMap::new());

        signal_running_attempt(
            &running,
            &pending_cancellations,
            &fetch_registration_windows,
            42,
        );

        assert!(
            pending_cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
    }

    fn retry_row(error_count: usize) -> JobRow {
        let now = DateTime::parse_from_rfc3339("2026-01-02T03:04:05Z")
            .unwrap()
            .with_timezone(&Utc);
        JobRow {
            attempt: i16::try_from(error_count).unwrap_or(i16::MAX),
            attempted_at: Some(now),
            attempted_by: vec!["test".to_owned()],
            created_at: now,
            encoded_args: serde_json::json!({}),
            errors: vec![
                AttemptError {
                    at: now,
                    attempt: 1,
                    error: "failed".to_owned(),
                    trace: String::new(),
                };
                error_count
            ],
            finalized_at: None,
            id: 42,
            kind: "retry_test".to_owned(),
            max_attempts: 1_000,
            metadata: Map::new(),
            priority: 1,
            queue: "default".to_owned(),
            scheduled_at: now,
            state: JobState::Retryable,
            tags: Vec::new(),
            unique_key: None,
            unique_states: None,
        }
    }

    #[test]
    fn retry_delay_is_seeded_bounded_and_capped() {
        let now = Utc::now();
        let row = retry_row(0);
        let first = default_retry_delay(&row, now, 123);
        assert_eq!(first, default_retry_delay(&row, now, 123));
        assert_ne!(first, default_retry_delay(&row, now, 456));
        assert!(first >= Duration::from_millis(900));
        assert!(first <= Duration::from_millis(1_100));

        assert_eq!(
            default_retry_delay(&retry_row(309), now, 123),
            Duration::from_nanos(i64::MAX as u64)
        );
    }
}
