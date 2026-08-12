//! PostgreSQL client, insertion, and worker runtime.

use std::{
    collections::HashMap,
    error::Error as StdError,
    fmt::Write as _,
    sync::{
        Arc, Mutex, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, NaiveTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{
    AssertSqlSafe, Executor, FromRow, PgConnection, PgPool, Postgres, Row,
    postgres::{PgListener, PgRow},
    types::Json,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot, watch},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info_span, warn};

use riverqueue_internal::{CompletionAction, CompletionParams, FetchParams, NoopPilot, Pilot};

use crate::{
    AttemptError, DefaultRetryPolicy, Error, ErrorHandler, ErrorHandlerResult, Event, EventKind,
    EventReceiver, FETCH_COOLDOWN_DEFAULT, FETCH_COOLDOWN_MIN, FETCH_POLL_INTERVAL_DEFAULT, Hook,
    InsertContext, InsertMiddleware, InsertOpts, InsertResult, JOB_STUCK_THRESHOLD_DEFAULT,
    JOB_TIMEOUT_DEFAULT, Job, JobArgs, JobRow, JobState, JobStatistics, MAX_ATTEMPTS_DEFAULT,
    Metric, Plugin, QUEUE_NUM_WORKERS_MAX, RawInsertResult, RetryPolicy, SchemaName,
    SubscribeConfig, WorkContext, WorkMiddleware, WorkOutcome, WorkResult, WorkerRegistry,
    WorkerTimeout,
    periodic::{PeriodicInsert, PeriodicJob, PeriodicJobs},
    unique::build_unique_key_parts,
};

const ATTEMPTED_BY_MAX: i32 = 100;
const EVENT_BUFFER_CAPACITY: usize = 10_000;
// Large queues otherwise become limited by a single PostgreSQL claim round trip.
// Concurrent `SKIP LOCKED` claims safely divide the available worker slots.
const PARALLEL_FETCH_MINIMUM: usize = 1_000;
const QUEUE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Leader-owned maintenance timing and retention settings.
#[derive(Clone, Debug)]
pub struct MaintenanceConfig {
    /// Retention for cancelled jobs; `None` disables deletion.
    pub cancelled_job_retention: Option<Duration>,
    /// Retention for completed jobs; `None` disables deletion.
    pub completed_job_retention: Option<Duration>,
    /// Retention for discarded jobs; `None` disables deletion.
    pub discarded_job_retention: Option<Duration>,
    /// Leader election and renewal interval.
    pub elect_interval: Duration,
    /// Job cleaner interval.
    pub job_cleaner_interval: Duration,
    /// Timeout for each job-cleaner deletion statement.
    pub job_cleaner_timeout: Duration,
    /// Age at which running jobs may be rescued.
    pub rescue_after: Duration,
    /// Stuck-job rescuer interval.
    pub rescuer_interval: Duration,
    /// Retention for inactive queue records.
    pub queue_retention: Duration,
    /// Inactive queue cleaner interval.
    pub queue_cleaner_interval: Duration,
    /// PostgreSQL indexes periodically rebuilt with `REINDEX CONCURRENTLY`.
    /// An empty list disables reindexing.
    pub reindexer_index_names: Vec<String>,
    /// Schedule for reindexer runs.
    pub reindexer_schedule: ReindexerSchedule,
    /// Statement timeout for each reindex operation.
    pub reindexer_timeout: Duration,
    /// Due-job scheduler interval.
    pub scheduler_interval: Duration,
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
            reindexer_index_names: vec![
                "river_job_args_index".to_owned(),
                "river_job_kind".to_owned(),
                "river_job_metadata_index".to_owned(),
                "river_job_pkey".to_owned(),
                "river_job_prioritized_fetching_index".to_owned(),
                "river_job_state_and_finalized_at_index".to_owned(),
                "river_job_unique_idx".to_owned(),
            ],
            reindexer_schedule: ReindexerSchedule::default(),
            reindexer_timeout: Duration::from_mins(1),
            scheduler_interval: Duration::from_secs(5),
        }
    }
}

/// Schedule used by River's leader-owned PostgreSQL reindexer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReindexerSchedule {
    /// Run each day at the supplied UTC wall-clock time.
    DailyUtc(NaiveTime),
    /// Run after each elapsed interval from client startup.
    Interval(Duration),
}

impl Default for ReindexerSchedule {
    fn default() -> Self {
        Self::DailyUtc(NaiveTime::MIN)
    }
}

/// Queue-specific worker settings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueConfig {
    /// Minimum delay between fetches.
    pub fetch_cooldown: Duration,
    /// Fallback polling interval.
    pub fetch_poll_interval: Duration,
    /// Maximum jobs run concurrently by this client.
    pub max_workers: usize,
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

    fn validate(&self, name: &str) -> Result<(), Error> {
        validate_queue(name)?;
        if !(1..=QUEUE_NUM_WORKERS_MAX).contains(&self.max_workers) {
            return Err(Error::InvalidJob(format!(
                "queue {name:?} max_workers must be between 1 and {QUEUE_NUM_WORKERS_MAX}"
            )));
        }
        if self.fetch_cooldown < FETCH_COOLDOWN_MIN {
            return Err(Error::InvalidJob(
                "fetch cooldown must be at least one millisecond".to_owned(),
            ));
        }
        if self.fetch_poll_interval < self.fetch_cooldown {
            return Err(Error::InvalidJob(
                "fetch poll interval cannot be shorter than fetch cooldown".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Builder for a River client.
pub struct ClientBuilder {
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
    pool: PgPool,
    queues: HashMap<String, QueueConfig>,
    retry_policy: Arc<dyn RetryPolicy>,
    schema: SchemaName,
    skip_job_kind_validation: bool,
    skip_unknown_job_check: bool,
    soft_stop_timeout: Option<Duration>,
    pub(crate) workers: WorkerRegistry,
    work_middleware: Vec<Arc<dyn WorkMiddleware>>,
}

impl ClientBuilder {
    /// Sets the maximum attempts used by `insert_default` when the job type
    /// leaves its insertion default unchanged.
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

    /// Uses periodic polling without PostgreSQL `LISTEN` connections.
    #[must_use]
    pub fn poll_only(mut self, poll_only: bool) -> Self {
        self.poll_only = poll_only;
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

    /// Uses an explicit PostgreSQL schema.
    #[must_use]
    pub fn schema(mut self, schema: SchemaName) -> Self {
        self.schema = schema;
        self
    }

    /// Temporarily permits legacy job kinds that do not match River's format.
    #[must_use]
    #[deprecated(note = "legacy escape hatch; valid River job kinds are required")]
    pub fn skip_job_kind_validation(mut self, skip: bool) -> Self {
        self.skip_job_kind_validation = skip;
        self
    }

    /// Allows inserting kinds with no worker in this client's registry.
    /// Insert-only clients already permit every kind.
    #[must_use]
    pub fn skip_unknown_job_check(mut self, skip: bool) -> Self {
        self.skip_unknown_job_check = skip;
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
    pub fn build(self) -> Result<Client, Error> {
        if self.default_max_attempts < 1 {
            return Err(Error::InvalidJob(
                "default max attempts must be greater than zero".to_owned(),
            ));
        }
        if self.id.is_empty() || self.id.len() > 100 {
            return Err(Error::InvalidJob(
                "client ID must contain between 1 and 100 bytes".to_owned(),
            ));
        }
        if self
            .soft_stop_timeout
            .is_some_and(|timeout| timeout.is_zero())
        {
            return Err(Error::InvalidJob(
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
            ("reindexer timeout", self.maintenance.reindexer_timeout),
            ("scheduler interval", self.maintenance.scheduler_interval),
        ] {
            if interval.is_zero() {
                return Err(Error::InvalidJob(format!("{name} must be positive")));
            }
        }
        if matches!(
            self.maintenance.reindexer_schedule,
            ReindexerSchedule::Interval(interval) if interval.is_zero()
        ) {
            return Err(Error::InvalidJob(
                "reindexer interval must be positive".to_owned(),
            ));
        }
        for index_name in &self.maintenance.reindexer_index_names {
            validate_identifier(index_name, "reindexer index")?;
        }
        if !self.queues.is_empty() && self.workers.kinds().is_empty() {
            return Err(Error::InvalidJob(
                "workers must be configured when queues are configured".to_owned(),
            ));
        }

        let periodic_jobs = PeriodicJobs::from_jobs(self.periodic_jobs)?;
        let (events, _) = broadcast::channel(EVENT_BUFFER_CAPACITY);
        let (queue_changes, _) = watch::channel(0_u64);
        let (queue_notifications, _) = broadcast::channel(1_024);
        Ok(Client {
            inner: Arc::new(ClientInner {
                default_max_attempts: self.default_max_attempts,
                error_handler: self.error_handler,
                events,
                hooks: self.hooks,
                id: self.id,
                job_stuck_threshold: self.job_stuck_threshold,
                job_timeout: self.job_timeout,
                maintenance: self.maintenance,
                insert_middleware: self.insert_middleware,
                periodic_jobs,
                pilot: self.pilot,
                poll_only: self.poll_only,
                pool: self.pool,
                queue_changes,
                queue_notifications,
                queues: RwLock::new(self.queues),
                retry_policy: self.retry_policy,
                running: Mutex::new(HashMap::new()),
                schema: self.schema,
                skip_job_kind_validation: self.skip_job_kind_validation,
                skip_unknown_job_check: self.skip_unknown_job_check,
                soft_stop_timeout: self.soft_stop_timeout,
                started: AtomicBool::new(false),
                workers: self.workers,
                work_middleware: self.work_middleware,
            }),
        })
    }
}

pub(crate) struct ClientInner {
    default_max_attempts: i16,
    error_handler: Option<Arc<dyn ErrorHandler>>,
    pub(crate) events: broadcast::Sender<Event>,
    pub(crate) hooks: Vec<Arc<dyn Hook>>,
    pub(crate) id: String,
    job_stuck_threshold: Duration,
    job_timeout: Option<Duration>,
    pub(crate) maintenance: MaintenanceConfig,
    insert_middleware: Vec<Arc<dyn InsertMiddleware>>,
    pub(crate) periodic_jobs: PeriodicJobs,
    pub(crate) pilot: Arc<dyn Pilot>,
    poll_only: bool,
    pub(crate) pool: PgPool,
    queue_changes: watch::Sender<u64>,
    queue_notifications: broadcast::Sender<String>,
    queues: RwLock<HashMap<String, QueueConfig>>,
    pub(crate) retry_policy: Arc<dyn RetryPolicy>,
    running: Mutex<HashMap<i64, CancellationToken>>,
    pub(crate) schema: SchemaName,
    skip_job_kind_validation: bool,
    skip_unknown_job_check: bool,
    soft_stop_timeout: Option<Duration>,
    started: AtomicBool,
    pub(crate) workers: WorkerRegistry,
    work_middleware: Vec<Arc<dyn WorkMiddleware>>,
}

/// A River client backed by a caller-owned SQLx PostgreSQL pool.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

/// Non-owning handle used by exact-version extension services.
#[doc(hidden)]
#[derive(Clone)]
pub struct WeakClient {
    inner: Weak<ClientInner>,
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
    pub fn builder(pool: PgPool) -> ClientBuilder {
        ClientBuilder {
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
            pool,
            queues: HashMap::new(),
            retry_policy: Arc::new(DefaultRetryPolicy::default()),
            schema: SchemaName::current(),
            skip_job_kind_validation: false,
            skip_unknown_job_check: false,
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

    /// Returns the dynamic periodic-job bundle for this client.
    #[must_use]
    pub fn periodic_jobs(&self) -> PeriodicJobs {
        self.inner.periodic_jobs.clone()
    }

    /// Returns the caller-owned PostgreSQL pool for exact-version helpers.
    #[doc(hidden)]
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }

    /// Returns the validated PostgreSQL schema selection.
    #[must_use]
    pub fn schema(&self) -> &SchemaName {
        &self.inner.schema
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
            return Err(Error::InvalidJob(
                "workers must be configured when queues are configured".to_owned(),
            ));
        }
        let previous = self
            .inner
            .queues
            .write()
            .map_err(|_| Error::Runtime("queue configuration lock poisoned".to_owned()))?
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
            .map_err(|_| Error::Runtime("queue configuration lock poisoned".to_owned()))
    }

    /// Stops and removes a configured queue. Persisted jobs and queue rows are
    /// left untouched for other clients.
    pub fn queue_remove(&self, name: &str) -> Result<Option<QueueConfig>, Error> {
        let previous = self
            .inner
            .queues
            .write()
            .map_err(|_| Error::Runtime("queue configuration lock poisoned".to_owned()))?
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
        self.subscribe_config(SubscribeConfig {
            buffer_capacity: 1_000,
            kinds: kinds.to_vec(),
        })
    }

    /// Subscribes with an explicit bounded-buffer capacity. When the receiver
    /// falls behind, the next receive reports how many events were dropped.
    pub fn subscribe_config(&self, config: SubscribeConfig) -> Result<EventReceiver, Error> {
        let SubscribeConfig {
            buffer_capacity,
            kinds,
        } = config;
        if self
            .inner
            .queues
            .read()
            .map_err(|_| Error::Runtime("queue configuration lock poisoned".to_owned()))?
            .is_empty()
        {
            return Err(Error::InvalidJob(
                "event subscriptions require a client configured to work queues".to_owned(),
            ));
        }
        if buffer_capacity == 0 {
            return Err(Error::InvalidJob(
                "event subscription buffer capacity must be positive".to_owned(),
            ));
        }
        let kinds = crate::event::validate_kinds(&kinds)?;
        let mut source = self.inner.events.subscribe();
        let (sender, receiver) = mpsc::channel(buffer_capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let dropped_for_task = Arc::clone(&dropped);
        tokio::runtime::Handle::try_current().map_err(|_| {
            Error::Runtime("event subscriptions require an active Tokio runtime".to_owned())
        })?;
        tokio::spawn(async move {
            loop {
                match source.recv().await {
                    Ok(event) if kinds.contains(&event.kind) => match sender.try_send(event) {
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

    /// Inserts a typed job using the pool.
    pub async fn insert<A: JobArgs>(
        &self,
        args: A,
        opts: InsertOpts,
    ) -> Result<InsertResult<A>, Error> {
        let result = self.insert_on(&self.inner.pool, args, opts).await?;
        self.signal_insert(&result.job.row, result.unique_skipped_as_duplicate);
        Ok(result)
    }

    /// Inserts a typed job using defaults declared by its argument type.
    pub async fn insert_default<A: JobArgs>(&self, args: A) -> Result<InsertResult<A>, Error> {
        let mut opts = A::default_insert_opts();
        if opts.max_attempts == MAX_ATTEMPTS_DEFAULT {
            opts.max_attempts = self.inner.default_max_attempts;
        }
        self.insert(args, opts).await
    }

    /// Inserts a typed batch atomically using the pool.
    pub async fn insert_many<A, I>(&self, jobs: I) -> Result<Vec<InsertResult<A>>, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let mut transaction = self.inner.pool.begin().await?;
        let results = self.insert_many_tx(&mut transaction, jobs).await?;
        transaction.commit().await?;
        for result in &results {
            self.signal_insert(&result.job.row, result.unique_skipped_as_duplicate);
        }
        Ok(results)
    }

    /// Inserts a typed batch with PostgreSQL `COPY FROM`. The operation is
    /// atomic and returns the inserted row count. As in River Go, a unique
    /// conflict fails the whole operation rather than returning an existing
    /// row. Per-job begin hooks and insertion middleware run before the copy;
    /// successful completion is reported through the fast-insert callbacks
    /// because PostgreSQL does not return the inserted rows.
    pub async fn insert_many_fast<A, I>(&self, jobs: I) -> Result<u64, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let mut transaction = self.inner.pool.begin().await?;
        let count = self.insert_many_fast_tx(&mut transaction, jobs).await?;
        transaction.commit().await?;
        if count > 0 {
            let _ = self.inner.queue_notifications.send("*".to_owned());
        }
        Ok(count)
    }

    /// Inserts a typed batch with `COPY FROM` inside a caller-managed
    /// transaction.
    pub async fn insert_many_fast_tx<A, I>(
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
            if !self.inner.skip_unknown_job_check
                && !self.inner.workers.kinds().is_empty()
                && !self.inner.workers.contains_kind(A::KIND)
            {
                return Err(Error::UnknownJobKind(A::KIND.to_owned()));
            }
            let mut insert = InsertContext {
                encoded_args: serde_json::to_value(args)?,
                kind: A::KIND.to_owned(),
                opts,
            };
            for hook in &self.inner.hooks {
                hook.insert_begin(&mut insert).await?;
            }
            for middleware in &self.inner.insert_middleware {
                middleware.before_insert(&mut insert).await?;
            }
            validate_insert_parts(
                &insert.kind,
                &insert.opts,
                self.inner.skip_job_kind_validation,
            )?;
            prepared.push(PreparedFastInsert::new(insert, A::unique_fields(), now)?);
        }
        if prepared.is_empty() {
            return Err(Error::InvalidJob("no jobs to insert".to_owned()));
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

    /// Inserts a typed batch on a caller-managed transaction. The caller
    /// chooses commit or rollback visibility.
    pub async fn insert_many_tx<A, I>(
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

    /// Inserts an encoded job through River's exact-version extension seam.
    #[doc(hidden)]
    pub async fn insert_raw(
        &self,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: Value,
        opts: InsertOpts,
    ) -> Result<RawInsertResult, Error> {
        self.validate_known_kind(kind)?;
        let (job, unique_skipped_as_duplicate) = self
            .insert_encoded_on(&self.inner.pool, kind, unique_fields, &encoded_args, opts)
            .await?;
        self.signal_insert(&job, unique_skipped_as_duplicate);
        Ok(RawInsertResult {
            job,
            unique_skipped_as_duplicate,
        })
    }

    /// Inserts an encoded job inside a caller-managed transaction.
    #[doc(hidden)]
    pub async fn insert_raw_tx(
        &self,
        connection: &mut PgConnection,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: Value,
        opts: InsertOpts,
    ) -> Result<RawInsertResult, Error> {
        self.validate_known_kind(kind)?;
        let (job, unique_skipped_as_duplicate) = self
            .insert_encoded_on(connection, kind, unique_fields, &encoded_args, opts)
            .await?;
        Ok(RawInsertResult {
            job,
            unique_skipped_as_duplicate,
        })
    }

    /// Inserts a typed job on a caller-managed transaction or connection.
    pub async fn insert_tx<A: JobArgs>(
        &self,
        connection: &mut PgConnection,
        args: A,
        opts: InsertOpts,
    ) -> Result<InsertResult<A>, Error> {
        self.insert_on(connection, args, opts).await
    }

    /// Gets one job by ID.
    pub async fn job_get(&self, id: i64) -> Result<JobRow, Error> {
        let table = self.inner.schema.qualify("river_job");
        let sql = format!(
            "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job WHERE id = $1 LIMIT 1",
            job_projection("job")
        );
        let record = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(id)
            .fetch_optional(&self.inner.pool)
            .await?
            .ok_or(Error::NotFound)?;
        record.into_job_row()
    }

    /// Cancels a job and returns its current row.
    pub async fn job_cancel(&self, id: i64) -> Result<JobRow, Error> {
        let mut transaction = self.inner.pool.begin().await?;
        let row = self.job_cancel_tx(&mut transaction, id).await?;
        transaction.commit().await?;
        Ok(row)
    }

    /// Cancels a job inside a caller-managed transaction. The notification is
    /// delivered only if the caller commits.
    pub async fn job_cancel_tx(
        &self,
        connection: &mut PgConnection,
        id: i64,
    ) -> Result<JobRow, Error> {
        let table = self.inner.schema.qualify("river_job");
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
        let record = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(id)
            .bind(self.inner.schema.as_deref())
            .bind(crate::NOTIFICATION_TOPIC_CONTROL)
            .fetch_optional(connection)
            .await?
            .ok_or(Error::NotFound)?;
        record.into_job_row()
    }

    /// Requests that the current leader resign after committing an internal
    /// transaction.
    pub async fn request_resign(&self) -> Result<(), Error> {
        let mut transaction = self.inner.pool.begin().await?;
        self.request_resign_tx(&mut transaction).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Requests leader resignation in a caller-managed transaction.
    pub async fn request_resign_tx(&self, connection: &mut PgConnection) -> Result<(), Error> {
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

    /// Starts configured queues and returns a lifecycle handle.
    pub fn start(&self) -> Result<RunHandle, Error> {
        if self
            .inner
            .queues
            .read()
            .map_err(|_| Error::Runtime("queue configuration lock poisoned".to_owned()))?
            .is_empty()
        {
            return Err(Error::InvalidJob(
                "at least one queue is required to start a client".to_owned(),
            ));
        }
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::Runtime("client is already running".to_owned()));
        }
        let fetch_cancel = CancellationToken::new();
        let work_cancel = CancellationToken::new();
        let inner = Arc::clone(&self.inner);
        let fetch_for_task = fetch_cancel.clone();
        let work_for_task = work_cancel.clone();
        let (ready_sender, ready) = oneshot::channel();
        let join = tokio::spawn(async move {
            let result = async {
                let notifications = inner.queue_notifications.clone();
                let (completion_sender, completion_receiver) = mpsc::channel(10_000);
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
                    queues.spawn(run_notifications(
                        Arc::clone(&inner),
                        fetch_for_task.child_token(),
                        notifications.clone(),
                        ready_sender,
                    ));
                }
                queues.spawn(crate::maintenance::run_maintenance(
                    Arc::clone(&inner),
                    fetch_for_task.child_token(),
                    notifications.subscribe(),
                ));
                for service in inner.pilot.runtime_services() {
                    let pool = inner.pool.clone();
                    let schema = inner.schema.clone();
                    let service_cancel = fetch_for_task.child_token();
                    queues.spawn(async move {
                        service
                            .run(pool, schema, service_cancel)
                            .await
                            .map_err(|service_error| {
                                Error::Runtime(format!(
                                    "River pilot runtime service failed: {service_error}"
                                ))
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
            fetch_cancel,
            join,
            ready: Some(ready),
            soft_stop_timeout: self.inner.soft_stop_timeout,
            work_cancel,
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
        E: Executor<'executor, Database = Postgres>,
    {
        self.validate_known_kind(A::KIND)?;
        let encoded_args = serde_json::to_value(&args)?;
        let (mut row, unique_skipped_as_duplicate) = self
            .insert_encoded_on(executor, A::KIND, A::unique_fields(), &encoded_args, opts)
            .await?;
        for hook in &self.inner.hooks {
            hook.decode_insert_result(&mut row).await?;
        }
        let args = serde_json::from_value(row.encoded_args.clone())?;
        Ok(InsertResult {
            job: Job { args, row },
            unique_skipped_as_duplicate,
        })
    }

    pub(crate) async fn insert_periodic(&self, insert: PeriodicInsert) -> Result<JobRow, Error> {
        let mut opts = insert.opts;
        if opts.max_attempts == MAX_ATTEMPTS_DEFAULT {
            opts.max_attempts = self.inner.default_max_attempts;
        }
        validate_insert_parts(insert.kind, &opts, self.inner.skip_job_kind_validation)?;
        let (row, unique_skipped_as_duplicate) = self
            .insert_encoded_on(
                &self.inner.pool,
                insert.kind,
                insert.unique_fields,
                &insert.encoded_args,
                opts,
            )
            .await?;
        self.signal_insert(&row, unique_skipped_as_duplicate);
        Ok(row)
    }

    fn signal_insert(&self, row: &JobRow, unique_skipped_as_duplicate: bool) {
        if row.state == JobState::Available && !unique_skipped_as_duplicate {
            let _ = self.inner.queue_notifications.send(row.queue.clone());
        }
    }

    async fn insert_encoded_on<'executor, E>(
        &self,
        executor: E,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: &Value,
        opts: InsertOpts,
    ) -> Result<(JobRow, bool), Error>
    where
        E: Executor<'executor, Database = Postgres>,
    {
        let mut insert = InsertContext {
            encoded_args: encoded_args.clone(),
            kind: kind.to_owned(),
            opts,
        };
        for hook in &self.inner.hooks {
            hook.insert_begin(&mut insert).await?;
        }
        for middleware in &self.inner.insert_middleware {
            middleware.before_insert(&mut insert).await?;
        }
        validate_insert_parts(
            &insert.kind,
            &insert.opts,
            self.inner.skip_job_kind_validation,
        )?;
        let InsertContext {
            encoded_args,
            kind,
            opts,
        } = insert;
        let now = Utc::now();
        let unique_key = build_unique_key_parts(
            &kind,
            unique_fields,
            &encoded_args,
            now,
            &opts.unique,
            &opts.queue,
            opts.scheduled_at,
        )?;
        let unique_states = unique_key.map(|_| i32::from(opts.unique.state_bitmask()));
        let state = if opts.pending {
            JobState::Pending
        } else if opts.scheduled_at.is_some_and(|scheduled| scheduled > now) {
            JobState::Scheduled
        } else {
            JobState::Available
        };
        let table = self.inner.schema.qualify("river_job");
        let state_type = self.inner.schema.qualify("river_job_state");
        let state_function = self.inner.schema.qualify("river_job_state_in_bitmask");
        let sql = format!(
            "WITH inserted AS (\
                INSERT INTO {table} (args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states) \
                VALUES ($1, $2, $3, $4, $5, $6, coalesce($7, now()), $8::text::{state_type}, $9, $10, $11::integer::bit(8)) \
                ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND unique_states IS NOT NULL AND {state_function}(unique_states, state) \
                DO NOTHING RETURNING *\
             ), notified AS (\
                SELECT pg_notify(concat(coalesce($12::text, current_schema()), '.', $13::text), json_build_object('queue', queue)::text) \
                FROM inserted WHERE state = 'available'\
             ) \
             SELECT {}, false AS unique_skipped_as_duplicate FROM inserted AS job LEFT JOIN notified ON true \
             UNION ALL \
             SELECT {}, true AS unique_skipped_as_duplicate FROM {table} AS job \
             WHERE NOT EXISTS (SELECT 1 FROM inserted) AND $10::bytea IS NOT NULL \
               AND unique_key = $10 AND unique_states IS NOT NULL AND {state_function}(unique_states, state) \
             LIMIT 1",
            job_projection("job"),
            job_projection("job")
        );
        let record = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(Json(&encoded_args))
            .bind(&kind)
            .bind(opts.max_attempts)
            .bind(Json(&opts.metadata))
            .bind(opts.priority)
            .bind(&opts.queue)
            .bind(opts.scheduled_at)
            .bind(state.as_str())
            .bind(&opts.tags)
            .bind(unique_key.map(|key| key.to_vec()))
            .bind(unique_states)
            .bind(self.inner.schema.as_deref())
            .bind(crate::NOTIFICATION_TOPIC_INSERT)
            .fetch_optional(executor)
            .await?
            .ok_or_else(|| {
                Error::InvalidJob("unique insert found no conflicting row".to_owned())
            })?;
        let unique_skipped_as_duplicate = record.unique_skipped_as_duplicate;
        let row = record.into_job_row()?;
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
        if !self.inner.skip_unknown_job_check
            && !self.inner.workers.kinds().is_empty()
            && !self.inner.workers.contains_kind(kind)
        {
            return Err(Error::UnknownJobKind(kind.to_owned()));
        }
        Ok(())
    }
}

/// Controls one running client instance.
pub struct RunHandle {
    fetch_cancel: CancellationToken,
    join: tokio::task::JoinHandle<Result<(), Error>>,
    ready: Option<oneshot::Receiver<Result<(), String>>>,
    soft_stop_timeout: Option<Duration>,
    work_cancel: CancellationToken,
}

impl RunHandle {
    /// Waits until PostgreSQL notification subscriptions are active.
    ///
    /// Poll-only clients are ready immediately. Calling this more than once is
    /// harmless.
    pub async fn wait_ready(&mut self) -> Result<(), Error> {
        let Some(ready) = self.ready.take() else {
            return Ok(());
        };
        ready
            .await
            .map_err(|_| Error::Runtime("client stopped before becoming ready".to_owned()))?
            .map_err(Error::Runtime)
    }

    /// Stops fetching and waits indefinitely for active jobs.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        self.fetch_cancel.cancel();
        if let Some(timeout) = self.soft_stop_timeout {
            tokio::select! {
                result = &mut self.join => return join_client_result(result),
                () = tokio::time::sleep(timeout) => self.work_cancel.cancel(),
            }
        }
        join_client_result(self.join.await)
    }

    /// Stops fetching and cancels active job contexts.
    pub async fn shutdown_now(self) -> Result<(), Error> {
        self.fetch_cancel.cancel();
        self.work_cancel.cancel();
        self.wait().await
    }

    /// Waits for the client to stop.
    pub async fn wait(self) -> Result<(), Error> {
        join_client_result(self.join.await)
    }
}

fn join_client_result(
    result: Result<Result<(), Error>, tokio::task::JoinError>,
) -> Result<(), Error> {
    result.map_err(Error::from_join)??;
    Ok(())
}

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

impl JobRecord {
    pub(crate) fn into_job_row(self) -> Result<JobRow, Error> {
        let Value::Object(metadata) = self.metadata.0 else {
            return Err(Error::InvalidJob(format!(
                "job {} metadata is not an object",
                self.id
            )));
        };
        let unique_states = self
            .unique_states
            .map(|bits| {
                let bitmask = u8::from_str_radix(&bits, 2).map_err(|error| {
                    Error::InvalidJob(format!(
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
            state: JobState::try_from(self.state.as_str()).map_err(Error::InvalidJob)?,
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
    event_kind: EventKind,
    finalized_at: Option<DateTime<Utc>>,
    job_id: i64,
    metadata: Map<String, Value>,
    scheduled_at: Option<DateTime<Utc>>,
    state: JobState,
    timing: CompletionTiming,
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
    const COMPLETION_BATCH_CONCURRENCY: usize = 2;
    const COMPLETION_BATCH_SIZE: usize = 5_000;
    const COMPLETION_BATCH_THRESHOLD: usize = COMPLETION_BATCH_SIZE;
    let mut batches = JoinSet::new();

    loop {
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

        while batches.len() >= COMPLETION_BATCH_CONCURRENCY {
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
    (batch, records): (Vec<CompletionUpdate>, Result<Vec<JobRecord>, sqlx::Error>),
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

async fn persist_completion_batch(
    inner: &ClientInner,
    batch: &[CompletionUpdate],
) -> Result<Vec<JobRecord>, sqlx::Error> {
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
            attempt = updates.attempt, \
            errors = CASE WHEN updates.attempt_error IS NULL THEN job.errors \
                ELSE array_append(coalesce(job.errors, '{{}}'), updates.attempt_error) END, \
            finalized_at = CASE WHEN updates.state IN ('retryable', 'scheduled') \
                AND job.metadata ? 'cancel_attempted_at' \
                THEN coalesce(updates.finalized_at, now()) ELSE updates.finalized_at END, \
            metadata = job.metadata || updates.metadata, \
            scheduled_at = coalesce(updates.scheduled_at, job.scheduled_at), \
            state = CASE WHEN updates.state IN ('retryable', 'scheduled') \
                AND job.metadata ? 'cancel_attempted_at' \
                THEN 'cancelled'::{state_type} ELSE updates.state::{state_type} END \
         FROM updates WHERE job.id = updates.id AND job.state = 'running' \
         RETURNING {}, false AS unique_skipped_as_duplicate",
        job_projection("job")
    );
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(ids)
        .bind(attempts)
        .bind(errors)
        .bind(finalized_at)
        .bind(metadata)
        .bind(scheduled_at)
        .bind(states)
        .fetch_all(&inner.pool)
        .await
}

fn finish_batched_completion(
    inner: &ClientInner,
    update: &CompletionUpdate,
    record: Option<JobRecord>,
) {
    if let Some(record) = record {
        match record.into_job_row() {
            Ok(row) => {
                let event_kind = if row.state == JobState::Cancelled {
                    EventKind::JobCancelled
                } else {
                    update.event_kind
                };
                let mut event = Event::job(event_kind, row);
                event.job_statistics = Some(JobStatistics {
                    complete_duration: update.timing.completion_started.elapsed(),
                    queue_wait_duration: update.timing.queue_wait_duration,
                    run_duration: update.timing.run_duration,
                });
                let _ = inner.events.send(event);
            }
            Err(operation_error) => {
                error!(
                    error = %operation_error,
                    job_id = update.job_id,
                    "failed to decode completed River job"
                );
            }
        }
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

async fn run_notifications(
    inner: Arc<ClientInner>,
    cancel: CancellationToken,
    queue_notifications: broadcast::Sender<String>,
    ready: oneshot::Sender<Result<(), String>>,
) -> Result<(), Error> {
    let schema = match inner.schema.as_deref() {
        Some(schema) => schema.to_owned(),
        None => sqlx::query_scalar::<_, Option<String>>("SELECT current_schema()")
            .fetch_one(&inner.pool)
            .await?
            .ok_or_else(|| Error::InvalidJob("PostgreSQL current_schema() is null".to_owned()))?,
    };
    let control_topic = format!("{schema}.{}", crate::NOTIFICATION_TOPIC_CONTROL);
    let insert_topic = format!("{schema}.{}", crate::NOTIFICATION_TOPIC_INSERT);
    let leadership_topic = format!("{schema}.{}", crate::NOTIFICATION_TOPIC_LEADERSHIP);
    let listener_result = async {
        let mut listener = PgListener::connect_with(&inner.pool).await?;
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
                let _ = queue_notifications.send(payload.queue);
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
                let marker = if payload.action == "request_resign" {
                    "__river_leadership_request_resign__"
                } else {
                    "__river_leadership__"
                };
                let _ = queue_notifications.send(marker.to_owned());
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
                let cancellation = payload.job_id.and_then(|job_id| {
                    inner
                        .running
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get(&job_id)
                        .cloned()
                });
                if let Some(cancellation) = cancellation {
                    cancellation.cancel();
                }
            }
            "pause" | "resume" => {
                if let Some(queue) = payload.queue {
                    let _ = queue_notifications.send(queue);
                }
            }
            _ => debug!(
                action = payload.action,
                "ignored unknown River control action"
            ),
        }
    }
}

async fn run_dynamic_queues(
    inner: Arc<ClientInner>,
    completion_sender: mpsc::Sender<CompletionUpdate>,
    fetch_cancel: CancellationToken,
    work_cancel: CancellationToken,
    notifications: broadcast::Sender<String>,
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
                    .ok_or_else(|| Error::Runtime("dynamic queue task set closed".to_owned()))?
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
    notifications: &broadcast::Sender<String>,
    active: &mut HashMap<String, (QueueConfig, CancellationToken, u64)>,
    tasks: &mut JoinSet<(String, u64, CancellationToken, Result<(), Error>)>,
    next_generation: &mut u64,
) -> Result<(), Error> {
    let configured = inner
        .queues
        .read()
        .map_err(|_| Error::Runtime("queue configuration lock poisoned".to_owned()))?
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

async fn run_queue(
    inner: Arc<ClientInner>,
    completion_sender: mpsc::Sender<CompletionUpdate>,
    queue: String,
    config: QueueConfig,
    fetch_cancel: CancellationToken,
    work_cancel: CancellationToken,
    mut notifications: broadcast::Receiver<String>,
) -> Result<(), Error> {
    crate::storage::touch_queue(&inner, &queue).await?;
    let permits = Arc::new(Semaphore::new(config.max_workers));
    let mut jobs = JoinSet::new();
    let mut last_fetch = tokio::time::Instant::now() - config.fetch_cooldown;
    let mut heartbeat = tokio::time::interval(QUEUE_HEARTBEAT_INTERVAL);
    let mut poll = tokio::time::interval(config.fetch_poll_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        let should_fetch = tokio::select! {
            () = fetch_cancel.cancelled() => break,
            _ = heartbeat.tick() => {
                crate::storage::touch_queue(&inner, &queue).await?;
                false
            },
            _ = poll.tick() => true,
            result = jobs.join_next(), if !jobs.is_empty() => {
                if let Some(Err(join_error)) = result {
                    error!(error = %join_error, "River queue task failed");
                }
                true
            },
            notification = notifications.recv() => match notification {
                Ok(notification_queue) => notification_queue == "*" || notification_queue == queue,
                Err(broadcast::error::RecvError::Lagged(_)) => true,
                Err(broadcast::error::RecvError::Closed) => false,
            },
        };
        if !should_fetch {
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
        let rows = if available >= PARALLEL_FETCH_MINIMUM && !inner.pilot.intercepts_fetch() {
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
                (Err(fetch_error), Err(_)) => return Err(fetch_error),
            }
        } else {
            fetch_jobs(&inner, &queue, available).await?
        };
        last_fetch = tokio::time::Instant::now();
        for row in rows {
            let permit = Arc::clone(&permits)
                .acquire_owned()
                .await
                .map_err(|_| Error::InvalidJob("queue worker semaphore closed".to_owned()))?;
            let inner = Arc::clone(&inner);
            let completion_sender = completion_sender.clone();
            let work_cancel = work_cancel.child_token();
            jobs.spawn(async move {
                execute_job(inner, row, work_cancel, completion_sender, permit).await;
            });
        }
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

async fn fetch_jobs(
    inner: &ClientInner,
    queue: &str,
    maximum: usize,
) -> Result<Vec<JobRow>, Error> {
    let fetch_started = (!inner.hooks.is_empty()).then(std::time::Instant::now);
    let table = inner.schema.qualify("river_job");
    let queue_table = inner.schema.qualify("river_queue");
    let maximum = i32::try_from(maximum)
        .map_err(|_| Error::InvalidJob("fetch maximum exceeds i32".to_owned()))?;
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
        let mut transaction = inner.pool.begin().await?;
        let selected_ids = inner
            .pilot
            .select_job_ids(
                &mut transaction,
                &FetchParams {
                    client_id: inner.id.clone(),
                    kinds: kinds.clone(),
                    maximum,
                    queue: queue.to_owned(),
                    schema: inner.schema.clone(),
                },
            )
            .await
            .map_err(|error| Error::Runtime(format!("pilot fetch selection: {error}")))?;
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
        fetch_oss_records(&inner.pool, oss_sql, queue, maximum, &inner.id).await?
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
    Ok(rows)
}

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
    completion_sender: mpsc::Sender<CompletionUpdate>,
    worker_permit: OwnedSemaphorePermit,
) {
    let span = info_span!("river_job", job_id = row.id, job_kind = %row.kind, queue = %row.queue);
    async move {
        let cancellation = hard_cancel.child_token();
        let queue_wait_duration = row
            .attempted_at
            .and_then(|attempted_at| {
                (attempted_at - row.scheduled_at.max(row.created_at))
                    .to_std()
                    .ok()
            })
            .unwrap_or_default();
        inner
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(row.id, cancellation.clone());
        let context = WorkContext::for_job(
            Client {
                inner: Arc::clone(&inner),
            },
            cancellation.clone(),
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

        let run_duration = work_started.elapsed();
        let mut result = result;
        let was_aborted = result
            .as_ref()
            .is_err_and(|failure| matches!(failure.kind, WorkerFailureKind::Aborted));
        if let Some(resumable_failure) = context.resumable_finish(result.is_err()).await {
            result = Err(WorkerFailure {
                error: resumable_failure,
                kind: WorkerFailureKind::Error,
                trace: String::new(),
            });
        }
        if cancellation_cause == Some(CancellationCause::Shutdown)
            && let Err(failure) = &mut result
        {
            failure.error.clear();
            failure.error.push_str("job interrupted by client shutdown");
            failure.kind = WorkerFailureKind::Interrupted;
            failure.trace.clear();
        }
        let work_result = public_work_result(&result);
        let mut error_handler_result = ErrorHandlerResult::default();
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
                let mut event = *event;
                event.job_statistics = Some(JobStatistics {
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
    trace: String,
}

#[derive(Debug)]
enum WorkerFailureKind {
    Aborted,
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
    worker_task: &mut tokio::task::JoinHandle<Result<WorkOutcome, Box<dyn StdError + Send + Sync>>>,
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
            trace: String::new(),
        }),
        Ok(result) => worker_join_result(result),
        Err(_) => Err(WorkerFailure {
            error: "job remained stuck after Tokio task abort".to_owned(),
            kind: WorkerFailureKind::Aborted,
            trace: String::new(),
        }),
    }
}

fn worker_join_result(
    result: Result<Result<WorkOutcome, Box<dyn StdError + Send + Sync>>, tokio::task::JoinError>,
) -> WorkerResult {
    match result {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(worker_error)) => Err(WorkerFailure {
            error: worker_error.to_string(),
            kind: WorkerFailureKind::Error,
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
            trace: format!("{join_error:?}"),
        }),
    }
}

fn boxed_extension_error(error: Error) -> Box<dyn StdError + Send + Sync> {
    Box::new(error)
}

fn erased_work_result(result: &Result<WorkOutcome, Box<dyn StdError + Send + Sync>>) -> WorkResult {
    match result {
        Ok(WorkOutcome::Cancel) => WorkResult::Cancelled,
        Ok(WorkOutcome::Complete) => WorkResult::Completed,
        Ok(WorkOutcome::Discard) => WorkResult::Discarded,
        Ok(WorkOutcome::Snooze(duration)) => WorkResult::Snoozed(*duration),
        Err(error) => WorkResult::Failed(error.to_string()),
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
            WorkerFailureKind::Error => WorkResult::Failed(failure.error.clone()),
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
    error_handler_result: ErrorHandlerResult,
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
                EventKind::JobCompleted,
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
                EventKind::JobCancelled,
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
                EventKind::JobFailed,
            ),
            Ok(WorkOutcome::Snooze(duration)) => {
                let scheduled_at = now
                    + chrono::Duration::from_std(duration)
                        .map_err(|error| Error::InvalidJob(error.to_string()))?;
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
                    EventKind::JobSnoozed,
                )
            }
            Err(failure) => {
                if matches!(failure.kind, WorkerFailureKind::Interrupted) {
                    return persist_interrupted(inner, row, metadata_updates)
                        .await
                        .map(|event| PersistResult::Finished(event.map(Box::new)));
                }
                let worker_retry_after = inner
                    .workers
                    .next_retry(row, &failure.error, now)
                    .unwrap_or_else(|retry_error| {
                        debug!(error = %retry_error, "could not evaluate worker retry override");
                        None
                    });
                let attempt_error = AttemptError {
                    at: row.attempted_at.unwrap_or(now),
                    attempt: row.attempt,
                    error: failure.error,
                    trace: failure.trace,
                };
                if error_handler_result.discard || row.attempt >= row.max_attempts {
                    (
                        JobState::Discarded,
                        Some(now),
                        None,
                        row.attempt,
                        Some(attempt_error),
                        metadata_updates,
                        EventKind::JobFailed,
                    )
                } else {
                    let delay = error_handler_result
                        .retry_after
                        .or(worker_retry_after)
                        .unwrap_or_else(|| {
                            inner
                                .retry_policy
                                .next_retry(row, &attempt_error.error, now)
                        });
                    let scheduled_at = now
                        + chrono::Duration::from_std(delay)
                            .map_err(|error| Error::InvalidJob(error.to_string()))?;
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
                        EventKind::JobFailed,
                    )
                }
            }
        };

    let table = inner.schema.qualify("river_job");
    let state_type = inner.schema.qualify("river_job_state");
    let sql = format!(
        "UPDATE {table} AS job SET \
            attempt = $2, \
            errors = CASE WHEN $3::jsonb IS NULL THEN errors ELSE array_append(coalesce(errors, '{{}}'), $3::jsonb) END, \
            finalized_at = CASE WHEN $7::text IN ('retryable', 'scheduled') AND metadata ? 'cancel_attempted_at' \
                                THEN coalesce($4, now()) ELSE $4 END, \
            metadata = metadata || $5::jsonb, \
            scheduled_at = coalesce($6, scheduled_at), \
            state = CASE WHEN $7::text IN ('retryable', 'scheduled') AND metadata ? 'cancel_attempted_at' \
                         THEN 'cancelled'::{state_type} ELSE $7::text::{state_type} END \
         WHERE id = $1 AND state = 'running' \
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
            .map_err(|_| Error::Runtime("completion batcher stopped".to_owned()))?;
        return Ok(PersistResult::Enqueued);
    }

    let record = {
        let mut transaction = inner.pool.begin().await?;
        let completion_action = inner
            .pilot
            .before_job_completion(
                &mut transaction,
                &CompletionParams {
                    job_id: row.id,
                    metadata_updates: metadata.clone(),
                    schema: inner.schema.clone(),
                    state: state.as_str().to_owned(),
                },
            )
            .await
            .map_err(|error| Error::Runtime(format!("pilot job completion: {error}")))?;
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
            "job result ignored because job is no longer running"
        );
        return Ok(PersistResult::Finished(None));
    };
    let row = record.into_job_row()?;
    let event_kind = if row.state == JobState::Cancelled {
        EventKind::JobCancelled
    } else {
        event_kind
    };
    Ok(PersistResult::Finished(Some(Box::new(Event::job(
        event_kind, row,
    )))))
}

#[allow(clippy::too_many_arguments)]
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
    let table = inner.schema.qualify("river_job");
    let sql = format!(
        "UPDATE {table} AS job SET attempt = greatest(job.attempt - 1, 0), \
         attempted_at = NULL, finalized_at = NULL, metadata = metadata || $2::jsonb, \
         scheduled_at = now(), state = 'available' WHERE id = $1 AND state = 'running' \
         RETURNING {}, false AS unique_skipped_as_duplicate",
        job_projection("job")
    );
    let record = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(row.id)
        .bind(Json(metadata_updates))
        .fetch_optional(&inner.pool)
        .await?;
    Ok(record
        .map(JobRecord::into_job_row)
        .transpose()?
        .map(|row| Event::job(EventKind::JobInterrupted, row)))
}

fn default_client_id() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_owned());
    format!("{host}-{}", std::process::id())
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
        } else if scheduled_at > now {
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
    opts: &InsertOpts,
    skip_job_kind_validation: bool,
) -> Result<(), Error> {
    let mut kind_characters = kind.chars();
    if !skip_job_kind_validation
        && (kind.len() < 2
            || kind.len() >= 128
            || !kind_characters.next().is_some_and(is_word)
            || !kind_characters.all(valid_kind_character))
    {
        return Err(Error::InvalidJob(format!("invalid job kind {kind:?}")));
    }
    if opts.max_attempts < 1 {
        return Err(Error::InvalidJob(
            "max_attempts must be greater than zero".to_owned(),
        ));
    }
    if !(1..=4).contains(&opts.priority) {
        return Err(Error::InvalidJob(
            "priority must be between one and four".to_owned(),
        ));
    }
    validate_queue(&opts.queue)?;
    for tag in &opts.tags {
        if tag.len() > 255 || tag.len() < 3 {
            return Err(Error::InvalidJob(
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
            return Err(Error::InvalidJob(format!("invalid tag {tag:?}")));
        }
    }
    opts.unique.validate().map_err(Error::InvalidJob)
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
        return Err(Error::InvalidJob(
            "queue name must contain between 1 and 64 bytes".to_owned(),
        ));
    }
    if !queue
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
    {
        return Err(Error::InvalidJob(format!("invalid queue name {queue:?}")));
    }
    let mut previous_separator = false;
    for character in queue.chars() {
        let separator = matches!(character, '_' | '-');
        if !(character.is_ascii_lowercase() || character.is_ascii_digit() || separator)
            || (separator && previous_separator)
        {
            return Err(Error::InvalidJob(format!("invalid queue name {queue:?}")));
        }
        previous_separator = separator;
    }
    if previous_separator {
        return Err(Error::InvalidJob(format!("invalid queue name {queue:?}")));
    }
    Ok(())
}

fn validate_identifier(identifier: &str, description: &str) -> Result<(), Error> {
    let mut characters = identifier.chars();
    if identifier.is_empty()
        || identifier.len() > 63
        || !characters
            .next()
            .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(Error::InvalidJob(format!(
            "invalid PostgreSQL {description} identifier {identifier:?}"
        )));
    }
    Ok(())
}

fn is_word(character: char) -> bool {
    character == '_' || character.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;

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
