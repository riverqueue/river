//! Client configuration and construction.

#[allow(clippy::wildcard_imports)]
use super::*;

/// Default age at which running jobs are rescued (Go
/// `JobRescuerRescueAfterDefault`).
const RESCUE_AFTER_DEFAULT: Duration = Duration::from_hours(1);

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
    /// Test-only batch sizes of bulk maintenance services.
    pub(crate) batch_sizes: crate::maintenance::BatchSizes,
    /// Explicit age at which running jobs may be rescued.
    pub(crate) rescue_after: Option<Duration>,
    /// Rescue age in effect, resolved against the job timeout at build time.
    pub(crate) rescue_after_effective: Duration,
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

    /// Returns the explicitly configured rescue age, if any.
    ///
    /// When unset, a client rescues jobs running longer than one hour, or
    /// than its job timeout plus one hour when a job timeout is configured,
    /// matching Go's `RescueStuckJobsAfter` default.
    #[must_use]
    pub const fn rescue_after(&self) -> Option<Duration> {
        self.rescue_after
    }

    /// Sets the age at which running jobs are considered stuck and rescued.
    /// It must not be shorter than the client's job timeout.
    #[must_use]
    pub const fn with_rescue_after(mut self, value: Duration) -> Self {
        self.rescue_after = Some(value);
        self
    }

    pub(crate) const fn effective_rescue_after(&self) -> Duration {
        self.rescue_after_effective
    }
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
            batch_sizes: crate::maintenance::BatchSizes::default(),
            rescue_after: None,
            rescue_after_effective: RESCUE_AFTER_DEFAULT,
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

    pub(super) fn validate(&self, name: &str) -> Result<(), Error> {
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
    pub(super) database: Database,
    pub(super) default_max_attempts: i16,
    pub(super) error_handler: Option<Arc<dyn crate::extension::DynErrorHandler>>,
    pub(super) hooks: Vec<Arc<dyn crate::extension::DynHook>>,
    pub(super) id: String,
    pub(super) job_stuck_threshold: Duration,
    pub(super) job_timeout: Option<Duration>,
    pub(super) maintenance: MaintenanceConfig,
    pub(super) insert_middleware: Vec<Arc<dyn crate::extension::DynInsertMiddleware>>,
    pub(super) periodic_jobs: Vec<PeriodicJob>,
    pub(super) pilot: Arc<dyn Pilot>,
    pub(super) poll_only: bool,
    pub(super) queues: HashMap<String, QueueConfig>,
    pub(super) retry_policy: Arc<dyn RetryPolicy>,
    pub(super) allow_legacy_job_kinds: bool,
    pub(super) allow_unregistered_job_kinds: bool,
    pub(super) soft_stop_timeout: Option<Duration>,
    pub(crate) workers: WorkerRegistry,
    pub(super) work_middleware: Vec<Arc<dyn WorkMiddleware>>,
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

    /// Installs a pilot from a companion crate.
    #[must_use]
    pub(crate) fn with_pilot<P: Pilot>(mut self, pilot: P) -> Self {
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

    /// Installs the hooks and middleware contributed by a plugin, after any
    /// registered earlier.
    #[must_use]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "taking the plugin by value matches the other registration methods"
    )]
    pub fn plugin<P: Plugin>(mut self, plugin: P) -> Self {
        let mut extensions = crate::Extensions::default();
        plugin.install(&mut extensions);
        self.hooks.extend(extensions.hooks);
        self.insert_middleware.extend(extensions.insert_middleware);
        self.work_middleware.extend(extensions.work_middleware);
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

    /// Escalates a soft stop to a hard stop after this duration, like Go's
    /// `SoftStopTimeout`. `None`, the default, lets running jobs finish
    /// without a limit.
    ///
    /// The client starts this timer when fetching stops, however the stop was
    /// requested: [`RunHandle::shutdown`](crate::RunHandle::shutdown),
    /// [`Stopper::stop`](crate::Stopper::stop), or the signal passed to
    /// [`Client::start_with_graceful_shutdown`]. Jobs still running when it
    /// expires are cancelled as if by
    /// [`Stopper::stop_now`](crate::Stopper::stop_now).
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
            (
                "rescue after",
                self.maintenance
                    .rescue_after
                    .unwrap_or(RESCUE_AFTER_DEFAULT),
            ),
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
        // Like Go, rescuing jobs before their timeout could run them twice.
        if let (Some(rescue_after), Some(job_timeout)) =
            (self.maintenance.rescue_after, self.job_timeout)
            && rescue_after < job_timeout
        {
            return Err(Error::configuration(
                "rescue after cannot be less than the job timeout".to_owned(),
            ));
        }
        let mut maintenance = self.maintenance;
        maintenance.rescue_after_effective = maintenance.rescue_after.unwrap_or_else(|| {
            self.job_timeout
                .filter(|timeout| !timeout.is_zero())
                .map_or(RESCUE_AFTER_DEFAULT, |timeout| {
                    timeout + RESCUE_AFTER_DEFAULT
                })
        });

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
                maintenance,
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
