//! Client configuration and construction.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::sync::{broadcast, watch};

use crate::__private::Pilot;
#[cfg(feature = "postgres")]
use crate::SchemaName;
#[cfg(feature = "postgres")]
use crate::client::validate::validate_identifier;
use crate::client::{ClientInner, EVENT_BUFFER_CAPACITY, InsertNotifyLimiter, validate_queue};
use crate::database::Database;
use crate::periodic::{PeriodicJob, PeriodicJobs};
use crate::{
    Client, Error, ErrorHandler, FETCH_COOLDOWN_MIN, FETCH_POLL_INTERVAL_DEFAULT, Hook,
    InsertMiddleware, Plugin, QUEUE_NUM_WORKERS_MAX, RetryPolicy, WorkMiddleware, Workers,
};

/// Default age at which running jobs are rescued (Go
/// `JobRescuerRescueAfterDefault`).
const RESCUE_AFTER_DEFAULT: Duration = Duration::from_hours(1);

/// How long the job cleaner keeps finalized jobs of one state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Retention {
    /// Delete jobs once they've been finalized for this long.
    DeleteAfter(Duration),
    /// Never delete these jobs.
    Keep,
}

impl Retention {
    const fn from_option(retention: Option<Duration>) -> Self {
        match retention {
            Some(duration) => Self::DeleteAfter(duration),
            None => Self::Keep,
        }
    }

    const fn into_option(self) -> Option<Duration> {
        match self {
            Self::DeleteAfter(duration) => Some(duration),
            Self::Keep => None,
        }
    }
}

/// Leader-owned maintenance timing and retention settings.
///
/// Like River's other configuration values, `MaintenanceConfig` has a getter
/// for each setting and a `with_*` method that returns the configuration
/// with that setting changed.
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

macro_rules! maintenance_retention {
    ($getter:ident, $setter:ident, $state:literal, $default:literal) => {
        #[doc = concat!("Returns how long ", $state, " jobs are kept before the job cleaner deletes them.")]
        #[must_use]
        pub const fn $getter(&self) -> Retention {
            Retention::from_option(self.$getter)
        }

        #[doc = concat!("Sets how long ", $state, " jobs are kept before the job cleaner deletes them. Defaults to deleting them after ", $default, ".")]
        #[must_use]
        pub const fn $setter(mut self, retention: Retention) -> Self {
            self.$getter = retention.into_option();
            self
        }
    };
}

macro_rules! maintenance_duration {
    ($getter:ident, $setter:ident, $what:literal, $default:literal) => {
        #[doc = concat!("Returns ", $what, ".")]
        #[must_use]
        pub const fn $getter(&self) -> Duration {
            self.$getter
        }

        #[doc = concat!("Sets ", $what, ". Defaults to ", $default, ".")]
        #[must_use]
        pub const fn $setter(mut self, value: Duration) -> Self {
            self.$getter = value;
            self
        }
    };
}

impl MaintenanceConfig {
    maintenance_retention!(
        cancelled_job_retention,
        with_cancelled_job_retention,
        "cancelled",
        "24 hours"
    );
    maintenance_retention!(
        completed_job_retention,
        with_completed_job_retention,
        "completed",
        "24 hours"
    );
    maintenance_retention!(
        discarded_job_retention,
        with_discarded_job_retention,
        "discarded",
        "7 days"
    );
    maintenance_duration!(
        elect_interval,
        with_elect_interval,
        "how often the client bids for leadership, or renews it while leader",
        "5 seconds"
    );
    maintenance_duration!(
        job_cleaner_interval,
        with_job_cleaner_interval,
        "how often the leader deletes finalized jobs past their retention",
        "30 seconds"
    );
    maintenance_duration!(
        job_cleaner_timeout,
        with_job_cleaner_timeout,
        "the timeout for each batch the job cleaner deletes",
        "30 seconds"
    );

    /// Returns the explicitly configured rescue age, if any.
    ///
    /// When unset, a client rescues jobs running longer than one hour, or
    /// than its job timeout plus one hour when a job timeout is configured.
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
    maintenance_duration!(
        rescuer_interval,
        with_rescuer_interval,
        "how often the leader looks for stuck jobs to rescue",
        "30 seconds"
    );
    maintenance_duration!(
        queue_retention,
        with_queue_retention,
        "how long a queue record no client has touched is kept before the queue cleaner deletes it",
        "24 hours"
    );
    maintenance_duration!(
        queue_cleaner_interval,
        with_queue_cleaner_interval,
        "how often the leader deletes queue records past their retention",
        "1 hour"
    );
    maintenance_duration!(
        scheduler_interval,
        with_scheduler_interval,
        "how often the leader makes due scheduled and retryable jobs available",
        "5 seconds"
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
    /// Settings for an add-on crate, which River passes through unchanged.
    pub(crate) extension_settings: Map<String, Value>,
    /// Minimum delay between fetches, overriding the client's
    /// [`ClientBuilder::fetch_cooldown`] when set.
    pub(crate) fetch_cooldown: Option<Duration>,
    /// Fallback polling interval.
    pub(crate) fetch_poll_interval: Duration,
    /// Maximum jobs run concurrently by this client.
    pub(crate) max_workers: usize,
}

impl QueueConfig {
    /// Creates queue configuration with River's timing defaults.
    #[must_use]
    pub fn new(max_workers: usize) -> Self {
        Self {
            extension_settings: Map::new(),
            fetch_cooldown: None,
            fetch_poll_interval: FETCH_POLL_INTERVAL_DEFAULT,
            max_workers,
        }
    }

    /// Returns this queue's minimum delay between fetches, or `None` when it
    /// uses the client's [`ClientBuilder::fetch_cooldown`].
    #[must_use]
    pub const fn fetch_cooldown(&self) -> Option<Duration> {
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

    /// Sets the minimum delay between fetches for this queue, overriding the
    /// client's [`ClientBuilder::fetch_cooldown`]. Throughput is limited by
    /// this value. It must be at least [`FETCH_COOLDOWN_MIN`](crate::FETCH_COOLDOWN_MIN) and no longer
    /// than the fetch poll interval.
    ///
    /// The override only paces this queue's fetches. Insert notifications
    /// are always suppressed for the client's fetch cooldown.
    #[must_use]
    pub const fn with_fetch_cooldown(mut self, interval: Duration) -> Self {
        self.fetch_cooldown = Some(interval);
        self
    }

    /// Sets how often the queue polls for jobs when no insert notification
    /// arrives. Defaults to
    /// [`FETCH_POLL_INTERVAL_DEFAULT`](crate::FETCH_POLL_INTERVAL_DEFAULT)
    /// (one second), and can't be shorter than the queue's fetch cooldown.
    /// River adds up to 10% of jitter to each poll.
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

    /// Returns the minimum delay between this queue's fetches, given the
    /// client's fetch cooldown.
    pub(crate) fn resolved_fetch_cooldown(&self, client_fetch_cooldown: Duration) -> Duration {
        self.fetch_cooldown.unwrap_or(client_fetch_cooldown)
    }

    /// Validates the queue, given the client's fetch cooldown.
    pub(super) fn validate(
        &self,
        name: &str,
        client_fetch_cooldown: Duration,
    ) -> Result<(), Error> {
        validate_queue(name)?;
        if !(1..=QUEUE_NUM_WORKERS_MAX).contains(&self.max_workers) {
            return Err(Error::configuration(format!(
                "queue {name:?} max_workers must be between 1 and {QUEUE_NUM_WORKERS_MAX}"
            )));
        }
        if self
            .fetch_cooldown
            .is_some_and(|cooldown| cooldown < FETCH_COOLDOWN_MIN)
        {
            return Err(Error::configuration(
                "fetch cooldown must be at least one millisecond".to_owned(),
            ));
        }
        if self.fetch_poll_interval < self.resolved_fetch_cooldown(client_fetch_cooldown) {
            return Err(Error::configuration(
                "fetch poll interval cannot be shorter than fetch cooldown".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Builder for a River client.
#[allow(
    clippy::struct_excessive_bools,
    reason = "each flag is an independent configuration option, not a state"
)]
pub struct ClientBuilder {
    pub(super) allow_legacy_job_kinds: bool,
    pub(super) allow_unregistered_job_kinds: bool,
    pub(super) database: Database,
    pub(super) default_max_attempts: i16,
    pub(super) error_handler: Option<Arc<dyn crate::extension::DynErrorHandler>>,
    pub(super) fetch_cooldown: Duration,
    pub(super) fetch_only_known_kinds: bool,
    pub(super) hooks: Vec<Arc<dyn crate::extension::DynHook>>,
    pub(super) id: String,
    pub(super) insert_middleware: Vec<Arc<dyn crate::extension::DynInsertMiddleware>>,
    pub(super) job_stuck_threshold: Duration,
    pub(super) job_timeout: Option<Duration>,
    pub(crate) leader_election_disabled: bool,
    pub(super) maintenance: MaintenanceConfig,
    pub(super) periodic_jobs: Vec<PeriodicJob>,
    pub(super) pilot: Arc<dyn Pilot>,
    pub(super) poll_only: bool,
    pub(crate) producer_report_interval: Duration,
    pub(super) queues: HashMap<String, QueueConfig>,
    pub(super) retry_policy: Arc<dyn RetryPolicy>,
    pub(super) soft_stop_timeout: Option<Duration>,
    pub(super) work_middleware: Vec<Arc<dyn crate::extension::DynWorkMiddleware>>,
    pub(crate) workers: Workers,
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
            .field("leader_election_disabled", &self.leader_election_disabled)
            .finish_non_exhaustive()
    }
}

impl ClientBuilder {
    /// Temporarily permits inserting legacy job kinds that don't match River's
    /// kind format: 2 to 127 bytes, starting with a letter, digit, or `_`,
    /// and otherwise made of letters, digits, and `_-[]<>/.·:+`.
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

    /// Sets the maximum attempts used by [`Client::insert`] when the job type
    /// does not override it. Defaults to
    /// [`MAX_ATTEMPTS_DEFAULT`](crate::MAX_ATTEMPTS_DEFAULT) (25), and must be
    /// at least one.
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

    /// Sets the minimum delay between fetches of new jobs. Jobs are fetched
    /// at most this often, and when no insert notifications arrive, fetches
    /// may wait as long as a queue's fetch poll interval. Throughput is
    /// limited by this value. A queue may override it with
    /// [`QueueConfig::with_fetch_cooldown`].
    ///
    /// It also paces insert notifications. After this client notifies a
    /// queue that jobs were inserted, further notifications for that queue
    /// are skipped until the cooldown has passed, whichever insertion,
    /// transaction, or scheduler pass would send them. The window starts when
    /// the notification is written, even if its transaction later rolls
    /// back. A job whose notification was skipped is found by the next fetch
    /// of its queue, which may wait for the queue's fetch poll interval.
    ///
    /// Like River Go's `Config.FetchCooldown`, it defaults to
    /// [`FETCH_COOLDOWN_DEFAULT`](crate::FETCH_COOLDOWN_DEFAULT) (100
    /// milliseconds) and must be at least
    /// [`FETCH_COOLDOWN_MIN`](crate::FETCH_COOLDOWN_MIN) (one millisecond).
    /// A queue's fetch poll interval can't be shorter than the cooldown it
    /// uses.
    #[must_use]
    pub fn fetch_cooldown(mut self, cooldown: Duration) -> Self {
        self.fetch_cooldown = cooldown;
        self
    }

    /// Restricts claims to the kinds of registered workers, including their
    /// aliases, like River Go's `Config.FetchOnlyKnownKinds`. Jobs of other
    /// kinds stay available without using attempts, so clients with
    /// different workers can share a queue.
    ///
    /// It only affects claiming. A leader's rescuer still handles stuck jobs
    /// in every queue and discards those whose kinds it doesn't know, so a
    /// client with only some of the workers should also be built
    /// [`without_leader_election`](Self::without_leader_election), with
    /// another client that has every worker eligible to lead.
    ///
    /// Disabled by default, so a job of an unknown kind is claimed and fails
    /// its attempt.
    #[must_use]
    pub const fn fetch_only_known_kinds(mut self, enabled: bool) -> Self {
        self.fetch_only_known_kinds = enabled;
        self
    }

    /// Adds an ordered lifecycle hook.
    #[must_use]
    pub fn hook<H: Hook>(mut self, hook: H) -> Self {
        self.hooks.push(Arc::new(hook));
        self
    }

    /// Sets a stable client identifier, 1 to 100 bytes long. It must be
    /// unique per running process, since leader election and job attempts
    /// record it. Defaults to the host name, the creation time, and a random
    /// suffix.
    #[must_use]
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// Adds ordered insertion middleware.
    #[must_use]
    pub fn insert_middleware<M: InsertMiddleware>(mut self, middleware: M) -> Self {
        self.insert_middleware.push(Arc::new(middleware));
        self
    }

    /// Sets how long a job may keep running after its
    /// [`WorkContext::cancellation_token`](crate::WorkContext::cancellation_token)
    /// is cancelled (by a timeout, a remote cancellation, or a hard stop)
    /// before River considers it stuck. A stuck job's
    /// [`ErrorHandler::handle_stuck`](crate::ErrorHandler::handle_stuck)
    /// runs, its task is aborted, and the attempt fails like any other failed
    /// attempt. Defaults to
    /// [`JOB_STUCK_THRESHOLD_DEFAULT`](crate::JOB_STUCK_THRESHOLD_DEFAULT)
    /// (10 seconds). The threshold must be positive.
    #[must_use]
    pub fn job_stuck_threshold(mut self, threshold: Duration) -> Self {
        self.job_stuck_threshold = threshold;
        self
    }

    /// Sets how long a job may run before its
    /// [`WorkContext::cancellation_token`](crate::WorkContext::cancellation_token) is cancelled and the attempt
    /// fails, unless its worker overrides it. Defaults to one minute. The
    /// timeout must be positive; use
    /// [`without_job_timeout`](Self::without_job_timeout) to let jobs run
    /// without a limit.
    #[must_use]
    pub fn job_timeout(mut self, timeout: Duration) -> Self {
        self.job_timeout = Some(timeout);
        self
    }

    /// Configures leader-owned maintenance services.
    ///
    /// Has no effect on a client built with
    /// [`without_leader_election`](Self::without_leader_election), which
    /// never runs them.
    #[must_use]
    pub fn maintenance(mut self, maintenance: MaintenanceConfig) -> Self {
        self.maintenance = maintenance;
        self
    }

    /// Adds a periodic job to the initial client configuration.
    ///
    /// Only the elected leader enqueues periodic jobs, so a client built
    /// with [`without_leader_election`](Self::without_leader_election)
    /// rejects them when built.
    #[must_use]
    pub fn periodic_job(mut self, job: PeriodicJob) -> Self {
        self.periodic_jobs.push(job);
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

    /// Escalates a soft stop to a hard stop after this duration. By default,
    /// running jobs finish without a limit.
    /// The timeout must be positive.
    ///
    /// The client starts this timer when fetching stops, however the stop was
    /// requested: [`RunHandle::stop`](crate::RunHandle::stop),
    /// [`Stopper::stop`](crate::Stopper::stop), or the signal passed to
    /// [`Client::start_with_graceful_stop`]. Jobs still running when it
    /// expires are cancelled as if by
    /// [`Stopper::stop_and_cancel`](crate::Stopper::stop_and_cancel).
    #[must_use]
    pub fn soft_stop_timeout(mut self, timeout: Duration) -> Self {
        self.soft_stop_timeout = Some(timeout);
        self
    }

    /// Installs a pilot from a companion crate.
    #[must_use]
    pub(crate) fn with_pilot<P: Pilot>(mut self, pilot: P) -> Self {
        self.pilot = Arc::new(pilot);
        self
    }

    /// Lets jobs run without a time limit unless their worker sets one.
    #[must_use]
    pub fn without_job_timeout(mut self) -> Self {
        self.job_timeout = None;
        self
    }

    /// Keeps this client out of leader election.
    ///
    /// The client still fetches and works jobs from its queues, sends and
    /// receives notifications, and runs extension runtime services, but it
    /// never becomes leader, so it never runs leader-owned maintenance: the
    /// scheduler, the periodic job enqueuer, the stuck job rescuer, the job
    /// and queue cleaners, the reindexer, and extension maintenance
    /// services. This suits clients dedicated to particular queues that
    /// should spend their resources only on those queues' jobs.
    ///
    /// At least one other started client using the same database and schema,
    /// in any River implementation, must remain eligible to lead. Otherwise
    /// scheduled jobs and retries never become available, periodic jobs are
    /// never enqueued, stuck jobs are never rescued, and finalized jobs are
    /// never deleted. This client stays ineligible even when no other client
    /// is running.
    ///
    /// Such a client can't configure periodic jobs: [`ClientBuilder::build`]
    /// fails when any were added with [`periodic_job`](Self::periodic_job),
    /// and [`PeriodicJobs::add`] and [`PeriodicJobs::add_many`] fail on its
    /// [`Client::periodic_jobs`]. It still works periodic jobs that a leader
    /// enqueues in its queues.
    #[must_use]
    pub fn without_leader_election(mut self) -> Self {
        self.leader_election_disabled = true;
        self
    }

    /// Disables the backend notification channel or outbox poller while
    /// retaining queue fetch polling.
    ///
    /// The client then polls for new jobs every queue's fetch poll interval,
    /// and every two seconds for queue changes and for cancellations of its
    /// running jobs requested by other clients. A client using a Postgres
    /// server without `LISTEN`/`NOTIFY`, like YugabyteDB by default, runs
    /// this way on its own.
    #[must_use]
    pub fn without_notifications(mut self) -> Self {
        self.poll_only = true;
        self
    }

    /// Adds ordered worker middleware.
    #[must_use]
    pub fn work_middleware<M: WorkMiddleware>(mut self, middleware: M) -> Self {
        self.work_middleware.push(Arc::new(middleware));
        self
    }

    /// Sets the client's workers.
    #[must_use]
    pub fn workers(mut self, workers: Workers) -> Self {
        self.workers = workers;
        self
    }

    /// Validates configuration and builds the client.
    #[allow(
        clippy::too_many_lines,
        reason = "central validation keeps builder failures deterministic before allocating runtime state"
    )]
    ///
    /// # Errors
    ///
    /// Returns [`Error::Configuration`] when a setting is out of range or
    /// settings conflict, such as queues configured without workers, a rescue
    /// age shorter than the job timeout, or periodic jobs on a client without
    /// leader election.
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
                "soft stop timeout must be positive".to_owned(),
            ));
        }
        if self.job_timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Err(Error::configuration(
                "job timeout must be positive; use without_job_timeout to disable it".to_owned(),
            ));
        }
        if self.job_stuck_threshold.is_zero() {
            return Err(Error::configuration(
                "job stuck threshold must be positive".to_owned(),
            ));
        }
        if self.fetch_cooldown < FETCH_COOLDOWN_MIN {
            return Err(Error::configuration(
                "fetch cooldown must be at least one millisecond".to_owned(),
            ));
        }
        for (name, config) in &self.queues {
            config.validate(name, self.fetch_cooldown)?;
            validate_queue_settings(self.pilot.as_ref(), name, config)?;
        }
        if self.producer_report_interval.is_zero() {
            return Err(Error::configuration(
                "producer report interval must be positive".to_owned(),
            ));
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
        if reindex.is_some_and(|config| config.timeout().is_some_and(|timeout| timeout.is_zero())) {
            return Err(Error::configuration(
                "reindexer timeout must be positive; use without_timeout to disable it".to_owned(),
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

        if self.leader_election_disabled && !self.periodic_jobs.is_empty() {
            return Err(Error::configuration(
                "periodic jobs must be empty when leader election is disabled".to_owned(),
            ));
        }
        let periodic_jobs =
            PeriodicJobs::from_jobs(self.periodic_jobs, self.leader_election_disabled)?;
        let fetch_kinds = self.fetch_only_known_kinds.then(|| {
            self.workers
                .kinds()
                .into_iter()
                .map(str::to_owned)
                .collect::<Arc<[String]>>()
        });
        #[cfg(feature = "postgres")]
        let schema = self
            .database
            .postgres_schema()
            .cloned()
            .unwrap_or_else(SchemaName::current);
        let (events, _) = broadcast::channel(EVENT_BUFFER_CAPACITY);
        let (queue_changes, _) = watch::channel(0_u64);
        let (leadership_wakeups, _) = broadcast::channel(1_024);
        let (queue_notifications, _) = broadcast::channel(1_024);
        let client = Client {
            inner: Arc::new(ClientInner {
                allow_legacy_job_kinds: self.allow_legacy_job_kinds,
                allow_unregistered_job_kinds: self.allow_unregistered_job_kinds,
                completion_sender: Mutex::new(None),
                database: self.database,
                default_max_attempts: self.default_max_attempts,
                error_handler: self.error_handler,
                events,
                fetch_cooldown: self.fetch_cooldown,
                fetch_kinds,
                fetch_registration_windows: AtomicU64::new(0),
                hooks: self.hooks,
                id: self.id,
                insert_middleware: self.insert_middleware,
                insert_notify_limiter: InsertNotifyLimiter::new(self.fetch_cooldown),
                job_stuck_threshold: self.job_stuck_threshold,
                job_timeout: self.job_timeout,
                leader_election_disabled: self.leader_election_disabled,
                leadership_wakeups,
                live_queues: watch::channel(std::collections::HashSet::new()).0,
                maintenance,
                #[cfg(test)]
                notifier_start_panics: AtomicU64::new(0),
                peer_owners: Mutex::new(HashMap::new()),
                pending_cancellations: Mutex::new(HashMap::new()),
                periodic_jobs,
                pilot: self.pilot,
                poll_only: self.poll_only,
                producer_report_interval: self.producer_report_interval,
                queue_changes,
                queue_notifications,
                queues: RwLock::new(self.queues),
                retry_policy: self.retry_policy,
                running: Mutex::new(HashMap::new()),
                #[cfg(feature = "postgres")]
                schema,
                soft_stop_timeout: self.soft_stop_timeout,
                started: AtomicBool::new(false),
                work_middleware: self.work_middleware,
                workers: self.workers,
            }),
        };
        client
            .inner
            .pilot
            .install(crate::__private::PilotInstallContext {
                client: client.downgrade(),
                database: client.inner.pilot_database(),
                producer_report_interval: client.inner.producer_report_interval,
            });
        Ok(client)
    }
}

/// Checks a queue's extension settings with the client's pilot.
pub(super) fn validate_queue_settings(
    pilot: &dyn Pilot,
    name: &str,
    config: &QueueConfig,
) -> Result<(), Error> {
    pilot
        .validate_queue_settings(name, &config.extension_settings)
        .map_err(|source| Error::Extension {
            phase: crate::ExtensionPhase::AddOn {
                operation: "queue settings",
            },
            source,
        })
}
