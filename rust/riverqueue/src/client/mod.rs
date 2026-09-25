//! Database-backed client, insertion, and worker runtime.

mod attempts;
mod backoff;
mod builder;
mod completer;
mod executor;
mod extension;
mod insert;
mod jobs;
mod local_queues;
mod notifier;
mod producer;
mod queues;
mod record;
mod request;
mod resign;
mod run;
#[cfg(test)]
mod tests;
mod validate;

pub use self::builder::{ClientBuilder, MaintenanceConfig, QueueConfig};
pub use self::extension::ExtensionClient;
pub use self::insert::{
    InsertBatchRequest, InsertManyFastRequest, InsertManyItem, InsertManyRequest, InsertRequest,
};
pub use self::jobs::{
    JobCancelRequest, JobCompleteRequest, JobCompleteTxRequest, JobDeleteManyRequest,
    JobDeleteRequest, JobGetRequest, JobListRequest, JobRetryRequest, JobUpdateRequest, Jobs,
};
pub use self::local_queues::LocalQueues;
pub use self::queues::{
    QueueGetRequest, QueueListRequest, QueuePauseRequest, QueueResumeRequest, QueueUpdateRequest,
    Queues,
};
#[cfg(feature = "sqlite")]
pub(crate) use self::record::FieldErrors;
pub(crate) use self::record::{DecodedJob, UndecodableJob, saturating_i16, tolerant_row};
#[cfg(feature = "postgres")]
pub(crate) use self::record::{JobRecord, decode_job_row, job_projection};
pub use self::resign::ResignRequest;
pub use self::run::{RunHandle, Stopper};
#[allow(clippy::wildcard_imports, unused_imports)]
use self::{
    attempts::*, backoff::*, builder::*, completer::*, executor::*, extension::*, insert::*,
    notifier::*, producer::*, request::*, run::*, validate::*,
};
pub(crate) use self::{
    completer::after_jobs_set_state, executor::default_retry_delay, notifier::RuntimeNotification,
    validate::validate_queue,
};

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
use serde_json::{Map, Value, value::RawValue};
use sha2::{Digest, Sha256};
use sqlx::AssertSqlSafe;
#[cfg(feature = "sqlite")]
use sqlx::SqlitePool;
#[cfg(feature = "postgres")]
use sqlx::{
    Executor, PgConnection, PgPool, Postgres,
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

use crate::__private::{
    DatabaseConfig as PilotDatabaseConfig, DatabasePool as PilotDatabasePool, NoopPilot, Pilot,
};
use crate::__private::{
    DatabaseConnection as PilotDatabaseConnection, FetchParams,
    JobInsertParams as PilotJobInsertParams, JobSetStateParams,
};

use crate::extension::{WorkEndpoint, WorkNext};
use crate::{
    AttemptError, BoxError, DefaultRetryPolicy, Error, ErrorHandler, ErrorHandlerDecision, Event,
    EventKind, EventReceiver, FETCH_COOLDOWN_DEFAULT, FETCH_COOLDOWN_MIN,
    FETCH_POLL_INTERVAL_DEFAULT, Hook, InsertBatch, InsertBatchResult, InsertContext,
    InsertMiddleware, InsertOpts, InsertParams, InsertResult, JOB_STUCK_THRESHOLD_DEFAULT,
    JOB_TIMEOUT_DEFAULT, Job, JobArgs, JobEventKind, JobRow, JobState, JobStatistics,
    MAX_ATTEMPTS_DEFAULT, Metric, Plugin, QUEUE_NUM_WORKERS_MAX, QueueEventKind, RetryPolicy,
    SchemaName, SubscribeConfig, WorkCancelled, WorkContext, WorkError, WorkMiddleware,
    WorkOutcome, WorkResult, WorkerRegistry, WorkerTimeout,
    database::{Database, DatabaseKind, DatabasePool, DatabaseTransactionExecutor, IntoDatabase},
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

#[allow(
    clippy::struct_excessive_bools,
    reason = "each flag is an independent configuration option, not a state"
)]
pub(crate) struct ClientInner {
    completion_sender: Mutex<Option<mpsc::WeakSender<CompletionUpdate>>>,
    pub(crate) database: Database,
    default_max_attempts: i16,
    error_handler: Option<Arc<dyn crate::extension::DynErrorHandler>>,
    pub(crate) events: broadcast::Sender<Event>,
    fetch_registration_windows: AtomicU64,
    pub(crate) hooks: Vec<Arc<dyn crate::extension::DynHook>>,
    pub(crate) id: String,
    job_stuck_threshold: Duration,
    pub(crate) job_timeout: Option<Duration>,
    leader_election_disabled: bool,
    pub(crate) maintenance: MaintenanceConfig,
    insert_middleware: Vec<Arc<dyn crate::extension::DynInsertMiddleware>>,
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
    work_middleware: Vec<Arc<dyn crate::extension::DynWorkMiddleware>>,
}

#[cfg(feature = "sqlite")]
fn sqlite_backend_error(error: crate::database::sqlite::BackendError) -> Error {
    Error::Database(Box::new(error))
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
    /// Borrows a caller-managed transaction's connection, rejecting a
    /// transaction from another backend.
    pub(crate) fn transaction_connection<'executor, E>(
        &self,
        transaction: E,
    ) -> Result<PilotDatabaseConnection<'executor>, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        Ok(self.database.connection(transaction)?)
    }

    #[cfg(feature = "postgres")]
    pub(crate) const fn database(&self) -> &Database {
        &self.database
    }

    /// Whether this client receives notifications from other clients, which
    /// needs a backend listener and a client that isn't poll-only. When it
    /// doesn't, it wakes its own runtime directly after committing a change,
    /// like Go's `notifyProducerWithoutListener*` helpers.
    pub(crate) const fn listens_for_notifications(&self) -> bool {
        self.database.supports_listener() && !self.poll_only
    }

    #[cfg(feature = "postgres")]
    #[cfg_attr(
        not(feature = "sqlite"),
        expect(
            clippy::unnecessary_wraps,
            reason = "another backend may be compiled in"
        )
    )]
    pub(crate) fn postgres_pool(&self) -> Option<&PgPool> {
        match self.database.pool() {
            DatabasePool::Postgres(pool) => Some(pool),
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(_) => None,
        }
    }

    #[cfg(feature = "sqlite")]
    #[cfg_attr(
        not(feature = "postgres"),
        expect(
            clippy::unnecessary_wraps,
            reason = "another backend may be compiled in"
        )
    )]
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

/// Non-owning handle used by extension services.
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
            leader_election_disabled: false,
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

    /// Creates a non-owning handle for an extension service.
    #[must_use]
    pub(crate) fn downgrade(&self) -> WeakClient {
        WeakClient {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Stable identifier recorded in `attempted_by`.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.inner.id
    }
}

impl Client {
    /// Returns the dynamic periodic-job bundle for this client.
    ///
    /// Only the elected leader enqueues periodic jobs, so jobs added here
    /// take effect only while this client leads. To fully enable or disable a
    /// periodic job, change it on every client eligible for leader election.
    /// A client built with
    /// [`without_leader_election`](ClientBuilder::without_leader_election)
    /// rejects additions.
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
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
                // Stop forwarding as soon as the subscriber drops its
                // receiver, rather than at the next matching event.
                let next = tokio::select! {
                    () = sender.closed() => break,
                    next = source.recv() => next,
                };
                match next {
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
}

impl Client {
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
}

impl Client {
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

/// Formats a time as River Go's `time.Time` JSON (RFC 3339 with nanoseconds
/// and trailing zeros trimmed, in UTC), which River stores for
/// `cancel_attempted_at`.
pub(crate) fn go_time_json(time: DateTime<Utc>) -> String {
    let formatted = time.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let Some((seconds, fraction)) = formatted.trim_end_matches('Z').split_once('.') else {
        return formatted;
    };
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        format!("{seconds}Z")
    } else {
        format!("{seconds}.{fraction}Z")
    }
}

/// Generates a client ID unique to this `Client` instance.
///
/// Like Go, the ID combines the host name (dots replaced by underscores and
/// truncated to 60 bytes) with the creation time to the microsecond. A random
/// suffix keeps IDs distinct when several clients start in the same
/// microsecond or containers report identical host names, because a shared ID
/// would let two clients renew one leadership lease.
fn default_client_id() -> String {
    default_client_id_with_host(&host_name(), Utc::now(), crate::maintenance::random_u64())
}

fn default_client_id_with_host(host: &str, created_at: DateTime<Utc>, random: u64) -> String {
    const MAX_HOST_LENGTH: usize = 60;

    let mut host = host.replace('.', "_");
    if host.len() > MAX_HOST_LENGTH {
        let mut end = MAX_HOST_LENGTH;
        while !host.is_char_boundary(end) {
            end -= 1;
        }
        host.truncate(end);
    }
    format!(
        "{host}_{}_{:08x}",
        created_at.format("%Y_%m_%dT%H_%M_%S_%6f"),
        random & 0xffff_ffff
    )
}

fn host_name() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/proc/sys/kernel/hostname").ok())
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|host| host.trim().to_owned())
        .filter(|host| !host.is_empty())
        .unwrap_or_else(|| "unknown_host".to_owned())
}

#[cfg(test)]
mod default_client_id_tests {
    use chrono::{TimeZone, Timelike};

    use super::*;

    #[test]
    fn default_client_id_matches_go_shape_and_is_unique() {
        let created_at = Utc
            .with_ymd_and_hms(2026, 1, 2, 3, 4, 5)
            .unwrap()
            .with_nanosecond(678_901_000)
            .unwrap();
        assert_eq!(
            default_client_id_with_host("worker.example.com", created_at, 0xdead_beef),
            "worker_example_com_2026_01_02T03_04_05_678901_deadbeef"
        );
        let long = "h".repeat(80);
        let id = default_client_id_with_host(&long, created_at, 1);
        assert!(id.starts_with(&"h".repeat(60)));
        assert!(!id.starts_with(&"h".repeat(61)));
        assert!(id.len() <= 100, "client IDs are limited to 100 bytes");

        let first = default_client_id();
        let second = default_client_id();
        assert_ne!(first, second);
    }
}
