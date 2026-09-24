//! Database-backed client, insertion, and worker runtime.

mod attempts;
mod backoff;
mod builder;
mod completer;
mod executor;
mod extension;
mod insert;
mod notifier;
mod producer;
mod record;
mod run;
#[cfg(test)]
mod tests;
mod validate;

pub use self::builder::{ClientBuilder, MaintenanceConfig, QueueConfig};
#[cfg(feature = "sqlite")]
pub(crate) use self::record::decode_attempt_error;
#[cfg(feature = "postgres")]
pub(crate) use self::record::{JobRecord, decode_job_row, job_projection};
pub(crate) use self::record::{UndecodableJob, saturating_i16};
pub use self::run::RunHandle;
#[allow(clippy::wildcard_imports, unused_imports)]
use self::{
    attempts::*, backoff::*, builder::*, completer::*, executor::*, extension::*, insert::*,
    notifier::*, producer::*, run::*, validate::*,
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
    DatabaseConfig as PilotDatabaseConfig, DatabasePool as PilotDatabasePool, NoopPilot, Pilot,
};
use riverqueue_internal::{
    DatabaseConnection as PilotDatabaseConnection, FetchParams,
    JobInsertParams as PilotJobInsertParams, JobSetStateParams, JobSetStateRow,
};

use crate::{
    AttemptError, BoxError, DefaultRetryPolicy, Error, ErrorHandler, ErrorHandlerDecision, Event,
    EventKind, EventReceiver, ExtensionClaimParams, ExtensionInsertParams, FETCH_COOLDOWN_DEFAULT,
    FETCH_COOLDOWN_MIN, FETCH_POLL_INTERVAL_DEFAULT, Hook, InsertBatch, InsertBatchResult,
    InsertContext, InsertMiddleware, InsertOpts, InsertParams, InsertResult,
    JOB_STUCK_THRESHOLD_DEFAULT, JOB_TIMEOUT_DEFAULT, Job, JobArgs, JobEventKind, JobRow, JobState,
    JobStatistics, MAX_ATTEMPTS_DEFAULT, Metric, Plugin, QUEUE_NUM_WORKERS_MAX, QueueEventKind,
    RawInsertResult, RetryPolicy, SchemaName, SubscribeConfig, WorkCancelled, WorkContext,
    WorkError, WorkMiddleware, WorkOutcome, WorkResult, WorkerRegistry, WorkerTimeout,
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
}

impl Client {
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
                    metadata = jsonb_set(metadata, '{{cancel_attempted_at}}'::text[], to_jsonb($4::text), true) \
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
                .bind(go_time_json(Utc::now()))
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

fn default_client_id() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_owned());
    format!("{host}-{}", std::process::id())
}
