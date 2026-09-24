//! Database sources and executor compatibility contracts.
//!
//! River's database abstraction is closed over its built-in backends. The
//! sealed conversion traits in this module let [`Client`](crate::Client)
//! remain non-generic while preventing an accidental public driver SPI.

use std::fmt;
#[cfg(feature = "postgres")]
use std::time::Duration;

#[cfg(feature = "postgres")]
use chrono::NaiveTime;
pub use riverqueue_migrate::{SchemaName, SchemaNameError};
#[cfg(feature = "postgres")]
use sqlx::{PgConnection, PgPool, Postgres};
#[cfg(feature = "sqlite")]
use sqlx::{Sqlite, SqliteConnection, SqlitePool};
use sqlx::{Transaction, pool::PoolConnection};
use thiserror::Error;

/// Begins a PostgreSQL transaction that is never abandoned half-started.
///
/// SQLx 0.9 records a transaction only once the server has answered `BEGIN`. If
/// the future beginning it is dropped after `BEGIN` reaches the server but
/// before that answer arrives, for example because a `select!` or timeout
/// around it fires, SQLx never queues a `ROLLBACK` and the connection goes
/// back to the pool idle in a transaction. River begins every transaction on
/// its own task instead: if the caller stops waiting, the task still
/// finishes, and dropping the finished transaction rolls it back.
#[cfg(feature = "postgres")]
pub(crate) async fn begin_postgres(
    pool: &PgPool,
) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
    let pool = pool.clone();
    run_to_completion(async move { pool.begin().await }).await
}

/// Begins a SQLite transaction that may write, with the same protection as
/// [`begin_postgres`] against being abandoned half-started.
///
/// `BEGIN IMMEDIATE` takes the write lock up front, so a transaction that
/// reads before writing can't fail with `SQLITE_BUSY_SNAPSHOT` when another
/// connection commits in between.
#[cfg(feature = "sqlite")]
pub(crate) async fn begin_sqlite_write(
    pool: &SqlitePool,
) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
    let pool = pool.clone();
    run_to_completion(async move { pool.begin_with("BEGIN IMMEDIATE").await }).await
}

/// Runs `operation` on its own task, so dropping the returned future doesn't
/// cancel it midway.
async fn run_to_completion<T: Send + 'static>(
    operation: impl Future<Output = Result<T, sqlx::Error>> + Send + 'static,
) -> Result<T, sqlx::Error> {
    match tokio::spawn(operation).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        // The runtime is shutting down and cancelled the task.
        Err(error) => Err(sqlx::Error::Io(std::io::Error::other(format!(
            "transaction begin task failed: {error}"
        )))),
    }
}

pub(crate) mod erased;
#[cfg(feature = "sqlite")]
pub(crate) mod sqlite;

use crate::__private::DatabaseConnection;
pub(crate) use erased::{
    Database, DatabaseInner, ErasedExecutor, ErasedTransaction, ExecutorInner,
};
pub(crate) use private::DatabaseExecutorSealed as DatabaseExecutor;

/// A database backend understood by River.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum DatabaseKind {
    /// PostgreSQL.
    #[cfg(feature = "postgres")]
    Postgres,
    /// SQLite and compatible implementations.
    #[cfg(feature = "sqlite")]
    Sqlite,
}

impl fmt::Display for DatabaseKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            #[cfg(feature = "postgres")]
            Self::Postgres => "PostgreSQL",
            #[cfg(feature = "sqlite")]
            Self::Sqlite => "SQLite",
        })
    }
}

/// A PostgreSQL source and its backend-specific River options.
#[cfg(feature = "postgres")]
#[derive(Clone)]
pub struct PostgresDatabase {
    pool: PgPool,
    reindex: PostgresReindexConfig,
    schema: SchemaName,
}

#[cfg(feature = "postgres")]
impl PostgresDatabase {
    /// Uses a PostgreSQL pool and the connection's current schema.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            reindex: PostgresReindexConfig::default(),
            schema: SchemaName::current(),
        }
    }

    /// Returns the underlying SQLx pool.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Configures PostgreSQL's periodic concurrent index rebuilding.
    #[must_use]
    pub fn reindex(mut self, reindex: PostgresReindexConfig) -> Self {
        self.reindex = reindex;
        self
    }

    /// Returns PostgreSQL reindexer configuration.
    #[must_use]
    pub const fn reindex_config(&self) -> &PostgresReindexConfig {
        &self.reindex
    }

    /// Uses an explicit PostgreSQL schema for River objects and notification
    /// channels.
    #[must_use]
    pub fn schema(mut self, schema: SchemaName) -> Self {
        self.schema = schema;
        self
    }

    /// Returns the configured PostgreSQL schema.
    #[must_use]
    pub const fn schema_name(&self) -> &SchemaName {
        &self.schema
    }
}

#[cfg(feature = "postgres")]
impl fmt::Debug for PostgresDatabase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresDatabase")
            .field("reindex", &self.reindex)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

/// PostgreSQL-specific concurrent reindexer configuration.
#[cfg(feature = "postgres")]
#[derive(Clone, Debug)]
pub struct PostgresReindexConfig {
    index_names: Vec<String>,
    schedule: PostgresReindexSchedule,
    timeout: Duration,
}

#[cfg(feature = "postgres")]
impl PostgresReindexConfig {
    /// Returns configured index names.
    #[must_use]
    pub fn index_names(&self) -> &[String] {
        &self.index_names
    }

    /// Returns the reindex schedule.
    #[must_use]
    pub const fn schedule(&self) -> PostgresReindexSchedule {
        self.schedule
    }

    /// Returns the per-index statement timeout.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Replaces indexes rebuilt by River. An empty iterator disables the
    /// service.
    #[must_use]
    pub fn with_index_names(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.index_names = names.into_iter().map(Into::into).collect();
        self
    }

    /// Replaces the reindex schedule.
    #[must_use]
    pub const fn with_schedule(mut self, schedule: PostgresReindexSchedule) -> Self {
        self.schedule = schedule;
        self
    }

    /// Replaces the per-index statement timeout.
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[cfg(feature = "postgres")]
impl Default for PostgresReindexConfig {
    fn default() -> Self {
        Self {
            index_names: vec![
                "river_job_args_index".to_owned(),
                "river_job_kind".to_owned(),
                "river_job_metadata_index".to_owned(),
                "river_job_pkey".to_owned(),
                "river_job_prioritized_fetching_index".to_owned(),
                "river_job_state_and_finalized_at_index".to_owned(),
                "river_job_unique_idx".to_owned(),
            ],
            schedule: PostgresReindexSchedule::default(),
            timeout: Duration::from_mins(1),
        }
    }
}

/// Schedule used by PostgreSQL's concurrent reindexer.
#[cfg(feature = "postgres")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PostgresReindexSchedule {
    /// Run each day at the supplied UTC wall-clock time.
    DailyUtc(NaiveTime),
    /// Run after each elapsed interval from client startup.
    Interval(Duration),
}

#[cfg(feature = "postgres")]
impl Default for PostgresReindexSchedule {
    fn default() -> Self {
        Self::DailyUtc(NaiveTime::MIN)
    }
}

/// A SQLite source and its backend-specific River options.
#[cfg(feature = "sqlite")]
#[derive(Clone)]
pub struct SqliteDatabase {
    pool: SqlitePool,
}

#[cfg(feature = "sqlite")]
impl SqliteDatabase {
    /// Uses a SQLite pool.
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Returns the underlying SQLx pool.
    #[must_use]
    pub const fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[cfg(feature = "sqlite")]
impl fmt::Debug for SqliteDatabase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteDatabase")
            .finish_non_exhaustive()
    }
}

/// Error returned when an operation receives an executor for another backend.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("database executor mismatch: expected {expected}, received {actual}")]
pub struct DatabaseMismatch {
    actual: DatabaseKind,
    expected: DatabaseKind,
}

impl DatabaseMismatch {
    /// Returns the executor's backend.
    #[must_use]
    pub const fn actual(&self) -> DatabaseKind {
        self.actual
    }

    /// Returns the database backend required by the client.
    #[must_use]
    pub const fn expected(&self) -> DatabaseKind {
        self.expected
    }
}

/// A value accepted as a built-in River database source.
///
/// This trait has no public methods and is sealed. Applications select a
/// backend by passing a [`PgPool`], [`SqlitePool`], [`PostgresDatabase`], or
/// [`SqliteDatabase`]; they do not implement a River driver trait.
pub trait IntoDatabase: private::IntoDatabaseSealed {}

impl<T> IntoDatabase for T where T: private::IntoDatabaseSealed {}

/// A caller-owned SQLx transaction accepted by River's transactional
/// operations.
///
/// This trait has no public methods and is sealed. It is deliberately not
/// implemented for pools or bare connections so a `_tx` method cannot
/// accidentally run in autocommit mode.
///
/// For SQLite transactions that may write, use
/// `pool.begin_with("BEGIN IMMEDIATE")`. A deferred transaction that reads
/// before writing can fail with `SQLITE_BUSY_SNAPSHOT` when another pool
/// connection commits between those operations; a busy timeout cannot make a
/// stale snapshot writable.
///
/// Bare connections and pool connections are intentionally rejected:
///
/// ```compile_fail
/// # async fn example(
/// #     client: &riverqueue::Client,
/// #     connection: &mut sqlx::PgConnection,
/// # ) -> Result<(), riverqueue::Error> {
/// client.job_get_tx(connection, 1).await?;
/// # Ok(())
/// # }
/// ```
///
/// ```compile_fail
/// # async fn example(
/// #     client: &riverqueue::Client,
/// #     connection: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
/// # ) -> Result<(), riverqueue::Error> {
/// client.job_get_tx(connection, 1).await?;
/// # Ok(())
/// # }
/// ```
pub trait DatabaseTransactionExecutor<'executor>:
    private::DatabaseTransactionExecutorSealed<'executor>
{
}

impl<'executor, T> DatabaseTransactionExecutor<'executor> for T where
    T: private::DatabaseTransactionExecutorSealed<'executor>
{
}

/// Converts a public sealed database source into River's internal erased form.
pub(crate) fn into_database<D: IntoDatabase>(database: D) -> Database {
    private::IntoDatabaseSealed::erase(database)
}

/// A borrowed built-in pool used by River's internal operation dispatch.
pub(crate) enum DatabasePool<'pool> {
    #[cfg(feature = "postgres")]
    Postgres(&'pool PgPool),
    #[cfg(feature = "sqlite")]
    Sqlite(&'pool SqlitePool),
}

mod private {
    use super::{
        Database, DatabaseConnection, DatabaseInner, DatabaseKind, ErasedExecutor,
        ErasedTransaction, ExecutorInner, PoolConnection, Transaction,
    };
    #[cfg(feature = "postgres")]
    use super::{PgConnection, PgPool, Postgres, PostgresDatabase};
    #[cfg(feature = "sqlite")]
    use super::{Sqlite, SqliteConnection, SqliteDatabase, SqlitePool};

    pub trait IntoDatabaseSealed {
        fn erase(self) -> Database;
    }

    pub trait DatabaseExecutorSealed<'executor> {
        fn erase(self) -> ErasedExecutor<'executor>;
    }

    /// A caller-managed transaction. Only transactions implement this, so
    /// the connection it yields is always inside a transaction River does
    /// not commit.
    pub trait DatabaseTransactionExecutorSealed<'executor>:
        DatabaseExecutorSealed<'executor>
    {
        fn connection(self) -> DatabaseConnection<'executor>;
    }

    const fn connection_executor(connection: DatabaseConnection<'_>) -> ErasedExecutor<'_> {
        ErasedExecutor {
            inner: ExecutorInner::Connection(connection),
        }
    }

    const fn pool_executor<'executor>(kind: DatabaseKind) -> ErasedExecutor<'executor> {
        ErasedExecutor {
            inner: ExecutorInner::Pool(kind),
        }
    }

    impl IntoDatabaseSealed for Database {
        fn erase(self) -> Database {
            self
        }
    }

    #[cfg(feature = "postgres")]
    impl IntoDatabaseSealed for PgPool {
        fn erase(self) -> Database {
            PostgresDatabase::new(self).erase()
        }
    }

    #[cfg(feature = "postgres")]
    impl IntoDatabaseSealed for &PgPool {
        fn erase(self) -> Database {
            self.clone().erase()
        }
    }

    #[cfg(feature = "postgres")]
    impl IntoDatabaseSealed for PostgresDatabase {
        fn erase(self) -> Database {
            Database {
                inner: DatabaseInner::Postgres(self),
            }
        }
    }

    #[cfg(feature = "postgres")]
    impl IntoDatabaseSealed for &PostgresDatabase {
        fn erase(self) -> Database {
            self.clone().erase()
        }
    }

    #[cfg(feature = "sqlite")]
    impl IntoDatabaseSealed for SqlitePool {
        fn erase(self) -> Database {
            SqliteDatabase::new(self).erase()
        }
    }

    #[cfg(feature = "sqlite")]
    impl IntoDatabaseSealed for &SqlitePool {
        fn erase(self) -> Database {
            self.clone().erase()
        }
    }

    #[cfg(feature = "sqlite")]
    impl IntoDatabaseSealed for SqliteDatabase {
        fn erase(self) -> Database {
            Database {
                inner: DatabaseInner::Sqlite(self),
            }
        }
    }

    #[cfg(feature = "sqlite")]
    impl IntoDatabaseSealed for &SqliteDatabase {
        fn erase(self) -> Database {
            self.clone().erase()
        }
    }

    #[cfg(feature = "postgres")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor mut PgConnection {
        fn erase(self) -> ErasedExecutor<'executor> {
            connection_executor(DatabaseConnection::Postgres(self))
        }
    }

    #[cfg(feature = "postgres")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor mut PoolConnection<Postgres> {
        fn erase(self) -> ErasedExecutor<'executor> {
            connection_executor(DatabaseConnection::Postgres(self.as_mut()))
        }
    }

    #[cfg(feature = "postgres")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor PgPool {
        fn erase(self) -> ErasedExecutor<'executor> {
            pool_executor(DatabaseKind::Postgres)
        }
    }

    #[cfg(feature = "postgres")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor mut Transaction<'_, Postgres> {
        fn erase(self) -> ErasedExecutor<'executor> {
            connection_executor(DatabaseTransactionExecutorSealed::connection(self))
        }
    }

    #[cfg(feature = "postgres")]
    impl<'executor> DatabaseTransactionExecutorSealed<'executor>
        for &'executor mut Transaction<'_, Postgres>
    {
        fn connection(self) -> DatabaseConnection<'executor> {
            DatabaseConnection::Postgres(self.as_mut())
        }
    }

    #[cfg(feature = "sqlite")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor mut SqliteConnection {
        fn erase(self) -> ErasedExecutor<'executor> {
            connection_executor(DatabaseConnection::Sqlite(self))
        }
    }

    #[cfg(feature = "sqlite")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor mut PoolConnection<Sqlite> {
        fn erase(self) -> ErasedExecutor<'executor> {
            connection_executor(DatabaseConnection::Sqlite(self.as_mut()))
        }
    }

    #[cfg(feature = "sqlite")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor SqlitePool {
        fn erase(self) -> ErasedExecutor<'executor> {
            pool_executor(DatabaseKind::Sqlite)
        }
    }

    #[cfg(feature = "sqlite")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor mut Transaction<'_, Sqlite> {
        fn erase(self) -> ErasedExecutor<'executor> {
            connection_executor(DatabaseTransactionExecutorSealed::connection(self))
        }
    }

    #[cfg(feature = "sqlite")]
    impl<'executor> DatabaseTransactionExecutorSealed<'executor>
        for &'executor mut Transaction<'_, Sqlite>
    {
        fn connection(self) -> DatabaseConnection<'executor> {
            DatabaseConnection::Sqlite(self.as_mut())
        }
    }

    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor mut ErasedTransaction<'_> {
        fn erase(self) -> ErasedExecutor<'executor> {
            connection_executor(self.connection())
        }
    }

    impl<'executor> DatabaseTransactionExecutorSealed<'executor>
        for &'executor mut ErasedTransaction<'_>
    {
        fn connection(self) -> DatabaseConnection<'executor> {
            ErasedTransaction::connection(self)
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::{str::FromStr, sync::Arc, time::Duration};

    use sqlx::{
        Executor,
        sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    };
    use tokio::sync::Barrier;

    use super::begin_sqlite_write;

    #[tokio::test]
    async fn immediate_writer_avoids_snapshot_upgrade_failure() {
        let database_path = std::env::temp_dir().join(format!(
            "river-sqlite-write-contention-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let options =
            SqliteConnectOptions::from_str(&format!("sqlite://{}", database_path.display()))
                .unwrap()
                .create_if_missing(true)
                .busy_timeout(Duration::from_secs(2))
                .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .unwrap();
        pool.execute("CREATE TABLE contention (value INTEGER NOT NULL)")
            .await
            .unwrap();
        pool.execute("INSERT INTO contention (value) VALUES (1)")
            .await
            .unwrap();

        let mut deferred = pool.begin().await.unwrap();
        let _: i64 = sqlx::query_scalar("SELECT value FROM contention")
            .fetch_one(&mut *deferred)
            .await
            .unwrap();
        pool.execute("UPDATE contention SET value = value + 1")
            .await
            .unwrap();
        let error = sqlx::query("UPDATE contention SET value = value + 1")
            .execute(&mut *deferred)
            .await
            .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::code)
                .as_deref(),
            Some("517"),
            "expected SQLITE_BUSY_SNAPSHOT, received {error}"
        );
        deferred.rollback().await.unwrap();

        let mut immediate = begin_sqlite_write(&pool).await.unwrap();
        let _: i64 = sqlx::query_scalar("SELECT value FROM contention")
            .fetch_one(&mut *immediate)
            .await
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let writer_barrier = Arc::clone(&barrier);
        let writer_pool = pool.clone();
        let mut competing_writer = tokio::spawn(async move {
            writer_barrier.wait().await;
            writer_pool
                .execute("UPDATE contention SET value = value + 1")
                .await
        });
        barrier.wait().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut competing_writer)
                .await
                .is_err(),
            "a competing writer should wait for the immediate transaction"
        );
        immediate
            .execute("UPDATE contention SET value = value + 1")
            .await
            .unwrap();
        immediate.commit().await.unwrap();
        competing_writer.await.unwrap().unwrap();
        let value: i64 = sqlx::query_scalar("SELECT value FROM contention")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(value, 4);

        pool.close().await;
        std::fs::remove_file(database_path).unwrap();
    }
}

#[cfg(all(test, feature = "postgres-tests"))]
mod postgres_begin_tests {
    use std::{sync::Arc, time::Duration};

    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{Notify, watch},
    };

    use super::begin_postgres;

    /// A TCP proxy that reports when a client sends `BEGIN` and can hold the
    /// server's replies, so a test can stop waiting for a begin that already
    /// reached the server.
    struct BeginProxy {
        address: std::net::SocketAddr,
        begin_sent: Arc<Notify>,
        hold_replies: watch::Sender<bool>,
    }

    impl BeginProxy {
        async fn start(upstream: &PgConnectOptions) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let upstream = format!("{}:{}", upstream.get_host(), upstream.get_port());
            let begin_sent = Arc::new(Notify::new());
            let (hold_replies, hold) = watch::channel(false);
            let sent = Arc::clone(&begin_sent);
            tokio::spawn(async move {
                while let Ok((client, _)) = listener.accept().await {
                    let server = TcpStream::connect(&upstream).await.unwrap();
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    let sent = Arc::clone(&sent);
                    tokio::spawn(async move {
                        let mut buffer = vec![0; 8192];
                        while let Ok(read) = client_read.read(&mut buffer).await {
                            if read == 0 || server_write.write_all(&buffer[..read]).await.is_err() {
                                break;
                            }
                            if buffer[..read].windows(5).any(|window| window == b"BEGIN") {
                                sent.notify_one();
                            }
                        }
                    });
                    let mut hold = hold.clone();
                    tokio::spawn(async move {
                        let mut buffer = vec![0; 8192];
                        while let Ok(read) = server_read.read(&mut buffer).await {
                            if read == 0 {
                                break;
                            }
                            if hold.wait_for(|held| !held).await.is_err() {
                                break;
                            }
                            if client_write.write_all(&buffer[..read]).await.is_err() {
                                break;
                            }
                        }
                    });
                }
            });
            Self {
                address,
                begin_sent,
                hold_replies,
            }
        }
    }

    /// Counts this pool's server connections that are idle inside a
    /// transaction, observed through a separate connection.
    async fn idle_in_transaction(observer: &PgPool, application_name: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND application_name = $1 \
               AND state = 'idle in transaction'",
        )
        .bind(application_name)
        .fetch_one(observer)
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn abandoned_begin_does_not_leave_a_connection_in_a_transaction() {
        let url = std::env::var("RIVER_RUST_DATABASE_URL")
            .expect("RIVER_RUST_DATABASE_URL must point at a disposable test database");
        let upstream: PgConnectOptions = url.parse().unwrap();
        let observer = PgPool::connect(&url).await.unwrap();
        let proxy = BeginProxy::start(&upstream).await;
        let application_name = format!("river-begin-cancel-{}", std::process::id());
        // One connection, used without a liveness query that the held
        // replies would stall.
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .test_before_acquire(false)
            .connect_with(
                upstream
                    .clone()
                    .host(&proxy.address.ip().to_string())
                    .port(proxy.address.port())
                    .application_name(&application_name),
            )
            .await
            .unwrap();

        // Stop waiting for a begin after `BEGIN` reached the server but
        // before its reply, as a `select!` or timeout around it would.
        proxy.hold_replies.send_replace(true);
        tokio::select! {
            result = begin_postgres(&pool) => panic!("begin finished while replies were held: {result:?}"),
            () = proxy.begin_sent.notified() => {}
        }
        proxy.hold_replies.send_replace(false);

        // The abandoned begin still finishes and rolls back, so the pool's
        // only connection ends up idle outside a transaction.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                // Use the connection so any unread reply is consumed, then
                // check its server-side state once it's back in the pool.
                let mut connection = pool.acquire().await.unwrap();
                sqlx::query("SELECT 1")
                    .execute(&mut *connection)
                    .await
                    .unwrap();
                drop(connection);
                if idle_in_transaction(&observer, &application_name).await == 0
                    && sqlx::query_scalar::<_, i64>(
                        "SELECT count(*) FROM pg_stat_activity \
                         WHERE datname = current_database() AND application_name = $1 \
                           AND state = 'idle'",
                    )
                    .bind(&application_name)
                    .fetch_one(&observer)
                    .await
                    .unwrap()
                        == 1
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("an abandoned begin left its connection inside a transaction");

        pool.close().await;
        observer.close().await;
    }
}
