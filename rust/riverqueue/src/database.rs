//! Database sources and executor compatibility contracts.
//!
//! River's database abstraction is closed over its built-in backends. The
//! sealed conversion traits in this module let [`Client`](crate::Client)
//! remain non-generic while preventing an accidental public driver SPI.
//! The repository's [Rust API design record](https://github.com/riverqueue/river/blob/master/docs/rust-api-design.md)
//! explains why the backend seam is sealed and how new built-in backends are
//! added without making the client generic.

use std::fmt;
#[cfg(feature = "postgres")]
use std::time::Duration;

#[cfg(feature = "postgres")]
use chrono::NaiveTime;
pub use riverqueue_internal::SchemaName;
#[cfg(feature = "postgres")]
use sqlx::{PgConnection, PgPool, Postgres};
#[cfg(feature = "sqlite")]
use sqlx::{Sqlite, SqliteConnection, SqlitePool};
use sqlx::{Transaction, pool::PoolConnection};
use thiserror::Error;

/// Encodes a UTC timestamp in River's canonical SQLite wire format.
///
/// This exact-version helper keeps companion crates aligned with River and
/// Go's millisecond-rounded, timezone-free SQLite representation.
#[cfg(feature = "sqlite")]
#[doc(hidden)]
#[must_use]
pub fn sqlite_timestamp(time: chrono::DateTime<chrono::Utc>) -> String {
    sqlite::sqlite_time(time)
}

#[cfg(feature = "sqlite")]
pub(crate) async fn begin_sqlite_write(
    pool: &SqlitePool,
) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
    pool.begin_with("BEGIN IMMEDIATE").await
}

#[cfg(feature = "sqlite")]
pub(crate) mod sqlite;

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

/// A type-erased built-in River database source.
///
/// This type is public only so the sealed [`IntoDatabase`] contract can be
/// composed across River's exact-version crates. Its backend representation is
/// intentionally private.
#[doc(hidden)]
#[derive(Clone)]
pub struct Database {
    inner: DatabaseInner,
}

impl Database {
    /// Erases a sealed built-in database source.
    #[doc(hidden)]
    #[must_use]
    pub fn from_source<D: IntoDatabase>(database: D) -> Self {
        into_database(database)
    }

    /// Returns the configured backend kind.
    #[must_use]
    pub const fn kind(&self) -> DatabaseKind {
        match &self.inner {
            #[cfg(feature = "postgres")]
            DatabaseInner::Postgres(_) => DatabaseKind::Postgres,
            #[cfg(feature = "sqlite")]
            DatabaseInner::Sqlite(_) => DatabaseKind::Sqlite,
        }
    }

    /// Returns the PostgreSQL schema, or `None` for a backend without
    /// PostgreSQL schemas.
    #[must_use]
    pub fn postgres_schema(&self) -> Option<&SchemaName> {
        match &self.inner {
            #[cfg(feature = "postgres")]
            DatabaseInner::Postgres(source) => Some(source.schema_name()),
            #[cfg(feature = "sqlite")]
            DatabaseInner::Sqlite(_) => None,
        }
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn postgres_reindex(&self) -> Option<&PostgresReindexConfig> {
        match &self.inner {
            DatabaseInner::Postgres(source) => Some(source.reindex_config()),
            #[cfg(feature = "sqlite")]
            DatabaseInner::Sqlite(_) => None,
        }
    }

    /// Erases and validates an executor before a backend operation uses it.
    #[doc(hidden)]
    pub fn executor<'executor, E>(
        &self,
        executor: E,
    ) -> Result<ErasedExecutor<'executor>, DatabaseMismatch>
    where
        E: DatabaseExecutor<'executor>,
    {
        let executor = private::DatabaseExecutorSealed::erase(executor);
        if self.kind() != executor.kind() {
            return Err(DatabaseMismatch {
                actual: executor.kind(),
                expected: self.kind(),
            });
        }
        Ok(executor)
    }

    /// Erases and validates an actual SQLx transaction while preserving its
    /// transaction-only capability for exact-version companion crates.
    #[doc(hidden)]
    pub fn transaction<'executor, E>(
        &self,
        transaction: E,
    ) -> Result<ErasedTransaction<'executor>, DatabaseMismatch>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        let executor = self.executor(transaction)?;
        Ok(ErasedTransaction {
            inner: executor.into_inner(),
        })
    }

    /// Returns a backend-specific borrowed pool for internal dispatch.
    pub(crate) const fn pool(&self) -> DatabasePool<'_> {
        match &self.inner {
            #[cfg(feature = "postgres")]
            DatabaseInner::Postgres(source) => DatabasePool::Postgres(source.pool()),
            #[cfg(feature = "sqlite")]
            DatabaseInner::Sqlite(source) => DatabasePool::Sqlite(source.pool()),
        }
    }
}

impl fmt::Debug for Database {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Database")
            .field("kind", &self.kind())
            .field("postgres_schema", &self.postgres_schema())
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
enum DatabaseInner {
    #[cfg(feature = "postgres")]
    Postgres(PostgresDatabase),
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteDatabase),
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

/// An SQLx pool or connection accepted by River's internal operation dispatch.
///
/// This trait has no public methods and is sealed. A client rejects an executor
/// whose backend differs from its database before issuing a query.
#[doc(hidden)]
pub trait DatabaseExecutor<'executor>: private::DatabaseExecutorSealed<'executor> {}

impl<'executor, T> DatabaseExecutor<'executor> for T where
    T: private::DatabaseExecutorSealed<'executor>
{
}

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
    DatabaseExecutor<'executor> + private::DatabaseTransactionExecutorSealed<'executor>
{
}

impl<'executor, T> DatabaseTransactionExecutor<'executor> for T where
    T: DatabaseExecutor<'executor> + private::DatabaseTransactionExecutorSealed<'executor>
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

/// A type-erased borrowed SQLx executor.
///
/// The value is created only through the sealed [`DatabaseExecutor`] contract.
#[doc(hidden)]
pub struct ErasedExecutor<'executor> {
    inner: ExecutorInner<'executor>,
}

/// Transaction-preserving exact-version executor erasure.
#[doc(hidden)]
pub struct ErasedTransaction<'executor> {
    inner: ExecutorInner<'executor>,
}

impl ErasedTransaction<'_> {
    /// Borrows the backend connection for exact-version SQL while retaining
    /// the marker needed to call River's transaction-only methods later.
    #[doc(hidden)]
    pub fn connection(&mut self) -> riverqueue_internal::DatabaseConnection<'_> {
        match &mut self.inner {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                riverqueue_internal::DatabaseConnection::Postgres(connection)
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                riverqueue_internal::DatabaseConnection::Sqlite(connection)
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => unreachable!("transactions cannot contain pools"),
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => unreachable!("transactions cannot contain pools"),
        }
    }
}

impl<'executor> ErasedExecutor<'executor> {
    /// Converts a connection-backed executor for an exact-version extension.
    /// Pool-backed executors return `None`.
    #[doc(hidden)]
    #[must_use]
    pub fn into_connection(self) -> Option<riverqueue_internal::DatabaseConnection<'executor>> {
        match self.inner {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => Some(
                riverqueue_internal::DatabaseConnection::Postgres(connection),
            ),
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => None,
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                Some(riverqueue_internal::DatabaseConnection::Sqlite(connection))
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => None,
        }
    }

    /// Returns the executor's backend kind.
    #[must_use]
    pub const fn kind(&self) -> DatabaseKind {
        match &self.inner {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(_) | ExecutorInner::PostgresPool(_) => {
                DatabaseKind::Postgres
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(_) | ExecutorInner::SqlitePool(_) => {
                DatabaseKind::Sqlite
            }
        }
    }

    pub(crate) fn into_inner(self) -> ExecutorInner<'executor> {
        self.inner
    }
}

pub(crate) enum ExecutorInner<'executor> {
    #[cfg(feature = "postgres")]
    PostgresConnection(&'executor mut PgConnection),
    #[cfg(feature = "postgres")]
    PostgresPool(&'executor PgPool),
    #[cfg(feature = "sqlite")]
    SqliteConnection(&'executor mut SqliteConnection),
    #[cfg(feature = "sqlite")]
    SqlitePool(&'executor SqlitePool),
}

mod private {
    use super::{
        Database, DatabaseInner, ErasedExecutor, ExecutorInner, PoolConnection, Transaction,
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

    pub trait DatabaseTransactionExecutorSealed<'executor>:
        DatabaseExecutorSealed<'executor>
    {
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
            ErasedExecutor {
                inner: ExecutorInner::PostgresConnection(self),
            }
        }
    }

    #[cfg(feature = "postgres")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor mut PoolConnection<Postgres> {
        fn erase(self) -> ErasedExecutor<'executor> {
            ErasedExecutor {
                inner: ExecutorInner::PostgresConnection(self.as_mut()),
            }
        }
    }

    #[cfg(feature = "postgres")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor PgPool {
        fn erase(self) -> ErasedExecutor<'executor> {
            ErasedExecutor {
                inner: ExecutorInner::PostgresPool(self),
            }
        }
    }

    #[cfg(feature = "postgres")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor mut Transaction<'_, Postgres> {
        fn erase(self) -> ErasedExecutor<'executor> {
            ErasedExecutor {
                inner: ExecutorInner::PostgresConnection(self.as_mut()),
            }
        }
    }

    #[cfg(feature = "postgres")]
    impl<'executor> DatabaseTransactionExecutorSealed<'executor>
        for &'executor mut Transaction<'_, Postgres>
    {
    }

    #[cfg(feature = "sqlite")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor mut SqliteConnection {
        fn erase(self) -> ErasedExecutor<'executor> {
            ErasedExecutor {
                inner: ExecutorInner::SqliteConnection(self),
            }
        }
    }

    #[cfg(feature = "sqlite")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor mut PoolConnection<Sqlite> {
        fn erase(self) -> ErasedExecutor<'executor> {
            ErasedExecutor {
                inner: ExecutorInner::SqliteConnection(self.as_mut()),
            }
        }
    }

    #[cfg(feature = "sqlite")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor SqlitePool {
        fn erase(self) -> ErasedExecutor<'executor> {
            ErasedExecutor {
                inner: ExecutorInner::SqlitePool(self),
            }
        }
    }

    #[cfg(feature = "sqlite")]
    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor mut Transaction<'_, Sqlite> {
        fn erase(self) -> ErasedExecutor<'executor> {
            ErasedExecutor {
                inner: ExecutorInner::SqliteConnection(self.as_mut()),
            }
        }
    }

    #[cfg(feature = "sqlite")]
    impl<'executor> DatabaseTransactionExecutorSealed<'executor>
        for &'executor mut Transaction<'_, Sqlite>
    {
    }

    impl<'executor> DatabaseExecutorSealed<'executor> for &'executor mut super::ErasedTransaction<'_> {
        fn erase(self) -> ErasedExecutor<'executor> {
            let inner = match &mut self.inner {
                #[cfg(feature = "postgres")]
                ExecutorInner::PostgresConnection(connection) => {
                    ExecutorInner::PostgresConnection(connection)
                }
                #[cfg(feature = "sqlite")]
                ExecutorInner::SqliteConnection(connection) => {
                    ExecutorInner::SqliteConnection(connection)
                }
                #[cfg(feature = "postgres")]
                ExecutorInner::PostgresPool(_) => unreachable!("transactions cannot contain pools"),
                #[cfg(feature = "sqlite")]
                ExecutorInner::SqlitePool(_) => unreachable!("transactions cannot contain pools"),
            };
            ErasedExecutor { inner }
        }
    }

    impl<'executor> DatabaseTransactionExecutorSealed<'executor>
        for &'executor mut super::ErasedTransaction<'_>
    {
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
