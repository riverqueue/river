//! Type-erased database sources and executors.
//!
//! These types are reachable only through `riverqueue::__private`.

use std::fmt;

#[cfg(feature = "sqlite")]
use super::SqliteDatabase;
use crate::__private::DatabaseConnection;

use super::{
    DatabaseExecutor, DatabaseKind, DatabaseMismatch, DatabasePool, DatabaseTransactionExecutor,
    IntoDatabase, SchemaName, into_database, private,
};
#[cfg(feature = "postgres")]
use super::{PostgresDatabase, PostgresReindexConfig};

/// A type-erased built-in River database source.
///
/// This type is public only so the sealed [`IntoDatabase`] contract can be
/// composed across River's exact-version crates. Its backend representation is
/// intentionally private.
#[doc(hidden)]
#[derive(Clone)]
pub struct Database {
    pub(super) inner: DatabaseInner,
}

impl Database {
    /// Erases a sealed built-in database source.
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

    /// Whether the backend delivers notifications to listeners when a
    /// transaction commits, like Go's `SupportsListener`. SQLite clients poll
    /// a notification outbox instead, so operations River commits itself also
    /// wake the local client directly.
    pub(crate) const fn supports_listener(&self) -> bool {
        match &self.inner {
            #[cfg(feature = "postgres")]
            DatabaseInner::Postgres(_) => true,
            #[cfg(feature = "sqlite")]
            DatabaseInner::Sqlite(_) => false,
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
    #[cfg_attr(
        not(feature = "sqlite"),
        expect(
            clippy::unnecessary_wraps,
            reason = "another backend may be compiled in"
        )
    )]
    pub(crate) fn postgres_reindex(&self) -> Option<&PostgresReindexConfig> {
        match &self.inner {
            DatabaseInner::Postgres(source) => Some(source.reindex_config()),
            #[cfg(feature = "sqlite")]
            DatabaseInner::Sqlite(_) => None,
        }
    }

    /// Erases and validates an executor before a backend operation uses it.
    pub fn executor<'executor, E>(
        &self,
        executor: E,
    ) -> Result<ErasedExecutor<'executor>, DatabaseMismatch>
    where
        E: DatabaseExecutor<'executor>,
    {
        let executor = private::DatabaseExecutorSealed::erase(executor);
        self.check_kind(executor.kind())?;
        Ok(executor)
    }

    /// Erases and validates an actual SQLx transaction while preserving its
    /// transaction-only capability for exact-version companion crates.
    pub fn transaction<'executor, E>(
        &self,
        transaction: E,
    ) -> Result<ErasedTransaction<'executor>, DatabaseMismatch>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        Ok(ErasedTransaction {
            connection: self.connection(transaction)?,
        })
    }

    /// Borrows a caller-managed transaction's connection after checking that
    /// it belongs to this database's backend.
    pub(crate) fn connection<'executor, E>(
        &self,
        transaction: E,
    ) -> Result<DatabaseConnection<'executor>, DatabaseMismatch>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        let connection = private::DatabaseTransactionExecutorSealed::connection(transaction);
        self.check_kind(connection.kind())?;
        Ok(connection)
    }

    fn check_kind(&self, actual: DatabaseKind) -> Result<(), DatabaseMismatch> {
        if self.kind() != actual {
            return Err(DatabaseMismatch {
                actual,
                expected: self.kind(),
            });
        }
        Ok(())
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
pub(crate) enum DatabaseInner {
    #[cfg(feature = "postgres")]
    Postgres(PostgresDatabase),
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteDatabase),
}

/// A type-erased borrowed SQLx executor.
///
/// The value is created only through the sealed executor contract.
#[doc(hidden)]
pub struct ErasedExecutor<'executor> {
    pub(super) inner: ExecutorInner<'executor>,
}

impl fmt::Debug for ErasedExecutor<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ErasedExecutor")
            .finish_non_exhaustive()
    }
}

/// Transaction-preserving exact-version executor erasure.
///
/// It holds only a transaction's connection, so an erased transaction can
/// never stand in for a pool.
#[doc(hidden)]
pub struct ErasedTransaction<'executor> {
    pub(super) connection: DatabaseConnection<'executor>,
}

impl fmt::Debug for ErasedTransaction<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ErasedTransaction")
            .finish_non_exhaustive()
    }
}

impl ErasedTransaction<'_> {
    /// Borrows the backend connection for exact-version SQL while retaining
    /// the marker needed to call River's transaction-only methods later.
    pub fn connection(&mut self) -> DatabaseConnection<'_> {
        self.connection.reborrow()
    }
}

impl<'executor> ErasedExecutor<'executor> {
    /// Converts a connection-backed executor for an exact-version extension.
    /// Pool-backed executors return `None`.
    #[must_use]
    pub fn into_connection(self) -> Option<DatabaseConnection<'executor>> {
        match self.inner {
            ExecutorInner::Connection(connection) => Some(connection),
            ExecutorInner::Pool(_) => None,
        }
    }

    /// Returns the executor's backend kind.
    #[must_use]
    pub const fn kind(&self) -> DatabaseKind {
        match &self.inner {
            ExecutorInner::Connection(connection) => connection.kind(),
            ExecutorInner::Pool(kind) => *kind,
        }
    }
}

/// What an erased executor borrows. A pool is recorded only by its backend:
/// River never runs an extension's statements on a pool it did not open.
pub(crate) enum ExecutorInner<'executor> {
    Connection(DatabaseConnection<'executor>),
    Pool(DatabaseKind),
}
