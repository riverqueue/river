//! Type-erased database sources and executors.
//!
//! These types are reachable only through `riverqueue::__private`.

use std::fmt;

#[cfg(feature = "postgres")]
use sqlx::{PgConnection, PgPool};
#[cfg(feature = "sqlite")]
use sqlx::{SqliteConnection, SqlitePool};

#[cfg(feature = "sqlite")]
use super::SqliteDatabase;
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

/// Transaction-preserving exact-version executor erasure.
#[doc(hidden)]
pub struct ErasedTransaction<'executor> {
    pub(super) inner: ExecutorInner<'executor>,
}

impl ErasedTransaction<'_> {
    /// Borrows the backend connection for exact-version SQL while retaining
    /// the marker needed to call River's transaction-only methods later.
    pub fn connection(&mut self) -> crate::__private::DatabaseConnection<'_> {
        match &mut self.inner {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                crate::__private::DatabaseConnection::Postgres(connection)
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                crate::__private::DatabaseConnection::Sqlite(connection)
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
    #[must_use]
    pub fn into_connection(self) -> Option<crate::__private::DatabaseConnection<'executor>> {
        match self.inner {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                Some(crate::__private::DatabaseConnection::Postgres(connection))
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => None,
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                Some(crate::__private::DatabaseConnection::Sqlite(connection))
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
    #[allow(
        dead_code,
        reason = "transactional operations reject pools without reading them"
    )]
    PostgresPool(&'executor PgPool),
    #[cfg(feature = "sqlite")]
    SqliteConnection(&'executor mut SqliteConnection),
    #[cfg(feature = "sqlite")]
    #[allow(
        dead_code,
        reason = "transactional operations reject pools without reading them"
    )]
    SqlitePool(&'executor SqlitePool),
}
