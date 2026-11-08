//! Pooled MySQL connections that are discarded when an operation is cancelled.

use std::ops::{Deref, DerefMut};

use sqlx::{
    MySql, MySqlConnection, MySqlPool, mysql::MySqlTransactionManager, pool::PoolConnection,
};
use sqlx_core::transaction::TransactionManager;

/// Returns a connection to the pool only after its operation finishes.
///
/// SQLx's MySQL protocol recovery can hang if a future is dropped while a
/// statement is being prepared. Returning that connection to the pool then
/// leaves its release-time ping waiting forever, including during pool close.
/// Closing the socket on cancellation also rolls back any open transaction.
#[derive(Debug)]
pub struct Connection(Option<PoolConnection<MySql>>);

impl Connection {
    /// Acquires a connection that is discarded unless explicitly released.
    ///
    /// # Errors
    /// Returns the pool's connection acquisition error.
    pub async fn acquire(pool: &MySqlPool) -> Result<Self, sqlx::Error> {
        Ok(Self(Some(pool.acquire().await?)))
    }

    /// Returns a connection whose operation completed to the caller's pool.
    pub fn release(mut self) {
        drop(self.0.take());
    }
}

impl Deref for Connection {
    type Target = MySqlConnection;

    fn deref(&self) -> &Self::Target {
        self.0.as_deref().expect("connection has not been released")
    }
}

impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
            .as_deref_mut()
            .expect("connection has not been released")
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // A graceful close or the pool's release-time ping could wait on the
        // abandoned statement. Detaching and dropping closes the socket instead.
        drop(self.0.take().map(PoolConnection::detach));
    }
}

/// A River-owned transaction that discards its connection unless committed.
#[derive(Debug)]
pub struct Transaction(Connection);

impl Transaction {
    /// Begins a transaction whose connection is discarded on cancellation.
    ///
    /// # Errors
    /// Returns an acquisition or transaction start error.
    pub async fn begin(pool: &MySqlPool) -> Result<Self, sqlx::Error> {
        let mut connection = Connection::acquire(pool).await?;
        // Use SQLx's transaction manager so extension savepoints see the
        // correct transaction depth, just as with a SQLx Transaction.
        MySqlTransactionManager::begin(
            &mut connection,
            Some(sqlx::SqlStr::from_static(
                "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN",
            )),
        )
        .await?;
        Ok(Self(connection))
    }

    /// Rolls back a completed operation and returns its connection to the pool.
    ///
    /// # Errors
    /// Returns a database rollback error.
    pub async fn rollback(mut self) -> Result<(), sqlx::Error> {
        MySqlTransactionManager::rollback(&mut self.0).await?;
        self.0.release();
        Ok(())
    }

    /// Commits a completed operation and returns its connection to the pool.
    ///
    /// # Errors
    /// Returns a database commit error.
    pub async fn commit(mut self) -> Result<(), sqlx::Error> {
        MySqlTransactionManager::commit(&mut self.0).await?;
        self.0.release();
        Ok(())
    }
}

impl Deref for Transaction {
    type Target = MySqlConnection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Transaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
