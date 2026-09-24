//! River's storage operations and their per-backend implementations.
//!
//! Each backend-specific statement is a method of the private [`Backend`]
//! trait, implemented once per built-in database. [`Storage`] binds a backend
//! to one connection and adds the semantics every backend shares: parameter
//! validation, not-found and state errors, extension hooks, and control
//! notifications. [`Session`] supplies that connection, either borrowed from
//! a caller-managed transaction or owned by River for one operation.
//!
//! Adding a backend means implementing [`Backend`] and adding a variant for
//! it to the connection, [`AnyBackend`], and [`Session`] enums; the
//! operations themselves don't change.

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

use serde_json::{Map, Value};
#[cfg(feature = "postgres")]
use sqlx::Postgres;
#[cfg(feature = "sqlite")]
use sqlx::Sqlite;
use sqlx::{Transaction, pool::PoolConnection};

use crate::__private::{DatabaseConnection, ExtensionClaimParams};
use crate::client::{ClientInner, after_jobs_set_state};
use crate::database::{Database, DatabasePool};
use crate::{
    Error, JobDeleteManyParams, JobListParams, JobRow, JobUpdateParams, Queue, QueueListParams,
};

/// Queue name that addresses every persisted queue in pause and resume. It is
/// part of River's cross-language storage and notification protocol, not the
/// public API.
pub(crate) const QUEUE_ALL: &str = "*";

/// One built-in backend's implementation of River's storage statements,
/// bound to a connection.
///
/// Methods run on the bound connection only; whether that connection is in a
/// transaction is the caller's concern.
pub(crate) trait Backend {
    /// Borrows the bound connection for an extension hook.
    fn connection(&mut self) -> DatabaseConnection<'_>;

    /// Cancels a job, notifying the client running it, and returns its
    /// current row. `None` means the job doesn't exist.
    async fn job_cancel(&mut self, id: i64) -> Result<Option<JobRow>, Error>;

    /// Completes a running job, merging metadata updates.
    async fn job_complete(
        &mut self,
        id: i64,
        metadata_updates: &Map<String, Value>,
    ) -> Result<JobRow, Error>;

    /// Deletes a non-running job.
    async fn job_delete(&mut self, id: i64) -> Result<JobRow, Error>;

    /// Deletes non-running jobs matching a validated filter.
    async fn job_delete_many(&mut self, filter: &JobListParams) -> Result<Vec<JobRow>, Error>;

    async fn job_get(&mut self, id: i64) -> Result<Option<JobRow>, Error>;

    /// Lists jobs matching validated parameters.
    async fn job_list(&mut self, params: &JobListParams) -> Result<Vec<JobRow>, Error>;

    /// Makes a job available again, notifying its queue, and returns its
    /// current row. `None` means the job doesn't exist.
    async fn job_retry(&mut self, id: i64) -> Result<Option<JobRow>, Error>;

    /// Merges metadata into a job. `None` means the job doesn't exist.
    async fn job_update(
        &mut self,
        id: i64,
        metadata: &Map<String, Value>,
    ) -> Result<Option<JobRow>, Error>;

    /// Claims available jobs matching an extension's filter.
    async fn jobs_claim_filtered(
        &mut self,
        client_id: &str,
        max_attempted_by: i32,
        params: &ExtensionClaimParams,
    ) -> Result<Vec<JobRow>, Error>;

    /// Sends a notification that is delivered when the connection's
    /// transaction commits.
    async fn notify(&mut self, topic: &str, payload: &str) -> Result<(), Error>;

    async fn queue_get(&mut self, name: &str) -> Result<Option<Queue>, Error>;

    async fn queue_list(&mut self, limit: i32) -> Result<Vec<Queue>, Error>;

    /// Pauses or resumes the named queue, or every queue for
    /// [`QUEUE_ALL`], returning how many queues matched.
    async fn queue_set_paused(&mut self, name: &str, paused: bool) -> Result<u64, Error>;

    /// Creates a queue record or refreshes its `updated_at`.
    async fn queue_touch(&mut self, name: &str) -> Result<Queue, Error>;

    /// Refreshes a queue's `updated_at`, replacing its metadata when given.
    /// `None` means the queue doesn't exist.
    async fn queue_update(
        &mut self,
        name: &str,
        metadata: Option<&Map<String, Value>>,
    ) -> Result<Option<Queue>, Error>;
}

/// The backend selected by a connection.
enum AnyBackend<'c> {
    #[cfg(feature = "postgres")]
    Postgres(postgres::PostgresBackend<'c>),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlite::SqliteBackend<'c>),
}

/// Runs `$call` with `$backend` bound to the concrete backend.
macro_rules! dispatch {
    ($any:expr, $backend:ident => $call:expr) => {
        match $any {
            #[cfg(feature = "postgres")]
            AnyBackend::Postgres($backend) => $call,
            #[cfg(feature = "sqlite")]
            AnyBackend::Sqlite($backend) => $call,
        }
    };
}

impl Backend for AnyBackend<'_> {
    fn connection(&mut self) -> DatabaseConnection<'_> {
        dispatch!(self, backend => backend.connection())
    }

    async fn job_cancel(&mut self, id: i64) -> Result<Option<JobRow>, Error> {
        dispatch!(self, backend => backend.job_cancel(id).await)
    }

    async fn job_complete(
        &mut self,
        id: i64,
        metadata_updates: &Map<String, Value>,
    ) -> Result<JobRow, Error> {
        dispatch!(self, backend => backend.job_complete(id, metadata_updates).await)
    }

    async fn job_delete(&mut self, id: i64) -> Result<JobRow, Error> {
        dispatch!(self, backend => backend.job_delete(id).await)
    }

    async fn job_delete_many(&mut self, filter: &JobListParams) -> Result<Vec<JobRow>, Error> {
        dispatch!(self, backend => backend.job_delete_many(filter).await)
    }

    async fn job_get(&mut self, id: i64) -> Result<Option<JobRow>, Error> {
        dispatch!(self, backend => backend.job_get(id).await)
    }

    async fn job_list(&mut self, params: &JobListParams) -> Result<Vec<JobRow>, Error> {
        dispatch!(self, backend => backend.job_list(params).await)
    }

    async fn job_retry(&mut self, id: i64) -> Result<Option<JobRow>, Error> {
        dispatch!(self, backend => backend.job_retry(id).await)
    }

    async fn job_update(
        &mut self,
        id: i64,
        metadata: &Map<String, Value>,
    ) -> Result<Option<JobRow>, Error> {
        dispatch!(self, backend => backend.job_update(id, metadata).await)
    }

    async fn jobs_claim_filtered(
        &mut self,
        client_id: &str,
        max_attempted_by: i32,
        params: &ExtensionClaimParams,
    ) -> Result<Vec<JobRow>, Error> {
        dispatch!(self, backend => {
            backend
                .jobs_claim_filtered(client_id, max_attempted_by, params)
                .await
        })
    }

    async fn notify(&mut self, topic: &str, payload: &str) -> Result<(), Error> {
        dispatch!(self, backend => backend.notify(topic, payload).await)
    }

    async fn queue_get(&mut self, name: &str) -> Result<Option<Queue>, Error> {
        dispatch!(self, backend => backend.queue_get(name).await)
    }

    async fn queue_list(&mut self, limit: i32) -> Result<Vec<Queue>, Error> {
        dispatch!(self, backend => backend.queue_list(limit).await)
    }

    async fn queue_set_paused(&mut self, name: &str, paused: bool) -> Result<u64, Error> {
        dispatch!(self, backend => backend.queue_set_paused(name, paused).await)
    }

    async fn queue_touch(&mut self, name: &str) -> Result<Queue, Error> {
        dispatch!(self, backend => backend.queue_touch(name).await)
    }

    async fn queue_update(
        &mut self,
        name: &str,
        metadata: Option<&Map<String, Value>>,
    ) -> Result<Option<Queue>, Error> {
        dispatch!(self, backend => backend.queue_update(name, metadata).await)
    }
}

/// River's storage operations on one connection.
pub(crate) struct Storage<'c> {
    backend: AnyBackend<'c>,
    inner: &'c ClientInner,
}

impl<'c> Storage<'c> {
    pub(crate) fn new(inner: &'c ClientInner, connection: DatabaseConnection<'c>) -> Self {
        let backend = match connection {
            #[cfg(feature = "postgres")]
            DatabaseConnection::Postgres(connection) => {
                AnyBackend::Postgres(postgres::PostgresBackend {
                    connection,
                    schema: &inner.schema,
                })
            }
            #[cfg(feature = "sqlite")]
            DatabaseConnection::Sqlite(connection) => {
                AnyBackend::Sqlite(sqlite::SqliteBackend { connection })
            }
        };
        Self { backend, inner }
    }

    /// Cancels a job and returns its current row. The running client is
    /// notified when the transaction commits.
    pub(crate) async fn job_cancel(&mut self, id: i64) -> Result<JobRow, Error> {
        let row = self.backend.job_cancel(id).await?.ok_or(Error::NotFound)?;
        self.after_job_update(&row, JobUpdate::Cancel).await?;
        Ok(row)
    }

    /// Completes a running job, merging metadata updates, and runs the
    /// extension's set-state hook in the same transaction.
    pub(crate) async fn job_complete(
        &mut self,
        id: i64,
        metadata_updates: &Map<String, Value>,
    ) -> Result<JobRow, Error> {
        let row = self.backend.job_complete(id, metadata_updates).await?;
        if self.inner.pilot.intercepts_job_set_state() {
            after_jobs_set_state(
                self.inner,
                self.backend.connection(),
                &[row.id],
                std::slice::from_ref(&row),
            )
            .await?;
        }
        Ok(row)
    }

    /// Deletes a non-running job and returns its former row.
    pub(crate) async fn job_delete(&mut self, id: i64) -> Result<JobRow, Error> {
        self.backend.job_delete(id).await
    }

    /// Deletes matching non-running jobs with an explicit safety guard.
    pub(crate) async fn job_delete_many(
        &mut self,
        params: &JobDeleteManyParams,
    ) -> Result<Vec<JobRow>, Error> {
        if !params.all && !params.filter.has_filter() {
            return Err(Error::invalid_job_context(
                "storage parameters",
                "bulk delete requires a filter or all=true".to_owned(),
            ));
        }
        params.filter.validate().map_err(Error::invalid_job)?;
        self.backend.job_delete_many(&params.filter).await
    }

    pub(crate) async fn job_get(&mut self, id: i64) -> Result<JobRow, Error> {
        self.backend.job_get(id).await?.ok_or(Error::NotFound)
    }

    pub(crate) async fn job_list(&mut self, params: &JobListParams) -> Result<Vec<JobRow>, Error> {
        params.validate().map_err(Error::invalid_job)?;
        self.backend.job_list(params).await
    }

    /// Makes a non-running job available again and returns its current row.
    pub(crate) async fn job_retry(&mut self, id: i64) -> Result<JobRow, Error> {
        let row = self.backend.job_retry(id).await?.ok_or(Error::NotFound)?;
        self.after_job_update(&row, JobUpdate::Retry).await?;
        Ok(row)
    }

    /// Merges job metadata and optionally sets recorded output.
    pub(crate) async fn job_update(
        &mut self,
        id: i64,
        params: JobUpdateParams,
    ) -> Result<JobRow, Error> {
        let mut metadata = params.metadata;
        if let Some(output) = params.output {
            metadata.insert(crate::METADATA_KEY_OUTPUT.to_owned(), output);
        }
        self.backend
            .job_update(id, &metadata)
            .await?
            .ok_or(Error::NotFound)
    }

    /// Claims available jobs matching an extension's filter for this client.
    pub(crate) async fn jobs_claim_filtered(
        &mut self,
        max_attempted_by: i32,
        params: &ExtensionClaimParams,
    ) -> Result<Vec<JobRow>, Error> {
        self.backend
            .jobs_claim_filtered(&self.inner.id, max_attempted_by, params)
            .await
    }

    /// Asks the current leader to resign once the transaction commits.
    pub(crate) async fn leader_request_resign(&mut self) -> Result<(), Error> {
        self.backend
            .notify(
                crate::NOTIFICATION_TOPIC_LEADERSHIP,
                r#"{"action":"request_resign"}"#,
            )
            .await
    }

    pub(crate) async fn queue_get(&mut self, name: &str) -> Result<Option<Queue>, Error> {
        self.backend.queue_get(name).await
    }

    pub(crate) async fn queue_list(
        &mut self,
        params: &QueueListParams,
    ) -> Result<Vec<Queue>, Error> {
        if !(1..=10_000).contains(&params.limit) {
            return Err(Error::invalid_job_context(
                "storage parameters",
                "queue list limit must be between 1 and 10000".to_owned(),
            ));
        }
        self.backend.queue_list(params.limit).await
    }

    /// Pauses or resumes one queue, or every queue for [`QUEUE_ALL`], and
    /// notifies clients when the transaction commits.
    ///
    /// Like Go, naming a queue that has no persisted record is an error,
    /// while [`QUEUE_ALL`] succeeds even when no queues exist yet.
    pub(crate) async fn queue_set_paused(&mut self, name: &str, paused: bool) -> Result<(), Error> {
        let updated = self.backend.queue_set_paused(name, paused).await?;
        if updated == 0 && name != QUEUE_ALL {
            return Err(Error::NotFound);
        }
        let payload = serde_json::json!({
            "action": if paused { "pause" } else { "resume" },
            "queue": name,
        });
        self.backend
            .notify(crate::NOTIFICATION_TOPIC_CONTROL, &payload.to_string())
            .await
    }

    pub(crate) async fn queue_touch(&mut self, name: &str) -> Result<Queue, Error> {
        self.backend.queue_touch(name).await
    }

    /// Updates a queue, refreshing its `updated_at`, and notifies clients of
    /// new metadata when the transaction commits.
    pub(crate) async fn queue_update(
        &mut self,
        name: &str,
        metadata: Option<&Map<String, Value>>,
    ) -> Result<Queue, Error> {
        let queue = self
            .backend
            .queue_update(name, metadata)
            .await?
            .ok_or(Error::NotFound)?;
        // Like Go, only a metadata change notifies clients.
        if let Some(metadata) = metadata {
            let payload = serde_json::json!({
                "action": "metadata_changed",
                "metadata": metadata,
                "queue": name,
            });
            self.backend
                .notify(crate::NOTIFICATION_TOPIC_CONTROL, &payload.to_string())
                .await?;
        }
        Ok(queue)
    }

    /// Runs the extension's cancel or retry post-hook in the operation's
    /// transaction when it intercepts those operations. An error rolls back
    /// the caller's transaction along with the update.
    async fn after_job_update(&mut self, row: &JobRow, update: JobUpdate) -> Result<(), Error> {
        let pilot = &self.inner.pilot;
        if !pilot.intercepts_job_cancel_retry() {
            return Ok(());
        }
        let params = crate::__private::JobUpdatedParams {
            database: self.inner.pilot_database_config(),
            job: row.clone(),
        };
        let connection = self.backend.connection();
        let (phase, result) = match update {
            JobUpdate::Cancel => (
                "job cancel",
                pilot.after_job_cancel(connection, &params).await,
            ),
            JobUpdate::Retry => (
                "job retry",
                pilot.after_job_retry(connection, &params).await,
            ),
        };
        result.map_err(|source| Error::Extension { phase, source })
    }
}

/// Which job operation an extension post-hook follows.
#[derive(Clone, Copy, Debug)]
enum JobUpdate {
    Cancel,
    Retry,
}

/// Error for transactional completion of a job that isn't running.
fn job_not_running(state: &str) -> Error {
    Error::invalid_job_context(
        "storage parameters",
        format!("job must be running for transactional completion; state is {state}"),
    )
}

/// How an operation River runs on its own pool uses its connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Access {
    /// A pooled connection in autocommit mode, for operations of a single
    /// statement.
    Autocommit,
    /// A transaction River commits, for operations of several statements or
    /// with notifications. SQLite transactions take the write lock up front.
    Transaction,
}

/// The connection one storage operation runs on.
pub(crate) struct Session<'a> {
    connection: SessionConnection<'a>,
}

enum SessionConnection<'a> {
    /// A caller-managed transaction, which River never commits.
    Caller(DatabaseConnection<'a>),
    #[cfg(feature = "postgres")]
    PostgresConnection(PoolConnection<Postgres>),
    #[cfg(feature = "postgres")]
    PostgresTransaction(Transaction<'static, Postgres>),
    #[cfg(feature = "sqlite")]
    SqliteConnection(PoolConnection<Sqlite>),
    #[cfg(feature = "sqlite")]
    SqliteTransaction(Transaction<'static, Sqlite>),
}

impl<'a> Session<'a> {
    /// Runs on a caller-managed transaction.
    pub(crate) const fn caller(connection: DatabaseConnection<'a>) -> Self {
        Self {
            connection: SessionConnection::Caller(connection),
        }
    }

    /// Acquires a connection from the client's own pool.
    pub(crate) async fn begin(database: &Database, access: Access) -> Result<Self, Error> {
        let connection = match (database.pool(), access) {
            #[cfg(feature = "postgres")]
            (DatabasePool::Postgres(pool), Access::Autocommit) => {
                SessionConnection::PostgresConnection(pool.acquire().await?)
            }
            #[cfg(feature = "postgres")]
            (DatabasePool::Postgres(pool), Access::Transaction) => {
                SessionConnection::PostgresTransaction(crate::database::begin_postgres(pool).await?)
            }
            #[cfg(feature = "sqlite")]
            (DatabasePool::Sqlite(pool), Access::Autocommit) => {
                SessionConnection::SqliteConnection(pool.acquire().await?)
            }
            #[cfg(feature = "sqlite")]
            (DatabasePool::Sqlite(pool), Access::Transaction) => {
                SessionConnection::SqliteTransaction(
                    crate::database::begin_sqlite_write(pool).await?,
                )
            }
        };
        Ok(Self { connection })
    }

    /// Returns storage operations bound to this session's connection.
    pub(crate) fn storage<'s>(&'s mut self, inner: &'s ClientInner) -> Storage<'s> {
        let connection = match &mut self.connection {
            SessionConnection::Caller(connection) => connection.reborrow(),
            #[cfg(feature = "postgres")]
            SessionConnection::PostgresConnection(connection) => {
                DatabaseConnection::Postgres(connection)
            }
            #[cfg(feature = "postgres")]
            SessionConnection::PostgresTransaction(transaction) => {
                DatabaseConnection::Postgres(transaction)
            }
            #[cfg(feature = "sqlite")]
            SessionConnection::SqliteConnection(connection) => {
                DatabaseConnection::Sqlite(connection)
            }
            #[cfg(feature = "sqlite")]
            SessionConnection::SqliteTransaction(transaction) => {
                DatabaseConnection::Sqlite(transaction)
            }
        };
        Storage::new(inner, connection)
    }

    /// Commits a transaction River owns. A caller-managed transaction is
    /// left for the caller to commit, and an autocommit connection has
    /// nothing to commit. Dropping a session instead rolls River's own
    /// transaction back.
    pub(crate) async fn commit(self) -> Result<(), Error> {
        match self.connection {
            #[cfg(feature = "postgres")]
            SessionConnection::PostgresTransaction(transaction) => transaction.commit().await?,
            #[cfg(feature = "sqlite")]
            SessionConnection::SqliteTransaction(transaction) => transaction.commit().await?,
            SessionConnection::Caller(_) => {}
            #[cfg(feature = "postgres")]
            SessionConnection::PostgresConnection(_) => {}
            #[cfg(feature = "sqlite")]
            SessionConnection::SqliteConnection(_) => {}
        }
        Ok(())
    }
}

/// Creates the client's queue record or refreshes its `updated_at`.
pub(crate) async fn touch_queue(inner: &ClientInner, name: &str) -> Result<Queue, Error> {
    let mut session = Session::begin(&inner.database, Access::Autocommit).await?;
    session.storage(inner).queue_touch(name).await
}

/// Loads a queue record, if one exists.
pub(crate) async fn load_queue(inner: &ClientInner, name: &str) -> Result<Option<Queue>, Error> {
    let mut session = Session::begin(&inner.database, Access::Autocommit).await?;
    session.storage(inner).queue_get(name).await
}

/// Transactional job operations for a [`WorkContext`](crate::WorkContext),
/// whose generic executor can't be reborrowed into a request builder.
impl crate::Client {
    /// Completes a running job in a caller-managed transaction, merging
    /// metadata recorded on the work context.
    pub(crate) async fn job_complete_tx_with_metadata<'executor, E>(
        &self,
        executor: E,
        id: i64,
        metadata_updates: Map<String, Value>,
    ) -> Result<JobRow, Error>
    where
        E: crate::database::DatabaseTransactionExecutor<'executor>,
    {
        let connection = self.inner.transaction_connection(executor)?;
        Storage::new(&self.inner, connection)
            .job_complete(id, &metadata_updates)
            .await
    }

    /// Updates a job in a caller-managed transaction.
    pub(crate) async fn job_update_tx<'executor, E>(
        &self,
        executor: E,
        id: i64,
        params: JobUpdateParams,
    ) -> Result<JobRow, Error>
    where
        E: crate::database::DatabaseTransactionExecutor<'executor>,
    {
        let connection = self.inner.transaction_connection(executor)?;
        Storage::new(&self.inner, connection)
            .job_update(id, params)
            .await
    }
}
