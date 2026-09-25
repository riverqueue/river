//! Public database CRUD operations.

#[cfg(feature = "sqlite")]
use chrono::Utc;
use serde_json::{Map, Value};
#[cfg(feature = "postgres")]
use sqlx::{AssertSqlSafe, Executor, FromRow, PgConnection, Postgres, types::Json};

use crate::__private::DatabaseConnection;
use crate::client::after_jobs_set_state;
#[cfg(feature = "sqlite")]
use crate::database::sqlite;
use crate::{
    Client, Error, JobDeleteManyParams, JobListParams, JobRow, JobState, JobUpdateParams, Queue,
    QueueListParams,
    client::ClientInner,
    database::{DatabaseTransactionExecutor, ExecutorInner},
};
#[cfg(feature = "postgres")]
use crate::{
    JobListOrderBy, SortDirection,
    client::{JobRecord, job_projection},
};

/// Queue name that addresses every persisted queue in pause and resume.
const QUEUE_ALL: &str = "*";

/// Which job operation an extension post-hook follows.
#[derive(Clone, Copy, Debug)]
pub(crate) enum JobUpdate {
    Cancel,
    Retry,
}

/// Runs the extension's cancel or retry post-hook in the operation's
/// transaction when it intercepts those operations. An error rolls back the
/// caller's transaction along with the update.
pub(crate) async fn after_job_cancel_or_retry(
    inner: &ClientInner,
    connection: DatabaseConnection<'_>,
    row: &JobRow,
    update: JobUpdate,
) -> Result<(), Error> {
    if !inner.pilot.intercepts_job_cancel_retry() {
        return Ok(());
    }
    let params = crate::__private::JobUpdatedParams {
        database: inner.pilot_database_config(),
        job: row.clone(),
    };
    let (phase, result) = match update {
        JobUpdate::Cancel => (
            "job cancel",
            inner.pilot.after_job_cancel(connection, &params).await,
        ),
        JobUpdate::Retry => (
            "job retry",
            inner.pilot.after_job_retry(connection, &params).await,
        ),
    };
    result.map_err(|source| Error::Extension { phase, source })
}

impl Client {
    /// Completes a running job inside a caller-managed transaction. If this is
    /// called from its worker, the normal completer observes that the row is no
    /// longer running and leaves the transactional result unchanged.
    pub async fn job_complete_tx<'executor, E>(&self, executor: E, id: i64) -> Result<JobRow, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        self.job_complete_tx_with_metadata(executor, id, Map::new())
            .await
    }

    pub(crate) async fn job_complete_tx_with_metadata<'executor, E>(
        &self,
        executor: E,
        id: i64,
        metadata_updates: Map<String, Value>,
    ) -> Result<JobRow, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        match transaction_executor(self, executor, "job completion")? {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                self.job_complete_postgres(connection, id, metadata_updates)
                    .await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                self.job_complete_sqlite(connection, id, &metadata_updates)
                    .await
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
        }
    }

    #[cfg(feature = "postgres")]
    async fn job_complete_postgres(
        &self,
        connection: &mut PgConnection,
        id: i64,
        metadata_updates: Map<String, Value>,
    ) -> Result<JobRow, Error> {
        let table = self.inner.schema.qualify("river_job");
        let state: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT state::text FROM {table} WHERE id = $1 FOR UPDATE"
        )))
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?;
        match state.as_deref() {
            None => return Err(Error::NotFound),
            Some("running") => {}
            Some(state) => {
                return Err(Error::invalid_job_context(
                    "storage parameters",
                    format!("job must be running for transactional completion; state is {state}"),
                ));
            }
        }
        let sql = format!(
            "UPDATE {table} AS job SET state = 'completed', finalized_at = now(), \
             metadata = metadata || $2::jsonb \
             WHERE id = $1 AND state = 'running' \
             RETURNING {}, false AS unique_skipped_as_duplicate",
            job_projection("job")
        );
        let row = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(id)
            .bind(Json(metadata_updates))
            .fetch_optional(&mut *connection)
            .await?
            .ok_or(Error::NotFound)?
            .into_job_row()?;
        if self.inner.pilot.intercepts_job_set_state() {
            after_jobs_set_state(
                &self.inner,
                DatabaseConnection::Postgres(&mut *connection),
                std::slice::from_ref(&row),
            )
            .await?;
        }
        Ok(row)
    }

    #[cfg(feature = "sqlite")]
    async fn job_complete_sqlite(
        &self,
        connection: &mut sqlx::SqliteConnection,
        id: i64,
        metadata_updates: &Map<String, Value>,
    ) -> Result<JobRow, Error> {
        let Some(job) = sqlite::get(connection, id).await.map_err(database_error)? else {
            return Err(Error::NotFound);
        };
        if job.state != JobState::Running {
            return Err(Error::invalid_job_context(
                "storage parameters",
                format!(
                    "job must be running for transactional completion; state is {}",
                    job.state.as_str()
                ),
            ));
        }
        let now = Utc::now();
        let row = sqlite::complete(
            connection,
            &sqlite::CompleteJob {
                attempt: None,
                error: None,
                finalized_at: Some(now),
                id,
                metadata_updates: Some(metadata_updates),
                now,
                scheduled_at: None,
                state: JobState::Completed,
            },
        )
        .await
        .map_err(database_error)?
        .ok_or(Error::NotFound)?;
        if self.inner.pilot.intercepts_job_set_state() {
            after_jobs_set_state(
                &self.inner,
                DatabaseConnection::Sqlite(connection),
                std::slice::from_ref(&row),
            )
            .await?;
        }
        Ok(row)
    }

    /// Deletes a non-running job and returns its former row.
    pub async fn job_delete(&self, id: i64) -> Result<JobRow, Error> {
        #[cfg(feature = "postgres")]
        if let Some(pool) = self.inner.postgres_pool() {
            let mut transaction = crate::database::begin_postgres(pool).await?;
            let row = job_delete_postgres(self, &mut transaction, id).await?;
            transaction.commit().await?;
            return Ok(row);
        }
        #[cfg(feature = "sqlite")]
        {
            let pool = sqlite_pool(self)?;
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let row = job_delete_sqlite(&mut transaction, id).await?;
            transaction.commit().await?;
            return Ok(row);
        }
        #[allow(unreachable_code)]
        Err(Error::runtime_context(
            "storage operation",
            "database dispatch selected no supported backend".to_owned(),
        ))
    }

    /// Deletes a non-running job inside a caller-managed transaction.
    pub async fn job_delete_tx<'executor, E>(&self, executor: E, id: i64) -> Result<JobRow, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        match transaction_executor(self, executor, "job deletion")? {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                job_delete_postgres(self, connection, id).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => job_delete_sqlite(connection, id).await,
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
        }
    }

    /// Deletes matching non-running jobs with an explicit safety guard.
    pub async fn job_delete_many(
        &self,
        params: &JobDeleteManyParams,
    ) -> Result<Vec<JobRow>, Error> {
        #[cfg(feature = "postgres")]
        if let Some(pool) = self.inner.postgres_pool() {
            let mut transaction = crate::database::begin_postgres(pool).await?;
            let rows = self.job_delete_many_tx(&mut transaction, params).await?;
            transaction.commit().await?;
            return Ok(rows);
        }
        #[cfg(feature = "sqlite")]
        {
            let pool = sqlite_pool(self)?;
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let rows = self.job_delete_many_tx(&mut transaction, params).await?;
            transaction.commit().await?;
            return Ok(rows);
        }
        #[allow(unreachable_code)]
        Err(Error::runtime_context(
            "storage operation",
            "database dispatch selected no supported backend".to_owned(),
        ))
    }

    /// Deletes matching non-running jobs inside a caller-managed transaction.
    pub async fn job_delete_many_tx<'executor, E>(
        &self,
        executor: E,
        params: &JobDeleteManyParams,
    ) -> Result<Vec<JobRow>, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        if !params.all && !params.filter.has_filter() {
            return Err(Error::invalid_job_context(
                "storage parameters",
                "bulk delete requires a filter or all=true".to_owned(),
            ));
        }
        match transaction_executor(self, executor, "bulk job deletion")? {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                job_delete_many_postgres(self, connection, params).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                job_delete_many_sqlite(connection, params).await
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
        }
    }

    /// Gets one job inside a caller-managed transaction.
    pub async fn job_get_tx<'executor, E>(&self, executor: E, id: i64) -> Result<JobRow, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        match transaction_executor(self, executor, "job lookup")? {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                job_get_postgres(self, connection, id).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => sqlite::get(connection, id)
                .await
                .map_err(database_error)?
                .ok_or(Error::NotFound),
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
        }
    }

    /// Lists jobs matching the supplied filters, ordering, and cursor.
    pub async fn job_list(&self, params: &JobListParams) -> Result<Vec<JobRow>, Error> {
        #[cfg(feature = "postgres")]
        if let Some(pool) = self.inner.postgres_pool() {
            return job_list_postgres(self, pool, params).await;
        }
        #[cfg(feature = "sqlite")]
        {
            let mut connection = sqlite_pool(self)?.acquire().await?;
            return job_list_sqlite(&mut connection, params, false).await;
        }
        #[allow(unreachable_code)]
        Err(Error::runtime_context(
            "storage operation",
            "database dispatch selected no supported backend".to_owned(),
        ))
    }

    /// Lists jobs inside a caller-managed transaction.
    pub async fn job_list_tx<'executor, E>(
        &self,
        executor: E,
        params: &JobListParams,
    ) -> Result<Vec<JobRow>, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        match transaction_executor(self, executor, "job listing")? {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                job_list_postgres(self, connection, params).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                job_list_sqlite(connection, params, false).await
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
        }
    }

    /// Makes a non-running job immediately available for another attempt.
    pub async fn job_retry(&self, id: i64) -> Result<JobRow, Error> {
        #[cfg(feature = "postgres")]
        if let Some(pool) = self.inner.postgres_pool() {
            let mut transaction = crate::database::begin_postgres(pool).await?;
            let row = job_retry_postgres(self, &mut transaction, id).await?;
            transaction.commit().await?;
            return Ok(row);
        }
        #[cfg(feature = "sqlite")]
        {
            let pool = sqlite_pool(self)?;
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let row = job_retry_sqlite(self, &mut transaction, id).await?;
            transaction.commit().await?;
            return Ok(row);
        }
        #[allow(unreachable_code)]
        Err(Error::runtime_context(
            "storage operation",
            "database dispatch selected no supported backend".to_owned(),
        ))
    }

    /// Retries a job inside a caller-managed transaction.
    pub async fn job_retry_tx<'executor, E>(&self, executor: E, id: i64) -> Result<JobRow, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        match transaction_executor(self, executor, "job retry")? {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                job_retry_postgres(self, connection, id).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                job_retry_sqlite(self, connection, id).await
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
        }
    }

    /// Merges job metadata and optionally sets recorded output.
    pub async fn job_update(&self, id: i64, params: JobUpdateParams) -> Result<JobRow, Error> {
        #[cfg(feature = "postgres")]
        if let Some(pool) = self.inner.postgres_pool() {
            return job_update_postgres(self, pool, id, params).await;
        }
        #[cfg(feature = "sqlite")]
        {
            let mut connection = sqlite_pool(self)?.acquire().await?;
            return job_update_sqlite(&mut connection, id, params).await;
        }
        #[allow(unreachable_code)]
        Err(Error::runtime_context(
            "storage operation",
            "database dispatch selected no supported backend".to_owned(),
        ))
    }

    /// Updates a job inside a caller-managed transaction.
    pub async fn job_update_tx<'executor, E>(
        &self,
        executor: E,
        id: i64,
        params: JobUpdateParams,
    ) -> Result<JobRow, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        match transaction_executor(self, executor, "job update")? {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                job_update_postgres(self, connection, id, params).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                job_update_sqlite(connection, id, params).await
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
        }
    }

    /// Gets one active queue record.
    pub async fn queue_get(&self, name: &str) -> Result<Queue, Error> {
        #[cfg(feature = "postgres")]
        if let Some(pool) = self.inner.postgres_pool() {
            return queue_get_postgres(self, pool, name).await;
        }
        #[cfg(feature = "sqlite")]
        {
            let mut connection = sqlite_pool(self)?.acquire().await?;
            return sqlite::queue_get(&mut connection, name)
                .await
                .map_err(database_error)?
                .ok_or(Error::NotFound);
        }
        #[allow(unreachable_code)]
        Err(Error::runtime_context(
            "storage operation",
            "database dispatch selected no supported backend".to_owned(),
        ))
    }

    /// Gets one queue inside a caller-managed transaction.
    pub async fn queue_get_tx<'executor, E>(&self, executor: E, name: &str) -> Result<Queue, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        match transaction_executor(self, executor, "queue lookup")? {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                queue_get_postgres(self, connection, name).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => sqlite::queue_get(connection, name)
                .await
                .map_err(database_error)?
                .ok_or(Error::NotFound),
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
        }
    }

    /// Lists active queues by name.
    pub async fn queue_list(&self, params: &QueueListParams) -> Result<Vec<Queue>, Error> {
        #[cfg(feature = "postgres")]
        if let Some(pool) = self.inner.postgres_pool() {
            return queue_list_postgres(self, pool, params).await;
        }
        #[cfg(feature = "sqlite")]
        {
            validate_queue_list(params)?;
            let mut connection = sqlite_pool(self)?.acquire().await?;
            return sqlite::queue_list(&mut connection, params.limit)
                .await
                .map_err(database_error);
        }
        #[allow(unreachable_code)]
        Err(Error::runtime_context(
            "storage operation",
            "database dispatch selected no supported backend".to_owned(),
        ))
    }

    /// Lists queues inside a caller-managed transaction.
    pub async fn queue_list_tx<'executor, E>(
        &self,
        executor: E,
        params: &QueueListParams,
    ) -> Result<Vec<Queue>, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        validate_queue_list(params)?;
        match transaction_executor(self, executor, "queue listing")? {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                queue_list_postgres(self, connection, params).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                sqlite::queue_list(connection, params.limit)
                    .await
                    .map_err(database_error)
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
        }
    }

    /// Pauses one queue, or every known queue when passed `"*"`.
    ///
    /// Returns [`Error::NotFound`] when a named queue has no persisted record.
    pub async fn queue_pause(&self, name: &str) -> Result<(), Error> {
        #[cfg(feature = "postgres")]
        if let Some(pool) = self.inner.postgres_pool() {
            let mut transaction = crate::database::begin_postgres(pool).await?;
            queue_set_paused_postgres(self, &mut transaction, name, true).await?;
            transaction.commit().await?;
        }
        #[cfg(feature = "sqlite")]
        if self.inner.sqlite_pool().is_some() {
            let pool = sqlite_pool(self)?;
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            queue_set_paused_sqlite(&mut transaction, name, true).await?;
            transaction.commit().await?;
        }
        self.signal_queue_control(name);
        Ok(())
    }

    /// Pauses queues inside a caller-managed transaction.
    pub async fn queue_pause_tx<'executor, E>(&self, executor: E, name: &str) -> Result<(), Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        queue_set_paused_transaction(self, executor, name, true).await
    }

    /// Resumes one queue, or every known queue when passed `"*"`.
    ///
    /// Returns [`Error::NotFound`] when a named queue has no persisted record.
    pub async fn queue_resume(&self, name: &str) -> Result<(), Error> {
        #[cfg(feature = "postgres")]
        if let Some(pool) = self.inner.postgres_pool() {
            let mut transaction = crate::database::begin_postgres(pool).await?;
            queue_set_paused_postgres(self, &mut transaction, name, false).await?;
            transaction.commit().await?;
        }
        #[cfg(feature = "sqlite")]
        if self.inner.sqlite_pool().is_some() {
            let pool = sqlite_pool(self)?;
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            queue_set_paused_sqlite(&mut transaction, name, false).await?;
            transaction.commit().await?;
        }
        self.signal_queue_control(name);
        Ok(())
    }

    /// Resumes queues inside a caller-managed transaction.
    pub async fn queue_resume_tx<'executor, E>(&self, executor: E, name: &str) -> Result<(), Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        queue_set_paused_transaction(self, executor, name, false).await
    }

    /// Replaces a queue's metadata object.
    pub async fn queue_update(
        &self,
        name: &str,
        metadata: Map<String, Value>,
    ) -> Result<Queue, Error> {
        #[cfg(feature = "postgres")]
        if let Some(pool) = self.inner.postgres_pool() {
            let mut transaction = crate::database::begin_postgres(pool).await?;
            let queue = queue_update_postgres(self, &mut transaction, name, metadata).await?;
            transaction.commit().await?;
            return Ok(queue);
        }
        #[cfg(feature = "sqlite")]
        {
            let pool = sqlite_pool(self)?;
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let queue = queue_update_sqlite(&mut transaction, name, &metadata).await?;
            transaction.commit().await?;
            return Ok(queue);
        }
        #[allow(unreachable_code)]
        Err(Error::runtime_context(
            "storage operation",
            "database dispatch selected no supported backend".to_owned(),
        ))
    }

    /// Updates queue metadata inside a caller-managed transaction.
    pub async fn queue_update_tx<'executor, E>(
        &self,
        executor: E,
        name: &str,
        metadata: Map<String, Value>,
    ) -> Result<Queue, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        match transaction_executor(self, executor, "queue update")? {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                queue_update_postgres(self, connection, name, metadata).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                queue_update_sqlite(connection, name, &metadata).await
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => {
                unreachable!("transaction_executor rejects pools")
            }
        }
    }
}

#[cfg(feature = "postgres")]
async fn job_delete_many_postgres(
    client: &Client,
    connection: &mut PgConnection,
    params: &JobDeleteManyParams,
) -> Result<Vec<JobRow>, Error> {
    params.filter.validate().map_err(Error::invalid_job)?;
    let table = client.inner.schema.qualify("river_job");
    let parts = job_list_sql_parts(&client.inner.schema, &params.filter, false);
    // Mirrors Go's `JobDeleteMany`: running jobs are excluded before the limit
    // applies, candidates already locked by another transaction are skipped
    // rather than waited on, and rows come back in the list order.
    let sql = format!(
        "WITH jobs_to_delete AS (\
            SELECT id FROM {table} AS job WHERE {where_sql} AND state != 'running' \
            ORDER BY {order_sql} LIMIT $11 FOR UPDATE SKIP LOCKED\
         ), deleted AS (\
            DELETE FROM {table} WHERE id IN (SELECT id FROM jobs_to_delete) RETURNING *\
         ) \
         SELECT {}, false AS unique_skipped_as_duplicate FROM deleted AS job ORDER BY {order_sql}",
        job_projection("job"),
        where_sql = parts.where_sql,
        order_sql = parts.order_sql,
    );
    let records = bind_job_list(
        sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql)),
        &params.filter,
    )
    .fetch_all(connection)
    .await?;
    records.into_iter().map(JobRecord::into_job_row).collect()
}

#[cfg(feature = "postgres")]
async fn job_delete_postgres(
    client: &Client,
    connection: &mut PgConnection,
    id: i64,
) -> Result<JobRow, Error> {
    let table = client.inner.schema.qualify("river_job");
    let state: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT state::text FROM {table} WHERE id = $1 FOR UPDATE"
    )))
    .bind(id)
    .fetch_optional(&mut *connection)
    .await?;
    match state.as_deref() {
        None => return Err(Error::NotFound),
        Some("running") => return Err(Error::JobRunning),
        Some(_) => {}
    }
    let sql = format!(
        "DELETE FROM {table} AS job WHERE id = $1 RETURNING {}, false AS unique_skipped_as_duplicate",
        job_projection("job")
    );
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(id)
        .fetch_one(connection)
        .await?
        .into_job_row()
}

#[cfg(feature = "sqlite")]
async fn job_delete_many_sqlite(
    connection: &mut sqlx::SqliteConnection,
    params: &JobDeleteManyParams,
) -> Result<Vec<JobRow>, Error> {
    // Running jobs are excluded before the limit applies, like Go. SQLite's
    // single writer makes the enclosing write transaction the lock.
    let jobs = job_list_sqlite(connection, &params.filter, true).await?;
    let mut deleted = Vec::with_capacity(jobs.len());
    for job in jobs {
        if let Some(row) = sqlite::delete(connection, job.id)
            .await
            .map_err(database_error)?
        {
            deleted.push(row);
        }
    }
    Ok(deleted)
}

#[cfg(feature = "sqlite")]
async fn job_delete_sqlite(
    connection: &mut sqlx::SqliteConnection,
    id: i64,
) -> Result<JobRow, Error> {
    if let Some(row) = sqlite::delete(connection, id)
        .await
        .map_err(database_error)?
    {
        return Ok(row);
    }
    match sqlite::get(connection, id).await.map_err(database_error)? {
        Some(job) if job.state == JobState::Running => Err(Error::JobRunning),
        None | Some(_) => Err(Error::NotFound),
    }
}

#[cfg(feature = "postgres")]
async fn job_get_postgres(
    client: &Client,
    connection: &mut PgConnection,
    id: i64,
) -> Result<JobRow, Error> {
    let table = client.inner.schema.qualify("river_job");
    let sql = format!(
        "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job WHERE id = $1 LIMIT 1",
        job_projection("job")
    );
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(id)
        .fetch_optional(connection)
        .await?
        .ok_or(Error::NotFound)?
        .into_job_row()
}

#[cfg(feature = "postgres")]
async fn job_list_postgres<'executor, E>(
    client: &Client,
    executor: E,
    params: &JobListParams,
) -> Result<Vec<JobRow>, Error>
where
    E: Executor<'executor, Database = Postgres>,
{
    params.validate().map_err(Error::invalid_job)?;
    let table = client.inner.schema.qualify("river_job");
    let parts = job_list_sql_parts(&client.inner.schema, params, true);
    let sql = format!(
        "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
         WHERE {} ORDER BY {} LIMIT $11",
        job_projection("job"),
        parts.where_sql,
        parts.order_sql,
    );
    let records = bind_job_list(sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql)), params)
        .fetch_all(executor)
        .await?;
    records.into_iter().map(JobRecord::into_job_row).collect()
}

/// SQL fragments shared by job listing and bulk deletion. Both bind the same
/// eleven positional parameters through [`bind_job_list`].
#[cfg(feature = "postgres")]
struct JobListSqlParts {
    order_sql: String,
    where_sql: String,
}

#[cfg(feature = "postgres")]
fn job_list_sql_parts(
    schema: &crate::SchemaName,
    params: &JobListParams,
    optimize_single_state: bool,
) -> JobListSqlParts {
    let sort_field = job_list_sort_field(params);
    let direction = match params.direction {
        SortDirection::Ascending => "ASC",
        SortDirection::Descending => "DESC",
    };
    let comparison = match params.direction {
        SortDirection::Ascending => ">",
        SortDirection::Descending => "<",
    };
    let cursor_predicate = if sort_field == "id" {
        format!("($10::bigint IS NULL OR id {comparison} $10)")
    } else {
        format!(
            "($9::timestamptz IS NULL OR ({sort_field} {comparison} $9 OR \
             ({sort_field} = $9 AND id {comparison} $10)))"
        )
    };
    let state_type = schema.qualify("river_job_state");
    // Like Go (upstream 35c4eab8), a single-state list without metadata
    // predicates compares state with equality so PostgreSQL can use the
    // `(state, <time>)` index ordering, and a single finalized state ordered by
    // `finalized_at` states the non-null invariant that the partial
    // finalized-time index requires. Bulk deletion keeps the generic form.
    let state_predicate =
        if optimize_single_state && params.states.len() == 1 && params.metadata.is_none() {
            let finalized = sort_field == "finalized_at"
                && matches!(
                    params.states[0],
                    JobState::Cancelled | JobState::Completed | JobState::Discarded
                );
            format!(
                "state = ($4::text[])[1]::{state_type}{}",
                if finalized {
                    " AND finalized_at IS NOT NULL"
                } else {
                    ""
                }
            )
        } else {
            format!("(cardinality($4::text[]) = 0 OR state = ANY($4::text[]::{state_type}[]))")
        };
    let where_sql = format!(
        "(cardinality($1::bigint[]) = 0 OR id = ANY($1)) \
         AND (cardinality($2::text[]) = 0 OR kind = ANY($2)) \
         AND (cardinality($3::text[]) = 0 OR queue = ANY($3)) \
         AND {state_predicate} \
         AND (cardinality($5::smallint[]) = 0 OR priority = ANY($5)) \
         AND (cardinality($6::varchar[]) = 0 OR tags @> $6::varchar[]) \
         AND (cardinality($7::varchar[]) = 0 OR tags && $7::varchar[]) \
         AND ($8::jsonb IS NULL OR metadata @> $8) \
         AND {cursor_predicate}"
    );
    let order_sql = if sort_field == "id" {
        format!("id {direction}")
    } else {
        format!("{sort_field} {direction}, id {direction}")
    };
    JobListSqlParts {
        order_sql,
        where_sql,
    }
}

#[cfg(feature = "postgres")]
fn job_list_sort_field(params: &JobListParams) -> &'static str {
    match params.order_by {
        JobListOrderBy::FinalizedAt => "finalized_at",
        JobListOrderBy::Id => "id",
        JobListOrderBy::ScheduledAt => "scheduled_at",
        JobListOrderBy::Time if params.states.is_empty() => "id",
        JobListOrderBy::Time => match params.states[0] {
            JobState::Available | JobState::Pending | JobState::Retryable | JobState::Scheduled => {
                "scheduled_at"
            }
            JobState::Running => "attempted_at",
            JobState::Cancelled | JobState::Completed | JobState::Discarded => "finalized_at",
        },
    }
}

#[cfg(feature = "postgres")]
fn bind_job_list<'query>(
    query: sqlx::query::QueryAs<'query, Postgres, JobRecord, sqlx::postgres::PgArguments>,
    params: &'query JobListParams,
) -> sqlx::query::QueryAs<'query, Postgres, JobRecord, sqlx::postgres::PgArguments> {
    let cursor_id = params
        .after
        .as_ref()
        .map(|cursor| cursor.id)
        .or(params.after_id);
    let cursor_time = params.after.as_ref().and_then(|cursor| cursor.sort_time);
    let states = params
        .states
        .iter()
        .map(|state| state.as_str().to_owned())
        .collect::<Vec<_>>();
    let metadata = params
        .metadata
        .as_ref()
        .map(|metadata| Json(Value::Object(metadata.clone())));
    query
        .bind(&params.ids)
        .bind(&params.kinds)
        .bind(&params.queues)
        .bind(states)
        .bind(&params.priorities)
        .bind(&params.tags_all)
        .bind(&params.tags_any)
        .bind(metadata)
        .bind(cursor_time)
        .bind(cursor_id)
        .bind(params.limit)
}

#[cfg(feature = "sqlite")]
async fn job_list_sqlite(
    connection: &mut sqlx::SqliteConnection,
    params: &JobListParams,
    exclude_running: bool,
) -> Result<Vec<JobRow>, Error> {
    params.validate().map_err(Error::invalid_job)?;
    let kinds = params.kinds.iter().map(String::as_str).collect::<Vec<_>>();
    let queues = params.queues.iter().map(String::as_str).collect::<Vec<_>>();
    let tags_all = params
        .tags_all
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let tags_any = params
        .tags_any
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let cursor_id = params
        .after
        .as_ref()
        .map(|cursor| cursor.id)
        .or(params.after_id);
    sqlite::list(
        connection,
        &sqlite::ListJobs {
            after_id: cursor_id,
            after_time: params.after.as_ref().and_then(|cursor| cursor.sort_time),
            direction: params.direction,
            exclude_running,
            ids: &params.ids,
            kinds: &kinds,
            limit: params.limit,
            metadata: params.metadata.as_ref(),
            order_by: params.order_by,
            priorities: &params.priorities,
            queues: &queues,
            states: &params.states,
            tags_all: &tags_all,
            tags_any: &tags_any,
        },
    )
    .await
    .map_err(database_error)
}

#[cfg(feature = "postgres")]
async fn job_retry_postgres(
    client: &Client,
    connection: &mut PgConnection,
    id: i64,
) -> Result<JobRow, Error> {
    let table = client.inner.schema.qualify("river_job");
    let sql = format!(
        "WITH locked AS (SELECT id FROM {table} WHERE id = $1 FOR UPDATE), \
         updated AS (UPDATE {table} AS job SET state = 'available', \
             max_attempts = CASE WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END, \
             finalized_at = NULL, scheduled_at = now() \
             FROM locked WHERE job.id = locked.id AND job.state != 'running' \
               AND NOT (job.state = 'available' AND job.scheduled_at < now()) RETURNING job.*), \
         notified AS (SELECT pg_notify(concat(coalesce($2::text, current_schema()), '.', $3::text), \
             json_build_object('queue', queue)::text) FROM updated WHERE state = 'available') \
         SELECT {}, false AS unique_skipped_as_duplicate FROM updated AS job LEFT JOIN notified ON true \
         UNION ALL SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
             WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM updated) LIMIT 1",
        job_projection("job"),
        job_projection("job")
    );
    let row = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(id)
        .bind(client.inner.schema.as_deref())
        .bind(crate::NOTIFICATION_TOPIC_INSERT)
        .fetch_optional(&mut *connection)
        .await?
        .ok_or(Error::NotFound)?
        .into_job_row()?;
    after_job_cancel_or_retry(
        &client.inner,
        DatabaseConnection::Postgres(connection),
        &row,
        JobUpdate::Retry,
    )
    .await?;
    Ok(row)
}

#[cfg(feature = "sqlite")]
async fn job_retry_sqlite(
    client: &Client,
    connection: &mut sqlx::SqliteConnection,
    id: i64,
) -> Result<JobRow, Error> {
    let now = Utc::now();
    let updated = sqlite::retry(connection, id, now)
        .await
        .map_err(database_error)?;
    let was_updated = updated.is_some();
    let row = match updated {
        Some(row) => row,
        None => sqlite::get(connection, id)
            .await
            .map_err(database_error)?
            .ok_or(Error::NotFound)?,
    };
    if was_updated {
        let payload = serde_json::json!({"queue": row.queue}).to_string();
        sqlite::notification_insert(
            connection,
            &[sqlite::NotificationInput {
                payload: &payload,
                topic: crate::NOTIFICATION_TOPIC_INSERT,
            }],
        )
        .await
        .map_err(database_error)?;
    }
    after_job_cancel_or_retry(
        &client.inner,
        DatabaseConnection::Sqlite(connection),
        &row,
        JobUpdate::Retry,
    )
    .await?;
    Ok(row)
}

#[cfg(feature = "postgres")]
async fn job_update_postgres<'executor, E>(
    client: &Client,
    executor: E,
    id: i64,
    params: JobUpdateParams,
) -> Result<JobRow, Error>
where
    E: Executor<'executor, Database = Postgres>,
{
    let mut metadata = params.metadata;
    if let Some(output) = params.output {
        metadata.insert(crate::METADATA_KEY_OUTPUT.to_owned(), output);
    }
    let table = client.inner.schema.qualify("river_job");
    let sql = format!(
        "UPDATE {table} AS job SET metadata = metadata || $2::jsonb WHERE id = $1 \
         RETURNING {}, false AS unique_skipped_as_duplicate",
        job_projection("job")
    );
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(id)
        .bind(Json(metadata))
        .fetch_optional(executor)
        .await?
        .ok_or(Error::NotFound)?
        .into_job_row()
}

#[cfg(feature = "sqlite")]
async fn job_update_sqlite(
    connection: &mut sqlx::SqliteConnection,
    id: i64,
    params: JobUpdateParams,
) -> Result<JobRow, Error> {
    let mut metadata = params.metadata;
    if let Some(output) = params.output {
        metadata.insert(crate::METADATA_KEY_OUTPUT.to_owned(), output);
    }
    sqlite::update(connection, id, &metadata)
        .await
        .map_err(database_error)?
        .ok_or(Error::NotFound)
}

#[derive(FromRow)]
#[cfg(feature = "postgres")]
struct QueueRecord {
    created_at: chrono::DateTime<chrono::Utc>,
    metadata: Json<Value>,
    name: String,
    paused_at: Option<chrono::DateTime<chrono::Utc>>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[cfg(feature = "postgres")]
impl QueueRecord {
    fn into_queue(self) -> Result<Queue, Error> {
        Ok(Queue {
            created_at: self.created_at,
            metadata: self.metadata.0.as_object().cloned().ok_or_else(|| {
                Error::invalid_job_context(
                    "storage parameters",
                    format!("queue {:?} metadata is not an object", self.name),
                )
            })?,
            name: self.name,
            paused_at: self.paused_at,
            updated_at: self.updated_at,
        })
    }
}

#[cfg(feature = "postgres")]
async fn queue_get_postgres<'executor, E>(
    client: &Client,
    executor: E,
    name: &str,
) -> Result<Queue, Error>
where
    E: Executor<'executor, Database = Postgres>,
{
    let table = client.inner.schema.qualify("river_queue");
    let sql = format!("SELECT * FROM {table} WHERE name = $1");
    sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql))
        .bind(name)
        .fetch_optional(executor)
        .await?
        .ok_or(Error::NotFound)?
        .into_queue()
}

#[cfg(feature = "postgres")]
async fn queue_list_postgres<'executor, E>(
    client: &Client,
    executor: E,
    params: &QueueListParams,
) -> Result<Vec<Queue>, Error>
where
    E: Executor<'executor, Database = Postgres>,
{
    validate_queue_list(params)?;
    let table = client.inner.schema.qualify("river_queue");
    let sql = format!("SELECT * FROM {table} ORDER BY name LIMIT $1");
    sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql))
        .bind(params.limit)
        .fetch_all(executor)
        .await?
        .into_iter()
        .map(QueueRecord::into_queue)
        .collect()
}

#[cfg(feature = "postgres")]
async fn queue_set_paused_postgres(
    client: &Client,
    connection: &mut PgConnection,
    name: &str,
    paused: bool,
) -> Result<(), Error> {
    let table = client.inner.schema.qualify("river_queue");
    let sql = if paused {
        format!(
            "UPDATE {table} SET paused_at = coalesce(paused_at, now()), \
             updated_at = CASE WHEN paused_at IS NULL THEN now() ELSE updated_at END \
             WHERE $1 = '*' OR name = $1"
        )
    } else {
        format!(
            "UPDATE {table} SET updated_at = CASE WHEN paused_at IS NOT NULL THEN now() ELSE updated_at END, \
             paused_at = NULL WHERE $1 = '*' OR name = $1"
        )
    };
    let result = sqlx::query(AssertSqlSafe(sql))
        .bind(name)
        .execute(&mut *connection)
        .await?;
    // Like Go, naming a queue that has no persisted record is an error, while
    // `*` succeeds even when no queues exist yet.
    if result.rows_affected() == 0 && name != QUEUE_ALL {
        return Err(Error::NotFound);
    }
    notify_control(
        client,
        connection,
        serde_json::json!({"action": if paused {"pause"} else {"resume"}, "queue": name}),
    )
    .await
}

#[cfg(feature = "sqlite")]
async fn queue_set_paused_sqlite(
    connection: &mut sqlx::SqliteConnection,
    name: &str,
    paused: bool,
) -> Result<(), Error> {
    let now = Utc::now();
    let updated = if paused {
        sqlite::queue_pause(connection, name, now)
            .await
            .map_err(database_error)?
    } else {
        sqlite::queue_resume(connection, name, now)
            .await
            .map_err(database_error)?
    };
    if updated.is_empty() && name != QUEUE_ALL {
        return Err(Error::NotFound);
    }
    let payload = serde_json::json!({
        "action": if paused {"pause"} else {"resume"},
        "queue": name,
    })
    .to_string();
    sqlite::notification_insert(
        connection,
        &[sqlite::NotificationInput {
            payload: &payload,
            topic: crate::NOTIFICATION_TOPIC_CONTROL,
        }],
    )
    .await
    .map_err(database_error)?;
    Ok(())
}

async fn queue_set_paused_transaction<'executor, E>(
    client: &Client,
    executor: E,
    name: &str,
    paused: bool,
) -> Result<(), Error>
where
    E: DatabaseTransactionExecutor<'executor>,
{
    match transaction_executor(client, executor, "queue pause or resume")? {
        #[cfg(feature = "postgres")]
        ExecutorInner::PostgresConnection(connection) => {
            queue_set_paused_postgres(client, connection, name, paused).await
        }
        #[cfg(feature = "sqlite")]
        ExecutorInner::SqliteConnection(connection) => {
            queue_set_paused_sqlite(connection, name, paused).await
        }
        #[cfg(feature = "postgres")]
        ExecutorInner::PostgresPool(_) => {
            unreachable!("transaction_executor rejects pools")
        }
        #[cfg(feature = "sqlite")]
        ExecutorInner::SqlitePool(_) => {
            unreachable!("transaction_executor rejects pools")
        }
    }
}

#[cfg(feature = "postgres")]
async fn queue_update_postgres(
    client: &Client,
    connection: &mut PgConnection,
    name: &str,
    metadata: Map<String, Value>,
) -> Result<Queue, Error> {
    let table = client.inner.schema.qualify("river_queue");
    let sql =
        format!("UPDATE {table} SET metadata = $2, updated_at = now() WHERE name = $1 RETURNING *");
    let queue = sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql))
        .bind(name)
        .bind(Json(metadata.clone()))
        .fetch_optional(&mut *connection)
        .await?
        .ok_or(Error::NotFound)?
        .into_queue()?;
    notify_control(
        client,
        connection,
        serde_json::json!({"action": "metadata_changed", "metadata": metadata, "queue": name}),
    )
    .await?;
    Ok(queue)
}

#[cfg(feature = "sqlite")]
async fn queue_update_sqlite(
    connection: &mut sqlx::SqliteConnection,
    name: &str,
    metadata: &Map<String, Value>,
) -> Result<Queue, Error> {
    let queue = sqlite::queue_update(connection, name, metadata, Utc::now())
        .await
        .map_err(database_error)?
        .ok_or(Error::NotFound)?;
    let payload = serde_json::json!({
        "action": "metadata_changed",
        "metadata": metadata,
        "queue": name,
    })
    .to_string();
    sqlite::notification_insert(
        connection,
        &[sqlite::NotificationInput {
            payload: &payload,
            topic: crate::NOTIFICATION_TOPIC_CONTROL,
        }],
    )
    .await
    .map_err(database_error)?;
    Ok(queue)
}

#[cfg(feature = "postgres")]
async fn notify_control(
    client: &Client,
    connection: &mut PgConnection,
    payload: Value,
) -> Result<(), Error> {
    sqlx::query(
        "SELECT pg_notify(concat(coalesce($1::text, current_schema()), '.', $2::text), $3)",
    )
    .bind(client.inner.schema.as_deref())
    .bind(crate::NOTIFICATION_TOPIC_CONTROL)
    .bind(payload.to_string())
    .execute(connection)
    .await?;
    Ok(())
}

pub(crate) async fn touch_queue(inner: &ClientInner, name: &str) -> Result<Queue, Error> {
    #[cfg(feature = "postgres")]
    if let Some(pool) = inner.postgres_pool() {
        let table = inner.schema.qualify("river_queue");
        let sql = format!(
            "INSERT INTO {table} (name, metadata, updated_at) VALUES ($1, '{{}}'::jsonb, now()) \
             ON CONFLICT (name) DO UPDATE SET updated_at = excluded.updated_at RETURNING *"
        );
        return sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql))
            .bind(name)
            .fetch_one(pool)
            .await?
            .into_queue();
    }
    #[cfg(feature = "sqlite")]
    {
        let mut connection = inner
            .sqlite_pool()
            .ok_or_else(|| {
                Error::configuration_context(
                    "storage backend",
                    "client has no supported database pool".to_owned(),
                )
            })?
            .acquire()
            .await?;
        return sqlite::queue_upsert(&mut connection, name, &Map::new(), None, Utc::now())
            .await
            .map_err(database_error);
    }
    #[allow(unreachable_code)]
    Err(Error::runtime_context(
        "storage operation",
        "database dispatch selected no supported backend".to_owned(),
    ))
}

pub(crate) async fn load_queue(inner: &ClientInner, name: &str) -> Result<Option<Queue>, Error> {
    #[cfg(feature = "postgres")]
    if let Some(pool) = inner.postgres_pool() {
        let table = inner.schema.qualify("river_queue");
        return sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(format!(
            "SELECT * FROM {table} WHERE name = $1"
        )))
        .bind(name)
        .fetch_optional(pool)
        .await?
        .map(QueueRecord::into_queue)
        .transpose();
    }
    #[cfg(feature = "sqlite")]
    {
        let mut connection = inner
            .sqlite_pool()
            .ok_or_else(|| {
                Error::configuration_context(
                    "storage backend",
                    "client has no supported database pool".to_owned(),
                )
            })?
            .acquire()
            .await?;
        return sqlite::queue_get(&mut connection, name)
            .await
            .map_err(database_error);
    }
    #[allow(unreachable_code)]
    Err(Error::runtime_context(
        "storage operation",
        "database dispatch selected no supported backend".to_owned(),
    ))
}

#[cfg(feature = "sqlite")]
fn database_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Database(Box::new(error))
}

#[cfg(feature = "sqlite")]
fn sqlite_pool(client: &Client) -> Result<&sqlx::SqlitePool, Error> {
    client.inner.sqlite_pool().ok_or_else(|| {
        Error::configuration_context(
            "storage backend",
            "SQLite operation used by a PostgreSQL client".to_owned(),
        )
    })
}

fn transaction_executor<'executor, E>(
    client: &Client,
    executor: E,
    operation: &str,
) -> Result<ExecutorInner<'executor>, Error>
where
    E: DatabaseTransactionExecutor<'executor>,
{
    let executor = client
        .inner
        .erase_executor(executor)
        .map_err(Error::from)?
        .into_inner();
    match executor {
        #[cfg(feature = "postgres")]
        ExecutorInner::PostgresPool(_) => Err(Error::configuration_context(
            "storage backend",
            format!(
                "{operation} requires a caller-managed transaction, not a pool or bare connection"
            ),
        )),
        #[cfg(feature = "sqlite")]
        ExecutorInner::SqlitePool(_) => Err(Error::configuration_context(
            "storage backend",
            format!(
                "{operation} requires a caller-managed transaction, not a pool or bare connection"
            ),
        )),
        #[cfg(feature = "postgres")]
        ExecutorInner::PostgresConnection(_) => Ok(executor),
        #[cfg(feature = "sqlite")]
        ExecutorInner::SqliteConnection(_) => Ok(executor),
    }
}

fn validate_queue_list(params: &QueueListParams) -> Result<(), Error> {
    if !(1..=10_000).contains(&params.limit) {
        return Err(Error::invalid_job_context(
            "storage parameters",
            "queue list limit must be between 1 and 10000".to_owned(),
        ));
    }
    Ok(())
}
