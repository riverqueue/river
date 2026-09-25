//! Deletes expired finalized jobs, idle queues, and (on SQLite) delivered
//! notifications, ports of Go's `JobCleaner`, `QueueCleaner`, and
//! `SQLiteNotificationCleaner`.

use std::time::Duration;

use chrono::{DateTime, Utc};
#[cfg(feature = "postgres")]
use sqlx::AssertSqlSafe;

#[cfg(feature = "sqlite")]
use crate::database::sqlite;
use crate::{__private::FinalizedJobDeleteParams, Error};

use super::{
    MaintenanceError, TIMEOUT_DEFAULT, batch_backoff, batch_size, maintainer::ServiceContext,
    record_batch,
};

/// Interval of the SQLite notification cleaner.
#[cfg(feature = "sqlite")]
pub(super) const NOTIFICATION_CLEANER_INTERVAL: Duration = Duration::from_mins(1);

/// Age after which SQLite notification outbox rows are deleted.
#[cfg(feature = "sqlite")]
const NOTIFICATION_RETENTION: Duration = Duration::from_mins(5);

fn horizon(
    now: DateTime<Utc>,
    retention: Option<Duration>,
) -> Result<Option<DateTime<Utc>>, Error> {
    retention
        .map(|retention| {
            chrono::Duration::from_std(retention)
                .map(|retention| now - retention)
                .map_err(|error| Error::configuration_context("maintenance", error.to_string()))
        })
        .transpose()
}

/// Deletes cancelled, completed, and discarded jobs past their retention in
/// batches. Queues named by an extension's `job_cleaner_queue_exclusions`
/// are skipped; the exclusion list is read on every pass.
pub(super) async fn clean_jobs(context: &ServiceContext) -> Result<(), MaintenanceError> {
    let maintenance = &context.inner.maintenance;
    // Like Go, skip the query entirely when every retention is indefinite.
    if maintenance.cancelled_job_retention.is_none()
        && maintenance.completed_job_retention.is_none()
        && maintenance.discarded_job_retention.is_none()
    {
        return Ok(());
    }
    let queues_excluded = context.inner.pilot.job_cleaner_queue_exclusions();
    loop {
        let now = Utc::now();
        let limit = batch_size(&context.breakers.job_cleaner);
        let mut params = FinalizedJobDeleteParams::new(limit);
        params.cancelled_before = horizon(now, maintenance.cancelled_job_retention)?;
        params.completed_before = horizon(now, maintenance.completed_job_retention)?;
        params.discarded_before = horizon(now, maintenance.discarded_job_retention)?;
        params.queues_excluded.clone_from(&queues_excluded);
        let result = clean_jobs_batch(context, &params).await;
        record_batch(&context.breakers.job_cleaner, &result);
        if i64::try_from(result?).unwrap_or(i64::MAX) < limit {
            return Ok(());
        }
        batch_backoff(&context.cancel).await?;
    }
}

async fn clean_jobs_batch(
    context: &ServiceContext,
    params: &FinalizedJobDeleteParams,
) -> Result<u64, MaintenanceError> {
    let timeout = context.inner.maintenance.job_cleaner_timeout;
    #[cfg(feature = "sqlite")]
    if let Some(pool) = context.inner.sqlite_pool() {
        let operation = async {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let count = sqlite_delete_finalized_jobs(&mut transaction, params).await?;
            transaction.commit().await?;
            Ok::<_, MaintenanceError>(count)
        };
        return super::sqlite_cancellable(&context.cancel, timeout, operation).await;
    }
    #[cfg(feature = "postgres")]
    {
        use super::postgres::{MaintenanceTransaction, cancellable};

        let pool = context
            .inner
            .postgres_pool()
            .expect("client database is PostgreSQL or SQLite");
        let mut transaction = MaintenanceTransaction::begin(pool, &context.cancel, timeout).await?;
        let backend_pid = transaction.backend_pid;
        let count = cancellable(
            pool,
            backend_pid,
            &context.cancel,
            timeout,
            postgres_delete_finalized_jobs(
                &mut transaction.transaction,
                &context.inner.schema,
                params,
            ),
        )
        .await?;
        transaction.commit(pool, &context.cancel).await?;
        return Ok(count);
    }
    #[allow(unreachable_code)]
    Ok(0)
}

/// Runs the job cleaner's deletion on PostgreSQL.
#[cfg(feature = "postgres")]
pub(crate) async fn postgres_delete_finalized_jobs(
    connection: &mut sqlx::PgConnection,
    schema: &crate::database::SchemaName,
    params: &FinalizedJobDeleteParams,
) -> Result<u64, sqlx::Error> {
    if params.limit <= 0
        || (params.cancelled_before.is_none()
            && params.completed_before.is_none()
            && params.discarded_before.is_none())
    {
        return Ok(0);
    }
    let table = schema.qualify("river_job");
    let now = Utc::now();
    let result = sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM {table} WHERE id IN (\
            SELECT id FROM {table} WHERE (\
                (state = 'cancelled' AND $1 AND finalized_at < $2) OR \
                (state = 'completed' AND $3 AND finalized_at < $4) OR \
                (state = 'discarded' AND $5 AND finalized_at < $6)\
            ) AND NOT (queue = ANY($7::text[])) \
              AND ($8::text[] IS NULL OR queue = ANY($8::text[])) \
            ORDER BY id LIMIT $9\
         )"
    )))
    .bind(params.cancelled_before.is_some())
    .bind(params.cancelled_before.unwrap_or(now))
    .bind(params.completed_before.is_some())
    .bind(params.completed_before.unwrap_or(now))
    .bind(params.discarded_before.is_some())
    .bind(params.discarded_before.unwrap_or(now))
    .bind(&params.queues_excluded)
    .bind(params.queues_included.as_deref())
    .bind(params.limit)
    .execute(connection)
    .await?;
    Ok(result.rows_affected())
}

/// Runs the job cleaner's deletion on SQLite.
#[cfg(feature = "sqlite")]
pub(crate) async fn sqlite_delete_finalized_jobs(
    connection: &mut sqlx::SqliteConnection,
    params: &FinalizedJobDeleteParams,
) -> Result<u64, sqlite::BackendError> {
    let queues_excluded = params
        .queues_excluded
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let queues_included = params
        .queues_included
        .as_ref()
        .map(|queues| queues.iter().map(String::as_str).collect::<Vec<_>>());
    sqlite::cleanup_jobs(
        connection,
        &sqlite::CleanupJobs {
            cancelled_before: params.cancelled_before,
            completed_before: params.completed_before,
            discarded_before: params.discarded_before,
            limit: i32::try_from(params.limit).unwrap_or(i32::MAX),
            queues_excluded: &queues_excluded,
            queues_included: queues_included.as_deref(),
        },
    )
    .await
}

/// Deletes queue records that no client has touched within the retention.
/// Active producers refresh `updated_at`, so their queues survive.
pub(super) async fn clean_queues(context: &ServiceContext) -> Result<(), MaintenanceError> {
    loop {
        let updated_before = Utc::now()
            - chrono::Duration::from_std(context.inner.maintenance.queue_retention)
                .map_err(|error| Error::configuration_context("maintenance", error.to_string()))?;
        let limit = batch_size(&context.breakers.queue_cleaner);
        let result = clean_queues_batch(context, updated_before, limit).await;
        record_batch(&context.breakers.queue_cleaner, &result);
        if i64::try_from(result?).unwrap_or(i64::MAX) < limit {
            return Ok(());
        }
        batch_backoff(&context.cancel).await?;
    }
}

async fn clean_queues_batch(
    context: &ServiceContext,
    updated_before: DateTime<Utc>,
    limit: i64,
) -> Result<usize, MaintenanceError> {
    #[cfg(feature = "sqlite")]
    if let Some(pool) = context.inner.sqlite_pool() {
        let operation = async {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let deleted = sqlite::queue_delete_expired(
                &mut transaction,
                updated_before,
                i32::try_from(limit).unwrap_or(i32::MAX),
            )
            .await?;
            transaction.commit().await?;
            Ok::<_, MaintenanceError>(deleted.len())
        };
        return super::sqlite_cancellable(&context.cancel, TIMEOUT_DEFAULT, operation).await;
    }
    #[cfg(feature = "postgres")]
    {
        use super::postgres::{MaintenanceTransaction, cancellable};

        let pool = context
            .inner
            .postgres_pool()
            .expect("client database is PostgreSQL or SQLite");
        let table = context.inner.schema.qualify("river_queue");
        let mut transaction =
            MaintenanceTransaction::begin(pool, &context.cancel, TIMEOUT_DEFAULT).await?;
        let backend_pid = transaction.backend_pid;
        let deleted = cancellable(
            pool,
            backend_pid,
            &context.cancel,
            TIMEOUT_DEFAULT,
            sqlx::query_scalar::<_, String>(AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE name IN (\
                    SELECT name FROM {table} WHERE updated_at < $1 ORDER BY name LIMIT $2\
                 ) RETURNING name"
            )))
            .bind(updated_before)
            .bind(limit)
            .fetch_all(&mut *transaction.transaction),
        )
        .await?;
        transaction.commit(pool, &context.cancel).await?;
        return Ok(deleted.len());
    }
    #[allow(unreachable_code)]
    Ok(0)
}

/// Deletes SQLite notification outbox rows old enough that every poller has
/// consumed them.
#[cfg(feature = "sqlite")]
pub(super) async fn clean_notifications(context: &ServiceContext) -> Result<(), MaintenanceError> {
    let pool = context
        .inner
        .sqlite_pool()
        .expect("notification cleanup is only started for SQLite");
    let retention = chrono::Duration::from_std(NOTIFICATION_RETENTION)
        .map_err(|error| Error::configuration_context("maintenance", error.to_string()))?;
    loop {
        let created_before = Utc::now() - retention;
        let limit = super::BATCH_SIZE_DEFAULT;
        let operation = async {
            let mut connection = pool.acquire().await?;
            Ok::<_, MaintenanceError>(
                sqlite::notification_cleanup(&mut connection, created_before, limit).await?,
            )
        };
        let count = super::sqlite_cancellable(&context.cancel, TIMEOUT_DEFAULT, operation).await?;
        if count < u64::try_from(limit).unwrap_or(u64::MAX) {
            return Ok(());
        }
        batch_backoff(&context.cancel).await?;
    }
}
