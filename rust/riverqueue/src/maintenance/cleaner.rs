//! Deletes expired finalized jobs, idle queues, and (on SQLite) delivered
//! notifications, ports of Go's `JobCleaner`, `QueueCleaner`, and
//! `SQLiteNotificationCleaner`.

use std::time::Duration;

use chrono::{DateTime, Utc};
#[cfg(feature = "postgres")]
use sqlx::AssertSqlSafe;

use crate::Error;
#[cfg(feature = "sqlite")]
use crate::database::sqlite;

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

/// Deletion horizons of one job cleaner pass. `None` keeps that state forever.
struct JobHorizons {
    cancelled: Option<DateTime<Utc>>,
    completed: Option<DateTime<Utc>>,
    discarded: Option<DateTime<Utc>>,
}

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
        let horizons = JobHorizons {
            cancelled: horizon(now, maintenance.cancelled_job_retention)?,
            completed: horizon(now, maintenance.completed_job_retention)?,
            discarded: horizon(now, maintenance.discarded_job_retention)?,
        };
        let limit = batch_size(&context.breakers.job_cleaner);
        let result = clean_jobs_batch(context, &horizons, &queues_excluded, limit).await;
        record_batch(&context.breakers.job_cleaner, &result);
        if i64::try_from(result?).unwrap_or(i64::MAX) < limit {
            return Ok(());
        }
        batch_backoff(&context.cancel).await?;
    }
}

async fn clean_jobs_batch(
    context: &ServiceContext,
    horizons: &JobHorizons,
    queues_excluded: &[String],
    limit: i64,
) -> Result<u64, MaintenanceError> {
    let timeout = context.inner.maintenance.job_cleaner_timeout;
    #[cfg(feature = "sqlite")]
    if let Some(pool) = context.inner.sqlite_pool() {
        let operation = async {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let queues_excluded = queues_excluded
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let count = sqlite::cleanup_jobs(
                &mut transaction,
                &sqlite::CleanupJobs {
                    cancelled_before: horizons.cancelled,
                    completed_before: horizons.completed,
                    discarded_before: horizons.discarded,
                    limit: i32::try_from(limit).unwrap_or(i32::MAX),
                    queues_excluded: &queues_excluded,
                },
            )
            .await?;
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
        let table = context.inner.schema.qualify("river_job");
        let mut transaction = MaintenanceTransaction::begin(pool, &context.cancel, timeout).await?;
        let backend_pid = transaction.backend_pid;
        let now = Utc::now();
        let result = cancellable(
            pool,
            backend_pid,
            &context.cancel,
            timeout,
            sqlx::query(AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE id IN (\
                    SELECT id FROM {table} WHERE (\
                        (state = 'cancelled' AND $1 AND finalized_at < $2) OR \
                        (state = 'completed' AND $3 AND finalized_at < $4) OR \
                        (state = 'discarded' AND $5 AND finalized_at < $6)\
                    ) AND NOT (queue = ANY($7::text[])) \
                    ORDER BY id LIMIT $8\
                 )"
            )))
            .bind(horizons.cancelled.is_some())
            .bind(horizons.cancelled.unwrap_or(now))
            .bind(horizons.completed.is_some())
            .bind(horizons.completed.unwrap_or(now))
            .bind(horizons.discarded.is_some())
            .bind(horizons.discarded.unwrap_or(now))
            .bind(queues_excluded)
            .bind(limit)
            .execute(&mut *transaction.transaction),
        )
        .await?;
        transaction.commit(pool, &context.cancel).await?;
        return Ok(result.rows_affected());
    }
    #[allow(unreachable_code)]
    Ok(0)
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
