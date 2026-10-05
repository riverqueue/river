//! Moves due `scheduled` and `retryable` jobs to `available`, a port of Go's
//! `JobScheduler`.

use std::{collections::BTreeSet, time::Duration};

use chrono::{DateTime, Utc};
#[cfg(feature = "postgres")]
use sqlx::{AssertSqlSafe, Row};

#[cfg(feature = "sqlite")]
use crate::client::InsertNotifyLimiter;
use crate::database::DatabasePool;
#[cfg(feature = "postgres")]
use crate::database::postgres_capabilities::CapabilitiesCache;
#[cfg(feature = "sqlite")]
use crate::database::sqlite;

use super::{
    MaintenanceError, TIMEOUT_DEFAULT, batch_backoff, batch_size, maintainer::ServiceContext,
    record_batch,
};

/// Jobs due within this margin of the scheduling pass are announced to
/// producers; later look-ahead jobs are left to fetch polling (Go uses 5ms).
const NOTIFICATION_HORIZON: Duration = Duration::from_millis(5);

/// A job the scheduler transitioned.
struct Scheduled {
    queue: String,
    scheduled_at: DateTime<Utc>,
}

/// Runs scheduling batches until a batch is smaller than the batch size.
///
/// Like Go, jobs due within one scheduler interval are made available now, so
/// they can be fetched as soon as they are due instead of waiting for the next
/// pass. Only queues with jobs due by the end of the pass are notified.
pub(super) async fn run_once(context: &ServiceContext) -> Result<(), MaintenanceError> {
    loop {
        let limit = batch_size(&context.breakers.scheduler);
        let result = schedule_batch(context, limit).await;
        record_batch(&context.breakers.scheduler, &result);
        if i64::try_from(result?).unwrap_or(i64::MAX) < limit {
            return Ok(());
        }
        batch_backoff(&context.cancel).await?;
    }
}

async fn schedule_batch(context: &ServiceContext, limit: i64) -> Result<usize, MaintenanceError> {
    let now = Utc::now();
    let look_ahead = now
        + chrono::Duration::from_std(context.inner.maintenance.scheduler_interval)
            .unwrap_or_default();
    match context.inner.database.pool() {
        #[cfg(feature = "sqlite")]
        DatabasePool::Sqlite(pool) => {
            super::sqlite_cancellable(
                &context.cancel,
                TIMEOUT_DEFAULT,
                schedule_batch_sqlite(
                    pool,
                    &context.inner.insert_notify_limiter,
                    look_ahead,
                    limit,
                ),
            )
            .await
        }
        #[cfg(feature = "postgres")]
        DatabasePool::Postgres(pool) => {
            schedule_batch_postgres(context, pool, look_ahead, limit).await
        }
    }
}

fn notified_queues(scheduled: &[Scheduled]) -> BTreeSet<String> {
    let horizon = Utc::now() + chrono::Duration::from_std(NOTIFICATION_HORIZON).unwrap_or_default();
    scheduled
        .iter()
        .filter(|job| job.scheduled_at <= horizon)
        .map(|job| job.queue.clone())
        .collect()
}

#[cfg(feature = "postgres")]
async fn schedule_batch_postgres(
    context: &ServiceContext,
    pool: &sqlx::PgPool,
    look_ahead: DateTime<Utc>,
    limit: i64,
) -> Result<usize, MaintenanceError> {
    use super::postgres::{MaintenanceTransaction, cancellable};

    let inner = &context.inner;
    let table = inner.schema.qualify("river_job");
    let state_function = inner.schema.qualify("river_job_state_in_bitmask");
    let state_type = inner.schema.qualify("river_job_state");
    // Mirrors Go's `JobSchedule`, including the index-friendly predicates and
    // using the look-ahead time for both eligibility and conflict finalization.
    let sql = format!(
        "WITH jobs_to_schedule AS (\
            SELECT id, unique_key, unique_states, priority, scheduled_at FROM {table} \
            WHERE state IN ('retryable', 'scheduled') AND priority >= 0 AND queue IS NOT NULL \
              AND scheduled_at <= $2 \
            ORDER BY priority, scheduled_at, id LIMIT $1 FOR UPDATE\
         ), jobs_with_rownum AS (\
            SELECT *, CASE WHEN unique_key IS NOT NULL AND unique_states IS NOT NULL THEN \
                row_number() OVER (PARTITION BY unique_key ORDER BY priority, scheduled_at, id) END AS row_num \
            FROM jobs_to_schedule\
         ), unique_conflicts AS (\
            SELECT job.unique_key FROM {table} AS job JOIN jobs_with_rownum AS candidate \
              ON job.unique_key = candidate.unique_key AND job.id != candidate.id \
            WHERE job.unique_key IS NOT NULL AND job.unique_states IS NOT NULL \
              AND {state_function}(job.unique_states, job.state)\
         ), job_updates AS (\
            SELECT candidate.id, CASE \
                WHEN candidate.row_num IS NULL THEN 'available'::{state_type} \
                WHEN conflict.unique_key IS NOT NULL THEN 'discarded'::{state_type} \
                WHEN candidate.row_num = 1 THEN 'available'::{state_type} \
                ELSE 'discarded'::{state_type} END AS new_state \
            FROM jobs_with_rownum AS candidate LEFT JOIN unique_conflicts AS conflict \
              ON candidate.unique_key = conflict.unique_key\
         ), updated AS (\
            UPDATE {table} AS job SET state = job_updates.new_state, \
              finalized_at = CASE WHEN job_updates.new_state = 'discarded' THEN $2 ELSE job.finalized_at END, \
              metadata = CASE WHEN job_updates.new_state = 'discarded' \
                THEN job.metadata || '{{\"unique_key_conflict\": \"scheduler_discarded\"}}'::jsonb \
                ELSE job.metadata END \
            FROM job_updates WHERE job.id = job_updates.id \
            RETURNING job.queue, job.scheduled_at, job.state::text\
         ) SELECT queue, scheduled_at, state FROM updated"
    );
    let mut transaction =
        MaintenanceTransaction::begin(pool, &context.cancel, TIMEOUT_DEFAULT).await?;
    let backend_pid = transaction.backend_pid;
    let rows = cancellable(
        pool,
        backend_pid,
        &context.cancel,
        TIMEOUT_DEFAULT,
        sqlx::query(AssertSqlSafe(sql))
            .bind(limit)
            .bind(look_ahead)
            .fetch_all(&mut *transaction.transaction),
    )
    .await?;
    let count = rows.len();
    let scheduled = rows
        .iter()
        .filter(|row| row.get::<String, _>("state") == "available")
        .map(|row| Scheduled {
            queue: row.get("queue"),
            scheduled_at: row.get("scheduled_at"),
        })
        .collect::<Vec<_>>();
    let notified = notified_queues(&scheduled);
    let queues = context
        .inner
        .insert_notify_limiter
        .due(notified.iter().map(String::as_str));
    if !queues.is_empty() && delivers_notifications(context, pool, &mut transaction).await? {
        cancellable(
            pool,
            backend_pid,
            &context.cancel,
            TIMEOUT_DEFAULT,
            sqlx::query(
                "SELECT pg_notify(concat(coalesce($1::text, current_schema()), '.', $2::text), payload) \
                 FROM unnest($3::text[]) AS payload",
            )
            .bind(context.inner.schema.as_deref())
            .bind(crate::NOTIFICATION_TOPIC_INSERT)
            .bind(
                queues
                    .iter()
                    .map(|queue| crate::protocol::insert_notification_payload(queue))
                    .collect::<Vec<_>>(),
            )
            .execute(&mut *transaction.transaction),
        )
        .await?;
    }
    transaction.commit(pool, &context.cancel).await?;
    Ok(count)
}

/// Whether the server delivers notifications, detected on the batch's
/// transaction. A server without `LISTEN`/`NOTIFY` gets none.
#[cfg(feature = "postgres")]
async fn delivers_notifications(
    context: &ServiceContext,
    pool: &sqlx::PgPool,
    transaction: &mut super::postgres::MaintenanceTransaction,
) -> Result<bool, MaintenanceError> {
    Ok(super::postgres::cancellable(
        pool,
        transaction.backend_pid,
        &context.cancel,
        TIMEOUT_DEFAULT,
        CapabilitiesCache::load_or_detect(
            context.inner.database.postgres_capabilities(),
            &mut *transaction.transaction,
        ),
    )
    .await?
    .supports_listen_notify)
}

#[cfg(feature = "sqlite")]
async fn schedule_batch_sqlite(
    pool: &sqlx::SqlitePool,
    notify_limiter: &InsertNotifyLimiter,
    look_ahead: DateTime<Utc>,
    limit: i64,
) -> Result<usize, MaintenanceError> {
    let mut transaction = crate::database::begin_sqlite_write(pool).await?;
    let candidates = sqlite::schedule_candidates(
        &mut transaction,
        look_ahead,
        i32::try_from(limit).unwrap_or(i32::MAX),
    )
    .await?;
    let count = candidates.len();
    let mut available_ids = Vec::new();
    let mut conflict_ids = Vec::new();
    let mut scheduled = Vec::new();
    for candidate in candidates {
        let Some(unique_key) = candidate.unique_key.as_deref() else {
            available_ids.push(candidate.id);
            continue;
        };
        if sqlite::schedule_has_unique_collision(&mut transaction, candidate.id, unique_key).await?
        {
            conflict_ids.push(candidate.id);
        } else {
            // Transition unique jobs one at a time so that a later duplicate
            // in the same batch observes the earlier one as a collision.
            let available =
                sqlite::schedule_set_available(&mut transaction, &[candidate.id]).await?;
            scheduled.extend(available.into_iter().map(|job| Scheduled {
                queue: job.queue,
                scheduled_at: job.scheduled_at,
            }));
        }
    }
    if !available_ids.is_empty() {
        let available = sqlite::schedule_set_available(&mut transaction, &available_ids).await?;
        scheduled.extend(available.into_iter().map(|job| Scheduled {
            queue: job.queue,
            scheduled_at: job.scheduled_at,
        }));
    }
    if !conflict_ids.is_empty() {
        sqlite::schedule_discard_conflicts(&mut transaction, &conflict_ids, look_ahead).await?;
    }
    let notified = notified_queues(&scheduled);
    for queue in notify_limiter.due(notified.iter().map(String::as_str)) {
        let payload = crate::protocol::insert_notification_payload(queue);
        sqlite::notification_insert(
            &mut transaction,
            &[sqlite::NotificationInput {
                payload: &payload,
                topic: crate::NOTIFICATION_TOPIC_INSERT,
            }],
        )
        .await?;
    }
    transaction.commit().await?;
    Ok(count)
}

#[cfg(all(test, feature = "sqlite"))]
mod sqlite_tests {
    use std::{sync::Arc, time::Duration};

    use chrono::Utc;
    use riverqueue_migrate::SqliteMigrator;
    use serde::{Deserialize, Serialize};
    use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};
    use tokio_util::sync::CancellationToken;

    use super::super::{BatchSizes, Breakers, maintainer::ServiceContext};
    use crate::{Client, InsertOpts, JobArgs, database::sqlite::sqlite_time};

    #[derive(Debug, Deserialize, JobArgs, Serialize)]
    #[river(kind = "scheduler_notification")]
    struct NotificationArgs {}

    async fn insert_due_job(pool: &SqlitePool, queue: &str) {
        sqlx::query(
            "INSERT INTO river_job (args, kind, max_attempts, metadata, queue, scheduled_at, state) \
             VALUES (jsonb('{}'), 'scheduler_notification', 25, jsonb('{}'), ?, ?, 'scheduled')",
        )
        .bind(queue)
        .bind(sqlite_time(Utc::now() - chrono::Duration::hours(1)))
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_notifications(pool: &SqlitePool, queue: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM river_notification WHERE topic = 'river_insert' \
             AND json_extract(payload, '$.queue') = ?",
        )
        .bind(queue)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// Like Go, the scheduler notifies through the client's insert
    /// notification limiter, which insertions share.
    #[tokio::test]
    async fn scheduler_notifications_wait_for_the_fetch_cooldown() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        SqliteMigrator::new(pool.clone())
            .migrate_up()
            .await
            .unwrap();
        let client = Client::builder(pool.clone())
            .fetch_cooldown(Duration::from_hours(1))
            .build()
            .unwrap();
        let context = ServiceContext {
            breakers: Arc::new(Breakers::new(BatchSizes::default())),
            cancel: CancellationToken::new(),
            inner: Arc::clone(&client.inner),
        };

        insert_due_job(&pool, "scheduled").await;
        super::run_once(&context).await.unwrap();
        assert_eq!(insert_notifications(&pool, "scheduled").await, 1);

        insert_due_job(&pool, "scheduled").await;
        super::run_once(&context).await.unwrap();
        assert_eq!(insert_notifications(&pool, "scheduled").await, 1);

        client
            .insert(NotificationArgs {})
            .opts(InsertOpts::default().with_queue("inserted"))
            .await
            .unwrap();
        insert_due_job(&pool, "inserted").await;
        super::run_once(&context).await.unwrap();
        assert_eq!(insert_notifications(&pool, "inserted").await, 1);

        let available: i64 =
            sqlx::query_scalar("SELECT count(*) FROM river_job WHERE state = 'available'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(available, 4);
    }
}
