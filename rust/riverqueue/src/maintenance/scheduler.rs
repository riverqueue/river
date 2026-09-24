//! Moves due `scheduled` and `retryable` jobs to `available`, a port of Go's
//! `JobScheduler`.

use std::{collections::BTreeSet, time::Duration};

use chrono::{DateTime, Utc};
#[cfg(feature = "postgres")]
use sqlx::{AssertSqlSafe, Row};

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
    #[cfg(feature = "sqlite")]
    if let Some(pool) = context.inner.sqlite_pool() {
        return super::sqlite_cancellable(
            &context.cancel,
            TIMEOUT_DEFAULT,
            schedule_batch_sqlite(pool, look_ahead, limit),
        )
        .await;
    }
    #[cfg(feature = "postgres")]
    return schedule_batch_postgres(context, look_ahead, limit).await;
    #[allow(unreachable_code)]
    Ok(0)
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
    look_ahead: DateTime<Utc>,
    limit: i64,
) -> Result<usize, MaintenanceError> {
    use super::postgres::{MaintenanceTransaction, cancellable};

    let inner = &context.inner;
    let pool = inner
        .postgres_pool()
        .expect("client database is PostgreSQL or SQLite");
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
    let queues = notified_queues(&scheduled).into_iter().collect::<Vec<_>>();
    if !queues.is_empty() {
        cancellable(
            pool,
            backend_pid,
            &context.cancel,
            TIMEOUT_DEFAULT,
            sqlx::query(
                "SELECT pg_notify(concat(coalesce($1::text, current_schema()), '.', $2::text), \
                 json_build_object('queue', queue)::text) FROM unnest($3::text[]) AS queue",
            )
            .bind(context.inner.schema.as_deref())
            .bind(crate::NOTIFICATION_TOPIC_INSERT)
            .bind(queues)
            .execute(&mut *transaction.transaction),
        )
        .await?;
    }
    transaction.commit(pool, &context.cancel).await?;
    Ok(count)
}

#[cfg(feature = "sqlite")]
async fn schedule_batch_sqlite(
    pool: &sqlx::SqlitePool,
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
    for queue in notified_queues(&scheduled) {
        let payload = serde_json::json!({"queue": queue}).to_string();
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
