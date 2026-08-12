//! Leader election and leader-owned PostgreSQL maintenance.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use sqlx::{AssertSqlSafe, Row, types::Json};
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::{
    AttemptError, Client, Error, JobState, ReindexerSchedule, WorkerTimeout,
    client::{ClientInner, JobRecord, job_projection},
};

const BATCH_SIZE: i64 = 10_000;
const LEADER_TTL_PADDING: Duration = Duration::from_secs(10);

#[allow(clippy::too_many_lines)]
pub(crate) async fn run_maintenance(
    inner: Arc<ClientInner>,
    cancel: CancellationToken,
    mut notifications: broadcast::Receiver<String>,
) -> Result<(), Error> {
    for hook in &inner.hooks {
        if let Err(hook_error) = hook.periodic_jobs_start(&inner.periodic_jobs).await {
            error!(error = %hook_error, "River periodic-jobs start hook failed");
        }
    }

    let mut cleaner_last = None;
    let mut queue_cleaner_last = None;
    let mut rescuer_last = None;
    let mut reindexer_next = None;
    let mut scheduler_last = None;
    let mut election_last = None;
    let mut extension_cancel = CancellationToken::new();
    let mut extension_tasks = JoinSet::new();
    let mut is_leader = false;
    let mut ticker =
        tokio::time::interval(inner.maintenance.elect_interval.min(Duration::from_secs(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            _ = ticker.tick() => {}
            notification = notifications.recv() => {
                let requested_resignation = matches!(
                    notification.as_deref(),
                    Ok("__river_leadership_request_resign__")
                );
                let resigned = requested_resignation && is_leader;
                if resigned
                {
                    if let Err(operation_error) = resign(&inner).await {
                        error!(error = %operation_error, "River leader requested resignation failed");
                    } else {
                        is_leader = false;
                        extension_cancel.cancel();
                        inner.periodic_jobs.reset_for_leadership().await;
                    }
                }
                if matches!(
                    notification.as_deref(),
                    Ok("__river_leadership__" | "__river_leadership_request_resign__")
                ) {
                    // Give peers one election interval to acquire the lease
                    // after an explicit resignation. Without this cooldown,
                    // the resigning client can immediately reacquire it in the
                    // same loop turn and starve the requester.
                    election_last = resigned.then_some(tokio::time::Instant::now());
                }
            }
        }
        if is_due(&mut election_last, inner.maintenance.elect_interval) {
            let was_leader = is_leader;
            is_leader = match try_hold_leadership(&inner).await {
                Ok(is_leader) => is_leader,
                Err(operation_error) => {
                    error!(error = %operation_error, "River leader election failed");
                    false
                }
            };
            if is_leader && !was_leader {
                inner.periodic_jobs.reset_for_leadership().await;
                extension_cancel = CancellationToken::new();
                for service in inner.pilot.maintenance_services() {
                    let pool = inner.pool.clone();
                    let schema = inner.schema.clone();
                    let service_cancel = extension_cancel.child_token();
                    extension_tasks
                        .spawn(async move { service.run(pool, schema, service_cancel).await });
                }
            } else if was_leader && !is_leader {
                inner.periodic_jobs.reset_for_leadership().await;
                extension_cancel.cancel();
            }
        }
        while let Some(result) = extension_tasks.try_join_next() {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(service_error)) => {
                    error!(error = %service_error, "River pilot maintenance service failed");
                }
                Err(join_error) => {
                    error!(error = %join_error, "River pilot maintenance task failed");
                }
            }
        }
        if !is_leader {
            continue;
        }

        inner
            .periodic_jobs
            .run_due(
                &Client {
                    inner: Arc::clone(&inner),
                },
                Utc::now(),
            )
            .await;

        if is_due(&mut scheduler_last, inner.maintenance.scheduler_interval)
            && let Err(operation_error) = schedule_jobs(&inner, &cancel).await
        {
            error!(error = %operation_error, "River job scheduler failed");
        }
        if is_due(&mut rescuer_last, inner.maintenance.rescuer_interval)
            && let Err(operation_error) = rescue_jobs(&inner, &cancel).await
        {
            error!(error = %operation_error, "River job rescuer failed");
        }
        if is_due(&mut cleaner_last, inner.maintenance.job_cleaner_interval)
            && let Err(operation_error) = clean_jobs(&inner, &cancel).await
        {
            error!(error = %operation_error, "River job cleaner failed");
        }
        if is_due(
            &mut queue_cleaner_last,
            inner.maintenance.queue_cleaner_interval,
        ) && let Err(operation_error) = clean_queues(&inner, &cancel).await
        {
            error!(error = %operation_error, "River queue cleaner failed");
        }
        if !inner.maintenance.reindexer_index_names.is_empty()
            && reindex_is_due(
                &mut reindexer_next,
                inner.maintenance.reindexer_schedule,
                Utc::now(),
            )
            && let Err(operation_error) = reindex(&inner).await
        {
            error!(error = %operation_error, "River reindexer failed");
        }
    }

    extension_cancel.cancel();
    while let Some(result) = extension_tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(service_error)) => {
                debug!(error = %service_error, "River pilot maintenance service failed during shutdown");
            }
            Err(join_error) => {
                debug!(error = %join_error, "River pilot maintenance task failed during shutdown");
            }
        }
    }
    if let Err(operation_error) = resign(&inner).await {
        debug!(error = %operation_error, "River leader resignation failed during shutdown");
    }
    inner.periodic_jobs.reset_for_leadership().await;
    Ok(())
}

fn reindex_is_due(
    next: &mut Option<DateTime<Utc>>,
    schedule: ReindexerSchedule,
    now: DateTime<Utc>,
) -> bool {
    let due = next.is_some_and(|next| now >= next);
    if next.is_none() || due {
        *next = Some(match schedule {
            ReindexerSchedule::DailyUtc(time) => {
                let today = now.date_naive().and_time(time).and_utc();
                if today > now {
                    today
                } else {
                    now.date_naive()
                        .succ_opt()
                        .expect("UTC date has a following day")
                        .and_time(time)
                        .and_utc()
                }
            }
            ReindexerSchedule::Interval(interval) => {
                now + chrono::Duration::from_std(interval)
                    .expect("validated reindexer interval fits chrono duration")
            }
        });
    }
    due
}

fn is_due(last: &mut Option<tokio::time::Instant>, interval: Duration) -> bool {
    let now = tokio::time::Instant::now();
    if last.is_none_or(|last| now.duration_since(last) >= interval) {
        *last = Some(now);
        true
    } else {
        false
    }
}

async fn try_hold_leadership(inner: &ClientInner) -> Result<bool, Error> {
    let table = inner.schema.qualify("river_leader");
    let ttl = (inner.maintenance.elect_interval + LEADER_TTL_PADDING).as_secs_f64();
    let sql = format!(
        "WITH expired AS (DELETE FROM {table} WHERE expires_at < now()), \
         elected AS (INSERT INTO {table} (leader_id, elected_at, expires_at) \
             VALUES ($1, now(), now() + make_interval(secs => $2)) \
             ON CONFLICT (name) DO UPDATE SET expires_at = now() + make_interval(secs => $2) \
             WHERE {table}.leader_id = excluded.leader_id AND {table}.expires_at >= now() \
             RETURNING leader_id) \
         SELECT EXISTS(SELECT 1 FROM elected)"
    );
    Ok(sqlx::query_scalar::<_, bool>(AssertSqlSafe(sql))
        .bind(&inner.id)
        .bind(ttl)
        .fetch_one(&inner.pool)
        .await?)
}

async fn resign(inner: &ClientInner) -> Result<(), Error> {
    let table = inner.schema.qualify("river_leader");
    let sql = format!(
        "WITH resigned AS (DELETE FROM {table} WHERE leader_id = $1 RETURNING leader_id), \
         notified AS (SELECT pg_notify(concat(coalesce($2::text, current_schema()), '.', $3::text), \
             json_build_object('action', 'resigned', 'leader_id', leader_id)::text) FROM resigned) \
         SELECT count(*) FROM notified"
    );
    sqlx::query_scalar::<_, i64>(AssertSqlSafe(sql))
        .bind(&inner.id)
        .bind(inner.schema.as_deref())
        .bind(crate::NOTIFICATION_TOPIC_LEADERSHIP)
        .fetch_one(&inner.pool)
        .await?;
    Ok(())
}

async fn schedule_jobs(inner: &ClientInner, cancel: &CancellationToken) -> Result<(), Error> {
    let table = inner.schema.qualify("river_job");
    let state_function = inner.schema.qualify("river_job_state_in_bitmask");
    let state_type = inner.schema.qualify("river_job_state");
    let sql = format!(
        "WITH jobs_to_schedule AS (\
            SELECT id, unique_key, unique_states, priority, scheduled_at FROM {table} \
            WHERE state IN ('retryable', 'scheduled') AND scheduled_at <= now() \
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
         ), updates AS (\
            SELECT candidate.id, CASE \
                WHEN candidate.row_num IS NULL THEN 'available'::{state_type} \
                WHEN conflict.unique_key IS NOT NULL OR candidate.row_num > 1 THEN 'discarded'::{state_type} \
                ELSE 'available'::{state_type} END AS new_state \
            FROM jobs_with_rownum AS candidate LEFT JOIN unique_conflicts AS conflict \
              ON candidate.unique_key = conflict.unique_key\
         ), updated AS (\
            UPDATE {table} AS job SET state = updates.new_state, \
              finalized_at = CASE WHEN updates.new_state = 'discarded' THEN now() ELSE finalized_at END, \
              metadata = CASE WHEN updates.new_state = 'discarded' \
                THEN metadata || '{{\"unique_key_conflict\":\"scheduler_discarded\"}}'::jsonb ELSE metadata END \
            FROM updates WHERE job.id = updates.id RETURNING job.queue, job.state::text\
         ) SELECT queue, state FROM updated"
    );
    loop {
        let mut transaction = inner.pool.begin().await?;
        let rows = sqlx::query(AssertSqlSafe(sql.clone()))
            .bind(BATCH_SIZE)
            .fetch_all(&mut *transaction)
            .await?;
        let row_count = rows.len();
        let queues = rows
            .iter()
            .filter(|row| row.get::<String, _>("state") == "available")
            .map(|row| row.get::<String, _>("queue"))
            .collect::<BTreeSet<_>>();
        for queue in queues {
            sqlx::query(
                "SELECT pg_notify(concat(coalesce($1::text, current_schema()), '.', $2::text), json_build_object('queue', $3::text)::text)",
            )
            .bind(inner.schema.as_deref())
            .bind(crate::NOTIFICATION_TOPIC_INSERT)
            .bind(queue)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        if row_count < usize::try_from(BATCH_SIZE).expect("batch size fits usize")
            || cancel.is_cancelled()
        {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
}

#[allow(clippy::too_many_lines)]
async fn rescue_jobs(inner: &ClientInner, cancel: &CancellationToken) -> Result<(), Error> {
    let table = inner.schema.qualify("river_job");
    let sql = format!(
        "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
         WHERE state = 'running' AND attempted_at < now() - make_interval(secs => $1) \
         ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED",
        job_projection("job")
    );
    loop {
        let mut transaction = inner.pool.begin().await?;
        let records = if inner.pilot.intercepts_rescue() {
            let ids = inner
                .pilot
                .select_rescue_job_ids(
                    &mut transaction,
                    &riverqueue_internal::RescueParams {
                        maximum: BATCH_SIZE,
                        rescue_after: inner.maintenance.rescue_after,
                        schema: inner.schema.clone(),
                    },
                )
                .await
                .map_err(|error| Error::Runtime(format!("pilot rescue selection: {error}")))?
                .unwrap_or_default();
            let selected_sql = format!(
                "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
                 WHERE id = ANY($1::bigint[]) ORDER BY id FOR UPDATE",
                job_projection("job")
            );
            sqlx::query_as::<_, JobRecord>(AssertSqlSafe(selected_sql))
                .bind(ids)
                .fetch_all(&mut *transaction)
                .await?
        } else {
            sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql.clone()))
                .bind(inner.maintenance.rescue_after.as_secs_f64())
                .bind(BATCH_SIZE)
                .fetch_all(&mut *transaction)
                .await?
        };
        let record_count = records.len();
        let now = Utc::now();
        for record in records {
            let row = record.into_job_row()?;
            let (finalized_at, scheduled_at, state) = if row
                .metadata
                .contains_key("cancel_attempted_at")
            {
                (Some(now), row.scheduled_at, JobState::Cancelled)
            } else if !inner.workers.contains_kind(&row.kind) {
                (Some(now), row.scheduled_at, JobState::Discarded)
            } else {
                let timeout = match inner.workers.timeout(&row) {
                    Ok(WorkerTimeout::After(timeout)) => Some(timeout),
                    Ok(WorkerTimeout::ClientDefault) => inner.job_timeout,
                    Ok(WorkerTimeout::Disabled) => None,
                    Err(timeout_error) => {
                        debug!(
                            error = %timeout_error,
                            job_id = row.id,
                            "could not evaluate rescued worker timeout; rescuing job"
                        );
                        Some(Duration::ZERO)
                    }
                };
                let timeout_elapsed = timeout.is_some_and(|timeout| {
                    row.attempted_at
                        .and_then(|attempted_at| {
                            now.signed_duration_since(attempted_at).to_std().ok()
                        })
                        .is_some_and(|elapsed| elapsed >= timeout)
                });
                if !timeout_elapsed {
                    continue;
                }
                if row.attempt >= row.max_attempts {
                    (Some(now), row.scheduled_at, JobState::Discarded)
                } else {
                    let retry_delay = match inner.workers.next_retry(&row, "", now) {
                        Ok(Some(retry_delay)) => retry_delay,
                        Ok(None) => inner.retry_policy.next_retry(&row, "", now),
                        Err(retry_error) => {
                            debug!(
                                error = %retry_error,
                                job_id = row.id,
                                "could not evaluate rescued worker retry policy; using client policy"
                            );
                            inner.retry_policy.next_retry(&row, "", now)
                        }
                    };
                    let retry_at = now
                        + chrono::Duration::from_std(retry_delay)
                            .map_err(|error| Error::InvalidJob(error.to_string()))?;
                    (None, retry_at, JobState::Retryable)
                }
            };
            let attempt_error = AttemptError {
                at: now,
                attempt: row.attempt.max(0),
                error: "Stuck job rescued by JobRescuer".to_owned(),
                trace: String::new(),
            };
            let update_sql = format!(
                "UPDATE {table} SET errors = array_append(coalesce(errors, '{{}}'), $2::jsonb), \
             finalized_at = $3, scheduled_at = $4, state = $5::text::{}, \
             metadata = metadata || jsonb_build_object('{}', \
               coalesce(CASE WHEN jsonb_typeof(metadata -> '{}') = 'number' \
                 THEN (metadata ->> '{}')::int END, 0) + 1) \
             WHERE id = $1 AND state = 'running'",
                inner.schema.qualify("river_job_state"),
                crate::METADATA_KEY_RESCUE_COUNT,
                crate::METADATA_KEY_RESCUE_COUNT,
                crate::METADATA_KEY_RESCUE_COUNT,
            );
            sqlx::query(AssertSqlSafe(update_sql))
                .bind(row.id)
                .bind(Json(attempt_error))
                .bind(finalized_at)
                .bind(scheduled_at)
                .bind(state.as_str())
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;
        if record_count < usize::try_from(BATCH_SIZE).expect("batch size fits usize")
            || cancel.is_cancelled()
        {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
}

async fn clean_jobs(inner: &ClientInner, cancel: &CancellationToken) -> Result<(), Error> {
    let table = inner.schema.qualify("river_job");
    let cancelled = inner.maintenance.cancelled_job_retention;
    let completed = inner.maintenance.completed_job_retention;
    let discarded = inner.maintenance.discarded_job_retention;
    let sql = format!(
        "DELETE FROM {table} WHERE id IN (SELECT id FROM {table} WHERE ( \
          (state = 'cancelled' AND $1 AND finalized_at < now() - make_interval(secs => $2)) OR \
          (state = 'completed' AND $3 AND finalized_at < now() - make_interval(secs => $4)) OR \
          (state = 'discarded' AND $5 AND finalized_at < now() - make_interval(secs => $6))) \
         AND (NOT $8 OR NOT (metadata ? 'workflow_id')) \
         ORDER BY id LIMIT $7)"
    );
    loop {
        let operation = sqlx::query(AssertSqlSafe(sql.clone()))
            .bind(cancelled.is_some())
            .bind(cancelled.unwrap_or_default().as_secs_f64())
            .bind(completed.is_some())
            .bind(completed.unwrap_or_default().as_secs_f64())
            .bind(discarded.is_some())
            .bind(discarded.unwrap_or_default().as_secs_f64())
            .bind(BATCH_SIZE)
            .bind(inner.pilot.excludes_workflow_jobs_from_cleaner())
            .execute(&inner.pool);
        let result = tokio::time::timeout(inner.maintenance.job_cleaner_timeout, operation)
            .await
            .map_err(|_| Error::Runtime("River job cleaner query timed out".to_owned()))??;
        if result.rows_affected() < BATCH_SIZE as u64 || cancel.is_cancelled() {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
}

async fn clean_queues(inner: &ClientInner, cancel: &CancellationToken) -> Result<(), Error> {
    let table = inner.schema.qualify("river_queue");
    let sql = format!(
        "DELETE FROM {table} WHERE name IN (SELECT name FROM {table} \
         WHERE updated_at < now() - make_interval(secs => $1) ORDER BY name LIMIT $2)"
    );
    loop {
        let result = sqlx::query(AssertSqlSafe(sql.clone()))
            .bind(inner.maintenance.queue_retention.as_secs_f64())
            .bind(BATCH_SIZE)
            .execute(&inner.pool)
            .await?;
        if result.rows_affected() < BATCH_SIZE as u64 || cancel.is_cancelled() {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
}

async fn reindex(inner: &ClientInner) -> Result<(), Error> {
    let mut connection = inner.pool.acquire().await?;
    let timeout_millis = i64::try_from(inner.maintenance.reindexer_timeout.as_millis())
        .map_err(|_| Error::InvalidJob("reindexer timeout is too large".to_owned()))?;
    sqlx::query("SELECT set_config('statement_timeout', $1, false)")
        .bind(timeout_millis.to_string())
        .execute(&mut *connection)
        .await?;
    let reindex_result = reindex_all(inner, &mut connection).await;
    let reset_result = sqlx::query("SELECT set_config('statement_timeout', '0', false)")
        .execute(&mut *connection)
        .await;
    reindex_result?;
    reset_result?;
    Ok(())
}

async fn reindex_all(
    inner: &ClientInner,
    connection: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
) -> Result<(), Error> {
    for index_name in &inner.maintenance.reindexer_index_names {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname = coalesce($1, current_schema()) AND indexname = $2)",
        )
        .bind(inner.schema.as_deref())
        .bind(index_name)
        .fetch_one(&mut **connection)
        .await?;
        if !exists {
            debug!(index_name, "River reindexer skipped missing index");
            continue;
        }
        let artifact_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname = coalesce($1, current_schema()) AND (indexname LIKE $2 OR indexname LIKE $3))",
        )
        .bind(inner.schema.as_deref())
        .bind(format!("{index_name}_ccnew%"))
        .bind(format!("{index_name}_ccold%"))
        .fetch_one(&mut **connection)
        .await?;
        if artifact_exists {
            debug!(
                index_name,
                "River reindexer skipped index with concurrent artifacts"
            );
            continue;
        }
        let sql = format!(
            "REINDEX INDEX CONCURRENTLY {}",
            inner.schema.qualify(index_name)
        );
        if let Err(reindex_error) = sqlx::raw_sql(AssertSqlSafe(sql))
            .execute(&mut **connection)
            .await
        {
            error!(error = %reindex_error, index_name, "River index rebuild failed");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{NaiveTime, TimeZone, Utc};

    use super::{ReindexerSchedule, reindex_is_due};

    #[test]
    fn reindex_schedule_daily_utc_and_interval() {
        let before_midnight = Utc.with_ymd_and_hms(2026, 8, 11, 23, 59, 0).unwrap();
        let midnight = Utc.with_ymd_and_hms(2026, 8, 12, 0, 0, 0).unwrap();
        let mut next = None;
        assert!(!reindex_is_due(
            &mut next,
            ReindexerSchedule::DailyUtc(NaiveTime::MIN),
            before_midnight,
        ));
        assert_eq!(next, Some(midnight));
        assert!(reindex_is_due(
            &mut next,
            ReindexerSchedule::DailyUtc(NaiveTime::MIN),
            midnight,
        ));
        assert_eq!(
            next,
            Some(Utc.with_ymd_and_hms(2026, 8, 13, 0, 0, 0).unwrap())
        );

        let mut next = None;
        assert!(!reindex_is_due(
            &mut next,
            ReindexerSchedule::Interval(Duration::from_secs(30)),
            before_midnight,
        ));
        assert_eq!(next, Some(before_midnight + chrono::Duration::seconds(30)));
    }
}
