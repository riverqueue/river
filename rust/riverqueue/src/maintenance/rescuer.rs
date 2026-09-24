//! Rescues jobs stuck in `running`, a port of Go's `JobRescuer`.

use chrono::{DateTime, Utc};
use serde_json::Value;
#[cfg(feature = "postgres")]
use sqlx::AssertSqlSafe;
use tracing::{debug, error};

use crate::__private::{
    DatabaseConnection as PilotDatabaseConnection, RescueAction, RescueJob, RescueManyParams,
    RescueParams,
};

#[cfg(feature = "postgres")]
use crate::client::{JobRecord, job_projection, tolerant_row};
#[cfg(feature = "sqlite")]
use crate::database::sqlite;
use crate::{AttemptError, Error, JobRow, JobState, WorkerTimeout, client::ClientInner};

use super::{
    MaintenanceError, TIMEOUT_DEFAULT, batch_backoff, batch_size, maintainer::ServiceContext,
    record_batch,
};

/// Error recorded on every rescued attempt, identical to Go's.
pub(crate) const RESCUE_ERROR: &str = "Stuck job rescued by JobRescuer";

/// Rescues stuck jobs in `id` order.
///
/// Like Go, the stuck horizon is computed once per pass and paging continues
/// after the last selected ID, so a full batch of running jobs whose worker
/// timeout has not elapsed cannot livelock the rescuer. Updates only apply to
/// rows that are still `running` with `attempted_at` before the horizon, which
/// leaves jobs that completed or were claimed again after selection untouched.
pub(super) async fn run_once(context: &ServiceContext) -> Result<(), MaintenanceError> {
    let stuck_horizon = Utc::now()
        - chrono::Duration::from_std(context.inner.maintenance.effective_rescue_after())
            .map_err(|error| Error::configuration_context("maintenance", error.to_string()))?;
    let mut after_id = 0_i64;
    loop {
        let limit = batch_size(&context.breakers.rescuer);
        let result = rescue_batch(context, after_id, limit, stuck_horizon).await;
        record_batch(&context.breakers.rescuer, &result);
        let batch = result?;
        if let Some(last_id) = batch.last_id {
            after_id = last_id;
        }
        if i64::try_from(batch.selected).unwrap_or(i64::MAX) < limit {
            return Ok(());
        }
        batch_backoff(&context.cancel).await?;
    }
}

struct Batch {
    last_id: Option<i64>,
    selected: usize,
}

async fn rescue_batch(
    context: &ServiceContext,
    after_id: i64,
    limit: i64,
    stuck_horizon: DateTime<Utc>,
) -> Result<Batch, MaintenanceError> {
    #[cfg(feature = "sqlite")]
    if let Some(pool) = context.inner.sqlite_pool() {
        return super::sqlite_cancellable(
            &context.cancel,
            TIMEOUT_DEFAULT,
            rescue_batch_sqlite(&context.inner, pool, after_id, limit, stuck_horizon),
        )
        .await;
    }
    #[cfg(feature = "postgres")]
    return rescue_batch_postgres(context, after_id, limit, stuck_horizon).await;
    #[allow(unreachable_code)]
    Ok(Batch {
        last_id: None,
        selected: 0,
    })
}

fn rescue_params(
    inner: &ClientInner,
    after_id: i64,
    limit: i64,
    stuck_horizon: DateTime<Utc>,
) -> RescueParams {
    RescueParams {
        after_id,
        database: inner.pilot_database_config(),
        maximum: limit,
        rescue_after: inner.maintenance.effective_rescue_after(),
        stuck_horizon,
    }
}

/// Decides what OSS writes for each selected job, skipping jobs whose worker
/// timeout has not elapsed yet.
fn rescue_jobs(
    inner: &ClientInner,
    rows: &[JobRow],
    now: DateTime<Utc>,
) -> Result<Vec<RescueJob>, Error> {
    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        let Some((state, finalized_at, scheduled_at)) = decide(inner, row, now)? else {
            continue;
        };
        let attempt_error = serde_json::to_value(AttemptError {
            at: now,
            attempt: row.attempt.max(0),
            error: RESCUE_ERROR.to_owned(),
            trace: String::new(),
        })?;
        jobs.push(RescueJob {
            attempt_error,
            finalized_at,
            id: row.id,
            scheduled_at,
            state,
        });
    }
    Ok(jobs)
}

type Decision = Option<(JobState, Option<DateTime<Utc>>, DateTime<Utc>)>;

/// Go's `makeRetryDecision`, preceded by its cancellation check.
fn decide(inner: &ClientInner, row: &JobRow, now: DateTime<Utc>) -> Result<Decision, Error> {
    if cancel_attempted(row.metadata.get("cancel_attempted_at")) {
        return Ok(Some((JobState::Cancelled, Some(now), row.scheduled_at)));
    }
    if !inner.workers.contains_kind(&row.kind) {
        error!(
            job_id = row.id,
            job_kind = row.kind,
            "River rescuer discarding a stuck job of an unhandled kind"
        );
        return Ok(Some((JobState::Discarded, Some(now), row.scheduled_at)));
    }
    let retry_or_discard = |retry_at: DateTime<Utc>| {
        if row.attempt < row.max_attempts.max(0) {
            (JobState::Retryable, None, retry_at)
        } else {
            (JobState::Discarded, Some(now), row.scheduled_at)
        }
    };
    let client_retry = |row: &JobRow| -> Result<DateTime<Utc>, Error> {
        let delay = inner.retry_policy.next_retry(row, "", now);
        Ok(now
            + chrono::Duration::from_std(delay)
                .map_err(|error| Error::invalid_job_context("maintenance", error.to_string()))?)
    };

    // A worker that cannot evaluate the job, for example because its args no
    // longer decode, is retried with the client policy like Go's unmarshal
    // failure path, without consulting the worker timeout.
    let timeout = match inner.workers.timeout(row) {
        Ok(WorkerTimeout::After(timeout)) => Some(timeout),
        Ok(WorkerTimeout::ClientDefault) => inner.job_timeout,
        Ok(WorkerTimeout::Disabled) => None,
        Err(timeout_error) => {
            debug!(error = %timeout_error, job_id = row.id, "River rescuer could not evaluate a stuck job");
            return Ok(Some(retry_or_discard(client_retry(row)?)));
        }
    };
    let Some(timeout) = timeout else {
        // A disabled timeout means the job may legitimately run forever.
        return Ok(None);
    };
    let elapsed = row
        .attempted_at
        .and_then(|attempted_at| now.signed_duration_since(attempted_at).to_std().ok())
        .unwrap_or_default();
    if !timeout.is_zero() && elapsed < timeout {
        return Ok(None);
    }

    let rescued_error = crate::WorkError::new(Box::new(std::io::Error::other(
        "job rescued after its worker stopped responding",
    )));
    let retry_at = match inner.workers.next_retry(row, &rescued_error, now) {
        Ok(Some(delay)) => {
            now + chrono::Duration::from_std(delay)
                .map_err(|error| Error::invalid_job_context("maintenance", error.to_string()))?
        }
        Ok(None) => client_retry(row)?,
        Err(retry_error) => {
            debug!(error = %retry_error, job_id = row.id, "River rescuer used the client retry policy");
            client_retry(row)?
        }
    };
    Ok(Some(retry_or_discard(retry_at)))
}

/// Go decodes `cancel_attempted_at` as a `time.Time` and cancels only when it
/// is a non-zero timestamp; absent, null, or unparsable values do not cancel.
fn cancel_attempted(value: Option<&Value>) -> bool {
    let go_zero_time =
        chrono::NaiveDate::from_ymd_opt(1, 1, 1).and_then(|date| date.and_hms_opt(0, 0, 0));
    value
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .is_some_and(|time| Some(time.naive_utc()) != go_zero_time)
}

#[cfg(feature = "postgres")]
#[expect(
    clippy::too_many_lines,
    reason = "selection, extension interception, and the guarded update share one transaction"
)]
async fn rescue_batch_postgres(
    context: &ServiceContext,
    after_id: i64,
    limit: i64,
    stuck_horizon: DateTime<Utc>,
) -> Result<Batch, MaintenanceError> {
    use super::postgres::{MaintenanceTransaction, cancellable};

    let inner = &context.inner;
    let pool = inner
        .postgres_pool()
        .expect("client database is PostgreSQL or SQLite");
    let table = inner.schema.qualify("river_job");
    let mut transaction =
        MaintenanceTransaction::begin(pool, &context.cancel, TIMEOUT_DEFAULT).await?;
    let backend_pid = transaction.backend_pid;

    let selected_ids = if inner.pilot.intercepts_rescue() {
        inner
            .pilot
            .select_rescue_job_ids(
                PilotDatabaseConnection::Postgres(&mut transaction.transaction),
                &rescue_params(inner, after_id, limit, stuck_horizon),
            )
            .await
            .map_err(|source| Error::Extension {
                phase: "rescue selection",
                source,
            })?
    } else {
        None
    };
    // Like Go's `JobGetStuck`, selection takes no row locks; the guarded
    // update below is what keeps a stale selection from rescuing a job.
    let records = match selected_ids {
        Some(ids) => {
            cancellable(
                pool,
                backend_pid,
                &context.cancel,
                TIMEOUT_DEFAULT,
                sqlx::query_as::<_, JobRecord>(AssertSqlSafe(format!(
                    "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
                     WHERE id = ANY($1::bigint[]) ORDER BY id",
                    job_projection("job")
                )))
                .bind(ids)
                .fetch_all(&mut *transaction.transaction),
            )
            .await?
        }
        None => {
            cancellable(
                pool,
                backend_pid,
                &context.cancel,
                TIMEOUT_DEFAULT,
                sqlx::query_as::<_, JobRecord>(AssertSqlSafe(format!(
                    "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
                     WHERE state = 'running' AND id > $1 AND attempted_at < $2 \
                     ORDER BY id LIMIT $3",
                    job_projection("job")
                )))
                .bind(after_id)
                .bind(stuck_horizon)
                .bind(limit)
                .fetch_all(&mut *transaction.transaction),
            )
            .await?
        }
    };
    // Like River Go's `JobGetStuck`, a row that can't be fully decoded is
    // still returned with its undecodable fields left empty, so a job
    // stranded by such a row can be rescued.
    let rows = records
        .into_iter()
        .filter_map(|record| tolerant_row(record.decode()))
        .collect::<Vec<_>>();
    let batch = Batch {
        last_id: rows.last().map(|row| row.id),
        selected: rows.len(),
    };
    let jobs = rescue_jobs(inner, &rows, Utc::now())?;
    if jobs.is_empty() {
        transaction.commit(pool, &context.cancel).await?;
        return Ok(batch);
    }

    let params = RescueManyParams {
        database: inner.pilot_database_config(),
        jobs,
        stuck_horizon,
    };
    let action = if inner.pilot.intercepts_rescue() {
        inner
            .pilot
            .rescue_jobs(
                PilotDatabaseConnection::Postgres(&mut transaction.transaction),
                &params,
            )
            .await
            .map_err(|source| Error::Extension {
                phase: "rescue",
                source,
            })?
    } else {
        RescueAction::Continue
    };
    if action == RescueAction::Continue {
        let state_type = inner.schema.qualify("river_job_state");
        let rescue_count = crate::METADATA_KEY_RESCUE_COUNT;
        cancellable(
            pool,
            backend_pid,
            &context.cancel,
            TIMEOUT_DEFAULT,
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE {table} AS job SET \
                    errors = array_append(job.errors, updated_job.error), \
                    finalized_at = updated_job.finalized_at, \
                    scheduled_at = updated_job.scheduled_at, \
                    metadata = job.metadata || jsonb_build_object('{rescue_count}', \
                        coalesce(CASE WHEN jsonb_typeof(job.metadata -> '{rescue_count}') = 'number' \
                            THEN (job.metadata ->> '{rescue_count}')::int END, 0) + 1), \
                    state = updated_job.state \
                 FROM (\
                    SELECT unnest($1::bigint[]) AS id, unnest($2::jsonb[]) AS error, \
                        unnest($3::timestamptz[]) AS finalized_at, \
                        unnest($4::timestamptz[]) AS scheduled_at, \
                        unnest($5::text[])::{state_type} AS state\
                 ) AS updated_job \
                 WHERE job.id = updated_job.id AND job.state = 'running' \
                   AND job.attempted_at < $6"
            )))
            .bind(params.jobs.iter().map(|job| job.id).collect::<Vec<_>>())
            .bind(
                params
                    .jobs
                    .iter()
                    .map(|job| job.attempt_error.clone())
                    .collect::<Vec<_>>(),
            )
            .bind(
                params
                    .jobs
                    .iter()
                    .map(|job| job.finalized_at)
                    .collect::<Vec<_>>(),
            )
            .bind(
                params
                    .jobs
                    .iter()
                    .map(|job| job.scheduled_at)
                    .collect::<Vec<_>>(),
            )
            .bind(
                params
                    .jobs
                    .iter()
                    .map(|job| job.state.as_str())
                    .collect::<Vec<_>>(),
            )
            .bind(stuck_horizon)
            .execute(&mut *transaction.transaction),
        )
        .await?;
    }
    transaction.commit(pool, &context.cancel).await?;
    Ok(batch)
}

#[cfg(feature = "sqlite")]
async fn rescue_batch_sqlite(
    inner: &ClientInner,
    pool: &sqlx::SqlitePool,
    after_id: i64,
    limit: i64,
    stuck_horizon: DateTime<Utc>,
) -> Result<Batch, MaintenanceError> {
    let mut transaction = crate::database::begin_sqlite_write(pool).await?;
    let selected_ids = if inner.pilot.intercepts_rescue() {
        inner
            .pilot
            .select_rescue_job_ids(
                PilotDatabaseConnection::Sqlite(&mut transaction),
                &rescue_params(inner, after_id, limit, stuck_horizon),
            )
            .await
            .map_err(|source| Error::Extension {
                phase: "rescue selection",
                source,
            })?
    } else {
        None
    };
    let rows = match selected_ids {
        Some(ids) => sqlite::jobs_by_ids(&mut transaction, &ids).await?,
        None => {
            sqlite::stuck_jobs(
                &mut transaction,
                after_id,
                stuck_horizon,
                i32::try_from(limit).unwrap_or(i32::MAX),
            )
            .await?
        }
    };
    let batch = Batch {
        last_id: rows.last().map(|row| row.id),
        selected: rows.len(),
    };
    let jobs = rescue_jobs(inner, &rows, Utc::now())?;
    if !jobs.is_empty() {
        let params = RescueManyParams {
            database: inner.pilot_database_config(),
            jobs,
            stuck_horizon,
        };
        let action = if inner.pilot.intercepts_rescue() {
            inner
                .pilot
                .rescue_jobs(PilotDatabaseConnection::Sqlite(&mut transaction), &params)
                .await
                .map_err(|source| Error::Extension {
                    phase: "rescue",
                    source,
                })?
        } else {
            RescueAction::Continue
        };
        if action == RescueAction::Continue {
            for job in &params.jobs {
                let state = job.state;
                let error: AttemptError =
                    serde_json::from_value(job.attempt_error.clone()).map_err(Error::from)?;
                sqlite::rescue(
                    &mut transaction,
                    &sqlite::RescueJob {
                        error: &error,
                        finalized_at: job.finalized_at,
                        id: job.id,
                        scheduled_at: job.scheduled_at,
                        state,
                        stuck_horizon,
                    },
                )
                .await?;
            }
        }
    }
    transaction.commit().await?;
    Ok(batch)
}

#[cfg(test)]
mod unit_tests {
    use serde_json::json;

    use super::cancel_attempted;

    #[test]
    fn cancel_attempted_requires_a_non_zero_timestamp_like_go() {
        assert!(cancel_attempted(Some(&json!("2026-01-02T03:04:05Z"))));
        assert!(cancel_attempted(Some(&json!(
            "2026-01-02T03:04:05.123456+00:00"
        ))));
        assert!(!cancel_attempted(None));
        assert!(!cancel_attempted(Some(&json!(null))));
        assert!(!cancel_attempted(Some(&json!("0001-01-01T00:00:00Z"))));
        assert!(!cancel_attempted(Some(&json!("not a time"))));
        assert!(!cancel_attempted(Some(&json!(true))));
    }
}
