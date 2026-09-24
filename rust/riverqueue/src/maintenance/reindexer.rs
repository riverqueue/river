//! Periodically rebuilds River's hot indexes with `REINDEX CONCURRENTLY`, a
//! port of Go's `Reindexer`.

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{AssertSqlSafe, PgConnection, PgPool};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{SchemaName, database::PostgresReindexSchedule};

use super::{
    MaintenanceError,
    maintainer::ServiceContext,
    postgres::{cancel_backend, cancellable, timeout_setting},
    sleep_cancellable,
};

/// Time allowed to drop concurrent-build artifacts after a cancelled rebuild
/// (Go uses 15 seconds).
const ARTIFACT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(15);

/// Timeout for catalog queries that decide what to rebuild.
const CATALOG_TIMEOUT: Duration = Duration::from_secs(30);

/// Returns the first scheduled run strictly after `after`.
pub(crate) fn next_run(schedule: PostgresReindexSchedule, after: DateTime<Utc>) -> DateTime<Utc> {
    match schedule {
        PostgresReindexSchedule::DailyUtc(time) => {
            let today = after.date_naive().and_time(time).and_utc();
            if today > after {
                today
            } else {
                after
                    .date_naive()
                    .succ_opt()
                    .expect("UTC date has a following day")
                    .and_time(time)
                    .and_utc()
            }
        }
        PostgresReindexSchedule::Interval(interval) => {
            after
                + chrono::Duration::from_std(interval)
                    .expect("validated reindexer interval fits chrono duration")
        }
    }
}

/// Runs the reindexer for one leadership term.
///
/// Each term schedules from its own start time, so a client that becomes
/// leader after another leader already ran today's reindex does not run it
/// again immediately. Later runs advance from the previous scheduled time, not
/// from when a run finished.
pub(super) async fn run(context: std::sync::Arc<ServiceContext>) {
    let Some(config) = context.inner.database().postgres_reindex().cloned() else {
        return;
    };
    if config.index_names().is_empty() {
        return;
    }
    let pool = context
        .inner
        .postgres_pool()
        .expect("reindexer only runs on PostgreSQL")
        .clone();
    let mut scheduled = next_run(config.schedule(), Utc::now());
    debug!(next_run_at = %scheduled, "River reindexer scheduled its first run");
    loop {
        let wait = (scheduled - Utc::now()).to_std().unwrap_or_default();
        if !sleep_cancellable(&context.cancel, wait).await {
            return;
        }
        match reindexable_index_names(
            &pool,
            &context.inner.schema,
            &context.cancel,
            config.index_names(),
        )
        .await
        {
            Ok(index_names) => {
                for index_name in index_names {
                    match reindex_one(
                        &pool,
                        &context.inner.schema,
                        &context.cancel,
                        &index_name,
                        config.timeout(),
                    )
                    .await
                    {
                        Ok(true) => info!(index_name, "River reindexer rebuilt an index"),
                        Ok(false) => {}
                        Err(MaintenanceError::Cancelled) => return,
                        Err(reindex_error) => {
                            error!(error = %reindex_error, index_name, "River reindexer failed");
                        }
                    }
                }
            }
            Err(MaintenanceError::Cancelled) => return,
            Err(list_error) => {
                error!(error = %list_error, "River reindexer could not list indexes");
            }
        }
        scheduled = next_run(config.schedule(), scheduled);
    }
}

/// Returns configured indexes that exist in the River schema, warning about
/// missing ones (Go `reindexableIndexNames`).
async fn reindexable_index_names(
    pool: &PgPool,
    schema: &SchemaName,
    cancel: &CancellationToken,
    index_names: &[String],
) -> Result<Vec<String>, MaintenanceError> {
    let rows = tokio::select! {
        biased;
        () = cancel.cancelled() => return Err(MaintenanceError::Cancelled),
        rows = tokio::time::timeout(CATALOG_TIMEOUT, sqlx::query_as::<_, (String, bool)>(
            "SELECT index_name::text, EXISTS (\
                SELECT 1 FROM pg_catalog.pg_class c \
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                WHERE n.nspname = coalesce($2::text, current_schema()) \
                  AND c.relname = index_name AND c.relkind = 'i'\
             ) FROM unnest($1::text[]) AS index_name",
        )
        .bind(index_names)
        .bind(schema.as_deref())
        .fetch_all(pool)) => rows.map_err(|_| MaintenanceError::TimedOut)??,
    };
    let mut existing = Vec::with_capacity(rows.len());
    let mut missing = Vec::new();
    for (index_name, exists) in rows {
        if exists {
            existing.push(index_name);
        } else {
            missing.push(index_name);
        }
    }
    if !missing.is_empty() {
        warn!(
            ?missing,
            "River reindexer indexes do not exist; run migrations or update the reindexer configuration"
        );
    }
    Ok(existing)
}

/// Lists leftovers of an interrupted `REINDEX CONCURRENTLY`: indexes named
/// like the target with a `_ccnew`/`_ccold` suffix and optional digits.
async fn reindex_artifacts(
    connection: &mut PgConnection,
    schema: &SchemaName,
    index_name: &str,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT index_name FROM (\
            SELECT c.relname::text AS index_name, \
                substring(c.relname FROM length($1::text) + 1) AS suffix \
            FROM pg_catalog.pg_class c \
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
            WHERE n.nspname = coalesce($2::text, current_schema()) \
              AND c.relkind = 'i' AND left(c.relname, length($1::text)) = $1::text\
         ) AS index_artifacts \
         WHERE suffix ~ '^_cc(new|old)[0-9]*$' ORDER BY index_name",
    )
    .bind(index_name)
    .bind(schema.as_deref())
    .fetch_all(connection)
    .await
}

/// Rebuilds one index unless a previous attempt left artifacts behind.
///
/// Like Go, an existing artifact means an earlier rebuild timed out, and
/// retrying would likely fail the same way, so the index is skipped with a
/// warning. When the term is cancelled mid-build, the running statement is
/// cancelled server-side and the fresh artifacts are dropped so that future
/// runs are not skipped forever.
async fn reindex_one(
    pool: &PgPool,
    schema: &SchemaName,
    cancel: &CancellationToken,
    index_name: &str,
    timeout: Duration,
) -> Result<bool, MaintenanceError> {
    let mut connection = tokio::select! {
        biased;
        () = cancel.cancelled() => return Err(MaintenanceError::Cancelled),
        connection = pool.acquire() => connection?,
    };
    let artifacts = cancellable_catalog(
        cancel,
        reindex_artifacts(&mut connection, schema, index_name),
    )
    .await?;
    if !artifacts.is_empty() {
        warn!(
            index_name,
            ?artifacts,
            "River reindexer found artifacts of a previous partially completed rebuild; skipping"
        );
        return Ok(false);
    }

    // `REINDEX CONCURRENTLY` cannot run inside a transaction, so the timeout
    // is set on the session and reset afterwards rather than overwritten with
    // an explicit `0`, preserving role and database defaults.
    let backend_pid: i32 = cancellable_catalog(
        cancel,
        sqlx::query_scalar(
            "SELECT pg_backend_pid() FROM set_config('statement_timeout', $1, false)",
        )
        .bind(timeout_setting(timeout))
        .fetch_one(&mut *connection),
    )
    .await?;
    let result = cancellable(
        pool,
        backend_pid,
        cancel,
        timeout,
        sqlx::raw_sql(AssertSqlSafe(format!(
            "REINDEX INDEX CONCURRENTLY {}",
            schema.qualify(index_name)
        )))
        .execute(&mut *connection),
    )
    .await;
    let reset = sqlx::query("RESET statement_timeout")
        .execute(&mut *connection)
        .await;

    if result.is_err() && cancel.is_cancelled() {
        drop_artifacts(pool, &mut connection, schema, index_name, backend_pid).await;
    }
    if let Err(reset_error) = reset {
        // Never return a connection with a lingering session timeout.
        debug!(error = %reset_error, "River reindexer closing a connection it could not reset");
        let _ = connection.detach();
    }
    result.map(|_| true)
}

#[cfg(all(test, feature = "postgres-tests"))]
pub(super) async fn reindex_one_for_test(
    pool: &PgPool,
    schema: &SchemaName,
    cancel: &CancellationToken,
    index_name: &str,
) -> Result<bool, MaintenanceError> {
    reindex_one(pool, schema, cancel, index_name, Duration::from_mins(1)).await
}

/// Drops concurrent-build artifacts left by a cancelled rebuild, bounded so
/// shutdown cannot hang on it.
async fn drop_artifacts(
    pool: &PgPool,
    connection: &mut PgConnection,
    schema: &SchemaName,
    index_name: &str,
    backend_pid: i32,
) {
    info!(
        index_name,
        "River reindexer stopped mid-build; dropping concurrent artifacts"
    );
    let cleanup = async {
        let artifacts = reindex_artifacts(&mut *connection, schema, index_name).await?;
        for artifact in artifacts {
            if let Err(drop_error) = sqlx::raw_sql(AssertSqlSafe(format!(
                "DROP INDEX CONCURRENTLY IF EXISTS {}",
                schema.qualify(&artifact)
            )))
            .execute(&mut *connection)
            .await
            {
                error!(error = %drop_error, artifact, "River reindexer could not drop an artifact");
            }
        }
        Ok::<_, sqlx::Error>(())
    };
    match tokio::time::timeout(ARTIFACT_CLEANUP_TIMEOUT, cleanup).await {
        Ok(Ok(())) => {}
        Ok(Err(list_error)) => {
            error!(error = %list_error, "River reindexer could not list artifacts");
        }
        Err(_) => {
            cancel_backend(pool, backend_pid).await;
            error!(index_name, "River reindexer timed out dropping artifacts");
        }
    }
}

async fn cancellable_catalog<T>(
    cancel: &CancellationToken,
    operation: impl Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, MaintenanceError> {
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(MaintenanceError::Cancelled),
        result = tokio::time::timeout(CATALOG_TIMEOUT, operation) => match result {
            Ok(result) => result.map_err(MaintenanceError::from),
            Err(_) => Err(MaintenanceError::TimedOut),
        },
    }
}

#[cfg(test)]
mod unit_tests {
    use chrono::{NaiveTime, TimeZone, Utc};

    use super::{PostgresReindexSchedule, next_run};

    #[test]
    fn schedule_advances_from_the_previous_run() {
        let before_midnight = Utc.with_ymd_and_hms(2026, 8, 11, 23, 59, 0).unwrap();
        let midnight = Utc.with_ymd_and_hms(2026, 8, 12, 0, 0, 0).unwrap();
        let daily = PostgresReindexSchedule::DailyUtc(NaiveTime::MIN);
        assert_eq!(next_run(daily, before_midnight), midnight);
        // A run exactly at midnight schedules the following midnight, like
        // Go's `t.Add(24h).Truncate(24h)`.
        assert_eq!(
            next_run(daily, midnight),
            Utc.with_ymd_and_hms(2026, 8, 13, 0, 0, 0).unwrap()
        );
        let interval = PostgresReindexSchedule::Interval(std::time::Duration::from_secs(30));
        assert_eq!(
            next_run(interval, before_midnight),
            before_midnight + chrono::Duration::seconds(30)
        );
    }
}
