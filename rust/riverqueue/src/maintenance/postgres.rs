//! Postgres helpers that make maintenance statements cancellable and
//! bounded on the server, not just abandoned by the client.

use std::time::Duration;

use sqlx::{PgPool, Postgres, Transaction};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::MaintenanceError;

/// Extra client-side patience beyond a server-side `statement_timeout`
/// before the client gives up on an unresponsive connection.
const CLIENT_TIMEOUT_GRACE: Duration = Duration::from_secs(5);

/// Upper bound on waiting for a cancelled statement to report back.
const CANCEL_GRACE: Duration = Duration::from_secs(5);

/// A maintenance transaction whose statements are bounded by
/// `SET LOCAL statement_timeout` and can be cancelled server-side.
pub(super) struct MaintenanceTransaction {
    pub(super) backend_pid: i32,
    pub(super) timeout: Duration,
    pub(super) transaction: Transaction<'static, Postgres>,
}

impl MaintenanceTransaction {
    /// Begins a transaction, records its backend PID, and applies a
    /// transaction-local statement timeout. Nothing outlives the transaction,
    /// so pooled connections keep their role and database defaults.
    pub(super) async fn begin(
        pool: &PgPool,
        cancel: &CancellationToken,
        timeout: Duration,
    ) -> Result<Self, MaintenanceError> {
        // Dropping the begin when cancellation wins is safe: River begins on
        // its own task, which rolls the transaction back once it starts.
        let mut transaction = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(MaintenanceError::Cancelled),
            transaction = crate::database::begin_postgres(pool) => transaction?,
        };
        let backend_pid = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(MaintenanceError::Cancelled),
            row = sqlx::query_as::<_, (i32, String)>(
                "SELECT pg_backend_pid(), set_config('statement_timeout', $1, true)",
            )
            .bind(timeout_setting(timeout))
            .fetch_one(&mut *transaction) => row?.0,
        };
        Ok(Self {
            backend_pid,
            timeout,
            transaction,
        })
    }

    /// Commits under the same cancellation and deadline rules.
    pub(super) async fn commit(
        self,
        pool: &PgPool,
        cancel: &CancellationToken,
    ) -> Result<(), MaintenanceError> {
        let backend_pid = self.backend_pid;
        let timeout = self.timeout;
        cancellable(
            pool,
            backend_pid,
            cancel,
            timeout,
            self.transaction.commit(),
        )
        .await
    }
}

/// Formats a duration for `statement_timeout`, in whole milliseconds.
fn timeout_setting(timeout: Duration) -> String {
    timeout.as_millis().max(1).to_string()
}

/// Runs one statement on the connection identified by `backend_pid`.
///
/// When `cancel` fires first, the statement is cancelled server-side with
/// `pg_cancel_backend` and awaited briefly so the connection is idle again.
/// A server that never answers is abandoned after the statement timeout plus a
/// grace period.
pub(super) async fn cancellable<T, E>(
    pool: &PgPool,
    backend_pid: i32,
    cancel: &CancellationToken,
    timeout: Duration,
    operation: impl Future<Output = Result<T, E>>,
) -> Result<T, MaintenanceError>
where
    MaintenanceError: From<E>,
{
    cancellable_within(
        pool,
        backend_pid,
        cancel,
        Some(timeout.saturating_add(CLIENT_TIMEOUT_GRACE)),
        operation,
    )
    .await
}

/// Like [`cancellable`], but for a statement without a server-side
/// `statement_timeout`: the statement is cancelled server-side once
/// `deadline` passes, or never when it's `None`.
pub(super) async fn cancellable_within<T, E>(
    pool: &PgPool,
    backend_pid: i32,
    cancel: &CancellationToken,
    deadline: Option<Duration>,
    operation: impl Future<Output = Result<T, E>>,
) -> Result<T, MaintenanceError>
where
    MaintenanceError: From<E>,
{
    tokio::pin!(operation);
    let deadline = async {
        match deadline {
            Some(deadline) => tokio::time::sleep(deadline).await,
            None => std::future::pending().await,
        }
    };
    let result = tokio::select! {
        biased;
        result = &mut operation => result.map_err(MaintenanceError::from),
        () = cancel.cancelled() => {
            cancel_backend(pool, backend_pid).await;
            let _ = tokio::time::timeout(CANCEL_GRACE, operation).await;
            Err(MaintenanceError::Cancelled)
        }
        () = deadline => {
            cancel_backend(pool, backend_pid).await;
            let _ = tokio::time::timeout(CANCEL_GRACE, operation).await;
            Err(MaintenanceError::TimedOut)
        }
    };
    // A completed query can report a driver error as cancellation arrives,
    // including while a timed-out query is being drained. Keep successful
    // results (especially commits), but don't report shutdown as a failure.
    if result.is_err() && cancel.is_cancelled() {
        Err(MaintenanceError::Cancelled)
    } else {
        result
    }
}

/// Asks Postgres to cancel the statement running on `backend_pid`.
pub(super) async fn cancel_backend(pool: &PgPool, backend_pid: i32) {
    let result = tokio::time::timeout(
        CANCEL_GRACE,
        sqlx::query("SELECT pg_cancel_backend($1)")
            .bind(backend_pid)
            .execute(pool),
    )
    .await;
    match result {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            debug!(error = %crate::error::Chain(&error), backend_pid, "River could not cancel a maintenance statement");
        }
        Err(_) => {
            debug!(
                backend_pid,
                "River timed out cancelling a maintenance statement"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;
    use tokio_util::sync::CancellationToken;

    use super::{MaintenanceError, cancellable_within};

    #[tokio::test]
    async fn completed_query_error_during_cancellation_is_cancelled() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result = cancellable_within(&pool, 0, &cancel, None, async {
            Err::<(), _>(sqlx::Error::PoolClosed)
        })
        .await;

        assert!(matches!(result, Err(MaintenanceError::Cancelled)));
    }

    #[tokio::test]
    async fn completed_query_error_without_cancellation_is_reported() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        let cancel = CancellationToken::new();

        let result = cancellable_within(&pool, 0, &cancel, None, async {
            Err::<(), _>(sqlx::Error::PoolClosed)
        })
        .await;

        assert!(matches!(result, Err(MaintenanceError::Failed(_))));
    }

    #[tokio::test]
    async fn completed_query_success_during_cancellation_is_preserved() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result =
            cancellable_within(&pool, 0, &cancel, None, async { Ok::<_, sqlx::Error>(42) }).await;

        assert_eq!(result.unwrap(), 42);
    }
}
