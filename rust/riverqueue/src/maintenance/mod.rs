//! Leader election and leader-owned database maintenance.
//!
//! The structure mirrors Go's `internal/leadership` and
//! `internal/maintenance` packages:
//!
//! - An elector task renews leadership on its own schedule, bounds every
//!   attempt by a deadline and by the locally trusted remainder of the current
//!   term, and guards renewal and resignation with the term's `elected_at`.
//! - A maintainer task (Go's `QueueMaintainerLeader`) starts every
//!   leader-owned service in its own task under a per-term cancellation token.
//!   The elector cancels that token the moment leadership is lost or its trust
//!   window elapses, and a new term starts only after the previous term's
//!   services have stopped.
//! - Every maintenance database call is selected against the term token. On
//!   PostgreSQL, cancellation and timeouts are enforced server-side with
//!   `SET LOCAL statement_timeout` and `pg_cancel_backend`, so abandoned work
//!   does not keep holding locks.

mod cleaner;
mod elector;
mod maintainer;
mod periodic_enqueuer;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
mod reindexer;
mod rescuer;
mod scheduler;
#[cfg(all(test, feature = "postgres-tests"))]
mod tests;

use std::{
    hash::{BuildHasher, Hasher},
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    Error,
    client::{ClientInner, RuntimeNotification},
};

/// Batch size used by bulk maintenance services (Go `BatchSizeDefault`).
pub(crate) const BATCH_SIZE_DEFAULT: i64 = 10_000;

/// Batch size used after repeated timeouts (Go `BatchSizeReduced`).
pub(crate) const BATCH_SIZE_REDUCED: i64 = 1_000;

/// Bounds of the pause between maintenance batches (Go `BatchBackoffMin/Max`).
const BATCH_BACKOFF_MIN: Duration = Duration::from_millis(50);
const BATCH_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// Timeout for one maintenance batch (Go `riversharedmaintenance.TimeoutDefault`).
const TIMEOUT_DEFAULT: Duration = Duration::from_secs(30);

/// Maximum random delay before a service's first run (Go `StaggerStart`).
const STAGGER_MAX: Duration = Duration::from_secs(1);

/// Runs election and leader-owned maintenance until `cancel` fires.
pub(crate) async fn run_maintenance(
    inner: Arc<ClientInner>,
    cancel: CancellationToken,
    notifications: broadcast::Receiver<RuntimeNotification>,
) -> Result<(), Error> {
    let (wakeup_sender, wakeup_receiver) = mpsc::unbounded_channel();
    let (term_sender, term_receiver) = mpsc::unbounded_channel();
    let elector = elector::Elector::new(
        Arc::new(elector::DatabaseLeaderStore::new(Arc::clone(&inner))),
        inner.id.clone(),
        inner.maintenance.elect_interval,
    );
    let maintainer = maintainer::Maintainer::new(Arc::clone(&inner));
    tokio::join!(
        forward_leadership_notifications(notifications, wakeup_sender, cancel.clone()),
        elector.run(cancel.clone(), wakeup_receiver, term_sender),
        maintainer.run(cancel, term_receiver),
    );
    Ok(())
}

/// Leadership events the elector reacts to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LeadershipWakeup {
    /// Another client resigned, or leadership may otherwise have changed.
    Changed,
    /// Some client asked the current leader to resign.
    RequestResign,
}

/// Moves leadership notifications off the shared runtime broadcast channel.
///
/// The broadcast channel also carries insert and queue-control wakeups, so a
/// busy receiver can lag and lose messages. This task does nothing but relay
/// leadership events into an unbounded queue, and if it ever lags it still
/// emits a wakeup so the elector re-checks the lease rather than silently
/// missing a transition.
async fn forward_leadership_notifications(
    mut notifications: broadcast::Receiver<RuntimeNotification>,
    wakeups: mpsc::UnboundedSender<LeadershipWakeup>,
    cancel: CancellationToken,
) {
    loop {
        let notification = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            notification = notifications.recv() => notification,
        };
        let wakeup = match notification {
            Ok(RuntimeNotification::LeadershipChanged) => LeadershipWakeup::Changed,
            Ok(RuntimeNotification::LeadershipRequestResign) => LeadershipWakeup::RequestResign,
            Ok(RuntimeNotification::Insert(_) | RuntimeNotification::QueueControl(_)) => continue,
            Err(broadcast::error::RecvError::Lagged(count)) => {
                warn!(
                    skipped = count,
                    "River leadership relay lagged; re-checking leadership"
                );
                LeadershipWakeup::Changed
            }
            Err(broadcast::error::RecvError::Closed) => return,
        };
        if wakeups.send(wakeup).is_err() {
            return;
        }
    }
}

/// Failure of one maintenance operation.
#[derive(Debug)]
pub(crate) enum MaintenanceError {
    /// The term or client was cancelled; nothing needs to be reported.
    Cancelled,
    /// A database call exceeded its deadline.
    TimedOut,
    /// Any other failure.
    Failed(Error),
}

impl std::fmt::Display for MaintenanceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("cancelled"),
            Self::TimedOut => formatter.write_str("timed out"),
            Self::Failed(error) => error.fmt(formatter),
        }
    }
}

impl From<Error> for MaintenanceError {
    fn from(error: Error) -> Self {
        Self::Failed(error)
    }
}

impl From<sqlx::Error> for MaintenanceError {
    fn from(error: sqlx::Error) -> Self {
        // PostgreSQL reports a `statement_timeout` expiry as `query_canceled`.
        // Explicit cancellation through `pg_cancel_backend` is reported as
        // `Cancelled` before a database error is ever mapped.
        if let sqlx::Error::Database(database_error) = &error
            && database_error.code().as_deref() == Some("57014")
        {
            return Self::TimedOut;
        }
        Self::Failed(error.into())
    }
}

#[cfg(feature = "sqlite")]
impl From<crate::database::sqlite::BackendError> for MaintenanceError {
    fn from(error: crate::database::sqlite::BackendError) -> Self {
        Self::Failed(Error::Database(Box::new(error)))
    }
}

/// Batch sizes of one maintenance service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BatchSizes {
    pub(crate) default: i64,
    pub(crate) reduced: i64,
}

impl Default for BatchSizes {
    fn default() -> Self {
        Self {
            default: BATCH_SIZE_DEFAULT,
            reduced: BATCH_SIZE_REDUCED,
        }
    }
}

/// Go's reduced-batch circuit breaker: three timeouts within ten minutes
/// switch a service to its reduced batch size for the life of the client.
#[derive(Debug)]
pub(crate) struct ReducedBatchBreaker {
    open: bool,
    sizes: BatchSizes,
    trips: Vec<tokio::time::Instant>,
}

impl ReducedBatchBreaker {
    const LIMIT: usize = 3;
    const WINDOW: Duration = Duration::from_mins(10);

    fn new(sizes: BatchSizes) -> Self {
        Self {
            open: false,
            sizes,
            trips: Vec::new(),
        }
    }

    fn batch_size(&self) -> i64 {
        if self.open {
            self.sizes.reduced
        } else {
            self.sizes.default
        }
    }

    fn reset_if_not_open(&mut self) {
        if !self.open {
            self.trips.clear();
        }
    }

    fn trip(&mut self) {
        if self.open {
            return;
        }
        let now = tokio::time::Instant::now();
        self.trips
            .retain(|trip| now.saturating_duration_since(*trip) <= Self::WINDOW);
        self.trips.push(now);
        if self.trips.len() >= Self::LIMIT {
            self.open = true;
        }
    }
}

/// Breakers shared by every term of one running client, like Go's services
/// which keep their breakers across leadership changes.
#[derive(Debug)]
pub(crate) struct Breakers {
    pub(crate) job_cleaner: Mutex<ReducedBatchBreaker>,
    pub(crate) queue_cleaner: Mutex<ReducedBatchBreaker>,
    pub(crate) rescuer: Mutex<ReducedBatchBreaker>,
    pub(crate) scheduler: Mutex<ReducedBatchBreaker>,
}

impl Breakers {
    fn new(sizes: BatchSizes) -> Self {
        Self {
            job_cleaner: Mutex::new(ReducedBatchBreaker::new(sizes)),
            queue_cleaner: Mutex::new(ReducedBatchBreaker::new(sizes)),
            rescuer: Mutex::new(ReducedBatchBreaker::new(sizes)),
            scheduler: Mutex::new(ReducedBatchBreaker::new(sizes)),
        }
    }
}

/// Runs one batch-oriented maintenance operation, feeding its outcome into
/// the service's breaker.
fn batch_size(breaker: &Mutex<ReducedBatchBreaker>) -> i64 {
    breaker
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .batch_size()
}

fn record_batch<T>(breaker: &Mutex<ReducedBatchBreaker>, result: &Result<T, MaintenanceError>) {
    let mut breaker = breaker
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match result {
        Ok(_) => breaker.reset_if_not_open(),
        Err(MaintenanceError::TimedOut) => breaker.trip(),
        Err(_) => {}
    }
}

/// Returns a random `u64` for jitter and identifiers. Not cryptographic.
pub(crate) fn random_u64() -> u64 {
    // `RandomState` is seeded randomly per process and advanced per instance.
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    hasher.finish()
}

/// Returns a uniformly distributed duration in `[minimum, maximum)`.
pub(crate) fn random_duration(minimum: Duration, maximum: Duration) -> Duration {
    if maximum <= minimum {
        return minimum;
    }
    let span_nanos = u64::try_from(maximum.saturating_sub(minimum).as_nanos()).unwrap_or(u64::MAX);
    minimum + Duration::from_nanos(random_u64() % span_nanos.max(1))
}

/// Go's `serviceutil.ExponentialBackoff`: `2^(attempt-1)` seconds with +/-10%
/// jitter, restarting after `max_attempts_before_reset` attempts.
pub(crate) fn exponential_backoff(attempt: u32, max_attempts_before_reset: u32) -> Duration {
    let attempt = attempt.saturating_sub(1) % max_attempts_before_reset.max(1);
    let seconds = f64::from(2_u32.saturating_pow(attempt));
    let jitter = random_duration(Duration::ZERO, Duration::from_secs(1)).as_secs_f64();
    Duration::from_secs_f64(seconds + seconds * (jitter * 0.2 - 0.1))
}

/// Sleeps unless cancellation happens first. Returns `false` when cancelled.
pub(crate) async fn sleep_cancellable(cancel: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        biased;
        () = cancel.cancelled() => false,
        () = tokio::time::sleep(duration) => true,
    }
}

/// Pauses between batches of a large maintenance backlog.
async fn batch_backoff(cancel: &CancellationToken) -> Result<(), MaintenanceError> {
    if sleep_cancellable(
        cancel,
        random_duration(BATCH_BACKOFF_MIN, BATCH_BACKOFF_MAX),
    )
    .await
    {
        Ok(())
    } else {
        Err(MaintenanceError::Cancelled)
    }
}

/// Runs a SQLite operation under the term token and a client-side deadline.
///
/// SQLite executes statements on the client, so dropping the future cannot
/// leave server-side work holding locks the way it can on PostgreSQL.
#[cfg(feature = "sqlite")]
async fn sqlite_cancellable<T, E>(
    cancel: &CancellationToken,
    timeout: Duration,
    operation: impl Future<Output = Result<T, E>>,
) -> Result<T, MaintenanceError>
where
    MaintenanceError: From<E>,
{
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(MaintenanceError::Cancelled),
        result = tokio::time::timeout(timeout, operation) => match result {
            Ok(result) => result.map_err(MaintenanceError::from),
            Err(_) => Err(MaintenanceError::TimedOut),
        },
    }
}

#[cfg(test)]
mod unit_tests {
    use super::{BatchSizes, ReducedBatchBreaker, exponential_backoff};

    #[tokio::test(start_paused = true)]
    async fn reduced_batch_breaker_opens_after_three_timeouts_in_ten_minutes() {
        let sizes = BatchSizes {
            default: 10,
            reduced: 2,
        };
        let mut breaker = ReducedBatchBreaker::new(sizes);
        breaker.trip();
        breaker.trip();
        // A success between failures resets the count.
        breaker.reset_if_not_open();
        breaker.trip();
        breaker.trip();
        assert_eq!(breaker.batch_size(), 10);
        // Trips older than the window no longer count.
        tokio::time::advance(std::time::Duration::from_mins(11)).await;
        breaker.trip();
        assert_eq!(breaker.batch_size(), 10);
        breaker.trip();
        breaker.trip();
        assert_eq!(breaker.batch_size(), 2);
        // Once open, the breaker stays open.
        breaker.reset_if_not_open();
        assert_eq!(breaker.batch_size(), 2);
    }

    #[test]
    fn exponential_backoff_matches_go_schedule() {
        for (attempt, seconds) in [(1, 1.0), (2, 2.0), (3, 4.0), (7, 64.0), (8, 1.0)] {
            let backoff = exponential_backoff(attempt, 7).as_secs_f64();
            assert!(
                (seconds * 0.9..=seconds * 1.1).contains(&backoff),
                "attempt {attempt}: {backoff}"
            );
        }
    }
}
