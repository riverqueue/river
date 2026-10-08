//! Batched persistence of job completions.
//!
//! This mirrors River Go's `BatchCompleter`: results accumulate briefly and are
//! written with one set-state-if-running statement per batch. A database write
//! is retried with backoff, and a batch that still fails stays queued and is
//! retried again rather than being dropped, so a transient error cannot leave
//! successfully worked jobs `running` until the rescuer runs them again.

use std::collections::HashMap;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::FutureExt as _;
use serde_json::{Map, Value};
#[cfg(feature = "postgres")]
use sqlx::AssertSqlSafe;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgRow;
#[cfg(feature = "postgres")]
use sqlx::types::Json;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::__private::{DatabaseConnection as PilotDatabaseConnection, JobSetStateParams};
use crate::client::attempts::remove_running_attempt;
use crate::client::backoff::exponential_backoff;
#[cfg(feature = "sqlite")]
use crate::client::sqlite_backend_error;
use crate::client::{ClientInner, RuntimeNotification, peers, tolerant_row};
#[cfg(feature = "postgres")]
use crate::client::{decode_job_row, job_projection};
use crate::database::{DatabaseKind, DatabasePool};
use crate::{AttemptError, Error, Event, JobEventKind, JobRow, JobState, JobStatistics};

/// Most updates written by one statement, matching River Go.
pub(super) const COMPLETION_BATCH_SIZE: usize = 5_000;
/// How long sparse results coalesce before they are written.
const COMPLETION_BATCH_DELAY: Duration = Duration::from_millis(10);
/// Ready and deferred updates held before the batcher stops accepting more.
/// The bounded channel then applies backpressure to workers, as River Go's
/// backlog wait does.
pub(super) const COMPLETION_BACKLOG_LIMIT: usize = COMPLETION_BATCH_SIZE * 2;
/// Most concurrent batch writes River OSS issues on Postgres.
#[cfg(feature = "postgres")]
const COMPLETION_POSTGRES_CONCURRENCY: usize = 2;
/// Attempts in one retry cycle, matching River Go's `numRetries`.
const COMPLETION_RETRY_ATTEMPTS: u32 = 3;
/// Per-attempt timeout, matching River Go's `HotOperationTimeout`.
pub(super) const HOT_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);

/// One requested job state transition.
///
/// The fields follow River Go's `JobSetStateIfRunningParams`: `None` leaves
/// the corresponding column unchanged, and metadata is merged only when it is
/// not empty.
pub(super) struct CompletionUpdate {
    /// Replacement attempt, sent only for snoozes and shutdown interrupts,
    /// which return an attempt that should not count.
    pub(super) attempt: Option<i16>,
    pub(super) cancellation: CancellationToken,
    pub(super) error: Option<AttemptError>,
    pub(super) event_kind: JobEventKind,
    pub(super) finalized_at: Option<DateTime<Utc>>,
    pub(super) job_id: i64,
    pub(super) metadata: Map<String, Value>,
    pub(super) scheduled_at: Option<DateTime<Utc>>,
    pub(super) state: JobState,
    pub(super) timing: CompletionTiming,
    /// Set for a peer attempt's outcome, whose ownership ends as it persists.
    pub(super) peer: Option<Arc<peers::PeerCompletion>>,
}

/// Maps a persisted row to the event it reports.
///
/// Returns `None` for `pending` and `running`: the row was moved out of the
/// requested transition by someone else (an operator, an extension, or a
/// newer attempt after a rescue), so reporting a completion would be wrong.
pub(super) fn persisted_completion_event_kind(
    state: JobState,
    requested: JobEventKind,
) -> Option<JobEventKind> {
    Some(match state {
        JobState::Available => match requested {
            JobEventKind::Failed | JobEventKind::Interrupted | JobEventKind::Snoozed => requested,
            JobEventKind::Cancelled | JobEventKind::Completed => JobEventKind::Failed,
        },
        JobState::Cancelled => JobEventKind::Cancelled,
        JobState::Completed => JobEventKind::Completed,
        JobState::Discarded | JobState::Retryable => JobEventKind::Failed,
        JobState::Scheduled => JobEventKind::Snoozed,
        JobState::Pending | JobState::Running => return None,
    })
}

pub(super) struct CompletionAttempt {
    pub(super) cancellation: CancellationToken,
    pub(super) timing: CompletionTiming,
}

#[derive(Clone, Copy)]
pub(super) struct CompletionTiming {
    pub(super) completion_started: std::time::Instant,
    pub(super) queue_wait_duration: Duration,
    pub(super) run_duration: Duration,
}

/// Persists completions until every sender is dropped and all accepted
/// updates were written or abandoned.
pub(super) async fn run_completion_batcher(
    inner: Arc<ClientInner>,
    receiver: mpsc::Receiver<CompletionUpdate>,
) -> Result<(), Error> {
    CompletionBatcher::new(inner).run(receiver).await;
    Ok(())
}

type BatchOutcome = (Vec<CompletionUpdate>, Result<Vec<JobRow>, Error>);

pub(super) struct CompletionBatcher {
    /// Updates accepted while a batch containing the same job is in flight.
    /// They replace the in-flight update's successor once it finishes.
    deferred: HashMap<i64, CompletionUpdate>,
    /// Whether ready updates may be written without waiting for a full batch.
    flush_due: bool,
    in_flight: HashSet<i64>,
    inner: Arc<ClientInner>,
    max_concurrency: usize,
    ready: HashMap<i64, CompletionUpdate>,
    ready_order: VecDeque<i64>,
    /// Set once a batch fails during shutdown; remaining updates are then
    /// abandoned without being attempted, like River Go's stop path, which
    /// stops flushing at the first error.
    stop_retrying: bool,
    tasks: JoinSet<BatchOutcome>,
    task_ids: HashMap<tokio::task::Id, Vec<i64>>,
}

impl CompletionBatcher {
    pub(super) fn new(inner: Arc<ClientInner>) -> Self {
        let backend_concurrency = match inner.database.kind() {
            #[cfg(feature = "postgres")]
            DatabaseKind::Postgres => COMPLETION_POSTGRES_CONCURRENCY,
            #[cfg(feature = "sqlite")]
            DatabaseKind::Sqlite => 1,
        };
        // Like River Go's `completionConcurrency`, an intercepting extension
        // can only lower the backend's limit.
        let max_concurrency = if inner.pilot.intercepts_job_set_state() {
            backend_concurrency.min(inner.pilot.job_set_state_concurrency().max(1))
        } else {
            backend_concurrency
        };
        Self {
            deferred: HashMap::new(),
            flush_due: false,
            in_flight: HashSet::new(),
            inner,
            max_concurrency,
            ready: HashMap::new(),
            ready_order: VecDeque::new(),
            stop_retrying: false,
            tasks: JoinSet::new(),
            task_ids: HashMap::new(),
        }
    }

    fn backlog(&self) -> usize {
        self.ready.len() + self.deferred.len()
    }

    /// Drops an update that will never be written, releasing its attempt.
    fn discard(&self, update: &CompletionUpdate) {
        remove_running_attempt(&self.inner.running, update.job_id, &update.cancellation);
    }

    fn enqueue(&mut self, update: CompletionUpdate) {
        if self.in_flight.contains(&update.job_id) {
            if let Some(superseded) = self.deferred.insert(update.job_id, update) {
                self.discard(&superseded);
            }
            return;
        }
        self.enqueue_ready(update);
    }

    /// Adds an update to the ready set. A newer update for the same job (from a
    /// later attempt after a rescue) supersedes an older unwritten one.
    fn enqueue_ready(&mut self, update: CompletionUpdate) {
        let job_id = update.job_id;
        match self.ready.insert(job_id, update) {
            Some(superseded) => self.discard(&superseded),
            None => self.ready_order.push_back(job_id),
        }
    }

    fn finish(&mut self, joined: Result<(tokio::task::Id, BatchOutcome), tokio::task::JoinError>) {
        let (task_id, (batch, result)) = match joined {
            Ok(joined) => joined,
            Err(join_error) => {
                // Batch tasks catch panics, so this only happens when the
                // runtime is shutting down. Release the batch's jobs so any
                // deferred successors are not stranded behind it.
                error!(error = %join_error, "River completion batch task stopped");
                for job_id in self.task_ids.remove(&join_error.id()).unwrap_or_default() {
                    self.release(job_id);
                }
                return;
            }
        };
        self.task_ids.remove(&task_id);
        match result {
            Ok(rows) => {
                let mut rows = rows
                    .into_iter()
                    .map(|row| (row.id, row))
                    .collect::<HashMap<_, _>>();
                for update in &batch {
                    finish_batched_completion(&self.inner, update, rows.remove(&update.job_id));
                }
                for update in batch {
                    self.release(update.job_id);
                }
            }
            Err(error) if self.stop_retrying || is_non_retryable_completion_error(&error) => {
                error!(
                    error = %crate::error::Chain(&error),
                    num_jobs = batch.len(),
                    "River could not persist job completions; the rescuer will retry them"
                );
                for update in batch {
                    self.discard(&update);
                    self.release(update.job_id);
                }
            }
            Err(error) => {
                debug!(
                    error = %crate::error::Chain(&error),
                    num_jobs = batch.len(),
                    "requeued River completion batch after repeated errors"
                );
                for update in batch {
                    self.in_flight.remove(&update.job_id);
                    match self.deferred.remove(&update.job_id) {
                        Some(newer) => {
                            self.discard(&update);
                            self.enqueue_ready(newer);
                        }
                        None => self.enqueue_ready(update),
                    }
                }
                self.flush_due = true;
            }
        }
    }

    /// Discards every update not yet written: ready ones, and any still
    /// queued in `receiver`, whose senders are all gone once shutdown
    /// abandons the backlog. Returns whether `receiver` is exhausted.
    fn abandon_backlog(&mut self, receiver: &mut mpsc::Receiver<CompletionUpdate>) -> bool {
        let mut abandoned = 0_usize;
        for job_id in std::mem::take(&mut self.ready_order) {
            if let Some(update) = self.ready.remove(&job_id) {
                self.discard(&update);
                abandoned += 1;
            }
        }
        let exhausted = loop {
            match receiver.try_recv() {
                Ok(update) => {
                    self.discard(&update);
                    abandoned += 1;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => break true,
                Err(mpsc::error::TryRecvError::Empty) => break false,
            }
        };
        if abandoned > 0 {
            error!(
                num_jobs = abandoned,
                "River client stopping after a completion error; abandoning unwritten job completions for the rescuer"
            );
        }
        exhausted
    }

    /// Marks a job's batch finished and promotes a deferred successor.
    fn release(&mut self, job_id: i64) {
        self.in_flight.remove(&job_id);
        if let Some(deferred) = self.deferred.remove(&job_id) {
            self.enqueue_ready(deferred);
            self.flush_due = true;
        }
    }

    async fn run(mut self, mut receiver: mpsc::Receiver<CompletionUpdate>) {
        let mut accepting = true;
        let coalesce = tokio::time::sleep(Duration::ZERO);
        tokio::pin!(coalesce);
        let mut coalescing = false;
        loop {
            if self.stop_retrying && self.abandon_backlog(&mut receiver) {
                accepting = false;
            }
            self.start_ready_batches(!accepting);
            if self.ready.is_empty() {
                self.flush_due = false;
            }
            if !accepting && self.tasks.is_empty() && self.ready.is_empty() {
                break;
            }
            let receiving = accepting && self.backlog() < COMPLETION_BACKLOG_LIMIT;
            tokio::select! {
                update = receiver.recv(), if receiving => match update {
                    Some(update) => {
                        self.enqueue(update);
                        if !coalescing && !self.flush_due {
                            coalesce
                                .as_mut()
                                .reset(tokio::time::Instant::now() + COMPLETION_BATCH_DELAY);
                            coalescing = true;
                        }
                    }
                    None => accepting = false,
                },
                joined = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                    if let Some(joined) = joined {
                        // Every sender is gone once the client is stopping,
                        // even while a full backlog keeps the batcher from
                        // receiving the channel's end, which an outage
                        // would otherwise postpone until the database
                        // returns.
                        let stopping = !accepting || receiver.is_closed();
                        if stopping && joined.as_ref().is_ok_and(|(_, (_, result))| result.is_err()) {
                            self.stop_retrying = true;
                        }
                        self.finish(joined);
                    }
                }
                () = &mut coalesce, if coalescing => {
                    coalescing = false;
                    self.flush_due = true;
                }
            }
        }
    }

    fn spawn_batch(&mut self) -> bool {
        let mut batch = Vec::with_capacity(self.ready.len().min(COMPLETION_BATCH_SIZE));
        while batch.len() < COMPLETION_BATCH_SIZE {
            let Some(job_id) = self.ready_order.pop_front() else {
                break;
            };
            if let Some(update) = self.ready.remove(&job_id) {
                self.in_flight.insert(job_id);
                batch.push(update);
            }
        }
        if batch.is_empty() {
            return false;
        }
        let job_ids = batch.iter().map(|update| update.job_id).collect();
        let inner = Arc::clone(&self.inner);
        let handle = self.tasks.spawn(async move {
            // Keep the batch even if persistence panics so its jobs are
            // retried rather than silently lost.
            let result = std::panic::AssertUnwindSafe(persist_with_retries(&inner, &batch))
                .catch_unwind()
                .await
                .unwrap_or_else(|panic| {
                    Err(Error::runtime_context(
                        "job completion",
                        format!(
                            "completion persistence panicked: {}",
                            crate::error::panic_message(&panic)
                        ),
                    ))
                });
            if let Ok(rows) = &result {
                notify_interrupted_jobs(&inner, &batch, rows).await;
            }
            (batch, result)
        });
        self.task_ids.insert(handle.id(), job_ids);
        true
    }

    /// Starts as many batches as policy allows.
    ///
    /// A single writer takes whatever is ready once the coalescing delay has
    /// passed. A second concurrent writer is worthwhile only for a full batch,
    /// which keeps sparse workloads at one query at a time like River Go.
    fn start_ready_batches(&mut self, draining: bool) {
        while self.tasks.len() < self.concurrency() && !self.ready.is_empty() {
            let full = self.ready.len() >= COMPLETION_BATCH_SIZE;
            let may_start = if self.tasks.is_empty() {
                full || self.flush_due || draining
            } else {
                full
            };
            if !may_start || !self.spawn_batch() {
                return;
            }
        }
    }

    pub(super) const fn concurrency(&self) -> usize {
        self.max_concurrency
    }
}

/// Whether a completion error can never succeed on retry, matching River Go's
/// `isNonRetryableCompleterError` for a closed pool.
fn is_non_retryable_completion_error(error: &Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(mut current) = source {
        // A database error is transparent, so its driver error appears in
        // the chain only through the variant.
        if let Some(Error::Database(inner)) = current.downcast_ref::<Error>() {
            current = inner;
        }
        if matches!(
            current.downcast_ref::<sqlx::Error>(),
            Some(sqlx::Error::PoolClosed)
        ) {
            return true;
        }
        source = current.source();
    }
    false
}

/// Runs a completion write up to three times with River's service backoff and
/// a per-attempt timeout, independent of client shutdown, like River Go's
/// `withRetries`. A closed pool is not retried.
pub(super) async fn with_completion_retries<T, F, Fut>(
    operation: &'static str,
    mut attempt: F,
) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
{
    let mut attempt_number = 1;
    loop {
        let error = match tokio::time::timeout(HOT_OPERATION_TIMEOUT, attempt()).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) if is_non_retryable_completion_error(&error) => return Err(error),
            Ok(Err(error)) => error,
            Err(_) => Error::runtime_context(
                operation,
                format!("attempt timed out after {HOT_OPERATION_TIMEOUT:?}"),
            ),
        };
        if attempt_number >= COMPLETION_RETRY_ATTEMPTS {
            error!(
                attempt = attempt_number,
                error = %crate::error::Chain(&error),
                operation,
                "River completer error; too many errors, giving up on this attempt cycle"
            );
            return Err(error);
        }
        let sleep = exponential_backoff(attempt_number);
        error!(
            attempt = attempt_number,
            error = %crate::error::Chain(&error),
            operation,
            sleep_duration = ?sleep,
            "River completer error (will retry after sleep)"
        );
        tokio::time::sleep(sleep).await;
        attempt_number += 1;
    }
}

async fn persist_with_retries(
    inner: &ClientInner,
    batch: &[CompletionUpdate],
) -> Result<Vec<JobRow>, Error> {
    with_completion_retries("job completion", || persist_completion_batch(inner, batch)).await
}

/// Wakes producers for jobs that client shutdown returned to `available`, so
/// another client picks them up without waiting for its next poll. SQLite
/// writes its durable wakeup inside the completion transaction instead.
#[cfg_attr(
    not(feature = "postgres"),
    allow(clippy::unused_async, reason = "only Postgres sends a notification")
)]
async fn notify_interrupted_jobs(inner: &ClientInner, batch: &[CompletionUpdate], rows: &[JobRow]) {
    let interrupted = batch
        .iter()
        .filter(|update| update.event_kind == JobEventKind::Interrupted)
        .map(|update| update.job_id)
        .collect::<HashSet<_>>();
    if interrupted.is_empty() {
        return;
    }
    let queues = rows
        .iter()
        .filter(|row| interrupted.contains(&row.id) && row.state == JobState::Available)
        .map(|row| row.queue.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for queue in &queues {
        let _ = inner
            .queue_notifications
            .send(RuntimeNotification::Insert((*queue).to_owned()));
    }
    match inner.database.pool() {
        // SQLite clients wrote these notifications with the completion.
        #[cfg(feature = "sqlite")]
        DatabasePool::Sqlite(_) => {}
        #[cfg(feature = "postgres")]
        DatabasePool::Postgres(pool) => {
            match inner.postgres_capabilities(pool).await {
                Ok(capabilities) if capabilities.supports_listen_notify => {}
                Ok(_) => return,
                Err(error) => {
                    debug!(error = %crate::error::Chain(&error), "could not notify peers about interrupted River jobs");
                    return;
                }
            }
            for queue in queues {
                if let Err(error) = sqlx::query(
                    "SELECT pg_notify(concat(coalesce($1::text, current_schema()), '.', $2::text), json_build_object('queue', $3::text)::text)",
                )
                .bind(inner.schema.as_deref())
                .bind(crate::NOTIFICATION_TOPIC_INSERT)
                .bind(queue)
                .execute(pool)
                .await
                {
                    debug!(error = %crate::error::Chain(&error), queue, "could not notify peers about interrupted River jobs");
                }
            }
        }
    }
}

/// Applies a batch of state transitions, returning the resulting row for every
/// job that still exists.
///
/// This is River Go's `JobSetStateIfRunningMany`: a job that is still running
/// takes the requested state (or `cancelled` when a cancellation was attempted
/// and the job would otherwise run again). A job that is no longer running
/// keeps its state, but still receives non-empty metadata updates, and its
/// current row is returned so its event reflects the state that won.
#[allow(
    clippy::too_many_lines,
    reason = "keeps Postgres batch and transactionally equivalent SQLite completion together"
)]
pub(super) async fn persist_completion_batch(
    inner: &ClientInner,
    batch: &[CompletionUpdate],
) -> Result<Vec<JobRow>, Error> {
    match inner.database.pool() {
        #[cfg(feature = "sqlite")]
        DatabasePool::Sqlite(pool) => {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let mut rows = Vec::with_capacity(batch.len());
            let now = Utc::now();
            for update in batch {
                let row = crate::database::sqlite::complete_decoded(
                    &mut transaction,
                    &crate::database::sqlite::CompleteJob {
                        attempt: update.attempt,
                        error: update.error.as_ref(),
                        finalized_at: update.finalized_at,
                        id: update.job_id,
                        metadata_updates: (!update.metadata.is_empty()).then_some(&update.metadata),
                        now,
                        scheduled_at: update.scheduled_at,
                        state: update.state,
                    },
                )
                .await
                .map_err(sqlite_backend_error)?;
                let row = match row {
                    Some(row) => Some(row),
                    None => crate::database::sqlite::merge_metadata_if_not_running(
                        &mut transaction,
                        update.job_id,
                        &update.metadata,
                    )
                    .await
                    .map_err(sqlite_backend_error)?,
                };
                rows.extend(row.and_then(tolerant_row));
            }
            let interrupted_queues = rows
                .iter()
                .filter(|row| row.state == JobState::Available)
                .filter(|row| {
                    batch.iter().any(|update| {
                        update.job_id == row.id && update.event_kind == JobEventKind::Interrupted
                    })
                })
                .map(|row| row.queue.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            for queue in interrupted_queues {
                let payload = serde_json::json!({ "queue": queue }).to_string();
                crate::database::sqlite::notification_insert(
                    &mut transaction,
                    &[crate::database::sqlite::NotificationInput {
                        payload: &payload,
                        topic: crate::NOTIFICATION_TOPIC_INSERT,
                    }],
                )
                .await
                .map_err(sqlite_backend_error)?;
            }
            if inner.pilot.intercepts_job_set_state() {
                after_jobs_set_state(
                    inner,
                    PilotDatabaseConnection::Sqlite(&mut transaction),
                    &batch.iter().map(|update| update.job_id).collect::<Vec<_>>(),
                    &rows,
                )
                .await?;
            }
            transaction.commit().await?;
            Ok(rows)
        }
        #[cfg(feature = "postgres")]
        DatabasePool::Postgres(pool) => {
            let attempt_do_update = batch
                .iter()
                .map(|update| update.attempt.is_some())
                .collect::<Vec<_>>();
            let attempts = batch
                .iter()
                .map(|update| update.attempt.unwrap_or_default())
                .collect::<Vec<_>>();
            let errors = batch
                .iter()
                .map(|update| update.error.as_ref().map(Json))
                .collect::<Vec<_>>();
            let finalized_at = batch
                .iter()
                .map(|update| update.finalized_at)
                .collect::<Vec<_>>();
            let ids = batch.iter().map(|update| update.job_id).collect::<Vec<_>>();
            let metadata_do_merge = batch
                .iter()
                .map(|update| !update.metadata.is_empty())
                .collect::<Vec<_>>();
            let metadata = batch
                .iter()
                .map(|update| Json(&update.metadata))
                .collect::<Vec<_>>();
            let scheduled_at = batch
                .iter()
                .map(|update| update.scheduled_at)
                .collect::<Vec<_>>();
            let states = batch
                .iter()
                .map(|update| update.state.as_str())
                .collect::<Vec<_>>();
            let table = inner.schema.qualify("river_job");
            let state_type = inner.schema.qualify("river_job_state");
            let should_cancel = "(job_input.state IN ('available', 'retryable', 'scheduled') \
                                 AND job.metadata ? 'cancel_attempted_at')";
            let sql = format!(
                "WITH job_input AS (\
                    SELECT * FROM unnest(\
                        $1::bigint[], $2::boolean[], $3::smallint[], $4::jsonb[], \
                        $5::timestamptz[], $6::boolean[], $7::jsonb[], $8::timestamptz[], $9::text[]\
                    ) AS job_input(\
                        id, attempt_do_update, attempt, errors, finalized_at, \
                        metadata_do_merge, metadata_updates, scheduled_at, state)\
                 ), updated AS (\
                    UPDATE {table} AS job SET \
                        attempt = CASE WHEN job.state = 'running' AND NOT {should_cancel} \
                            AND job_input.attempt_do_update \
                            THEN job_input.attempt ELSE job.attempt END, \
                        errors = CASE WHEN job.state = 'running' AND job_input.errors IS NOT NULL \
                            THEN array_append(coalesce(job.errors, '{{}}'), job_input.errors) \
                            ELSE job.errors END, \
                        finalized_at = CASE WHEN job.state = 'running' AND {should_cancel} THEN now() \
                            WHEN job.state = 'running' AND job_input.finalized_at IS NOT NULL \
                            THEN job_input.finalized_at ELSE job.finalized_at END, \
                        metadata = CASE WHEN job_input.metadata_do_merge \
                            THEN job.metadata || job_input.metadata_updates ELSE job.metadata END, \
                        scheduled_at = CASE WHEN job.state = 'running' AND NOT {should_cancel} \
                            AND job_input.scheduled_at IS NOT NULL \
                            THEN job_input.scheduled_at ELSE job.scheduled_at END, \
                        state = CASE WHEN job.state = 'running' AND {should_cancel} \
                            THEN 'cancelled'::{state_type} \
                            WHEN job.state = 'running' THEN job_input.state::{state_type} \
                            ELSE job.state END \
                    FROM job_input \
                    WHERE job.id = job_input.id \
                        AND (job.state = 'running' OR job_input.metadata_do_merge) \
                    RETURNING job.*\
                 ) \
                 SELECT {projection}, false AS unique_skipped_as_duplicate \
                 FROM {table} AS job JOIN job_input ON job.id = job_input.id \
                 WHERE NOT EXISTS (SELECT 1 FROM updated WHERE updated.id = job.id) \
                 UNION ALL \
                 SELECT {projection}, false AS unique_skipped_as_duplicate FROM updated AS job",
                projection = job_projection("job"),
            );
            let query = sqlx::query(AssertSqlSafe(sql))
                .bind(ids)
                .bind(attempt_do_update)
                .bind(attempts)
                .bind(errors)
                .bind(finalized_at)
                .bind(metadata_do_merge)
                .bind(metadata)
                .bind(scheduled_at)
                .bind(states);
            if inner.pilot.intercepts_job_set_state() {
                let mut transaction = crate::database::begin_postgres(pool).await?;
                let rows = decode_completion_rows(&query.fetch_all(&mut *transaction).await?);
                after_jobs_set_state(
                    inner,
                    PilotDatabaseConnection::Postgres(&mut transaction),
                    &batch.iter().map(|update| update.job_id).collect::<Vec<_>>(),
                    &rows,
                )
                .await?;
                transaction.commit().await?;
                return Ok(rows);
            }
            Ok(decode_completion_rows(&query.fetch_all(pool).await?))
        }
    }
}

/// Decodes rows returned by a completion. Like River Go's
/// `JobSetStateIfRunningMany`, a row that can't be fully decoded is still
/// returned with its undecodable fields left empty, so its state change is
/// reported like any other.
#[cfg(feature = "postgres")]
fn decode_completion_rows(records: &[PgRow]) -> Vec<JobRow> {
    records
        .iter()
        .filter_map(|row| tolerant_row(decode_job_row(row)))
        .collect()
}

/// Calls the extension hook for rows updated in the current transaction.
pub(crate) async fn after_jobs_set_state(
    inner: &ClientInner,
    connection: PilotDatabaseConnection<'_>,
    job_ids: &[i64],
    rows: &[JobRow],
) -> Result<(), Error> {
    let params = JobSetStateParams {
        database: inner.pilot_database_config(),
        job_ids,
        jobs: rows,
    };
    inner
        .pilot
        .after_jobs_set_state(connection, &params)
        .await
        .map_err(|source| Error::Extension {
            phase: crate::ExtensionPhase::AddOn {
                operation: "job set state",
            },
            source,
        })
}

pub(super) fn finish_batched_completion(
    inner: &ClientInner,
    update: &CompletionUpdate,
    record: Option<JobRow>,
) {
    // A stale result settles the peer's ownership just as an applied one
    // does, and before its event, so the job can be claimed again at once.
    if let Some(peer) = &update.peer {
        peer.persisted(inner);
    }
    if let Some(row) = record {
        if let Some(event_kind) = persisted_completion_event_kind(row.state, update.event_kind) {
            let event = Event::job_with_statistics(
                event_kind,
                row,
                JobStatistics {
                    complete_duration: update.timing.completion_started.elapsed(),
                    queue_wait_duration: update.timing.queue_wait_duration,
                    run_duration: update.timing.run_duration,
                },
            );
            let _ = inner.events.send(event);
        } else {
            debug!(
                job_id = update.job_id,
                state = row.state.as_str(),
                "job result ignored because the job was moved back to a non-final state"
            );
        }
    } else {
        debug!(
            job_id = update.job_id,
            "job result ignored because the job no longer exists"
        );
    }
    remove_running_attempt(&inner.running, update.job_id, &update.cancellation);
}
