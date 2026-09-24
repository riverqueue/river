//! Execution of individual job attempts.

#[allow(clippy::wildcard_imports)]
use super::*;

/// Runs one claimed job's attempt and persists its result.
///
/// `decode_error` is set for a job whose row couldn't be fully decoded, in
/// which case `row` holds only the fields that could be. Like River Go, such a
/// job isn't worked: its attempt fails with the decode error before hooks or
/// middleware run, the same way as an unknown job kind, and goes through
/// ordinary error handling. The error handler sees the partial row, and the
/// job is retried with the client's retry policy or discarded at its maximum
/// attempts.
#[allow(clippy::too_many_lines)]
pub(super) async fn execute_job(
    inner: Arc<ClientInner>,
    row: JobRow,
    decode_error: Option<String>,
    hard_cancel: CancellationToken,
    cancellation: CancellationToken,
    completion_sender: mpsc::Sender<CompletionUpdate>,
    worker_permit: OwnedSemaphorePermit,
) {
    let span = info_span!("river_job", job_id = row.id, job_kind = %row.kind, queue = %row.queue);
    async move {
        let queue_wait_duration = row
            .attempted_at
            .and_then(|attempted_at| {
                (attempted_at - row.scheduled_at.max(row.created_at))
                    .to_std()
                    .ok()
            })
            .unwrap_or_default();
        let context = WorkContext::for_job(
            Client {
                inner: Arc::clone(&inner),
            },
            cancellation.clone(),
            row.id,
            &row.metadata,
        );
        // Like River Go's executor start time, which it records as the
        // attempt error's `at`.
        let attempt_started_at = Utc::now();
        let work_started = std::time::Instant::now();
        let mut cancellation_cause = None;
        let worked = decode_error.is_none();
        let result = match decode_error {
            Some(decode_error) => {
                error!(error = %decode_error, "River job row couldn't be decoded; failing attempt without working it");
                Some(Err(worker_failure_from_source(
                    format!("job row couldn't be decoded: {decode_error}").into(),
                )))
            }
            None => {
                run_worker(
                    &inner,
                    &row,
                    &context,
                    &hard_cancel,
                    &cancellation,
                    &mut cancellation_cause,
                )
                .await
            }
        };
        let Some(result) = result else {
            // The task outlived its abort during shutdown and may still be
            // running. Leave the row `running` for the rescuer rather than
            // making it available to run concurrently with the original.
            drop(worker_permit);
            remove_running_attempt(&inner.running, row.id, &cancellation);
            return;
        };

        // A cooperative worker can observe cancellation and return before this
        // select polls the cancellation branch. Preserve the cancellation cause
        // in that race so remote cancellation still gets its canonical outcome.
        if cancellation_cause.is_none() && cancellation.is_cancelled() {
            cancellation_cause = Some(if hard_cancel.is_cancelled() {
                CancellationCause::Shutdown
            } else {
                CancellationCause::Remote
            });
        }

        let run_duration = work_started.elapsed();
        let mut result = result;
        if let Some(resumable_failure) = context.resumable_finish(result.is_err())
            && result.is_ok()
        {
            result = Err(WorkerFailure {
                error: resumable_failure.to_string(),
                kind: WorkerFailureKind::Error,
                source: Some(resumable_failure),
                trace: String::new(),
            });
        }
        if cancellation_cause == Some(CancellationCause::Shutdown)
            && let Err(failure) = &mut result
            && is_soft_stop_failure(failure)
        {
            failure.error.clear();
            failure.error.push_str("job interrupted by client shutdown");
            failure.kind = WorkerFailureKind::Interrupted;
            failure.source = None;
            failure.trace.clear();
        }
        if cancellation_cause == Some(CancellationCause::Remote)
            && !matches!(result, Ok(WorkOutcome::Complete))
        {
            result = Err(WorkerFailure {
                error: "JobCancelError: job cancelled remotely".to_owned(),
                kind: WorkerFailureKind::Cancelled,
                source: None,
                trace: String::new(),
            });
        }
        let work_result = public_work_result(&result);
        let mut error_handler_result = ErrorHandlerDecision::default();
        if let Some(error_handler) = &inner.error_handler
            && matches!(
                work_result,
                WorkResult::Aborted | WorkResult::Failed(_) | WorkResult::Panicked(_)
            )
        {
            match error_handler
                .handle_error(&context, &row, &work_result)
                .await
            {
                Ok(handler_result) => error_handler_result = handler_result,
                Err(handler_error) => {
                    error!(error = %handler_error, "River error handler failed");
                }
            }
        }
        let metadata_updates = context.metadata_updates();
        let completion = CompletionAttempt {
            cancellation: cancellation.clone(),
            timing: CompletionTiming {
                completion_started: std::time::Instant::now(),
                queue_wait_duration,
                run_duration,
            },
        };
        let persisted = persist_result(
            &inner,
            &row,
            attempt_started_at,
            &completion,
            result,
            metadata_updates,
            error_handler_result,
            worked,
            &completion_sender,
        )
        .await;
        drop(worker_permit);
        // Once enqueued, the completer owns the running attempt until the
        // result is written.
        if let Err(operation_error) = persisted {
            error!(error = %operation_error, "failed to persist River job result");
            remove_running_attempt(&inner.running, row.id, &cancellation);
        }
    }
    .instrument(span)
    .await;
}

/// Works a job, returning its result, or `None` when the worker task outlived
/// its abort during shutdown.
async fn run_worker(
    inner: &Arc<ClientInner>,
    row: &JobRow,
    context: &WorkContext,
    hard_cancel: &CancellationToken,
    cancellation: &CancellationToken,
    cancellation_cause: &mut Option<CancellationCause>,
) -> Option<WorkerResult> {
    let mut worker_row = row.clone();
    let worker_context = context.clone();
    let worker_inner = Arc::clone(inner);
    let (timeout_sender, timeout_receiver) = oneshot::channel();
    let mut worker_task = AbortOnDrop(tokio::spawn(async move {
        worker_context.resumable_validate()?;
        for hook in &worker_inner.hooks {
            hook.work_begin(&worker_context, &mut worker_row)
                .await
                .map_err(boxed_extension_error)?;
        }
        for middleware in &worker_inner.work_middleware {
            middleware
                .before_work(&worker_context, &mut worker_row)
                .await
                .map_err(boxed_extension_error)?;
        }
        let result = worker_inner
            .workers
            .work(worker_context.clone(), &worker_row, timeout_sender)
            .await;
        let public_result = erased_work_result(&result);
        for middleware in worker_inner.work_middleware.iter().rev() {
            middleware
                .after_work(&worker_context, &worker_row, &public_result)
                .await
                .map_err(boxed_extension_error)?;
        }
        for hook in &worker_inner.hooks {
            hook.work_end(&worker_context, &worker_row, &public_result)
                .await
                .map_err(boxed_extension_error)?;
        }
        result
    }));

    // The worker reports its timeout after decoding the job's arguments,
    // following any hooks and middleware, so the timeout covers the work
    // itself as in River Go.
    let timeout_elapsed = async {
        let timeout = match timeout_receiver.await {
            Ok(WorkerTimeout::After(timeout)) => Some(timeout),
            Ok(WorkerTimeout::ClientDefault) => inner.job_timeout,
            // Disabled, or the attempt ended before work started.
            Ok(WorkerTimeout::Disabled) | Err(_) => None,
        };
        match timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        result = &mut worker_task.0 => Some(worker_join_result(result)),
        () = cancellation.cancelled() => {
            *cancellation_cause = Some(if hard_cancel.is_cancelled() {
                CancellationCause::Shutdown
            } else {
                CancellationCause::Remote
            });
            finish_cancelled_task(inner, row, &mut worker_task.0, hard_cancel).await
        }
        () = timeout_elapsed => {
            *cancellation_cause = Some(CancellationCause::Timeout);
            cancellation.cancel();
            finish_cancelled_task(inner, row, &mut worker_task.0, hard_cancel).await
        }
    }
}

pub(super) type WorkerResult = Result<WorkOutcome, WorkerFailure>;

#[derive(Debug)]
pub(super) struct WorkerFailure {
    pub(super) error: String,
    pub(super) kind: WorkerFailureKind,
    pub(super) source: Option<WorkError>,
    pub(super) trace: String,
}

#[derive(Debug)]
pub(super) enum WorkerFailureKind {
    Aborted,
    Cancelled,
    Error,
    Interrupted,
    Panic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CancellationCause {
    Remote,
    Shutdown,
    Timeout,
}

/// Aborts a spawned worker task when the executor that owns it is dropped, so
/// a stopped client never leaves work running detached from its runtime.
pub(super) struct AbortOnDrop<T>(pub(super) tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// How long shutdown waits for an aborted task before abandoning it.
const ABORT_GRACE_DURING_SHUTDOWN: Duration = Duration::from_millis(100);

/// Waits for a cancelled job to return, then treats it as stuck.
///
/// After `job_stuck_threshold`, the stuck handler runs and the task is
/// aborted. Tokio abort only takes effect at the task's next `.await`, so a
/// task blocked in synchronous code keeps its worker slot until it actually
/// ends: the queue never exceeds `max_workers`, and the job is not persisted
/// (and so cannot be fetched again) while the original may still be running.
/// During shutdown the executor gives up after a short grace period and
/// returns `None`, leaving the row `running` for the rescuer.
pub(super) async fn finish_cancelled_task(
    inner: &ClientInner,
    row: &JobRow,
    worker_task: &mut tokio::task::JoinHandle<Result<WorkOutcome, WorkError>>,
    hard_cancel: &CancellationToken,
) -> Option<WorkerResult> {
    let stuck_threshold = inner.job_stuck_threshold;
    if let Ok(result) = tokio::time::timeout(stuck_threshold, &mut *worker_task).await {
        return Some(worker_join_result(result));
    }
    warn!(
        ?stuck_threshold,
        "River job remained active after cancellation; treating it as stuck and aborting its task"
    );
    if let Some(error_handler) = &inner.error_handler
        && let Err(handler_error) = error_handler.handle_stuck(row).await
    {
        error!(error = %handler_error, "River stuck handler failed");
    }
    worker_task.abort();
    let result = tokio::select! {
        result = &mut *worker_task => Some(result),
        () = async {
            hard_cancel.cancelled().await;
            tokio::time::sleep(ABORT_GRACE_DURING_SHUTDOWN).await;
        } => None,
    };
    let Some(result) = result else {
        error!(
            "River job remained stuck after its task was aborted during shutdown; leaving it running for the rescuer"
        );
        return None;
    };
    Some(match result {
        Err(join_error) if join_error.is_cancelled() => Err(WorkerFailure {
            error: "job aborted after ignoring cancellation".to_owned(),
            kind: WorkerFailureKind::Aborted,
            source: None,
            trace: String::new(),
        }),
        result => worker_join_result(result),
    })
}

/// Whether a failure during hard shutdown is the job stopping because the
/// client cancelled it, mirroring River Go's `isSoftStopCancelError`.
///
/// A worker that returns [`WorkCancelled`] (anywhere in its error's source
/// chain) stopped cooperatively, and a task River aborted after the stuck
/// threshold was stopped by the client. Panics and other returned errors are
/// genuine failures that are recorded and retried.
pub(super) fn is_soft_stop_failure(failure: &WorkerFailure) -> bool {
    match failure.kind {
        WorkerFailureKind::Aborted => true,
        WorkerFailureKind::Error => failure
            .source
            .as_ref()
            .is_some_and(|error| WorkCancelled::is_in_chain(error.source_ref())),
        WorkerFailureKind::Cancelled
        | WorkerFailureKind::Interrupted
        | WorkerFailureKind::Panic => false,
    }
}

pub(super) fn worker_join_result(
    result: Result<Result<WorkOutcome, WorkError>, tokio::task::JoinError>,
) -> WorkerResult {
    match result {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(worker_error)) => Err(WorkerFailure {
            error: worker_error.to_string(),
            kind: WorkerFailureKind::Error,
            source: Some(worker_error),
            trace: String::new(),
        }),
        Err(join_error) => Err(WorkerFailure {
            error: if join_error.is_panic() {
                format!("job panicked: {join_error}")
            } else {
                format!("job task cancelled: {join_error}")
            },
            kind: if join_error.is_panic() {
                WorkerFailureKind::Panic
            } else {
                WorkerFailureKind::Aborted
            },
            source: None,
            trace: format!("{join_error:?}"),
        }),
    }
}

pub(super) fn worker_failure_from_source(error: BoxError) -> WorkerFailure {
    let error = WorkError::new(error);
    WorkerFailure {
        error: error.to_string(),
        kind: WorkerFailureKind::Error,
        source: Some(error),
        trace: String::new(),
    }
}

pub(super) fn boxed_extension_error(error: Error) -> WorkError {
    WorkError::new(Box::new(error))
}

pub(super) fn erased_work_result(result: &Result<WorkOutcome, WorkError>) -> WorkResult {
    match result {
        Ok(WorkOutcome::Cancel) => WorkResult::Cancelled,
        Ok(WorkOutcome::Complete) => WorkResult::Completed,
        Ok(WorkOutcome::Discard) => WorkResult::Discarded,
        Ok(WorkOutcome::Snooze(duration)) => WorkResult::Snoozed(*duration),
        Err(error) => WorkResult::Failed(error.clone()),
    }
}

pub(super) fn public_work_result(result: &WorkerResult) -> WorkResult {
    match result {
        Ok(WorkOutcome::Cancel) => WorkResult::Cancelled,
        Ok(WorkOutcome::Complete) => WorkResult::Completed,
        Ok(WorkOutcome::Discard) => WorkResult::Discarded,
        Ok(WorkOutcome::Snooze(duration)) => WorkResult::Snoozed(*duration),
        Err(failure) => match failure.kind {
            WorkerFailureKind::Aborted => WorkResult::Aborted,
            WorkerFailureKind::Cancelled => WorkResult::Cancelled,
            WorkerFailureKind::Error => {
                WorkResult::Failed(failure.source.clone().unwrap_or_else(|| {
                    WorkError::new(Box::new(std::io::Error::other(failure.error.clone())))
                }))
            }
            WorkerFailureKind::Interrupted => WorkResult::Interrupted,
            WorkerFailureKind::Panic => WorkResult::Panicked(failure.error.clone()),
        },
    }
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
pub(super) async fn persist_result(
    inner: &ClientInner,
    row: &JobRow,
    attempt_started_at: DateTime<Utc>,
    completion: &CompletionAttempt,
    result: WorkerResult,
    metadata_updates: Map<String, Value>,
    error_handler_result: ErrorHandlerDecision,
    worked: bool,
    completion_sender: &mpsc::Sender<CompletionUpdate>,
) -> Result<(), Error> {
    let now = Utc::now();
    let (state, finalized_at, scheduled_at, attempt, attempt_error, metadata, event_kind) =
        match result {
            Ok(WorkOutcome::Complete) => (
                JobState::Completed,
                Some(now),
                None,
                None,
                None,
                metadata_updates,
                JobEventKind::Completed,
            ),
            Ok(WorkOutcome::Cancel) => (
                JobState::Cancelled,
                Some(now),
                None,
                None,
                Some(AttemptError {
                    at: attempt_started_at,
                    attempt: row.attempt,
                    error: "job cancelled by worker".to_owned(),
                    trace: String::new(),
                }),
                metadata_updates,
                JobEventKind::Cancelled,
            ),
            Ok(WorkOutcome::Discard) => (
                JobState::Discarded,
                Some(now),
                None,
                None,
                Some(AttemptError {
                    at: attempt_started_at,
                    attempt: row.attempt,
                    error: "job discarded by worker".to_owned(),
                    trace: String::new(),
                }),
                metadata_updates,
                JobEventKind::Failed,
            ),
            Ok(WorkOutcome::Snooze(duration)) => {
                let scheduled_at = scheduled_after(now, duration);
                let state = if duration <= inner.maintenance.scheduler_interval {
                    JobState::Available
                } else {
                    JobState::Scheduled
                };
                let mut metadata = metadata_updates;
                let snoozes = go_json_int(row.metadata.get("snoozes")).wrapping_add(1);
                metadata.insert("snoozes".to_owned(), Value::from(snoozes));
                (
                    state,
                    None,
                    Some(scheduled_at),
                    Some(row.attempt - 1),
                    None,
                    metadata,
                    JobEventKind::Snoozed,
                )
            }
            // River Go's `JobSetStateInterrupted`: make the job available now
            // without recording an error or counting the attempt. The
            // completer keeps `attempted_at` and still honors a cancellation
            // that was attempted while the job ran.
            Err(failure) if matches!(failure.kind, WorkerFailureKind::Interrupted) => (
                JobState::Available,
                None,
                Some(now),
                Some((row.attempt - 1).max(0)),
                None,
                metadata_updates,
                JobEventKind::Interrupted,
            ),
            Err(failure) => {
                let retry_error = failure.source.clone().unwrap_or_else(|| {
                    WorkError::new(Box::new(std::io::Error::other(failure.error.clone())))
                });
                let attempt_error = AttemptError {
                    at: attempt_started_at,
                    attempt: row.attempt,
                    error: failure.error,
                    trace: failure.trace,
                };
                if matches!(failure.kind, WorkerFailureKind::Cancelled)
                    || error_handler_result == ErrorHandlerDecision::Cancel
                {
                    (
                        JobState::Cancelled,
                        Some(now),
                        None,
                        None,
                        Some(attempt_error),
                        metadata_updates,
                        JobEventKind::Cancelled,
                    )
                } else if row.attempt >= row.max_attempts {
                    (
                        JobState::Discarded,
                        Some(now),
                        None,
                        None,
                        Some(attempt_error),
                        metadata_updates,
                        JobEventKind::Failed,
                    )
                } else {
                    // Like River Go, a job that wasn't worked because its row
                    // couldn't be decoded uses only the client's retry policy.
                    let worker_retry_after = if worked {
                        inner
                            .workers
                            .next_retry(row, &retry_error, now)
                            .unwrap_or_else(|retry_error| {
                                debug!(error = %retry_error, "could not evaluate worker retry override");
                                None
                            })
                    } else {
                        None
                    };
                    let delay = worker_retry_after.unwrap_or_else(|| {
                        inner
                            .retry_policy
                            .next_retry(row, &attempt_error.error, now)
                    });
                    let scheduled_at = scheduled_after(now, delay);
                    let state = if delay <= inner.maintenance.scheduler_interval {
                        JobState::Available
                    } else {
                        JobState::Retryable
                    };
                    (
                        state,
                        None,
                        Some(scheduled_at),
                        None,
                        Some(attempt_error),
                        metadata_updates,
                        JobEventKind::Failed,
                    )
                }
            }
        };

    completion_sender
        .send(CompletionUpdate {
            attempt,
            cancellation: completion.cancellation.clone(),
            error: attempt_error,
            event_kind,
            finalized_at,
            job_id: row.id,
            metadata,
            scheduled_at,
            state,
            timing: completion.timing,
        })
        .await
        .map_err(|_| Error::runtime("completion batcher stopped".to_owned()))
}

/// Longest delay River schedules ahead, matching Go's `time.Duration` range.
const MAX_SCHEDULE_DELAY: Duration = Duration::from_nanos(i64::MAX.cast_unsigned());

/// Adds a snooze or retry delay to `now`, clamping out-of-range delays the way
/// River Go's `time.Duration` arithmetic bounds them instead of failing.
pub(super) fn scheduled_after(now: DateTime<Utc>, delay: Duration) -> DateTime<Utc> {
    chrono::Duration::from_std(delay.min(MAX_SCHEDULE_DELAY))
        .ok()
        .and_then(|delay| now.checked_add_signed(delay))
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

pub(crate) fn default_retry_delay(row: &JobRow, now: DateTime<Utc>, seed: u64) -> Duration {
    const MAX_RETRY_NANOS: u64 = i64::MAX as u64;

    let error_count = u32::try_from(row.errors.len().saturating_add(1)).unwrap_or(u32::MAX);
    let base_seconds = u128::from(error_count).pow(4);
    if base_seconds.saturating_mul(1_000_000_000) >= u128::from(MAX_RETRY_NANOS) {
        return Duration::from_nanos(MAX_RETRY_NANOS);
    }
    let base_seconds = u64::try_from(base_seconds).expect("capped retry seconds fit u64");
    let base = Duration::from_secs(base_seconds);

    let mut hasher = Sha256::new();
    hasher.update(seed.to_be_bytes());
    hasher.update(row.id.to_be_bytes());
    hasher.update(error_count.to_be_bytes());
    hasher.update(now.timestamp_nanos_opt().unwrap_or_default().to_be_bytes());
    let hash = hasher.finalize();
    let sample = u32::from_be_bytes(hash[..4].try_into().unwrap());
    let ratio = f64::from(sample) / f64::from(u32::MAX);
    // Jitter can push a delay just below the cap past it; Go caps after
    // jitter as well.
    base.mul_f64(0.9 + ratio * 0.2)
        .min(Duration::from_nanos(MAX_RETRY_NANOS))
}

/// Coerces a metadata value to an integer exactly like Go's `gjson.Int`, which
/// the Go executor uses to read the `snoozes` counter. Numbers truncate toward
/// zero, numeric strings of optional sign and digits parse, `true` is one, and
/// everything else is zero.
fn go_json_int(value: Option<&Value>) -> i64 {
    fn parse_digits(text: &str) -> Option<i64> {
        let (negative, digits) = text
            .strip_prefix('-')
            .map_or((false, text), |digits| (true, digits));
        if digits.is_empty() {
            return None;
        }
        let mut number = 0_i64;
        for byte in digits.bytes() {
            if !byte.is_ascii_digit() {
                return None;
            }
            number = number.wrapping_mul(10).wrapping_add(i64::from(byte - b'0'));
        }
        Some(if negative {
            number.wrapping_neg()
        } else {
            number
        })
    }

    const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;
    match value {
        Some(Value::Bool(true)) => 1,
        Some(Value::String(text)) => parse_digits(text).unwrap_or(0),
        Some(Value::Number(number)) => {
            let raw = number.to_string();
            let float = raw.parse::<f64>().unwrap_or(0.0);
            if (-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&float) {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "Go truncates safe floats toward zero"
                )]
                return float as i64;
            }
            #[expect(
                clippy::cast_possible_truncation,
                reason = "Go falls back to a float conversion for huge numbers"
            )]
            parse_digits(&raw).unwrap_or(float as i64)
        }
        _ => 0,
    }
}

#[cfg(test)]
mod go_json_int_tests {
    use serde::Deserialize;
    use serde_json::{Map, Value};

    use super::go_json_int;

    #[derive(Deserialize)]
    struct Fixture {
        snooze_counters: Vec<SnoozeCounter>,
    }

    #[derive(Deserialize)]
    struct SnoozeCounter {
        expected_snoozes: i64,
        metadata: Map<String, Value>,
        name: String,
    }

    #[test]
    fn snooze_counter_matches_go_fixture() {
        let fixture: Fixture = serde_json::from_str(include_str!(
            "../../../../conformance/fixtures/maintenance_values.json"
        ))
        .unwrap();
        assert!(!fixture.snooze_counters.is_empty());
        for case in fixture.snooze_counters {
            assert_eq!(
                go_json_int(case.metadata.get("snoozes")).wrapping_add(1),
                case.expected_snoozes,
                "{}",
                case.name
            );
        }
    }
}
