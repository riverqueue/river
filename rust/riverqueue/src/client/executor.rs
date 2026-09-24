//! Execution of individual job attempts.

#[allow(clippy::wildcard_imports)]
use super::*;

#[allow(clippy::too_many_lines)]
pub(super) async fn execute_job(
    inner: Arc<ClientInner>,
    row: JobRow,
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
        let mut worker_row = row.clone();
        let worker_context = context.clone();
        let worker_inner = Arc::clone(&inner);
        let (timeout_sender, timeout_receiver) = oneshot::channel();
        let mut worker_task = tokio::spawn(async move {
            worker_context.resumable_validate().await?;
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
        });

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
        let mut cancellation_cause = None;
        let work_started = std::time::Instant::now();
        let result = tokio::select! {
            result = &mut worker_task => worker_join_result(result),
            () = cancellation.cancelled() => {
                cancellation_cause = Some(if hard_cancel.is_cancelled() {
                    CancellationCause::Shutdown
                } else {
                    CancellationCause::Remote
                });
                cancellation.cancel();
                finish_cancelled_task(&mut worker_task, inner.job_stuck_threshold).await
            }
            () = timeout_elapsed => {
                cancellation_cause = Some(CancellationCause::Timeout);
                cancellation.cancel();
                finish_cancelled_task(&mut worker_task, inner.job_stuck_threshold).await
            }
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
        let was_aborted = result
            .as_ref()
            .is_err_and(|failure| matches!(failure.kind, WorkerFailureKind::Aborted));
        if let Some(resumable_failure) = context.resumable_finish(result.is_err()).await
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
        if was_aborted
            && let Some(error_handler) = &inner.error_handler
            && let Err(handler_error) = error_handler.handle_stuck(&row).await
        {
            error!(error = %handler_error, "River stuck handler failed");
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
        let completion_enqueued = match persist_result(
            &inner,
            &row,
            &completion,
            result,
            metadata_updates,
            error_handler_result,
            &completion_sender,
        )
        .await
        {
            Ok(PersistResult::Finished(Some(event))) => {
                let Event::Job(job_event) = *event else {
                    unreachable!("job persistence returns only job events")
                };
                let event = Event::job_with_statistics(
                    job_event.kind,
                    job_event.job,
                    JobStatistics {
                        complete_duration: completion.timing.completion_started.elapsed(),
                        queue_wait_duration,
                        run_duration,
                    },
                );
                let _ = inner.events.send(event);
                false
            }
            Ok(PersistResult::Enqueued) => true,
            Ok(PersistResult::Finished(None)) => false,
            Err(operation_error) => {
                error!(error = %operation_error, "failed to persist River job result");
                false
            }
        };
        drop(worker_permit);
        if completion_enqueued {
            return;
        }
        remove_running_attempt(&inner.running, row.id, &cancellation);
    }
    .instrument(span)
    .await;
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

pub(super) async fn finish_cancelled_task(
    worker_task: &mut tokio::task::JoinHandle<Result<WorkOutcome, WorkError>>,
    stuck_threshold: Duration,
) -> WorkerResult {
    if let Ok(result) = tokio::time::timeout(stuck_threshold, &mut *worker_task).await {
        return worker_join_result(result);
    }
    warn!(
        ?stuck_threshold,
        "River job remained active after cancellation; aborting task"
    );
    worker_task.abort();
    match tokio::time::timeout(Duration::from_millis(100), &mut *worker_task).await {
        Ok(Err(join_error)) if join_error.is_cancelled() => Err(WorkerFailure {
            error: "job aborted after ignoring cancellation".to_owned(),
            kind: WorkerFailureKind::Aborted,
            source: None,
            trace: String::new(),
        }),
        Ok(result) => worker_join_result(result),
        Err(_) => Err(WorkerFailure {
            error: "job remained stuck after Tokio task abort".to_owned(),
            kind: WorkerFailureKind::Aborted,
            source: None,
            trace: String::new(),
        }),
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
pub(super) async fn persist_result(
    inner: &ClientInner,
    row: &JobRow,
    completion: &CompletionAttempt,
    result: WorkerResult,
    metadata_updates: Map<String, Value>,
    error_handler_result: ErrorHandlerDecision,
    completion_sender: &mpsc::Sender<CompletionUpdate>,
) -> Result<PersistResult, Error> {
    let now = Utc::now();
    let (state, finalized_at, scheduled_at, attempt, attempt_error, metadata, event_kind) =
        match result {
            Ok(WorkOutcome::Complete) => (
                JobState::Completed,
                Some(now),
                None,
                row.attempt,
                None,
                metadata_updates,
                JobEventKind::Completed,
            ),
            Ok(WorkOutcome::Cancel) => (
                JobState::Cancelled,
                Some(now),
                None,
                row.attempt,
                Some(AttemptError {
                    at: row.attempted_at.unwrap_or(now),
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
                row.attempt,
                Some(AttemptError {
                    at: row.attempted_at.unwrap_or(now),
                    attempt: row.attempt,
                    error: "job discarded by worker".to_owned(),
                    trace: String::new(),
                }),
                metadata_updates,
                JobEventKind::Failed,
            ),
            Ok(WorkOutcome::Snooze(duration)) => {
                let scheduled_at = now
                    + chrono::Duration::from_std(duration)
                        .map_err(|error| Error::invalid_job(error.to_string()))?;
                let state = if duration <= inner.maintenance.scheduler_interval {
                    JobState::Available
                } else {
                    JobState::Scheduled
                };
                let mut metadata = metadata_updates;
                let snoozes = row
                    .metadata
                    .get("snoozes")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    + 1;
                metadata.insert("snoozes".to_owned(), Value::from(snoozes));
                (
                    state,
                    None,
                    Some(scheduled_at),
                    row.attempt - 1,
                    None,
                    metadata,
                    JobEventKind::Snoozed,
                )
            }
            Err(failure) => {
                if matches!(failure.kind, WorkerFailureKind::Interrupted) {
                    return persist_interrupted(inner, row, metadata_updates)
                        .await
                        .map(|event| PersistResult::Finished(event.map(Box::new)));
                }
                let retry_error = failure.source.clone().unwrap_or_else(|| {
                    WorkError::new(Box::new(std::io::Error::other(failure.error.clone())))
                });
                let attempt_error = AttemptError {
                    at: row.attempted_at.unwrap_or(now),
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
                        row.attempt,
                        Some(attempt_error),
                        metadata_updates,
                        JobEventKind::Cancelled,
                    )
                } else if row.attempt >= row.max_attempts {
                    (
                        JobState::Discarded,
                        Some(now),
                        None,
                        row.attempt,
                        Some(attempt_error),
                        metadata_updates,
                        JobEventKind::Failed,
                    )
                } else {
                    let worker_retry_after = inner
                        .workers
                        .next_retry(row, &retry_error, now)
                        .unwrap_or_else(|retry_error| {
                            debug!(error = %retry_error, "could not evaluate worker retry override");
                            None
                        });
                    let delay = worker_retry_after.unwrap_or_else(|| {
                        inner
                            .retry_policy
                            .next_retry(row, &attempt_error.error, now)
                    });
                    let scheduled_at = now
                        + chrono::Duration::from_std(delay)
                            .map_err(|error| Error::invalid_job(error.to_string()))?;
                    let state = if delay <= inner.maintenance.scheduler_interval {
                        JobState::Available
                    } else {
                        JobState::Retryable
                    };
                    (
                        state,
                        None,
                        Some(scheduled_at),
                        row.attempt,
                        Some(attempt_error),
                        metadata_updates,
                        JobEventKind::Failed,
                    )
                }
            }
        };

    #[cfg(feature = "postgres")]
    let table = inner.schema.qualify("river_job");
    #[cfg(feature = "postgres")]
    let state_type = inner.schema.qualify("river_job_state");
    #[cfg(feature = "postgres")]
    let sql = format!(
        "UPDATE {table} AS job SET \
            attempt = CASE WHEN state = 'running' \
                                AND NOT ($7::text IN ('available', 'retryable', 'scheduled') \
                                    AND metadata ? 'cancel_attempted_at') \
                           THEN $2 ELSE attempt END, \
            errors = CASE WHEN state != 'running' OR $3::jsonb IS NULL THEN errors ELSE array_append(coalesce(errors, '{{}}'), $3::jsonb) END, \
            finalized_at = CASE WHEN state != 'running' THEN finalized_at \
                                WHEN $7::text IN ('available', 'retryable', 'scheduled') AND metadata ? 'cancel_attempted_at' \
                                THEN coalesce($4, now()) ELSE $4 END, \
            metadata = metadata || $5::jsonb, \
            scheduled_at = CASE WHEN state = 'running' \
                                     AND NOT ($7::text IN ('available', 'retryable', 'scheduled') \
                                         AND metadata ? 'cancel_attempted_at') \
                                THEN coalesce($6, scheduled_at) ELSE scheduled_at END, \
            state = CASE WHEN state != 'running' THEN state \
                         WHEN $7::text IN ('available', 'retryable', 'scheduled') AND metadata ? 'cancel_attempted_at' \
                         THEN 'cancelled'::{state_type} ELSE $7::text::{state_type} END \
         WHERE id = $1 \
         RETURNING {}, false AS unique_skipped_as_duplicate",
        job_projection("job")
    );
    let error_json = attempt_error
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;
    if !inner.pilot.intercepts_completion() {
        completion_sender
            .send(CompletionUpdate {
                attempt,
                cancellation: completion.cancellation.clone(),
                error_json,
                event_kind,
                finalized_at,
                job_id: row.id,
                metadata,
                scheduled_at,
                state,
                timing: completion.timing,
            })
            .await
            .map_err(|_| Error::runtime("completion batcher stopped".to_owned()))?;
        return Ok(PersistResult::Enqueued);
    }

    #[cfg(feature = "sqlite")]
    if let Some(pool) = inner.sqlite_pool() {
        let record = {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let completion_action = inner
                .pilot
                .before_job_completion(
                    PilotDatabaseConnection::Sqlite(&mut transaction),
                    &CompletionParams {
                        database: inner.pilot_database_config(),
                        job_id: row.id,
                        metadata_updates: metadata.clone(),
                        state: state.as_str().to_owned(),
                    },
                )
                .await
                .map_err(|source| Error::Extension {
                    phase: "job completion",
                    source,
                })?;
            let record = match completion_action {
                CompletionAction::Continue => {
                    let updated = crate::database::sqlite::complete(
                        &mut transaction,
                        &crate::database::sqlite::CompleteJob {
                            attempt: Some(attempt),
                            error: attempt_error.as_ref(),
                            finalized_at,
                            id: row.id,
                            metadata_updates: Some(&metadata),
                            now,
                            scheduled_at,
                            state,
                        },
                    )
                    .await
                    .map_err(sqlite_backend_error)?;
                    match updated {
                        Some(row) => Some(row),
                        None => crate::database::sqlite::merge_metadata_if_not_running(
                            &mut transaction,
                            row.id,
                            &metadata,
                        )
                        .await
                        .map_err(sqlite_backend_error)?
                        .map(|row| row.map_err(|job| Error::invalid_job(job.error)))
                        .transpose()?,
                    }
                }
                CompletionAction::Handled => crate::database::sqlite::get(&mut transaction, row.id)
                    .await
                    .map_err(sqlite_backend_error)?,
            };
            transaction.commit().await?;
            record
        };
        let Some(row) = record else {
            debug!(
                job_id = row.id,
                "job result ignored because the job no longer exists"
            );
            return Ok(PersistResult::Finished(None));
        };
        let event_kind = persisted_completion_event_kind(row.state, event_kind);
        return Ok(PersistResult::Finished(Some(Box::new(Event::job(
            event_kind, row,
        )))));
    }
    #[cfg(feature = "postgres")]
    if let Some(pool) = inner.postgres_pool() {
        let record = {
            let mut transaction = pool.begin().await?;
            let completion_action = inner
                .pilot
                .before_job_completion(
                    PilotDatabaseConnection::Postgres(&mut transaction),
                    &CompletionParams {
                        database: inner.pilot_database_config(),
                        job_id: row.id,
                        metadata_updates: metadata.clone(),
                        state: state.as_str().to_owned(),
                    },
                )
                .await
                .map_err(|source| Error::Extension {
                    phase: "job completion",
                    source,
                })?;
            let record = match completion_action {
                CompletionAction::Continue => {
                    persist_completion_update(
                        &mut *transaction,
                        &sql,
                        row.id,
                        attempt,
                        error_json.as_ref(),
                        finalized_at,
                        &metadata,
                        scheduled_at,
                        state,
                    )
                    .await?
                }
                CompletionAction::Handled => {
                    let sql = format!(
                        "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
                 WHERE id = $1 LIMIT 1",
                        job_projection("job")
                    );
                    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
                        .bind(row.id)
                        .fetch_optional(&mut *transaction)
                        .await?
                }
            };
            transaction.commit().await?;
            record
        };
        let Some(record) = record else {
            debug!(
                job_id = row.id,
                "job result ignored because the job no longer exists"
            );
            return Ok(PersistResult::Finished(None));
        };
        let row = record.into_job_row()?;
        let event_kind = persisted_completion_event_kind(row.state, event_kind);
        return Ok(PersistResult::Finished(Some(Box::new(Event::job(
            event_kind, row,
        )))));
    }
    #[allow(unreachable_code)]
    Err(Error::runtime(
        "database dispatch selected no supported backend".to_owned(),
    ))
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "postgres")]
pub(super) async fn persist_completion_update<'executor, E>(
    executor: E,
    sql: &str,
    job_id: i64,
    attempt: i16,
    error_json: Option<&Value>,
    finalized_at: Option<DateTime<Utc>>,
    metadata: &Map<String, Value>,
    scheduled_at: Option<DateTime<Utc>>,
    state: JobState,
) -> Result<Option<JobRecord>, Error>
where
    E: Executor<'executor, Database = Postgres>,
{
    Ok(
        sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql.to_owned()))
            .bind(job_id)
            .bind(attempt)
            .bind(error_json.map(Json))
            .bind(finalized_at)
            .bind(Json(metadata))
            .bind(scheduled_at)
            .bind(state.as_str())
            .fetch_optional(executor)
            .await?,
    )
}

pub(super) async fn persist_interrupted(
    inner: &ClientInner,
    row: &JobRow,
    metadata_updates: Map<String, Value>,
) -> Result<Option<Event>, Error> {
    #[cfg(feature = "sqlite")]
    if let Some(pool) = inner.sqlite_pool() {
        let mut transaction = crate::database::begin_sqlite_write(pool).await?;
        let updated = crate::database::sqlite::interrupt(
            &mut transaction,
            row.id,
            &metadata_updates,
            Utc::now(),
        )
        .await
        .map_err(sqlite_backend_error)?;
        let updated = match updated {
            Some(row) => Some(row),
            None => crate::database::sqlite::merge_metadata_if_not_running(
                &mut transaction,
                row.id,
                &metadata_updates,
            )
            .await
            .map_err(sqlite_backend_error)?
            .map(|row| row.map_err(|job| Error::invalid_job(job.error)))
            .transpose()?,
        };
        if let Some(updated) = &updated
            && updated.state == JobState::Available
        {
            let payload = serde_json::json!({"queue": updated.queue}).to_string();
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
        transaction.commit().await?;
        return Ok(updated.map(|row| {
            Event::job(
                persisted_completion_event_kind(row.state, JobEventKind::Interrupted),
                row,
            )
        }));
    }
    #[cfg(feature = "postgres")]
    {
        let table = inner.schema.qualify("river_job");
        let sql = format!(
            "UPDATE {table} AS job SET \
         attempt = CASE WHEN state = 'running' THEN greatest(job.attempt - 1, 0) ELSE attempt END, \
         attempted_at = CASE WHEN state = 'running' THEN NULL ELSE attempted_at END, \
         finalized_at = CASE WHEN state = 'running' THEN NULL ELSE finalized_at END, \
         metadata = metadata || $2::jsonb, \
         scheduled_at = CASE WHEN state = 'running' THEN now() ELSE scheduled_at END, \
         state = CASE WHEN state = 'running' THEN 'available'::{state_type} ELSE state END \
         WHERE id = $1 \
         RETURNING {}, false AS unique_skipped_as_duplicate",
            job_projection("job"),
            state_type = inner.schema.qualify("river_job_state")
        );
        let record = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(row.id)
            .bind(Json(metadata_updates))
            .fetch_optional(
                inner
                    .postgres_pool()
                    .expect("PostgreSQL completion path requires a PostgreSQL pool"),
            )
            .await?;
        return Ok(record.map(JobRecord::into_job_row).transpose()?.map(|row| {
            Event::job(
                persisted_completion_event_kind(row.state, JobEventKind::Interrupted),
                row,
            )
        }));
    }
    #[allow(unreachable_code)]
    Err(Error::runtime(
        "database dispatch selected no supported backend".to_owned(),
    ))
}

/// Records a failed attempt for a claimed row that could not be decoded.
///
/// River Go's executor records an argument decoding failure as an ordinary
/// attempt error, so the job is retried or discarded instead of staying
/// `running` until the rescuer. Without a decoded row, the client retry policy
/// cannot run; River's default `attempt^4` schedule is used instead.
pub(super) async fn record_undecodable_job(
    inner: &ClientInner,
    completion_sender: &mpsc::Sender<CompletionUpdate>,
    job: UndecodableJob,
) {
    let Some(job_id) = job.id else {
        error!(error = %job.error, "claimed River job row has no decodable ID; leaving it for the rescuer");
        return;
    };
    error!(job_id, error = %job.error, "claimed River job row could not be decoded; recording a failed attempt");
    let now = Utc::now();
    let attempt = saturating_i16(job.attempt);
    let attempt_error = AttemptError {
        at: now,
        attempt,
        error: format!("River could not decode the job row: {}", job.error),
        trace: String::new(),
    };
    let (state, finalized_at, scheduled_at) = if job.attempt >= job.max_attempts {
        (JobState::Discarded, Some(now), None)
    } else {
        let error_count = u32::try_from(job.error_count.saturating_add(1)).unwrap_or(u32::MAX);
        let delay = Duration::from_secs(u64::from(error_count).saturating_pow(4));
        let state = if delay <= inner.maintenance.scheduler_interval {
            JobState::Available
        } else {
            JobState::Retryable
        };
        (state, None, Some(scheduled_after(now, delay)))
    };
    let error_json = match serde_json::to_value(&attempt_error) {
        Ok(error_json) => error_json,
        Err(json_error) => {
            error!(job_id, error = %json_error, "could not encode River attempt error");
            return;
        }
    };
    let update = CompletionUpdate {
        attempt,
        cancellation: CancellationToken::new(),
        error_json: Some(error_json),
        event_kind: JobEventKind::Failed,
        finalized_at,
        job_id,
        metadata: Map::new(),
        scheduled_at,
        state,
        timing: CompletionTiming {
            completion_started: std::time::Instant::now(),
            queue_wait_duration: Duration::ZERO,
            run_duration: Duration::ZERO,
        },
    };
    if completion_sender.send(update).await.is_err() {
        error!(
            job_id,
            "completion batcher stopped before recording an undecodable River job"
        );
    }
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
    base.mul_f64(0.9 + ratio * 0.2)
}
