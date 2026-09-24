//! Batched persistence of job completions.

#[allow(clippy::wildcard_imports)]
use super::*;

pub(super) struct CompletionUpdate {
    pub(super) attempt: i16,
    pub(super) cancellation: CancellationToken,
    pub(super) error_json: Option<Value>,
    pub(super) event_kind: JobEventKind,
    pub(super) finalized_at: Option<DateTime<Utc>>,
    pub(super) job_id: i64,
    pub(super) metadata: Map<String, Value>,
    pub(super) scheduled_at: Option<DateTime<Utc>>,
    pub(super) state: JobState,
    pub(super) timing: CompletionTiming,
}

pub(super) fn persisted_completion_event_kind(
    state: JobState,
    requested: JobEventKind,
) -> JobEventKind {
    match state {
        JobState::Available => match requested {
            JobEventKind::Failed | JobEventKind::Interrupted | JobEventKind::Snoozed => requested,
            JobEventKind::Cancelled | JobEventKind::Completed => JobEventKind::Failed,
        },
        JobState::Cancelled => JobEventKind::Cancelled,
        JobState::Completed => JobEventKind::Completed,
        JobState::Discarded | JobState::Retryable => JobEventKind::Failed,
        JobState::Scheduled => JobEventKind::Snoozed,
        JobState::Pending | JobState::Running => {
            panic!("completion event received a job that was not finalized")
        }
    }
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

pub(super) enum PersistResult {
    Enqueued,
    Finished(Option<Box<Event>>),
}

pub(super) async fn run_completion_batcher(
    inner: Arc<ClientInner>,
    mut receiver: mpsc::Receiver<CompletionUpdate>,
) -> Result<(), Error> {
    const COMPLETION_BATCH_DELAY: Duration = Duration::from_millis(10);
    #[cfg(feature = "postgres")]
    const COMPLETION_BATCH_CONCURRENCY: usize = 2;
    const COMPLETION_BATCH_SIZE: usize = 5_000;
    const COMPLETION_BATCH_THRESHOLD: usize = COMPLETION_BATCH_SIZE;
    let mut batches = JoinSet::new();
    let batch_concurrency = match inner.database.kind() {
        #[cfg(feature = "postgres")]
        DatabaseKind::Postgres => COMPLETION_BATCH_CONCURRENCY,
        #[cfg(feature = "sqlite")]
        DatabaseKind::Sqlite => 1,
    };

    loop {
        // Never build a third coordinator-owned batch while both persistence
        // slots are occupied. The bounded receiver applies backpressure until
        // one of the two database writes finishes.
        while batches.len() >= batch_concurrency {
            let result = batches
                .join_next()
                .await
                .expect("completion batch task is present")
                .map_err(Error::from_join)?;
            finish_completion_batch(&inner, result);
        }
        let first = if batches.is_empty() {
            receiver.recv().await
        } else {
            tokio::select! {
                update = receiver.recv() => update,
                result = batches.join_next() => {
                    let result = result
                        .expect("completion batch task is present")
                        .map_err(Error::from_join)?;
                    finish_completion_batch(&inner, result);
                    continue;
                }
            }
        };
        let Some(first) = first else {
            break;
        };
        let mut batch = Vec::with_capacity(COMPLETION_BATCH_SIZE);
        batch.push(first);
        let delay = tokio::time::sleep(COMPLETION_BATCH_DELAY);
        tokio::pin!(delay);
        while batch.len() < COMPLETION_BATCH_THRESHOLD {
            tokio::select! {
                () = &mut delay => break,
                update = receiver.recv() => match update {
                    Some(update) => batch.push(update),
                    None => break,
                },
            }
        }
        while batch.len() < COMPLETION_BATCH_SIZE {
            match receiver.try_recv() {
                Ok(update) => batch.push(update),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }

        // A second database write is worthwhile only when work has already
        // filled a complete batch. Partial batches wait for the active writer,
        // retaining the low-query behavior of the serial architecture.
        while !batches.is_empty() && batch.len() < COMPLETION_BATCH_THRESHOLD {
            let result = batches
                .join_next()
                .await
                .expect("completion batch task is present")
                .map_err(Error::from_join)?;
            finish_completion_batch(&inner, result);
        }
        let batch_inner = Arc::clone(&inner);
        batches.spawn(async move {
            let records = persist_completion_batch(&batch_inner, &batch).await;
            (batch, records)
        });
        while let Some(result) = batches.try_join_next() {
            finish_completion_batch(&inner, result.map_err(Error::from_join)?);
        }
    }
    while let Some(result) = batches.join_next().await {
        finish_completion_batch(&inner, result.map_err(Error::from_join)?);
    }
    Ok(())
}

pub(super) fn finish_completion_batch(
    inner: &ClientInner,
    (batch, records): (Vec<CompletionUpdate>, Result<Vec<JobRow>, Error>),
) {
    match records {
        Ok(records) => {
            let mut rows = HashMap::with_capacity(records.len());
            for record in records {
                let id = record.id;
                rows.insert(id, record);
            }
            for update in &batch {
                let job_id = update.job_id;
                finish_batched_completion(inner, update, rows.remove(&job_id));
            }
        }
        Err(error) => {
            error!(
                error = %error,
                count = batch.len(),
                "failed to persist River job completion batch"
            );
            for update in &batch {
                remove_running_attempt(&inner.running, update.job_id, &update.cancellation);
            }
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "keeps PostgreSQL batch and transactionally equivalent SQLite completion together"
)]
pub(super) async fn persist_completion_batch(
    inner: &ClientInner,
    batch: &[CompletionUpdate],
) -> Result<Vec<JobRow>, Error> {
    #[cfg(feature = "sqlite")]
    if let Some(pool) = inner.sqlite_pool() {
        let mut transaction = crate::database::begin_sqlite_write(pool).await?;
        let mut rows = Vec::with_capacity(batch.len());
        for update in batch {
            let attempt_error = update
                .error_json
                .clone()
                .map(serde_json::from_value::<AttemptError>)
                .transpose()?;
            let row = crate::database::sqlite::complete_decoded(
                &mut transaction,
                &crate::database::sqlite::CompleteJob {
                    attempt: Some(update.attempt),
                    error: attempt_error.as_ref(),
                    finalized_at: update.finalized_at,
                    id: update.job_id,
                    metadata_updates: Some(&update.metadata),
                    now: Utc::now(),
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
            match row {
                Some(Ok(row)) => rows.push(row),
                Some(Err(job)) => error!(
                    job_id = update.job_id,
                    error = %job.error,
                    "River job row persisted by completion could not be decoded; skipping its event"
                ),
                None => {}
            }
        }
        transaction.commit().await?;
        return Ok(rows);
    }
    #[cfg(feature = "postgres")]
    {
        let attempts = batch
            .iter()
            .map(|update| update.attempt)
            .collect::<Vec<_>>();
        let errors = batch
            .iter()
            .map(|update| update.error_json.clone().map(Json))
            .collect::<Vec<_>>();
        let finalized_at = batch
            .iter()
            .map(|update| update.finalized_at)
            .collect::<Vec<_>>();
        let ids = batch.iter().map(|update| update.job_id).collect::<Vec<_>>();
        let metadata = batch
            .iter()
            .map(|update| Json(Value::Object(update.metadata.clone())))
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
        let sql = format!(
            "WITH updates AS (\
            SELECT * FROM unnest(\
                $1::bigint[], $2::smallint[], $3::jsonb[], $4::timestamptz[], \
                $5::jsonb[], $6::timestamptz[], $7::text[]\
            ) AS update_params(\
                id, attempt, attempt_error, finalized_at, metadata, scheduled_at, state)\
         ) \
         UPDATE {table} AS job SET \
            attempt = CASE WHEN job.state = 'running' \
                AND NOT (updates.state IN ('available', 'retryable', 'scheduled') \
                    AND job.metadata ? 'cancel_attempted_at') \
                THEN updates.attempt ELSE job.attempt END, \
            errors = CASE WHEN job.state != 'running' OR updates.attempt_error IS NULL THEN job.errors \
                ELSE array_append(coalesce(job.errors, '{{}}'), updates.attempt_error) END, \
            finalized_at = CASE WHEN job.state != 'running' THEN job.finalized_at \
                WHEN updates.state IN ('available', 'retryable', 'scheduled') \
                AND job.metadata ? 'cancel_attempted_at' \
                THEN coalesce(updates.finalized_at, now()) ELSE updates.finalized_at END, \
            metadata = job.metadata || updates.metadata, \
            scheduled_at = CASE WHEN job.state = 'running' \
                AND NOT (updates.state IN ('available', 'retryable', 'scheduled') \
                    AND job.metadata ? 'cancel_attempted_at') \
                THEN coalesce(updates.scheduled_at, job.scheduled_at) ELSE job.scheduled_at END, \
            state = CASE WHEN job.state != 'running' THEN job.state \
                WHEN updates.state IN ('available', 'retryable', 'scheduled') \
                AND job.metadata ? 'cancel_attempted_at' \
                THEN 'cancelled'::{state_type} ELSE updates.state::{state_type} END \
         FROM updates WHERE job.id = updates.id \
         RETURNING {}, false AS unique_skipped_as_duplicate",
            job_projection("job")
        );
        let records = sqlx::query(AssertSqlSafe(sql))
            .bind(ids)
            .bind(attempts)
            .bind(errors)
            .bind(finalized_at)
            .bind(metadata)
            .bind(scheduled_at)
            .bind(states)
            .fetch_all(
                inner
                    .postgres_pool()
                    .expect("PostgreSQL completion path requires a PostgreSQL pool"),
            )
            .await?;
        return Ok(records
            .iter()
            .filter_map(|row| match decode_job_row(row) {
                Ok(row) => Some(row),
                Err(job) => {
                    error!(
                        job_id = job.id,
                        error = %job.error,
                        "River job row persisted by completion could not be decoded; skipping its event"
                    );
                    None
                }
            })
            .collect());
    }
    #[allow(unreachable_code)]
    Err(Error::runtime(
        "database dispatch selected no supported backend".to_owned(),
    ))
}

pub(super) fn finish_batched_completion(
    inner: &ClientInner,
    update: &CompletionUpdate,
    record: Option<JobRow>,
) {
    if let Some(row) = record {
        let event_kind = persisted_completion_event_kind(row.state, update.event_kind);
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
            "job result ignored because job is no longer running"
        );
    }
    remove_running_attempt(&inner.running, update.job_id, &update.cancellation);
}
