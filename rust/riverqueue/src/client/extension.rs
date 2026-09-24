//! Unstable extension entry points used by companion crates.

#[allow(clippy::wildcard_imports)]
use super::*;

impl Client {
    /// Atomically claims complete job rows for an exact-version extension.
    ///
    /// Eligible jobs are available, due, and match the supplied kind, queue,
    /// and top-level metadata values. River records the attempt and client ID,
    /// applies the metadata updates in the same transaction, and returns rows
    /// ordered by priority, scheduled time, and ID.
    #[doc(hidden)]
    pub async fn extension_claim_jobs(
        &self,
        params: ExtensionClaimParams,
    ) -> Result<Vec<JobRow>, Error> {
        if params.maximum <= 0 {
            return Ok(Vec::new());
        }
        #[cfg(feature = "sqlite")]
        if let Some(pool) = self.inner.sqlite_pool() {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let rows = crate::database::sqlite::claim_filtered(
                &mut transaction,
                &crate::database::sqlite::ClaimFilteredJobs {
                    client_id: &self.inner.id,
                    excluded_job_id: params.excluded_job_id,
                    kind: &params.kind,
                    limit: params.maximum,
                    max_attempted_by: ATTEMPTED_BY_MAX,
                    metadata_matches: &params.metadata_matches,
                    metadata_updates: &params.metadata_updates,
                    now: Utc::now(),
                    queue: &params.queue,
                },
            )
            .await;
            return match rows {
                Ok(mut rows) => {
                    sort_claimed_jobs(&mut rows);
                    transaction.commit().await?;
                    Ok(rows)
                }
                Err(error) => {
                    transaction.rollback().await?;
                    Err(sqlite_backend_error(error))
                }
            };
        }
        #[cfg(feature = "postgres")]
        {
            let pool = self
                .inner
                .postgres_pool()
                .expect("PostgreSQL claim path requires a PostgreSQL pool");
            let table = self.inner.schema.qualify("river_job");
            let sql = format!(
                "WITH locked AS (\
                    SELECT id FROM {table} \
                    WHERE state = 'available' AND queue = $1 AND kind = $2 \
                      AND id != $3 AND scheduled_at <= now() \
                      AND metadata @> $4::jsonb \
                    ORDER BY priority ASC, scheduled_at ASC, id ASC \
                    LIMIT $5 FOR UPDATE SKIP LOCKED\
                 ) UPDATE {table} AS job \
                    SET state = 'running', attempt = job.attempt + 1, \
                        attempted_at = now(), attempted_by = array_append(\
                            CASE WHEN array_length(job.attempted_by, 1) >= $7 \
                                 THEN job.attempted_by[array_length(job.attempted_by, 1) + 2 - $7:] \
                                 ELSE job.attempted_by END, $6), \
                        metadata = job.metadata || $8::jsonb \
                    FROM locked WHERE job.id = locked.id \
                    RETURNING {}, false AS unique_skipped_as_duplicate",
                job_projection("job")
            );
            let mut transaction = pool.begin().await?;
            let records = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
                .bind(&params.queue)
                .bind(&params.kind)
                .bind(params.excluded_job_id)
                .bind(Json(&params.metadata_matches))
                .bind(params.maximum)
                .bind(&self.inner.id)
                .bind(ATTEMPTED_BY_MAX)
                .bind(Json(&params.metadata_updates))
                .fetch_all(&mut *transaction)
                .await;
            let records = match records {
                Ok(records) => records,
                Err(error) => {
                    transaction.rollback().await?;
                    return Err(error.into());
                }
            };
            let rows = records
                .into_iter()
                .map(JobRecord::into_job_row)
                .collect::<Result<Vec<_>, _>>();
            return match rows {
                Ok(mut rows) => {
                    sort_claimed_jobs(&mut rows);
                    transaction.commit().await?;
                    Ok(rows)
                }
                Err(error) => {
                    transaction.rollback().await?;
                    Err(error)
                }
            };
        }
        #[allow(unreachable_code)]
        Err(Error::runtime(
            "database dispatch selected no supported backend".to_owned(),
        ))
    }

    /// Computes the configured retry delay for an exact-version extension.
    #[doc(hidden)]
    #[must_use]
    pub fn extension_retry_delay(&self, row: &JobRow, error: &str, now: DateTime<Utc>) -> Duration {
        self.inner.retry_policy.next_retry(row, error, now)
    }

    /// Returns the scheduler horizon used by exact-version completion helpers.
    #[doc(hidden)]
    #[must_use]
    pub fn extension_scheduler_interval(&self) -> Duration {
        self.inner.maintenance.scheduler_interval
    }

    /// Reports outcomes for jobs claimed and executed by an exact-version
    /// extension through River's canonical completion pipeline.
    ///
    /// The extension's handler context supplies metadata updates shared by the
    /// execution. Each failed outcome runs the error handler for its own job,
    /// then every outcome uses ordinary retry selection, completion
    /// interception, persistence batching, event delivery, and statistics.
    /// Work middleware and work hooks are deliberately not invoked again: they
    /// surround the extension's handler once, before it reports these results.
    /// Outcomes racing an external terminal transition preserve the persisted
    /// state while retaining the submitted event reason.
    ///
    /// # Errors
    ///
    /// Returns an error when the client runtime is not accepting completions,
    /// or when synchronous extension interception or result normalization
    /// fails. Ordinary batched persistence failures are reported by the
    /// running completion service, matching regular worker behavior.
    #[doc(hidden)]
    pub async fn extension_persist_claimed_outcomes(
        &self,
        execution_context: &WorkContext,
        outcomes: Vec<(JobRow, Result<WorkOutcome, BoxError>)>,
    ) -> Result<(), Error> {
        if outcomes.is_empty() {
            return Ok(());
        }
        let metadata_updates = execution_context.metadata_updates();
        let completion_sender = self
            .inner
            .completion_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(mpsc::WeakSender::upgrade)
            .ok_or_else(|| {
                Error::runtime_context(
                    "completion pipeline",
                    "client runtime is not accepting job completions".to_owned(),
                )
            })?;
        let mut first_error = None;
        for (row, result) in outcomes {
            if let Err(error) = self
                .extension_persist_claimed_outcome(
                    row,
                    result,
                    execution_context.cancellation_token(),
                    &metadata_updates,
                    &completion_sender,
                )
                .await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    pub(super) async fn extension_persist_claimed_outcome(
        &self,
        row: JobRow,
        result: Result<WorkOutcome, BoxError>,
        execution_cancellation: &CancellationToken,
        metadata_updates: &Map<String, Value>,
        completion_sender: &mpsc::Sender<CompletionUpdate>,
    ) -> Result<(), Error> {
        let cancellation = CancellationToken::new();
        let context = WorkContext::for_job(
            self.clone(),
            execution_cancellation.clone(),
            row.id,
            &row.metadata,
        );
        for (key, value) in metadata_updates {
            context.insert_metadata(key.clone(), value.clone());
        }
        let result = result.map_err(worker_failure_from_source);
        let work_result = public_work_result(&result);
        let mut error_handler_result = ErrorHandlerDecision::default();
        if let Some(error_handler) = &self.inner.error_handler
            && matches!(work_result, WorkResult::Failed(_))
        {
            match error_handler
                .handle_error(&context, &row, &work_result)
                .await
            {
                Ok(decision) => error_handler_result = decision,
                Err(error) => error!(error = %error, "River error handler failed"),
            }
        }
        let queue_wait_duration = row
            .attempted_at
            .and_then(|attempted_at| {
                (attempted_at - row.scheduled_at.max(row.created_at))
                    .to_std()
                    .ok()
            })
            .unwrap_or_default();
        let completion = CompletionAttempt {
            cancellation,
            timing: CompletionTiming {
                completion_started: std::time::Instant::now(),
                queue_wait_duration,
                run_duration: Duration::ZERO,
            },
        };
        match persist_result(
            &self.inner,
            &row,
            Utc::now(),
            &completion,
            result,
            context.metadata_updates(),
            error_handler_result,
            completion_sender,
        )
        .await?
        {
            PersistResult::Finished(Some(event)) => {
                let Event::Job(event) = *event else {
                    unreachable!("job persistence returns only job events")
                };
                let event = Event::job_with_statistics(
                    event.kind,
                    event.job,
                    JobStatistics {
                        complete_duration: completion.timing.completion_started.elapsed(),
                        queue_wait_duration,
                        run_duration: Duration::ZERO,
                    },
                );
                let _ = self.inner.events.send(event);
            }
            PersistResult::Enqueued | PersistResult::Finished(None) => {}
        }
        Ok(())
    }
}

impl Client {
    /// Inserts an encoded job through River's exact-version extension seam.
    #[doc(hidden)]
    pub async fn insert_raw(
        &self,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: Box<RawValue>,
        opts: InsertOpts,
    ) -> Result<RawInsertResult, Error> {
        let opts =
            InsertOpts::resolve(self.inner.default_max_attempts, InsertOpts::default(), opts);
        self.insert_raw_params(kind, unique_fields, encoded_args, opts)
            .await
    }

    /// Inserts an encoded job with already-resolved parameters through River's
    /// exact-version extension seam.
    #[doc(hidden)]
    pub async fn insert_raw_params(
        &self,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: Box<RawValue>,
        opts: InsertParams,
    ) -> Result<RawInsertResult, Error> {
        self.validate_known_kind(kind)?;
        let (mut job, unique_skipped_as_duplicate) = match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => {
                self.insert_encoded_on(pool, kind, unique_fields, &encoded_args, opts)
                    .await?
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                self.insert_encoded_on(pool, kind, unique_fields, &encoded_args, opts)
                    .await?
            }
        };
        for hook in self.inner.hooks.iter().rev() {
            hook.decode_insert_result(&mut job).await?;
        }
        self.signal_insert(&job, unique_skipped_as_duplicate);
        Ok(RawInsertResult {
            job,
            unique_skipped_as_duplicate,
        })
    }

    /// Inserts an encoded job inside a caller-managed transaction.
    #[doc(hidden)]
    pub async fn insert_raw_tx<'executor, E>(
        &self,
        connection: E,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: Box<RawValue>,
        opts: InsertOpts,
    ) -> Result<RawInsertResult, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        let opts =
            InsertOpts::resolve(self.inner.default_max_attempts, InsertOpts::default(), opts);
        self.insert_raw_params_tx(connection, kind, unique_fields, encoded_args, opts)
            .await
    }

    /// Inserts an encoded job with already-resolved parameters inside a
    /// caller-managed transaction.
    #[doc(hidden)]
    pub async fn insert_raw_params_tx<'executor, E>(
        &self,
        connection: E,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: Box<RawValue>,
        opts: InsertParams,
    ) -> Result<RawInsertResult, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        self.validate_known_kind(kind)?;
        let (mut job, unique_skipped_as_duplicate) = self
            .insert_encoded_on(connection, kind, unique_fields, &encoded_args, opts)
            .await?;
        for hook in self.inner.hooks.iter().rev() {
            hook.decode_insert_result(&mut job).await?;
        }
        Ok(RawInsertResult {
            job,
            unique_skipped_as_duplicate,
        })
    }

    /// Reinserts persisted fields through River's canonical insertion
    /// pipeline inside a caller-managed transaction.
    ///
    /// This exact-version operation lets the backend allocate the ID rather
    /// than explicitly retaining a source ID, and resets execution state while
    /// retaining the supplied creation, schedule, and uniqueness wire values.
    /// Begin hooks, insertion interception, middleware, end callbacks, and the
    /// backend notification all run exactly once.
    #[doc(hidden)]
    pub async fn extension_insert_tx<'executor, E>(
        &self,
        transaction: E,
        params: ExtensionInsertParams,
    ) -> Result<RawInsertResult, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        let mut source_row = JobRow {
            attempt: 0,
            attempted_at: None,
            attempted_by: Vec::new(),
            created_at: params.created_at,
            encoded_args: params.encoded_args,
            errors: Vec::new(),
            finalized_at: None,
            id: 0,
            kind: params.kind,
            max_attempts: params.max_attempts,
            metadata: params.metadata,
            priority: params.priority,
            queue: params.queue,
            scheduled_at: params.scheduled_at,
            state: JobState::Available,
            tags: params.tags,
            unique_key: params.unique_key,
            unique_states: params.unique_states,
        };
        // Exact-version callers supply persisted wire fields. Normalize them
        // before the ordinary begin pipeline so storage transforms are not
        // applied twice, then decode the newly persisted result below just as
        // a typed insertion does.
        for hook in self.inner.hooks.iter().rev() {
            hook.decode_insert_result(&mut source_row).await?;
        }
        let unique_states = match (&source_row.unique_key, &source_row.unique_states) {
            (None, None) => None,
            (Some(_), Some(states)) => Some(
                states
                    .iter()
                    .fold(0, |bitmask, state| bitmask | state.unique_bit()),
            ),
            _ => {
                return Err(Error::invalid_job_context(
                    "exact-version insertion",
                    "unique_key and unique_states must either both be set or both be absent"
                        .to_owned(),
                ));
            }
        };
        let opts = InsertParams {
            max_attempts: source_row.max_attempts,
            metadata: source_row.metadata,
            pending: false,
            priority: source_row.priority,
            queue: source_row.queue,
            scheduled_at: Some(source_row.scheduled_at),
            tags: source_row.tags,
            unique: crate::UniqueOpts::default(),
        };
        let executor = self
            .inner
            .erase_executor(transaction)
            .map_err(Error::from)?
            .into_inner();
        let (mut job, unique_skipped_as_duplicate) = self
            .insert_encoded_inner(
                executor,
                &source_row.kind,
                &[],
                &source_row.encoded_args,
                opts,
                Some(ExtensionInsertWire {
                    created_at: source_row.created_at,
                    unique_key: source_row.unique_key,
                    unique_states,
                }),
            )
            .await?;
        for hook in self.inner.hooks.iter().rev() {
            hook.decode_insert_result(&mut job).await?;
        }
        Ok(RawInsertResult {
            job,
            unique_skipped_as_duplicate,
        })
    }
}
