//! Unstable extension entry points used by companion crates.

#[allow(clippy::wildcard_imports)]
use super::*;
use crate::__private::{ExtensionClaimParams, ExtensionInsertParams, RawInsertResult};

/// Client operations reserved for River's own companion crates.
///
/// Reached through `riverqueue::__private`, this wrapper keeps these
/// operations off [`Client`]'s public API.
#[derive(Clone, Copy, Debug)]
pub struct ExtensionClient<'client> {
    client: &'client Client,
}

impl<'client> ExtensionClient<'client> {
    /// Wraps a client.
    #[must_use]
    pub const fn new(client: &'client Client) -> Self {
        Self { client }
    }

    /// Returns the wrapped client.
    #[must_use]
    pub const fn client(&self) -> &'client Client {
        self.client
    }

    /// Creates a non-owning handle for an extension service.
    #[must_use]
    pub fn downgrade(&self) -> WeakClient {
        self.client.downgrade()
    }

    /// Resolves typed insertion options the same way a typed insert does.
    #[must_use]
    pub fn resolve_insert_opts<A: JobArgs>(&self, opts: InsertOpts) -> InsertParams {
        self.client.resolve_insert_opts::<A>(opts)
    }
}

impl ExtensionClient<'_> {
    /// Atomically claims complete job rows for an exact-version extension.
    ///
    /// Eligible jobs are available, due, and match the supplied kind, queue,
    /// and top-level metadata values. River records the attempt and client ID,
    /// applies the metadata updates in the same transaction, and returns rows
    /// ordered by priority, scheduled time, and ID.
    ///
    /// # Errors
    ///
    /// Returns an error when the claim query or its transaction fails.
    ///
    /// # Panics
    ///
    /// Panics if a PostgreSQL client has no PostgreSQL pool, which River's
    /// builder never constructs.
    #[expect(
        clippy::too_many_lines,
        reason = "each backend's claim transaction reads best inline"
    )]
    pub async fn claim_jobs(&self, params: ExtensionClaimParams) -> Result<Vec<JobRow>, Error> {
        if params.maximum <= 0 {
            return Ok(Vec::new());
        }
        #[cfg(feature = "sqlite")]
        if let Some(pool) = self.client.inner.sqlite_pool() {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let rows = crate::database::sqlite::claim_filtered(
                &mut transaction,
                &crate::database::sqlite::ClaimFilteredJobs {
                    client_id: &self.client.inner.id,
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
                .client
                .inner
                .postgres_pool()
                .expect("PostgreSQL claim path requires a PostgreSQL pool");
            let table = self.client.inner.schema.qualify("river_job");
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
                .bind(&self.client.inner.id)
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
    #[must_use]
    pub fn retry_delay(&self, row: &JobRow, error: &str, now: DateTime<Utc>) -> Duration {
        self.client.inner.retry_policy.next_retry(row, error, now)
    }

    /// Returns the scheduler horizon used by exact-version completion helpers.
    #[must_use]
    pub fn scheduler_interval(&self) -> Duration {
        self.client.inner.maintenance.scheduler_interval
    }

    /// Reports outcomes for jobs claimed and executed by an exact-version
    /// extension through River's canonical completion pipeline.
    ///
    /// The extension's handler context supplies metadata updates shared by the
    /// execution. Each failed outcome runs the error handler for its own job,
    /// then every outcome uses ordinary retry selection, persistence batching
    /// (including the extension's set-state hook and retries), event
    /// delivery, and statistics.
    /// Work middleware and work hooks are deliberately not invoked again: they
    /// surround the extension's handler once, before it reports these results.
    /// Outcomes racing an external terminal transition preserve the persisted
    /// state while retaining the submitted event reason.
    ///
    /// # Errors
    ///
    /// Returns an error when the client runtime is not accepting completions.
    /// Persistence failures, including errors from the extension's set-state
    /// hook, are retried and reported by the running completion service,
    /// matching regular worker behavior.
    pub async fn persist_claimed_outcomes(
        &self,
        execution_context: &WorkContext,
        outcomes: Vec<(JobRow, Result<WorkOutcome, BoxError>)>,
    ) -> Result<(), Error> {
        if outcomes.is_empty() {
            return Ok(());
        }
        let metadata_updates = execution_context.metadata_updates();
        let completion_sender = self
            .client
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
                .persist_claimed_outcome(
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

    async fn persist_claimed_outcome(
        &self,
        row: JobRow,
        result: Result<WorkOutcome, BoxError>,
        execution_cancellation: &CancellationToken,
        metadata_updates: &Map<String, Value>,
        completion_sender: &mpsc::Sender<CompletionUpdate>,
    ) -> Result<(), Error> {
        let cancellation = CancellationToken::new();
        let context = WorkContext::for_job(
            self.client.clone(),
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
        if let Some(error_handler) = &self.client.inner.error_handler
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
        persist_result(
            &self.client.inner,
            &row,
            Utc::now(),
            &completion,
            result,
            context.metadata_updates(),
            error_handler_result,
            completion_sender,
        )
        .await
    }

    /// Inserts an encoded job through River's exact-version extension seam.
    pub async fn insert_raw(
        &self,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: Box<RawValue>,
        opts: InsertOpts,
    ) -> Result<RawInsertResult, Error> {
        let opts = InsertOpts::resolve(
            self.client.inner.default_max_attempts,
            InsertOpts::default(),
            opts,
        );
        self.insert_raw_params(kind, unique_fields, encoded_args, opts)
            .await
    }

    /// Inserts an encoded job with already-resolved parameters through River's
    /// exact-version extension seam.
    pub async fn insert_raw_params(
        &self,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: Box<RawValue>,
        opts: InsertParams,
    ) -> Result<RawInsertResult, Error> {
        self.client.validate_known_kind(kind)?;
        let job =
            self.client
                .prepare_encoded(kind, unique_fields, encoded_args, opts, Utc::now())?;
        self.insert_raw_job(None, job).await
    }

    /// Inserts an encoded job inside a caller-managed transaction.
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
        let opts = InsertOpts::resolve(
            self.client.inner.default_max_attempts,
            InsertOpts::default(),
            opts,
        );
        self.insert_raw_params_tx(connection, kind, unique_fields, encoded_args, opts)
            .await
    }

    /// Inserts an encoded job with already-resolved parameters inside a
    /// caller-managed transaction.
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
        let executor = self
            .client
            .inner
            .erase_executor(connection)
            .map_err(Error::from)?
            .into_inner();
        self.client.validate_known_kind(kind)?;
        let job =
            self.client
                .prepare_encoded(kind, unique_fields, encoded_args, opts, Utc::now())?;
        self.insert_raw_job(Some(executor), job).await
    }

    /// Inserts an encoded periodic job due at `target` inside a
    /// caller-managed transaction, exactly as River's periodic job enqueuer
    /// does.
    ///
    /// When `opts.scheduled_at` is unset, the job is inserted `available`
    /// with `scheduled_at` set to `target` so it runs immediately, and a
    /// `by_period` unique key uses the target's period. An explicit
    /// `scheduled_at` inserts a `scheduled` job, and `pending` is kept.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction belongs to another backend, the
    /// kind is not registered, the options are invalid, or insertion fails.
    pub async fn insert_periodic_tx<'executor, E>(
        &self,
        transaction: E,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: Box<RawValue>,
        opts: InsertParams,
        target: DateTime<Utc>,
    ) -> Result<RawInsertResult, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        let executor = self
            .client
            .inner
            .erase_executor(transaction)
            .map_err(Error::from)?
            .into_inner();
        self.client.validate_known_kind(kind)?;
        let job = self.client.prepare_periodic(
            kind,
            unique_fields,
            encoded_args,
            opts,
            target,
            Utc::now(),
        )?;
        self.insert_raw_job(Some(executor), job).await
    }

    /// Reinserts persisted fields through River's canonical insertion
    /// pipeline inside a caller-managed transaction.
    ///
    /// This exact-version operation lets the backend allocate the ID rather
    /// than explicitly retaining a source ID, and resets execution state while
    /// retaining the supplied creation, schedule, and uniqueness wire values.
    /// Insertion middleware, begin hooks, insertion interception, and the
    /// backend notification all run exactly once.
    pub async fn insert_tx<'executor, E>(
        &self,
        transaction: E,
        params: ExtensionInsertParams,
    ) -> Result<RawInsertResult, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        let executor = self
            .client
            .inner
            .erase_executor(transaction)
            .map_err(Error::from)?
            .into_inner();
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
        for hook in self.client.inner.hooks.iter().rev() {
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
        let job = InsertContext {
            encoded_args: source_row.encoded_args,
            kind: source_row.kind,
            opts: InsertParams {
                max_attempts: source_row.max_attempts,
                metadata: source_row.metadata,
                pending: false,
                priority: source_row.priority,
                queue: source_row.queue,
                scheduled_at: Some(source_row.scheduled_at),
                tags: source_row.tags,
                unique: crate::UniqueOpts::default(),
            },
            state: JobState::Available,
            created_at: Some(source_row.created_at),
            unique_key: source_row.unique_key,
            unique_states,
        };
        self.insert_raw_job(Some(executor), job).await
    }

    async fn insert_raw_job(
        &self,
        executor: Option<ExecutorInner<'_>>,
        job: InsertContext,
    ) -> Result<RawInsertResult, Error> {
        let rows = self
            .client
            .run_insert(executor, vec![job], InsertMode::Rows)
            .await?;
        let row = rows.into_iter().next().ok_or_else(|| {
            Error::runtime_context("exact-version insertion", "insertion returned no row")
        })?;
        Ok(RawInsertResult {
            job: row.job,
            unique_skipped_as_duplicate: row.unique_skipped_as_duplicate,
        })
    }
}
