//! Job insertion.

#[allow(clippy::wildcard_imports)]
use super::*;

impl Client {
    /// Inserts a typed job using its job-type, client, and River defaults.
    pub async fn insert<A: JobArgs>(&self, args: A) -> Result<InsertResult<A>, Error> {
        self.insert_with(args, InsertOpts::default()).await
    }

    /// Inserts a typed job with options overlaid on its job-type defaults.
    pub async fn insert_with<A: JobArgs>(
        &self,
        args: A,
        opts: InsertOpts,
    ) -> Result<InsertResult<A>, Error> {
        let result = match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => self.insert_on(pool, args, opts).await?,
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => self.insert_on(pool, args, opts).await?,
        };
        self.signal_insert(&result.job.row, result.unique_skipped_as_duplicate);
        Ok(result)
    }

    /// Atomically inserts a batch containing one or more job argument types.
    ///
    /// Results correspond positionally to the batch items. Each item retains
    /// its own [`JobArgs`] defaults and uniqueness definition.
    pub async fn insert_batch(&self, batch: InsertBatch) -> Result<Vec<InsertBatchResult>, Error> {
        validate_nonempty_batch(&batch)?;
        let results = match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await?;
                let results = self.insert_batch_tx(&mut transaction, batch).await?;
                transaction.commit().await?;
                results
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                let mut transaction = crate::database::begin_sqlite_write(pool).await?;
                let results = self.insert_batch_tx(&mut transaction, batch).await?;
                transaction.commit().await?;
                results
            }
        };
        for result in &results {
            self.signal_insert(&result.job, result.unique_skipped_as_duplicate);
        }
        Ok(results)
    }

    /// Atomically inserts a heterogeneous batch in a caller-managed
    /// transaction.
    pub async fn insert_batch_tx<'executor, E>(
        &self,
        executor: E,
        batch: InsertBatch,
    ) -> Result<Vec<InsertBatchResult>, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        validate_nonempty_batch(&batch)?;
        match self
            .inner
            .erase_executor(executor)
            .map_err(Error::from)?
            .into_inner()
        {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                let savepoint = self.batch_savepoint("heterogeneous");
                begin_postgres_savepoint(connection, &savepoint).await?;
                let result = self.insert_batch_postgres(connection, batch).await;
                finish_postgres_savepoint(connection, &savepoint, result).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                let savepoint = self.batch_savepoint("heterogeneous");
                begin_sqlite_savepoint(connection, &savepoint).await?;
                let result = self.insert_batch_sqlite(connection, batch).await;
                finish_sqlite_savepoint(connection, &savepoint, result).await
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => Err(transaction_pool_error("insert_batch_tx")),
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => Err(transaction_pool_error("insert_batch_tx")),
        }
    }

    #[cfg(feature = "postgres")]
    pub(super) async fn insert_batch_postgres(
        &self,
        connection: &mut PgConnection,
        batch: InsertBatch,
    ) -> Result<Vec<InsertBatchResult>, Error> {
        self.insert_batch_on_postgres(connection, batch).await
    }

    #[cfg(feature = "postgres")]
    pub(super) async fn insert_batch_on_postgres(
        &self,
        connection: &mut PgConnection,
        batch: InsertBatch,
    ) -> Result<Vec<InsertBatchResult>, Error> {
        let mut results = Vec::with_capacity(batch.len());
        for item in batch.items {
            self.validate_known_kind(item.kind)?;
            let opts =
                InsertOpts::resolve(self.inner.default_max_attempts, item.defaults, item.opts);
            let encoded_args = item.encoded_args?;
            let (mut job, unique_skipped_as_duplicate) = self
                .insert_encoded_on(
                    &mut *connection,
                    item.kind,
                    item.unique_fields,
                    &encoded_args,
                    opts,
                )
                .await?;
            for hook in self.inner.hooks.iter().rev() {
                hook.decode_insert_result(&mut job).await?;
            }
            results.push(InsertBatchResult {
                job,
                unique_skipped_as_duplicate,
            });
        }
        Ok(results)
    }

    #[cfg(feature = "sqlite")]
    pub(super) async fn insert_batch_sqlite(
        &self,
        connection: &mut sqlx::SqliteConnection,
        batch: InsertBatch,
    ) -> Result<Vec<InsertBatchResult>, Error> {
        let mut results = Vec::with_capacity(batch.len());
        for item in batch.items {
            self.validate_known_kind(item.kind)?;
            let opts =
                InsertOpts::resolve(self.inner.default_max_attempts, item.defaults, item.opts);
            let encoded_args = item.encoded_args?;
            let (mut job, unique_skipped_as_duplicate) = self
                .insert_encoded_on(
                    &mut *connection,
                    item.kind,
                    item.unique_fields,
                    &encoded_args,
                    opts,
                )
                .await?;
            for hook in self.inner.hooks.iter().rev() {
                hook.decode_insert_result(&mut job).await?;
            }
            results.push(InsertBatchResult {
                job,
                unique_skipped_as_duplicate,
            });
        }
        Ok(results)
    }

    /// Inserts a homogeneous typed batch atomically using each job type's
    /// insertion defaults.
    pub async fn insert_many<A, I>(&self, jobs: I) -> Result<Vec<InsertResult<A>>, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = A>,
    {
        self.insert_many_with(jobs.into_iter().map(|args| (args, InsertOpts::default())))
            .await
    }

    /// Inserts a homogeneous typed batch atomically with per-job insertion
    /// options.
    pub async fn insert_many_with<A, I>(&self, jobs: I) -> Result<Vec<InsertResult<A>>, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let jobs = collect_nonempty_jobs(jobs)?;
        let results = match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await?;
                let results = self.insert_many_tx_with(&mut transaction, jobs).await?;
                transaction.commit().await?;
                results
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                let mut transaction = crate::database::begin_sqlite_write(pool).await?;
                let results = self.insert_many_tx_with(&mut transaction, jobs).await?;
                transaction.commit().await?;
                results
            }
        };
        for result in &results {
            self.signal_insert(&result.job.row, result.unique_skipped_as_duplicate);
        }
        Ok(results)
    }

    /// Inserts a typed batch using the backend's optimized atomic path and
    /// returns only the inserted row count. PostgreSQL uses COPY; SQLite uses
    /// a transactional fallback. A unique conflict fails the whole operation.
    /// Per-job begin hooks and insertion middleware run before persistence;
    /// successful completion uses the fast-insert callbacks because no rows
    /// are returned.
    pub async fn insert_many_fast<A, I>(&self, jobs: I) -> Result<u64, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = A>,
    {
        self.insert_many_fast_with(jobs.into_iter().map(|args| (args, InsertOpts::default())))
            .await
    }

    /// Inserts a typed batch with per-job options using the backend's
    /// optimized insertion path.
    pub async fn insert_many_fast_with<A, I>(&self, jobs: I) -> Result<u64, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let count = match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await?;
                let count = self
                    .insert_many_fast_tx_with(&mut transaction, jobs)
                    .await?;
                transaction.commit().await?;
                count
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                let mut transaction = crate::database::begin_sqlite_write(pool).await?;
                let count = self
                    .insert_many_fast_tx_with(&mut transaction, jobs)
                    .await?;
                transaction.commit().await?;
                count
            }
        };
        if count > 0 {
            let _ = self
                .inner
                .queue_notifications
                .send(RuntimeNotification::Insert("*".to_owned()));
        }
        Ok(count)
    }

    #[cfg(feature = "sqlite")]
    #[allow(
        clippy::too_many_lines,
        reason = "keeps fast-insert hooks, interception, persistence, and callbacks in one ordered path"
    )]
    pub(super) async fn insert_many_fast_sqlite<A, I>(
        &self,
        connection: &mut sqlx::SqliteConnection,
        jobs: I,
    ) -> Result<u64, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let now = Utc::now();
        let mut prepared = Vec::new();
        for (args, opts) in jobs {
            self.validate_known_kind(A::KIND)?;
            let mut insert = InsertContext {
                encoded_args: crate::encoding::encode_args(&args)?,
                kind: A::KIND.to_owned(),
                opts: InsertOpts::resolve(
                    self.inner.default_max_attempts,
                    A::default_insert_opts(),
                    opts,
                ),
            };
            for hook in &self.inner.hooks {
                hook.insert_begin(&mut insert).await?;
            }
            if self.inner.pilot.intercepts_insert() {
                let InsertContext {
                    encoded_args,
                    kind,
                    opts,
                } = &mut insert;
                self.inner
                    .pilot
                    .before_job_insert(
                        PilotDatabaseConnection::Sqlite(&mut *connection),
                        &mut PilotJobInsertParams {
                            encoded_args,
                            kind,
                            metadata: &mut opts.metadata,
                            queue: &mut opts.queue,
                        },
                    )
                    .await
                    .map_err(|source| Error::Extension {
                        phase: "job insertion",
                        source,
                    })?;
            }
            for middleware in &self.inner.insert_middleware {
                middleware.before_insert(&mut insert).await?;
            }
            validate_insert_parts(
                &insert.kind,
                &insert.opts,
                self.inner.allow_legacy_job_kinds,
            )?;
            prepared.push(PreparedFastInsert::new(insert, A::unique_fields(), now)?);
        }
        if prepared.is_empty() {
            return Err(Error::invalid_job("no jobs to insert".to_owned()));
        }

        let mut queues = std::collections::BTreeSet::new();
        for job in &prepared {
            let nonce = job.unique_key.map(|_| self.unique_insert_nonce());
            let inserted = crate::database::sqlite::insert(
                &mut *connection,
                &crate::database::sqlite::InsertJob {
                    attempt: 0,
                    attempted_at: None,
                    attempted_by: &[],
                    created_at: job.now,
                    encoded_args: &job.encoded_args,
                    errors: &[],
                    finalized_at: None,
                    id: None,
                    kind: &job.kind,
                    max_attempts: job.max_attempts,
                    metadata: &job.metadata,
                    priority: job.priority,
                    queue: &job.queue,
                    scheduled_at: job.scheduled_at,
                    state: job.state,
                    tags: &job.tags,
                    unique_key: job.unique_key.as_ref().map(<[u8; 32]>::as_slice),
                    unique_nonce: nonce.as_deref(),
                    unique_states: job.unique_states,
                },
            )
            .await
            .map_err(sqlite_backend_error)?;
            if inserted.unique_skipped_as_duplicate {
                return Err(Error::invalid_job(
                    "fast insertion encountered a unique conflict".to_owned(),
                ));
            }
            if inserted.job.state == JobState::Available {
                queues.insert(inserted.job.queue);
            }
        }
        for queue in queues {
            let payload = serde_json::json!({"queue": queue}).to_string();
            crate::database::sqlite::notification_insert(
                &mut *connection,
                &[crate::database::sqlite::NotificationInput {
                    payload: &payload,
                    topic: crate::NOTIFICATION_TOPIC_INSERT,
                }],
            )
            .await
            .map_err(sqlite_backend_error)?;
        }
        let count = u64::try_from(prepared.len()).unwrap_or(u64::MAX);
        for middleware in self.inner.insert_middleware.iter().rev() {
            middleware.after_insert_many_fast(count).await?;
        }
        for hook in &self.inner.hooks {
            hook.insert_many_fast_end(count).await?;
        }
        Ok(count)
    }

    /// Inserts a typed batch using the backend's optimized path inside a
    /// caller-managed transaction.
    pub async fn insert_many_fast_tx<'executor, A, I, E>(
        &self,
        executor: E,
        jobs: I,
    ) -> Result<u64, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = A>,
        E: DatabaseTransactionExecutor<'executor>,
    {
        self.insert_many_fast_tx_with(
            executor,
            jobs.into_iter().map(|args| (args, InsertOpts::default())),
        )
        .await
    }

    /// Inserts a typed batch with per-job options using the backend's
    /// optimized path inside a caller-managed transaction.
    pub async fn insert_many_fast_tx_with<'executor, A, I, E>(
        &self,
        executor: E,
        jobs: I,
    ) -> Result<u64, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
        E: DatabaseTransactionExecutor<'executor>,
    {
        match self
            .inner
            .erase_executor(executor)
            .map_err(Error::from)?
            .into_inner()
        {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                let savepoint = self.batch_savepoint("fast");
                begin_postgres_savepoint(connection, &savepoint).await?;
                let result = self.insert_many_fast_postgres(connection, jobs).await;
                finish_postgres_savepoint(connection, &savepoint, result).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                let savepoint = self.batch_savepoint("fast");
                begin_sqlite_savepoint(connection, &savepoint).await?;
                let result = self.insert_many_fast_sqlite(connection, jobs).await;
                finish_sqlite_savepoint(connection, &savepoint, result).await
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => Err(transaction_pool_error("insert_many_fast_tx")),
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => Err(transaction_pool_error("insert_many_fast_tx")),
        }
    }

    #[cfg(feature = "postgres")]
    pub(super) async fn insert_many_fast_postgres<A, I>(
        &self,
        connection: &mut PgConnection,
        jobs: I,
    ) -> Result<u64, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let now = Utc::now();
        let mut prepared = Vec::new();
        for (args, opts) in jobs {
            if !self.inner.allow_unregistered_job_kinds
                && !self.inner.workers.kinds().is_empty()
                && !self.inner.workers.contains_kind(A::KIND)
            {
                return Err(Error::UnknownJobKind(A::KIND.to_owned()));
            }
            let mut insert = InsertContext {
                encoded_args: crate::encoding::encode_args(&args)?,
                kind: A::KIND.to_owned(),
                opts: InsertOpts::resolve(
                    self.inner.default_max_attempts,
                    A::default_insert_opts(),
                    opts,
                ),
            };
            for hook in &self.inner.hooks {
                hook.insert_begin(&mut insert).await?;
            }
            if self.inner.pilot.intercepts_insert() {
                let InsertContext {
                    encoded_args,
                    kind,
                    opts,
                } = &mut insert;
                self.inner
                    .pilot
                    .before_job_insert(
                        PilotDatabaseConnection::Postgres(&mut *connection),
                        &mut PilotJobInsertParams {
                            encoded_args,
                            kind,
                            metadata: &mut opts.metadata,
                            queue: &mut opts.queue,
                        },
                    )
                    .await
                    .map_err(|source| Error::Extension {
                        phase: "job insertion",
                        source,
                    })?;
            }
            for middleware in &self.inner.insert_middleware {
                middleware.before_insert(&mut insert).await?;
            }
            validate_insert_parts(
                &insert.kind,
                &insert.opts,
                self.inner.allow_legacy_job_kinds,
            )?;
            prepared.push(PreparedFastInsert::new(insert, A::unique_fields(), now)?);
        }
        if prepared.is_empty() {
            return Err(Error::invalid_job("no jobs to insert".to_owned()));
        }

        let table = self.inner.schema.qualify("river_job");
        let copy_sql = format!(
            "COPY {table} (args, created_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states) FROM STDIN WITH (FORMAT csv, NULL '\\N')"
        );
        let data = encode_fast_copy(&prepared);
        let mut copy = connection.copy_in_raw(&copy_sql).await?;
        if let Err(copy_error) = copy.send(data).await {
            let _ = copy.abort("River fast insertion failed").await;
            return Err(copy_error.into());
        }
        let count = copy.finish().await?;

        let queues = prepared
            .iter()
            .filter(|job| job.state == JobState::Available)
            .map(|job| job.queue.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        for queue in queues {
            sqlx::query(
                "SELECT pg_notify(concat(coalesce($1::text, current_schema()), '.', $2::text), json_build_object('queue', $3::text)::text)",
            )
            .bind(self.inner.schema.as_deref())
            .bind(crate::NOTIFICATION_TOPIC_INSERT)
            .bind(queue)
            .execute(&mut *connection)
            .await?;
        }
        for middleware in self.inner.insert_middleware.iter().rev() {
            middleware.after_insert_many_fast(count).await?;
        }
        for hook in &self.inner.hooks {
            hook.insert_many_fast_end(count).await?;
        }
        Ok(count)
    }

    /// Inserts a homogeneous typed batch on a caller-managed transaction using
    /// each job type's insertion defaults.
    pub async fn insert_many_tx<'executor, A, I, E>(
        &self,
        executor: E,
        jobs: I,
    ) -> Result<Vec<InsertResult<A>>, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = A>,
        E: DatabaseTransactionExecutor<'executor>,
    {
        self.insert_many_tx_with(
            executor,
            jobs.into_iter().map(|args| (args, InsertOpts::default())),
        )
        .await
    }

    /// Inserts a homogeneous typed batch with per-job options on a
    /// caller-managed transaction. The caller chooses commit or rollback
    /// visibility.
    pub async fn insert_many_tx_with<'executor, A, I, E>(
        &self,
        executor: E,
        jobs: I,
    ) -> Result<Vec<InsertResult<A>>, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
        E: DatabaseTransactionExecutor<'executor>,
    {
        let jobs = collect_nonempty_jobs(jobs)?;
        match self
            .inner
            .erase_executor(executor)
            .map_err(Error::from)?
            .into_inner()
        {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                let savepoint = self.batch_savepoint("regular");
                begin_postgres_savepoint(connection, &savepoint).await?;
                let result = self.insert_many_postgres(connection, jobs).await;
                finish_postgres_savepoint(connection, &savepoint, result).await
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                let savepoint = self.batch_savepoint("regular");
                begin_sqlite_savepoint(connection, &savepoint).await?;
                let result = self.insert_many_sqlite(connection, jobs).await;
                finish_sqlite_savepoint(connection, &savepoint, result).await
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(_) => Err(transaction_pool_error("insert_many_tx")),
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(_) => Err(transaction_pool_error("insert_many_tx")),
        }
    }

    /// Returns a nonce that marks a SQLite unique insert as this call's own.
    ///
    /// SQLite reports a skipped duplicate by checking whether the returned
    /// row carries the nonce the insert wrote. Like River Go's
    /// `randutil.Hex(8)`, the nonce must not repeat across processes: client
    /// IDs and counters can (a restarted container keeps its hostname and
    /// PID), so 128 unpredictable bits are drawn from the standard library's
    /// randomly keyed hasher.
    #[cfg(feature = "sqlite")]
    pub(super) fn unique_insert_nonce(&self) -> String {
        use std::hash::{BuildHasher as _, Hasher as _};

        let counter = self.inner.unique_nonce.fetch_add(1, Ordering::Relaxed);
        let mut halves = [0_u64; 2];
        for (index, half) in halves.iter_mut().enumerate() {
            let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
            hasher.write_u64(counter);
            hasher.write_usize(index);
            hasher.write_u128(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
            );
            *half = hasher.finish();
        }
        format!("{:016x}{:016x}", halves[0], halves[1])
    }

    pub(super) fn batch_savepoint(&self, operation: &str) -> String {
        let nonce = self.inner.unique_nonce.fetch_add(1, Ordering::Relaxed);
        format!("river_{operation}_insert_many_{nonce}")
    }

    #[cfg(feature = "postgres")]
    pub(super) async fn insert_many_postgres<A, I>(
        &self,
        connection: &mut PgConnection,
        jobs: I,
    ) -> Result<Vec<InsertResult<A>>, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let jobs = jobs.into_iter();
        let (lower, _) = jobs.size_hint();
        let mut results = Vec::with_capacity(lower);
        for (args, opts) in jobs {
            results.push(self.insert_on(&mut *connection, args, opts).await?);
        }
        Ok(results)
    }

    #[cfg(feature = "sqlite")]
    pub(super) async fn insert_many_sqlite<A, I>(
        &self,
        connection: &mut sqlx::SqliteConnection,
        jobs: I,
    ) -> Result<Vec<InsertResult<A>>, Error>
    where
        A: JobArgs,
        I: IntoIterator<Item = (A, InsertOpts)>,
    {
        let jobs = jobs.into_iter();
        let (lower, _) = jobs.size_hint();
        let mut results = Vec::with_capacity(lower);
        for (args, opts) in jobs {
            results.push(self.insert_on(&mut *connection, args, opts).await?);
        }
        Ok(results)
    }
}

impl Client {
    /// Inserts a typed job on a caller-managed transaction using
    /// its job-type, client, and River defaults.
    pub async fn insert_tx<'executor, A, E>(
        &self,
        connection: E,
        args: A,
    ) -> Result<InsertResult<A>, Error>
    where
        A: JobArgs,
        E: DatabaseTransactionExecutor<'executor>,
    {
        self.insert_tx_with(connection, args, InsertOpts::default())
            .await
    }

    /// Inserts a typed job with options on a caller-managed transaction.
    pub async fn insert_tx_with<'executor, A, E>(
        &self,
        connection: E,
        args: A,
        opts: InsertOpts,
    ) -> Result<InsertResult<A>, Error>
    where
        A: JobArgs,
        E: DatabaseTransactionExecutor<'executor>,
    {
        self.insert_on(connection, args, opts).await
    }

    /// Resolves typed insertion options for an exact-version extension.
    #[doc(hidden)]
    #[must_use]
    pub fn resolve_insert_opts<A: JobArgs>(&self, opts: InsertOpts) -> InsertParams {
        InsertOpts::resolve(
            self.inner.default_max_attempts,
            A::default_insert_opts(),
            opts,
        )
    }
}

impl Client {
    pub(super) async fn insert_on<'executor, A, E>(
        &self,
        executor: E,
        args: A,
        opts: InsertOpts,
    ) -> Result<InsertResult<A>, Error>
    where
        A: JobArgs,
        E: DatabaseExecutor<'executor>,
    {
        self.validate_known_kind(A::KIND)?;
        let encoded_args = crate::encoding::encode_args(&args)?;
        let opts = InsertOpts::resolve(
            self.inner.default_max_attempts,
            A::default_insert_opts(),
            opts,
        );
        let (mut row, unique_skipped_as_duplicate) = self
            .insert_encoded_on(executor, A::KIND, A::unique_fields(), &encoded_args, opts)
            .await?;
        for hook in self.inner.hooks.iter().rev() {
            hook.decode_insert_result(&mut row).await?;
        }
        let args = row.decode_args()?;
        Ok(InsertResult {
            job: Job { args, row },
            unique_skipped_as_duplicate,
        })
    }

    pub(crate) async fn insert_periodic(
        &self,
        insert: PeriodicInsert,
        opts: InsertParams,
    ) -> Result<JobRow, Error> {
        validate_insert_parts(insert.kind, &opts, self.inner.allow_legacy_job_kinds)?;
        let (mut row, unique_skipped_as_duplicate) = match self.inner.database.pool() {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => {
                self.insert_encoded_on(
                    pool,
                    insert.kind,
                    insert.unique_fields,
                    &insert.encoded_args,
                    opts,
                )
                .await?
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                self.insert_encoded_on(
                    pool,
                    insert.kind,
                    insert.unique_fields,
                    &insert.encoded_args,
                    opts,
                )
                .await?
            }
        };
        for hook in self.inner.hooks.iter().rev() {
            hook.decode_insert_result(&mut row).await?;
        }
        self.signal_insert(&row, unique_skipped_as_duplicate);
        Ok(row)
    }
}

impl Client {
    #[allow(
        clippy::too_many_lines,
        reason = "keeps backend insert hook ordering identical across dispatch branches"
    )]
    pub(super) async fn insert_encoded_on<'executor, E>(
        &self,
        executor: E,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: &RawValue,
        opts: InsertParams,
    ) -> Result<(JobRow, bool), Error>
    where
        E: DatabaseExecutor<'executor>,
    {
        // Resolve and validate the backend before invoking user hooks or
        // middleware so a mismatched executor cannot cause side effects.
        let executor = self
            .inner
            .erase_executor(executor)
            .map_err(Error::from)?
            .into_inner();
        if self.inner.pilot.intercepts_insert() {
            match executor {
                #[cfg(feature = "postgres")]
                ExecutorInner::PostgresPool(pool) => {
                    let mut transaction = pool.begin().await?;
                    let result = self
                        .insert_encoded_inner(
                            ExecutorInner::PostgresConnection(&mut transaction),
                            kind,
                            unique_fields,
                            encoded_args,
                            opts,
                            None,
                        )
                        .await?;
                    transaction.commit().await?;
                    return Ok(result);
                }
                #[cfg(feature = "sqlite")]
                ExecutorInner::SqlitePool(pool) => {
                    let mut transaction = crate::database::begin_sqlite_write(pool).await?;
                    let result = self
                        .insert_encoded_inner(
                            ExecutorInner::SqliteConnection(&mut transaction),
                            kind,
                            unique_fields,
                            encoded_args,
                            opts,
                            None,
                        )
                        .await?;
                    transaction.commit().await?;
                    return Ok(result);
                }
                executor => {
                    return self
                        .insert_encoded_inner(
                            executor,
                            kind,
                            unique_fields,
                            encoded_args,
                            opts,
                            None,
                        )
                        .await;
                }
            }
        }
        self.insert_encoded_inner(executor, kind, unique_fields, encoded_args, opts, None)
            .await
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "keeps backend insertion and exact-version wire semantics aligned"
    )]
    pub(super) async fn insert_encoded_inner(
        &self,
        mut executor: ExecutorInner<'_>,
        kind: &str,
        unique_fields: &[&str],
        encoded_args: &RawValue,
        opts: InsertParams,
        wire: Option<ExtensionInsertWire>,
    ) -> Result<(JobRow, bool), Error> {
        let mut insert = InsertContext {
            encoded_args: encoded_args.to_owned(),
            kind: kind.to_owned(),
            opts,
        };
        for hook in &self.inner.hooks {
            hook.insert_begin(&mut insert).await?;
        }
        if self.inner.pilot.intercepts_insert() {
            let connection = match &mut executor {
                #[cfg(feature = "postgres")]
                ExecutorInner::PostgresConnection(connection) => {
                    PilotDatabaseConnection::Postgres(connection)
                }
                #[cfg(feature = "sqlite")]
                ExecutorInner::SqliteConnection(connection) => {
                    PilotDatabaseConnection::Sqlite(connection)
                }
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                _ => {
                    return Err(Error::runtime_context(
                        "job insertion interception",
                        "insertion pilot requires a transaction connection".to_owned(),
                    ));
                }
            };
            let InsertContext {
                encoded_args,
                kind,
                opts,
            } = &mut insert;
            self.inner
                .pilot
                .before_job_insert(
                    connection,
                    &mut PilotJobInsertParams {
                        encoded_args,
                        kind,
                        metadata: &mut opts.metadata,
                        queue: &mut opts.queue,
                    },
                )
                .await
                .map_err(|source| Error::Extension {
                    phase: "job insertion",
                    source,
                })?;
        }
        for middleware in &self.inner.insert_middleware {
            middleware.before_insert(&mut insert).await?;
        }
        if wire.is_none() {
            validate_insert_parts(
                &insert.kind,
                &insert.opts,
                self.inner.allow_legacy_job_kinds,
            )?;
        }
        let InsertContext {
            encoded_args,
            kind,
            opts,
        } = insert;
        let now = Utc::now();
        let (created_at, state, unique_key, unique_states) = if let Some(wire) = wire {
            (
                Some(wire.created_at),
                JobState::Available,
                wire.unique_key,
                wire.unique_states,
            )
        } else {
            let unique_key = build_unique_key_parts(
                &kind,
                unique_fields,
                &encoded_args,
                now,
                &opts.unique,
                &opts.queue,
                opts.scheduled_at,
            )?
            .map(|key| key.to_vec());
            let unique_states = unique_key.as_ref().map(|_| opts.unique.state_bitmask());
            let state = if opts.pending {
                JobState::Pending
            } else if opts.scheduled_at.is_some() {
                JobState::Scheduled
            } else {
                JobState::Available
            };
            (None, state, unique_key, unique_states)
        };
        #[cfg(feature = "postgres")]
        let table = self.inner.schema.qualify("river_job");
        #[cfg(feature = "postgres")]
        let state_type = self.inner.schema.qualify("river_job_state");
        #[cfg(feature = "postgres")]
        let state_function = self.inner.schema.qualify("river_job_state_in_bitmask");
        // The no-op update is intentional and matches River Go. `DO NOTHING`
        // followed by a select in this CTE cannot see a conflicting row that
        // committed after the statement snapshot was taken.
        #[cfg(feature = "postgres")]
        let sql = format!(
            "WITH inserted AS (\
                INSERT INTO {table} (args, created_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states) \
                VALUES ($1, coalesce($2, now()), $3, $4, $5, $6, $7, coalesce($8, now()), $9::text::{state_type}, $10, $11, $12::integer::bit(8)) \
                ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND unique_states IS NOT NULL AND {state_function}(unique_states, state) \
                DO UPDATE SET kind = EXCLUDED.kind \
                RETURNING *, (xmax != 0) AS unique_skipped_as_duplicate\
             ), notified AS (\
                SELECT pg_notify(concat(coalesce($13::text, current_schema()), '.', $14::text), json_build_object('queue', queue)::text) \
                FROM inserted WHERE state = 'available' AND NOT unique_skipped_as_duplicate\
             ) \
             SELECT {}, job.unique_skipped_as_duplicate \
             FROM inserted AS job LEFT JOIN notified ON true",
            job_projection("job")
        );
        #[cfg(feature = "postgres")]
        let postgres_query = || {
            sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql.clone()))
                .bind(Json(&encoded_args))
                .bind(created_at)
                .bind(&kind)
                .bind(opts.max_attempts)
                .bind(Json(&opts.metadata))
                .bind(opts.priority)
                .bind(&opts.queue)
                .bind(opts.scheduled_at)
                .bind(state.as_str())
                .bind(&opts.tags)
                .bind(unique_key.clone())
                .bind(unique_states.map(i32::from))
                .bind(self.inner.schema.as_deref())
                .bind(crate::NOTIFICATION_TOPIC_INSERT)
        };
        let (row, unique_skipped_as_duplicate) = match executor {
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresConnection(connection) => {
                let record = postgres_query()
                    .fetch_optional(connection)
                    .await?
                    .ok_or_else(|| {
                        Error::invalid_job("unique insert found no conflicting row".to_owned())
                    })?;
                let duplicate = record.unique_skipped_as_duplicate;
                (record.into_job_row()?, duplicate)
            }
            #[cfg(feature = "postgres")]
            ExecutorInner::PostgresPool(pool) => {
                let record = postgres_query()
                    .fetch_optional(pool)
                    .await?
                    .ok_or_else(|| {
                        Error::invalid_job("unique insert found no conflicting row".to_owned())
                    })?;
                let duplicate = record.unique_skipped_as_duplicate;
                (record.into_job_row()?, duplicate)
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqliteConnection(connection) => {
                let nonce = unique_key.as_ref().map(|_| self.unique_insert_nonce());
                let inserted = crate::database::sqlite::insert(
                    connection,
                    &crate::database::sqlite::InsertJob {
                        attempt: 0,
                        attempted_at: None,
                        attempted_by: &[],
                        created_at: created_at.unwrap_or(now),
                        encoded_args: &encoded_args,
                        errors: &[],
                        finalized_at: None,
                        id: None,
                        kind: &kind,
                        max_attempts: opts.max_attempts,
                        metadata: &opts.metadata,
                        priority: opts.priority,
                        queue: &opts.queue,
                        scheduled_at: opts.scheduled_at.unwrap_or(now),
                        state,
                        tags: &opts.tags,
                        unique_key: unique_key.as_deref(),
                        unique_nonce: nonce.as_deref(),
                        unique_states,
                    },
                )
                .await
                .map_err(sqlite_backend_error)?;
                if inserted.job.state == JobState::Available
                    && !inserted.unique_skipped_as_duplicate
                {
                    let payload = serde_json::json!({"queue": inserted.job.queue}).to_string();
                    crate::database::sqlite::notification_insert(
                        connection,
                        &[crate::database::sqlite::NotificationInput {
                            payload: &payload,
                            topic: crate::NOTIFICATION_TOPIC_INSERT,
                        }],
                    )
                    .await
                    .map_err(sqlite_backend_error)?;
                }
                (inserted.job, inserted.unique_skipped_as_duplicate)
            }
            #[cfg(feature = "sqlite")]
            ExecutorInner::SqlitePool(pool) => {
                let mut transaction = crate::database::begin_sqlite_write(pool).await?;
                let nonce = unique_key.as_ref().map(|_| self.unique_insert_nonce());
                let inserted = crate::database::sqlite::insert(
                    &mut transaction,
                    &crate::database::sqlite::InsertJob {
                        attempt: 0,
                        attempted_at: None,
                        attempted_by: &[],
                        created_at: created_at.unwrap_or(now),
                        encoded_args: &encoded_args,
                        errors: &[],
                        finalized_at: None,
                        id: None,
                        kind: &kind,
                        max_attempts: opts.max_attempts,
                        metadata: &opts.metadata,
                        priority: opts.priority,
                        queue: &opts.queue,
                        scheduled_at: opts.scheduled_at.unwrap_or(now),
                        state,
                        tags: &opts.tags,
                        unique_key: unique_key.as_deref(),
                        unique_nonce: nonce.as_deref(),
                        unique_states,
                    },
                )
                .await
                .map_err(sqlite_backend_error)?;
                if inserted.job.state == JobState::Available
                    && !inserted.unique_skipped_as_duplicate
                {
                    let payload = serde_json::json!({"queue": inserted.job.queue}).to_string();
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
                (inserted.job, inserted.unique_skipped_as_duplicate)
            }
        };
        for middleware in self.inner.insert_middleware.iter().rev() {
            middleware
                .after_insert(&row, unique_skipped_as_duplicate)
                .await?;
        }
        for hook in &self.inner.hooks {
            hook.insert_end(&row, unique_skipped_as_duplicate).await?;
        }
        Ok((row, unique_skipped_as_duplicate))
    }
}

pub(super) fn collect_nonempty_jobs<T>(jobs: impl IntoIterator<Item = T>) -> Result<Vec<T>, Error> {
    let jobs = jobs.into_iter().collect::<Vec<_>>();
    if jobs.is_empty() {
        return Err(Error::invalid_job("no jobs to insert".to_owned()));
    }
    Ok(jobs)
}

pub(super) fn validate_nonempty_batch(batch: &InsertBatch) -> Result<(), Error> {
    if batch.is_empty() {
        return Err(Error::invalid_job("no jobs to insert".to_owned()));
    }
    Ok(())
}

pub(super) struct ExtensionInsertWire {
    pub(super) created_at: DateTime<Utc>,
    pub(super) unique_key: Option<Vec<u8>>,
    pub(super) unique_states: Option<u8>,
}

pub(super) struct PreparedFastInsert {
    pub(super) encoded_args: Box<RawValue>,
    pub(super) kind: String,
    pub(super) max_attempts: i16,
    pub(super) metadata: Map<String, Value>,
    pub(super) now: DateTime<Utc>,
    pub(super) priority: i16,
    pub(super) queue: String,
    pub(super) scheduled_at: DateTime<Utc>,
    pub(super) state: JobState,
    pub(super) tags: Vec<String>,
    pub(super) unique_key: Option<[u8; 32]>,
    pub(super) unique_states: Option<u8>,
}

impl PreparedFastInsert {
    pub(super) fn new(
        insert: InsertContext,
        unique_fields: &[&str],
        now: DateTime<Utc>,
    ) -> Result<Self, Error> {
        let InsertContext {
            encoded_args,
            kind,
            opts,
        } = insert;
        let unique_key = build_unique_key_parts(
            &kind,
            unique_fields,
            &encoded_args,
            now,
            &opts.unique,
            &opts.queue,
            opts.scheduled_at,
        )?;
        let unique_states = unique_key.map(|_| opts.unique.state_bitmask());
        let scheduled_at = opts.scheduled_at.unwrap_or(now);
        let state = if opts.pending {
            JobState::Pending
        } else if opts.scheduled_at.is_some() {
            JobState::Scheduled
        } else {
            JobState::Available
        };
        Ok(Self {
            encoded_args,
            kind,
            max_attempts: opts.max_attempts,
            metadata: opts.metadata,
            now,
            priority: opts.priority,
            queue: opts.queue,
            scheduled_at,
            state,
            tags: opts.tags,
            unique_key,
            unique_states,
        })
    }
}

#[cfg(feature = "postgres")]
pub(super) fn encode_fast_copy(jobs: &[PreparedFastInsert]) -> Vec<u8> {
    let mut output = String::new();
    for job in jobs {
        let unique_key = job.unique_key.map(|key| {
            let mut value = String::from("\\x");
            for byte in key {
                write!(value, "{byte:02x}").expect("writing to a string cannot fail");
            }
            value
        });
        let unique_states = job.unique_states.map(|states| format!("{states:08b}"));
        let fields = [
            Some(job.encoded_args.to_string()),
            Some(job.now.to_rfc3339_opts(SecondsFormat::Micros, true)),
            Some(job.kind.clone()),
            Some(job.max_attempts.to_string()),
            Some(Value::Object(job.metadata.clone()).to_string()),
            Some(job.priority.to_string()),
            Some(job.queue.clone()),
            Some(
                job.scheduled_at
                    .to_rfc3339_opts(SecondsFormat::Micros, true),
            ),
            Some(job.state.as_str().to_owned()),
            Some(postgres_array(&job.tags)),
            unique_key,
            unique_states,
        ];
        for (index, field) in fields.iter().enumerate() {
            if index > 0 {
                output.push(',');
            }
            match field {
                Some(field) => {
                    output.push('"');
                    output.push_str(&field.replace('"', "\"\""));
                    output.push('"');
                }
                None => output.push_str("\\N"),
            }
        }
        output.push('\n');
    }
    output.into_bytes()
}

#[cfg(feature = "postgres")]
pub(super) fn postgres_array(values: &[String]) -> String {
    let values = values
        .iter()
        .map(|value| format!(r#""{}""#, value.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{values}}}")
}
