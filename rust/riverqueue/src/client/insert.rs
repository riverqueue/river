//! Job insertion.
//!
//! Every insertion, whether a single typed job, a homogeneous or
//! heterogeneous batch, a periodic job, or an extension's raw insert, runs
//! through one pipeline that mirrors River Go's `insertManyShared`:
//!
//! 1. Options are resolved and validated, and the unique key and initial
//!    state are computed from the original arguments.
//! 2. Insertion middleware wraps the rest of the operation.
//! 3. Inside the middleware, begin hooks and any extension interception run
//!    for each job, the jobs are written, and one insert notification is sent
//!    per queue that gained available jobs.
//! 4. Decode hooks run on returned rows.

use std::{
    fmt,
    future::{Future, IntoFuture},
    pin::Pin,
};

#[allow(clippy::wildcard_imports)]
use super::*;
use crate::extension::{InsertEndpoint, InsertNext, InsertedJob, InsertedJobs};

/// One job of a homogeneous [`Client::insert_many`] batch: arguments plus
/// options that override the job type's defaults.
///
/// Batches accept bare arguments or `(args, opts)` tuples, both of which
/// convert into this type.
#[derive(Debug)]
pub struct InsertManyItem<A> {
    args: A,
    opts: InsertOpts,
}

impl<A> InsertManyItem<A> {
    /// Pairs job arguments with insertion options.
    pub const fn new(args: A, opts: InsertOpts) -> Self {
        Self { args, opts }
    }
}

impl<A: JobArgs> From<A> for InsertManyItem<A> {
    fn from(args: A) -> Self {
        Self::new(args, InsertOpts::default())
    }
}

impl<A: JobArgs> From<(A, InsertOpts)> for InsertManyItem<A> {
    fn from((args, opts): (A, InsertOpts)) -> Self {
        Self::new(args, opts)
    }
}

/// A single-job insertion, returned by [`Client::insert`]. Await it to insert
/// the job.
///
/// The job type's defaults, the client's defaults, and River's defaults apply
/// unless overridden with [`opts`](Self::opts).
#[must_use = "insert requests do nothing unless awaited"]
pub struct InsertRequest<'a, A> {
    args: A,
    client: &'a Client,
    opts: InsertOpts,
    target: Target<'a>,
}

impl<'a, A: JobArgs> InsertRequest<'a, A> {
    /// Overrides options for this job. Options not set here fall back to
    /// the job type's defaults.
    pub fn opts(mut self, opts: InsertOpts) -> Self {
        self.opts = opts;
        self
    }

    /// Inserts the job in a caller-managed transaction.
    ///
    /// The job becomes visible to workers only when the transaction commits
    /// and is discarded if it rolls back. `executor` must be a SQLx
    /// transaction for the client's database backend.
    pub fn tx<'t, E>(self, executor: E) -> InsertRequest<'t, A>
    where
        'a: 't,
        E: DatabaseTransactionExecutor<'t>,
    {
        InsertRequest {
            args: self.args,
            client: self.client,
            opts: self.opts,
            target: Target::transaction(self.client, executor),
        }
    }
}

impl<A> fmt::Debug for InsertRequest<'_, A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InsertRequest")
            .field("opts", &self.opts)
            .finish_non_exhaustive()
    }
}

impl<'a, A: JobArgs> IntoFuture for InsertRequest<'a, A> {
    type Output = Result<InsertResult<A>, Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let Self {
                args,
                client,
                opts,
                target,
            } = self;
            let job = client.prepare_typed(&args, opts, Utc::now())?;
            let rows = client
                .run_insert(target.into_executor()?, vec![job])
                .await?;
            let row = rows.into_iter().next().ok_or_else(|| {
                Error::runtime_context("job insertion", "insertion returned no row")
            })?;
            let args = row.job.decode_args()?;
            Ok(InsertResult {
                job: Job { args, row: row.job },
                unique_skipped_as_duplicate: row.unique_skipped_as_duplicate,
            })
        })
    }
}

/// An atomic homogeneous batch insertion, returned by
/// [`Client::insert_many`]. Await it to insert the jobs and get one result
/// per job, in input order.
#[must_use = "insert requests do nothing unless awaited"]
pub struct InsertManyRequest<'a, A> {
    client: &'a Client,
    jobs: Vec<InsertManyItem<A>>,
    target: Target<'a>,
}

impl<'a, A: JobArgs> InsertManyRequest<'a, A> {
    /// Inserts the jobs in a caller-managed transaction.
    ///
    /// The jobs become visible to workers only when the transaction commits.
    /// If the batch fails, River rolls back to a savepoint so the transaction
    /// remains usable.
    pub fn tx<'t, E>(self, executor: E) -> InsertManyRequest<'t, A>
    where
        'a: 't,
        E: DatabaseTransactionExecutor<'t>,
    {
        InsertManyRequest {
            client: self.client,
            jobs: self.jobs,
            target: Target::transaction(self.client, executor),
        }
    }

    fn prepare(client: &Client, jobs: Vec<InsertManyItem<A>>) -> Result<Vec<InsertContext>, Error> {
        let now = Utc::now();
        jobs.into_iter()
            .map(|item| client.prepare_typed(&item.args, item.opts, now))
            .collect()
    }
}

impl<A> fmt::Debug for InsertManyRequest<'_, A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InsertManyRequest")
            .field("jobs", &self.jobs.len())
            .finish_non_exhaustive()
    }
}

impl<'a, A: JobArgs> IntoFuture for InsertManyRequest<'a, A> {
    type Output = Result<Vec<InsertResult<A>>, Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let Self {
                client,
                jobs,
                target,
            } = self;
            let jobs = Self::prepare(client, jobs)?;
            let rows = client.run_insert(target.into_executor()?, jobs).await?;
            rows.into_iter()
                .map(|row| {
                    let args = row.job.decode_args()?;
                    Ok(InsertResult {
                        job: Job { args, row: row.job },
                        unique_skipped_as_duplicate: row.unique_skipped_as_duplicate,
                    })
                })
                .collect()
        })
    }
}

/// An atomic heterogeneous batch insertion, returned by
/// [`Client::insert_batch`]. Await it to insert the jobs and get one result
/// per job, in input order.
#[must_use = "insert requests do nothing unless awaited"]
pub struct InsertBatchRequest<'a> {
    batch: InsertBatch,
    client: &'a Client,
    target: Target<'a>,
}

impl<'a> InsertBatchRequest<'a> {
    /// Inserts the batch in a caller-managed transaction.
    pub fn tx<'t, E>(self, executor: E) -> InsertBatchRequest<'t>
    where
        'a: 't,
        E: DatabaseTransactionExecutor<'t>,
    {
        InsertBatchRequest {
            batch: self.batch,
            client: self.client,
            target: Target::transaction(self.client, executor),
        }
    }
}

impl fmt::Debug for InsertBatchRequest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InsertBatchRequest")
            .field("jobs", &self.batch.len())
            .finish_non_exhaustive()
    }
}

impl<'a> IntoFuture for InsertBatchRequest<'a> {
    type Output = Result<Vec<InsertBatchResult>, Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let Self {
                batch,
                client,
                target,
            } = self;
            let now = Utc::now();
            let mut jobs = Vec::with_capacity(batch.len());
            for item in batch.items {
                client.validate_known_kind(item.kind)?;
                let opts = InsertOpts::resolve(
                    client.inner.default_max_attempts,
                    item.defaults,
                    item.opts,
                );
                jobs.push(client.prepare_encoded(
                    item.kind,
                    item.unique_fields,
                    item.encoded_args?,
                    opts,
                    now,
                )?);
            }
            let rows = client.run_insert(target.into_executor()?, jobs).await?;
            Ok(rows
                .into_iter()
                .map(|row| InsertBatchResult {
                    job: row.job,
                    unique_skipped_as_duplicate: row.unique_skipped_as_duplicate,
                })
                .collect())
        })
    }
}

impl Client {
    /// Inserts a job.
    ///
    /// Await the returned request to insert the job with its job type's
    /// defaults, or chain [`opts`](InsertRequest::opts) to override options
    /// and [`tx`](InsertRequest::tx) to insert in a caller-managed
    /// transaction:
    ///
    /// ```no_run
    /// # use riverqueue::{Client, InsertOpts, JobArgs};
    /// # use serde::{Deserialize, Serialize};
    /// #[derive(Deserialize, JobArgs, Serialize)]
    /// #[river(kind = "send_email")]
    /// struct SendEmail {
    ///     address: String,
    /// }
    ///
    /// # async fn example(client: Client, pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    /// let inserted = client
    ///     .insert(SendEmail { address: "user@example.com".to_owned() })
    ///     .await?;
    /// println!("inserted job {}", inserted.id());
    ///
    /// let mut tx = riverqueue::database::begin_postgres(&pool).await?;
    /// client
    ///     .insert(SendEmail { address: "admin@example.com".to_owned() })
    ///     .opts(InsertOpts::default().with_queue("email"))
    ///     .tx(&mut tx)
    ///     .await?;
    /// tx.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A unique job whose insertion matches an existing job returns that job
    /// with [`InsertResult::unique_skipped_as_duplicate`] set.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidJob`] for invalid options,
    /// [`Error::UnknownJobKind`] when the client has workers but none for
    /// this kind, [`Error::DatabaseMismatch`] for a transaction from another
    /// backend, [`Error::Extension`] when a hook or middleware fails, and
    /// [`Error::Database`] when the database operation fails.
    ///
    /// # Cancel safety
    ///
    /// Dropping the future before it completes rolls back River's own
    /// transaction. With [`tx`](InsertRequest::tx), the caller's transaction
    /// may contain a partial insertion and should be rolled back.
    pub fn insert<A: JobArgs>(&self, args: A) -> InsertRequest<'_, A> {
        InsertRequest {
            args,
            client: self,
            opts: InsertOpts::default(),
            target: Target::Client,
        }
    }

    /// Atomically inserts a batch of one or more jobs of one type.
    ///
    /// Items are job arguments or `(args, opts)` tuples. Await the request to
    /// get one [`InsertResult`] per job in input order.
    ///
    /// ```no_run
    /// # use riverqueue::{Client, InsertOpts, JobArgs};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Deserialize, JobArgs, Serialize)]
    /// # #[river(kind = "send_email")]
    /// # struct SendEmail { address: String }
    /// # async fn example(client: Client) -> Result<(), riverqueue::Error> {
    /// let results = client
    ///     .insert_many([
    ///         (SendEmail { address: "a@example.com".to_owned() }, InsertOpts::default()),
    ///         (SendEmail { address: "b@example.com".to_owned() }, InsertOpts::default().with_priority(2)),
    ///     ])
    ///     .await?;
    /// assert_eq!(results.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`insert`](Self::insert), and
    /// [`Error::InvalidJob`] for an empty batch. Any error rolls back the
    /// whole batch.
    ///
    /// # Cancel safety
    ///
    /// Same as [`insert`](Self::insert).
    pub fn insert_many<A, I>(&self, jobs: I) -> InsertManyRequest<'_, A>
    where
        A: JobArgs,
        I: IntoIterator,
        I::Item: Into<InsertManyItem<A>>,
    {
        InsertManyRequest {
            client: self,
            jobs: jobs.into_iter().map(Into::into).collect(),
            target: Target::Client,
        }
    }

    /// Atomically inserts a batch that can mix job types.
    ///
    /// Each item keeps its own job type's defaults and uniqueness. Results
    /// correspond positionally to the batch items.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`insert_many`](Self::insert_many), plus
    /// [`Error::Json`] if an item's arguments failed to serialize when it was
    /// added to the batch.
    ///
    /// # Cancel safety
    ///
    /// Same as [`insert`](Self::insert).
    pub fn insert_batch(&self, batch: InsertBatch) -> InsertBatchRequest<'_> {
        InsertBatchRequest {
            batch,
            client: self,
            target: Target::Client,
        }
    }

    /// Resolves typed insertion options for an extension.
    #[must_use]
    pub(crate) fn resolve_insert_opts<A: JobArgs>(&self, opts: InsertOpts) -> InsertParams {
        InsertOpts::resolve(
            self.inner.default_max_attempts,
            A::default_insert_opts(),
            opts,
        )
    }

    pub(crate) async fn insert_periodic(
        &self,
        insert: PeriodicInsert,
        opts: InsertParams,
        target: DateTime<Utc>,
    ) -> Result<JobRow, Error> {
        let job = self.prepare_periodic(
            insert.kind,
            insert.unique_fields,
            insert.encoded_args,
            opts,
            target,
            Utc::now(),
        )?;
        let rows = self.run_insert(None, vec![job]).await?;
        rows.into_iter().next().map(|row| row.job).ok_or_else(|| {
            Error::runtime_context("periodic job insertion", "insertion returned no row")
        })
    }

    /// Resolves, validates, and computes the uniqueness of a typed job.
    fn prepare_typed<A: JobArgs>(
        &self,
        args: &A,
        opts: InsertOpts,
        now: DateTime<Utc>,
    ) -> Result<InsertContext, Error> {
        self.validate_known_kind(A::KIND)?;
        let encoded_args = crate::encoding::encode_args(args)?;
        let opts = InsertOpts::resolve(
            self.inner.default_max_attempts,
            A::default_insert_opts(),
            opts,
        );
        self.prepare_encoded(A::KIND, A::unique_fields(), encoded_args, opts, now)
    }

    /// Prepares a periodic job due at `target`, as River Go's periodic job
    /// enqueuer does.
    ///
    /// When the constructor leaves the schedule unset, the job runs at its
    /// target time: it is inserted `available` with `scheduled_at` set to the
    /// target, and a `by_period` unique key describes the target's period.
    /// An explicit schedule from the constructor keeps the ordinary
    /// `scheduled` state, and a pending job stays pending.
    pub(super) fn prepare_periodic(
        &self,
        kind: &str,
        unique_fields: &[&[&str]],
        encoded_args: Box<RawValue>,
        mut opts: InsertParams,
        target: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<InsertContext, Error> {
        let due_at_target = opts.scheduled_at.is_none();
        opts.scheduled_at.get_or_insert(target);
        let mut job = self.prepare_encoded(kind, unique_fields, encoded_args, opts, now)?;
        if due_at_target && job.state == JobState::Scheduled {
            job.state = JobState::Available;
        }
        Ok(job)
    }

    /// Validates an encoded job and computes its unique key and initial
    /// state, as River Go does before insertion middleware and hooks run.
    pub(super) fn prepare_encoded(
        &self,
        kind: &str,
        unique_fields: &[&[&str]],
        encoded_args: Box<RawValue>,
        opts: InsertParams,
        now: DateTime<Utc>,
    ) -> Result<InsertContext, Error> {
        validate_insert_parts(kind, &opts, self.inner.allow_legacy_job_kinds)?;
        let unique_key = build_unique_key_parts(
            kind,
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
        Ok(InsertContext {
            encoded_args,
            kind: kind.to_owned(),
            opts,
            state,
            created_at: None,
            unique_key,
            unique_states,
        })
    }

    /// Runs the insertion pipeline and decodes the returned rows with decode
    /// hooks.
    pub(super) async fn run_insert(
        &self,
        executor: Option<PilotDatabaseConnection<'_>>,
        jobs: Vec<InsertContext>,
    ) -> Result<Vec<InsertedJob>, Error> {
        let InsertedJobs::Rows(mut rows) = self.run_insert_inserted(executor, jobs).await?;
        for row in &mut rows {
            for hook in self.inner.hooks.iter().rev() {
                hook.decode_insert_result(&mut row.job).await?;
            }
        }
        Ok(rows)
    }

    /// Runs the insertion pipeline, returning what middleware returned.
    ///
    /// Without a caller transaction, validation has already run, and
    /// middleware, begin hooks, extension interception, the write, and the
    /// insert notification all run in one transaction, like River Go's
    /// `Insert` and `InsertMany`: an error anywhere, including in middleware
    /// after the write, rolls the whole insertion back.
    async fn run_insert_inserted(
        &self,
        executor: Option<PilotDatabaseConnection<'_>>,
        jobs: Vec<InsertContext>,
    ) -> Result<InsertedJobs, Error> {
        if jobs.is_empty() {
            return Err(Error::invalid_job("no jobs to insert".to_owned()));
        }
        let atomic = jobs.len() > 1;
        let Some(executor) = executor else {
            let inserted = match self.inner.database.pool() {
                #[cfg(feature = "postgres")]
                DatabasePool::Postgres(pool) => {
                    let mut transaction = crate::database::begin_postgres(pool).await?;
                    let inserted = self
                        .insert_on_connection(
                            PilotDatabaseConnection::Postgres(&mut transaction),
                            jobs,
                        )
                        .await?;
                    transaction.commit().await?;
                    inserted
                }
                #[cfg(feature = "sqlite")]
                DatabasePool::Sqlite(pool) => {
                    let mut transaction = crate::database::begin_sqlite_write(pool).await?;
                    let inserted = self
                        .insert_on_connection(
                            PilotDatabaseConnection::Sqlite(&mut transaction),
                            jobs,
                        )
                        .await?;
                    transaction.commit().await?;
                    inserted
                }
            };
            self.signal_inserted(&inserted);
            return Ok(inserted);
        };
        match executor {
            #[cfg(feature = "postgres")]
            PilotDatabaseConnection::Postgres(connection) => {
                if !atomic {
                    return self
                        .insert_on_connection(PilotDatabaseConnection::Postgres(connection), jobs)
                        .await;
                }
                let savepoint = self.batch_savepoint();
                begin_postgres_savepoint(connection, &savepoint).await?;
                let result = self
                    .insert_on_connection(PilotDatabaseConnection::Postgres(&mut *connection), jobs)
                    .await;
                finish_postgres_savepoint(connection, &savepoint, result).await
            }
            #[cfg(feature = "sqlite")]
            PilotDatabaseConnection::Sqlite(connection) => {
                if !atomic {
                    return self
                        .insert_on_connection(PilotDatabaseConnection::Sqlite(connection), jobs)
                        .await;
                }
                let savepoint = self.batch_savepoint();
                begin_sqlite_savepoint(connection, &savepoint).await?;
                let result = self
                    .insert_on_connection(PilotDatabaseConnection::Sqlite(&mut *connection), jobs)
                    .await;
                finish_sqlite_savepoint(connection, &savepoint, result).await
            }
        }
    }

    fn batch_savepoint(&self) -> String {
        let sequence = self
            .inner
            .insert_savepoint_sequence
            .fetch_add(1, Ordering::Relaxed);
        format!("river_insert_{sequence}")
    }

    /// Wakes local producers for jobs this client committed itself.
    fn signal_inserted(&self, inserted: &InsertedJobs) {
        let InsertedJobs::Rows(rows) = inserted;
        for row in rows {
            self.signal_insert(&row.job, row.unique_skipped_as_duplicate);
        }
    }

    /// Runs insertion middleware around persistence of `jobs`.
    async fn insert_on_connection<'c>(
        &'c self,
        connection: PilotDatabaseConnection<'c>,
        jobs: Vec<InsertContext>,
    ) -> Result<InsertedJobs, Error> {
        let endpoint: InsertEndpoint<'c> =
            Box::new(move |jobs| Box::pin(self.persist_jobs(connection, jobs)));
        InsertNext::new(&self.inner.insert_middleware, endpoint)
            .run(jobs)
            .await
    }

    /// Runs begin hooks and extension interception for each job, writes the
    /// jobs, and notifies queues that gained available jobs.
    async fn persist_jobs(
        &self,
        mut connection: PilotDatabaseConnection<'_>,
        mut jobs: Vec<InsertContext>,
    ) -> Result<InsertedJobs, Error> {
        if jobs.is_empty() {
            return Err(Error::invalid_job("no jobs to insert".to_owned()));
        }
        let intercepts = self.inner.pilot.intercepts_insert();
        for job in &mut jobs {
            for hook in &self.inner.hooks {
                hook.insert_begin(job).await?;
            }
        }
        if intercepts {
            let mut params = extension_insert_params(&mut jobs);
            self.inner
                .pilot
                .before_jobs_insert(connection.reborrow(), &mut params)
                .await
                .map_err(|source| Error::Extension {
                    phase: "job insertion",
                    source,
                })?;
        }
        for job in &jobs {
            if !matches!(
                job.state,
                JobState::Available | JobState::Pending | JobState::Scheduled
            ) {
                return Err(Error::invalid_job(format!(
                    "jobs can't be inserted in the {} state",
                    job.state.as_str()
                )));
            }
        }

        let now = Utc::now();
        let mut rows = Vec::with_capacity(jobs.len());
        for job in jobs {
            rows.push(self.insert_row(connection.reborrow(), job, now).await?);
        }
        if intercepts {
            self.after_jobs_inserted(connection.reborrow(), &rows)
                .await?;
        }
        let queues = rows
            .iter()
            .filter(|row| row.job.state == JobState::Available && !row.unique_skipped_as_duplicate)
            .map(|row| row.job.queue.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        self.notify_insert(connection.reborrow(), queues).await?;
        Ok(InsertedJobs::Rows(rows))
    }

    /// Runs the extension's post-insert hook on the rows an insertion wrote.
    async fn after_jobs_inserted(
        &self,
        mut connection: PilotDatabaseConnection<'_>,
        rows: &[InsertedJob],
    ) -> Result<(), Error> {
        let inserted = rows
            .iter()
            .filter(|row| !row.unique_skipped_as_duplicate)
            .map(|row| row.job.clone())
            .collect::<Vec<_>>();
        if inserted.is_empty() {
            return Ok(());
        }
        self.inner
            .pilot
            .after_jobs_inserted(
                connection.reborrow(),
                &crate::__private::JobsInsertedParams {
                    database: self.inner.pilot_database_config(),
                    jobs: &inserted,
                },
            )
            .await
            .map_err(|source| Error::Extension {
                phase: "job insertion",
                source,
            })
    }

    /// Sends one insert notification per queue, in the insertion's
    /// transaction so it's delivered only if the jobs commit.
    async fn notify_insert(
        &self,
        connection: PilotDatabaseConnection<'_>,
        queues: std::collections::BTreeSet<&str>,
    ) -> Result<(), Error> {
        if queues.is_empty() {
            return Ok(());
        }
        match connection {
            #[cfg(feature = "postgres")]
            PilotDatabaseConnection::Postgres(connection) => {
                let queues = queues.into_iter().collect::<Vec<_>>();
                sqlx::query(
                    "SELECT pg_notify(concat(coalesce($1::text, current_schema()), '.', $2::text), json_build_object('queue', queue)::text) \
                     FROM unnest($3::text[]) AS queue",
                )
                .bind(self.inner.schema.as_deref())
                .bind(crate::protocol::NOTIFICATION_TOPIC_INSERT)
                .bind(queues)
                .execute(connection)
                .await?;
            }
            #[cfg(feature = "sqlite")]
            PilotDatabaseConnection::Sqlite(connection) => {
                let payloads = queues
                    .into_iter()
                    .map(|queue| serde_json::json!({ "queue": queue }).to_string())
                    .collect::<Vec<_>>();
                let notifications = payloads
                    .iter()
                    .map(|payload| crate::database::sqlite::NotificationInput {
                        payload,
                        topic: crate::protocol::NOTIFICATION_TOPIC_INSERT,
                    })
                    .collect::<Vec<_>>();
                crate::database::sqlite::notification_insert(connection, &notifications)
                    .await
                    .map_err(sqlite_backend_error)?;
            }
        }
        Ok(())
    }

    /// Writes one job, returning it or the existing unique job it matched.
    async fn insert_row(
        &self,
        connection: PilotDatabaseConnection<'_>,
        job: InsertContext,
        now: DateTime<Utc>,
    ) -> Result<InsertedJob, Error> {
        let InsertContext {
            encoded_args,
            kind,
            opts,
            state,
            created_at,
            unique_key,
            unique_states,
        } = job;
        let _ = now;
        match connection {
            #[cfg(feature = "postgres")]
            PilotDatabaseConnection::Postgres(connection) => {
                let table = self.inner.schema.qualify("river_job");
                let state_type = self.inner.schema.qualify("river_job_state");
                let state_function = self.inner.schema.qualify("river_job_state_in_bitmask");
                // The no-op update is intentional and matches River Go. `DO
                // NOTHING` followed by a select cannot see a conflicting row
                // that committed after the statement's snapshot was taken.
                let sql = format!(
                    "WITH inserted AS (\
                        INSERT INTO {table} (args, created_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states) \
                        VALUES ($1, coalesce($2, now()), $3, $4, $5, $6, $7, coalesce($8, now()), $9::text::{state_type}, $10, $11, $12::integer::bit(8)) \
                        ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND unique_states IS NOT NULL AND {state_function}(unique_states, state) \
                        DO UPDATE SET kind = EXCLUDED.kind \
                        RETURNING *, (xmax != 0) AS unique_skipped_as_duplicate\
                     ) \
                     SELECT {}, job.unique_skipped_as_duplicate FROM inserted AS job",
                    job_projection("job")
                );
                let record = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
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
                    .bind(unique_key)
                    .bind(unique_states.map(i32::from))
                    .fetch_optional(connection)
                    .await?
                    .ok_or_else(|| {
                        Error::invalid_job("unique insert found no conflicting row".to_owned())
                    })?;
                let duplicate = record.unique_skipped_as_duplicate;
                Ok(InsertedJob::new(record.into_job_row()?, duplicate))
            }
            #[cfg(feature = "sqlite")]
            PilotDatabaseConnection::Sqlite(connection) => {
                // Like Go's SQLite driver, every inserted row carries a nonce.
                let nonce = Self::unique_insert_nonce();
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
                        unique_nonce: Some(&nonce),
                        unique_states,
                    },
                )
                .await
                .map_err(sqlite_backend_error)?;
                Ok(InsertedJob::new(
                    inserted.job,
                    inserted.unique_skipped_as_duplicate,
                ))
            }
        }
    }

    /// Returns a nonce that marks a SQLite insert as this call's own.
    ///
    /// SQLite reports a skipped unique duplicate by checking whether the
    /// returned row carries the nonce the insert wrote. The nonce must not
    /// repeat across processes: client IDs and counters can (a restarted
    /// container keeps its hostname and PID), so it's eight random bytes in
    /// lowercase hex, the format of River Go's `randutil.Hex(8)`.
    ///
    /// Unlike Go, which draws one nonce per insertion call and writes it to
    /// every row of the batch, River draws one per row. Two rows of one batch
    /// with the same unique key are therefore reported as a duplicate here,
    /// while Go reports neither as skipped.
    #[cfg(feature = "sqlite")]
    fn unique_insert_nonce() -> String {
        format!("{:016x}", rand::random::<u64>())
    }
}

/// Exposes the mutable fields of each job to an extension's insert hook.
fn extension_insert_params(jobs: &mut [InsertContext]) -> Vec<PilotJobInsertParams<'_>> {
    jobs.iter_mut()
        .map(|job| {
            let InsertContext {
                encoded_args,
                kind,
                opts,
                state,
                ..
            } = job;
            PilotJobInsertParams {
                encoded_args,
                kind,
                metadata: &mut opts.metadata,
                queue: &mut opts.queue,
                state,
            }
        })
        .collect()
}
