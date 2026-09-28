//! Unstable extension entry points used by companion crates.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Map;
use serde_json::value::RawValue;

use crate::__private::DatabaseConnection as PilotDatabaseConnection;
use crate::__private::{PreparedInsertParams, RawInsertResult};
use crate::client::WeakClient;
use crate::client::validate::validate_insert_parts;
use crate::database::DatabaseTransactionExecutor;
use crate::{
    Client, Error, InsertContext, InsertOpts, InsertParams, JobArgs, JobRow, JobState, WorkError,
};

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
    /// Computes the configured retry delay for an exact-version extension.
    #[must_use]
    pub fn retry_delay(&self, row: &JobRow, error: &WorkError, now: DateTime<Utc>) -> Duration {
        self.client.inner.retry_policy.next_retry(row, error, now)
    }

    /// Returns the scheduler horizon used by exact-version completion helpers.
    #[must_use]
    pub fn scheduler_interval(&self) -> Duration {
        self.client.inner.maintenance.scheduler_interval
    }

    /// Inserts an encoded job through River's exact-version extension seam.
    ///
    /// # Errors
    ///
    /// Returns the errors of an ordinary insertion: invalid options, an
    /// extension failure, or a database error.
    pub async fn insert_raw(
        &self,
        kind: &str,
        unique_fields: &[&[&str]],
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
    ///
    /// # Errors
    ///
    /// Returns the errors of an ordinary insertion: invalid options, an
    /// extension failure, or a database error.
    pub async fn insert_raw_params(
        &self,
        kind: &str,
        unique_fields: &[&[&str]],
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
    ///
    /// # Errors
    ///
    /// Returns the errors of an ordinary insertion: invalid options, an
    /// extension failure, or a database error.
    pub async fn insert_raw_tx<'executor, E>(
        &self,
        connection: E,
        kind: &str,
        unique_fields: &[&[&str]],
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
    ///
    /// # Errors
    ///
    /// Returns the errors of an ordinary insertion: invalid options, an
    /// extension failure, or a database error.
    pub async fn insert_raw_params_tx<'executor, E>(
        &self,
        connection: E,
        kind: &str,
        unique_fields: &[&[&str]],
        encoded_args: Box<RawValue>,
        opts: InsertParams,
    ) -> Result<RawInsertResult, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        let executor = self.client.inner.transaction_connection(connection)?;
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
        unique_fields: &[&[&str]],
        encoded_args: Box<RawValue>,
        opts: InsertParams,
        target: DateTime<Utc>,
    ) -> Result<RawInsertResult, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        let executor = self.client.inner.transaction_connection(transaction)?;
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

    /// Inserts stored jobs again, such as jobs set aside and retried later,
    /// like River Go's ordinary `insertMany`: insert middleware, begin hooks,
    /// the extension's insertion step, and notifications run once, in one
    /// transaction. See [`PreparedInsertParams`] for what the jobs keep.
    ///
    /// # Errors
    ///
    /// Returns the errors of an ordinary insertion: invalid parameters, an
    /// extension failure, or a database error.
    pub async fn insert_prepared(
        &self,
        params: Vec<PreparedInsertParams>,
    ) -> Result<Vec<RawInsertResult>, Error> {
        if params.is_empty() {
            return Ok(Vec::new());
        }
        let jobs = Self::prepared_jobs(params)?;
        self.insert_raw_jobs(None, jobs).await
    }

    /// Inserts stored jobs again inside a caller-managed transaction, like
    /// [`insert_prepared`](Self::insert_prepared).
    ///
    /// # Errors
    ///
    /// Returns the errors of an ordinary insertion: invalid parameters, an
    /// extension failure, a transaction from another backend, or a database
    /// error.
    pub async fn insert_prepared_tx<'executor, E>(
        &self,
        transaction: E,
        params: Vec<PreparedInsertParams>,
    ) -> Result<Vec<RawInsertResult>, Error>
    where
        E: DatabaseTransactionExecutor<'executor>,
    {
        let executor = self.client.inner.transaction_connection(transaction)?;
        if params.is_empty() {
            return Ok(Vec::new());
        }
        let jobs = Self::prepared_jobs(params)?;
        self.insert_raw_jobs(Some(executor), jobs).await
    }

    /// Validates stored jobs and turns them into insertions that keep their
    /// unique key and states, creation time, and schedule.
    fn prepared_jobs(params: Vec<PreparedInsertParams>) -> Result<Vec<InsertContext>, Error> {
        params
            .into_iter()
            .map(|params| {
                let unique_states = match (&params.unique_key, &params.unique_states) {
                    (None, None) => None,
                    (Some(_), Some(states)) => Some(
                        states
                            .iter()
                            .fold(0, |bitmask, state| bitmask | state.unique_bit()),
                    ),
                    _ => {
                        return Err(Error::invalid_job_context(
                            "prepared insertion",
                            "unique_key and unique_states must either both be set or both be absent"
                                .to_owned(),
                        ));
                    }
                };
                let opts = InsertParams {
                    extension_options: Map::new(),
                    max_attempts: params.max_attempts,
                    metadata: params.metadata,
                    pending: false,
                    priority: params.priority,
                    queue: params.queue,
                    scheduled_at: Some(params.scheduled_at),
                    tags: params.tags,
                    unique: crate::UniqueOpts::default(),
                };
                // A stored job's kind was accepted when it was first
                // inserted, possibly by an older client, so only its options
                // are checked again.
                validate_insert_parts(&params.kind, &opts, true)?;
                Ok(InsertContext {
                    encoded_args: params.encoded_args,
                    kind: params.kind,
                    opts,
                    state: JobState::Available,
                    created_at: Some(params.created_at),
                    unique_key: params.unique_key,
                    unique_states,
                })
            })
            .collect()
    }

    async fn insert_raw_jobs(
        &self,
        executor: Option<PilotDatabaseConnection<'_>>,
        jobs: Vec<InsertContext>,
    ) -> Result<Vec<RawInsertResult>, Error> {
        self.client
            .run_insert(executor, jobs, |rows| {
                Ok(rows
                    .into_iter()
                    .map(|row| RawInsertResult {
                        job: row.job,
                        unique_skipped_as_duplicate: row.unique_skipped_as_duplicate,
                    })
                    .collect())
            })
            .await
    }

    async fn insert_raw_job(
        &self,
        executor: Option<PilotDatabaseConnection<'_>>,
        job: InsertContext,
    ) -> Result<RawInsertResult, Error> {
        self.client
            .run_insert(executor, vec![job], |rows| {
                let row = rows.into_iter().next().ok_or_else(|| {
                    Error::runtime_context("exact-version insertion", "insertion returned no row")
                })?;
                Ok(RawInsertResult {
                    job: row.job,
                    unique_skipped_as_duplicate: row.unique_skipped_as_duplicate,
                })
            })
            .await
    }
}
