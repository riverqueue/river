//! Unstable extension entry points used by companion crates.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Map;
use serde_json::value::RawValue;

use crate::__private::DatabaseConnection as PilotDatabaseConnection;
use crate::__private::{PreparedInsertParams, RawInsertResult};
use crate::client::WeakClient;
use crate::client::request::{Target, request_type};
use crate::client::validate::validate_insert_parts;
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

impl<'client> ExtensionClient<'client> {
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

    /// Inserts an encoded job through River's exact-version extension seam,
    /// with `opts` resolved against the client's defaults the same way a
    /// typed insert resolves them.
    ///
    /// The request runs when awaited, in a caller-managed transaction with
    /// [`tx`](RawInsertRequest::tx). Awaiting it returns the errors of an
    /// ordinary insertion: invalid options, an unregistered kind, an
    /// extension failure, a transaction from another backend, or a database
    /// error.
    pub fn insert_raw<'a>(
        &self,
        kind: &'a str,
        unique_fields: &'a [&'a [&'a str]],
        encoded_args: Box<RawValue>,
        opts: InsertOpts,
    ) -> RawInsertRequest<'a>
    where
        'client: 'a,
    {
        self.raw_request(
            kind,
            unique_fields,
            encoded_args,
            RawInsertOptions::Opts(opts),
        )
    }

    /// Like [`insert_raw`](Self::insert_raw), with already-resolved
    /// insertion parameters.
    pub fn insert_raw_params<'a>(
        &self,
        kind: &'a str,
        unique_fields: &'a [&'a [&'a str]],
        encoded_args: Box<RawValue>,
        params: InsertParams,
    ) -> RawInsertRequest<'a>
    where
        'client: 'a,
    {
        self.raw_request(
            kind,
            unique_fields,
            encoded_args,
            RawInsertOptions::Params(params),
        )
    }

    /// Like [`insert_raw_params`](Self::insert_raw_params), inserting the job
    /// as the occurrence of a periodic job due at `target`, exactly as
    /// River's periodic job enqueuer does.
    ///
    /// When `params.scheduled_at` is unset, the job is inserted `available`
    /// with `scheduled_at` set to `target` so it runs immediately, and a
    /// `by_period` unique key uses the target's period. An explicit
    /// `scheduled_at` inserts a `scheduled` job, and `pending` is kept.
    pub fn insert_periodic<'a>(
        &self,
        kind: &'a str,
        unique_fields: &'a [&'a [&'a str]],
        encoded_args: Box<RawValue>,
        params: InsertParams,
        target: DateTime<Utc>,
    ) -> RawInsertRequest<'a>
    where
        'client: 'a,
    {
        self.raw_request(
            kind,
            unique_fields,
            encoded_args,
            RawInsertOptions::Periodic { params, target },
        )
    }

    fn raw_request<'a>(
        self,
        kind: &'a str,
        unique_fields: &'a [&'a [&'a str]],
        encoded_args: Box<RawValue>,
        options: RawInsertOptions,
    ) -> RawInsertRequest<'a>
    where
        'client: 'a,
    {
        RawInsertRequest {
            client: self.client,
            encoded_args,
            kind,
            options,
            target: Target::Client,
            unique_fields,
        }
    }

    /// Inserts stored jobs again, such as jobs set aside and retried later,
    /// the way an ordinary batch insertion runs: insert middleware, begin
    /// hooks, the extension's insertion step, and notifications run once, in
    /// one transaction. See [`PreparedInsertParams`] for what the jobs keep.
    ///
    /// The request runs when awaited, in a caller-managed transaction with
    /// [`tx`](PreparedInsertRequest::tx). Awaiting it returns the errors of
    /// an ordinary insertion: invalid parameters, an extension failure, a
    /// transaction from another backend, or a database error.
    pub fn insert_prepared<'a>(
        &self,
        params: Vec<PreparedInsertParams>,
    ) -> PreparedInsertRequest<'a>
    where
        'client: 'a,
    {
        PreparedInsertRequest {
            client: self.client,
            params,
            target: Target::Client,
        }
    }

    /// Validates stored jobs and turns them into insertions that keep their
    /// unique key and states, creation time, and schedule.
    pub(crate) fn prepared_jobs(
        params: Vec<PreparedInsertParams>,
    ) -> Result<Vec<InsertContext>, Error> {
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
}

async fn insert_raw_jobs(
    client: &Client,
    executor: Option<PilotDatabaseConnection<'_>>,
    jobs: Vec<InsertContext>,
) -> Result<Vec<RawInsertResult>, Error> {
    client
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
    client: &Client,
    executor: Option<PilotDatabaseConnection<'_>>,
    job: InsertContext,
) -> Result<RawInsertResult, Error> {
    client
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

/// How a [`RawInsertRequest`] sets the job's options.
#[derive(Debug)]
enum RawInsertOptions {
    /// Options resolved against the client's defaults, like a typed insert.
    Opts(InsertOpts),
    /// Already-resolved parameters.
    Params(InsertParams),
    /// A periodic occurrence due at `target`.
    Periodic {
        params: InsertParams,
        target: DateTime<Utc>,
    },
}

request_type! {
    /// An encoded insertion through River's exact-version extension seam,
    /// returned by [`ExtensionClient::insert_raw`],
    /// [`ExtensionClient::insert_raw_params`], or
    /// [`ExtensionClient::insert_periodic`]. Await it to insert.
    write RawInsertRequest {
        encoded_args: Box<RawValue>,
        kind: &'a str,
        options: RawInsertOptions,
        unique_fields: &'a [&'a [&'a str]],
    } -> RawInsertResult
}

impl RawInsertRequest<'_> {
    async fn run(self) -> Result<RawInsertResult, Error> {
        let executor = self.target.into_executor()?;
        let client = self.client;
        client.validate_known_kind(self.kind)?;
        let now = Utc::now();
        let job = match self.options {
            RawInsertOptions::Opts(opts) => {
                let params = InsertOpts::resolve(
                    client.inner.default_max_attempts,
                    InsertOpts::default(),
                    opts,
                );
                client.prepare_encoded(
                    self.kind,
                    self.unique_fields,
                    self.encoded_args,
                    params,
                    now,
                )?
            }
            RawInsertOptions::Params(params) => client.prepare_encoded(
                self.kind,
                self.unique_fields,
                self.encoded_args,
                params,
                now,
            )?,
            RawInsertOptions::Periodic { params, target } => client.prepare_periodic(
                self.kind,
                self.unique_fields,
                self.encoded_args,
                params,
                target,
                now,
            )?,
        };
        insert_raw_job(client, executor, job).await
    }
}

request_type! {
    /// A reinsertion of stored jobs, returned by
    /// [`ExtensionClient::insert_prepared`]. Await it to insert.
    write PreparedInsertRequest {
        params: Vec<PreparedInsertParams>,
    } -> Vec<RawInsertResult>
}

impl PreparedInsertRequest<'_> {
    async fn run(self) -> Result<Vec<RawInsertResult>, Error> {
        let executor = self.target.into_executor()?;
        if self.params.is_empty() {
            return Ok(Vec::new());
        }
        let jobs = ExtensionClient::prepared_jobs(self.params)?;
        insert_raw_jobs(self.client, executor, jobs).await
    }
}
