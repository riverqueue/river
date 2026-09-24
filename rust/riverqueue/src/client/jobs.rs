//! Operations on persisted jobs.

use std::{
    fmt,
    future::{Future, IntoFuture},
    pin::Pin,
};

#[allow(clippy::wildcard_imports)]
use super::*;
use crate::storage::Access;
use crate::{JobDeleteManyParams, JobListCursor, JobListParams, JobListResult, JobUpdateParams};

/// Operations on persisted jobs, returned by [`Client::jobs`].
///
/// Each method returns a request that runs on the client's own pool when
/// awaited, or in a caller-managed transaction after `.tx(&mut tx)`:
///
/// ```no_run
/// # async fn example(client: riverqueue::Client, pool: sqlx::PgPool) -> Result<(), riverqueue::Error> {
/// let job = client.jobs().get(42).await?;
///
/// let mut tx = pool.begin().await?;
/// client.jobs().cancel(job.id).tx(&mut tx).await?;
/// tx.commit().await?;
/// # Ok(())
/// # }
/// ```
///
/// Requests don't run until awaited. Dropping one before it completes rolls
/// back River's own transaction; with `.tx`, the caller's transaction may
/// contain the operation's partial effects and should be rolled back.
#[derive(Clone, Copy, Debug)]
pub struct Jobs<'a> {
    client: &'a Client,
}

impl Client {
    /// Returns operations on persisted jobs: getting, listing, cancelling,
    /// retrying, updating, and deleting them.
    #[must_use]
    pub const fn jobs(&self) -> Jobs<'_> {
        Jobs { client: self }
    }
}

impl<'a> Jobs<'a> {
    /// Cancels a job and returns its current row.
    ///
    /// A job that is available, scheduled, retryable, or pending is cancelled
    /// immediately and won't run again. A running job is marked for
    /// cancellation, and the client running it cancels the attempt's
    /// [`WorkContext::cancellation_token`]: if the worker then returns an
    /// error, the job is cancelled rather than retried, while a job that
    /// completes successfully stays completed. A finalized job is returned
    /// unchanged.
    ///
    /// With [`tx`](JobCancelRequest::tx), the cancellation and its
    /// notification take effect only when the transaction commits.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when the job doesn't exist,
    /// [`Error::DatabaseMismatch`] for a transaction from another backend,
    /// [`Error::Extension`] when an extension's cancellation hook fails, and
    /// [`Error::Database`] when the database operation fails.
    pub fn cancel(&self, id: i64) -> JobCancelRequest<'a> {
        JobCancelRequest {
            client: self.client,
            id,
            target: Target::Client,
        }
    }

    /// Completes a running job in a caller-managed transaction, for example
    /// alongside business writes that the job performed.
    ///
    /// The returned request has no effect until it's given the transaction
    /// with [`tx`](JobCompleteRequest::tx) and awaited. The job becomes
    /// completed only when the transaction commits. If this completes a job
    /// that is still being worked, the worker's own result is discarded when
    /// it finishes, because the job is no longer running. Workers can use
    /// [`WorkContext::job_complete_tx`], which also records metadata set on
    /// the work context.
    ///
    /// ```no_run
    /// # async fn example(client: riverqueue::Client, pool: sqlx::PgPool) -> Result<(), riverqueue::Error> {
    /// let mut tx = pool.begin().await?;
    /// // ... business writes in `tx` ...
    /// client.jobs().complete(42).tx(&mut tx).await?;
    /// tx.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn complete(&self, id: i64) -> JobCompleteRequest<'a> {
        JobCompleteRequest {
            client: self.client,
            id,
        }
    }

    /// Deletes a job that isn't running and returns its former row.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when the job doesn't exist,
    /// [`Error::JobRunning`] when it's running, [`Error::DatabaseMismatch`]
    /// for a transaction from another backend, and [`Error::Database`] when
    /// the database operation fails.
    pub fn delete(&self, id: i64) -> JobDeleteRequest<'a> {
        JobDeleteRequest {
            client: self.client,
            id,
            target: Target::Client,
        }
    }

    /// Deletes jobs that aren't running and match the parameters, returning
    /// the deleted rows in list order.
    ///
    /// At most the filter's limit of jobs are deleted. Running jobs are
    /// skipped before the limit applies, and PostgreSQL also skips jobs
    /// locked by another transaction rather than waiting for them.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidJob`] for invalid list parameters,
    /// [`Error::DatabaseMismatch`] for a transaction from another backend, and
    /// [`Error::Database`] when the database operation fails.
    pub fn delete_many(&self, params: JobDeleteManyParams) -> JobDeleteManyRequest<'a> {
        JobDeleteManyRequest {
            client: self.client,
            params,
            target: Target::Client,
        }
    }

    /// Gets a job by ID.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when the job doesn't exist,
    /// [`Error::DatabaseMismatch`] for a transaction from another backend, and
    /// [`Error::Database`] when the database operation fails.
    pub fn get(&self, id: i64) -> JobGetRequest<'a> {
        JobGetRequest {
            client: self.client,
            id,
            target: Target::Client,
        }
    }

    /// Lists jobs matching the parameters, one page at a time.
    ///
    /// Pass the result's [`last_cursor`](JobListResult::last_cursor) to
    /// [`JobListParams::with_after`] with otherwise identical parameters to
    /// request the next page:
    ///
    /// ```no_run
    /// # use riverqueue::{JobListParams, JobState};
    /// # async fn example(client: riverqueue::Client) -> Result<(), riverqueue::Error> {
    /// let params = JobListParams::default().with_limit(100);
    /// let mut page = client.jobs().list(params.clone()).await?;
    /// while let Some(cursor) = page.last_cursor.take() {
    ///     for job in &page.jobs {
    ///         println!("{} {:?}", job.id, job.state);
    ///     }
    ///     page = client.jobs().list(params.clone().with_after(cursor)).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidJob`] for invalid parameters, such as a limit
    /// outside one through 10,000 or a cursor from a different ordering,
    /// [`Error::DatabaseMismatch`] for a transaction from another backend, and
    /// [`Error::Database`] when the database operation fails.
    pub fn list(&self, params: JobListParams) -> JobListRequest<'a> {
        JobListRequest {
            client: self.client,
            params,
            target: Target::Client,
        }
    }

    /// Makes a job that isn't running available to be worked again and
    /// returns its current row.
    ///
    /// The job's `scheduled_at` moves to now unless it's already available
    /// and due, so a waiting job doesn't lose its place, and a job that has
    /// used all of its attempts gets one more. A running job is returned
    /// unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when the job doesn't exist,
    /// [`Error::DatabaseMismatch`] for a transaction from another backend,
    /// [`Error::Extension`] when an extension's retry hook fails, and
    /// [`Error::Database`] when the database operation fails.
    pub fn retry(&self, id: i64) -> JobRetryRequest<'a> {
        JobRetryRequest {
            client: self.client,
            id,
            target: Target::Client,
        }
    }

    /// Merges metadata into a job, optionally setting its recorded output,
    /// and returns the updated row.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when the job doesn't exist,
    /// [`Error::DatabaseMismatch`] for a transaction from another backend, and
    /// [`Error::Database`] when the database operation fails.
    pub fn update(&self, id: i64, params: JobUpdateParams) -> JobUpdateRequest<'a> {
        JobUpdateRequest {
            client: self.client,
            id,
            params,
            target: Target::Client,
        }
    }
}

request_type! {
    /// A job cancellation, returned by [`Jobs::cancel`]. Await it to cancel
    /// the job and get its current row.
    JobCancelRequest { id: i64 } -> JobRow
}

impl JobCancelRequest<'_> {
    async fn run(self) -> Result<JobRow, Error> {
        let inner = &self.client.inner;
        let own_transaction = !self.target.is_transaction();
        let mut session = self.target.session(inner, Access::Transaction).await?;
        let row = session.storage(inner).job_cancel(self.id).await?;
        session.commit().await?;
        // Without a listener, wake this client's running attempt directly,
        // like Go's `notifyProducerWithoutListenerQueueControlEvent`. Other
        // clients observe the committed notification when they poll.
        if own_transaction && !inner.database.supports_listener() {
            signal_running_attempt(
                &inner.running,
                &inner.pending_cancellations,
                &inner.fetch_registration_windows,
                self.id,
            );
        }
        Ok(row)
    }
}

request_type! {
    /// A job deletion, returned by [`Jobs::delete`]. Await it to delete the
    /// job and get its former row.
    JobDeleteRequest { id: i64 } -> JobRow
}

impl JobDeleteRequest<'_> {
    async fn run(self) -> Result<JobRow, Error> {
        let inner = &self.client.inner;
        let mut session = self.target.session(inner, Access::Transaction).await?;
        let row = session.storage(inner).job_delete(self.id).await?;
        session.commit().await?;
        Ok(row)
    }
}

request_type! {
    /// A bulk job deletion, returned by [`Jobs::delete_many`]. Await it to
    /// delete the jobs and get their former rows.
    JobDeleteManyRequest { params: JobDeleteManyParams } -> Vec<JobRow>
}

impl JobDeleteManyRequest<'_> {
    async fn run(self) -> Result<Vec<JobRow>, Error> {
        let inner = &self.client.inner;
        let mut session = self.target.session(inner, Access::Transaction).await?;
        let rows = session.storage(inner).job_delete_many(&self.params).await?;
        session.commit().await?;
        Ok(rows)
    }
}

request_type! {
    /// A job lookup, returned by [`Jobs::get`]. Await it to get the job.
    JobGetRequest { id: i64 } -> JobRow
}

impl JobGetRequest<'_> {
    async fn run(self) -> Result<JobRow, Error> {
        let inner = &self.client.inner;
        let mut session = self.target.session(inner, Access::Autocommit).await?;
        session.storage(inner).job_get(self.id).await
    }
}

request_type! {
    /// A job listing, returned by [`Jobs::list`]. Await it to get a page of
    /// jobs.
    JobListRequest { params: JobListParams } -> JobListResult
}

impl JobListRequest<'_> {
    async fn run(self) -> Result<JobListResult, Error> {
        let inner = &self.client.inner;
        let mut session = self.target.session(inner, Access::Autocommit).await?;
        let jobs = session.storage(inner).job_list(&self.params).await?;
        let last_cursor = jobs
            .last()
            .map(|job| JobListCursor::after_job(job, &self.params));
        Ok(JobListResult { jobs, last_cursor })
    }
}

request_type! {
    /// A job retry, returned by [`Jobs::retry`]. Await it to make the job
    /// available and get its current row.
    JobRetryRequest { id: i64 } -> JobRow
}

impl JobRetryRequest<'_> {
    async fn run(self) -> Result<JobRow, Error> {
        let inner = &self.client.inner;
        let mut session = self.target.session(inner, Access::Transaction).await?;
        let row = session.storage(inner).job_retry(self.id).await?;
        session.commit().await?;
        Ok(row)
    }
}

request_type! {
    /// A job update, returned by [`Jobs::update`]. Await it to update the job
    /// and get its new row.
    JobUpdateRequest { id: i64, params: JobUpdateParams } -> JobRow
}

impl JobUpdateRequest<'_> {
    async fn run(self) -> Result<JobRow, Error> {
        let inner = &self.client.inner;
        let mut session = self.target.session(inner, Access::Autocommit).await?;
        session
            .storage(inner)
            .job_update(self.id, self.params)
            .await
    }
}

/// A transactional job completion, returned by [`Jobs::complete`].
///
/// It has no effect on its own: pass the transaction to complete the job in
/// with [`tx`](Self::tx) and await the result.
#[must_use = "a completion needs `.tx(&mut tx)` and must be awaited"]
#[derive(Debug)]
pub struct JobCompleteRequest<'a> {
    client: &'a Client,
    id: i64,
}

impl<'a> JobCompleteRequest<'a> {
    /// Completes the job in a caller-managed transaction. The job becomes
    /// completed only when the transaction commits.
    ///
    /// `executor` must be a SQLx transaction for the client's database
    /// backend; begin SQLite transactions with `BEGIN IMMEDIATE`.
    pub fn tx<'t, E>(self, executor: E) -> JobCompleteTxRequest<'t>
    where
        'a: 't,
        E: DatabaseTransactionExecutor<'t>,
    {
        JobCompleteTxRequest {
            client: self.client,
            connection: self.client.inner.transaction_connection(executor),
            id: self.id,
        }
    }
}

/// A job completion in a caller-managed transaction, returned by
/// [`JobCompleteRequest::tx`]. Await it to complete the job and get its new
/// row.
///
/// # Errors
///
/// Awaiting it returns [`Error::NotFound`] when the job doesn't exist,
/// [`Error::InvalidJob`] when the job isn't running,
/// [`Error::DatabaseMismatch`] for a transaction from another backend,
/// [`Error::Extension`] when an extension's completion hook fails, and
/// [`Error::Database`] when the database operation fails.
#[must_use = "requests do nothing unless awaited"]
pub struct JobCompleteTxRequest<'a> {
    client: &'a Client,
    connection: Result<PilotDatabaseConnection<'a>, Error>,
    id: i64,
}

impl fmt::Debug for JobCompleteTxRequest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobCompleteTxRequest")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<'a> IntoFuture for JobCompleteTxRequest<'a> {
    type Output = Result<JobRow, Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let connection = self.connection?;
            crate::storage::Storage::new(&self.client.inner, connection)
                .job_complete(self.id, &Map::new())
                .await
        })
    }
}
