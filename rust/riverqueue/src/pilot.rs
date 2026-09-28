//! Producer sessions and database bindings for River's own companion crates.
//!
//! Everything here is re-exported from [`crate::__private`] and shares its
//! stability rules: it changes without notice between any two versions.

use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
#[cfg(feature = "postgres")]
use sqlx::Postgres;
#[cfg(feature = "sqlite")]
use sqlx::Sqlite;
use sqlx::Transaction;
use tokio_util::sync::CancellationToken;

use crate::__private::{ClaimedJob, DatabaseConfig, DatabaseConnection, DatabasePool, PilotError};
use crate::client::ClientInner;
use crate::database::DatabaseKind;
use crate::{Error, JobRow, Queue};

/// The client's database as seen by an extension: its caller-owned pool and
/// backend configuration.
///
/// Cloning is cheap. An extension opens its own transactions with
/// [`PilotDatabase::begin`], which, like River's own, can be dropped at any
/// point without leaking an open transaction.
#[derive(Clone)]
pub struct PilotDatabase {
    config: DatabaseConfig,
    pool: DatabasePool,
}

impl PilotDatabase {
    pub(crate) const fn new(pool: DatabasePool, config: DatabaseConfig) -> Self {
        Self { config, pool }
    }

    /// Begins a transaction that may write. SQLite transactions take the
    /// write lock up front with `BEGIN IMMEDIATE`.
    ///
    /// # Errors
    ///
    /// Returns the database error when the transaction can't begin.
    pub async fn begin(&self) -> Result<PilotTransaction, Error> {
        let transaction = match &self.pool {
            #[cfg(feature = "postgres")]
            DatabasePool::Postgres(pool) => {
                PilotTransactionInner::Postgres(crate::database::begin_postgres(pool).await?)
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                PilotTransactionInner::Sqlite(crate::database::begin_sqlite_write(pool).await?)
            }
        };
        Ok(PilotTransaction(transaction))
    }

    /// Returns the backend configuration.
    #[must_use]
    pub const fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Returns the selected backend.
    #[must_use]
    pub const fn kind(&self) -> DatabaseKind {
        self.pool.kind()
    }

    /// Returns the caller-owned pool.
    #[must_use]
    pub const fn pool(&self) -> &DatabasePool {
        &self.pool
    }
}

impl fmt::Debug for PilotDatabase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PilotDatabase")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// A transaction an extension opened with [`PilotDatabase::begin`]. Dropping
/// it without committing rolls it back.
pub struct PilotTransaction(PilotTransactionInner);

enum PilotTransactionInner {
    #[cfg(feature = "postgres")]
    Postgres(Transaction<'static, Postgres>),
    #[cfg(feature = "sqlite")]
    Sqlite(Transaction<'static, Sqlite>),
}

impl PilotTransaction {
    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// Returns the database error when the commit fails.
    pub async fn commit(self) -> Result<(), Error> {
        match self.0 {
            #[cfg(feature = "postgres")]
            PilotTransactionInner::Postgres(transaction) => transaction.commit().await?,
            #[cfg(feature = "sqlite")]
            PilotTransactionInner::Sqlite(transaction) => transaction.commit().await?,
        }
        Ok(())
    }

    /// Borrows the transaction's connection.
    pub fn connection(&mut self) -> DatabaseConnection<'_> {
        match &mut self.0 {
            #[cfg(feature = "postgres")]
            PilotTransactionInner::Postgres(transaction) => {
                DatabaseConnection::Postgres(transaction)
            }
            #[cfg(feature = "sqlite")]
            PilotTransactionInner::Sqlite(transaction) => DatabaseConnection::Sqlite(transaction),
        }
    }
}

impl fmt::Debug for PilotTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match &self.0 {
            #[cfg(feature = "postgres")]
            PilotTransactionInner::Postgres(_) => DatabaseKind::Postgres,
            #[cfg(feature = "sqlite")]
            PilotTransactionInner::Sqlite(_) => DatabaseKind::Sqlite,
        };
        formatter
            .debug_struct("PilotTransaction")
            .field("kind", &kind)
            .finish_non_exhaustive()
    }
}

/// A queue producer's configuration, as an extension's
/// [`PilotProducer`] sees it.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ProducerConfiguration {
    /// Most jobs this client runs from the queue at once.
    pub max_workers: usize,
    /// The queue's persisted record, including its metadata and pause state.
    pub queue: Queue,
    /// The extension's settings for this queue, as configured with
    /// [`QueueConfigExt::with_extension_setting`](crate::__private::QueueConfigExt::with_extension_setting)
    /// and accepted by
    /// [`Pilot::validate_queue_settings`](crate::__private::Pilot::validate_queue_settings).
    pub settings: Map<String, Value>,
}

/// Inputs to [`Pilot::start_producer`](crate::__private::Pilot::start_producer).
#[derive(Debug)]
#[non_exhaustive]
pub struct ProducerStartContext {
    /// This client's identifier, recorded in `attempted_by` by claims.
    pub client_id: String,
    /// The producer's initial configuration.
    pub configuration: ProducerConfiguration,
    /// The client's database.
    pub database: PilotDatabase,
}

/// Inputs to one [`PilotProducer::keep_alive`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ProducerKeepAliveContext {
    /// Peers that haven't reported since this time are stale, like River
    /// Go's `StaleUpdatedAtHorizon`.
    pub stale_before: DateTime<Utc>,
}

/// Inputs to one [`PilotProducer::shutdown`] attempt.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ProducerShutdownContext {
    /// One-based attempt number, up to four.
    pub attempt: u32,
    /// How long River waits for this attempt before dropping it and trying
    /// again with a longer deadline.
    pub timeout: Duration,
}

/// Inputs to one [`PilotProducer::claim`].
#[derive(Debug)]
#[non_exhaustive]
pub struct ProducerClaimContext<'a> {
    /// This client's identifier. Claimed rows must end their `attempted_by`
    /// with it.
    pub client_id: &'a str,
    /// Cancelled when the producer stops claiming. It ends retries and
    /// backoff; a claim that already committed must still be returned.
    pub claim_stop: &'a CancellationToken,
    /// The client's database.
    pub database: &'a PilotDatabase,
    /// Most jobs the claim may return.
    pub limit: usize,
    /// The queue being claimed from.
    pub queue: &'a str,
}

/// River's standard claim, handed to [`PilotProducer::claim`].
///
/// [`claim`](Self::claim) consumes it, so a session runs River's claim at most
/// once per call, on the transaction of its choosing.
pub struct ProducerClaimNext<'a> {
    inner: &'a ClientInner,
    limit: usize,
    queue: &'a str,
}

impl<'a> ProducerClaimNext<'a> {
    pub(crate) const fn new(inner: &'a ClientInner, queue: &'a str, limit: usize) -> Self {
        Self {
            inner,
            limit,
            queue,
        }
    }

    /// Claims up to the claim's limit of available jobs on `connection`,
    /// exactly as River claims them without an extension. The claim takes
    /// effect when the connection's transaction commits.
    ///
    /// # Errors
    ///
    /// Returns the database error when the claim fails.
    pub async fn claim(self, connection: DatabaseConnection<'_>) -> Result<Vec<ClaimedJob>, Error> {
        Ok(
            crate::client::standard_claim(self.inner, connection, self.queue, self.limit)
                .await?
                .into_iter()
                .map(ClaimedJob::from_decoded)
                .collect(),
        )
    }
}

impl fmt::Debug for ProducerClaimNext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerClaimNext")
            .field("limit", &self.limit)
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}

/// An extension's state for one generation of one queue's producer, like
/// River Go's pilot `ProducerState`.
///
/// River creates a session with
/// [`Pilot::start_producer`](crate::__private::Pilot::start_producer) when a
/// producer starts, before its first claim, and drops it once the producer
/// has stopped. A session is never shared between generations: removing and
/// adding a queue again, or restarting a failed producer, starts a new one.
///
/// Calls into one session follow these rules:
///
/// - At most one [`claim`](Self::claim) is in flight at a time.
/// - [`configuration_changed`](Self::configuration_changed) runs between
///   claims, never during one.
/// - [`job_finished`](Self::job_finished) may run at any time, including while
///   a claim is in flight, and runs once for every row a claim returned that
///   River accepted.
/// - [`keep_alive`](Self::keep_alive) runs at River's producer report
///   interval, never overlapping another report, and may overlap a claim. It
///   keeps running while the producer drains after it stops claiming.
/// - [`shutdown`](Self::shutdown) runs once the last attempt has left the
///   producer and reporting has stopped. No other call follows it.
///
/// A producer stops claiming when the client stops or the queue is removed.
/// It then drains its running attempts and reports until they finish, so
/// peers keep counting them, before it shuts the session down.
///
/// A panic in [`claim`](Self::claim),
/// [`configuration_changed`](Self::configuration_changed), or
/// [`job_finished`](Self::job_finished) stops the client like a broken claim:
/// the producer cancels and drains its attempts, still calls `job_finished`
/// for each, stops reporting, and shuts the session down. A panic in
/// [`keep_alive`](Self::keep_alive) or [`shutdown`](Self::shutdown) is
/// logged and handled like an error from it.
#[async_trait]
pub trait PilotProducer: Send + Sync + 'static {
    /// Whether River claims through [`PilotProducer::claim`]. When `false`,
    /// River claims with its own statement and no transaction.
    fn intercepts_claim(&self) -> bool {
        false
    }

    /// Claims up to `context.limit` jobs, like River Go's pilot
    /// `JobGetAvailable`.
    ///
    /// The session owns the transaction boundary: it opens each transaction,
    /// runs [`ProducerClaimNext::claim`] or its own claim on it, and commits.
    /// It may retry a failed attempt in a new transaction, releasing the
    /// connection between attempts, until `context.claim_stop` is cancelled.
    /// It returns only committed rows, and must undo any tentative
    /// bookkeeping itself when it returns an error or when its future is
    /// dropped, which River does only when its runtime shuts down.
    ///
    /// River checks the returned rows before working them: they must
    /// identify distinct jobs, at most `context.limit` of them, running, in
    /// this queue, and last attempted by this client. A partly decoded row is
    /// checked with the fields that could be decoded. A result that breaks those
    /// rules is a protocol error that stops the client; its rows are left for
    /// the rescuer.
    ///
    /// The default opens one transaction and runs River's claim in it.
    ///
    /// # Errors
    ///
    /// Returns an error when nothing was claimed. River logs it and tries
    /// again after the queue's fetch cooldown.
    async fn claim(
        &self,
        context: ProducerClaimContext<'_>,
        next: ProducerClaimNext<'_>,
    ) -> Result<Vec<ClaimedJob>, PilotError> {
        let mut transaction = context.database.begin().await?;
        let jobs = next.claim(transaction.connection()).await?;
        transaction.commit().await?;
        Ok(jobs)
    }

    /// Replaces the session's configuration, between claims.
    ///
    /// River calls it when the queue's persisted record changes, such as its
    /// metadata or pause state, and when this client's configuration of the
    /// queue changes through
    /// [`LocalQueues::update`](crate::LocalQueues::update). It must not block
    /// or perform I/O.
    fn configuration_changed(&self, _configuration: &ProducerConfiguration) {}

    /// Reports that a claimed job's attempt left the producer, like River Go's
    /// `ProducerState.JobFinish`.
    ///
    /// River calls it once for each accepted claimed row, with that row as
    /// claimed, when its attempt exits: after its result is handed to the
    /// completer, or when its attempt is abandoned, for example because its
    /// worker outlived an abort during shutdown or its result couldn't be
    /// handed off. It doesn't wait for the result to be persisted. It must not
    /// block or perform I/O.
    fn job_finished(&self, _job: &JobRow) {}

    /// Reports that the producer is alive, like River Go's pilot
    /// `ProducerKeepAlive`.
    ///
    /// River calls it after a random delay of up to a second, then at the
    /// client's producer report interval, 30 seconds by default, including
    /// while the producer drains. River drops a call that runs longer than
    /// ten seconds.
    ///
    /// # Errors
    ///
    /// Returns an error when the report failed. River logs it and reports
    /// again at the next interval.
    async fn keep_alive(&self, _context: ProducerKeepAliveContext) -> Result<(), PilotError> {
        Ok(())
    }

    /// Releases the session's shared state once the producer has stopped,
    /// like River Go's pilot `ProducerShutdown`.
    ///
    /// River makes up to four attempts, one at a time, with deadlines of
    /// 100 milliseconds, 500 milliseconds, 2.5 seconds, and 12.5 seconds,
    /// dropping an attempt when its deadline passes, and logs the failure
    /// when every attempt fails.
    ///
    /// # Errors
    ///
    /// Returns an error when this attempt failed and another may succeed.
    async fn shutdown(&self, _context: ProducerShutdownContext) -> Result<(), PilotError> {
        Ok(())
    }
}

/// Checks the rows a session claimed before River works them.
pub(crate) fn validate_claimed(
    claimed: &[ClaimedJob],
    client_id: &str,
    queue: &str,
    limit: usize,
) -> Result<(), String> {
    if claimed.len() > limit {
        return Err(format!(
            "claim returned {} jobs, more than its limit of {limit}",
            claimed.len()
        ));
    }
    let mut ids = std::collections::HashSet::with_capacity(claimed.len());
    for job in claimed {
        // A row River can't identify could never be finished, so the
        // extension's accounting for it would leak.
        let Some(row) = job.row() else {
            return Err(format!(
                "claim returned a row that couldn't be identified: {}",
                job.decode_error().unwrap_or_default()
            ));
        };
        let id = row.id;
        if !ids.insert(id) {
            return Err(format!("claim returned job {id} more than once"));
        }
        // A partly decoded row still has its state and queue, which River
        // always decodes. Its `attempted_by` is empty when that column is
        // what couldn't be decoded, and then can't be checked.
        if row.state != crate::JobState::Running {
            return Err(format!("claim returned job {id} in state {}", row.state));
        }
        if row.queue != queue {
            return Err(format!(
                "claim for queue {queue:?} returned job {id} from queue {:?}",
                row.queue
            ));
        }
        let attempted_by_undecodable = job.column_undecodable("attempted_by");
        if !attempted_by_undecodable
            && row.attempted_by.last().map(String::as_str) != Some(client_id)
        {
            return Err(format!(
                "claim returned job {id} not last attempted by this client"
            ));
        }
    }
    Ok(())
}

/// A producer session River runs for a queue generation.
pub(crate) type SharedProducer = Arc<dyn PilotProducer>;
