//! Operations on persisted queue records.

#[allow(clippy::wildcard_imports)]
use super::*;
use crate::storage::Access;
use crate::{Queue, QueueListParams, QueueSelector, QueueUpdateParams};

/// Operations on persisted queue records, returned by [`Client::queues`].
///
/// A queue record exists for every queue a client has worked, and is shared
/// by every client of the database, including River clients in other
/// languages. Pausing a queue stops every client from fetching its jobs.
///
/// Each method returns a request that runs on the client's own pool when
/// awaited, or in a caller-managed transaction after `.tx(&mut tx)`:
///
/// ```no_run
/// # use riverqueue::QueueSelector;
/// # async fn example(client: riverqueue::Client, pool: sqlx::PgPool) -> Result<(), riverqueue::Error> {
/// client.queues().pause("email").await?;
///
/// let mut tx = pool.begin().await?;
/// client.queues().resume(QueueSelector::All).tx(&mut tx).await?;
/// tx.commit().await?;
/// # Ok(())
/// # }
/// ```
///
/// Requests don't run until awaited. Dropping one before it completes rolls
/// back River's own transaction; with `.tx`, the caller's transaction may
/// contain the operation's partial effects and should be rolled back.
#[derive(Clone, Copy, Debug)]
pub struct Queues<'a> {
    client: &'a Client,
}

impl Client {
    /// Returns operations on persisted queue records: getting, listing,
    /// pausing, resuming, and updating them.
    #[must_use]
    pub const fn queues(&self) -> Queues<'_> {
        Queues { client: self }
    }
}

impl<'a> Queues<'a> {
    /// Gets a queue record by name.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when the queue has no record,
    /// [`Error::DatabaseMismatch`] for a transaction from another backend, and
    /// [`Error::Database`] when the database operation fails.
    pub fn get(&self, name: impl Into<String>) -> QueueGetRequest<'a> {
        QueueGetRequest {
            client: self.client,
            name: name.into(),
            target: Target::Client,
        }
    }

    /// Lists queue records in name order.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidJob`] for a limit outside one through 10,000,
    /// [`Error::DatabaseMismatch`] for a transaction from another backend, and
    /// [`Error::Database`] when the database operation fails.
    pub fn list(&self, params: QueueListParams) -> QueueListRequest<'a> {
        QueueListRequest {
            client: self.client,
            params,
            target: Target::Client,
        }
    }

    /// Pauses one queue, or every queue with [`QueueSelector::All`].
    ///
    /// Clients stop fetching jobs from a paused queue, while jobs already
    /// running finish normally. Clients learn of the pause through a
    /// notification when it commits, or on their next poll of the queue's
    /// record when they run without notifications. Pausing a paused queue
    /// changes nothing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when a named queue has no record,
    /// [`Error::DatabaseMismatch`] for a transaction from another backend, and
    /// [`Error::Database`] when the database operation fails.
    pub fn pause(&self, selector: impl Into<QueueSelector>) -> QueuePauseRequest<'a> {
        QueuePauseRequest {
            client: self.client,
            selector: selector.into(),
            target: Target::Client,
        }
    }

    /// Resumes one paused queue, or every queue with [`QueueSelector::All`].
    ///
    /// Clients learn of the change as they do for [`pause`](Self::pause).
    /// Resuming a queue that isn't paused changes nothing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when a named queue has no record,
    /// [`Error::DatabaseMismatch`] for a transaction from another backend, and
    /// [`Error::Database`] when the database operation fails.
    pub fn resume(&self, selector: impl Into<QueueSelector>) -> QueueResumeRequest<'a> {
        QueueResumeRequest {
            client: self.client,
            selector: selector.into(),
            target: Target::Client,
        }
    }

    /// Updates a queue record and returns it.
    ///
    /// New metadata is sent to the clients working the queue when the update
    /// commits.
    ///
    /// ```no_run
    /// # use riverqueue::QueueUpdateParams;
    /// # async fn example(client: riverqueue::Client) -> Result<(), riverqueue::Error> {
    /// let mut metadata = serde_json::Map::new();
    /// metadata.insert("owner".to_owned(), "billing".into());
    /// let queue = client
    ///     .queues()
    ///     .update("invoices", QueueUpdateParams::new().metadata(metadata))
    ///     .await?;
    /// assert_eq!(queue.metadata["owner"], "billing");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when the queue has no record,
    /// [`Error::DatabaseMismatch`] for a transaction from another backend, and
    /// [`Error::Database`] when the database operation fails.
    pub fn update(
        &self,
        name: impl Into<String>,
        params: QueueUpdateParams,
    ) -> QueueUpdateRequest<'a> {
        QueueUpdateRequest {
            client: self.client,
            name: name.into(),
            params,
            target: Target::Client,
        }
    }
}

request_type! {
    /// A queue lookup, returned by [`Queues::get`]. Await it to get the
    /// queue record.
    QueueGetRequest { name: String } -> Queue
}

impl QueueGetRequest<'_> {
    async fn run(self) -> Result<Queue, Error> {
        let inner = &self.client.inner;
        let mut session = self.target.session(inner, Access::Autocommit).await?;
        session
            .storage(inner)
            .queue_get(&self.name)
            .await?
            .ok_or(Error::NotFound)
    }
}

request_type! {
    /// A queue listing, returned by [`Queues::list`]. Await it to get queue
    /// records in name order.
    QueueListRequest { params: QueueListParams } -> Vec<Queue>
}

impl QueueListRequest<'_> {
    async fn run(self) -> Result<Vec<Queue>, Error> {
        let inner = &self.client.inner;
        let mut session = self.target.session(inner, Access::Autocommit).await?;
        session.storage(inner).queue_list(&self.params).await
    }
}

request_type! {
    /// A queue pause, returned by [`Queues::pause`]. Await it to pause the
    /// selected queues.
    QueuePauseRequest { selector: QueueSelector } -> ()
}

impl QueuePauseRequest<'_> {
    async fn run(self) -> Result<(), Error> {
        set_paused(self.client, self.target, &self.selector, true).await
    }
}

request_type! {
    /// A queue resumption, returned by [`Queues::resume`]. Await it to
    /// resume the selected queues.
    QueueResumeRequest { selector: QueueSelector } -> ()
}

impl QueueResumeRequest<'_> {
    async fn run(self) -> Result<(), Error> {
        set_paused(self.client, self.target, &self.selector, false).await
    }
}

async fn set_paused(
    client: &Client,
    target: Target<'_>,
    selector: &QueueSelector,
    paused: bool,
) -> Result<(), Error> {
    let Some(name) = selector.protocol_name() else {
        return Err(Error::NotFound);
    };
    let inner = &client.inner;
    let own_transaction = !target.is_transaction();
    let mut session = target.session(inner, Access::Transaction).await?;
    session
        .storage(inner)
        .queue_set_paused(name, paused)
        .await?;
    session.commit().await?;
    // Wake this client's producers at once rather than at their next
    // notification or poll. A caller's transaction may still roll back, so
    // it relies on the committed notification alone.
    if own_transaction {
        client.signal_queue_control(name);
    }
    Ok(())
}

request_type! {
    /// A queue update, returned by [`Queues::update`]. Await it to update the
    /// queue and get its new record.
    QueueUpdateRequest { name: String, params: QueueUpdateParams } -> Queue
}

impl QueueUpdateRequest<'_> {
    async fn run(self) -> Result<Queue, Error> {
        let inner = &self.client.inner;
        let mut session = self.target.session(inner, Access::Transaction).await?;
        let queue = session
            .storage(inner)
            .queue_update(&self.name, self.params.metadata.as_ref())
            .await?;
        session.commit().await?;
        Ok(queue)
    }
}
