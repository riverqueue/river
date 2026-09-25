//! Leadership resignation requests.

#[allow(clippy::wildcard_imports)]
use super::*;
use crate::storage::Access;

impl Client {
    /// Asks the current leader to resign, so that clients elect a leader
    /// again.
    ///
    /// The request is a notification delivered to every client, which usually
    /// makes the leader resign, but has no effect when no leader is elected.
    /// With [`tx`](ResignRequest::tx), the notification is sent only when the
    /// transaction commits.
    ///
    /// ```no_run
    /// # async fn example(client: riverqueue::Client) -> Result<(), riverqueue::Error> {
    /// client.request_resign().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::DatabaseMismatch`] for a transaction from another
    /// backend and [`Error::Database`] when the database operation fails.
    pub fn request_resign(&self) -> ResignRequest<'_> {
        ResignRequest {
            client: self,
            target: Target::Client,
        }
    }
}

request_type! {
    /// A leadership resignation request, returned by
    /// [`Client::request_resign`]. Await it to send the request.
    ResignRequest {} -> ()
}

impl ResignRequest<'_> {
    async fn run(self) -> Result<(), Error> {
        let inner = &self.client.inner;
        let own_transaction = !self.target.is_transaction();
        let mut session = self.target.session(inner, Access::Transaction).await?;
        session.storage(inner).leader_request_resign().await?;
        session.commit().await?;
        // A poll-only client reads neither a listener nor the notification
        // outbox, so it learns of its own request directly. Any other client
        // receives the committed notification like every other client does;
        // also signalling it locally would deliver the request twice.
        if own_transaction && inner.poll_only {
            let _ = inner
                .queue_notifications
                .send(RuntimeNotification::LeadershipRequestResign);
        }
        Ok(())
    }
}
