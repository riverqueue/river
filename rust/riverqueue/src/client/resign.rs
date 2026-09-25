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
        // Without a listener, this client also learns of the request directly
        // rather than at its next poll of the notification outbox.
        if own_transaction && !inner.database.supports_listener() {
            let _ = inner
                .queue_notifications
                .send(RuntimeNotification::LeadershipRequestResign);
        }
        Ok(())
    }
}
