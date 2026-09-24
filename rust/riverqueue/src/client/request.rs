//! Plumbing shared by the client's request builders.

#[allow(clippy::wildcard_imports)]
use super::*;

/// Where a request runs.
pub(super) enum Target<'a> {
    /// The client's own pool, in a transaction River commits when the
    /// operation needs one.
    Client,
    /// A caller-managed transaction. A transaction from another backend is
    /// reported when the request is awaited.
    Transaction(Result<PilotDatabaseConnection<'a>, Error>),
}

impl<'a> Target<'a> {
    pub(super) fn transaction<E>(client: &Client, executor: E) -> Self
    where
        E: DatabaseTransactionExecutor<'a>,
    {
        Self::Transaction(client.inner.transaction_connection(executor))
    }

    /// Returns the caller's transaction connection, or `None` to use the
    /// client's own pool.
    pub(super) fn into_executor(self) -> Result<Option<PilotDatabaseConnection<'a>>, Error> {
        match self {
            Self::Client => Ok(None),
            Self::Transaction(connection) => connection.map(Some),
        }
    }
}
