//! Plumbing shared by the client's request builders.

use std::fmt;

#[allow(clippy::wildcard_imports)]
use super::*;
use crate::storage::{Access, Session};

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

impl<'a> Target<'a> {
    /// Opens the session the request runs in: the caller's transaction, or
    /// a connection from the client's own pool with the given access.
    pub(super) async fn session(
        self,
        inner: &ClientInner,
        access: Access,
    ) -> Result<Session<'a>, Error> {
        match self {
            Self::Client => Session::begin(&inner.database, access).await,
            Self::Transaction(connection) => Ok(Session::caller(connection?)),
        }
    }

    pub(super) const fn is_transaction(&self) -> bool {
        matches!(self, Self::Transaction(_))
    }
}

impl fmt::Debug for Target<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(if self.is_transaction() {
            "Transaction"
        } else {
            "Client"
        })
    }
}

/// Defines a request builder that runs on the client's pool or, with `tx`,
/// in a caller-managed transaction, and that runs when awaited.
///
/// The request type must implement `async fn run(self) -> Result<Output,
/// Error>`.
macro_rules! request_type {
    (
        $(#[$attr:meta])*
        $name:ident { $($field:ident: $type:ty),* $(,)? } -> $output:ty
    ) => {
        $(#[$attr])*
        #[must_use = "requests do nothing unless awaited"]
        pub struct $name<'a> {
            client: &'a Client,
            $($field: $type,)*
            target: Target<'a>,
        }

        impl<'a> $name<'a> {
            /// Runs the request in a caller-managed transaction instead of
            /// on the client's own pool.
            ///
            /// The request sees the transaction's uncommitted changes, and
            /// its own changes and notifications take effect only when the
            /// caller commits. `executor` must be a SQLx transaction for the
            /// client's database backend; begin SQLite transactions that may
            /// write with `BEGIN IMMEDIATE`.
            pub fn tx<'t, E>(self, executor: E) -> $name<'t>
            where
                'a: 't,
                E: DatabaseTransactionExecutor<'t>,
            {
                $name {
                    client: self.client,
                    $($field: self.$field,)*
                    target: Target::transaction(self.client, executor),
                }
            }
        }

        impl std::fmt::Debug for $name<'_> {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    $(.field(stringify!($field), &self.$field))*
                    .field("target", &self.target)
                    .finish_non_exhaustive()
            }
        }

        impl<'a> std::future::IntoFuture for $name<'a> {
            type Output = Result<$output, Error>;
            type IntoFuture =
                std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

            fn into_future(self) -> Self::IntoFuture {
                Box::pin(self.run())
            }
        }
    };
}

pub(super) use request_type;
