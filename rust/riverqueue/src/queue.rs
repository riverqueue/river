//! Persisted queue configuration.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A queue currently or recently operated by a River client.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[non_exhaustive]
pub struct Queue {
    /// Time at which this active queue record was created.
    pub created_at: DateTime<Utc>,
    /// Reserved queue metadata.
    pub metadata: Map<String, Value>,
    /// Stable queue name.
    pub name: String,
    /// Time at which the queue was paused.
    pub paused_at: Option<DateTime<Utc>>,
    /// Last client heartbeat or configuration update.
    pub updated_at: DateTime<Utc>,
}

/// Parameters for listing queues.
#[derive(Clone, Debug)]
pub struct QueueListParams {
    pub(crate) limit: u32,
}

impl Default for QueueListParams {
    fn default() -> Self {
        Self { limit: 100 }
    }
}

impl QueueListParams {
    /// Sets the maximum number of queues returned, from one through 10,000.
    /// Defaults to 100. Listing fails with a limit outside that range.
    #[must_use]
    pub const fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }
}

/// The persisted queues that [`Queues::pause`](crate::Queues::pause) and
/// [`Queues::resume`](crate::Queues::resume) act on.
///
/// Strings convert into [`Named`](Self::Named), so a queue can be passed by
/// name:
///
/// ```no_run
/// # use riverqueue::QueueSelector;
/// # async fn example(client: riverqueue::Client) -> Result<(), riverqueue::Error> {
/// client.queues().pause("email").await?;
/// client.queues().resume(QueueSelector::All).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum QueueSelector {
    /// Every queue that has a persisted record. Succeeds even when there are
    /// none.
    All,
    /// The queue with this name, which must have a persisted record. A name
    /// is matched literally, so `Named("*")` names no queue.
    Named(String),
}

impl QueueSelector {
    /// Returns the queue name River's storage and notification protocol use
    /// for this selection, or `None` for a name no queue can have.
    pub(crate) fn protocol_name(&self) -> Option<&str> {
        match self {
            Self::All => Some(crate::storage::QUEUE_ALL),
            Self::Named(name) if name == crate::storage::QUEUE_ALL => None,
            Self::Named(name) => Some(name),
        }
    }
}

impl From<&str> for QueueSelector {
    fn from(name: &str) -> Self {
        Self::Named(name.to_owned())
    }
}

impl From<&String> for QueueSelector {
    fn from(name: &String) -> Self {
        Self::Named(name.clone())
    }
}

impl From<String> for QueueSelector {
    fn from(name: String) -> Self {
        Self::Named(name)
    }
}

/// Changes applied by [`Queues::update`](crate::Queues::update).
///
/// Fields left unset keep their current value. The queue's `updated_at` is
/// refreshed either way, like River Go.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct QueueUpdateParams {
    pub(crate) metadata: Option<Map<String, Value>>,
}

impl QueueUpdateParams {
    /// Creates parameters that change nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the queue's metadata object. Clients working the queue are
    /// notified of the new metadata.
    #[must_use]
    pub fn metadata(mut self, metadata: Map<String, Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }
}
