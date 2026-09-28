//! Persisted queue configuration.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

/// A queue currently or recently operated by a River client.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[non_exhaustive]
pub struct Queue {
    /// Time at which this active queue record was created.
    pub created_at: DateTime<Utc>,
    /// Reserved queue metadata.
    pub metadata: Map<String, Value>,
    /// The metadata's text as the database renders it, keeping the key
    /// order, duplicate keys, and number literals the parsed map loses.
    #[serde(skip)]
    pub(crate) metadata_text: String,
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
/// refreshed either way.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct QueueUpdateParams {
    pub(crate) metadata: Option<QueueMetadata>,
}

/// New queue metadata, as a map River encodes or as the caller's JSON text.
#[derive(Clone, Debug)]
pub(crate) enum QueueMetadata {
    Map(Map<String, Value>),
    Raw(Box<RawValue>),
}

impl QueueMetadata {
    /// Returns the metadata's JSON text, encoding a map with River's
    /// encoding.
    pub(crate) fn into_raw(self) -> Result<Box<RawValue>, serde_json::Error> {
        match self {
            Self::Map(map) => RawValue::from_string(crate::encoding::to_go_string(&map)?),
            Self::Raw(raw) => Ok(raw),
        }
    }
}

impl PartialEq for QueueUpdateParams {
    fn eq(&self, other: &Self) -> bool {
        match (&self.metadata, &other.metadata) {
            (None, None) => true,
            (Some(QueueMetadata::Map(left)), Some(QueueMetadata::Map(right))) => left == right,
            (Some(QueueMetadata::Raw(left)), Some(QueueMetadata::Raw(right))) => {
                left.get() == right.get()
            }
            _ => false,
        }
    }
}

impl QueueUpdateParams {
    /// Creates parameters that change nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the queue's metadata object. Clients working the queue are
    /// notified of the new metadata.
    ///
    /// The map is encoded with River's JSON [`encoding`](crate::encoding),
    /// with its keys in sorted order. Use
    /// [`metadata_raw`](Self::metadata_raw) to keep an existing JSON text
    /// as written.
    #[must_use]
    pub fn metadata(mut self, metadata: Map<String, Value>) -> Self {
        self.metadata = Some(QueueMetadata::Map(metadata));
        self
    }

    /// Replaces the queue's metadata object with JSON text kept as written,
    /// in its key order and with its escapes, the way other River clients
    /// store and announce metadata given as text. The text must be a JSON
    /// object; the update fails with [`Error::Configuration`](crate::Error::Configuration)
    /// otherwise.
    #[must_use]
    pub fn metadata_raw(mut self, metadata: Box<RawValue>) -> Self {
        self.metadata = Some(QueueMetadata::Raw(metadata));
        self
    }
}
