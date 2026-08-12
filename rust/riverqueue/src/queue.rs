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
#[non_exhaustive]
pub struct QueueListParams {
    /// Maximum rows, from one through 10,000.
    pub limit: i32,
}

impl Default for QueueListParams {
    fn default() -> Self {
        Self { limit: 100 }
    }
}

impl QueueListParams {
    /// Sets the maximum returned rows.
    #[must_use]
    pub const fn with_limit(mut self, limit: i32) -> Self {
        self.limit = limit;
        self
    }
}
