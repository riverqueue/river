//! This client's own queue configuration.

use std::sync::PoisonError;

#[allow(clippy::wildcard_imports)]
use super::*;

/// The queues this client works, returned by [`Client::local_queues`].
///
/// This is the client's runtime configuration, not the shared queue records
/// managed through [`Client::queues`]: adding or removing a queue here
/// changes only which queues this client's producers fetch from.
///
/// Changes apply to a running client asynchronously. Each method updates the
/// configuration and returns at once, and the client then starts, stops, or
/// restarts the affected producer:
///
/// - An added queue starts fetching jobs.
/// - A removed queue stops fetching. Jobs it already fetched keep running
///   until they finish, and its persisted jobs and queue record are left for
///   other clients.
/// - A reconfigured queue stops fetching under its old configuration, and its
///   replacement starts only after every job the old producer fetched has
///   finished, so the queue never runs jobs under both configurations at
///   once.
///
/// Until the client observes a change, the previous producer may still fetch
/// jobs, which then run under the previous configuration.
///
/// ```no_run
/// # use riverqueue::QueueConfig;
/// # fn example(client: &riverqueue::Client) -> Result<(), riverqueue::Error> {
/// client.local_queues().add("reports", QueueConfig::new(2))?;
/// assert!(client.local_queues().configs().contains_key("reports"));
/// let removed = client.local_queues().remove("reports");
/// assert_eq!(removed, Some(QueueConfig::new(2)));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug)]
pub struct LocalQueues<'a> {
    client: &'a Client,
}

impl Client {
    /// Returns the configuration of the queues this client works, which can
    /// change while it runs.
    #[must_use]
    pub const fn local_queues(&self) -> LocalQueues<'_> {
        LocalQueues { client: self }
    }
}

impl LocalQueues<'_> {
    /// Adds a queue for this client to work, or replaces the configuration
    /// of a queue it already works.
    ///
    /// A running client starts or restarts only this queue's producer; other
    /// queues keep running. See [`LocalQueues`] for when the change takes
    /// effect. Unlike River Go's `QueueBundle.Add`, adding a configured queue
    /// reconfigures it rather than failing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidJob`] for an invalid queue name, and
    /// [`Error::Configuration`] for an invalid configuration or when the
    /// client has no workers to run the queue's jobs.
    pub fn add(&self, name: impl Into<String>, config: QueueConfig) -> Result<(), Error> {
        let name = name.into();
        config.validate(&name)?;
        let inner = &self.client.inner;
        if inner.workers.kinds().is_empty() {
            return Err(Error::configuration(
                "workers must be configured when queues are configured".to_owned(),
            ));
        }
        inner
            .queues
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name, config);
        self.changed();
        Ok(())
    }

    /// Returns a snapshot of the queues this client works and their
    /// configurations.
    #[must_use]
    pub fn configs(&self) -> HashMap<String, QueueConfig> {
        self.client
            .inner
            .queues
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Stops working a queue and returns its configuration, or `None` when
    /// this client doesn't work the queue.
    ///
    /// See [`LocalQueues`] for when the change takes effect. Unlike River Go's
    /// `QueueBundle.Remove`, this doesn't wait for the queue's running jobs to
    /// finish.
    pub fn remove(&self, name: &str) -> Option<QueueConfig> {
        let previous = self
            .client
            .inner
            .queues
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(name);
        if previous.is_some() {
            self.changed();
        }
        previous
    }

    /// Tells a running client's queue supervisor to reconcile its producers.
    fn changed(self) {
        self.client
            .inner
            .queue_changes
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}
