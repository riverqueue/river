//! This client's own queue configuration.

use std::sync::PoisonError;

#[allow(clippy::wildcard_imports)]
use super::*;

/// The queues this client works, returned by [`Client::local_queues`].
///
/// This is the client's runtime configuration, not the shared queue records
/// managed through [`Client::queues`]: adding or removing a queue here
/// changes only which queues this client's producers fetch from. Like River
/// Go's `QueueBundle`, adding a queue that's already added is an error, and
/// removing one waits for its producer to stop.
///
/// Changes apply to a running client asynchronously:
///
/// - An added queue starts fetching jobs shortly after [`add`](Self::add)
///   returns.
/// - An updated queue applies its new configuration while it runs. Lowering
///   `max_workers` stops new fetches until enough running jobs finish; it
///   never cancels them.
/// - A removed queue stops fetching, and [`remove`](Self::remove) waits for
///   the jobs it already fetched to finish. Its persisted jobs and queue
///   record are left for other clients. Its name stays reserved until then,
///   so the queue never runs under two producers at once.
///
/// ```no_run
/// # use riverqueue::QueueConfig;
/// # async fn example(client: &riverqueue::Client) -> Result<(), riverqueue::Error> {
/// client.local_queues().add("reports", QueueConfig::new(2))?;
/// client.local_queues().update("reports", QueueConfig::new(4))?;
/// assert!(client.local_queues().configs().contains_key("reports"));
/// let removed = client.local_queues().remove("reports").await?;
/// assert_eq!(removed, QueueConfig::new(4));
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
    /// Adds a queue for this client to work.
    ///
    /// A running client starts only this queue's producer; other queues keep
    /// running. See [`LocalQueues`] for when the change takes effect.
    ///
    /// # Errors
    ///
    /// Returns [`Error::QueueAlreadyAdded`] when the queue is already added
    /// or a removal of it is still waiting for its producer to stop,
    /// [`Error::InvalidJob`] for an invalid queue name,
    /// [`Error::Configuration`] for an invalid configuration or when the
    /// client has no workers to run the queue's jobs, and
    /// [`Error::Extension`] when an add-on crate rejects the configuration's
    /// extension settings.
    pub fn add(&self, name: impl Into<String>, config: QueueConfig) -> Result<(), Error> {
        let name = name.into();
        let inner = &self.client.inner;
        self.validate(&name, &config)?;
        if inner.workers.kinds().is_empty() {
            return Err(Error::configuration(
                "workers must be configured when queues are configured".to_owned(),
            ));
        }
        {
            let mut queues = inner.queues.write().unwrap_or_else(PoisonError::into_inner);
            if queues.contains_key(&name) || inner.live_queues.borrow().contains(&name) {
                return Err(Error::QueueAlreadyAdded { name });
            }
            queues.insert(name, config);
        }
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

    /// Stops working a queue, waits until its producer has stopped, and
    /// returns the queue's configuration.
    ///
    /// The producer stops fetching at once and then waits for the jobs it
    /// fetched to finish, like River Go's `QueueBundle.Remove`. A client
    /// that isn't running returns at once.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. The queue is removed when the future is
    /// first polled; dropping the future afterwards stops only the wait,
    /// and the queue's name stays reserved until its producer stops.
    ///
    /// # Errors
    ///
    /// Returns [`Error::QueueNotAdded`] when this client doesn't work the
    /// queue.
    pub async fn remove(&self, name: &str) -> Result<QueueConfig, Error> {
        let inner = &self.client.inner;
        let mut live = inner.live_queues.subscribe();
        let config = inner
            .queues
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(name)
            .ok_or_else(|| Error::QueueNotAdded {
                name: name.to_owned(),
            })?;
        self.changed();
        // The sender lives as long as the client, so this ends only when the
        // producer is gone.
        let _ = live.wait_for(|live| !live.contains(name)).await;
        Ok(config)
    }

    /// Replaces the configuration of a queue this client works.
    ///
    /// A running producer applies the new configuration without stopping.
    /// See [`LocalQueues`] for when the change takes effect.
    ///
    /// # Errors
    ///
    /// Returns [`Error::QueueNotAdded`] when this client doesn't work the
    /// queue, [`Error::Configuration`] for an invalid configuration, and
    /// [`Error::Extension`] when an add-on crate rejects the configuration's
    /// extension settings.
    pub fn update(&self, name: &str, config: QueueConfig) -> Result<(), Error> {
        self.validate(name, &config)?;
        {
            let mut queues = self
                .client
                .inner
                .queues
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            let Some(current) = queues.get_mut(name) else {
                return Err(Error::QueueNotAdded {
                    name: name.to_owned(),
                });
            };
            *current = config;
        }
        self.changed();
        Ok(())
    }

    /// Tells a running client's queue supervisor to reconcile its producers.
    fn changed(self) {
        self.client
            .inner
            .queue_changes
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    fn validate(self, name: &str, config: &QueueConfig) -> Result<(), Error> {
        config.validate(name)?;
        validate_queue_settings(self.client.inner.pilot.as_ref(), name, config)
    }
}
