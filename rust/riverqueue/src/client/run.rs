//! Starting clients and observing their lifecycle.

#[allow(clippy::wildcard_imports)]
use super::*;

impl Client {
    /// Starts configured queues and returns a lifecycle handle.
    #[allow(
        clippy::too_many_lines,
        reason = "keeps startup ordering and ownership visible"
    )]
    pub fn start(&self) -> Result<RunHandle, Error> {
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| Error::RuntimeUnavailable {
                operation: "starting a client",
            })?;
        if self
            .inner
            .queues
            .read()
            .map_err(|_| Error::runtime("queue configuration lock poisoned".to_owned()))?
            .is_empty()
        {
            return Err(Error::configuration(
                "at least one queue is required to start a client".to_owned(),
            ));
        }
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::runtime("client is already running".to_owned()));
        }
        let fetch_cancel = CancellationToken::new();
        let work_cancel = CancellationToken::new();
        let inner = Arc::clone(&self.inner);
        let fetch_for_task = fetch_cancel.clone();
        let work_for_task = work_cancel.clone();
        let (ready_sender, ready) = oneshot::channel();
        let join = runtime.spawn(async move {
            let result = async {
                let notifications = inner.queue_notifications.clone();
                let (completion_sender, completion_receiver) = mpsc::channel(10_000);
                *inner
                    .completion_sender
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some(completion_sender.downgrade());
                let mut queues = JoinSet::new();
                queues.spawn(run_dynamic_queues(
                    Arc::clone(&inner),
                    completion_sender,
                    fetch_for_task.child_token(),
                    work_for_task.child_token(),
                    notifications.clone(),
                    inner.queue_changes.subscribe(),
                ));
                if inner.poll_only {
                    let _ = ready_sender.send(Ok(()));
                } else {
                    match inner.database.kind() {
                        #[cfg(feature = "postgres")]
                        DatabaseKind::Postgres => {
                            queues.spawn(run_notifications(
                                Arc::clone(&inner),
                                fetch_for_task.child_token(),
                                notifications.clone(),
                                ready_sender,
                            ));
                        }
                        #[cfg(feature = "sqlite")]
                        DatabaseKind::Sqlite => {
                            queues.spawn(run_sqlite_notifications(
                                Arc::clone(&inner),
                                fetch_for_task.child_token(),
                                notifications.clone(),
                                ready_sender,
                            ));
                        }
                    }
                }
                queues.spawn(crate::maintenance::run_maintenance(
                    Arc::clone(&inner),
                    fetch_for_task.child_token(),
                    notifications.subscribe(),
                ));
                for service in inner.pilot.runtime_services() {
                    let pool = inner.pilot_database_pool();
                    let database = inner.pilot_database_config();
                    let service_cancel = fetch_for_task.child_token();
                    queues.spawn(async move {
                        service
                            .run(pool, database, service_cancel)
                            .await
                            .map_err(|service_error| Error::Extension {
                                phase: "runtime service",
                                source: service_error,
                            })
                    });
                }
                queues.spawn(run_completion_batcher(
                    Arc::clone(&inner),
                    completion_receiver,
                ));
                while let Some(result) = queues.join_next().await {
                    result.map_err(Error::from_join)??;
                }
                Ok(())
            }
            .await;
            inner.started.store(false, Ordering::Release);
            result
        });
        Ok(RunHandle {
            fetch_cancel: Some(fetch_cancel),
            join: Some(join),
            ready: Some(ready),
            soft_stop_timeout: self.inner.soft_stop_timeout,
            work_cancel: Some(work_cancel),
        })
    }
}

/// Controls one running client instance.
///
/// Dropping the handle requests immediate cancellation but cannot wait for
/// in-flight database work to finish. Use [`RunHandle::shutdown`] or
/// [`RunHandle::shutdown_now`] when shutdown must be observed before returning,
/// or [`RunHandle::detach`] to deliberately leave the client running.
#[must_use = "dropping the handle requests immediate client shutdown; call detach to run it independently"]
pub struct RunHandle {
    fetch_cancel: Option<CancellationToken>,
    join: Option<tokio::task::JoinHandle<Result<(), Error>>>,
    ready: Option<oneshot::Receiver<Result<(), String>>>,
    soft_stop_timeout: Option<Duration>,
    work_cancel: Option<CancellationToken>,
}

impl std::fmt::Debug for RunHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RunHandle")
            .field("attached", &self.join.is_some())
            .field("ready_observed", &self.ready.is_none())
            .finish_non_exhaustive()
    }
}

impl RunHandle {
    /// Leaves the client running independently of this handle.
    ///
    /// This permanently relinquishes lifecycle control. The runtime then ends
    /// only on an internal error or process shutdown. Most applications should
    /// retain the handle and use an awaited shutdown method instead.
    pub fn detach(mut self) {
        self.fetch_cancel.take();
        self.work_cancel.take();
        self.join.take();
    }

    /// Waits until the selected backend's notification path is active.
    ///
    /// Poll-only clients are ready immediately. Calling this more than once is
    /// harmless.
    pub async fn wait_ready(&mut self) -> Result<(), Error> {
        let Some(ready) = self.ready.take() else {
            return Ok(());
        };
        ready
            .await
            .map_err(|_| Error::runtime("client stopped before becoming ready".to_owned()))?
            .map_err(Error::runtime)
    }

    /// Stops fetching and waits indefinitely for active jobs.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        if let Some(cancellation) = self.fetch_cancel.take() {
            cancellation.cancel();
        }
        let Some(mut join) = self.join.take() else {
            return Ok(());
        };
        if let Some(timeout) = self.soft_stop_timeout {
            tokio::select! {
                result = &mut join => return join_client_result(result),
                () = tokio::time::sleep(timeout) => {
                    if let Some(cancellation) = self.work_cancel.take() {
                        cancellation.cancel();
                    }
                },
            }
        }
        join_client_result(join.await)
    }

    /// Stops fetching and cancels active job contexts.
    pub async fn shutdown_now(mut self) -> Result<(), Error> {
        if let Some(cancellation) = self.fetch_cancel.take() {
            cancellation.cancel();
        }
        if let Some(cancellation) = self.work_cancel.take() {
            cancellation.cancel();
        }
        match self.join.take() {
            Some(join) => join_client_result(join.await),
            None => Ok(()),
        }
    }

    /// Waits for the client to stop.
    pub async fn wait(mut self) -> Result<(), Error> {
        match self.join.take() {
            Some(join) => join_client_result(join.await),
            None => Ok(()),
        }
    }
}

impl Drop for RunHandle {
    fn drop(&mut self) {
        if let Some(cancellation) = &self.fetch_cancel {
            cancellation.cancel();
        }
        if let Some(cancellation) = &self.work_cancel {
            cancellation.cancel();
        }
    }
}

pub(super) fn join_client_result(
    result: Result<Result<(), Error>, tokio::task::JoinError>,
) -> Result<(), Error> {
    result.map_err(Error::from_join)??;
    Ok(())
}
