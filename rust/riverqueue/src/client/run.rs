//! Starting clients and observing their lifecycle.

#[allow(clippy::wildcard_imports)]
use super::*;

impl Client {
    /// Starts configured queues and returns a lifecycle handle.
    ///
    /// The client supervises its services: a notification listener, SQLite
    /// outbox poller, maintenance, or extension service that fails is logged
    /// and restarted with backoff, and producers keep polling meanwhile, so a
    /// database outage never stops the client. Only a failure of the
    /// producers or the completer, which would leave jobs unworked or
    /// unpersisted, stops the client: work is then cancelled, every worker is
    /// awaited, and [`RunHandle::wait`] returns the error.
    ///
    /// With notifications enabled on PostgreSQL, the client opens one
    /// dedicated listener connection with the pool's connect options. It is
    /// not taken from, and does not count against, the pool's
    /// `max_connections`.
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
        let (ready_sender, ready) = oneshot::channel();
        let supervisor = Supervisor {
            fetch_cancel: fetch_cancel.clone(),
            inner: Arc::clone(&inner),
            restarts: HashMap::new(),
            services: HashMap::new(),
            tasks: JoinSet::new(),
            work_cancel: work_cancel.clone(),
        };
        let join = runtime.spawn(async move {
            let result = supervisor.run(ready_sender).await;
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

/// A long-running service owned by a started client.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Service {
    Completer,
    Maintenance,
    Notifier,
    Queues,
    Extension(usize),
}

impl Service {
    /// Services whose failure leaves jobs unworked or unpersisted. Every other
    /// service is restarted after a failure.
    const fn is_essential(self) -> bool {
        matches!(self, Self::Completer | Self::Queues)
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Completer => "completer",
            Self::Maintenance => "maintenance",
            Self::Notifier => "notifier",
            Self::Queues => "producers",
            Self::Extension(_) => "extension runtime service",
        }
    }
}

/// Runs a started client's services and restarts the ones that fail.
struct Supervisor {
    fetch_cancel: CancellationToken,
    inner: Arc<ClientInner>,
    restarts: HashMap<Service, u32>,
    services: HashMap<tokio::task::Id, Service>,
    tasks: JoinSet<Result<(), Error>>,
    work_cancel: CancellationToken,
}

impl Supervisor {
    async fn run(mut self, ready: ReadySender) -> Result<(), Error> {
        let inner = Arc::clone(&self.inner);
        let (completion_sender, completion_receiver) = mpsc::channel(10_000);
        *inner
            .completion_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(completion_sender.downgrade());
        self.spawn_task(
            Service::Queues,
            run_dynamic_queues(
                Arc::clone(&inner),
                completion_sender,
                self.fetch_cancel.child_token(),
                self.work_cancel.child_token(),
                inner.queue_notifications.clone(),
                inner.queue_changes.subscribe(),
            ),
        );
        self.spawn_task(
            Service::Completer,
            run_completion_batcher(Arc::clone(&inner), completion_receiver),
        );
        if inner.poll_only {
            let _ = ready.send(Ok(()));
        } else {
            self.spawn_service(Service::Notifier, Duration::ZERO, Some(ready));
        }
        self.spawn_service(Service::Maintenance, Duration::ZERO, None);
        for index in 0..inner.pilot.runtime_services().len() {
            self.spawn_service(Service::Extension(index), Duration::ZERO, None);
        }

        let mut fatal = None;
        while let Some(joined) = self.tasks.join_next_with_id().await {
            let (task_id, outcome) = match joined {
                Ok((task_id, outcome)) => (task_id, outcome),
                Err(join_error) => (join_error.id(), Err(Error::from_join(join_error))),
            };
            let Some(service) = self.services.remove(&task_id) else {
                continue;
            };
            let stopping = self.fetch_cancel.is_cancelled();
            match outcome {
                // The completer ends once every producer has dropped its sender.
                Ok(()) if stopping || service == Service::Completer => {}
                Err(service_error) if stopping && !service.is_essential() => {
                    debug!(
                        service = service.name(),
                        error = %service_error,
                        "River service stopped with an error during shutdown"
                    );
                }
                outcome if service.is_essential() => {
                    let service_error = outcome.err().unwrap_or_else(|| {
                        Error::runtime_context(service.name(), "exited unexpectedly".to_owned())
                    });
                    error!(
                        service = service.name(),
                        error = %service_error,
                        "River service failed; stopping the client after in-flight work"
                    );
                    fatal.get_or_insert(service_error);
                    self.fetch_cancel.cancel();
                    self.work_cancel.cancel();
                }
                outcome => {
                    let attempt = self.restarts.entry(service).or_default();
                    *attempt += 1;
                    let delay = exponential_backoff(*attempt);
                    error!(
                        service = service.name(),
                        attempt = *attempt,
                        error = %outcome.err().map_or_else(|| "exited unexpectedly".to_owned(), |error| error.to_string()),
                        sleep_duration = ?delay,
                        "River service failed; restarting after backoff"
                    );
                    self.spawn_service(service, delay, None);
                }
            }
        }
        fatal.map_or(Ok(()), Err)
    }

    fn spawn_task<F>(&mut self, service: Service, task: F)
    where
        F: std::future::Future<Output = Result<(), Error>> + Send + 'static,
    {
        let handle = self.tasks.spawn(task);
        self.services.insert(handle.id(), service);
    }

    /// Starts a restartable service after `delay`, unless the client stops
    /// first.
    fn spawn_service(&mut self, service: Service, delay: Duration, ready: Option<ReadySender>) {
        let inner = Arc::clone(&self.inner);
        let cancel = self.fetch_cancel.child_token();
        let run: std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send>> =
            match service {
                Service::Maintenance => Box::pin(crate::maintenance::run_maintenance(
                    Arc::clone(&inner),
                    cancel.clone(),
                    inner.queue_notifications.subscribe(),
                )),
                Service::Notifier => match inner.database.kind() {
                    #[cfg(feature = "postgres")]
                    DatabaseKind::Postgres => Box::pin(run_notifications(
                        Arc::clone(&inner),
                        cancel.clone(),
                        inner.queue_notifications.clone(),
                        ready,
                    )),
                    #[cfg(feature = "sqlite")]
                    DatabaseKind::Sqlite => Box::pin(run_sqlite_notifications(
                        Arc::clone(&inner),
                        cancel.clone(),
                        inner.queue_notifications.clone(),
                        ready,
                    )),
                },
                Service::Extension(index) => {
                    let Some(runtime_service) =
                        inner.pilot.runtime_services().into_iter().nth(index)
                    else {
                        return;
                    };
                    let pool = inner.pilot_database_pool();
                    let database = inner.pilot_database_config();
                    let service_cancel = cancel.clone();
                    Box::pin(async move {
                        runtime_service
                            .run(pool, database, service_cancel)
                            .await
                            .map_err(|service_error| Error::Extension {
                                phase: "runtime service",
                                source: service_error,
                            })
                    })
                }
                Service::Completer | Service::Queues => {
                    unreachable!("essential services are started once")
                }
            };
        self.spawn_task(service, async move {
            if !delay.is_zero() {
                tokio::select! {
                    () = cancel.cancelled() => return Ok(()),
                    () = tokio::time::sleep(delay) => {}
                }
            }
            run.await
        });
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
