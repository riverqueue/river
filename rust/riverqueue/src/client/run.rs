//! Starting clients and observing their lifecycle.

#[allow(clippy::wildcard_imports)]
use super::*;

/// A boxed application shutdown signal awaited by a started client.
type ShutdownSignal = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

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
    ///
    /// A client can run once at a time. After it stops, it can be started
    /// again.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RuntimeUnavailable`] when called outside a Tokio
    /// runtime, a configuration error when the client has no queues, and a
    /// runtime error when the client is already running.
    pub fn start(&self) -> Result<RunHandle, Error> {
        self.start_inner(None)
    }

    /// Starts the client like [`Client::start`] and stops it gracefully once
    /// `signal` completes.
    ///
    /// Completing `signal` has the same effect as [`Stopper::stop`]: the
    /// client stops fetching jobs and lets running jobs finish, and the
    /// builder's `soft_stop_timeout` escalates to cancelling them when set.
    /// Hard stops remain available through [`RunHandle::stopper`] and
    /// [`RunHandle::shutdown_now`]. The client drops `signal` without
    /// awaiting it further once it stops for any other reason.
    ///
    /// This mirrors the graceful shutdown hooks of Tokio servers such as
    /// axum's `with_graceful_shutdown`, so one application signal can stop an
    /// HTTP server and River together. An application
    /// [`CancellationToken`] works as a signal through
    /// [`CancellationToken::cancelled_owned`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(client: riverqueue::Client) -> Result<(), riverqueue::Error> {
    /// let mut run = client.start_with_graceful_shutdown(async {
    ///     let _ = tokio::signal::ctrl_c().await;
    /// })?;
    /// // Returns after Ctrl-C once in-flight jobs have finished.
    /// run.wait().await
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Client::start`].
    pub fn start_with_graceful_shutdown<F>(&self, signal: F) -> Result<RunHandle, Error>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.start_inner(Some(Box::pin(signal)))
    }

    fn start_inner(&self, shutdown_signal: Option<ShutdownSignal>) -> Result<RunHandle, Error> {
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
        let stopper = Stopper {
            fetch_cancel: CancellationToken::new(),
            work_cancel: CancellationToken::new(),
        };
        let inner = Arc::clone(&self.inner);
        let (ready_sender, ready) = oneshot::channel();
        let supervisor = Supervisor {
            fetch_cancel: stopper.fetch_cancel.clone(),
            inner: Arc::clone(&inner),
            restarts: HashMap::new(),
            services: HashMap::new(),
            tasks: JoinSet::new(),
            work_cancel: stopper.work_cancel.clone(),
        };
        let join = runtime.spawn(async move {
            let result = supervisor.run(ready_sender, shutdown_signal).await;
            inner.started.store(false, Ordering::Release);
            result
        });
        Ok(RunHandle {
            join: Some(join),
            ready: Readiness::Pending(ready),
            stopper,
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
    async fn run(
        mut self,
        ready: ReadySender,
        shutdown_signal: Option<ShutdownSignal>,
    ) -> Result<(), Error> {
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

        let stop_watch = watch_stop(
            self.fetch_cancel.clone(),
            self.work_cancel.clone(),
            inner.soft_stop_timeout,
            shutdown_signal,
        );
        tokio::pin!(stop_watch);
        let mut stop_watch_done = false;
        let mut fatal = None;
        loop {
            let joined = tokio::select! {
                joined = self.tasks.join_next_with_id() => joined,
                () = &mut stop_watch, if !stop_watch_done => {
                    stop_watch_done = true;
                    continue;
                }
            };
            let Some(joined) = joined else {
                break;
            };
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

/// Requests a soft stop when `shutdown_signal` completes, then escalates any
/// soft stop to a hard stop after `soft_stop_timeout`.
///
/// The escalation belongs to the client rather than to a caller awaiting
/// [`RunHandle::shutdown`], so it applies however the stop was requested and
/// dropping a shutdown future never changes it. This matches Go's client,
/// which starts its soft stop timer when fetching stops.
async fn watch_stop(
    fetch_cancel: CancellationToken,
    work_cancel: CancellationToken,
    soft_stop_timeout: Option<Duration>,
    shutdown_signal: Option<ShutdownSignal>,
) {
    match shutdown_signal {
        Some(signal) => tokio::select! {
            () = fetch_cancel.cancelled() => {}
            () = signal => {
                tracing::info!("River client received its shutdown signal; stopping gracefully");
                fetch_cancel.cancel();
            }
        },
        None => fetch_cancel.cancelled().await,
    }
    let Some(timeout) = soft_stop_timeout else {
        return;
    };
    tokio::select! {
        () = work_cancel.cancelled() => {}
        () = tokio::time::sleep(timeout) => {
            warn!(
                soft_stop_timeout = ?timeout,
                "River client soft stop timed out; cancelling remaining jobs"
            );
            work_cancel.cancel();
        }
    }
}

/// Requests that a running client stop.
///
/// A stopper is a cheap, cloneable trigger obtained from
/// [`RunHandle::stopper`]. It lets any task, such as a signal handler, stop a
/// client while another task owns the [`RunHandle`] and awaits
/// [`RunHandle::wait`]. Its methods only request a stop and return
/// immediately; observe completion through the handle.
///
/// Requests are idempotent and ordered by severity: calling [`Stopper::stop`]
/// after [`Stopper::stop_now`] does not undo the hard stop, and requests made
/// after the client stopped do nothing. A stopper only affects the run it came
/// from, not a later restart of the same [`Client`].
///
/// # Examples
///
/// ```no_run
/// # async fn example(client: riverqueue::Client) -> Result<(), riverqueue::Error> {
/// let mut run = client.start()?;
/// let stopper = run.stopper();
/// tokio::spawn(async move {
///     let _ = tokio::signal::ctrl_c().await;
///     // Stop fetching and let running jobs finish.
///     stopper.stop();
///     let _ = tokio::signal::ctrl_c().await;
///     // A second Ctrl-C cancels jobs that are still running.
///     stopper.stop_now();
/// });
/// run.wait().await
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Stopper {
    fetch_cancel: CancellationToken,
    work_cancel: CancellationToken,
}

impl Stopper {
    /// Requests a soft stop, like Go's `Client.Stop`.
    ///
    /// The client stops fetching new jobs and lets running jobs finish before
    /// it stops. When the builder's `soft_stop_timeout` is set, jobs still
    /// running after that timeout are cancelled as if by
    /// [`Stopper::stop_now`].
    pub fn stop(&self) {
        self.fetch_cancel.cancel();
    }

    /// Requests a hard stop, like Go's `Client.StopAndCancel`.
    ///
    /// The client stops fetching new jobs and cancels the
    /// [`WorkContext::cancellation_token`] of every running job. The client
    /// still waits for workers to return: a job that returns promptly after
    /// cancellation is made available again without using up its attempt,
    /// and one that ignores cancellation for longer than the job stuck
    /// threshold is aborted. A job whose cancellation was requested with
    /// `job_cancel` is cancelled rather than made available.
    pub fn stop_now(&self) {
        self.fetch_cancel.cancel();
        self.work_cancel.cancel();
    }
}

/// Controls one running client instance.
///
/// [`RunHandle::wait`], [`RunHandle::shutdown`], and
/// [`RunHandle::shutdown_now`] take `&mut self`, can be called repeatedly, and
/// are cancel safe: dropping one of their futures, for example from
/// `tokio::time::timeout` or `tokio::select!`, leaves the client and the
/// handle as they were, apart from any stop the method already requested. To
/// stop the client from another task, obtain a [`Stopper`] with
/// [`RunHandle::stopper`] or start the client with
/// [`Client::start_with_graceful_shutdown`].
///
/// The client's result is reported to the first call that observes it
/// stopping; later calls return `Ok(())`.
///
/// Dropping the handle requests a hard stop, like [`Stopper::stop_now`], but
/// cannot wait for in-flight work to be recorded. Use [`RunHandle::shutdown`]
/// or [`RunHandle::shutdown_now`] when shutdown must finish before returning,
/// or [`RunHandle::detach`] to deliberately leave the client running.
///
/// # Examples
///
/// Stop gracefully, but cancel jobs that are still running after 30 seconds:
///
/// ```no_run
/// # async fn example(client: riverqueue::Client) -> Result<(), riverqueue::Error> {
/// use std::time::Duration;
///
/// let mut run = client.start()?;
/// // ... serve until the application stops ...
/// if tokio::time::timeout(Duration::from_secs(30), run.shutdown())
///     .await
///     .is_err()
/// {
///     run.shutdown_now().await?;
/// }
/// # Ok(())
/// # }
/// ```
///
/// [`Client`] also offers `soft_stop_timeout` on its builder, which performs
/// this escalation inside the client however the stop was requested.
#[must_use = "dropping the handle requests immediate client shutdown; call detach to run it independently"]
pub struct RunHandle {
    join: Option<tokio::task::JoinHandle<Result<(), Error>>>,
    ready: Readiness,
    stopper: Stopper,
}

/// Whether the client's notification path has become ready.
#[derive(Debug)]
enum Readiness {
    Failed(String),
    Pending(oneshot::Receiver<Result<(), String>>),
    Ready,
}

impl std::fmt::Debug for RunHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RunHandle")
            .field("running", &self.join.is_some())
            .field("ready", &matches!(self.ready, Readiness::Ready))
            .finish_non_exhaustive()
    }
}

impl RunHandle {
    /// Leaves the client running independently of this handle.
    ///
    /// This relinquishes waiting for the client: it runs until a [`Stopper`]
    /// obtained earlier from [`RunHandle::stopper`] or the signal passed to
    /// [`Client::start_with_graceful_shutdown`] stops it, an essential service
    /// fails, or the process exits. Nothing observes its result, and jobs
    /// running when the process exits are left `running` for the rescuer.
    /// Most applications should keep the handle and await
    /// [`RunHandle::shutdown`] instead.
    pub fn detach(mut self) {
        // Without a join handle, dropping the handle requests no stop.
        self.join.take();
    }

    /// Requests a soft stop and waits for the client to stop.
    ///
    /// This is [`Stopper::stop`] followed by [`RunHandle::wait`]. The stop is
    /// requested when the future is first polled.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. Dropping the future after its first poll
    /// leaves the soft stop in progress, including any `soft_stop_timeout`
    /// escalation, and never escalates to a hard stop by itself. The handle
    /// remains usable: call [`RunHandle::shutdown_now`] to cancel running jobs
    /// or [`RunHandle::wait`] to keep waiting.
    ///
    /// # Errors
    ///
    /// Returns the error that stopped the client, as [`RunHandle::wait`] does.
    pub async fn shutdown(&mut self) -> Result<(), Error> {
        self.stopper.stop();
        self.wait().await
    }

    /// Requests a hard stop and waits for the client to stop.
    ///
    /// This is [`Stopper::stop_now`] followed by [`RunHandle::wait`]. The stop
    /// is requested when the future is first polled.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. Dropping the future after its first poll
    /// leaves the hard stop in progress, and the handle remains usable.
    ///
    /// # Errors
    ///
    /// Returns the error that stopped the client, as [`RunHandle::wait`] does.
    pub async fn shutdown_now(&mut self) -> Result<(), Error> {
        self.stopper.stop_now();
        self.wait().await
    }

    /// Returns a [`Stopper`] that can stop this client from any task.
    pub fn stopper(&self) -> Stopper {
        self.stopper.clone()
    }

    /// Waits for the client to stop, without requesting a stop.
    ///
    /// Returns immediately when the client has already stopped and its
    /// result was reported by an earlier call, or when the handle was never
    /// attached to a running client.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. Dropping the future leaves the client
    /// running and the handle usable; a later call keeps waiting.
    ///
    /// # Errors
    ///
    /// Returns the error from an essential service, such as producers or the
    /// completer, whose failure stopped the client, or
    /// [`Error::RuntimeTask`] when the client's supervisor task panicked or was
    /// cancelled by its runtime shutting down.
    pub async fn wait(&mut self) -> Result<(), Error> {
        let Some(join) = self.join.as_mut() else {
            return Ok(());
        };
        let result = join.await;
        self.join = None;
        join_client_result(result)
    }

    /// Waits until the selected backend's notification path is active.
    ///
    /// Poll-only clients are ready immediately. Once readiness is observed,
    /// later calls return the same result immediately.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. Dropping the future leaves the client
    /// running, and a later call keeps waiting.
    ///
    /// # Errors
    ///
    /// Returns an error when the client stops before becoming ready, or when
    /// its notification path failed to start.
    pub async fn wait_ready(&mut self) -> Result<(), Error> {
        if let Readiness::Pending(receiver) = &mut self.ready {
            self.ready = match receiver.await {
                Ok(Ok(())) => Readiness::Ready,
                Ok(Err(message)) => Readiness::Failed(message),
                Err(_) => Readiness::Failed("client stopped before becoming ready".to_owned()),
            };
        }
        match &self.ready {
            Readiness::Failed(message) => Err(Error::runtime(message.clone())),
            Readiness::Pending(_) | Readiness::Ready => Ok(()),
        }
    }
}

impl Drop for RunHandle {
    fn drop(&mut self) {
        if self.join.is_some() {
            self.stopper.stop_now();
        }
    }
}

pub(super) fn join_client_result(
    result: Result<Result<(), Error>, tokio::task::JoinError>,
) -> Result<(), Error> {
    result.map_err(Error::from_join)??;
    Ok(())
}
