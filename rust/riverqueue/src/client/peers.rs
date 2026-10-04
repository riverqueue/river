//! Peer attempts: jobs a running attempt, their coordinator, claims and
//! completes alongside its own job, such as a group of related jobs it works
//! together.
//!
//! River owns each peer from the commit of the claim that took it until its
//! outcome persists, under the attempt that claimed it. A peer moves from
//! `Claimed` to `Preparing` once an outcome for it is accepted, to
//! `Submitted` once the completer accepts that outcome, and to `Settled` once
//! the completer persisted it; an outcome that fails before the completer
//! accepts it returns the peer to `Claimed`. When the coordinator ends,
//! River stops accepting its peer operations, waits for those it accepted,
//! and completes every peer still `Claimed` with an outcome of its own, all
//! before the coordinator's own outcome. Peers don't take producer slots, and
//! their producer's session never hears about them, like the other jobs of a
//! multi-job result in River for Go.
//!
//! A soft stop doesn't end a coordinator's claims: the producer stops
//! fetching new jobs, but a running coordinator may keep claiming peers until
//! its attempt ends, so it can finish gathering and work the group it has.
//! The stop waits for that attempt, which settles every peer before its own
//! outcome, so the stop waits for the peers too. Only the attempt's
//! cancellation, by a hard stop or a remote cancellation, and the attempt's
//! end refuse claims.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use serde_json::{Map, Value};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::__private::{ClaimedJob, PilotDatabase, PilotError};
use crate::client::ClientInner;
use crate::client::completer::{CompletionAttempt, CompletionTiming, CompletionUpdate};
use crate::client::executor::{
    WorkerFailure, WorkerFailureKind, WorkerResult, persist_result, public_work_result,
    worker_failure_from_source,
};
use crate::{Client, Error, ErrorHandlerDecision, JobRow, JobState, WorkContext, WorkResult};

/// Numbers ledgers so the client-wide owner map can tell them apart.
static LEDGER_IDS: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PeerState {
    Claimed,
    Preparing,
    Submitted,
    Settled,
}

struct Peer {
    /// The row as claimed, which identifies the peer's attempt.
    claimed: JobRow,
    state: PeerState,
}

#[derive(Default)]
struct LedgerState {
    /// Set once the coordinator ended; no new operation starts.
    closed: bool,
    /// Set once River stopped tracking the peers; see `abandon`.
    released: bool,
    /// Operations accepted and not yet settled.
    operations: usize,
    /// Peers by job ID, including settled ones.
    peers: HashMap<i64, Peer>,
}

/// The peers of one coordinating attempt.
pub(crate) struct PeerLedger {
    /// The coordinator's job ID.
    coordinator: i64,
    /// When the coordinator's attempt started, recorded as its peers'
    /// attempt errors' time.
    started_at: DateTime<Utc>,
    id: u64,
    idle: Notify,
    state: Mutex<LedgerState>,
}

impl std::fmt::Debug for PeerLedger {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PeerLedger")
            .field("coordinator", &self.coordinator)
            .finish_non_exhaustive()
    }
}

/// Ends an accepted operation, waking a coordinator waiting to finish.
struct Operation<'a>(&'a PeerLedger);

impl Drop for Operation<'_> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.operations -= 1;
        if state.operations == 0 {
            self.0.idle.notify_waiters();
        }
    }
}

/// Peer jobs a claim reserved in the client's owner map, released when the
/// guard drops unless the claim recorded them as peers.
struct ReservedOwners<'a> {
    ids: Vec<i64>,
    inner: &'a ClientInner,
    ledger: u64,
}

impl Drop for ReservedOwners<'_> {
    fn drop(&mut self) {
        for id in &self.ids {
            self.inner.release_peer(*id, self.ledger);
        }
    }
}

/// Peers whose outcomes a submission is handing to the completer. When the
/// guard drops, a peer still `Preparing`, whose outcome never reached the
/// completer because the operation's future was dropped, returns to
/// `Claimed`, so its coordinator's end still gives it an outcome.
struct Handover<'a> {
    ids: Vec<i64>,
    ledger: &'a PeerLedger,
}

impl Drop for Handover<'_> {
    fn drop(&mut self) {
        for id in &self.ids {
            self.ledger
                .set_state(*id, PeerState::Claimed, PeerState::Preparing);
        }
    }
}

/// Tells a peer's ledger that the completer persisted its outcome.
pub(super) struct PeerCompletion {
    done: Mutex<Option<oneshot::Sender<()>>>,
    job_id: i64,
    ledger: Arc<PeerLedger>,
}

impl PeerCompletion {
    /// Ends ownership as the outcome persists, before its event, so the job
    /// can be claimed again at once.
    pub(super) fn persisted(&self, inner: &ClientInner) {
        if let Some(peer) = self.ledger.lock().peers.get_mut(&self.job_id) {
            peer.state = PeerState::Settled;
        }
        inner.release_peer(self.job_id, self.ledger.id);
        if let Some(done) = self
            .done
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = done.send(());
        }
    }
}

/// An outcome for one peer and the peer it's for.
struct Submission {
    job: JobRow,
    result: WorkerResult,
}

fn peer_error(message: impl Into<String>) -> Error {
    Error::Extension {
        phase: crate::ExtensionPhase::AddOn {
            operation: "peer attempts",
        },
        source: message.into().into(),
    }
}

fn not_running() -> Error {
    peer_error("peer operations require a running attempt")
}

impl PeerLedger {
    pub(super) fn new(coordinator: i64, started_at: DateTime<Utc>) -> Self {
        Self {
            coordinator,
            started_at,
            id: LEDGER_IDS.fetch_add(1, Ordering::Relaxed),
            idle: Notify::new(),
            state: Mutex::new(LedgerState::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LedgerState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Accepts an operation unless the coordinator ended.
    fn begin(&self) -> Result<Operation<'_>, Error> {
        let mut state = self.lock();
        if state.closed {
            return Err(not_running());
        }
        state.operations += 1;
        Ok(Operation(self))
    }

    /// Claims peers with `run` in a transaction River commits, and returns
    /// the decoded rows River now tracks. Rows that couldn't be decoded are
    /// completed as failures instead. Claims continue through a soft stop and
    /// end with the attempt's cancellation or its end.
    pub(crate) async fn claim<F>(
        self: &Arc<Self>,
        inner: &Arc<ClientInner>,
        context: &WorkContext,
        run: F,
    ) -> Result<Vec<JobRow>, Error>
    where
        F: for<'c> FnOnce(
                crate::__private::PeerClaimContext<'c>,
            ) -> BoxFuture<'c, Result<Vec<ClaimedJob>, PilotError>>
            + Send,
    {
        let _operation = self.begin()?;
        let cancellation = context.cancellation_token();
        if cancellation.is_cancelled() {
            return Err(peer_error("peer claim cancelled"));
        }
        let database: PilotDatabase = inner.pilot_database();
        let mut transaction = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(peer_error("peer claim cancelled")),
            transaction = database.begin() => transaction?,
        };
        let claimed = run(crate::__private::PeerClaimContext {
            cancellation,
            client_id: &inner.id,
            connection: transaction.connection(),
            database: &database,
        })
        .await
        .map_err(|source| Error::Extension {
            phase: crate::ExtensionPhase::AddOn {
                operation: "peer attempts",
            },
            source,
        })?;
        // A claim whose coordinator was cancelled before commit rolls back.
        if cancellation.is_cancelled() {
            return Err(peer_error("peer claim cancelled"));
        }
        // Released again unless the rows become the coordinator's peers,
        // including when this future is dropped during the commit.
        let mut reserved = ReservedOwners {
            ids: self.reserve_claim(inner, &claimed)?,
            inner,
            ledger: self.id,
        };
        transaction.commit().await?;
        // The claim committed: every row is this coordinator's peer now,
        // even when the coordinator was cancelled meanwhile, unless River
        // stopped tracking its peers while the claim ran, which leaves the
        // rows to the rescuer. Checking and recording under one lock keeps
        // `abandon` from running in between.
        let mut rows = Vec::new();
        let mut failures = Vec::new();
        {
            let mut state = self.lock();
            if state.released {
                return Err(not_running());
            }
            reserved.ids.clear();
            for claimed in claimed {
                let decode_error = claimed.decode_error().map(str::to_owned);
                let Some(row) = claimed
                    .into_decoded()
                    .map_or_else(|undecodable| undecodable.row.map(|row| *row), Some)
                else {
                    continue;
                };
                state.peers.insert(
                    row.id,
                    Peer {
                        claimed: row.clone(),
                        state: if decode_error.is_some() {
                            PeerState::Preparing
                        } else {
                            PeerState::Claimed
                        },
                    },
                );
                match decode_error {
                    Some(error) => failures.push(Submission {
                        job: row,
                        result: Err(worker_failure_from_source(
                            format!("job row couldn't be decoded: {error}").into(),
                        )),
                    }),
                    None => rows.push(row),
                }
            }
        }
        if !failures.is_empty() {
            self.submit(inner, context, failures).await?;
        }
        Ok(rows)
    }

    /// Checks a claim's rows before it commits, and reserves them so no other
    /// claim of this client can take them meanwhile.
    fn reserve_claim(
        &self,
        inner: &ClientInner,
        claimed: &[ClaimedJob],
    ) -> Result<Vec<i64>, Error> {
        let fail = |reason: String| peer_error(format!("a peer claim returned {reason}"));
        let state = self.lock();
        let running = inner
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut owners = inner.lock_peer_owners();
        let mut seen = std::collections::HashSet::new();
        for job in claimed {
            let Some(row) = job.row() else {
                return Err(fail(format!(
                    "a row that couldn't be identified: {}",
                    job.decode_error().unwrap_or_default()
                )));
            };
            let id = row.id;
            if !seen.insert(id) {
                return Err(fail(format!("job {id} twice")));
            }
            if id == self.coordinator {
                return Err(fail(format!("job {id}, the claiming attempt's own job")));
            }
            if owners.contains_key(&id) {
                return Err(fail(format!(
                    "job {id}, which this client already works as a peer"
                )));
            }
            if running.contains_key(&id) {
                return Err(fail(format!("job {id}, which this client already works")));
            }
            if row.attempt < 1 {
                return Err(fail(format!("job {id}, which has no attempt")));
            }
            if let Some(earlier) = state.peers.get(&id)
                && row.attempt <= earlier.claimed.attempt
            {
                return Err(fail(format!(
                    "job {id} at attempt {}, which already ended here",
                    row.attempt
                )));
            }
            if row.state != JobState::Running {
                return Err(fail(format!("job {id}, which isn't running")));
            }
            let attempted_by_undecodable = job.column_undecodable("attempted_by");
            if !attempted_by_undecodable
                && row.attempted_by.last().map(String::as_str) != Some(inner.id.as_str())
            {
                return Err(fail(format!("job {id}, which another client claimed")));
            }
        }
        let reserved = seen.into_iter().collect::<Vec<_>>();
        for id in &reserved {
            owners.insert(*id, self.id);
        }
        Ok(reserved)
    }

    /// Completes peers through the ordinary completion pipeline, returning
    /// once every outcome persisted.
    pub(crate) async fn complete(
        self: &Arc<Self>,
        inner: &Arc<ClientInner>,
        context: &WorkContext,
        outcomes: Vec<crate::__private::PeerOutcome>,
    ) -> Result<(), Error> {
        let _operation = self.begin()?;
        let submissions = self.reserve_outcomes(outcomes)?;
        self.submit(inner, context, submissions).await
    }

    /// Accepts one outcome for each peer, all or none: each job must be a
    /// peer of this ledger, identified by its ID, attempt, and attempting
    /// client, have no outcome yet, and appear once.
    fn reserve_outcomes(
        &self,
        outcomes: Vec<crate::__private::PeerOutcome>,
    ) -> Result<Vec<Submission>, Error> {
        let mut state = self.lock();
        let mut seen = std::collections::HashSet::new();
        for outcome in &outcomes {
            let id = outcome.job.id;
            let Some(peer) = state.peers.get(&id) else {
                return Err(peer_error(format!(
                    "job {id} isn't a peer of the attempt completing it"
                )));
            };
            if outcome.job.attempt != peer.claimed.attempt
                || outcome.job.attempted_by.last() != peer.claimed.attempted_by.last()
            {
                return Err(peer_error(format!(
                    "job {id} attempt {} isn't the peer attempt {} this attempt owns",
                    outcome.job.attempt, peer.claimed.attempt
                )));
            }
            if !seen.insert(id) {
                return Err(peer_error(format!("job {id} has two outcomes")));
            }
            if peer.state != PeerState::Claimed {
                return Err(peer_error(format!("job {id} already has an outcome")));
            }
        }
        let mut submissions = Vec::with_capacity(outcomes.len());
        for outcome in outcomes {
            let peer = state
                .peers
                .get_mut(&outcome.job.id)
                .expect("peer checked above");
            peer.state = PeerState::Preparing;
            submissions.push(Submission {
                job: peer.claimed.clone(),
                result: outcome.result.map_err(worker_failure_from_source),
            });
        }
        Ok(submissions)
    }

    /// Hands outcomes to the completer, running the error handler and adding
    /// the coordinator's metadata first, like the coordinator's own outcome,
    /// and waits for them to persist. An outcome the completer doesn't
    /// accept, including one whose submission is dropped first, returns its
    /// peer to `Claimed`.
    async fn submit(
        self: &Arc<Self>,
        inner: &Arc<ClientInner>,
        context: &WorkContext,
        submissions: Vec<Submission>,
    ) -> Result<(), Error> {
        let _handover = Handover {
            ids: submissions
                .iter()
                .map(|submission| submission.job.id)
                .collect(),
            ledger: self,
        };
        let sender = inner.completion_sender();
        let shared_metadata = context.metadata_updates();
        let mut persisting = Vec::with_capacity(submissions.len());
        let mut failure = None;
        for submission in submissions {
            let id = submission.job.id;
            let (done, persisted) = oneshot::channel();
            let completion = Arc::new(PeerCompletion {
                done: Mutex::new(Some(done)),
                job_id: id,
                ledger: Arc::clone(self),
            });
            let submitted = match &sender {
                Some(sender) => {
                    self.persist(
                        inner,
                        context,
                        submission,
                        &shared_metadata,
                        sender,
                        completion,
                    )
                    .await
                }
                None => Err(Error::runtime_context(
                    "job completion",
                    "client runtime is not accepting job completions",
                )),
            };
            // No await separates the completer accepting the outcome from
            // this state change, so a dropped submission never mistakes an
            // accepted outcome for a missing one.
            match submitted {
                Ok(()) => {
                    self.set_state(id, PeerState::Submitted, PeerState::Preparing);
                    persisting.push((id, persisted));
                }
                Err(submit_error) => {
                    self.set_state(id, PeerState::Claimed, PeerState::Preparing);
                    failure.get_or_insert(submit_error);
                }
            }
        }
        for (id, persisted) in persisting {
            if persisted.await.is_err() {
                // The completer gave the outcome up; the peer stays owned
                // until its coordinator ends and is left to the rescuer.
                failure.get_or_insert_with(|| {
                    Error::runtime_context(
                        "job completion",
                        format!("the outcome of peer job {id} was not persisted"),
                    )
                });
            }
        }
        failure.map_or(Ok(()), Err)
    }

    fn set_state(&self, id: i64, to: PeerState, from: PeerState) {
        if let Some(peer) = self.lock().peers.get_mut(&id)
            && peer.state == from
        {
            peer.state = to;
        }
    }

    async fn persist(
        &self,
        inner: &Arc<ClientInner>,
        coordinator: &WorkContext,
        Submission { job: row, result }: Submission,
        shared_metadata: &Map<String, Value>,
        sender: &mpsc::Sender<CompletionUpdate>,
        completion: Arc<PeerCompletion>,
    ) -> Result<(), Error> {
        let context = WorkContext::for_job(
            Client {
                inner: Arc::clone(inner),
            },
            coordinator.cancellation_token().clone(),
            row.id,
            &row.metadata,
        );
        for (key, value) in shared_metadata {
            context.insert_metadata(key.clone(), value.clone());
        }
        let work_result = public_work_result(&result);
        let mut decision = ErrorHandlerDecision::default();
        // Like a worker's result, only a failed outcome runs the error
        // handler: completions, snoozes, cancellations, discards, and
        // interruptions don't.
        if let Some(error_handler) = &inner.error_handler
            && matches!(work_result, WorkResult::Failed(_))
        {
            match error_handler
                .handle_error(&context, &row, &work_result)
                .await
            {
                Ok(handler_decision) => decision = handler_decision,
                Err(handler_error) => {
                    error!(error = %crate::error::Chain(&handler_error), "River error handler failed");
                }
            }
        }
        let queue_wait_duration = row
            .attempted_at
            .and_then(|attempted_at| {
                (attempted_at - row.scheduled_at.max(row.created_at))
                    .to_std()
                    .ok()
            })
            .unwrap_or_default();
        persist_result(
            inner,
            &row,
            self.started_at,
            &CompletionAttempt {
                cancellation: CancellationToken::new(),
                timing: CompletionTiming {
                    completion_started: std::time::Instant::now(),
                    queue_wait_duration,
                    run_duration: Duration::ZERO,
                },
            },
            result,
            context.metadata_updates(),
            decision,
            true,
            sender,
            Some(completion),
        )
        .await
    }

    /// Ends the coordinator's peers once its attempt ended: refuses new peer
    /// operations, waits for the accepted ones, then completes each peer still
    /// without an outcome, as interrupted when River stopped the coordinator
    /// and as failed otherwise.
    pub(super) async fn finish(
        self: &Arc<Self>,
        inner: &Arc<ClientInner>,
        context: &WorkContext,
        interrupted: bool,
    ) {
        self.lock().closed = true;
        loop {
            let idle = self.idle.notified();
            if self.lock().operations == 0 {
                break;
            }
            idle.await;
        }
        let missing = {
            let mut state = self.lock();
            state
                .peers
                .values_mut()
                .filter(|peer| peer.state == PeerState::Claimed)
                .map(|peer| {
                    peer.state = PeerState::Preparing;
                    peer.claimed.clone()
                })
                .collect::<Vec<_>>()
        };
        if !missing.is_empty() {
            let count = missing.len();
            let submissions = missing
                .into_iter()
                .map(|job| Submission {
                    job,
                    result: Err(if interrupted {
                        WorkerFailure {
                            error: "job interrupted by client shutdown".to_owned(),
                            kind: WorkerFailureKind::Interrupted,
                            source: None,
                            trace: String::new(),
                        }
                    } else {
                        worker_failure_from_source(
                            format!(
                                "the attempt of job {} ended without an outcome for this job",
                                self.coordinator
                            )
                            .into(),
                        )
                    }),
                })
                .collect();
            if let Err(submit_error) = self.submit(inner, context, submissions).await {
                error!(
                    job_id = self.coordinator,
                    peers = count,
                    error = %crate::error::Chain(&submit_error),
                    "River failed to complete peers their attempt left without an outcome"
                );
            }
        }
        self.abandon(inner);
    }

    /// Stops tracking the coordinator's peers, leaving any without a
    /// persisted outcome to the rescuer. For an attempt that ends without
    /// [`finish`](Self::finish).
    pub(super) fn abandon(&self, inner: &ClientInner) {
        let peers = {
            let mut state = self.lock();
            state.closed = true;
            state.released = true;
            state.peers.keys().copied().collect::<Vec<_>>()
        };
        for id in peers {
            inner.release_peer(id, self.id);
        }
    }
}

impl ClientInner {
    fn lock_peer_owners(&self) -> std::sync::MutexGuard<'_, HashMap<i64, u64>> {
        self.peer_owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Ends `ledger`'s ownership of peer job `id`, if it still owns it.
    fn release_peer(&self, id: i64, ledger: u64) {
        let mut owners = self.lock_peer_owners();
        if owners.get(&id) == Some(&ledger) {
            owners.remove(&id);
        }
    }

    /// The completer's sender, while the client runs.
    fn completion_sender(&self) -> Option<mpsc::Sender<CompletionUpdate>> {
        self.completion_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(mpsc::WeakSender::upgrade)
    }
}
