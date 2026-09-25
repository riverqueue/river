//! Leader election, a port of Go's `internal/leadership` elector.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
#[cfg(feature = "postgres")]
use sqlx::AssertSqlSafe;
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[cfg(feature = "sqlite")]
use crate::database::sqlite;
use crate::{Error, client::ClientInner};

use super::{LeadershipWakeup, exponential_backoff, random_duration, sleep_cancellable};

/// Lease padding added to the elect interval (Go `electIntervalTTLPaddingDefault`).
pub(crate) const LEADER_TTL_PADDING: Duration = Duration::from_secs(10);

/// Maximum duration of one election or renewal attempt (Go `deadlineTimeout`).
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Margin subtracted from the lease when deciding how long this client trusts
/// that it still leads (Go `leaderLocalDeadlineSafetyMargin`).
const LOCAL_DEADLINE_SAFETY_MARGIN: Duration = Duration::from_secs(1);

/// Go's `ElectIntervalJitter` default. Rust scales it down for short elect
/// intervals so that tests with millisecond intervals stay fast.
const ELECT_INTERVAL_JITTER_MAX: Duration = Duration::from_secs(1);

/// Upper bound on the random delay before bidding after another client
/// resigns, so that followers do not all bid at once.
const RESIGNED_WAKEUP_JITTER: Duration = Duration::from_millis(50);

/// Number of resignation attempts on step-down (Go `attemptResignLoop`).
const RESIGN_ATTEMPTS: u32 = 3;

/// Default exponential backoff reset (Go `MaxAttemptsBeforeResetDefault`).
const BACKOFF_RESET: u32 = 7;

/// A leadership term held by this client.
#[derive(Clone, Debug)]
pub(crate) struct Term {
    /// Database `elected_at` identifying this term.
    #[cfg_attr(
        not(all(test, feature = "postgres-tests")),
        expect(dead_code, reason = "observed by tests")
    )]
    pub(crate) elected_at: DateTime<Utc>,
    /// Cancelled the moment this client stops trusting the term.
    pub(crate) token: CancellationToken,
}

/// Local view of a held lease (Go `leadershipTerm`).
#[derive(Clone, Copy, Debug)]
struct Lease {
    elected_at: DateTime<Utc>,
    trusted_until: Instant,
}

impl Lease {
    /// Trusts the lease until `ttl - 1s` after the attempt *started*, so time
    /// spent waiting on a slow database never extends local trust.
    fn new(elected_at: DateTime<Utc>, attempt_started: Instant, ttl: Duration) -> Self {
        Self {
            elected_at,
            trusted_until: attempt_started + ttl.saturating_sub(LOCAL_DEADLINE_SAFETY_MARGIN),
        }
    }

    fn remaining(&self, now: Instant) -> Duration {
        self.trusted_until.saturating_duration_since(now)
    }

    fn reelect_attempt_timeout(&self, now: Instant) -> Duration {
        self.remaining(now).min(ATTEMPT_TIMEOUT)
    }
}

/// Database operations behind the elector, separated so tests can inject
/// slow or failing renewals.
#[async_trait]
pub(crate) trait LeaderStore: Send + Sync + 'static {
    /// Deletes an expired lease and inserts this client's lease if none
    /// exists (`ON CONFLICT DO NOTHING`). Returns the new term's `elected_at`.
    async fn elect(&self, ttl: Duration) -> Result<Option<DateTime<Utc>>, Error>;

    /// Extends the lease only when it still belongs to this client *and* term.
    /// Returns the renewed term's `elected_at`, or `None` if it was lost.
    async fn reelect(
        &self,
        elected_at: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<Option<DateTime<Utc>>, Error>;

    /// Deletes this client's lease for exactly this term and announces it.
    async fn resign(&self, elected_at: DateTime<Utc>) -> Result<bool, Error>;
}

/// Events observed by tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ElectorEvent {
    Denied,
    Gained,
    Lost,
    Maintained,
    Resigned,
}

/// Why the leader state ended.
enum StepDown {
    /// The client is shutting down; resign before exiting.
    Shutdown,
    /// Resign and return to the follower state.
    Resign { requested: bool },
    /// The lease is gone already; do not resign.
    Lost,
}

pub(crate) struct Elector {
    client_id: String,
    elect_interval: Duration,
    events: Option<mpsc::UnboundedSender<ElectorEvent>>,
    store: Arc<dyn LeaderStore>,
}

impl Elector {
    pub(crate) fn new(
        store: Arc<dyn LeaderStore>,
        client_id: String,
        elect_interval: Duration,
    ) -> Self {
        Self {
            client_id,
            elect_interval,
            events: None,
            store,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_events(mut self, events: mpsc::UnboundedSender<ElectorEvent>) -> Self {
        self.events = Some(events);
        self
    }

    fn ttl(&self) -> Duration {
        self.elect_interval + LEADER_TTL_PADDING
    }

    fn signal(&self, event: ElectorEvent) {
        if let Some(events) = &self.events {
            let _ = events.send(event);
        }
    }

    /// Runs the follower/leader state machine until `cancel` fires. Each
    /// gained term is published to the maintainer; its token is cancelled when
    /// the term ends.
    pub(crate) async fn run(
        mut self,
        cancel: CancellationToken,
        mut wakeups: mpsc::UnboundedReceiver<LeadershipWakeup>,
        terms: mpsc::UnboundedSender<Term>,
    ) {
        let mut cooldown = false;
        loop {
            let Some(lease) = self.run_follower(&cancel, &mut wakeups, cooldown).await else {
                return;
            };
            let term = Term {
                elected_at: lease.elected_at,
                token: cancel.child_token(),
            };
            debug!(client_id = %self.client_id, "River client gained leadership");
            self.signal(ElectorEvent::Gained);
            let _ = terms.send(term.clone());
            // Like Go, which honors a resignation request only when it
            // arrives while this client leads, drop requests that were
            // queued while it was bidding.
            if !drain_wakeups(&mut wakeups) {
                term.token.cancel();
                self.resign(lease.elected_at).await;
                return;
            }

            let (step_down, lease) = self.run_leader(&cancel, &mut wakeups, lease).await;
            term.token.cancel();
            cooldown = false;
            match step_down {
                StepDown::Shutdown => {
                    self.resign(lease.elected_at).await;
                    return;
                }
                StepDown::Resign { requested } => {
                    self.resign(lease.elected_at).await;
                    // Unlike Go, which bids again immediately, give peers one
                    // elect interval to take the lease after an explicit
                    // resignation request. Wakeups do not shorten it.
                    cooldown = requested;
                }
                StepDown::Lost => {}
            }
            if cancel.is_cancelled() {
                return;
            }
        }
    }

    /// Bids for leadership until elected or cancelled.
    async fn run_follower(
        &mut self,
        cancel: &CancellationToken,
        wakeups: &mut mpsc::UnboundedReceiver<LeadershipWakeup>,
        cooldown: bool,
    ) -> Option<Lease> {
        if cooldown
            && !self
                .sleep_ignoring_wakeups(cancel, wakeups, self.elect_interval)
                .await
        {
            return None;
        }
        let mut attempt = 0_u32;
        loop {
            attempt += 1;
            let attempt_started = Instant::now();
            let result = tokio::select! {
                biased;
                () = cancel.cancelled() => return None,
                result = tokio::time::timeout(ATTEMPT_TIMEOUT, self.store.elect(self.ttl())) => result,
            };
            match result {
                Ok(Ok(Some(elected_at))) => {
                    return Some(Lease::new(elected_at, attempt_started, self.ttl()));
                }
                Ok(Ok(None)) => {}
                Ok(Err(elect_error)) => {
                    let backoff = exponential_backoff(attempt, BACKOFF_RESET);
                    error!(error = %elect_error, attempt, ?backoff, "River leader election failed");
                    if !sleep_cancellable(cancel, backoff).await {
                        return None;
                    }
                    continue;
                }
                Err(_) => {
                    let backoff = exponential_backoff(attempt, BACKOFF_RESET);
                    error!(attempt, ?backoff, "River leader election timed out");
                    if !sleep_cancellable(cancel, backoff).await {
                        return None;
                    }
                    continue;
                }
            }

            attempt = 0;
            self.signal(ElectorEvent::Denied);
            let jitter_max = ELECT_INTERVAL_JITTER_MAX.min(self.elect_interval / 5);
            let wait = self.elect_interval + random_duration(Duration::ZERO, jitter_max);
            tokio::select! {
                biased;
                () = cancel.cancelled() => return None,
                () = tokio::time::sleep(wait) => {}
                wakeup = wakeups.recv() => match wakeup {
                    None => return None,
                    // A follower ignores resignation requests.
                    Some(LeadershipWakeup::RequestResign) => {}
                    Some(LeadershipWakeup::Changed) => {
                        // Somebody resigned; bid soon, but not all at once.
                        if !sleep_cancellable(
                            cancel,
                            random_duration(Duration::ZERO, RESIGNED_WAKEUP_JITTER),
                        )
                        .await
                        {
                            return None;
                        }
                    }
                },
            }
        }
    }

    /// Keeps renewing the lease until it is lost, its trust window elapses, a
    /// resignation is requested, or the client stops.
    async fn run_leader(
        &mut self,
        cancel: &CancellationToken,
        wakeups: &mut mpsc::UnboundedReceiver<LeadershipWakeup>,
        mut lease: Lease,
    ) -> (StepDown, Lease) {
        let mut wait = self.elect_interval;
        let mut errors = 0_u32;
        loop {
            let deadline = tokio::time::sleep(wait);
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return (StepDown::Shutdown, lease),
                    wakeup = wakeups.recv() => match wakeup {
                        None => return (StepDown::Shutdown, lease),
                        Some(LeadershipWakeup::RequestResign) => {
                            info!(client_id = %self.client_id, "River leader received a resignation request");
                            return (StepDown::Resign { requested: true }, lease);
                        }
                        Some(LeadershipWakeup::Changed) => {}
                    },
                    () = &mut deadline => break,
                }
            }

            let attempt_started = Instant::now();
            let attempt_timeout = lease.reelect_attempt_timeout(attempt_started);
            if attempt_timeout.is_zero() {
                warn!(
                    client_id = %self.client_id,
                    "River leader stepping down because its renewal deadline elapsed"
                );
                self.signal(ElectorEvent::Lost);
                return (StepDown::Resign { requested: false }, lease);
            }
            let result = tokio::select! {
                biased;
                () = cancel.cancelled() => return (StepDown::Shutdown, lease),
                result = tokio::time::timeout(
                    attempt_timeout,
                    self.store.reelect(lease.elected_at, self.ttl()),
                ) => result,
            };
            match result {
                Ok(Ok(Some(elected_at))) => {
                    errors = 0;
                    lease = Lease::new(elected_at, attempt_started, self.ttl());
                    self.signal(ElectorEvent::Maintained);
                    wait = self.elect_interval;
                }
                Ok(Ok(None)) => {
                    info!(client_id = %self.client_id, "River leader lost its lease");
                    self.signal(ElectorEvent::Lost);
                    return (StepDown::Lost, lease);
                }
                failure => {
                    errors += 1;
                    let remaining = lease.remaining(Instant::now());
                    if remaining.is_zero() {
                        warn!(
                            client_id = %self.client_id,
                            "River leader stepping down because its renewal deadline elapsed after an error"
                        );
                        self.signal(ElectorEvent::Lost);
                        return (StepDown::Resign { requested: false }, lease);
                    }
                    let backoff = exponential_backoff(errors, 3).min(remaining);
                    if let Ok(Err(renew_error)) = failure {
                        error!(error = %renew_error, attempt = errors, ?backoff, "River leader renewal failed");
                    } else {
                        error!(attempt = errors, ?backoff, "River leader renewal timed out");
                    }
                    if !sleep_cancellable(cancel, backoff).await {
                        return (StepDown::Shutdown, lease);
                    }
                    // Retry immediately because the failed attempt already
                    // consumed part of this lease's trust window.
                    wait = Duration::ZERO;
                }
            }
        }
    }

    /// Makes a bounded, good-faith attempt to give up the lease even during
    /// shutdown. The TTL is the backstop if every attempt fails.
    async fn resign(&self, elected_at: DateTime<Utc>) {
        for attempt in 1..=RESIGN_ATTEMPTS {
            let timeout = Duration::from_secs(u64::from(attempt));
            match tokio::time::timeout(timeout, self.store.resign(elected_at)).await {
                Ok(Ok(resigned)) => {
                    if resigned {
                        debug!(client_id = %self.client_id, "River leader resigned");
                        self.signal(ElectorEvent::Resigned);
                    }
                    return;
                }
                Ok(Err(resign_error)) => {
                    error!(error = %resign_error, attempt, "River leader resignation failed");
                }
                Err(_) => error!(attempt, "River leader resignation timed out"),
            }
            if attempt < RESIGN_ATTEMPTS {
                tokio::time::sleep(exponential_backoff(attempt, RESIGN_ATTEMPTS)).await;
            }
        }
    }

    /// Sleeps while draining wakeups, returning `false` on cancellation.
    async fn sleep_ignoring_wakeups(
        &self,
        cancel: &CancellationToken,
        wakeups: &mut mpsc::UnboundedReceiver<LeadershipWakeup>,
        duration: Duration,
    ) -> bool {
        let deadline = tokio::time::sleep(duration);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return false,
                () = &mut deadline => return true,
                wakeup = wakeups.recv() => if wakeup.is_none() {
                    return false;
                },
            }
        }
    }
}

/// Discards queued wakeups, returning `false` once the channel is closed.
fn drain_wakeups(wakeups: &mut mpsc::UnboundedReceiver<LeadershipWakeup>) -> bool {
    loop {
        match wakeups.try_recv() {
            Ok(_) => {}
            Err(mpsc::error::TryRecvError::Empty) => return true,
            Err(mpsc::error::TryRecvError::Disconnected) => return false,
        }
    }
}

/// Leader persistence for the client's configured backend.
pub(crate) struct DatabaseLeaderStore {
    inner: Arc<ClientInner>,
}

impl DatabaseLeaderStore {
    pub(crate) fn new(inner: Arc<ClientInner>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl LeaderStore for DatabaseLeaderStore {
    async fn elect(&self, ttl: Duration) -> Result<Option<DateTime<Utc>>, Error> {
        #[cfg(feature = "sqlite")]
        if let Some(pool) = self.inner.sqlite_pool() {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let now = Utc::now();
            sqlite::leader_delete_expired(&mut transaction, now)
                .await
                .map_err(sqlite_error)?;
            let leader = sqlite::leader_elect(&mut transaction, &self.inner.id, now, ttl)
                .await
                .map_err(sqlite_error)?;
            transaction.commit().await?;
            return Ok(leader.map(|leader| leader.elected_at));
        }
        #[cfg(feature = "postgres")]
        {
            let pool = self
                .inner
                .postgres_pool()
                .expect("client database is PostgreSQL or SQLite");
            let table = self.inner.schema.qualify("river_leader");
            let mut transaction = crate::database::begin_postgres(pool).await?;
            sqlx::query(AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE expires_at < now()"
            )))
            .execute(&mut *transaction)
            .await?;
            let elected_at = sqlx::query_scalar::<_, DateTime<Utc>>(AssertSqlSafe(format!(
                "INSERT INTO {table} (leader_id, elected_at, expires_at) \
                 VALUES ($1, now(), now() + make_interval(secs => $2)) \
                 ON CONFLICT (name) DO NOTHING RETURNING elected_at"
            )))
            .bind(&self.inner.id)
            .bind(ttl.as_secs_f64())
            .fetch_optional(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(elected_at);
        }
        #[allow(unreachable_code)]
        Err(no_backend())
    }

    async fn reelect(
        &self,
        elected_at: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<Option<DateTime<Utc>>, Error> {
        #[cfg(feature = "sqlite")]
        if let Some(pool) = self.inner.sqlite_pool() {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let leader = sqlite::leader_reelect(
                &mut transaction,
                &self.inner.id,
                elected_at,
                Utc::now(),
                ttl,
            )
            .await
            .map_err(sqlite_error)?;
            transaction.commit().await?;
            return Ok(leader.map(|leader| leader.elected_at));
        }
        #[cfg(feature = "postgres")]
        {
            let pool = self
                .inner
                .postgres_pool()
                .expect("client database is PostgreSQL or SQLite");
            let table = self.inner.schema.qualify("river_leader");
            return Ok(
                sqlx::query_scalar::<_, DateTime<Utc>>(AssertSqlSafe(format!(
                    "UPDATE {table} SET expires_at = now() + make_interval(secs => $1) \
                 WHERE elected_at = $2 AND expires_at >= now() AND leader_id = $3 \
                 RETURNING elected_at"
                )))
                .bind(ttl.as_secs_f64())
                .bind(elected_at)
                .bind(&self.inner.id)
                .fetch_optional(pool)
                .await?,
            );
        }
        #[allow(unreachable_code)]
        Err(no_backend())
    }

    async fn resign(&self, elected_at: DateTime<Utc>) -> Result<bool, Error> {
        #[cfg(feature = "sqlite")]
        if let Some(pool) = self.inner.sqlite_pool() {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let resigned = sqlite::leader_resign(&mut transaction, &self.inner.id, elected_at)
                .await
                .map_err(sqlite_error)?;
            if resigned {
                // Go's SQLite driver does not announce resignations, but a
                // durable outbox row lets polling peers bid promptly.
                let payload = serde_json::json!({
                    "action": "resigned",
                    "leader_id": self.inner.id,
                })
                .to_string();
                sqlite::notification_insert(
                    &mut transaction,
                    &[sqlite::NotificationInput {
                        payload: &payload,
                        topic: crate::NOTIFICATION_TOPIC_LEADERSHIP,
                    }],
                )
                .await
                .map_err(sqlite_error)?;
            }
            transaction.commit().await?;
            return Ok(resigned);
        }
        #[cfg(feature = "postgres")]
        {
            let pool = self
                .inner
                .postgres_pool()
                .expect("client database is PostgreSQL or SQLite");
            let table = self.inner.schema.qualify("river_leader");
            let result = sqlx::query(AssertSqlSafe(format!(
                "WITH currently_held_leaders AS (\
                    SELECT * FROM {table} WHERE elected_at = $1 AND leader_id = $2 FOR UPDATE\
                 ), notified_resignations AS (\
                    SELECT pg_notify(\
                        concat(coalesce($3::text, current_schema()), '.', $4::text), \
                        json_build_object('leader_id', leader_id, 'action', 'resigned')::text\
                    ) FROM currently_held_leaders\
                 ) \
                 DELETE FROM {table} USING notified_resignations"
            )))
            .bind(elected_at)
            .bind(&self.inner.id)
            .bind(self.inner.schema.as_deref())
            .bind(crate::NOTIFICATION_TOPIC_LEADERSHIP)
            .execute(pool)
            .await?;
            return Ok(result.rows_affected() > 0);
        }
        #[allow(unreachable_code)]
        Err(no_backend())
    }
}

#[cfg(feature = "sqlite")]
fn sqlite_error(error: sqlite::BackendError) -> Error {
    Error::Database(Box::new(error))
}

#[allow(dead_code, reason = "only reachable when no backend feature matches")]
fn no_backend() -> Error {
    Error::runtime_context(
        "maintenance",
        "database dispatch selected no supported backend".to_owned(),
    )
}

#[cfg(test)]
mod unit_tests {
    use std::sync::Mutex;

    use super::*;

    /// A lease store whose renewals can be slowed or failed.
    struct ScriptedStore {
        elected_at: DateTime<Utc>,
        reelect: Mutex<Vec<Reelect>>,
        resigned: Mutex<Vec<DateTime<Utc>>>,
    }

    enum Reelect {
        /// Succeeds only after local time passes the whole TTL.
        SlowSuccess(Duration),
        /// Fails after local time passes the whole TTL.
        SlowFailure(Duration),
    }

    #[async_trait]
    impl LeaderStore for ScriptedStore {
        async fn elect(&self, _ttl: Duration) -> Result<Option<DateTime<Utc>>, Error> {
            Ok(Some(self.elected_at))
        }

        async fn reelect(
            &self,
            elected_at: DateTime<Utc>,
            _ttl: Duration,
        ) -> Result<Option<DateTime<Utc>>, Error> {
            let step = self.reelect.lock().unwrap().pop();
            match step {
                Some(Reelect::SlowSuccess(delay)) => {
                    tokio::time::advance(delay).await;
                    Ok(Some(elected_at))
                }
                Some(Reelect::SlowFailure(delay)) => {
                    tokio::time::advance(delay).await;
                    Err(Error::runtime("renewal failed".to_owned()))
                }
                // Later terms only need to stay alive until the test stops.
                None => std::future::pending().await,
            }
        }

        async fn resign(&self, elected_at: DateTime<Utc>) -> Result<bool, Error> {
            self.resigned.lock().unwrap().push(elected_at);
            Ok(true)
        }
    }

    async fn run_scripted(
        reelect: Reelect,
    ) -> (Vec<ElectorEvent>, Vec<DateTime<Utc>>, DateTime<Utc>) {
        let elect_interval = Duration::from_millis(100);
        let elected_at = Utc::now();
        let store = Arc::new(ScriptedStore {
            elected_at,
            reelect: Mutex::new(vec![reelect]),
            resigned: Mutex::new(Vec::new()),
        });
        let (events_sender, mut events) = mpsc::unbounded_channel();
        let (_wakeup_sender, wakeups) = mpsc::unbounded_channel();
        let (terms_sender, mut terms) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let elector = Elector::new(
            Arc::clone(&store) as Arc<dyn LeaderStore>,
            "scripted".to_owned(),
            elect_interval,
        )
        .with_events(events_sender);
        let run = tokio::spawn(elector.run(cancel.clone(), wakeups, terms_sender));

        let term = terms.recv().await.unwrap();
        // Wait for the term to end, then stop before the next election.
        term.token.cancelled().await;
        let mut observed = Vec::new();
        while let Some(event) = events.recv().await {
            observed.push(event);
            if event == ElectorEvent::Resigned {
                break;
            }
        }
        cancel.cancel();
        run.await.unwrap();
        let resigned = store.resigned.lock().unwrap().clone();
        (observed, resigned, elected_at)
    }

    #[tokio::test(start_paused = true)]
    async fn ignores_resign_requests_queued_before_gaining_leadership() {
        let elected_at = Utc::now();
        let store = Arc::new(ScriptedStore {
            elected_at,
            reelect: Mutex::new(Vec::new()),
            resigned: Mutex::new(Vec::new()),
        });
        let (wakeup_sender, wakeups) = mpsc::unbounded_channel();
        let (terms_sender, mut terms) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        // Queued while the client is still a follower, as when a request
        // arrives during its election attempt.
        wakeup_sender.send(LeadershipWakeup::RequestResign).unwrap();
        let elector = Elector::new(
            Arc::clone(&store) as Arc<dyn LeaderStore>,
            "stale-resign".to_owned(),
            Duration::from_millis(100),
        );
        let run = tokio::spawn(elector.run(cancel.clone(), wakeups, terms_sender));

        let term = terms.recv().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!term.token.is_cancelled());
        assert!(store.resigned.lock().unwrap().is_empty());

        // A request that arrives while the client leads is honored.
        wakeup_sender.send(LeadershipWakeup::RequestResign).unwrap();
        term.token.cancelled().await;
        cancel.cancel();
        run.await.unwrap();
        assert_eq!(store.resigned.lock().unwrap().first(), Some(&elected_at));
    }

    #[tokio::test(start_paused = true)]
    async fn slow_successful_reelect_does_not_extend_trust_window() {
        let ttl = Duration::from_millis(100) + LEADER_TTL_PADDING;
        let (events, resigned, elected_at) = run_scripted(Reelect::SlowSuccess(ttl)).await;
        // The renewal succeeded, but it started a full TTL ago, so the next
        // renewal is never attempted and the term is resigned instead.
        assert_eq!(
            events,
            [
                ElectorEvent::Gained,
                ElectorEvent::Maintained,
                ElectorEvent::Lost,
                ElectorEvent::Resigned
            ]
        );
        assert_eq!(resigned.first(), Some(&elected_at));
    }

    #[tokio::test(start_paused = true)]
    async fn resigns_current_term_after_reelect_errors_exhaust_trust() {
        let ttl = Duration::from_millis(100) + LEADER_TTL_PADDING;
        let (events, resigned, elected_at) = run_scripted(Reelect::SlowFailure(ttl)).await;
        assert_eq!(
            events,
            [
                ElectorEvent::Gained,
                ElectorEvent::Lost,
                ElectorEvent::Resigned
            ]
        );
        assert_eq!(resigned.first(), Some(&elected_at));
    }

    #[test]
    fn lease_trust_is_measured_from_attempt_start() {
        let started = Instant::now();
        let lease = Lease::new(Utc::now(), started, Duration::from_secs(15));
        assert_eq!(lease.remaining(started), Duration::from_secs(14));
        assert_eq!(lease.reelect_attempt_timeout(started), ATTEMPT_TIMEOUT);
        assert_eq!(
            lease.reelect_attempt_timeout(started + Duration::from_secs(12)),
            Duration::from_secs(2)
        );
        assert!(
            lease
                .reelect_attempt_timeout(started + Duration::from_secs(20))
                .is_zero()
        );
    }
}
