//! Peer attempts: jobs a running attempt claims and completes alongside its
//! own, owned by River from the claim's commit until each outcome persists.
//!
//! Every scenario uses generic claim statements written here, on PostgreSQL
//! (in a unique schema, failing rather than skipping when
//! `RIVER_RUST_DATABASE_URL` is unset) and SQLite (in a temporary file).

#![cfg(any(all(feature = "postgres", river_postgres_tests), feature = "sqlite"))]

mod support;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures_util::future::BoxFuture;
use riverqueue::__private::{
    ClaimedJob, ClientBuilderExt, DatabaseConnection, JobSetStateParams, PeerAttempts,
    PeerClaimContext, PeerOutcome, Pilot, PilotError, PilotProducer, ProducerStartContext,
};
use riverqueue::{
    BoxError, Client, ErrorHandler, ErrorHandlerDecision, Event, EventKind, InsertOpts, Job,
    JobArgs, JobRow, JobState, QueueConfig, WorkContext, WorkOutcome, WorkResult, WorkerRegistry,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};

const WAIT: Duration = Duration::from_secs(10);

/// The client ID every scenario's client uses.
const PEER_CLIENT: &str = "peer-client";

/// A job this client works as an ordinary attempt, held until released.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "peer_busy")]
struct BusyArgs {}

/// The job whose attempt coordinates peers, running a scenario's script.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "peer_coordinator")]
struct CoordinatorArgs {}

/// A peer job. Peers go to a queue no client works, so only claims take
/// them.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "peer_job")]
struct PeerArgs {
    value: i64,
}

/// Runs raw statements on either backend.
#[derive(Clone)]
enum Db {
    #[cfg(all(feature = "postgres", river_postgres_tests))]
    /// The pool, the qualified job table, and the qualified name for a
    /// test function.
    Postgres(sqlx::PgPool, String, String),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool),
}

impl Db {
    /// Runs `postgres` (with `{table}` for the job table) or `sqlite` on job
    /// `id`, bound as the only parameter.
    async fn exec(&self, postgres: &str, sqlite: &str, id: i64) {
        let _ = (postgres, sqlite);
        match self {
            #[cfg(all(feature = "postgres", river_postgres_tests))]
            Self::Postgres(pool, table, _) => {
                sqlx::query(sqlx::AssertSqlSafe(postgres.replace("{table}", table)))
                    .bind(id)
                    .execute(pool)
                    .await
                    .unwrap();
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool) => {
                sqlx::query(sqlx::AssertSqlSafe(sqlite.to_owned()))
                    .bind(id)
                    .execute(pool)
                    .await
                    .unwrap();
            }
        }
    }

    /// Runs statements that take no parameters.
    async fn raw(&self, postgres: &str, sqlite: &str) {
        let _ = (postgres, sqlite);
        match self {
            #[cfg(all(feature = "postgres", river_postgres_tests))]
            Self::Postgres(pool, table, function) => {
                sqlx::raw_sql(sqlx::AssertSqlSafe(
                    postgres
                        .replace("{table}", table)
                        .replace("{function}", function),
                ))
                .execute(pool)
                .await
                .unwrap();
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool) => {
                sqlx::raw_sql(sqlx::AssertSqlSafe(sqlite.to_owned()))
                    .execute(pool)
                    .await
                    .unwrap();
            }
        }
    }

    /// Makes every commit that leaves a peer job running fail.
    async fn fail_peer_commits(&self) {
        self.raw(
            "CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN IF NEW.state = 'running' AND NEW.kind = 'peer_job' THEN \
             RAISE EXCEPTION 'peer commit failed on purpose'; END IF; RETURN NULL; END $$; \
             CREATE CONSTRAINT TRIGGER fail_peer_commit AFTER UPDATE ON {table} \
             DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {function}();",
            "CREATE TABLE guard_parent (id INTEGER PRIMARY KEY); \
             CREATE TABLE peer_guard (parent_id INTEGER REFERENCES guard_parent (id) \
                 DEFERRABLE INITIALLY DEFERRED); \
             CREATE TRIGGER fail_peer_commit AFTER UPDATE OF state ON river_job \
             WHEN NEW.state = 'running' AND NEW.kind = 'peer_job' \
             BEGIN INSERT INTO peer_guard VALUES (-1); END;",
        )
        .await;
    }

    async fn allow_peer_commits(&self) {
        self.raw(
            "DROP TRIGGER fail_peer_commit ON {table}",
            "DROP TRIGGER fail_peer_commit",
        )
        .await;
    }

    /// Marks job `id` completed at attempt 1 by this scenario's client.
    async fn complete_behind_the_back(&self, id: i64) {
        self.exec(
            &format!(
                "UPDATE {{table}} SET state = 'completed', attempt = 1, finalized_at = now(), \
                 attempted_by = ARRAY['{PEER_CLIENT}'] WHERE id = $1"
            ),
            &format!(
                "UPDATE river_job SET state = 'completed', attempt = 1, \
                 finalized_at = strftime('%Y-%m-%d %H:%M:%f', 'now'), \
                 attempted_by = jsonb('[\"{PEER_CLIENT}\"]') WHERE id = ?"
            ),
            id,
        )
        .await;
    }

    /// Makes job `id` undecodable while keeping its identity.
    async fn corrupt(&self, id: i64) {
        self.exec(
            "UPDATE {table} SET metadata = '[1]'::jsonb WHERE id = $1",
            "UPDATE river_job SET tags = jsonb('{\"not\":\"an array\"}') WHERE id = ?",
            id,
        )
        .await;
    }

    /// Finalizes a running job behind River's back.
    async fn discard(&self, id: i64) {
        self.exec(
            "UPDATE {table} SET state = 'discarded', finalized_at = now() WHERE id = $1",
            "UPDATE river_job SET state = 'discarded', \
             finalized_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = ?",
            id,
        )
        .await;
    }
}

/// How a scripted claim takes its rows.
#[derive(Clone, Copy, Debug)]
enum Take {
    /// Claims the rows for this client, as a real peer claim does.
    Claim,
    /// Claims the rows for another client.
    Foreign,
    /// Claims the rows, then returns the first one twice.
    Duplicate,
    /// Reads the rows without claiming them.
    Read,
    /// Returns a row River can't identify.
    Unidentifiable,
    /// Claims the rows, then waits for the coordinator's cancellation before
    /// returning, so it's cancelled before commit.
    ClaimUntilCancelled(&'static str),
    /// Claims the rows, then waits for the scenario's gate before returning.
    ClaimAfterGate,
    /// Claims the rows, then raises a signal as it returns.
    ClaimAndSignal(&'static str),
}

/// Forces the claim callback's signature.
fn claim_callback<F>(callback: F) -> F
where
    F: for<'c> FnOnce(PeerClaimContext<'c>) -> BoxFuture<'c, Result<Vec<ClaimedJob>, PilotError>>
        + Send,
{
    callback
}

/// A peer claim of `ids`, taken as `take` says.
async fn scripted_claim(
    context: PeerClaimContext<'_>,
    take: Take,
    ids: Vec<i64>,
    gate: Arc<Notify>,
    signals: Signals,
) -> Result<Vec<ClaimedJob>, PilotError> {
    let client_id = match take {
        Take::Foreign => "another-client".to_owned(),
        _ => context.client_id.to_owned(),
    };
    let claims = !matches!(take, Take::Read | Take::Unidentifiable);
    let mut rows = match context.connection {
        #[cfg(feature = "postgres")]
        DatabaseConnection::Postgres(connection) => {
            let projection = riverqueue::__private::postgres_job_projection("job");
            let table = context
                .database
                .config()
                .postgres_schema()
                .unwrap()
                .qualify("river_job");
            let rows = if matches!(take, Take::Unidentifiable) {
                sqlx::query("SELECT 1 AS id").fetch_all(connection).await?
            } else if claims {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {table} AS job SET state = 'running', attempt = job.attempt + 1, \
                     attempted_at = now(), attempted_by = array_append(job.attempted_by, $1) \
                     WHERE id = ANY($2) RETURNING {projection}, false AS unique_skipped_as_duplicate"
                )))
                .bind(&client_id)
                .bind(&ids)
                .fetch_all(connection)
                .await?
            } else {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "SELECT {projection}, false AS unique_skipped_as_duplicate FROM {table} AS job \
                     WHERE id = ANY($1)"
                )))
                .bind(&ids)
                .fetch_all(connection)
                .await?
            };
            rows.iter()
                .map(riverqueue::__private::claimed_postgres_job)
                .collect::<Vec<_>>()
        }
        #[cfg(feature = "sqlite")]
        DatabaseConnection::Sqlite(connection) => {
            let columns = riverqueue::__private::SQLITE_JOB_COLUMNS;
            let ids_json = serde_json::to_string(&ids)?;
            let rows = if matches!(take, Take::Unidentifiable) {
                sqlx::query("SELECT 1 AS id").fetch_all(connection).await?
            } else if claims {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE river_job SET state = 'running', attempt = attempt + 1, \
                     attempted_at = ?, \
                     attempted_by = jsonb_insert(coalesce(attempted_by, jsonb('[]')), '$[#]', ?) \
                     WHERE id IN (SELECT value FROM json_each(?)) RETURNING {columns}"
                )))
                .bind(riverqueue::__private::sqlite_timestamp(chrono::Utc::now()))
                .bind(&client_id)
                .bind(&ids_json)
                .fetch_all(connection)
                .await?
            } else {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "SELECT {columns} FROM river_job WHERE id IN (SELECT value FROM json_each(?))"
                )))
                .bind(&ids_json)
                .fetch_all(connection)
                .await?
            };
            rows.iter()
                .map(riverqueue::__private::claimed_sqlite_job)
                .collect::<Vec<_>>()
        }
        #[allow(unreachable_patterns)]
        _ => unreachable!("built-in backends only"),
    };
    match take {
        Take::Duplicate => {
            let first = rows[0].job().unwrap().clone();
            rows.push(first.into());
        }
        Take::ClaimUntilCancelled(signal) => {
            signals.raise(signal);
            context.cancellation.cancelled().await;
        }
        Take::ClaimAfterGate => {
            signals.raise("claim holds its rows");
            gate.notified().await;
        }
        Take::ClaimAndSignal(signal) => signals.raise(signal),
        _ => {}
    }
    Ok(rows)
}

/// Named signals a script raises for the test to wait on.
#[derive(Clone, Default)]
struct Signals {
    changed: Arc<Notify>,
    raised: Arc<Mutex<Vec<&'static str>>>,
}

impl Signals {
    fn raise(&self, signal: &'static str) {
        self.raised.lock().unwrap().push(signal);
        self.changed.notify_waiters();
    }

    async fn wait(&self, signal: &'static str) {
        tokio::time::timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                if self.raised.lock().unwrap().contains(&signal) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {signal}"));
    }
}

/// What a scenario's script observed, by name, checked by the test.
type Checks = Vec<(String, bool)>;

/// Everything a script can use.
#[derive(Clone)]
struct Env {
    checks: mpsc::UnboundedSender<Checks>,
    db: Db,
    gate: Arc<Notify>,
    /// The coordinator's context, kept for operations after it ended.
    kept: Arc<Mutex<Option<WorkContext>>>,
    /// Releases [`BusyArgs`] jobs.
    busy: Arc<Notify>,
    peers: Vec<i64>,
    signals: Signals,
}

impl Env {
    async fn claim(
        &self,
        context: &WorkContext,
        take: Take,
        ids: &[i64],
    ) -> Result<Vec<JobRow>, riverqueue::Error> {
        let (ids, gate, signals) = (ids.to_vec(), Arc::clone(&self.gate), self.signals.clone());
        PeerAttempts::new(context)
            .claim(claim_callback(move |claim| {
                Box::pin(scripted_claim(claim, take, ids, gate, signals))
            }))
            .await
    }
}

type Script =
    Arc<dyn Fn(WorkContext, JobRow, Env) -> BoxFuture<'static, WorkOutcome> + Send + Sync>;

fn peer_error(error: &riverqueue::Error, text: &str) -> bool {
    matches!(
        error,
        riverqueue::Error::Extension {
            phase: riverqueue::ExtensionPhase::AddOnPeerAttempts,
            ..
        }
    ) && riverqueue::__private::error_chain(error).contains(text)
}

/// Records what River tells the coordinator's producer session and the
/// completion step, to show peers take neither.
#[derive(Clone, Default)]
struct PeerPilot {
    finished: Arc<Mutex<Vec<i64>>>,
    set_state: Arc<Mutex<Vec<i64>>>,
}

#[async_trait]
impl Pilot for PeerPilot {
    fn intercepts_job_set_state(&self) -> bool {
        true
    }

    async fn after_jobs_set_state(
        &self,
        _connection: DatabaseConnection<'_>,
        params: &JobSetStateParams,
    ) -> Result<(), PilotError> {
        self.set_state
            .lock()
            .unwrap()
            .extend_from_slice(params.job_ids);
        Ok(())
    }

    async fn start_producer(
        &self,
        _context: ProducerStartContext,
    ) -> Result<Option<Box<dyn PilotProducer>>, PilotError> {
        Ok(Some(Box::new(self.clone())))
    }
}

impl PilotProducer for PeerPilot {
    fn job_finished(&self, job: &JobRow) {
        self.finished.lock().unwrap().push(job.id);
    }
}

/// Cancels failed peers whose `value` is 2 and records every job it sees.
#[derive(Clone, Default)]
struct PeerErrorHandler(Arc<Mutex<Vec<i64>>>);

impl ErrorHandler for PeerErrorHandler {
    async fn handle_error(
        &self,
        _context: &WorkContext,
        job: &JobRow,
        _result: &WorkResult,
    ) -> Result<ErrorHandlerDecision, BoxError> {
        tokio::task::yield_now().await;
        self.0.lock().unwrap().push(job.id);
        let args: PeerArgs = job.decode_args()?;
        Ok(if args.value == 2 {
            ErrorHandlerDecision::Cancel
        } else {
            ErrorHandlerDecision::default()
        })
    }
}

/// A started scenario.
struct Run {
    client: Client,
    coordinator: i64,
    events: riverqueue::EventReceiver,
    handle: riverqueue::RunHandle,
    env: Env,
    checks: mpsc::UnboundedReceiver<Checks>,
    handler: PeerErrorHandler,
    pilot: PeerPilot,
}

impl Run {
    async fn start(
        builder: riverqueue::ClientBuilder,
        db: Db,
        peers: usize,
        script: Script,
    ) -> Self {
        let pilot = PeerPilot::default();
        let handler = PeerErrorHandler::default();
        let (checks_sender, checks) = mpsc::unbounded_channel();
        let env = Env {
            checks: checks_sender,
            db,
            gate: Arc::new(Notify::new()),
            kept: Arc::default(),
            busy: Arc::new(Notify::new()),
            peers: Vec::new(),
            signals: Signals::default(),
        };
        let env_slot = Arc::new(Mutex::new(None::<Env>));
        let mut workers = WorkerRegistry::new();
        let worker_env = Arc::clone(&env_slot);
        workers
            .register_fn(move |context: WorkContext, job: Job<CoordinatorArgs>| {
                let env = worker_env
                    .lock()
                    .unwrap()
                    .clone()
                    .expect("scenario started");
                let run = script(context, job.row, env);
                async move { Ok::<_, BoxError>(run.await) }
            })
            .unwrap()
            .register_fn(|_context: WorkContext, _job: Job<PeerArgs>| async {
                Ok::<_, BoxError>(WorkOutcome::Complete)
            })
            .unwrap();
        let busy_env = Arc::clone(&env_slot);
        workers
            .register_fn(move |_context: WorkContext, _job: Job<BusyArgs>| {
                let env = busy_env.lock().unwrap().clone().expect("scenario started");
                async move {
                    let released = env.busy.notified();
                    env.signals.raise("busy");
                    released.await;
                    Ok::<_, BoxError>(WorkOutcome::Complete)
                }
            })
            .unwrap();
        let client = builder
            .id(PEER_CLIENT)
            .pilot(pilot.clone())
            .error_handler(handler.clone())
            .queue(
                "default",
                QueueConfig::new(2)
                    .with_fetch_cooldown(Duration::from_millis(1))
                    .with_fetch_poll_interval(Duration::from_millis(10)),
            )
            .workers(workers)
            .build()
            .unwrap();
        let mut env = env;
        for value in 1..=i64::try_from(peers).unwrap() {
            env.peers.push(
                client
                    .insert(PeerArgs { value })
                    .opts(InsertOpts::default().with_queue("peers"))
                    .await
                    .unwrap()
                    .id(),
            );
        }
        *env_slot.lock().unwrap() = Some(env.clone());
        let events = client
            .subscribe(&[
                EventKind::JobCancelled,
                EventKind::JobCompleted,
                EventKind::JobFailed,
                EventKind::JobSnoozed,
            ])
            .unwrap();
        let coordinator = client.insert(CoordinatorArgs {}).await.unwrap().id();
        let handle = client.start().unwrap();
        Self {
            client,
            coordinator,
            events,
            handle,
            env,
            checks,
            handler,
            pilot,
        }
    }

    /// Waits for the script's checks and asserts every one.
    async fn assert_checks(&mut self) {
        let checks = tokio::time::timeout(WAIT, self.checks.recv())
            .await
            .expect("script reports its checks")
            .unwrap();
        let failed = checks
            .iter()
            .filter(|(_, passed)| !passed)
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();
        assert!(failed.is_empty(), "failed checks: {failed:?}");
    }

    /// Waits for job `id` to reach a state other than `running`.
    async fn settled(&self, id: i64) -> JobRow {
        tokio::time::timeout(WAIT, async {
            loop {
                let row = self.client.jobs().get(id).await.unwrap();
                if row.state != JobState::Running {
                    return row;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("job {id} never settled"))
    }

    /// Collects events until one for each of `ids` arrived, in order.
    async fn events_for(&mut self, ids: &[i64]) -> Vec<Event> {
        let mut seen = Vec::new();
        tokio::time::timeout(WAIT, async {
            while !ids.iter().all(|id| {
                seen.iter()
                    .any(|event: &Event| event.as_job().unwrap().job.id == *id)
            }) {
                seen.push(self.events.recv().await.unwrap());
            }
        })
        .await
        .expect("events arrive");
        seen
    }

    async fn stop(mut self) {
        tokio::time::timeout(WAIT, self.handle.shutdown())
            .await
            .expect("client stops")
            .unwrap();
    }
}

fn check(checks: &mut Checks, name: &str, passed: bool) {
    checks.push((name.to_owned(), passed));
}

/// Peer outcomes go through River's ordinary completion pipeline: the error
/// handler, the coordinator's metadata, the completion step, fencing, and
/// events. A cancellation requested while a peer ran wins, a stale result
/// leaves the row alone, and peers never reach the producer session.
async fn assert_outcomes_use_the_completion_pipeline(builder: riverqueue::ClientBuilder, db: Db) {
    let script: Script = Arc::new(|context, _row, env| {
        Box::pin(async move {
            let mut checks = Checks::new();
            context.metadata_set("shared", true).unwrap();
            let rows = env.claim(&context, Take::Claim, &env.peers).await.unwrap();
            check(&mut checks, "claims all four", rows.len() == 4);
            let row = |id: i64| rows.iter().find(|row| row.id == id).unwrap().clone();
            let [p1, p2, p3, p4] = [env.peers[0], env.peers[1], env.peers[2], env.peers[3]];
            env.db.discard(p3).await;
            let client = context.client().unwrap();
            client.jobs().cancel(p4).await.unwrap();
            let completed = PeerAttempts::new(&context)
                .complete(vec![
                    PeerOutcome::new(row(p1), Ok(WorkOutcome::Complete)),
                    PeerOutcome::new(row(p2), Err("peer failed".into())),
                    PeerOutcome::new(row(p3), Ok(WorkOutcome::Complete)),
                    PeerOutcome::new(row(p4), Ok(WorkOutcome::Snooze(Duration::from_hours(1)))),
                ])
                .await;
            check(&mut checks, "complete succeeds", completed.is_ok());
            let _ = env.checks.send(checks);
            WorkOutcome::Complete
        })
    });
    let mut run = Run::start(builder, db, 4, script).await;
    run.assert_checks().await;
    let [p1, p2, p3, p4] = [
        run.env.peers[0],
        run.env.peers[1],
        run.env.peers[2],
        run.env.peers[3],
    ];
    let events = run.events_for(&[p1, p2, p4, run.coordinator]).await;
    let states = events
        .iter()
        .map(|event| {
            (
                event.as_job().unwrap().job.id,
                event.as_job().unwrap().job.state,
            )
        })
        .collect::<HashMap<_, _>>();
    assert_eq!(states[&p1], JobState::Completed);
    // The error handler cancelled the failed peer.
    assert_eq!(states[&p2], JobState::Cancelled);
    // The cancellation requested while the peer ran wins over its snooze.
    assert_eq!(states[&p4], JobState::Cancelled);
    let p1_event = events
        .iter()
        .find(|event| event.as_job().unwrap().job.id == p1)
        .unwrap();
    assert_eq!(
        p1_event
            .as_job()
            .unwrap()
            .job
            .metadata
            .get::<bool>("shared")
            .unwrap(),
        Some(true)
    );
    // The stale result left the finalized row alone.
    assert_eq!(
        run.client.jobs().get(p3).await.unwrap().state,
        JobState::Discarded
    );
    assert_eq!(*run.handler.0.lock().unwrap(), [p2]);
    let set_state = run.pilot.set_state.lock().unwrap().clone();
    for id in [p1, p2, p3, p4] {
        assert!(set_state.contains(&id), "{id} in {set_state:?}");
    }
    // Peers take no producer slot, so the session never hears of them.
    run.settled(run.coordinator).await;
    let coordinator = run.coordinator;
    tokio::time::timeout(WAIT, async {
        while !run.pilot.finished.lock().unwrap().contains(&coordinator) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(*run.pilot.finished.lock().unwrap(), [coordinator]);
    run.stop().await;
}

/// A claim River can't accept rolls back whole: duplicate rows, the
/// coordinator's own job, another client's attempt, a row that isn't
/// running, one River can't identify, one already owned, and a stale
/// attempt. A peer is owned only until its outcome persists, after which the
/// same coordinator can claim it again at a new attempt.
#[allow(
    clippy::too_many_lines,
    reason = "one coordinator walks through every rejected claim in turn"
)]
async fn assert_claims_are_checked(builder: riverqueue::ClientBuilder, db: Db) {
    let script: Script = Arc::new(|context, row, env| {
        Box::pin(async move {
            let mut checks = Checks::new();
            let [p1, p2, p3] = [env.peers[0], env.peers[1], env.peers[2]];
            let client = context.client().unwrap().clone();
            let job_state = |id: i64| {
                let client = client.clone();
                async move { client.jobs().get(id).await.unwrap().state }
            };
            for (name, take, ids, reason) in [
                ("duplicate", Take::Duplicate, vec![p1], "twice"),
                ("coordinator", Take::Claim, vec![row.id], "own job"),
                ("foreign", Take::Foreign, vec![p1], "another client"),
                ("not running", Take::Read, vec![p1], "has no attempt"),
                (
                    "unidentifiable",
                    Take::Unidentifiable,
                    vec![p1],
                    "couldn't be identified",
                ),
            ] {
                let claimed = env.claim(&context, take, &ids).await;
                check(
                    &mut checks,
                    &format!("{name} rejected: {claimed:?}"),
                    claimed
                        .as_ref()
                        .is_err_and(|error| peer_error(error, reason)),
                );
                check(
                    &mut checks,
                    &format!("{name} rolled back"),
                    job_state(p1).await == JobState::Available,
                );
            }
            // A row at an attempt of this client that finished elsewhere.
            let p4 = env.peers[3];
            env.db.complete_behind_the_back(p4).await;
            let finished = env.claim(&context, Take::Read, &[p4]).await;
            check(
                &mut checks,
                "not running rejected",
                finished.is_err_and(|error| peer_error(&error, "isn't running")),
            );
            // A job this client works as an ordinary attempt.
            let busy = client.insert(BusyArgs {}).await.unwrap().id();
            env.signals.wait("busy").await;
            let worked = env.claim(&context, Take::Read, &[busy]).await;
            check(
                &mut checks,
                "worked here rejected",
                worked.is_err_and(|error| peer_error(&error, "which this client already works")),
            );
            env.busy.notify_one();
            let owned = env.claim(&context, Take::Claim, &[p2]).await.unwrap();
            let again = env.claim(&context, Take::Read, &[p2]).await;
            check(
                &mut checks,
                "owned rejected",
                again.is_err_and(|error| peer_error(&error, "already works as a peer")),
            );
            // A snooze gives the attempt back, so claiming the job again
            // reaches the same attempt number, which already ended here.
            PeerAttempts::new(&context)
                .complete(vec![PeerOutcome::new(
                    owned[0].clone(),
                    Ok(WorkOutcome::Snooze(Duration::ZERO)),
                )])
                .await
                .unwrap();
            let stale = env.claim(&context, Take::Claim, &[p2]).await;
            check(
                &mut checks,
                "stale rejected",
                stale.is_err_and(|error| peer_error(&error, "already ended here")),
            );
            // A failure keeps the attempt, and ownership ends once the
            // outcome persisted, so the job can be claimed again at once.
            let first = env.claim(&context, Take::Claim, &[p3]).await.unwrap();
            PeerAttempts::new(&context)
                .complete(vec![PeerOutcome::new(
                    first[0].clone(),
                    Err("retry now".into()),
                )])
                .await
                .unwrap();
            let second = env.claim(&context, Take::Claim, &[p3]).await;
            check(
                &mut checks,
                "re-claimed at the next attempt",
                second
                    .as_ref()
                    .is_ok_and(|rows| rows.len() == 1 && rows[0].attempt == 2),
            );
            if let Ok(rows) = second {
                PeerAttempts::new(&context)
                    .complete(vec![PeerOutcome::new(
                        rows[0].clone(),
                        Ok(WorkOutcome::Complete),
                    )])
                    .await
                    .unwrap();
            }
            let _ = env.checks.send(checks);
            WorkOutcome::Complete
        })
    });
    let mut run = Run::start(builder, db, 4, script).await;
    run.assert_checks().await;
    run.settled(run.coordinator).await;
    run.stop().await;
}

/// Outcomes are accepted all or none: each must be for one of this
/// coordinator's peers at its claimed attempt, once, without an earlier
/// outcome. Of two concurrent submissions for one peer, exactly one wins.
async fn assert_outcomes_are_checked(builder: riverqueue::ClientBuilder, db: Db) {
    let script: Script = Arc::new(|context, _row, env| {
        Box::pin(async move {
            let mut checks = Checks::new();
            let [p1, p2, p3] = [env.peers[0], env.peers[1], env.peers[2]];
            let rows = env.claim(&context, Take::Claim, &[p1, p2]).await.unwrap();
            let row = |id: i64| rows.iter().find(|row| row.id == id).unwrap().clone();
            let peers = PeerAttempts::new(&context);
            let outsider = context.client().unwrap().jobs().get(p3).await.unwrap();
            let not_peer = peers
                .complete(vec![
                    PeerOutcome::new(row(p1), Ok(WorkOutcome::Complete)),
                    PeerOutcome::new(outsider, Ok(WorkOutcome::Complete)),
                ])
                .await;
            check(
                &mut checks,
                "not a peer rejected",
                not_peer.is_err_and(|error| peer_error(&error, "isn't a peer")),
            );
            let mut wrong_attempt = row(p1);
            wrong_attempt.attempt += 1;
            let wrong = peers
                .complete(vec![PeerOutcome::new(
                    wrong_attempt,
                    Ok(WorkOutcome::Complete),
                )])
                .await;
            check(
                &mut checks,
                "wrong attempt rejected",
                wrong.is_err_and(|error| peer_error(&error, "isn't the peer attempt")),
            );
            let mut other_client = row(p1);
            other_client.attempted_by.push("another-client".to_owned());
            let foreign = peers
                .complete(vec![PeerOutcome::new(
                    other_client,
                    Ok(WorkOutcome::Complete),
                )])
                .await;
            check(
                &mut checks,
                "another client's attempt rejected",
                foreign.is_err_and(|error| peer_error(&error, "isn't the peer attempt")),
            );
            let twice = peers
                .complete(vec![
                    PeerOutcome::new(row(p1), Ok(WorkOutcome::Complete)),
                    PeerOutcome::new(row(p1), Ok(WorkOutcome::Complete)),
                ])
                .await;
            check(
                &mut checks,
                "two outcomes rejected",
                twice.is_err_and(|error| peer_error(&error, "two outcomes")),
            );
            // Nothing above was accepted, so p1 still takes an outcome.
            let (first, second) = tokio::join!(
                peers.complete(vec![PeerOutcome::new(row(p2), Ok(WorkOutcome::Complete))]),
                peers.complete(vec![PeerOutcome::new(row(p2), Ok(WorkOutcome::Complete))]),
            );
            check(
                &mut checks,
                "exactly one concurrent outcome wins",
                first.is_ok() != second.is_ok(),
            );
            let late = [first, second]
                .into_iter()
                .find_map(Result::err)
                .is_some_and(|error| peer_error(&error, "already has an outcome"));
            check(&mut checks, "the other already had one", late);
            check(
                &mut checks,
                "p1 still takes an outcome",
                peers
                    .complete(vec![PeerOutcome::new(row(p1), Ok(WorkOutcome::Complete))])
                    .await
                    .is_ok(),
            );
            let _ = env.checks.send(checks);
            WorkOutcome::Complete
        })
    });
    let mut run = Run::start(builder, db, 3, script).await;
    run.assert_checks().await;
    for id in [run.env.peers[0], run.env.peers[1]] {
        assert_eq!(run.settled(id).await.state, JobState::Completed);
    }
    run.stop().await;
}

/// A peer the coordinator left without an outcome fails when the
/// coordinator ends on its own, before the coordinator's own outcome, and a
/// row River couldn't decode fails inside the claim.
async fn assert_missing_outcomes_fail(builder: riverqueue::ClientBuilder, db: Db) {
    let script: Script = Arc::new(|context, _row, env| {
        Box::pin(async move {
            let mut checks = Checks::new();
            env.db.corrupt(env.peers[1]).await;
            let rows = env.claim(&context, Take::Claim, &env.peers).await;
            check(
                &mut checks,
                "only the decodable peer returned",
                rows.as_ref()
                    .is_ok_and(|rows| rows.len() == 1 && rows[0].id == env.peers[0]),
            );
            let _ = env.checks.send(checks);
            WorkOutcome::Complete
        })
    });
    let mut run = Run::start(builder, db.clone(), 2, script).await;
    run.assert_checks().await;
    let (p1, coordinator) = (run.env.peers[0], run.coordinator);
    let events = run.events_for(&[p1, coordinator]).await;
    let position = |id: i64| {
        events
            .iter()
            .position(|event| event.as_job().unwrap().job.id == id)
            .unwrap()
    };
    assert!(position(p1) < position(coordinator), "peers settle first");
    let peer = run.client.jobs().get(p1).await.unwrap();
    assert!(matches!(
        peer.state,
        JobState::Available | JobState::Retryable
    ));
    assert!(
        peer.errors[0].error.contains("ended without an outcome"),
        "{:?}",
        peer.errors
    );
    // The undecodable peer's attempt failed as well; it can't be read back.
    db.exec(
        "UPDATE {table} SET metadata = '{}'::jsonb WHERE id = $1",
        "UPDATE river_job SET tags = jsonb('[]') WHERE id = ?",
        run.env.peers[1],
    )
    .await;
    let undecodable = run.client.jobs().get(run.env.peers[1]).await.unwrap();
    assert!(
        undecodable.errors[0].error.contains("couldn't be decoded"),
        "{:?}",
        undecodable.errors
    );
    run.stop().await;
}

/// A hard stop interrupts peers without an outcome, like the coordinator,
/// while a remote cancellation of the coordinator fails them.
async fn assert_stops_interrupt_and_cancellations_fail(
    builder: impl Fn() -> riverqueue::ClientBuilder,
    db: Db,
) {
    for remote in [false, true] {
        let script: Script = Arc::new(|context, _row, env| {
            Box::pin(async move {
                env.claim(&context, Take::Claim, &env.peers).await.unwrap();
                env.signals.raise("claimed");
                context.cancellation_token().cancelled().await;
                WorkOutcome::Complete
            })
        });
        let mut run = Run::start(builder(), db.clone(), 1, script).await;
        run.env.signals.wait("claimed").await;
        let peer = run.env.peers[0];
        if remote {
            // The coordinator hears of its cancellation through the
            // notification listener, which must be listening first.
            run.handle.wait_ready().await.unwrap();
            run.client.jobs().cancel(run.coordinator).await.unwrap();
            let row = run.settled(peer).await;
            assert!(
                matches!(row.state, JobState::Available | JobState::Retryable),
                "{row:?}"
            );
            assert!(row.errors[0].error.contains("ended without an outcome"));
            run.stop().await;
        } else {
            tokio::time::timeout(WAIT, run.handle.shutdown_now())
                .await
                .expect("client stops")
                .unwrap();
            let row = run.client.jobs().get(peer).await.unwrap();
            assert_eq!(row.state, JobState::Available, "{row:?}");
            assert_eq!(row.attempt, 0);
            assert!(row.errors.is_empty(), "{:?}", row.errors);
        }
    }
}

/// A soft stop doesn't end a coordinator's claims: a coordinator that starts
/// claiming after its producer stopped fetching still claims and completes
/// its peers, and the stop resolves only after they persisted. A claim after
/// a hard stop or a remote cancellation of the coordinator is refused and
/// leaves the peers untouched.
async fn assert_soft_stops_keep_claims_open(
    builder: impl Fn() -> riverqueue::ClientBuilder,
    db: Db,
) {
    let script: Script = Arc::new(|context, _row, env| {
        Box::pin(async move {
            let mut checks = Checks::new();
            env.signals.raise("running");
            env.gate.notified().await;
            let claimed = env.claim(&context, Take::Claim, &env.peers).await;
            check(
                &mut checks,
                "claim during a soft stop succeeds",
                claimed
                    .as_ref()
                    .is_ok_and(|rows| rows.len() == env.peers.len()),
            );
            let outcomes = claimed
                .unwrap_or_default()
                .into_iter()
                .map(|row| PeerOutcome::new(row, Ok(WorkOutcome::Complete)))
                .collect();
            check(
                &mut checks,
                "peers complete during a soft stop",
                PeerAttempts::new(&context).complete(outcomes).await.is_ok(),
            );
            let _ = env.checks.send(checks);
            WorkOutcome::Complete
        })
    });
    let mut run = Run::start(builder(), db.clone(), 2, script).await;
    run.env.signals.wait("running").await;
    // The producer stops fetching at once; only then does the coordinator
    // start claiming.
    run.handle.stopper().stop();
    run.env.gate.notify_one();
    tokio::time::timeout(WAIT, run.handle.wait())
        .await
        .expect("client stops")
        .unwrap();
    run.assert_checks().await;
    for id in run.env.peers.iter().copied().chain([run.coordinator]) {
        let row = run.client.jobs().get(id).await.unwrap();
        assert_eq!(row.state, JobState::Completed, "{row:?}");
    }
    assert_eq!(*run.pilot.finished.lock().unwrap(), vec![run.coordinator]);

    for remote in [false, true] {
        let script: Script = Arc::new(|context, _row, env| {
            Box::pin(async move {
                let mut checks = Checks::new();
                env.signals.raise("running");
                context.cancellation_token().cancelled().await;
                check(
                    &mut checks,
                    "claim after cancellation is refused",
                    env.claim(&context, Take::Claim, &env.peers)
                        .await
                        .is_err_and(|error| peer_error(&error, "cancelled")),
                );
                let _ = env.checks.send(checks);
                WorkOutcome::Complete
            })
        });
        let mut run = Run::start(builder(), db.clone(), 1, script).await;
        run.env.signals.wait("running").await;
        if remote {
            // The coordinator hears of its cancellation through the
            // notification listener, which must be listening first.
            run.handle.wait_ready().await.unwrap();
            run.client.jobs().cancel(run.coordinator).await.unwrap();
            run.assert_checks().await;
            run.settled(run.coordinator).await;
        } else {
            run.handle.stopper().stop_now();
            run.assert_checks().await;
        }
        let peer = run.client.jobs().get(run.env.peers[0]).await.unwrap();
        assert_eq!(peer.state, JobState::Available, "{peer:?}");
        assert_eq!(peer.attempt, 0);
        run.stop().await;
    }
}

/// Once the coordinator's attempt ended, its peer operations are refused.
/// A claim still in flight when it ends is waited for: its rows become
/// peers and then fail like any peer left without an outcome. A claim
/// whose coordinator is cancelled before commit rolls back.
async fn assert_coordinator_lifetime_bounds_operations(
    builder: impl Fn() -> riverqueue::ClientBuilder,
    db: Db,
) {
    // In-flight claim at exit, then late operations.
    let script: Script = Arc::new(|context, _row, env| {
        Box::pin(async move {
            *env.kept.lock().unwrap() = Some(context.clone());
            let claim_env = env.clone();
            let claim_context = context.clone();
            tokio::spawn(async move {
                let claimed = claim_env
                    .claim(&claim_context, Take::ClaimAfterGate, &claim_env.peers)
                    .await;
                claim_env.signals.raise(if claimed.is_ok() {
                    "claim committed"
                } else {
                    "claim failed"
                });
            });
            env.signals.wait("claim holds its rows").await;
            env.signals.raise("coordinator returns");
            WorkOutcome::Complete
        })
    });
    let run = Run::start(builder(), db.clone(), 1, script).await;
    run.env.signals.wait("coordinator returns").await;
    // The coordinator's end waits for the claim it accepted.
    run.env.gate.notify_one();
    run.env.signals.wait("claim committed").await;
    let peer = run.settled(run.env.peers[0]).await;
    assert!(peer.errors[0].error.contains("ended without an outcome"));
    assert_eq!(
        run.settled(run.coordinator).await.state,
        JobState::Completed
    );
    let kept = run.env.kept.lock().unwrap().clone().unwrap();
    let late = run.env.claim(&kept, Take::Claim, &run.env.peers).await;
    assert!(late.is_err_and(|error| peer_error(&error, "running attempt")));
    let late = PeerAttempts::new(&kept).complete(Vec::new()).await;
    assert!(late.is_err_and(|error| peer_error(&error, "running attempt")));
    run.stop().await;

    // Cancelled before commit.
    let script: Script = Arc::new(|context, _row, env| {
        Box::pin(async move {
            let mut checks = Checks::new();
            let claimed = env
                .claim(&context, Take::ClaimUntilCancelled("claiming"), &env.peers)
                .await;
            check(
                &mut checks,
                "cancelled claim fails",
                claimed.is_err_and(|error| peer_error(&error, "cancelled")),
            );
            let _ = env.checks.send(checks);
            WorkOutcome::Complete
        })
    });
    let mut run = Run::start(builder(), db, 1, script).await;
    run.env.signals.wait("claiming").await;
    // A hard stop cancels the coordinator without a write, which SQLite
    // couldn't take while the claim holds its write lock.
    run.handle.stopper().stop_now();
    run.assert_checks().await;
    let peer = run.client.jobs().get(run.env.peers[0]).await.unwrap();
    assert_eq!(peer.state, JobState::Available);
    assert_eq!(peer.attempt, 0);
    run.stop().await;
}

/// An operation whose future is dropped before its outcomes reached the
/// completer, as by a `select!` or `timeout` around it or an aborted task,
/// leaves its peers without an outcome, so the coordinator's end still gives
/// them one instead of leaving them running.
async fn assert_dropped_operations_leave_peers_without_outcomes(
    builder: riverqueue::ClientBuilder,
    db: Db,
) {
    let script: Script = Arc::new(|context, _row, env| {
        Box::pin(async move {
            let mut checks = Checks::new();
            let rows = env.claim(&context, Take::Claim, &env.peers).await.unwrap();
            // The error handler yields, so the completion is pending when the
            // other branch wins and the completion's future is dropped.
            let dropped = tokio::select! {
                biased;
                _ = PeerAttempts::new(&context)
                    .complete(vec![PeerOutcome::new(rows[0].clone(), Err("dropped".into()))]) => false,
                () = std::future::ready(()) => true,
            };
            check(&mut checks, "completion dropped", dropped);
            let _ = env.checks.send(checks);
            WorkOutcome::Complete
        })
    });
    let mut run = Run::start(builder, db, 1, script).await;
    run.assert_checks().await;
    let peer = run.settled(run.env.peers[0]).await;
    assert!(
        peer.errors
            .last()
            .is_some_and(|error| error.error.contains("ended without an outcome")),
        "{peer:?}"
    );
    run.stop().await;
}

/// A claim whose commit fails, or whose future is dropped while it commits,
/// gives its rows back, so the same coordinator can claim them again.
async fn assert_failed_commits_release_reservations(builder: riverqueue::ClientBuilder, db: Db) {
    db.fail_peer_commits().await;
    let script: Script = Arc::new(|context, _row, env| {
        Box::pin(async move {
            let mut checks = Checks::new();
            let failed = env.claim(&context, Take::Claim, &env.peers).await;
            check(
                &mut checks,
                "commit failed",
                matches!(failed, Err(riverqueue::Error::Database(_))),
            );
            env.db.allow_peer_commits().await;
            let again = env.claim(&context, Take::Claim, &env.peers).await;
            check(
                &mut checks,
                "claimed again after the failed commit",
                again.as_ref().is_ok_and(|rows| rows.len() == 1),
            );
            if let Ok(rows) = again {
                PeerAttempts::new(&context)
                    .complete(vec![PeerOutcome::new(
                        rows[0].clone(),
                        Ok(WorkOutcome::Complete),
                    )])
                    .await
                    .unwrap();
            }
            let _ = env.checks.send(checks);
            WorkOutcome::Complete
        })
    });
    let mut run = Run::start(builder, db, 1, script).await;
    run.assert_checks().await;
    assert_eq!(
        run.settled(run.env.peers[0]).await.state,
        JobState::Completed
    );
    run.stop().await;
}

/// An attempt abandoned after its worker outlived an abort leaves its peers
/// to the rescuer but stops owning them, so a later attempt of the same
/// client can claim them. With `racing_claim`, a claim still committing when
/// the attempt is abandoned is refused once it commits and gives its rows
/// back too; that needs a commit slow enough to abandon the attempt during
/// it.
async fn assert_abandoned_attempts_release_peers(
    builder: riverqueue::ClientBuilder,
    db: Db,
    racing_claim: bool,
) {
    let (unblock, blocked) = std::sync::mpsc::channel::<()>();
    let blocked = Arc::new(Mutex::new(Some(blocked)));
    let runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let script: Script = Arc::new(move |context, _row, env| {
        let blocked = Arc::clone(&blocked);
        let first = runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
        Box::pin(async move {
            if first {
                env.claim(&context, Take::Claim, &env.peers[..1])
                    .await
                    .unwrap();
                if racing_claim {
                    let claim_env = env.clone();
                    let claim_context = context.clone();
                    tokio::spawn(async move {
                        let claimed = claim_env
                            .claim(
                                &claim_context,
                                Take::ClaimAndSignal("claim commits"),
                                &claim_env.peers[1..],
                            )
                            .await;
                        claim_env.signals.raise(
                            if claimed.is_err_and(|error| peer_error(&error, "running attempt")) {
                                "late claim refused"
                            } else {
                                "late claim accepted"
                            },
                        );
                    });
                    env.signals.wait("claim commits").await;
                }
                env.signals.raise("coordinator blocks");
                // Blocks the worker's thread, so neither cancellation nor an
                // abort can end it.
                let receiver = blocked.lock().unwrap().take().unwrap();
                let _ = receiver.recv();
                return WorkOutcome::Complete;
            }
            let mut checks = Checks::new();
            let rows = env.claim(&context, Take::Read, &env.peers).await;
            check(
                &mut checks,
                &format!("claimed again by a later attempt: {rows:?}"),
                rows.as_ref()
                    .is_ok_and(|rows| rows.len() == env.peers.len()),
            );
            if let Ok(rows) = rows {
                PeerAttempts::new(&context)
                    .complete(
                        rows.into_iter()
                            .map(|row| PeerOutcome::new(row, Ok(WorkOutcome::Complete)))
                            .collect(),
                    )
                    .await
                    .unwrap();
            }
            let _ = env.checks.send(checks);
            WorkOutcome::Complete
        })
    });
    let peers = if racing_claim { 2 } else { 1 };
    let mut run = Run::start(
        builder.job_stuck_threshold(Duration::from_millis(50)),
        db,
        peers,
        script,
    )
    .await;
    run.env.signals.wait("coordinator blocks").await;
    tokio::time::timeout(WAIT, run.handle.shutdown_now())
        .await
        .expect("client stops")
        .unwrap();
    if racing_claim {
        run.env.signals.wait("late claim refused").await;
    }
    unblock.send(()).unwrap();

    run.client.insert(CoordinatorArgs {}).await.unwrap();
    run.handle = run.client.start().unwrap();
    run.assert_checks().await;
    for id in run.env.peers.clone() {
        assert_eq!(run.settled(id).await.state, JobState::Completed);
    }
    run.stop().await;
}

#[cfg(all(feature = "postgres", river_postgres_tests))]
mod postgres {
    use riverqueue::database::PostgresDatabase;

    use super::*;
    use crate::support::PostgresSchema;

    fn builder(schema: &PostgresSchema) -> riverqueue::ClientBuilder {
        Client::builder(
            PostgresDatabase::new(schema.pool.clone()).with_schema(schema.schema.clone()),
        )
    }

    fn db(schema: &PostgresSchema) -> Db {
        Db::Postgres(
            schema.pool.clone(),
            schema.table("river_job"),
            schema.table("peer_test_function"),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn abandoned_attempts_release_peers() {
        let schema = PostgresSchema::new("peer_abandoned").await;
        assert_abandoned_attempts_release_peers(builder(&schema), db(&schema), false).await;
        schema.cleanup().await;
    }

    /// Only PostgreSQL can hold a commit open long enough to abandon the
    /// attempt while its claim commits.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn claims_committing_while_abandoned_release_peers() {
        let schema = PostgresSchema::new("peer_abandon_race").await;
        let db = db(&schema);
        slow_peer_commits(&db).await;
        assert_abandoned_attempts_release_peers(builder(&schema), db, true).await;
        schema.cleanup().await;
    }

    /// Makes each commit that changes a peer job take a second.
    async fn slow_peer_commits(db: &Db) {
        db.raw(
            "CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN IF NEW.kind = 'peer_job' AND NEW.state = 'running' THEN \
             PERFORM pg_sleep(1); END IF; RETURN NULL; END $$; \
             CREATE CONSTRAINT TRIGGER slow_peer_commit AFTER UPDATE ON {table} \
             DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {function}();",
            "",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_operations_leave_peers_without_outcomes() {
        let schema = PostgresSchema::new("peer_dropped").await;
        assert_dropped_operations_leave_peers_without_outcomes(builder(&schema), db(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_commits_release_reservations() {
        let schema = PostgresSchema::new("peer_commit").await;
        assert_failed_commits_release_reservations(builder(&schema), db(&schema)).await;
        schema.cleanup().await;
    }

    /// Only PostgreSQL can hold a commit open long enough to drop the claim
    /// while it commits.
    #[tokio::test(flavor = "multi_thread")]
    async fn claims_dropped_while_committing_release_reservations() {
        let schema = PostgresSchema::new("peer_commit_drop").await;
        let db = db(&schema);
        slow_peer_commits(&db).await;
        let script: Script = Arc::new(|context, _row, env| {
            Box::pin(async move {
                let mut checks = Checks::new();
                let dropped = tokio::time::timeout(
                    Duration::from_millis(300),
                    env.claim(&context, Take::Claim, &env.peers),
                )
                .await;
                check(
                    &mut checks,
                    "claim dropped while committing",
                    dropped.is_err(),
                );
                // Waits for the commit the dropped claim started.
                env.db
                    .raw("DROP TRIGGER slow_peer_commit ON {table}", "")
                    .await;
                let state = context
                    .client()
                    .unwrap()
                    .jobs()
                    .get(env.peers[0])
                    .await
                    .unwrap()
                    .state;
                let take = if state == JobState::Running {
                    Take::Read
                } else {
                    Take::Claim
                };
                let again = env.claim(&context, take, &env.peers).await;
                check(
                    &mut checks,
                    &format!("claimed again: {again:?}"),
                    again.as_ref().is_ok_and(|rows| rows.len() == 1),
                );
                if let Ok(rows) = again {
                    PeerAttempts::new(&context)
                        .complete(vec![PeerOutcome::new(
                            rows[0].clone(),
                            Ok(WorkOutcome::Complete),
                        )])
                        .await
                        .unwrap();
                }
                let _ = env.checks.send(checks);
                WorkOutcome::Complete
            })
        });
        let mut run = Run::start(builder(&schema), db, 1, script).await;
        run.assert_checks().await;
        run.stop().await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn claims_are_checked() {
        let schema = PostgresSchema::new("peer_claims").await;
        assert_claims_are_checked(builder(&schema), db(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn coordinator_lifetime_bounds_operations() {
        let schema = PostgresSchema::new("peer_lifetime").await;
        assert_coordinator_lifetime_bounds_operations(|| builder(&schema), db(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn missing_outcomes_fail() {
        let schema = PostgresSchema::new("peer_missing").await;
        assert_missing_outcomes_fail(builder(&schema), db(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn outcomes_are_checked() {
        let schema = PostgresSchema::new("peer_outcomes").await;
        assert_outcomes_are_checked(builder(&schema), db(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn outcomes_use_the_completion_pipeline() {
        let schema = PostgresSchema::new("peer_pipeline").await;
        assert_outcomes_use_the_completion_pipeline(builder(&schema), db(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn soft_stops_keep_claims_open() {
        let schema = PostgresSchema::new("peer_soft_stop").await;
        assert_soft_stops_keep_claims_open(|| builder(&schema), db(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stops_interrupt_and_cancellations_fail() {
        let schema = PostgresSchema::new("peer_stop").await;
        assert_stops_interrupt_and_cancellations_fail(|| builder(&schema), db(&schema)).await;
        schema.cleanup().await;
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    use crate::support::{sqlite_cleanup, sqlite_file_pool};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn abandoned_attempts_release_peers() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_abandoned_attempts_release_peers(
            Client::builder(pool.clone()),
            Db::Sqlite(pool.clone()),
            false,
        )
        .await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_operations_leave_peers_without_outcomes() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_dropped_operations_leave_peers_without_outcomes(
            Client::builder(pool.clone()),
            Db::Sqlite(pool.clone()),
        )
        .await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_commits_release_reservations() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_failed_commits_release_reservations(
            Client::builder(pool.clone()),
            Db::Sqlite(pool.clone()),
        )
        .await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn claims_are_checked() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_claims_are_checked(Client::builder(pool.clone()), Db::Sqlite(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn coordinator_lifetime_bounds_operations() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_coordinator_lifetime_bounds_operations(
            || Client::builder(pool.clone()),
            Db::Sqlite(pool.clone()),
        )
        .await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn missing_outcomes_fail() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_missing_outcomes_fail(Client::builder(pool.clone()), Db::Sqlite(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn outcomes_are_checked() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_outcomes_are_checked(Client::builder(pool.clone()), Db::Sqlite(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn outcomes_use_the_completion_pipeline() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_outcomes_use_the_completion_pipeline(
            Client::builder(pool.clone()),
            Db::Sqlite(pool.clone()),
        )
        .await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn soft_stops_keep_claims_open() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_soft_stops_keep_claims_open(
            || Client::builder(pool.clone()),
            Db::Sqlite(pool.clone()),
        )
        .await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stops_interrupt_and_cancellations_fail() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_stops_interrupt_and_cancellations_fail(
            || Client::builder(pool.clone()),
            Db::Sqlite(pool.clone()),
        )
        .await;
        sqlite_cleanup(pool, path).await;
    }
}
