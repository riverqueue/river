//! The contract's methods, served by one River `Client` per schema over any
//! backend.

use std::{
    collections::{HashMap, hash_map::Entry},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use riverqueue::{
    Client, EventRecvError, InsertManyItem, JobListCursor, JobListOrderBy, JobListParams,
    JobMetadata, JobState, QueueSelector, QueueUpdateParams, RunHandle, SortDirection,
    migrate::{Direction, MigrateOpts},
    sqlx::Transaction,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, value::RawValue};
use tokio::task::JoinHandle;

use crate::{
    backend::Backend,
    protocol::{
        Empty, HandshakeResult, InsertOpts, InsertParams, InsertResult, Job, JobInsertResult,
        JobParams, ListParams, ListResult, MigrateParams, MigrateResult, QueueParams,
        ReleaseParams, RequestResignParams, RpcError, StartParams, StopParams, TxEndParams,
        TxParams,
    },
    worker::{self, Barriers, EVENT_KINDS, EchoArgs, Stats},
};

/// How long stopping a client may take.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// Awaits a River request in the transaction `executor`, if any.
macro_rules! run {
    ($request:expr, $executor:expr) => {
        match $executor {
            Some(executor) => $request.tx(executor).await,
            None => $request.await,
        }
    };
}

pub struct Server<B: Backend> {
    backend: B,
    barriers: Arc<Barriers>,
    /// Clients for everything but working jobs, by schema.
    clients: HashMap<String, Client>,
    running: Option<Running>,
    txs: HashMap<String, Transaction<'static, B::Db>>,
}

/// The worker client `start` started.
struct Running {
    claim_barrier: String,
    events: JoinHandle<()>,
    handle: RunHandle,
    stats: Arc<Stats>,
}

impl<B: Backend> Server<B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            barriers: Arc::default(),
            clients: HashMap::new(),
            running: None,
            txs: HashMap::new(),
        }
    }

    pub async fn handle(
        &mut self,
        method: &str,
        params: Option<&RawValue>,
    ) -> Result<Box<RawValue>, RpcError> {
        match method {
            "cancel" | "retry" => respond(self.job(method, decode(params)?).await),
            "handshake" => {
                let Empty {} = decode(params)?;
                respond(Ok(HandshakeResult {
                    driver: B::DRIVER,
                    implementation: "rust",
                    version: env!("CARGO_PKG_VERSION"),
                }))
            }
            "insert" => respond(self.insert(decode(params)?).await),
            "list" => respond(self.list(decode(params)?).await),
            "migrate" => respond(self.migrate(decode(params)?).await),
            "queue" => respond(self.queue(decode(params)?).await),
            "release" => {
                let params: ReleaseParams = decode(params)?;
                self.barriers.release(&params.name);
                respond(Ok(()))
            }
            "request_resign" => respond(self.request_resign(decode(params)?).await),
            "start" => respond(self.start(decode(params)?).await),
            "stats" => {
                let Empty {} = decode(params)?;
                let running = self.running.as_ref().ok_or_else(not_running)?;
                respond(Ok(running.stats.snapshot()))
            }
            "stop" => {
                let params: StopParams = decode(params)?;
                respond(self.stop(params.cancel).await)
            }
            "tx_begin" => respond(self.tx_begin(decode(params)?).await),
            "tx_end" => respond(self.tx_end(decode(params)?).await),
            _ => Err(RpcError::new(
                RpcError::METHOD_NOT_FOUND,
                format!("unknown method {method:?}"),
            )),
        }
    }

    /// Stops the running client and rolls back open transactions.
    pub async fn shutdown(&mut self) {
        if self.running.is_some() {
            let _ = self.stop(true).await;
        }
        for (_, transaction) in self.txs.drain() {
            let _ = transaction.rollback().await;
        }
    }

    /// Returns the client for `schema` and the open transaction named `tx`,
    /// if it's set, as an executor.
    fn target(
        &mut self,
        schema: &str,
        tx: &str,
    ) -> Result<
        (
            &Client,
            Option<impl riverqueue::database::DatabaseTransactionExecutor<'_>>,
        ),
        RpcError,
    > {
        let transaction = if tx.is_empty() {
            None
        } else {
            let transaction = self
                .txs
                .get_mut(tx)
                .ok_or_else(|| RpcError::not_found(format!("transaction {tx:?} is not open")))?;
            Some(B::executor(transaction))
        };
        let client = match self.clients.entry(schema.to_owned()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(self.backend.builder(schema)?.build()?),
        };
        Ok((client, transaction))
    }

    async fn insert(&mut self, params: InsertParams) -> Result<InsertResult, RpcError> {
        let items = params
            .jobs
            .into_iter()
            .map(|job| {
                let args = EchoArgs {
                    behavior: job.behavior,
                    duration_ms: job.duration_ms,
                    message: job.message,
                };
                Ok(InsertManyItem::new(args, insert_opts(job.opts)?))
            })
            .collect::<Result<Vec<_>, RpcError>>()?;
        let (client, tx) = self.target(&params.schema, &params.tx)?;
        let results = run!(client.insert_many(items), tx)?
            .into_iter()
            .map(|result| JobInsertResult {
                job: result.job.row.into(),
                unique_skipped_as_duplicate: result.unique_skipped_as_duplicate,
            })
            .collect();
        Ok(InsertResult { results })
    }

    async fn job(&mut self, method: &str, params: JobParams) -> Result<Job, RpcError> {
        let (client, tx) = self.target(&params.schema, &params.tx)?;
        let row = if method == "cancel" {
            run!(client.jobs().cancel(params.id), tx)?
        } else {
            run!(client.jobs().retry(params.id), tx)?
        };
        Ok(row.into())
    }

    async fn list(&mut self, mut params: ListParams) -> Result<ListResult, RpcError> {
        let (schema, tx) = (
            std::mem::take(&mut params.schema),
            std::mem::take(&mut params.tx),
        );
        let list = list_params(params)?;
        let (client, tx) = self.target(&schema, &tx)?;
        let listed = run!(client.jobs().list(list), tx)?;
        Ok(ListResult {
            cursor: listed.last_cursor.as_ref().map(JobListCursor::encode),
            jobs: listed.jobs.into_iter().map(Job::from).collect(),
        })
    }

    async fn migrate(&self, params: MigrateParams) -> Result<MigrateResult, RpcError> {
        let direction = match params.direction.as_str() {
            "" | "up" => Direction::Up,
            "down" => Direction::Down,
            other => {
                return Err(RpcError::invalid_params(format!(
                    "unknown direction {other:?}"
                )));
            }
        };
        let mut opts = MigrateOpts::new();
        if let Some(target) = params.target_version {
            opts = opts.with_target_version(target);
        }
        let migrated = self
            .backend
            .migrate(&params.schema, direction, opts)
            .await?;
        Ok(MigrateResult {
            versions: migrated
                .versions
                .iter()
                .map(|version| version.version)
                .collect(),
        })
    }

    async fn queue(&mut self, params: QueueParams) -> Result<(), RpcError> {
        let (client, tx) = self.target(&params.schema, &params.tx)?;
        let selector = || match params.name.as_str() {
            "*" => QueueSelector::All,
            name => QueueSelector::from(name),
        };
        match params.action.as_str() {
            "pause" => run!(client.queues().pause(selector()), tx)?,
            "resume" => run!(client.queues().resume(selector()), tx)?,
            "update" => {
                let mut update = QueueUpdateParams::new();
                if let Some(metadata) = params.metadata {
                    update = update.metadata(object(metadata, "queue metadata")?);
                }
                run!(client.queues().update(&params.name, update), tx)?;
            }
            action => {
                return Err(RpcError::invalid_params(format!(
                    "unknown queue action {action:?}"
                )));
            }
        }
        Ok(())
    }

    async fn request_resign(&mut self, params: RequestResignParams) -> Result<(), RpcError> {
        let (client, tx) = self.target(&params.schema, &params.tx)?;
        run!(client.request_resign(), tx)?;
        Ok(())
    }

    async fn start(&mut self, params: StartParams) -> Result<(), RpcError> {
        if self.running.is_some() {
            return Err(RpcError::rejected("a client is already running"));
        }
        let stats = Arc::new(Stats::default());
        let client = worker::configure(
            self.backend.builder(&params.schema)?,
            &params,
            &self.barriers,
            &stats,
        )?
        .build()?;

        let mut receiver = client.subscribe(&EVENT_KINDS)?;
        let events = tokio::spawn({
            let stats = Arc::clone(&stats);
            async move {
                loop {
                    match receiver.recv().await {
                        Ok(event) => stats.record_event(&event),
                        Err(EventRecvError::Lagged(_)) => {}
                        Err(_) => break,
                    }
                }
            }
        });
        let mut handle = match client.start() {
            Ok(handle) => handle,
            Err(error) => {
                events.abort();
                return Err(error.into());
            }
        };
        handle.wait_ready().await?;
        self.running = Some(Running {
            claim_barrier: params.claim_barrier,
            events,
            handle,
            stats,
        });
        Ok(())
    }

    async fn stop(&mut self, cancel: bool) -> Result<(), RpcError> {
        let mut running = self.running.take().ok_or_else(not_running)?;
        // A claim held on its barrier would keep the client from stopping.
        if !running.claim_barrier.is_empty() {
            self.barriers.release(&running.claim_barrier);
        }
        let stopped = if cancel {
            tokio::time::timeout(STOP_TIMEOUT, running.handle.stop_and_cancel()).await
        } else {
            tokio::time::timeout(STOP_TIMEOUT, running.handle.stop()).await
        };
        running.events.abort();
        stopped.map_err(|_| RpcError::rejected("timed out stopping the client"))??;
        Ok(())
    }

    async fn tx_begin(&mut self, params: TxParams) -> Result<(), RpcError> {
        if params.tx.is_empty() {
            return Err(RpcError::invalid_params("tx is required"));
        }
        if self.txs.contains_key(&params.tx) {
            return Err(RpcError::rejected(format!(
                "transaction {:?} is already open",
                params.tx
            )));
        }
        let transaction = self.backend.begin().await.map_err(RpcError::rejected)?;
        self.txs.insert(params.tx, transaction);
        Ok(())
    }

    async fn tx_end(&mut self, params: TxEndParams) -> Result<(), RpcError> {
        let transaction = self.txs.remove(&params.tx).ok_or_else(|| {
            RpcError::not_found(format!("transaction {:?} is not open", params.tx))
        })?;
        if params.commit {
            transaction.commit().await
        } else {
            transaction.rollback().await
        }
        .map_err(RpcError::rejected)
    }
}

/// Decodes params strictly. Missing params are an empty object.
fn decode<T: DeserializeOwned>(params: Option<&RawValue>) -> Result<T, RpcError> {
    let params = params.map_or("{}", RawValue::get);
    let params = if params == "null" { "{}" } else { params };
    serde_json::from_str(params).map_err(RpcError::invalid_params)
}

/// Encodes a result. Jobs keep their stored JSON, so it's never decoded into
/// a `Value`, whose numbers are only floats. An empty result is an empty
/// object.
fn respond<T: Serialize>(result: Result<T, RpcError>) -> Result<Box<RawValue>, RpcError> {
    let encoded = serde_json::value::to_raw_value(&result?)
        .map_err(|error| RpcError::new(RpcError::INTERNAL, error))?;
    if encoded.get() == "null" {
        return Ok(RawValue::from_string("{}".to_owned()).expect("an empty object is JSON"));
    }
    Ok(encoded)
}

fn not_running() -> RpcError {
    RpcError::rejected("no client is running")
}

/// Decodes a JSON object River takes as a map, rejecting anything else as
/// River does.
fn object(value: Value, what: &str) -> Result<Map<String, Value>, RpcError> {
    match value {
        Value::Object(map) => Ok(map),
        other => Err(RpcError::rejected(format!(
            "{what} must be an object, not {other}"
        ))),
    }
}

/// Parses a value River's Go implementation would only reject on use, so
/// an invalid one is rejected rather than invalid.
fn parse<T: FromStr>(value: &str) -> Result<T, RpcError>
where
    T::Err: std::fmt::Display,
{
    value.parse().map_err(RpcError::rejected)
}

fn parse_all<T: FromStr>(values: &[String]) -> Result<Vec<T>, RpcError>
where
    T::Err: std::fmt::Display,
{
    values.iter().map(|value| parse(value)).collect()
}

fn insert_opts(opts: Option<InsertOpts>) -> Result<riverqueue::InsertOpts, RpcError> {
    let mut insert = riverqueue::InsertOpts::default();
    let Some(opts) = opts else {
        return Ok(insert);
    };
    if opts.max_attempts != 0 {
        insert = insert.with_max_attempts(narrow(opts.max_attempts)?);
    }
    if let Some(metadata) = opts.metadata {
        insert = insert.with_metadata(JobMetadata::try_from(metadata).map_err(RpcError::rejected)?);
    }
    insert = insert.with_pending(opts.pending);
    if opts.priority != 0 {
        insert = insert.with_priority(narrow(opts.priority)?);
    }
    if !opts.queue.is_empty() {
        insert = insert.with_queue(opts.queue);
    }
    if let Some(scheduled_at) = opts.scheduled_at {
        insert = insert.with_scheduled_at(scheduled_at);
    }
    if !opts.tags.is_empty() {
        insert = insert.with_tags(opts.tags);
    }
    if let Some(unique) = opts.unique {
        let states = parse_all::<JobState>(&unique.by_state)?;
        let mut unique_opts = riverqueue::UniqueOpts::new()
            .with_by_args(unique.by_args)
            .with_by_queue(unique.by_queue)
            .with_exclude_kind(unique.exclude_kind);
        if unique.by_period_ms > 0 {
            unique_opts = unique_opts.with_by_period(Duration::from_millis(unique.by_period_ms));
        }
        if !states.is_empty() {
            unique_opts = unique_opts.with_by_state(states);
        }
        insert = insert.with_unique(unique_opts);
    }
    Ok(insert)
}

/// Narrows a maximum attempt count or priority, which River rejects out of
/// range.
fn narrow<T: TryFrom<i64>>(value: i64) -> Result<T, RpcError> {
    T::try_from(value).map_err(|_| RpcError::rejected(format!("{value} is out of range")))
}

fn list_params(params: ListParams) -> Result<JobListParams, RpcError> {
    let mut list = JobListParams::default()
        .ids(params.ids)
        .kinds(params.kinds)
        .priorities(params.priorities)
        .queues(params.queues)
        .states(parse_all::<JobState>(&params.states)?)
        .tags_all(params.tags_all)
        .order_by(if params.order_by.is_empty() {
            JobListOrderBy::Id
        } else {
            parse(&params.order_by)?
        })
        .direction(match params.direction.as_str() {
            "" | "asc" => SortDirection::Ascending,
            "desc" => SortDirection::Descending,
            other => {
                return Err(RpcError::invalid_params(format!(
                    "unknown direction {other:?}"
                )));
            }
        });
    if !params.after.is_empty() {
        let cursor = JobListCursor::decode(&params.after)
            .map_err(|error| RpcError::invalid_params(format!("invalid cursor: {error}")))?;
        list = list.after(cursor);
    }
    if params.limit > 0 {
        list = list.limit(params.limit);
    }
    if let Some(metadata) = params.metadata {
        list = list.metadata(object(metadata, "metadata filter")?);
    }
    Ok(list)
}
