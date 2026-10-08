/**
 * Exact-version SPI through which a first-party companion package takes part
 * in the operations River owns. Only `riverqueue/unstable-driver` exports it;
 * applications never configure a pilot.
 */
import type { Client } from "./client.js";
import type {
  DriverCapability,
  DriverInsertResult,
  InsertDriver,
  JobClaimResult,
  JobCompletionCommand,
  JobCompletionResult,
  JobInsertParams,
  LeaderTerm,
  QueueRow,
  RuntimeJobRescue,
} from "./driver.js";
import type { WorkAttemptResult } from "./extensions.js";
import type { JobRow } from "./job.js";
import type { Logger } from "./logger.js";
import type { PeriodicJobStore } from "./periodic-job-store.js";
import type { WorkAttemptContext, WorkContext } from "./worker.js";

/**
 * Filters for one batch of {@link PilotDatabase.deleteFinalizedJobs}, the
 * same ones River's job cleaner uses. A job is deleted when it's in a state
 * whose cutoff is set and was finalized before that cutoff.
 */
export interface FinalizedJobDeleteParams {
  /** Delete cancelled jobs finalized before this, or none when `null`. */
  readonly cancelledBefore: Temporal.Instant | null;
  /** Delete completed jobs finalized before this, or none when `null`. */
  readonly completedBefore: Temporal.Instant | null;
  /** Delete discarded jobs finalized before this, or none when `null`. */
  readonly discardedBefore: Temporal.Instant | null;
  /** The most jobs the batch deletes, taking the lowest IDs first. */
  readonly limit: number;
  /** Queues whose jobs are kept, even when `queuesIncluded` lists them. */
  readonly queuesExcluded?: readonly string[];
  /**
   * Queues the batch is limited to. Absent or `null` matches every queue,
   * while an empty list matches none.
   */
  readonly queuesIncluded?: readonly string[] | null;
}

/**
 * A driver's database, as River hands it to a pilot: native connections and
 * transactions on the connection River uses, plus the few River statements a
 * companion needs inside its own transactions.
 *
 * `Transaction` is the driver's native handle: a node-postgres client, or a
 * `node:sqlite` `DatabaseSync`. River's own transactions reach a pilot as
 * native handles too, never as opaque values. Callbacks must not keep or
 * close a handle, and must not begin, commit, or roll back transactions on
 * it themselves. River can't detect a handle kept past its callback: a
 * statement run on it later joins whatever that connection then has open.
 */
export interface PilotDatabase<Transaction> {
  /** The backend's name, such as `"postgres"` or `"sqlite"`. */
  readonly backend: string;
  /** The schema holding River's tables, or null for the default. */
  readonly schema: string | null;

  /**
   * Run `callback` with a native handle River has borrowed for it, which is
   * not in a transaction: a pooled client on Postgres, or River's own
   * connection on SQLite while River's lock on it is held. Use it for reads
   * and single autocommit statements. `signal` stops only the wait for the
   * handle. A transaction the callback leaves open is rolled back, and the
   * call rejects with a `TransactionScopeError`.
   */
  connection<Result>(
    callback: (handle: Transaction) => PromiseLike<Result> | Result,
    options?: { readonly signal?: AbortSignal }
  ): Promise<Result>;

  /**
   * Run `callback` in a new transaction, committed once it resolves and
   * rolled back when it rejects, or directly in a supplied `tx`. Like River
   * for Go, River opens no savepoint in `tx` and never commits or rolls it
   * back: when `callback` rejects, its writes stay in `tx` until the
   * transaction's owner rolls it back.
   *
   * `signal` stops the wait to begin, and when it has aborted by the time
   * `callback` resolves the work is rolled back instead of committed, or,
   * in a supplied `tx`, the call rejects.
   * `callback` runs at most once: it never runs when River can't begin, and
   * is never run again. On SQLite, River retries beginning while another
   * connection holds the write lock; on Postgres, a failure to lease a
   * connection or begin rejects at once. A failed commit rejects without
   * claiming whether the database kept the changes.
   *
   * On SQLite, River's transaction holds the database's write lock, so
   * `callback` may await only promises (other River work and statements),
   * not I/O or timers, as for insert middleware.
   */
  transaction<Result>(
    callback: (tx: Transaction) => PromiseLike<Result> | Result,
    options?: {
      readonly signal?: AbortSignal;
      readonly tx?: Transaction;
    }
  ): Promise<Result>;

  /**
   * Delete one batch of finalized jobs with River's job cleaner statement,
   * resolving with how many it deleted, so an extension's own cleaner
   * passes, such as per-queue retention, delete exactly what River's
   * would. Like River for Go's driver `JobDeleteBefore`, the queue filters
   * apply before the limit, so jobs they keep never use up a batch. It
   * needs no leadership term.
   *
   * With `tx`, it runs in that transaction, otherwise in a transaction of
   * its own. It has no timeout or cancellation; to bound the wait for a
   * connection, run it in {@link PilotDatabase.transaction} with a signal.
   */
  deleteFinalizedJobs(
    params: FinalizedJobDeleteParams,
    options?: { readonly tx?: Transaction }
  ): Promise<number>;

  /**
   * Read claimed jobs in `tx`, decoding them as River's own claim does: a
   * row that can't be fully decoded is returned with its error in
   * `decodeErrors`. Rejects when an ID is repeated or has no row.
   */
  loadClaimed(
    ids: readonly bigint[],
    options: { readonly tx: Transaction }
  ): Promise<JobClaimResult>;

  /**
   * Send River notifications on `topic` in `tx`. They reach listeners only
   * once `tx` commits, and never if it rolls back.
   */
  notify(
    topic: "control" | "insert",
    payloads: readonly string[],
    options: { readonly tx: Transaction }
  ): Promise<void>;
}

/** What every interceptor that runs in a transaction receives. */
export interface PilotTransactionContext<Transaction> {
  readonly database: PilotDatabase<Transaction>;
  /** Aborts when River abandons the operation. */
  readonly signal: AbortSignal;
  /**
   * The transaction the operation runs in: River's own, or the caller's.
   * `next` runs River's standard operation in it too. Its owner commits or
   * rolls it back, never the interceptor, which keeps the operation's
   * related writes in it.
   */
  readonly tx: Transaction;
}

/** An intercepted insertion. */
export interface PilotInsertContext<
  Transaction,
> extends PilotTransactionContext<Transaction> {
  readonly operation: "insert" | "insertMany";
  /**
   * Each prepared row's arguments as JSON text from before the client's
   * argument transforms rewrote them, in the order of `params`. A
   * transform, such as one that encrypts arguments, runs before the
   * interceptor, so `params` carries its output, which is what River
   * stores. An interceptor that derives values from the arguments, such as
   * keys or routing, reads them here instead. A row no transform changed
   * has its `encodedArgs` here, and a row from
   * {@link PilotHost.insertPrepared} has the `encodedArgs` it was given.
   */
  readonly originalEncodedArgs: readonly string[];
  /** Prepared rows, in order. */
  readonly params: readonly JobInsertParams[];
}

/** Rows that replace an insertion's prepared rows, one for one. */
export interface PilotInsertReplacement {
  /**
   * The replacement rows. Each row's `encodedArgs` is what River stores and
   * returns, whatever its `args`. Insert hooks and middleware still see the
   * requests as prepared before the replacement.
   */
  readonly params: readonly JobInsertParams[];
}

/** An intercepted completion batch. */
export interface PilotCompleteContext<
  Transaction,
> extends PilotTransactionContext<Transaction> {
  readonly commands: readonly JobCompletionCommand[];
}

/** An intercepted cancellation or retry of one job. */
export interface PilotJobContext<
  Transaction,
> extends PilotTransactionContext<Transaction> {
  readonly id: bigint;
}

/**
 * The rescuer's read of one page of stuck jobs. It runs in no transaction;
 * River's standard read fences it by `leader` itself.
 */
export interface PilotStuckContext<Transaction> {
  readonly afterId: bigint;
  readonly attemptedBefore: Temporal.Instant;
  readonly database: PilotDatabase<Transaction>;
  readonly leader: LeaderTerm;
  readonly limit: number;
  /** Aborts when the read's timeout elapses or the leadership term ends. */
  readonly signal: AbortSignal;
  /**
   * The read's timeout in milliseconds, or `null` for none. River's standard
   * read also sets it as the statement's timeout on Postgres; a read the
   * pilot runs itself should do the same.
   */
  readonly timeoutMs: number | null;
}

/** The rescuer's update of one page of stuck jobs. */
export interface PilotRescueContext<
  Transaction,
> extends PilotTransactionContext<Transaction> {
  readonly attemptedBefore: Temporal.Instant;
  readonly jobs: readonly RuntimeJobRescue[];
  readonly leader: LeaderTerm;
}

/**
 * Operations a pilot wraps, Koa-style, around River's standard operation,
 * which `next` runs. `next` is bound to the context's transaction.
 *
 * `insert`, `complete`, `cancel`, and `retry` must call `next` exactly once
 * and resolve with exactly what it resolved with; they add effects in the
 * same transaction. `insert` alone may pass replacement rows to `next`, one
 * for each prepared row, in order. `getStuck` and `rescue` may instead
 * replace River's operation: they call `next` at most once, and resolve with
 * its result when they do.
 *
 * River awaits `next` before settling the operation, even when the
 * interceptor doesn't. A second call, or one after the interceptor settled,
 * rejects. Any violation, and any rejection, fails the operation and rolls
 * its transaction back with an `ExtensionError`. River freezes result
 * lists and checks the rows' identities, but doesn't copy rows: an
 * interceptor must not change their contents, which callers see.
 */
export interface PilotInterceptors<Transaction> {
  cancel?(
    context: PilotJobContext<Transaction>,
    next: () => Promise<JobRow | null>
  ): Promise<JobRow | null>;
  complete?(
    context: PilotCompleteContext<Transaction>,
    next: () => Promise<readonly JobCompletionResult[]>
  ): Promise<readonly JobCompletionResult[]>;
  getStuck?(
    context: PilotStuckContext<Transaction>,
    next: () => Promise<readonly JobRow[]>
  ): Promise<readonly JobRow[]>;
  insert?(
    context: PilotInsertContext<Transaction>,
    next: (
      replacement?: PilotInsertReplacement
    ) => Promise<readonly DriverInsertResult[]>
  ): Promise<readonly DriverInsertResult[]>;
  rescue?(
    context: PilotRescueContext<Transaction>,
    next: () => Promise<number>
  ): Promise<number>;
  retry?(
    context: PilotJobContext<Transaction>,
    next: () => Promise<JobRow | null>
  ): Promise<JobRow | null>;
}

/**
 * Queue configuration keys a pilot owns. River rejects queue keys that
 * neither it nor the pilot owns.
 */
export interface PilotQueueOptions<Settings> {
  /** The owned keys. They must not be River's own queue keys. */
  readonly keys: readonly string[];
  /**
   * Validate one queue's owned keys, those present in `config`, and return
   * its settings. It runs synchronously for every configured queue, and for
   * every `addQueue` and `updateQueue`, before River changes anything. Throw
   * a `ValidationError` to reject the configuration.
   */
  parse(queue: string, config: Readonly<Record<string, unknown>>): Settings;
}

/**
 * The one companion attached to a client. Every member is optional. `init`
 * runs with the pilot as `this`, and interceptors with `intercept`.
 */
export interface Pilot<
  Transaction,
  QueueSettings = unknown,
  ClientType extends Client<Transaction> = Client<Transaction>,
> {
  /**
   * Called once, synchronously, while the client is constructed. It must
   * not perform I/O or call the client: `host.client` is usable only once
   * construction returns.
   */
  init?(host: PilotHost<Transaction, ClientType>): void;
  readonly intercept?: PilotInterceptors<Transaction>;
  readonly queueOptions?: PilotQueueOptions<QueueSettings>;

  /**
   * Start the producer session of one queue generation, once River has
   * persisted the queue and before its first claim. River owns the
   * session's lifetime: it claims through it, reports configuration
   * changes and finished jobs to it, keeps it alive, and shuts it down once
   * the queue drained. A rejection fails the queue's start, and the pilot
   * must first release whatever it allocated.
   */
  startProducer?(
    context: ProducerStartContext<Transaction, QueueSettings>
  ): Promise<PilotProducer<Transaction, QueueSettings>>;

  /**
   * The most background completion batches this client persists at once.
   * It counts local batches, not database connections, and never limits a
   * worker's transactional completion. A batch waiting its turn doesn't
   * spend its timeout or retries, and a stop still persists every pending
   * completion. Default: no limit.
   */
  readonly completionConcurrency?: number;
  /** Queues River's job cleaner leaves alone, for the pilot to clean. */
  readonly jobCleanerQueuesExcluded?: readonly string[];
  /**
   * Durable storage for periodic job schedules, used instead of a periodic
   * job store plugin. River runs `upsertMany` in the transaction inserting
   * the periodic jobs, and passes it a native handle, as it does to
   * interceptors.
   */
  readonly periodicJobs?: PeriodicJobStore<Transaction>;

  /**
   * Services River runs for as long as the runtime runs, listed once per
   * run and started before any queue claims. See {@link PilotService}.
   * Services run in the background, so a service can't fail the client's
   * `start()`; one that keeps failing is logged and restarted instead.
   */
  services?(): readonly PilotService<void>[];

  /**
   * Services River runs while this client leads maintenance, once per
   * leadership term, with the term and a signal that aborts when the term
   * ends. A new term's services start only once the previous term's
   * settled. Listed once per run.
   */
  maintenanceServices?(): readonly PilotService<LeaderTerm>[];
}

/**
 * A background service River supervises. `run` should resolve only once
 * `signal` aborted. River restarts a run that rejects, or resolves before
 * then, after capped exponential backoff with jitter, which resets after a
 * long healthy run; it waits for a run to settle before starting another,
 * and stops restarting once `signal` aborts.
 */
export interface PilotService<Term> {
  /** Names the service in River's logs. */
  readonly name: string;
  run(context: {
    readonly signal: AbortSignal;
    readonly term: Term;
  }): Promise<void>;
}

/** One queue generation's configuration, replaced as a whole. */
export interface ProducerConfiguration<Settings = unknown> {
  /** The most jobs of the queue this client works at once. */
  readonly maxWorkers: number;
  /**
   * The queue's metadata as its database stores it, such as `{"retries":
   * 1.0}`, for a pilot that decodes it more strictly than River's parsed
   * `queue.metadata`, whose number literals `1.0` and `1e2` read as plain
   * numbers.
   */
  readonly metadataText: string;
  /** The persisted queue, including its metadata. */
  readonly queue: QueueRow;
  /** The pilot's own settings, parsed by its `queueOptions`. */
  readonly settings: Settings;
}

/** What {@link Pilot.startProducer} receives. */
export interface ProducerStartContext<
  Transaction,
  Settings = unknown,
> extends ProducerConfiguration<Settings> {
  readonly clientId: string;
  readonly database: PilotDatabase<Transaction>;
  /** Aborts when the runtime stops while the producer starts. */
  readonly signal: AbortSignal;
}

/**
 * River's standard claim, which a producer session's claim may call once
 * in its transaction `tx`.
 */
export type ProducerClaimNext<Transaction> = (options: {
  readonly tx: Transaction;
}) => Promise<JobClaimResult>;

/** What {@link PilotProducer.keepAlive} receives. */
export interface ProducerKeepAliveContext {
  /** Aborts when the report times out, after 10 s, or reports stop. */
  readonly signal: AbortSignal;
  /** Sessions that haven't reported since this time are stale. */
  readonly staleBefore: Temporal.Instant;
}

/** What {@link PilotProducer.shutdown} receives. */
export interface ProducerShutdownContext {
  /** Aborts at the attempt's deadline. */
  readonly signal: AbortSignal;
}

/** One claim of a producer session. */
export interface ProducerClaimContext<Transaction> {
  /** The client ID claimed jobs must record as their attempt's owner. */
  readonly attemptedBy: string;
  readonly database: PilotDatabase<Transaction>;
  /**
   * The kinds the claim may return, sorted, or empty for every kind. A
   * client with `fetchOnlyKnownKinds` passes the kinds it has workers for,
   * like River for Go's `JobGetAvailableParams.Kind`.
   */
  readonly kinds: readonly string[];
  /** The most jobs the claim may return. */
  readonly limit: number;
  readonly queue: string;
  /**
   * Aborts once River stops claiming the queue. It ends retries and
   * backoff; a claim that already committed must still be returned.
   */
  readonly retrySignal: AbortSignal;
  /** Aborts when River abandons the claim's work entirely. */
  readonly signal: AbortSignal;
}

/**
 * A pilot's producer session for one queue generation. Every member is
 * optional; River calls them with the session as `this`.
 *
 * At most one claim and one keep-alive run at a time, and they may overlap
 * each other; `jobFinished` may run during either. Configuration changes
 * run between claims. Once River starts draining the queue it starts no
 * new claim or configuration change, and after `shutdown` settles it calls
 * nothing more.
 *
 * River waits for every call it starts to settle, so a `keepAlive` or
 * `shutdown` that ignores its aborted signal and never settles stalls the
 * client's `stop()`.
 */
export interface PilotProducer<Transaction, Settings = unknown> {
  /**
   * Claim jobs for the queue. `next({ tx })` runs River's standard claim in
   * the pilot's transaction `tx`. The claim may instead select jobs itself,
   * reading them with `database.loadClaimed`, and calls `next` at most
   * once; when it does, it resolves with `next`'s result.
   *
   * Resolve only with rows whose claim committed, and record them before
   * resolving. River checks them before working any: each must be running,
   * in this queue, owned by `attemptedBy`, on an attempt of at least 1,
   * listed once, not already worked here, and no more than `limit`. A
   * claim that breaks those rules stops the runtime; its rows are left to
   * the rescuer. A rejection is retried after backoff like River's own
   * claim failures, so the pilot must undo reservations of a claim that
   * didn't commit.
   */
  claim?(
    context: ProducerClaimContext<Transaction>,
    next: ProducerClaimNext<Transaction>
  ): Promise<JobClaimResult>;

  /**
   * Validate and adopt a new configuration, synchronously and without I/O.
   * Throw to reject it: River keeps the previous configuration, and rejects
   * an `updateQueue` that asked for it or logs a persisted change.
   */
  configurationChanged?(configuration: ProducerConfiguration<Settings>): void;

  /**
   * One claimed job's attempt ended and its outcome went to River's
   * completer, which may not have persisted it yet. `job` is the row as
   * claimed. Called exactly once for each job the session claimed and
   * River accepted, including jobs River couldn't decode or work.
   */
  jobFinished?(job: JobRow): void;

  /**
   * Report the session as alive, after an initial jitter and then at the
   * client's producer report interval, including while the queue drains.
   * Reports keep a fixed rate and never overlap: a slow report delays the
   * next one. A rejection is logged and the next report runs on schedule.
   */
  keepAlive?(context: ProducerKeepAliveContext): Promise<void>;

  /**
   * Release the session once its queue drained and its reports stopped.
   * River tries up to four times, one after another, aborting `signal`
   * after 100 ms, 500 ms, 2.5 s, and 12.5 s, and logs the failure when all
   * four fail.
   */
  shutdown?(context: ProducerShutdownContext): Promise<void>;
}

/** What a peer claim's callback receives. */
export interface PeerClaimContext<Transaction> {
  /**
   * The claiming attempt's signal. It aborts when the attempt is cancelled,
   * by a hard stop, its job's cancellation, or its timeout, and once the
   * attempt finished; River then rolls the claim back if it hasn't
   * committed. A graceful stop doesn't abort it: a coordinator still
   * running keeps claiming, and the stop waits for the peers it claims.
   */
  readonly signal: AbortSignal;
  /** The transaction the claim commits in, once the callback resolves. */
  readonly tx: Transaction;
}

/** The outcome of one peer, for {@link PilotAttempts.complete}. */
export interface PeerOutcome {
  /**
   * The peer, as {@link PilotAttempts.claim} returned it. Its ID, attempt,
   * and attempting client identify it; River persists the row it tracks.
   */
  readonly job: JobRow;
  readonly result: WorkAttemptResult;
}

/**
 * Peer attempts: jobs a running attempt of this client, their coordinator,
 * works together with its own job, such as a group of related jobs handled
 * in one go. River tracks each peer under its coordinator from the claim's
 * commit until its outcome persists. Peers don't take the queue's worker
 * slots, and their producer session never hears of them. River doesn't
 * cancel a peer remotely; cancelling the coordinator's job reaches peers
 * only through the coordinator's signal.
 *
 * `attempt` is the coordinator's context, as its handler or middleware
 * received it. Both methods reject once the coordinator's attempt ended,
 * including while its own outcome persists, and a claim also rejects once
 * the attempt is cancelled. A graceful stop ends neither: a running
 * coordinator keeps claiming and completing peers, and the stop resolves
 * only after they have outcomes. When the attempt ends, River waits for
 * the calls it accepted, then completes every peer still without an
 * outcome: it interrupts them only when the runtime stopped or cancelled
 * its work (the attempt's abort reason is a `LifecycleError`), and
 * otherwise, including after the coordinator's job was cancelled or timed
 * out, fails them with an `ExtensionError`, so the retry policy applies.
 */
export interface PilotAttempts<Transaction> {
  /**
   * Claim peers of `attempt`. River begins a transaction and calls `run`
   * with it; `run` moves the peers to `running` for this client with its
   * own statements in that transaction and resolves with them read back by
   * {@link PilotDatabase.loadClaimed}. River checks them before committing
   * and rolls back, rejecting with an `ExtensionError`, unless each is
   * running, on an attempt of this client, listed once, and neither
   * `attempt`'s own job nor a job this client already works as an attempt
   * or peer, nor one this coordinator already finished at that attempt.
   *
   * Resolves with the peers after the client's argument transforms. A peer
   * River can't decode or transform isn't returned: River completes it as
   * a failed attempt.
   */
  claim(
    attempt: WorkAttemptContext | WorkContext,
    run: (context: PeerClaimContext<Transaction>) => Promise<JobClaimResult>
  ): Promise<readonly JobRow[]>;

  /**
   * Complete peers of `attempt` through River's completion pipeline, like
   * the coordinator's own outcome: the error handler runs for failures,
   * metadata the coordinator set is added, and the pilot's `complete`
   * interceptor sees the persistence. Resolves once every outcome
   * persisted.
   *
   * River accepts the outcomes all or none: each job must be a peer of
   * `attempt` without an outcome yet, listed once. An outcome that fails
   * before River's completer accepts it, such as an invalid result or one
   * whose output is too large, leaves its peer without one, and the call
   * rejects; once accepted, the outcome is River's, and its peer is never
   * completed again.
   */
  complete(
    attempt: WorkAttemptContext | WorkContext,
    outcomes: readonly PeerOutcome[]
  ): Promise<void>;
}

/**
 * A job's stored fields, for {@link PilotHost.insertPrepared}. Its
 * arguments are `encodedArgs`, any JSON text, stored as given.
 */
export type PreparedInsertParams = Omit<JobInsertParams, "args">;

/** What River gives a pilot in {@link Pilot.init}. */
export interface PilotHost<
  Transaction,
  ClientType extends Client<Transaction> = Client<Transaction>,
> {
  /** Peer attempts of this client's running attempts. */
  readonly attempts: PilotAttempts<Transaction>;
  /**
   * The client, usable once its construction returns. It is the final
   * client object, such as a companion package's subclass of
   * `PilotClient`, which the pilot names as `ClientType`: River creates the
   * pilot before that subclass exists, so it can't check the type.
   */
  readonly client: ClientType;
  readonly clientId: string;
  readonly database: PilotDatabase<Transaction>;
  readonly logger: Logger;
  /** How often producers report their queues. */
  readonly producerReportInterval: Temporal.Duration;
  /** The job kinds this client has workers for. */
  readonly workerKinds: readonly string[];

  /**
   * Insert rows already prepared, such as jobs taken out of River that go
   * back in with their stored fields, like an ordinary insertion of them:
   * the client's insert metadata and argument transforms, insert
   * middleware, and insert hooks each run once, then the pilot's insert
   * interceptor, and inserts notify as usual.
   *
   * Transforms, middleware, and hooks see the stored arguments, read from
   * `encodedArgs`, and no job definition. What they return is stored, so a
   * transform keeps a row as it is by returning it unchanged, such as
   * arguments it already transformed on an earlier insertion:
   * `encodedArgs` passes through them byte for byte. Stored arguments that
   * aren't a JSON object, which other River clients may insert, are kept
   * as they are: argument transforms don't run for them, and metadata
   * transforms, middleware, and hooks see empty arguments in their place.
   * The unique key and states, creation time, and schedule are kept, and
   * no unique key is computed.
   */
  insertPrepared(
    params: readonly PreparedInsertParams[],
    options?: {
      readonly signal?: AbortSignal;
      readonly tx?: Transaction;
    }
  ): Promise<readonly DriverInsertResult[]>;

  /**
   * Wake this client's producers for inserted jobs another transaction
   * owner has committed. It's only a local optimization: producers find the
   * jobs anyway.
   */
  notifyCommitted(results: readonly DriverInsertResult[]): void;
}

/** Creates the pilot of one client from its driver's database. */
export type PilotFactory<
  Transaction,
  QueueSettings = unknown,
  ClientType extends Client<Transaction> = Client<Transaction>,
> = (
  database: PilotDatabase<Transaction>
) => Pilot<Transaction, QueueSettings, ClientType>;

/**
 * The connection a driver's migrations run on: a node-postgres pool or
 * connected client with River's schema (undefined for the `search_path`), or
 * a `node:sqlite` database.
 */
export type DriverMigrationTarget =
  | { readonly client: object; readonly schema: string | undefined }
  | {
      readonly database: object;
      /**
       * Run one synchronous migration attempt on `database` under the
       * driver's lock, retrying the whole attempt while another connection
       * holds SQLite's write lock. The attempt leaves no transaction open
       * when it throws.
       */
      readonly run?: <T>(attempt: (database: object) => T) => Promise<T>;
    }
  | { readonly pool: object; readonly schema: string | undefined };

/**
 * What a first-party driver registers about one driver instance with
 * {@link registerDriver}.
 */
export interface DriverRecord<Transaction> {
  /** The backend's name, such as `postgres` or `sqlite`, for errors. */
  readonly backend: string;
  /** Whether the driver can run workers, or only insert. */
  readonly capability: "insert" | "runtime";
  /** The database a pilot uses, when the driver supports pilots. */
  readonly database?: PilotDatabase<Transaction>;
  /**
   * The connection and schema `createMigrator` migrates when given this
   * driver, when the driver supports migrations.
   */
  readonly migration?: DriverMigrationTarget;
  /**
   * The operations River runs through the driver, kept off the public
   * driver object. A runtime driver's must implement every runtime
   * operation.
   */
  readonly operations: InsertDriver<Transaction, DriverCapability>;
}
