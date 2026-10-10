import { randomBytes, randomUUID } from "node:crypto";
import { DatabaseSync, type DatabaseSyncOptions } from "node:sqlite";
import { setTimeout as sleep } from "node:timers/promises";
import {
  LifecycleError,
  RiverError,
  toJsonObject,
  TransactionScopeError,
} from "riverqueue";
import type {
  DriverInsertResult,
  DriverRecord,
  InsertDriver,
  InsertDriverOptions,
  JobInsertParams,
  JobClaimOptions,
  JobClaimParams,
  JobClaimResult,
  JobCompletionCommand,
  JobCompletionResult,
  JobDeleteManyParams,
  JobDeleteResult,
  JobListParams,
  JobUpdateParams,
  QueueListParams,
  QueueRow,
  QueueUpdateParams,
  RuntimeJobCleanupParams,
  RuntimeJobRescue,
  RuntimeLeader,
  RuntimeMaintenanceBatch,
  RuntimeNotification,
  RuntimeScheduleParams,
  RuntimeWaitOptions,
  PilotDatabase,
  RuntimeDriver,
} from "riverqueue/unstable-driver";
import {
  durationToMilliseconds,
  LinkedAbortSignal,
  queueMetadataUpdate,
  registerDriver,
} from "riverqueue/unstable-driver";

import {
  JOB_COLUMNS,
  QUEUE_COLUMNS,
  decodeJobRow,
  decodeJobRowPartial,
  decodeQueueRow,
  encodeEncodedJson,
  encodeJson,
  encodeUniqueStates,
  invalidJsonTextSql,
  sqliteTimestamp,
  sqliteTimestampOrNull,
  validateInt64,
  validateLookupName,
  validateName,
  validateSmallInteger,
} from "./codecs.js";
import { FifoLock, retryBusy, type BusyRetryPolicy } from "./coordination.js";
import { eventLoopTurns, retainTurnCounter } from "./strict.js";
import {
  backendMismatchError,
  configurationError,
  databaseError,
  invalidInputError,
  invalidRowError,
  isRetryableSqliteError,
  isSqliteError,
  SQLITE_BACKEND,
} from "./errors.js";
import {
  claimJobs,
  cleanupJobs,
  completeJobs,
  leaderAttemptElect,
  leaderAttemptReelect,
  leaderGet,
  leaderResign,
  listJobs,
  listJobsMetadataPage,
  loadClaimedJobs,
  notificationCleanup,
  notificationLastId,
  notificationPoll,
  queueDeleteExpired,
  rescueJobs,
  resultStatement,
  scheduleJobs,
  stuckJobs,
  updateJob,
} from "./operations.js";
import type {
  SqliteCancelResult,
  SqliteCleanupJobsParams,
  SqliteDeleteResult,
  SqliteDriverOptions,
  SqliteInsertJobParams,
  SqliteInsertResult,
  SqliteJobRow,
  SqliteJsonObject,
  SqliteLeader,
  SqliteNotification,
  SqliteOperationOptions,
  SqliteQueueRow,
  SqliteRescueJobParams,
  SqliteRetryResult,
  SqliteRiverScope,
  SqliteScheduleResult,
} from "./types.js";
import {
  databaseKey,
  fileKey,
  openFrames,
  registerDatabase,
  riverFrame,
  runInFrame,
  sameDatabase,
  type TransactionFrame,
} from "./scope.js";

const BUSY_TIMEOUT_MS_DEFAULT = 5_000;
/** Notifications a subscription reads per poll, like Go's SQLite listener. */
const NOTIFICATION_BATCH_SIZE = 256;
const NOTIFICATION_TOPIC_CONTROL = "river_control";
const NOTIFICATION_TOPIC_INSERT = "river_insert";
const UNIQUE_NONCE_KEY = "river:unique_nonce";
const NON_RUNNING_JOB_STATES = Object.freeze([
  "available",
  "cancelled",
  "completed",
  "discarded",
  "pending",
  "retryable",
  "scheduled",
] as const);
const UNIQUE_STATE_MATCH_SQL = `
  CASE state
    WHEN 'available' THEN unique_states & (1 << 0)
    WHEN 'cancelled' THEN unique_states & (1 << 1)
    WHEN 'completed' THEN unique_states & (1 << 2)
    WHEN 'discarded' THEN unique_states & (1 << 3)
    WHEN 'pending' THEN unique_states & (1 << 4)
    WHEN 'retryable' THEN unique_states & (1 << 5)
    WHEN 'running' THEN unique_states & (1 << 6)
    WHEN 'scheduled' THEN unique_states & (1 << 7)
    ELSE 0
  END >= 1`;
const INSERT_COLUMNS_SQL = `
  id, args, attempt, attempted_at, attempted_by, created_at, errors,
  finalized_at, kind, max_attempts, metadata, priority, queue,
  scheduled_at, state, tags, unique_key, unique_states`;
// Like River for Go, a row without a creation or scheduled time takes the
// database's current time, the same for both within one statement.
const INSERT_VALUES_SQL = `
  ?, jsonb(?), ?, ?, jsonb(?), coalesce(?, datetime('now', 'subsec')),
  jsonb(?), ?, ?, ?, jsonb(?), ?, ?, coalesce(?, datetime('now', 'subsec')),
  ?, jsonb(?), ?, ?`;

/**
 * Deterministic clock and sleep used by tests. Not part of the public API.
 * @internal
 */
export const SQLITE_DRIVER_TEST_HOOKS = Symbol.for(
  "riverqueue.sqlite.driver.test_hooks"
);

/**
 * Test-only driver options, passed under {@link SQLITE_DRIVER_TEST_HOOKS}.
 * The key is a registered symbol so River's and its first-party
 * extensions' test suites can reach it without a public export.
 * @internal
 */
interface SqliteDriverTestHooks {
  readonly now?: () => number;
  readonly sleep?: (milliseconds: number) => Promise<void>;
  /**
   * Fail every River transaction that stays open across a turn of the
   * event loop, deterministically, unlike the default probe. It hooks every
   * async resource in the process while the driver is open, so it is only
   * for tests and conformance runs.
   */
  readonly strictLockWindow?: boolean;
}

/** The memdb URI `SqliteDriver.memory()` passes to its constructor. */
const MEMORY_LOCATION = Symbol("riverqueue.sqlite.memory_location");

/**
 * One transaction River owns on its private connection. It begins lazily,
 * with `BEGIN IMMEDIATE` at its first River statement, so work an insert
 * middleware does before calling `next()` holds no lock. The object itself
 * is the opaque `tx` passed to an operation scope's callback.
 */
class OwnedScope {
  /** Settles once `BEGIN IMMEDIATE` succeeded or failed. */
  beginning: Promise<void> | null = null;
  readonly driver: SqliteRuntime;
  /** Why the probe rolled the transaction back, once it has. */
  failure: TransactionScopeError | null = null;
  readonly frame: TransactionFrame;
  /** Whether the transaction holds River's connection lock. */
  holdsLock = false;
  /**
   * Aborts River calls from inside this transaction's async context that
   * wait for the connection lock, once the transaction takes it: they would
   * wait for the transaction, which waits for them.
   */
  readonly lockWaiters = new Set<() => void>();
  /**
   * How many pilot transactions are running in this transaction. While any
   * is, River's private connection passed as `{ tx }` from inside the
   * transaction stands for it.
   */
  pilotDepth = 0;
  /** Fires at the event loop's next turn to check the transaction ended. */
  probe: NodeJS.Immediate | null = null;
  /** Event loop turns counted when the transaction began, in strict mode. */
  turnsAtBegin = 0;
  /** Releases the connection lock held while the transaction is open. */
  release: (() => void) | null = null;
  state:
    "beginning" | "committing" | "ended" | "idle" | "open" | "rolled_back" =
    "idle";

  constructor(driver: SqliteRuntime, key: string) {
    this.driver = driver;
    this.frame = riverFrame(driver, key, this, () => this.holdsLock);
  }
}

/** SQLite bindings for one normalized job insertion. */
interface InsertValues {
  readonly args: string;
  readonly attempt: number;
  readonly attemptedAt: string | null;
  readonly attemptedBy: string | null;
  /** Null to use the database's current time. */
  readonly createdAt: string | null;
  readonly errors: string | null;
  readonly finalizedAt: string | null;
  readonly id: bigint | null;
  readonly kind: string;
  readonly maxAttempts: number;
  readonly metadata: string;
  readonly priority: number;
  readonly queue: string;
  /** Null to use the database's current time. */
  readonly scheduledAt: string | null;
  readonly state: string;
  readonly tags: string;
  readonly uniqueKey: Uint8Array | null;
  readonly uniqueNonce: string;
  readonly uniqueStates: bigint | null;
}

/**
 * Complete SQLite storage backend on Node's built-in `node:sqlite`.
 *
 * Like River for Go, River runs on a connection of its own. The driver
 * opens a private `DatabaseSync` on the application's database file (in WAL
 * mode, with a zero busy timeout) and never hands it out, so application
 * statements can't join River's transactions, and River's can't join the
 * application's. When another connection or process holds SQLite's write
 * lock, River retries with an asynchronous backoff so the event loop keeps
 * running.
 *
 * Pass `{ tx }` to run a River operation in an application transaction: any
 * `DatabaseSync` on the same database with a transaction open, such as one
 * begun with {@link transaction}. River runs its statements directly in
 * that transaction, opening no savepoint, and never ends it. When a River
 * call fails, statements it already ran stay in the transaction, so roll
 * the transaction back; to recover and continue it instead, wrap the call
 * in a savepoint of your own.
 *
 * An insertion without `{ tx }` runs in a transaction River owns, begun at
 * its first statement, which holds SQLite's write lock until it commits.
 * Insert middleware and hooks run inside it, so on SQLite they must not
 * await I/O after `next()`. When River's transaction is still open at the
 * event loop's next turn, River rolls it back, releasing the lock, and fails
 * the operation with a `TransactionScopeError`. That check finds most such
 * mistakes, including all slow I/O and every insertion started from an I/O
 * callback such as an HTTP handler, but not fast local I/O awaited in an
 * insertion started from a timer or `setImmediate` callback.
 *
 * Statements block the event loop while they run. Use a dedicated Node
 * process for a heavily loaded worker so database work can't stall an HTTP
 * server's event loop.
 */
export class SqliteDriver implements Disposable {
  /**
   * Type-only marker: a full runtime driver whose transactions are
   * application handles with a transaction open.
   */
  declare readonly "~river"?: {
    readonly capability: "runtime";
    readonly transaction: DatabaseSync;
  };

  readonly #runtime: SqliteRuntime;

  /**
   * Create a driver for the database `database` is open on. River opens its
   * own connection to the same file and never uses or closes `database`.
   *
   * An in-memory database can't be shared between connections this way:
   * use {@link SqliteDriver.memory} instead.
   *
   * @throws {ConfigurationError} when `database` is in memory or closed.
   */
  constructor(database: DatabaseSync, options: SqliteDriverOptions = {}) {
    this.#runtime = new SqliteRuntime(database, options);
    registerDriver<DatabaseSync>(this, this.#runtime.driverRecord());
  }

  /**
   * Create a driver for a new, empty in-memory database.
   *
   * A `:memory:` database belongs to one connection, so River can't open
   * its own connection to an application's. This creates a uniquely named
   * in-memory database (SQLite's `memdb` VFS) that River's connection and
   * application handles share. Get handles with {@link connect}. The
   * database lives until the driver and every handle from `connect()` are
   * closed.
   */
  static memory(options: SqliteDriverOptions = {}): SqliteDriver {
    return memoryDatabase(
      (database, memoryOptions) => new SqliteDriver(database, memoryOptions),
      options
    );
  }

  /**
   * Close River's private connection. Stop every client using the driver
   * first. Handles from {@link connect} and the one passed to the
   * constructor stay open. Closing twice does nothing.
   */
  close(): void {
    this.#runtime.close();
  }

  /**
   * Open another application handle on this driver's database. The caller
   * owns and closes it.
   *
   * `options` are `node:sqlite`'s. The busy `timeout` defaults to zero, so a
   * statement that meets another connection's write lock fails at once
   * instead of blocking the event loop. Pass a nonzero `timeout` when other
   * processes write the database (see the package README).
   */
  connect(options: DatabaseSyncOptions = {}): DatabaseSync {
    return this.#runtime.connect(options);
  }

  /** Same as {@link close}, for `using` declarations. */
  [Symbol.dispose](): void {
    this.close();
  }
}

/**
 * Create a driver on a new uniquely named in-memory database, whose first
 * application handle the driver owns and closes.
 */
function memoryDatabase<Driver>(
  create: (database: DatabaseSync, options: SqliteDriverOptions) => Driver,
  options: SqliteDriverOptions
): Driver {
  const location = `file:/river-${randomUUID()}?vfs=memdb`;
  const database = new DatabaseSync(location, { timeout: 0 });
  try {
    return create(database, {
      ...options,
      [MEMORY_LOCATION]: location,
    } as SqliteDriverOptions);
  } catch (error: unknown) {
    database.close();
    throw error;
  }
}

/**
 * @internal A registered driver whose operations are callable, for this
 * package's tests.
 */
export function testSqliteDriver(
  database: DatabaseSync,
  options: SqliteDriverOptions = {}
): SqliteRuntime {
  const runtime = new SqliteRuntime(database, options);
  registerDriver<DatabaseSync>(runtime, runtime.driverRecord());
  return runtime;
}

/** @internal Like {@link testSqliteDriver}, on a new in-memory database. */
export function testSqliteMemory(
  options: SqliteDriverOptions = {}
): SqliteRuntime {
  return memoryDatabase(testSqliteDriver, options);
}

/**
 * @internal The operations behind a {@link SqliteDriver}, which River
 * reaches through its private driver registry.
 */
export class SqliteRuntime implements RuntimeDriver<
  DatabaseSync | SqliteRiverScope
> {
  declare readonly "~river"?: {
    readonly capability: "runtime";
    readonly transaction: DatabaseSync;
  };

  /** @internal */
  readonly backend = "sqlite";
  /** @internal */
  readonly capabilities = Object.freeze({
    insert: true,
    leadership: true,
    maintenance: true,
    notifications: true,
    runtime: true,
    transactions: true,
  });
  /** The application handle the driver was created with. */
  readonly #application: DatabaseSync;
  readonly #busyPolicy: BusyRetryPolicy;
  #closed = false;
  /** River's private connection. */
  readonly #connection: DatabaseSync;
  /** Application handles River opened or was given, for diagnostics. */
  readonly #handles = new Set<WeakRef<DatabaseSync>>();
  /** Identifies the database, as `databaseKey` does for its handles. */
  readonly #key: string;
  /** The path or URI River opens connections to. */
  readonly #location: string;
  /** Stops strict mode's turn counter, when this driver started it. */
  readonly #releaseTurnCounter: (() => void) | null = null;
  /** Serializes River's operations on its private connection. */
  readonly #lock = new FifoLock();
  /** The River transaction holding {@link #lock}, while one does. */
  #lockHolder: OwnedScope | null = null;
  /** Whether `close()` also closes {@link #application}. */
  readonly #ownsApplication: boolean;
  /**
   * Whether the switch to WAL still has to happen, because the database was
   * busy when the driver was constructed.
   */
  #walPending = false;

  /**
   * Create a driver for the database `database` is open on. River opens its
   * own connection to the same file and never uses or closes `database`.
   *
   * An in-memory database can't be shared between connections this way:
   * use {@link SqliteDriver.memory} instead.
   *
   * @throws {ConfigurationError} when `database` is in memory or closed.
   */
  constructor(database: DatabaseSync, options: SqliteDriverOptions = {}) {
    const internal = options as SqliteDriverOptions & {
      readonly [MEMORY_LOCATION]?: string;
      readonly [SQLITE_DRIVER_TEST_HOOKS]?: SqliteDriverTestHooks;
    };
    if (!(database instanceof DatabaseSync) || !database.isOpen) {
      throw configurationError(
        "construct",
        "SqliteDriver requires an open node:sqlite DatabaseSync"
      );
    }
    const memoryLocation = internal[MEMORY_LOCATION];
    const location = memoryLocation ?? database.location();
    if (location === null) {
      throw configurationError(
        "construct",
        "SqliteDriver opens its own connection to the application's " +
          "database, which an in-memory database can't share; use " +
          "SqliteDriver.memory() and open application handles with " +
          "driver.connect()"
      );
    }
    const busyTimeoutMs = validateSmallInteger(
      options.busyTimeout === undefined
        ? BUSY_TIMEOUT_MS_DEFAULT
        : durationToMilliseconds("busyTimeout", options.busyTimeout, {
            allowZero: true,
          }),
      "busyTimeout",
      0,
      2_147_483_647
    );
    const hooks = internal[SQLITE_DRIVER_TEST_HOOKS];
    if (hooks?.strictLockWindow === true) {
      this.#releaseTurnCounter = retainTurnCounter();
    }
    this.#busyPolicy = {
      now: hooks?.now ?? (() => performance.now()),
      sleep: hooks?.sleep ?? ((milliseconds) => sleep(milliseconds)),
      timeoutMs: busyTimeoutMs,
    };
    this.#application = database;
    this.#location = location;
    this.#ownsApplication = memoryLocation !== undefined;
    this.#key =
      memoryLocation === undefined
        ? (fileKey(location) ?? `path:${location}`)
        : `memdb:${location}`;
    this.#addHandle(database);
    const opened = this.#databaseOperation("configure", () =>
      openConnection(location, memoryLocation === undefined)
    );
    this.#connection = opened.connection;
    this.#walPending = opened.walPending;
  }

  /** What River's private registry records about this driver. */
  driverRecord(): DriverRecord<DatabaseSync> {
    return {
      backend: this.backend,
      capability: "runtime",
      database: this.#pilotDatabase() as PilotDatabase<DatabaseSync>,
      // River's own connection, which migrations share with its operations,
      // so they run under River's lock and retry a busy database like them.
      migration: {
        database: this.#connection,
        run: <T>(attempt: (database: object) => T) =>
          this.#exclusive("migrate", () =>
            retryBusy(this.#busyPolicy, () => attempt(this.#connection))
          ),
      },
      operations: this as InsertDriver<DatabaseSync, "runtime">,
    };
  }

  /**
   * The application handle the driver was created with, for this
   * package's tests.
   */
  get database(): DatabaseSync {
    return this.#application;
  }

  /**
   * Close River's private connection, and for {@link SqliteDriver.memory}
   * its {@link database} handle. Stop every client using the driver first.
   * Handles from {@link connect} and the one passed to the constructor stay
   * open. Closing twice does nothing.
   */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#releaseTurnCounter?.();
    // Closing rolls back a transaction River still has open.
    this.#connection.close();
    if (this.#ownsApplication && this.#application.isOpen) {
      this.#application.close();
    }
  }

  /**
   * Open another application handle on this driver's database. The caller
   * owns and closes it.
   *
   * `options` are `node:sqlite`'s. The busy `timeout` defaults to zero, so a
   * statement that meets another connection's write lock fails at once
   * instead of blocking the event loop. Pass a nonzero `timeout` when other
   * processes write the database (see the package README).
   */
  connect(options: DatabaseSyncOptions = {}): DatabaseSync {
    this.#assertOpen("connect");
    const database = new DatabaseSync(this.#location, {
      timeout: 0,
      ...options,
    });
    this.#addHandle(database);
    return database;
  }

  /** Same as {@link close}, for `using` declarations. */
  [Symbol.dispose](): void {
    this.close();
  }

  /** @internal */
  async jobCancel(
    id: bigint,
    options: InsertDriverOptions<DatabaseSync | SqliteRiverScope> = {}
  ): Promise<SqliteJobRow | null> {
    const result = await this.jobCancelDetailed(id, options);
    return result.status === "not_found" ? null : result.job;
  }

  /** @internal */
  async jobCancelDetailed(
    id: bigint,
    options: SqliteOperationOptions & { now?: Temporal.Instant } = {}
  ): Promise<SqliteCancelResult> {
    validateInt64(id, "id");
    const now = options.now ?? Temporal.Now.instant();
    return this.#write("cancel", options, (database) => {
      const raw = resultStatement(
        database,
        `
        UPDATE river_job
        SET
          state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END,
          finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE ? END,
          metadata = jsonb_set(metadata, '$.cancel_attempted_at', ?)
        WHERE id = ?
          AND state NOT IN ('cancelled', 'completed', 'discarded')
          AND finalized_at IS NULL
        RETURNING ${JOB_COLUMNS}
        `
      ).get(sqliteTimestamp(now), now.toString(), id);
      const updated = raw === undefined ? null : decodeJobRowPartial(raw).job;
      const job = updated ?? getJob(database, id, { partial: true });
      if (job === null) return { status: "not_found" };

      if (updated !== null) {
        // River for Go's `NotificationInsertJobCancel`, in the same
        // transaction.
        insertNotification(
          database,
          NOTIFICATION_TOPIC_CONTROL,
          `{"action":"cancel","job_id":${updated.id},"queue":${JSON.stringify(updated.queue)}}`
        );
      }
      return {
        job,
        status: updated === null ? "unchanged" : "cancelled",
      };
    });
  }

  /** @internal */
  async jobDelete(
    id: bigint,
    options: InsertDriverOptions<DatabaseSync | SqliteRiverScope> = {}
  ): Promise<JobDeleteResult> {
    return this.jobDeleteDetailed(id, options);
  }

  /** @internal */
  async jobDeleteMany(
    params: JobDeleteManyParams,
    options: InsertDriverOptions<DatabaseSync | SqliteRiverScope> = {}
  ): Promise<readonly SqliteJobRow[]> {
    if (
      !Number.isSafeInteger(params.limit) ||
      params.limit < 1 ||
      params.limit > 10_000
    ) {
      throw new RangeError("bulk delete maximum must be from 1 to 10000");
    }
    const hasFilter =
      params.ids.length > 0 ||
      params.kinds.length > 0 ||
      params.priorities.length > 0 ||
      params.queues.length > 0 ||
      params.states.length > 0;
    if (!params.all && !hasFilter) {
      throw new RangeError("bulk delete requires a filter or all=true");
    }
    if (params.all && hasFilter) {
      throw new RangeError(
        "bulk delete all=true cannot be combined with filters"
      );
    }

    const requestedStates = params.all
      ? NON_RUNNING_JOB_STATES
      : params.states.length === 0
        ? NON_RUNNING_JOB_STATES
        : params.states.filter((state) => state !== "running");
    if (requestedStates.length === 0) return [];

    return this.#write("job_delete_many", options, (database) => {
      const jobs = listJobs(database, {
        after: null,
        ids: params.ids,
        kinds: params.kinds,
        limit: params.limit,
        metadata: null,
        priorities: params.priorities,
        queues: params.queues,
        sortDirection: "asc",
        sortField: "id",
        states: requestedStates,
        tagsAll: [],
        tagsAny: [],
      });
      if (jobs.length === 0) return [];

      const statement = resultStatement(
        database,
        `DELETE FROM river_job
         WHERE id = ? AND state != 'running'
         RETURNING ${JOB_COLUMNS}`
      );
      const deleted: SqliteJobRow[] = [];
      for (const job of jobs) {
        const raw = statement.get(job.id);
        if (raw !== undefined) deleted.push(decodeJobRowPartial(raw).job);
      }
      return deleted;
    });
  }

  /** @internal */
  async jobDeleteDetailed(
    id: bigint,
    options: SqliteOperationOptions = {}
  ): Promise<SqliteDeleteResult> {
    validateInt64(id, "id");
    return this.#write("delete", options, (database) => {
      const raw = resultStatement(
        database,
        `DELETE FROM river_job
         WHERE id = ? AND state != 'running'
         RETURNING ${JOB_COLUMNS}`
      ).get(id);
      if (raw !== undefined)
        return { job: decodeJobRowPartial(raw).job, status: "deleted" };
      const job = getJob(database, id, { partial: true });
      if (job === null) return { status: "not_found" };
      return job.state === "running"
        ? { job, status: "running" }
        : { status: "not_found" };
    });
  }

  /** @internal */
  async jobGet(
    id: bigint,
    options: SqliteOperationOptions = {}
  ): Promise<SqliteJobRow | null> {
    validateInt64(id, "id");
    return this.#read("get", options, (database) => getJob(database, id));
  }

  /**
   * The IDs among `ids` of running jobs with a cancellation request, like
   * River for Go's `JobGetCancelRequested`. A poll-only runtime checks its
   * running attempts with it.
   * @internal
   */
  async jobGetCancelRequested(
    ids: readonly bigint[],
    options: RuntimeWaitOptions = {}
  ): Promise<readonly bigint[]> {
    options.signal?.throwIfAborted();
    if (ids.length === 0) return [];
    for (const id of ids) validateInt64(id, "id");
    return this.#read("job_get_cancel_requested", {}, (database) =>
      resultStatement(
        database,
        // Metadata that isn't valid JSON has no cancellation request, so it
        // can't fail the lookup for the other jobs, as in Go.
        `SELECT id FROM river_job
         WHERE id IN (SELECT value FROM json_each(?))
           AND (CASE WHEN ${invalidJsonTextSql("metadata")} THEN NULL
             ELSE metadata -> 'cancel_attempted_at' END) IS NOT NULL
           AND state = 'running'
         ORDER BY id`
      )
        .all(`[${ids.map((id) => id.toString(10)).join(",")}]`)
        .map(({ id }) => id as bigint)
    );
  }

  /** @internal */
  async jobList(
    params: JobListParams,
    options: InsertDriverOptions<DatabaseSync | SqliteRiverScope> = {}
  ): Promise<readonly SqliteJobRow[]> {
    if (params.metadata === null) {
      return this.#read("job_list", options, (database) =>
        listJobs(database, { ...params, metadata: null })
      );
    }

    // Filter metadata one bounded page at a time, yielding to the event loop
    // (and, outside a transaction, releasing the handle) between pages so a
    // sparse match can't block either for a whole-table scan.
    const limit = validateSmallInteger(params.limit, "limit", 0, 10_000);
    const selected: SqliteJobRow[] = [];
    let after = params.after;
    while (selected.length < limit) {
      const page = await this.#read("job_list", options, (database) =>
        listJobsMetadataPage(database, {
          ...params,
          after,
          limit: limit - selected.length,
        })
      );
      selected.push(...page.jobs);
      if (page.next === null) break;
      after = page.next;
      await new Promise((resolve) => setImmediate(resolve));
    }
    return selected;
  }

  /** @internal */
  async jobUpdate(
    id: bigint,
    params: JobUpdateParams,
    options: InsertDriverOptions<DatabaseSync | SqliteRiverScope> = {}
  ): Promise<SqliteJobRow | null> {
    return this.#write("job_update", options, (database) =>
      updateJob(database, id, params)
    );
  }

  /** @internal */
  async jobClaim(
    params: JobClaimParams,
    options: JobClaimOptions<DatabaseSync | SqliteRiverScope> = {}
  ): Promise<JobClaimResult> {
    return this.#write(
      "job_claim",
      options.tx === undefined ? {} : { tx: options.tx },
      (database) => claimJobs(database, params)
    );
  }

  /** @internal */
  async jobCompleteMany(
    items: readonly JobCompletionCommand[],
    options: {
      readonly signal?: AbortSignal;
      readonly tx?: DatabaseSync | SqliteRiverScope;
    } = {}
  ): Promise<readonly JobCompletionResult[]> {
    if (items.length === 0) return [];
    options.signal?.throwIfAborted();
    return this.#write("job_complete_many", options, (database) =>
      completeJobs(database, items)
    );
  }

  /** @internal */
  async jobGetStuck(
    params: {
      afterId?: bigint;
      attemptedBefore: Temporal.Instant;
      limit?: number;
    },
    options: SqliteOperationOptions = {}
  ): Promise<readonly SqliteJobRow[]> {
    return this.#read("job_get_stuck", options, (database) =>
      stuckJobs(database, params)
    );
  }

  /**
   * Rescue jobs selected with `attemptedBefore`, skipping any that are no
   * longer running with an attempt before that same horizon.
   * @internal
   */
  async jobRescueMany(
    items: readonly SqliteRescueJobParams[],
    attemptedBefore: Temporal.Instant,
    options: SqliteOperationOptions = {}
  ): Promise<readonly SqliteJobRow[]> {
    if (items.length === 0) return [];
    return this.#write("job_rescue_many", options, (database) =>
      rescueJobs(database, items, attemptedBefore)
    );
  }

  /** @internal */
  async jobCleanup(
    params: SqliteCleanupJobsParams,
    options: SqliteOperationOptions = {}
  ): Promise<number> {
    return this.#write("job_cleanup", options, (database) =>
      cleanupJobs(database, params)
    );
  }

  /** @internal */
  async jobSchedule(
    params: {
      limit?: number;
      now?: Temporal.Instant;
      scheduledAtHorizon?: Temporal.Instant;
    } = {},
    options: SqliteOperationOptions = {}
  ): Promise<readonly SqliteScheduleResult[]> {
    return this.#write("job_schedule", options, (database) =>
      scheduleJobs(database, params)
    );
  }

  /**
   * Insert one resolved job and its wakeup in the same SQLite transaction.
   *
   * Part of River's unstable driver interface, which `Client` calls;
   * applications insert through the client instead. Its parameter and result
   * types come from `riverqueue/unstable-driver` and may change in any
   * release.
   */
  jobInsert(
    params: JobInsertParams,
    options?: InsertDriverOptions<DatabaseSync | SqliteRiverScope>
  ): Promise<DriverInsertResult>;
  /** @internal */
  jobInsert<TArgs extends SqliteJsonObject>(
    params: SqliteInsertJobParams<TArgs>,
    options?: SqliteOperationOptions
  ): Promise<SqliteInsertResult<TArgs>>;
  async jobInsert<TArgs extends SqliteJsonObject>(
    params: SqliteInsertJobParams<TArgs>,
    options: SqliteOperationOptions = {}
  ): Promise<SqliteInsertResult<TArgs>> {
    // Validate before writing, so invalid input writes nothing even in a
    // caller's transaction.
    const values = this.#normalizeInsert(params);
    return this.#write("insert", options, (database) =>
      this.#insertRow<TArgs>(database, values)
    );
  }

  /**
   * Insert an ordered batch atomically on the caller-owned SQLite handle.
   *
   * Part of River's unstable driver interface, which `Client` calls;
   * applications insert through the client instead. Its parameter and result
   * types come from `riverqueue/unstable-driver` and may change in any
   * release.
   */
  jobInsertMany(
    params: readonly JobInsertParams[],
    options?: InsertDriverOptions<DatabaseSync | SqliteRiverScope>
  ): Promise<readonly DriverInsertResult[]>;
  /** @internal */
  jobInsertMany<TArgs extends SqliteJsonObject>(
    params: readonly SqliteInsertJobParams<TArgs>[],
    options?: SqliteOperationOptions
  ): Promise<readonly SqliteInsertResult<TArgs>[]>;
  async jobInsertMany(
    params: readonly SqliteInsertJobParams[],
    options: SqliteOperationOptions = {}
  ): Promise<readonly SqliteInsertResult[]> {
    if (params.length === 0) return [];
    // Validate every row before writing any of them, so an invalid row
    // writes nothing even in a caller's transaction.
    const values = params.map((item) => this.#normalizeInsert(item));
    return this.#write("insert_many", options, (database) =>
      values.map((item) => this.#insertRow(database, item))
    );
  }

  /**
   * Notify producers of new jobs in each of `queues`, delivered once the
   * transaction commits.
   *
   * Part of River's unstable driver interface, which `Client` calls after
   * inserting jobs. Its parameter types come from
   * `riverqueue/unstable-driver` and may change in any release.
   */
  async notifyInsert(
    queues: readonly string[],
    options: InsertDriverOptions<DatabaseSync | SqliteRiverScope> = {}
  ): Promise<void> {
    if (queues.length === 0) return;
    await this.#write("notify_insert", options, (database) => {
      writeInsertNotifications(database, queues);
    });
  }

  /** @internal */
  async queueList(
    params: QueueListParams,
    options: InsertDriverOptions<DatabaseSync | SqliteRiverScope> = {}
  ): Promise<readonly QueueRow[]> {
    const limit = validateSmallInteger(params.limit, "limit", 0, 1_000_000);
    if (limit === 0) return [];
    return this.#read("queue_list", options, (database) =>
      resultStatement(
        database,
        `SELECT ${QUEUE_COLUMNS}
         FROM river_queue
         WHERE (? IS NULL OR name > ?)
         ORDER BY name ASC LIMIT ?`
      )
        .all(params.nameAfter, params.nameAfter, limit)
        .map(decodeQueueRow)
    );
  }

  /** @internal */
  async queueGet(
    name: string,
    options: SqliteOperationOptions = {}
  ): Promise<SqliteQueueRow | null> {
    validateLookupName(name, "name");
    return this.#read("queue_get", options, (database) => {
      const raw = resultStatement(
        database,
        `SELECT ${QUEUE_COLUMNS} FROM river_queue WHERE name = ?`
      ).get(name);
      return raw === undefined ? null : decodeQueueRow(raw);
    });
  }

  /** @internal */
  async queuePause(
    name: string,
    options: InsertDriverOptions<DatabaseSync | SqliteRiverScope> = {}
  ): Promise<QueueRow | null> {
    const queues = await this.#queueSetPaused(name, true, options);
    return name === "*" ? null : (queues[0] ?? null);
  }

  /** @internal */
  async queueResume(
    name: string,
    options: InsertDriverOptions<DatabaseSync | SqliteRiverScope> = {}
  ): Promise<QueueRow | null> {
    const queues = await this.#queueSetPaused(name, false, options);
    return name === "*" ? null : (queues[0] ?? null);
  }

  /** @internal */
  async queueUpsert(
    name: string,
    params: {
      metadata?: SqliteJsonObject;
      now?: Temporal.Instant;
      pausedAt?: Temporal.Instant | null;
    } = {},
    options: SqliteOperationOptions = {}
  ): Promise<SqliteQueueRow> {
    validateName(name, "name");
    const now = params.now ?? Temporal.Now.instant();
    return this.#write("queue_upsert", options, (database) => {
      const raw = resultStatement(
        database,
        `
        INSERT INTO river_queue (created_at, metadata, name, paused_at, updated_at)
        VALUES (?, jsonb(?), ?, ?, ?)
        ON CONFLICT (name) DO UPDATE SET updated_at = excluded.updated_at
        RETURNING ${QUEUE_COLUMNS}
        `
      ).get(
        sqliteTimestamp(now),
        encodeJson(params.metadata ?? {}, "metadata"),
        name,
        sqliteTimestampOrNull(params.pausedAt),
        sqliteTimestamp(now)
      );
      if (raw === undefined) {
        throw databaseError(
          "queue_upsert",
          "SQLite queue upsert returned no row"
        );
      }
      return decodeQueueRow(raw);
    });
  }

  /** @internal */
  async queueUpdate(
    name: string,
    params: QueueUpdateParams,
    options: InsertDriverOptions<DatabaseSync | SqliteRiverScope> = {}
  ): Promise<QueueRow | null> {
    validateLookupName(name, "name");
    const now = Temporal.Now.instant();
    const update = queueMetadataUpdate(name, params);
    return this.#write("queue_update", options, (database) => {
      const raw = resultStatement(
        database,
        `
        UPDATE river_queue
        SET metadata = CASE WHEN ? THEN jsonb(?) ELSE metadata END,
            updated_at = ?
        WHERE name = ?
        RETURNING ${QUEUE_COLUMNS}
        `
      ).get(
        update === undefined ? 0 : 1,
        update?.text ?? "{}",
        sqliteTimestamp(now),
        name
      );
      if (raw === undefined) return null;
      const queue = decodeQueueRow(raw);
      if (update !== undefined) {
        insertNotification(
          database,
          NOTIFICATION_TOPIC_CONTROL,
          update.notification
        );
      }
      return queue;
    });
  }

  /** @internal */
  async queueCleanup(
    params: { limit?: number; updatedBefore: Temporal.Instant },
    options: SqliteOperationOptions = {}
  ): Promise<readonly string[]> {
    return this.#write("queue_cleanup", options, (database) =>
      queueDeleteExpired(database, params)
    );
  }

  /** @internal */
  async leaderElect(
    params: { leaderId: string; now?: Temporal.Instant; ttlMs: number },
    options: SqliteOperationOptions = {}
  ): Promise<SqliteLeader | null> {
    return this.#write("leader_elect", options, (database) =>
      leaderAttemptElect(database, params)
    );
  }

  /** @internal */
  async leaderReelect(
    leader: SqliteLeader,
    params: { now?: Temporal.Instant; ttlMs: number },
    options: SqliteOperationOptions = {}
  ): Promise<SqliteLeader | null> {
    return this.#write("leader_reelect", options, (database) =>
      leaderAttemptReelect(database, leader, params)
    );
  }

  /** @internal */
  async leaderGet(
    options: SqliteOperationOptions = {}
  ): Promise<SqliteLeader | null> {
    return this.#read("leader_get", options, leaderGet);
  }

  /** @internal */
  async leaderResign(
    leader: Pick<SqliteLeader, "electedAt" | "leaderId">,
    options: SqliteOperationOptions = {}
  ): Promise<boolean> {
    return this.#write("leader_resign", options, (database) =>
      leaderResign(database, leader)
    );
  }

  /** @internal */
  async notificationPoll(
    params: {
      afterId?: bigint;
      limit?: number;
      topics?: readonly string[];
    } = {},
    options: SqliteOperationOptions = {}
  ): Promise<readonly SqliteNotification[]> {
    return this.#read("notification_poll", options, (database) =>
      notificationPoll(database, params)
    );
  }

  /** @internal */
  async notificationLastId(
    options: SqliteOperationOptions = {}
  ): Promise<bigint> {
    return this.#read("notification_last_id", options, notificationLastId);
  }

  /** @internal */
  async notificationCleanup(
    params: { createdBefore: Temporal.Instant; limit?: number },
    options: SqliteOperationOptions = {}
  ): Promise<number> {
    return this.#write("notification_cleanup", options, (database) =>
      notificationCleanup(database, params)
    );
  }

  /**
   * Poll the durable outbox for the given topics. Like Go's SQLite listener, a
   * subscription starts after the outbox's current last ID, so it never
   * replays rows written before it, including rows written while an earlier
   * subscription was closed. Its cursor and unread rows end with it.
   *
   * @internal
   */
  async *runtimeNotificationSubscribe(
    topics: readonly RuntimeNotification["topic"][],
    signal: AbortSignal,
    ready: () => void
  ): AsyncGenerator<RuntimeNotification> {
    let afterId = await this.notificationLastId();
    const backendTopics = topics.map(runtimeTopicName);
    ready();
    while (!signal.aborted) {
      const notifications = await this.notificationPoll({
        afterId,
        limit: NOTIFICATION_BATCH_SIZE,
        topics: backendTopics,
      });
      for (const notification of notifications) {
        // Like Go's listener, closing discards rows read but not delivered.
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while a row is yielded
        if (signal.aborted) return;
        afterId = notification.id;
        yield {
          payload: notification.payload,
          topic: runtimeTopic(notification.topic),
        };
      }
      if (notifications.length === 0) await waitForPoll(100, signal);
    }
  }

  /** @internal */
  async runtimeQueueUpsert(
    name: string,
    now: Temporal.Instant
  ): Promise<QueueRow> {
    return this.queueUpsert(name, { now });
  }

  /** @internal */
  async runtimeRequestLeadershipResignation(
    options: SqliteOperationOptions = {}
  ): Promise<void> {
    await this.#write(
      "runtime_request_leadership_resignation",
      options,
      (database) => {
        insertNotification(
          database,
          "river_leadership",
          '{"action":"request_resign","leader_id":""}'
        );
      }
    );
  }

  /** @internal */
  async maintenanceLeaderAcquire(
    leaderId: string,
    now: Temporal.Instant,
    ttlMs: number,
    held: RuntimeLeader | null
  ): Promise<RuntimeLeader | null> {
    return this.#write("maintenance_leader_acquire", {}, (database) =>
      held === null
        ? leaderAttemptElect(database, { leaderId, now, ttlMs })
        : leaderAttemptReelect(database, { ...held, leaderId }, { now, ttlMs })
    );
  }

  /** @internal */
  async maintenanceLeaderResign(leader: RuntimeLeader): Promise<boolean> {
    return this.#write("maintenance_leader_resign", {}, (database) => {
      // Like River for Go's SQLite driver, other clients learn of the
      // resignation at their next election attempt, without a notification.
      return leaderResign(database, leader);
    });
  }

  /** @internal */
  async maintenanceSchedule(
    leader: RuntimeLeader,
    params: RuntimeScheduleParams,
    batch?: RuntimeMaintenanceBatch
  ): Promise<number> {
    return (
      (await this.#withMaintenanceLeader(
        leader,
        "maintenance_schedule",
        (database) => {
          const results = scheduleJobs(database, {
            limit: params.limit,
            now: params.now,
            scheduledAtHorizon: params.scheduledAtHorizon,
          });
          // Like River for Go's scheduler, wake producers of jobs that are
          // due, or nearly due, through the client's insert notification
          // limiter. Stored times are rounded to the millisecond, so round
          // the horizon the same way; otherwise a job scheduled "now" can
          // round past it.
          const horizon = params.notificationHorizon.round({
            roundingMode: "halfExpand",
            smallestUnit: "millisecond",
          });
          writeInsertNotifications(
            database,
            params.allowInsertNotifications(
              results.flatMap(({ job }) =>
                Temporal.Instant.compare(job.scheduledAt, horizon) <= 0
                  ? [job.queue]
                  : []
              )
            )
          );
          return results.length;
        },
        batch
      )) ?? 0
    );
  }

  /** @internal */
  async maintenanceGetStuck(
    leader: RuntimeLeader,
    attemptedBefore: Temporal.Instant,
    afterId: bigint,
    limit: number,
    batch?: RuntimeMaintenanceBatch
  ): Promise<readonly SqliteJobRow[]> {
    return (
      (await this.#withMaintenanceLeader(
        leader,
        "maintenance_get_stuck",
        (database) => stuckJobs(database, { afterId, attemptedBefore, limit }),
        batch
      )) ?? []
    );
  }

  /**
   * Rescue a page read by {@link maintenanceGetStuck} with the same
   * `attemptedBefore`. Jobs that finished, were released, or were claimed
   * again in between are left untouched.
   * @internal
   */
  async maintenanceRescue(
    leader: RuntimeLeader,
    attemptedBefore: Temporal.Instant,
    jobs: readonly RuntimeJobRescue[],
    options: InsertDriverOptions<DatabaseSync | SqliteRiverScope> = {}
  ): Promise<number> {
    return (
      (await this.#withMaintenanceLeader(
        leader,
        "maintenance_rescue",
        (database) => rescueJobs(database, jobs, attemptedBefore).length,
        undefined,
        options
      )) ?? 0
    );
  }

  /** @internal */
  async maintenanceCleanJobs(
    leader: RuntimeLeader,
    params: RuntimeJobCleanupParams,
    timeoutMs: number | null,
    signal: AbortSignal
  ): Promise<number> {
    const count =
      (await this.#withMaintenanceLeader(
        leader,
        "maintenance_clean_jobs",
        (database) => cleanupJobs(database, params),
        { signal, timeoutMs }
      )) ?? 0;
    return count;
  }

  /** @internal */
  async maintenanceCleanQueues(
    leader: RuntimeLeader,
    updatedBefore: Temporal.Instant,
    limit: number,
    batch?: RuntimeMaintenanceBatch
  ): Promise<number> {
    return (
      (await this.#withMaintenanceLeader(
        leader,
        "maintenance_clean_queues",
        (database) =>
          queueDeleteExpired(database, { limit, updatedBefore }).length,
        batch
      )) ?? 0
    );
  }

  /** @internal */
  async maintenanceCleanNotifications(
    leader: RuntimeLeader,
    createdBefore: Temporal.Instant,
    limit: number,
    batch?: RuntimeMaintenanceBatch
  ): Promise<number> {
    return (
      (await this.#withMaintenanceLeader(
        leader,
        "maintenance_clean_notifications",
        (database) => notificationCleanup(database, { createdBefore, limit }),
        batch
      )) ?? 0
    );
  }

  /** @internal */
  async jobRetry(
    id: bigint,
    options: InsertDriverOptions<DatabaseSync | SqliteRiverScope> = {}
  ): Promise<SqliteJobRow | null> {
    const result = await this.jobRetryDetailed(id, options);
    return result.status === "not_found" ? null : result.job;
  }

  /** @internal */
  async jobRetryDetailed(
    id: bigint,
    options: SqliteOperationOptions & { now?: Temporal.Instant } = {}
  ): Promise<SqliteRetryResult> {
    validateInt64(id, "id");
    const now = options.now ?? Temporal.Now.instant();
    return this.#write("retry", options, (database) => {
      const timestamp = sqliteTimestamp(now);
      const raw = resultStatement(
        database,
        `
        UPDATE river_job
        SET
          state = 'available',
          max_attempts = CASE
            WHEN attempt = max_attempts THEN max_attempts + 1
            ELSE max_attempts
          END,
          finalized_at = NULL,
          scheduled_at = ?
        WHERE id = ?
          AND state != 'running'
          AND (state != 'available' OR scheduled_at > ?)
        RETURNING ${JOB_COLUMNS}
        `
      ).get(timestamp, id, timestamp);
      const updated = raw === undefined ? null : decodeJobRowPartial(raw).job;
      const job = updated ?? getJob(database, id, { partial: true });
      if (job === null) return { status: "not_found" };
      return {
        job,
        status: updated === null ? "unchanged" : "retried",
      };
    });
  }

  /**
   * Run synchronous SQL for a first-party extension that stores its own
   * state beside River's, on the connection its operation belongs to.
   *
   * With `tx`, `callback` runs directly in that transaction (River's scope,
   * which it begins if it hasn't yet, or an application transaction).
   * Otherwise it runs in a short transaction of its own on
   * River's connection, retried while another connection holds the lock,
   * so it must be safe to run again.
   *
   * @internal
   */
  execute<T>(
    operation: string,
    options: SqliteOperationOptions,
    callback: (database: DatabaseSync) => T
  ): Promise<T> {
    return this.#write(operation, options, (database) => callback(database));
  }

  /**
   * Run one River operation in a transaction, like River for Go's
   * `dbutil.WithTxV`.
   *
   * With `tx`, the operation joins that application transaction. Otherwise
   * River owns a transaction on its private connection. It begins with
   * `BEGIN IMMEDIATE` at the operation's first River statement, commits
   * when `callback` resolves, and rolls back when it rejects. `callback`
   * receives an opaque value standing for the transaction: pass it back to
   * this driver as `{ tx }`, or to {@link execute}.
   *
   * Once the transaction has begun, a River call without `{ tx }` from
   * inside it would wait for it forever, so it fails at once with a
   * `TransactionScopeError`. Before its first statement River holds no
   * lock, and such calls run normally.
   *
   * @internal
   */
  async operationScope<T>(
    tx: DatabaseSync | SqliteRiverScope | undefined,
    callback: (tx: DatabaseSync | SqliteRiverScope) => Promise<T>
  ): Promise<T> {
    if (tx !== undefined) {
      this.#resolveTransaction(tx, "operation_scope");
      return callback(tx);
    }
    this.#assertOpen("operation_scope");
    this.#assertNotReentrant("operation_scope", true);
    const scope = new OwnedScope(this, this.#key);
    let result: T;
    try {
      result = await runInFrame(scope.frame, () =>
        callback(scope as unknown as SqliteRiverScope)
      );
    } catch (error: unknown) {
      await this.#endScope(scope, false);
      throw error;
    }
    await this.#endScope(scope, true);
    return result;
  }

  /**
   * Wait for River's connection lock. A caller inside River's own
   * transactions that haven't begun yet stops waiting, with a
   * `TransactionScopeError`, as soon as one of them takes the lock: that
   * transaction would wait for the caller, which would wait for it.
   */
  async #acquireLock(
    operation: string,
    own: OwnedScope | null = null,
    signal?: AbortSignal
  ): Promise<() => void> {
    const enclosing: OwnedScope[] = [];
    for (const frame of openFrames()) {
      if (
        frame.kind === "river" &&
        frame.driver === this &&
        frame.scope !== own
      ) {
        enclosing.push(frame.scope as OwnedScope);
      }
    }
    if (enclosing.length === 0) return this.#lock.acquire(signal);
    const controller = new AbortController();
    const abort = (): void => {
      controller.abort(this.#reentrantError(operation, true));
    };
    for (const scope of enclosing) scope.lockWaiters.add(abort);
    const link =
      signal === undefined
        ? undefined
        : new LinkedAbortSignal([controller.signal, signal]);
    try {
      return await this.#lock.acquire(link?.signal ?? controller.signal);
    } finally {
      link?.[Symbol.dispose]();
      for (const scope of enclosing) scope.lockWaiters.delete(abort);
    }
  }

  /** Track an application handle on this driver's database. */
  #addHandle(database: DatabaseSync): void {
    registerDatabase(database, this.#key);
    this.#handles.add(new WeakRef(database));
  }

  /** Whether an application handle River knows has a transaction open. */
  #applicationTransactionOpen(): boolean {
    for (const reference of this.#handles) {
      const database = reference.deref();
      if (database === undefined || !database.isOpen) {
        this.#handles.delete(reference);
      } else if (database.isTransaction) {
        return true;
      }
    }
    return false;
  }

  #assertOpen(operation: string): void {
    if (this.#closed) {
      throw new LifecycleError(
        `SQLite driver is closed, so ${operation} can't run`,
        { details: { backend: SQLITE_BACKEND, operation } }
      );
    }
  }

  /**
   * Fail a River call without `{ tx }` that would wait for a transaction
   * the calling code is inside: River's own transaction on this database
   * once it holds the lock (insert middleware after `next()`, `afterInsert`
   * hooks), or for a write, an application transaction on the same
   * database.
   */
  #assertNotReentrant(operation: string, write: boolean): void {
    const details = { backend: SQLITE_BACKEND, operation };
    for (const frame of openFrames()) {
      if (
        frame.kind === "river" &&
        frame.holdsLock() &&
        (frame.driver === this || sameDatabase(frame.key, this.#key))
      ) {
        throw this.#reentrantError(operation, frame.driver === this);
      }
      if (
        write &&
        frame.kind === "application" &&
        sameDatabase(frame.key, this.#key)
      ) {
        throw new TransactionScopeError(
          "reentrant",
          `SQLite River operation ${operation} was called without { tx } ` +
            "inside transaction() on the same database, so it would wait " +
            "for that transaction's write lock. Pass the transaction's " +
            "handle as { tx }",
          { details }
        );
      }
    }
  }

  /**
   * Begin a River-owned transaction at its first statement: wait for the
   * private connection, then take SQLite's write lock with
   * `BEGIN IMMEDIATE`, retrying asynchronously while another connection
   * holds it.
   */
  #beginScope(scope: OwnedScope, signal?: AbortSignal): Promise<void> {
    scope.beginning ??= (async () => {
      scope.state = "beginning";
      const release = await this.#acquireLock(
        "transaction_begin",
        scope,
        signal
      );
      scope.holdsLock = true;
      this.#lockHolder = scope;
      for (const abort of [...scope.lockWaiters]) abort();
      try {
        this.#assertOpen("transaction_begin");
        await this.#switchToWal("transaction_begin");
        await this.#retryBusy("transaction_begin", () => {
          this.#connection.exec("BEGIN IMMEDIATE");
        });
      } catch (error: unknown) {
        scope.holdsLock = false;
        if (this.#lockHolder === scope) this.#lockHolder = null;
        release();
        scope.state = "idle";
        throw error;
      }
      scope.release = release;
      scope.state = "open";
      scope.turnsAtBegin = eventLoopTurns();
      // River's transaction must end within the event loop turn it began
      // in: code awaiting only promises can't let other I/O, timers, or
      // requests run. If the probe runs first, something awaited I/O.
      scope.probe = setImmediate(() => {
        this.#probeScope(scope);
      });
    })();
    return scope.beginning;
  }

  #databaseOperation<T>(operation: string, callback: () => T): T {
    try {
      return callback();
    } catch (cause: unknown) {
      throw this.#wrapError(operation, cause);
    }
  }

  /**
   * End a River-owned transaction: commit it, or roll it back when the
   * operation failed, and release the private connection.
   */
  async #endScope(scope: OwnedScope, commit: boolean): Promise<void> {
    try {
      if (scope.beginning !== null) {
        // A failed begin was already reported through the statement.
        await scope.beginning.catch(() => undefined);
      }
      this.#checkTurns(scope);
      // The probe's failure explains whatever else the operation threw.
      if (scope.failure !== null) throw scope.failure;
      if (scope.state !== "open") return;
      if (!commit) {
        this.#rollbackQuietly();
        return;
      }
      // Closing the driver rolled the transaction back.
      this.#assertOpen("transaction_commit");
      scope.state = "committing";
      try {
        // SQLite keeps a transaction open when COMMIT is busy, so COMMIT
        // alone can be retried.
        await this.#retryBusy("transaction_commit", () => {
          this.#connection.exec("COMMIT");
        });
      } catch (error: unknown) {
        this.#rollbackQuietly();
        throw error;
      }
    } finally {
      this.#releaseScope(scope);
      scope.state = "ended";
      scope.frame.ended = true;
    }
  }

  /** Run a River operation on the private connection, one at a time. */
  async #exclusive<T>(operation: string, run: () => Promise<T>): Promise<T> {
    const release = await this.#acquireLock(operation);
    try {
      this.#assertOpen(operation);
      await this.#switchToWal(operation);
      return await run();
    } finally {
      release();
    }
  }

  /**
   * Run statements in a River-owned transaction, beginning it if this is
   * its first statement. Like River for Go, River opens no savepoint in its
   * own transaction: a failed operation fails the transaction's owner,
   * which rolls it back.
   */
  async #inScope<T>(
    scope: OwnedScope,
    operation: string,
    callback: (database: DatabaseSync) => T
  ): Promise<T> {
    this.#checkTurns(scope);
    if (scope.failure !== null) throw scope.failure;
    if (scope.state !== "open") await this.#beginScope(scope);
    this.#assertOpen(operation);
    if (scope.state !== "open") {
      throw backendMismatchError(
        operation,
        "SQLite River transaction already ended"
      );
    }
    return this.#databaseOperation(operation, () => callback(this.#connection));
  }

  #insertRow<TArgs extends SqliteJsonObject>(
    database: DatabaseSync,
    values: InsertValues
  ): SqliteInsertResult<TArgs> {
    const raw = resultStatement(
      database,
      `
      INSERT INTO river_job (${INSERT_COLUMNS_SQL})
      VALUES (${INSERT_VALUES_SQL})
      ON CONFLICT (unique_key)
        WHERE unique_key IS NOT NULL
          AND unique_states IS NOT NULL
          AND ${UNIQUE_STATE_MATCH_SQL}
        DO UPDATE SET kind = river_job.kind
      RETURNING ${JOB_COLUMNS}
      `
    ).get(...insertBindings(values));
    if (raw === undefined) {
      throw databaseError("insert", "SQLite insert returned no River job");
    }

    // Go's returning SQLite insert tags every row with a nonce, including
    // non-unique rows, and keeps it in stored and returned metadata. A
    // different nonce means the upsert returned an existing unique job.
    const job = decodeJobRowPartial(raw).job as SqliteJobRow<TArgs>;
    const duplicate = job.metadata[UNIQUE_NONCE_KEY] !== values.uniqueNonce;
    return { job, status: duplicate ? "duplicate" : "inserted" };
  }

  /**
   * Whether an application handle is on this driver's database: one River
   * knows (the handle it was created with, or one from `connect()`), or one
   * opened on the same database file.
   */
  #isSameDatabase(database: DatabaseSync): boolean {
    return sameDatabase(databaseKey(database), this.#key);
  }

  #normalizeInsert<TArgs extends SqliteJsonObject>(
    params: SqliteInsertJobParams<TArgs>
  ): InsertValues {
    const state =
      params.state ??
      (params.scheduledAt !== undefined &&
      Temporal.Instant.compare(
        params.scheduledAt,
        params.createdAt ?? Temporal.Now.instant()
      ) > 0
        ? "scheduled"
        : "available");
    const uniqueKey = params.uniqueKey ?? null;
    if (uniqueKey !== null && uniqueKey.byteLength !== 32) {
      throw invalidInputError(
        "insert",
        "invalid SQLite River input uniqueKey: expected 32 bytes"
      );
    }
    // Like Go's returning insert, roll a nonce for each row. The client
    // rejects a batch holding two rows with the same active unique key
    // before writing it, like Go.
    const uniqueNonce = randomBytes(8).toString("hex");
    const metadata = toJsonObject(params.metadata ?? {});
    metadata[UNIQUE_NONCE_KEY] = uniqueNonce;

    const errors = (params.errors ?? []).map((error) => ({
      at: error.at.toString(),
      attempt: validateSmallInteger(
        error.attempt,
        "error.attempt",
        0,
        Number.MAX_SAFE_INTEGER
      ),
      error: error.error,
      trace: error.trace,
    }));
    return {
      // Like Go, store the caller's encoded arguments rather than `args`.
      args:
        params.encodedArgs === undefined
          ? encodeJson(params.args, "args")
          : encodeEncodedJson(params.encodedArgs, "encodedArgs"),
      // Like River for Go, store attempt counts SQLite's 64-bit integers
      // hold, wider than Postgres's 16-bit columns.
      attempt: validateSmallInteger(
        params.attempt ?? 0,
        "attempt",
        0,
        Number.MAX_SAFE_INTEGER
      ),
      attemptedAt: sqliteTimestampOrNull(params.attemptedAt),
      // Go writes NULL for absent client IDs and errors, not an empty array.
      attemptedBy:
        params.attemptedBy === undefined
          ? null
          : encodeJson(params.attemptedBy, "attemptedBy"),
      createdAt: sqliteTimestampOrNull(params.createdAt),
      errors: errors.length === 0 ? null : encodeJson(errors, "errors"),
      finalizedAt: sqliteTimestampOrNull(params.finalizedAt),
      id: params.id === undefined ? null : validateInt64(params.id, "id"),
      kind: validateName(params.kind, "kind"),
      maxAttempts: validateSmallInteger(
        params.maxAttempts ?? 25,
        "maxAttempts",
        1,
        Number.MAX_SAFE_INTEGER
      ),
      metadata: encodeJson(metadata, "metadata"),
      priority: validateSmallInteger(params.priority ?? 1, "priority", 1, 4),
      queue: validateName(params.queue ?? "default", "queue"),
      scheduledAt: sqliteTimestampOrNull(params.scheduledAt),
      state: validateJobState(state),
      tags: encodeJson(params.tags ?? [], "tags"),
      uniqueKey,
      uniqueNonce,
      uniqueStates: encodeUniqueStates(params.uniqueStates),
    };
  }

  /**
   * Borrow River's private connection, outside any transaction, for a
   * pilot's reads and single autocommit statements, holding River's lock on
   * it meanwhile. A River call without `{ tx }` from inside fails at once
   * instead of waiting for the lock forever.
   */
  async #pilotConnection<Result>(
    callback: (handle: DatabaseSync) => PromiseLike<Result> | Result,
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<Result> {
    const operation = "pilot_connection";
    options.signal?.throwIfAborted();
    this.#assertOpen(operation);
    this.#assertNotReentrant(operation, false);
    // The frame marks River's lock as held for re-entry detection only; it
    // is never a transaction.
    const scope = new OwnedScope(this, this.#key);
    const release = await this.#acquireLock(operation, scope, options.signal);
    scope.holdsLock = true;
    try {
      this.#assertOpen(operation);
      await this.#switchToWal(operation);
      const result = await runInFrame(scope.frame, () =>
        callback(this.#connection)
      );
      if (this.#connection.isTransaction) {
        throw new TransactionScopeError(
          "nested",
          "a pilot's connection callback left a transaction open; use " +
            "the pilot database's transaction() instead",
          { details: { backend: SQLITE_BACKEND, operation } }
        );
      }
      return result;
    } catch (cause: unknown) {
      this.#rollbackQuietly();
      throw this.#wrapError(operation, cause);
    } finally {
      scope.holdsLock = false;
      scope.frame.ended = true;
      release();
    }
  }

  /** The database this driver gives a client's pilot. */
  #pilotDatabase(): PilotDatabase<DatabaseSync | SqliteRiverScope> {
    return {
      backend: SQLITE_BACKEND,
      connection: (callback, options) =>
        this.#pilotConnection(callback, options),
      deleteFinalizedJobs: async (params, options) =>
        this.#write(
          "delete_finalized_jobs",
          options?.tx === undefined ? {} : requireTx(options),
          (database) => cleanupJobs(database, params)
        ),
      loadClaimed: async (ids, options) =>
        this.#read("load_claimed", requireTx(options), (database) =>
          loadClaimedJobs(database, ids)
        ),
      notify: async (topic, payloads, options) =>
        this.#write("notify", requireTx(options), (database) => {
          const name = runtimeTopicName(pilotTopic(topic));
          for (const payload of payloads) {
            insertNotification(database, name, payload);
          }
        }),
      schema: null,
      transaction: (callback, options) =>
        this.#pilotTransaction(callback, options),
    };
  }

  /**
   * Run a pilot's callback directly in River's transaction `scope`,
   * beginning it if this is its first statement. River's private connection
   * passed as `{ tx }` from inside stands for `scope` meanwhile. Like River
   * for Go, River opens no savepoint: when the callback fails, its writes
   * stay in `scope` until the scope's owner rolls it back.
   */
  async #pilotInScope<Result>(
    scope: OwnedScope,
    callback: (tx: DatabaseSync) => PromiseLike<Result> | Result,
    signal: AbortSignal | undefined
  ): Promise<Result> {
    const operation = "pilot_transaction";
    this.#throwIfFailed(scope);
    if (scope.state !== "open") await this.#beginScope(scope, signal);
    this.#assertOpen(operation);
    if (!this.#scopeOpen(scope)) {
      throw backendMismatchError(
        operation,
        "SQLite River transaction already ended"
      );
    }
    scope.pilotDepth++;
    try {
      const result = await runInFrame(scope.frame, () =>
        callback(this.#connection)
      );
      this.#throwIfFailed(scope);
      signal?.throwIfAborted();
      return result;
    } catch (cause: unknown) {
      // The probe's failure explains whatever else the callback threw.
      throw this.#scopeFailure(scope) ?? cause;
    } finally {
      scope.pilotDepth--;
    }
  }

  /**
   * River's transaction that River's private connection stands for when a
   * pilot passes it as `{ tx }`: the one holding the connection, while a
   * pilot transaction is running in it and the caller runs inside it.
   * Anywhere else the private connection is never a valid `{ tx }`.
   */
  #pilotScope(): OwnedScope | null {
    const holder = this.#lockHolder;
    if (holder === null || holder.pilotDepth === 0) return null;
    for (const frame of openFrames()) {
      if (frame.kind === "river" && frame.scope === holder) return holder;
    }
    return null;
  }

  /**
   * Run a pilot's callback in a new transaction River owns on its private
   * connection, or directly in `tx`, opening no savepoint. River's
   * transaction begins before
   * the callback runs, retrying a busy database first, and like any River
   * transaction must end in the event loop turn it began in.
   */
  async #pilotTransaction<Result>(
    callback: (tx: DatabaseSync) => PromiseLike<Result> | Result,
    options: {
      readonly signal?: AbortSignal;
      readonly tx?: DatabaseSync | SqliteRiverScope;
    } = {}
  ): Promise<Result> {
    const operation = "pilot_transaction";
    const signal = options.signal;
    signal?.throwIfAborted();
    if (options.tx !== undefined) {
      const target = this.#resolveTransaction(options.tx, operation);
      if (target instanceof OwnedScope) {
        return this.#pilotInScope(target, callback, signal);
      }
      try {
        const result = await callback(target);
        signal?.throwIfAborted();
        return result;
      } catch (cause: unknown) {
        throw this.#wrapError(operation, cause);
      }
    }
    this.#assertOpen(operation);
    this.#assertNotReentrant(operation, true);
    const scope = new OwnedScope(this, this.#key);
    scope.pilotDepth = 1;
    let result: Result;
    try {
      result = await runInFrame(scope.frame, async () => {
        await this.#beginScope(scope, signal);
        const value = await callback(this.#connection);
        this.#throwIfFailed(scope);
        signal?.throwIfAborted();
        return value;
      });
    } catch (error: unknown) {
      await this.#endScope(scope, false).catch(() => undefined);
      throw this.#scopeFailure(scope) ?? error;
    }
    await this.#endScope(scope, true);
    return result;
  }

  async #queueSetPaused(
    name: string,
    paused: boolean,
    options: SqliteOperationOptions & { now?: Temporal.Instant }
  ): Promise<readonly SqliteQueueRow[]> {
    validateLookupName(name, "name");
    const now = options.now ?? Temporal.Now.instant();
    return this.#write(
      paused ? "queue_pause" : "queue_resume",
      options,
      (database) => {
        const pausedSql = paused ? "coalesce(paused_at, ?)" : "NULL";
        const changedSql = paused
          ? "paused_at IS NULL"
          : "paused_at IS NOT NULL";
        const statement = resultStatement(
          database,
          `
        UPDATE river_queue
        SET
          paused_at = ${pausedSql},
          updated_at = CASE WHEN ${changedSql} THEN ? ELSE updated_at END
        WHERE (? = '*' OR name = ?) AND ${changedSql}
        RETURNING ${QUEUE_COLUMNS}
        `
        );
        const timestamp = sqliteTimestamp(now);
        const rows = paused
          ? statement.all(timestamp, timestamp, name, name)
          : statement.all(timestamp, name, name);
        const queues = rows.map(decodeQueueRow);
        if (queues.length > 0) {
          insertNotification(
            database,
            NOTIFICATION_TOPIC_CONTROL,
            `{"action":"${paused ? "pause" : "resume"}","queue":${JSON.stringify(name)}}`
          );
        }
        return queues;
      }
    );
  }

  /**
   * Run a read. Inside a caller transaction it sees that transaction's
   * writes; otherwise it waits for the handle and retries a busy database.
   */
  async #read<T>(
    operation: string,
    options: SqliteOperationOptions,
    callback: (database: DatabaseSync) => T
  ): Promise<T> {
    if (options.tx !== undefined) {
      const target = this.#resolveTransaction(options.tx, operation);
      if (target instanceof OwnedScope) {
        return this.#inScope(target, operation, (database) =>
          callback(database)
        );
      }
      return this.#databaseOperation(operation, () => callback(target));
    }
    this.#assertNotReentrant(operation, false);
    return this.#exclusive(operation, () =>
      this.#retryBusy(operation, () => callback(this.#connection))
    );
  }

  /**
   * Finish the switch to WAL that construction found the database too busy
   * for, retrying asynchronously like any other River statement. Call with
   * River's lock held.
   */
  async #switchToWal(operation: string): Promise<void> {
    if (!this.#walPending) return;
    await this.#retryBusy(operation, () => {
      switchToWal(this.#connection);
    });
    this.#walPending = false;
  }

  async #retryBusy<T>(operation: string, attempt: () => T): Promise<T> {
    try {
      return await retryBusy(this.#busyPolicy, attempt);
    } catch (cause: unknown) {
      throw this.#wrapError(operation, cause);
    }
  }

  /** The error for a River call that would wait for its own transaction. */
  #reentrantError(
    operation: string,
    sameDriver: boolean
  ): TransactionScopeError {
    return new TransactionScopeError(
      "reentrant",
      `SQLite River operation ${operation} was called without { tx } from ` +
        "inside River's own transaction " +
        (sameDriver
          ? "on this driver"
          : "on another SqliteDriver for the same database") +
        ", which insert middleware and hooks run in. River holds the " +
        "database's write lock until the middleware or hook returns, so " +
        "the call would wait for it forever. Move the call before next() " +
        "or out of the hook, or react to committed jobs with " +
        "client.subscribe",
      { details: { backend: SQLITE_BACKEND, operation } }
    );
  }

  /**
   * Resolve `{ tx }` to River's own transaction on this driver or an
   * application handle on this driver's database with a transaction open.
   */
  #resolveTransaction(
    tx: unknown,
    operation: string
  ): DatabaseSync | OwnedScope {
    if (tx instanceof OwnedScope) {
      if (tx.driver !== this) {
        throw backendMismatchError(
          operation,
          "SQLite transaction belongs to another SqliteDriver"
        );
      }
      if (tx.state === "ended") {
        throw backendMismatchError(
          operation,
          "SQLite River transaction already ended"
        );
      }
      return tx;
    }
    if (!(tx instanceof DatabaseSync)) {
      throw backendMismatchError(
        operation,
        "SQLite { tx } must be a node:sqlite DatabaseSync with an open transaction"
      );
    }
    if (tx === this.#connection) {
      const admitted = this.#pilotScope();
      if (admitted !== null) return admitted;
      throw backendMismatchError(
        operation,
        "River's private SQLite connection can't be passed as { tx }"
      );
    }
    if (!tx.isOpen || !this.#isSameDatabase(tx)) {
      throw backendMismatchError(
        operation,
        "SQLite { tx } is not open on this driver's database; open " +
          "application handles on the same file or with driver.connect()"
      );
    }
    if (!tx.isTransaction) {
      throw new TransactionScopeError(
        "no_transaction",
        "SQLite { tx } has no open transaction; begin one with " +
          "transaction(db, …) or BEGIN IMMEDIATE and pass the handle while " +
          "it is open",
        { details: { backend: SQLITE_BACKEND, operation } }
      );
    }
    return tx;
  }

  /**
   * Roll back River's transaction when it is still open at the event loop's
   * next turn, which releases SQLite's write lock for every other writer at
   * once, and fail the operation. Committing may legitimately wait across
   * turns while COMMIT is busy.
   */
  #probeScope(scope: OwnedScope): void {
    scope.probe = null;
    if (scope.state !== "open") return;
    this.#failScope(scope);
  }

  /** Why River's transaction `scope` failed, once it has, checking turns. */
  #scopeFailure(scope: OwnedScope): TransactionScopeError | null {
    this.#checkTurns(scope);
    return scope.failure;
  }

  /** Whether River's transaction `scope` is still open. */
  #scopeOpen(scope: OwnedScope): boolean {
    return scope.state === "open";
  }

  /** Throw why River's transaction `scope` failed, once it has. */
  #throwIfFailed(scope: OwnedScope): void {
    const failure = this.#scopeFailure(scope);
    if (failure !== null) throw failure;
  }

  /**
   * In strict mode, fail River's open transaction as the probe would once
   * any macrotask callback has run since it began, which catches I/O too
   * fast for the probe to see.
   */
  #checkTurns(scope: OwnedScope): void {
    if (
      this.#releaseTurnCounter !== null &&
      scope.state === "open" &&
      eventLoopTurns() !== scope.turnsAtBegin
    ) {
      this.#failScope(scope);
    }
  }

  /** Roll back River's transaction and fail its operation. */
  #failScope(scope: OwnedScope): void {
    this.#rollbackQuietly();
    scope.state = "rolled_back";
    scope.failure = new TransactionScopeError(
      "event_loop_turn",
      "River's SQLite transaction stayed open across a turn of the event " +
        "loop: " +
        (scope.pilotDepth > 0
          ? "a companion's transaction or operation interceptor awaited " +
            "I/O while River held SQLite's write lock, which blocks every " +
            "other writer. River rolled the operation back. Such a " +
            "transaction must await only promises, not I/O or timers"
          : "insert middleware or a hook awaited I/O after next() while " +
            "River held SQLite's write lock, which blocks every other " +
            "writer. River rolled the operation back. Move the I/O before " +
            "calling next(), or react to the committed job with " +
            "client.subscribe"),
      { details: { backend: SQLITE_BACKEND, operation: "transaction" } }
    );
    this.#releaseScope(scope);
  }

  /** Release the private connection a River-owned transaction holds. */
  #releaseScope(scope: OwnedScope): void {
    if (scope.probe !== null) {
      clearImmediate(scope.probe);
      scope.probe = null;
    }
    scope.holdsLock = false;
    if (this.#lockHolder === scope) this.#lockHolder = null;
    scope.release?.();
    scope.release = null;
  }

  /**
   * Wrap a failure: a `LifecycleError` once the driver is closed, and a
   * busy database with a hint when an application handle in this process
   * holds the lock, which a caller that forgot `{ tx }` can't wait out.
   */
  #wrapError(operation: string, cause: unknown): unknown {
    if (cause instanceof RiverError) return cause;
    const details = { backend: SQLITE_BACKEND, operation };
    if (this.#closed) {
      return new LifecycleError(
        `SQLite driver was closed while ${operation} ran`,
        { cause, details }
      );
    }
    if (isRetryableSqliteError(cause) && this.#applicationTransactionOpen()) {
      return databaseError(
        operation,
        `SQLite River operation ${operation} failed: ` +
          `${(cause as Error).message}. An application handle on this ` +
          "database in this process has a transaction open, such as one " +
          "begun with a raw BEGIN; if this call belongs to that " +
          "transaction, pass the handle as { tx }",
        { cause }
      );
    }
    return wrapSqliteError(operation, cause);
  }

  #rollbackQuietly(): void {
    if (!this.#connection.isOpen || !this.#connection.isTransaction) return;
    try {
      this.#connection.exec("ROLLBACK");
    } catch {
      // Preserve the operation failure as the primary cause.
    }
  }

  /**
   * Run `callback` in a write fenced by `leader`'s exact term. SQLite
   * statements can't be interrupted, so a `batch` only stops work that hasn't
   * started when its signal aborts.
   */
  async #withMaintenanceLeader<T>(
    leader: RuntimeLeader,
    operation: string,
    callback: (database: DatabaseSync) => T,
    batch?: RuntimeMaintenanceBatch,
    options: SqliteOperationOptions = {}
  ): Promise<T | null> {
    batch?.signal.throwIfAborted();
    return this.#write(operation, options, (database) => {
      batch?.signal.throwIfAborted();
      const current = leaderGet(database);
      if (
        current === null ||
        current.leaderId !== leader.leaderId ||
        !current.electedAt.equals(leader.electedAt) ||
        Temporal.Instant.compare(current.expiresAt, Temporal.Now.instant()) < 0
      ) {
        return null;
      }
      return callback(database);
    });
  }

  /**
   * Run a write in a transaction.
   *
   * With `tx` the operation runs directly in that transaction, like River
   * for Go, opening no savepoint: when it fails part way through, its
   * statements stay in the transaction until the transaction's owner rolls
   * it back. Otherwise it runs in its own `BEGIN IMMEDIATE`
   * transaction on River's connection; when another connection holds the
   * write lock the whole attempt rolls back and is retried after an
   * asynchronous backoff, so no transaction stays open across an await.
   */
  async #write<T>(
    operation: string,
    options: SqliteOperationOptions,
    callback: (database: DatabaseSync) => T
  ): Promise<T> {
    if (options.tx !== undefined) {
      const target = this.#resolveTransaction(options.tx, operation);
      if (target instanceof OwnedScope) {
        return this.#inScope(target, operation, callback);
      }
      return this.#databaseOperation(operation, () => callback(target));
    }

    this.#assertNotReentrant(operation, true);
    return this.#exclusive(operation, () =>
      this.#retryBusy(operation, () => {
        const database = this.#connection;
        database.exec("BEGIN IMMEDIATE");
        try {
          const result = callback(database);
          database.exec("COMMIT");
          return result;
        } catch (cause: unknown) {
          this.#rollbackQuietly();
          throw cause;
        }
      })
    );
  }
}

/** A pilot's notification topic, checked for untyped callers. */
function pilotTopic(topic: string): "control" | "insert" {
  if (topic === "control" || topic === "insert") return topic;
  throw invalidInputError(
    "notify",
    `River notifications can be sent on "control" or "insert", not ${JSON.stringify(topic)}`
  );
}

/** The `{ tx }` a pilot's statement must run in. */
function requireTx(
  options: { readonly tx?: unknown } | undefined
): SqliteOperationOptions {
  const tx = options?.tx;
  if (tx === undefined) {
    throw new TransactionScopeError(
      "no_transaction",
      "this pilot database operation requires { tx }",
      { details: { backend: SQLITE_BACKEND } }
    );
  }
  return { tx } as SqliteOperationOptions;
}

/**
 * Open River's private connection with a zero busy timeout: River retries a
 * busy database asynchronously instead of letting SQLite block the event
 * loop. A file database is switched to WAL, so application reads proceed
 * while River writes; an in-memory database has no WAL. A constructor can't
 * wait, so when another connection keeps the database busy, the switch is
 * left pending for River's first operation, which retries it asynchronously.
 */
function openConnection(
  location: string,
  file: boolean
): { connection: DatabaseSync; walPending: boolean } {
  const connection = new DatabaseSync(location, { timeout: 0 });
  try {
    if (!file) return { connection, walPending: false };
    try {
      switchToWal(connection);
    } catch (error: unknown) {
      if (!isRetryableSqliteError(error)) throw error;
      return { connection, walPending: true };
    }
    return { connection, walPending: false };
  } catch (error: unknown) {
    connection.close();
    throw error;
  }
}

/** Switch a file database to WAL, which SQLite records in the file. */
function switchToWal(connection: DatabaseSync): void {
  const mode = connection.prepare("PRAGMA journal_mode").get();
  if (mode?.journal_mode !== "wal") {
    connection.exec("PRAGMA journal_mode = WAL");
  }
}

/**
 * Read a job by ID. With `partial`, fields River can't decode are left empty
 * instead of throwing, so operations by ID work on any row.
 */
function getJob(
  database: DatabaseSync,
  id: bigint,
  options: { readonly partial?: boolean } = {}
): SqliteJobRow | null {
  const raw = resultStatement(
    database,
    `SELECT ${JOB_COLUMNS} FROM river_job WHERE id = ? LIMIT 1`
  ).get(id);
  if (raw === undefined) return null;
  return options.partial === true
    ? decodeJobRowPartial(raw).job
    : decodeJobRow(raw);
}

function insertBindings(
  values: InsertValues
): (bigint | number | string | Uint8Array | null)[] {
  return [
    values.id,
    values.args,
    values.attempt,
    values.attemptedAt,
    values.attemptedBy,
    values.createdAt,
    values.errors,
    values.finalizedAt,
    values.kind,
    values.maxAttempts,
    values.metadata,
    values.priority,
    values.queue,
    values.scheduledAt,
    values.state,
    values.tags,
    values.uniqueKey,
    values.uniqueStates,
  ];
}

function insertNotification(
  database: DatabaseSync,
  topic: string,
  payload: string
): void {
  resultStatement(
    database,
    "INSERT INTO river_notification (payload, topic) VALUES (?, ?)"
  ).run(payload, topic);
}

/** Write River for Go's insert notification for each of `queues`. */
function writeInsertNotifications(
  database: DatabaseSync,
  queues: readonly string[]
): void {
  for (const queue of queues) {
    // River for Go's `{"queue": %q}`, with a space after the colon.
    insertNotification(
      database,
      NOTIFICATION_TOPIC_INSERT,
      `{"queue": ${JSON.stringify(queue)}}`
    );
  }
}

function runtimeTopic(topic: string): RuntimeNotification["topic"] {
  switch (topic) {
    case "river_control":
      return "control";
    case "river_insert":
      return "insert";
    case "river_leadership":
      return "leadership";
    default:
      throw invalidRowError(
        "runtime_notification_subscribe",
        `unknown River notification topic ${JSON.stringify(topic)}`
      );
  }
}

function runtimeTopicName(topic: RuntimeNotification["topic"]): string {
  switch (topic) {
    case "control":
      return "river_control";
    case "insert":
      return "river_insert";
    case "leadership":
      return "river_leadership";
  }
}

function validateJobState(state: string): string {
  if (
    state !== "available" &&
    state !== "cancelled" &&
    state !== "completed" &&
    state !== "discarded" &&
    state !== "pending" &&
    state !== "retryable" &&
    state !== "running" &&
    state !== "scheduled"
  ) {
    throw invalidInputError(
      "insert",
      `invalid SQLite River input state: unknown state ${JSON.stringify(state)}`
    );
  }
  return state;
}

function waitForPoll(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(finish, milliseconds);
    timer.unref();
    signal.addEventListener("abort", finish, { once: true });

    function finish(): void {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    }
  });
}

/** Wrap a `node:sqlite` failure; River and other errors pass through. */
function wrapSqliteError(operation: string, cause: unknown): unknown {
  if (cause instanceof RiverError || !isSqliteError(cause)) return cause;
  return databaseError(
    operation,
    `SQLite River operation ${operation} failed: ${cause.message}`,
    { cause }
  );
}
