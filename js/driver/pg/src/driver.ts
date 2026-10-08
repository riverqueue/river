import { Buffer } from "node:buffer";
import type { Client as PgClient, ClientBase, Pool, PoolClient } from "pg";
import type { JobRow } from "riverqueue";
import type {
  DriverInsertResult,
  DriverRecord,
  InsertDriverOptions,
  JobClaimOptions,
  JobClaimParams,
  JobClaimResult,
  JobCompletionCommand,
  JobCompletionResult,
  JobDeleteManyParams,
  JobDeleteResult,
  JobInsertParams,
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
  RuntimeDriver,
  RuntimeWaitOptions,
} from "riverqueue/unstable-driver";
import { registerDriver } from "riverqueue/unstable-driver";
import { isQueryable, PgDatabase } from "./database.js";
import { configurationError, unsupportedError } from "./errors.js";
import { PgPilotDatabase } from "./pilot.js";
import type {
  PgDriverOptions,
  PgJobCancelParams,
  PgJobDeleteBeforeParams,
  PgJobRescueManyParams,
  PgJobRetryParams,
  PgJobScheduleResult,
  PgLeader,
  PgLeaderElectParams,
  PgLeaderTermParams,
  PgNotification,
  PgOperationOptions,
  PgQueueControlParams,
  PgQueueRow,
  PgQueueUpsertParams,
} from "./types.js";
import * as jobSql from "./sql/jobs.js";
import * as leaderSql from "./sql/leader.js";
import * as maintenanceSql from "./sql/maintenance.js";
import * as notifySql from "./sql/notify.js";
import * as queueSql from "./sql/queues.js";

const RIVER_SCHEMA_MAX_BYTES = 46;
const RIVER_SCHEMA_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * River's complete node-postgres backend.
 *
 * The pool/client is caller-owned, and `PgDriver` never closes it. Keep
 * your own reference to it: the driver exposes nothing but its
 * construction. Passing `{ tx }` keeps the entire operation on that exact
 * checked-out client, and River never ends that transaction. An insertion
 * without `{ tx }` runs in a transaction River begins on a connection it
 * leases from the pool, like River for Go, so a driver constructed from a
 * single client requires `{ tx }` for insertions.
 */
export class PgDriver {
  /**
   * Type-only marker: a full runtime driver whose transactions are any
   * node-postgres client (`pg.Client` or a `PoolClient` checked out from a
   * pool) after `BEGIN`.
   */
  declare readonly "~river"?: {
    readonly capability: "runtime";
    readonly transaction: ClientBase;
  };

  constructor(
    client: Pool | PoolClient | PgClient,
    options: PgDriverOptions = {}
  ) {
    const runtime = new PgRuntime(client, options);
    registerDriver<ClientBase>(this, runtime.driverRecord());
  }
}

/**
 * @internal A registered driver whose operations are callable, for this
 * package's tests.
 */
export function testPgDriver(
  client: Pool | PoolClient | PgClient,
  options: PgDriverOptions = {}
): PgRuntime {
  const runtime = new PgRuntime(client, options);
  registerDriver<ClientBase>(runtime, runtime.driverRecord());
  return runtime;
}

/**
 * @internal The operations behind a {@link PgDriver}, which River reaches
 * through its private driver registry.
 */
export class PgRuntime implements RuntimeDriver<ClientBase> {
  declare readonly "~river"?: {
    readonly capability: "runtime";
    readonly transaction: ClientBase;
  };

  readonly backend = "postgres" as const;

  /** The caller-owned pool, or null for a driver of a single client. */
  readonly pool: Pool | null;

  /** The pool or client this driver was constructed with. */
  readonly #connection: Pool | PoolClient | PgClient;

  /** Naming and execution shared by the query modules this class delegates to. */
  readonly #db: PgDatabase;

  constructor(
    client: Pool | PoolClient | PgClient,
    options: PgDriverOptions = {}
  ) {
    if (!isQueryable(client)) {
      throw configurationError(
        "construct",
        "PgDriver requires a node-postgres Pool or Client"
      );
    }

    const schema = options.schema;
    if (schema !== undefined) validateSchema(schema);

    this.#connection = client;
    this.#db = new PgDatabase(client, schema ?? null);
    this.pool = this.#db.pool;
  }

  /** What River's private registry records about this driver. */
  driverRecord(): DriverRecord<ClientBase> {
    return {
      backend: this.backend,
      capability: "runtime",
      migration:
        this.pool === null
          ? { client: this.#connection, schema: this.schema }
          : { pool: this.pool, schema: this.schema },
      operations: this,
      ...(this.pool === null
        ? {}
        : { database: new PgPilotDatabase(this.#db, this.pool) }),
    };
  }

  /**
   * The configured Postgres schema containing River's tables, or undefined
   * when River uses the connection's `search_path`.
   */
  get schema(): string | undefined {
    return this.#db.schemaName ?? undefined;
  }

  /**
   * Reject worker startup unless River can lease independent connections.
   *
   * @internal
   */
  runtimeStartPreflight(
    options = { maintenance: true, notifications: true, reindex: true }
  ): void {
    if (this.pool === null) {
      throw unsupportedError(
        "runtime",
        "the Postgres worker runtime requires PgDriver to be constructed with a Pool"
      );
    }
    const minimumPoolSize =
      1 +
      (options.notifications ? 1 : 0) +
      (options.maintenance ? 1 : 0) +
      (options.reindex ? 1 : 0);
    if (this.pool.options.max < minimumPoolSize) {
      throw configurationError(
        "runtimeStartPreflight",
        `the Postgres worker runtime requires Pool max to be at least ${minimumPoolSize} for the enabled worker, notification, maintenance, and reindex services`
      );
    }
  }

  /**
   * Cancel a job using River's canonical persisted cancellation semantics.
   *
   * @internal
   */
  jobCancel(id: bigint, options?: PgOperationOptions): Promise<JobRow | null> {
    return jobSql.jobCancel(this.#db, id, options);
  }

  /**
   * Backend test hook for deterministic cancellation clocks and topics.
   *
   * @internal
   */
  jobCancelWithOptions(
    params: PgJobCancelParams,
    options?: PgOperationOptions
  ): Promise<JobRow | null> {
    return jobSql.jobCancelWithOptions(this.#db, params, options);
  }

  /**
   * Delete a job unless it is currently running.
   *
   * @internal
   */
  jobDelete(
    id: bigint,
    options?: PgOperationOptions
  ): Promise<JobDeleteResult> {
    return jobSql.jobDelete(this.#db, id, options);
  }

  /**
   * Delete a bounded, explicitly authorized set of non-running jobs.
   *
   * @internal
   */
  jobDeleteMany(
    params: JobDeleteManyParams,
    options?: PgOperationOptions
  ): Promise<readonly JobRow[]> {
    return jobSql.jobDeleteMany(this.#db, params, options);
  }

  /**
   * Get a job by exact 64-bit ID.
   *
   * @internal
   */
  jobGet(id: bigint, options?: PgOperationOptions): Promise<JobRow | null> {
    return jobSql.jobGet(this.#db, id, options);
  }

  /**
   * Atomically claim runnable jobs using River's priority order and SKIP LOCKED.
   *
   * @internal
   */
  jobClaim(
    params: JobClaimParams,
    options?: JobClaimOptions<ClientBase>
  ): Promise<JobClaimResult> {
    return jobSql.jobClaim(this.#db, params, options);
  }

  /**
   * Persist attempt-conditional worker outcomes in one bounded query.
   *
   * @internal
   */
  jobCompleteMany(
    commands: readonly JobCompletionCommand[],
    options?: { readonly signal?: AbortSignal; readonly tx?: ClientBase }
  ): Promise<readonly JobCompletionResult[]> {
    return jobSql.jobCompleteMany(this.#db, commands, options);
  }

  /**
   * List jobs through a fixed parameterized filter grammar.
   *
   * @internal
   */
  jobList(
    params: JobListParams,
    options?: PgOperationOptions
  ): Promise<readonly JobRow[]> {
    return jobSql.jobList(this.#db, params, options);
  }

  /**
   * Merge metadata into a job while locking the target row.
   *
   * @internal
   */
  jobUpdate(
    id: bigint,
    params: JobUpdateParams,
    options?: PgOperationOptions
  ): Promise<JobRow | null> {
    return jobSql.jobUpdate(this.#db, id, params, options);
  }

  /**
   * Insert one job, or return the existing job that holds its unique key.
   *
   * Part of River's unstable driver interface, which `Client` calls;
   * applications insert through the client instead. Its parameter and result
   * types come from `riverqueue/unstable-driver` and may change in any
   * release.
   */
  jobInsert(
    params: JobInsertParams,
    options?: InsertDriverOptions<ClientBase>
  ): Promise<DriverInsertResult> {
    return jobSql.jobInsert(this.#db, params, options);
  }

  /**
   * Insert an ordered batch of jobs atomically.
   *
   * Part of River's unstable driver interface, which `Client` calls;
   * applications insert through the client instead. Its parameter and result
   * types come from `riverqueue/unstable-driver` and may change in any
   * release.
   */
  jobInsertMany(
    params: readonly JobInsertParams[],
    options?: InsertDriverOptions<ClientBase>
  ): Promise<readonly DriverInsertResult[]> {
    return jobSql.jobInsertMany(this.#db, params, options);
  }

  /**
   * The IDs among `ids` of running jobs with a cancellation request, which a
   * runtime without notifications polls for.
   *
   * @internal
   */
  jobGetCancelRequested(
    ids: readonly bigint[],
    options?: RuntimeWaitOptions
  ): Promise<readonly bigint[]> {
    return jobSql.jobGetCancelRequested(this.#db, ids, options);
  }

  /**
   * Retry a non-running job immediately using River's canonical transition.
   *
   * @internal
   */
  jobRetry(id: bigint, options?: PgOperationOptions): Promise<JobRow | null> {
    return jobSql.jobRetry(this.#db, id, options);
  }

  /**
   * Backend test hook for deterministic retry clocks.
   *
   * @internal
   */
  jobRetryWithOptions(
    params: PgJobRetryParams,
    options?: PgOperationOptions
  ): Promise<JobRow | null> {
    return jobSql.jobRetryWithOptions(this.#db, params, options);
  }

  /**
   * Delete terminal jobs below configured retention horizons.
   *
   * @internal
   */
  jobDeleteBefore(
    params: PgJobDeleteBeforeParams,
    options?: PgOperationOptions
  ): Promise<number> {
    return maintenanceSql.jobDeleteBefore(this.#db, params, options);
  }

  /**
   * Read running jobs old enough for rescuer inspection.
   *
   * @internal
   */
  jobGetStuck(
    params: { afterId?: bigint; max: number; stuckHorizon: Temporal.Instant },
    options?: PgOperationOptions
  ): Promise<readonly JobRow[]> {
    return maintenanceSql.jobGetStuck(this.#db, params, options);
  }

  /**
   * Rescue still-stuck running jobs without overwriting concurrent completion.
   *
   * @internal
   */
  jobRescueMany(
    params: PgJobRescueManyParams,
    options?: PgOperationOptions
  ): Promise<number> {
    return maintenanceSql.jobRescueMany(this.#db, params, options);
  }

  /**
   * Move due scheduled/retryable jobs into the runnable state.
   *
   * @internal
   */
  jobSchedule(
    params: {
      max: number;
      now?: Temporal.Instant;
      scheduledAtHorizon?: Temporal.Instant;
    },
    options?: PgOperationOptions
  ): Promise<readonly PgJobScheduleResult[]> {
    return maintenanceSql.jobSchedule(this.#db, params, options);
  }

  /**
   * Get a persisted queue by name.
   *
   * @internal
   */
  queueGet(
    name: string,
    options?: PgOperationOptions
  ): Promise<PgQueueRow | null> {
    return queueSql.queueGet(this.#db, name, options);
  }

  /**
   * Create a queue or refresh its liveness timestamp without erasing metadata.
   *
   * @internal
   */
  queueUpsert(
    params: PgQueueUpsertParams,
    options?: PgOperationOptions
  ): Promise<PgQueueRow> {
    return queueSql.queueUpsert(this.#db, params, options);
  }

  /**
   * Delete stale queue rows in stable name order.
   *
   * @internal
   */
  queueDeleteExpired(
    params: { max: number; updatedAtHorizon: Temporal.Instant },
    options?: PgOperationOptions
  ): Promise<readonly PgQueueRow[]> {
    return queueSql.queueDeleteExpired(this.#db, params, options);
  }

  /**
   * List persisted queues in canonical name order.
   *
   * @internal
   */
  queueList(
    params: QueueListParams,
    options?: PgOperationOptions
  ): Promise<readonly QueueRow[]> {
    return queueSql.queueList(this.#db, params, options);
  }

  /**
   * Pause one queue, or all queues with the `"*"` sentinel.
   *
   * @internal
   */
  queuePause(
    name: string,
    options?: PgOperationOptions
  ): Promise<PgQueueRow | null> {
    return queueSql.queuePause(this.#db, name, options);
  }

  /**
   * Backend test hook for deterministic queue pause clocks.
   *
   * @internal
   */
  queuePauseWithOptions(
    params: PgQueueControlParams,
    options?: PgOperationOptions
  ): Promise<number> {
    return queueSql.queuePauseWithOptions(this.#db, params, options);
  }

  /**
   * Resume one queue, or all queues with the `"*"` sentinel.
   *
   * @internal
   */
  queueResume(
    name: string,
    options?: PgOperationOptions
  ): Promise<PgQueueRow | null> {
    return queueSql.queueResume(this.#db, name, options);
  }

  /**
   * Backend test hook for deterministic queue resume clocks.
   *
   * @internal
   */
  queueResumeWithOptions(
    params: PgQueueControlParams,
    options?: PgOperationOptions
  ): Promise<number> {
    return queueSql.queueResumeWithOptions(this.#db, params, options);
  }

  /**
   * Update the mutable fields of a persisted queue.
   *
   * @internal
   */
  queueUpdate(
    name: string,
    params: QueueUpdateParams,
    options?: PgOperationOptions
  ): Promise<PgQueueRow | null> {
    return queueSql.queueUpdate(this.#db, name, params, options);
  }

  /**
   * Attempt to acquire the singleton River leadership lease.
   *
   * @internal
   */
  leaderElect(
    params: PgLeaderElectParams,
    options?: PgOperationOptions
  ): Promise<PgLeader | null> {
    return leaderSql.leaderElect(this.#db, params, options);
  }

  /**
   * Renew a leadership lease only for the exact current election term.
   *
   * @internal
   */
  leaderReelect(
    params: PgLeaderTermParams,
    options?: PgOperationOptions
  ): Promise<PgLeader | null> {
    return leaderSql.leaderReelect(this.#db, params, options);
  }

  /**
   * Read the currently persisted leader, whether or not its lease is expired.
   *
   * @internal
   */
  leaderGet(options?: PgOperationOptions): Promise<PgLeader | null> {
    return leaderSql.leaderGet(this.#db, options);
  }

  /**
   * Remove expired leadership rows so a new election can proceed.
   *
   * @internal
   */
  leaderDeleteExpired(
    now?: Temporal.Instant,
    options?: PgOperationOptions
  ): Promise<number> {
    return leaderSql.leaderDeleteExpired(this.#db, now, options);
  }

  /**
   * Resign only the exact election term and notify leadership observers.
   *
   * @internal
   */
  leaderResign(
    params: PgLeaderTermParams & { leadershipTopic: string },
    options?: PgOperationOptions
  ): Promise<boolean> {
    return leaderSql.leaderResign(this.#db, params, options);
  }

  /**
   * Notify producers of new jobs in each of `queues`.
   *
   * Part of River's unstable driver interface, which `Client` calls after
   * inserting jobs. Its parameter types come from
   * `riverqueue/unstable-driver` and may change in any release.
   */
  notifyInsert(
    queues: readonly string[],
    options?: InsertDriverOptions<ClientBase>
  ): Promise<void> {
    return notifySql.notifyInsert(this.#db, queues, options);
  }

  /**
   * Send one or more Postgres notifications on a River topic.
   *
   * @internal
   */
  notifyMany(
    topic: string,
    payloads: readonly string[],
    options?: PgOperationOptions
  ): Promise<void> {
    return notifySql.notifyMany(this.#db, topic, payloads, options);
  }

  /**
   * Yield namespaced Postgres notifications with reconnect recovery.
   *
   * Notifications are hints only: callers must retain polling because NOTIFY
   * is not durable and a connection may be between reconnect attempts. An
   * idle connection is pinged every five seconds, like River's Go notifier,
   * so a half-open socket is detected and replaced instead of silently
   * dropping every later notification. Connecting and subscribing are
   * bounded by a ten-second timeout and stop as soon as `signal` aborts.
   * `ready` runs after each successful (re)connection so callers can poll
   * for anything missed meanwhile.
   *
   * @internal
   */
  listen(
    topics: readonly string[],
    signal: AbortSignal,
    ready?: () => void,
    options: {
      readonly pingIntervalMs?: number;
      readonly setupTimeoutMs?: number;
    } = {}
  ): AsyncGenerator<PgNotification> {
    return notifySql.listen(this.#db, topics, signal, ready, options);
  }

  /**
   * Whether the server delivers notifications, detected the first time and
   * cached for this driver's lifetime. YugabyteDB without
   * `yb_enable_listen_notify` doesn't, so its runtimes poll instead.
   *
   * @internal
   */
  async runtimeDeliversNotifications(
    options?: RuntimeWaitOptions
  ): Promise<boolean> {
    const capabilities = await this.#db.withConnection(
      options?.signal,
      (connection) => this.#db.capabilities(connection)
    );
    return capabilities.supportsListenNotify;
  }

  /**
   * Adapt namespaced backend hints to the common runtime notification SPI.
   *
   * @internal
   */
  runtimeNotificationSubscribe(
    topics: readonly RuntimeNotification["topic"][],
    signal: AbortSignal,
    ready: () => void
  ): AsyncGenerator<RuntimeNotification> {
    return notifySql.runtimeNotificationSubscribe(
      this.#db,
      topics,
      signal,
      ready
    );
  }

  /**
   * Run one River operation in a transaction, like River for Go's
   * `dbutil.WithTxV`. With `tx`, the operation joins that caller-owned
   * transaction. Otherwise River leases a pool connection and runs `BEGIN`,
   * then commits when `callback` resolves and rolls back when it rejects.
   *
   * Like River for Go without a pool, a driver constructed from a single
   * client can't open transactions of its own.
   *
   * @internal
   */
  async operationScope<T>(
    tx: ClientBase | undefined,
    callback: (tx: ClientBase) => Promise<T>
  ): Promise<T> {
    if (tx !== undefined) return callback(tx);
    if (this.pool === null) {
      throw configurationError(
        "operation_scope",
        "PgDriver was constructed with a single client, so River can't open " +
          "a transaction of its own for this operation; pass { tx } or " +
          "construct PgDriver with a Pool"
      );
    }
    return this.#db.transaction(
      "operationScope",
      this.pool,
      undefined,
      callback
    );
  }

  /**
   * Broadcast a request for whichever runtime currently leads to resign.
   *
   * @internal
   */
  runtimeRequestLeadershipResignation(
    options?: PgOperationOptions
  ): Promise<void> {
    return this.notifyMany(
      "river_leadership",
      // River for Go's payload names no leader.
      ['{"action":"request_resign","leader_id":""}'],
      options
    );
  }

  /**
   * Refresh a configured runtime queue without replacing controls/metadata.
   *
   * @internal
   */
  runtimeQueueUpsert(
    name: string,
    now: Temporal.Instant,
    options?: RuntimeWaitOptions
  ): Promise<QueueRow> {
    return this.#db.withConnection(options?.signal, (connection) =>
      queueSql.queueUpsert(this.#db, { name, now }, connection)
    );
  }

  /**
   * Acquire or renew one exact leadership lease after expiring stale terms.
   *
   * @internal
   */
  maintenanceLeaderAcquire(
    leaderId: string,
    _now: Temporal.Instant,
    ttlMs: number,
    held: RuntimeLeader | null,
    options?: RuntimeWaitOptions
  ): Promise<RuntimeLeader | null> {
    return maintenanceSql.maintenanceLeaderAcquire(
      this.#db,
      leaderId,
      ttlMs,
      held,
      options?.signal
    );
  }

  /** @internal */
  maintenanceLeaderResign(leader: RuntimeLeader): Promise<boolean> {
    return maintenanceSql.maintenanceLeaderResign(this.#db, leader);
  }

  /** @internal */
  maintenanceSchedule(
    leader: RuntimeLeader,
    params: RuntimeScheduleParams,
    batch?: RuntimeMaintenanceBatch
  ): Promise<number> {
    return maintenanceSql.maintenanceSchedule(this.#db, leader, params, batch);
  }

  /** @internal */
  maintenanceGetStuck(
    leader: RuntimeLeader,
    attemptedBefore: Temporal.Instant,
    afterId: bigint,
    limit: number,
    batch?: RuntimeMaintenanceBatch
  ): Promise<readonly JobRow[]> {
    return maintenanceSql.maintenanceGetStuck(
      this.#db,
      leader,
      attemptedBefore,
      afterId,
      limit,
      batch
    );
  }

  /** @internal */
  maintenanceRescue(
    leader: RuntimeLeader,
    attemptedBefore: Temporal.Instant,
    jobs: readonly RuntimeJobRescue[],
    options?: InsertDriverOptions<ClientBase>
  ): Promise<number> {
    return maintenanceSql.maintenanceRescue(
      this.#db,
      leader,
      attemptedBefore,
      jobs,
      options?.tx
    );
  }

  /** @internal */
  maintenanceCleanJobs(
    leader: RuntimeLeader,
    params: RuntimeJobCleanupParams,
    timeoutMs: number | null,
    signal: AbortSignal
  ): Promise<number> {
    return maintenanceSql.maintenanceCleanJobs(
      this.#db,
      leader,
      params,
      timeoutMs,
      signal
    );
  }

  /** @internal */
  maintenanceCleanQueues(
    leader: RuntimeLeader,
    updatedBefore: Temporal.Instant,
    limit: number,
    batch?: RuntimeMaintenanceBatch
  ): Promise<number> {
    return maintenanceSql.maintenanceCleanQueues(
      this.#db,
      leader,
      updatedBefore,
      limit,
      batch
    );
  }

  /**
   * Convert control-topic notifications into attempt-owner cancellation hints.
   *
   * @internal
   */
  jobCancellationSubscribe(
    attemptedBy: string,
    signal: AbortSignal,
    ready?: () => void
  ): AsyncGenerator<{ attemptedBy: string; id: bigint }> {
    return notifySql.jobCancellationSubscribe(
      this.#db,
      attemptedBy,
      signal,
      ready
    );
  }

  /**
   * Delete up to `max` durable SQLite-style notification rows retained by
   * migration v7 from before a horizon, oldest first.
   *
   * @internal
   */
  notificationDeleteBefore(
    params: { createdAtHorizon: Temporal.Instant; max: number },
    options?: PgOperationOptions
  ): Promise<number> {
    return maintenanceSql.notificationDeleteBefore(this.#db, params, options);
  }

  /**
   * Discover `_ccnew`/`_ccold` artifacts from an interrupted reindex.
   *
   * @internal
   */
  indexReindexArtifacts(
    index: string,
    options?: PgOperationOptions
  ): Promise<readonly string[]> {
    return maintenanceSql.indexReindexArtifacts(this.#db, index, options);
  }

  /**
   * Reindex one allow-listed River index using a safely quoted identifier.
   *
   * @internal
   */
  indexReindex(index: string, options?: PgOperationOptions): Promise<void> {
    return maintenanceSql.indexReindex(this.#db, index, options);
  }

  /**
   * Drop a concurrent-reindex artifact using a safely quoted identifier.
   *
   * @internal
   */
  indexDropIfExists(
    index: string,
    options?: PgOperationOptions
  ): Promise<void> {
    return maintenanceSql.indexDropIfExists(this.#db, index, options);
  }

  /**
   * Return exact existence results for indexes in the configured schema.
   *
   * @internal
   */
  indexesExist(
    indexes: readonly string[],
    options?: PgOperationOptions
  ): Promise<ReadonlyMap<string, boolean>> {
    return maintenanceSql.indexesExist(this.#db, indexes, options);
  }

  /**
   * Rebuild existing configured indexes with River's artifact safeguards.
   *
   * @internal
   */
  maintenanceReindex(
    leader: RuntimeLeader,
    indexes: readonly string[],
    timeoutMs: number | null,
    signal: AbortSignal
  ): Promise<number> {
    return maintenanceSql.maintenanceReindex(
      this.#db,
      leader,
      indexes,
      timeoutMs,
      signal
    );
  }
}

function validateSchema(value: string): void {
  if (!RIVER_SCHEMA_RE.test(value)) {
    throw configurationError(
      "construct",
      "Postgres schema must start with a letter or underscore and contain only letters, numbers, and underscores"
    );
  }
  if (Buffer.byteLength(value, "utf8") > RIVER_SCHEMA_MAX_BYTES) {
    throw configurationError(
      "construct",
      `Postgres schema must not exceed ${RIVER_SCHEMA_MAX_BYTES} bytes so River notification topics remain valid`
    );
  }
}
