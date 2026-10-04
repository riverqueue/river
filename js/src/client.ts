import { createHash } from "node:crypto";

import type {
  ClientDriver,
  DriverCapability,
  DriverInsertResult,
  InsertDriver,
  InsertDriverOptions,
  JobInsertParams,
  QueueRow,
  RuntimeDriver,
} from "./driver.js";
import {
  ConfigurationError,
  JobRunningError,
  LifecycleError,
  ValidationError,
} from "./errors.js";
import { UnsupportedCapabilityError } from "./errors.js";
import { EventHub } from "./events.js";
import { EventDispatcher } from "./internal/event-dispatcher.js";
import { bytesToHex } from "./internal/hex.js";
import type {
  EventSubscription,
  RiverEvent,
  RiverEventKind,
  SubscribedEvent,
  SubscribeOptions,
} from "./events.js";
import type {
  InsertContext,
  InsertMiddleware,
  RiverHooks,
} from "./extensions.js";
import type { JobDefinition, JobDefinitionInput } from "./job-definition.js";
import { prepareJobInput } from "./job-definition.js";
import type {
  InsertOptions,
  NormalizedInsertOptions,
  NormalizedUniqueOptions,
  ResolvedInsertOptions,
  UniqueOptions,
} from "./insert-options.js";
import {
  normalizeInsertOptions,
  normalizeUniqueOptions,
  parseUniquePath,
  resolveScheduledAt,
  uniquePeriodNanoseconds,
} from "./insert-options.js";
import {
  JOB_STATE,
  MAX_ATTEMPTS_DEFAULT,
  PRIORITY_DEFAULT,
  QUEUE_DEFAULT,
} from "./job.js";
import type { JobRow, JobState } from "./job.js";
import type { JsonObject } from "./json.js";
import {
  isExactJsonNumber,
  parseJson,
  stringifyJson,
  stringifySelectedUniqueJson,
  stringifyUniqueJson,
  toJsonObject,
} from "./json.js";
import {
  getJobArgsTransformers,
  transformJobArgsForInsert,
  transformJobArgsForRead,
} from "./job-args-transform.js";
import type { JobArgsTransformer } from "./job-args-transform.js";
import {
  getJobInsertMetadataTransformers,
  transformJobInsertMetadata,
} from "./job-insert-metadata-transform.js";
import type { JobInsertMetadataTransformer } from "./job-insert-metadata-transform.js";
import { uniqueBitmaskFromStates } from "./unique-bitmask.js";
import { assertRuntimeSupport } from "./runtime-support.js";
import type {
  JobListOptions,
  JobListResult,
  JobDeleteManyOptions,
  JobUpdateOptions,
  QueueListOptions,
  QueueListResult,
  QueueUpdateOptions,
} from "./query.js";
import {
  encodeJobListCursor,
  encodeQueueCursor,
  normalizeJobDeleteManyOptions,
  normalizeJobListOptions,
  normalizeJobUpdateOptions,
  normalizeQueueListOptions,
} from "./query.js";
import type { RuntimeBinding, RuntimeSettings } from "./runtime.js";
import {
  normalizeRuntimeSettings,
  RunHandle,
  RuntimeController,
} from "./runtime.js";
import { driverRecord, pilotFactory } from "./internal/driver-registry.js";
import { withHandle } from "./internal/handle-gate.js";
import { InsertNotifyLimiter } from "./internal/insert-notify-limiter.js";
import { millisecondsToDuration } from "./internal/duration.js";
import type { PeriodicJobStore } from "./periodic-job-store.js";
import type {
  DriverRecord,
  Pilot,
  PilotAttempts,
  PilotDatabase,
  PilotFactory,
  PilotHost,
  PilotInterceptors,
  PreparedInsertParams,
} from "./pilot.js";
import {
  PilotOperations,
  serializedDatabase,
  validatePreparedParams,
} from "./runtime/pilot-operations.js";
import {
  DEFAULT_FETCH_COOLDOWN_MS,
  makeClientId,
  resolveQueues,
  type PilotQueueParser,
} from "./runtime/settings.js";
import {
  disablePeriodicJobs,
  PeriodicJobs,
  periodicOccurrenceOptions,
} from "./periodic.js";
import { toRuntimeSettings, type ClientOptions } from "./options.js";
import { queueLookupName } from "./identifiers.js";
import type { PeerAttempts } from "./runtime/peer-attempts.js";
import {
  internalLogger,
  resolveLogger,
  type InternalLogger,
} from "./logger.js";

const NANOSECONDS_FROM_YEAR_ONE_TO_UNIX_EPOCH =
  62_135_596_800n * 1_000_000_000n;

const DEFAULT_UNIQUE_STATES: readonly JobState[] = Object.freeze([
  JOB_STATE.available,
  JOB_STATE.completed,
  JOB_STATE.pending,
  JOB_STATE.retryable,
  JOB_STATE.running,
  JOB_STATE.scheduled,
]);

export interface InsertResult<Args extends object = JsonObject> {
  /** Inserted row, or the conflicting row for a duplicate unique job. */
  readonly job: JobRow<Args>;

  /** Whether River inserted a new row or returned a unique conflict. */
  readonly status: "duplicate" | "inserted";
}

/** One plain-object item in an insertMany call. */
export interface InsertManyItem<
  Definition extends JobDefinition = JobDefinition<JsonObject>,
> {
  readonly args: JobDefinitionInput<Definition>;
  readonly job: Definition;
  readonly options?: InsertOptions;
}

/** Checks each batch item's args against its own item's definition. */
export type CheckedInsertManyItems<Items extends readonly InsertManyItem[]> = {
  readonly [Index in keyof Items]: Items[Index] extends {
    readonly job: infer Definition extends JobDefinition;
  }
    ? Omit<Items[Index], "args"> & {
        readonly args: JobDefinitionInput<Definition>;
      }
    : never;
};

/** Exact result tuple corresponding to a heterogeneous insertion tuple. */
export type InsertManyResults<Items extends readonly InsertManyItem[]> = {
  readonly [Index in keyof Items]: Items[Index] extends {
    readonly job: infer Definition extends JobDefinition;
  }
    ? InsertResult<JobDefinitionInput<Definition>>
    : never;
};

/** Operation options that may run in a caller-owned transaction. */
export interface TransactionOptions<Transaction = unknown> {
  /**
   * Run the operation in this caller-owned transaction, such as a
   * node-postgres client after `BEGIN`. Like River for Go, River runs its
   * statements directly in it, opening no savepoint, and never commits or
   * rolls it back. When the operation fails, writes it already made, such
   * as a job inserted before insert middleware or a hook threw, stay in the
   * transaction, so roll it back. To recover from a failure and continue
   * the transaction, wrap the call in a savepoint of your own.
   */
  tx?: Transaction;
}

/**
 * Job insertion, available with every driver including insert-only ones such
 * as `@riverqueue/driver-prisma`. Type producer-only code against this
 * interface so it accepts full clients and test clients alike.
 */
export interface InsertClient<Transaction = unknown> {
  /**
   * Validate and insert one job, optionally in a caller-owned transaction.
   *
   * Resolves with `status: "duplicate"` and the existing row when a unique
   * job already exists.
   */
  insert<Definition extends JobDefinition>(
    definition: Definition,
    args: JobDefinitionInput<Definition>,
    options?: InsertOptions & TransactionOptions<Transaction>
  ): Promise<InsertResult<JobDefinitionInput<Definition>>>;

  /**
   * Insert a heterogeneous batch atomically, preserving input order in the
   * result tuple. An empty batch resolves to `[]` without a database call.
   */
  insertMany<const Items extends readonly InsertManyItem[]>(
    items: Items & CheckedInsertManyItems<Items>,
    options?: TransactionOptions<Transaction>
  ): Promise<InsertManyResults<Items>>;
}

/** Job queries and controls, available as {@link Client.jobs}. */
export interface JobOperations<Transaction = unknown> {
  /**
   * Cancel a job. A running attempt anywhere in the fleet is asked to stop
   * cooperatively through its `signal`; returns null when the job does not
   * exist.
   */
  cancel(
    id: bigint,
    options?: TransactionOptions<Transaction>
  ): Promise<JobRow | null>;
  /**
   * Delete a job that is not running, returning null when it does not exist.
   * Throws {@link JobRunningError} for a running job.
   */
  delete(
    id: bigint,
    options?: TransactionOptions<Transaction>
  ): Promise<JobRow | null>;
  /** Delete a bounded, explicitly filtered set of non-running jobs. */
  deleteMany(
    options: JobDeleteManyOptions & TransactionOptions<Transaction>
  ): Promise<readonly JobRow[]>;
  /** Get one job, returning null when it does not exist. */
  get(
    id: bigint,
    options?: TransactionOptions<Transaction>
  ): Promise<JobRow | null>;
  /** List jobs with exact, opaque keyset pagination through `nextCursor`. */
  list(
    options?: JobListOptions & TransactionOptions<Transaction>
  ): Promise<JobListResult>;
  /**
   * Make a job that is not running immediately available for another
   * attempt. Returns null when it does not exist.
   */
  retry(
    id: bigint,
    options?: TransactionOptions<Transaction>
  ): Promise<JobRow | null>;
  /**
   * Merge metadata into a job and set its output, like River for Go's
   * `JobUpdate`. Returns null when the job does not exist.
   */
  update(
    id: bigint,
    updates: JobUpdateOptions,
    options?: TransactionOptions<Transaction>
  ): Promise<JobRow | null>;
}

/**
 * Queue queries and controls, available as {@link Client.queues}. Like River
 * for Go, these look queues up by
 * name without checking it against the queue-name grammar, so a name no
 * queue can have is simply not found (null).
 */
export interface QueueOperations<Transaction = unknown> {
  /** Get one queue, returning null when it does not exist. */
  get(
    name: string,
    options?: TransactionOptions<Transaction>
  ): Promise<QueueRow | null>;
  /** List queues with opaque name pagination through `nextCursor`. */
  list(
    options?: QueueListOptions & TransactionOptions<Transaction>
  ): Promise<QueueListResult>;
  /**
   * Pause a queue across the fleet. Returns null when the queue does not
   * exist.
   *
   * `"*"` pauses every queue, like River for Go, and always resolves null
   * because no single queue row describes the result.
   */
  pause(
    name: string,
    options?: TransactionOptions<Transaction>
  ): Promise<QueueRow | null>;
  /**
   * Resume a paused queue across the fleet. Returns null when the queue does
   * not exist.
   *
   * `"*"` resumes every queue, like River for Go, and always resolves null
   * because no single queue row describes the result.
   */
  resume(
    name: string,
    options?: TransactionOptions<Transaction>
  ): Promise<QueueRow | null>;
  /** Update queue metadata. Returns null when the queue does not exist. */
  update(
    name: string,
    updates: QueueUpdateOptions,
    options?: TransactionOptions<Transaction>
  ): Promise<QueueRow | null>;
}

/**
 * A River client: typed job insertion plus, for drivers that support it, job
 * and queue operations and the worker runtime.
 *
 * `Transaction` is the driver's caller-owned transaction type (for example a
 * node-postgres client); it is inferred from the driver passed to
 * `new Client(driver)`.
 */
export interface Client<
  Transaction = unknown,
> extends InsertClient<Transaction> {
  /** Job queries and controls. */
  readonly jobs: JobOperations<Transaction>;
  /**
   * Dynamically configurable leader-owned periodic jobs. Modifying them throws
   * a {@link ConfigurationError} when the client was created with
   * `leaderElectionDisabled: true`, because it never leads.
   */
  readonly periodicJobs: PeriodicJobs;
  /** Queue queries and controls. */
  readonly queues: QueueOperations<Transaction>;

  /**
   * Ask whichever client currently leads maintenance to resign so another
   * can take over, optionally when a caller-owned transaction commits.
   */
  requestLeadershipResignation(
    options?: TransactionOptions<Transaction>
  ): Promise<void>;

  /**
   * Start the worker runtime: queues, workers, notifications, and (when
   * elected leader) maintenance. A client starts at most once; stop it with
   * the returned handle or `await using`.
   */
  start(): Promise<RunHandle>;

  /**
   * Subscribe to bounded job, queue, leadership, and maintenance events,
   * emitted after their database transitions commit. Filtering by `kinds`
   * narrows the yielded event type.
   *
   * @example
   * ```ts
   * using failures = client.subscribe({ kinds: ["job_failed"] });
   * for await (const event of failures) {
   *   if (event.kind === "job_failed") report(event.job, event.error);
   * }
   * ```
   */
  subscribe<Kind extends RiverEventKind = RiverEventKind>(
    options?: SubscribeOptions<Kind>
  ): EventSubscription<SubscribedEvent<Kind>>;
}

/**
 * Constructor for {@link Client}.
 *
 * A driver that supports the worker runtime, such as `PgDriver` or
 * `SqliteDriver`, produces a full {@link Client}. An insert-only driver such as
 * `PrismaDriver` produces an {@link InsertClient}, so runtime-only calls fail at
 * compile time (and with {@link UnsupportedCapabilityError} from untyped
 * JavaScript).
 */
export interface ClientConstructor {
  /** Create a client for a driver that supports the worker runtime. */
  new <Transaction>(
    driver: ClientDriver<Transaction, "runtime">,
    options?: ClientOptions<Transaction>
  ): Client<Transaction>;
  /** Create an insert-only client for an insertion-only driver. */
  new <Transaction>(
    driver: ClientDriver<Transaction, "insert">,
    options?: ClientOptions<Transaction>
  ): InsertClient<Transaction>;
  readonly prototype: Client;
}

/**
 * @internal The client implementation behind {@link Client}, which
 * `PilotClient` extends.
 */
export class RiverClient<Transaction = unknown> implements Client<Transaction> {
  readonly jobs: JobOperations<Transaction>;
  readonly periodicJobs: PeriodicJobs;
  readonly queues: QueueOperations<Transaction>;
  /** The backend's name, for errors. */
  readonly #backend: string;
  readonly #defaultInsertOptions: Readonly<NormalizedInsertOptions>;
  /** The registered operations of the client's driver. */
  readonly #driver: InsertDriver<Transaction, DriverCapability>;
  readonly #driverCapability: DriverCapability;
  readonly #eventHub = new EventHub();
  readonly #eventHooks = new EventDispatcher<RiverEvent>(
    (event) => this.#deliverEventHooks(event),
    EVENT_HOOK_QUEUE_CAPACITY
  );
  readonly #hooks: readonly RiverHooks[];
  readonly #insertMiddleware: readonly InsertMiddleware[];
  readonly #jobArgsTransformers: readonly Readonly<JobArgsTransformer>[];
  readonly #jobInsertMetadataTransformers: readonly Readonly<JobInsertMetadataTransformer>[];
  /** The client's pilot and its database, when it has one. */
  readonly #attached: AttachedPilot<Transaction> | undefined;
  /** Whether the pilot's `init` is running, when the client is unusable. */
  #initializing = false;
  /** Suppresses repeated insert notifications for a queue. */
  readonly #insertNotifyLimiter: InsertNotifyLimiter;
  readonly #logger: InternalLogger;
  readonly #operations: PilotOperations<Transaction>;
  readonly #pilotQueueParser: PilotQueueParser | undefined;
  /** Each configured queue's settings parsed by the pilot. */
  readonly #pilotQueueSettings: Readonly<Record<string, unknown>>;
  readonly #runtimeOptions: Readonly<RuntimeSettings>;
  #runtime: RuntimeController | undefined;

  /**
   * `pilotBinding` is private: only `PilotClient` can create one, and
   * anything else, such as an extra argument from untyped JavaScript, is
   * ignored.
   */
  constructor(
    driver: ClientDriver<Transaction>,
    options: ClientOptions<Transaction> = {},
    pilotBinding?: unknown
  ) {
    assertRuntimeSupport();
    // Validates untyped JavaScript input, such as a pool passed by mistake.
    const value: unknown = driver;
    const record =
      typeof value === "object" && value !== null
        ? driverRecord<Transaction>(value)
        : undefined;
    if (record === undefined) {
      throw new ConfigurationError(
        "Client requires a River driver, such as new PgDriver(pool), not a " +
          "database connection or pool; if riverqueue is installed twice, " +
          "check `npm ls riverqueue`"
      );
    }
    this.#backend = record.backend;
    this.#driver = record.operations;
    this.#driverCapability = record.capability;
    const createPilot = pilotFactory<Transaction>(pilotBinding);
    const attached =
      createPilot === undefined ? undefined : attachPilot(record, createPilot);
    this.#attached = attached;
    this.#operations = new PilotOperations(attached?.pilot, attached?.database);
    this.#pilotQueueParser = pilotQueueParser(attached?.pilot);
    const { defaultInsertOptions, ...runtimeOptions } = options;
    this.#defaultInsertOptions = normalizeInsertOptions(defaultInsertOptions);
    const settings = toRuntimeSettings(runtimeOptions);
    if (attached === undefined) {
      this.#pilotQueueSettings = {};
      this.#runtimeOptions = normalizeRuntimeSettings(settings);
    } else {
      // The pilot owns some queue keys: parse them once, keeping River's own
      // settings separately. A pilot's host needs the client ID now.
      const { queues, ...rest } = settings;
      const resolved =
        queues === undefined
          ? undefined
          : resolveQueues(
              queues,
              this.#pilotQueueParser,
              rest.fetchCooldownMs ?? DEFAULT_FETCH_COOLDOWN_MS
            );
      this.#pilotQueueSettings = Object.freeze(
        Object.fromEntries(
          Object.entries(resolved ?? {}).map(([name, queue]) => [
            name,
            queue.pilotSettings,
          ])
        )
      );
      this.#runtimeOptions = normalizeRuntimeSettings({
        ...rest,
        clientId: settings.clientId ?? makeClientId(),
        ...(resolved === undefined
          ? {}
          : {
              queues: Object.fromEntries(
                Object.entries(resolved).map(([name, queue]) => [
                  name,
                  queue.config,
                ])
              ),
            }),
      });
    }
    this.#insertNotifyLimiter = new InsertNotifyLimiter(
      this.#runtimeOptions.fetchCooldownMs ?? DEFAULT_FETCH_COOLDOWN_MS
    );
    this.#logger = internalLogger(resolveLogger(this.#runtimeOptions.logger));
    this.periodicJobs = new PeriodicJobs(
      this.#runtimeOptions.periodicJobs ?? []
    );
    if (this.#runtimeOptions.leaderElectionDisabled === true) {
      disablePeriodicJobs(this.periodicJobs);
    }
    this.jobs = Object.freeze({
      cancel: (id, options) =>
        this.#serialize(options?.tx, () => this.#jobCancel(id, options)),
      delete: (id, options) =>
        this.#serialize(options?.tx, () => this.#jobDelete(id, options)),
      deleteMany: (options) =>
        this.#serialize(options.tx, () => this.#jobDeleteMany(options)),
      get: (id, options) =>
        this.#serialize(options?.tx, () => this.#jobGet(id, options)),
      list: (options) =>
        this.#serialize(options?.tx, () => this.#jobList(options)),
      retry: (id, options) =>
        this.#serialize(options?.tx, () => this.#jobRetry(id, options)),
      update: (id, updates, options) =>
        this.#serialize(options?.tx, () =>
          this.#jobUpdate(id, updates, options)
        ),
    } satisfies JobOperations<Transaction>);
    this.queues = Object.freeze({
      get: (name, options) =>
        this.#serialize(options?.tx, () => this.#queueGet(name, options)),
      list: (options) =>
        this.#serialize(options?.tx, () => this.#queueList(options)),
      pause: (name, options) =>
        this.#serialize(options?.tx, () => this.#queuePause(name, options)),
      resume: (name, options) =>
        this.#serialize(options?.tx, () => this.#queueResume(name, options)),
      update: (name, updates, options) =>
        this.#serialize(options?.tx, () =>
          this.#queueUpdate(name, updates, options)
        ),
    } satisfies QueueOperations<Transaction>);
    this.#hooks = Object.freeze([
      ...(this.#runtimeOptions.plugins?.flatMap((plugin) =>
        plugin.hooks === undefined ? [] : [plugin.hooks]
      ) ?? []),
      ...(this.#runtimeOptions.hooks === undefined
        ? []
        : [this.#runtimeOptions.hooks]),
    ]);
    this.#insertMiddleware = Object.freeze([
      ...(this.#runtimeOptions.plugins?.flatMap(
        (plugin) => plugin.insertMiddleware ?? []
      ) ?? []),
      ...(this.#runtimeOptions.insertMiddleware ?? []),
    ]);
    this.#jobArgsTransformers = Object.freeze(
      getJobArgsTransformers(this.#runtimeOptions.plugins)
    );
    this.#jobInsertMetadataTransformers = Object.freeze(
      getJobInsertMetadataTransformers(this.#runtimeOptions.plugins)
    );
    if (attached !== undefined) this.#initPilot(attached);
  }

  /** Cancel a job and cooperatively abort its matching local attempt. */
  async #jobCancel(
    id: bigint,
    options: TransactionOptions<Transaction> = {}
  ): Promise<JobRow | null> {
    const job = await this.#operations.cancel(
      this.#runtimeDriver("job cancellation"),
      validateJobId(id),
      options.tx
    );
    // A caller-owned transaction may still roll back. Its transactional
    // notification is delivered only after commit and is the authoritative
    // point at which a local attempt may be aborted.
    if (job !== null && options.tx === undefined)
      this.#runtime?.cancelLocal(job);
    return job;
  }

  /** Delete a non-running job, returning null when it does not exist. */
  async #jobDelete(
    id: bigint,
    options: TransactionOptions<Transaction> = {}
  ): Promise<JobRow | null> {
    const result = await this.#runtimeDriver("job deletion").jobDelete(
      validateJobId(id),
      driverOptions(options.tx)
    );
    if (result.status === "not_found") return null;
    if (result.status === "running") throw new JobRunningError(result.job.id);
    return result.job;
  }

  /** Delete a bounded, explicitly filtered set of non-running jobs. */
  async #jobDeleteMany(
    options: JobDeleteManyOptions & TransactionOptions<Transaction>
  ): Promise<readonly JobRow[]> {
    const { tx, ...deleteOptions } = options;
    return this.#runtimeDriver("bulk job deletion").jobDeleteMany(
      normalizeJobDeleteManyOptions(deleteOptions),
      driverOptions(tx)
    );
  }

  /** Get one job exactly, returning null when it does not exist. */
  async #jobGet(
    id: bigint,
    options: TransactionOptions<Transaction> = {}
  ): Promise<JobRow | null> {
    return this.#runtimeDriver("job queries").jobGet(
      validateJobId(id),
      driverOptions(options.tx)
    );
  }

  /** Insert one validated job, optionally in a caller-owned transaction. */
  async insert<Definition extends JobDefinition>(
    definition: Definition,
    args: JobDefinitionInput<Definition>,
    options: InsertOptions & TransactionOptions<Transaction> = {}
  ): Promise<InsertResult<JobDefinitionInput<Definition>>> {
    return this.#operationScope(options.tx, async (tx) => {
      const params = await this.#makeInsertParams(definition, args, options);
      const [result] = await this.#runInsertExtensions("insert", [params], tx);
      if (result === undefined) {
        throw new Error("insertion adapter returned no result");
      }
      return result as InsertResult<JobDefinitionInput<Definition>>;
    });
  }

  /** Insert a heterogeneous batch atomically while preserving input order. */
  async insertMany<const Items extends readonly InsertManyItem[]>(
    items: Items & CheckedInsertManyItems<Items>,
    options: TransactionOptions<Transaction> = {}
  ): Promise<InsertManyResults<Items>> {
    if (items.length === 0) return [] as unknown as InsertManyResults<Items>;
    return this.#operationScope(options.tx, async (tx) => {
      const allParams: JobInsertParams[] = [];
      for (const item of items) {
        allParams.push(
          await this.#makeInsertParams(item.job, item.args, item.options ?? {})
        );
      }

      const results = await this.#runInsertExtensions(
        "insertMany",
        allParams,
        tx
      );
      if (results.length !== allParams.length) {
        throw new Error(
          `insertion adapter returned ${results.length} results for ${allParams.length} jobs`
        );
      }
      return results as unknown as InsertManyResults<Items>;
    });
  }

  /** List jobs with exact, opaque keyset pagination. */
  async #jobList(
    options: JobListOptions & TransactionOptions<Transaction> = {}
  ): Promise<JobListResult> {
    const { tx, ...listOptions } = options;
    const params = normalizeJobListOptions(listOptions);
    const jobs = await this.#runtimeDriver("job queries").jobList(
      params,
      driverOptions(tx)
    );
    const last = jobs.at(-1);
    return {
      jobs,
      nextCursor:
        last === undefined || jobs.length < params.limit
          ? null
          : encodeJobListCursor(last, params),
    };
  }

  async #queueGet(
    name: string,
    options: TransactionOptions<Transaction> = {}
  ): Promise<QueueRow | null> {
    return this.#runtimeDriver("queue queries").queueGet(
      queueLookupName(name),
      driverOptions(options.tx)
    );
  }

  /** List dynamic queues with opaque name pagination. */
  async #queueList(
    options: QueueListOptions & TransactionOptions<Transaction> = {}
  ): Promise<QueueListResult> {
    const { tx, ...listOptions } = options;
    const params = normalizeQueueListOptions(listOptions);
    const queues = await this.#runtimeDriver("queue queries").queueList(
      params,
      driverOptions(tx)
    );
    const last = queues.at(-1);
    return {
      nextCursor:
        last === undefined || queues.length < params.limit
          ? null
          : encodeQueueCursor(last),
      queues,
    };
  }

  /** Pause a queue after the backend transition commits. */
  async #queuePause(
    name: string,
    options: TransactionOptions<Transaction> = {}
  ): Promise<QueueRow | null> {
    const queue = await this.#runtimeDriver("queue pause").queuePause(
      queueLookupName(name),
      driverOptions(options.tx)
    );
    if (queue !== null && options.tx === undefined) {
      this.#runtime?.applyCommittedQueueControl(queue);
      await this.#emit({
        at: Temporal.Now.instant(),
        kind: "queue_paused",
        queue,
      });
    }
    // "*" returns no row, so have this client's queues reread their pause
    // state now instead of at the next control poll.
    if (name === "*" && options.tx === undefined) {
      this.#runtime?.wakeQueueControl();
    }
    return queue;
  }

  /** Resume a queue after the backend transition commits. */
  async #queueResume(
    name: string,
    options: TransactionOptions<Transaction> = {}
  ): Promise<QueueRow | null> {
    const queue = await this.#runtimeDriver("queue resume").queueResume(
      queueLookupName(name),
      driverOptions(options.tx)
    );
    if (queue !== null && options.tx === undefined) {
      this.#runtime?.applyCommittedQueueControl(queue);
      await this.#emit({
        at: Temporal.Now.instant(),
        kind: "queue_resumed",
        queue,
      });
    }
    // "*" returns no row, so have this client's queues reread their pause
    // state now instead of at the next control poll.
    if (name === "*" && options.tx === undefined) {
      this.#runtime?.wakeQueueControl();
    }
    return queue;
  }

  async #queueUpdate(
    name: string,
    updates: QueueUpdateOptions,
    options: TransactionOptions<Transaction> = {}
  ): Promise<QueueRow | null> {
    const params =
      updates.metadata === undefined
        ? {}
        : { metadata: toJsonObject(updates.metadata) };
    const queue = await this.#runtimeDriver("queue updates").queueUpdate(
      queueLookupName(name),
      params,
      driverOptions(options.tx)
    );
    if (
      queue !== null &&
      options.tx === undefined &&
      updates.metadata !== undefined
    ) {
      await this.#emit({
        at: Temporal.Now.instant(),
        kind: "queue_updated",
        queue,
      });
    }
    return queue;
  }

  /** Ask whichever runtime currently leads to resign, optionally on commit. */
  async requestLeadershipResignation(
    options: TransactionOptions<Transaction> = {}
  ): Promise<void> {
    await this.#serialize(options.tx, () =>
      this.#requestLeadershipResignation(options)
    );
  }

  async #requestLeadershipResignation(
    options: TransactionOptions<Transaction>
  ): Promise<void> {
    const driver = this.#runtimeDriver("leadership resignation requests");
    const request = driver.runtimeRequestLeadershipResignation?.bind(driver);
    if (request === undefined) {
      throw new UnsupportedCapabilityError(
        this.#backend,
        "leadership resignation requests"
      );
    }
    await request(driverOptions(options.tx));
  }

  /** Make a non-running job immediately eligible for another attempt. */
  async #jobRetry(
    id: bigint,
    options: TransactionOptions<Transaction> = {}
  ): Promise<JobRow | null> {
    return this.#operations.retry(
      this.#runtimeDriver("job retry"),
      validateJobId(id),
      options.tx
    );
  }

  /** Start the configured worker runtime exactly once. */
  async start(): Promise<RunHandle> {
    if (this.#runtime !== undefined) {
      throw new LifecycleError(
        "client runtime has already been started; a Client starts at most once, so create a new Client to start again"
      );
    }
    const driver = this.#runtimeDriver("runtime");
    const binding: RuntimeBinding = {
      allowInsertNotifications: (queues) =>
        this.#insertNotifyLimiter.allow(queues),
      backend: this.#backend,
      ...(this.#attached === undefined
        ? {}
        : {
            database: this.#attached.database,
            pilot: this.#attached.pilot,
          }),
      operations: this.#operations,
      pilotQueueParser: this.#pilotQueueParser,
      pilotQueueSettings: this.#pilotQueueSettings,
    };
    this.#runtime = new RuntimeController(
      this,
      driver,
      {
        drain: () => this.#eventHooks.drain(),
        emit: (event) => this.#emit(event),
      },
      this.periodicJobs,
      this.#runtimeOptions,
      binding
    );
    try {
      await this.#runtime.ready;
    } catch (error: unknown) {
      // A failed start tears down what it started before rejecting.
      await this.#runtime.completed.catch(() => undefined);
      throw error;
    }
    return new RunHandle(this.#runtime);
  }

  /** Subscribe to bounded post-transition observations. */
  subscribe<Kind extends RiverEventKind = RiverEventKind>(
    options: SubscribeOptions<Kind> = {}
  ): EventSubscription<SubscribedEvent<Kind>> {
    return this.#eventHub.subscribe(options);
  }

  async #jobUpdate(
    id: bigint,
    updates: JobUpdateOptions,
    options: TransactionOptions<Transaction> = {}
  ): Promise<JobRow | null> {
    return this.#runtimeDriver("job updates").jobUpdate(
      validateJobId(id),
      normalizeJobUpdateOptions(updates),
      driverOptions(options.tx)
    );
  }

  #runtimeDriver(capability: string): RuntimeDriver<Transaction> {
    this.#assertConstructed();
    if (this.#driverCapability === "runtime") {
      return this.#driver as RuntimeDriver<Transaction>;
    }
    throw new UnsupportedCapabilityError(this.#backend, capability);
  }

  async #emit(event: RiverEvent): Promise<void> {
    this.#eventHub.publish(event);
    if (this.#hooks.some((hooks) => hooks.onEvent !== undefined)) {
      await this.#eventHooks.enqueue(event);
    }
  }

  async #deliverEventHooks(event: RiverEvent): Promise<void> {
    for (const hooks of this.#hooks) {
      if (hooks.onEvent === undefined) continue;
      try {
        await hooks.onEvent(event);
      } catch (cause: unknown) {
        this.#logger.error("River onEvent hook failed", {
          error:
            cause instanceof Error
              ? (cause.stack ?? cause.message)
              : String(cause),
          eventKind: event.kind,
        });
      }
    }
  }

  /**
   * Run one insertion in a driver operation scope, like River for Go's
   * `dbutil.WithTxV`: without `tx`, validation, insert middleware, hooks,
   * and the write share one transaction River owns, so an error anywhere,
   * even after middleware's `next()` returns, rolls the jobs back.
   */
  #operationScope<T>(
    tx: Transaction | undefined,
    callback: (tx: Transaction | undefined) => Promise<T>
  ): Promise<T> {
    try {
      this.#assertConstructed();
    } catch (error: unknown) {
      return Promise.reject(error);
    }
    const driver = this.#driver;
    return this.#serialize(tx, () =>
      driver.operationScope === undefined
        ? callback(tx)
        : driver.operationScope(tx, callback)
    );
  }

  /**
   * Run an operation in the caller's transaction `tx` once no other
   * operation of this client's pilot holds it, when the client has a pilot.
   * An intercepted operation runs statements on `tx` across its
   * interceptor's awaits, so operations sharing `tx` must not interleave.
   */
  #serialize<T>(
    tx: Transaction | undefined,
    run: () => Promise<T>
  ): Promise<T> {
    if (tx === undefined || !this.#operations.serializes) return run();
    return withHandle(tx, run);
  }

  async #runInsertExtensions(
    operation: InsertContext["operation"],
    params: readonly JobInsertParams[],
    tx: Transaction | undefined,
    signal?: AbortSignal
  ): Promise<readonly DriverInsertResult[]> {
    // A job without a schedule is stored with the database's time; hooks
    // and middleware see when the insertion was requested.
    const requestedAt = Temporal.Now.instant();
    const context: InsertContext = {
      operation,
      requests: Object.freeze(
        params.map((paramsItem) =>
          Object.freeze({
            args: paramsItem.args,
            definition: insertDefinitions.get(paramsItem),
            kind: paramsItem.kind,
            maxAttempts: paramsItem.maxAttempts,
            metadata: paramsItem.metadata,
            priority: paramsItem.priority,
            queue: paramsItem.queue,
            scheduledAt: paramsItem.scheduledAt ?? requestedAt,
            state: paramsItem.state,
            tags: paramsItem.tags,
            unique: paramsItem.uniqueKey !== null,
          })
        )
      ),
    };
    const originals = params.map(
      (row) => insertOriginalEncodedArgs.get(row) ?? row.encodedArgs
    );
    const database = async (): Promise<readonly DriverInsertResult[]> => {
      return this.#operations.insert(
        operation,
        params,
        tx,
        async (rows, scopeTx) => {
          rejectRepeatedUniqueKeys(rows);
          const results =
            operation === "insert"
              ? [
                  await this.#driver.jobInsert(
                    rows[0] as JobInsertParams,
                    driverOptions(scopeTx)
                  ),
                ]
              : await this.#driver.jobInsertMany(rows, driverOptions(scopeTx));
          await this.#notifyInsert(rows, scopeTx);
          return results;
        },
        signal,
        originals
      );
    };
    // Like River for Go, insert hooks run inside the innermost insert
    // middleware, around the database write.
    const hooked = async (): Promise<readonly DriverInsertResult[]> => {
      for (const hooks of this.#hooks) {
        await hooks.beforeInsert?.(context);
      }
      const results = await database();
      for (const hooks of this.#hooks) {
        await hooks.afterInsert?.(context, results);
      }
      return results;
    };
    const invoke = composeInsertMiddleware(
      this.#insertMiddleware,
      context,
      hooked
    );
    const storageResults = await invoke();
    const results: DriverInsertResult[] = [];
    for (const result of storageResults) {
      const args = transformJobArgsForRead(
        this.#jobArgsTransformers,
        result.job.kind,
        result.job.args
      );
      results.push(
        args === result.job.args
          ? result
          : { ...result, job: { ...result.job, args } }
      );
    }
    return results;
  }

  /** Reject use of the client while its pilot's `init` runs. */
  #assertConstructed(): void {
    if (this.#initializing) {
      throw new LifecycleError(
        "the client can't be used until its construction returns"
      );
    }
  }

  /** Call the pilot's `init` with its host, once, synchronously. */
  #initPilot(attached: AttachedPilot<Transaction>): void {
    const { database, pilot } = attached;
    const attempts: PilotAttempts<Transaction> = {
      claim: (attempt, run) =>
        this.#withPeerAttempts((peers) =>
          peers.claim(attempt, run as Parameters<PeerAttempts["claim"]>[1])
        ),
      complete: (attempt, outcomes) =>
        this.#withPeerAttempts((peers) => peers.complete(attempt, outcomes)),
    };
    const host: PilotHost<Transaction> = {
      attempts: Object.freeze(attempts),
      client: this,
      clientId: this.#runtimeOptions.clientId ?? "",
      database,
      insertPrepared: (params, options) =>
        this.#insertPrepared(params, options),
      logger: resolveLogger(this.#runtimeOptions.logger),
      notifyCommitted: (results) => {
        this.#notifyCommitted(results);
      },
      producerReportInterval: millisecondsToDuration(
        this.#runtimeOptions.queueHeartbeatIntervalMs ?? 30_000
      ),
      workerKinds: Object.freeze([
        ...(this.#runtimeOptions.workers?.kinds() ?? []),
      ]),
    };
    Object.freeze(host);
    if (pilot.init === undefined) return;
    // `init` is typed as returning nothing; check untyped pilots anyway.
    const init: (host: PilotHost<Transaction>) => unknown =
      pilot.init.bind(pilot);
    this.#initializing = true;
    let returned: unknown;
    try {
      returned = init(host);
    } finally {
      this.#initializing = false;
    }
    if (isThenable(returned)) {
      void Promise.resolve(returned).catch(() => undefined);
      throw new ConfigurationError(
        "a pilot's init must run synchronously, without I/O"
      );
    }
  }

  /**
   * Run `operation` on the running runtime's peer attempts, for
   * {@link PilotHost.attempts}. It starts synchronously.
   */
  #withPeerAttempts<T>(
    operation: (peers: PeerAttempts) => Promise<T>
  ): Promise<T> {
    const peers = this.#runtime?.peerAttempts;
    if (peers === undefined) {
      return Promise.reject(
        new LifecycleError("peer attempts require a running River runtime")
      );
    }
    return operation(peers);
  }

  /** Insert rows already prepared, for {@link PilotHost.insertPrepared}. */
  async #insertPrepared(
    params: readonly PreparedInsertParams[],
    options: {
      readonly signal?: AbortSignal;
      readonly tx?: Transaction;
    } = {}
  ): Promise<readonly DriverInsertResult[]> {
    const rows = validatePreparedParams(params);
    options.signal?.throwIfAborted();
    if (rows.length === 0) return [];
    return this.#operationScope(options.tx, (tx) =>
      this.#runInsertExtensions(
        "insertMany",
        rows.map((row) => this.#preparedInsertParams(row)),
        tx,
        options.signal
      )
    );
  }

  /**
   * A prepared row as an ordinary insertion of it stores it: the client's
   * insert metadata and argument transforms run on its stored arguments,
   * without a job definition, and its other fields are kept. Stored
   * arguments that aren't a JSON object skip the argument transforms, and
   * everything else sees empty arguments, like River for Go's stand-in
   * arguments for such rows.
   */
  #preparedInsertParams(row: PreparedInsertParams): JobInsertParams {
    const stored = parseJson(row.encodedArgs);
    const isObject =
      typeof stored === "object" && stored !== null && !Array.isArray(stored);
    const args: JsonObject = isObject ? (stored as JsonObject) : {};
    const insertMetadata = transformJobInsertMetadata(
      this.#jobInsertMetadataTransformers,
      undefined,
      row.kind,
      args,
      row.metadata,
      row.queue,
      row.state === JOB_STATE.pending
    );
    const prepared: JobInsertParams = {
      ...row,
      ...(isObject
        ? transformJobArgsForInsert(
            this.#jobArgsTransformers,
            undefined,
            row.kind,
            args,
            row.encodedArgs
          )
        : { args, encodedArgs: row.encodedArgs }),
      metadata: insertMetadata.metadata,
      state: insertMetadata.pending ? JOB_STATE.pending : row.state,
    };
    insertOriginalEncodedArgs.set(prepared, row.encodedArgs);
    return prepared;
  }

  /** Wake local producers for jobs another transaction owner committed. */
  #notifyCommitted(results: readonly DriverInsertResult[]): void {
    const queues = new Set<string>();
    for (const result of results) {
      if (result.status === "inserted" && result.job.state === "available") {
        queues.add(result.job.queue);
      }
    }
    if (queues.size > 0) this.#runtime?.wakeQueues(queues);
  }

  /**
   * Notify producers of the queues of available jobs just inserted in
   * `tx`, like River for Go's client, whether or not each job was a unique
   * duplicate, skipping queues this client notified within its fetch
   * cooldown.
   */
  async #notifyInsert(
    rows: readonly JobInsertParams[],
    tx: Transaction | undefined
  ): Promise<void> {
    const notifyInsert = this.#driver.notifyInsert?.bind(this.#driver);
    if (notifyInsert === undefined) return;
    const queues = this.#insertNotifyLimiter.allow(
      rows.flatMap((row) =>
        row.state === JOB_STATE.available ? [row.queue] : []
      )
    );
    if (queues.length > 0) await notifyInsert(queues, driverOptions(tx));
  }

  async #makeInsertParams<Definition extends JobDefinition>(
    definition: Definition,
    input: JobDefinitionInput<Definition>,
    callOptions: InsertOptions
  ): Promise<JobInsertParams> {
    const args = await prepareJobInput(definition, input);
    const { options, scheduledExplicitly } = resolveInsertOptions(
      callOptions,
      definition.defaults,
      this.#defaultInsertOptions
    );
    const insertMetadata = transformJobInsertMetadata(
      this.#jobInsertMetadataTransformers,
      definition,
      definition.kind,
      args,
      options.metadata,
      options.queue,
      options.pending
    );
    const metadata = insertMetadata.metadata;

    const params: JobInsertParams = {
      args,
      encodedArgs: stringifyJson(args),
      kind: definition.kind,
      maxAttempts: options.maxAttempts,
      metadata,
      priority: options.priority,
      queue: options.queue,
      // Like River for Go, a job without a schedule takes the database's
      // current time.
      ...(scheduledExplicitly ? { scheduledAt: options.scheduledAt } : {}),
      // A periodic occurrence's time doesn't make the job `scheduled`.
      state: insertMetadata.pending
        ? JOB_STATE.pending
        : scheduledExplicitly && !periodicOccurrenceOptions.has(callOptions)
          ? JOB_STATE.scheduled
          : JOB_STATE.available,
      tags: options.tags,
      uniqueKey: null,
      uniqueStates: null,
    };

    let finalizedParams = params;
    if (options.unique !== undefined && hasUniqueConstraints(options.unique)) {
      const [uniqueKey, uniqueStates] = buildUniqueKey(params, options.unique);
      finalizedParams = { ...params, uniqueKey, uniqueStates };
    }
    const transformed = transformJobArgsForInsert(
      this.#jobArgsTransformers,
      definition,
      finalizedParams.kind,
      finalizedParams.args,
      finalizedParams.encodedArgs
    );
    const prepared = { ...finalizedParams, ...transformed };
    insertDefinitions.set(prepared, definition);
    insertOriginalEncodedArgs.set(prepared, finalizedParams.encodedArgs);
    return prepared;
  }
}

/**
 * Create a River client from a driver.
 *
 * @example
 * ```ts
 * const client = new Client(new PgDriver(pool), {
 *   queues: { default: { maxWorkers: 50 } },
 *   workers,
 * });
 * ```
 */
export const Client: ClientConstructor = RiverClient;

/** A pilot a client attached, with the database its driver provided. */
interface AttachedPilot<Transaction> {
  readonly database: PilotDatabase<Transaction>;
  readonly pilot: Pilot<Transaction>;
}

/** Pilots already attached to a client; each belongs to one client. */
const attachedPilots = new WeakSet<object>();

const INTERCEPTED_OPERATIONS: ReadonlySet<string> = new Set<
  keyof PilotInterceptors<unknown>
>(["cancel", "complete", "getStuck", "insert", "rescue", "retry"]);

/** River's own public queue keys, which a pilot can't own. */
const RIVER_QUEUE_KEYS: ReadonlySet<string> = new Set([
  "fetchCooldown",
  "maxWorkers",
  "pollInterval",
]);

/**
 * Create a client's pilot from its driver's registered database and check
 * its shape.
 */
function attachPilot<Transaction>(
  record: DriverRecord<Transaction>,
  createPilot: PilotFactory<Transaction>
): AttachedPilot<Transaction> {
  if (record.database === undefined) {
    throw new UnsupportedCapabilityError(record.backend, "pilots", {
      message:
        "this driver can't serve a pilot; use PgDriver constructed with a " +
        "Pool, or SqliteDriver",
    });
  }
  // Statements a pilot runs in a caller's transaction wait their turn with
  // the client's other operations on it.
  const database = serializedDatabase(record.database);
  const pilot: unknown = createPilot(database);
  if (typeof pilot !== "object" || pilot === null || isThenable(pilot)) {
    throw new ConfigurationError(
      "a pilot factory must synchronously return a pilot object"
    );
  }
  if (attachedPilots.has(pilot)) {
    throw new ConfigurationError(
      "a pilot belongs to one client; create a new pilot for each client"
    );
  }
  const attached = pilot as Pilot<Transaction>;
  validatePilot(attached);
  attachedPilots.add(attached);
  return { database, pilot: attached };
}

function validatePilot(pilot: Pilot<unknown>): void {
  const concurrency: unknown = pilot.completionConcurrency;
  if (
    concurrency !== undefined &&
    (typeof concurrency !== "number" ||
      !Number.isSafeInteger(concurrency) ||
      concurrency < 1)
  ) {
    throw new ConfigurationError(
      "a pilot's completionConcurrency must be a positive integer"
    );
  }
  const excluded: unknown = pilot.jobCleanerQueuesExcluded;
  if (
    excluded !== undefined &&
    (!Array.isArray(excluded) ||
      !excluded.every((queue) => typeof queue === "string"))
  ) {
    throw new ConfigurationError(
      "a pilot's jobCleanerQueuesExcluded must be an array of queue names"
    );
  }
  const store: unknown = pilot.periodicJobs;
  if (
    store !== undefined &&
    (typeof store !== "object" ||
      store === null ||
      typeof (store as Partial<PeriodicJobStore>).getAll !== "function" ||
      typeof (store as Partial<PeriodicJobStore>).keepAliveAndReap !==
        "function" ||
      typeof (store as Partial<PeriodicJobStore>).upsertMany !== "function")
  ) {
    throw new ConfigurationError(
      "a pilot's periodicJobs must implement getAll, keepAliveAndReap, and upsertMany"
    );
  }
  for (const method of [
    "init",
    "maintenanceServices",
    "services",
    "startProducer",
  ] as const) {
    if (pilot[method] !== undefined && typeof pilot[method] !== "function") {
      throw new ConfigurationError(`a pilot's ${method} must be a function`);
    }
  }
  const intercept: unknown = pilot.intercept;
  if (intercept !== undefined) {
    if (typeof intercept !== "object" || intercept === null) {
      throw new ConfigurationError("a pilot's intercept must be an object");
    }
    for (const [name, value] of Object.entries(intercept)) {
      if (!INTERCEPTED_OPERATIONS.has(name)) {
        throw new ConfigurationError(
          `a pilot can't intercept ${JSON.stringify(name)}`
        );
      }
      if (value !== undefined && typeof value !== "function") {
        throw new ConfigurationError(
          `a pilot's intercept.${name} must be a function`
        );
      }
    }
  }
  const queueOptions: unknown = pilot.queueOptions;
  if (queueOptions === undefined) return;
  const { keys, parse } = (queueOptions ?? {}) as {
    readonly keys?: unknown;
    readonly parse?: unknown;
  };
  if (
    !Array.isArray(keys) ||
    typeof parse !== "function" ||
    !keys.every((key) => typeof key === "string" && key.length > 0)
  ) {
    throw new ConfigurationError(
      "a pilot's queueOptions needs keys (non-empty strings) and a parse function"
    );
  }
  const seen = new Set<string>();
  for (const key of keys as string[]) {
    // River rejects queue keys ending in `Ms` before any owner sees them.
    if (RIVER_QUEUE_KEYS.has(key) || seen.has(key) || /[a-z]Ms$/.test(key)) {
      throw new ConfigurationError(
        `a pilot can't own the queue key ${JSON.stringify(key)}`
      );
    }
    seen.add(key);
  }
}

/** The queue keys a pilot owns, bound to its parser. */
function pilotQueueParser(
  pilot: Pilot<unknown> | undefined
): PilotQueueParser | undefined {
  const options = pilot?.queueOptions;
  if (options === undefined) return undefined;
  return Object.freeze({
    keys: new Set(options.keys),
    parse: (queue: string, config: Readonly<Record<string, unknown>>) =>
      options.parse(queue, config),
  });
}

function isThenable(value: unknown): boolean {
  return (
    (typeof value === "object" || typeof value === "function") &&
    value !== null &&
    typeof (value as { readonly then?: unknown }).then === "function"
  );
}

/** Events buffered for `onEvent` hooks before emitters wait. */
const EVENT_HOOK_QUEUE_CAPACITY = 1_024;

/** The definition each prepared insertion came from, for extension inputs. */
const insertDefinitions = new WeakMap<JobInsertParams, JobDefinition>();

/**
 * Each prepared insertion's arguments before the client's argument
 * transforms, for a pilot's insert interceptor.
 */
const insertOriginalEncodedArgs = new WeakMap<JobInsertParams, string>();

function validateJobId(id: bigint): bigint {
  if (typeof id !== "bigint" || id <= 0n) {
    throw new ValidationError("job id must be a positive bigint");
  }
  return id;
}

/**
 * Reject a batch in which a unique key appears more than once among the jobs
 * whose state it covers, before anything is written. River for Go writes a
 * batch in one statement, which PostgreSQL refuses when two rows claim the
 * same key, and checks SQLite batches the same way.
 */
function rejectRepeatedUniqueKeys(params: readonly JobInsertParams[]): void {
  const keys = new Set<string>();
  for (const { state, uniqueKey, uniqueStates } of params) {
    if (uniqueKey === null || uniqueKey.length === 0) continue;
    if (uniqueStates === null || !uniqueStates.includes(state)) continue;
    const key = bytesToHex(uniqueKey);
    if (keys.has(key)) {
      throw new ValidationError("unique key appears more than once in batch");
    }
    keys.add(key);
  }
}

function driverOptions<Transaction>(
  tx: Transaction | undefined
): InsertDriverOptions<Transaction> | undefined {
  return tx === undefined ? undefined : { tx };
}

function composeInsertMiddleware(
  middleware: readonly InsertMiddleware[],
  context: InsertContext,
  database: () => Promise<readonly DriverInsertResult[]>
): () => Promise<readonly DriverInsertResult[]> {
  return async () => {
    const dispatch = async (
      index: number
    ): Promise<readonly DriverInsertResult[]> => {
      const current = middleware[index];
      if (current === undefined) return database();
      let called = false;
      return current(context, async () => {
        if (called) {
          throw new Error("insert middleware called next more than once");
        }
        called = true;
        return dispatch(index + 1);
      });
    };
    return dispatch(0);
  };
}

function hasUniqueConstraints(options: NormalizedUniqueOptions): boolean {
  return (
    options.byArgs === true ||
    (Array.isArray(options.byArgs) && options.byArgs.length > 0) ||
    options.byPeriod !== undefined ||
    options.byQueue === true ||
    options.byState !== undefined ||
    options.excludeKind === true
  );
}

/**
 * Exact-version hashing seam shared by insertion and conformance. Like
 * River for Go, a period key uses the job's scheduled time, or the current
 * time for a job without one.
 */
export function buildUniqueKey(
  params: Pick<JobInsertParams, "args" | "kind" | "queue" | "scheduledAt">,
  options: UniqueOptions
): [Uint8Array, readonly JobState[]] {
  const normalized = normalizeUniqueOptions(options);
  let uniqueKeyString = "";

  if (options.excludeKind !== true) {
    uniqueKeyString += `&kind=${params.kind}`;
  }

  if (options.byArgs !== undefined) {
    uniqueKeyString += `&args=${encodeUniqueArgs(params.args, options.byArgs)}`;
  }

  const periodNanoseconds = uniquePeriodNanoseconds(normalized);
  if (periodNanoseconds !== null) {
    uniqueKeyString += `&period=${truncateInstant(
      params.scheduledAt ?? Temporal.Now.instant(),
      periodNanoseconds
    ).toString({ smallestUnit: "second" })}`;
  }

  if (options.byQueue === true) uniqueKeyString += `&queue=${params.queue}`;

  const uniqueKey = createHash("sha256").update(uniqueKeyString).digest();
  const states =
    options.byState === undefined || options.byState.length === 0
      ? DEFAULT_UNIQUE_STATES
      : options.byState;
  // Validate the shared state-to-bit mapping here even though each backend
  // performs its own physical encoding at the database boundary.
  uniqueBitmaskFromStates(states);
  return [new Uint8Array(uniqueKey), Object.freeze([...states])];
}

/**
 * Encode the arguments part of a unique key exactly as River for Go does:
 * the text hashed after `&args=`. With `byArgs: true` it is every top-level
 * argument, keys sorted bytewise; with a list of paths it is the selected
 * values assembled in sorted path order, or an empty string when none of the
 * paths is present. An unescaped dot descends into an object; a backslash
 * quotes the following character for a literal field name. Paths sort by
 * their unescaped field names. Keys are written the way Go's `sjson` writes
 * them, and nested values keep their own order.
 *
 * Arguments must be a JSON object. Like River for Go, `byArgs: true` hashes
 * an empty array as `{}`.
 *
 * Extensions that derive other keys from job arguments use this so they
 * hash arguments identically to River's unique keys.
 *
 * @throws {ValidationError} for arguments that aren't a JSON object (other
 * than an empty array with `byArgs: true`), an invalid selected path,
 * including one with a segment River for Go reads as an array index (an
 * unsigned integer or `-1`), or an empty path list.
 */
export function encodeUniqueArgs(
  args: JsonObject,
  byArgs: true | readonly string[]
): string {
  if (byArgs !== true) {
    normalizeUniqueOptions({ byArgs });
  }
  let uniqueArgs = uniqueArgsObject(args, byArgs === true);
  // River's own intermediate objects for selected paths, as opposed to
  // argument values copied into them.
  const assembled = new Set<object>();
  if (byArgs !== true) {
    uniqueArgs = Object.create(null) as JsonObject;
    assembled.add(uniqueArgs);
    const paths = byArgs
      .map((path) => {
        const segments = parseUniquePath(path);
        return { path, segments, sortKey: segments.join(".") };
      })
      .sort(
        (a, b) =>
          Buffer.compare(Buffer.from(a.sortKey), Buffer.from(b.sortKey)) ||
          Buffer.compare(Buffer.from(a.path), Buffer.from(b.path))
      );
    const selectedPaths: (readonly string[])[] = [];
    for (const { segments } of paths) {
      // Selecting an object already includes its descendants. Avoid writing
      // through that borrowed object when another path selects a child.
      if (
        selectedPaths.some(
          (selected) =>
            selected.length < segments.length &&
            selected.every((part, index) => part === segments[index])
        )
      )
        continue;
      let value: unknown = args;
      for (const segment of segments) {
        value =
          value !== null &&
          typeof value === "object" &&
          !Array.isArray(value) &&
          Object.hasOwn(value, segment)
            ? (value as JsonObject)[segment]
            : undefined;
      }
      if (value !== undefined) {
        let target = uniqueArgs;
        for (const segment of segments.slice(0, -1)) {
          if (!Object.hasOwn(target, segment)) {
            const child = Object.create(null) as JsonObject;
            assembled.add(child);
            target[segment] = child;
          }
          const child = target[segment];
          if (
            child === null ||
            typeof child !== "object" ||
            Array.isArray(child)
          ) {
            throw new ValidationError(
              "unique.byArgs contains overlapping paths"
            );
          }
          target = child as JsonObject;
        }
        const leaf = segments.at(-1);
        if (leaf === undefined)
          throw new ValidationError("unique.byArgs contains an empty path");
        target[leaf] = value as JsonObject[string];
        selectedPaths.push(segments);
      }
    }
  }
  let encodedArgs: string;
  if (!Array.isArray(byArgs)) {
    encodedArgs = stringifyUniqueJson(uniqueArgs);
  } else if (Object.keys(uniqueArgs).length === 0) {
    encodedArgs = "";
  } else {
    encodedArgs = stringifySelectedUniqueJson(uniqueArgs, assembled);
  }
  return encodedArgs;
}

/**
 * Return `args` if it's a JSON object. River for Go rejects any other
 * arguments for argument uniqueness, except that when every argument is
 * hashed, an empty array is treated as an empty object.
 */
function uniqueArgsObject(args: unknown, allArgs: boolean): JsonObject {
  if (
    args !== null &&
    typeof args === "object" &&
    !Array.isArray(args) &&
    !isExactJsonNumber(args)
  ) {
    return args as JsonObject;
  }
  if (allArgs && Array.isArray(args) && args.length === 0) {
    return Object.create(null) as JsonObject;
  }
  throw new ValidationError("unique args must encode a JSON object");
}

function resolveInsertOptions(
  callOptions: InsertOptions,
  definition: Readonly<NormalizedInsertOptions>,
  client: Readonly<NormalizedInsertOptions>
): { options: ResolvedInsertOptions; scheduledExplicitly: boolean } {
  const call = normalizeInsertOptions(callOptions);
  // Scheduling is one dimension: the most specific level that sets either an
  // absolute time or a delay wins, so a call-site delay overrides a
  // definition-level scheduledAt and vice versa.
  const scheduling = [call, definition, client].find(
    (level) => level.scheduledAt !== undefined || level.delay !== undefined
  );
  const now = Temporal.Now.instant();
  const scheduledAt =
    scheduling === undefined ? undefined : resolveScheduledAt(scheduling, now);
  const unique = call.unique ?? definition.unique ?? client.unique;

  const options: ResolvedInsertOptions = {
    maxAttempts:
      call.maxAttempts ??
      definition.maxAttempts ??
      client.maxAttempts ??
      MAX_ATTEMPTS_DEFAULT,
    metadata: call.metadata ?? definition.metadata ?? client.metadata ?? {},
    pending: call.pending ?? definition.pending ?? client.pending ?? false,
    priority:
      call.priority ??
      definition.priority ??
      client.priority ??
      PRIORITY_DEFAULT,
    queue: call.queue ?? definition.queue ?? client.queue ?? QUEUE_DEFAULT,
    scheduledAt: scheduledAt ?? now,
    tags: call.tags ?? definition.tags ?? client.tags ?? [],
  };
  if (unique !== undefined) options.unique = unique;
  return { options, scheduledExplicitly: scheduledAt !== undefined };
}

function truncateInstant(
  instant: Temporal.Instant,
  intervalNanoseconds: bigint
): Temporal.Instant {
  const absoluteNanoseconds =
    instant.epochNanoseconds + NANOSECONDS_FROM_YEAR_ONE_TO_UNIX_EPOCH;
  let period = absoluteNanoseconds / intervalNanoseconds;
  if (
    absoluteNanoseconds < 0n &&
    absoluteNanoseconds % intervalNanoseconds !== 0n
  ) {
    period--;
  }
  return Temporal.Instant.fromEpochNanoseconds(
    period * intervalNanoseconds - NANOSECONDS_FROM_YEAR_ONE_TO_UNIX_EPOCH
  );
}
