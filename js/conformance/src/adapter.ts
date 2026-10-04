import type { DatabaseSync } from "node:sqlite";
import {
  Client,
  JobCancelledError,
  Workers,
  complete,
  defineJob,
  discard,
  JOB_STATE,
  JobRunningError,
  recordOutput,
  stringifyJson,
  snooze,
  type ClientOptions,
  type InsertManyItem,
  type InsertOptions,
  type JobRow,
  type JobState,
  type JsonObject,
  type JsonValue,
  type RiverPlugin,
  type RunHandle,
  type WorkContext,
  type WorkOutcome,
} from "riverqueue";
import {
  SqliteDriver,
  transaction,
  type SqliteDriverOptions,
} from "@riverqueue/driver-sqlite";
import { createMigrator, type Migrator } from "@riverqueue/migrate";
import {
  abortableDelay,
  encodeJobListCursor,
  type JobListOrderBy,
  type JobListParams,
  type PilotDatabase,
} from "riverqueue/unstable-driver";

import { abortPromise } from "./abort.js";
import {
  invalidParams,
  methodNotFound,
  notFound,
  unsupported,
} from "./errors.js";
import {
  ALL_JOB_STATES,
  deleteFinalizedParams,
  optionalBigInt,
  optionalBigInts,
  optionalBoolean,
  optionalInteger,
  optionalIntegers,
  optionalIntegerValue,
  optionalRawJsonObject,
  optionalRecord,
  optionalStates,
  queueUpdateOptions,
  optionalString,
  optionalStrings,
  optionalStringValue,
  requiredBoolean,
  requiredId,
  requiredInteger,
  requiredNonEmptyString,
  requiredRecord,
  requiredSignedBigInt,
  requiredString,
  requiredUnsignedBigInt,
  requireRecordValue,
} from "./params.js";
import {
  deterministicCronNext,
  deterministicRetryDelayNanoseconds,
  deterministicUniqueKey,
  selectedUniqueComponents,
} from "./deterministic.js";
import { exactJsonTokens, normalizeJob, normalizeQueue } from "./normalize.js";
import {
  ClaimBarrierClient,
  CONFORMANCE_EVENT_KINDS,
  conformanceKindAliases,
  RuntimeProbe,
  startFetchOnlyKnownKinds,
  startLeadershipOptions,
  startPeriodicJobs,
  startTimeoutOptions,
  startWorkerKinds,
  type ConformanceWorkerKind,
} from "./start.js";
import { PilotDatabaseClient } from "./pilot-database.js";
import type {
  PortableStorageProfile,
  SqliteRuntimeProfile,
} from "./profile.js";

const ROLLBACK = Symbol("river conformance rollback");
/**
 * The SQLite driver's private test options. Strict mode fails every River
 * transaction that stays open across a turn of the event loop, so
 * conformance runs catch lock windows the driver's default probe can miss.
 */
const SQLITE_DRIVER_STRICT = {
  [Symbol.for("riverqueue.sqlite.driver.test_hooks")]: {
    strictLockWindow: true,
  },
} as SqliteDriverOptions;

interface InsertRequest {
  readonly behavior?: unknown;
  readonly duration_ms?: unknown;
  readonly kind?: unknown;
  readonly message?: unknown;
  readonly opts?: unknown;
  readonly schema?: unknown;
}

interface TransactionSession {
  finish(action: "commit" | "rollback"): Promise<void>;
  readonly tx: DatabaseSync;
}

interface Barrier {
  readonly promise: Promise<void>;
  readonly release: () => void;
}

interface RunningClient {
  readonly abort: AbortController;
  readonly claimBarrier: string | undefined;
  readonly client: Client<DatabaseSync>;
  readonly handle: RunHandle;
  readonly probe: RuntimeProbe;
  readonly pump: Promise<void>;
}

interface ParsedInsert {
  readonly args: ConformanceEchoArgs;
  readonly options: InsertOptions;
}

interface ConformanceEchoArgs extends JsonObject {
  readonly behavior: string;
  readonly duration_ms: number;
  readonly message: string;
}

const conformanceEcho = defineJob<ConformanceEchoArgs>()({
  kind: "conformance_echo",
});

/** The built-in worker's definition under each kind `start` registers. */
const conformanceWorkerDefinitions = new Map<
  ConformanceWorkerKind,
  typeof conformanceEcho
>([["conformance_echo", conformanceEcho]]);

function conformanceWorkerDefinition(
  kind: ConformanceWorkerKind
): typeof conformanceEcho {
  let definition = conformanceWorkerDefinitions.get(kind);
  if (definition === undefined) {
    definition = defineJob<ConformanceEchoArgs>()({
      kind,
      kindAliases: conformanceKindAliases(kind),
    }) as typeof conformanceEcho;
    conformanceWorkerDefinitions.set(kind, definition);
  }
  return definition;
}

/** Public SQLite portable-storage-v1 JSON-RPC implementation. */
export class PortableSqliteAdapter {
  readonly #barriers = new Map<string, Barrier>();
  readonly #database: DatabaseSync;
  readonly #client: Client<DatabaseSync>;
  readonly #driver: SqliteDriver;
  readonly #migrator: Migrator;
  readonly #profile: PortableStorageProfile | SqliteRuntimeProfile;
  readonly #transactions = new Map<string, TransactionSession>();
  #clock: Temporal.Instant | null = null;
  /** A handle of its own for workers' transactional completions. */
  #completionDatabase: DatabaseSync | null = null;
  /** The database of an extension's pilot, for `delete_finalized`. */
  #pilotDatabase: PilotDatabase<DatabaseSync> | null = null;
  #rngSeed = 0n;
  #running: RunningClient | null = null;

  constructor(
    database: DatabaseSync,
    profile: PortableStorageProfile | SqliteRuntimeProfile
  ) {
    this.#database = database;
    this.#driver = new SqliteDriver(database, SQLITE_DRIVER_STRICT);
    // River for Go's reference adapter builds a new client for each call
    // while none is running, so its inserts are never suppressed by an
    // earlier call's notification; the 1 ms cooldown of its running
    // clients comes closest for this long-lived one.
    this.#client = new Client(this.#driver, {
      fetchCooldown: { milliseconds: 1 },
    });
    this.#migrator = createMigrator({ database });
    this.#profile = profile;
    const methods =
      profile.name === "sqlite-runtime-v1"
        ? RUNTIME_IMPLEMENTED_METHODS
        : IMPLEMENTED_METHODS;
    const implemented = new Set<string>(methods);
    const missing = profile.methods.filter(
      (method) => !implemented.has(method)
    );
    const extra = methods.filter((method) => !profile.methods.includes(method));
    if (missing.length > 0 || extra.length > 0) {
      throw new Error(
        `portable adapter inventory mismatch; missing=[${missing.join(",")}], extra=[${extra.join(",")}]`
      );
    }
  }

  async close(): Promise<void> {
    if (this.#running !== null) await this.#stop({ cancel: true });
    for (const [handle, session] of [...this.#transactions]) {
      this.#transactions.delete(handle);
      await session.finish("rollback");
    }
    for (const barrier of this.#barriers.values()) barrier.release();
    this.#barriers.clear();
    this.#completionDatabase?.close();
    this.#driver.close();
  }

  async dispatch(
    method: string,
    params: Record<string, unknown>
  ): Promise<unknown> {
    if (!this.#profile.methods.includes(method)) throw methodNotFound(method);
    this.#profile.params.check(method, params);
    switch (method) {
      case "handshake":
        return {
          adapter_version: this.#profile.adapterVersion,
          backend: this.#profile.backend,
          capabilities: this.#profile.capabilities,
          implementation: "javascript",
          implementation_version: this.#profile.implementationVersion,
          methods: this.#profile.methods,
          migration_lines: {
            [this.#profile.migrationLine]: this.#profile.latestMigration,
          },
          profile: this.#profile.name,
          protocol_revision: this.#profile.protocolRevision,
        };
      case "migrate":
        return this.#migrate(params);
      case "reset":
        return this.#reset();
      case "clock_set":
        this.#clock = Temporal.Instant.from(requiredString(params, "now"));
        return {};
      case "cron_next":
        return deterministicCronNext(params);
      case "rng_seed":
        this.#rngSeed = requiredUnsignedBigInt(params, "seed");
        return {};
      case "retry_delay":
        return this.#retryDelay(params);
      case "unique_key":
        return this.#uniqueKey(params);
      case "insert": {
        const result = await this.#insert(parseInsert(params));
        return normalizeJob(result.job);
      }
      case "insert_many":
        return this.#insertMany(parseInsertMany(params), undefined);
      case "get":
        rejectSchema(params.schema);
        return normalizeJob(await this.#requiredJob(requiredId(params)));
      case "list":
        return this.#list(params);
      case "cancel":
        return normalizeJob(
          await this.#requiredMutation("cancel", requiredId(params))
        );
      case "delete":
        return normalizeJob(
          await this.#requiredMutation("delete", requiredId(params))
        );
      case "delete_finalized":
        return this.#deleteFinalized(params);
      case "delete_many":
        return this.#deleteMany(params);
      case "retry":
        return normalizeJob(
          await this.#requiredMutation("retry", requiredId(params))
        );
      case "update":
        return normalizeJob(await this.#update(params));
      case "queue_get":
        return normalizeQueue(await this.#requiredQueue(params));
      case "queue_list":
        return this.#queueList(params);
      case "queue_pause":
      case "queue_resume":
        await this.#queueControl(method, params);
        return {};
      case "queue_update":
        return normalizeQueue(await this.#queueUpdate(params));
      case "raw_job_timestamps":
        return this.#rawJobTimestamps(requiredId(params));
      case "raw_job_exact_json":
        return exactJsonTokens(await this.#requiredJob(requiredId(params)));
      case "raw_job_row":
        return this.#rawJobRow(requiredId(params));
      case "raw_notifications":
        return this.#rawNotifications(
          requiredUnsignedBigInt(params, "after_id")
        );
      case "raw_replace_json_text":
        return this.#rawReplaceJsonText(params);
      case "raw_set_kind":
        return this.#rawSetKind(params);
      case "raw_insert_exact_json":
        return this.#rawInsertExactJson(params);
      case "raw_insert_no_notify":
        return this.#rawInsertNoNotify(params);
      case "raw_finalize":
        return this.#rawFinalize(params);
      case "tx_begin":
        await this.#transactionBegin(requiredString(params, "handle"));
        return {};
      case "tx_commit":
        await this.#transactionFinish(
          requiredString(params, "handle"),
          "commit"
        );
        return {};
      case "tx_rollback":
        await this.#transactionFinish(
          requiredString(params, "handle"),
          "rollback"
        );
        return {};
      case "tx_insert": {
        const session = this.#transaction(requiredString(params, "handle"));
        const job = requiredRecord(params, "job") as InsertRequest;
        const result = await this.#insert(parseInsert(job), session.tx);
        return normalizeJob(result.job);
      }
      case "tx_insert_many": {
        const session = this.#transaction(requiredString(params, "handle"));
        return this.#insertMany(parseInsertMany(params), session.tx);
      }
      case "tx_get": {
        const tx = this.#transaction(requiredString(params, "handle")).tx;
        return normalizeJob(await this.#requiredJob(requiredId(params), tx));
      }
      case "tx_cancel":
      case "tx_delete":
      case "tx_retry": {
        const tx = this.#transaction(requiredString(params, "handle")).tx;
        return normalizeJob(
          await this.#requiredMutation(
            method.slice(3) as Mutation,
            requiredId(params),
            tx
          )
        );
      }
      case "tx_update": {
        const tx = this.#transaction(requiredString(params, "handle")).tx;
        return normalizeJob(await this.#update(params, tx));
      }
      case "tx_list": {
        const tx = this.#transaction(requiredString(params, "handle")).tx;
        return this.#list(params, tx);
      }
      case "tx_delete_many": {
        const tx = this.#transaction(requiredString(params, "handle")).tx;
        return this.#deleteMany(params, tx);
      }
      case "tx_queue_get": {
        const tx = this.#transaction(requiredString(params, "handle")).tx;
        return normalizeQueue(await this.#requiredQueue(params, tx));
      }
      case "tx_queue_list": {
        const tx = this.#transaction(requiredString(params, "handle")).tx;
        return this.#queueList(params, tx);
      }
      case "tx_queue_pause":
      case "tx_queue_resume": {
        const tx = this.#transaction(requiredString(params, "handle")).tx;
        await this.#queueControl(
          method.slice(3) as "queue_pause" | "queue_resume",
          params,
          tx
        );
        return {};
      }
      case "tx_queue_update": {
        const tx = this.#transaction(requiredString(params, "handle")).tx;
        return normalizeQueue(await this.#queueUpdate(params, tx));
      }
      case "barrier_create":
        this.#barrierCreate(requiredString(params, "name"));
        return {};
      case "barrier_release":
        this.#barrierRelease(requiredString(params, "name"));
        return {};
      case "start":
        return this.#start(params);
      case "stop":
        return this.#stop(params);
      case "wait":
        return normalizeJob(await this.#wait(params));
      case "work":
        return normalizeJob(await this.#work(params));
      case "runtime_stats":
        return this.#runtimeStats();
      case "leader":
        return this.#leader();
      case "queue_add":
        return this.#runtimeQueueAdd(params);
      case "queue_remove":
        return this.#runtimeQueueRemove(params);
      case "request_resign": {
        if (params.handle !== undefined) {
          await this.#activeClient().requestLeadershipResignation({
            tx: this.#transaction(requiredString(params, "handle")).tx,
          });
        } else if (this.#running === null) {
          await this.#client.requestLeadershipResignation();
        } else {
          await this.#running.handle.requestLeadershipResignation();
        }
        return {};
      }
    }
    throw new Error(`unimplemented SQLite profile method: ${method}`);
  }

  #activeClient(): Client<DatabaseSync> {
    return this.#running?.client ?? this.#client;
  }

  #barrierCreate(name: string): void {
    if (this.#barriers.has(name)) {
      throw new Error(`barrier ${JSON.stringify(name)} already exists`);
    }
    let release!: () => void;
    const promise = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.#barriers.set(name, { promise, release });
  }

  #barrierRelease(name: string): void {
    const barrier = this.#barriers.get(name);
    if (barrier === undefined) {
      throw notFound(`barrier ${JSON.stringify(name)} not found`);
    }
    this.#barriers.delete(name);
    barrier.release();
  }

  /**
   * The barrier `start`'s `claim_barrier` names, which must exist, or
   * undefined without one.
   */
  #claimBarrier(params: Record<string, unknown>): Barrier | undefined {
    if (params.claim_barrier === undefined) return undefined;
    const name = requiredNonEmptyString(params, "claim_barrier");
    const barrier = this.#barriers.get(name);
    if (barrier === undefined) {
      throw invalidParams(
        `claim_barrier ${JSON.stringify(name)} does not exist`
      );
    }
    return barrier;
  }

  async #deleteFinalized(
    params: Record<string, unknown>
  ): Promise<{ readonly deleted: number }> {
    this.#pilotDatabase ??= new PilotDatabaseClient(this.#driver).database;
    return {
      deleted: await this.#pilotDatabase.deleteFinalizedJobs(
        deleteFinalizedParams(params)
      ),
    };
  }

  async #deleteMany(
    params: Record<string, unknown>,
    transaction?: DatabaseSync
  ): Promise<{ readonly jobs: readonly Record<string, unknown>[] }> {
    const all = params.all === true;
    const limit = optionalInteger(params, "limit", 100, 1, 10_000);
    const jobs = await this.#activeClient().jobs.deleteMany({
      ...(all ? { all: true as const } : {}),
      ids: optionalBigInts(params, "ids"),
      kinds: optionalStrings(params, "kinds"),
      limit,
      priorities: optionalIntegers(params, "priorities", 1, 4),
      queues: optionalStrings(params, "queues"),
      states: optionalStates(params, "states", []) ?? [],
      ...(transaction === undefined ? {} : { tx: transaction }),
    });
    return { jobs: jobs.map(normalizeJob) };
  }

  #insert(params: ParsedInsert, transaction?: DatabaseSync) {
    return this.#activeClient().insert(
      conformanceEcho,
      params.args,
      transaction === undefined
        ? params.options
        : { ...params.options, tx: transaction }
    );
  }

  async #insertMany(
    jobs: readonly ParsedInsert[],
    transaction: DatabaseSync | undefined
  ): Promise<unknown> {
    if (jobs.length === 0) throw new Error("no jobs to insert");
    const items: InsertManyItem[] = jobs.map((job) => ({
      args: job.args,
      job: conformanceEcho,
      options: job.options,
    }));
    const results = await this.#activeClient().insertMany(
      items,
      transaction === undefined ? {} : { tx: transaction }
    );
    return {
      results: results.map((result) => ({
        job: normalizeJob(result.job),
        unique_skipped_as_duplicate: result.status === "duplicate",
      })),
    };
  }

  async #list(
    params: Record<string, unknown>,
    transaction?: DatabaseSync
  ): Promise<{
    readonly cursor: string | null;
    readonly jobs: readonly Record<string, unknown>[];
  }> {
    const parsed = parseListParams(params);
    const result = await this.#activeClient().jobs.list({
      ...(parsed.after === null ? {} : { after: parsed.after }),
      ids: parsed.ids,
      kinds: parsed.kinds,
      limit: parsed.limit,
      ...(parsed.metadata === null ? {} : { metadata: parsed.metadata }),
      orderBy: parsed.sortField,
      priorities: parsed.priorities,
      queues: parsed.queues,
      sortDirection: parsed.sortDirection,
      states: parsed.states,
      tagsAll: parsed.tagsAll,
      tagsAny: parsed.tagsAny,
      ...(transaction === undefined ? {} : { tx: transaction }),
    });
    // Like Go's `LastCursor`, even a short final page reports its cursor.
    const last = result.jobs.at(-1);
    return {
      cursor: last === undefined ? null : encodeJobListCursor(last, parsed),
      jobs: result.jobs.map(normalizeJob),
    };
  }

  async #migrate(params: Record<string, unknown>): Promise<unknown> {
    rejectSchema(params.schema);
    if (this.#transactions.size > 0 || this.#running !== null) {
      throw new Error("migrate requires no running client or open transaction");
    }
    const direction = params.direction ?? "up";
    if (direction !== "up" && direction !== "down") {
      throw invalidParams(
        `unknown migration direction ${JSON.stringify(direction)}`
      );
    }
    const options: {
      dryRun?: boolean;
      maxSteps?: number;
      targetVersion?: number;
    } = {};
    if (params.dry_run !== undefined)
      options.dryRun = requiredBoolean(params, "dry_run");
    if (params.max_steps !== undefined) {
      options.maxSteps = requiredInteger(params, "max_steps", 0, 10_000);
    }
    if (params.target_version !== undefined) {
      // The shared protocol follows Go, where -1 reverts every version;
      // `@riverqueue/migrate` spells that `targetVersion: 0`.
      const targetVersion = requiredInteger(
        params,
        "target_version",
        -1,
        2_147_483_647
      );
      options.targetVersion = targetVersion === -1 ? 0 : targetVersion;
    }
    const result =
      direction === "up"
        ? await this.#migrator.migrateUp(options)
        : await this.#migrator.migrateDown(options);
    const existing = await this.#migrator.existingVersions();
    const valid = (await this.#migrator.validate()).ok;
    return {
      applied: result.versions.map(({ version }) => version),
      existing,
      valid,
    };
  }

  #rawJobTimestamps(id: bigint): { created_at: string; scheduled_at: string } {
    const statement = this.#database.prepare(
      "SELECT CAST(created_at AS TEXT) AS created_at, " +
        "CAST(scheduled_at AS TEXT) AS scheduled_at FROM river_job WHERE id = ?"
    );
    const row = statement.get(id);
    if (row === undefined) throw notFound(`job ${id.toString(10)} not found`);
    if (
      typeof row.created_at !== "string" ||
      typeof row.scheduled_at !== "string"
    ) {
      throw new Error("SQLite job timestamps are not text");
    }
    return { created_at: row.created_at, scheduled_at: row.scheduled_at };
  }

  /**
   * Read the notification outbox rows after `afterId`, in ID order, exactly
   * as stored: topic and payload text, and SQLite's type of the payload.
   */
  #rawNotifications(afterId: bigint): {
    notifications: {
      id: bigint;
      payload: string;
      payload_type: string;
      topic: string;
    }[];
  } {
    const statement = this.#database.prepare(
      `SELECT id, payload, typeof(payload) AS payload_type, topic
       FROM river_notification
       WHERE id > ?
       ORDER BY id`
    );
    statement.setReadBigInts(true);
    const notifications = statement.all(afterId).map((row) => {
      const { id, payload, payload_type: payloadType, topic } = row;
      if (
        typeof id !== "bigint" ||
        typeof payload !== "string" ||
        typeof payloadType !== "string" ||
        typeof topic !== "string"
      ) {
        throw new Error("SQLite notification row has an unexpected type");
      }
      return { id, payload, payload_type: payloadType, topic };
    });
    return { notifications };
  }

  /**
   * Read a job's columns as SQLite renders them, plus each JSONB column's
   * stored bytes as uppercase hex so the harness can check that each
   * implementation stored the column as JSONB rather than text.
   */
  #rawJobRow(
    id: bigint
  ): Record<string, Record<string, string | null> | string | null> {
    const statement = this.#database.prepare(
      `SELECT json(args) AS args, CAST(attempted_at AS TEXT) AS attempted_at,
              json(attempted_by) AS attempted_by,
              CAST(created_at AS TEXT) AS created_at, json(errors) AS errors,
              CAST(finalized_at AS TEXT) AS finalized_at,
              json(metadata) AS metadata,
              CAST(scheduled_at AS TEXT) AS scheduled_at, json(tags) AS tags,
              hex(args) AS jsonb_args,
              CASE WHEN attempted_by IS NULL THEN NULL
                ELSE hex(attempted_by) END AS jsonb_attempted_by,
              CASE WHEN errors IS NULL THEN NULL
                ELSE hex(errors) END AS jsonb_errors,
              hex(metadata) AS jsonb_metadata, hex(tags) AS jsonb_tags,
              CASE WHEN unique_key IS NULL THEN NULL
                ELSE hex(unique_key) END AS unique_key,
              CASE WHEN unique_key IS NULL THEN NULL
                ELSE typeof(unique_key) END AS unique_key_type,
              CAST(unique_states AS TEXT) AS unique_states,
              CASE WHEN unique_states IS NULL THEN NULL
                ELSE typeof(unique_states) END AS unique_states_type
       FROM river_job WHERE id = ?`
    );
    const row = statement.get(id);
    if (row === undefined) throw notFound(`job ${id.toString(10)} not found`);
    const text = (column: string): string | null => {
      const value = row[column];
      if (typeof value !== "string" && value !== null) {
        throw new Error(`SQLite job column ${column} is not text`);
      }
      return value;
    };
    const result: Record<
      string,
      Record<string, string | null> | string | null
    > = {};
    for (const column of [
      "args",
      "attempted_at",
      "attempted_by",
      "created_at",
      "errors",
      "finalized_at",
      "metadata",
      "scheduled_at",
      "tags",
      "unique_key",
      "unique_key_type",
      "unique_states",
      "unique_states_type",
    ]) {
      result[column] = text(column);
    }
    const jsonb: Record<string, string | null> = {};
    for (const column of [
      "args",
      "attempted_by",
      "errors",
      "metadata",
      "tags",
    ]) {
      jsonb[column] = text(`jsonb_${column}`);
    }
    result.jsonb = jsonb;
    return result;
  }

  /**
   * Replace one of a job's JSON columns with `text` stored as TEXT, which
   * needn't be valid JSON, as an out-of-band change could, or with NULL.
   * Returns the column's previous value, as stored when it was TEXT and
   * rendered with `json()` otherwise, and its SQLite type.
   */
  async #rawReplaceJsonText(
    params: Record<string, unknown>
  ): Promise<{ previous: string | null; previous_type: string }> {
    const id = requiredId(params);
    const column = requiredString(params, "column");
    if (!RAW_JSON_COLUMNS.has(column)) {
      throw invalidParams(`unknown JSON column ${JSON.stringify(column)}`);
    }
    const text = params.text;
    if (typeof text !== "string" && text !== null) {
      throw invalidParams("text must be a string or null");
    }
    return this.#rawWrite((database) => {
      const row = database
        .prepare(
          `SELECT CASE WHEN typeof(${column}) = 'text' THEN ${column}
                    ELSE json(${column}) END AS previous,
                  typeof(${column}) AS previous_type
           FROM river_job WHERE id = ?`
        )
        .get(id);
      if (row === undefined) throw notFound(`job ${id.toString(10)} not found`);
      const { previous, previous_type: previousType } = row;
      if (
        (typeof previous !== "string" && previous !== null) ||
        typeof previousType !== "string"
      ) {
        throw new Error(`SQLite job column ${column} is not text`);
      }
      database
        .prepare(`UPDATE river_job SET ${column} = ? WHERE id = ?`)
        .run(text, id);
      return { previous, previous_type: previousType };
    });
  }

  async #rawSetKind(params: Record<string, unknown>): Promise<unknown> {
    const id = requiredId(params);
    const kind = requiredNonEmptyString(params, "kind");
    const result = await this.#rawWrite((database) =>
      database
        .prepare("UPDATE river_job SET kind = ? WHERE id = ?")
        .run(kind, id)
    );
    if (result.changes !== 1 && result.changes !== 1n) {
      throw notFound(`job ${id.toString(10)} not found`);
    }
    return normalizeJob(await this.#requiredJob(id));
  }

  async #rawInsertExactJson(
    params: Record<string, unknown>
  ): Promise<{ id: bigint }> {
    const requestedId = params.id === undefined ? null : requiredId(params);
    const metadata = optionalRawJsonObject(
      params,
      "metadata_json",
      '{"negative":-9223372036854775808}'
    );
    const result = await this.#rawWrite((database) => {
      const statement = database.prepare(
        `INSERT INTO river_job (id, args, kind, max_attempts, metadata)
         VALUES (?, jsonb(?), ?, 25, jsonb(?)) RETURNING id`
      );
      statement.setReadBigInts(true);
      return statement.get(
        requestedId,
        '{"decimal":0.12345678901234567890123456789,"integer":9223372036854775807}',
        "conformance_exact_json",
        metadata
      );
    });
    if (result === undefined || typeof result.id !== "bigint") {
      throw new Error("exact JSON insert returned no ID");
    }
    return { id: result.id };
  }

  async #rawFinalize(params: Record<string, unknown>): Promise<unknown> {
    const id = requiredId(params);
    const state = requiredString(params, "state");
    if (state !== "completed" && state !== "discarded") {
      throw invalidParams("state must be completed or discarded");
    }
    const metadata = optionalRecord(params, "metadata") ?? {};
    const error = {
      at: "2026-02-03T04:05:06.789Z",
      attempt: 1,
      error: "external discard",
      trace: "external trace",
    };
    const result = await this.#rawWrite((database) =>
      database
        .prepare(
          `UPDATE river_job
           SET errors = CASE WHEN ? = 'discarded'
                 THEN jsonb(json_insert(coalesce(json(errors), '[]'), '$[#]', jsonb(?)))
                 ELSE errors END,
               finalized_at = strftime('%Y-%m-%d %H:%M:%f', 'now'),
               metadata = jsonb_patch(metadata, jsonb(?)),
               state = ?
           WHERE id = ? AND state = 'running'`
        )
        .run(state, stringifyJson(error), stringifyJson(metadata), state, id)
    );
    if (result.changes !== 1 && result.changes !== 1n) {
      throw notFound("running job not found");
    }
    return normalizeJob(await this.#requiredJob(id));
  }

  async #rawInsertNoNotify(params: Record<string, unknown>): Promise<unknown> {
    const message = requiredString(params, "message");
    const behavior = optionalString(params, "behavior", "");
    const durationMs = optionalInteger(
      params,
      "duration_ms",
      0,
      0,
      Number.MAX_SAFE_INTEGER
    );
    const opts = optionalRecord(params, "opts") ?? {};
    const kind = optionalString(params, "kind", "conformance_echo");
    const maxAttempts = optionalInteger(opts, "max_attempts", 25, 1, 32_767);
    const result = await this.#rawWrite((database) => {
      const statement = database.prepare(
        `INSERT INTO river_job (args, kind, max_attempts)
         VALUES (jsonb(?), ?, ?) RETURNING id`
      );
      statement.setReadBigInts(true);
      return statement.get(
        stringifyJson({ behavior, duration_ms: durationMs, message }),
        kind,
        maxAttempts
      );
    });
    if (result === undefined || typeof result.id !== "bigint") {
      throw new Error("raw insert returned no ID");
    }
    return normalizeJob(await this.#requiredJob(result.id));
  }

  async #queueControl(
    method: "queue_pause" | "queue_resume",
    params: Record<string, unknown>,
    transaction?: DatabaseSync
  ): Promise<void> {
    const name = requiredString(params, "name");
    const options = transaction === undefined ? {} : { tx: transaction };
    const queue =
      method === "queue_pause"
        ? await this.#activeClient().queues.pause(name, options)
        : await this.#activeClient().queues.resume(name, options);
    if (name !== "*" && queue === null) {
      throw notFound(`queue ${JSON.stringify(name)} not found`);
    }
  }

  async #queueList(
    params: Record<string, unknown>,
    transaction?: DatabaseSync
  ): Promise<{ queues: readonly Record<string, unknown>[] }> {
    const result = await this.#activeClient().queues.list({
      limit: optionalInteger(params, "limit", 100, 1, 10_000),
      ...(transaction === undefined ? {} : { tx: transaction }),
    });
    return { queues: result.queues.map(normalizeQueue) };
  }

  async #queueUpdate(
    params: Record<string, unknown>,
    transaction?: DatabaseSync
  ) {
    const name = requiredString(params, "name");
    const queue = await this.#activeClient().queues.update(
      name,
      queueUpdateOptions(params),
      transaction === undefined ? {} : { tx: transaction }
    );
    if (queue === null) {
      throw notFound(`queue ${JSON.stringify(name)} not found`);
    }
    return queue;
  }

  async #requiredQueue(
    params: Record<string, unknown>,
    transaction?: DatabaseSync
  ) {
    const name = requiredString(params, "name");
    const queue = await this.#activeClient().queues.get(
      name,
      transaction === undefined ? {} : { tx: transaction }
    );
    if (queue === null) {
      throw notFound(`queue ${JSON.stringify(name)} not found`);
    }
    return queue;
  }

  async #requiredJob(id: bigint, transaction?: DatabaseSync): Promise<JobRow> {
    const job = await this.#activeClient().jobs.get(
      id,
      transaction === undefined ? {} : { tx: transaction }
    );
    if (job === null) throw notFound(`job ${id.toString(10)} not found`);
    return job;
  }

  async #requiredMutation(
    mutation: Mutation,
    id: bigint,
    transaction?: DatabaseSync
  ): Promise<JobRow> {
    const options = transaction === undefined ? {} : { tx: transaction };
    if (mutation === "delete") {
      try {
        const job = await this.#activeClient().jobs.delete(id, options);
        if (job === null) throw notFound(`job ${id.toString(10)} not found`);
        return job;
      } catch (error: unknown) {
        if (error instanceof JobRunningError) {
          throw new Error(`job ${id.toString(10)} is running`, {
            cause: error,
          });
        }
        throw error;
      }
    }
    const job =
      mutation === "cancel"
        ? await this.#activeClient().jobs.cancel(id, options)
        : await this.#activeClient().jobs.retry(id, options);
    if (job === null) throw notFound(`job ${id.toString(10)} not found`);
    return job;
  }

  async #reset(): Promise<Record<string, never>> {
    if (this.#transactions.size > 0 || this.#running !== null) {
      throw new Error("reset requires no running client or open transaction");
    }
    for (const barrier of this.#barriers.values()) barrier.release();
    this.#barriers.clear();
    await this.#rawWrite((database) => {
      database.exec(
        "DELETE FROM river_notification; DELETE FROM river_job; " +
          "DELETE FROM river_queue; DELETE FROM river_leader"
      );
    });
    return {};
  }

  /**
   * Run raw SQL that writes on the application handle: in the transaction a
   * session has open on it, or in one of its own, which waits for another
   * connection's write lock without blocking the event loop.
   */
  async #rawWrite<T>(write: (database: DatabaseSync) => T): Promise<T> {
    if (this.#database.isTransaction) return write(this.#database);
    return transaction(this.#database, write);
  }

  #retryDelay(params: Record<string, unknown>): { delay_ns: bigint } {
    if (this.#clock === null) {
      throw new Error("clock_set is required before retry_delay");
    }
    return {
      delay_ns: deterministicRetryDelayNanoseconds({
        errorCount: requiredInteger(params, "error_count", 1, 2_147_483_647),
        jobId: requiredSignedBigInt(params, "job_id"),
        now: this.#clock,
        seed: this.#rngSeed,
      }),
    };
  }

  async #transactionBegin(handle: string): Promise<void> {
    if (this.#transactions.has(handle)) {
      throw new Error(`transaction ${JSON.stringify(handle)} already exists`);
    }
    if (this.#transactions.size > 0) {
      throw new Error(
        "SQLite portable adapter supports one serialized transaction at a time"
      );
    }
    let readyResolve!: (transaction: DatabaseSync) => void;
    let readyReject!: (error: unknown) => void;
    const ready = new Promise<DatabaseSync>((resolve, reject) => {
      readyResolve = resolve;
      readyReject = reject;
    });
    let decisionResolve!: (action: "commit" | "rollback") => void;
    const decision = new Promise<"commit" | "rollback">((resolve) => {
      decisionResolve = resolve;
    });
    const finished = transaction(this.#database, async (tx) => {
      readyResolve(tx);
      // eslint-disable-next-line @typescript-eslint/only-throw-error -- a unique sentinel the driver rethrows unchanged
      if ((await decision) === "rollback") throw ROLLBACK;
    });
    void finished.catch((error: unknown) => readyReject(error));
    const tx = await ready;
    this.#transactions.set(handle, {
      finish: async (action) => {
        decisionResolve(action);
        try {
          await finished;
        } catch (error: unknown) {
          if (action !== "rollback" || error !== ROLLBACK) throw error;
        }
      },
      tx,
    });
  }

  async #transactionFinish(
    handle: string,
    action: "commit" | "rollback"
  ): Promise<void> {
    const session = this.#transaction(handle);
    this.#transactions.delete(handle);
    await session.finish(action);
  }

  #transaction(handle: string): TransactionSession {
    const session = this.#transactions.get(handle);
    if (session === undefined) {
      throw notFound(`transaction ${JSON.stringify(handle)} not found`);
    }
    return session;
  }

  async #handleWork(
    context: WorkContext,
    probe: RuntimeProbe
  ): Promise<WorkOutcome | undefined> {
    const args = context.job.args as Record<string, unknown>;
    const behavior = typeof args.behavior === "string" ? args.behavior : "";
    const message = typeof args.message === "string" ? args.message : "";
    const durationMs =
      typeof args.duration_ms === "number" &&
      Number.isSafeInteger(args.duration_ms)
        ? args.duration_ms
        : 0;
    switch (behavior) {
      case "barrier_output":
      case "barrier_wait": {
        const barrier = this.#barriers.get(message);
        if (barrier === undefined) {
          throw new Error(`barrier ${JSON.stringify(message)} not found`);
        }
        await Promise.race([barrier.promise, abortPromise(context.signal)]);
        if (behavior === "barrier_output") recordOutput({ race: "worker" });
        return;
      }
      case "cancel":
        throw new JobCancelledError(context.job.id);
      // Like `cooperative_cancel`, these wait for the attempt's signal, then
      // fail genuinely so shutdown can tell a failure from a cooperative stop.
      case "cancel_error":
        await abortPromise(context.signal).catch(() => undefined);
        throw new Error("conformance failure after cancellation");
      case "cancel_panic":
        await abortPromise(context.signal).catch(() => undefined);
        throw new TypeError("conformance panic after cancellation");
      case "cooperative_cancel":
        if (context.signal.aborted) probe.cancelledAtStart++;
        await abortPromise(context.signal);
        return;
      case "discard":
        return discard({ reason: "conformance discard" });
      case "error":
        throw new Error("conformance retryable error");
      case "output":
        recordOutput({ message });
        return complete();
      case "panic":
        // A runtime fault is JavaScript's analog of a Go panic, so River
        // records its stack trace.
        throw new TypeError("conformance worker panic");
      case "sleep":
        await abortableDelay(durationMs, context.signal);
        return;
      case "snooze_once":
      case "snooze_then_cancel":
        if (!Object.hasOwn(context.job.metadata, "snoozes")) {
          return snooze({
            seconds: Math.max(1, Math.ceil(durationMs / 1_000)),
          });
        }
        if (behavior === "snooze_then_cancel") {
          await abortPromise(context.signal);
        }
        return;
      case "resumable_cursor":
        await context.resumable
          .step("first", () => {
            context.setMetadata("first_attempt", context.job.attempt);
          })
          .catch(() => undefined);
        await context.resumable
          .stepWithCursor("second", (cursor) => {
            if (context.job.attempt === 1) {
              context.resumable.setCursor(7);
              throw new Error("retry with cursor");
            }
            if (cursor !== 7)
              throw new Error(
                `expected cursor 7, got ${JSON.stringify(cursor)}`
              );
            context.setMetadata("cursor_observed", cursor);
          })
          .catch(() => undefined);
        await context.resumable
          .step("third", () => {
            if (context.job.attempt === 2)
              throw new Error("retry after consuming cursor");
          })
          .catch(() => undefined);
        return;
      case "resumable":
      case "resumable_duplicate":
        await context.resumable.step("first", () => {
          probe.resumableFirstRuns++;
        });
        await context.resumable.step(
          behavior === "resumable_duplicate" ? "first" : "second",
          () => {
            probe.resumableSecondRuns++;
            if (context.job.attempt === 1) {
              throw new Error("fail second resumable step once");
            }
          }
        );
        return;
      case "transactional_complete":
        return this.#transactionalComplete(context);
      default:
        return;
    }
  }

  async #leader(): Promise<{
    elected_at: string | null;
    leader_id: string | null;
  }> {
    // The driver exposes no operations, so read River's leader row directly.
    const row = this.#database
      .prepare(
        "SELECT elected_at, expires_at, leader_id FROM river_leader LIMIT 1"
      )
      .get();
    const instant = (value: unknown): Temporal.Instant => {
      if (typeof value !== "string") {
        throw new Error("SQLite leader timestamp is not text");
      }
      return Temporal.Instant.from(`${value.replace(" ", "T")}Z`);
    };
    if (
      row === undefined ||
      typeof row.leader_id !== "string" ||
      Temporal.Instant.compare(
        instant(row.expires_at),
        Temporal.Now.instant()
      ) < 0
    ) {
      return { elected_at: null, leader_id: null };
    }
    return {
      elected_at: instant(row.elected_at).toString(),
      leader_id: row.leader_id,
    };
  }

  #runtimeStats(): Record<string, unknown> {
    if (this.#running === null) {
      throw new Error("runtime_stats requires a running client");
    }
    return this.#running.probe.snapshot();
  }

  async #runtimeQueueAdd(
    params: Record<string, unknown>
  ): Promise<Record<string, never>> {
    const running = this.#running;
    if (running === null)
      throw new Error("queue_add requires a running client");
    const name = requiredString(params, "name");
    const config = {
      maxWorkers: optionalInteger(params, "max_workers", 1, 1, 10_000),
      pollInterval: {
        milliseconds: optionalInteger(
          params,
          "fetch_poll_interval_ms",
          10,
          1,
          Number.MAX_SAFE_INTEGER
        ),
      },
    };
    if (Object.hasOwn(running.handle.diagnostics.queues, name)) {
      await running.handle.updateQueue(name, config);
    } else {
      await running.handle.addQueue(name, config);
    }
    return {};
  }

  async #runtimeQueueRemove(
    params: Record<string, unknown>
  ): Promise<Record<string, never>> {
    const running = this.#running;
    if (running === null) {
      throw new Error("queue_remove requires a running client");
    }
    const name = requiredString(params, "name");
    if (!(await running.handle.removeQueue(name))) {
      throw new Error(`queue ${JSON.stringify(name)} is not configured`);
    }
    return {};
  }

  async #start(
    params: Record<string, unknown>
  ): Promise<Record<string, never>> {
    if (this.#running !== null) throw new Error("client already running");
    const queue = optionalString(params, "queue", "default");
    const maxWorkers = optionalInteger(params, "max_workers", 4, 1, 10_000);
    const pollIntervalMs = optionalInteger(
      params,
      "fetch_poll_interval_ms",
      10,
      1,
      Number.MAX_SAFE_INTEGER
    );
    const probe = new RuntimeProbe();
    const workers = new Workers();
    for (const kind of startWorkerKinds(params)) {
      workers.add(conformanceWorkerDefinition(kind), (context) =>
        this.#handleWork(context, probe)
      );
    }
    const retryDelay =
      params.retry_delay_ms === undefined
        ? undefined
        : requiredInteger(params, "retry_delay_ms", 0, Number.MAX_SAFE_INTEGER);
    const instrumented = params.instrumented === true;
    const plugin: RiverPlugin = {
      hooks: {
        afterWork: () => {
          probe.trace.push("hook:work_end");
        },
        beforeInsert: () => {
          probe.trace.push("hook:insert_begin");
        },
        beforeWork: () => {
          probe.trace.push("hook:work_begin");
        },
        onPeriodicJobsStart: () => {
          probe.periodicStarts++;
          probe.trace.push("hook:periodic_start");
        },
      },
      insertMiddleware: [
        async (_context, next) => {
          probe.trace.push("middleware:insert_before");
          const results = await next();
          probe.trace.push("middleware:insert_after");
          return results;
        },
      ],
      middleware: [
        async (_context, next) => {
          probe.trace.push("middleware:work_before");
          const outcome = await next();
          probe.trace.push("middleware:work_after");
          return outcome;
        },
      ],
      name: "conformance",
    };
    const options: ClientOptions<DatabaseSync> = {
      clientId: optionalString(
        params,
        "client_id",
        "javascript-conformance-adapter"
      ),
      errorHandler: (_context, error) => {
        if (error instanceof JobCancelledError) return { cancel: true };
        if (params.error_handler_cancel === true) {
          probe.errorHandlerCalls++;
          return { cancel: true };
        }
        return undefined;
      },
      // Like River for Go's reference adapter, fetch and notify inserts
      // with a 1 ms cooldown.
      fetchCooldown: { milliseconds: 1 },
      fetchOnlyKnownKinds: startFetchOnlyKnownKinds(params),
      ...startTimeoutOptions(params),
      ...startLeadershipOptions(params),
      periodicJobs: startPeriodicJobs(params, conformanceEcho),
      pollOnly: params.poll_only === true,
      queues: {
        [queue]: {
          maxWorkers,
          pollInterval: { milliseconds: pollIntervalMs },
        },
      },
      queueControlPollInterval: { milliseconds: 20 },
      stuckHandler: probe.stuckHandler,
      ...(instrumented ? { plugins: [plugin] } : {}),
      ...(retryDelay === undefined
        ? {}
        : {
            retryPolicy: (_job: Readonly<JobRow>, now: Temporal.Instant) =>
              now.add({ milliseconds: retryDelay }),
          }),
      workers,
    };
    const claimBarrier = this.#claimBarrier(params);
    const claimBarrierName =
      claimBarrier === undefined
        ? undefined
        : requiredNonEmptyString(params, "claim_barrier");
    const client =
      claimBarrier === undefined
        ? new Client(this.#driver, options)
        : new ClaimBarrierClient(this.#driver, options, claimBarrier.promise);
    const abort = new AbortController();
    const subscription = client.subscribe({
      kinds: CONFORMANCE_EVENT_KINDS,
      signal: abort.signal,
    });
    const pump = (async () => {
      for await (const event of subscription) probe.events.push(event.kind);
    })();
    const handle = await client.start();
    this.#running = {
      abort,
      claimBarrier: claimBarrierName,
      client,
      handle,
      probe,
      pump,
    };
    return {};
  }

  async #stop(params: Record<string, unknown>): Promise<Record<string, never>> {
    const running = this.#running;
    if (running === null) throw new Error("client is not running");
    this.#running = null;
    // A claim held on the barrier would keep the client from stopping.
    if (running.claimBarrier !== undefined) {
      const barrier = this.#barriers.get(running.claimBarrier);
      this.#barriers.delete(running.claimBarrier);
      barrier?.release();
    }
    try {
      await running.handle.stop({
        mode: params.cancel === true ? "cancel" : "graceful",
        timeout: { milliseconds: 10_000 },
      });
    } finally {
      running.abort.abort();
      await running.pump;
    }
    return {};
  }

  async #transactionalComplete(context: WorkContext): Promise<WorkOutcome> {
    this.#completionDatabase ??= this.#driver.connect();
    await transaction(this.#completionDatabase, async (tx) => {
      const updated = await context.client.jobs.update(
        context.job.id,
        { metadata: { transactional_completion: true } },
        { tx }
      );
      if (updated === null) {
        throw new Error(
          `transactional completion job ${context.job.id.toString(10)} not found`
        );
      }
      await context.completeTx(tx);
    });
    return complete();
  }

  async #wait(params: Record<string, unknown>): Promise<JobRow> {
    const id = requiredId(params);
    const states = optionalStates(params, "states", [
      JOB_STATE.cancelled,
      JOB_STATE.completed,
      JOB_STATE.discarded,
    ]) as readonly JobState[];
    const deadline = Date.now() + 10_000;
    for (;;) {
      const job = await this.#activeClient().jobs.get(id);
      if (job === null) throw notFound(`job ${id.toString(10)} not found`);
      if (states.includes(job.state)) return job;
      if (Date.now() >= deadline) {
        throw new Error(`timed out waiting for job ${id.toString(10)}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }

  async #work(params: Record<string, unknown>): Promise<JobRow> {
    if (this.#running !== null) {
      throw new Error("work requires no running client");
    }
    await this.#start({
      client_id: optionalString(
        params,
        "client_id",
        "javascript-conformance-adapter"
      ),
      max_workers: 1,
    });
    try {
      return await this.#wait(params);
    } finally {
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- started while awaiting
      if (this.#running !== null) await this.#stop({});
    }
  }

  #uniqueKey(params: Record<string, unknown>): unknown {
    const options = requiredRecord(params, "options");
    const kind = requiredString(params, "kind");
    const selectedPaths =
      kind === "conformance_selected_args"
        ? ["account.id", "account.region", "label", "path/key"]
        : kind === "conformance_dotted_selected_args"
          ? ["user.id", "user\\.id"]
          : null;
    if (!UNIQUE_FIXTURE_KINDS.has(kind)) {
      throw new Error(
        `unsupported unique fixture kind ${JSON.stringify(kind)}`
      );
    }
    const states = optionalStates(options, "by_state", undefined);
    return deterministicUniqueKey({
      args: params.args,
      kind,
      now: requiredString(params, "now"),
      options: {
        by_args: optionalBoolean(options, "by_args", false),
        by_period_nanos: optionalBigInt(options, "by_period_nanos", 0n),
        by_queue: optionalBoolean(options, "by_queue", false),
        ...(states === undefined ? {} : { by_state: states }),
        exclude_kind: optionalBoolean(options, "exclude_kind", false),
      },
      queue: requiredString(params, "queue"),
      scheduled_at:
        params.scheduled_at === null || params.scheduled_at === undefined
          ? null
          : requiredString(params, "scheduled_at"),
      selected_unique_components: selectedUniqueComponents(params),
      selected_unique_paths: selectedPaths,
    });
  }

  async #update(
    params: Record<string, unknown>,
    transaction?: DatabaseSync
  ): Promise<JobRow> {
    const updates: {
      metadata?: JsonObject;
      output?: JsonValue;
    } = {};
    const metadata = optionalRecord(params, "metadata");
    if (metadata !== null) updates.metadata = metadata as JsonObject;
    if (Object.hasOwn(params, "output")) {
      updates.output = params.output as JsonValue;
    }
    const job = await this.#activeClient().jobs.update(
      requiredId(params),
      updates,
      transaction === undefined ? {} : { tx: transaction }
    );
    if (job === null)
      throw notFound(`job ${requiredId(params).toString(10)} not found`);
    return job;
  }
}

type Mutation = "cancel" | "delete" | "retry";

const IMPLEMENTED_METHODS = [
  "cancel",
  "clock_set",
  "cron_next",
  "delete",
  "delete_many",
  "get",
  "handshake",
  "insert",
  "insert_many",
  "list",
  "migrate",
  "raw_insert_exact_json",
  "raw_job_exact_json",
  "raw_job_row",
  "raw_job_timestamps",
  "reset",
  "retry",
  "retry_delay",
  "rng_seed",
  "tx_begin",
  "tx_cancel",
  "tx_commit",
  "tx_delete",
  "tx_delete_many",
  "tx_get",
  "tx_insert",
  "tx_insert_many",
  "tx_list",
  "tx_retry",
  "tx_rollback",
  "tx_update",
  "unique_key",
  "update",
] as const;
const RUNTIME_IMPLEMENTED_METHODS = [
  "barrier_create",
  "barrier_release",
  "cancel",
  "clock_set",
  "cron_next",
  "delete",
  "delete_finalized",
  "delete_many",
  "get",
  "handshake",
  "insert",
  "insert_many",
  "leader",
  "list",
  "migrate",
  "queue_add",
  "queue_get",
  "queue_list",
  "queue_pause",
  "queue_remove",
  "queue_resume",
  "queue_update",
  "raw_finalize",
  "raw_insert_exact_json",
  "raw_insert_no_notify",
  "raw_job_exact_json",
  "raw_job_row",
  "raw_job_timestamps",
  "raw_notifications",
  "raw_replace_json_text",
  "raw_set_kind",
  "request_resign",
  "reset",
  "retry",
  "retry_delay",
  "rng_seed",
  "runtime_stats",
  "start",
  "stop",
  "tx_begin",
  "tx_cancel",
  "tx_commit",
  "tx_delete",
  "tx_delete_many",
  "tx_get",
  "tx_insert",
  "tx_insert_many",
  "tx_list",
  "tx_queue_get",
  "tx_queue_list",
  "tx_queue_pause",
  "tx_queue_resume",
  "tx_queue_update",
  "tx_retry",
  "tx_rollback",
  "tx_update",
  "unique_key",
  "update",
  "wait",
  "work",
] as const;
/** The `river_job` JSON columns `raw_replace_json_text` may replace. */
const RAW_JSON_COLUMNS: ReadonlySet<string> = new Set([
  "args",
  "attempted_by",
  "errors",
  "metadata",
  "tags",
]);
const UNIQUE_FIXTURE_KINDS = new Set([
  "conformance_all_args",
  "conformance_dotted_selected_args",
  "conformance_numeric_boundaries",
  "conformance_selected_args",
  "conformance_simple",
]);

function parseInsert(request: InsertRequest): ParsedInsert {
  rejectSchema(request.schema);
  const message = requiredString(request as Record<string, unknown>, "message");
  const opts =
    request.opts === undefined ? {} : requireRecordValue(request.opts, "opts");
  const behavior = optionalStringValue(request.behavior, "behavior", "");
  const durationMs = optionalIntegerValue(
    request.duration_ms,
    "duration_ms",
    0,
    0,
    Number.MAX_SAFE_INTEGER
  );
  const createdAt = Temporal.Now.instant();
  const hasExplicitSchedule =
    opts.scheduled_at !== undefined && opts.scheduled_at !== null;
  const scheduledAt = !hasExplicitSchedule
    ? createdAt
    : Temporal.Instant.from(requiredString(opts, "scheduled_at"));
  const unique = optionalRecord(opts, "unique") ?? {};
  const byPeriodMs = optionalBigInt(unique, "by_period_ms", 0n);
  const uniqueStates = optionalStates(unique, "by_state", undefined);
  const uniqueEnabled =
    optionalBoolean(unique, "by_args", false) ||
    byPeriodMs > 0n ||
    optionalBoolean(unique, "by_queue", false) ||
    optionalBoolean(unique, "exclude_kind", false) ||
    uniqueStates !== undefined;
  const queue = optionalString(opts, "queue", "default");
  const args = { behavior, duration_ms: durationMs, message };
  const options: InsertOptions = {
    maxAttempts: optionalInteger(opts, "max_attempts", 25, 1, 32_767),
    metadata: (optionalRecord(opts, "metadata") ?? {}) as JsonObject,
    pending: optionalBoolean(opts, "pending", false),
    // River validates the priority range, which the contract leaves open.
    priority: optionalInteger(
      opts,
      "priority",
      1,
      Number.MIN_SAFE_INTEGER,
      Number.MAX_SAFE_INTEGER
    ),
    queue,
    tags: optionalStrings(opts, "tags"),
  };
  if (hasExplicitSchedule) options.scheduledAt = scheduledAt;
  if (uniqueEnabled) {
    const byPeriodMilliseconds = Number(byPeriodMs);
    if (!Number.isSafeInteger(byPeriodMilliseconds)) {
      throw new RangeError("unique.by_period_ms exceeds the safe JS range");
    }
    options.unique = {
      ...(optionalBoolean(unique, "by_args", false) ? { byArgs: true } : {}),
      ...(byPeriodMilliseconds === 0
        ? {}
        : {
            byPeriod: Temporal.Duration.from({
              milliseconds: byPeriodMilliseconds,
            }),
          }),
      byQueue: optionalBoolean(unique, "by_queue", false),
      ...(uniqueStates === undefined ? {} : { byState: uniqueStates }),
      excludeKind: optionalBoolean(unique, "exclude_kind", false),
    };
  }
  return { args, options };
}

function parseInsertMany(
  params: Record<string, unknown>
): readonly ParsedInsert[] {
  const jobs = params.jobs;
  if (!Array.isArray(jobs)) throw invalidParams("missing jobs");
  return jobs.map((job, index) => {
    if (job === null || typeof job !== "object" || Array.isArray(job)) {
      throw invalidParams(`jobs[${index}] must be an object`);
    }
    return parseInsert(job as InsertRequest);
  });
}

/** List parameters with the caller's cursor text, for the public API. */
function parseListParams(
  params: Record<string, unknown>
): Omit<JobListParams, "after"> & { readonly after: string | null } {
  const orderBy = params.order_by ?? "id";
  const sortField: JobListOrderBy =
    orderBy === "id"
      ? "id"
      : orderBy === "scheduled_at"
        ? "scheduledAt"
        : orderBy === "finalized_at"
          ? "finalizedAt"
          : orderBy === "time"
            ? "time"
            : (() => {
                throw invalidParams(
                  `unsupported order_by ${JSON.stringify(orderBy)}`
                );
              })();
  const direction = params.direction ?? "asc";
  if (direction !== "asc" && direction !== "desc") {
    throw invalidParams(`unsupported direction ${JSON.stringify(direction)}`);
  }
  return {
    after:
      params.after === undefined || params.after === ""
        ? null
        : requiredString(params, "after"),
    ids: optionalBigInts(params, "ids"),
    kinds: optionalStrings(params, "kinds"),
    limit: optionalInteger(params, "limit", 100, 1, 10_000),
    metadata: optionalRecord(params, "metadata") as JsonObject | null,
    priorities: optionalIntegers(params, "priorities", 1, 4),
    queues: optionalStrings(params, "queues"),
    sortDirection: direction,
    sortField,
    states: optionalStates(params, "states", ALL_JOB_STATES) ?? ALL_JOB_STATES,
    tagsAll: optionalStrings(params, "tags_all"),
    tagsAny: optionalStrings(params, "tags_any"),
  };
}

function rejectSchema(value: unknown): void {
  if (value !== undefined && value !== "") {
    throw unsupported("SQLite conformance does not support custom schemas");
  }
}
