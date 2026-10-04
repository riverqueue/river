import { Buffer } from "node:buffer";
import type { ClientBase, Pool, PoolClient } from "pg";
import {
  Client,
  JOB_STATE,
  JobCancelledError,
  Workers,
  complete,
  defineJob,
  discard,
  recordOutput,
  snooze,
  type ClientOptions,
  type InsertManyItem,
  type InsertOptions,
  type JobDefinition,
  type JobRow,
  type JobState,
  type JsonObject,
  type JsonValue,
  type RiverPlugin,
  type RunHandle,
  type WorkContext,
  type WorkOutcome,
} from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { createMigrator } from "@riverqueue/migrate";
import { WorkerThreads } from "@riverqueue/worker-threads";
import {
  abortableDelay,
  decodeJobListCursor,
  encodeJobListCursor,
  type JobListOrderBy,
  type JobListParams,
  type PilotDatabase,
  quoteIdentifier,
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
} from "./start.js";
import { PilotDatabaseClient } from "./pilot-database.js";
import type { PostgresFullProfile } from "./profile.js";

export const POSTGRES_CONFORMANCE_APPLICATION_NAME =
  process.env.RIVER_CONFORMANCE_APPLICATION_NAME ||
  "river-conformance-javascript";
const UNIQUE_FIXTURE_KINDS = new Set([
  "conformance_all_args",
  "conformance_dotted_selected_args",
  "conformance_numeric_boundaries",
  "conformance_selected_args",
  "conformance_simple",
]);

interface InsertRequest extends Record<string, unknown> {
  readonly behavior?: unknown;
  readonly duration_ms?: unknown;
  readonly kind?: unknown;
  readonly message?: unknown;
  readonly opts?: unknown;
  readonly schema?: unknown;
}

export interface ParsedInsert {
  readonly args: JsonObject;
  readonly definition: JobDefinition<JsonObject>;
  readonly options: InsertOptions;
  readonly schema: string | undefined;
}

interface RunningClient {
  readonly abort: AbortController;
  readonly claimBarrier: string | undefined;
  readonly client: Client<PoolClient>;
  /** Worker-thread executor owned by this running client, if any. */
  readonly executor: WorkerThreads | undefined;
  readonly handle: RunHandle;
  readonly pump: Promise<void>;
  readonly probe: RuntimeProbe;
  readonly schema: string | undefined;
}

class AdapterPgLease {
  readonly client: PoolClient;
  readonly #failure: Promise<never>;
  #failureError: Error | undefined;
  #failureReject!: (error: Error) => void;
  #released = false;

  constructor(client: PoolClient) {
    this.client = client;
    this.#failure = new Promise<never>((_resolve, reject) => {
      this.#failureReject = reject;
    });
    void this.#failure.catch(() => undefined);
    this.client.on("error", this.#onFailure);
  }

  get failed(): boolean {
    return this.#failureError !== undefined;
  }

  destroy(): void {
    if (this.#released) return;
    this.#released = true;
    this.client.once("end", () => this.client.off("error", this.#onFailure));
    this.client.release(true);
  }

  async race<T>(operation: PromiseLike<T> | T): Promise<T> {
    if (this.#failureError !== undefined) throw this.#failureError;
    return Promise.race([Promise.resolve(operation), this.#failure]);
  }

  release(): void {
    if (this.#released) return;
    this.#released = true;
    this.client.off("error", this.#onFailure);
    this.client.release();
  }

  readonly #onFailure = (error: Error): void => {
    if (this.#failureError !== undefined) return;
    this.#failureError = error;
    this.destroy();
    this.#failureReject(error);
  };
}

interface Barrier {
  readonly promise: Promise<void>;
  readonly release: () => void;
}

/** PostgreSQL implementation of River's exact full conformance contract. */
export class PostgresConformanceAdapter {
  readonly #barriers = new Map<string, Barrier>();
  readonly #definitions = new Map<string, JobDefinition<JsonObject>>();
  readonly #pool: Pool;
  readonly #profile: PostgresFullProfile;
  readonly #transactions = new Map<string, AdapterPgLease>();
  #clock: Temporal.Instant | null = null;
  /** The database of an extension's pilot, for `delete_finalized`. */
  #pilotDatabase: PilotDatabase<ClientBase> | null = null;
  #rngSeed = 0n;
  #running: RunningClient | null = null;

  constructor(pool: Pool, profile: PostgresFullProfile) {
    this.#pool = pool;
    this.#profile = profile;
    this.#definition("conformance_echo");
  }

  async close(): Promise<void> {
    if (this.#running !== null) await this.#stop({ cancel: true });
    for (const lease of this.#transactions.values()) {
      try {
        if (!lease.failed) {
          await lease.race(lease.client.query("ROLLBACK"));
        }
      } finally {
        lease.release();
      }
    }
    this.#transactions.clear();
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
          application_name: POSTGRES_CONFORMANCE_APPLICATION_NAME,
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
        return this.#reset(params);
      case "clock_set":
        this.#clock = Temporal.Instant.from(
          requiredNonEmptyString(params, "now")
        );
        return {};
      case "cron_next":
        return deterministicCronNext(params);
      case "rng_seed":
        this.#rngSeed = requiredUnsignedBigInt(params, "seed");
        return {};
      case "retry_delay":
        return this.#retryDelay(params);
      case "unique_key":
        return uniqueKeyResult(params);
      case "insert":
        return normalizeJob(await this.#insert(parseInsert(params)));
      case "insert_many":
        return this.#insertMany(params);
      case "benchmark_enqueue":
        return this.#benchmarkEnqueue(params);
      case "get":
        return normalizeJob(await this.#requiredJob(params));
      case "list":
        return this.#list(params);
      case "cancel":
      case "delete":
      case "retry":
        return normalizeJob(
          await this.#requiredMutation(method, params, undefined)
        );
      case "delete_finalized":
        return this.#deleteFinalized(params);
      case "delete_many":
        return this.#deleteMany(params);
      case "update":
        return normalizeJob(await this.#update(params));
      case "queue_get":
        return normalizeQueue(await this.#requiredQueue(params, undefined));
      case "queue_list":
        return this.#queueList(params);
      case "queue_pause":
      case "queue_resume":
        await this.#queueControl(method, params, undefined);
        return {};
      case "queue_update":
        return normalizeQueue(await this.#queueUpdate(params, undefined));
      case "tx_begin":
        await this.#transactionBegin(requiredNonEmptyString(params, "handle"));
        return {};
      case "tx_commit":
      case "tx_rollback":
        await this.#transactionFinish(
          requiredNonEmptyString(params, "handle"),
          method === "tx_commit" ? "COMMIT" : "ROLLBACK"
        );
        return {};
      case "tx_fail":
        await this.#transaction(requiredNonEmptyString(params, "handle")).query(
          "SELECT 1 / 0"
        );
        return {};
      case "tx_insert": {
        const parsed = parseInsert(requiredRecord(params, "job"));
        return normalizeJob(
          await this.#insert(
            parsed,
            this.#transaction(requiredNonEmptyString(params, "handle"))
          )
        );
      }
      case "tx_insert_many":
        return this.#insertMany(
          params,
          this.#transaction(requiredNonEmptyString(params, "handle"))
        );
      case "tx_get":
        return normalizeJob(
          await this.#requiredJob(
            params,
            this.#transaction(requiredNonEmptyString(params, "handle"))
          )
        );
      case "tx_cancel":
      case "tx_delete":
      case "tx_retry":
        return normalizeJob(
          await this.#requiredMutation(
            method.slice(3) as "cancel" | "delete" | "retry",
            params,
            this.#transaction(requiredNonEmptyString(params, "handle"))
          )
        );
      case "tx_update":
        return normalizeJob(
          await this.#update(
            params,
            this.#transaction(requiredNonEmptyString(params, "handle"))
          )
        );
      case "tx_list":
        return this.#list(
          params,
          this.#transaction(requiredNonEmptyString(params, "handle"))
        );
      case "tx_delete_many":
        return this.#deleteMany(
          params,
          this.#transaction(requiredNonEmptyString(params, "handle"))
        );
      case "tx_queue_get":
        return normalizeQueue(
          await this.#requiredQueue(
            params,
            this.#transaction(requiredNonEmptyString(params, "handle"))
          )
        );
      case "tx_queue_list":
        return this.#queueList(
          params,
          this.#transaction(requiredNonEmptyString(params, "handle"))
        );
      case "tx_queue_pause":
      case "tx_queue_resume":
        await this.#queueControl(
          method.slice(3) as "queue_pause" | "queue_resume",
          params,
          this.#transaction(requiredNonEmptyString(params, "handle"))
        );
        return {};
      case "tx_queue_update":
        return normalizeQueue(
          await this.#queueUpdate(
            params,
            this.#transaction(requiredNonEmptyString(params, "handle"))
          )
        );
      case "raw_job_timestamps":
        return this.#rawJobTimestamps(requiredId(params));
      case "raw_notifications":
        requiredUnsignedBigInt(params, "after_id");
        throw unsupported("PostgreSQL has no notification outbox");
      case "raw_replace_json_text":
        requiredId(params);
        requiredString(params, "column");
        throw unsupported(
          "PostgreSQL JSON columns can't hold text that isn't JSON"
        );
      case "raw_set_kind":
        return this.#rawSetKind(params);
      case "raw_job_exact_json":
        return exactJsonTokens(
          await this.#requiredJob({ id: requiredId(params) })
        );
      case "raw_job_row":
        return this.#rawJobRow(requiredId(params));
      case "raw_insert_exact_json":
        return this.#rawInsertExactJson(params);
      case "raw_insert_no_notify":
        return this.#rawInsertNoNotify(params);
      case "raw_insert_full_row":
        return this.#rawInsertFullRow();
      case "raw_finalize":
        return this.#rawFinalize(params);
      case "listener_count":
        return this.#connectionCount(true);
      case "connection_count":
        return this.#connectionCount(false);
      case "fault_disconnect_listeners":
        return this.#disconnectApplication(
          POSTGRES_CONFORMANCE_APPLICATION_NAME,
          true
        );
      case "fault_disconnect_application":
        return this.#disconnectApplication(
          requiredNonEmptyString(params, "application_name"),
          false
        );
      case "fault_expire_leader":
        await this.#pool.query(
          "UPDATE river_leader SET expires_at = now() - interval '1 second'"
        );
        return {};
      case "barrier_create":
        this.#barrierCreate(requiredNonEmptyString(params, "name"));
        return {};
      case "barrier_release":
        this.#barrierRelease(requiredNonEmptyString(params, "name"));
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
          await this.#client(
            optionalSchema(params)
          ).requestLeadershipResignation({
            tx: this.#transaction(requiredNonEmptyString(params, "handle")),
          });
        } else if (this.#running === null) {
          await this.#client(
            optionalSchema(params)
          ).requestLeadershipResignation();
        } else {
          await this.#running.handle.requestLeadershipResignation();
        }
        return {};
      }
    }
    throw new Error(`unimplemented PostgreSQL contract method: ${method}`);
  }

  async #benchmarkEnqueue(
    params: Record<string, unknown>
  ): Promise<{ duration_ns: bigint; p95_ns: bigint }> {
    const jobs = requiredInteger(params, "jobs", 1, 10_000_000);
    const client = this.#client();
    const latencies: bigint[] = [];
    const startedAt = process.hrtime.bigint();
    for (let index = 0; index < jobs; index++) {
      const insertedAt = process.hrtime.bigint();
      await client.insert(this.#definition("conformance_echo"), {
        behavior: "",
        duration_ms: 0,
        message: `benchmark-enqueue-${index}`,
      });
      latencies.push(process.hrtime.bigint() - insertedAt);
    }
    const duration = process.hrtime.bigint() - startedAt;
    latencies.sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));
    return {
      duration_ns: duration,
      p95_ns: latencies[Math.max(0, Math.ceil(jobs * 0.95) - 1)] ?? 0n,
    };
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

  #client(schema?: string): Client<PoolClient> {
    if (this.#running !== null) {
      if (this.#running.schema !== schema) {
        throw new Error(
          `running client schema ${JSON.stringify(this.#running.schema ?? "")} ` +
            `does not match requested schema ${JSON.stringify(schema ?? "")}`
        );
      }
      return this.#running.client;
    }
    return new Client(new PgDriver(this.#pool, schemaOption(schema)));
  }

  async #connectionCount(listenerOnly: boolean): Promise<{ count: number }> {
    const result = await retryTransientPoolOperation(() =>
      this.#pool.query<{ count: string }>(
        `SELECT count(*)::text AS count
         FROM pg_stat_activity
         WHERE application_name = $1::text
           AND datname = current_database()
           AND ($2::boolean = false OR query LIKE 'LISTEN %')`,
        [POSTGRES_CONFORMANCE_APPLICATION_NAME, listenerOnly]
      )
    );
    return { count: Number(result.rows[0]?.count ?? "0") };
  }

  #definition(kind: string): JobDefinition<JsonObject> {
    let definition = this.#definitions.get(kind);
    if (definition === undefined) {
      definition = defineJob({
        kind,
        kindAliases: conformanceKindAliases(kind),
      });
      this.#definitions.set(kind, definition);
    }
    return definition;
  }

  async #deleteFinalized(
    params: Record<string, unknown>
  ): Promise<{ readonly deleted: number }> {
    this.#pilotDatabase ??= new PilotDatabaseClient(
      new PgDriver(this.#pool)
    ).database;
    return {
      deleted: await this.#pilotDatabase.deleteFinalizedJobs(
        deleteFinalizedParams(params)
      ),
    };
  }

  async #deleteMany(
    params: Record<string, unknown>,
    transaction?: PoolClient
  ): Promise<{ jobs: readonly Record<string, unknown>[] }> {
    const client = this.#client(optionalSchema(params));
    const jobs = await client.jobs.deleteMany({
      ...(params.all === true ? { all: true as const } : {}),
      ids: optionalBigInts(params, "ids"),
      kinds: optionalStrings(params, "kinds"),
      limit: optionalInteger(params, "limit", 100, 1, 10_000),
      priorities: optionalIntegers(params, "priorities", 1, 4),
      queues: optionalStrings(params, "queues"),
      states: optionalStates(params, "states", []) ?? [],
      ...(transaction === undefined ? {} : { tx: transaction }),
    });
    return { jobs: jobs.map(normalizeJob) };
  }

  async #disconnectApplication(
    applicationName: string,
    listenerOnly: boolean
  ): Promise<{ count: number }> {
    if (
      !applicationName.startsWith("river-conformance-") ||
      Buffer.byteLength(applicationName) > 63
    ) {
      throw new Error("unsupported conformance application_name");
    }
    const result = await retryTransientPoolOperation(() =>
      this.#pool.query<{ count: string }>(
        `SELECT count(*)::text AS count FROM (
           SELECT pg_terminate_backend(pid)
           FROM pg_stat_activity
           WHERE application_name = $1::text
             AND datname = current_database()
             AND pid != pg_backend_pid()
             AND ($2::boolean = false OR query LIKE 'LISTEN %')
         ) AS terminated`,
        [applicationName, listenerOnly]
      )
    );
    return { count: Number(result.rows[0]?.count ?? "0") };
  }

  async #insert(
    parsed: ParsedInsert,
    transaction?: PoolClient
  ): Promise<JobRow> {
    const result = await this.#client(parsed.schema).insert(
      parsed.definition,
      parsed.args,
      {
        ...parsed.options,
        ...(transaction === undefined ? {} : { tx: transaction }),
      }
    );
    return result.job;
  }

  async #insertMany(
    params: Record<string, unknown>,
    transaction?: PoolClient
  ): Promise<unknown> {
    const value = params.jobs;
    if (!Array.isArray(value) || value.length === 0) {
      throw new Error("no jobs to insert");
    }
    const parsed = value.map((job, index) => {
      if (job === null || typeof job !== "object" || Array.isArray(job)) {
        throw invalidParams(`jobs[${index}] must be an object`);
      }
      return parseInsert(job as Record<string, unknown>);
    });
    const schemas = new Set(parsed.map(({ schema }) => schema ?? ""));
    if (schemas.size !== 1) throw new Error("batch jobs must use one schema");
    const items: InsertManyItem[] = parsed.map((job) => ({
      args: job.args,
      job: job.definition,
      options: job.options,
    }));
    const options = {
      ...(transaction === undefined ? {} : { tx: transaction }),
    };
    const client = this.#client(parsed[0]?.schema);
    const results = await client.insertMany(items, options);
    return {
      results: results.map((result) => ({
        job: normalizeJob(result.job),
        unique_skipped_as_duplicate: result.status === "duplicate",
      })),
    };
  }

  async #leader(): Promise<{
    elected_at: string | null;
    leader_id: string | null;
  }> {
    const result = await this.#pool.query<{
      elected_at: string;
      leader_id: string;
    }>(`SELECT elected_at::text, leader_id FROM river_leader
        WHERE name = 'default' AND expires_at >= now()`);
    const row = result.rows[0];
    return row === undefined
      ? { elected_at: null, leader_id: null }
      : {
          elected_at: postgresTimestampToInstant(row.elected_at).toString(),
          leader_id: row.leader_id,
        };
  }

  async #list(
    params: Record<string, unknown>,
    transaction?: PoolClient
  ): Promise<{
    cursor: string | null;
    jobs: readonly Record<string, unknown>[];
  }> {
    const parsed = parseListParams(params);
    const result = await this.#client(optionalSchema(params)).jobs.list({
      // The cursor as given; parsing it above rejects a malformed one.
      ...(parsed.after === null
        ? {}
        : { after: requiredNonEmptyString(params, "after") }),
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
    const rows = result.jobs;
    const last = rows.at(-1);
    return {
      cursor: last === undefined ? null : encodeJobListCursor(last, parsed),
      jobs: rows.map(normalizeJob),
    };
  }

  async #migrate(params: Record<string, unknown>): Promise<unknown> {
    if (this.#transactions.size > 0 || this.#running !== null) {
      throw new Error("migrate requires no running client or open transaction");
    }
    const schema = optionalSchema(params);
    if (schema !== undefined) {
      await this.#pool.query(
        `CREATE SCHEMA IF NOT EXISTS ${quoteValidIdentifier(schema)}`
      );
    }
    const direction = params.direction ?? "up";
    if (direction !== "up" && direction !== "down") {
      throw invalidParams(
        `unknown migration direction ${JSON.stringify(direction)}`
      );
    }
    const migrator = createMigrator({ pool: this.#pool, schema });
    const options: {
      dryRun?: boolean;
      maxSteps?: number;
      targetVersion?: number;
    } = {};
    if (params.dry_run !== undefined) {
      options.dryRun = requiredBoolean(params, "dry_run");
    }
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
        ? await migrator.migrateUp(options)
        : await migrator.migrateDown(options);
    return {
      applied: result.versions.map(({ version }) => version),
      existing: await migrator.existingVersions(),
      valid: (await migrator.validate()).ok,
    };
  }

  async #queueControl(
    method: "queue_pause" | "queue_resume",
    params: Record<string, unknown>,
    transaction?: PoolClient
  ): Promise<void> {
    const client = this.#client(optionalSchema(params));
    const name = requiredNonEmptyString(params, "name");
    const queue =
      method === "queue_pause"
        ? await client.queues.pause(name, txOptions(transaction))
        : await client.queues.resume(name, txOptions(transaction));
    if (name !== "*" && queue === null) {
      throw notFound(`queue ${JSON.stringify(name)} not found`);
    }
  }

  async #queueList(
    params: Record<string, unknown>,
    transaction?: PoolClient
  ): Promise<{ queues: readonly Record<string, unknown>[] }> {
    const result = await this.#client(optionalSchema(params)).queues.list({
      limit: optionalInteger(params, "limit", 100, 1, 10_000),
      ...(transaction === undefined ? {} : { tx: transaction }),
    });
    return { queues: result.queues.map(normalizeQueue) };
  }

  async #queueUpdate(
    params: Record<string, unknown>,
    transaction?: PoolClient
  ) {
    const queue = await this.#client(optionalSchema(params)).queues.update(
      requiredNonEmptyString(params, "name"),
      queueUpdateOptions(params),
      txOptions(transaction)
    );
    if (queue === null) {
      throw notFound(`queue ${JSON.stringify(params.name)} not found`);
    }
    return queue;
  }

  async #rawFinalize(params: Record<string, unknown>): Promise<unknown> {
    const id = requiredId(params);
    const state = requiredNonEmptyString(params, "state");
    if (state !== "completed" && state !== "discarded") {
      throw invalidParams("state must be completed or discarded");
    }
    const metadata = optionalRecord(params, "metadata") ?? {};
    const error = JSON.stringify({
      at: "2026-02-03T04:05:06.789Z",
      attempt: 1,
      error: "external discard",
      trace: "external trace",
    });
    const result = await this.#pool.query(
      `UPDATE river_job
       SET errors = CASE WHEN $2::text = 'discarded'
             THEN array_append(errors, $4::jsonb) ELSE errors END,
           finalized_at = now(),
           metadata = metadata || $3::jsonb,
           state = $2::text::river_job_state
       WHERE id = $1::bigint AND state = 'running'`,
      [id.toString(10), state, JSON.stringify(metadata), error]
    );
    if (result.rowCount !== 1) throw notFound("running job not found");
    return normalizeJob(await this.#requiredJob({ id }));
  }

  async #rawSetKind(params: Record<string, unknown>): Promise<unknown> {
    const id = requiredId(params);
    const kind = requiredNonEmptyString(params, "kind");
    const result = await this.#pool.query(
      "UPDATE river_job SET kind = $2 WHERE id = $1::bigint",
      [id.toString(10), kind]
    );
    if (result.rowCount !== 1) {
      throw notFound(`job ${id.toString(10)} not found`);
    }
    return normalizeJob(await this.#requiredJob({ id }));
  }

  async #rawInsertFullRow(): Promise<unknown> {
    const result = await this.#pool.query<{ id: string }>(`
      INSERT INTO river_job (
        args, attempt, attempted_at, attempted_by, created_at, errors,
        finalized_at, kind, max_attempts, metadata, priority, queue,
        scheduled_at, state, tags, unique_key, unique_states
      ) VALUES (
        '{"nested":{"enabled":true},"values":[1,"two",null]}'::jsonb,
        3, '2026-01-02T03:04:06.123456Z', ARRAY['go-client','candidate-client'],
        '2026-01-02T03:04:05.6789Z',
        ARRAY['{"at":"2026-01-02T03:04:06.123456Z","attempt":3,"error":"worker failed: escaped \\"detail\\"","trace":"frame one\\nframe two"}'::jsonb],
        '2026-01-02T03:04:07.000001Z', 'conformance_full_row', 4,
        '{"output":{"ok":true},"river:rescue_count":2,"user":"metadata"}'::jsonb,
        2, 'priority_jobs', '2026-01-02T03:04:05.999999Z', 'discarded',
        ARRAY['alpha_tag','beta_tag'], decode(repeat('ab', 32), 'hex'), B'11110101'
      ) RETURNING id::text
    `);
    const id = result.rows[0]?.id;
    if (id === undefined) throw new Error("full-row insert returned no ID");
    return normalizeJob(await this.#requiredJob({ id }));
  }

  async #rawInsertExactJson(
    params: Record<string, unknown>
  ): Promise<{ id: bigint }> {
    const requestedId =
      params.id === undefined ? null : requiredId(params).toString(10);
    const result = await this.#pool.query<{ id: string }>(
      `
      INSERT INTO river_job (id, args, kind, max_attempts, metadata)
      VALUES (
        COALESCE($1::bigint, nextval(pg_get_serial_sequence('river_job', 'id'))),
        '{"decimal":0.12345678901234567890123456789,"integer":9223372036854775807}'::jsonb,
        'conformance_exact_json', 25,
        $2::jsonb
      ) RETURNING id::text
    `,
      [
        requestedId,
        optionalRawJsonObject(
          params,
          "metadata_json",
          '{"negative":-9223372036854775808}'
        ),
      ]
    );
    const id = result.rows[0]?.id;
    if (id === undefined) throw new Error("exact JSON insert returned no ID");
    return { id: BigInt(id) };
  }

  async #rawInsertNoNotify(params: Record<string, unknown>): Promise<unknown> {
    // A raw row bypasses the client like Go's reference adapter, so its kind
    // is stored as given rather than validated as a job definition.
    const { kind, ...insert } = params;
    const parsed = parseInsert(insert);
    const result = await this.#pool.query<{ id: string }>(
      `INSERT INTO river_job (args, kind, max_attempts)
       VALUES ($1::jsonb, $2::text, $3::smallint)
       RETURNING id::text`,
      [
        JSON.stringify(parsed.args),
        optionalStringValue(kind, "kind", "conformance_echo"),
        parsed.options.maxAttempts ?? 25,
      ]
    );
    const id = result.rows[0]?.id;
    if (id === undefined) throw new Error("raw insert returned no ID");
    return normalizeJob(await this.#requiredJob({ id }));
  }

  async #rawJobTimestamps(
    id: bigint
  ): Promise<{ created_at: string; scheduled_at: string }> {
    const result = await this.#pool.query<{
      created_at: string;
      scheduled_at: string;
    }>(
      `SELECT created_at::text, scheduled_at::text
       FROM river_job WHERE id = $1::bigint`,
      [id.toString(10)]
    );
    const row = result.rows[0];
    if (row === undefined) throw notFound(`job ${id.toString(10)} not found`);
    return row;
  }

  async #rawJobRow(id: bigint): Promise<Record<string, string | null>> {
    const result = await this.#pool.query<Record<string, string | null>>(
      `SELECT args::text, attempted_at::text, attempted_by::text,
              created_at::text, errors::text, finalized_at::text,
              metadata::text, scheduled_at::text, tags::text,
              upper(encode(unique_key, 'hex')) AS unique_key,
              unique_states::text AS unique_states
       FROM river_job WHERE id = $1::bigint`,
      [id.toString(10)]
    );
    const row = result.rows[0];
    if (row === undefined) throw notFound(`job ${id.toString(10)} not found`);
    // PostgreSQL has no stored JSONB element or column storage types.
    return {
      ...row,
      jsonb: null,
      unique_key_type: null,
      unique_states_type: null,
    };
  }

  async #requiredJob(
    params: Record<string, unknown>,
    transaction?: PoolClient
  ): Promise<JobRow> {
    const id = requiredId(params);
    const job = await this.#client(optionalSchema(params)).jobs.get(
      id,
      txOptions(transaction)
    );
    if (job === null) throw notFound(`job ${id.toString(10)} not found`);
    return job;
  }

  async #requiredMutation(
    mutation: "cancel" | "delete" | "retry",
    params: Record<string, unknown>,
    transaction?: PoolClient
  ): Promise<JobRow> {
    const id = requiredId(params);
    const client = this.#client(optionalSchema(params));
    const options = txOptions(transaction);
    const job =
      mutation === "cancel"
        ? await client.jobs.cancel(id, options)
        : mutation === "delete"
          ? await client.jobs.delete(id, options)
          : await client.jobs.retry(id, options);
    if (job === null) throw notFound(`job ${id.toString(10)} not found`);
    return job;
  }

  async #requiredQueue(
    params: Record<string, unknown>,
    transaction?: PoolClient
  ) {
    const name = requiredNonEmptyString(params, "name");
    const queue = await this.#client(optionalSchema(params)).queues.get(
      name,
      txOptions(transaction)
    );
    if (queue === null)
      throw notFound(`queue ${JSON.stringify(name)} not found`);
    return queue;
  }

  async #reset(
    params: Record<string, unknown>
  ): Promise<Record<string, never>> {
    if (this.#transactions.size > 0 || this.#running !== null) {
      throw new Error("reset requires no running client or open transaction");
    }
    for (const barrier of this.#barriers.values()) barrier.release();
    this.#barriers.clear();
    const prefix = schemaPrefix(optionalSchema(params));
    await this.#pool.query(
      `TRUNCATE ${prefix}"river_job", ${prefix}"river_notification", ` +
        `${prefix}"river_queue", ${prefix}"river_leader" RESTART IDENTITY CASCADE`
    );
    return {};
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
    const lease = new AdapterPgLease(await this.#pool.connect());
    try {
      await lease.race(lease.client.query("BEGIN"));
      this.#transactions.set(handle, lease);
    } catch (error: unknown) {
      lease.destroy();
      throw error;
    }
  }

  async #transactionFinish(
    handle: string,
    action: "COMMIT" | "ROLLBACK"
  ): Promise<void> {
    const lease = this.#transactionLease(handle);
    this.#transactions.delete(handle);
    try {
      if (lease.failed) await lease.race(undefined);
      await lease.race(lease.client.query(action));
    } finally {
      lease.release();
    }
  }

  #transaction(handle: string): PoolClient {
    return this.#transactionLease(handle).client;
  }

  #transactionLease(handle: string): AdapterPgLease {
    const lease = this.#transactions.get(handle);
    if (lease === undefined) {
      throw notFound(`transaction ${JSON.stringify(handle)} not found`);
    }
    return lease;
  }

  async #update(
    params: Record<string, unknown>,
    transaction?: PoolClient
  ): Promise<JobRow> {
    const updates: {
      metadata?: JsonObject;
      output?: JsonValue;
    } = {};
    const metadata = optionalRecord(params, "metadata");
    if (metadata !== null) updates.metadata = metadata as JsonObject;
    if (Object.hasOwn(params, "output"))
      updates.output = params.output as JsonValue;
    const job = await this.#client(optionalSchema(params)).jobs.update(
      requiredId(params),
      updates,
      txOptions(transaction)
    );
    if (job === null) throw notFound(`job ${String(params.id)} not found`);
    return job;
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
        if (behavior === "barrier_output") {
          recordOutput({ race: "worker" });
        }
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
      case "ignored_cancel":
        await new Promise<never>(() => {});
        return;
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

  async #transactionalComplete(context: WorkContext): Promise<WorkOutcome> {
    const lease = new AdapterPgLease(await this.#pool.connect());
    const transaction = lease.client;
    try {
      await lease.race(transaction.query("BEGIN"));
      const updated = await context.client.jobs.update(
        context.job.id,
        { metadata: { transactional_completion: true } },
        { tx: transaction }
      );
      if (updated === null) {
        throw new Error(
          `transactional completion job ${context.job.id.toString(10)} not found`
        );
      }
      await lease.race(context.completeTx(transaction));
      await lease.race(transaction.query("COMMIT"));
    } catch (error: unknown) {
      if (!lease.failed) {
        try {
          await lease.race(transaction.query("ROLLBACK"));
        } catch {
          // Preserve the original transaction error.
        }
      }
      throw error;
    } finally {
      lease.release();
    }
    return complete();
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
    const name = requiredNonEmptyString(params, "name");
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
    const name = requiredNonEmptyString(params, "name");
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
    const schema = optionalSchema(params);
    const maxWorkers = optionalInteger(params, "max_workers", 4, 1, 10_000);
    const pollIntervalMs = optionalInteger(
      params,
      "fetch_poll_interval_ms",
      10,
      1,
      Number.MAX_SAFE_INTEGER
    );
    const definition = this.#definition("conformance_echo");
    const probe = new RuntimeProbe();
    const workers = new Workers();
    let executor: WorkerThreads | undefined;
    for (const kind of startWorkerKinds(params)) {
      const worker = this.#definition(kind);
      if (queue === "ignored") {
        // A job that ignores its cancellation runs in a worker thread, which
        // the client terminates `job_stuck_threshold_ms` after aborting it.
        executor ??= new WorkerThreads({ maxThreads: maxWorkers });
        workers.addExecutor(
          worker,
          executor.handler(worker, {
            exportName: "work",
            module: postgresWorkerModuleUrl(),
          })
        );
      } else {
        workers.add(worker, (context) => this.#handleWork(context, probe));
      }
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
    const options: ClientOptions<PoolClient> = {
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
      periodicJobs: startPeriodicJobs(params, definition),
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
    const driver = new PgDriver(this.#pool, schemaOption(schema));
    const client =
      claimBarrier === undefined
        ? new Client(driver, options)
        : new ClaimBarrierClient(driver, options, claimBarrier.promise);
    const abort = new AbortController();
    const subscription = client.subscribe({
      kinds: CONFORMANCE_EVENT_KINDS,
      signal: abort.signal,
    });
    const pump = (async () => {
      for await (const event of subscription) {
        probe.events.push(event.kind);
      }
    })();
    const handle = await client.start();
    this.#running = {
      abort,
      claimBarrier: claimBarrierName,
      client,
      executor,
      handle,
      probe,
      pump,
      schema,
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
      await running.executor?.close();
    }
    return {};
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
      const job = await this.#client(optionalSchema(params)).jobs.get(id);
      if (job === null) throw notFound(`job ${id.toString(10)} not found`);
      if (states.includes(job.state)) return job;
      if (Date.now() >= deadline) {
        throw new Error(`timed out waiting for job ${id.toString(10)}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }

  async #work(params: Record<string, unknown>): Promise<JobRow> {
    if (this.#running !== null)
      throw new Error("work requires no running client");
    await this.#start({
      client_id: optionalString(
        params,
        "client_id",
        "javascript-conformance-adapter"
      ),
      max_workers: 1,
      schema: params.schema,
    });
    try {
      return await this.#wait(params);
    } finally {
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- started while awaiting
      if (this.#running !== null) await this.#stop({});
    }
  }
}

function postgresWorkerModuleUrl(): URL {
  return import.meta.url.endsWith(".ts")
    ? new URL("../dist/pg-worker.js", import.meta.url)
    : new URL("./pg-worker.js", import.meta.url);
}

async function retryTransientPoolOperation<T>(
  operation: () => Promise<T>
): Promise<T> {
  const deadline = Date.now() + 5_000;
  let delayMs = 10;
  for (;;) {
    try {
      return await operation();
    } catch (error: unknown) {
      if (!isTransientPostgresError(error)) throw error;
      if (Date.now() >= deadline) throw error;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      delayMs = Math.min(delayMs * 2, 1_000);
    }
  }
}

function isTransientPostgresError(error: unknown): boolean {
  if (error === null || typeof error !== "object") return false;
  const code = (error as { readonly code?: unknown }).code;
  const message = (error as { readonly message?: unknown }).message;
  return (
    (typeof code === "string" &&
      (code.startsWith("08") ||
        code === "57P01" ||
        code === "57P02" ||
        code === "57P03" ||
        code === "ECONNRESET" ||
        code === "EPIPE")) ||
    (typeof message === "string" &&
      /connection (?:ended|terminated)(?: unexpectedly)?/i.test(message))
  );
}

/** Compute `unique_key` from the shared fixture request, like Go's goldens. */
export function uniqueKeyResult(params: Record<string, unknown>): unknown {
  const options = requiredRecord(params, "options");
  const kind = requiredNonEmptyString(params, "kind");
  if (!UNIQUE_FIXTURE_KINDS.has(kind)) {
    throw new Error(`unsupported unique fixture kind ${JSON.stringify(kind)}`);
  }
  const states = optionalStates(options, "by_state", undefined);
  return deterministicUniqueKey({
    args: params.args,
    kind,
    now: requiredNonEmptyString(params, "now"),
    options: {
      by_args: optionalBoolean(options, "by_args", false),
      by_period_nanos: optionalBigInt(options, "by_period_nanos", 0n),
      by_queue: optionalBoolean(options, "by_queue", false),
      ...(states === undefined ? {} : { by_state: states }),
      exclude_kind: optionalBoolean(options, "exclude_kind", false),
    },
    queue: requiredNonEmptyString(params, "queue"),
    scheduled_at:
      params.scheduled_at === null || params.scheduled_at === undefined
        ? null
        : requiredNonEmptyString(params, "scheduled_at"),
    selected_unique_components: selectedUniqueComponents(params),
    selected_unique_paths:
      kind === "conformance_selected_args"
        ? ["account.id", "account.region", "label", "path/key"]
        : kind === "conformance_dotted_selected_args"
          ? ["user.id", "user\\.id"]
          : null,
  });
}

function optionalSchema(params: Record<string, unknown>): string | undefined {
  const value = params.schema;
  if (value === undefined || value === "") return undefined;
  if (typeof value !== "string") throw new TypeError("schema must be a string");
  validateIdentifier(value, "schema");
  return value;
}

/** Decode an `insert` request into River insert options. */
export function parseInsert(params: Record<string, unknown>): ParsedInsert {
  const request = params as InsertRequest;
  const opts =
    request.opts === undefined ? {} : requireRecordValue(request.opts, "opts");
  const behavior = optionalStringValue(request.behavior, "behavior", "");
  const durationMs = optionalInteger(
    { duration_ms: request.duration_ms },
    "duration_ms",
    0,
    0,
    Number.MAX_SAFE_INTEGER
  );
  const message = optionalStringValue(request.message, "message", "");
  const kind = optionalStringValue(request.kind, "kind", "conformance_echo");
  const options: InsertOptions = {};
  if (opts.max_attempts !== undefined) {
    options.maxAttempts = requiredInteger(opts, "max_attempts", 1, 32_767);
  }
  const metadata = optionalRecord(opts, "metadata");
  if (metadata !== null) options.metadata = metadata as JsonObject;
  if (opts.pending !== undefined)
    options.pending = requiredBoolean(opts, "pending");
  if (opts.priority !== undefined) {
    // River validates the priority range, which the contract leaves open.
    options.priority = requiredInteger(
      opts,
      "priority",
      Number.MIN_SAFE_INTEGER,
      Number.MAX_SAFE_INTEGER
    );
  }
  if (opts.queue !== undefined)
    options.queue = requiredNonEmptyString(opts, "queue");
  if (opts.scheduled_at !== undefined && opts.scheduled_at !== null) {
    options.scheduledAt = Temporal.Instant.from(
      requiredNonEmptyString(opts, "scheduled_at")
    );
  }
  if (opts.tags !== undefined) options.tags = optionalStrings(opts, "tags");
  const unique = optionalRecord(opts, "unique");
  if (unique !== null) {
    const byPeriodMs = optionalInteger(
      unique,
      "by_period_ms",
      0,
      0,
      Number.MAX_SAFE_INTEGER
    );
    if (byPeriodMs % 1_000 !== 0) {
      throw new Error(
        "JavaScript public unique periods currently require whole seconds"
      );
    }
    const states = optionalStates(unique, "by_state", undefined);
    options.unique = {
      ...(optionalBoolean(unique, "by_args", false) ? { byArgs: true } : {}),
      ...(byPeriodMs === 0 ? {} : { byPeriod: { milliseconds: byPeriodMs } }),
      ...(optionalBoolean(unique, "by_queue", false) ? { byQueue: true } : {}),
      ...(states === undefined ? {} : { byState: states }),
      ...(optionalBoolean(unique, "exclude_kind", false)
        ? { excludeKind: true }
        : {}),
    };
  }
  return {
    args: { behavior, duration_ms: durationMs, message },
    definition: defineJob({ kind }),
    options,
    schema: optionalSchema(params),
  };
}

function parseListParams(params: Record<string, unknown>): JobListParams {
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
        : decodeJobListCursor(requiredNonEmptyString(params, "after")),
    ids: optionalBigInts(params, "ids"),
    kinds: optionalStrings(params, "kinds"),
    limit: optionalInteger(params, "limit", 100, 1, 10_000),
    metadata: (optionalRecord(params, "metadata") as JsonObject | null) ?? null,
    priorities: optionalIntegers(params, "priorities", 1, 4),
    queues: optionalStrings(params, "queues"),
    sortDirection: direction,
    sortField,
    states: optionalStates(params, "states", ALL_JOB_STATES) ?? ALL_JOB_STATES,
    tagsAll: optionalStrings(params, "tags_all"),
    tagsAny: optionalStrings(params, "tags_any"),
  };
}

function postgresTimestampToInstant(value: string): Temporal.Instant {
  return Temporal.Instant.from(value.replace(" ", "T"));
}

function quoteValidIdentifier(value: string): string {
  validateIdentifier(value, "identifier");
  return quoteIdentifier(value);
}

/** The driver `schema` option for an optional schema name. */
export function schemaOption(schema: string | undefined): { schema?: string } {
  return schema === undefined ? {} : { schema };
}

function schemaPrefix(schema: string | undefined): string {
  return schema === undefined ? "" : `${quoteValidIdentifier(schema)}.`;
}

function txOptions(transaction: PoolClient | undefined): { tx?: PoolClient } {
  return transaction === undefined ? {} : { tx: transaction };
}

function validateIdentifier(value: string, name: string): void {
  if (value.length === 0 || value.includes("\0")) {
    throw new Error(`${name} is not a valid PostgreSQL identifier`);
  }
  if (Buffer.byteLength(value, "utf8") > 63) {
    throw new Error(`${name} exceeds PostgreSQL's 63-byte limit`);
  }
}
