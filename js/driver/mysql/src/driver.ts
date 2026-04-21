import { Buffer } from "node:buffer";
import { randomBytes } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import type {
  Pool,
  PoolConnection,
  ResultSetHeader,
  RowDataPacket,
} from "mysql2/promise";
import {
  ConfigurationError,
  DatabaseOperationError,
  RiverError,
  TransactionScopeError,
  isExactJsonNumber,
} from "riverqueue";
import type { JobRow, JobState, JsonObject, JsonValue } from "riverqueue";
import {
  jobCompletionKey,
  jobListKeyset,
  jobListKeysetSql,
  queueMetadataUpdate,
  registerDriver,
  uniqueBitmaskFromStates,
} from "riverqueue/unstable-driver";
import type {
  DriverRecord,
  DriverInsertResult,
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
  RuntimeDriver,
  RuntimeJobCleanupParams,
  RuntimeJobRescue,
  RuntimeLeader,
  RuntimeMaintenanceBatch,
  RuntimeNotification,
  RuntimeScheduleParams,
  RuntimeWaitOptions,
} from "riverqueue/unstable-driver";
import {
  decodeJob,
  decodeJobPartial,
  decodeQueue,
  instant,
  JOB_COLUMNS,
  json,
  QUEUE_COLUMNS,
  timestamp,
  text,
} from "./codecs.js";
import type { Row } from "./codecs.js";

/** MySQL database containing River's tables; defaults to the pool's database. */
export interface MySqlDriverOptions {
  readonly schema?: string;
}
type Options = InsertDriverOptions<PoolConnection> & RuntimeWaitOptions;
const NONCE = "river:unique_nonce";

/**
 * MySQL 8.0.17+ backend using a caller-owned mysql2 pool. Pass an open
 * connection as `{ tx }` to join an application transaction. River never
 * closes the pool or commits a caller's transaction.
 */
export class MySqlDriver {
  declare readonly "~river"?: {
    readonly capability: "runtime";
    readonly transaction: PoolConnection;
  };
  constructor(pool: Pool, options: MySqlDriverOptions = {}) {
    const runtime = new MySqlRuntime(pool, options);
    registerDriver(this, runtime.driverRecord());
  }
}

/** @internal */
export class MySqlRuntime implements RuntimeDriver<PoolConnection> {
  readonly backend = "mysql";
  readonly #pool: Pool;
  readonly #prefix: string;
  readonly #schema: string | undefined;

  constructor(pool: Pool, options: MySqlDriverOptions = {}) {
    if (typeof pool.getConnection !== "function")
      throw new ConfigurationError(
        "MySqlDriver requires a mysql2 promise Pool"
      );
    if (
      options.schema !== undefined &&
      !/^[A-Za-z_][A-Za-z0-9_]{0,45}$/.test(options.schema)
    )
      throw new ConfigurationError("Invalid MySQL schema name");
    this.#pool = pool;
    this.#schema = options.schema;
    this.#prefix = options.schema === undefined ? "" : `\`${options.schema}\`.`;
  }

  driverRecord(): DriverRecord<PoolConnection> {
    return {
      backend: this.backend,
      capability: "runtime",
      operations: this,
      migration: { backend: "mysql", pool: this.#pool, schema: this.#schema },
    };
  }

  async operationScope<T>(
    tx: PoolConnection | undefined,
    callback: (tx: PoolConnection) => Promise<T>
  ): Promise<T> {
    return this.#transaction(tx === undefined ? {} : { tx }, callback);
  }

  async #transaction<T>(
    options: Options,
    callback: (tx: PoolConnection) => Promise<T>
  ): Promise<T> {
    options.signal?.throwIfAborted();
    if (options.tx !== undefined) {
      if (
        typeof options.tx.execute !== "function" ||
        typeof options.tx.release !== "function"
      )
        throw new ConfigurationError(
          "Expected a mysql2 pool connection transaction"
        );
      const [status] = await options.tx.query<ResultSetHeader>("DO 0");
      if ((status.serverStatus & 1) === 0)
        throw new TransactionScopeError(
          "no_transaction",
          "MySQL connection has no open transaction"
        );
      return callback(options.tx);
    }
    const acquiring = this.#pool.getConnection();
    let tx: PoolConnection;
    if (options.signal) {
      const signal = options.signal;
      let abort!: () => void;
      try {
        tx = await Promise.race([
          acquiring,
          new Promise<never>((_, reject) => {
            abort = () => reject(signal.reason);
            signal.addEventListener("abort", abort, { once: true });
          }),
        ]);
      } catch (error) {
        void acquiring.then(
          (late) => late.release(),
          () => undefined
        );
        throw error;
      } finally {
        signal.removeEventListener("abort", abort);
      }
    } else tx = await acquiring;
    let abort: (() => void) | undefined;
    try {
      options.signal?.throwIfAborted();
      const operation = async () => {
        await tx.query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
        await tx.beginTransaction();
        try {
          const result = await callback(tx);
          await tx.commit();
          return result;
        } catch (error) {
          try {
            await tx.rollback();
          } catch {
            tx.destroy();
          }
          throw error;
        }
      };
      if (!options.signal) return await operation();
      const signal = options.signal;
      return await Promise.race([
        new Promise<never>((_, reject) => {
          abort = () => {
            // A pending locking read must not delay shutdown or return to
            // the pool. Only River-owned connections are cancelled this way.
            tx.destroy();
            reject(signal.reason);
          };
          signal.addEventListener("abort", abort, { once: true });
        }),
        operation(),
      ]);
    } catch (cause) {
      options.signal?.throwIfAborted();
      if (cause instanceof RiverError) throw cause;
      const code = (cause as { code?: string }).code ?? "";
      throw new DatabaseOperationError("MySQL operation failed", {
        backend: "mysql",
        operation: "transaction",
        cause,
        retryable:
          /DEADLOCK|LOCK_WAIT_TIMEOUT|ECONN|EPIPE|ETIMEDOUT|PROTOCOL_CONNECTION_LOST/.test(
            code
          ),
      });
    } finally {
      if (abort) options.signal?.removeEventListener("abort", abort);
      tx.release();
    }
  }

  async #rows(
    tx: PoolConnection,
    sql: string,
    values: readonly unknown[] = []
  ): Promise<Row[]> {
    const [rows] = await tx.execute<RowDataPacket[]>(
      sql.replaceAll("{schema}", this.#prefix),
      values.map(binding)
    );
    return rows;
  }
  async #write(
    tx: PoolConnection,
    sql: string,
    values: readonly unknown[] = []
  ): Promise<number> {
    const [result] = await tx.execute<ResultSetHeader>(
      sql.replaceAll("{schema}", this.#prefix),
      values.map(binding)
    );
    return result.affectedRows;
  }
  async #get(
    tx: PoolConnection,
    id: bigint,
    lock = false
  ): Promise<Row | null> {
    return (
      (
        await this.#rows(
          tx,
          `SELECT ${JOB_COLUMNS} FROM {schema}river_job WHERE id=?${lock ? " FOR UPDATE" : ""}`,
          [id]
        )
      )[0] ?? null
    );
  }

  async jobInsert(
    params: JobInsertParams,
    options: Options = {}
  ): Promise<DriverInsertResult> {
    return this.#transaction(options, async (tx) => {
      const metadata = {
        ...params.metadata,
        [NONCE]: randomBytes(8).toString("hex"),
      };
      await this.#write(
        tx,
        `INSERT INTO {schema}river_job (args,created_at,kind,max_attempts,metadata,priority,queue,scheduled_at,state,tags,unique_key,unique_states)
        VALUES (CAST(? AS JSON),COALESCE(?,UTC_TIMESTAMP(6)),?,?,CAST(? AS JSON),?,?,COALESCE(?,UTC_TIMESTAMP(6)),?,CAST(? AS JSON),?,?)
        ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)`,
        [
          params.encodedArgs,
          timestamp(params.createdAt),
          params.kind,
          Math.min(params.maxAttempts, 2_147_483_647),
          json(metadata),
          params.priority,
          params.queue,
          timestamp(params.scheduledAt),
          params.state,
          json(params.tags),
          params.uniqueKey === null ? null : Buffer.from(params.uniqueKey),
          params.uniqueStates === null
            ? null
            : Number.parseInt(uniqueBitmaskFromStates(params.uniqueStates), 2),
        ]
      );
      const row = (
        await this.#rows(
          tx,
          `SELECT ${JOB_COLUMNS} FROM {schema}river_job WHERE id=LAST_INSERT_ID() FOR UPDATE`
        )
      )[0];
      if (!row) throw new Error("MySQL insert returned no row");
      const job = decodeJob(row);
      const duplicate = job.metadata[NONCE] !== metadata[NONCE];
      if (
        duplicate &&
        (params.uniqueKey === null ||
          job.uniqueKey === null ||
          !Buffer.from(params.uniqueKey).equals(job.uniqueKey) ||
          !job.uniqueStates?.includes(job.state))
      )
        throw new Error("Insertion violated an unrelated unique constraint");
      return { job, status: duplicate ? "duplicate" : "inserted" };
    });
  }
  async jobInsertMany(
    params: readonly JobInsertParams[],
    options: Options = {}
  ): Promise<readonly DriverInsertResult[]> {
    return this.#transaction(options, async (tx) => {
      const keys = new Set<string>();
      const results = [];
      for (const item of params) {
        if (item.uniqueKey !== null) {
          const key = Buffer.from(item.uniqueKey).toString("hex");
          if (keys.has(key))
            throw new ConfigurationError("Batch repeats a unique key");
          keys.add(key);
        }
        results.push(await this.jobInsert(item, { tx }));
      }
      return results;
    });
  }
  async jobGet(id: bigint, options: Options = {}): Promise<JobRow | null> {
    return this.#transaction(options, async (tx) => {
      const row = await this.#get(tx, id);
      return row ? decodeJob(row) : null;
    });
  }
  async jobCancel(id: bigint, options: Options = {}): Promise<JobRow | null> {
    return this.#transaction(options, async (tx) => {
      const row = await this.#get(tx, id, true);
      if (!row) return null;
      const job = decodeJobPartial(row).job;
      if (
        ["cancelled", "completed", "discarded"].includes(job.state) ||
        job.finalizedAt !== null
      )
        return job;
      const now = Temporal.Now.instant();
      await this.#write(
        tx,
        `UPDATE {schema}river_job SET finalized_at=CASE WHEN state='running' THEN finalized_at ELSE ? END,
        metadata=JSON_SET(metadata,'$.cancel_attempted_at',?), state=CASE WHEN state='running' THEN state ELSE 'cancelled' END WHERE id=?`,
        [timestamp(now), now.toString(), id]
      );
      if (job.state === "running")
        await this.#notify(
          tx,
          "control",
          `{"action":"cancel","job_id":${id},"queue":${JSON.stringify(job.queue)}}`
        );
      return decodeJobPartial(required(await this.#get(tx, id, true))).job;
    });
  }
  async jobDelete(id: bigint, options: Options = {}): Promise<JobDeleteResult> {
    return this.#transaction(options, async (tx) => {
      const row = await this.#get(tx, id, true);
      if (!row) return { status: "not_found" };
      const job = decodeJobPartial(row).job;
      if (job.state === "running") return { job, status: "running" };
      await this.#write(tx, "DELETE FROM {schema}river_job WHERE id=?", [id]);
      return { job, status: "deleted" };
    });
  }
  async jobDeleteMany(
    params: JobDeleteManyParams,
    options: Options = {}
  ): Promise<readonly JobRow[]> {
    return this.#transaction(options, async (tx) => {
      const { where, values } = filters(params);
      if (!params.all && where.length === 0)
        throw new ConfigurationError("Deleting all jobs requires all=true");
      where.push("state != 'running'");
      const rows = await this.#rows(
        tx,
        `SELECT ${JOB_COLUMNS} FROM {schema}river_job WHERE ${where.join(" AND ")} ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED`,
        [...values, params.limit]
      );
      for (const row of rows)
        await this.#write(tx, "DELETE FROM {schema}river_job WHERE id=?", [
          row.id,
        ]);
      return rows.map((row) => decodeJobPartial(row).job);
    });
  }
  async jobList(
    params: JobListParams,
    options: Options = {}
  ): Promise<readonly JobRow[]> {
    return this.#transaction(options, async (tx) => {
      const { where, values } = filters(params);
      if (params.metadata !== null) {
        where.push(metadataContains(params.metadata, "metadata", values));
      }
      if (params.tagsAll.length) {
        where.push("JSON_CONTAINS(tags,CAST(? AS JSON))");
        values.push(json(params.tagsAll));
      }
      if (params.tagsAny.length) {
        where.push("JSON_OVERLAPS(tags,CAST(? AS JSON))");
        values.push(json(params.tagsAny));
      }
      const keyset = jobListKeyset(params);
      const order = jobListKeysetSql(keyset, (value) => {
        values.push(typeof value === "bigint" ? value : timestamp(value));
        return "?";
      });
      if (order.after) where.push(order.after);
      const orderBy = order.orderBy.replace(
        /(\w+) (ASC|DESC) NULLS (LAST|FIRST)/g,
        (_, field: string, direction: string, nulls: string) =>
          `${field} IS NULL ${nulls === "LAST" ? "ASC" : "DESC"}, ${field} ${direction}`
      );
      return (
        await this.#rows(
          tx,
          `SELECT ${JOB_COLUMNS} FROM {schema}river_job WHERE ${where.join(" AND ") || "true"} ORDER BY ${orderBy} LIMIT ?`,
          [...values, params.limit]
        )
      ).map(decodeJob);
    });
  }
  async jobRetry(id: bigint, options: Options = {}): Promise<JobRow | null> {
    return this.#transaction(options, async (tx) => {
      const row = await this.#get(tx, id, true);
      if (!row) return null;
      const job = decodeJobPartial(row).job;
      const now = Temporal.Now.instant();
      if (
        job.state === "running" ||
        (job.state === "available" &&
          Temporal.Instant.compare(job.scheduledAt, now) < 0)
      )
        return job;
      await this.#write(
        tx,
        `UPDATE {schema}river_job SET finalized_at=NULL, max_attempts=CASE WHEN attempt=max_attempts THEN max_attempts+1 ELSE max_attempts END, scheduled_at=?, state='available' WHERE id=?`,
        [timestamp(now), id]
      );
      await this.notifyInsert([job.queue], { tx });
      return decodeJobPartial(required(await this.#get(tx, id, true))).job;
    });
  }
  async #metadata(
    tx: PoolConnection,
    id: bigint,
    metadata: JsonObject
  ): Promise<void> {
    const entries = Object.entries(metadata);
    if (!entries.length) return;
    const values = entries.flatMap(([key, value]) => [
      `$.${JSON.stringify(key)}`,
      json(value),
    ]);
    await this.#write(
      tx,
      `UPDATE {schema}river_job SET metadata=JSON_SET(metadata,${entries.map(() => "?,CAST(? AS JSON)").join(",")}) WHERE id=?`,
      [...values, id]
    );
  }
  async jobUpdate(
    id: bigint,
    params: JobUpdateParams,
    options: Options = {}
  ): Promise<JobRow | null> {
    return this.#transaction(options, async (tx) => {
      if (!(await this.#get(tx, id, true))) return null;
      await this.#metadata(tx, id, {
        ...params.metadata,
        ...(params.output === undefined ? {} : { output: params.output }),
      });
      return decodeJobPartial(required(await this.#get(tx, id, true))).job;
    });
  }
  async jobClaim(
    params: JobClaimParams,
    options: JobClaimOptions<PoolConnection> = {}
  ): Promise<JobClaimResult> {
    return this.#transaction(options, async (tx) => {
      const jobs: JobRow[] = [];
      const decodeErrors = new Map<bigint, Error>();
      const now = timestamp(Temporal.Now.instant());
      for (const queue of params.queues) {
        if (!queue.limit) continue;
        const kindSql = params.kinds.length
          ? ` AND CAST(kind AS BINARY) IN (${params.kinds.map(() => "?").join(",")})`
          : "";
        const rows = await this.#rows(
          tx,
          `SELECT CAST(id AS CHAR) AS id FROM {schema}river_job WHERE queue=CAST(? AS BINARY) AND state='available' AND scheduled_at<=?
          AND NOT EXISTS(SELECT 1 FROM {schema}river_queue q WHERE q.name=CAST(river_job.queue AS BINARY) AND paused_at IS NOT NULL)${kindSql}
          ORDER BY priority,scheduled_at,id LIMIT ? FOR UPDATE SKIP LOCKED`,
          [queue.name, now, ...params.kinds, queue.limit]
        );
        for (const row of rows) {
          await this.#write(
            tx,
            `UPDATE {schema}river_job SET attempted_at=?, attempted_by=JSON_ARRAY_APPEND(CASE WHEN JSON_TYPE(attempted_by) != 'ARRAY' OR attempted_by IS NULL THEN JSON_ARRAY() WHEN JSON_LENGTH(attempted_by)>=100 THEN JSON_EXTRACT(attempted_by,'$[last-98 to last]') ELSE attempted_by END,'$',?), attempt=attempt+1,state='running' WHERE id=?`,
            [now, params.attemptedBy, row.id]
          );
          const result = decodeJobPartial(
            required(await this.#get(tx, BigInt(String(row.id)), true))
          );
          jobs.push(result.job);
          if (result.error) decodeErrors.set(result.job.id, result.error);
        }
      }
      return { jobs, decodeErrors };
    });
  }
  async jobCompleteMany(
    commands: readonly JobCompletionCommand[],
    options: Options = {}
  ): Promise<readonly JobCompletionResult[]> {
    return this.#transaction(options, async (tx) => {
      const results: JobCompletionResult[] = [];
      for (const command of commands) {
        const raw = await this.#get(tx, command.id, true);
        const job = raw ? decodeJobPartial(raw).job : null;
        const key = jobCompletionKey(command);
        if (
          !job ||
          job.attempt !== command.attempt ||
          job.attemptedBy.at(-1) !== command.attemptedBy
        ) {
          results.push({ job, key, status: "stale" });
          continue;
        }
        let state: JobState =
          command.kind === "complete"
            ? "completed"
            : command.kind === "cancel"
              ? "cancelled"
              : command.kind === "discard"
                ? "discarded"
                : command.kind === "interrupt" || command.available
                  ? "available"
                  : command.kind === "retry"
                    ? "retryable"
                    : "scheduled";
        const cancelled =
          ["available", "retryable", "scheduled"].includes(state) &&
          Object.hasOwn(job.metadata, "cancel_attempted_at");
        let finalizedAt = command.finalizedAt;
        let scheduledAt = command.scheduledAt ?? job.scheduledAt;
        let attempt =
          command.kind === "snooze" || command.kind === "interrupt"
            ? Math.max(0, command.attempt - 1)
            : command.attempt;
        if (cancelled) {
          state = "cancelled";
          finalizedAt = Temporal.Now.instant();
          scheduledAt = job.scheduledAt;
          attempt = job.attempt;
        }
        if (job.state === "running") {
          if (
            ["cancelled", "completed", "discarded"].includes(state) !==
            (finalizedAt !== null)
          )
            throw new ConfigurationError("Invalid completion timestamp");
          await this.#write(
            tx,
            `UPDATE {schema}river_job SET attempt=?,finalized_at=?,scheduled_at=?,state=?,
            errors=CASE WHEN ? THEN JSON_ARRAY_APPEND(CASE WHEN errors IS NULL THEN JSON_ARRAY() WHEN JSON_TYPE(errors)='ARRAY' THEN errors ELSE JSON_ARRAY(errors) END,'$',CAST(? AS JSON)) ELSE errors END WHERE id=?`,
            [
              attempt,
              timestamp(finalizedAt),
              timestamp(scheduledAt),
              state,
              command.error !== null,
              command.error === null
                ? "null"
                : json({
                    ...command.error,
                    at: command.error.at.toString(),
                    attempt: command.attempt,
                  }),
              command.id,
            ]
          );
        }
        await this.#metadata(tx, command.id, {
          ...command.metadata,
          ...(command.outputSet ? { output: command.output } : {}),
        });
        results.push({
          job: decodeJobPartial(required(await this.#get(tx, command.id, true)))
            .job,
          key,
          status: job.state === "running" ? "applied" : "stale",
        });
      }
      return results;
    });
  }

  async #notify(
    tx: PoolConnection,
    topic: RuntimeNotification["topic"],
    payload: string
  ): Promise<void> {
    await this.#write(
      tx,
      "INSERT INTO {schema}river_notification(topic,payload) VALUES (?,?)",
      [`river_${topic}`, payload]
    );
  }
  async notifyInsert(
    queues: readonly string[],
    options: Options = {}
  ): Promise<void> {
    await this.#transaction(options, async (tx) => {
      for (const queue of new Set(queues))
        await this.#notify(tx, "insert", json({ queue }));
    });
  }
  async jobGetCancelRequested(
    ids: readonly bigint[],
    options: RuntimeWaitOptions = {}
  ): Promise<readonly bigint[]> {
    if (!ids.length) return [];
    return this.#transaction(options, async (tx) =>
      (
        await this.#rows(
          tx,
          `SELECT CAST(id AS CHAR) AS id FROM {schema}river_job WHERE id IN (${ids.map(() => "?").join(",")}) AND state='running' AND JSON_CONTAINS_PATH(metadata,'one','$.cancel_attempted_at')`,
          ids
        )
      ).map((row) => BigInt(String(row.id)))
    );
  }
  async runtimeRequestLeadershipResignation(
    options: Options = {}
  ): Promise<void> {
    await this.#transaction(options, (tx) =>
      this.#notify(
        tx,
        "leadership",
        json({ action: "request_resign", leader_id: "" })
      )
    );
  }
  async *runtimeNotificationSubscribe(
    topics: readonly RuntimeNotification["topic"][],
    signal: AbortSignal,
    ready: () => void
  ): AsyncIterable<RuntimeNotification> {
    try {
      let after = await this.#transaction({ signal }, async (tx) => {
        const rows = await this.#rows(
          tx,
          "SELECT CAST(id AS CHAR) AS id FROM {schema}river_notification ORDER BY id FOR UPDATE"
        );
        return BigInt(text(rows.at(-1)?.id ?? "0"));
      });
      ready();
      while (!signal.aborted) {
        const rows = await this.#transaction({ signal }, (tx) =>
          this.#rows(
            tx,
            "SELECT CAST(id AS CHAR) AS id,topic,payload FROM {schema}river_notification WHERE id>? ORDER BY id LIMIT 256 FOR SHARE",
            [after]
          )
        );
        for (const row of rows) {
          after = BigInt(String(row.id));
          const topic = String(row.topic).replace(
            /^river_/,
            ""
          ) as RuntimeNotification["topic"];
          if (topics.includes(topic))
            yield { topic, payload: String(row.payload) };
        }
        try {
          await sleep(25, undefined, { signal });
        } catch (error) {
          if (!(error instanceof Error && error.name === "AbortError"))
            throw error;
        }
      }
    } catch (error) {
      if (!signal.aborted) throw error;
    }
  }

  async #queue(
    tx: PoolConnection,
    name: string,
    lock = false
  ): Promise<QueueRow | null> {
    const row = (
      await this.#rows(
        tx,
        `SELECT ${QUEUE_COLUMNS} FROM {schema}river_queue WHERE name=CAST(? AS BINARY)${lock ? " FOR UPDATE" : ""}`,
        [name]
      )
    )[0];
    return row ? decodeQueue(row) : null;
  }
  async queueGet(
    name: string,
    options: Options = {}
  ): Promise<QueueRow | null> {
    return this.#transaction(options, (tx) => this.#queue(tx, name));
  }
  async queueList(
    params: QueueListParams,
    options: Options = {}
  ): Promise<readonly QueueRow[]> {
    return this.#transaction(options, async (tx) =>
      (
        await this.#rows(
          tx,
          `SELECT ${QUEUE_COLUMNS} FROM {schema}river_queue WHERE (? IS NULL OR name>?) ORDER BY name LIMIT ?`,
          [params.nameAfter, params.nameAfter, params.limit]
        )
      ).map(decodeQueue)
    );
  }
  async queuePause(
    name: string,
    options: Options = {}
  ): Promise<QueueRow | null> {
    return this.#setPaused(name, true, options);
  }
  async queueResume(
    name: string,
    options: Options = {}
  ): Promise<QueueRow | null> {
    return this.#setPaused(name, false, options);
  }
  async #setPaused(
    name: string,
    paused: boolean,
    options: Options
  ): Promise<QueueRow | null> {
    return this.#transaction(options, async (tx) => {
      const condition = paused ? "paused_at IS NULL" : "paused_at IS NOT NULL";
      await this.#write(
        tx,
        `UPDATE {schema}river_queue SET updated_at=CASE WHEN ${condition} THEN UTC_TIMESTAMP(6) ELSE updated_at END, paused_at=${paused ? "COALESCE(paused_at,UTC_TIMESTAMP(6))" : "NULL"} WHERE (?='*' OR name=CAST(? AS BINARY))`,
        [name, name]
      );
      await this.#notify(
        tx,
        "control",
        json({ action: paused ? "pause" : "resume", queue: name })
      );
      return this.#queue(tx, name);
    });
  }
  async queueUpdate(
    name: string,
    params: QueueUpdateParams,
    options: Options = {}
  ): Promise<QueueRow | null> {
    return this.#transaction(options, async (tx) => {
      const before = await this.#queue(tx, name, true);
      if (!before) return null;
      const updated = queueMetadataUpdate(name, params);
      if (updated) {
        await this.#write(
          tx,
          "UPDATE {schema}river_queue SET metadata=CAST(? AS JSON),updated_at=UTC_TIMESTAMP(6) WHERE name=CAST(? AS BINARY)",
          [updated.text, name]
        );
        await this.#notify(tx, "control", updated.notification);
      }
      return this.#queue(tx, name);
    });
  }
  async runtimeQueueUpsert(
    name: string,
    now: Temporal.Instant,
    options: RuntimeWaitOptions = {}
  ): Promise<QueueRow> {
    return this.#transaction(options, async (tx) => {
      await this.#write(
        tx,
        "INSERT INTO {schema}river_queue(name,created_at,updated_at) VALUES (?,?,?) ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at)",
        [name, timestamp(now), timestamp(now)]
      );
      return required(await this.#queue(tx, name, true));
    });
  }

  async maintenanceLeaderAcquire(
    leaderId: string,
    now: Temporal.Instant,
    ttlMs: number,
    held: RuntimeLeader | null,
    options: RuntimeWaitOptions = {}
  ): Promise<RuntimeLeader | null> {
    return this.#transaction(options, async (tx) => {
      const expiresAt = now.add({ milliseconds: ttlMs });
      if (held) {
        const updated = await this.#write(
          tx,
          "UPDATE {schema}river_leader SET expires_at=? WHERE name='default' AND leader_id=CAST(? AS BINARY) AND elected_at=? AND expires_at>?",
          [
            timestamp(expiresAt),
            held.leaderId,
            timestamp(held.electedAt),
            timestamp(now),
          ]
        );
        return updated ? { ...held, expiresAt } : null;
      }
      await this.#write(
        tx,
        "DELETE FROM {schema}river_leader WHERE name='default' AND expires_at<?",
        [timestamp(now)]
      );
      await this.#write(
        tx,
        "INSERT INTO {schema}river_leader(name,leader_id,elected_at,expires_at) VALUES ('default',?,?,?) ON DUPLICATE KEY UPDATE name=name",
        [leaderId, timestamp(now), timestamp(expiresAt)]
      );
      const rows = await this.#rows(
        tx,
        "SELECT leader_id,CAST(elected_at AS CHAR) AS elected_at,CAST(expires_at AS CHAR) AS expires_at FROM {schema}river_leader WHERE name='default' AND leader_id=CAST(? AS BINARY) AND elected_at=? FOR UPDATE",
        [leaderId, timestamp(now)]
      );
      return rows.length
        ? {
            leaderId,
            electedAt: instant(required(rows[0]).elected_at),
            expiresAt: instant(required(rows[0]).expires_at),
          }
        : null;
    });
  }
  async maintenanceLeaderResign(leader: RuntimeLeader): Promise<boolean> {
    return this.#transaction({}, async (tx) => {
      const changed = await this.#write(
        tx,
        "DELETE FROM {schema}river_leader WHERE name='default' AND leader_id=CAST(? AS BINARY) AND elected_at=?",
        [leader.leaderId, timestamp(leader.electedAt)]
      );
      if (changed)
        await this.#notify(
          tx,
          "leadership",
          json({ action: "resigned", leader_id: leader.leaderId })
        );
      return changed > 0;
    });
  }
  async #fenced<T>(
    leader: RuntimeLeader,
    options: Options,
    callback: (tx: PoolConnection) => Promise<T>
  ): Promise<T | null> {
    return this.#transaction(options, async (tx) => {
      const rows = await this.#rows(
        tx,
        "SELECT leader_id FROM {schema}river_leader WHERE name='default' AND leader_id=CAST(? AS BINARY) AND elected_at=? AND expires_at>UTC_TIMESTAMP(6) FOR SHARE",
        [leader.leaderId, timestamp(leader.electedAt)]
      );
      if (!rows.length) return null;
      return callback(tx);
    });
  }
  async maintenanceTransaction<T>(
    leader: RuntimeLeader,
    batch: RuntimeMaintenanceBatch,
    callback: (tx: PoolConnection) => Promise<T>
  ): Promise<T | null> {
    return this.#fenced(leader, { signal: batch.signal }, async (tx) => {
      const result = await callback(tx);
      batch.signal.throwIfAborted();
      return result;
    });
  }
  async maintenanceSchedule(
    leader: RuntimeLeader,
    params: RuntimeScheduleParams,
    batch?: RuntimeMaintenanceBatch
  ): Promise<number> {
    return (
      (await this.#fenced(
        leader,
        batch ? { signal: batch.signal } : {},
        async (tx) => {
          const rows = await this.#rows(
            tx,
            `SELECT ${JOB_COLUMNS} FROM {schema}river_job WHERE state IN ('retryable','scheduled') AND scheduled_at<=? ORDER BY priority,scheduled_at,id LIMIT ? FOR UPDATE SKIP LOCKED`,
            [timestamp(params.scheduledAtHorizon), params.limit]
          );
          const queues: string[] = [];
          for (const row of rows) {
            const job = decodeJobPartial(row).job;
            // A savepoint contains only the conflicting state change, leaving the
            // batch usable after MySQL's duplicate-key error.
            await tx.query("SAVEPOINT river_schedule");
            try {
              await this.#write(
                tx,
                "UPDATE {schema}river_job SET state='available' WHERE id=?",
                [job.id]
              );
              if (
                Temporal.Instant.compare(
                  job.scheduledAt,
                  params.notificationHorizon
                ) <= 0
              )
                queues.push(job.queue);
            } catch (error) {
              if ((error as { code?: string }).code !== "ER_DUP_ENTRY")
                throw error;
              await tx.query("ROLLBACK TO SAVEPOINT river_schedule");
              await this.#write(
                tx,
                "UPDATE {schema}river_job SET state='discarded',finalized_at=?,metadata=JSON_SET(metadata,'$.unique_key_conflict','scheduler_discarded') WHERE id=?",
                [timestamp(params.now), job.id]
              );
            } finally {
              await tx.query("RELEASE SAVEPOINT river_schedule");
            }
          }
          await this.notifyInsert(params.allowInsertNotifications(queues), {
            tx,
          });
          return rows.length;
        }
      )) ?? 0
    );
  }
  async maintenanceGetStuck(
    leader: RuntimeLeader,
    attemptedBefore: Temporal.Instant,
    afterId: bigint,
    limit: number,
    batch?: RuntimeMaintenanceBatch
  ): Promise<readonly JobRow[]> {
    return (
      (await this.#fenced(
        leader,
        batch ? { signal: batch.signal } : {},
        async (tx) =>
          (
            await this.#rows(
              tx,
              `SELECT ${JOB_COLUMNS} FROM {schema}river_job WHERE state='running' AND attempted_at<? AND id>? ORDER BY id LIMIT ?`,
              [timestamp(attemptedBefore), afterId, limit]
            )
          ).map((row) => decodeJobPartial(row).job)
      )) ?? []
    );
  }
  async maintenanceRescue(
    leader: RuntimeLeader,
    attemptedBefore: Temporal.Instant,
    jobs: readonly RuntimeJobRescue[],
    options: Options = {}
  ): Promise<number> {
    return (
      (await this.#fenced(leader, options, async (tx) => {
        let count = 0;
        for (const job of jobs) {
          const raw = await this.#get(tx, job.id, true);
          if (!raw) continue;
          const before = decodeJobPartial(raw).job;
          if (
            before.state !== "running" ||
            before.attemptedAt === null ||
            Temporal.Instant.compare(before.attemptedAt, attemptedBefore) >= 0
          )
            continue;
          count += await this.#write(
            tx,
            `UPDATE {schema}river_job SET finalized_at=?,scheduled_at=?,state=?,metadata=JSON_SET(metadata,'$."river:rescue_count"',COALESCE(CASE WHEN JSON_TYPE(JSON_EXTRACT(metadata,'$."river:rescue_count"')) IN ('INTEGER','DOUBLE') THEN JSON_EXTRACT(metadata,'$."river:rescue_count"') ELSE NULL END,0)+1),errors=JSON_ARRAY_APPEND(COALESCE(errors,JSON_ARRAY()),'$',CAST(? AS JSON)) WHERE id=?`,
            [
              timestamp(job.finalizedAt),
              timestamp(job.scheduledAt),
              job.state,
              json({ ...job.error, at: job.error.at.toString() }),
              job.id,
            ]
          );
        }
        return count;
      })) ?? 0
    );
  }
  async maintenanceCleanJobs(
    leader: RuntimeLeader,
    params: RuntimeJobCleanupParams,
    _timeoutMs: number | null,
    signal: AbortSignal
  ): Promise<number> {
    return (
      (await this.#fenced(leader, { signal }, (tx) =>
        this.#write(
          tx,
          `DELETE FROM {schema}river_job WHERE ((state='cancelled' AND finalized_at<?) OR (state='completed' AND finalized_at<?) OR (state='discarded' AND finalized_at<?)) ${params.queuesExcluded?.length ? `AND CAST(queue AS BINARY) NOT IN (${params.queuesExcluded.map(() => "?").join(",")})` : ""} ORDER BY id LIMIT ?`,
          [
            timestamp(params.cancelledBefore),
            timestamp(params.completedBefore),
            timestamp(params.discardedBefore),
            ...(params.queuesExcluded ?? []),
            params.limit,
          ]
        )
      )) ?? 0
    );
  }
  async maintenanceCleanQueues(
    leader: RuntimeLeader,
    updatedBefore: Temporal.Instant,
    limit: number,
    batch?: RuntimeMaintenanceBatch
  ): Promise<number> {
    return (
      (await this.#fenced(leader, batch ? { signal: batch.signal } : {}, (tx) =>
        this.#write(
          tx,
          "DELETE FROM {schema}river_queue WHERE updated_at<? ORDER BY updated_at,name LIMIT ?",
          [timestamp(updatedBefore), limit]
        )
      )) ?? 0
    );
  }
  async maintenanceCleanNotifications(
    leader: RuntimeLeader,
    createdBefore: Temporal.Instant,
    limit: number,
    batch?: RuntimeMaintenanceBatch
  ): Promise<number> {
    return (
      (await this.#fenced(leader, batch ? { signal: batch.signal } : {}, (tx) =>
        this.#write(
          tx,
          "DELETE FROM {schema}river_notification WHERE created_at<? ORDER BY created_at,id LIMIT ?",
          [timestamp(createdBefore), limit]
        )
      )) ?? 0
    );
  }
}

function filters(
  params: Pick<
    JobListParams,
    "ids" | "kinds" | "priorities" | "queues" | "states"
  >
): { where: string[]; values: unknown[] } {
  const where: string[] = [];
  const values: unknown[] = [];
  for (const [column, items] of [
    ["id", params.ids],
    ["CAST(kind AS BINARY)", params.kinds],
    ["priority", params.priorities],
    ["CAST(queue AS BINARY)", params.queues],
    ["state", params.states],
  ] as const) {
    if (items.length) {
      where.push(`${column} IN (${items.map(() => "?").join(",")})`);
      values.push(...items);
    }
  }
  return { where, values };
}

function binding(value: unknown): string | number | boolean | Buffer | null {
  if (typeof value === "bigint") return value.toString();
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    Buffer.isBuffer(value)
  )
    return value;
  throw new TypeError("Invalid MySQL SQL parameter");
}

// MySQL JSON_CONTAINS flattens nested arrays and accepts scalar-in-array
// matches. Preserve Postgres containment boundaries by testing each element.
// Scalar subqueries avoid MySQL 8 semijoin rewrites losing nested correlations.
function metadataContains(
  wanted: JsonValue,
  value: string,
  values: unknown[],
  depth = 0
): string {
  const alias = `metadata_${depth}`;
  if (
    wanted !== null &&
    typeof wanted === "object" &&
    !isExactJsonNumber(wanted)
  ) {
    const array = Array.isArray(wanted);
    const children = array
      ? wanted.map((item) => {
          const predicate = metadataContains(
            item,
            `${alias}.value`,
            values,
            depth + 1
          );
          return `(SELECT 1 FROM JSON_TABLE(${value}, '$[*]' COLUMNS(value JSON PATH '$')) AS ${alias} WHERE ${predicate} LIMIT 1) IS NOT NULL`;
        })
      : Object.entries(wanted).map(([key, item]) => {
          values.push(`$.${JSON.stringify(key)}`);
          const predicate = metadataContains(
            item,
            `${alias}.value`,
            values,
            depth + 1
          );
          return `(SELECT 1 FROM JSON_TABLE(JSON_EXTRACT(${value},?), '$' COLUMNS(value JSON PATH '$')) AS ${alias} WHERE ${predicate} LIMIT 1) IS NOT NULL`;
        });
    return `(JSON_TYPE(${value})='${array ? "ARRAY" : "OBJECT"}' AND ${children.join(" AND ") || "true"})`;
  }
  if (wanted === null)
    return `(${value} IS NULL OR JSON_TYPE(${value})='NULL')`;
  values.push(json(wanted));
  return `(JSON_TYPE(${value}) NOT IN ('ARRAY','OBJECT') AND JSON_CONTAINS(${value},CAST(? AS JSON)))`;
}

function required<T>(value: T | null | undefined): T {
  if (value == null) throw new Error("MySQL row disappeared while locked");
  return value;
}
