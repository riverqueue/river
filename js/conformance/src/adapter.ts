// The adapter's request handler: one class over River's driver interface,
// serving Postgres and SQLite alike.

import { createMigrator } from "@riverqueue/migrate";
import {
  Client,
  JOB_STATE,
  type ClientDriver,
  type InsertManyItem,
  type InsertOptions,
  type JobListOrderBy,
  type JobRow,
  type JobState,
  type JsonObject,
  type JsonValue,
  type UniqueOptions,
} from "riverqueue";
import {
  decodeJobListCursor,
  encodeJobListCursor,
} from "riverqueue/unstable-driver";

import {
  CODE,
  invalidParams,
  notFound,
  Params,
  ProtocolError,
  rejected,
  toProtocolJob,
} from "./protocol.js";
import {
  Barriers,
  echo,
  START_FIELDS,
  startClient,
  type RunningClient,
} from "./worker.js";

/** A database the adapter runs River on. */
export interface Backend<Tx> {
  readonly name: "postgres" | "sqlite";
  /** Begin a transaction for `tx_begin`. */
  begin(): Promise<OpenTransaction<Tx>>;
  close(): Promise<void>;
  /** River's driver for a schema, or the default for "". */
  driver(schema: string): ClientDriver<Tx, "runtime">;
}

/** A transaction `tx_begin` opened. */
export interface OpenTransaction<Tx> {
  end(commit: boolean): Promise<void>;
  readonly tx: Tx;
}

const LIST_ORDER: Readonly<Record<string, JobListOrderBy>> = {
  finalized_at: "finalizedAt",
  id: "id",
  scheduled_at: "scheduledAt",
  time: "time",
};

/** Handles the contract's requests for one backend. */
export class Adapter<Tx> {
  readonly #backend: Backend<Tx>;
  readonly #barriers = new Barriers();
  /** Clients for everything but working jobs, by schema. */
  readonly #clients = new Map<string, Client<Tx>>();
  readonly #transactions = new Map<string, OpenTransaction<Tx>>();
  readonly #version: string;
  #running: RunningClient | null = null;

  constructor(backend: Backend<Tx>, version: string) {
    this.#backend = backend;
    this.#version = version;
  }

  async handle(
    method: string,
    params: JsonValue | undefined
  ): Promise<unknown> {
    switch (method) {
      case "cancel":
      case "retry":
        return this.#job(
          method,
          new Params(params, "params", ["id", "schema", "tx"])
        );
      case "handshake":
        new Params(params, "params", []);
        return {
          driver: this.#backend.name,
          implementation: "js",
          version: this.#version,
        };
      case "insert":
        return this.#insert(
          new Params(params, "params", ["jobs", "schema", "tx"])
        );
      case "list":
        return this.#list(new Params(params, "params", LIST_FIELDS));
      case "migrate":
        return this.#migrate(
          new Params(params, "params", [
            "direction",
            "schema",
            "target_version",
          ])
        );
      case "queue":
        return this.#queue(
          new Params(params, "params", [
            "action",
            "metadata",
            "name",
            "schema",
            "tx",
          ])
        );
      case "release":
        this.#barriers.release(
          new Params(params, "params", ["name"]).string("name")
        );
        return {};
      case "request_resign": {
        const request = new Params(params, "params", ["schema", "tx"]);
        await this.#client(request).requestLeadershipResignation(
          this.#tx(request)
        );
        return {};
      }
      case "start":
        return this.#start(new Params(params, "params", START_FIELDS));
      case "stats":
        new Params(params, "params", []);
        return this.#requireRunning().stats();
      case "stop": {
        const cancelJobs = new Params(params, "params", ["cancel"]).boolean(
          "cancel"
        );
        const running = this.#requireRunning();
        this.#running = null;
        await running.stop(cancelJobs);
        return {};
      }
      case "tx_begin":
        return this.#txBegin(new Params(params, "params", ["tx"]));
      case "tx_end":
        return this.#txEnd(new Params(params, "params", ["commit", "tx"]));
    }
    throw new ProtocolError(
      CODE.methodNotFound,
      `unknown method ${JSON.stringify(method)}`
    );
  }

  /** Stop the running client and roll back open transactions. */
  async shutdown(): Promise<void> {
    const running = this.#running;
    this.#running = null;
    await running?.stop(true).catch(() => undefined);
    for (const transaction of this.#transactions.values()) {
      await transaction.end(false).catch(() => undefined);
    }
    this.#transactions.clear();
  }

  #client(params: Params): Client<Tx> {
    const schema = params.string("schema");
    let client = this.#clients.get(schema);
    if (client === undefined) {
      client = new Client(this.#backend.driver(schema));
      this.#clients.set(schema, client);
    }
    return client;
  }

  async #insert(params: Params): Promise<unknown> {
    const items: InsertManyItem[] = (params.array("jobs") ?? []).map(
      (value, index) => {
        const job = new Params(value, `params.jobs[${index}]`, [
          "behavior",
          "duration_ms",
          "message",
          "opts",
        ]);
        return {
          args: {
            behavior: job.string("behavior"),
            duration_ms: job.integer("duration_ms"),
            message: job.string("message"),
          },
          job: echo,
          options: insertOptions(job.object("opts", OPTS_FIELDS)),
        };
      }
    );
    const results = await this.#client(params).insertMany(
      items,
      this.#tx(params)
    );
    return {
      results: results.map((result) => ({
        job: toProtocolJob(result.job),
        unique_skipped_as_duplicate: result.status === "duplicate",
      })),
    };
  }

  async #job(method: "cancel" | "retry", params: Params): Promise<unknown> {
    const id = params.bigint("id") ?? 0n;
    const client = this.#client(params);
    const job =
      method === "cancel"
        ? await client.jobs.cancel(id, this.#tx(params))
        : await client.jobs.retry(id, this.#tx(params));
    if (job === null) throw notFound(`job ${id} not found`);
    return toProtocolJob(job);
  }

  async #list(params: Params): Promise<unknown> {
    const orderBy = LIST_ORDER[params.string("order_by") || "id"];
    if (orderBy === undefined) throw invalidParams("unknown order_by");
    const direction = params.string("direction") || "asc";
    if (direction !== "asc" && direction !== "desc") {
      throw invalidParams(`unknown direction ${JSON.stringify(direction)}`);
    }
    const after = params.string("after");
    if (after !== "") {
      try {
        decodeJobListCursor(after);
      } catch (error: unknown) {
        throw invalidParams(`invalid cursor: ${String(error)}`);
      }
    }
    const states = params.strings("states") as JobState[] | undefined;
    const limit = params.integer("limit");
    const metadata = params.json("metadata");

    const listed = await this.#client(params).jobs.list({
      ...(after !== "" && { after }),
      ...defined("ids", params.bigints("ids")),
      ...defined("kinds", params.strings("kinds")),
      ...(limit > 0 && { limit }),
      ...defined("metadata", metadata as JsonObject | undefined),
      orderBy,
      ...defined("priorities", params.integers("priorities")),
      ...defined("queues", params.strings("queues")),
      sortDirection: direction,
      ...defined("states", states),
      ...defined("tagsAll", params.strings("tags_all")),
      ...this.#tx(params),
    });
    // Like Go's LastCursor, every page that lists a job has a cursor.
    const last = listed.jobs.at(-1);
    return {
      cursor:
        last === undefined
          ? null
          : encodeJobListCursor(last, {
              sortField: orderBy,
              states: states ?? Object.values(JOB_STATE),
            }),
      jobs: listed.jobs.map((job: JobRow) => toProtocolJob(job)),
    };
  }

  async #migrate(params: Params): Promise<unknown> {
    const migrator = createMigrator(
      this.#backend.driver(params.string("schema"))
    );
    const target = params.json("target_version");
    if (target !== undefined && typeof target !== "number") {
      throw invalidParams("target_version must be an integer");
    }
    // Go's -1 migrates down past the first version, which is 0 here.
    const options =
      target === undefined ? {} : { targetVersion: Math.max(target, 0) };
    let result;
    switch (params.string("direction")) {
      case "":
      case "up":
        result = await migrator.migrateUp(options);
        break;
      case "down":
        result = await migrator.migrateDown(options);
        break;
      default:
        throw invalidParams("unknown direction");
    }
    return { versions: result.versions.map(({ version }) => version) };
  }

  async #queue(params: Params): Promise<unknown> {
    const name = params.string("name");
    const queues = this.#client(params).queues;
    const tx = this.#tx(params);
    let queue;
    switch (params.string("action")) {
      case "pause":
        queue = await queues.pause(name, tx);
        break;
      case "resume":
        queue = await queues.resume(name, tx);
        break;
      case "update": {
        const metadata = params.json("metadata") as JsonObject | undefined;
        queue = await queues.update(name, defined("metadata", metadata), tx);
        break;
      }
      default:
        throw invalidParams("unknown queue action");
    }
    // "*" has no single row to return.
    if (queue === null && name !== "*") {
      throw notFound(`queue ${JSON.stringify(name)} not found`);
    }
    return {};
  }

  #requireRunning(): RunningClient {
    if (this.#running === null) throw rejected("no client is running");
    return this.#running;
  }

  async #start(params: Params): Promise<unknown> {
    if (this.#running !== null) throw rejected("a client is already running");
    this.#running = await startClient(
      this.#backend.driver(params.string("schema")),
      params,
      this.#barriers
    );
    return {};
  }

  /** The transaction option for the request's `tx`, if it names one. */
  #tx(params: Params): { tx?: Tx } {
    const name = params.string("tx");
    if (name === "") return {};
    const transaction = this.#transactions.get(name);
    if (transaction === undefined) {
      throw notFound(`transaction ${JSON.stringify(name)} is not open`);
    }
    return { tx: transaction.tx };
  }

  async #txBegin(params: Params): Promise<unknown> {
    const name = params.string("tx");
    if (name === "") throw invalidParams("tx is required");
    if (this.#transactions.has(name)) {
      throw rejected(`transaction ${JSON.stringify(name)} is already open`);
    }
    this.#transactions.set(name, await this.#backend.begin());
    return {};
  }

  async #txEnd(params: Params): Promise<unknown> {
    const name = params.string("tx");
    const transaction = this.#transactions.get(name);
    if (transaction === undefined) {
      throw notFound(`transaction ${JSON.stringify(name)} is not open`);
    }
    this.#transactions.delete(name);
    await transaction.end(params.boolean("commit"));
    return {};
  }
}

const LIST_FIELDS = [
  "after",
  "direction",
  "ids",
  "kinds",
  "limit",
  "metadata",
  "order_by",
  "priorities",
  "queues",
  "schema",
  "states",
  "tags_all",
  "tx",
];
const OPTS_FIELDS = [
  "max_attempts",
  "metadata",
  "pending",
  "priority",
  "queue",
  "scheduled_at",
  "tags",
  "unique",
];
const UNIQUE_FIELDS = [
  "by_args",
  "by_period_ms",
  "by_queue",
  "by_state",
  "exclude_kind",
];

/** River's insert options for `opts`, whose zero values are defaults. */
function insertOptions(opts: Params | undefined): InsertOptions {
  if (opts === undefined) return {};
  const options: InsertOptions = {};
  const maxAttempts = opts.integer("max_attempts");
  if (maxAttempts !== 0) options.maxAttempts = maxAttempts;
  const metadata = opts.json("metadata");
  if (metadata !== undefined) options.metadata = metadata as JsonObject;
  if (opts.boolean("pending")) options.pending = true;
  const priority = opts.integer("priority");
  if (priority !== 0) options.priority = priority;
  const queue = opts.string("queue");
  if (queue !== "") options.queue = queue;
  const scheduledAt = opts.string("scheduled_at");
  if (scheduledAt !== "") {
    try {
      options.scheduledAt = Temporal.Instant.from(scheduledAt);
    } catch {
      throw invalidParams(
        `invalid scheduled_at ${JSON.stringify(scheduledAt)}`
      );
    }
  }
  const tags = opts.strings("tags");
  if (tags !== undefined) options.tags = tags;

  const unique = opts.object("unique", UNIQUE_FIELDS);
  if (unique !== undefined) {
    const uniqueOptions: UniqueOptions = {};
    if (unique.boolean("by_args")) uniqueOptions.byArgs = true;
    const byPeriod = unique.integer("by_period_ms");
    if (byPeriod !== 0) uniqueOptions.byPeriod = { milliseconds: byPeriod };
    if (unique.boolean("by_queue")) uniqueOptions.byQueue = true;
    const byState = unique.strings("by_state") as JobState[] | undefined;
    if (byState !== undefined) uniqueOptions.byState = byState;
    if (unique.boolean("exclude_kind")) uniqueOptions.excludeKind = true;
    // Like Go's zero UniqueOpts, options that select nothing aren't unique.
    if (Object.keys(uniqueOptions).length > 0) options.unique = uniqueOptions;
  }
  return options;
}

/** `{ [key]: value }` when value is defined, else nothing. */
function defined<Key extends string, Value>(
  key: Key,
  value: Value | undefined
): Partial<Record<Key, Value>> {
  return value === undefined ? {} : ({ [key]: value } as Record<Key, Value>);
}
