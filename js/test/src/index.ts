/**
 * Typed helpers for testing River job producers and workers.
 *
 * - {@link createTestClient} records insertions instead of writing them, and
 *   {@link requireInserted} / {@link requireNotInserted} /
 *   {@link requireManyInserted} assert on that log like Go's `rivertest`.
 *   {@link requireInsertedInDatabase} / {@link requireNotInsertedInDatabase}
 *   assert on the jobs a real client persisted, optionally inside a
 *   transaction.
 * - {@link testJob} builds a realistic job by validating producer input
 *   through its definition, exactly as the runtime does before working.
 * - {@link workOnce} runs one handler (directly or from a `Workers`
 *   registry) and returns its outcome, output, metadata, and logs.
 *
 * For full runtime semantics (middleware, retries, scheduling, persistence),
 * run a real client against an in-memory `node:sqlite` database; see the
 * testing guide.
 *
 * @packageDocumentation
 */
import { AssertionError } from "node:assert";

import {
  Client,
  JOB_STATE,
  MAX_ATTEMPTS_DEFAULT,
  PRIORITY_DEFAULT,
  QUEUE_DEFAULT,
  toJsonObject,
  toJsonValue,
  UnsupportedCapabilityError,
} from "riverqueue";
import type {
  ClientOptions,
  InsertClient,
  Job,
  JobDefinition,
  JobDefinitionArgs,
  JobDefinitionInput,
  JobListResult,
  JobOperations,
  JobRow,
  JobState,
  JsonObject,
  JsonValue,
  LogAttributes,
  Resumable,
  WorkContext,
  WorkHandler,
  WorkLogFunction,
  WorkLogger,
  Workers,
  WorkOutcome,
} from "riverqueue";
import {
  createResumable,
  decodeJobArgs,
  finishResumable,
  jsonValuesEqual,
  registerDriver,
  workerRegistration,
} from "riverqueue/unstable-driver";
import type {
  DriverInsertResult,
  InsertDriver,
  InsertDriverOptions,
  JobInsertParams,
} from "riverqueue/unstable-driver";

/** One job recorded by a test client, with the transaction it was given. */
export interface TestInsertion<Transaction = unknown> {
  /** The row River would have persisted, with a deterministic ID. */
  readonly job: JobRow;
  /** The caller-owned transaction passed as `{ tx }`, if any. */
  readonly transaction: Transaction | undefined;
}

/**
 * Options for {@link createTestClient}. Insert options, hooks, insert
 * middleware, and plugins configure the client as they would a real one, so
 * a test sees the jobs they produce.
 */
export interface TestClientOptions<Transaction = unknown> extends Pick<
  ClientOptions<Transaction>,
  "defaultInsertOptions" | "hooks" | "insertMiddleware" | "plugins"
> {
  /** Clock used for `createdAt`. Defaults to `Temporal.Now.instant`. */
  readonly now?: () => Temporal.Instant;
  /** First ID assigned to an inserted job. Defaults to `1n`. */
  readonly startingId?: bigint;
}

/** An insert-only test client and its insertion log. */
export interface TestClient<Transaction = unknown> {
  /**
   * An insert-only client. Pass it wherever application code takes an
   * `InsertClient<Transaction>`.
   */
  readonly client: InsertClient<Transaction>;
  /** Insertions in call order. The array grows as the client inserts. */
  readonly insertions: readonly TestInsertion<Transaction>[];
}

/**
 * Create a deterministic, database-free client that records insertions.
 *
 * `Transaction` is the transaction type application code passes as `{ tx }`
 * (for example `PoolClient`); it is recorded but never used.
 *
 * @example
 * ```ts
 * const { client, insertions } = createTestClient<PoolClient>();
 * await signUp(client, "person@example.com");
 * requireInserted(insertions, sendWelcomeEmail, {
 *   args: { to: "person@example.com" },
 * });
 * ```
 */
export function createTestClient<Transaction = unknown>(
  options: TestClientOptions<Transaction> = {}
): TestClient<Transaction> {
  const {
    defaultInsertOptions,
    hooks,
    insertMiddleware,
    now,
    plugins,
    startingId,
  } = options;
  const driver = new TestInsertDriver<Transaction>({
    ...(now === undefined ? {} : { now }),
    ...(startingId === undefined ? {} : { startingId }),
  });
  return {
    client: new Client(driver, {
      ...(defaultInsertOptions === undefined ? {} : { defaultInsertOptions }),
      ...(hooks === undefined ? {} : { hooks }),
      ...(insertMiddleware === undefined ? {} : { insertMiddleware }),
      ...(plugins === undefined ? {} : { plugins }),
    }),
    insertions: driver.insertions,
  };
}

/** Fields an inserted job must match; omitted fields are not compared. */
export interface InsertedJobMatch<Definition extends JobDefinition> {
  /** Top-level args that must be equal (compared as JSON). */
  readonly args?: Partial<JobDefinitionInput<Definition>>;
  /** Attempts allowed before the job is discarded. */
  readonly maxAttempts?: number;
  /** Metadata that must be equal (compared as JSON). */
  readonly metadata?: JsonObject;
  /** Priority the job was inserted with. */
  readonly priority?: number;
  /** Queue the job was inserted into. */
  readonly queue?: string;
  /** Time the job was scheduled for, compared exactly. */
  readonly scheduledAt?: Temporal.Instant;
  /** State the job was inserted in, such as `scheduled`. */
  readonly state?: JobState;
  /** Tags that must be equal, in order. */
  readonly tags?: readonly string[];
}

/** A test client or its insertion log. */
export type InsertionLog =
  Pick<TestClient, "insertions"> | readonly TestInsertion[];

/**
 * Assert that exactly one recorded insertion matches `definition` (and
 * `match`, when given) and return it with its producer args typed. Throws an
 * `AssertionError` describing the recorded jobs otherwise.
 */
export function requireInserted<Definition extends JobDefinition>(
  log: InsertionLog,
  definition: Definition,
  match: InsertedJobMatch<Definition> = {}
): JobRow<JobDefinitionInput<Definition>> {
  const matches = findInserted(log, definition, match);
  if (matches.length !== 1) {
    throw new AssertionError({
      message: `expected exactly one inserted ${JSON.stringify(definition.kind)} job matching ${JSON.stringify(match)}, found ${matches.length}; inserted: ${describeLog(log)}`,
    });
  }
  return matches[0] as JobRow<JobDefinitionInput<Definition>>;
}

/**
 * Assert that no recorded insertion matches `definition` (and `match`, when
 * given).
 */
export function requireNotInserted<Definition extends JobDefinition>(
  log: InsertionLog,
  definition: Definition,
  match: InsertedJobMatch<Definition> = {}
): void {
  const matches = findInserted(log, definition, match);
  if (matches.length > 0) {
    throw new AssertionError({
      message: `expected no inserted ${JSON.stringify(definition.kind)} job matching ${JSON.stringify(match)}, found ${matches.length}`,
    });
  }
}

/** A client whose persisted jobs can be listed, such as a runtime `Client`. */
export interface JobListingClient<Transaction = unknown> {
  readonly jobs: Pick<JobOperations<Transaction>, "list">;
}

/**
 * Assert that exactly one job in the database matches `definition` (and
 * `match`, when given) and return it with its producer args typed, like
 * Go's `rivertest.RequireInsertedTx`. Pass `tx` to look inside a transaction
 * that hasn't committed yet. Throws an `AssertionError` otherwise.
 */
export async function requireInsertedInDatabase<
  Definition extends JobDefinition,
  Transaction = unknown,
>(
  client: JobListingClient<Transaction>,
  definition: Definition,
  match: InsertedJobMatch<Definition> = {},
  options: { readonly tx?: Transaction } = {}
): Promise<JobRow<JobDefinitionInput<Definition>>> {
  const matches = await findPersisted(client, definition, match, options);
  if (matches.length !== 1) {
    throw new AssertionError({
      message: `expected exactly one ${JSON.stringify(definition.kind)} job in the database matching ${JSON.stringify(match)}, found ${matches.length}`,
    });
  }
  return matches[0] as JobRow<JobDefinitionInput<Definition>>;
}

/**
 * Assert that no job in the database matches `definition` (and `match`,
 * when given), like Go's `rivertest.RequireNotInsertedTx`.
 */
export async function requireNotInsertedInDatabase<
  Definition extends JobDefinition,
  Transaction = unknown,
>(
  client: JobListingClient<Transaction>,
  definition: Definition,
  match: InsertedJobMatch<Definition> = {},
  options: { readonly tx?: Transaction } = {}
): Promise<void> {
  const matches = await findPersisted(client, definition, match, options);
  if (matches.length > 0) {
    throw new AssertionError({
      message: `expected no ${JSON.stringify(definition.kind)} job in the database matching ${JSON.stringify(match)}, found ${matches.length}`,
    });
  }
}

/** One expected insertion for {@link requireManyInserted}. */
export interface ExpectedInsertion<
  Definition extends JobDefinition = JobDefinition,
> extends InsertedJobMatch<Definition> {
  /** Definition the inserted job must belong to. */
  readonly job: Definition;
}

/**
 * Assert that the recorded insertions are exactly `expected`, in order, and
 * return them.
 */
export function requireManyInserted(
  log: InsertionLog,
  expected: readonly ExpectedInsertion[]
): readonly JobRow[] {
  const jobs = insertionsOf(log).map(({ job }) => job);
  const ok =
    jobs.length === expected.length &&
    expected.every((item, index) => {
      const job = jobs[index];
      return (
        job !== undefined && job.kind === item.job.kind && jobMatches(job, item)
      );
    });
  if (!ok) {
    throw new AssertionError({
      message: `expected inserted jobs ${JSON.stringify(expected.map(({ job }) => job.kind))}, found ${describeLog(log)}`,
    });
  }
  return jobs;
}

/** Row fields that {@link testJob} lets a test override. */
export interface TestJobOptions {
  /** Attempt number of the running attempt. Defaults to 1. */
  readonly attempt?: number;
  /** When the attempt started. Defaults to `now`. */
  readonly attemptedAt?: Temporal.Instant | null;
  /** Clients that attempted the job, oldest first; `riverqueue-test` by default. */
  readonly attemptedBy?: readonly string[];
  /** When the job was inserted. Defaults to `now`. */
  readonly createdAt?: Temporal.Instant;
  /** Errors recorded by earlier attempts, oldest first. */
  readonly errors?: JobRow["errors"];
  /** When the job reached a final state; null while it can still run. */
  readonly finalizedAt?: Temporal.Instant | null;
  /** Job ID. Defaults to 1. */
  readonly id?: bigint;
  /** Attempts allowed. Defaults to the definition's default, else River's. */
  readonly maxAttempts?: number;
  /** Job metadata. Defaults to `{}`. */
  readonly metadata?: JsonObject;
  /** Clock for unspecified timestamps. Defaults to `Temporal.Now.instant()`. */
  readonly now?: Temporal.Instant;
  /** Priority from 1 (first) to 4. Defaults like `maxAttempts`. */
  readonly priority?: number;
  /** Queue of the job. Defaults like `maxAttempts`. */
  readonly queue?: string;
  /** When the job became available. Defaults to the definition's, else `now`. */
  readonly scheduledAt?: Temporal.Instant;
  /** Job state. Defaults to `running`, as a handler sees it. */
  readonly state?: JobState;
  readonly tags?: readonly string[];
  /** Unique key bytes of a unique job, or null. */
  readonly uniqueKey?: Uint8Array | null;
  /** States in which the unique key is enforced, or null. */
  readonly uniqueStates?: readonly JobState[] | null;
}

/**
 * Build a realistic running job for a worker test.
 *
 * `input` is what a producer would insert. It is converted to River JSON and
 * validated with the definition's schema or decoder, exactly as the runtime
 * does before working, so `job.args` is the worker's typed args and
 * `job.rawArgs` the persisted JSON. Row fields default from the definition's
 * insertion defaults.
 */
export async function testJob<Definition extends JobDefinition>(
  definition: Definition,
  input: JobDefinitionInput<Definition>,
  options: TestJobOptions = {}
): Promise<Job<Definition>> {
  const now = options.now ?? Temporal.Now.instant();
  const defaults = definition.defaults;
  const rawArgs = toJsonObject(input);
  const args: JobDefinitionArgs<Definition> = await decodeJobArgs(
    definition,
    rawArgs
  );
  return {
    args,
    attempt: options.attempt ?? 1,
    attemptedAt: options.attemptedAt === undefined ? now : options.attemptedAt,
    attemptedBy: Object.freeze([
      ...(options.attemptedBy ?? ["riverqueue-test"]),
    ]),
    createdAt: options.createdAt ?? now,
    errors: Object.freeze([...(options.errors ?? [])]),
    finalizedAt: options.finalizedAt ?? null,
    id: options.id ?? 1n,
    kind: definition.kind,
    maxAttempts:
      options.maxAttempts ?? defaults.maxAttempts ?? MAX_ATTEMPTS_DEFAULT,
    metadata: toJsonObject(options.metadata ?? defaults.metadata ?? {}),
    priority: options.priority ?? defaults.priority ?? PRIORITY_DEFAULT,
    queue: options.queue ?? defaults.queue ?? QUEUE_DEFAULT,
    rawArgs,
    scheduledAt: options.scheduledAt ?? defaults.scheduledAt ?? now,
    state: options.state ?? JOB_STATE.running,
    tags: Object.freeze([...(options.tags ?? defaults.tags ?? [])]),
    uniqueKey: options.uniqueKey ?? null,
    uniqueStates: options.uniqueStates ?? null,
  };
}

/** One message logged through the work context's logger. */
export interface TestLogEntry {
  readonly attributes: LogAttributes | undefined;
  readonly level: "debug" | "error" | "info" | "warn";
  readonly message: string;
}

/** Options for {@link workOnce}. */
export interface WorkOnceOptions<Transaction = unknown> {
  /** Worker identity recorded in `ctx.execution`. */
  readonly attemptedBy?: string;
  /**
   * Client exposed as `ctx.client`. Defaults to a {@link createTestClient}
   * client that records insertions; its job and queue operations throw
   * `UnsupportedCapabilityError`.
   */
  readonly client?: Client<Transaction>;
  /** Implementation of `ctx.completeTx`. Defaults to one that rejects. */
  readonly completeTx?: (
    tx: Transaction,
    options?: { readonly output?: JsonValue }
  ) => Promise<JobRow>;
  /** Start time recorded in `ctx.execution`. */
  readonly now?: Temporal.Instant;
  /** Resumable state. Defaults to one read from the job's metadata. */
  readonly resumable?: Resumable;
  /** Abort signal exposed as `ctx.signal`. */
  readonly signal?: AbortSignal;
}

interface WorkOnceResultBase<Definition extends JobDefinition, Transaction> {
  readonly context: WorkContext<Definition, Transaction>;
  /** Messages the handler logged. */
  readonly logs: readonly TestLogEntry[];
  /** Metadata updates to merge with this attempt, including resumable progress. */
  readonly metadata: JsonObject;
  /** Output recorded with `recordOutput` or `complete({ output })`. */
  readonly output: JsonValue | undefined;
}

/** Result of {@link workOnce}, narrowed by `status`. */
export type WorkOnceResult<
  Definition extends JobDefinition,
  Transaction = unknown,
> =
  | (WorkOnceResultBase<Definition, Transaction> & {
      readonly outcome: WorkOutcome | undefined;
      readonly status: "succeeded";
    })
  | (WorkOnceResultBase<Definition, Transaction> & {
      readonly error: unknown;
      readonly status: "failed";
    });

/**
 * Run one handler against a job without a database or background runtime.
 *
 * `handler` is either a handler function or a `Workers` registry, in which
 * case the in-process handler registered for `job.kind` runs with its
 * configured timeout applied to `ctx.signal`. Middleware, hooks, retries,
 * and persistence are not simulated.
 */
export async function workOnce<
  Definition extends JobDefinition,
  Transaction = unknown,
>(
  job: Job<Definition>,
  handler: WorkHandler<Definition, Transaction> | Workers<Transaction>,
  options: WorkOnceOptions<Transaction> = {}
): Promise<WorkOnceResult<Definition, Transaction>> {
  const { handler: work, timeoutSignal } = resolveHandler(job, handler);
  const logs: TestLogEntry[] = [];
  const metadata: JsonObject = {};
  let output: JsonValue | undefined;
  const client =
    options.client ??
    (createTestClient<Transaction>().client as Client<Transaction>);
  const signals = [options.signal, timeoutSignal].filter(
    (signal): signal is AbortSignal => signal !== undefined
  );
  const context: WorkContext<Definition, Transaction> = {
    client,
    completeTx:
      options.completeTx ??
      (() =>
        Promise.reject(
          new UnsupportedCapabilityError(
            "@riverqueue/test workOnce",
            "transactional completion"
          )
        )),
    execution: {
      attemptedBy: options.attemptedBy ?? "riverqueue-test",
      startedAt: options.now ?? Temporal.Now.instant(),
    },
    job,
    logger: testLogger(logs),
    recordOutput: (value) => {
      output = toJsonValue(value);
      metadata.output = output;
    },
    resumable:
      options.resumable ??
      createResumable(client, { ...job, args: job.rawArgs }),
    setMetadata: (key, value) => {
      metadata[key] = toJsonValue(value);
    },
    signal:
      signals.length === 0
        ? new AbortController().signal
        : AbortSignal.any(signals),
  };
  try {
    context.signal.throwIfAborted();
    const outcome = (await work(context)) as WorkOutcome | undefined;
    if (outcome?.type === "complete" && outcome.output !== undefined) {
      context.recordOutput(outcome.output);
    }
    const finished = finishResumable(context.resumable, false);
    Object.assign(metadata, finished.metadata);
    if (finished.error !== null) throw finished.error;
    return {
      context,
      logs,
      metadata: toJsonObject(metadata),
      outcome,
      output,
      status: "succeeded",
    };
  } catch (error: unknown) {
    Object.assign(metadata, finishResumable(context.resumable, true).metadata);
    return {
      context,
      error,
      logs,
      metadata: toJsonObject(metadata),
      output,
      status: "failed",
    };
  }
}

function resolveHandler<Definition extends JobDefinition, Transaction>(
  job: Job<Definition>,
  handler: WorkHandler<Definition, Transaction> | Workers<Transaction>
): {
  readonly handler: WorkHandler<Definition, Transaction>;
  readonly timeoutSignal: AbortSignal | undefined;
} {
  if (typeof handler === "function") {
    return { handler, timeoutSignal: undefined };
  }
  const registration = workerRegistration(handler, job.kind);
  if (registration === undefined) {
    throw new AssertionError({
      message: `no worker registered for job kind ${JSON.stringify(job.kind)}`,
    });
  }
  if (registration.type !== "in_process") {
    throw new UnsupportedCapabilityError(
      "@riverqueue/test workOnce",
      "executor-owned workers"
    );
  }
  const timeout = registration.options.timeout;
  return {
    handler: registration.handler as unknown as WorkHandler<
      Definition,
      Transaction
    >,
    timeoutSignal:
      timeout === undefined || timeout === null
        ? undefined
        : AbortSignal.timeout(Math.max(1, timeout.total("milliseconds"))),
  };
}

class TestInsertDriver<Transaction> implements InsertDriver<Transaction> {
  declare readonly "~river"?: {
    readonly capability: "insert";
    readonly transaction: Transaction;
  };
  readonly insertions: TestInsertion<Transaction>[] = [];
  readonly #now: () => Temporal.Instant;
  #nextId: bigint;

  constructor(options: Pick<TestClientOptions, "now" | "startingId">) {
    this.#nextId = options.startingId ?? 1n;
    this.#now = options.now ?? (() => Temporal.Now.instant());
    registerDriver<Transaction>(this, {
      backend: "test",
      capability: "insert",
      operations: this,
    });
  }

  jobInsert(
    params: JobInsertParams,
    options: InsertDriverOptions<Transaction> = {}
  ): DriverInsertResult {
    const [result] = this.#insert([params], options);
    if (result === undefined) throw new Error("test insertion returned no job");
    return result;
  }

  jobInsertMany(
    params: readonly JobInsertParams[],
    options: InsertDriverOptions<Transaction> = {}
  ): readonly DriverInsertResult[] {
    return this.#insert(params, options);
  }

  #insert(
    params: readonly JobInsertParams[],
    options: InsertDriverOptions<Transaction>
  ): readonly DriverInsertResult[] {
    return params.map((item) => {
      const job: JobRow = Object.freeze({
        args: item.args,
        attempt: 0,
        attemptedAt: null,
        attemptedBy: [],
        createdAt: this.#now(),
        errors: [],
        finalizedAt: null,
        id: this.#nextId++,
        kind: item.kind,
        maxAttempts: item.maxAttempts,
        metadata: item.metadata,
        priority: item.priority,
        queue: item.queue,
        scheduledAt: item.scheduledAt ?? Temporal.Now.instant(),
        state: item.state,
        tags: item.tags,
        uniqueKey: item.uniqueKey,
        uniqueStates: item.uniqueStates,
      });
      this.insertions.push(Object.freeze({ job, transaction: options.tx }));
      return { job, status: "inserted" };
    });
  }
}

function insertionsOf(log: InsertionLog): readonly TestInsertion[] {
  return "insertions" in log ? log.insertions : log;
}

function findInserted<Definition extends JobDefinition>(
  log: InsertionLog,
  definition: Definition,
  match: InsertedJobMatch<Definition>
): readonly JobRow[] {
  return insertionsOf(log)
    .map(({ job }) => job)
    .filter((job) => job.kind === definition.kind && jobMatches(job, match));
}

/** Every persisted job of `definition`'s kind matching `match`. */
async function findPersisted<Transaction>(
  client: JobListingClient<Transaction>,
  definition: JobDefinition,
  match: InsertedJobMatch<JobDefinition>,
  options: { readonly tx?: Transaction }
): Promise<JobRow[]> {
  const matches: JobRow[] = [];
  let after: string | null = null;
  do {
    const page: JobListResult = await client.jobs.list({
      ...(after === null ? {} : { after }),
      kinds: [definition.kind],
      limit: 1_000,
      ...(options.tx === undefined ? {} : { tx: options.tx }),
    });
    matches.push(...page.jobs.filter((job) => jobMatches(job, match)));
    after = page.nextCursor;
  } while (after !== null);
  return matches;
}

function jobMatches(
  job: JobRow,
  match: InsertedJobMatch<JobDefinition>
): boolean {
  if (match.args !== undefined) {
    for (const [key, value] of Object.entries(toJsonObject(match.args))) {
      const actual = job.args[key];
      if (actual === undefined || !jsonValuesEqual(actual, value)) return false;
    }
  }
  if (match.metadata !== undefined) {
    const metadata = toJsonObject(match.metadata);
    for (const [key, value] of Object.entries(metadata)) {
      const actual = job.metadata[key];
      if (actual === undefined || !jsonValuesEqual(actual, value)) return false;
    }
  }
  return (
    (match.maxAttempts === undefined ||
      job.maxAttempts === match.maxAttempts) &&
    (match.priority === undefined || job.priority === match.priority) &&
    (match.queue === undefined || job.queue === match.queue) &&
    (match.scheduledAt === undefined ||
      job.scheduledAt.equals(match.scheduledAt)) &&
    (match.state === undefined || job.state === match.state) &&
    (match.tags === undefined ||
      (job.tags.length === match.tags.length &&
        job.tags.every((tag, index) => tag === match.tags?.[index])))
  );
}

function describeLog(log: InsertionLog): string {
  return JSON.stringify(
    insertionsOf(log).map(({ job }) => ({
      args: job.args,
      kind: job.kind,
      queue: job.queue,
    }))
  );
}

function testLogger(entries: TestLogEntry[]): WorkLogger {
  const append =
    (level: TestLogEntry["level"]): WorkLogFunction =>
    (first: LogAttributes | string, message?: string): void => {
      entries.push(
        typeof first === "string"
          ? { attributes: undefined, level, message: first }
          : { attributes: first, level, message: message ?? "" }
      );
    };
  return {
    debug: append("debug"),
    error: append("error"),
    info: append("info"),
    warn: append("warn"),
  };
}
