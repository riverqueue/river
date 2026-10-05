/**
 * Dispatch of the operations a client's pilot intercepts. Each intercepted
 * operation runs in one transaction (River's own, or the caller's) around
 * the pilot's interceptor and River's standard operation, which the
 * interceptor runs through a continuation bound to that transaction.
 * Without an interceptor the standard operation runs as before.
 */
import type {
  DriverInsertResult,
  JobCompletionCommand,
  JobCompletionResult,
  JobInsertParams,
  RuntimeDriver,
  RuntimeJobRescue,
  RuntimeLeader,
  RuntimeMaintenanceBatch,
} from "../driver.js";
import { ExtensionError, ValidationError } from "../errors.js";
import { withHandle } from "../internal/handle-gate.js";
import { JOB_STATE, type JobRow, type JobState } from "../job.js";
import { parseJson } from "../json.js";
import type {
  FinalizedJobDeleteParams,
  Pilot,
  PilotDatabase,
  PilotInsertReplacement,
  PilotInterceptors,
  PreparedInsertParams,
} from "../pilot.js";

/** A signal for operations nothing can abandon. */
const NEVER_ABORTED = new AbortController().signal;

/** The standard insertion River runs for `next`. */
export type StandardInsert<Transaction> = (
  params: readonly JobInsertParams[],
  tx: Transaction | undefined
) => Promise<readonly DriverInsertResult[]>;

/**
 * The client's operations that its pilot may intercept. Without a pilot, or
 * for an operation it doesn't intercept, each runs River's standard
 * operation directly.
 */
export class PilotOperations<Transaction = unknown> {
  readonly #database: PilotDatabase<Transaction> | undefined;
  readonly #intercept: PilotInterceptors<Transaction>;
  readonly #pilot: Pilot<Transaction> | undefined;

  constructor(
    pilot?: Pilot<Transaction>,
    database?: PilotDatabase<Transaction>
  ) {
    this.#database = database;
    this.#intercept = pilot?.intercept ?? {};
    this.#pilot = pilot;
  }

  /** Cancel one job, as `client.jobs.cancel` does. */
  cancel(
    driver: RuntimeDriver<Transaction>,
    id: bigint,
    tx: Transaction | undefined
  ): Promise<JobRow | null> {
    const interceptor = this.#intercept.cancel?.bind(this.#intercept);
    if (interceptor === undefined) {
      return Promise.resolve(driver.jobCancel(id, driverOptions(tx)));
    }
    return this.#jobOperation("cancel", interceptor, id, tx, (scopeTx) =>
      Promise.resolve(driver.jobCancel(id, { tx: scopeTx }))
    );
  }

  /**
   * Persist completions: a background batch (with `signal`) or a worker's
   * transactional completion (with `tx`).
   */
  complete(
    driver: RuntimeDriver<Transaction>,
    commands: readonly JobCompletionCommand[],
    options: { readonly signal?: AbortSignal; readonly tx?: Transaction }
  ): Promise<readonly JobCompletionResult[]> {
    if (options.tx !== undefined && this.serializes) {
      // A worker's transactional completion shares the caller's
      // transaction with the client's other operations.
      return withHandle(options.tx, () =>
        this.#complete(driver, commands, options)
      );
    }
    return this.#complete(driver, commands, options);
  }

  #complete(
    driver: RuntimeDriver<Transaction>,
    commands: readonly JobCompletionCommand[],
    options: { readonly signal?: AbortSignal; readonly tx?: Transaction }
  ): Promise<readonly JobCompletionResult[]> {
    const interceptor = this.#intercept.complete?.bind(this.#intercept);
    if (interceptor === undefined || commands.length === 0) {
      return Promise.resolve(driver.jobCompleteMany(commands, options));
    }
    const database = this.#requireDatabase();
    const signal = options.signal ?? NEVER_ABORTED;
    return database.transaction(
      (tx) =>
        runInterceptor<readonly JobCompletionResult[]>({
          invoke: (next) =>
            interceptor(
              Object.freeze({
                commands: Object.freeze([...commands]),
                database,
                signal,
                tx,
              }),
              next
            ),
          mode: "once",
          operation: "complete",
          snapshot: listFields,
          standard: async () =>
            protectArray(await driver.jobCompleteMany(commands, { tx })),
        }),
      transactionOptions(options.signal, options.tx)
    );
  }

  /** Read one page of stuck jobs for the rescuer. */
  getStuck(
    driver: RuntimeDriver<Transaction>,
    leader: RuntimeLeader,
    attemptedBefore: Temporal.Instant,
    afterId: bigint,
    limit: number,
    batch: RuntimeMaintenanceBatch
  ): Promise<readonly JobRow[]> {
    const standard = async (): Promise<readonly JobRow[]> =>
      (await driver.maintenanceGetStuck?.(
        leader,
        attemptedBefore,
        afterId,
        limit,
        batch
      )) ?? [];
    const interceptor = this.#intercept.getStuck?.bind(this.#intercept);
    if (interceptor === undefined) return standard();
    const database = this.#requireDatabase();
    return runInterceptor<readonly JobRow[]>({
      invoke: (next) =>
        interceptor(
          Object.freeze({
            afterId,
            attemptedBefore,
            database,
            leader,
            limit,
            signal: batch.signal,
            timeoutMs: batch.timeoutMs,
          }),
          next
        ),
      mode: "optional",
      operation: "getStuck",
      replacement: (result) => validateStuckRows(result, afterId, limit),
      snapshot: listFields,
      standard: async () => protectArray(await standard()),
    });
  }

  /**
   * Run one insertion's database write: River's standard insertion, or the
   * pilot's interceptor around it in `tx` or a transaction of River's.
   * `originalEncodedArgs` holds each row's arguments before the client's
   * argument transforms, for the interceptor.
   */
  insert(
    operation: "insert" | "insertMany",
    params: readonly JobInsertParams[],
    tx: Transaction | undefined,
    standard: StandardInsert<Transaction>,
    signal: AbortSignal = NEVER_ABORTED,
    originalEncodedArgs: readonly string[] = params.map(
      (row) => row.encodedArgs
    )
  ): Promise<readonly DriverInsertResult[]> {
    const interceptor = this.#intercept.insert?.bind(this.#intercept);
    if (interceptor === undefined) return standard(params, tx);
    const database = this.#requireDatabase();
    const prepared = Object.freeze([...params]);
    const originals = Object.freeze([...originalEncodedArgs]);
    return database.transaction(
      (scopeTx) =>
        runInterceptor<
          readonly DriverInsertResult[],
          [replacement?: PilotInsertReplacement]
        >({
          invoke: (next) =>
            interceptor(
              Object.freeze({
                database,
                operation,
                originalEncodedArgs: originals,
                params: prepared,
                signal,
                tx: scopeTx,
              }),
              next
            ),
          mode: "once",
          operation: "insert",
          snapshot: listFields,
          standard: async (replacement?: PilotInsertReplacement) => {
            const rows =
              replacement === undefined
                ? prepared
                : validateReplacement(replacement, prepared.length);
            const results = await standard(rows, scopeTx);
            if (results.length !== rows.length) {
              throw violation(
                `River's insertion returned ${results.length} results for ${rows.length} jobs`
              );
            }
            return protectArray(results);
          },
        }),
      transactionOptions(signal, tx)
    );
  }

  /**
   * Whether operations on one caller transaction must run one at a time,
   * because a pilot may run statements on it across awaits.
   */
  get serializes(): boolean {
    return this.#pilot !== undefined;
  }

  /** Rescue one page of stuck jobs, fenced by `leader`. */
  rescue(
    driver: RuntimeDriver<Transaction>,
    leader: RuntimeLeader,
    attemptedBefore: Temporal.Instant,
    jobs: readonly RuntimeJobRescue[],
    signal: AbortSignal
  ): Promise<number> {
    const interceptor = this.#intercept.rescue?.bind(this.#intercept);
    if (interceptor === undefined) {
      return Promise.resolve(
        driver.maintenanceRescue?.(leader, attemptedBefore, jobs) ?? 0
      );
    }
    const database = this.#requireDatabase();
    const frozenJobs = Object.freeze([...jobs]);
    return database.transaction(
      (tx) =>
        runInterceptor<number>({
          invoke: (next) =>
            interceptor(
              Object.freeze({
                attemptedBefore,
                database,
                jobs: frozenJobs,
                leader,
                signal,
                tx,
              }),
              next
            ),
          mode: "optional",
          operation: "rescue",
          replacement: (result) => {
            if (
              typeof result !== "number" ||
              !Number.isSafeInteger(result) ||
              result < 0 ||
              result > frozenJobs.length
            ) {
              throw violation(
                `rescue interceptor resolved with ${String(result)}, not a count of at most ${frozenJobs.length} rescued jobs`
              );
            }
            return result;
          },
          snapshot: (count) => count,
          standard: async () =>
            (await driver.maintenanceRescue?.(
              leader,
              attemptedBefore,
              frozenJobs,
              { tx }
            )) ?? 0,
        }),
      { signal }
    );
  }

  /** Retry one job, as `client.jobs.retry` does. */
  retry(
    driver: RuntimeDriver<Transaction>,
    id: bigint,
    tx: Transaction | undefined
  ): Promise<JobRow | null> {
    const interceptor = this.#intercept.retry?.bind(this.#intercept);
    if (interceptor === undefined) {
      return Promise.resolve(driver.jobRetry(id, driverOptions(tx)));
    }
    return this.#jobOperation("retry", interceptor, id, tx, (scopeTx) =>
      Promise.resolve(driver.jobRetry(id, { tx: scopeTx }))
    );
  }

  #jobOperation(
    operation: "cancel" | "retry",
    interceptor: NonNullable<PilotInterceptors<Transaction>["cancel"]>,
    id: bigint,
    tx: Transaction | undefined,
    standard: (tx: Transaction) => Promise<JobRow | null>
  ): Promise<JobRow | null> {
    const database = this.#requireDatabase();
    return database.transaction(
      (scopeTx) =>
        runInterceptor<JobRow | null>({
          invoke: (next) =>
            interceptor(
              Object.freeze({
                database,
                id,
                signal: NEVER_ABORTED,
                tx: scopeTx,
              }),
              next
            ),
          mode: "once",
          operation,
          snapshot: rowFields,
          standard: () => standard(scopeTx),
        }),
      transactionOptions(undefined, tx)
    );
  }

  #requireDatabase(): PilotDatabase<Transaction> {
    if (this.#database === undefined) {
      // Construction attaches a pilot only with a database.
      throw new ExtensionError("the client's pilot has no database");
    }
    return this.#database;
  }
}

/**
 * A pilot's view of its driver's database, where statements in a supplied
 * transaction wait for the client's other operations on it, as the
 * client's own operations do, instead of interleaving with another
 * operation's statements.
 */
export function serializedDatabase<Transaction>(
  database: PilotDatabase<Transaction>
): PilotDatabase<Transaction> {
  return Object.freeze({
    backend: database.backend,
    connection: <Result>(
      callback: (handle: Transaction) => PromiseLike<Result> | Result,
      options?: { readonly signal?: AbortSignal }
    ) => database.connection(callback, options),
    deleteFinalizedJobs: (
      params: FinalizedJobDeleteParams,
      options?: { readonly tx?: Transaction }
    ) =>
      options?.tx === undefined
        ? database.deleteFinalizedJobs(params, options)
        : withHandle(options.tx, () =>
            database.deleteFinalizedJobs(params, options)
          ),
    loadClaimed: (
      ids: readonly bigint[],
      options: { readonly tx: Transaction }
    ) => withHandle(options.tx, () => database.loadClaimed(ids, options)),
    notify: (
      topic: "control" | "insert",
      payloads: readonly string[],
      options: { readonly tx: Transaction }
    ) =>
      withHandle(options.tx, () => database.notify(topic, payloads, options)),
    schema: database.schema,
    transaction: <Result>(
      callback: (tx: Transaction) => PromiseLike<Result> | Result,
      options?: { readonly signal?: AbortSignal; readonly tx?: Transaction }
    ) =>
      options?.tx === undefined
        ? database.transaction(callback, options)
        : withHandle(options.tx, () => database.transaction(callback, options)),
  });
}

/** Errors River raised because an interceptor broke its contract. */
const violations = new WeakSet<object>();

function violation(
  message: string,
  options: {
    readonly cause?: unknown;
    readonly details?: Readonly<Record<string, unknown>>;
  } = {}
): ExtensionError {
  const error = new ExtensionError(message, options);
  violations.add(error);
  return error;
}

/**
 * Whether `error` is River's report of an interceptor breaking its
 * contract, rather than a failure the interceptor itself raised.
 */
export function isContractViolation(error: unknown): boolean {
  return typeof error === "object" && error !== null && violations.has(error);
}

/** @internal An interceptor call with its continuation rules. */
export interface InterceptorCall<Result, Args extends unknown[]> {
  readonly invoke: (
    next: (...args: Args) => Promise<Result>
  ) => PromiseLike<unknown>;
  /** Whether `next` must be called exactly once, or at most once. */
  readonly mode: "once" | "optional";
  readonly operation: string;
  /**
   * Check a result an interceptor produced without calling `next`, for an
   * operation it may replace.
   */
  readonly replacement?: (result: unknown) => Result;
  /** The fields of River's result that an interceptor must not change. */
  readonly snapshot: (value: Result) => unknown;
  readonly standard: (...args: Args) => Promise<Result>;
}

/**
 * @internal Run an interceptor around River's standard operation, enforcing the
 * continuation rules: `next` at most once and only while the interceptor
 * runs, never left in flight, and a result River can trust.
 */
export async function runInterceptor<Result, Args extends unknown[] = []>(
  call: InterceptorCall<Result, Args>
): Promise<Result> {
  const details = { operation: call.operation };
  let settled = false;
  let calls = 0;
  let inFlight: Promise<Result> | undefined;
  let outcome:
    | {
        readonly fields: unknown;
        readonly ok: true;
        readonly value: Result;
      }
    | { readonly ok: false; readonly reason: unknown }
    | undefined;
  const next = (...args: Args): Promise<Result> => {
    if (settled) {
      return Promise.reject(
        violation(
          `${call.operation} interceptor called next() after it settled`,
          { details }
        )
      );
    }
    if (calls > 0) {
      return Promise.reject(
        violation(
          `${call.operation} interceptor called next() more than once`,
          { details }
        )
      );
    }
    calls++;
    let started: Promise<Result>;
    try {
      started = call.standard(...args);
    } catch (error: unknown) {
      started = Promise.reject(error);
    }
    const tracked = started.then(
      (value) => {
        // Snapshot before the interceptor can see, or change, the result.
        outcome = { fields: call.snapshot(value), ok: true, value };
        return value;
      },
      (reason: unknown) => {
        outcome = { ok: false, reason };
        throw reason;
      }
    );
    void tracked.catch(() => undefined);
    inFlight = tracked;
    return tracked;
  };

  let result: unknown;
  let failure: { readonly error: unknown } | undefined;
  const settlement = { early: false };
  try {
    result = await Promise.resolve(call.invoke(next)).finally(() => {
      settled = true;
      settlement.early = inFlight !== undefined && outcome === undefined;
    });
  } catch (error: unknown) {
    failure = { error };
  }
  settled = true;
  // Never let the transaction end with River's operation still running in
  // it, even when the interceptor didn't await it.
  if (inFlight !== undefined) await inFlight.catch(() => undefined);
  if (failure !== undefined) throw failure.error;
  if (settlement.early) {
    throw violation(
      `${call.operation} interceptor settled before its next() continuation did; await it`,
      { details }
    );
  }
  const settledOutcome = outcome;
  if (settledOutcome === undefined) {
    if (call.mode === "once" || call.replacement === undefined) {
      throw violation(
        `${call.operation} interceptor must call next() exactly once`,
        { details }
      );
    }
    return call.replacement(result);
  }
  if (!settledOutcome.ok) {
    // The interceptor swallowed a failure of River's own operation, so it
    // has no result River can accept.
    throw settledOutcome.reason;
  }
  if (result !== settledOutcome.value) {
    throw violation(
      `${call.operation} interceptor must resolve with the result of next()`,
      { details }
    );
  }
  if (!sameFields(settledOutcome.fields, call.snapshot(settledOutcome.value))) {
    throw violation(
      `${call.operation} interceptor changed the result of next()`,
      { details }
    );
  }
  return settledOutcome.value;
}

/** Freeze a result container River dispatches on. */
function protectArray<Item>(items: readonly Item[]): readonly Item[] {
  return Object.isFrozen(items) ? items : Object.freeze([...items]);
}

/** The identity fields of each item of a result list. */
function listFields(items: readonly unknown[]): unknown {
  return items.map((item) => {
    if (typeof item !== "object" || item === null) return item;
    if ("status" in item && "job" in item) {
      const result = item as {
        readonly job: JobRow | null;
        readonly key?: string;
        readonly status: string;
      };
      return [
        item,
        result.status,
        result.key,
        result.job,
        result.job === null ? null : rowFields(result.job),
      ];
    }
    return [item, rowFields(item as JobRow)];
  });
}

/** The fields of a job row River uses to dispatch and fence it. */
function rowFields(row: JobRow | null): unknown {
  if (row === null) return null;
  return [
    row.id,
    row.attempt,
    row.kind,
    row.queue,
    row.state,
    row.attemptedBy.at(-1),
  ];
}

function sameFields(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (!Array.isArray(left) || !Array.isArray(right)) return false;
  if (left.length !== right.length) return false;
  return left.every((value, index) => sameFields(value, right[index]));
}

function driverOptions<Transaction>(
  tx: Transaction | undefined
): { readonly tx: Transaction } | undefined {
  return tx === undefined ? undefined : { tx };
}

function transactionOptions<Transaction>(
  signal: AbortSignal | undefined,
  tx: Transaction | undefined
): { readonly signal?: AbortSignal; readonly tx?: Transaction } {
  return {
    ...(signal === undefined ? {} : { signal }),
    ...(tx === undefined ? {} : { tx }),
  };
}

/**
 * Check rows a `getStuck` interceptor read instead of River: at most
 * `limit` running jobs in ascending ID order after `afterId`, which is how
 * the rescuer pages through them.
 */
function validateStuckRows(
  result: unknown,
  afterId: bigint,
  limit: number
): readonly JobRow[] {
  if (!Array.isArray(result) || result.length > limit) {
    throw violation(
      `getStuck interceptor must resolve with at most ${limit} jobs`
    );
  }
  let previous = afterId;
  for (const row of result as unknown[]) {
    if (
      typeof row !== "object" ||
      row === null ||
      typeof (row as JobRow).id !== "bigint" ||
      (row as JobRow).id <= previous ||
      (row as JobRow).state !== JOB_STATE.running
    ) {
      throw violation(
        "getStuck interceptor must resolve with running jobs in ascending ID order after afterId"
      );
    }
    previous = (row as JobRow).id;
  }
  return protectArray(result as readonly JobRow[]);
}

function validateReplacement(
  replacement: unknown,
  expected: number
): readonly JobInsertParams[] {
  const params =
    typeof replacement === "object" && replacement !== null
      ? (replacement as { readonly params?: unknown }).params
      : undefined;
  if (!Array.isArray(params) || params.length !== expected) {
    throw violation(
      `insert interceptor must replace the ${expected} prepared jobs one for one`
    );
  }
  try {
    params.forEach((item: unknown, index) => {
      validatePreparedRow(item, index, true);
    });
    return Object.freeze([...(params as readonly JobInsertParams[])]);
  } catch (error: unknown) {
    throw violation("insert interceptor passed an invalid replacement job", {
      cause: error,
    });
  }
}

const JOB_STATES: ReadonlySet<string> = new Set(Object.values(JOB_STATE));

/**
 * Check reinserted jobs' stored fields, and snapshot the list.
 *
 * @throws {ValidationError} for a row River can't insert.
 */
export function validatePreparedParams(
  params: readonly unknown[]
): readonly PreparedInsertParams[] {
  if (!Array.isArray(params)) {
    throw new ValidationError("prepared jobs must be an array");
  }
  params.forEach((item, index) => {
    validatePreparedRow(item, index, false);
  });
  return Object.freeze([...(params as readonly PreparedInsertParams[])]);
}

/**
 * Check one row prepared outside River's own preparation. An
 * interceptor's replacement row carries its own `args`; a reinserted row
 * takes its arguments from `encodedArgs` alone.
 */
function validatePreparedRow(
  item: unknown,
  index: number,
  withArgs: boolean
): void {
  const fail = (field: string, expected: string): never => {
    throw new ValidationError(
      `prepared job ${index} ${field} must be ${expected}`,
      { details: { field, index } }
    );
  };
  if (typeof item !== "object" || item === null || Array.isArray(item)) {
    fail("", "an object");
  }
  const row = item as Partial<Record<keyof JobInsertParams, unknown>>;
  if (withArgs) {
    if (!isPlainObject(row.args)) fail("args", "a JSON object");
  } else if ("args" in row) {
    // Arguments come from `encodedArgs` alone.
    fail("args", "omitted");
  }
  if (typeof row.encodedArgs !== "string" || !isJsonText(row.encodedArgs)) {
    fail("encodedArgs", "JSON text");
  }
  if (typeof row.kind !== "string" || row.kind.length === 0) {
    fail("kind", "a non-empty string");
  }
  if (typeof row.queue !== "string" || row.queue.length === 0) {
    fail("queue", "a non-empty string");
  }
  if (!isIntegerBetween(row.maxAttempts, 1, 32_767)) {
    fail("maxAttempts", "an integer from 1 to 32767");
  }
  if (!isIntegerBetween(row.priority, 1, 4)) {
    fail("priority", "an integer from 1 to 4");
  }
  if (!isPlainObject(row.metadata)) fail("metadata", "a JSON object");
  if (
    row.scheduledAt !== undefined &&
    !(row.scheduledAt instanceof Temporal.Instant)
  ) {
    fail("scheduledAt", "a Temporal.Instant or omitted");
  }
  if (
    row.createdAt !== undefined &&
    !(row.createdAt instanceof Temporal.Instant)
  ) {
    fail("createdAt", "a Temporal.Instant or omitted");
  }
  if (typeof row.state !== "string" || !JOB_STATES.has(row.state)) {
    fail("state", "a job state");
  }
  if (
    !Array.isArray(row.tags) ||
    !row.tags.every((tag) => typeof tag === "string")
  ) {
    fail("tags", "an array of strings");
  }
  if (row.uniqueKey !== null && !(row.uniqueKey instanceof Uint8Array)) {
    fail("uniqueKey", "bytes or null");
  }
  if (
    row.uniqueStates !== null &&
    !(
      Array.isArray(row.uniqueStates) &&
      row.uniqueStates.every(
        (state: unknown) =>
          typeof state === "string" && JOB_STATES.has(state as JobState)
      )
    )
  ) {
    fail("uniqueStates", "an array of job states or null");
  }
}

function isIntegerBetween(value: unknown, min: number, max: number): boolean {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= min &&
    value <= max
  );
}

function isJsonText(text: string): boolean {
  try {
    parseJson(text);
    return true;
  } catch {
    return false;
  }
}

function isPlainObject(value: unknown): boolean {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
