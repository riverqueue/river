import type { RiverEvent } from "./events.js";
import type { InsertResult } from "./client.js";
import type { JobDefinition } from "./job-definition.js";
import type { JobState } from "./job.js";
import type { JsonObject, JsonValue } from "./json.js";
import type { PeriodicJobsStartParams } from "./periodic.js";
import type { RiverMetric } from "./metrics.js";
import type { WorkAttemptContext, WorkContext, WorkOutcome } from "./worker.js";

/** A Koa-style around-work extension. `next` may be called exactly once. */
/* eslint-disable @typescript-eslint/no-invalid-void-type -- ordinary and async no-return middleware are valid work handlers */
export type WorkMiddleware = (
  context: WorkAttemptContext,
  next: () => Promise<WorkOutcome | undefined>
) => PromiseLike<WorkOutcome | void> | WorkOutcome | void;
/* eslint-enable @typescript-eslint/no-invalid-void-type */

/** What an {@link InsertMiddleware} sees about the insertion it wraps. */
export interface InsertContext {
  readonly operation: "insert" | "insertMany";
  /** Immutable application-level insertion requests in call order. */
  readonly requests: readonly InsertRequest[];
}

/** Immutable application-level view of an insertion request. */
export interface InsertRequest {
  readonly args: JsonObject;
  /**
   * The job definition the caller inserted (the same object identity), or
   * undefined for an insertion without one.
   */
  readonly definition: JobDefinition | undefined;
  readonly kind: string;
  readonly maxAttempts: number;
  readonly metadata: JsonObject;
  readonly priority: number;
  readonly queue: string;
  /**
   * When the job becomes workable. For a job inserted without a schedule,
   * when the insertion was requested: like River for Go, the database stores
   * its own current time for such a job.
   */
  readonly scheduledAt: Temporal.Instant;
  readonly state: JobState;
  readonly tags: readonly string[];
  readonly unique: boolean;
}

/**
 * Wraps every insertion, like Koa middleware: call `next()` once to insert
 * and return (or adjust) its results.
 *
 * As in River for Go, an insertion without `{ tx }` runs in one transaction
 * River owns, from argument validation through every middleware and hook to
 * the write, so an error thrown anywhere, even after `next()` returns, rolls
 * the jobs back. With `{ tx }`, it runs in the caller's transaction, which
 * the caller commits or rolls back.
 *
 * On SQLite, River holds the database's write lock from the write until it
 * commits, so middleware must not await I/O after `next()` returns, and
 * `afterInsert` hooks must not await I/O at all. River detects most such
 * insertions and fails them, but not every one: see the SQLite driver's
 * documentation. Do the I/O before calling `next()`, or react to committed
 * jobs with `client.subscribe`.
 */
export type InsertMiddleware = (
  context: InsertContext,
  next: () => Promise<readonly InsertResult[]>
) => PromiseLike<readonly InsertResult[]>;

/**
 * Ordered extension points. As in River for Go, insert and work hooks run
 * inside the innermost middleware: insert middleware wraps `beforeInsert`,
 * the database write, and `afterInsert`, and work middleware wraps
 * `beforeWork`, argument decoding, the worker, and `afterWork`. A job whose
 * kind has no worker, or whose row can't be decoded, fails before any
 * middleware or hook runs.
 */
export interface RiverHooks {
  /** Observe inserted jobs, inside the innermost insert middleware. */
  afterInsert?(
    context: InsertContext,
    results: readonly InsertResult[]
  ): PromiseLike<void> | void;
  /** Observe any River event after it is published to subscribers. */
  onEvent?(event: RiverEvent): PromiseLike<void> | void;
  /** Observe a runtime metric without blocking job fetching. */
  onMetric?(metric: RiverMetric): PromiseLike<void> | void;
  /**
   * Observe, or replace by returning another, an attempt's result. The
   * returned result becomes the attempt's result, like River for Go's
   * `HookWorkEnd`; returning nothing keeps it.
   */
  afterWork?(
    context: WorkContext,
    result: WorkAttemptResult
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- returning nothing preserves the prior result
  ): PromiseLike<WorkAttemptResult | void> | WorkAttemptResult | void;
  /**
   * Runs before arguments are decoded and the worker runs. An error it
   * throws becomes the attempt's error, and the worker doesn't run.
   */
  beforeWork?(context: WorkAttemptContext): PromiseLike<void> | void;
  /** Runs before jobs are inserted; an error fails the insertion. */
  beforeInsert?(context: InsertContext): PromiseLike<void> | void;
  /**
   * Runs each time this client becomes leader and starts inserting periodic
   * jobs, with durable records from a periodic job store when one is
   * configured. A rejection is logged and periodic enqueuing retries on the
   * next loop, like Go River's `HookPeriodicJobsStart`.
   */
  onPeriodicJobsStart?(
    params: PeriodicJobsStartParams
  ): PromiseLike<void> | void;
}

interface WorkAttemptResultBase {
  readonly metadata?: JsonObject;
  readonly output?: JsonValue;
}

/** Closed result of one handler attempt, narrowed by `status`. */
export type WorkAttemptResult =
  | (WorkAttemptResultBase & {
      readonly cancel?: never;
      readonly error: unknown;
      readonly outcome?: never;
      readonly status: "cancelled";
    })
  | (WorkAttemptResultBase & {
      readonly cancel?: boolean;
      readonly error: unknown;
      readonly outcome?: never;
      readonly status: "failed";
    })
  | (WorkAttemptResultBase & {
      readonly cancel?: never;
      readonly error?: never;
      readonly outcome?: WorkOutcome;
      readonly status: "succeeded";
    });

/** What an {@link RiverErrorHandler} may ask River to do with a failed job. */
export interface ErrorHandlerResult {
  readonly cancel?: boolean;
}

/** Attempt context visible after work extension processing has finished. */
export type ErrorHandlerContext = Omit<WorkAttemptContext, "setMetadata">;

/**
 * Nonfatal application policy invoked once for each failed attempt,
 * including an attempt that stopped because its timeout expired (the error
 * is then a `JobTimeoutError`). Like River for Go, it isn't invoked
 * for an attempt interrupted by a client stop, or for one that fails after
 * the job was cancelled remotely (the cancellation decides that attempt's
 * outcome), unless the failure is a JavaScript runtime fault such as a
 * `TypeError`, River's analog of a Go panic.
 */
export type RiverErrorHandler = (
  context: ErrorHandlerContext,
  error: unknown
) =>
  ErrorHandlerResult | PromiseLike<ErrorHandlerResult | undefined> | undefined;

/** A named collection of ordinary middleware and hooks. */
export interface RiverPlugin {
  readonly hooks?: RiverHooks;
  readonly insertMiddleware?: readonly InsertMiddleware[];
  readonly middleware?: readonly WorkMiddleware[];
  readonly name: string;
}
