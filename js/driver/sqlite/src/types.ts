import type { DatabaseSync } from "node:sqlite";
import { JOB_STATE } from "riverqueue";
import type {
  AttemptError,
  DurationInput,
  JobRow,
  JobState,
  JsonObject,
  JsonValue,
} from "riverqueue";

/** Persisted River job states in their protocol bit order. */
export const SQLITE_JOB_STATE = JOB_STATE;

export type SqliteJobState = JobState;

/** Values accepted by River's JSON persistence boundary. */
export type SqliteJsonValue = JsonValue;

/** A JSON object accepted by River's SQLite backend. */
export type SqliteJsonObject = JsonObject;

/** One persisted work-attempt failure. */
export type SqliteAttemptError = AttemptError;

/** Exact properties of a River job read from SQLite. */
export type SqliteJobRow<TArgs extends SqliteJsonObject = SqliteJsonObject> =
  JobRow<TArgs>;

/** Exact properties of a River queue read from SQLite. */
export interface SqliteQueueRow {
  createdAt: Temporal.Instant;
  metadata: SqliteJsonObject;
  name: string;
  pausedAt: Temporal.Instant | null;
  updatedAt: Temporal.Instant;
}

/** One durable notification-outbox record. */
export interface SqliteNotification {
  createdAt: Temporal.Instant;
  id: bigint;
  payload: string;
  topic: string;
}

/** The elected portable River leader and its fenced term. */
export interface SqliteLeader {
  electedAt: Temporal.Instant;
  expiresAt: Temporal.Instant;
  leaderId: string;
}

/** One stuck-job rescue transition. */
export interface SqliteRescueJobParams {
  error: SqliteAttemptError;
  finalizedAt?: Temporal.Instant | null;
  id: bigint;
  scheduledAt: Temporal.Instant;
  state: "cancelled" | "discarded" | "retryable";
}

/** Retention horizons for a bounded cleaner pass. */
export interface SqliteCleanupJobsParams {
  cancelledBefore: Temporal.Instant | null;
  completedBefore: Temporal.Instant | null;
  discardedBefore: Temporal.Instant | null;
  limit?: number;
  metadataExclusions?: readonly string[];
  /** Queues whose jobs the pass keeps, even when they're also included. */
  queuesExcluded?: readonly string[];
  /**
   * Queues the pass is limited to. Absent or `null` matches every queue,
   * while an empty list matches none.
   */
  queuesIncluded?: readonly string[] | null;
}

/** Result of moving a due scheduled/retryable job toward availability. */
export interface SqliteScheduleResult {
  conflictDiscarded: boolean;
  job: SqliteJobRow;
}

/** Already-resolved semantic inputs for one River job insertion. */
export interface SqliteInsertJobParams<
  TArgs extends SqliteJsonObject = SqliteJsonObject,
> {
  args: TArgs;
  attempt?: number;
  attemptedAt?: Temporal.Instant | null;
  attemptedBy?: readonly string[];
  createdAt?: Temporal.Instant;
  /** Stored instead of `args` when set, like Go's `EncodedArgs`. */
  encodedArgs?: string;
  errors?: readonly SqliteAttemptError[];
  finalizedAt?: Temporal.Instant | null;
  id?: bigint;
  kind: string;
  maxAttempts?: number;
  metadata?: SqliteJsonObject;
  priority?: number;
  queue?: string;
  scheduledAt?: Temporal.Instant;
  state?: SqliteJobState;
  tags?: readonly string[];
  uniqueKey?: Uint8Array | null;
  uniqueStates?: readonly SqliteJobState[] | null;
}

/** Result of one insert, including a unique-key conflict. */
export interface SqliteInsertResult<
  TArgs extends SqliteJsonObject = SqliteJsonObject,
> {
  job: SqliteJobRow<TArgs>;
  status: "duplicate" | "inserted";
}

export type SqliteCancelResult =
  | { job: SqliteJobRow; status: "cancelled" | "unchanged" }
  | { status: "not_found" };

export type SqliteDeleteResult =
  | { job: SqliteJobRow; status: "deleted" }
  | { job: SqliteJobRow; status: "running" }
  | { status: "not_found" };

export type SqliteRetryResult =
  | { job: SqliteJobRow; status: "retried" | "unchanged" }
  | { status: "not_found" };

declare const SQLITE_RIVER_SCOPE: unique symbol;

/**
 * River's own transaction on its private SQLite connection, as River passes
 * it between its own operations. It is opaque, never reaches application
 * code, and isn't exported from the package.
 */
export interface SqliteRiverScope {
  readonly [SQLITE_RIVER_SCOPE]: true;
}

export interface SqliteOperationOptions {
  /**
   * Run in this transaction: an application handle on the driver's
   * database with a transaction open, or River's own transaction.
   */
  tx?: DatabaseSync | SqliteRiverScope;
}

/** SQLite setup owned by the backend, not by the common River client. */
export interface SqliteDriverOptions {
  /**
   * Total time a River operation keeps retrying while another connection or
   * process holds SQLite's lock, such as `{ seconds: 5 }`. Defaults to 5
   * seconds, the busy timeout River's Go conformance adapter and Rust
   * implementation configure.
   *
   * River's private connection has a zero `busy_timeout`, so SQLite never
   * blocks the event loop waiting for a lock. Between attempts River waits
   * asynchronously with exponential backoff, so timers and I/O keep
   * running. When the bound is exceeded the operation fails with a
   * `DatabaseOperationError` whose `retryable` is `true`.
   */
  busyTimeout?: DurationInput;
}
