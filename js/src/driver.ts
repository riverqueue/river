import type { AttemptError, JobRow, JobState } from "./job.js";
import type { JsonObject, JsonValue } from "./json.js";

/** Semantic result of deleting one job without deleting running work. */
export type JobDeleteResult =
  | { readonly job: JobRow; readonly status: "deleted" }
  | { readonly job: JobRow; readonly status: "running" }
  | { readonly status: "not_found" };

/** Bounded filters for deleting non-running jobs. */
export interface JobDeleteManyParams {
  readonly all: boolean;
  readonly ids: readonly bigint[];
  readonly kinds: readonly string[];
  readonly limit: number;
  readonly priorities: readonly number[];
  readonly queues: readonly string[];
  readonly states: readonly JobState[];
}

/**
 * Where a job list resumes, relative to its ordering: after an ID alone,
 * after a cursor job whose time field is null, or after a cursor job's time.
 */
export type JobListAfter =
  | { readonly id: bigint; readonly kind: "id" }
  | { readonly id: bigint; readonly kind: "nullTime" }
  | {
      readonly id: bigint;
      readonly kind: "time";
      readonly time: Temporal.Instant;
    };

/**
 * Exact keyset boundary passed to full-engine backends. Like River for Go,
 * `time` is the value of the field the list is ordered by, and `null` when
 * ordering by ID or when that field is null for the cursor's job. For a field
 * that can't be null for the listed states, a boundary without a `time`
 * resumes after `id` alone.
 */
export interface JobListCursorValue {
  readonly id: bigint;
  readonly kind: string;
  readonly queue: string;
  readonly sortField: JobListOrderBy;
  readonly time: Temporal.Instant | null;
}

/**
 * How a job list is ordered and where it resumes, which each backend renders
 * as SQL so that every backend orders and pages identically.
 */
export interface JobListKeyset {
  readonly after: JobListAfter | null;
  readonly direction: SortDirection;
  /**
   * Whether the time field may be null for listed jobs. Nulls then sort
   * explicitly last ascending and first descending, Postgres's default, so
   * every backend agrees and cursors can match them.
   */
  readonly nullable: boolean;
  /** The time column ordered before ID, or `null` to order by ID alone. */
  readonly timeField: JobListTimeField | null;
}

/**
 * The field jobs are listed by. `time` is the time field of the first listed
 * state (`scheduled_at` when no state is listed), like River for Go.
 */
export type JobListOrderBy = "finalizedAt" | "id" | "scheduledAt" | "time";

/** A time column a job list can be ordered by. */
export type JobListTimeField = "attempted_at" | "finalized_at" | "scheduled_at";

/** A list order. */
export type SortDirection = "asc" | "desc";

/** Normalized, backend-neutral job list operation. */
export interface JobListParams {
  readonly after: JobListCursorValue | null;
  readonly ids: readonly bigint[];
  readonly kinds: readonly string[];
  readonly limit: number;
  readonly metadata: JsonObject | null;
  readonly priorities: readonly number[];
  readonly queues: readonly string[];
  readonly sortDirection: SortDirection;
  readonly sortField: JobListOrderBy;
  readonly states: readonly JobState[];
  readonly tagsAll: readonly string[];
  readonly tagsAny: readonly string[];
}

/** Job update. Omitted fields leave the job unchanged. */
export interface JobUpdateParams {
  /** Merge these top-level keys into the job's metadata. */
  readonly metadata?: JsonObject;
  /** Set the job's output at `metadata.output`. */
  readonly output?: JsonValue;
}

/** Persisted dynamic queue row. */
export interface QueueRow {
  readonly createdAt: Temporal.Instant;
  readonly metadata: JsonObject;
  readonly name: string;
  readonly pausedAt: Temporal.Instant | null;
  readonly updatedAt: Temporal.Instant;
}

/** Keyset pagination for listing queues, ordered by name. */
export interface QueueListParams {
  readonly limit: number;
  readonly nameAfter: string | null;
}

/** Changes to a queue's persisted settings. */
export interface QueueUpdateParams {
  readonly metadata?: JsonObject;
}

/** One queue and capacity request in an atomic claim operation. */
export interface JobClaimQueue {
  readonly limit: number;
  readonly name: string;
}

/** Which jobs a claim may lock, and the client that claims them. */
export interface JobClaimParams {
  readonly attemptedBy: string;
  /**
   * Claim only jobs of these kinds, filtered before the limit and locking,
   * or jobs of every kind when empty.
   */
  readonly kinds: readonly string[];
  readonly queues: readonly JobClaimQueue[];
}

/**
 * Jobs locked by {@link RuntimeDriver.jobClaim}, in the order they were
 * claimed. Every job has been moved to `running`, including any whose row
 * couldn't be fully decoded, so the runtime must finish an attempt for each
 * of them.
 */
export interface JobClaimResult {
  /**
   * Why rows couldn't be fully decoded, by job ID. Absent or empty when
   * every row decoded. River doesn't work such a job: its attempt fails with
   * the decode error through the normal failure path.
   */
  readonly decodeErrors?: ReadonlyMap<bigint, Error>;
  /**
   * Every claimed job, including those whose rows couldn't be fully decoded,
   * which have the fields that couldn't be decoded left empty (`{}` or
   * `[]`) and their errors in `decodeErrors`.
   */
  readonly jobs: readonly JobRow[];
}

/** Attempt-identity-safe terminal command. */
export interface JobCompletionCommand {
  readonly attempt: number;
  readonly attemptedBy: string;
  /**
   * Persist a `retry` or `snooze` as `available` rather than `retryable` or
   * `scheduled` because its delay is within the scheduler interval, like
   * River's near-future fast path. Producers claim it once `scheduledAt`
   * passes. Whether the attempt is refunded still follows `kind`: a snooze
   * or interruption refunds it and a retry never does.
   */
  readonly available?: boolean;
  readonly error: DriverAttemptError | null;
  /** Captured handler-finish time for terminal transitions; otherwise null. */
  readonly finalizedAt: Temporal.Instant | null;
  readonly id: bigint;
  readonly kind:
    "cancel" | "complete" | "discard" | "interrupt" | "retry" | "snooze";
  /** Atomically merged attempt metadata (resumable checkpoints, etc.). */
  readonly metadata?: JsonObject;
  readonly output: JsonValue | null;
  /** Distinguishes no output update from recording the JSON value `null`. */
  readonly outputSet: boolean;
  readonly scheduledAt: Temporal.Instant | null;
}

/** A backend-neutral notification hint. Notifications are never authoritative. */
export interface RuntimeNotification {
  readonly payload: string;
  readonly topic: "control" | "insert" | "leadership";
}

/**
 * One maintenance leadership term: the client that leads (`leaderId`), when it
 * was elected, and when its lease expires unless renewed. A new election
 * starts a new term with a new `electedAt`.
 */
export interface LeaderTerm {
  readonly electedAt: Temporal.Instant;
  readonly expiresAt: Temporal.Instant;
  readonly leaderId: string;
}

/** A leadership term as passed to leader-fenced driver operations. */
export type RuntimeLeader = LeaderTerm;

/** One semantic transition selected by the stuck-job rescuer. */
export interface RuntimeJobRescue {
  readonly error: AttemptError;
  readonly finalizedAt: Temporal.Instant | null;
  readonly id: bigint;
  readonly scheduledAt: Temporal.Instant;
  readonly state: "cancelled" | "discarded" | "retryable";
}

/** Retention horizons for one bounded leader-owned cleaner pass. */
export interface RuntimeJobCleanupParams {
  readonly cancelledBefore: Temporal.Instant | null;
  readonly completedBefore: Temporal.Instant | null;
  readonly discardedBefore: Temporal.Instant | null;
  readonly limit: number;
  /** Queues whose jobs the pass leaves alone. */
  readonly queuesExcluded?: readonly string[];
}

/**
 * Bounds of one leader-owned maintenance batch. A backend that can cancel
 * database work should stop the batch after `timeoutMs`, like Postgres's
 * `statement_timeout`; `signal` aborts at the timeout or when the leadership
 * term ends.
 */
export interface RuntimeMaintenanceBatch {
  readonly signal: AbortSignal;
  /** The batch's timeout in milliseconds, or `null` for none. */
  readonly timeoutMs: number | null;
}

/** Exact horizons for one leader-owned scheduler pass. */
export interface RuntimeScheduleParams {
  /**
   * The queues among `queues` to send an insert notification for, each once,
   * from the client's insert notification limiter. The pass calls it with
   * the queue of every job it scheduled at or before `notificationHorizon`,
   * and sends the notifications in its own transaction.
   */
  readonly allowInsertNotifications: (
    queues: readonly string[]
  ) => readonly string[];
  readonly limit: number;
  /** Timestamp used for terminal metadata written by this pass. */
  readonly now: Temporal.Instant;
  /** Scheduled jobs at or before this instant may wake waiting producers. */
  readonly notificationHorizon: Temporal.Instant;
  /** Scheduled jobs at or before this instant may be promoted early. */
  readonly scheduledAtHorizon: Temporal.Instant;
}

/**
 * Bounds on a runtime operation's wait to start. `signal` aborts waiting
 * for a connection or lock, such as when the runtime stops during an
 * outage; an operation that has started always finishes, so no write is
 * abandoned half done.
 */
export interface RuntimeWaitOptions {
  readonly signal?: AbortSignal;
}

/**
 * Options for {@link RuntimeDriver.jobClaim}. With `tx`, the claim runs in
 * that transaction, which its owner commits or rolls back; without it, the
 * claim commits on its own.
 */
export interface JobClaimOptions<
  Transaction = unknown,
> extends RuntimeWaitOptions {
  readonly tx?: Transaction;
}

/** An attempt error as a completion command persists it. */
export interface DriverAttemptError {
  readonly at: Temporal.Instant;
  readonly error: string;
  readonly trace: string;
}

/** A completion applies only when the claimed attempt still owns the row. */
export interface JobCompletionResult {
  readonly job: JobRow | null;
  /** The {@link jobCompletionKey} of the command this result answers. */
  readonly key: string;
  readonly status: "applied" | "stale";
}

/**
 * Identify one completion command by its attempt: job ID, attempt number, and
 * the claiming client. Drivers return it as {@link JobCompletionResult.key}
 * so the runtime can match each result to the attempt that produced it.
 */
export function jobCompletionKey(
  command: Pick<JobCompletionCommand, "attempt" | "attemptedBy" | "id">
): string {
  return `${command.id.toString(10)}:${command.attempt}:${command.attemptedBy}`;
}

/** Remote cancellation of an attempt currently owned by this process. */
export interface JobCancellationNotice {
  readonly attemptedBy: string;
  readonly id: bigint;
}

/** Internal exact insertion parameters sent to a first-party adapter. */
export interface JobInsertParams {
  readonly args: JsonObject;
  /**
   * The row's creation time. Omit it, as River's own inserts do, to use the
   * current time. A caller that reinserts a job it took out of River sets it
   * to keep the job's original creation time, like the `CreatedAt` of River
   * for Go's driver insert parameters.
   */
  readonly createdAt?: Temporal.Instant;
  /**
   * The arguments as JSON text, which drivers store instead of re-encoding
   * `args`.
   */
  readonly encodedArgs: string;
  readonly kind: string;
  readonly maxAttempts: number;
  readonly metadata: JsonObject;
  readonly priority: number;
  readonly queue: string;
  /**
   * When the job becomes workable. Omit it, as River does for a job
   * inserted without a schedule, to use the database's current time, like
   * River for Go, so an application clock ahead of the database's doesn't
   * delay the job.
   */
  readonly scheduledAt?: Temporal.Instant;
  readonly state: JobState;
  readonly tags: readonly string[];
  readonly uniqueKey: Uint8Array | null;
  /** Persisted states participating in uniqueness conflicts. */
  readonly uniqueStates: readonly JobState[] | null;
}

/** A row returned by an insertion adapter. */
export interface DriverInsertResult {
  readonly job: JobRow;
  readonly status: "duplicate" | "inserted";
}

/** Options passed to an insertion adapter operation. */
export interface InsertDriverOptions<Transaction = unknown> {
  /** Optional caller-owned transaction for this operation. */
  tx?: Transaction;
}

/** A backend operation may be native-async or synchronously serialized. */
export type BackendResult<T> = PromiseLike<T> | T;

/** Whether a driver supports only insertion or the full worker runtime. */
export type DriverCapability = "insert" | "runtime";

/**
 * Type-level description of a River driver, used by `new Client(driver)` to
 * infer the driver's transaction type and whether it supports the worker
 * runtime. Applications never implement it; first-party drivers declare the
 * marker with `declare readonly "~river"` so it has no runtime cost.
 */
export interface ClientDriver<
  Transaction = unknown,
  Capability extends DriverCapability = DriverCapability,
> {
  /** Type-only marker. It is never set at runtime. */
  readonly "~river"?:
    | {
        readonly capability: Capability;
        readonly transaction: Transaction;
      }
    | undefined;
}

/**
 * Transaction types contributed by installed River drivers.
 *
 * Driver packages augment this interface (for example `@riverqueue/driver-pg`
 * adds node-postgres clients) so worker contexts accept exactly the
 * transaction types of the drivers an application uses. Applications never
 * need to augment it.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type -- augmented by driver packages
export interface RiverTransactionRegistry {}

/**
 * Union of transaction types from installed drivers, or `unknown` when no
 * driver package registered one.
 */
export type RegisteredTransaction = [keyof RiverTransactionRegistry] extends [
  never,
]
  ? unknown
  : RiverTransactionRegistry[keyof RiverTransactionRegistry];

/**
 * Narrow protocol implemented by River's producer adapters.
 *
 * This is not the full runtime database engine boundary. Postgres schema and
 * other backend-specific configuration belong to the adapter constructor.
 */
export interface InsertDriver<
  Transaction = unknown,
  Capability extends DriverCapability = "insert",
> extends ClientDriver<Transaction, Capability> {
  jobInsert(
    params: JobInsertParams,
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<DriverInsertResult>;

  jobInsertMany(
    params: readonly JobInsertParams[],
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<readonly DriverInsertResult[]>;

  /**
   * Send an insert notification for each of `queues`, in `options.tx` when
   * given, like River for Go's `NotifyMany` on its insert topic. Insertion
   * itself notifies nobody: after inserting available jobs, the client calls
   * this in the same transaction for the queues its insert notification
   * limiter allows. A driver without this method sends no insert
   * notifications.
   */
  notifyInsert?(
    queues: readonly string[],
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<void>;

  /**
   * Run one River operation in a transaction, like River for Go's
   * `dbutil.WithTxV`. The client runs a whole insertion in one scope
   * (validation, insert middleware, hooks, and the write), and the leader
   * runs a durable periodic batch together with its next-run times.
   *
   * With `tx`, the operation joins that caller-owned transaction: the driver
   * checks it and calls `callback` with it, and never commits or rolls it
   * back. Without `tx`, the driver opens a transaction River owns, calls
   * `callback` with a value that stands for it, commits when `callback`
   * resolves, and rolls back when it rejects.
   *
   * The value passed to `callback` is only for River operations on this
   * driver, passed as `{ tx }`. For some backends it is an opaque token
   * rather than a usable connection. A driver without this method runs
   * every operation without a scope.
   */
  operationScope?<T>(
    tx: Transaction | undefined,
    callback: (tx: Transaction) => Promise<T>
  ): Promise<T>;
}

/**
 * Unstable semantic contract implemented by first-party full-engine backends.
 *
 * This SPI intentionally exposes no SQL or backend client types. It is public
 * only so separately packaged first-party drivers can implement it; user code
 * must not implement it. Its shape may change in any release before 1.0.
 */
export interface RuntimeDriver<Transaction = unknown> extends InsertDriver<
  Transaction,
  "runtime"
> {
  /** Synchronous capability/configuration preflight before tasks are started. */
  runtimeStartPreflight?(options: {
    readonly maintenance: boolean;
    readonly notifications: boolean;
    readonly reindex: boolean;
  }): void;

  jobCancel(
    id: bigint,
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<JobRow | null>;
  /**
   * Lock available jobs for work, moving them to `running`. A locked row
   * that can't be fully decoded doesn't fail the call; it is returned with
   * its error in `decodeErrors` so the runtime fails its attempt instead of
   * stranding it.
   * `options.signal` aborts only waiting to start: a backend stops waiting
   * for a connection or lock when it aborts, but never abandons a claim
   * that has begun, so no job is claimed by a runtime that stopped. With
   * `options.tx`, the claim runs in that transaction instead of committing
   * on its own.
   */
  jobClaim(
    params: JobClaimParams,
    options?: JobClaimOptions<Transaction>
  ): BackendResult<JobClaimResult>;
  jobCompleteMany(
    commands: readonly JobCompletionCommand[],
    options?: {
      readonly signal?: AbortSignal;
      readonly tx?: Transaction;
    }
  ): BackendResult<readonly JobCompletionResult[]>;
  jobDelete(
    id: bigint,
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<JobDeleteResult>;
  jobDeleteMany(
    params: JobDeleteManyParams,
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<readonly JobRow[]>;
  jobGet(
    id: bigint,
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<JobRow | null>;
  jobList(
    params: JobListParams,
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<readonly JobRow[]>;
  jobRetry(
    id: bigint,
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<JobRow | null>;
  jobUpdate(
    id: bigint,
    params: JobUpdateParams,
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<JobRow | null>;
  queueGet(
    name: string,
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<QueueRow | null>;
  queueList(
    params: QueueListParams,
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<readonly QueueRow[]>;
  queuePause(
    name: string,
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<QueueRow | null>;
  queueResume(
    name: string,
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<QueueRow | null>;
  queueUpdate(
    name: string,
    params: QueueUpdateParams,
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<QueueRow | null>;

  /** Refresh one locally configured queue without replacing its controls. */
  runtimeQueueUpsert?(
    name: string,
    now: Temporal.Instant,
    options?: RuntimeWaitOptions
  ): BackendResult<QueueRow>;

  /**
   * Whether the database delivers notifications to listeners, detecting the
   * server the first time, like River for Go's `InitDriver` followed by
   * `SupportsListener`. A runtime whose database doesn't, such as YugabyteDB
   * without `yb_enable_listen_notify`, polls instead as if `pollOnly` were
   * set. A driver without this method always delivers them.
   */
  runtimeDeliversNotifications?(
    options?: RuntimeWaitOptions
  ): BackendResult<boolean>;

  /** Notification hints for inserts, controls, and leadership changes. */
  runtimeNotificationSubscribe?(
    topics: readonly RuntimeNotification["topic"][],
    signal: AbortSignal,
    ready: () => void
  ): AsyncIterable<RuntimeNotification>;

  /** Broadcast a request for whichever runtime leads to resign its term. */
  runtimeRequestLeadershipResignation?(
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<void>;

  /**
   * Renew `held`, the exact term this client leads, or elect a new term when
   * it holds none. Like Go River, a held term is renewed only while the
   * persisted row still has its `elected_at`, and an unexpired row is never
   * adopted, even one with this client's `leaderId`: another process using
   * the same client ID must lose its term before a fresh one is elected.
   */
  maintenanceLeaderAcquire?(
    leaderId: string,
    now: Temporal.Instant,
    ttlMs: number,
    held: RuntimeLeader | null,
    options?: RuntimeWaitOptions
  ): BackendResult<RuntimeLeader | null>;
  /** Resign only the supplied exact term. */
  maintenanceLeaderResign?(leader: RuntimeLeader): BackendResult<boolean>;
  /** Move one bounded page of due jobs toward availability. */
  maintenanceSchedule?(
    leader: RuntimeLeader,
    params: RuntimeScheduleParams,
    batch?: RuntimeMaintenanceBatch
  ): BackendResult<number>;
  /** Read one stable page of attempts eligible for rescue. */
  maintenanceGetStuck?(
    leader: RuntimeLeader,
    attemptedBefore: Temporal.Instant,
    afterId: bigint,
    limit: number,
    batch?: RuntimeMaintenanceBatch
  ): BackendResult<readonly JobRow[]>;
  /**
   * Rescue a previously inspected page with an attempted-at fence.
   *
   * `attemptedBefore` is the same horizon the pass passed to
   * `maintenanceGetStuck`. Implementations update only rows that are still
   * `running` with `attempted_at` strictly before it, so a job completed,
   * released, or claimed again after it was selected is left untouched.
   * With `options.tx`, the fenced update runs in that transaction.
   */
  maintenanceRescue?(
    leader: RuntimeLeader,
    attemptedBefore: Temporal.Instant,
    jobs: readonly RuntimeJobRescue[],
    options?: InsertDriverOptions<Transaction>
  ): BackendResult<number>;
  /** Delete one bounded page of terminal jobs. */
  maintenanceCleanJobs?(
    leader: RuntimeLeader,
    params: RuntimeJobCleanupParams,
    timeoutMs: number | null,
    signal: AbortSignal
  ): BackendResult<number>;
  /** Delete one bounded page of inactive queue records. */
  maintenanceCleanQueues?(
    leader: RuntimeLeader,
    updatedBefore: Temporal.Instant,
    limit: number,
    batch?: RuntimeMaintenanceBatch
  ): BackendResult<number>;
  /**
   * Delete up to `limit` durable notification hints created before
   * `createdBefore`, oldest first, where applicable.
   */
  maintenanceCleanNotifications?(
    leader: RuntimeLeader,
    createdBefore: Temporal.Instant,
    limit: number,
    batch?: RuntimeMaintenanceBatch
  ): BackendResult<number>;
  /** Rebuild configured backend indexes when the backend supports it. */
  maintenanceReindex?(
    leader: RuntimeLeader,
    indexNames: readonly string[],
    timeoutMs: number | null,
    signal: AbortSignal
  ): BackendResult<number>;

  /**
   * The IDs among `ids` of running jobs with a cancellation request, like
   * River for Go's `JobGetCancelRequested`. A runtime without notifications
   * checks its running attempts with it every two seconds.
   */
  jobGetCancelRequested?(
    ids: readonly bigint[],
    options?: RuntimeWaitOptions
  ): BackendResult<readonly bigint[]>;

  /**
   * Optional backend-native remote-cancellation stream. `ready` runs once the
   * stream receives every later cancellation, so the runtime can check for
   * cancellations missed while it reconnected.
   */
  jobCancellationSubscribe?(
    attemptedBy: string,
    signal: AbortSignal,
    ready?: () => void
  ): AsyncIterable<JobCancellationNotice>;
}
