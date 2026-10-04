/**
 * Stable error categories exposed by River.
 *
 * Every error River throws deliberately is a {@link RiverError} whose `code`
 * is one of these values. Each subclass narrows `code` to its own category, so
 * either `instanceof` or a `switch` on `code` identifies the failure.
 */
export const RIVER_ERROR_CODE = {
  backendMismatch: "backend_mismatch",
  configuration: "configuration",
  database: "database",
  extension: "extension",
  jobAborted: "job_aborted",
  jobAttemptFinished: "job_attempt_finished",
  jobCancelled: "job_cancelled",
  jobRunning: "job_running",
  jobStuck: "job_stuck",
  jobTimeout: "job_timeout",
  lifecycle: "lifecycle",
  migration: "migration",
  payloadValidation: "payload_validation",
  subscriptionLag: "subscription_lag",
  transactionScope: "transaction_scope",
  unknownJobKind: "unknown_job_kind",
  unsupportedCapability: "unsupported_capability",
  validation: "validation",
} as const;

/** One of River's stable error categories. */
export type RiverErrorCode =
  (typeof RIVER_ERROR_CODE)[keyof typeof RIVER_ERROR_CODE];

/** Options accepted by {@link RiverError} and its subclasses. */
export interface RiverErrorOptions<
  Code extends string = RiverErrorCode,
> extends ErrorOptions {
  /** Stable error category. */
  code: Code;
  /** Structured, secret-free context such as an operation or job ID. */
  details?: Readonly<Record<string, unknown>>;
  /** Whether retrying the failed operation is known to be safe. */
  retryable?: boolean;
}

/** Options accepted by River error subclasses, which fix their own `code`. */
export type RiverErrorSubclassOptions = Omit<RiverErrorOptions, "code">;

/**
 * Base class for every error River throws deliberately.
 *
 * Catch this class to handle all River failures, including database,
 * migration, and job-abort errors. Internal invariant violations remain
 * ordinary `Error` values so they stay visibly different.
 *
 * A companion package roots its errors here too, so one `instanceof
 * RiverError` catches them. Where one of River's categories fits, it
 * extends that subclass, such as {@link ValidationError}. Otherwise it
 * extends `RiverError` with codes of its own, prefixed with its name and a
 * dot, such as `"example.cycle"`, so they never collide with River's
 * unprefixed codes; code switching on `code` keeps a default branch for
 * such codes.
 */
export class RiverError<Code extends string = RiverErrorCode> extends Error {
  /** Stable error category. */
  readonly code: Code;
  /** Structured, secret-free context such as an operation or job ID. */
  readonly details?: Readonly<Record<string, unknown>>;
  /** Whether retrying the failed operation is known to be safe. */
  readonly retryable: boolean;

  constructor(message: string, options: RiverErrorOptions<Code>) {
    super(message, { cause: options.cause });
    this.name = "RiverError";
    this.code = options.code;
    this.retryable = options.retryable ?? false;
    if (options.details !== undefined) this.details = options.details;
  }
}

/** Whether retrying a failed River operation is explicitly safe. */
export function isRetryableError(
  error: unknown
): error is RiverError & { readonly retryable: true } {
  return error instanceof RiverError && error.retryable;
}

/** The selected backend cannot perform an operation required by the client. */
export class UnsupportedCapabilityError extends RiverError<"unsupported_capability"> {
  /** Backend that lacks the capability, such as `"prisma"`. */
  readonly backend: string;
  /** Capability that was requested, such as `"runtime"`. */
  readonly capability: string;

  constructor(
    backend: string,
    capability: string,
    options: RiverErrorSubclassOptions & { message?: string } = {}
  ) {
    const { message, ...rest } = options;
    super(
      message ??
        `backend ${JSON.stringify(backend)} does not support ${JSON.stringify(capability)}`,
      {
        ...rest,
        code: RIVER_ERROR_CODE.unsupportedCapability,
        details: { ...rest.details, backend, capability },
      }
    );
    this.name = "UnsupportedCapabilityError";
    this.backend = backend;
    this.capability = capability;
  }
}

/** An operation received a transaction belonging to another backend. */
export class BackendMismatchError extends RiverError<"backend_mismatch"> {
  /** Backend whose operation rejected the transaction. */
  readonly backend: string;

  constructor(
    backend: string,
    message: string,
    options: RiverErrorSubclassOptions = {}
  ) {
    super(message, {
      ...options,
      code: RIVER_ERROR_CODE.backendMismatch,
      details: { ...options.details, backend },
    });
    this.name = "BackendMismatchError";
    this.backend = backend;
  }
}

/** Options for {@link DatabaseOperationError}. */
export interface DatabaseOperationErrorOptions extends RiverErrorSubclassOptions {
  /** Backend that failed, such as `"postgres"` or `"sqlite"`. */
  backend: string;
  /** River operation that failed, such as `"jobInsert"`. */
  operation: string;
}

/**
 * A database operation failed.
 *
 * The message never contains SQL text, bound values, or credentials; the
 * underlying driver error is available as `cause`. `retryable` is true when
 * the failure is transient (a lost connection, serialization failure, lock or
 * statement timeout, or a busy SQLite database), so the whole operation can
 * safely be attempted again.
 */
export class DatabaseOperationError extends RiverError<"database"> {
  /** Backend that failed, such as `"postgres"` or `"sqlite"`. */
  readonly backend: string;
  /** River operation that failed, such as `"jobInsert"`. */
  readonly operation: string;

  constructor(message: string, options: DatabaseOperationErrorOptions) {
    const { backend, operation, ...rest } = options;
    super(message, {
      ...rest,
      code: RIVER_ERROR_CODE.database,
      details: { ...rest.details, backend, operation },
    });
    this.name = "DatabaseOperationError";
    this.backend = backend;
    this.operation = operation;
  }
}

/** Options for {@link MigrationError}. */
export interface MigrationErrorOptions extends RiverErrorSubclassOptions {
  /** Backend being migrated, such as `"postgres"` or `"sqlite"`. */
  backend: string;
  /** Migration operation that failed, such as `"migrate"` or `"validate"`. */
  operation: string;
}

/** A migration could not be planned, applied, or validated. */
export class MigrationError extends RiverError<"migration"> {
  /** Backend being migrated, such as `"postgres"` or `"sqlite"`. */
  readonly backend: string;
  /** Migration operation that failed, such as `"migrate"` or `"validate"`. */
  readonly operation: string;

  constructor(message: string, options: MigrationErrorOptions) {
    const { backend, operation, ...rest } = options;
    super(message, {
      ...rest,
      code: RIVER_ERROR_CODE.migration,
      details: { ...rest.details, backend, operation },
    });
    this.name = "MigrationError";
    this.backend = backend;
    this.operation = operation;
  }
}

/** Runtime lifecycle or supervised background failure. */
export class LifecycleError extends RiverError<"lifecycle"> {
  constructor(message: string, options: RiverErrorSubclassOptions = {}) {
    super(message, { ...options, code: RIVER_ERROR_CODE.lifecycle });
    this.name = "LifecycleError";
  }
}

/** A protected running job cannot be deleted while its attempt is active. */
export class JobRunningError extends RiverError<"job_running"> {
  readonly jobId: bigint;

  constructor(jobId: bigint) {
    super(`River job ${jobId} is running and cannot be deleted`, {
      code: RIVER_ERROR_CODE.jobRunning,
      details: { jobId: jobId.toString(10) },
    });
    this.name = "JobRunningError";
    this.jobId = jobId;
  }
}

/**
 * The error an attempt fails with when its handler ignored the aborted
 * `signal` of a stopping client, such as `stop({ mode: "cancel" })`, until
 * its executor ended it by force after the client's `jobStuckThreshold`,
 * such as by terminating its worker thread. Unlike a handler that stops
 * because of the abort, the attempt counts, and the retry policy and
 * `maxAttempts` apply. `cause` is the abort reason the handler ignored.
 */
export class JobAbortedError extends RiverError<"job_aborted"> {
  readonly jobId: bigint;

  constructor(jobId: bigint, options: ErrorOptions = {}) {
    super("job aborted after ignoring cancellation", {
      cause: options.cause,
      code: RIVER_ERROR_CODE.jobAborted,
      details: { jobId: jobId.toString(10) },
    });
    this.name = "JobAbortedError";
    this.jobId = jobId;
  }
}

/**
 * Abort reason delivered to a handler's `signal` once its attempt finished,
 * like River for Go cancelling a job's context when its executor returns,
 * so work the handler left running stops instead of outliving the attempt.
 */
export class JobAttemptFinishedError extends RiverError<"job_attempt_finished"> {
  readonly jobId: bigint;

  constructor(jobId: bigint) {
    super(`River job ${jobId}'s attempt finished`, {
      code: RIVER_ERROR_CODE.jobAttemptFinished,
      details: { jobId: jobId.toString(10) },
    });
    this.name = "JobAttemptFinishedError";
    this.jobId = jobId;
  }
}

/**
 * Abort reason delivered to a handler's `signal` when its job is cancelled
 * remotely, for example with `client.cancel(id)` from any River client.
 */
export class JobCancelledError extends RiverError<"job_cancelled"> {
  readonly jobId: bigint;

  constructor(jobId: bigint) {
    super(`River job ${jobId} was cancelled`, {
      code: RIVER_ERROR_CODE.jobCancelled,
      details: { jobId: jobId.toString(10) },
    });
    this.name = "JobCancelledError";
    this.jobId = jobId;
  }
}

/** Abort reason delivered to a handler when its cooperative timeout expires. */
export class JobTimeoutError extends RiverError<"job_timeout"> {
  readonly jobId: bigint;
  readonly timeout: Temporal.Duration;

  constructor(jobId: bigint, timeout: Temporal.Duration) {
    super(
      `River job ${jobId} exceeded its ${formatMilliseconds(timeout)} timeout`,
      {
        code: RIVER_ERROR_CODE.jobTimeout,
        details: { jobId: jobId.toString(10), timeout: timeout.toString() },
      }
    );
    this.name = "JobTimeoutError";
    this.jobId = jobId;
    this.timeout = timeout;
  }
}

/** Observation reported when an attempt remains unsettled past its threshold. */
export class JobStuckError extends RiverError<"job_stuck"> {
  readonly jobId: bigint;
  /** Time the attempt stayed unsettled after its timeout. */
  readonly threshold: Temporal.Duration;
  /** Timeout that expired before the attempt became stuck. */
  readonly timeout: Temporal.Duration;

  constructor(
    jobId: bigint,
    timeout: Temporal.Duration,
    threshold: Temporal.Duration
  ) {
    super(
      `River job ${jobId} remained unsettled for ${formatMilliseconds(threshold)} after its ${formatMilliseconds(timeout)} timeout`,
      {
        code: RIVER_ERROR_CODE.jobStuck,
        details: {
          jobId: jobId.toString(10),
          threshold: threshold.toString(),
          timeout: timeout.toString(),
        },
      }
    );
    this.name = "JobStuckError";
    this.jobId = jobId;
    this.threshold = threshold;
    this.timeout = timeout;
  }
}

/** A runtime claimed a job whose kind has no registered worker. */
export class UnknownJobKindError extends RiverError<"unknown_job_kind"> {
  readonly kind: string;

  constructor(kind: string) {
    super(
      `job kind is not registered in the client's Workers bundle: ${kind}`,
      {
        code: RIVER_ERROR_CODE.unknownJobKind,
        details: { kind },
      }
    );
    this.name = "UnknownJobKindError";
    this.kind = kind;
  }
}

/** A work middleware, hook, or plugin failed. */
export class ExtensionError extends RiverError<"extension"> {
  constructor(message: string, options: RiverErrorSubclassOptions = {}) {
    super(message, { ...options, code: RIVER_ERROR_CODE.extension });
    this.name = "ExtensionError";
  }
}

/** A bounded subscription dropped events because its consumer fell behind. */
export class SubscriptionLagError extends RiverError<"subscription_lag"> {
  /** Number of events dropped since the previous delivered event. */
  readonly dropped: number;

  constructor(dropped: number) {
    super(`River subscription dropped ${dropped} event(s)`, {
      code: RIVER_ERROR_CODE.subscriptionLag,
      details: { dropped },
    });
    this.name = "SubscriptionLagError";
    this.dropped = dropped;
  }
}

/** Why a {@link TransactionScopeError} was thrown. */
export type TransactionScopeErrorReason =
  /**
   * River's own SQLite transaction stayed open across a turn of the event
   * loop, because insert middleware or a hook awaited I/O while River held
   * SQLite's write lock. River rolled the operation back to release it.
   */
  | "event_loop_turn"
  /** A transaction was begun inside one that is already open. */
  | "nested"
  /** A value passed as `{ tx }` has no open transaction. */
  | "no_transaction"
  /**
   * River was called without `{ tx }` from inside a transaction that the
   * call would have to wait for, such as River's own transaction around an
   * insertion, which its insert middleware and hooks run inside.
   */
  | "reentrant";

/**
 * A database transaction was used in a way that can't work, such as calling
 * River without `{ tx }` from inside insert middleware while River's own
 * transaction holds SQLite's write lock. River fails the call at once
 * instead of waiting for a lock that can't be released. Retrying the same
 * code fails the same way.
 */
export class TransactionScopeError extends RiverError<"transaction_scope"> {
  /** What was wrong with the transaction's use. */
  readonly reason: TransactionScopeErrorReason;

  constructor(
    reason: TransactionScopeErrorReason,
    message: string,
    options: RiverErrorSubclassOptions = {}
  ) {
    super(message, {
      ...options,
      code: RIVER_ERROR_CODE.transactionScope,
      details: { ...options.details, reason },
    });
    this.name = "TransactionScopeError";
    this.reason = reason;
  }
}

/** An invalid static client, job, driver, or worker configuration. */
export class ConfigurationError extends RiverError<"configuration"> {
  constructor(message: string, options: RiverErrorSubclassOptions = {}) {
    super(message, { ...options, code: RIVER_ERROR_CODE.configuration });
    this.name = "ConfigurationError";
  }
}

/** A value rejected at a public River boundary. */
export class ValidationError extends RiverError<"validation"> {
  constructor(message: string, options: RiverErrorSubclassOptions = {}) {
    super(message, { ...options, code: RIVER_ERROR_CODE.validation });
    this.name = "ValidationError";
  }
}

/** Where a job payload failed validation. */
export type PayloadValidationPhase = "insert" | "work";

/**
 * Job arguments rejected by their job definition's schema or decoder.
 *
 * `phase` is `"insert"` when a producer passed invalid arguments and `"work"`
 * when a persisted job (possibly inserted by another language or an older
 * producer) failed validation before its handler ran.
 */
export class PayloadValidationError extends RiverError<"payload_validation"> {
  /** Kind of the job whose arguments were rejected. */
  readonly kind: string;
  /** Whether validation failed while inserting or before working. */
  readonly phase: PayloadValidationPhase;

  constructor(
    kind: string,
    phase: PayloadValidationPhase,
    message: string,
    options: RiverErrorSubclassOptions = {}
  ) {
    super(message, {
      ...options,
      code: RIVER_ERROR_CODE.payloadValidation,
      details: { ...options.details, kind, phase },
    });
    this.name = "PayloadValidationError";
    this.kind = kind;
    this.phase = phase;
  }
}

/** A duration in milliseconds for an error message, such as `1500 ms`. */
function formatMilliseconds(duration: Temporal.Duration): string {
  return `${duration.total("milliseconds")} ms`;
}
