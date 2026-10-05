import type { Client } from "./client.js";
import type { RegisteredTransaction } from "./driver.js";
import type { WorkLogger } from "./logger.js";
import {
  millisecondsToDuration,
  toDuration,
  toMilliseconds,
  type DurationInput,
} from "./internal/duration.js";
import { ConfigurationError, ValidationError } from "./errors.js";
import type { JobDefinition, JobDefinitionArgs } from "./job-definition.js";
import type { JobRow } from "./job.js";
import { isJobArgsTransformPlugin } from "./job-args-transform.js";
import type { JsonObject, JsonValue } from "./json.js";
import { toJsonValue } from "./json.js";
import type { Resumable } from "./resumable.js";
import type { RetryPolicy } from "./runtime/settings.js";
import type { WorkAttemptResult, WorkMiddleware } from "./extensions.js";

/** A worked job with decoded args and transformed JSON before decoding. */
export type Job<Definition extends JobDefinition = JobDefinition> = Omit<
  JobRow,
  "args"
> & {
  readonly args: JobDefinitionArgs<Definition>;
  readonly rawArgs: JsonObject;
};

/** Raw attempt context passed to middleware and pre-work hooks. */
export interface WorkAttemptContext<Transaction = RegisteredTransaction> {
  /** The client working this job, for inserting follow-up jobs. */
  readonly client: Client<Transaction>;
  readonly execution: WorkExecution;
  readonly job: Readonly<JobRow>;
  /**
   * The client's logger with `jobId`, `jobKind`, and `attempt` attached.
   * Call it as `logger.info("message")` or `logger.info({ key }, "message")`.
   */
  readonly logger: WorkLogger;
  /** Record JSON output for this attempt, including on failure or cancellation. */
  readonly recordOutput: (value: JsonValue) => void;
  /** Merge one JSON value into persisted metadata for this attempt. */
  readonly setMetadata: (key: string, value: JsonValue) => void;
  readonly signal: AbortSignal;
}

/** Decoded context passed to a registered job handler. */
export interface WorkContext<
  Definition extends JobDefinition = JobDefinition,
  Transaction = RegisteredTransaction,
> extends Omit<WorkAttemptContext<Transaction>, "job"> {
  /**
   * Complete this exact attempt inside a caller-owned transaction, so the
   * job's completion commits or rolls back with the handler's own writes.
   * `Transaction` defaults to the transaction types of installed drivers.
   */
  readonly completeTx: (
    tx: Transaction,
    options?: { readonly output?: JsonValue }
  ) => Promise<JobRow>;
  readonly job: Job<Definition>;
  /** Named steps whose progress is checkpointed when an attempt fails. */
  readonly resumable: Resumable;
}

/** Identity and timing metadata for one claimed attempt. */
export interface WorkExecution {
  readonly attemptedBy: string;
  readonly startedAt: Temporal.Instant;
}

/** Job-kind-specific work lifecycle hooks. */
export interface WorkerHooks {
  afterWork?(
    context: WorkContext,
    result: WorkAttemptResult
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- returning nothing preserves the prior result
  ): PromiseLike<WorkAttemptResult | void> | WorkAttemptResult | void;
  beforeWork?(context: WorkAttemptContext): PromiseLike<void> | void;
}

/** Named job-kind-specific work extensions. */
export interface WorkerPlugin {
  readonly hooks?: WorkerHooks;
  readonly middleware?: readonly WorkMiddleware[];
  readonly name: string;
}

/**
 * Permanent cancellation without another retry, like Go's `river.JobCancel`.
 * The reason is recorded as the attempt error.
 */
export interface CancelOutcome {
  readonly reason?: string;
  readonly type: "cancel";
}

/** Explicit successful completion, optionally recording JSON output. */
export interface CompleteOutcome {
  readonly output?: JsonValue;
  readonly type: "complete";
}

/** Explicit discard without another retry. */
export interface DiscardOutcome {
  readonly reason?: string;
  readonly type: "discard";
}

/** Reschedule work without consuming an attempt. */
export interface SnoozeOutcome {
  /** How long to snooze, rounded up to whole milliseconds by {@link snooze}. */
  readonly duration: Temporal.Duration;
  readonly type: "snooze";
}

/**
 * An explicit handler outcome, built by `complete()`, `snooze()`,
 * `discard()`, or `cancel()`.
 */
export type WorkOutcome =
  CancelOutcome | CompleteOutcome | DiscardOutcome | SnoozeOutcome;

/** A handler succeeds by returning nothing or an explicit River outcome. */
/* eslint-disable @typescript-eslint/no-invalid-void-type -- ordinary and async no-return handlers are valid */
export type WorkHandler<
  Definition extends JobDefinition,
  Transaction = RegisteredTransaction,
> = (
  context: WorkContext<Definition, Transaction>
) => PromiseLike<WorkOutcome | void> | WorkOutcome | void;
/* eslint-enable @typescript-eslint/no-invalid-void-type */

/**
 * Builds a job's handler from the definition it is registered with, for an
 * integration whose handler needs the definition, such as to decode other
 * jobs of the same kind. `Workers.add` calls `createWorkHandler` once.
 *
 * ```ts
 * workers.add(definition, integrationWorker(options));
 * ```
 */
export interface WorkHandlerFactory<
  Definition extends JobDefinition,
  Transaction = RegisteredTransaction,
> {
  readonly createWorkHandler: (
    definition: Definition
  ) => WorkHandler<Definition, Transaction>;
}

/** Per-handler runtime policy. */
export interface WorkerOptions {
  /** Job-kind-specific hooks, ordered after global hooks. */
  hooks?: WorkerHooks;
  /** Job-kind-specific work middleware, wrapping global middleware. */
  middleware?: readonly WorkMiddleware[];
  /** Named job-kind-specific extensions. */
  plugins?: readonly WorkerPlugin[];
  /**
   * When this worker's failed jobs run next, like River for Go's
   * `Worker.NextRetry`. Consulted before the client's `retryPolicy` once a
   * job's arguments decode, both after a failed attempt and when the rescuer
   * retries a stuck job. When it throws or returns something other than a
   * `Temporal.Instant`, the client's policy decides; a time in the past
   * uses River's default schedule.
   */
  retryPolicy?: RetryPolicy;
  /**
   * Cooperative timeout for this worker's attempts, such as `{ minutes: 5 }`.
   * Overrides the client's `jobTimeout`; `null` disables it.
   */
  timeout?: DurationInput | null;
}

/** A worker's options after validation. */
export interface NormalizedWorkerOptions extends Omit<
  WorkerOptions,
  "timeout"
> {
  /** Validated timeout, or null when disabled for this worker. */
  timeout?: Temporal.Duration | null;
}

interface WorkerRegistrationBase<
  Definition extends JobDefinition = JobDefinition,
> {
  readonly definition: Definition;
  readonly options: Readonly<NormalizedWorkerOptions>;
}

/** An ordinary handler that executes on the River runtime's event loop. */
export interface InProcessWorkerRegistration<
  Definition extends JobDefinition = JobDefinition,
> extends WorkerRegistrationBase<Definition> {
  readonly handler: WorkHandler<Definition>;
  readonly type: "in_process";
}

/** A handler owned by an optional executor integration. */
export interface ExecutorWorkerRegistration<
  Definition extends JobDefinition = JobDefinition,
> extends WorkerRegistrationBase<Definition> {
  readonly target: WorkExecutorTarget;
  readonly type: "executor";
}

/** A worker registered in {@link Workers}, run in process or by an executor. */
export type WorkerRegistration<
  Definition extends JobDefinition = JobDefinition,
> =
  | InProcessWorkerRegistration<Definition>
  | ExecutorWorkerRegistration<Definition>;

/** How River asks a {@link WorkExecutor} to stop one attempt. */
export interface WorkExecutorAbortOptions {
  /**
   * How long the handler has to settle after its signal aborts before the
   * executor may end it by force. River passes the client's
   * `jobStuckThreshold`.
   */
  readonly gracePeriod: Temporal.Duration;
}

/** The result of asking a {@link WorkExecutor} to stop one attempt. */
export interface WorkExecutorAbortResult {
  /**
   * Whether the executor stopped the attempt itself, by removing it before
   * it began or by ending its handler by force, rather than the handler
   * settling on its own. True only once the handler no longer executes.
   * When the abort came from stopping the client, rather than from the
   * job's cancellation or timeout, an attempt ended by force after it began
   * fails with a `JobAbortedError`, so its attempt counts.
   */
  readonly terminated: boolean;
}

/** One running attempt owned by a pluggable executor. */
export interface WorkExecutorHandle {
  readonly result: PromiseLike<WorkOutcome | undefined>;
  /**
   * Settles once the attempt begins executing, for executors that queue
   * attempts for capacity of their own. River arms the job timeout and stuck
   * detection only after it resolves, so waiting for capacity does not spend
   * the attempt's time. Omit it when attempts start immediately.
   */
  readonly started?: PromiseLike<void>;
  /**
   * Abort the handler's signal with `reason`. An executor that can end a
   * handler by force should wait `options.gracePeriod` for it to settle
   * first.
   */
  abort(
    reason: unknown,
    options: WorkExecutorAbortOptions
  ): PromiseLike<WorkExecutorAbortResult>;
}

/**
 * Runs handlers somewhere other than River's own event loop, such as in
 * worker threads (`@riverqueue/worker-threads`).
 *
 * An executor belongs to the application that constructs it and may serve
 * several clients or runtimes. River never closes an executor; stopping a
 * runtime only aborts that runtime's own attempts.
 */
export interface WorkExecutor {
  readonly name: string;
  diagnostics?(): JsonObject;
  start(context: WorkContext, handler: unknown): WorkExecutorHandle;
}

/** Opaque handler registration produced by an optional executor package. */
export interface WorkExecutorTarget {
  readonly executor: WorkExecutor;
  readonly handler: unknown;
}

/** Reads a registry's private registrations; set by {@link Workers}. */
let readRegistration: (
  workers: Workers<never>,
  kind: string
) => WorkerRegistration | undefined;

/**
 * Typed registry of River job handlers.
 *
 * `Transaction` types `ctx.client` and `ctx.completeTx` inside handlers. It
 * defaults to the transaction types of installed drivers; narrow it with
 * `new Workers<PoolClient>()` when a codebase uses one driver.
 */
export class Workers<Transaction = RegisteredTransaction> {
  readonly #registrations = new Map<string, WorkerRegistration>();

  static {
    readRegistration = (workers, kind) => {
      if (!(#registrations in workers)) {
        throw new ConfigurationError(
          "a Workers registry from another installed copy of riverqueue " +
            "can't be used; check `npm ls riverqueue`"
        );
      }
      return workers.#registrations.get(kind);
    };
  }

  /**
   * Register exactly one handler for a job definition: a handler function,
   * or a {@link WorkHandlerFactory} that builds one for the definition.
   */
  add<Definition extends JobDefinition>(
    definition: Definition,
    handler:
      | WorkHandler<Definition, Transaction>
      | WorkHandlerFactory<Definition, Transaction>,
    options: WorkerOptions = {}
  ): this {
    this.#requireUnregistered(definition);

    if (
      typeof handler === "object" &&
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
      handler !== null &&
      typeof handler.createWorkHandler === "function"
    ) {
      handler = handler.createWorkHandler(definition);
    }
    if (typeof handler !== "function") {
      throw new ConfigurationError("worker handler must be a function");
    }

    const normalizedOptions = normalizeWorkerOptions(options);

    this.#register(
      definition,
      Object.freeze({
        definition,
        handler,
        options: Object.freeze(normalizedOptions),
        type: "in_process",
      })
    );
    return this;
  }

  /** Register a handler produced by an optional executor integration. */
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters -- keeps the published signature
  addExecutor<Definition extends JobDefinition>(
    definition: Definition,
    target: WorkExecutorTarget,
    options: WorkerOptions = {}
  ): this {
    this.#requireUnregistered(definition);
    if (
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
      target === null ||
      typeof target !== "object" ||
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
      target.executor === null ||
      typeof target.executor !== "object" ||
      target.executor.name.length === 0 ||
      typeof target.executor.start !== "function"
    ) {
      throw new ConfigurationError("invalid work executor target");
    }

    this.#register(
      definition,
      Object.freeze({
        definition,
        options: Object.freeze(normalizeWorkerOptions(options)),
        target: Object.freeze({ ...target }),
        type: "executor",
      })
    );
    return this;
  }

  /** Return whether this registry contains a job kind. */
  has(kind: string): boolean {
    return this.#registrations.has(kind);
  }

  /**
   * Return registered job kinds, each definition's kind aliases included, in
   * registration order.
   */
  kinds(): readonly string[] {
    return Object.freeze([...this.#registrations.keys()]);
  }

  /** Number of registered job kinds, kind aliases included. */
  get size(): number {
    return this.#registrations.size;
  }

  /** Register a worker under its definition's kind and kind aliases, like Go. */
  #register(definition: JobDefinition, registration: WorkerRegistration): void {
    for (const kind of definitionKinds(definition)) {
      this.#registrations.set(kind, registration);
    }
  }

  #requireUnregistered(definition: JobDefinition): void {
    for (const kind of definitionKinds(definition)) {
      if (this.#registrations.has(kind)) {
        throw new ConfigurationError(
          `worker already registered for job kind ${JSON.stringify(kind)}`
        );
      }
    }
  }
}

/**
 * The registration for `kind` in `workers`, or undefined for an unknown
 * kind, for River's runtime and test packages.
 */
export function workerRegistration(
  workers: Workers<never>,
  kind: string
): WorkerRegistration | undefined {
  return readRegistration(workers, kind);
}

/** A definition's kind followed by its kind aliases. */
function definitionKinds(definition: JobDefinition): readonly string[] {
  return [definition.kind, ...(definition.kindAliases ?? [])];
}

function normalizeWorkerOptions(
  options: WorkerOptions
): NormalizedWorkerOptions {
  const normalizedOptions: NormalizedWorkerOptions = {};
  if (options.hooks !== undefined) {
    normalizedOptions.hooks = Object.freeze({ ...options.hooks });
  }
  if (options.middleware !== undefined) {
    normalizedOptions.middleware = Object.freeze([...options.middleware]);
  }
  if (options.plugins !== undefined) {
    const names = new Set<string>();
    normalizedOptions.plugins = Object.freeze(
      options.plugins.map((plugin) => {
        if (isJobArgsTransformPlugin(plugin)) {
          throw new ConfigurationError(
            "job argument transform plugins must be configured on Client"
          );
        }
        if (plugin.name.length === 0) {
          throw new ConfigurationError("worker plugin name is empty");
        }
        if (names.has(plugin.name)) {
          throw new ConfigurationError(
            `duplicate worker plugin name ${JSON.stringify(plugin.name)}`
          );
        }
        names.add(plugin.name);
        return Object.freeze({
          ...(plugin.hooks === undefined
            ? {}
            : { hooks: Object.freeze({ ...plugin.hooks }) }),
          ...(plugin.middleware === undefined
            ? {}
            : { middleware: Object.freeze([...plugin.middleware]) }),
          name: plugin.name,
        });
      })
    );
  }
  if (options.retryPolicy !== undefined) {
    if (typeof options.retryPolicy !== "function") {
      throw new ConfigurationError("worker retryPolicy must be a function");
    }
    normalizedOptions.retryPolicy = options.retryPolicy;
  }
  if (options.timeout !== undefined) {
    normalizedOptions.timeout =
      options.timeout === null
        ? null
        : toDuration("worker timeout", options.timeout);
  }
  return normalizedOptions;
}

/**
 * Construct an outcome that cancels the job permanently, like Go's
 * `river.JobCancel`. The job moves to `cancelled` without another retry and
 * `JobCancelError: <reason>` is recorded as the attempt error.
 */
export function cancel(
  options: { readonly reason?: string } = {}
): CancelOutcome {
  if (options.reason === undefined) return Object.freeze({ type: "cancel" });
  if (typeof options.reason !== "string" || options.reason.length === 0) {
    throw new ValidationError("cancel reason must be a non-empty string");
  }
  return Object.freeze({ reason: options.reason, type: "cancel" });
}

/** Construct an explicit successful-completion outcome. */
export function complete(
  options: { readonly output?: JsonValue } = {}
): CompleteOutcome {
  if (options.output === undefined) return Object.freeze({ type: "complete" });
  return Object.freeze({
    output: toJsonValue(options.output),
    type: "complete",
  });
}

/** Construct an explicit discard outcome. */
export function discard(
  options: { readonly reason?: string } = {}
): DiscardOutcome {
  if (options.reason === undefined) return Object.freeze({ type: "discard" });
  if (options.reason.length === 0) {
    throw new ValidationError("discard reason must not be empty");
  }
  return Object.freeze({ reason: options.reason, type: "discard" });
}

/**
 * Snooze the job: run it again after `duration` without using an attempt,
 * for example `return snooze({ minutes: 5 })`. Like River for Go, a snooze
 * no longer than the scheduler interval (including zero) is stored as
 * available with its future scheduled time, so it runs on time without
 * waiting for the scheduler.
 */
export function snooze(duration: DurationInput): SnoozeOutcome {
  const milliseconds = toMilliseconds("snooze duration", duration, {
    allowZero: true,
    error: ValidationError,
  });
  return Object.freeze({
    duration: millisecondsToDuration(milliseconds),
    type: "snooze",
  });
}
