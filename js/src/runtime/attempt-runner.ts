/**
 * The attempt runner: builds each attempt's work context, runs it through
 * middleware, hooks, and the configured executor, enforces timeouts and
 * reports stuck attempts, and reconciles the result with any abort that
 * raced it before handing it to the completion pipeline.
 */
import type { RuntimeDriver, RuntimeJobRescue } from "../driver.js";
import {
  JobAbortedError,
  JobAttemptFinishedError,
  JobCancelledError,
  JobStuckError,
  JobTimeoutError,
  UnknownJobKindError,
  ValidationError,
} from "../errors.js";
import type {
  RiverErrorHandler,
  RiverHooks,
  WorkAttemptResult,
  WorkMiddleware,
} from "../extensions.js";
import {
  LinkedAbortSignal,
  raceWithAbort,
  unrefTimeout,
} from "../internal/abort.js";
import type { AttemptExecutionHandle } from "../internal/attempt-executor.js";
import { AttemptExecutor } from "../internal/attempt-executor.js";
import {
  millisecondsToDuration,
  toMilliseconds,
} from "../internal/duration.js";
import type { JobArgsTransformer } from "../job-args-transform.js";
import { transformJobArgsForRead } from "../job-args-transform.js";
import { decodeJobArgs } from "../job-definition.js";
import type { JobRow } from "../job.js";
import type { JsonObject } from "../json.js";
import { toJsonObject } from "../json.js";
import type { Logger } from "../logger.js";
import { createWorkLogger } from "../logger.js";
import type {
  WorkAttemptContext,
  WorkContext,
  WorkerRegistration,
} from "../worker.js";
import { workerRegistration, type Workers } from "../worker.js";
import {
  attemptData,
  composeMiddleware,
  isPlainCompletion,
  normalizeWorkAttemptResult,
  succeededResult,
  thrownResult,
} from "./attempt-result.js";
import type { CompletionPipeline } from "./completion-pipeline.js";
import type { RuntimeContext } from "./context.js";
import { canonicalError, describeError, isRuntimeFault } from "./failures.js";
import type { PeerAttempts } from "./peer-attempts.js";
import type { JobStuckHandler, RetryPolicy } from "./settings.js";
import { validatePlugins } from "./settings.js";
import type { WorkOutputState } from "./work-context.js";
import {
  beginWorkAttempt,
  errorHandlerContext,
  makeDecodedWorkContext,
  normalizeOutput,
  publishWorkResult,
  resultWithMetadata,
  resultWithOutput,
  runInWorkContext,
  setWorkMetadata,
  shareWorkAttempt,
} from "./work-context.js";

/** Configuration for an {@link AttemptRunner}. */
export interface AttemptRunnerOptions {
  readonly completions: CompletionPipeline;
  readonly errorHandler: RiverErrorHandler | undefined;
  /** Client-level hooks, plugin hooks first. */
  readonly hooks: readonly RiverHooks[];
  readonly jobArgsTransformers: readonly Readonly<JobArgsTransformer>[];
  /**
   * Wait after a timeout before reporting a stuck attempt, and after
   * aborting a handler before an executor may end it by force.
   */
  readonly jobStuckThresholdMs: number;
  /** Default job timeout; null disables it. */
  readonly jobTimeoutMs: number | null;
  /** Client-level work middleware, plugin middleware first. */
  readonly middleware: readonly WorkMiddleware[];
  /** The peers of attempts, when the client's pilot can claim them. */
  readonly peers?: PeerAttempts | undefined;
  /** Aborts every attempt when the runtime cancels its work. */
  readonly runSignal: AbortSignal;
  readonly stuckHandler: JobStuckHandler | undefined;
  readonly workLogger: Logger;
  readonly workers: Workers;
}

/** IDs per cancellation check query, like River for Go's producer. */
const CANCEL_POLL_BATCH_SIZE = 1_000;

/** Bound on each cancellation check query, like River for Go's producer. */
const CANCEL_POLL_TIMEOUT_MS = 10_000;

/** Runs claimed jobs and tracks the attempts in flight. */
export class AttemptRunner {
  readonly #attemptExecutor = new AttemptExecutor();
  /** In-flight attempts by `attemptKey`. */
  readonly #attempts = new Map<string, AbortController>();
  /**
   * For each claim in flight, the `attemptKey`s of cancellations that
   * found no attempt running here.
   */
  readonly #claimCancellations = new Set<Set<string>>();
  readonly #completions: CompletionPipeline;
  readonly #context: RuntimeContext;
  readonly #errorHandler: RiverErrorHandler | undefined;
  readonly #hooks: readonly RiverHooks[];
  readonly #jobArgsTransformers: readonly Readonly<JobArgsTransformer>[];
  readonly #jobStuckThresholdMs: number;
  readonly #jobTimeoutMs: number | null;
  readonly #middleware: readonly WorkMiddleware[];
  readonly #peers: PeerAttempts | undefined;
  readonly #runSignal: AbortSignal;
  readonly #stuckHandler: JobStuckHandler | undefined;
  /** Attempts currently reported stuck. */
  #stuckJobs = 0;
  readonly #workLogger: Logger;
  readonly #workers: Workers;

  constructor(context: RuntimeContext, options: AttemptRunnerOptions) {
    this.#completions = options.completions;
    this.#context = context;
    this.#errorHandler = options.errorHandler;
    this.#hooks = options.hooks;
    this.#jobArgsTransformers = options.jobArgsTransformers;
    this.#jobStuckThresholdMs = options.jobStuckThresholdMs;
    this.#jobTimeoutMs = options.jobTimeoutMs;
    this.#middleware = options.middleware;
    this.#peers = options.peers;
    this.#runSignal = options.runSignal;
    this.#stuckHandler = options.stuckHandler;
    this.#workLogger = options.workLogger;
    this.#workers = options.workers;
  }

  /** Attempts currently running in this process. */
  get activeAttempts(): number {
    return this.#attempts.size;
  }

  /** Abort every in-flight attempt with `reason`. */
  abortAttempts(reason: unknown): void {
    for (const controller of this.#attempts.values()) controller.abort(reason);
  }

  /** Cancel the attempt of job `id` that `attemptedBy` owns, if it runs here. */
  cancelAttempt(id: bigint, attemptedBy: string): void {
    const key = attemptKey(id, attemptedBy);
    const attempt = this.#attempts.get(key);
    if (attempt !== undefined) {
      attempt.abort(new JobCancelledError(id));
      return;
    }
    for (const pending of this.#claimCancellations) pending.add(key);
  }

  /**
   * Cancel every attempt this client runs whose job was cancelled while
   * cancellation notices couldn't arrive, such as while a notification
   * stream was reconnecting. Reads the attempts' jobs like
   * {@link pollCancellations}, in batches of 1,000 IDs each bounded by ten
   * seconds, and stops when `signal` aborts.
   */
  async recoverCancellations(signal: AbortSignal): Promise<void> {
    const clientId = this.#context.clientId;
    const driver = this.#context.driver;
    const requested = driver.jobGetCancelRequested?.bind(driver);
    const ids = this.#ownAttemptIds();
    if (requested === undefined) {
      // A backend without the batched read checks each job's row.
      for (const id of ids) {
        signal.throwIfAborted();
        const job = await raceWithAbort(driver.jobGet(id), signal);
        if (
          job?.state === "running" &&
          job.attemptedBy.at(-1) === clientId &&
          job.metadata.cancel_attempted_at !== undefined
        ) {
          this.cancelAttempt(id, clientId);
        }
      }
      return;
    }
    await this.#cancelRequested(requested, ids, signal);
  }

  /**
   * Cancel this client's running attempts whose jobs have a cancellation
   * request, checking every `intervalMs` until `signal` aborts, like River
   * for Go's producers without a notifier. Each check reads the attempts'
   * jobs in batches of 1,000 IDs, each bounded by ten seconds, and reads
   * nothing while no attempt runs. A failed check is logged and the next
   * one tries again.
   */
  async pollCancellations(
    intervalMs: number,
    signal: AbortSignal
  ): Promise<void> {
    const driver = this.#context.driver;
    const requested = driver.jobGetCancelRequested?.bind(driver);
    if (requested === undefined) return;
    for (;;) {
      try {
        await this.#context.timer.delay(intervalMs, signal);
      } catch {
        return;
      }
      try {
        await this.#cancelRequested(requested, this.#ownAttemptIds(), signal);
      } catch (error: unknown) {
        if (signal.aborted) return;
        this.#context.logger.error(
          "River could not check running jobs for cancellation requests",
          { error: describeError(error) }
        );
      }
    }
  }

  /** Cancel this process's attempts of a job just cancelled through this client. */
  cancelLocal(job: JobRow): void {
    const keyPrefix = `${job.id.toString(10)}:`;
    for (const [key, controller] of this.#attempts) {
      if (key.startsWith(keyPrefix)) {
        controller.abort(new JobCancelledError(job.id));
      }
    }
    // The job may be in a claim still in flight here.
    const key = attemptKey(job.id, this.#context.clientId);
    if (!this.#attempts.has(key)) {
      for (const pending of this.#claimCancellations) pending.add(key);
    }
  }

  /** Diagnostics reported by each work executor in use. */
  executorDiagnostics(): Readonly<Record<string, JsonObject>> {
    return this.#attemptExecutor.diagnostics();
  }

  /**
   * Decide how the rescuer recovers a stuck job, like River's JobRescuer:
   * cancel it after a cancellation request, discard an unknown kind, and
   * otherwise retry or discard it once its timeout has passed or its
   * payload no longer decodes. Returns null to leave it running.
   */
  async rescue(
    job: JobRow,
    now: Temporal.Instant,
    signal: AbortSignal
  ): Promise<RuntimeJobRescue | null> {
    signal.throwIfAborted();
    const error = {
      at: now,
      attempt: Math.max(job.attempt, 0),
      error: "Stuck job rescued by JobRescuer",
      trace: "",
    };
    if (job.metadata.cancel_attempted_at !== undefined) {
      return {
        error,
        finalizedAt: now,
        id: job.id,
        scheduledAt: job.scheduledAt,
        state: "cancelled",
      };
    }

    const registration = workerRegistration(this.#workers, job.kind);
    if (registration === undefined) {
      return {
        error,
        finalizedAt: now,
        id: job.id,
        scheduledAt: job.scheduledAt,
        state: "discarded",
      };
    }

    let payloadInvalid = false;
    let retryPolicyJob = job;
    try {
      const transformedJob = this.transformJobArgs(job);
      retryPolicyJob = transformedJob;
      signal.throwIfAborted();
      await raceWithAbort(
        decodeJobArgs(registration.definition, transformedJob.args),
        signal
      );
    } catch {
      signal.throwIfAborted();
      payloadInvalid = true;
    }
    // Match River's rescuer: a payload that cannot be decoded is rescued
    // regardless of timeout, while a kind whose timeout is disabled may run
    // indefinitely and is never rescued.
    const timeoutMs = this.#effectiveJobTimeout(registration);
    if (!payloadInvalid && timeoutMs === null) return null;
    if (
      !payloadInvalid &&
      timeoutMs !== null &&
      job.attemptedAt !== null &&
      now.epochNanoseconds - job.attemptedAt.epochNanoseconds <
        BigInt(timeoutMs) * 1_000_000n
    ) {
      return null;
    }

    if (job.attempt >= job.maxAttempts) {
      return {
        error,
        finalizedAt: now,
        id: job.id,
        scheduledAt: job.scheduledAt,
        state: "discarded",
      };
    }
    return {
      error,
      finalizedAt: null,
      id: job.id,
      scheduledAt: this.#completions.nextRetryAt(
        retryPolicyJob,
        now,
        payloadInvalid ? undefined : registration.options.retryPolicy
      ),
      state: "retryable",
    };
  }

  /**
   * Work one claimed row and persist its outcome. `releaseCapacity` frees
   * its producer slot early, when a stuck handler adds a worker slot.
   *
   * A row the driver couldn't fully decode (`decodeError`) isn't worked. Like
   * an unknown kind, its attempt fails before hooks or middleware run, so the
   * error handler sees it and the retry policy retries or discards it.
   *
   * `cancelled` starts the attempt already cancelled, for a job whose
   * cancellation arrived while the claim that took it was in flight.
   */
  async run(
    row: JobRow,
    releaseCapacity: () => void,
    decodeError?: Error,
    cancelled = false
  ): Promise<void> {
    const registration = workerRegistration(this.#workers, row.kind);
    const attemptController = new AbortController();
    const timeoutController = new AbortController();
    const key = attemptKey(row.id, this.#context.clientId);
    this.#attempts.set(key, attemptController);
    if (cancelled) attemptController.abort(new JobCancelledError(row.id));
    // Like Go, a remote cancellation or shutdown is tracked apart from the
    // attempt's own timeout: a remote cancellation that arrives after the
    // timeout still decides how the attempt ends.
    const cancelLink = new LinkedAbortSignal([
      this.#runSignal,
      attemptController.signal,
    ]);
    const cancelSignal = cancelLink.signal;
    // Like River for Go's executor cancelling the job's context as it
    // returns, the handler's signal aborts once the attempt finished.
    const finishedController = new AbortController();
    const link = new LinkedAbortSignal([
      cancelSignal,
      timeoutController.signal,
      finishedController.signal,
    ]);
    const signal = link.signal;
    const remotelyCancelled = (): boolean =>
      cancelSignal.aborted && cancelSignal.reason instanceof JobCancelledError;
    const timers = new AttemptTimers(row.id, timeoutController);
    let transformedRow = row;
    let transformFailure: { readonly error: unknown } | undefined;
    if (decodeError !== undefined) {
      transformFailure = {
        error: new Error(
          `job row couldn't be decoded: ${decodeError.message}`,
          {
            cause: decodeError,
          }
        ),
      };
    } else if (registration !== undefined) {
      try {
        transformedRow = this.transformJobArgs(row);
      } catch (error: unknown) {
        transformFailure = { error };
      }
    }
    const outputState: WorkOutputState = {};
    const rawContext = this.#attemptContext(
      transformedRow,
      signal,
      outputState
    );
    const metadataState = beginWorkAttempt(rawContext, outputState);

    try {
      await this.#context.emit({
        at: rawContext.execution.startedAt,
        job: transformedRow,
        kind: "job_started",
      });

      let handle: AttemptExecutionHandle | undefined;
      // Like River for Go, the worker's retry policy applies once the job's
      // arguments decode.
      let workerRetryPolicy: RetryPolicy | undefined;
      const execution = (
        transformFailure !== undefined
          ? this.#executePreWorkFailure(
              rawContext,
              transformFailure.error,
              remotelyCancelled
            )
          : registration === undefined
            ? this.#executePreWorkFailure(
                rawContext,
                new UnknownJobKindError(row.kind),
                remotelyCancelled
              )
            : this.#execute(
                rawContext,
                registration,
                remotelyCancelled,
                async () => {
                  const workContext = await this.#prepareWorkContext(
                    rawContext,
                    registration,
                    transformedRow,
                    outputState,
                    timers,
                    releaseCapacity
                  );
                  workerRetryPolicy = registration.options.retryPolicy;
                  return workContext;
                },
                (started) => {
                  handle = started;
                  timers.started(started);
                }
              )
      ).finally(() => {
        if (timers.settle()) this.#stuckJobs -= 1;
      });
      const result = await this.#awaitResult(execution, signal, () => handle);
      const abortReason: unknown = remotelyCancelled()
        ? cancelSignal.reason
        : signal.aborted
          ? signal.reason
          : undefined;
      // The attempt's peers end before it, each with an outcome.
      await this.#peers?.finish(metadataState, abortReason);
      await this.#persistAttempt(
        transformedRow,
        result,
        abortReason,
        rawContext.execution.startedAt,
        workerRetryPolicy
      );
    } finally {
      finishedController.abort(new JobAttemptFinishedError(row.id));
      link[Symbol.dispose]();
      cancelLink[Symbol.dispose]();
      timers.dispose();
      this.#peers?.abandon(metadataState);
      metadataState.active = false;
      this.#attempts.delete(key);
    }
  }

  /** Whether this client is working an attempt of job `id`, or owns it as a peer. */
  isActive(id: bigint): boolean {
    return this.isWorking(id) || this.#peers?.owns(id) === true;
  }

  /** Whether this client is working an attempt of job `id`. */
  isWorking(id: bigint): boolean {
    return this.#attempts.has(attemptKey(id, this.#context.clientId));
  }

  /**
   * Keep cancellations of jobs not worked here that arrive until `end()`,
   * for one claim, like River for Go's producer does for one fetch: an
   * attempt the claim starts for such a job starts cancelled. Other
   * cancellations are discarded when the claim ends.
   */
  watchCancellations(): {
    cancelled(id: bigint): boolean;
    end(): void;
  } {
    const pending = new Set<string>();
    this.#claimCancellations.add(pending);
    return {
      cancelled: (id) => pending.has(attemptKey(id, this.#context.clientId)),
      end: () => {
        this.#claimCancellations.delete(pending);
      },
    };
  }

  /** Apply the configured argument transformers to a claimed row. */
  transformJobArgs(row: JobRow): JobRow {
    const args = transformJobArgsForRead(
      this.#jobArgsTransformers,
      row.kind,
      row.args
    );
    return args === row.args ? row : { ...row, args };
  }

  /** The retry policy of the worker registered for `kind`, if any. */
  workerRetryPolicy(kind: string): RetryPolicy | undefined {
    return workerRegistration(this.#workers, kind)?.options.retryPolicy;
  }

  /** The context an attempt's hooks, middleware, and executor start from. */
  #attemptContext(
    row: JobRow,
    signal: AbortSignal,
    outputState: WorkOutputState
  ): WorkAttemptContext {
    const context: WorkAttemptContext = {
      client: this.#context.client,
      execution: {
        attemptedBy: this.#context.clientId,
        startedAt: this.#context.now(),
      },
      job: row,
      logger: createWorkLogger(this.#workLogger, {
        attempt: row.attempt,
        jobId: row.id.toString(10),
        jobKind: row.kind,
      }),
      recordOutput: (value) => {
        outputState.output = normalizeOutput(value);
      },
      setMetadata: (metadataKey, value) => {
        setWorkMetadata(context, metadataKey, value);
      },
      signal,
    };
    return context;
  }

  /**
   * Wait for an attempt to settle. If its signal aborts first, ask the
   * executor to stop, giving the handler the stuck threshold to settle
   * before the executor may end it by force, then keep capacity until the
   * handler really settles.
   * Cancellation and interruption events are emitted only once, after the
   * resulting transition commits.
   */
  async #awaitResult(
    execution: Promise<WorkAttemptResult>,
    signal: AbortSignal,
    handle: () => AttemptExecutionHandle | undefined
  ): Promise<WorkAttemptResult> {
    const winner = await Promise.race([
      execution.then((value) => ({ type: "execution" as const, value })),
      waitForAbort(signal).then((reason) => ({
        reason,
        type: "abort" as const,
      })),
    ]);
    if (winner.type === "execution") return winner.value;
    const started = handle();
    const { terminated } =
      started === undefined
        ? { terminated: false }
        : await started.abort(winner.reason, this.#jobStuckThresholdMs);
    const result = await execution;
    // An executor that forcibly terminated the handler proves the abort
    // stopped it, whatever error the termination surfaced, unless the
    // handler ignored a stop's abort, which fails its attempt.
    if (
      terminated &&
      result.status === "failed" &&
      !(result.error instanceof JobAbortedError)
    ) {
      return {
        ...attemptData(result),
        error: winner.reason,
        status: "cancelled",
      };
    }
    return result;
  }

  #effectiveJobTimeout(registration: WorkerRegistration): number | null {
    const timeout = registration.options.timeout;
    if (timeout === undefined) return this.#jobTimeoutMs;
    return timeout === null ? null : toMilliseconds("worker timeout", timeout);
  }

  async #execute(
    rawContext: WorkAttemptContext,
    registration: WorkerRegistration,
    remotelyCancelled: () => boolean,
    prepareContext: () => Promise<WorkContext>,
    started: (handle: AttemptExecutionHandle) => void
  ): Promise<WorkAttemptResult> {
    const kindPlugins = registration.options.plugins ?? [];
    validatePlugins(kindPlugins, "worker");
    const hooks = Object.freeze([
      ...this.#hooks,
      ...kindPlugins.flatMap((plugin) =>
        plugin.hooks === undefined ? [] : [plugin.hooks]
      ),
      ...(registration.options.hooks === undefined
        ? []
        : [registration.options.hooks]),
    ]);
    // Worker middleware is outermost like Go's Worker.Middleware; job-kind
    // plugin middleware is innermost like JobArgs.Plugin middleware.
    const middleware = Object.freeze([
      ...(registration.options.middleware ?? []),
      ...this.#middleware,
      ...kindPlugins.flatMap((plugin) => plugin.middleware ?? []),
    ]);
    let decodedContext: WorkContext | undefined;
    let workResult: WorkAttemptResult | undefined;
    let result: WorkAttemptResult;
    try {
      const invoke = composeMiddleware(middleware, async () => {
        for (const hook of hooks) {
          await hook.beforeWork?.(rawContext);
        }
        decodedContext = await prepareContext();
        const context = decodedContext;
        return runInWorkContext(context, async () => {
          let innerResult: WorkAttemptResult;
          let handle: AttemptExecutionHandle | undefined;
          try {
            handle = this.#attemptExecutor.start(registration, context);
            started(handle);
            const outcome = await handle.result;
            innerResult = succeededResult(outcome);
          } catch (error: unknown) {
            innerResult =
              (await ignoredStopFailure(handle, context)) ??
              thrownResult(error, context.signal);
          }
          innerResult = resultWithOutput(innerResult, context);
          for (const hook of hooks) {
            if (hook.afterWork === undefined) continue;
            try {
              const replacement = await hook.afterWork(context, innerResult);
              if (replacement !== undefined) {
                innerResult = normalizeWorkAttemptResult(replacement);
              }
            } catch (error: unknown) {
              innerResult = { error, status: "failed" };
              break;
            }
          }
          workResult = innerResult;
          if (innerResult.status !== "succeeded") throw innerResult.error;
          return innerResult.outcome;
        });
      });
      const outcome = await runInWorkContext(rawContext, () =>
        invoke(rawContext)
      );
      result =
        workResult?.status === "succeeded" &&
        Object.is(outcome, workResult.outcome)
          ? workResult
          : succeededResult(outcome, workResult);
    } catch (error: unknown) {
      result =
        workResult !== undefined &&
        workResult.status !== "succeeded" &&
        Object.is(error, workResult.error)
          ? workResult
          : thrownResult(error, rawContext.signal, workResult);
    }
    if (decodedContext !== undefined) {
      const resumable = decodedContext.resumable.finish(
        result.status !== "succeeded"
      );
      if (resumable.error !== null && result.status !== "failed") {
        result = { error: resumable.error, status: "failed" };
      }
      if (Object.keys(resumable.metadata).length > 0) {
        result = {
          ...result,
          metadata: toJsonObject({
            ...(result.metadata ?? {}),
            ...resumable.metadata,
          }),
        };
      }
    }
    // A handler that stopped because its timeout expired failed with the
    // timeout, like a Go worker returning its context's deadline error.
    if (
      result.status === "cancelled" &&
      result.error instanceof JobTimeoutError
    ) {
      result = {
        ...attemptData(result),
        error: result.error,
        status: "failed",
      };
    }
    result = await this.#handleError(rawContext, result, remotelyCancelled);
    result = resultWithOutput(result, rawContext);
    result = resultWithMetadata(result, rawContext);
    publishWorkResult(rawContext, result);
    return result;
  }

  async #executePreWorkFailure(
    context: WorkAttemptContext,
    error: unknown,
    remotelyCancelled: () => boolean
  ): Promise<WorkAttemptResult> {
    let result = thrownResult(error, context.signal);
    result = await this.#handleError(context, result, remotelyCancelled);
    result = resultWithOutput(result, context);
    result = resultWithMetadata(result, context);
    publishWorkResult(context, result);
    return result;
  }

  /**
   * Give the error handler a failed attempt, like Go's executor: every
   * failure, including an attempt that stopped on its timeout, reaches it.
   * An error after a remote cancellation doesn't, because Go replaces it
   * with the cancellation, but a runtime fault (Go's panic, which Go still
   * hands to `HandlePanic`) does. Interrupted (`cancelled`) attempts never
   * reach it.
   */
  async #handleError(
    context: WorkAttemptContext,
    result: WorkAttemptResult,
    remotelyCancelled: () => boolean
  ): Promise<WorkAttemptResult> {
    if (
      result.status !== "failed" ||
      this.#errorHandler === undefined ||
      (remotelyCancelled() && !isRuntimeFault(result.error))
    ) {
      return result;
    }
    try {
      const decision = await this.#errorHandler(
        errorHandlerContext(context),
        result.error
      );
      if (
        decision?.cancel !== undefined &&
        typeof decision.cancel !== "boolean"
      ) {
        throw new ValidationError("errorHandler cancel must be a boolean");
      }
      if (decision?.cancel === true) return { ...result, cancel: true };
    } catch (error: unknown) {
      this.#context.logger.error("River error handler failed", {
        error: canonicalError(error, this.#context.now()).error,
      });
    }
    return result;
  }

  /**
   * Persist a settled attempt, reconciling it with any abort that raced it
   * the way River's Go executor does:
   *
   * - A remote cancellation wins over everything except a plain successful
   *   completion, which is persisted as `completed`.
   * - After a timeout, a success completes; a handler that stopped because of
   *   the abort records the timeout as its error, and any other failure is
   *   recorded as returned.
   * - After a shutdown abort, only a handler that stopped because of that
   *   abort is interrupted; a success completes and a genuine failure is
   *   recorded and retried normally, like a handler its executor stopped by
   *   force after it ignored the abort.
   */
  async #persistAttempt(
    row: JobRow,
    result: WorkAttemptResult,
    abortReason: unknown,
    startedAt: Temporal.Instant,
    retryPolicy: RetryPolicy | undefined
  ): Promise<void> {
    const completions = this.#completions;
    const persistResult = (): Promise<void> =>
      completions.persistResult(row, result, startedAt, undefined, retryPolicy);
    const persistAbort = (reason: unknown): Promise<void> =>
      completions.persistAbort(
        row,
        reason,
        startedAt,
        result,
        undefined,
        retryPolicy
      );
    if (abortReason === undefined) {
      await (result.status === "cancelled"
        ? persistAbort(result.error)
        : persistResult());
      return;
    }
    if (abortReason instanceof JobCancelledError) {
      await (isPlainCompletion(result)
        ? persistResult()
        : persistAbort(abortReason));
      return;
    }
    await (result.status === "cancelled"
      ? persistAbort(abortReason)
      : persistResult());
  }

  /**
   * Decode the attempt's arguments and build the context its handler
   * receives, preparing its timeout and stuck-report timers to arm once the
   * executor starts it.
   */
  async #prepareWorkContext(
    rawContext: WorkAttemptContext,
    registration: WorkerRegistration,
    row: JobRow,
    outputState: WorkOutputState,
    timers: AttemptTimers,
    releaseCapacity: () => void
  ): Promise<WorkContext> {
    const decodedArgs = await decodeJobArgs(registration.definition, row.args);
    timers.prepare(
      this.#effectiveJobTimeout(registration),
      this.#jobStuckThresholdMs,
      (timeoutMs, thresholdMs) => {
        this.#stuckJobs += 1;
        void this.#reportStuck(
          row,
          timeoutMs,
          thresholdMs,
          releaseCapacity
        ).catch((error: unknown) => {
          this.#context.fail(error);
        });
      }
    );
    const context = makeDecodedWorkContext(
      rawContext,
      decodedArgs,
      row,
      outputState,
      this.#context.clientId,
      this.#context.driver,
      this.#context.operations
    );
    shareWorkAttempt(rawContext, context, outputState);
    return context;
  }

  /**
   * Cancel the attempts among `ids` whose jobs have a cancellation request,
   * reading them in batches of 1,000 IDs, each bounded by ten seconds.
   */
  async #cancelRequested(
    requested: NonNullable<RuntimeDriver["jobGetCancelRequested"]>,
    ids: readonly bigint[],
    signal: AbortSignal
  ): Promise<void> {
    const clientId = this.#context.clientId;
    for (let start = 0; start < ids.length; start += CANCEL_POLL_BATCH_SIZE) {
      const timeout = this.#context.timer.timeout(
        CANCEL_POLL_TIMEOUT_MS,
        () => new Error("River's cancellation check timed out")
      );
      const link = new LinkedAbortSignal([signal, timeout.signal]);
      try {
        const cancelled = await raceWithAbort(
          requested(ids.slice(start, start + CANCEL_POLL_BATCH_SIZE), {
            signal: link.signal,
          }),
          link.signal
        );
        for (const id of cancelled) this.cancelAttempt(id, clientId);
      } finally {
        link[Symbol.dispose]();
        timeout.dispose();
      }
    }
  }

  /** The job IDs of the attempts this client runs itself. */
  #ownAttemptIds(): bigint[] {
    const suffix = `:${this.#context.clientId}`;
    return [...this.#attempts.keys()]
      .filter((key) => key.endsWith(suffix))
      .map((key) => BigInt(key.slice(0, -suffix.length)));
  }

  async #reportStuck(
    job: JobRow,
    timeoutMs: number,
    thresholdMs: number,
    releaseCapacity: () => void
  ): Promise<void> {
    // Like River for Go, report the timeout the attempt ran with, the
    // worker's own when it sets one.
    this.#context.logger.warn("River job appears to be stuck", {
      jobId: job.id.toString(10),
      kind: job.kind,
      timeoutMs,
    });
    await this.#context.emit({
      at: this.#context.now(),
      error: new JobStuckError(
        job.id,
        millisecondsToDuration(timeoutMs),
        millisecondsToDuration(thresholdMs)
      ),
      job,
      kind: "job_stuck",
    });
    if (this.#stuckHandler === undefined) return;
    try {
      const decision = await this.#stuckHandler({
        id: job.id,
        kind: job.kind,
        queue: job.queue,
        totalStuckJobs: this.#stuckJobs,
      });
      if (
        decision?.addWorkerSlot !== undefined &&
        typeof decision.addWorkerSlot !== "boolean"
      ) {
        throw new ValidationError(
          "stuckHandler addWorkerSlot must be a boolean"
        );
      }
      if (decision?.addWorkerSlot === true) releaseCapacity();
    } catch (error: unknown) {
      this.#context.logger.error("River stuck handler failed", {
        error: canonicalError(error, this.#context.now()).error,
      });
    }
  }
}

/** The key identifying one attempt: the job ID and its claiming client. */
function attemptKey(id: bigint, attemptedBy: string): string {
  return `${id}:${attemptedBy}`;
}

/** Abort `controller` with a timeout error after `timeoutMs`; returns the disposer. */
function armJobTimeout(
  jobId: bigint,
  timeoutMs: number | null,
  controller: AbortController
): () => void {
  if (timeoutMs === null) return () => undefined;
  return unrefTimeout(() => {
    controller.abort(
      new JobTimeoutError(jobId, millisecondsToDuration(timeoutMs))
    );
  }, timeoutMs);
}

/**
 * The failure of an attempt whose executor stopped it by force because it
 * still ignored a stop's abort after the client's stuck threshold. It had
 * that long to respond, so unlike a handler that stops because of the
 * abort, its attempt counts. A remote cancellation or a timeout decides the
 * outcome of a forced stop as usual.
 */
async function ignoredStopFailure(
  handle: AttemptExecutionHandle | undefined,
  context: WorkContext
): Promise<WorkAttemptResult | undefined> {
  const reason: unknown = context.signal.reason;
  if (
    handle === undefined ||
    !context.signal.aborted ||
    reason instanceof JobCancelledError ||
    reason instanceof JobTimeoutError ||
    !(await handle.forciblyStopped())
  ) {
    return undefined;
  }
  return {
    error: new JobAbortedError(context.job.id, { cause: reason }),
    status: "failed",
  };
}

/** Resolve with the signal's abort reason once it aborts. */
function waitForAbort(signal: AbortSignal): Promise<unknown> {
  if (signal.aborted) return Promise.resolve(signal.reason);
  return new Promise((resolve) =>
    signal.addEventListener("abort", () => resolve(signal.reason), {
      once: true,
    })
  );
}

/**
 * An attempt's timeout and stuck-report timers. Both arm once the executor
 * actually starts the attempt, which an executor with its own capacity may
 * delay (see `WorkExecutorHandle.started`), and stop when execution settles.
 */
class AttemptTimers {
  #arm: (() => void) | undefined;
  #disposeTimeout: () => void = () => undefined;
  readonly #jobId: bigint;
  #settled = false;
  #stuckReported = false;
  #cancelStuckTimer: (() => void) | undefined;
  readonly #timeoutController: AbortController;

  constructor(jobId: bigint, timeoutController: AbortController) {
    this.#jobId = jobId;
    this.#timeoutController = timeoutController;
  }

  /** Stop the timeout without marking execution settled. */
  dispose(): void {
    this.#disposeTimeout();
  }

  /**
   * Prepare the timers: a timeout after `timeoutMs`, and `onStuck` once
   * `stuckThresholdMs` more passes without the attempt settling.
   */
  prepare(
    timeoutMs: number | null,
    stuckThresholdMs: number,
    onStuck: (timeoutMs: number, stuckThresholdMs: number) => void
  ): void {
    this.#arm = () => {
      this.#arm = undefined;
      if (this.#settled) return;
      this.#disposeTimeout = armJobTimeout(
        this.#jobId,
        timeoutMs,
        this.#timeoutController
      );
      if (timeoutMs !== null) {
        this.#cancelStuckTimer = unrefTimeout(() => {
          if (this.#settled) return;
          this.#stuckReported = true;
          onStuck(timeoutMs, stuckThresholdMs);
        }, timeoutMs + stuckThresholdMs);
      }
    };
  }

  /**
   * Stop both timers because execution settled. Returns whether the attempt
   * had been reported stuck.
   */
  settle(): boolean {
    this.#settled = true;
    this.#disposeTimeout();
    this.#cancelStuckTimer?.();
    return this.#stuckReported;
  }

  /** Arm the prepared timers once the executor starts the attempt. */
  started(handle: AttemptExecutionHandle): void {
    if (handle.started === undefined) {
      this.#arm?.();
    } else {
      void Promise.resolve(handle.started).then(
        () => this.#arm?.(),
        () => undefined
      );
    }
  }
}
