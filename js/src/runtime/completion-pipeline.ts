/**
 * The completion pipeline: batches attempt completions, persists them with
 * River's bounded retry policy, requeues or drops batches that keep failing,
 * and emits each job's event once its transition commits.
 */
import type { JobCompletionCommand, JobCompletionResult } from "../driver.js";
import { jobCompletionKey } from "../driver.js";
import {
  DatabaseOperationError,
  isRetryableError,
  JobCancelledError,
  JobTimeoutError,
} from "../errors.js";
import { jobEvent } from "../events.js";
import type { WorkAttemptResult } from "../extensions.js";
import { LinkedAbortSignal, raceWithAbort } from "../internal/abort.js";
import { exponentialBackoffMs } from "../internal/backoff.js";
import {
  CompletionBatcher,
  CompletionDroppedError,
} from "../internal/completion-batcher.js";
import type { JobRow } from "../job.js";
import {
  completionCommand,
  completionEventKind,
  defaultNextRetry,
} from "./completion-command.js";
import type { RuntimeContext } from "./context.js";
import { describeError } from "./failures.js";
import type { RetryPolicy } from "./settings.js";

/** Persistence attempts per completion batch before requeueing or dropping. */
const COMPLETION_ATTEMPTS = 3;
/** Retry delays between completion attempts: 1 s, 2 s, then 4 s. */
const COMPLETION_BACKOFF = Object.freeze({ baseMs: 1_000, maxMs: 4_000 });
/**
 * Bound on one completion query, matching River's hot-operation timeout. A
 * lock wait or dead connection cannot hold completion capacity indefinitely.
 */
const COMPLETION_TIMEOUT_MS = 10_000;

/** Configuration for a {@link CompletionPipeline}. */
export interface CompletionPipelineOptions {
  /** Completions per persistence query; twice this many may be pending. */
  readonly batchSize: number;
  /**
   * The most batches persisted at once, or undefined for no limit beyond
   * the batcher's own.
   */
  readonly concurrency?: number | undefined;
  /** How long a partial batch waits for more completions. */
  readonly flushIntervalMs: number;
  readonly retryPolicy: RetryPolicy | undefined;
  /** Retries and snoozes due within this interval are made available now. */
  readonly schedulerIntervalMs: number;
}

/**
 * How a caller follows one completion it hands over. Tracked completions
 * resolve only once persisted, and reject when they can't be.
 */
export interface CompletionTracking {
  /** The completer accepted it and now retries it until it settles. */
  accepted?(): void;
  /** It was persisted, applied or stale, before its event is delivered. */
  persisted?(): void;
}

/** Owns the completion batcher and the work that follows each commit. */
export class CompletionPipeline {
  readonly #batcher: CompletionBatcher<
    JobCompletionCommand,
    JobCompletionResult
  >;
  readonly #context: RuntimeContext;
  #draining = false;
  /** Permits for concurrent batches, when their number is limited. */
  readonly #permits: Semaphore | undefined;
  readonly #retryPolicy: RetryPolicy | undefined;
  readonly #schedulerIntervalMs: number;
  /** Post-commit work (events) not yet finished. */
  readonly #tasks = new Set<Promise<void>>();

  constructor(context: RuntimeContext, options: CompletionPipelineOptions) {
    this.#context = context;
    this.#permits =
      options.concurrency === undefined
        ? undefined
        : new Semaphore(options.concurrency);
    this.#retryPolicy = options.retryPolicy;
    this.#schedulerIntervalMs = options.schedulerIntervalMs;
    this.#batcher = new CompletionBatcher({
      batchSize: options.batchSize,
      flushIntervalMs: options.flushIntervalMs,
      maxPendingItems: options.batchSize * 2,
      onDrop: (error, count) => this.#reportDroppedCompletions(error, count),
      onPersistFailure: (error, count) =>
        this.#completionFailureAction(error, count),
      persist: (commands, signal) => this.#persistCompletions(commands, signal),
    });
  }

  /** Persistence queries currently running. */
  get inFlightQueries(): number {
    return this.#batcher.inFlightQueries;
  }

  /** Completions that may be pending before attempts wait for capacity. */
  get maxPendingItems(): number {
    return this.#batcher.maxPendingItems;
  }

  /** Completions accepted but not yet persisted. */
  get pendingItems(): number {
    return this.#batcher.pendingItems;
  }

  /** Fail every pending completion with `reason`, as the runtime fails. */
  abort(reason: unknown): void {
    this.#batcher.abort(reason);
  }

  /**
   * Flush everything pending, then wait for post-commit work. Rethrows a
   * flush failure only after that work finishes.
   */
  async close(): Promise<void> {
    let closeFailure: { readonly error: unknown } | undefined;
    try {
      await this.#batcher.close();
    } catch (error: unknown) {
      closeFailure = { error };
    }
    while (this.#tasks.size > 0) {
      await Promise.allSettled(this.#tasks);
    }
    if (closeFailure !== undefined) throw closeFailure.error;
  }

  /**
   * Stop requeueing failed batches so a stop during a database outage
   * finishes: the first persistent failure abandons the rest to the rescuer,
   * exactly like River's other runtimes.
   */
  drain(): void {
    this.#draining = true;
    this.#batcher.drain();
  }

  /**
   * When a failed attempt runs next, like River for Go's executor: the
   * worker's retry policy, else the client's, else River's default schedule
   * when neither gives a time or the time is in the past. A policy that
   * throws or returns something other than an instant gives no time.
   */
  nextRetryAt(
    job: JobRow,
    now: Temporal.Instant,
    workerRetryPolicy?: RetryPolicy
  ): Temporal.Instant {
    const scheduledAt =
      policyRetryAt(workerRetryPolicy, job, now) ??
      policyRetryAt(this.#retryPolicy, job, now);
    if (
      scheduledAt !== undefined &&
      Temporal.Instant.compare(scheduledAt, now) >= 0
    ) {
      return scheduledAt;
    }
    return defaultNextRetry(job, now, this.#context.random);
  }

  /**
   * Persist an attempt stopped by an abort. Like River's executor, attempt
   * errors are stamped with the attempt's start time and a remote
   * cancellation records no trace.
   */
  async persistAbort(
    row: JobRow,
    reason: unknown,
    startedAt: Temporal.Instant,
    result?: WorkAttemptResult,
    tracking?: CompletionTracking,
    workerRetryPolicy?: RetryPolicy
  ): Promise<void> {
    const hasOutput = result !== undefined && "output" in result;
    const output = result?.output ?? null;
    if (reason instanceof JobCancelledError) {
      await this.#persistCommand(
        row,
        {
          attempt: row.attempt,
          attemptedBy: this.#context.clientId,
          error: {
            at: startedAt,
            error: "JobCancelError: job cancelled remotely",
            trace: "",
          },
          finalizedAt: this.#context.now(),
          id: row.id,
          kind: "cancel",
          metadata: result?.metadata ?? {},
          output,
          outputSet: hasOutput,
          scheduledAt: null,
        },
        undefined,
        tracking
      );
      return;
    }
    if (reason instanceof JobTimeoutError) {
      await this.persistResult(
        row,
        {
          error: reason,
          ...(result?.metadata === undefined
            ? {}
            : { metadata: result.metadata }),
          ...(hasOutput ? { output } : {}),
          status: "failed",
        },
        startedAt,
        tracking,
        workerRetryPolicy
      );
      return;
    }
    await this.#persistCommand(
      row,
      {
        attempt: row.attempt,
        attemptedBy: this.#context.clientId,
        error: null,
        finalizedAt: null,
        id: row.id,
        kind: "interrupt",
        metadata: result?.metadata ?? {},
        output,
        outputSet: hasOutput,
        scheduledAt: this.#context.now(),
      },
      undefined,
      tracking
    );
  }

  /**
   * Persist a settled attempt's result. The returned promise resolves once
   * the completion is accepted for batching, or, with `tracking`, once it is
   * persisted and its event delivered.
   */
  async persistResult(
    row: JobRow,
    result: WorkAttemptResult,
    startedAt: Temporal.Instant,
    tracking?: CompletionTracking,
    workerRetryPolicy?: RetryPolicy
  ): Promise<void> {
    const now = this.#context.now();
    const nextRetryAt =
      result.status === "failed"
        ? this.nextRetryAt(row, now, workerRetryPolicy)
        : now;
    const command = completionCommand(
      row,
      this.#context.clientId,
      result,
      now,
      startedAt,
      nextRetryAt,
      this.#schedulerIntervalMs
    );
    await this.#persistCommand(row, command, result.error, tracking);
  }

  #completionFailureAction(error: unknown, count: number): "drop" | "requeue" {
    if (!isRetryableError(error)) return "drop";
    this.#context.logger.error(
      "River completion persistence failed repeatedly; requeueing batch",
      { error: describeError(error), jobs: count }
    );
    this.#context.emitMetric({ count, name: "job_completion_requeued" });
    return "requeue";
  }

  async #persistCommand(
    row: JobRow,
    command: JobCompletionCommand,
    localError?: unknown,
    tracking?: CompletionTracking
  ): Promise<void> {
    const awaitPersistence = tracking !== undefined;
    const submission = this.#batcher.submit(
      jobCompletionKey(command),
      command,
      command.id.toString(10)
    );
    try {
      await submission.accepted;
    } catch (error: unknown) {
      // A completion abandoned while waiting for capacity was reported when
      // it was dropped; its row stays running until the rescuer recovers it.
      if (!(error instanceof CompletionDroppedError)) throw error;
      if (awaitPersistence) throw error.cause;
      return;
    }
    tracking?.accepted?.();
    const persistence = this.#context.guard(
      submission.result
        .then(
          async (completion) => {
            tracking?.persisted?.();
            if (completion.job === null) {
              await this.#context.emit({
                at: this.#context.now(),
                job: row,
                kind: "job_race",
              });
              return;
            }
            await this.#context.emit(
              jobEvent(
                completionEventKind(command.kind, completion.job),
                this.#context.now(),
                completion.job,
                localError
              )
            );
          },
          (error: unknown) => {
            if (!(error instanceof CompletionDroppedError)) throw error;
            if (awaitPersistence) throw error.cause;
          }
        )
        .finally(submission.acknowledge)
    );
    this.#trackTask(persistence);
    if (awaitPersistence) await persistence;
  }

  /**
   * Persist one completion batch with River's bounded retry policy: each
   * attempt is limited to {@link COMPLETION_TIMEOUT_MS}, and up to
   * {@link COMPLETION_ATTEMPTS} attempts run with exponential backoff before
   * the batcher requeues or drops the batch.
   */
  async #persistCompletions(
    commands: readonly JobCompletionCommand[],
    batcherSignal: AbortSignal
  ): Promise<ReadonlyMap<string, JobCompletionResult>> {
    for (let attempt = 1; ; attempt++) {
      batcherSignal.throwIfAborted();
      // The permit comes before a database connection, so a batch never
      // holds a connection while it waits for a permit, and before the
      // attempt's deadline, so waiting spends neither it nor a retry.
      const release = await this.#permits?.acquire(batcherSignal);
      const timeout = this.#context.timer.timeout(
        COMPLETION_TIMEOUT_MS,
        () =>
          new DatabaseOperationError(
            `River completion persistence timed out after ${COMPLETION_TIMEOUT_MS} ms`,
            {
              backend: this.#context.backend,
              operation: "jobCompleteMany",
              retryable: true,
            }
          )
      );
      const link = new LinkedAbortSignal([batcherSignal, timeout.signal]);
      const signal = link.signal;
      try {
        const operation = (async () =>
          this.#context.operations.complete(this.#context.driver, commands, {
            signal,
          }))();
        // The query keeps its signal until it settles, even past a timeout.
        const unlink = () => {
          link[Symbol.dispose]();
        };
        void operation.then(unlink, unlink);
        // The permit bounds queries, not attempts: one that outlives its
        // deadline keeps the permit until it settles.
        if (release !== undefined) void operation.then(release, release);
        const results = await raceWithAbort(operation, signal);
        const byKey = new Map<string, JobCompletionResult>();
        for (const result of results) byKey.set(result.key, result);
        // An earlier attempt that timed out may still have committed, which
        // leaves nothing for this one to update. Report such a completion as
        // what it was, not as a race with another process.
        if (attempt > 1) {
          await this.#recoverLateCommits(commands, byKey, signal);
        }
        return byKey;
      } catch (thrown: unknown) {
        if (batcherSignal.aborted) throw thrown;
        const error: unknown = timeout.signal.aborted
          ? timeout.signal.reason
          : thrown;
        const lastAttempt = attempt >= COMPLETION_ATTEMPTS;
        // Mirror River's completer: back off after every failed attempt,
        // including the last one before a requeue, but never delay a
        // shutdown that will abandon the batch anyway.
        const delayMs =
          lastAttempt && this.#draining
            ? 0
            : exponentialBackoffMs(
                attempt,
                COMPLETION_BACKOFF,
                this.#context.random
              );
        this.#context.logger.warn(
          "River completion persistence attempt failed",
          {
            attempt,
            attempts: COMPLETION_ATTEMPTS,
            delayMs,
            error: describeError(error),
            jobs: commands.length,
            retryable: isRetryableError(error),
          }
        );
        if (delayMs > 0)
          await this.#context.timer.delay(delayMs, batcherSignal);
        if (lastAttempt) throw error;
      } finally {
        timeout.dispose();
      }
    }
  }

  /**
   * Replace each unapplied result in `byKey` with the job's row when the row
   * shows that `commands`' own earlier, timed-out attempt committed the
   * transition.
   */
  async #recoverLateCommits(
    commands: readonly JobCompletionCommand[],
    byKey: Map<string, JobCompletionResult>,
    signal: AbortSignal
  ): Promise<void> {
    const unapplied = commands.filter(
      (command) => byKey.get(jobCompletionKey(command))?.job === null
    );
    if (unapplied.length === 0) return;
    let rows: readonly JobRow[];
    try {
      // One read for the whole batch, bounded like the attempt itself.
      rows = await raceWithAbort(
        this.#context.driver.jobList({
          after: null,
          ids: unapplied.map(({ id }) => id),
          kinds: [],
          limit: unapplied.length,
          metadata: null,
          priorities: [],
          queues: [],
          sortDirection: "asc",
          sortField: "id",
          states: [],
          tagsAll: [],
          tagsAny: [],
        }),
        signal
      );
    } catch {
      // Without the rows, the race reports stand.
      return;
    }
    const byId = new Map(rows.map((row) => [row.id, row]));
    for (const command of unapplied) {
      const row = byId.get(command.id);
      if (row !== undefined && committedBy(command, row)) {
        const key = jobCompletionKey(command);
        byKey.set(key, { job: row, key, status: "applied" });
      }
    }
  }

  #reportDroppedCompletions(error: unknown, count: number): void {
    this.#context.logger.error(
      "River dropped completions after persistence failed; the jobs stay running until the rescuer recovers them",
      { error: describeError(error), jobs: count }
    );
    this.#context.emitMetric({ count, name: "job_completion_dropped" });
  }

  #trackTask(task: Promise<void>): void {
    this.#tasks.add(task);
    void task.then(
      () => this.#tasks.delete(task),
      () => this.#tasks.delete(task)
    );
  }
}

/**
 * Whether `row` holds the transition `command` persists, so an earlier attempt
 * of the same completion committed it: the attempt, client, state, and time
 * all match, which another process's completion, cancellation, rescue, or
 * claim can't reproduce.
 */
function committedBy(command: JobCompletionCommand, row: JobRow): boolean {
  if (row.attemptedBy.at(-1) !== command.attemptedBy) return false;
  const sameTime = (
    stored: Temporal.Instant | null,
    expected: Temporal.Instant | null
  ) =>
    stored !== null &&
    expected !== null &&
    stored.epochMilliseconds === expected.epochMilliseconds;
  const finalized = (state: JobRow["state"]) =>
    row.attempt === command.attempt &&
    row.state === state &&
    sameTime(row.finalizedAt, command.finalizedAt);
  const rescheduled = (state: JobRow["state"], attempt: number) =>
    row.attempt === attempt &&
    row.state === (command.available === true ? "available" : state) &&
    sameTime(row.scheduledAt, command.scheduledAt);
  switch (command.kind) {
    case "cancel":
      return finalized("cancelled");
    case "complete":
      return finalized("completed");
    case "discard":
      return finalized("discarded");
    case "interrupt":
      return rescheduled("available", Math.max(command.attempt - 1, 0));
    case "retry":
      return (
        rescheduled("retryable", command.attempt) &&
        row.errors.at(-1)?.attempt === command.attempt &&
        row.errors.at(-1)?.error === command.error?.error
      );
    case "snooze":
      return rescheduled("scheduled", Math.max(command.attempt - 1, 0));
  }
}

/** A counting semaphore whose waits end when their signal aborts. */
class Semaphore {
  #available: number;
  readonly #waiters: { readonly grant: () => void }[] = [];

  constructor(permits: number) {
    this.#available = permits;
  }

  async acquire(signal: AbortSignal): Promise<() => void> {
    signal.throwIfAborted();
    if (this.#available > 0) {
      this.#available--;
    } else {
      await new Promise<void>((resolve, reject) => {
        const waiter = {
          grant: () => {
            signal.removeEventListener("abort", onAbort);
            resolve();
          },
        };
        const onAbort = () => {
          const index = this.#waiters.indexOf(waiter);
          if (index !== -1) this.#waiters.splice(index, 1);
          reject(signal.reason as Error);
        };
        signal.addEventListener("abort", onAbort, { once: true });
        this.#waiters.push(waiter);
      });
    }
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = this.#waiters.shift();
      if (next === undefined) this.#available++;
      else next.grant();
    };
  }
}

/** A retry policy's time, or undefined when it throws or gives no instant. */
function policyRetryAt(
  policy: RetryPolicy | undefined,
  job: JobRow,
  now: Temporal.Instant
): Temporal.Instant | undefined {
  if (policy === undefined) return undefined;
  try {
    const scheduledAt: unknown = policy(job, now);
    return scheduledAt instanceof Temporal.Instant ? scheduledAt : undefined;
  } catch {
    // Like a zero time from Go's NextRetry, a failed policy gives no time.
    return undefined;
  }
}
