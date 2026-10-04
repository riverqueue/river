import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { Client } from "./client.js";
import type {
  DriverInsertResult,
  JobCompletionCommand,
  JobCompletionResult,
  JobDeleteResult,
  JobClaimParams,
  JobClaimResult,
  JobInsertParams,
  JobListParams,
  QueueRow,
  RuntimeDriver,
  RuntimeJobRescue,
  RuntimeLeader,
  RuntimeNotification,
} from "./driver.js";
import { jobCompletionKey } from "./driver.js";
import {
  ConfigurationError,
  DatabaseOperationError,
  JobAttemptFinishedError,
  JobStuckError,
  JobTimeoutError,
  ValidationError,
} from "./errors.js";
import { registerDriver } from "./internal/driver-registry.js";
import { defineJob } from "./job-definition.js";
import type { RiverEvent } from "./events.js";
import type { WorkMiddleware } from "./extensions.js";
import type { JobRow } from "./job.js";
import { createJobArgsTransformPlugin } from "./job-args-transform.js";
import type { JsonObject } from "./json.js";
import type { OperationTimeout, RuntimeTimer } from "./internal/backoff.js";
import type { RiverMetric } from "./metrics.js";
import type { MaintenanceOptions } from "./options.js";
import { periodicJob } from "./periodic.js";
import {
  currentWorkContext,
  defaultNextRetry,
  overrideRuntimeTiming,
  recordOutput,
  setMetadata,
} from "./runtime.js";
import type { ClientOptions } from "./options.js";
import type { QueueRuntimeDiagnostics } from "./runtime.js";
import { cancel, snooze, Workers } from "./worker.js";
import type { WorkExecutor } from "./worker.js";

class FakeRuntimeDriver implements RuntimeDriver {
  declare readonly "~river"?: {
    readonly capability: "runtime";
    readonly transaction: unknown;
  };

  constructor() {
    registerDriver(this, {
      backend: "fake",
      capability: "runtime",
      operations: this,
    });
  }

  readonly completions: JobCompletionCommand[] = [];
  readonly completionBatches: JobCompletionCommand[][] = [];
  completionCalls = 0;
  completionError: Error | undefined;
  completionGate: Promise<void> | undefined;
  completionOverride:
    ((command: JobCompletionCommand) => JobCompletionResult) | undefined;
  completionFailures = 0;
  readonly completionSignals: (AbortSignal | undefined)[] = [];
  claim: JobRow[] = [];
  claimFailures = 0;
  /** Errors thrown by successive claims before the claim queue is consulted. */
  readonly claimFaults: unknown[] = [];
  /** Replaces the default never-yielding notification stream. */
  notificationStream:
    | ((
        topics: readonly RuntimeNotification["topic"][],
        signal: AbortSignal,
        ready: () => void
      ) => AsyncIterable<RuntimeNotification>)
    | undefined;
  queueGetCalls = 0;
  /** Errors thrown by successive queue reads. */
  readonly queueGetFaults: unknown[] = [];
  readonly claimed: JobRow[] = [];
  readonly claimRequests: number[] = [];
  readonly claimQueues: string[] = [];
  readonly claimStartedAtMs: number[] = [];
  cancelled: JobRow | null = null;
  deleteResult: JobDeleteResult = { status: "not_found" };
  lastClaim: JobClaimParams | undefined;
  lastList: JobListParams | undefined;
  listRows: JobRow[] = [];
  readonly queues = new Map<string, QueueRow>();
  /** Decode errors the fake reports for claimed jobs, by ID. */
  readonly decodeErrors = new Map<bigint, Error>();
  notificationSubscriptions = 0;
  notificationReadyGate: Promise<void> | undefined;
  leadershipResignRequests = 0;
  leadershipResignTransaction: unknown;

  jobCancel(): JobRow | null {
    return this.cancelled;
  }

  /** Holds the next claim's result until it resolves, once. */
  claimGate: Promise<void> | undefined;

  jobClaim(params: JobClaimParams): JobClaimResult | Promise<JobClaimResult> {
    const result = this.#claim(params);
    const gate = this.claimGate;
    if (gate === undefined || result.jobs.length === 0) return result;
    this.claimGate = undefined;
    return gate.then(() => result);
  }

  #claim(params: JobClaimParams): JobClaimResult {
    this.claimQueues.push(params.queues[0]?.name ?? "");
    this.claimStartedAtMs.push(performance.now());
    this.lastClaim = params;
    if (this.claimFaults.length > 0) throw this.claimFaults.shift();
    if (this.claimFailures > 0) {
      this.claimFailures -= 1;
      throw new DatabaseOperationError("temporary claim failure", {
        backend: "fake",
        operation: "jobClaim",
        retryable: true,
      });
    }
    const limit = params.queues.reduce((sum, queue) => sum + queue.limit, 0);
    this.claimRequests.push(limit);
    const rows = this.claim.splice(0, limit);
    this.claimed.push(...rows);
    const decodeErrors = new Map(
      [...this.decodeErrors].filter(([id]) => rows.some((row) => row.id === id))
    );
    return { decodeErrors, jobs: rows };
  }

  async jobCompleteMany(
    commands: readonly JobCompletionCommand[],
    options?: { readonly signal?: AbortSignal }
  ): Promise<readonly JobCompletionResult[]> {
    this.completionCalls += 1;
    this.completionSignals.push(options?.signal);
    if (this.completionFailures > 0) {
      this.completionFailures -= 1;
      throw new DatabaseOperationError("temporary completion failure", {
        backend: "fake",
        operation: "jobCompleteMany",
        retryable: true,
      });
    }
    await this.completionGate;
    if (this.completionError !== undefined) throw this.completionError;
    this.completionBatches.push([...commands]);
    this.completions.push(...commands);
    return commands.map(
      (command) =>
        this.completionOverride?.(command) ?? {
          job: applyCompletion(
            this.claimed.findLast(({ id }) => id === command.id) ??
              fakeJob("test", command.id),
            command
          ),
          key: jobCompletionKey(command),
          status: "applied",
        }
    );
  }

  jobDelete(): JobDeleteResult {
    return this.deleteResult;
  }

  jobDeleteMany(): readonly JobRow[] {
    return [];
  }

  jobGet(id: bigint): JobRow | null {
    return this.listRows.find((job) => job.id === id) ?? null;
  }

  jobInsert(params: JobInsertParams): DriverInsertResult {
    return { job: fakeJob(params.kind), status: "inserted" };
  }

  jobInsertMany(params: readonly JobInsertParams[]): DriverInsertResult[] {
    return params.map((item) => this.jobInsert(item));
  }

  jobList(params: JobListParams): readonly JobRow[] {
    this.lastList = params;
    return this.listRows;
  }

  jobRetry(): JobRow | null {
    return null;
  }

  jobUpdate(): JobRow | null {
    return null;
  }

  queueGet(name: string): QueueRow | null {
    this.queueGetCalls += 1;
    if (this.queueGetFaults.length > 0) throw this.queueGetFaults.shift();
    return this.queues.get(name) ?? null;
  }

  queueList(): readonly QueueRow[] {
    return [];
  }

  queuePause(_name: string): QueueRow | null {
    void _name;
    return null;
  }

  queueResume(_name: string): QueueRow | null {
    void _name;
    return null;
  }

  queueUpdate(): QueueRow | null {
    return null;
  }

  runtimeQueueUpsert(name: string, now: Temporal.Instant): QueueRow {
    const existing = this.queues.get(name);
    if (existing !== undefined) {
      const refreshed = { ...existing, updatedAt: now };
      this.queues.set(name, refreshed);
      return refreshed;
    }
    const queue = {
      createdAt: now,
      metadata: {},
      name,
      pausedAt: null,
      updatedAt: now,
    };
    this.queues.set(name, queue);
    return queue;
  }

  runtimeRequestLeadershipResignation(options?: { tx?: unknown }): void {
    this.leadershipResignRequests += 1;
    this.leadershipResignTransaction = options?.tx;
  }

  async *runtimeNotificationSubscribe(
    topics: readonly ("control" | "insert" | "leadership")[],
    signal: AbortSignal,
    ready: () => void
  ): AsyncGenerator<RuntimeNotification> {
    if (this.notificationStream !== undefined) {
      this.notificationSubscriptions++;
      yield* this.notificationStream(topics, signal, ready);
      return;
    }
    this.notificationSubscriptions++;
    await this.notificationReadyGate;
    ready();
    await new Promise<void>((resolve) =>
      signal.addEventListener("abort", () => resolve(), { once: true })
    );
    if (!signal.aborted) {
      yield { payload: "{}", topic: "insert" as const };
    }
  }
}

/**
 * Deterministic runtime timer. Backoff delays resolve on the next macrotask
 * instead of after wall-clock time, and operation timeouts fire only when a
 * test calls `expireTimeouts`.
 */
class FakeTimer implements RuntimeTimer {
  readonly delays: number[] = [];
  readonly #timeouts = new Set<{
    controller: AbortController;
    reason: () => unknown;
  }>();

  get activeTimeouts(): number {
    return this.#timeouts.size;
  }

  delay(milliseconds: number, signal: AbortSignal): Promise<void> {
    this.delays.push(milliseconds);
    if (signal.aborted) return Promise.reject(signal.reason);
    return new Promise((resolve, reject) => {
      const onAbort = () => {
        clearImmediate(immediate);
        reject(signal.reason);
      };
      const immediate = setImmediate(() => {
        signal.removeEventListener("abort", onAbort);
        resolve();
      });
      signal.addEventListener("abort", onAbort, { once: true });
    });
  }

  now(): number {
    return performance.now();
  }

  expireTimeouts(): void {
    for (const timeout of [...this.#timeouts]) {
      this.#timeouts.delete(timeout);
      timeout.controller.abort(timeout.reason());
    }
  }

  timeout(_milliseconds: number, reason: () => unknown): OperationTimeout {
    const entry = { controller: new AbortController(), reason };
    this.#timeouts.add(entry);
    return {
      dispose: () => this.#timeouts.delete(entry),
      signal: entry.controller.signal,
    };
  }
}

interface LogEntry {
  readonly attributes: Readonly<Record<string, unknown>> | undefined;
  readonly level: "debug" | "error" | "info" | "warn";
  readonly message: string;
}

function durationsInMilliseconds(
  diagnostics: QueueRuntimeDiagnostics | undefined
): Record<string, unknown> | undefined {
  return (
    diagnostics && {
      ...diagnostics,
      fetchCooldown: diagnostics.fetchCooldown.total("milliseconds"),
      pollInterval: diagnostics.pollInterval.total("milliseconds"),
    }
  );
}

function recordingLogger(entries: LogEntry[]) {
  const log =
    (level: LogEntry["level"]) =>
    (attributes: Readonly<Record<string, unknown>>, message: string) => {
      entries.push({ attributes, level, message });
    };
  return {
    debug: log("debug"),
    error: log("error"),
    info: log("info"),
    warn: log("warn"),
  };
}

/**
 * Apply a completion the way River's backends do for the attempt that still
 * owns a running row: the target state follows the command, snoozes and
 * interruptions refund the attempt, errors append, and metadata merges.
 */
function applyCompletion(row: JobRow, command: JobCompletionCommand): JobRow {
  const cancelRequested = row.metadata.cancel_attempted_at !== undefined;
  const nonTerminal =
    command.kind === "interrupt" ||
    command.kind === "retry" ||
    command.kind === "snooze";
  const state: JobRow["state"] =
    nonTerminal && cancelRequested
      ? "cancelled"
      : command.available === true
        ? "available"
        : {
            cancel: "cancelled" as const,
            complete: "completed" as const,
            discard: "discarded" as const,
            interrupt: "available" as const,
            retry: "retryable" as const,
            snooze: "scheduled" as const,
          }[command.kind];
  const refund =
    (command.kind === "interrupt" || command.kind === "snooze") &&
    !cancelRequested;
  return {
    ...row,
    attempt: refund ? Math.max(row.attempt - 1, 0) : row.attempt,
    errors:
      command.error === null
        ? row.errors
        : [...row.errors, { ...command.error, attempt: command.attempt }],
    finalizedAt:
      state === "cancelled" || state === "completed" || state === "discarded"
        ? (command.finalizedAt ?? Temporal.Now.instant())
        : null,
    metadata: {
      ...row.metadata,
      ...command.metadata,
      ...(command.outputSet ? { output: command.output } : {}),
    },
    scheduledAt:
      nonTerminal && cancelRequested
        ? row.scheduledAt
        : (command.scheduledAt ?? row.scheduledAt),
    state,
  };
}

function fakeJob(kind = "test", id = 101n): JobRow {
  const now = Temporal.Instant.from("2026-08-30T12:00:00.123456789Z");
  return {
    args: { value: "work" },
    attempt: 1,
    attemptedAt: now,
    attemptedBy: ["runtime-test"],
    createdAt: now,
    errors: [],
    finalizedAt: null,
    id,
    kind,
    maxAttempts: 3,
    metadata: {},
    priority: 1,
    queue: "default",
    scheduledAt: now,
    state: "running",
    tags: [],
    uniqueKey: null,
    uniqueStates: null,
  };
}

describe("Client runtime", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("derives default retry delay from persisted error count", () => {
    const now = Temporal.Instant.from("2026-08-30T12:00:00Z");
    const job = {
      ...fakeJob(),
      attempt: 99,
      errors: [
        { at: now, attempt: 1, error: "one", trace: "" },
        { at: now, attempt: 2, error: "two", trace: "" },
      ],
      maxAttempts: 100,
    };

    expect(defaultNextRetry(job, now, () => 0.5)).toEqual(
      now.add({ seconds: 81 })
    );
  });

  it("caps the default retry delay at exactly Go's maximum duration", () => {
    const now = Temporal.Instant.from("2026-08-30T12:00:00Z");
    const errors = Array.from({ length: 309 }, (_, index) => ({
      at: now,
      attempt: index + 1,
      error: "failed",
      trace: "",
    }));
    const job = { ...fakeJob(), attempt: 310, errors, maxAttempts: 1_000 };

    // From the 310th error on, like Go's `secondsAsCappedDuration`.
    for (const random of [0, 0.5, 0.999]) {
      expect(
        defaultNextRetry(job, now, () => random).epochNanoseconds -
          now.epochNanoseconds
      ).toBe(9_223_372_036_854_775_807n);
    }
  });

  it("throws a structured error instead of reporting a running job as deleted", async () => {
    const driver = new FakeRuntimeDriver();
    driver.deleteResult = { job: fakeJob(), status: "running" };
    const client = new Client(driver);

    await expect(client.jobs.delete(101n)).rejects.toMatchObject({
      code: "job_running",
      jobId: 101n,
      name: "JobRunningError",
    });
  });

  it("works and completes a claimed job with ordered extensions", async () => {
    const driver = new FakeRuntimeDriver();
    const definition = defineJob({
      kind: "test",
      schema: z.object({ value: z.string() }),
    });
    const order: string[] = [];
    const workers = new Workers().add(definition, ({ job }) => {
      order.push(`handler:${job.args.value}`);
    });
    driver.claim = [{ ...fakeJob(), args: { envelope: { value: "work" } } }];
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      hooks: {
        onEvent: (event) => {
          order.push(`commit:${event.kind}`);
        },
        afterWork: () => {
          order.push("after");
        },
        beforeWork: (context) => {
          expect(context.job.args).toEqual({ value: "work" });
          order.push("before");
        },
      },
      middleware: [
        async (context, next) => {
          order.push(`middleware-before:${context.job.id}`);
          const result = await next();
          order.push("middleware-after");
          return result;
        },
      ],
      plugins: [
        createJobArgsTransformPlugin({
          name: "envelope",
          onRead: ({ args }) => args.envelope as JsonObject,
        }),
      ],
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const events = client.subscribe({ capacity: 8 });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop({ mode: "graceful" });

    expect(driver.completions[0]).toMatchObject({
      attempt: 1,
      attemptedBy: "runtime-test",
      id: 101n,
      kind: "complete",
    });
    expect(order).toEqual([
      "commit:job_started",
      "middleware-before:101",
      "before",
      "handler:work",
      "after",
      "middleware-after",
      "commit:job_completed",
    ]);
    expect((await events.next()).value.kind).toBe("job_started");
    expect((await events.next()).value.kind).toBe("job_completed");
    expect(run.state).toBe("stopped");
    events.close();
  });

  it("fails malformed transformed payloads before work extensions", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    let beforeWorkCalls = 0;
    let handlerCalls = 0;
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      errorHandler: (_context, error) => {
        expect(error).toMatchObject({ message: "ciphertext is malformed" });
      },
      hooks: {
        beforeWork: () => {
          beforeWorkCalls += 1;
        },
      },
      leaderElectionDisabled: true,
      plugins: [
        createJobArgsTransformPlugin({
          name: "malformed",
          onRead: () => {
            throw new Error("ciphertext is malformed");
          },
        }),
      ],
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => {
        handlerCalls += 1;
      }),
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(beforeWorkCalls).toBe(0);
    expect(handlerCalls).toBe(0);
    expect(driver.completions[0]).toMatchObject({
      id: 101n,
      kind: "retry",
    });
  });

  it("retries explicitly retryable claim and completion failures", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    driver.claimFailures = 2;
    driver.completionFailures = 2;
    const timer = new FakeTimer();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    overrideRuntimeTiming(client, { random: () => 0.5, timer });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(driver.claimFailures).toBe(0);
    expect(driver.completionFailures).toBe(0);
    expect(driver.completionCalls).toBe(3);
    expect(driver.completions[0]?.kind).toBe("complete");
    expect(timer.delays.slice(-2)).toEqual([1_000, 2_000]);
    expect(timer.activeTimeouts).toBe(0);
    expect(run.state).toBe("stopped");
  });

  it("batches completions beyond one queue's worker capacity", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = Array.from({ length: 8 }, (_, index) =>
      fakeJob("test", BigInt(index + 1))
    );
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 4,
      completionFlushInterval: { milliseconds: 10_000 },
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 2, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 8);
    await run.stop();

    expect(driver.completionBatches.map((batch) => batch.length)).toEqual([
      4, 4,
    ]);
    expect(driver.claimRequests.length).toBeLessThanOrEqual(5);
  });

  it("aborts a handler's signal once its attempt finished, like Go", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob("test", 7n)];
    const definition = defineJob({ kind: "test" });
    let signal: AbortSignal | undefined;
    let abortedDuringWork: boolean | undefined;
    const client = new Client(driver, {
      clientId: "runtime-test",
      leaderElectionDisabled: true,
      queues: { default: { maxWorkers: 1 } },
      workers: new Workers().add(definition, (context) => {
        signal = context.signal;
        abortedDuringWork = context.signal.aborted;
      }),
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await waitUntil(() => signal?.aborted === true);
    await run.stop();

    expect(abortedDuringWork).toBe(false);
    expect(signal?.reason).toBeInstanceOf(JobAttemptFinishedError);
    expect(signal?.reason).toMatchObject({
      code: "job_attempt_finished",
      jobId: 7n,
    });
  });

  it("leaves no listener on its long-lived signals once attempts settle", async () => {
    // Links from settled attempts and completions would keep a listener on
    // the runtime's run and claim signals; count the links' live listeners.
    const live = new Set<string>();
    const ids = new WeakMap<object, number>();
    let nextId = 0;
    const key = (target: object, listener: unknown): string => {
      if (!ids.has(target)) ids.set(target, nextId++);
      if (!ids.has(listener as object)) ids.set(listener as object, nextId++);
      return `${ids.get(target)}:${ids.get(listener as object)}`;
    };
    const isLink = (type: string, listener: unknown): boolean =>
      type === "abort" &&
      typeof listener === "function" &&
      listener.name === "#onAbort";
    const add = EventTarget.prototype.addEventListener;
    const remove = EventTarget.prototype.removeEventListener;
    vi.spyOn(EventTarget.prototype, "addEventListener").mockImplementation(
      function (this: EventTarget, type, listener, options) {
        if (isLink(type, listener)) live.add(key(this, listener));
        add.call(this, type, listener, options);
      }
    );
    vi.spyOn(EventTarget.prototype, "removeEventListener").mockImplementation(
      function (this: EventTarget, type, listener, options) {
        if (isLink(type, listener)) live.delete(key(this, listener));
        remove.call(this, type, listener, options);
      }
    );
    const any = vi.spyOn(AbortSignal, "any");
    const driver = new FakeRuntimeDriver();
    driver.claim = Array.from({ length: 200 }, (_, index) =>
      fakeJob("test", BigInt(index + 1))
    );
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      leaderElectionDisabled: true,
      queues: { default: { maxWorkers: 50 } },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 200);
    const working = live.size;
    await run.stop();

    // Only the queue loop's link, on the claim signal and its generation's.
    expect(working).toBe(2);
    expect(live.size).toBe(0);
    // Nothing per attempt or completion uses `AbortSignal.any`.
    expect(any).not.toHaveBeenCalled();
  });

  it("bounds completion ownership and backpressures saturated attempts", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = Array.from({ length: 6 }, (_, index) =>
      fakeJob("test", BigInt(index + 1))
    );
    let releaseCompletions!: () => void;
    driver.completionGate = new Promise<void>((resolve) => {
      releaseCompletions = resolve;
    });
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      completionFlushInterval: { milliseconds: 10_000 },
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 4, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();

    await waitUntil(
      () =>
        run.diagnostics.completionQueries === 2 &&
        run.diagnostics.activeAttempts === 2
    );
    expect(run.diagnostics).toMatchObject({
      activeAttempts: 2,
      completionCapacity: 2,
      completionQueries: 2,
      pendingCompletions: 2,
    });

    releaseCompletions();
    await waitUntil(() => driver.completions.length === 6);
    await run.stop();
  });

  it("drops a non-retryable completion batch and keeps working", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob("test", 1n)];
    driver.completionError = new DatabaseOperationError("constraint failed", {
      backend: "fake",
      operation: "jobCompleteMany",
    });
    const logs: LogEntry[] = [];
    const metrics: RiverMetric[] = [];
    const timer = new FakeTimer();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      hooks: { onMetric: (metric) => void metrics.push(metric) },
      logger: recordingLogger(logs),
      leaderElectionDisabled: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 10 },
        },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    overrideRuntimeTiming(client, { random: () => 0.5, timer });
    const run = await client.start();

    await waitUntil(() =>
      metrics.some(({ name }) => name === "job_completion_dropped")
    );
    // Go's completer makes three bounded attempts before giving up.
    expect(driver.completionCalls).toBe(3);
    expect(run.state).toBe("running");
    expect(run.diagnostics.pendingCompletions).toBe(0);
    expect(logs.filter(({ level }) => level === "error")).toEqual([
      expect.objectContaining({
        attributes: { error: "constraint failed", jobs: 1 },
        message: expect.stringContaining("dropped completions"),
      }),
    ]);

    // The runtime keeps claiming and completing new work afterwards.
    driver.completionError = undefined;
    driver.claim = [fakeJob("test", 2n)];
    await waitUntil(() => driver.completions.some(({ id }) => id === 2n));
    await run.stop();
    expect(run.state).toBe("stopped");
    expect(metrics).toContainEqual({
      count: 1,
      name: "job_completion_dropped",
    });
  });

  it("requeues retryable completion failures until persistence recovers", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    driver.completionFailures = 5;
    const metrics: RiverMetric[] = [];
    const timer = new FakeTimer();
    const definition = defineJob({ kind: "test" });
    const events: string[] = [];
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      hooks: {
        onEvent: ({ kind }) => void events.push(kind),
        onMetric: (metric) => void metrics.push(metric),
      },
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    overrideRuntimeTiming(client, { random: () => 0.5, timer });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(driver.completionCalls).toBe(6);
    expect(metrics).toContainEqual({
      count: 1,
      name: "job_completion_requeued",
    });
    // Every failed attempt backs off, including the last before a requeue.
    expect(timer.delays).toEqual([1_000, 2_000, 4_000, 1_000, 2_000]);
    expect(events).toEqual(["job_started", "job_completed"]);
  });

  it("bounds a hung completion query with a per-attempt timeout", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    driver.completionGate = new Promise<void>(() => undefined);
    const logs: LogEntry[] = [];
    const timer = new FakeTimer();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      logger: recordingLogger(logs),
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    overrideRuntimeTiming(client, { random: () => 0.5, timer });
    const run = await client.start();

    await waitUntil(() => driver.completionCalls === 1);
    expect(timer.activeTimeouts).toBe(1);
    const signal = driver.completionSignals[0];
    driver.completionGate = undefined;
    timer.expireTimeouts();

    await waitUntil(() => driver.completions.length === 1);
    expect(signal?.aborted).toBe(true);
    expect(signal?.reason).toMatchObject({
      name: "DatabaseOperationError",
      operation: "jobCompleteMany",
      retryable: true,
    });
    expect(logs).toContainEqual(
      expect.objectContaining({
        attributes: expect.objectContaining({
          attempt: 1,
          error: "River completion persistence timed out after 10000 ms",
          retryable: true,
        }),
        level: "warn",
      })
    );
    await run.stop();
    expect(run.state).toBe("stopped");
  });

  it("reports a completion that committed after its attempt timed out", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob("test", 1n), fakeJob("test", 2n)];
    // The first attempt commits both completions, but its reply arrives only
    // after the attempt timed out. The retry then finds the rows finished.
    // Job 2 was meanwhile cancelled by another process, which is a race.
    const completeMany = driver.jobCompleteMany.bind(driver);
    driver.jobCompleteMany = async (commands, options) => {
      await completeMany(commands, options);
      if (driver.completionCalls === 1) {
        driver.listRows = commands.map((command) => {
          const row = applyCompletion(
            driver.claimed.findLast(({ id }) => id === command.id)!,
            command
          );
          return command.id === 2n
            ? {
                ...row,
                finalizedAt: Temporal.Now.instant(),
                state: "cancelled",
              }
            : row;
        });
        await new Promise((_resolve, reject) => {
          options?.signal?.addEventListener(
            "abort",
            () => reject(options.signal?.reason),
            { once: true }
          );
        });
      }
      return commands.map((command) => ({
        job: null,
        key: jobCompletionKey(command),
        status: "applied" as const,
      }));
    };
    const events: RiverEvent[] = [];
    const timer = new FakeTimer();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 2,
      hooks: { onEvent: (event) => void events.push(event) },
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 2, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    overrideRuntimeTiming(client, { random: () => 0.5, timer });
    const run = await client.start();

    await waitUntil(
      () => timer.activeTimeouts === 1 && driver.listRows.length === 2
    );
    timer.expireTimeouts();
    await waitUntil(() => driver.completionCalls === 2 && events.length >= 2);
    await run.stop();

    const outcomes = events
      .filter(({ kind }) => kind === "job_completed" || kind === "job_race")
      .map((event) => [
        "job" in event ? event.job.id : undefined,
        event.kind,
        "job" in event ? event.job.state : undefined,
      ]);
    expect(outcomes).toEqual(
      expect.arrayContaining([
        [1n, "job_completed", "completed"],
        [2n, "job_race", "running"],
      ])
    );
    expect(outcomes).toHaveLength(2);
    // Both rows are read back in one listing, not one read per job.
    expect(driver.lastList).toMatchObject({ ids: [1n, 2n], limit: 2 });
  });

  it("finishes a graceful stop during a completion outage", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob("test", 1n), fakeJob("test", 2n)];
    driver.completionFailures = Number.POSITIVE_INFINITY;
    const metrics: RiverMetric[] = [];
    const timer = new FakeTimer();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      hooks: { onMetric: (metric) => void metrics.push(metric) },
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 2, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    overrideRuntimeTiming(client, { random: () => 0.5, timer });
    const run = await client.start();

    await waitUntil(() =>
      metrics.some(({ name }) => name === "job_completion_requeued")
    );
    await run.stop();

    expect(run.state).toBe("stopped");
    expect(driver.completions).toEqual([]);
    expect(
      metrics
        .filter(({ name }) => name === "job_completion_dropped")
        .reduce(
          (total, metric) => total + ("count" in metric ? metric.count : 0),
          0
        )
    ).toBe(2);
  });

  it("releases completion capacity held by a dropped batch", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = Array.from({ length: 6 }, (_, index) =>
      fakeJob("test", BigInt(index + 1))
    );
    driver.completionError = new Error("driver invariant broke");
    const timer = new FakeTimer();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      completionFlushInterval: { milliseconds: 0 },
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 4, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    overrideRuntimeTiming(client, { random: () => 0.5, timer });
    const run = await client.start();

    // Six attempts through a two-item completion capacity can only finish if
    // every dropped batch releases its ownership.
    await waitUntil(() => driver.completionCalls === 18);
    await waitUntil(
      () =>
        run.diagnostics.activeAttempts === 0 &&
        run.diagnostics.pendingCompletions === 0
    );
    expect(run.state).toBe("running");
    await run.stop();
    expect(run.state).toBe("stopped");
  });

  it("releases worker and completion capacity while a hook runs but drains events on stop", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    let releaseCompletion!: () => void;
    driver.completionGate = new Promise<void>((resolve) => {
      releaseCompletion = resolve;
    });
    let releaseCompletionEvent!: () => void;
    const completionEventGate = new Promise<void>((resolve) => {
      releaseCompletionEvent = resolve;
    });
    const order: string[] = [];
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      hooks: {
        afterWork: () => {
          order.push("after-work");
        },
        onEvent: async ({ kind }) => {
          order.push(`event:${kind}`);
          if (kind === "job_completed") await completionEventGate;
        },
      },
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();

    await waitUntil(() => driver.completionCalls === 1);
    await waitUntil(() => run.diagnostics.activeAttempts === 0);
    expect(order).toEqual(["event:job_started", "after-work"]);
    let stopped = false;
    const stopping = run.stop().then(() => {
      stopped = true;
    });
    await Promise.resolve();
    expect(stopped).toBe(false);

    releaseCompletion();
    await waitUntil(() => order.includes("event:job_completed"));
    // A slow onEvent hook no longer holds completion capacity.
    await waitUntil(() => run.diagnostics.pendingCompletions === 0);
    expect(stopped).toBe(false);
    releaseCompletionEvent();
    await stopping;
    expect(order).toEqual([
      "event:job_started",
      "after-work",
      "event:job_completed",
    ]);
  });

  it("refills a full worker pool with coalesced bounded claim batches", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = Array.from({ length: 8 }, (_, index) =>
      fakeJob("test", BigInt(index + 1))
    );
    const definition = defineJob({ kind: "test" });
    const releases: Array<() => void> = [];
    let hold = true;
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 4,
      completionFlushInterval: { milliseconds: 0 },
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 4, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, async () => {
        if (!hold) return;
        await new Promise<void>((resolve) => releases.push(resolve));
      }),
    });
    const run = await client.start();
    await waitUntil(() => releases.length === 4);

    for (const release of releases.slice(0, 3)) release();
    await waitUntil(() => driver.claimRequests.length >= 2);

    expect(driver.claimRequests.slice(0, 2)).toEqual([4, 3]);
    hold = false;
    for (const release of releases) release();
    await waitUntil(() => driver.completions.length === 8);
    await run.stop();
    expect(driver.claimRequests.length).toBeLessThanOrEqual(3);
  });

  it("claims into a slot freed after a full claim while another job runs on", async () => {
    // A claim that fills every slot suggests more jobs wait. Like River for
    // Go, the next claim follows the first freed slot, not the long job.
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob("test", 1n), fakeJob("test", 2n)];
    const definition = defineJob({ kind: "test" });
    const releases = new Map<bigint, () => void>();
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      completionFlushInterval: { milliseconds: 0 },
      leaderElectionDisabled: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 2,
          pollInterval: { milliseconds: 60_000 },
        },
      },
      workers: new Workers().add(definition, async ({ job }) => {
        await new Promise<void>((resolve) => releases.set(job.id, resolve));
      }),
    });
    const run = await client.start();
    await waitUntil(() => releases.size === 2);
    driver.claim.push(fakeJob("test", 3n));

    releases.get(2n)?.();
    await waitUntil(() => releases.has(3n));

    expect(driver.claimRequests).toEqual([2, 1]);
    releases.get(1n)?.();
    releases.get(3n)?.();
    await waitUntil(() => driver.completions.length === 3);
    await run.stop();
  });

  it("refills low-volume work while an earlier job remains active", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob("test", 1n)];
    const definition = defineJob({ kind: "test" });
    let releaseFirst: (() => void) | undefined;
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      completionFlushInterval: { milliseconds: 0 },
      leaderElectionDisabled: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 4,
          pollInterval: { milliseconds: 20 },
        },
      },
      workers: new Workers().add(definition, async ({ job }) => {
        if (job.id === 1n) {
          await new Promise<void>((resolve) => {
            releaseFirst = resolve;
          });
        }
      }),
    });
    const run = await client.start();
    await waitUntil(() => releaseFirst !== undefined);

    driver.claim.push(fakeJob("test", 2n));
    await waitUntil(() => driver.claimed.some(({ id }) => id === 2n));

    expect(run.diagnostics.activeAttempts).toBeGreaterThanOrEqual(1);
    expect(driver.claimRequests).toContain(3);
    releaseFirst?.();
    await waitUntil(() => driver.completions.length === 2);
    await run.stop();
  });

  it("applies fetch cooldowns independently to every queue", async () => {
    const driver = new FakeRuntimeDriver();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      leaderElectionDisabled: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 10_000 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 10_000 },
        },
        other: {
          fetchCooldown: { milliseconds: 10_000 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 10_000 },
        },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();

    await waitUntil(() => driver.claimQueues.length >= 2);
    await run.stop();

    expect(driver.claimQueues).toEqual(
      expect.arrayContaining(["default", "other"])
    );
  });

  it("bounds full-batch refills with the queue fetch cooldown", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob("test", 101n), fakeJob("test", 102n)];
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      completionFlushInterval: { milliseconds: 0 },
      leaderElectionDisabled: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 30 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 1_000 },
        },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 2);
    await run.stop();

    expect(driver.claimStartedAtMs).toHaveLength(2);
    expect(
      (driver.claimStartedAtMs[1] ?? 0) - (driver.claimStartedAtMs[0] ?? 0)
    ).toBeGreaterThanOrEqual(25);
  });

  it("aborts an outstanding fetch cooldown during shutdown", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 10_000 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 10_000 },
        },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await expect(
      run.stop({ timeout: { milliseconds: 250 } })
    ).resolves.toBeUndefined();

    expect(driver.claimStartedAtMs).toHaveLength(1);
  });

  it("rejects millisecond option names", async () => {
    const definition = defineJob({ kind: "test" });
    const workers = new Workers().add(definition, () => undefined);
    const construct = (options: object) => () =>
      new Client(new FakeRuntimeDriver(), {
        workers,
        ...(options as ClientOptions),
      });

    expect(construct({ jobTimeoutMs: 1_000 })).toThrow(
      "jobTimeoutMs is not an option; use jobTimeout with a Temporal duration such as { seconds: 5 }"
    );
    expect(construct({ completionFlushIntervalMs: 10 })).toThrow(
      "completionFlushIntervalMs is not an option; use completionFlushInterval"
    );
    expect(
      construct({ queues: { default: { maxWorkers: 1, pollIntervalMs: 50 } } })
    ).toThrow("queue pollIntervalMs is not an option; use queue pollInterval");
    expect(construct({ maintenance: { rescueAfterMs: 1_000 } })).toThrow(
      "maintenance.rescueAfterMs is not an option; use maintenance.rescueAfter"
    );
    expect(construct({ eventLoopDelay: { resolutionMs: 10 } })).toThrow(
      "eventLoopDelay.resolutionMs is not an option; use eventLoopDelay.resolution"
    );

    const driver = new FakeRuntimeDriver();
    const client = new Client(driver, {
      clientId: "runtime-test",
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();
    await expect(
      run.updateQueue("default", {
        maxWorkers: 1,
        ...({ fetchCooldownMs: 10 } as object),
      })
    ).rejects.toThrow("queue fetchCooldownMs is not an option");
    await expect(
      run.stop({ ...({ timeoutMs: 5_000 } as object) })
    ).rejects.toThrow("stop timeoutMs is not an option; use stop timeout");
    await run.stop();
  });

  it("rejects option names River doesn't know", async () => {
    const definition = defineJob({ kind: "test" });
    const workers = new Workers().add(definition, () => undefined);
    const construct = (options: object) => () =>
      new Client(new FakeRuntimeDriver(), {
        workers,
        ...(options as ClientOptions),
      });

    expect(construct({ jobTimout: { seconds: 1 } })).toThrow(ValidationError);
    expect(construct({ jobTimout: { seconds: 1 } })).toThrow(
      'client has no option "jobTimout"'
    );
    expect(construct({ maintenance: { rescueAftr: { hours: 2 } } })).toThrow(
      'maintenance has no option "rescueAftr"'
    );
    expect(
      construct({ eventLoopDelay: { resolutin: { seconds: 1 } } })
    ).toThrow('eventLoopDelay has no option "resolutin"');

    const client = new Client(new FakeRuntimeDriver(), {
      clientId: "runtime-test",
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();
    await expect(
      run.stop({ ...({ timout: { seconds: 5 } } as object) })
    ).rejects.toThrow('stop has no option "timout"');
    await run.stop();
  });

  it("validates the job stuck threshold", () => {
    const definition = defineJob({ kind: "test" });
    const workers = new Workers().add(definition, () => undefined);
    const construct = (jobStuckThreshold: unknown) => () =>
      new Client(new FakeRuntimeDriver(), {
        jobStuckThreshold: jobStuckThreshold as Temporal.DurationLike,
        workers,
      });

    expect(construct({ seconds: -1 })).toThrow(
      "jobStuckThreshold must not be negative"
    );
    expect(construct(null)).toThrow(
      "jobStuckThreshold must be a Temporal duration"
    );
    expect(construct({ seconds: 0 })).not.toThrow();
  });

  it("validates queue fetch timing and worker bounds", () => {
    const definition = defineJob({ kind: "test" });
    const workers = new Workers().add(definition, () => undefined);

    expect(
      () =>
        new Client(new FakeRuntimeDriver(), {
          queues: {
            default: {
              fetchCooldown: { milliseconds: 20 },
              maxWorkers: 1,
              pollInterval: { milliseconds: 10 },
            },
          },
          workers,
        })
    ).toThrow(
      "queue pollInterval cannot be shorter than fetchCooldown, which defaults to the client's fetchCooldown"
    );
    expect(
      () =>
        new Client(new FakeRuntimeDriver(), {
          queues: {
            default: { fetchCooldown: { milliseconds: 0 }, maxWorkers: 1 },
          },
          workers,
        })
    ).toThrow("queue fetchCooldown must be positive");
    expect(
      () =>
        new Client(new FakeRuntimeDriver(), {
          queues: { default: { maxWorkers: 10_001 } },
          workers,
        })
    ).toThrow("maxWorkers must be at most 10000");
    expect(
      () =>
        new Client(new FakeRuntimeDriver(), {
          queues: { "Invalid Queue": { maxWorkers: 1 } },
          workers,
        })
    ).toThrow("queue name must contain lowercase letters");
    expect(
      () =>
        new Client(new FakeRuntimeDriver(), {
          clientId: "a".repeat(101),
          queues: { default: { maxWorkers: 1 } },
          workers,
        })
    ).toThrow("between 1 and 100 characters");
  });

  it("requests the currently available capacity for a 2,000-worker queue", async () => {
    const driver = new FakeRuntimeDriver();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 100,
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 2_000, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();

    await waitUntil(() => driver.claimRequests.length > 0);
    expect(driver.claimRequests[0]).toBe(2_000);
    await run.stop();
  });

  it("emits compatible fetch metrics without blocking claims", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    const metrics: RiverMetric[] = [];
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      hooks: {
        onMetric: (metric) => {
          metrics.push(metric);
        },
      },
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(metrics).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          duration: expect.any(Temporal.Duration),
          name: "job_get_available_duration",
          queue: "default",
        }),
        {
          count: 1,
          name: "job_get_available_count",
          queue: "default",
        },
      ])
    );
  });

  it("claims and fails an unregistered kind compatibly", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [
      {
        ...fakeJob("conformance_unregistered"),
        maxAttempts: 1,
      },
    ];
    const known = defineJob({ kind: "known" });
    const extensionOrder: string[] = [];
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      errorHandler: (_context, error) => {
        extensionOrder.push(`error:${(error as Error).name}`);
      },
      hooks: {
        afterWork: () => {
          extensionOrder.push("after");
        },
        beforeWork: () => {
          extensionOrder.push("before");
        },
      },
      leaderElectionDisabled: true,
      middleware: [
        (_context, next) => {
          extensionOrder.push("middleware");
          return next();
        },
      ],
      plugins: [
        createJobArgsTransformPlugin({
          name: "must-not-run-for-unknown-kind",
          onRead: () => {
            throw new Error("unknown kinds must not transform arguments");
          },
        }),
      ],
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(known, () => undefined),
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(driver.lastClaim?.kinds).toEqual([]);
    expect(driver.completions[0]).toMatchObject({
      error: {
        error:
          "job kind is not registered in the client's Workers bundle: conformance_unregistered",
      },
      finalizedAt: expect.any(Temporal.Instant),
      id: 101n,
      kind: "discard",
    });
    expect(extensionOrder).toEqual(["error:UnknownJobKindError"]);
    expect(run.state).toBe("stopped");
  });

  it("fails the attempts of undecodable claimed rows without working them", async () => {
    const driver = new FakeRuntimeDriver();
    const kind = defineJob({ kind: "known" });
    driver.claim = [
      { ...fakeJob("known", 302n), maxAttempts: 5 },
      fakeJob("known", 301n),
      { ...fakeJob("known", 303n), attempt: 1, maxAttempts: 1 },
    ];
    for (const id of [302n, 303n]) {
      driver.decodeErrors.set(
        id,
        new TypeError("could not decode `metadata`: not an object")
      );
    }
    const handled: string[] = [];
    const worked: bigint[] = [];
    const retryAt = Temporal.Instant.from("2030-01-01T00:00:00Z");
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      errorHandler: ({ job }, error) => {
        handled.push(`${job.id.toString()}:${(error as Error).message}`);
      },
      hooks: {
        beforeWork: () => {
          handled.push("before");
        },
      },
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 3, pollInterval: { milliseconds: 10_000 } },
      },
      retryPolicy: () => retryAt,
      workers: new Workers().add(kind, ({ job }) => {
        worked.push(job.id);
      }),
    });
    const failed = client.subscribe({ kinds: ["job_failed"] });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 3);
    await run.stop();

    const message =
      "job row couldn't be decoded: could not decode `metadata`: not an object";
    expect(worked).toEqual([301n]);
    expect(handled.filter((entry) => entry !== "before").sort()).toEqual([
      `302:${message}`,
      `303:${message}`,
    ]);
    const byId = new Map(driver.completions.map((c) => [c.id, c]));
    expect(byId.get(302n)).toMatchObject({
      error: { error: message },
      kind: "retry",
      scheduledAt: retryAt,
    });
    expect(byId.get(303n)).toMatchObject({
      error: { error: message },
      kind: "discard",
    });
    const events: bigint[] = [];
    for await (const event of failed) {
      if (event.kind === "job_failed") events.push(event.job.id);
      if (events.length === 2) break;
    }
    expect(events.sort()).toEqual([302n, 303n]);
  });

  it("makes compatible per-job rescue decisions", async () => {
    const driver = new FakeRuntimeDriver();
    const startedAt = Temporal.Now.instant();
    const oldAttempt = startedAt.subtract({ hours: 2 });
    const recentAttempt = startedAt.subtract({ milliseconds: 1 });
    const storedJobs = [
      {
        ...fakeJob("known", 201n),
        attemptedAt: oldAttempt,
        metadata: { cancel_attempted_at: startedAt.toString() },
      },
      { ...fakeJob("unknown", 202n), attemptedAt: oldAttempt },
      {
        ...fakeJob("known", 203n),
        args: { invalid: true },
        attemptedAt: recentAttempt,
      },
      {
        ...fakeJob("known", 204n),
        args: { invalid: true },
        attempt: 3,
        attemptedAt: oldAttempt,
        maxAttempts: 3,
      },
      { ...fakeJob("known", 205n), attemptedAt: recentAttempt },
      { ...fakeJob("known", 206n), attemptedAt: oldAttempt },
      {
        ...fakeJob("known", 207n),
        attempt: 3,
        attemptedAt: oldAttempt,
        maxAttempts: 3,
      },
      // A kind with a disabled timeout may legitimately run for hours.
      { ...fakeJob("unbounded", 208n), attemptedAt: oldAttempt },
      // An undecodable payload is rescued even when the timeout is disabled.
      {
        ...fakeJob("unbounded", 209n),
        args: { invalid: true },
        attemptedAt: oldAttempt,
      },
    ];
    const jobs = storedJobs.map((job) =>
      job.kind === "unknown" ? job : { ...job, args: { envelope: job.args } }
    );
    let electedAt: Temporal.Instant | null = null;
    let delivered = false;
    let rescues: readonly RuntimeJobRescue[] = [];
    Object.assign(driver, {
      maintenanceCleanJobs: () => 0,
      maintenanceCleanQueues: () => 0,
      maintenanceGetStuck: () => {
        if (delivered) return [];
        delivered = true;
        return jobs;
      },
      maintenanceLeaderAcquire: (
        leaderId: string,
        now: Temporal.Instant,
        ttlMs: number
      ): RuntimeLeader => {
        electedAt ??= now;
        return {
          electedAt,
          expiresAt: now.add({ milliseconds: ttlMs }),
          leaderId,
        };
      },
      maintenanceLeaderResign: () => true,
      maintenanceRescue: (
        _leader: RuntimeLeader,
        _attemptedBefore: Temporal.Instant,
        decisions: readonly RuntimeJobRescue[]
      ) => {
        rescues = decisions;
        return decisions.length;
      },
      maintenanceSchedule: () => 0,
    });
    const definition = defineJob({
      decode: (value: { invalid?: boolean; value?: string }) => {
        if (value.invalid === true) throw new Error("invalid payload");
        return value;
      },
      kind: "known",
    });
    const retryPolicyArgs = new Map<bigint, JsonObject>();
    const client = new Client(driver, {
      clientId: "runtime-test",
      jobTimeout: { milliseconds: 10 },
      maintenance: {
        electionInterval: { milliseconds: 1 },
        jobCleanerInterval: { milliseconds: 60_000 },
        queueCleanerInterval: { milliseconds: 60_000 },
        rescueAfter: { milliseconds: 10 },
        rescuerInterval: { milliseconds: 1 },
        schedulerInterval: { milliseconds: 60_000 },
      },
      plugins: [
        createJobArgsTransformPlugin({
          name: "envelope",
          onRead: ({ args }) => args.envelope as JsonObject,
        }),
      ],
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      retryPolicy: (job, now) => {
        retryPolicyArgs.set(job.id, job.args);
        return now.add({ seconds: 3 });
      },
      workers: new Workers()
        .add(definition, () => undefined, {
          // Like Go's rescuer, the worker's policy decides once the
          // arguments decode, and the client's otherwise.
          retryPolicy: (_job, now) => now.add({ seconds: 7 }),
          timeout: { milliseconds: 10 },
        })
        .add(
          defineJob({
            decode: (value: { invalid?: boolean }) => {
              if (value.invalid === true) throw new Error("invalid payload");
              return value;
            },
            kind: "unbounded",
          }),
          () => undefined,
          { timeout: null }
        ),
    });
    // Freeze the runtime clock: job 205's attempt must stay 1 ms old no
    // matter how long the rescuer takes to run under load.
    overrideRuntimeTiming(client, { now: () => startedAt });
    const run = await client.start();

    await waitUntil(() => rescues.length === 7);
    await run.stop();

    expect(rescues).toMatchObject([
      {
        finalizedAt: expect.any(Temporal.Instant),
        id: 201n,
        state: "cancelled",
      },
      {
        finalizedAt: expect.any(Temporal.Instant),
        id: 202n,
        state: "discarded",
      },
      { finalizedAt: null, id: 203n, state: "retryable" },
      {
        finalizedAt: expect.any(Temporal.Instant),
        id: 204n,
        state: "discarded",
      },
      { finalizedAt: null, id: 206n, state: "retryable" },
      {
        finalizedAt: expect.any(Temporal.Instant),
        id: 207n,
        state: "discarded",
      },
      { finalizedAt: null, id: 209n, state: "retryable" },
    ]);
    expect(rescues.some(({ id }) => id === 205n)).toBe(false);
    expect(rescues.some(({ id }) => id === 208n)).toBe(false);
    expect(
      rescues.every(
        ({ error }) => error.error === "Stuck job rescued by JobRescuer"
      )
    ).toBe(true);
    const retry = rescues.find(({ id }) => id === 206n);
    expect(retry).toBeDefined();
    if (retry === undefined) throw new Error("missing retry rescue decision");
    expect(
      retry.scheduledAt.epochNanoseconds - retry.error.at.epochNanoseconds
    ).toBe(7_000_000_000n);
    expect(retryPolicyArgs.has(206n)).toBe(false);
    expect(retryPolicyArgs.get(203n)).toEqual({ invalid: true });
    expect(retryPolicyArgs.get(209n)).toEqual({ invalid: true });
  });

  it("completes a remotely cancelled attempt whose handler still succeeds", async () => {
    // Go's executor only replaces a returned error with the cancellation
    // (`RemoteCancellationJobNotCancelledIfNoErrorReturned`).
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    driver.cancelled = { ...fakeJob(), state: "cancelled" };
    const definition = defineJob({ kind: "test" });
    const finish = Promise.withResolvers<undefined>();
    const workers = new Workers().add(definition, () =>
      finish.promise.then(() => undefined)
    );
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const events: string[] = [];
    const subscription = client.subscribe();
    const run = await client.start();
    await waitUntil(() => run.diagnostics.activeAttempts === 1);

    await client.jobs.cancel(101n);
    expect(run.diagnostics.activeAttempts).toBe(1);
    finish.resolve(undefined);
    await waitUntil(() => run.diagnostics.activeAttempts === 0);
    await run.stop({ mode: "graceful" });

    expect(driver.completions).toMatchObject([
      { error: null, kind: "complete" },
    ]);
    for await (const event of subscription) {
      events.push(`${event.kind}:${"job" in event ? event.job.state : ""}`);
      if (events.length === 2) break;
    }
    expect(events).toEqual(["job_started:running", "job_completed:completed"]);
  });

  it("emits job_cancelled once, after a remote cancellation commits", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    driver.cancelled = { ...fakeJob(), state: "cancelled" };
    const definition = defineJob({ kind: "test" });
    const release = Promise.withResolvers<undefined>();
    const workers = new Workers().add(definition, async ({ signal }) => {
      await release.promise;
      signal.throwIfAborted();
    });
    const events: string[] = [];
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      hooks: {
        onEvent: (event) =>
          void events.push(
            `${event.kind}:${"job" in event ? event.job.state : ""}`
          ),
      },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();
    await waitUntil(() => run.diagnostics.activeAttempts === 1);

    await client.jobs.cancel(101n);
    // The handler has not settled, so nothing has been persisted or emitted.
    await new Promise((resolve) => setImmediate(resolve));
    expect(events).toEqual(["job_started:running"]);
    release.resolve(undefined);
    await waitUntil(() => driver.completions.length === 1);
    await run.stop({ mode: "graceful" });

    expect(driver.completions).toMatchObject([
      {
        error: { error: "JobCancelError: job cancelled remotely" },
        kind: "cancel",
      },
    ]);
    expect(events).toEqual(["job_started:running", "job_cancelled:cancelled"]);
  });

  it("cancels a remotely cancelled attempt that snoozes", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    driver.cancelled = { ...fakeJob(), state: "cancelled" };
    const definition = defineJob({ kind: "test" });
    const release = Promise.withResolvers<undefined>();
    const workers = new Workers().add(definition, async () => {
      await release.promise;
      return snooze({ seconds: 60 });
    });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();
    await waitUntil(() => run.diagnostics.activeAttempts === 1);

    await client.jobs.cancel(101n);
    release.resolve(undefined);
    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(driver.completions).toMatchObject([{ kind: "cancel" }]);
  });

  it.each([
    {
      calls: [],
      name: "an ordinary error",
      thrown: new Error("cleanup failed"),
    },
    {
      calls: ["TypeError"],
      name: "a runtime fault",
      thrown: new TypeError("cleanup failed"),
    },
  ])(
    "gives the error handler $name after a remote cancellation like Go",
    async ({ calls, thrown }) => {
      // Go replaces a returned error with the cancellation before its error
      // handler runs, but still hands a panic to `HandlePanic`.
      const driver = new FakeRuntimeDriver();
      driver.claim = [fakeJob()];
      driver.cancelled = { ...fakeJob(), state: "cancelled" };
      const definition = defineJob({ kind: "test" });
      const release = Promise.withResolvers<undefined>();
      const workers = new Workers().add(definition, async () => {
        await release.promise;
        throw thrown;
      });
      const handled: string[] = [];
      const client = new Client(driver, {
        clientId: "runtime-test",
        completionBatchSize: 1,
        errorHandler: (_context, error) => {
          handled.push((error as Error).name);
        },
        queues: {
          default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
        },
        workers,
      });
      const run = await client.start();
      await waitUntil(() => run.diagnostics.activeAttempts === 1);

      await client.jobs.cancel(101n);
      release.resolve(undefined);
      await waitUntil(() => driver.completions.length === 1);
      await run.stop();

      expect(handled).toEqual(calls);
      expect(driver.completions).toMatchObject([
        {
          error: { error: "JobCancelError: job cancelled remotely" },
          kind: "cancel",
        },
      ]);
    }
  );

  it("gives the error handler an attempt stopped by its timeout like Go", async () => {
    // A Go worker that returns its context's deadline error reaches
    // `HandleError`, which may cancel the job.
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    const workers = new Workers().add(
      definition,
      async ({ signal }) => {
        await new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () => {
            reject(signal.reason as Error);
          });
        });
      },
      { timeout: { milliseconds: 20 } }
    );
    const handled: unknown[] = [];
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      errorHandler: (_context, error) => {
        handled.push(error);
        return { cancel: true };
      },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(handled).toHaveLength(1);
    expect(handled[0]).toBeInstanceOf(JobTimeoutError);
    expect(driver.completions).toMatchObject([
      {
        error: { error: "River job 101 exceeded its 20 ms timeout" },
        kind: "cancel",
      },
    ]);
  });

  it("cancels an attempt remotely cancelled after its timeout like Go", async () => {
    // Go checks the remote cancellation on the attempt's outer context, so it
    // still wins after the attempt's own timeout expired.
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    driver.cancelled = { ...fakeJob(), state: "cancelled" };
    const definition = defineJob({ kind: "test" });
    const timedOut = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const workers = new Workers().add(
      definition,
      async ({ signal }) => {
        await new Promise((resolve) => {
          signal.addEventListener("abort", resolve);
        });
        timedOut.resolve(undefined);
        await release.promise;
        throw signal.reason as Error;
      },
      { timeout: { milliseconds: 20 } }
    );
    let errorHandlerCalls = 0;
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      errorHandler: () => {
        errorHandlerCalls += 1;
      },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();
    await timedOut.promise;

    await client.jobs.cancel(101n);
    release.resolve(undefined);
    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(errorHandlerCalls).toBe(0);
    expect(driver.completions).toMatchObject([
      {
        error: { error: "JobCancelError: job cancelled remotely" },
        kind: "cancel",
      },
    ]);
  });

  it("interrupts only attempts stopped by the graceful deadline", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [
      fakeJob("test", 1n),
      fakeJob("test", 2n),
      fakeJob("test", 3n),
    ];
    const definition = defineJob({ kind: "test" });
    const workers = new Workers().add(definition, ({ job, signal }) => {
      return new Promise<void>((resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => {
            if (job.id === 1n) reject(signal.reason as Error);
            else if (job.id === 2n) reject(new Error("flush failed"));
            else resolve();
          },
          { once: true }
        );
      });
    });
    const events: string[] = [];
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      hooks: {
        onEvent: (event) => {
          if ("job" in event && event.kind !== "job_started") {
            events.push(`${event.job.id}:${event.kind}:${event.job.state}`);
          }
        },
      },
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 3, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();
    await waitUntil(() => run.diagnostics.activeAttempts === 3);

    await expect(run.stop({ timeout: { milliseconds: 1 } })).rejects.toThrow(
      "River runtime stop timed out"
    );
    await run.completed;

    const byId = new Map(driver.completions.map((item) => [item.id, item]));
    // Stopped by the shutdown abort: available again, attempt refunded.
    expect(byId.get(1n)).toMatchObject({ error: null, kind: "interrupt" });
    // A genuine failure during shutdown is recorded and retried like Go;
    // the first retry is due within a scheduler pass, so it is available.
    expect(byId.get(2n)).toMatchObject({
      available: true,
      error: { error: "flush failed" },
      kind: "retry",
    });
    // Finishing successfully after the deadline still completes the job.
    expect(byId.get(3n)).toMatchObject({ error: null, kind: "complete" });
    expect(events.sort()).toEqual([
      "1:job_interrupted:available",
      "2:job_failed:available",
      "3:job_completed:completed",
    ]);
  });

  it("does not abort a local attempt before transactional cancellation commits", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    driver.cancelled = { ...fakeJob(), state: "cancelled" };
    const definition = defineJob({ kind: "test" });
    let aborted = false;
    let finish!: () => void;
    const handler = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const workers = new Workers().add(definition, ({ signal }) => {
      signal.addEventListener("abort", () => {
        aborted = true;
      });
      return handler;
    });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();
    await waitUntil(() => run.diagnostics.activeAttempts === 1);

    await client.jobs.cancel(101n, { tx: {} });
    expect(aborted).toBe(false);
    finish();
    await waitUntil(() => run.diagnostics.activeAttempts === 0);
    await run.stop();
  });

  it("retains queue capacity until an ignored abort actually settles", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [
      fakeJob("test", 101n),
      fakeJob("test", 102n),
      fakeJob("test", 103n),
    ];
    const definition = defineJob({ kind: "test" });
    const releases: Array<() => void> = [];
    let activeHandlers = 0;
    let maximumActiveHandlers = 0;
    const workers = new Workers().add(
      definition,
      () =>
        new Promise<undefined>((resolve) => {
          activeHandlers++;
          maximumActiveHandlers = Math.max(
            maximumActiveHandlers,
            activeHandlers
          );
          releases.push(() => {
            activeHandlers--;
            resolve(undefined);
          });
        }),
      { timeout: { milliseconds: 1 } }
    );
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 1 },
        },
      },
      workers,
    });
    const run = await client.start();

    await waitUntil(() => releases.length === 1);
    // Polling every millisecond, a queue that released its slot early would
    // claim the next job well within this window.
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(driver.claimed.map(({ id }) => id)).toEqual([101n]);
    expect(driver.completions).toEqual([]);
    expect(run.diagnostics.activeAttempts).toBe(1);

    releases[0]?.();
    await waitUntil(() => releases.length === 2);
    const stopping = run.stop({ mode: "cancel" });
    releases[1]?.();
    await stopping;

    expect(driver.claimed.map(({ id }) => id)).toEqual([101n, 102n]);
    expect(driver.completions.map(({ kind }) => kind)).toEqual([
      "complete",
      "complete",
    ]);
    expect(maximumActiveHandlers).toBe(1);
    expect(run.state).toBe("stopped");
  });

  it("replaces capacity only when the stuck handler requests it", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob("test", 101n), fakeJob("test", 102n)];
    const definition = defineJob({ kind: "test" });
    const releases: Array<() => void> = [];
    const seenTotals: number[] = [];
    const workers = new Workers().add(
      definition,
      () =>
        new Promise<undefined>((resolve) => {
          releases.push(() => resolve(undefined));
        }),
      { timeout: { milliseconds: 1 } }
    );
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 1 },
        },
      },
      stuckHandler: ({ totalStuckJobs }) => {
        seenTotals.push(totalStuckJobs);
        return { addWorkerSlot: totalStuckJobs === 1 };
      },
      jobStuckThreshold: { milliseconds: 1 },
      workers,
    });
    const run = await client.start();

    await waitUntil(() => releases.length === 2);
    expect(driver.claimed.map(({ id }) => id)).toEqual([101n, 102n]);
    expect(seenTotals[0]).toBe(1);
    expect(run.diagnostics.activeAttempts).toBe(2);

    releases.forEach((release) => release());
    await waitUntil(() => driver.completions.length === 2);
    await run.stop();
  });

  it("retains attempt ownership until an async decoder settles", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob("test", 101n), fakeJob("test", 102n)];
    let releaseDecode!: () => void;
    const decoderBlocked = new Promise<void>((resolve) => {
      releaseDecode = resolve;
    });
    const definition = defineJob({
      decode: async (input) => {
        await decoderBlocked;
        return input;
      },
      kind: "test",
    });
    let handlerCalled = false;
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 1 },
        },
      },
      workers: new Workers().add(definition, ({ signal }) => {
        // The stop cancelled the attempt while its decoder ran.
        handlerCalled = signal.aborted;
        signal.throwIfAborted();
      }),
    });
    const run = await client.start();
    await waitUntil(() => run.diagnostics.activeAttempts === 1);

    await expect(
      run.stop({ mode: "cancel", timeout: { milliseconds: 1 } })
    ).rejects.toThrow("timed out");
    expect(driver.claimed.map(({ id }) => id)).toEqual([101n]);
    expect(run.diagnostics.activeAttempts).toBe(1);

    releaseDecode();
    await run.stop();

    expect(handlerCalled).toBe(true);
    expect(driver.completions.map(({ kind }) => kind)).toEqual(["interrupt"]);
  });

  it("records last-write-wins output on error-handler cancellation", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    const order: string[] = [];
    const workers = new Workers().add(definition, (context) => {
      recordOutput({ source: "handler" });
      context.recordOutput({ source: "last-handler-write" });
      order.push("worker");
      // eslint-disable-next-line @typescript-eslint/only-throw-error -- exercises a non-Error failure
      throw "non-Error failure";
    });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      errorHandler: (context, error) => {
        order.push(`error:${String(error)}`);
        context.recordOutput({ source: "error-handler" });
        return { cancel: true };
      },
      hooks: {
        afterWork: (_context, result) => {
          order.push(`after:${result.cancel}`);
        },
      },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(driver.completions[0]).toMatchObject({
      error: {
        error: "non-Error failure",
        trace: "",
      },
      kind: "cancel",
      output: { source: "error-handler" },
      outputSet: true,
    });
    expect(order).toEqual([
      "worker",
      "after:undefined",
      "error:non-Error failure",
    ]);
  });

  it("cancels a stuck-job decoder during shutdown", async () => {
    const driver = new FakeRuntimeDriver();
    const now = Temporal.Now.instant();
    let decodeStarted = false;
    let delivered = false;
    let electedAt: Temporal.Instant | null = null;
    Object.assign(driver, {
      maintenanceCleanJobs: () => 0,
      maintenanceCleanQueues: () => 0,
      maintenanceGetStuck: () => {
        if (delivered) return [];
        delivered = true;
        return [
          {
            ...fakeJob("known", 208n),
            attemptedAt: now.subtract({ hours: 2 }),
          },
        ];
      },
      maintenanceLeaderAcquire: (
        leaderId: string,
        acquiredAt: Temporal.Instant,
        ttlMs: number
      ): RuntimeLeader => {
        electedAt ??= acquiredAt;
        return {
          electedAt,
          expiresAt: acquiredAt.add({ milliseconds: ttlMs }),
          leaderId,
        };
      },
      maintenanceLeaderResign: () => true,
      maintenanceRescue: () => 0,
      maintenanceSchedule: () => 0,
    });
    const definition = defineJob({
      decode: async (value: { value?: string }) => {
        decodeStarted = true;
        await new Promise(() => undefined);
        return value;
      },
      kind: "known",
    });
    const client = new Client(driver, {
      clientId: "runtime-test",
      jobTimeout: null,
      maintenance: {
        electionInterval: { milliseconds: 1 },
        jobCleanerInterval: { milliseconds: 60_000 },
        queueCleanerInterval: { milliseconds: 60_000 },
        rescueAfter: { milliseconds: 1 },
        rescuerInterval: { milliseconds: 1 },
        schedulerInterval: { milliseconds: 60_000 },
      },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();

    await waitUntil(() => decodeStarted);

    await expect(
      run.stop({ mode: "cancel", timeout: { milliseconds: 500 } })
    ).resolves.toBeUndefined();
  });

  it("treats decoder and error-handler failures as nonfatal attempts", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({
      decode: () => {
        throw new Error("invalid persisted payload");
      },
      kind: "test",
    });
    let handlerCalled = false;
    const logged: string[] = [];
    const workers = new Workers().add(definition, () => {
      handlerCalled = true;
    });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      errorHandler: () => {
        throw new Error("application error handler failed");
      },
      logger: {
        debug: () => undefined,
        error: (_attributes, message) => logged.push(message),
        info: () => undefined,
        warn: () => undefined,
      },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(handlerCalled).toBe(false);
    expect(driver.completions[0]).toMatchObject({
      kind: "retry",
      outputSet: false,
    });
    expect(logged).toEqual(["River error handler failed"]);
    expect(run.state).toBe("stopped");
  });

  it("reports a stuck attempt with its worker's timeout, not the client's", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    let finish!: () => void;
    const blocked = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const warnings: { message: string; timeoutMs: unknown }[] = [];
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      jobTimeout: { minutes: 1 },
      logger: {
        debug: () => undefined,
        error: () => undefined,
        info: () => undefined,
        warn: (attributes, message) => {
          warnings.push({ message, timeoutMs: attributes.timeoutMs });
        },
      },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      jobStuckThreshold: { milliseconds: 1 },
      workers: new Workers().add(
        definition,
        () => blocked.then(() => undefined),
        { timeout: { milliseconds: 5 } }
      ),
    });
    const events = client.subscribe({ kinds: ["job_stuck"] });
    const run = await client.start();

    const event = (await events.next()).value;
    finish();
    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(String((event.error as JobStuckError).timeout)).toBe("PT0.005S");
    expect(warnings).toContainEqual({
      message: "River job appears to be stuck",
      timeoutMs: 5,
    });
  });

  it("reports an attempt once after its timeout and stuck margin", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    let finish!: () => void;
    const blocked = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const workers = new Workers().add(definition, () =>
      blocked.then(() => undefined)
    );
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      jobTimeout: { milliseconds: 1 },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      jobStuckThreshold: { milliseconds: 1 },
      workers,
    });
    const events = client.subscribe({ kinds: ["job_stuck"] });
    const run = await client.start();

    const event = (await events.next()).value;
    expect(event).toMatchObject({ job: { id: 101n }, kind: "job_stuck" });
    expect(event.error).toMatchObject({
      jobId: 101n,
      message:
        "River job 101 remained unsettled for 1 ms after its 1 ms timeout",
      name: "JobStuckError",
    });
    const stuck = event.error as JobStuckError;
    expect([stuck.threshold, stuck.timeout].map(String)).toEqual([
      "PT0.001S",
      "PT0.001S",
    ]);
    expect(driver.completions).toEqual([]);

    finish();
    await waitUntil(() => driver.completions.length === 1);
    await run.stop();
    events.close();
  });

  it("arms the job timeout only once an executor starts the attempt", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    const aborts: unknown[] = [];
    const gracePeriods: Temporal.Duration[] = [];
    let executorStarted = false;
    let markStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    const executor: WorkExecutor = {
      name: "deferred",
      start: () => {
        executorStarted = true;
        let finish!: () => void;
        const result = new Promise<undefined>((resolve) => {
          finish = () => resolve(undefined);
        });
        return {
          abort: (reason, { gracePeriod }) => {
            aborts.push(reason);
            gracePeriods.push(gracePeriod);
            finish();
            return Promise.resolve({ terminated: true });
          },
          result,
          started,
        };
      },
    };
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      jobTimeout: { milliseconds: 1 },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().addExecutor(definition, {
        executor,
        handler: null,
      }),
    });
    const run = await client.start();

    await waitUntil(() => executorStarted);
    // Waiting for executor capacity must not spend the attempt's timeout.
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(aborts).toEqual([]);

    markStarted();
    await waitUntil(() => aborts.length === 1);
    expect(aborts[0]).toBeInstanceOf(JobTimeoutError);
    // The executor may end the handler by force after the stuck threshold,
    // which defaults to 10 seconds.
    expect(gracePeriods.map((period) => period.total("milliseconds"))).toEqual([
      10_000,
    ]);
    await waitUntil(() => driver.completions.length === 1);
    await run.stop();
  });

  it("keeps shutdown observable after a graceful stop bound expires", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    let finish!: () => void;
    const blocked = new Promise<void>((resolve) => {
      finish = resolve;
    });
    let aborted = false;
    const cooperativeWorkers = new Workers().add(definition, ({ signal }) => {
      signal.addEventListener("abort", () => {
        aborted = true;
      });
      return blocked.then(() => undefined);
    });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: cooperativeWorkers,
    });
    const run = await client.start();
    await waitUntil(() => run.diagnostics.activeAttempts === 1);

    await expect(run.stop({ timeout: { milliseconds: 1 } })).rejects.toThrow(
      "timed out"
    );
    expect(aborted).toBe(true);
    expect(run.state).toBe("stopping");
    expect(run.diagnostics.activeAttempts).toBe(1);

    finish();
    await expect(run.stop()).resolves.toBeUndefined();
    await expect(run.completed).resolves.toBeUndefined();
    expect(run.state).toBe("stopped");
  });

  it.each([
    {
      expectedOrder: ["before", "error:Error"],
      makeOptions: (order: string[]): ClientOptions => ({
        errorHandler: (_context, error) => {
          order.push(`error:${(error as Error).name}`);
        },
        hooks: {
          afterWork: (_context, result) => {
            order.push(`after:${result.status}`);
          },
          beforeWork: () => {
            order.push("before");
            throw new Error("before failed");
          },
        },
      }),
      name: "beforeWork",
    },
    {
      expectedOrder: ["middleware", "error:Error"],
      makeOptions: (order: string[]): ClientOptions => ({
        errorHandler: (_context, error) => {
          order.push(`error:${(error as Error).name}`);
        },
        hooks: {
          afterWork: (_context, result) => {
            order.push(`after:${result.status}`);
          },
          beforeWork: () => {
            order.push("before");
          },
        },
        middleware: [
          () => {
            order.push("middleware");
            throw new Error("middleware failed");
          },
        ],
      }),
      name: "middleware",
    },
    {
      expectedOrder: ["before", "handler", "after:succeeded", "error:Error"],
      makeOptions: (order: string[]): ClientOptions => ({
        errorHandler: (_context, error) => {
          order.push(`error:${(error as Error).name}`);
        },
        hooks: {
          afterWork: (_context, result) => {
            order.push(`after:${result.status}`);
            throw new Error("after failed");
          },
          beforeWork: () => {
            order.push("before");
          },
        },
      }),
      name: "afterWork",
    },
  ])(
    "persists $name extension failures as ordinary attempts",
    async (testCase) => {
      const driver = new FakeRuntimeDriver();
      driver.claim = [fakeJob()];
      const definition = defineJob({ kind: "test" });
      const order: string[] = [];
      const workers = new Workers().add(definition, () => {
        order.push("handler");
      });
      const client = new Client(driver, {
        clientId: "runtime-test",
        completionBatchSize: 1,
        ...testCase.makeOptions(order),
        queues: {
          default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
        },
        workers,
      });
      const run = await client.start();

      await waitUntil(() => driver.completions.length === 1);
      await run.stop();

      expect(driver.completions[0]?.kind).toBe("retry");
      expect(order).toEqual(testCase.expectedOrder);
      expect(run.state).toBe("stopped");
    }
  );

  it("matches Go work-extension nesting across global and job-kind scopes", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const order: string[] = [];
    const definition = defineJob({
      decode: (input) => {
        order.push("decode");
        return input;
      },
      kind: "test",
    });
    const around =
      (name: string): WorkMiddleware =>
      async (_context, next) => {
        order.push(`${name}-before`);
        const result = await next();
        order.push(`${name}-after`);
        return result;
      };
    const workers = new Workers().add(
      definition,
      () => {
        order.push("handler");
      },
      {
        hooks: {
          afterWork: () => {
            order.push("kind-hook-after");
          },
          beforeWork: () => {
            order.push("kind-hook-before");
          },
        },
        middleware: [around("worker")],
        plugins: [
          {
            hooks: {
              afterWork: () => {
                order.push("kind-plugin-after");
              },
              beforeWork: () => {
                order.push("kind-plugin-before");
              },
            },
            middleware: [around("kind-plugin")],
            name: "kind-plugin",
          },
        ],
      }
    );
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      hooks: {
        afterWork: () => {
          order.push("global-after");
        },
        beforeWork: () => {
          order.push("global-before");
        },
      },
      middleware: [around("global")],
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(order).toEqual([
      "worker-before",
      "global-before",
      "kind-plugin-before",
      "global-before",
      "kind-plugin-before",
      "kind-hook-before",
      "decode",
      "handler",
      "global-after",
      "kind-plugin-after",
      "kind-hook-after",
      "kind-plugin-after",
      "global-after",
      "worker-after",
    ]);
  });

  it("snapshots mutable runtime and job-kind configuration", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    const order: string[] = [];
    const globalMiddleware: WorkMiddleware[] = [
      async (_context, next) => {
        order.push("global");
        return next();
      },
    ];
    const kindMiddleware: WorkMiddleware[] = [
      async (_context, next) => {
        order.push("kind");
        return next();
      },
    ];
    const queues = {
      default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
    };
    const workers = new Workers().add(
      definition,
      () => {
        order.push("handler");
      },
      { middleware: kindMiddleware }
    );
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      middleware: globalMiddleware,
      queues,
      workers,
    });

    globalMiddleware.push(() => {
      order.push("mutated-global");
    });
    kindMiddleware.push(() => {
      order.push("mutated-kind");
    });
    queues.default.maxWorkers = 50;

    const run = await client.start();
    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(order).toEqual(["kind", "global", "handler"]);
    expect(run.diagnostics.queues.default?.maxWorkers).toBe(1);
  });

  it("threads afterWork replacements through every hook", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    const seen: string[] = [];
    let errorHandlerCalled = false;
    const workers = new Workers().add(
      definition,
      () => {
        throw new Error("handler failed");
      },
      {
        hooks: {
          afterWork: (context, result) => {
            seen.push(`kind:${result.status}`);
            context.setMetadata("kind", true);
            return { status: "succeeded" };
          },
        },
        plugins: [
          {
            hooks: {
              afterWork: (context, result) => {
                seen.push(`plugin:${result.status}`);
                context.setMetadata("plugin", true);
                return {
                  error: new Error("plugin reintroduced failure"),
                  status: "failed",
                };
              },
            },
            name: "kind-plugin",
          },
        ],
      }
    );
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      errorHandler: () => {
        errorHandlerCalled = true;
      },
      hooks: {
        afterWork: (context, result) => {
          seen.push(`global:${result.status}`);
          context.setMetadata("global", true);
          return { status: "succeeded" };
        },
      },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(seen).toEqual(["global:failed", "plugin:succeeded", "kind:failed"]);
    expect(errorHandlerCalled).toBe(false);
    expect(driver.completions[0]).toMatchObject({
      kind: "complete",
      metadata: { global: true, kind: true, plugin: true },
    });
  });

  it("keeps metadata independent across the complete work lifecycle", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    const workers = new Workers().add(definition, async (context) => {
      await Promise.resolve();
      setMetadata("phase", "handler-helper");
      context.setMetadata("handler", true);
      context.setMetadata("__proto__", { safe: true });
    });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      hooks: {
        afterWork: (context) => {
          context.setMetadata("phase", "after");
          context.setMetadata("after", true);
        },
        beforeWork: (context) => {
          context.setMetadata("phase", "before");
          setMetadata("before", true);
        },
      },
      middleware: [
        async (context, next) => {
          context.setMetadata("phase", "middleware-before");
          const outcome = await next();
          setMetadata("phase", "middleware-after");
          return outcome;
        },
      ],
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    const metadata = driver.completions[0]?.metadata;
    expect(metadata).toMatchObject({
      after: true,
      before: true,
      handler: true,
      phase: "middleware-after",
    });
    expect(Object.hasOwn(metadata ?? {}, "__proto__")).toBe(true);
    expect(metadata?.__proto__).toEqual({ safe: true });
    expect(() => setMetadata("outside", true)).toThrow("while working a job");
  });

  it("lets outer middleware suppress decode failures without running afterWork", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const order: string[] = [];
    const definition = defineJob({
      decode: () => {
        order.push("decode");
        throw new Error("bad persisted args");
      },
      kind: "test",
    });
    const workers = new Workers().add(
      definition,
      () => {
        order.push("handler");
      },
      {
        hooks: {
          afterWork: () => {
            order.push("after");
          },
          beforeWork: (context) => {
            context.setMetadata("before", true);
            order.push("before");
          },
        },
        middleware: [
          async (_context, next) => {
            order.push("middleware-before");
            try {
              await next();
            } catch {
              order.push("middleware-caught");
            }
          },
        ],
      }
    );
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers,
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(order).toEqual([
      "middleware-before",
      "before",
      "decode",
      "middleware-caught",
    ]);
    expect(driver.completions[0]).toMatchObject({
      kind: "complete",
      metadata: { before: true },
    });
  });

  it("keeps ALS and metadata mutation out of the error handler", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    let hasSetter = true;
    let storedContext: unknown;
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      errorHandler: (context) => {
        hasSetter = "setMetadata" in context;
        storedContext = currentWorkContext();
        context.recordOutput({ handled: true });
      },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => {
        throw new Error("failure");
      }),
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(hasSetter).toBe(false);
    expect(storedContext).toBeUndefined();
    expect(driver.completions[0]).toMatchObject({
      kind: "retry",
      output: { handled: true },
      outputSet: true,
    });
  });

  it("prefers a worker's retry policy over the client's, like Go's Worker.NextRetry", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [
      fakeJob("own", 1n),
      fakeJob("throws", 2n),
      fakeJob("past", 3n),
      fakeJob("undecodable", 4n),
    ];
    const now = Temporal.Instant.from("2026-09-01T00:00:00Z");
    const fail = () => {
      throw new Error("try again");
    };
    const workerRetryAt = now.add({ hours: 1 });
    const clientRetryAt = now.add({ hours: 2 });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 4, pollInterval: { milliseconds: 10_000 } },
      },
      retryPolicy: () => clientRetryAt,
      workers: new Workers()
        .add(defineJob({ kind: "own" }), fail, {
          retryPolicy: () => workerRetryAt,
        })
        .add(defineJob({ kind: "past" }), fail, {
          retryPolicy: (_job, at) => at.subtract({ seconds: 1 }),
        })
        .add(defineJob({ kind: "throws" }), fail, {
          retryPolicy: () => {
            throw new Error("no policy");
          },
        })
        .add(
          defineJob({
            decode: (): never => {
              throw new Error("invalid payload");
            },
            kind: "undecodable",
          }),
          fail,
          { retryPolicy: () => workerRetryAt }
        ),
    });
    overrideRuntimeTiming(client, { now: () => now });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 4);
    await run.stop();

    const scheduledAt = new Map(
      driver.completions.map((item) => [item.id, item.scheduledAt])
    );
    expect(scheduledAt.get(1n)).toEqual(workerRetryAt);
    expect(scheduledAt.get(2n)).toEqual(clientRetryAt);
    // A time in the past uses River's default schedule, about a second for
    // a first attempt.
    const pastRetry = scheduledAt.get(3n)!;
    expect(Temporal.Instant.compare(pastRetry, now)).toBe(1);
    expect(Temporal.Instant.compare(pastRetry, now.add({ seconds: 2 }))).toBe(
      -1
    );
    // Arguments that don't decode leave the decision to the client.
    expect(scheduledAt.get(4n)).toEqual(clientRetryAt);
  });

  it("rejects a worker retry policy that isn't a function", () => {
    expect(() =>
      new Workers().add(defineJob({ kind: "test" }), () => undefined, {
        retryPolicy: "soon" as never,
      })
    ).toThrow(new ConfigurationError("worker retryPolicy must be a function"));
  });

  it("persists retries and snoozes due within a scheduler pass as available", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [
      fakeJob("test", 1n),
      fakeJob("test", 2n),
      fakeJob("test", 3n),
      fakeJob("test", 4n),
    ];
    const definition = defineJob({ kind: "test" });
    const now = Temporal.Instant.from("2026-09-01T00:00:00Z");
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 4, pollInterval: { milliseconds: 10_000 } },
      },
      // Jobs 1 and 2 fail; job 1's retry lands exactly on River's default
      // five second scheduler interval and job 2's just beyond it.
      retryPolicy: (job, at) =>
        at.add({ milliseconds: job.id === 1n ? 5_000 : 5_001 }),
      workers: new Workers().add(definition, ({ job }) => {
        switch (job.id) {
          case 1n:
          case 2n:
            throw new Error("try again");
          case 3n:
            return snooze({ seconds: 5 });
          default:
            return snooze({ milliseconds: 5_001 });
        }
      }),
    });
    overrideRuntimeTiming(client, { now: () => now });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 4);
    await run.stop();

    const byId = new Map(driver.completions.map((item) => [item.id, item]));
    expect(byId.get(1n)).toMatchObject({ available: true, kind: "retry" });
    expect(byId.get(2n)).not.toHaveProperty("available");
    expect(byId.get(3n)).toMatchObject({ available: true, kind: "snooze" });
    expect(byId.get(4n)).not.toHaveProperty("available");
    // The fast path changes only the state: an error keeps its attempt and
    // a snooze refunds it.
    const states = driver.completionBatches
      .flat()
      .map((command) => applyCompletion(fakeJob("test", command.id), command))
      .map(({ attempt, id, state }) => [id, state, attempt]);
    expect(
      states.sort(([left], [right]) => Number(left) - Number(right))
    ).toEqual([
      [1n, "available", 1],
      [2n, "retryable", 1],
      [3n, "available", 0],
      [4n, "scheduled", 0],
    ]);
  });

  it("records attempt errors at the attempt start with traces only for faults", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [
      fakeJob("test", 1n),
      fakeJob("test", 2n),
      fakeJob("test", 3n),
    ];
    const definition = defineJob({ kind: "test" });
    let clock = Temporal.Instant.from("2026-09-01T00:00:00Z");
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, ({ job }) => {
        // Every attempt takes a minute of runtime-clock time.
        clock = clock.add({ minutes: 1 });
        if (job.id === 1n) throw new Error("expected failure");
        if (job.id === 2n) {
          const missing = undefined as unknown as { readonly field: number };
          return void missing.field;
        }
        throw new (class ValidationFailure extends TypeError {})(
          "deliberate subclass"
        );
      }),
    });
    overrideRuntimeTiming(client, { now: () => clock });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 3);
    await run.stop();

    const errors = new Map(
      driver.completions.map(({ error, id }) => [id, error])
    );
    const start = Temporal.Instant.from("2026-09-01T00:00:00Z");
    expect(errors.get(1n)).toEqual({
      at: start,
      error: "expected failure",
      trace: "",
    });
    // Reading a property of undefined is a runtime fault: River's panic.
    expect(errors.get(2n)?.at).toEqual(start.add({ minutes: 1 }));
    expect(errors.get(2n)?.trace).toContain("TypeError");
    expect(errors.get(3n)).toEqual({
      at: start.add({ minutes: 2 }),
      error: "deliberate subclass",
      trace: "",
    });
  });

  it("cancels a job permanently when its handler returns cancel()", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob("test", 1n), fakeJob("test", 2n)];
    const definition = defineJob({ kind: "test" });
    const startedAt = Temporal.Instant.from("2026-09-01T00:00:00Z");
    const events: string[] = [];
    let errorHandlerCalls = 0;
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      errorHandler: () => {
        errorHandlerCalls += 1;
      },
      hooks: {
        onEvent: (event) => {
          if ("job" in event) {
            events.push(`${event.job.id}:${event.kind}:${event.job.state}`);
          }
        },
      },
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, ({ job }) =>
        job.id === 1n ? cancel({ reason: "account closed" }) : cancel()
      ),
    });
    overrideRuntimeTiming(client, { now: () => startedAt });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 2);
    await run.stop();

    // Like Go's `river.JobCancel`, the reason is the attempt error.
    expect(driver.completions).toMatchObject([
      {
        error: {
          at: startedAt,
          error: "JobCancelError: account closed",
          trace: "",
        },
        finalizedAt: startedAt,
        id: 1n,
        kind: "cancel",
      },
      {
        error: { error: "JobCancelError: <nil>", trace: "" },
        id: 2n,
        kind: "cancel",
      },
    ]);
    expect(errorHandlerCalls).toBe(0);
    expect(events).toEqual([
      "1:job_started:running",
      "1:job_cancelled:cancelled",
      "2:job_started:running",
      "2:job_cancelled:cancelled",
    ]);
  });

  it("increments canonical persisted snooze metadata", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [
      {
        ...fakeJob(),
        metadata: { snoozes: "2", user: true },
      },
    ];
    const definition = defineJob({ kind: "test" });
    const beforeSnooze = Temporal.Now.instant();
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => snooze({ milliseconds: 1 })),
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(driver.completions[0]).toMatchObject({
      kind: "snooze",
      metadata: { snoozes: 3 },
    });
    const scheduledAt = driver.completions[0]?.scheduledAt;
    expect(scheduledAt).not.toBeNull();
    expect(
      scheduledAt!.epochNanoseconds - beforeSnooze.epochNanoseconds
    ).toBeGreaterThanOrEqual(1_000_000n);
  });

  it("persists and dynamically reconfigures runtime queues", async () => {
    const driver = new FakeRuntimeDriver();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      leaderElectionDisabled: true,
      queues: { default: { maxWorkers: 1 } },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();

    expect(driver.queues.has("default")).toBe(true);
    expect(
      durationsInMilliseconds(run.diagnostics.queues.default)
    ).toMatchObject({
      fetchCooldown: 100,
      pollInterval: 1_000,
    });
    await run.addQueue("extra", {
      fetchCooldown: { milliseconds: 5 },
      maxWorkers: 2,
      pollInterval: { milliseconds: 25 },
    });
    expect(driver.queues.has("extra")).toBe(true);
    expect(durationsInMilliseconds(run.diagnostics.queues.extra)).toMatchObject(
      {
        fetchCooldown: 5,
        maxWorkers: 2,
        paused: false,
        pollInterval: 25,
      }
    );

    await run.updateQueue("extra", {
      fetchCooldown: { milliseconds: 10 },
      maxWorkers: 3,
      pollInterval: { milliseconds: 50 },
    });
    expect(durationsInMilliseconds(run.diagnostics.queues.extra)).toMatchObject(
      {
        fetchCooldown: 10,
        maxWorkers: 3,
        pollInterval: 50,
      }
    );
    await expect(run.removeQueue("extra")).resolves.toBe(true);
    expect(run.diagnostics.queues.extra).toBeUndefined();

    await run.stop();
    expect(driver.queues.has("default")).toBe(true);
  });

  it("broadcasts leadership resignation without requiring a local term", async () => {
    const driver = new FakeRuntimeDriver();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 100 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const transaction = { exact: true };
    await expect(
      client.requestLeadershipResignation({ tx: transaction })
    ).resolves.toBeUndefined();
    expect(driver.leadershipResignRequests).toBe(1);
    expect(driver.leadershipResignTransaction).toBe(transaction);

    const run = await client.start();

    await expect(run.requestLeadershipResignation()).resolves.toBeUndefined();
    expect(driver.leadershipResignRequests).toBe(2);
    await run.stop();
  });

  it("treats onEvent observer failures as nonfatal after publication", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    const errors: string[] = [];
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      hooks: {
        onEvent: () => {
          throw new Error("observer failed");
        },
      },
      logger: {
        debug: () => undefined,
        error: (_attributes, message) => errors.push(message),
        info: () => undefined,
        warn: () => undefined,
      },
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const events = client.subscribe({ kinds: ["job_completed"] });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    expect((await events.next()).value).toMatchObject({
      kind: "job_completed",
    });
    await run.stop();

    expect(run.state).toBe("stopped");
    expect(errors).toContain("River onEvent hook failed");
  });

  it("reports persisted queue pause transitions recovered by polling", async () => {
    const driver = new FakeRuntimeDriver();
    const definition = defineJob({ kind: "test" });
    const observed: string[] = [];
    const client = new Client(driver, {
      hooks: {
        onEvent: ({ kind }) => {
          observed.push(kind);
        },
      },
      leaderElectionDisabled: true,
      pollOnly: true,
      queueControlPollInterval: { milliseconds: 1 },
      queueHeartbeatInterval: { milliseconds: 60_000 },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const events = client.subscribe({
      kinds: ["queue_paused", "queue_resumed"],
    });
    const run = await client.start();
    const queue = driver.queues.get("default")!;
    const pausedAt = Temporal.Now.instant();

    driver.queues.set("default", { ...queue, pausedAt, updatedAt: pausedAt });
    await waitUntil(() => run.diagnostics.queues.default?.paused === true);
    expect((await events.next()).value).toMatchObject({
      kind: "queue_paused",
      queue: { name: "default", pausedAt },
    });

    const resumedAt = Temporal.Now.instant();
    driver.queues.set("default", {
      ...queue,
      pausedAt: null,
      updatedAt: resumedAt,
    });
    await waitUntil(() => run.diagnostics.queues.default?.paused === false);
    expect((await events.next()).value).toMatchObject({
      kind: "queue_resumed",
      queue: { name: "default", pausedAt: null },
    });
    expect(observed.filter((kind) => kind === "queue_paused")).toHaveLength(1);
    expect(observed.filter((kind) => kind === "queue_resumed")).toHaveLength(1);

    events.close();
    await run.stop();
  });

  it("publishes same-client queue control exactly once", async () => {
    const driver = new FakeRuntimeDriver();
    const definition = defineJob({ kind: "test" });
    const observed: string[] = [];
    const client = new Client(driver, {
      hooks: {
        onEvent: ({ kind }) => {
          observed.push(kind);
        },
      },
      leaderElectionDisabled: true,
      pollOnly: true,
      queueControlPollInterval: { milliseconds: 1 },
      queueHeartbeatInterval: { milliseconds: 60_000 },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();
    driver.queuePause = (name) => {
      const queue = driver.queues.get(name);
      if (queue === undefined) return null;
      const pausedAt = Temporal.Now.instant();
      const paused = { ...queue, pausedAt, updatedAt: pausedAt };
      driver.queues.set(name, paused);
      return paused;
    };
    driver.queueResume = (name) => {
      const queue = driver.queues.get(name);
      if (queue === undefined) return null;
      const updatedAt = Temporal.Now.instant();
      const resumed = { ...queue, pausedAt: null, updatedAt };
      driver.queues.set(name, resumed);
      return resumed;
    };

    await client.queues.pause("default");
    expect(run.diagnostics.queues.default?.paused).toBe(true);
    await client.queues.resume("default");
    expect(run.diagnostics.queues.default?.paused).toBe(false);
    // Let the control poller read the queue several times, so it would have
    // published a duplicate event by now.
    const polls = driver.queueGetCalls;
    await waitUntil(() => driver.queueGetCalls >= polls + 3);

    expect(observed.filter((kind) => kind === "queue_paused")).toHaveLength(1);
    expect(observed.filter((kind) => kind === "queue_resumed")).toHaveLength(1);
    await run.stop();
  });

  it("applies a pause of every queue to this client's queues at once", async () => {
    const driver = new FakeRuntimeDriver();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      leaderElectionDisabled: true,
      pollOnly: true,
      queueControlPollInterval: { seconds: 60 },
      queueHeartbeatInterval: { milliseconds: 60_000 },
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();
    // Like Go, "*" updates every queue row and returns none.
    const setPaused = (paused: boolean) => (name: string) => {
      expect(name).toBe("*");
      const at = Temporal.Now.instant();
      for (const [queueName, queue] of driver.queues) {
        driver.queues.set(queueName, {
          ...queue,
          pausedAt: paused ? at : null,
          updatedAt: at,
        });
      }
      return null;
    };
    driver.queuePause = setPaused(true);
    driver.queueResume = setPaused(false);

    await expect(client.queues.pause("*")).resolves.toBeNull();
    await waitUntil(() => run.diagnostics.queues.default?.paused === true);
    await expect(client.queues.resume("*")).resolves.toBeNull();
    await waitUntil(() => run.diagnostics.queues.default?.paused === false);
    await run.stop();
  });

  it("looks queues up by any name like Go, finding invalid names absent", async () => {
    const driver = new FakeRuntimeDriver();
    const looked: string[] = [];
    Object.assign(driver, {
      queueGet: (name: string) => (looked.push(`get:${name}`), null),
      queuePause: (name: string) => (looked.push(`pause:${name}`), null),
      queueResume: (name: string) => (looked.push(`resume:${name}`), null),
      queueUpdate: (name: string) => (looked.push(`update:${name}`), null),
    });
    const client = new Client(driver, { clientId: "runtime-test" });

    for (const name of ["Not A Queue!", "", "x".repeat(200)]) {
      await expect(client.queues.get(name)).resolves.toBeNull();
      await expect(client.queues.pause(name)).resolves.toBeNull();
      await expect(client.queues.resume(name)).resolves.toBeNull();
      await expect(
        client.queues.update(name, { metadata: {} })
      ).resolves.toBeNull();
    }
    expect(looked).toHaveLength(12);
    expect(looked).toContain("pause:Not A Queue!");
  });

  it("reports an externally discarded stale completion as failed", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    driver.completionOverride = (command) => ({
      job: {
        ...fakeJob(),
        finalizedAt: Temporal.Now.instant(),
        state: "discarded",
      },
      key: jobCompletionKey(command),
      status: "stale",
    });
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const events = client.subscribe();
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect((await events.next()).value.kind).toBe("job_started");
    expect((await events.next()).value.kind).toBe("job_failed");
    events.close();
  });

  it("wraps ordinary and periodic insertions with insert extensions", async () => {
    const driver = new FakeRuntimeDriver();
    const definition = defineJob({ kind: "insert-extension" });
    const order: string[] = [];
    const client = new Client(driver, {
      hooks: {
        afterInsert: (context) => {
          order.push(`after:${context.operation}`);
        },
        beforeInsert: (context) => {
          order.push(`before:${context.operation}`);
        },
      },
      insertMiddleware: [
        async (context, next) => {
          order.push(`middleware-before:${context.operation}`);
          const results = await next();
          order.push(`middleware-after:${context.operation}`);
          return results;
        },
      ],
    });

    await client.insert(definition, {});
    await client.insertMany([
      { args: {}, job: definition },
      { args: {}, job: definition },
    ]);

    // Like River for Go, insert hooks run inside the innermost middleware.
    expect(order).toEqual([
      "middleware-before:insert",
      "before:insert",
      "after:insert",
      "middleware-after:insert",
      "middleware-before:insertMany",
      "before:insertMany",
      "after:insertMany",
      "middleware-after:insertMany",
    ]);
  });

  it("nests work hooks inside the innermost work middleware like Go", async () => {
    const driver = new FakeRuntimeDriver();
    const definition = defineJob({ kind: "nested" });
    driver.claim = [fakeJob("nested", 401n), fakeJob("nested", 402n)];
    const order: string[] = [];
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      hooks: {
        afterWork: ({ job }, result) => {
          order.push(`hook:end:${job.id}`);
          // A work-end hook's result replaces the worker's.
          return job.id === 402n
            ? { error: new Error("replaced"), status: "failed" }
            : result;
        },
        beforeWork: ({ job }) => {
          order.push(`hook:begin:${job.id}`);
        },
      },
      leaderElectionDisabled: true,
      middleware: [
        async ({ job }, next) => {
          order.push(`outer:before:${job.id}`);
          const outcome = await next();
          order.push(`outer:after:${job.id}`);
          return outcome;
        },
        async ({ job }, next) => {
          order.push(`inner:before:${job.id}`);
          const outcome = await next();
          order.push(`inner:after:${job.id}`);
          return outcome;
        },
      ],
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, ({ job }) => {
        order.push(`worker:${job.id}`);
      }),
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 2);
    await run.stop();

    expect(order.filter((entry) => entry.endsWith(":401"))).toEqual([
      "outer:before:401",
      "inner:before:401",
      "hook:begin:401",
      "worker:401",
      "hook:end:401",
      "inner:after:401",
      "outer:after:401",
    ]);
    const byId = new Map(driver.completions.map((c) => [c.id, c]));
    expect(byId.get(401n)?.kind).toBe("complete");
    expect(byId.get(402n)).toMatchObject({
      error: { error: "replaced" },
      kind: "retry",
    });
  });

  it("runs without opening notification streams in poll-only mode", async () => {
    const driver = new FakeRuntimeDriver();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      leaderElectionDisabled: true,
      pollOnly: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 10 },
        },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();
    await run.stop();

    expect(driver.notificationSubscriptions).toBe(0);
  });

  it("claims only the kinds its workers had at start with fetchOnlyKnownKinds", async () => {
    const claims: (readonly string[])[] = [];
    for (const fetchOnlyKnownKinds of [true, false]) {
      const driver = new FakeRuntimeDriver();
      const workers = new Workers()
        .add(
          defineJob({ kind: "second", kindAliases: ["alias"] }),
          () => undefined
        )
        .add(defineJob({ kind: "first" }), () => undefined);
      const client = new Client(driver, {
        fetchOnlyKnownKinds,
        leaderElectionDisabled: true,
        queues: {
          default: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 1,
            pollInterval: { milliseconds: 5 },
          },
        },
        workers,
      });
      const run = await client.start();
      // Like Go, kinds registered after the start don't change claims.
      workers.add(defineJob({ kind: "later" }), () => undefined);
      await waitUntil(() => driver.claimRequests.length >= 2);
      await run.stop();
      claims.push(driver.lastClaim!.kinds);
    }

    // Like Go, a worker's kind aliases are known kinds too.
    expect(claims).toEqual([["alias", "first", "second"], []]);
  });

  it("rejects invalid leaderElectionDisabled and maintenance options", () => {
    expect(
      () =>
        new Client(new FakeRuntimeDriver(), {
          fetchOnlyKnownKinds: "yes" as unknown as boolean,
        })
    ).toThrow(new ValidationError("fetchOnlyKnownKinds must be a boolean"));
    expect(
      () =>
        new Client(new FakeRuntimeDriver(), {
          leaderElectionDisabled: "yes" as unknown as boolean,
        })
    ).toThrow(new ValidationError("leaderElectionDisabled must be a boolean"));
    expect(
      () =>
        new Client(new FakeRuntimeDriver(), {
          maintenance: false as unknown as MaintenanceOptions,
        })
    ).toThrow(new ValidationError("maintenance must be an object"));
  });

  it("rejects periodic jobs on a client with leader election disabled", () => {
    const definition = defineJob({ kind: "test" });
    const periodic = periodicJob({
      args: {},
      every: { minutes: 1 },
      job: definition,
    });

    expect(
      () =>
        new Client(new FakeRuntimeDriver(), {
          leaderElectionDisabled: true,
          periodicJobs: [periodic],
        })
    ).toThrow(
      new ConfigurationError(
        "periodicJobs must be empty when leaderElectionDisabled is true, because this client never leads"
      )
    );
    expect(
      new Client(new FakeRuntimeDriver(), {
        leaderElectionDisabled: true,
        periodicJobs: [],
      }).periodicJobs.size
    ).toBe(0);
  });

  it("rejects periodic job changes on a client with leader election disabled", () => {
    const definition = defineJob({ kind: "test" });
    const periodic = periodicJob({
      args: {},
      every: { minutes: 1 },
      id: "disabled",
      job: definition,
    });
    const enabled = new Client(new FakeRuntimeDriver());
    const handle = enabled.periodicJobs.add(periodic);
    const client = new Client(new FakeRuntimeDriver(), {
      leaderElectionDisabled: true,
    });

    for (const modify of [
      () => client.periodicJobs.add(periodic),
      () => client.periodicJobs.addMany([]),
      () => {
        client.periodicJobs.clear();
      },
      () => client.periodicJobs.remove(handle),
      () => client.periodicJobs.removeById("disabled"),
    ]) {
      expect(modify).toThrow(
        new ConfigurationError(
          "cannot modify periodic jobs when leaderElectionDisabled is true, because this client never leads"
        )
      );
    }
    expect(client.periodicJobs.size).toBe(0);
    expect(enabled.periodicJobs.removeById("disabled")).toBe(true);
  });

  it("works jobs without leader election", async () => {
    const driver = new FakeRuntimeDriver();
    let leaderAcquisitions = 0;
    Object.assign(driver, {
      maintenanceLeaderAcquire: () => {
        leaderAcquisitions++;
        return null;
      },
    });
    driver.claim = [fakeJob()];
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      // Maintenance settings are accepted but have nothing to apply to.
      maintenance: { rescuerInterval: { seconds: 1 } },
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 10 },
        },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    expect(driver.completions[0]).toMatchObject({ id: 101n, kind: "complete" });
    expect(run.diagnostics.maintenance).toBeNull();
    await run.stop();
    expect(leaderAcquisitions).toBe(0);
    expect(driver.notificationSubscriptions).toBe(1);
  });

  it("waits for the notification subscription before start resolves", async () => {
    const driver = new FakeRuntimeDriver();
    const gate = Promise.withResolvers<undefined>();
    driver.notificationReadyGate = gate.promise;
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      leaderElectionDisabled: true,
      queues: { default: { maxWorkers: 1 } },
      workers: new Workers().add(definition, () => undefined),
    });
    let started = false;
    const starting = client.start().then((run) => {
      started = true;
      return run;
    });

    await expect.poll(() => driver.notificationSubscriptions).toBe(1);
    expect(started).toBe(false);
    gate.resolve(undefined);
    const run = await starting;
    expect(started).toBe(true);
    await run.stop();
  });

  it("persists resumable progress only when an attempt fails", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [
      {
        ...fakeJob(),
        metadata: { "river:resumable_step": "first" },
      },
    ];
    const definition = defineJob({ kind: "test" });
    const order: string[] = [];
    const client = new Client(driver, {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 100 } },
      },
      workers: new Workers().add(definition, async ({ resumable }) => {
        await resumable.step("first", () => {
          order.push("first");
        });
        await resumable.step("second", () => {
          order.push("second");
          throw new Error("retry after checkpoint");
        });
      }),
    });
    const run = await client.start();
    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(order).toEqual(["second"]);
    expect(driver.completions[0]).toMatchObject({
      kind: "retry",
      metadata: { "river:resumable_step": "first" },
    });
  });

  it("round-trips exact job cursors through the semantic backend", async () => {
    const driver = new FakeRuntimeDriver();
    driver.listRows = [fakeJob("test", 9_007_199_254_740_993n)];
    const client = new Client(driver);

    const first = await client.jobs.list({ limit: 1, orderBy: "scheduledAt" });
    expect(first.nextCursor).not.toBeNull();
    await client.jobs.list({
      after: first.nextCursor!,
      limit: 1,
      orderBy: "scheduledAt",
    });

    expect(driver.lastList?.after).toMatchObject({
      id: 9_007_199_254_740_993n,
      sortField: "scheduledAt",
      time: Temporal.Instant.from("2026-08-30T12:00:00.123456789Z"),
    });
  });

  it("reports explicit lag on bounded subscriptions", async () => {
    const driver = new FakeRuntimeDriver();
    const client = new Client(driver);
    const subscription = client.subscribe({ capacity: 1 });

    await client.queues.pause("one");
    driver.cancelled = fakeJob();
    await client.jobs.cancel(101n);
    // queuePause returns null in the fake, but cancellation doesn't emit a
    // public event without a matching local runtime. Exercise the hub through
    // a queue transition that returns a row.
    const now = Temporal.Now.instant();
    driver.queuePause = () => ({
      createdAt: now,
      metadata: {},
      name: "one",
      pausedAt: now,
      updatedAt: now,
    });
    driver.queueResume = () => ({
      createdAt: now,
      metadata: {},
      name: "one",
      pausedAt: null,
      updatedAt: now,
    });
    await client.queues.pause("one");
    await client.queues.resume("one");

    const lag = (await subscription.next()).value;
    expect(lag).toMatchObject({ dropped: 1, kind: "subscription_lag" });
    expect((await subscription.next()).value.kind).toBe("queue_resumed");
    subscription.close();
  });
});

describe("Client runtime stress", () => {
  it("settles racing completions and cancellations exactly once", async () => {
    // Bounded, deterministic repetition of the races between a handler
    // settling, a remote cancellation, and transient completion failures.
    // Every attempt must persist exactly one outcome and emit exactly one
    // post-commit event, with no leaked promise.
    const iterations = 50;
    const jobsPerIteration = 4;
    for (let iteration = 0; iteration < iterations; iteration++) {
      const driver = new FakeRuntimeDriver();
      const ids = Array.from({ length: jobsPerIteration }, (_, index) =>
        BigInt(iteration * jobsPerIteration + index + 1)
      );
      driver.claim = ids.map((id) => fakeJob("test", id));
      driver.completionFailures = iteration % 7 === 0 ? 2 : 0;
      Object.defineProperty(driver, "jobCancel", {
        value: (id: bigint) => ({ ...fakeJob("test", id), state: "running" }),
      });
      const gates = new Map(
        ids.map((id) => [id, Promise.withResolvers<undefined>()])
      );
      const behavior = (id: bigint) =>
        (Number(id) + iteration) % 4 === 0
          ? "succeed-after-cancel"
          : (Number(id) + iteration) % 4 === 1
            ? "succeed-before-cancel"
            : (Number(id) + iteration) % 4 === 2
              ? "stop-on-cancel"
              : "fail-while-cancelled";
      const events = new Map<bigint, string[]>();
      const definition = defineJob({ kind: "test" });
      const client = new Client(driver, {
        clientId: "runtime-test",
        completionBatchSize: 2,
        completionFlushInterval: { milliseconds: 0 },
        hooks: {
          onEvent: (event) => {
            if (!("job" in event) || event.kind === "job_started") return;
            const seen = events.get(event.job.id) ?? [];
            seen.push(event.kind);
            events.set(event.job.id, seen);
          },
        },
        leaderElectionDisabled: true,
        queues: {
          default: {
            maxWorkers: jobsPerIteration,
            pollInterval: { milliseconds: 10_000 },
          },
        },
        workers: new Workers().add(definition, async ({ job, signal }) => {
          await gates.get(job.id)?.promise;
          switch (behavior(job.id)) {
            case "stop-on-cancel":
              signal.throwIfAborted();
              return;
            case "fail-while-cancelled":
              throw new Error("handler failed");
            default:
              return;
          }
        }),
      });
      overrideRuntimeTiming(client, {
        random: () => 0.5,
        timer: new FakeTimer(),
      });
      const run = await client.start();
      await waitUntil(() => run.diagnostics.activeAttempts === ids.length);

      for (const id of ids) {
        const gate = gates.get(id);
        if (behavior(id) === "succeed-before-cancel") {
          gate?.resolve(undefined);
          await client.jobs.cancel(id);
        } else {
          await client.jobs.cancel(id);
          gate?.resolve(undefined);
        }
      }
      await waitUntil(() => driver.completions.length === ids.length);
      await run.stop();

      expect(run.state).toBe("stopped");
      const kinds = new Map(
        driver.completions.map((command) => [command.id, command.kind])
      );
      expect(kinds.size).toBe(ids.length);
      for (const id of ids) {
        const expected =
          behavior(id) === "stop-on-cancel" ||
          behavior(id) === "fail-while-cancelled"
            ? "cancel"
            : "complete";
        expect(kinds.get(id), `${iteration}:${id}`).toBe(expected);
        expect(events.get(id), `${iteration}:${id}`).toEqual([
          expected === "cancel" ? "job_cancelled" : "job_completed",
        ]);
      }
    }
  });
});

describe("Client runtime database faults", () => {
  const nonRetryable = () =>
    new DatabaseOperationError("relation is locked in an unexpected way", {
      backend: "fake",
      operation: "fake",
    });
  const retryable = () =>
    new DatabaseOperationError("statement timeout", {
      backend: "fake",
      operation: "fake",
      retryable: true,
    });

  function faultClient(
    driver: FakeRuntimeDriver,
    logs: LogEntry[],
    options: Partial<ClientOptions> = {}
  ) {
    const timer = new FakeTimer();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      logger: recordingLogger(logs),
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, () => undefined),
      ...options,
    });
    overrideRuntimeTiming(client, { random: () => 0.5, timer });
    return { client, timer };
  }

  it("jitters the queue poll interval by up to a tenth, like Go", async () => {
    const driver = new FakeRuntimeDriver();
    const logs: LogEntry[] = [];
    const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout");
    const { client } = faultClient(driver, logs, {
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 50 },
        },
        other: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
    });
    const run = await client.start();
    await waitUntil(() => driver.claimRequests.length >= 2);
    await run.stop();
    const delays = setTimeoutSpy.mock.calls.map(([, delay]) => delay);
    setTimeoutSpy.mockRestore();

    // With random() at 0.5: half of the 10 ms minimum, and half of a tenth.
    expect(delays).toContain(55);
    expect(delays).toContain(10_500);
  });

  it("backs off failed claims and keeps working", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    driver.claimFaults.push(nonRetryable(), retryable(), new Error("decode"));
    const logs: LogEntry[] = [];
    const { client, timer } = faultClient(driver, logs);
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(run.state).toBe("stopped");
    expect(timer.delays).toEqual([250, 500, 1_000]);
    expect(
      logs.map(({ attributes, level }) => [
        level,
        attributes?.queue,
        attributes?.retryable,
      ])
    ).toEqual([
      ["error", "default", false],
      ["warn", "default", true],
      ["error", "default", false],
    ]);
  });

  it("fails the runtime when claims reveal a configuration error", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claimFaults.push(new ConfigurationError("pool is too small"));
    const { client } = faultClient(driver, []);
    const run = await client.start();

    await expect(run.completed).rejects.toThrow("River background task failed");
    expect(run.state).toBe("failed");
    await expect(run.stop()).rejects.toThrow("River background task failed");
  });

  it("keeps polling queue controls through database failures", async () => {
    const driver = new FakeRuntimeDriver();
    const logs: LogEntry[] = [];
    const events: string[] = [];
    const { client, timer } = faultClient(driver, logs, {
      hooks: { onEvent: ({ kind }) => void events.push(kind) },
      queueControlPollInterval: { milliseconds: 1 },
    });
    const run = await client.start();
    await waitUntil(() => driver.queueGetCalls > 0);

    driver.queueGetFaults.push(nonRetryable(), retryable());
    const queue = driver.queues.get("default");
    if (queue === undefined) throw new Error("queue was not persisted");
    driver.queues.set("default", {
      ...queue,
      pausedAt: Temporal.Now.instant(),
    });

    await waitUntil(() => events.includes("queue_paused"));
    expect(run.diagnostics.queues.default?.paused).toBe(true);
    expect(run.state).toBe("running");
    expect(timer.delays).toEqual([250, 500]);
    expect(logs.map(({ message }) => message)).toEqual([
      "River queue control poll failed; retrying after backoff",
      "River queue control poll failed; retrying after backoff",
    ]);
    await run.stop();
  });

  it("cancels a job whose cancellation arrived while its claim was in flight", async () => {
    const driver = new FakeRuntimeDriver();
    driver.claim = [fakeJob()];
    const release = Promise.withResolvers<undefined>();
    driver.claimGate = release.promise;
    const handled = Promise.withResolvers<undefined>();
    driver.notificationStream = (_topics, signal, ready) => ({
      [Symbol.asyncIterator]: () => {
        ready();
        let sent = false;
        return {
          next: async (): Promise<IteratorResult<RuntimeNotification>> => {
            if (!sent) {
              // Once the claim took the job, and before it returns.
              await waitUntil(() => driver.claimed.length === 1);
              sent = true;
              return {
                done: false,
                value: {
                  payload: '{"action":"cancel","job_id":101,"queue":"default"}',
                  topic: "control",
                },
              };
            }
            // The pump asks for more only after handling the cancellation.
            handled.resolve(undefined);
            await new Promise<void>((resolve) =>
              signal.addEventListener("abort", () => resolve(), { once: true })
            );
            return { done: true, value: undefined };
          },
        };
      },
    });
    // Like River for Go, the worker starts with its cancellation applied.
    let startedCancelled: boolean | undefined;
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(defineJob({ kind: "test" }), ({ signal }) => {
        startedCancelled = signal.aborted;
        signal.throwIfAborted();
      }),
    });
    const run = await client.start();

    await handled.promise;
    release.resolve(undefined);
    await waitUntil(() => driver.completions.length === 1);
    await run.stop();

    expect(startedCancelled).toBe(true);
    expect(driver.completions[0]).toMatchObject({ id: 101n, kind: "cancel" });
  });

  it("fails to start before claiming when the notification stream can't subscribe", async () => {
    const driver = new FakeRuntimeDriver();
    const failure = new DatabaseOperationError("LISTEN is not supported", {
      backend: "fake",
      operation: "listen",
      retryable: true,
    });
    driver.notificationStream = () => ({
      [Symbol.asyncIterator]: () => ({
        next: () => Promise.reject(failure),
      }),
    });
    driver.claim = [fakeJob()];
    const logs: LogEntry[] = [];
    const { client } = faultClient(driver, logs);

    await expect(client.start()).rejects.toBe(failure);
    expect(driver.claimRequests).toEqual([]);
    expect(driver.completions).toEqual([]);
    // The completer did nothing wrong, so it reports no failure of its own.
    expect(logs.map(({ message }) => message)).not.toContain(
      "River completer failed while stopping"
    );
  });

  it("resubscribes a failed notification stream and polls for missed work", async () => {
    const driver = new FakeRuntimeDriver();
    const listenerLost = Promise.withResolvers<undefined>();
    let subscriptions = 0;
    // Each subscription yields nothing: the first fails once the listener is
    // lost, the second ends unexpectedly, and the third lasts until shutdown.
    driver.notificationStream = (_topics, signal, ready) => ({
      [Symbol.asyncIterator]: () => {
        subscriptions++;
        const subscription = subscriptions;
        ready();
        return {
          next: async (): Promise<IteratorResult<RuntimeNotification>> => {
            if (subscription === 1) {
              await listenerLost.promise;
              throw new DatabaseOperationError("listener connection lost", {
                backend: "fake",
                operation: "listen",
                retryable: true,
              });
            }
            if (subscription > 2) {
              await new Promise<void>((resolve) =>
                signal.addEventListener("abort", () => resolve(), {
                  once: true,
                })
              );
            }
            return { done: true, value: undefined };
          },
        };
      },
    });
    const logs: LogEntry[] = [];
    const { client, timer } = faultClient(driver, logs);
    const run = await client.start();
    await waitUntil(() => driver.claimRequests.length === 1);

    // This job's insert notification was lost with the listener. With a 10
    // second poll interval it is claimed promptly only because every
    // successful resubscription polls.
    driver.claim = [fakeJob()];
    listenerLost.resolve(undefined);

    await waitUntil(
      () => driver.completions.length === 1 && subscriptions === 3
    );
    expect(run.state).toBe("running");
    expect(timer.delays).toEqual([250, 250]);
    expect(logs.map(({ level, message }) => [level, message])).toEqual([
      [
        "warn",
        "River runtime notification stream failed; retrying after backoff",
      ],
      [
        "error",
        "River runtime notification stream failed; retrying after backoff",
      ],
    ]);
    await run.stop();
    expect(run.state).toBe("stopped");
  });

  it("resubscribes a failed remote cancellation stream", async () => {
    const driver = new FakeRuntimeDriver();
    Object.defineProperty(driver, "runtimeNotificationSubscribe", {
      value: undefined,
    });
    let subscriptions = 0;
    let handlerSignal: AbortSignal | undefined;
    const handlerStarted = Promise.withResolvers<undefined>();
    Object.defineProperty(driver, "jobCancellationSubscribe", {
      value: async function* (attemptedBy: string, signal: AbortSignal) {
        subscriptions++;
        if (subscriptions === 1) throw new Error("listener lost");
        await handlerStarted.promise;
        yield { attemptedBy, id: 101n };
        await new Promise<void>((resolve) =>
          signal.addEventListener("abort", () => resolve(), { once: true })
        );
      },
    });
    driver.claim = [fakeJob()];
    const logs: LogEntry[] = [];
    const timer = new FakeTimer();
    const definition = defineJob({ kind: "test" });
    const client = new Client(driver, {
      clientId: "runtime-test",
      completionBatchSize: 1,
      logger: recordingLogger(logs),
      leaderElectionDisabled: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
      workers: new Workers().add(definition, async ({ signal }) => {
        handlerSignal = signal;
        handlerStarted.resolve(undefined);
        await new Promise<void>((resolve) =>
          signal.addEventListener("abort", () => resolve(), { once: true })
        );
        signal.throwIfAborted();
      }),
    });
    overrideRuntimeTiming(client, { random: () => 0.5, timer });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    expect(handlerSignal?.reason).toMatchObject({ name: "JobCancelledError" });
    expect(driver.completions[0]?.kind).toBe("cancel");
    expect(subscriptions).toBe(2);
    expect(logs).toHaveLength(1);
    await run.stop();
  });

  for (const [stream, read] of [
    ["remote cancellation", "each row"],
    ["runtime notification", "each row"],
    ["runtime notification", "batched cancellation requests"],
  ] as const) {
    it(`cancels an attempt whose notice was lost while the ${stream} stream reconnected, reading ${read}`, async () => {
      const driver = new FakeRuntimeDriver();
      if (read === "batched cancellation requests") {
        Object.assign(driver, {
          jobGet: () => {
            throw new Error("recovery reads cancellation requests in batches");
          },
          jobGetCancelRequested: (ids: readonly bigint[]) =>
            ids.filter((id) =>
              driver.listRows.some(
                (row) =>
                  row.id === id &&
                  row.state === "running" &&
                  row.metadata.cancel_attempted_at !== undefined
              )
            ),
        });
      }
      let subscriptions = 0;
      const handlerStarted = Promise.withResolvers<undefined>();
      // The first subscription fails once the job runs; the job is cancelled
      // before the second one connects, so its notice never arrives.
      const failThenIdle = async (signal: AbortSignal) => {
        subscriptions++;
        if (subscriptions === 1) {
          await handlerStarted.promise;
          driver.listRows = driver.claimed.map((job) => ({
            ...job,
            metadata: {
              ...job.metadata,
              cancel_attempted_at: "2026-01-02T03:04:05Z",
            },
          }));
          throw new Error("listener lost");
        }
        await new Promise<void>((resolve) =>
          signal.addEventListener("abort", () => resolve(), { once: true })
        );
      };
      if (stream === "remote cancellation") {
        Object.defineProperty(driver, "runtimeNotificationSubscribe", {
          value: undefined,
        });
        Object.defineProperty(driver, "jobCancellationSubscribe", {
          // eslint-disable-next-line require-yield -- never yields a notice
          value: async function* (
            _attemptedBy: string,
            signal: AbortSignal,
            ready: () => void
          ) {
            ready();
            await failThenIdle(signal);
          },
        });
      } else {
        driver.notificationStream = (_topics, signal, ready) => ({
          [Symbol.asyncIterator]: () => {
            ready();
            return {
              next: async (): Promise<IteratorResult<RuntimeNotification>> => {
                await failThenIdle(signal);
                return { done: true, value: undefined };
              },
            };
          },
        });
      }
      driver.claim = [fakeJob()];
      let handlerSignal: AbortSignal | undefined;
      const timer = new FakeTimer();
      const definition = defineJob({ kind: "test" });
      const client = new Client(driver, {
        clientId: "runtime-test",
        completionBatchSize: 1,
        leaderElectionDisabled: true,
        logger: recordingLogger([]),
        queues: {
          default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
        },
        workers: new Workers().add(definition, async ({ signal }) => {
          handlerSignal = signal;
          handlerStarted.resolve(undefined);
          await new Promise<void>((resolve) =>
            signal.addEventListener("abort", () => resolve(), { once: true })
          );
          signal.throwIfAborted();
        }),
      });
      overrideRuntimeTiming(client, { random: () => 0.5, timer });
      const run = await client.start();

      await waitUntil(() => driver.completions.length === 1);
      expect(handlerSignal?.reason).toMatchObject({
        name: "JobCancelledError",
      });
      expect(driver.completions[0]?.kind).toBe("cancel");
      expect(subscriptions).toBe(2);
      await run.stop();
    });
  }

  it("stops without waiting for a database connection during an outage", async () => {
    const driver = new FakeRuntimeDriver();
    // Every operation waits for a pool connection that never comes. A claim
    // that honors its signal stops waiting when the runtime stops.
    const outage = new Promise<never>(() => undefined);
    const claimSignals: (AbortSignal | undefined)[] = [];
    Object.defineProperty(driver, "jobClaim", {
      value: (
        _params: JobClaimParams,
        options?: { readonly signal?: AbortSignal }
      ) => {
        claimSignals.push(options?.signal);
        return new Promise<never>((_resolve, reject) => {
          options?.signal?.addEventListener(
            "abort",
            () => reject(options.signal?.reason),
            { once: true }
          );
        });
      },
    });
    const { client } = faultClient(driver, [], {
      queueControlPollInterval: { milliseconds: 1 },
    });
    const run = await client.start();
    await waitUntil(() => claimSignals.length === 1);
    let hungReads = 0;
    const hang = () => {
      hungReads++;
      return outage;
    };
    Object.defineProperty(driver, "queueGet", { value: hang });
    // A heartbeat only stops waiting for its connection.
    Object.defineProperty(driver, "runtimeQueueUpsert", {
      value: (
        _name: string,
        _now: Temporal.Instant,
        options?: { readonly signal?: AbortSignal }
      ) =>
        new Promise<never>((_resolve, reject) => {
          hungReads++;
          options?.signal?.addEventListener(
            "abort",
            () => reject(options.signal?.reason),
            { once: true }
          );
        }),
    });
    await waitUntil(() => hungReads === 1);

    await run.stop({ timeout: { seconds: 2 } });

    expect(run.state).toBe("stopped");
    expect(claimSignals[0]?.aborted).toBe(true);
  });

  it("isolates a hung claim to its own queue", async () => {
    const driver = new FakeRuntimeDriver();
    const hung = Promise.withResolvers<JobClaimResult>();
    const claim = driver.jobClaim.bind(driver);
    Object.defineProperty(driver, "jobClaim", {
      value: (params: JobClaimParams) =>
        params.queues[0]?.name === "stuck" ? hung.promise : claim(params),
    });
    driver.claim = [fakeJob()];
    const { client } = faultClient(driver, [], {
      queues: {
        default: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
        stuck: { maxWorkers: 1, pollInterval: { milliseconds: 10_000 } },
      },
    });
    const run = await client.start();

    await waitUntil(() => driver.completions.length === 1);
    await expect(run.stop({ timeout: { milliseconds: 20 } })).rejects.toThrow(
      "River runtime stop timed out"
    );
    hung.resolve({ jobs: [] });
    await run.completed;
    expect(run.state).toBe("stopped");
  });
});

async function waitUntil(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!predicate()) {
    if (Date.now() > deadline)
      throw new Error("timed out waiting for condition");
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}
