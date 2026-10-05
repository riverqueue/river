import { describe, expect, it } from "vitest";

import type {
  JobClaimParams,
  JobClaimResult,
  JobCompletionCommand,
  JobCompletionResult,
  QueueRow,
  RuntimeDriver,
} from "./driver.js";
import type { ClientOptions } from "./options.js";
import { ExtensionError, LifecycleError, ValidationError } from "./errors.js";
import { registerDriver } from "./internal/driver-registry.js";
import { ManualTimer } from "./internal/manual-timer.js";
import { createJobArgsTransformPlugin } from "./job-args-transform.js";
import { defineJob } from "./job-definition.js";
import type { JobRow } from "./job.js";
import type { Logger } from "./logger.js";
import type { QueueConfig } from "./options.js";
import type {
  Pilot,
  PilotAttempts,
  PilotDatabase,
  PilotHost,
  PilotProducer,
  ProducerConfiguration,
  ProducerStartContext,
} from "./pilot.js";
import { PilotClient } from "./pilot-client.js";
import { overrideRuntimeTiming } from "./runtime.js";
import {
  complete,
  snooze,
  Workers,
  type WorkContext,
  type WorkerOptions,
} from "./worker.js";

type Transaction = { readonly tx: string };

/** An in-memory runtime backend with one queue table and one job table. */
class FakeDriver implements RuntimeDriver<Transaction> {
  declare readonly "~river"?: {
    readonly capability: "runtime";
    readonly transaction: Transaction;
  };
  readonly available: JobRow[] = [];
  readonly claims: { readonly limit: number; readonly tx: unknown }[] = [];
  readonly queues = new Map<string, QueueRow>();

  jobCancel(): null {
    return null;
  }

  leader: { electedAt: Temporal.Instant; leaderId: string } | null = null;

  maintenanceCleanJobs(): number {
    return 0;
  }

  maintenanceCleanQueues(): number {
    return 0;
  }

  maintenanceGetStuck(): readonly JobRow[] {
    return [];
  }

  maintenanceLeaderAcquire(
    leaderId: string,
    now: Temporal.Instant,
    ttlMs: number,
    held: { readonly electedAt: Temporal.Instant } | null
  ) {
    if (this.leader === null) this.leader = { electedAt: now, leaderId };
    if (this.leader.leaderId !== leaderId) return null;
    if (held !== null && !held.electedAt.equals(this.leader.electedAt)) {
      return null;
    }
    return { ...this.leader, expiresAt: now.add({ milliseconds: ttlMs }) };
  }

  maintenanceLeaderResign(): boolean {
    this.leader = null;
    return true;
  }

  maintenanceRescue(): number {
    return 0;
  }

  maintenanceSchedule(): number {
    return 0;
  }

  jobClaim(
    params: JobClaimParams,
    options?: { readonly tx?: Transaction }
  ): JobClaimResult {
    const queue = params.queues[0];
    if (queue === undefined) return { jobs: [] };
    this.claims.push({ limit: queue.limit, tx: options?.tx });
    const jobs: JobRow[] = [];
    for (const job of [...this.available]) {
      if (jobs.length >= queue.limit || job.queue !== queue.name) continue;
      this.available.splice(this.available.indexOf(job), 1);
      jobs.push({
        ...job,
        attempt: job.attempt + 1,
        attemptedBy: [...job.attemptedBy, params.attemptedBy],
        state: "running",
      });
    }
    return { jobs };
  }

  /** Holds background completion batches, which have no transaction. */
  completionGate: Promise<void> | undefined;
  readonly completionCalls: { readonly tx: unknown }[] = [];
  /** Every completion command, background or transactional. */
  readonly completionCommands: JobCompletionCommand[] = [];
  /** IDs of the jobs background completion batches persisted. */
  readonly completedIds: bigint[] = [];
  completionsInFlight = 0;
  maxCompletionsInFlight = 0;

  async jobCompleteMany(
    commands: readonly JobCompletionCommand[],
    options?: { readonly tx?: Transaction }
  ): Promise<readonly JobCompletionResult[]> {
    this.completionCalls.push({ tx: options?.tx });
    this.completionCommands.push(...commands);
    if (options?.tx === undefined) {
      this.completionsInFlight++;
      this.maxCompletionsInFlight = Math.max(
        this.maxCompletionsInFlight,
        this.completionsInFlight
      );
      try {
        await this.completionGate;
      } finally {
        this.completionsInFlight--;
      }
      this.completedIds.push(...commands.map(({ id }) => id));
    }
    return commands.map((command) => ({
      job: null,
      key: `${command.id}:${command.attempt}:${command.attemptedBy}`,
      status: "stale",
    }));
  }

  jobDelete() {
    return { status: "not_found" as const };
  }

  jobDeleteMany(): readonly JobRow[] {
    return [];
  }

  jobGet(): null {
    return null;
  }

  jobInsert(): never {
    throw new Error("not used");
  }

  jobInsertMany(): never {
    throw new Error("not used");
  }

  jobList(): readonly JobRow[] {
    return [];
  }

  jobRetry(): null {
    return null;
  }

  jobUpdate(): null {
    return null;
  }

  queueGets = 0;

  queueGet(name: string): QueueRow | null {
    this.queueGets++;
    return this.queues.get(name) ?? null;
  }

  queueList(): readonly QueueRow[] {
    return [];
  }

  queuePause(): null {
    return null;
  }

  queueResume(): null {
    return null;
  }

  queueUpdate(): null {
    return null;
  }

  runtimeQueueUpsert(name: string, now: Temporal.Instant): QueueRow {
    const row = this.queues.get(name) ?? {
      createdAt: now,
      metadata: {},
      name,
      pausedAt: null,
      updatedAt: now,
    };
    this.queues.set(name, row);
    return row;
  }
}

const database: PilotDatabase<Transaction> = {
  backend: "fake",
  connection: (callback) => Promise.resolve(callback({ tx: "connection" })),
  deleteFinalizedJobs: () => Promise.resolve(0),
  loadClaimed: () => Promise.resolve({ jobs: [] }),
  notify: () => Promise.resolve(),
  schema: null,
  transaction: (callback) => Promise.resolve(callback({ tx: "claim" })),
};

class TestClient extends PilotClient<Transaction> {}

const job = defineJob({ kind: "lifetime_job" });

function jobRow(
  id: bigint,
  kind: string = job.kind,
  queue = "default"
): JobRow {
  const now = Temporal.Now.instant();
  return {
    args: {},
    attempt: 0,
    attemptedAt: null,
    attemptedBy: [],
    createdAt: now,
    errors: [],
    finalizedAt: null,
    id,
    kind,
    maxAttempts: 25,
    metadata: {},
    priority: 1,
    queue,
    scheduledAt: now,
    state: "available",
    tags: [],
    uniqueKey: null,
    uniqueStates: null,
  };
}

interface Deferred<T = undefined> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
  reject(reason: unknown): void;
}

function deferred<T = undefined>(): Deferred<T> {
  return Promise.withResolvers<T>();
}

async function waitUntil(predicate: () => boolean): Promise<void> {
  for (let turn = 0; turn < 10_000; turn++) {
    if (predicate()) return;
    await new Promise((resolve) => setImmediate(resolve));
  }
  throw new Error("condition was not reached");
}

const silentLogger: Logger = {
  debug: () => undefined,
  error: () => undefined,
  info: () => undefined,
  warn: () => undefined,
};

interface Setup {
  readonly client: TestClient;
  readonly driver: FakeDriver;
  readonly errors: string[];
  readonly log: string[];
  readonly timer: ManualTimer;
}

function setup(
  producer: (
    context: ProducerStartContext<Transaction>,
    log: string[]
  ) => PilotProducer<Transaction> | Promise<PilotProducer<Transaction>>,
  options: {
    readonly client?: Partial<ClientOptions<Transaction>>;
    readonly completionBatchSize?: number;
    /** The pilot's database; default: one whose transactions always commit. */
    readonly database?: PilotDatabase<Transaction>;
    readonly maintenance?: boolean;
    readonly pilot?: Partial<Pilot<Transaction>>;
    readonly queueControlPollInterval?: { readonly milliseconds: number };
    readonly queues?: Readonly<Record<string, QueueConfig>>;
    readonly workers?: Workers<Transaction>;
  } = {}
): Setup {
  const driver = new FakeDriver();
  registerDriver<Transaction>(driver, {
    backend: "fake",
    capability: "runtime",
    database: options.database ?? database,
    operations: driver,
  });
  const log: string[] = [];
  const errors: string[] = [];
  const timer = new ManualTimer();
  const client = new TestClient(
    driver,
    {
      clientId: "lifetime-client",
      ...(options.completionBatchSize === undefined
        ? {}
        : { completionBatchSize: options.completionBatchSize }),
      eventLoopDelay: false,
      leaderElectionDisabled: options.maintenance !== true,
      ...(options.maintenance === true
        ? {
            maintenance: {
              electionInterval: { milliseconds: 5 },
              rescuerInterval: { hours: 1 },
            },
          }
        : {}),
      logger: {
        ...silentLogger,
        error: (attributes, message) => {
          errors.push(`${message}: ${String(attributes.error)}`);
        },
      },
      pollOnly: true,
      ...(options.queueControlPollInterval === undefined
        ? {}
        : { queueControlPollInterval: options.queueControlPollInterval }),
      queues: options.queues ?? {
        default: { maxWorkers: 5, pollInterval: { minutes: 1 } },
      },
      workers: options.workers ?? new Workers().add(job, () => undefined),
      ...options.client,
    },
    () => ({
      ...options.pilot,
      startProducer: async (context) => {
        log.push(`start ${context.queue.name}`);
        return producer(context, log);
      },
    })
  );
  overrideRuntimeTiming(client, { random: () => 0.5, timer });
  return { client, driver, errors, log, timer };
}

describe("pilot producer sessions", () => {
  it("claims through the session and finishes each handed-off job once", async () => {
    const finished: JobRow[] = [];
    const claimed: JobRow[] = [];
    const blockers = new Map<bigint, Deferred>();
    const { client, driver } = setup(
      () => ({
        async claim(context, next) {
          const result = await database.transaction((tx) => next({ tx }));
          claimed.push(...result.jobs);
          expect(context.kinds).toEqual([]);
          expect(context.limit).toBe(5);
          expect(context.retrySignal.aborted).toBe(false);
          return result;
        },
        jobFinished(row) {
          finished.push(row);
        },
      }),
      {
        workers: new Workers().add(job, async ({ job: row }) => {
          await blockers.get(row.id)?.promise;
        }),
      }
    );
    driver.available.push(jobRow(1n), jobRow(2n, "unknown_kind"), jobRow(3n));
    blockers.set(3n, deferred());

    const run = await client.start();
    await waitUntil(() => finished.length === 2);
    // Job 3 is still being worked.
    expect(finished.map(({ id }) => id).sort()).toEqual([1n, 2n]);
    blockers.get(3n)?.resolve(undefined);
    await waitUntil(() => finished.length === 3);
    await run.stop();

    expect(driver.claims[0]?.tx).toEqual({ tx: "claim" });
    // Each is the row as claimed, reported exactly once.
    expect(finished.length).toBe(3);
    for (const row of finished) expect(claimed).toContain(row);
  });

  it("hands a session the kinds a client with fetchOnlyKnownKinds claims", async () => {
    const kinds: (readonly string[])[] = [];
    const { client } = setup(
      () => ({
        async claim(context, next) {
          kinds.push(context.kinds);
          return database.transaction((tx) => next({ tx }));
        },
      }),
      { client: { fetchOnlyKnownKinds: true } }
    );

    const run = await client.start();
    await waitUntil(() => kinds.length > 0);
    await run.stop();

    expect(kinds[0]).toEqual([job.kind]);
  });

  it("finishes an undecodable job once, with its fallback row", async () => {
    const finished: JobRow[] = [];
    const fallback = {
      ...jobRow(7n),
      attempt: 1,
      attemptedBy: ["lifetime-client"],
      state: "running" as const,
    };
    const { client } = setup(() => {
      let served = false;
      return {
        claim() {
          if (served) return Promise.resolve({ jobs: [] });
          served = true;
          return Promise.resolve({
            decodeErrors: new Map([[fallback.id, new Error("bad args")]]),
            jobs: [fallback],
          });
        },
        jobFinished(row) {
          finished.push(row);
        },
      };
    });

    const run = await client.start();
    await waitUntil(() => finished.length === 1);
    await run.stop();

    expect(finished).toEqual([fallback]);
    expect(finished[0]).toBe(fallback);
  });

  it("retries a rejected claim after backoff and finishes nothing for it", async () => {
    const finished: bigint[] = [];
    let attempts = 0;
    const { client, driver, timer } = setup(() => ({
      async claim(_context, next) {
        attempts++;
        if (attempts === 1) throw new Error("claim transaction failed");
        return database.transaction((tx) => next({ tx }));
      },
      jobFinished(row) {
        finished.push(row.id);
      },
    }));
    driver.available.push(jobRow(1n));

    const run = await client.start();
    const backoff = await timer.waitFor(({ kind }) => kind === "delay");
    expect(attempts).toBe(1);
    // The claim backs off like River's own claim failures.
    await timer.advance(backoff.ms);
    await waitUntil(() => finished.length === 1);
    await run.stop();

    expect(attempts).toBe(2);
    expect(finished).toEqual([1n]);
  });

  it("stops the runtime when a claim breaks its contract", async () => {
    const finished: bigint[] = [];
    const worked: bigint[] = [];
    const foreign = {
      ...jobRow(9n, job.kind, "other"),
      attempt: 1,
      attemptedBy: ["lifetime-client"],
      state: "running" as const,
    };
    const { client } = setup(
      () => ({
        claim: () => Promise.resolve({ jobs: [foreign] }),
        jobFinished(row) {
          finished.push(row.id);
        },
      }),
      {
        workers: new Workers().add(job, ({ job: row }) => {
          worked.push(row.id);
        }),
      }
    );

    const run = await client.start();
    await expect(run.completed).rejects.toMatchObject({
      cause: expect.objectContaining({
        message: expect.stringContaining("job 9 of another queue") as unknown,
      }) as unknown,
    });

    expect(worked).toEqual([]);
    expect(finished).toEqual([]);
  });

  it("stops the runtime when a claim breaks its contract during a stop", async () => {
    const entered = deferred();
    const gate = deferred();
    const { client, errors } = setup(() => ({
      claim: async () => {
        entered.resolve(undefined);
        await gate.promise;
        return {
          jobs: [
            {
              ...jobRow(9n, job.kind, "other"),
              attempt: 1,
              attemptedBy: ["lifetime-client"],
              state: "running" as const,
            },
          ],
        };
      },
    }));

    const run = await client.start();
    await entered.promise;
    const stopping = run.stop();
    gate.resolve(undefined);
    await expect(run.completed).rejects.toMatchObject({
      cause: expect.objectContaining({
        message: expect.stringContaining("job 9 of another queue") as unknown,
      }) as unknown,
    });
    await stopping.catch(() => undefined);

    expect(errors).toContainEqual(
      expect.stringContaining("River producer's claim broke its contract")
    );
  });

  it("rejects claims that break the rules River checks", async () => {
    const running = (id: bigint, overrides: Partial<JobRow> = {}): JobRow => ({
      ...jobRow(id),
      attempt: 1,
      attemptedBy: ["lifetime-client"],
      state: "running",
      ...overrides,
    });
    const withoutId = Object.fromEntries(
      Object.entries(running(1n)).filter(([key]) => key !== "id")
    );
    for (const [result, reason] of [
      [{ jobs: [running(1n), running(1n)] }, "job 1 twice"],
      [{ jobs: [running(1n, { attempt: 0 })] }, "job 1, which has no attempt"],
      [{ jobs: [withoutId] }, "a job without an ID"],
      [{ decodeErrors: [] }, "a result without a job list"],
      [
        { decodeErrors: new Map([[1n, "bad"]]), jobs: [running(1n)] },
        "a decode error that isn't an Error keyed by a job ID",
      ],
      [
        { decodeErrors: new Map([[2n, new Error("bad")]]), jobs: [] },
        "a decode error for job 2, which it didn't return",
      ],
      [{ jobs: [running(1n, { queue: "other" })] }, "job 1 of another queue"],
      [{ jobs: [running(1n, { state: "available" })] }, "isn't running"],
      [{ jobs: [running(1n, { attemptedBy: ["other"] })] }, "another client"],
      [
        {
          jobs: Array.from({ length: 6 }, (_, index) =>
            running(BigInt(index + 1))
          ),
        },
        "6 jobs for a limit of 5",
      ],
      [null, "no claim result"],
    ] as const) {
      const { client } = setup(() => ({
        claim: () => Promise.resolve(result as unknown as JobClaimResult),
      }));
      const run = await client.start();
      await expect(run.completed).rejects.toMatchObject({
        cause: expect.objectContaining({
          message: expect.stringContaining(reason) as unknown,
        }) as unknown,
      });
    }
  });

  it("rejects a claimed job this client is already working", async () => {
    const release = deferred();
    const row: JobRow = {
      ...jobRow(1n),
      attempt: 1,
      attemptedBy: ["lifetime-client"],
      state: "running",
    };
    let claims = 0;
    const { client } = setup(
      () => ({
        claim: () => {
          claims++;
          return Promise.resolve({
            jobs: claims <= 2 ? [row] : [],
          });
        },
      }),
      {
        queues: {
          default: { maxWorkers: 5, pollInterval: { milliseconds: 100 } },
        },
        workers: new Workers().add(job, async () => {
          await release.promise;
        }),
      }
    );

    const run = await client.start();
    let rejection: unknown;
    void run.completed.catch((error: unknown) => {
      rejection = error;
    });
    await waitUntil(() => claims >= 2);
    release.resolve(undefined);
    await expect(run.completed).rejects.toMatchObject({
      cause: expect.objectContaining({
        message: expect.stringContaining(
          "job 1, which this client is already working"
        ) as unknown,
      }) as unknown,
    });
    expect(rejection).toBeDefined();
  });

  it("applies configuration between claims, validating before River changes anything", async () => {
    const configurations: ProducerConfiguration[] = [];
    const claimGate = deferred();
    let claims = 0;
    const { client, driver, errors, log } = setup(
      (context, sessionLog) => {
        configurations.push(context);
        return {
          async claim(_context, next) {
            claims++;
            sessionLog.push(`claim ${claims}`);
            if (claims === 1) await claimGate.promise;
            sessionLog.push(`claimed ${claims}`);
            return database.transaction((tx) => next({ tx }));
          },
          configurationChanged(configuration) {
            sessionLog.push(`configure ${configuration.maxWorkers}`);
            expect(Object.isFrozen(configuration.queue)).toBe(true);
            expect(Object.isFrozen(configuration.queue.metadata)).toBe(true);
            if (configuration.queue.metadata.bad === true) {
              sessionLog.push("offered bad");
            }
            if (configuration.queue.metadata.stale === true) {
              sessionLog.push("offered stale");
            }
            if (configuration.maxWorkers === 13) {
              throw new ValidationError("13 workers are unlucky");
            }
            if (configuration.queue.metadata.bad === true) {
              throw new ValidationError("bad queue metadata");
            }
            configurations.push(configuration);
          },
        };
      },
      {
        pilot: {
          queueOptions: {
            keys: ["limit"],
            parse: (_queue, config) => ({ limit: config.limit ?? null }),
          },
        },
        queueControlPollInterval: { milliseconds: 1 },
        queues: {
          default: {
            limit: 1,
            maxWorkers: 5,
            pollInterval: { minutes: 1 },
          } as QueueConfig,
        },
      }
    );

    const run = await client.start();
    await waitUntil(() => log.includes("claim 1"));
    const update = run.updateQueue("default", {
      limit: 2,
      maxWorkers: 3,
    } as QueueConfig);
    await new Promise((resolve) => setImmediate(resolve));
    // The configuration waits for the claim in flight.
    expect(log).not.toContain("configure 3");
    claimGate.resolve(undefined);
    await update;
    expect(log.indexOf("claimed 1")).toBeLessThan(log.indexOf("configure 3"));

    await expect(
      run.updateQueue("default", { maxWorkers: 13 })
    ).rejects.toThrow("13 workers are unlucky");
    expect(run.diagnostics.queues.default?.maxWorkers).toBe(3);

    // A persisted metadata change reaches the session too, and one it
    // rejects is logged and ignored.
    const row = driver.queues.get("default") as QueueRow;
    driver.queues.set("default", { ...row, metadata: { region: "west" } });
    await waitUntil(() =>
      configurations.some(({ queue }) => queue.metadata.region === "west")
    );
    driver.queues.set("default", { ...row, metadata: { bad: true } });
    const rejections = () =>
      errors.filter((line) =>
        line.startsWith(
          "River producer rejected the queue's persisted configuration"
        )
      ).length;
    await waitUntil(() => rejections() === 1);
    // Later polls don't offer, or log, the same rejected value again.
    const polls = driver.queueGets;
    await waitUntil(() => driver.queueGets >= polls + 5);
    expect(rejections()).toBe(1);
    expect(log.filter((entry) => entry === "offered bad")).toHaveLength(1);
    // A row written before the one the session has is never offered.
    driver.queues.set("default", {
      ...row,
      metadata: { stale: true },
      updatedAt: row.updatedAt.subtract({ seconds: 1 }),
    });
    const stalePolls = driver.queueGets;
    await waitUntil(() => driver.queueGets >= stalePolls + 5);
    expect(log).not.toContain("offered stale");
    await run.stop();

    expect(
      configurations.map(({ maxWorkers, settings }) => [maxWorkers, settings])
    ).toEqual([
      [5, { limit: 1 }],
      [3, { limit: 2 }],
      [3, { limit: 2 }],
    ]);
    expect(configurations.at(-1)?.queue.metadata).toEqual({ region: "west" });
  });

  it("keeps reporting through a long drain, then stops reports before shutdown", async () => {
    const release = deferred();
    const reports: Temporal.Instant[] = [];
    let reportSignal: AbortSignal | undefined;
    let reportsAtShutdown: unknown;
    const { client, driver, log, timer } = setup(
      (_context, sessionLog) => ({
        keepAlive({ signal, staleBefore }) {
          reports.push(staleBefore);
          reportSignal = signal;
          sessionLog.push("report");
          return Promise.resolve();
        },
        shutdown() {
          sessionLog.push("shutdown");
          // Reports stopped before shutdown: none is scheduled, and the
          // last one's signal aborted.
          reportsAtShutdown = {
            aborted: reportSignal?.aborted,
            scheduled: timer.pending().some(({ ms }) => ms === 30_000),
          };
          return Promise.resolve();
        },
      }),
      {
        workers: new Workers().add(job, async () => {
          log.push("working");
          await release.promise;
          log.push("worked");
        }),
      }
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    // The first report follows the initial jitter (half of one second with
    // this randomness), later ones the report interval (30 s by default).
    await timer.waitFor(({ ms }) => ms === 500);
    await timer.advance(500);
    await waitUntil(() => log.includes("report") && log.includes("working"));
    const stopping = run.stop();
    await timer.waitFor(({ ms }) => ms === 30_000);
    await timer.advance(30_000);
    await waitUntil(
      () => log.filter((entry) => entry === "report").length === 2
    );
    await timer.waitFor(({ ms }) => ms === 30_000);
    await timer.advance(30_000);
    await waitUntil(
      () => log.filter((entry) => entry === "report").length === 3
    );
    expect(log).not.toContain("shutdown");
    release.resolve(undefined);
    await stopping;

    expect(log.slice(-2)).toEqual(["worked", "shutdown"]);
    expect(reportsAtShutdown).toEqual({ aborted: true, scheduled: false });
    const now = Temporal.Now.instant();
    for (const staleBefore of reports) {
      expect(now.since(staleBefore).total("minutes")).toBeGreaterThanOrEqual(5);
    }
  });

  it("reports at a fixed rate, never overlapping a slow report", async () => {
    const reports: Deferred[] = [];
    const { client, timer } = setup(() => ({
      keepAlive() {
        const report = deferred();
        reports.push(report);
        return report.promise;
      },
    }));

    const run = await client.start();
    await timer.waitFor(({ ms }) => ms === 500);
    await timer.advance(500);
    expect(reports).toHaveLength(1);
    // A report that takes 4 s delays the next by the rest of the interval.
    await timer.advance(4_000);
    reports[0]?.resolve(undefined);
    await timer.waitFor(({ kind }) => kind === "delay");
    await timer.advance(25_999);
    expect(reports).toHaveLength(1);
    await timer.advance(1);
    expect(reports).toHaveLength(2);
    // One slower than the interval runs the next as soon as it settles,
    // never alongside it.
    await timer.advance(40_000);
    expect(reports).toHaveLength(2);
    reports[1]?.resolve(undefined);
    await timer.waitFor(({ kind, ms }) => kind === "delay" && ms === 0);
    await timer.advance(0);
    expect(reports).toHaveLength(3);
    reports[2]?.resolve(undefined);
    await run.stop();
  });

  it("tries shutdown four times with growing deadlines, one after another", async () => {
    const attempts: Deferred[] = [];
    const { client, errors, timer } = setup(() => ({
      shutdown({ signal }) {
        const attempt = deferred();
        attempts.push(attempt);
        // A noncooperative shutdown settles only when the test says so.
        signal.addEventListener("abort", () => undefined);
        return attempt.promise;
      },
    }));

    const run = await client.start();
    const stopped = run.stop();
    for (const [index, deadline] of [100, 500, 2_500, 12_500].entries()) {
      await waitUntil(() => attempts.length === index + 1);
      await timer.waitFor(
        ({ kind, ms }) => kind === "timeout" && ms === deadline
      );
      await timer.advance(deadline);
      await new Promise((resolve) => setImmediate(resolve));
      // The next attempt waits for this one to settle.
      expect(attempts).toHaveLength(index + 1);
      attempts[index]?.reject(new Error(`attempt ${index + 1} failed`));
    }
    await stopped;

    expect(attempts).toHaveLength(4);
    expect(
      errors.filter((line) => line.startsWith("River producer shutdown failed"))
    ).toHaveLength(4);
  });
});

describe("pilot queue generations", () => {
  it("reserves a queue's name until its generation stopped", async () => {
    const shutdowns: Deferred[] = [];
    const { client, log } = setup((context, sessionLog) => ({
      shutdown() {
        sessionLog.push(`shutdown ${context.queue.name}`);
        const done = deferred();
        shutdowns.push(done);
        return done.promise;
      },
    }));

    const run = await client.start();
    await run.addQueue("extra", { maxWorkers: 1 });
    await expect(
      run.addQueue("extra", { maxWorkers: 1 })
    ).rejects.toBeInstanceOf(ValidationError);

    const removal = run.removeQueue("extra");
    // A second removal joins the first.
    const second = run.removeQueue("extra");
    await waitUntil(() => log.includes("shutdown extra"));
    const readd = run.addQueue("extra", { maxWorkers: 2 });
    await new Promise((resolve) => setImmediate(resolve));
    // The new generation doesn't overlap the old one's shutdown.
    expect(log.filter((entry) => entry === "start extra")).toHaveLength(1);
    shutdowns[0]?.resolve(undefined);
    await expect(removal).resolves.toBe(true);
    await expect(second).resolves.toBe(true);
    await readd;
    expect(log.filter((entry) => entry === "start extra")).toHaveLength(2);
    expect(run.diagnostics.queues.extra?.maxWorkers).toBe(2);

    const stopping = run.stop();
    await waitUntil(() => shutdowns.length === 3);
    for (const done of shutdowns) done.resolve(undefined);
    await stopping;
  });

  it("releases a queue whose producer failed to start, leaving the others running", async () => {
    let fail = true;
    const { client, log } = setup((context) => {
      if (context.queue.name === "flaky" && fail) {
        return Promise.reject(new Error("producer failed to start"));
      }
      return {};
    });

    const run = await client.start();
    await expect(run.addQueue("flaky", { maxWorkers: 1 })).rejects.toThrow(
      "producer failed to start"
    );
    expect(Object.keys(run.diagnostics.queues)).toEqual(["default"]);
    fail = false;
    await run.addQueue("flaky", { maxWorkers: 1 });
    expect(Object.keys(run.diagnostics.queues).sort()).toEqual([
      "default",
      "flaky",
    ]);
    await run.stop();

    expect(log).toEqual(["start default", "start flaky", "start flaky"]);
  });

  it("shuts started producers down when the initial start fails", async () => {
    const { client, log } = setup(
      (context, sessionLog) => {
        if (context.queue.name === "second") {
          return Promise.reject(new Error("second producer failed"));
        }
        return {
          shutdown() {
            sessionLog.push(`shutdown ${context.queue.name}`);
            return Promise.resolve();
          },
        };
      },
      {
        queues: {
          first: { maxWorkers: 1 },
          second: { maxWorkers: 1 },
        },
      }
    );

    await expect(client.start()).rejects.toThrow("second producer failed");

    expect(log).toEqual(["start first", "start second", "shutdown first"]);
  });

  it("claims no more than the queue's current capacity", async () => {
    const limits: number[] = [];
    const { client, driver } = setup(() => ({
      claim(context, next) {
        limits.push(context.limit);
        return database.transaction((tx) => next({ tx }));
      },
    }));

    const run = await client.start();
    await waitUntil(() => limits.length === 1);
    driver.available.push(jobRow(1n));
    // Updating wakes the queue, which claims with its new capacity.
    await run.updateQueue("default", { maxWorkers: 2 });
    await waitUntil(() => limits.length === 2);
    await run.stop();

    expect(limits.slice(0, 2)).toEqual([5, 2]);
  });
});

describe("pilot services and teardown", () => {
  it("starts services before queues and ends them after producers drained", async () => {
    const { client, log } = setup(
      (context, sessionLog) => ({
        shutdown() {
          sessionLog.push(`shutdown ${context.queue.name}`);
          return Promise.resolve();
        },
      }),
      {
        pilot: {
          services: () => [
            {
              name: "watcher",
              run: ({ signal }) =>
                new Promise<void>((resolve) => {
                  log.push("service started");
                  signal.addEventListener("abort", () => {
                    log.push("service stopped");
                    resolve();
                  });
                }),
            },
          ],
        },
      }
    );

    const run = await client.start();
    await run.stop();

    expect(log).toEqual([
      "service started",
      "start default",
      "shutdown default",
      "service stopped",
    ]);
  });

  it("restarts a service that fails or returns early, after backoff that a healthy run resets", async () => {
    let runs = 0;
    const longRun = deferred();
    const { client, errors, timer } = setup(() => ({}), {
      pilot: {
        services: () => [
          {
            name: "flaky",
            run: ({ signal }) => {
              runs++;
              if (runs === 1)
                return Promise.reject(new Error("service failed"));
              if (runs === 2) return Promise.resolve();
              if (runs === 3) return longRun.promise;
              return new Promise<void>((resolve) => {
                signal.addEventListener("abort", () => {
                  resolve();
                });
              });
            },
          },
        ],
      },
    });

    const run = await client.start();
    const first = await timer.waitFor(({ kind }) => kind === "delay");
    expect(runs).toBe(1);
    await timer.advance(first.ms);
    await waitUntil(() => runs === 2);
    const second = await timer.waitFor(({ kind }) => kind === "delay");
    // Backoff grows while the service keeps failing quickly.
    expect(second.ms).toBeGreaterThan(first.ms);
    await timer.advance(second.ms);
    await waitUntil(() => runs === 3);
    // A failure after a healthy minute starts backoff over.
    await timer.advance(60_000);
    longRun.reject(new Error("service failed after a while"));
    const third = await timer.waitFor(({ kind }) => kind === "delay");
    expect(third.ms).toBe(first.ms);
    await timer.advance(third.ms);
    await waitUntil(() => runs === 4);
    await run.stop();

    expect(runs).toBe(4);
    expect(
      errors.filter((line) =>
        line.startsWith("River service failed; restarting after backoff")
      )
    ).toHaveLength(3);
  });

  it("doesn't restart a service once the runtime stopped", async () => {
    let runs = 0;
    const { client, timer } = setup(() => ({}), {
      pilot: {
        services: () => [
          {
            name: "failing",
            run: () => {
              runs++;
              return Promise.reject(new Error("service failed"));
            },
          },
        ],
      },
    });

    const run = await client.start();
    await timer.waitFor(({ kind }) => kind === "delay");
    await run.stop();

    expect(runs).toBe(1);
    expect(timer.pending().filter(({ kind }) => kind === "delay")).toEqual([]);
  });

  it("shares one teardown between concurrent stops", async () => {
    const release = deferred();
    let shutdowns = 0;
    const { client } = setup(() => ({
      async shutdown() {
        shutdowns++;
        await release.promise;
      },
    }));

    const run = await client.start();
    const first = run.stop();
    const second = run.stop({ mode: "cancel" });
    let settled = false;
    void Promise.all([first, second]).then(() => {
      settled = true;
    });
    await waitUntil(() => shutdowns === 1);
    await new Promise((resolve) => setImmediate(resolve));
    expect(settled).toBe(false);
    release.resolve(undefined);
    await Promise.all([first, second]);

    expect(shutdowns).toBe(1);
    expect(run.state).toBe("stopped");
  });

  it("waits for every running attempt after a fatal failure before shutting producers down", async () => {
    const log: string[] = [];
    const finished: bigint[] = [];
    const brokenClaim = deferred();
    const slow = deferred();
    let working = 0;
    const { client, driver } = setup(
      (context) => ({
        ...(context.queue.name === "broken"
          ? {
              claim: async () => {
                await brokenClaim.promise;
                return {
                  jobs: [
                    {
                      ...jobRow(9n, job.kind, "other"),
                      attempt: 1,
                      attemptedBy: ["lifetime-client"],
                      state: "running" as const,
                    },
                  ],
                };
              },
            }
          : {}),
        jobFinished(row) {
          finished.push(row.id);
        },
        shutdown() {
          log.push(`shutdown ${context.queue.name}`);
          return Promise.resolve();
        },
      }),
      {
        queues: {
          broken: { maxWorkers: 1, pollInterval: { minutes: 1 } },
          default: { maxWorkers: 3, pollInterval: { minutes: 1 } },
        },
        workers: new Workers().add(job, async ({ job: row, signal }) => {
          working++;
          if (row.id === 1n) {
            // This attempt ends, and fails to persist, as soon as the
            // runtime fails.
            await new Promise<void>((resolve) => {
              signal.addEventListener("abort", () => {
                resolve();
              });
            });
            log.push("job 1 done");
            return;
          }
          await slow.promise;
          log.push("job 2 done");
        }),
      }
    );
    driver.available.push(jobRow(1n), jobRow(2n));

    const run = await client.start();
    await waitUntil(() => working === 2);
    let settled = false;
    void run.completed.catch(() => {
      settled = true;
    });
    brokenClaim.resolve(undefined);
    await waitUntil(() => log.includes("job 1 done"));
    for (let turn = 0; turn < 20; turn++) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    // The other attempt is still running, so nothing shut down yet.
    expect(log).not.toContain("shutdown default");
    expect(settled).toBe(false);
    slow.resolve(undefined);
    await expect(run.completed).rejects.toBeInstanceOf(Error);

    expect(log.indexOf("job 2 done")).toBeLessThan(
      log.indexOf("shutdown default")
    );
    expect(finished.toSorted()).toEqual([1n, 2n]);
  });

  it("tears down after a fatal failure before completed rejects", async () => {
    const release = deferred();
    const log: string[] = [];
    const foreign = {
      ...jobRow(9n, job.kind, "other"),
      attempt: 1,
      attemptedBy: ["lifetime-client"],
      state: "running" as const,
    };
    const { client } = setup(
      (context) => ({
        ...(context.queue.name === "broken"
          ? {
              claim: () => Promise.resolve({ jobs: [foreign] }),
            }
          : {}),
        async shutdown() {
          log.push(`shutdown ${context.queue.name}`);
          if (context.queue.name === "default") await release.promise;
        },
      }),
      {
        pilot: {
          services: () => [
            {
              name: "watcher",
              run: ({ signal }) =>
                new Promise<void>((resolve) => {
                  signal.addEventListener("abort", () => {
                    log.push("service stopped");
                    resolve();
                  });
                }),
            },
          ],
        },
        queues: {
          broken: { maxWorkers: 1, pollInterval: { minutes: 1 } },
          default: { maxWorkers: 1, pollInterval: { minutes: 1 } },
        },
      }
    );

    const run = await client.start();
    let rejected: unknown;
    void run.completed.catch((error: unknown) => {
      rejected = error;
    });
    await waitUntil(() => log.includes("shutdown default"));
    await new Promise((resolve) => setImmediate(resolve));
    expect(run.state).toBe("failed");
    expect(rejected).toBeUndefined();
    release.resolve(undefined);
    await expect(run.completed).rejects.toMatchObject({
      cause: expect.objectContaining({
        message: expect.stringContaining("job 9 of another queue") as unknown,
      }) as unknown,
    });
    await expect(run.stop()).rejects.toBe(rejected);

    expect(log.sort()).toEqual([
      "service stopped",
      "shutdown broken",
      "shutdown default",
    ]);
  });

  it("limits background completion batches but never a transactional completion", async () => {
    const gate = deferred();
    const txJob = defineJob({ kind: "tx_completion" });
    const completedInTx = deferred();
    const { client, driver } = setup(() => ({}), {
      completionBatchSize: 1,
      pilot: { completionConcurrency: 1 },
      workers: new Workers()
        .add(job, () => undefined)
        .add(txJob, async ({ completeTx }) => {
          await completeTx({ tx: "application" }).catch(() => undefined);
          completedInTx.resolve(undefined);
        }),
    });
    driver.completionGate = gate.promise;
    driver.available.push(jobRow(1n), jobRow(2n), jobRow(3n, txJob.kind));

    const run = await client.start();
    await waitUntil(() => driver.completionsInFlight === 1);
    // A second batch waits for the permit; the transactional completion
    // doesn't.
    await completedInTx.promise;
    expect(driver.maxCompletionsInFlight).toBe(1);
    gate.resolve(undefined);
    await run.stop();

    expect(driver.maxCompletionsInFlight).toBe(1);
    expect(driver.completionCalls).toContainEqual({
      tx: { tx: "application" },
    });
    expect(
      driver.completionCalls.filter(({ tx }) => tx === undefined).length
    ).toBeGreaterThanOrEqual(2);
  });

  it("holds a completion permit until a timed-out batch's query settles, and never times out a batch waiting for one", async () => {
    const gate = deferred();
    let worked = 0;
    const { client, driver, timer } = setup(() => ({}), {
      completionBatchSize: 1,
      pilot: { completionConcurrency: 1 },
      workers: new Workers().add(job, () => {
        worked++;
      }),
    });
    driver.completionGate = gate.promise;
    driver.available.push(jobRow(1n), jobRow(2n));
    const completionTimeouts = () =>
      timer
        .pending()
        .filter(({ kind, ms }) => kind === "timeout" && ms === 10_000).length;

    const run = await client.start();
    await waitUntil(() => driver.completionsInFlight === 1 && worked === 2);
    for (let turn = 0; turn < 10; turn++) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    // Only the batch in the database has a deadline; the other waits for
    // the permit without one.
    expect(completionTimeouts()).toBe(1);
    const stopping = run.stop();
    // The first batch's attempt times out while its query still runs, so
    // it keeps the permit.
    await timer.advance(10_000);
    const backoff = await timer.waitFor(({ kind }) => kind === "delay");
    await timer.advance(backoff.ms);
    await timer.advance(60_000);
    expect(driver.completionsInFlight).toBe(1);
    expect(
      driver.completionCalls.filter(({ tx }) => tx === undefined)
    ).toHaveLength(1);
    gate.resolve(undefined);
    await stopping;

    expect(driver.maxCompletionsInFlight).toBe(1);
    // A stop persisted both, though one waited far past a batch's deadline.
    expect(driver.completedIds).toEqual(expect.arrayContaining([1n, 2n]));
  });

  it("rejects pilot settings River can't use", () => {
    for (const pilot of [
      { completionConcurrency: 0 },
      { jobCleanerQueuesExcluded: "default" },
      { periodicJobs: {} },
      { services: [] },
    ]) {
      expect(() =>
        setup(() => ({}), {
          pilot: pilot as unknown as Partial<Pilot<Transaction>>,
        })
      ).toThrow("a pilot's");
    }
  });

  it("ends leadership and maintenance as a stop begins, while producers keep reporting through the drain", async () => {
    const release = deferred();
    const log: string[] = [];
    const { client, driver, timer } = setup(
      (context) => ({
        keepAlive() {
          log.push("report");
          return Promise.resolve();
        },
        shutdown() {
          log.push(`shutdown ${context.queue.name}`);
          return Promise.resolve();
        },
      }),
      {
        maintenance: true,
        pilot: {
          maintenanceServices: () => [
            {
              name: "term",
              run: ({ signal }) => {
                log.push("term started");
                return new Promise<void>((resolve) => {
                  signal.addEventListener("abort", () => {
                    log.push("term ended");
                    resolve();
                  });
                });
              },
            },
          ],
        },
        workers: new Workers().add(job, async () => {
          await release.promise;
          log.push("worked");
        }),
      }
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    await waitUntil(() => log.includes("term started"));
    await waitUntil(() => driver.available.length === 0);
    await timer.waitFor(({ ms }) => ms === 500);
    await timer.advance(500);
    const stopping = run.stop();
    // Like River for Go, leadership ends before the drain does.
    await waitUntil(() => log.includes("term ended"));
    await waitUntil(() => driver.leader === null);
    await timer.waitFor(({ ms }) => ms === 30_000);
    await timer.advance(30_000);
    release.resolve(undefined);
    await stopping;

    expect(log).toEqual([
      "term started",
      "report",
      "term ended",
      "report",
      "worked",
      "shutdown default",
    ]);
  });
});

describe("pilot peer attempts", () => {
  /** A job this client claimed as a peer, on attempt `attempt`. */
  function peerRow(id: bigint, attempt = 1, owner = "lifetime-client"): JobRow {
    return { ...jobRow(id), attempt, attemptedBy: [owner], state: "running" };
  }

  /** A claim callback resolving with `jobs` and their decode errors. */
  function claimOf(
    jobs: readonly JobRow[],
    decodeErrors?: JobClaimResult["decodeErrors"]
  ) {
    return () =>
      Promise.resolve(
        decodeErrors === undefined ? { jobs } : { decodeErrors, jobs }
      );
  }

  type Work = (
    context: WorkContext,
    attempts: PilotAttempts<Transaction>
  ) => Promise<void> | void;

  /**
   * A client whose job handler gets the pilot's peer attempts, and whose
   * pilot database logs its transactions. `commit` runs before a commit.
   */
  function setupPeers(
    work: Work,
    options: {
      readonly client?: Partial<ClientOptions<Transaction>>;
      readonly commit?: (tx: string) => Promise<void>;
      readonly pilot?: Partial<Pilot<Transaction>>;
      readonly producer?: (
        context: ProducerStartContext<Transaction>
      ) => PilotProducer<Transaction>;
      readonly worker?: WorkerOptions;
    } = {}
  ) {
    const hosts: PilotHost<Transaction>[] = [];
    const transactions: string[] = [];
    let begun = 0;
    const peerDatabase: PilotDatabase<Transaction> = {
      ...database,
      transaction: async (callback, transactionOptions = {}) => {
        const signal = transactionOptions.signal;
        signal?.throwIfAborted();
        const name = `tx${++begun}`;
        transactions.push(`begin ${name}`);
        try {
          const result = await callback({ tx: name });
          // Like River's databases, an aborted signal rolls back.
          signal?.throwIfAborted();
          await options.commit?.(name);
          transactions.push(`commit ${name}`);
          return result;
        } catch (error: unknown) {
          transactions.push(`rollback ${name}`);
          throw error;
        }
      },
    };
    const bundle = setup((context) => options.producer?.(context) ?? {}, {
      client: {
        completionFlushInterval: { milliseconds: 0 },
        ...options.client,
      },
      database: peerDatabase,
      pilot: {
        ...options.pilot,
        init(host) {
          hosts.push(host);
        },
      },
      workers: new Workers().add(
        job,
        (context) => {
          const host = hosts[0];
          if (host === undefined) throw new Error("the pilot has no host");
          return work(context, host.attempts);
        },
        options.worker
      ),
    });
    return {
      ...bundle,
      /** The pilot's peer attempts. */
      attempts: () => {
        const host = hosts[0];
        if (host === undefined) throw new Error("the pilot has no host");
        return host.attempts;
      },
      /** The completion command persisted for job `id`. */
      command: (id: bigint) =>
        bundle.driver.completionCommands.find((command) => command.id === id),
      transactions,
    };
  }

  it("claims peers in its transaction and completes them like the attempt's own outcome", async () => {
    const handled: bigint[] = [];
    const intercepted: bigint[] = [];
    let claimTx: unknown;
    const { client, command, driver, transactions } = setupPeers(
      async (context, attempts) => {
        const claimed = await attempts.claim(context, ({ tx }) => {
          claimTx = tx;
          return Promise.resolve({
            jobs: [peerRow(2n), peerRow(3n), peerRow(4n)],
          });
        });
        expect(claimed.map(({ id }) => id)).toEqual([2n, 3n, 4n]);
        context.setMetadata("shared", true);
        await attempts.complete(context, [
          {
            job: claimed[0] as JobRow,
            result: {
              metadata: { item: "complete" },
              outcome: complete({ output: { value: "done" } }),
              status: "succeeded",
            },
          },
          {
            job: claimed[1] as JobRow,
            result: { error: new Error("peer failed"), status: "failed" },
          },
          {
            job: claimed[2] as JobRow,
            result: { outcome: snooze({ seconds: 30 }), status: "succeeded" },
          },
        ]);
      },
      {
        client: {
          errorHandler: (context) => {
            handled.push(context.job.id);
            return { cancel: context.job.id === 3n };
          },
        },
        pilot: {
          intercept: {
            async complete(context, next) {
              intercepted.push(...context.commands.map(({ id }) => id));
              return next();
            },
          },
        },
      }
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    await waitUntil(() => command(1n) !== undefined);
    await run.stop();

    expect(claimTx).toEqual({ tx: "tx1" });
    expect(transactions.slice(0, 2)).toEqual(["begin tx1", "commit tx1"]);
    expect(handled).toEqual([3n]);
    expect(command(2n)).toMatchObject({
      kind: "complete",
      metadata: { item: "complete", shared: true },
      output: { value: "done" },
    });
    expect(command(3n)).toMatchObject({
      kind: "cancel",
      metadata: { shared: true },
    });
    expect(command(4n)).toMatchObject({ kind: "snooze" });
    // Every outcome goes through the pilot's completion interceptor, and
    // the peers' persist before the attempt's own.
    expect(intercepted.toSorted()).toEqual([1n, 2n, 3n, 4n]);
    expect(driver.completionCommands.at(-1)?.id).toBe(1n);
  });

  it("rolls back a peer claim of jobs the attempt can't own", async () => {
    const outcomes: string[] = [];
    const secondClaimed = deferred();
    const release = deferred();
    const { client, command, driver, transactions } = setupPeers(
      async (context, attempts) => {
        if (context.job.id === 8n) {
          // A second coordinator owns job 20 until the test ends.
          await attempts.claim(context, claimOf([peerRow(20n)]));
          secondClaimed.resolve(undefined);
          await release.promise;
          return;
        }
        await secondClaimed.promise;
        const attempt = async (name: string, jobs: readonly JobRow[]) => {
          try {
            await attempts.claim(context, claimOf(jobs));
            outcomes.push(`${name}: claimed`);
          } catch (error: unknown) {
            expect(error).toBeInstanceOf(ExtensionError);
            outcomes.push(`${name}: ${(error as Error).message}`);
          }
        };
        await attempt("twice", [peerRow(2n), peerRow(2n)]);
        await attempt("own", [peerRow(1n)]);
        await attempt("foreign", [peerRow(3n, 1, "other-client")]);
        await attempt("available", [{ ...peerRow(4n), state: "available" }]);
        await attempt("no attempt", [peerRow(5n, 0)]);
        await attempt("worked", [peerRow(8n)]);
        await attempt("owned", [peerRow(20n)]);
        await attempt("first", [peerRow(9n)]);
        await attempts.complete(context, [
          { job: peerRow(9n), result: { status: "succeeded" } },
        ]);
        await attempt("stale", [peerRow(9n)]);
        await attempt("retried", [peerRow(9n, 2)]);
        release.resolve(undefined);
      }
    );
    driver.available.push(jobRow(1n), jobRow(8n));

    const run = await client.start();
    await waitUntil(() => command(1n) !== undefined);
    await run.stop();

    expect(outcomes).toEqual([
      "twice: a peer claim returned job 2 twice",
      "own: a peer claim returned job 1, the claiming attempt's own job",
      "foreign: a peer claim returned job 3, which another client claimed",
      "available: a peer claim returned job 4, which isn't running",
      "no attempt: a peer claim returned job 5, which has no attempt",
      "worked: a peer claim returned job 8, which this client already works",
      "owned: a peer claim returned job 20, which this client already works as a peer",
      "first: claimed",
      "stale: a peer claim returned job 9 at attempt 1, which already ended here",
      "retried: claimed",
    ]);
    // Every rejected claim rolled back; the rest committed.
    expect(
      transactions.filter((line) => line.startsWith("rollback")).length
    ).toBe(8);
    expect(driver.completionCommands.map(({ id }) => id)).not.toContain(2n);
  });

  it("accepts outcomes only for the attempt's own peers, once each", async () => {
    const outcomes: string[] = [];
    const secondClaimed = deferred();
    const release = deferred();
    const { client, command, driver } = setupPeers(
      async (context, attempts) => {
        if (context.job.id === 8n) {
          await attempts.claim(context, claimOf([peerRow(20n)]));
          secondClaimed.resolve(undefined);
          await release.promise;
          return;
        }
        await secondClaimed.promise;
        const [peer] = await attempts.claim(context, claimOf([peerRow(2n)]));
        if (peer === undefined) throw new Error("no peer");
        const succeeded = { status: "succeeded" } as const;
        const attempt = async (
          name: string,
          outcome: Parameters<PilotAttempts<Transaction>["complete"]>[1]
        ) => {
          try {
            await attempts.complete(context, outcome);
            outcomes.push(`${name}: completed`);
          } catch (error: unknown) {
            outcomes.push(
              `${name}: ${(error as Error).constructor.name} ${(error as Error).message}`
            );
          }
        };
        await attempt("unclaimed", [{ job: peerRow(30n), result: succeeded }]);
        await attempt("another's", [{ job: peerRow(20n), result: succeeded }]);
        await attempt("stale", [
          { job: { ...peer, attempt: 2 }, result: succeeded },
        ]);
        await attempt("another client's", [
          {
            job: { ...peer, attemptedBy: ["other-client"] },
            result: succeeded,
          },
        ]);
        await attempt("twice", [
          { job: peer, result: succeeded },
          { job: peer, result: succeeded },
        ]);
        await attempt("invalid", [
          { job: peer, result: { status: "unknown" } as never },
        ]);
        // Two overlapping submissions: River accepts the first only.
        const first = attempts.complete(context, [
          { job: peer, result: succeeded },
        ]);
        const second = attempts.complete(context, [
          { job: peer, result: { error: new Error("late"), status: "failed" } },
        ]);
        await expect(second).rejects.toThrow("job 2 already has an outcome");
        await first;
        await attempt("again", [{ job: peer, result: succeeded }]);
        release.resolve(undefined);
      }
    );
    driver.available.push(jobRow(1n), jobRow(8n));

    const run = await client.start();
    await waitUntil(() => command(1n) !== undefined);
    await run.stop();

    expect(outcomes).toEqual([
      "unclaimed: ExtensionError job 30 isn't a peer of the attempt completing it",
      "another's: ExtensionError job 20 isn't a peer of the attempt completing it",
      "stale: ExtensionError job 2 attempt 2 isn't the peer attempt 1 this attempt owns",
      "another client's: ExtensionError job 2 attempt 1 isn't the peer attempt 1 this attempt owns",
      "twice: ExtensionError job 2 has two outcomes",
      "invalid: ValidationError unknown work attempt result status",
      "again: ExtensionError job 2 already has an outcome",
    ]);
    // The first overlapping outcome is the only one persisted.
    expect(
      driver.completionCommands.filter(({ id }) => id === 2n)
    ).toMatchObject([{ kind: "complete" }]);
  });

  it("rejects a peer claim once the attempt ended, even while its own outcome persists", async () => {
    let ended: WorkContext | undefined;
    let late: Promise<unknown> | undefined;
    let peers: PilotAttempts<Transaction> | undefined;
    const { client, command, driver, transactions } = setupPeers(
      (context, peerAttempts) => {
        ended = context;
        peers = peerAttempts;
        throw new Error("the attempt fails");
      },
      {
        client: {
          // Runs while River persists the attempt's own outcome.
          retryPolicy: (_job, now) => {
            late = peers?.claim(ended as WorkContext, claimOf([peerRow(3n)]));
            void late?.catch(() => undefined);
            return now;
          },
        },
      }
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    await waitUntil(() => command(1n) !== undefined);
    await run.stop();

    expect(late).toBeDefined();
    await expect(late).rejects.toBeInstanceOf(LifecycleError);
    // No claim began, so nothing was claimed or left owned.
    expect(transactions).toEqual([]);
    expect(command(3n)).toBeUndefined();
  });

  it("lets a producer claim a peer again as soon as its outcome persisted", async () => {
    const delivered = deferred();
    const handedOff: bigint[] = [];
    let reclaim = false;
    const { client, command, driver, errors } = setupPeers(
      async (context, attempts) => {
        if (context.job.id !== 1n) return;
        const [peer] = await attempts.claim(context, claimOf([peerRow(2n)]));
        // Due again at once, and claimed while its event is delivered.
        reclaim = true;
        await attempts.complete(context, [
          {
            job: peer as JobRow,
            result: { outcome: snooze({ seconds: 0 }), status: "succeeded" },
          },
        ]);
      },
      {
        client: {
          hooks: {
            onEvent: async (event) => {
              if (event.kind === "job_snoozed") await delivered.promise;
            },
          },
          queues: {
            default: {
              fetchCooldown: { milliseconds: 1 },
              maxWorkers: 5,
              pollInterval: { milliseconds: 5 },
            },
          },
        },
        producer: () => ({
          claim: async (_context, next) => {
            if (
              reclaim &&
              command(2n) !== undefined &&
              handedOff.length === 0
            ) {
              handedOff.push(2n);
              return { jobs: [peerRow(2n)] };
            }
            return database.transaction((tx) => next({ tx }));
          },
        }),
      }
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    await waitUntil(() => handedOff.length === 1);
    delivered.resolve(undefined);
    await waitUntil(
      () => driver.completionCommands.filter(({ id }) => id === 2n).length === 2
    );
    await run.stop();

    expect(run.state).toBe("stopped");
    expect(errors).toEqual([]);
  });

  it("stops the runtime when a producer claim hands off a job owned as a peer", async () => {
    const claimed = deferred();
    const release = deferred();
    let handOff = false;
    const { client, driver, errors, timer } = setupPeers(
      async (context, attempts) => {
        await attempts.claim(context, claimOf([peerRow(2n)]));
        handOff = true;
        claimed.resolve(undefined);
        await release.promise;
      },
      {
        client: {
          queues: {
            default: {
              fetchCooldown: { milliseconds: 1 },
              maxWorkers: 5,
              pollInterval: { milliseconds: 5 },
            },
          },
        },
        producer: () => ({
          claim: async (_context, next) => {
            if (handOff) {
              handOff = false;
              return { jobs: [peerRow(2n)] };
            }
            return database.transaction((tx) => next({ tx }));
          },
        }),
      }
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    await claimed.promise;
    // The next poll claims again, breaking the claim's contract; the
    // runtime fails once the coordinator ends.
    for (let turn = 0; turn < 100 && errors.length === 0; turn++) {
      await timer.advance(5);
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    expect(errors).toEqual([
      `River producer's claim broke its contract; the runtime stops: a producer's claim for queue "default" returned job 2, which this client is already working`,
    ]);
    release.resolve(undefined);
    await expect(run.completed).rejects.toMatchObject({
      cause: { message: expect.stringContaining("already working") },
    });
  });

  it("fails peers the attempt left without an outcome before its own", async () => {
    const handled: bigint[] = [];
    const { client, command, driver } = setupPeers(
      async (context, attempts) => {
        const [peer] = await attempts.claim(
          context,
          claimOf([peerRow(2n), peerRow(3n)])
        );
        await attempts.complete(context, [
          { job: peer as JobRow, result: { status: "succeeded" } },
        ]);
      },
      {
        client: {
          errorHandler: (context) => {
            handled.push(context.job.id);
          },
        },
      }
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    await waitUntil(() => command(1n) !== undefined);
    await run.stop();

    expect(command(2n)).toMatchObject({ kind: "complete" });
    expect(command(3n)).toMatchObject({ kind: "retry" });
    expect(command(3n)?.error?.error).toContain(
      "the attempt of job 1 ended without an outcome for this job"
    );
    expect(handled).toEqual([3n]);
    expect(driver.completionCommands.map(({ id }) => id)).toEqual([2n, 3n, 1n]);
  });

  it("interrupts peers when the runtime interrupts their attempt", async () => {
    const claimed = deferred();
    const { client, command, driver } = setupPeers(
      async (context, attempts) => {
        await attempts.claim(context, claimOf([peerRow(2n)]));
        claimed.resolve(undefined);
        await new Promise((resolve) => {
          context.signal.addEventListener("abort", resolve);
        });
        context.signal.throwIfAborted();
      }
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    await claimed.promise;
    await run.stop({ mode: "cancel" });

    expect(command(2n)).toMatchObject({ kind: "interrupt" });
    expect(command(1n)).toMatchObject({ kind: "interrupt" });
  });

  it("waits for a claim still in flight when the attempt ends", async () => {
    const started = deferred();
    const release = deferred();
    let ended: WorkContext | undefined;
    let claim: Promise<readonly JobRow[]> | undefined;
    const { attempts, client, command, driver } = setupPeers(
      (context, peerAttempts) => {
        ended = context;
        claim = peerAttempts.claim(context, async () => {
          started.resolve(undefined);
          await release.promise;
          return { jobs: [peerRow(2n)] };
        });
        // The handler returns while its claim is still running.
      }
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    await started.promise;
    for (let turn = 0; turn < 20; turn++) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    // The attempt's own outcome waits for the claim, and the attempt
    // accepts no new peer operation meanwhile.
    expect(driver.completionCommands).toEqual([]);
    await expect(
      attempts().claim(ended as WorkContext, claimOf([peerRow(3n)]))
    ).rejects.toBeInstanceOf(LifecycleError);
    release.resolve(undefined);
    await expect(claim).resolves.toMatchObject([{ id: 2n }]);
    await waitUntil(() => command(1n) !== undefined);
    await expect(
      attempts().complete(ended as WorkContext, [])
    ).rejects.toBeInstanceOf(LifecycleError);
    await run.stop();

    // The claimed peer got the outcome the attempt didn't give it.
    expect(command(2n)).toMatchObject({ kind: "retry" });
    expect(driver.completionCommands.map(({ id }) => id)).toEqual([2n, 1n]);
  });

  it("owns nothing from a claim the attempt's cancellation rolled back", async () => {
    const started = deferred();
    const release = deferred();
    let claim: Promise<unknown> | undefined;
    const { client, command, driver, transactions } = setupPeers(
      async (context, attempts) => {
        claim = attempts.claim(context, async () => {
          started.resolve(undefined);
          await release.promise;
          return { jobs: [peerRow(2n)] };
        });
        await claim.catch(() => undefined);
        context.signal.throwIfAborted();
      }
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    await started.promise;
    const stopping = run.stop({ mode: "cancel" });
    release.resolve(undefined);
    await stopping;

    await expect(claim).rejects.toBeInstanceOf(LifecycleError);
    expect(transactions).toEqual(["begin tx1", "rollback tx1"]);
    expect(command(2n)).toBeUndefined();
    expect(command(1n)).toMatchObject({ kind: "interrupt" });
  });

  it("owns the peers of a claim that committed as the attempt was cancelled", async () => {
    const committing = deferred();
    const commit = deferred();
    const { client, command, driver } = setupPeers(
      async (context, attempts) => {
        const claimed = await attempts.claim(context, claimOf([peerRow(2n)]));
        expect(claimed).toMatchObject([{ id: 2n }]);
        context.signal.throwIfAborted();
      },
      {
        commit: async () => {
          committing.resolve(undefined);
          await commit.promise;
        },
      }
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    await committing.promise;
    const stopping = run.stop({ mode: "cancel" });
    commit.resolve(undefined);
    await stopping;

    expect(command(2n)).toMatchObject({ kind: "interrupt" });
    expect(command(1n)).toMatchObject({ kind: "interrupt" });
  });

  it("fails peers it can't decode or transform instead of returning them", async () => {
    const handled: bigint[] = [];
    let returned: readonly JobRow[] = [];
    const { client, command, driver } = setupPeers(
      async (context, attempts) => {
        returned = await attempts.claim(
          context,
          claimOf(
            [
              peerRow(2n),
              { ...peerRow(3n), args: { malformed: true } },
              peerRow(4n),
            ],
            new Map([[4n, new Error("bad bytes")]])
          )
        );
        await attempts.complete(
          context,
          returned.map((job) => ({ job, result: { status: "succeeded" } }))
        );
      },
      {
        client: {
          errorHandler: (context) => {
            handled.push(context.job.id);
          },
          plugins: [
            createJobArgsTransformPlugin({
              name: "envelope",
              onRead: ({ args }) => {
                if (args.malformed === true) throw new Error("malformed");
                return { ...args, read: true };
              },
            }),
          ],
        },
      }
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    await waitUntil(() => command(1n) !== undefined);
    await run.stop();

    expect(returned).toMatchObject([{ args: { read: true }, id: 2n }]);
    expect(command(2n)).toMatchObject({ kind: "complete" });
    expect(command(3n)).toMatchObject({ kind: "retry" });
    expect(command(3n)?.error?.error).toContain("malformed");
    expect(command(4n)).toMatchObject({ kind: "retry" });
    expect(command(4n)?.error?.error).toContain(
      "job row couldn't be decoded: bad bytes"
    );
    expect(handled.toSorted()).toEqual([3n, 4n]);
  });

  it("rejects peer operations outside a running attempt of this client", async () => {
    let other: WorkContext | undefined;
    const { attempts, client, command, driver } = setupPeers((context) => {
      other = { ...context, client: {} as WorkContext["client"] };
    });
    // A context River didn't create for a running attempt.
    const idle = { client, job: jobRow(1n) } as unknown as WorkContext;
    await expect(attempts().claim(idle, claimOf([]))).rejects.toBeInstanceOf(
      LifecycleError
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    await waitUntil(() => command(1n) !== undefined);
    await expect(
      attempts().claim(other as WorkContext, claimOf([]))
    ).rejects.toBeInstanceOf(ValidationError);
    await expect(attempts().claim(idle, claimOf([]))).rejects.toBeInstanceOf(
      LifecycleError
    );
    await expect(
      attempts().complete(null as unknown as WorkContext, [])
    ).rejects.toBeInstanceOf(ValidationError);
    await run.stop();
  });

  it("retries a failed peer on its worker's retry policy, like Go", async () => {
    const retryAt = Temporal.Now.instant().add({ hours: 1 });
    const { client, command, driver } = setupPeers(
      async (context, attempts) => {
        if (context.job.id !== 1n) return;
        const [peer] = await attempts.claim(context, claimOf([peerRow(2n)]));
        await attempts.complete(context, [
          {
            job: peer as JobRow,
            result: { error: new Error("peer failed"), status: "failed" },
          },
        ]);
      },
      { worker: { retryPolicy: () => retryAt } }
    );
    driver.available.push(jobRow(1n));

    const run = await client.start();
    await waitUntil(() => command(1n) !== undefined);
    await run.stop();

    const retry = command(2n);
    expect(retry).toMatchObject({ kind: "retry" });
    expect(retry?.scheduledAt?.equals(retryAt)).toBe(true);
  });
});
