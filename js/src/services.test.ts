import { describe, expect, it } from "vitest";

import type { Client } from "./client.js";
import type {
  RuntimeDriver,
  RuntimeJobRescue,
  RuntimeLeader,
  RuntimeMaintenanceBatch,
} from "./driver.js";
import type { RiverEvent } from "./events.js";
import { ManualTimer } from "./internal/manual-timer.js";
import type { JobRow } from "./job.js";
import { defineJob } from "./job-definition.js";
import { periodicJob, PeriodicJobs } from "./periodic.js";
import { RuntimeServices } from "./services.js";

function leadershipEvents(events: readonly RiverEvent[]): string[] {
  return events
    .map(({ kind }) => kind)
    .filter((kind) => kind.startsWith("leader_"));
}

class SharedLeadership {
  leader: RuntimeLeader | null = null;
}

function serviceDriver(shared: SharedLeadership): RuntimeDriver {
  return {
    maintenanceCleanJobs: () => 0,
    maintenanceCleanQueues: () => 0,
    maintenanceGetStuck: () => [],
    // Mirrors Go River's elector: renew only the exact held term, and elect
    // only when no unexpired term exists, whoever holds it.
    maintenanceLeaderAcquire: (
      candidate: string,
      now: Temporal.Instant,
      ttlMs: number,
      held: RuntimeLeader | null
    ) => {
      const current = shared.leader;
      const live =
        current !== null &&
        Temporal.Instant.compare(current.expiresAt, now) >= 0;
      if (held !== null) {
        if (
          !live ||
          current.leaderId !== candidate ||
          !current.electedAt.equals(held.electedAt)
        ) {
          return null;
        }
        shared.leader = {
          ...current,
          expiresAt: now.add({ milliseconds: ttlMs }),
        };
        return shared.leader;
      }
      if (live) return null;
      shared.leader = {
        electedAt: now,
        expiresAt: now.add({ milliseconds: ttlMs }),
        leaderId: candidate,
      };
      return shared.leader;
    },
    maintenanceLeaderResign: (leader: RuntimeLeader) => {
      const current = shared.leader;
      if (
        current?.leaderId !== leader.leaderId ||
        !current.electedAt.equals(leader.electedAt)
      ) {
        return false;
      }
      shared.leader = null;
      return true;
    },
    maintenanceRescue: () => 0,
    maintenanceSchedule: () => 0,
  } as unknown as RuntimeDriver;
}

describe("RuntimeServices", () => {
  it("enqueues periodic jobs with a durable store, hooks, and dropped failures", async () => {
    const shared = new SharedLeadership();
    const definition = defineJob<{ n: number }>()({ kind: "periodic" });
    const now = Temporal.Instant.from("2026-08-30T12:00:00Z");
    const events: string[] = [];
    const inserted: unknown[] = [];
    const transactions: string[] = [];
    let failNextInsert = false;
    const driver: RuntimeDriver = {
      ...serviceDriver(shared),
      operationScope: async <T>(
        tx: unknown,
        callback: (tx: unknown) => Promise<T>
      ) => {
        expect(tx).toBeUndefined();
        transactions.push("begin");
        try {
          const result = await callback("tx");
          transactions.push("commit");
          return result;
        } catch (error: unknown) {
          transactions.push("rollback");
          throw error;
        }
      },
    };
    const client = {
      insertMany: (items: readonly unknown[], options?: { tx?: unknown }) => {
        if (failNextInsert) {
          failNextInsert = false;
          return Promise.reject(new Error("insert failed"));
        }
        inserted.push(...items.map((item) => [item, options?.tx]));
        return Promise.resolve([]);
      },
    } as unknown as Client;
    const upserts: unknown[] = [];
    const kept: (readonly string[])[] = [];
    const periodicJobs = new PeriodicJobs([
      periodicJob({
        args: { n: 1 },
        every: { minutes: 1 },
        id: "durable",
        job: definition,
      }),
      periodicJob({
        args: { n: 2 },
        every: { hours: 1 },
        job: definition,
        runOnStart: true,
      }),
    ]);
    const logged: string[] = [];
    const services = new RuntimeServices({
      client,
      clientId: "leader",
      driver,
      emit: () => Promise.resolve(),
      logger: {
        error: (message) => logged.push(message),
        warn: (message) => logged.push(message),
      },
      maintenance: {
        electionIntervalMs: 5,
        jobCleanerIntervalMs: 60_000,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 60_000,
      },
      now: () => now,
      onPeriodicJobsStart: ({ durableJobs }) => {
        events.push(`start:${durableJobs.map(({ id }) => id).join(",")}`);
        return Promise.resolve();
      },
      periodicJobStore: {
        getAll: () =>
          Promise.resolve([
            {
              createdAt: now,
              id: "durable",
              nextRunAt: now.subtract({ seconds: 1 }),
              updatedAt: now,
            },
          ]),
        keepAliveAndReap: (ids) => {
          kept.push(ids);
          return Promise.resolve();
        },
        upsertMany: (tx, jobs) => {
          upserts.push([tx, jobs.map(({ id, nextRunAt }) => [id, nextRunAt])]);
          return Promise.resolve();
        },
      },
      periodicJobs,
      rescue: () => Promise.resolve(null),
    });
    failNextInsert = true;
    const controller = new AbortController();
    const running = services.run(controller.signal);

    // The first batch (run-on-start plus the seeded durable next run) fails
    // and is dropped; the overdue durable occurrence is then inserted.
    await waitUntil(() => inserted.length >= 1);
    controller.abort();
    await running;

    expect(events).toEqual(["start:durable"]);
    expect(kept).toEqual([["durable"]]);
    expect(transactions.slice(0, 2)).toEqual(["begin", "rollback"]);
    expect(logged).toContain("River maintenance service failed");
    expect(inserted[0]).toEqual([
      expect.objectContaining({
        args: { n: 1 },
        options: expect.objectContaining({
          metadata: { periodic: true, "river:periodic_job_id": "durable" },
          scheduledAt: now.subtract({ seconds: 1 }),
        }),
      }),
      "tx",
    ]);
    expect(upserts.at(-1)).toEqual([
      "tx",
      [["durable", now.subtract({ seconds: 1 }).add({ minutes: 1 })]],
    ]);
  });

  it("resigns its own term once maintenance fails to start three times, like Go", async () => {
    const shared = new SharedLeadership();
    const base = serviceDriver(shared);
    const resigned: RuntimeLeader[] = [];
    const notified: string[] = [];
    const driver: RuntimeDriver = {
      ...base,
      maintenanceLeaderResign: (leader: RuntimeLeader) => {
        resigned.push(leader);
        return base.maintenanceLeaderResign?.(leader) ?? false;
      },
      runtimeRequestLeadershipResignation: () => {
        notified.push("request_resign");
      },
    };
    const terms: Temporal.Instant[] = [];
    const logged: string[] = [];
    let now = Temporal.Instant.from("2026-08-30T12:00:00Z");
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: () => Promise.resolve(),
      logger: {
        error: (message) => logged.push(message),
        warn: (message) => logged.push(message),
      },
      maintenance: {
        electionIntervalMs: 5,
        jobCleanerIntervalMs: 60_000,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 60_000,
      },
      now: () => {
        // Each election starts a distinct term.
        now = now.add({ milliseconds: 1 });
        return now;
      },
      onPeriodicJobsStart: () => {
        if (shared.leader !== null) terms.push(shared.leader.electedAt);
        return Promise.reject(new Error("start hook failed"));
      },
      periodicJobs: new PeriodicJobs(),
      random: () => 0,
      rescue: () => Promise.resolve(null),
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => resigned.length >= 1, 5_000);
    controller.abort();
    await running;

    // Three attempts of the first term, a second apart then two, before it
    // resigns that exact term without asking other clients to.
    expect(terms.slice(0, 3)).toEqual([terms[0], terms[0], terms[0]]);
    expect(resigned[0]?.electedAt).toEqual(terms[0]);
    expect(notified).toEqual([]);
    expect(logged).toContain(
      "River maintenance failed to start after all attempts; resigning leadership"
    );
  });

  it("passes a bounded timeout and leadership signal to the job cleaner", async () => {
    const shared = new SharedLeadership();
    let observed:
      | { readonly signal: AbortSignal; readonly timeoutMs: number | null }
      | undefined;
    const driver: RuntimeDriver = {
      ...serviceDriver(shared),
      maintenanceCleanJobs: (_leader, _params, timeoutMs, signal) => {
        observed = { signal, timeoutMs };
        return 0;
      },
    };
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: () => Promise.resolve(),
      maintenance: {
        electionIntervalMs: 1,
        jobCleanerIntervalMs: 1,
        jobCleanerTimeoutMs: 321,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 60_000,
      },
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      rescue: () => Promise.resolve(null),
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => observed !== undefined);
    controller.abort();
    await running;

    expect(observed?.timeoutMs).toBe(321);
    expect(observed?.signal.aborted).toBe(true);
  });

  it("bounds rescue decisions and cancels them with the leadership term", async () => {
    const shared = new SharedLeadership();
    const jobs = Array.from(
      { length: 100 },
      (_, index) => ({ id: BigInt(index + 1) }) as JobRow
    );
    let delivered = false;
    let active = 0;
    let maximum = 0;
    const driver: RuntimeDriver = {
      ...serviceDriver(shared),
      maintenanceGetStuck: () => {
        if (delivered) return [];
        delivered = true;
        return jobs;
      },
    };
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: () => Promise.resolve(),
      maintenance: {
        electionIntervalMs: 1,
        jobCleanerIntervalMs: 60_000,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 1,
        schedulerIntervalMs: 60_000,
      },
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      rescue: async (_job, _now, signal) => {
        active++;
        maximum = Math.max(maximum, active);
        try {
          await new Promise<void>((_resolve, reject) =>
            signal.addEventListener("abort", () => reject(signal.reason), {
              once: true,
            })
          );
        } finally {
          active--;
        }
        return null;
      },
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => maximum === 32);
    controller.abort(new Error("test shutdown"));
    await running;

    expect(maximum).toBe(32);
    expect(active).toBe(0);
  });

  it("logs leadership and maintenance failures and keeps running", async () => {
    const shared = new SharedLeadership();
    const baseDriver = serviceDriver(shared);
    const acquire = baseDriver.maintenanceLeaderAcquire!;
    let acquireFailures = 2;
    let scheduleFailures = 1;
    let scheduled = 0;
    const driver: RuntimeDriver = {
      ...baseDriver,
      maintenanceLeaderAcquire: (...args) => {
        if (acquireFailures > 0) {
          acquireFailures -= 1;
          throw new Error("could not obtain lock on river_leader");
        }
        return acquire(...args);
      },
      maintenanceSchedule: () => {
        if (scheduleFailures > 0) {
          scheduleFailures -= 1;
          throw new Error("canceling statement due to statement timeout");
        }
        scheduled += 1;
        return 0;
      },
    };
    const events: RiverEvent[] = [];
    const logs: [
      string,
      string,
      Readonly<Record<string, unknown>> | undefined,
    ][] = [];
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: (event) => {
        events.push(event);
        return Promise.resolve();
      },
      logger: {
        error: (message, attributes) =>
          logs.push(["error", message, attributes]),
        warn: (message, attributes) => logs.push(["warn", message, attributes]),
      },
      maintenance: {
        electionIntervalMs: 1,
        jobCleanerIntervalMs: 60_000,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 1,
      },
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      random: () => 0.5,
      rescue: () => Promise.resolve(null),
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => scheduled > 0);
    controller.abort();
    await running;

    expect(logs).toEqual([
      [
        "warn",
        "River leader election failed; retrying",
        { error: "could not obtain lock on river_leader", leader: false },
      ],
      [
        "warn",
        "River leader election failed; retrying",
        { error: "could not obtain lock on river_leader", leader: false },
      ],
      [
        "error",
        "River maintenance service failed",
        {
          error: "canceling statement due to statement timeout",
          service: "scheduler",
        },
      ],
    ]);
    expect(
      events.filter(({ kind }) => kind === "maintenance_failed")
    ).toMatchObject([{ service: "scheduler" }]);
    expect(events.some(({ kind }) => kind === "leader_acquired")).toBe(true);
  });

  it("guards each rescue with the horizon used to select it", async () => {
    const shared = new SharedLeadership();
    const now = Temporal.Instant.from("2026-09-01T12:00:00Z");
    const stuck = { id: 7n } as JobRow;
    let selectedBefore: Temporal.Instant | undefined;
    let rescuedBefore: Temporal.Instant | undefined;
    let rescued: readonly RuntimeJobRescue[] = [];
    const driver: RuntimeDriver = {
      ...serviceDriver(shared),
      maintenanceGetStuck: (_leader, attemptedBefore, afterId) => {
        selectedBefore = attemptedBefore;
        return afterId === 0n ? [stuck] : [];
      },
      maintenanceRescue: (_leader, attemptedBefore, jobs) => {
        rescuedBefore = attemptedBefore;
        rescued = jobs;
        return jobs.length;
      },
    };
    let clock = now;
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: () => Promise.resolve(),
      maintenance: {
        electionIntervalMs: 1,
        jobCleanerIntervalMs: 60_000,
        queueCleanerIntervalMs: 60_000,
        rescueAfterMs: 3_600_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 60_000,
      },
      // Time advances while the pass runs; the rescue must still use the
      // horizon computed when the pass selected its jobs.
      now: () => {
        clock = clock.add({ seconds: 1 });
        return clock;
      },
      periodicJobs: new PeriodicJobs(),
      rescue: (job, at) =>
        Promise.resolve({
          error: { at, attempt: 1, error: "stuck", trace: "" },
          finalizedAt: null,
          id: job.id,
          scheduledAt: at,
          state: "retryable",
        }),
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => rescued.length === 1);
    controller.abort();
    await running;

    expect(rescuedBefore).toBeDefined();
    expect(rescuedBefore?.equals(selectedBefore ?? now)).toBe(true);
    expect(rescued[0]?.id).toBe(7n);
  });

  it("passes Go-compatible promotion and notification horizons", async () => {
    const shared = new SharedLeadership();
    const now = Temporal.Instant.from("2026-08-30T12:00:00Z");
    let observed:
      | Parameters<NonNullable<RuntimeDriver["maintenanceSchedule"]>>[1]
      | undefined;
    const driver: RuntimeDriver = {
      ...serviceDriver(shared),
      maintenanceSchedule: (_leader, params) => {
        observed = params;
        return 0;
      },
    };
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: () => Promise.resolve(),
      maintenance: {
        electionIntervalMs: 60_000,
        jobCleanerIntervalMs: 60_000,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 5_000,
      },
      now: () => now,
      periodicJobs: new PeriodicJobs(),
      rescue: () => Promise.resolve(null),
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => observed !== undefined);
    controller.abort();
    await running;

    expect(observed?.now.toString()).toBe(now.toString());
    expect(observed?.notificationHorizon.toString()).toBe(
      "2026-08-30T12:00:00.005Z"
    );
    expect(observed?.scheduledAtHorizon.toString()).toBe(
      "2026-08-30T12:00:05Z"
    );
  });

  it("cleans notifications in batches against one horizon until a short batch", async () => {
    const shared = new SharedLeadership();
    const now = Temporal.Instant.from("2026-08-30T12:00:00Z");
    const calls: { createdBefore: string; limit: number }[] = [];
    const calledAt: number[] = [];
    const deleted = [10_000, 10_000, 3];
    const driver: RuntimeDriver = {
      ...serviceDriver(shared),
      maintenanceCleanNotifications: (_leader, createdBefore, limit) => {
        calls.push({ createdBefore: createdBefore.toString(), limit });
        calledAt.push(performance.now());
        return deleted[calls.length - 1] ?? 0;
      },
    };
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: () => Promise.resolve(),
      maintenance: {
        electionIntervalMs: 60_000,
        jobCleanerIntervalMs: 60_000,
        notificationCleanerIntervalMs: 60_000,
        notificationRetentionMs: 3_600_000,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 60_000,
      },
      now: () => now,
      periodicJobs: new PeriodicJobs(),
      random: () => 0,
      rescue: () => Promise.resolve(null),
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => calls.length === 3);
    // Two full batches followed by a short one end the pass.
    await new Promise((resolve) => setTimeout(resolve, 20));
    controller.abort();
    await running;

    expect(calls).toEqual(
      Array.from({ length: 3 }, () => ({
        createdBefore: "2026-08-30T11:00:00Z",
        limit: 10_000,
      }))
    );
    // Like Go, the pass pauses at least 50 ms between batches.
    expect(calledAt[1]! - calledAt[0]!).toBeGreaterThanOrEqual(49);
    expect(calledAt[2]! - calledAt[1]!).toBeGreaterThanOrEqual(49);
  });

  it("switches to reduced batches after three timed-out batches in a row", async () => {
    const shared = new SharedLeadership();
    const limits: number[] = [];
    let failing = true;
    const timedOutBatch = (batch: RuntimeMaintenanceBatch | undefined) => {
      expect(batch?.timeoutMs).toBe(5);
      // Like Postgres's statement_timeout, the backend stops the batch at
      // its timeout.
      return new Promise<number>((resolve, reject) => {
        if (!failing) {
          resolve(0);
          return;
        }
        batch?.signal.addEventListener(
          "abort",
          () =>
            reject(new Error("canceling statement due to statement timeout")),
          { once: true }
        );
      });
    };
    const driver: RuntimeDriver = {
      ...serviceDriver(shared),
      maintenanceCleanNotifications: (
        _leader,
        _createdBefore,
        limit,
        batch
      ) => {
        limits.push(limit);
        return timedOutBatch(batch);
      },
    };
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: () => Promise.resolve(),
      maintenance: {
        electionIntervalMs: 60_000,
        jobCleanerIntervalMs: 60_000,
        maintenanceTimeoutMs: 5,
        notificationCleanerIntervalMs: 1,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 60_000,
      },
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      random: () => 0,
      rescue: () => Promise.resolve(null),
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => limits.length === 4);
    failing = false;
    await waitUntil(() => limits.length === 6);
    controller.abort();
    await running;

    // Each timed-out pass fails; the third in a row opens the breaker, and
    // later passes keep the reduced size even when they succeed.
    expect(limits).toEqual([10_000, 10_000, 10_000, 1_000, 1_000, 1_000]);
    expect(services.diagnostics.runs.notification_cleaner).toBe(2);
  });

  it("bounds each scheduler, rescuer, and queue cleaner batch", async () => {
    const shared = new SharedLeadership();
    const observed = new Map<string, number | null | undefined>();
    const driver: RuntimeDriver = {
      ...serviceDriver(shared),
      maintenanceCleanQueues: (_leader, _before, _limit, batch) => {
        observed.set("queue_cleaner", batch?.timeoutMs);
        return 0;
      },
      maintenanceGetStuck: (_leader, _before, _afterId, _limit, batch) => {
        observed.set("rescuer", batch?.timeoutMs);
        return [];
      },
      maintenanceSchedule: (_leader, _params, batch) => {
        observed.set("scheduler", batch?.timeoutMs);
        return 0;
      },
    };
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: () => Promise.resolve(),
      maintenance: {
        electionIntervalMs: 1,
        jobCleanerIntervalMs: 60_000,
        queueCleanerIntervalMs: 1,
        rescuerIntervalMs: 1,
        schedulerIntervalMs: 1,
      },
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      rescue: () => Promise.resolve(null),
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => observed.size === 3);
    controller.abort();
    await running;

    // Go River's default maintenance timeout.
    expect(Object.fromEntries(observed)).toEqual({
      queue_cleaner: 30_000,
      rescuer: 30_000,
      scheduler: 30_000,
    });
  });

  it("stops cleaning notifications between batches when cancelled", async () => {
    const shared = new SharedLeadership();
    const controller = new AbortController();
    let calls = 0;
    const driver: RuntimeDriver = {
      ...serviceDriver(shared),
      maintenanceCleanNotifications: () => {
        calls += 1;
        controller.abort();
        return 10_000;
      },
    };
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: () => Promise.resolve(),
      maintenance: {
        electionIntervalMs: 60_000,
        jobCleanerIntervalMs: 60_000,
        notificationCleanerIntervalMs: 1,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 60_000,
      },
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      rescue: () => Promise.resolve(null),
    });

    await services.run(controller.signal);

    expect(calls).toBe(1);
  });

  it("aborts backend work immediately when its exact term is resigned", async () => {
    const shared = new SharedLeadership();
    const signals: AbortSignal[] = [];
    const events: RiverEvent[] = [];
    const driver: RuntimeDriver = {
      ...serviceDriver(shared),
      maintenanceReindex: (_leader, _indexes, _timeoutMs, signal) => {
        signals.push(signal);
        return new Promise<number>((_resolve, reject) =>
          signal.addEventListener("abort", () => reject(signal.reason), {
            once: true,
          })
        );
      },
    };
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: (event) => {
        events.push(event);
        return Promise.resolve();
      },
      maintenance: {
        electionIntervalMs: 60_000,
        jobCleanerIntervalMs: 60_000,
        queueCleanerIntervalMs: 60_000,
        reindexerSchedule: (after) => after.add({ milliseconds: 1 }),
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 60_000,
      },
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      rescue: () => Promise.resolve(null),
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => signals.length === 1);
    await expect(services.resignLeadership()).resolves.toBe(true);
    expect(signals[0]?.aborted).toBe(true);
    controller.abort();
    await running;

    expect(events.some(({ kind }) => kind === "maintenance_failed")).toBe(
      false
    );
  });

  it("runs backend-specific reindex maintenance on its configured schedule", async () => {
    const shared = new SharedLeadership();
    const calls: Array<{
      indexes: readonly string[];
      signal: AbortSignal;
      timeoutMs: number | null;
    }> = [];
    const driver: RuntimeDriver = {
      ...serviceDriver(shared),
      maintenanceReindex: (_leader, indexes, timeoutMs, signal) => {
        calls.push({ indexes, signal, timeoutMs });
        return 2;
      },
    };
    const events: RiverEvent[] = [];
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: (event) => {
        events.push(event);
        return Promise.resolve();
      },
      maintenance: {
        electionIntervalMs: 1,
        jobCleanerIntervalMs: 60_000,
        queueCleanerIntervalMs: 60_000,
        reindexerIndexNames: ["river_one", "river_two"],
        reindexerSchedule: (after) => after.add({ milliseconds: 1 }),
        reindexerTimeoutMs: 321,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 60_000,
      },
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      rescue: () => Promise.resolve(null),
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => calls.length === 1);
    controller.abort();
    await running;

    expect(calls[0]).toMatchObject({
      indexes: ["river_one", "river_two"],
      signal: controller.signal,
      timeoutMs: 321,
    });
    expect(
      events.some(
        (event) =>
          event.kind === "maintenance_succeeded" &&
          event.service === "reindexer" &&
          event.count === 2
      )
    ).toBe(true);
  });

  it("renews leadership while another maintenance service is blocked", async () => {
    const shared = new SharedLeadership();
    const baseDriver = serviceDriver(shared);
    const acquire = baseDriver.maintenanceLeaderAcquire!;
    let acquireCalls = 0;
    let releaseScheduler!: () => void;
    let schedulerStarted = false;
    const schedulerGate = new Promise<number>((resolve) => {
      releaseScheduler = () => resolve(0);
    });
    const driver: RuntimeDriver = {
      ...baseDriver,
      maintenanceLeaderAcquire: (...args) => {
        acquireCalls += 1;
        return acquire(...args);
      },
      maintenanceSchedule: () => {
        schedulerStarted = true;
        return schedulerGate;
      },
    };
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: () => Promise.resolve(),
      maintenance: {
        electionIntervalMs: 1,
        jobCleanerIntervalMs: 60_000,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 1,
      },
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      rescue: () => Promise.resolve(null),
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => schedulerStarted);
    await waitUntil(() => acquireCalls >= 3);
    controller.abort();
    releaseScheduler();
    await running;

    expect(acquireCalls).toBeGreaterThanOrEqual(3);
    expect(shared.leader).toBeNull();
  });

  it("stops during an outage without waiting on election or resignation", async () => {
    const shared = new SharedLeadership();
    const base = serviceDriver(shared);
    let outage = false;
    let resignAttempts = 0;
    const hang = new Promise<never>(() => undefined);
    // A connection that never comes; the wait ends when its signal aborts.
    const waitForConnection = (signal: AbortSignal | undefined) =>
      new Promise<never>((_resolve, reject) => {
        signal?.addEventListener("abort", () => reject(signal.reason), {
          once: true,
        });
      });
    const driver: RuntimeDriver = {
      ...base,
      maintenanceLeaderAcquire: (leaderId, now, ttlMs, held, options) =>
        outage
          ? waitForConnection(options?.signal)
          : base.maintenanceLeaderAcquire!(leaderId, now, ttlMs, held),
      maintenanceLeaderResign: (leader) => {
        if (!outage) return base.maintenanceLeaderResign!(leader);
        resignAttempts++;
        return hang;
      },
    };
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: () => Promise.resolve(),
      maintenance: {
        electionIntervalMs: 1,
        jobCleanerIntervalMs: 60_000,
        leaderResignTimeoutMs: 5,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 60_000,
      },
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      rescue: () => Promise.resolve(null),
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);
    await waitUntil(() => services.diagnostics.isLeader);

    // The database stops answering: the next election and the resignation
    // on stop wait for a connection that never comes.
    outage = true;
    await new Promise((resolve) => setTimeout(resolve, 10));
    controller.abort();
    await running;

    // Like Go, three bounded resignation attempts, then the lease expires.
    expect(resignAttempts).toBe(3);
  });

  it("bids within 50 ms of another client's resignation, like Go", async () => {
    const shared = new SharedLeadership();
    const options = {
      // Long enough that only the resignation can explain a prompt bid.
      electionIntervalMs: 60_000,
      jobCleanerIntervalMs: 60_000,
      queueCleanerIntervalMs: 60_000,
      rescuerIntervalMs: 60_000,
      schedulerIntervalMs: 60_000,
    };
    const services = (clientId: string) =>
      new RuntimeServices({
        client: {} as Client,
        clientId,
        driver: serviceDriver(shared),
        emit: () => Promise.resolve(),
        maintenance: options,
        now: () => Temporal.Now.instant(),
        periodicJobs: new PeriodicJobs(),
        random: () => 0.99,
        rescue: () => Promise.resolve(null),
      });
    const first = services("first");
    const second = services("second");
    const firstAbort = new AbortController();
    const secondAbort = new AbortController();
    const firstRun = first.run(firstAbort.signal);
    await waitUntil(() => first.diagnostics.isLeader);
    const secondRun = second.run(secondAbort.signal);
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(second.diagnostics.isLeader).toBe(false);

    firstAbort.abort();
    await firstRun;
    expect(shared.leader).toBeNull();
    second.leaderResigned();
    await waitUntil(() => second.diagnostics.isLeader, 500);

    secondAbort.abort();
    await secondRun;
  });

  it("fences leadership, resigns, and permits failover without leaked loops", async () => {
    const shared = new SharedLeadership();
    const firstEvents: RiverEvent[] = [];
    const secondEvents: RiverEvent[] = [];
    const options = {
      electionIntervalMs: 1,
      jobCleanerIntervalMs: 60_000,
      queueCleanerIntervalMs: 60_000,
      rescuerIntervalMs: 60_000,
      schedulerIntervalMs: 60_000,
    };
    const first = new RuntimeServices({
      client: {} as Client,
      clientId: "first",
      driver: serviceDriver(shared),
      emit: (event) => {
        firstEvents.push(event);
        return Promise.resolve();
      },
      maintenance: options,
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      rescue: () => Promise.resolve(null),
    });
    const second = new RuntimeServices({
      client: {} as Client,
      clientId: "second",
      driver: serviceDriver(shared),
      emit: (event) => {
        secondEvents.push(event);
        return Promise.resolve();
      },
      maintenance: options,
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      rescue: () => Promise.resolve(null),
    });
    const firstAbort = new AbortController();
    const secondAbort = new AbortController();
    const firstRun = first.run(firstAbort.signal);
    await waitUntil(() => first.diagnostics.isLeader);
    const secondRun = second.run(secondAbort.signal);
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(second.diagnostics.isLeader).toBe(false);

    await expect(second.resignLeadership()).resolves.toBe(false);
    await expect(first.resignLeadership()).resolves.toBe(true);
    firstAbort.abort();
    await firstRun;
    await waitUntil(() => second.diagnostics.isLeader);
    expect(firstEvents.some(({ kind }) => kind === "leader_lost")).toBe(true);
    expect(second.diagnostics.leader?.leaderId).toBe("second");
    secondAbort.abort();
    await secondRun;

    expect(firstEvents.some(({ kind }) => kind === "leader_acquired")).toBe(
      true
    );
    expect(firstEvents.some(({ kind }) => kind === "leader_lost")).toBe(true);
    expect(secondEvents.some(({ kind }) => kind === "leader_acquired")).toBe(
      true
    );
    expect(shared.leader).toBeNull();
  });

  it("runs term services once per term, never overlapping terms", async () => {
    const shared = new SharedLeadership();
    const runs: {
      release: () => void;
      readonly term: RuntimeLeader;
      ended: boolean;
    }[] = [];
    const base = serviceDriver(shared);
    let acquisitions = 0;
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver: {
        ...base,
        maintenanceLeaderAcquire: (...args) => {
          acquisitions++;
          return base.maintenanceLeaderAcquire?.(...args) ?? null;
        },
      },
      emit: () => Promise.resolve(),
      maintenance: {
        electionIntervalMs: 1,
        jobCleanerIntervalMs: 60_000,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 60_000,
      },
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      rescue: () => Promise.resolve(null),
      termServices: [
        {
          name: "term",
          run: ({ signal, term }) => {
            const run: (typeof runs)[number] = {
              ended: false,
              release: () => undefined,
              term,
            };
            runs.push(run);
            return new Promise<void>((resolve) => {
              signal.addEventListener("abort", () => {
                run.ended = true;
                // The service settles only when the test releases it.
                run.release = () => {
                  resolve();
                };
              });
            });
          },
        },
      ],
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => runs.length === 1);
    expect(runs[0]?.term.leaderId).toBe("leader");
    await services.resignLeadership();
    expect(runs[0]?.ended).toBe(true);
    // A new term is elected, but its services wait for the old term's.
    await waitUntil(
      () =>
        shared.leader !== null &&
        !shared.leader.electedAt.equals(
          runs[0]?.term.electedAt as Temporal.Instant
        )
    );
    // Leadership keeps renewing, and the loop keeps waiting.
    const renewals = acquisitions;
    await waitUntil(() => acquisitions >= renewals + 3);
    expect(runs).toHaveLength(1);
    runs[0]?.release();
    await waitUntil(() => runs.length === 2);
    expect(
      runs[1]?.term.electedAt.equals(
        shared.leader?.electedAt as Temporal.Instant
      )
    ).toBe(true);

    controller.abort();
    await waitUntil(() => runs[1]?.ended === true);
    runs[1]?.release();
    await running;
  });

  it("ends a term at its local deadline while renewal hangs, and never revives it", async () => {
    const shared = new SharedLeadership();
    const base = serviceDriver(shared);
    const renewal = Promise.withResolvers<RuntimeLeader | null>();
    let acquisitions = 0;
    const driver: RuntimeDriver = {
      ...base,
      maintenanceLeaderAcquire: (...args) => {
        acquisitions++;
        if (acquisitions === 1)
          return base.maintenanceLeaderAcquire?.(...args) ?? null;
        if (acquisitions === 2) return renewal.promise;
        return null;
      },
    };
    const signals: AbortSignal[] = [];
    const events: RiverEvent[] = [];
    const timer = new ManualTimer();
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver,
      emit: (event) => {
        events.push(event);
        return Promise.resolve();
      },
      maintenance: {
        electionIntervalMs: 1,
        jobCleanerIntervalMs: 60_000,
        leaderDeadlineSafetyMs: 1,
        leaderTtlPaddingMs: 30,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 60_000,
      },
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      rescue: () => Promise.resolve(null),
      termServices: [
        {
          name: "term",
          run: ({ signal }) => {
            signals.push(signal);
            return new Promise<void>((resolve) => {
              signal.addEventListener("abort", () => {
                resolve();
              });
            });
          },
        },
      ],
      timer,
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => signals.length === 1 && acquisitions === 2);
    // The renewal hangs; the term still ends at its local deadline: the
    // TTL (the election interval plus padding) less the safety margin.
    await timer.advance(29);
    expect(signals[0]?.aborted).toBe(false);
    expect(services.diagnostics.isLeader).toBe(true);
    await timer.advance(1);
    expect(signals[0]?.aborted).toBe(true);
    // Like Go's elector, the client stops leading and says so at once.
    expect(services.diagnostics).toMatchObject({
      isLeader: false,
      leader: null,
    });
    expect(leadershipEvents(events)).toEqual([
      "leader_acquired",
      "leader_lost",
    ]);
    const term = shared.leader as RuntimeLeader;
    renewal.resolve({
      ...term,
      expiresAt: Temporal.Now.instant().add({ minutes: 1 }),
    });
    // The late renewal resigns instead of reviving the term.
    await waitUntil(() => shared.leader === null);
    expect(signals).toHaveLength(1);
    expect(leadershipEvents(events)).toEqual([
      "leader_acquired",
      "leader_lost",
    ]);

    controller.abort();
    await running;
  });

  it("leaves a pilot's queues to its own job cleaner", async () => {
    const shared = new SharedLeadership();
    let excluded: readonly string[] | undefined;
    const services = new RuntimeServices({
      client: {} as Client,
      clientId: "leader",
      driver: {
        ...serviceDriver(shared),
        maintenanceCleanJobs: (_leader, params) => {
          excluded = params.queuesExcluded;
          return 0;
        },
      },
      emit: () => Promise.resolve(),
      jobCleanerQueuesExcluded: ["own_cleaner"],
      maintenance: {
        electionIntervalMs: 1,
        jobCleanerIntervalMs: 1,
        queueCleanerIntervalMs: 60_000,
        rescuerIntervalMs: 60_000,
        schedulerIntervalMs: 60_000,
      },
      now: () => Temporal.Now.instant(),
      periodicJobs: new PeriodicJobs(),
      rescue: () => Promise.resolve(null),
    });
    const controller = new AbortController();
    const running = services.run(controller.signal);

    await waitUntil(() => excluded !== undefined);
    controller.abort();
    await running;

    expect(excluded).toEqual(["own_cleaner"]);
  });
});

async function waitUntil(
  predicate: () => boolean,
  timeoutMs = 1_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out waiting for service");
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}
