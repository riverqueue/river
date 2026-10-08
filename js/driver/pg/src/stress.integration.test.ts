import { setTimeout as delay } from "node:timers/promises";

import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Client, defineJob, Workers } from "riverqueue";
import type { JobState, RiverEvent, RunHandle } from "riverqueue";

import { PgDriver } from "./driver.js";

// Bounded adversarial loops over several clients sharing one database.
// Scale them for a soak run, for example:
//
//   RIVER_STRESS_ITERATIONS=200 RIVER_STRESS_SEED=7 pnpm run test:integration \
//     driver/pg/src/stress.integration.test.ts
const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  "postgres://localhost:5432/river_test?sslmode=disable";
const ITERATIONS = positiveInteger("RIVER_STRESS_ITERATIONS", 10);
const SEED = positiveInteger("RIVER_STRESS_SEED", 1);
// Generous per-test budget that grows with the iteration count.
const TEST_TIMEOUT_MS = 20_000 + ITERATIONS * 3_000;
const CLIENTS = 3;
const JOBS_PER_ITERATION = 120;
const TERMINAL_STATES: readonly JobState[] = [
  "cancelled",
  "completed",
  "discarded",
];
const filePrefix = `js_stress_${Math.random().toString(36).slice(2, 10)}`;

function positiveInteger(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined || value === "") return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

/** Small deterministic PRNG (mulberry32) so a failing seed replays. */
function random(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
  };
}

interface Fleet {
  readonly clients: Client<pg.ClientBase>[];
  readonly events: RiverEvent[];
  /** Gracefully stop one client and start a fresh one in its place. */
  readonly restart: (index: number) => Promise<void>;
  readonly stop: () => Promise<void>;
}

/** Start competing clients that each record every terminal job event. */
async function startFleet(
  queue: string,
  workers: Workers<pg.ClientBase>
): Promise<Fleet> {
  const clients: Client<pg.ClientBase>[] = [];
  const consumers: Promise<void>[] = [];
  const events: RiverEvent[] = [];
  const pools: pg.Pool[] = [];
  const runs: RunHandle[] = [];
  const subscriptions: { close(): void }[] = [];
  let generation = 0;

  const startMember = async (index: number, pool: pg.Pool) => {
    const client = new Client(new PgDriver(pool), {
      clientId: `${filePrefix}_${index}_${generation++}`,
      completionFlushInterval: { milliseconds: 1 },
      leaderElectionDisabled: true,
      queues: {
        [queue]: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 8,
          pollInterval: { milliseconds: 20 },
        },
      },
      workers,
    });
    const subscription = client.subscribe({
      capacity: 100_000,
      kinds: ["job_cancelled", "job_completed", "job_failed"],
    });
    subscriptions.push(subscription);
    consumers.push(
      (async () => {
        for await (const event of subscription) events.push(event);
      })()
    );
    clients[index] = client;
    runs[index] = await client.start();
  };

  for (let index = 0; index < CLIENTS; index++) {
    const pool = new pg.Pool({ connectionString: TEST_DATABASE_URL, max: 6 });
    pools.push(pool);
    await startMember(index, pool);
  }
  return {
    clients,
    events,
    restart: async (index) => {
      const pool = pools[index];
      if (pool === undefined) throw new Error(`no client ${index}`);
      await runs[index]?.stop({ mode: "graceful", timeout: { seconds: 5 } });
      await startMember(index, pool);
    },
    stop: async () => {
      await Promise.all(
        runs.map((run) => run.stop({ timeout: { seconds: 5 } }))
      );
      for (const subscription of subscriptions) subscription.close();
      await Promise.all(consumers);
      await Promise.all(pools.map((pool) => pool.end()));
    },
  };
}

describe("Postgres multi-client stress", () => {
  let admin: pg.Pool;

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: TEST_DATABASE_URL });
    await admin.query("SELECT 1");
  });

  afterAll(async () => {
    await admin.query("DELETE FROM river_job WHERE kind LIKE $1", [
      `${filePrefix}%`,
    ]);
    await admin.query("DELETE FROM river_queue WHERE name LIKE $1", [
      `${filePrefix}%`,
    ]);
    await admin.end();
  });

  it(
    "works every job exactly once while clients compete, insert, and restart",
    async () => {
      const queue = `${filePrefix}_complete`;
      const job = defineJob({ kind: `${filePrefix}_complete` });
      const invocations = new Map<bigint, number>();
      const workers = new Workers<pg.ClientBase>().add(job, async ({ job }) => {
        invocations.set(job.id, (invocations.get(job.id) ?? 0) + 1);
        await delay(job.id % 3n === 0n ? 1 : 0);
      });
      const fleet = await startFleet(queue, workers);
      const next = random(SEED);
      try {
        for (let iteration = 0; iteration < ITERATIONS; iteration++) {
          const context = `iteration ${iteration}, seed ${SEED}`;
          fleet.events.length = 0;
          invocations.clear();

          // Insert from every client concurrently, in uneven batches, while
          // all of them are fetching.
          const inserts: Promise<readonly bigint[]>[] = [];
          for (let remaining = JOBS_PER_ITERATION; remaining > 0;) {
            const size = Math.min(remaining, 1 + Math.floor(next() * 30));
            remaining -= size;
            const client = fleet.clients[Math.floor(next() * CLIENTS)];
            if (client === undefined) throw new Error("missing client");
            inserts.push(
              client
                .insertMany(
                  Array.from({ length: size }, () => ({
                    args: {},
                    job,
                    options: { queue },
                  }))
                )
                .then((results) => results.map((result) => result.job.id))
            );
          }
          // Meanwhile, gracefully replace one client while it is working.
          const restart = delay(Math.floor(next() * 20)).then(() =>
            fleet.restart(Math.floor(next() * CLIENTS))
          );
          const ids = (await Promise.all(inserts)).flat();
          await restart;
          expect(new Set(ids).size, context).toBe(JOBS_PER_ITERATION);

          await waitFor(
            async () =>
              (await nonTerminalCount(admin, job.kind)) === 0 &&
              fleet.events.length >= JOBS_PER_ITERATION,
            context
          );
          // Let any late duplicate event arrive before asserting.
          await delay(50);

          const rows = await jobRows(admin, job.kind);
          expect(rows.size, context).toBe(JOBS_PER_ITERATION);
          for (const id of ids) {
            const row = rows.get(id);
            expect(row, `${context}: job ${id}`).toMatchObject({
              attempt: 1,
              attemptedBy: 1,
              state: "completed",
            });
            expect(invocations.get(id), `${context}: job ${id}`).toBe(1);
          }
          const terminal = terminalEventsById(fleet.events);
          for (const id of ids) {
            expect(terminal.get(id), `${context}: job ${id} events`).toEqual([
              "job_completed",
            ]);
          }
          expect(terminal.size, context).toBe(JOBS_PER_ITERATION);

          await admin.query("DELETE FROM river_job WHERE kind = $1", [
            job.kind,
          ]);
        }
      } finally {
        await fleet.stop();
      }
    },
    TEST_TIMEOUT_MS
  );

  it(
    "settles insert, work, and cancellation races consistently",
    async () => {
      const queue = `${filePrefix}_cancel`;
      const job = defineJob({ kind: `${filePrefix}_cancel` });
      const invocations = new Map<bigint, number>();
      const workDurations = new Map<bigint, number>();
      const workers = new Workers<pg.ClientBase>().add(
        job,
        async ({ job, signal }) => {
          invocations.set(job.id, (invocations.get(job.id) ?? 0) + 1);
          await delay(workDurations.get(job.id) ?? 0, undefined, { signal });
        }
      );
      const fleet = await startFleet(queue, workers);
      const next = random(SEED + 1);
      try {
        for (let iteration = 0; iteration < ITERATIONS; iteration++) {
          const context = `iteration ${iteration}, seed ${SEED}`;
          fleet.events.length = 0;
          invocations.clear();
          workDurations.clear();

          const cancelled = new Map<bigint, JobState | null>();
          const cancellations: Promise<void>[] = [];
          const inserts: Promise<void>[] = [];
          for (let remaining = JOBS_PER_ITERATION; remaining > 0;) {
            const size = Math.min(remaining, 1 + Math.floor(next() * 20));
            remaining -= size;
            const client = fleet.clients[Math.floor(next() * CLIENTS)];
            const canceller = fleet.clients[Math.floor(next() * CLIENTS)];
            if (client === undefined || canceller === undefined) {
              throw new Error("missing client");
            }
            // Decide each job's work time and cancellation delay up front so
            // the seed alone determines the schedule.
            const plan = Array.from({ length: size }, () => ({
              cancelAfterMs: next() < 0.5 ? Math.floor(next() * 40) : null,
              workMs: Math.floor(next() * 30),
            }));
            inserts.push(
              client
                .insertMany(
                  plan.map(() => ({ args: {}, job, options: { queue } }))
                )
                .then((results) => {
                  results.forEach((result, index) => {
                    const { cancelAfterMs, workMs } = plan[index] ?? {};
                    workDurations.set(result.job.id, workMs ?? 0);
                    if (cancelAfterMs === null || cancelAfterMs === undefined) {
                      return;
                    }
                    cancellations.push(
                      delay(cancelAfterMs).then(async () => {
                        const row = await canceller.jobs.cancel(result.job.id);
                        cancelled.set(result.job.id, row?.state ?? null);
                      })
                    );
                  });
                })
            );
          }
          await Promise.all(inserts);
          await Promise.all(cancellations);

          await waitFor(
            async () => (await nonTerminalCount(admin, job.kind)) === 0,
            context
          );
          const rows = await jobRows(admin, job.kind);
          expect(rows.size, context).toBe(JOBS_PER_ITERATION);
          const worked = [...rows.keys()].filter((id) => invocations.has(id));
          await waitFor(
            () => terminalEventsById(fleet.events).size >= worked.length,
            context
          );
          // Let any late duplicate event arrive before asserting.
          await delay(50);

          const terminal = terminalEventsById(fleet.events);
          for (const [id, row] of rows) {
            const where = `${context}: job ${id}`;
            expect(["cancelled", "completed"], where).toContain(row.state);
            expect(row.finalized, where).toBe(true);
            const attempts = invocations.get(id) ?? 0;
            // A terminal outcome is never retried or worked twice.
            expect(attempts, where).toBeLessThanOrEqual(1);
            expect(row.attempt, where).toBe(attempts);
            if (!cancelled.has(id)) {
              expect(row.state, where).toBe("completed");
            }
            if (attempts === 0) {
              // Cancelled before any client claimed it.
              expect(row.state, where).toBe("cancelled");
              expect(terminal.get(id), where).toBeUndefined();
            } else {
              // Exactly one terminal observation, agreeing with the row.
              expect(terminal.get(id), where).toEqual([
                row.state === "completed" ? "job_completed" : "job_cancelled",
              ]);
            }
            // A cancel that found the job already completed leaves it so.
            if (cancelled.get(id) === "completed") {
              expect(row.state, where).toBe("completed");
            }
          }
          expect(
            fleet.events.filter((event) => event.kind === "job_failed"),
            context
          ).toEqual([]);

          await admin.query("DELETE FROM river_job WHERE kind = $1", [
            job.kind,
          ]);
        }
      } finally {
        await fleet.stop();
      }
    },
    TEST_TIMEOUT_MS
  );
});

interface JobRowSummary {
  readonly attempt: number;
  readonly attemptedBy: number;
  readonly finalized: boolean;
  readonly state: JobState;
}

async function jobRows(
  pool: pg.Pool,
  kind: string
): Promise<Map<bigint, JobRowSummary>> {
  const result = await pool.query<{
    attempt: number;
    attempted_by: number;
    finalized: boolean;
    id: string;
    state: JobState;
  }>(
    `SELECT id::text AS id, state::text AS state, attempt,
            coalesce(array_length(attempted_by, 1), 0) AS attempted_by,
            finalized_at IS NOT NULL AS finalized
       FROM river_job WHERE kind = $1`,
    [kind]
  );
  return new Map(
    result.rows.map((row) => [
      BigInt(row.id),
      {
        attempt: row.attempt,
        attemptedBy: row.attempted_by,
        finalized: row.finalized,
        state: row.state,
      },
    ])
  );
}

async function nonTerminalCount(pool: pg.Pool, kind: string): Promise<number> {
  const result = await pool.query<{ count: string }>(
    "SELECT count(*)::text AS count FROM river_job WHERE kind = $1 AND NOT (state::text = ANY($2))",
    [kind, TERMINAL_STATES]
  );
  return Number(result.rows[0]?.count ?? "0");
}

/** Terminal job event kinds observed for each job, across every client. */
function terminalEventsById(
  events: readonly RiverEvent[]
): Map<bigint, string[]> {
  const byId = new Map<bigint, string[]>();
  for (const event of events) {
    if (
      event.kind !== "job_cancelled" &&
      event.kind !== "job_completed" &&
      event.kind !== "job_failed"
    ) {
      continue;
    }
    byId.set(event.job.id, [...(byId.get(event.job.id) ?? []), event.kind]);
  }
  return byId;
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  context: string,
  timeoutMs = 15_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) {
      throw new Error(`condition was not reached (${context})`);
    }
    await delay(10);
  }
}
