import { performance } from "node:perf_hooks";
import { setTimeout as sleep } from "node:timers/promises";
import { createInterface } from "node:readline/promises";

import { PgDriver } from "@riverqueue/driver-pg";
import { createMigrator } from "@riverqueue/migrate";
import type pg from "pg";
import { Client, defineJob, Workers } from "riverqueue";
import type { EventSubscription, RunHandle } from "riverqueue";

import { BenchmarkResourceMonitor } from "./benchmark-metrics.js";
import { writeLine, type Command, type CommandContext } from "./command.js";
import {
  describePostgresTarget,
  openPostgresPool,
  parseDatabaseUrl,
  STATEMENT_TIMEOUT_OPTION,
  statementTimeoutValue,
} from "./database.js";
import {
  booleanValue,
  integerValue,
  parseDuration,
  stringValue,
  UsageError,
  type OptionValues,
} from "./options.js";

const DEFAULT_BACKLOG = 75_000;
const DEFAULT_BATCH_SIZE = 5_000;
const DEFAULT_MAX_CONNECTIONS = 50;
const DEFAULT_MAX_WORKERS = 2_000;
const ITERATION_MS = 2_000;
const PRODUCER_CHECK_MS = 250;

// The tables River's Go benchmark truncates for the current main line.
const RESET_TABLES = [
  "river_job",
  "river_leader",
  "river_queue",
  "river_notification",
] as const;

export const benchCommand: Command = {
  description: `
Measure River's throughput by inserting and working no-op jobs.

WARNING: bench deletes every row from River's job, leader, queue, and
notification tables and then runs VACUUM FULL on the job table. Use it only
on a disposable database. It needs an explicit --database-url, and --yes
unless you confirm at an interactive prompt.

By default the benchmark keeps a backlog of jobs and runs until interrupted
with Ctrl-C (press it twice to stop without waiting for running jobs).
--duration stops after a time such as 30s or 5m. --num-total-jobs inserts a
fixed number of jobs first and stops once all of them are worked.

Every two seconds it prints the jobs worked and inserted and jobs per second.
The summary adds the overall rate, the 95th percentile time from insert to
completion, and peak resource use. The workload matches River's Go and Rust
benchmarks so results are comparable.`,
  name: "bench",
  options: {
    backlog: {
      description: `Jobs to keep queued while running without --num-total-jobs (default: ${DEFAULT_BACKLOG})`,
      type: "string",
      valueName: "N",
    },
    "batch-size": {
      description: `Jobs inserted per batch (default: ${DEFAULT_BATCH_SIZE})`,
      type: "string",
      valueName: "N",
    },
    "database-url": {
      description:
        "PostgreSQL database to benchmark (required; its River tables are emptied)",
      type: "string",
      valueName: "URL",
    },
    duration: {
      description: "Stop after this long, such as 30s, 5m, or 1h30m",
      type: "string",
      valueName: "DURATION",
    },
    "max-connections": {
      description: `PostgreSQL pool size (default: ${DEFAULT_MAX_CONNECTIONS})`,
      type: "string",
      valueName: "N",
    },
    "max-workers": {
      description: `Jobs worked at once (default: ${DEFAULT_MAX_WORKERS})`,
      type: "string",
      valueName: "N",
    },
    "num-total-jobs": {
      description: "Insert N jobs up front, then stop when all are worked",
      short: "n",
      type: "string",
      valueName: "N",
    },
    schema: {
      description:
        "PostgreSQL schema containing River's tables (default: the search_path)",
      type: "string",
      valueName: "NAME",
    },
    "skip-vacuum": {
      description: "Don't run VACUUM FULL after emptying the tables",
      type: "boolean",
    },
    "statement-timeout": STATEMENT_TIMEOUT_OPTION,
    yes: {
      description: "Empty River's tables without asking for confirmation",
      short: "y",
      type: "boolean",
    },
  },
  summary: "Benchmark River's job throughput (empties River's tables)",
  run: async (values, context) => runBench(parseBenchOptions(values), context),
};

interface BenchOptions {
  readonly backlog: number;
  readonly batchSize: number;
  readonly databaseUrl: string;
  readonly durationMs: number | undefined;
  readonly maxConnections: number;
  readonly maxWorkers: number;
  readonly numTotalJobs: number | undefined;
  readonly schema: string | undefined;
  readonly skipVacuum: boolean;
  readonly statementTimeoutMs: number | undefined;
  readonly yes: boolean;
}

interface BenchmarkCounters {
  failed: number;
  inserted: number;
  lastWorkedAt: number;
  worked: number;
}

// Same kind and args as River's Go benchmark job.
const benchmarkJob = defineJob({
  decode(value) {
    const num = value.num;
    if (typeof num !== "number" || !Number.isSafeInteger(num)) {
      throw new TypeError("benchmark job num must be an integer");
    }
    return { num };
  },
  kind: "benchmark",
});

function parseBenchOptions(values: OptionValues): BenchOptions {
  const command = "bench";
  const databaseUrl = stringValue(values, "database-url");
  if (databaseUrl === undefined) {
    throw new UsageError(
      "--database-url is required because bench empties River's tables",
      command
    );
  }
  const duration = stringValue(values, "duration");
  const durationMs =
    duration === undefined
      ? undefined
      : parseDuration(command, "--duration", duration);
  const numTotalJobs = integerValue(command, values, "num-total-jobs", 1);
  if (durationMs !== undefined && numTotalJobs !== undefined) {
    throw new UsageError(
      "pass at most one of --duration and --num-total-jobs",
      command
    );
  }
  return {
    backlog: integerValue(command, values, "backlog", 1) ?? DEFAULT_BACKLOG,
    batchSize:
      integerValue(command, values, "batch-size", 1) ?? DEFAULT_BATCH_SIZE,
    databaseUrl,
    durationMs,
    maxConnections:
      integerValue(command, values, "max-connections", 1) ??
      DEFAULT_MAX_CONNECTIONS,
    maxWorkers:
      integerValue(command, values, "max-workers", 1) ?? DEFAULT_MAX_WORKERS,
    numTotalJobs,
    schema: stringValue(values, "schema"),
    skipVacuum: booleanValue(values, "skip-vacuum"),
    statementTimeoutMs: statementTimeoutValue(command, values),
    yes: booleanValue(values, "yes"),
  };
}

async function runBench(
  options: BenchOptions,
  context: CommandContext
): Promise<number> {
  const location = parseDatabaseUrl("bench", options.databaseUrl, context.env);
  if (location.backend !== "postgres") {
    throw new UsageError(
      "only PostgreSQL databases can be benchmarked",
      "bench"
    );
  }
  const interactive =
    context.stdin.isTTY === true && context.stderr.isTTY === true;
  if (!options.yes && !interactive) {
    throw new UsageError(
      "pass --yes to confirm emptying River's tables, or run bench in an " +
        "interactive terminal to be asked",
      "bench"
    );
  }
  const pool = openPostgresPool(location, {
    max: options.maxConnections,
    statementTimeoutMs: options.statementTimeoutMs,
  });
  try {
    const driver = new PgDriver(
      pool,
      options.schema === undefined ? {} : { schema: options.schema }
    );
    const validation = await createMigrator(driver).validate();
    if (!validation.ok) {
      throw new Error(
        `the database is not fully migrated (${validation.messages.join("; ")}); ` +
          `run ${context.program} migrate-up first`
      );
    }

    const tables = RESET_TABLES.map((table) =>
      qualifiedTable(options.schema, table)
    );
    const target = describePostgresTarget(options.databaseUrl);
    if (!options.yes && !(await confirmReset(context, tables, target))) {
      writeLine(context.stderr, "bench: cancelled; no tables were changed");
      return 1;
    }
    writeLine(context.stderr, `bench: emptying ${tables.join(", ")}`);
    await pool.query(`TRUNCATE TABLE ${tables.join(", ")}`);
    if (!options.skipVacuum) {
      await pool.query(
        `VACUUM FULL ${qualifiedTable(options.schema, "river_job")}`
      );
    }

    return await runWorkload(options, context, pool, driver);
  } finally {
    await pool.end();
  }
}

async function confirmReset(
  context: CommandContext,
  tables: readonly string[],
  target: string
): Promise<boolean> {
  context.stderr.write(
    `bench will delete every row from ${tables.join(", ")} in ${target}.\n` +
      "Continue? [y/N] "
  );
  const prompt = createInterface({ input: context.stdin });
  try {
    const answer = await prompt.question("");
    return /^y(es)?$/i.test(answer.trim());
  } finally {
    prompt.close();
  }
}

async function runWorkload(
  options: BenchOptions,
  context: CommandContext,
  pool: pg.Pool,
  driver: PgDriver
): Promise<number> {
  const allWorked = new AbortController();
  const forceShutdown = new AbortController();
  const shutdown = new AbortController();
  let countEvents: Promise<void> | undefined;
  let producer: Promise<void> | undefined;
  let producerFailure: { readonly error: unknown } | undefined;
  let run: RunHandle | undefined;
  let resourceSampling: NodeJS.Timeout | undefined;
  let subscription: EventSubscription | undefined;
  let signals = 0;
  // The first signal stops gracefully so completions are recorded and the
  // summary still prints; a second one cancels running jobs.
  const onSignal = () => {
    signals++;
    if (signals === 1) shutdown.abort(new Error("benchmark interrupted"));
    else forceShutdown.abort(new Error("benchmark force-stopped"));
  };
  process.on("SIGINT", onSignal);
  process.on("SIGTERM", onSignal);

  try {
    const workers = new Workers().add(benchmarkJob, () => undefined);
    const client = new Client(driver, {
      clientId: `riverqueue-js-benchmark-${process.pid}`,
      // Go's benchmark fetch settings, which also limit insert
      // notifications.
      fetchCooldown: { milliseconds: 2 },
      queues: {
        default: {
          maxWorkers: options.maxWorkers,
          pollInterval: { milliseconds: 20 },
        },
      },
      workers,
    });
    subscription = client.subscribe({
      capacity: Math.max(options.backlog, options.batchSize),
      kinds: [
        "job_cancelled",
        "job_completed",
        "job_failed",
        "subscription_lag",
      ],
    });
    const counters: BenchmarkCounters = {
      failed: 0,
      inserted: 0,
      lastWorkedAt: 0,
      worked: 0,
    };
    const events = subscription;
    countEvents = (async () => {
      for await (const event of events) {
        if (event.kind === "job_completed") {
          counters.worked++;
          counters.lastWorkedAt = performance.now();
          if (counters.worked === options.numTotalJobs) allWorked.abort();
          continue;
        }
        counters.failed++;
        shutdown.abort(
          "error" in event && event.error instanceof Error
            ? event.error
            : new Error(`unexpected benchmark event ${event.kind}`)
        );
      }
    })();

    let nextNum = 0;
    const insert = async (count: number) => {
      let remaining = count;
      while (remaining > 0 && !shutdown.signal.aborted) {
        const size = Math.min(remaining, options.batchSize);
        const items = Array.from({ length: size }, () => ({
          args: { num: ++nextNum },
          job: benchmarkJob,
        }));
        const results = await client.insertMany(items);
        counters.inserted += results.length;
        remaining -= results.length;
      }
    };

    await insert(options.numTotalJobs ?? options.backlog);
    run = await client.start();
    const running = run;
    const resources = new BenchmarkResourceMonitor({
      maxConnections: options.maxConnections,
      maxPendingCompletions: running.diagnostics.completionCapacity,
      maxWorkers: options.maxWorkers,
    });
    const sampleResources = () => {
      resources.sample(running.diagnostics, pool);
    };
    sampleResources();
    resourceSampling = setInterval(sampleResources, 10);
    resourceSampling.unref();
    const startedAt = performance.now();
    producer = (
      options.numTotalJobs === undefined
        ? produceContinuously(
            insert,
            counters,
            options.backlog,
            shutdown.signal
          )
        : Promise.resolve()
    ).catch((error: unknown) => {
      producerFailure = { error };
      shutdown.abort(error);
    });

    let previousAt = startedAt;
    let previousInserted = 0;
    let previousWorked = 0;
    // Report every two seconds, ending exactly at --duration, or as soon as
    // every --num-total-jobs job has been worked.
    for (let iteration = 1; ; iteration++) {
      const reportAt = Math.min(
        iteration * ITERATION_MS,
        options.durationMs ?? Number.POSITIVE_INFINITY
      );
      await interruptibleSleep(
        Math.max(startedAt + reportAt - performance.now(), 0),
        AbortSignal.any([allWorked.signal, shutdown.signal])
      );
      if (shutdown.signal.aborted) break;
      const now = performance.now();
      const inserted = counters.inserted - previousInserted;
      const worked = counters.worked - previousWorked;
      writeLine(
        context.stdout,
        `bench: jobs worked [ ${formatInteger(worked)} ], inserted [ ${formatInteger(inserted)} ], ` +
          `job/sec [ ${formatRate((worked * 1_000) / Math.max(now - previousAt, 1))} ] ` +
          `[${formatSeconds(now - startedAt)}]`
      );
      previousAt = now;
      previousInserted = counters.inserted;
      previousWorked = counters.worked;
      if (allWorked.signal.aborted || reportAt === options.durationMs) break;
    }

    shutdown.abort(new Error("benchmark complete"));
    await producer;
    if (producerFailure !== undefined) throw producerFailure.error;
    // A graceful stop waits for running jobs and flushes their completions,
    // so the statistics below include every worked job.
    await run.stop({
      mode: forceShutdown.signal.aborted ? "cancel" : "graceful",
      signal: forceShutdown.signal,
    });
    sampleResources();
    clearInterval(resourceSampling);
    resourceSampling = undefined;
    resources.assertBounded();
    subscription.close();
    await countEvents;

    const statistics = await benchmarkStatistics(
      pool,
      qualifiedTable(options.schema, "river_job")
    );
    if (counters.failed > 0 || statistics.failed > 0) {
      throw new Error(
        `${Math.max(counters.failed, statistics.failed)} benchmark jobs failed`
      );
    }
    const finishedAt =
      counters.lastWorkedAt === 0 ? performance.now() : counters.lastWorkedAt;
    const elapsedMs = Math.max(finishedAt - startedAt, 0);
    writeLine(
      context.stdout,
      `bench: total jobs worked [ ${formatInteger(statistics.worked)} ], ` +
        `total jobs inserted [ ${formatInteger(counters.inserted)} ], ` +
        `overall job/sec [ ${formatRate(
          elapsedMs === 0 ? 0 : (statistics.worked * 1_000) / elapsedMs
        )} ], p95 [ ${formatP95(statistics.p95Seconds)} ], ` +
        `running ${formatSeconds(elapsedMs)}`
    );
    const peaks = resources.peaks;
    writeLine(
      context.stdout,
      `bench: peak running jobs ${peaks.activeAttempts}, ` +
        `completion backlog ${peaks.pendingCompletions}, ` +
        `completion queries ${peaks.completionQueries}, ` +
        `pool active/total/waiting ${peaks.poolActiveConnections}/` +
        `${peaks.poolTotalConnections}/${peaks.poolWaitingRequests}, ` +
        `event-loop delay p99/max ${formatMilliseconds(peaks.eventLoopDelayP99Ms)}/` +
        `${formatMilliseconds(peaks.eventLoopDelayMaxMs)}, ` +
        `heap/RSS ${formatBytes(peaks.heapUsedBytes)}/${formatBytes(peaks.rssBytes)}`
    );
    return 0;
  } finally {
    if (resourceSampling !== undefined) clearInterval(resourceSampling);
    shutdown.abort(new Error("benchmark cleanup"));
    await producer?.catch(() => undefined);
    if (run !== undefined && run.state !== "stopped") {
      await run
        .stop({ mode: "cancel", timeout: { milliseconds: 5_000 } })
        .catch(() => undefined);
    }
    subscription?.close();
    await countEvents?.catch(() => undefined);
    process.off("SIGINT", onSignal);
    process.off("SIGTERM", onSignal);
  }
}

// Top the backlog back up whenever workers have drained part of it, the way
// River's Go benchmark does.
async function produceContinuously(
  insert: (count: number) => Promise<void>,
  counters: BenchmarkCounters,
  backlog: number,
  signal: AbortSignal
): Promise<void> {
  while (!signal.aborted) {
    const jobsLeft = Math.max(counters.inserted - counters.worked, 0);
    if (jobsLeft < backlog) await insert(backlog - jobsLeft);
    await interruptibleSleep(PRODUCER_CHECK_MS, signal);
  }
}

/** Wait without keeping the process alive, ending early once `signal` aborts. */
function interruptibleSleep(
  milliseconds: number,
  signal: AbortSignal
): Promise<void> {
  return sleep(milliseconds, undefined, { ref: false, signal }).catch(
    () => undefined
  );
}

// p95 measures insert-to-completion time, the same way as River's Rust
// benchmark. It includes time spent waiting in the backlog.
async function benchmarkStatistics(
  pool: pg.Pool,
  table: string
): Promise<{ failed: number; p95Seconds: number | null; worked: number }> {
  const result = await pool.query<{
    failed: string;
    p95_seconds: number | null;
    worked: string;
  }>(`
    SELECT
      count(*) FILTER (WHERE state IN ('cancelled', 'discarded'))::bigint AS failed,
      percentile_cont(0.95) WITHIN GROUP (
        ORDER BY extract(epoch FROM (finalized_at - created_at))::double precision
      ) FILTER (WHERE state = 'completed') AS p95_seconds,
      count(*) FILTER (WHERE state = 'completed')::bigint AS worked
    FROM ${table}
  `);
  const row = result.rows[0];
  if (row === undefined) {
    throw new Error("benchmark statistics returned no row");
  }
  return {
    failed: exactCount(row.failed, "failed"),
    p95Seconds: row.p95_seconds,
    worked: exactCount(row.worked, "worked"),
  };
}

function exactCount(value: string, name: string): number {
  const count = Number(value);
  if (!Number.isSafeInteger(count) || count < 0) {
    throw new Error(`benchmark ${name} count exceeds JavaScript's safe range`);
  }
  return count;
}

function formatBytes(value: number): string {
  return `${(value / (1024 * 1024)).toFixed(1)}MiB`;
}

function formatInteger(value: number): string {
  return Math.trunc(value).toString(10).padStart(10);
}

function formatMilliseconds(value: number): string {
  return `${value.toFixed(1)}ms`;
}

function formatP95(value: number | null): string {
  return value === null
    ? "n/a".padStart(10)
    : `${value.toFixed(3)}s`.padStart(10);
}

function formatRate(value: number): string {
  return value.toFixed(1).padStart(10);
}

function formatSeconds(milliseconds: number): string {
  return `${(milliseconds / 1_000).toFixed(1)}s`;
}

function qualifiedTable(schema: string | undefined, table: string): string {
  const quoted = `"${table}"`;
  return schema === undefined
    ? quoted
    : `"${schema.replaceAll('"', '""')}".${quoted}`;
}
