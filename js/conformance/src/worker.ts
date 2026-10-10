// The worker client `start` runs: its configuration, the built-in worker's
// behaviors, the barriers jobs and claims wait on, and its stats.

import {
  cancel,
  Client,
  defineJob,
  periodicJob,
  snooze,
  Workers,
  type ClientDriver,
  type ClientOptions,
  type DurationInput,
  type JobDefinition,
  type JsonObject,
  type RiverEventKind,
  type RunHandle,
  type WorkContext,
  type WorkOutcome,
} from "riverqueue";
import { abortableDelay, PilotClient } from "riverqueue/unstable-driver";

import { invalidParams, type Params } from "./protocol.js";

/** The fields of `start`'s params, and of its `tuning`. */
export const START_FIELDS = [
  "claim_barrier",
  "client_id",
  "error_handler_cancel",
  "fetch_only_known_kinds",
  "fetch_poll_interval_ms",
  "job_timeout_ms",
  "leader_election_disabled",
  "max_workers",
  "periodic_run_on_start",
  "periodic_unique",
  "poll_only",
  "queues",
  "rescue_after_ms",
  "retry_delay_ms",
  "schema",
  "tuning",
  "worker_kinds",
];
const TUNING_FIELDS = [
  "elect_interval_ms",
  "rescuer_interval_ms",
  "scheduler_interval_ms",
];

/** The events `stats` reports, as River emits them. */
const EVENT_KINDS: RiverEventKind[] = [
  "job_cancelled",
  "job_completed",
  "job_failed",
  "job_snoozed",
  "queue_paused",
  "queue_resumed",
];

const KIND_ECHO = "conformance_echo";

/** The args of every job the adapter inserts. */
export interface EchoArgs extends JsonObject {
  behavior: string;
  duration_ms: number;
  message: string;
}

export const echo = defineJob<EchoArgs>()({ kind: KIND_ECHO });

/** The kinds the built-in worker can be registered under. */
const WORKER_KINDS: Readonly<Record<string, JobDefinition>> = {
  [KIND_ECHO]: echo,
  conformance_echo_peer: defineJob<EchoArgs>()({
    kind: "conformance_echo_peer",
  }),
  conformance_echo_renamed: defineJob<EchoArgs>()({
    kind: "conformance_echo_renamed",
    kindAliases: [KIND_ECHO],
  }),
};

/**
 * Named barriers that jobs and claims wait on. A barrier exists from its
 * first use, whether a wait or a release.
 */
export class Barriers {
  readonly #barriers = new Map<string, PromiseWithResolvers<undefined>>();

  release(name: string): void {
    this.#get(name).resolve(undefined);
  }

  /** Wait for the barrier's release, or reject when signal aborts. */
  async wait(name: string, signal: AbortSignal): Promise<void> {
    signal.throwIfAborted();
    const aborted = Promise.withResolvers<never>();
    const onAbort = () => aborted.reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    try {
      await Promise.race([this.#get(name).promise, aborted.promise]);
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
  }

  #get(name: string): PromiseWithResolvers<undefined> {
    let barrier = this.#barriers.get(name);
    if (barrier === undefined) {
      barrier = Promise.withResolvers<undefined>();
      this.#barriers.set(name, barrier);
    }
    return barrier;
  }
}

/** What a running client observed, as `stats` reports it. */
interface Stats {
  cancelled_at_start: number;
  error_handler_calls: number;
  events: string[];
  periodic_starts: number;
}

/** The client `start` started. */
export interface RunningClient {
  stats(): Stats;
  /** Stop gracefully, or with River's stop and cancel. */
  stop(cancelJobs: boolean): Promise<void>;
}

/** Start a worker client configured by `start`'s params. */
export async function startClient<Tx>(
  driver: ClientDriver<Tx, "runtime">,
  params: Params,
  barriers: Barriers
): Promise<RunningClient> {
  const stats: Stats = {
    cancelled_at_start: 0,
    error_handler_calls: 0,
    events: [],
    periodic_starts: 0,
  };
  const options = clientOptions<Tx>(params, barriers, stats);
  const claimBarrier = params.string("claim_barrier");
  const client =
    claimBarrier === ""
      ? new Client(driver, options)
      : new ClaimBarrierClient(driver, options, barriers, claimBarrier);

  const unsubscribe = new AbortController();
  const subscription = client.subscribe({
    kinds: EVENT_KINDS,
    signal: unsubscribe.signal,
  });
  const events = (async () => {
    for await (const event of subscription) stats.events.push(event.kind);
  })();
  const closeEvents = async () => {
    unsubscribe.abort();
    await events;
  };

  let handle: RunHandle;
  try {
    handle = await client.start();
  } catch (error: unknown) {
    await closeEvents();
    throw error;
  }
  return {
    stats: () => ({ ...stats, events: [...stats.events] }),
    async stop(cancelJobs) {
      // A claim held on its barrier would keep the client from stopping.
      if (claimBarrier !== "") barriers.release(claimBarrier);
      try {
        await handle.stop({
          mode: cancelJobs ? "cancel" : "graceful",
          timeout: { seconds: 10 },
        });
      } finally {
        await closeEvents();
      }
    },
  };
}

function clientOptions<Tx>(
  params: Params,
  barriers: Barriers,
  stats: Stats
): ClientOptions<Tx> {
  const workers = new Workers<Tx>();
  for (const kind of params.strings("worker_kinds") ?? [KIND_ECHO]) {
    const definition = WORKER_KINDS[kind];
    if (definition === undefined) {
      throw invalidParams(`unknown worker kind ${JSON.stringify(kind)}`);
    }
    workers.add(definition, (context) => work(context, barriers, stats));
  }

  const maxWorkers = params.integer("max_workers") || 4;
  const pollInterval = milliseconds(params.integer("fetch_poll_interval_ms"));
  const queues = Object.fromEntries(
    (params.strings("queues") ?? ["default"]).map((name) => [
      name,
      { maxWorkers, ...(pollInterval && { pollInterval }) },
    ])
  );

  const tuning = params.object("tuning", TUNING_FIELDS);
  const maintenance = {
    ...optional("electionInterval", tuning?.integer("elect_interval_ms")),
    ...optional("rescueAfter", params.integer("rescue_after_ms")),
    ...optional("rescuerInterval", tuning?.integer("rescuer_interval_ms")),
    ...optional("schedulerInterval", tuning?.integer("scheduler_interval_ms")),
  };

  const retryDelay = params.integer("retry_delay_ms");
  const options: ClientOptions<Tx> = {
    clientId: params.string("client_id"),
    fetchCooldown: { milliseconds: 1 },
    fetchOnlyKnownKinds: params.boolean("fetch_only_known_kinds"),
    hooks: {
      onPeriodicJobsStart: () => {
        stats.periodic_starts++;
      },
    },
    leaderElectionDisabled: params.boolean("leader_election_disabled"),
    maintenance,
    periodicJobs: periodicJobs(params),
    pollOnly: params.boolean("poll_only"),
    queues,
    workers,
    ...optional("jobTimeout", params.integer("job_timeout_ms")),
  };
  return {
    ...options,
    ...(params.boolean("error_handler_cancel") && {
      errorHandler: () => {
        stats.error_handler_calls++;
        return { cancel: true };
      },
    }),
    ...(retryDelay > 0 && {
      retryPolicy: (_job, now) => now.add({ milliseconds: retryDelay }),
    }),
  };
}

/**
 * With `periodic_run_on_start`, an hourly periodic job run on start, unique
 * by args and queue with `periodic_unique`, which also adds a non-unique
 * marker job after it whose insertion shows the unique one's was attempted.
 */
function periodicJobs(params: Params) {
  const unique = params.boolean("periodic_unique");
  if (!params.boolean("periodic_run_on_start")) {
    if (unique) {
      throw invalidParams("periodic_unique requires periodic_run_on_start");
    }
    return [];
  }
  const job = (id: string, message: string, isUnique: boolean) =>
    periodicJob({
      args: { behavior: "", duration_ms: 0, message },
      every: { hours: 1 },
      id,
      job: echo,
      options: {
        metadata: { periodic: true },
        ...(isUnique && { unique: { byArgs: true, byQueue: true } }),
      },
      runOnStart: true,
    });
  return unique
    ? [
        job("conformance-periodic", "periodic run on start", true),
        job("conformance-periodic-marker", "periodic marker", false),
      ]
    : [job("conformance-periodic", "periodic run on start", false)];
}

/** Work a job by its `behavior`, as the contract defines each. */
async function work<Tx>(
  context: WorkContext<JobDefinition, Tx>,
  barriers: Barriers,
  stats: Stats
): Promise<WorkOutcome | undefined> {
  const { job, signal } = context;
  const { behavior, duration_ms: durationMs, message } = job.args as EchoArgs;
  switch (behavior) {
    case "":
      return;
    case "barrier_output":
    case "barrier_wait":
      await barriers.wait(message, signal);
      if (behavior === "barrier_output")
        context.recordOutput({ race: "worker" });
      return;
    case "cancel":
      return cancel({ reason: "cancelled by conformance worker" });
    case "cooperative_cancel":
      if (signal.aborted) stats.cancelled_at_start++;
      await new Promise((_resolve, reject) => {
        signal.addEventListener("abort", () => reject(signal.reason), {
          once: true,
        });
        if (signal.aborted) reject(signal.reason);
      });
      return;
    case "error":
      throw new Error("conformance retryable error");
    case "output":
      context.recordOutput({ message });
      return;
    case "resumable_cursor":
      await resumableCursor(context);
      return;
    case "sleep":
      await abortableDelay(durationMs, signal);
      return;
    case "snooze_once":
      if (Object.hasOwn(job.metadata, "snoozes")) return;
      return snooze({ milliseconds: Math.max(durationMs, 1) });
  }
  throw new Error(`unknown behavior ${JSON.stringify(behavior)}`);
}

/**
 * Three resumable steps. "first" records the attempt. "second" is a cursor
 * step that sets cursor 7 and fails on attempt 1, and requires and records
 * the cursor later. "third" fails on attempt 2. A failed step fails the
 * attempt and skips the rest, so the steps' own failures are ignored here.
 */
async function resumableCursor<Tx>(
  context: WorkContext<JobDefinition, Tx>
): Promise<void> {
  const { job, resumable } = context;
  const ignore = () => undefined;
  await resumable
    .step("first", () => context.setMetadata("first_attempt", job.attempt))
    .catch(ignore);
  await resumable
    .stepWithCursor("second", (cursor) => {
      if (job.attempt === 1) {
        resumable.setCursor(7);
        throw new Error("retry with cursor");
      }
      if (cursor !== 7) {
        throw new Error(`expected cursor 7, got ${JSON.stringify(cursor)}`);
      }
      context.setMetadata("cursor_observed", cursor);
    })
    .catch(ignore);
  await resumable
    .step("third", () => {
      if (job.attempt === 2) throw new Error("retry after consuming cursor");
    })
    .catch(ignore);
}

/**
 * A client whose first claim that returns jobs holds them, committed and
 * running but not yet worked, until a barrier is released, so a
 * cancellation can arrive between a claim and its work. Stopping the client
 * releases the barrier.
 */
class ClaimBarrierClient<Tx> extends PilotClient<Tx> {
  constructor(
    driver: ClientDriver<Tx, "runtime">,
    options: ClientOptions<Tx>,
    barriers: Barriers,
    name: string
  ) {
    let waited = false;
    super(driver, options, () => ({
      startProducer: async () => ({
        async claim(claimContext, next) {
          const claimed = await claimContext.database.transaction(
            (tx) => next({ tx }),
            { signal: claimContext.signal }
          );
          if (waited || claimed.jobs.length === 0) return claimed;
          waited = true;
          // The claim committed, so its jobs are returned however the wait
          // ends.
          await barriers
            .wait(name, claimContext.retrySignal)
            .catch(() => undefined);
          return claimed;
        },
      }),
    }));
  }
}

function milliseconds(value: number): DurationInput | undefined {
  return value === 0 ? undefined : { milliseconds: value };
}

/** `{ [key]: duration }` for a nonzero millisecond value, else nothing. */
function optional<Key extends string>(
  key: Key,
  value: number | undefined
): Partial<Record<Key, DurationInput>> {
  const duration = milliseconds(value ?? 0);
  return duration === undefined
    ? {}
    : ({ [key]: duration } as Record<Key, DurationInput>);
}
