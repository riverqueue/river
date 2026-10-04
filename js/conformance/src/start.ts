/**
 * Shared decoding of the contract's `start` tuning, so the SQLite and
 * PostgreSQL adapters configure River's runtime identically.
 */
import {
  periodicJob,
  type DurationInput,
  type JobDefinition,
  type ClientDriver,
  type ClientOptions,
  type JobStuckHandler,
  type MaintenanceOptions,
  type PeriodicJob,
  type RiverEventKind,
} from "riverqueue";
import {
  PilotClient,
  type JobClaimResult,
  type PilotFactory,
} from "riverqueue/unstable-driver";

import { invalidParams } from "./errors.js";
import { optionalBoolean, optionalStrings, requiredInteger } from "./params.js";

/** Event kinds every runtime adapter records for `runtime_stats`. */
export const CONFORMANCE_EVENT_KINDS = Object.freeze<RiverEventKind[]>([
  "job_cancelled",
  "job_completed",
  "job_failed",
  "job_interrupted",
  "job_snoozed",
  "queue_paused",
  "queue_resumed",
]);

/** Normalized runtime observations reported by `runtime_stats`. */
export class RuntimeProbe {
  readonly events: RiverEventKind[] = [];
  readonly trace: string[] = [];
  /** `cooperative_cancel` attempts whose worker started already cancelled. */
  cancelledAtStart = 0;
  errorHandlerCalls = 0;
  periodicStarts = 0;
  resumableFirstRuns = 0;
  resumableSecondRuns = 0;
  stuckJobs = 0;

  /** Count each attempt River reports stuck, without adding capacity. */
  readonly stuckHandler: JobStuckHandler = () => {
    this.stuckJobs++;
    return undefined;
  };

  snapshot(): Record<string, unknown> {
    return {
      cancelled_at_start: this.cancelledAtStart,
      error_handler_calls: this.errorHandlerCalls,
      events: [...this.events],
      periodic_starts: this.periodicStarts,
      resumable_first_runs: this.resumableFirstRuns,
      resumable_second_runs: this.resumableSecondRuns,
      stuck_jobs: this.stuckJobs,
      trace: [...this.trace],
    };
  }
}

/**
 * A client whose first claim that returns jobs waits for `released`; see
 * {@link claimBarrierPilot}.
 */
export class ClaimBarrierClient<Transaction> extends PilotClient<Transaction> {
  constructor(
    driver: ClientDriver<Transaction, "runtime">,
    options: ClientOptions<Transaction>,
    released: Promise<void>
  ) {
    super(driver, options, claimBarrierPilot<Transaction>(released));
  }
}

/**
 * A pilot whose producer sessions hold the first claim that returns jobs,
 * committed but not started, until `released` settles, so a cancellation
 * can arrive between a claim and the start of its jobs. Later claims don't
 * wait, and a stop ends the wait.
 */
function claimBarrierPilot<Transaction>(
  released: Promise<void>
): PilotFactory<Transaction> {
  let waited = false;
  return () => ({
    startProducer: () =>
      Promise.resolve({
        async claim(context, next): Promise<JobClaimResult> {
          const claimed = await context.database.transaction(
            (tx) => next({ tx }),
            { signal: context.signal }
          );
          if (waited || claimed.jobs.length === 0) return claimed;
          waited = true;
          // The claim committed, so its jobs are returned however the wait
          // ends.
          await Promise.race([
            released,
            new Promise<void>((resolve) => {
              context.retrySignal.addEventListener("abort", () => resolve(), {
                once: true,
              });
            }),
          ]);
          return claimed;
        },
      }),
  });
}

/**
 * The `start` periodic jobs: with `periodic_run_on_start`, one hourly job
 * (`conformance-periodic`) that runs on start, unique by args and queue
 * with `periodic_unique`, which also adds a non-unique marker job after it
 * (`conformance-periodic-marker`) whose insertion shows the unique one's
 * was attempted.
 */
export function startPeriodicJobs(
  params: Record<string, unknown>,
  definition: JobDefinition
): PeriodicJob[] {
  const unique = optionalBoolean(params, "periodic_unique", false);
  if (params.periodic_run_on_start !== true) {
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
      job: definition,
      options: {
        metadata: { periodic: true },
        ...(isUnique ? { unique: { byArgs: true, byQueue: true } } : {}),
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

/**
 * Client-wide timeouts from `start`: `job_timeout_ms` or
 * `job_timeout_disabled`, and `job_stuck_threshold_ms`. Absent keys keep
 * River's defaults.
 */
export function startTimeoutOptions(params: Record<string, unknown>): {
  jobStuckThreshold?: DurationInput;
  jobTimeout?: DurationInput | null;
} {
  const options: {
    jobStuckThreshold?: DurationInput;
    jobTimeout?: DurationInput | null;
  } = {};
  if (params.job_timeout_disabled === true) {
    options.jobTimeout = null;
  } else if (params.job_timeout_ms !== undefined) {
    options.jobTimeout = milliseconds(params, "job_timeout_ms", 1);
  }
  if (params.job_stuck_threshold_ms !== undefined) {
    options.jobStuckThreshold = milliseconds(
      params,
      "job_stuck_threshold_ms",
      1
    );
  }
  return options;
}

/**
 * Leader election and leader maintenance from `start`, spread into the
 * client options. `leader_election_disabled` maps to the client's
 * `leaderElectionDisabled`, the counterpart of Go's
 * `Config.LeaderElectionDisabled`, which never elects and rejects periodic
 * jobs. This is the only place the adapters translate it.
 */
export function startLeadershipOptions(params: Record<string, unknown>): {
  leaderElectionDisabled: boolean;
  maintenance: MaintenanceOptions;
} {
  return {
    leaderElectionDisabled: optionalBoolean(
      params,
      "leader_election_disabled",
      false
    ),
    maintenance: startMaintenanceOptions(params),
  };
}

/**
 * Leader maintenance from `start`. Absent keys keep River's defaults, like
 * the Go reference, so scenarios that rely on default intervals observe the
 * same timing. A retention of `-1` keeps that state forever.
 */
function startMaintenanceOptions(
  params: Record<string, unknown>
): MaintenanceOptions {
  const options: {
    -readonly [Key in keyof MaintenanceOptions]: MaintenanceOptions[Key];
  } = {};
  const durations = [
    ["elect_interval_ms", "electionInterval"],
    ["job_cleaner_interval_ms", "jobCleanerInterval"],
    ["queue_cleaner_interval_ms", "queueCleanerInterval"],
    ["rescue_after_ms", "rescueAfter"],
    ["rescuer_interval_ms", "rescuerInterval"],
    ["scheduler_interval_ms", "schedulerInterval"],
  ] as const;
  for (const [name, option] of durations) {
    if (params[name] !== undefined) {
      options[option] = milliseconds(params, name, 1);
    }
  }
  const retentions = [
    ["cancelled_job_retention_ms", "cancelledJobRetention"],
    ["completed_job_retention_ms", "completedJobRetention"],
    ["discarded_job_retention_ms", "discardedJobRetention"],
  ] as const;
  for (const [name, option] of retentions) {
    if (params[name] === undefined) continue;
    const retention = requiredInteger(
      params,
      name,
      -1,
      Number.MAX_SAFE_INTEGER
    );
    options[option] = retention === -1 ? null : { milliseconds: retention };
  }
  if (params.reindexer_index_names !== undefined) {
    options.reindexerIndexNames = optionalStrings(
      params,
      "reindexer_index_names"
    );
  }
  if (params.reindexer_interval_ms !== undefined) {
    const interval = milliseconds(params, "reindexer_interval_ms", 1);
    options.reindexerSchedule = (after) => after.add(interval);
  }
  return options;
}

function milliseconds(
  params: Record<string, unknown>,
  name: string,
  minimum: number
): { milliseconds: number } {
  return {
    milliseconds: requiredInteger(
      params,
      name,
      minimum,
      Number.MAX_SAFE_INTEGER
    ),
  };
}

/** The kinds `start`'s `worker_kinds` may register the built-in worker under. */
const CONFORMANCE_WORKER_KINDS = [
  "conformance_echo",
  "conformance_echo_peer",
  "conformance_echo_renamed",
] as const;

/** A kind `start` registers the built-in worker under. */
export type ConformanceWorkerKind = (typeof CONFORMANCE_WORKER_KINDS)[number];

/**
 * The kinds `start` registers the built-in worker under: `worker_kinds`, or
 * `conformance_echo` by default. `conformance_echo_renamed` keeps
 * `conformance_echo` as a kind alias, so the two can't be registered
 * together, as River's `Workers` rejects.
 */
export function startWorkerKinds(
  params: Record<string, unknown>
): readonly ConformanceWorkerKind[] {
  if (params.worker_kinds === undefined) return ["conformance_echo"];
  const kinds = optionalStrings(params, "worker_kinds");
  if (kinds.length === 0) throw invalidParams("worker_kinds must not be empty");
  if (new Set(kinds).size !== kinds.length) {
    throw invalidParams("worker_kinds must not repeat a kind");
  }
  for (const kind of kinds) {
    if (!(CONFORMANCE_WORKER_KINDS as readonly string[]).includes(kind)) {
      throw invalidParams(`unknown worker kind ${JSON.stringify(kind)}`);
    }
  }
  if (
    kinds.includes("conformance_echo") &&
    kinds.includes("conformance_echo_renamed")
  ) {
    throw invalidParams(
      "conformance_echo_renamed keeps conformance_echo as a kind alias, so they can't both be registered"
    );
  }
  return kinds as readonly ConformanceWorkerKind[];
}

/** The kind aliases of a kind `start` registers the built-in worker under. */
export function conformanceKindAliases(kind: string): readonly string[] {
  return kind === "conformance_echo_renamed" ? ["conformance_echo"] : [];
}

/** `start`'s `fetch_only_known_kinds`, the client's `fetchOnlyKnownKinds`. */
export function startFetchOnlyKnownKinds(
  params: Record<string, unknown>
): boolean {
  return optionalBoolean(params, "fetch_only_known_kinds", false);
}
