import type {
  InsertMiddleware,
  RiverErrorHandler,
  RiverHooks,
  RiverPlugin,
  WorkMiddleware,
} from "./extensions.js";
import type { RegisteredTransaction } from "./driver.js";
import type { InsertOptions } from "./insert-options.js";
import type { Logger } from "./logger.js";
import type { PeriodicJob } from "./periodic.js";
import type {
  EventLoopDelaySettings,
  JobStuckHandler,
  QueueSettings,
  RetryPolicy,
  RuntimeSettings,
  StopSettings,
} from "./runtime.js";
import type { Workers } from "./worker.js";
import type { MaintenanceSettings, ReindexerSchedule } from "./services.js";
import { ValidationError } from "./errors.js";
import {
  toMilliseconds,
  toNullableMilliseconds,
  type DurationInput,
} from "./internal/duration.js";

export type { DurationInput } from "./internal/duration.js";

/**
 * Local configuration of one queue this client works.
 *
 * Durations accept a `Temporal.Duration` or a duration-like object such as
 * `{ seconds: 5 }`.
 */
export interface QueueConfig {
  /**
   * Minimum time between claim queries. Defaults to the client's
   * `fetchCooldown`.
   */
  readonly fetchCooldown?: DurationInput;
  /** Maximum jobs from this queue worked concurrently by this client. */
  readonly maxWorkers: number;
  /**
   * How often to poll for jobs when no insert notification arrives. Defaults
   * to 1 second.
   */
  readonly pollInterval?: DurationInput;
}

/** Event-loop delay monitoring, enabled by default. */
export interface EventLoopDelayOptions {
  /** How often to report `runtime_event_loop_delay` events. */
  readonly reportInterval?: DurationInput;
  /** Sampling resolution of the delay histogram. */
  readonly resolution?: DurationInput;
  /** Delay above which River logs a warning. */
  readonly warningThreshold?: DurationInput;
}

/** Options for `RunHandle.stop`. */
export interface StopOptions {
  /**
   * `"graceful"` (the default) stops claiming and lets running jobs finish;
   * `"cancel"` also aborts every running job's `signal`.
   */
  readonly mode?: "cancel" | "graceful";
  /** Abort a graceful stop early, escalating to cancellation. */
  readonly signal?: AbortSignal;
  /**
   * Escalate a graceful stop to cancellation after this long. Without it, a
   * graceful stop waits for running jobs indefinitely.
   */
  readonly timeout?: DurationInput;
}

/**
 * Leader-owned maintenance: election, scheduling, rescue, cleaning, and
 * reindexing. Retentions accept `null` to keep rows forever and timeouts
 * accept `null` for no limit.
 */
export interface MaintenanceOptions {
  /** Keep cancelled jobs this long. Defaults to 24 hours. */
  readonly cancelledJobRetention?: DurationInput | null;
  /** Keep completed jobs this long. Defaults to 24 hours. */
  readonly completedJobRetention?: DurationInput | null;
  /** Keep discarded jobs this long. Defaults to 7 days. */
  readonly discardedJobRetention?: DurationInput | null;
  /**
   * How often a leader renews its term and a follower bids for leadership.
   * Defaults to 5 seconds. Like River for Go, a follower's interval is
   * jittered by up to a fifth, and a follower bids within 50 ms of another
   * client's resignation.
   */
  readonly electionInterval?: DurationInput;
  /** How often the job cleaner runs. Defaults to 30 seconds. */
  readonly jobCleanerInterval?: DurationInput;
  /** Bound on one job cleaner pass. Defaults to 1 minute. */
  readonly jobCleanerTimeout?: DurationInput | null;
  /** How often expired SQLite notification rows are deleted. */
  readonly notificationCleanerInterval?: DurationInput;
  /** Keep SQLite notification rows this long. */
  readonly notificationRetention?: DurationInput;
  /** How often the queue cleaner runs. */
  readonly queueCleanerInterval?: DurationInput;
  /** Delete queues nothing has reported for this long. */
  readonly queueRetention?: DurationInput;
  /** Indexes rebuilt by the Postgres reindexer. */
  readonly reindexerIndexNames?: readonly string[];
  /** When the Postgres reindexer runs next after a given instant. */
  readonly reindexerSchedule?: ReindexerSchedule;
  /** Bound on one index rebuild. */
  readonly reindexerTimeout?: DurationInput | null;
  /**
   * Rescue a running job whose attempt started longer ago than this (jobs
   * whose timeout is disabled are never rescued). Defaults to 1 hour.
   */
  readonly rescueAfter?: DurationInput;
  /** How often the rescuer runs. Defaults to 30 seconds. */
  readonly rescuerInterval?: DurationInput;
  /** How often scheduled and retryable jobs are made available. */
  readonly schedulerInterval?: DurationInput;
}

/** Options for constructing a River client. */
export interface ClientOptions<Transaction = RegisteredTransaction> {
  /**
   * Stable identifier of this client, recorded in `attempted_by` and used
   * for leadership. Defaults to a random value.
   */
  readonly clientId?: string;
  /** Maximum completions persisted in one query. Defaults to 1,000. */
  readonly completionBatchSize?: number;
  /**
   * How long a partly filled completion batch waits for more completions.
   * Zero flushes immediately.
   */
  readonly completionFlushInterval?: DurationInput;
  /** Defaults below job-definition defaults and call-site options. */
  readonly defaultInsertOptions?: InsertOptions;
  /** Invoked once for each failed attempt; may request cancellation. */
  readonly errorHandler?: RiverErrorHandler;
  /** Event-loop delay monitoring; `false` disables it. */
  readonly eventLoopDelay?: false | EventLoopDelayOptions;
  /**
   * Minimum time between claim queries for queues that don't set their own
   * `fetchCooldown`, like River for Go's `Config.FetchCooldown`. Defaults to
   * 100 milliseconds and must be at least 1 millisecond.
   *
   * It also limits insert notifications: after this client notifies
   * producers of a queue's new jobs, it sends no other notification for that
   * queue until the cooldown passes. Producers fetch at most this often
   * anyway, and poll for jobs whose notification was suppressed.
   */
  readonly fetchCooldown?: DurationInput;
  /**
   * Claim only jobs whose kinds have workers in `workers`, like River for
   * Go's `Config.FetchOnlyKnownKinds`. Defaults to false. Jobs of other kinds
   * stay available without using an attempt, so clients with different
   * workers can share a queue, such as while moving job kinds from one
   * language to another. The kinds are those registered when the client
   * starts.
   *
   * This affects only claiming. A leader's rescuer still handles stuck jobs
   * of every queue and discards those whose kinds it doesn't know, so a
   * client with some of the kinds should set `leaderElectionDisabled`, and
   * another eligible client should have workers for every kind. Without
   * this option, a job of an unknown kind is claimed and fails with an
   * unknown job kind error.
   */
  readonly fetchOnlyKnownKinds?: boolean;
  /** Client-wide hooks, run after plugin hooks. */
  readonly hooks?: RiverHooks;
  /** Client-wide insert middleware, run after plugin middleware. */
  readonly insertMiddleware?: readonly InsertMiddleware[];
  /**
   * How long River waits after an attempt's timeout, or after it asks a
   * running handler to stop, before treating the attempt as stuck, like
   * River for Go's `Config.JobStuckThreshold`. Defaults to 10 seconds and
   * must not be negative.
   *
   * A handler still running this long after its timeout is reported stuck
   * and passed to `stuckHandler`. An executor that can end a handler by
   * force, such as `@riverqueue/worker-threads`, waits this long after
   * aborting a handler's signal before terminating it.
   */
  readonly jobStuckThreshold?: DurationInput;
  /**
   * Default cooperative timeout for each job attempt. Defaults to 1 minute;
   * `null` disables it. A worker's own `timeout` takes precedence.
   */
  readonly jobTimeout?: DurationInput | null;
  /**
   * Keep this client out of leader election, like River for Go's
   * `Config.LeaderElectionDisabled`. Defaults to false. The client never runs
   * leader-owned maintenance or inserts periodic jobs, but still works jobs
   * from its configured queues, including periodic jobs other clients insert.
   *
   * At least one other started client on the same database and schema must
   * remain eligible to lead for scheduled jobs, retries, periodic jobs,
   * stuck-job rescue, and cleanup to progress. A client with leader election
   * disabled never leads, even when no other client is running.
   * `periodicJobs` must be empty, and `maintenance` settings have no effect.
   */
  readonly leaderElectionDisabled?: boolean;
  /**
   * Structured logger with pino's `(attributes, message)` argument order.
   * Defaults to `console` for warnings and errors; `false` silences River.
   */
  readonly logger?: Logger | false;
  /** Settings for leader-owned maintenance. */
  readonly maintenance?: MaintenanceOptions;
  /** Client-wide work middleware, wrapping every handler. */
  readonly middleware?: readonly WorkMiddleware[];
  /**
   * Periodic jobs the leader inserts; see `periodicJob`. Must be empty when
   * `leaderElectionDisabled` is true.
   */
  readonly periodicJobs?: readonly PeriodicJob[];
  /** Named collections of hooks and middleware. */
  readonly plugins?: readonly RiverPlugin[];
  /**
   * Disable notification streams and rely on polling alone. Running jobs
   * then learn of cancellations by polling every `queueControlPollInterval`.
   * A client of a Postgres server without `LISTEN`/`NOTIFY`, such as
   * YugabyteDB without `yb_enable_listen_notify`, polls this way on its own.
   */
  readonly pollOnly?: boolean;
  /**
   * How often persisted queue pauses and resumes are polled, and, for a
   * client without notifications, its running jobs' cancellations.
   */
  readonly queueControlPollInterval?: DurationInput;
  /** How often this client reports its configured queues. */
  readonly queueHeartbeatInterval?: DurationInput;
  /** Queues this client works, keyed by name. */
  readonly queues?: Readonly<Record<string, QueueConfig>>;
  /** Override retry scheduling; invalid times fall back to River's default. */
  readonly retryPolicy?: RetryPolicy;
  /** Policy invoked after a timed-out attempt exceeds `jobStuckThreshold`. */
  readonly stuckHandler?: JobStuckHandler;
  /** Job handlers worked by `client.start()`. */
  readonly workers?: Workers<Transaction>;
}

/** Every client option the runtime takes, so a misspelled one is rejected. */
const CLIENT_OPTIONS: Readonly<
  Record<Exclude<keyof ClientOptions, "defaultInsertOptions">, true>
> = {
  clientId: true,
  completionBatchSize: true,
  completionFlushInterval: true,
  errorHandler: true,
  eventLoopDelay: true,
  fetchCooldown: true,
  fetchOnlyKnownKinds: true,
  hooks: true,
  insertMiddleware: true,
  jobStuckThreshold: true,
  jobTimeout: true,
  leaderElectionDisabled: true,
  logger: true,
  maintenance: true,
  middleware: true,
  periodicJobs: true,
  plugins: true,
  pollOnly: true,
  queueControlPollInterval: true,
  queueHeartbeatInterval: true,
  queues: true,
  retryPolicy: true,
  stuckHandler: true,
  workers: true,
};

const EVENT_LOOP_DELAY_OPTIONS: Readonly<
  Record<keyof EventLoopDelayOptions, true>
> = { reportInterval: true, resolution: true, warningThreshold: true };

const MAINTENANCE_OPTIONS: Readonly<Record<keyof MaintenanceOptions, true>> = {
  cancelledJobRetention: true,
  completedJobRetention: true,
  discardedJobRetention: true,
  electionInterval: true,
  jobCleanerInterval: true,
  jobCleanerTimeout: true,
  notificationCleanerInterval: true,
  notificationRetention: true,
  queueCleanerInterval: true,
  queueRetention: true,
  reindexerIndexNames: true,
  reindexerSchedule: true,
  reindexerTimeout: true,
  rescueAfter: true,
  rescuerInterval: true,
  schedulerInterval: true,
};

const STOP_OPTIONS: Readonly<Record<keyof StopOptions, true>> = {
  mode: true,
  signal: true,
  timeout: true,
};

/** @internal Convert public client options to the runtime's settings. */
export function toRuntimeSettings(
  options: Omit<ClientOptions, "defaultInsertOptions">
): RuntimeSettings {
  rejectMillisecondOptions("", options);
  rejectUnknownOptions("client", options, CLIENT_OPTIONS);
  const {
    completionFlushInterval,
    eventLoopDelay,
    fetchCooldown,
    jobStuckThreshold,
    jobTimeout,
    maintenance,
    queueControlPollInterval,
    queueHeartbeatInterval,
    queues,
    ...rest
  } = options;
  return {
    ...rest,
    ...(completionFlushInterval === undefined
      ? {}
      : {
          completionFlushIntervalMs: toMilliseconds(
            "completionFlushInterval",
            completionFlushInterval,
            { allowZero: true }
          ),
        }),
    ...(eventLoopDelay === undefined
      ? {}
      : {
          eventLoopDelay:
            eventLoopDelay === false
              ? false
              : toEventLoopDelaySettings(eventLoopDelay),
        }),
    ...(fetchCooldown === undefined
      ? {}
      : { fetchCooldownMs: toMilliseconds("fetchCooldown", fetchCooldown) }),
    ...(jobStuckThreshold === undefined
      ? {}
      : {
          jobStuckThresholdMs: toMilliseconds(
            "jobStuckThreshold",
            jobStuckThreshold,
            { allowZero: true }
          ),
        }),
    ...(jobTimeout === undefined
      ? {}
      : { jobTimeoutMs: toNullableMilliseconds("jobTimeout", jobTimeout) }),
    ...(maintenance === undefined
      ? {}
      : {
          maintenance: toMaintenanceSettings(maintenance),
        }),
    ...(queueControlPollInterval === undefined
      ? {}
      : {
          queueControlPollIntervalMs: toMilliseconds(
            "queueControlPollInterval",
            queueControlPollInterval
          ),
        }),
    ...(queueHeartbeatInterval === undefined
      ? {}
      : {
          queueHeartbeatIntervalMs: toMilliseconds(
            "queueHeartbeatInterval",
            queueHeartbeatInterval
          ),
        }),
    ...(queues === undefined
      ? {}
      : {
          queues: Object.fromEntries(
            Object.entries(queues).map(([name, config]) => [
              name,
              toQueueSettings(config),
            ])
          ),
        }),
  };
}

/** @internal Convert a public queue configuration. */
export function toQueueSettings(config: QueueConfig): QueueSettings {
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (config === null || typeof config !== "object") return config;
  rejectMillisecondOptions("queue ", config);
  const { fetchCooldown, pollInterval, ...rest } = config;
  return {
    ...rest,
    ...(fetchCooldown === undefined
      ? {}
      : {
          fetchCooldownMs: toMilliseconds("queue fetchCooldown", fetchCooldown),
        }),
    ...(pollInterval === undefined
      ? {}
      : {
          pollIntervalMs: toMilliseconds("queue pollInterval", pollInterval),
        }),
  };
}

/** @internal Convert public stop options. */
export function toStopSettings(options: StopOptions): StopSettings {
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (options !== null && typeof options === "object") {
    rejectMillisecondOptions("stop ", options);
    rejectUnknownOptions("stop", options, STOP_OPTIONS);
  }
  const { timeout, ...rest } = options;
  return {
    ...rest,
    ...(timeout === undefined
      ? {}
      : { timeoutMs: toMilliseconds("stop timeout", timeout) }),
  };
}

/**
 * Reject a millisecond option name such as `pollIntervalMs`. River's options
 * take durations under the name without the suffix, so an old or guessed
 * `*Ms` name would otherwise be silently ignored or bypass validation.
 */
function rejectMillisecondOptions(scope: string, options: object): void {
  for (const key of Object.keys(options)) {
    if (/[a-z]Ms$/.test(key)) {
      throw new ValidationError(
        `${scope}${key} is not an option; use ${scope}${key.slice(0, -2)} with a Temporal duration such as { seconds: 5 }`
      );
    }
  }
}

/**
 * Reject a key `known` doesn't have, like a misspelled `rescueAfter`, which
 * would otherwise be silently ignored, as queue configuration does.
 */
function rejectUnknownOptions(
  scope: string,
  options: object,
  known: Readonly<Record<string, true>>
): void {
  for (const key of Object.keys(options)) {
    if (!Object.hasOwn(known, key)) {
      throw new ValidationError(
        `${scope} has no option ${JSON.stringify(key)}`,
        { details: { option: key } }
      );
    }
  }
}

function toEventLoopDelaySettings(
  options: EventLoopDelayOptions
): EventLoopDelaySettings {
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (options !== null && typeof options === "object") {
    rejectMillisecondOptions("eventLoopDelay.", options);
    rejectUnknownOptions("eventLoopDelay", options, EVENT_LOOP_DELAY_OPTIONS);
  }
  return {
    ...(options.reportInterval === undefined
      ? {}
      : {
          reportIntervalMs: toMilliseconds(
            "eventLoopDelay.reportInterval",
            options.reportInterval
          ),
        }),
    ...(options.resolution === undefined
      ? {}
      : {
          resolutionMs: toMilliseconds(
            "eventLoopDelay.resolution",
            options.resolution
          ),
        }),
    ...(options.warningThreshold === undefined
      ? {}
      : {
          warningThresholdMs: toMilliseconds(
            "eventLoopDelay.warningThreshold",
            options.warningThreshold
          ),
        }),
  };
}

const MAINTENANCE_DURATIONS = [
  ["cancelledJobRetention", "cancelledJobRetentionMs", true],
  ["completedJobRetention", "completedJobRetentionMs", true],
  ["discardedJobRetention", "discardedJobRetentionMs", true],
  ["electionInterval", "electionIntervalMs", false],
  ["jobCleanerInterval", "jobCleanerIntervalMs", false],
  ["jobCleanerTimeout", "jobCleanerTimeoutMs", true],
  ["notificationCleanerInterval", "notificationCleanerIntervalMs", false],
  ["notificationRetention", "notificationRetentionMs", false],
  ["queueCleanerInterval", "queueCleanerIntervalMs", false],
  ["queueRetention", "queueRetentionMs", false],
  ["reindexerTimeout", "reindexerTimeoutMs", true],
  ["rescueAfter", "rescueAfterMs", false],
  ["rescuerInterval", "rescuerIntervalMs", false],
  ["schedulerInterval", "schedulerIntervalMs", false],
] as const;

function toMaintenanceSettings(
  options: MaintenanceOptions
): MaintenanceSettings {
  // Validates untyped JavaScript input.
  const value: unknown = options;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new ValidationError("maintenance must be an object");
  }
  rejectMillisecondOptions("maintenance.", options);
  rejectUnknownOptions("maintenance", options, MAINTENANCE_OPTIONS);
  const settings: Record<string, unknown> = {};
  if (options.reindexerIndexNames !== undefined) {
    settings.reindexerIndexNames = options.reindexerIndexNames;
  }
  if (options.reindexerSchedule !== undefined) {
    settings.reindexerSchedule = options.reindexerSchedule;
  }
  for (const [name, setting, nullable] of MAINTENANCE_DURATIONS) {
    const value: DurationInput | null | undefined = options[name];
    if (value === undefined) continue;
    settings[setting] =
      value === null && nullable
        ? null
        : toMilliseconds(`maintenance.${name}`, value as DurationInput);
  }
  return settings;
}
