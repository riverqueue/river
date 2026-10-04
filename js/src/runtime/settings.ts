/**
 * Runtime configuration: settings types, validation, and normalization.
 */
import type { RuntimeDriver } from "../driver.js";
import { ConfigurationError, ValidationError } from "../errors.js";
import type { RiverEvent } from "../events.js";
import type {
  InsertMiddleware,
  RiverErrorHandler,
  RiverHooks,
  RiverPlugin,
  WorkMiddleware,
} from "../extensions.js";
import { validateQueueName } from "../identifiers.js";
import type { RuntimeTimer } from "../internal/backoff.js";
import type {
  EventLoopDelayMonitorOptions,
  EventLoopDelayObservation,
} from "../internal/event-loop-delay-monitor.js";
import type { JobArgsTransformer } from "../job-args-transform.js";
import {
  cloneJobArgsTransformPlugin,
  isJobArgsTransformPlugin,
} from "../job-args-transform.js";
import { getJobArgsTransformers } from "../job-args-transform.js";
import {
  cloneJobInsertMetadataTransformPlugin,
  isJobInsertMetadataTransformPlugin,
} from "../job-insert-metadata-transform.js";
import type { JobRow } from "../job.js";
import type { JsonObject } from "../json.js";
import type { Logger } from "../logger.js";
import type { InternalLogger } from "../logger.js";
import { isLoggerOption } from "../logger.js";
import { internalLogger, resolveLogger } from "../logger.js";
import type { PeriodicJob } from "../periodic.js";
import type {
  MaintenanceDiagnostics,
  MaintenanceSettings,
} from "../services.js";
import { supportsMaintenance } from "../services.js";
import type { Workers } from "../worker.js";
import {
  rejectExplicitUndefined,
  requireNonNegativeInteger,
  requirePositiveInteger,
} from "./validation.js";

/** River for Go's `FetchCooldownDefault`. */
export const DEFAULT_FETCH_COOLDOWN_MS = 100;

/** One queue's capacity and polling configuration, in milliseconds. */
export interface QueueSettings {
  /** Minimum interval between claim queries. Defaults to the client's. */
  readonly fetchCooldownMs?: number;
  readonly maxWorkers: number;
  /** Polling fallback when no notification arrives. Defaults to 1 second. */
  readonly pollIntervalMs?: number;
}

/** Information supplied when a timed-out attempt remains unsettled. */
export interface JobStuckHandlerParams {
  readonly id: bigint;
  readonly kind: string;
  readonly queue: string;
  readonly totalStuckJobs: number;
}

/** Capacity policy returned after observing a stuck attempt. */
export interface JobStuckHandlerResult {
  readonly addWorkerSlot?: boolean;
}

/**
 * Called when an attempt keeps running past its timeout. Return
 * `{ addWorkerSlot: true }` to let the queue start another job meanwhile.
 */
export type JobStuckHandler = (
  params: JobStuckHandlerParams
) =>
  | JobStuckHandlerResult
  | PromiseLike<JobStuckHandlerResult | undefined>
  | undefined;

/** Runtime configuration after conversion from the public client options. */
export interface RuntimeSettings {
  readonly clientId?: string;
  readonly completionBatchSize?: number;
  readonly completionFlushIntervalMs?: number;
  /** Event-loop delay monitoring, enabled by default. */
  readonly eventLoopDelay?: false | EventLoopDelaySettings;
  readonly errorHandler?: RiverErrorHandler;
  /**
   * Default claim cooldown for queues, and how long a queue's insert
   * notifications are suppressed after one. Defaults to 100 milliseconds.
   */
  readonly fetchCooldownMs?: number;
  /** Claim only the kinds `workers` has when the runtime starts. */
  readonly fetchOnlyKnownKinds?: boolean;
  readonly hooks?: RiverHooks;
  readonly insertMiddleware?: readonly InsertMiddleware[];
  /**
   * Wait after a timeout, or after aborting a running handler, before an
   * attempt is stuck. Defaults to 10 seconds.
   */
  readonly jobStuckThresholdMs?: number;
  /** Default cooperative job timeout. Defaults to one minute; null disables it. */
  readonly jobTimeoutMs?: number | null;
  /** Never elect this client, so it runs no leader-owned services. */
  readonly leaderElectionDisabled?: boolean;
  /**
   * Structured logger with pino's `(attributes, message)` argument order.
   * Defaults to `console` for warnings and errors; `false` silences River.
   */
  readonly logger?: Logger | false;
  /** Settings for leader-owned services. */
  readonly maintenance?: MaintenanceSettings;
  readonly middleware?: readonly WorkMiddleware[];
  readonly plugins?: readonly RiverPlugin[];
  /** Disable backend notification streams and rely on bounded polling. */
  readonly pollOnly?: boolean;
  readonly periodicJobs?: readonly PeriodicJob[];
  readonly queues?: Readonly<Record<string, QueueSettings>>;
  /** Persisted queue control polling interval. Defaults to 2 seconds. */
  readonly queueControlPollIntervalMs?: number;
  /** Configured queue heartbeat interval. Defaults to 30 seconds. */
  readonly queueHeartbeatIntervalMs?: number;
  /** Override retry scheduling; invalid times fall back to River's default. */
  readonly retryPolicy?: RetryPolicy;
  /** Policy invoked after a timed-out attempt exceeds its stuck threshold. */
  readonly stuckHandler?: JobStuckHandler;
  readonly workers?: Workers;
}

/** Returns when a failed job should next run. */
export type RetryPolicy = (
  job: Readonly<JobRow>,
  now: Temporal.Instant
) => Temporal.Instant;

/** @internal Validate and snapshot caller-owned runtime configuration. */
export function normalizeRuntimeSettings(
  options: RuntimeSettings
): Readonly<RuntimeSettings> {
  rejectExplicitUndefined(options);
  if (options.clientId !== undefined) validateClientId(options.clientId);
  if (options.completionBatchSize !== undefined) {
    requirePositiveInteger("completionBatchSize", options.completionBatchSize);
  }
  if (options.completionFlushIntervalMs !== undefined) {
    requireNonNegativeInteger(
      "completionFlushInterval",
      options.completionFlushIntervalMs
    );
  }
  if (options.queueControlPollIntervalMs !== undefined) {
    requirePositiveInteger(
      "queueControlPollInterval",
      options.queueControlPollIntervalMs
    );
  }
  if (options.queueHeartbeatIntervalMs !== undefined) {
    requirePositiveInteger(
      "queueHeartbeatInterval",
      options.queueHeartbeatIntervalMs
    );
  }
  if (options.fetchCooldownMs !== undefined) {
    requirePositiveInteger("fetchCooldown", options.fetchCooldownMs);
  }
  if (options.jobStuckThresholdMs !== undefined) {
    requireNonNegativeInteger("jobStuckThreshold", options.jobStuckThresholdMs);
  }
  if (options.jobTimeoutMs !== undefined && options.jobTimeoutMs !== null) {
    requirePositiveInteger("jobTimeout", options.jobTimeoutMs);
  }
  if (options.logger !== undefined && !isLoggerOption(options.logger)) {
    throw new ValidationError(
      "logger must implement debug, info, warn, and error, or be false"
    );
  }
  if (
    options.stuckHandler !== undefined &&
    typeof options.stuckHandler !== "function"
  ) {
    throw new ValidationError("stuckHandler must be a function");
  }
  normalizeEventLoopDelay(options.eventLoopDelay);
  validatePlugins(options.plugins, "client");
  if (options.workers !== undefined) requireWorkers(options.workers);
  if (
    options.fetchOnlyKnownKinds !== undefined &&
    typeof options.fetchOnlyKnownKinds !== "boolean"
  ) {
    throw new ValidationError("fetchOnlyKnownKinds must be a boolean");
  }
  if (
    options.leaderElectionDisabled !== undefined &&
    typeof options.leaderElectionDisabled !== "boolean"
  ) {
    throw new ValidationError("leaderElectionDisabled must be a boolean");
  }
  if (
    options.leaderElectionDisabled === true &&
    options.periodicJobs !== undefined &&
    options.periodicJobs.length > 0
  ) {
    throw new ConfigurationError(
      "periodicJobs must be empty when leaderElectionDisabled is true, because this client never leads"
    );
  }

  const plugins = options.plugins?.map((plugin) => {
    const clone = {
      ...(plugin.hooks === undefined
        ? {}
        : { hooks: Object.freeze({ ...plugin.hooks }) }),
      ...(plugin.insertMiddleware === undefined
        ? {}
        : {
            insertMiddleware: Object.freeze([...plugin.insertMiddleware]),
          }),
      ...(plugin.middleware === undefined
        ? {}
        : { middleware: Object.freeze([...plugin.middleware]) }),
      name: plugin.name,
    };
    cloneJobArgsTransformPlugin(plugin, clone);
    cloneJobInsertMetadataTransformPlugin(plugin, clone);
    return Object.freeze(clone);
  });
  const maintenance =
    options.maintenance === undefined
      ? undefined
      : Object.freeze({
          ...options.maintenance,
          ...(options.maintenance.reindexerIndexNames === undefined
            ? {}
            : {
                reindexerIndexNames: Object.freeze([
                  ...options.maintenance.reindexerIndexNames,
                ]),
              }),
        });
  return Object.freeze({
    ...options,
    ...(options.eventLoopDelay === undefined || options.eventLoopDelay === false
      ? {}
      : { eventLoopDelay: Object.freeze({ ...options.eventLoopDelay }) }),
    ...(options.hooks === undefined
      ? {}
      : { hooks: Object.freeze({ ...options.hooks }) }),
    ...(options.insertMiddleware === undefined
      ? {}
      : {
          insertMiddleware: Object.freeze([...options.insertMiddleware]),
        }),
    ...(maintenance === undefined ? {} : { maintenance }),
    ...(options.middleware === undefined
      ? {}
      : { middleware: Object.freeze([...options.middleware]) }),
    ...(options.periodicJobs === undefined
      ? {}
      : { periodicJobs: Object.freeze([...options.periodicJobs]) }),
    ...(plugins === undefined ? {} : { plugins: Object.freeze(plugins) }),
    ...(options.queues === undefined
      ? {}
      : {
          queues: normalizeQueues(
            options.queues,
            options.fetchCooldownMs ?? DEFAULT_FETCH_COOLDOWN_MS
          ),
        }),
  });
}

/** @internal Where the runtime publishes events and waits for delivery. */
export interface RuntimeEventSink {
  drain(): Promise<void>;
  emit(event: RiverEvent): Promise<void>;
}

/**
 * Replacement clock, randomness, and timers for a client's runtime, so
 * tests drive River's timing deterministically. See
 * `overrideRuntimeTiming` in `riverqueue/unstable-driver`.
 */
export interface RuntimeTiming {
  /** Wall-clock time River records. Default: `Temporal.Now.instant()`. */
  readonly now?: () => Temporal.Instant;
  /** Numbers in `[0, 1)` for jitter. Default: `Math.random`. */
  readonly random?: () => number;
  /**
   * Every delay, deadline, and interval River waits for, and the monotonic
   * clock they count against. Default: unreferenced `setTimeout` handles
   * and `performance.now()`.
   */
  readonly timer?: RuntimeTimer;
}

/** Event-loop delay monitoring configuration, in milliseconds. */
export interface EventLoopDelaySettings {
  readonly reportIntervalMs?: number;
  readonly resolutionMs?: number;
  readonly warningThresholdMs?: number;
}

/** A running client's lifecycle state. */
export type RunState = "failed" | "running" | "stopped" | "stopping";

/** A snapshot of a running client, from `run.diagnostics`. */
export interface RunDiagnostics {
  readonly activeAttempts: number;
  readonly clientId: string;
  readonly completionCapacity: number;
  readonly completionQueries: number;
  readonly eventLoopDelay: EventLoopDelayObservation | null;
  readonly maintenance: MaintenanceDiagnostics | null;
  readonly pendingCompletions: number;
  readonly queues: Readonly<Record<string, QueueRuntimeDiagnostics>>;
  readonly state: RunState;
  readonly executors: Readonly<Record<string, JsonObject>>;
}

/** One queue's configuration and pause state in {@link RunDiagnostics}. */
export interface QueueRuntimeDiagnostics {
  readonly fetchCooldown: Temporal.Duration;
  readonly maxWorkers: number;
  readonly paused: boolean;
  readonly pollInterval: Temporal.Duration;
}

/** Stop configuration after conversion from the public stop options. */
export interface StopSettings {
  readonly mode?: "cancel" | "graceful";
  readonly signal?: AbortSignal;
  readonly timeoutMs?: number;
}

/**
 * @internal Queue configuration keys a pilot owns, and how it parses them
 * into its own settings for one queue.
 */
export interface PilotQueueParser {
  readonly keys: ReadonlySet<string>;
  parse(queue: string, config: Readonly<Record<string, unknown>>): unknown;
}

/** @internal One queue's validated configuration. */
export interface ResolvedQueue {
  readonly config: Required<QueueSettings>;
  /** The pilot's parsed settings, or undefined without a pilot parser. */
  readonly pilotSettings: unknown;
}

/** River's own queue keys, after conversion to settings. */
const QUEUE_SETTINGS_KEYS: ReadonlySet<string> = new Set([
  "fetchCooldownMs",
  "maxWorkers",
  "pollIntervalMs",
]);

/**
 * @internal Validate one queue's configuration: River's keys, and the keys the
 * pilot owns, which it parses synchronously. Any other key is rejected.
 * Nothing is changed when this throws.
 */
export function resolveQueueConfig(
  name: string,
  config: QueueSettings,
  parser: PilotQueueParser | undefined,
  defaultFetchCooldownMs: number
): ResolvedQueue {
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (config === null || typeof config !== "object") {
    return {
      config: normalizeQueueConfig(config, defaultFetchCooldownMs),
      pilotSettings: undefined,
    };
  }
  const own: Record<string, unknown> = {};
  const owned: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(config)) {
    if (QUEUE_SETTINGS_KEYS.has(key)) own[key] = value;
    else if (parser?.keys.has(key) === true) owned[key] = value;
    else throw unknownQueueOption(name, key);
  }
  const normalized = normalizeQueueConfig(
    own as unknown as QueueSettings,
    defaultFetchCooldownMs
  );
  if (parser === undefined) {
    return { config: normalized, pilotSettings: undefined };
  }
  const parsed = parser.parse(name, Object.freeze(owned));
  if (
    (typeof parsed === "object" || typeof parsed === "function") &&
    parsed !== null &&
    typeof (parsed as { readonly then?: unknown }).then === "function"
  ) {
    throw new ConfigurationError(
      "a pilot's queue option parser must return its settings synchronously"
    );
  }
  return { config: normalized, pilotSettings: parsed };
}

/** @internal Validate every configured queue, requiring at least one. */
export function resolveQueues(
  queues: Readonly<Record<string, QueueSettings>> | undefined,
  parser: PilotQueueParser | undefined,
  defaultFetchCooldownMs: number
): Readonly<Record<string, ResolvedQueue>> {
  if (queues === undefined || Object.keys(queues).length === 0) {
    throw new ValidationError("runtime requires at least one configured queue");
  }
  const result = Object.create(null) as Record<string, ResolvedQueue>;
  for (const [name, config] of Object.entries(queues)) {
    result[validateQueueName(name)] = Object.freeze(
      resolveQueueConfig(name, config, parser, defaultFetchCooldownMs)
    );
  }
  return Object.freeze(result);
}

function unknownQueueOption(queue: string, key: string): ValidationError {
  return new ValidationError(
    `queue ${JSON.stringify(queue)} has no option ${JSON.stringify(key)}`,
    { details: { option: key, queue } }
  );
}

/** Validate the configured queues, requiring at least one. */
function normalizeQueues(
  queues: Readonly<Record<string, QueueSettings>> | undefined,
  defaultFetchCooldownMs: number
): Readonly<Record<string, Required<QueueSettings>>> {
  if (queues === undefined || Object.keys(queues).length === 0) {
    throw new ValidationError("runtime requires at least one configured queue");
  }
  const result = Object.create(null) as Record<string, Required<QueueSettings>>;
  for (const [name, config] of Object.entries(queues)) {
    result[validateQueueName(name)] = resolveQueueConfig(
      name,
      config,
      undefined,
      defaultFetchCooldownMs
    ).config;
  }
  return Object.freeze(result);
}

/**
 * Validate one queue configuration and fill in its defaults, taking its
 * claim cooldown from the client's unless it sets its own.
 */
function normalizeQueueConfig(
  config: QueueSettings,
  defaultFetchCooldownMs: number
): Required<QueueSettings> {
  rejectExplicitUndefined(config);
  const fetchCooldownMs = requirePositiveInteger(
    "queue fetchCooldown",
    config.fetchCooldownMs ?? defaultFetchCooldownMs
  );
  const pollIntervalMs = requirePositiveInteger(
    "queue pollInterval",
    config.pollIntervalMs ?? 1_000
  );
  if (pollIntervalMs < fetchCooldownMs) {
    throw new ValidationError(
      "queue pollInterval cannot be shorter than fetchCooldown, which defaults to the client's fetchCooldown"
    );
  }
  const maxWorkers = requirePositiveInteger(
    "queue maxWorkers",
    config.maxWorkers
  );
  if (maxWorkers > 10_000) {
    throw new ValidationError("queue maxWorkers must be at most 10000");
  }
  return Object.freeze({
    fetchCooldownMs,
    maxWorkers,
    pollIntervalMs,
  });
}

/** Validate event-loop delay settings, or null when monitoring is off. */
function normalizeEventLoopDelay(
  options: false | EventLoopDelaySettings | undefined
): EventLoopDelayMonitorOptions | null {
  if (options === false) return null;
  const value = options ?? {};
  rejectExplicitUndefined(value);
  return {
    reportIntervalMs: requirePositiveInteger(
      "eventLoopDelay.reportInterval",
      value.reportIntervalMs ?? 1_000
    ),
    resolutionMs: requirePositiveInteger(
      "eventLoopDelay.resolution",
      value.resolutionMs ?? 20
    ),
    warningThresholdMs: requireNonNegativeInteger(
      "eventLoopDelay.warningThreshold",
      value.warningThresholdMs ?? 100
    ),
  };
}

/** Require a worker registry with at least one registered kind. */
function requireWorkers(workers: Workers | undefined): Workers {
  if (workers === undefined || workers.size === 0) {
    throw new ValidationError(
      "runtime requires at least one registered worker"
    );
  }
  return workers;
}

/** Require a client ID of 1 to 100 characters. */
function validateClientId(value: string): string {
  if (value.length === 0 || value.length > 100) {
    throw new ValidationError(
      "clientId must contain between 1 and 100 characters"
    );
  }
  return value;
}

/**
 * Reject duplicate or unnamed plugins, and client-only plugins configured
 * on a worker.
 */
export function validatePlugins(
  plugins: readonly { readonly name: string }[] | undefined,
  scope: "client" | "worker"
): void {
  const names = new Set<string>();
  for (const plugin of plugins ?? []) {
    if (scope === "worker" && isJobArgsTransformPlugin(plugin)) {
      throw new ValidationError(
        "job argument transform plugins must be configured on Client"
      );
    }
    if (scope === "worker" && isJobInsertMetadataTransformPlugin(plugin)) {
      throw new ValidationError(
        "job insert metadata transform plugins must be configured on Client"
      );
    }
    if (plugin.name.length === 0)
      throw new ValidationError("plugin name is empty");
    if (names.has(plugin.name)) {
      throw new ValidationError(`duplicate plugin name ${plugin.name}`);
    }
    names.add(plugin.name);
  }
}

/** A unique client ID for this process. */
export function makeClientId(): string {
  return `riverqueue-js-${process.pid}-${crypto.randomUUID()}`;
}

/** River's default scheduler interval, which bounds the near-future fast path. */
const DEFAULT_SCHEDULER_INTERVAL_MS = 5_000;

/** @internal Runtime settings validated against the driver, with defaults applied. */
export interface ResolvedRuntimeSettings {
  readonly clientId: string;
  readonly completionBatchSize: number;
  readonly completionFlushIntervalMs: number;
  readonly errorHandler: RiverErrorHandler | undefined;
  /** Event-loop delay monitoring, or null when it is disabled. */
  readonly eventLoopDelay: EventLoopDelayMonitorOptions | null;
  readonly fetchCooldownMs: number;
  /**
   * The kinds claims are limited to, sorted, or empty to claim every kind.
   * `workers` is never empty, so neither is this when claims are limited.
   */
  readonly fetchKinds: readonly string[];
  /** Client hooks, plugin hooks first. */
  readonly hooks: readonly RiverHooks[];
  readonly jobArgsTransformers: readonly Readonly<JobArgsTransformer>[];
  readonly jobStuckThresholdMs: number;
  readonly jobTimeoutMs: number | null;
  readonly logger: InternalLogger;
  /** Settings for leader-owned services, or null when they do not run. */
  readonly maintenance: MaintenanceSettings | null;
  /** Client work middleware, plugin middleware first. */
  readonly middleware: readonly WorkMiddleware[];
  readonly pollOnly: boolean;
  readonly queueControlPollIntervalMs: number;
  readonly queueHeartbeatIntervalMs: number;
  readonly queues: Readonly<Record<string, Required<QueueSettings>>>;
  readonly retryPolicy: RetryPolicy | undefined;
  readonly schedulerIntervalMs: number;
  readonly stuckHandler: JobStuckHandler | undefined;
  readonly workLogger: Logger;
  readonly workers: Workers;
}

/**
 * @internal Validate runtime settings against `driver` and apply defaults. This runs
 * the driver's start preflight, so a misconfigured backend fails before any
 * runtime work begins.
 */
export function resolveRuntimeSettings(
  driver: RuntimeDriver,
  options: RuntimeSettings
): ResolvedRuntimeSettings {
  const workers = requireWorkers(options.workers);
  const clientId = validateClientId(options.clientId ?? makeClientId());
  const fetchCooldownMs = requirePositiveInteger(
    "fetchCooldown",
    options.fetchCooldownMs ?? DEFAULT_FETCH_COOLDOWN_MS
  );
  const queues = normalizeQueues(options.queues, fetchCooldownMs);
  // Like Go, the kinds are those registered when the runtime starts.
  const fetchKinds = Object.freeze(
    options.fetchOnlyKnownKinds === true ? [...workers.kinds()].sort() : []
  );
  const pollOnly = options.pollOnly ?? false;
  const maintenanceEnabled = options.leaderElectionDisabled !== true;
  const reindexEnabled =
    maintenanceEnabled &&
    driver.maintenanceReindex !== undefined &&
    (options.maintenance === undefined ||
      options.maintenance.reindexerIndexNames === undefined ||
      options.maintenance.reindexerIndexNames.length > 0);
  driver.runtimeStartPreflight?.({
    maintenance: maintenanceEnabled,
    notifications: !pollOnly,
    reindex: reindexEnabled,
  });
  const queueControlPollIntervalMs = requirePositiveInteger(
    "queueControlPollInterval",
    options.queueControlPollIntervalMs ?? 2_000
  );
  const queueHeartbeatIntervalMs = requirePositiveInteger(
    "queueHeartbeatInterval",
    options.queueHeartbeatIntervalMs ?? 30_000
  );
  const jobTimeoutMs =
    options.jobTimeoutMs === undefined
      ? 60_000
      : options.jobTimeoutMs === null
        ? null
        : requirePositiveInteger("jobTimeout", options.jobTimeoutMs);
  const jobStuckThresholdMs = requireNonNegativeInteger(
    "jobStuckThreshold",
    options.jobStuckThresholdMs ?? 10_000
  );
  const middleware = Object.freeze([
    ...(options.plugins?.flatMap((plugin) => plugin.middleware ?? []) ?? []),
    ...(options.middleware ?? []),
  ]);
  const hooks = Object.freeze([
    ...(options.plugins?.flatMap((plugin) =>
      plugin.hooks === undefined ? [] : [plugin.hooks]
    ) ?? []),
    ...(options.hooks === undefined ? [] : [options.hooks]),
  ]);
  const jobArgsTransformers = Object.freeze(
    getJobArgsTransformers(options.plugins)
  );
  const workLogger = resolveLogger(options.logger);
  validatePlugins(options.plugins, "client");
  const eventLoopDelay = normalizeEventLoopDelay(options.eventLoopDelay);
  const completionBatchSize = requirePositiveInteger(
    "completionBatchSize",
    options.completionBatchSize ?? 100
  );
  const completionFlushIntervalMs = requireNonNegativeInteger(
    "completionFlushInterval",
    options.completionFlushIntervalMs ?? 50
  );
  if (
    maintenanceEnabled &&
    options.maintenance !== undefined &&
    !supportsMaintenance(driver)
  ) {
    throw new ValidationError(
      "maintenance was configured but the runtime backend does not support it"
    );
  }
  const maintenance = resolveMaintenanceSettings(
    options.maintenance,
    jobTimeoutMs,
    options.jobTimeoutMs !== undefined
  );
  return Object.freeze({
    clientId,
    completionBatchSize,
    completionFlushIntervalMs,
    errorHandler: options.errorHandler,
    eventLoopDelay,
    fetchCooldownMs,
    fetchKinds,
    hooks,
    jobArgsTransformers,
    jobStuckThresholdMs,
    jobTimeoutMs,
    logger: internalLogger(workLogger),
    maintenance:
      !maintenanceEnabled || !supportsMaintenance(driver) ? null : maintenance,
    middleware,
    pollOnly,
    queueControlPollIntervalMs,
    queueHeartbeatIntervalMs,
    queues,
    retryPolicy: options.retryPolicy,
    // Retries and snoozes due within one scheduler pass are persisted as
    // available, as River's executor does, whether or not this runtime runs
    // the scheduler itself.
    schedulerIntervalMs:
      maintenance.schedulerIntervalMs ?? DEFAULT_SCHEDULER_INTERVAL_MS,
    stuckHandler: options.stuckHandler,
    workLogger,
    workers,
  });
}

/**
 * Maintenance settings for the configured job timeout. Unless configured,
 * the rescuer waits an hour past an explicitly configured job timeout, and
 * it may never rescue sooner than the timeout.
 */
function resolveMaintenanceSettings(
  value: MaintenanceSettings | undefined,
  timeoutMs: number | null,
  jobTimeoutConfigured: boolean
): MaintenanceSettings {
  if (
    value?.rescueAfterMs !== undefined &&
    timeoutMs !== null &&
    value.rescueAfterMs < timeoutMs
  ) {
    throw new ValidationError(
      "maintenance.rescueAfter cannot be shorter than the longest job timeout"
    );
  }
  if (
    value?.rescueAfterMs !== undefined ||
    !jobTimeoutConfigured ||
    timeoutMs === null
  ) {
    return value ?? {};
  }
  return {
    ...value,
    rescueAfterMs: timeoutMs + 3_600_000,
  };
}
