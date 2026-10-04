import type { Client } from "./client.js";
import type { QueueRow, RuntimeDriver } from "./driver.js";
import { LifecycleError, ValidationError } from "./errors.js";
import type { RiverEvent } from "./events.js";
import type { RiverHooks } from "./extensions.js";
import { unrefTimeout } from "./internal/abort.js";
import { SYSTEM_TIMER, type RuntimeTimer } from "./internal/backoff.js";
import { EventLoopDelayMonitor } from "./internal/event-loop-delay-monitor.js";
import type { JobRow } from "./job.js";
import type { InternalLogger } from "./logger.js";
import { publishRiverMetric, type RiverMetric } from "./metrics.js";
import {
  toQueueSettings,
  toStopSettings,
  type QueueConfig,
  type StopOptions,
} from "./options.js";
import type { PeriodicJobStore } from "./periodic-job-store.js";
import type { PeriodicJobs, PeriodicJobsStartParams } from "./periodic.js";
import type { Pilot, PilotDatabase, PilotService } from "./pilot.js";
import { AttemptRunner } from "./runtime/attempt-runner.js";
import { CompletionPipeline } from "./runtime/completion-pipeline.js";
import type { RuntimeContext } from "./runtime/context.js";
import { canonicalError, invokeHook } from "./runtime/failures.js";
import { NotificationPump } from "./runtime/notification-pump.js";
import { PeerAttempts } from "./runtime/peer-attempts.js";
import type { PilotOperations } from "./runtime/pilot-operations.js";
import { QueueProducer } from "./runtime/queue-producer.js";
import { serviceList, superviseService } from "./runtime/service-supervisor.js";
import {
  resolveQueueConfig,
  resolveRuntimeSettings,
  type PilotQueueParser,
  type QueueSettings,
  type ResolvedQueue,
  type RunDiagnostics,
  type RunState,
  type RuntimeTiming,
  type RuntimeEventSink,
  type RuntimeSettings,
  type StopSettings,
} from "./runtime/settings.js";
import {
  rejectExplicitUndefined,
  requirePositiveInteger,
} from "./runtime/validation.js";
import { RuntimeServices } from "./services.js";

export type { EventLoopDelayObservation } from "./internal/event-loop-delay-monitor.js";
/** @internal */
export { defaultNextRetry } from "./runtime/completion-command.js";
/** @internal */
export { normalizeRuntimeSettings } from "./runtime/settings.js";
export type {
  EventLoopDelaySettings,
  JobStuckHandler,
  JobStuckHandlerParams,
  JobStuckHandlerResult,
  QueueRuntimeDiagnostics,
  QueueSettings,
  RetryPolicy,
  RunDiagnostics,
  RunState,
  RuntimeSettings,
  StopSettings,
} from "./runtime/settings.js";
/** @internal */
export type { RuntimeEventSink } from "./runtime/settings.js";
export type { RuntimeTiming } from "./runtime/settings.js";
export {
  currentWorkContext,
  recordOutput,
  setMetadata,
} from "./runtime/work-context.js";
export type { CurrentWorkContext } from "./runtime/work-context.js";

const runtimeTimingOverrides = new WeakMap<object, RuntimeTiming>();

/**
 * Replace the clock, randomness, and timers of the runtime `client` starts
 * next, including those it runs a companion's producer sessions and
 * services with: report jitter and intervals, keep-alive and shutdown
 * deadlines, service backoff, and leadership deadlines. Call it before
 * `start()`; a running runtime keeps its timing. For tests only.
 */
export function overrideRuntimeTiming(
  client: object,
  timing: RuntimeTiming
): void {
  // JavaScript callers may pass anything.
  const target: unknown = client;
  if (typeof target !== "object" || target === null) {
    throw new ValidationError("overrideRuntimeTiming requires a client");
  }
  for (const key of ["now", "random"] as const) {
    if (timing[key] !== undefined && typeof timing[key] !== "function") {
      throw new ValidationError(`timing.${key} must be a function`);
    }
  }
  const timer: unknown = timing.timer;
  if (
    timer !== undefined &&
    (typeof timer !== "object" ||
      timer === null ||
      typeof (timer as Partial<RuntimeTimer>).delay !== "function" ||
      typeof (timer as Partial<RuntimeTimer>).now !== "function" ||
      typeof (timer as Partial<RuntimeTimer>).timeout !== "function")
  ) {
    throw new ValidationError(
      "timing.timer must have delay, now, and timeout methods"
    );
  }
  runtimeTimingOverrides.set(client, {
    ...(timing.now !== undefined && { now: timing.now }),
    ...(timing.random !== undefined && { random: timing.random }),
    ...(timing.timer !== undefined && { timer: timing.timer }),
  });
}

/**
 * @internal What a client hands its runtime: its backend's name, the
 * operations its pilot may intercept, and the pilot's parts, if any.
 */
export interface RuntimeBinding {
  /** The client's insert notification limiter, for the scheduler. */
  readonly allowInsertNotifications: (
    queues: readonly string[]
  ) => readonly string[];
  readonly backend: string;
  readonly database?: PilotDatabase<unknown>;
  readonly operations: PilotOperations;
  readonly pilot?: Pilot<unknown>;
  readonly pilotQueueParser: PilotQueueParser | undefined;
  /** Each configured queue's settings, as the pilot parsed them. */
  readonly pilotQueueSettings: Readonly<Record<string, unknown>>;
}

/**
 * Handle owning one supervised Client runtime.
 *
 * `Config` is the queue configuration `addQueue` and `updateQueue` accept.
 * TypeScript compares handles structurally, so a handle for River's own
 * queue configuration also type-checks as one accepting more keys; the
 * runtime rejects any queue key the client doesn't know.
 */
export class RunHandle<Config extends QueueConfig = QueueConfig> {
  readonly #controller: RuntimeController;

  /** Rejects immediately if an owned background task fails. */
  readonly completed: Promise<void>;

  /** @internal */
  constructor(controller: RuntimeController) {
    this.#controller = controller;
    this.completed = controller.completed;
  }

  get diagnostics(): RunDiagnostics {
    return this.#controller.diagnostics;
  }

  get state(): RunState {
    return this.#controller.state;
  }

  /** Add and persist a queue, starting claims only after its controls load. */
  async addQueue(name: string, config: Config): Promise<void> {
    await this.#controller.addQueue(name, toQueueSettings(config));
  }

  /** Stop locally claiming a queue; its persisted row expires naturally. */
  removeQueue(name: string): Promise<boolean> {
    return this.#controller.removeQueue(name);
  }

  /** Ask whichever runtime currently leads to resign its exact term. */
  requestLeadershipResignation(): Promise<void> {
    return this.#controller.requestLeadershipResignation();
  }

  /** Atomically replace local queue capacity/polling configuration. */
  async updateQueue(name: string, config: Config): Promise<void> {
    await this.#controller.updateQueue(name, toQueueSettings(config));
  }

  /**
   * Stop the runtime. A graceful stop (the default) waits for running jobs;
   * `mode: "cancel"`, the `signal`, or an elapsed `timeout` aborts them.
   */
  async stop(options: StopOptions = {}): Promise<void> {
    await this.#controller.stop(toStopSettings(options));
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.stop({ mode: "graceful" });
  }
}

/**
 * @internal Runtime implementation created by Client.start.
 *
 * The controller owns the runtime's lifecycle (running, stopping, stopped, or
 * failed), its background task supervision, and event and metric delivery.
 * The work itself is split among collaborators sharing a
 * {@link RuntimeContext}: a {@link QueueProducer} claims jobs, an
 * {@link AttemptRunner} works them, a {@link CompletionPipeline} persists
 * their outcomes, and a {@link NotificationPump} applies backend
 * notifications.
 */
export class RuntimeController {
  readonly ready: Promise<void>;

  /**
   * Ends polling for cancellations once producers drained, so a job can
   * still be cancelled while its queue drains.
   */
  readonly #cancellationPollAbort = new AbortController();
  /** How often a runtime without notifications checks for cancellations. */
  readonly #cancellationPollIntervalMs: number;
  readonly #claimAbort = new AbortController();
  readonly #client: Client;
  readonly #clientId: string;
  readonly #completedPromise: Promise<void>;
  #completedReject!: (reason: unknown) => void;
  #completedResolve!: () => void;
  readonly #completions: CompletionPipeline;
  readonly #context: RuntimeContext;
  readonly #eventLoopDelayMonitor: EventLoopDelayMonitor | undefined;
  readonly #events: RuntimeEventSink;
  #fatalError: LifecycleError | undefined;
  /** The client's claim cooldown, for queues that don't set their own. */
  readonly #fetchCooldownMs: number;
  readonly #hooks: readonly RiverHooks[];
  readonly #initialQueues: Readonly<Record<string, ResolvedQueue>>;
  /** Tasks that end only after producers drained. */
  readonly #lateTasks = new Set<Promise<void>>();
  #livenessTimer: NodeJS.Timeout | undefined;
  readonly #logger: InternalLogger;
  /** Ends maintenance and leadership, as a stop begins. */
  readonly #maintenanceAbort = new AbortController();
  readonly #notifications: NotificationPump;
  readonly #nowFunc: () => Temporal.Instant;
  readonly #peers: PeerAttempts | undefined;
  readonly #pilotQueueParser: PilotQueueParser | undefined;
  readonly #pollOnly: boolean;
  readonly #producer: QueueProducer;
  readonly #runAbort = new AbortController();
  readonly #runner: AttemptRunner;
  /** The pilot's runtime services, listed once per run. */
  readonly #runtimeServices: readonly PilotService<void>[];
  readonly #services: RuntimeServices | null;
  /** Ends the pilot's runtime services, once producers drained. */
  readonly #servicesAbort = new AbortController();
  #shutdownPromise: Promise<void> | undefined;
  #state: RunState = "running";
  readonly #tasks = new Set<Promise<void>>();

  constructor(
    client: Client,
    driver: RuntimeDriver,
    events: RuntimeEventSink,
    periodicJobs: PeriodicJobs,
    options: RuntimeSettings,
    binding: RuntimeBinding,
    dependencies: RuntimeTiming = runtimeTimingOverrides.get(client) ?? {}
  ) {
    const settings = resolveRuntimeSettings(driver, options);
    // Like River for Go's producers, which check queue settings and
    // cancellations on the same interval.
    this.#cancellationPollIntervalMs = settings.queueControlPollIntervalMs;
    this.#client = client;
    this.#clientId = settings.clientId;
    this.#events = events;
    this.#fetchCooldownMs = settings.fetchCooldownMs;
    this.#hooks = settings.hooks;
    this.#initialQueues = Object.fromEntries(
      Object.entries(settings.queues).map(([name, config]) => [
        name,
        { config, pilotSettings: binding.pilotQueueSettings[name] },
      ])
    );
    this.#pilotQueueParser = binding.pilotQueueParser;
    this.#pollOnly = settings.pollOnly;
    this.#logger = settings.logger;
    this.#nowFunc = dependencies.now ?? (() => Temporal.Now.instant());
    const random = dependencies.random ?? Math.random;

    const context: RuntimeContext = Object.freeze({
      claimSignal: this.#claimAbort.signal,
      backend: binding.backend,
      client,
      clientId: settings.clientId,
      driver,
      emit: (event: RiverEvent) => this.#emit(event),
      emitMetric: (metric: RiverMetric) => {
        this.#emitMetric(metric);
      },
      fail: (error: unknown) => {
        this.#fail(error);
      },
      guard: (task: Promise<void>) => this.#guard(task),
      logger: settings.logger,
      now: () => this.#now(),
      operations: binding.operations,
      random,
      runSignal: this.#runAbort.signal,
      timer: dependencies.timer ?? SYSTEM_TIMER,
      trackTask: (task: Promise<void>) => {
        this.#trackTask(task);
      },
    });
    this.#context = context;
    this.#completions = new CompletionPipeline(context, {
      batchSize: settings.completionBatchSize,
      concurrency: binding.pilot?.completionConcurrency,
      flushIntervalMs: settings.completionFlushIntervalMs,
      retryPolicy: settings.retryPolicy,
      schedulerIntervalMs: settings.schedulerIntervalMs,
    });
    this.#peers =
      binding.database === undefined
        ? undefined
        : new PeerAttempts(context, {
            completions: this.#completions,
            database: binding.database,
            errorHandler: settings.errorHandler,
            isWorking: (id) => this.#runner.isWorking(id),
            transformJobArgs: (row) => this.#runner.transformJobArgs(row),
            workerRetryPolicy: (kind) => this.#runner.workerRetryPolicy(kind),
          });
    this.#runner = new AttemptRunner(context, {
      completions: this.#completions,
      errorHandler: settings.errorHandler,
      hooks: settings.hooks,
      jobArgsTransformers: settings.jobArgsTransformers,
      jobStuckThresholdMs: settings.jobStuckThresholdMs,
      jobTimeoutMs: settings.jobTimeoutMs,
      middleware: settings.middleware,
      peers: this.#peers,
      runSignal: this.#runAbort.signal,
      stuckHandler: settings.stuckHandler,
      workLogger: settings.workLogger,
      workers: settings.workers,
    });
    const startProducer = binding.pilot?.startProducer?.bind(binding.pilot);
    this.#producer = new QueueProducer(context, this.#runner, {
      controlPollIntervalMs: settings.queueControlPollIntervalMs,
      fetchKinds: settings.fetchKinds,
      heartbeatIntervalMs: settings.queueHeartbeatIntervalMs,
      ...(startProducer === undefined || binding.database === undefined
        ? {}
        : {
            pilot: {
              database: binding.database,
              reportIntervalMs: settings.queueHeartbeatIntervalMs,
              startProducer,
            },
          }),
    });
    this.#services =
      settings.maintenance === null
        ? null
        : new RuntimeServices({
            allowInsertNotifications: binding.allowInsertNotifications,
            client,
            clientId: settings.clientId,
            driver,
            emit: (event) => this.#emit(event),
            logger: settings.logger,
            maintenance: settings.maintenance,
            now: () => this.#now(),
            onPeriodicJobsStart: (params) => this.#onPeriodicJobsStart(params),
            operations: binding.operations,
            periodicJobStore:
              binding.pilot?.periodicJobs === undefined
                ? undefined
                : pilotPeriodicJobStore(
                    binding.pilot.periodicJobs,
                    binding.database
                  ),
            periodicJobs,
            random,
            rescue: (job, now, signal) => this.#runner.rescue(job, now, signal),
            termServices:
              binding.pilot?.maintenanceServices === undefined
                ? []
                : serviceList(
                    binding.pilot.maintenanceServices(),
                    "maintenanceServices()"
                  ),
            timer: context.timer,
            ...(binding.pilot?.jobCleanerQueuesExcluded === undefined
              ? {}
              : {
                  jobCleanerQueuesExcluded:
                    binding.pilot.jobCleanerQueuesExcluded,
                }),
          });
    this.#runtimeServices =
      binding.pilot?.services === undefined
        ? []
        : serviceList(binding.pilot.services(), "services()");
    this.#notifications = new NotificationPump(context, {
      leaderResigned: () => this.#services?.leaderResigned(),
      producer: this.#producer,
      resignLeadership: () => this.#services?.resignLeadership(),
      runner: this.#runner,
    });
    if (settings.eventLoopDelay !== null) {
      this.#eventLoopDelayMonitor = new EventLoopDelayMonitor(
        settings.eventLoopDelay,
        (observation) => {
          if (!observation.exceededThreshold) return;
          void this.#emit({
            at: this.#now(),
            eventLoopDelay: observation,
            kind: "runtime_event_loop_delay",
          }).catch((error: unknown) => {
            this.#fail(error);
          });
        }
      );
    }

    this.#completedPromise = new Promise((resolve, reject) => {
      this.#completedResolve = resolve;
      this.#completedReject = reject;
    });
    void this.#completedPromise.catch(() => undefined);
    // An active worker runtime owns unfinished database work. Keep Node alive
    // even though individual polling and batching timers are unref'ed so an
    // insert-only client remains process-neutral.
    this.#livenessTimer = setInterval(() => undefined, 60_000);
    this.ready = this.#initialize();
    void this.ready.catch((error: unknown) => {
      this.#fail(error);
    });
  }

  get completed(): Promise<void> {
    return this.#completedPromise;
  }

  get diagnostics(): RunDiagnostics {
    return {
      activeAttempts: this.#runner.activeAttempts,
      clientId: this.#clientId,
      completionCapacity: this.#completions.maxPendingItems,
      completionQueries: this.#completions.inFlightQueries,
      eventLoopDelay: this.#eventLoopDelayMonitor?.last ?? null,
      maintenance: this.#services?.diagnostics ?? null,
      pendingCompletions: this.#completions.pendingItems,
      queues: this.#producer.diagnostics(),
      state: this.#state,
      executors: this.#runner.executorDiagnostics(),
    };
  }

  /** @internal The peer attempts of the pilot's client. */
  get peerAttempts(): PeerAttempts | undefined {
    return this.#peers;
  }

  get state(): RunState {
    return this.#state;
  }

  async addQueue(name: string, config: QueueSettings): Promise<void> {
    this.#requireRunning();
    await this.#producer.add(
      name,
      resolveQueueConfig(
        name,
        config,
        this.#pilotQueueParser,
        this.#fetchCooldownMs
      )
    );
  }

  /** @internal Apply a queue command already committed by this client. */
  applyCommittedQueueControl(queue: QueueRow): void {
    this.#producer.applyCommittedControl(queue);
  }

  cancelLocal(job: JobRow): void {
    this.#runner.cancelLocal(job);
  }

  async removeQueue(name: string): Promise<boolean> {
    this.#requireRunning();
    return this.#producer.remove(name);
  }

  async requestLeadershipResignation(): Promise<void> {
    this.#requireRunning();
    await this.#client.requestLeadershipResignation();
    await this.#services?.resignLeadership();
  }

  stop(options: StopSettings = {}): Promise<void> {
    rejectExplicitUndefined(options);
    const mode = options.mode ?? "graceful";
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
    if (mode !== "graceful" && mode !== "cancel") {
      return Promise.reject(new ValidationError("invalid stop mode"));
    }
    if (options.timeoutMs !== undefined) {
      requirePositiveInteger("stop timeout", options.timeoutMs);
    }
    if (options.signal?.aborted) return Promise.reject(options.signal.reason);

    if (this.#shutdownPromise === undefined) {
      if (this.#state !== "failed") this.#state = "stopping";
      this.#claimAbort.abort(new LifecycleError("River runtime is stopping"));
      this.#completions.drain();
      this.#shutdownPromise = this.#finishStop();
      void this.#shutdownPromise.catch(() => undefined);
    }
    if (mode === "cancel") {
      this.#cancelAttempts(new LifecycleError("River runtime cancelled"));
    }

    return withStopBounds(
      this.#shutdownPromise,
      options,
      mode === "graceful"
        ? () => {
            this.#cancelAttempts(
              new LifecycleError("River graceful stop deadline elapsed")
            );
          }
        : undefined
    );
  }

  async updateQueue(name: string, config: QueueSettings): Promise<void> {
    this.#requireRunning();
    await this.#producer.update(
      name,
      resolveQueueConfig(
        name,
        config,
        this.#pilotQueueParser,
        this.#fetchCooldownMs
      )
    );
  }
  /** Reread persisted queue pauses now, after this client paused or resumed every queue. */
  wakeQueueControl(): void {
    this.#producer.wakeControl();
  }

  /** Wake the claim loops of `queues` after another owner committed jobs. */
  wakeQueues(queues: Iterable<string>): void {
    for (const queue of queues) this.#producer.wake(queue);
  }

  #cancelAttempts(reason: LifecycleError): void {
    this.#runAbort.abort(reason);
    this.#runner.abortAttempts(reason);
  }

  async #emit(event: RiverEvent): Promise<void> {
    await this.#events.emit(event);
  }

  #emitMetric(metric: RiverMetric): void {
    publishRiverMetric(metric);
    for (const hooks of this.#hooks) {
      if (hooks.onMetric === undefined) continue;
      try {
        void Promise.resolve(hooks.onMetric(metric)).catch((error: unknown) => {
          this.#logger.error("River metric hook failed", {
            error: canonicalError(error, this.#now()).error,
          });
        });
      } catch (error: unknown) {
        this.#logger.error("River metric hook failed", {
          error: canonicalError(error, this.#now()).error,
        });
      }
    }
  }

  #fail(error: unknown): void {
    if (this.#fatalError !== undefined || this.#state === "stopped") return;
    this.#fatalError =
      error instanceof LifecycleError
        ? error
        : new LifecycleError("River background task failed", { cause: error });
    this.#state = "failed";
    this.#eventLoopDelayMonitor?.stop();
    this.#claimAbort.abort(this.#fatalError);
    this.#runAbort.abort(this.#fatalError);
    this.#completions.abort(this.#fatalError);
    this.#runner.abortAttempts(this.#fatalError);
    // A fatal failure tears the runtime down like a stop; `completed`
    // rejects once that cleanup finished.
    if (this.#shutdownPromise === undefined) {
      this.#shutdownPromise = this.#finishStop();
      void this.#shutdownPromise.catch(() => undefined);
    }
  }

  /**
   * Tear the runtime down in River's order, like River for Go: claims
   * already stopped, and leadership, maintenance, and the pilot's
   * maintenance services end as the stop begins; each queue drains its
   * attempts while its producer keeps reporting; then producers stop
   * reporting and shut down; then the pilot's services end while the
   * completer flushes; finally committed events are delivered. `completed`
   * settles only after all of it, rejecting with the first fatal failure.
   */
  async #finishStop(): Promise<void> {
    await this.ready.catch(() => undefined);
    this.#maintenanceAbort.abort(
      new LifecycleError("River runtime is stopping")
    );
    await this.#producer.drainAll();
    this.#cancellationPollAbort.abort(
      new LifecycleError("River runtime is stopping")
    );
    await Promise.allSettled(this.#tasks);
    this.#servicesAbort.abort(new LifecycleError("River runtime is stopping"));
    const [completionClose] = await Promise.allSettled([
      this.#completions.close(),
      ...this.#lateTasks,
    ]);
    // Events already committed are still delivered to `onEvent` hooks
    // before the runtime reports that it stopped.
    await this.#events.drain();
    this.#eventLoopDelayMonitor?.stop();
    this.#releaseLiveness();
    if (this.#fatalError !== undefined) {
      // The failure aborts the completer with itself, which is no failure
      // of the completer's own.
      if (
        completionClose.status === "rejected" &&
        completionClose.reason !== this.#fatalError
      ) {
        this.#logger.error("River completer failed while stopping", {
          error: canonicalError(completionClose.reason, this.#now()).error,
        });
      }
      this.#completedReject(this.#fatalError);
      throw this.#fatalError;
    }
    if (completionClose.status === "rejected") {
      this.#completedReject(completionClose.reason);
      throw completionClose.reason;
    }
    this.#state = "stopped";
    this.#completedResolve();
  }

  async #guard(task: Promise<void>): Promise<void> {
    try {
      await task;
    } catch (error: unknown) {
      this.#fail(error);
      throw error;
    }
  }

  async #initialize(): Promise<void> {
    // Like River for Go's client, learn whether the database delivers
    // notifications before anything starts. Without them, running jobs
    // learn of cancellations by polling until every producer has drained.
    const listen = await this.#listens();
    if (!listen) {
      this.#trackTask(
        this.#runner.pollCancellations(
          this.#cancellationPollIntervalMs,
          this.#cancellationPollAbort.signal
        )
      );
    }
    // Like River for Go's notifier, notifications are listening before any
    // queue claims, and a failure to listen fails the start.
    await this.#notifications.start(listen);
    // The pilot's services start before any queue claims; River promises
    // the order, not that they are ready.
    for (const service of this.#runtimeServices) {
      this.#trackLateTask(
        superviseService(service, undefined, this.#servicesAbort.signal, {
          logger: this.#logger,
          random: this.#context.random,
          timer: this.#context.timer,
        })
      );
    }
    try {
      for (const [name, queue] of Object.entries(this.#initialQueues)) {
        await this.#producer.start(name, queue, false);
      }
    } catch (error: unknown) {
      // Queues this start already started drain before it fails.
      this.#claimAbort.abort(
        new LifecycleError("River runtime failed to start", { cause: error })
      );
      await this.#producer.drainAll();
      throw error;
    }
    this.#trackTask(this.#guard(this.#producer.runControlLoop()));
    if (this.#services !== null) {
      // Leadership and maintenance end as a stop begins, like River for Go.
      this.#trackLateTask(
        this.#guard(this.#services.run(this.#maintenanceAbort.signal))
      );
    }
    this.#eventLoopDelayMonitor?.start();
  }

  /**
   * Whether this runtime hears notifications: it isn't poll-only, its driver
   * can subscribe, and the database delivers them, which a PostgreSQL
   * driver detects from the server.
   */
  async #listens(): Promise<boolean> {
    const driver = this.#context.driver;
    if (
      this.#pollOnly ||
      (driver.runtimeNotificationSubscribe === undefined &&
        driver.jobCancellationSubscribe === undefined)
    ) {
      return false;
    }
    const delivers = driver.runtimeDeliversNotifications?.bind(driver);
    if (
      delivers === undefined ||
      (await delivers({ signal: this.#claimAbort.signal }))
    ) {
      return true;
    }
    this.#logger.info(
      "River's database does not support LISTEN/NOTIFY; polling instead"
    );
    return false;
  }

  #now(): Temporal.Instant {
    return this.#nowFunc();
  }

  async #onPeriodicJobsStart(params: PeriodicJobsStartParams): Promise<void> {
    for (const hooks of this.#hooks) {
      if (hooks.onPeriodicJobsStart === undefined) continue;
      await invokeHook("onPeriodicJobsStart", () =>
        hooks.onPeriodicJobsStart?.(params)
      );
    }
  }

  #releaseLiveness(): void {
    if (this.#livenessTimer === undefined) return;
    clearInterval(this.#livenessTimer);
    this.#livenessTimer = undefined;
  }

  #requireRunning(): void {
    if (this.#state !== "running" || this.#claimAbort.signal.aborted) {
      throw new LifecycleError("River runtime is not running");
    }
  }

  /**
   * End leadership, maintenance, and the pilot's term services: the one
   * step of a stop whose place in the order is a setting.
   */
  #trackLateTask(task: Promise<void>): void {
    this.#lateTasks.add(task);
    void task.then(
      () => this.#lateTasks.delete(task),
      () => this.#lateTasks.delete(task)
    );
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
 * A pilot's periodic job store, whose `upsertMany` River runs in a pilot
 * transaction on the transaction inserting the jobs, so that it receives a
 * native handle like the pilot's interceptors do.
 */
function pilotPeriodicJobStore(
  store: PeriodicJobStore,
  database: PilotDatabase<unknown> | undefined
): PeriodicJobStore {
  if (database === undefined) return store;
  return {
    getAll: (options) => store.getAll(options),
    keepAliveAndReap: (ids, options) => store.keepAliveAndReap(ids, options),
    upsertMany: (tx, jobs) =>
      database.transaction((handle) => store.upsertMany(handle, jobs), { tx }),
  };
}

async function withStopBounds(
  stopping: Promise<void>,
  options: StopSettings,
  onTimeout?: () => void
): Promise<void> {
  const bounds: Promise<never>[] = [];
  let cancelTimer: (() => void) | undefined;
  let onAbort: (() => void) | undefined;
  const { timeoutMs } = options;
  if (timeoutMs !== undefined) {
    bounds.push(
      new Promise((_, reject) => {
        cancelTimer = unrefTimeout(() => {
          onTimeout?.();
          reject(new LifecycleError("River runtime stop timed out"));
        }, timeoutMs);
      })
    );
  }
  if (options.signal !== undefined) {
    bounds.push(
      new Promise((_, reject) => {
        onAbort = () => reject(options.signal?.reason);
        options.signal?.addEventListener("abort", onAbort, { once: true });
      })
    );
  }
  try {
    await Promise.race([stopping, ...bounds]);
  } finally {
    cancelTimer?.();
    if (onAbort !== undefined) {
      options.signal?.removeEventListener("abort", onAbort);
    }
  }
}
