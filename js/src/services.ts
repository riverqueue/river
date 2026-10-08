import type { Client, InsertManyItem } from "./client.js";
import type {
  LeaderTerm,
  RuntimeDriver,
  RuntimeJobCleanupParams,
  RuntimeJobRescue,
  RuntimeLeader,
} from "./driver.js";
import { ConfigurationError } from "./errors.js";
import type { RiverEvent } from "./events.js";
import type { JobRow } from "./job.js";
import type { PeriodicJobs, PeriodicJobsStartParams } from "./periodic.js";
import {
  advancePeriodicJobs,
  buildPeriodicInsert,
  nextPeriodicRunAt,
  periodicJobIds,
  resetPeriodicJobs,
  setPeriodicJobsChangeHandler,
} from "./periodic.js";
import type { PeriodicJobStore } from "./periodic-job-store.js";
import type { InternalLogger } from "./logger.js";
import { internalLogger, resolveLogger } from "./logger.js";
import {
  interruptibleDelay,
  raceWithAbort,
  unrefTimeout,
} from "./internal/abort.js";
import type { PilotService } from "./pilot.js";
import {
  superviseService,
  type SupervisorOptions,
} from "./runtime/service-supervisor.js";
import {
  BACKGROUND_BACKOFF,
  exponentialBackoffMs,
  SYSTEM_TIMER,
  type BackoffPolicy,
  type OperationTimeout,
  type RuntimeTimer,
} from "./internal/backoff.js";
import {
  MAINTENANCE_TIMEOUT_DEFAULT_MS,
  MaintenanceBatcher,
} from "./internal/maintenance-batch.js";
import { PilotOperations } from "./runtime/pilot-operations.js";

const PERIODIC_KEEP_ALIVE_INTERVAL_MS = 10 * 60_000;
const LEADER_LOCAL_DEADLINE_SAFETY_MS = 2_000;
const LEADER_TTL_PADDING_MS = 10_000;
const LEADER_RESIGN_ATTEMPTS = 3;
/**
 * Attempts to start a term's periodic job enqueuer before resigning the
 * term, like River for Go's `queueMaintainerMaxStartAttempts`.
 */
const MAINTENANCE_START_ATTEMPTS = 3;
/** Waits between those attempts: about 1 s, then 2 s, like Go's. */
const MAINTENANCE_START_BACKOFF: BackoffPolicy = Object.freeze({
  baseMs: 1_000,
  maxMs: 30_000,
});
const RESCUE_DECISION_CONCURRENCY = 32;
const SCHEDULER_NOTIFICATION_LOOKAHEAD_MS = 5;

/** Postgres indexes River rebuilds by default to control table bloat. */
export const REINDEXER_INDEX_NAMES_DEFAULT = Object.freeze([
  "river_job_args_index",
  "river_job_kind",
  "river_job_metadata_index",
  "river_job_pkey",
  "river_job_prioritized_fetching_index",
  "river_job_state_and_finalized_at_index",
  "river_job_unique_idx",
] as const);

/** Returns when the reindexer should next run after `after`. */
export type ReindexerSchedule = (after: Temporal.Instant) => Temporal.Instant;

export interface MaintenanceSettings {
  readonly cancelledJobRetentionMs?: number | null;
  readonly completedJobRetentionMs?: number | null;
  readonly discardedJobRetentionMs?: number | null;
  readonly electionIntervalMs?: number;
  readonly jobCleanerIntervalMs?: number;
  readonly jobCleanerTimeoutMs?: number | null;
  /**
   * How much shorter than the lease this client trusts its own term, so it
   * stops acting as leader before the lease can expire. Defaults to 2 s.
   * @internal
   */
  readonly leaderDeadlineSafetyMs?: number;
  /**
   * How much longer than the election interval a lease lasts. Defaults to
   * 10 s.
   * @internal
   */
  readonly leaderTtlPaddingMs?: number;
  /**
   * Timeout of the first attempt to resign leadership; later attempts wait
   * two and three times as long, like Go's elector.
   * @internal
   */
  readonly leaderResignTimeoutMs?: number;
  /**
   * Timeout of one batch for the scheduler, the rescuer's reads, and the
   * queue and notification cleaners. Defaults to Go River's 30 s.
   * @internal
   */
  readonly maintenanceTimeoutMs?: number;
  readonly notificationCleanerIntervalMs?: number;
  readonly notificationRetentionMs?: number;
  readonly queueCleanerIntervalMs?: number;
  readonly queueRetentionMs?: number;
  readonly reindexerIndexNames?: readonly string[];
  readonly reindexerSchedule?: ReindexerSchedule;
  readonly reindexerTimeoutMs?: number | null;
  readonly rescueAfterMs?: number;
  readonly rescuerIntervalMs?: number;
  readonly schedulerIntervalMs?: number;
}

/** The leader-owned maintenance services' state in {@link RunDiagnostics}. */
export interface MaintenanceDiagnostics {
  readonly isLeader: boolean;
  readonly lastError: string | null;
  readonly leader: LeaderTerm | null;
  readonly runs: Readonly<Record<MaintenanceServiceName, number>>;
}

/** The maintenance services a leader runs. */
export type MaintenanceServiceName =
  | "job_cleaner"
  | "notification_cleaner"
  | "periodic"
  | "queue_cleaner"
  | "reindexer"
  | "rescuer"
  | "scheduler";

interface NormalizedMaintenanceOptions {
  readonly cancelledJobRetentionMs: number | null;
  readonly completedJobRetentionMs: number | null;
  readonly discardedJobRetentionMs: number | null;
  readonly electionIntervalMs: number;
  readonly jobCleanerIntervalMs: number;
  readonly jobCleanerTimeoutMs: number | null;
  readonly leaderDeadlineSafetyMs: number;
  readonly leaderResignTimeoutMs: number;
  readonly leaderTtlPaddingMs: number;
  readonly maintenanceTimeoutMs: number;
  readonly notificationCleanerIntervalMs: number;
  readonly notificationRetentionMs: number;
  readonly queueCleanerIntervalMs: number;
  readonly queueRetentionMs: number;
  readonly reindexerIndexNames: readonly string[];
  readonly reindexerSchedule: ReindexerSchedule;
  readonly reindexerTimeoutMs: number | null;
  readonly rescueAfterMs: number;
  readonly rescuerIntervalMs: number;
  readonly schedulerIntervalMs: number;
}

interface RuntimeServicesOptions {
  /**
   * The client's insert notification limiter, for scheduled jobs. Defaults
   * to notifying every queue.
   */
  readonly allowInsertNotifications?: (
    queues: readonly string[]
  ) => readonly string[];
  readonly client: Client;
  readonly clientId: string;
  readonly driver: RuntimeDriver;
  readonly emit: (event: RiverEvent) => Promise<void>;
  /** Receives leadership and maintenance failures. Defaults to no logging. */
  readonly logger?: Pick<InternalLogger, "error" | "warn">;
  readonly jobCleanerQueuesExcluded?: readonly string[];
  readonly maintenance: MaintenanceSettings;
  readonly now: () => Temporal.Instant;
  /**
   * The rescuer's reads and updates, which the client's pilot may intercept.
   * Defaults to the driver's own.
   */
  readonly operations?: PilotOperations;
  /** Invoked when a leadership term starts enqueuing periodic jobs. */
  readonly onPeriodicJobsStart?: (
    params: PeriodicJobsStartParams
  ) => Promise<void>;
  readonly periodicJobStore?: PeriodicJobStore | undefined;
  readonly periodicJobs: PeriodicJobs;
  /** Jitter source for leadership retry backoff. Defaults to `Math.random`. */
  readonly random?: () => number;
  readonly rescue: (
    job: JobRow,
    now: Temporal.Instant,
    signal: AbortSignal
  ) => Promise<RuntimeJobRescue | null>;
  /**
   * A pilot's services run once per leadership term while this client
   * leads. Default: none.
   */
  readonly termServices?: readonly PilotService<LeaderTerm>[];
  /** Delays for restarting term services. Default: real time. */
  readonly timer?: RuntimeTimer;
}

/** @internal Supervised leader election and leader-owned common services. */
export class RuntimeServices {
  readonly #allowInsertNotifications: (
    queues: readonly string[]
  ) => readonly string[];
  readonly #client: Client;
  readonly #clientId: string;
  readonly #driver: RuntimeDriver;
  readonly #emit: (event: RiverEvent) => Promise<void>;
  readonly #logger: Pick<InternalLogger, "error" | "warn">;
  readonly #maintenance: NormalizedMaintenanceOptions;
  readonly #now: () => Temporal.Instant;
  readonly #operations: PilotOperations;
  readonly #onPeriodicJobsStart: (
    params: PeriodicJobsStartParams
  ) => Promise<void>;
  readonly #jobCleanerQueuesExcluded: readonly string[];
  readonly #periodicJobStore: PeriodicJobStore | undefined;
  readonly #periodicJobs: PeriodicJobs;
  readonly #random: () => number;
  readonly #termServices: readonly PilotService<LeaderTerm>[];
  readonly #timer: RuntimeTimer;
  /** Ends the term once the local trust deadline passes. */
  #trustDeadline: OperationTimeout | undefined;
  readonly #rescue: (
    job: JobRow,
    now: Temporal.Instant,
    signal: AbortSignal
  ) => Promise<RuntimeJobRescue | null>;
  readonly #runs: Record<MaintenanceServiceName, number> = {
    job_cleaner: 0,
    notification_cleaner: 0,
    periodic: 0,
    queue_cleaner: 0,
    reindexer: 0,
    rescuer: 0,
    scheduler: 0,
  };
  /** Each batched service's batch size, timeout, and backoff. */
  readonly #batchers: Record<
    | "job_cleaner"
    | "notification_cleaner"
    | "queue_cleaner"
    | "rescuer"
    | "scheduler",
    MaintenanceBatcher
  >;
  #lastError: string | null = null;
  /**
   * The term the trust deadline ended. A late renewal of it resigns
   * instead of reviving it.
   */
  #expiredTerm: RuntimeLeader | null = null;
  /** Ends a follower's wait for its next election, while it waits. */
  #electionWake: (() => void) | null = null;
  #leader: RuntimeLeader | null = null;
  #leaderAbort: AbortController | null = null;
  #leaderTrustedUntil = Number.NEGATIVE_INFINITY;
  #leadershipOperations: Promise<void> = Promise.resolve();
  readonly #serviceWaiters = new Set<() => void>();

  constructor(options: RuntimeServicesOptions) {
    requireMaintenanceCapabilities(options.driver);
    this.#allowInsertNotifications =
      options.allowInsertNotifications ?? ((queues) => [...new Set(queues)]);
    this.#client = options.client;
    this.#clientId = options.clientId;
    this.#driver = options.driver;
    this.#emit = options.emit;
    this.#logger = options.logger ?? internalLogger(resolveLogger(false));
    this.#maintenance = normalizeMaintenance(options.maintenance);
    this.#now = options.now;
    this.#operations = options.operations ?? new PilotOperations();
    this.#onPeriodicJobsStart =
      options.onPeriodicJobsStart ?? (() => Promise.resolve());
    this.#periodicJobStore = options.periodicJobStore;
    this.#periodicJobs = options.periodicJobs;
    this.#random = options.random ?? Math.random;
    const batcher = (timeoutMs: number | null) =>
      new MaintenanceBatcher({ random: this.#random, timeoutMs });
    const timeoutMs = this.#maintenance.maintenanceTimeoutMs;
    this.#batchers = {
      job_cleaner: batcher(this.#maintenance.jobCleanerTimeoutMs),
      notification_cleaner: batcher(timeoutMs),
      queue_cleaner: batcher(timeoutMs),
      rescuer: batcher(timeoutMs),
      scheduler: batcher(timeoutMs),
    };
    setPeriodicJobsChangeHandler(this.#periodicJobs, () =>
      this.#wakeServiceLoops()
    );
    this.#rescue = options.rescue;
    this.#jobCleanerQueuesExcluded = Object.freeze([
      ...(options.jobCleanerQueuesExcluded ?? []),
    ]);
    this.#termServices = options.termServices ?? [];
    this.#timer = options.timer ?? SYSTEM_TIMER;
  }

  get diagnostics(): MaintenanceDiagnostics {
    return {
      isLeader: this.#leader !== null && this.#hasTrustedLeadership(),
      lastError: this.#lastError,
      leader: this.#leader,
      runs: { ...this.#runs },
    };
  }

  /**
   * Another client resigned leadership. Like River for Go's elector, a
   * follower waiting for its next election bids after a random 0 to 50 ms
   * instead of waiting out its interval.
   */
  leaderResigned(): void {
    this.#electionWake?.();
  }

  /** Resign this runtime's current exact leadership term, if it owns one. */
  resignLeadership(): Promise<boolean> {
    return this.#withLeadershipOperation(() => this.#loseLeadership(true));
  }

  async run(signal: AbortSignal): Promise<void> {
    const tasks = [
      this.#leadershipLoop(signal),
      this.#periodicLoop(signal),
      this.#serviceLoop(
        "scheduler",
        this.#maintenance.schedulerIntervalMs,
        signal,
        (now, term, termSignal) => this.#drainScheduler(now, termSignal, term)
      ),
      this.#serviceLoop(
        "rescuer",
        this.#maintenance.rescuerIntervalMs,
        signal,
        (now, term, termSignal) => this.#drainRescuer(now, termSignal, term)
      ),
      this.#serviceLoop(
        "job_cleaner",
        this.#maintenance.jobCleanerIntervalMs,
        signal,
        (now, term, termSignal) => this.#drainJobCleaner(now, termSignal, term)
      ),
      this.#serviceLoop(
        "queue_cleaner",
        this.#maintenance.queueCleanerIntervalMs,
        signal,
        (now, term, termSignal) =>
          this.#drainQueueCleaner(now, termSignal, term)
      ),
    ];
    if (this.#driver.maintenanceCleanNotifications !== undefined) {
      tasks.push(
        this.#serviceLoop(
          "notification_cleaner",
          this.#maintenance.notificationCleanerIntervalMs,
          signal,
          (now, term, termSignal) =>
            this.#drainNotificationCleaner(now, termSignal, term)
        )
      );
    }
    if (this.#driver.maintenanceReindex !== undefined) {
      tasks.push(this.#reindexerLoop(signal));
    }
    if (this.#termServices.length > 0) {
      tasks.push(this.#termServicesLoop(signal));
    }
    try {
      await Promise.all(tasks);
    } finally {
      setPeriodicJobsChangeHandler(this.#periodicJobs, undefined);
    }
  }

  async #leadershipLoop(signal: AbortSignal): Promise<void> {
    let failures = 0;
    try {
      while (!signal.aborted) {
        const now = this.#now();
        const refreshed = await this.#withLeadershipOperation(() =>
          this.#refreshLeadership(now, signal)
        );
        failures = refreshed ? 0 : failures + 1;
        const intervalMs = this.#maintenance.electionIntervalMs;
        if (failures > 0) {
          // Retry a failed election sooner, never later, than the normal
          // interval so a leader renews its lease before the TTL expires.
          await interruptibleDelay(
            Math.min(
              intervalMs,
              exponentialBackoffMs(failures, BACKGROUND_BACKOFF, this.#random)
            ),
            signal
          );
        } else if (this.#leader !== null) {
          await interruptibleDelay(intervalMs, signal);
        } else if (
          await this.#waitForElection(
            // Like River for Go's elector, a follower's interval is
            // jittered so clients started together don't bid in lockstep,
            // by up to a fifth of it (Go's 1 second over 5).
            intervalMs + Math.floor(this.#random() * (intervalMs / 5)),
            signal
          )
        ) {
          await interruptibleDelay(Math.floor(this.#random() * 50), signal);
        }
      }
    } finally {
      await this.#withLeadershipOperation(() => this.#loseLeadership(true));
    }
  }

  /** Acquire or renew leadership; false when the attempt failed. */
  async #refreshLeadership(
    now: Temporal.Instant,
    signal: AbortSignal
  ): Promise<boolean> {
    const attemptStarted = this.#timer.now();
    const ttlMs =
      this.#maintenance.electionIntervalMs +
      this.#maintenance.leaderTtlPaddingMs;
    try {
      // A stop ends the wait for a connection during an outage, but never
      // abandons an election that has started.
      const leader = await this.#driver.maintenanceLeaderAcquire?.(
        this.#clientId,
        now,
        ttlMs,
        this.#leader,
        { signal }
      );
      this.#lastError = null;
      if (leader === null || leader === undefined) {
        if (this.#leader !== null && !this.#hasTrustedLeadership()) {
          await this.#loseLeadership(false);
        }
        return true;
      }
      const expired = this.#expiredTerm;
      if (
        expired !== null &&
        expired.leaderId === leader.leaderId &&
        expired.electedAt.equals(leader.electedAt)
      ) {
        // The local deadline already ended this term. A late renewal never
        // revives it; resign so another client can lead.
        this.#expiredTerm = null;
        await this.#resign(leader);
        return true;
      }
      const newlyElected =
        this.#leader === null ||
        !this.#leader.electedAt.equals(leader.electedAt);
      if (newlyElected) this.#expiredTerm = null;
      this.#leader = leader;
      this.#leaderTrustedUntil =
        attemptStarted +
        Math.max(0, ttlMs - this.#maintenance.leaderDeadlineSafetyMs);
      if (newlyElected) {
        this.#leaderAbort?.abort(leadershipLostError());
        this.#leaderAbort = new AbortController();
      }
      this.#armTrustDeadline();
      if (newlyElected) {
        resetPeriodicJobs(this.#periodicJobs);
        this.#wakeServiceLoops();
        await this.#emit({
          at: now,
          kind: "leader_acquired",
          leader,
        });
      }
      return true;
    } catch (error: unknown) {
      if (signal.aborted) return false;
      this.#lastError = errorMessage(error);
      this.#logger.warn("River leader election failed; retrying", {
        error: this.#lastError,
        leader: this.#leader !== null,
      });
      if (this.#leader !== null && !this.#hasTrustedLeadership()) {
        await this.#loseLeadership(false);
      }
      return false;
    }
  }

  async #loseLeadership(resign: boolean): Promise<boolean> {
    const leader = this.#leader;
    if (leader === null) return false;
    this.#leader = null;
    this.#leaderAbort?.abort(leadershipLostError());
    this.#leaderAbort = null;
    this.#trustDeadline?.dispose();
    this.#trustDeadline = undefined;
    this.#leaderTrustedUntil = Number.NEGATIVE_INFINITY;
    resetPeriodicJobs(this.#periodicJobs);
    this.#wakeServiceLoops();
    const resigned = resign ? await this.#resign(leader) : false;
    await this.#emit({
      at: this.#now(),
      kind: "leader_lost",
      leader,
    });
    return resigned;
  }

  /**
   * Resign `leader`'s term like Go's elector: up to three attempts bounded
   * by one, two, and three times the resign timeout, so a stop during an
   * outage doesn't wait on the database. The lease's expiry covers a failed
   * resignation.
   */
  async #resign(leader: RuntimeLeader): Promise<boolean> {
    for (let attempt = 1; attempt <= LEADER_RESIGN_ATTEMPTS; attempt++) {
      const timeout = AbortSignal.timeout(
        attempt * this.#maintenance.leaderResignTimeoutMs
      );
      try {
        return (
          (await raceWithAbort(
            this.#driver.maintenanceLeaderResign?.(leader),
            timeout
          )) ?? false
        );
      } catch (error: unknown) {
        this.#lastError = errorMessage(error);
        this.#logger.warn("River leadership resignation failed", {
          attempt,
          error: this.#lastError,
        });
      }
    }
    return false;
  }

  /**
   * Resign `term` if this runtime still leads it, like River for Go's
   * elector honoring a local resignation request for one term.
   */
  #resignTerm(term: RuntimeLeader): Promise<boolean> {
    return this.#withLeadershipOperation(async () => {
      const leader = this.#leader;
      if (
        leader?.leaderId !== term.leaderId ||
        !leader.electedAt.equals(term.electedAt)
      ) {
        return false;
      }
      return this.#loseLeadership(true);
    });
  }

  #withLeadershipOperation<T>(callback: () => Promise<T>): Promise<T> {
    const operation = this.#leadershipOperations.then(callback, callback);
    this.#leadershipOperations = operation.then(
      () => undefined,
      () => undefined
    );
    return operation;
  }

  async #runService(
    name: MaintenanceServiceName,
    run: () => Promise<number>,
    emitEmpty = true,
    signal?: AbortSignal
  ): Promise<void> {
    try {
      const count = await run();
      this.#runs[name]++;
      this.#lastError = null;
      if (emitEmpty || count > 0) {
        await this.#emit({
          at: this.#now(),
          count,
          kind: "maintenance_succeeded",
          service: name,
        });
      }
    } catch (error: unknown) {
      if (signal?.aborted === true && Object.is(error, signal.reason)) return;
      this.#lastError = errorMessage(error);
      // Maintenance retries on its own interval, which already bounds the
      // retry rate, exactly like River's other runtimes.
      this.#logger.error("River maintenance service failed", {
        error: this.#lastError,
        service: name,
      });
      await this.#emit({
        at: this.#now(),
        error,
        kind: "maintenance_failed",
        service: name,
      });
    }
  }

  async #serviceLoop(
    name: MaintenanceServiceName,
    intervalMs: number,
    signal: AbortSignal,
    run: (
      now: Temporal.Instant,
      term: RuntimeLeader,
      signal: AbortSignal
    ) => Promise<number>
  ): Promise<void> {
    let lastRun = Number.NEGATIVE_INFINITY;
    let lastTerm = "";
    while (!signal.aborted) {
      const now = this.#now();
      const nowMs = now.epochMilliseconds;
      const term = this.#leader;
      if (term !== null && this.#ownsLeadership(term)) {
        const termKey = `${term.leaderId}\0${term.electedAt.toString()}`;
        if (termKey !== lastTerm || nowMs - lastRun >= intervalMs) {
          lastTerm = termKey;
          lastRun = nowMs;
          const termSignal = this.#termSignal(term, signal);
          await this.#runService(
            name,
            () => run(now, term, termSignal),
            true,
            termSignal
          );
        }
      }
      const elapsed = this.#now().epochMilliseconds - lastRun;
      const untilDue =
        term === null || !Number.isFinite(elapsed)
          ? this.#maintenance.electionIntervalMs
          : Math.max(1, intervalMs - elapsed);
      await this.#waitForServiceLoop(
        Math.min(untilDue, this.#maintenance.electionIntervalMs),
        signal
      );
    }
  }

  /**
   * Insert periodic jobs while this runtime leads, like Go River's periodic
   * job enqueuer: each leadership term seeds next runs from a durable store
   * (when an extension configures one), runs the start hooks, and then
   * inserts due occurrences in batches. A failed occurrence is logged and
   * dropped rather than retried, so a failing constructor or database cannot
   * wedge the schedule.
   */
  async #periodicLoop(signal: AbortSignal): Promise<void> {
    let startedTerm: RuntimeLeader | null = null;
    let durableNextRuns = new Map<string, Temporal.Instant>();
    let keepAliveDue = 0;
    let failedStarts = 0;
    let failedTerm: RuntimeLeader | null = null;
    while (!signal.aborted) {
      const term = this.#leader;
      if (term === null || !this.#ownsLeadership(term)) {
        startedTerm = null;
      } else {
        const termSignal = this.#termSignal(term, signal);
        if (
          startedTerm === null ||
          !startedTerm.electedAt.equals(term.electedAt)
        ) {
          const seeded = await this.#startPeriodicTerm(termSignal);
          if (seeded !== null) {
            startedTerm = term;
            durableNextRuns = seeded;
            keepAliveDue = 0;
            failedStarts = 0;
            failedTerm = null;
          } else if (!termSignal.aborted) {
            failedStarts =
              failedTerm?.electedAt.equals(term.electedAt) === true
                ? failedStarts + 1
                : 1;
            failedTerm = term;
            if (failedStarts < MAINTENANCE_START_ATTEMPTS) {
              await interruptibleDelay(
                exponentialBackoffMs(
                  failedStarts,
                  MAINTENANCE_START_BACKOFF,
                  this.#random
                ),
                termSignal
              );
              continue;
            }
            // Resign locally rather than through a notification, which a
            // client without notifications wouldn't hear, and only this
            // term, so a late failure can't resign a newer one.
            this.#logger.error(
              "River maintenance failed to start after all attempts; resigning leadership",
              { error: this.#lastError }
            );
            failedStarts = 0;
            failedTerm = null;
            await this.#resignTerm(term);
            continue;
          }
        }
        if (startedTerm !== null) {
          await this.#runService(
            "periodic",
            () => this.#enqueuePeriodic(term, durableNextRuns, termSignal),
            false,
            termSignal
          );
          if (performance.now() >= keepAliveDue) {
            keepAliveDue = performance.now() + PERIODIC_KEEP_ALIVE_INTERVAL_MS;
            await this.#keepPeriodicJobsAlive(termSignal);
          }
        }
      }
      const nextRunAt = nextPeriodicRunAt(this.#periodicJobs);
      const untilDue =
        startedTerm === null || nextRunAt === null
          ? this.#maintenance.electionIntervalMs
          : Math.max(
              0,
              Number(
                (nextRunAt.epochNanoseconds - this.#now().epochNanoseconds) /
                  1_000_000n
              )
            );
      await this.#waitForServiceLoop(
        Math.min(untilDue, this.#maintenance.electionIntervalMs),
        signal
      );
    }
  }

  /**
   * Begin a leadership term's periodic enqueuing: read durable next runs and
   * run `onPeriodicJobsStart` hooks. Returns null (and retries next loop)
   * when either fails, as Go River refuses to start its enqueuer.
   */
  async #startPeriodicTerm(
    signal: AbortSignal
  ): Promise<Map<string, Temporal.Instant> | null> {
    try {
      const durableJobs =
        this.#periodicJobStore === undefined
          ? []
          : await this.#periodicJobStore.getAll({ signal });
      await this.#onPeriodicJobsStart({
        durableJobs: Object.freeze([...durableJobs]),
        periodicJobs: this.#periodicJobs,
      });
      resetPeriodicJobs(this.#periodicJobs);
      return new Map(durableJobs.map(({ id, nextRunAt }) => [id, nextRunAt]));
    } catch (error: unknown) {
      if (signal.aborted) return null;
      this.#lastError = errorMessage(error);
      this.#logger.error("River periodic job enqueuer failed to start", {
        error: this.#lastError,
      });
      return null;
    }
  }

  async #enqueuePeriodic(
    term: RuntimeLeader,
    durableNextRuns: Map<string, Temporal.Instant>,
    signal: AbortSignal
  ): Promise<number> {
    const now = this.#now();
    const batch = advancePeriodicJobs(
      this.#periodicJobs,
      now,
      durableNextRuns,
      (job, error) => {
        this.#logger.error(
          "River periodic job schedule failed; the job will not run again until it is re-registered",
          {
            error: errorMessage(error),
            ...(job.id === null ? {} : { id: job.id }),
            kind: job.job.kind,
          }
        );
      }
    );
    const items: InsertManyItem[] = [];
    for (const occurrence of batch.occurrences) {
      try {
        const item = await abortable(buildPeriodicInsert(occurrence), signal);
        if (item !== null) items.push(item);
      } catch (error: unknown) {
        if (signal.aborted) throw signal.reason;
        this.#logger.error("River periodic job constructor failed", {
          error: errorMessage(error),
          ...(occurrence.job.id === null ? {} : { id: occurrence.job.id }),
          kind: occurrence.job.job.kind,
        });
      }
    }
    if (items.length === 0 && batch.durableUpdates.length === 0) return 0;
    if (!this.#ownsLeadership(term)) return 0;
    try {
      const store = this.#periodicJobStore;
      const updatedAt = this.#now();
      const upserts = batch.durableUpdates.map((update) => ({
        ...update,
        updatedAt,
      }));
      if (store !== undefined && upserts.length > 0) {
        const scope = this.#driver.operationScope?.bind(this.#driver);
        if (scope === undefined) {
          throw new ConfigurationError(
            "a periodic job store requires a driver with operation scopes"
          );
        }
        // Like River for Go, the batch's jobs (with their insert middleware
        // and hooks) and their next-run times commit together or not at all.
        await scope(undefined, async (tx) => {
          if (items.length > 0) await this.#client.insertMany(items, { tx });
          await store.upsertMany(tx, upserts);
        });
      } else if (items.length > 0) {
        await this.#client.insertMany(items);
      }
    } catch (error: unknown) {
      if (signal.aborted) throw signal.reason;
      // Like Go River, drop the batch's occurrences: their next runs have
      // already advanced, and the schedule continues. The service runner
      // logs the failure and emits `maintenance_failed`.
      throw error;
    }
    return items.length;
  }

  async #keepPeriodicJobsAlive(signal: AbortSignal): Promise<void> {
    const store = this.#periodicJobStore;
    if (store === undefined) return;
    const ids = periodicJobIds(this.#periodicJobs);
    if (ids.length === 0) return;
    try {
      await store.keepAliveAndReap(ids, { signal });
    } catch (error: unknown) {
      if (signal.aborted) return;
      this.#logger.error("River periodic job keep-alive failed", {
        error: errorMessage(error),
      });
    }
  }

  /**
   * Abort the current term's signal when the local trust deadline passes,
   * whether or not a renewal is still in flight.
   */
  #armTrustDeadline(): void {
    this.#trustDeadline?.dispose();
    const controller = this.#leaderAbort;
    const deadline = this.#timer.timeout(
      Math.max(0, this.#leaderTrustedUntil - this.#timer.now()),
      leadershipLostError
    );
    this.#trustDeadline = deadline;
    deadline.signal.addEventListener(
      "abort",
      () => {
        if (this.#trustDeadline !== deadline) return;
        this.#trustDeadline = undefined;
        if (this.#leaderAbort !== controller || controller === null) return;
        if (this.#hasTrustedLeadership()) {
          this.#armTrustDeadline();
          return;
        }
        this.#expireLeadership(controller);
      },
      { once: true }
    );
  }

  /**
   * End the current term at its trust deadline, even while a renewal is
   * in flight: its signal aborts, this client stops reporting itself as
   * leader, and `leader_lost` is emitted, like River for Go's elector.
   */
  #expireLeadership(controller: AbortController): void {
    const leader = this.#leader;
    if (leader === null) return;
    this.#expiredTerm = leader;
    this.#leader = null;
    this.#leaderAbort = null;
    this.#leaderTrustedUntil = Number.NEGATIVE_INFINITY;
    controller.abort(leadershipLostError());
    resetPeriodicJobs(this.#periodicJobs);
    this.#wakeServiceLoops();
    this.#logger.warn("River leadership expired before it could be renewed", {
      leaderId: leader.leaderId,
    });
    void this.#emit({ at: this.#now(), kind: "leader_lost", leader }).catch(
      (error: unknown) => {
        this.#logger.error("River failed to report lost leadership", {
          error: errorMessage(error),
        });
      }
    );
  }

  /**
   * Run the pilot's term services while this client leads: once per term,
   * with the term's signal, starting a new term's only after the previous
   * term's settled.
   */
  async #termServicesLoop(signal: AbortSignal): Promise<void> {
    const supervisor: SupervisorOptions = {
      logger: this.#logger,
      random: this.#random,
      timer: this.#timer,
    };
    let running: Promise<void> | undefined;
    let runningTerm: RuntimeLeader | undefined;
    try {
      while (!signal.aborted) {
        const term = this.#leader;
        const current =
          term !== null &&
          this.#ownsLeadership(term) &&
          runningTerm !== undefined &&
          runningTerm.electedAt.equals(term.electedAt);
        if (!current) {
          if (running !== undefined) {
            // Services of an ended or replaced term settle first.
            await running;
            running = undefined;
            runningTerm = undefined;
            continue;
          }
          if (term !== null && this.#ownsLeadership(term)) {
            const termSignal = this.#termSignal(term, signal);
            runningTerm = term;
            running = Promise.all(
              this.#termServices.map((service) =>
                superviseService(service, term, termSignal, supervisor)
              )
            ).then(() => undefined);
            continue;
          }
        }
        await this.#waitForServiceLoop(
          this.#maintenance.electionIntervalMs,
          signal
        );
      }
    } finally {
      await running;
    }
  }

  #ownsLeadership(term: RuntimeLeader): boolean {
    return (
      this.#hasTrustedLeadership() &&
      this.#leader?.leaderId === term.leaderId &&
      this.#leader.electedAt.equals(term.electedAt)
    );
  }

  #hasTrustedLeadership(): boolean {
    return this.#timer.now() < this.#leaderTrustedUntil;
  }

  #termSignal(term: RuntimeLeader, runSignal: AbortSignal): AbortSignal {
    const controller = this.#leaderAbort;
    if (!this.#ownsLeadership(term) || controller === null) {
      return AbortSignal.abort(leadershipLostError());
    }
    // A few per service interval, so `AbortSignal.any` stays cheap, and the
    // signal still aborts with the term after the service run returns.
    // eslint-disable-next-line no-restricted-properties
    return AbortSignal.any([runSignal, controller.signal]);
  }

  /**
   * Wait for a follower's next election, resolving true when another
   * client's resignation ended the wait early.
   */
  #waitForElection(
    milliseconds: number,
    signal: AbortSignal
  ): Promise<boolean> {
    if (signal.aborted) return Promise.resolve(false);
    return new Promise((resolve) => {
      const finish = (woken: boolean): void => {
        cancel();
        signal.removeEventListener("abort", onAbort);
        if (this.#electionWake === wake) this.#electionWake = null;
        resolve(woken);
      };
      const wake = (): void => {
        finish(true);
      };
      const onAbort = (): void => {
        finish(false);
      };
      const cancel = unrefTimeout(() => {
        finish(false);
      }, milliseconds);
      this.#electionWake = wake;
      signal.addEventListener("abort", onAbort, { once: true });
    });
  }

  #wakeServiceLoops(): void {
    for (const wake of [...this.#serviceWaiters]) wake();
  }

  #waitForServiceLoop(
    milliseconds: number,
    signal: AbortSignal
  ): Promise<void> {
    if (signal.aborted) return Promise.resolve();
    return new Promise((resolve) => {
      const cancel = unrefTimeout(done, milliseconds);
      const onAbort = () => done();
      const waiters = this.#serviceWaiters;
      waiters.add(done);
      signal.addEventListener("abort", onAbort, { once: true });
      function done() {
        cancel();
        signal.removeEventListener("abort", onAbort);
        waiters.delete(done);
        resolve();
      }
    });
  }

  async #drainScheduler(
    now: Temporal.Instant,
    signal: AbortSignal,
    term: RuntimeLeader
  ) {
    const scheduledAtHorizon = now.add({
      milliseconds: this.#maintenance.schedulerIntervalMs,
    });
    const notificationHorizon = now.add({
      milliseconds: SCHEDULER_NOTIFICATION_LOOKAHEAD_MS,
    });
    const batcher = this.#batchers.scheduler;
    let total = 0;
    while (!signal.aborted && this.#ownsLeadership(term)) {
      const limit = batcher.batchSize;
      const count = await batcher.run(
        signal,
        async (batch) =>
          (await this.#driver.maintenanceSchedule?.(
            term,
            {
              allowInsertNotifications: this.#allowInsertNotifications,
              limit,
              notificationHorizon,
              now,
              scheduledAtHorizon,
            },
            batch
          )) ?? 0
      );
      total += count;
      if (count < limit) break;
      await batcher.backoff(signal);
    }
    return total;
  }

  async #drainRescuer(
    now: Temporal.Instant,
    signal: AbortSignal,
    term: RuntimeLeader
  ) {
    const attemptedBefore = subtractMilliseconds(
      now,
      this.#maintenance.rescueAfterMs
    );
    const batcher = this.#batchers.rescuer;
    let afterId = 0n;
    let total = 0;
    while (!signal.aborted && this.#ownsLeadership(term)) {
      const limit = batcher.batchSize;
      // Like Go, the timeout bounds reading stuck jobs, not rescuing them.
      const jobs = await batcher.run(signal, (batch) =>
        this.#operations.getStuck(
          this.#driver,
          term,
          attemptedBefore,
          afterId,
          limit,
          batch
        )
      );
      if (jobs.length === 0) break;
      const decisions = await mapConcurrentOrdered(
        jobs,
        RESCUE_DECISION_CONCURRENCY,
        signal,
        (job) => this.#rescue(job, now, signal)
      );
      const rescues = decisions.filter(
        (decision): decision is RuntimeJobRescue => decision !== null
      );
      if (rescues.length > 0) {
        total += await this.#operations.rescue(
          this.#driver,
          term,
          attemptedBefore,
          rescues,
          signal
        );
      }
      afterId = jobs.at(-1)?.id ?? afterId;
      if (jobs.length < limit) break;
      await batcher.backoff(signal);
    }
    return total;
  }

  async #drainJobCleaner(
    now: Temporal.Instant,
    signal: AbortSignal,
    term: RuntimeLeader
  ) {
    const batcher = this.#batchers.job_cleaner;
    const horizons = {
      cancelledBefore: horizon(now, this.#maintenance.cancelledJobRetentionMs),
      completedBefore: horizon(now, this.#maintenance.completedJobRetentionMs),
      discardedBefore: horizon(now, this.#maintenance.discardedJobRetentionMs),
    };
    let total = 0;
    while (!signal.aborted && this.#ownsLeadership(term)) {
      const params: RuntimeJobCleanupParams = {
        ...horizons,
        limit: batcher.batchSize,
        ...(this.#jobCleanerQueuesExcluded.length === 0
          ? {}
          : { queuesExcluded: this.#jobCleanerQueuesExcluded }),
      };
      const count = await batcher.run(
        signal,
        async (batch) =>
          (await this.#driver.maintenanceCleanJobs?.(
            term,
            params,
            batch.timeoutMs,
            batch.signal
          )) ?? 0
      );
      total += count;
      if (count < params.limit) break;
      await batcher.backoff(signal);
    }
    return total;
  }

  async #drainQueueCleaner(
    now: Temporal.Instant,
    signal: AbortSignal,
    term: RuntimeLeader
  ) {
    const horizon = subtractMilliseconds(
      now,
      this.#maintenance.queueRetentionMs
    );
    const batcher = this.#batchers.queue_cleaner;
    let total = 0;
    while (!signal.aborted && this.#ownsLeadership(term)) {
      const limit = batcher.batchSize;
      const count = await batcher.run(
        signal,
        async (batch) =>
          (await this.#driver.maintenanceCleanQueues?.(
            term,
            horizon,
            limit,
            batch
          )) ?? 0
      );
      total += count;
      if (count < limit) break;
      await batcher.backoff(signal);
    }
    return total;
  }

  async #drainNotificationCleaner(
    now: Temporal.Instant,
    signal: AbortSignal,
    term: RuntimeLeader
  ) {
    const horizon = subtractMilliseconds(
      now,
      this.#maintenance.notificationRetentionMs
    );
    const batcher = this.#batchers.notification_cleaner;
    let total = 0;
    while (!signal.aborted && this.#ownsLeadership(term)) {
      const limit = batcher.batchSize;
      const count = await batcher.run(
        signal,
        async (batch) =>
          (await this.#driver.maintenanceCleanNotifications?.(
            term,
            horizon,
            limit,
            batch
          )) ?? 0
      );
      total += count;
      if (count < limit) break;
      await batcher.backoff(signal);
    }
    return total;
  }

  async #reindexerLoop(signal: AbortSignal): Promise<void> {
    let nextRunAt: Temporal.Instant | null = null;
    let termKey = "";
    while (!signal.aborted) {
      const now = this.#now();
      const term = this.#leader;
      if (term === null) {
        nextRunAt = null;
        termKey = "";
      } else {
        const currentTermKey = `${term.leaderId}\0${term.electedAt.toString()}`;
        if (currentTermKey !== termKey || nextRunAt === null) {
          termKey = currentTermKey;
          nextRunAt = nextReindexAt(this.#maintenance.reindexerSchedule, now);
        }
        if (Temporal.Instant.compare(now, nextRunAt) >= 0) {
          const scheduledAt = nextRunAt;
          const termSignal = this.#termSignal(term, signal);
          await this.#runService(
            "reindexer",
            async () => {
              if (!this.#ownsLeadership(term)) return 0;
              return (
                (await this.#driver.maintenanceReindex?.(
                  term,
                  this.#maintenance.reindexerIndexNames,
                  this.#maintenance.reindexerTimeoutMs,
                  termSignal
                )) ?? 0
              );
            },
            true,
            termSignal
          );
          nextRunAt = nextReindexAt(
            this.#maintenance.reindexerSchedule,
            scheduledAt
          );
        }
      }
      const waitMs =
        nextRunAt === null
          ? this.#maintenance.electionIntervalMs
          : Math.max(
              1,
              Math.min(
                this.#maintenance.electionIntervalMs,
                Number(
                  (nextRunAt.epochNanoseconds - this.#now().epochNanoseconds) /
                    1_000_000n
                )
              )
            );
      await this.#waitForServiceLoop(waitMs, signal);
    }
  }
}

/** @internal Whether a backend supplies the complete common maintenance SPI. */
export function supportsMaintenance(driver: RuntimeDriver): boolean {
  return (
    driver.maintenanceLeaderAcquire !== undefined &&
    driver.maintenanceLeaderResign !== undefined &&
    driver.maintenanceSchedule !== undefined &&
    driver.maintenanceGetStuck !== undefined &&
    driver.maintenanceRescue !== undefined &&
    driver.maintenanceCleanJobs !== undefined &&
    driver.maintenanceCleanQueues !== undefined
  );
}

function requireMaintenanceCapabilities(driver: RuntimeDriver): void {
  const required = [
    "maintenanceLeaderAcquire",
    "maintenanceLeaderResign",
    "maintenanceSchedule",
    "maintenanceGetStuck",
    "maintenanceRescue",
    "maintenanceCleanJobs",
    "maintenanceCleanQueues",
  ] as const;
  const missing = required.filter((name) => driver[name] === undefined);
  if (missing.length > 0) {
    throw new ConfigurationError(
      `runtime backend lacks maintenance capabilities: ${missing.join(", ")}`
    );
  }
}

function normalizeMaintenance(
  options: MaintenanceSettings
): NormalizedMaintenanceOptions {
  return {
    cancelledJobRetentionMs: optionalDuration(
      "cancelledJobRetentionMs",
      options.cancelledJobRetentionMs,
      86_400_000
    ),
    completedJobRetentionMs: optionalDuration(
      "completedJobRetentionMs",
      options.completedJobRetentionMs,
      86_400_000
    ),
    discardedJobRetentionMs: optionalDuration(
      "discardedJobRetentionMs",
      options.discardedJobRetentionMs,
      604_800_000
    ),
    electionIntervalMs: duration(
      "electionIntervalMs",
      options.electionIntervalMs,
      5_000
    ),
    jobCleanerIntervalMs: duration(
      "jobCleanerIntervalMs",
      options.jobCleanerIntervalMs,
      30_000
    ),
    jobCleanerTimeoutMs: optionalDuration(
      "jobCleanerTimeoutMs",
      options.jobCleanerTimeoutMs,
      30_000
    ),
    leaderDeadlineSafetyMs: duration(
      "leaderDeadlineSafetyMs",
      options.leaderDeadlineSafetyMs,
      LEADER_LOCAL_DEADLINE_SAFETY_MS
    ),
    leaderResignTimeoutMs: duration(
      "leaderResignTimeoutMs",
      options.leaderResignTimeoutMs,
      1_000
    ),
    leaderTtlPaddingMs: duration(
      "leaderTtlPaddingMs",
      options.leaderTtlPaddingMs,
      LEADER_TTL_PADDING_MS
    ),
    maintenanceTimeoutMs: duration(
      "maintenanceTimeoutMs",
      options.maintenanceTimeoutMs,
      MAINTENANCE_TIMEOUT_DEFAULT_MS
    ),
    notificationCleanerIntervalMs: duration(
      "notificationCleanerIntervalMs",
      options.notificationCleanerIntervalMs,
      60_000
    ),
    notificationRetentionMs: duration(
      "notificationRetentionMs",
      options.notificationRetentionMs,
      300_000
    ),
    queueCleanerIntervalMs: duration(
      "queueCleanerIntervalMs",
      options.queueCleanerIntervalMs,
      3_600_000
    ),
    queueRetentionMs: duration(
      "queueRetentionMs",
      options.queueRetentionMs,
      86_400_000
    ),
    reindexerIndexNames: Object.freeze(
      [...(options.reindexerIndexNames ?? REINDEXER_INDEX_NAMES_DEFAULT)].map(
        (name) => {
          if (typeof name !== "string" || name.length === 0) {
            throw new ConfigurationError(
              "reindexerIndexNames must contain nonempty strings"
            );
          }
          return name;
        }
      )
    ),
    reindexerSchedule: options.reindexerSchedule ?? nextMidnightUtc,
    reindexerTimeoutMs: optionalDuration(
      "reindexerTimeoutMs",
      options.reindexerTimeoutMs,
      60_000
    ),
    rescueAfterMs: duration("rescueAfterMs", options.rescueAfterMs, 3_600_000),
    rescuerIntervalMs: duration(
      "rescuerIntervalMs",
      options.rescuerIntervalMs,
      30_000
    ),
    schedulerIntervalMs: duration(
      "schedulerIntervalMs",
      options.schedulerIntervalMs,
      5_000
    ),
  };
}

function duration(name: string, value: number | undefined, fallback: number) {
  const selected = value ?? fallback;
  if (!Number.isSafeInteger(selected) || selected < 1) {
    throw new ConfigurationError(`${name} must be a positive safe integer`);
  }
  return selected;
}

function optionalDuration(
  name: string,
  value: number | null | undefined,
  fallback: number
) {
  return value === null ? null : duration(name, value, fallback);
}

function horizon(now: Temporal.Instant, retentionMs: number | null) {
  return retentionMs === null ? null : subtractMilliseconds(now, retentionMs);
}

function subtractMilliseconds(now: Temporal.Instant, milliseconds: number) {
  return now.subtract({ milliseconds });
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function leadershipLostError(): Error {
  return new Error("River leadership term lost");
}

async function mapConcurrentOrdered<T, U>(
  items: readonly T[],
  concurrency: number,
  signal: AbortSignal,
  map: (item: T, index: number) => Promise<U>
): Promise<readonly U[]> {
  const results = new Array<U>(items.length);
  let nextIndex = 0;
  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    async () => {
      while (true) {
        signal.throwIfAborted();
        const index = nextIndex++;
        if (index >= items.length) return;
        results[index] = await map(items[index] as T, index);
      }
    }
  );
  await Promise.all(workers);
  return results;
}

function nextMidnightUtc(after: Temporal.Instant): Temporal.Instant {
  return after
    .toZonedDateTimeISO("UTC")
    .startOfDay()
    .add({ days: 1 })
    .toInstant();
}

function nextReindexAt(
  schedule: ReindexerSchedule,
  after: Temporal.Instant
): Temporal.Instant {
  const next = schedule(after);
  if (
    !(next instanceof Temporal.Instant) ||
    Temporal.Instant.compare(next, after) <= 0
  ) {
    throw new ConfigurationError(
      "reindexerSchedule must return a Temporal.Instant after its input"
    );
  }
  return next;
}

function abortable<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    operation.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      }
    );
  });
}
