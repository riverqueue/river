import type { InsertManyItem } from "./client.js";
import { ConfigurationError, ValidationError } from "./errors.js";
import { isUserSpecifiedIdOrKind } from "./identifiers.js";
import type { InsertOptions } from "./insert-options.js";
import type { JobDefinition, JobDefinitionInput } from "./job-definition.js";
import { isJobDefinition } from "./job-definition.js";
import type { JsonObject } from "./json.js";
import {
  durationNanoseconds,
  toDuration,
  type DurationInput,
} from "./internal/duration.js";

/**
 * When a periodic job runs: `next(after)` returns the first occurrence
 * strictly after `after`, or null to stop scheduling it.
 *
 * `cron()` builds one from a River Go-compatible cron expression; implement
 * it directly for other calendar rules.
 */
export interface PeriodicSchedule {
  next(after: Temporal.Instant): Temporal.Instant | null;
}

/** One occurrence built by a periodic job's `construct` callback. */
export interface PeriodicJobInsert<Definition extends JobDefinition> {
  readonly args: JobDefinitionInput<Definition>;
  readonly options?: InsertOptions;
}

/** Static arguments, or a callback building each occurrence. */
export type PeriodicJobArgs<Definition extends JobDefinition> =
  | {
      /** Arguments inserted on every occurrence. */
      readonly args: JobDefinitionInput<Definition>;
      readonly construct?: never;
      /** Insertion options for every occurrence. */
      readonly options?: InsertOptions;
    }
  | {
      readonly args?: never;
      /**
       * Build each occurrence's args and options, or return null to skip it.
       * A thrown error is logged and that occurrence is skipped.
       */
      readonly construct: () =>
        | PeriodicJobInsert<Definition>
        | null
        | PromiseLike<PeriodicJobInsert<Definition> | null>;
      readonly options?: never;
    };

/** A fixed interval or a custom schedule. */
export type PeriodicJobTiming =
  | {
      /**
       * Fixed interval between occurrences, such as `{ hours: 1 }`. Calendar
       * units (years, months, weeks) are rejected; a day is 24 hours.
       */
      readonly every: DurationInput;
      readonly schedule?: never;
    }
  | {
      readonly every?: never;
      /** Custom schedule, such as one built by `cron()`. */
      readonly schedule: PeriodicSchedule;
    };

/** Options for {@link periodicJob}. */
export type PeriodicJobOptions<Definition extends JobDefinition> = {
  /**
   * Stable ID, unique within a client. It is recorded in the inserted job's
   * `river:periodic_job_id` metadata and lets a durable schedule store (an
   * extension) remember the next run across leader changes.
   */
  readonly id?: string;
  /** Job definition inserted on each occurrence. */
  readonly job: Definition;
  /** Also insert once each time this client becomes leader. */
  readonly runOnStart?: boolean;
} & PeriodicJobArgs<Definition> &
  PeriodicJobTiming;

declare const periodicJobBrand: unique symbol;

/** An immutable periodic job created by {@link periodicJob}. */
export interface PeriodicJob<Definition extends JobDefinition = JobDefinition> {
  /** Stable ID, or null when none was configured. */
  readonly id: string | null;
  /** Job definition inserted on each occurrence. */
  readonly job: Definition;
  /** Whether an occurrence is also inserted when leadership starts. */
  readonly runOnStart: boolean;
  /** When occurrences are due. */
  readonly schedule: PeriodicSchedule;
  /** Type-only brand; periodic jobs come from {@link periodicJob}. */
  readonly [periodicJobBrand]?: true;
}

/**
 * Define a job that the elected leader inserts on a schedule.
 *
 * Periodic jobs run only on the client that currently holds River
 * leadership. In a fleet mixing River implementations (Go, Rust, and
 * JavaScript), register the same periodic jobs in every implementation, or
 * leadership moving between languages silently changes which periodic jobs
 * run.
 *
 * @example
 * ```ts
 * const hourlyReport = periodicJob({
 *   args: { scope: "all" },
 *   every: { hours: 1 },
 *   id: "hourly_report",
 *   job: buildReport,
 *   runOnStart: true,
 * });
 * const client = new Client(driver, { periodicJobs: [hourlyReport], workers });
 * ```
 */
export function periodicJob<Definition extends JobDefinition>(
  options: PeriodicJobOptions<Definition>
): PeriodicJob<Definition> {
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
  if (options === null || typeof options !== "object") {
    throw new ConfigurationError("periodic job options must be an object");
  }
  if (!isJobDefinition(options.job)) {
    throw new ConfigurationError(
      "periodic job requires a job definition created by defineJob"
    );
  }
  if (options.id !== undefined) validatePeriodicJobId(options.id);
  if (
    options.runOnStart !== undefined &&
    typeof options.runOnStart !== "boolean"
  ) {
    throw new ConfigurationError("periodic job runOnStart must be a boolean");
  }

  const schedule = resolveSchedule(options);
  const construct = resolveConstruct(options);
  const job: PeriodicJob<Definition> = Object.freeze({
    id: options.id ?? null,
    job: options.job,
    runOnStart: options.runOnStart ?? false,
    schedule,
  });
  periodicConstructors.set(job, construct);
  return job;
}

/** Opaque removal handle returned by {@link PeriodicJobs.add}. */
export interface PeriodicJobHandle {
  readonly "~periodicJobHandle": number;
}

/** A durable periodic job record reported by a periodic job store. */
export interface DurablePeriodicJob {
  readonly createdAt: Temporal.Instant;
  readonly id: string;
  readonly nextRunAt: Temporal.Instant;
  readonly updatedAt: Temporal.Instant;
}

/** Parameters for an `onPeriodicJobsStart` hook. */
export interface PeriodicJobsStartParams {
  /**
   * Durable periodic job records found by a configured periodic job store,
   * including records for jobs that were removed but not yet reaped. Empty
   * unless an extension provides a store.
   */
  readonly durableJobs: readonly DurablePeriodicJob[];
  /** The client's periodic job registry, which the hook may modify. */
  readonly periodicJobs: PeriodicJobs;
}

interface PeriodicEntry {
  readonly handle: number;
  initialized: boolean;
  readonly job: PeriodicJob;
  nextRun: Temporal.Instant | null;
}

/** @internal One occurrence due for insertion. */
export interface DuePeriodicOccurrence {
  readonly job: PeriodicJob;
  readonly scheduledAt: Temporal.Instant;
}

/** @internal Result of advancing the registry to `now`. */
export interface DuePeriodicBatch {
  /** Durable next-run times to persist with this batch's insertions. */
  readonly durableUpdates: readonly {
    readonly id: string;
    readonly nextRunAt: Temporal.Instant;
  }[];
  readonly occurrences: readonly DuePeriodicOccurrence[];
}

interface RegistryInternals {
  readonly entries: Map<number, PeriodicEntry>;
  disable(): void;
  setChangeHandler(change: (() => void) | undefined): void;
}

/** Go River's margin for inserting occurrences due in the very near future. */
const DUE_MARGIN = { milliseconds: 100 } as const;

/**
 * @internal Insert options whose `scheduledAt` is a periodic occurrence time
 * rather than a caller's schedule. Like Go River's periodic enqueuer, which
 * sets the occurrence time only after the insert state is resolved, such a
 * job is inserted `available` even when the occurrence is due within
 * {@link DUE_MARGIN} of now, instead of `scheduled` until the scheduler runs.
 */
export const periodicOccurrenceOptions = new WeakSet<InsertOptions>();

const periodicConstructors = new WeakMap<
  PeriodicJob,
  () => PromiseLike<PeriodicJobInsert<JobDefinition> | null>
>();

let registryInternals: (registry: PeriodicJobs) => RegistryInternals;

/**
 * A client's mutable registry of periodic jobs, available as
 * `client.periodicJobs`. Jobs may be added and removed while the client runs;
 * the leader picks up changes immediately.
 *
 * Only the elected leader inserts periodic jobs, so a change takes full effect
 * only when it's made on every client in the fleet that may lead. The registry
 * of a client configured with `leaderElectionDisabled: true` can't be
 * modified, because that client never leads.
 */
export class PeriodicJobs {
  #change: (() => void) | undefined;
  #disabled = false;
  readonly #entries = new Map<number, PeriodicEntry>();
  #nextHandle = 1;

  static {
    registryInternals = (registry) => ({
      disable: () => {
        registry.#disabled = true;
      },
      entries: registry.#entries,
      setChangeHandler: (change) => {
        registry.#change = change;
      },
    });
  }

  constructor(jobs: readonly PeriodicJob[] = []) {
    this.addMany(jobs);
  }

  /** Number of registered periodic jobs. */
  get size(): number {
    return this.#entries.size;
  }

  /** Register a periodic job and return a handle for removing it. */
  add(job: PeriodicJob): PeriodicJobHandle {
    const [handle] = this.addMany([job]);
    if (handle === undefined) throw new Error("periodic job was not added");
    return handle;
  }

  /** Register several periodic jobs at once. */
  addMany(jobs: readonly PeriodicJob[]): readonly PeriodicJobHandle[] {
    this.#requireModifiable();
    this.#validate(jobs);
    const handles = jobs.map((job) => {
      const value = this.#nextHandle++;
      this.#entries.set(value, {
        handle: value,
        initialized: false,
        job,
        nextRun: null,
      });
      return Object.freeze({ "~periodicJobHandle": value });
    });
    if (handles.length > 0) this.#change?.();
    return Object.freeze(handles);
  }

  /** Remove every periodic job. */
  clear(): void {
    this.#requireModifiable();
    this.#entries.clear();
    this.#change?.();
  }

  /** Remove the job registered with `handle`; false when already removed. */
  remove(handle: PeriodicJobHandle): boolean {
    this.#requireModifiable();
    const removed = this.#entries.delete(handle["~periodicJobHandle"]);
    if (removed) this.#change?.();
    return removed;
  }

  /** Remove the job registered with `id`; false when none matches. */
  removeById(id: string): boolean {
    this.#requireModifiable();
    for (const [handle, entry] of this.#entries) {
      if (entry.job.id === id) {
        this.#entries.delete(handle);
        this.#change?.();
        return true;
      }
    }
    return false;
  }

  #requireModifiable(): void {
    if (this.#disabled) {
      throw new ConfigurationError(
        "cannot modify periodic jobs when leaderElectionDisabled is true, because this client never leads"
      );
    }
  }

  #validate(jobs: readonly PeriodicJob[]): void {
    const ids = new Set(
      [...this.#entries.values()]
        .map(({ job }) => job.id)
        .filter((id): id is string => id !== null)
    );
    for (const job of jobs) {
      if (!periodicConstructors.has(job)) {
        throw new ValidationError(
          "periodic jobs must be created with periodicJob()"
        );
      }
      if (job.id !== null) {
        if (ids.has(job.id)) {
          throw new ValidationError(`duplicate periodic job id ${job.id}`);
        }
        ids.add(job.id);
      }
    }
  }
}

/** @internal Reject changes to the registry of a client that never leads. */
export function disablePeriodicJobs(registry: PeriodicJobs): void {
  registryInternals(registry).disable();
}

/** @internal Wake the leader's enqueuer when registry membership changes. */
export function setPeriodicJobsChangeHandler(
  registry: PeriodicJobs,
  change: (() => void) | undefined
): void {
  registryInternals(registry).setChangeHandler(change);
}

/** @internal Forget all scheduling state when leadership starts or ends. */
export function resetPeriodicJobs(registry: PeriodicJobs): void {
  for (const entry of registryInternals(registry).entries.values()) {
    entry.initialized = false;
    entry.nextRun = null;
  }
}

/** @internal IDs of registered periodic jobs, for durable keep-alive. */
export function periodicJobIds(registry: PeriodicJobs): readonly string[] {
  return [...registryInternals(registry).entries.values()].flatMap(({ job }) =>
    job.id === null ? [] : [job.id]
  );
}

/** @internal Earliest scheduled occurrence, or null when none is scheduled. */
export function nextPeriodicRunAt(
  registry: PeriodicJobs
): Temporal.Instant | null {
  let earliest: Temporal.Instant | null = null;
  for (const { nextRun } of registryInternals(registry).entries.values()) {
    if (
      nextRun !== null &&
      (earliest === null || Temporal.Instant.compare(nextRun, earliest) < 0)
    ) {
      earliest = nextRun;
    }
  }
  return earliest;
}

/** @internal Whether a newly registered job still needs initialization. */
export function hasUninitializedPeriodicJobs(registry: PeriodicJobs): boolean {
  for (const entry of registryInternals(registry).entries.values()) {
    if (!entry.initialized) return true;
  }
  return false;
}

/**
 * @internal Advance every registered job to `now`, like Go River's periodic
 * job enqueuer: newly registered jobs get their first run (seeded from
 * `durableNextRuns` when their ID has one) and a run-on-start occurrence;
 * jobs due within a small margin produce one occurrence and advance from
 * their scheduled time. Occurrences advance whether or not their insertion
 * later succeeds.
 */
export function advancePeriodicJobs(
  registry: PeriodicJobs,
  now: Temporal.Instant,
  durableNextRuns: Map<string, Temporal.Instant>,
  onScheduleError: (job: PeriodicJob, error: unknown) => void
): DuePeriodicBatch {
  const occurrences: DuePeriodicOccurrence[] = [];
  const durableUpdates: { id: string; nextRunAt: Temporal.Instant }[] = [];
  const dueBefore = now.add(DUE_MARGIN);
  const entries = [...registryInternals(registry).entries.values()].sort(
    (left, right) => left.handle - right.handle
  );
  for (const entry of entries) {
    const id = entry.job.id;
    if (!entry.initialized) {
      entry.initialized = true;
      const seeded = id === null ? undefined : durableNextRuns.get(id);
      if (id !== null) durableNextRuns.delete(id);
      entry.nextRun =
        seeded ?? safeNextOccurrence(entry.job, now, onScheduleError);
      if (id !== null && entry.nextRun !== null) {
        durableUpdates.push({ id, nextRunAt: entry.nextRun });
      }
      if (entry.job.runOnStart) {
        occurrences.push({ job: entry.job, scheduledAt: now });
      }
      continue;
    }
    if (
      entry.nextRun === null ||
      Temporal.Instant.compare(entry.nextRun, dueBefore) >= 0
    ) {
      continue;
    }
    occurrences.push({ job: entry.job, scheduledAt: entry.nextRun });
    entry.nextRun = safeNextOccurrence(
      entry.job,
      entry.nextRun,
      onScheduleError
    );
    if (id !== null && entry.nextRun !== null) {
      durableUpdates.push({ id, nextRunAt: entry.nextRun });
    }
  }
  return { durableUpdates, occurrences };
}

/**
 * @internal Build the insertion for one occurrence, or null when its
 * constructor skipped it. Constructor errors propagate to the caller, which
 * logs them.
 */
export async function buildPeriodicInsert(
  occurrence: DuePeriodicOccurrence
): Promise<InsertManyItem | null> {
  const construct = periodicConstructors.get(occurrence.job);
  if (construct === undefined) throw new Error("unknown periodic job");
  const insert = await construct();
  if (insert === null) return null;
  const options = insert.options ?? {};
  const metadata: JsonObject = {
    ...(options.metadata ?? {}),
    periodic: true,
    ...(occurrence.job.id === null
      ? {}
      : { "river:periodic_job_id": occurrence.job.id }),
  };
  const scheduledByOccurrence =
    options.scheduledAt === undefined && options.delay === undefined;
  const periodicOptions: InsertOptions = {
    ...options,
    metadata,
    ...(scheduledByOccurrence ? { scheduledAt: occurrence.scheduledAt } : {}),
  };
  if (scheduledByOccurrence) periodicOccurrenceOptions.add(periodicOptions);
  return {
    args: insert.args as JsonObject,
    job: occurrence.job.job as JobDefinition<JsonObject>,
    options: periodicOptions,
  };
}

function resolveSchedule(timing: PeriodicJobTiming): PeriodicSchedule {
  const hasEvery = timing.every !== undefined;
  const hasSchedule = timing.schedule !== undefined;
  if (hasEvery === hasSchedule) {
    throw new ConfigurationError(
      "periodic job requires exactly one of every or schedule"
    );
  }
  if (timing.schedule !== undefined) {
    const schedule = timing.schedule;
    if (typeof schedule.next !== "function") {
      throw new ConfigurationError("periodic schedule must implement next()");
    }
    return schedule;
  }
  return everySchedule(timing.every);
}

function resolveConstruct<Definition extends JobDefinition>(
  options: PeriodicJobArgs<Definition>
): () => PromiseLike<PeriodicJobInsert<JobDefinition> | null> {
  const hasArgs = options.args !== undefined;
  const hasConstruct = options.construct !== undefined;
  if (hasArgs === hasConstruct) {
    throw new ConfigurationError(
      "periodic job requires exactly one of args or construct"
    );
  }
  if (options.construct !== undefined) {
    const construct = options.construct;
    if (typeof construct !== "function") {
      throw new ConfigurationError("periodic job construct must be a function");
    }
    return async () => await construct();
  }
  const insert = Object.freeze({
    args: options.args,
    ...(options.options === undefined ? {} : { options: options.options }),
  }) as PeriodicJobInsert<JobDefinition>;
  return () => Promise.resolve(insert);
}

function everySchedule(interval: DurationInput): PeriodicSchedule {
  const nanoseconds = durationNanoseconds(
    toDuration("periodic job every", interval)
  );
  return Object.freeze({
    next: (after: Temporal.Instant) =>
      Temporal.Instant.fromEpochNanoseconds(
        after.epochNanoseconds + nanoseconds
      ),
  });
}

/** A schedule that throws stops scheduling its job instead of spinning. */
function safeNextOccurrence(
  job: PeriodicJob,
  after: Temporal.Instant,
  onScheduleError: (job: PeriodicJob, error: unknown) => void
): Temporal.Instant | null {
  try {
    return nextOccurrence(job.schedule, after);
  } catch (error: unknown) {
    onScheduleError(job, error);
    return null;
  }
}

function nextOccurrence(
  schedule: PeriodicSchedule,
  after: Temporal.Instant
): Temporal.Instant | null {
  const next = schedule.next(after);
  if (next === null) return null;
  if (
    !(next instanceof Temporal.Instant) ||
    Temporal.Instant.compare(next, after) <= 0
  ) {
    throw new ValidationError(
      "periodic schedule next() must return null or an instant after its input"
    );
  }
  return next;
}

function validatePeriodicJobId(id: string): void {
  if (
    typeof id !== "string" ||
    id.length >= 128 ||
    !isUserSpecifiedIdOrKind(id)
  ) {
    throw new ConfigurationError(
      "periodic job id must be 2 to 127 characters, start with a letter, number, or underscore, and contain only letters, numbers, and _-[]<>/.·:+"
    );
  }
}
