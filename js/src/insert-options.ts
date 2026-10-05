import { ValidationError } from "./errors.js";
import { durationNanoseconds, toDuration } from "./internal/duration.js";
import type { DurationInput } from "./internal/duration.js";
import { validateQueueName } from "./identifiers.js";
import { JOB_STATE } from "./job.js";
import type { JobState } from "./job.js";
import type { JsonObject } from "./json.js";
import { deepFreezeJson, toJsonObject } from "./json.js";

const TAG_RE = /^\w[\w-]+\w$/;
const MAX_GO_DURATION_NANOSECONDS = 9_223_372_036_854_775_807n;
const NANOSECONDS_PER_SECOND = 1_000_000_000n;

const REQUIRED_UNIQUE_STATES: readonly JobState[] = Object.freeze([
  JOB_STATE.available,
  JOB_STATE.pending,
  JOB_STATE.running,
  JOB_STATE.scheduled,
]);

/** Options that may be supplied at any insertion-default level. */
export interface InsertOptions {
  /**
   * Delay from insertion time before the job becomes eligible to run, such as
   * `{ minutes: 5 }`. Mutually exclusive with `scheduledAt` at the same level;
   * a call-site `delay` overrides a definition-level `scheduledAt` and vice
   * versa. Calendar units (years, months, weeks) are rejected; a day is 24
   * hours.
   */
  delay?: DurationInput;

  /** Maximum total attempts, including the first attempt. */
  maxAttempts?: number;

  /** Application metadata stored with the job. */
  metadata?: JsonObject;

  /** Insert the job as pending rather than immediately available. */
  pending?: boolean;

  /** Priority from 1 (highest) through 4 (lowest). */
  priority?: number;

  /** Queue on which the job will be worked. */
  queue?: string;

  /**
   * Absolute time at which the job becomes eligible to run. A `Date` is
   * converted to a `Temporal.Instant`; persisted rows always use `Instant`.
   */
  scheduledAt?: Date | Temporal.Instant;

  /** Tags used to group and query jobs. */
  tags?: readonly string[];

  /** Dimensions used to deduplicate jobs. */
  unique?: UniqueOptions;
}

/** Dimensions used to deduplicate a job. */
export interface UniqueOptions {
  /** Include all args or the named fields, using dot-separated nested paths. */
  byArgs?: true | readonly string[];

  /**
   * Deduplicate within fixed windows of this length, such as `{ hours: 1 }`,
   * aligned like Go River's by-period uniqueness. At least one second;
   * calendar units (years, months, weeks) are rejected and a day is 24 hours.
   */
  byPeriod?: DurationInput;

  /** Include the queue in the unique key. */
  byQueue?: boolean;

  /** States in which an existing job conflicts with an insertion. */
  byState?: readonly JobState[];

  /**
   * Omit kind from the unique key, deduplicating across all jobs regardless
   * of kind. Requires `byArgs`, `byQueue`, or `byPeriod`.
   */
  excludeKind?: boolean;
}

/** Snapshot of insertion options after validation and normalization. */
export interface NormalizedInsertOptions extends Omit<
  InsertOptions,
  "delay" | "scheduledAt" | "unique"
> {
  delay?: Temporal.Duration;
  scheduledAt?: Temporal.Instant;
  unique?: NormalizedUniqueOptions;
}

/** Uniqueness options after validation and normalization. */
export interface NormalizedUniqueOptions extends Omit<
  UniqueOptions,
  "byPeriod"
> {
  byPeriod?: Temporal.Duration;
}

/** Resolved, validated options sent to an insertion backend. */
export interface ResolvedInsertOptions {
  maxAttempts: number;
  metadata: JsonObject;
  pending: boolean;
  priority: number;
  queue: string;
  scheduledAt: Temporal.Instant;
  tags: readonly string[];
  unique?: NormalizedUniqueOptions;
}

/** Snapshot and validate a partial insertion configuration. */
export function normalizeInsertOptions(
  options: InsertOptions = {}
): Readonly<NormalizedInsertOptions> {
  const copy: NormalizedInsertOptions = {};

  if (options.maxAttempts !== undefined) {
    requireInteger("maxAttempts", options.maxAttempts, 1, 32_767);
    copy.maxAttempts = options.maxAttempts;
  }
  if (options.metadata !== undefined) {
    copy.metadata = deepFreezeJson(toJsonObject(options.metadata));
  }
  if (options.pending !== undefined) {
    if (typeof options.pending !== "boolean") {
      throw new ValidationError("pending must be a boolean");
    }
    copy.pending = options.pending;
  }
  if (options.priority !== undefined) {
    requireInteger("priority", options.priority, 1, 4);
    copy.priority = options.priority;
  }
  if (options.queue !== undefined) {
    validateQueueName(options.queue);
    copy.queue = options.queue;
  }
  if (options.delay !== undefined && options.scheduledAt !== undefined) {
    throw new ValidationError("delay and scheduledAt are mutually exclusive");
  }
  if (options.delay !== undefined) {
    copy.delay = toDuration("delay", options.delay, {
      allowZero: true,
      error: ValidationError,
    });
  }
  if (options.scheduledAt !== undefined) {
    copy.scheduledAt = toInstant("scheduledAt", options.scheduledAt);
  }
  if (options.tags !== undefined) {
    copy.tags = validateTags(options.tags);
  }
  if (options.unique !== undefined) {
    copy.unique = normalizeUniqueOptions(options.unique);
  }
  return Object.freeze(copy);
}

/**
 * @internal Split a selected field path on unescaped dots. A backslash quotes
 * the next character, as in the paths River Go derives from JSON field names.
 *
 * A segment that is an unsigned integer or `-1`, escaped or not, is rejected.
 * River Go assembles selected values with `sjson`, which builds a JSON array
 * rather than an object for such a segment, so the hashed text would differ
 * from the object JavaScript assembles.
 */
export function parseUniquePath(path: string): readonly string[] {
  const segments: string[] = [];
  let segment = "";
  let escaped = false;
  const pushSegment = () => {
    if (/^[0-9]+$/.test(segment) || segment === "-1") {
      throw new ValidationError(
        "unique path " +
          JSON.stringify(path) +
          " has segment " +
          JSON.stringify(segment) +
          ", which River Go treats as an array index when assembling unique" +
          " args, so its unique key can't match Go's; select fields whose" +
          " names aren't unsigned integers or -1"
      );
    }
    segments.push(segment);
    segment = "";
  };
  for (const character of path) {
    if (escaped) {
      segment += character;
      escaped = false;
    } else if (character === "\\") {
      escaped = true;
    } else if (character === ".") {
      if (segment.length === 0) {
        throw new ValidationError(
          "unique path " + JSON.stringify(path) + " has an empty segment"
        );
      }
      pushSegment();
    } else {
      segment += character;
    }
  }
  if (escaped || segment.length === 0) {
    throw new ValidationError(
      "unique path " +
        JSON.stringify(path) +
        " has an empty segment or trailing escape"
    );
  }
  pushSegment();
  return segments;
}

/** @internal Snapshot and validate uniqueness options. */
export function normalizeUniqueOptions(
  options: UniqueOptions
): Readonly<NormalizedUniqueOptions> {
  const copy: NormalizedUniqueOptions = {};
  if (options.byArgs !== undefined) {
    const byArgs: unknown = options.byArgs;
    if (byArgs !== true && !Array.isArray(byArgs)) {
      throw new ValidationError(
        "unique.byArgs must be true or an array of field names"
      );
    }
    if (options.byArgs !== true) {
      if (options.byArgs.length === 0) {
        throw new ValidationError("unique.byArgs field list must not be empty");
      }
      for (const path of options.byArgs) {
        if (typeof path !== "string") {
          throw new ValidationError(
            "unique.byArgs field names must be strings"
          );
        }
        parseUniquePath(path);
      }
      copy.byArgs = Object.freeze([...options.byArgs]);
    } else {
      copy.byArgs = options.byArgs;
    }
  }
  if (options.byPeriod !== undefined) {
    copy.byPeriod = toDuration("unique.byPeriod", options.byPeriod, {
      error: ValidationError,
    });
    uniquePeriodNanoseconds(copy);
  }
  if (options.byQueue !== undefined) {
    if (typeof options.byQueue !== "boolean") {
      throw new ValidationError("unique.byQueue must be a boolean");
    }
    copy.byQueue = options.byQueue;
  }
  if (options.byState !== undefined) {
    for (const state of options.byState) {
      if (!Object.values(JOB_STATE).includes(state)) {
        throw new ValidationError(
          `unknown River job state: ${JSON.stringify(state)}`
        );
      }
    }
    if (options.byState.length > 0) {
      for (const required of REQUIRED_UNIQUE_STATES) {
        if (!options.byState.includes(required)) {
          throw new ValidationError(
            `unique.byState must include required state ${JSON.stringify(required)}`
          );
        }
      }
    }
    copy.byState = Object.freeze([...options.byState]);
  }
  if (options.excludeKind !== undefined) {
    if (typeof options.excludeKind !== "boolean") {
      throw new ValidationError("unique.excludeKind must be a boolean");
    }
    copy.excludeKind = options.excludeKind;
  }
  // Like Go, a key without the kind needs another dimension: otherwise it
  // would be the same for every job in the table.
  if (
    copy.excludeKind === true &&
    copy.byArgs === undefined &&
    copy.byPeriod === undefined &&
    copy.byQueue !== true
  ) {
    throw new ValidationError(
      "unique.excludeKind requires byArgs, byQueue, or byPeriod"
    );
  }
  return Object.freeze(copy);
}

/** @internal Resolve River's exact Go-compatible uniqueness period. */
export function uniquePeriodNanoseconds(
  options: NormalizedUniqueOptions
): bigint | null {
  if (options.byPeriod === undefined) return null;
  const nanoseconds = durationNanoseconds(options.byPeriod);
  if (nanoseconds < NANOSECONDS_PER_SECOND) {
    throw new ValidationError("unique.byPeriod must be at least one second");
  }
  if (nanoseconds > MAX_GO_DURATION_NANOSECONDS) {
    throw new ValidationError("unique.byPeriod exceeds River's range");
  }
  return nanoseconds;
}

/** @internal Resolve an insertion's scheduled time from its options. */
export function resolveScheduledAt(
  options: Pick<NormalizedInsertOptions, "delay" | "scheduledAt">,
  now: Temporal.Instant
): Temporal.Instant | undefined {
  if (options.scheduledAt !== undefined) return options.scheduledAt;
  if (options.delay === undefined) return undefined;
  return now.add({
    nanoseconds: Number(durationNanoseconds(options.delay)),
  });
}

function toInstant(
  name: string,
  value: Date | Temporal.Instant
): Temporal.Instant {
  if (value instanceof Temporal.Instant) return value;
  if (value instanceof Date) {
    const milliseconds = value.getTime();
    if (Number.isNaN(milliseconds)) {
      throw new ValidationError(`${name} must be a valid Date`);
    }
    return Temporal.Instant.fromEpochMilliseconds(milliseconds);
  }
  throw new ValidationError(`${name} must be a Temporal.Instant or Date`);
}

function requireInteger(
  name: string,
  value: number,
  min: number,
  max?: number
): void {
  if (
    !Number.isSafeInteger(value) ||
    value < min ||
    (max !== undefined && value > max)
  ) {
    const range =
      max === undefined ? `at least ${min}` : `between ${min} and ${max}`;
    throw new ValidationError(`${name} must be a safe integer ${range}`);
  }
}

function validateTags(tags: readonly string[]): readonly string[] {
  const value: unknown = tags;
  if (!Array.isArray(value)) throw new ValidationError("tags must be an array");
  const copy = [...tags];
  for (const tag of copy) {
    if (typeof tag !== "string") {
      throw new ValidationError("tags must contain only strings");
    }
    if (tag.length > 255) {
      throw new ValidationError("tags must be at most 255 characters");
    }
    if (!TAG_RE.test(tag)) {
      throw new ValidationError(`tag must match ${TAG_RE}`);
    }
  }
  return Object.freeze(copy);
}
