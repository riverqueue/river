import { ConfigurationError } from "./errors.js";
import { parseGoDuration } from "./internal/go-duration.js";
import type { PeriodicSchedule } from "./periodic.js";

/** Options for {@link cron}. */
export interface CronOptions {
  /**
   * Time zone the expression is evaluated in when it has no `CRON_TZ=` or
   * `TZ=` prefix: an IANA name such as `"America/Chicago"`, `"UTC"`, or a
   * fixed offset such as `"+05:30"`. Defaults to the process's local time
   * zone, `Temporal.Now.timeZoneId()`, which is what River Go uses.
   */
  readonly timeZone?: string;
}

/** A standard cron schedule created by {@link cron}. */
export interface CronSchedule extends PeriodicSchedule {
  /** The expression the schedule was parsed from. */
  readonly expression: string;
  /**
   * Time zone occurrences are computed in: the expression's `CRON_TZ=`
   * prefix, else {@link CronOptions.timeZone}, else the process's local
   * zone when the schedule was created.
   */
  readonly timeZone: string;
}

/**
 * Parse a standard cron expression into a periodic schedule that fires at
 * exactly the times River Go would.
 *
 * River Go documents robfig/cron's `ParseStandard` syntax, and this is a
 * faithful port of that parser and its `Next` algorithm, so one expression
 * string behaves the same whichever language holds leadership:
 *
 * - five fields: minute (0-59), hour (0-23), day of month (1-31), month
 *   (1-12 or `jan`-`dec`), and day of week (0-6 from Sunday, or
 *   `sun`-`sat`); names are case-insensitive and `7` is not Sunday;
 * - `*` or `?` for every value, lists (`1,15`), ranges (`9-17`), and steps
 *   on wildcards, values, or ranges (`5/15` is `5-59/15`);
 * - when both day of month and day of week are restricted, a day matching
 *   either one fires; when either is `*` or `?`, both must match. A wildcard
 *   with a step greater than one counts as restricted;
 * - the descriptors `@yearly` (or `@annually`), `@monthly`, `@weekly`,
 *   `@daily` (or `@midnight`), and `@hourly`;
 * - `@every <duration>` with Go's duration syntax (`1h30m`, `1.5h`, `90s`),
 *   measured from the previous occurrence, truncated to whole seconds, and
 *   at least one second;
 * - a leading `CRON_TZ=<zone>` or `TZ=<zone>` naming an IANA time zone.
 *
 * Seconds fields, `L`, `W`, `#`, and `@reboot` are rejected, as in Go.
 *
 * Occurrences follow wall-clock time in the schedule's time zone, including
 * robfig's handling of daylight saving time: a time skipped by a transition
 * does not fire that day, and a time repeated by one can fire twice.
 *
 * The time zone is the expression's `CRON_TZ=` prefix, else
 * `options.timeZone`, else the process's local time zone, matching River Go,
 * which evaluates unprefixed expressions in `time.Local`. Periodic jobs run
 * on whichever client holds leadership, so every process in a fleet,
 * including Go and Rust ones, must resolve the same zone. Pin it explicitly,
 * preferably with a prefix such as `CRON_TZ=UTC` that every language reads
 * from the shared expression, or with `options.timeZone`.
 *
 * @param expression - A standard cron expression or descriptor.
 * @param options - Optional default time zone.
 * @returns A schedule for {@link periodicJob}'s `schedule` option.
 * @throws ConfigurationError when the expression is not valid River Go cron
 *   syntax or the time zone is unknown. The schedule's `next()` also throws
 *   it when an occurrence would have to cross a calendar day the zone
 *   skipped, as Samoa skipped 2011-12-30; River Go never returns there.
 *
 * @example
 * ```ts
 * const weekdayReport = periodicJob({
 *   args: { scope: "all" },
 *   id: "weekday_report",
 *   job: buildReport,
 *   // 09:00 New York time, Monday through Friday.
 *   schedule: cron("CRON_TZ=America/New_York 0 9 * * mon-fri"),
 * });
 * ```
 */
export function cron(expression: string, options?: CronOptions): CronSchedule {
  if (typeof expression !== "string") {
    throw new ConfigurationError("cron expression must be a string");
  }
  if (
    options !== undefined &&
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- validates untyped JavaScript input
    (options === null || typeof options !== "object")
  ) {
    throw new ConfigurationError("cron options must be an object");
  }
  let parsed: ParsedCronExpression;
  try {
    parsed = parseCronExpression(expression);
  } catch (cause: unknown) {
    throw new ConfigurationError(
      `invalid cron expression ${JSON.stringify(expression)}: ${(cause as Error).message}`,
      { cause, details: { expression } }
    );
  }
  const timeZone = parsed.timeZone ?? resolveTimeZoneOption(options?.timeZone);
  const spec = parsed.spec;
  return Object.freeze({
    expression,
    next: (after: Temporal.Instant) =>
      nextCronOccurrence(spec, timeZone, after),
    timeZone,
  });
}

/** @internal A parsed field: value bits plus {@link STAR_BIT}. */
export type CronBits = bigint;

/** @internal robfig's `SpecSchedule` or `ConstantDelaySchedule`. */
export type CronSpec =
  | {
      /** Whole-second delay between occurrences. */
      readonly delayNanoseconds: bigint;
      readonly kind: "every";
    }
  | {
      readonly dom: CronBits;
      readonly dow: CronBits;
      readonly hour: CronBits;
      readonly kind: "fields";
      readonly minute: CronBits;
      readonly month: CronBits;
    };

/** @internal Result of {@link parseCronExpression}. */
export interface ParsedCronExpression {
  readonly spec: CronSpec;
  /** Zone from a `CRON_TZ=`/`TZ=` prefix; undefined without one or for `Local`. */
  readonly timeZone: string | undefined;
}

/** @internal Inclusive bounds and names of one cron field. */
export interface CronBounds {
  readonly maximum: number;
  readonly minimum: number;
  readonly names?: ReadonlyMap<string, number>;
}

/** @internal Set when a field was written as `*` or `?` (robfig's `starBit`). */
export const STAR_BIT: CronBits = 1n << 63n;

/** @internal */
export const MINUTES: CronBounds = { maximum: 59, minimum: 0 };
/** @internal */
export const HOURS: CronBounds = { maximum: 23, minimum: 0 };
/** @internal */
export const DAYS_OF_MONTH: CronBounds = { maximum: 31, minimum: 1 };
/** @internal */
export const MONTHS: CronBounds = {
  maximum: 12,
  minimum: 1,
  names: new Map([
    ["jan", 1],
    ["feb", 2],
    ["mar", 3],
    ["apr", 4],
    ["may", 5],
    ["jun", 6],
    ["jul", 7],
    ["aug", 8],
    ["sep", 9],
    ["oct", 10],
    ["nov", 11],
    ["dec", 12],
  ]),
};
/** @internal */
export const DAYS_OF_WEEK: CronBounds = {
  maximum: 6,
  minimum: 0,
  names: new Map([
    ["sun", 0],
    ["mon", 1],
    ["tue", 2],
    ["wed", 3],
    ["thu", 4],
    ["fri", 5],
    ["sat", 6],
  ]),
};

/**
 * Characters Go's `unicode.IsSpace` accepts, which `strings.Fields` and
 * `strings.TrimSpace` split and trim on. Unlike JavaScript's `\s`, this
 * includes U+0085 and excludes U+FEFF.
 */
const GO_SPACE =
  "\\t\\n\\v\\f\\r \\u0085\\u00a0\\u1680\\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000";
const GO_FIELDS = new RegExp(`[^${GO_SPACE}]+`, "g");
const GO_TRIM = new RegExp(`^[${GO_SPACE}]+|[${GO_SPACE}]+$`, "g");

const NANOSECONDS_PER_SECOND = 1_000_000_000n;

/**
 * Latest instant from which an occurrence is computed: robfig searches up to
 * five years ahead, and Temporal cannot represent instants past
 * +275760-09-13.
 */
const LATEST_INSTANT_NANOSECONDS = 8_640_000_000_000_000_000_000n;
const LATEST_EVALUABLE_NANOSECONDS =
  LATEST_INSTANT_NANOSECONDS - 7n * 366n * 86_400n * NANOSECONDS_PER_SECOND;

/**
 * @internal Parse a cron expression exactly like robfig/cron's
 * `ParseStandard`, throwing an `Error` with robfig's message on rejection.
 */
export function parseCronExpression(expression: string): ParsedCronExpression {
  if (expression.length === 0) throw new Error("empty spec string");

  let spec = expression;
  let timeZone: string | undefined;
  if (spec.startsWith("TZ=") || spec.startsWith("CRON_TZ=")) {
    const space = spec.indexOf(" ");
    // robfig slices up to the first space and panics without one.
    if (space < 0) {
      throw new Error("time zone prefix must be followed by a schedule");
    }
    const name = spec.slice(spec.indexOf("=") + 1, space);
    timeZone = loadGoLocation(name);
    spec = spec.slice(space).replace(GO_TRIM, "");
  }

  if (spec.startsWith("@")) return { spec: parseDescriptor(spec), timeZone };

  const fields = spec.match(GO_FIELDS) ?? [];
  const [minute, hour, dom, month, dow] = fields;
  if (
    fields.length !== 5 ||
    minute === undefined ||
    hour === undefined ||
    dom === undefined ||
    month === undefined ||
    dow === undefined
  ) {
    throw new Error(
      `expected exactly 5 fields, found ${fields.length}: [${fields.join(" ")}]`
    );
  }
  return {
    spec: {
      dom: parseCronField(dom, DAYS_OF_MONTH),
      dow: parseCronField(dow, DAYS_OF_WEEK),
      hour: parseCronField(hour, HOURS),
      kind: "fields",
      minute: parseCronField(minute, MINUTES),
      month: parseCronField(month, MONTHS),
    },
    timeZone,
  };
}

/**
 * @internal robfig's `getField`: a comma-separated list of ranges, skipping
 * empty items like Go's `strings.FieldsFunc`.
 */
export function parseCronField(field: string, bounds: CronBounds): CronBits {
  let bits = 0n;
  for (const range of field.split(",")) {
    if (range !== "") bits |= parseCronRange(range, bounds);
  }
  return bits;
}

/**
 * @internal robfig's `getRange`: `*`, `?`, a number or name, or a range,
 * each optionally followed by `/step`.
 */
export function parseCronRange(
  expression: string,
  bounds: CronBounds
): CronBits {
  const rangeAndStep = expression.split("/");
  const lowAndHigh = (rangeAndStep[0] ?? "").split("-");
  const low = lowAndHigh[0] ?? "";
  const single = lowAndHigh.length === 1;

  let start: number;
  let end: number;
  let extra = 0n;
  if (low === "*" || low === "?") {
    start = bounds.minimum;
    end = bounds.maximum;
    extra = STAR_BIT;
  } else {
    start = parseIntOrName(low, bounds.names);
    if (lowAndHigh.length === 1) {
      end = start;
    } else if (lowAndHigh.length === 2) {
      end = parseIntOrName(lowAndHigh[1] ?? "", bounds.names);
    } else {
      throw new Error(`too many hyphens: ${expression}`);
    }
  }

  let step: number;
  if (rangeAndStep.length === 1) {
    step = 1;
  } else if (rangeAndStep.length === 2) {
    step = parseNonNegativeInt(rangeAndStep[1] ?? "");
    // "N/step" means "N-max/step".
    if (single) end = bounds.maximum;
    // A real step makes a wildcard a restriction for the day rule.
    if (step > 1) extra = 0n;
  } else {
    throw new Error(`too many slashes: ${expression}`);
  }

  if (start < bounds.minimum) {
    throw new Error(
      `beginning of range (${start}) below minimum (${bounds.minimum}): ${expression}`
    );
  }
  if (end > bounds.maximum) {
    throw new Error(
      `end of range (${end}) above maximum (${bounds.maximum}): ${expression}`
    );
  }
  if (start > end) {
    throw new Error(
      `beginning of range (${start}) beyond end of range (${end}): ${expression}`
    );
  }
  if (step === 0) {
    throw new Error(`step of range should be a positive number: ${expression}`);
  }
  return cronBits(start, end, step) | extra;
}

/** @internal robfig's `getBits`: every `step`th value in `[minimum, maximum]`. */
export function cronBits(
  minimum: number,
  maximum: number,
  step: number
): CronBits {
  let bits = 0n;
  for (let value = minimum; value <= maximum; value += step) {
    bits |= 1n << BigInt(value);
  }
  return bits;
}

/** @internal robfig's `all`: every value in `bounds`, plus the star bit. */
export function allCronBits(bounds: CronBounds): CronBits {
  return cronBits(bounds.minimum, bounds.maximum, 1) | STAR_BIT;
}

function parseDescriptor(descriptor: string): CronSpec {
  const fields = (
    overrides: Partial<Record<"dom" | "dow" | "hour" | "month", CronBits>>
  ): CronSpec => ({
    dom: overrides.dom ?? allCronBits(DAYS_OF_MONTH),
    dow: overrides.dow ?? allCronBits(DAYS_OF_WEEK),
    hour: overrides.hour ?? 1n << BigInt(HOURS.minimum),
    kind: "fields",
    minute: 1n << BigInt(MINUTES.minimum),
    month: overrides.month ?? allCronBits(MONTHS),
  });
  switch (descriptor) {
    case "@yearly":
    case "@annually":
      return fields({
        dom: 1n << BigInt(DAYS_OF_MONTH.minimum),
        month: 1n << BigInt(MONTHS.minimum),
      });
    case "@monthly":
      return fields({ dom: 1n << BigInt(DAYS_OF_MONTH.minimum) });
    case "@weekly":
      return fields({ dow: 1n << BigInt(DAYS_OF_WEEK.minimum) });
    case "@daily":
    case "@midnight":
      return fields({});
    case "@hourly":
      return fields({ hour: allCronBits(HOURS) });
  }

  const every = "@every ";
  if (!descriptor.startsWith(every)) {
    throw new Error(`unrecognized descriptor: ${descriptor}`);
  }
  let duration: bigint;
  try {
    duration = parseGoDuration(descriptor.slice(every.length));
  } catch (cause: unknown) {
    throw new Error(
      `failed to parse duration ${descriptor}: ${(cause as Error).message}`,
      { cause }
    );
  }
  // robfig's `Every` rounds up to one second and drops subseconds.
  if (duration < NANOSECONDS_PER_SECOND) duration = NANOSECONDS_PER_SECOND;
  return {
    delayNanoseconds: duration - (duration % NANOSECONDS_PER_SECOND),
    kind: "every",
  };
}

/** robfig's `parseIntOrName`, with Go's lowercasing of names. */
function parseIntOrName(
  expression: string,
  names: ReadonlyMap<string, number> | undefined
): number {
  if (names !== undefined) {
    // Go's `strings.ToLower` maps U+0130 to a plain "i"; JavaScript adds a
    // combining dot. No other non-ASCII letter lowercases into a name.
    const named = names.get(expression.replaceAll("\u0130", "i").toLowerCase());
    if (named !== undefined) return named;
  }
  return parseNonNegativeInt(expression);
}

/**
 * robfig's `mustParseInt`: Go's `strconv.Atoi` (an optional sign and ASCII
 * digits within int64) and then a non-negative check. Values past any field's
 * range are clamped, since they are rejected or only ever used as a step.
 */
function parseNonNegativeInt(expression: string): number {
  if (!/^[+-]?[0-9]+$/.test(expression)) {
    throw new Error(
      `failed to parse int from ${expression}: strconv.Atoi: parsing ${JSON.stringify(expression)}: invalid syntax`
    );
  }
  const value = BigInt(expression);
  if (
    value > 9_223_372_036_854_775_807n ||
    value < -9_223_372_036_854_775_808n
  ) {
    throw new Error(
      `failed to parse int from ${expression}: strconv.Atoi: parsing ${JSON.stringify(expression)}: value out of range`
    );
  }
  if (value < 0n) {
    throw new Error(`negative number (${value}) not allowed: ${expression}`);
  }
  return Number(value > 4_294_967_295n ? 4_294_967_295n : value);
}

/**
 * Go's `time.LoadLocation` for a `CRON_TZ=` name: `""` and `UTC` are UTC,
 * `Local` defers to the default zone, and anything else must be an IANA name
 * spelled exactly as the time zone database spells it.
 */
function loadGoLocation(name: string): string | undefined {
  if (name === "" || name === "UTC") return "UTC";
  if (name === "Local") return undefined;
  // Temporal also accepts offsets, bracketed date-times, and names in any
  // case; Go's zoneinfo lookup accepts none of them.
  if (!name.startsWith("+") && !name.startsWith("-")) {
    const resolved = timeZoneId(name);
    if (resolved === name) return resolved;
  }
  throw new Error(`provided bad location ${name}: unknown time zone ${name}`);
}

function resolveTimeZoneOption(timeZone: string | undefined): string {
  if (timeZone === undefined) return Temporal.Now.timeZoneId();
  const resolved =
    typeof timeZone === "string" ? timeZoneId(timeZone) : undefined;
  if (resolved === undefined) {
    throw new ConfigurationError(
      `cron timeZone ${JSON.stringify(timeZone)} is not a known time zone`
    );
  }
  return resolved;
}

function timeZoneId(name: string): string | undefined {
  try {
    return new Temporal.ZonedDateTime(0n, name).timeZoneId;
  } catch {
    return undefined;
  }
}

/**
 * @internal robfig's `SpecSchedule.Next` or `ConstantDelaySchedule.Next`,
 * evaluated in `timeZone`. Returns null when nothing matches within five
 * years, where robfig returns Go's zero time, and when the occurrence would
 * fall outside Temporal's range.
 */
function nextCronOccurrence(
  spec: CronSpec,
  timeZone: string,
  after: Temporal.Instant
): Temporal.Instant | null {
  const afterNanoseconds = after.epochNanoseconds;
  if (afterNanoseconds > LATEST_EVALUABLE_NANOSECONDS) return null;
  const subsecond =
    ((afterNanoseconds % NANOSECONDS_PER_SECOND) + NANOSECONDS_PER_SECOND) %
    NANOSECONDS_PER_SECOND;
  if (spec.kind === "every") {
    const next = afterNanoseconds + spec.delayNanoseconds - subsecond;
    return next > LATEST_INSTANT_NANOSECONDS
      ? null
      : Temporal.Instant.fromEpochNanoseconds(next);
  }
  if (!canMatch(spec)) return null;

  const zone = new GoZone(timeZone);
  // Start at the earliest possible time (the upcoming second). Every later
  // step keeps whole seconds, so `time` is in epoch seconds.
  let time =
    Number((afterNanoseconds - subsecond) / NANOSECONDS_PER_SECOND) + 1;
  let added = false;
  const yearLimit = zone.wall(time).year + 5;

  // Each `continue wrap` is robfig's `goto WRAP`: a field rolled over, so
  // every earlier field must be checked again.
  wrap: for (;;) {
    let wall = zone.wall(time);
    if (wall.year > yearLimit) return null;

    while (!hasBit(spec.month, wall.month)) {
      if (!added) {
        added = true;
        time = zone.date(wall.year, wall.month, 1, 0, 0, 0);
        wall = zone.wall(time);
      }
      // `t.AddDate(0, 1, 0)`, normalizing an overflowing day.
      time = zone.date(
        wall.year,
        wall.month + 1,
        wall.day,
        wall.hour,
        wall.minute,
        wall.second
      );
      wall = zone.wall(time);
      if (wall.month === 1) continue wrap;
    }

    while (!dayMatches(spec, wall)) {
      if (!added) {
        added = true;
        time = zone.date(wall.year, wall.month, wall.day, 0, 0, 0);
        wall = zone.wall(time);
      }
      const previous = time;
      const previousWall = wall;
      time = zone.date(
        wall.year,
        wall.month,
        wall.day + 1,
        wall.hour,
        wall.minute,
        wall.second
      );
      wall = zone.wall(time);
      // Midnight may not exist on a daylight saving transition.
      if (wall.hour !== 0) {
        time += wall.hour > 12 ? (24 - wall.hour) * 3600 : -wall.hour * 3600;
        wall = zone.wall(time);
      }
      // A zone that skips a whole calendar day, as Pacific/Apia did on
      // 2011-12-30, resolves the skipped midnight back to the day before,
      // and robfig loops forever. Fail instead of blocking the event loop.
      if (time <= previous) throw skippedDayError(timeZone, previousWall);
      if (wall.day === 1) continue wrap;
    }

    while (!hasBit(spec.hour, wall.hour)) {
      if (!added) {
        added = true;
        time = zone.date(wall.year, wall.month, wall.day, wall.hour, 0, 0);
      }
      time += 3600;
      wall = zone.wall(time);
      if (wall.hour === 0) continue wrap;
    }

    while (!hasBit(spec.minute, wall.minute)) {
      if (!added) {
        added = true;
        // `t.Truncate(time.Minute)` rounds absolute time, not wall time.
        time -= floorMod(time, 60);
      }
      time += 60;
      wall = zone.wall(time);
      if (wall.minute === 0) continue wrap;
    }

    // Standard expressions always fire at second zero. Times are already
    // whole seconds, so robfig's `t.Truncate(time.Second)` is a no-op.
    while (wall.second !== 0) {
      added = true;
      time += 1;
      wall = zone.wall(time);
      if (wall.second === 0) continue wrap;
    }
    return Temporal.Instant.fromEpochMilliseconds(time * 1000);
  }
}

function skippedDayError(
  timeZone: string,
  wall: WallClock
): ConfigurationError {
  const day = Temporal.PlainDate.from({
    day: wall.day,
    month: wall.month,
    year: wall.year,
  }).add({ days: 1 });
  return new ConfigurationError(
    `cron schedule cannot pass ${day.toString()} in ${timeZone}, a day the time zone skips; River Go never returns from this schedule`,
    { details: { day: day.toString(), timeZone } }
  );
}

interface WallClock {
  readonly day: number;
  readonly hour: number;
  readonly minute: number;
  readonly month: number;
  readonly second: number;
  /** Go's `Weekday`: 0 is Sunday. */
  readonly weekday: number;
  readonly year: number;
}

/** Go's view of a `*time.Location`, over epoch seconds. */
class GoZone {
  readonly #timeZone: string;

  constructor(timeZone: string) {
    this.#timeZone = timeZone;
  }

  /**
   * Go's `time.Date`: overflowing months and days roll forward, and a wall
   * time a transition skips or repeats resolves exactly as Go resolves it,
   * which depends on the direction of the zone's offset.
   */
  date(
    year: number,
    month: number,
    day: number,
    hour: number,
    minute: number,
    second: number
  ): number {
    const monthIndex = month - 1;
    const normalizedYear = year + Math.floor(monthIndex / 12);
    const normalizedMonth = floorMod(monthIndex, 12) + 1;
    const local =
      (daysFromCivil(normalizedYear, normalizedMonth, 1) + day - 1) * 86_400 +
      hour * 3600 +
      minute * 60 +
      second;
    // Go looks up the offset at the local time read as UTC, then again at
    // the UTC time that offset implies, and uses the second offset.
    return local - this.#offset(local - this.#offset(local));
  }

  wall(epochSeconds: number): WallClock {
    const zoned = this.#zoned(epochSeconds);
    return {
      day: zoned.day,
      hour: zoned.hour,
      minute: zoned.minute,
      month: zoned.month,
      second: zoned.second,
      weekday: zoned.dayOfWeek % 7,
      year: zoned.year,
    };
  }

  #offset(epochSeconds: number): number {
    return this.#zoned(epochSeconds).offsetNanoseconds / 1_000_000_000;
  }

  #zoned(epochSeconds: number): Temporal.ZonedDateTime {
    return Temporal.Instant.fromEpochMilliseconds(
      epochSeconds * 1000
    ).toZonedDateTimeISO(this.#timeZone);
  }
}

/**
 * Whether any time can satisfy every field. A field left empty by an
 * expression like `,` never matches; robfig then scans five years minute by
 * minute before giving up, and skipping that scan returns the same null.
 */
function canMatch(spec: CronSpec & { kind: "fields" }): boolean {
  const values = (bits: CronBits) => (bits & ~STAR_BIT) !== 0n;
  const days =
    (spec.dom & STAR_BIT) !== 0n || (spec.dow & STAR_BIT) !== 0n
      ? values(spec.dom) && values(spec.dow)
      : values(spec.dom) || values(spec.dow);
  return values(spec.minute) && values(spec.hour) && values(spec.month) && days;
}

/**
 * robfig's `dayMatches`: when either day field is a wildcard both must
 * match; otherwise either may.
 */
function dayMatches(
  spec: CronSpec & { kind: "fields" },
  wall: WallClock
): boolean {
  const dom = hasBit(spec.dom, wall.day);
  const dow = hasBit(spec.dow, wall.weekday);
  if ((spec.dom & STAR_BIT) !== 0n || (spec.dow & STAR_BIT) !== 0n) {
    return dom && dow;
  }
  return dom || dow;
}

function hasBit(bits: CronBits, value: number): boolean {
  return (bits & (1n << BigInt(value))) !== 0n;
}

/** Days from 1970-01-01 to a proleptic Gregorian date. */
function daysFromCivil(year: number, month: number, day: number): number {
  const shifted = month <= 2 ? year - 1 : year;
  const era = Math.floor(shifted / 400);
  const yearOfEra = shifted - era * 400;
  const dayOfYear =
    Math.floor((153 * (month + (month > 2 ? -3 : 9)) + 2) / 5) + day - 1;
  const dayOfEra =
    yearOfEra * 365 +
    Math.floor(yearOfEra / 4) -
    Math.floor(yearOfEra / 100) +
    dayOfYear;
  return era * 146_097 + dayOfEra - 719_468;
}

function floorMod(value: number, divisor: number): number {
  return ((value % divisor) + divisor) % divisor;
}
