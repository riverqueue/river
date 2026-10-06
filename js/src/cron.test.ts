import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

import {
  allCronBits,
  cron,
  cronBits,
  DAYS_OF_MONTH,
  DAYS_OF_WEEK,
  HOURS,
  MINUTES,
  MONTHS,
  parseCronExpression,
  parseCronField,
  parseCronRange,
  STAR_BIT,
  type CronSchedule,
  type CronSpec,
} from "./cron.js";
import { ConfigurationError } from "./errors.js";
import { defineJob } from "./job-definition.js";
import { periodicJob } from "./periodic.js";

interface CronGoldens {
  readonly cron_cases: readonly {
    readonly expression: string;
    readonly from: string;
    readonly name: string;
    readonly next: readonly string[];
  }[];
  readonly cron_invalid: readonly string[];
  readonly cron_named_zone_cases: readonly {
    readonly expression: string;
    readonly from: string;
    readonly name: string;
    readonly next: readonly string[];
  }[];
}

/** River Go's cron goldens, generated with robfig/cron; see the file. */
const GOLDENS = new URL(
  "../../conformance/testdata/cron_schedules.json",
  import.meta.url
);

/**
 * Reads a fixture that `make generate/fixtures` writes from River's Go
 * implementation. A missing fixture fails the test rather than skipping it.
 */
async function readFixture(url: URL): Promise<string> {
  try {
    return await readFile(url, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(
        `missing conformance fixture ${fileURLToPath(url)}; run \`make generate/fixtures\` from the repository root`,
        { cause: error }
      );
    }
    throw error;
  }
}

/** Up to `count` successive occurrences after `from`, like robfig's tests. */
function occurrences(
  schedule: CronSchedule,
  from: Temporal.Instant,
  count: number
): Temporal.Instant[] {
  const result: Temporal.Instant[] = [];
  let current = from;
  while (result.length < count) {
    const next = schedule.next(current);
    if (next === null) break;
    result.push(next);
    current = next;
  }
  return result;
}

/** RFC 3339 in `timeZone`, with Go's `Z` for a zero offset. */
function format(instant: Temporal.Instant, timeZone: string): string {
  return instant
    .toZonedDateTimeISO(timeZone)
    .toString({ timeZoneName: "never" })
    .replace(/\+00:00$/, "Z");
}

/** The fixed offset an RFC 3339 time was written in, as a time zone. */
function offsetZone(rfc3339: string): string {
  const offset = /(?:Z|[+-]\d{2}:\d{2})$/i.exec(rfc3339)?.[0];
  if (offset === undefined) throw new Error(`no offset in ${rfc3339}`);
  return offset.toUpperCase() === "Z" ? "UTC" : offset;
}

/**
 * Next occurrence after `from` (RFC 3339, evaluated in its offset), or null.
 * robfig's tests run in `time.Local`; these use UTC for the same wall times.
 */
function nextAfter(expression: string, from: string, timeZone?: string) {
  const zone = timeZone ?? offsetZone(from);
  const next = cron(expression, { timeZone: zone }).next(
    Temporal.Instant.from(from)
  );
  return next === null ? null : format(next, zone);
}

function spec(expression: string): CronSpec {
  return parseCronExpression(expression).spec;
}

function fieldsSpec(fields: {
  dom?: bigint;
  dow?: bigint;
  hour?: bigint;
  minute: bigint;
  month?: bigint;
}): CronSpec {
  return {
    dom: fields.dom ?? allCronBits(DAYS_OF_MONTH),
    dow: fields.dow ?? allCronBits(DAYS_OF_WEEK),
    hour: fields.hour ?? allCronBits(HOURS),
    kind: "fields",
    minute: fields.minute,
    month: fields.month ?? allCronBits(MONTHS),
  };
}

const midnight = fieldsSpec({ hour: 1n, minute: 1n });

describe("cron", () => {
  describe("River Go goldens", () => {
    const load = async (): Promise<CronGoldens> =>
      JSON.parse(await readFixture(GOLDENS)) as CronGoldens;

    it("returns robfig's successive occurrences in the reference offset", async () => {
      const goldens = await load();
      expect(goldens.cron_cases.length).toBeGreaterThan(0);
      for (const golden of goldens.cron_cases) {
        const zone = offsetZone(golden.from);
        const schedule = cron(golden.expression, { timeZone: zone });
        expect(
          occurrences(schedule, Temporal.Instant.from(golden.from), 5).map(
            (next) => format(next, zone)
          ),
          golden.name
        ).toEqual(golden.next);
      }
    });

    it("resolves IANA CRON_TZ zones across DST like Go's time.Date", async () => {
      const goldens = await load();
      expect(goldens.cron_named_zone_cases.length).toBeGreaterThan(0);
      for (const golden of goldens.cron_named_zone_cases) {
        // The prefix names the zone; any default zone must not matter.
        const zone = offsetZone(golden.from);
        const schedule = cron(golden.expression, {
          timeZone: "Pacific/Chatham",
        });
        expect(
          occurrences(schedule, Temporal.Instant.from(golden.from), 5).map(
            (next) => format(next, zone)
          ),
          golden.name
        ).toEqual(golden.next);
      }
    });

    it("rejects every expression robfig rejects", async () => {
      const goldens = await load();
      expect(goldens.cron_invalid.length).toBeGreaterThan(0);
      for (const expression of goldens.cron_invalid) {
        expect(() => cron(expression), JSON.stringify(expression)).toThrow(
          ConfigurationError
        );
      }
    });
  });

  // robfig/cron v3.0.1 parser_test.go and spec_test.go, limited to the
  // standard five-field parser River Go documents.
  describe("robfig parser tests", () => {
    it.each<[string, number, number, bigint, string]>([
      ["5", 0, 7, 1n << 5n, ""],
      ["0", 0, 7, 1n << 0n, ""],
      ["7", 0, 7, 1n << 7n, ""],
      ["5-5", 0, 7, 1n << 5n, ""],
      ["5-6", 0, 7, (1n << 5n) | (1n << 6n), ""],
      ["5-7", 0, 7, (1n << 5n) | (1n << 6n) | (1n << 7n), ""],
      ["5-6/2", 0, 7, 1n << 5n, ""],
      ["5-7/2", 0, 7, (1n << 5n) | (1n << 7n), ""],
      ["5-7/1", 0, 7, (1n << 5n) | (1n << 6n) | (1n << 7n), ""],
      ["*", 1, 3, (1n << 1n) | (1n << 2n) | (1n << 3n) | STAR_BIT, ""],
      ["*/2", 1, 3, (1n << 1n) | (1n << 3n), ""],
      ["5--5", 0, 0, 0n, "too many hyphens"],
      ["jan-x", 0, 0, 0n, "failed to parse int from"],
      ["2-x", 1, 5, 0n, "failed to parse int from"],
      ["*/-12", 0, 0, 0n, "negative number"],
      ["*//2", 0, 0, 0n, "too many slashes"],
      ["1", 3, 5, 0n, "below minimum"],
      ["6", 3, 5, 0n, "above maximum"],
      ["5-3", 3, 5, 0n, "beyond end of range"],
      ["*/0", 0, 0, 0n, "should be a positive number"],
    ])(
      "parses range %j in [%i, %i]",
      (expression, minimum, maximum, bits, error) => {
        const parse = () => parseCronRange(expression, { maximum, minimum });
        if (error === "") {
          expect(parse()).toBe(bits);
        } else {
          expect(parse).toThrow(error);
        }
      }
    );

    it.each<[string, number, number, bigint]>([
      ["5", 1, 7, 1n << 5n],
      ["5,6", 1, 7, (1n << 5n) | (1n << 6n)],
      ["5,6,7", 1, 7, (1n << 5n) | (1n << 6n) | (1n << 7n)],
      ["1,5-7/2,3", 1, 7, (1n << 1n) | (1n << 5n) | (1n << 7n) | (1n << 3n)],
    ])("parses field %j in [%i, %i]", (expression, minimum, maximum, bits) => {
      expect(parseCronField(expression, { maximum, minimum })).toBe(bits);
    });

    it("sets every value and the star bit for a wildcard", () => {
      expect(allCronBits(MINUTES)).toBe(0xfffffffffffffffn | STAR_BIT);
      expect(allCronBits(HOURS)).toBe(0xffffffn | STAR_BIT);
      expect(allCronBits(DAYS_OF_MONTH)).toBe(0xfffffffen | STAR_BIT);
      expect(allCronBits(MONTHS)).toBe(0x1ffen | STAR_BIT);
      expect(allCronBits(DAYS_OF_WEEK)).toBe(0x7fn | STAR_BIT);
    });

    it.each<[number, number, number, bigint]>([
      [0, 0, 1, 0x1n],
      [1, 1, 1, 0x2n],
      [1, 5, 2, 0x2an],
      [1, 4, 2, 0xan],
    ])("sets bits %i-%i/%i", (minimum, maximum, step, bits) => {
      expect(cronBits(minimum, maximum, step)).toBe(bits);
    });

    it("parses schedules, descriptors, and time zone prefixes", () => {
      const every5min = fieldsSpec({ minute: 1n << 5n });
      expect(parseCronExpression("5 * * * *")).toEqual({
        spec: every5min,
        timeZone: undefined,
      });
      expect(parseCronExpression("CRON_TZ=UTC  5 * * * *")).toEqual({
        spec: every5min,
        timeZone: "UTC",
      });
      expect(parseCronExpression("CRON_TZ=Asia/Tokyo 5 * * * *")).toEqual({
        spec: every5min,
        timeZone: "Asia/Tokyo",
      });
      expect(spec("@every 5m")).toEqual({
        delayNanoseconds: 300_000_000_000n,
        kind: "every",
      });
      expect(spec("@midnight")).toEqual(midnight);
      expect(parseCronExpression("TZ=UTC  @midnight")).toEqual({
        spec: midnight,
        timeZone: "UTC",
      });
      expect(parseCronExpression("TZ=Asia/Tokyo @midnight")).toEqual({
        spec: midnight,
        timeZone: "Asia/Tokyo",
      });
      const annual = fieldsSpec({
        dom: 1n << 1n,
        hour: 1n,
        minute: 1n,
        month: 1n << 1n,
      });
      expect(spec("@yearly")).toEqual(annual);
      expect(spec("@annually")).toEqual(annual);
      expect(spec("@monthly")).toEqual(
        fieldsSpec({ dom: 1n << 1n, hour: 1n, minute: 1n })
      );
      expect(spec("@weekly")).toEqual(
        fieldsSpec({ dow: 1n, hour: 1n, minute: 1n })
      );
      expect(spec("@daily")).toEqual(midnight);
      expect(spec("@hourly")).toEqual(fieldsSpec({ minute: 1n }));
    });

    it.each([
      ["5 j * * *", "failed to parse int from"],
      ["* * * *", "expected exactly 5 fields"],
      ["* 5 j * * *", "expected exactly 5 fields"],
      ["@every Xm", "failed to parse duration"],
      ["@unrecognized", "unrecognized descriptor"],
      ["", "empty spec string"],
      ["xyz", "expected exactly 5 fields"],
      ["60 0 * * *", "above maximum"],
      ["0 60 * * *", "above maximum"],
      ["0 0 * * XYZ", "failed to parse int from"],
      // robfig issue 144: a zero step must not hang.
      ["TZ=America/New_York 15/0 * * * *", "should be a positive number"],
    ])("rejects %j", (expression, message) => {
      expect(() => cron(expression)).toThrow(message);
    });
  });

  describe("robfig Next tests", () => {
    it.each<[string, string, boolean]>([
      // Every fifteen minutes.
      ["2012-07-09T15:00:00Z", "0/15 * * * *", true],
      ["2012-07-09T15:45:00Z", "0/15 * * * *", true],
      ["2012-07-09T15:40:00Z", "0/15 * * * *", false],
      // Every fifteen minutes, starting at 5 minutes.
      ["2012-07-09T15:05:00Z", "5/15 * * * *", true],
      ["2012-07-09T15:20:00Z", "5/15 * * * *", true],
      ["2012-07-09T15:50:00Z", "5/15 * * * *", true],
      // Named months.
      ["2012-07-15T15:00:00Z", "0/15 * * Jul *", true],
      ["2012-07-15T15:00:00Z", "0/15 * * Jun *", false],
      // Everything set.
      ["2012-07-15T08:30:00Z", "30 08 ? Jul Sun", true],
      ["2012-07-15T08:30:00Z", "30 08 15 Jul ?", true],
      ["2012-07-16T08:30:00Z", "30 08 ? Jul Sun", false],
      ["2012-07-16T08:30:00Z", "30 08 15 Jul ?", false],
      // Predefined schedules.
      ["2012-07-09T15:00:00Z", "@hourly", true],
      ["2012-07-09T15:04:00Z", "@hourly", false],
      ["2012-07-09T15:00:00Z", "@daily", false],
      ["2012-07-09T00:00:00Z", "@daily", true],
      ["2012-07-09T00:00:00Z", "@weekly", false],
      ["2012-07-08T00:00:00Z", "@weekly", true],
      ["2012-07-08T01:00:00Z", "@weekly", false],
      ["2012-07-08T00:00:00Z", "@monthly", false],
      ["2012-07-01T00:00:00Z", "@monthly", true],
      // When both day fields are restricted, only one needs to match.
      ["2012-07-15T00:00:00Z", "* * 1,15 * Sun", true],
      ["2012-06-15T00:00:00Z", "* * 1,15 * Sun", true],
      ["2012-08-01T00:00:00Z", "* * 1,15 * Sun", true],
      ["2012-07-15T00:00:00Z", "* * */10 * Sun", true],
      // When either is a wildcard, both need to match.
      ["2012-07-15T00:00:00Z", "* * * * Mon", false],
      ["2012-07-09T00:00:00Z", "* * 1,15 * *", false],
      ["2012-07-15T00:00:00Z", "* * 1,15 * *", true],
      ["2012-07-15T00:00:00Z", "* * */2 * Sun", true],
    ])("activates at %s for %j: %s", (time, expression, expected) => {
      const before = Temporal.Instant.from(time).subtract({ seconds: 1 });
      const next = nextAfter(expression, before.toString(), "UTC");
      expect(next === time).toBe(expected);
    });

    // robfig's seconds-field cases are omitted; a leading `0` seconds field
    // is dropped from the rest.
    it.each<[string, string, string | null, string?]>([
      // Simple cases.
      ["2012-07-09T14:45:00Z", "0/15 * * * *", "2012-07-09T15:00:00Z"],
      ["2012-07-09T14:59:00Z", "0/15 * * * *", "2012-07-09T15:00:00Z"],
      ["2012-07-09T14:59:59Z", "0/15 * * * *", "2012-07-09T15:00:00Z"],
      // Wrap around hours.
      ["2012-07-09T15:45:00Z", "20-35/15 * * * *", "2012-07-09T16:20:00Z"],
      // Wrap around days.
      ["2012-07-09T23:46:00Z", "*/15 * * * *", "2012-07-10T00:00:00Z"],
      ["2012-07-09T23:45:00Z", "20-35/15 * * * *", "2012-07-10T00:20:00Z"],
      // Wrap around months.
      ["2012-07-09T23:35:00Z", "0 0 9 Apr-Oct ?", "2012-08-09T00:00:00Z"],
      [
        "2012-07-09T23:35:00Z",
        "0 0 */5 Apr,Aug,Oct Mon",
        "2012-08-01T00:00:00Z",
      ],
      ["2012-07-09T23:35:00Z", "0 0 */5 Oct Mon", "2012-10-01T00:00:00Z"],
      // Wrap around years.
      ["2012-07-09T23:35:00Z", "0 0 * Feb Mon", "2013-02-04T00:00:00Z"],
      ["2012-07-09T23:35:00Z", "0 0 * Feb Mon/2", "2013-02-01T00:00:00Z"],
      // Wrap around minute, hour, day, month, and year.
      ["2012-12-31T23:59:45Z", "* * * * *", "2013-01-01T00:00:00Z"],
      // Leap year.
      ["2012-07-09T23:35:00Z", "0 0 29 Feb ?", "2016-02-29T00:00:00Z"],
      // Daylight saving time 2am EST (-5) -> 3am EDT (-4).
      [
        "2012-03-11T00:00:00-05:00",
        "TZ=America/New_York 30 2 11 Mar ?",
        "2013-03-11T02:30:00-04:00",
      ],
      // Hourly job.
      [
        "2012-03-11T00:00:00-05:00",
        "TZ=America/New_York 0 * * * ?",
        "2012-03-11T01:00:00-05:00",
      ],
      [
        "2012-03-11T01:00:00-05:00",
        "TZ=America/New_York 0 * * * ?",
        "2012-03-11T03:00:00-04:00",
      ],
      [
        "2012-03-11T03:00:00-04:00",
        "TZ=America/New_York 0 * * * ?",
        "2012-03-11T04:00:00-04:00",
      ],
      [
        "2012-03-11T04:00:00-04:00",
        "TZ=America/New_York 0 * * * ?",
        "2012-03-11T05:00:00-04:00",
      ],
      // Hourly job using CRON_TZ.
      [
        "2012-03-11T00:00:00-05:00",
        "CRON_TZ=America/New_York 0 * * * ?",
        "2012-03-11T01:00:00-05:00",
      ],
      [
        "2012-03-11T01:00:00-05:00",
        "CRON_TZ=America/New_York 0 * * * ?",
        "2012-03-11T03:00:00-04:00",
      ],
      [
        "2012-03-11T03:00:00-04:00",
        "CRON_TZ=America/New_York 0 * * * ?",
        "2012-03-11T04:00:00-04:00",
      ],
      [
        "2012-03-11T04:00:00-04:00",
        "CRON_TZ=America/New_York 0 * * * ?",
        "2012-03-11T05:00:00-04:00",
      ],
      // 1am nightly job.
      [
        "2012-03-11T00:00:00-05:00",
        "TZ=America/New_York 0 1 * * ?",
        "2012-03-11T01:00:00-05:00",
      ],
      [
        "2012-03-11T01:00:00-05:00",
        "TZ=America/New_York 0 1 * * ?",
        "2012-03-12T01:00:00-04:00",
      ],
      // 2am nightly job (skipped).
      [
        "2012-03-11T00:00:00-05:00",
        "TZ=America/New_York 0 2 * * ?",
        "2012-03-12T02:00:00-04:00",
      ],
      // Daylight saving time 2am EDT (-4) -> 1am EST (-5).
      [
        "2012-11-04T00:00:00-04:00",
        "TZ=America/New_York 30 2 04 Nov ?",
        "2012-11-04T02:30:00-05:00",
      ],
      [
        "2012-11-04T01:45:00-04:00",
        "TZ=America/New_York 30 1 04 Nov ?",
        "2012-11-04T01:30:00-05:00",
      ],
      // Hourly job.
      [
        "2012-11-04T00:00:00-04:00",
        "TZ=America/New_York 0 * * * ?",
        "2012-11-04T01:00:00-04:00",
      ],
      [
        "2012-11-04T01:00:00-04:00",
        "TZ=America/New_York 0 * * * ?",
        "2012-11-04T01:00:00-05:00",
      ],
      [
        "2012-11-04T01:00:00-05:00",
        "TZ=America/New_York 0 * * * ?",
        "2012-11-04T02:00:00-05:00",
      ],
      // 1am nightly job (runs twice).
      [
        "2012-11-04T00:00:00-04:00",
        "TZ=America/New_York 0 1 * * ?",
        "2012-11-04T01:00:00-04:00",
      ],
      [
        "2012-11-04T01:00:00-04:00",
        "TZ=America/New_York 0 1 * * ?",
        "2012-11-04T01:00:00-05:00",
      ],
      [
        "2012-11-04T01:00:00-05:00",
        "TZ=America/New_York 0 1 * * ?",
        "2012-11-05T01:00:00-05:00",
      ],
      // 2am nightly job.
      [
        "2012-11-04T00:00:00-04:00",
        "TZ=America/New_York 0 2 * * ?",
        "2012-11-04T02:00:00-05:00",
      ],
      [
        "2012-11-04T02:00:00-05:00",
        "TZ=America/New_York 0 2 * * ?",
        "2012-11-05T02:00:00-05:00",
      ],
      // 3am nightly job.
      [
        "2012-11-04T00:00:00-04:00",
        "TZ=America/New_York 0 3 * * ?",
        "2012-11-04T03:00:00-05:00",
      ],
      [
        "2012-11-04T03:00:00-05:00",
        "TZ=America/New_York 0 3 * * ?",
        "2012-11-05T03:00:00-05:00",
      ],
      // The same jobs in the reference time's zone instead of a prefix.
      [
        "2012-11-04T00:00:00-04:00",
        "0 * * * ?",
        "2012-11-04T01:00:00-04:00",
        "America/New_York",
      ],
      [
        "2012-11-04T01:00:00-04:00",
        "0 * * * ?",
        "2012-11-04T01:00:00-05:00",
        "America/New_York",
      ],
      [
        "2012-11-04T01:00:00-05:00",
        "0 * * * ?",
        "2012-11-04T02:00:00-05:00",
        "America/New_York",
      ],
      [
        "2012-11-04T00:00:00-04:00",
        "0 1 * * ?",
        "2012-11-04T01:00:00-04:00",
        "America/New_York",
      ],
      [
        "2012-11-04T01:00:00-04:00",
        "0 1 * * ?",
        "2012-11-04T01:00:00-05:00",
        "America/New_York",
      ],
      [
        "2012-11-04T01:00:00-05:00",
        "0 1 * * ?",
        "2012-11-05T01:00:00-05:00",
        "America/New_York",
      ],
      [
        "2012-11-04T00:00:00-04:00",
        "0 2 * * ?",
        "2012-11-04T02:00:00-05:00",
        "America/New_York",
      ],
      [
        "2012-11-04T02:00:00-05:00",
        "0 2 * * ?",
        "2012-11-05T02:00:00-05:00",
        "America/New_York",
      ],
      [
        "2012-11-04T00:00:00-04:00",
        "0 3 * * ?",
        "2012-11-04T03:00:00-05:00",
        "America/New_York",
      ],
      [
        "2012-11-04T03:00:00-05:00",
        "0 3 * * ?",
        "2012-11-05T03:00:00-05:00",
        "America/New_York",
      ],
      // Unsatisfiable.
      ["2012-07-09T23:35:00Z", "0 0 30 Feb ?", null],
      ["2012-07-09T23:35:00Z", "0 0 31 Apr ?", null],
      // Monthly job.
      [
        "2012-11-04T00:00:00-04:00",
        "0 3 3 * ?",
        "2012-12-03T03:00:00-05:00",
        "America/New_York",
      ],
      // DST making midnight invalid (robfig issue 157).
      [
        "2018-10-17T05:00:00-04:00",
        "TZ=America/Sao_Paulo 0 9 10 * ?",
        "2018-11-10T06:00:00-05:00",
      ],
      [
        "2018-02-14T05:00:00-05:00",
        "TZ=America/Sao_Paulo 0 9 22 * ?",
        "2018-02-22T07:00:00-05:00",
      ],
      // The reference time's own fixed offset (robfig TestNextWithTz).
      ["2016-01-03T13:09:03+05:30", "14 14 * * *", "2016-01-03T14:14:00+05:30"],
      ["2016-01-03T04:09:03+05:30", "14 14 * * ?", "2016-01-03T14:14:00+05:30"],
      ["2016-01-03T14:09:03+05:30", "14 14 * * *", "2016-01-03T14:14:00+05:30"],
      ["2016-01-03T14:00:00+05:30", "14 14 * * ?", "2016-01-03T14:14:00+05:30"],
    ])("from %s, %j is next at %s", (from, expression, expected, timeZone) => {
      // Results are compared as instants in the reference time's offset.
      const next = nextAfter(expression, from, timeZone);
      if (expected === null) {
        expect(next).toBeNull();
      } else {
        expect(next === null ? null : Temporal.Instant.from(next)).toEqual(
          Temporal.Instant.from(expected)
        );
      }
    });

    // robfig's constantdelay_test.go, through `@every`.
    it.each<[string, string, string]>([
      ["2012-07-09T14:45:00Z", "15m50ns", "2012-07-09T15:00:00Z"],
      ["2012-07-09T14:59:00Z", "15m", "2012-07-09T15:14:00Z"],
      ["2012-07-09T14:59:59Z", "15m", "2012-07-09T15:14:59Z"],
      ["2012-07-09T15:45:00Z", "35m", "2012-07-09T16:20:00Z"],
      ["2012-07-09T23:46:00Z", "14m", "2012-07-10T00:00:00Z"],
      ["2012-07-09T23:45:00Z", "35m", "2012-07-10T00:20:00Z"],
      ["2012-07-09T23:35:51Z", "44m24s", "2012-07-10T00:20:15Z"],
      ["2012-07-09T23:35:51Z", "25h44m24s", "2012-07-11T01:20:15Z"],
      ["2012-07-09T23:35:00Z", "2184h25m", "2012-10-09T00:00:00Z"],
      ["2012-12-31T23:59:45Z", "15s", "2013-01-01T00:00:00Z"],
      ["2012-07-09T14:45:00Z", "15ms", "2012-07-09T14:45:01Z"],
      ["2012-07-09T14:45:00.005Z", "15m", "2012-07-09T15:00:00Z"],
      ["2012-07-09T14:45:00.005Z", "15m50ns", "2012-07-09T15:00:00Z"],
    ])("from %s, @every %s is next at %s", (from, duration, expected) => {
      expect(nextAfter(`@every ${duration}`, from)).toBe(expected);
    });
  });

  // Expectations generated with robfig/cron v3.0.1's `Next` in each zone.
  describe("daylight saving transitions", () => {
    it.each<[string, string, string, string[]]>([
      // Berlin springs forward from 02:00 to 03:00: 02:30 is skipped.
      [
        "30 2 * * *",
        "Europe/Berlin",
        "2026-03-28T12:00:00Z",
        [
          "2026-03-30T02:30:00+02:00",
          "2026-03-31T02:30:00+02:00",
          "2026-04-01T02:30:00+02:00",
          "2026-04-02T02:30:00+02:00",
        ],
      ],
      // Berlin falls back from 03:00 to 02:00: 02:30 runs twice.
      [
        "30 2 * * *",
        "Europe/Berlin",
        "2026-10-24T12:00:00Z",
        [
          "2026-10-25T02:30:00+02:00",
          "2026-10-25T02:30:00+01:00",
          "2026-10-26T02:30:00+01:00",
          "2026-10-27T02:30:00+01:00",
        ],
      ],
      [
        "30 1 * * *",
        "Europe/London",
        "2026-03-28T12:00:00Z",
        [
          "2026-03-30T01:30:00+01:00",
          "2026-03-31T01:30:00+01:00",
          "2026-04-01T01:30:00+01:00",
          "2026-04-02T01:30:00+01:00",
        ],
      ],
      [
        "30 1 * * *",
        "Europe/London",
        "2026-10-24T12:00:00Z",
        [
          "2026-10-25T01:30:00+01:00",
          "2026-10-25T01:30:00+00:00",
          "2026-10-26T01:30:00+00:00",
          "2026-10-27T01:30:00+00:00",
        ],
      ],
      // Lord Howe shifts by half an hour.
      [
        "15 2 * * *",
        "Australia/Lord_Howe",
        "2026-10-03T00:00:00Z",
        [
          "2026-10-05T02:15:00+11:00",
          "2026-10-06T02:15:00+11:00",
          "2026-10-07T02:15:00+11:00",
          "2026-10-08T02:15:00+11:00",
        ],
      ],
      [
        "*/20 1-2 * * *",
        "Australia/Lord_Howe",
        "2026-04-04T12:00:00Z",
        [
          "2026-04-05T01:00:00+11:00",
          "2026-04-05T01:20:00+11:00",
          "2026-04-05T01:40:00+11:00",
          "2026-04-05T01:40:00+10:30",
        ],
      ],
      // Midnight does not exist on 2018-11-04 in São Paulo.
      [
        "0 0 * * *",
        "America/Sao_Paulo",
        "2018-11-02T12:00:00Z",
        [
          "2018-11-03T00:00:00-03:00",
          "2018-11-05T00:00:00-02:00",
          "2018-11-06T00:00:00-02:00",
          "2018-11-07T00:00:00-02:00",
        ],
      ],
      [
        "0 * * * *",
        "America/New_York",
        "2026-03-08T05:30:00Z",
        [
          "2026-03-08T01:00:00-05:00",
          "2026-03-08T03:00:00-04:00",
          "2026-03-08T04:00:00-04:00",
          "2026-03-08T05:00:00-04:00",
        ],
      ],
      [
        "30 * * * *",
        "America/New_York",
        "2026-11-01T04:00:00Z",
        [
          "2026-11-01T00:30:00-04:00",
          "2026-11-01T01:30:00-04:00",
          "2026-11-01T01:30:00-05:00",
          "2026-11-01T02:30:00-05:00",
        ],
      ],
      // Samoa skipped 2011-12-30 entirely.
      [
        "0 0 * * *",
        "Pacific/Apia",
        "2011-12-27T00:00:00Z",
        [
          "2011-12-27T00:00:00-10:00",
          "2011-12-28T00:00:00-10:00",
          "2011-12-29T00:00:00-10:00",
          "2011-12-31T00:00:00+14:00",
        ],
      ],
    ])("%j in %s from %s", (expression, timeZone, from, expected) => {
      const schedule = cron(expression, { timeZone });
      expect(
        occurrences(schedule, Temporal.Instant.from(from), 4).map((next) =>
          next.toZonedDateTimeISO(timeZone).toString({ timeZoneName: "never" })
        )
      ).toEqual(expected);
    });

    it("fails where robfig would never return across a skipped day", () => {
      // robfig's day loop resolves the missing 2011-12-30 midnight back to
      // the 29th and spins forever.
      const schedule = cron("0 0 31 * *", { timeZone: "Pacific/Apia" });
      const error = captureError(() =>
        schedule.next(Temporal.Instant.from("2011-12-27T00:00:00Z"))
      );
      expect(error).toBeInstanceOf(ConfigurationError);
      expect(error).toMatchObject({
        details: { day: "2011-12-30", timeZone: "Pacific/Apia" },
      });
    });
  });

  describe("time zones", () => {
    const from = Temporal.Instant.from("2026-03-07T13:00:00Z");

    it("defaults to the process's local time zone", () => {
      const schedule = cron("0 9 * * *");
      expect(schedule.timeZone).toBe(Temporal.Now.timeZoneId());
      expect(cron("CRON_TZ=Local 0 9 * * *").timeZone).toBe(
        Temporal.Now.timeZoneId()
      );
    });

    it("uses the timeZone option for unprefixed expressions", () => {
      const schedule = cron("0 9 * * *", { timeZone: "America/New_York" });
      expect(schedule.timeZone).toBe("America/New_York");
      expect(schedule.next(from)?.toString()).toBe("2026-03-07T14:00:00Z");
      expect(
        cron("0 9 * * *", { timeZone: "+05:30" }).next(from)?.toString()
      ).toBe("2026-03-08T03:30:00Z");
      // Temporal normalizes the case of IANA names in the option.
      expect(cron("0 9 * * *", { timeZone: "america/chicago" }).timeZone).toBe(
        "America/Chicago"
      );
    });

    it("prefers a CRON_TZ prefix, where Local defers to the option", () => {
      const options = { timeZone: "America/New_York" };
      const utc = cron("CRON_TZ=UTC 0 9 * * *", options);
      expect(utc.timeZone).toBe("UTC");
      expect(utc.next(from)?.toString()).toBe("2026-03-08T09:00:00Z");
      expect(cron("TZ= 0 9 * * *", options).timeZone).toBe("UTC");
      expect(cron("TZ=Asia/Tokyo 0 9 * * *", options).timeZone).toBe(
        "Asia/Tokyo"
      );
      expect(cron("CRON_TZ=Local 0 9 * * *", options).timeZone).toBe(
        "America/New_York"
      );
    });

    it.each([
      // Go's zoneinfo lookup is case-sensitive on Linux.
      "CRON_TZ=america/new_york 0 9 * * *",
      "CRON_TZ=utc 0 9 * * *",
      // Temporal accepts these as time zones; Go does not.
      "CRON_TZ=+05:00 0 9 * * *",
      "CRON_TZ=2020-01-01[UTC] 0 9 * * *",
      "CRON_TZ=Nowhere/Invalid 0 9 * * *",
      // robfig panics without a space after the prefix.
      "CRON_TZ=UTC",
      "TZ=UTC\t0 9 * * *",
      "CRON_TZ=UTC ",
    ])("rejects %j", (expression) => {
      expect(() => cron(expression)).toThrow(ConfigurationError);
    });

    it("rejects an unknown timeZone option", () => {
      expect(() => cron("0 9 * * *", { timeZone: "Nowhere/Invalid" })).toThrow(
        'cron timeZone "Nowhere/Invalid" is not a known time zone'
      );
    });
  });

  describe("syntax edge cases", () => {
    const from = "2026-01-02T03:04:05Z";

    it("splits and trims on Go's whitespace, not JavaScript's", () => {
      expect(nextAfter("0\u00859 * *\u3000*", from)).toBe(
        "2026-01-02T09:00:00Z"
      );
      expect(() => cron("0\ufeff 9 * * * *")).toThrow(ConfigurationError);
      expect(() => cron(" @daily")).toThrow("expected exactly 5 fields");
      expect(() => cron("@daily ")).toThrow("unrecognized descriptor");
      expect(cron("CRON_TZ=UTC \u00a0@daily\u2003").timeZone).toBe("UTC");
    });

    it("parses integers with Go's strconv.Atoi", () => {
      expect(nextAfter("+5 * * * *", from)).toBe("2026-01-02T03:05:00Z");
      expect(nextAfter("007 * * * *", from)).toBe("2026-01-02T03:07:00Z");
      // A huge step selects only the start.
      expect(nextAfter("0/99999999999 * * * *", from)).toBe(
        "2026-01-02T04:00:00Z"
      );
      expect(() => cron("0/9223372036854775808 * * * *")).toThrow(
        "value out of range"
      );
      expect(() => cron("0/1_0 * * * *")).toThrow("invalid syntax");
      expect(() => cron("\uff15 * * * *")).toThrow("invalid syntax");
    });

    it("lowercases names like Go", () => {
      // Go's `strings.ToLower` maps U+0130 to "i".
      expect(nextAfter("0 9 * * FR\u0130", from)).toBe("2026-01-02T09:00:00Z");
      expect(nextAfter("0 9 1 MAY *", from)).toBe("2026-05-01T09:00:00Z");
      expect(() => cron("0 9 mon * *")).toThrow("failed to parse int from");
    });

    it("skips empty list items and never fires an empty field", () => {
      expect(nextAfter("1,,2 * * * *", from)).toBe("2026-01-02T04:01:00Z");
      expect(nextAfter(", * * * *", from)).toBeNull();
      expect(nextAfter("0 9 * * ,", from)).toBeNull();
      expect(nextAfter("0 9 , * 1", from)).toBe("2026-01-05T09:00:00Z");
    });

    it("rounds @every like robfig", () => {
      expect(nextAfter("@every -5m", from)).toBe("2026-01-02T03:04:06Z");
      expect(nextAfter("@every 0", from)).toBe("2026-01-02T03:04:06Z");
      expect(nextAfter("@every 1.9999999999s", from)).toBe(
        "2026-01-02T03:04:06Z"
      );
      expect(() => cron("@every")).toThrow("unrecognized descriptor");
      expect(() => cron("@every 1d")).toThrow(
        'failed to parse duration @every 1d: time: unknown unit "d" in duration "1d"'
      );
    });

    it("stops before Temporal's representable range ends", () => {
      const latest =
        Temporal.Instant.fromEpochNanoseconds(8_640_000_000_000_000_000_000n);
      expect(cron("@daily", { timeZone: "UTC" }).next(latest)).toBeNull();
      expect(cron("@every 1s").next(latest)).toBeNull();
      expect(
        cron("@every 2562047h").next(
          Temporal.Instant.from("+275700-01-01T00:00:00Z")
        )
      ).toBeNull();
    });
  });

  describe("API", () => {
    it("returns a frozen schedule that describes itself", () => {
      const schedule = cron("0 9 * * 1", { timeZone: "UTC" });
      expect(Object.isFrozen(schedule)).toBe(true);
      expect(schedule).toMatchObject({
        expression: "0 9 * * 1",
        timeZone: "UTC",
      });
    });

    it("reports invalid expressions as configuration errors", () => {
      const error = captureError(() => cron("0 9 * * 7"));
      expect(error).toBeInstanceOf(ConfigurationError);
      expect(error).toMatchObject({
        details: { expression: "0 9 * * 7" },
        message:
          'invalid cron expression "0 9 * * 7": end of range (7) above maximum (6): 7',
      });
      expect((error as ConfigurationError).cause).toBeInstanceOf(Error);
    });

    it("rejects non-string expressions and non-object options", () => {
      expect(() => cron(9 as unknown as string)).toThrow(
        "cron expression must be a string"
      );
      expect(() => cron("0 9 * * *", null as unknown as undefined)).toThrow(
        "cron options must be an object"
      );
    });

    it("schedules a periodic job", () => {
      const schedule = cron("0 9 * * mon-fri", { timeZone: "UTC" });
      const job = periodicJob({
        args: { scope: "all" },
        job: defineJob<{ scope: string }>()({ kind: "cron_report" }),
        schedule,
      });
      expect(job.schedule).toBe(schedule);
    });
  });
});

function captureError(callback: () => unknown): unknown {
  try {
    callback();
  } catch (error: unknown) {
    return error;
  }
  throw new Error("expected an error");
}
