import fc from "fast-check";
import { describe, expect, it } from "vitest";

import { cron } from "./cron.js";

/**
 * Zones whose transitions skip or repeat whole wall-clock hours away from
 * midnight, plus fixed and fractional offsets.
 */
const HOUR_ALIGNED_TIME_ZONES = [
  "America/New_York",
  "America/St_Johns",
  "Asia/Kolkata",
  "Europe/Berlin",
  "Europe/London",
  "UTC",
  "+05:45",
] as const;

/**
 * Zones that shift by half an hour, at a quarter past, or at midnight. Here
 * robfig's hour and minute loops can step across a skipped wall time without
 * checking earlier fields again, so River Go may fire at a time that does not
 * match its own expression, such as 02:40 for hours `1,3` on Lord Howe
 * Island. The port keeps that behavior.
 */
const IRREGULAR_TIME_ZONES = [
  "America/Sao_Paulo",
  "Australia/Lord_Howe",
  "Pacific/Chatham",
] as const;

const MONTH_NAMES = ["jan", "FEB", "Mar", "jun", "Oct", "dec"] as const;
const WEEKDAY_NAMES = ["sun", "MON", "Tue", "fri", "sat"] as const;

/** One list item: a value or range, optionally stepped, or a wildcard. */
function rangeExpression(
  minimum: number,
  maximum: number,
  names: readonly string[] = []
): fc.Arbitrary<string> {
  const value = fc.integer({ max: maximum, min: minimum });
  const bound =
    names.length === 0
      ? value.map(String)
      : fc.oneof(value.map(String), fc.constantFrom(...names));
  const step = fc.option(fc.integer({ max: maximum + 1, min: 1 }), {
    nil: undefined,
  });
  const base = fc.oneof(
    fc.constantFrom("*", "?"),
    bound,
    fc
      .tuple(value, value)
      .map(([low, high]) => `${Math.min(low, high)}-${Math.max(low, high)}`)
  );
  return fc
    .tuple(base, step)
    .map(([range, every]) =>
      every === undefined ? range : `${range}/${every}`
    );
}

function field(
  minimum: number,
  maximum: number,
  names: readonly string[] = []
): fc.Arbitrary<string> {
  return fc
    .array(rangeExpression(minimum, maximum, names), {
      maxLength: 3,
      minLength: 1,
    })
    .map((items) => items.join(","));
}

/** Valid five-field expressions River Go accepts. */
const fieldsExpression = fc
  .tuple(
    field(0, 59),
    field(0, 23),
    fc.oneof(fc.constant("*"), field(1, 31)),
    fc.oneof(fc.constant("*"), field(1, 12, MONTH_NAMES)),
    fc.oneof(fc.constant("*"), field(0, 6, WEEKDAY_NAMES))
  )
  .map((fields) => fields.join(" "));

/** Valid expressions, including descriptors. */
const expression = fc.oneof(
  { arbitrary: fieldsExpression, weight: 4 },
  {
    arbitrary: fc.constantFrom(
      "@yearly",
      "@monthly",
      "@weekly",
      "@daily",
      "@hourly",
      "@every 1h30m",
      "@every 90s",
      "@every 1.5s"
    ),
    weight: 1,
  }
);

/** Instants from 2000 to 2040 with arbitrary sub-second parts. */
const instant = fc
  .bigInt({ max: 2_208_988_800_000_000_000n, min: 946_684_800_000_000_000n })
  .map((nanoseconds) => Temporal.Instant.fromEpochNanoseconds(nanoseconds));

describe("cron properties", () => {
  it("returns strictly increasing whole-second occurrences", () => {
    fc.assert(
      fc.property(
        expression,
        fc.constantFrom(...HOUR_ALIGNED_TIME_ZONES, ...IRREGULAR_TIME_ZONES),
        instant,
        (text, timeZone, from) => {
          const schedule = cron(text, { timeZone });
          let previous = from;
          for (let index = 0; index < 4; index++) {
            const next = schedule.next(previous);
            if (next === null) break;
            expect(Temporal.Instant.compare(next, previous)).toBe(1);
            expect(next.epochNanoseconds % 1_000_000_000n).toBe(0n);
            previous = next;
          }
        }
      ),
      { numRuns: 300 }
    );
  });

  it("finds each occurrence again from just before it", () => {
    fc.assert(
      fc.property(
        fieldsExpression,
        fc.constantFrom(...HOUR_ALIGNED_TIME_ZONES),
        instant,
        (text, timeZone, from) => {
          const schedule = cron(text, { timeZone });
          let previous = from;
          for (let index = 0; index < 4; index++) {
            const next = schedule.next(previous);
            if (next === null) break;
            expect(
              schedule.next(next.subtract({ nanoseconds: 1 }))?.equals(next)
            ).toBe(true);
            previous = next;
          }
        }
      ),
      { numRuns: 300 }
    );
  });
});
