import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";

import fc from "fast-check";
import { describe, expect, it } from "vitest";

import { buildUniqueKey } from "./client.js";
import { ValidationError } from "./errors.js";
import type { UniqueOptions } from "./insert-options.js";
import { exactJsonNumber, isExactJsonNumber, toJsonObject } from "./json.js";
import type { JsonObject, JsonValue } from "./json.js";

// Go's time.Truncate aligns periods to 0001-01-01T00:00:00Z, not to the Unix
// epoch, so periods that do not divide a day still match Go exactly.
const YEAR_ONE_TO_UNIX_EPOCH_NS = 62_135_596_800n * 1_000_000_000n;
const SECOND_NS = 1_000_000_000n;

// Selected path tests use plain segments; arbitrary top-level argument keys
// are tested separately, including gjson/sjson path punctuation.
const hashableKey = (key: string): boolean =>
  key.length > 0 && !key.startsWith(":") && !/[.*?|#@\\]/.test(key);

// A key usable as a selected path segment: also not an array index.
const selectableKey = (key: string): boolean =>
  hashableKey(key) && !/^[0-9]+$/.test(key) && key !== "-1";

const keyArbitrary = fc.oneof(
  fc.constantFrom(
    "__proto__",
    "a",
    "b",
    "id",
    "10",
    "2",
    "\u00e9",
    "\u{1f600}"
  ),
  fc.string({ maxLength: 5 }).filter(hashableKey)
);

const leafArbitrary: fc.Arbitrary<JsonValue> = fc.oneof(
  fc.constant(null),
  fc.boolean(),
  fc.integer(),
  fc.double({ max: 1e6, min: -1e6, noNaN: true }),
  fc
    .bigInt({ max: 2n ** 63n - 1n, min: -(2n ** 63n) })
    .map((value) => exactJsonNumber(value.toString())),
  fc.string({ maxLength: 6 }),
  fc.constantFrom("<&>", "\u2028", "\u{1f600}")
);

function objectFromEntries(
  entries: readonly (readonly [string, JsonValue])[]
): JsonObject {
  const result = Object.create(null) as JsonObject;
  for (const [key, value] of entries) result[key] = value;
  return result;
}

const { object: argsArbitrary } = fc.letrec<{
  json: JsonValue;
  object: JsonObject;
}>((tie) => ({
  json: fc.oneof(
    { depthSize: "small", withCrossShrink: true },
    leafArbitrary,
    fc.array(tie("json"), { maxLength: 3 }),
    tie("object")
  ),
  object: fc
    .uniqueArray(fc.tuple(keyArbitrary, tie("json")), {
      maxLength: 5,
      selector: ([key]) => key,
    })
    .map(objectFromEntries),
}));

const instantArbitrary = fc
  .bigInt({
    // 0001-01-01 through 9999-12-31, the RFC 3339 range Go formats.
    max: 253_402_300_799n * SECOND_NS,
    min: -YEAR_ONE_TO_UNIX_EPOCH_NS,
  })
  .map((nanoseconds) => Temporal.Instant.fromEpochNanoseconds(nanoseconds));

// Periods from one second to ten days, including ones that do not divide a
// minute, an hour, or a day, and sub-second remainders.
const periodArbitrary = fc.oneof(
  fc
    .constantFrom(1n, 60n, 3_600n, 86_400n, 7n, 90n, 5_400n, 100_000n)
    .map((seconds) => seconds * SECOND_NS),
  fc.bigInt({ max: 864_000n * SECOND_NS, min: SECOND_NS })
);

const compareUtf8 = (left: string, right: string) =>
  Buffer.compare(Buffer.from(left), Buffer.from(right));

/** Go's HTML-safe string escaping. */
function goString(text: string): string {
  return JSON.stringify(text).replace(
    /[<>&\u2028\u2029]/g,
    (character) => `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`
  );
}

/** How sjson writes a key: verbatim when printable ASCII without `"` or `\`. */
function sjsonKey(key: string): string {
  return /^[\x20-\x7f]*$/.test(key) && !/["\\]/.test(key)
    ? `"${key}"`
    : goString(key);
}

/**
 * Reference encoder for River's unique args: top-level keys sorted by UTF-8
 * bytes (as Go sorts `@keys`) and written by sjson, nested objects in their
 * original order with encoding/json keys.
 */
function referenceArgs(value: JsonValue, sortKeys: boolean): string {
  if (value === null || typeof value === "boolean") return String(value);
  if (typeof value === "string") return goString(value);
  if (typeof value === "number") {
    return Object.is(value, -0) ? "-0" : JSON.stringify(value);
  }
  if (isExactJsonNumber(value)) return value.rawJSON;
  if (Array.isArray(value)) {
    return `[${value.map((item) => referenceArgs(item, false)).join(",")}]`;
  }
  const keys = Object.keys(value);
  if (sortKeys) keys.sort(compareUtf8);
  const writeKey = sortKeys ? sjsonKey : goString;
  return `{${keys
    .map(
      (key) => `${writeKey(key)}:${referenceArgs(value[key] ?? null, false)}`
    )
    .join(",")}}`;
}

/**
 * Reference encoder for a selection: River's assembled objects keep sorted
 * path order and sjson keys, selected values keep their own encoding.
 */
function referenceSelected(
  value: JsonObject,
  assembled: ReadonlySet<object>
): string {
  return `{${Object.keys(value)
    .map((key) => {
      const child = value[key] ?? null;
      return `${sjsonKey(key)}:${
        typeof child === "object" && child !== null && assembled.has(child)
          ? referenceSelected(child as JsonObject, assembled)
          : referenceArgs(child, false)
      }`;
    })
    .join(",")}}`;
}

/** Reference selection of dotted byArgs paths, set in sorted path order. */
function referenceSelection(
  args: JsonObject,
  paths: readonly string[],
  assembled: Set<object>
): JsonObject {
  const selected = Object.create(null) as JsonObject;
  assembled.add(selected);
  const chosen: string[] = [];
  for (const path of [...new Set(paths)].sort(compareUtf8)) {
    if (chosen.some((prefix) => path.startsWith(`${prefix}.`))) continue;
    const segments = path.split(".");
    let value: JsonValue | undefined = args;
    for (const segment of segments) {
      value =
        value !== null &&
        typeof value === "object" &&
        !Array.isArray(value) &&
        !isExactJsonNumber(value) &&
        Object.hasOwn(value, segment)
          ? value[segment]
          : undefined;
    }
    if (value === undefined) continue;
    let target = selected;
    for (const segment of segments.slice(0, -1)) {
      if (target[segment] === undefined) {
        const child = Object.create(null) as JsonObject;
        assembled.add(child);
        target[segment] = child;
      }
      target = target[segment] as JsonObject;
    }
    target[segments.at(-1) as string] = value;
    chosen.push(path);
  }
  return selected;
}

function referencePeriodLabel(
  scheduledAt: Temporal.Instant,
  periodNanoseconds: bigint
): string {
  const sinceYearOne = scheduledAt.epochNanoseconds + YEAR_ONE_TO_UNIX_EPOCH_NS;
  // Every representable instant is after year one, so plain division floors.
  const start =
    (sinceYearOne / periodNanoseconds) * periodNanoseconds -
    YEAR_ONE_TO_UNIX_EPOCH_NS;
  // RFC 3339 in UTC at second precision, as Go's time.RFC3339 layout.
  return Temporal.Instant.fromEpochNanoseconds(start).toString({
    smallestUnit: "second",
  });
}

interface KeyInput {
  readonly args: JsonObject;
  readonly kind: string;
  readonly queue: string;
  readonly scheduledAt: Temporal.Instant;
}

function keyHex(input: KeyInput, options: UniqueOptions): string {
  const [key] = buildUniqueKey(input, options);
  return Buffer.from(key).toString("hex");
}

function referenceKeyHex(
  input: KeyInput,
  options: UniqueOptions & { readonly periodNanoseconds?: bigint }
): string {
  let source = "";
  if (options.excludeKind !== true) source += `&kind=${input.kind}`;
  if (options.byArgs === true) {
    source += `&args=${referenceArgs(input.args, true)}`;
  } else if (options.byArgs !== undefined) {
    const assembled = new Set<object>();
    const selection = referenceSelection(input.args, options.byArgs, assembled);
    source += `&args=${
      Object.keys(selection).length === 0
        ? ""
        : referenceSelected(selection, assembled)
    }`;
  }
  if (options.periodNanoseconds !== undefined) {
    source += `&period=${referencePeriodLabel(
      input.scheduledAt,
      options.periodNanoseconds
    )}`;
  }
  if (options.byQueue === true) source += `&queue=${input.queue}`;
  return createHash("sha256").update(source).digest("hex");
}

function period(nanoseconds: bigint): Temporal.Duration {
  return Temporal.Duration.from({
    nanoseconds: Number(nanoseconds % 1_000n),
    microseconds: Number((nanoseconds / 1_000n) % 1_000n),
    milliseconds: Number((nanoseconds / 1_000_000n) % 1_000n),
    seconds: Number(nanoseconds / SECOND_NS),
  });
}

/**
 * Every valid dotted byArgs path to a value in `args`, one level of nesting
 * deep. River rejects paths with an empty or array-index segment.
 */
function pathsOf(args: JsonObject): string[] {
  return Object.entries(args)
    .flatMap(([key, value]) =>
      value !== null &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      !isExactJsonNumber(value)
        ? [key, ...Object.keys(value).map((child) => `${key}.${child}`)]
        : [key]
    )
    .filter((path) => path.split(".").every(selectableKey));
}

const inputArbitrary: fc.Arbitrary<KeyInput> = fc.record({
  args: argsArbitrary,
  kind: fc.constantFrom("email", "sync_account", "a<b>"),
  queue: fc.constantFrom("default", "critical", "q\u00e9"),
  scheduledAt: instantArbitrary,
});

describe("unique key properties", () => {
  it("matches the reference key for every option combination", () => {
    fc.assert(
      fc.property(
        inputArbitrary,
        fc.boolean(),
        fc.boolean(),
        fc.option(periodArbitrary, { nil: undefined }),
        fc.option(
          fc.oneof(
            fc.constant(true as const),
            fc.array(fc.string({ maxLength: 3 }).filter(selectableKey), {
              minLength: 1,
            })
          ),
          { nil: undefined }
        ),
        (input, excludeKind, byQueue, periodNanoseconds, byArgsChoice) => {
          const byArgs =
            byArgsChoice === true || byArgsChoice === undefined
              ? byArgsChoice
              : [...byArgsChoice, ...pathsOf(input.args)];
          const options: UniqueOptions = {
            byQueue,
            excludeKind,
            ...(byArgs === undefined ||
            (Array.isArray(byArgs) && byArgs.length === 0)
              ? {}
              : { byArgs }),
            ...(periodNanoseconds === undefined
              ? {}
              : { byPeriod: period(periodNanoseconds) }),
          };
          // Like Go, a key without the kind needs another dimension.
          if (
            excludeKind &&
            !byQueue &&
            options.byArgs === undefined &&
            periodNanoseconds === undefined
          ) {
            expect(() => keyHex(input, options)).toThrow(
              "unique.excludeKind requires byArgs, byQueue, or byPeriod"
            );
            return;
          }
          const expected = referenceKeyHex(input, {
            ...options,
            ...(periodNanoseconds === undefined ? {} : { periodNanoseconds }),
          });
          expect(keyHex(input, options)).toBe(expected);
          // Deterministic for an identical, independently copied input.
          expect(
            keyHex({ ...input, args: toJsonObject(input.args) }, options)
          ).toBe(expected);
        }
      ),
      { numRuns: 400 }
    );
  });

  it("ignores top-level key order but not nested key order", () => {
    // JavaScript enumerates integer-like keys first whatever their insertion
    // order, so only other keys can carry a distinct nested order.
    const nestedArbitrary = fc
      .uniqueArray(
        fc.tuple(
          keyArbitrary.filter((key) => !/^(?:0|[1-9]\d*)$/.test(key)),
          leafArbitrary
        ),
        {
          minLength: 2,
          maxLength: 4,
          selector: ([key]) => key,
        }
      )
      .map(objectFromEntries);
    fc.assert(
      fc.property(
        inputArbitrary,
        keyArbitrary.filter(selectableKey),
        nestedArbitrary,
        (input, nestedKey, nested) => {
          const args = objectFromEntries([
            ...Object.entries(input.args).filter(([key]) => key !== nestedKey),
            [nestedKey, nested],
          ]);
          const topReversed = objectFromEntries(Object.entries(args).reverse());
          const nestedReversed = objectFromEntries([
            ...Object.entries(args).filter(([key]) => key !== nestedKey),
            [nestedKey, objectFromEntries(Object.entries(nested).reverse())],
          ]);
          const keyFor = (
            value: JsonObject,
            byArgs: NonNullable<UniqueOptions["byArgs"]>
          ) => keyHex({ ...input, args: value }, { byArgs });

          for (const byArgs of [true, [nestedKey]] as const) {
            expect(keyFor(topReversed, byArgs)).toBe(keyFor(args, byArgs));
            expect(keyFor(nestedReversed, byArgs)).not.toBe(
              keyFor(args, byArgs)
            );
          }
        }
      ),
      { numRuns: 300 }
    );
  });

  it("depends only on the selected byArgs paths, in any order", () => {
    fc.assert(
      fc.property(
        inputArbitrary,
        fc.nat(),
        leafArbitrary,
        (input, seed, replacement) => {
          const paths = pathsOf(input.args);
          fc.pre(paths.length > 0);
          const selectedPaths = paths.filter((_, index) => (seed >> index) & 1);
          fc.pre(selectedPaths.length > 0);
          const shuffled = [...selectedPaths].reverse();
          const duplicated = [...selectedPaths, ...selectedPaths];
          const base = keyHex(input, { byArgs: selectedPaths });
          expect(keyHex(input, { byArgs: shuffled })).toBe(base);
          expect(keyHex(input, { byArgs: duplicated })).toBe(base);
          expect(
            keyHex(input, { byArgs: [...selectedPaths, "missing\u0000field"] })
          ).toBe(base);

          // Changing an unselected top-level field leaves the key alone;
          // changing a selected one does not.
          const topLevel = Object.keys(input.args);
          const unselected = topLevel.find(
            (key) =>
              !selectedPaths.some(
                (path) => path === key || path.startsWith(`${key}.`)
              )
          );
          if (unselected !== undefined) {
            const changed = objectFromEntries([
              ...Object.entries(input.args),
              [unselected, [replacement]],
            ]);
            expect(
              keyHex({ ...input, args: changed }, { byArgs: selectedPaths })
            ).toBe(base);
          }
          const selectedTop = selectedPaths.find((path) => !path.includes("."));
          if (selectedTop !== undefined) {
            const changed = objectFromEntries([
              ...Object.entries(input.args),
              [selectedTop, [input.args[selectedTop] ?? null]],
            ]);
            expect(
              keyHex({ ...input, args: changed }, { byArgs: selectedPaths })
            ).not.toBe(base);
          }
        }
      ),
      { numRuns: 300 }
    );
  });

  it("hashes all top-level keys literally and rejects malformed paths", () => {
    const syntaxKey = fc
      .tuple(
        fc.string({ maxLength: 3 }),
        fc.constantFrom(".", "*", "?", "|", "#", "@", "\\"),
        fc.string({ maxLength: 3 })
      )
      .map(([before, syntax, after]) => `${before}${syntax}${after}`);
    fc.assert(
      fc.property(inputArbitrary, syntaxKey, (input, key) => {
        const args = objectFromEntries([
          ...Object.entries(input.args),
          [key, 1],
        ]);
        expect(keyHex({ ...input, args }, { byArgs: true })).toBe(
          referenceKeyHex({ ...input, args }, { byArgs: true })
        );
        // Nested keys are hashed verbatim and stay unrestricted.
        expect(() =>
          buildUniqueKey(
            { ...input, args: objectFromEntries([["nested", args]]) },
            { byArgs: true }
          )
        ).not.toThrow();
      }),
      { numRuns: 200 }
    );
    for (const path of ["", "a..b", "a\\"]) {
      expect(() =>
        buildUniqueKey(
          {
            args: objectFromEntries([["a", 1]]),
            kind: "email",
            queue: "default",
            scheduledAt: Temporal.Instant.fromEpochMilliseconds(0),
          },
          { byArgs: [path] }
        )
      ).toThrow(ValidationError);
    }
  });

  it("rejects any selected path with a segment Go reads as an array index", () => {
    // sjson builds a JSON array for an unsigned integer or `-1` segment,
    // escaped or not, so River rejects the path rather than hash an object.
    const indexSegment = fc.oneof(
      fc.nat().map(String),
      fc.stringMatching(/^[0-9]{1,24}$/),
      fc.constant("-1")
    );
    const plainSegments = fc.array(
      fc.string({ maxLength: 3 }).filter(selectableKey),
      { maxLength: 2 }
    );
    fc.assert(
      fc.property(
        inputArbitrary,
        plainSegments,
        indexSegment,
        fc.boolean(),
        plainSegments,
        (input, before, index, escape, after) => {
          const segment = escape ? `\\${index}` : index;
          const path = [...before, segment, ...after].join(".");
          expect(() => buildUniqueKey(input, { byArgs: [path] })).toThrow(
            ValidationError
          );
          expect(() =>
            buildUniqueKey(input, { byArgs: [...pathsOf(input.args), path] })
          ).toThrow(ValidationError);
        }
      ),
      { numRuns: 300 }
    );
  });

  it("shares one key per Go-aligned period window, labelled in UTC", () => {
    fc.assert(
      fc.property(
        instantArbitrary,
        periodArbitrary,
        fc.bigInt({ max: 10n ** 18n, min: 0n }),
        (scheduledAt, periodNanoseconds, offsetSeed) => {
          const input = {
            args: Object.create(null) as JsonObject,
            kind: "periodic",
            queue: "default",
            scheduledAt,
          };
          const options = { byPeriod: period(periodNanoseconds) };
          const key = keyHex(input, options);
          expect(key).toBe(
            referenceKeyHex(input, { ...options, periodNanoseconds })
          );

          const sinceYearOne =
            scheduledAt.epochNanoseconds + YEAR_ONE_TO_UNIX_EPOCH_NS;
          const start =
            sinceYearOne -
            (sinceYearOne % periodNanoseconds) -
            YEAR_ONE_TO_UNIX_EPOCH_NS;
          const inWindow = start + (offsetSeed % periodNanoseconds);
          expect(
            keyHex(
              {
                ...input,
                scheduledAt: Temporal.Instant.fromEpochNanoseconds(inWindow),
              },
              options
            )
          ).toBe(key);
          if (start - 1n >= -YEAR_ONE_TO_UNIX_EPOCH_NS) {
            expect(
              keyHex(
                {
                  ...input,
                  scheduledAt: Temporal.Instant.fromEpochNanoseconds(
                    start - 1n
                  ),
                },
                options
              )
            ).not.toBe(key);
          }
        }
      ),
      { numRuns: 400 }
    );
  });
});
