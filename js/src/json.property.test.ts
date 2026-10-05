import fc from "fast-check";
import { describe, expect, it } from "vitest";

import {
  exactJsonNumber,
  isExactJsonNumber,
  jsonNumberToBigInt,
  jsonValuesEqual,
  JsonValueError,
  parseJson,
  stringifyJson,
  toJsonObject,
  toJsonValue,
} from "./json.js";
import type { JsonValue } from "./json.js";

const INT8_MAX = 9_223_372_036_854_775_807n;
const INT8_MIN = -9_223_372_036_854_775_808n;
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

// Keys that look like prototype members, identifiers, numbers, and non-BMP
// text, so paths, ordering, and prototype handling all get exercised.
const keyArbitrary = fc.oneof(
  { weight: 1, arbitrary: fc.constantFrom("__proto__", "constructor", "") },
  { weight: 1, arbitrary: fc.constantFrom("prototype", "toString", "10", "2") },
  { weight: 4, arbitrary: fc.string({ maxLength: 6 }) },
  { weight: 2, arbitrary: fc.string({ maxLength: 4, unit: "binary" }) }
);

const numberArbitrary: fc.Arbitrary<JsonValue> = fc.oneof(
  fc.integer(),
  fc.maxSafeInteger(),
  fc
    .double({ noDefaultInfinity: true, noNaN: true })
    .filter((value) => !Number.isInteger(value) || Number.isSafeInteger(value)),
  fc
    .bigInt({ max: INT8_MAX, min: INT8_MIN })
    .map((value) => exactJsonNumber(value.toString(10)))
);

const leafArbitrary: fc.Arbitrary<JsonValue> = fc.oneof(
  fc.constant(null),
  fc.boolean(),
  numberArbitrary,
  fc.string({ maxLength: 8 }),
  fc.string({ maxLength: 6, unit: "binary" }),
  fc.constantFrom("<&>", "\u2028\u2029", "\u{1f600}")
);

/** Plain objects built with own data properties, even for `__proto__`. */
function objectFromEntries(
  entries: readonly (readonly [string, JsonValue])[],
  nullPrototype: boolean
): Record<string, JsonValue> {
  const result = (nullPrototype ? Object.create(null) : {}) as Record<
    string,
    JsonValue
  >;
  for (const [key, value] of entries) {
    Object.defineProperty(result, key, {
      configurable: true,
      enumerable: true,
      value,
      writable: true,
    });
  }
  return result;
}

const { json: jsonArbitrary } = fc.letrec<{
  array: JsonValue[];
  json: JsonValue;
  object: Record<string, JsonValue>;
}>((tie) => ({
  array: fc.array(tie("json"), { maxLength: 4 }),
  json: fc.oneof(
    { depthSize: "small", withCrossShrink: true },
    leafArbitrary,
    tie("array"),
    tie("object")
  ),
  object: fc
    .tuple(
      fc.uniqueArray(fc.tuple(keyArbitrary, tie("json")), {
        maxLength: 5,
        selector: ([key]) => key,
      }),
      fc.boolean()
    )
    .map(([entries, nullPrototype]) =>
      objectFromEntries(entries, nullPrototype)
    ),
}));

const jsonObjectArbitrary = fc
  .tuple(
    fc.uniqueArray(fc.tuple(keyArbitrary, jsonArbitrary), {
      maxLength: 6,
      selector: ([key]) => key,
    }),
    fc.boolean()
  )
  .map(([entries, nullPrototype]) => objectFromEntries(entries, nullPrototype));

/** Recursively rebuild objects with their keys in a different order. */
function reorderKeys(value: JsonValue, seed: number): JsonValue {
  if (value === null || typeof value !== "object" || isExactJsonNumber(value))
    return value;
  if (Array.isArray(value))
    return value.map((item, index) => reorderKeys(item, seed + index));
  const keys = Object.keys(value);
  const rotation = keys.length === 0 ? 0 : seed % keys.length;
  const reordered = [...keys.slice(rotation), ...keys.slice(0, rotation)]
    .reverse()
    .map((key, index) => {
      const child = value[key];
      if (child === undefined) throw new Error("unexpected missing key");
      return [key, reorderKeys(child, seed + index + 1)] as const;
    });
  return objectFromEntries(reordered, Object.getPrototypeOf(value) === null);
}

/** One step from a container to the child holding a planted value. */
type PathSegment =
  | {
      readonly key: string;
      readonly kind: "key";
      readonly siblings: readonly JsonValue[];
    }
  | { readonly before: readonly JsonValue[]; readonly kind: "index" };

const pathSegmentArbitrary: fc.Arbitrary<PathSegment> = fc.oneof(
  fc.record({
    key: keyArbitrary,
    kind: fc.constant("key" as const),
    siblings: fc.array(leafArbitrary, { maxLength: 2 }),
  }),
  fc.record({
    before: fc.array(leafArbitrary, { maxLength: 2 }),
    kind: fc.constant("index" as const),
  })
);

/** Documented JSONPath-like location format of {@link JsonValueError}. */
function pathOf(segments: readonly PathSegment[]): string {
  let path = "$";
  for (const segment of segments) {
    if (segment.kind === "key") {
      path += /^[A-Za-z_$][\w$]*$/.test(segment.key)
        ? `.${segment.key}`
        : `[${JSON.stringify(segment.key)}]`;
    } else {
      path += `[${segment.before.length}]`;
    }
  }
  return path;
}

/** Place `leaf` at the end of `segments`, surrounded by valid siblings. */
function plant(leaf: unknown, segments: readonly PathSegment[]): unknown {
  let value = leaf;
  for (const segment of [...segments].reverse()) {
    if (segment.kind === "key") {
      const entries: [string, unknown][] = segment.siblings.map(
        (sibling, index) => [`${segment.key}\u0000${index}`, sibling]
      );
      entries.splice(entries.length >> 1, 0, [segment.key, value]);
      value = objectFromEntries(
        entries as [string, JsonValue][],
        segment.siblings.length % 2 === 0
      );
    } else {
      value = [...segment.before, value, null];
    }
  }
  return value;
}

describe("River JSON properties", () => {
  it("copies every valid value into an equal null-prototype value", () => {
    fc.assert(
      fc.property(jsonArbitrary, (value) => {
        const copy = toJsonValue(value);
        expect(jsonValuesEqual(copy, value)).toBe(true);
        expect(stringifyJson(copy)).toBe(JSON.stringify(value));
        const visit = (item: JsonValue): void => {
          if (item === null || typeof item !== "object") return;
          if (isExactJsonNumber(item)) return;
          if (Array.isArray(item)) {
            item.forEach(visit);
            return;
          }
          expect(Object.getPrototypeOf(item)).toBeNull();
          Object.values(item).forEach(visit);
        };
        visit(copy);
      }),
      { numRuns: 300 }
    );
  });

  it("round-trips serialized values through the exact parser", () => {
    fc.assert(
      fc.property(jsonArbitrary, (value) => {
        const text = stringifyJson(value);
        const parsed = parseJson(text);
        expect(jsonValuesEqual(parsed, value)).toBe(true);
        // One pass may normalize a non-canonical exact token (`1.0` reads
        // as `1`); after that, serialization is a fixed point.
        const normalized = stringifyJson(parsed);
        expect(stringifyJson(parseJson(normalized))).toBe(normalized);
      }),
      { numRuns: 300 }
    );
  });

  it("parses every int8 value exactly and every safe integer as a number", () => {
    const aroundSafeLimit = fc
      .integer({ max: 4096, min: -4096 })
      .chain((offset) =>
        fc.constantFrom(MAX_SAFE + BigInt(offset), -MAX_SAFE - BigInt(offset))
      );
    const aroundInt8Limit = fc
      .bigInt({ max: 4096n, min: 0n })
      .chain((offset) => fc.constantFrom(INT8_MAX - offset, INT8_MIN + offset));
    fc.assert(
      fc.property(
        fc.oneof(
          fc.bigInt({ max: INT8_MAX, min: INT8_MIN }),
          aroundSafeLimit,
          aroundInt8Limit
        ),
        (integer) => {
          const parsed = parseJson(`{"id":${integer}}`);
          if (parsed === null || typeof parsed !== "object") {
            throw new Error("expected an object");
          }
          const id = (parsed as Record<string, JsonValue>).id;
          if (
            id === undefined ||
            !(typeof id === "number" || isExactJsonNumber(id))
          ) {
            throw new Error("expected a JSON number");
          }
          expect(jsonNumberToBigInt(id)).toBe(integer);
          expect(typeof id === "number").toBe(
            integer <= MAX_SAFE && integer >= -MAX_SAFE
          );
          expect(stringifyJson(parsed)).toBe(`{"id":${integer}}`);
        }
      ),
      { numRuns: 500 }
    );
  });

  it("treats every spelling of one decimal value as equal", () => {
    // mantissa × 10^exponent written with shifted decimal points, padding
    // zeros, and exponent forms.
    const spellings = (mantissa: bigint, exponent: number) => {
      const sign = mantissa < 0n ? "-" : "";
      const digits = (mantissa < 0n ? -mantissa : mantissa).toString();
      return [
        `${sign}${digits}e${exponent}`,
        `${sign}${digits}0E${exponent - 1}`,
        `${sign}${digits}e+${exponent}`.replace("e+-", "e-"),
        `${sign}0.${digits}e${exponent + digits.length}`,
        `${sign}${digits}.000e${exponent}`,
      ].map(exactJsonNumber);
    };
    fc.assert(
      fc.property(
        fc
          .bigInt({ max: 10n ** 30n, min: -(10n ** 30n) })
          .filter((mantissa) => mantissa !== 0n),
        fc.integer({ max: 40, min: -40 }),
        (mantissa, exponent) => {
          const forms = spellings(mantissa, exponent);
          for (const left of forms) {
            for (const right of forms) {
              expect(jsonValuesEqual(left, right)).toBe(true);
            }
          }
          const different = exactJsonNumber(
            `${mantissa * 10n + (mantissa < 0n ? -1n : 1n)}e${exponent - 1}`
          );
          expect(jsonValuesEqual(forms[0] as JsonValue, different)).toBe(false);
        }
      ),
      { numRuns: 300 }
    );
  });

  it("compares by value, independent of object key order", () => {
    fc.assert(
      fc.property(jsonArbitrary, fc.nat(), (value, seed) => {
        const reordered = reorderKeys(value, seed);
        expect(jsonValuesEqual(value, reordered)).toBe(true);
        expect(jsonValuesEqual(reordered, value)).toBe(true);
        // Wrapping any value changes it.
        expect(jsonValuesEqual(value, [value])).toBe(false);
        expect(jsonValuesEqual([value], value)).toBe(false);
      }),
      { numRuns: 300 }
    );
  });

  it("detects a change to any single leaf", () => {
    fc.assert(
      fc.property(
        fc.array(pathSegmentArbitrary, { maxLength: 4 }),
        leafArbitrary,
        (segments, leaf) => {
          const original = toJsonValue(plant(leaf, segments));
          const changed = toJsonValue(plant([leaf], segments));
          expect(jsonValuesEqual(original, changed)).toBe(false);
          expect(jsonValuesEqual(changed, original)).toBe(false);
        }
      ),
      { numRuns: 300 }
    );
  });

  it("omits undefined properties at any depth", () => {
    fc.assert(
      fc.property(
        fc.array(pathSegmentArbitrary, { maxLength: 4 }),
        leafArbitrary,
        keyArbitrary,
        (segments, leaf, extraKey) => {
          const container = plant(
            objectFromEntries([["kept", leaf]], false),
            segments
          );
          const withUndefined = plant(
            Object.defineProperty(
              objectFromEntries([["kept", leaf]], false),
              extraKey === "kept" ? "kept\u0000" : extraKey,
              { enumerable: true, value: undefined }
            ),
            segments
          );
          expect(stringifyJson(withUndefined)).toBe(stringifyJson(container));
        }
      ),
      { numRuns: 200 }
    );
  });

  it("rejects an invalid value anywhere and reports its path", () => {
    const sparse = () => {
      const array = new Array<unknown>(2);
      array[1] = 1;
      return array;
    };
    const cyclic = () => {
      const value: Record<string, unknown> = {};
      value.self = value;
      return value;
    };
    const invalidLeaf = fc.oneof(
      fc.bigInt().map((value) => ({ at: "", value })),
      fc
        .constantFrom(
          Number.NaN,
          Number.POSITIVE_INFINITY,
          Number.NEGATIVE_INFINITY
        )
        .map((value) => ({ at: "", value })),
      fc
        .oneof(
          fc.bigInt({ max: INT8_MAX, min: MAX_SAFE + 1n }),
          fc.bigInt({ max: -MAX_SAFE - 1n, min: INT8_MIN })
        )
        .map((value) => ({ at: "", value: Number(value) })),
      fc.integer({ max: 0xdfff, min: 0xd800 }).map((code) => ({
        at: "",
        value: `a${String.fromCharCode(code)}`,
      })),
      fc.constant({ at: "", value: () => undefined }),
      fc.constant({ at: "", value: Symbol("leaf") }),
      fc.constant({ at: "", value: new Date(0) }),
      fc.constant({ at: "", value: new Map() }),
      fc.constant({
        at: ".value",
        value: Object.defineProperty({}, "value", {
          enumerable: true,
          get: () => 1,
        }),
      }),
      fc.constant({ at: "[0]", value: sparse() }),
      fc.constant({ at: "[0]", value: [undefined] }),
      fc.constant({ at: ".self", value: cyclic() }),
      fc.integer({ max: 0xdfff, min: 0xd800 }).map((code) => ({
        at: " key",
        value: objectFromEntries([[String.fromCharCode(code), 1]], true),
      })),
      fc.constant({
        at: "",
        value: (JSON as unknown as { rawJSON(text: string): unknown }).rawJSON(
          '"text"'
        ),
      })
    );
    fc.assert(
      fc.property(
        fc.array(pathSegmentArbitrary, { maxLength: 4 }),
        invalidLeaf,
        (segments, { at, value }) => {
          const planted = plant(value, segments);
          const expectedPath = `${pathOf(segments)}${at}`;
          for (const encode of [toJsonValue, stringifyJson]) {
            let thrown: unknown;
            try {
              encode(planted);
            } catch (error: unknown) {
              thrown = error;
            }
            expect(thrown).toBeInstanceOf(JsonValueError);
            expect((thrown as JsonValueError).path).toBe(expectedPath);
          }
        }
      ),
      { numRuns: 400 }
    );
  });

  it("keeps prototype-named keys as data without polluting prototypes", () => {
    fc.assert(
      fc.property(jsonObjectArbitrary, jsonArbitrary, (object, payload) => {
        const text = `{"__proto__":${stringifyJson(payload)},"rest":${stringifyJson(object)}}`;
        const parsed = parseJson(text);
        const copied = toJsonObject(parsed);
        expect(Object.hasOwn(copied, "__proto__")).toBe(true);
        expect(Object.getPrototypeOf(copied)).toBeNull();
        expect(jsonValuesEqual(copied.__proto__ as JsonValue, payload)).toBe(
          true
        );
        expect(Object.getPrototypeOf({})).toBe(Object.prototype);
        expect(Object.keys(Object.prototype)).toEqual([]);
      }),
      { numRuns: 200 }
    );
  });
});
