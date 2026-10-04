import { describe, expect, it } from "vitest";

import {
  exactJsonNumber,
  isExactJsonNumber,
  isJsonNumber,
  jsonNumberToBigInt,
  JsonValueError,
  jsonValuesEqual,
  parseJson,
  parseJsonObject,
  stringifyJson,
  stringifyUniqueJson,
  toJsonObject,
} from "./json.js";
import type { ExactJsonNumber } from "./json.js";

describe("River JSON", () => {
  it("compares exact JSON with database value semantics", () => {
    expect(jsonValuesEqual(exactJsonNumber("1.00"), 1)).toBe(true);
    expect(jsonValuesEqual(exactJsonNumber("-0"), 0)).toBe(true);
    expect(
      jsonValuesEqual(
        {
          amount: exactJsonNumber("0.12345678901234567890"),
          nested: { b: 2, a: 1 },
        },
        {
          nested: { a: 1, b: 2 },
          amount: exactJsonNumber("0.1234567890123456789"),
        }
      )
    ).toBe(true);
    expect(
      jsonValuesEqual(
        exactJsonNumber("0.12345678901234567890"),
        exactJsonNumber("0.12345678901234567891")
      )
    ).toBe(false);
    expect(jsonValuesEqual([1, 2], [2, 1])).toBe(false);
  });

  it("copies valid values into null-prototype dictionaries", () => {
    const input = { items: [{ enabled: true }], value: null };
    const output = toJsonObject(input);

    expect(output).toEqual(input);
    expect(Object.getPrototypeOf(output)).toBeNull();
    expect(Object.getPrototypeOf((output.items as object[])[0])).toBeNull();
    expect(output).not.toBe(input);
  });

  it.each([
    ["bigint", { value: 1n }],
    [
      "cycle",
      (() => {
        const value: Record<string, unknown> = {};
        value.self = value;
        return value;
      })(),
    ],
    ["function", { value: () => undefined }],
    ["infinity", { value: Number.POSITIVE_INFINITY }],
    ["NaN", { value: Number.NaN }],
    ["unsafe integer", { value: Number.MAX_SAFE_INTEGER + 1 }],
    ["undefined array element", { value: [undefined] }],
    ["undefined value", undefined],
    ["unpaired surrogate", { value: "\ud800" }],
  ])("rejects %s instead of coercing it", (_name, value) => {
    expect(() => stringifyJson(value)).toThrow(JsonValueError);
  });

  it("reads ordinary and exact integers as bigint", () => {
    const value = parseJsonObject(
      '{"big":9223372036854775807,"small":42,"f":1.5}'
    );
    expect(isJsonNumber(value.big)).toBe(true);
    expect(isJsonNumber(value.small)).toBe(true);
    expect(isJsonNumber("42")).toBe(false);
    expect(jsonNumberToBigInt(value.big as ExactJsonNumber)).toBe(
      9223372036854775807n
    );
    expect(jsonNumberToBigInt(42)).toBe(42n);
    expect(() => jsonNumberToBigInt(1.5)).toThrow(JsonValueError);
    expect(() => jsonNumberToBigInt(exactJsonNumber("1e400"))).toThrow(
      JsonValueError
    );
  });

  it("omits undefined object properties like JSON.stringify", () => {
    expect(
      stringifyJson({ a: 1, b: undefined, nested: { c: undefined } })
    ).toBe('{"a":1,"nested":{}}');
    expect(Object.keys(toJsonObject({ optional: undefined }))).toEqual([]);
  });

  it("retains finite fractions, including exponent form", () => {
    expect(stringifyJson({ exponent: 1e-100, fraction: 1.25 })).toBe(
      '{"exponent":1e-100,"fraction":1.25}'
    );
  });

  it("preserves persisted numbers that JavaScript cannot round-trip", () => {
    const value = parseJsonObject(
      '{"decimal":0.1234567890123456789,"integer":9223372036854775807,"ordinary":0.1,"underflow":1e-400}'
    );

    expect(isExactJsonNumber(value.decimal)).toBe(true);
    expect(isExactJsonNumber(value.integer)).toBe(true);
    expect(isExactJsonNumber(value.underflow)).toBe(true);
    expect(value.ordinary).toBe(0.1);
    expect((value.decimal as { rawJSON: string }).rawJSON).toBe(
      "0.1234567890123456789"
    );
    expect(JSON.stringify(value)).toBe(
      '{"decimal":0.1234567890123456789,"integer":9223372036854775807,"ordinary":0.1,"underflow":1e-400}'
    );
  });

  it("constructs exact numeric values with native raw JSON", () => {
    const exact = exactJsonNumber("9223372036854775807");

    expect(isExactJsonNumber(exact)).toBe(true);
    expect(stringifyJson({ exact })).toBe('{"exact":9223372036854775807}');
    expect(() => exactJsonNumber("true")).toThrow("numeric token");
    const rawBoolean = (
      JSON as typeof JSON & { rawJSON(source: string): unknown }
    ).rawJSON("true");
    expect(() => stringifyJson({ raw: rawBoolean })).toThrow(
      "must be a number"
    );
  });

  it("rejects exact numeric primitives where an object is required", () => {
    expect(() => parseJsonObject("9223372036854775807")).toThrow(
      "must be an object"
    );
    expect(parseJson("-0")).toMatchObject({ rawJSON: "-0" });
  });

  it("rejects unsafe integer-valued numbers even in exponent form", () => {
    expect(() => stringifyJson({ value: 1e100 })).toThrow("safe integer range");
  });

  it("rejects non-finite numbers with Go's encoding/json text", () => {
    for (const [value, text] of [
      [Number.NaN, "NaN"],
      [Number.POSITIVE_INFINITY, "+Inf"],
      [Number.NEGATIVE_INFINITY, "-Inf"],
    ] as const) {
      const message = `$.nested.value: unsupported value: ${text}`;
      for (const encode of [toJsonObject, stringifyJson]) {
        expect(() => encode({ nested: { value } }), text).toThrow(message);
      }
    }
  });

  it("rejects holes, accessors, and classes", () => {
    const sparse = new Array(2);
    sparse[1] = "present";
    expect(() => toJsonObject({ sparse })).toThrow("array holes");

    const accessor = Object.defineProperty({}, "value", {
      enumerable: true,
      get: () => "surprise",
    });
    expect(() => toJsonObject(accessor)).toThrow("accessors");

    class Payload {
      value = "class";
    }
    expect(() => toJsonObject(new Payload())).toThrow("class instances");
  });

  it("preserves prototype-looking JSON keys without prototype mutation", () => {
    const input = JSON.parse(
      '{"__proto__":{"polluted":true},"constructor":1,"prototype":2}'
    ) as unknown;

    const output = toJsonObject(input);

    expect(Object.getPrototypeOf(output)).toBeNull();
    expect(Object.hasOwn(output, "__proto__")).toBe(true);
    expect(output.__proto__).toEqual({ polluted: true });
    expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
    expect(stringifyJson(output)).toBe(
      '{"__proto__":{"polluted":true},"constructor":1,"prototype":2}'
    );
  });

  it("writes top-level unique argument keys the way sjson does", () => {
    // Printable ASCII keys stay verbatim at the top level, where River Go
    // rewrites them; nested keys keep encoding/json's escaping.
    expect(
      stringifyUniqueJson({
        "a<b>": { "<k>": 2 },
        "é&": 1,
        'q"': 3,
      })
    ).toBe('{"a<b>":{"\\u003ck\\u003e":2},"q\\"":3,"é\\u0026":1}');
  });
});
