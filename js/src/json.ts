import { Buffer } from "node:buffer";

import { ValidationError } from "./errors.js";

/**
 * An exact JSON number represented by Node's immutable raw-JSON primitive.
 *
 * River returns this representation when a persisted number cannot round-trip
 * through JavaScript `number` without changing its JSON value. Read the exact
 * token through {@link rawJSON}; pass it back to River or `JSON.stringify`
 * without losing precision.
 */
export interface ExactJsonNumber {
  readonly rawJSON: string;
}

/** A value that can be represented by River's JSON protocol. */
export type JsonValue =
  boolean | ExactJsonNumber | JsonObject | JsonValue[] | null | number | string;

/** A JSON object accepted by River at a persistence boundary. */
export interface JsonObject {
  [key: string]: JsonValue;
}

/** An error raised when a value cannot safely cross River's JSON boundary. */
export class JsonValueError extends ValidationError {
  /** JSONPath-like location of the rejected value, such as `$.user.id`. */
  readonly path: string;

  constructor(path: string, message: string) {
    super(`${path}: ${message}`, { details: { path } });
    this.name = "JsonValueError";
    this.path = path;
  }
}

const JSON_NUMBER_PATTERN = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/;
const jsonRaw = JSON as typeof JSON & {
  isRawJSON(value: unknown): value is ExactJsonNumber;
  rawJSON(source: string): ExactJsonNumber;
};
const parseWithSource = JSON.parse as (
  text: string,
  reviver: (
    key: string,
    value: unknown,
    context: { readonly source: string } | undefined
  ) => unknown
) => unknown;

/**
 * Reject a non-finite number with Go's `encoding/json` wording, such as
 * `unsupported value: NaN`, so every River implementation reports it alike.
 */
function nonFiniteError(path: string, value: number): JsonValueError {
  const text = Number.isNaN(value) ? "NaN" : value > 0 ? "+Inf" : "-Inf";
  return new JsonValueError(path, `unsupported value: ${text}`);
}

/** @internal Freeze a JSON object and every nested object and array in place. */
export function deepFreezeJson<T extends JsonObject>(value: T): T {
  freezeJson(value);
  return value;
}

/** Construct an exact JSON number from one valid JSON numeric token. */
export function exactJsonNumber(source: string): ExactJsonNumber {
  if (typeof source !== "string" || !JSON_NUMBER_PATTERN.test(source)) {
    throw new JsonValueError("$", "exact JSON number is not a numeric token");
  }
  return jsonRaw.rawJSON(source);
}

export function isExactJsonNumber(value: unknown): value is ExactJsonNumber {
  return (
    jsonRaw.isRawJSON(value) &&
    typeof value.rawJSON === "string" &&
    JSON_NUMBER_PATTERN.test(value.rawJSON)
  );
}

/**
 * Whether a value is a JSON number: an ordinary `number` or an
 * {@link ExactJsonNumber} that River uses for numbers JavaScript cannot
 * represent exactly (such as a 64-bit ID written by another language).
 *
 * Prefer validating args with a schema; use this when reading untyped
 * `JsonObject` values, where `typeof value === "number"` misses exact numbers.
 */
export function isJsonNumber(
  value: unknown
): value is ExactJsonNumber | number {
  return typeof value === "number" || isExactJsonNumber(value);
}

/**
 * Convert an integral JSON number, ordinary or exact, to a `bigint` without
 * losing precision. Throws {@link JsonValueError} for fractions.
 */
export function jsonNumberToBigInt(value: ExactJsonNumber | number): bigint {
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) {
      throw new JsonValueError("$", "number is not a safe integer");
    }
    return BigInt(value);
  }
  if (!isExactJsonNumber(value) || !/^-?\d+$/.test(value.rawJSON)) {
    throw new JsonValueError("$", "exact JSON number is not an integer");
  }
  return BigInt(value.rawJSON);
}

/** Compare two validated River JSON values using database JSON semantics. */
export function jsonValuesEqual(left: JsonValue, right: JsonValue): boolean {
  const leftIsNumber = typeof left === "number" || isExactJsonNumber(left);
  const rightIsNumber = typeof right === "number" || isExactJsonNumber(right);
  if (leftIsNumber || rightIsNumber) {
    if (!leftIsNumber || !rightIsNumber) return false;
    const leftSource = isExactJsonNumber(left)
      ? left.rawJSON
      : JSON.stringify(left);
    const rightSource = isExactJsonNumber(right)
      ? right.rawJSON
      : JSON.stringify(right);
    return (
      canonicalEqualityDecimal(leftSource) ===
      canonicalEqualityDecimal(rightSource)
    );
  }
  if (left === null || right === null) return left === right;
  if (typeof left !== "object" || typeof right !== "object") {
    return left === right;
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    if (
      !Array.isArray(left) ||
      !Array.isArray(right) ||
      left.length !== right.length
    ) {
      return false;
    }
    return left.every((value, index) => {
      const other = right[index];
      return other !== undefined && jsonValuesEqual(value, other);
    });
  }
  const keys = Object.keys(left);
  return (
    keys.length === Object.keys(right).length &&
    keys.every((key) => {
      const leftValue = left[key];
      const rightValue = right[key];
      return (
        leftValue !== undefined &&
        rightValue !== undefined &&
        Object.hasOwn(right, key) &&
        jsonValuesEqual(leftValue, rightValue)
      );
    })
  );
}

/** Parse JSON while preserving numbers that JavaScript cannot round-trip. */
export function parseJson(text: string): JsonValue {
  if (typeof text !== "string") {
    throw new JsonValueError("$", "JSON input must be a string");
  }
  const parsed = parseWithSource(text, (_key, value, context) => {
    if (typeof value !== "number" || context === undefined) return value;
    return numberRoundTrips(context.source, value)
      ? value
      : exactJsonNumber(context.source);
  });
  return toJsonValue(parsed);
}

export function parseJsonObject(text: string): JsonObject {
  const value = parseJson(text);
  if (
    value === null ||
    Array.isArray(value) ||
    typeof value !== "object" ||
    isExactJsonNumber(value)
  ) {
    throw new JsonValueError("$", "JSON value must be an object");
  }
  return value;
}

/**
 * Validate and copy an unknown value into River's JSON domain.
 *
 * Objects are copied into null-prototype dictionaries. This intentionally
 * rejects accessors, class instances, sparse arrays, cycles, non-finite
 * numbers, unsafe integers, `bigint`, and a top-level or array-element
 * `undefined` instead of relying on JSON.stringify's lossy coercions. Like
 * JSON.stringify, an object property whose value is `undefined` is omitted, so
 * optional properties behave as they do in every JSON library. Keys such as
 * `__proto__` remain ordinary data because the copy has a null prototype and
 * is never merged into application objects.
 */
export function toJsonValue(value: unknown): JsonValue {
  return copyJsonValue(value, "$", new Set());
}

/** Validate and copy an unknown value as a JSON object. */
export function toJsonObject(value: unknown): JsonObject {
  const result = copyJsonValue(value, "$", new Set());
  if (
    result === null ||
    Array.isArray(result) ||
    typeof result !== "object" ||
    isExactJsonNumber(result)
  ) {
    throw new JsonValueError("$", "job arguments must be a JSON object");
  }
  return result;
}

/** Serialize a previously unknown value without JSON's lossy coercions. */
export function stringifyJson(value: unknown): string {
  return JSON.stringify(toJsonValue(value));
}

/**
 * Apply the escaping Go's `encoding/json` adds when it marshals JSON text:
 * `<`, `>`, `&`, U+2028, and U+2029 become `\u003c`, `\u003e`, `\u0026`,
 * `\u2028`, and `\u2029`. Unique-key hash inputs and job list cursors use
 * it because every River implementation must produce the same bytes for
 * them.
 *
 * Those characters can only occur inside strings of valid JSON text, so the
 * result encodes the same JSON value, and text that already has them escaped
 * is returned unchanged.
 */
function escapeGoJson(text: string): string {
  return text.replace(/[<>&\u2028\u2029]/g, (character) => {
    switch (character) {
      case "<":
        return "\\u003c";
      case ">":
        return "\\u003e";
      case "&":
        return "\\u0026";
      case "\u2028":
        return "\\u2028";
      case "\u2029":
        return "\\u2029";
      default:
        return character;
    }
  });
}

/**
 * Encode a string as Go's `encoding/json` does: like `JSON.stringify`, but
 * also escaping `<`, `>`, `&`, U+2028, and U+2029.
 */
export function stringifyGoJsonString(value: string): string {
  return escapeGoJson(JSON.stringify(value));
}

/**
 * Encode arguments for a by-arguments unique key the way River Go assembles
 * them with `sjson`: top-level keys sorted bytewise and written as
 * {@link sjsonKey} does, nested values in their own wire order.
 */
export function stringifyUniqueJson(value: JsonObject): string {
  return encodeCanonicalJson(value, "$", new Set(), true);
}

/**
 * Encode the object River assembles from selected unique paths. Objects in
 * `assembled` are River's own intermediate objects, written in insertion
 * (sorted path) order with `sjson` keys; every other value is an argument
 * value, written in its own wire order.
 */
export function stringifySelectedUniqueJson(
  value: JsonObject,
  assembled: ReadonlySet<object>
): string {
  const ancestors = new Set<object>();
  const encodeAssembled = (object: JsonObject, path: string): string => {
    const members: string[] = [];
    for (const [key, child] of Object.entries(object)) {
      const childPath = propertyPath(path, key);
      validateUnicode(key, `${path} key`);
      members.push(
        `${sjsonKey(key)}:${
          child !== null && typeof child === "object" && assembled.has(child)
            ? encodeAssembled(child as JsonObject, childPath)
            : encodeCanonicalJson(child, childPath, ancestors)
        }`
      );
    }
    return `{${members.join(",")}}`;
  };
  return encodeAssembled(value, "$");
}

function copyJsonValue(
  value: unknown,
  path: string,
  ancestors: Set<object>
): JsonValue {
  if (value === null || typeof value === "boolean") return value;

  if (typeof value === "string") {
    validateUnicode(value, path);
    return value;
  }

  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw nonFiniteError(path, value);
    }
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
      throw new JsonValueError(
        path,
        "integer-valued numbers must be within the safe integer range; use a string for exact larger integers"
      );
    }
    return value;
  }

  if (typeof value !== "object") {
    throw new JsonValueError(path, `${typeof value} is not a JSON value`);
  }

  if (jsonRaw.isRawJSON(value)) {
    if (!isExactJsonNumber(value)) {
      throw new JsonValueError(path, "raw JSON value must be a number");
    }
    return value;
  }

  if (ancestors.has(value)) {
    throw new JsonValueError(path, "cyclic values are not supported");
  }

  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      const result: JsonValue[] = [];
      for (let index = 0; index < value.length; index++) {
        if (!Object.hasOwn(value, index)) {
          throw new JsonValueError(
            `${path}[${index}]`,
            "array holes are not supported"
          );
        }
        result.push(
          copyJsonValue(value[index], `${path}[${index}]`, ancestors)
        );
      }
      return result;
    }

    const prototype = Object.getPrototypeOf(value) as unknown;
    if (prototype !== Object.prototype && prototype !== null) {
      throw new JsonValueError(path, "class instances are not JSON objects");
    }

    const descriptors = Object.getOwnPropertyDescriptors(value);
    const result = Object.create(null) as JsonObject;
    for (const key of Object.keys(descriptors)) {
      validateUnicode(key, `${path} key`);
      const descriptor = descriptors[key];
      if (descriptor === undefined || !descriptor.enumerable) continue;
      if (!("value" in descriptor)) {
        throw new JsonValueError(
          propertyPath(path, key),
          "accessors are not supported"
        );
      }
      // Like JSON.stringify, omit properties whose value is undefined so
      // optional properties produced by validation libraries round-trip.
      if (descriptor.value === undefined) continue;

      result[key] = copyJsonValue(
        descriptor.value,
        propertyPath(path, key),
        ancestors
      );
    }
    return result;
  } finally {
    ancestors.delete(value);
  }
}

/**
 * Encode `value` for a unique-key hash input with Go's escaping, keeping
 * exact integers exact. Object keys keep their wire order, except that with
 * `root`, the keys of `value` itself are sorted bytewise and written as
 * {@link sjsonKey} does, as River Go's `sjson` assembly does.
 */
function encodeCanonicalJson(
  value: unknown,
  path: string,
  ancestors: Set<object>,
  root = false
): string {
  if (value === null) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "string") {
    validateUnicode(value, path);
    return escapeGoJson(JSON.stringify(value));
  }
  if (typeof value === "bigint") return value.toString(10);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw nonFiniteError(path, value);
    }
    if (Object.is(value, -0)) return "-0";
    return JSON.stringify(value);
  }
  if (typeof value !== "object") {
    throw new JsonValueError(path, `${typeof value} is not a JSON value`);
  }
  if (jsonRaw.isRawJSON(value)) {
    if (!isExactJsonNumber(value)) {
      throw new JsonValueError(path, "raw JSON value must be a number");
    }
    return value.rawJSON;
  }
  if (ancestors.has(value)) {
    throw new JsonValueError(path, "cyclic values are not supported");
  }

  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      const encoded: string[] = [];
      for (let index = 0; index < value.length; index++) {
        if (!Object.hasOwn(value, index)) {
          throw new JsonValueError(
            `${path}[${index}]`,
            "array holes are not supported"
          );
        }
        encoded.push(
          encodeCanonicalJson(value[index], `${path}[${index}]`, ancestors)
        );
      }
      return `[${encoded.join(",")}]`;
    }

    const prototype = Object.getPrototypeOf(value) as unknown;
    if (prototype !== Object.prototype && prototype !== null) {
      throw new JsonValueError(path, "class instances are not JSON objects");
    }
    const descriptors = Object.getOwnPropertyDescriptors(value);
    const keys = Object.keys(descriptors).filter(
      (key) => descriptors[key]?.enumerable === true
    );
    if (root) keys.sort(compareUtf8);
    const encoded: string[] = [];
    for (const key of keys) {
      validateUnicode(key, `${path} key`);
      const descriptor = descriptors[key];
      if (descriptor === undefined || !("value" in descriptor)) {
        throw new JsonValueError(
          propertyPath(path, key),
          "accessors are not supported"
        );
      }
      if (descriptor.value === undefined) continue;
      const encodedKey = root
        ? sjsonKey(key)
        : escapeGoJson(JSON.stringify(key));
      encoded.push(
        `${encodedKey}:${encodeCanonicalJson(
          descriptor.value,
          propertyPath(path, key),
          ancestors
        )}`
      );
    }
    return `{${encoded.join(",")}}`;
  } finally {
    ancestors.delete(value);
  }
}

/** Compare two strings by their UTF-8 bytes, as Go compares strings. */
export function compareUtf8(left: string, right: string): number {
  return Buffer.compare(Buffer.from(left, "utf8"), Buffer.from(right, "utf8"));
}

/**
 * Whether `value`, parsed from the JSON number `source`, is exactly the
 * number `source` denotes, so re-encoding it can't change its value.
 */
export function numberRoundTrips(source: string, value: number): boolean {
  if (!Number.isFinite(value)) return false;
  if (Number.isInteger(value) && !Number.isSafeInteger(value)) return false;
  if (Object.is(value, -0)) return false;
  return canonicalDecimal(source) === canonicalDecimal(JSON.stringify(value));
}

/**
 * The canonical form of the JSON number `source`, such as `1e2` for
 * `100.0`, so numerically equal tokens compare equal as text.
 *
 * @throws {JsonValueError} when `source` isn't a JSON number.
 */
export function canonicalDecimal(source: string): string {
  const match = /^(-)?(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(source);
  if (match === null) {
    throw new JsonValueError("$", "invalid JSON numeric token");
  }
  const [, negative, integer, fraction = "", exponent = "0"] = match;
  let digits = `${integer}${fraction}`.replace(/^0+/, "");
  if (digits.length === 0) return negative === undefined ? "0" : "-0";

  let decimalExponent = BigInt(exponent) - BigInt(fraction.length);
  const trailingZeros = /0+$/.exec(digits)?.[0].length ?? 0;
  if (trailingZeros > 0) {
    digits = digits.slice(0, -trailingZeros);
    decimalExponent += BigInt(trailingZeros);
  }
  return `${negative ?? ""}${digits}e${decimalExponent}`;
}

/** Like {@link canonicalDecimal}, but with `-0` equal to `0`. */
export function canonicalEqualityDecimal(source: string): string {
  const canonical = canonicalDecimal(source);
  return canonical === "-0" ? "0" : canonical;
}

/**
 * Write an object key the way `sjson` does when River Go assembles unique
 * arguments: verbatim when it is printable ASCII without a quote or
 * backslash, even though `encoding/json` would escape `<`, `>`, or `&`, and
 * otherwise with Go's `encoding/json` escaping.
 */
export function sjsonKey(key: string): string {
  return /^[\x20-\x7f]*$/.test(key) && !/["\\]/.test(key)
    ? `"${key}"`
    : escapeGoJson(JSON.stringify(key));
}

function propertyPath(path: string, key: string): string {
  return /^[A-Za-z_$][\w$]*$/.test(key)
    ? `${path}.${key}`
    : `${path}[${JSON.stringify(key)}]`;
}

function validateUnicode(value: string, path: string): void {
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) {
        throw new JsonValueError(path, "string contains an unpaired surrogate");
      }
      index++;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      throw new JsonValueError(path, "string contains an unpaired surrogate");
    }
  }
}

function freezeJson(value: unknown): void {
  if (value === null || typeof value !== "object") return;
  for (const child of Array.isArray(value) ? value : Object.values(value)) {
    freezeJson(child);
  }
  Object.freeze(value);
}
