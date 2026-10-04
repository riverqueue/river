import type { AttemptError, JobState } from "./job.js";
import { JOB_STATE } from "./job.js";

const ALL_JOB_STATES: ReadonlySet<string> = new Set(Object.values(JOB_STATE));

/** Go's zero `time.Time`, which River leaves in an `at` it can't read. */
const GO_ZERO_TIME = Temporal.Instant.from("0001-01-01T00:00:00Z");

const ATTEMPT_ERROR_FIELDS = ["at", "attempt", "error", "trace"] as const;

type AttemptErrorField = (typeof ATTEMPT_ERROR_FIELDS)[number];

/** An attempt error field's name and the JSON text of its value. */
type AttemptErrorMember = readonly [AttemptErrorField, string];

/**
 * Decode one persisted attempt error from its JSON text, as River for Go's
 * drivers do when they read a job's `errors`.
 *
 * River always writes attempt errors in one shape, which decodes as Go's
 * `encoding/json` decodes it. Because a job can't be read or worked unless
 * all of its attempt errors decode, an element written by another tool or
 * edited by hand that is valid JSON in any other shape decodes on a best
 * effort basis instead:
 *
 * - Field names match case-insensitively, and unknown fields are ignored.
 * - `at` accepts only what Go's `time.Time` does: RFC 3339 as Go's
 *   `time.Parse` reads it, taken from the string as written without
 *   unescaping it. Anything else is Go's zero time.
 * - `attempt` accepts integers, and numbers or strings holding a number with
 *   an integral value no larger in magnitude than 2^53. Anything else is
 *   `0`.
 * - `error` and `trace` keep any value other than a string or `null` as its
 *   compacted JSON text.
 * - A string element is used as `error`, and any other element that isn't
 *   an object is kept as its compacted JSON text in `error`.
 *
 * Only text that isn't valid JSON throws. This tolerance is for database
 * reads only: decoding a job's public JSON form stays strict.
 */
export function decodeAttemptError(json: string): AttemptError {
  JSON.parse(json);
  return decodeValidAttemptError(json.trim());
}

/**
 * Decode a persisted JSON array of attempt errors, decoding each element
 * like {@link decodeAttemptError}. `null` is empty. Text that isn't valid
 * JSON, or that isn't an array, throws, so the job can be reported as
 * undecodable.
 */
export function decodeAttemptErrors(json: string): AttemptError[] {
  const parsed: unknown = JSON.parse(json);
  if (parsed === null) return [];
  if (!Array.isArray(parsed)) throw new TypeError("JSON is not an array");
  return jsonElements(json.trim()).map(decodeValidAttemptError);
}

/** Validate a persisted job state, rejecting values River does not define. */
export function decodeJobState(value: string): JobState {
  if (!ALL_JOB_STATES.has(value)) {
    throw new TypeError(`unknown River job state: ${JSON.stringify(value)}`);
  }
  return value as JobState;
}

/**
 * Decode one attempt error from valid, trimmed JSON text like Go's
 * `riverdriver.UnmarshalAttemptError`: as `encoding/json` decodes it when it
 * can, and leniently otherwise.
 */
function decodeValidAttemptError(json: string): AttemptError {
  if (!json.startsWith("{")) {
    return {
      at: GO_ZERO_TIME,
      attempt: 0,
      error: lenientString(json),
      trace: "",
    };
  }
  // Like Go, field names match case-insensitively.
  const members: AttemptErrorMember[] = [];
  for (const [name, value] of jsonMembers(json)) {
    const folded = asciiLowerCase(name);
    const field = ATTEMPT_ERROR_FIELDS.find((each) => each === folded);
    if (field !== undefined) members.push([field, value]);
  }
  return strictAttemptError(members) ?? lenientAttemptError(members);
}

/**
 * Decode an attempt error's fields as Go's `encoding/json` does, in order,
 * returning `undefined` if it would reject any of them. As in Go, `null`
 * leaves a field as it was.
 */
function strictAttemptError(
  members: readonly AttemptErrorMember[]
): AttemptError | undefined {
  let at = GO_ZERO_TIME;
  let attempt = 0;
  let error = "";
  let trace = "";
  for (const [field, value] of members) {
    if (value === "null") continue;
    switch (field) {
      case "at": {
        const decoded = goTime(value);
        if (decoded === undefined) return undefined;
        at = decoded;
        break;
      }
      case "attempt": {
        const integer = /^-?\d+$/.test(value) ? int64(value) : undefined;
        if (integer === undefined) return undefined;
        attempt = integer;
        break;
      }
      case "error":
      case "trace":
        if (!value.startsWith('"')) return undefined;
        if (field === "error") error = goString(value);
        else trace = goString(value);
        break;
    }
  }
  return { at, attempt, error, trace };
}

/**
 * Decode an attempt error's fields leniently, like Go's fallback: each
 * field from its last value, `null` included.
 */
function lenientAttemptError(
  members: readonly AttemptErrorMember[]
): AttemptError {
  const last = new Map(members);
  const at = last.get("at");
  const attempt = last.get("attempt");
  return {
    at: (at === undefined ? undefined : goTime(at)) ?? GO_ZERO_TIME,
    attempt: attempt === undefined ? 0 : lenientInteger(attempt),
    error: lenientString(last.get("error") ?? ""),
    trace: lenientString(last.get("trace") ?? ""),
  };
}

const INT64_MAX = 9_223_372_036_854_775_807n;
const INT64_MIN = -9_223_372_036_854_775_808n;
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

/**
 * Leading and trailing white space as Go's `strings.TrimSpace` trims it,
 * which differs from `String.prototype.trim`.
 */
const GO_SPACE_EDGES =
  /^[\t\n\v\f\r \u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+|[\t\n\v\f\r \u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+$/g;

/**
 * Go's lenient `attempt`: an int64, or a number with an integral value no
 * larger in magnitude than 2^53, from a JSON number or from a string holding
 * one, which Go reads with `strconv`.
 */
function lenientInteger(json: string): number {
  let text: string;
  if (json.startsWith('"')) {
    text = goString(json).replace(GO_SPACE_EDGES, "");
  } else if (/^[-\d]/.test(json)) {
    text = json;
  } else {
    return 0;
  }
  if (/^[+-]?\d+$/.test(text)) {
    const integer = int64(text);
    if (integer !== undefined) return integer;
  }
  const number = goParseFloat(text);
  // `+ 0` turns a negative zero into zero, as Go's `int` conversion does.
  return Number.isInteger(number) && Math.abs(number) <= 2 ** 53
    ? number + 0
    : 0;
}

/**
 * Parse decimal integer text in Go's int64 range, saturating values beyond
 * JavaScript's safe range as other persisted counts do. Returns `undefined`
 * out of range.
 */
function int64(text: string): number | undefined {
  const integer = BigInt(text);
  if (integer < INT64_MIN || integer > INT64_MAX) return undefined;
  if (integer > MAX_SAFE) return Number.MAX_SAFE_INTEGER;
  if (integer < -MAX_SAFE) return -Number.MAX_SAFE_INTEGER;
  return Number(integer);
}

/**
 * Parse text like Go's `strconv.ParseFloat`, decimal or hexadecimal with
 * Go's digit separators, returning `NaN` for text Go rejects. Infinities and
 * values Go reports out of range can't be attempts either, so they are
 * `NaN` too.
 */
function goParseFloat(text: string): number {
  if (!underscoresAllowed(text)) return Number.NaN;
  const hex = /^([+-]?)0x([\da-f_]*)(?:\.([\da-f_]*))?p([+-]?[\d_]+)$/i.exec(
    text
  );
  if (hex !== null) {
    const [, sign, whole = "", fraction = "", exponent = ""] = hex;
    const fractionDigits = fraction.replaceAll("_", "");
    const digits = `${whole.replaceAll("_", "")}${fractionDigits}`;
    if (digits === "") return Number.NaN;
    const value =
      Number(BigInt(`0x${digits}`)) *
      2 ** (Number(exponent.replaceAll("_", "")) - 4 * fractionDigits.length);
    return sign === "-" ? -value : value;
  }
  if (
    !/^[+-]?(?:[\d_]+\.?[\d_]*|\.[\d_]+)(?:e[+-]?[\d_]+)?$/i.test(text) ||
    !/\d/.test(text)
  ) {
    return Number.NaN;
  }
  const value = Number(text.replaceAll("_", ""));
  return Number.isFinite(value) ? value : Number.NaN;
}

/**
 * Whether a number's underscores are where Go's `strconv` allows them: each
 * between digits, or between a base prefix and a digit.
 */
function underscoresAllowed(text: string): boolean {
  if (!text.includes("_")) return true;
  let body = text.replace(/^[+-]/, "");
  // A base prefix counts as a digit.
  let saw: "!" | "0" | "^" | "_" = "^";
  const hex = /^0x/i.test(body);
  if (hex) {
    body = body.slice(2);
    saw = "0";
  }
  for (const character of body) {
    if (hex ? /[\da-f]/i.test(character) : /\d/.test(character)) {
      saw = "0";
    } else if (character === "_") {
      if (saw !== "0") return false;
      saw = "_";
    } else {
      if (saw === "_") return false;
      saw = "!";
    }
  }
  return saw !== "_";
}

/**
 * Go's lenient `error` and `trace`: a string is used as is, `null` or a
 * missing field is empty, and any other value is kept as its compacted JSON
 * text.
 */
function lenientString(json: string): string {
  if (json === "" || json === "null") return "";
  if (json.startsWith('"')) return goString(json);
  return compactJson(json);
}

/**
 * Decode a JSON string as Go does, which replaces an unpaired surrogate with
 * U+FFFD.
 */
function goString(json: string): string {
  return (JSON.parse(json) as string).toWellFormed();
}

/**
 * Decode an attempt error's `at` like Go 1.26's `time.Time.UnmarshalJSON`:
 * a string holding a timestamp that Go's `time.Parse` reads with its RFC
 * 3339 layout, taken as written without unescaping. Returns `undefined` for
 * anything else.
 */
function goTime(json: string): Temporal.Instant | undefined {
  if (json.length < 2 || !json.startsWith('"') || !json.endsWith('"')) {
    return undefined;
  }
  return parseGoRfc3339(json.slice(1, -1));
}

/**
 * Parse a timestamp as Go's `time.Parse` does with its RFC 3339 layout:
 * `YYYY-MM-DD`, `T`, a one or two digit hour, `:MM:SS` in range without a
 * leap second, an optional fraction introduced by `.` or `,` whose digits
 * past nanoseconds are ignored, and `Z` or a `±hh:mm` offset of up to 24
 * hours and 60 minutes.
 */
function parseGoRfc3339(text: string): Temporal.Instant | undefined {
  const match =
    /^(\d{4})-(\d{2})-(\d{2})T(\d{1,2}):(\d{2}):(\d{2})(?:[.,](\d+))?(?:Z|([+-])(\d{2}):(\d{2}))$/.exec(
      text
    );
  if (match === null) return undefined;
  const [, year, month, day, hour, minute, second, fraction = ""] = match;
  const [sign, offsetHours = "0", offsetMinutes = "0"] = match.slice(8);
  if (
    Number(hour) > 23 ||
    Number(minute) > 59 ||
    Number(second) > 59 ||
    Number(offsetHours) > 24 ||
    Number(offsetMinutes) > 60
  ) {
    return undefined;
  }
  const nanoseconds = Number(fraction.slice(0, 9).padEnd(9, "0"));
  let local: Temporal.PlainDateTime;
  try {
    local = Temporal.PlainDateTime.from(
      {
        day: Number(day),
        hour: Number(hour),
        microsecond: Math.floor(nanoseconds / 1_000) % 1_000,
        millisecond: Math.floor(nanoseconds / 1_000_000),
        minute: Number(minute),
        month: Number(month),
        nanosecond: nanoseconds % 1_000,
        second: Number(second),
        year: Number(year),
      },
      { overflow: "reject" }
    );
  } catch {
    return undefined;
  }
  const offsetSeconds =
    (sign === "-" ? -1 : 1) *
    (Number(offsetHours) * 3_600 + Number(offsetMinutes) * 60);
  return local
    .toZonedDateTime("UTC")
    .toInstant()
    .subtract({ seconds: offsetSeconds });
}

/**
 * Lower-case ASCII letters only. No other character folds to a letter of an
 * attempt error's field names in Go.
 */
function asciiLowerCase(text: string): string {
  return text.replace(/[A-Z]/g, (letter) => letter.toLowerCase());
}

const JSON_WHITESPACE: ReadonlySet<string> = new Set([" ", "\t", "\n", "\r"]);

/**
 * Remove insignificant white space from valid JSON text without otherwise
 * changing it, like Go's `json.Compact`.
 */
function compactJson(json: string): string {
  let compacted = "";
  let index = 0;
  while (index < json.length) {
    if (json[index] === '"') {
      const end = skipString(json, index);
      compacted += json.slice(index, end);
      index = end;
    } else {
      const character = json.charAt(index);
      if (!JSON_WHITESPACE.has(character)) compacted += character;
      index += 1;
    }
  }
  return compacted;
}

/** The text of each element of valid, trimmed JSON array text. */
function jsonElements(json: string): string[] {
  const elements: string[] = [];
  let index = skipWhitespace(json, 1);
  while (json[index] !== "]") {
    const end = skipValue(json, index);
    elements.push(json.slice(index, end));
    index = skipWhitespace(json, end);
    if (json[index] === ",") index = skipWhitespace(json, index + 1);
  }
  return elements;
}

/** Each member's decoded name and value text, of valid, trimmed object text. */
function jsonMembers(json: string): [string, string][] {
  const members: [string, string][] = [];
  let index = skipWhitespace(json, 1);
  while (json[index] !== "}") {
    const nameEnd = skipString(json, index);
    const name = JSON.parse(json.slice(index, nameEnd)) as string;
    const valueStart = skipWhitespace(json, skipWhitespace(json, nameEnd) + 1);
    const valueEnd = skipValue(json, valueStart);
    members.push([name, json.slice(valueStart, valueEnd)]);
    index = skipWhitespace(json, valueEnd);
    if (json[index] === ",") index = skipWhitespace(json, index + 1);
  }
  return members;
}

/** The index just past the valid JSON value starting at `start`. */
function skipValue(json: string, start: number): number {
  const first = json[start];
  if (first === '"') return skipString(json, start);
  let index = start;
  if (first === "{" || first === "[") {
    let depth = 0;
    while (index < json.length) {
      const character = json[index];
      if (character === '"') {
        index = skipString(json, index);
        continue;
      }
      if (character === "{" || character === "[") depth += 1;
      else if (character === "}" || character === "]") depth -= 1;
      index += 1;
      if (depth === 0) break;
    }
    return index;
  }
  while (index < json.length && !/[\s,\]}]/.test(json.charAt(index))) {
    index += 1;
  }
  return index;
}

/** The index just past the JSON string starting at `start`. */
function skipString(json: string, start: number): number {
  let index = start + 1;
  while (json[index] !== '"') index += json[index] === "\\" ? 2 : 1;
  return index + 1;
}

function skipWhitespace(json: string, start: number): number {
  let index = start;
  while (JSON_WHITESPACE.has(json[index] ?? "")) index += 1;
  return index;
}
