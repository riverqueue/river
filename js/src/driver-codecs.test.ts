import { describe, expect, it } from "vitest";

import { decodeAttemptError, decodeAttemptErrors } from "./driver-codecs.js";

const ZERO = Temporal.Instant.from("0001-01-01T00:00:00Z");
const ATTEMPT_AT = Temporal.Instant.from("2024-01-02T03:04:05.123456Z");

function attemptError(
  fields: Partial<ReturnType<typeof decodeAttemptError>>
): ReturnType<typeof decodeAttemptError> {
  return { at: ZERO, attempt: 0, error: "", trace: "", ...fields };
}

describe("decodeAttemptError", () => {
  it("rejects text that isn't valid JSON", () => {
    expect(() => decodeAttemptError(`{"at":`)).toThrow(SyntaxError);
  });

  it("decodes the shape River writes exactly", () => {
    expect(
      decodeAttemptError(
        `{"at":"2024-01-02T03:04:05.123456Z","attempt":3,"error":"job failed","trace":"goroutine 1 [running]:"}`
      )
    ).toEqual({
      at: ATTEMPT_AT,
      attempt: 3,
      error: "job failed",
      trace: "goroutine 1 [running]:",
    });
  });

  it("decodes missing fields and nulls as Go's encoding/json does", () => {
    expect(decodeAttemptError(`{"attempt":2,"error":null}`)).toEqual(
      attemptError({ attempt: 2 })
    );
    expect(decodeAttemptError(`null`)).toEqual(attemptError({}));
  });

  // The cases of River for Go's `TestUnmarshalAttemptError`.
  it.each([
    [
      "AtInvalid",
      `{"at":"not a time","attempt":2,"error":"err"}`,
      { attempt: 2, error: "err" },
    ],
    [
      "AtNoOffset",
      `{"at":"2024-01-02T03:04:05.123456","attempt":2}`,
      { attempt: 2 },
    ],
    ["AtNumber", `{"at":1704164645,"attempt":2}`, { attempt: 2 }],
    [
      "AtPostgresText",
      `{"at":"2024-01-02 03:04:05.123456+00","attempt":2}`,
      { attempt: 2 },
    ],
    [
      "AtRFC3339WithOtherInvalidField",
      `{"at":"2024-01-02T03:04:05.123456Z","attempt":"2"}`,
      { at: ATTEMPT_AT, attempt: 2 },
    ],
    [
      "AtSpaceNoOffset",
      `{"at":"2024-01-02 03:04:05.123456","attempt":2}`,
      { attempt: 2 },
    ],
    [
      "AttemptFloat",
      `{"attempt":3.0,"error":"err"}`,
      { attempt: 3, error: "err" },
    ],
    ["AttemptFractional", `{"attempt":3.5,"error":"err"}`, { error: "err" }],
    ["AttemptObject", `{"attempt":{},"error":"err"}`, { error: "err" }],
    [
      "AttemptString",
      `{"attempt":" 3 ","error":"err"}`,
      { attempt: 3, error: "err" },
    ],
    [
      "AttemptStringInvalid",
      `{"attempt":"three","error":"err"}`,
      { error: "err" },
    ],
    ["ElementArray", `[1, "two"]`, { error: `[1,"two"]` }],
    ["ElementNumber", `123`, { error: "123" }],
    ["ElementString", `"job failed"`, { error: "job failed" }],
    [
      "ErrorObject",
      `{"attempt":1,"error":{"message": "boom", "code": 7}}`,
      { attempt: 1, error: `{"message":"boom","code":7}` },
    ],
    [
      "TraceArray",
      `{"attempt":1,"error":"err","trace":["frame1", "frame2"]}`,
      { attempt: 1, error: "err", trace: `["frame1","frame2"]` },
    ],
    [
      "TraceNullWithInvalidField",
      `{"attempt":"x","error":null,"trace":null}`,
      {},
    ],
  ])(
    "decodes an unexpected shape leniently like Go: %s",
    (_name, json, expected) => {
      expect(decodeAttemptError(json)).toEqual(attemptError(expected));
    }
  );

  // Go's `time.Time` reads `at` with `time.Parse` and its RFC 3339 layout,
  // from the string as written (Go 1.26 doesn't unescape it).
  it.each([
    [
      "OneDigitHour",
      "2024-01-02T3:04:05.123456Z",
      "2024-01-02T03:04:05.123456Z",
    ],
    [
      "CommaFraction",
      "2024-01-02T03:04:05,123456Z",
      "2024-01-02T03:04:05.123456Z",
    ],
    [
      "FractionPastNanoseconds",
      "2024-01-02T03:04:05.1234567891Z",
      "2024-01-02T03:04:05.123456789Z",
    ],
    [
      "Offset",
      "2024-01-02T05:34:05.123456+02:30",
      "2024-01-02T03:04:05.123456Z",
    ],
    [
      "NegativeZeroOffset",
      "2024-01-02T03:04:05.123456-00:00",
      "2024-01-02T03:04:05.123456Z",
    ],
    [
      "LargestOffset",
      "2024-01-03T04:04:05.123456+24:60",
      "2024-01-02T03:04:05.123456Z",
    ],
    ["YearZero", "0000-01-02T03:04:05Z", "0000-01-02T03:04:05Z"],
    ["LeapDay", "2024-02-29T03:04:05Z", "2024-02-29T03:04:05Z"],
    ["EscapedZone", "2024-01-02T03:04:05BSLu005a", null],
    ["OffsetMinutePastRange", "2024-01-02T03:04:05+24:61", null],
    ["OffsetWithoutColon", "2024-01-02T03:04:05+0100", null],
    ["OffsetHourOnly", "2024-01-02T03:04:05+01", null],
    ["FractionWithoutDigits", "2024-01-02T03:04:05.Z", null],
    ["Hour24", "2024-01-02T24:00:00Z", null],
    ["LeapSecond", "2024-01-02T03:04:60Z", null],
    ["OneDigitMinute", "2024-01-02T03:4:05Z", null],
    ["NotALeapDay", "2023-02-29T03:04:05Z", null],
    ["LowerCase", "2024-01-02t03:04:05z", null],
    ["Padded", " 2024-01-02T03:04:05Z", null],
  ])("reads `at` like Go: %s", (_name, at, expected) => {
    expect(
      decodeAttemptError(
        `{"at":${JSON.stringify(at).replaceAll("BSL", "\\")},"attempt":1}`
      ).at
    ).toEqual(expected === null ? ZERO : Temporal.Instant.from(expected));
  });

  // Go reads a lenient `attempt` with `strconv`, which takes digit
  // separators, hexadecimal floats, and signs.
  it.each([
    ["Exponent", `1E2`, 100],
    ["NegativeZero", `-0`, 0],
    ["BeyondInt64", `9223372036854775808`, 0],
    ["Separators", `"1_000"`, 1000],
    ["SeparatorAfterPoint", `"1_0.0"`, 10],
    ["MisplacedSeparator", `"1__0"`, 0],
    ["HexFloat", `"0x1.8p1"`, 3],
    ["HexFloatSeparatorAfterPrefix", `"0x_1p4"`, 16],
    ["HexWithoutExponent", `"0x10"`, 0],
    ["PlusSign", `"+3"`, 3],
    ["Infinity", `"Inf"`, 0],
    ["Boolean", `true`, 0],
  ])("reads `attempt` like Go: %s", (_name, attempt, expected) => {
    expect(
      decodeAttemptError(`{"attempt":${attempt},"error":"err"}`).attempt
    ).toBe(expected);
  });

  it("matches field names case-insensitively, keeping the last", () => {
    expect(
      decodeAttemptError(
        `{"AT":"2024-01-02T03:04:05.123456Z","Attempt":2,"ERROR":"first","error":"last","Trace":"t"}`
      )
    ).toEqual({ at: ATTEMPT_AT, attempt: 2, error: "last", trace: "t" });
  });

  it("lets a later null leave a field unless the element is lenient", () => {
    // Go's encoding/json leaves a field as it was for `null`.
    expect(decodeAttemptError(`{"error":"kept","Error":null}`)).toEqual(
      attemptError({ error: "kept" })
    );
    // Go's lenient fallback takes each field's last value.
    expect(
      decodeAttemptError(`{"error":"kept","Error":null,"attempt":"2"}`)
    ).toEqual(attemptError({ attempt: 2 }));
  });

  it("keeps other values' JSON text as written, without white space", () => {
    expect(
      decodeAttemptError(
        `{"error":{"message": "BSLu00e9", "code": 1.50},"trace":["BSLud800"]}`.replaceAll(
          "BSL",
          "\\"
        )
      )
    ).toEqual(
      attemptError({
        error: `{"message":"BSLu00e9","code":1.50}`.replaceAll("BSL", "\\"),
        trace: `["BSLud800"]`.replaceAll("BSL", "\\"),
      })
    );
  });

  it("replaces an unpaired surrogate in a string like Go", () => {
    expect(
      decodeAttemptError(`{"error":"aBSLud800b"}`.replaceAll("BSL", "\\")).error
    ).toBe("a\ufffdb");
  });
});

describe("decodeAttemptErrors", () => {
  it("decodes an empty array and null as empty", () => {
    expect(decodeAttemptErrors(`[]`)).toEqual([]);
    expect(decodeAttemptErrors(`null`)).toEqual([]);
  });

  it("rejects text that isn't valid JSON or isn't an array", () => {
    expect(() => decodeAttemptErrors(`[{"at":`)).toThrow(SyntaxError);
    expect(() => decodeAttemptErrors(`{"error":"not an array"}`)).toThrow(
      "JSON is not an array"
    );
  });

  it("decodes each element on its own, like Go", () => {
    expect(
      decodeAttemptErrors(
        ` [ {"at":"2024-01-02T03:04:05.123456Z","attempt":1,"error":"err1","trace":""} , "err2", {"at":"invalid","attempt":"2","error":"err"}, {"error":"next","Error":null} ] `
      )
    ).toEqual([
      attemptError({ at: ATTEMPT_AT, attempt: 1, error: "err1" }),
      attemptError({ error: "err2" }),
      attemptError({ attempt: 2, error: "err" }),
      attemptError({ error: "next" }),
    ]);
  });
});
