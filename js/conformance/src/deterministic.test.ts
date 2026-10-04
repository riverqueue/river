import { describe, expect, it } from "vitest";

import {
  deterministicCronNext,
  deterministicRetryDelayNanoseconds,
  deterministicUniqueKey,
} from "./deterministic.js";
import {
  ADAPTER_ERROR_CODE,
  AdapterError,
  adapterErrorCode,
} from "./errors.js";

describe("deterministic conformance controls", () => {
  it("returns cron occurrences in the reference offset with Go's Z", () => {
    expect(
      deterministicCronNext({
        count: 3,
        expression: "30 0 * * *",
        from: "2026-03-07T23:45:00+05:30",
      })
    ).toEqual({
      next: [
        "2026-03-08T00:30:00+05:30",
        "2026-03-09T00:30:00+05:30",
        "2026-03-10T00:30:00+05:30",
      ],
    });
    expect(
      deterministicCronNext({
        count: 2,
        expression: "@every 1500ms",
        from: "2026-01-02T03:04:05.6789Z",
      })
    ).toEqual({ next: ["2026-01-02T03:04:06Z", "2026-01-02T03:04:07Z"] });
    expect(
      deterministicCronNext({
        count: 5,
        expression: "0 0 30 2 *",
        from: "2026-01-02T03:04:05Z",
      })
    ).toEqual({ next: [] });
  });

  it("rejects invalid cron expressions and malformed parameters", () => {
    const rejected = (() => {
      try {
        deterministicCronNext({
          count: 1,
          expression: "",
          from: "2026-01-02T03:04:05Z",
        });
      } catch (error: unknown) {
        return error;
      }
      return undefined;
    })();
    expect(rejected).toBeInstanceOf(AdapterError);
    expect(rejected).toMatchObject({
      code: -32_002,
      message: 'invalid cron expression "": empty spec string',
    });
    expect(() =>
      deterministicCronNext({
        count: 0,
        expression: "@daily",
        from: "2026-01-02T03:04:05Z",
      })
    ).toThrow("count must be positive");
    expect(() =>
      deterministicCronNext({
        count: 1,
        expression: "@daily",
        from: "2026-01-02T03:04:05",
      })
    ).toThrow("from must be an RFC 3339 time with an offset");
  });

  it.each([
    [1, 42n, 0n, 1_007_585_966n],
    [2, 42n, 123n, 15_351_903_370n],
    [3, 9_007_199_254_740_991n, 18_446_744_073_709_551_615n, 83_973_474_471n],
    [11, 1n, 456n, 14_472_375_008_645n],
    [310, 42n, 123n, 9_223_372_036_854_775_807n],
  ])("matches retry count %i", (errorCount, jobId, seed, expected) => {
    expect(
      deterministicRetryDelayNanoseconds({
        errorCount,
        jobId,
        now: Temporal.Instant.from("2026-01-02T03:04:05.6789Z"),
        seed,
      })
    ).toBe(expected);
  });

  it("matches sorted and Go-escaped arguments", () => {
    expect(
      deterministicUniqueKey({
        args: {
          zeta: 'quoted \\"value\\" and \\\\ slash',
          alpha: "<alpha>&\u2028line",
          maximum: 9_007_199_254_740_991,
        },
        kind: "conformance_all_args",
        now: "2026-01-02T03:04:05.6789Z",
        options: { by_args: true, by_period_nanos: 0 },
        queue: "default",
        scheduled_at: null,
        selected_unique_paths: null,
      })
    ).toEqual({
      sha256:
        "7a84c62c8d470ca388a0a1e41c311b9eb1ea21f7b88157ceb876fc82e698b6af",
      state_mask: 245,
    });
  });

  it("hashes empty all-args arrays as an object and rejects non-objects", () => {
    const request = {
      kind: "conformance_all_args",
      now: "2026-01-02T03:04:05.6789Z",
      options: { by_args: true, by_period_nanos: 0 },
      queue: "default",
      scheduled_at: null,
      selected_unique_paths: null,
    };
    expect(deterministicUniqueKey({ ...request, args: [] })).toEqual({
      sha256:
        "fe05a58ddb79a8d4544da962582d9a290d59788c920afd3597da3a62e3c1b0ac",
      state_mask: 245,
    });
    for (const args of [[1], null, "args"]) {
      const error = (() => {
        try {
          deterministicUniqueKey({ ...request, args });
        } catch (error: unknown) {
          return error;
        }
        return undefined;
      })();
      expect(error).toMatchObject({
        message: "unique args must encode a JSON object",
      });
      expect(adapterErrorCode(error)).toBe(ADAPTER_ERROR_CODE.rejected);
    }
  });

  it.each([
    [
      "period_from_non_utc_now",
      "2026-01-01T22:04:05.6789-05:00",
      null,
      "5396f06a082abd7a929915135ebd363a9a47d800176b03ce7736f93a5ba9e22e",
    ],
    [
      "period_from_non_utc_schedule",
      "2026-01-02T03:04:05.6789Z",
      "2026-01-02T10:51:05.6789+05:30",
      "b7f3c49952996b760b8b3ff6cf48f426e03a6ef0f004fb6faa51725365cf309a",
    ],
  ])("truncates %s periods in UTC", (_name, now, scheduledAt, sha256) => {
    expect(
      deterministicUniqueKey({
        args: { id: 42 },
        kind: "conformance_simple",
        now,
        options: {
          by_args: false,
          by_period_nanos: 3_600_000_000_000,
          by_queue: false,
          exclude_kind: false,
        },
        queue: "default",
        scheduled_at: scheduledAt,
        selected_unique_paths: null,
      })
    ).toEqual({ sha256, state_mask: 245 });
  });

  it("matches Go map ordering and negative zero", () => {
    expect(
      deterministicUniqueKey({
        args: { "2": 2, "10": 10, zero: -0, "😀": 1, "": 2 },
        kind: "conformance_all_args",
        now: "2026-01-02T03:04:05.6789Z",
        options: { by_args: true, by_period_nanos: 0 },
        queue: "default",
        scheduled_at: null,
        selected_unique_paths: null,
      }).sha256
    ).toBe("fcdf33e0c39c1fc7e956876345a985f2418bd69c6e4d6a5c794abf1e78cdfdb6");
  });

  it("matches selected nested arguments and year-one periods", () => {
    expect(
      deterministicUniqueKey({
        args: {
          account: { id: "acct-123", ignored: "not selected" },
          ignored: true,
          label: "selected",
        },
        kind: "conformance_selected_args",
        now: "2026-01-02T03:04:05.6789Z",
        options: { by_args: true, by_period_nanos: 0 },
        queue: "default",
        scheduled_at: null,
        selected_unique_paths: ["account.id", "label"],
      }).sha256
    ).toBe("6130dc4f753402d1faeb6bbc3e6c21415245bb282ad1fd16bcbfeebde525e726");

    expect(
      deterministicUniqueKey({
        args: { id: 42 },
        kind: "conformance_simple",
        now: "2026-01-02T03:04:05.6789Z",
        options: { by_period_nanos: 5_400_000_000_000n },
        queue: "default",
        scheduled_at: null,
        selected_unique_paths: null,
      }).sha256
    ).toBe("5396f06a082abd7a929915135ebd363a9a47d800176b03ce7736f93a5ba9e22e");
  });

  it("uses literal selected components for Unicode field names", () => {
    expect(
      deterministicUniqueKey({
        args: { user: {}, é: "café" },
        kind: "conformance_dotted_selected_args",
        now: "2026-01-02T03:04:05.6789Z",
        options: { by_args: true, by_period_nanos: 0 },
        queue: "default",
        scheduled_at: null,
        selected_unique_components: [["user", "id"], ["user.id"], ["é"]],
        selected_unique_paths: ["user.id", "user\\.id", "é"],
      }).sha256
    ).toBe("28513f484784e6b0fe8aed6cc1fadb04498f43305b74619aa56e701a2feff578");
  });

  it("preserves protocol-only integer boundaries as raw JSON numbers", () => {
    expect(
      deterministicUniqueKey({
        args: {
          exponent: 1e100,
          fraction: 1.25,
          maximum: 9_223_372_036_854_775_807n,
          minimum: -9_223_372_036_854_775_808n,
          unsigned_maximum: 18_446_744_073_709_551_615n,
        },
        kind: "conformance_numeric_boundaries",
        now: "2026-01-02T03:04:05.6789Z",
        options: { by_args: true, by_period_nanos: 0 },
        queue: "default",
        scheduled_at: null,
        selected_unique_paths: null,
      }).sha256
    ).toBe("2c1533b3ab43068407d14e82ddb34a295a51375ae3a27fef6931123f07677f38");
  });
});
