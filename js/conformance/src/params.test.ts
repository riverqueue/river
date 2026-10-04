import { describe, expect, it } from "vitest";

import { ADAPTER_ERROR_CODE } from "./errors.js";
import {
  deleteFinalizedParams,
  optionalBigInts,
  optionalIntegers,
  optionalRawJsonObject,
  optionalRecord,
  requiredInteger,
  requiredNonEmptyString,
  requiredString,
} from "./params.js";

describe("conformance parameter decoders", () => {
  it("rejects a missing or out-of-range required integer", () => {
    expect(requiredInteger({ value: 3 }, "value", 1, 4)).toBe(3);
    expect(() => requiredInteger({}, "value", 1, 4)).toThrow(
      "value must be an integer between 1 and 4"
    );
    expect(() => requiredInteger({ value: 5 }, "value", 1, 4)).toThrow(
      expect.objectContaining({ code: ADAPTER_ERROR_CODE.invalidParams })
    );
    expect(() => requiredInteger({ value: "3" }, "value", 1, 4)).toThrow(
      expect.objectContaining({ code: ADAPTER_ERROR_CODE.invalidParams })
    );
  });

  it("treats JSON null optional collections as absent", () => {
    expect(optionalBigInts({ ids: null }, "ids")).toEqual([]);
    expect(optionalIntegers({ priorities: null }, "priorities", 1, 4)).toEqual(
      []
    );
    expect(optionalRecord({ metadata: null }, "metadata")).toBeNull();
    expect(() =>
      optionalIntegers({ priorities: [1, 9] }, "priorities", 1, 4)
    ).toThrow("priorities[1] must be an integer between 1 and 4");
  });

  it("decodes delete_finalized, keeping a null included list apart from an empty one", () => {
    const before = Temporal.Instant.from("2026-01-02T03:04:05Z");
    expect(
      deleteFinalizedParams({
        before: "2026-01-02T03:04:05Z",
        limit: 2,
        queues_excluded: ["kept"],
        queues_included: null,
      })
    ).toEqual({
      cancelledBefore: before,
      completedBefore: before,
      discardedBefore: before,
      limit: 2,
      queuesExcluded: ["kept"],
      queuesIncluded: null,
    });
    expect(
      deleteFinalizedParams({ before: "2026-01-02T03:04:05Z", limit: 1 })
        .queuesIncluded
    ).toBeNull();
    expect(
      deleteFinalizedParams({
        before: "2026-01-02T03:04:05Z",
        limit: 1,
        queues_included: [],
      }).queuesIncluded
    ).toEqual([]);
    expect(() => deleteFinalizedParams({ before: "soon", limit: 1 })).toThrow(
      expect.objectContaining({ code: ADAPTER_ERROR_CODE.invalidParams })
    );
    expect(() =>
      deleteFinalizedParams({ before: "2026-01-02T03:04:05Z", limit: 0 })
    ).toThrow(
      expect.objectContaining({ code: ADAPTER_ERROR_CODE.invalidParams })
    );
  });

  it("distinguishes possibly empty and non-empty strings", () => {
    expect(requiredString({ name: "" }, "name")).toBe("");
    expect(() => requiredString({}, "name")).toThrow(
      expect.objectContaining({ code: ADAPTER_ERROR_CODE.invalidParams })
    );
    expect(requiredNonEmptyString({ name: "a" }, "name")).toBe("a");
    expect(() => requiredNonEmptyString({ name: "" }, "name")).toThrow(
      "name must be a non-empty string"
    );
  });

  it("retains raw metadata JSON with large numeric tokens", () => {
    const raw =
      '{"beyond_float":1e400,"big_integer":123456789012345678901234567890}';
    expect(
      optionalRawJsonObject({ metadata_json: raw }, "metadata_json", "{}")
    ).toBe(raw);
    expect(optionalRawJsonObject({}, "metadata_json", "{}")).toBe("{}");
    for (const value of ["[1]", "{broken", 3]) {
      expect(() =>
        optionalRawJsonObject({ metadata_json: value }, "metadata_json", "{}")
      ).toThrow(
        expect.objectContaining({ code: ADAPTER_ERROR_CODE.invalidParams })
      );
    }
  });
});
