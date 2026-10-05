import { afterEach, describe, expect, it, vi } from "vitest";

import { ConfigurationError } from "./errors.js";
import { assertRuntimeSupport } from "./runtime-support.js";

describe("assertRuntimeSupport", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("accepts the supported Node runtime and native Temporal implementation", () => {
    expect(() => assertRuntimeSupport()).not.toThrow();
    expect(
      Temporal.Instant.from("2026-08-30T18:00:00.123456Z").toString()
    ).toBe("2026-08-30T18:00:00.123456Z");
  });

  it("points runtimes without native Temporal at the requirements", () => {
    vi.stubGlobal("Temporal", undefined);

    expect(() => assertRuntimeSupport()).toThrow(ConfigurationError);
    expect(() => assertRuntimeSupport()).toThrow(
      /native Temporal API.*typeof Temporal.*river\/tree\/master\/js#requirements/u
    );
  });
});
