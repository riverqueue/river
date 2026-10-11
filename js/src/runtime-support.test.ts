import { afterEach, describe, expect, it, vi } from "vitest";

import { ConfigurationError } from "./errors.js";
import { assertRuntimeSupport } from "./runtime-support.js";

describe("assertRuntimeSupport", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("accepts the supported Node runtime and Temporal implementation", () => {
    expect(() => assertRuntimeSupport()).not.toThrow();
    expect(
      Temporal.Instant.from("2026-08-30T18:00:00.123456Z").toString()
    ).toBe("2026-08-30T18:00:00.123456Z");
  });

  it("points runtimes without Temporal at the requirements", () => {
    vi.stubGlobal("Temporal", undefined);

    expect(() => assertRuntimeSupport()).toThrow(ConfigurationError);
    expect(() => assertRuntimeSupport()).toThrow(
      /Temporal API.*river\/tree\/master\/js#requirements/u
    );
  });

  it("rejects Node versions older than 24", () => {
    vi.stubGlobal("process", {
      ...process,
      versions: { ...process.versions, node: "22.0.0" },
    });

    expect(() => assertRuntimeSupport()).toThrow(
      /requires Node.js 24 or newer; found 22.0.0/u
    );
  });
});
