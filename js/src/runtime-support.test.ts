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

  it.each([undefined, null, {}, { Instant: {} }])(
    "points runtimes with missing or incomplete Temporal (%j) at the requirements",
    (temporal) => {
      vi.stubGlobal("Temporal", temporal);

      expect(() => assertRuntimeSupport()).toThrow(ConfigurationError);
      expect(() => assertRuntimeSupport()).toThrow(
        /preload a Temporal polyfill before importing River.*river\/tree\/master\/js#temporal-on-nodejs-24/u
      );
      expect(Reflect.get(globalThis, "Temporal")).toBe(temporal);
    }
  );

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
