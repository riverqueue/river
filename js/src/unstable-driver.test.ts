import { createHash } from "node:crypto";

import { describe, expect, it } from "vitest";

import { Client } from "./client.js";
import { ValidationError } from "./errors.js";
import { registerDriver } from "./internal/driver-registry.js";
import type { JsonObject } from "./json.js";
import {
  buildUniqueKey,
  createJobArgsTransformPlugin,
  createJobInsertMetadataTransformPlugin,
  encodeUniqueArgs,
} from "./unstable-driver.js";

/** A registered insert-only driver that never inserts. */
class UnusedDriver {
  declare readonly "~river"?: {
    readonly capability: "insert";
    readonly transaction: never;
  };

  jobInsert(): never {
    throw new Error("not used");
  }

  jobInsertMany(): never {
    throw new Error("not used");
  }
}
const driver = new UnusedDriver();
registerDriver(driver, {
  backend: "fake",
  capability: "insert",
  operations: driver,
});

describe("transform plugins", () => {
  it("keep their transformers off the plugin object", () => {
    const plugins = [
      createJobArgsTransformPlugin({
        name: "args",
        onRead: ({ args }) => args,
      }),
      createJobInsertMetadataTransformPlugin({
        name: "metadata",
        onInsert: ({ metadata }) => ({ metadata }),
      }),
    ];
    for (const plugin of plugins) {
      // The name, and a brand without the transformer.
      const keys = Reflect.ownKeys(plugin);
      expect(keys).toHaveLength(2);
      expect(keys[0]).toBe("name");
      expect(Reflect.get(plugin, keys[1] as symbol)).toBe(true);
      expect(Object.isFrozen(plugin)).toBe(true);
    }
  });

  it("reject plugins from another installed copy, and spread copies", async () => {
    // Second copies of the plugin modules, as another installation loads.
    const argsSpecifier = "./job-args-transform.js?second-copy";
    const metadataSpecifier = "./job-insert-metadata-transform.js?second-copy";
    const foreignArgs = (await import(
      /* @vite-ignore */ argsSpecifier
    )) as typeof import("./job-args-transform.js");
    const foreignMetadata = (await import(
      /* @vite-ignore */ metadataSpecifier
    )) as typeof import("./job-insert-metadata-transform.js");
    expect(foreignArgs.createJobArgsTransformPlugin).not.toBe(
      createJobArgsTransformPlugin
    );
    expect(foreignMetadata.createJobInsertMetadataTransformPlugin).not.toBe(
      createJobInsertMetadataTransformPlugin
    );
    const plugins = [
      foreignArgs.createJobArgsTransformPlugin({
        name: "foreign-args",
        onInsert: ({ args, encodedArgs }) => ({ args, encodedArgs }),
        onRead: ({ args }) => args,
      }),
      foreignMetadata.createJobInsertMetadataTransformPlugin({
        name: "foreign-metadata",
        onInsert: ({ metadata }) => ({ metadata }),
      }),
      {
        ...createJobArgsTransformPlugin({
          name: "spread",
          onRead: ({ args }) => args,
        }),
      },
    ];
    for (const plugin of plugins) {
      expect(() => new Client(driver, { plugins: [plugin] })).toThrow(
        /another installed copy of riverqueue; check `npm ls riverqueue`/
      );
    }
  });
});

describe("encodeUniqueArgs", () => {
  const args = { "<k>": 3, a: { b: 1, z: [2] }, "a-c": 2, id: "x" };

  it.each([
    [true as const, '{"<k>":3,"a":{"b":1,"z":[2]},"a-c":2,"id":"x"}'],
    [["a.b", "a-c", "<k>"], '{"<k>":3,"a-c":2,"a":{"b":1}}'],
    [["missing"], ""],
  ])("encodes %j exactly as unique keys hash it", (byArgs, expected) => {
    expect(encodeUniqueArgs(args, byArgs)).toBe(expected);
    const [key] = buildUniqueKey(
      {
        args,
        kind: "k",
        queue: "default",
        scheduledAt: Temporal.Instant.fromEpochMilliseconds(0),
      },
      { byArgs, excludeKind: true }
    );
    expect(Buffer.from(key).toString("hex")).toBe(
      createHash("sha256").update(`&args=${expected}`).digest("hex")
    );
  });

  it("distinguishes literal dotted keys from nested paths", () => {
    const dotted = { "a.b": 1, a: { b: 2 }, ":lead": 3 };
    expect(encodeUniqueArgs(dotted, ["a\\.b"])).toBe('{"a.b":1}');
    expect(encodeUniqueArgs(dotted, ["a.b"])).toBe('{"a":{"b":2}}');
    expect(encodeUniqueArgs(dotted, ["a\\.b", "a.b"])).toBe(
      '{"a":{"b":2},"a.b":1}'
    );
    expect(encodeUniqueArgs(dotted, [":lead"])).toBe('{":lead":3}');
    expect(encodeUniqueArgs(dotted, ["\\:lead"])).toBe('{":lead":3}');
    expect(() => encodeUniqueArgs(args, [])).toThrow(ValidationError);
    expect(encodeUniqueArgs({ "a#b": 1 }, true)).toBe('{"a#b":1}');
  });

  it("rejects non-object arguments like Go", () => {
    // Go hashes an empty array as `{}` only when every argument is hashed.
    expect(encodeUniqueArgs([] as unknown as JsonObject, true)).toBe("{}");
    for (const value of [[1], [[]], [{}], 1, true, null, "text", []]) {
      const nonObject = value as unknown as JsonObject;
      if (!Array.isArray(value) || value.length > 0) {
        expect(() => encodeUniqueArgs(nonObject, true)).toThrow(
          new ValidationError("unique args must encode a JSON object")
        );
      }
      expect(() => encodeUniqueArgs(nonObject, ["a"])).toThrow(
        new ValidationError("unique args must encode a JSON object")
      );
    }
  });
});
