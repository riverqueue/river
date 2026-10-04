import * as v from "valibot";
import { describe, expect, expectTypeOf, it } from "vitest";
import { z } from "zod";

import { PayloadValidationError } from "./errors.js";
import type { JsonObject } from "./json.js";
import {
  decodeJobArgs,
  defineJob,
  isJobDefinition,
  prepareJobInput,
  type JobDefinitionArgs,
  type JobDefinitionInput,
  type StandardSchemaV1,
} from "./job-definition.js";

describe("defineJob", () => {
  it("creates an immutable definition and snapshots defaults", () => {
    const tags = ["one-tag"];
    const definition = defineJob({
      defaults: { queue: "work", tags },
      kind: "immutable",
    });
    tags.push("later-tag");

    expect(Object.isFrozen(definition)).toBe(true);
    expect(Object.isFrozen(definition.defaults)).toBe(true);
    expect(definition.defaults.tags).toEqual(["one-tag"]);
    expect(isJobDefinition(definition)).toBe(true);
    expect(isJobDefinition({ defaults: {}, kind: "immutable" })).toBe(false);
  });

  it("types unchecked definitions as JSON objects on both sides", async () => {
    const definition = defineJob({ kind: "unchecked" });
    const decoded = await decodeJobArgs(definition, { value: "hello" });

    expectTypeOf(definition.kind).toEqualTypeOf<"unchecked">();
    expectTypeOf<
      JobDefinitionInput<typeof definition>
    >().toEqualTypeOf<JsonObject>();
    expectTypeOf(decoded).toEqualTypeOf<JsonObject>();
    expect(decoded).toEqual({ value: "hello" });

    // Without a type argument, the curried form takes JSON objects too.
    const curried = defineJob()({ kind: "curried_unchecked" });
    expect(curried.kind).toBe("curried_unchecked");
    expectTypeOf<
      JobDefinitionInput<typeof curried>
    >().toEqualTypeOf<JsonObject>();
  });

  it("keeps worker args unvalidated when only a producer type is declared", async () => {
    interface Input {
      value: string;
    }
    const definition = defineJob<Input>()({ kind: "declared" });
    const decoded = await decodeJobArgs(definition, { value: 1 });

    expectTypeOf(definition.kind).toEqualTypeOf<"declared">();
    expectTypeOf<
      JobDefinitionInput<typeof definition>
    >().toEqualTypeOf<Input>();
    expectTypeOf(decoded).toEqualTypeOf<JsonObject>();
    expect(decoded).toEqual({ value: 1 });
  });

  it("infers Standard Schema input and transformed worker output", async () => {
    const definition = defineJob({
      kind: "standard",
      schema: {
        "~standard": {
          types: undefined as unknown as {
            input: { count: number };
            output: { count: number; doubled: number };
          },
          validate(value: unknown) {
            const input = value as { count?: unknown };
            return typeof input.count === "number"
              ? { value: { count: input.count, doubled: input.count * 2 } }
              : {
                  issues: [
                    { message: "count must be a number", path: ["count"] },
                  ],
                };
          },
          vendor: "river-test",
          version: 1 as const,
        },
      },
    });

    const persisted = await prepareJobInput(definition, { count: 2 });
    const decoded = await decodeJobArgs(definition, persisted);
    expectTypeOf(decoded).toEqualTypeOf<{ count: number; doubled: number }>();
    expect(persisted).toEqual({ count: 2 });
    expect(decoded).toEqual({ count: 2, doubled: 4 });
  });

  it("works with Zod and Valibot schemas", async () => {
    const zodJob = defineJob({
      kind: "zod_job",
      schema: z.object({
        count: z.number().int().default(1),
        to: z.email(),
      }),
    });
    const valibotJob = defineJob({
      kind: "valibot_job",
      schema: v.object({
        to: v.pipe(
          v.string(),
          v.email(),
          v.transform((value) => value.toLowerCase())
        ),
      }),
    });

    expectTypeOf<JobDefinitionInput<typeof zodJob>>().toEqualTypeOf<{
      count?: number | undefined;
      to: string;
    }>();
    expectTypeOf<JobDefinitionArgs<typeof zodJob>>().toEqualTypeOf<{
      count: number;
      to: string;
    }>();
    expectTypeOf<JobDefinitionArgs<typeof valibotJob>>().toEqualTypeOf<{
      to: string;
    }>();

    await expect(
      decodeJobArgs(zodJob, { to: "someone@example.com" })
    ).resolves.toEqual({ count: 1, to: "someone@example.com" });
    await expect(
      decodeJobArgs(valibotJob, { to: "Someone@Example.com" })
    ).resolves.toEqual({ to: "someone@example.com" });
    await expect(
      prepareJobInput(zodJob, { to: "not-an-email" })
    ).rejects.toMatchObject({
      code: "payload_validation",
      kind: "zod_job",
      phase: "insert",
    });
    await expect(decodeJobArgs(valibotJob, { to: 5 })).rejects.toMatchObject({
      code: "payload_validation",
      phase: "work",
    });
  });

  it("rejects schemas whose input is not JSON at definition time", () => {
    expect(() =>
      defineJob({
        kind: "not_json",
        // @ts-expect-error -- Date values cannot be persisted as River JSON.
        schema: v.object({ at: v.date() }),
      })
    ).not.toThrow();
  });

  it("accepts interface producer types", () => {
    interface EmailArgs {
      readonly cc?: readonly string[];
      readonly to: string;
    }
    const definition = defineJob({
      decode(value): EmailArgs {
        if (typeof value.to !== "string") throw new TypeError("to required");
        return { to: value.to };
      },
      kind: "interface_args",
    });

    expect(definition.kind).toBe("interface_args");
    expectTypeOf<
      JobDefinitionInput<typeof definition>
    >().toEqualTypeOf<EmailArgs>();
    expectTypeOf<
      JobDefinitionArgs<typeof definition>
    >().toEqualTypeOf<EmailArgs>();
  });

  it("requires an explicit producer type when a decoder returns non-JSON args", async () => {
    const definition = defineJob({
      decode(value) {
        if (typeof value.at !== "string") throw new TypeError("at required");
        return { at: Temporal.Instant.from(value.at) };
      },
      kind: "non_json_args",
    });
    const input = { at: "2026-01-01T00:00:00Z" };
    // @ts-expect-error -- producers must declare a JSON input type.
    await prepareJobInput(definition, input);

    const declared = defineJob<{ at: string }>()({
      decode(value) {
        if (typeof value.at !== "string") throw new TypeError("at required");
        return { at: Temporal.Instant.from(value.at) };
      },
      kind: "declared_non_json",
    });
    await expect(prepareJobInput(declared, input)).resolves.toEqual(input);
    const decoded = await decodeJobArgs(declared, input);
    expectTypeOf(decoded).toEqualTypeOf<{ at: Temporal.Instant }>();
    expect(decoded.at.epochMilliseconds).toBe(Date.UTC(2026, 0, 1));
  });

  it("wraps decoder failures as payload validation errors", async () => {
    const definition = defineJob({
      async decode(value) {
        await Promise.resolve();
        if (typeof value.value !== "string") {
          throw new TypeError("value must be a string");
        }
        return { length: value.value.length };
      },
      kind: "decoded",
    });

    const decoded = await decodeJobArgs(definition, { value: "river" });
    expectTypeOf(decoded).toEqualTypeOf<{ length: number }>();
    expect(decoded).toEqual({ length: 5 });

    const failure = await decodeJobArgs(definition, { value: 1 }).catch(
      (error: unknown) => error
    );
    expect(failure).toBeInstanceOf(PayloadValidationError);
    expect(failure).toMatchObject({
      cause: expect.any(TypeError),
      message: 'invalid payload for job kind "decoded": value must be a string',
      phase: "work",
    });
  });

  it("rejects invalid definitions and payloads", async () => {
    expect(() => defineJob({ kind: "" })).toThrow(
      "start with a letter, number, or underscore"
    );
    expect(() => defineJob({ kind: " river" })).toThrow(
      "start with a letter, number, or underscore"
    );
    expect(() => defineJob({ kind: "a" })).toThrow("at least 2 characters");
    expect(() => defineJob({ kind: "has,comma" })).toThrow(
      "start with a letter, number, or underscore"
    );
    expect(() => defineJob({ kind: ":leading" })).toThrow(
      "start with a letter, number, or underscore"
    );
    expect(() => defineJob({ kind: "with[brackets]" })).not.toThrow();
    expect(() => defineJob({ kind: "river_internal_task" })).toThrow(
      "reserved"
    );
    expect(() =>
      defineJob({
        kind: "bad_schema",
        schema: {} as unknown as StandardSchemaV1<JsonObject>,
      })
    ).toThrow("Standard Schema");

    const definition = defineJob({
      kind: "invalid_payload",
      schema: {
        "~standard": {
          validate: () => ({ issues: [{ message: "no", path: ["a", 0] }] }),
          vendor: "river-test",
          version: 1 as const,
        },
      },
    });
    await expect(prepareJobInput(definition, {})).rejects.toMatchObject({
      code: "payload_validation",
      kind: "invalid_payload",
      message: 'invalid payload for job kind "invalid_payload": a.0: no',
      phase: "insert",
    });
    await expect(
      prepareJobInput(defineJob({ kind: "plain" }), [] as unknown as JsonObject)
    ).rejects.toMatchObject({ code: "validation" });
  });
});
