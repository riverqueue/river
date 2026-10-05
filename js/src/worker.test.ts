import { describe, expect, expectTypeOf, it } from "vitest";

import { defineJob, type JobDefinition } from "./job-definition.js";
import { createJobArgsTransformPlugin } from "./job-args-transform.js";
import type { JsonObject } from "./json.js";
import {
  cancel,
  complete,
  discard,
  snooze,
  workerRegistration,
  Workers,
  type WorkContext,
  type WorkHandlerFactory,
} from "./worker.js";

describe("Workers", () => {
  it("types completeTx with the declared transaction", () => {
    const definition = defineJob({ kind: "typed_tx" });
    new Workers<{ readonly name: string }>().add(
      definition,
      async ({ client, completeTx }) => {
        await completeTx({ name: "application transaction" });
        await client.insert(definition, {}, { tx: { name: "same" } });
        // @ts-expect-error -- narrowed to the declared transaction type.
        await completeTx({ other: true });
      }
    );
  });

  it("infers validated handler args and stores immutable policy", () => {
    const definition = defineJob({
      decode(value) {
        if (typeof value.message !== "string") {
          throw new TypeError("message must be a string");
        }
        return { message: value.message, validated: true as const };
      },
      kind: "email",
    });
    const workers = new Workers();

    workers.add(
      definition,
      ({ job, signal }) => {
        expectTypeOf(job.args).toEqualTypeOf<{
          message: string;
          validated: true;
        }>();
        expectTypeOf(job.rawArgs).toEqualTypeOf<JsonObject>();
        expectTypeOf(signal).toEqualTypeOf<AbortSignal>();
      },
      { timeout: { milliseconds: 1_000 } }
    );

    expect(workers.kinds()).toEqual(["email"]);
    expect(
      workerRegistration(workers, "email")?.options.timeout?.total("seconds")
    ).toBe(1);
    expect(Object.isFrozen(workerRegistration(workers, "email")?.options)).toBe(
      true
    );
  });

  it("registers a handler a factory builds for the definition", () => {
    const definition = defineJob({
      decode(value) {
        if (typeof value.message !== "string") {
          throw new TypeError("message must be a string");
        }
        return { message: value.message };
      },
      kind: "factory",
    });
    // An integration that needs the definition its handler is registered
    // with, typed from that definition at the call site.
    const seen: unknown[] = [];
    function integrationWorker<Definition extends JobDefinition>(
      handle: (context: WorkContext<Definition>, kind: string) => void
    ): WorkHandlerFactory<Definition> {
      return {
        createWorkHandler(registered) {
          seen.push(registered);
          return (context) => {
            handle(context, registered.kind);
          };
        },
      };
    }
    const workers = new Workers();

    workers.add(
      definition,
      integrationWorker((context, kind) => {
        expectTypeOf(context.job.args).toEqualTypeOf<{ message: string }>();
        expectTypeOf(kind).toEqualTypeOf<string>();
      })
    );

    expect(seen).toEqual([definition]);
    const registration = workerRegistration(workers, "factory");
    expect(registration?.type).toBe("in_process");
    expect(
      registration?.type === "in_process" && typeof registration.handler
    ).toBe("function");
    expect(() =>
      new Workers().add(definition, {
        createWorkHandler: () => "not a handler",
      } as never)
    ).toThrow("worker handler must be a function");
    expect("get" in workers).toBe(false);
  });

  it("registers a worker under its definition's kind aliases, like Go", () => {
    const renamed = defineJob({
      kind: "new_name",
      kindAliases: ["old_name"],
    });
    const workers = new Workers().add(renamed, () => undefined);

    expect(workers.kinds()).toEqual(["new_name", "old_name"]);
    expect(workerRegistration(workers, "old_name")).toBe(
      workerRegistration(workers, "new_name")
    );
    expect(workerRegistration(workers, "old_name")?.definition).toBe(renamed);
    // An alias can't take a kind another worker already has, or the reverse.
    expect(() =>
      workers.add(defineJob({ kind: "old_name" }), () => undefined)
    ).toThrow('worker already registered for job kind "old_name"');
    expect(() =>
      new Workers()
        .add(defineJob({ kind: "old_name" }), () => undefined)
        .add(renamed, () => undefined)
    ).toThrow('worker already registered for job kind "old_name"');
    expect(() => defineJob({ kind: "same", kindAliases: ["same"] })).toThrow(
      'job kind alias "same" repeats a kind of the same job'
    );
    expect(() => defineJob({ kind: "bad_alias", kindAliases: ["x"] })).toThrow(
      "job kind must be at least 2 characters"
    );
    expect(defineJob({ kind: "plain" }).kindAliases).toEqual([]);
  });

  it("rejects duplicates and invalid timeouts", () => {
    const definition = defineJob({ kind: "duplicate" });
    const workers = new Workers().add(definition, () => undefined);

    expect(() => workers.add(definition, () => undefined)).toThrow(
      "worker already registered"
    );
    expect(() =>
      new Workers().add(definition, () => undefined, {
        timeout: { milliseconds: 0 },
      })
    ).toThrow("worker timeout must be positive");
    expect(() =>
      new Workers().add(definition, () => undefined, {
        // @ts-expect-error -- bare numbers are ambiguous and rejected.
        timeout: 1_000,
      })
    ).toThrow("not a bare number");
    expect(
      workerRegistration(
        new Workers().add(defineJob({ kind: "no_timeout" }), () => undefined, {
          timeout: null,
        }),
        "no_timeout"
      )?.options.timeout
    ).toBeNull();
    expect(() =>
      new Workers().add(definition, () => undefined, {
        plugins: [
          createJobArgsTransformPlugin({
            name: "wrong-scope",
            onRead: ({ args }) => args,
          }),
        ],
      })
    ).toThrow("must be configured on Client");
  });
});

describe("work outcomes", () => {
  it("constructs closed immutable discriminated values", () => {
    expect(cancel()).toEqual({ type: "cancel" });
    expect(cancel({ reason: "account closed" })).toEqual({
      reason: "account closed",
      type: "cancel",
    });
    expect(Object.isFrozen(cancel({ reason: "account closed" }))).toBe(true);
    expect(complete()).toEqual({ type: "complete" });
    expect(complete({ output: { id: "provider-id" } })).toEqual({
      output: { id: "provider-id" },
      type: "complete",
    });
    expect(discard({ reason: "not found" })).toEqual({
      reason: "not found",
      type: "discard",
    });
    const snoozed = (duration: Temporal.DurationLike) =>
      snooze(duration).duration.total("milliseconds");
    expect(snooze({ seconds: 30 })).toEqual({
      duration: Temporal.Duration.from({ seconds: 30 }),
      type: "snooze",
    });
    expect(snoozed({ milliseconds: 1 })).toBe(1);
    expect(snoozed({ minutes: 1, seconds: 30 })).toBe(90_000);
    expect(snoozed(Temporal.Duration.from({ hours: 1 }))).toBe(3_600_000);
    expect(snoozed({ seconds: 0 })).toBe(0);
    // Like the runtime, a snooze rounds up to whole milliseconds.
    expect(snoozed({ microseconds: 1 })).toBe(1);
    expect(Object.isFrozen(snooze({ seconds: 30 }))).toBe(true);
  });

  it("validates outcome values", () => {
    expect(() => snooze({ seconds: -1 })).toThrow("must not be negative");
    expect(() => snooze({ months: 1 })).toThrow("calendar units");
    // @ts-expect-error -- bare numbers are ambiguous and rejected.
    expect(() => snooze(1_000)).toThrow("not a bare number");
    expect(() => discard({ reason: "" })).toThrow("must not be empty");
    expect(() => cancel({ reason: "" })).toThrow("non-empty string");
  });
});
