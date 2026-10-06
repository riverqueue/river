import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  ConfigurationError,
  exactJsonNumber,
  isExactJsonNumber,
  LifecycleError,
} from "riverqueue";
import type { JsonObject, JsonValue, WorkContext } from "riverqueue";

import { WorkerThreadHandlerError, WorkerThreads } from "./index.js";
import type { WorkerThreadModule } from "./index.js";
import type * as testHandlers from "./testdata/handlers.js";
import { richJob, testJob } from "./testdata/jobs.js";

// Only `handlers.ts` exists; threads resolve the `.js` name to the source.
const handlers: WorkerThreadModule<typeof testHandlers> = new URL(
  "./testdata/handlers.js",
  import.meta.url
);

// A thread failure must never surface as a host-process failure.
const hostFailures: unknown[] = [];
const recordHostFailure = (reason: unknown) => hostFailures.push(reason);
beforeAll(() => {
  process.on("uncaughtException", recordHostFailure);
  process.on("unhandledRejection", recordHostFailure);
});
afterAll(() => {
  process.off("uncaughtException", recordHostFailure);
  process.off("unhandledRejection", recordHostFailure);
  expect(hostFailures).toEqual([]);
});

describe("WorkerThreads", () => {
  it("bounds native thread concurrency", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const started = signals();
    const target = executor.handler(testJob, {
      exportName: "sleep",
      module: handlers,
    });
    const first = executor.start(
      context({ milliseconds: 30 }, started),
      target.handler
    );
    const second = executor.start(
      context({ milliseconds: 30 }, started),
      target.handler
    );

    await started.next();
    expect(executor.diagnostics()).toMatchObject({
      activeThreads: 1,
      pendingTasks: 1,
      totalThreads: 1,
    });

    await Promise.all([first.result, second.result]);
    expect(executor.diagnostics()).toMatchObject({
      activeThreads: 0,
      idleThreads: 1,
      pendingTasks: 0,
      totalThreads: 1,
    });
    await executor.close();
    expect(executor.diagnostics()).toMatchObject({ totalThreads: 0 });
  });

  it("reports an attempt as started only once a thread takes it", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const started = signals();
    const target = executor.handler(testJob, {
      exportName: "sleep",
      module: handlers,
    });
    const first = executor.start(
      context({ milliseconds: 20 }, started),
      target.handler
    );
    const second = executor.start(context({ milliseconds: 0 }), target.handler);
    let secondStarted = false;
    void second.started?.then(() => {
      secondStarted = true;
    });

    await first.started;
    await started.next();
    expect(secondStarted).toBe(false);

    await first.result;
    await second.started;
    await second.result;
    await executor.close();
  });

  it("cooperatively aborts and reuses a healthy thread", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const started = signals();
    const target = executor.handler(testJob, {
      exportName: "cooperate",
      module: handlers,
    });
    const handle = executor.start(context({}, started), target.handler);
    await started.next();

    const reason = new Error("cancelled");
    reason.name = "JobCancelledError";
    // The handler stopped on its own, so River classifies its rejection.
    await expect(
      handle.abort(reason, { gracePeriod: gracePeriod(100) })
    ).resolves.toEqual({
      terminated: false,
    });
    await expect(handle.result).rejects.toMatchObject({
      message: "cancelled",
      name: "JobCancelledError",
    });
    expect(executor.diagnostics()).toMatchObject({
      idleThreads: 1,
      totalThreads: 1,
    });

    const completeTarget = executor.handler(testJob, {
      exportName: "complete",
      module: handlers,
    });
    await expect(
      executor.start(context({ value: "reused" }), completeTarget.handler)
        .result
    ).resolves.toEqual({ output: { value: "reused" }, type: "complete" });
    expect(executor.diagnostics()).toMatchObject({ totalThreads: 1 });
    await executor.close();
  });

  it("forcibly terminates a synchronous loop without blocking the main event loop", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const started = signals();
    const target = executor.handler(testJob, {
      exportName: "spin",
      module: handlers,
    });
    const handle = executor.start(context({}, started), target.handler);
    await started.next();

    let mainLoopAdvanced = false;
    const mainLoopTimer = setTimeout(() => {
      mainLoopAdvanced = true;
    }, 0);
    await expect(
      handle.abort(new Error("stop"), { gracePeriod: gracePeriod(1) })
    ).resolves.toEqual({
      terminated: true,
    });
    await expect(handle.result).rejects.toThrow("stop");
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    clearTimeout(mainLoopTimer);
    expect(mainLoopAdvanced).toBe(true);
    expect(executor.diagnostics()).toMatchObject({ totalThreads: 0 });
    await executor.close();
  });

  it("shuts down active work and leaves no owned thread handles", async () => {
    const executor = new WorkerThreads({ maxThreads: 2 });
    const started = signals();
    const target = executor.handler(testJob, {
      exportName: "sleep",
      module: handlers,
    });
    const handle = executor.start(
      context({ milliseconds: 60_000 }, started),
      target.handler
    );
    await started.next();

    await executor.close();
    await expect(handle.result).rejects.toThrow(LifecycleError);
    expect(executor.diagnostics()).toEqual({
      activeThreads: 0,
      crashedThreads: 0,
      idleThreads: 0,
      pendingTasks: 0,
      totalThreads: 0,
    });
  });

  it("rejects queued and later tasks when closed", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const started = signals();
    const sleeping = executor.handler(testJob, {
      exportName: "sleep",
      module: handlers,
    });
    const running = executor.start(
      context({ milliseconds: 60_000 }, started),
      sleeping.handler
    );
    const queued = executor.start(
      context({ milliseconds: 0 }),
      sleeping.handler
    );
    await started.next();
    expect(executor.diagnostics()).toMatchObject({ pendingTasks: 1 });

    await executor.close();
    await expect(running.result).rejects.toThrow(
      "closed while the attempt was running"
    );
    await expect(queued.result).rejects.toThrow(
      "worker thread executor is closed"
    );
    await expect(
      executor.start(context({ milliseconds: 0 }), sleeping.handler).result
    ).rejects.toThrow(LifecycleError);
    expect(executor.diagnostics()).toMatchObject({
      pendingTasks: 0,
      totalThreads: 0,
    });
  });

  it("removes an aborted task from the queue without disturbing the running one", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const started = signals();
    const sleeping = executor.handler(testJob, {
      exportName: "sleep",
      module: handlers,
    });
    const running = executor.start(
      context({ milliseconds: 20 }, started),
      sleeping.handler
    );
    const queued = executor.start(
      context({ milliseconds: 0 }),
      sleeping.handler
    );
    await started.next();

    await expect(
      queued.abort(new Error("not needed"), { gracePeriod: gracePeriod(0) })
    ).resolves.toEqual({
      terminated: true,
    });
    await expect(queued.result).rejects.toThrow("not needed");
    expect(executor.diagnostics()).toMatchObject({ pendingTasks: 0 });
    await expect(running.result).resolves.toBeUndefined();
    await executor.close();
  });

  it("fences task messages while reusing a thread after handler failure", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const failed = executor.handler(testJob, {
      exportName: "fail",
      module: handlers,
    });
    await expect(
      executor.start(context({}), failed.handler).result
    ).rejects.toThrow("handler failed");

    const completed = executor.handler(testJob, {
      exportName: "complete",
      module: handlers,
    });
    await expect(
      executor.start(context({ value: "next" }), completed.handler).result
    ).resolves.toEqual({ output: { value: "next" }, type: "complete" });
    expect(executor.diagnostics()).toMatchObject({
      idleThreads: 1,
      totalThreads: 1,
    });
    await executor.close();
  });

  it("forwards recorded output before a failed isolated result", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const outputs: unknown[] = [];
    const target = executor.handler(testJob, {
      exportName: "outputThenFail",
      module: handlers,
    });
    await expect(
      executor.start(context({}, signals(), outputs), target.handler).result
    ).rejects.toThrow("failed after output");
    expect(outputs).toEqual([{ beforeFailure: true }]);
    await executor.close();
  });

  it("returns a snooze's duration from a thread", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const target = executor.handler(testJob, {
      exportName: "snooze",
      module: handlers,
    });
    const outcome = await executor.start(context({}), target.handler).result;
    expect(outcome).toEqual({
      duration: Temporal.Duration.from({ seconds: 30 }),
      type: "snooze",
    });
    expect(
      (outcome as { duration: Temporal.Duration }).duration
    ).toBeInstanceOf(Temporal.Duration);
    await executor.close();
  });

  it("forwards attempt metadata before an isolated result", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const metadata: Array<[string, unknown]> = [];
    const target = executor.handler(testJob, {
      exportName: "metadataThenComplete",
      module: handlers,
    });

    await expect(
      executor.start(
        context({}, signals(), [], (key, value) => metadata.push([key, value])),
        target.handler
      ).result
    ).resolves.toEqual({ type: "complete" });
    expect(metadata).toEqual([["thread", { forwarded: true }]]);
    await executor.close();
  });

  describe("args", () => {
    it("passes decoded args alongside the persisted input", async () => {
      const executor = new WorkerThreads({ maxThreads: 1 });
      const persisted = context({ value: "persisted" });
      const target = executor.handler(testJob, {
        exportName: "echoArgs",
        module: handlers,
      });

      await expect(
        executor.start(
          {
            ...persisted,
            job: { ...persisted.job, args: { value: "decoded" } },
          },
          target.handler
        ).result
      ).resolves.toEqual({
        output: {
          args: { value: "decoded" },
          rawArgs: { value: "persisted" },
        },
        type: "complete",
      });
      await executor.close();
    });

    it("carries structured-clone args that are not River JSON", async () => {
      const executor = new WorkerThreads({ maxThreads: 1 });
      const base = context({ at: "2026-09-01T00:00:00.000Z", big: "1" });
      const target = executor.handler(richJob, {
        exportName: "describeRich",
        module: handlers,
      });

      await expect(
        executor.start(
          {
            ...base,
            job: {
              ...base.job,
              args: {
                at: new Date("2026-09-01T00:00:00.000Z"),
                big: 2n ** 70n,
                bytes: new Uint8Array([1, 2, 3]),
                lookup: new Map([["one", 1]]),
                tags: new Set(["a", "b"]),
              },
              kind: richJob.kind,
            },
          },
          target.handler
        ).result
      ).resolves.toEqual({
        output: {
          at: "date:2026-09-01T00:00:00.000Z",
          big: (2n ** 70n).toString(),
          bytes: "bytes:3",
          lookup: "map:1",
          tags: "set:2",
        },
        type: "complete",
      });
      await executor.close();
    });

    it("rejects decoded args that would not arrive unchanged", async () => {
      class Money {
        constructor(readonly cents: bigint) {}
      }
      const executor = new WorkerThreads({ maxThreads: 1 });
      const base = context({});
      const target = executor.handler(testJob, {
        exportName: "complete",
        module: handlers,
      });

      for (const [args, path] of [
        [{ price: new Money(1n) }, "$.price is a Money instance"],
        [{ nested: [() => undefined] }, "$.nested[0] is a function"],
        [
          { big: 1n, id: exactJsonNumber("9007199254740993") },
          "$.id is an exact JSON number",
        ],
      ] as const) {
        expect(() =>
          executor.start(
            { ...base, job: { ...base.job, args } },
            target.handler
          )
        ).toThrow(path);
      }
      expect(executor.diagnostics()).toMatchObject({ totalThreads: 0 });
      await executor.close();
    });
  });

  it("preserves exact JSON numbers without leaking the only thread", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const exact = exactJsonNumber("9007199254740993");
    const logged: unknown[] = [];
    const metadata: Array<[string, unknown]> = [];
    const outputs: unknown[] = [];
    const base = context({ id: exact }, signals(), outputs, (key, value) =>
      metadata.push([key, value])
    );
    const target = executor.handler(testJob, {
      exportName: "echoExact",
      module: handlers,
    });

    const outcome = await executor.start(
      {
        ...base,
        logger: {
          ...base.logger,
          info: (attributes: unknown) => {
            logged.push(attributes);
          },
        },
      },
      target.handler
    ).result;

    const exactId = (value: unknown): string | undefined => {
      const id = (value as { id?: JsonValue } | undefined)?.id;
      return isExactJsonNumber(id) ? id.rawJSON : undefined;
    };
    expect(exactId((outcome as { output?: unknown }).output)).toBe(
      "9007199254740993"
    );
    expect(logged.map(exactId)).toEqual(["9007199254740993"]);
    expect(outputs.map(exactId)).toEqual(["9007199254740993"]);
    expect(
      metadata.map(([key, value]) => [key, exactId({ id: value })])
    ).toEqual([["id", "9007199254740993"]]);

    const complete = executor.handler(testJob, {
      exportName: "complete",
      module: handlers,
    });
    await expect(
      executor.start(context({ value: "next" }), complete.handler).result
    ).resolves.toEqual({ output: { value: "next" }, type: "complete" });
    expect(executor.diagnostics()).toMatchObject({
      activeThreads: 0,
      idleThreads: 1,
      totalThreads: 1,
    });
    await executor.close();
  });

  it("reports non-JSON outcomes without hanging or poisoning the thread", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const uncloneable = executor.handler(testJob, {
      exportName: "uncloneableOutcome",
      module: handlers,
    });

    await expect(
      executor.start(context({}), uncloneable.handler).result
    ).rejects.toThrow(/function/i);

    const complete = executor.handler(testJob, {
      exportName: "complete",
      module: handlers,
    });
    await expect(
      executor.start(context({ value: "reused" }), complete.handler).result
    ).resolves.toEqual({ output: { value: "reused" }, type: "complete" });
    await executor.close();
  });

  it("destroys a running thread when forwarding a log fails", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const started = signals();
    const poisoned = context({ milliseconds: 60_000 }, started);
    const sleeping = executor.handler(testJob, {
      exportName: "sleep",
      module: handlers,
    });
    const handle = executor.start(
      {
        ...poisoned,
        logger: {
          ...poisoned.logger,
          info: () => {
            started.send();
            throw new Error("log forwarding failed");
          },
        },
      },
      sleeping.handler
    );

    await started.next();
    await expect(handle.result).rejects.toThrow("log forwarding failed");

    const complete = executor.handler(testJob, {
      exportName: "complete",
      module: handlers,
    });
    await expect(
      executor.start(context({ value: "replacement" }), complete.handler).result
    ).resolves.toEqual({ output: { value: "replacement" }, type: "complete" });
    expect(executor.diagnostics()).toMatchObject({
      idleThreads: 1,
      totalThreads: 1,
    });
    await executor.close();
  });

  it("rejects handlers from another executor or for another job kind", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const other = new WorkerThreads({ maxThreads: 1 });
    const foreign = other.handler(testJob, {
      exportName: "complete",
      module: handlers,
    });
    const rich = executor.handler(richJob, {
      exportName: "describeRich",
      module: handlers,
    });

    expect(() => executor.start(context({}), foreign.handler)).toThrow(
      "not created by this executor"
    );
    expect(() => executor.start(context({}), rich.handler)).toThrow(
      'handler for job kind "test_rich" cannot work job kind "test"'
    );
    // Like the Workers registry, a handler also works its kind aliases.
    const renamedJob = {
      ...testJob,
      kind: "test_renamed",
      kindAliases: ["test"],
    } as unknown as typeof testJob;
    const renamed = executor.handler(renamedJob, {
      exportName: "complete",
      module: handlers,
    });
    await expect(
      executor.start(context({ value: "alias" }), renamed.handler).result
    ).resolves.toEqual({ output: { value: "alias" }, type: "complete" });
    expect(() =>
      executor.handler(
        testJob,
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion -- the compiler rejects this invalid export name without it
        {
          exportName: "",
          module: handlers,
        } as never
      )
    ).toThrow(ConfigurationError);
    expect(() =>
      executor.handler(testJob, {
        exportName: "complete",
        module: handlers.href,
      } as never)
    ).toThrow(ConfigurationError);
    await Promise.all([executor.close(), other.close()]);
  });

  it("type-checks export names against the module and definition", () => {
    const executor = new WorkerThreads({ maxThreads: 1 });

    executor.handler(testJob, { exportName: "complete", module: handlers });
    executor.handler(richJob, { exportName: "describeRich", module: handlers });
    // A plain URL has no module type, so any export name compiles.
    executor.handler(testJob, {
      exportName: "anything",
      module: new URL(handlers.href),
    });
    // @ts-expect-error The module has no such export.
    executor.handler(testJob, { exportName: "completee", module: handlers });
    // @ts-expect-error `nthSquare` is not a worker-thread handler.
    executor.handler(testJob, { exportName: "nthSquare", module: handlers });
    // @ts-expect-error `describeRich` handles a different definition's args.
    executor.handler(testJob, { exportName: "describeRich", module: handlers });
  });

  it("reports a missing handler module or export", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const missingModule = executor.handler(testJob, {
      exportName: "complete",
      module: new URL("./testdata/missing.js", import.meta.url),
    });
    const missingExport = executor.handler(testJob, {
      exportName: "missing",
      module: new URL(handlers.href),
    });

    await expect(
      executor.start(context({}), missingModule.handler).result
    ).rejects.toThrow(/missing\.js/);
    await expect(
      executor.start(context({}), missingExport.handler).result
    ).rejects.toThrow('ESM export "missing" is not a function');
    await executor.close();
  });

  it("bounds error records crossing the thread boundary", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const target = executor.handler(testJob, {
      exportName: "failWithHugeError",
      module: handlers,
    });

    const error: unknown = await executor
      .start(context({}), target.handler)
      .result.then(undefined, (thrown: unknown) => thrown);
    expect(error).toBeInstanceOf(WorkerThreadHandlerError);
    expect((error as Error).message).toHaveLength(32_768);
    expect((error as Error).stack?.length).toBeLessThanOrEqual(32_768);
    await executor.close();
  });

  it("reports an error whose getters throw without crashing its thread", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const failing = executor.handler(testJob, {
      exportName: "failWithThrowingGetters",
      module: handlers,
    });
    const healthy = executor.handler(testJob, {
      exportName: "complete",
      module: handlers,
    });

    const error: unknown = await executor
      .start(context({}), failing.handler)
      .result.then(undefined, (thrown: unknown) => thrown);
    expect(error).toBeInstanceOf(WorkerThreadHandlerError);
    expect(error).toMatchObject({
      message: "unreadable error message",
      name: "Error",
    });
    // The same thread keeps working.
    await expect(
      executor.start(context({ value: 1 }), healthy.handler).result
    ).resolves.toBeDefined();
    expect(executor.diagnostics()).toMatchObject({
      crashedThreads: 0,
      totalThreads: 1,
    });
    await executor.close();
  });

  it("bounds logs, output, and metadata sent from a thread", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const logged: Array<[string, unknown]> = [];
    const logging = executor.handler(testJob, {
      exportName: "logHuge",
      module: handlers,
    });
    const logContext = context({});
    await executor.start(
      {
        ...logContext,
        logger: {
          ...logContext.logger,
          info: (first: unknown, second?: unknown) => {
            logged.push(
              typeof first === "string"
                ? [first, undefined]
                : [String(second), first]
            );
          },
        },
      },
      logging.handler
    ).result;
    expect(logged[0]?.[0]).toHaveLength(32_768);
    expect(logged[1]?.[0]).toBe(
      "with attributes [log attributes omitted: 100011 characters]"
    );

    const outputs: unknown[] = [];
    const metadata: unknown[] = [];
    const oversized = executor.handler(testJob, {
      exportName: "oversizedOutput",
      module: handlers,
    });
    const error: unknown = await executor
      .start(
        context({}, signals(), outputs, (key) => metadata.push(key)),
        oversized.handler
      )
      .result.then(undefined, (thrown: unknown) => thrown);
    expect(metadata).toEqual([]);
    expect(outputs).toEqual([
      { metadataRejected: "job metadata value must not exceed 32 MiB" },
    ]);
    expect(error).toBeInstanceOf(WorkerThreadHandlerError);
    expect(error).toMatchObject({
      message: "job output must not exceed 32 MiB",
      name: "ValidationError",
    });
    await executor.close();
  });

  it("validates executor options", () => {
    expect(() => new WorkerThreads({ maxThreads: 0 })).toThrow(
      ConfigurationError
    );
    expect(
      () =>
        new WorkerThreads({
          maxThreads: 1,
          resourceLimits: { maxOldGenerationSizeMb: 0 },
        })
    ).toThrow(ConfigurationError);
    expect(
      () =>
        new WorkerThreads({
          maxThreads: 1,
          resourceLimits: { maxHeap: 1 } as never,
        })
    ).toThrow(ConfigurationError);
  });

  describe("thread failures", () => {
    it.each([
      ["an uncaught exception", "throwWhenIdle"],
      ["an unhandled rejection", "rejectWhenIdle"],
      ["process.exit()", "exitWhenIdle"],
    ] as const)(
      "evicts an idle thread that fails with %s and replaces it lazily",
      async (_failure, exportName) => {
        const executor = new WorkerThreads({ maxThreads: 1 });
        const failing = executor.handler(testJob, {
          exportName,
          module: handlers,
        });
        await expect(
          executor.start(context({}), failing.handler).result
        ).resolves.toEqual({ type: "complete" });

        await waitFor(() => executor.diagnostics().crashedThreads === 1);
        expect(executor.diagnostics()).toMatchObject({
          idleThreads: 0,
          totalThreads: 0,
        });

        const complete = executor.handler(testJob, {
          exportName: "complete",
          module: handlers,
        });
        await expect(
          executor.start(context({ value: "replacement" }), complete.handler)
            .result
        ).resolves.toEqual({
          output: { value: "replacement" },
          type: "complete",
        });
        expect(executor.diagnostics()).toMatchObject({
          crashedThreads: 1,
          idleThreads: 1,
          totalThreads: 1,
        });
        await executor.close();
      }
    );

    it("retries a task once when its reused thread dies before starting it", async ({
      onTestFinished,
    }) => {
      const executor = new WorkerThreads({ maxThreads: 1 });
      onTestFinished(() => executor.close());
      const exiting = executor.handler(testJob, {
        exportName: "exitBeforeNextTask",
        module: handlers,
      });
      const complete = executor.handler(testJob, {
        exportName: "complete",
        module: handlers,
      });

      const next = await executor
        .start(context({}), exiting.handler)
        .result.then(
          () =>
            executor.start(context({ value: "retried" }), complete.handler)
              .result
        );

      expect(next).toEqual({ output: { value: "retried" }, type: "complete" });
      expect(executor.diagnostics()).toMatchObject({
        crashedThreads: 1,
        totalThreads: 1,
      });
    });

    it("fails an attempt that exceeds its thread's heap limit", async () => {
      const executor = new WorkerThreads({
        maxThreads: 1,
        resourceLimits: {
          maxOldGenerationSizeMb: 16,
          maxYoungGenerationSizeMb: 4,
        },
      });
      const allocating = executor.handler(testJob, {
        exportName: "allocateForever",
        module: handlers,
      });

      await expect(
        executor.start(context({}), allocating.handler).result
      ).rejects.toThrow(/memory limit/);

      const complete = executor.handler(testJob, {
        exportName: "complete",
        module: handlers,
      });
      await expect(
        executor.start(context({ value: "after" }), complete.handler).result
      ).resolves.toEqual({ output: { value: "after" }, type: "complete" });
      expect(executor.diagnostics()).toMatchObject({
        crashedThreads: 1,
        totalThreads: 1,
      });
      await executor.close();
    });

    it("fails the running attempt when its thread crashes", async () => {
      const executor = new WorkerThreads({ maxThreads: 1 });
      const crashing = executor.handler(testJob, {
        exportName: "crashWhileRunning",
        module: handlers,
      });

      const result = executor.start(context({}), crashing.handler).result;
      await expect(result).rejects.toThrow(WorkerThreadHandlerError);
      await expect(result).rejects.toThrow("crashed in flight");

      const complete = executor.handler(testJob, {
        exportName: "complete",
        module: handlers,
      });
      await expect(
        executor.start(context({ value: "after" }), complete.handler).result
      ).resolves.toEqual({ output: { value: "after" }, type: "complete" });
      expect(executor.diagnostics()).toMatchObject({
        crashedThreads: 1,
        totalThreads: 1,
      });
      await executor.close();
    });
  });

  it("revives execution timestamps with the worker realm's native Temporal", async () => {
    const executor = new WorkerThreads({ maxThreads: 1 });
    const target = executor.handler(testJob, {
      exportName: "confirmNativeTemporal",
      module: handlers,
    });

    await expect(
      executor.start(context({}), target.handler).result
    ).resolves.toEqual({
      output: { startedAt: "2026-08-30T12:00:00.123456789Z" },
      type: "complete",
    });
    await executor.close();
  });
});

function context(
  args: JsonObject,
  started = signals(),
  outputs: unknown[] = [],
  metadata: (key: string, value: unknown) => void = () => undefined
): WorkContext {
  const now = Temporal.Instant.from("2026-08-30T12:00:00.123456789Z");
  return {
    client: {} as WorkContext["client"],
    completeTx: () =>
      Promise.reject(new Error("test context has no transaction")),
    execution: { attemptedBy: "worker-thread-test", startedAt: now },
    job: {
      args,
      attempt: 1,
      attemptedAt: now,
      attemptedBy: ["worker-thread-test"],
      createdAt: now,
      errors: [],
      finalizedAt: null,
      id: 42n,
      kind: "test",
      maxAttempts: 3,
      metadata: {},
      priority: 1,
      queue: "default",
      rawArgs: args,
      scheduledAt: now,
      state: "running",
      tags: [],
      uniqueKey: null,
      uniqueStates: null,
    },
    logger: {
      debug: () => undefined,
      error: () => undefined,
      info: (_attributes: unknown, message?: string) => {
        if (message === "started") started.send();
      },
      warn: () => undefined,
    },
    recordOutput: (value) => outputs.push(value),
    resumable: {} as WorkContext["resumable"],
    setMetadata: metadata,
    signal: new AbortController().signal,
  };
}

function gracePeriod(milliseconds: number): Temporal.Duration {
  return Temporal.Duration.from({ milliseconds });
}

/** Poll between event-loop turns until a condition driven by thread events holds. */
async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("condition was not reached");
    await new Promise<void>((resolve) => setTimeout(resolve, 1));
  }
}

function signals(): { next(): Promise<void>; send(): void } {
  const queue: Array<() => void> = [];
  let buffered = 0;
  return {
    next: () =>
      buffered > 0
        ? (buffered--, Promise.resolve())
        : new Promise<void>((resolve) => queue.push(resolve)),
    send: () => {
      const resolve = queue.shift();
      if (resolve === undefined) buffered++;
      else resolve();
    },
  };
}
