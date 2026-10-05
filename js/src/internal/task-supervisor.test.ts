import { describe, expect, it } from "vitest";

import { BackgroundTaskError, TaskSupervisor } from "./task-supervisor.js";

describe("TaskSupervisor", () => {
  it("aborts sibling tasks and reports a fatal rejection", async () => {
    const supervisor = new TaskSupervisor();
    const failure = new Error("database disconnected");
    let siblingReason: unknown;

    supervisor.start("listener", async () => {
      throw failure;
    });
    supervisor.start("producer", async (signal) => {
      await new Promise<void>((resolve) => {
        signal.addEventListener(
          "abort",
          () => {
            siblingReason = signal.reason;
            resolve();
          },
          { once: true }
        );
      });
    });

    await expect(supervisor.completed).rejects.toMatchObject({
      cause: failure,
      name: "BackgroundTaskError",
      taskName: "listener",
    });
    await expect(supervisor.stop()).rejects.toBeInstanceOf(BackgroundTaskError);
    expect(siblingReason).toBeInstanceOf(BackgroundTaskError);
  });

  it("treats unexpected task completion as fatal", async () => {
    const supervisor = new TaskSupervisor();

    supervisor.start("listener", async () => undefined);

    await expect(supervisor.completed).rejects.toMatchObject({
      taskName: "listener",
    });
  });

  it("allows explicitly finite tasks", async () => {
    const supervisor = new TaskSupervisor();

    supervisor.start("initial-sync", async () => undefined, {
      allowCompletion: true,
    });
    await Promise.resolve();
    await Promise.resolve();

    expect(supervisor.activeTaskNames).toEqual([]);
    await supervisor.stop();
    await expect(supervisor.completed).resolves.toBeUndefined();
  });

  it("gracefully aborts and waits for active tasks", async () => {
    const supervisor = new TaskSupervisor();
    const observed: unknown[] = [];
    supervisor.start("producer", async (signal) => {
      await new Promise<void>((resolve) => {
        signal.addEventListener(
          "abort",
          () => {
            observed.push(signal.reason);
            resolve();
          },
          { once: true }
        );
      });
    });

    const reason = new Error("graceful stop");
    await supervisor.stop(reason);

    expect(observed).toEqual([reason]);
    await expect(supervisor.completed).resolves.toBeUndefined();
  });

  it("rejects duplicate and post-shutdown task names", async () => {
    const supervisor = new TaskSupervisor();
    supervisor.start("producer", async (signal) => {
      await new Promise<void>((resolve) => {
        signal.addEventListener("abort", () => resolve(), { once: true });
      });
    });

    expect(() => supervisor.start("producer", async () => undefined)).toThrow(
      "already exists"
    );
    await supervisor.stop();
    expect(() => supervisor.start("later", async () => undefined)).toThrow(
      "after supervisor shutdown"
    );
  });
});
