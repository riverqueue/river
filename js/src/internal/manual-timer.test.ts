import { AssertionError } from "node:assert";

import { describe, expect, test } from "vitest";

import { ManualTimer } from "./manual-timer.js";

describe("ManualTimer", () => {
  test("fires delays and timeouts in order only as the clock advances", async () => {
    const timer = new ManualTimer();
    const fired: string[] = [];
    void timer.delay(20, new AbortController().signal).then(() => {
      fired.push("delay 20");
      // A timer created while advancing fires within the same advance.
      void timer.delay(5, new AbortController().signal).then(() => {
        fired.push("delay 5 more");
      });
    });
    const timeout = timer.timeout(10, () => new Error("deadline"));
    timeout.signal.addEventListener("abort", () => {
      fired.push("timeout 10");
    });
    expect(timer.pending().map(({ kind, ms }) => [kind, ms])).toEqual([
      ["timeout", 10],
      ["delay", 20],
    ]);

    await timer.advance(9);
    expect(fired).toEqual([]);
    expect(timer.now()).toBe(9);
    await timer.advance(16);
    expect(fired).toEqual(["timeout 10", "delay 20", "delay 5 more"]);
    expect(timeout.signal.reason).toEqual(new Error("deadline"));
    expect(timer.now()).toBe(25);
    expect(timer.pending()).toEqual([]);
  });

  test("drops aborted delays and disposed timeouts", async () => {
    const timer = new ManualTimer();
    const controller = new AbortController();
    const delayed = timer.delay(10, controller.signal);
    const timeout = timer.timeout(10, () => "late");
    controller.abort("stopped");
    timeout.dispose();

    await expect(delayed).rejects.toBe("stopped");
    await timer.advance(10);
    expect(timeout.signal.aborted).toBe(false);
    expect(timer.pending()).toEqual([]);
    await expect(timer.delay(1, controller.signal)).rejects.toBe("stopped");
  });

  test("waits for a matching timer", async () => {
    const timer = new ManualTimer();
    setImmediate(() => {
      void timer.delay(30, new AbortController().signal);
    });

    await expect(timer.waitFor(({ ms }) => ms === 30)).resolves.toMatchObject({
      dueAt: 30,
      kind: "delay",
      ms: 30,
    });
    await expect(
      timer.waitFor(({ ms }) => ms === 40, { timeoutMs: 10 })
    ).rejects.toThrow(AssertionError);
    await expect(timer.advance(-1)).rejects.toThrow(RangeError);
  });
});
