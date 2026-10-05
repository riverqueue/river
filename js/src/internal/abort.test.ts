import { getEventListeners } from "node:events";

import { describe, expect, it, vi } from "vitest";

import {
  abortableDelay,
  interruptibleDelay,
  LinkedAbortSignal,
  raceWithAbort,
  unrefTimeout,
} from "./abort.js";

describe("abortableDelay", () => {
  it("resolves after the delay", async () => {
    await expect(
      abortableDelay(1, new AbortController().signal)
    ).resolves.toBeUndefined();
  });

  it("rejects with the abort reason, including when already aborted", async () => {
    const controller = new AbortController();
    const reason = new Error("stopping");
    const delayed = abortableDelay(60_000, controller.signal);

    controller.abort(reason);

    await expect(delayed).rejects.toBe(reason);
    await expect(abortableDelay(1, controller.signal)).rejects.toBe(reason);
  });
});

describe("interruptibleDelay", () => {
  it("resolves early without rejecting once the signal aborts", async () => {
    const controller = new AbortController();
    const delayed = interruptibleDelay(60_000, controller.signal);

    controller.abort(new Error("stopping"));

    await expect(delayed).resolves.toBeUndefined();
    await expect(
      interruptibleDelay(60_000, controller.signal)
    ).resolves.toBeUndefined();
  });
});

describe("LinkedAbortSignal", () => {
  it("aborts with the reason of the first parent to abort", () => {
    const first = new AbortController();
    const second = new AbortController();
    using link = new LinkedAbortSignal([first.signal, second.signal]);
    const seen: string[] = [];
    first.signal.addEventListener("abort", () => seen.push("parent"));
    link.signal.addEventListener("abort", () =>
      seen.push(`link ${String(link.signal.reason)}`)
    );

    second.abort("second");
    first.abort("first");

    expect(link.signal.reason).toBe("second");
    // The first parent no longer aborts it.
    expect(seen).toEqual(["link second", "parent"]);
    expect(getEventListeners(first.signal, "abort")).toHaveLength(1);
  });

  it("starts aborted by the first aborted parent, in order", () => {
    const pending = new AbortController();
    using link = new LinkedAbortSignal([
      pending.signal,
      AbortSignal.abort("first"),
      AbortSignal.abort("second"),
    ]);

    expect(link.signal.aborted).toBe(true);
    expect(link.signal.reason).toBe("first");
    expect(getEventListeners(pending.signal, "abort")).toHaveLength(0);
  });

  it("removes its parent listeners once disposed", () => {
    const parent = new AbortController();
    const links = Array.from(
      { length: 100 },
      () => new LinkedAbortSignal([parent.signal])
    );
    expect(getEventListeners(parent.signal, "abort")).toHaveLength(100);

    for (const link of links) link[Symbol.dispose]();
    parent.abort("late");

    expect(getEventListeners(parent.signal, "abort")).toHaveLength(0);
    expect(links.every((link) => !link.signal.aborted)).toBe(true);
  });
});

describe("raceWithAbort", () => {
  it("settles with the operation", async () => {
    const signal = new AbortController().signal;
    const failure = new Error("failed");

    await expect(raceWithAbort(Promise.resolve(1), signal)).resolves.toBe(1);
    await expect(raceWithAbort(2, signal)).resolves.toBe(2);
    await expect(raceWithAbort(Promise.reject(failure), signal)).rejects.toBe(
      failure
    );
  });

  it("rejects with the abort reason while the operation is pending", async () => {
    const controller = new AbortController();
    const reason = new Error("stopping");
    const raced = raceWithAbort(
      new Promise<never>(() => undefined),
      controller.signal
    );

    controller.abort(reason);

    await expect(raced).rejects.toBe(reason);
    await expect(raceWithAbort(1, controller.signal)).rejects.toBe(reason);
  });
});

describe("unrefTimeout", () => {
  it("waits out a delay longer than Node's timers support", () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      const thirtyDays = 30 * 24 * 60 * 60 * 1_000;
      let fired = 0;
      unrefTimeout(() => {
        fired++;
      }, thirtyDays);

      vi.advanceTimersByTime(2_147_483_647);
      expect(fired).toBe(0);
      vi.advanceTimersByTime(thirtyDays - 2_147_483_647 - 1);
      expect(fired).toBe(0);
      vi.advanceTimersByTime(1);
      expect(fired).toBe(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("cancels a long delay after its first chunk", () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      let fired = 0;
      const cancel = unrefTimeout(() => {
        fired++;
      }, 3_000_000_000);

      vi.advanceTimersByTime(2_147_483_647);
      cancel();
      vi.advanceTimersByTime(3_000_000_000);
      expect(fired).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
});
