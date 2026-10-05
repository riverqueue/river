import { describe, expect, it } from "vitest";

import { EventDispatcher } from "./event-dispatcher.js";

describe("EventDispatcher", () => {
  it("delivers in order, applies backpressure only when full, and drains", async () => {
    const delivered: number[] = [];
    let release!: () => void;
    let gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const dispatcher = new EventDispatcher<number>(async (event) => {
      await gate;
      delivered.push(event);
    }, 2);

    await dispatcher.enqueue(1);
    await dispatcher.enqueue(2);
    await dispatcher.enqueue(3);
    let fourthQueued = false;
    const fourth = dispatcher.enqueue(4).then(() => {
      fourthQueued = true;
    });
    await Promise.resolve();
    expect(fourthQueued).toBe(false);
    expect(dispatcher.pending).toBe(2);

    release();
    gate = Promise.resolve();
    await fourth;
    await dispatcher.drain();

    expect(delivered).toEqual([1, 2, 3, 4]);
    expect(dispatcher.pending).toBe(0);
    await expect(dispatcher.drain()).resolves.toBeUndefined();
  });
});
