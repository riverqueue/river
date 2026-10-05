import { describe, expect, it, vi } from "vitest";

import {
  CompletionBatcher,
  CompletionDroppedError,
} from "./completion-batcher.js";

interface Item {
  key: string;
}

function results(items: readonly Item[]): ReadonlyMap<string, string> {
  return new Map(items.map((item) => [item.key, `persisted:${item.key}`]));
}

describe("CompletionBatcher", () => {
  it("applies bounded acceptance backpressure and promotes in FIFO order", async () => {
    const releases: (() => void)[] = [];
    const batcher = new CompletionBatcher<Item, string>({
      batchSize: 1,
      flushIntervalMs: 1_000,
      maxPendingItems: 2,
      persist: async (items) => {
        await new Promise<void>((resolve) => releases.push(resolve));
        return results(items);
      },
    });

    const first = batcher.submit("1/1", { key: "1/1" });
    const second = batcher.submit("2/1", { key: "2/1" });
    const third = batcher.submit("3/1", { key: "3/1" });
    let thirdAccepted = false;
    void third.accepted.then(() => {
      thirdAccepted = true;
    });

    await Promise.all([first.accepted, second.accepted]);
    await Promise.resolve();
    expect(batcher.pendingItems).toBe(2);
    expect(thirdAccepted).toBe(false);

    releases.shift()?.();
    await first.result;
    first.acknowledge();
    await third.accepted;
    expect(thirdAccepted).toBe(true);
    expect(batcher.pendingItems).toBe(2);

    for (const release of releases.splice(0)) release();
    await vi.waitFor(() => expect(releases).toHaveLength(1));
    releases.shift()?.();
    await Promise.all([second.result, third.result]);
    second.acknowledge();
    third.acknowledge();
    await batcher.close();
  });

  it("rejects accepted and capacity-blocked submissions on abort", async () => {
    const failure = new Error("runtime failed");
    const batcher = new CompletionBatcher<Item, string>({
      batchSize: 1,
      flushIntervalMs: 1_000,
      maxPendingItems: 1,
      persist: (_items, signal) =>
        new Promise((_resolve, reject) =>
          signal.addEventListener("abort", () => reject(signal.reason), {
            once: true,
          })
        ),
    });
    const accepted = batcher.submit("1/1", { key: "1/1" });
    const blocked = batcher.submit("2/1", { key: "2/1" });
    await accepted.accepted;

    batcher.abort(failure);

    await expect(accepted.result).rejects.toBe(failure);
    await expect(blocked.accepted).rejects.toBe(failure);
    await expect(blocked.result).rejects.toBe(failure);
    await expect(batcher.close()).rejects.toBe(failure);
  });

  it("maps persistence results by attempt key", async () => {
    const batcher = new CompletionBatcher<Item, string>({
      batchSize: 2,
      flushIntervalMs: 10,
      persist: async (items) =>
        new Map([...items].reverse().map((item) => [item.key, item.key])),
    });

    const first = batcher.enqueue("10/1", { key: "10/1" });
    const second = batcher.enqueue("11/2", { key: "11/2" });

    await expect(first).resolves.toBe("10/1");
    await expect(second).resolves.toBe("11/2");
    await batcher.close();
  });

  it("partitions a 5,000-item batch in stable order", async () => {
    const batches: string[][] = [];
    const batcher = new CompletionBatcher<Item, string>({
      batchSize: 5_000,
      flushIntervalMs: 1_000,
      maxPendingItems: 10_000,
      persist: async (items) => {
        batches.push(items.map((item) => item.key));
        return results(items);
      },
    });
    const completions = Array.from({ length: 5_000 }, (_, index) => {
      const key = `${index + 1}/1`;
      return batcher.enqueue(key, { key });
    });

    await Promise.all(completions);
    await batcher.close();

    expect(batches).toHaveLength(1);
    expect(batches[0]).toHaveLength(5_000);
    expect(batches[0]?.slice(0, 3)).toEqual(["1/1", "2/1", "3/1"]);
    expect(batches[0]?.slice(-3)).toEqual(["4998/1", "4999/1", "5000/1"]);
  });

  it("runs at most two queries and starts the second only when full", async () => {
    const releases: (() => void)[] = [];
    const batches: string[][] = [];
    let active = 0;
    let maximumActive = 0;
    const batcher = new CompletionBatcher<Item, string>({
      batchSize: 2,
      flushIntervalMs: 1_000,
      persist: async (items) => {
        batches.push(items.map((item) => item.key));
        active += 1;
        maximumActive = Math.max(maximumActive, active);
        await new Promise<void>((resolve) => releases.push(resolve));
        active -= 1;
        return results(items);
      },
    });

    const promises = [
      batcher.enqueue("1/1", { key: "1/1" }),
      batcher.enqueue("2/1", { key: "2/1" }),
      batcher.enqueue("3/1", { key: "3/1" }),
    ];
    expect(batches).toEqual([["1/1", "2/1"]]);
    expect(batcher.inFlightQueries).toBe(1);

    promises.push(batcher.enqueue("4/1", { key: "4/1" }));
    expect(batches).toEqual([
      ["1/1", "2/1"],
      ["3/1", "4/1"],
    ]);
    expect(batcher.inFlightQueries).toBe(2);

    promises.push(batcher.enqueue("5/1", { key: "5/1" }));
    promises.push(batcher.enqueue("6/1", { key: "6/1" }));
    expect(batches).toHaveLength(2);

    releases.shift()?.();
    await vi.waitFor(() => expect(batches).toHaveLength(3));
    expect(maximumActive).toBe(2);

    for (const release of releases.splice(0)) release();
    await Promise.all(promises);
    await batcher.close();
  });

  it("does not start a timed partial batch beside an active query", async () => {
    vi.useFakeTimers();
    try {
      const releases: (() => void)[] = [];
      const batches: string[][] = [];
      const batcher = new CompletionBatcher<Item, string>({
        batchSize: 2,
        flushIntervalMs: 10,
        persist: async (items) => {
          batches.push(items.map((item) => item.key));
          await new Promise<void>((resolve) => releases.push(resolve));
          return results(items);
        },
      });

      const first = batcher.enqueue("1/1", { key: "1/1" });
      const second = batcher.enqueue("2/1", { key: "2/1" });
      const partial = batcher.enqueue("3/1", { key: "3/1" });
      await vi.advanceTimersByTimeAsync(10);
      expect(batches).toEqual([["1/1", "2/1"]]);

      releases.shift()?.();
      await vi.waitFor(() => expect(batches).toHaveLength(2));
      expect(batches[1]).toEqual(["3/1"]);

      releases.shift()?.();
      await Promise.all([first, second, partial]);
      await batcher.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it("never persists overlapping attempts of one job concurrently", async () => {
    const releases: (() => void)[] = [];
    const batches: string[][] = [];
    const batcher = new CompletionBatcher<Item, string>({
      batchSize: 2,
      flushIntervalMs: 10,
      persist: async (items) => {
        batches.push(items.map((item) => item.key));
        await new Promise<void>((resolve) => releases.push(resolve));
        return results(items);
      },
    });

    const firstAttempt = batcher.enqueue("1/1", { key: "1/1" }, "1");
    const otherJob = batcher.enqueue("2/1", { key: "2/1" }, "2");
    const secondAttempt = batcher.enqueue("1/2", { key: "1/2" }, "1");
    const thirdJob = batcher.enqueue("3/1", { key: "3/1" }, "3");
    const fourthJob = batcher.enqueue("4/1", { key: "4/1" }, "4");

    expect(batches).toEqual([
      ["1/1", "2/1"],
      ["3/1", "4/1"],
    ]);
    releases.shift()?.();
    await expect(firstAttempt).resolves.toBe("persisted:1/1");
    await expect(otherJob).resolves.toBe("persisted:2/1");
    releases.shift()?.();
    await expect(thirdJob).resolves.toBe("persisted:3/1");
    await expect(fourthJob).resolves.toBe("persisted:4/1");
    await vi.waitFor(() => expect(batches.at(-1)).toEqual(["1/2"]));
    releases.shift()?.();
    await expect(secondAttempt).resolves.toBe("persisted:1/2");
    await batcher.close();
  });

  it("rejects missing and duplicate attempt keys", async () => {
    const batcher = new CompletionBatcher<Item, string>({
      batchSize: 2,
      flushIntervalMs: 1_000,
      persist: async () => new Map(),
    });

    const first = batcher.enqueue("1/1", { key: "1/1" });
    await expect(batcher.enqueue("1/1", { key: "duplicate" })).rejects.toThrow(
      "duplicate completion key"
    );
    const second = batcher.enqueue("2/1", { key: "2/1" });

    await expect(first).rejects.toThrow("missing key");
    await expect(second).rejects.toThrow("missing key");
    await expect(batcher.close()).rejects.toThrow("missing key");
  });

  it("requeues a failed batch at the front and keeps its capacity", async () => {
    const failure = new Error("transient");
    const calls: string[][] = [];
    const decisions: [unknown, number][] = [];
    const batcher = new CompletionBatcher<Item, string>({
      batchSize: 2,
      flushIntervalMs: 0,
      maxPendingItems: 2,
      onPersistFailure: (error, count) => {
        decisions.push([error, count]);
        return "requeue";
      },
      persist: async (items) => {
        calls.push(items.map(({ key }) => key));
        if (calls.length === 1) throw failure;
        return results(items);
      },
    });
    const first = batcher.submit("1/1", { key: "1/1" });
    const second = batcher.submit("2/1", { key: "2/1" });
    const blocked = batcher.submit("3/1", { key: "3/1" });
    let blockedAccepted = false;
    void blocked.accepted.then(() => {
      blockedAccepted = true;
    });

    await expect(first.result).resolves.toBe("persisted:1/1");
    await expect(second.result).resolves.toBe("persisted:2/1");
    expect(blockedAccepted).toBe(false);
    first.acknowledge();
    second.acknowledge();
    await expect(blocked.result).resolves.toBe("persisted:3/1");
    blocked.acknowledge();
    await batcher.close();

    expect(decisions).toEqual([[failure, 2]]);
    expect(calls).toEqual([["1/1", "2/1"], ["1/1", "2/1"], ["3/1"]]);
  });

  it("drops a failed batch without failing later completions", async () => {
    const failure = new Error("permanent");
    const drops: [unknown, number][] = [];
    let fail = true;
    const batcher = new CompletionBatcher<Item, string>({
      batchSize: 1,
      flushIntervalMs: 0,
      maxPendingItems: 1,
      onDrop: (error, count) => drops.push([error, count]),
      onPersistFailure: () => "drop",
      persist: async (items) => {
        if (fail) {
          fail = false;
          throw failure;
        }
        return results(items);
      },
    });
    const dropped = batcher.submit("1/1", { key: "1/1" });
    const later = batcher.submit("2/1", { key: "2/1" });

    const rejection = await dropped.result.catch((error: unknown) => error);
    expect(rejection).toBeInstanceOf(CompletionDroppedError);
    expect((rejection as Error).cause).toBe(failure);
    await expect(later.result).resolves.toBe("persisted:2/1");
    dropped.acknowledge();
    later.acknowledge();
    expect(batcher.pendingItems).toBe(0);
    expect(drops).toEqual([[failure, 1]]);
    await batcher.close();
  });

  it("drops every unpersisted completion after a failure while draining", async () => {
    const failure = new Error("outage");
    const drops: [unknown, number][] = [];
    let rejectFirst!: (error: unknown) => void;
    const batcher = new CompletionBatcher<Item, string>({
      batchSize: 1,
      flushIntervalMs: 0,
      maxPendingItems: 1,
      onDrop: (error, count) => drops.push([error, count]),
      onPersistFailure: () => "requeue",
      persist: () =>
        new Promise((_resolve, reject) => {
          rejectFirst = reject;
        }),
    });
    const inFlight = batcher.submit("1/1", { key: "1/1" });
    const waiting = batcher.submit("2/1", { key: "2/1" });
    await vi.waitFor(() => expect(rejectFirst).toBeTypeOf("function"));

    batcher.drain();
    rejectFirst(failure);

    await expect(inFlight.result).rejects.toBeInstanceOf(
      CompletionDroppedError
    );
    await expect(waiting.accepted).rejects.toBeInstanceOf(
      CompletionDroppedError
    );
    await expect(waiting.result).rejects.toBeInstanceOf(CompletionDroppedError);
    inFlight.acknowledge();
    expect(drops).toEqual([[failure, 2]]);
    expect(batcher.pendingItems).toBe(0);
    await batcher.close();
  });

  it("never requeues after close so shutdown finishes", async () => {
    const failure = new Error("outage");
    let calls = 0;
    const batcher = new CompletionBatcher<Item, string>({
      batchSize: 10,
      flushIntervalMs: 1_000,
      onPersistFailure: () => "requeue",
      persist: async () => {
        calls += 1;
        throw failure;
      },
    });
    const pending = batcher.submit("1/1", { key: "1/1" });

    await batcher.close();

    await expect(pending.result).rejects.toBeInstanceOf(CompletionDroppedError);
    expect(calls).toBe(1);
  });
});
