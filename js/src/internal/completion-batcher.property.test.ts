import { setTimeout as delay } from "node:timers/promises";

import fc from "fast-check";
import { describe, expect, it } from "vitest";

import {
  CompletionBatcher,
  CompletionDroppedError,
} from "./completion-batcher.js";
import type { CompletionFailureAction } from "./completion-batcher.js";

interface Item {
  readonly key: string;
  readonly serialKey: string;
}

/** One persistence query the batcher started and the test settles. */
interface PersistCall {
  readonly items: readonly Item[];
  readonly reject: (error: unknown) => void;
  readonly resolve: (results: ReadonlyMap<string, string>) => void;
}

type Settlement =
  | { readonly state: "dropped" }
  | { readonly state: "pending" }
  | { readonly state: "persisted"; readonly value: string };

interface Submission {
  accepted: boolean;
  acknowledged: boolean;
  readonly acknowledge: () => void;
  readonly autoAcknowledge: boolean;
  readonly key: string;
  settlement: Settlement;
}

/**
 * The system under test plus everything observed about it. Assertions are
 * invariants rather than an exact model, because batch composition depends
 * on when the flush timer fires.
 */
interface Real {
  readonly batcher: CompletionBatcher<Item, string>;
  readonly batchSize: number;
  readonly calls: PersistCall[];
  failureAction: CompletionFailureAction;
  readonly maxPendingItems: number;
  nextKey: number;
  readonly persistedKeys: Map<string, number>;
  readonly submissions: Submission[];
  readonly violations: string[];
}

interface Model {
  submitted: number;
}

type BatcherCommand = fc.AsyncCommand<Model, Real>;

/** Let promise reactions and a zero-delay flush timer run. */
async function settle(): Promise<void> {
  await delay(0);
  await delay(0);
}

function assertInvariants(real: Real): void {
  expect(real.violations).toEqual([]);
  expect(real.batcher.inFlightQueries).toBe(real.calls.length);
  expect(real.calls.length).toBeLessThanOrEqual(2);
  expect(real.batcher.pendingItems).toBeLessThanOrEqual(real.maxPendingItems);
  const inFlightSerialKeys = real.calls.flatMap((call) =>
    call.items.map((item) => item.serialKey)
  );
  expect(new Set(inFlightSerialKeys).size).toBe(inFlightSerialKeys.length);
  for (const [key, count] of real.persistedKeys) {
    expect(count, `${key} persisted more than once`).toBe(1);
  }
  for (const submission of real.submissions) {
    if (submission.settlement.state === "persisted") {
      expect(submission.settlement.value).toBe(`persisted:${submission.key}`);
      expect(real.persistedKeys.get(submission.key)).toBe(1);
    }
  }
}

class SubmitCommand implements BatcherCommand {
  constructor(
    readonly serialKey: number,
    readonly autoAcknowledge: boolean
  ) {}

  check(): boolean {
    return true;
  }

  async run(model: Model, real: Real): Promise<void> {
    model.submitted++;
    const key = `job_${real.nextKey++}`;
    const item = { key, serialKey: `serial_${this.serialKey}` };
    const submission = real.batcher.submit(key, item, item.serialKey);
    const record: Submission = {
      accepted: false,
      acknowledge: submission.acknowledge,
      acknowledged: false,
      autoAcknowledge: this.autoAcknowledge,
      key,
      settlement: { state: "pending" },
    };
    real.submissions.push(record);
    void submission.accepted.then(
      () => {
        record.accepted = true;
      },
      () => undefined
    );
    void submission.result.then(
      (value) => {
        record.settlement = { state: "persisted", value };
        if (record.autoAcknowledge) {
          record.acknowledged = true;
          record.acknowledge();
        }
      },
      (error: unknown) => {
        if (!(error instanceof CompletionDroppedError)) {
          real.violations.push(`${key} failed with ${String(error)}`);
        }
        record.settlement = { state: "dropped" };
      }
    );
    await settle();
    assertInvariants(real);
  }

  toString(): string {
    return `submit(serial_${this.serialKey}, autoAck=${this.autoAcknowledge})`;
  }
}

class SettleCommand implements BatcherCommand {
  constructor(
    readonly position: number,
    readonly outcome: "drop" | "persist" | "requeue"
  ) {}

  check(): boolean {
    return true;
  }

  async run(_model: Model, real: Real): Promise<void> {
    if (real.calls.length === 0) return;
    const [call] = real.calls.splice(this.position % real.calls.length, 1);
    if (call === undefined) throw new Error("persist call missing");
    if (this.outcome === "persist") {
      for (const { key } of call.items) {
        real.persistedKeys.set(key, (real.persistedKeys.get(key) ?? 0) + 1);
      }
      call.resolve(
        new Map(call.items.map(({ key }) => [key, `persisted:${key}`]))
      );
    } else {
      real.failureAction = this.outcome;
      call.reject(new Error("database unavailable"));
    }
    await settle();
    assertInvariants(real);
  }

  toString(): string {
    return `settle(${this.position}, ${this.outcome})`;
  }
}

class AcknowledgeCommand implements BatcherCommand {
  constructor(readonly position: number) {}

  check(): boolean {
    return true;
  }

  async run(_model: Model, real: Real): Promise<void> {
    const ready = real.submissions.filter(
      (submission) =>
        submission.settlement.state === "persisted" && !submission.acknowledged
    );
    const submission = ready[this.position % Math.max(ready.length, 1)];
    if (submission === undefined) return;
    submission.acknowledged = true;
    submission.acknowledge();
    // Acknowledging twice is harmless.
    submission.acknowledge();
    await settle();
    assertInvariants(real);
  }

  toString(): string {
    return `acknowledge(${this.position})`;
  }
}

const commandsArbitrary = fc.commands(
  [
    fc
      .tuple(fc.integer({ max: 5, min: 0 }), fc.boolean())
      .map(([serialKey, auto]) => new SubmitCommand(serialKey, auto)),
    fc
      .tuple(
        fc.nat(),
        fc.oneof(
          { arbitrary: fc.constant("persist" as const), weight: 4 },
          { arbitrary: fc.constant("requeue" as const), weight: 1 },
          { arbitrary: fc.constant("drop" as const), weight: 1 }
        )
      )
      .map(([position, outcome]) => new SettleCommand(position, outcome)),
    fc.nat().map((position) => new AcknowledgeCommand(position)),
  ],
  { maxCommands: 40 }
);

describe("CompletionBatcher properties", () => {
  it("persists or drops every accepted completion exactly once", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ max: 4, min: 1 }),
        fc.integer({ max: 3, min: 1 }),
        commandsArbitrary,
        async (batchSize, pendingFactor, commands) => {
          const calls: PersistCall[] = [];
          const violations: string[] = [];
          const real: Real = {
            batchSize,
            batcher: new CompletionBatcher<Item, string>({
              batchSize,
              flushIntervalMs: 0,
              maxPendingItems: batchSize * pendingFactor,
              onPersistFailure: () => real.failureAction,
              persist: (items) => {
                // A second concurrent query starts only for a full batch.
                if (calls.length > 0 && items.length !== batchSize) {
                  violations.push(
                    `partial batch of ${items.length} started beside another query`
                  );
                }
                const serialKeys = items.map((item) => item.serialKey);
                if (new Set(serialKeys).size !== serialKeys.length) {
                  violations.push(`one batch repeats a serial key`);
                }
                if (items.length > batchSize) {
                  violations.push(`batch of ${items.length} exceeds its size`);
                }
                return new Promise((resolve, reject) => {
                  calls.push({ items, reject, resolve });
                });
              },
            }),
            calls,
            failureAction: "requeue",
            maxPendingItems: batchSize * pendingFactor,
            nextKey: 0,
            persistedKeys: new Map(),
            submissions: [],
            violations,
          };

          await fc.asyncModelRun(
            () => ({ model: { submitted: 0 }, real }),
            commands
          );

          // Drain: acknowledge as the runtime does, then persist everything.
          const closed = real.batcher.close().then(
            () => "closed" as const,
            (error: unknown) => error
          );
          for (let round = 0; round < 1_000; round++) {
            for (const submission of real.submissions) {
              if (
                submission.settlement.state === "persisted" &&
                !submission.acknowledged
              ) {
                submission.acknowledged = true;
                submission.acknowledge();
              }
            }
            const call = real.calls.shift();
            if (call !== undefined) {
              for (const { key } of call.items) {
                real.persistedKeys.set(
                  key,
                  (real.persistedKeys.get(key) ?? 0) + 1
                );
              }
              call.resolve(
                new Map(call.items.map(({ key }) => [key, `persisted:${key}`]))
              );
            }
            await settle();
            if (
              real.calls.length === 0 &&
              real.submissions.every(
                (submission) =>
                  submission.settlement.state !== "pending" &&
                  (submission.settlement.state === "dropped" ||
                    submission.acknowledged)
              )
            ) {
              break;
            }
          }

          await expect(closed).resolves.toBe("closed");
          assertInvariants(real);
          expect(real.batcher.pendingItems).toBe(0);
          for (const submission of real.submissions) {
            // No completion is lost: each one is persisted exactly once or
            // explicitly dropped for the rescuer to recover.
            expect(submission.settlement.state).not.toBe("pending");
            expect(real.persistedKeys.get(submission.key) ?? 0).toBe(
              submission.settlement.state === "persisted" ? 1 : 0
            );
          }
        }
      ),
      { numRuns: 100 }
    );
  });
});
