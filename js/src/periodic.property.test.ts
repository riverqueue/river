import fc from "fast-check";
import { describe, expect, it } from "vitest";

import { defineJob } from "./job-definition.js";
import {
  advancePeriodicJobs,
  hasUninitializedPeriodicJobs,
  nextPeriodicRunAt,
  periodicJob,
  periodicJobIds,
  PeriodicJobs,
  resetPeriodicJobs,
  setPeriodicJobsChangeHandler,
} from "./periodic.js";
import type { PeriodicJob, PeriodicJobHandle } from "./periodic.js";

const report = defineJob<{ scope: string }>()({ kind: "periodic_property" });

/** Go River's margin for inserting occurrences due in the near future. */
const DUE_MARGIN_NS = 100_000_000n;
const MILLISECOND_NS = 1_000_000n;

interface ModelEntry {
  readonly durableSeedNs: bigint | undefined;
  readonly everyNs: bigint;
  readonly handle: PeriodicJobHandle;
  readonly id: string | null;
  initialized: boolean;
  readonly job: PeriodicJob;
  nextRunNs: bigint | null;
  readonly runOnStart: boolean;
}

/**
 * A straightforward model of Go River's periodic job enqueuer: a new or
 * reset entry is initialized on the next tick (from a durable next run when
 * one is known) and inserts once if it runs on start; afterwards each tick
 * inserts at most one occurrence per due entry and advances it by exactly
 * one interval from its scheduled time.
 */
interface Model {
  changes: number;
  entries: ModelEntry[];
  nextId: number;
  nowNs: bigint;
}

interface Real {
  changes: number;
  readonly registry: PeriodicJobs;
}

type PeriodicCommand = fc.Command<Model, Real>;

/** Registry queries agree with the model after every command. */
function assertQueries(model: Readonly<Model>, real: Real): void {
  const { registry } = real;
  expect(registry.size).toBe(model.entries.length);
  expect(real.changes).toBe(model.changes);
  expect(hasUninitializedPeriodicJobs(registry)).toBe(
    model.entries.some((entry) => !entry.initialized)
  );
  expect(periodicJobIds(registry)).toEqual(
    model.entries.flatMap((entry) => (entry.id === null ? [] : [entry.id]))
  );
  const scheduled = model.entries
    .map((entry) => entry.nextRunNs)
    .filter((next): next is bigint => next !== null);
  expect(nextPeriodicRunAt(registry)?.epochNanoseconds).toBe(
    scheduled.length === 0
      ? undefined
      : scheduled.reduce((min, next) => (next < min ? next : min))
  );
}

class AddCommand implements PeriodicCommand {
  constructor(
    readonly everyMs: number,
    readonly runOnStart: boolean,
    readonly withId: boolean,
    readonly durableSeedMs: number | undefined
  ) {}

  check(): boolean {
    return true;
  }

  run(model: Model, real: Real): void {
    const id = this.withId ? `job_${model.nextId++}` : null;
    const job = periodicJob({
      args: { scope: "all" },
      every: { milliseconds: this.everyMs },
      job: report,
      runOnStart: this.runOnStart,
      ...(id === null ? {} : { id }),
    });
    const handle = real.registry.add(job);
    model.changes++;
    model.entries.push({
      durableSeedNs:
        id === null || this.durableSeedMs === undefined
          ? undefined
          : model.nowNs + BigInt(this.durableSeedMs) * MILLISECOND_NS,
      everyNs: BigInt(this.everyMs) * MILLISECOND_NS,
      handle,
      id,
      initialized: false,
      job,
      nextRunNs: null,
      runOnStart: this.runOnStart,
    });
    assertQueries(model, real);
  }

  toString(): string {
    return `add(every=${this.everyMs}ms, runOnStart=${this.runOnStart}, id=${this.withId}, durable=${this.durableSeedMs})`;
  }
}

class AdvanceCommand implements PeriodicCommand {
  constructor(readonly elapsedMs: number) {}

  check(): boolean {
    return true;
  }

  run(model: Model, real: Real): void {
    model.nowNs += BigInt(this.elapsedMs) * MILLISECOND_NS;
    const now = Temporal.Instant.fromEpochNanoseconds(model.nowNs);
    const durable = new Map<string, Temporal.Instant>();
    for (const entry of model.entries) {
      if (
        !entry.initialized &&
        entry.id !== null &&
        entry.durableSeedNs !== undefined
      ) {
        durable.set(
          entry.id,
          Temporal.Instant.fromEpochNanoseconds(entry.durableSeedNs)
        );
      }
    }
    const errors: unknown[] = [];
    const batch = advancePeriodicJobs(real.registry, now, durable, (_, error) =>
      errors.push(error)
    );

    const expectedOccurrences: [PeriodicJob, bigint][] = [];
    const expectedUpdates: [string, bigint][] = [];
    for (const entry of model.entries) {
      if (!entry.initialized) {
        entry.initialized = true;
        entry.nextRunNs = entry.durableSeedNs ?? model.nowNs + entry.everyNs;
        if (entry.id !== null)
          expectedUpdates.push([entry.id, entry.nextRunNs]);
        if (entry.runOnStart)
          expectedOccurrences.push([entry.job, model.nowNs]);
        continue;
      }
      if (
        entry.nextRunNs === null ||
        entry.nextRunNs >= model.nowNs + DUE_MARGIN_NS
      ) {
        continue;
      }
      expectedOccurrences.push([entry.job, entry.nextRunNs]);
      entry.nextRunNs += entry.everyNs;
      if (entry.id !== null) expectedUpdates.push([entry.id, entry.nextRunNs]);
    }

    expect(errors).toEqual([]);
    expect(
      batch.occurrences.map(({ job, scheduledAt }) => [
        job,
        scheduledAt.epochNanoseconds,
      ])
    ).toEqual(expectedOccurrences);
    expect(
      batch.durableUpdates.map(({ id, nextRunAt }) => [
        id,
        nextRunAt.epochNanoseconds,
      ])
    ).toEqual(expectedUpdates);
    // Every durable seed is consumed by the entry it initialized.
    expect([...durable.keys()]).toEqual([]);
    assertQueries(model, real);
  }

  toString(): string {
    return `advance(${this.elapsedMs}ms)`;
  }
}

class ClearCommand implements PeriodicCommand {
  check(): boolean {
    return true;
  }

  run(model: Model, real: Real): void {
    real.registry.clear();
    model.entries = [];
    model.changes++;
    assertQueries(model, real);
  }

  toString(): string {
    return "clear()";
  }
}

class RemoveCommand implements PeriodicCommand {
  constructor(
    readonly position: number,
    readonly byId: boolean
  ) {}

  check(model: Readonly<Model>): boolean {
    return model.entries.length > 0;
  }

  run(model: Model, real: Real): void {
    const index = this.position % model.entries.length;
    const [entry] = model.entries.splice(index, 1);
    if (entry === undefined) throw new Error("model entry missing");
    if (this.byId && entry.id !== null) {
      expect(real.registry.removeById(entry.id)).toBe(true);
      expect(real.registry.removeById(entry.id)).toBe(false);
    } else {
      expect(real.registry.remove(entry.handle)).toBe(true);
      expect(real.registry.remove(entry.handle)).toBe(false);
    }
    model.changes++;
    assertQueries(model, real);
  }

  toString(): string {
    return `remove(${this.position}, byId=${this.byId})`;
  }
}

/** Leadership changes forget scheduling state but keep registrations. */
class ResetCommand implements PeriodicCommand {
  check(): boolean {
    return true;
  }

  run(model: Model, real: Real): void {
    resetPeriodicJobs(real.registry);
    for (const entry of model.entries) {
      entry.initialized = false;
      entry.nextRunNs = null;
    }
    assertQueries(model, real);
  }

  toString(): string {
    return "reset()";
  }
}

const commandsArbitrary = fc.commands(
  [
    fc
      .tuple(
        fc.integer({ max: 5_000, min: 1 }),
        fc.boolean(),
        fc.boolean(),
        fc.option(fc.integer({ max: 5_000, min: -5_000 }), { nil: undefined })
      )
      .map(
        ([everyMs, runOnStart, withId, durableSeedMs]) =>
          new AddCommand(everyMs, runOnStart, withId, durableSeedMs)
      ),
    fc
      .oneof(
        fc.integer({ max: 150, min: 0 }),
        fc.integer({ max: 20_000, min: 0 })
      )
      .map((elapsedMs) => new AdvanceCommand(elapsedMs)),
    fc
      .tuple(fc.nat(), fc.boolean())
      .map(([position, byId]) => new RemoveCommand(position, byId)),
    fc.constant(new ClearCommand()),
    fc.constant(new ResetCommand()),
  ],
  { maxCommands: 60 }
);

describe("periodic registry properties", () => {
  it("advances like Go's periodic job enqueuer under any command order", () => {
    fc.assert(
      fc.property(commandsArbitrary, (commands) => {
        const registry = new PeriodicJobs();
        const real: Real = { changes: 0, registry };
        setPeriodicJobsChangeHandler(registry, () => {
          real.changes++;
        });
        const model: Model = {
          changes: 0,
          entries: [],
          nextId: 0,
          nowNs: Temporal.Instant.from("2026-09-01T00:00:00Z").epochNanoseconds,
        };
        fc.modelRun(() => ({ model, real }), commands);
      }),
      { numRuns: 200 }
    );
  });
});
