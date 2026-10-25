import { describe, expect, it } from "vitest";

import { defineJob } from "./job-definition.js";
import {
  advancePeriodicJobs,
  periodicJob,
  periodicJobKeepAliveIds,
  PeriodicJobs,
  resetPeriodicJobs,
} from "./periodic.js";

const report = defineJob<{ scope: string }>()({ kind: "periodic_report" });
const start = Temporal.Instant.from("2026-08-30T12:00:00Z");

function hourly(id: string) {
  return periodicJob({
    args: { scope: id },
    every: { hours: 1 },
    id,
    job: report,
  });
}

function advance(jobs: PeriodicJobs, now = start) {
  return advancePeriodicJobs(jobs, now, new Map(), (_job, error) => {
    throw error;
  });
}

describe("periodic registration persistence", () => {
  it("keeps only saved registrations eligible before a batch commits", () => {
    const jobs = new PeriodicJobs([hourly("fresh"), hourly("pending")]);
    const saved = new Map([["fresh", start.add({ hours: 2 })]]);
    resetPeriodicJobs(jobs, saved);
    advancePeriodicJobs(jobs, start, saved, (_job, error) => {
      throw error;
    });

    expect(periodicJobKeepAliveIds(jobs)).toEqual(["fresh"]);
  });

  it("ignores a commit for a removed registration", () => {
    const jobs = new PeriodicJobs();
    const handle = jobs.add(hourly("hourly"));
    const oldBatch = advance(jobs);
    jobs.remove(handle);
    jobs.add(hourly("hourly"));
    oldBatch.markPersisted();

    expect(periodicJobKeepAliveIds(jobs)).toEqual([]);
    advance(jobs).markPersisted();
    expect(periodicJobKeepAliveIds(jobs)).toEqual(["hourly"]);
  });

  it("ignores a commit from an earlier leadership term", () => {
    const jobs = new PeriodicJobs([hourly("hourly")]);
    const oldBatch = advance(jobs);
    resetPeriodicJobs(jobs);
    oldBatch.markPersisted();

    expect(periodicJobKeepAliveIds(jobs)).toEqual([]);
    const newBatch = advance(jobs);
    newBatch.markPersisted();
    expect(periodicJobKeepAliveIds(jobs)).toEqual(["hourly"]);
  });

  it("keeps a stopped schedule pending until it has a saved position", () => {
    const jobs = new PeriodicJobs([
      periodicJob({
        args: { scope: "stopped" },
        id: "stopped",
        job: report,
        schedule: { next: () => null },
      }),
    ]);
    const batch = advance(jobs);
    expect(batch.durableUpdates).toEqual([]);
    batch.markPersisted();

    expect(periodicJobKeepAliveIds(jobs)).toEqual([]);
  });

  it("promotes only registrations included in a committed batch", () => {
    const jobs = new PeriodicJobs([hourly("first")]);
    const firstBatch = advance(jobs);
    jobs.add(hourly("second"));
    const secondBatch = advance(jobs);
    firstBatch.markPersisted();

    expect(periodicJobKeepAliveIds(jobs)).toEqual(["first"]);
    secondBatch.markPersisted();
    expect(periodicJobKeepAliveIds(jobs)).toEqual(["first", "second"]);
  });

  it("marks a registration after a later occurrence commits", () => {
    const jobs = new PeriodicJobs([hourly("hourly")]);
    advance(jobs);
    advance(jobs, start.add({ hours: 1 })).markPersisted();

    expect(periodicJobKeepAliveIds(jobs)).toEqual(["hourly"]);
  });

  it("clears eligibility when a new leadership term starts", () => {
    const jobs = new PeriodicJobs([hourly("hourly")]);
    advance(jobs).markPersisted();
    resetPeriodicJobs(jobs);

    expect(periodicJobKeepAliveIds(jobs)).toEqual([]);
    resetPeriodicJobs(jobs, new Map([["hourly", start.add({ hours: 1 })]]));
    expect(periodicJobKeepAliveIds(jobs)).toEqual(["hourly"]);
  });

  it("preserves eligibility after a later failed occurrence", () => {
    const jobs = new PeriodicJobs([hourly("hourly")]);
    advance(jobs).markPersisted();
    advance(jobs, start.add({ hours: 1 }));

    expect(periodicJobKeepAliveIds(jobs)).toEqual(["hourly"]);
  });
});
