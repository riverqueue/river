import { describe, expect, it } from "vitest";

import { defineJob } from "./job-definition.js";
import {
  advancePeriodicJobs,
  buildPeriodicInsert,
  nextPeriodicRunAt,
  periodicJob,
  periodicJobIds,
  PeriodicJobs,
  resetPeriodicJobs,
  setPeriodicJobsChangeHandler,
  type PeriodicJob,
} from "./periodic.js";

const report = defineJob<{ scope: string }>()({ kind: "periodic_report" });
const start = Temporal.Instant.from("2026-08-30T12:00:00Z");

function advance(
  jobs: PeriodicJobs,
  now: Temporal.Instant,
  durable = new Map<string, Temporal.Instant>()
) {
  const errors: [PeriodicJob, unknown][] = [];
  const batch = advancePeriodicJobs(jobs, now, durable, (job, error) =>
    errors.push([job, error])
  );
  return { ...batch, errors };
}

describe("periodicJob", () => {
  it("validates its configuration", () => {
    expect(() =>
      periodicJob({ args: { scope: "a" }, every: { hours: 1 }, job: report })
    ).not.toThrow();
    expect(() =>
      // @ts-expect-error -- every and schedule are mutually exclusive.
      periodicJob({
        args: { scope: "a" },
        every: { hours: 1 },
        job: report,
        schedule: { next: () => null },
      })
    ).toThrow("exactly one of every or schedule");
    expect(() =>
      // @ts-expect-error -- args and construct are mutually exclusive.
      periodicJob({
        args: { scope: "a" },
        construct: () => null,
        every: { hours: 1 },
        job: report,
      })
    ).toThrow("exactly one of args or construct");
    expect(() =>
      periodicJob({ args: { scope: "a" }, every: { months: 1 }, job: report })
    ).toThrow("calendar units");
    expect(() =>
      periodicJob({ args: { scope: "a" }, every: { seconds: 0 }, job: report })
    ).toThrow("positive");
    expect(() =>
      periodicJob({
        args: { scope: "a" },
        every: { hours: 1 },
        id: "has space",
        job: report,
      })
    ).toThrow("contain only letters, numbers, and _-[]<>/.·:+");
    expect(() =>
      periodicJob({
        args: { scope: "a" },
        every: { hours: 1 },
        id: "x",
        job: report,
      })
    ).toThrow("2 to 127 characters");
    expect(() =>
      periodicJob({
        args: {},
        every: { hours: 1 },
        job: { defaults: {}, kind: "fake" },
      })
    ).toThrow("defineJob");
    expect(() =>
      periodicJob({
        // @ts-expect-error -- producer input is typed by the definition.
        args: { other: 1 },
        every: { hours: 1 },
        job: report,
      })
    ).not.toThrow();
  });

  it("treats a day as 24 hours", () => {
    const job = periodicJob({
      args: { scope: "a" },
      every: { days: 1 },
      job: report,
    });
    expect(job.schedule.next(start)).toEqual(start.add({ hours: 24 }));
  });
});

describe("PeriodicJobs", () => {
  it("adds, removes, and rejects duplicate IDs", () => {
    const hourly = periodicJob({
      args: { scope: "a" },
      every: { hours: 1 },
      id: "hourly",
      job: report,
    });
    const jobs = new PeriodicJobs([hourly]);
    let changes = 0;
    setPeriodicJobsChangeHandler(jobs, () => changes++);

    expect(() => jobs.add(hourly)).toThrow("duplicate periodic job id");
    const handle = jobs.add(
      periodicJob({ args: { scope: "b" }, every: { hours: 2 }, job: report })
    );
    expect(jobs.size).toBe(2);
    expect(periodicJobIds(jobs)).toEqual(["hourly"]);
    expect(jobs.remove(handle)).toBe(true);
    expect(jobs.remove(handle)).toBe(false);
    expect(jobs.removeById("hourly")).toBe(true);
    expect(jobs.removeById("hourly")).toBe(false);
    jobs.clear();
    expect(changes).toBe(4);
    expect(() =>
      jobs.add({
        id: null,
        job: report,
        runOnStart: false,
        schedule: { next: () => null },
      })
    ).toThrow("periodicJob()");
  });
});

describe("advancePeriodicJobs", () => {
  it("runs on start, then inserts occurrences from their scheduled time", () => {
    const jobs = new PeriodicJobs([
      periodicJob({
        args: { scope: "a" },
        every: { seconds: 10 },
        id: "heartbeat",
        job: report,
        runOnStart: true,
      }),
    ]);

    const first = advance(jobs, start);
    expect(first.occurrences.map(({ scheduledAt }) => scheduledAt)).toEqual([
      start,
    ]);
    expect(first.durableUpdates).toEqual([
      { id: "heartbeat", nextRunAt: start.add({ seconds: 10 }) },
    ]);
    expect(nextPeriodicRunAt(jobs)).toEqual(start.add({ seconds: 10 }));

    // Not due yet, except within Go River's 100 ms margin.
    expect(advance(jobs, start.add({ seconds: 9 })).occurrences).toEqual([]);
    const early = advance(jobs, start.add({ milliseconds: 9_950 }));
    expect(early.occurrences.map(({ scheduledAt }) => scheduledAt)).toEqual([
      start.add({ seconds: 10 }),
    ]);
    expect(nextPeriodicRunAt(jobs)).toEqual(start.add({ seconds: 20 }));

    // A leader that fell behind catches up one occurrence per pass.
    const late = start.add({ seconds: 45 });
    expect(advance(jobs, late).occurrences[0]?.scheduledAt).toEqual(
      start.add({ seconds: 20 })
    );
    expect(advance(jobs, late).occurrences[0]?.scheduledAt).toEqual(
      start.add({ seconds: 30 })
    );
  });

  it("seeds next runs from durable records and initializes added jobs", () => {
    const jobs = new PeriodicJobs([
      periodicJob({
        args: { scope: "a" },
        every: { hours: 1 },
        id: "durable",
        job: report,
      }),
    ]);
    const durable = new Map([["durable", start.add({ minutes: 5 })]]);

    expect(advance(jobs, start, durable)).toMatchObject({
      durableUpdates: [{ id: "durable", nextRunAt: start.add({ minutes: 5 }) }],
      occurrences: [],
    });
    expect(durable.size).toBe(0);

    jobs.add(
      periodicJob({
        args: { scope: "b" },
        every: { minutes: 1 },
        job: report,
        runOnStart: true,
      })
    );
    const later = start.add({ minutes: 1 });
    expect(advance(jobs, later).occurrences).toHaveLength(1);
    expect(nextPeriodicRunAt(jobs)).toEqual(later.add({ minutes: 1 }));

    resetPeriodicJobs(jobs);
    expect(nextPeriodicRunAt(jobs)).toBeNull();
  });

  it("stops scheduling a job whose schedule throws without affecting others", () => {
    const broken = periodicJob({
      args: { scope: "broken" },
      job: report,
      schedule: {
        next: () => {
          throw new Error("bad cron");
        },
      },
    });
    const healthy = periodicJob({
      args: { scope: "ok" },
      every: { minutes: 1 },
      job: report,
    });
    const jobs = new PeriodicJobs([broken, healthy]);

    const batch = advance(jobs, start);

    expect(batch.errors.map(([job]) => job)).toEqual([broken]);
    expect(nextPeriodicRunAt(jobs)).toEqual(start.add({ minutes: 1 }));
  });
});

describe("buildPeriodicInsert", () => {
  it("adds periodic metadata and the occurrence time", async () => {
    const job = periodicJob({
      args: { scope: "a" },
      every: { hours: 1 },
      id: "report",
      job: report,
      options: { metadata: { owner: "ops" }, queue: "reports" },
    });

    await expect(
      buildPeriodicInsert({ job, scheduledAt: start })
    ).resolves.toEqual({
      args: { scope: "a" },
      job: report,
      options: {
        metadata: {
          owner: "ops",
          periodic: true,
          "river:periodic_job_id": "report",
        },
        queue: "reports",
        scheduledAt: start,
      },
    });
  });

  it("skips null occurrences and propagates constructor errors", async () => {
    let calls = 0;
    const job = periodicJob({
      construct: () => {
        calls += 1;
        if (calls === 1) return null;
        if (calls === 2) throw new Error("constructor failed");
        return { args: { scope: "c" }, options: { delay: { minutes: 1 } } };
      },
      every: { hours: 1 },
      job: report,
    });

    await expect(
      buildPeriodicInsert({ job, scheduledAt: start })
    ).resolves.toBe(null);
    await expect(
      buildPeriodicInsert({ job, scheduledAt: start })
    ).rejects.toThrow("constructor failed");
    // An explicit delay wins over the occurrence time.
    await expect(
      buildPeriodicInsert({ job, scheduledAt: start })
    ).resolves.toMatchObject({
      options: { delay: { minutes: 1 }, metadata: { periodic: true } },
    });
  });
});
