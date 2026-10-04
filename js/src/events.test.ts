import { describe, expect, expectTypeOf, test } from "vitest";

import { JobStuckError } from "./errors.js";
import {
  EventHub,
  jobEvent,
  type JobFailedEvent,
  type SubscriptionLagEvent,
} from "./events.js";
import type { JobRow } from "./job.js";

describe("EventSubscription", () => {
  test("narrows events by kind and closes with using", async () => {
    const hub = new EventHub();
    const job = { id: 1n } as JobRow;
    {
      using failures = hub.subscribe({ kinds: ["job_failed"] });
      expectTypeOf<Awaited<ReturnType<typeof failures.next>>>().toEqualTypeOf<
        IteratorResult<JobFailedEvent | SubscriptionLagEvent>
      >();
      hub.publish(jobEvent("job_completed", Temporal.Now.instant(), job, null));
      hub.publish(
        jobEvent("job_failed", Temporal.Now.instant(), job, new Error("boom"))
      );
      const next = await failures.next();
      expect(next.value).toMatchObject({ error: new Error("boom"), job });
    }
    // `using` closed and unregistered the subscription.
    hub.publish(jobEvent("job_failed", Temporal.Now.instant(), job, null));
  });

  test("builds job events whose fields match their kind", () => {
    const at = Temporal.Now.instant();
    const job = { id: 1n } as JobRow;
    expect(jobEvent("job_completed", at, job, new Error("ignored"))).toEqual({
      at,
      job,
      kind: "job_completed",
    });
    expect(jobEvent("job_cancelled", at, job, undefined)).toEqual({
      at,
      job,
      kind: "job_cancelled",
    });
    const stuck = new JobStuckError(
      1n,
      Temporal.Duration.from({ milliseconds: 10 }),
      Temporal.Duration.from({ milliseconds: 20 })
    );
    expect(jobEvent("job_stuck", at, job, stuck)).toMatchObject({
      error: stuck,
    });
  });

  test("closing a lagging subscription makes every subsequent next done", async () => {
    const hub = new EventHub();
    const subscription = hub.subscribe({ capacity: 1 });
    for (const queueName of ["one", "two"])
      hub.publish({
        at: Temporal.Now.instant(),
        kind: "queue_removed",
        queueName,
      });
    await subscription.return();
    await expect(subscription.next()).resolves.toEqual({
      done: true,
      value: undefined,
    });
    await expect(subscription.next()).resolves.toEqual({
      done: true,
      value: undefined,
    });
  });
  test("a broken for-await loop closes and unregisters the subscription", async () => {
    const hub = new EventHub();
    const subscription = hub.subscribe();
    const consumed: string[] = [];
    const loop = (async () => {
      for await (const event of subscription) {
        consumed.push(event.kind);
        break;
      }
    })();

    hub.publish({
      at: Temporal.Now.instant(),
      kind: "queue_removed",
      queueName: "first",
    });
    await loop;
    hub.publish({
      at: Temporal.Now.instant(),
      kind: "queue_removed",
      queueName: "second",
    });

    expect(consumed).toEqual(["queue_removed"]);
    await expect(subscription.next()).resolves.toEqual({
      done: true,
      value: undefined,
    });
  });
});
