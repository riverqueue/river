# Logging, events, and metrics

River reports what it does through four channels: a logger, hooks and
middleware, event subscriptions, and Node's `diagnostics_channel`. None of
them require a particular logging or telemetry library.

## Logging

Pass any logger with pino's argument order, `(attributes, message)`:

<!-- ts-setup
import { Client, Workers } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
declare const workers: Workers;
-->

```ts
import pino from "pino";

const client = new Client(new PgDriver(new Pool()), {
  logger: pino({ name: "worker" }),
  queues: { default: { maxWorkers: 50 } },
  workers,
});
```

River logs failures in its background work (database retries, dropped
completions, stuck jobs, failing hooks) at `warn` and `error`. Without a
`logger`, those two levels go to `console`; pass `logger: false` to silence
them. Handlers get `ctx.logger`, the same logger with `jobId`, `jobKind`, and
`attempt` attached, which accepts `logger.info("message")` or
`logger.info({ key: value }, "message")`.

## Hooks and middleware

Middleware wraps work like Koa middleware and must call `next()` once. Hooks
observe lifecycle points: `beforeInsert`/`afterInsert`,
`beforeWork`/`afterWork`, `onEvent`, `onMetric`, and `onPeriodicJobsStart`.
Both run in registration order, plugins first, then the client's own. As in
River for Go, insert and work hooks run inside the innermost middleware, so a
middleware span covers them. An `afterWork` hook that returns a result
replaces the attempt's result, and an error thrown by `beforeWork` becomes the
attempt's error without the worker running:

<!-- ts-setup
import { Client, Workers } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
declare const workers: Workers;
declare const metrics: {
  increment(name: string, tags: Record<string, string>): void;
  timing(name: string, milliseconds: number, tags: Record<string, string>): void;
};
-->

```ts
const client = new Client(new PgDriver(new Pool()), {
  hooks: {
    onEvent: (event) => {
      if (event.kind === "job_failed") {
        metrics.increment("river.job.failed", { kind: event.job.kind });
      }
    },
  },
  middleware: [
    async ({ job }, next) => {
      const started = performance.now();
      try {
        return await next();
      } finally {
        metrics.timing("river.job.duration", performance.now() - started, {
          kind: job.kind,
        });
      }
    },
  ],
  queues: { default: { maxWorkers: 50 } },
  workers,
});
```

Middleware and work hooks run inside the attempt, so their time is part of the
job's. `onEvent` hooks receive events after the database change commits,
through a bounded queue, so a slow hook doesn't hold up job processing; a
stopping client delivers queued events before it finishes stopping. A hook
that throws is logged and ignored.

## Event subscriptions

`client.subscribe()` returns an async iterable of events, emitted after their
database transitions commit. Filtering by `kinds` narrows the event type:

<!-- ts-setup
import { Client } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
const client = new Client(new PgDriver(new Pool()));
-->

```ts
using failures = client.subscribe({ capacity: 1_000, kinds: ["job_failed"] });
for await (const event of failures) {
  if (event.kind === "subscription_lag") {
    console.warn(`dropped ${event.dropped} events`);
  } else {
    console.error(event.job.id, event.error);
  }
}
```

| Events                                                                                | Carry                         |
| ------------------------------------------------------------------------------------- | ----------------------------- |
| `job_started`, `job_completed`, `job_snoozed`, `job_interrupted`                      | `job`                         |
| `job_failed`                                                                          | `job`, `error`                |
| `job_cancelled`                                                                       | `job`, `error` if one         |
| `job_stuck`                                                                           | `job`, a `JobStuckError`      |
| `job_race` (a finished attempt no longer owned its row)                               | `job`                         |
| `queue_added`, `queue_paused`, `queue_resumed`, `queue_updated`, `queue_reconfigured` | `queue`                       |
| `queue_removed`                                                                       | `queueName`                   |
| `leader_acquired`, `leader_lost`                                                      | `leader`                      |
| `maintenance_succeeded`, `maintenance_failed`                                         | `service`, `count` or `error` |
| `runtime_event_loop_delay`                                                            | `eventLoopDelay`              |
| `subscription_lag`                                                                    | `dropped`, `error`            |

A subscription buffers up to `capacity` events (256 by default). A consumer
that falls behind loses the oldest events and receives one
`subscription_lag` event saying how many. Subscriptions are for observing
River, not a second queue: react to business events with jobs. Breaking out
of `for await`, `using`, `close()`, or an aborted `signal` closes a
subscription.

## `diagnostics_channel`

River publishes to three channels, which cost nothing without subscribers and
suit tracing and APM integrations:

| Channel             | Message                                                          |
| ------------------- | ---------------------------------------------------------------- |
| `riverqueue:event`  | Every `RiverEvent`, as delivered to subscriptions                |
| `riverqueue:metric` | Every `RiverMetric`                                              |
| `riverqueue:work`   | `{ context, result }` after each attempt, before it is persisted |

<!-- ts-setup
declare const histogram: { record(value: number, tags: object): void };
-->

```ts
import { subscribe } from "node:diagnostics_channel";
import type { RiverMetric } from "riverqueue";

subscribe("riverqueue:metric", (message) => {
  const metric = message as RiverMetric;
  if (metric.name === "job_get_available_duration") {
    histogram.record(metric.duration.total("milliseconds"), {
      queue: metric.queue,
    });
  }
});
```

Metrics are `job_get_available_duration` and `job_get_available_count` (per
claim query, by queue) and `job_completion_requeued`/`job_completion_dropped`
(completion persistence failures). Hooks receive the same values through
`onMetric`.

River core has no OpenTelemetry dependency. To trace jobs, start a span in a
work middleware (the middleware runs inside the attempt's async context) and
record attributes from `ctx.job`; propagate trace context from the producer
through job `metadata`.

## Runtime diagnostics

`run.diagnostics` is a snapshot of the running client: active attempts,
pending completions, per-queue configuration and pause state, the latest
event-loop delay, leadership, and maintenance runs. Worth alerting on:
sustained event-loop delay, `job_stuck` events, `job_completion_dropped`,
repeated `maintenance_failed`, `subscription_lag`, and growing pool wait in
your `pg` instrumentation.

Never put job arguments, database URLs, or credentials into metric labels,
and redact diagnostics before sending them across a trust boundary.
