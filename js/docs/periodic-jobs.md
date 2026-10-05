# Periodic jobs

A periodic job is inserted on a schedule by whichever client currently holds
River's leadership. Define one with `periodicJob` and pass it to the client:

<!-- ts-setup
import { Client, Workers, defineJob } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
import { z } from "zod";
declare const workers: Workers;
-->

```ts
import { periodicJob } from "riverqueue";

const pruneSessions = defineJob({
  kind: "prune_sessions",
  schema: z.object({ olderThanDays: z.number().int().positive() }),
});

const client = new Client(new PgDriver(new Pool()), {
  periodicJobs: [
    periodicJob({
      args: { olderThanDays: 30 },
      every: { hours: 1 },
      id: "prune_sessions",
      job: pruneSessions,
      runOnStart: true,
    }),
  ],
  queues: { default: { maxWorkers: 10 } },
  workers,
});
```

Each occurrence is an ordinary job: it is validated, inserted with
`metadata.periodic: true` (and `river:periodic_job_id` when the periodic job
has an `id`), and worked by any client with a worker for its kind.

| Option       | Meaning                                                        |
| ------------ | -------------------------------------------------------------- |
| `job`        | The job definition to insert                                   |
| `args`       | Arguments for every occurrence                                 |
| `construct`  | Or, a function building each occurrence's `{ args, options }`  |
| `options`    | Insert options for every occurrence (with `args`)              |
| `every`      | A fixed interval such as `{ minutes: 15 }` (a day is 24 hours) |
| `schedule`   | Or, a schedule such as `cron("0 9 * * *")`; see below          |
| `runOnStart` | Also insert once each time a client becomes leader             |
| `id`         | A stable identifier, unique per client; see below              |

`construct` may return `null` to skip an occurrence, which suits "only on
weekdays" or "only if there is work" rules:

<!-- ts-setup
import { defineJob, periodicJob } from "riverqueue";
const pruneSessions = defineJob<{ olderThanDays: number }>()({
  kind: "prune_sessions",
});
-->

```ts
const weekdayPrune = periodicJob({
  construct: () => {
    const today = Temporal.Now.plainDateISO("UTC");
    return today.dayOfWeek >= 6 ? null : { args: { olderThanDays: 30 } };
  },
  every: { days: 1 },
  job: pruneSessions,
});
```

## Cron schedules

`cron` parses a standard cron expression with exactly the syntax and timing of
River for Go (robfig/cron's `ParseStandard`), so a schedule fires at the same
times whichever language holds leadership:

<!-- ts-setup
import { defineJob, periodicJob } from "riverqueue";
const sendDigest = defineJob({ kind: "send_digest" });
-->

```ts
import { cron } from "riverqueue";

const dailyDigest = periodicJob({
  args: {},
  id: "daily_digest",
  job: sendDigest,
  schedule: cron("CRON_TZ=America/Chicago 0 9 * * mon-fri"),
});
```

It accepts five fields (minute, hour, day of month, month, and day of week
numbered 0-6 from Sunday), lists, ranges, steps, `*` and `?`, month and
weekday names, the descriptors `@yearly`, `@annually`, `@monthly`, `@weekly`,
`@daily`, `@midnight`, and `@hourly`, and `@every` with a Go duration such as
`@every 1h30m`. When both day of month and day of week are restricted, a day
matching either fires. Anything River Go rejects, including a seconds field
or `7` for Sunday, throws a `ConfigurationError`.

### Time zones

An expression is evaluated in the zone of its `CRON_TZ=` (or `TZ=`) prefix,
else the `timeZone` option, else **the process's local time zone**, which is
what River for Go and Rust use too. Occurrences follow wall-clock time in that
zone: a time skipped by a daylight saving transition doesn't fire that day,
and a repeated one can fire twice.

Periodic jobs run on whichever client leads, and different machines often
have different local zones (containers usually run in UTC). **Pin the zone
explicitly in a mixed-language or multi-region fleet**, preferably in the
expression itself so every language reads it from the same string. River for
Rust bundles no time zone database and accepts only `UTC`, `Local`, and
`Etc/GMT±N` prefixes, so `CRON_TZ=UTC` is the most portable choice:

<!-- ts-setup
import { cron } from "riverqueue";
-->

```ts
cron("CRON_TZ=UTC 0 9 * * *");
cron("0 9 * * *", { timeZone: "UTC" }); // Equivalent, but JavaScript-only.
```

### Custom schedules

`schedule` accepts any `PeriodicSchedule`, an object whose
`next(after: Temporal.Instant)` returns the next occurrence, so calendar
rules cron can't express can be written directly.

`next` must return an instant after `after`, or `null` to stop scheduling. A
schedule that throws stops only its own job, and River logs the error.

## Leadership and failures

Only the leader inserts periodic jobs, so a fleet of clients inserts each
occurrence once. When a client becomes leader it computes every job's next
run from the current time and, with `runOnStart`, inserts one occurrence
immediately. Occurrences due within 100 milliseconds are inserted together
in one transaction.

As in River for Go, a failed occurrence (a constructor that throws, or an
insert that fails) is logged and skipped; the schedule moves on to the next
occurrence rather than retrying, so a persistent failure can't wedge it. Make
the job itself idempotent, or give it `unique` options, if duplicates after a
leadership change would matter.

## Adding and removing at runtime

`client.periodicJobs` is the live registry. Changes take effect on the leader
immediately, so make the same change on every client that may lead. A client
created with `leaderElectionDisabled: true` never leads, and modifying its
registry throws a `ConfigurationError`:

<!-- ts-setup
import { Client, defineJob, periodicJob } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
const client = new Client(new PgDriver(new Pool()));
const refreshCache = defineJob({ kind: "refresh_cache" });
-->

```ts
const handle = client.periodicJobs.add(
  periodicJob({
    args: {},
    every: { minutes: 5 },
    id: "cache",
    job: refreshCache,
  })
);
client.periodicJobs.remove(handle);
client.periodicJobs.removeById("cache");
```

## Mixed-language fleets

Leadership is shared across River for Go, Rust, and JavaScript. Periodic jobs
run in whichever language currently leads, and only the periodic jobs that
client registered run. If a Go service registers a periodic job that the
JavaScript services don't (or the reverse), that job silently stops whenever
leadership moves to the other language, which can happen on any deploy.

Either register the same periodic jobs, with the same kinds, schedules, and
IDs, in every language that may lead (with cron schedules pinned to one time
zone, such as `CRON_TZ=UTC`), or keep periodic registration in one
language and disable leader election (`leaderElectionDisabled: true`) on
clients in the others so they never lead.

## Durable schedules

A periodic job with an `id` can have its next run persisted by an extension
that provides a periodic job store, so a new leader continues the schedule
instead of restarting it from the current time. River core ships no store;
the `onPeriodicJobsStart` hook reports the durable records an installed
store found when a leader starts inserting periodic jobs.
