# Errors, retries, timeouts, and cancellation

## Outcomes

A handler's result decides what happens to its job:

| Handler                           | Job becomes                                                     |
| --------------------------------- | --------------------------------------------------------------- |
| resolves, or returns `complete()` | `completed`; `complete({ output })` also records JSON output    |
| throws or rejects                 | `retryable` (retried later), or `discarded` after `maxAttempts` |
| returns `snooze({ minutes: 5 })`  | scheduled again without using an attempt                        |
| returns `discard({ reason })`     | `discarded` without further retries                             |
| returns `cancel({ reason })`      | `cancelled`, recording `JobCancelError: <reason>` as its error  |

Outcomes are closed values built by those helpers; returning any other object
is an error, so output is never recorded by accident. Use `snooze` for "try
again later" conditions such as rate limits, `discard` when retrying can't
help (a permanently invalid request), and `cancel` when the work should no
longer happen at all. A snooze or retry due within the scheduler interval (5
seconds by default) becomes available immediately, as in River for Go.

Recorded errors hold the thrown message, bounded in size, and the time the
attempt started. Like River for Go, which records stack traces only for
panics, River records a stack only for JavaScript's runtime faults (a native
`TypeError`, `RangeError`, `ReferenceError`, `SyntaxError`, `EvalError`, or
`URIError`); deliberate errors record their message alone. Don't put secrets
or full payloads in error messages: they are stored on the job.

A job whose row River can't read (for example, metadata another tool rewrote
as a JSON array) is never worked, and neither is a job whose kind has no
worker. Its attempt fails before hooks and middleware run, with an error such
as `job row couldn't be decoded: …`, so `errorHandler` sees it and the retry
schedule applies as usual. `client.jobs.get` reports such a row as an error,
but `client.jobs.list`, `cancel`, `retry`, and `delete` still act on it, as in
River for Go, returning the job with the fields River couldn't read left empty
(`{}` or `[]`). Like River for Go, a recorded error that another tool wrote in
a shape River doesn't, such as an `at` that isn't an RFC 3339 timestamp, an
`attempt` stored as a string, or an `error` that's an object, doesn't make
the row unreadable: River keeps what it can of it, leaving such an `at` as the
zero time `0001-01-01T00:00:00Z` and keeping other values as their JSON text.

## Retry schedule

By default the delay before attempt _n + 1_ is _n⁴_ seconds with ±10% jitter,
where _n_ is the number of errors so far, the same curve as River for Go. Set
`maxAttempts` per job, per definition (`defaults`), or per client
(`defaultInsertOptions`). Replace the schedule with `retryPolicy`:

<!-- ts-setup
import { Client, Workers } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
declare const workers: Workers;
-->

```ts
const client = new Client(new PgDriver(new Pool()), {
  // Retry every 30 seconds, whatever the attempt.
  retryPolicy: (_job, now) => now.add({ seconds: 30 }),
  queues: { default: { maxWorkers: 10 } },
  workers,
});
```

A policy that throws, or returns a time that is not a `Temporal.Instant` or
is in the past, falls back to the default schedule.

A worker can set its own `retryPolicy`, like River for Go's
`Worker.NextRetry`. It decides for that kind, both after a failed attempt and
when the rescuer retries a stuck job, once the job's arguments decode; when it
throws or returns something other than a `Temporal.Instant`, the client's
policy decides:

<!-- ts-setup
import { defineJob, Workers } from "riverqueue";
declare const sendEmail: ReturnType<typeof defineJob>;
-->

```ts
const workers = new Workers().add(
  sendEmail,
  async () => {
    // ...
  },
  // Retry email delivery every five minutes.
  { retryPolicy: (_job, now) => now.add({ minutes: 5 }) }
);
```

## Error handler

`errorHandler` observes every failed attempt, after work middleware and hooks
have run, and may cancel the job instead of retrying it. That includes an
attempt stopped by its timeout, whose error is a `JobTimeoutError`. As in
River for Go, it isn't called for an attempt interrupted by a client stop, or
for an error thrown after the job was cancelled remotely, because the
cancellation already decides the outcome (a runtime fault such as a
`TypeError`, the JavaScript analog of a Go panic, still reaches it):

<!-- ts-setup
import { Client, Workers, JobTimeoutError } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
declare const workers: Workers;
declare const errorTracker: { capture(error: unknown, tags: object): void };
-->

```ts
class PermanentError extends Error {}

const client = new Client(new PgDriver(new Pool()), {
  errorHandler: ({ job }, error) => {
    errorTracker.capture(error, { attempt: job.attempt, kind: job.kind });
    if (error instanceof PermanentError) return { cancel: true };
    // Give up on a job that keeps timing out instead of retrying it.
    if (error instanceof JobTimeoutError && job.attempt >= 3) {
      return { cancel: true };
    }
    return undefined;
  },
  queues: { default: { maxWorkers: 10 } },
  workers,
});
```

A failing error handler is logged and ignored; it never changes the job's
outcome.

## Timeouts

Every attempt has a cooperative timeout: `jobTimeout` on the client (1 minute
by default) or `timeout` on the worker registration, which takes precedence.
`null` disables it.

<!-- ts-setup
import { Workers, defineJob } from "riverqueue";
const buildReport = defineJob({ kind: "build_report" });
declare function renderReport(
  args: object,
  options: { signal: AbortSignal }
): Promise<void>;
-->

```ts
const workers = new Workers().add(
  buildReport,
  async ({ job, signal }) => {
    await renderReport(job.args, { signal });
  },
  { timeout: { minutes: 30 } }
);
```

When the timeout expires, the handler's `signal` aborts with a
`JobTimeoutError`. Node cannot stop a running function, so the handler must
pass the signal to the operations it awaits (or check
`signal.throwIfAborted()` in loops). A handler stopped by the abort records the
timeout as its error, reaches `errorHandler`, and is retried.

A job whose timeout is disabled is never rescued: River cannot tell a
long-running attempt from an abandoned one, so it trusts the attempt, as River
for Go does. Otherwise the leader's rescuer retries jobs whose attempt started
longer ago than `maintenance.rescueAfter` (1 hour by default), which recovers
jobs from crashed processes.

## Stuck jobs

An attempt that ignores its signal and keeps running past its timeout is
reported as stuck after the client's `jobStuckThreshold` (10 seconds by
default, like River for Go's `JobStuckThreshold`; it must not be negative):
River emits a `job_stuck` event carrying a
`JobStuckError`, logs it, and calls `stuckHandler`. The handler may return
`{ addWorkerSlot: true }` to let the queue start another job while the stuck
one keeps its slot. The stuck attempt's late result is still guarded: it
cannot overwrite a newer attempt.

For CPU-bound handlers that can't observe a signal, use
[`@riverqueue/worker-threads`](../worker-threads/README.md), which terminates
a handler's thread once it has ignored its aborted signal for
`jobStuckThreshold`.

## Cancellation

`client.jobs.cancel(id)` cancels a job from any client, in any language. A job
that hasn't started never runs. A running attempt's `signal` aborts with a
`JobCancelledError` on whichever process is running it; if the handler then
throws, the job is `cancelled`, and if it completes anyway, the job is
`completed`, matching River for Go.
A job cancelled after a client claimed it but before its handler started
still runs its handler, with its `signal` already aborted, as River for Go
starts its worker with a cancelled context.

When a client stops with `mode: "cancel"`, or a graceful stop's `timeout`
passes, running handlers' signals abort. A handler that stops because of that
abort puts its job back to `available` without using an attempt; a genuine
error is recorded and retried as usual. A handler that still ignores the
abort after `jobStuckThreshold`, such as a worker thread that
`@riverqueue/worker-threads` then terminates, fails with a `JobAbortedError`: its
attempt counts, and the retry policy and `maxAttempts` apply, so a job that
hangs on every stop doesn't retry forever.

Once an attempt finishes, its handler's `signal` aborts with a
`JobAttemptFinishedError`, like River for Go cancelling a job's context when
its executor returns, so work the handler started and left running stops
instead of outliving the attempt.

## Error classes

Everything River throws on purpose is a `RiverError` with a stable `code`,
and each subclass narrows `code`:

| Class                        | `code`                   | When                                                   |
| ---------------------------- | ------------------------ | ------------------------------------------------------ |
| `ConfigurationError`         | `configuration`          | Invalid client, job, driver, or worker configuration   |
| `ValidationError`            | `validation`             | An invalid value passed to a River API                 |
| `PayloadValidationError`     | `payload_validation`     | Args rejected by a schema or decoder (`phase`)         |
| `DatabaseOperationError`     | `database`               | A database operation failed (`retryable` if transient) |
| `MigrationError`             | `migration`              | Planning, applying, or validating migrations failed    |
| `UnsupportedCapabilityError` | `unsupported_capability` | The driver can't do this (a Prisma client can't work)  |
| `BackendMismatchError`       | `backend_mismatch`       | A `tx` that belongs to another driver                  |
| `LifecycleError`             | `lifecycle`              | Misuse of the runtime's lifecycle                      |
| `JobAbortedError`            | `job_aborted`            | A handler ended by force after ignoring a stop's abort |
| `JobAttemptFinishedError`    | `job_attempt_finished`   | A handler signal's reason once its attempt finished    |
| `JobCancelledError`          | `job_cancelled`          | A handler signal's reason after remote cancellation    |
| `JobTimeoutError`            | `job_timeout`            | A handler signal's reason after its timeout            |
| `JobStuckError`              | `job_stuck`              | Reported for stuck attempts                            |
| `JobRunningError`            | `job_running`            | Deleting a running job                                 |
| `UnknownJobKindError`        | `unknown_job_kind`       | A claimed job has no registered worker                 |
| `ExtensionError`             | `extension`              | A hook, middleware, or plugin failed                   |
| `SubscriptionLagError`       | `subscription_lag`       | A subscription dropped events                          |

<!-- ts-setup
import { Client, DatabaseOperationError, RiverError, defineJob } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
const client = new Client(new PgDriver(new Pool()));
const sync = defineJob({ kind: "sync" });
-->

```ts
try {
  await client.insert(sync, {});
} catch (error) {
  if (error instanceof DatabaseOperationError && error.retryable) {
    // A transient failure (lost connection, lock timeout, ...): try again.
  } else if (error instanceof RiverError) {
    console.error(error.code, error.message);
  }
  throw error;
}
```

Database errors in the runtime's own background work (claiming, completing,
leadership) are never fatal: they are logged and retried with backoff. See
[database failures](./runtime.md#database-failures).
