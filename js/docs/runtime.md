# Runtime, concurrency, and the event loop

River's Node.js runtime is asynchronous and supervised. `client.start()`
returns a `RunHandle`; the application owns that handle until shutdown. Keep
`run.completed` observed because it rejects if the runtime fails.

<!-- ts-setup
import { Client, Workers } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
declare const workers: Workers;
const client = new Client(new PgDriver(new Pool()), {
  queues: { default: { maxWorkers: 10 } },
  workers,
});
-->

```ts
await using run = await client.start();

process.once("SIGTERM", () => {
  void run.stop({ mode: "graceful", timeout: { seconds: 30 } });
});

await run.completed;
```

Graceful shutdown stops fetching new work and lets active handlers finish, up
to `timeout` if one is given. River never installs signal handlers itself;
wire `SIGTERM` as above. Cancel shutdown also aborts active handler signals. A
job timeout, remote cancellation, or shutdown abort is cooperative for an
in-process handler; a late result is guarded by the attempt identity and
cannot overwrite a newer attempt.

Like River for Go, a stopping leader resigns and ends maintenance as soon as
the stop begins, while its queues drain; another client can take over
maintenance meanwhile. Stopping calls share one shutdown. When the runtime
fails, it shuts down the same way, and `run.completed` rejects with the
failure once that cleanup finished.

## Concurrency and the event loop

`maxWorkers` bounds active handlers per queue. This is I/O concurrency, not a
pool of native threads. Async database and network handlers normally belong
in-process, where Node can run many of them efficiently. Synchronous CPU work
blocks job claiming, completion, cancellation, and application code in that
process even when `maxWorkers` is high.

Each queue also has a `fetchCooldown` minimum between claim queries and a
`pollInterval` fallback when no insertion notification arrives. They default
to the client's `fetchCooldown`, 100 milliseconds unless set, and 1 second,
matching River's other runtimes. Like River for Go, each poll waits a random
extra of up to a tenth of `pollInterval`, at least 10 milliseconds, so
producers don't poll in lockstep. A notification wakes the poll wait but never
bypasses the cooldown, so bursts coalesce instead of creating a database query
per inserted job. Lower the cooldown deliberately for high-throughput queues
and keep the poll interval at least as large as the cooldown.

Like River for Go, a client sends at most one insert notification per queue
per client `fetchCooldown`, on every driver, whether it inserts jobs directly,
through periodic jobs, or by scheduling them as their time comes. A producer
fetches at most once per cooldown anyway, and polls for a job whose
notification was suppressed. Retrying a job sends no insert notification.

River measures event-loop delay and exposes it in runtime diagnostics. Treat
sustained delay as an operational fault: move CPU work to
`@riverqueue/worker-threads`, a separate process, or a dedicated service rather
than increasing queue concurrency.

Worker threads are bounded and terminate a handler that ignores its abort
signal for the client's `jobStuckThreshold`. The structured-clone boundary
carries persisted JSON arguments, not decoder functions or arbitrary
transformed class values. Worker modules should decode any richer local
representation themselves.

## Retries and errors

A resolved handler completes the job. Throwing or rejecting records an attempt
error and applies the configured retry policy. Return `snooze(...)` when the
attempt should not count, `discard(...)` when no retry should occur, or
`cancel({ reason })` to cancel the job permanently like Go's `river.JobCancel`,
recording `JobCancelError: <reason>` as the attempt error. River
bounds recorded errors and stacks; do not place credentials or sensitive
payloads in thrown messages.

Each recorded error is stamped with the time its attempt started. Go records a
stack trace only for panics, and River's JavaScript analog of a panic is a
runtime fault raised as a native `TypeError`, `RangeError`, `ReferenceError`,
`SyntaxError`, `EvalError`, or `URIError`. Only those record their stack; an
error thrown deliberately, including a subclass of one of those classes,
records its message with an empty trace.

Like River's other runtimes, a retry or snooze due
within one scheduler interval (5 seconds by default) is stored as `available`
with its future `scheduled_at`, so it runs on time instead of waiting for the
leader's next scheduler pass.

Handlers receive one `AbortSignal`. Pass it into database, HTTP, and other
cancel-aware operations, and catch an abort only to clean up. How an aborted
attempt is recorded matches River's other runtimes:

- After a remote cancellation, a handler that still resolves successfully
  completes the job. Throwing (for example through `signal.throwIfAborted()`)
  or returning `snooze(...)` or `discard(...)` cancels it.
- After a job timeout, success completes the job, a handler stopped by the
  abort records the timeout as its error, and any other error is recorded as
  thrown.
- During shutdown, only a handler stopped by the shutdown abort is made
  available again without consuming an attempt. Success completes the job and
  a genuine error is recorded and retried as usual.

`job_cancelled` and `job_interrupted` events are emitted once, after the
database transition commits. A late result from an attempt that no longer owns
its row is ignored.

## Database failures

Database errors in background work are operational, not fatal. A lock held by
another session, a `statement_timeout`, a failover, or a saturated pool is
logged through the client's `logger` and retried; it never stops the runtime.
`run.completed` rejects only for configuration errors and internal invariant
violations.

Claims, queue control polling, and notification streams retry after
exponential backoff with jitter, from 250 milliseconds up to 30 seconds, and
reset after a success. A notification stream that fails is resubscribed, and
every successful resubscription polls all queues and their controls so work
inserted while the listener was down is not left waiting for the next poll
interval. It also reads the client's running jobs and cancels any that were
cancelled while the listener was down. Like River for Go, a client whose
notification stream can't connect and listen when it starts rejects
`client.start()` with that error before claiming any job. Leader election
retries sooner than its normal interval after a failure; maintenance services
record the failure as a `maintenance_failed` event and try again on their next
interval. Transient failures are logged as warnings and other failures as
errors.

Completions are persisted in batches. Each attempt is bounded to 10 seconds
and a batch gets three attempts with 1, 2, and 4 second backoff, matching
River's other runtimes. After that, a transient failure (a
`DatabaseOperationError` whose `retryable` is true) requeues the batch, and
workers wait for completion capacity until the database recovers. Any other
failure drops the batch: its jobs stay `running` and the rescuer retries them
after `maintenance.rescueAfter`. Both outcomes are logged and reported as the
`job_completion_requeued` and `job_completion_dropped` metrics. During
shutdown the first persistent failure abandons the remaining completions to
the rescuer, so `stop()` finishes even while the database is unavailable.

A completion changes a job only while the job still belongs to the attempt
that produced it: the same attempt number, claimed by the same client. When
that attempt's job has already left `running`, for example because the
rescuer retried it or it was cancelled, the attempt's recorded output and
metadata are still merged into the job, as in River for Go, and its state is
left alone. Unlike River for Go, a late completion never changes a job that
another client has since claimed again; it's reported as a `job_race` event.

## Process scaling

One process can efficiently run I/O-bound work. Add ordinary process replicas
for CPU/control overhead, availability, or deployment isolation. River's
database protocol coordinates claims and leadership across JavaScript, Go, and
Rust processes; no JavaScript-specific process manager is hidden in the
library.

## Clients that never lead

`leaderElectionDisabled: true` keeps a client out of leader election, like
River for Go's `Config.LeaderElectionDisabled`. It works jobs from its
configured queues as usual, but never runs the scheduler, rescuer, cleaners,
reindexer, or periodic jobs. Use it for processes dedicated to particular job
kinds that shouldn't take on anything else:

<!-- ts-setup
import { Client, Workers } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
declare const videoWorkers: Workers;
-->

```ts
const videoClient = new Client(new PgDriver(new Pool()), {
  leaderElectionDisabled: true,
  queues: { video: { maxWorkers: 4 } },
  workers: videoWorkers,
});
```

At least one other started client on the same database and schema, in any
language, must remain eligible to lead. Otherwise scheduled jobs, retries,
periodic jobs, stuck-job rescue, and cleanup stop progressing: a client with
leader election disabled never leads, even when no other client is running.
Such a client can't configure `periodicJobs` or modify `client.periodicJobs`
(both throw a `ConfigurationError`), though it works periodic jobs that the
leader inserts into its queues. Its `maintenance` settings have no effect.

## Sharing a queue with clients that know other kinds

By default a client claims every job in its queues, and a job whose kind has
no worker fails with an unknown job kind error. `fetchOnlyKnownKinds: true`
limits claims to the kinds the client has workers for when it starts, like
River for Go's `Config.FetchOnlyKnownKinds`. Jobs of other kinds stay
available without using an attempt, so clients with different workers can
share a queue, such as while job kinds move from one language to another:

<!-- ts-setup
import { Client, Workers } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
declare const migratedWorkers: Workers;
-->

```ts
const client = new Client(new PgDriver(new Pool()), {
  fetchOnlyKnownKinds: true,
  leaderElectionDisabled: true,
  queues: { default: { maxWorkers: 10 } },
  workers: migratedWorkers,
});
```

The option affects only claiming. A leader's rescuer still handles stuck jobs
in every queue and discards those whose kinds it doesn't know, so a client
with only some of the kinds should also set `leaderElectionDisabled`, with
another eligible client, in any language, that has workers for every kind.
