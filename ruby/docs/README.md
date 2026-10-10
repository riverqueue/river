# River client for Ruby [![Build Status](https://github.com/riverqueue/river/actions/workflows/ruby.yaml/badge.svg)](https://github.com/riverqueue/river/actions/workflows/ruby.yaml) [![Gem Version](https://badge.fury.io/rb/riverqueue.svg)](https://badge.fury.io/rb/riverqueue)

A Ruby client for [River](https://github.com/riverqueue/river), packaged in the [`riverqueue` gem](https://rubygems.org/gems/riverqueue). It inserts and works jobs using River's canonical database schema and state machine, so Ruby and Go clients can safely share a River database. Separate queues are recommended when each language recognizes different job kinds.

## Installation

Moving an existing application? See [Migrating from Sidekiq](migrating_from_sidekiq.md).

Add one River driver and only the database adapter your application uses. The driver brings in `riverqueue`:

```ruby
# Sequel
gem "riverqueue-sequel"
gem "pg" # or: gem "sqlite3"

# Active Record
gem "riverqueue-activerecord"
gem "pg" # or: gem "sqlite3"
```

Apply River's canonical migrations before using the client. See [Schema and migrations](#schema-and-migrations).

Both SQL drivers also support [YugabyteDB](yugabyte.md), automatically detecting
its uniqueness and notification capabilities.

## Schema and migrations

Run migrations before inserting jobs or starting workers. The bundled `river`
command uses Go's canonical Postgres and SQLite migrations; no Go installation
is needed. After installing a River driver and its database adapter, create your
application's Postgres database and run:

```sh
export DATABASE_URL=postgres://localhost/my_app

bundle exec river migrate-status
bundle exec river migrate-up --dry-run
bundle exec river migrate-up
```

`migrate-up` applies all pending migrations and is safe to rerun: already-applied
versions are skipped, including migrations applied by Go. `--dry-run` previews
the plan without changing the database. Pass `--database-url URL` to override
`DATABASE_URL`.

For SQLite, ensure the parent directory exists, then use a SQLite URL:

```sh
# With riverqueue-sequel:
bundle exec river migrate-up --database-url sqlite://storage/river.sqlite3
```

With `riverqueue-activerecord`, use `sqlite3://storage/river.sqlite3` instead.
The command auto-detects the installed River driver, preferring Sequel when
both are in your bundle.

For River Pro, install `riverqueue-pro`, apply the main migrations above, then
run `bundle exec river migrate-up --line pro` against the same database.

See the [migration guide](migrations.md) for the Ruby API, Postgres schemas,
target versions, downgrade precautions, and existing SQLite Pro installations.
Test schema snapshots under `spec/support` are test-only and must not be used
to provision production databases.

## Basic usage

Define JSON-serializable job arguments and a worker with the same `kind`, register the worker on a queue, and start the client:

```ruby
require "riverqueue-sequel"

class SortArgs
  attr_reader :strings

  def initialize(strings:)
    @strings = strings
  end

  def kind = "sort"
  def to_json = JSON.generate(strings: strings)
end

class SortWorker
  def self.kind = "sort"

  def work(job)
    job.output = {strings: job.args.fetch("strings").sort}
  end
end

client = River::Client.new(
  River::Driver::Sequel.new(DB),
  config: River::Config.new(
    queues: {ruby: 10},
    workers: River::Workers.new.add(SortWorker)
  )
).start

result = client.insert(
  SortArgs.new(strings: %w[whale tiger bear]),
  queue: :ruby
)
result.job # River::JobRow

client.stop
```

Job arguments must respond to `#kind` and `#to_json`. They may also return default options from `#insert_opts`; options passed directly to `#insert` take precedence. Workers receive a `River::Job`, which delegates persisted attributes like `id`, `args`, `attempt`, and `metadata` to its `River::JobRow`.

Use strings for `#kind` definitions, and symbols for identifiers such as queues,
states, periodic job IDs, and resumable step names. Both forms are accepted.
Persisted job attributes and JSON object keys remain strings, preserving
compatibility with Go clients.
Examples omit parentheses on simple calls and `do...end` blocks, retaining them
for nested expressions and `{ ... }` blocks where they make binding clear.

## Core features

### Block workers

Pass a kind and a block to `Workers#add` to define a worker inline:

```ruby
workers = River::Workers.new
workers.add(:message) do |job|
  puts "Message: #{job.args.fetch("message")}"
end

# With client configured to use this registry and the ruby queue:
client.insert River::JobArgsHash.new(:message, message: "Hello!"), queue: :ruby
```

Pass this registry as `workers:` in `River::Config`. The kind accepts a string
or symbol and must match the inserted job's kind. The block receives a
`River::Job` and uses the client's retry and timeout policies. Returning normally
completes the job; the block's return value is ignored. Raise exceptions to fail,
cancel, or snooze work as with a worker class. Optionally use `job.output =` to
persist a result. Aliases are supported through `aliases:` as for other workers.
Supply a worker or a block, not both. Existing procs or methods can be passed as
the block with `&`.
The same block may run concurrently for multiple jobs, so any captured mutable
state must be safe to share across threads.

### [Accessing the client from workers](https://riverqueue.com/docs/context-client)

Every running `River::Job` exposes the client that claimed it. Workers can use
`job.client` to insert follow-up work or call other client APIs without relying
on a global:

```ruby
def work(job)
  result = process(job.args)
  job.client.insert NotifyArgs.new(result_id: result.id)
end
```

### [Job insertion and options](https://riverqueue.com/docs/inserting-and-working-jobs)

Insertion keywords control the queue, priority, maximum attempts, schedule,
tags, metadata, and uniqueness of a job. Priority `1` is highest.
`max_attempts` must be between 1 and 32,767 on every database.
An explicit `state` must be `:available`, `:pending`, or `:scheduled` (strings
are also accepted). Other states raise `ArgumentError` before any jobs are
inserted, including when an invalid state appears in a batch. Without an
explicit state, jobs are available immediately or scheduled when `scheduled_at`
is in the future.

```ruby
result = client.insert(args,
  max_attempts: 10,
  metadata: {trace_id: trace_id},
  priority: 1,
  queue: :critical,
  tags: %w[billing customer-42]
)
```

For reusable options, pass `insert_opts: River::InsertOpts.new(...)` instead.
Do not mix an options object with keyword options in the same call. Options
supplied at insertion override argument-level `#insert_opts` defaults; metadata
is merged, with call-site values winning.

For simple jobs, `River::JobArgsHash.new(:kind, hash)` avoids defining an argument class.

### [Transactional enqueueing](https://riverqueue.com/docs/transactional-enqueueing)

Insertion hooks and middleware execute inside the insertion transaction. If
they raise, their database writes and the enqueue roll back together. Postgres
insert notifications are delivered only when the surrounding transaction commits.

Inserts automatically join a transaction opened through the same Active Record connection or Sequel database object. A rollback also rolls back the job:

The ActiveRecord driver defaults to `ActiveRecord::Base`. Use
`River::Driver::ActiveRecord.new(connection_class: ApplicationRecord)` to select
an abstract connection class. Queries, inserts, transactions, runtime operations,
and migrations all use its pool. Each driver has an isolated internal model and
does not inherit application scopes or callbacks. Transactions on a different
connection do not roll back River inserts.

```ruby
DB.transaction do
  save_order
  client.insert FulfillOrderArgs.new(order_id: order.id)
end
```

The equivalent works inside `ActiveRecord::Base.transaction` with the Active Record driver.

### [Bulk insertion](https://riverqueue.com/docs/inserting-many-jobs)

`#insert_many` inserts a batch atomically and returns one `River::JobInsertResult` per input. Use `River::InsertManyParams` when jobs need different options:

```ruby
results = client.insert_many([
  SortArgs.new(strings: %w[c b a]),
  River::InsertManyParams.new(
    SortArgs.new(strings: %w[z y x]),
    queue: :bulk
  )
])
```

### [Scheduled jobs](https://riverqueue.com/docs/scheduled-jobs)

Set `scheduled_at` to keep a job from becoming available before a future UTC time. River's maintenance leader promotes it when due:

```ruby
client.insert args, scheduled_at: Time.now.utc + 3600
```

### [Unique jobs](https://riverqueue.com/docs/unique-jobs)

`River::UniqueOpts` can make a kind unique by all or selected arguments, time period, queue, and state. A conflict returns the existing job with `result.unique_skipped_as_duplicate?` true. The older `unique_skipped_as_duplicated` reader remains available as a compatibility alias.

```ruby
result = client.insert(args,
  unique_opts: River::UniqueOpts.new(
    by_args: [:account_id],
    by_period: 15 * 60,
    by_queue: true
  )
)
```

Custom `by_state` sets must contain `:available`, `:pending`, `:running`, and `:scheduled`. Set `exclude_kind: true` to enforce the same key across multiple job kinds.
It requires `by_args`, `by_queue`, or a nonzero `by_period`; without another
key dimension, insertion raises `ArgumentError`. Setting `by_state` alone
does not satisfy this requirement.

Use nested arrays to select nested unique fields, for example
`by_args: [[:account, :id], :region]`. A string such as `"account.id"` selects a
literal top-level key. For cross-language deduplication, producers must agree
on encoded argument values: escaping, number representations, and nested key
order affect the hash even when the decoded JSON is equivalent.

### [Reliable execution and stuck jobs](https://riverqueue.com/docs/reliable-workers)

Claims and state transitions are atomic in the database. If a process disappears while working, the elected maintenance client checks for stale running jobs after an hour, retrying or discarding them according to their attempt count. Rescue respects longer client and worker timeouts; disabling a job's timeout also disables automatic timeout-based rescue for it. Cancellation requests still take precedence. Keep worker registrations and timeout configuration consistent across maintenance clients. `attempted_by`, attempt errors, and final state remain in the canonical River row for inspection by Ruby, Go, or River UI.

### [Job retries](https://riverqueue.com/docs/job-retries)

An exception normally moves a job to `retryable`; exhausting `max_attempts` moves it to `discarded`. Workers may choose an absolute retry time, or a client-wide policy may calculate it:

```ruby
class APIWorker
  def self.kind = "api"
  def work(job) = call_api(job.args)
  def next_retry(_job, _error) = Time.now.utc + 30
end

config = River::Config.new(
  retry_policy: MyRetryPolicy.new # responds to next_retry(job, error, now:)
)
```

Use `client.job_retry(job_id)` to make a non-running job available immediately.
This clears its previous cancellation request. Once a job has reached 32,767
attempts, requesting another retry raises `ArgumentError` without changing it.

A worker may implement `retry?(job, error)` and return false to discard a
reported error immediately, without reducing the attempt budget used for crash
recovery. The Rails integration uses this to let Active Job own application retries.
Jobs claimed and finished by batch extensions honor the registered worker's
`retry?` and `next_retry` hooks too, including workers registered as classes.
If a retry hook or policy raises, River logs the callback error and falls back to
retrying with the default backoff. The original work error is still recorded.
Stuck-job rescue also falls back per job when a retry policy raises or returns
an invalid time, allowing other rescues and cleanup to continue.

### [Error handling and timeouts](https://riverqueue.com/docs/error-handling)

Errors are recorded on the job with their attempt, message, timestamp, and trace. `error_handler` may return `:cancel` or `true` to cancel instead of retrying. `job_timeout` defaults to 60 seconds; a worker-specific `timeout(job)` may override it, return `nil` to disable it, or return `0` to use the client default.

A stored job that cannot be decoded fails with `River::JobRowDecodeError` before
worker hooks or middleware run. Its attempt uses the client's error handler and
retry policy; healthy jobs in the same fetch still run. Administrative reads
raise the same exception, with readable fields available through `error.job`.
Corrupt values remain stored for diagnosis, except that malformed error history
is wrapped in an array so the new failure can be appended.

```ruby
config = River::Config.new(
  error_handler: ->(error, _job) { :cancel if error.is_a?(PermanentError) },
  job_timeout: 30
)
```

### [Cancelling jobs](https://riverqueue.com/docs/cancelling-jobs)

Cancel a job externally with `client.job_cancel(id)`. Available jobs finalize immediately; running workers are interrupted after the runtime observes the cancellation marker. A worker can cancel itself by raising the error returned from `River.job_cancel`.

```ruby
client.job_cancel job_id

def work(job)
  raise River.job_cancel("account closed") if account_closed?(job)
end
```

`River.job_cancel` also accepts an exception, which is retained as the
`River::JobCancelError` cause. The error class is public for rescue clauses and
test assertions.

### [Snoozing jobs](https://riverqueue.com/docs/snoozing-jobs)

Raise the error returned by `River.job_snooze` to reschedule without consuming an attempt. Short snoozes become immediately fetchable after their delay; longer ones are promoted by maintenance. `River::JobSnoozeError` remains public for rescue clauses and test assertions.

```ruby
def work(job)
  raise River.job_snooze(30) unless dependency_ready?(job)
end
```

### [Multiple queues](https://riverqueue.com/docs/multiple-queues)

Queues isolate throughput and set independent thread concurrency. Each queue has a producer thread, and claimed jobs run in worker threads up to `max_workers`.

```ruby
config = River::Config.new(queues: {
  bulk: River::QueueConfig.new(
    fetch_cooldown: 0.2,
    fetch_poll_interval: 1.0,
    max_workers: 4
  ),
  critical: River::QueueConfig.new(max_workers: 20)
})
```

Queues may also be added and removed at runtime with `client.queue_add(name, config)` and `client.queue_remove(name)`.
Queue removal stops fetching and keeps observing cancellation requests until its
active jobs finish. Removing a queue from one of its own workers raises
`ThreadError`, because that worker cannot wait for itself to finish.

### [Pausing queues](https://riverqueue.com/docs/pausing-queues)

Pausing is persisted, so every client sharing the database observes it. Pass `"*"` to affect all queues.

```ruby
client.queue_pause :bulk
client.queue_resume :bulk

client.queue_pause "*"
client.queue_resume "*"
```

Use `queue_get`, `queue_list`, and `queue_update` to inspect queues and attach metadata.
Pause and resume events contain a snapshot of each affected queue. Wildcard
operations publish events for every affected queue; size subscription buffers
to accommodate the number of events you expect to receive.

### Rails and Active Job

Install the separate `riverqueue-rails` gem for `config.active_job.queue_adapter = :river`,
Active Job/Action Mailer execution, Rails context handling, and `bin/jobs start`.
See the [Rails integration guide](../rails/riverqueue-rails/README.md) for setup,
transactional enqueueing, retry semantics, and supported Rails versions.

### [Periodic jobs](https://riverqueue.com/docs/periodic-jobs)

Register a schedule and a factory block that returns job arguments,
`[arguments, insert_options]`, or `nil` to skip that run. The block runs when
the job is due, not during registration. A reusable callable can be passed as
`constructor:` instead; don't supply both. Core periodic schedules live in the
client process; River Pro adds durable schedules.

A started client can produce periodic jobs with `queues: {}` while other clients
consume them. Jobs added through `client.periodic_jobs` also start maintenance
when needed. Periodic registration requires leader election to be enabled,
including jobs registered after startup.

```ruby
cleanup = River::PeriodicJob.new(
  id: :cleanup,
  run_on_start: true,
  schedule: River::PeriodicInterval.new(3600)
) do
  [CleanupArgs.new, River::InsertOpts.new(queue: :maintenance)]
end

config = River::Config.new(periodic_jobs: [cleanup])
handle = client.periodic_jobs.add(another_periodic_job)
client.periodic_jobs.remove handle
```

`client.periodic_jobs` returns a `River::PeriodicJobBundle`. Removing a handle
returns the removed `PeriodicJob`, or `nil` when absent. `clear` removes all
registrations and returns the bundle.
`add_many` registers a batch atomically: duplicate IDs or invalid schedules leave
the registry unchanged.

Inserted jobs carry `periodic: true` in metadata and, when the registration has
an ID, `river:periodic_job_id`. Constructor options and application metadata are
preserved.

Schedules can be callbacks (`schedule: ->(now) { now + 300 }`) or objects
implementing `next(time)`. They must return a `Time` strictly after the supplied
time; they do not execute the job themselves. A schedule that raises at runtime
is logged and retried on the next maintenance pass without blocking other jobs.

For calendar schedules, add `gem "fugit", "~> 1.13"` to your Gemfile:

```ruby
cleanup = River::PeriodicJob.new(
  id: :weekday_cleanup,
  schedule: River::PeriodicCron.new("0 9 * * 1-5", timezone: "America/New_York")
) { CleanupArgs.new }
```

`PeriodicCron` parses once and loads Fugit only when constructed. Fugit is not
a runtime dependency of the River gem. The timezone defaults explicitly to UTC;
set `timezone:` or use a `CRON_TZ=` / `TZ=` prefix, which takes precedence.
Five-field cron, optional seconds, and aliases such as `@daily` use Fugit's
syntax; `?` is also accepted as a wildcard. `@every 1h30m` uses Go duration
syntax, rounded down to whole seconds with a one-second minimum. Unlike
`PeriodicInterval`, these intervals align to whole-second boundaries.
Results are UTC `Time` objects. Nonexistent spring-forward times are skipped;
both occurrences of a repeated fall-back time are scheduled. See
[conformance differences](conformance.md) before sharing schedules across
languages. Cron does not replay missed occurrences after downtime.

For a one-time date, insert a job with
`client.insert(args, scheduled_at: Time.utc(2026, 9, 20, 9))` instead of registering
a periodic job. This stores the scheduled job immediately in the database.

### [Resumable jobs](https://riverqueue.com/docs/resumable-jobs)

Long jobs can checkpoint idempotent steps and cursor progress. On retry, River skips completed steps and resumes a cursor step from its last recorded value using the same metadata format as Go.

```ruby
def work(job)
  job.resumable_step :download do
    download(job.args)
  end

  job.resumable_step_cursor :rows, default: 0 do |last_row|
    import_rows(after: last_row) do |row|
      job.resumable_set_cursor row.id
    end
  end
end
```

Step exceptions propagate immediately: later code in the worker does not run
unless it explicitly rescues the error. Middleware and error hooks see the same
exception as for ordinary work.
Step names must be unique within an invocation, and steps cannot be nested:
skipping an outer step on retry would make an inner checkpoint unreachable.

`job.resumable_checkpoint(cursor: value)` writes a checkpoint immediately. Omit
`cursor:` to checkpoint the current step with any cursor already recorded. For
atomic application writes, put the transaction **inside** the step and let
rollback errors propagate out of it:

```ruby
job.resumable_step_cursor :rows, default: 0 do |last_row|
  import_rows(after: last_row) do |row|
    job.client.driver.transaction do
      save_row(row)
      job.resumable_checkpoint cursor: row.id
    end
  end
end
```

Use the same database connection for `save_row` and the checkpoint. A rolled-back
checkpoint is not replayed when the attempt fails; retries use the last committed
progress. Do not swallow rollback errors or wrap an entire completed step in a
transaction that may subsequently roll back. As with ordinary jobs, steps must
remain idempotent.

### [Recorded output and metadata](https://riverqueue.com/docs/recorded-output)

Assign `job.output` to store JSON-compatible output under `metadata["output"]`. Use `job.update_metadata` for other metadata that should be committed with the attempt's final transition.

Values are validated and copied when assigned. Invalid JSON fails the attempt
normally without preventing its error from being recorded. `job.metadata` and
`job.metadata_updates` return snapshots; use `update_metadata` to persist changes.

```ruby
def work(job)
  job.update_metadata provider_request_id: request_id
  job.output = {imported: 42}
end
```

### Job-persisted logging

Add `River::JobPersistedLogging::Plugin` to save worker logs with each job attempt,
using the same `metadata["river:log"]` format as Go's `riverlog` and River UI. This
is a core feature; it does not require Pro or additional migrations.

```ruby
config = River::Config.new(
  plugins: [River::JobPersistedLogging::Plugin.new],
  workers: workers,
  queues: {default: 10}
)

def work(job)
  job.logger.info "Starting import"
  job.logger.warn "Skipped a malformed row"
end
```

`job.logger` is a fresh standard Ruby `Logger` at INFO level for each attempt.
Calling it without the plugin raises a configuration error. To customize
formatting, severity, or use another logger, supply a factory block:

```ruby
logging = River::JobPersistedLogging::Plugin.new(
  max_size_bytes: 256 * 1024,
  max_total_bytes: 1024 * 1024
) do |writer|
  Logger.new(writer, level: :debug,
    formatter: ->(severity, time, _progname, message) {
      JSON.generate(level: severity, time: time.utc.iso8601, message: message) + "\n"
    })
end
```

The factory runs once per attempt and receives a thread-safe, bounded writer
supporting `write` and `close`. Return a new logger that writes to it. Only this
logger's output is captured: the plugin does not replace `Config#logger`,
`Rails.logger`, Active Job's logger, or redirect stdout/stderr. Pass `job.logger`
to application code that should contribute to the job log. Put the plugin
before other work middleware if that middleware also needs `job.logger`.

Logs are saved with the attempt's final state transition, including failures,
timeouts, snoozes, cancellations, and graceful interruption. Entries have the
shape `{"attempt": 1, "log": "..."}` and append across retries. Empty attempts
add nothing. No separate database write is made for each log line. Logs are not
live-streamed; a process crash or failed finalization can lose the current
attempt's buffered logs. Jobs deleted by an ephemeral-job plugin retain no logs.

The default capture limit is 2 MiB per attempt; excess bytes are discarded as
they are written. The default history limit is 8 MiB of serialized JSON, capped
at 64 MiB. Oldest entries are dropped first, but the newest entry is always
retained even if it alone exceeds the history limit. Both settings must be
positive integers. Invalid/incomplete UTF-8 sequences and NUL bytes are removed
so the log is safe to store. Truncation, dropped history, and malformed existing
log metadata are reported through `Config#logger`; malformed history is left
unchanged. Join any child threads before returning from work; writes after
capture closes are ignored. Avoid logging secrets: logs live in job metadata
and follow the job's retention policy.

### Plugins

Plugins provide one ordered configuration point for lifecycle callbacks and
wrapping middleware. These are two distinct extension styles even though both
are registered through `Config#plugins`:

- A **hook** runs at one specific lifecycle point and then returns. Hooks are
  appropriate for observing or making a small change at that point.
- **Middleware** wraps a complete insertion or work operation. It can run code
  before and after the inner operation, and must call `operation.call` to let
  that operation continue.

A plugin may implement any combination of these methods:

| Style      | Method                           | When it runs                                                                                 |
| ---------- | -------------------------------- | -------------------------------------------------------------------------------------------- |
| Hook       | `insert_begin(params)`           | Before each job is inserted; `params` may be modified.                                       |
| Hook       | `insert_end(result)`             | After each job is inserted.                                                                  |
| Hook       | `work_begin(job)`                | After a job is claimed, immediately before its worker runs.                                  |
| Hook       | `work_end(job, error)`           | After the worker returns or raises; `error` is `nil` on success.                             |
| Hook       | `job_finalize(job, state)`       | Before successful finalization; returning `:delete` deletes the job instead of retaining it. |
| Middleware | `insert_many(params, operation)` | Around one insertion call; `params` is an array even for `Client#insert`.                    |
| Middleware | `work(job, operation)`           | Around the work hooks and worker for one claimed job.                                        |

A single plugin can provide both styles. For example, it might use
`insert_begin` to add metadata and `work` to time the complete work operation.
After insertion hooks run, each job must still have an initial state of
`available`, `pending`, or `scheduled`. Invalid states roll back the batch
and any database writes made by its hooks.

```ruby
class TimingPlugin
  def work(job, operation)
    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    operation.call
  ensure
    Metrics.observe(
      job.kind,
      Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
    )
  end
end

config = River::Config.new(plugins: [AuditPlugin.new, TimingPlugin.new])
```

Plugins earlier in the list are the outermost wrappers. Begin callbacks run in
configuration order, while `insert_end` callbacks run in reverse order. In
effect, work execution is nested as: middleware before, `work_begin`, worker,
`work_end`, middleware after.

### [Subscriptions](https://riverqueue.com/docs/subscriptions)

Subscribe to job and queue events for logging or metrics. Subscriptions are
bounded and drop new events rather than blocking workers when their buffer is
full. `subscription.close` unregisters it from the client and wakes waiting
readers; closing more than once is safe. Buffered events remain readable, then
`each` ends and blocking `pop` calls return `nil`. Non-blocking `pop(true)` raises
`ThreadError` whenever no event is available, including after closure.

```ruby
subscription = client.subscribe(
  :job_completed,
  :job_failed,
  buffer_size: 1_000
)

subscription.each { |event| consume(event) }
subscription.close
```

Events include completed, failed, cancelled, snoozed, and interrupted jobs, plus paused and resumed queues.

### Job administration

The client can fetch, filter, update, cancel, retry, and delete jobs. Lists return
a `JobListCursor` in `last_cursor`. Pass it as `after`, preserving the same filters
and ordering, to fetch the next page. Cursors retain both the sort value and ID,
so timestamp ordering handles ties and continues working if the cursor job is
deleted. Null timestamps sort last in either direction. For ID ordering only,
`after_id` is also available as a shortcut with an integer ID.

```ruby
list_options = {
  limit: 100,
  queues: [:bulk],
  sort_by: :scheduled_at,
  states: [:discarded],
  tags_any: ["billing"]
}
page = client.job_list(**list_options)

next_page = client.job_list(**list_options, after: page.last_cursor)
client.job_update job_id, max_attempts: 50
client.job_delete_many states: [:cancelled]
```

Reusable `JobListParams` and `JobUpdateParams` objects are also accepted as
positional arguments, instead of keywords. For updates, omitted fields remain
unchanged; an explicit `nil` clears a nullable field. Metadata must be a Hash;
use `metadata: {}` to clear it. Values inside the Hash may be `nil`.
Updated `max_attempts` must be between 1 and 32,767; `attempt` must be between
0 and 32,767. Claims cap the attempt counter at 32,767 so an administratively
requeued job cannot prevent other jobs from being claimed.

Bulk deletion requires at least one filter and never deletes running jobs.
Metadata filters compare complete JSON values at each supplied top-level key,
including nested objects and arrays. Numbers, strings, and booleans remain
distinct; a null value matches a present JSON null, not a missing key.

### [Leader election](https://riverqueue.com/docs/leader-election)

Running clients coordinate through the canonical `river_leader` table. Only the
current leader performs database-wide scheduling, stuck-job rescue, retention,
and custom maintenance, and another client can take over after its lease
expires.

Set `leader_election_disabled: true` for a worker-only client. It continues
fetching and executing jobs, including queues added after startup, but never
runs maintenance. Another client in the same database/schema must remain
eligible to lead. Periodic jobs cannot be configured or modified on a client
with leader election disabled.

For clients that share a queue but implement different job kinds, set
`fetch_only_known_kinds: true`. Only registered kinds and aliases are claimed;
other jobs remain available without consuming attempts. Register workers before
starting the client. An empty registry fetches nothing. This option affects
fetching only; combine it with `leader_election_disabled` when another client
should own database-wide maintenance. Both options default to `false`.

### [Maintenance services and retention](https://riverqueue.com/docs/maintenance-services)

The maintenance leader promotes scheduled jobs, rescues stuck work, deletes
finalized rows, and runs custom services. Retention is configured in seconds;
use `nil` or `-1` to retain a state indefinitely.

```ruby
config = River::Config.new(
  cancelled_job_retention_period: 86_400,
  completed_job_retention_period: 86_400,
  discarded_job_retention_period: 7 * 86_400,
  maintenance_services: [MyMaintenanceService.new]
)
```

A custom service implements `run(client, driver, now)` and runs only while this client holds leadership.
Service errors are logged without interrupting other services, stuck-job rescue,
or cleanup. A failed service is retried on the next scheduled maintenance pass.

On SQLite, the leader also removes notification outbox entries older than five
minutes in bounded batches. Cancellation requests write Go-compatible control
notifications in the same transaction as the job update; Ruby workers continue
to observe cancellation through polling.

### [Renaming job kinds](https://riverqueue.com/docs/renaming-jobs)

Register old names as aliases while producers migrate to a new kind. All aliases resolve to the same worker:

```ruby
workers = River::Workers.new.add(NewReportWorker, aliases: [:old_report])
```

Keep aliases registered until no jobs with the old kind remain.

Use `workers[:kind]` for a lookup that returns `nil` when missing, or
`workers.fetch(:kind)` to raise `KeyError`. Like `Hash#fetch`, it also accepts
an explicit default (`workers.fetch(:kind, nil)`) or a fallback block.

### [Stopping gracefully](https://riverqueue.com/docs/graceful-shutdown)

For a dedicated foreground worker process with application boot and signal
handling, use the [worker command](./workers.md):

```sh
bundle exec river worker --config config/river.rb --stop-timeout 30
# Rails, from the application root:
RAILS_ENV=production bundle exec river worker --rails
```

The Ruby configuration file must return an unstarted client. The following client
methods are for applications managing their own runtime lifecycle:

`client.stop` stops fetching and waits for active jobs to finish. `client.stop_and_cancel` interrupts active worker threads and returns their jobs to `available` without consuming the interrupted attempt.
While draining, the client continues polling for cancellation requests from
other clients, including for workers with no timeout. Transient polling errors
are retried until the active attempts finish.

Blocking stop calls from the client's own workers or other runtime threads
raise `ThreadError`. A worker can request shutdown with `client.stop(wait: false)`
and then finish its work.

Use `client.stop(wait: false)` to request stop and return immediately without
interrupting active workers. Call `client.stop` later to wait for draining and
finish cleanup. Until that waiting call completes, `started?` remains true and
`stopped?` remains false. In-flight fetches or maintenance operations may finish.

### [Insert-only clients](https://riverqueue.com/docs/insert-only-clients)

A client with no configured queues can insert and administer jobs without starting worker or maintenance threads:

```ruby
client = River::Client.new(driver, config: River::Config.new(queues: {}))
client.insert args
```

## Threads and Ractors

Queue producers, maintenance, and jobs run in threads. River keeps core constants shareable and mutable runtime state per client. Tests exercise insertion, uniqueness, resumable work, events, periodic scheduling, and worker threads inside a non-main Ractor using an in-memory test driver. This is groundwork for Ractor support, not a guarantee that the supplied database drivers work in Ractors.

Load River and worker definitions in the main Ractor before spawning others. Each Ractor must create and own its clients, configuration, worker instances, callbacks, and database connections; do not share live clients or pools across Ractors. Custom argument encoders should prefer `JSON.generate` to `JSON.dump`, which depends on mutable global options.

Database drivers, Active Record, Sequel, and optional dependencies such as Fugit still need their own Ractor compatibility. Job timeouts also depend on Ruby and the `timeout` gem: the test suite exercises them on Ruby 4 with `timeout` 0.6.1; tests on older Rubies disable job timeouts. `WorkerRunner` handles process-wide signals and must run on the main thread of the main Ractor.

Ruby 4.0.2 with `timeout` 0.6.1 can intermittently deadlock during VM shutdown
while terminating Ractors and their timeout helper threads, even after work
finishes successfully. This reproduces without River. Tests bypass that shutdown
path only in their disposable subprocesses, after checking both Ractors' results.
Keep production workers on the thread-based runtime until the Ruby and driver
limitations are resolved.

## RBS and type checking

The gem bundles [RBS files](https://github.com/riverqueue/river/tree/master/ruby/sig) for tools such as [Steep](https://github.com/soutaro/steep) and other RBS-compatible type checkers.

## Drivers

### Active Record

```ruby
require "riverqueue-activerecord"

ActiveRecord::Base.establish_connection("postgres://...")
client = River::Client.new(River::Driver::ActiveRecord.new)
```

### Sequel

```ruby
require "riverqueue-sequel"

DB = Sequel.connect("postgres://...")
client = River::Client.new(River::Driver::Sequel.new(DB))
```

Neither driver installs `pg` or `sqlite3`; the application chooses its adapter.

## Testing

For database-backed insertion assertions and synchronous worker tests, see
[Testing River jobs](./testing.md). Helpers ship in the core gem; RSpec and
Minitest integrations are optional and explicitly loaded.

## River Pro

River Pro is kept in the separate, privately distributed `riverqueue-pro` gem, so possession of that package is the access boundary. It is not included in the MPL-2.0 core gem. Its implementation and documentation live in the private `riverqueue-ruby-pro` repository; see that repository's README for configuration and feature examples.

## Current differences from the Go client

Ruby checks Go-generated fixtures for unique keys, cron, snooze counters, and
shared protocol values.
See [conformance coverage and differences](conformance.md); this is not yet a
full mixed-language runtime suite.

The Ruby client does not currently provide dedicated OpenTelemetry/metrics
integrations or
transactional job completion alongside application writes. Plugins and
subscriptions provide integration points for telemetry. Job execution uses Ruby
worker objects rather than Go's work-function API.

## Development

See [development](./development.md).
