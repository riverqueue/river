# Migrating from Sidekiq to River

This guide migrates a Ruby application from Sidekiq to the Ruby River client in
this repository. It covers producers, workers, configuration, tests, and queued
work. It targets the current repository API, including worker-runtime features.
Pin and verify a release or repository revision containing these APIs
before changing an application; do not assume an older installed gem supports
them. Sidekiq references were checked on September 7, 2026.

River stores jobs in PostgreSQL or SQLite and can insert them in the same
transaction as application records. Workers run in Ruby threads. Redis payloads
are not River rows: changing gems alone does not move existing jobs.

## Why migrate to River?

For applications already using PostgreSQL or SQLite, River offers:

- **Atomic enqueueing:** application changes and jobs commit or roll back
  together in one database transaction. Sidekiq's
  [transactional push](https://github.com/sidekiq/sidekiq/wiki/Advanced-Options#transactional-push)
  waits until commit, but the subsequent Redis write is separate. River removes
  that gap. See [transactional enqueueing](https://riverqueue.com/docs/transactional-enqueueing).
- **Less infrastructure:** reuse your application database instead of operating
  Redis for the job queue. Redis may still be needed for unrelated features.
- **SQL visibility:** inspect job arguments, attempts, errors, and retained
  results with ordinary SQL, and correlate them with application records.
- **More built into the core client:** unique jobs, periodic intervals, snoozing
  without consuming attempts, resumable checkpoints, and recorded output reduce
  the need for application glue or extra extensions. See the
  [Ruby feature guide](README.md#core-features).
- **A path between Ruby and Go:** clients can share River's schema and exchange
  jobs using compatible kinds and JSON arguments, allowing workers to move
  between languages incrementally.

This is not a claim of higher throughput or exactly-once execution: jobs still
need idempotent behavior, and queue traffic adds database load. Check the
compatibility gaps below before migrating.

## Instructions for an automated migration

Work through the numbered sections in order. Use the Ruby source linked below
as the API authority; Go documentation describes concepts but its method names
are not Ruby methods. Code examples use application-owned names such as
`FulfillOrder` and `AppJobs`; create or adapt them explicitly.

Before editing, produce a migration inventory with one row per job class:

| Existing class | Argument schema | Queue | Retry policy | Producers | Middleware/context | Destination kind | Cutover policy |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `FulfillOrderJob` | `order_id` integer | `orders` | 5 retries | Checkout service | Rails executor | `fulfill_order` | Drain old jobs |

Classify every item as **direct rewrite**, **semantic change**, **requires River
Pro**, or **application implementation required**. Record unresolved behavior
instead of inventing a River API. The separate `riverqueue-rails` gem provides
an Active Job adapter and worker entry point (see section 8). Core River has no
Sidekiq compatibility module, `perform_async`, Sidekiq-style YAML loader, or
fake/inline testing mode. The native-worker examples below do not require the
Rails integration gem.

Keep the existing system runnable during migration. Separate code conversion
from the production cutover and from any Redis data transfer.

## 1. Inventory the application

Search source, tests, initializers, scripts, deployment manifests, and dependencies:

```sh
rg -n 'Sidekiq|sidekiq|perform_async|perform_in\b|perform_at\b|perform_bulk|push_bulk' .
rg -n 'perform_later|deliver_later|queue_adapter|retry_on|discard_on|queue_as' .
rg -n 'unique_for|unique_until|sidekiq_options|sidekiq_retry_in|sidekiq_retries_exhausted' .
rg -n 'sidekiq-cron|sidekiq-scheduler|sidekiq-unique-jobs|REDIS_URL' .
```

Also inventory scheduled, retrying, dead, and in-flight jobs; dynamically chosen
queues/classes; cron schedules and time zones; batch callbacks; rate limits;
tenant/locale/tracing context; and code storing Sidekiq JIDs. Include producers
outside the main repository. Determine whether Redis serves other application
features before removing its infrastructure.

[Sidekiq's feature list](https://sidekiq.org/) distinguishes OSS, Pro, and
Enterprise. Record which features the application actually uses, including
third-party extensions, rather than mapping the purchased edition as a whole.

## 2. Install a driver and provision the schema

For an Active Record application using PostgreSQL:

```ruby
# Gemfile: keep Sidekiq while it drains existing work.
gem "riverqueue-activerecord"
gem "pg"
```

For Sequel, use `riverqueue-sequel`. For SQLite, use `sqlite3` instead of `pg`.
Each driver depends on `riverqueue` but leaves database gem selection to the
application. Use the same database as the business records when atomic enqueueing
is required.

Apply the bundled [canonical River migrations](migrations.md) without installing Go:

```sh
bundle exec river migrate-up --database-url postgres://localhost/my_app
```

Use a gem version containing the required migrations. Provision
both development and isolated test databases before running the examples.
Do not copy `spec/support` schema fixtures into production or recreate River
tables through Rails models. Pro additionally requires its canonical migration
line and separately distributed gem.

## 3. Convert a job and its producers

Sidekiq invokes `perform` with positional JSON arguments. River resolves an
explicit kind through a worker registry and passes one `River::Job` to `work`.
See [Sidekiq Getting Started](https://github.com/sidekiq/sidekiq/wiki/Getting-Started)
and River's [worker implementation](../lib/worker.rb).

Before:

```ruby
class FulfillOrderJob
  include Sidekiq::Job
  sidekiq_options queue: "orders", retry: 5

  def perform(order_id)
    FulfillOrder.call(order_id)
  end
end

jid = FulfillOrderJob.perform_async(42)
```

After, put each class in a correspondingly named file loaded by your application:

```ruby
class FulfillOrderArgs
  def initialize(order_id:)
    @order_id = order_id
  end

  def kind = "fulfill_order"
  def to_json = JSON.generate(order_id: @order_id)

  def insert_opts
    River::InsertOpts.new(max_attempts: 6, queue: :orders)
  end
end

class FulfillOrderWorker
  def self.kind = "fulfill_order"

  def work(job)
    FulfillOrder.call(job.args.fetch("order_id"))
  end
end
```

Keep `FulfillOrder.call` as the application's idempotent business operation.
Register the worker class, not an instance, when you want a new instance for each
attempt. Registered instances and plugin instances are shared across threads.

Create an insertion client in the web process after its database connection is
configured. This Rails initializer defines an application-owned accessor:

```ruby
# config/initializers/river.rb
require "riverqueue-activerecord"

module AppJobs
  def self.client
    @client ||= River::Client.new(River::Driver::ActiveRecord.new)
  end
end
```

This accessor is for a normal web-process boot; initialize clients after forking
and avoid sharing clients or pools between processes or Ractors. Worker processes
use the separately configured client in section 5.

```ruby
result = AppJobs.client.insert(FulfillOrderArgs.new(order_id: 42))
job_id = result.job.id
```

The return value is `River::JobInsertResult`, and `job.id` is a database integer,
not a Sidekiq JID string. Update API contracts, stored references, cancellation
endpoints, and logs accordingly. For a simpler producer without an argument class:

```ruby
result = AppJobs.client.insert(
  River::JobArgsHash.new(:fulfill_order, {order_id: 42}),
  insert_opts: River::InsertOpts.new(max_attempts: 6, queue: :orders)
)
```

`JobArgsHash` does not inherit `FulfillOrderArgs#insert_opts`. Supply the intended
options whenever using it. Worker classes do not supply insertion defaults.

Use string keys when reading decoded JSON. Explicitly translate each old
positional argument to a named field; do not put the complete Sidekiq envelope
under River `args`. Keep IDs and simple JSON values rather than model instances,
GlobalID wrappers, or arbitrary Ruby objects. Preserve idempotency: both systems
can execute work more than once after failures. See
[Sidekiq Best Practices](https://github.com/sidekiq/sidekiq/wiki/Best-Practices).

## 4. Translate enqueueing and transaction boundaries

| Sidekiq operation | Ruby River operation |
| --- | --- |
| `perform_async(...)` | `client.insert(args)` |
| `perform_in(seconds, ...)` | `InsertOpts.new(scheduled_at: Time.now.utc + seconds)` |
| `perform_at(time, ...)` | `InsertOpts.new(scheduled_at: time.getutc)`; convert numeric epochs with `Time.at` |
| `.set(queue: ...).perform_async(...)` | `client.insert(args, insert_opts: River::InsertOpts.new(queue: ...))` |
| `perform_bulk` / `push_bulk` | `client.insert_many` with args or `River::InsertManyParams` |
| Enqueue children inside `perform` | `job.client.insert(...)` inside `work` |

```ruby
client = AppJobs.client
client.insert(
  FulfillOrderArgs.new(order_id: 42),
  insert_opts: River::InsertOpts.new(scheduled_at: Time.now.utc + 300)
)

results = client.insert_many([42, 43].map do |order_id|
  River::InsertManyParams.new(
    FulfillOrderArgs.new(order_id: order_id),
    insert_opts: River::InsertOpts.new(queue: :orders)
  )
end)
job_ids = results.map { |result| result.job.id }
```

Scheduled jobs need a running maintenance leader and a consumer for their queue.
Scheduling sets an earliest execution time, not an exact deadline. Chunk very
large bulk imports; each call is atomic, but several calls are not one transaction
unless explicitly wrapped.

Put application writes and River insertion in the same transaction:

```ruby
ActiveRecord::Base.transaction do
  order = Order.create!(status: "pending")
  AppJobs.client.insert FulfillOrderArgs.new(order_id: order.id)
end
```

Sequel's equivalent is `DB.transaction` with a River driver constructed from
that same `DB`. Sharing a URL alone is insufficient: the insertion must use the
same transaction connection. Audit multi-database and sharded applications
explicitly. Do not move enqueueing into `after_commit` when the intent is one
atomic write. A remote service call still cannot join this transaction, and this
Ruby client does not atomically commit business writes with job completion.

## 5. Start and stop a worker process

Sidekiq queue weights and strict queue ordering do not translate into River queue
worker counts. River allocates concurrent work separately to each queue; priority
`1` through `4` orders jobs within a queue. Ten workers on each of three queues
permits thirty concurrent jobs per client. Multiple processes multiply that
capacity. See [Sidekiq Advanced Options](https://github.com/sidekiq/sidekiq/wiki/Advanced-Options).

Use `river worker` for a dedicated process with application boot, signal handling,
and a stop deadline. With `riverqueue-rails`, configure queues and native
workers in `config.river.configure` (see section 8), then run:

```sh
RAILS_ENV=production bundle exec river worker --rails --stop-timeout 30
```

Alternatively, this core-gem configuration uses one queue. Place the worker and args
classes above in eager-loaded application paths. The plugin wraps application
work in the [Rails executor](https://guides.rubyonrails.org/threading_and_code_execution.html)
when using the core gem directly. The optional `riverqueue-rails` package supplies
this execution wrapping and a worker entry point automatically (see section 8).
Use this configuration with eager-loaded code; development reload support requires
separate integration.

```ruby
# config/river.rb
# frozen_string_literal: true

require_relative "../config/environment"
require "riverqueue-activerecord"
Rails.application.eager_load!

class RailsExecutionPlugin
  def work(_job, operation)
    Rails.application.executor.wrap { operation.call }
  end
end

River::Client.new(
  River::Driver::ActiveRecord.new,
  config: River::Config.new(
    job_timeout: 300,
    logger: Rails.logger,
    plugins: [RailsExecutionPlugin.new],
    queues: {orders: 10},
    workers: River::Workers.new.add(FulfillOrderWorker)
  )
)
```

Run it under your process supervisor:

```sh
RAILS_ENV=production bundle exec river worker --config config/river.rb --stop-timeout 30
```

The file must return an unstarted client; the command starts it and keeps the
main thread alive. TERM/INT stop fetching and drain active attempts. TSTP requests
`stop(wait: false)` without exiting; send TERM afterward to finish stop.
The graceful deadline triggers interruption, followed by five seconds for
finalization before forced exit. Allow a longer termination window in your
supervisor. See [Dedicated worker processes](./workers.md) for exit statuses,
recovery, and rolling replacements. Do not keep your old application-owned signal
loop when adopting this command.

Budget database connections for workers, queue producers, maintenance, and any
web traffic sharing a pool. Load-test the intended process count and concurrency;
do not set pool size equal to job concurrency without allowing overhead. Monitor
database contention, particularly when using SQLite. Create clients after any
prefork step. Starting a client in every web initializer starts consumers in
every web process; use insert-only clients there unless that is intentional.

## 6. Translate retries, cancellation, and timeouts

Sidekiq's integer `retry: n` counts retries after the first execution. River's
`max_attempts` counts all attempts. For fresh jobs, use `n + 1`; Sidekiq's default
25 retries corresponds to 26 attempts, while River defaults to 25 attempts.
Retry schedules also differ. See
[Sidekiq Error Handling](https://github.com/sidekiq/sidekiq/wiki/Error-Handling).

| Existing behavior | Migration decision |
| --- | --- |
| `retry: 5` | `max_attempts: 6` on insertion |
| `retry: 0` or `retry: false` | `max_attempts: 1` prevents retries; River retains a failed job as `discarded`, so deletion/dead-set behavior needs a separate decision |
| `sidekiq_retry_in` | Implement `next_retry(job, error)` returning an absolute `Time`, not delay seconds |
| `sidekiq_retries_exhausted`, death handlers | Implement durable terminal-failure handling in application code; there is no matching callback registration API |
| Permanent cancellation | Raise `River.job_cancel(reason)` from work |
| Dependency not ready | Raise `River.job_snooze(seconds)` to reschedule without consuming an attempt |

For a fixed retry delay:

```ruby
class FulfillOrderWorker
  def next_retry(_job, _error)
    Time.now.utc + 60
  end
end
```

The runtime records raised errors. Do not rescue an error and silently return if
the job should retry: a normal return means success. River's default timeout is
60 seconds. Review existing job durations explicitly; configure `job_timeout`,
or implement `timeout(job)` returning seconds, `nil` to disable, or `0` to inherit
the client default. Use network-client timeouts as well.

An error reporter must avoid accidentally requesting cancellation:

```ruby
error_handler = lambda do |error, job|
  ErrorReporter.capture(error, job_id: job.id, kind: job.kind)
  nil # Returning true or :cancel tells River to cancel the job.
end
config = River::Config.new(error_handler: error_handler)
```

Core River keeps discarded jobs in `river_job`; configure retention deliberately.
Defaults are one day for completed/cancelled jobs and seven days for discarded
jobs. `nil` or `-1` retains a state indefinitely. River Pro dead-letter storage
is separate and requires Pro configuration. Neither retention model should be
assumed equivalent to the Sidekiq Dead set.

## 7. Replace middleware and execution context

Sidekiq has separate client and server middleware chains. River accepts plugin
instances through `Config.new(plugins: [...])`; see
[Sidekiq Middleware](https://github.com/sidekiq/sidekiq/wiki/Middleware) and
[River's plugin signatures](README.md#plugins).

| Purpose | River plugin method |
| --- | --- |
| Modify each insertion's metadata | Hook: `insert_begin(params)` |
| Observe each insertion result | Hook: `insert_end(result)` |
| Wrap insertion | Middleware: `insert_many(params, operation)` |
| Restore context before work | Hook: `work_begin(job)` |
| Observe return/error | Hook: `work_end(job, error)` |
| Wrap work and clean up context | Middleware: `work(job, operation)` |

```ruby
class LocalePlugin
  def insert_begin(params)
    params.metadata["locale"] = I18n.locale.to_s
  end

  def work(job, operation)
    I18n.with_locale(job.metadata.fetch("locale", I18n.default_locale)) do
      operation.call
    end
  end
end
```

Register insertion plugins on every producer, including clients used by workers
to enqueue children. Register work plugins on consumers. Earlier plugins wrap
later plugins; for Rails, put `RailsExecutionPlugin` before application context
plugins. Keep attempt-specific context out of shared instance variables and
restore thread-local context even on exceptions.

Middleware must call `operation.call` to continue. Do not port a Sidekiq
middleware veto by returning early from River work middleware: normal return can
mark a job completed without executing its worker. Use explicit cancellation or
application validation. `insert_end` is not an after-commit notification; it can
run inside an outer transaction that later rolls back.

Subscriptions are useful for local telemetry, but events are bounded, may be
dropped, and are not a durable cross-process callback system. Use persisted
application records or workflow tasks for essential follow-up actions.

## 8. Handle Active Job and Action Mailer explicitly

An Active Record driver alone is not an Active Job adapter. To keep existing
Active Job and Action Mailer jobs, install `riverqueue-rails` and run:

```sh
bin/rails generate river:install
bin/rails river:migrate
bin/jobs start
```

The installer configures `config.active_job.queue_adapter = :river`. Existing
`perform_later` and `deliver_later` then use River. Review the generated queue
configuration and the [Rails integration guide](../rails/riverqueue-rails/README.md)
before cutover. In particular, Active Job owns application retries: unhandled
exceptions discard a River delivery without an additional backend retry cycle.
Disable after-commit deferral if relying on same-connection atomic enqueueing.
The integration preserves Active Job serialization, callbacks, GlobalID, locale,
and execution context; it does not import existing Redis jobs.

Alternatively, keep Active Job on its existing backend during migration, or
extract its business operation into a native River worker. Translate callbacks,
`retry_on`, `discard_on`, GlobalID deserialization, locale, and queue naming
deliberately. For email, enqueue a native River job containing recipient/model
IDs and call `deliver_now` from that worker. Calling `deliver_later` there would
enqueue another Active Job rather than finish the email in River.

## 9. Map recurring, unique, and commercial features

### Recurring schedules

For an hourly interval, configure a periodic job on the worker client:

```ruby
periodic = River::PeriodicJob.new(
  id: :hourly_order_reconciliation,
  constructor: -> {
    [River::JobArgsHash.new("reconcile_orders", {}),
      River::InsertOpts.new(queue: :orders)]
  },
  schedule: River::PeriodicInterval.new(3600)
)
config = River::Config.new(
  periodic_jobs: [periodic],
  queues: {orders: 10},
  workers: workers # Also register a worker for "reconcile_orders".
)
```

Pass this config to the consuming client. An interval is not a cron expression:
for calendar schedules, add `gem "fugit", "~> 1.13"` and use
`River::PeriodicCron.new("0 9 * * 1-5", timezone: "America/New_York")` as the
`schedule:`. Fugit is optional and loaded only when constructing this helper;
the default timezone is UTC. Custom schedules can still respond to `next(time)`
or be callables returning the next `Time` after the supplied time.
Test daylight-saving transitions and missed runs. Core schedules live in memory;
Pro offers durable scheduling. Configure compatible schedules on clients eligible
for leadership. Disable the old recurring producer when enabling its replacement
so both schedulers do not enqueue the same occurrence.

### Uniqueness

Sidekiq Enterprise `unique_for` is a lock TTL. River `by_period` uses time buckets
derived from scheduling time; it is not a sliding lock TTL. `unique_until: :start`
also has no direct mapping because River requires `running` among custom unique
states. See [Sidekiq uniqueness](https://github.com/sidekiq/sidekiq/wiki/Ent-Unique-Jobs).

For uniqueness while equivalent work remains unfinished:

```ruby
unique = River::UniqueOpts.new(
  by_args: true,
  by_queue: true,
  by_state: %w[available pending running scheduled retryable]
)
result = AppJobs.client.insert(
  FulfillOrderArgs.new(order_id: 42),
  unique_opts: unique
)
duplicate = result.unique_skipped_as_duplicate?
```

This deliberately excludes completed jobs; River's default unique state set
includes them until retention removes them. A duplicate returns the existing
row, which callers must not treat as newly inserted. Uniqueness does not make
external side effects exactly once and does not deduplicate across Redis and SQL.

### Sidekiq Pro batches

Sidekiq batches coordinate a collection of jobs and callbacks. River Pro workflows
are the closer abstraction; `River::Pro::BatchWorker` instead processes multiple
jobs in a single `work_many` invocation. See
[Sidekiq Batches](https://github.com/sidekiq/sidekiq/wiki/Batches).

With the privately distributed `riverqueue-pro` installed and its migrations
applied, a fan-out followed by a success task looks like:

```ruby
require "riverqueue-pro"

# Register "import_row" and "finish_import" workers before starting this client.
pro_client = River::Pro::Client.new(
  River::Driver::ActiveRecord.new,
  config: River::Pro::Config.new(
    core: River::Config.new(queues: {imports: 10}, workers: workers)
  )
)
workflow = River::Pro.workflow name: "import" do |flow|
  tasks = [101, 102].map do |row_id|
    flow.add(
      "row_#{row_id}",
      River::JobArgsHash.new(:import_row, {row_id: row_id}),
      queue: :imports
    )
  end
  flow.add(
    :finish,
    River::JobArgsHash.new(:finish_import, {import_id: 7}),
    after: tasks,
    queue: :imports
  )
end
pro_client.insert_many workflow.jobs
```

A consuming Pro client must be running. By default, cancelled/discarded
dependencies prevent the success task from running. Sidekiq's `complete`
callback means all jobs have run once, which differs from dependency finalization;
its `death` callback also requires explicit redesign. Test retry, cancellation,
dynamic task addition, empty input, and terminal-failure behavior before replacing
a production batch.

### Other feature decisions

| Sidekiq feature or extension | River migration path |
| --- | --- |
| Enterprise rate limiting | Application rate limiter; Pro concurrency limits bound simultaneous jobs, not requests per second |
| Enterprise periodic scheduling | Core periodic jobs or Pro durable periodic jobs; review calendar semantics |
| Enterprise encryption | Pro `EncryptPlugin` with an application encryptor; decode and re-encode old payloads explicitly |
| Expiring jobs | Application deadline check with explicit cancellation; retention and execution timeouts do not expire queued jobs |
| Long-running iteration/checkpoints | Core resumable steps/cursors; checkpoint formats require explicit translation |
| Sequential work | Pro sequences, with explicit grouping and failure policy |
| Web UI and metrics | Deploy River UI separately and integrate plugins/subscriptions; no `Sidekiq::Web` Rack mount replacement in this gem |
| Multi-process supervision, rolling restarts | Application deployment supervisor and River stop policy |

For exact Pro APIs and distribution requirements, see the
README in the private `riverqueue-ruby-pro` repository.
It is a separate package and may not be present in a public checkout. Do not
assume that a Sidekiq commercial feature has identical behavior in River Pro.

## 10. Rewrite tests against persisted behavior

River has no equivalent of Sidekiq's fake/inline harness, including the newer
`Sidekiq.testing!` API. See [Sidekiq Testing](https://github.com/sidekiq/sidekiq/wiki/Testing).
Unit-test business operations directly, then test enqueueing, rollback, and
execution against an isolated, migrated database matching production's adapter.

The core gem includes [database-backed testing helpers](./testing.md), with
optional RSpec and Minitest integrations. These assert new persisted rows (not
existing rows returned by uniqueness) and can execute one real attempt on the
calling thread:

```ruby
require "riverqueue/testing/rspec"

RSpec.configure { |config| config.include River::Testing::RSpec }

expect { enqueue_order(42) }.to insert_job(
  AppJobs.client,
  args: {"order_id" => 42},
  kind: "fulfill_order"
)

row = AppJobs.client.insert(FulfillOrderArgs.new(order_id: 42)).job
result = River::Testing.perform_job(AppJobs.client, row.id)
expect(result).to have_attributes(error: nil, outcome: :completed)
```

Use a stopped client with registered workers and an isolated database. Unlike
threaded execution, synchronous helpers can see jobs in the caller's transaction
when the driver uses that same connection. They bypass queue capacity/pause and
do not run maintenance or periodic producers. Keep threaded smoke tests for
those runtime behaviors.

An RSpec insertion test, using the application accessor from section 3:

```ruby
it "enqueues the expected payload and options" do
  result = AppJobs.client.insert(FulfillOrderArgs.new(order_id: 42))
  row = AppJobs.client.job_get(result.job.id)
  expect(row).to have_attributes(
    args: {"order_id" => 42},
    kind: "fulfill_order",
    max_attempts: 6,
    queue: "orders"
  )
end
```

An execution smoke test that exercises the real runtime without application
business dependencies:

```ruby
class MigrationProbeWorker
  def self.kind = "migration_probe"
  def work(job) = job.output = {seen: job.args.fetch("value")}
end

it "works a committed job" do
  client = River::Client.new(
    River::Driver::ActiveRecord.new,
    config: River::Config.new(
      queues: {migration_test: 1},
      workers: River::Workers.new.add(MigrationProbeWorker)
    )
  )
  result = client.insert(
    River::JobArgsHash.new(:migration_probe, {value: 42}),
    insert_opts: River::InsertOpts.new(queue: :migration_test)
  )
  begin
    client.start
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
    loop do
      row = client.job_get(result.job.id)
      break if row.state == River::JOB_STATE_COMPLETED
      raise "job did not complete: #{row.state}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
      sleep(0.01)
    end
    expect(client.job_get(result.job.id).metadata.fetch("output")).to eq("seen" => 42)
  ensure
    client.stop_and_cancel
  end
end
```

Disable transactional fixtures for threaded execution tests: other connections
cannot see an uncommitted insertion. Clean up only the isolated test database
after stopping clients. For SQLite, use a temporary file shared by pool
connections rather than separate per-connection in-memory databases.

Also assert transaction rollback removes the job; failures consume the intended
attempt budget; snoozes do not; schedules do not run early; queue isolation and
uniqueness work; context resets on errors; and stop allows recovery. Test
business idempotency by invoking the same logical operation twice. A direct
`worker.work(job)` unit test does not verify runtime retry or finalization.

## 11. Cut over existing work and preserve rollback

Prefer draining Sidekiq while routing new logical work to River, one job kind at
a time. Both consumers may run during the transition, but each enqueue decision
must choose one backend. Keep legacy classes available for old Redis payloads.
If strict ordering matters, finish the old stream before enabling the new one.

1. Deploy schema, River consumers, converted workers, and a producer routing
   switch that initially selects Sidekiq.
2. Verify River with a small canary workload, then switch selected producers.
   Include worker-created children and recurring producers in that switch.
3. Drain old ready and in-flight jobs. Account separately for scheduled jobs,
   retries, dead jobs, and commercial batch state; an empty ready queue is not
   proof that Redis no longer contains relevant work.
4. Keep Sidekiq consumers and necessary schedules available until their assigned
   work is finished or explicitly transferred. Track counts and failures by
   backend and job kind.
5. Remove Sidekiq dependencies, routes, initializers, deployment processes, and
   Redis-only job infrastructure only after that inventory is reconciled.

If long-lived scheduled/retry work must be transferred, build a separate,
restartable importer using the [Sidekiq public API](https://github.com/sidekiq/sidekiq/wiki/API)
and `River::Client#insert`. There is no atomic transaction spanning Redis and SQL.
Use this transfer protocol:

1. Stop producers, schedulers, and consumers that can mutate the selected source
   jobs, and reconcile in-flight work. Export a stable inventory before deleting
   anything. API enumeration of a live queue can race with mutations.
2. Map each allowlisted Sidekiq class to an explicit River kind, argument
   transformation, queue, and attempt policy. Reject unknown classes and wrapped
   Active Job, encrypted, or batch payloads until a dedicated conversion exists.
3. In one SQL transaction, insert the River job and an application-owned transfer
   receipt protected by a unique constraint on source identity (include Redis
   namespace/cluster plus JID). Preserve the JID in metadata for correlation.
   Keep receipts independently of River job retention.
4. Commit SQL before acknowledging/removing that exact Redis job. On restart,
   consult the receipt instead of enqueueing it again. Handle uniqueness results
   explicitly; a returned existing River job is not automatically proof that the
   intended source job was transferred.
5. Verify receipts against both source inventory and destination jobs, including
   source jobs removed after the commit. Do not blindly replay the export.

Preserve future execution times, and make a deliberate choice about remaining
attempts for retries; do not apply the fresh-job `n + 1` rule to an already
partially executed job. Archive dead jobs or move them into a reviewed manual
retry process rather than making every dead job immediately runnable. Do not
write Sidekiq error histories directly into River's schema. In-flight commercial
batches generally need to finish in Sidekiq or be rebuilt as reviewed workflows.

For rollback, switch new producers back to Sidekiq while keeping a River consumer
available for work already committed there, or explicitly pause that work and
plan its recovery. Re-enqueueing all River jobs in Redis can duplicate completed
side effects. Keep shared business idempotency keys stable across both backends.

## Completion criteria

The migration is complete when every inventory row has a verified destination
and behavior, producer paths choose the intended backend, the deployed consumers
cover all target queues and kinds, integration tests pass on the production
adapter, and old queued/scheduled/retrying work is reconciled. Confirm retry
budgets, timeouts, retention, pool capacity, recurring schedules, and stop
under representative load before removing the old system.

For further Ruby API details, use the [main guide](README.md),
[client](../lib/client.rb), [configuration](../lib/config.rb),
[insertion options](../lib/insert_opts.rb), and [runtime](../lib/client_runtime.rb).
