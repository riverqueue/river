# River for Rails

`riverqueue-rails` integrates Active Job with River's existing PostgreSQL/SQLite
runtime. It supports Rails 7.2, 8.0, and 8.1 and lives separately from the
Rails-independent core gem. Database gems remain application-selected.

## Install

```ruby
gem "riverqueue-rails"
gem "pg" # Or sqlite3.
```

```sh
bin/rails generate river:install
bin/rails river:migrate
bin/jobs start
```

The generator installs `config/initializers/river.rb` and `bin/jobs`. Migration
tasks use the canonical bundled Go SQL through `River::Migrator`; no separate
Rails schema is created. `bin/rails river:status` shows migration status.
Migrate explicitly during deployment, not on every application boot.

## Configuration

```ruby
Rails.application.configure do
  config.active_job.queue_adapter = :river unless Rails.env.test?
  config.river.stop_timeout = 30

  config.river.configure do
    River::Config.new(
      job_timeout: 300,
      logger: Rails.logger,
      queues: {default: 10, mailers: 5}
    )
  end
end
```

The block runs lazily after application boot and again when constructing a
consumer. It must return a fresh `River::Config`. The integration registers the
reserved `active_job` worker and installs Rails execution wrapping automatically.
Web processes only insert; `river worker --rails` or `bin/jobs start` starts consumers. Configure all
actual queue names, including Active Job queue prefixes and Action Mailer queues.
Counts are per queue, per process. Size the database pool for application work,
producers, and maintenance as well as consumer threads.

Clients are rebuilt after a fork; Rails/the process server remains responsible
for its connection-pool fork lifecycle. Do not share clients across Ractors.

## Dedicated worker process

From the Rails application root:

```sh
RAILS_ENV=production bundle exec river worker --rails
# Override config.river.stop_timeout for this process:
RAILS_ENV=production bundle exec river worker --rails --stop-timeout 60
```

The command boots Rails and builds a consumer using the configuration above.
Do not start it in a web initializer. Migrate before starting consumers, and use
an external supervisor for process counts and restarts.

TERM/INT request stop and drain active attempts. At the graceful deadline,
the runner interrupts attempts and allows five seconds for finalization before
forcing exit. TSTP requests `stop(wait: false)` but keeps the process alive until
a later TERM/INT. Successfully finalized interruptions make jobs available again;
forced termination may leave jobs for stuck-job recovery. Give the supervisor a
termination window longer than the grace period plus finalization and a margin.

See [Dedicated worker processes](../../docs/workers.md) for signal behavior,
exit statuses, pool sizing, and rolling replacements. The generated `bin/jobs
start` delegates to the same runner; CLI options belong to `river worker`.

## Jobs and mail

```ruby
class FulfillOrderJob < ApplicationJob
  queue_as :default
  retry_on PaymentGateway::Unavailable, attempts: 5, wait: 30.seconds
  discard_on ActiveJob::DeserializationError

  def perform(order)
    FulfillOrder.call(order)
  end
end

FulfillOrderJob.perform_later order
FulfillOrderJob.set(wait: 10.minutes).perform_later order
OrderMailer.receipt(order).deliver_later
```

Active Job handles serialization, GlobalID, callbacks, locale, timezone, and
custom serializers. Bulk `ActiveJob.perform_all_later` uses River's atomic bulk
insertion. Priorities must be `nil` (River priority 1) or integers 1 through 4.
Unsupported priorities raise instead of silently changing meaning.

## Transactions

The default connection class is `ActiveRecord::Base`. Select a different abstract
Active Record class for producers, consumers, and migration tasks together:

```ruby
Rails.application.configure do
  config.river.connection_class = "ApplicationRecord"
end
```

Prefer a class name in Rails initializers: it is resolved lazily after boot and
the insertion client is rebuilt if Rails reloads the class. An actual class
object is also accepted. Configure its database through ordinary Active Record
`establish_connection` or `connects_to` configuration. Restart consumers after
changing connection configuration.

Without the Rails integration, select the class directly on the driver:

```ruby
River::Driver::ActiveRecord.new(connection_class: ApplicationRecord)
```

Insert jobs in the selected connection's transaction for atomicity. Sharing a
database URL alone does not provide atomicity across connections. A dedicated
queue connection class/database is supported, but does not commit atomically
with application writes on another connection.

Shard routing and worker management remain explicit. Inserts follow the selected
class's current Active Record role/shard; consumers need their own appropriate
connection configuration. A request's `connected_to` block is not propagated to
worker threads. Use the same database backend/schema layout across roles/shards
accessed by a driver; no automatic shard discovery or cross-shard transactions
are provided.

For Rails 8.x, disable after-commit deferral explicitly when relying on atomicity:

```ruby
class ApplicationJob < ActiveJob::Base
  self.enqueue_after_transaction_commit = false
end

ApplicationRecord.transaction do
  order = Order.create!(status: "pending")
  FulfillOrderJob.perform_later(order)
end
```

The Rails 7.2 adapter default is immediate insertion; its explicit policies use
`:never` and `:always` instead of Rails 8's `false` and `true`. Explicit application
after-commit policies are respected on every supported version, but lose the
atomic-write guarantee. A returned provider ID does not prove an outer transaction
has committed. Job completion and business writes are not automatically atomic.

## Retries and observability

Active Job owns application retries. `retry_on` creates a new River row with the
same Active Job UUID, while `provider_job_id` becomes the new River row ID string.
Unhandled errors, including exhausted `retry_on`, discard the current row without
an additional River retry cycle. Native River workers retain their normal policy.
The 25-attempt River budget is retained for crash recovery; stuck claims are
rescued by normal River maintenance (currently after an hour).

River interruption, cancellation, and snooze exceptions bypass Active Job's
retry/discard handlers, including `retry_on StandardError`. Avoid swallowing
these exceptions in broad Ruby `rescue` blocks inside application code.

Handled retries/discards complete the current delivery row. Its metadata records
`active_job_outcome` (`retried` or `discarded`), and retried deliveries record
`active_job_retry_id`. A completed delivery need not mean the logical job succeeded.
Active Job logging and notifications remain available. Rows store a versioned
Active Job envelope under the reserved `active_job` kind; Go can share the schema,
but Ruby must execute these jobs. Use native River kinds for cross-language jobs.

Retry enqueueing and predecessor finalization are not atomic. Crashes can cause
duplicates; jobs must remain idempotent. Rails' executor/reloader wraps each work
attempt and periodic constructor, cleaning context and connections. Active Job
classes are resolved per execution. Restart workers when changing native River
worker/plugin registrations or periodic configuration; these retain Ruby objects.

## Testing

Keep Rails' `:test` adapter and `ActiveJob::TestHelper` for ordinary application
tests. Use `:river` and a migrated isolated database for persistence/runtime
integration tests. Disable transactional fixtures for threaded worker tests.
The repository's `make test` includes this gem's PostgreSQL and SQLite tests.
`RIVER_REQUIRE_DATABASES=1 make test` makes missing PostgreSQL an error.

Outside a Rails app, instantiate `ActiveJob::QueueAdapters::RiverAdapter.new(client:)`
and register `River::Rails::Worker` on your consuming River client. Rails-specific
boot, configuration, and execution wrapping are then your responsibility.
