# River for Ruby

River is a fast, reliable background job system backed by Postgres or SQLite. The Ruby client inserts and works jobs using the same database schema and job protocol as River's other languages. It includes Active Record and Sequel drivers, plus Rails and Active Job integration.

## Installation

Ruby 3.2 or newer is required. Add a River driver and its database adapter to your application's `Gemfile`; the driver brings in the core `riverqueue` gem:

```ruby
gem "riverqueue-sequel"
gem "pg"
```

Use `riverqueue-activerecord` for Active Record, or `sqlite3` instead of `pg` for SQLite. Install the gems with `bundle install`. Rails applications can use [`riverqueue-rails`](rails/riverqueue-rails/README.md) to configure River as their Active Job backend.

## Quick start

Set `DATABASE_URL` to an existing application database and apply River's migrations before inserting or working jobs:

```sh
export DATABASE_URL=postgres://localhost/my_app
bundle exec river migrate-up
```

Define job arguments and a worker with the same stable kind, register the worker, and insert a job. Save this as `worker.rb`:

```ruby
require "riverqueue-sequel"

class SendEmailArgs
  def initialize(address:)
    @address = address
  end

  def kind = "send_email"
  def to_json = JSON.generate(address: @address)
end

class SendEmailWorker
  def self.kind = "send_email"

  def work(job)
    puts "Sending email to #{job.args.fetch("address")}"
    job.output = {delivered: true}
  end
end

db = Sequel.connect(ENV.fetch("DATABASE_URL"), max_connections: 12)
client = River::Client.new(
  River::Driver::Sequel.new(db),
  config: River::Config.new(
    queues: {default: 10},
    workers: River::Workers.new.add(SendEmailWorker)
  )
)

begin
  client.start
  result = client.insert(SendEmailArgs.new(address: "person@example.com"))
  puts "Inserted job #{result.job.id}; press Ctrl-C to stop"
  sleep
rescue Interrupt
  # Stop fetching and wait for active jobs before closing the database pool.
ensure
  client.stop
  db.disconnect
end
```

Run `bundle exec ruby worker.rb`. Replace the worker body with your mail service. Job arguments implement `kind` and `to_json`; workers read decoded JSON keys as strings. Services sharing a job must agree on its kind and JSON fields. For SQLite and alternate Postgres schemas, see [migrations](docs/migrations.md).

A client without configured queues can insert jobs without starting worker threads. Applications own their database pools and should close them only after stopping River.

## Transactions and other features

Inserts join a transaction opened on the same Sequel database or Active Record connection, so jobs commit or roll back with the application writes that caused them. See [transactional enqueueing](docs/README.md#transactional-enqueueing) for examples.

- [Bulk insertion](docs/README.md#bulk-insertion), [scheduled jobs](docs/README.md#scheduled-jobs), and [unique jobs](docs/README.md#unique-jobs).
- [Retries](docs/README.md#job-retries), [cancellation](docs/README.md#cancelling-jobs), and [snoozing](docs/README.md#snoozing-jobs).
- [Multiple queues](docs/README.md#multiple-queues), [periodic jobs](docs/README.md#periodic-jobs), and [resumable jobs](docs/README.md#resumable-jobs).
- [Event subscriptions](docs/README.md#subscriptions) and [job administration](docs/README.md#job-administration).
- [Graceful stopping](docs/README.md#stopping-gracefully) and a foreground [worker command](docs/workers.md).
- [Testing helpers](docs/testing.md) for application jobs and workers.

## Documentation

See the [usage and configuration guide](docs/README.md), [migration guide](docs/migrations.md), and [Rails and Active Job integration](rails/riverqueue-rails/README.md). The [Sidekiq migration guide](docs/migrating_from_sidekiq.md) covers moving an existing application.

## Development

See [developing River for Ruby](docs/development.md).
