# Dedicated worker processes

Run River consumers separately from web processes with `river worker`. The
command owns one foreground process: it boots your application, starts a client,
handles stop signals, and waits for workers. It does not daemonize, fork
children, run migrations, or supervise replacement processes.

## Plain Ruby

Install `riverqueue`, your driver gem, and the database gem that driver needs.
Apply [migrations](./migrations.md) before starting consumers. Create a Ruby
configuration file whose **last expression returns an unstarted client**:

```ruby
# config/river.rb
require_relative "../app" # Boots your application and defines SortWorker.
require "riverqueue-sequel"

River::Client.new(
  River::Driver::Sequel.new(
    Sequel.connect(ENV.fetch("DATABASE_URL"), max_connections: 20)
  ),
  config: River::Config.new(
    queues: {ruby: 10},
    workers: River::Workers.new.add(SortWorker)
  )
)
```

Run from your application's root directory:

```sh
bundle exec river worker --config config/river.rb
```

Do not call `.start` in the configuration file or install your own signal
handlers. Configure at least one queue and register its workers. Producers must
insert into the same database/schema and queue names. Ten workers means up to
ten concurrent job threads in this process, not ten OS processes.

The configuration file is evaluated as Ruby, not parsed as data; use only trusted
application code. It can return an ActiveRecord-backed client or a
`River::Pro::Client` instead. Pro must be installed and required by the application;
the command does not load it automatically. Pool sizing is application-specific:
allow connections for queue producers, maintenance, and application database work
as well as worker threads. The example's pool size is illustrative.

## Rails

Install and configure [riverqueue-rails](../rails/riverqueue-rails/README.md), then
run from the Rails application root:

```sh
RAILS_ENV=production bundle exec river worker --rails
```

This boots `config/environment.rb` and builds a consumer from
`Rails.application.config.river`, including the configured connection class,
Active Job worker, and Rails execution wrapping. Configure native workers through
the same integration when needed. Keep web processes insertion-only; do not start
a consumer in a web initializer.

The generated `bin/jobs start` also invokes the shared runner. Use the `river`
command for the CLI options shown here. Do not combine `--rails` with `--config`.

## Stopping

```sh
bundle exec river worker --config config/river.rb --stop-timeout 60
```

The graceful stop timeout defaults to 30 seconds for plain Ruby. Rails uses
`config.river.stop_timeout`; `--stop-timeout` overrides it. The value must
be finite and nonnegative; zero skips the grace period. It is separate from the
per-job execution timeout.

| Signal / event        | Behavior                                                                                 |
|-----------------------|------------------------------------------------------------------------------------------|
| `SIGTSTP`             | Calls `client.stop(wait: false)`: stops accepting work but keeps the process alive.         |
| `SIGTERM` / `SIGINT`   | Requests a stop, waits for active attempts, then exits.                                    |
| Another stop signal   | Skips the remaining grace period and interrupts active attempts.                          |
| Grace period expires  | Interrupts active attempts and allows five seconds for finalization.                      |
| Finalization expires  | Forces process exit with status 1, bypassing `at_exit` handlers.                           |

After interruption, a further stop signal can force exit before the finalization
window ends. In-flight fetches or maintenance operations may finish after stop
is requested. `SIGTSTP` does not pause queues in the database, does not start the
stop deadline, and does not exit automatically when work drains. Send
`SIGTERM` afterward to complete stop; there is no signal to resume fetching.

Attempts successfully finalized as interrupted become available again without
consuming the attempt. If the process dies before finalization, running jobs are
left for normal stuck-job recovery. Workers must tolerate retries; stop cannot
roll back external side effects. Keep an external supervisor's kill deadline as
a backstop for uninterruptible native calls or blocked application code.

For `river worker`, a clean drain exits with status 0. Boot failures, detected
runtime-thread failures, and interrupted stops return status 1; forced
termination also exits with status 1. Lifecycle messages go to standard output;
worker logging uses the client's configured logger.

## Deployment

Configure your deployment platform's worker command as one of the commands above.
Run multiple independent instances for multiple processes; queue concurrency is
per process. A shell wrapper should use `exec` so signals reach the worker:

```sh
exec bundle exec river worker --config config/river.rb --stop-timeout 30
```

Give the supervisor more than the graceful timeout plus the five-second
finalization window before it forcibly kills the process, with additional margin
for scheduling and cleanup. Migrate once as a deployment step, not on each worker
boot. Configure restart policy in the supervisor, accounting for status 1 after
an interrupted stop.

For a rolling replacement, start new instances, confirm startup and database
connectivity, then send `SIGTERM` to old instances and allow them to drain. An
optional earlier `SIGTSTP` stops old instances from accepting more work. The
`River worker: ready pid=...` log means the initial database check and client start
have completed; it is not a health endpoint or a guarantee of ongoing queue health.

River does not provide a multiprocess supervisor, rolling-restart coordinator,
PID-file management, or a readiness probe server. Those remain deployment-platform
responsibilities.
