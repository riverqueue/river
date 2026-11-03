# riverqueue-ruby development

All commands on this page run from `ruby/`, unless specified otherwise.

From the repository root, the equivalent setup and checks are:

```sh
make -C ruby install
createdb river_test
RIVER_REQUIRE_DATABASES=1 make test/ruby
make lint/ruby typecheck/ruby
```

`make test/ruby/conformance` runs only the Go-generated fixture checks and needs no Postgres server.

## Install dependencies

```shell
$ bundle install
$ pushd driver/riverqueue-activerecord && bundle install && popd
$ pushd driver/riverqueue-sequel && bundle install && popd
$ pushd rails/riverqueue-rails && bundle install && popd
```

The private Pro gem is developed and tested separately in `riverqueue-ruby-pro`.
Public install, test, and lint targets do not traverse sibling checkouts.

Keep the root lockfile usable on both macOS and Linux when updating dependencies.
Run `bundle lock --add-platform x86_64-linux` and commit the resulting lockfile;
CI uses frozen dependency installation and cannot add missing platforms itself.

## Run tests

Create a test database:

```shell
$ createdb river_test
```

Run the core, SQL driver packages, and Rails integration:

```shell
$ RIVER_REQUIRE_DATABASES=1 make test
```

Package suites run in separate processes, four at a time by default. Set
`TEST_JOBS=1` for serial execution, or choose another concurrency with
`make test TEST_JOBS=2`. Each package retains its own coverage checks.

CI runs Ruby 4.0 against Postgres 14–18, and Ruby 3.2–3.4 against Postgres 18.
SQLite and conformance checks run separately on Ruby 4.0. One Rails job tests
Rails 7.2, 8.0, and 8.1 on Ruby 4.0, reusing its Postgres service and gems.

Use `TEST_DATABASE=postgres` or `TEST_DATABASE=sqlite` to select a backend;
the default is `all`. CI merges the core and driver coverage reports from
Postgres and SQLite to enforce 100% line and branch coverage. Filtered local
runs collect coverage without enforcing that threshold individually.

Real database tests run by default. `RIVER_REQUIRE_DATABASES=1` requires both
the selected databases to be available instead of permitting local skips. CI
also requires them. Set `TEST_DATABASE_URL` to override the Postgres test
database URL. Tests create and migrate disposable schemas with the bundled SQL;
the database user must be able to create and drop schemas. No existing River tables are needed. The Go toolchain from `../go.work` is
required to generate the conformance fixtures before running core tests.

Rollback-wrapped tests share an empty schema for the suite, then drop it on exit.
Each example rolls back its writes instead of deleting shared tables, so test
speed is independent of existing data in `public`, which is left untouched.

Both driver packages run the same insertion and runtime contracts from
`spec/driver_shared_examples.rb` and `spec/driver_runtime_shared_examples.rb`
against Postgres and SQLite. These cover job state transitions, scheduling,
rescue, metadata, filtering, deletion, transactions, queues, and leadership.
Adapter-specific conversion tests remain in each driver's suite.

The driver suites also exercise Yugabyte capability simulations on
Postgres. For actual YSQL storage/transaction checks, run `make test/yugabyte`
with `YUGABYTE_DATABASE_URL` pointing to a disposable database. See
[Yugabyte verification](yugabyte.md#verification).

`spec/client_driver_shared_examples.rb` additionally starts real worker threads
for each combination, testing transaction visibility and rollback, committed
bulk insertion, output, retries, and exhausted jobs. These tests need committed
data, so they use disposable Postgres schemas and temporary file-backed SQLite
databases, initialized by batching the bundled canonical migration SQL, not
the shared public job tables. Migration tests still use `River::Migrator`,
covering upgrades, downgrades, legacy history, rollback, and populated data. See [migrations](migrations.md)
for synchronizing the SQL with upstream Go.

`bundle exec rspec spec` from `ruby/` runs only the core suite; use `make test`
for both SQL adapters and Rails. Generate fixtures first with
`make generate/fixtures` when invoking RSpec directly.

## Benchmark database operations

Run the same driver microbenchmarks against this checkout and an unchanged
checkout, using the installed gems from the current bundle:

```sh
RIVER_BENCH_ROOT=/path/to/baseline ITERATIONS=200 SAMPLES=7 \
  bundle exec ruby scripts/benchmark_driver.rb > /tmp/river-before.json
ITERATIONS=200 SAMPLES=7 \
  bundle exec ruby scripts/benchmark_driver.rb > /tmp/river-after.json
```

The script tests ActiveRecord and Sequel against local Postgres and temporary
file-backed SQLite databases. `TEST_DATABASE_URL` overrides the default
`postgres://localhost/river_test`. It creates and removes its own Postgres
schemas, leaving existing tables untouched. `RIVER_BENCH_ROOT` is the repository
root, not its `ruby/` directory.

Each operation warms up ten times, then reports the median of the per-sample
mean durations, allocations per operation, and a separate SQL trace. Fixture
resets and SQL recording are outside the timed sections. Run comparisons
sequentially, with tests and other benchmarks stopped; repeat in reverse order
to check local timing variance. These measurements describe database operations,
not end-to-end worker throughput, and include the adapters' default SQLite
journal and synchronization settings.

The thread benchmark performs a metadata update in a new thread and explicitly
reaps the pool after it exits. This includes the cost of reclaiming a leaked
connection; it also records how many dead leases remain after a thread finishes.
SQL traces are omitted for this case because SQLite tracing is connection-local.

To examine completion plans with stale state statistics, use:

```sh
TEST_DATABASE_URL=postgres://localhost:55436/river_test \
  RIVER_BENCH_ROOT=/path/to/baseline \
  bundle exec ruby scripts/benchmark_completion.rb > /tmp/completion-before.json
TEST_DATABASE_URL=postgres://localhost:55436/river_test \
  bundle exec ruby scripts/benchmark_completion.rb > /tmp/completion-after.json
```

This Postgres-only benchmark runs both adapters. It creates 50,000 jobs in
unclustered ID order, analyzes the table, then changes 5,000 jobs to running
without updating the statistics. It times completion separately from claiming
and captures `EXPLAIN (ANALYZE, BUFFERS)` for the actual completion SQL. Override
`JOBS`, `RUNNING_JOBS`, `ITERATIONS`, and `SAMPLES` to vary the workload; set
`RUNNING_JOBS=0` for the less contentious case. Autovacuum is disabled only on
the disposable benchmark table so both runs use the same statistics. This
models a burst of state changes rather than steady-state worker throughput.

## Conformance and migrations

`make test/conformance` checks shared Go-generated fixtures without Postgres.
See [conformance](conformance.md) for coverage and known parity gaps.

`make verify` checks bundled Postgres and SQLite migrations against the
canonical files in `../riverdriver/`, including filenames, bytes, license, and
manifest checksums. It requires only Ruby, not installed gems or a database.
Run `ruby scripts/sync_migrations.rb` to update the bundle after Go migrations
change, or `make generate/ruby-migrations` from the repository root.

## Run lint

```shell
$ bundle exec standardrb --fix
```

## Run type check (Steep)

```shell
$ bundle exec steep check
```

## Code coverage

The core and driver suites require 100% line and branch coverage of production
code; shared test files are excluded. Run the suite and open
`coverage/index.html` to find lines or branches that weren't covered:

```shell
$ bundle exec rspec spec
$ open coverage/index.html
```

## Publish gems

The Pro gem is released separately from the private `riverqueue-ruby-pro`
repository. Follow its README; do not include it in the public release below.

1. Choose a version, run scripts to update the versions in each gemspec file, build each gem, and `bundle install` which will update its `Gemfile.lock` with the new version:

    ```shell
    git checkout master && git pull --rebase
    export VERSION=v0.x.0

    ruby scripts/update_gemspec_version.rb riverqueue.gemspec
    ruby scripts/update_gemspec_version.rb driver/riverqueue-activerecord/riverqueue-activerecord.gemspec
    ruby scripts/update_gemspec_version.rb driver/riverqueue-sequel/riverqueue-sequel.gemspec
    ruby scripts/update_gemspec_version.rb rails/riverqueue-rails/riverqueue-rails.gemspec

    gem build riverqueue.gemspec
    pushd driver/riverqueue-activerecord && gem build riverqueue-activerecord.gemspec && popd
    pushd driver/riverqueue-sequel && gem build riverqueue-sequel.gemspec && popd
    pushd rails/riverqueue-rails && gem build riverqueue-rails.gemspec && popd

    bundle install
    pushd driver/riverqueue-activerecord && bundle install && popd
    pushd driver/riverqueue-sequel && bundle install && popd
    pushd rails/riverqueue-rails && bundle install && popd

    git checkout -b $USER-ruby-$VERSION
    ```

2. Update `CHANGELOG.md` to include the new version and open a pull request with those changes and the ones to the gemspecs and `Gemfile.lock`s above.

3. After the PR is merged, pull master and rebuild all gems with `make build`.
   Push those rebuilt archives, then tag the release with the `ruby/` language
   prefix, for example `ruby/v0.13.0` (plain `v*` tags are reserved for Go):

    ```shell
    git pull origin master
    make build

    gem push riverqueue-${VERSION#v}.gem
    pushd driver/riverqueue-activerecord && gem push riverqueue-activerecord-${VERSION#v}.gem && popd
    pushd driver/riverqueue-sequel && gem push riverqueue-sequel-${VERSION#v}.gem && popd
    pushd rails/riverqueue-rails && gem push riverqueue-rails-${VERSION#v}.gem && popd

    git tag ruby/$VERSION
    git push origin ruby/$VERSION
    ```

4. Cut a new GitHub release by visiting [new release](https://github.com/riverqueue/river/releases/new), selecting the new tag, and copying in the version's `CHANGELOG.md` content as the release body.
