# River development

The [Ruby](../ruby/), [Rust](../rust/), and [JavaScript](../js/) ports have
separate development instructions and `make test/<language>` targets.

## Run tests

Raise the test database:

    createdb river_test

Run tests:

    make test

By default, this runs all workspace modules and database drivers except MySQL,
which is opt-in. Use `make test/race` to enable the race detector.

To run only the MySQL driver tests (no Postgres needed):

    make test TEST_DATABASE=mysql

These connect as `root` without a password on `localhost:3306`. Set
`TEST_MYSQL_URL` to override the connection DSN. To include MySQL in the full
suite, set `RIVER_MYSQL_TESTS_ENABLED=1`.

To run only the SQLite, libSQL, and Turso driver tests (no Postgres needed):

    make test TEST_DATABASE=sqlite

To run the regular suite with the MySQL and SQLite driver tests excluded:

    make test TEST_DATABASE=postgres

`TEST_DATABASE` also applies to `make test/race`. CI separates these suites so
the Postgres version matrix doesn't repeat the MySQL and SQLite tests. Postgres
and SQLite run on both supported Go versions, with the latest combination in
each matrix using the race detector. MySQL runs separately with the race
detector on the latest supported Go version.

## Compare database performance

Run the same Go workload against MySQL, Postgres, and file-backed SQLite:

    make bench/compare

The command needs local Postgres and MySQL servers. It defaults to
`postgres://localhost/postgres` and `root@tcp(127.0.0.1:3306)/`. Override these
with `RIVER_BENCH_POSTGRES_URL` and `RIVER_BENCH_MYSQL_DSN` (the latter uses the
Go MySQL driver's DSN syntax). Credentials are not written into the results.
The Postgres role needs permission to create schemas in the selected database;
the MySQL user needs permission to create databases.

Each trial creates and migrates its own randomly named Postgres schema, MySQL
database, or temporary SQLite file, and removes it afterward. Existing River
tables are never truncated. SIGINT/SIGTERM also trigger cleanup; an uncatchable
termination may leave a `river_bench_*` schema/database behind.

By default, the benchmark uses:

- 30,000 identical, non-unique jobs with a 256-byte payload string per trial.
- `InsertMany` batches of 1,000, including returned job decoding and commit.
- 100 no-op workers, 2 ms fetch cooldown, and 20 ms polling interval.
- `GOMAXPROCS=4`, pools of 10 for Postgres/MySQL, and one connection for SQLite.
- File-backed SQLite with WAL and `synchronous=FULL`. Postgres and MySQL retain
  their configured durability settings, which are recorded in the JSON.
- One discarded warm-up round and three measured rounds. Database order rotates
  every round, and only one benchmark runs at a time.

Insertion throughput measures enqueueing all jobs before workers start. Drain
throughput measures client startup through receipt of all committed completion
events. Shutdown, migration, verification, and cleanup are outside these timers.
A trial fails on a job failure, retry, timeout, or incorrect persisted completion
count. These are separate batch-insert and backlog-drain workloads, **not** a
steady-state benchmark of producers inserting while workers run. They measure
River's Go implementations, including their different driver strategies, rather
than database engines in isolation. They do not measure application job work,
uniqueness conflicts, or latency distributions.

The command prints median and min/max throughput and saves every trial, database
versions/settings, Git status, Go build information, and before/after load
averages to a new JSON file. The default output path is in the system temporary
directory. Existing result files are never overwritten. Save a named result and
repeat the same settings later when other builds and tests are idle:

    make bench/compare BENCH_COMPARE_ARGS="-output /tmp/river-bench-busy.json"
    make bench/compare BENCH_COMPARE_ARGS="-output /tmp/river-bench-quiet.json"

For longer runs or a concurrency sweep, use the command's flags:

    make bench/compare BENCH_COMPARE_ARGS="-jobs 100000 -samples 6 -workers 1000 -output /tmp/river-bench-long.json"
    make bench/compare BENCH_COMPARE_ARGS="-h"

To isolate a driver during optimization, select just that backend:

    make bench/compare BENCH_COMPARE_ARGS="-backends mysql -samples 6 -output /tmp/river-bench-mysql.json"

To measure the count-only `InsertManyFast` API, which avoids fetching inserted
job rows and IDs, use:

    make bench/compare BENCH_COMPARE_ARGS="-insert-mode fast -output /tmp/river-bench-fast.json"

The default is `-insert-mode returning` (`InsertMany`). The mode is recorded in
the results. Both modes use the same non-unique jobs and verify their eventual
completion. Compare modes separately: requesting inserted rows does additional
work, particularly on MySQL where the driver must emulate `RETURNING`.

For MySQL, also compare the Go driver's optional parameter interpolation. This
avoids preparing and closing a server-side statement for each parameterized
query. The benchmark records `interpolate_params` without saving the DSN:

    RIVER_BENCH_MYSQL_DSN='root@tcp(127.0.0.1:3306)/?interpolateParams=true' make bench/compare BENCH_COMPARE_ARGS="-backends mysql -output /tmp/river-bench-mysql-interpolated.json"

When comparing driver changes, build both versions first and alternate their
execution order. Keep connection settings, including parameter interpolation,
the same for both versions; measure connection tuning separately.

Keep the same source revision, batch size, concurrency, payload, server settings,
and machine when comparing reruns. Warm-ups do not clear OS or database caches;
these are warm-server measurements on fresh tables. Shared-machine results are
provisional, and load averages alone cannot correct for CPU or disk contention.
The servers' durability mechanisms and cache sizes also differ, so inspect the
recorded settings before interpreting differences as engine speed.

## MySQL design and operation

MySQL support uses InnoDB and MySQL 8.0+ features; MariaDB is not a supported
substitute. The canonical migrations are in `riverdriver/rivermysql/migration`.
`make generate` copies them to Java, JavaScript, Ruby, and Rust. Migration
versions match the other backends even when a backend needs a no-op migration.

The schema deliberately uses these choices:

- `DATETIME(6)` stores UTC with microsecond precision without `TIMESTAMP`'s
  2038 upper bound or session time zone conversion. Drivers must encode and
  decode these values as UTC.
- `utf8mb4_0900_bin` gives identifiers exact, case-sensitive comparisons,
  including trailing spaces. The older `utf8mb4_bin` pads comparisons and can
  collapse distinct queue names. Every table also declares its character set,
  so notification payloads support Unicode regardless of database defaults.
- Native `JSON` validates documents and supports indexed/filterable operations.
  MySQL stores integers within its native signed/unsigned 64-bit range exactly,
  but can normalize larger numbers to floating point. Applications requiring
  arbitrary-precision values should encode them as JSON strings. Metadata
  merges are shallow and preserve explicit nulls; `JSON_MERGE_PATCH` would
  incorrectly merge nested objects and delete null-valued keys.
- Job states are checked strings. The unique-job index evaluates the unique
  state bitmask and yields SQL NULL for jobs outside that set. This preserves
  conditional uniqueness without a partial-index feature. The fetching index
  starts with state and queue, followed by priority, schedule, and ID to match
  the claiming order. Keep these predicates indexable: they determine which
  rows locking reads visit.

River-owned write transactions use READ COMMITTED and row locks. Operations
joining a caller's transaction preserve its isolation and commit/rollback
ownership. Without `RETURNING`, a mutation and the read of its result must
remain in one transaction. Locking reads and `SKIP LOCKED` let workers and
maintenance tasks avoid waiting on jobs another transaction owns.

Notifications use a transactionally written outbox and an independent cursor
per listener. Auto-increment IDs are allocated before commit, so a higher ID
can commit first. Listeners use locking scans when establishing and advancing
the cursor, waiting for earlier uncommitted rows; a snapshot `MAX(id)` or
`SKIP LOCKED` scan could lose notifications. Cleanup is bounded and skips
locked rows.

MySQL DDL commits implicitly. Run one migration process at a time; the port
migrators also use named locks, while Go's migrator relies on deployment
coordination. A failed migration can leave partial DDL applied; inspect and
repair the schema before retrying. Application transactions cannot roll back
schema changes. Go rejects MySQL calls to the deprecated `MigrateTx` API before
executing SQL; use `Migrate` on a pool instead.

For maximum-size job output, configure `max_allowed_packet` to at least
128 MiB on the server (and compatible client limits). Ruby SQL adapters can
escape a 32 MiB output to more than 64 MiB of SQL. For a local test server:

    mysql -uroot -e 'SET GLOBAL max_allowed_packet = 134217728'

Use a fresh connection after changing this setting. The Ruby MySQL CI job sets
it explicitly. Go tests accept the driver's DSN in `TEST_MYSQL_URL`; the ports
use URLs, with Ruby requiring the `mysql2://` scheme.

See MySQL's [binary collation rules](https://dev.mysql.com/doc/refman/8.4/en/charset-binary-collations.html),
[native JSON representation](https://dev.mysql.com/doc/refman/8.4/en/json.html),
and [locking reads](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html).

## Run lint

Run the linter and try to autofix:

    make lint

## Generate sqlc

The project uses sqlc (`brew install sqlc`) to generate Go targets for Postgres
queries. After changing an sqlc `.sql` file, generate Go with:

    make generate

## Raise and migrate development database

To run programs locally outside of tests, create and raise a development database:

    createdb river_dev
    go run ./cmd/river migrate-up --database-url postgres:///river_dev --line main

If needed, override Postgres timeouts for long-running migrations with root CLI
flags:

    go run ./cmd/river --statement-timeout 2m migrate-up --database-url postgres:///river_dev --line main

## Releasing a new version

1. Fetch changes to the repo and any new tags. Export `VERSION` by incrementing the last tag. Execute `update-mod-version` to add it the project's `go.mod` files:

    ```shell
    git checkout master && git pull --rebase
    export VERSION=v0.x.y
    make update-mod-version
    git checkout -b $USER-$VERSION
    ```

2. Prepare a PR with the changes, updating `CHANGELOG.md` with any necessary additions at the same time. Include **`[skip ci]`** in the commit description so that CI doesn't run and pollute the Go Module cache by trying to fetch a version that's not available yet (and it would fail anyway). Have it reviewed and merged.

3. Upon merge, pull down the changes, tag each module with the new version, and push the new tags:

    ```shell
    git checkout master && git pull --rebase
    git tag cmd/river/$VERSION -m "release cmd/river/$VERSION"
    git tag riverdriver/$VERSION -m "release riverdriver/$VERSION"
    git tag riverdriver/rivermysql/$VERSION -m "release riverdriver/rivermysql/$VERSION"
    git tag riverdriver/riverpgxv5/$VERSION -m "release riverdriver/riverpgxv5/$VERSION"
    git tag riverdriver/riverdatabasesql/$VERSION -m "release riverdriver/riverdatabasesql/$VERSION"
    git tag riverdriver/riversqlite/$VERSION -m "release riverdriver/riversqlite/$VERSION"
    git tag rivershared/$VERSION -m "release rivershared/$VERSION"
    git tag rivertype/$VERSION -m "release rivertype/$VERSION"
    git tag $VERSION
    ```

4. Push new tags to GitHub:

    ```shell
    git push --tags
    ```

5. Cut a new GitHub release by visiting [new release](https://github.com/riverqueue/river/releases/new), selecting the new tag, and copying in the version's `CHANGELOG.md` content as the release body.

### Updating Go or toolchain versions in all `go.mod` files

Modify `go.work` so it contains the new desired version in `go` and/or `toolchain` directives, then run `make update-mod-go` to have it reflect the new version(s) into all the workspace's `go.mod` files:

```shell
make update-mod-go
```
