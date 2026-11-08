# riverqueue-cli

Command-line tools for [River](https://riverqueue.com)'s Rust client. Install
the `riverqueue` binary with:

```sh
cargo install riverqueue-cli
```

## Migrations

River's schema is managed by versioned migrations shared with every River
implementation. Apply them before starting clients:

```sh
riverqueue migrate-up --database-url postgres://localhost/app
riverqueue migrate-up --database-url postgres://localhost/app --schema river
riverqueue migrate-up --database-url sqlite://app.sqlite3
riverqueue migrate-up --database-url mysql://localhost/app
```

The CLI includes Postgres, MySQL, and SQLite support by default. For MySQL,
select an existing database in the URL; `--schema` is a Postgres option.

`migrate-down`, `migrate-list`, and `validate` take the same connection
options. `--target-version N`, `--max-steps N`, and `--dry-run` limit or
preview a migration run. Applications can instead migrate from Rust with the
[`riverqueue-migrate`](https://docs.rs/riverqueue-migrate) crate.

`migrate-list` prints every bundled version and its name, including pending
migrations, and marks the latest applied version with `*`, like the Go CLI.
Migration progress uses Go's `applied migration 001 [up] ... [duration]` format.
A dry run prints `migration 001 [up] [DRY RUN]`; add `--show-sql` to print the
SQL as well. `--show-sql` also works when applying migrations. Runs with nothing
to apply print `no migrations to apply`.

Successful `validate` is silent. Outstanding migrations are reported on stderr
with exit status 2; connection and usage errors exit with status 1.

### Exporting SQL

`migrate-get` exports SQL for another migration framework without connecting to
a database. It defaults to Postgres and ignores `DATABASE_URL`;
use `--database-url mysql://` or `--database-url sqlite://` to select its SQL dialect.

```sh
riverqueue migrate-get --version 3 --up > river_3.up.sql
riverqueue migrate-get --version 3,2,1 --down > river.down.sql
riverqueue migrate-get --all --exclude-version 1 --up > river_all.up.sql
riverqueue migrate-get --all --down --database-url sqlite:// > river_sqlite.down.sql
```

Pass exactly one of `--all` or `--version`, and exactly one of `--up` or `--down`.
`--version` and `--exclude-version` accept comma-separated lists and repeated
flags. Explicit versions retain their supplied order; `--all` uses ascending
order for up migrations and descending order for down migrations. Exclusions
apply to either selection mode. Version 1 contains River's migration tables,
which are often unnecessary when using another migration framework.

Each migration includes a header such as `-- River main migration 003 [up]`.
`--schema NAME` substitutes a schema prefix verbatim; include SQL identifier
quoting in the value if needed, for example
`--schema '"MyRiver"'`. Only the `main` migration line is bundled (`--line main`).
Exports require the corresponding `postgres` or `sqlite` build feature; both
are enabled by default in the CLI.

## Benchmark

`riverqueue bench` measures worker throughput and end-to-end latency. It
**truncates the River job table** in the selected database, so only point it at
a disposable database:

```sh
riverqueue bench --database-url postgres://localhost/river_bench --duration 30s
```

Run `riverqueue bench --help` for its options.
