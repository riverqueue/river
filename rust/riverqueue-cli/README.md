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
```

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

## Benchmark

`riverqueue bench` measures worker throughput and end-to-end latency. It
**truncates the River job table** in the selected database, so only point it at
a disposable database:

```sh
riverqueue bench --database-url postgres://localhost/river_bench --duration 30s
```

Run `riverqueue bench --help` for its options.
