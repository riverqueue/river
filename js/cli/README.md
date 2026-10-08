# `@riverqueue/cli`

The `riverqueue` command runs River's database migrations and a throughput
benchmark, and upgrades code written for `riverqueue@0.1`. It requires
Node.js 26 or newer and mirrors the commands of River's Go CLI.

```sh
npm install --save-dev @riverqueue/cli
npx riverqueue migrate-up --database-url postgres://localhost/myapp
```

Run `npx riverqueue --help` for every command, and
`npx riverqueue <command> --help` for a command's flags.

## Migrations

| Command        | What it does                                             |
| -------------- | -------------------------------------------------------- |
| `migrate-up`   | Apply missing migrations                                 |
| `migrate-down` | Revert the newest migration, or more with a target       |
| `migrate-list` | List migrations and mark the newest applied one with `*` |
| `validate`     | Exit with status 1 if any migration is missing           |
| `migrate-get`  | Print migration SQL for use with another migration tool  |
| `version`      | Print the CLI and Node.js versions (also `--version`)    |

`--database-url` selects the database:

- `postgres://…` or `postgresql://…` for Postgres. Without
  `--database-url`, Postgres commands use the standard `PG*` environment
  variables when `PGDATABASE` is set.
- `sqlite://PATH` for SQLite, for example `sqlite:///var/lib/app/river.db`
  for an absolute path or `sqlite://river.db` for a relative one.

Other common flags:

- `--schema NAME`: the Postgres schema holding River's tables. It must match
  the `schema` given to `PgDriver`.
- `--target-version N`: the version to end at. With `migrate-down`,
  `--target-version 0` reverts every migration and drops River's tables.
- `--max-steps N`: run at most N migrations.
- `--dry-run` and `--show-sql`: print what would run, with its SQL.
- `--statement-timeout DURATION`: Postgres's `statement_timeout`, such as
  `30s` or `5m`. It defaults to a `statement_timeout` parameter in the URL,
  and otherwise to 10 seconds, as in River's Go CLI.

```sh
# Check in CI that a database is migrated.
npx riverqueue validate --database-url "$DATABASE_URL"

# Preview and apply migrations in a custom schema.
npx riverqueue migrate-up --database-url "$DATABASE_URL" --schema river \
  --dry-run --show-sql
npx riverqueue migrate-up --database-url "$DATABASE_URL" --schema river

# Hand River's SQL to another migration tool, without its tracking table.
npx riverqueue migrate-get --all --exclude-version 1 --up > river.up.sql
```

Applications can also migrate from code with `@riverqueue/migrate`.

## Upgrading from riverqueue 0.1

`riverqueue codemod-0.1` rewrites code written for the `riverqueue@0.1`
client to the current API and marks what it cannot rewrite with
`TODO(riverqueue-0.1)` comments. It needs the `typescript` package, version 5
or 6, in the project:

```sh
npx riverqueue codemod-0.1 --write src
```

Without `--write` it reports what would change; `--check` exits with status 1
if any file would change. [Migrating from riverqueue 0.1][migrating]
describes what it rewrites and what it leaves for review.

[migrating]: https://github.com/riverqueue/river/blob/master/js/docs/migrating-from-0.1.md

## Benchmark

`riverqueue bench` inserts and works no-op jobs and reports throughput. It is
destructive: it empties River's `river_job`, `river_leader`, `river_queue`,
and `river_notification` tables and runs `VACUUM FULL` on `river_job`, so
use it only on a disposable Postgres database. It requires an explicit
`--database-url` (it never reads `PG*` variables or `DATABASE_URL`), and
`--yes` unless it can ask for confirmation in an interactive terminal.

```sh
npx riverqueue bench --database-url postgres://localhost/river_bench --yes \
  --duration 30s
```

Without `--duration` it runs until Ctrl-C; press Ctrl-C again to stop without
waiting for running jobs. `--num-total-jobs N` (`-n`) inserts N jobs up front
and stops when all of them are worked.

Every two seconds the benchmark prints the jobs worked and inserted and jobs
per second. The summary adds the overall rate, the 95th percentile time from
insert to completion, the run time, and peak resource use: running jobs,
pending completions, completion queries, pool connections, event-loop delay,
heap, and RSS. The 95th percentile includes time spent waiting in the
backlog, so it reflects queue depth as much as per-job latency. The command
fails if running jobs or pending completions exceed their configured limits,
or if the pool exceeds `--max-connections`.

The default workload matches River's Go and Rust benchmarks so results are
comparable: a 75,000-job backlog topped up in batches of 5,000 and 2,000
concurrent workers. The pool defaults to 50 connections. `--backlog`,
`--batch-size`, `--max-workers`, `--max-connections`, and `--skip-vacuum`
change the workload for controlled comparisons.

## Running from code

`run(argv)` runs the same commands in-process and resolves to an exit code:

```ts
import { run } from "@riverqueue/cli";

process.exitCode = await run([
  "migrate-up",
  "--database-url",
  "postgres://localhost/myapp",
]);
```

Extension packages can embed this CLI and add migration lines, as with
River's Go CLI. The migration commands select an added line with
`--line NAME`, while River's `main` line stays the default:

```ts
import type { Migration, MigrationBackend } from "@riverqueue/migrate";
import { run } from "@riverqueue/cli";

declare function extensionMigrations(
  backend: MigrationBackend
): readonly Migration[];

process.exitCode = await run(process.argv.slice(2), {
  migrationLines: { extension: extensionMigrations },
  program: "extension",
});
```

## Requirements

Node.js 26 or newer with native `Temporal`: `node -p "typeof Temporal"` must
print `object`. Official Node.js binaries include it; some builds compiled from
source, including some distribution and Homebrew packages, do not.

The CLI installs its own matching `riverqueue` and database packages.

TypeScript users need TypeScript 6.0 or newer and `@types/node` and `@types/pg`,
with `"node"` listed in `compilerOptions.types`. See [River's
requirements](https://github.com/riverqueue/river/tree/master/js#requirements) for
details.
