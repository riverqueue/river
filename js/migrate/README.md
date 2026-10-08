# `@riverqueue/migrate`

River's Postgres and SQLite migrations, with a runner that applies them.
Requires Node.js 26 or newer.

River never migrates on its own: clients don't touch the schema when they are
constructed or started. Run migrations as a deployment step, either with this
package or with the `riverqueue` command from `@riverqueue/cli`, and keep
`@riverqueue/migrate` on the same version as `riverqueue`.

## Postgres

Pass the driver your client uses, so the pool and schema are configured once:

```ts
import { PgDriver } from "@riverqueue/driver-pg";
import { createMigrator } from "@riverqueue/migrate";
import { Pool } from "pg";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const driver = new PgDriver(pool, { schema: "river" });

const result = await createMigrator(driver).migrateUp();
for (const { name, version } of result.versions) {
  console.log(`applied ${version} ${name}`);
}
```

A deploy script that doesn't build a driver can pass the connection directly,
as `{ pool, schema }` or, for a single connected `pg.Client`,
`{ client, schema }`. Leave out `schema` to use the connection's
`search_path`.

## SQLite

Pass the `SqliteDriver`, or `{ database }` to migrate without a driver:

```ts
import { DatabaseSync } from "node:sqlite";

import { SqliteDriver } from "@riverqueue/driver-sqlite";
import { createMigrator } from "@riverqueue/migrate";

const database = new DatabaseSync("river.db");
await createMigrator(new SqliteDriver(database)).migrateUp();
```

Through a driver, each migration waits for the driver's other operations and,
while another connection holds the database's write lock, retries
asynchronously for up to the driver's `busyTimeout`, like River's other
statements. With `{ database }`, migrations run directly on that handle,
which waits for a busy database for its own `timeout`.

Version 8 rebuilds SQLite's `river_job` table with an `AUTOINCREMENT` key, so
a deleted job's ID is never handed to a new job; on Postgres it changes
nothing. Like River for Go's migration, it refuses to run in either direction
while the database holds a `river_job_sequence`, `river_job_workflow_scheduling`,
or `river_workflow` object, which extensions install alongside River's tables
and which the rebuild would drop. Migrate the main line to version 8 before
installing such an extension.

## Migrating up and down

`migrateUp()` applies every missing version. `migrateDown()` reverts only the
newest version, because reverting drops tables and their data. Both accept:

- `targetVersion`: the version to end at. Up applies versions through the
  target, or nothing when the target is already applied. Down reverts
  versions above it; `targetVersion: 0` reverts every version and removes
  River's tables.
- `maxSteps`: the most versions to run.
- `dryRun`: return the versions and SQL that would run without running them.

```ts
import type { PgDriver } from "@riverqueue/driver-pg";
import { createMigrator, type Migration } from "@riverqueue/migrate";

declare const driver: PgDriver;
const migrator = createMigrator(driver);

// Print the SQL that the next migrateUp() would run.
const pending = await migrator.migrateUp({ dryRun: true });
for (const { sql } of pending.versions) console.log(sql);

// Revert everything above version 5.
await migrator.migrateDown({ targetVersion: 5 });

// Fail a health check if the schema is behind.
const { messages, ok } = await migrator.validate();
if (!ok) throw new Error(messages.join("; "));
```

Each version runs in its own transaction along with its row in the
`river_migration` table. When several processes migrate the same database at
once, they take turns, and a version another process already applied is
skipped. Versions in the database that are newer than this package, for
example after a newer River release migrated it, are ignored, as River's Go
migrator ignores them.

Failures throw `MigrationError`, which extends `RiverError` and names the
`backend` and `operation` that failed.

## Additional migration lines

A package that extends River can ship its own migration line and apply it with
the same runner once the main line is at version 5 or later:

```ts continued
declare const extraMigrations: readonly Migration[];

await createMigrator(driver, {
  line: "extra",
  migrations: extraMigrations,
}).migrateUp();
```

The SQL in this package is copied from River's canonical migrations and
checked for drift, so don't edit it by hand.

## Requirements

Node.js 26 or newer with native `Temporal`: `node -p "typeof Temporal"` must
print `object`. Official Node.js binaries include it; some builds compiled from
source, including some distribution and Homebrew packages, do not.

Install `riverqueue` at exactly this package's version. It is a peer dependency,
so npm rejects a mismatched pair instead of loading two copies.

TypeScript users need TypeScript 6.0 or newer and `@types/node`, with `"node"`
listed in `compilerOptions.types`. See [River's
requirements](https://github.com/riverqueue/river/tree/master/js#requirements) for
details.
