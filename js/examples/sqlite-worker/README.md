# SQLite worker example

On Node.js 24, [set up Temporal](../../README.md#temporal-on-nodejs-24) and
set `NODE_OPTIONS` to preload it before running the commands below. Node.js 26
provides Temporal natively.

This example applies River's SQLite migrations to an in-memory database,
inserts a typed job, works it in-process, observes its committed completion,
and shuts down cleanly. It needs Node.js 24 or newer and the workspace packages.

From the repository root:

```sh
pnpm run build:all
pnpm --filter riverqueue-example-sqlite-worker run build
pnpm --filter riverqueue-example-sqlite-worker run start
```

Applications normally pass a file-backed `DatabaseSync` to
`new SqliteDriver(database)` and run migrations as a separate deployment step.
`SqliteDriver.memory()` creates an in-memory database instead, which keeps this
example disposable.
