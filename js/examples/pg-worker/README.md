# PostgreSQL worker example

This example migrates a PostgreSQL database, inserts a job, and works it in
the same process: the handler snoozes once as if a payment provider were busy,
then succeeds and inserts a follow-up job through the worker's own client. It
shuts down gracefully when the follow-up completes or on `SIGTERM`.

It uses `node-postgres`, Zod for validation, and structured logging through
the job's `logger` (which writes warnings and errors to `console` unless the
client is given a logger such as pino).

From the repository root, with a disposable PostgreSQL database:

```sh
pnpm install
pnpm run build:all
pnpm --filter riverqueue-example-pg-worker run build
DATABASE_URL=postgres://localhost:5432/river_example \
  pnpm --filter riverqueue-example-pg-worker run start
```
