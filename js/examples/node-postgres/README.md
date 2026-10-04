# River TypeScript Example: node-postgres

A minimal example demonstrating how to use the [River](https://github.com/riverqueue/river/tree/master/js) TypeScript client with `node-postgres` (`pg`) to insert background jobs into PostgreSQL.

The example defines typed jobs and shows single insertion, scheduling, batch
insertion, and an explicit caller-owned PostgreSQL transaction that commits an
application row and its River job atomically.

## Prerequisites

- Node.js >= 26
- pnpm
- PostgreSQL with [River's schema](https://riverqueue.com/docs) migrated

## Setup

From the repository root, install dependencies (this is a workspace project):

```sh
pnpm install
```

Build the River packages and the example:

```sh
pnpm run build:all
cd examples/node-postgres
pnpm run build
```

## Running

```sh
DATABASE_URL=postgres://localhost:5432/river_dev pnpm start
```

If `DATABASE_URL` is not set, it defaults to `postgres://localhost:5432/river_dev`.
