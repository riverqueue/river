# River TypeScript Example: Prisma

A minimal example demonstrating how to use the [River](https://github.com/riverqueue/river/tree/master/js) TypeScript client with [Prisma](https://www.prisma.io/) to insert background jobs into Postgres.

The example defines typed jobs and shows single insertion, scheduling, batch
insertion, and a caller-owned Prisma interactive transaction that commits an
application row and its River job atomically.

## Prerequisites

- Node.js >= 26
- pnpm
- Postgres with [River's schema](https://riverqueue.com/docs) migrated

## Setup

From the repository root, install dependencies (this is a workspace project):

```sh
pnpm install
```

Generate the Prisma client:

```sh
pnpm --dir examples/prisma run generate
```

Build the River packages and the example:

```sh
pnpm run build:all
pnpm --filter=riverqueue-example-prisma run build
```

The example build regenerates the Prisma client automatically.

## Running

```sh
DATABASE_URL=postgres://localhost:5432/river_dev pnpm start
```

If `DATABASE_URL` is not set, it defaults to `postgres://localhost:5432/river_dev`.
