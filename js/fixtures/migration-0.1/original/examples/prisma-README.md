# River TypeScript Example: Prisma

A minimal example demonstrating how to use the [River](https://github.com/riverqueue/riverqueue-js) TypeScript client with [Prisma](https://www.prisma.io/) to insert background jobs into PostgreSQL.

The example defines two job types (`SortArgs` and `SendEmailArgs`) and shows single job insertion, insertion with scheduling options, and batch insertion.

## Prerequisites

- Node.js >= 18
- pnpm
- PostgreSQL with [River's schema](https://riverqueue.com/docs) migrated

## Setup

From the repository root, install dependencies (this is a workspace project):

```sh
pnpm install
```

Generate the Prisma client:

```sh
cd examples/prisma
npx prisma generate
```

Build the River packages and the example:

```sh
pnpm run build:all
cd examples/prisma
pnpm run build
```

## Running

```sh
DATABASE_URL=postgres://localhost:5432/river_dev pnpm start
```

If `DATABASE_URL` is not set, it defaults to `postgres://localhost:5432/river_dev`.
