# River TypeScript Client

TypeScript client for [River](https://github.com/riverqueue/river), a fast and reliable background job framework for PostgreSQL.

This is an **insert-only** client — it can enqueue jobs for processing, but jobs are executed by a River server written in Go. Both the client and server share the same PostgreSQL database.

## Packages

The project is structured as a monorepo with a core package and driver packages:

| Package | Description |
|---------|-------------|
| [`riverqueue`](.) | Core client, types, and job insertion logic. |
| [`@riverqueue/driver-pg`](./driver/pg) | Driver for [node-postgres (`pg`)](https://node-postgres.com/). |
| [`@riverqueue/driver-prisma`](./driver/prisma) | Driver for [Prisma](https://www.prisma.io/). |

Drivers are separate packages so that ORM/database libraries not in use don't become transitive dependencies.

## Installation

Install the core package along with the driver for your database library:

```sh
# Using node-postgres (pg)
pnpm add riverqueue @riverqueue/driver-pg pg

# Using Prisma
pnpm add riverqueue @riverqueue/driver-prisma
```

## Usage

### Defining Job Args

Job args must implement the `JobArgs` interface with a `kind` string that identifies the job type. Use `toJSON()` to control which fields are serialized as the job's args in the database:

```typescript
import type { JobArgs } from "riverqueue";

class SortArgs implements JobArgs {
  kind = "sort";

  constructor(public strings: string[]) {}

  toJSON() {
    return { strings: this.strings };
  }
}
```

For quick one-off jobs, use `JobArgsObject`:

```typescript
import { JobArgsObject } from "riverqueue";

const args = new JobArgsObject("sort", { strings: ["whale", "tiger", "bear"] });
```

### Inserting Jobs

#### With node-postgres

```typescript
import { Pool } from "pg";
import { Client } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";

const pool = new Pool({ connectionString: "postgres://localhost/mydb" });
const client = new Client(new PgDriver(pool));

// Insert a single job
const result = await client.insert(new SortArgs(["whale", "tiger", "bear"]));
console.log(result.job.id); // inserted job ID

// Insert with options
const result2 = await client.insert(
  new SortArgs(["whale", "tiger", "bear"]),
  {
    queue: "high_priority",
    priority: 2,
    maxAttempts: 5,
  }
);

// Insert many jobs at once
const results = await client.insertMany([
  new SortArgs(["whale", "tiger"]),
  new SortArgs(["bear", "fox"]),
]);
```

#### With Prisma

```typescript
import { PrismaClient } from "@prisma/client";
import { Client } from "riverqueue";
import { PrismaDriver } from "@riverqueue/driver-prisma";

const prisma = new PrismaClient();
const client = new Client(new PrismaDriver(prisma));

const result = await client.insert(new SortArgs(["whale", "tiger", "bear"]));
```

### Scheduled Jobs

Schedule jobs to run at a future time:

```typescript
await client.insert(new SortArgs(["whale", "tiger"]), {
  scheduledAt: new Date(Date.now() + 60 * 60 * 1000), // 1 hour from now
});
```

### Unique Jobs

Unique jobs prevent duplicate insertions based on configurable criteria:

```typescript
await client.insert(new SortArgs(["whale", "tiger"]), {
  uniqueOpts: {
    byArgs: true,    // unique per args
    byQueue: true,   // unique per queue
    byPeriod: 900,   // unique within 15-minute windows
  },
});
```

### Batch Inserts

Use `insertMany` for efficient batch insertions:

```typescript
import { InsertManyParams } from "riverqueue";

const results = await client.insertMany([
  // Raw job args use default options
  new SortArgs(["whale", "tiger"]),

  // InsertManyParams pairs args with per-job options
  new InsertManyParams(new SortArgs(["bear", "fox"]), {
    queue: "high_priority",
    maxAttempts: 10,
  }),
]);
```

### Transactions

#### With node-postgres

```typescript
const poolClient = await pool.connect();
try {
  await poolClient.query("BEGIN");

  await client.insert(new SortArgs(["whale"]), { tx: poolClient });
  await client.insert(new SortArgs(["tiger"]), { tx: poolClient });

  await poolClient.query("COMMIT");
} catch (e) {
  await poolClient.query("ROLLBACK");
  throw e;
} finally {
  poolClient.release();
}
```

#### With Prisma

```typescript
await prisma.$transaction(async (tx) => {
  await client.insert(new SortArgs(["whale"]), { tx });
  await client.insert(new SortArgs(["tiger"]), { tx });
});
```

## Development

See [developing River TypeScript](https://github.com/riverqueue/riverqueue-js/blob/master/docs/development.md).
