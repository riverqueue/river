# MySQL driver

`@riverqueue/driver-mysql` provides River's client, worker runtime, and migrations
on MySQL 8.0 and later. Use the latest patch release of your MySQL version.
Jobs use the same schema and protocol as River for Go.

```sh
npm install riverqueue @riverqueue/driver-mysql @riverqueue/migrate mysql2
```

```ts
import { createPool } from "mysql2/promise";
import { Client, defineJob, Workers } from "riverqueue";
import { MySqlDriver } from "@riverqueue/driver-mysql";
import { createMigrator } from "@riverqueue/migrate";

const pool = createPool("mysql://localhost/my_app");
const driver = new MySqlDriver(pool);
await createMigrator(driver).migrateUp();

const message = defineJob({ kind: "message" });
const workers = new Workers().add(message, () =>
  console.log("Message received")
);
const client = new Client(driver, {
  queues: { default: { maxWorkers: 10 } },
  workers,
});
const run = await client.start();
await client.insert(message, {});
await run.stop();
await pool.end();
```

Run migrations during deployment before starting workers. The CLI accepts
`--database-url mysql://localhost/my_app`. MySQL DDL commits implicitly: a
migration failure can leave partially applied DDL, while migration history is
recorded only after a version succeeds. Inspect and repair the failed version
before retrying. A named lock serializes migrators for the same database.

Pass `{ schema: "background_jobs" }` to the driver to keep River's tables in a
separate MySQL database. River never closes your pool. Its own transactions use
READ COMMITTED; caller transactions retain their configured isolation level.

To insert within an application transaction, check out a connection, call
`beginTransaction()`, and pass it as `{ tx }` to River. Commit or roll back the
connection yourself, then release it. Worker contexts also support completing
jobs in that transaction through `completeTx`.

Claims use `FOR UPDATE SKIP LOCKED`. Notifications use the durable
`river_notification` outbox, including cancellation, queue controls, and
leadership resignation requests. Job IDs remain `bigint` and timestamps retain
microsecond precision regardless of the pool's JSON, date, or bigint options.
