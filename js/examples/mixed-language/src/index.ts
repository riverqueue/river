import { randomUUID } from "node:crypto";

import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
import { Client, defineJob } from "riverqueue";
import { z } from "zod";

const generateReport = defineJob({
  kind: "mixed_language.generate_report",
  schema: z.object({
    reportId: z.string().min(1),
    requestedBy: z.string().min(1),
    schemaVersion: z.number().int().positive(),
  }),
});

const pool = new Pool({
  connectionString:
    process.env.DATABASE_URL ?? "postgres://localhost:5432/river_dev",
});
const client = new Client(new PgDriver(pool));
const reportId = randomUUID();
const inserted = await client.insert(
  generateReport,
  {
    reportId,
    requestedBy: "typescript-api",
    schemaVersion: 1,
  },
  { scheduledAt: Temporal.Now.instant() }
);

const persisted = await pool.query<{
  args: {
    reportId: string;
    requestedBy: string;
    schemaVersion: number;
  };
  kind: string;
}>("SELECT args, kind FROM river_job WHERE id = $1", [inserted.job.id]);
const row = persisted.rows[0];
if (
  row?.kind !== generateReport.kind ||
  row.args.reportId !== reportId ||
  row.args.schemaVersion !== 1
) {
  throw new Error("persisted mixed-language contract did not round-trip");
}
console.log(
  `inserted ${row.kind} job ${inserted.job.id} for a Go, Rust, or JS worker`
);
await pool.end();
