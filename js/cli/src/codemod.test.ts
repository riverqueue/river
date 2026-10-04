import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { migrateSources, type CodemodFileResult } from "./codemod.js";
import { loadTypeScript } from "./codemod-command.js";

const ts = loadTypeScript([import.meta.dirname]);

const ROOT = "/project/src";

/** Strip the indentation common to every non-blank line of a template. */
function code(strings: TemplateStringsArray, ...values: unknown[]): string {
  const text = String.raw({ raw: strings }, ...values).replace(/^\n/, "");
  const indents = text
    .split("\n")
    .filter((line) => line.trim() !== "")
    .map((line) => /^ */.exec(line)?.[0].length ?? 0);
  const indent = Math.min(...indents);
  return text
    .split("\n")
    .map((line) => line.slice(indent))
    .join("\n")
    .replace(/[ ]+$/, "");
}

/**
 * Migrate files named relative to a fake project root, and check that a
 * second run over the output changes nothing.
 */
function migrate(
  files: Record<string, string>
): Map<string, CodemodFileResult> {
  const results = migrateSources(
    ts,
    Object.entries(files).map(([name, text]) => ({
      path: `${ROOT}/${name}`,
      text,
    }))
  );
  const again = migrateSources(
    ts,
    results.map(({ output, path }) => ({ path, text: output }))
  );
  for (const [index, result] of again.entries()) {
    expect(result.output, `second run over ${result.path}`).toBe(
      results[index]?.output
    );
  }
  return new Map(
    results.map((result) => [result.path.slice(ROOT.length + 1), result])
  );
}

function migrateOne(text: string, name = "app.ts"): string {
  const result = migrate({ [name]: text }).get(name);
  if (result === undefined) throw new Error(`no result for ${name}`);
  return result.output;
}

describe("migrateSources", () => {
  describe("argument classes", () => {
    it("converts a parameter-property class and its insert sites", () => {
      expect(
        migrateOne(code`
          import { Client, type JobArgs } from "riverqueue";

          declare const client: Client;

          /** Sorts strings. */
          export class SortArgs implements JobArgs {
            kind = "sort";
            // Sorting has its own queue.
            insertOpts = { queue: "sorting", uniqueOpts: { byPeriod: 60 } };

            constructor(readonly strings: string[], public reverse?: boolean) {}
          }

          export async function enqueue(strings: string[]) {
            await client.insert(new SortArgs(strings, true), { priority: 2 });
            await client.insert(new SortArgs(["a"]));
            await client.insertMany([new SortArgs(strings)]);
          }
        `)
      ).toBe(code`
        import { Client, defineJob } from "riverqueue";

        declare const client: Client;

        /** Sorts strings. */
        export const sort = defineJob<{ strings: string[]; reverse?: boolean }>()({
          kind: "sort",
          // Sorting has its own queue.
          defaults: { queue: "sorting", unique: { byPeriod: { seconds: 60 } } },
        });

        export async function enqueue(strings: string[]) {
          await client.insert(sort, { strings, reverse: true }, { priority: 2 });
          await client.insert(sort, { strings: ["a"] });
          await client.insertMany([{ job: sort, args: { strings } }]);
        }
      `);
    });

    it("accepts a toJSON that returns exactly the parameters", () => {
      expect(
        migrateOne(code`
          import type { JobArgs } from "riverqueue";

          class SortArgs implements JobArgs {
            readonly kind = 'sort' as const;

            constructor(
              /** Strings to sort. */
              public strings: string[],
            ) {}

            toJSON() {
              return { strings: this.strings };
            }
          }
        `)
      ).toBe(code`
        import { defineJob } from "riverqueue";

        const sort = defineJob<{
          /** Strings to sort. */
          strings: string[];
        }>()({
          kind: 'sort',
        });
      `);
    });

    it("defines a class without parameters as an unchecked job", () => {
      expect(
        migrateOne(code`
          import { Client, JobArgs } from "riverqueue";

          declare const client: Client;

          class PingArgs implements JobArgs {
            kind = "ping";
          }

          await client.insert(new PingArgs());
        `)
      ).toBe(code`
        import { Client, defineJob } from "riverqueue";

        declare const client: Client;

        const ping = defineJob({
          kind: "ping",
        });

        await client.insert(ping, {});
      `);
    });

    it("avoids names that are already taken", () => {
      expect(
        migrateOne(code`
          import type { JobArgs } from "riverqueue";

          const sort = (values: string[]) => values.sort();

          class SortArgs implements JobArgs {
            kind = "sort";
            constructor(public strings: string[]) {}
          }

          class DeleteArgs implements JobArgs {
            kind = "delete";
            constructor(public id: string) {}
          }
        `)
      ).toBe(code`
        import { defineJob } from "riverqueue";

        const sort = (values: string[]) => values.sort();

        const sortJob = defineJob<{ strings: string[] }>()({
          kind: "sort",
        });

        const deleteJob = defineJob<{ id: string }>()({
          kind: "delete",
        });
      `);
    });

    it.each([
      [
        "a method",
        "describe() { return this.id; }",
        "it has members other than `kind`, `insertOpts`, and constructor parameter properties",
      ],
      [
        "a computed kind",
        "",
        "its `kind` is not a string literal",
        'kind = ["re", "port"].join("");',
      ],
      [
        "a constructor body",
        "",
        "its constructor has a body",
        'kind = "report";',
        "constructor(public id: string) { console.log(id); }",
      ],
      [
        "a plain constructor parameter",
        "",
        "a constructor parameter is not a parameter property",
        'kind = "report";',
        "constructor(id: string) {}",
      ],
      [
        "a default parameter",
        "",
        "a constructor parameter is a rest, default, or decorated parameter",
        'kind = "report";',
        'constructor(public id = "x") {}',
      ],
      [
        "a toJSON that renames fields",
        "toJSON() { return { report_id: this.id }; }",
        "its `toJSON` does not return exactly its parameters",
      ],
    ])(
      "flags a class with %s",
      (
        _case,
        extra,
        reason,
        kind = 'kind = "report";',
        constructor = "constructor(public id: string) {}"
      ) => {
        const input = code`
          import { Client, type JobArgs } from "riverqueue";

          declare const client: Client;

          class ReportArgs implements JobArgs {
            ${kind}
            ${constructor}
            ${extra}
          }

          await client.insert(new ReportArgs("r1"));
        `;
        const output = migrateOne(input);

        expect(output).toContain(
          `// TODO(riverqueue-0.1): convert \`ReportArgs\` to \`defineJob\` by hand: ${reason}\nclass ReportArgs implements JobArgs {`
        );
        expect(output).toContain(
          "// TODO(riverqueue-0.1): `ReportArgs` could not be converted automatically; insert a job definition with a plain args object\n" +
            'await client.insert(new ReportArgs("r1"));'
        );
        expect(output).toContain(
          'import { Client, type JobArgs } from "riverqueue";'
        );
      }
    );

    it("flags a construction outside insert calls and other references", () => {
      expect(
        migrateOne(code`
          import { Client, type JobArgs } from "riverqueue";

          declare const client: Client;

          class SortArgs implements JobArgs {
            kind = "sort";
            constructor(public strings: string[]) {}
          }

          const args = new SortArgs(["b", "a"]);
          export function describe(value: SortArgs) {
            return value.strings;
          }
          await client.insertMany([new SortArgs(...[["a"]])]);
        `)
      ).toBe(code`
        import { Client, defineJob } from "riverqueue";

        declare const client: Client;

        const sort = defineJob<{ strings: string[] }>()({
          kind: "sort",
        });

        // TODO(riverqueue-0.1): pass \`sort\` and the args object \`{ strings: ["b", "a"] }\` to insert or insertMany
        const args = new SortArgs(["b", "a"]);
        // TODO(riverqueue-0.1): \`SortArgs\` is now the \`sort\` job definition; pass it with a plain args object
        export function describe(value: SortArgs) {
          return value.strings;
        }
        // TODO(riverqueue-0.1): spell out the \`SortArgs\` arguments as a plain args object for \`sort\`
        await client.insertMany([new SortArgs(...[["a"]])]);
      `);
    });

    it("rewrites imports, aliases, and re-exports across files", () => {
      const results = migrate({
        "app.ts": code`
          import type { Client } from "riverqueue";
          import { SendEmailArgs, Mail, SortArgs as Sort } from "./jobs/index.js";

          declare const client: Client;
          const sendEmail = "taken";

          await client.insert(new SendEmailArgs("a@example.com"));
          await client.insert(new Mail("b@example.com"));
          await client.insert(new Sort(["b"]));
        `,
        "jobs/email.ts": code`
          import type { JobArgs } from "riverqueue";

          export class SendEmailArgs implements JobArgs {
            kind = "send_email";
            constructor(public to: string) {}
          }

          class SortArgs implements JobArgs {
            kind = "sort";
            constructor(public strings: string[]) {}
          }

          export { SortArgs };
        `,
        "jobs/index.ts": code`
          export * from "./email.js";
          export { SendEmailArgs as Mail } from "./email.js";
        `,
      });

      expect(results.get("app.ts")?.output).toBe(code`
        import type { Client } from "riverqueue";
        import { sendEmail as sendEmailJob, Mail, sort as Sort } from "./jobs/index.js";

        declare const client: Client;
        const sendEmail = "taken";

        await client.insert(sendEmailJob, { to: "a@example.com" });
        await client.insert(Mail, { to: "b@example.com" });
        await client.insert(Sort, { strings: ["b"] });
      `);
      expect(results.get("jobs/email.ts")?.output).toBe(code`
        import { defineJob } from "riverqueue";

        export const sendEmail = defineJob<{ to: string }>()({
          kind: "send_email",
        });

        const sort = defineJob<{ strings: string[] }>()({
          kind: "sort",
        });

        export { sort };
      `);
      expect(results.get("jobs/index.ts")?.output).toBe(code`
        export * from "./email.js";
        export { sendEmail as Mail } from "./email.js";
      `);
    });

    it("matches a path-alias import by its unique exported class name", () => {
      const results = migrate({
        "app.ts": code`
          import type { Client } from "riverqueue";
          import { SortArgs } from "@/jobs";

          declare const client: Client;
          await client.insert(new SortArgs(["b"]));
        `,
        "jobs.ts": code`
          import type { JobArgs } from "riverqueue";

          export class SortArgs implements JobArgs {
            kind = "sort";
            constructor(public strings: string[]) {}
          }
        `,
      });

      expect(results.get("app.ts")?.output).toContain(
        'import { sort } from "@/jobs";'
      );
      expect(results.get("app.ts")?.output).toContain(
        'await client.insert(sort, { strings: ["b"] });'
      );
    });

    it("flags an unknown class constructed in an insert call", () => {
      expect(
        migrateOne(code`
          import type { Client } from "riverqueue";
          import { SortArgs } from "./elsewhere.js";

          declare const client: Client;
          await client.insert(new SortArgs(["b"]));
        `)
      ).toContain(
        "// TODO(riverqueue-0.1): `SortArgs` was not converted; insert a job definition with a plain args object\n" +
          "await client.insert(new SortArgs"
      );
    });
  });

  describe("batch items", () => {
    it("converts InsertManyParams and JobArgsObject", () => {
      expect(
        migrateOne(code`
          import {
            Client,
            InsertManyParams,
            JobArgsObject,
          } from "riverqueue";

          declare const client: Client;
          declare const tx: unknown;

          await client.insert(new JobArgsObject("send_email", { to: "a" }), { tx });
          await client.insertMany(
            [
              new InsertManyParams(new JobArgsObject("send_email", { to: "b" }), {
                priority: 2,
              }),
              new InsertManyParams(new JobArgsObject("email.digest", {})),
              new JobArgsObject("email.digest", { weekly: true }),
            ],
            { tx }
          );
        `)
      ).toBe(code`
        import { Client, defineJob } from "riverqueue";

        const sendEmailJob = defineJob({ kind: "send_email" });
        const emailDigestJob = defineJob({ kind: "email.digest" });

        declare const client: Client;
        declare const tx: unknown;

        await client.insert(sendEmailJob, { to: "a" }, { tx });
        await client.insertMany(
          [
            { job: sendEmailJob, args: { to: "b" }, options: {
              priority: 2,
            } },
            { job: emailDigestJob, args: {} },
            { job: emailDigestJob, args: { weekly: true } },
          ],
          { tx }
        );
      `);
    });

    it("moves a TODO out of a rewritten batch item", () => {
      expect(
        migrateOne(code`
          import { Client, InsertManyParams, JobArgsObject } from "riverqueue";

          declare const client: Client;
          declare const period: number;

          await client.insertMany([
            new InsertManyParams(new JobArgsObject("sort", {}), {
              uniqueOpts: { byPeriod: period },
            }),
          ]);
        `)
      ).toBe(code`
        import { Client, defineJob } from "riverqueue";

        const sortJob = defineJob({ kind: "sort" });

        declare const client: Client;
        declare const period: number;

        await client.insertMany([
          // TODO(riverqueue-0.1): \`byPeriod\` was a number of seconds and is now a duration; check the value
          { job: sortJob, args: {}, options: {
            unique: { byPeriod: { seconds: period } },
          } },
        ]);
      `);
    });

    it("flags forms it cannot convert", () => {
      expect(
        migrateOne(code`
          import { Client, InsertManyParams, JobArgsObject } from "riverqueue";

          declare const client: Client;
          declare const kind: string;
          declare const legacy: InsertManyParams[];

          const args = new JobArgsObject("sort", { strings: [] });
          await client.insert(new JobArgsObject(kind, {}));
          await client.insertMany([new InsertManyParams(args, { priority: 2 })]);
        `)
      ).toBe(code`
        import { Client, InsertManyParams, JobArgsObject } from "riverqueue";

        declare const client: Client;
        declare const kind: string;
        // TODO(riverqueue-0.1): \`InsertManyParams\` was removed; use a job definition with a plain args object
        declare const legacy: InsertManyParams[];

        // TODO(riverqueue-0.1): pass \`defineJob({ kind: "sort" })\` and the args object \`{ strings: [] }\` to insert or insertMany
        const args = new JobArgsObject("sort", { strings: [] });
        // TODO(riverqueue-0.1): define this \`JobArgsObject\` kind with \`defineJob({ kind })\` and insert a plain args object
        await client.insert(new JobArgsObject(kind, {}));
        // TODO(riverqueue-0.1): replace \`InsertManyParams\` with a \`{ job, args, options }\` item built from a job definition
        await client.insertMany([new InsertManyParams(args, { priority: 2 })]);
      `);
    });

    it("uses a namespace import", () => {
      expect(
        migrateOne(code`
          import * as river from "riverqueue";

          declare const client: river.Client;

          await client.insertMany([
            new river.InsertManyParams(new river.JobArgsObject("sort", {})),
          ]);
          const state = river.JOB_STATE_AVAILABLE;
        `)
      ).toBe(code`
        import * as river from "riverqueue";

        const sortJob = river.defineJob({ kind: "sort" });

        declare const client: river.Client;

        await client.insertMany([
          { job: sortJob, args: {} },
        ]);
        const state = river.JOB_STATE.available;
      `);
    });
  });

  describe("insert options", () => {
    it("imports renamed option types under their 0.1 names", () => {
      expect(
        migrateOne(code`
          import { Client, type ClientOpts } from "riverqueue";

          declare const options: ClientOpts;
          void Client;
        `)
      ).toBe(code`
        import { Client, type ClientOptions as ClientOpts } from "riverqueue";

        declare const options: ClientOpts;
        void Client;
      `);
    });

    it("renames uniqueOpts and converts byPeriod seconds", () => {
      expect(
        migrateOne(code`
          import type { Client, InsertOpts, UniqueOpts } from "riverqueue";

          declare const client: Client;
          declare const args: never;
          declare function period(): number;
          declare const shared: UniqueOpts;

          const defaults: InsertOpts = {
            uniqueOpts: { byArgs: ["id"], byPeriod: 15 * 60, byQueue: true },
          };
          const unique = { byPeriod: 60 } satisfies UniqueOpts;
          await client.insert(args, { uniqueOpts: { byPeriod: period() } });
          await client.insert(args, { uniqueOpts: shared });
          await client.insert(args, {
            uniqueOpts: { byArgs: false, byPeriod: Temporal.Duration.from({ hours: 1 }) },
          });
          const unrelated = { uniqueOpts: true };
        `)
      ).toBe(code`
        import type {
          Client,
          InsertOptions as InsertOpts,
          UniqueOptions as UniqueOpts,
        } from "riverqueue";

        declare const client: Client;
        declare const args: never;
        declare function period(): number;
        declare const shared: UniqueOpts;

        const defaults: InsertOpts = {
          unique: { byArgs: ["id"], byPeriod: { seconds: 15 * 60 }, byQueue: true },
        };
        const unique = { byPeriod: { seconds: 60 } } satisfies UniqueOpts;
        // TODO(riverqueue-0.1): \`byPeriod\` was a number of seconds and is now a duration; check the value
        await client.insert(args, { unique: { byPeriod: { seconds: period() } } });
        // TODO(riverqueue-0.1): \`unique.byPeriod\` is now a duration such as \`{ seconds: 60 }\`, not a number of seconds
        await client.insert(args, { unique: shared });
        await client.insert(args, {
          // TODO(riverqueue-0.1): \`byArgs\` takes \`true\` or a list of fields; omit it instead of passing false
          unique: { byArgs: false, byPeriod: Temporal.Duration.from({ hours: 1 }) },
        });
        // TODO(riverqueue-0.1): if these are River insert options, rename \`uniqueOpts\` to \`unique\` (\`byPeriod\` is now a duration)
        const unrelated = { uniqueOpts: true };
      `);
    });

    it("moves the client schema option to PgDriver", () => {
      expect(
        migrateOne(code`
          import { Client } from "riverqueue";
          import { PgDriver } from "@riverqueue/driver-pg";

          declare const pool: never;
          declare const options: { schema: string };

          export const client = new Client(new PgDriver(pool), { schema: "river" });
          export const other = new Client(new PgDriver(pool), options);
        `)
      ).toBe(code`
        import { Client } from "riverqueue";
        import { PgDriver } from "@riverqueue/driver-pg";

        declare const pool: never;
        declare const options: { schema: string };

        export const client = new Client(new PgDriver(pool, { schema: "river" }));
        // TODO(riverqueue-0.1): the \`schema\` client option moved to the driver: \`new PgDriver(pool, { schema })\`
        export const other = new Client(new PgDriver(pool), options);
      `);
    });
  });

  describe("results and rows", () => {
    it("replaces uniqueSkippedAsDuplicated with the status", () => {
      expect(
        migrateOne(code`
          import type { InsertResult } from "riverqueue";

          declare const result: InsertResult;
          declare const maybe: InsertResult | undefined;

          if (result.uniqueSkippedAsDuplicated) console.log("duplicate");
          if (!result.uniqueSkippedAsDuplicated) console.log("inserted");
          if (!maybe?.uniqueSkippedAsDuplicated) console.log("maybe inserted");
          const same = result.uniqueSkippedAsDuplicated === true;
          const flags = [result.uniqueSkippedAsDuplicated && true];
          const { uniqueSkippedAsDuplicated } = result;
        `)
      ).toBe(code`
        import type { InsertResult } from "riverqueue";

        declare const result: InsertResult;
        declare const maybe: InsertResult | undefined;

        if (result.status === "duplicate") console.log("duplicate");
        if (result.status === "inserted") console.log("inserted");
        if (maybe?.status !== "duplicate") console.log("maybe inserted");
        const same = (result.status === "duplicate") === true;
        const flags = [result.status === "duplicate" && true];
        // TODO(riverqueue-0.1): replace \`uniqueSkippedAsDuplicated\` with \`status === "duplicate"\`
        const { uniqueSkippedAsDuplicated } = result;
      `);
    });

    it("flags job IDs and timestamps without changing them", () => {
      expect(
        migrateOne(code`
          import type { InsertResult } from "riverqueue";

          declare const result: InsertResult;

          const id: number = result.job.id;
          console.log(\`job \${result.job.id}\`, result.job.id.toString());
          const age = Date.now() - result.job.createdAt.getTime();
          const at = \`
            \${result.job.scheduledAt}
          \`;
        `)
      ).toBe(code`
        import type { InsertResult } from "riverqueue";

        declare const result: InsertResult;

        // TODO(riverqueue-0.1): \`JobRow.id\` is now a \`bigint\`; review number annotations, arithmetic, and JSON serialization
        const id: number = result.job.id;
        console.log(\`job \${result.job.id}\`, result.job.id.toString());
        // TODO(riverqueue-0.1): \`JobRow.createdAt\` is now a \`Temporal.Instant\`, not a \`Date\`
        const age = Date.now() - result.job.createdAt.getTime();
        // TODO(riverqueue-0.1): \`JobRow.scheduledAt\` is now a \`Temporal.Instant\`, not a \`Date\`
        const at = \`
          \${result.job.scheduledAt}
        \`;
      `);
    });

    it("leaves files that do not use riverqueue alone", () => {
      const input = code`
        const job = { id: 1, createdAt: new Date() };
        console.log(job.id, { uniqueOpts: 1 });
      `;
      const results = migrate({ "other.ts": input });

      expect(results.get("other.ts")).toMatchObject({
        changed: false,
        output: input,
        sites: [],
      });
    });
  });

  describe("imports", () => {
    it("replaces JOB_STATE constants", () => {
      expect(
        migrateOne(code`
          import { JOB_STATE_AVAILABLE, JOB_STATE_RUNNING, type JobState } from "riverqueue";

          const states: JobState[] = [JOB_STATE_AVAILABLE, JOB_STATE_RUNNING];
          const labels = { JOB_STATE_AVAILABLE };
        `)
      ).toBe(code`
        import { JOB_STATE, type JobState } from "riverqueue";

        const states: JobState[] = [JOB_STATE.available, JOB_STATE.running];
        const labels = { JOB_STATE_AVAILABLE: JOB_STATE.available };
      `);
    });

    it("keeps a still-referenced import and flags removed exports", () => {
      expect(
        migrateOne(code`
          import {
            Client,
            type Driver,
            type JobArgs,
            uniqueBitmaskFromStates,
          } from "riverqueue";

          export function enqueue(client: Client, args: JobArgs, driver: Driver) {
            return uniqueBitmaskFromStates(["available"]);
          }
        `)
      ).toBe(code`
        // TODO(riverqueue-0.1): \`Driver\` is no longer exported: drivers now implement \`riverqueue/unstable-driver\`
        // TODO(riverqueue-0.1): \`uniqueBitmaskFromStates\` is no longer exported: it moved to \`riverqueue/unstable-driver\`
        import {
          Client,
          type Driver,
          type JobArgs,
          uniqueBitmaskFromStates,
        } from "riverqueue";

        // TODO(riverqueue-0.1): \`JobArgs\` was removed; accept a \`JobDefinition\` and its args, or an \`InsertManyItem\`
        export function enqueue(client: Client, args: JobArgs, driver: Driver) {
          return uniqueBitmaskFromStates(["available"]);
        }
      `);
    });

    it("preserves CRLF line endings", () => {
      const input = code`
        import type { JobArgs } from "riverqueue";

        class PingArgs implements JobArgs {
          kind = "ping";
        }
      `.replaceAll("\n", "\r\n");

      expect(migrateOne(input)).toBe(
        code`
          import { defineJob } from "riverqueue";

          const ping = defineJob({
            kind: "ping",
          });
        `.replaceAll("\n", "\r\n")
      );
    });
  });

  it("migrates the riverqueue 0.1 fixture to the recorded output", () => {
    const fixture = new URL("../../fixtures/migration-0.1/", import.meta.url);
    const results = migrate({
      "consumer.ts": readFileSync(new URL("before.ts.txt", fixture), "utf8"),
    });

    expect(results.get("consumer.ts")?.output).toBe(
      readFileSync(new URL("codemod.ts.txt", fixture), "utf8")
    );
    expect(results.get("consumer.ts")?.sites).toEqual([
      {
        line: 19,
        message:
          "`JobRow.id` is now a `bigint`; review number annotations, arithmetic, and JSON serialization",
      },
    ]);
  });
});
