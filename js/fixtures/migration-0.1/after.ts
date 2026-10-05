import { Client, defineJob } from "riverqueue";

declare const client: Client;

const sort = defineJob({
  defaults: { queue: "sorting" },
  kind: "sort",
  decode(value) {
    const strings = value.strings;
    if (
      !Array.isArray(strings) ||
      !strings.every((item): item is string => typeof item === "string")
    ) {
      throw new TypeError("strings must be an array of strings");
    }
    return { strings };
  },
});

async function insertMigratedJobs() {
  const result = await client.insert(
    sort,
    { strings: ["b", "a"] },
    {
      scheduledAt: Temporal.Now.instant().add({ minutes: 1 }),
      unique: {
        byArgs: true,
        byPeriod: Temporal.Duration.from({ minutes: 1 }),
      },
    }
  );

  const id: bigint = result.job.id;
  if (result.status === "duplicate") console.log("duplicate", id.toString());

  await client.insertMany([
    // `JobArgsObject` had no insertion defaults, so this job used the default
    // queue rather than the `sort` definition's.
    {
      job: sort,
      args: { strings: ["c"] },
      options: { priority: 2, queue: "default" },
    },
    { job: sort, args: { strings: ["d"] } },
  ] as const);
}

void insertMigratedJobs;
