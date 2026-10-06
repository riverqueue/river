import { EventEmitter } from "node:events";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import type { Pool } from "pg";
import { describe, expect, it, onTestFinished, vi } from "vitest";

import { PgDatabase } from "../../driver/pg/src/database.js";
import { runtimeNotificationSubscribe } from "../../driver/pg/src/sql/notify.js";
import { testSqliteMemory } from "../../driver/sqlite/src/driver.js";
import type { RuntimeDriver, RuntimeNotification } from "../driver.js";
import { parseJson, stringifyJson, type JsonObject } from "../json.js";
import type { AttemptRunner } from "./attempt-runner.js";
import type { RuntimeContext } from "./context.js";
import { NotificationPump } from "./notification-pump.js";
import type { QueueProducer } from "./queue-producer.js";

interface NotificationFixture {
  readonly name: string;
  readonly payload: JsonObject;
  readonly topic: string;
}

const url = new URL(
  "../../../conformance/testdata/protocol_values.json",
  import.meta.url
);
const protocol = parseJson(
  await readFile(url, "utf8").catch((error: unknown) => {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(
        `missing conformance fixture ${fileURLToPath(url)}; run \`make generate/fixtures\` from the repository root`,
        { cause: error }
      );
    }
    throw error;
  })
) as unknown as {
  readonly notifications: readonly NotificationFixture[];
  readonly topics: Readonly<Record<RuntimeNotification["topic"], string>>;
};

it("covers every Go notification action", () => {
  expect(protocol.notifications.map(({ name }) => name).sort()).toEqual([
    "cancel",
    "insert",
    "metadata_changed",
    "pause",
    "request_resign",
    "resigned",
    "resume",
  ]);
});

describe.each(["postgres", "sqlite"] as const)(
  "Go notifications through %s",
  (backend) => {
    describe.each(["client-1", "observer"])("client %s", (clientId) => {
      it.each(protocol.notifications)("dispatches $name", async (fixture) => {
        const stop = new AbortController();
        const tasks: Promise<void>[] = [];
        onTestFinished(async () => {
          stop.abort();
          await Promise.allSettled(tasks);
        });
        const subscribe =
          backend === "postgres"
            ? postgresStream(fixture)
            : sqliteStream(fixture);
        const context = {
          claimSignal: stop.signal,
          clientId,
          driver: {
            async *runtimeNotificationSubscribe(topics, signal, ready) {
              for await (const notification of subscribe(
                topics,
                signal,
                ready
              )) {
                yield notification;
                // The pump has handled this one fixture. Stop without timers
                // or resubscribing; failures before delivery reject start().
                stop.abort();
              }
            },
          } satisfies Pick<RuntimeDriver, "runtimeNotificationSubscribe">,
          guard: (task: Promise<void>) => task,
          trackTask: (task: Promise<void>) => {
            tasks.push(task);
          },
        } as unknown as RuntimeContext;

        const effects: unknown[][] = [];
        const producer = {
          refresh: (queue: string) => {
            effects.push(["refresh", queue]);
          },
          refreshAll: () => {
            effects.push(["refreshAll"]);
          },
          wake: (queue: string) => {
            effects.push(["wake", queue]);
          },
          wakeAll: () => {
            effects.push(["wakeAll"]);
          },
        } as unknown as QueueProducer;
        const runner = {
          cancelAttempt: (id: bigint, owner: string) => {
            effects.push(["cancel", id, owner]);
          },
        } as unknown as AttemptRunner;
        const pump = new NotificationPump(context, {
          leaderResigned: () => {
            effects.push(["leaderResigned"]);
          },
          producer,
          resignLeadership: () => {
            effects.push(["resignLeadership"]);
            return undefined;
          },
          runner,
        });

        await pump.start(true);
        await Promise.all(tasks);

        // Semantic expectations are independent of the fixture's payload.
        // Checking all effects also rules out unrelated cancellations/wakeups.
        const expected: Record<string, unknown[][]> = {
          cancel: [
            ["cancel", 42n, clientId],
            ["refresh", "priority"],
          ],
          insert: [["wake", "priority"]],
          metadata_changed: [["refresh", "priority"]],
          pause: [["refresh", "priority"]],
          request_resign: [["resignLeadership"]],
          resigned: clientId === "client-1" ? [] : [["leaderResigned"]],
          resume: [["refresh", "priority"]],
        };
        expect(effects).toEqual(expected[fixture.name]);
      });
    });
  }
);

type Subscribe = NonNullable<RuntimeDriver["runtimeNotificationSubscribe"]>;

/** Exercise LISTEN channel naming and decoding with only the socket faked. */
function postgresStream(fixture: NotificationFixture): Subscribe {
  const client = Object.assign(new EventEmitter(), {
    query: vi.fn().mockResolvedValue({ rows: [] }),
    release: vi.fn(),
  });
  const pool = {
    connect: async () => client,
    idleCount: 0,
    totalCount: 0,
  } as unknown as Pool;
  const database = new PgDatabase(pool, "conformance");
  return (topics, signal, ready) =>
    runtimeNotificationSubscribe(database, topics, signal, () => {
      expect(client.query.mock.calls.map(([sql]) => sql).sort()).toEqual(
        Object.values(protocol.topics)
          .map((topic) => `LISTEN "conformance.${topic}"`)
          .sort()
      );
      ready();
      client.emit("notification", {
        channel: `conformance.${fixture.topic}`,
        payload: stringifyJson(fixture.payload),
      });
    });
}

/** Exercise outbox topic filtering and decoding with database reads faked. */
function sqliteStream(fixture: NotificationFixture): Subscribe {
  const driver = testSqliteMemory();
  onTestFinished(() => driver.close());
  vi.spyOn(driver, "notificationLastId").mockResolvedValue(0n);
  vi.spyOn(driver, "notificationPoll").mockImplementation(async (params) => {
    expect([...(params?.topics ?? [])].sort()).toEqual(
      Object.values(protocol.topics).sort()
    );
    return [
      {
        createdAt: Temporal.Now.instant(),
        id: 1n,
        topic: fixture.topic,
        payload: stringifyJson(fixture.payload),
      },
    ];
  });
  return driver.runtimeNotificationSubscribe.bind(driver);
}
