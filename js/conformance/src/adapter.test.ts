import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { Client } from "riverqueue";
import { afterEach, describe, expect, it, onTestFinished, vi } from "vitest";

import { PortableSqliteAdapter } from "./adapter.js";
import { ContractParams } from "./contract.js";
import { ADAPTER_ERROR_CODE, adapterErrorCode } from "./errors.js";
import type { PortableStorageProfile } from "./profile.js";

/** Go's SQLite time format, `2006-01-02 15:04:05.000`. */
const SQLITE_TIME = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/;

const METHODS = [
  "cancel",
  "clock_set",
  "cron_next",
  "delete",
  "delete_many",
  "get",
  "handshake",
  "insert",
  "insert_many",
  "list",
  "migrate",
  "raw_insert_exact_json",
  "raw_job_exact_json",
  "raw_job_row",
  "raw_job_timestamps",
  "reset",
  "retry",
  "retry_delay",
  "rng_seed",
  "tx_begin",
  "tx_cancel",
  "tx_commit",
  "tx_delete",
  "tx_delete_many",
  "tx_get",
  "tx_insert",
  "tx_insert_many",
  "tx_list",
  "tx_retry",
  "tx_rollback",
  "tx_update",
  "unique_key",
  "update",
] as const;

const PROFILE: PortableStorageProfile = {
  adapterVersion: 16,
  backend: "sqlite",
  capabilities: [],
  implementationVersion: "test",
  latestMigration: 7,
  methods: METHODS,
  migrationLine: "main",
  name: "portable-storage-v1",
  params: new ContractParams({
    $defs: {
      insert_opts: {
        additionalProperties: false,
        properties: {
          metadata: { type: "object" },
          priority: { type: "integer" },
          tags: { items: { type: "string" }, type: "array" },
        },
        type: "object",
      },
    },
    methods: [
      {
        name: "handshake",
        params: { additionalProperties: false, properties: {}, type: "object" },
      },
      {
        name: "insert",
        params: {
          additionalProperties: false,
          properties: {
            message: { type: "string" },
            opts: { $ref: "#/$defs/insert_opts" },
          },
          type: "object",
        },
      },
    ],
  }),
  protocolRevision: 1,
  repository: "/fixture",
};

function withCode(code: number): unknown {
  return expect.objectContaining({ code });
}

describe("PortableSqliteAdapter", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("advertises its exact profile and rejects methods outside it", async () => {
    const adapter = setup();

    expect(await adapter.dispatch("handshake", {})).toEqual({
      adapter_version: 16,
      backend: "sqlite",
      capabilities: [],
      implementation: "javascript",
      implementation_version: "test",
      methods: METHODS,
      migration_lines: { main: 7 },
      profile: "portable-storage-v1",
      protocol_revision: 1,
    });
    // The runtime methods belong to another profile.
    await expect(adapter.dispatch("start", {})).rejects.toThrow(
      withCode(ADAPTER_ERROR_CODE.methodNotFound)
    );
  });

  it("evaluates cron expressions in the reference time's offset", async () => {
    const adapter = setup();

    await expect(
      adapter.dispatch("cron_next", {
        count: 2,
        expression: "CRON_TZ=UTC 0 9 * * *",
        from: "2026-03-07T08:00:00-05:00",
      })
    ).resolves.toEqual({
      next: ["2026-03-08T04:00:00-05:00", "2026-03-09T04:00:00-05:00"],
    });
    await expect(
      adapter.dispatch("cron_next", {
        count: 1,
        expression: "0 9 * * 7",
        from: "2026-01-02T03:04:05Z",
      })
    ).rejects.toMatchObject({ code: -32_002 });
  });

  it("rejects params the contract does not declare, nested ones too", async () => {
    const adapter = setup();

    await expect(
      adapter.dispatch("handshake", { unexpected: true })
    ).rejects.toThrow(withCode(ADAPTER_ERROR_CODE.invalidParams));
    await expect(
      adapter.dispatch("insert", {
        message: "unknown option",
        opts: { not_an_option: true },
      })
    ).rejects.toThrow(withCode(ADAPTER_ERROR_CODE.invalidParams));
  });

  it("classifies missing rows and River rejections", async () => {
    const adapter = setup();
    await adapter.dispatch("migrate", {});

    await expect(adapter.dispatch("get", { id: 1 })).rejects.toThrow(
      withCode(ADAPTER_ERROR_CODE.notFound)
    );
    // River itself validates the priority, so the failure is a rejection
    // rather than malformed params.
    const failure = await adapter
      .dispatch("insert", { message: "invalid", opts: { priority: 99 } })
      .then(
        () => undefined,
        (error: unknown) => error
      );
    expect(adapterErrorCode(failure)).toBe(ADAPTER_ERROR_CODE.rejected);
  });

  it("implements atomic CRUD, output merge, and safe bulk deletion", async () => {
    const adapter = setup();
    await adapter.dispatch("migrate", {});
    const inserted = (await adapter.dispatch("insert", {
      message: "one",
      opts: { metadata: { retained: true }, tags: ["bulk"] },
    })) as Record<string, unknown>;
    const second = (await adapter.dispatch("insert", {
      message: "two",
      opts: { tags: ["bulk"] },
    })) as Record<string, unknown>;

    const updated = (await adapter.dispatch("update", {
      id: inserted.id,
      output: { ok: true },
    })) as { metadata: Record<string, unknown> };
    expect(updated.metadata).toEqual({ output: { ok: true }, retained: true });
    await expect(adapter.dispatch("delete_many", {})).rejects.toThrow(
      /requires a filter or all(?:: true|=true)/
    );

    const deleted = (await adapter.dispatch("delete_many", {
      ids: [second.id, inserted.id],
    })) as { jobs: readonly Record<string, unknown>[] };
    expect(deleted.jobs.map(({ id }) => id)).toEqual([inserted.id, second.id]);
  });

  it("routes ordinary storage behavior through the public Client", async () => {
    const adapter = setup();
    await adapter.dispatch("migrate", {});
    // Drivers expose no operations, so listing and updating can only go
    // through the client too.
    const insertSpy = vi.spyOn(Client.prototype, "insert");

    const inserted = (await adapter.dispatch("insert", {
      message: "public API",
    })) as Record<string, unknown>;
    await adapter.dispatch("update", {
      id: inserted.id,
      output: null,
    });
    await adapter.dispatch("list", {});

    expect(insertSpy).toHaveBeenCalledOnce();
  });

  it("reports exact JSON tokens after driver decoding", async () => {
    const adapter = setup();
    await adapter.dispatch("migrate", {});
    const inserted = (await adapter.dispatch("raw_insert_exact_json", {})) as {
      id: bigint;
    };

    await expect(
      adapter.dispatch("raw_job_exact_json", { id: inserted.id })
    ).resolves.toEqual({
      decimal: "0.12345678901234567890123456789",
      integer: "9223372036854775807",
      negative: "-9223372036854775808",
    });
  });

  it("reads a job's stored JSON and timestamp text", async () => {
    const adapter = setup();
    await adapter.dispatch("migrate", {});
    const inserted = (await adapter.dispatch("raw_insert_exact_json", {})) as {
      id: bigint;
    };

    // Raw SQL leaves both times to the schema's `CURRENT_TIMESTAMP` default.
    const exact = (await adapter.dispatch("raw_job_row", {
      id: inserted.id,
    })) as Record<string, string | null>;
    expect(exact).toEqual({
      args: '{"decimal":0.12345678901234567890123456789,"integer":9223372036854775807}',
      attempted_at: null,
      attempted_by: null,
      created_at: expect.stringMatching(
        /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/
      ),
      errors: null,
      finalized_at: null,
      // The stored JSONB: an object of a TEXT key and a FLOAT, and a TEXT key
      // and an INT, and the schema's empty tags array.
      jsonb: {
        args: "CC4677646563696D616CC51F302E313233343536373839303132333435363738393031323334353637383977696E7465676572C31339323233333732303336383534373735383037",
        attempted_by: null,
        errors: null,
        metadata:
          "CC1F876E65676174697665C3142D39323233333732303336383534373735383038",
        tags: "0B",
      },
      metadata: '{"negative":-9223372036854775808}',
      scheduled_at: exact.created_at,
      tags: "[]",
      unique_key: null,
      unique_key_type: null,
      unique_states: null,
      unique_states_type: null,
    });
    await expect(
      adapter.dispatch("raw_job_row", { id: inserted.id + 1n })
    ).rejects.toThrow(withCode(ADAPTER_ERROR_CODE.notFound));

    // Like Go's returning insert, the client stores a nonce in every row it
    // inserts, including one without a unique key.
    const job = (await adapter.dispatch("insert", {
      message: "no unique key",
    })) as { id: bigint };
    const raw = (await adapter.dispatch("raw_job_row", { id: job.id })) as {
      created_at: string;
      metadata: string;
      scheduled_at: string;
    };
    expect(raw.metadata).toMatch(/^\{"river:unique_nonce":"[0-9a-f]{16}"\}$/);
    expect(raw.created_at).toMatch(SQLITE_TIME);
    expect(raw.scheduled_at).toBe(raw.created_at);
  });

  it("retains huge metadata numbers through SQLite read and update", async () => {
    const adapter = setup();
    await adapter.dispatch("migrate", {});
    const metadataJson =
      '{"negative":-9223372036854775808,"big_integer":123456789012345678901234567890,"beyond_float":1e400,"long_decimal":0.1000000000000000055511151231257827}';
    const inserted = (await adapter.dispatch("raw_insert_exact_json", {
      metadata_json: metadataJson,
    })) as { id: bigint };
    const expected = {
      beyond_float: "1e400",
      big_integer: "123456789012345678901234567890",
      decimal: "0.12345678901234567890123456789",
      integer: "9223372036854775807",
      long_decimal: "0.1000000000000000055511151231257827",
      negative: "-9223372036854775808",
    };
    await expect(
      adapter.dispatch("raw_job_exact_json", { id: inserted.id })
    ).resolves.toEqual(expected);
    await adapter.dispatch("update", {
      id: inserted.id,
      output: { updated: true },
    });
    await expect(
      adapter.dispatch("raw_job_exact_json", { id: inserted.id })
    ).resolves.toEqual(expected);
  });

  it("keeps named transactions isolated and batch errors atomic", async () => {
    const adapter = setup();
    await adapter.dispatch("migrate", {});
    await adapter.dispatch("tx_begin", { handle: "rollback" });
    const rolledBack = (await adapter.dispatch("tx_insert", {
      handle: "rollback",
      job: { message: "rolled back" },
    })) as Record<string, unknown>;
    await adapter.dispatch("tx_rollback", { handle: "rollback" });
    await expect(
      adapter.dispatch("get", { id: rolledBack.id })
    ).rejects.toThrow(withCode(ADAPTER_ERROR_CODE.notFound));

    await adapter.dispatch("tx_begin", { handle: "savepoint" });
    await expect(
      adapter.dispatch("tx_insert_many", {
        handle: "savepoint",
        jobs: [
          { message: "must roll back", opts: { tags: ["atomic"] } },
          { message: "invalid", opts: { priority: 99 } },
        ],
      })
    ).rejects.toThrow("priority");
    await adapter.dispatch("tx_commit", { handle: "savepoint" });
    const listed = (await adapter.dispatch("list", {
      tags_all: ["atomic"],
    })) as { jobs: readonly unknown[] };
    expect(listed.jobs).toEqual([]);
  });
});

function setup(): PortableSqliteAdapter {
  // The adapter's driver opens its own connection to the database file.
  const directory = mkdtempSync(join(tmpdir(), "river-conformance-"));
  const database = new DatabaseSync(join(directory, "river.db"));
  const adapter = new PortableSqliteAdapter(database, PROFILE);
  onTestFinished(async () => {
    await adapter.close();
    database.close();
    rmSync(directory, { force: true, recursive: true });
  });
  return adapter;
}
