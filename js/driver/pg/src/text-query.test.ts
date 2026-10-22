import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import { postgresTextQuery } from "./text-query.js";

describe("Postgres text-result decoding", () => {
  it("keeps large IDs, JSON numbers, error text, and microsecond timestamps exact", () => {
    const row = postgresTextQuery("SELECT 1").decode(`{
      "id": 9223372036854775807,
      "args": {"id": 9223372036854775807},
      "created_at": "2026-10-17T12:34:56.123456+02:00",
      "errors": [{"attempt": 1, "error": "prior error", "extra": 9223372036854775807}, null],
      "finalized_at": null,
      "unique_key": "\\\\x00ff",
      "unique_states": "11111111"
    }`) as Record<string, unknown>;
    expect(row.id).toBe(9_223_372_036_854_775_807n);
    expect(JSON.stringify(row.args)).toBe('{"id":9223372036854775807}');
    expect(row.created_at).toEqual(
      Temporal.Instant.from("2026-10-17T10:34:56.123456Z")
    );
    expect(row.errors).toEqual([
      '{"attempt":1,"error":"prior error","extra":9223372036854775807}',
      null,
    ]);
    expect(row.finalized_at).toBeNull();
    expect(row.unique_key).toEqual(Buffer.from([0, 255]));
    expect(row.unique_states).toBe("11111111");
  });

  it("preserves explicitly selected text IDs and escape-format bytes", () => {
    const row = postgresTextQuery("SELECT 1").decode(
      JSON.stringify({ id: "9223372036854775807", unique_key: "\\000\\377" })
    );
    expect(row).toEqual({
      id: "9223372036854775807",
      unique_key: Buffer.from([0, 255]),
    });
  });
});
