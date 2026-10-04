import { describe, expect, it } from "vitest";

import { loadMigrations } from "./index.js";

describe("loadMigrations", () => {
  it.each(["postgres", "sqlite"] as const)(
    "loads the complete %s main line",
    (backend) => {
      const migrations = loadMigrations(backend);

      expect(migrations.map(({ version }) => version)).toEqual([
        1, 2, 3, 4, 5, 6, 7, 8,
      ]);
      for (const migration of migrations) {
        expect(migration.name).not.toBe("");
        expect(migration.upSql.trim()).not.toBe("");
        expect(migration.downSql.trim()).not.toBe("");
      }
    }
  );
});
