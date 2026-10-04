import { MigrationError } from "riverqueue";
import { describe, expect, it } from "vitest";

import type { Migration } from "./bundle.js";
import {
  planMigrations,
  validatePlanOptions,
  versionRecord,
  type MigrationDirection,
  type MigrationPlanOptions,
} from "./plan.js";

const MIGRATIONS: readonly Migration[] = [1, 2, 3, 4, 5].map((version) => ({
  downSql: `-- down ${version}`,
  name: `migration_${version}`,
  upSql: `-- up ${version}`,
  version,
}));

function plan(
  direction: MigrationDirection,
  applied: readonly number[],
  options: Partial<MigrationPlanOptions> = {}
): readonly number[] {
  const normalized = {
    maxSteps: options.maxSteps,
    targetVersion: options.targetVersion,
  };
  validatePlanOptions("sqlite", MIGRATIONS, direction, normalized);
  return planMigrations(
    "sqlite",
    MIGRATIONS,
    direction,
    normalized,
    applied
  ).map(({ version }) => version);
}

describe("planMigrations", () => {
  it("applies every missing version up by default", () => {
    expect(plan("up", [])).toEqual([1, 2, 3, 4, 5]);
    expect(plan("up", [1, 2])).toEqual([3, 4, 5]);
    expect(plan("up", [1, 2, 3, 4, 5])).toEqual([]);
  });

  it("migrates up to and including a target version", () => {
    expect(plan("up", [1], { targetVersion: 3 })).toEqual([2, 3]);
    expect(plan("up", [1, 2, 3], { targetVersion: 2 })).toEqual([]);
  });

  it("does nothing up to a target that is already applied", () => {
    expect(plan("up", [1, 2], { targetVersion: 2 })).toEqual([]);
    expect(plan("up", [1, 2], { maxSteps: 1, targetVersion: 1 })).toEqual([]);
    // Even a missing version below the target stays missing.
    expect(plan("up", [1, 3], { targetVersion: 3 })).toEqual([]);
    expect(plan("up", [1, 3], { targetVersion: 4 })).toEqual([2, 4]);
  });

  it("limits steps in either direction", () => {
    expect(plan("up", [], { maxSteps: 2 })).toEqual([1, 2]);
    expect(plan("up", [], { maxSteps: 0 })).toEqual([]);
    expect(plan("down", [1, 2, 3, 4], { maxSteps: 3 })).toEqual([4, 3, 2]);
    expect(
      plan("down", [1, 2, 3, 4], { maxSteps: 1, targetVersion: 0 })
    ).toEqual([4]);
  });

  it("reverts one version down by default", () => {
    expect(plan("down", [1, 2, 3])).toEqual([3]);
    expect(plan("down", [])).toEqual([]);
  });

  it("reverts versions above a target, or all of them for target 0", () => {
    expect(plan("down", [1, 2, 3, 4, 5], { targetVersion: 2 })).toEqual([
      5, 4, 3,
    ]);
    expect(plan("down", [1, 2, 3], { targetVersion: 3 })).toEqual([]);
    expect(plan("down", [1, 2, 3], { targetVersion: 0 })).toEqual([3, 2, 1]);
    expect(plan("down", [], { targetVersion: 0 })).toEqual([]);
  });

  it("rejects a down target that is not applied", () => {
    expect(() => plan("down", [1, 2], { targetVersion: 4 })).toThrow(
      "cannot migrate down to version 4 because it is not applied"
    );
  });

  it("ignores applied versions that this package does not know", () => {
    expect(plan("up", [1, 2, 3, 4, 5, 6])).toEqual([]);
    expect(plan("up", [1, 2, 6])).toEqual([3, 4, 5]);
    expect(plan("down", [1, 2, 3, 4, 5, 6])).toEqual([5]);
    expect(plan("down", [1, 2, 3, 4, 5, 6], { targetVersion: 0 })).toEqual([
      5, 4, 3, 2, 1,
    ]);
  });
});

describe("validatePlanOptions", () => {
  it.each([
    ["up", { maxSteps: -1 }, "maxSteps must be a non-negative integer"],
    ["up", { maxSteps: 1.5 }, "maxSteps must be a non-negative integer"],
    ["down", { targetVersion: -1 }, "targetVersion must be a non-negative"],
    ["up", { targetVersion: 0 }, "only valid when migrating down"],
    ["up", { targetVersion: 6 }, "available versions: 1, 2, 3, 4, 5"],
    ["down", { targetVersion: 9 }, "version 9 is not a migration version"],
  ] as const)("rejects %s %j", (direction, options, message) => {
    const run = () =>
      validatePlanOptions("postgres", MIGRATIONS, direction, {
        maxSteps: undefined,
        targetVersion: undefined,
        ...options,
      });

    expect(run).toThrow(MigrationError);
    expect(run).toThrow(message);
  });
});

describe("versionRecord", () => {
  it("omits the line column for main versions that predate it", () => {
    expect(versionRecord("up", "main", 4)).toEqual({
      kind: "insert_without_line",
      version: 4,
    });
    expect(versionRecord("up", "main", 5)).toEqual({
      kind: "insert",
      line: "main",
      version: 5,
    });
    expect(versionRecord("down", "main", 5)).toEqual({
      kind: "delete_without_line",
      version: 5,
    });
    expect(versionRecord("down", "main", 6)).toEqual({
      kind: "delete",
      line: "main",
      version: 6,
    });
  });

  it("skips bookkeeping after main version 1 drops the table", () => {
    expect(versionRecord("down", "main", 1)).toEqual({ kind: "none" });
  });

  it("always uses the line column for other lines", () => {
    expect(versionRecord("up", "extra", 1)).toEqual({
      kind: "insert",
      line: "extra",
      version: 1,
    });
    expect(versionRecord("down", "extra", 1)).toEqual({
      kind: "delete",
      line: "extra",
      version: 1,
    });
  });
});
