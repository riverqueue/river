import { describe, expect, it } from "vitest";

import {
  postgresCapabilities,
  postgresCapabilitiesFromRow,
  uniqueInsertConflictSql,
} from "./postgres-capabilities.js";

describe("postgresCapabilities", () => {
  it.each([
    ["PostgreSQL 15.12", 150_012, false, true, "xmax"],
    ["PostgreSQL 17.4 on aarch64-apple-darwin", 170_004, false, true, "xmax"],
    ["PostgreSQL 18.0", 180_000, false, true, "returning_old"],
    [
      "PostgreSQL 15.12-YB-2025.2.1.0-b1",
      150_012,
      false,
      false,
      "metadata_nonce",
    ],
    [
      "PostgreSQL 15.12-YB-2025.2.3.0-b1",
      150_012,
      false,
      false,
      "metadata_nonce",
    ],
    [
      "PostgreSQL 15.12-YB-2025.2.3.0-b1",
      150_012,
      true,
      true,
      "metadata_nonce",
    ],
    ["YugabyteDB", 150_012, false, false, "metadata_nonce"],
  ] as const)(
    "detects %s (%i, yb_enable_listen_notify %s) like River for Go",
    (product, versionNum, ybListenNotify, listenNotify, mode) => {
      expect(postgresCapabilities(product, versionNum, ybListenNotify)).toEqual(
        { supportsListenNotify: listenNotify, uniqueInsertMode: mode }
      );
    }
  );

  it("decodes a detection row whose version arrives as text", () => {
    expect(
      postgresCapabilitiesFromRow({
        product: "PostgreSQL 18.1",
        version_num: "180001",
        yb_listen_notify_enabled: false,
      })
    ).toEqual({
      supportsListenNotify: true,
      uniqueInsertMode: "returning_old",
    });
    expect(() =>
      postgresCapabilitiesFromRow({
        product: null,
        version_num: 180_001,
        yb_listen_notify_enabled: false,
      })
    ).toThrow(TypeError);
  });

  it("renders each mode's conflict expression", () => {
    expect(uniqueInsertConflictSql("metadata_nonce")).toBe("false");
    expect(uniqueInsertConflictSql("returning_old")).toBe(
      "(OLD.id IS NOT NULL)"
    );
    expect(uniqueInsertConflictSql("xmax")).toBe("(xmax != 0)");
  });
});
