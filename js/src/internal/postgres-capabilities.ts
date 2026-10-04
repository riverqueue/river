/**
 * Features of a PostgreSQL-compatible server that River adapts to, like
 * River for Go's `riverdriver.PostgresCapabilities`.
 *
 * YugabyteDB speaks PostgreSQL's protocol but has no `xmax` system column
 * and, unless configured for it, no `LISTEN`/`NOTIFY`. River's PostgreSQL
 * drivers detect the server once and cache the result for the driver's
 * lifetime, so enabling Yugabyte's notifications takes effect only for a new
 * driver, such as after a restart.
 */

/**
 * Reads the server's product, version, and Yugabyte notification setting,
 * and the session's `DateStyle`, which a driver reading timestamps as text
 * needs to be ISO. The functions are unqualified, as in River for Go, so
 * they resolve through the connection's `search_path`.
 */
export const POSTGRES_CAPABILITIES_SQL = `
  SELECT
    current_setting('DateStyle') AS date_style,
    version()::text AS product,
    current_setting('server_version_num')::int AS version_num,
    coalesce(current_setting('yb_enable_listen_notify', true), 'off')::boolean
      AS yb_listen_notify_enabled
`;

/**
 * How an insert that may conflict on its unique key tells a new row from an
 * existing one it returned instead:
 *
 * - `metadata_nonce`: each proposed row's metadata carries a random nonce
 *   under {@link UNIQUE_INSERT_NONCE_KEY}, and a returned row without the
 *   one its insert wrote already existed. Used where `xmax` is unavailable.
 * - `returning_old`: PostgreSQL 18's `OLD` row in `RETURNING`.
 * - `xmax`: PostgreSQL's `xmax` system column, nonzero for an updated row.
 */
export type UniqueInsertMode = "metadata_nonce" | "returning_old" | "xmax";

/** Features detected from a PostgreSQL-compatible server. */
export interface PostgresCapabilities {
  /**
   * Whether `pg_notify` delivers notifications to listeners. Without it,
   * River sends no notifications and clients poll instead.
   */
  readonly supportsListenNotify: boolean;
  readonly uniqueInsertMode: UniqueInsertMode;
}

/** The metadata key of a unique insert's nonce, shared with River for Go. */
export const UNIQUE_INSERT_NONCE_KEY = "river:unique_nonce";

/**
 * Derive capabilities from the server's `version()` text, its
 * `server_version_num`, and Yugabyte's `yb_enable_listen_notify` setting,
 * which reads as off when absent.
 */
export function postgresCapabilities(
  product: string,
  versionNum: number,
  ybListenNotifyEnabled: boolean
): PostgresCapabilities {
  const lower = product.toLowerCase();
  const yugabyte = lower.includes("-yb") || lower.includes("yugabyte");
  return Object.freeze({
    // Yugabyte's notifications need 2025.2.3 or later with
    // `ysql_yb_enable_listen_notify=true` on both masters and tservers.
    supportsListenNotify: !yugabyte || ybListenNotifyEnabled,
    uniqueInsertMode: yugabyte
      ? "metadata_nonce"
      : versionNum >= 180_000
        ? "returning_old"
        : "xmax",
  });
}

/**
 * Decode a row of {@link POSTGRES_CAPABILITIES_SQL}, whose `version_num`
 * may arrive as a number or a decimal string depending on the client's type
 * parsers.
 */
export function postgresCapabilitiesFromRow(row: {
  readonly product: unknown;
  readonly version_num: unknown;
  readonly yb_listen_notify_enabled: unknown;
}): PostgresCapabilities {
  const versionNum = Number(row.version_num);
  if (
    typeof row.product !== "string" ||
    !Number.isSafeInteger(versionNum) ||
    typeof row.yb_listen_notify_enabled !== "boolean"
  ) {
    throw new TypeError("unexpected PostgreSQL server capabilities row");
  }
  return postgresCapabilities(
    row.product,
    versionNum,
    row.yb_listen_notify_enabled
  );
}

/**
 * The SQL expression that's true for an existing row a unique insert
 * returned, in a `RETURNING` clause. It's always false for
 * `metadata_nonce`, which compares nonces after the insert instead.
 */
export function uniqueInsertConflictSql(mode: UniqueInsertMode): string {
  switch (mode) {
    case "metadata_nonce":
      return "false";
    case "returning_old":
      return "(OLD.id IS NOT NULL)";
    case "xmax":
      return "(xmax != 0)";
  }
}
