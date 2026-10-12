import "./internal/temporal.js";

import { ConfigurationError } from "./errors.js";

const REQUIREMENTS_URL =
  "https://github.com/riverqueue/river/tree/master/js#requirements";

/**
 * Verify the runtime features River relies on before database work begins.
 *
 * River provides a Temporal polyfill when the runtime has no native
 * implementation. This check gives unsupported runtimes a useful failure
 * instead of a later `ReferenceError` or a lossy timestamp fallback.
 */
export function assertRuntimeSupport(): void {
  const major = Number.parseInt(process.versions.node.split(".")[0] ?? "", 10);
  if (!Number.isSafeInteger(major) || major < 24) {
    throw new ConfigurationError(
      `River requires Node.js 24 or newer; found ${process.versions.node} ` +
        `(see ${REQUIREMENTS_URL})`
    );
  }

  const temporal = Reflect.get(globalThis, "Temporal") as
    | {
        Instant?: {
          from?: unknown;
          fromEpochNanoseconds?: unknown;
        };
        Now?: { instant?: unknown };
      }
    | undefined;
  if (
    temporal === undefined ||
    typeof temporal.Instant?.from !== "function" ||
    typeof temporal.Instant.fromEpochNanoseconds !== "function" ||
    typeof temporal.Now?.instant !== "function"
  ) {
    throw new ConfigurationError(
      "River requires the Temporal API; keep the native implementation " +
        "or the polyfill installed by River available on globalThis " +
        `(see ${REQUIREMENTS_URL})`
    );
  }

  const rawJSON: unknown = Reflect.get(JSON, "rawJSON");
  const isRawJSON: unknown = Reflect.get(JSON, "isRawJSON");
  if (typeof rawJSON !== "function" || typeof isRawJSON !== "function") {
    throw new ConfigurationError(
      "River requires a Node.js build with native JSON raw-number support " +
        `(see ${REQUIREMENTS_URL})`
    );
  }
}
