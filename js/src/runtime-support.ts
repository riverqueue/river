import { ConfigurationError } from "./errors.js";

const REQUIREMENTS_URL =
  "https://github.com/riverqueue/river/tree/master/js#requirements";
const TEMPORAL_SETUP_URL =
  "https://github.com/riverqueue/river/tree/master/js#temporal-on-nodejs-24";

/**
 * Verify the runtime features River relies on before database work begins.
 *
 * Runtimes without native Temporal must preload an application-provided
 * polyfill before importing River, including in worker threads. River checks
 * these requirements on import and when constructing a client.
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
    | null
    | undefined;
  if (
    temporal == null ||
    typeof temporal.Instant?.from !== "function" ||
    typeof temporal.Instant.fromEpochNanoseconds !== "function" ||
    typeof temporal.Now?.instant !== "function"
  ) {
    throw new ConfigurationError(
      "River requires the Temporal API on globalThis; use a runtime with " +
        "native Temporal or preload a Temporal polyfill before importing River " +
        `(see ${TEMPORAL_SETUP_URL})`
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
