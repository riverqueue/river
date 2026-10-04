import { createHash } from "node:crypto";
import { Buffer } from "node:buffer";
import { cron, type JobState, type JsonObject } from "riverqueue";
import {
  buildUniqueKey,
  uniqueBitmaskFromStates,
} from "riverqueue/unstable-driver";

import { invalidParams, rejected } from "./errors.js";

const MAX_INT64 = 9_223_372_036_854_775_807n;
const MAX_UINT32 = 4_294_967_295;
const NANOSECONDS_PER_SECOND = 1_000_000_000n;

export interface UniqueKeyRequest {
  readonly args: unknown;
  readonly kind: string;
  readonly now: string;
  readonly options: {
    readonly by_args?: boolean;
    readonly by_period_nanos?: bigint | number;
    readonly by_queue?: boolean;
    readonly by_state?: readonly JobState[];
    readonly exclude_kind?: boolean;
  };
  readonly queue: string;
  readonly scheduled_at: string | null;
  readonly selected_unique_components?: readonly (readonly string[])[] | null;
  readonly selected_unique_paths: readonly string[] | null;
}

/** Read literal field components without depending on Go's path escaping. */
export function selectedUniqueComponents(
  params: Record<string, unknown>
): readonly (readonly string[])[] | null {
  const value = params.selected_unique_components;
  if (value === undefined || value === null) return null;
  if (
    !Array.isArray(value) ||
    !value.every(
      (components: unknown) =>
        Array.isArray(components) &&
        components.length > 0 &&
        components.every((segment: unknown) => typeof segment === "string")
    )
  ) {
    throw invalidParams("selected_unique_components must be arrays of strings");
  }
  return value as readonly (readonly string[])[];
}

/**
 * The `cron_next` adapter method: up to `count` successive occurrences of a
 * River Go cron expression after an RFC 3339 `from`, evaluated and formatted
 * in `from`'s own offset like River Go's `schedule.Next`. An expression River
 * Go rejects is a `rejected` JSON-RPC error.
 */
export function deterministicCronNext(params: Record<string, unknown>): {
  readonly next: readonly string[];
} {
  const { count, expression, from } = params;
  if (typeof expression !== "string") {
    throw invalidParams("expression must be a string");
  }
  if (typeof from !== "string") throw invalidParams("from must be a string");
  if (!Number.isSafeInteger(count) || (count as number) < 1) {
    throw invalidParams("count must be positive");
  }
  const offset = /(?:Z|[+-]\d{2}:\d{2})$/i.exec(from)?.[0].toUpperCase();
  if (offset === undefined) {
    throw invalidParams("from must be an RFC 3339 time with an offset");
  }
  const timeZone = offset === "Z" ? "UTC" : offset;
  let current = Temporal.Instant.from(from);

  const next: string[] = [];
  try {
    const schedule = cron(expression, { timeZone });
    while (next.length < (count as number)) {
      const occurrence = schedule.next(current);
      if (occurrence === null) break;
      next.push(
        occurrence
          .toZonedDateTimeISO(timeZone)
          .toString({ timeZoneName: "never" })
          // Go's RFC 3339 writes a zero offset as `Z`.
          .replace(/\+00:00$/, "Z")
      );
      current = occurrence;
    }
  } catch (error: unknown) {
    throw rejected(error instanceof Error ? error.message : String(error), {
      cause: error,
    });
  }
  return { next };
}

/** Match River's deterministic retry jitter without a process-global RNG. */
export function deterministicRetryDelayNanoseconds(options: {
  errorCount: number;
  jobId: bigint;
  now: Temporal.Instant;
  seed: bigint;
}): bigint {
  if (!Number.isInteger(options.errorCount) || options.errorCount < 1) {
    throw new RangeError("error_count must be positive");
  }
  const baseSeconds = options.errorCount ** 4;
  const baseNanoseconds = baseSeconds * 1_000_000_000;
  if (
    !Number.isFinite(baseNanoseconds) ||
    baseNanoseconds >= Number(MAX_INT64)
  ) {
    return MAX_INT64;
  }

  const input = Buffer.alloc(28);
  input.writeBigUInt64BE(BigInt.asUintN(64, options.seed), 0);
  input.writeBigUInt64BE(BigInt.asUintN(64, options.jobId), 8);
  input.writeUInt32BE(options.errorCount, 16);
  input.writeBigUInt64BE(BigInt.asUintN(64, options.now.epochNanoseconds), 20);
  const sample = createHash("sha256").update(input).digest().readUInt32BE(0);
  const ratio = sample / MAX_UINT32;
  return BigInt(Math.round(baseNanoseconds * (0.9 + ratio * 0.2)));
}

/** Calculate unique fixtures through the exact insertion implementation. */
export function deterministicUniqueKey(request: UniqueKeyRequest): {
  readonly sha256: string;
  readonly state_mask: number;
} {
  const period = BigInt(request.options.by_period_nanos ?? 0);
  const selectedPaths =
    request.selected_unique_components?.map((components) =>
      components
        .map((component) =>
          component.replaceAll("\\", "\\\\").replaceAll(".", "\\.")
        )
        .join(".")
    ) ?? request.selected_unique_paths;
  const [key, states] = buildUniqueKey(
    {
      args: request.args as JsonObject,
      kind: request.kind,
      queue: request.queue,
      scheduledAt: Temporal.Instant.from(request.scheduled_at ?? request.now),
    },
    {
      ...(request.options.by_args === true
        ? {
            byArgs: selectedPaths?.length ? selectedPaths : (true as const),
          }
        : {}),
      ...(period > 0n
        ? {
            byPeriod: Temporal.Duration.from({
              seconds: Number(period / NANOSECONDS_PER_SECOND),
              nanoseconds: Number(period % NANOSECONDS_PER_SECOND),
            }),
          }
        : {}),
      ...(request.options.by_queue === undefined
        ? {}
        : { byQueue: request.options.by_queue }),
      ...(request.options.by_state === undefined
        ? {}
        : { byState: request.options.by_state }),
      ...(request.options.exclude_kind === undefined
        ? {}
        : { excludeKind: request.options.exclude_kind }),
    }
  );
  return {
    sha256: Buffer.from(key).toString("hex"),
    state_mask: Number.parseInt(uniqueBitmaskFromStates(states), 2),
  };
}
