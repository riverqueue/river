/**
 * Supervision of a pilot's background services: each runs independently,
 * and one that fails or returns early restarts after backoff.
 */
import { ExtensionError } from "../errors.js";
import {
  BACKGROUND_BACKOFF,
  exponentialBackoffMs,
  type RuntimeTimer,
} from "../internal/backoff.js";
import type { InternalLogger } from "../logger.js";
import type { PilotService } from "../pilot.js";
import { describeError } from "./failures.js";

/**
 * How long a service must run before a failure starts its backoff over,
 * so a service that keeps failing quickly backs off further each time.
 */
const HEALTHY_RUN_MS = 60_000;

/** What {@link superviseService} needs from its runtime. */
export interface SupervisorOptions {
  readonly logger: Pick<InternalLogger, "error">;
  readonly random: () => number;
  readonly timer: RuntimeTimer;
}

/**
 * Run `service` until `signal` aborts. A run that rejects, or resolves
 * before `signal` aborted, is logged and restarted after capped
 * exponential backoff with jitter, once it has settled. Backoff ends at
 * once when `signal` aborts, and nothing restarts after that.
 */
export async function superviseService<Term>(
  service: PilotService<Term>,
  term: Term,
  signal: AbortSignal,
  options: SupervisorOptions
): Promise<void> {
  let failures = 0;
  while (!signal.aborted) {
    const startedAt = options.timer.now();
    let failure: unknown;
    try {
      await service.run({ signal, term });
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
      if (signal.aborted) return;
      failure = new ExtensionError(
        `service ${service.name} returned while it should still run`
      );
    } catch (error: unknown) {
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the signal can abort while awaiting
      if (signal.aborted) return;
      failure = error;
    }
    failures =
      options.timer.now() - startedAt >= HEALTHY_RUN_MS ? 1 : failures + 1;
    const delayMs = exponentialBackoffMs(
      failures,
      BACKGROUND_BACKOFF,
      options.random
    );
    options.logger.error("River service failed; restarting after backoff", {
      attempt: failures,
      delayMs,
      error: describeError(failure),
      service: service.name,
    });
    try {
      await options.timer.delay(delayMs, signal);
    } catch {
      return;
    }
  }
}

/** Check and snapshot a pilot's list of services. */
export function serviceList<Term>(
  services: unknown,
  what: string
): readonly PilotService<Term>[] {
  if (!Array.isArray(services)) {
    throw new ExtensionError(`a pilot's ${what} must return an array`);
  }
  for (const service of services as unknown[]) {
    if (
      typeof service !== "object" ||
      service === null ||
      typeof (service as { readonly name?: unknown }).name !== "string" ||
      typeof (service as { readonly run?: unknown }).run !== "function"
    ) {
      throw new ExtensionError(
        `each of a pilot's ${what} needs a name and a run function`
      );
    }
  }
  return Object.freeze([...(services as PilotService<Term>[])]);
}
