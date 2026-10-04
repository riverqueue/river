import { isExactJsonNumber, type JobRow, type JsonValue } from "riverqueue";
import type { QueueRow } from "riverqueue/unstable-driver";

/** Normalize an exact JavaScript job to the shared JSON-RPC wire shape. */
export function normalizeJob(job: JobRow): Record<string, unknown> {
  const metadata = Object.fromEntries(Object.entries(job.metadata));
  delete metadata["river:unique_nonce"];
  return {
    args: job.args,
    attempt: job.attempt,
    attempted_at: job.attemptedAt?.toString() ?? null,
    attempted_by: [...job.attemptedBy],
    created_at: job.createdAt.toString(),
    errors: job.errors.map((error) => ({
      at: error.at.toString(),
      attempt: error.attempt,
      error: error.error,
      trace: error.trace,
    })),
    finalized_at: job.finalizedAt?.toString() ?? null,
    id: job.id,
    kind: job.kind,
    max_attempts: job.maxAttempts,
    metadata,
    priority: job.priority,
    queue: job.queue,
    scheduled_at: job.scheduledAt.toString(),
    state: job.state,
    tags: [...job.tags],
    unique_key:
      job.uniqueKey === null
        ? null
        : Buffer.from(job.uniqueKey).toString("hex"),
    unique_states:
      job.uniqueStates === null ? null : [...job.uniqueStates].sort(),
  };
}

export function normalizeQueue(queue: QueueRow): Record<string, unknown> {
  return {
    created_at: queue.createdAt.toString(),
    metadata: queue.metadata,
    name: queue.name,
    paused_at: queue.pausedAt?.toString() ?? null,
    updated_at: queue.updatedAt.toString(),
  };
}

/** Extract numeric lexemes after a job has crossed the public driver seam. */
export function exactJsonTokens(job: JobRow): Record<string, string> {
  const tokens: Record<string, string> = {
    decimal: exactJsonToken(job.args.decimal, "args.decimal"),
    integer: exactJsonToken(job.args.integer, "args.integer"),
    negative: exactJsonToken(job.metadata.negative, "metadata.negative"),
  };
  for (const field of [
    "big_integer",
    "beyond_float",
    "long_decimal",
  ] as const) {
    if (Object.hasOwn(job.metadata, field)) {
      tokens[field] = exactJsonToken(job.metadata[field], `metadata.${field}`);
    }
  }
  return tokens;
}

function exactJsonToken(value: JsonValue | undefined, path: string): string {
  if (!isExactJsonNumber(value)) {
    throw new Error(`${path} was not decoded as an exact JSON number`);
  }
  return value.rawJSON;
}
