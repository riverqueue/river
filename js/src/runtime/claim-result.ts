/**
 * Checking the claim results that extensions hand to the runtime.
 */
import type { JobClaimResult } from "../driver.js";
import type { JobRow } from "../job.js";

/** A claimed row an extension returned, before its fields are checked. */
export interface ClaimedRow {
  readonly job: unknown;
  /** Whether the row couldn't be fully decoded, so it may lack fields. */
  readonly partial: boolean;
}

/**
 * Check the shape of a claim result an extension returned: a list of job
 * rows, and any decode errors, each an `Error` keyed by the ID of one of
 * those rows. Returns each row, in claim order, for the caller to check its
 * fields, and a frozen copy of the result to keep once they pass.
 */
export function checkClaimResult(
  result: unknown,
  fail: (reason: string) => never
): { readonly checked: JobClaimResult; readonly rows: readonly ClaimedRow[] } {
  if (typeof result !== "object" || result === null) fail("no claim result");
  const { decodeErrors, jobs } = result as Partial<JobClaimResult>;
  if (!Array.isArray(jobs)) fail("a result without a job list");
  if (decodeErrors !== undefined && !(decodeErrors instanceof Map)) {
    fail("decode errors that aren't a Map");
  }
  const errors = new Map<bigint, Error>();
  for (const [id, error] of decodeErrors ?? []) {
    if (typeof id !== "bigint" || !(error instanceof Error)) {
      fail("a decode error that isn't an Error keyed by a job ID");
    }
    errors.set(id, error);
  }
  const ids = new Set<unknown>();
  const rows = (jobs as readonly unknown[]).map((job): ClaimedRow => {
    const id = (job as Partial<JobRow> | null | undefined)?.id;
    ids.add(id);
    return { job, partial: typeof id === "bigint" && errors.has(id) };
  });
  for (const id of errors.keys()) {
    if (!ids.has(id))
      fail(`a decode error for job ${id}, which it didn't return`);
  }
  return {
    checked: Object.freeze({
      decodeErrors: errors,
      jobs: Object.freeze([...(jobs as readonly JobRow[])]),
    }),
    rows,
  };
}
