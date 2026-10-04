import { ValidationError } from "./errors.js";

/** Go River's queue grammar: `|` separates segments like `_` and `-`. */
const QUEUE_NAME_RE = /^[a-z0-9]+(?:[_|-]?[a-z0-9]+)*$/;
const USER_SPECIFIED_ID_OR_KIND_RE = new RegExp(
  "^[A-Za-z0-9_][A-Za-z0-9_\\-\\[\\]<>/.·:+]+$"
);

/** @internal Match River's portable job-kind and user-ID grammar. */
export function isUserSpecifiedIdOrKind(value: string): boolean {
  return USER_SPECIFIED_ID_OR_KIND_RE.test(value);
}

/** @internal Validate the queue grammar shared by inserts and runtimes. */
export function validateQueueName(value: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new ValidationError("queue name must not be empty");
  }
  if (value.length > 64) {
    throw new ValidationError("queue name must be at most 64 characters");
  }
  if (!QUEUE_NAME_RE.test(value)) {
    throw new ValidationError(
      "queue name must contain lowercase letters and numbers separated by underscores, pipes, or hyphens"
    );
  }
  return value;
}

/**
 * @internal Check a queue name that only looks up an existing queue (get,
 * pause, resume, update). Like River for Go, the name isn't checked against
 * the queue grammar: a name no queue can have is simply not found.
 */
export function queueLookupName(value: string): string {
  if (typeof value !== "string") {
    throw new ValidationError("queue name must be a string");
  }
  return value;
}
