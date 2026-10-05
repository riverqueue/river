/**
 * The stored text of queue rows' metadata, which drivers record as they
 * decode rows so a pilot can read it exactly, such as number literals
 * like `1.0` that decoding folds into plain numbers. Nothing here is
 * readable through a package entry point.
 */
import type { QueueRow } from "../driver.js";
import { ValidationError } from "../errors.js";
import { stringifyJson } from "../json.js";

const texts = new WeakMap<object, string>();

/**
 * Record `text`, the metadata of `queue` as its database stores it. A
 * first-party driver calls it for each queue row it decodes.
 */
export function recordQueueMetadataText(queue: QueueRow, text: string): void {
  // JavaScript callers may pass anything.
  const value: unknown = queue;
  if (typeof value !== "object" || value === null) {
    throw new ValidationError("a queue row must be an object");
  }
  if (typeof text !== "string") {
    throw new ValidationError("queue metadata text must be a string");
  }
  texts.set(queue, text);
}

/**
 * The stored text of `queue`'s metadata, or River's encoding of its parsed
 * metadata when no driver recorded it.
 */
export function queueMetadataText(queue: QueueRow): string {
  return texts.get(queue) ?? stringifyJson(queue.metadata);
}

/** Carry `from`'s recorded text over to a copy of it. */
export function copyQueueMetadataText(from: QueueRow, to: QueueRow): void {
  const text = texts.get(from);
  if (text !== undefined) texts.set(to, text);
}
