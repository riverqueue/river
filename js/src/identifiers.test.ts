import { describe, expect, it } from "vitest";

import { ValidationError } from "./errors.js";
import { queueLookupName, validateQueueName } from "./identifiers.js";

describe("validateQueueName", () => {
  // Mirrors Go River's `^(?:[a-z0-9])+(?:[_|\-]?[a-z0-9]+)*$` and its
  // 64-character limit.
  it.each([
    "0",
    "a",
    "a-b",
    "a_b",
    "a|b",
    "default",
    "tenant|priority_emails-2",
    "a".repeat(64),
  ])("accepts %j like Go", (name) => {
    expect(validateQueueName(name)).toBe(name);
  });

  it.each([
    "",
    "-a",
    "A",
    "_a",
    "a b",
    "a-",
    "a.b",
    "a__b",
    "a_|b",
    "a|",
    "|a",
    "a".repeat(65),
  ])("rejects %j like Go", (name) => {
    expect(() => validateQueueName(name)).toThrow(ValidationError);
  });

  it("rejects the all-queues sentinel as a queue name", () => {
    expect(() => validateQueueName("*")).toThrow(ValidationError);
  });
});

describe("queueLookupName", () => {
  it("accepts any string, as Go looks queues up without validating", () => {
    for (const name of ["*", "", "Not A Queue!", "x".repeat(200)]) {
      expect(queueLookupName(name)).toBe(name);
    }
    expect(() => queueLookupName(1 as unknown as string)).toThrow(
      ValidationError
    );
  });
});
