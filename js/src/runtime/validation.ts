/**
 * Argument validation shared by runtime configuration and operations.
 */
import { ValidationError } from "../errors.js";

export function requireNonNegativeInteger(name: string, value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new ValidationError(`${name} must be a non-negative safe integer`);
  }
  return value;
}

export function requirePositiveInteger(name: string, value: number): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new ValidationError(`${name} must be a positive safe integer`);
  }
  return value;
}

/** Reject options whose keys are present with an `undefined` value. */
export function rejectExplicitUndefined(value: object): void {
  for (const [key, item] of Object.entries(value)) {
    if (item === undefined) {
      throw new ValidationError(`${key} must be omitted instead of undefined`);
    }
  }
}
