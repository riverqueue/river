/**
 * A sibling module imported by `handlers.ts` through its compiled `.js` name,
 * proving that a thread resolves TypeScript sources imported that way.
 */
export function describeValue(value: unknown): string {
  if (value instanceof Date) return `date:${value.toISOString()}`;
  if (value instanceof Map) return `map:${value.size}`;
  if (value instanceof Set) return `set:${value.size}`;
  if (value instanceof Uint8Array) return `bytes:${value.length}`;
  return typeof value;
}
