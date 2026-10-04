/** Lowercase hexadecimal text of `value`'s bytes. */
export function bytesToHex(value: Uint8Array): string {
  return Array.from(value, (byte) => byte.toString(16).padStart(2, "0")).join(
    ""
  );
}
