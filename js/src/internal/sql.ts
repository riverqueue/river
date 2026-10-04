/**
 * Quote an identifier for PostgreSQL or SQLite, doubling embedded quotes.
 * It doesn't check the identifier's length or characters.
 */
export function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}
