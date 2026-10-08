import { Buffer } from "node:buffer";

import { types as defaultPgTypes } from "pg";
import type { CustomTypesConfig } from "pg";
import { parse as parsePostgresArray } from "postgres-array";
import { parseJson } from "riverqueue";

const PG_EPOCH_UNIX_MICROSECONDS = 946_684_800_000_000n;
const PG_BIT_OID = 1560;
const PG_BOOL_OID = 16;
const PG_BYTEA_OID = 17;
const PG_INT2_OID = 21;
const PG_INT4_OID = 23;
const PG_INT8_OID = 20;
const PG_JSON_ARRAY_OID = 199;
const PG_JSON_OID = 114;
const PG_JSONB_ARRAY_OID = 3807;
const PG_JSONB_OID = 3802;
const PG_NAME_OID = 19;
const PG_TEXT_ARRAY_OID = 1009;
const PG_TEXT_OID = 25;
const PG_TIMESTAMPTZ_OID = 1184;
const PG_VARBIT_OID = 1562;
const PG_VARCHAR_ARRAY_OID = 1015;
const PG_VARCHAR_OID = 1043;

/**
 * River's own text parsers for the other built-in types its queries return,
 * so an application that changes node-postgres's process-global parsers
 * (`pg.types.setTypeParser`) can't change how River reads its rows.
 */
const TEXT_PARSERS: ReadonlyMap<number, (value: string) => unknown> = new Map<
  number,
  (value: string) => unknown
>([
  [PG_BIT_OID, identity],
  [PG_BOOL_OID, parseTextBool],
  [PG_BYTEA_OID, parseTextBytea],
  [PG_INT2_OID, parseTextInt4],
  [PG_INT4_OID, parseTextInt4],
  [PG_NAME_OID, identity],
  [PG_TEXT_ARRAY_OID, parseTextArray],
  [PG_TEXT_OID, identity],
  [PG_VARBIT_OID, identity],
  [PG_VARCHAR_ARRAY_OID, parseTextArray],
  [PG_VARCHAR_OID, identity],
]);

/**
 * Query-scoped Postgres parsers for River's exact persisted values.
 *
 * This object delegates unknown OIDs to node-postgres and never mutates its
 * process-global parser registry. It is safe to use with caller-owned pools.
 */
export const PG_EXACT_TYPES: CustomTypesConfig = {
  getTypeParser(oid, format = "text") {
    const numericOid: number = oid;
    if (numericOid === PG_INT8_OID) {
      return format === "binary" ? parseBinaryInt8 : parseTextInt8;
    }
    if (numericOid === PG_TIMESTAMPTZ_OID) {
      return format === "binary"
        ? parseBinaryTimestamptz
        : parseTextTimestamptz;
    }
    if (
      format === "text" &&
      (numericOid === PG_JSON_OID || numericOid === PG_JSONB_OID)
    ) {
      return parseJson;
    }
    // River's only array of JSON is a job's `errors`, whose elements it
    // decodes from their text like River for Go, so they're left as text.
    if (
      format === "text" &&
      (numericOid === PG_JSON_ARRAY_OID || numericOid === PG_JSONB_ARRAY_OID)
    ) {
      return parseTextArray;
    }
    const parser = format === "text" ? TEXT_PARSERS.get(numericOid) : undefined;
    if (parser !== undefined) return parser;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return -- node-postgres types every parser's result as any
    return defaultPgTypes.getTypeParser(oid, format);
  },
};

function parseBinaryInt8(value: Buffer): bigint {
  if (value.byteLength !== 8) {
    throw new RangeError(
      `invalid Postgres int8 binary length: ${value.byteLength}`
    );
  }
  return value.readBigInt64BE();
}

function parseBinaryTimestamptz(value: Buffer): Temporal.Instant {
  const postgresMicroseconds = parseBinaryInt8(value);
  if (
    postgresMicroseconds === 9_223_372_036_854_775_807n ||
    postgresMicroseconds === -9_223_372_036_854_775_808n
  ) {
    throw new RangeError("Postgres infinite timestamps are not River instants");
  }

  const unixNanoseconds =
    (postgresMicroseconds + PG_EPOCH_UNIX_MICROSECONDS) * 1_000n;
  return Temporal.Instant.fromEpochNanoseconds(unixNanoseconds);
}

function identity(value: string): string {
  return value;
}

function parseTextArray(value: string): string[] {
  return parsePostgresArray(value, identity);
}

function parseTextBool(value: string): boolean {
  if (value === "t") return true;
  if (value === "f") return false;
  throw new RangeError(`invalid Postgres bool text: ${JSON.stringify(value)}`);
}

/** Decode `bytea` in either `bytea_output` format, `hex` or `escape`. */
function parseTextBytea(value: string): Buffer {
  if (value.startsWith("\\x")) {
    const hex = value.slice(2);
    if (!/^(?:[0-9a-fA-F]{2})*$/.test(hex)) {
      throw new RangeError("invalid Postgres bytea hex text");
    }
    return Buffer.from(hex, "hex");
  }
  const bytes: number[] = [];
  for (let index = 0; index < value.length; index++) {
    const character = value[index];
    if (character !== "\\") {
      const code = value.charCodeAt(index);
      if (code > 0xff) throw new RangeError("invalid Postgres bytea text");
      bytes.push(code);
    } else if (value[index + 1] === "\\") {
      bytes.push(0x5c);
      index++;
    } else {
      const octal = value.slice(index + 1, index + 4);
      if (!/^[0-3][0-7]{2}$/.test(octal)) {
        throw new RangeError("invalid Postgres bytea escape text");
      }
      bytes.push(Number.parseInt(octal, 8));
      index += 3;
    }
  }
  return Buffer.from(bytes);
}

function parseTextInt4(value: string): number {
  if (!/^-?(0|[1-9]\d*)$/.test(value)) {
    throw new RangeError(
      `invalid Postgres integer text: ${JSON.stringify(value)}`
    );
  }
  return Number.parseInt(value, 10);
}

function parseTextInt8(value: string): bigint {
  if (!/^-?(0|[1-9]\d*)$/.test(value)) {
    throw new RangeError(
      `invalid Postgres int8 text: ${JSON.stringify(value)}`
    );
  }
  return BigInt(value);
}

function parseTextTimestamptz(value: string): Temporal.Instant {
  if (value === "infinity" || value === "-infinity") {
    throw new RangeError("Postgres infinite timestamps are not River instants");
  }

  const match =
    /^(\d{4,6})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(\.\d{1,9})?([+-]\d{2}(?::?\d{2}(?::?\d{2})?)?)( BC)?$/.exec(
      value
    );
  if (match === null) {
    throw new RangeError(
      `invalid Postgres timestamptz text: ${JSON.stringify(value)}`
    );
  }

  const [
    ,
    pgYear,
    month,
    day,
    hour,
    minute,
    second,
    fraction = "",
    rawOffset,
    bc,
  ] = match;
  const year = bc === undefined ? pgYear : isoYearFromBc(pgYear as string);
  const offset = normalizeOffset(rawOffset as string);

  return Temporal.Instant.from(
    `${year}-${month}-${day}T${hour}:${minute}:${second}${fraction}${offset}`
  );
}

function isoYearFromBc(value: string): string {
  const isoYear = 1 - Number.parseInt(value, 10);
  if (isoYear >= 0) return isoYear.toString(10).padStart(4, "0");
  return `-${Math.abs(isoYear).toString(10).padStart(6, "0")}`;
}

/**
 * Write a Postgres offset as `±HH:MM` or `±HH:MM:SS`. Historical local
 * mean times, such as `+05:53:28`, have seconds.
 */
function normalizeOffset(value: string): string {
  const digits = value.slice(1).replaceAll(":", "");
  const parts = [digits.slice(0, 2), digits.slice(2, 4) || "00"];
  if (digits.length > 4) parts.push(digits.slice(4));
  return `${value[0] ?? "+"}${parts.join(":")}`;
}
