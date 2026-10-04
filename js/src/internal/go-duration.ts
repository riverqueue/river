/**
 * Go's `time.ParseDuration`, ported so cron `@every` descriptors accept and
 * reject exactly what River Go accepts.
 */

const MAX_UINT64 = (1n << 64n) - 1n;
const OVERFLOW = 1n << 63n;

/** Nanoseconds per unit, keyed by Go's unit spellings. */
const UNITS: ReadonlyMap<string, bigint> = new Map([
  ["ns", 1n],
  ["us", 1_000n],
  ["\u00b5s", 1_000n], // U+00B5 MICRO SIGN
  ["\u03bcs", 1_000n], // U+03BC GREEK SMALL LETTER MU
  ["ms", 1_000_000n],
  ["s", 1_000_000_000n],
  ["m", 60_000_000_000n],
  ["h", 3_600_000_000_000n],
]);

/**
 * Parse a Go duration string such as `1h30m` or `-1.5s` into signed
 * nanoseconds, with Go's syntax, overflow rules, and floating-point handling
 * of fractions. Throws an `Error` whose message matches Go's.
 */
export function parseGoDuration(text: string): bigint {
  const invalid = () => new Error(`time: invalid duration ${quote(text)}`);
  let rest = text;
  let negative = false;
  if (rest.startsWith("-") || rest.startsWith("+")) {
    negative = rest.startsWith("-");
    rest = rest.slice(1);
  }
  // Special case: a bare "0" needs no unit.
  if (rest === "0") return 0n;
  if (rest === "") throw invalid();

  // Go accumulates in a uint64, so a sum reaching 2^64 wraps silently.
  let total = 0n;
  while (rest !== "") {
    // The next character must be [0-9.].
    if (!(rest.startsWith(".") || isDigit(rest, 0))) throw invalid();

    const integerText = leadingDigits(rest);
    rest = rest.slice(integerText.length);
    const integer = leadingInteger(integerText);
    if (integer === null) throw invalid();

    let fraction = 0n;
    let scale = 1;
    let fractionText = "";
    if (rest.startsWith(".")) {
      rest = rest.slice(1);
      fractionText = leadingDigits(rest);
      rest = rest.slice(fractionText.length);
      [fraction, scale] = leadingFraction(fractionText);
    }
    // No digits at all, as in ".s" or "-.s".
    if (integerText === "" && fractionText === "") throw invalid();

    let unitLength = 0;
    while (
      unitLength < rest.length &&
      rest[unitLength] !== "." &&
      !isDigit(rest, unitLength)
    ) {
      unitLength++;
    }
    if (unitLength === 0) {
      throw new Error(`time: missing unit in duration ${quote(text)}`);
    }
    const unitText = rest.slice(0, unitLength);
    rest = rest.slice(unitLength);
    const unit = UNITS.get(unitText);
    if (unit === undefined) {
      throw new Error(
        `time: unknown unit ${quote(unitText)} in duration ${quote(text)}`
      );
    }

    if (integer > OVERFLOW / unit) throw invalid();
    let value = integer * unit;
    if (fraction > 0n) {
      // Go uses float64 here to stay nanosecond-accurate for fractional
      // hours; truncating the product toward zero matches `uint64(...)`.
      value += BigInt(Math.trunc(Number(fraction) * (Number(unit) / scale)));
      if (value > OVERFLOW) throw invalid();
    }
    total = (total + value) & MAX_UINT64;
    if (total > OVERFLOW) throw invalid();
  }
  if (negative) return BigInt.asIntN(64, -total);
  if (total > OVERFLOW - 1n) throw invalid();
  return total;
}

function isDigit(text: string, index: number): boolean {
  const code = text.charCodeAt(index);
  return code >= 48 && code <= 57;
}

function leadingDigits(text: string): string {
  let length = 0;
  while (length < text.length && isDigit(text, length)) length++;
  return text.slice(0, length);
}

/** Go's `leadingInt`: null when the digits overflow 2^63. */
function leadingInteger(digits: string): bigint | null {
  let value = 0n;
  for (const digit of digits) {
    if (value > OVERFLOW / 10n) return null;
    value = value * 10n + BigInt(digit);
    if (value > OVERFLOW) return null;
  }
  return value;
}

/**
 * Go's `leadingFraction`: the fraction's digits as an integer and the power
 * of ten dividing them, silently dropping precision on overflow.
 */
function leadingFraction(digits: string): [bigint, number] {
  let value = 0n;
  let scale = 1;
  let overflow = false;
  for (const digit of digits) {
    if (overflow) continue;
    if (value > (OVERFLOW - 1n) / 10n) {
      overflow = true;
      continue;
    }
    const next = value * 10n + BigInt(digit);
    if (next > OVERFLOW) {
      overflow = true;
      continue;
    }
    value = next;
    scale *= 10;
  }
  return [value, scale];
}

function quote(text: string): string {
  return JSON.stringify(text);
}
