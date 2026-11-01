// Exact money arithmetic in integer minor units (e.g. agorot or cents).
//
// The API speaks currency units as JSON numbers (e.g. -12.34). Floats can't
// add exactly (0.1 + 0.2 !== 0.3), so split sums are computed in minor units.

export const DECIMALS = 2;
const SCALE = 10 ** DECIMALS;

/** Currency units to minor units, e.g. -12.34 to -1234. */
export function toMinor(amount: number): number {
  return Math.round(amount * SCALE);
}

/** Minor units to currency units, e.g. -1234 to -12.34. */
export function fromMinor(minor: number): number {
  return minor / SCALE;
}

const formatter = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: DECIMALS,
  maximumFractionDigits: DECIMALS,
});

/** E.g. "-1,234.50", or "+1,234.50" with `signed`. */
export function formatMinor(minor: number, { signed = false } = {}): string {
  const text = formatter.format(Math.abs(minor) / SCALE);
  if (minor < 0) return `−${text}`; // a real minus sign, as wide as "+"
  return signed && minor > 0 ? `+${text}` : text;
}

export function formatAmount(amount: number, options = {}): string {
  return formatMinor(toMinor(amount), options);
}

export type ParsedAmount = { ok: true; minor: number } | { ok: false; error: string };

/**
 * Parse an amount typed by the user, to minor units.
 *
 * Without a sign, the amount takes `defaultSign` (the transaction's sign), so
 * a split part of an expense is typed as a plain "30". An explicit "+" or "-"
 * is kept as is. Commas are thousands separators.
 */
export function parseAmount(text: string, defaultSign: 1 | -1 = 1): ParsedAmount {
  const cleaned = text.trim().replaceAll(",", "").replace("−", "-");
  const match = /^([+-]?)(\d*)(?:\.(\d*))?$/.exec(cleaned);
  if (!match || (match[2] === "" && (match[3] ?? "") === "")) {
    return { ok: false, error: "not an amount" };
  }
  const [, sign, whole, fraction = ""] = match;
  if (fraction.length > DECIMALS) {
    return { ok: false, error: `at most ${DECIMALS} decimals` };
  }
  const magnitude = Number(whole || "0") * SCALE + Number(fraction.padEnd(DECIMALS, "0"));
  const signValue = sign === "-" ? -1 : sign === "+" ? 1 : defaultSign;
  return { ok: true, minor: signValue * magnitude };
}

/** The sign of an amount, for parsing parts: zero counts as positive. */
export function signOf(minor: number): 1 | -1 {
  return minor < 0 ? -1 : 1;
}
