import { describe, expect, it } from "vitest";
import { formatMinor, fromMinor, parseAmount, toMinor } from "./money";

describe("minor units", () => {
  it("converts without float noise", () => {
    expect(toMinor(-62.69)).toBe(-6269);
    expect(toMinor(0.1 + 0.2)).toBe(30);
    expect(fromMinor(-6269)).toBe(-62.69);
  });

  it("formats with a real minus sign and thousands", () => {
    expect(formatMinor(-123456)).toBe("−1,234.56");
    expect(formatMinor(5, { signed: true })).toBe("+0.05");
    expect(formatMinor(0, { signed: true })).toBe("0.00");
  });
});

describe("parseAmount", () => {
  it("takes the default sign without one", () => {
    expect(parseAmount("30", -1)).toEqual({ ok: true, minor: -3000 });
    expect(parseAmount("12.5", -1)).toEqual({ ok: true, minor: -1250 });
    expect(parseAmount(".05", 1)).toEqual({ ok: true, minor: 5 });
  });

  it("keeps an explicit sign", () => {
    expect(parseAmount("+30", -1)).toEqual({ ok: true, minor: 3000 });
    expect(parseAmount("-1,234.50", 1)).toEqual({ ok: true, minor: -123450 });
    expect(parseAmount("−7", 1)).toEqual({ ok: true, minor: -700 });
  });

  it("rejects what isn't an amount", () => {
    expect(parseAmount("1.234").ok).toBe(false);
    expect(parseAmount("abc").ok).toBe(false);
    expect(parseAmount("").ok).toBe(false);
    expect(parseAmount("-").ok).toBe(false);
    expect(parseAmount("1.2.3").ok).toBe(false);
  });
});
