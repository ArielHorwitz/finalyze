import { describe, expect, it } from "vitest";
import { compactFilters, describeFilters, fromSimple, isSimple, moved, parseFiltersJson, toSimple } from "./rules";

describe("rule filters", () => {
  it("describe nested filters", () => {
    expect(
      describeFilters({
        invert: true,
        or_filters: [{ raw_description: "Cafe", invert: false }, { account: "kim", source: "card", invert: false }],
      }),
    ).toBe("not (any of (raw description ~ Cafe; account = kim, source = card))");
  });

  it("round-trip through the simple form", () => {
    const filters = { raw_description: "SuperMart", start_date: "2025-01-01", invert: true };
    expect(isSimple(filters)).toBe(true);
    expect(fromSimple(toSimple(filters))).toEqual(filters);
    expect(isSimple({ or_filters: [{ raw_description: "a" }] })).toBe(false);
  });

  it("drop nulls for the JSON editor", () => {
    expect(compactFilters({ raw_description: "a", account: null, invert: false, and_filters: [{ source: "b", tags: null }] })).toEqual({
      raw_description: "a",
      and_filters: [{ source: "b" }],
    });
  });

  it("parse JSON", () => {
    expect(parseFiltersJson('{"raw_description": "a"}')).toEqual({ ok: true, filters: { raw_description: "a" } });
    expect(parseFiltersJson("[1]").ok).toBe(false);
    expect(parseFiltersJson("{").ok).toBe(false);
  });

  it("move items", () => {
    expect(moved([1, 2, 3], 0, 1)).toEqual([2, 1, 3]);
    expect(moved([1, 2, 3], 2, 0)).toEqual([3, 1, 2]);
  });
});
