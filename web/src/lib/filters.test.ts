import { describe, expect, it } from "vitest";
import {
  activeFilterCount,
  emptyFilters,
  FILTER_GROUPS,
  filterErrors,
  filtersFromQuery,
  filtersToQuery,
  groupFilterCount,
  toQuery,
} from "./filters";

describe("toQuery", () => {
  it("leaves out empty fields", () => {
    expect(Object.values(toQuery(emptyFilters())).every((value) => value === null)).toBe(true);
    expect(activeFilterCount(emptyFilters())).toBe(0);
  });

  it("converts the typed values", () => {
    const form = {
      ...emptyFilters(),
      search: " super ",
      needsReview: "yes" as const,
      accounts: ["alice"],
      minAmount: "-1,000",
      maxAmount: "abc",
      tags: "food, car,",
    };
    expect(toQuery(form)).toMatchObject({
      search: "super",
      needs_review: true,
      accounts: ["alice"],
      min_amount: -1000,
      max_amount: null,
      tags: ["food", "car"],
    });
    expect(filterErrors(form)).toEqual(["max amount: not an amount"]);
  });
});

describe("filters in the URL", () => {
  it("round-trip every field, leaving empty ones out", () => {
    const form = {
      search: "super mart",
      status: "rule" as const,
      needsReview: "no" as const,
      accounts: ["alice", "kim"],
      startDate: "2025-01-01",
      endDate: "2025-02-01",
      minAmount: "-10",
      maxAmount: "5.5",
      tags: "food, car",
      subtags: "fuel",
      sources: "card",
      authors: "claude",
      kinds: ["one-off" as const, "transfer" as const],
      ids: ["a", "b"],
    };
    expect(filtersFromQuery(filtersToQuery(form))).toEqual(form);
    expect(filtersToQuery(emptyFilters())).toBe("");
    expect(filtersToQuery({ ...emptyFilters(), tags: "food" })).toBe("tags=food");
  });

  it("ignore other parameters and unknown values", () => {
    expect(filtersFromQuery("review&status=nope&needs_review=maybe&kinds=nope&x=1")).toEqual(emptyFilters());
    expect(filtersFromQuery("kinds=saving,nope").kinds).toEqual(["saving"]);
  });
});

describe("filter groups", () => {
  it("cover every filter but the search and the chosen IDs, once", () => {
    const fields = FILTER_GROUPS.flatMap((group) => group.fields as readonly string[]);
    const others = Object.keys(emptyFilters()).filter((field) => !["search", "ids"].includes(field));
    expect([...fields].sort()).toEqual(others.sort());
  });

  it("count the filters set in each", () => {
    const form = { ...emptyFilters(), startDate: "2025-01-01", minAmount: "5", kinds: ["one-off" as const], tags: " " };
    expect(FILTER_GROUPS.map((group) => groupFilterCount(form, group.fields))).toEqual([2, 1, 0, 0]);
  });
});
