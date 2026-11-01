import { describe, expect, it } from "vitest";
import type { Config } from "./api";
import { configOf, formChanged, formOf, otherFilters, ruleSummaries } from "./settings";

const CONFIG: Config = {
  ingestion: {
    rules: [
      { match: { source: "checking", raw_description: "^CARD$" }, action: "transfer", to_source: "card", shift_days: 0 },
      { match: { raw_description: "^Tranzila", invert: true }, action: "drop", shift_days: 0 },
    ],
    filters: { start_date: "2024-04-01", invert: false },
  },
};

describe("config form", () => {
  it("round trips a config", () => {
    const form = formOf(CONFIG);
    expect(form).toEqual({ startDate: "2024-04-01", endDate: "" });
    const { config, errors } = configOf(form, CONFIG);
    expect(errors).toEqual([]);
    expect(config).toEqual({
      ...CONFIG,
      ingestion: { ...CONFIG.ingestion, filters: { start_date: "2024-04-01", end_date: null, invert: false } },
    });
  });

  it("tells what is wrong", () => {
    const form = formOf(CONFIG);
    form.startDate = "April";
    form.endDate = " 2025-01-01 ";
    expect(configOf(form, CONFIG).errors).toEqual(["Start date must be YYYY-MM-DD"]);
  });

  it("keeps the ingestion rules, and sums them up", () => {
    expect(configOf(formOf(CONFIG), CONFIG).config.ingestion?.rules).toEqual(CONFIG.ingestion?.rules);
    expect(ruleSummaries(CONFIG)).toEqual([
      "Transfer to card: source checking, raw_description ^CARD$",
      "Drop: not (raw_description ^Tranzila)",
    ]);
    const rewrite = { ...CONFIG, ingestion: { rules: [{ match: { source: "card" }, action: "rewrite" as const, raw_description: "Card", shift_days: -2 }] } };
    expect(ruleSummaries(rewrite)).toEqual(['Rewrite: source card → raw description "Card", date -2 days']);
    const other = { ...CONFIG, ingestion: { rules: [{ match: {}, action: "transfer" as const, to_account: "kim", to_source: "bit", shift_days: 0 }] } };
    expect(ruleSummaries(other)).toEqual(["Transfer to kim bit: every row"]);
  });

  it("keeps the filters it doesn't show", () => {
    const config = { ...CONFIG, ingestion: { ...CONFIG.ingestion, filters: { raw_description: "x", invert: true } } };
    expect(otherFilters(config)).toEqual(["raw_description", "invert"]);
    expect(configOf(formOf(config), config).config.ingestion?.filters).toEqual({
      raw_description: "x",
      invert: true,
      start_date: null,
      end_date: null,
    });
    expect(otherFilters(CONFIG)).toEqual([]);
  });

  it("tells changes", () => {
    const form = formOf(CONFIG);
    expect(formChanged(form, formOf(CONFIG))).toBe(false);
    form.endDate = "2025-01-01";
    expect(formChanged(form, formOf(CONFIG))).toBe(true);
  });
});
