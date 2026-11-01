// Rule filters: a readable summary, and the simple form for the common case.
import type { Filters } from "./api";
import { showDate } from "./display.svelte";

/** E.g. `raw description ~ SuperMart, account = alice`, or `any of (…)`. */
export function describeFilters(filters: Filters): string {
  const parts: string[] = [];
  if (filters.raw_description != null) parts.push(`raw description ~ ${filters.raw_description}`);
  if (filters.account != null) parts.push(`account = ${filters.account}`);
  if (filters.source != null) parts.push(`source = ${filters.source}`);
  if (filters.start_date != null || filters.end_date != null) {
    parts.push(`${filters.start_date ? showDate(filters.start_date) : "…"} ≤ date < ${filters.end_date ? showDate(filters.end_date) : "…"}`);
  }
  if (filters.tags != null) parts.push(`tags: ${filters.tags.join(", ")}`);
  if (filters.subtags != null) parts.push(`subtags: ${filters.subtags.join(", ")}`);
  if (filters.and_filters?.length) {
    parts.push(`all of (${filters.and_filters.map(describeFilters).join("; ")})`);
  }
  if (filters.or_filters?.length) {
    parts.push(`any of (${filters.or_filters.map(describeFilters).join("; ")})`);
  }
  const text = parts.join(", ") || "everything";
  return filters.invert ? `not (${text})` : text;
}

/** The fields of the simple filter form (empty strings for unset). */
export interface SimpleFilters {
  rawDescription: string;
  account: string;
  source: string;
  startDate: string;
  endDate: string;
  invert: boolean;
}

/** Whether the simple form can show these filters (no nesting or tags). */
export function isSimple(filters: Filters): boolean {
  return !filters.and_filters?.length && !filters.or_filters?.length && filters.tags == null && filters.subtags == null;
}

export function toSimple(filters: Filters): SimpleFilters {
  return {
    rawDescription: filters.raw_description ?? "",
    account: filters.account ?? "",
    source: filters.source ?? "",
    startDate: filters.start_date ?? "",
    endDate: filters.end_date ?? "",
    invert: filters.invert ?? false,
  };
}

export function fromSimple(form: SimpleFilters): Filters {
  const filters: Filters = {};
  if (form.rawDescription) filters.raw_description = form.rawDescription;
  if (form.account) filters.account = form.account;
  if (form.source) filters.source = form.source;
  if (form.startDate) filters.start_date = form.startDate;
  if (form.endDate) filters.end_date = form.endDate;
  if (form.invert) filters.invert = true;
  return filters;
}

/** Filters without their null and default fields, for showing as JSON. */
export function compactFilters(filters: Filters): Filters {
  const compact: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(filters)) {
    if (value === null || value === undefined || (key === "invert" && value === false)) continue;
    compact[key] = Array.isArray(value) && key.endsWith("_filters") ? value.map(compactFilters) : value;
  }
  return compact as Filters;
}

/** Parse filters typed as JSON, or explain why they aren't. */
export function parseFiltersJson(text: string): { ok: true; filters: Filters } | { ok: false; error: string } {
  try {
    const value = JSON.parse(text);
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      return { ok: false, error: "filters must be a JSON object" };
    }
    return { ok: true, filters: value as Filters };
  } catch (error) {
    return { ok: false, error: (error as Error).message };
  }
}

/** Move an item of a list, e.g. a rule up or down in order. */
export function moved<T>(items: T[], from: number, to: number): T[] {
  const result = [...items];
  const [item] = result.splice(from, 1);
  result.splice(Math.max(0, Math.min(to, result.length)), 0, item);
  return result;
}
