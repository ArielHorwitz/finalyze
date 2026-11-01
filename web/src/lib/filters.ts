// The transaction filter form: what the user types, and the API query it means.
import type { TransactionQuery } from "./api";
import { TAG_KINDS, type TagKind } from "./kinds";
import { fromMinor, parseAmount } from "./money";

export interface FilterForm {
  search: string;
  status: "" | "untagged" | "annotated" | "rule" | "tagged";
  needsReview: "" | "yes" | "no";
  accounts: string[];
  startDate: string;
  endDate: string;
  minAmount: string;
  maxAmount: string;
  /** Comma-separated lists */
  tags: string;
  subtags: string;
  sources: string;
  authors: string;
  /** Effective kinds of any part (untagged rows are regular). */
  kinds: TagKind[];
  /** Only these transactions, e.g. from a dashboard drill-down. */
  ids: string[];
}

export function emptyFilters(): FilterForm {
  return {
    search: "",
    status: "",
    needsReview: "",
    accounts: [],
    startDate: "",
    endDate: "",
    minAmount: "",
    maxAmount: "",
    tags: "",
    subtags: "",
    sources: "",
    authors: "",
    kinds: [],
    ids: [],
  };
}

function list(text: string): string[] | null {
  const items = text
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  return items.length ? items : null;
}

function amount(text: string): number | null {
  if (!text.trim()) return null;
  const parsed = parseAmount(text);
  return parsed.ok ? fromMinor(parsed.minor) : null;
}

/** The API query of a filter form: empty fields don't filter. */
export function toQuery(form: FilterForm): TransactionQuery {
  return {
    search: form.search.trim() || null,
    status: form.status || null,
    needs_review: form.needsReview === "" ? null : form.needsReview === "yes",
    accounts: form.accounts.length ? form.accounts : null,
    start_date: form.startDate || null,
    end_date: form.endDate || null,
    min_amount: amount(form.minAmount),
    max_amount: amount(form.maxAmount),
    tags: list(form.tags),
    subtags: list(form.subtags),
    sources: list(form.sources),
    authors: list(form.authors),
    kinds: form.kinds.length ? form.kinds : null,
    ids: form.ids.length ? form.ids : null,
  };
}

// The URL query parameters of the filters, each a form field
const TEXT_PARAMS = [
  ["search", "search"],
  ["tags", "tags"],
  ["subtags", "subtags"],
  ["sources", "sources"],
  ["authors", "authors"],
  ["start_date", "startDate"],
  ["end_date", "endDate"],
  ["min_amount", "minAmount"],
  ["max_amount", "maxAmount"],
] as const;
const LIST_PARAMS = [
  ["accounts", "accounts"],
  ["ids", "ids"],
] as const;
const STATUSES = ["untagged", "annotated", "rule", "tagged"];
const KINDS: string[] = TAG_KINDS.map((kind) => kind.id);

/**
 * Filters from a page's URL query, e.g. "ids=a,b" or "tags=food&start_date=2025-05-01".
 * Unknown parameters are ignored.
 */
export function filtersFromQuery(query: string): FilterForm {
  const params = new URLSearchParams(query);
  const form = emptyFilters();
  const text = (name: string) => params.get(name) ?? "";
  for (const [name, field] of TEXT_PARAMS) form[field] = text(name);
  for (const [name, field] of LIST_PARAMS) form[field] = list(text(name)) ?? [];
  if (STATUSES.includes(text("status"))) form.status = text("status") as FilterForm["status"];
  form.kinds = (list(text("kinds")) ?? []).filter((kind): kind is TagKind => KINDS.includes(kind));
  if (["yes", "no"].includes(text("needs_review"))) form.needsReview = text("needs_review") as FilterForm["needsReview"];
  return form;
}

/** The URL query of filters (filtersFromQuery's), "" without any. */
export function filtersToQuery(form: FilterForm): string {
  const params = new URLSearchParams();
  for (const [name, field] of TEXT_PARAMS) if (form[field].trim()) params.set(name, form[field]);
  for (const [name, field] of LIST_PARAMS) if (form[field].length) params.set(name, form[field].join(","));
  if (form.status) params.set("status", form.status);
  if (form.needsReview) params.set("needs_review", form.needsReview);
  if (form.kinds.length) params.set("kinds", form.kinds.join(","));
  return params.toString();
}

/** How many filters are set. */
export function activeFilterCount(form: FilterForm): number {
  return Object.values(toQuery(form)).filter((value) => value !== null).length;
}

/** The filters' groups, by what they filter on, as the filter bar shows them. */
export const FILTER_GROUPS = [
  { id: "dates", label: "Dates & amounts", fields: ["startDate", "endDate", "minAmount", "maxAmount"] },
  { id: "tags", label: "Tags & kinds", fields: ["tags", "subtags", "kinds"] },
  { id: "accounts", label: "Accounts & sources", fields: ["accounts", "sources"] },
  { id: "status", label: "Status & review", fields: ["status", "needsReview", "authors"] },
] as const satisfies readonly { id: string; label: string; fields: readonly (keyof FilterForm)[] }[];

/** How many filters of a group are set. */
export function groupFilterCount(form: FilterForm, fields: readonly (keyof FilterForm)[]): number {
  return fields.filter((field) => {
    const value = form[field];
    return Array.isArray(value) ? value.length > 0 : value.trim() !== "";
  }).length;
}

/** Problems with the typed values, e.g. an amount that isn't one. */
export function filterErrors(form: FilterForm): string[] {
  const errors = [];
  for (const [label, text] of [
    ["min amount", form.minAmount],
    ["max amount", form.maxAmount],
  ]) {
    if (text.trim() && !parseAmount(text).ok) errors.push(`${label}: not an amount`);
  }
  return errors;
}
