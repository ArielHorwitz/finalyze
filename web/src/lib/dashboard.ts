// The dashboard's settings: its view and options, kept in the URL (e.g.
// "#/dashboard?view=trends&period=6m&net=1"), and what its numbers mean as
// row selections and transaction queries (for drilling down).
import type { Dashboard, DashboardDefaults, DashboardOptions, DashboardSelection, TransactionQuery } from "./api";
import { isoOfLocal } from "./dates";
import { showDate } from "./display.svelte";
import { averagedKinds, FLOW_KINDS, type TagKind } from "./kinds";

export const VIEWS = [
  { id: "overview", title: "Overview", term: "view_overview" },
  { id: "balance", title: "Balance", term: "view_balance" },
  { id: "flow", title: "Cash flow", term: "view_flow" },
  { id: "breakdown", title: "Breakdown", term: "view_breakdown" },
  { id: "trends", title: "Trends", term: "view_trends" },
  { id: "monthly", title: "Monthly", term: "view_monthly" },
  { id: "ledger", title: "Ledger", term: "view_ledger" },
] as const;
export type ViewId = (typeof VIEWS)[number]["id"];

export const PERIODS = [
  { id: "3m", label: "3M", title: "Last 3 months" },
  { id: "6m", label: "6M", title: "Last 6 months" },
  { id: "12m", label: "12M", title: "Last 12 months" },
  { id: "24m", label: "24M", title: "Last 24 months" },
  { id: "ytd", label: "YTD", title: "Year to date" },
  { id: "all", label: "All", title: "All months" },
] as const;
export type PeriodId = (typeof PERIODS)[number]["id"];

export type Kind = "incomes" | "expenses";
export type BreakdownMode = "total" | "rolling" | "month";
/** How the balance chart draws groups: stacked areas (whose top is the net worth), lines, or not at all. */
export type GroupsShown = "stacked" | "lines" | "hidden";
/** How tags are ranked: by their change from the baseline in ₪ ("movers"), or by amount. */
export type TagRanking = "movers" | "largest";

/** What the URL holds. Options left null take their defaults (the Settings page's). */
export interface DashboardState {
  view: ViewId;
  period: PeriodId;
  startMonth: string | null;
  endMonth: string | null;
  accounts: string[];
  /** Groups of the cash flows and breakdowns: none for the spending groups. */
  groups: string[];
  /** One-offs count in averages and trends too. */
  oneOffs: boolean;
  tags: string[];
  netByTag: boolean | null;
  /** The rolling window shown, in months (every window is computed). */
  rolling: string | null;
  breakdownMonths: number | null;
  completeMonths: boolean | null;
  kind: Kind;
  // How views show their data (no reload)
  /** Trends: monthly sums rather than rolling means. */
  trendsSums: boolean;
  trendsStacked: boolean;
  /** Trends: how many of the largest tags are drawn at first. */
  trendsTop: number;
  /** Trends: by subtag rather than by tag. */
  trendsSubtags: boolean;
  /** Trends by subtag: only this tag's subtags, else every tag's. */
  trendsOf: string | null;
  breakdownMode: BreakdownMode;
  /** Breakdown: the month of the month mode, else the period's last. */
  breakdownMonth: string | null;
  balanceGroups: GroupsShown;
  balanceAccounts: boolean;
  /** Cash flow: draw the rolling means. */
  flowRolling: boolean;
  /** The top expenses card: movers or the largest, null for the view's
   * default (see `topRanking`). */
  topBy: TagRanking | null;
  /** The Trends table: movers or the largest. */
  trendsBy: TagRanking;
}

export const TRENDS_TOP = [3, 5, 8, 12, 100];
const BREAKDOWN_MODES: BreakdownMode[] = ["total", "rolling", "month"];
const GROUPS_SHOWN: GroupsShown[] = ["stacked", "lines", "hidden"];

export const DEFAULT_PERIOD: PeriodId = "12m";

export function defaultState(): DashboardState {
  return {
    view: "overview",
    period: DEFAULT_PERIOD,
    startMonth: null,
    endMonth: null,
    accounts: [],
    groups: [],
    oneOffs: false,
    tags: [],
    netByTag: null,
    rolling: null,
    breakdownMonths: null,
    completeMonths: null,
    kind: "expenses",
    trendsSums: false,
    trendsStacked: false,
    trendsTop: 8,
    trendsSubtags: false,
    trendsOf: null,
    breakdownMode: "total",
    breakdownMonth: null,
    balanceGroups: "stacked",
    balanceAccounts: false,
    flowRolling: true,
    topBy: null,
    trendsBy: "largest",
  };
}

const MONTH = /^\d{4}-(0[1-9]|1[0-2])$/;

function flag(value: string | null): boolean | null {
  if (value === "1") return true;
  if (value === "0") return false;
  return null;
}


/** The settings in a URL query, e.g. "view=trends&period=6m". Unknown values are ignored. */
export function parseState(query: string): DashboardState {
  const params = new URLSearchParams(query);
  const settings = defaultState();
  // Old names: the ledger was "data" (the source data), and the overview was
  // "answers" (the new overview, next to an older one)
  const OLD_VIEWS: Record<string, ViewId> = { data: "ledger", answers: "overview" };
  const view = OLD_VIEWS[params.get("view") ?? ""] ?? params.get("view");
  if (VIEWS.some((item) => item.id === view)) settings.view = view as ViewId;
  const period = params.get("period");
  if (PERIODS.some((item) => item.id === period)) settings.period = period as PeriodId;
  const from = params.get("from");
  const to = params.get("to");
  if (from && MONTH.test(from)) settings.startMonth = from;
  if (to && MONTH.test(to)) settings.endMonth = to;
  // Repeated, since names may contain commas: "tags=food&tags=car"
  settings.accounts = params.getAll("account").filter(Boolean);
  settings.groups = params.getAll("group").filter(Boolean);
  settings.tags = params.getAll("tag").filter(Boolean);
  settings.oneOffs = params.get("one_offs") === "1";
  settings.netByTag = flag(params.get("net"));
  settings.rolling = params.get("rolling") || null;
  const months = Number(params.get("months"));
  settings.breakdownMonths = Number.isInteger(months) && months > 0 ? months : null;
  settings.completeMonths = flag(params.get("complete_months"));
  if (params.get("kind") === "incomes") settings.kind = "incomes";
  settings.trendsSums = params.get("trends") === "sums";
  settings.trendsStacked = params.get("stacked") === "1";
  const top = Number(params.get("top"));
  if (TRENDS_TOP.includes(top)) settings.trendsTop = top;
  settings.trendsSubtags = params.get("trends_level") === "subtag";
  settings.trendsOf = params.get("trends_of") || null;
  const mode = params.get("breakdown");
  if (BREAKDOWN_MODES.includes(mode as BreakdownMode)) settings.breakdownMode = mode as BreakdownMode;
  const month = params.get("month");
  if (month && MONTH.test(month)) settings.breakdownMonth = month;
  const groupsShown = params.get("groups_shown");
  if (GROUPS_SHOWN.includes(groupsShown as GroupsShown)) settings.balanceGroups = groupsShown as GroupsShown;
  settings.balanceAccounts = params.get("accounts_shown") === "1";
  settings.flowRolling = params.get("flow_rolling") !== "0";
  const topBy = params.get("top_by");
  if (topBy === "movers" || topBy === "largest") settings.topBy = topBy;
  if (params.get("trends_by") === "movers") settings.trendsBy = "movers";
  return settings;
}

/** The URL query of a settings, without default values. */
export function stateQuery(settings: DashboardState): string {
  const defaults = defaultState();
  const params = new URLSearchParams();
  const bit = (value: boolean) => (value ? "1" : "0");
  if (settings.view !== defaults.view) params.set("view", settings.view);
  if (settings.period !== defaults.period) params.set("period", settings.period);
  if (settings.startMonth) params.set("from", settings.startMonth);
  if (settings.endMonth) params.set("to", settings.endMonth);
  for (const account of settings.accounts) params.append("account", account);
  for (const group of settings.groups) params.append("group", group);
  for (const tag of settings.tags) params.append("tag", tag);
  if (settings.oneOffs) params.set("one_offs", "1");
  if (settings.netByTag !== null) params.set("net", bit(settings.netByTag));
  if (settings.rolling) params.set("rolling", settings.rolling);
  if (settings.breakdownMonths !== null) params.set("months", String(settings.breakdownMonths));
  if (settings.completeMonths !== null) params.set("complete_months", bit(settings.completeMonths));
  if (settings.kind !== defaults.kind) params.set("kind", settings.kind);
  if (settings.trendsSums) params.set("trends", "sums");
  if (settings.trendsStacked) params.set("stacked", "1");
  if (settings.trendsTop !== defaults.trendsTop) params.set("top", String(settings.trendsTop));
  if (settings.trendsSubtags) params.set("trends_level", "subtag");
  if (settings.trendsOf) params.set("trends_of", settings.trendsOf);
  if (settings.breakdownMode !== defaults.breakdownMode) params.set("breakdown", settings.breakdownMode);
  if (settings.breakdownMonth) params.set("month", settings.breakdownMonth);
  if (settings.balanceGroups !== defaults.balanceGroups) params.set("groups_shown", settings.balanceGroups);
  if (settings.balanceAccounts) params.set("accounts_shown", "1");
  if (!settings.flowRolling) params.set("flow_rolling", "0");
  if (settings.topBy !== null) params.set("top_by", settings.topBy);
  if (settings.trendsBy !== defaults.trendsBy) params.set("trends_by", settings.trendsBy);
  return params.toString();
}

/** How the top expenses card ranks: as chosen, else by movers on the
 * overview (whose answers are about changes) and by the largest elsewhere. */
export function topRanking(settings: DashboardState): TagRanking {
  return settings.topBy ?? (settings.view === "overview" ? "movers" : "largest");
}

/** The top expenses card's choice to keep: null if it is the view's default. */
export function rankingChoice(settings: DashboardState, choice: TagRanking): TagRanking | null {
  return choice === topRanking({ ...settings, topBy: null }) ? null : choice;
}

/** The API options of a settings (the view, kind and how views show their data aren't computed). */
export function stateOptions(settings: DashboardState): DashboardOptions {
  return {
    period: settings.period,
    start_month: settings.startMonth,
    end_month: settings.endMonth,
    accounts: settings.accounts.length ? settings.accounts : null,
    groups: settings.groups.length ? settings.groups : null,
    include_one_offs: settings.oneOffs,
    tags: settings.tags.length ? settings.tags : null,
    net_by_tag: settings.netByTag,
    breakdown_months: settings.breakdownMonths,
    // Every rolling window is computed: the chosen one isn't sent
    complete_months_only: settings.completeMonths,
  };
}

/** The dashboard's controls, in groups by what they do. */
export const CONTROL_GROUPS = [
  { id: "when", label: "When" },
  { id: "which", label: "Which money" },
  { id: "how", label: "How to compute" },
] as const;
export type ControlGroup = (typeof CONTROL_GROUPS)[number]["id"];

/** The options that differ from their defaults, by group of controls, each
 * with what it is set to, e.g. `{ how: { netByTag: "net by tag" } }`. Without
 * the defaults (while they load), any option set in the URL differs. */
export type ChangedControls = Record<ControlGroup, Partial<Record<keyof DashboardState, string>>>;

export function changedControls(settings: DashboardState, defaults: DashboardDefaults | null): ChangedControls {
  const differs = <T>(value: T | null, fallback: T | undefined) => value !== null && value !== fallback;
  const changed: ChangedControls = { when: {}, which: {}, how: {} };
  if (settings.startMonth || settings.endMonth) changed.when.startMonth = "explicit months";
  else if (settings.period !== DEFAULT_PERIOD) changed.when.period = PERIODS.find((period) => period.id === settings.period)!.title.toLowerCase();
  if (settings.accounts.length) changed.which.accounts = `only ${settings.accounts.join(", ")}`;
  if (settings.groups.length) changed.which.groups = `flows of ${settings.groups.join(", ")}`;
  if (settings.tags.length) changed.which.tags = `only the tags ${settings.tags.join(", ")}`;
  if (differs(settings.rolling, defaults ? String(defaults.rolling_months) : undefined)) changed.how.rolling = `average of ${windowName(settings.rolling!)}`;
  if (settings.oneOffs) changed.how.oneOffs = "one-offs in averages";
  if (differs(settings.netByTag, defaults?.net_by_tag)) changed.how.netByTag = settings.netByTag ? "net by tag" : "not net by tag";
  if (differs(settings.completeMonths, defaults?.complete_months_only)) {
    changed.how.completeMonths = settings.completeMonths ? "complete months only" : "with the current month";
  }
  if (differs(settings.breakdownMonths, defaults?.breakdown_months)) changed.how.breakdownMonths = `Monthly view of ${settings.breakdownMonths} months`;
  return changed;
}

/** The options "How to compute" sets back to their defaults. */
export const COMPUTE_DEFAULTS: Partial<DashboardState> = { rolling: null, oneOffs: false, netByTag: null, completeMonths: null, breakdownMonths: null };

/** What every dashboard view gets. */
export interface ViewProps {
  data: Dashboard;
  settings: DashboardState;
  /** The rolling window shown, in months, as a key of rolling means, e.g. "6". */
  rollingName: string;
  update: (changes: Partial<DashboardState>) => void;
  ondrill: (drill: Drill) => void;
}

// Months, as "YYYY-MM"

export function addMonths(month: string, count: number): string {
  const [year, number] = month.split("-").map(Number);
  const index = year * 12 + number - 1 + count;
  return `${String(Math.floor(index / 12)).padStart(4, "0")}-${String((index % 12) + 1).padStart(2, "0")}`;
}

const MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** E.g. "Jun 2025", or "Jun" with `short`. */
export function monthLabel(month: string, { short = false } = {}): string {
  const [year, number] = month.split("-");
  const name = MONTH_NAMES[Number(number) - 1];
  return short ? name : `${name} ${year}`;
}

/** E.g. "Jan 2025 – Jun 2025". */
export function periodLabel(months: string[]): string {
  if (months.length === 0) return "no months";
  if (months.length === 1) return monthLabel(months[0]);
  return `${monthLabel(months[0])} – ${monthLabel(months[months.length - 1])}`;
}

/** A date (ISO, or a chart's timestamp, which ECharts reads in local time) in
 * the date format, e.g. "15/06/2025". */
export function dateLabel(date: string | number): string {
  return showDate(typeof date === "string" ? date : isoOfLocal(new Date(date)));
}

/** E.g. "as of 15/06/2025". */
export function asOfLabel(date: string | null | undefined): string {
  return date ? `as of ${dateLabel(date)}` : "";
}

/** The months of a window ending with a month, compactly: "Mar–Aug 2026",
 * "Nov 2025–Apr 2026", or "Aug 2026" for one month. */
export function windowMonths(end: string, months: number): string {
  const start = addMonths(end, 1 - months);
  if (start === end) return monthLabel(end);
  if (start.slice(0, 4) === end.slice(0, 4)) return `${monthLabel(start, { short: true })}–${monthLabel(end)}`;
  return `${monthLabel(start)}–${monthLabel(end)}`;
}

// Rolling means: plain means of the last months, named by their window

/** E.g. "6-mo avg": a rolling mean of the window. */
export function rollingLabel(window: string): string {
  return `${window}-mo avg`;
}

/** What a month is compared with: the rolling mean of the months before it,
 * e.g. "6-mo avg", or the "previous month". */
export function baselineName(window: string): string {
  return window === "1" ? "previous month" : rollingLabel(window);
}

/** The options of the rolling window, e.g. "6 months". */
export function windowName(window: number | string): string {
  return Number(window) === 1 ? "previous month" : `${window} months`;
}

/** What averages and comparisons count: "excl. one-offs" or "incl. one-offs". */
export function oneOffBasis(included: boolean): string {
  return included ? "incl. one-offs" : "excl. one-offs";
}

/** E.g. "avg/mo, 12 mo": a monthly mean over the period's months. */
export function periodAverageLabel(months: number): string {
  return `avg/mo, ${months} mo`;
}

/** E.g. "6-mo avg before Sep (Mar–Aug 2026)": the baseline of a month. */
export function baselineTitle(window: string, month: string): string {
  const months = windowMonths(addMonths(month, -1), Number(window));
  if (window === "1") return `Previous month (${months})`;
  return `${rollingLabel(window)} before ${monthLabel(month, { short: true })} (${months})`;
}

/** E.g. "6-mo avg through Sep (Apr–Sep 2026)": a rolling mean of a month. */
export function rollingTitle(window: string, month: string): string {
  return `${rollingLabel(window)} through ${monthLabel(month, { short: true })} (${windowMonths(month, Number(window))})`;
}

/** Every month from `start` to `end`, inclusive. */
export function monthsBetween(start: string, end: string): string[] {
  const months = [];
  for (let month = start; month <= end; month = addMonths(month, 1)) months.push(month);
  return months;
}

/**
 * The months a settings asks for, as the server resolves them: a preset ends
 * with the last available month, and explicit months override its start or
 * end. Known from the URL alone (and the months the data offers), so the
 * period shows and moves at once, not when its data has loaded.
 */
export function periodMonths(settings: DashboardState, available: string[]): string[] {
  if (available.length === 0) return [];
  const last = available[available.length - 1];
  let start = available[0];
  if (settings.period === "ytd") start = `${last.slice(0, 4)}-01`;
  else if (settings.period !== "all") start = addMonths(last, 1 - Number.parseInt(settings.period));
  return monthsBetween(settings.startMonth ?? start, settings.endMonth ?? last);
}

/**
 * The period moved by whole periods (`step` -1 is the one before), as explicit
 * months. A preset becomes the months it means. Null if there's nothing
 * available that way.
 */
export function shiftPeriod(
  months: string[],
  step: number,
  available: string[],
): { startMonth: string; endMonth: string } | null {
  if (months.length === 0 || available.length === 0) return null;
  if (step < 0 && months[0] <= available[0]) return null;
  if (step > 0 && months[months.length - 1] >= available[available.length - 1]) return null;
  const count = months.length * step;
  return { startMonth: addMonths(months[0], count), endMonth: addMonths(months[months.length - 1], count) };
}

// Drilling down: the rows behind a number

export interface Drill {
  /** E.g. "food · expenses · May 2025". */
  title: string;
  selection: DashboardSelection;
  /** What the rows are, when it isn't obvious, e.g. for a rolling mean. */
  note?: string;
}

/** The note of rows behind a rolling mean: they are only one month's. */
export function rollingNote(month: string): string {
  return `A rolling mean averages several months: these are the rows of ${monthLabel(month)} alone.`;
}

/** The selection of a month's (or the period's) incomes or expenses, or the
 * like: of cash flows and breakdowns, one-offs included. */
export function flowSelection(flow: "net" | "incomes" | "expenses" | "saved"): Partial<DashboardSelection> {
  return { flow, kinds: FLOW_KINDS };
}

/** The selection of what averages and trends sum: one-offs only if included. */
export function averagedSelection(flow: Kind, settings: DashboardState): Partial<DashboardSelection> {
  return { flow, kinds: averagedKinds(settings.oneOffs) };
}

/** The groups of cash flows and breakdowns: the chosen ones, or the spending groups. */
export function shownGroups(settings: DashboardState, groups: Dashboard["groups"]): string[] {
  if (settings.groups.length) return settings.groups;
  return groups.filter((group) => group.role === "spending").map((group) => group.name);
}

/** The groups chosen after toggling one, empty if that is the default, or
 * null if it is the last one (flows need a group). */
export function toggleGroup(settings: DashboardState, groups: Dashboard["groups"], name: string): string[] | null {
  const shown = shownGroups(settings, groups);
  if (shown.length === 1 && shown[0] === name) return null;
  const next = shown.includes(name) ? shown.filter((other) => other !== name) : [...shown, name];
  const ordered = groups.map((group) => group.name).filter((group) => next.includes(group));
  const defaults = shownGroups({ ...settings, groups: [] }, groups);
  const isDefault = ordered.length === defaults.length && ordered.every((group) => defaults.includes(group));
  return isDefault ? [] : ordered;
}

/** The rows behind a click on the cash flow chart: a part of the month's
 * incomes or expenses, or what a rolling mean sums (the month's rows of it). */
export function cashFlowSelection(
  click: { kind: "net" | "incomes" | "expenses" | "saved"; month: string; part: TagKind | null; rolling: boolean },
  settings: DashboardState,
): DashboardSelection {
  if (click.rolling && click.kind !== "saved") {
    return { flow: click.kind, kinds: averagedKinds(settings.oneOffs), month: click.month };
  }
  return { ...flowSelection(click.kind), month: click.month, kind: click.part };
}

/** E.g. "vs 6-mo avg": what comparisons with a rolling mean say. */
export function baselineLabel(window: string): string {
  return `vs ${baselineName(window)}`;
}

/** The selection of a tag's (or untagged) rows. */
export function tagSelection(tag: string | null, subtag?: string | null): Partial<DashboardSelection> {
  if (tag === null) return { untagged: true };
  return subtag === undefined ? { tag } : { tag, subtag };
}

/** The first and last day (exclusive) of a month, for transaction queries. */
export function monthDates(month: string): { start_date: string; end_date: string } {
  return { start_date: `${month}-01`, end_date: `${addMonths(month, 1)}-01` };
}

/** A transactions query for some IDs, e.g. the rows of a drill-down. */
export function idsQuery(ids: (string | null)[]): TransactionQuery {
  return { ids: [...new Set(ids.filter((id): id is string => id !== null))] };
}

/** The Transactions page's URL query for IDs. */
export function transactionsQuery(ids: (string | null)[]): string {
  return `ids=${idsQuery(ids).ids!.join(",")}`;
}

// Labels and colors

export const UNTAGGED = "untagged";

export function tagLabel(tag: string | null): string {
  return tag ?? UNTAGGED;
}

/** E.g. "food / groceries", or "food / –" without a subtag, or "groceries"
 * when only one tag's subtags are shown. Untagged without a subtag. */
export function subtagLabel(tag: string | null, subtag: string | null, withTag = true): string {
  if (tag === null) return tagLabel(tag);
  return withTag ? `${tag} / ${subtag || "–"}` : subtag || "–";
}

// Distinct on a dark background, for labels without a color of their own
const PALETTE = [
  "#6ea8fe", "#f0a35e", "#63c5a0", "#e57f9a", "#b49bf0", "#e6cf6b",
  "#5cc8d6", "#d98c6a", "#9ccf6d", "#c98fd0", "#7f9cc9", "#d3a77a",
];

/** A stable color for a label: its own (from the store), else from a palette. */
export function colorOf(label: string | null, colors: Record<string, string>, fallbackIndex: number): string {
  if (label === null) return "#8f96a2";
  return colors[label] ?? PALETTE[fallbackIndex % PALETTE.length];
}

/** Colors for labels in order, keeping a label's own color. */
export function colorsFor(labels: (string | null)[], colors: Record<string, string>): Map<string | null, string> {
  const result = new Map<string | null, string>();
  let fallback = 0;
  for (const label of labels) {
    if (result.has(label)) continue;
    const own = label === null ? undefined : colors[label];
    result.set(label, own ?? colorOf(label, {}, fallback++));
  }
  return result;
}

// Numbers

const compact = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });
const whole = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

/** E.g. "12.3K", for axes. */
export function formatCompact(value: number): string {
  return compact.format(value).replace("-", "−");
}

/** E.g. "12,346", for cards and tables. */
export function formatWhole(value: number, { signed = false } = {}): string {
  const text = whole.format(Math.abs(value));
  if (Math.round(value) < 0) return `−${text}`;
  return signed && Math.round(value) > 0 ? `+${text}` : text;
}

/** The change from `previous` to `current`, as a percent (null without a base). */
export function percentChange(current: number | null, previous: number | null): number | null {
  if (current === null || previous === null || previous === 0) return null;
  return ((current - previous) / Math.abs(previous)) * 100;
}

/** A percent change as shown, e.g. "▲ 12%", and its direction once rounded
 * (so a change that rounds to 0% has no arrow). */
export function formatChange(change: number, { space = true } = {}): { text: string; direction: -1 | 0 | 1 } {
  const rounded = Math.round(change);
  if (rounded === 0) return { text: "0%", direction: 0 };
  const direction = rounded > 0 ? 1 : -1;
  return { text: `${direction > 0 ? "▲" : "▼"}${space ? " " : ""}${Math.abs(rounded)}%`, direction };
}

// Movers: tags by their change from the baseline in ₪

export interface Mover {
  tag: string | null;
  direction: Kind;
  /** The last month, as averages sum it (without one-offs, unless included). */
  amount: number;
  /** The rolling mean of the months before, null without one. */
  baseline: number | null;
  /** amount − baseline (amount if there's no baseline). */
  difference: number;
  /** In percent, null without a baseline (a new tag). */
  change: number | null;
}

/** The tags of the period's last month by how far they are from their
 * baseline in ₪, largest first (either way). Tags at 0 with a baseline of 0
 * are left out. */
export function movers(trends: Dashboard["trends"], window: string, directions: Kind[] = ["expenses", "incomes"]): Mover[] {
  const result: Mover[] = [];
  for (const direction of directions) {
    for (const trend of trends[direction]) {
      const amount = trend.amounts.at(-1) ?? 0;
      const baseline = trend.baseline[window]?.at(-1) ?? null;
      const difference = amount - (baseline ?? 0);
      if (Math.round(difference) === 0) continue;
      result.push({ tag: trend.tag, direction, amount, baseline, difference, change: percentChange(amount, baseline) });
    }
  }
  return result.sort((first, second) => Math.abs(second.difference) - Math.abs(first.difference));
}

/** Whether a mover is good news: less spent, or more earned. */
export function moverIsGood(mover: Pick<Mover, "direction" | "difference">): boolean {
  return mover.direction === "expenses" ? mover.difference < 0 : mover.difference > 0;
}

/** "Kept" months: the share of incomes not spent, as "32%" ("–" without incomes). */
export function formatRate(rate: number | null | undefined): string {
  return rate === null || rate === undefined ? "–" : `${formatWhole(rate * 100)}%`;
}
