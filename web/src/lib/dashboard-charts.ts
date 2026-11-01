// Chart options of the dashboard's views, built from the API's data.
import type { Dashboard } from "./api";
import { COLORS, LEGEND, TOOLTIP, VALUE_AXIS, escapeHtml, tooltipLine, tooltipTitle, withAlpha, type EChartsOption } from "./charts";
import { colorOf, dateLabel, formatCompact, monthLabel, rollingLabel, rollingTitle, tagLabel, type GroupsShown, type Kind } from "./dashboard";
import type { TagKind } from "./kinds";
import { oneLine, rowDescription } from "./descriptions";
import { formatAmount } from "./money";

type Balance = Dashboard["balance"];
type BalanceSeries = Balance["series"][number];
type TagTrend = Dashboard["trends"]["expenses"][number];
/** A trend of a tag (or subtag) with its name and color in the chart. */
export type ShownTrend = TagTrend & { label: string; color: string };

const MAX_TOOLTIP_ROWS = 6;

const MONTH_AXIS_LABEL = {
  formatter: (month: string) => {
    const [year, number] = month.split("-");
    return number === "01" ? `{year|${year}}` : monthLabel(month, { short: true });
  },
  rich: { year: { color: COLORS.text, fontWeight: 600 } },
};

function monthAxis(months: string[]) {
  return { type: "category", data: months, axisLabel: MONTH_AXIS_LABEL, axisPointer: { type: "shadow" } };
}

// Balance

/** Colors of groups (their own), by name. */
export function groupColors(groups: Dashboard["groups"]): Record<string, string> {
  return Object.fromEntries(groups.filter((group) => group.color).map((group) => [group.name, group.color!]));
}

export function balanceColor(
  series: Pick<BalanceSeries, "kind" | "label" | "account">,
  colors: Record<string, string>,
  index: number,
  ownGroupColors: Record<string, string> = {},
) {
  if (series.kind === "net_worth") return colors["net worth"] ?? COLORS.net;
  if (series.kind === "available") return colors["available"] ?? COLORS.accent;
  if (series.kind === "group") return ownGroupColors[series.label] ?? colorOf(series.label, {}, index + 3);
  return colors[series.label] ?? (series.account ? colors[series.account] : undefined) ?? colorOf(series.label, {}, index);
}

/** Balances over time, a point at the end of each day: step lines of the net
 * worth, the available balance, the groups' (stacked or not), and every
 * account and source. */
/**
 * A balance row in a tooltip, as HTML: its description (the part's, else the
 * transaction's, on one line), else the raw description in italics (see
 * TransactionDescription).
 */
export function balanceRowText(rows: Balance["rows"], row: number): string {
  const description = rowDescription({ description: rows.description[row], transaction_description: rows.transaction_description[row] });
  if (!description) return `<span dir="auto" style="font-style:italic">${escapeHtml(rows.raw_description[row])}</span>`;
  return `<span dir="auto" style="color:${COLORS.text}">${escapeHtml(oneLine(description))}</span>`;
}

export function balanceOption(
  balance: Balance,
  colors: Record<string, string>,
  {
    accounts = true,
    groups = "stacked",
    ownGroupColors = {},
  }: { accounts?: boolean; groups?: GroupsShown; ownGroupColors?: Record<string, string> } = {},
): EChartsOption {
  const shown = balance.series.filter(
    (series) => (accounts || series.kind !== "account_source") && (groups !== "hidden" || series.kind !== "group"),
  );
  const rows = balance.rows;
  return {
    animationDuration: 500,
    grid: { left: 8, right: 16, top: 36, bottom: 56, containLabel: true },
    legend: { ...LEGEND, data: shown.map((series) => series.label) },
    tooltip: {
      ...TOOLTIP,
      trigger: "axis",
      axisPointer: { type: "line", lineStyle: { color: COLORS.faint } },
      formatter: (params: { seriesName: string; color: string; value: [string, number, number[]] }[]) => {
        if (!params.length) return "";
        // The day's rows of every line shown (lines share rows, e.g. the net
        // worth's include the available balance's)
        const dayRows = new Set<number>();
        for (const param of params) for (const row of param.value[2] ?? []) dayRows.add(row);
        const lines = [tooltipTitle(`${dateLabel(params[0].value[0])} · end of day`)];
        for (const param of params) lines.push(tooltipLine(param.color, param.seriesName, param.value[1]));
        if (dayRows.size) {
          const shown = [...dayRows].slice(0, MAX_TOOLTIP_ROWS);
          lines.push(`<div style="margin-top:6px;border-top:1px solid ${COLORS.border};padding-top:5px;color:${COLORS.muted}">`);
          for (const row of shown) {
            const tags = rows.tags[row] ? ` <span style="color:${COLORS.faint}">${escapeHtml(rows.tags[row]!)}</span>` : "";
            lines.push(
              `<div style="display:flex;justify-content:space-between;gap:12px"><span>${balanceRowText(rows, row)}${tags}</span>` +
                `<span style="font-variant-numeric:tabular-nums;color:${rows.amount[row] < 0 ? COLORS.expense : COLORS.income}">${formatAmount(rows.amount[row], { signed: true })}</span></div>`,
            );
          }
          if (dayRows.size > shown.length) lines.push(`<div>and ${dayRows.size - shown.length} more</div>`);
          lines.push("</div>");
        }
        return lines.join("");
      },
    },
    xAxis: { type: "time", axisLabel: { hideOverlap: true } },
    yAxis: { ...VALUE_AXIS, scale: true },
    dataZoom: [
      { type: "inside", filterMode: "none" },
      {
        type: "slider",
        height: 22,
        bottom: 8,
        borderColor: COLORS.border,
        fillerColor: withAlpha(COLORS.accent, 0.12),
        dataBackground: { lineStyle: { color: COLORS.faint }, areaStyle: { color: withAlpha(COLORS.faint, 0.2) } },
        textStyle: { color: COLORS.muted },
        labelFormatter: (value: number) => dateLabel(value),
        handleStyle: { color: COLORS.surface3, borderColor: COLORS.faint },
      },
    ],
    series: shown.map((series, index) => {
      const color = balanceColor(series, colors, index, ownGroupColors);
      const main = series.kind === "net_worth" || series.kind === "available";
      const stackedGroup = series.kind === "group" && groups === "stacked";
      return {
        name: series.label,
        type: "line",
        step: "end",
        showSymbol: false,
        symbolSize: 5,
        color,
        stack: stackedGroup ? "groups" : undefined,
        // Signed sums, so that debts take from the stack: its top is the net worth
        stackStrategy: stackedGroup ? "all" : undefined,
        lineStyle: { width: main ? 2.2 : stackedGroup ? 0.8 : 1.4, opacity: main ? 1 : 0.85, type: series.kind === "group" && !stackedGroup ? "dashed" : "solid" },
        emphasis: { focus: "series", lineStyle: { width: 3 } },
        areaStyle: stackedGroup
          ? { color: withAlpha(color, 0.28) }
          : series.kind === "available"
            ? {
                color: {
                  type: "linear",
                  x: 0,
                  y: 0,
                  x2: 0,
                  y2: 1,
                  colorStops: [
                    { offset: 0, color: withAlpha(color, 0.22) },
                    { offset: 1, color: withAlpha(color, 0) },
                  ],
                },
              }
            : undefined,
        z: main ? 3 : stackedGroup ? 1 : 2,
        data: stackedGroup
          ? groupPoints(series, balance.series.find((other) => other.kind === "net_worth"))
          : series.dates.map((date, point) => [date, series.values[point], series.rows[point]]),
      };
    }),
  };
}

/** The net worth and the available balance only, as step lines with their
 * last value at their end, for a compact preview. */
export function balanceLinesOption(balance: Balance, colors: Record<string, string>, { hidden = [] }: { hidden?: string[] } = {}): EChartsOption {
  const shown = balance.series.filter((series) => series.kind === "net_worth" || series.kind === "available");
  return {
    animationDuration: 500,
    grid: { left: 8, right: 76, top: 12, bottom: 4, containLabel: true },
    legend: { show: false, selected: Object.fromEntries(hidden.map((name) => [name, false])) },
    tooltip: {
      ...TOOLTIP,
      trigger: "axis",
      axisPointer: { type: "line", lineStyle: { color: COLORS.faint } },
      formatter: (params: { seriesName: string; color: string; value: [string, number] }[]) =>
        params.length
          ? [tooltipTitle(`${dateLabel(params[0].value[0])} · end of day`), ...params.map((param) => tooltipLine(param.color, param.seriesName, param.value[1]))].join("")
          : "",
    },
    xAxis: { type: "time", axisLabel: { hideOverlap: true } },
    yAxis: { ...VALUE_AXIS, scale: true },
    series: shown.map((series, index) => {
      const color = balanceColor(series, colors, index);
      return {
        name: series.label,
        type: "line",
        step: "end",
        showSymbol: false,
        color,
        lineStyle: { width: 2 },
        emphasis: { focus: "series" },
        endLabel: { show: true, color, fontSize: 11, formatter: (param: { value: [string, number] }) => formatCompact(param.value[1]) },
        data: series.dates.map((date, point) => [date, series.values[point], series.rows[point]]),
      };
    }),
  };
}

/** A group's balance at every date of the net worth, so that stacked areas add
 * up at every point (lines only have points where they change). */
function groupPoints(series: BalanceSeries, netWorth: BalanceSeries | undefined) {
  if (!netWorth) return series.dates.map((date, point) => [date, series.values[point], series.rows[point]]);
  const dates = [...new Set([...netWorth.dates, ...series.dates])].sort();
  let point = -1;
  return dates.map((date) => {
    while (point + 1 < series.dates.length && series.dates[point + 1] <= date) point++;
    const rows = point >= 0 && series.dates[point] === date ? series.rows[point] : [];
    return [date, point >= 0 ? series.values[point] : 0, rows];
  });
}

// Cash flow

export const FLOW_NAMES = { incomes: "Incomes", expenses: "Expenses", net: "Net", saved: "Set aside" } as const;
const ONE_OFF_INCOMES = "One-off incomes";
const ONE_OFF_EXPENSES = "One-off expenses";
/** Series drawn below zero, whose values are shown positive. */
const DOWNWARD = [FLOW_NAMES.expenses, ONE_OFF_EXPENSES, FLOW_NAMES.saved];
const ONE_OFF_DECAL = { symbol: "rect", dashArrayX: [1, 0], dashArrayY: [2, 3], rotation: -Math.PI / 4, color: "rgba(0,0,0,0.35)" };

/** The drawn parts of a month's incomes or expenses: the one-off part, within
 * 0 and the total (a refunded one-off only lowers the total, and a one-off
 * larger than the total fills it), and the rest, so that the bar's height is
 * the total and both parts are on one side of 0. */
export function flowParts(total: number, oneOff: number): { regular: number; oneOff: number } {
  const drawnOneOff = Math.min(Math.max(0, oneOff), Math.max(0, total));
  return { regular: total - drawnOneOff, oneOff: drawnOneOff };
}

/** How far beyond the routine months a month must reach for the axis to
 * break, and where breaks start and end around them. */
const BREAK_RATIO = 2.5;
const BREAK_MARGIN = 1.12;
const BREAK_KEEP = 0.92;

/** Axis breaks that keep months far beyond the routine ones (e.g. a large
 * one-off) from setting the scale: on each side of 0, from a bit beyond the
 * routine months (without their one-offs) to a bit before the largest bar.
 * None on a side without such a month. */
export function flowBreaks(cashFlow: Dashboard["cash_flow"]): { start: number; end: number; gap: string }[] {
  const months = cashFlow.months.map((_, index) => index);
  const incomes = months.map((index) => flowParts(cashFlow.incomes[index], cashFlow.one_offs.incomes[index]));
  const expenses = months.map((index) => flowParts(cashFlow.expenses[index], cashFlow.one_offs.expenses[index]));
  const net = cashFlow.net;
  // The net without the one-offs, for the routine months
  const regularNet = months.map((index) => net[index] - incomes[index].oneOff + expenses[index].oneOff);
  const sides = [
    {
      sign: 1,
      routine: Math.max(0, ...incomes.map((part) => part.regular), ...regularNet),
      largest: Math.max(0, ...incomes.map((part) => part.regular + part.oneOff), ...net),
    },
    {
      sign: -1,
      routine: Math.max(0, ...months.map((index) => expenses[index].regular + cashFlow.saved[index]), ...regularNet.map((value) => -value)),
      largest: Math.max(0, ...months.map((index) => expenses[index].regular + expenses[index].oneOff + cashFlow.saved[index]), ...net.map((value) => -value)),
    },
  ];
  const breaks = [];
  for (const side of sides) {
    if (side.routine <= 0 || side.largest < side.routine * BREAK_RATIO) continue;
    const near = side.routine * BREAK_MARGIN;
    const far = side.largest * BREAK_KEEP;
    if (far <= near) continue;
    const [start, end] = side.sign > 0 ? [near, far] : [-far, -near];
    breaks.push({ start: Math.round(start), end: Math.round(end), gap: "4%" });
  }
  return breaks;
}

/** Monthly incomes (up) and expenses and money set aside (down) as bars, with
 * their one-off parts hatched, the net as a line, and rolling means. Months
 * far beyond the others break the axis (click a break to expand it).
 * `hidden` series aren't drawn. */
export function cashFlowOption(
  cashFlow: Dashboard["cash_flow"],
  window: string,
  {
    compact = false,
    rolling = true,
    legend = true,
    hidden = [],
  }: { compact?: boolean; rolling?: boolean; legend?: boolean; hidden?: string[] } = {},
): EChartsOption {
  const months = cashFlow.months;
  // The mean of one month is the month itself: not drawn again
  const means = window === "1" ? undefined : cashFlow.rolling[window];
  const parts = (kind: "incomes" | "expenses") => cashFlow[kind].map((total, index) => flowParts(total, cashFlow.one_offs[kind][index]));
  const incomes = parts("incomes");
  const expenses = parts("expenses");
  const rollingSeries =
    rolling && means
      ? (["net", "incomes", "expenses"] as const).map((kind) => ({
          name: `${FLOW_NAMES[kind]} · ${rollingLabel(window)}`,
          type: "line",
          smooth: true,
          showSymbol: false,
          connectNulls: true,
          color: kind === "net" ? withAlpha(COLORS.net, 0.7) : kind === "incomes" ? "#8fe0bb" : "#f2b3b3",
          lineStyle: { width: 1.6, type: "dashed" },
          data: means[kind].map((value) => (value === null ? null : kind === "expenses" ? -value : value)),
          z: 4,
        }))
      : [];
  return {
    animationDuration: 500,
    grid: { left: 8, right: 16, top: legend ? (compact ? 28 : 36) : 10, bottom: 4, containLabel: true },
    legend: { ...LEGEND, show: legend, selected: Object.fromEntries(hidden.map((name) => [name, false])) },
    tooltip: {
      ...TOOLTIP,
      trigger: "axis",
      axisPointer: { type: "shadow", shadowStyle: { color: "rgba(255,255,255,0.04)" } },
      formatter: (params: { seriesName: string; color: string; value: number | null; dataIndex: number }[]) => {
        if (!params.length) return "";
        const index = params[0].dataIndex;
        const month = months[index];
        const lines = [tooltipTitle(monthLabel(month))];
        // The totals and their actual one-off parts, not the drawn parts
        const actual: Record<string, number> = {
          [FLOW_NAMES.incomes]: cashFlow.incomes[index],
          [ONE_OFF_INCOMES]: cashFlow.one_offs.incomes[index],
          [FLOW_NAMES.expenses]: cashFlow.expenses[index],
          [ONE_OFF_EXPENSES]: cashFlow.one_offs.expenses[index],
        };
        for (const param of params) {
          const downward = DOWNWARD.some((name) => param.seriesName.startsWith(name));
          let value = param.value === null ? null : downward ? -param.value : param.value;
          if (param.seriesName in actual) value = actual[param.seriesName];
          if (param.seriesName.startsWith("One-off")) {
            if (!value) continue;
            lines.push(tooltipLine(param.color, "of which one-off", value));
            continue;
          }
          const name = means && param.seriesName.includes(" avg") ? `${param.seriesName.split(" · ")[0]} · ${rollingTitle(window, month)}` : param.seriesName;
          lines.push(tooltipLine(param.color, name, value));
        }
        return lines.join("") + (compact ? "" : `<div style="color:${COLORS.faint};margin-top:4px">Click for the rows</div>`);
      },
    },
    xAxis: monthAxis(months),
    yAxis: {
      ...VALUE_AXIS,
      // The ends of a break aren't labelled: the zigzag says the axis skips
      axisLabel: {
        formatter: (value: number, _index: number, extra?: { break?: unknown }) => (extra?.break ? "" : formatCompact(value)),
      },
      breaks: flowBreaks(cashFlow),
      breakArea: { expandOnClick: true, zigzagAmplitude: 3, itemStyle: { color: COLORS.surface, borderColor: COLORS.faint, opacity: 0.9 } },
    },
    series: [
      {
        name: FLOW_NAMES.incomes,
        type: "bar",
        stack: "flow",
        color: COLORS.income,
        barMaxWidth: 28,
        itemStyle: { opacity: 0.85 },
        emphasis: { itemStyle: { opacity: 1 } },
        data: incomes.map((part) => part.regular),
      },
      {
        name: ONE_OFF_INCOMES,
        type: "bar",
        stack: "flow",
        color: withAlpha(COLORS.income, 0.55),
        barMaxWidth: 28,
        itemStyle: { decal: ONE_OFF_DECAL },
        data: incomes.map((part) => part.oneOff),
      },
      {
        name: FLOW_NAMES.expenses,
        type: "bar",
        stack: "flow",
        color: COLORS.expense,
        barMaxWidth: 28,
        itemStyle: { opacity: 0.85 },
        emphasis: { itemStyle: { opacity: 1 } },
        data: expenses.map((part) => -part.regular),
      },
      {
        name: ONE_OFF_EXPENSES,
        type: "bar",
        stack: "flow",
        color: withAlpha(COLORS.expense, 0.55),
        barMaxWidth: 28,
        itemStyle: { decal: ONE_OFF_DECAL },
        data: expenses.map((part) => -part.oneOff),
      },
      {
        name: FLOW_NAMES.saved,
        type: "bar",
        stack: "flow",
        color: COLORS.saved,
        barMaxWidth: 28,
        itemStyle: { opacity: 0.85 },
        data: cashFlow.saved.map((value) => -value),
      },
      {
        name: FLOW_NAMES.net,
        type: "line",
        color: COLORS.net,
        symbol: "circle",
        symbolSize: compact ? 4 : 6,
        lineStyle: { width: 2 },
        data: cashFlow.net,
        z: 5,
        markLine: compact
          ? undefined
          : { silent: true, symbol: "none", lineStyle: { color: COLORS.faint, type: "solid", width: 1 }, label: { show: false }, data: [{ yAxis: 0 }] },
      },
      ...rollingSeries,
    ],
  };
}

/** What a click on the cash flow chart selects: the flow and month, which
 * part of incomes or expenses (regular or one-off), and whether it was on a
 * rolling mean. */
export function cashFlowClick(months: string[], seriesName: string | undefined, dataIndex: number) {
  const name = (seriesName ?? "").toLowerCase();
  const kind = name.includes("incomes")
    ? "incomes"
    : name.includes("expenses")
      ? "expenses"
      : name.startsWith(FLOW_NAMES.saved.toLowerCase())
        ? "saved"
        : "net";
  const rolling = name.includes(" avg");
  const part: TagKind | null =
    rolling || kind === "net" || kind === "saved" ? null : name.startsWith("one-off") ? "one-off" : "regular";
  return { kind, month: months[dataIndex], part, rolling } as const;
}

// Trends

/** Monthly amounts (or rolling means) by tag or subtag, as lines (or stacked
 * bars). The top ones are shown at first. */
export function trendsOption(
  trends: ShownTrend[],
  months: string[],
  { window, rolling = true, shownTags = 8, stacked = false }: { window: string; rolling?: boolean; shownTags?: number; stacked?: boolean },
): EChartsOption {
  const selected: Record<string, boolean> = {};
  trends.forEach((trend, index) => (selected[trend.label] = index < shownTags));
  return {
    animationDuration: 500,
    grid: { left: 8, right: 16, top: 40, bottom: 4, containLabel: true },
    legend: { ...LEGEND, selected, data: trends.map((trend) => trend.label) },
    tooltip: {
      ...TOOLTIP,
      trigger: "axis",
      order: "valueDesc",
      formatter: (params: { seriesName: string; color: string; value: number | null; dataIndex: number }[]) => {
        if (!params.length) return "";
        // Tags without a value this month (e.g. no rolling mean yet) are left out
        const sorted = params
          .filter((param) => typeof param.value === "number" && Number.isFinite(param.value))
          .sort((first, second) => (second.value ?? 0) - (first.value ?? 0));
        return [tooltipTitle(monthLabel(months[params[0].dataIndex])), ...sorted.map((param) => tooltipLine(param.color, param.seriesName, param.value))].join("");
      },
    },
    xAxis: { ...monthAxis(months), boundaryGap: stacked ? true : false, axisPointer: { type: "line" } },
    yAxis: { ...VALUE_AXIS },
    series: trends.map((trend) => {
      return {
        name: trend.label,
        type: stacked ? "bar" : "line",
        stack: stacked ? "tags" : undefined,
        color: trend.color,
        smooth: rolling && !stacked ? 0.3 : false,
        connectNulls: true,
        symbol: "circle",
        symbolSize: 6,
        lineStyle: { width: 2 },
        barMaxWidth: 30,
        emphasis: { focus: "series" },
        data: rolling ? (trend.rolling[window] ?? []) : trend.amounts,
      };
    }),
  };
}

// Breakdown

export interface BreakdownItem {
  tag: string | null;
  subtag: string | null;
  kind?: TagKind;
  amount: number;
}

/** Items grouped by tag, largest first, with their subtags largest first. */
export function groupByTag(items: BreakdownItem[]) {
  const groups = new Map<string | null, { tag: string | null; amount: number; subtags: BreakdownItem[] }>();
  for (const item of items) {
    const group = groups.get(item.tag) ?? { tag: item.tag, amount: 0, subtags: [] };
    group.amount += item.amount;
    group.subtags.push(item);
    groups.set(item.tag, group);
  }
  const sorted = [...groups.values()].sort((first, second) => second.amount - first.amount);
  for (const group of sorted) group.subtags.sort((first, second) => second.amount - first.amount);
  return sorted;
}

/** The (signed) total of breakdown items, which percents are of. */
export function breakdownTotal(items: BreakdownItem[]): number {
  return items.reduce((sum, item) => sum + item.amount, 0);
}

/** A treemap of tags and their subtags. Only positive amounts have an area,
 * and percents are of the signed total, as in the sorted list. */
export function treemapOption(items: BreakdownItem[], colors: Map<string | null, string>, kind: Kind): EChartsOption {
  const groups = groupByTag(items).filter((group) => group.amount > 0);
  const total = breakdownTotal(items) || 1;
  const percent = (value: number) => `${((value / total) * 100).toFixed(Math.abs(value / total) < 0.1 ? 1 : 0)}%`;
  const text = { color: "#f4f6fa", textShadowColor: "rgba(0,0,0,0.55)", textShadowBlur: 3 };
  return {
    animationDurationUpdate: 500,
    tooltip: {
      ...TOOLTIP,
      formatter: (param: { data: { tag: string | null; subtag?: string | null; value: number }; color: string }) => {
        const { tag, subtag, value } = param.data;
        const name = subtag === undefined ? tagLabel(tag) : `${tagLabel(tag)} / ${subtag || "–"}`;
        const oneOff = (param.data as { kind?: TagKind }).kind === "one-off" ? " · one-off" : "";
        return tooltipLine(param.color, name, value, `${percent(value)} of ${kind}${oneOff} · click for the rows`);
      },
    },
    series: [
      {
        type: "treemap",
        roam: false,
        nodeClick: false,
        breadcrumb: { show: false },
        width: "100%",
        height: "100%",
        top: 0,
        left: 0,
        squareRatio: 1,
        visibleMin: 1,
        levels: [
          // The whole: no label, gaps between tags
          { itemStyle: { borderWidth: 0, gapWidth: 3 }, upperLabel: { show: false } },
          // Tags: their name on top
          {
            itemStyle: { borderWidth: 3, gapWidth: 1, borderRadius: 4 },
            upperLabel: {
              show: true,
              height: 22,
              ...text,
              fontWeight: 600,
              overflow: "truncate",
              formatter: (param: { data: { tag: string | null; value: number } }) =>
                `${tagLabel(param.data.tag)}  ${formatCompact(param.data.value)} · ${percent(param.data.value)}`,
            },
          },
          // Subtags: lighter the smaller
          { colorAlpha: [0.45, 0.9], itemStyle: { borderWidth: 0, gapWidth: 1, borderRadius: 2 } },
        ],
        label: {
          show: true,
          ...text,
          overflow: "truncate",
          formatter: (param: { data: { subtag?: string | null; kind?: TagKind; value: number } }) =>
            `{name|${param.data.kind === "one-off" ? "◆ " : ""}${param.data.subtag || "–"}}\n{value|${formatCompact(param.data.value)}}`,
          rich: {
            name: { fontSize: 12, lineHeight: 16, ...text },
            value: { fontSize: 11, lineHeight: 15, color: "rgba(244,246,250,0.75)" },
          },
        },
        data: groups.map((group) => {
          const color = colors.get(group.tag) ?? COLORS.muted;
          return {
            name: tagLabel(group.tag),
            tag: group.tag,
            value: group.amount,
            itemStyle: { color, borderColor: mix(color, COLORS.surface, 0.55) },
            children: group.subtags.filter((item) => item.amount > 0).map((item) => ({
              name: item.subtag ?? "",
              tag: item.tag,
              subtag: item.subtag,
              kind: item.kind,
              value: item.amount,
              itemStyle: { color: mix(color, COLORS.surface, 0.25) },
            })),
          };
        }),
      },
    ],
  };
}

/** A color moved toward another, e.g. toward the background to calm it. */
export function mix(color: string, toward: string, amount: number): string {
  const parse = (value: string) => {
    let hex = value.replace("#", "");
    if (hex.length === 3) hex = [...hex].map((char) => char + char).join("");
    const number = parseInt(hex.slice(0, 6), 16);
    return [(number >> 16) & 255, (number >> 8) & 255, number & 255];
  };
  const from = parse(color);
  const to = parse(toward);
  const channels = from.map((channel, index) => Math.round(channel + (to[index] - channel) * amount));
  return `#${channels.map((channel) => channel.toString(16).padStart(2, "0")).join("")}`;
}
