// Charts: ECharts with only the parts the dashboard uses (so the bundle stays
// small), a dark theme matching the app, and shared option pieces.
import { BarChart, LineChart, TreemapChart } from "echarts/charts";
import {
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  MarkLineComponent,
  TooltipComponent,
} from "echarts/components";
import * as echarts from "echarts/core";
import { AxisBreak } from "echarts/features";
import { CanvasRenderer } from "echarts/renderers";
import { formatAmount } from "./money";
import { formatCompact } from "./dashboard";

echarts.use([
  BarChart,
  LineChart,
  TreemapChart,
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  MarkLineComponent,
  TooltipComponent,
  AxisBreak,
  CanvasRenderer,
]);

export { echarts };
export type EChartsOption = echarts.EChartsCoreOption;
export type ChartClick = {
  componentType: string;
  seriesType?: string;
  seriesName?: string;
  seriesIndex?: number;
  dataIndex: number;
  name: string;
  value: unknown;
  data: unknown;
};

// The app's CSS variables (see app.css), for canvas drawing
export const COLORS = {
  text: "#d9dce2",
  muted: "#8f96a2",
  faint: "#5f6672",
  border: "#2a2f38",
  grid: "#20242b",
  surface: "#171a1f",
  surface3: "#252a33",
  accent: "#7aa7f0",
  income: "#5cc596",
  expense: "#e48a8a",
  net: "#e9ecf1",
  saved: "#b49bf0",
};

const FONT = 'system-ui, -apple-system, "Segoe UI", "Noto Sans", "Noto Sans Hebrew", sans-serif';

echarts.registerTheme("finalyze", {
  backgroundColor: "transparent",
  textStyle: { fontFamily: FONT, color: COLORS.muted },
  legend: { textStyle: { color: COLORS.muted }, inactiveColor: COLORS.faint },
  categoryAxis: {
    axisLine: { lineStyle: { color: COLORS.border } },
    axisTick: { show: false },
    axisLabel: { color: COLORS.muted },
    splitLine: { show: false },
  },
  valueAxis: {
    axisLine: { show: false },
    axisLabel: { color: COLORS.faint },
    splitLine: { lineStyle: { color: COLORS.grid } },
  },
  timeAxis: {
    axisLine: { lineStyle: { color: COLORS.border } },
    axisLabel: { color: COLORS.muted },
    splitLine: { show: false },
  },
});

/** Tooltip box styling, shared by every chart. */
export const TOOLTIP = {
  backgroundColor: "rgba(29, 33, 40, 0.97)",
  borderColor: "#3a414d",
  borderWidth: 1,
  padding: [8, 10],
  textStyle: { color: COLORS.text, fontSize: 12, fontFamily: FONT },
  extraCssText: "box-shadow: 0 8px 24px rgba(0,0,0,.45); border-radius: 6px;",
  confine: true,
};

export const VALUE_AXIS = {
  type: "value",
  axisLabel: { formatter: (value: number) => formatCompact(value) },
};

export const LEGEND = {
  type: "scroll",
  top: 0,
  left: 0,
  right: 0,
  icon: "roundRect",
  itemWidth: 12,
  itemHeight: 8,
  pageIconColor: COLORS.muted,
  pageTextStyle: { color: COLORS.muted },
};

/** A tooltip line: a color dot, a name and an amount. */
export function tooltipLine(color: string, name: string, amount: number | null | undefined, extra = ""): string {
  // Charts give missing values as null, undefined or "-"
  const value = typeof amount === "number" && Number.isFinite(amount) ? formatAmount(amount) : "–";
  return (
    `<div style="display:flex;gap:10px;align-items:center;justify-content:space-between">` +
    `<span><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:${color};margin-right:6px"></span>${escapeHtml(name)}</span>` +
    `<b style="font-variant-numeric:tabular-nums;font-weight:600">${value}</b></div>` +
    (extra ? `<div style="color:${COLORS.muted};margin:0 0 4px 14px;max-width:340px;white-space:normal">${extra}</div>` : "")
  );
}

export function tooltipTitle(text: string): string {
  return `<div style="color:${COLORS.muted};margin-bottom:4px">${escapeHtml(text)}</div>`;
}

export function escapeHtml(text: string): string {
  return text.replace(/[&<>"']/g, (char) => `&#${char.charCodeAt(0)};`);
}

/** A color with transparency, from "#rgb" or "#rrggbb". */
export function withAlpha(color: string, alpha: number): string {
  let hex = color.replace("#", "");
  if (hex.length === 3) hex = [...hex].map((char) => char + char).join("");
  const value = parseInt(hex.slice(0, 6), 16);
  return `rgba(${(value >> 16) & 255}, ${(value >> 8) & 255}, ${value & 255}, ${alpha})`;
}
