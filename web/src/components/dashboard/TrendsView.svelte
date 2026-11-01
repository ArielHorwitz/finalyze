<!--
  Incomes or expenses of every tag (or subtag, of every tag or one) over time:
  rolling means or monthly sums, as lines or stacked bars. Then a table of
  them: the last month against its baseline (in ₪ and %), ranked by the
  change in ₪ (movers) or by total.
-->
<script lang="ts">
  import Chart from "../Chart.svelte";
  import Panel from "./Panel.svelte";
  import Term from "../Term.svelte";
  import type { ChartClick } from "../../lib/charts";
  import { trendsOption, type ShownTrend } from "../../lib/dashboard-charts";
  import {
    averagedSelection,
    baselineLabel,
    baselineName,
    baselineTitle,
    colorsFor,
    formatChange,
    formatWhole,
    monthLabel,
    oneOffBasis,
    percentChange,
    periodAverageLabel,
    rollingLabel,
    rollingNote,
    subtagLabel,
    tagLabel,
    tagSelection,
    TRENDS_TOP,
    type ViewProps,
  } from "../../lib/dashboard";
  import { bind, useKeys } from "../../lib/use-keys.svelte";
  import { keyContext, keyName } from "../../lib/state.svelte";

  let { data, settings, rollingName, update, ondrill }: ViewProps = $props();

  const rolling = $derived(!settings.trendsSums);
  const stacked = $derived(settings.trendsStacked);
  const shownTags = $derived(settings.trendsTop);
  const kind = $derived(settings.kind);
  const months = $derived(data.period.months);
  const tagColors = $derived(colorsFor(data.trends.tag_order, data.colors));
  const bySubtag = $derived(settings.trendsSubtags);
  // Loaded only in subtag mode: none until they arrive
  const subtagTrends = $derived(data.trends.subtags?.[kind] ?? []);
  /** Tags whose subtags can be shown alone: those with subtag trends, and the chosen one. */
  const ofTags = $derived.by(() => {
    const tags = new Set(subtagTrends.map((trend) => trend.tag).filter((tag): tag is string => tag !== null));
    if (settings.trendsOf) tags.add(settings.trendsOf);
    return [...tags].sort();
  });
  const of = $derived(bySubtag ? settings.trendsOf : null);
  const trends: ShownTrend[] = $derived.by(() => {
    if (!bySubtag) {
      return data.trends[kind].map((trend) => ({ ...trend, label: tagLabel(trend.tag), color: tagColors.get(trend.tag) ?? "" }));
    }
    const items = subtagTrends.filter((trend) => of === null || trend.tag === of);
    const labels = items.map((trend) => subtagLabel(trend.tag, trend.subtag ?? null, of === null));
    const colors = colorsFor(labels, {});
    return items.map((trend, index) => ({
      ...trend,
      label: labels[index],
      color: (trend.tag === null ? tagColors.get(null) : colors.get(labels[index])) ?? "",
    }));
  });
  const level = $derived(bySubtag ? "subtag" : "tag");
  const basis = $derived(oneOffBasis(settings.oneOffs));
  const summary = $derived(
    trends
      .map((trend) => {
      const total = trend.amounts.reduce((sum, value) => sum + value, 0);
      const average = months.length ? total / months.length : 0;
      const last = trend.amounts.at(-1) ?? 0;
      const before = trend.amounts.at(-2) ?? null;
      const baseline = trend.baseline[rollingName]?.at(-1) ?? null;
      return {
        tag: trend.tag,
        subtag: trend.subtag,
        label: trend.label,
        color: trend.color,
        total,
        average,
        last,
        baseline,
        difference: last - (baseline ?? 0),
        change: percentChange(last, baseline),
        previousChange: before === null ? null : percentChange(last, before),
        amounts: trend.amounts,
      };
      })
      .sort((first, second) => (settings.trendsBy === "movers" ? Math.abs(second.difference) - Math.abs(first.difference) : 0)),
  );

  function drill(trend: { tag: string | null; subtag?: string | null }, month: string, fromRolling = false) {
    const subtag = bySubtag ? trend.subtag : undefined;
    ondrill({
      title: `${subtag === undefined ? tagLabel(trend.tag) : subtagLabel(trend.tag, subtag)} · ${kind} · ${monthLabel(month)}`,
      selection: { ...averagedSelection(kind, settings), month, ...tagSelection(trend.tag, subtag) },
      note: fromRolling ? rollingNote(month) : undefined,
    });
  }

  function onClick(event: ChartClick) {
    const trend = trends.find((item) => item.label === event.seriesName);
    if (trend) drill(trend, months[event.dataIndex], rolling);
  }

  /** A tiny line of the monthly sums, as an SVG path: its own scale, which
   * includes 0 (so negative months are below it), and a flat line at 0 if
   * every month is 0. */
  function sparkline(values: number[], width = 90, height = 20): string {
    if (values.length < 2) return "";
    const low = Math.min(0, ...values);
    const high = Math.max(0, ...values);
    const span = high - low || 1;
    return values
      .map((value, index) => `${index ? "L" : "M"}${((index / (values.length - 1)) * width).toFixed(1)},${(height - ((value - low) / span) * height).toFixed(1)}`)
      .join(" ");
  }

  useKeys("Trends", () => [
    bind("trends.mode", () => update({ trendsSums: rolling })),
    bind("trends.stacked", () => update({ trendsStacked: !stacked })),
    bind("trends.subtags", () => update({ trendsSubtags: !bySubtag })),
  ]);
</script>

<div class="toolbar">
  <div class="segmented" role="group" aria-label="Incomes or expenses">
    <button class:on={kind === "expenses"} onclick={() => update({ kind: "expenses" })} title={keyName("dashboard.kind")}><Term key="expenses" context={keyContext("dashboard.kind")}>Expenses</Term></button>
    <button class:on={kind === "incomes"} onclick={() => update({ kind: "incomes" })} title={keyName("dashboard.kind")}><Term key="incomes" context={keyContext("dashboard.kind")}>Incomes</Term></button>
  </div>
  <div class="segmented" role="group" aria-label="By tag or by subtag">
    <button class:on={!bySubtag} onclick={() => update({ trendsSubtags: false })} title={keyName("trends.subtags")}><Term key="trends_by_subtag" context={keyContext("trends.subtags")}>By tag</Term></button>
    <button class:on={bySubtag} onclick={() => update({ trendsSubtags: true })} title={keyName("trends.subtags")}><Term key="trends_by_subtag" context={keyContext("trends.subtags")}>By subtag</Term></button>
  </div>
  {#if bySubtag}
    <label class="muted">
      <Term key="trends_by_subtag">of</Term>
      <select value={settings.trendsOf ?? ""} onchange={(event) => update({ trendsOf: event.currentTarget.value || null })} aria-label="Subtags of">
        <option value="">every tag</option>
        {#each ofTags as tag}<option value={tag}>{tag}</option>{/each}
      </select>
    </label>
  {/if}
  <div class="segmented" role="group" aria-label="Values">
    <button class:on={rolling} onclick={() => update({ trendsSums: false })} title={keyName("trends.mode")}><Term key="trends_values" context={keyContext("trends.mode")}>{rollingLabel(rollingName)}</Term></button>
    <button class:on={!rolling} onclick={() => update({ trendsSums: true })} title={keyName("trends.mode")}><Term key="trends_values" context={keyContext("trends.mode")}>Monthly sums</Term></button>
  </div>
  <div class="segmented" role="group" aria-label="Chart">
    <button class:on={!stacked} onclick={() => update({ trendsStacked: false })} title={keyName("trends.stacked")}><Term key="trends_values" context={keyContext("trends.stacked")}>Lines</Term></button>
    <button class:on={stacked} onclick={() => update({ trendsStacked: true })} title={keyName("trends.stacked")}><Term key="trends_values" context={keyContext("trends.stacked")}>Stacked</Term></button>
  </div>
  <label class="muted">
    <Term key="first_shown">first shown</Term>
    <select value={shownTags} onchange={(event) => update({ trendsTop: Number(event.currentTarget.value) })}>
      {#each TRENDS_TOP as count}<option value={count}>{count === 100 ? "all" : `top ${count}`}</option>{/each}
    </select>
  </label>
  <span class="faint">Click legend items to show or hide {level}s.</span>
</div>

<Panel title="{kind === 'expenses' ? 'Expenses' : 'Incomes'}{of === null ? '' : ` of ${of}`} by {level}" term="view_trends" subtitle="click a point for its rows">
  {#key `${kind}-${shownTags}-${level}-${of}`}
    <Chart
      label="Trends by {level}"
      height="460px"
      option={trendsOption(trends, months, { window: rollingName, rolling, shownTags, stacked })}
      onclick={onClick}
    />
  {/key}
</Panel>

<Panel title={bySubtag ? "Subtags" : "Tags"} term="trends_table" subtitle="the last month against the {baselineName(rollingName)}{rollingName === '1' ? '' : ' of the months before'}, {basis}">
  {#snippet actions()}
    <div class="segmented" role="group" aria-label="Rank by">
      <button class:on={settings.trendsBy === "movers"} onclick={() => update({ trendsBy: "movers" })}><Term key="movers" context="The largest changes from the baseline in ₪ first, either way">movers</Term></button>
      <button class:on={settings.trendsBy === "largest"} onclick={() => update({ trendsBy: "largest" })}><Term key="largest" context="The largest totals of the period first">largest</Term></button>
    </div>
  {/snippet}
  <table class="grid tags">
    <thead>
      <tr>
        <th>{#if bySubtag}<Term key="tag">Subtag</Term>{:else}<Term key="tag">Tag</Term>{/if}</th>
        <th><Term key="sparkline">Monthly</Term></th>
        <th class="amount"><Term key="trends_total" context={basis}>Total</Term></th>
        <th class="amount"><Term key="period_average" context={basis}>{periodAverageLabel(months.length)}</Term></th>
        <th class="amount"><Term key="last_month_amount" context={basis}>{months.length ? monthLabel(months[months.length - 1]) : "Last"}</Term></th>
        <th class="amount"><Term key="baseline" context={months.length ? baselineTitle(rollingName, months[months.length - 1]) : ""}>{baselineName(rollingName)} before</Term></th>
        <th class="amount"><Term key="change_amount">Change ₪</Term></th>
        <th class="amount"><Term key="change_percent">{baselineLabel(rollingName)}</Term></th>
      </tr>
    </thead>
    <tbody>
      {#each summary as row (JSON.stringify([row.tag, row.subtag]))}
        <tr>
          <td>
            <span class="swatch" style:background={row.color}></span>
            {#if !bySubtag && row.tag !== null}
              <button class="cell" onclick={() => update({ trendsSubtags: true, trendsOf: row.tag })} title="Show the subtags of {row.tag}">{row.label}</button>
            {:else}
              <span class:untagged={row.tag === null}>{row.label}</span>
            {/if}
          </td>
          <td>
            <svg width="90" height="20" class="spark" aria-hidden="true">
              <path d={sparkline(row.amounts)} stroke={row.color} fill="none" stroke-width="1.5" />
            </svg>
          </td>
          <td class="amount">{formatWhole(row.total)}</td>
          <td class="amount muted">{formatWhole(row.average)}</td>
          <td class="amount"><button class="cell" onclick={() => drill(row, months[months.length - 1])}>{formatWhole(row.last)}</button></td>
          <td class="amount muted">{row.baseline === null ? "–" : formatWhole(row.baseline)}</td>
          <td
            class="amount"
            class:bad={Math.round(row.difference) !== 0 && (kind === "expenses" ? row.difference > 0 : row.difference < 0)}
            class:good={Math.round(row.difference) !== 0 && (kind === "expenses" ? row.difference < 0 : row.difference > 0)}
          >
            {Math.round(row.difference) === 0 ? "" : formatWhole(row.difference, { signed: true })}
          </td>
          <td
            class="amount change"
            title="{months.length ? baselineTitle(rollingName, months[months.length - 1]) : ''}: {row.baseline === null ? '–' : formatWhole(row.baseline)}{row.previousChange === null ? '' : `\nvs month before: ${formatChange(row.previousChange).text}`}"
            class:bad={kind === "expenses" ? (row.change ?? 0) > 10 : (row.change ?? 0) < -10}
            class:good={kind === "expenses" ? (row.change ?? 0) < -10 : (row.change ?? 0) > 10}
          >
            {#if row.change !== null}{formatChange(row.change).text}{:else if row.last && !row.baseline}<Term key="new_tag">new</Term>{/if}
          </td>
        </tr>
      {:else}
        <tr><td colspan="8" class="faint">No {kind}{of === null ? "" : ` of ${of}`} in these months.</td></tr>
      {/each}
    </tbody>
  </table>
</Panel>

<style>
  .toolbar {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
  }
  .toolbar label {
    display: flex;
    gap: 5px;
    align-items: center;
  }
  .tags td {
    padding: 3px 8px;
  }
  .spark {
    display: block;
  }
  .cell {
    background: none;
    border: none;
    padding: 0;
    font-variant-numeric: tabular-nums;
  }
  .cell:hover {
    text-decoration: underline;
    color: var(--accent);
  }
  .change {
    color: var(--faint);
  }
  .good {
    color: var(--income);
  }
  .bad {
    color: var(--expense);
  }
  .untagged {
    font-style: italic;
    color: var(--warning);
  }
</style>
