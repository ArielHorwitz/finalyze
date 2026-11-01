<!--
  Incomes or expenses by tag and subtag: totals over the period, rolling means
  in its last month, or one month. A treemap and a sorted list, both clickable.
  Tags are incomes or expenses as in the cash flow, so they add up to it, and
  amounts are signed (a refunded tag can be negative).
-->
<script lang="ts">
  import Chart from "../Chart.svelte";
  import Panel from "./Panel.svelte";
  import Term from "../Term.svelte";
  import type { ChartClick } from "../../lib/charts";
  import { breakdownTotal, groupByTag, treemapOption, type BreakdownItem } from "../../lib/dashboard-charts";
  import {
    averagedSelection,
    colorsFor,
    flowSelection,
    formatWhole,
    monthLabel,
    periodLabel,
    rollingLabel,
    rollingNote,
    tagLabel,
    tagSelection,
    windowMonths,
    type BreakdownMode,
    type Kind,
    type ViewProps,
  } from "../../lib/dashboard";
  import type { DashboardSelection } from "../../lib/api";
  import { bind, useKeys } from "../../lib/use-keys.svelte";
  import { keyContext, keyName, titled } from "../../lib/state.svelte";

  let { data, settings, rollingName, update, ondrill }: ViewProps = $props();

  const mode = $derived(settings.breakdownMode);
  // Any month of the period, the last by default
  const monthlyMonths = $derived(data.breakdown.monthly.months);
  const month = $derived(
    settings.breakdownMonth && monthlyMonths.includes(settings.breakdownMonth) ? settings.breakdownMonth : (monthlyMonths.at(-1) ?? null),
  );
  let expanded = $state(new Set<string | null>());
  const kind = $derived(settings.kind);
  const tagColors = $derived(colorsFor(data.trends.tag_order, data.colors));
  const lastMonth = $derived(data.period.months.at(-1) ?? "");

  const items: BreakdownItem[] = $derived.by(() => {
    if (mode === "rolling") {
      return (data.breakdown.rolling[rollingName]?.[kind] ?? []).map((item) => ({ ...item, amount: item.rolling }));
    }
    if (mode === "month") return data.breakdown.monthly[kind].filter((item) => item.month === month);
    return data.breakdown.totals[kind];
  });
  // Tags with their negative subtags (e.g. a refund when netting by tag), so a
  // tag's amount is its rows' sum. Negative tags are listed last.
  const groups = $derived(groupByTag(items).filter((group) => group.amount !== 0));
  const total = $derived(breakdownTotal(items));
  const largest = $derived(Math.max(1, groups[0]?.amount ?? 1));
  const scope = $derived(
    mode === "total"
      ? periodLabel(data.period.months)
      : mode === "rolling"
        ? `${rollingLabel(rollingName)} through ${lastMonth ? `${monthLabel(lastMonth)} (${windowMonths(lastMonth, Number(rollingName))})` : ""}`
        : month
          ? monthLabel(month)
          : "",
  );

  /** The rows behind an item. Rolling means show the last month's rows. */
  function selectionOf(tag: string | null, subtag?: string | null): DashboardSelection {
    const base = { ...flowSelection(kind), ...tagSelection(tag, subtag) };
    if (mode === "total") return base;
    if (mode === "rolling") return { ...base, ...averagedSelection(kind, settings), month: lastMonth };
    return { ...base, month };
  }

  function drill(tag: string | null, subtag?: string | null) {
    const label = subtag === undefined ? tagLabel(tag) : `${tagLabel(tag)} / ${subtag ?? ""}`;
    const when = mode === "rolling" ? monthLabel(lastMonth) : scope;
    ondrill({
      title: `${label} · ${kind} · ${when}`,
      selection: selectionOf(tag, subtag),
      note: mode === "rolling" ? rollingNote(lastMonth) : undefined,
    });
  }

  function onTreemapClick(event: ChartClick) {
    const item = event.data as { tag: string | null; subtag?: string | null };
    drill(item.tag, item.subtag);
  }

  function toggle(tag: string | null) {
    const next = new Set(expanded);
    if (next.has(tag)) next.delete(tag);
    else next.add(tag);
    expanded = next;
  }

  function setKind(next: Kind) {
    update({ kind: next });
  }

  function setMode(next: BreakdownMode) {
    update({ breakdownMode: next });
  }

  function setMonth(next: string) {
    // The period's last month is the default
    update({ breakdownMonth: next === monthlyMonths.at(-1) ? null : next });
  }

  function stepMonth(step: number) {
    if (mode !== "month" || month === null) return;
    const index = Math.min(monthlyMonths.length - 1, Math.max(0, monthlyMonths.indexOf(month) + step));
    setMonth(monthlyMonths[index]);
  }

  const MODES: BreakdownMode[] = ["total", "rolling", "month"];
  useKeys("Breakdown", () => [
    bind("breakdown.mode", () => setMode(MODES[(MODES.indexOf(mode) + 1) % MODES.length])),
    bind("breakdown.previousMonth", () => stepMonth(-1)),
    bind("breakdown.nextMonth", () => stepMonth(1)),
  ]);
</script>

<div class="toolbar">
  <div class="segmented" role="group" aria-label="Incomes or expenses">
    <button class:on={kind === "expenses"} onclick={() => setKind("expenses")} title={keyName("dashboard.kind")}><Term key="expenses" context={keyContext("dashboard.kind")}>Expenses</Term></button>
    <button class:on={kind === "incomes"} onclick={() => setKind("incomes")} title={keyName("dashboard.kind")}><Term key="incomes" context={keyContext("dashboard.kind")}>Incomes</Term></button>
  </div>
  <div class="segmented" role="group" aria-label="Breakdown of">
    <button class:on={mode === "total"} onclick={() => setMode("total")} title={keyName("breakdown.mode")}><Term key="breakdown_total" context={keyContext("breakdown.mode")}>Total</Term></button>
    <button class:on={mode === "rolling"} onclick={() => setMode("rolling")} title={keyName("breakdown.mode")}><Term key="breakdown_rolling" context={keyContext("breakdown.mode")}>{rollingLabel(rollingName)}</Term></button>
    <button class:on={mode === "month"} onclick={() => setMode("month")} title={keyName("breakdown.mode")}><Term key="breakdown_month" context={keyContext("breakdown.mode")}>Month</Term></button>
  </div>
  {#if mode === "month" && month}
    <div class="range">
      <button class="arrow" onclick={() => stepMonth(-1)} disabled={month === monthlyMonths[0]} title={titled("Previous month", "breakdown.previousMonth")} aria-label="Previous month">‹</button>
      <select value={month} onchange={(event) => setMonth(event.currentTarget.value)} aria-label="Month">
        {#each monthlyMonths as option}<option value={option}>{monthLabel(option)}</option>{/each}
      </select>
      <button class="arrow" onclick={() => stepMonth(1)} disabled={month === monthlyMonths.at(-1)} title={titled("Next month", "breakdown.nextMonth")} aria-label="Next month">›</button>
    </div>
  {/if}
  <span class="muted">{scope} · <b>{formatWhole(total)}</b>{#if mode === "total" && data.period.months.length > 1}<span class="faint"> · <Term key="breakdown_total">{formatWhole(total / data.period.months.length)} a month</Term></span>{/if}</span>
</div>

<div class="split">
  <Panel title="By tag and subtag" term="breakdown_treemap" subtitle="area is the amount · click for the rows">
    {#if groups.some((group) => group.amount > 0)}
      <Chart label="Breakdown treemap" height="520px" option={treemapOption(items, tagColors, kind)} onclick={onTreemapClick} />
    {:else}
      <p class="empty">Nothing here.</p>
    {/if}
  </Panel>
  <Panel title="Sorted" term="breakdown_sorted" subtitle="click a tag for its subtags, an amount for its rows">
    <ol class="list">
      {#each groups as group (group.tag)}
        <li>
          <div class="item">
            <button class="name" onclick={() => toggle(group.tag)} aria-expanded={expanded.has(group.tag)}>
              <span class="caret">{expanded.has(group.tag) ? "▾" : "▸"}</span>
              <span class="swatch" style:background={tagColors.get(group.tag)}></span>
              <span class:untagged={group.tag === null}>{tagLabel(group.tag)}</span>
              {#if group.subtags.every((item) => item.kind === "one-off")}<span class="one-off"><Term key="one_off">one-off</Term></span>{/if}
            </button>
            <span class="bar"><span style:width="{(Math.max(0, group.amount) / largest) * 100}%" style:background={tagColors.get(group.tag)}></span></span>
            <button class="value" onclick={() => drill(group.tag)}>{formatWhole(group.amount)}</button>
            <span class="percent"><Term key="breakdown_share" quiet>{total ? ((group.amount / total) * 100).toFixed(1) : 0}%</Term></span>
          </div>
          {#if expanded.has(group.tag)}
            <ol class="subtags">
              {#each group.subtags as item (item.subtag)}
                <li class="item sub">
                  <span class="name muted">
                    {item.subtag || "–"}
                    {#if item.kind === "one-off" && !group.subtags.every((other) => other.kind === "one-off")}<span class="one-off"><Term key="one_off">one-off</Term></span>{/if}
                  </span>
                  <span class="bar"><span style:width="{(Math.max(0, item.amount) / largest) * 100}%" style:background={tagColors.get(group.tag)} style:opacity="0.55"></span></span>
                  <button class="value" onclick={() => drill(group.tag, item.subtag)}>{formatWhole(item.amount)}</button>
                  <span class="percent"><Term key="breakdown_share" quiet>{total ? ((item.amount / total) * 100).toFixed(1) : 0}%</Term></span>
                </li>
              {/each}
            </ol>
          {/if}
        </li>
      {/each}
    </ol>
  </Panel>
</div>

<style>
  .toolbar {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
  }
  .range {
    display: flex;
    align-items: center;
    gap: 4px;
  }
  .arrow {
    padding: 2px 8px;
  }
  .split {
    display: grid;
    grid-template-columns: minmax(0, 1.5fr) minmax(360px, 1fr);
    gap: 12px;
  }
  @media (max-width: 1000px) {
    .split {
      grid-template-columns: minmax(0, 1fr);
    }
  }
  .list,
  .subtags {
    list-style: none;
    margin: 0;
    padding: 0;
  }
  .list {
    max-height: 520px;
    overflow: auto;
  }
  .item {
    display: grid;
    grid-template-columns: minmax(120px, 1.2fr) 1fr 80px 52px;
    gap: 10px;
    align-items: center;
    padding: 3px 4px;
    border-radius: 4px;
  }
  .item:hover {
    background: var(--surface-2);
  }
  .sub .name {
    padding-left: 34px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .name {
    display: flex;
    align-items: center;
    gap: 6px;
    background: none;
    border: none;
    padding: 0;
    text-align: left;
    min-width: 0;
  }
  .caret {
    width: 10px;
    color: var(--faint);
  }
  .one-off {
    margin-left: 6px;
    font-size: 10px;
    color: var(--warning);
    text-transform: uppercase;
  }
  .untagged {
    font-style: italic;
    color: var(--warning);
  }
  .bar {
    height: 8px;
    border-radius: 4px;
    background: var(--surface-3);
    overflow: hidden;
  }
  .bar span {
    display: block;
    height: 100%;
    border-radius: 4px;
  }
  .value {
    background: none;
    border: none;
    padding: 0;
    text-align: right;
    font-variant-numeric: tabular-nums;
  }
  .value:hover {
    color: var(--accent);
    text-decoration: underline;
  }
  .percent {
    text-align: right;
    color: var(--faint);
    font-variant-numeric: tabular-nums;
    font-size: 12px;
  }
</style>
