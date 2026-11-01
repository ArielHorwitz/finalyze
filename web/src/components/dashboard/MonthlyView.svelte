<!--
  The last calendar months of the period side by side: a heat table of tags
  (and subtags) by month, with changes against the rolling average of the
  months before (without one-offs, unless included). How many months is an
  option (+ / -).
-->
<script lang="ts">
  import Panel from "./Panel.svelte";
  import Term from "../Term.svelte";
  import { withAlpha } from "../../lib/charts";
  import {
    baselineLabel,
    baselineTitle,
    colorsFor,
    flowSelection,
    formatChange,
    formatWhole,
    monthLabel,
    oneOffBasis,
    percentChange,
    tagLabel,
    tagSelection,
    type ViewProps,
  } from "../../lib/dashboard";
  import { bind, useKeys } from "../../lib/use-keys.svelte";
  import { keyContext, keyName } from "../../lib/state.svelte";

  let { data, settings, rollingName, update, ondrill }: ViewProps = $props();

  const kind = $derived(settings.kind);
  const periodMonths = $derived(data.breakdown.monthly.months);
  const count = $derived(settings.breakdownMonths ?? data.options.breakdown_months ?? 3);
  // The last months of the period, as many as chosen (or the period has)
  const months = $derived(periodMonths.slice(-count));
  const tagColors = $derived(colorsFor(data.trends.tag_order, data.colors));
  let expanded = $state(new Set<string | null>());

  interface Row {
    tag: string | null;
    subtag?: string | null;
    amounts: number[];
    /** The one-off part of each amount. */
    oneOffs: number[];
    total: number;
  }

  const table = $derived.by(() => {
    const byTag = new Map<string | null, { row: Row; subtags: Map<string | null, Row> }>();
    for (const item of data.breakdown.monthly[kind]) {
      const index = months.indexOf(item.month);
      if (index < 0) continue;
      let entry = byTag.get(item.tag);
      if (!entry) {
        entry = { row: { tag: item.tag, amounts: months.map(() => 0), oneOffs: months.map(() => 0), total: 0 }, subtags: new Map() };
        byTag.set(item.tag, entry);
      }
      const oneOff = item.kind === "one-off" ? item.amount : 0;
      entry.row.amounts[index] += item.amount;
      entry.row.oneOffs[index] += oneOff;
      entry.row.total += item.amount;
      let sub = entry.subtags.get(item.subtag);
      if (!sub) {
        sub = { tag: item.tag, subtag: item.subtag, amounts: months.map(() => 0), oneOffs: months.map(() => 0), total: 0 };
        entry.subtags.set(item.subtag, sub);
      }
      sub.amounts[index] += item.amount;
      sub.oneOffs[index] += oneOff;
      sub.total += item.amount;
    }
    return [...byTag.values()]
      .map((entry) => ({ ...entry.row, subtags: [...entry.subtags.values()].sort((first, second) => second.total - first.total) }))
      .sort((first, second) => second.total - first.total);
  });
  const totals = $derived(months.map((_, index) => table.reduce((sum, row) => sum + row.amounts[index], 0)));
  // What each month is compared with: the rolling mean of the months before,
  // and what is compared: the month as averages sum it (without one-offs,
  // unless included)
  const tagTrends = $derived(new Map(data.trends[kind].map((trend) => [trend.tag, trend])));
  const totalBaselines = $derived(data.cash_flow.baseline[rollingName]?.[kind] ?? []);
  const totalAveraged = $derived(data.cash_flow.averaged[kind]);
  const versus = $derived(baselineLabel(rollingName));

  /** A month's value of a series aligned with the period's months. */
  function valueOf(values: (number | null)[] | undefined, month: string): number | null {
    return values?.[data.period.months.indexOf(month)] ?? null;
  }
  const largest = $derived(Math.max(1, ...table.flatMap((row) => row.amounts)));

  function toggle(tag: string | null) {
    const next = new Set(expanded);
    if (next.has(tag)) next.delete(tag);
    else next.add(tag);
    expanded = next;
  }

  function drill(row: Row, month: string) {
    const label = row.subtag === undefined ? tagLabel(row.tag) : `${tagLabel(row.tag)} / ${row.subtag ?? ""}`;
    ondrill({
      title: `${label} · ${kind} · ${monthLabel(month)}`,
      selection: { ...flowSelection(kind), month, ...tagSelection(row.tag, row.subtag) },
    });
  }

  /** Steps from the months shown (fewer than chosen if the period is shorter). */
  function setCount(next: number) {
    update({ breakdownMonths: Math.max(1, Math.min(36, periodMonths.length, next)) });
  }

  useKeys("Monthly", () => [
    bind("monthly.more", () => setCount(months.length + 1)),
    bind("monthly.fewer", () => setCount(months.length - 1)),
  ]);
</script>

{#snippet cells(row: Row, color: string)}
  {@const trend = row.subtag === undefined ? tagTrends.get(row.tag) : undefined}
  {#each row.amounts as amount, index}
    {@const baseline = valueOf(trend?.baseline[rollingName], months[index])}
    {@const change = trend ? percentChange(valueOf(trend.amounts, months[index]), baseline) : null}
    <td class="amount cell" style:background={amount > 0 ? withAlpha(color, 0.08 + (amount / largest) * 0.5) : "transparent"}>
      {#if amount !== 0}
        {#if row.oneOffs[index]}<span class="one-off"><Term key="one_off" quiet context="{formatWhole(row.oneOffs[index])} of one-offs">◆</Term></span>{/if}
        <button
          onclick={() => drill(row, months[index])}
          title={baseline === null ? undefined : `${baselineTitle(rollingName, months[index])}: ${formatWhole(baseline)}`}
        >{formatWhole(amount)}</button>
      {:else}
        <span class="faint">·</span>
      {/if}
      <!-- Not on empty cells: nothing to compare -->
      {#if amount !== 0 && change !== null && Math.abs(change) >= 10}
        <span class="change" class:bad={(change > 0) === (kind === "expenses")}><Term key="monthly_arrows" quiet>{change > 0 ? "▲" : "▼"}</Term></span>
      {/if}
    </td>
  {/each}
  <td class="amount muted">{formatWhole(row.total / months.length)}</td>
{/snippet}

<div class="toolbar">
  <div class="segmented" role="group" aria-label="Incomes or expenses">
    <button class:on={kind === "expenses"} onclick={() => update({ kind: "expenses" })} title={keyName("dashboard.kind")}><Term key="expenses" context={keyContext("dashboard.kind")}>Expenses</Term></button>
    <button class:on={kind === "incomes"} onclick={() => update({ kind: "incomes" })} title={keyName("dashboard.kind")}><Term key="incomes" context={keyContext("dashboard.kind")}>Incomes</Term></button>
  </div>
  <span class="muted"><Term key="monthly_view_months">months</Term></span>
  <div class="stepper">
    <button onclick={() => setCount(months.length - 1)} disabled={months.length <= 1} title="-">−</button>
    <span title={count > months.length ? `Up to ${count}: the period has ${months.length}` : undefined}>{months.length}</span>
    <button onclick={() => setCount(months.length + 1)} disabled={months.length >= Math.min(36, periodMonths.length)} title="+">+</button>
  </div>
  <span class="faint"><Term key="monthly_arrows">▲▼ change of 10% or more</Term> <Term key="baseline">{versus}</Term> (the months before), <Term key="one_offs_in_averages">{oneOffBasis(settings.oneOffs)}</Term> (◆) · click a tag for its subtags</span>
</div>

<Panel title="{kind === 'expenses' ? 'Expenses' : 'Incomes'} by month" term="monthly_table" subtitle="the last {months.length} months of the period, one-offs included (◆)">
  <div class="wide">
    <table class="grid matrix">
      <thead>
        <tr>
          <th><Term key="tag">Tag</Term></th>
          {#each months as month}<th class="amount"><Term key="monthly_table" quiet>{monthLabel(month)}</Term></th>{/each}
          <th class="amount"><Term key="monthly_average">Avg of {months.length} shown</Term> <span class="basis">incl. one-offs</span></th>
        </tr>
      </thead>
      <tbody>
        {#each table as row (row.tag)}
          {@const color = tagColors.get(row.tag) ?? "#8f96a2"}
          <tr>
            <td>
              <button class="name" onclick={() => toggle(row.tag)} aria-expanded={expanded.has(row.tag)}>
                <span class="caret">{expanded.has(row.tag) ? "▾" : "▸"}</span>
                <span class="swatch" style:background={color}></span>
                <span class:untagged={row.tag === null}>{tagLabel(row.tag)}</span>
              </button>
            </td>
            {@render cells(row, color)}
          </tr>
          {#if expanded.has(row.tag)}
            {#each row.subtags as sub (sub.subtag)}
              <tr class="sub">
                <td class="muted subname">{sub.subtag || "–"}</td>
                {@render cells(sub, color)}
              </tr>
            {/each}
          {/if}
        {/each}
      </tbody>
      <tfoot>
        <tr>
          <td><Term key="monthly_total">Total</Term></td>
          {#each totals as value, index}
            {@const baseline = valueOf(totalBaselines, months[index])}
            {@const change = percentChange(valueOf(totalAveraged, months[index]), baseline)}
            {@const previous = index > 0 ? percentChange(value, totals[index - 1]) : null}
            <td
              class="amount"
              title="{baseline === null ? '' : `${baselineTitle(rollingName, months[index])}: ${formatWhole(baseline)}`}{previous === null ? '' : `\nvs month before: ${formatChange(previous).text}`}"
            >
              {formatWhole(value)}
              {#if change !== null}
                {@const shown = formatChange(change, { space: false })}
                <span class="total-change" class:bad={shown.direction !== 0 && (shown.direction > 0) === (kind === "expenses")}>{shown.text}</span>
              {/if}
            </td>
          {/each}
          <td class="amount" title="The average of the months shown, one-offs included">{formatWhole(totals.reduce((sum, value) => sum + value, 0) / Math.max(1, months.length))}</td>
        </tr>
      </tfoot>
    </table>
  </div>
</Panel>

<style>
  .toolbar {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
  }
  .basis {
    text-transform: none;
    font-weight: 400;
    color: var(--faint);
  }
  .stepper {
    display: flex;
    align-items: center;
    gap: 6px;
    font-variant-numeric: tabular-nums;
  }
  .stepper button {
    padding: 1px 8px;
  }
  .wide {
    overflow-x: auto;
  }
  .matrix td {
    padding: 4px 10px;
    white-space: nowrap;
  }
  .matrix .cell {
    position: relative;
    min-width: 92px;
  }
  .cell button {
    background: none;
    border: none;
    padding: 0;
    font-variant-numeric: tabular-nums;
  }
  .cell button:hover {
    text-decoration: underline;
  }
  .change {
    position: absolute;
    top: 2px;
    right: 2px;
    font-size: 8px;
    color: var(--income);
  }
  .change.bad {
    color: var(--expense);
  }
  .name {
    display: flex;
    align-items: center;
    gap: 6px;
    background: none;
    border: none;
    padding: 0;
  }
  .caret {
    width: 10px;
    color: var(--faint);
  }
  .subname {
    padding-left: 38px !important;
  }
  .untagged {
    font-style: italic;
    color: var(--warning);
  }
  tfoot td {
    font-weight: 600;
    border-top: 1px solid var(--border-strong);
  }
  .total-change {
    font-weight: 400;
    font-size: 11px;
    margin-left: 4px;
    color: var(--income);
  }
  .total-change.bad {
    color: var(--expense);
  }
  .one-off {
    color: var(--warning);
    font-size: 9px;
    margin-right: 3px;
  }
</style>
