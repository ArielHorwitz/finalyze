<!--
  Monthly cash flow: incomes, expenses (one-offs hatched), money set aside and
  net, with rolling means. Months far beyond the others (e.g. of a large
  one-off) break the axis. Then a table by month: what was kept (net) and set
  aside, with their shares of incomes, changes against the rolling average
  before each month, and the period's total with and without one-offs.
-->
<script lang="ts">
  import Chart from "../Chart.svelte";
  import Panel from "./Panel.svelte";
  import Term from "../Term.svelte";
  import type { ChartClick } from "../../lib/charts";
  import { cashFlowClick, cashFlowOption, flowBreaks, FLOW_NAMES } from "../../lib/dashboard-charts";
  import {
    baselineLabel,
    baselineTitle,
    cashFlowSelection,
    flowSelection,
    formatChange,
    formatRate,
    formatWhole,
    monthLabel,
    oneOffBasis,
    percentChange,
    periodLabel,
    rollingLabel,
    rollingNote,
    rollingTitle,
    type ViewProps,
  } from "../../lib/dashboard";

  let { data, settings, rollingName, update, ondrill }: ViewProps = $props();

  const flow = $derived(data.cash_flow);
  const totals = $derived(data.headline.totals);
  const baseline = $derived(flow.baseline[rollingName]);
  const rows = $derived(
    flow.months
      .map((month, index) => {
        // As the averages sum them (without one-offs, unless included)
        const expenses = flow.averaged.expenses[index];
        return {
          month,
          index,
          incomes: flow.incomes[index],
          expenses: flow.expenses[index],
          oneOffExpenses: flow.one_offs.expenses[index],
          oneOffIncomes: flow.one_offs.incomes[index],
          saved: flow.saved[index],
          savedRate: flow.saved_rate[index],
          keptRate: flow.kept_rate[index],
          compared: expenses,
          net: flow.net[index],
          rolling: flow.rolling[rollingName]?.net[index] ?? null,
          baseline: baseline?.expenses[index] ?? null,
          expensesChange: percentChange(expenses, baseline?.expenses[index] ?? null),
          previousChange: index > 0 ? percentChange(flow.expenses[index], flow.expenses[index - 1]) : null,
        };
      })
      .reverse(),
  );

  type Flow = "net" | "incomes" | "expenses" | "saved";

  function drill(kind: Flow, month: string, fromRolling = false) {
    ondrill({
      title: `${FLOW_NAMES[kind]} · ${monthLabel(month)}`,
      selection: { ...flowSelection(kind), month },
      note: fromRolling ? rollingNote(month) : undefined,
    });
  }

  function onClick(event: ChartClick) {
    const click = cashFlowClick(flow.months, event.seriesName, event.dataIndex);
    const part = click.part === "one-off" ? " · one-offs" : click.part === "regular" && flow.one_offs[click.kind as "incomes"]?.[event.dataIndex] ? " · without one-offs" : "";
    ondrill({
      title: `${FLOW_NAMES[click.kind]}${part} · ${monthLabel(click.month)}`,
      selection: cashFlowSelection(click, settings),
      note: click.rolling ? rollingNote(click.month) : undefined,
    });
  }

  const percent = (rate: number | null) => (rate === null ? "" : formatRate(rate));
  const arrow = (change: number) => formatChange(change).text;
  const basis = $derived(oneOffBasis(settings.oneOffs));
  // The period's one-offs, and its net without them (F2)
  const oneOffNet = $derived((totals?.one_off_incomes ?? 0) - (totals?.one_off_expenses ?? 0));
  const regularNet = $derived((totals?.net ?? 0) - oneOffNet);
  const regularIncomes = $derived((totals?.incomes ?? 0) - (totals?.one_off_incomes ?? 0));
  const breaks = $derived(flowBreaks(flow).length > 0);

  function changeTitle(row: (typeof rows)[number]): string {
    const lines = [];
    if (row.baseline !== null) {
      lines.push(`${baselineTitle(rollingName, row.month)}: ${formatWhole(row.baseline)}`);
      if (row.oneOffExpenses && !settings.oneOffs) lines.push(`Compared without the one-offs: ${formatWhole(flow.averaged.expenses[row.index] ?? 0)}`);
    }
    if (row.previousChange !== null) lines.push(`vs month before: ${arrow(row.previousChange)}`);
    return lines.join("\n");
  }
</script>

<Panel title="Cash flow" term="cash_flow_chart" subtitle="incomes up, expenses and money set aside down (one-offs hatched), net line · click a bar for its rows">
  {#snippet actions()}
    {#if breaks}<span class="faint"><Term key="axis_break">the axis breaks (≈)</Term></span>{/if}
    {#if rollingName !== "1"}
      <label class="toggle">
        <input type="checkbox" checked={settings.flowRolling} onchange={(event) => update({ flowRolling: event.currentTarget.checked })} />
        <Term key="rolling_average" context="The average of the last months, up to each month, {basis}">{rollingLabel(rollingName)}</Term>
      </label>
    {/if}
  {/snippet}
  <Chart label="Cash flow by month" height="400px" option={cashFlowOption(flow, rollingName, { rolling: settings.flowRolling })} onclick={onClick} />
</Panel>

<Panel title="By month" term="by_month" subtitle="latest first · click a number for its rows · hover a change for the month before">
  {#if oneOffNet}
    <p class="summary">
      Over {periodLabel(flow.months)}, the net was
      <strong class:income={(totals?.net ?? 0) > 0} class:expense={(totals?.net ?? 0) < 0}>{formatWhole(totals?.net ?? 0, { signed: true })}</strong>, <Term key="cash_flow_total">of which one-offs</Term>
      <strong class="one-off-text">{formatWhole(oneOffNet, { signed: true })}</strong>: without them it was
      <strong class:income={regularNet > 0} class:expense={regularNet < 0}>{formatWhole(regularNet, { signed: true })}</strong>
      ({formatRate(regularIncomes > 0 ? regularNet / regularIncomes : null)} of incomes without one-offs, against {formatRate(totals?.kept_rate)}).
    </p>
  {/if}
  <table class="grid months">
    <thead>
      <tr>
        <th><Term key="by_month">Month</Term></th>
        <th class="amount"><Term key="incomes">Incomes</Term></th>
        <th class="amount"><Term key="expenses">Expenses</Term></th>
        <th class="amount"><Term key="baseline" context="Expenses against the average of the months before, both {basis}">{baselineLabel(rollingName)}</Term> <span class="basis">{basis}</span></th>
        <th class="amount"><Term key="kept">Net (kept)</Term></th>
        <th class="amount"><Term key="kept" context="Net / incomes">Kept %</Term></th>
        <th class="amount"><Term key="set_aside">Set aside</Term></th>
        <th class="amount"><Term key="set_aside" context="Set aside / incomes">Set aside %</Term></th>
        <th class="amount"><Term key="rolling_average" context="The average net of the last months, up to and including each month, {basis}">Net, {rollingLabel(rollingName)} through</Term></th>
      </tr>
    </thead>
    <tbody>
      {#each rows as row (row.month)}
        <tr>
          <td>{monthLabel(row.month)}</td>
          <td class="amount">
            {#if row.oneOffIncomes}<span class="one-off" title="{formatWhole(row.oneOffIncomes)} of one-offs">◆</span>{/if}
            <button class="cell income" onclick={() => drill("incomes", row.month)}>{formatWhole(row.incomes)}</button>
          </td>
          <td class="amount">
            {#if row.oneOffExpenses}<span class="one-off" title="{formatWhole(row.oneOffExpenses)} of one-offs">◆</span>{/if}
            <button class="cell expense" onclick={() => drill("expenses", row.month)}>{formatWhole(row.expenses)}</button>
          </td>
          <td class="amount change" class:bad={(row.expensesChange ?? 0) > 5} class:good={(row.expensesChange ?? 0) < -5} title={changeTitle(row)}>
            {#if row.oneOffExpenses && !settings.oneOffs && row.compared !== null}<span class="compared">{formatWhole(row.compared)} excl. ·</span>{/if}
            {row.expensesChange === null ? "" : arrow(row.expensesChange)}
          </td>
          <td class="amount">
            <button class="cell" class:income={row.net > 0} class:expense={row.net < 0} onclick={() => drill("net", row.month)}>
              {formatWhole(row.net, { signed: true })}
            </button>
          </td>
          <td class="amount muted">{percent(row.keptRate)}</td>
          <td class="amount">
            <button class="cell saved" class:zero={!row.saved} onclick={() => drill("saved", row.month)}>{formatWhole(row.saved)}</button>
          </td>
          <td class="amount muted">{percent(row.savedRate)}</td>
          <td class="amount muted" title={rollingTitle(rollingName, row.month)}>{row.rolling === null ? "" : formatWhole(row.rolling, { signed: true })}</td>
        </tr>
      {/each}
    </tbody>
    <tfoot>
      <tr>
        <td><Term key="cash_flow_total">Total · {flow.months.length} months</Term></td>
        <td class="amount income">{formatWhole(totals?.incomes ?? 0)}</td>
        <td class="amount expense">{formatWhole(totals?.expenses ?? 0)}</td>
        <td></td>
        <td class="amount">{formatWhole(totals?.net ?? 0, { signed: true })}</td>
        <td class="amount muted">{percent(totals?.kept_rate ?? null)}</td>
        <td class="amount saved">{formatWhole(totals?.saved ?? 0)}</td>
        <td class="amount muted">{percent(totals?.saved_rate ?? null)}</td>
        <td></td>
      </tr>
      {#if oneOffNet || totals?.one_off_incomes || totals?.one_off_expenses}
        <tr class="sub-total">
          <td><Term key="cash_flow_total">of which one-offs</Term></td>
          <td class="amount one-off-text">{totals?.one_off_incomes ? formatWhole(totals.one_off_incomes) : ""}</td>
          <td class="amount one-off-text">{totals?.one_off_expenses ? formatWhole(totals.one_off_expenses) : ""}</td>
          <td></td>
          <td class="amount one-off-text">{formatWhole(oneOffNet, { signed: true })}</td>
          <td colspan="4"></td>
        </tr>
        <tr class="sub-total">
          <td><Term key="cash_flow_total">without one-offs</Term></td>
          <td class="amount">{formatWhole(regularIncomes)}</td>
          <td class="amount">{formatWhole((totals?.expenses ?? 0) - (totals?.one_off_expenses ?? 0))}</td>
          <td></td>
          <td class="amount" class:income={regularNet > 0} class:expense={regularNet < 0}>{formatWhole(regularNet, { signed: true })}</td>
          <td class="amount muted">{percent(regularIncomes > 0 ? regularNet / regularIncomes : null)}</td>
          <td colspan="3"></td>
        </tr>
      {/if}
    </tfoot>
  </table>
  <p class="faint legend">
    <Term key="one_off">◆</Term> the month has one-offs: they are in the sums, and {settings.oneOffs ? "included in averages and comparisons" : "left out of averages and comparisons"}.
  </p>
</Panel>

<style>
  .toggle {
    display: flex;
    gap: 5px;
    align-items: center;
    color: var(--muted);
  }
  .months td {
    padding: 4px 8px;
  }
  tfoot td {
    font-weight: 600;
    border-top: 1px solid var(--border-strong);
    padding: 6px 8px;
  }
  .cell {
    background: none;
    border: none;
    padding: 0 4px;
    font-variant-numeric: tabular-nums;
  }
  .cell:hover {
    text-decoration: underline;
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
  .saved {
    color: #b49bf0;
  }
  .one-off {
    color: var(--warning);
    font-size: 9px;
    margin-right: 2px;
  }
  .legend {
    margin: 6px 0 0;
    font-size: 12px;
  }
  .summary {
    margin: 0 0 4px;
    color: var(--muted);
  }
  .summary strong {
    font-variant-numeric: tabular-nums;
  }
  .one-off-text {
    color: var(--warning);
  }
  tfoot tr.sub-total td {
    font-weight: 400;
    color: var(--muted);
    border-top: none;
    padding: 2px 8px;
  }
  .compared {
    color: var(--faint);
    font-size: 11px;
    margin-right: 3px;
  }
  .basis {
    text-transform: none;
    font-weight: 400;
    color: var(--faint);
  }
  .cell.zero {
    color: var(--faint);
  }
</style>
