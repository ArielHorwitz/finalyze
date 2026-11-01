<!--
  The overview: answers rather than small copies of the other views.
  Under the headline cards (are we OK, kept and set aside, runway):
  - what the numbers are of (period, accounts, groups, averages, balances' date);
  - the cash flow (incomes, expenses, net: one-offs hatched, far months
    breaking the axis) and what changed vs usual (movers, in ₪);
  - the net worth and available balance, and where the money went (ranked
    bars, one-offs hatched);
  - the one-offs' effect on the period, and checks (the available balance's
    bridge, and what in the data needs a look).
  Legends are buttons, so they can be used from the keyboard.
-->
<script lang="ts">
  import Chart from "../Chart.svelte";
  import Panel from "./Panel.svelte";
  import Bridge from "./Bridge.svelte";
  import Health from "./Health.svelte";
  import Term from "../Term.svelte";
  import type { ChartClick } from "../../lib/charts";
  import { COLORS } from "../../lib/charts";
  import { balanceColor, balanceLinesOption, cashFlowClick, cashFlowOption, FLOW_NAMES, groupByTag } from "../../lib/dashboard-charts";
  import {
    asOfLabel,
    baselineName,
    baselineTitle,
    cashFlowSelection,
    colorsFor,
    flowSelection,
    formatChange,
    formatRate,
    formatWhole,
    monthLabel,
    moverIsGood,
    movers,
    oneOffBasis,
    periodLabel,
    rollingLabel,
    shownGroups,
    tagLabel,
    tagSelection,
    type Mover,
    type ViewProps,
  } from "../../lib/dashboard";
  import { app, keyContext, keyName } from "../../lib/state.svelte";

  let { data, settings, rollingName, update, ondrill }: ViewProps = $props();

  const kind = $derived(settings.kind);
  const months = $derived(data.cash_flow.months);
  const month = $derived(data.headline.month);
  const monthName = $derived(month ? monthLabel(month) : "");
  const period = $derived(periodLabel(data.period.months));
  const basis = $derived(oneOffBasis(settings.oneOffs));
  const tagColors = $derived(colorsFor(data.trends.tag_order, data.colors));
  const totals = $derived(data.headline.totals);

  // What the numbers are of
  const accounts = $derived(settings.accounts.length ? settings.accounts.join(", ") : `all ${app.accounts.length} accounts`);
  const groups = $derived(shownGroups(settings, data.groups).join(", "));
  const average = $derived(rollingName === "1" ? "the previous month" : rollingLabel(rollingName));

  // Cash flow, with a legend of buttons
  const FLOW_LEGEND = [
    { name: FLOW_NAMES.incomes, term: "incomes", series: [FLOW_NAMES.incomes, "One-off incomes"], color: COLORS.income },
    { name: FLOW_NAMES.expenses, term: "expenses", series: [FLOW_NAMES.expenses, "One-off expenses"], color: COLORS.expense },
    { name: FLOW_NAMES.saved, term: "set_aside", series: [FLOW_NAMES.saved], color: COLORS.saved },
    { name: FLOW_NAMES.net, term: "kept", series: [FLOW_NAMES.net], color: COLORS.net },
  ];
  let hiddenFlows = $state<string[]>([]);
  const hiddenSeries = $derived(FLOW_LEGEND.filter((item) => hiddenFlows.includes(item.name)).flatMap((item) => item.series));

  function toggle(list: string[], name: string): string[] {
    return list.includes(name) ? list.filter((other) => other !== name) : [...list, name];
  }

  function onFlowClick(event: ChartClick) {
    const click = cashFlowClick(months, event.seriesName, event.dataIndex);
    ondrill({
      title: `${FLOW_NAMES[click.kind]}${click.part === "one-off" ? " · one-offs" : ""} · ${monthLabel(click.month)}`,
      selection: cashFlowSelection(click, settings),
    });
  }

  // What changed vs usual
  const changes = $derived(movers(data.trends, rollingName).slice(0, 7));
  const changeMax = $derived(Math.max(1, ...changes.map((mover) => Math.abs(mover.difference))));

  function drillMover(mover: Mover) {
    if (!month) return;
    ondrill({
      title: `${tagLabel(mover.tag)} · ${mover.direction} · ${monthName}`,
      selection: { flow: mover.direction, kinds: settings.oneOffs ? ["regular", "one-off"] : ["regular"], month, ...tagSelection(mover.tag) },
    });
  }

  // Balances, with a legend of buttons
  const balanceLines = $derived(data.balance.series.filter((series) => series.kind === "net_worth" || series.kind === "available"));
  let hiddenLines = $state<string[]>([]);

  // Where it went: the largest tags, then the rest as one
  const SHOWN_TAGS = 8;
  const ranked = $derived.by(() => {
    const tags = groupByTag(data.breakdown.totals[kind]).map((group) => ({
      tag: group.tag,
      amount: group.amount,
      oneOff: group.subtags.filter((item) => item.kind === "one-off").reduce((sum, item) => sum + item.amount, 0),
    }));
    const shown = tags.slice(0, SHOWN_TAGS);
    const rest = tags.slice(SHOWN_TAGS);
    const total = tags.reduce((sum, item) => sum + item.amount, 0);
    return {
      shown,
      rest: rest.length ? { count: rest.length, amount: rest.reduce((sum, item) => sum + item.amount, 0) } : null,
      total,
      largest: Math.max(1, ...shown.map((item) => item.amount), rest.reduce((sum, item) => sum + item.amount, 0)),
    };
  });

  function drillTag(tag: string | null) {
    ondrill({ title: `${tagLabel(tag)} · ${kind} · ${period}`, selection: { ...flowSelection(kind), ...tagSelection(tag) } });
  }

  // The one-offs' effect on the period
  const oneOffNet = $derived((totals?.one_off_incomes ?? 0) - (totals?.one_off_expenses ?? 0));
  const regularNet = $derived((totals?.net ?? 0) - oneOffNet);
  const regularIncomes = $derived((totals?.incomes ?? 0) - (totals?.one_off_incomes ?? 0));
  const largestOneOffs = $derived(
    (["expenses", "incomes"] as const)
      .flatMap((direction) => data.breakdown.totals[direction].filter((item) => item.kind === "one-off").map((item) => ({ ...item, direction })))
      .sort((first, second) => Math.abs(second.amount) - Math.abs(first.amount))
      .slice(0, 4),
  );

  // In short
  const last = $derived(data.headline.last);
  const runway = $derived(data.headline.runway[rollingName]);
  const expensesBaseline = $derived(data.cash_flow.baseline[rollingName]?.expenses.at(-1) ?? null);
  const expensesCompared = $derived(data.cash_flow.averaged.expenses.at(-1) ?? null);
</script>

<p class="scope">
  <span><Term key="period">{period}</Term> <span class="faint">· {data.period.months.length} mo</span></span>
  <span><span class="faint"><Term key="accounts_filter">accounts</Term></span> {accounts}</span>
  <span><span class="faint"><Term key="flows_of">flows of</Term></span> {groups}</span>
  <span><span class="faint"><Term key="baseline">compared with</Term></span> {average}, <Term key="one_offs_in_averages">{basis}</Term></span>
  <span><span class="faint"><Term key="as_of">balances</Term></span> {asOfLabel(data.balance.as_of)}</span>
  {#if settings.tags.length}<span class="warning"><Term key="only_these_tags">only {settings.tags.join(", ")}</Term></span>{/if}
</p>

{#if last && month}
  <p class="short">
    In {monthName},
    {#if (last.net ?? 0) >= 0}
      <span class="income">{formatWhole(last.net ?? 0, { signed: true })}</span> was <Term key="kept">kept</Term> ({formatRate(last.kept_rate)} of incomes)
    {:else}
      <span class="expense">{formatWhole(-(last.net ?? 0))}</span> more was spent than earned (<Term key="kept">kept</Term>)
    {/if}
    and {formatWhole(last.saved ?? 0)} <Term key="set_aside">set aside</Term>{#if data.health.saving_rows === 0}&nbsp;<span class="faint">(nothing in the period is tagged as saving)</span>{/if}.
    {#if expensesCompared !== null && expensesBaseline}
      {@const change = formatChange(((expensesCompared - expensesBaseline) / expensesBaseline) * 100)}
      Expenses{settings.oneOffs ? "" : " without one-offs"} were
      <span class:income={change.direction < 0} class:expense={change.direction > 0}>{change.direction === 0 ? "as usual" : `${change.text.replace(/[▲▼] /, "")} ${change.direction > 0 ? "above" : "below"}`}</span>
      the <Term key="baseline" context={month ? baselineTitle(rollingName, month) : ""}>{baselineName(rollingName)}</Term>.
    {/if}
    {#if runway?.months !== null && runway?.months !== undefined}
      The available balance covers <strong><Term key="runway">{runway.months.toFixed(1)} months</Term></strong> of typical expenses.
    {/if}
  </p>
{/if}

<div class="grid">
  <Panel title="Cash flow" term="cash_flow_chart" subtitle="by month · one-offs hatched" onopen={() => update({ view: "flow" })}>
    {#snippet actions()}
      <div class="legend" role="group" aria-label="Cash flow series">
        {#each FLOW_LEGEND as item}
          <button class="key" class:off={hiddenFlows.includes(item.name)} aria-pressed={!hiddenFlows.includes(item.name)} onclick={() => (hiddenFlows = toggle(hiddenFlows, item.name))}>
            <span class="swatch" style:background={item.color}></span><Term key={item.term}>{item.name}</Term>
          </button>
        {/each}
      </div>
    {/snippet}
    <Chart
      label="Cash flow"
      height="250px"
      option={cashFlowOption(data.cash_flow, rollingName, { compact: true, rolling: false, legend: false, hidden: hiddenSeries })}
      onclick={onFlowClick}
    />
  </Panel>

  <Panel title="What changed" term="what_changed" subtitle="{monthName} vs the {baselineName(rollingName)}, in ₪, {basis}" onopen={() => update({ view: "trends" })}>
    <ol class="changes">
      {#each changes as mover}
        {@const good = moverIsGood(mover)}
        <li>
          <button
            class="row"
            onclick={() => drillMover(mover)}
            title="{tagLabel(mover.tag)}, {mover.direction}: {formatWhole(mover.amount)} in {monthName}\n{month ? baselineTitle(rollingName, month) : ''}: {mover.baseline === null ? '–' : formatWhole(mover.baseline)}"
          >
            <span class="tag"><span class="swatch" style:background={tagColors.get(mover.tag)}></span>{tagLabel(mover.tag)}</span>
            <span class="direction faint"><Term key={mover.direction} quiet>{mover.direction === "expenses" ? "spent" : "earned"}</Term></span>
            <span class="diverging">
              <span class="half left">{#if mover.difference < 0}<span class:good-bar={good} class:bad-bar={!good} style:width="{(-mover.difference / changeMax) * 100}%"></span>{/if}</span>
              <span class="half">{#if mover.difference > 0}<span class:good-bar={good} class:bad-bar={!good} style:width="{(mover.difference / changeMax) * 100}%"></span>{/if}</span>
            </span>
            <span class="number" class:income={good} class:expense={!good}>{formatWhole(mover.difference, { signed: true })}</span>
            <span class="number faint">{#if mover.change === null}<Term key="new_tag">new</Term>{:else}{formatChange(mover.change, { space: false }).text}{/if}</span>
            <span class="number faint detail">{mover.baseline === null ? "–" : formatWhole(mover.baseline)} → {formatWhole(mover.amount)}</span>
          </button>
        </li>
      {:else}
        <li class="faint">Nothing changed from the {baselineName(rollingName)}.</li>
      {/each}
    </ol>
  </Panel>

  <Panel title="Net worth and available" term="balances_over_time" subtitle="at the end of each day, {asOfLabel(data.balance.as_of)}" onopen={() => update({ view: "balance" })}>
    {#snippet actions()}
      <div class="legend" role="group" aria-label="Balance lines">
        {#each balanceLines as line, index}
          <button class="key" class:off={hiddenLines.includes(line.label)} aria-pressed={!hiddenLines.includes(line.label)} onclick={() => (hiddenLines = toggle(hiddenLines, line.label))}>
            <span class="swatch" style:background={balanceColor(line, data.colors, index)}></span>{#if line.kind === "net_worth"}<Term key="net_worth">Net worth</Term>{:else}<Term key="available">Available</Term>{/if}
          </button>
        {/each}
      </div>
    {/snippet}
    <Chart label="Net worth and available balance" height="230px" option={balanceLinesOption(data.balance, data.colors, { hidden: hiddenLines })} />
  </Panel>

  <Panel title="Where it went" term="where_it_went" subtitle="{kind} by tag, {period}, one-offs hatched" onopen={() => update({ view: "breakdown" })}>
    {#snippet actions()}
      <div class="segmented" role="group" aria-label="Incomes or expenses">
        <button class:on={kind === "expenses"} onclick={() => update({ kind: "expenses" })} title={keyName("dashboard.kind")}><Term key="expenses" context={keyContext("dashboard.kind")}>Expenses</Term></button>
        <button class:on={kind === "incomes"} onclick={() => update({ kind: "incomes" })} title={keyName("dashboard.kind")}><Term key="incomes" context={keyContext("dashboard.kind")}>Incomes</Term></button>
      </div>
    {/snippet}
    <ol class="ranked">
      {#each ranked.shown as item}
        {@const color = tagColors.get(item.tag) ?? "#8f96a2"}
        {@const regular = Math.max(0, item.amount - Math.max(0, item.oneOff))}
        <li>
          <button class="row" onclick={() => drillTag(item.tag)} title="{tagLabel(item.tag)}: {formatWhole(item.amount)}{item.oneOff ? `, of which one-offs ${formatWhole(item.oneOff)}` : ''}">
            <span class="tag"><span class="swatch" style:background={color}></span>{tagLabel(item.tag)}</span>
            <span class="bar">
              <span style:width="{(regular / ranked.largest) * 100}%" style:background={color}></span>
              {#if item.oneOff > 0}<span class="one-off-bar" style:width="{(Math.min(item.oneOff, item.amount) / ranked.largest) * 100}%" style:background={color}></span>{/if}
            </span>
            <span class="number">{formatWhole(item.amount)}</span>
            <span class="number faint">{ranked.total ? `${formatWhole((item.amount / ranked.total) * 100)}%` : ""}</span>
          </button>
        </li>
      {/each}
      {#if ranked.rest}
        <li>
          <button class="row" onclick={() => update({ view: "breakdown" })} title="The other tags: open the breakdown">
            <span class="tag faint"><Term key="where_it_went">other ({ranked.rest.count} tags)</Term></span>
            <span class="bar"><span style:width="{(Math.max(0, ranked.rest.amount) / ranked.largest) * 100}%" style:background="#5f6672"></span></span>
            <span class="number">{formatWhole(ranked.rest.amount)}</span>
            <span class="number faint">{ranked.total ? `${formatWhole((ranked.rest.amount / ranked.total) * 100)}%` : ""}</span>
          </button>
        </li>
      {/if}
    </ol>
    <p class="faint total"><Term key="where_it_went">Total {formatWhole(ranked.total)} · {formatWhole(ranked.total / Math.max(1, data.period.months.length))}/mo with one-offs</Term></p>
  </Panel>

  <Panel title="The period" term="the_period" subtitle="{period}: what was kept and set aside, and what one-offs did" onopen={() => update({ view: "flow" })}>
    <table class="grid period">
      <tbody>
        <tr>
          <td><Term key="kept">Kept</Term> <span class="faint">(net)</span></td>
          <td class="amount" class:income={(totals?.net ?? 0) > 0} class:expense={(totals?.net ?? 0) < 0}>{formatWhole(totals?.net ?? 0, { signed: true })}</td>
          <td class="amount faint">{formatRate(totals?.kept_rate)} of incomes</td>
        </tr>
        <tr>
          <td><Term key="the_period">of which one-offs</Term></td>
          <td class="amount one-off">{formatWhole(oneOffNet, { signed: true })}</td>
          <td class="amount faint">{formatWhole(totals?.one_off_incomes ?? 0)} in · {formatWhole(totals?.one_off_expenses ?? 0)} out</td>
        </tr>
        <tr class="strong">
          <td><Term key="the_period">Kept without one-offs</Term></td>
          <td class="amount" class:income={regularNet > 0} class:expense={regularNet < 0}>{formatWhole(regularNet, { signed: true })}</td>
          <td class="amount faint">{formatRate(regularIncomes > 0 ? regularNet / regularIncomes : null)} of incomes</td>
        </tr>
        <tr>
          <td><Term key="set_aside">Set aside</Term></td>
          <td class="amount">{formatWhole(totals?.saved ?? 0)}</td>
          <td class="amount faint">{#if data.health.saving_rows === 0}<Term key="nothing_tagged_as_saving">nothing tagged as saving</Term>{:else}{formatRate(totals?.saved_rate)} of incomes{/if}</td>
        </tr>
      </tbody>
    </table>
    {#if largestOneOffs.length}
      <p class="faint caption"><Term key="the_period">Largest one-offs</Term></p>
      <ul class="one-offs">
        {#each largestOneOffs as item}
          <li>
            <button
              class="row"
              onclick={() => ondrill({ title: `${tagLabel(item.tag)} / ${item.subtag ?? "–"} · ${item.direction} · ${period}`, selection: { ...flowSelection(item.direction), ...tagSelection(item.tag, item.subtag) } })}
            >
              <span class="tag">◆ {tagLabel(item.tag)} / {item.subtag || "–"}</span>
              <span class="number" class:income={item.direction === "incomes"} class:expense={item.direction === "expenses"}>
                {formatWhole(item.direction === "expenses" ? -item.amount : item.amount, { signed: true })}
              </span>
            </button>
          </li>
        {/each}
      </ul>
    {/if}
  </Panel>

  <Panel title="Checks" term="checks" subtitle="does the balance add up, and does the data need a look" onopen={() => update({ view: "balance" })}>
    <Bridge bridge={data.bridge} {ondrill} compact />
    <Health health={data.health} months={data.period.months} {settings} {ondrill} onopen={() => update({ view: "balance" })} compact />
  </Panel>
</div>

<style>
  .scope {
    display: flex;
    flex-wrap: wrap;
    gap: 2px 16px;
    margin: 0;
    font-size: 12px;
    color: var(--text);
  }
  .short {
    margin: 0;
    font-size: 14px;
    color: var(--muted);
    line-height: 1.5;
  }
  .short span,
  .short strong {
    font-variant-numeric: tabular-nums;
  }
  .short strong {
    color: var(--text);
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 12px;
  }
  .legend {
    display: flex;
    gap: 2px;
  }
  .key {
    display: flex;
    align-items: center;
    gap: 5px;
    padding: 1px 6px;
    font-size: 12px;
    border: none;
    background: none;
    color: var(--muted);
    border-radius: 4px;
  }
  .key:hover,
  .key:focus-visible {
    background: var(--surface-3);
    color: var(--text);
  }
  .key.off {
    color: var(--faint);
    text-decoration: line-through;
  }
  .key.off .swatch {
    opacity: 0.3;
  }
  ol,
  ul {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
  }
  .row {
    display: grid;
    align-items: center;
    gap: 10px;
    width: 100%;
    padding: 2px 4px;
    border: none;
    background: none;
    font-size: 13px;
    line-height: 20px;
    text-align: left;
    border-radius: 4px;
  }
  .row:hover,
  .row:focus-visible {
    background: var(--surface-3);
  }
  .changes .row {
    grid-template-columns: minmax(0, 1fr) 42px minmax(60px, 1fr) 64px 42px 110px;
  }
  .ranked .row {
    grid-template-columns: minmax(0, 1fr) minmax(0, 1.6fr) 64px 36px;
  }
  .one-offs .row {
    grid-template-columns: minmax(0, 1fr) auto;
  }
  .tag {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .tag .swatch {
    margin-right: 6px;
  }
  .number {
    text-align: right;
    font-variant-numeric: tabular-nums;
  }
  .detail {
    font-size: 11px;
  }
  .direction {
    font-size: 11px;
  }
  .diverging {
    display: grid;
    grid-template-columns: 1fr 1fr;
    height: 7px;
  }
  .half {
    display: flex;
    background: var(--surface-3);
    overflow: hidden;
  }
  .half.left {
    justify-content: flex-end;
    border-right: 1px solid var(--faint);
  }
  .good-bar,
  .bad-bar {
    display: block;
    height: 100%;
    opacity: 0.8;
  }
  .good-bar {
    background: var(--income);
  }
  .bad-bar {
    background: var(--expense);
  }
  .bar {
    display: flex;
    height: 8px;
    border-radius: 3px;
    background: var(--surface-3);
    overflow: hidden;
  }
  .bar span {
    display: block;
    height: 100%;
    opacity: 0.85;
  }
  .bar .one-off-bar {
    opacity: 0.45;
    background-image: repeating-linear-gradient(135deg, transparent 0 2px, rgba(0, 0, 0, 0.5) 2px 4px) !important;
  }
  .total,
  .caption {
    margin: 6px 4px 0;
    font-size: 12px;
  }
  .period {
    width: 100%;
  }
  .period td {
    padding: 3px 8px;
  }
  .period tr.strong td {
    font-weight: 600;
    border-top: 1px solid var(--border-strong);
  }
  .one-off {
    color: var(--warning);
  }
  .warning {
    color: var(--warning);
  }
  @media (max-width: 1000px) {
    .grid {
      grid-template-columns: minmax(0, 1fr);
    }
  }
</style>
