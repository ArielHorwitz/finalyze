<!--
  Headline numbers, grouped by what they are of:
  - balances, as of the latest data: the available balance and the net worth
    (with their change over the period), and the runway (the available
    balance in months of typical expenses);
  - the period's last month: incomes, expenses (compared with the rolling
    average before it, and without one-offs unless included), what was kept
    (net) and what was set aside (saving tags), each with its share of income,
    in the month and the period;
  - the period: monthly averages, without one-offs unless included;
  - the top expenses of the month: the movers (the largest changes from the
    baseline, in ₪) or the largest.
  Clicking a number shows its rows. `compact` is a one-line strip of the main
  numbers, for views other than the overview.
-->
<script lang="ts">
  import Term from "../Term.svelte";
  import {
    asOfLabel,
    averagedSelection,
    baselineName,
    baselineTitle,
    colorsFor,
    dateLabel,
    flowSelection,
    formatChange,
    formatRate,
    formatWhole,
    monthLabel,
    movers,
    oneOffBasis,
    percentChange,
    periodLabel,
    rankingChoice,
    rollingLabel,
    rollingTitle,
    topRanking,
    tagLabel,
    tagSelection,
    type ViewProps,
  } from "../../lib/dashboard";
  import { keyName, titled } from "../../lib/state.svelte";

  let { data, settings, rollingName, ondrill, update, compact = false }: ViewProps & { compact?: boolean } = $props();

  const headline = $derived(data.headline);
  const month = $derived(headline.month);
  const monthName = $derived(month ? monthLabel(month) : "");
  const shortMonth = $derived(month ? monthLabel(month, { short: true }) : "");
  const previousName = $derived(headline.previous_month ? monthLabel(headline.previous_month, { short: true }) : "");
  const periodMonths = $derived(data.period.months.length);
  const lastIndex = $derived(data.cash_flow.months.length - 1);
  const baseline = $derived(data.cash_flow.baseline[rollingName]);
  const averaged = $derived(data.cash_flow.averaged);
  const basis = $derived(oneOffBasis(settings.oneOffs));
  const tagColors = $derived(colorsFor(data.trends.tag_order, data.colors));
  const asOf = $derived(asOfLabel(headline.as_of));
  const totals = $derived(new Map(data.balance.totals.map((total) => [total.key, total])));
  const since = $derived(data.period.months.length ? dateLabel(`${data.period.months[0]}-01`) : "the start");
  const runway = $derived(headline.runway[rollingName]);
  const noSavings = $derived(data.health.saving_rows === 0);
  const oneOffKey = $derived(keyName("dashboard.oneOffs"));
  const oneOffTitle = $derived(
    settings.oneOffs
      ? titled("One-offs: included in averages and comparisons", "dashboard.oneOffs")
      : `One-offs: left out of averages and comparisons${oneOffKey ? ` (${oneOffKey} includes them)` : ""}`,
  );

  type Flow = "net" | "incomes" | "expenses" | "saved";
  const titles: Record<Flow, string> = { net: "Kept (net)", incomes: "Incomes", expenses: "Expenses", saved: "Set aside" };

  function drillMonth(flow: Flow) {
    if (!month) return;
    ondrill({ title: `${titles[flow]} · ${monthName}`, selection: { ...flowSelection(flow), month } });
  }

  function drillAverage(flow: "incomes" | "expenses") {
    ondrill({
      title: `${titles[flow]} · ${periodLabel(data.period.months)}, ${basis}`,
      selection: averagedSelection(flow, settings),
    });
  }

  function drillTag(tag: string | null) {
    if (!month) return;
    ondrill({
      title: `${tagLabel(tag)} · expenses · ${monthName}`,
      selection: { ...flowSelection("expenses"), month, ...tagSelection(tag) },
    });
  }

  /** A mover's rows: as averages sum them, like its amount. */
  function drillMover(tag: string | null) {
    if (!month) return;
    ondrill({
      title: `${tagLabel(tag)} · expenses · ${monthName}, ${basis}`,
      selection: { ...averagedSelection("expenses", settings), month, ...tagSelection(tag) },
    });
  }

  /** "▲ 12%", against the baseline, with the direction's meaning (more expenses is bad). */
  function change(current: number | null | undefined, base: number | null | undefined, goodWhenUp: boolean) {
    const value = percentChange(current ?? null, base ?? null);
    if (value === null) return null;
    const { text, direction } = formatChange(value);
    return { text, good: direction === 0 ? null : direction > 0 === goodWhenUp };
  }

  /** The tooltip of a comparison: what is compared, the base, and the month before. */
  function versusTitle(flow: Flow, compared: number | null | undefined, base: number | null | undefined) {
    const lines = [];
    if (compared !== null && compared !== undefined) lines.push(`${monthName}, ${basis}: ${formatWhole(compared, { signed: flow === "net" })}`);
    if (base !== null && base !== undefined && month) lines.push(`${baselineTitle(rollingName, month)}, ${basis}: ${formatWhole(base, { signed: flow === "net" })}`);
    const previous = headline.previous?.[flow];
    if (previous !== null && previous !== undefined) lines.push(`${previousName}: ${formatWhole(previous, { signed: flow === "net" })}`);
    return lines.join("\n");
  }

  /** The month's flow and what is compared with its baseline (C4): without
   * its one-offs, unless they are included. */
  function compared(flow: "incomes" | "expenses") {
    const total = headline.last?.[flow] ?? 0;
    const oneOff = flow === "incomes" ? (headline.last?.one_off_incomes ?? 0) : (headline.last?.one_off_expenses ?? 0);
    const amount = averaged[flow][lastIndex] ?? null;
    return { total, oneOff, amount, delta: change(amount, baseline?.[flow][lastIndex], flow === "incomes") };
  }

  const topBy = $derived(topRanking(settings));
  const topMovers = $derived(movers(data.trends, rollingName, ["expenses"]).slice(0, 5));
  const moverMax = $derived(Math.max(1, ...topMovers.map((mover) => Math.abs(mover.difference))));
  const topMax = $derived(Math.max(1, ...headline.top_expenses.map((item) => item.amount)));
</script>

{#snippet delta(info: { text: string; good: boolean | null } | null)}
  {#if info}
    <span class="delta" class:good={info.good === true} class:bad={info.good === false}>{info.text}</span>
  {/if}
{/snippet}

{#snippet flowCard(flow: "incomes" | "expenses")}
  {@const shown = compared(flow)}
  <button class="card" onclick={() => drillMonth(flow)} title={versusTitle(flow, shown.amount, baseline?.[flow][lastIndex])}>
    <span class="label"><Term key={flow}>{titles[flow]}</Term></span>
    <span class="value">{formatWhole(shown.total)}</span>
    <span class="sub">
      {#if shown.oneOff && !settings.oneOffs}
        <span class="faint"><Term key="compared_amount" context={oneOffTitle}>{formatWhole(shown.amount ?? 0)} excl. one-offs</Term></span>
      {:else if shown.oneOff}
        <span class="one-off"><Term key="compared_amount" context={oneOffTitle}>incl. {formatWhole(shown.oneOff)} one-off</Term></span>
      {/if}
      {@render delta(shown.delta)}
    </span>
  </button>
{/snippet}

{#snippet rateLine(amount: number | null | undefined, rate: number | null | undefined, signed: boolean)}
  <span class="faint"><Term key="period_rates" context={periodLabel(data.period.months)}>{periodMonths} mo: {formatWhole(amount ?? 0, { signed })} · {formatRate(rate)}</Term></span>
{/snippet}

{#if compact}
  <div class="strip" aria-label="Headline numbers">
    <button onclick={() => update({ view: "balance" })} title="The spending groups' balance, {asOf}">
      <span class="faint"><Term key="available" context={asOf}>Available</Term></span> {formatWhole(headline.available ?? 0)}
      {#if runway?.months !== null && runway?.months !== undefined}<span class="faint">· <Term key="runway">{runway.months.toFixed(1)} mo runway</Term></span>{/if}
    </button>
    <button onclick={() => update({ view: "balance" })} title="Every group's balance, {asOf}">
      <span class="faint"><Term key="net_worth" context={asOf}>Net worth</Term></span> {formatWhole(headline.net_worth ?? 0)}
    </button>
    <span class="faint as-of"><Term key="as_of">{asOf}</Term></span>
    <span class="divider"></span>
    <span class="faint"><Term key="month_cards" context={month ? baselineTitle(rollingName, month) : ""}>{monthName}</Term></span>
    <button onclick={() => drillMonth("incomes")}><span class="faint"><Term key="incomes">Incomes</Term></span> {formatWhole(headline.last?.incomes ?? 0)}</button>
    <button onclick={() => drillMonth("expenses")}>
      <span class="faint"><Term key="expenses">Expenses</Term></span> {formatWhole(headline.last?.expenses ?? 0)}
      {@render delta(compared("expenses").delta)}
    </button>
    <button onclick={() => drillMonth("net")}>
      <span class="faint"><Term key="kept">Kept</Term></span>
      <span class:good={(headline.last?.net ?? 0) > 0} class:bad={(headline.last?.net ?? 0) < 0}>{formatWhole(headline.last?.net ?? 0, { signed: true })}</span>
      <span class="faint">({formatRate(headline.last?.kept_rate)})</span>
    </button>
    <button onclick={() => drillMonth("saved")}>
      <span class="faint"><Term key="set_aside">Set aside</Term></span> {formatWhole(headline.last?.saved ?? 0)} <span class="faint">({formatRate(headline.last?.saved_rate)})</span>
    </button>
  </div>
{:else}
  <div class="cards">
    <section class="group" style:--cards={3} aria-label="Balances">
      <h3><Term key="balances_table">Balances</Term> <span class="faint">· <Term key="as_of">{asOf}</Term></span></h3>
      <div class="row">
        <button class="card" onclick={() => update({ view: "balance" })} title="The spending groups' balance, {asOf}, and its change since {since} (open the balance view)">
          <span class="label"><Term key="available" context={asOf}>Available</Term></span>
          <span class="value">{formatWhole(headline.available ?? 0)}</span>
          <span class="sub">
            {@render signedChange(totals.get("available")?.change ?? null)}
            <span class="faint"><Term key="balance_change">since {since}</Term></span>
          </span>
        </button>
        <button
          class="card"
          onclick={() => update({ view: "flow" })}
          title="The available balance in months of typical expenses: {formatWhole(headline.available ?? 0)} ÷ {runway?.expenses === null || runway?.expenses === undefined ? '–' : formatWhole(runway.expenses)}, the {rollingLabel(rollingName)} of expenses through {shortMonth}, excl. one-offs (whatever O says). The available balance already has the cards' balances: they aren't taken again."
        >
          <span class="label"><Term key="runway" context={month ? rollingTitle(rollingName, month) : ""}>Runway</Term></span>
          <span class="value">{runway?.months === null || runway?.months === undefined ? "–" : runway.months.toFixed(1)} <small class="faint">mo</small></span>
          <span class="sub">
            <span class="faint">of {runway?.expenses === null || runway?.expenses === undefined ? "–" : formatWhole(runway.expenses)}/mo spent</span>
          </span>
        </button>
        <button class="card" onclick={() => update({ view: "balance" })} title="Every group's balance, {asOf}, and its change since {since}, with the one-offs in it (open the balance view)">
          <span class="label"><Term key="net_worth" context={asOf}>Net worth</Term></span>
          <span class="value">{formatWhole(headline.net_worth ?? 0)}</span>
          <span class="sub">
            {@render signedChange(totals.get("net_worth")?.change ?? null)}
            <span class="faint"><Term key="balance_change">since {since}</Term></span>
            {#if totals.get("net_worth")?.one_offs}
              <span class="one-off"><Term key="balance_change">incl. {formatWhole(totals.get("net_worth")!.one_offs, { signed: true })} one-offs</Term></span>
            {/if}
          </span>
        </button>
      </div>
    </section>

    <section class="group" style:--cards={4} aria-label={monthName}>
      <h3><Term key="month_cards">{monthName}</Term> <span class="faint">· <Term key="baseline" context={month ? baselineTitle(rollingName, month) : ""}>▲▼ vs {baselineName(rollingName)}</Term>, <Term key="one_offs_in_averages">{basis}</Term></span></h3>
      <div class="row">
        {@render flowCard("incomes")}
        {@render flowCard("expenses")}
        <button class="card" onclick={() => drillMonth("net")} title="Kept: incomes less expenses (the net), and its share of incomes. Over {periodLabel(data.period.months)}: {formatWhole(headline.totals?.net ?? 0, { signed: true })}, {formatRate(headline.totals?.kept_rate)} of incomes (one-offs included).{versusTitle('net', averaged.net[lastIndex], baseline?.net[lastIndex]) ? `\n${versusTitle('net', averaged.net[lastIndex], baseline?.net[lastIndex])}` : ''}">
          <span class="label"><Term key="kept">Kept</Term></span>
          <span class="value" class:income={(headline.last?.net ?? 0) > 0} class:expense={(headline.last?.net ?? 0) < 0}>
            {formatWhole(headline.last?.net ?? 0, { signed: true })}
          </span>
          <span class="sub">
            <span class="faint"><Term key="kept">{formatRate(headline.last?.kept_rate)} of incomes</Term></span>
            {@render rateLine(headline.totals?.net, headline.totals?.kept_rate, true)}
          </span>
        </button>
        <button
          class="card"
          onclick={() => drillMonth("saved")}
          title="Set aside: money moved out under saving tags, from the spending groups, and its share of incomes. Over {periodLabel(data.period.months)}: {formatWhole(headline.totals?.saved ?? 0)}, {formatRate(headline.totals?.saved_rate)} of incomes.{noSavings ? '\nNothing in the period is tagged as saving: tag the moves to savings with a saving tag to see them here.' : ''}"
        >
          <span class="label"><Term key="set_aside">Set aside</Term></span>
          <span class="value">{formatWhole(headline.last?.saved ?? 0)}</span>
          <span class="sub">
            <span class="faint"><Term key="set_aside">{formatRate(headline.last?.saved_rate)} of incomes</Term></span>
            {#if noSavings}
              <span class="warning"><Term key="nothing_tagged_as_saving">nothing tagged as saving in {periodMonths} mo</Term></span>
            {:else}
              {@render rateLine(headline.totals?.saved, headline.totals?.saved_rate, false)}
            {/if}
          </span>
        </button>
      </div>
    </section>

    <section class="group" style:--cards={1} aria-label="The period">
      <h3><Term key="period" context={periodLabel(data.period.months)}>Period</Term> <span class="faint">· {periodMonths} mo</span></h3>
      <div class="row">
        <button class="card" onclick={() => drillAverage("expenses")} title="Monthly averages over the {periodMonths} months, {basis} (O to {settings.oneOffs ? 'leave them out' : 'include them'}): net (kept), expenses and incomes">
          <span class="label"><Term key="avg_per_month" context={periodLabel(data.period.months)}>Avg/mo</Term></span>
          <span class="value" class:income={(headline.averages?.net ?? 0) > 0} class:expense={(headline.averages?.net ?? 0) < 0}>
            {formatWhole(headline.averages?.net ?? 0, { signed: true })} <small class="faint">net</small>
          </span>
          <span class="sub">
            <span class="faint">{formatWhole(headline.averages?.expenses ?? 0)} spent</span>
            <span class="faint">{formatWhole(headline.averages?.incomes ?? 0)} earned</span>
            <span class="faint"><Term key="one_offs_in_averages">{basis}</Term></span>
          </span>
        </button>
      </div>
    </section>

    <section class="group top" style:--cards={2.4} aria-label="Top expenses">
      <h3>
        <Term key="top_expenses">Top expenses</Term> · {shortMonth}
        <span class="segmented small" role="group" aria-label="Rank by">
          <button class:on={topBy === "movers"} onclick={() => update({ topBy: rankingChoice(settings, "movers") })}><Term key="movers" context="The largest changes from the {baselineName(rollingName)}, in ₪ ({basis})">movers</Term></button>
          <button class:on={topBy === "largest"} onclick={() => update({ topBy: rankingChoice(settings, "largest") })}><Term key="largest" context="The largest expenses of the month">largest</Term></button>
        </span>
      </h3>
      <div class="card">
        <ol>
          {#if topBy === "movers"}
            {#each topMovers as mover}
              <li>
                <button
                  class="link-row"
                  onclick={() => drillMover(mover.tag)}
                  title="{monthName}, {basis}: {formatWhole(mover.amount)}\n{month ? baselineTitle(rollingName, month) : ''}: {mover.baseline === null ? '–' : formatWhole(mover.baseline)}"
                >
                  <span class="tag"><span class="swatch" style:background={tagColors.get(mover.tag)}></span>{tagLabel(mover.tag)}</span>
                  <span class="diverging">
                    <span class="half left">{#if mover.difference < 0}<span class="good-bar" style:width="{(-mover.difference / moverMax) * 100}%"></span>{/if}</span>
                    <span class="half">{#if mover.difference > 0}<span class="bad-bar" style:width="{(mover.difference / moverMax) * 100}%"></span>{/if}</span>
                  </span>
                  <span class="amount" class:bad={mover.difference > 0} class:good={mover.difference < 0}>{formatWhole(mover.difference, { signed: true })}</span>
                  <span class="trend">{#if mover.change === null}<Term key="new_tag">new</Term>{:else}{formatChange(mover.change, { space: false }).text}{/if}</span>
                </button>
              </li>
            {:else}
              <li class="faint">No changes from the {baselineName(rollingName)}</li>
            {/each}
          {:else}
            {#each headline.top_expenses as item}
              {@const base = item.baseline[rollingName] ?? null}
              {@const value = percentChange(item.averaged, base)}
              <li>
                <button
                  class="link-row"
                  onclick={() => drillTag(item.tag)}
                  title="{month ? baselineTitle(rollingName, month) : ''}: {base === null ? '–' : formatWhole(base)}{item.previous === null ? '' : `\n${previousName}: ${formatWhole(item.previous)}`}{item.one_off ? `\nOne-off: ${formatWhole(item.one_off)}` : ''}"
                >
                  <span class="tag"><span class="swatch" style:background={tagColors.get(item.tag)}></span>{tagLabel(item.tag)}</span>
                  <span class="bar">
                    <span style:width="{((item.amount - item.one_off) / topMax) * 100}%" style:background={tagColors.get(item.tag)}></span>
                    {#if item.one_off}<span class="one-off-bar" style:width="{(item.one_off / topMax) * 100}%" style:background={tagColors.get(item.tag)}></span>{/if}
                  </span>
                  <span class="amount">{formatWhole(item.amount)}</span>
                  <span class="trend" class:bad={value !== null && value > 5} class:good={value !== null && value < -5}>
                    {#if item.averaged === null && item.one_off}<span class="one-off"><Term key="one_off">one-off</Term></span>{:else if value !== null}{formatChange(value, { space: false }).text}{/if}
                  </span>
                </button>
              </li>
            {/each}
          {/if}
        </ol>
      </div>
    </section>
  </div>
{/if}

{#snippet signedChange(value: number | null)}
  {#if value !== null}
    <span class="delta" class:good={value > 0} class:bad={value < 0}>{formatWhole(value, { signed: true })}</span>
  {/if}
{/snippet}

<style>
  .cards {
    display: flex;
    flex-wrap: wrap;
    gap: 8px 14px;
    flex-shrink: 0;
  }
  /* Groups share a row in proportion to their cards, and wrap when their
     cards would get narrower than they can be */
  .group {
    display: flex;
    flex-direction: column;
    gap: 4px;
    flex: var(--cards) 1 0;
    min-width: calc(var(--cards) * 124px);
  }
  .group h3 {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 0 2px;
    font-size: 11px;
    font-weight: 600;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    min-height: 18px;
  }
  .group h3 .faint {
    text-transform: none;
    letter-spacing: 0;
    font-weight: 400;
  }
  .row {
    display: grid;
    grid-auto-flow: column;
    grid-auto-columns: minmax(0, 1fr);
    gap: 8px;
    flex: 1;
  }
  .card {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 1px;
    padding: 8px 12px;
    min-width: 0;
    height: 100%;
    text-align: left;
    background: linear-gradient(180deg, var(--surface-2), var(--surface));
    border: 1px solid var(--border);
    border-radius: 8px;
  }
  button.card:hover {
    border-color: var(--border-strong);
    background: var(--surface-2);
  }
  .label {
    color: var(--muted);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 100%;
  }
  .value {
    font-size: 22px;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
    letter-spacing: -0.01em;
  }
  .value small {
    font-size: 12px;
    font-weight: 400;
  }
  .sub {
    display: flex;
    flex-wrap: wrap;
    column-gap: 6px;
    font-size: 12px;
    max-width: 100%;
  }
  .delta {
    color: var(--muted);
  }
  .good {
    color: var(--income);
  }
  .bad {
    color: var(--expense);
  }
  .warning {
    color: var(--warning);
  }
  .top ol {
    list-style: none;
    margin: 0;
    padding: 0;
    width: 100%;
    display: flex;
    flex-direction: column;
  }
  .link-row {
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 1.2fr) auto 42px;
    align-items: center;
    gap: 8px;
    width: 100%;
    padding: 0 4px;
    border: none;
    background: none;
    font-size: 12px;
    line-height: 17px;
    text-align: left;
  }
  .link-row:hover,
  .link-row:focus-visible {
    background: var(--surface-3);
  }
  .tag {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .tag .swatch {
    margin-right: 6px;
  }
  .bar {
    display: flex;
    height: 6px;
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
    opacity: 0.4;
    background-image: repeating-linear-gradient(135deg, transparent 0 2px, rgba(0, 0, 0, 0.45) 2px 4px) !important;
  }
  .diverging {
    display: grid;
    grid-template-columns: 1fr 1fr;
    height: 6px;
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
  .one-off {
    color: var(--warning);
    font-size: 11px;
  }
  .amount {
    font-variant-numeric: tabular-nums;
    text-align: right;
  }
  .trend {
    color: var(--faint);
    text-align: right;
    font-variant-numeric: tabular-nums;
  }
  .segmented.small button {
    font-size: 10px;
    padding: 0 6px;
    text-transform: none;
    letter-spacing: 0;
  }
  .strip {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 4px 14px;
    padding: 5px 12px;
    font-size: 13px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    font-variant-numeric: tabular-nums;
    flex-shrink: 0;
  }
  .strip button {
    background: none;
    border: none;
    padding: 0;
    font: inherit;
    color: var(--text);
  }
  .strip button:hover,
  .strip button:focus-visible {
    text-decoration: underline;
  }
  .strip .as-of {
    font-size: 12px;
  }
  .divider {
    width: 1px;
    align-self: stretch;
    background: var(--border-strong);
  }
</style>
