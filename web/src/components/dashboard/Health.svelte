<!--
  Checks of the data behind the numbers: how recent each account and source
  is, whether anything in the period is tagged as saving, tags whose sign
  flipped in the period, transfers whose other side is missing, and months
  whose incomes are far from the usual. `compact` is one line of chips, only
  of what needs a look (with a link to the full checks).
-->
<script lang="ts">
  import type { Dashboard } from "../../lib/api";
  import Term from "../Term.svelte";
  import { averagedSelection, dateLabel, flowSelection, formatWhole, monthLabel, oneOffBasis, periodLabel, tagLabel, type DashboardState, type Drill } from "../../lib/dashboard";

  let {
    health,
    months,
    settings,
    ondrill,
    onopen,
    compact = false,
  }: {
    health: Dashboard["health"];
    months: string[];
    settings: DashboardState;
    ondrill: (drill: Drill) => void;
    onopen?: () => void;
    compact?: boolean;
  } = $props();

  // Usually active, but quiet for over a month: its raw files may be missing
  const stale = $derived(health.last_dates.filter((item) => item.behind));
  const period = $derived(periodLabel(months));
  const transfersOff = $derived(Math.round(health.transfer_residual) !== 0);

  function drillTransfers() {
    ondrill({ title: `Transfers of every group · ${period}`, selection: { line: "net_worth", kinds: ["transfer"] } });
  }

  function drillTag(tag: string) {
    ondrill({ title: `${tag} · ${period}`, selection: { ...flowSelection("net"), tag } });
  }

  /** As the check sums them: as averages do (without one-offs, unless included). */
  function drillIncomes(month: string) {
    ondrill({ title: `Incomes · ${monthLabel(month)}, ${oneOffBasis(settings.oneOffs)}`, selection: { ...averagedSelection("incomes", settings), month } });
  }

  const change = (incomes: number, usual: number) => `${incomes > usual ? "+" : "−"}${formatWhole((Math.abs(incomes - usual) / usual) * 100)}%`;
</script>

{#if compact}
  <div class="chips">
    {#if stale.length}
      <button class="chip warn" onclick={onopen}>
        <Term key="latest_rows" context={stale.map((item) => `${item.account_source}: last on ${dateLabel(item.last_date)}`).join("\n")}>{stale.length} source{stale.length > 1 ? "s" : ""} behind by over a month</Term>
      </button>
    {:else}
      <span class="chip ok"><Term key="latest_rows" context="Every account and source that usually has rows has some within a month of {health.latest ? dateLabel(health.latest) : 'the latest data'}">accounts up to date</Term></span>
    {/if}
    {#if health.saving_rows === 0}
      <button class="chip warn" onclick={onopen}><Term key="nothing_tagged_as_saving" context="None in {period}">nothing tagged as saving</Term></button>
    {/if}
    {#each health.sign_flips as flip}
      <button class="chip warn" onclick={() => drillTag(flip.tag)}>
        <Term key="sign_flip" context="{flip.tag} is {flip.direction} over all the data, but {formatWhole(flip.amount, { signed: true })} in {period}">{flip.tag} flipped sign</Term>
      </button>
    {/each}
    {#if transfersOff}
      <button class="chip warn" onclick={drillTransfers}>
        <Term key="transfer_residual" context="Transfers of every group in {period} add up to {formatWhole(health.transfer_residual, { signed: true })}, not 0">transfers off by {formatWhole(health.transfer_residual, { signed: true })}</Term>
      </button>
    {/if}
    {#each health.income_months as month}
      <button class="chip warn" onclick={() => drillIncomes(month.month)}>
        <Term key="income_outliers" context="Incomes of {monthLabel(month.month)}: {formatWhole(month.incomes)}, the usual month: {formatWhole(month.usual)}">incomes {change(month.incomes, month.usual)} in {monthLabel(month.month, { short: true })}</Term>
      </button>
    {/each}
  </div>
{:else}
  <div class="checks">
    <section>
      <h4><Term key="latest_rows">Latest rows</Term> <span class="faint">· the data runs to {health.latest ? dateLabel(health.latest) : "–"}</span></h4>
      <table class="grid">
        <tbody>
          {#each health.last_dates as item (item.account_source)}
            <tr
              class:stale={item.behind}
              title={item.behind
                ? "Usually has rows, but none for over a month: are its raw files ingested?"
                : `Rows in ${item.active_months} of the period's ${months.length} months`}
            >
              <td>{item.account_source}</td>
              <td class="amount">{dateLabel(item.last_date)}</td>
              <td class="amount faint">{item.days_behind ? `${item.days_behind} day${item.days_behind === 1 ? "" : "s"} before` : "latest"}</td>
              <td class="faint">{#if item.behind}<Term key="latest_rows">behind</Term>{:else if item.active_months * 2 < months.length}<Term key="latest_rows">quiet</Term>{/if}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </section>
    <section>
      <h4><Term key="data_checks">In {period}</Term></h4>
      <ul>
        <li class:warn={health.saving_rows === 0}>
          {#if health.saving_rows === 0}
            <Term key="nothing_tagged_as_saving">No rows tagged as saving</Term>: Set aside is 0 because nothing is tagged, not necessarily because nothing was saved.
          {:else}
            {health.saving_rows} <Term key="saving">rows tagged as saving</Term>, from the spending groups.
          {/if}
        </li>
        <li class:warn={transfersOff}>
          <Term key="transfer_residual">Transfers of every group</Term>:
          <button class="link" onclick={drillTransfers}>{health.transfer_rows} rows</button>
          {#if transfersOff}
            adding up to {formatWhole(health.transfer_residual, { signed: true })}, not 0: some have one side only (e.g. cash taken out with no wallet). Rows with no other side by nature, such as opening balances, are <Term key="reconciliation">reconciliations</Term>.
          {:else}
            adding up to 0: both sides are recorded.
          {/if}
        </li>
        <li class:warn={health.sign_flips.length > 0}>
          {#if health.sign_flips.length === 0}
            No tag <Term key="sign_flip">flipped sign</Term>: every tag is on its usual side.
          {:else}
            <Term key="sign_flip">Tags on the other side than over all the data</Term>:
            {#each health.sign_flips as flip, index}
              {index ? ", " : ""}<button class="link" onclick={() => drillTag(flip.tag)}>{tagLabel(flip.tag)}</button>
              <span class="faint">({flip.direction}, {formatWhole(flip.amount, { signed: true })} here)</span>
            {/each}
          {/if}
        </li>
        <li class:warn={health.income_months.length > 0}>
          {#if health.income_months.length === 0}
            Every month's incomes are <Term key="income_outliers">within 40% of the usual month</Term>.
          {:else}
            <Term key="income_outliers">Incomes ({oneOffBasis(settings.oneOffs)}) far from the usual month</Term> ({formatWhole(health.income_months[0].usual)}):
            {#each health.income_months as month, index}
              {index ? ", " : ""}<button class="link" onclick={() => drillIncomes(month.month)}>{monthLabel(month.month)}</button>
              <span class="faint">{formatWhole(month.incomes)} ({change(month.incomes, month.usual)})</span>
            {/each}
            <span class="faint">· e.g. a paycheck landing in the month before</span>
          {/if}
        </li>
      </ul>
    </section>
  </div>
{/if}

<style>
  .chips {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
  }
  .chip {
    font-size: 12px;
    padding: 1px 8px;
    border-radius: 10px;
    border: 1px solid var(--border);
    background: var(--surface-2);
    color: var(--muted);
  }
  .chip.warn {
    border-color: color-mix(in srgb, var(--warning) 45%, transparent);
    color: var(--warning);
  }
  .chip.ok {
    color: var(--income);
  }
  .checks {
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 2fr);
    gap: 18px;
  }
  h4 {
    margin: 0 0 4px;
    font-size: 12px;
    font-weight: 600;
    color: var(--muted);
  }
  table td {
    padding: 2px 8px;
  }
  tr.stale td {
    color: var(--warning);
  }
  ul {
    margin: 0;
    padding-left: 16px;
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  li.warn::marker {
    color: var(--warning);
  }
  li.warn {
    color: var(--text);
  }
  .link {
    background: none;
    border: none;
    padding: 0;
    font: inherit;
    color: var(--accent);
  }
  .link:hover,
  .link:focus-visible {
    text-decoration: underline;
  }
  @media (max-width: 1000px) {
    .checks {
      grid-template-columns: minmax(0, 1fr);
    }
  }
</style>
