<!--
  Balances over time, at the end of each day: the net worth (every group), the
  available balance (the spending groups), each group's (stacked, so they add
  up to the net worth, or as lines), and every account and source. Then the
  balances as of the latest data (or the period's end, if earlier), the
  bridge of how the available balance and the net worth changed, and checks
  of the data.
-->
<script lang="ts">
  import Chart from "../Chart.svelte";
  import Panel from "./Panel.svelte";
  import Term from "../Term.svelte";
  import Bridge from "./Bridge.svelte";
  import Health from "./Health.svelte";
  import type { ChartClick } from "../../lib/charts";
  import { balanceColor, balanceOption, groupColors } from "../../lib/dashboard-charts";
  import { asOfLabel, dateLabel, formatWhole, periodLabel, type ViewProps } from "../../lib/dashboard";
  import type { DashboardSelection } from "../../lib/api";
  import { formatAmount } from "../../lib/money";

  let { data, settings, update, ondrill }: ViewProps = $props();

  const balance = $derived(data.balance);
  const ownGroupColors = $derived(groupColors(data.groups));
  const colorOfSeries = (key: string) => {
    const index = balance.series.findIndex((series) => series.key === key);
    const series = balance.series[index];
    return series ? balanceColor(series, data.colors, Math.max(0, index), ownGroupColors) : "#8f96a2";
  };
  const roles = $derived(new Map(data.groups.map((group) => [group.name, group.role])));
  const asOf = $derived(asOfLabel(balance.as_of));
  // The balances' days: from the period's start to the latest data (or the period's end)
  const days = $derived(balance.as_of ? `${data.period.start ? dateLabel(`${data.period.start}-01`) : "the start"} – ${dateLabel(balance.as_of)}` : periodLabel(data.period.months));

  /** The rows of a balance line (all groups' rows, filtered like the line). */
  function lineSelection(key: string): Partial<DashboardSelection> | null {
    const series = balance.series.find((item) => item.key === key);
    if (!series) return null;
    if (series.kind === "net_worth") return { all_groups: true };
    if (series.kind === "available") return { all_groups: true, group_role: "spending" };
    if (series.kind === "group") return { all_groups: true, group: series.group };
    return { all_groups: true, account_source: series.label };
  }

  function onBalanceClick(event: ChartClick) {
    const series = balance.series.find((item) => item.label === event.seriesName);
    const [date] = event.value as [string, number, number | null];
    const selection = series ? lineSelection(series.key) : null;
    if (!series || !selection) return;
    const day = new Date(date).toISOString().slice(0, 10);
    ondrill({ title: `${series.label} · ${dateLabel(day)}`, selection: { ...selection, date: day } });
  }

  function drillLine(key: string, label: string) {
    const selection = lineSelection(key);
    if (!selection) return;
    ondrill({ title: `${label} · ${days}`, selection });
  }
</script>

{#snippet changeCell(change: number | null)}
  <td class="amount" class:income={(change ?? 0) > 0} class:expense={(change ?? 0) < 0}>
    {change === null ? "" : formatWhole(change, { signed: true })}
  </td>
{/snippet}

<Panel title="Balances over time" term="balances_over_time" subtitle="at the end of each day, {days} · drag the slider or scroll to zoom · click a point for its day's rows">
  {#snippet actions()}
    <div class="segmented" role="group" aria-label="Groups">
      <button class:on={settings.balanceGroups === "stacked"} onclick={() => update({ balanceGroups: "stacked" })} ><Term key="groups_shown">groups stacked</Term></button>
      <button class:on={settings.balanceGroups === "lines"} onclick={() => update({ balanceGroups: "lines" })}><Term key="groups_shown">as lines</Term></button>
      <button class:on={settings.balanceGroups === "hidden"} onclick={() => update({ balanceGroups: "hidden" })}><Term key="groups_shown">hidden</Term></button>
    </div>
    <label class="toggle">
      <input type="checkbox" checked={settings.balanceAccounts} onchange={(event) => update({ balanceAccounts: event.currentTarget.checked })} /> <Term key="groups_shown">accounts and sources</Term>
    </label>
  {/snippet}
  <Chart
    label="Balances over time"
    height="420px"
    option={balanceOption(balance, data.colors, { accounts: settings.balanceAccounts, groups: settings.balanceGroups, ownGroupColors })}
    onclick={onBalanceClick}
  />
</Panel>

<div class="row">
  <Panel title="Balances" term="balances_table" termContext={asOf} subtitle="{asOf} · change since {data.period.start ? dateLabel(`${data.period.start}-01`) : 'the start'} · click for the rows">
    <table class="grid balances">
      <thead>
        <tr>
          <th><Term key="group">Group or account</Term></th>
          <th><Term key="account">Source</Term></th>
          <th class="amount"><Term key="balances_table" context={asOf}>Balance</Term></th>
          <th class="amount"><Term key="balance_change">Change in period</Term></th>
        </tr>
      </thead>
      <tbody>
        {#each balance.totals.filter((total) => total.kind !== "group") as total (total.key)}
          <tr class="total clickable" onclick={() => drillLine(total.key, total.label)} title="Show the rows">
            <td>
              <span class="swatch" style:background={colorOfSeries(total.key)}></span>
              <button class="row-link" onclick={(event) => (event.stopPropagation(), drillLine(total.key, total.label))}>
                {#if total.kind === "net_worth"}<Term key="net_worth">Net worth</Term>{:else}<Term key="available">Available</Term>{/if}
              </button>
              <span class="faint">{#if total.kind === "net_worth"}<Term key="group">every group</Term>{:else}<Term key="role_spending">spending groups</Term>{/if}</span>
            </td>
            <td></td>
            <td class="amount">{formatAmount(total.balance)}</td>
            {@render changeCell(total.change)}
          </tr>
        {/each}
        {#each balance.totals.filter((total) => total.kind === "group") as total (total.key)}
          <tr class="group clickable" onclick={() => drillLine(total.key, total.label)} title="Show the rows">
            <td>
              <span class="swatch" style:background={colorOfSeries(total.key)}></span>
              <button class="row-link" onclick={(event) => (event.stopPropagation(), drillLine(total.key, total.label))}>{total.label}</button>
              <span class="faint">{#if roles.get(total.group ?? "") === "holding"}<Term key="role_holding">holding</Term>{:else}<Term key="role_spending">spending</Term>{/if}</span>
            </td>
            <td></td>
            <td class="amount">{formatAmount(total.balance)}</td>
            {@render changeCell(total.change)}
          </tr>
          {#each balance.accounts.filter((row) => row.group === total.group) as row (row.account_source)}
            <tr class="clickable" onclick={() => drillLine(row.account_source, row.account_source)} title="Show the rows">
              <td class="account">
                <span class="swatch" style:background={colorOfSeries(row.account_source)}></span>
                <button class="row-link" onclick={(event) => (event.stopPropagation(), drillLine(row.account_source, row.account_source))}>{row.account}</button>
              </td>
              <td class="muted">{row.source}</td>
              <td class="amount">{formatAmount(row.balance)}</td>
              {@render changeCell(row.change)}
            </tr>
          {/each}
        {/each}
      </tbody>
    </table>
  </Panel>
  <Panel title="Bridge" term="bridge" subtitle="how the balances changed over {periodLabel(data.period.months)}: every row is in one term · click a term for its rows">
    <Bridge bridge={data.bridge} {ondrill} />
  </Panel>
</div>

<Panel title="Data checks" term="data_checks" subtitle="what the numbers rest on">
  <Health health={data.health} months={data.period.months} {settings} {ondrill} />
</Panel>

<style>
  .row {
    display: grid;
    grid-template-columns: minmax(0, 3fr) minmax(320px, 2fr);
    gap: 12px;
    align-items: start;
  }
  @media (max-width: 1000px) {
    .row {
      grid-template-columns: minmax(0, 1fr);
    }
  }
  .row-link {
    background: none;
    border: none;
    padding: 0;
    font: inherit;
    color: inherit;
  }
  .row-link:hover,
  .row-link:focus-visible {
    text-decoration: underline;
  }
  .toggle {
    display: flex;
    gap: 5px;
    align-items: center;
    color: var(--muted);
  }
  .segmented button {
    font-size: 12px;
  }
  .balances td {
    padding: 5px 8px;
  }
  .balances tr.total td {
    font-weight: 600;
    background: var(--surface-2);
  }
  .balances tr.group td {
    font-weight: 600;
    border-top: 1px solid var(--border-strong);
  }
  .balances td.account {
    padding-left: 22px;
  }
  .clickable {
    cursor: pointer;
  }
</style>
