<!--
  The dashboard: headline cards, then one view at a time (the overview,
  balance, cash flow, breakdown, trends, monthly, the ledger). Other views than
  the overview have a one-line strip of cards. Its controls live in the URL, so a view can be bookmarked or shared, and Back returns to it.
  Clicking a number shows the rows behind it, which open in Transactions.
-->
<script lang="ts">
  import { untrack } from "svelte";
  import Cards from "../components/dashboard/Cards.svelte";
  import Controls from "../components/dashboard/Controls.svelte";
  import DrillPanel from "../components/dashboard/DrillPanel.svelte";
  import BalanceView from "../components/dashboard/BalanceView.svelte";
  import BreakdownView from "../components/dashboard/BreakdownView.svelte";
  import CashFlowView from "../components/dashboard/CashFlowView.svelte";
  import LedgerView from "../components/dashboard/LedgerView.svelte";
  import MonthlyView from "../components/dashboard/MonthlyView.svelte";
  import OverviewView from "../components/dashboard/OverviewView.svelte";
  import TrendsView from "../components/dashboard/TrendsView.svelte";
  import Term from "../components/Term.svelte";
  import { api, type Dashboard, type DashboardOptions } from "../lib/api";
  import {
    PERIODS,
    VIEWS,
    parseState,
    periodMonths,
    shiftPeriod,
    stateOptions,
    stateQuery,
    type DashboardState,
    type Drill,
  } from "../lib/dashboard";
  import { router, setQuery } from "../lib/router.svelte";
  import { app, keyContext, titled, toastError } from "../lib/state.svelte";
  import { bind, useKeys } from "../lib/use-keys.svelte";

  const settings = $derived(parseState(router.query));
  let data = $state<Dashboard | null>(null);
  let loading = $state(false);
  let error = $state<string | null>(null);
  // The rows behind a number, with the options of the data it came from
  let drill = $state<{ drill: Drill; options: DashboardOptions } | null>(null);
  let requestNumber = 0;

  function update(changes: Partial<DashboardState>) {
    setQuery(stateQuery({ ...settings, ...changes }));
  }

  async function load(options = currentOptions()) {
    const number = ++requestNumber;
    loading = true;
    try {
      const result = await api.dashboard(options);
      if (number !== requestNumber) return; // a newer request is on its way
      data = result;
      error = null;
    } catch (failure) {
      if (number !== requestNumber) return;
      error = failure instanceof Error ? failure.message : String(failure);
      toastError(failure, "Dashboard");
    } finally {
      if (number === requestNumber) loading = false;
    }
  }

  // Reload when the options change (not the view), or the data does. Trends
  // by subtag are only asked for while shown: data with them still does
  // without, so going back from them doesn't reload.
  const optionsKey = $derived(JSON.stringify(stateOptions(settings)));
  const subtagTrends = $derived(settings.view === "trends" && settings.trendsSubtags);
  function currentOptions(): DashboardOptions {
    return { ...stateOptions(settings), subtag_trends: subtagTrends };
  }
  let requested: { key: string; dataVersion: number; subtagTrends: boolean } | null = null;
  $effect(() => {
    const next = { key: optionsKey, dataVersion: app.dataVersion, subtagTrends };
    untrack(() => {
      const covered =
        requested !== null &&
        requested.key === next.key &&
        requested.dataVersion === next.dataVersion &&
        (requested.subtagTrends || !next.subtagTrends);
      if (covered) return;
      requested = next;
      load();
    });
  });

  /** The rolling window shown: the chosen one, else the default. */
  const windows = $derived((data?.rolling_windows ?? []).map(String));
  const rollingName = $derived.by(() => {
    if (!data) return null;
    if (settings.rolling && windows.includes(settings.rolling)) return settings.rolling;
    return String(data.options.rolling_months ?? windows[0]);
  });

  function cycle<T>(items: readonly T[], current: T, step: number): T {
    const index = items.indexOf(current);
    return items[(index + step + items.length) % items.length];
  }

  function cycleView(step: number) {
    update({ view: cycle(VIEWS.map((view) => view.id), settings.view, step) });
  }

  function cyclePeriod(step: number) {
    const ids = PERIODS.map((period) => period.id);
    update({ period: cycle(ids, settings.period, step), startMonth: null, endMonth: null });
  }

  // From the URL rather than the last data loaded, so moves made while it
  // loads add up (and a preset's months show at once)
  function movePeriod(step: number) {
    const available = data?.available_months ?? [];
    const moved = shiftPeriod(periodMonths(settings, available), step, available);
    if (moved) update(moved);
  }

  function cycleRolling(step: number) {
    if (!data || !rollingName) return;
    update({ rolling: cycle(windows, rollingName, step) });
  }

  useKeys("Dashboard", () => [
    ...VIEWS.map((view) => bind(`dashboard.view.${view.id}`, () => update({ view: view.id }))),
    bind("dashboard.nextView", () => cycleView(1)),
    bind("dashboard.previousView", () => cycleView(-1)),
    bind("dashboard.nextPeriod", () => cyclePeriod(1)),
    bind("dashboard.previousPeriod", () => cyclePeriod(-1)),
    bind("dashboard.periodBack", () => movePeriod(-1)),
    bind("dashboard.periodForward", () => movePeriod(1)),
    bind("dashboard.kind", () => update({ kind: settings.kind === "expenses" ? "incomes" : "expenses" })),
    bind("dashboard.nextRolling", () => cycleRolling(1)),
    bind("dashboard.previousRolling", () => cycleRolling(-1)),
    bind("dashboard.groups", () => update({ groups: settings.groups.length || !data ? [] : data.groups.map((group) => group.name) })),
    bind("dashboard.oneOffs", () => update({ oneOffs: !settings.oneOffs })),
    bind("dashboard.netByTag", () => update({ netByTag: !(settings.netByTag ?? data?.options.net_by_tag ?? false) })),
    // Through the data version, so the ledger reloads too
    bind("dashboard.reload", () => app.dataVersion++),
  ]);

  const viewProps = $derived(
    data && rollingName ? { data, settings, rollingName, update, ondrill: (next: Drill) => (drill = { drill: next, options: data!.options }) } : null,
  );
</script>

<div class="page dashboard">
  <div class="page-header">
    <h1>Dashboard</h1>
    <nav class="views" aria-label="Dashboard views">
      {#each VIEWS as view}
        <button class="view" class:current={settings.view === view.id} onclick={() => update({ view: view.id })} title={titled(view.title, `dashboard.view.${view.id}`)}>
          <Term key={view.term} context={keyContext(`dashboard.view.${view.id}`)}>{view.title}</Term>
        </button>
      {/each}
    </nav>
    <div class="spacer"></div>
    {#if loading}<span class="loading" aria-live="polite">updating…</span>{/if}
  </div>
  <Controls {settings} {data} {rollingName} {update} />

  <div class="content" class:stale={loading && data !== null}>
    {#if error && data}
      <div class="failed">
        Showing the last data that loaded: the new options failed ({error}).
        <button onclick={() => load()}>Retry</button>
      </div>
    {/if}
    {#if viewProps}
      <Cards {...viewProps} compact={settings.view !== "overview"} />
      {#if settings.view === "overview"}
        <OverviewView {...viewProps} />
      {:else if settings.view === "balance"}
        <BalanceView {...viewProps} />
      {:else if settings.view === "flow"}
        <CashFlowView {...viewProps} />
      {:else if settings.view === "breakdown"}
        <BreakdownView {...viewProps} />
      {:else if settings.view === "trends"}
        <TrendsView {...viewProps} />
      {:else if settings.view === "monthly"}
        <MonthlyView {...viewProps} />
      {:else if settings.view === "ledger"}
        <LedgerView {...viewProps} />
      {/if}
    {:else if error}
      <div class="empty">
        <p>{error}</p>
        <button onclick={() => load()}>Retry</button>
      </div>
    {:else}
      <div class="empty">Loading…</div>
    {/if}
  </div>
</div>

{#if drill}
  <DrillPanel drill={drill.drill} options={drill.options} onclose={() => (drill = null)} />
{/if}

<style>
  .dashboard {
    gap: 8px;
  }
  .views {
    display: flex;
    gap: 2px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 7px;
    padding: 2px;
  }
  .view {
    border: none;
    background: none;
    color: var(--muted);
    padding: 3px 11px;
    border-radius: 5px;
  }
  .view:hover:not(.current) {
    color: var(--text);
  }
  .view.current {
    background: var(--surface-3);
    color: var(--text);
  }
  .loading {
    color: var(--faint);
    font-size: 12px;
  }
  .content {
    flex: 1;
    min-height: 0;
    overflow: auto;
    display: flex;
    flex-direction: column;
    gap: 12px;
    padding-bottom: 16px;
    transition: opacity 0.15s;
  }
  .content.stale {
    opacity: 0.75;
  }
  .failed {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 6px 12px;
    border: 1px solid var(--danger);
    border-radius: var(--radius);
    background: var(--danger-soft);
  }
</style>
