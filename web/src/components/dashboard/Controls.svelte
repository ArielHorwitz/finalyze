<!--
  The dashboard's options under its header, in a panel of labeled groups by
  what they do:
  - When: the period (see `PeriodControls`).
  - Which money: the accounts, the groups of cash flows and breakdowns, and
    only some tags (in a popover).
  - How to compute: the rolling window, one-offs in averages, netting,
    complete months, and the Monthly view's months.
  A group's label lights up while any of its options isn't the default.
  Changes go to the URL, which reloads the data.
-->
<script lang="ts">
  import { onMount } from "svelte";
  import { api, type Dashboard, type DashboardDefaults } from "../../lib/api";
  import { changedControls, COMPUTE_DEFAULTS, CONTROL_GROUPS, shownGroups, toggleGroup, windowName, type DashboardState } from "../../lib/dashboard";
  import { app, keyContext } from "../../lib/state.svelte";
  import Term from "../Term.svelte";
  import PeriodControls from "./PeriodControls.svelte";

  let {
    settings,
    data,
    rollingName,
    update,
  }: {
    settings: DashboardState;
    data: Dashboard | null;
    rollingName: string | null;
    update: (changes: Partial<DashboardState>) => void;
  } = $props();

  let tagsOpen = $state(false);
  // The Settings page's defaults, to tell which options differ from them
  let defaults = $state<DashboardDefaults | null>(null);
  onMount(() => {
    api.dashboardDefaults().then((loaded) => (defaults = loaded), () => {});
  });
  const changed = $derived(changedControls(settings, defaults));
  const netByTag = $derived(settings.netByTag ?? data?.options.net_by_tag ?? false);
  const completeMonths = $derived(settings.completeMonths ?? data?.options.complete_months_only ?? true);
  const tagNames = $derived(app.tags.map((tag) => tag.name));
  const groups = $derived(data?.groups ?? []);
  const flowGroups = $derived(shownGroups(settings, groups));
  const holding = $derived(new Set(groups.filter((group) => group.role === "holding").flatMap((group) => group.accounts)));

  function toggle<T>(items: T[], item: T): T[] {
    return items.includes(item) ? items.filter((other) => other !== item) : [...items, item];
  }
</script>

{#snippet legend(group: (typeof CONTROL_GROUPS)[number])}
  {@const changes = Object.values(changed[group.id])}
  <legend class:active={changes.length > 0}>
    <Term key="dashboard_controls" context={changes.length ? `Not the default: ${changes.join(", ")}` : ""}>{group.label}</Term>{#if changes.length}&nbsp;•{/if}
  </legend>
{/snippet}

<div class="controls control-panel">
  <PeriodControls {settings} {data} {update} />

  <fieldset class="control-group">
    {@render legend(CONTROL_GROUPS[1])}
    <div class="chips" role="group" aria-label="Accounts">
      {#each app.accounts as account}
        <button
          class="chip"
          class:on={settings.accounts.includes(account.name)}
          aria-pressed={settings.accounts.includes(account.name)}
          onclick={() => update({ accounts: toggle(settings.accounts, account.name) })}
          title={settings.accounts.length === 0 ? "All accounts: click to show only some" : "Toggle the account"}
        >
          {#if account.color}<span class="swatch" style:background={account.color}></span>{/if}
          {account.name}
          {#if holding.has(account.name)}<span class="ext"><Term key="role_holding">hold</Term></span>{/if}
        </button>
      {/each}
    </div>

    {#if groups.length > 1}
      <div class="chips" role="group" aria-label="Groups in cash flows and breakdowns">
        <span class="faint"><Term key="flows_of">flows of</Term></span>
        {#each groups as group (group.name)}
          {@const next = toggleGroup(settings, groups, group.name)}
          <button
            class="chip group"
            class:on={flowGroups.includes(group.name)}
            class:custom={settings.groups.length > 0}
            disabled={next === null}
            onclick={() => next && update({ groups: next })}
            title={next === null
              ? "The only group in the flows: choose another one first"
              : `${group.role === "holding" ? "Holding" : "Spending"} group: toggle it in cash flows, breakdowns and trends (e: every group)`}
          >
            {#if group.color}<span class="swatch" style:background={group.color}></span>{/if}
            {group.name}
          </button>
        {/each}
      </div>
    {/if}

    <!-- svelte-ignore a11y_no_static_element_interactions (Escape from the popover's buttons) -->
    <div class="only-tags" onkeydown={(event) => event.key === "Escape" && tagsOpen && ((tagsOpen = false), event.stopPropagation())}>
      <span class="faint"><Term key="only_these_tags">tags</Term></span>
      <button class="chip" class:on={settings.tags.length > 0} aria-expanded={tagsOpen} onclick={() => (tagsOpen = !tagsOpen)}>
        {#if settings.tags.length === 0}all{:else if settings.tags.length <= 2}{settings.tags.join(", ")}{:else}{settings.tags.length} tags{/if} ▾
      </button>
      {#if tagsOpen}
        <div class="popover" role="dialog" aria-label="Only these tags">
          <h4><Term key="only_these_tags">Only these tags</Term> <span class="faint">(cash flow and breakdowns)</span></h4>
          <div class="chips">
            {#each tagNames as tag}
              <button class="chip small" class:on={settings.tags.includes(tag)} onclick={() => update({ tags: toggle(settings.tags, tag) })}>{tag}</button>
            {/each}
          </div>
          {#if settings.tags.length}<button class="link" onclick={() => update({ tags: [] })}>All tags</button>{/if}
        </div>
      {/if}
    </div>
  </fieldset>

  <fieldset class="control-group">
    {@render legend(CONTROL_GROUPS[2])}
    {#if data && rollingName}
      <label class:changed={changed.how.rolling}>
        <Term key="average_of" context="Keys: w and W">average of</Term>
        <select value={rollingName} onchange={(event) => update({ rolling: event.currentTarget.value })}>
          {#each data.rolling_windows as window}
            <option value={String(window)}>{windowName(window)}</option>
          {/each}
        </select>
      </label>
    {/if}
    <label class:changed={changed.how.oneOffs}>
      <input type="checkbox" checked={settings.oneOffs} onchange={() => update({ oneOffs: !settings.oneOffs })} />
      <Term key="one_offs_in_averages" context={keyContext("dashboard.oneOffs")}>one-offs in averages</Term>
    </label>
    <label class:changed={changed.how.netByTag}>
      <input type="checkbox" checked={netByTag} onchange={() => update({ netByTag: !netByTag })} />
      <Term key="net_by_tag" context={keyContext("dashboard.netByTag")}>net by tag</Term>
    </label>
    <label class:changed={changed.how.completeMonths}>
      <input type="checkbox" checked={completeMonths} onchange={(event) => update({ completeMonths: event.currentTarget.checked })} />
      <Term key="complete_months_only">complete months only</Term>
    </label>
    <label class:changed={changed.how.breakdownMonths}>
      <Term key="monthly_view_months">Monthly view</Term>
      <input
        type="number"
        min="1"
        max="36"
        aria-label="Monthly view months"
        value={settings.breakdownMonths ?? data?.options.breakdown_months ?? 3}
        onchange={(event) => update({ breakdownMonths: Math.max(1, Number(event.currentTarget.value) || 1) })}
      />
      mo
    </label>
    {#if Object.keys(changed.how).length}
      <button class="link" onclick={() => update(COMPUTE_DEFAULTS)}><Term key="reset_defaults">reset</Term></button>
      <a class="defaults" href="#/settings" title="Change the defaults in Settings">defaults…</a>
    {/if}
  </fieldset>
</div>

<svelte:window
  onmousedown={(event) => {
    if (tagsOpen && !(event.target as HTMLElement).closest(".only-tags")) tagsOpen = false;
  }}
/>

<style>
  .controls {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .chips {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 4px;
  }
  .chip {
    border-radius: 12px;
    padding: 2px 10px;
    color: var(--muted);
    border-color: var(--border);
  }
  .chip.small {
    padding: 1px 8px;
    font-size: 12px;
  }
  .chip.on {
    color: var(--text);
    border-color: var(--accent);
    background: var(--accent-soft);
  }
  .chip.group.on:not(.custom) {
    border-color: var(--border-strong);
    background: var(--surface-3);
  }
  .ext {
    margin-left: 4px;
    font-size: 10px;
    color: var(--faint);
    text-transform: uppercase;
  }
  label {
    display: flex;
    align-items: center;
    gap: 5px;
    color: var(--muted);
  }
  label.changed {
    color: var(--accent);
  }
  input[type="number"] {
    width: 46px;
  }
  .only-tags {
    position: relative;
    display: flex;
    align-items: center;
    gap: 4px;
  }
  .popover {
    position: absolute;
    z-index: 50;
    top: calc(100% + 6px);
    left: 0;
    width: 420px;
    padding: 12px;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 8px;
    background: var(--surface-2);
    border: 1px solid var(--border-strong);
    border-radius: 7px;
    box-shadow: 0 12px 32px rgba(0, 0, 0, 0.5);
  }
  h4 {
    margin: 0;
    font-weight: 500;
    font-size: 12px;
  }
  .defaults {
    font-size: 12px;
    color: var(--faint);
  }
</style>
