<!--
  When: the dashboard's period, a preset or explicit months moved with the
  arrows. The first line of the dashboard's `Controls`, the most used of
  them. Changes go to the URL, which reloads the data.
-->
<script lang="ts">
  import type { Dashboard } from "../../lib/api";
  import { changedControls, CONTROL_GROUPS, PERIODS, periodLabel, periodMonths, shiftPeriod, type DashboardState } from "../../lib/dashboard";
  import { titled } from "../../lib/state.svelte";
  import Term from "../Term.svelte";

  let {
    settings,
    data,
    update,
  }: {
    settings: DashboardState;
    data: Dashboard | null;
    update: (changes: Partial<DashboardState>) => void;
  } = $props();

  const custom = $derived(settings.startMonth !== null || settings.endMonth !== null);
  const available = $derived(data?.available_months ?? []);
  // From the URL, so a change shows before its data loads
  const months = $derived(periodMonths(settings, available));
  const changed = $derived(Object.values(changedControls(settings, null).when));

  function move(step: number) {
    const moved = shiftPeriod(months, step, available);
    if (moved) update(moved);
  }

  const atStart = $derived(shiftPeriod(months, -1, available) === null);
  const atEnd = $derived(shiftPeriod(months, 1, available) === null);
</script>

<fieldset class="control-group period">
  <legend class:active={changed.length > 0}>
    <Term key="dashboard_controls" context={changed.length ? `Not the default: ${changed.join(", ")}` : ""}>{CONTROL_GROUPS[0].label}</Term>{#if changed.length}&nbsp;•{/if}
  </legend>
  <div class="segmented" role="group" aria-label="Period">
    {#each PERIODS as period}
      <button
        class:on={!custom && settings.period === period.id}
        onclick={() => update({ period: period.id, startMonth: null, endMonth: null })}
        title={titled(period.title, "dashboard.nextPeriod")}
      >
        <Term key="period" context="{period.title} (key: p)">{period.label}</Term>
      </button>
    {/each}
  </div>
  <div class="range">
    <button class="arrow" onclick={() => move(-1)} disabled={atStart} title={titled("Previous period", "dashboard.periodBack")} aria-label="Previous period">‹</button>
    <span class="months" class:custom>
      <Term key="period" context={custom ? "Explicit months (choose a preset to go back)" : "Keys: , and ."}>{periodLabel(months)}</Term>
      <span class="faint">· {months.length} mo</span>
    </span>
    <button class="arrow" onclick={() => move(1)} disabled={atEnd} title={titled("Next period", "dashboard.periodForward")} aria-label="Next period">›</button>
  </div>
</fieldset>

<style>
  .range {
    display: flex;
    align-items: center;
    gap: 4px;
  }
  .arrow {
    padding: 2px 8px;
  }
  .months {
    min-width: 170px;
    text-align: center;
    font-variant-numeric: tabular-nums;
  }
  .months.custom {
    color: var(--accent);
  }
</style>
