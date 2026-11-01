<!--
  The rows behind a number of the dashboard, in a side panel. They open in
  Transactions, to annotate them. Each shows its part's description, else its
  transaction's, else the raw description (see TransactionDescription).
-->
<script lang="ts">
  import { onMount } from "svelte";
  import { showDate } from "../../lib/display.svelte";
  import { api, type AnalysisRow, type DashboardOptions } from "../../lib/api";
  import { transactionsQuery, type Drill } from "../../lib/dashboard";
  import { rowDescription } from "../../lib/descriptions";
  import { formatAmount } from "../../lib/money";
  import { navigate } from "../../lib/router.svelte";
  import { toastError } from "../../lib/state.svelte";
  import { bind, useKeys } from "../../lib/use-keys.svelte";
  import Amount from "../Amount.svelte";
  import Term from "../Term.svelte";
  import TransactionDescription from "../TransactionDescription.svelte";

  let { drill, options, onclose }: { drill: Drill; options: DashboardOptions; onclose: () => void } = $props();

  let rows = $state<AnalysisRow[] | null>(null);
  let panel: HTMLElement;
  // Expenses and savings come as positive amounts: show them as the transactions' (negative)
  const sign = $derived(drill.selection.flow === "expenses" || drill.selection.flow === "saved" ? -1 : 1);
  const total = $derived((rows ?? []).reduce((sum, row) => sum + row.amount, 0));
  const ids = $derived([...new Set((rows ?? []).map((row) => row.id).filter((id) => id !== null))]);

  let requestNumber = 0;
  async function load() {
    const number = ++requestNumber;
    rows = null;
    try {
      const result = await api.dashboardRows(options, drill.selection);
      if (number === requestNumber) rows = result.rows;
    } catch (error) {
      if (number !== requestNumber) return;
      toastError(error, "Rows");
      rows = [];
    }
  }

  $effect(() => {
    drill;
    load();
  });

  function open(only?: string | null) {
    const selected = only ? [only] : ids;
    if (selected.length === 0) return;
    navigate("transactions", transactionsQuery(selected));
  }

  useKeys(
    "Rows",
    () => [
      bind("dialog.close", onclose, { description: "Close the rows" }),
      bind("drill.open", () => open()),
    ],
    { modal: true, allowsNavigation: true },
  );

  onMount(() => panel.focus());
</script>

<div class="backdrop" onmousedown={(event) => event.target === event.currentTarget && onclose()} role="presentation">
  <aside class="panel" bind:this={panel} tabindex="-1" aria-label="Rows of {drill.title}">
    <header>
      <div>
        <h2>{drill.title}</h2>
        <div class="muted summary">
          {#if rows === null}loading…{:else}
            {rows.length} row{rows.length === 1 ? "" : "s"} · total <b>{formatAmount(total)}</b>
          {/if}
        </div>
      </div>
      <button class="link close" onclick={onclose} aria-label="Close">✕</button>
    </header>
    {#if drill.note}<p class="note-line">{drill.note}</p>{/if}
    <div class="actions">
      <button class="primary" onclick={() => open()} disabled={ids.length === 0} title="Enter">
        Open {ids.length} in Transactions
      </button>
      <span class="faint">to annotate them</span>
    </div>
    <div class="list">
      {#if rows && rows.length === 0}
        <p class="empty">No rows.</p>
      {/if}
      {#each rows ?? [] as row, index (`${row.id}-${index}`)}
        <button class="row" onclick={() => open(row.id)} title="Open in Transactions">
          <span class="date">{showDate(row.date)}</span>
          <span class="what"><TransactionDescription description={rowDescription(row)} raw={row.raw_description} /></span>
          <Amount value={sign * row.amount} />
          <span class="meta">
            {row.account_source}
            {#if row.tag}· <span class="tag">{row.tag}{row.subtag ? ` / ${row.subtag}` : ""}</span>{:else}· <span class="untagged">untagged</span>{/if}
            {#if row.kind_set}· <span class="kind kind-{row.kind}"><Term key="kind_set" quiet context="Set on this transaction">◆ {row.kind}</Term></span>{:else if row.kind !== "regular"}· <span class="kind kind-{row.kind}">{row.kind}</span>{/if}
          </span>
        </button>
      {/each}
    </div>
    <p class="faint hint">Split transactions show only the parts counted here.</p>
  </aside>
</div>

<style>
  .backdrop {
    position: fixed;
    inset: 0;
    z-index: 100;
    background: rgba(5, 7, 10, 0.35);
  }
  .panel {
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    width: min(560px, 92vw);
    display: flex;
    flex-direction: column;
    gap: 10px;
    padding: 14px 16px;
    background: var(--surface);
    border-left: 1px solid var(--border-strong);
    box-shadow: -16px 0 40px rgba(0, 0, 0, 0.45);
    outline: none;
    animation: slide 0.16s ease-out;
  }
  @keyframes slide {
    from {
      transform: translateX(24px);
    }
  }
  header {
    display: flex;
    justify-content: space-between;
    gap: 12px;
  }
  h2 {
    margin: 0 0 2px;
    font-size: 15px;
    font-weight: 600;
  }
  .summary {
    font-variant-numeric: tabular-nums;
  }
  .close {
    color: var(--muted);
    font-size: 16px;
  }
  .actions {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .list {
    flex: 1;
    overflow: auto;
    border: 1px solid var(--border);
    border-radius: var(--radius);
  }
  .row {
    display: grid;
    grid-template-columns: 82px 1fr auto;
    gap: 2px 10px;
    width: 100%;
    padding: 6px 10px;
    border: none;
    border-bottom: 1px solid var(--border);
    border-radius: 0;
    background: none;
    text-align: left;
  }
  .row:hover {
    background: var(--surface-2);
  }
  .date {
    color: var(--muted);
    font-variant-numeric: tabular-nums;
    font-size: 12px;
  }
  .what {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    text-align: left;
  }
  .meta {
    grid-column: 2 / 4;
    color: var(--faint);
    font-size: 12px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .tag {
    color: var(--muted);
  }
  .kind-one-off {
    color: var(--warning);
  }
  .kind-saving {
    color: #b49bf0;
  }
  .untagged {
    color: var(--warning);
  }
  .note-line {
    margin: 0;
    padding: 6px 10px;
    border-radius: var(--radius);
    background: var(--accent-soft);
    color: var(--text);
    font-size: 12px;
  }
  .hint {
    margin: 0;
    font-size: 12px;
  }
</style>
