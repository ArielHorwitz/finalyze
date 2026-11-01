<!--
  What a batch edit does to each row: its changes, "unchanged", or why it's
  skipped.
-->
<script lang="ts">
  import type { Transaction } from "../lib/api";
  import { showDate } from "../lib/display.svelte";
  import type { BatchOutcome } from "../lib/batch";
  import { writtenDescription } from "../lib/descriptions";
  import Amount from "./Amount.svelte";
  import TransactionDescription from "./TransactionDescription.svelte";

  let { items }: { items: { transaction: Transaction; outcome: BatchOutcome }[] } = $props();

  const changed = $derived(items.filter(({ outcome }) => "changes" in outcome && outcome.changes.length > 0).length);
  const skipped = $derived(items.filter(({ outcome }) => "skipped" in outcome).length);
  const unchanged = $derived(items.length - changed - skipped);
</script>

<p class="summary">
  <strong>{changed}</strong> to change{#if unchanged}, <span class="muted">{unchanged} unchanged</span>{/if}{#if skipped}, <span class="warning-text">{skipped} skipped</span>{/if}.
</p>
<div class="list">
  <table>
    <tbody>
      {#each items as { transaction, outcome } (transaction.id)}
        <tr class:skipped={"skipped" in outcome}>
          <td class="muted">{showDate(transaction.date)}</td>
          <td class="what"><TransactionDescription description={writtenDescription(transaction.annotation, transaction.status === "rule")} raw={transaction.raw_description} /></td>
          <td class="amount"><Amount value={transaction.amount} /></td>
          <td class="changes">
            {#if "skipped" in outcome}
              <span class="warning-text">skipped: {outcome.skipped}</span>
            {:else if outcome.changes.length === 0}
              <span class="faint">unchanged</span>
            {:else}
              {outcome.changes.join(" · ")}
            {/if}
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
</div>

<style>
  .summary {
    margin: 0 0 8px;
  }
  .warning-text {
    color: var(--warning);
  }
  .list {
    max-height: 280px;
    overflow: auto;
    border: 1px solid var(--border);
    border-radius: var(--radius);
  }
  table {
    width: 100%;
    border-collapse: collapse;
  }
  td {
    padding: 3px 8px;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  .what {
    max-width: 280px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .changes {
    white-space: normal;
  }
  tr.skipped td:not(.changes) {
    color: var(--faint);
  }
</style>
