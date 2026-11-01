<script lang="ts">
  import type { IngestSummary } from "../lib/api";
  import Modal from "./Modal.svelte";
  import { bind } from "../lib/use-keys.svelte";

  let { result, onclose }: { result: IngestSummary; onclose: () => void } = $props();
</script>

<Modal title="Ingested" {onclose} width="420px" allowsNavigation bindings={() => [bind("dialog.confirm", onclose, { description: "OK" })]}>
  <table class="summary">
    <tbody>
      {#each Object.entries(result.accounts) as [account, count]}
        <tr><td>{account}</td><td class="amount">{count}</td></tr>
      {/each}
      <tr class="total"><td>Transactions</td><td class="amount">{result.transactions}</td></tr>
      <tr><td>New since the last ingest</td><td class="amount">{result.new_ids.length}</td></tr>
      <tr><td>Vanished</td><td class="amount">{result.vanished_ids.length}</td></tr>
      {#if result.changed_ids.length}
        <tr><td>Changed</td><td class="amount">{result.changed_ids.length}</td></tr>
      {/if}
      <tr class:warn={result.new_orphan_ids.length > 0}>
        <td>Orphans (new)</td>
        <td class="amount">{result.orphans} ({result.new_orphan_ids.length})</td>
      </tr>
    </tbody>
  </table>
  {#if !result.data_changed}<p class="muted">Nothing changed: the ingested data is as it was.</p>{/if}
  {#snippet footer()}
    <button class="primary" data-autofocus onclick={onclose}>OK <kbd>Enter</kbd></button>
  {/snippet}
</Modal>

<style>
  .summary {
    width: 100%;
    border-collapse: collapse;
  }
  td {
    padding: 4px 0;
    border-bottom: 1px solid var(--border);
  }
  .total td {
    font-weight: 600;
  }
  .warn td {
    color: var(--warning);
  }
</style>
