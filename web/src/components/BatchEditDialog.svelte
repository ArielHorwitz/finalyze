<!--
  Set the same tags, kind, description or review flag on the selected rows' drafts,
  each field optional, with a preview of each row's changes. Nothing is
  written until the drafts are submitted.
-->
<script lang="ts">
  import { batchDraft, type BatchFields } from "../lib/batch";
  import type { Row } from "../lib/rows.svelte";
  import { bind } from "../lib/use-keys.svelte";
  import BatchFieldsForm from "./BatchFieldsForm.svelte";
  import BatchPreview from "./BatchPreview.svelte";
  import KeyHint from "./KeyHint.svelte";
  import Modal from "./Modal.svelte";

  /** The selected rows: with none, nothing can be applied. */
  let { rows, onapply, onclose }: { rows: Row[]; onapply: (fields: BatchFields) => void; onclose: () => void } = $props();

  let fields = $state<BatchFields>({ label: null, kind: null, description: null, needsReview: null });
  let ready = $state(false);
  const items = $derived(rows.map((row) => ({ transaction: row.transaction, outcome: batchDraft(row.transaction, row.draft, fields) })));
  const canApply = $derived(ready && items.some(({ outcome }) => "draft" in outcome));

  function apply() {
    if (canApply) onapply(fields);
  }
</script>

<Modal title="Batch edit" width="760px" {onclose} bindings={() => [bind("dialog.commit", apply, { description: "Apply" })]}>
  {#if rows.length === 0}
    <p class="scope none">Applies to 0 rows (none selected). Select rows to edit.</p>
  {:else}
    <p class="muted scope">Applies to {rows.length} selected row{rows.length === 1 ? "" : "s"}. Nothing is saved until you submit.</p>
  {/if}
  <BatchFieldsForm bind:fields bind:ready />
  {#if ready && rows.length > 0}
    <div class="preview"><BatchPreview {items} /></div>
  {/if}
  {#snippet footer()}
    <button onclick={onclose}>Cancel</button>
    <button class="primary" onclick={apply} disabled={!canApply}>Apply to drafts <KeyHint action="dialog.commit" /></button>
  {/snippet}
</Modal>

<style>
  .scope {
    margin: 0 0 12px;
  }
  .scope.none {
    color: var(--warning);
  }
  .preview {
    margin-top: 14px;
    border-top: 1px solid var(--border);
    padding-top: 10px;
  }
</style>
