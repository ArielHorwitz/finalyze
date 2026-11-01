<!--
  Set the tags, kind, description or review flag of many transactions at once (each
  field optional) through the API's bulk endpoint: first a preview of each
  transaction's changes, then a commit of exactly those.
-->
<script lang="ts">
  import { api, transactionsByIds, type BulkResult, type Transaction, type TransactionQuery } from "../lib/api";
  import { batchDraft, type BatchFields, type BatchOutcome } from "../lib/batch";
  import { baselineDraft } from "../lib/drafts";
  import { labelExists, toast, toastError } from "../lib/state.svelte";
  import { bind } from "../lib/use-keys.svelte";
  import BatchFieldsForm from "./BatchFieldsForm.svelte";
  import BatchPreview from "./BatchPreview.svelte";
  import Modal from "./Modal.svelte";

  let {
    selectedIds,
    filterQuery,
    matching,
    oncommitted,
    onclose,
  }: {
    selectedIds: string[];
    /** The page's filters, and how many transactions they match (over every page). */
    filterQuery: TransactionQuery;
    matching: number;
    oncommitted: () => void;
    onclose: () => void;
  } = $props();

  // The selected rows, unless all those matching the filters are chosen explicitly
  let scope = $state<"selected" | "matching">("selected");
  const query = $derived<TransactionQuery>(scope === "selected" ? { ids: selectedIds } : filterQuery);
  const noTargets = $derived(scope === "selected" && selectedIds.length === 0);
  const unfiltered = $derived(Object.values(filterQuery).every((value) => value === null));

  let fields = $state<BatchFields>({ label: null, kind: null, description: null, needsReview: null });
  let ready = $state(false);
  let preview = $state<BulkResult | null>(null);
  let previewItems = $state<{ transaction: Transaction; outcome: BatchOutcome }[]>([]);
  let busy = $state(false);
  const isNew = $derived(fields.label !== null && !labelExists(fields.label.tag, fields.label.subtag));

  function request(dryRun: boolean) {
    return {
      query,
      tag: fields.label?.tag ?? null,
      subtag: fields.label?.subtag ?? null,
      kind: fields.kind,
      description: fields.description,
      needs_review: fields.needsReview,
      dry_run: dryRun,
      expected_ids: dryRun ? null : preview!.affected_ids,
      allow_new_tags: isNew,
      author: "owner",
    };
  }

  /** Each transaction's changes, worked out as the API does (which decides
   * which are affected and which skipped). */
  function outcomeOf(transaction: Transaction, result: BulkResult): BatchOutcome {
    const outcome = batchDraft(transaction, baselineDraft(transaction), fields);
    if (result.skipped_rule_ids.includes(transaction.id)) return { skipped: "tagged by a rule, which a batch can't change" };
    if (result.skipped_untagged_ids.includes(transaction.id)) {
      return "skipped" in outcome ? outcome : { skipped: "untagged" };
    }
    if (result.unchanged_ids.includes(transaction.id)) return { draft: baselineDraft(transaction), changes: [] };
    return "skipped" in outcome ? { draft: baselineDraft(transaction), changes: ["changed"] } : outcome;
  }

  async function runPreview() {
    if (!ready || busy || noTargets) return;
    busy = true;
    const previewed = generation;
    try {
      const result = await api.bulk(request(true));
      const rows = await transactionsByIds([
        ...result.affected_ids,
        ...result.unchanged_ids,
        ...result.skipped_rule_ids,
        ...result.skipped_untagged_ids,
      ]);
      if (previewed !== generation) return; // the inputs changed meanwhile
      preview = result;
      previewItems = rows.map((transaction) => ({ transaction, outcome: outcomeOf(transaction, result) }));
    } catch (error) {
      toastError(error, "Preview failed");
    } finally {
      busy = false;
    }
  }

  async function commit() {
    if (!preview || busy || preview.affected_ids.length === 0) return;
    busy = true;
    try {
      const result = await api.bulk(request(false));
      toast(`Changed ${result.affected_ids.length} transactions`, "success");
      oncommitted();
    } catch (error) {
      toastError(error, "Commit failed");
    } finally {
      busy = false;
    }
  }

  /** Counts input changes, so a preview of older inputs is dropped. */
  let generation = 0;

  /** Any change to the inputs invalidates the preview. */
  function edited() {
    generation++;
    preview = null;
  }
</script>

<Modal
  title="Batch apply"
  {onclose}
  width="760px"
  bindings={() => [bind("dialog.commit", () => (preview ? commit() : runPreview()), { description: "Preview, then commit" })]}
>
  <div class="form" oninput={edited} onchange={edited}>
    <span class="field-label">Apply to</span>
    <div class="scopes">
      <label><input type="radio" bind:group={scope} value="selected" /> the {selectedIds.length} selected</label>
      <label>
        <input type="radio" bind:group={scope} value="matching" />
        {unfiltered ? `all ${matching} transactions (no filters set)` : `all ${matching} matching the filters`}
      </label>
    </div>
    <span></span>
    {#if noTargets}
      <p class="scope none">Applies to 0 rows (none selected). Select rows to edit.</p>
    {:else}
      <p class="muted scope">
        Writes on commit. Transactions tagged by rules are skipped.
        {#if scope === "matching"}Every page of the results counts, not only the rows shown.{/if}
      </p>
    {/if}
  </div>
  <div class="fields">
    <BatchFieldsForm bind:fields bind:ready onedited={edited} />
  </div>

  {#if preview}
    <div class="preview">
      {#if isNew}<p><span class="badge warning">new tag</span> {fields.label?.tag} / {fields.label?.subtag} is created on commit.</p>{/if}
      <BatchPreview items={previewItems} />
    </div>
  {/if}

  {#snippet footer()}
    <button onclick={onclose}>Cancel</button>
    {#if preview}
      <button class="primary" onclick={commit} disabled={busy || preview.affected_ids.length === 0}>
        Commit {preview.affected_ids.length} <kbd>Ctrl</kbd>+<kbd>Enter</kbd>
      </button>
    {:else}
      <button class="primary" onclick={runPreview} disabled={busy || noTargets || !ready}>
        Preview <kbd>Ctrl</kbd>+<kbd>Enter</kbd>
      </button>
    {/if}
  {/snippet}
</Modal>

<style>
  .scope {
    margin: 0;
  }
  .scope.none {
    color: var(--warning);
  }
  .scopes {
    display: flex;
    gap: 16px;
  }
  .form {
    display: grid;
    grid-template-columns: 90px 1fr;
    gap: 10px;
    align-items: center;
  }
  .field-label {
    color: var(--muted);
  }
  label {
    color: var(--muted);
    white-space: nowrap;
  }
  .fields {
    margin-top: 12px;
    padding-top: 12px;
    border-top: 1px solid var(--border);
  }
  .preview {
    margin-top: 14px;
    border-top: 1px solid var(--border);
    padding-top: 10px;
  }
  .preview p {
    margin: 0 0 8px;
  }
</style>
