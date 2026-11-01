<!--
  The annotation queue: all untagged transactions (or those flagged for
  review, or a custom filter), narrowed by filters, with editable drafts,
  submitted together.
-->
<script module lang="ts">
  import { emptyFilters, type FilterForm } from "../lib/filters";

  type Mode = "untagged" | "review" | "custom";

  /** Each queue's filters, as last used while the app is open. */
  const savedFilters: Record<Mode, FilterForm> = { untagged: emptyFilters(), review: emptyFilters(), custom: emptyFilters() };
</script>

<script lang="ts">
  import { onMount, untrack } from "svelte";
  import BatchEditDialog from "../components/BatchEditDialog.svelte";
  import FilterBar from "../components/FilterBar.svelte";
  import KeyHint from "../components/KeyHint.svelte";
  import SelectionCount from "../components/SelectionCount.svelte";
  import TransactionTable from "../components/TransactionTable.svelte";
  import { api, type TransactionListRequest } from "../lib/api";
  import { batchDraft, type BatchFields } from "../lib/batch";
  import { activeFilterCount, filtersFromQuery, filtersToQuery, toQuery } from "../lib/filters";
  import { guardLeaving, router, setQuery } from "../lib/router.svelte";
  import { Rows, submitRows, type Row } from "../lib/rows.svelte";
  import { app, confirm, keyName, titled, toastError } from "../lib/state.svelte";
  import { bind, useKeys } from "../lib/use-keys.svelte";
  import Term from "../components/Term.svelte";

  const MODES: { mode: Mode; label: string }[] = [
    { mode: "untagged", label: "Untagged" },
    { mode: "review", label: "Needs review" },
    { mode: "custom", label: "Custom" },
  ];
  /**
   * The queue a URL query asks for, and its filters if it sets any, e.g.
   * "#/annotate?review&tags=food" ("filter" is the custom queue's old name).
   */
  function parseQuery(query: string): { mode: Mode; filters: FilterForm | null } {
    const params = new URLSearchParams(query);
    const mode = params.has("review") ? "review" : params.has("custom") || params.has("filter") ? "custom" : "untagged";
    const filters = filtersFromQuery(query);
    return { mode, filters: filtersToQuery(filters) ? filters : null };
  }
  const queryOf = (queue: Mode, form: FilterForm) =>
    [queue === "untagged" ? "" : queue, filtersToQuery(form)].filter(Boolean).join("&");
  /** The most rows the custom queue shows. */
  const FILTER_LIMIT = 500;

  /** Each queue keeps its own selection. */
  const viewOf = (queue: Mode) => `annotate:${queue}`;

  // The URL's filters, else the queue's as last used
  const initial = parseQuery(router.query);
  let mode = $state<Mode>(initial.mode);
  const rows = new Rows(viewOf(initial.mode));
  let filters = $state(initial.filters ?? savedFilters[initial.mode]);
  let total = $state(0);
  let loading = $state(false);
  let batchTargets = $state<Row[] | null>(null);

  /** The queue, narrowed by its filters (but for what makes the queue). */
  function request(): TransactionListRequest {
    const base = { ...toQuery(filters), sort: "date" as const, descending: true, offset: 0 };
    if (mode === "untagged") return { ...base, status: "untagged", limit: null };
    if (mode === "review") return { ...base, needs_review: true, limit: null };
    return { ...base, limit: FILTER_LIMIT };
  }

  /** Keep the queue and its filters in the URL, and for the session. */
  function saveFilters() {
    savedFilters[mode] = $state.snapshot(filters);
    setQuery(queryOf(mode, filters));
  }

  /** The latest load: an earlier one's answer (of another queue, or filters) is dropped. */
  let latestLoad = 0;

  async function load() {
    const thisLoad = ++latestLoad;
    loading = true;
    saveFilters();
    try {
      const page = await api.queryTransactions(request());
      if (thisLoad !== latestLoad) return;
      total = page.total;
      rows.load(page.items);
    } catch (error) {
      if (thisLoad === latestLoad) toastError(error, "Loading transactions");
    } finally {
      if (thisLoad === latestLoad) loading = false;
    }
  }

  /** Go to another queue, with its filters as last used, unless a link gives some. */
  async function setMode(next: Mode, linkedFilters: FilterForm | null = null) {
    if (next === mode) return;
    const dirty = rows.dirtyRows.length;
    if (dirty > 0 && !(await confirm(`Discard ${dirty} unsaved edits?`, { confirmLabel: "Discard", danger: true }))) {
      setQuery(queryOf(mode, filters)); // in case a link asked for the queue
      return;
    }
    mode = next;
    filters = linkedFilters ?? savedFilters[next];
    rows.rows = [];
    rows.setView(viewOf(next));
    // Synchronously, so the URL never disagrees with the mode (see the effect below)
    saveFilters();
    await load();
  }

  const submit = (onlySelected = false) => submitRows(rows, load, { onlySelected });

  /** Only ever the selected rows: with none, the dialog says to select some. */
  function openBatch() {
    batchTargets = rows.selectedRows;
  }

  /** Only checked fields change: a batch edit sets the review flag only if
   * asked to (unlike editing a row, which counts as reviewing it). */
  function applyBatch(fields: BatchFields) {
    for (const row of batchTargets ?? []) {
      const outcome = batchDraft(row.transaction, row.draft, fields);
      if ("skipped" in outcome) continue;
      row.draft = outcome.draft;
      row.error = null;
    }
    batchTargets = null;
  }

  useKeys("Annotate", () => [
    bind("edits.submit", () => submit()),
    bind("edits.submitSelected", () => submit(true)),
    bind("annotate.acceptAll", () => rows.acceptGuesses(rows.guessRows)),
    bind("annotate.batch", openBatch),
    bind("annotate.queue", () => setMode(MODES[(MODES.findIndex((item) => item.mode === mode) + 1) % MODES.length].mode)),
  ]);

  // Ask before leaving the page with unsaved edits
  onMount(() =>
    guardLeaving(async () => {
      const dirty = rows.dirtyRows.length;
      return dirty === 0 || (await confirm(`Discard ${dirty} unsaved edits?`, { confirmLabel: "Discard", danger: true }));
    }),
  );

  onMount(() => {
    load(); // which sets the URL, e.g. from an old link's name of the queue
    const warn = (event: BeforeUnloadEvent) => {
      if (rows.dirtyRows.length > 0) event.preventDefault();
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  });

  // Follow links to the queues, e.g. "to review" in the header (the queue's
  // filters stay, unless the link gives some)
  $effect(() => {
    const linked = parseQuery(router.query);
    untrack(() => {
      if (linked.mode !== mode) setMode(linked.mode, linked.filters);
      else if (linked.filters && filtersToQuery(linked.filters) !== filtersToQuery(filters)) {
        filters = linked.filters;
        load();
      } else if (router.query !== queryOf(mode, filters)) setQuery(queryOf(mode, filters));
    });
  });

  // Reload after an ingest
  let seenVersion = app.dataVersion;
  $effect(() => {
    if (app.dataVersion !== seenVersion) {
      seenVersion = app.dataVersion;
      load();
    }
  });
</script>

<div class="page">
  <div class="page-header">
    <h1>Annotate</h1>
    <div class="modes" role="tablist">
      {#each MODES as item}
        <button role="tab" aria-selected={mode === item.mode} class:on={mode === item.mode} onclick={() => setMode(item.mode)}>
          <Term key="queue" quiet>{item.label}</Term>
          {#if item.mode === "untagged" && app.status}<span class="count">{app.status.untagged}</span>{/if}
          {#if item.mode === "review" && app.status}<span class="count">{app.status.needs_review}</span>{/if}
        </button>
      {/each}
    </div>
    <span class="muted summary">
      {#if loading}loading…{:else}
        {rows.rows.length}{total > rows.rows.length ? ` of ${total}` : ""} shown
        {#if rows.guessRows.length}· <span class="guess-count"><Term key="guess">{rows.guessRows.length} guess{rows.guessRows.length === 1 ? "" : "es"}</Term></span>{/if}
        <SelectionCount count={rows.selectedRows.length} onclear={() => rows.clearSelection()} />
      {/if}
    </span>
    <div class="spacer"></div>
    <button onclick={openBatch} title={titled("Batch edit", "annotate.batch")}>Batch edit</button>
    {#if rows.selectedRows.length > 0}
      <button onclick={() => submit(true)} disabled={rows.submitting} title={keyName("edits.submitSelected")}>Submit selected</button>
    {/if}
    <button class="primary" onclick={() => submit()} disabled={rows.submitting || rows.dirtyRows.length === 0} title={keyName("edits.submit")}>
      Submit {rows.dirtyRows.length || ""} edit{rows.dirtyRows.length === 1 ? "" : "s"}
    </button>
  </div>
  <FilterBar bind:form={filters} onchange={load} showStatus={mode !== "untagged"} showReview={mode !== "review"} />
  <TransactionTable {rows}>
    {#snippet empty()}
      {#if loading}Loading…{:else if activeFilterCount(filters) > 0}No transactions in the queue match the filters{:else if mode === "untagged"}Nothing left to tag. <span class="faint">Press <KeyHint action="annotate.queue" /> for other queues.</span>{:else if mode === "review"}Nothing flagged for review. <span class="faint">Press <KeyHint action="annotate.queue" /> for other queues.</span>{:else}No transactions match the filters{/if}
    {/snippet}
  </TransactionTable>
  <div class="legend faint">
    <span><KeyHint action="help.open" /> shortcuts</span>
    <span><kbd>Enter</kbd> edit</span>
    <span><KeyHint action="table.accept" /> accept <Term key="guess">guess</Term></span>
    <span><KeyHint action="table.split" /> split</span>
    <span><KeyHint action="table.review" /> <Term key="needs_review">review flag</Term></span>
    <span><KeyHint action="select.toggle" /> select</span>
    <span><KeyHint action="select.all" /> select all</span>
    <span><KeyHint action="annotate.batch" /> batch edit</span>
    <span><KeyHint action="edits.submit" /> submit</span>
  </div>
</div>

{#if batchTargets}
  <BatchEditDialog rows={batchTargets} onapply={applyBatch} onclose={() => (batchTargets = null)} />
{/if}

<style>
  .modes {
    display: flex;
    background: var(--surface-2);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 2px;
  }
  .modes button {
    border: none;
    background: none;
    color: var(--muted);
    padding: 3px 10px;
    border-radius: 4px;
  }
  .modes button.on {
    background: var(--surface-3);
    color: var(--text);
  }
  .count {
    margin-left: 4px;
    font-size: 11px;
    color: var(--faint);
    font-variant-numeric: tabular-nums;
  }
  .summary {
    font-size: 12px;
  }
  .guess-count {
    color: var(--guess);
  }
  .legend {
    display: flex;
    gap: 16px;
    font-size: 11px;
    flex-wrap: wrap;
  }
</style>
