<!--
  Browse and search all transactions, edit any annotation, and tag many
  transactions at once (batch apply, previewed before committing).
-->
<script module lang="ts">
  import type { FilterForm } from "../lib/filters";

  const SORTS = [
    { value: "date", label: "date" },
    { value: "amount", label: "amount" },
    { value: "description", label: "description" },
    { value: "raw_description", label: "raw description" },
    { value: "account", label: "account" },
  ] as const;
  type Sort = (typeof SORTS)[number]["value"];

  /** The view as last left (its selection is kept by Rows), while the app is open. */
  let saved: { filters: FilterForm; sort: Sort; descending: boolean; offset: number } | null = null;
</script>

<script lang="ts">
  import { onMount } from "svelte";
  import BulkApplyDialog from "../components/BulkApplyDialog.svelte";
  import FilterBar from "../components/FilterBar.svelte";
  import SelectionCount from "../components/SelectionCount.svelte";
  import TransactionTable from "../components/TransactionTable.svelte";
  import { api } from "../lib/api";
  import { filtersFromQuery, toQuery } from "../lib/filters";
  import { guardLeaving, router } from "../lib/router.svelte";
  import { Rows, submitRows } from "../lib/rows.svelte";
  import { app, confirm, keyName, refreshStatus, refreshTags, titled, toastError } from "../lib/state.svelte";
  import { bind, useKeys } from "../lib/use-keys.svelte";

  const PAGE_SIZE = 200;

  const rows = new Rows("transactions");
  // The URL may set filters, e.g. "#/transactions?ids=..." from the dashboard,
  // else the view is as it was left
  const restored = router.query ? null : saved;
  let filters = $state(restored?.filters ?? filtersFromQuery(router.query));
  let sort = $state<Sort>(saved?.sort ?? "date");
  let descending = $state(saved?.descending ?? true);
  let offset = $state(restored?.offset ?? 0);
  let total = $state(0);
  let loading = $state(false);
  let bulkOpen = $state(false);

  async function load() {
    loading = true;
    saved = { filters: $state.snapshot(filters), sort, descending, offset };
    try {
      const page = await api.queryTransactions({
        ...toQuery(filters),
        sort,
        descending,
        limit: PAGE_SIZE,
        offset,
      });
      total = page.total;
      rows.load(page.items);
    } catch (error) {
      toastError(error, "Loading transactions");
    } finally {
      loading = false;
    }
  }

  /** Reload from the first page, after the filters or sorting changed. */
  async function refilter() {
    const dirty = rows.dirtyRows.length;
    if (dirty > 0 && !(await confirm(`Discard ${dirty} unsaved edits?`, { confirmLabel: "Discard", danger: true }))) {
      return;
    }
    offset = 0;
    await load();
  }

  async function turnPage(step: number) {
    const next = offset + step * PAGE_SIZE;
    if (next < 0 || next >= total) return;
    const dirty = rows.dirtyRows.length;
    if (dirty > 0 && !(await confirm(`Discard ${dirty} unsaved edits?`, { confirmLabel: "Discard", danger: true }))) {
      return;
    }
    offset = next;
    await load();
    rows.moveTo(0);
  }

  const openBulk = () => (bulkOpen = true);

  useKeys("Transactions", () => [
    bind("edits.submit", () => submitRows(rows, load)),
    bind("edits.submitSelected", () => submitRows(rows, load, { onlySelected: true })),
    bind("transactions.batch", openBulk),
    bind("transactions.nextPage", () => turnPage(1)),
    bind("transactions.previousPage", () => turnPage(-1)),
    bind("transactions.reverse", () => {
      descending = !descending;
      refilter();
    }),
  ]);

  // Ask before leaving the page with unsaved edits
  onMount(() =>
    guardLeaving(async () => {
      const dirty = rows.dirtyRows.length;
      return dirty === 0 || (await confirm(`Discard ${dirty} unsaved edits?`, { confirmLabel: "Discard", danger: true }));
    }),
  );

  onMount(load);

  let seenQuery = router.query;
  $effect(() => {
    if (router.query !== seenQuery) {
      seenQuery = router.query;
      filters = filtersFromQuery(router.query);
      refilter();
    }
  });

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
    <h1>Transactions</h1>
    <span class="muted summary">
      {#if loading}loading…{:else if total === 0}none{:else}
        {offset + 1}–{Math.min(offset + PAGE_SIZE, total)} of {total}
        <SelectionCount count={rows.selectedRows.length} onclear={() => rows.clearSelection()} />
      {/if}
    </span>
    <button onclick={() => turnPage(-1)} disabled={offset === 0} title={titled("Previous page", "transactions.previousPage")}>‹</button>
    <button onclick={() => turnPage(1)} disabled={offset + PAGE_SIZE >= total} title={titled("Next page", "transactions.nextPage")}>›</button>
    <label class="muted sort">
      sort
      <select bind:value={sort} onchange={refilter}>
        {#each SORTS as option}<option value={option.value}>{option.label}</option>{/each}
      </select>
      <button
        onclick={() => {
          descending = !descending;
          refilter();
        }}
        title={titled("Reverse", "transactions.reverse")}>{descending ? "↓" : "↑"}</button
      >
    </label>
    <div class="spacer"></div>
    <button onclick={openBulk} title={keyName("transactions.batch")}>Batch apply</button>
    {#if rows.selectedRows.length > 0}
      <button onclick={() => submitRows(rows, load, { onlySelected: true })} disabled={rows.submitting} title={keyName("edits.submitSelected")}>
        Submit selected
      </button>
    {/if}
    <button class="primary" onclick={() => submitRows(rows, load)} disabled={rows.submitting || rows.dirtyRows.length === 0} title={keyName("edits.submit")}>
      Submit {rows.dirtyRows.length || ""} edit{rows.dirtyRows.length === 1 ? "" : "s"}
    </button>
  </div>
  <FilterBar bind:form={filters} onchange={refilter} />
  <TransactionTable {rows} />
</div>

{#if bulkOpen}
  <BulkApplyDialog
    selectedIds={rows.selectedRows.map((row) => row.transaction.id)}
    filterQuery={toQuery(filters)}
    matching={total}
    onclose={() => (bulkOpen = false)}
    oncommitted={() => {
      bulkOpen = false;
      rows.clearSelection();
      load();
      refreshStatus();
      refreshTags();
    }}
  />
{/if}

<style>
  .summary {
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }
  .sort {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
  }
</style>
