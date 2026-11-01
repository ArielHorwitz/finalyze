<!--
  The ledger of the period, to the latest data: every analysis row (one per
  split part), with running balances, every group included. Search, sort, choose
  columns, and click a row to open it in Transactions. Rows of a day are in the
  order balances accumulate, so the balances read as running balances.
-->
<script lang="ts">
  import { showDate } from "../../lib/display.svelte";
  import { untrack } from "svelte";
  import { api, type AnalysisRow } from "../../lib/api";
  import { stateOptions, transactionsQuery, type ViewProps } from "../../lib/dashboard";
  import { formatAmount } from "../../lib/money";
  import { rowDescription } from "../../lib/descriptions";
  import DescriptionText from "../DescriptionText.svelte";
  import TransactionDescription from "../TransactionDescription.svelte";
  import { navigate } from "../../lib/router.svelte";
  import { app, toastError } from "../../lib/state.svelte";
  import { bind, useKeys } from "../../lib/use-keys.svelte";
  import Panel from "./Panel.svelte";
  import Term from "../Term.svelte";

  let { settings }: ViewProps = $props();

  // "what": the description (the part's, else the transaction's), else the raw
  // description, as TransactionDescription shows it
  type Column = keyof AnalysisRow | "what";
  // With their glossary entries
  const COLUMNS: { key: Column; label: string; term: string; numeric?: boolean }[] = [
    { key: "date", label: "Date", term: "ledger" },
    { key: "account", label: "Account", term: "account" },
    { key: "source", label: "Source", term: "account" },
    { key: "amount", label: "Amount", term: "ledger_sum", numeric: true },
    { key: "tag", label: "Tag", term: "tag" },
    { key: "subtag", label: "Subtag", term: "tag" },
    { key: "what", label: "Description", term: "description" },
    { key: "raw_description", label: "Raw description", term: "description" },
    { key: "description", label: "Part description", term: "description" },
    { key: "transaction_description", label: "Transaction description", term: "description" },
    { key: "kind", label: "Kind", term: "ledger_kind_group_role" },
    { key: "group", label: "Group", term: "ledger_kind_group_role" },
    { key: "group_role", label: "Role", term: "ledger_kind_group_role" },
    { key: "balance_total", label: "Balance (net worth)", term: "ledger_balances", numeric: true },
    { key: "balance_role", label: "Balance (role)", term: "ledger_balances", numeric: true },
    { key: "balance_group", label: "Balance (group)", term: "ledger_balances", numeric: true },
    { key: "balance_account", label: "Balance (account)", term: "ledger_balances", numeric: true },
    { key: "balance_source", label: "Balance (source)", term: "ledger_balances", numeric: true },
    { key: "id", label: "ID", term: "ledger" },
  ];
  const DEFAULT_COLUMNS: Column[] = ["date", "account", "source", "amount", "tag", "subtag", "what", "balance_total", "balance_source"];
  const STEP = 300;

  let rows = $state<AnalysisRow[]>([]);
  let loading = $state(true);
  let search = $state("");
  let sortKey = $state<Column>("date");
  let descending = $state(true);
  let shown = $state<Column[]>(DEFAULT_COLUMNS);
  let limit = $state(STEP);
  let choosing = $state(false);
  let searchInput: HTMLInputElement;

  // Reload when the options change, or the data does (e.g. after an ingest)
  const optionsKey = $derived(JSON.stringify(stateOptions(settings)));
  $effect(() => {
    optionsKey;
    app.dataVersion;
    untrack(loadRows);
  });

  let requestNumber = 0;
  async function loadRows() {
    const number = ++requestNumber;
    loading = true;
    try {
      const result = await api.dashboardRows(stateOptions(settings), { all_groups: true });
      if (number !== requestNumber) return; // a newer request is on its way
      rows = result.rows;
      limit = STEP;
    } catch (error) {
      if (number === requestNumber) toastError(error, "Ledger");
    } finally {
      if (number === requestNumber) loading = false;
    }
  }

  const columns = $derived(COLUMNS.filter((column) => shown.includes(column.key)));
  const filtered = $derived.by(() => {
    const needle = search.trim().toLowerCase();
    const matching = needle
      ? rows.filter((row) =>
          [row.raw_description, row.tag, row.subtag, row.description, row.transaction_description, row.account, row.source]
            .some((text) => text?.toLowerCase().includes(needle)),
        )
      : rows;
    const sign = descending ? -1 : 1;
    // Ties keep the server's order (latest first, in the reverse of the order
    // balances accumulate), reversed when ascending
    const position = new Map(rows.map((row, index) => [row, index]));
    return [...matching].sort((first, second) => {
      const a = value(first, sortKey);
      const b = value(second, sortKey);
      if (a === b) return (position.get(first)! - position.get(second)!) * -sign;
      if (a === null) return 1;
      if (b === null) return -1;
      return (a < b ? -1 : 1) * sign;
    });
  });
  const total = $derived(filtered.reduce((sum, row) => sum + row.amount, 0));

  function sortBy(key: Column) {
    if (sortKey === key) descending = !descending;
    else {
      sortKey = key;
      descending = key === "date" || key === "amount";
    }
  }

  /** A column's value, to sort by and show. */
  function value(row: AnalysisRow, key: Column) {
    return key === "what" ? (rowDescription(row) ?? row.raw_description).toLowerCase() : row[key];
  }

  function format(row: AnalysisRow, column: (typeof COLUMNS)[number]): string {
    const value = column.key === "what" ? (rowDescription(row) ?? row.raw_description) : row[column.key];
    if (column.key === "tag" && value === null) return "untagged";
    // ◆: set on the transaction, over its tag's
    if (column.key === "kind" && row.kind_set) return `◆ ${value}`;
    if (value === null) return "";
    if (typeof value === "boolean") return value ? "yes" : "";
    if (column.numeric) return formatAmount(value as number);
    if (column.key === "date") return showDate(String(value));
    return String(value);
  }

  function open(row: AnalysisRow) {
    if (row.id) navigate("transactions", transactionsQuery([row.id]));
  }

  function toggleColumn(key: Column) {
    shown = shown.includes(key) ? shown.filter((other) => other !== key) : COLUMNS.map((column) => column.key).filter((other) => other === key || shown.includes(other));
  }

  useKeys("Ledger", () => [bind("ledger.search", () => searchInput?.focus())]);
</script>

<Panel title="Ledger" term="ledger" subtitleTerm="ledger_sum" subtitle={loading ? "loading…" : `${filtered.length} of ${rows.length} rows · sum of the rows shown ${formatAmount(total, { signed: true })}${filtered.length === rows.length ? " (the change in net worth)" : ""}`}>
  {#snippet actions()}
    <input bind:this={searchInput} bind:value={search} placeholder="Search  /" dir="auto" class="search" />
    <div class="chooser">
      <button onclick={() => (choosing = !choosing)} aria-expanded={choosing}>Columns ▾</button>
      {#if choosing}
        <div class="popover">
          {#each COLUMNS as column}
            <label><input type="checkbox" checked={shown.includes(column.key)} onchange={() => toggleColumn(column.key)} /> <Term key={column.term}>{column.label}</Term></label>
          {/each}
        </div>
      {/if}
    </div>
  {/snippet}
  <div class="table">
    <table class="grid data">
      <thead>
        <tr>
          {#each columns as column}
            <th class:amount={column.numeric}>
              <button class="sort" onclick={() => sortBy(column.key)}>
                <Term key={column.term}>{column.label}</Term>{#if sortKey === column.key}<span class="arrow">{descending ? " ↓" : " ↑"}</span>{/if}
              </button>
            </th>
          {/each}
        </tr>
      </thead>
      <tbody>
        {#each filtered.slice(0, limit) as row, index (`${row.id}-${index}`)}
          <tr onclick={() => open(row)} class:holding={row.group_role === "holding"} title="Open in Transactions">
            {#each columns as column, columnIndex}
              <td
                class:amount={column.numeric}
                class:income={column.key === "amount" && row.amount > 0}
                class:expense={column.key === "amount" && row.amount < 0}
                class:untagged={column.key === "tag" && row.tag === null}
                dir={column.key === "raw_description" ? "auto" : undefined}
              >
                {#if columnIndex === 0}
                  <!-- Focusable, to open rows by keyboard -->
                  <button class="open" onclick={(event) => (event.stopPropagation(), open(row))}>{format(row, column)}</button>
                {:else if column.key === "what"}
                  <TransactionDescription description={rowDescription(row)} raw={row.raw_description} />
                {:else if column.key === "description" || column.key === "transaction_description"}
                  <DescriptionText description={row[column.key]} />
                {:else}
                  {format(row, column)}
                {/if}
              </td>
            {/each}
          </tr>
        {/each}
      </tbody>
    </table>
    {#if filtered.length > limit}
      <div class="more">
        <button onclick={() => (limit += STEP)}>Show {Math.min(STEP, filtered.length - limit)} more</button>
        <button onclick={() => (limit = filtered.length)}>Show all {filtered.length}</button>
      </div>
    {/if}
  </div>
</Panel>

<svelte:window
  onmousedown={(event) => {
    if (choosing && !(event.target as HTMLElement).closest(".chooser")) choosing = false;
  }}
/>

<style>
  .search {
    width: 240px;
  }
  .chooser {
    position: relative;
  }
  .popover {
    position: absolute;
    z-index: 30;
    right: 0;
    top: calc(100% + 4px);
    display: flex;
    flex-direction: column;
    gap: 4px;
    padding: 10px 12px;
    width: 200px;
    background: var(--surface-2);
    border: 1px solid var(--border-strong);
    border-radius: 6px;
    box-shadow: 0 10px 28px rgba(0, 0, 0, 0.5);
  }
  .popover label {
    display: flex;
    gap: 6px;
    align-items: center;
  }
  .table {
    max-height: calc(100vh - 330px);
    min-height: 300px;
    overflow: auto;
  }
  .data td {
    padding: 3px 8px;
    white-space: nowrap;
    max-width: 340px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .data tr {
    cursor: pointer;
  }
  .open {
    background: none;
    border: none;
    padding: 0;
    font: inherit;
    color: inherit;
  }
  .open:focus-visible {
    text-decoration: underline;
  }
  .data tr.holding td {
    color: var(--muted);
  }
  .sort {
    background: none;
    border: none;
    padding: 0;
    color: inherit;
    font: inherit;
    text-transform: inherit;
    letter-spacing: inherit;
  }
  .arrow {
    color: var(--accent);
  }
  .untagged {
    color: var(--warning) !important;
    font-style: italic;
  }
  .more {
    display: flex;
    gap: 8px;
    justify-content: center;
    padding: 10px;
  }
</style>
