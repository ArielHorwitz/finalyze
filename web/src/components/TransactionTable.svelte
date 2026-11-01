<!--
  A keyboard-driven table of transactions with editable draft annotations.
  The page owns the rows (a `Rows`) and the submit and batch shortcuts.
-->
<script lang="ts">
  import type { Snippet } from "svelte";
  import { isTextInput } from "../lib/keys";
  import type { Rows } from "../lib/rows.svelte";
  import { bind, useKeys } from "../lib/use-keys.svelte";
  import Term from "./Term.svelte";
  import TransactionRow from "./TransactionRow.svelte";

  let { rows, empty }: { rows: Rows; empty?: Snippet } = $props();

  let rowComponents: TransactionRow[] = $state([]);
  let container: HTMLDivElement;

  // Keep the cursor row in view
  $effect(() => {
    const cursor = rows.cursor;
    if (!container?.contains(document.activeElement) || !isTextInput(document.activeElement)) {
      rowComponents[cursor]?.scrollIntoView();
    }
  });

  /** Leave an input, back to moving between rows. */
  function leaveInput() {
    (document.activeElement as HTMLElement | null)?.blur();
  }

  useKeys("Table", () => {
    const targets = () => rows.targets();
    return [
      bind("list.down", () => rows.move(1)),
      bind("list.up", () => rows.move(-1)),
      bind("list.pageDown", () => rows.move(15)),
      bind("list.pageUp", () => rows.move(-15)),
      bind("list.first", () => rows.moveTo(0)),
      bind("list.last", () => rows.moveTo(rows.rows.length - 1)),
      bind("table.edit", () => rowComponents[rows.cursor]?.focusFirst()),
      // Never clears the selection: that is too easy to lose (see select.clear)
      bind("table.leave", () => {
        if (isTextInput(document.activeElement)) leaveInput();
      }),
      bind("table.accept", () => rows.acceptGuesses(targets())),
      bind("table.review", () => rows.toggleReview(targets())),
      bind("table.split", () => rowComponents[rows.cursor]?.addSplitPart()),
      bind("table.oneOff", () => rows.toggleOneOff(targets())),
      bind("select.toggle", () => rows.current && rows.toggleSelected(rows.current)),
      bind("table.selectDown", () => {
        if (rows.current) rows.toggleSelected(rows.current);
        rows.move(1);
      }),
      bind("select.all", () => rows.selectAll()),
      bind("select.clear", () => rows.clearSelection()),
      bind("table.undo", () => rows.revert(targets())),
      bind("table.clear", () => rows.clear(targets())),
    ];
  });
</script>

<!-- Tab goes through the fields as their columns go (the page's order) -->
<div class="scroll" bind:this={container}>
  <table class="transactions">
    <colgroup>
      <col style:width="30px" />
      <col style:width="88px" />
      <col style:width="118px" />
      <col />
      <col style:width="108px" />
      <col style:width="128px" />
      <col style:width="128px" />
      <col style:width="92px" />
      <col style:width="66px" />
      <col style:width="150px" />
    </colgroup>
    <thead>
      <tr>
        <th>
          <input
            type="checkbox"
            tabindex="-1"
            aria-label="Select all"
            checked={rows.rows.length > 0 && rows.selectedRows.length === rows.rows.length}
            onchange={(event) => (event.currentTarget.checked ? rows.selectAll() : rows.clearSelection())}
          />
        </th>
        <th>Date</th>
        <th>Account</th>
        <th><Term key="description" context="Without one, the raw description stands in, in italics. Once written, the raw description is on hover">Description</Term></th>
        <th class="right">Amount</th>
        <th>Tag</th>
        <th>Subtag</th>
        <th><Term key="kind_set">Kind</Term></th>
        <th></th>
        <th></th>
      </tr>
    </thead>
    {#each rows.rows as row, index (row.transaction.id)}
      <TransactionRow
        bind:this={rowComponents[index]}
        {row}
        {index}
        {rows}
        active={index === rows.cursor}
        selected={rows.selectedIds.has(row.transaction.id)}
      />
    {/each}
  </table>
  {#if rows.rows.length === 0}
    <div class="empty">
      {#if empty}{@render empty()}{:else}No transactions{/if}
    </div>
  {/if}
</div>

<style>
  table {
    width: 100%;
    table-layout: fixed;
    border-collapse: separate;
    border-spacing: 0;
  }
  th {
    position: sticky;
    top: 0;
    z-index: 2;
    background: var(--surface-2);
    color: var(--muted);
    font-weight: 500;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    text-align: left;
    padding: 6px;
    border-bottom: 1px solid var(--border);
  }
  th.right {
    text-align: right;
  }
  th + th {
    border-left: 1px solid var(--grid-line);
  }
</style>
