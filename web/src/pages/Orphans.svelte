<!--
  Orphans (annotations of transactions that no longer exist) with explicit
  deletion, and overridden annotations (whose tags a rule overrides) for reference.
-->
<script lang="ts">
  import { onMount } from "svelte";
  import { showDate } from "../lib/display.svelte";
  import Amount from "../components/Amount.svelte";
  import DescriptionText from "../components/DescriptionText.svelte";
  import TransactionDescription from "../components/TransactionDescription.svelte";
  import SelectionCount from "../components/SelectionCount.svelte";
  import { api, type StoredAnnotation } from "../lib/api";
  import { Cursor, scrollToIndex } from "../lib/cursor.svelte";
  import { savedSelection, saveSelection } from "../lib/selections";
  import { app, confirm, keyName, refreshStatus, toast, toastError } from "../lib/state.svelte";
  import { bind, useKeys } from "../lib/use-keys.svelte";
  import Term from "../components/Term.svelte";

  let orphans = $state<StoredAnnotation[]>([]);
  let overridden = $state<StoredAnnotation[]>([]);
  // Kept while the app is open, as are the other pages' selections
  let selected = $state<Set<string>>(savedSelection("orphans"));
  $effect(() => saveSelection("orphans", selected));
  let loaded = $state(false);
  const cursor = new Cursor(() => orphans.length);
  let container: HTMLDivElement | undefined = $state();
  $effect(() => scrollToIndex(container, cursor.index));

  async function load() {
    try {
      [orphans, overridden] = await Promise.all([api.orphans(), api.overridden()]);
      const ids = new Set(orphans.map((orphan) => orphan.transaction_id));
      selected = new Set([...selected].filter((id) => ids.has(id)));
      cursor.moveTo(cursor.index);
      loaded = true;
    } catch (error) {
      toastError(error, "Loading orphans");
    }
  }

  function toggle(id: string) {
    const next = new Set(selected);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    selected = next;
  }

  async function remove() {
    const ids = selected.size ? [...selected] : orphans[cursor.index] ? [orphans[cursor.index].transaction_id] : [];
    if (ids.length === 0) return;
    const confirmed = await confirm(
      `Delete ${ids.length} orphaned annotation${ids.length === 1 ? "" : "s"}? Their tags and descriptions are lost.`,
      { title: "Delete orphans", confirmLabel: "Delete", danger: true },
    );
    if (!confirmed) return;
    try {
      const result = await api.deleteOrphans(ids);
      toast(`Deleted ${result.deleted_ids.length} orphans`, "success");
      selected = new Set();
      await load();
      refreshStatus();
    } catch (error) {
      toastError(error);
    }
  }

  useKeys("Orphans", () => [
    ...cursor.bindings(),
    bind("select.toggle", () => orphans[cursor.index] && toggle(orphans[cursor.index].transaction_id), { description: "Select the orphan" }),
    bind("select.all", () => (selected = new Set(orphans.map((orphan) => orphan.transaction_id))), { description: "Select all orphans" }),
    bind("select.clear", () => (selected = new Set())),
    bind("orphans.delete", remove),
  ]);

  onMount(load);

  let seenVersion = app.dataVersion;
  $effect(() => {
    if (app.dataVersion !== seenVersion) {
      seenVersion = app.dataVersion;
      load();
    }
  });
</script>

{#snippet annotationRow(annotation: StoredAnnotation)}
  <td class="muted">{showDate(annotation.snapshot.date)}</td>
  <td class="muted">{annotation.snapshot.account}&nbsp;<span class="faint">· {annotation.snapshot.source}</span></td>
  <td><TransactionDescription description={annotation.description} raw={annotation.snapshot.raw_description} /></td>
  <td class="amount"><Amount value={annotation.snapshot.amount} /></td>
  <td>
    {#each annotation.parts as part}
      <div class="part">
        {part.tag} / {part.subtag}
        {#if annotation.parts.length > 1}<span class="faint">(<Amount value={part.amount} />)</span>{/if}
        {#if part.description}<span class="muted">— <DescriptionText description={part.description} /></span>{/if}
      </div>
    {:else}
      <span class="faint">description only</span>
    {/each}
  </td>
  <td>
    {#if annotation.needs_review}<span class="badge warning"><Term key="needs_review" quiet>review</Term></span>{/if}
    {#if annotation.authors.some((author) => author !== "owner")}
      <span class="badge"><Term key="authors" quiet>{annotation.authors.join(", ")}</Term></span>
    {/if}
  </td>
{/snippet}

<div class="page">
  <div class="page-header">
    <h1><Term key="orphan">Orphans</Term></h1>
    <span class="muted summary">Annotations whose transaction no longer exists in the ingested data. Delete them once you are sure.
      <SelectionCount count={selected.size} onclear={() => (selected = new Set())} /></span>
    <div class="spacer"></div>
    <button class="danger" onclick={remove} disabled={orphans.length === 0} title={keyName("orphans.delete")}>
      Delete {selected.size ? `${selected.size} selected` : "orphan"}
    </button>
  </div>
  <div class="scroll" bind:this={container}>
    <table class="grid">
      <thead>
        <tr>
          <th class="check"></th><th>Date</th><th>Account</th><th><Term key="description" context="Without one, the raw description, in italics">Description</Term></th><th class="right">Amount</th><th>Tags</th><th></th>
        </tr>
      </thead>
      <tbody>
        {#each orphans as orphan, index (orphan.transaction_id)}
          <tr data-index={index} class:active={index === cursor.index} onmousedown={() => cursor.moveTo(index)}>
            <td class="check">
              <input type="checkbox" tabindex="-1" checked={selected.has(orphan.transaction_id)} onchange={() => toggle(orphan.transaction_id)} />
            </td>
            {@render annotationRow(orphan)}
          </tr>
        {/each}
      </tbody>
    </table>
    {#if loaded && orphans.length === 0}<div class="empty">No orphans</div>{/if}

    <h2><Term key="overridden">Overridden annotations</Term> <span class="faint">({overridden.length})</span></h2>
    <p class="muted explain">Annotations of transactions that a rule tags: the rule's tags apply, and only the description is used. Change or delete the rule to use them again.</p>
    <table class="grid">
      <thead>
        <tr><th>Date</th><th>Account</th><th><Term key="description" context="Without one, the raw description, in italics">Description</Term></th><th class="right">Amount</th><th>Annotation</th><th></th><th>Rule</th></tr>
      </thead>
      <tbody>
        {#each overridden as annotation (annotation.transaction_id)}
          <tr>
            {@render annotationRow(annotation)}
            <td class="muted">
              {#if annotation.rule}rule {annotation.rule.position + 1}: {annotation.rule.tag} / {annotation.rule.subtag}{/if}
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
    {#if loaded && overridden.length === 0}<div class="empty">None</div>{/if}
  </div>
</div>

<style>
  .summary {
    font-size: 12px;
  }
  table.grid td {
    padding: 5px 8px;
    vertical-align: top;
  }
  .check {
    width: 30px;
    text-align: center;
  }
  .part + .part {
    margin-top: 2px;
  }
  h2 {
    font-size: 13px;
    font-weight: 600;
    margin: 22px 12px 4px;
  }
  .explain {
    margin: 0 12px 10px;
    font-size: 12px;
  }
</style>
