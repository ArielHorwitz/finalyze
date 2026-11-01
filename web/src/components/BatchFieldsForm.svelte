<!--
  The fields of a batch edit, each optional: only checked fields are set, the
  others keep each row's. Editing a field checks it.
-->
<script lang="ts">
  import { INHERIT, type BatchFields } from "../lib/batch";
  import { TAG_KINDS, type TagKind } from "../lib/kinds";
  import LabelInput from "./LabelInput.svelte";
  import Term from "./Term.svelte";
  import DescriptionInput from "./DescriptionInput.svelte";

  let {
    fields = $bindable(),
    ready = $bindable(false),
    onedited,
  }: {
    /** The checked fields (the others null). */
    fields: BatchFields;
    /** At least one field is checked, and a checked tag is given. */
    ready: boolean;
    /** After any change. */
    onedited?: () => void;
  } = $props();

  let withLabel = $state(false);
  let tag = $state("");
  let subtag = $state("");
  let withKind = $state(false);
  let kind = $state<TagKind | typeof INHERIT>("one-off");
  let withDescription = $state(false);
  let description = $state("");
  let withReview = $state(false);
  let review = $state<"set" | "clear">("set");

  const missingTag = $derived(withLabel && !tag.trim());

  $effect(() => {
    fields = {
      label: withLabel ? { tag: tag.trim(), subtag: subtag.trim() } : null,
      kind: withKind ? kind : null,
      description: withDescription ? description : null,
      needsReview: withReview ? review === "set" : null,
    };
    ready = (withLabel || withKind || withDescription || withReview) && !missingTag;
  });

  function edited() {
    onedited?.();
  }
</script>

<div class="fields" oninput={edited} onchange={edited}>
  <label class="include"><input type="checkbox" bind:checked={withLabel} aria-label="Include the tags" /> Tags</label>
  <div class="row" oninput={() => (withLabel = true)}>
    <LabelInput bind:tag bind:subtag autofocus onpicked={() => ((withLabel = true), edited())} />
    {#if missingTag}<span class="warning-text">a tag is needed</span>{/if}
  </div>

  <label class="include">
    <input type="checkbox" bind:checked={withKind} aria-label="Include the kind" /> <Term key="kind_set">Kind</Term>
  </label>
  <select bind:value={kind} aria-label="Kind" onchange={() => (withKind = true)}>
    <option value={INHERIT}>inherit from the tag (clear the kind set)</option>
    {#each TAG_KINDS as item (item.id)}<option value={item.id}>{item.label}</option>{/each}
  </select>

  <label class="include">
    <input type="checkbox" bind:checked={withDescription} aria-label="Include the description" /> <Term key="description">Description</Term>
  </label>
  <DescriptionInput
    bind:value={description}
    oninput={() => (withDescription = true)}
    placeholder="replaces their descriptions (empty clears them; Shift+Enter: a new line)"
    aria-label="Description"
  />

  <label class="include"><input type="checkbox" bind:checked={withReview} aria-label="Include the review flag" /> Review</label>
  <select bind:value={review} aria-label="Review" onchange={() => (withReview = true)}>
    <option value="set">flag for review</option>
    <option value="clear">mark reviewed</option>
  </select>
</div>
<p class="faint hint"><Term key="batch_edit">Only checked fields change</Term>: the others keep each row's. Editing a field checks it.</p>

<style>
  .fields {
    display: grid;
    grid-template-columns: 90px 1fr;
    gap: 10px;
    align-items: center;
  }
  .include {
    color: var(--muted);
    white-space: nowrap;
  }
  .row {
    display: flex;
    gap: 8px;
    align-items: center;
  }
  .warning-text {
    color: var(--warning);
  }
  .hint {
    margin: 10px 0 0;
  }
</style>
