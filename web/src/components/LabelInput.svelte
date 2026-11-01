<!--
  Tag and subtag inputs with suggestions. Typing part of either name (or
  "tag/subtag") in the tag input suggests pairs, and picking one fills both.
-->
<script lang="ts">
  import { app, labelExists, tagColor } from "../lib/state.svelte";
  import { suggestLabels, suggestSubtags } from "../lib/suggest";
  import Autocomplete from "./Autocomplete.svelte";

  let {
    tag = $bindable(""),
    subtag = $bindable(""),
    onpicked,
    autofocus = false,
  }: { tag: string; subtag: string; onpicked?: () => void; autofocus?: boolean } = $props();

  const isNew = $derived(tag.trim() !== "" && !labelExists(tag.trim(), subtag.trim()));
</script>

<div class="label-input">
  <Autocomplete
    bind:value={tag}
    placeholder="tag"
    data-autofocus={autofocus ? "" : undefined}
    suggest={(query) =>
      suggestLabels(app.tags, query).map((item) => ({
        key: JSON.stringify([item.tag, item.subtag]),
        label: `${item.tag} / ${item.subtag || "(none)"}`,
        detail: String(item.count),
        color: tagColor(item.tag, item.subtag ?? undefined),
      }))}
    onpick={(item) => {
      [tag, subtag] = JSON.parse(item.key);
      onpicked?.();
    }}
  />
  <span class="slash">/</span>
  <Autocomplete
    bind:value={subtag}
    placeholder="subtag"
    suggest={(query) =>
      suggestSubtags(app.tags, tag, query).map((item) => ({
        key: item.subtag ?? "",
        label: item.subtag || "(none)",
        detail: String(item.count),
      }))}
    onpick={(item) => {
      subtag = item.key;
      onpicked?.();
    }}
  />
  {#if isNew}<span class="badge warning" title="Created after confirming">new</span>{/if}
</div>

<style>
  .label-input {
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .label-input :global(input) {
    width: 150px;
  }
  .slash {
    color: var(--faint);
  }
</style>
