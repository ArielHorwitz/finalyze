<!--
  A description shown in a table or list: its first line, a subtle marker when
  it has more, and the whole description on hover (or `title`, if given).
-->
<script lang="ts">
  import { descriptionSummary, moreLinesText } from "../lib/descriptions";

  let { description, title }: { description: string | null | undefined; title?: string } = $props();

  const summary = $derived(descriptionSummary(description ?? ""));
</script>

{#if description}
  <span class="description-text" dir="auto" title={title ?? (summary.more > 0 ? description : undefined)}
    >{summary.first}{#if summary.more > 0}<span class="more-lines">{moreLinesText(summary.more)}</span>{/if}</span
  >
{/if}

<style>
  .more-lines {
    margin-left: 6px;
    padding: 0 4px;
    border-radius: 3px;
    font-size: 10px;
    color: var(--faint);
    border: 1px solid var(--border);
    white-space: nowrap;
  }
</style>
