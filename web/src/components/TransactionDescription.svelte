<!--
  What a transaction is, read-only: its written description (its first line,
  see DescriptionText), with the raw description on hover. Without a
  description, the raw description stands in, in italics, so its absence shows.
-->
<script lang="ts">
  import { describedTitle } from "../lib/descriptions";
  import DescriptionText from "./DescriptionText.svelte";

  let { description, raw }: { description: string | null | undefined; raw: string } = $props();
</script>

{#if description}
  <span class="transaction-description"><DescriptionText {description} title={describedTitle(description, raw)} /></span>
{:else}
  <span class="transaction-description raw-only" dir="auto" title="Raw description (no description written): {raw}">{raw}</span>
{/if}

<style>
  .transaction-description {
    color: var(--text);
  }
  .raw-only {
    font-style: italic;
    color: var(--muted);
  }
</style>
