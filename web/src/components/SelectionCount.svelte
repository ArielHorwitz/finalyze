<!-- "· 3 selected", and a button that clears the selection. -->
<script lang="ts">
  import { titled } from "../lib/state.svelte";
  let { count, onclear }: { count: number; onclear: () => void } = $props();
</script>

{#if count > 0}
  · <span class="selection">{count} selected</span>
  <button class="link clear" onclick={onclear} title={titled("Clear the selection", "select.clear")}>clear</button>
{/if}

<style>
  .clear {
    margin-left: 4px;
    font-size: inherit;
  }
</style>
