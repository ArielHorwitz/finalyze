<!--
  The key of an action, as keycaps: its first own key in the keymap (else its
  first fixed one), e.g. Ctrl+Enter. Nothing if it has no key.
-->
<script lang="ts">
  import { keysOf, ownKeys } from "../lib/state.svelte";

  let { action }: { action: string } = $props();

  const key = $derived(ownKeys(action)[0] ?? keysOf(action)[0]);
  // "Ctrl+Enter" as two caps, but "+" as one
  const caps = $derived(key === undefined ? [] : key.split(/\+(?!$)/));
</script>

{#each caps as cap, index}{#if index > 0}+{/if}<kbd>{cap}</kbd>{/each}
