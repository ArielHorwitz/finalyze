<!--
  The tooltip of the label under the mouse (see `lib/glossary.svelte.ts`): its
  glossary entry's short definition, what the figure is of, and a link to the
  full entry. Mouse only: nothing in it is focusable.
-->
<script lang="ts">
  import { onMount } from "svelte";
  import { followTerms, glossary, glossaryHref, hideTerm, hideTermSoon, keepTerm, termTooltip } from "../lib/glossary.svelte";

  let element = $state<HTMLElement | undefined>();
  let position = $state({ left: 0, top: 0 });
  const entry = $derived(termTooltip.key ? glossary.byKey[termTooltip.key] : undefined);

  onMount(() => followTerms(document));

  // Below the label, or above it without room, within the window
  $effect(() => {
    const anchor = termTooltip.anchor;
    if (!element || !anchor || !entry) return;
    const { width, height } = element.getBoundingClientRect();
    const margin = 8;
    const below = anchor.bottom + 6;
    const top = below + height + margin > window.innerHeight ? Math.max(margin, anchor.top - 6 - height) : below;
    const left = Math.min(Math.max(margin, anchor.left), window.innerWidth - width - margin);
    position = { left, top };
  });
</script>

{#if entry}
  <div
    class="term-tooltip"
    role="tooltip"
    bind:this={element}
    style:left="{position.left}px"
    style:top="{position.top}px"
    onpointerenter={keepTerm}
    onpointerleave={hideTermSoon}
  >
    <p class="short">{entry.short}</p>
    {#if termTooltip.context}<p class="context">{termTooltip.context}</p>{/if}
    <a href={glossaryHref(entry.key)} tabindex="-1" onclick={hideTerm}>How it's computed →</a>
  </div>
{/if}

<style>
  .term-tooltip {
    position: fixed;
    z-index: 300;
    max-width: 340px;
    padding: 8px 10px;
    background: var(--surface-3);
    border: 1px solid var(--border-strong);
    border-radius: 6px;
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.45);
    font-size: 12px;
    line-height: 1.45;
    color: var(--text);
    text-transform: none;
    letter-spacing: normal;
    font-weight: 400;
    white-space: normal;
    text-align: left;
  }
  p {
    margin: 0 0 4px;
  }
  .context {
    color: var(--muted);
    white-space: pre-line;
    font-variant-numeric: tabular-nums;
  }
  a {
    font-size: 11px;
    color: var(--accent);
  }
</style>
