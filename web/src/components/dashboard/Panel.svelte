<!-- A titled box of the dashboard, e.g. around a chart, with optional actions on the right. -->
<script lang="ts">
  import type { Snippet } from "svelte";
  import Term from "../Term.svelte";

  let {
    title,
    term,
    termContext = "",
    subtitle = "",
    subtitleTerm,
    actions,
    children,
    onopen,
  }: {
    title: string;
    /** The title's glossary entry. */
    term: string;
    /** What the panel is of, for the title's tooltip. */
    termContext?: string;
    subtitle?: string;
    /** The subtitle's glossary entry, if it has a figure of its own. */
    subtitleTerm?: string;
    actions?: Snippet;
    children: Snippet;
    /** Makes the title a link, e.g. to the view with more detail. */
    onopen?: () => void;
  } = $props();
</script>

<section class="panel">
  <header>
    {#if onopen}
      <button class="title link-title" onclick={onopen}><Term key={term} context={termContext}>{title}</Term> <span class="arrow">→</span></button>
    {:else}
      <h2 class="title"><Term key={term} context={termContext}>{title}</Term></h2>
    {/if}
    {#if subtitle}<span class="subtitle">{#if subtitleTerm}<Term key={subtitleTerm} quiet>{subtitle}</Term>{:else}{subtitle}{/if}</span>{/if}
    <div class="spacer"></div>
    {#if actions}<div class="actions">{@render actions()}</div>{/if}
  </header>
  {@render children()}
</section>

<style>
  .panel {
    display: flex;
    flex-direction: column;
    gap: 8px;
    min-width: 0;
    padding: 12px 14px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
  }
  header {
    display: flex;
    align-items: center;
    gap: 10px;
    min-height: 24px;
    flex-wrap: wrap;
  }
  .title {
    margin: 0;
    font-size: 13px;
    font-weight: 600;
    color: var(--text);
  }
  .link-title {
    background: none;
    border: none;
    padding: 0;
  }
  .link-title .arrow {
    color: var(--faint);
    transition: transform 0.15s;
    display: inline-block;
  }
  .link-title:hover .arrow {
    color: var(--accent);
    transform: translateX(2px);
  }
  .subtitle {
    color: var(--faint);
    font-size: 12px;
  }
  .actions {
    display: flex;
    align-items: center;
    gap: 8px;
  }
</style>
