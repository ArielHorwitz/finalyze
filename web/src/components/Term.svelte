<!--
  A label with a glossary entry: hovering it with the mouse shows the entry's
  definition (and `context`, e.g. which months an average is of), with a link
  to the Glossary page. It isn't focusable, and its empty title keeps the
  title of a button around it from showing over the tooltip. `quiet` leaves
  out the dotted underline, for figures repeated on every row.
-->
<script lang="ts">
  import type { Snippet } from "svelte";

  let { key, context = "", quiet = false, children }: { key: string; context?: string; quiet?: boolean; children: Snippet } = $props();
</script>

<span class="term" class:quiet data-term={key} data-term-context={context || undefined} title="">{@render children()}</span>

<style>
  .term {
    text-decoration: underline dotted;
    text-decoration-color: color-mix(in srgb, currentColor 30%, transparent);
    text-decoration-thickness: 1px;
    text-underline-offset: 3px;
    text-transform: inherit;
  }
  .term:hover {
    text-decoration-color: currentColor;
    cursor: help;
  }
  .term.quiet,
  .term.quiet:hover {
    text-decoration: none;
  }
  :global(button) .term:hover {
    cursor: inherit;
  }
</style>
