<!--
  The glossary: what every figure, label and concept of the app means, by
  section, with how it is computed. Each entry has its own URL, e.g.
  "#/glossary?kept", which tooltips link to: the page scrolls to it.
-->
<script lang="ts">
  import { tick } from "svelte";
  import { glossary, glossaryHref, loadGlossary, renderMarkdown } from "../lib/glossary.svelte";
  import { router } from "../lib/router.svelte";
  import { bind, useKeys } from "../lib/use-keys.svelte";

  let search = $state("");
  let searchInput: HTMLInputElement;
  const selected = $derived(decodeURIComponent(router.query));

  const matching = $derived.by(() => {
    const needle = search.trim().toLowerCase();
    if (!needle) return glossary.entries;
    return glossary.entries.filter((entry) =>
      [entry.key, entry.title, entry.short, entry.computed, ...entry.labels].some((text) => text.toLowerCase().includes(needle)),
    );
  });
  const sections = $derived.by(() => {
    const bySection = new Map<string, typeof glossary.entries>();
    for (const entry of matching) bySection.set(entry.section, [...(bySection.get(entry.section) ?? []), entry]);
    return [...bySection.entries()];
  });

  // Scroll to the entry of the URL, once it is there
  $effect(() => {
    const key = selected;
    if (!key || !glossary.byKey[key]) return;
    tick().then(() => document.getElementById(`term-${key}`)?.scrollIntoView({ block: "start" }));
  });

  loadGlossary();

  useKeys("Glossary", () => [bind("glossary.search", () => searchInput?.focus())]);
</script>

<div class="page glossary">
  <div class="page-header">
    <h1>Glossary</h1>
    <span class="faint">what every figure and label means, and how it's computed</span>
    <div class="spacer"></div>
    <input bind:this={searchInput} bind:value={search} placeholder="Search  /" class="search" aria-label="Search the glossary" />
  </div>
  {#if glossary.failed}
    <p class="empty">The glossary failed to load.</p>
  {:else if glossary.entries.length === 0}
    <p class="empty">Loading…</p>
  {:else}
    <div class="layout">
      <nav class="sections" aria-label="Sections">
        {#each sections as [section, entries]}
          <a href="#term-{entries[0].key}" onclick={(event) => (event.preventDefault(), document.getElementById(`term-${entries[0].key}`)?.scrollIntoView({ block: "start" }))}>{section}</a>
        {/each}
      </nav>
      <div class="entries">
        {#each sections as [section, entries] (section)}
          <h2>{section}</h2>
          {#each entries as entry (entry.key)}
            <section class="entry" class:selected={entry.key === selected} id="term-{entry.key}">
              <h3>
                <a class="anchor" href={glossaryHref(entry.key)} title="Link to this entry">{entry.title}</a>
                {#each entry.labels as label}<span class="label">{label}</span>{/each}
              </h3>
              <p class="short">{entry.short}</p>
              <div class="computed">{@html renderMarkdown(entry.computed)}</div>
              {#if entry.related?.length}
                <p class="related">
                  <span class="faint">See also</span>
                  {#each entry.related ?? [] as key, index}{index ? ", " : " "}<a href={glossaryHref(key)}>{glossary.byKey[key]?.title ?? key}</a>{/each}
                </p>
              {/if}
            </section>
          {/each}
        {:else}
          <p class="empty">Nothing matches.</p>
        {/each}
      </div>
    </div>
  {/if}
</div>

<style>
  .glossary {
    display: flex;
    flex-direction: column;
    height: 100%;
  }
  .search {
    width: 240px;
  }
  .layout {
    display: grid;
    grid-template-columns: 190px minmax(0, 1fr);
    gap: 24px;
    min-height: 0;
    flex: 1;
  }
  .sections {
    display: flex;
    flex-direction: column;
    gap: 2px;
    overflow: auto;
  }
  .sections a {
    color: var(--muted);
    text-decoration: none;
    padding: 2px 6px;
    border-radius: 4px;
  }
  .sections a:hover {
    color: var(--text);
    background: var(--surface-2);
  }
  .entries {
    overflow: auto;
    max-width: 860px;
    padding-right: 12px;
  }
  h2 {
    margin: 18px 0 8px;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--muted);
  }
  h2:first-child {
    margin-top: 0;
  }
  .entry {
    padding: 10px 12px;
    margin-bottom: 8px;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--surface);
    scroll-margin-top: 8px;
  }
  .entry.selected {
    border-color: var(--accent);
  }
  h3 {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 4px 8px;
    margin: 0 0 4px;
    font-size: 14px;
  }
  .anchor {
    color: var(--text);
    text-decoration: none;
  }
  .anchor:hover {
    text-decoration: underline;
  }
  .label {
    font-size: 12px;
    font-weight: 400;
    color: var(--muted);
    padding: 0 6px;
    border: 1px solid var(--border);
    border-radius: 10px;
  }
  .short {
    margin: 0 0 6px;
  }
  .computed {
    color: var(--muted);
    line-height: 1.5;
  }
  .computed :global(p) {
    margin: 0 0 6px;
  }
  .computed :global(ul) {
    margin: 0 0 6px;
    padding-left: 18px;
  }
  .computed :global(code) {
    font-size: 12px;
    padding: 0 3px;
    border-radius: 3px;
    background: var(--surface-3);
  }
  .computed :global(strong) {
    color: var(--text);
  }
  .related {
    margin: 4px 0 0;
    font-size: 12px;
  }
</style>
