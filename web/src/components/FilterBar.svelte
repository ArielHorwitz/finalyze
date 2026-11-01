<!--
  Filters for transaction queries. `onchange` is called (debounced) whenever
  the filters change. All of them are always shown: the search, then a group
  of each kind of filter (dates and amounts, tags and kinds, accounts and
  sources, status and review), the most used first. `/` focuses the search
  and `Tab` moves on through the groups.
-->
<script lang="ts">
  import DateInput from "./DateInput.svelte";
  import Term from "./Term.svelte";
  import { activeFilterCount, emptyFilters, FILTER_GROUPS, filterErrors, groupFilterCount, type FilterForm } from "../lib/filters";
  import { TAG_KINDS, type TagKind } from "../lib/kinds";
  import { app } from "../lib/state.svelte";
  import { suggestNames } from "../lib/suggest";
  import { bind, useKeys } from "../lib/use-keys.svelte";
  import Autocomplete, { type Suggestion } from "./Autocomplete.svelte";

  let {
    form = $bindable(),
    onchange,
    showStatus = true,
    showReview = true,
  }: { form: FilterForm; onchange: () => void; showStatus?: boolean; showReview?: boolean } = $props();

  let searchInput: HTMLInputElement;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const errors = $derived(filterErrors(form));
  const count = $derived(activeFilterCount(form));

  function changed() {
    clearTimeout(timer);
    timer = setTimeout(() => {
      if (filterErrors(form).length === 0) onchange();
    }, 250);
  }

  function clear() {
    form = emptyFilters();
    changed();
  }

  function toggleAccount(name: string) {
    form.accounts = form.accounts.includes(name)
      ? form.accounts.filter((account) => account !== name)
      : [...form.accounts, name];
    changed();
  }

  function toggleKind(kind: TagKind) {
    form.kinds = form.kinds.includes(kind) ? form.kinds.filter((other) => other !== kind) : [...form.kinds, kind];
    changed();
  }

  /** Suggestions for the last item of a comma-separated list. */
  function listSuggestions(names: string[], text: string): Suggestion[] {
    const last = text.split(",").at(-1) ?? "";
    return suggestNames(names, last).map((name) => ({ key: name, label: name }));
  }

  function pickIntoList(text: string, name: string): string {
    const items = text.split(",").slice(0, -1).map((item) => item.trim()).filter(Boolean);
    return [...items, name].join(", ");
  }

  const tagNames = $derived(app.tags.map((tag) => tag.name));
  const subtagNames = $derived([...new Set(app.tags.flatMap((tag) => tag.subtags.map((subtag) => subtag.name)))]);

  useKeys("Filters", () => [
    bind("filters.search", () => searchInput?.focus()),
  ]);
</script>

<div class="filters control-panel">
  <div class="line">
    <input
      bind:this={searchInput}
      bind:value={form.search}
      oninput={changed}
      class="search"
      dir="auto"
      placeholder="Search descriptions (written and raw)  /"
      aria-label="Search descriptions"
    />
    {#if form.ids.length}
      <span class="ids badge accent">
        Only {form.ids.length} chosen transaction{form.ids.length === 1 ? "" : "s"}
        <button class="link" onclick={() => { form.ids = []; changed(); }} title="Show all transactions" aria-label="Clear the chosen transactions">✕</button>
      </span>
    {/if}
    {#if count > 0}
      <button class="clear" onclick={clear} title="Clear all filters">Clear {count}</button>
    {/if}
  </div>
  <div class="groups">
    {#snippet legend(group: (typeof FILTER_GROUPS)[number])}
      {@const set = groupFilterCount(form, group.fields)}
      <legend class:active={set > 0}><Term key="filter_groups" context={set ? `${set} set in ${group.label.toLowerCase()}` : ""}>{group.label}</Term>{#if set}&nbsp;•{/if}</legend>
    {/snippet}
    <fieldset class="control-group">
      {@render legend(FILTER_GROUPS[0])}
      <label>From <DateInput bind:value={form.startDate} onchange={changed} aria-label="From" /></label>
      <label title="Exclusive">until <DateInput bind:value={form.endDate} onchange={changed} aria-label="until" /></label>
      <label
        >Amount <input class="short" bind:value={form.minAmount} oninput={changed} placeholder="min" aria-label="Minimum amount" /> –
        <input class="short" bind:value={form.maxAmount} oninput={changed} placeholder="max" aria-label="Maximum amount" /></label
      >
    </fieldset>
    <fieldset class="control-group">
      {@render legend(FILTER_GROUPS[1])}
      <label
        >Tags <Autocomplete
          bind:value={form.tags}
          inputClass="list"
          placeholder="food, car"
          suggest={(text) => listSuggestions(tagNames, text)}
          onpick={(item) => {
            form.tags = pickIntoList(form.tags, item.label);
            changed();
          }}
          oninput={changed}
        /></label
      >
      <label
        >Subtags <Autocomplete
          bind:value={form.subtags}
          inputClass="list"
          suggest={(text) => listSuggestions(subtagNames, text)}
          onpick={(item) => {
            form.subtags = pickIntoList(form.subtags, item.label);
            changed();
          }}
          oninput={changed}
        /></label
      >
      <div class="chips" role="group" aria-label="Kinds">
        {#each TAG_KINDS as kind}
          <button class="chip" class:on={form.kinds.includes(kind.id)} aria-pressed={form.kinds.includes(kind.id)} onclick={() => toggleKind(kind.id)} title="Kind: {kind.description}">
            {kind.label}
          </button>
        {/each}
      </div>
    </fieldset>
    <fieldset class="control-group">
      {@render legend(FILTER_GROUPS[2])}
      <div class="chips" role="group" aria-label="Accounts">
        {#each app.accounts as account}
          <button
            class="chip"
            class:on={form.accounts.includes(account.name)}
            aria-pressed={form.accounts.includes(account.name)}
            onclick={() => toggleAccount(account.name)}
            title="Filter by account"
          >
            {#if account.color}<span class="swatch" style:background={account.color}></span>{/if}
            {account.name}
          </button>
        {/each}
      </div>
      <label>Sources <input class="list" bind:value={form.sources} oninput={changed} placeholder="card, checking" /></label>
    </fieldset>
    <fieldset class="control-group">
      {@render legend(FILTER_GROUPS[3])}
      {#if showStatus}
        <select bind:value={form.status} onchange={changed} title="Status" aria-label="Status">
          <option value="">any status</option>
          <option value="untagged">untagged</option>
          <option value="tagged">tagged</option>
          <option value="annotated">annotated</option>
          <option value="rule">rule-tagged</option>
        </select>
      {/if}
      {#if showReview}
        <select bind:value={form.needsReview} onchange={changed} title="Needs review" aria-label="Needs review">
          <option value="">any review</option>
          <option value="yes">needs review</option>
          <option value="no">not flagged</option>
        </select>
      {/if}
      <label>Authors <input class="list" bind:value={form.authors} oninput={changed} placeholder="owner, claude" /></label>
    </fieldset>
  </div>
  {#if errors.length}
    <div class="errors">{errors.join(", ")}</div>
  {/if}
</div>

<style>
  .filters {
    container: filters / inline-size;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .line {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 4px 10px;
  }
  .search {
    flex: 0 1 420px;
    min-width: 200px;
  }
  .groups {
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    gap: 4px 16px;
  }
  /* Two columns where tags and kinds fit on one line in the second, which
     takes what they need (the accounts in the first can wrap): about 625px
     for dates and amounts, 16px apart, and 815px for tags and kinds. Else a
     line for each group. */
  @container filters (min-width: 1460px) {
    .groups {
      grid-template-columns: minmax(0, 1fr) max-content;
    }
  }
  label {
    display: flex;
    align-items: center;
    gap: 5px;
    color: var(--muted);
  }
  .short {
    width: 70px;
  }
  label :global(.list) {
    width: 110px;
  }
  .chips {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
  }
  .chip {
    border-radius: 12px;
    padding: 2px 8px;
    color: var(--muted);
    border-color: var(--border);
  }
  .chip.on {
    color: var(--text);
    border-color: var(--accent);
    background: var(--accent-soft);
  }
  .clear {
    color: var(--muted);
  }
  .ids {
    display: inline-flex;
    gap: 8px;
    align-items: center;
    padding: 2px 10px;
    font-size: 12px;
  }
  .errors {
    color: var(--danger);
    font-size: 12px;
  }
</style>
