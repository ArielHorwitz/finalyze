<!--
  One transaction in the table, with inputs for its draft annotation: tag,
  subtag, description and the kind set on it (over its tag's), or one sub-row per part
  when split. The description comes first: without one, the raw description
  stands in as its placeholder (in italics). Once written, the raw description
  is on hover, and under it while the row is being edited.
-->
<script lang="ts">
  import { showDate } from "../lib/display.svelte";
  import {
    addPart,
    amountText,
    draftErrors,
    fillRemainder,
    isDirty,
    isOneOff,
    removePart,
    setKind,
    splitState,
    type Draft,
    type PartDraft,
  } from "../lib/drafts";
  import { comboOf } from "../lib/keys";
  import { TAG_KINDS, type TagKind } from "../lib/kinds";
  import { describedTitle, textDirection } from "../lib/descriptions";
  import { formatMinor } from "../lib/money";
  import type { Row, Rows } from "../lib/rows.svelte";
  import { app, keyName, keysOf, labelExists, ownKeys, tagColor, tagKind, titled } from "../lib/state.svelte";
  import { suggestLabels, suggestSubtags } from "../lib/suggest";
  import Amount from "./Amount.svelte";
  import DescriptionInput from "./DescriptionInput.svelte";
  import Autocomplete, { type Suggestion } from "./Autocomplete.svelte";
  import Term from "./Term.svelte";

  let {
    row,
    index,
    rows,
    active,
    selected,
  }: { row: Row; index: number; rows: Rows; active: boolean; selected: boolean } = $props();

  let element: HTMLTableSectionElement;

  const transaction = $derived(row.transaction);
  const draft = $derived(row.draft);
  const dirty = $derived(isDirty(transaction, draft));
  const split = $derived(splitState(transaction, draft));
  const isRule = $derived(transaction.status === "rule");
  // Shown unless the owner is the only author
  const authors = $derived(transaction.annotation?.authors ?? []);
  const shownAuthors = $derived(authors.some((author) => author !== "owner") ? authors.join(", ") : null);
  const problems = $derived(dirty ? draftErrors(transaction, draft) : []);
  // The kinds set on the transaction (over its tags'), as drafted
  const kindsSet = $derived([...new Set(draft.parts.map((part) => part.kind).filter((kind) => kind !== null))]);

  function update(change: (draft: Draft) => Draft) {
    rows.setDraft(row, change(row.draft));
  }

  function updatePart(partIndex: number, patch: Partial<PartDraft>) {
    update((current) => ({
      ...current,
      fromGuess: false,
      parts: current.parts.map((part, other) => (other === partIndex ? { ...part, ...patch } : part)),
    }));
  }

  /** This row's fields, in Tab order: as their columns go. */
  const fields = () => [...(element?.querySelectorAll<HTMLElement>("[data-field]") ?? [])];

  /** Focus a field of this row, e.g. "description" or "part-1-amount". */
  export function focusField(field: string) {
    // After the DOM updates, e.g. when a split part was just added
    queueMicrotask(() => element?.querySelector<HTMLElement>(`[data-field="${field}"]`)?.focus());
  }

  export function focusFirst() {
    fields()[0]?.focus();
  }

  export function addSplitPart() {
    if (isRule) return;
    update((current) => addPart(current));
    focusField(`part-${row.draft.parts.length - 1}-amount`);
  }

  export function scrollIntoView() {
    element?.scrollIntoView({ block: "nearest" });
  }

  function labelSuggestions(query: string): Suggestion[] {
    return suggestLabels(app.tags, query).map((item) => ({
      key: JSON.stringify([item.tag, item.subtag]),
      label: `${item.tag} / ${item.subtag || "(none)"}`,
      detail: String(item.count),
      color: tagColor(item.tag, item.subtag ?? undefined),
    }));
  }

  function subtagSuggestions(tag: string, query: string): Suggestion[] {
    return suggestSubtags(app.tags, tag, query).map((item) => ({
      key: item.subtag ?? "",
      label: item.subtag || "(none)",
      detail: String(item.count),
    }));
  }

  /**
   * After a pick (by Enter or a click), go on to the field after the subtag
   * (the part's kind), as both are set: not by Tab, which moves on itself.
   */
  function moveOnAfterPick(prefix: string, event: KeyboardEvent | MouseEvent) {
    if (event instanceof KeyboardEvent && event.key === "Tab") return;
    queueMicrotask(() => {
      const all = fields();
      all[all.findIndex((field) => field.dataset.field === `${prefix}subtag`) + 1]?.focus();
    });
  }

  /** A part's kind as drafted: set on it, else its tag's. */
  const inheritedKind = (part: PartDraft): TagKind => tagKind(part.tag.trim(), part.subtag.trim());

  function setPartKind(partIndex: number, kind: TagKind | null) {
    updatePart(partIndex, { kind });
  }

  const isNewLabel = (part: PartDraft) =>
    !draft.fromGuess && part.tag.trim() !== "" && !labelExists(part.tag.trim(), part.subtag.trim());
</script>

{#snippet kindSelect(part: PartDraft, partIndex: number, prefix: string)}
  <td class="kind">
    <select
      class="cell"
      class:set={part.kind !== null}
      class:one-off={(part.kind ?? inheritedKind(part)) === "one-off"}
      data-field="{prefix}kind"
      aria-label="Kind"
      title={part.kind === null ? "The tag's kind. Choose one to set it on this transaction" : "Set on this transaction, over the tag's kind"}
      value={part.kind ?? ""}
      onchange={(event) => setPartKind(partIndex, (event.currentTarget.value || null) as TagKind | null)}
    >
      <option value="">{inheritedKind(part)}</option>
      {#each TAG_KINDS as item (item.id)}<option value={item.id}>◆ {item.label}</option>{/each}
    </select>
  </td>
{/snippet}

{#snippet tagInputs(part: PartDraft, partIndex: number, prefix: string)}
  <td class="tag" class:new-label={isNewLabel(part)}>
    <Autocomplete
      value={part.tag}
      inputClass="cell"
      escapeLeaves
      data-field="{prefix}tag"
      placeholder={split.isSplit ? "tag" : "—"}
      title={isNewLabel(part) ? "New tag (created on submit, after confirming)" : undefined}
      suggest={labelSuggestions}
      oninput={(value) => updatePart(partIndex, { tag: value })}
      onpick={(item, event) => {
        const [tag, subtag] = JSON.parse(item.key);
        updatePart(partIndex, { tag, subtag });
        moveOnAfterPick(prefix, event);
      }}
    />
  </td>
  <td class="subtag" class:new-label={isNewLabel(part)}>
    <Autocomplete
      value={part.subtag}
      inputClass="cell"
      escapeLeaves
      data-field="{prefix}subtag"
      suggest={(query) => subtagSuggestions(part.tag, query)}
      oninput={(value) => updatePart(partIndex, { subtag: value })}
      onpick={(item, event) => {
        updatePart(partIndex, { subtag: item.key });
        moveOnAfterPick(prefix, event);
      }}
    />
  </td>
{/snippet}

<tbody
  bind:this={element}
  class:active
  class:selected
  class:dirty
  class:guess={draft.fromGuess}
  class:error={row.error !== null}
  class:review={draft.needsReview}
  data-id={transaction.id}
  onfocusin={(event) => {
    rows.moveTo(index);
    // Typing replaces a field's value, as in a spreadsheet
    if ((event.target instanceof HTMLInputElement || event.target instanceof HTMLTextAreaElement) && event.target.classList.contains("cell")) {
      event.target.select();
    }
  }}
>
  <tr class="main" class:active onmousedown={() => rows.moveTo(index)}>
    <td class="select">
      <input
        type="checkbox"
        tabindex="-1"
        checked={selected}
        onchange={() => rows.toggleSelected(row)}
        aria-label="Select"
      />
    </td>
    <td class="date">{showDate(transaction.date)}</td>
    <td class="account" title="{transaction.account} · {transaction.source}">
      {transaction.account}<span class="faint">&nbsp;· {transaction.source}</span>
    </td>
    <td class="description" class:raw-only={!draft.description}>
      <DescriptionInput
        class="cell"
        data-field="description"
        aria-label="Description"
        placeholder={transaction.raw_description}
        dir={draft.description ? "auto" : textDirection(transaction.raw_description)}
        title={draft.description
          ? describedTitle(draft.description, transaction.raw_description)
          : `Raw description (no description written): ${transaction.raw_description}`}
        value={draft.description}
        oninput={(description) => update((current) => ({ ...current, description }))}
      />
      {#if draft.description}<span class="raw-in-editor" dir="auto">{transaction.raw_description}</span>{/if}
    </td>
    <td class="amount-cell"><Amount value={transaction.amount} /></td>
    {#if isRule && transaction.rule}
      <td class="rule-label" colspan="2">
        <span class="swatch" style:background={tagColor(transaction.rule.tag, transaction.rule.subtag) ?? "transparent"}></span>
        {transaction.rule.tag} / {transaction.rule.subtag}
      </td>
    {:else if split.isSplit}
      <td class="split-label muted" colspan="2">split in {draft.parts.length} parts</td>
    {:else}
      {@render tagInputs(draft.parts[0], 0, "")}
    {/if}
    {#if isRule && transaction.rule}
      <td class="kind faint" title="The rule's tag's kind (a rule's transactions can't have their own)">{transaction.rule.kind}</td>
    {:else if split.isSplit}
      <td class="kind faint">{kindsSet.length ? "◆ by part" : ""}</td>
    {:else}
      {@render kindSelect(draft.parts[0], 0, "")}
    {/if}
    <td class="actions">
      <button
        class="icon flag"
        class:on={draft.needsReview}
        tabindex="-1"
        title={titled(draft.needsReview ? "Needs review" : "Flag for review", "table.review")}
        onclick={() => rows.toggleReview([row])}>⚑</button
      >
      {#if !isRule}
        <button
          class="icon one-off-toggle"
          class:on={isOneOff(draft)}
          tabindex="-1"
          title={titled(isOneOff(draft) ? "Set as a one-off: back to the tag's kind" : "Set as a one-off, over the tag's kind", "table.oneOff")}
          onclick={() => rows.toggleOneOff([row])}>◆</button
        >
        <button class="icon" tabindex="-1" title={titled("Split off a part", "table.split")} onclick={addSplitPart}>⊕</button>
      {/if}
    </td>
    <td class="info">
      <div class="badges">
        {#if draft.fromGuess}
          <span class="badge guess"><Term key="guess" quiet context={keyName("table.accept") ? `Press ${keyName("table.accept")} to accept` : ""}>guess</Term></span>
        {/if}
        {#if isRule && transaction.rule}
          <span class="badge accent"><Term key="rule" quiet context="Tagged by rule #{transaction.rule.position + 1}">rule {transaction.rule.position + 1}</Term></span>
          {#if transaction.annotation?.parts.length}
            <span class="badge warning"><Term key="overridden" quiet>overridden</Term></span>
          {/if}
        {/if}
        {#if kindsSet.length}
          <span class="badge warning kind-set"><Term key="kind_set" quiet context="Set on this transaction">{kindsSet.length === 1 ? kindsSet[0] : "kinds"} set</Term></span>
        {/if}
        {#if shownAuthors}
          <span class="badge"><Term key="authors" quiet context="Annotated by {shownAuthors}">{shownAuthors}</Term></span>
        {/if}
        {#if dirty}
          <span class="badge edited"><Term key="draft" quiet>edited</Term></span>
        {/if}
      </div>
    </td>
  </tr>
  {#if split.isSplit}
    {#each draft.parts as part, partIndex}
      {@const amount = split.amounts[partIndex]}
      <tr class="part">
        <td></td>
        <td class="part-label faint" colspan="2">
          part {partIndex + 1}{#if partIndex === 0 && split.firstTakesRest}<span class="part-hint"> · takes the rest</span>{/if}
        </td>
        <td class="description">
          <DescriptionInput
            class="cell"
            data-field="part-{partIndex}-description"
            aria-label="Part description"
            placeholder="part description"
            value={part.description}
            oninput={(description) => updatePart(partIndex, { description })}
          />
        </td>
        <td class="amount-cell">
          <input
            class="cell amount-input"
            class:invalid={typeof amount === "string"}
            data-field="part-{partIndex}-amount"
            value={part.amount}
            placeholder={typeof amount === "number" ? amountText(amount, split.total) : ""}
            title={typeof amount === "string" ? amount : `Without a sign, the transaction's sign.${ownKeys("amount.fill").length ? ` ${ownKeys("amount.fill").join(" or ")} fills the rest` : ""}`}
            onkeydown={(event) => {
              if (keysOf("amount.fill").includes(comboOf(event))) {
                event.preventDefault();
                update((current) => fillRemainder(transaction, current, partIndex));
              }
            }}
            oninput={(event) => updatePart(partIndex, { amount: event.currentTarget.value })}
          />
        </td>
        {@render tagInputs(part, partIndex, `part-${partIndex}-`)}
        {@render kindSelect(part, partIndex, `part-${partIndex}-`)}
        <td class="actions">
          <button
            class="icon one-off-toggle"
            class:on={part.kind === "one-off"}
            tabindex="-1"
            title={part.kind === "one-off" ? "This part is a one-off: back to the tag's kind" : "Set this part as a one-off"}
            onclick={() => setPartKind(partIndex, part.kind === "one-off" ? null : "one-off")}>◆</button
          >
          <button
            class="icon"
            tabindex="-1"
            title="Remove this part"
            onclick={() => update((current) => removePart(current, partIndex))}>✕</button
          >
        </td>
        <td></td>
      </tr>
    {/each}
    <tr class="part remainder-row">
      <td colspan="4"></td>
      <td class="amount-cell remainder" class:off={split.remainder !== 0 || split.restOverflows}>
        {#if split.amounts.some((amount) => typeof amount === "string")}
          <span class="faint">fix the amounts</span>
        {:else if split.restOverflows}
          over the amount
        {:else if split.remainder === 0}
          <span class="faint">balanced ✓</span>
        {:else}
          {formatMinor(split.remainder, { signed: true })} left
        {/if}
      </td>
      <td colspan="5"></td>
    </tr>
  {/if}
  {#if row.error || problems.length}
    <tr class="message-row">
      <td colspan="4"></td>
      <td colspan="6" class="message" class:error-text={row.error}>
        {row.error ?? problems.join(", ")}
      </td>
    </tr>
  {/if}
</tbody>

<style>
  tbody {
    scroll-margin-top: 32px;
  }
  td {
    padding: 2px 6px;
    border-bottom: 1px solid var(--border);
    vertical-align: middle;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  tr.part td,
  tr.message-row td {
    border-bottom: none;
  }
  tbody > tr:last-child td {
    border-bottom: 1px solid var(--border);
  }
  td + td {
    border-left: 1px solid var(--grid-line);
  }
  tbody:nth-of-type(even) td {
    background: var(--zebra);
  }
  tbody.active td {
    background: var(--surface-2);
  }
  tbody.selected td {
    background: rgba(122, 167, 240, 0.08);
  }
  tbody.selected.active td {
    background: rgba(122, 167, 240, 0.13);
  }
  /* Row state bar on the left: active, edited, error */
  td.select {
    box-shadow: inset 3px 0 0 transparent;
  }
  tbody.dirty td.select {
    box-shadow: inset 3px 0 0 var(--accent);
  }
  tbody.error td.select {
    box-shadow: inset 3px 0 0 var(--danger);
  }
  tbody.active tr.main td.select {
    outline: none;
  }
  tbody.active tr.main td.date {
    color: var(--text);
  }
  .select {
    text-align: center;
  }
  .date {
    color: var(--muted);
    font-variant-numeric: tabular-nums;
  }
  .account {
    color: var(--muted);
  }
  /* A written description's raw one, while the row is being edited */
  .raw-in-editor {
    display: none;
    padding: 0 6px 2px;
    font-size: 11px;
    color: var(--faint);
    overflow: hidden;
    text-overflow: ellipsis;
  }
  tbody:focus-within .raw-in-editor {
    display: block;
  }
  /* Without a description, the raw description stands in, in italics */
  td.raw-only :global(textarea.cell::placeholder) {
    color: var(--muted);
    font-style: italic;
    opacity: 1;
  }
  .amount-cell {
    text-align: right;
  }
  .rule-label {
    color: var(--muted);
    padding-left: 12px;
  }
  .split-label {
    padding-left: 12px;
    font-style: italic;
  }
  .part-label {
    text-align: right;
    font-size: 11px;
  }
  .part-hint {
    font-style: italic;
  }
  .remainder {
    font-variant-numeric: tabular-nums;
    font-size: 12px;
  }
  .remainder.off {
    color: var(--warning);
  }
  .message {
    font-size: 12px;
    color: var(--warning);
    padding-bottom: 5px;
  }
  .message.error-text {
    color: var(--danger);
  }
  .actions {
    text-align: center;
  }
  .badges {
    display: flex;
    gap: 4px;
    align-items: center;
    overflow: hidden;
  }
  .badge.edited {
    background: var(--accent-soft);
    color: var(--accent);
  }
  .icon {
    background: none;
    border: none;
    padding: 0 3px;
    color: var(--faint);
    font-size: 13px;
  }
  .icon:hover {
    color: var(--text);
  }
  .flag.on {
    color: var(--warning);
  }
  .one-off-toggle.on {
    color: var(--warning);
  }
  td.kind {
    font-size: 12px;
  }
  td.kind select.cell {
    width: 100%;
    background: transparent;
    border-color: transparent;
    color: var(--faint);
    padding: 1px 2px;
    font-size: 12px;
  }
  td.kind select.cell:hover {
    border-color: var(--border);
  }
  td.kind select.cell:focus {
    border-color: var(--accent);
    background: var(--surface-3);
  }
  td.kind select.cell.set {
    color: var(--text);
    font-weight: 600;
  }
  td.kind select.cell.one-off {
    color: var(--warning);
  }
  tbody.review tr.main td.date {
    box-shadow: inset 0 -1px 0 transparent;
  }
  /* Inputs blend into the table until hovered or focused */
  td :global(input.cell),
  td :global(textarea.cell) {
    width: 100%;
    background: transparent;
    border-color: transparent;
    padding: 2px 5px;
  }
  td :global(input.cell:hover),
  td :global(textarea.cell:hover) {
    border-color: var(--border);
  }
  td :global(input.cell:focus),
  td :global(textarea.cell:focus) {
    border-color: var(--accent);
    background: var(--surface-3);
  }
  td :global(input.cell::placeholder),
  td :global(textarea.cell::placeholder) {
    color: var(--faint);
  }
  tbody.guess td.tag :global(input.cell),
  tbody.guess td.subtag :global(input.cell) {
    color: var(--guess);
    font-style: italic;
  }
  td.new-label :global(input.cell) {
    text-decoration: underline wavy var(--warning);
    text-underline-offset: 3px;
  }
  .amount-input {
    text-align: right;
    font-variant-numeric: tabular-nums;
  }
  .amount-input.invalid {
    border-color: var(--danger) !important;
  }
</style>
