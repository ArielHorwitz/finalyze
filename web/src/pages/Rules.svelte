<!--
  Rules in order (the first matching rule tags a transaction): create,
  edit with a live preview of what changes, delete, and reorder.
-->
<script lang="ts">
  import { onMount } from "svelte";
  import Amount from "../components/Amount.svelte";
  import DateInput from "../components/DateInput.svelte";
  import KeyHint from "../components/KeyHint.svelte";
  import LabelInput from "../components/LabelInput.svelte";
  import Modal from "../components/Modal.svelte";
  import TransactionDescription from "../components/TransactionDescription.svelte";
  import { showDate } from "../lib/display.svelte";
  import { api, transactionsByIds, type Filters, type Rule, type RulePreview, type Transaction } from "../lib/api";
  import { Cursor, scrollToIndex } from "../lib/cursor.svelte";
  import { writtenDescription } from "../lib/descriptions";
  import {
    compactFilters,
    describeFilters,
    fromSimple,
    isSimple,
    moved,
    parseFiltersJson,
    toSimple,
    type SimpleFilters,
  } from "../lib/rules";
  import { app, confirm, keyName, labelExists, refreshStatus, refreshTags, tagColor, toast, toastError } from "../lib/state.svelte";
  import { bind, useKeys } from "../lib/use-keys.svelte";
  import Term from "../components/Term.svelte";

  let rules = $state<Rule[]>([]);
  const cursor = new Cursor(() => rules.length);
  let container: HTMLDivElement | undefined = $state();
  $effect(() => scrollToIndex(container, cursor.index));

  async function load() {
    try {
      rules = await api.rules();
      cursor.moveTo(cursor.index);
    } catch (error) {
      toastError(error, "Loading rules");
    }
  }

  // The editor: a new rule (ruleId null) or a change to one
  interface Editor {
    ruleId: number | null;
    tag: string;
    subtag: string;
    position: number;
    mode: "simple" | "json";
    simple: SimpleFilters;
    json: string;
  }
  let editor = $state<Editor | null>(null);
  let preview = $state<RulePreview | null>(null);
  let previewError = $state<string | null>(null);
  // The simple editor's dates typed that aren't dates (their fields keep the last date)
  let startDateInvalid = $state(false);
  let endDateInvalid = $state(false);
  type PreviewTab = "newly_tagged_ids" | "overridden_annotation_ids" | "effective_ids" | "matched_ids";
  let previewTab = $state<PreviewTab>("newly_tagged_ids");
  let previewRows = $state<Transaction[]>([]);
  let saving = $state(false);
  const PREVIEW_ROWS = 200;

  function openEditor(rule: Rule | null) {
    const filters: Filters = rule ? compactFilters(rule.filters) : {};
    editor = {
      ruleId: rule?.id ?? null,
      tag: rule?.tag ?? "",
      subtag: rule?.subtag ?? "",
      position: rule ? rule.position : rules.length,
      mode: isSimple(filters) ? "simple" : "json",
      simple: toSimple(filters),
      json: JSON.stringify(filters, null, 2),
    };
    preview = null;
    previewError = null;
    schedulePreview();
  }

  function editorFilters(current: Editor): { ok: true; filters: Filters } | { ok: false; error: string } {
    if (current.mode === "simple") return { ok: true, filters: fromSimple(current.simple) };
    return parseFiltersJson(current.json);
  }

  function setMode(mode: "simple" | "json") {
    if (!editor || editor.mode === mode) return;
    if (mode === "json") {
      editor.json = JSON.stringify(fromSimple(editor.simple), null, 2);
    } else {
      const parsed = parseFiltersJson(editor.json);
      if (!parsed.ok || !isSimple(parsed.filters)) {
        toast("These filters need the JSON editor (nesting or tags)", "error");
        return;
      }
      editor.simple = toSimple(parsed.filters);
    }
    editor.mode = mode;
  }

  let previewTimer: ReturnType<typeof setTimeout> | undefined;
  function schedulePreview() {
    clearTimeout(previewTimer);
    previewTimer = setTimeout(runPreview, 300);
  }

  async function runPreview() {
    if (!editor) return;
    const parsed = editorFilters(editor);
    if (!parsed.ok) {
      previewError = parsed.error;
      preview = null;
      return;
    }
    try {
      preview = await api.previewRule({
        filters: parsed.filters,
        position: editor.position,
        rule_id: editor.ruleId,
      });
      previewError = null;
      await loadPreviewRows();
    } catch (error) {
      previewError = (error as Error).message;
      preview = null;
    }
  }

  async function loadPreviewRows() {
    previewRows = preview ? await transactionsByIds(preview[previewTab].slice(0, PREVIEW_ROWS)) : [];
  }

  async function save() {
    if (!editor || saving) return;
    const parsed = editorFilters(editor);
    if (!parsed.ok) return toast(parsed.error, "error");
    if (editor.mode === "simple" && (startDateInvalid || endDateInvalid)) return toast("A date of the filters isn't a date", "error");
    if (!editor.tag.trim()) return toast("The rule needs a tag", "error");
    const isNew = !labelExists(editor.tag.trim(), editor.subtag.trim());
    const overridden = preview?.overridden_annotation_ids.length ?? 0;
    const warnings = [
      isNew ? `${editor.tag} / ${editor.subtag} is a new tag.` : "",
      overridden ? `It overrides ${overridden} annotations (they stay stored, but unused).` : "",
    ].filter(Boolean);
    if (warnings.length && !(await confirm(warnings.join("\n"), { title: "Save rule", confirmLabel: "Save" }))) return;
    const body = {
      filters: parsed.filters,
      tag: editor.tag.trim(),
      subtag: editor.subtag.trim(),
      position: editor.position,
      allow_new_tags: isNew,
    };
    saving = true;
    try {
      if (editor.ruleId === null) await api.createRule(body);
      else await api.updateRule(editor.ruleId, body);
      toast(editor.ruleId === null ? "Rule created" : "Rule saved", "success");
      cursor.moveTo(editor.position);
      editor = null;
      await Promise.all([load(), refreshStatus(), refreshTags()]);
    } catch (error) {
      toastError(error, "Saving the rule");
    } finally {
      saving = false;
    }
  }

  async function remove() {
    const rule = rules[cursor.index];
    if (!rule) return;
    const confirmed = await confirm(
      `Delete rule ${rule.position + 1}: ${describeFilters(rule.filters)} → ${rule.tag} / ${rule.subtag}?\n\n` +
        `The ${rule.transactions} transactions it tags fall back to their annotations, or become untagged.`,
      { title: "Delete rule", confirmLabel: "Delete", danger: true },
    );
    if (!confirmed) return;
    try {
      await api.deleteRule(rule.id);
      toast("Rule deleted", "success");
      await Promise.all([load(), refreshStatus(), refreshTags()]);
    } catch (error) {
      toastError(error);
    }
  }

  async function move(step: number) {
    const from = cursor.index;
    const to = from + step;
    if (to < 0 || to >= rules.length) return;
    const order = moved(rules, from, to);
    rules = order.map((rule, position) => ({ ...rule, position }));
    cursor.moveTo(to);
    try {
      rules = await api.reorderRules(order.map((rule) => rule.id));
      refreshStatus();
    } catch (error) {
      toastError(error, "Reordering");
      load();
    }
  }

  useKeys("Rules", () => [
    ...cursor.bindings(),
    bind("rules.edit", () => rules[cursor.index] && openEditor(rules[cursor.index])),
    bind("rules.create", () => openEditor(null)),
    bind("rules.delete", remove),
    bind("reorder.up", () => move(-1), { description: "Move the rule up (earlier)" }),
    bind("reorder.down", () => move(1), { description: "Move the rule down (later)" }),
  ]);

  onMount(load);

  const PREVIEW_TABS: { key: PreviewTab; label: string; hint: string }[] = [
    { key: "newly_tagged_ids", label: "Newly tagged", hint: "Untagged now, tagged by this rule" },
    { key: "overridden_annotation_ids", label: "Overridden", hint: "Annotations this rule would override" },
    { key: "effective_ids", label: "Tagged by it", hint: "Everything this rule would tag" },
    { key: "matched_ids", label: "Matched", hint: "Matching, including those an earlier rule takes" },
  ];
</script>

<div class="page">
  <div class="page-header">
    <h1>Rules</h1>
    <span class="muted summary">{rules.length} rules, first match wins</span>
    <div class="spacer"></div>
    <button onclick={() => openEditor(null)} title={keyName("rules.create")}>New rule</button>
  </div>
  <div class="scroll" bind:this={container}>
    <table class="grid">
      <thead>
        <tr>
          <th class="num"><Term key="rule">#</Term></th>
          <th><Term key="rule_filters">Filters</Term></th>
          <th>Tags</th>
          <th class="num">Transactions</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {#each rules as rule, index (rule.id)}
          <tr data-index={index} class:active={index === cursor.index} onmousedown={() => cursor.moveTo(index)} ondblclick={() => openEditor(rule)}>
            <td class="num faint">{rule.position + 1}</td>
            <td class="filters" dir="auto">{describeFilters(rule.filters)}</td>
            <td class="label">
              <span class="swatch" style:background={tagColor(rule.tag, rule.subtag) ?? "transparent"}></span>
              {rule.tag} / {rule.subtag}
            </td>
            <td class="num" class:faint={rule.transactions === 0}>{rule.transactions}</td>
            <td class="row-actions">
              <button class="link" onclick={() => openEditor(rule)}>edit</button>
              <button class="link" onclick={() => { cursor.moveTo(index); move(-1); }} title={keyName("reorder.up")}>↑</button>
              <button class="link" onclick={() => { cursor.moveTo(index); move(1); }} title={keyName("reorder.down")}>↓</button>
              <button class="link danger-link" onclick={() => { cursor.moveTo(index); remove(); }}>delete</button>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
    {#if rules.length === 0}<div class="empty">No rules</div>{/if}
  </div>
</div>

{#if editor}
  <Modal
    title={editor.ruleId === null ? "New rule" : `Edit rule ${rules.find((rule) => rule.id === editor?.ruleId)?.position! + 1}`}
    onclose={() => (editor = null)}
    width="900px"
    bindings={() => [bind("dialog.commit", save, { description: "Save" })]}
  >
    <div class="editor" oninput={schedulePreview} onchange={schedulePreview}>
      <div class="form">
        <span class="field-label">Tags</span>
        <LabelInput bind:tag={editor.tag} bind:subtag={editor.subtag} />
        <span class="field-label"><Term key="rule">Position</Term></span>
        <div class="row">
          <input type="number" min="1" max={rules.length + (editor.ruleId === null ? 1 : 0)} class="position" value={editor.position + 1} oninput={(event) => editor && (editor.position = Math.max(0, Number(event.currentTarget.value) - 1))} />
          <span class="faint">earlier rules win</span>
        </div>
        <span class="field-label"><Term key="rule_filters">Filters</Term></span>
        <div class="modes">
          <button class:on={editor.mode === "simple"} onclick={() => setMode("simple")}>Simple</button>
          <button class:on={editor.mode === "json"} onclick={() => setMode("json")}>JSON</button>
        </div>
        {#if editor.mode === "simple"}
          <span class="field-label sub">raw description</span>
          <input bind:value={editor.simple.rawDescription} placeholder="regular expression, e.g. SuperMart|Green Grocer" dir="auto" data-autofocus />
          <span class="field-label sub">account</span>
          <select bind:value={editor.simple.account}>
            <option value="">any</option>
            {#each app.accounts as account}<option value={account.name}>{account.name}</option>{/each}
          </select>
          <span class="field-label sub">source</span>
          <input bind:value={editor.simple.source} placeholder="exact, e.g. card" />
          <span class="field-label sub">dates</span>
          <div class="row">
            <DateInput bind:value={editor.simple.startDate} bind:invalid={startDateInvalid} aria-label="From" /> <span class="faint">until (exclusive)</span>
            <DateInput bind:value={editor.simple.endDate} bind:invalid={endDateInvalid} aria-label="Until (exclusive)" />
          </div>
          <span></span>
          <label class="muted"><input type="checkbox" bind:checked={editor.simple.invert} /> invert (match everything else)</label>
        {:else}
          <span></span>
          <div>
            <textarea bind:value={editor.json} rows="8" spellcheck="false"></textarea>
            <p class="faint hint">
              Fields: raw_description (a regular expression), source and account (exact), start_date, end_date (exclusive), invert, and
              and_filters / or_filters (lists of filters, not combined with other fields).
            </p>
          </div>
        {/if}
      </div>
    </div>

    <div class="preview">
      {#if previewError}
        <p class="error-text">{previewError}</p>
      {:else if preview}
        <div class="tabs">
          {#each PREVIEW_TABS as tab}
            <button
              class:on={previewTab === tab.key}
              class:warn={tab.key === "overridden_annotation_ids" && preview.overridden_annotation_ids.length > 0}
              onclick={() => {
                previewTab = tab.key;
                loadPreviewRows();
              }}
            >
              <Term key="rule_preview" context={tab.hint}>{tab.label}</Term> <strong>{preview[tab.key].length}</strong>
            </button>
          {/each}
        </div>
        <div class="list">
          <table>
            <tbody>
              {#each previewRows as transaction (transaction.id)}
                <tr>
                  <td class="muted">{showDate(transaction.date)}</td>
                  <td class="muted">{transaction.account}</td>
                  <td class="what"><TransactionDescription description={writtenDescription(transaction.annotation, transaction.status === "rule")} raw={transaction.raw_description} /></td>
                  <td class="amount"><Amount value={transaction.amount} /></td>
                  <td class="muted">
                    {#if transaction.rule && transaction.rule.id !== editor.ruleId}rule {transaction.rule.position + 1}: {transaction.rule.tag} / {transaction.rule.subtag}
                    {:else if transaction.annotation?.parts.length}{transaction.annotation.parts.map((part) => `${part.tag} / ${part.subtag}`).join(" + ")}
                    {:else}untagged{/if}
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
          {#if previewRows.length === 0}<div class="empty">None</div>{/if}
          {#if preview[previewTab].length > PREVIEW_ROWS}<div class="faint more">and {preview[previewTab].length - PREVIEW_ROWS} more</div>{/if}
        </div>
      {:else}
        <p class="faint">Previewing…</p>
      {/if}
    </div>

    {#snippet footer()}
      <button onclick={() => (editor = null)}>Cancel</button>
      <button class="primary" onclick={save} disabled={saving || !!previewError}>Save <KeyHint action="dialog.commit" /></button>
    {/snippet}
  </Modal>
{/if}

<style>
  .summary {
    font-size: 12px;
  }
  .num {
    text-align: right;
    font-variant-numeric: tabular-nums;
    width: 60px;
  }
  table.grid td {
    padding: 5px 8px;
  }
  .filters {
    font-family: var(--mono);
    font-size: 12px;
  }
  .label {
    white-space: nowrap;
  }
  .row-actions {
    text-align: right;
    white-space: nowrap;
    width: 160px;
  }
  .row-actions button {
    opacity: 0;
  }
  tr:hover .row-actions button,
  tr.active .row-actions button,
  .row-actions button:focus-visible {
    opacity: 1;
  }
  .row-actions button {
    margin-left: 10px;
    font-size: 12px;
  }
  .danger-link {
    color: var(--danger);
  }
  .form {
    display: grid;
    grid-template-columns: 90px 1fr;
    gap: 8px 10px;
    align-items: center;
  }
  .field-label {
    color: var(--muted);
  }
  .field-label.sub {
    text-align: right;
    font-size: 12px;
  }
  .row {
    display: flex;
    gap: 8px;
    align-items: center;
  }
  .position {
    width: 70px;
  }
  textarea {
    width: 100%;
    font: 12px/1.5 var(--mono);
  }
  .hint {
    margin: 4px 0 0;
    font-size: 12px;
  }
  .modes,
  .tabs {
    display: flex;
    gap: 4px;
  }
  .modes button,
  .tabs button {
    border-color: var(--border);
    color: var(--muted);
  }
  .modes button.on,
  .tabs button.on {
    color: var(--text);
    border-color: var(--accent);
    background: var(--accent-soft);
  }
  .tabs button.warn strong {
    color: var(--warning);
  }
  .preview {
    margin-top: 14px;
    padding-top: 10px;
    border-top: 1px solid var(--border);
  }
  .list {
    margin-top: 8px;
    max-height: 260px;
    overflow: auto;
    border: 1px solid var(--border);
    border-radius: var(--radius);
  }
  .list table {
    width: 100%;
    border-collapse: collapse;
  }
  .list td {
    padding: 3px 8px;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  .what {
    max-width: 280px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .empty {
    padding: 14px;
  }
  .more {
    padding: 6px 8px;
  }
  .error-text {
    color: var(--danger);
  }
</style>
