<!--
  Tags and subtags with their usage, properties (color and kind: regular,
  transfer, saving, one-off or reconciliation; subtags inherit them unless
  they have their own), and rename, merge and delete.
-->
<script lang="ts">
  import { onMount } from "svelte";
  import Autocomplete from "../components/Autocomplete.svelte";
  import KeyHint from "../components/KeyHint.svelte";
  import Modal from "../components/Modal.svelte";
  import { api, type Subtag, type Tag } from "../lib/api";
  import { Cursor, scrollToIndex } from "../lib/cursor.svelte";
  import { kindDescription, nextOf, TAG_KINDS, type TagKind } from "../lib/kinds";
  import { app, confirm, keyName, refreshStatus, refreshTags, toast, toastError } from "../lib/state.svelte";
  import { suggestNames } from "../lib/suggest";
  import { bind, useKeys } from "../lib/use-keys.svelte";
  import Term from "../components/Term.svelte";

  interface Item {
    tag: Tag;
    subtag: Subtag | null;
  }
  type Dialog = "edit" | "rename" | "merge" | "create";

  const items: Item[] = $derived(
    app.tags.flatMap((tag) => [
      { tag, subtag: null },
      ...tag.subtags.map((subtag) => ({ tag, subtag })),
    ]),
  );
  const cursor = new Cursor(() => items.length);
  const current = $derived(items[cursor.index] as Item | undefined);
  let container: HTMLDivElement | undefined = $state();
  let dialog = $state<Dialog | null>(null);

  // Dialog fields
  let color = $state("");
  let tagKind = $state<TagKind | "inherit">("regular");
  let newName = $state("");
  let intoTag = $state("");
  let intoSubtag = $state("");
  let createTag = $state("");
  let createSubtag = $state("");

  $effect(() => scrollToIndex(container, cursor.index));

  const nameOf = (item: Item) => (item.subtag ? `${item.tag.name} / ${item.subtag.name || "(none)"}` : item.tag.name);
  const ref = (item: Item) => ({ tag: item.tag.name, subtag: item.subtag ? item.subtag.name : null });

  /** Annotations and rules that use a tag (all its subtags) or subtag. */
  function usage(item: Item): { annotations: number; rules: number } {
    const subtags = item.subtag ? [item.subtag] : item.tag.subtags;
    return {
      annotations: subtags.reduce((sum, subtag) => sum + subtag.annotations, 0),
      rules: subtags.reduce((sum, subtag) => sum + subtag.rules, 0),
    };
  }

  function open(kind: Dialog) {
    const item = current;
    if (!item && kind !== "create") return;
    if (kind === "edit" && item) {
      color = (item.subtag ? item.subtag.color : item.tag.color) ?? "";
      tagKind = (item.subtag ? item.subtag.kind : item.tag.kind) ?? "inherit";
    }
    if (kind === "rename" && item) newName = item.subtag ? item.subtag.name : item.tag.name;
    if (kind === "merge" && item) {
      intoTag = item.tag.name;
      intoSubtag = "";
      if (!item.subtag) intoTag = "";
    }
    if (kind === "create") {
      createTag = item?.tag.name ?? "";
      createSubtag = "";
    }
    dialog = kind;
  }

  async function run(action: () => Promise<unknown>, message: string) {
    try {
      await action();
      toast(message, "success");
      dialog = null;
      await refreshTags();
      refreshStatus();
    } catch (error) {
      toastError(error);
    }
  }

  function saveProperties() {
    const item = current!;
    const value = color.trim() || null;
    if (value && !/^#?[0-9a-f]{3}([0-9a-f]{3})?$/i.test(value) && !/^[a-z]+$/i.test(value)) {
      toast("Colors look like #3987e5", "error");
      return;
    }
    run(
      () =>
        api.updateTag({
          ...ref(item),
          color: value,
          kind: tagKind === "inherit" ? null : tagKind,
        }),
      `Updated ${nameOf(item)}`,
    );
  }

  /** x: the next kind (or inheriting, for a subtag), without a dialog. */
  function cycleKind() {
    const item = current;
    if (!item) return;
    // A subtag inherits, or has its own kind, other than its tag's
    const kinds: (TagKind | null)[] = item.subtag
      ? [null, ...TAG_KINDS.map((option) => option.id).filter((id) => id !== item.tag.kind)]
      : TAG_KINDS.map((option) => option.id);
    const next = nextOf(kinds, item.subtag ? item.subtag.kind : item.tag.kind);
    const label = next ?? `inherited (${item.tag.kind})`;
    run(() => api.updateTag({ ...ref(item), kind: next }), `${nameOf(item)} is ${label}`);
  }

  function rename() {
    const item = current!;
    if (!newName.trim()) return;
    run(() => api.renameTag({ ...ref(item), new_name: newName.trim() }), `Renamed ${nameOf(item)}`);
  }

  async function merge() {
    const item = current!;
    const into = item.subtag ? `${intoTag} / ${intoSubtag}` : intoTag;
    if (!intoTag.trim() || (item.subtag && !intoSubtag.trim())) return;
    const { annotations, rules } = usage(item);
    const confirmed = await confirm(
      `Merge ${nameOf(item)} into ${into}?\n\n${annotations} annotations and ${rules} rules move to ${into}, ` +
        `and ${nameOf(item)} is deleted with its properties.`,
      { title: "Merge", confirmLabel: "Merge", danger: true },
    );
    if (!confirmed) return;
    run(
      () =>
        api.mergeTag({
          ...ref(item),
          into_tag: intoTag.trim(),
          into_subtag: item.subtag ? intoSubtag.trim() : null,
        }),
      `Merged ${nameOf(item)} into ${into}`,
    );
  }

  async function remove() {
    const item = current;
    if (!item) return;
    const { annotations, rules } = usage(item);
    if (annotations > 0 || rules > 0) {
      toast(`${nameOf(item)} is used by ${annotations} annotations and ${rules} rules: merge it instead`, "error");
      return;
    }
    if (!(await confirm(`Delete ${nameOf(item)}?`, { title: "Delete", confirmLabel: "Delete", danger: true }))) return;
    run(() => api.deleteTag(ref(item)), `Deleted ${nameOf(item)}`);
  }

  function create() {
    if (!createTag.trim()) return;
    run(
      () => api.createTag({ tag: createTag.trim(), subtag: createSubtag.trim() || null }),
      `Created ${createTag}${createSubtag ? ` / ${createSubtag}` : ""}`,
    );
  }

  const tagNames = $derived(app.tags.map((tag) => tag.name));
  const subtagNamesOf = (tag: string) => app.tags.find((item) => item.name === tag)?.subtags.map((subtag) => subtag.name) ?? [];

  useKeys("Tags", () => [
    ...cursor.bindings(),
    bind("tags.edit", () => open("edit")),
    bind("tags.kind", cycleKind),
    bind("tags.rename", () => open("rename")),
    bind("tags.merge", () => open("merge")),
    bind("tags.delete", remove),
    bind("tags.create", () => open("create")),
  ]);

  onMount(() => {
    refreshTags().catch(toastError);
  });

  const dialogKeys = (action: () => void) => () => [
    bind("dialog.confirm", action, { description: "Save", inInputs: true }),
  ];
</script>

<div class="page">
  <div class="page-header">
    <h1>Tags</h1>
    <span class="muted summary">{app.tags.length} tags, {items.length - app.tags.length} subtags</span>
    <div class="spacer"></div>
    <button onclick={() => open("create")} title={keyName("tags.create")}>New tag</button>
  </div>
  <div class="scroll" bind:this={container}>
    <table class="grid">
      <thead>
        <tr>
          <th><Term key="tag">Name</Term></th>
          <th>Color</th>
          <th><Term key="kind">Kind</Term></th>
          <th class="num">Transactions</th>
          <th class="num">Annotations</th>
          <th class="num">Rules</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {#each items as item, index (nameOf(item))}
          {@const usageCounts = usage(item)}
          {@const ownColor = item.subtag ? item.subtag.color : item.tag.color}
          {@const shownColor = item.subtag ? item.subtag.effective_color : item.tag.color}
          {@const ownKind = item.subtag ? item.subtag.kind : item.tag.kind}
          {@const kindShown = item.subtag ? item.subtag.effective_kind : item.tag.kind}
          <tr
            data-index={index}
            class:active={index === cursor.index}
            class:tag-row={!item.subtag}
            onmousedown={() => cursor.moveTo(index)}
            ondblclick={() => open("edit")}
          >
            <td class="name" dir="auto">
              {#if item.subtag}<span class="indent">{item.subtag.name || "(none)"}</span>{:else}{item.tag.name}{/if}
            </td>
            <td>
              {#if shownColor}<span class="swatch" style:background={shownColor}></span>{/if}
              <span class:faint={item.subtag && ownColor === null}>
                {shownColor ?? "—"}{#if item.subtag && ownColor === null && shownColor}&nbsp;<Term key="inherited">(inherited)</Term>{:else if item.subtag && ownColor !== null}&nbsp;<Term key="own">(own)</Term>{/if}
              </span>
            </td>
            <td>
              <span class="kind kind-{kindShown}" class:faint={item.subtag && ownKind === null}><Term key={kindShown.replace("-", "_")} quiet context={kindDescription(kindShown)}>{kindShown}</Term></span>
              {#if item.subtag}
                <span class="faint">{#if ownKind === null}<Term key="inherited">(inherited)</Term>{:else}<Term key="own">(own)</Term>{/if}</span>
              {/if}
            </td>
            <td class="num">{item.subtag ? item.subtag.transactions : item.tag.transactions}</td>
            <td class="num">{usageCounts.annotations}</td>
            <td class="num">{usageCounts.rules}</td>
            <td class="row-actions">
              <button class="link" onclick={() => { cursor.moveTo(index); open("edit"); }}>edit</button>
              <button class="link" onclick={() => { cursor.moveTo(index); open("rename"); }}>rename</button>
              <button class="link" onclick={() => { cursor.moveTo(index); open("merge"); }}>merge</button>
              {#if usageCounts.annotations === 0 && usageCounts.rules === 0}
                <button class="link danger-link" onclick={() => { cursor.moveTo(index); remove(); }}>delete</button>
              {/if}
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</div>

{#if dialog === "edit" && current}
  <Modal title="Edit {nameOf(current)}" onclose={() => (dialog = null)} bindings={dialogKeys(saveProperties)} width="440px">
    <div class="form">
      <span class="field-label">Color</span>
      <div class="color-row">
        <input bind:value={color} placeholder={current.subtag ? "inherit from the tag" : "none"} />
        <input type="color" tabindex="-1" value={/^#[0-9a-f]{6}$/i.test(color) ? color : "#888888"} oninput={(event) => (color = event.currentTarget.value)} />
        {#if color}<button class="link" onclick={() => (color = "")}>{current.subtag ? "inherit" : "clear"}</button>{/if}
      </div>
      <span class="field-label">Kind</span>
      <select bind:value={tagKind}>
        {#if current.subtag}<option value="inherit">inherit from the tag ({current.tag.kind})</option>{/if}
        {#each TAG_KINDS as option (option.id)}<option value={option.id}>{option.label}</option>{/each}
      </select>
    </div>
    <p class="faint">{tagKind === "inherit" ? kindDescription(current.tag.kind) : kindDescription(tagKind)}.</p>
    {#snippet footer()}
      <button onclick={() => (dialog = null)}>Cancel</button>
      <button class="primary" onclick={saveProperties}>Save <kbd>Enter</kbd></button>
    {/snippet}
  </Modal>
{/if}

{#if dialog === "rename" && current}
  <Modal title="Rename {nameOf(current)}" onclose={() => (dialog = null)} bindings={dialogKeys(rename)} width="440px">
    <input class="wide" bind:value={newName} dir="auto" />
    <p class="faint">Annotations and rules keep using it under the new name. To combine it with an existing one, merge instead.</p>
    {#snippet footer()}
      <button onclick={() => (dialog = null)}>Cancel</button>
      <button class="primary" onclick={rename}>Rename <kbd>Enter</kbd></button>
    {/snippet}
  </Modal>
{/if}

{#if dialog === "merge" && current}
  <Modal title="Merge {nameOf(current)}" onclose={() => (dialog = null)} bindings={() => [bind("dialog.commit", merge, { description: "Merge" })]} width="460px">
    <div class="form">
      <span class="field-label">Into tag</span>
      <Autocomplete bind:value={intoTag} suggest={(query) => suggestNames(tagNames.filter((name) => current?.subtag || name !== current?.tag.name), query).map((name) => ({ key: name, label: name }))} />
      {#if current.subtag}
        <span class="field-label">subtag</span>
        <Autocomplete bind:value={intoSubtag} suggest={(query) => suggestNames(subtagNamesOf(intoTag), query).map((name) => ({ key: name, label: name }))} />
      {/if}
    </div>
    <p class="faint">
      Its {usage(current).annotations} annotations and {usage(current).rules} rules move to the target, which keeps its own
      properties.{#if current.subtag} A missing subtag is created.{/if}
    </p>
    {#snippet footer()}
      <button onclick={() => (dialog = null)}>Cancel</button>
      <button class="danger" onclick={merge}>Merge… <KeyHint action="dialog.commit" /></button>
    {/snippet}
  </Modal>
{/if}

{#if dialog === "create"}
  <Modal title="New tag" onclose={() => (dialog = null)} bindings={dialogKeys(create)} width="440px">
    <div class="form">
      <span class="field-label">Tag</span>
      <Autocomplete bind:value={createTag} suggest={(query) => suggestNames(tagNames, query).map((name) => ({ key: name, label: name }))} />
      <span class="field-label">Subtag</span>
      <input bind:value={createSubtag} placeholder="optional" />
    </div>
    {#snippet footer()}
      <button onclick={() => (dialog = null)}>Cancel</button>
      <button class="primary" onclick={create}>Create <kbd>Enter</kbd></button>
    {/snippet}
  </Modal>
{/if}

<style>
  .summary {
    font-size: 12px;
  }
  table.grid td {
    padding: 4px 8px;
  }
  .tag-row td {
    font-weight: 600;
    border-top: 1px solid var(--border-strong);
  }
  .tag-row td.num,
  .tag-row td:nth-child(2),
  .tag-row td:nth-child(3) {
    font-weight: 400;
  }
  .indent {
    padding-left: 18px;
    color: var(--text);
    font-weight: 400;
  }
  .num {
    text-align: right;
    font-variant-numeric: tabular-nums;
    width: 110px;
  }
  .kind-transfer,
  .kind-reconciliation {
    color: var(--muted);
  }
  .kind-saving {
    color: var(--income);
  }
  .kind-one-off {
    color: var(--warning);
  }
  .row-actions {
    text-align: right;
    white-space: nowrap;
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
    font-weight: 400;
    font-size: 12px;
  }
  .danger-link {
    color: var(--danger);
  }
  .form {
    display: grid;
    grid-template-columns: 90px 1fr;
    gap: 10px;
    align-items: center;
  }
  .field-label {
    color: var(--muted);
  }
  .color-row {
    display: flex;
    gap: 8px;
    align-items: center;
  }
  input[type="color"] {
    width: 34px;
    height: 26px;
    padding: 1px;
  }
  .wide {
    width: 100%;
  }
</style>
