<!--
  The keyboard shortcuts, on the Settings page: the keys of the four
  directions, and every action by page with its keys, to change (press the new
  key), add to, reset or clear. Saved as they change, unless keys clash (two actions of a
  page with one key): then the clashes show, and nothing is saved until they
  are resolved.
-->
<script lang="ts">
  import { onMount } from "svelte";
  import { api } from "../lib/api";
  import { comboOf } from "../lib/keys";
  import {
    ACTIONS,
    DEFAULT_KEYMAP,
    DIRECTION_PRESETS,
    DIRECTIONS,
    GO_PREFIX,
    SCOPES,
    clashes,
    clashText,
    defaultKeys,
    describe,
    isConfigurable,
    isReserved,
    keysOf,
    normalized,
    ownKeys,
    problems,
    settingsOf,
    type Action,
    type Direction,
    type DirectionKeys,
    type KeymapSettings,
    type Scope,
  } from "../lib/keymap";
  import { keymap, loadKeymap, toast, toastError } from "../lib/state.svelte";
  import Modal from "./Modal.svelte";
  import Term from "./Term.svelte";

  /** Whether the shown keymap isn't saved (it clashes). */
  let { unsaved = $bindable(false) }: { unsaved?: boolean } = $props();

  let draft = $state<KeymapSettings | null>(null);
  let filter = $state("");
  let filterInput: HTMLInputElement | undefined = $state();
  let container: HTMLElement | undefined = $state();

  const found = $derived(draft ? problems(draft) : []);
  const clashing = $derived(draft ? clashes(draft) : []);
  $effect(() => {
    unsaved = !!draft && JSON.stringify(draft) !== JSON.stringify(normalized(keymap.settings));
  });

  const groups = $derived.by(() => {
    if (!draft) return [];
    const needle = filter.trim().toLowerCase();
    const matches = (action: Action) =>
      !needle ||
      [action.description, action.id, ...keysOf(draft!, action.id)].some((text) => text.toLowerCase().includes(needle));
    return (Object.keys(SCOPES) as Scope[])
      .map((scope) => ({ scope, actions: ACTIONS.filter((action) => action.scope === scope && matches(action)) }))
      .filter((group) => group.actions.length > 0);
  });

  onMount(async () => {
    await loadKeymap();
    draft = normalized($state.snapshot(keymap.settings));
  });

  /** Show a change, and save it unless something clashes. */
  async function change(next: KeymapSettings) {
    draft = normalized(next);
    if (problems(draft).length > 0) return;
    const saving = $state.snapshot(draft);
    try {
      keymap.settings = settingsOf(await api.setKeymap(saving));
      toast("Saved the keyboard shortcuts", "success", 1500);
    } catch (error) {
      toastError(error, "Keyboard shortcuts");
    }
  }

  function setKeys(id: string, keys: string[]) {
    if (!draft) return;
    change({ ...draft, overrides: { ...draft.overrides, [id]: keys } });
  }

  function reset(id: string) {
    if (!draft) return;
    const { [id]: _, ...overrides } = draft.overrides;
    change({ ...draft, overrides });
  }

  function resetAll() {
    if (draft) change({ ...draft, overrides: {} });
  }

  function setDirections(directions: DirectionKeys) {
    if (draft) change({ ...draft, directions });
  }

  const ARROWS: Record<Direction, string> = { left: "ArrowLeft", down: "ArrowDown", up: "ArrowUp", right: "ArrowRight" };

  // Capturing a key: replacing an action's keys, adding one, or a direction's key
  let capturing = $state<{ action: Action; adding: boolean } | { direction: Direction } | null>(null);
  let captureError = $state<string | null>(null);

  function capture(action: Action, adding: boolean) {
    captureError = null;
    capturing = { action, adding };
  }

  function captureDirection(direction: Direction) {
    captureError = null;
    capturing = { direction };
  }

  /** Why a pressed key can't be the captured one, if it can't. */
  function captureProblem(key: string): string | null {
    if (isReserved(key)) return `${key} keeps its own meaning: press another key`;
    if (["Dead", "Unidentified", "Process"].includes(key)) return "That key can't be told apart: press another";
    if (!capturing) return null;
    if ("direction" in capturing) {
      // With Shift, the directions reorder
      if (key.length !== 1 || /[A-Z]/.test(key)) return "A direction is a single key, without Shift, Ctrl or Alt";
    } else if ((capturing.action.navigation || capturing.action.id === GO_PREFIX) && key.includes("+") && key !== "+") {
      return "This one is a single key, without Ctrl or Alt";
    }
    return null;
  }

  $effect(() => {
    if (!capturing) return;
    const captured = capturing;
    // Before the app's shortcuts: the key is the new one, not a shortcut
    const onKey = (event: KeyboardEvent) => {
      if (["Shift", "Control", "Alt", "Meta"].includes(event.key) || event.key === "Escape") return;
      event.preventDefault();
      event.stopPropagation();
      const key = comboOf(event);
      captureError = captureProblem(key);
      if (captureError || !draft) return;
      if ("direction" in captured) {
        setDirections({ ...draft.directions, [captured.direction]: key });
      } else {
        const current = ownKeys(draft, captured.action.id);
        setKeys(captured.action.id, captured.adding ? [...current.filter((other) => other !== key), key] : [key]);
      }
      capturing = null;
    };
    window.addEventListener("keydown", onKey, true);
    return () => window.removeEventListener("keydown", onKey, true);
  });

  /** "Ctrl+Enter" as keycaps, and "g t" as a sequence. */
  function keycaps(key: string): string[][] {
    return key.split(" ").map((step) => (step === "+" ? ["+"] : step.split(/\+(?!$)/)));
  }

  function clashesOf(id: string): string[] {
    return clashing.filter((clash) => clash.actions.includes(id)).map(clashText);
  }

  /** The clashes of the actions that follow a direction. */
  function directionClashes(direction: Direction): string[] {
    const followers = ACTIONS.filter((action) => action.direction === direction).map((action) => action.id);
    return clashing.filter((clash) => clash.actions.some((id) => followers.includes(id))).map(clashText);
  }

  /** Focus the search, to find an action (Settings' shortcut). */
  export function focus() {
    filterInput?.focus();
    filterInput?.scrollIntoView({ block: "start" });
  }

  /** Enter in the search goes to the first action shown. */
  function onFilterKey(event: KeyboardEvent) {
    if (event.key !== "Enter") return;
    event.preventDefault();
    container?.querySelector<HTMLElement>("tr.action button")?.focus();
  }
</script>

{#if draft}
  {@const settings = draft}
  <div class="fields">
    <div class="directions">
      <div class="inline">
        <strong><Term key="directions">Directions</Term></strong>
        <span class="faint">rows, the breakdown's months, and with Shift, reordering</span>
        <span class="spacer"></span>
        {#each DIRECTION_PRESETS as preset}
          <button onclick={() => setDirections(preset.keys)} aria-label="Directions: {preset.title}">{preset.title}</button>
        {/each}
      </div>
      <table class="grid">
        <tbody>
          {#each DIRECTIONS as direction (direction.id)}
            {@const key = settings.directions[direction.id]}
            {@const directionTexts = directionClashes(direction.id)}
            <tr class="direction" class:clash={directionTexts.length > 0} data-direction={direction.id}>
              <td>
                {direction.title}
                {#each directionTexts as text}<div class="clash-text">{text}</div>{/each}
              </td>
              <td class="keys">
                <kbd>{key}</kbd><span class="or">/</span><kbd class="fixed" title="Fixed">{ARROWS[direction.id]}</kbd>
                {#if key !== DEFAULT_KEYMAP.directions[direction.id]}
                  <span class="changed" title="Default: {DEFAULT_KEYMAP.directions[direction.id]}">changed</span>
                {/if}
              </td>
              <td class="row-actions">
                <button class="link" onclick={() => captureDirection(direction.id)} aria-label="Change the key of {direction.id}">change</button>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    <div class="inline">
      <input
        type="search"
        class="filter"
        placeholder="Find an action or a key"
        aria-label="Find a shortcut"
        bind:value={filter}
        bind:this={filterInput}
        onkeydown={onFilterKey}
      />
      <span class="spacer"></span>
      {#if Object.keys(settings.overrides).length}
        <span class="faint">{Object.keys(settings.overrides).length} changed</span>
        <button onclick={resetAll}>Reset all to the defaults</button>
      {/if}
    </div>
    {#if found.length}
      <div class="problems" role="alert">
        <strong><Term key="key_clash">Not saved</Term>:</strong>
        <ul class="errors">{#each found as problem}<li>{problem}</li>{/each}</ul>
      </div>
    {/if}
  </div>

  <div class="keymap" bind:this={container}>
    <table class="grid">
      {#each groups as group (group.scope)}
        <tbody>
          <tr class="scope"><th colspan="3">{SCOPES[group.scope]}</th></tr>
          {#each group.actions as action (action.id)}
            {@const own = ownKeys(settings, action.id)}
            {@const changed = action.id in settings.overrides}
            {@const actionClashes = clashesOf(action.id)}
            <tr class="action" class:clash={actionClashes.length > 0} data-action={action.id}>
              <td>
                {action.description}
                <span class="id">{action.id}</span>
                {#each actionClashes as text}<div class="clash-text">{text}</div>{/each}
              </td>
              <td class="keys">
                {#each own as key, keyIndex}
                  {#if keyIndex > 0}<span class="or">/</span>{/if}
                  {#if action.navigation}<kbd>{ownKeys(settings, GO_PREFIX)[0]}</kbd><span class="then">then</span>{/if}
                  {#each keycaps(key) as step}{#each step as cap, capIndex}{#if capIndex > 0}+{/if}<kbd>{cap}</kbd>{/each}{/each}
                {/each}
                {#if isConfigurable(action) && own.length === 0}<span class="faint">none</span>{/if}
                {#each action.fixed ?? [] as key, keyIndex}
                  {#if keyIndex > 0 || own.length > 0}<span class="or">/</span>{/if}
                  <kbd class="fixed" title="Fixed">{key}</kbd>
                {/each}
                {#if changed}<span class="changed" title="Default: {defaultKeys(action, settings.directions).join(', ') || 'none'}">changed</span>{/if}
              </td>
              <td class="row-actions">
                {#if isConfigurable(action)}
                  <button class="link" onclick={() => capture(action, false)} aria-label="Change the key of {action.id}">change</button>
                  {#if !action.single}
                    <button class="link" onclick={() => capture(action, true)} aria-label="Add a key to {action.id}">add</button>
                    {#if own.length}<button class="link" onclick={() => setKeys(action.id, [])} aria-label="Clear the keys of {action.id}">clear</button>{/if}
                  {/if}
                  {#if changed}<button class="link" onclick={() => reset(action.id)} aria-label="Reset {action.id}">reset</button>{/if}
                {:else}
                  <span class="faint">fixed</span>
                {/if}
              </td>
            </tr>
          {/each}
        </tbody>
      {/each}
    </table>
  </div>
{/if}

{#if capturing && "direction" in capturing}
  {@const direction = DIRECTIONS.find((item) => item.id === (capturing as { direction: Direction }).direction)}
  <Modal title="Change the key" onclose={() => (capturing = null)} width="440px">
    <p><strong>{direction?.title}</strong> <span class="faint">(directions)</span></p>
    <p class="prompt">Press the key, without Shift (which reorders). The arrows always work too. <kbd>Esc</kbd> cancels.</p>
    {#if captureError}<p class="errors" role="alert">{captureError}</p>{/if}
  </Modal>
{:else if capturing}
  {@const action = capturing.action}
  <Modal title={capturing.adding ? "Add a key" : "Change the key"} onclose={() => (capturing = null)} width="440px">
    <p><strong>{describe(action.id)}</strong> <span class="faint">({SCOPES[action.scope]})</span></p>
    <p class="prompt">
      {#if action.navigation}
        Press the page's letter, typed after <kbd>{draft ? ownKeys(draft, GO_PREFIX)[0] : "g"}</kbd>.
      {:else}
        Press the key, or a combination with Ctrl or Alt.
      {/if}
      <kbd>Esc</kbd> cancels.
    </p>
    {#if captureError}<p class="errors" role="alert">{captureError}</p>{/if}
  </Modal>
{/if}

<style>
  .fields {
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-bottom: 8px;
  }
  .inline {
    display: flex;
    gap: 8px;
    align-items: center;
  }
  .spacer {
    flex: 1;
  }
  .directions table {
    width: auto;
    min-width: 360px;
  }
  .filter {
    width: 280px;
  }
  .problems {
    padding: 6px 10px;
    border-radius: var(--radius);
    border: 1px solid var(--danger);
  }
  .errors {
    margin: 0;
    padding-left: 18px;
    color: var(--danger);
  }
  p.errors {
    padding-left: 0;
  }
  .keymap {
    max-height: 480px;
    overflow: auto;
  }
  table {
    width: 100%;
  }
  td {
    padding: 4px 8px;
    vertical-align: top;
  }
  /* Not sticky like other tables' headings, which would pile up */
  tr.scope th {
    position: static;
    padding: 10px 8px 4px;
    text-align: left;
    font-size: 11px;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--muted);
  }
  .id {
    margin-left: 6px;
    font: 11px var(--mono);
    color: var(--faint);
  }
  tr.clash td:first-child {
    box-shadow: inset 2px 0 0 var(--danger);
  }
  .clash-text {
    color: var(--danger);
    font-size: 12px;
  }
  .keys {
    white-space: nowrap;
  }
  kbd.fixed {
    opacity: 0.6;
  }
  .or,
  .then {
    color: var(--faint);
    margin: 0 3px;
    font-size: 11px;
  }
  .changed {
    margin-left: 6px;
    font-size: 11px;
    color: var(--accent);
  }
  .row-actions {
    text-align: right;
    white-space: nowrap;
    width: 1%;
  }
  .prompt {
    color: var(--muted);
  }
</style>
