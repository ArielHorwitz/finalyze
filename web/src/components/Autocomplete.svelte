<!--
  A text input with a dropdown of suggestions.

  Arrows move in the dropdown, Enter picks the highlighted suggestion (so does
  Tab, after moving with the arrows), Escape closes the dropdown (a second
  Escape leaves the input), unless `escapeLeaves` is set: then Escape is left
  to the app, e.g. to leave a table cell at once (the dropdown closes on blur).
  The keys it handles are marked with preventDefault, so the app's shortcuts
  ignore them.
-->
<script lang="ts" module>
  export interface Suggestion {
    key: string;
    label: string;
    /** Shown dimmed after the label, e.g. a usage count. */
    detail?: string;
    color?: string | null;
  }
</script>

<script lang="ts">
  import type { HTMLInputAttributes } from "svelte/elements";

  let {
    value = $bindable(""),
    input = $bindable(),
    suggest,
    onpick,
    oninput,
    onfocus,
    inputClass = "",
    escapeLeaves = false,
    ...rest
  }: {
    value?: string;
    input?: HTMLInputElement;
    suggest: (query: string) => Suggestion[];
    /** Called with the picked suggestion. Default: the value becomes its label. */
    onpick?: (suggestion: Suggestion, event: KeyboardEvent | MouseEvent) => void;
    oninput?: (value: string) => void;
    onfocus?: () => void;
    inputClass?: string;
    /** Leave Escape to the app, even while the dropdown is open. */
    escapeLeaves?: boolean;
  } & Omit<HTMLInputAttributes, "value" | "oninput" | "onfocus"> = $props();

  let open = $state(false);
  let highlighted = $state(0);
  /** The user moved in the dropdown, so Tab picks rather than keeping the text. */
  let navigated = $state(false);
  let position = $state({ left: 0, top: 0, width: 0 });
  let items = $derived(open ? suggest(value) : []);
  let list: HTMLUListElement | undefined = $state();

  function show() {
    const rect = input!.getBoundingClientRect();
    const below = window.innerHeight - rect.bottom > 220;
    position = { left: rect.left, top: below ? rect.bottom + 2 : rect.top - 2, width: rect.width };
    dropUp = !below;
    open = true;
  }
  let dropUp = $state(false);

  function pick(item: Suggestion, event: KeyboardEvent | MouseEvent) {
    open = false;
    if (onpick) onpick(item, event);
    else {
      value = item.label;
      oninput?.(value);
    }
  }

  function onkeydown(event: KeyboardEvent) {
    if (event.isComposing) return;
    const hasItems = open && items.length > 0;
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      if (!open) return show();
      if (!items.length) return;
      const step = event.key === "ArrowDown" ? 1 : -1;
      highlighted = (highlighted + step + items.length) % items.length;
      navigated = true;
      list?.children[highlighted]?.scrollIntoView({ block: "nearest" });
    } else if (event.key === "Enter" && !event.ctrlKey && !event.metaKey && hasItems) {
      event.preventDefault();
      pick(items[highlighted], event);
    } else if (event.key === "Tab" && !event.shiftKey && hasItems && navigated) {
      pick(items[highlighted], event);
    } else if (event.key === "Escape" && open && !escapeLeaves) {
      event.preventDefault();
      open = false;
    }
  }
</script>

<input
  {...rest}
  bind:this={input}
  bind:value
  class={inputClass}
  autocomplete="off"
  role="combobox"
  aria-expanded={open}
  aria-autocomplete="list"
  onkeydown={onkeydown}
  oninput={() => {
    highlighted = 0;
    navigated = false;
    if (!open) show();
    oninput?.(value);
  }}
  onfocus={() => {
    highlighted = 0;
    navigated = false;
    show();
    onfocus?.();
  }}
  onblur={() => (open = false)}
/>

{#if open && items.length > 0}
  <!-- Out of the Tab order: an overflowing list is focusable, so Tab would land
       on it, the blur would close it, and the focus would be lost -->
  <ul
    class="dropdown"
    tabindex="-1"
    class:up={dropUp}
    bind:this={list}
    role="listbox"
    style:left="{position.left}px"
    style:top="{position.top}px"
    style:min-width="{Math.max(position.width, 180)}px"
  >
    {#each items as item, index (item.key)}
      <li
        role="option"
        aria-selected={index === highlighted}
        class:highlighted={index === highlighted}
        onmousedown={(event) => {
          event.preventDefault(); // keep the focus in the input
          pick(item, event);
        }}
        onmousemove={() => (highlighted = index)}
      >
        {#if item.color !== undefined}
          <span class="swatch" style:background={item.color ?? "transparent"}></span>
        {/if}
        <span class="label" dir="auto">{item.label}</span>
        {#if item.detail}<span class="detail">{item.detail}</span>{/if}
      </li>
    {/each}
  </ul>
{/if}

<style>
  .dropdown {
    position: fixed;
    z-index: 100;
    max-height: 220px;
    overflow: auto;
    margin: 0;
    padding: 3px;
    list-style: none;
    background: var(--surface-2);
    border: 1px solid var(--border-strong);
    border-radius: var(--radius);
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.45);
  }
  .dropdown.up {
    transform: translateY(-100%);
  }
  li {
    display: flex;
    align-items: center;
    gap: 7px;
    padding: 3px 7px;
    border-radius: 4px;
    cursor: pointer;
    white-space: nowrap;
  }
  li.highlighted {
    background: var(--accent-soft);
  }
  .label {
    flex: 1;
  }
  .detail {
    color: var(--faint);
    font-size: 11px;
    font-variant-numeric: tabular-nums;
  }
</style>
