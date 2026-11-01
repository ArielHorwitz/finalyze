<!--
  A description's editor: Shift+Enter adds a line, and Enter is done with it
  (it leaves the field, keeping the text). It grows to fit while focused;
  otherwise it shows the first line, a marker when there are more, and the
  whole description on hover (or the `title`, if given). Other attributes (e.g. data-field, aria-label, a
  dir for the placeholder) go to the textarea.
-->
<script lang="ts">
  import type { HTMLTextareaAttributes } from "svelte/elements";
  import { descriptionSummary, moreLinesText } from "../lib/descriptions";

  let {
    value = $bindable(""),
    oninput,
    class: className = "",
    ...rest
  }: Omit<HTMLTextareaAttributes, "value" | "oninput"> & { value?: string; oninput?: (value: string) => void } = $props();

  let textarea: HTMLTextAreaElement | undefined = $state();
  let focused = $state(false);
  const summary = $derived(descriptionSummary(value ?? ""));

  // Fit the text while editing, one line otherwise
  $effect(() => {
    void value;
    if (!textarea) return;
    textarea.style.height = "";
    if (focused) textarea.style.height = `${textarea.scrollHeight + textarea.offsetHeight - textarea.clientHeight}px`;
  });

  function onkeydown(event: KeyboardEvent) {
    if (event.key !== "Enter" || event.isComposing || event.shiftKey || event.ctrlKey || event.metaKey || event.altKey) return;
    event.preventDefault();
    textarea?.blur();
  }
</script>

<span class="description-input">
  <textarea
    dir="auto"
    {...rest}
    bind:this={textarea}
    bind:value
    class={className}
    class:collapsed={!focused}
    class:has-more={!focused && summary.more > 0}
    rows="1"
    title={rest.title ?? (!focused && summary.more > 0 ? value : undefined)}
    {onkeydown}
    onfocus={() => (focused = true)}
    onblur={() => (focused = false)}
    oninput={() => oninput?.(value)}
  ></textarea>
  {#if !focused && summary.more > 0}<span class="more-lines" aria-hidden="true">{moreLinesText(summary.more)}</span>{/if}
</span>

<style>
  .description-input {
    position: relative;
    display: block;
  }
  textarea {
    display: block;
    width: 100%;
    resize: none;
    overflow: hidden;
  }
  /* One line: the first, unwrapped */
  textarea.collapsed {
    white-space: pre;
  }
  textarea.has-more {
    padding-right: 52px;
  }
  .more-lines {
    position: absolute;
    right: 4px;
    top: 50%;
    transform: translateY(-50%);
    padding: 0 4px;
    border-radius: 3px;
    font-size: 10px;
    color: var(--faint);
    background: var(--surface-2);
    border: 1px solid var(--border);
    pointer-events: none;
  }
</style>
