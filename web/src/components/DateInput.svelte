<!--
  A date's text field, in the date format of the display settings (e.g.
  14/06/2025), bound to an ISO date ("2025-06-14", "" for none). Typing is
  lenient (see parseDate): the value follows the text whenever it is a date,
  and once the field is left (or on Enter) the text is shown back in the
  format, or an error says why it isn't a date (the value stays the last
  date). `invalid` tells whether the text isn't a date. Up and Down step a
  day. Other attributes (e.g. aria-label) go to the input.
-->
<script lang="ts">
  import type { HTMLInputAttributes } from "svelte/elements";
  import { parseDate } from "../lib/dates";
  import { display, showDate } from "../lib/display.svelte";

  let {
    value = $bindable(""),
    invalid = $bindable(false),
    onchange,
    class: className = "",
    ...rest
  }: Omit<HTMLInputAttributes, "value" | "onchange"> & {
    value?: string;
    invalid?: boolean;
    onchange?: (value: string) => void;
  } = $props();

  const errorId = $props.id();
  let text = $state("");
  /** Why the text isn't a date, shown once the field is left. */
  let error = $state("");
  let showError = $state(false);
  // The value and format that the text shows: it follows them when they change
  // from outside (e.g. a filter reset, or the format set in Settings)
  let shown = { value: "", format: "" };

  $effect.pre(() => {
    if (value === shown.value && display.settings.date_format === shown.format) return;
    shown = { value, format: display.settings.date_format };
    text = value ? showDate(value) : "";
    error = "";
    showError = false;
  });

  $effect.pre(() => {
    invalid = error !== "";
  });

  function setValue(date: string) {
    if (date === value) return;
    value = date;
    shown.value = date;
    onchange?.(date);
  }

  /** Read the text: the value follows it if it's a date (or empty). */
  function read(): boolean {
    const parsed = text.trim() ? parseDate(text, display.settings.date_format) : { date: "" };
    error = parsed.error ?? "";
    if (parsed.date !== undefined) setValue(parsed.date);
    return parsed.date !== undefined;
  }

  /** Done typing: the text in the format, or its error shown. */
  function commit() {
    if (read()) text = value ? showDate(value) : "";
    showError = error !== "";
  }

  function step(days: number) {
    if (!read() || !value) return;
    const date = new Date(`${value}T00:00:00Z`);
    date.setUTCDate(date.getUTCDate() + days);
    setValue(date.toISOString().slice(0, 10));
    text = showDate(value);
  }

  function onkeydown(event: KeyboardEvent) {
    if (event.isComposing || event.metaKey || event.altKey) return;
    if (event.key === "Enter") commit();
    else if (!event.ctrlKey && (event.key === "ArrowUp" || event.key === "ArrowDown")) {
      event.preventDefault();
      step(event.key === "ArrowUp" ? 1 : -1);
    }
  }

  function oninput() {
    read();
    // A shown error goes as soon as the text is a date again
    if (!error) showError = false;
  }
</script>

<span class="date-input">
  <input
    {...rest}
    class="date {className}"
    bind:value={text}
    placeholder={display.settings.date_format}
    inputmode="numeric"
    autocomplete="off"
    spellcheck="false"
    aria-invalid={showError ? "true" : undefined}
    aria-describedby={showError ? errorId : undefined}
    title={(showError && error) || rest.title || `A date, as ${display.settings.date_format} (Up and Down step a day)`}
    onchange={commit}
    {onkeydown}
    {oninput}
  />
  <!-- Hidden from assistive technology, the field describes it (give the field an
  aria-label in a label: the label's text would include the error) -->
  {#if showError}<span class="error" id={errorId} aria-hidden="true">{error}</span>{/if}
</span>

<style>
  .date-input {
    position: relative;
    display: inline-block;
  }

  .date {
    width: 7.5em;
    font-variant-numeric: tabular-nums;
  }

  .date[aria-invalid="true"] {
    border-color: var(--danger);
  }

  .error {
    position: absolute;
    top: 100%;
    left: 0;
    z-index: 5;
    margin-top: 2px;
    padding: 1px 6px;
    white-space: nowrap;
    font-size: 11px;
    color: var(--danger);
    background: var(--surface);
    border: 1px solid var(--danger-soft);
    border-radius: var(--radius);
  }
</style>
