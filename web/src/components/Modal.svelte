<!-- A dialog over the page. Escape closes it, and it takes the keyboard while open. -->
<script lang="ts">
  import { onMount, type Snippet } from "svelte";
  import type { Binding } from "../lib/keys";
  import { bind, useKeys } from "../lib/use-keys.svelte";

  let {
    title,
    onclose,
    bindings = () => [],
    width = "520px",
    allowsNavigation = false,
    children,
    footer,
  }: {
    title: string;
    onclose: () => void;
    bindings?: () => Binding[];
    width?: string;
    /** Let the page navigation keys through, for a dialog with nothing to lose. */
    allowsNavigation?: boolean;
    children: Snippet;
    footer?: Snippet;
  } = $props();

  let dialog: HTMLDivElement;

  // A dialog allows navigating or not for as long as it is open
  // svelte-ignore state_referenced_locally
  useKeys(
    "Dialog",
    () => [
      ...bindings(),
      bind("dialog.close", onclose),
    ],
    { modal: true, allowsNavigation },
  );

  onMount(() => {
    // Focus the first field, or the dialog itself so keys work right away
    const previous = document.activeElement as HTMLElement | null;
    const field =
      dialog.querySelector<HTMLElement>("[data-autofocus]") ??
      dialog.querySelector<HTMLElement>("input, select, textarea");
    (field ?? dialog).focus();
    return () => previous?.focus?.();
  });
</script>

<div class="backdrop" onmousedown={(event) => event.target === event.currentTarget && onclose()} role="presentation">
  <div class="dialog" style:width bind:this={dialog} role="dialog" aria-modal="true" aria-label={title} tabindex="-1">
    <header>
      <h2>{title}</h2>
      <button class="link close" onclick={onclose} aria-label="Close" tabindex="-1">✕</button>
    </header>
    <div class="body">{@render children()}</div>
    {#if footer}
      <footer>{@render footer()}</footer>
    {/if}
  </div>
</div>

<style>
  .backdrop {
    position: fixed;
    inset: 0;
    z-index: 50;
    background: rgba(5, 7, 10, 0.6);
    display: flex;
    align-items: flex-start;
    justify-content: center;
    padding-top: 10vh;
  }
  .dialog {
    max-width: calc(100vw - 40px);
    max-height: 80vh;
    display: flex;
    flex-direction: column;
    background: var(--surface);
    border: 1px solid var(--border-strong);
    border-radius: 8px;
    box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
    outline: none;
  }
  header {
    display: flex;
    align-items: center;
    padding: 12px 16px 8px;
  }
  h2 {
    flex: 1;
    margin: 0;
    font-size: 14px;
    font-weight: 600;
  }
  .close {
    color: var(--muted);
  }
  .body {
    padding: 4px 16px 12px;
    overflow: auto;
  }
  footer {
    display: flex;
    gap: 8px;
    justify-content: flex-end;
    align-items: center;
    padding: 10px 16px;
    border-top: 1px solid var(--border);
  }
</style>
