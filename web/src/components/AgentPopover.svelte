<!--
  The header's "Agent API" button: a small popover with the URL of this
  instance's instructions for agents (/agent.md), to copy and give an agent.
-->
<script lang="ts">
  import { keyboard, titled } from "../lib/state.svelte";
  import { bind, useKeys } from "../lib/use-keys.svelte";
  import KeyHint from "./KeyHint.svelte";

  const url = `${window.location.origin}/agent.md`;
  let open = $state(false);
  let copied = $state(false);
  let root: HTMLDivElement | undefined = $state();
  let field: HTMLInputElement | undefined = $state();

  async function copy() {
    try {
      await navigator.clipboard.writeText(url);
    } catch {
      // Without clipboard access (e.g. not a secure context), copy the selection
      field?.select();
      document.execCommand("copy");
    }
    copied = true;
  }

  function show() {
    copied = false;
    open = true;
  }

  useKeys("Agent API", () => [bind("agent.show", show)]);

  // While open, the popover takes the keyboard like a dialog
  $effect(() => {
    if (!open) return;
    const removeLayer = keyboard.push({
      name: "Agent API",
      modal: true,
      // The URL's field is read-only, so its keys are free even while it is focused
      get bindings() {
        return [
          bind("agent.copy", copy),
          bind("agent.open", () => window.open(url, "_blank")),
          bind("dialog.close", () => (open = false)),
        ];
      },
    });
    const closeOutside = (event: PointerEvent) => {
      if (!root?.contains(event.target as Node)) open = false;
    };
    window.addEventListener("pointerdown", closeOutside);
    queueMicrotask(() => field?.select());
    return () => {
      removeLayer();
      window.removeEventListener("pointerdown", closeOutside);
    };
  });
</script>

<div class="agent" bind:this={root}>
  <button class="outline" class:on={open} onclick={() => (open ? (open = false) : show())} title={titled("Instructions for agents to use this app's API", "agent.show")} aria-expanded={open}>
    <!-- A robot -->
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" aria-hidden="true">
      <path d="M8 1.8v2.4M1.2 7.6v2.4M14.8 7.6v2.4M6.2 11h3.6" />
      <rect x="3" y="4.2" width="10" height="9" rx="2.4" />
      <circle cx="6" cy="8.1" r="1" fill="currentColor" stroke="none" />
      <circle cx="10" cy="8.1" r="1" fill="currentColor" stroke="none" />
    </svg>
    Agent API
  </button>
  {#if open}
    <div class="popover" role="dialog" aria-label="Agent API">
      <p>Markdown instructions for an AI agent to use this app's API. Give the agent this URL.</p>
      <input bind:this={field} value={url} readonly aria-label="Agent instructions URL" />
      <div class="actions">
        <button class="primary" onclick={copy}>{copied ? "Copied" : "Copy URL"} <kbd>Enter</kbd></button>
        <a href={url} target="_blank" rel="noreferrer">Open <KeyHint action="agent.open" /></a>
      </div>
    </div>
  {/if}
</div>

<style>
  .agent {
    position: relative;
  }
  .popover {
    position: absolute;
    right: 0;
    top: calc(100% + 8px);
    z-index: 30;
    width: 340px;
    padding: 12px;
    background: var(--surface-3);
    border: 1px solid var(--border-strong);
    border-radius: 6px;
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.4);
  }
  p {
    margin: 0 0 8px;
    font-size: 12px;
    color: var(--muted);
  }
  input {
    width: 100%;
    font-family: var(--mono);
    font-size: 12px;
  }
  .actions {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-top: 10px;
  }
</style>
