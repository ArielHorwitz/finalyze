<script lang="ts">
  import type { ConfirmRequest } from "../lib/state.svelte";
  import Modal from "./Modal.svelte";
  import { bind } from "../lib/use-keys.svelte";

  let { request }: { request: ConfirmRequest } = $props();
</script>

<Modal
  title={request.title}
  onclose={() => request.resolve(false)}
  bindings={() => [bind("dialog.confirm", () => request.resolve(true), { description: request.confirmLabel })]}
  width="440px"
>
  <p class="message">{request.message}</p>
  {#snippet footer()}
    <button onclick={() => request.resolve(false)}>Cancel <kbd>Esc</kbd></button>
    <button class={request.danger ? "danger" : "primary"} data-autofocus onclick={() => request.resolve(true)}>
      {request.confirmLabel} <kbd>Enter</kbd>
    </button>
  {/snippet}
</Modal>

<style>
  .message {
    white-space: pre-line;
    margin: 4px 0;
  }
</style>
