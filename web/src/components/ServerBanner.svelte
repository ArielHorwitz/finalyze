<!--
  A warning about how the server runs: a web app older than its sources
  (rebuild it).
-->
<script lang="ts">
  import type { ServerInfo } from "../lib/api";

  let { info }: { info: ServerInfo | null } = $props();
  let dismissed = $state(false);
</script>

{#if info?.stale_web_app && !dismissed}
  <div class="banner warning" role="alert">
    <span>{info.stale_web_app}</span>
    <button class="link" onclick={() => (dismissed = true)} aria-label="Dismiss">✕</button>
  </div>
{/if}

<style>
  .banner {
    display: flex;
    gap: 12px;
    align-items: center;
    padding: 5px 16px;
    font-size: 12px;
    border-bottom: 1px solid var(--border);
  }
  .warning {
    background: var(--warning-soft);
    color: var(--warning);
  }
  .banner span {
    flex: 1;
  }
</style>
