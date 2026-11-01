<!--
  Export and import of the whole instance, on the Settings page: an export is
  downloaded, an import uploaded (checked first, a dry run, then confirmed in a
  dialog that backs up the current data first), replacing all the data.
-->
<script lang="ts">
  import { api, ApiError, type ImportResult } from "../lib/api";
  import { download } from "../lib/archive";
  import { refreshAfterReplacing } from "../lib/backups.svelte";
  import { showTime } from "../lib/display.svelte";
  import { toast } from "../lib/state.svelte";
  import ReplaceDialog from "./ReplaceDialog.svelte";

  let { onimported }: { onimported: () => void } = $props();

  let zipInput: HTMLInputElement | undefined = $state();
  let zip = $state<File | null>(null);
  let checking = $state(false);
  let importError = $state<string | null>(null);
  let check = $state<ImportResult | null>(null);

  async function checkImport() {
    if (checking || !zip) return;
    checking = true;
    importError = null;
    try {
      // (the backup is the dialog's to ask for)
      check = await api.importInstance(zip, { dry_run: true, backup: false });
    } catch (error) {
      importError = error instanceof ApiError ? error.detail : String(error);
    } finally {
      checking = false;
    }
  }

  async function importNow(backup: boolean, name: string) {
    if (!zip) return;
    // (uploaded again: the server keeps nothing of a dry run)
    const result = await api.importInstance(zip, { dry_run: false, backup, backup_name: name });
    check = null;
    toast(result.backup ? `Imported. The previous data is backed up as “${result.backup.name}”` : "Imported", "success", 8000);
    zip = null;
    if (zipInput) zipInput.value = "";
    await refreshAfterReplacing();
    onimported();
  }
</script>

<p class="muted">
  Everything in one zip file: the store (annotations, tags, rules, accounts and settings), the ingested transactions, the raw folder and the
  config, but not where the data is on this machine. To keep a copy, or to move to another machine. Exports are downloaded, and imports
  uploaded, from wherever this browser is. An export holds all the data: keep it private.
</p>

<h3>Export</h3>
<div class="actions">
  <button class="primary" onclick={() => download(api.exportUrl)}>Download an export</button>
  <span class="faint">a zip named by the time it was made. Nothing is kept on the server (backups are, below).</span>
</div>

<h3>Import</h3>
<div class="fields">
  <label class="row">
    Export to import
    <input type="file" accept=".zip,application/zip" bind:this={zipInput} onchange={(event) => ((zip = event.currentTarget.files?.[0] ?? null), (importError = null))} aria-label="Zip to import" />
  </label>
  <p class="faint">
    Its config, store and ingested data replace these, and this machine's raw folder will hold exactly its raw files. Where the data is stays.
  </p>
  <div class="actions">
    <button class="danger" onclick={checkImport} disabled={checking || !zip}>{checking ? "Checking…" : "Import…"}</button>
    <span class="faint">Checks the file, then shows what it replaces, and backs up the current data first unless you uncheck it.</span>
  </div>
  {#if importError}<p class="errors">{importError}</p>{/if}
</div>

{#if check}
  <ReplaceDialog
    action="import"
    {check}
    title="Replace all the data?"
    replacement="the export of {showTime(check.manifest.created_at)}"
    onconfirm={importNow}
    onclose={() => (check = null)}
  />
{/if}

<style>
  h3 {
    margin: 16px 0 6px;
    font-size: 13px;
    color: var(--muted);
  }
  p {
    margin: 0 0 10px;
  }
  .fields {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .fields p {
    margin: 0;
  }
  .row {
    display: grid;
    grid-template-columns: 180px 1fr;
    gap: 8px;
    align-items: center;
  }
  .actions {
    display: flex;
    gap: 12px;
    align-items: center;
  }
  .actions .faint {
    font-size: 12px;
  }
  .errors {
    color: var(--danger);
    overflow-wrap: anywhere;
  }
</style>
