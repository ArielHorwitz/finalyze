<!--
  The backups, on the Settings page: named copies of all the data kept on this
  machine, made here or before an import or a restore. Each can be restored
  (checked first, then confirmed in a dialog that backs up the current data
  first), downloaded or deleted. A warning shows ahead of the limit, since new
  backups are then refused.
-->
<script lang="ts">
  import { onMount } from "svelte";
  import { api, ApiError, type Backup, type ImportResult } from "../lib/api";
  import { backupNameProblem, backupsWarning, download, formatSize } from "../lib/archive";
  import { backups, loadBackups, refreshAfterReplacing } from "../lib/backups.svelte";
  import { showTime } from "../lib/display.svelte";
  import { confirm, toast, toastError } from "../lib/state.svelte";
  import ReplaceDialog from "./ReplaceDialog.svelte";

  let { onrestored }: { onrestored: () => void } = $props();

  let name = $state("");
  let creating = $state(false);
  let createError = $state<string | null>(null);
  let restoring = $state<{ backup: Backup; check: ImportResult } | null>(null);

  const listing = $derived(backups.listing);
  const warning = $derived(listing ? backupsWarning(listing) : null);
  const nameProblem = $derived(name.trim() ? backupNameProblem(name, listing?.backups ?? []) : null);

  async function createNow() {
    if (creating || !name.trim() || nameProblem || warning?.full) return;
    creating = true;
    createError = null;
    try {
      const created = await api.createBackup(name.trim());
      toast(`Backed up as “${created.name}”`, "success");
      name = "";
    } catch (error) {
      createError = error instanceof ApiError ? error.detail : String(error);
    } finally {
      creating = false;
    }
    await loadBackups();
  }

  async function checkRestore(backup: Backup) {
    try {
      // (the backup of the current data is the dialog's to ask for)
      const check = await api.restoreBackup(backup.file, { dry_run: true, backup: false });
      restoring = { backup, check };
    } catch (error) {
      toastError(error, `Can't restore “${backup.name}”`);
    }
  }

  async function restoreNow(backupFirst: boolean, backupName: string) {
    if (!restoring) return;
    const restored = restoring.backup;
    const result = await api.restoreBackup(restored.file, { dry_run: false, backup: backupFirst, backup_name: backupName });
    restoring = null;
    toast(
      result.backup ? `Restored “${restored.name}”. The previous data is backed up as “${result.backup.name}”` : `Restored “${restored.name}”`,
      "success",
      8000,
    );
    await refreshAfterReplacing();
    onrestored();
  }

  async function deleteBackup(backup: Backup) {
    const question = `Delete the backup “${backup.name}”, made ${showTime(backup.created_at)}? It may be the only copy of the data it holds.`;
    if (!(await confirm(question, { title: "Delete a backup?", confirmLabel: "Delete", danger: true }))) return;
    try {
      await api.deleteBackup(backup.file);
      toast(`Deleted “${backup.name}”`, "success", 1500);
    } catch (error) {
      toastError(error);
    }
    await loadBackups();
  }

  onMount(loadBackups);
</script>

{#if listing}
  <p class="muted">
    Copies of all the data (as an export holds), kept on this machine in <code>{listing.folder}</code>, each with a name. Made here, and before every
    import or restore unless you uncheck it. At most {listing.limit}, and none is ever deleted automatically.
  </p>
  {#if warning}
    <p class={warning.full ? "warning-text" : "faint"} role="status">{warning.message}</p>
  {/if}

  <div class="create">
    <input bind:value={name} placeholder="Name, e.g. before cleaning tags" aria-label="New backup name" maxlength="200" onkeydown={(event) => event.key === "Enter" && createNow()} />
    <button class="primary" onclick={createNow} disabled={creating || !name.trim() || !!nameProblem || !!warning?.full}>
      {creating ? "Backing up…" : "Back up now"}
    </button>
  </div>
  {#if nameProblem}<p class="warning-text">{nameProblem}</p>{/if}
  {#if createError}<p class="errors">{createError}</p>{/if}

  {#if listing.backups.length}
    <table class="grid backups">
      <thead><tr><th>Name</th><th>Made</th><th class="num">Size</th><th></th></tr></thead>
      <tbody>
        {#each listing.backups as backup (backup.file)}
          <tr>
            <td class="name" title={backup.file}>{backup.name}</td>
            <td class="muted">{showTime(backup.created_at)}</td>
            <td class="num muted">{formatSize(backup.size)}</td>
            <td class="row-actions">
              <button class="link" onclick={() => checkRestore(backup)}>restore</button>
              <button class="link" onclick={() => download(api.backupUrl(backup.file))}>download</button>
              <button class="link" onclick={() => deleteBackup(backup)}>delete</button>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  {:else}
    <p class="faint">None yet.</p>
  {/if}
{/if}

{#if restoring}
  <ReplaceDialog
    action="restore"
    check={restoring.check}
    title="Restore “{restoring.backup.name}”?"
    replacement="the backup “{restoring.backup.name}”, made {showTime(restoring.backup.created_at)}"
    onconfirm={restoreNow}
    onclose={() => (restoring = null)}
  />
{/if}

<style>
  p {
    margin: 0 0 10px;
  }
  code {
    font: 12px var(--mono);
  }
  .create {
    display: flex;
    gap: 8px;
    margin-bottom: 10px;
  }
  .create input {
    flex: 1;
    max-width: 420px;
  }
  .warning-text {
    color: var(--warning);
  }
  .errors {
    color: var(--danger);
    overflow-wrap: anywhere;
  }
  table.backups td {
    padding: 4px 8px;
  }
  .row-actions {
    text-align: right;
    white-space: nowrap;
  }
  .row-actions button + button {
    margin-left: 8px;
  }
</style>
