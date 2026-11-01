<!--
  The confirmation of an import or a restore, from its dry run: what replaces
  the data, what happens to the raw folder's files, and the backup of the
  current data first (checked by default, named, refused at the limit). Without
  a backup, it says plainly what will be gone.
-->
<script lang="ts">
  import { ApiError, type ImportResult } from "../lib/api";
  import { backupNameProblem, backupsWarning, counted, firstOf } from "../lib/archive";
  import { backups, loadBackups, showBackupsList } from "../lib/backups.svelte";
  import { bind } from "../lib/use-keys.svelte";
  import Modal from "./Modal.svelte";

  let {
    action,
    check,
    title,
    replacement,
    onconfirm,
    onclose,
  }: {
    action: "import" | "restore";
    /** The dry run. */
    check: ImportResult;
    title: string;
    /** What replaces the data, e.g. "the export of 15/06/2025 10:00". */
    replacement: string;
    /** Replace the data (backed up first if `backup`): throws if it can't. */
    onconfirm: (backup: boolean, name: string) => Promise<void>;
    onclose: () => void;
  } = $props();

  let backupFirst = $state(true);
  // svelte-ignore state_referenced_locally
  let name = $state(backups.listing?.default_names[action] ?? "");
  let busy = $state(false);
  let error = $state<string | null>(null);

  const warning = $derived(backups.listing ? backupsWarning(backups.listing) : null);
  const nameProblem = $derived(backupNameProblem(name, backups.listing?.backups ?? []));
  const blocked = $derived(backupFirst && (!!warning?.full || !!nameProblem));
  const unbacked = $derived(new Set(check.unbacked_raw_files));
  // Those in no backup first, all of them shown
  const removed = $derived(
    firstOf(
      [...check.unbacked_raw_files, ...check.removed_raw_files.filter((file) => !unbacked.has(file))],
      Math.max(10, unbacked.size),
    ),
  );
  const confirmLabel = $derived(action === "import" ? "Import" : "Restore");

  async function confirmNow() {
    if (busy || blocked) return;
    busy = true;
    error = null;
    try {
      await onconfirm(backupFirst, name.trim());
    } catch (caught) {
      error = caught instanceof ApiError ? caught.detail : String(caught);
      // (its backup may have been made, before what failed)
      await loadBackups();
    } finally {
      busy = false;
    }
  }

  function toBackupsList() {
    onclose();
    showBackupsList();
  }
</script>

<Modal {title} {onclose} width="640px" bindings={() => [bind("dialog.confirm", confirmNow, { description: confirmLabel, inInputs: true })]}>
  <p>
    Replace all the data (annotations, tags, rules, accounts, settings, the config and ingested transactions) with {replacement}? Accounts:
    {check.manifest.accounts.join(", ") || "none"}.
  </p>

  <h3>Raw folder</h3>
  <p>
    This machine's raw folder, <code>{check.raw_dir}</code>, will hold exactly its {counted(check.raw_files, "raw file")}.
    {#if check.replaced_raw_files.length}{counted(check.replaced_raw_files.length, "file")} there {check.replaced_raw_files.length === 1 ? "is" : "are"} replaced by
      {check.replaced_raw_files.length === 1 ? "the one" : "those"} of the same name.{/if}
  </p>
  {#if check.replaced_raw_files.length}
    <details>
      <summary class="faint">Files replaced</summary>
      <ul class="files" aria-label="Raw files replaced">
        {#each check.replaced_raw_files as file (file)}<li><code>{file}</code></li>{/each}
      </ul>
    </details>
  {/if}
  {#if check.removed_raw_files.length}
    <p>{counted(check.removed_raw_files.length, "file")} there {check.removed_raw_files.length === 1 ? "isn't" : "aren't"} in it, and will be removed:</p>
    <ul class="files" aria-label="Raw files removed">
      {#each removed.shown as file (file)}
        <li><code>{file}</code>{#if unbacked.has(file)} <span class="badge warning">in no backup</span>{/if}</li>
      {/each}
      {#if removed.more}<li class="faint">and {removed.more} more</li>{/if}
    </ul>
    {#if unbacked.size}
      <p class="warning-text">
        {counted(unbacked.size, "file")} marked “in no backup” {unbacked.size === 1 ? "is a file" : "are files"} finalyze ignores (hidden, in a subfolder, or outside account
        folders): backups don't hold {unbacked.size === 1 ? "it" : "them"}, so {unbacked.size === 1 ? "it" : "they"} will be gone either way.
      </p>
    {/if}
  {/if}

  <h3>Backup</h3>
  <div class="backup">
    <label class="check"><input type="checkbox" bind:checked={backupFirst} /> Back up the current data first</label>
    <input bind:value={name} disabled={!backupFirst} aria-label="Backup name" maxlength="200" />
  </div>
  {#if backupFirst && warning?.full}
    <p class="warning-text" role="status">
      {warning.message} <button class="link" onclick={toBackupsList}>Go to the backups list</button>, or uncheck the backup.
    </p>
  {:else if backupFirst && nameProblem}
    <p class="warning-text">{nameProblem}</p>
  {:else if backupFirst && warning}
    <p class="faint" role="status">This backup takes the last free place of {backups.listing?.limit}.</p>
  {:else if backupFirst}
    <p class="faint">Kept in the backups list, to restore or download.</p>
  {:else}
    <p class="danger-text">
      Without a backup, the current data{#if check.removed_raw_files.length}
        and the {counted(check.removed_raw_files.length, "raw file")} listed above{/if} will be gone for good.
    </p>
  {/if}
  {#if error}<p class="errors">{error}</p>{/if}

  {#snippet footer()}
    <button onclick={onclose}>Cancel <kbd>Esc</kbd></button>
    <button class="danger" data-autofocus onclick={confirmNow} disabled={busy || blocked}>
      {busy ? (action === "import" ? "Importing…" : "Restoring…") : confirmLabel} <kbd>Enter</kbd>
    </button>
  {/snippet}
</Modal>

<style>
  p {
    margin: 0 0 8px;
  }
  h3 {
    margin: 14px 0 6px;
    font-size: 13px;
    color: var(--muted);
  }
  code {
    font: 12px var(--mono);
    overflow-wrap: anywhere;
  }
  .files {
    margin: 0 0 8px;
    padding-left: 20px;
    max-height: 180px;
    overflow: auto;
  }
  details {
    margin: -4px 0 8px;
  }
  summary {
    cursor: pointer;
    font-size: 12px;
  }
  .backup {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 12px;
    align-items: center;
    margin-bottom: 8px;
  }
  .check {
    display: flex;
    gap: 8px;
    align-items: center;
  }
  .warning-text {
    color: var(--warning);
  }
  .danger-text,
  .errors {
    color: var(--danger);
    overflow-wrap: anywhere;
  }
</style>
