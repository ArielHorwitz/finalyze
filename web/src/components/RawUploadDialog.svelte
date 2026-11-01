<!--
  Upload raw files into an account's folder, from wherever this browser is:
  pick files and an account (or a new one), check them (each file through the
  parsers, as the dry run does), then upload those a parser reads. Replacing a
  file of the same name asks first, comparing both files' coverage.
-->
<script lang="ts">
  import { api, ApiError, type RawUpload } from "../lib/api";
  import { showDate } from "../lib/display.svelte";
  import { confirm } from "../lib/state.svelte";
  import { accepted, accountNameProblem, coverage, filesProblem, replaceConfirmation } from "../lib/upload";
  import { counted } from "../lib/archive";
  import { bind } from "../lib/use-keys.svelte";
  import Modal from "./Modal.svelte";
  import Term from "./Term.svelte";

  let {
    accounts,
    onclose,
    onuploaded,
  }: { accounts: string[]; onclose: () => void; onuploaded: (result: RawUpload) => void } = $props();

  const NEW = "\u0000new";
  // svelte-ignore state_referenced_locally
  let choice = $state(accounts[0] ?? NEW);
  let newName = $state("");
  let files = $state<File[]>([]);
  let result = $state<RawUpload | null>(null);
  let busy = $state(false);
  let error = $state<string | null>(null);

  const account = $derived(choice === NEW ? newName : choice);
  const problem = $derived(
    files.length === 0 ? "Pick files" : (filesProblem(files) ?? (choice === NEW ? accountNameProblem(newName) : null)),
  );
  // The check is of these files for this account: changing either checks again
  const checked = $derived(result !== null && result.account === account);
  const uploadable = $derived(result && checked ? accepted(result) : []);

  function picked(list: FileList | null) {
    files = [...(list ?? [])];
    result = null;
    error = null;
  }

  async function check() {
    if (busy || problem) return;
    busy = true;
    error = null;
    try {
      result = await api.uploadRaw(account, files, { dry_run: true, replace: false });
    } catch (caught) {
      result = null;
      error = caught instanceof ApiError ? caught.detail : String(caught);
    } finally {
      busy = false;
    }
  }

  async function upload() {
    if (busy || !result || !checked || uploadable.length === 0) return;
    const replace = uploadable.some((file) => file.replaces);
    if (replace) {
      const question = replaceConfirmation(uploadable, account, showDate);
      if (!(await confirm(question, { title: "Replace files?", confirmLabel: "Replace", danger: true }))) return;
    }
    busy = true;
    error = null;
    const names = new Set(uploadable.map((file) => file.name));
    try {
      const written = await api.uploadRaw(
        account,
        files.filter((file) => names.has(file.name)),
        { dry_run: false, replace },
      );
      onuploaded(written);
    } catch (caught) {
      error = caught instanceof ApiError ? caught.detail : String(caught);
    } finally {
      busy = false;
    }
  }

  function primary() {
    if (checked) upload();
    else check();
  }
</script>

<Modal title="Upload raw files" {onclose} width="760px" bindings={() => [bind("dialog.confirm", primary, { description: checked ? "Upload" : "Check", inInputs: true })]}>
  <p class="muted">
    Bank exports from this computer, into an account's folder of the <Term key="raw_folder">raw folder</Term>. Each file is checked by the parsers
    first, and nothing is ingested until you ingest.
  </p>
  <div class="fields">
    <label class="row">
      Files
      <input type="file" multiple data-autofocus onchange={(event) => picked(event.currentTarget.files)} aria-label="Raw files" />
    </label>
    <label class="row">
      Account
      <select bind:value={choice} aria-label="Account">
        {#each accounts as name}<option value={name}>{name}</option>{/each}
        <option value={NEW}>New account…</option>
      </select>
    </label>
    {#if choice === NEW}
      <label class="row">
        Name
        <input bind:value={newName} placeholder="e.g. joint" aria-label="New account" />
      </label>
    {/if}
  </div>

  {#if error}<p class="errors">{error}</p>{/if}

  {#if result && checked}
    <table class="grid" data-testid="upload-check">
      <thead><tr><th>File</th><th>Parser</th><th>Coverage</th><th></th></tr></thead>
      <tbody>
        {#each result.files as file (file.name)}
          <tr class:failed={!file.accepted}>
            <td class="name">{file.name}</td>
            <td>{#if file.accepted}{file.check.parser}{:else}<span class="danger-text">{file.check.error}</span>{/if}</td>
            <td class="muted">{#if file.accepted}{coverage(file.check, showDate)}{/if}</td>
            <td>
              {#if !file.accepted}
                <span class="badge danger">left out</span>
              {:else if file.replaces}
                <span class="badge warning" title="Now: {coverage(file.replaces, showDate)}">replaces</span>
              {:else}
                <span class="badge accent">new</span>
              {/if}
            </td>
          </tr>
          {#if file.accepted && file.replaces}
            <tr class="replaced"><td></td><td colspan="3" class="faint">replaces the file there now: {coverage(file.replaces, showDate)}</td></tr>
          {/if}
        {/each}
      </tbody>
    </table>
    <p class="muted">
      {#if result.new_account}A new account: its folder is made. {/if}
      {#if uploadable.length < result.files.length}
        {counted(result.files.length - uploadable.length, "file")} no parser reads {result.files.length - uploadable.length === 1 ? "is" : "are"} left out.
      {/if}
    </p>
  {/if}

  {#snippet footer()}
    {#if problem && files.length}<span class="errors">{problem}</span>{/if}
    <button onclick={onclose}>Cancel <kbd>Esc</kbd></button>
    {#if checked}
      <button class="primary" onclick={upload} disabled={busy || uploadable.length === 0}>
        {busy ? "Uploading…" : `Upload ${counted(uploadable.length, "file")}`} <kbd>Enter</kbd>
      </button>
    {:else}
      <button class="primary" onclick={check} disabled={busy || problem !== null}>{busy ? "Checking…" : "Check"} <kbd>Enter</kbd></button>
    {/if}
  {/snippet}
</Modal>

<style>
  p {
    margin: 4px 0 10px;
  }
  .fields {
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-bottom: 10px;
  }
  .row {
    display: grid;
    grid-template-columns: 100px 1fr;
    gap: 8px;
    align-items: center;
  }
  .errors,
  .danger-text {
    color: var(--danger);
  }
  table {
    margin: 8px 0;
    border: 1px solid var(--border);
  }
  td.name {
    font: 12px var(--mono);
  }
  tr.failed td.name {
    color: var(--danger);
  }
  tr.replaced td {
    padding-top: 0;
    font-size: 12px;
  }
</style>
