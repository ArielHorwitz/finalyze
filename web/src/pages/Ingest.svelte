<!--
  Ingest: a dry run of every raw file (parser, rows, dates, sum), likely
  problems, and a preview of what ingesting would change, before ingesting.
  Checking reads the raw files only: nothing is written until "Ingest". The
  raw folder's path is shown to copy, and raw files can be uploaded into it.
-->
<script lang="ts">
  import { onMount } from "svelte";
  import Amount from "../components/Amount.svelte";
  import DescriptionText from "../components/DescriptionText.svelte";
  import RawUploadDialog from "../components/RawUploadDialog.svelte";
  import TransactionDescription from "../components/TransactionDescription.svelte";
  import { api, ApiError, type IngestDryRun, type IngestPreview, type IngestProblem, type IngestRow, type IngestSummary, type RawUpload, type StoredAnnotation } from "../lib/api";
  import { counted } from "../lib/archive";
  import { app, confirm, keyName, refreshAccounts, refreshStatus, refreshTags, toast, toastError } from "../lib/state.svelte";
  import { bind, useKeys } from "../lib/use-keys.svelte";
  import KeyHint from "../components/KeyHint.svelte";
  import Term from "../components/Term.svelte";
  import { ago } from "../lib/time";
  import { showDate, showTime } from "../lib/display.svelte";

  const PROBLEM_LABELS: Record<IngestProblem["kind"], string> = {
    no_files: "no files",
    unparseable: "unparseable",
    empty: "empty",
    filtered_out: "filtered out",
    overlap: "duplicates",
    missing_account: "no folder",
    stray_file: "stray file",
  };
  const SERIOUS: IngestProblem["kind"][] = ["unparseable", "missing_account"];

  let dryRun = $state<IngestDryRun | null>(null);
  let preview = $state<IngestPreview | null>(null);
  /** Why the ingest would fail, if it would. */
  let previewError = $state<string | null>(null);
  let checking = $state(false);
  let ingesting = $state(false);
  let result = $state<IngestSummary | null>(null);
  let uploading = $state(false);
  let rawDirField: HTMLInputElement | undefined = $state();

  const problemsByPath = $derived.by(() => {
    const byPath = new Map<string, IngestProblem[]>();
    for (const problem of dryRun?.problems ?? []) {
      for (const path of problem.paths) {
        const known = byPath.get(path) ?? [];
        if (!known.some((other) => other.kind === problem.kind)) byPath.set(path, [...known, problem]);
      }
    }
    return byPath;
  });
  const benignOverlaps = $derived((dryRun?.overlaps ?? []).filter((overlap) => overlap.shared_rows === 0));
  const changes = $derived(preview ? preview.new.length + preview.vanished.length + preview.changed.length : 0);

  async function check() {
    checking = true;
    try {
      const [dryRunResult, previewResult] = await Promise.allSettled([api.ingestDryRun(), api.previewIngest()]);
      if (dryRunResult.status === "fulfilled") dryRun = dryRunResult.value;
      else toastError(dryRunResult.reason, "Dry run");
      if (previewResult.status === "fulfilled") {
        preview = previewResult.value;
        previewError = null;
      } else {
        preview = null;
        const error = previewResult.reason;
        previewError = error instanceof ApiError ? error.detail : String(error);
      }
    } finally {
      checking = false;
    }
  }

  async function ingest() {
    if (ingesting || checking) return;
    const orphaned = preview?.new_orphans.length ?? 0;
    if (orphaned > 0) {
      const confirmed = await confirm(
        `${orphaned} annotation${orphaned === 1 ? "" : "s"} would be orphaned: their transactions vanish from the raw files. Orphans are kept until you delete them. Ingest anyway?`,
        { title: "Ingest", confirmLabel: "Ingest", danger: true },
      );
      if (!confirmed) return;
    }
    ingesting = true;
    try {
      result = await api.ingest();
      await Promise.all([refreshStatus(), refreshTags(), refreshAccounts()]);
      app.dataVersion++; // pages reload their data, this one included
    } catch (error) {
      toastError(error, "Ingest failed");
    } finally {
      ingesting = false;
    }
  }

  function uploaded(written: RawUpload) {
    uploading = false;
    toast(`Uploaded ${counted(written.files.length, "file")} to ${written.account}. Their effect is below: ingest to apply it.`, "success", 6000);
    check();
  }

  async function copyRawDir() {
    if (!dryRun) return;
    try {
      await navigator.clipboard.writeText(dryRun.raw_dir);
    } catch {
      // Without clipboard access (e.g. not a secure context), copy the selection
      rawDirField?.select();
      document.execCommand("copy");
    }
    toast("Copied the raw folder's path", "success", 1500);
  }

  useKeys("Ingest", () => [
    bind("ingest.run", ingest),
    bind("ingest.upload", () => (uploading = true)),
    // The app's ingest key ingests here with the preview
    bind("app.ingest", ingest, { description: "Ingest the raw files" }),
    bind("ingest.check", check),
  ]);

  onMount(check);

  let seenVersion = app.dataVersion;
  $effect(() => {
    if (app.dataVersion !== seenVersion) {
      seenVersion = app.dataVersion;
      check();
    }
  });

  function formatSize(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} kB`;
    return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  }

  const fileName = (path: string) => path.split("/").pop() ?? path;
</script>

{#snippet rowsTable(rows: IngestRow[])}
  <table class="grid">
    <thead>
      <tr><th>Date</th><th>Account</th><th>Raw description</th><th class="amount">Amount</th></tr>
    </thead>
    <tbody>
      {#each rows as row (row.id)}
        <tr>
          <td class="muted">{showDate(row.date)}</td>
          <td class="muted">{row.account}&nbsp;<span class="faint">· {row.source}</span></td>
          <td dir="auto">{row.raw_description}</td>
          <td class="amount"><Amount value={row.amount} /></td>
        </tr>
      {/each}
    </tbody>
  </table>
{/snippet}

{#snippet orphansTable(annotations: StoredAnnotation[])}
  <table class="grid orphans">
    <thead>
      <tr><th>Date</th><th>Account</th><th><Term key="description" context="Without one, the raw description, in italics">Description</Term></th><th class="amount">Amount</th><th>Tags</th><th></th></tr>
    </thead>
    <tbody>
      {#each annotations as annotation (annotation.transaction_id)}
        <tr>
          <td class="muted">{showDate(annotation.snapshot.date)}</td>
          <td class="muted">{annotation.snapshot.account}&nbsp;<span class="faint">· {annotation.snapshot.source}</span></td>
          <td><TransactionDescription description={annotation.description} raw={annotation.snapshot.raw_description} /></td>
          <td class="amount"><Amount value={annotation.snapshot.amount} /></td>
          <td>
            {#each annotation.parts as part}
              <div>
                {part.tag} / {part.subtag}
                {#if annotation.parts.length > 1}<span class="faint">(<Amount value={part.amount} />)</span>{/if}
                {#if part.description}<span class="muted">— <DescriptionText description={part.description} /></span>{/if}
              </div>
            {:else}
              <span class="faint">description only</span>
            {/each}
          </td>
          <td>
            {#if annotation.rule}<span class="badge" title="A rule tags this transaction">rule</span>{/if}
            {#if annotation.needs_review}<span class="badge warning">review</span>{/if}
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
{/snippet}

<div class="page">
  <div class="page-header">
    <h1>Ingest</h1>
    <span class="muted summary">
      {#if dryRun}
        {#if dryRun.ingested_changed_at}
          Ingested data last changed {showTime(dryRun.ingested_changed_at)} ({ago(dryRun.ingested_changed_at)}).
        {:else}
          Never ingested.
        {/if}
      {/if}
      Checking reads the raw files only. Nothing is written until you ingest.
    </span>
    <div class="spacer"></div>
    <button onclick={() => (uploading = true)} title={keyName("ingest.upload")}>Upload files… <KeyHint action="ingest.upload" /></button>
    <button onclick={check} disabled={checking} title={keyName("ingest.check")}>{checking ? "Checking…" : "Check again"}</button>
    <button class="primary" onclick={ingest} disabled={ingesting || checking || previewError !== null} title={[keyName("ingest.run"), keyName("app.ingest")].filter(Boolean).join(" or ")}>
      {ingesting ? "Ingesting…" : "Ingest"} <KeyHint action="ingest.run" />
    </button>
  </div>

  <div class="scroll" class:stale={checking}>
    {#if result}
      <section class="result" data-testid="ingest-result">
        <h2>Ingested</h2>
        <p>
          <strong>{result.transactions}</strong> transactions
          ({Object.entries(result.accounts).map(([account, count]) => `${account} ${count}`).join(", ")}):
          <strong>{result.new_ids.length}</strong> new, <strong>{result.vanished_ids.length}</strong> vanished{#if result.changed_ids.length}, <strong>{result.changed_ids.length}</strong> changed{/if}.
          {#if result.new_orphan_ids.length}
            <span class="warning-text"><strong>{result.new_orphan_ids.length}</strong> new orphans</span> (see <a href="#/orphans">Orphans</a>).
          {:else}
            No new orphans.
          {/if}
        </p>
        <p class="muted" data-testid="ingest-times">
          {#if result.data_changed}
            The ingested data changed {showTime(result.ingested_checked_at)}.
          {:else}
            Nothing changed: the ingested data is as it was{#if result.ingested_changed_at}, last changed {showTime(result.ingested_changed_at)} ({ago(result.ingested_changed_at)}){/if}. Checked {showTime(result.ingested_checked_at)}.
          {/if}
        </p>
      </section>
    {/if}

    {#if !dryRun && checking}
      <div class="empty">Checking the raw files…</div>
    {/if}

    {#if dryRun && dryRun.problems.length}
      <section data-testid="ingest-problems">
        <h2><Term key="ingest_problems">Likely problems</Term> <span class="faint">({dryRun.problems.length})</span></h2>
        <ul class="problems">
          {#each dryRun.problems as problem}
            <li>
              <span class="badge {SERIOUS.includes(problem.kind) ? 'danger' : 'warning'}"><Term key="ingest_problems" quiet>{PROBLEM_LABELS[problem.kind]}</Term></span>
              {#if problem.account !== null}<span class="muted">{problem.account}:</span>{/if}
              {#if problem.paths.length}<span class="paths">{problem.paths.map(fileName).join(" and ")}</span>{/if}
              <span class="muted message">{problem.message}</span>
            </li>
          {/each}
        </ul>
      </section>
    {/if}

    <section data-testid="ingest-preview">
      <h2><Term key="ingest_preview">Effect of ingesting</Term></h2>
      {#if previewError}
        <p class="error-box">{previewError}</p>
      {:else if preview}
        <p>
          <Term key="ingest_preview"><strong>{preview.new.length}</strong> new, <strong>{preview.vanished.length}</strong> vanished,
          {#if preview.changed.length}<strong>{preview.changed.length}</strong> changed,{/if}
          <strong>{preview.unchanged}</strong> unchanged transactions</Term>.
          {#if changes === 0}<span class="muted">Ingesting would change nothing.</span>{/if}
        </p>
        <table class="grid counts">
          <thead>
            <tr><th><Term key="account">Account</Term></th><th class="num"><Term key="ingest_preview">Now</Term></th><th class="num"><Term key="ingest_preview">After</Term></th><th class="num"><Term key="ingest_preview">Change</Term></th></tr>
          </thead>
          <tbody>
            {#each Object.entries(preview.accounts) as [account, counts]}
              <tr>
                <td>{account}</td>
                <td class="num">{counts.before}</td>
                <td class="num">{counts.after}</td>
                <td class="num" class:muted={counts.after === counts.before}>
                  {counts.after === counts.before ? "–" : counts.after > counts.before ? `+${counts.after - counts.before}` : `−${counts.before - counts.after}`}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
        {#if preview.new_orphans.length}
          <h3 class="warning-text"><Term key="orphan">Annotations that would be orphaned</Term> ({preview.new_orphans.length})</h3>
          <p class="muted explain">Their transactions are no longer in the raw files (or changed, which changes their ID). Orphans are kept until you delete them.</p>
          {@render orphansTable(preview.new_orphans)}
        {/if}
        {#if preview.restored_orphan_ids.length}
          <p class="muted">{preview.restored_orphan_ids.length} orphaned annotations would get their transaction back.</p>
        {/if}
        {#if preview.new.length}
          <details>
            <summary>New transactions ({preview.new.length})</summary>
            {@render rowsTable(preview.new)}
          </details>
        {/if}
        {#if preview.vanished.length}
          <details>
            <summary>Vanished transactions ({preview.vanished.length})</summary>
            {@render rowsTable(preview.vanished)}
          </details>
        {/if}
        {#if preview.changed.length}
          <details>
            <summary>Changed transactions ({preview.changed.length}, as they would be)</summary>
            {@render rowsTable(preview.changed)}
          </details>
        {/if}
      {:else if checking}
        <p class="muted">Parsing…</p>
      {/if}
    </section>

    {#if dryRun}
      <section>
        <h2><Term key="raw_folder">Raw files</Term></h2>
        <div class="raw-dir">
          <span class="muted">Raw folder, a folder per account:</span>
          <input readonly value={dryRun.raw_dir} bind:this={rawDirField} aria-label="Raw folder" size={Math.min(80, dryRun.raw_dir.length + 2)} />
          <button onclick={copyRawDir}>Copy</button>
        </div>
        {#if !dryRun.raw_dir_exists}
          <p class="muted">It doesn't exist yet: make it with a folder for each account holding its bank exports, or upload files for a new account.</p>
        {:else if dryRun.accounts.length === 0}
          <p class="muted">No accounts yet: make a folder for each account in it, holding its bank exports, or upload files for a new account.</p>
        {/if}
        <table class="grid files">
          <thead>
            <tr>
              <th>File</th><th>Parser</th><th class="num">Rows</th><th>From</th><th>Until</th><th class="amount">Sum</th><th class="num">Size</th><th>Modified</th><th></th>
            </tr>
          </thead>
          {#each dryRun.accounts as account (account.name)}
            <tbody>
              <tr class="account">
                <td colspan="9">
                  <strong>{account.name}</strong>
                  <span class="path">{account.folder}/</span>
                </td>
              </tr>
              {#each account.files as file (file.path)}
                {@const problems = problemsByPath.get(file.path) ?? []}
                <tr class:failed={file.error !== null}>
                  <td class="name" title={file.path}>{file.name}</td>
                  <td>
                    {#if file.parser}
                      {file.parser}
                    {:else}
                      <details class="errors">
                        <summary class="danger-text">{file.error}</summary>
                        <ul>
                          {#each Object.entries(file.parser_errors) as [parser, error]}
                            <li><span class="muted">{parser}:</span> {error}</li>
                          {/each}
                        </ul>
                      </details>
                    {/if}
                  </td>
                  <td class="num">
                    {#if file.kept_rows !== file.rows}<span class="faint" title="Kept by the ingestion rules and filters, of all rows">{file.kept_rows} of</span>{/if}
                    {file.rows}
                    {#if file.transfer_rows}<span class="faint" title="The other legs of transfers, which ingestion rules add in other sources">+{file.transfer_rows}</span>{/if}
                  </td>
                  <td class="muted">{file.start_date ? showDate(file.start_date) : ""}</td>
                  <td class="muted">{file.end_date ? showDate(file.end_date) : ""}</td>
                  <td class="amount">{#if file.parser}<Amount value={file.total} />{/if}</td>
                  <td class="num muted">{formatSize(file.size)}</td>
                  <td class="muted">{showTime(file.modified_at)}</td>
                  <td>
                    {#if file.newer_than_ingest}<span class="badge accent" title="Modified after the last ingest">new</span>{/if}
                    {#each problems as problem}
                      <span class="badge {SERIOUS.includes(problem.kind) ? 'danger' : 'warning'}" title={problem.message}>{PROBLEM_LABELS[problem.kind]}</span>
                    {/each}
                  </td>
                </tr>
              {:else}
                <tr><td colspan="9" class="faint">No files</td></tr>
              {/each}
            </tbody>
          {/each}
        </table>
        {#if benignOverlaps.length}
          <details class="overlaps">
            <summary class="muted">
              {benignOverlaps.length} date overlaps without shared rows, not flagged (e.g. two cards of one account, or statements spanning months)
            </summary>
            <ul>
              {#each benignOverlaps as overlap}
                <li>
                  <span class="muted">{overlap.account} · {overlap.source}:</span>
                  {fileName(overlap.files[0])} and {fileName(overlap.files[1])},
                  <span class="muted">{showDate(overlap.start_date)} to {showDate(overlap.end_date)}</span>
                </li>
              {/each}
            </ul>
          </details>
        {/if}
      </section>
    {/if}
  </div>
</div>

{#if uploading}
  <RawUploadDialog accounts={dryRun?.accounts.map((account) => account.name) ?? []} onclose={() => (uploading = false)} onuploaded={uploaded} />
{/if}

<style>
  .summary {
    font-size: 12px;
  }
  .scroll {
    padding-bottom: 16px;
  }
  /* Results of the previous check, while checking again */
  .stale section:not(.result) {
    opacity: 0.55;
  }
  section {
    margin: 0 0 6px;
  }
  section > :is(p, ul, details, h3) {
    margin-left: 12px;
    margin-right: 12px;
  }
  h2 {
    font-size: 13px;
    font-weight: 600;
    margin: 16px 12px 6px;
  }
  h3 {
    font-size: 12px;
    font-weight: 600;
    margin-top: 14px;
    margin-bottom: 4px;
  }
  .explain {
    font-size: 12px;
    margin-top: 0;
  }
  .result {
    background: var(--accent-soft);
    border-bottom: 1px solid var(--border);
    padding: 2px 0 4px;
  }
  .problems {
    list-style: none;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .problems .paths {
    font-family: var(--mono);
    font-size: 12px;
  }
  .message {
    margin-left: 6px;
  }
  .error-box {
    padding: 8px 10px;
    border: 1px solid var(--danger);
    border-radius: var(--radius);
    background: var(--danger-soft);
    white-space: pre-wrap;
    font-family: var(--mono);
    font-size: 12px;
  }
  .warning-text {
    color: var(--warning);
  }
  .danger-text {
    color: var(--danger);
  }
  table.counts {
    width: auto;
    min-width: 360px;
    margin: 0 12px 8px;
    border: 1px solid var(--border);
  }
  details summary {
    cursor: pointer;
    margin: 6px 0;
  }
  details > table.grid {
    margin-bottom: 8px;
    border: 1px solid var(--border);
  }
  table.files tr.account > td {
    background: var(--surface-2);
    padding-top: 6px;
  }
  .path {
    margin-left: 10px;
    font-family: var(--mono);
    font-size: 11px;
    color: var(--faint);
  }
  .raw-dir {
    display: flex;
    gap: 8px;
    align-items: center;
    margin: 0 12px 8px;
  }
  .raw-dir input {
    font: 12px var(--mono);
    max-width: 100%;
  }
  td.name {
    font-family: var(--mono);
    font-size: 12px;
  }
  tr.failed td.name {
    color: var(--danger);
  }
  details.errors summary {
    margin: 0;
  }
  details.errors ul {
    margin: 4px 0;
    padding-left: 16px;
    font-size: 12px;
  }
  .overlaps ul {
    font-size: 12px;
    padding-left: 16px;
  }
  .badge + .badge {
    margin-left: 4px;
  }
</style>
