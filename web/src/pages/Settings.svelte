<!--
  Settings: the display (the date format), the dashboard's defaults, the colors of chart labels and the
  keyboard shortcuts (all in the store, saved on change), where the data is
  on this machine (the host file, read-only), the config file (saved on
  request, validated by the server, which rewrites the file without its
  comments), export and import of everything, and the backups.
-->
<script lang="ts">
  import { onMount } from "svelte";
  import Backups from "../components/Backups.svelte";
  import DateInput from "../components/DateInput.svelte";
  import ExportImport from "../components/ExportImport.svelte";
  import Modal from "../components/Modal.svelte";
  import Term from "../components/Term.svelte";
  import KeyHint from "../components/KeyHint.svelte";
  import KeymapEditor from "../components/KeymapEditor.svelte";
  import { api, type ConfigSettings, type DashboardDefaults, type DateFormat, type HostSettings, type LabelColors } from "../lib/api";
  import { Cursor, scrollToIndex } from "../lib/cursor.svelte";
  import { guardLeaving } from "../lib/router.svelte";
  import { windowName } from "../lib/dashboard";
  import { BACKUPS_SECTION_ID } from "../lib/backups.svelte";
  import { DATE_FORMATS } from "../lib/dates";
  import { display } from "../lib/display.svelte";
  import { configOf, formChanged, formOf, otherFilters, ruleSummaries, type ConfigForm } from "../lib/settings";
  import { app, confirm, refreshAccounts, refreshStatus, refreshTags, toast, toastError } from "../lib/state.svelte";
  import { bind, useKeys } from "../lib/use-keys.svelte";

  let defaults = $state<DashboardDefaults | null>(null);
  let colors = $state<LabelColors | null>(null);
  let settings = $state<ConfigSettings | null>(null);
  let hostSettings = $state<HostSettings | null>(null);
  let form = $state<ConfigForm | null>(null);
  let saved = $state<ConfigForm | null>(null);
  let saving = $state(false);
  let serverError = $state<string | null>(null);
  let configSection: HTMLElement | undefined = $state();
  let colorsContainer: HTMLElement | undefined = $state();
  let keymapEditor: KeymapEditor | undefined = $state();
  let keymapUnsaved = $state(false);

  // The dashboard's labels, then other labels that have a color
  const labels = $derived(
    colors ? [...colors.labels, ...Object.keys(colors.colors).filter((label) => !colors!.labels.includes(label))] : [],
  );
  const cursor = new Cursor(() => labels.length);
  $effect(() => scrollToIndex(colorsContainer, cursor.index));

  const checked = $derived(form && settings ? configOf(form, settings.config) : null);
  // Dates typed that aren't dates (their fields keep the last date)
  let startDateInvalid = $state(false);
  let endDateInvalid = $state(false);
  const errors = $derived([
    ...(checked?.errors ?? []),
    ...[["Start date", startDateInvalid], ["End date", endDateInvalid] as const]
      .filter(([, invalid]) => invalid)
      .map(([label]) => `${label} isn't a date (as ${display.settings.date_format})`),
  ]);
  const dirty = $derived(!!form && !!saved && formChanged(form, saved));
  const keptFilters = $derived(settings ? otherFilters(settings.config) : []);
  const keptRules = $derived(settings ? ruleSummaries(settings.config) : []);

  async function load() {
    try {
      [defaults, colors, settings, hostSettings] = await Promise.all([
        api.dashboardDefaults(),
        api.labelColors(),
        api.configSettings(),
        api.hostSettings(),
      ]);
      form = formOf(settings.config);
      saved = formOf(settings.config);
    } catch (error) {
      toastError(error, "Settings");
    }
  }

  // Display

  async function setDateFormat(dateFormat: DateFormat) {
    const before = display.settings;
    display.settings = { ...display.settings, date_format: dateFormat };
    try {
      display.settings = { ...display.settings, ...(await api.setDisplaySettings($state.snapshot(display.settings))) };
      toast("Saved the date format", "success", 1500);
    } catch (error) {
      toastError(error);
      display.settings = before;
    }
  }

  // Dashboard defaults

  // Changes show at once, and are written one after the other, each with all
  // the changes so far
  let writingDefaults = Promise.resolve();

  function setDefaults(change: Partial<DashboardDefaults>) {
    if (!defaults) return;
    defaults = { ...defaults, ...change };
    writingDefaults = writingDefaults.then(async () => {
      try {
        await api.setDashboardDefaults($state.snapshot(defaults!));
        toast("Saved the dashboard defaults", "success", 1500);
      } catch (error) {
        toastError(error);
        defaults = await api.dashboardDefaults().catch(() => defaults);
      }
    });
  }

  function stepMonths(step: number) {
    if (defaults) setDefaults({ breakdown_months: Math.max(1, (defaults.breakdown_months ?? 3) + step) });
  }

  const WINDOWS = [1, 3, 6, 12] as const;

  function cycleWindow(step: number) {
    if (!defaults) return;
    const index = WINDOWS.indexOf(defaults.rolling_months ?? 6);
    setDefaults({ rolling_months: WINDOWS[(index + step + WINDOWS.length) % WINDOWS.length] });
  }

  // Label colors

  let editing = $state<string | null>(null);
  let color = $state("");

  function openColor(label: string | undefined) {
    if (label === undefined || !colors) return;
    editing = label;
    color = colors.colors[label] ?? "";
  }

  async function saveColor(label: string, value: string | null) {
    try {
      colors = await api.setLabelColor(label, value);
      editing = null;
      toast(value ? `Colored ${label}` : `Removed the color of ${label}`, "success", 1500);
    } catch (error) {
      toastError(error);
    }
  }

  // Config file

  async function saveConfig() {
    if (!form || !checked || saving) return;
    if (errors.length) {
      toastError(errors.join("\n"), "Not saved");
      return;
    }
    saving = true;
    try {
      settings = await api.setConfig(checked.config);
      form = formOf(settings.config);
      saved = formOf(settings.config);
      serverError = null;
      toast("Saved the config file", "success");
      // Ingestion settings change what the next ingest does
      await Promise.all([refreshStatus(), refreshAccounts(), refreshTags(), load()]);
      app.dataVersion++;
    } catch (error) {
      serverError = error instanceof Error ? error.message : String(error);
    } finally {
      saving = false;
    }
  }

  async function revert() {
    if (!saved || !dirty) return;
    if (await confirm("Discard the changes to the config?", { confirmLabel: "Discard", danger: true })) {
      form = structuredClone($state.snapshot(saved));
      serverError = null;
    }
  }

  function editShortcuts() {
    keymapEditor?.focus();
  }

  function editConfig() {
    configSection?.querySelector<HTMLElement>("input, select, textarea")?.focus();
  }

  useKeys("Settings", () => [
    bind("settings.netByTag", () => defaults && setDefaults({ net_by_tag: !defaults.net_by_tag })),
    bind("settings.completeMonths", () => defaults && setDefaults({ complete_months_only: !defaults.complete_months_only })),
    bind("settings.nextWindow", () => cycleWindow(1)),
    bind("settings.previousWindow", () => cycleWindow(-1)),
    bind("settings.moreMonths", () => stepMonths(1)),
    bind("settings.fewerMonths", () => stepMonths(-1)),
    ...cursor.bindings().map((binding) => ({ ...binding, description: binding.description?.replace("row", "label color") })),
    bind("settings.editColor", () => openColor(labels[cursor.index])),
    bind("settings.removeColor", () => labels[cursor.index] !== undefined && saveColor(labels[cursor.index], null)),
    bind("settings.editConfig", editConfig),
    bind("settings.saveConfig", saveConfig),
    bind("settings.discardConfig", revert),
    bind("settings.shortcuts", editShortcuts),
  ]);

  onMount(() =>
    guardLeaving(async () => {
      if (dirty && !(await confirm("Discard the changes to the config?", { confirmLabel: "Discard", danger: true }))) return false;
      return !keymapUnsaved || (await confirm("Discard the keyboard shortcuts that clash?", { confirmLabel: "Discard", danger: true }));
    }),
  );

  onMount(() => {
    load();
    const warn = (event: BeforeUnloadEvent) => {
      if (dirty) event.preventDefault();
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  });
</script>

<div class="page">
  <div class="page-header">
    <h1>Settings</h1>
    <span class="muted summary">The date format, dashboard defaults, label colors and keyboard shortcuts are saved as you change them. The config file is saved with <KeyHint action="settings.saveConfig" />.</span>
  </div>
  <div class="content">
    <div class="sections">
      <section aria-label="Display">
        <h2>Display</h2>
        <div class="fields">
          <label class="inline">
            <Term key="date_format">Date format</Term>
            <select value={display.settings.date_format} onchange={(event) => setDateFormat(event.currentTarget.value as DateFormat)}>
              {#each DATE_FORMATS as format}<option value={format.id}>{format.title}</option>{/each}
            </select>
            <span class="faint">how dates are shown and typed (month names stay). URLs and the API keep ISO</span>
          </label>
        </div>
      </section>

      <section aria-label="Dashboard defaults">
        <h2>Dashboard defaults</h2>
        <p class="muted">What the dashboard shows until you change it there (its options in the URL win). What its figures mean is in the <a href="#/glossary">glossary</a>.</p>
        {#if defaults}
          <div class="fields">
            <label class="check">
              <input type="checkbox" checked={defaults.net_by_tag} onchange={(event) => setDefaults({ net_by_tag: event.currentTarget.checked })} />
              Net incomes and expenses by tag <KeyHint action="settings.netByTag" />
              <span class="faint">a tag is an income or an expense by its net over all the data, rather than each transaction by its sign</span>
            </label>
            <label class="check">
              <input type="checkbox" checked={defaults.complete_months_only} onchange={(event) => setDefaults({ complete_months_only: event.currentTarget.checked })} />
              Monthly figures: complete months only <KeyHint action="settings.completeMonths" />
              <span class="faint">cash flows, breakdowns, trends and averages leave out the current month until it ends. Balances always run to the latest data</span>
            </label>
            <label class="inline">
              Rolling average
              <select value={String(defaults.rolling_months ?? 6)} onchange={(event) => setDefaults({ rolling_months: Number(event.currentTarget.value) as 1 | 3 | 6 | 12 })}>
                {#each WINDOWS as window}<option value={String(window)}>{windowName(window)}</option>{/each}
              </select>
              <KeyHint action="settings.nextWindow" />
              <span class="faint">the plain average of the last months, shown and compared with</span>
            </label>
            <label class="inline">
              Months in the monthly view
              <input
                type="number"
                min="1"
                class="short"
                value={defaults.breakdown_months}
                onchange={(event) => {
                  const months = Number(event.currentTarget.value);
                  if (Number.isInteger(months) && months >= 1) setDefaults({ breakdown_months: months });
                }}
              />
              <KeyHint action="settings.moreMonths" /><KeyHint action="settings.fewerMonths" />
            </label>
          </div>
        {/if}
      </section>

      <section aria-label="Label colors">
        <h2>Label colors</h2>
        <p class="muted">
          Colors of the dashboard's balance lines. Accounts and tags have their own colors, on the
          <a href="#/accounts">Accounts</a> and <a href="#/tags">Tags</a> pages, which win over these.
        </p>
        {#if colors}
          <div class="colors" bind:this={colorsContainer}>
            <table class="grid">
              <thead><tr><th>Label</th><th>Color</th><th></th></tr></thead>
              <tbody>
                {#each labels as label, index (label)}
                  {@const own = colors.colors[label]}
                  <tr data-index={index} class:active={index === cursor.index} onmousedown={() => cursor.moveTo(index)}>
                    <td>{label}{#if !colors.labels.includes(label)} <span class="faint">(not drawn)</span>{/if}</td>
                    <td>
                      {#if own}<span class="swatch" style:background={own}></span>{/if}
                      <span class:faint={!own}>{own ?? "default"}</span>
                    </td>
                    <td class="row-actions">
                      <button class="link" onclick={() => openColor(label)}>edit</button>
                      {#if own}<button class="link" onclick={() => saveColor(label, null)}>remove</button>{/if}
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {/if}
      </section>

      <section aria-label="Where the data is">
        <h2><Term key="host_settings">Where the data is</Term></h2>
        {#if hostSettings}
          <p class="muted">
            On this machine, from <code>{hostSettings.file}</code>{#if !hostSettings.file_exists} (which doesn't exist: these are the defaults){/if}.
            Edited in that file, and used when the server starts. It belongs to the machine: exports never hold it, and imports keep it.
          </p>
          <table class="grid host">
            <tbody>
              <tr><td>Data directory</td><td><code>{hostSettings.data_dir}</code> <span class="faint">store, ingested data</span></td></tr>
              <tr><td><Term key="raw_folder">Raw folder</Term></td><td><code>{hostSettings.raw_dir}</code> <span class="faint">a folder per account</span></td></tr>
              <tr><td><Term key="backups">Backups</Term></td><td><code>{hostSettings.backups_dir}</code></td></tr>
              {#if hostSettings.host || hostSettings.port}
                <tr><td>Served on</td><td><code>{hostSettings.host ?? "127.0.0.1"}:{hostSettings.port ?? 8765}</code> <span class="faint">unless given to the command</span></td></tr>
              {/if}
            </tbody>
          </table>
        {/if}
      </section>

      <section aria-label="Config file" bind:this={configSection}>
        <h2>Config file <KeyHint action="settings.editConfig" /></h2>
        {#if settings && form && checked}
          <p class="muted">
            <code>{settings.file}</code>. Saving validates it, rewrites the file <strong>without its comments</strong> (the previous file is kept as
            <code>config.toml.bak</code>), and uses it right away. Ingestion settings apply from the next ingest. The accounts are the raw
            folder's folders (see the <a href="#/ingest">Ingest</a> page).
          </p>

          <h3>Ingestion rules</h3>
          <p class="muted">
            Drop parsed rows, change their raw descriptions or dates, or turn them into transfers to another source (e.g. the monthly card charge
            in checking). Edited in the file (<code>ingestion.rules</code>), and kept as they are here.
          </p>
          {#if keptRules.length}
            <ol>{#each keptRules as rule}<li>{rule}</li>{/each}</ol>
          {:else}
            <p class="faint">None.</p>
          {/if}

          <h3>Date range</h3>
          <p class="muted">Ingest only transactions in this range, from every account. Leave empty for no limit.</p>
          <div class="fields">
            <label class="row">From <DateInput bind:value={form.startDate} bind:invalid={startDateInvalid} aria-label="From" /> <span class="faint">inclusive</span></label>
            <label class="row">Before <DateInput bind:value={form.endDate} bind:invalid={endDateInvalid} aria-label="Before" /> <span class="faint">exclusive</span></label>
            {#if keptFilters.length}
              <p class="faint">Other ingestion filters ({keptFilters.join(", ")}) are kept as they are in the file.</p>
            {/if}
          </div>

          <div class="actions">
            {#if errors.length}
              <ul class="errors">{#each errors as error}<li>{error}</li>{/each}</ul>
            {:else if serverError}
              <ul class="errors"><li>{serverError}</li></ul>
            {/if}
            <span class="spacer"></span>
            <button onclick={revert} disabled={!dirty}>Discard changes <KeyHint action="settings.discardConfig" /></button>
            <button class="primary" onclick={saveConfig} disabled={!dirty || saving || errors.length > 0}>Save <KeyHint action="settings.saveConfig" /></button>
          </div>
        {/if}
      </section>

      <section aria-label="Export and import">
        <h2><Term key="export_import">Export and import</Term></h2>
        <ExportImport onimported={load} />
      </section>

      <section aria-label="Backups" id={BACKUPS_SECTION_ID}>
        <h2><Term key="backups">Backups</Term></h2>
        <Backups onrestored={load} />
      </section>

      <section aria-label="Keyboard shortcuts">
        <h2><Term key="keymap">Keyboard shortcuts</Term> <KeyHint action="settings.shortcuts" /></h2>
        <p class="muted">
          Every action's keys, by page. Two actions active at once (on a page, or everywhere) can't share a key, and fixed keys (Enter, Escape, Tab,
          Space, Delete, the arrows, Home, End) keep their meaning. <KeyHint action="help.open" /> on a page lists the keys there.
        </p>
        <KeymapEditor bind:this={keymapEditor} bind:unsaved={keymapUnsaved} />
      </section>
    </div>
  </div>
</div>

{#if editing !== null}
  {@const label = editing}
  <Modal title="Color of {label}" onclose={() => (editing = null)} width="400px" bindings={() => [bind("dialog.confirm", () => saveColor(label, color.trim() || null), { description: "Save", inInputs: true })]}>
    <div class="color-row">
      <input bind:value={color} placeholder="default, e.g. #3987e5" />
      <input type="color" tabindex="-1" value={/^#[0-9a-f]{6}$/i.test(color) ? color : "#888888"} oninput={(event) => (color = event.currentTarget.value)} />
      {#if color}<button class="link" onclick={() => (color = "")}>clear</button>{/if}
    </div>
    {#snippet footer()}
      <button onclick={() => (editing = null)}>Cancel</button>
      <button class="primary" onclick={() => saveColor(label, color.trim() || null)}>Save <kbd>Enter</kbd></button>
    {/snippet}
  </Modal>
{/if}

<style>
  .summary {
    font-size: 12px;
  }
  .content {
    overflow: auto;
    min-height: 0;
    flex: 1;
  }
  .sections {
    display: flex;
    flex-direction: column;
    gap: 18px;
    max-width: 900px;
    padding-bottom: 24px;
  }
  section {
    padding: 12px 16px 16px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 6px;
  }
  h2 {
    margin: 0 0 4px;
    font-size: 15px;
  }
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
  .check,
  .inline {
    display: flex;
    gap: 8px;
    align-items: center;
  }
  .check .faint {
    font-size: 12px;
  }
  .row {
    display: grid;
    grid-template-columns: 180px auto 1fr;
    gap: 8px;
    align-items: center;
  }
  .short {
    width: 80px;
  }
  .colors {
    max-height: 320px;
    overflow: auto;
  }
  table.grid td {
    padding: 4px 8px;
  }
  table.host td:first-child {
    width: 160px;
    color: var(--muted);
  }
  .row-actions {
    text-align: right;
    white-space: nowrap;
    width: 1%;
  }
  .colors .row-actions button {
    opacity: 0;
  }
  .colors tr:hover .row-actions button,
  .colors tr.active .row-actions button,
  .colors .row-actions button:focus-visible {
    opacity: 1;
  }
  .actions {
    display: flex;
    gap: 8px;
    align-items: flex-start;
    margin-top: 16px;
  }
  .errors {
    margin: 0;
    padding-left: 18px;
    color: var(--danger);
  }
  code {
    font: 12px var(--mono);
  }
  .color-row {
    display: flex;
    gap: 8px;
    align-items: center;
  }
  input[type="color"] {
    width: 34px;
    height: 26px;
    padding: 1px;
  }
</style>
