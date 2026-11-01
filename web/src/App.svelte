<!--
  The app shell: a header with the pages and the data's status, the current
  page, and the app-wide dialogs (help, confirmations) and toasts.
-->
<script lang="ts">
  import { onMount } from "svelte";
  import ConfirmDialog from "./components/ConfirmDialog.svelte";
  import HelpOverlay from "./components/HelpOverlay.svelte";
  import AgentPopover from "./components/AgentPopover.svelte";
  import IngestDialog from "./components/IngestDialog.svelte";
  import ServerBanner from "./components/ServerBanner.svelte";
  import Term from "./components/Term.svelte";
  import TermTooltip from "./components/TermTooltip.svelte";
  import { api, type IngestSummary, type ServerInfo, type Status } from "./lib/api";
  import { isNativeControl, isTextInput, releaseSelectsAfterPicking, watchLayout } from "./lib/keys";
  import { navigate, router } from "./lib/router.svelte";
  import { ROUTES, splitTitle } from "./lib/routes";
  import {
    app,
    dialogs,
    goKey,
    keyboard,
    keymap,
    loadDisplay,
    loadKeymap,
    ownKeys,
    refreshAccounts,
    refreshStatus,
    refreshTags,
    titled,
    toastError,
  } from "./lib/state.svelte";
  import { loadGlossary } from "./lib/glossary.svelte";
  import { ago } from "./lib/time";
  import { showTime } from "./lib/display.svelte";
  import { bind, useKeys } from "./lib/use-keys.svelte";
  import Accounts from "./pages/Accounts.svelte";
  import Annotate from "./pages/Annotate.svelte";
  import Glossary from "./pages/Glossary.svelte";
  import Ingest from "./pages/Ingest.svelte";
  import Orphans from "./pages/Orphans.svelte";
  import Rules from "./pages/Rules.svelte";
  import Settings from "./pages/Settings.svelte";
  import Tags from "./pages/Tags.svelte";
  import Transactions from "./pages/Transactions.svelte";

  const PAGES = {
    annotate: Annotate,
    transactions: Transactions,
    ingest: Ingest,
    tags: Tags,
    rules: Rules,
    accounts: Accounts,
    orphans: Orphans,
    settings: Settings,
    glossary: Glossary,
  } as const;
  const Page = $derived(PAGES[router.path as keyof typeof PAGES]);
  // The dashboard (and its charts library) loads only when opened
  const loadDashboard = () => import("./pages/Dashboard.svelte");

  let server: ServerInfo | null = $state(null);
  let ingesting = $state(false);
  let ingestResult: IngestSummary | null = $state(null);

  async function ingest() {
    if (ingesting) return;
    ingesting = true;
    try {
      ingestResult = await api.ingest();
      await Promise.all([refreshStatus(), refreshTags(), refreshAccounts()]);
      app.dataVersion++; // pages reload their data
    } catch (error) {
      toastError(error, "Ingest failed");
    } finally {
      ingesting = false;
    }
  }

  function onkeydown(event: KeyboardEvent) {
    // Keys that a component already handled (e.g. a dropdown's arrows), or
    // before the keymap is known (they would be the defaults')
    if (event.defaultPrevented || !keymap.loaded) return;
    const focused = document.activeElement;
    if ((event.key === "Enter" || event.key === " ") && !event.ctrlKey && isNativeControl(focused)) return;
    keyboard.handle(event, isTextInput(focused));
  }

  useKeys("App", () => [
    bind("help.open", () => (app.helpOpen = true)),
    ...ROUTES.map((route) => bind(`go.${route.path}`, () => navigate(route.path))),
    bind("app.ingest", ingest),
    bind("app.leaveField", () => {
      if (isTextInput(document.activeElement)) (document.activeElement as HTMLElement).blur();
    }),
  ]);

  // The app's dialogs that allow navigating close on the way
  $effect(() => {
    router.path;
    app.helpOpen = false;
    ingestResult = null;
  });

  onMount(() => {
    const stopReleasingSelects = releaseSelectsAfterPicking(window);
    const stopWatchingLayout = watchLayout(window);
    loadKeymap();
    loadDisplay();
    refreshStatus();
    loadGlossary();
    api.server().then(
      (info) => (server = info),
      // An older server without this endpoint: nothing to say
      () => {},
    );
    refreshTags().catch((error) => toastError(error, "Tags"));
    refreshAccounts().catch((error) => toastError(error, "Accounts"));
    return () => {
      stopReleasingSelects();
      stopWatchingLayout();
    };
  });

  /** The freshness hint's hover text: when the data changed, and was checked. */
  function freshnessTitle(status: Status): string {
    if (!status.ingested_changed_at || !status.ingested_checked_at) return "Never ingested";
    return `Ingested data last changed ${showTime(status.ingested_changed_at)}. Last checked (an ingest ran) ${showTime(status.ingested_checked_at)}, ${ago(status.ingested_checked_at)}.`;
  }
</script>

<svelte:window {onkeydown} />

<div class="shell">
  <header>
    <span class="brand">finalyze</span>
    {#if server?.demo}
      <span class="demo-badge" title="Invented demo data (served with --demo): changes are lost when the server stops">Demo</span>
    {/if}
    <nav class:going={app.pendingKeys === goKey()}>
      {#each ROUTES as route, index}
        {@const key = ownKeys(`go.${route.path}`)[0]}
        {@const [before, letter, after] = splitTitle(route, key)}
        {#if index > 0 && route.group !== ROUTES[index - 1].group}<span class="divider" aria-hidden="true"></span>{/if}
        <a href="#/{route.path}" class:current={router.path === route.path} title={key ? `${goKey()} then ${key}` : undefined}>
          {before}<u>{letter}</u>{after}{#if key && !letter}<u class="key">{key}</u>{/if}
        </a>
      {/each}
    </nav>
    <div class="spacer"></div>
    <!-- With the keymap too: once shown, keys are the keymap's -->
    {#if app.status && keymap.loaded}
      {@const status = app.status}
      <div class="counts">
        <a href="#/annotate" class="count" class:attention={status.untagged > 0} title="Untagged transactions">
          <Term key="untagged" quiet><strong>{status.untagged}</strong> untagged</Term>
        </a>
        <a href="#/annotate?review" class="count" class:attention={status.needs_review > 0} title="Annotations flagged for review">
          <Term key="needs_review" quiet><strong>{status.needs_review}</strong> to review</Term>
        </a>
        <a href="#/orphans" class="count" class:attention={status.orphans > 0} title="Annotations of missing transactions">
          <Term key="orphan" quiet><strong>{status.orphans}</strong> orphans</Term>
        </a>
        <span class="freshness" data-testid="freshness" title={freshnessTitle(status)}>
          {status.ingested_changed_at ? `ingested ${ago(status.ingested_changed_at)}` : "never ingested"}
        </span>
      </div>
    {/if}
    <AgentPopover />
    <button class="outline help" onclick={() => (app.helpOpen = true)} title={titled("Keyboard shortcuts", "help.open")} aria-label="Keyboard shortcuts">?</button>
  </header>
  <ServerBanner info={server} />

  <main>
    {#key router.path}
      {#if router.path === "dashboard"}
        {#await loadDashboard() then module}
          <module.default />
        {:catch error}
          <div class="empty">The dashboard failed to load: {error}</div>
        {/await}
      {:else}
        <Page />
      {/if}
    {/key}
  </main>

  {#if app.pendingKeys}
    <div class="pending">
      <kbd>{app.pendingKeys}</kbd> …
      {#if app.pendingKeys === goKey()}<span class="faint">then a page's underlined letter, <kbd>Esc</kbd> cancels</span>{/if}
    </div>
  {/if}

  <div class="toasts" aria-live="polite">
    {#each app.toasts as item (item.id)}
      <div class="toast {item.kind}">{item.message}</div>
    {/each}
  </div>
</div>

{#if app.helpOpen}
  <HelpOverlay onclose={() => (app.helpOpen = false)} />
{/if}
{#if ingestResult}
  <IngestDialog result={ingestResult} onclose={() => (ingestResult = null)} />
{/if}
{#if dialogs.confirm}
  <ConfirmDialog request={dialogs.confirm} />
{/if}
<TermTooltip />

<style>
  .shell {
    display: flex;
    flex-direction: column;
    height: 100%;
  }
  header {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0 16px;
    padding: 0 16px;
    /* Narrow windows wrap it rather than scroll sideways */
    min-height: 42px;
    flex-shrink: 0;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
  }
  .brand {
    font-weight: 700;
    letter-spacing: 0.02em;
    color: var(--accent);
  }
  .demo-badge {
    margin-left: -10px;
    padding: 1px 7px;
    border: 1px solid var(--accent);
    border-radius: 999px;
    font-size: 11px;
    font-weight: 600;
    color: var(--accent);
    background: var(--accent-soft);
  }
  nav {
    display: flex;
    align-items: center;
    gap: 2px;
  }
  /* Between the groups of pages: see, routine, setup, reference */
  nav .divider {
    width: 1px;
    height: 16px;
    margin: 0 8px;
    background: var(--border-strong);
  }
  nav a {
    padding: 5px 10px;
    border-radius: var(--radius);
    color: var(--muted);
    text-decoration: none;
  }
  nav a:hover {
    color: var(--text);
    background: var(--surface-2);
  }
  nav a.current {
    color: var(--text);
    background: var(--surface-3);
  }
  nav u {
    text-decoration-color: var(--faint);
    text-underline-offset: 3px;
  }
  /* A letter that isn't in the title, after it */
  nav u.key {
    margin-left: 4px;
    font-size: 11px;
    color: var(--faint);
  }
  /* After "g": the letters that go on */
  nav.going u {
    color: var(--accent);
    text-decoration-color: var(--accent);
  }
  .spacer {
    flex: 1;
  }
  .counts {
    display: flex;
    align-items: center;
    gap: 14px;
    color: var(--muted);
    font-size: 12px;
  }
  .count {
    color: var(--muted);
    text-decoration: none;
  }
  .count strong {
    color: var(--text);
    font-weight: 600;
    font-variant-numeric: tabular-nums;
  }
  .count.attention strong {
    color: var(--warning);
  }
  .count:hover {
    color: var(--text);
  }
  .freshness {
    color: var(--faint);
  }
  .help {
    width: 26px;
    padding-left: 0;
    padding-right: 0;
  }
  main {
    flex: 1;
    min-height: 0;
  }
  .pending {
    position: fixed;
    bottom: 14px;
    left: 16px;
    z-index: 200;
    padding: 4px 10px;
    border-radius: var(--radius);
    background: var(--surface-3);
    border: 1px solid var(--border-strong);
    color: var(--muted);
  }
  .toasts {
    position: fixed;
    right: 16px;
    bottom: 16px;
    z-index: 200;
    display: flex;
    flex-direction: column;
    gap: 8px;
    max-width: 420px;
  }
  .toast {
    padding: 8px 12px;
    border-radius: 6px;
    background: var(--surface-3);
    border: 1px solid var(--border-strong);
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.4);
    white-space: pre-line;
  }
  .toast.success {
    border-color: rgba(92, 197, 150, 0.5);
  }
  .toast.error {
    border-color: var(--danger);
    color: #f6b4b4;
  }
</style>
