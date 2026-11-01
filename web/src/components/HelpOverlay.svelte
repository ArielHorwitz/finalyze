<!-- Lists the shortcuts of everything on screen: the page's, then the app's. -->
<script lang="ts">
  import { activeLayers, type Layer } from "../lib/keys";
  import { keyboard, keysOf } from "../lib/state.svelte";
  import Modal from "./Modal.svelte";

  let { onclose }: { onclose: () => void } = $props();

  // Read once when opening: the layers below this dialog, with each action
  // once (the topmost, which takes its keys) and only the bound ones
  const shown = new Set<string>();
  const layers: Layer[] = activeLayers(keyboard.layers)
    .map((layer) => ({
      ...layer,
      bindings: layer.bindings.filter((binding) => {
        if (!binding.description || binding.keys.length === 0 || (binding.action && shown.has(binding.action))) return false;
        if (binding.action) shown.add(binding.action);
        return true;
      }),
    }))
    .filter((layer) => layer.bindings.length > 0);

  const glossaryKeys = keysOf("go.glossary")[0]?.split(" ") ?? [];

  /** "Ctrl+Enter" as keycaps, and "g t" as a sequence. */
  function keycaps(key: string): string[][] {
    return key.split(" ").map((step) => (step === "+" ? ["+"] : step.split(/\+(?!$)/)));
  }
</script>

<Modal title="Keyboard shortcuts" {onclose} width="760px" allowsNavigation>
  <div class="columns">
    {#each layers as layer (layer.name)}
      <section>
        <h3>{layer.name}</h3>
        <dl>
          {#each layer.bindings as binding}
            <dt>
              {#each binding.keys as key, keyIndex}
                {#if keyIndex > 0}<span class="or">/</span>{/if}
                {#each keycaps(key) as step, stepIndex}
                  {#if stepIndex > 0}<span class="then">then</span>{/if}
                  {#each step as cap, capIndex}{#if capIndex > 0}+{/if}<kbd>{cap}</kbd>{/each}
                {/each}
              {/each}
            </dt>
            <dd>{binding.description}</dd>
          {/each}
        </dl>
      </section>
    {/each}
  </div>
  <p class="faint hint">
    Mouse works too: click rows, fields and buttons, and hover a label with a dotted underline for what it means. The
    <a href="#/glossary">glossary</a>{#if glossaryKeys.length}
      (<kbd>{glossaryKeys[0]}</kbd> <kbd>{glossaryKeys[1]}</kbd>, the nav's last tab){/if} explains every figure and label. Change the keys in
    <a href="#/settings">Settings</a>.
  </p>
</Modal>

<style>
  .columns {
    columns: 2;
    column-gap: 28px;
  }
  section {
    break-inside: avoid;
    margin-bottom: 14px;
  }
  h3 {
    margin: 0 0 6px;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--muted);
    font-weight: 500;
  }
  dl {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 4px 12px;
    margin: 0;
  }
  dt {
    white-space: nowrap;
    text-align: right;
  }
  dd {
    margin: 0;
    color: var(--text);
  }
  .or,
  .then {
    color: var(--faint);
    margin: 0 3px;
    font-size: 11px;
  }
  .hint {
    margin: 4px 0 0;
  }
</style>
