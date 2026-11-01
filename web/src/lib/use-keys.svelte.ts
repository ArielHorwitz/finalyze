import { onMount } from "svelte";
import type { Binding } from "./keys";
import { ACTIONS_BY_ID } from "./keymap";
import { keyboard, keysOf } from "./state.svelte";

/**
 * Register a component's keyboard shortcuts while it is mounted.
 *
 * `bindings` is a function, so the bindings can depend on the component's
 * current state (and the keymap). A `modal` layer (a dialog) hides the
 * shortcuts below it, except for the page navigation if it `allowsNavigation`.
 */
export function useKeys(name: string, bindings: () => Binding[], { modal = false, allowsNavigation = false } = {}) {
  onMount(() =>
    keyboard.push({
      name,
      modal,
      allowsNavigation,
      get bindings() {
        return bindings();
      },
    }),
  );
}

/**
 * A binding of an action of the registry (keymap.ts), with its keys in the
 * keymap. The description is the action's unless given (e.g. a dialog's).
 */
export function bind(
  action: string,
  run: (event: KeyboardEvent) => void,
  { description, inInputs }: { description?: string; inInputs?: boolean } = {},
): Binding {
  const known = ACTIONS_BY_ID[action];
  if (!known) throw new Error(`Unknown action ${action}`);
  return {
    action,
    keys: keysOf(action),
    description: description ?? known.description,
    inInputs: inInputs ?? known.inInputs,
    navigation: known.navigation,
    run,
  };
}
