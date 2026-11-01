// Keyboard shortcuts: matching key presses to bindings.
//
// Bindings live in layers: the app's global keys at the bottom, the current
// page's above them, and dialogs on top. A key goes to the topmost layer that
// binds it. A "modal" layer (a dialog) hides the layers below it, except for
// the page navigation keys if it allows them (e.g. a read-only panel).
//
// Keys are written like "j", "J" (shift+j), "?", "Enter", "Ctrl+Enter",
// "Ctrl+Shift+Enter", "Space", or sequences like "g t" (g, then t).

export interface Binding {
  /** The action of the registry (keymap.ts) it binds, e.g. "tags.rename". */
  action?: string;
  keys: string[];
  /** Shown in the help overlay. Bindings without one are hidden. */
  description?: string;
  run: (event: KeyboardEvent) => void;
  /** Also active while typing in an input (e.g. Ctrl+Enter, Escape). */
  inInputs?: boolean;
  /** Goes to another page: also active under modal layers that allow it. */
  navigation?: boolean;
}

export interface Layer {
  name: string;
  bindings: Binding[];
  /** Hide the layers below, e.g. for a dialog. */
  modal?: boolean;
  /** For a modal layer: keep the navigation bindings of the layers below. */
  allowsNavigation?: boolean;
}

export interface KeyLike {
  key: string;
  /** The physical key, e.g. "KeyG" (absent in some tests). */
  code?: string;
  ctrlKey: boolean;
  metaKey: boolean;
  altKey: boolean;
  shiftKey: boolean;
}

const KEY_NAMES: Record<string, string> = { " ": "Space", Esc: "Escape" };

/**
 * The name of a key press, e.g. "j", "J", "Ctrl+Enter".
 *
 * Shift is part of the character for printable keys ("J", "?"), so it is
 * only named for other keys ("Shift+Tab") or with Ctrl/Alt ("Ctrl+Shift+Enter").
 */
export function comboOf(event: KeyLike): string {
  const key = KEY_NAMES[event.key] ?? latinLetter(event) ?? event.key;
  const printable = key.length === 1;
  const modifiers = [];
  if (event.ctrlKey || event.metaKey) modifiers.push("Ctrl");
  if (event.altKey) modifiers.push("Alt");
  if (event.shiftKey && (!printable || modifiers.length > 0)) modifiers.push("Shift");
  // With Ctrl, letters are named in lower case: "Ctrl+Shift+a", not "Ctrl+Shift+A"
  const name = printable && modifiers.length > 0 ? key.toLowerCase() : key;
  return [...modifiers, name].join("+");
}

// Keys that don't tell the character, e.g. a dead key starting an accent
const UNNAMED_KEYS = ["Dead", "Process", "Unidentified"];

// The characters of punctuation keys on a US layout, unshifted and shifted
const PUNCTUATION: Record<string, [string, string]> = {
  Semicolon: [";", ":"],
  Comma: [",", "<"],
  Period: [".", ">"],
  Slash: ["/", "?"],
  Quote: ["'", '"'],
  BracketLeft: ["[", "{"],
  BracketRight: ["]", "}"],
  Backquote: ["`", "~"],
  Minus: ["-", "_"],
  Equal: ["=", "+"],
};

/**
 * Whether the keyboard layout types another script than Latin (e.g. Hebrew),
 * as the last letter key typed tells (see `noteLayout`).
 */
export const layout = { otherScript: false };

/**
 * Learn the layout's script from a letter key typed without modifiers: a Latin
 * letter (English, Dvorak, AZERTY...) or another script's (Hebrew, Russian...).
 */
export function noteLayout(event: KeyLike) {
  if (event.ctrlKey || event.metaKey || event.altKey || event.shiftKey) return;
  if (!/^Key[A-Z]$/.test(event.code ?? "") || event.key.length !== 1) return;
  if (/\p{Script=Latin}/u.test(event.key)) layout.otherScript = false;
  else if (/\p{L}/u.test(event.key)) layout.otherScript = true;
}

/** Learn the layout from every key typed, also in fields. Returns a function that stops it. */
export function watchLayout(target: Window): () => void {
  const onKey = (event: KeyboardEvent) => noteLayout(event);
  target.addEventListener("keydown", onKey, true);
  return () => target.removeEventListener("keydown", onKey, true);
}

/**
 * The US character of the physical key when the layout types another script
 * (e.g. "g" for the key of "ע" in Hebrew, ";" for the key of "ף") or nothing
 * yet (a dead key), so shortcuts work in any layout. In such a layout, the
 * letter and punctuation keys that type ASCII go by position too (Hebrew's Q
 * types "/", and its W "'"). Null for other keys, and in Latin layouts, whose
 * keys are their characters (Dvorak's Q key is "'").
 */
function latinLetter(event: KeyLike): string | null {
  const otherScript = event.key.length === 1 && (!/[\x00-\x7f]/.test(event.key) || layout.otherScript);
  if (!otherScript && !UNNAMED_KEYS.includes(event.key)) return null;
  const code = event.code ?? "";
  const letter = /^Key([A-Z])$/.exec(code);
  if (letter) return event.shiftKey ? letter[1] : letter[1].toLowerCase();
  const punctuation = PUNCTUATION[code];
  if (punctuation) return punctuation[event.shiftKey ? 1 : 0];
  return null;
}

export type Match =
  | { kind: "binding"; binding: Binding }
  /** The key starts a sequence (e.g. "g" of "g t"): wait for the next key. */
  | { kind: "prefix"; prefix: string }
  | { kind: "none" };

/**
 * The layers that receive keys: from the top, down to the first modal one.
 * If that one allows navigation, the navigation bindings below it follow.
 */
export function activeLayers(layers: Layer[]): Layer[] {
  const active = [];
  const fromTop = [...layers].reverse();
  for (const [index, layer] of fromTop.entries()) {
    active.push(layer);
    if (!layer.modal) continue;
    if (layer.allowsNavigation) {
      const navigation = fromTop.slice(index + 1).flatMap((below) => below.bindings.filter((binding) => binding.navigation));
      if (navigation.length > 0) active.push({ name: "Navigation", bindings: navigation });
    }
    break;
  }
  return active;
}

/**
 * Match a key press to a binding of the active layers, from the top.
 *
 * `pending` is the start of a sequence typed before (e.g. "g"), and `inInput`
 * tells whether the user is typing in a text field.
 */
export function matchKey(
  layers: Layer[],
  combo: string,
  { pending = null, inInput = false }: { pending?: string | null; inInput?: boolean } = {},
): Match {
  const typed = pending ? `${pending} ${combo}` : combo;
  for (const layer of activeLayers(layers)) {
    for (const binding of layer.bindings) {
      if (inInput && !binding.inInputs) continue;
      if (binding.keys.includes(typed)) return { kind: "binding", binding };
    }
  }
  if (!pending && !inInput) {
    const isPrefix = activeLayers(layers).some((layer) =>
      layer.bindings.some((binding) => binding.keys.some((key) => key.startsWith(`${typed} `))),
    );
    if (isPrefix) return { kind: "prefix", prefix: typed };
  }
  return { kind: "none" };
}

/** Whether the element takes typed text, so plain keys must not trigger shortcuts. */
export function isTextInput(element: Element | null): boolean {
  if (!element) return false;
  if (element instanceof HTMLTextAreaElement || element instanceof HTMLSelectElement) return true;
  if (element instanceof HTMLInputElement) {
    return !["checkbox", "radio", "button", "submit", "reset"].includes(element.type);
  }
  return (element as HTMLElement).isContentEditable ?? false;
}

/**
 * Whether Enter and Space should keep their native meaning on the element:
 * pressing a focused button, or toggling a checkbox.
 */
export function isNativeControl(element: Element | null): boolean {
  if (!element) return false;
  if (element instanceof HTMLButtonElement || element instanceof HTMLAnchorElement) return true;
  return element instanceof HTMLInputElement && ["checkbox", "radio", "button", "submit"].includes(element.type);
}

/**
 * Give the keyboard back to the shortcuts after picking an option of a
 * `<select>` with the mouse. A focused select counts as a text input (it uses
 * typed letters and digits), so otherwise every shortcut is ignored until
 * something else is clicked. Picking with the keyboard keeps the focus.
 * Returns a function that stops it.
 */
export function releaseSelectsAfterPicking(target: Window): () => void {
  let pointerUsed = false;
  const onPointer = () => (pointerUsed = true);
  const onKey = () => (pointerUsed = false);
  const onChange = (event: Event) => {
    if (pointerUsed && event.target instanceof HTMLSelectElement) event.target.blur();
  };
  target.addEventListener("pointerdown", onPointer, true);
  target.addEventListener("keydown", onKey, true);
  target.addEventListener("change", onChange, true);
  return () => {
    target.removeEventListener("pointerdown", onPointer, true);
    target.removeEventListener("keydown", onKey, true);
    target.removeEventListener("change", onChange, true);
  };
}

/** How long a sequence's first key waits for the next one. */
export const SEQUENCE_TIMEOUT_MS = 1200;

/**
 * Dispatches key presses to the bindings of a stack of layers. One instance
 * serves the whole app (see `keyboard` in state.svelte.ts).
 */
export class Keyboard {
  layers: Layer[] = [];
  pending: string | null = null;
  private pendingTimer: ReturnType<typeof setTimeout> | null = null;
  /** Called when the pending sequence changes, e.g. to show "g …". */
  onPending: (pending: string | null) => void = () => {};

  /** Add a layer on top. Returns a function that removes it. */
  push(layer: Layer): () => void {
    this.layers = [...this.layers, layer];
    return () => {
      this.layers = this.layers.filter((other) => other !== layer);
    };
  }

  private setPending(pending: string | null) {
    if (this.pendingTimer) clearTimeout(this.pendingTimer);
    this.pending = pending;
    this.onPending(pending);
    if (pending) {
      this.pendingTimer = setTimeout(() => this.setPending(null), SEQUENCE_TIMEOUT_MS);
    }
  }

  /** Handle a keydown event. Returns whether a binding took it. */
  handle(event: KeyboardEvent, inInput: boolean): boolean {
    // Keys of an input method composing text (e.g. its Enter) are the text's
    if (["Shift", "Control", "Alt", "Meta"].includes(event.key) || event.isComposing) return false;
    // A sequence doesn't go on into a field focused since its first key
    const pending = inInput ? null : this.pending;
    const match = matchKey(this.layers, comboOf(event), { pending, inInput });
    this.setPending(null);
    if (match.kind === "prefix") {
      event.preventDefault();
      this.setPending(match.prefix);
      return true;
    }
    if (match.kind === "binding") {
      event.preventDefault();
      match.binding.run(event);
      return true;
    }
    // A key that doesn't go on with the sequence only cancels it (e.g. Escape)
    if (pending && !(event.ctrlKey || event.metaKey || event.altKey)) {
      event.preventDefault();
      return true;
    }
    return false;
  }
}
