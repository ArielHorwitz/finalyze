// App-wide state, shared by the pages: Svelte 5 "runes" ($state) make these
// objects reactive, so components that read them update when they change.
import { api, ApiError, type Account, type Group, type Status, type Tag } from "./api";
import { Keyboard } from "./keys";
import { DEFAULT_KEYMAP, GO_PREFIX, settingsOf, keysOf as keymapKeysOf, ownKeys as keymapOwnKeys, type KeymapSettings } from "./keymap";
import type { TagKind } from "./kinds";
import { display } from "./display.svelte";

export const keyboard = new Keyboard();

export const app = $state({
  status: null as Status | null,
  tags: [] as Tag[],
  accounts: [] as Account[],
  groups: [] as Group[],
  /** The start of a key sequence being typed, e.g. "g". */
  pendingKeys: null as string | null,
  helpOpen: false,
  /** Incremented when the data changes under the pages, e.g. on ingest. */
  dataVersion: 0,
  toasts: [] as Toast[],
});

keyboard.onPending = (pending) => {
  app.pendingKeys = pending;
};

/** The keyboard shortcuts in use (see keymap.ts). */
export const keymap = $state({ settings: DEFAULT_KEYMAP as KeymapSettings, loaded: false });

export async function loadKeymap() {
  try {
    keymap.settings = settingsOf(await api.keymap());
  } catch (error) {
    toastError(error, "Keyboard shortcuts");
  } finally {
    keymap.loaded = true;
  }
}

/** The display settings, e.g. the date format, that display.svelte.ts shows dates in. */
export async function loadDisplay() {
  try {
    display.settings = { ...display.settings, ...(await api.displaySettings()) };
  } catch (error) {
    toastError(error, "Display settings");
  } finally {
    display.loaded = true;
  }
}

/** The keys an action answers to, e.g. ["g t"] or ["e", "Enter"]. */
export function keysOf(id: string): string[] {
  return keymapKeysOf(keymap.settings, id);
}

/** An action's keys that can be changed (a page's: its letter after the go key). */
export function ownKeys(id: string): string[] {
  return keymapOwnKeys(keymap.settings, id);
}

/** An action's key to show: its first own key, else its first fixed one ("" for none). */
export function keyName(id: string): string {
  return ownKeys(id)[0] ?? keysOf(id)[0] ?? "";
}

/** A title with an action's key, e.g. "Reverse (o)". */
export function titled(text: string, id: string): string {
  const key = keyName(id);
  return key ? `${text} (${key})` : text;
}

/** A glossary label's context naming an action's key, e.g. "Key: i". */
export function keyContext(id: string): string {
  const key = keyName(id);
  return key ? `Key: ${key}` : "";
}

/** The key that starts the go sequences, e.g. "g". */
export function goKey(): string {
  return ownKeys(GO_PREFIX)[0];
}

export interface Toast {
  id: number;
  kind: "info" | "success" | "error";
  message: string;
}

let nextToastId = 1;

/** Show a short message in the corner, which goes away by itself. */
export function toast(message: string, kind: Toast["kind"] = "info", durationMs = 4000) {
  const id = nextToastId++;
  app.toasts.push({ id, kind, message });
  setTimeout(() => {
    app.toasts = app.toasts.filter((item) => item.id !== id);
  }, durationMs);
}

/** Show an error as a toast, with the API's message if it is an API error. */
export function toastError(error: unknown, prefix = "") {
  const message = error instanceof ApiError ? error.detail : String(error);
  toast(prefix ? `${prefix}: ${message}` : message, "error", 8000);
}

export async function refreshStatus() {
  try {
    app.status = await api.status();
  } catch (error) {
    toastError(error, "Status");
  }
}

export async function refreshTags() {
  app.tags = await api.tags();
}

/** Accounts and their groups. */
export async function refreshAccounts() {
  [app.accounts, app.groups] = await Promise.all([api.accounts(), api.groups()]);
}

/** Whether a tag and subtag exist, to warn before creating new ones. */
export function labelExists(tag: string, subtag: string): boolean {
  const known = app.tags.find((item) => item.name === tag);
  return known?.subtags.some((item) => item.name === subtag) ?? false;
}

/** The kind a tag and subtag give (its own, else its tag's), regular if unknown. */
export function tagKind(tag: string, subtag: string): TagKind {
  const known = app.tags.find((item) => item.name === tag);
  const sub = known?.subtags.find((item) => item.name === subtag);
  return sub?.effective_kind ?? known?.kind ?? "regular";
}

export function tagColor(tag: string, subtag?: string): string | null {
  const known = app.tags.find((item) => item.name === tag);
  if (!known) return null;
  const sub = subtag === undefined ? undefined : known.subtags.find((item) => item.name === subtag);
  return sub?.effective_color ?? known.color ?? null;
}

export interface ConfirmRequest {
  title: string;
  message: string;
  confirmLabel: string;
  danger: boolean;
  resolve: (confirmed: boolean) => void;
}

export const dialogs = $state({ confirm: null as ConfirmRequest | null });

/** Ask the user to confirm, in a dialog (Enter confirms, Escape cancels). */
export function confirm(
  message: string,
  { title = "Confirm", confirmLabel = "Confirm", danger = false } = {},
): Promise<boolean> {
  return new Promise((resolve) => {
    dialogs.confirm = {
      title,
      message,
      confirmLabel,
      danger,
      resolve: (confirmed) => {
        dialogs.confirm = null;
        resolve(confirmed);
      },
    };
  });
}
