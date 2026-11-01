// The keyboard shortcuts: every one is a named action, with its scope (where
// it is active), what it does and its default keys. Components bind actions
// (see `bind` in use-keys.svelte.ts), never keys, and the keymap (stored by
// the server, see GET /api/settings/keymap) gives the actions their keys: the
// keys of the four directions, and keys of single actions over their defaults.
//
// The registry is finalyze/keymap.json, which the server reads too
// (finalyze/keymap.py): both refuse keymaps whose keys clash, with the same
// rules. Keep the two in step.
//
// Some keys are fixed (Enter, Escape, Tab, the arrows and the like): they can't
// be bound, and keep their meaning.
import type { Keymap } from "./api";
import registry from "../../../finalyze/keymap.json";

export type Direction = "left" | "down" | "up" | "right";
export type DirectionKeys = Record<Direction, string>;
/** The keymap as the server keeps it (`Keymap`), with nothing left out. */
export type KeymapSettings = { directions: DirectionKeys; overrides: Record<string, string[]> };

/** In the order shown. */
export const DIRECTIONS: { id: Direction; title: string }[] = [
  { id: "left", title: "Left" },
  { id: "down", title: "Down" },
  { id: "up", title: "Up" },
  { id: "right", title: "Right" },
];

/** Keys to fill the four directions with at once. */
export const DIRECTION_PRESETS: { title: string; keys: DirectionKeys }[] = [
  { title: "vim (h j k l)", keys: { left: "h", down: "j", up: "k", right: "l" } },
  { title: "j k l ;", keys: { left: "j", down: "k", up: "l", right: ";" } },
];

/** The keys of the directions with Shift (on a US layout), to reorder. */
const SHIFTED: Record<string, string> = registry.shifted;

function shifted(key: string): string {
  return SHIFTED[key] ?? key.toUpperCase();
}

export const DEFAULT_KEYMAP: KeymapSettings = { directions: registry.directions, overrides: {} };

/** The keymap the server gives, with the defaults of what it leaves out. */
export function settingsOf(keymap: Keymap): KeymapSettings {
  return { directions: { ...DEFAULT_KEYMAP.directions, ...keymap.directions }, overrides: keymap.overrides ?? {} };
}

/** Where actions are active (e.g. "tags"), by their titles, in the order of the Settings page. */
export const SCOPES: Record<string, string> = Object.fromEntries(registry.scopes.map((scope) => [scope.id, scope.title]));
export type Scope = string;

export interface Action {
  /** Stable, as stored in the keymap's overrides, e.g. "tags.rename". */
  id: string;
  scope: Scope;
  description: string;
  /** The default keys that can be changed. */
  keys?: string[];
  /** Or the default key is a direction's (with Shift: to reorder). */
  direction?: Direction;
  shift?: boolean;
  /** Keys that always work and can't be changed (e.g. Enter, the arrows). */
  fixed?: string[];
  /** Also active while typing in a field (e.g. Ctrl+Enter). */
  inInputs?: boolean;
  /** Opens a page: its keys are letters after the go key. */
  navigation?: boolean;
  /** Has exactly one key (the go key). */
  single?: boolean;
}

/** The key that starts the go sequences, e.g. "g" of "g t". */
export const GO_PREFIX = "go.prefix";

export const ACTIONS: Action[] = registry.scopes.flatMap((scope) =>
  scope.actions.map((action) => ({ ...action, scope: scope.id }) as Action),
);

export const ACTIONS_BY_ID: Record<string, Action> = Object.fromEntries(ACTIONS.map((action) => [action.id, action]));

/** Scopes active together, e.g. on a page: the same key can't do two actions of one. */
export const CONTEXTS: { name: string; scopes: Scope[] }[] = registry.contexts;

/** Keys that keep their meaning (e.g. pressing a button, moving in a field), so they can't be bound. */
export const RESERVED_KEYS: string[] = registry.reserved;

/** Whether a key can't be bound: a reserved one, alone or with Shift. */
export function isReserved(key: string): boolean {
  return RESERVED_KEYS.includes(key.replace(/^Shift\+/, ""));
}

/** An action's default keys, which can be changed, under the direction keys. */
export function defaultKeys(action: Action, directions: DirectionKeys): string[] {
  if (!action.direction) return action.keys ?? [];
  const key = directions[action.direction];
  return [action.shift ? shifted(key) : key];
}

/** Whether an action's keys can be changed (it has some, or a direction). */
export function isConfigurable(action: Action): boolean {
  return !!action.direction || (action.keys?.length ?? 0) > 0;
}

/**
 * An action's keys that can be changed, as the keymap sets them (for a page,
 * its letter after the go key). Unknown actions and invalid overrides (a go
 * key that isn't one key) fall back to the defaults.
 */
export function ownKeys(settings: KeymapSettings, id: string): string[] {
  const action = ACTIONS_BY_ID[id];
  if (!action) return [];
  const override = settings.overrides[id];
  if (override && !(action.single && override.length !== 1)) return override;
  return defaultKeys(action, settings.directions);
}

/**
 * The keys an action answers to, as matched: its own (a page's after the go
 * key, e.g. "g t"), then its fixed ones.
 */
export function keysOf(settings: KeymapSettings, id: string): string[] {
  const action = ACTIONS_BY_ID[id];
  if (!action) return [];
  const own = ownKeys(settings, id);
  const keys = action.navigation ? own.map((letter) => `${ownKeys(settings, GO_PREFIX)[0]} ${letter}`) : own;
  return [...keys, ...(action.fixed ?? [])];
}

/** The overrides without the ones equal to their default (so they follow the directions) or of unknown actions. */
export function normalized(settings: KeymapSettings): KeymapSettings {
  const overrides = Object.fromEntries(
    Object.entries(settings.overrides).filter(([id, keys]) => {
      const action = ACTIONS_BY_ID[id];
      return action && isConfigurable(action) && JSON.stringify(keys) !== JSON.stringify(defaultKeys(action, settings.directions));
    }),
  );
  return { directions: settings.directions, overrides };
}

export interface Clash {
  key: string;
  /** The two actions' IDs. */
  actions: [string, string];
  /** Where both are active, e.g. "Tags". */
  context: string;
}

/**
 * Keys that do two actions active together: the same key, or a key that
 * starts another's sequence (e.g. "g" and "g t"). Fixed keys only clash with
 * keys that can be changed: the ones they share are by design (e.g. Escape).
 */
export function clashes(settings: KeymapSettings): Clash[] {
  const found = new Map<string, Clash>();
  for (const context of CONTEXTS) {
    const keys: { key: string; action: string; fixed: boolean }[] = [];
    for (const action of ACTIONS.filter((item) => context.scopes.includes(item.scope))) {
      const all = keysOf(settings, action.id);
      const own = all.length - (action.fixed?.length ?? 0);
      all.forEach((key, index) => keys.push({ key, action: action.id, fixed: index >= own }));
    }
    for (const [index, first] of keys.entries()) {
      for (const second of keys.slice(index + 1)) {
        if (first.action === second.action || (first.fixed && second.fixed)) continue;
        const [shorter, longer] = first.key.length <= second.key.length ? [first, second] : [second, first];
        // The go key starting its pages' sequences is how they work
        const starts = longer.key.startsWith(`${shorter.key} `) && shorter.action !== GO_PREFIX;
        if (shorter.key !== longer.key && !starts) continue;
        const actions = [first.action, second.action].sort() as [string, string];
        const id = `${shorter.key}\n${actions.join("\n")}`;
        if (!found.has(id)) found.set(id, { key: shorter.key, actions, context: context.name });
      }
    }
  }
  return [...found.values()];
}

/**
 * Problems that keep a keymap from being saved, as sentences: reserved keys, a
 * key for two directions, and clashes. The server refuses the same ones.
 */
export function problems(settings: KeymapSettings): string[] {
  const { directions } = settings;
  const reserved = [
    ...Object.entries(settings.overrides).flatMap(([id, keys]) =>
      keys.filter(isReserved).map((key) => `${key} can't be bound (${describe(id)}): it keeps its own meaning`),
    ),
    ...DIRECTIONS.filter(({ id }) => isReserved(directions[id])).map(
      ({ id }) => `${directions[id]} can't be a direction (${id}): it keeps its own meaning`,
    ),
  ];
  const shared = DIRECTIONS.flatMap((first, index) =>
    DIRECTIONS.slice(index + 1)
      .filter((second) => directions[first.id] === directions[second.id])
      .map((second) => `${directions[first.id]} is both ${first.id} and ${second.id}`),
  );
  return [...reserved, ...shared, ...clashes(settings).map(clashText)];
}

export function describe(id: string): string {
  return ACTIONS_BY_ID[id]?.description ?? id;
}

export function clashText(clash: Clash): string {
  return `${clash.key} is both "${describe(clash.actions[0])}" and "${describe(clash.actions[1])}" (${clash.context})`;
}
