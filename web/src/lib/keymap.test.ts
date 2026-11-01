import { describe, expect, it } from "vitest";
import registry from "../../../finalyze/keymap.json";
import cases from "../../../tests/fixtures/keymap_problems.json";
import type { Keymap } from "./api";
import { VIEWS } from "./dashboard";
import {
  ACTIONS,
  CONTEXTS,
  DEFAULT_KEYMAP,
  DIRECTION_PRESETS,
  GO_PREFIX,
  SCOPES,
  clashes,
  keysOf,
  normalized,
  ownKeys,
  problems,
  settingsOf,
  type DirectionKeys,
  type KeymapSettings,
} from "./keymap";
import { ROUTES } from "./routes";

const JKL_KEYS: DirectionKeys = { left: "j", down: "k", up: "l", right: ";" };
const JKL: KeymapSettings = { directions: JKL_KEYS, overrides: {} };
const withOverrides = (overrides: Record<string, string[]>, directions: DirectionKeys = DEFAULT_KEYMAP.directions): KeymapSettings => ({
  directions,
  overrides,
});

describe("the registry", () => {
  it("has unique IDs, in known scopes, each in a context", () => {
    const ids = ACTIONS.map((action) => action.id);
    expect(new Set(ids).size).toBe(ids.length);
    for (const action of ACTIONS) {
      expect(Object.keys(SCOPES)).toContain(action.scope);
      expect(CONTEXTS.some((context) => context.scopes.includes(action.scope))).toBe(true);
      // Every action has a key, or is the go key's
      expect(keysOf(DEFAULT_KEYMAP, action.id).length).toBeGreaterThan(0);
    }
  });

  it("is the shared registry, finalyze/keymap.json, which the server reads too", () => {
    expect(ACTIONS.map((action) => action.id)).toEqual(registry.scopes.flatMap((scope) => scope.actions.map((action) => action.id)));
    expect(CONTEXTS).toEqual(registry.contexts);
    expect(DEFAULT_KEYMAP.directions).toEqual({ left: "h", down: "j", up: "k", right: "l" });
  });

  it("has every page after the go key, and every dashboard view", () => {
    const pages = ACTIONS.filter((action) => action.navigation).map((action) => action.id);
    expect(pages).toEqual(ROUTES.map((route) => `go.${route.path}`));
    for (const route of ROUTES) expect(keysOf(DEFAULT_KEYMAP, `go.${route.path}`)).toEqual([expect.stringMatching(/^g [a-z]$/)]);
    const views = ACTIONS.filter((action) => action.id.startsWith("dashboard.view.")).map((action) => action.id);
    expect(views).toEqual(VIEWS.map((view) => `dashboard.view.${view.id}`));
  });

  for (const preset of DIRECTION_PRESETS) {
    it(`has no clashes by default, with the ${preset.title} directions`, () => {
      expect(problems({ directions: preset.keys, overrides: {} })).toEqual([]);
    });
  }

  it("keeps the arrows for every direction, whatever its key", () => {
    const arrows = { left: "ArrowLeft", down: "ArrowDown", up: "ArrowUp", right: "ArrowRight" };
    const directed = ACTIONS.filter((action) => action.direction);
    expect(directed.length).toBeGreaterThan(0);
    for (const action of directed) {
      const arrow = `${action.shift ? "Shift+" : ""}${arrows[action.direction!]}`;
      for (const directions of [DEFAULT_KEYMAP.directions, JKL_KEYS, { left: "a", down: "s", up: "w", right: "d" }]) {
        expect(keysOf(withOverrides({}, directions), action.id)).toContain(arrow);
        expect(keysOf(withOverrides({ [action.id]: [] }, directions), action.id)).toEqual([arrow]);
      }
    }
  });
});

describe("the same problems as the server", () => {
  // tests/fixtures/keymap_problems.json: tests/test_settings.py checks the server's against it
  for (const { name, keymap, problems: expected } of cases) {
    it(name, () => expect(problems(settingsOf(keymap as Keymap))).toEqual(expected));
  }
});

describe("the directions", () => {
  it("move rows, months and reorder by their keys", () => {
    expect(keysOf(DEFAULT_KEYMAP, "list.down")).toEqual(["j", "ArrowDown"]);
    expect(keysOf(DEFAULT_KEYMAP, "reorder.up")).toEqual(["K", "Shift+ArrowUp"]);
    expect(keysOf(DEFAULT_KEYMAP, "breakdown.previousMonth")).toEqual(["h", "ArrowLeft"]);
    expect(keysOf(JKL, "list.down")).toEqual(["k", "ArrowDown"]);
    expect(keysOf(JKL, "list.up")).toEqual(["l", "ArrowUp"]);
    expect(keysOf(JKL, "breakdown.previousMonth")).toEqual(["j", "ArrowLeft"]);
    expect(keysOf(JKL, "breakdown.nextMonth")).toEqual([";", "ArrowRight"]);
    expect(keysOf(JKL, "reorder.down")).toEqual(["K", "Shift+ArrowDown"]);
    expect(keysOf(JKL, "reorder.up")).toEqual(["L", "Shift+ArrowUp"]);
    // Each its own
    const mixed = withOverrides({}, { left: "a", down: "j", up: "w", right: "/" });
    expect(keysOf(mixed, "breakdown.previousMonth")).toEqual(["a", "ArrowLeft"]);
    expect(keysOf(mixed, "breakdown.nextMonth")).toEqual(["/", "ArrowRight"]);
    expect(keysOf(mixed, "reorder.up")).toEqual(["W", "Shift+ArrowUp"]);
  });

  it("can't share a key, or be a fixed one", () => {
    expect(problems(withOverrides({}, { left: "h", down: "j", up: "j", right: "l" }))).toContain("j is both down and up");
    expect(problems(withOverrides({}, { left: "h", down: "j", up: "k", right: "Tab" }))).toContain(
      "Tab can't be a direction (right): it keeps its own meaning",
    );
  });

  it("clash with other keys", () => {
    // On the dashboard, e (every group) against a Breakdown month
    expect(clashes(withOverrides({}, { left: "e", down: "j", up: "k", right: "l" }))).toEqual([
      { key: "e", actions: ["breakdown.previousMonth", "dashboard.groups"], context: "Dashboard: Breakdown" },
    ]);
  });

  it("give way to an action's own keys", () => {
    const keymap = withOverrides({ "list.down": ["n"] }, JKL_KEYS);
    expect(keysOf(keymap, "list.down")).toEqual(["n", "ArrowDown"]);
    expect(keysOf(keymap, "list.up")).toEqual(["l", "ArrowUp"]);
  });
});

describe("overrides", () => {
  it("replace an action's keys, or unbind it", () => {
    expect(keysOf(withOverrides({ "tags.rename": ["N", "F2"] }), "tags.rename")).toEqual(["N", "F2"]);
    expect(keysOf(withOverrides({ "tags.delete": [] }), "tags.delete")).toEqual(["Delete"]);
  });

  it("move the pages' letters and the go key", () => {
    const keymap = withOverrides({ "go.tags": ["q"], [GO_PREFIX]: ["'"] });
    expect(keysOf(keymap, "go.tags")).toEqual(["' q"]);
    expect(keysOf(keymap, "go.rules")).toEqual(["' r"]);
    // The go key is one key
    expect(ownKeys(withOverrides({ [GO_PREFIX]: [] }), GO_PREFIX)).toEqual(["g"]);
  });

  it("ignore unknown actions, and drop overrides equal to the defaults", () => {
    expect(keysOf(withOverrides({ "no.such": ["q"] }), "no.such")).toEqual([]);
    expect(normalized(withOverrides({ "no.such": ["q"], "tags.rename": ["n"], "list.down": ["j"], "tags.merge": ["M"] }))).toEqual(
      withOverrides({ "tags.merge": ["M"] }),
    );
    // A direction's default depends on the direction keys
    expect(normalized(withOverrides({ "list.down": ["j"] }, JKL_KEYS)).overrides).toEqual({ "list.down": ["j"] });
  });
});

describe("clashes", () => {
  it("are keys of two actions active together", () => {
    const keymap = withOverrides({ "tags.rename": ["e"] });
    expect(clashes(keymap)).toEqual([{ key: "e", actions: ["tags.edit", "tags.rename"], context: "Tags" }]);
    expect(problems(keymap)).toEqual(['e is both "Edit color and kind" and "Rename" (Tags)']);
  });

  it("are not across pages, or between fixed keys", () => {
    // n renames tags, and nets by tag on the dashboard; Escape is fixed everywhere
    expect(clashes(withOverrides({ "tags.rename": ["i"] }))).toEqual([]);
  });

  it("are with the keys of every page (global, lists) and with fixed keys", () => {
    expect(clashes(withOverrides({ "tags.rename": ["?"] }))[0].actions).toEqual(["help.open", "tags.rename"]);
    expect(clashes(withOverrides({ "tags.rename": ["j"] }))[0].actions).toEqual(["list.down", "tags.rename"]);
    expect(clashes(withOverrides({ "tags.rename": ["Delete"] }))[0].actions).toEqual(["tags.delete", "tags.rename"]);
    expect(problems(withOverrides({ "tags.rename": ["Delete"] }))[0]).toMatch(/^Delete can't be bound/);
  });

  it("are a key that starts the go sequences, or that a page answers to after it", () => {
    expect(clashes(withOverrides({ "tags.rename": ["g"] }))).toContainEqual({ key: "g", actions: [GO_PREFIX, "tags.rename"], context: "Tags" });
    expect(clashes(withOverrides({ "go.tags": ["d"] }))).toContainEqual({ key: "g d", actions: ["go.dashboard", "go.tags"], context: "Annotate" });
    // Not a page's own key that is a page's letter
    expect(clashes(withOverrides({ "tags.rename": ["t"] }))).toEqual([]);
  });

  it("are found in dialogs that let the go keys through", () => {
    expect(clashes(withOverrides({ "drill.open": ["g"] }))).toContainEqual({
      key: "g",
      actions: ["drill.open", GO_PREFIX],
      context: "Dashboard: the rows behind a number",
    });
  });
});
