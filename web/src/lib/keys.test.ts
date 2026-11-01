import { afterEach, describe, expect, it, vi } from "vitest";
import { activeLayers, comboOf, Keyboard, layout, matchKey, noteLayout, SEQUENCE_TIMEOUT_MS, type Binding, type KeyLike, type Layer } from "./keys";
import { DEFAULT_KEYMAP, ownKeys } from "./keymap";
import { ROUTES, splitTitle, type Route } from "./routes";

function press(key: string, modifiers: Partial<KeyLike> = {}): KeyLike {
  return { key, ctrlKey: false, metaKey: false, altKey: false, shiftKey: false, ...modifiers };
}

/** A KeyboardEvent stand-in for the Keyboard class. */
function event(key: string, modifiers: Partial<KeyLike> = {}) {
  return { ...press(key, modifiers), preventDefault: vi.fn() } as unknown as KeyboardEvent;
}

const binding = (keys: string[], extra: Partial<Binding> = {}): Binding => ({ keys, run: vi.fn(), ...extra });

describe("comboOf", () => {
  it("names printable keys by their character", () => {
    expect(comboOf(press("j"))).toBe("j");
    expect(comboOf(press("J", { shiftKey: true }))).toBe("J");
    expect(comboOf(press("?", { shiftKey: true }))).toBe("?");
    expect(comboOf(press(" "))).toBe("Space");
  });

  it("names modifiers", () => {
    expect(comboOf(press("Enter", { ctrlKey: true }))).toBe("Ctrl+Enter");
    expect(comboOf(press("Enter", { metaKey: true, shiftKey: true }))).toBe("Ctrl+Shift+Enter");
    expect(comboOf(press("Tab", { shiftKey: true }))).toBe("Shift+Tab");
    expect(comboOf(press("A", { ctrlKey: true, shiftKey: true }))).toBe("Ctrl+Shift+a");
  });

  it("names letters by their Latin key in other layouts", () => {
    expect(comboOf(press("ע", { code: "KeyG" }))).toBe("g");
    expect(comboOf(press("ע", { code: "KeyG", shiftKey: true }))).toBe("G");
    expect(comboOf(press("4", { code: "Digit4" }))).toBe("4");
    expect(comboOf(press(".", { code: "Slash" }))).toBe(".");
    expect(comboOf(press("ע"))).toBe("ע");
    // A dead key (e.g. the start of an accent) still names its letter
    expect(comboOf(press("Dead", { code: "KeyA" }))).toBe("a");
  });

  it("names punctuation by its US key in other layouts", () => {
    // Hebrew: "ף" is on the ; key, "ת" on the comma's and "ץ" on the period's
    expect(comboOf(press("ף", { code: "Semicolon" }))).toBe(";");
    expect(comboOf(press(":", { code: "Semicolon", shiftKey: true }))).toBe(":");
    expect(comboOf(press("ת", { code: "Comma" }))).toBe(",");
    expect(comboOf(press("ץ", { code: "Period" }))).toBe(".");
  });
});

describe("the layout's script", () => {
  afterEach(() => (layout.otherScript = false));
  const hebrew = () => noteLayout(press("ש", { code: "KeyA" }));
  const english = () => noteLayout(press("a", { code: "KeyA" }));

  it("is learned from letter keys typed without modifiers", () => {
    hebrew();
    expect(layout.otherScript).toBe(true);
    english();
    expect(layout.otherScript).toBe(false);
    // Not from digits, punctuation, Shift (which types Latin capitals in some Hebrew layouts), or Ctrl
    hebrew();
    const others = [
      press("1", { code: "Digit1" }),
      press(";", { code: "Semicolon" }),
      press("A", { code: "KeyA", shiftKey: true }),
      press("a", { code: "KeyA", ctrlKey: true }),
    ];
    for (const other of others) {
      noteLayout(other);
      expect(layout.otherScript).toBe(true);
    }
    // Latin letters beyond ASCII are Latin (e.g. Turkish ı)
    noteLayout(press("ı", { code: "KeyI" }));
    expect(layout.otherScript).toBe(false);
  });

  it("makes Hebrew's Q and W, which type / and ', their letters", () => {
    hebrew();
    expect(comboOf(press("/", { code: "KeyQ" }))).toBe("q");
    expect(comboOf(press("'", { code: "KeyW" }))).toBe("w");
    expect(comboOf(press("Q", { code: "KeyQ", shiftKey: true }))).toBe("Q");
    // The other keys typing ASCII go by position too: Hebrew's / key types "."
    expect(comboOf(press(".", { code: "Slash" }))).toBe("/");
    expect(comboOf(press("4", { code: "Digit4" }))).toBe("4");
  });

  it("leaves Latin layouts' keys as typed", () => {
    english();
    expect(comboOf(press("/", { code: "Slash" }))).toBe("/");
    expect(comboOf(press("?", { code: "Slash", shiftKey: true }))).toBe("?");
    expect(comboOf(press("w", { code: "KeyW" }))).toBe("w");
    // Dvorak: ' on the Q key, . on the E key; AZERTY: , on the M key
    expect(comboOf(press("'", { code: "KeyQ" }))).toBe("'");
    expect(comboOf(press(".", { code: "KeyE" }))).toBe(".");
    expect(comboOf(press(",", { code: "KeyM" }))).toBe(",");
    // Before any letter is typed, too
    layout.otherScript = false;
    expect(comboOf(press("/", { code: "KeyQ" }))).toBe("/");
  });
});

describe("matchKey", () => {
  const app: Layer = { name: "app", bindings: [binding(["g t"]), binding(["?"])] };
  const page: Layer = {
    name: "page",
    bindings: [binding(["j"]), binding(["Ctrl+Enter"], { inInputs: true }), binding(["?"])],
  };
  const dialog: Layer = { name: "dialog", modal: true, bindings: [binding(["Escape"], { inInputs: true })] };

  it("prefers the topmost layer", () => {
    const match = matchKey([app, page], "?");
    expect(match.kind === "binding" && match.binding).toBe(page.bindings[2]);
  });

  it("ignores plain keys in inputs", () => {
    expect(matchKey([app, page], "j", { inInput: true }).kind).toBe("none");
    expect(matchKey([app, page], "Ctrl+Enter", { inInput: true }).kind).toBe("binding");
  });

  it("matches sequences", () => {
    expect(matchKey([app, page], "g")).toEqual({ kind: "prefix", prefix: "g" });
    const match = matchKey([app, page], "t", { pending: "g" });
    expect(match.kind === "binding" && match.binding).toBe(app.bindings[0]);
  });

  it("hides the layers below a modal one", () => {
    expect(matchKey([app, page, dialog], "j").kind).toBe("none");
    expect(matchKey([app, page, dialog], "Escape").kind).toBe("binding");
  });

  it("keeps the navigation under a modal layer that allows it", () => {
    const go = binding(["g t"], { navigation: true });
    const shell: Layer = { name: "app", bindings: [go, binding(["?"])] };
    const panel: Layer = { name: "panel", modal: true, allowsNavigation: true, bindings: [binding(["Escape"])] };
    expect(matchKey([shell, page, panel], "g")).toEqual({ kind: "prefix", prefix: "g" });
    const match = matchKey([shell, page, panel], "t", { pending: "g" });
    expect(match.kind === "binding" && match.binding).toBe(go);
    // Only the navigation: the other keys below stay hidden
    expect(matchKey([shell, page, panel], "?").kind).toBe("none");
    expect(matchKey([shell, page, panel], "j").kind).toBe("none");
    expect(activeLayers([shell, page, panel]).map((layer) => layer.name)).toEqual(["panel", "Navigation"]);
    // A dialog that doesn't allow it hides it
    expect(matchKey([shell, page, dialog], "g").kind).toBe("none");
  });
});

describe("Keyboard", () => {
  it("runs bindings and removes layers", () => {
    const keyboard = new Keyboard();
    const next = binding(["j"]);
    const remove = keyboard.push({ name: "page", bindings: [next] });
    const press = event("j");
    expect(keyboard.handle(press, false)).toBe(true);
    expect(next.run).toHaveBeenCalledOnce();
    expect(press.preventDefault).toHaveBeenCalled();
    remove();
    expect(keyboard.handle(event("j"), false)).toBe(false);
  });

  it("waits for the second key of a sequence", () => {
    const keyboard = new Keyboard();
    const go = binding(["g t"]);
    const pending = vi.fn();
    keyboard.onPending = pending;
    keyboard.push({ name: "app", bindings: [go] });
    keyboard.handle(event("g"), false);
    expect(pending).toHaveBeenLastCalledWith("g");
    keyboard.handle(event("t"), false);
    expect(go.run).toHaveBeenCalledOnce();
    expect(pending).toHaveBeenLastCalledWith(null);
  });

  it("cancels a sequence with a key that doesn't go on with it, such as Escape", () => {
    const keyboard = new Keyboard();
    const go = binding(["g t"]);
    const escape = binding(["Escape"], { inInputs: true });
    keyboard.push({ name: "app", bindings: [go, escape] });
    keyboard.handle(event("g"), false);
    const cancel = event("Escape");
    expect(keyboard.handle(cancel, false)).toBe(true);
    expect(cancel.preventDefault).toHaveBeenCalled();
    expect(escape.run).not.toHaveBeenCalled();
    expect(keyboard.pending).toBe(null);
    keyboard.handle(event("t"), false);
    expect(go.run).not.toHaveBeenCalled();
    // A key with Ctrl keeps its meaning (e.g. Ctrl+r reloads)
    keyboard.handle(event("g"), false);
    expect(keyboard.handle(event("r", { ctrlKey: true }), false)).toBe(false);
  });

  it("doesn't go on with a sequence in a field focused since its first key", () => {
    const keyboard = new Keyboard();
    const go = binding(["g t"]);
    keyboard.push({ name: "app", bindings: [go] });
    keyboard.handle(event("g"), false);
    const typed = event("t");
    expect(keyboard.handle(typed, true)).toBe(false);
    expect(typed.preventDefault).not.toHaveBeenCalled();
    expect(go.run).not.toHaveBeenCalled();
  });

  describe("the sequence's timeout", () => {
    afterEach(() => {
      vi.useRealTimers();
    });

    it("forgets the first key after a while", () => {
      vi.useFakeTimers();
      const keyboard = new Keyboard();
      const go = binding(["g t"]);
      const next = binding(["t"]);
      keyboard.push({ name: "app", bindings: [go, next] });
      keyboard.handle(event("g"), false);
      vi.advanceTimersByTime(SEQUENCE_TIMEOUT_MS + 1);
      expect(keyboard.pending).toBe(null);
      keyboard.handle(event("t"), false);
      expect(go.run).not.toHaveBeenCalled();
      expect(next.run).toHaveBeenCalledOnce();
    });
  });

  it("ignores lone modifier keys", () => {
    const keyboard = new Keyboard();
    keyboard.push({ name: "app", bindings: [binding(["Shift"])] });
    expect(keyboard.handle(event("Shift", { shiftKey: true }), false)).toBe(false);
  });
});

describe("the page keys", () => {
  /** A page's default letter after the go key, e.g. "t". */
  const goKey = (route: Route) => ownKeys(DEFAULT_KEYMAP, `go.${route.path}`)[0];

  /** The app's navigation bindings, as App.svelte makes them. */
  function navigationLayer(visits: string[]): Layer {
    return {
      name: "app",
      bindings: ROUTES.map((route) => ({ keys: [`g ${goKey(route)}`], navigation: true, run: () => visits.push(route.path) })),
    };
  }

  it("are distinct letters of their titles", () => {
    const letters = ROUTES.map(goKey);
    expect(new Set(letters).size).toBe(letters.length);
    for (const route of ROUTES) {
      expect(goKey(route)).toMatch(/^[a-z]$/);
      expect(splitTitle(route, goKey(route)).join("")).toBe(route.title);
      expect(splitTitle(route, goKey(route))[1].toLowerCase()).toBe(goKey(route));
    }
    expect(splitTitle(ROUTES.find((route) => route.path === "tags")!, "g")).toEqual(["Ta", "g", "s"]);
    expect(splitTitle(ROUTES.find((route) => route.path === "accounts")!, "c")).toEqual(["A", "c", "counts"]);
    // Another letter: the title's, else none (a capital takes Shift)
    expect(splitTitle(ROUTES.find((route) => route.path === "tags")!, "s")).toEqual(["Tag", "s", ""]);
    expect(splitTitle(ROUTES.find((route) => route.path === "tags")!, "q")).toEqual(["Tags", "", ""]);
    expect(splitTitle(ROUTES.find((route) => route.path === "tags")!, "T")).toEqual(["Tags", "", ""]);
  });

  it("open every page after g, also in a Hebrew layout, and over the page's own letters", () => {
    const visits: string[] = [];
    const keyboard = new Keyboard();
    keyboard.push(navigationLayer(visits));
    // A page that binds the same letters (and g's neighbors) alone
    keyboard.push({ name: "page", bindings: ["a", "t", "d", "i", "r", "c", "o", "s", "G"].map((key) => binding([key])) });
    for (const route of ROUTES) {
      keyboard.handle(event("g", { code: "KeyG" }), false);
      keyboard.handle(event(goKey(route), { code: `Key${goKey(route).toUpperCase()}` }), false);
    }
    // Hebrew: "ע" is on the G key, and "ש" on the A key
    keyboard.handle(event("ע", { code: "KeyG" }), false);
    keyboard.handle(event("ש", { code: "KeyA" }), false);
    keyboard.handle(event("ע", { code: "KeyG" }), false);
    keyboard.handle(event("ע", { code: "KeyG" }), false);
    expect(visits).toEqual([...ROUTES.map((route) => route.path), "annotate", "tags"]);
  });

  it("are no longer digits", () => {
    const visits: string[] = [];
    const keyboard = new Keyboard();
    keyboard.push(navigationLayer(visits));
    expect(keyboard.handle(event("2", { code: "Digit2" }), false)).toBe(false);
    keyboard.handle(event("g"), false);
    keyboard.handle(event("2", { code: "Digit2" }), false);
    expect(visits).toEqual([]);
  });
});
