import { expect, test, type Page } from "@playwright/test";
import { apiGet, goTo, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

/** Set the keymap through the API, and reload so the app uses it. */
async function setKeymap(page: Page, keymap: object) {
  const response = await page.request.put("/api/settings/keymap", { data: keymap });
  expect(response.ok()).toBe(true);
}

/** On Settings: find an action, and change (or add) its key by pressing one. */
async function rebind(page: Page, search: string, key: string, { adding = false } = {}) {
  const find = page.getByLabel("Find a shortcut");
  await expect(find).toBeVisible();
  await page.keyboard.press("s");
  await expect(find).toBeFocused();
  await find.fill(search);
  const row = page.locator("tr[data-action]").first();
  await row.getByRole("button", { name: adding ? /^Add a key/ : /^Change the key/ }).click();
  const dialog = page.getByRole("dialog", { name: adding ? "Add a key" : "Change the key" });
  await expect(dialog).toBeVisible();
  await page.keyboard.press(key);
  await expect(dialog).toBeHidden();
  // Back to the page's shortcuts
  await find.focus();
  await page.keyboard.press("Escape");
}

const VIM = { left: "h", down: "j", up: "k", right: "l" };
const JKL = { left: "j", down: "k", up: "l", right: ";" };

/** Dispatch a key press as a layout types it, e.g. Hebrew's "ש" on the A key. */
async function typeInLayout(page: Page, key: string, code: string) {
  await page.evaluate(
    ([key, code]) => window.dispatchEvent(new KeyboardEvent("keydown", { key, code, bubbles: true, cancelable: true })),
    [key, code],
  );
}

const activeIndex = (page: Page) => page.locator("tr.active").getAttribute("data-index");

test("rebind a key in Settings by pressing it: saved, and used on its page and in the help", async ({ page }) => {
  await openPage(page, "settings");
  await rebind(page, "Rename", "N");
  await expect.poll(() => apiGet(page, "/settings/keymap")).toEqual({ directions: VIM, overrides: { "tags.rename": ["N"] } });
  await expect(page.locator('tr[data-action="tags.rename"] .changed')).toBeVisible();

  await goTo(page, "g");
  await page.getByRole("row", { name: /dining out/ }).click();
  await page.keyboard.press("n");
  await page.waitForTimeout(100);
  await expect(page.getByRole("dialog")).toBeHidden();
  await page.keyboard.press("N");
  await expect(page.getByRole("dialog", { name: /Rename/ })).toBeVisible();
  await page.keyboard.press("Escape");

  await page.keyboard.press("?");
  const help = page.getByRole("dialog", { name: "Keyboard shortcuts" });
  await expect(help.locator("dt", { has: page.locator("kbd", { hasText: /^N$/ }) })).toHaveCount(1);
  await expect(help.locator("dd", { hasText: /^Rename$/ })).toBeVisible();
  await page.keyboard.press("Escape");

  // Reset it, and add a second key instead
  await goTo(page, "s");
  await page.keyboard.press("s");
  await page.getByLabel("Find a shortcut").fill("Rename");
  await page.locator('tr[data-action="tags.rename"]').getByRole("button", { name: /^Reset/ }).click();
  await expect.poll(() => apiGet(page, "/settings/keymap")).toEqual({ directions: VIM, overrides: {} });
  await rebind(page, "Rename", "F2", { adding: true });
  await expect.poll(() => apiGet(page, "/settings/keymap")).toEqual({ directions: VIM, overrides: { "tags.rename": ["n", "F2"] } });
});

test("a clash is shown and not saved, and fixed keys can't be bound", async ({ page }) => {
  await openPage(page, "settings");
  // e already edits a tag
  await rebind(page, "Rename", "e");
  const problems = page.getByRole("alert");
  await expect(problems).toContainText('e is both "Edit color and kind" and "Rename" (Tags)');
  await expect(page.locator('tr[data-action="tags.rename"]')).toHaveClass(/\bclash\b/);
  await page.waitForTimeout(200);
  expect(await apiGet(page, "/settings/keymap")).toEqual({ directions: VIM, overrides: {} });

  // Leaving with a clash asks first
  await goTo(page, "d");
  const leave = page.getByRole("dialog", { name: "Confirm" });
  await expect(leave).toContainText("Discard the keyboard shortcuts that clash?");
  await page.keyboard.press("Escape");
  await expect(page.getByRole("heading", { name: "Settings" })).toBeVisible();

  // Moving the other action's key resolves it: both are saved
  await rebind(page, "Edit color and kind", "E");
  await expect(problems).toBeHidden();
  await expect
    .poll(() => apiGet(page, "/settings/keymap"))
    .toEqual({ directions: VIM, overrides: { "tags.rename": ["e"], "tags.edit": ["E"] } });

  // A fixed key is refused, and the dialog waits for another
  await page.keyboard.press("s");
  await page.getByLabel("Find a shortcut").fill("Merge into another");
  await page.locator('tr[data-action="tags.merge"]').getByRole("button", { name: /^Change the key/ }).click();
  const dialog = page.getByRole("dialog", { name: "Change the key" });
  await page.keyboard.press("Tab");
  await expect(dialog.getByRole("alert")).toContainText("Tab keeps its own meaning");
  await page.keyboard.press("Escape");
  await expect(dialog).toBeHidden();

  // Reset all
  await page.getByRole("button", { name: "Reset all to the defaults" }).click();
  await expect.poll(() => apiGet(page, "/settings/keymap")).toEqual({ directions: VIM, overrides: {} });

  // The server refuses a clash too, in the same words
  const refused = await page.request.put("/api/settings/keymap", { data: { overrides: { "tags.rename": ["e"] } } });
  expect(refused.status()).toBe(422);
  expect((await refused.json()).problems).toEqual(['e is both "Edit color and kind" and "Rename" (Tags)']);
  expect(await apiGet(page, "/settings/keymap")).toEqual({ directions: VIM, overrides: {} });
});

/** On Settings: change a direction's key by pressing one. */
async function setDirection(page: Page, direction: string, key: string) {
  await page.getByRole("button", { name: `Change the key of ${direction}` }).click();
  const dialog = page.getByRole("dialog", { name: "Change the key" });
  await expect(dialog).toBeVisible();
  await page.keyboard.press(key);
  await expect(dialog).toBeHidden();
}

test("the directions: four keys, filled at once or each by pressing it, follow everywhere", async ({ page }) => {
  await openPage(page, "settings");
  await expect(page.locator("tr.direction")).toHaveText([/^Left\s*h/, /^Down\s*j/, /^Up\s*k/, /^Right\s*l/]);
  await page.getByRole("button", { name: "Directions: j k l ;" }).click();
  await expect.poll(() => apiGet(page, "/settings/keymap")).toEqual({ directions: JKL, overrides: {} });
  await expect(page.locator("tr.direction")).toHaveText([/^Left\s*j/, /^Down\s*k/, /^Up\s*l/, /^Right\s*;/]);
  await expect(page.getByRole("alert")).toBeHidden();
  await page.getByRole("button", { name: "Directions: j k l ;" }).blur();

  // Rows: k down, l up
  await goTo(page, "g");
  await expect(page.locator("tr.active")).toBeVisible();
  expect(await activeIndex(page)).toBe("0");
  await page.keyboard.press("k");
  await page.keyboard.press("k");
  expect(await activeIndex(page)).toBe("2");
  await page.keyboard.press("l");
  expect(await activeIndex(page)).toBe("1");
  await page.keyboard.press("j");
  expect(await activeIndex(page)).toBe("1");

  // In the transactions' table too
  await goTo(page, "t");
  await expect(page.locator("tr.main").first()).toHaveClass(/\bactive\b/);
  await page.keyboard.press("k");
  await expect(page.locator("tr.main").nth(1)).toHaveClass(/\bactive\b/);

  // Reordering: K moves the rule down (later)
  await goTo(page, "r");
  const before = (await apiGet(page, "/rules")).map((rule: { id: number }) => rule.id);
  await expect(page.locator("tr.active")).toBeVisible();
  await page.keyboard.press("K");
  await expect.poll(async () => (await apiGet(page, "/rules")).map((rule: { id: number }) => rule.id)).toEqual([before[1], before[0], ...before.slice(2)]);

  // The breakdown's months: j back, ; forward (also by position, in a Hebrew layout)
  await openPage(page, "dashboard?view=breakdown&breakdown=month&month=2024-08");
  await expect(page.getByRole("combobox", { name: "Month" })).toHaveValue("2024-08");
  await page.keyboard.press(";");
  await expect(page).toHaveURL(/month=2024-09/);
  await page.keyboard.press("j");
  await expect(page).toHaveURL(/month=2024-08/);
  await typeInLayout(page, "ף", "Semicolon");
  await expect(page).toHaveURL(/month=2024-09/);

  // One direction by pressing its key: Right is ' now, the others stay
  await openPage(page, "settings");
  await setDirection(page, "right", "'");
  await expect.poll(() => apiGet(page, "/settings/keymap")).toEqual({ directions: { ...JKL, right: "'" }, overrides: {} });
  await expect(page.locator('tr[data-direction="right"] .changed')).toBeVisible();
  // Not Shift (it reorders), nor a fixed key, nor a key that clashes
  await page.getByRole("button", { name: "Change the key of left" }).click();
  const dialog = page.getByRole("dialog", { name: "Change the key" });
  await page.keyboard.press("H");
  await expect(dialog.getByRole("alert")).toContainText("without Shift");
  await page.keyboard.press("ArrowLeft");
  await expect(dialog.getByRole("alert")).toContainText("ArrowLeft keeps its own meaning");
  await page.keyboard.press("e");
  await expect(dialog).toBeHidden();
  await expect(page.getByRole("alert")).toContainText('e is both "Breakdown: previous month" and "Every group in cash flows and breakdowns, or the spending groups" (Dashboard: Breakdown)');
  await expect(page.locator('tr[data-direction="left"]')).toHaveClass(/\bclash\b/);
  await page.waitForTimeout(200);
  expect((await apiGet(page, "/settings/keymap")).directions).toEqual({ ...JKL, right: "'" });
  await setDirection(page, "left", "a");
  await expect(page.getByRole("alert")).toBeHidden();
  await expect.poll(async () => (await apiGet(page, "/settings/keymap")).directions).toEqual({ ...JKL, left: "a", right: "'" });
  await openPage(page, "dashboard?view=breakdown&breakdown=month&month=2024-08");
  await expect(page.getByRole("combobox", { name: "Month" })).toHaveValue("2024-08");
  await page.keyboard.press("'");
  await expect(page).toHaveURL(/month=2024-09/);
  await page.keyboard.press("a");
  await expect(page).toHaveURL(/month=2024-08/);

  // The vim keys back, at once
  await openPage(page, "settings");
  await page.getByRole("button", { name: "Directions: vim (h j k l)" }).click();
  await expect.poll(() => apiGet(page, "/settings/keymap")).toEqual({ directions: VIM, overrides: {} });
});

test("the arrows always work for the directions, whatever their keys", async ({ page }) => {
  await setKeymap(page, { directions: { left: "a", down: "s", up: "w", right: "d" }, overrides: { "list.down": [], "list.up": [] } });
  // Rows
  await openPage(page, "tags");
  await expect(page.locator("tr.active")).toBeVisible();
  expect(await activeIndex(page)).toBe("0");
  await page.keyboard.press("ArrowDown");
  await page.keyboard.press("ArrowDown");
  expect(await activeIndex(page)).toBe("2");
  await page.keyboard.press("ArrowUp");
  expect(await activeIndex(page)).toBe("1");

  // Reordering: Shift and the arrows
  await openPage(page, "rules");
  await expect(page.locator("tr.active")).toBeVisible();
  const before = (await apiGet(page, "/rules")).map((rule: { id: number }) => rule.id);
  await page.keyboard.press("Shift+ArrowDown");
  const moved = [before[1], before[0], ...before.slice(2)];
  await expect.poll(async () => (await apiGet(page, "/rules")).map((rule: { id: number }) => rule.id)).toEqual(moved);
  await page.keyboard.press("Shift+ArrowUp");
  await expect.poll(async () => (await apiGet(page, "/rules")).map((rule: { id: number }) => rule.id)).toEqual(before);

  // The breakdown's months
  await openPage(page, "dashboard?view=breakdown&breakdown=month&month=2024-08");
  await expect(page.getByRole("combobox", { name: "Month" })).toHaveValue("2024-08");
  await page.keyboard.press("ArrowRight");
  await expect(page).toHaveURL(/month=2024-09/);
  await page.keyboard.press("ArrowLeft");
  await expect(page).toHaveURL(/month=2024-08/);
});

test("letters by their key in a Hebrew layout, also those typing punctuation (Q and W); / stays / in English", async ({ page }) => {
  await openPage(page, "dashboard");
  await expect(page.locator(".cards h3").first()).toBeVisible();
  // English: w is the next rolling window
  await page.keyboard.press("w");
  await expect(page).toHaveURL(/rolling=12/);
  // Hebrew (shown by a letter typed: ז on the Z key): its W key types ', and is w
  await typeInLayout(page, "ז", "KeyZ");
  await typeInLayout(page, "'", "KeyW");
  await expect(page).toHaveURL(/rolling=(?!12\b)\d+/);

  // Its Q key types /, and is q: not the transactions' search
  await openPage(page, "transactions");
  await expect(page.locator("tbody[data-id]").first()).toBeVisible();
  const search = page.getByPlaceholder("Search descriptions (written and raw)");
  await typeInLayout(page, "ז", "KeyZ");
  await typeInLayout(page, "/", "KeyQ");
  await page.waitForTimeout(100);
  await expect(search).toBeVisible();
  await expect(search).not.toBeFocused();
  // Back in English, / is the search again
  await typeInLayout(page, "z", "KeyZ");
  await typeInLayout(page, "/", "Slash");
  await expect(search).toBeFocused();
});

test("rebound keys work on every page (sampled), with the go key and the nav's letters", async ({ page }) => {
  await setKeymap(page, {
    directions: VIM,
    overrides: {
      "go.prefix": ["'"],
      "go.tags": ["q"],
      "transactions.reverse": ["z"],
      "dashboard.view.trends": ["y"],
      "annotate.queue": ["M"],
      "settings.netByTag": ["N"],
      "help.open": ["F1"],
    },
  });
  await openPage(page, "annotate");
  // The nav shows a letter that isn't in the title after it
  const tags = page.locator("nav a", { hasText: "Tags" });
  await expect(tags).toHaveAttribute("title", "' then q");
  await expect(tags.locator("u.key")).toHaveText("q");
  await expect(page.locator(".legend")).toContainText("F1 shortcuts");

  // Annotate: M switches the queue, m no longer does
  const url = page.url();
  await page.keyboard.press("m");
  await page.waitForTimeout(100);
  expect(page.url()).toBe(url);
  await page.keyboard.press("M");
  await expect(page).not.toHaveURL(url);

  // The go key
  await page.keyboard.press("g");
  await page.keyboard.press("t");
  await page.waitForTimeout(100);
  await expect(page).toHaveURL(/#\/annotate/);
  await page.keyboard.press("'");
  await expect(page.locator(".pending")).toContainText("then a page's underlined letter");
  await page.keyboard.press("q");
  await expect(page).toHaveURL(/#\/tags$/);

  // Transactions: z reverses the sort
  await page.keyboard.press("'");
  await page.keyboard.press("t");
  await expect(page).toHaveURL(/#\/transactions/);
  const firstDate = () => page.locator("tr.main td.date").first().textContent();
  await expect(page.locator("tr.main").first()).toBeVisible();
  const newest = await firstDate();
  await page.keyboard.press("z");
  await expect.poll(firstDate).not.toBe(newest);

  // Dashboard: y opens the trends, and the view's button says so
  await page.keyboard.press("'");
  await page.keyboard.press("d");
  await expect(page.getByRole("button", { name: "Trends" })).toHaveAttribute("title", "Trends (y)");
  await page.keyboard.press("y");
  await expect(page).toHaveURL(/view=trends/);

  // Settings: N nets by tag
  await page.keyboard.press("'");
  await page.keyboard.press("s");
  // (the demo nets by tag)
  await expect(page.getByLabel(/Net incomes and expenses by tag/)).toBeChecked();
  await page.keyboard.press("N");
  await expect.poll(async () => (await apiGet(page, "/settings/dashboard")).net_by_tag).toBe(false);

  // The help: F1, with the new keys
  await page.keyboard.press("F1");
  const help = page.getByRole("dialog", { name: "Keyboard shortcuts" });
  await expect(help).toBeVisible();
  await expect(help).toContainText("Go to Tags");
  await expect(help.locator("dt", { hasText: /'\s*then\s*q/ })).toHaveCount(1);
});

test("keys typed into a field are text, never shortcuts, right after Enter or Tab", async ({ page }) => {
  await setKeymap(page, { directions: JKL, overrides: { "tags.rename": ["N"] } });
  await openPage(page, "annotate");
  await expect(page.locator("tbody[data-id]").first()).toBeVisible();
  const row = page.locator("tbody[data-id]").first();
  const url = page.url();
  // Every printable default key of the app, Annotate and its rows, with no delay
  const text = "?@IgdtiaoArsxXObmu/[]jkl;hpPwWeEnRv+-=,.";
  await page.keyboard.press("Enter");
  await page.keyboard.type(text);
  await expect(row.locator('[data-field="description"]')).toHaveValue(text);
  await page.keyboard.press("Tab");
  await page.keyboard.press("Control+a");
  await page.keyboard.type(text);
  await expect(row.locator('[data-field="tag"]')).toHaveValue(text);
  await page.keyboard.press("Tab");
  await page.keyboard.type(text);
  await expect(row.locator('[data-field="subtag"]')).toHaveValue(text);
  expect(page.url()).toBe(url);
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page.locator(".pending")).toHaveCount(0);
});
