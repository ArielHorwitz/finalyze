import { expect, test, type Page } from "@playwright/test";
import { goTo, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

// The pages and their letters, in the nav's order
const PAGES = [
  ["dashboard", "d"],
  ["transactions", "t"],
  ["ingest", "i"],
  ["annotate", "a"],
  ["orphans", "o"],
  ["tags", "g"],
  ["rules", "r"],
  ["accounts", "c"],
  ["settings", "s"],
  ["glossary", "l"],
] as const;

const url = (path: string) => new RegExp(`#/${path}$`);

/** Go to a page with g and its letter, and wait for it. */
async function go(page: Page, path: string) {
  const letter = PAGES.find(([other]) => other === path)![1];
  await goTo(page, letter);
  await expect(page).toHaveURL(url(path));
}

test("g then a letter goes from every page to every other", async ({ page }) => {
  test.slow(); // 90 pairs of pages
  await openPage(page, "annotate");
  for (const [from] of PAGES) {
    for (const [to] of PAGES) {
      if (to === from) continue;
      await go(page, from);
      await go(page, to);
    }
  }
});

test("the nav groups the pages (see, routine, setup, reference), and the dashboard is the landing page", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Dashboard" })).toBeVisible();
  await expect(page.locator("header nav a.current")).toHaveText("Dashboard");
  // A divider between the groups, and only there
  const items = await page.locator("header nav > *").evaluateAll((elements) =>
    elements.map((element) => (element.classList.contains("divider") ? "|" : element.textContent!.trim())),
  );
  expect(items).toEqual(["Dashboard", "Transactions", "|", "Ingest", "Annotate", "Orphans", "|", "Tags", "Rules", "Accounts", "Settings", "|", "Glossary"]);
  // An unknown page lands there too
  await page.goto("/#/nowhere");
  await expect(page.locator("header nav a.current")).toHaveText("Dashboard");
});

test("the nav underlines the letters, and the digits do nothing", async ({ page }) => {
  await openPage(page, "annotate");
  await expect(page.locator("nav a")).toHaveText(["Dashboard", "Transactions", "Ingest", "Annotate", "Orphans", "Tags", "Rules", "Accounts", "Settings", "Glossary"]);
  await expect(page.locator("nav a u")).toHaveText(["D", "T", "I", "A", "O", "g", "R", "c", "S", "l"]);
  await expect(page.locator("nav a").nth(5)).toHaveAttribute("title", "g then g");
  for (const digit of ["2", "5"]) {
    await page.keyboard.press(digit);
    await page.keyboard.press("g");
    await page.keyboard.press(digit);
  }
  await page.waitForTimeout(200);
  await expect(page).toHaveURL(url("annotate"));
});

test("g shows that it waits for a letter, until Escape or a timeout", async ({ page }) => {
  await openPage(page, "transactions");
  await expect(page.locator("tbody[data-id]").first()).toBeVisible();
  const pending = page.locator(".pending");
  await page.keyboard.press("g");
  await expect(pending).toContainText("then a page's underlined letter");
  await expect(page.locator("nav")).toHaveClass(/going/);
  // Escape cancels it, and only that: the next letter is a plain key again
  await page.keyboard.press("Escape");
  await expect(pending).toBeHidden();
  await expect(page.locator("nav")).not.toHaveClass(/going/);
  await page.keyboard.press("r"); // Transactions: toggle needs-review, not Rules
  await expect(page).toHaveURL(url("transactions"));
  await expect(page.locator("tbody.dirty")).toHaveCount(1);
  await page.keyboard.press("u");
  // It also times out
  await page.keyboard.press("g");
  await expect(pending).toBeVisible();
  await expect(pending).toBeHidden({ timeout: 3000 });
  await page.keyboard.press("o"); // reverse the sort, not Orphans
  await expect(page).toHaveURL(url("transactions"));
});

test("g then a letter works with the focus on buttons, links, checkboxes and chips", async ({ page }) => {
  // A queue tab of Annotate
  await openPage(page, "annotate");
  await page.getByRole("tab", { name: /Needs review/ }).click();
  await expect(page.getByRole("tab", { name: /Needs review/ })).toBeFocused();
  await go(page, "transactions");
  // A row's checkbox
  await page.locator("tbody[data-id] input[type=checkbox]").first().click();
  await expect(page.locator("tbody[data-id] input[type=checkbox]").first()).toBeFocused();
  await go(page, "dashboard");
  // The dashboard's view button and a period preset
  await page.getByRole("button", { name: "Balance", exact: true }).click();
  await page.getByRole("button", { name: "3M", exact: true }).click();
  await expect(page.getByRole("button", { name: "3M", exact: true })).toBeFocused();
  await go(page, "transactions");
  // An account chip of the filters
  await page.locator(".chip").first().click();
  await expect(page.locator(".chip").first()).toBeFocused();
  await go(page, "accounts");
  // A link of the nav
  await page.locator("nav a", { hasText: "Ingest" }).click();
  await expect(page).toHaveURL(url("ingest"));
  await expect(page.locator("nav a", { hasText: "Ingest" })).toBeFocused();
  await go(page, "orphans");
  // The Ingest page's buttons
  await go(page, "ingest");
  await page.getByRole("button", { name: "Check again" }).click();
  await go(page, "settings");
});

test("g then a letter works after a dropdown, a select, and Escape out of a field", async ({ page }) => {
  // The tag input's suggestions: one Escape leaves the cell
  await openPage(page, "annotate");
  await expect(page.locator("tbody[data-id]").first()).toBeVisible();
  await page.keyboard.press("Enter");
  await page.keyboard.press("Tab");
  const tagInput = page.locator("tbody.active input[data-field=tag]");
  await expect(tagInput).toBeFocused();
  await page.keyboard.type("gro");
  await expect(page.getByRole("option", { name: /groceries/ }).first()).toBeVisible();
  // While typing, g and a letter are typed
  await goTo(page, "t");
  await expect(tagInput).toHaveValue("grogt");
  await expect(page).toHaveURL(url("annotate"));
  await page.keyboard.press("Escape");
  await expect(tagInput).not.toBeFocused();
  await page.keyboard.press("u"); // undo the typing
  await go(page, "transactions");

  // A select picked with the mouse
  await expect(page.locator(".page-header .summary")).toContainText(/of \d+/);
  const status = page.locator("select:visible").first();
  await status.click();
  await status.selectOption({ index: 1 });
  await page.evaluate(() => {
    // selectOption doesn't press the pointer: do as a mouse pick does
    const select = document.querySelector("select")!;
    select.focus();
    window.dispatchEvent(new PointerEvent("pointerdown"));
    select.dispatchEvent(new Event("change", { bubbles: true }));
  });
  await expect(status).not.toBeFocused();
  await go(page, "tags");

  // The search, a date and a number field: Escape gives the keys back
  await go(page, "transactions");
  await page.keyboard.press("/");
  await page.keyboard.type("פיצה");
  await page.keyboard.press("Escape");
  await go(page, "rules");
  await go(page, "transactions");
  await page.getByLabel("From", { exact: true }).click();
  await page.keyboard.press("Escape");
  await go(page, "settings");
  await page.locator("input[type=number]").first().click();
  await page.keyboard.press("Escape");
  await go(page, "annotate");

  // The dashboard's tags popover
  await go(page, "dashboard");
  await page.getByRole("button", { name: /^all ▾/ }).click();
  await go(page, "accounts");
});

test("g then a letter leaves the dashboard's rows panel and the help, but not a dialog", async ({ page }) => {
  await openPage(page, "dashboard");
  await page.locator(".cards .card").filter({ hasText: /^Expenses/ }).click();
  const panel = page.getByRole("complementary", { name: /Rows of Expenses/ });
  await expect(panel).toBeVisible();
  await expect(panel).toBeFocused();
  await go(page, "tags");
  await expect(panel).toHaveCount(0);

  // The help lists the letters, and g goes on from it
  await page.keyboard.press("?");
  const help = page.getByRole("dialog", { name: "Keyboard shortcuts" });
  await expect(help).toContainText("Go to Tags");
  await expect(help.locator("dt", { has: page.locator("kbd", { hasText: /^c$/ }) }).filter({ hasText: "then" }).first()).toBeVisible();
  await go(page, "rules");
  await expect(help).toHaveCount(0);

  // A dialog with something to lose keeps the keyboard
  await page.keyboard.press("c"); // a new rule
  const editor = page.getByRole("dialog");
  await expect(editor).toBeVisible();
  await page.locator("body").focus();
  await editor.focus();
  await goTo(page, "t");
  await expect(page).toHaveURL(url("rules"));
  await expect(editor).toBeVisible();
});

test("g then a letter works in a Hebrew keyboard layout, by the key's position", async ({ page }) => {
  await openPage(page, "annotate");
  const press = (key: string, code: string) =>
    page.evaluate(([key, code]) => {
      window.dispatchEvent(new KeyboardEvent("keydown", { key, code, bubbles: true, cancelable: true }));
    }, [key, code]);
  // "ע" is on the G key, "א" on T, and "ג" on D
  await press("ע", "KeyG");
  await press("א", "KeyT");
  await expect(page).toHaveURL(url("transactions"));
  await press("ע", "KeyG");
  await press("ע", "KeyG");
  await expect(page).toHaveURL(url("tags"));
  await press("ע", "KeyG");
  await press("ג", "KeyD");
  await expect(page).toHaveURL(url("dashboard"));
});

test("keys right after g and a letter go to the new page, and Back returns", async ({ page }) => {
  await openPage(page, "transactions");
  await expect(page.locator(".page-header .summary")).toContainText(/of \d+/);
  // Pressed at once, before the browser would announce a hash change
  await goTo(page, "a");
  await page.keyboard.press("m");
  await expect(page).toHaveURL(/#\/annotate\?review$/);
  await go(page, "rules");
  await page.goBack();
  await expect(page).toHaveURL(/#\/annotate\?review$/);
  await expect(page.getByRole("tab", { name: /Needs review/ })).toHaveAttribute("aria-selected", "true");
  await page.goBack();
  await expect(page).toHaveURL(url("transactions"));
  await expect(page.getByRole("heading", { name: "Transactions" })).toBeVisible();
});
