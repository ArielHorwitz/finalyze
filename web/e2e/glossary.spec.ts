// The glossary: tooltips of labels (mouse only), their entries, and the
// Glossary page.
import { expect, test, type Page } from "@playwright/test";
import { apiGet, goTo, openPage, resetDemoData } from "./helpers";

test.beforeAll(resetDemoData);

const tooltip = (page: Page) => page.locator(".term-tooltip");

/** Open the dashboard (or a page), once its data is there. */
async function open(page: Page, path: string) {
  await openPage(page, path);
  if (path.startsWith("dashboard")) await expect(page.locator(".content .panel").first()).toBeVisible();
}

/** The keys of the labels with a tooltip on screen. */
async function shownKeys(page: Page): Promise<string[]> {
  return page.locator("[data-term]").evaluateAll((elements) => elements.map((element) => (element as HTMLElement).dataset.term!));
}

/** What Tab focuses, in order, from the top of the page: a description of
 * each element, failing if a tooltip or a label of one gets the focus. */
async function tabOrder(page: Page, steps = 60): Promise<string[]> {
  await page.locator("body").click({ position: { x: 1, y: 1 } });
  const order = [];
  for (let step = 0; step < steps; step++) {
    await page.keyboard.press("Tab");
    const focused = await page.evaluate(() => {
      const element = document.activeElement as HTMLElement | null;
      if (!element || element === document.body) return null;
      if (element.closest(".term, .term-tooltip")) return "TOOLTIP";
      return `${element.tagName.toLowerCase()}:${(element.getAttribute("aria-label") ?? element.textContent ?? "").trim().slice(0, 30)}`;
    });
    expect(focused).not.toBe("TOOLTIP");
    if (focused) order.push(focused);
  }
  return order;
}

test("hovering a label explains it, and links to its entry", async ({ page }) => {
  const entries = await apiGet(page, "/glossary");
  const kept = entries.find((entry: { key: string }) => entry.key === "kept");
  await open(page, "dashboard");
  await page.locator(".cards .label [data-term=kept]").hover();
  await expect(tooltip(page)).toContainText(kept.short);
  // Dynamic context: the months the baseline is of
  await page.locator(".cards h3 [data-term=baseline]").hover();
  await expect(tooltip(page)).toContainText(/6-mo avg before \w+ \(\w+( \d{4})?–\w+ \d{4}\)/);
  // The mouse can move into it, and follow its link
  await tooltip(page).getByRole("link").click();
  await expect(page).toHaveURL(/#\/glossary\?baseline$/);
  await expect(page.locator("#term-baseline")).toHaveClass(/selected/);
  await expect(page.locator("#term-baseline")).toBeInViewport();
  await expect(tooltip(page)).toHaveCount(0);
});

test("tooltips are for the mouse: the keyboard never reaches them, and they leave", async ({ page }) => {
  await open(page, "dashboard");
  const before = await tabOrder(page);
  await page.locator(".cards .label [data-term=runway]").hover();
  await expect(tooltip(page)).toBeVisible();
  // Nothing in it can be tabbed to
  const tabbable = await tooltip(page).locator("a, button, input, [tabindex]").evaluateAll((elements) => elements.map((element) => (element as HTMLElement).tabIndex));
  expect(tabbable.every((index) => index < 0)).toBe(true);
  // A key closes it, and Tab goes where it went without it
  expect(await tabOrder(page)).toEqual(before);
  await expect(tooltip(page)).toHaveCount(0);
  // Labels themselves aren't focusable
  const focusable = await page.locator("[data-term]").evaluateAll((elements) => elements.filter((element) => (element as HTMLElement).tabIndex >= 0).length);
  expect(focusable).toBe(0);
});

test("every label's entry exists, on every view and page", async ({ page }) => {
  const keys = new Set((await apiGet(page, "/glossary")).map((entry: { key: string }) => entry.key));
  const views = ["overview", "balance", "flow", "breakdown", "trends", "monthly", "ledger"];
  const pages = [...views.map((view) => `dashboard?view=${view}`), "annotate", "tags", "accounts", "ingest", "rules", "orphans", "settings"];
  const used = new Set<string>();
  for (const path of pages) {
    await open(page, path);
    await expect(page.locator("main [data-term]").first()).toBeVisible();
    for (const key of await shownKeys(page)) used.add(key);
  }
  expect([...used].filter((key) => !keys.has(key))).toEqual([]);
  expect(used.size).toBeGreaterThan(70);
});

test("the glossary page: by section, searchable, from the help, the nav and g l", async ({ page }) => {
  await openPage(page, "annotate");
  await page.keyboard.press("?");
  await page.getByRole("dialog").getByRole("link", { name: "glossary" }).click();
  await expect(page).toHaveURL(/#\/glossary$/);
  await expect(page.getByRole("heading", { name: "Scope of the dashboard" })).toBeVisible();
  await expect(page.locator(".entry")).not.toHaveCount(0);
  await page.keyboard.press("/");
  await page.keyboard.type("runway");
  await expect(page.locator(".entry#term-runway")).toBeVisible();
  await expect(page.locator(".entry#term-ledger")).toHaveCount(0);
  // Related entries link to each other
  await page.locator("#term-runway .related").getByRole("link", { name: "Available" }).click();
  await expect(page).toHaveURL(/#\/glossary\?available$/);
  // The nav's last tab, and g l, open it
  await openPage(page, "tags");
  const tab = page.locator("header nav a").last();
  await expect(tab).toHaveText("Glossary");
  await tab.click();
  await expect(page).toHaveURL(/#\/glossary$/);
  await expect(tab).toHaveClass(/current/);
  await openPage(page, "tags");
  await goTo(page, "l");
  await expect(page).toHaveURL(/#\/glossary$/);
});
