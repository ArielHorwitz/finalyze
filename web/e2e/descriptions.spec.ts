// Descriptions may have several lines. In the editor, Shift+Enter adds a line and
// Enter is done with it (back to moving between rows, the edit kept as a
// draft); it grows to fit while editing. Tables show a description's first line and
// how many more, with the whole description (and the raw one) on hover. Editing a
// row starts at its description, Enter in the tag and subtag still picks a
// suggestion and goes on to the kind, and Ctrl+Enter submits.
import { expect, test, type Locator, type Page } from "@playwright/test";
import { apiGet, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

async function openRows(page: Page, path: string) {
  await openPage(page, path);
  await expect(page.locator("tbody[data-id]").first()).toBeVisible();
}

/** The demo's overridden annotation, and its transaction's raw description. */
const GYM = "The gym's summer promo month\ncounts as fun, not health";
const GYM_RAW = "כושר פלוס - מנוי חודשי";

const focusedFields = (page: Page) => page.locator("input:focus, textarea:focus");
const activeIndex = (page: Page) => page.locator("tbody[data-id]:has(tr.active)").getAttribute("data-index");
const height = async (locator: Locator) => (await locator.boundingBox())!.height;

for (const path of ["annotate", "transactions?status=untagged"]) {
  test(`${path.split("?")[0]}: Enter edits from the description, Enter in the tag picks and goes to the kind, Enter in the description goes back to the rows`, async ({ page }) => {
    await openRows(page, path);
    const row = page.locator("tbody[data-id]").first();
    const id = await row.getAttribute("data-id");
    await page.keyboard.press("Enter");
    const description = row.locator('[data-field="description"]');
    await expect(description).toBeFocused();
    await page.keyboard.press("Tab");
    await expect(row.locator('[data-field="tag"]')).toBeFocused();
    await page.keyboard.type("dining");
    await expect(page.getByRole("option", { name: /food \/ dining out/ })).toBeVisible();
    await page.keyboard.press("Enter");
    await expect(row.locator('[data-field="kind"]')).toBeFocused();
    await expect(row.locator('[data-field="subtag"]')).toHaveValue("dining out");
    await page.keyboard.press("Escape");
    await page.keyboard.press("Enter");
    await expect(description).toBeFocused();
    await page.keyboard.type("lunch with Bob");
    await page.keyboard.press("Enter");

    // Out of the field, the draft kept (not submitted)
    await expect(focusedFields(page)).toHaveCount(0);
    await expect(description).toHaveValue("lunch with Bob");
    await expect(row.locator(".badge.edited")).toBeVisible();
    expect((await apiGet(page, `/transactions/${id}`)).annotation?.description ?? null).not.toBe("lunch with Bob");

    // Row keys work again: j moves down, and Ctrl+Enter submits the draft
    await expect(row.locator("tr.main")).toHaveClass(/\bactive\b/);
    await page.keyboard.press("j");
    await expect(page.locator("tbody[data-id]").nth(1).locator("tr.main")).toHaveClass(/\bactive\b/);
    await page.keyboard.press("Control+Enter");
    await expect(page.getByText(/^Saved \d+ annotations?/)).toBeVisible();
    expect((await apiGet(page, `/transactions/${id}`)).annotation).toMatchObject({ description: "lunch with Bob" });
  });
}

test("Shift+Enter adds a line and the editor grows; Enter is done, showing the first line and +N lines; Ctrl+Enter submits", async ({ page }) => {
  await openRows(page, "annotate");
  const row = page.locator("tbody[data-id]").first();
  const id = await row.getAttribute("data-id");
  await page.keyboard.press("Enter");
  const description = row.locator('[data-field="description"]');
  await expect(description).toBeFocused();
  const oneLine = await height(description);
  await page.keyboard.type("first");
  await page.keyboard.press("Shift+Enter");
  await page.keyboard.type("second");
  await page.keyboard.press("Shift+Enter");
  await page.keyboard.type("third");
  await expect(description).toBeFocused();
  await expect(description).toHaveValue("first\nsecond\nthird");
  await expect.poll(() => height(description)).toBeGreaterThan(oneLine * 2);
  await expect(row.locator(".more-lines")).toHaveCount(0);

  // Done: one line again, the first, with a marker and the whole description on hover
  await page.keyboard.press("Enter");
  await expect(focusedFields(page)).toHaveCount(0);
  await expect(description).toHaveValue("first\nsecond\nthird");
  await expect.poll(() => height(description)).toBeLessThan(oneLine + 2);
  await expect(row.locator(".more-lines")).toHaveText("+2 lines");
  await expect(description).toHaveAttribute("title", /^first\nsecond\nthird\nRaw description: ./);

  // Back in it, it grows again, and keeps every line
  await description.click();
  await expect.poll(() => height(description)).toBeGreaterThan(oneLine * 2);
  await page.keyboard.press("Escape");
  await page.keyboard.press("Control+Enter");
  await expect(page.getByText(/^Saved 1 annotation/)).toBeVisible();
  expect((await apiGet(page, `/transactions/${id}`)).annotation).toMatchObject({ description: "first\nsecond\nthird" });
});

test("a split part's description: Shift+Enter adds a line, Enter goes back to the rows", async ({ page }) => {
  await openRows(page, "annotate");
  const row = page.locator("tbody[data-id]").first();
  await page.keyboard.press("s");
  await expect(row.locator('[data-field="part-1-amount"]')).toBeFocused();
  const partDescription = row.locator('[data-field="part-1-description"]');
  await partDescription.focus();
  await page.keyboard.type("the tip");
  await page.keyboard.press("Shift+Enter");
  await page.keyboard.type("in cash");
  await page.keyboard.press("Enter");
  await expect(focusedFields(page)).toHaveCount(0);
  await expect(partDescription).toHaveValue("the tip\nin cash");
  await expect(row.locator(".more-lines")).toHaveText("+1 line");
  expect(await activeIndex(page)).toBe(await row.getAttribute("data-index"));
});

test("the batch dialog's description: Shift+Enter adds a line, Enter is done without applying, Ctrl+Enter applies", async ({ page }) => {
  await openRows(page, "annotate");
  const row = page.locator("tbody[data-id]").first();
  await page.keyboard.press("x");
  await page.keyboard.press("b");
  const dialog = page.getByRole("dialog", { name: "Batch edit" });
  const description = dialog.getByLabel("Description", { exact: true });
  await description.click();
  await page.keyboard.type("checked with the bank");
  await page.keyboard.press("Shift+Enter");
  await page.keyboard.type("twice");
  await page.keyboard.press("Enter");
  await expect(description).not.toBeFocused();
  await expect(dialog).toBeVisible();
  await expect(description).toHaveValue("checked with the bank\ntwice");
  await expect(dialog.getByLabel("Include the description")).toBeChecked();
  await page.keyboard.press("Control+Enter");
  await expect(dialog).toBeHidden();
  await expect(row.locator('[data-field="description"]')).toHaveValue("checked with the bank\ntwice");
  await expect(row.locator(".more-lines")).toHaveText("+1 line");
});

test("tables show a description's first line and +N lines, with the whole description on hover: orphans, the ledger, transactions, the rows of a number", async ({ page }) => {
  // The demo's orphan and its overridden annotation have descriptions of two lines
  await openPage(page, "orphans");
  const orphan = page.locator(".description-text", { hasText: "Order cancelled" });
  await expect(orphan).toHaveText("Order cancelled, the seller never shipped+1 line");
  await expect(orphan).toHaveAttribute("title", /^Order cancelled, the seller never shipped\nGone from the next export, the refund cancelled it out\nRaw description: ./);

  await openPage(page, "dashboard?view=ledger&period=all");
  await page.getByRole("textbox", { name: "Search /" }).fill("promo month");
  // In the Description column, shown by default
  const ledgerDescription = page.locator("table .description-text", { hasText: "promo month" });
  await expect(ledgerDescription).toHaveText("The gym's summer promo month+1 line");
  await expect(ledgerDescription).toHaveAttribute("title", `${GYM}\nRaw description: ${GYM_RAW}`);
  await expect(ledgerDescription.locator(".more-lines")).toBeVisible();

  // In Transactions, the editor (a rule's row keeps its description)
  await openPage(page, "transactions?search=promo%20month");
  const row = page.locator("tbody[data-id]").first();
  await expect(row.locator('[data-field="description"]')).toHaveValue(GYM);
  await expect(row.locator(".more-lines")).toHaveText("+1 line");

  // The rows behind a number: a description set on a row of the month's expenses
  const expense = (await apiGet(page, "/transactions?descending=true&limit=200")).items.find(
    (item: { amount: number; rule: unknown; date: string }) => item.amount < 0 && !item.rule && item.date.startsWith("2025-06"),
  );
  const saved = await page.request.put(`/api/transactions/${expense.id}/annotation`, {
    data: { description: "shared with Kim\nshe paid half", parts: [{ tag: "food", subtag: "groceries" }] },
  });
  expect(saved.ok()).toBe(true);
  await openPage(page, "dashboard?period=3m");
  await page.locator(".cards .card").filter({ hasText: /^Expenses/ }).click();
  const panel = page.getByRole("complementary", { name: /Rows of Expenses · Jun 2025/ });
  const drillDescription = panel.locator(".description-text", { hasText: "shared with Kim" });
  await expect(drillDescription).toHaveText("shared with Kim+1 line");
  await expect(drillDescription).toHaveAttribute("title", /^shared with Kim\nshe paid half\nRaw description: ./);
});

test("one Description column: the description, with the raw description on hover and under it while editing", async ({ page }) => {
  await openRows(page, "transactions?search=promo%20month");
  const headers = page.locator("thead th");
  await expect(headers.filter({ hasText: /^Raw description$/ })).toHaveCount(0);
  await expect(headers.filter({ hasText: /^Description$/ })).toHaveCount(1);
  const row = page.locator("tbody[data-id]").first();
  const description = row.locator('[data-field="description"]');
  await expect(description).toHaveValue(GYM);
  await expect(description).toHaveAttribute("title", `${GYM}\nRaw description: ${GYM_RAW}`);
  const rawInEditor = row.locator(".raw-in-editor");
  await expect(rawInEditor).toBeHidden();
  await page.keyboard.press("Enter");
  await expect(rawInEditor).toHaveText(GYM_RAW);
  await expect(rawInEditor).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(rawInEditor).toBeHidden();
});

test("without a description, the raw description stands in, in italics, and goes to the hover once one is written", async ({ page }) => {
  await openRows(page, "transactions?search=FRESHWAY%20MARKET");
  const row = page.locator("tbody[data-id]").first();
  const description = row.locator('[data-field="description"]');
  await expect(description).toHaveValue("");
  await expect(description).toHaveAttribute("placeholder", "FRESHWAY MARKET 12");
  await expect(description).toHaveAttribute("title", "Raw description (no description written): FRESHWAY MARKET 12");
  await expect(row.locator("td.description")).toHaveClass(/raw-only/);
  await expect(row.locator(".raw-in-editor")).toHaveCount(0);
  // Written: the raw description is on hover, and under it while editing
  await description.click();
  await page.keyboard.type("weekly groceries");
  await expect(row.locator(".raw-in-editor")).toHaveText("FRESHWAY MARKET 12");
  await expect(description).toHaveAttribute("title", "weekly groceries\nRaw description: FRESHWAY MARKET 12");
  await expect(row.locator("td.description")).not.toHaveClass(/raw-only/);
  await page.keyboard.press("Tab");
  await expect(row.locator('[data-field="tag"]')).toBeFocused();
  await expect(row.locator(".raw-in-editor")).toBeVisible(); // in any field of the row
  await page.keyboard.press("Escape");
  await page.keyboard.press("Control+Enter");
  await expect(row.locator(".badge.edited")).toHaveCount(0);

  // The ledger: the description, the raw one on hover; else the raw one alone
  await openPage(page, "dashboard?view=ledger&period=all");
  await page.getByRole("textbox", { name: "Search /" }).fill("FRESHWAY MARKET");
  const described = page.locator("table .transaction-description", { hasText: "weekly groceries" });
  await expect(described).toHaveText("weekly groceries");
  await expect(described.locator(".description-text")).toHaveAttribute("title", "weekly groceries\nRaw description: FRESHWAY MARKET 12");
  await expect(page.locator("table .transaction-description.raw-only").first()).toHaveText("FRESHWAY MARKET 12");
  await expect(page.locator("table .transaction-description.raw-only").first()).toHaveCSS("font-style", "italic");
});

test("Enter elsewhere is unchanged: a dialog's Enter confirms, the tag without suggestions stays", async ({ page }) => {
  // A dialog's Enter (Tags: rename, Enter saves)
  await openPage(page, "tags");
  await page.getByRole("row", { name: /dining out/ }).click();
  await page.keyboard.press("n");
  const dialog = page.getByRole("dialog", { name: /Rename/ });
  await dialog.getByRole("textbox").fill("eating out");
  await page.keyboard.press("Enter");
  await expect(dialog).toBeHidden();
  await expect(page.getByRole("row", { name: /eating out/ })).toBeVisible();

  await openRows(page, "annotate");
  const row = page.locator("tbody[data-id]").first();
  await page.keyboard.press("Enter");
  await page.keyboard.press("Tab");
  const tag = row.locator('[data-field="tag"]');
  await expect(tag).toBeFocused();
  await page.keyboard.type("zzzz-no-such-tag");
  await expect(page.getByRole("listbox")).toBeHidden();
  await page.keyboard.press("Enter");
  await expect(tag).toBeFocused();
});
