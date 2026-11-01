// Tab and Shift+Tab move through a row's fields as their columns go (the
// description first), empty or not: the tag's dropdown (a long, scrollable list
// while the tag is empty) is not a stop.
import { expect, test, type Locator, type Page } from "@playwright/test";
import { openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

/** Open a page of rows, once they are there: keys before that find no row to edit. */
async function openRows(page: Page, path: string) {
  await openPage(page, path);
  await expect(page.locator("tbody[data-id]").first()).toBeVisible();
}

/** Press Tab (or Shift+Tab) through the fields, expecting each to be focused in turn. */
async function tabThrough(page: Page, row: Locator, fields: string[], key = "Tab") {
  for (const field of fields) {
    await page.keyboard.press(key);
    await expect(row.locator(`[data-field="${field}"]`)).toBeFocused();
  }
}

// (Transactions' untagged ones: rules' rows have no tag fields)
for (const path of ["annotate", "transactions?status=untagged"]) {
  test(`${path.split("?")[0]}: Tab follows the columns, from the description to an empty tag, the subtag and kind, on to the next row, and Shift+Tab back`, async ({ page }) => {
    await openRows(page, path);
    const row = page.locator("tbody[data-id]").nth(0);
    const next = page.locator("tbody[data-id]").nth(1);
    await page.keyboard.press("Enter");
    await expect(row.locator('[data-field="description"]')).toBeFocused();
    await tabThrough(page, row, ["tag"]);
    const tag = row.locator('[data-field="tag"]');
    await page.keyboard.press("Backspace");
    await expect(tag).toHaveValue("");
    await expect(page.getByRole("listbox")).toBeVisible();

    await tabThrough(page, row, ["subtag", "kind"]);
    await tabThrough(page, next, ["description"]);
    await tabThrough(page, row, ["kind", "subtag", "tag", "description"], "Shift+Tab");
    // A subtag without a tag is only an error on submit
    await tabThrough(page, row, ["tag", "subtag"]);
    await page.keyboard.type("anything");
    await tabThrough(page, row, ["kind"]);
    await expect(tag).toHaveValue("");
    await expect(row.locator('[data-field="subtag"]')).toHaveValue("anything");
  });
}

test("a split part: Tab moves through its empty tag, subtag and kind, and Shift+Tab back through its amount and description to the first part's", async ({ page }) => {
  await openRows(page, "annotate");
  const row = page.locator("tbody[data-id]").nth(0);
  await page.keyboard.press("s");
  await expect(row.locator('[data-field="part-1-amount"]')).toBeFocused();

  await tabThrough(page, row, ["part-1-tag", "part-1-subtag", "part-1-kind"]);
  await tabThrough(page, row, ["part-1-subtag", "part-1-tag", "part-1-amount", "part-1-description"], "Shift+Tab");
  // The first part's fields, then the transaction's description
  await tabThrough(page, row, ["part-0-kind", "part-0-subtag", "part-0-tag", "part-0-amount", "part-0-description", "description"], "Shift+Tab");
});

test("Enter in a split part's tag picks the pair and goes on to the part's kind", async ({ page }) => {
  await openRows(page, "annotate");
  const row = page.locator("tbody[data-id]").nth(0);
  await page.keyboard.press("s");
  await tabThrough(page, row, ["part-1-tag"]);
  await page.keyboard.type("dining");
  await expect(page.getByRole("option", { name: /food \/ dining out/ })).toBeVisible();
  await page.keyboard.press("Enter");
  await expect(row.locator('[data-field="part-1-kind"]')).toBeFocused();
  await expect(row.locator('[data-field="part-1-subtag"]')).toHaveValue("dining out");
});

test("Tab picks a suggestion only after moving in the dropdown, then goes to the next field", async ({ page }) => {
  await openRows(page, "annotate");
  const row = page.locator("tbody[data-id]").nth(0);
  const tag = row.locator('[data-field="tag"]');
  const subtag = row.locator('[data-field="subtag"]');
  await page.keyboard.press("Enter");
  await tabThrough(page, row, ["tag"]);

  // Typed but not navigated: Tab keeps the text
  await page.keyboard.type("dining");
  await expect(page.getByRole("option", { name: /food \/ dining out/ })).toBeVisible();
  await tabThrough(page, row, ["subtag"]);
  await expect(tag).toHaveValue("dining");

  // Navigated: Tab picks the pair and moves on to the subtag, as Tab does
  await tabThrough(page, row, ["tag"], "Shift+Tab");
  await page.keyboard.type("dining");
  await page.keyboard.press("ArrowDown");
  await page.keyboard.press("ArrowUp");
  await tabThrough(page, row, ["subtag"]);
  await expect(tag).toHaveValue("food");
  await expect(subtag).toHaveValue("dining out");
});

test("the batch dialog: Tab moves from an empty tag to the subtag", async ({ page }) => {
  await openRows(page, "annotate");
  await page.keyboard.press("b");
  const dialog = page.getByRole("dialog", { name: "Batch edit" });
  await expect(dialog.getByPlaceholder("tag", { exact: true })).toBeFocused();
  await expect(page.getByRole("listbox")).toBeVisible();
  await page.keyboard.press("Tab");
  await expect(dialog.getByPlaceholder("subtag")).toBeFocused();
  await page.keyboard.press("Shift+Tab");
  await expect(dialog.getByPlaceholder("tag", { exact: true })).toBeFocused();
});
