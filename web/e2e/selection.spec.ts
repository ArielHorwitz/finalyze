import { expect, test, type Page } from "@playwright/test";
import { apiGet, apiPost, goTo, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

const NONE_SELECTED = "Applies to 0 rows (none selected). Select rows to edit.";
const summary = (page: Page) => page.locator(".page-header .summary");
/** How many parking charges there are, searched for as PARKO. */
const parkingCount = async (page: Page) => (await apiGet(page, "/transactions?search=PARKO")).total;

test("Annotate: batch edit with nothing selected applies to nothing", async ({ page }) => {
  await openPage(page, "annotate");
  const rows = page.locator("tbody[data-id]");
  await expect(rows).toHaveCount(12);

  await page.keyboard.press("b");
  const dialog = page.getByRole("dialog", { name: "Batch edit" });
  await expect(dialog).toContainText(NONE_SELECTED);
  await page.keyboard.type("groceries");
  await page.keyboard.press("Enter");
  await expect(dialog.getByRole("button", { name: /^Apply/ })).toBeDisabled();
  await page.keyboard.press("Control+Enter");
  await expect(dialog).toBeVisible();
  await page.keyboard.press("Escape");
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);
  await expect(page.locator("tbody.dirty")).toHaveCount(0);
  await expect(page.getByRole("button", { name: /^Submit/ })).toBeDisabled();

  // All of them only when explicitly selected
  await page.keyboard.press("Control+a");
  await expect(summary(page)).toContainText("12 selected");
  await page.keyboard.press("b");
  await expect(dialog).toContainText("Applies to 12 selected rows");
  // Until a field is checked, there's nothing to apply
  await expect(dialog.getByRole("button", { name: /^Apply/ })).toBeDisabled();
  await page.keyboard.type("groceries");
  await page.keyboard.press("Enter");
  await expect(dialog.getByRole("button", { name: /^Apply/ })).toBeEnabled();
});

test("Transactions: batch apply with nothing selected applies to nothing, unless all matching is chosen", async ({ page }) => {
  await openPage(page, "transactions");
  const before = await apiGet(page, "/status");
  // Without filters, the choice says it means every transaction
  await page.getByRole("button", { name: "Batch apply" }).click();
  await expect(page.getByRole("dialog")).toContainText(`all ${before.transactions} transactions (no filters set)`);
  await page.getByRole("dialog").getByRole("button", { name: "Cancel" }).click();
  await page.keyboard.press("/");
  await page.keyboard.type("PARKO");
  await expect(summary(page)).toContainText(`of ${await parkingCount(page)}`);
  await page.keyboard.press("Escape");

  await page.keyboard.press("b");
  const dialog = page.getByRole("dialog", { name: "Batch apply" });
  await expect(dialog).toContainText(NONE_SELECTED);
  await expect(dialog.getByLabel("the 0 selected")).toBeChecked();
  await page.keyboard.type("dining");
  await page.keyboard.press("Enter");
  const preview = dialog.getByRole("button", { name: /^Preview/ });
  await expect(preview).toBeDisabled();
  await page.keyboard.press("Control+Enter");
  await page.keyboard.press("Control+Enter");
  await expect(dialog).not.toContainText("to change");
  expect(await apiGet(page, "/status")).toEqual(before);

  // The filter's matches are a separate, explicit choice, still previewed first
  const parking = await parkingCount(page);
  await dialog.getByLabel(`all ${parking} matching the filters`).check();
  await expect(dialog).not.toContainText(NONE_SELECTED);
  await preview.click();
  await expect(dialog).toContainText(`${parking} to change`);
  await expect(dialog).toContainText("tags: car / parking → food / dining out");
  await page.keyboard.press("Escape");
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);
  expect(await apiGet(page, "/status")).toEqual(before);
});

for (const path of ["annotate", "transactions"]) {
  test(`${path}: Escape keeps the selection, Ctrl+Backspace and the clear button clear it`, async ({ page }) => {
    await openPage(page, path);
    const rows = page.locator("tbody[data-id]");
    await expect(rows.first()).toBeVisible();
    await page.keyboard.press("x");
    await page.keyboard.press("j");
    await page.keyboard.press("x");
    await expect(page.locator("tbody.selected")).toHaveCount(2);

    // Escape: out of a cell, a dialog, the search, and once more on its own
    await page.keyboard.press("Enter");
    await page.keyboard.press("Escape");
    await page.keyboard.press("?");
    await page.keyboard.press("Escape");
    await page.keyboard.press("/");
    await page.keyboard.press("Escape");
    await page.keyboard.press("Escape");
    await page.keyboard.press("Escape");
    await expect(page.locator("tbody.selected")).toHaveCount(2);
    await expect(summary(page)).toContainText("2 selected");

    // The page takes the chord from the browser (run in Firefox too: FINALYZE_E2E_BROWSER=firefox)
    await page.evaluate(() => {
      window.addEventListener("keydown", (event) => {
        if (event.key === "Backspace") document.body.dataset.chordPrevented = String(event.defaultPrevented);
      });
    });
    await page.keyboard.press("Control+Backspace");
    await expect(page.locator("tbody.selected")).toHaveCount(0);
    await expect(summary(page)).not.toContainText("selected");
    await expect(page.locator("body")).toHaveAttribute("data-chord-prevented", "true");
    await expect(page).toHaveURL(new RegExp(`#/${path}$`));

    await page.keyboard.press("Control+a");
    const count = await rows.count();
    await expect(summary(page)).toContainText(`${count} selected`);
    await summary(page).getByRole("button", { name: "clear" }).click();
    await expect(page.locator("tbody.selected")).toHaveCount(0);
  });
}

test("Orphans: Escape keeps the selection, Ctrl+Backspace clears it", async ({ page }) => {
  await openPage(page, "orphans");
  const checkbox = page.locator("table.grid").first().locator("tbody input[type=checkbox]");
  await expect(checkbox).toHaveCount(1);
  await page.keyboard.press("x");
  await expect(checkbox).toBeChecked();
  await page.keyboard.press("Escape");
  await expect(checkbox).toBeChecked();
  await expect(summary(page)).toContainText("1 selected");
  // Kept across pages
  await goTo(page, "a");
  await expect(page).toHaveURL(/#\/annotate$/);
  await goTo(page, "o");
  await expect(checkbox).toBeChecked();
  await expect(summary(page)).toContainText("1 selected");
  await page.keyboard.press("Control+Backspace");
  await expect(checkbox).not.toBeChecked();
  await page.keyboard.press("Control+a");
  await expect(checkbox).toBeChecked();
  await summary(page).getByRole("button", { name: "clear" }).click();
  await expect(checkbox).not.toBeChecked();
});

test("the help lists select all and clear the selection", async ({ page }) => {
  for (const path of ["annotate", "transactions", "orphans"]) {
    await openPage(page, path);
    await page.keyboard.press("?");
    const help = page.getByRole("dialog", { name: "Keyboard shortcuts" });
    await expect(help).toContainText(/Select all/);
    await expect(help).toContainText("Clear the selection");
    await expect(help.locator("dt", { hasText: "Backspace" })).toContainText("Ctrl");
    await page.keyboard.press("Escape");
  }
});

const selectedIds = (page: Page) =>
  page.locator("tbody.selected").evaluateAll((rows) => rows.map((row) => row.getAttribute("data-id")));

/** Select the cursor row and the next one. */
async function selectTwo(page: Page) {
  await expect(page.locator("tbody[data-id]").first()).toBeVisible();
  await page.keyboard.press("x");
  await page.keyboard.press("j");
  await page.keyboard.press("x");
  await expect(page.locator("tbody.selected")).toHaveCount(2);
}

test("Annotate: each queue keeps its own selection, also across pages", async ({ page }) => {
  await openPage(page, "annotate");
  await selectTwo(page);
  const untagged = await selectedIds(page);

  // Another queue starts with its own (empty) selection
  await page.keyboard.press("m");
  await expect(page).toHaveURL(/#\/annotate\?review$/);
  await expect(page.locator("tbody[data-id]").first()).toBeVisible();
  await expect(page.locator("tbody.selected")).toHaveCount(0);
  await expect(summary(page)).not.toContainText("selected");
  await page.keyboard.press("x");
  await expect(summary(page)).toContainText("1 selected");
  const review = await selectedIds(page);

  // Back: the untagged queue's selection is as it was left
  await page.keyboard.press("m");
  await page.keyboard.press("m");
  await expect(page).toHaveURL(/#\/annotate$/);
  await expect(summary(page)).toContainText("2 selected");
  expect(await selectedIds(page)).toEqual(untagged);

  // Another page and back, to both queues
  await goTo(page, "t");
  await expect(page).toHaveURL(/#\/transactions$/);
  await expect(page.locator("tbody[data-id]").first()).toBeVisible();
  await expect(page.locator("tbody.selected")).toHaveCount(0);
  await goTo(page, "a");
  await expect(summary(page)).toContainText("2 selected");
  expect(await selectedIds(page)).toEqual(untagged);
  await page.getByRole("tab", { name: /^Needs review/ }).click();
  await expect(summary(page)).toContainText("1 selected");
  expect(await selectedIds(page)).toEqual(review);

  // Batch edit acts on this queue's selection only
  await page.keyboard.press("b");
  await expect(page.getByRole("dialog", { name: "Batch edit" })).toContainText("Applies to 1 selected row");
});

test("Transactions: the selection and the filters are kept across pages", async ({ page }) => {
  await openPage(page, "transactions");
  await page.keyboard.press("/");
  await page.keyboard.type("PARKO");
  await expect(summary(page)).toContainText(`of ${await parkingCount(page)}`);
  await page.keyboard.press("Escape");
  await selectTwo(page);
  const chosen = await selectedIds(page);

  await goTo(page, "a");
  await expect(page.locator("tbody[data-id]").first()).toBeVisible();
  await expect(page.locator("tbody.selected")).toHaveCount(0);
  await goTo(page, "t");
  await expect(page.getByPlaceholder(/search/i)).toHaveValue("PARKO");
  await expect(summary(page)).toContainText(`of ${await parkingCount(page)}`);
  await expect(summary(page)).toContainText("2 selected");
  expect(await selectedIds(page)).toEqual(chosen);
});

test("Transactions: narrowing the filters drops only the selected rows that leave the view", async ({ page }) => {
  await openPage(page, "transactions");
  await expect(page.locator("tbody[data-id]")).toHaveCount(200);
  await page.keyboard.press("Control+a");
  await expect(summary(page)).toContainText("200 selected");

  await page.keyboard.press("/");
  await page.keyboard.type("PARKO");
  await expect(summary(page)).toContainText(`of ${await parkingCount(page)}`);
  await page.keyboard.press("Escape");
  // The selected rows still shown stay selected, and the count is of those
  const kept = await page.locator("tbody.selected").count();
  expect(kept).toBeGreaterThan(0);
  expect(kept).toBeLessThan(await parkingCount(page));
  await expect(summary(page)).toContainText(`${kept} selected`);
  await page.keyboard.press("b");
  const dialog = page.getByRole("dialog", { name: "Batch apply" });
  await expect(dialog.getByLabel(`the ${kept} selected`)).toBeChecked();
  await page.keyboard.press("Escape");
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);

  // Widening again doesn't bring back the dropped ones
  await page.getByRole("button", { name: /^Clear/ }).click();
  await expect(page.locator("tbody[data-id]")).toHaveCount(200);
  await expect(summary(page)).toContainText(`${kept} selected`);
  await expect(page.locator("tbody.selected")).toHaveCount(kept);
});

test("Annotate: rows that leave the queue while away leave the selection", async ({ page }) => {
  await openPage(page, "annotate");
  await selectTwo(page);
  const [gone, stays] = await selectedIds(page);

  await goTo(page, "t");
  await expect(page.locator("tbody[data-id]").first()).toBeVisible();
  await apiPost(page, "/annotations/bulk", { query: { ids: [gone] }, tag: "food", subtag: "groceries", dry_run: false });
  await goTo(page, "a");
  await expect(summary(page)).toContainText("1 selected");
  expect(await selectedIds(page)).toEqual([stays]);
  await page.keyboard.press("b");
  await expect(page.getByRole("dialog", { name: "Batch edit" })).toContainText("Applies to 1 selected row");
});

test("Annotate: submitting clears the selection", async ({ page }) => {
  await openPage(page, "annotate");
  await selectTwo(page);
  await page.keyboard.press("b");
  const dialog = page.getByRole("dialog", { name: "Batch edit" });
  await page.keyboard.type("groceries");
  await page.keyboard.press("Enter");
  await dialog.getByRole("button", { name: /^Apply/ }).click();
  await expect(page.locator("tbody.dirty")).toHaveCount(2);
  await page.keyboard.press("Control+Enter");
  await expect(page.getByText("Saved 2 annotations")).toBeVisible();
  await expect(page.locator("tbody[data-id]")).toHaveCount(10);
  await expect(page.locator("tbody.selected")).toHaveCount(0);
  await expect(summary(page)).not.toContainText("selected");
  // Also when coming back (each queue shown before switching again)
  for (const queue of ["Needs review", "Custom", "Untagged"]) {
    await page.keyboard.press("m");
    await expect(page.getByRole("tab", { selected: true })).toContainText(queue);
  }
  await expect(page.locator("tbody[data-id]")).toHaveCount(10);
  await expect(page.locator("tbody.selected")).toHaveCount(0);
});
