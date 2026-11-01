// A kind set on a transaction, over its tag's: the one-off toggle (key and
// button), the Kind column, split parts, and batch apply.
import { expect, test } from "@playwright/test";
import { apiGet, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

interface Part {
  tag: string;
  subtag: string;
  kind: string;
  kind_set: boolean;
}

async function parts(page: import("@playwright/test").Page, id: string): Promise<Part[]> {
  return (await apiGet(page, `/transactions/${id}`)).annotation?.parts ?? [];
}

test("O sets a row as a one-off, and back to its tag's kind", async ({ page }) => {
  await openPage(page, "annotate");
  const rows = page.locator("tbody[data-id]");
  const row = rows.nth(0);
  const id = (await row.getAttribute("data-id"))!;
  // On a guess: it is accepted with the one-off
  await page.keyboard.press("O");
  await expect(row).toHaveClass(/dirty/);
  await expect(row.locator(".badge.kind-set")).toHaveText("one-off set");
  await expect(row.locator('[data-field="kind"]')).toHaveValue("one-off");
  await expect(row.locator(".one-off-toggle")).toHaveClass(/\bon\b/);
  await page.keyboard.press("Control+Enter");
  // (Polled: a toast of an earlier save may still be shown)
  await expect.poll(async () => (await parts(page, id))[0]).toMatchObject({ kind: "one-off", kind_set: true });
  // Once reloaded, nothing is left unsaved (else leaving asks to discard it)
  await expect(page.locator("tbody.dirty")).toHaveCount(0);

  // In Transactions, the stored kind shows, and O clears it
  await openPage(page, `transactions?ids=${id}`);
  const stored = page.locator(`tbody[data-id="${id}"]`);
  await expect(stored.locator(".badge.kind-set")).toHaveText("one-off set");
  await page.keyboard.press("O");
  await expect(stored.locator(".badge.kind-set")).toHaveCount(0);
  await page.keyboard.press("Control+Enter");
  await expect.poll(async () => (await parts(page, id))[0].kind_set).toBe(false);

  // The ◆ button, and any kind from the Kind column
  await stored.locator(".one-off-toggle").click();
  await expect(stored.locator('[data-field="kind"]')).toHaveValue("one-off");
  await stored.locator('[data-field="kind"]').selectOption("saving");
  await expect(stored.locator(".badge.kind-set")).toHaveText("saving set");
  await page.keyboard.press("Control+Enter");
  await expect.poll(async () => (await parts(page, id))[0]).toMatchObject({ kind: "saving", kind_set: true });
});

test("a part of a split is set as a one-off", async ({ page }) => {
  const [item] = (await apiGet(page, "/transactions?status=untagged&search=SHOPGLOBE&limit=1")).items;
  await page.request.put(`/api/transactions/${item.id}/annotation`, {
    data: {
      parts: [
        { tag: "shopping", subtag: "electronics", amount: -10 },
        { tag: "shopping", subtag: "electronics", amount: Math.round((item.amount + 10) * 100) / 100 },
      ],
      allow_new_tags: true,
    },
  });
  await openPage(page, `transactions?ids=${item.id}`);
  const row = page.locator(`tbody[data-id="${item.id}"]`);
  await row.locator("tr.part").nth(1).locator(".one-off-toggle").click();
  await expect(row.locator('[data-field="part-1-kind"]')).toHaveValue("one-off");
  await expect(row.locator('[data-field="part-0-kind"]')).toHaveValue("");
  await page.keyboard.press("Control+Enter");
  await expect.poll(async () => (await parts(page, item.id)).map((part) => part.kind_set)).toEqual([false, true]);
});

test("batch apply sets a kind, and a rule's row has none", async ({ page }) => {
  const kidzone = (await apiGet(page, "/transactions?search=KIDZONE")).total;
  const setBefore = (await apiGet(page, "/transactions?kind_set=true&limit=100")).total;
  await openPage(page, "transactions");
  await page.keyboard.press("/");
  await page.keyboard.type("KIDZONE");
  await expect(page.locator(".page-header .summary")).toContainText(`of ${kidzone}`);
  await page.keyboard.press("Escape");
  await page.keyboard.press("b");
  const dialog = page.getByRole("dialog", { name: "Batch apply" });
  await page.keyboard.type("groceries");
  await page.keyboard.press("Enter");
  await dialog.getByRole("combobox", { name: "Kind" }).selectOption("one-off");
  await dialog.getByLabel(`all ${kidzone} matching the filters`).check();
  await page.keyboard.press("Control+Enter");
  await expect(dialog).toContainText(`${kidzone} to change`);
  await expect(dialog).toContainText("tags: kids / clothes → food / groceries · kind: the tag's → one-off");
  await page.keyboard.press("Control+Enter");
  await expect(page.getByText(`Changed ${kidzone} transactions`)).toBeVisible();
  const set = await apiGet(page, "/transactions?kind_set=true&limit=100");
  expect(set.total).toBe(setBefore + kidzone);

  // Rules' rows show their tag's kind, without a selector or toggle
  const ruled = (await apiGet(page, "/transactions?status=rule&limit=1")).items[0];
  await openPage(page, `transactions?ids=${ruled.id}`);
  const row = page.locator(`tbody[data-id="${ruled.id}"]`);
  await expect(row.locator("td.kind")).toHaveText(ruled.rule.kind);
  await expect(row.locator(".one-off-toggle")).toHaveCount(0);
  await page.keyboard.press("O");
  await expect(row).not.toHaveClass(/dirty/);
  // Via the API, it is refused
  const response = await page.request.post("/api/annotations/kind", { data: { ids: [ruled.id], kind: "one-off" } });
  expect(response.status()).toBe(422);
});
