// Batch edit and batch apply with optional fields: e.g. only the kind.
import { expect, test, type Page } from "@playwright/test";
import { apiGet, apiPost, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

// With SCREENSHOT_DIR set, the dialogs are also captured
const DIR = process.env.SCREENSHOT_DIR;
const shot = async (page: Page, name: string) => {
  if (DIR) await page.screenshot({ path: `${DIR}/${name}.png` });
};

interface Ids {
  tagged: string;
  split: string;
  untagged: string;
  ruled: string;
}

/** A tagged, a split and an untagged transaction of the untagged ones (only the
 * first two flagged for review), and one tagged by a rule. */
async function setUp(page: Page): Promise<Ids> {
  await page.goto("/");
  // Only these are flagged
  const flagged = (await apiGet(page, "/transactions?needs_review=true&limit=100")).items;
  await apiPost(page, "/annotations/review", { ids: flagged.map((item: { id: string }) => item.id), needs_review: false });
  // (without the one only described)
  const grocer = (await apiGet(page, "/transactions?status=untagged&limit=20")).items.filter(
    (item: { annotation: unknown }) => item.annotation === null,
  );
  const [tagged, split, untagged] = grocer.map((item: { id: string }) => item.id);
  await apiPost(page, "/annotations/bulk", { query: { ids: [tagged] }, tag: "food", subtag: "groceries", description: "weekly", needs_review: true, dry_run: false });
  const amount = grocer[1].amount;
  const response = await page.request.put(`/api/transactions/${split}/annotation`, {
    data: {
      needs_review: true,
      parts: [
        { tag: "food", subtag: "groceries", amount: -10 },
        { tag: "gifts", subtag: "birthdays", amount: Math.round((amount + 10) * 100) / 100 },
      ],
    },
  });
  expect(response.ok()).toBe(true);
  const ruled = (await apiGet(page, "/transactions?status=rule&limit=1")).items[0].id;
  return { tagged, split, untagged, ruled };
}

test("batch apply sets only the kind, skipping untagged and rules' rows", async ({ page }) => {
  const ids = await setUp(page);
  await openPage(page, `transactions?ids=${Object.values(ids).join(",")}`);
  await expect(page.locator("tbody[data-id]")).toHaveCount(4);
  await page.keyboard.press("Control+a");
  await page.keyboard.press("b");
  const dialog = page.getByRole("dialog", { name: "Batch apply" });
  const preview = dialog.getByRole("button", { name: /^Preview/ });
  // Nothing checked, nothing to apply. A checked tag needs a tag
  await expect(preview).toBeDisabled();
  await dialog.getByRole("checkbox", { name: "Include the tags" }).check();
  await expect(dialog).toContainText("a tag is needed");
  await expect(preview).toBeDisabled();
  await dialog.getByRole("checkbox", { name: "Include the tags" }).uncheck();
  // Choosing a kind checks it
  await dialog.getByRole("combobox", { name: "Kind" }).selectOption("one-off");
  await expect(dialog.getByRole("checkbox", { name: "Include the kind" })).toBeChecked();
  await preview.click();
  await expect(dialog).toContainText("2 to change, 2 skipped");
  const rows = dialog.locator("tbody tr");
  await expect(rows.filter({ hasText: "kind: the tag's → one-off on all 2 parts" })).toHaveCount(1);
  await expect(rows.filter({ hasText: /^(?!.*parts).*kind: the tag's → one-off/ })).toHaveCount(1);
  await expect(rows.filter({ hasText: "skipped: untagged: a kind needs a tag" })).toHaveCount(1);
  await expect(rows.filter({ hasText: "skipped: tagged by a rule" })).toHaveCount(1);
  await shot(page, "2-transactions-batch-apply-kind-only");
  await dialog.getByRole("button", { name: /^Commit 2/ }).click();
  await expect(page.getByText("Changed 2 transactions")).toBeVisible();

  // Only the kind changed: tags, descriptions and review flags are kept
  const tagged = await apiGet(page, `/transactions/${ids.tagged}`);
  expect(tagged.annotation).toMatchObject({ description: "weekly", needs_review: true, parts: [{ subtag: "groceries", kind: "one-off", kind_set: true }] });
  const split = await apiGet(page, `/transactions/${ids.split}`);
  expect(split.annotation.parts.map((part: { kind_set: boolean }) => part.kind_set)).toEqual([true, true]);
  expect((await apiGet(page, `/transactions/${ids.untagged}`)).annotation).toBeNull();
  expect((await apiGet(page, `/transactions/${ids.ruled}`)).annotation).toBeNull();
});

test("batch apply sets only a description and the review flag", async ({ page }) => {
  const ids = await setUp(page);
  await openPage(page, `transactions?ids=${ids.tagged},${ids.untagged}`);
  await expect(page.locator("tbody[data-id]")).toHaveCount(2);
  await page.keyboard.press("Control+a");
  await page.keyboard.press("b");
  const dialog = page.getByRole("dialog", { name: "Batch apply" });
  await dialog.getByRole("textbox", { name: "Description" }).fill("ask Bob");
  await dialog.getByRole("checkbox", { name: "Include the review flag" }).check();
  await dialog.getByRole("button", { name: /^Preview/ }).click();
  await expect(dialog).toContainText("2 to change");
  await expect(dialog.locator("tbody tr").filter({ hasText: "description: “ask Bob”" })).toHaveCount(2);
  await page.keyboard.press("Control+Enter");
  await expect(page.getByText("Changed 2 transactions")).toBeVisible();
  const untagged = await apiGet(page, `/transactions/${ids.untagged}`);
  expect(untagged).toMatchObject({ status: "untagged", annotation: { description: "ask Bob", needs_review: true, parts: [] } });
  const tagged = await apiGet(page, `/transactions/${ids.tagged}`);
  expect(tagged.annotation).toMatchObject({ description: "ask Bob", parts: [{ subtag: "groceries" }] });
});

test("batch edit sets only the kind on the drafts, on every part of a split", async ({ page }) => {
  await setUp(page);
  await openPage(page, "annotate?review");
  const rows = page.locator("tbody[data-id]");
  await expect(rows).toHaveCount(2);
  await page.keyboard.press("Control+a");
  await page.keyboard.press("b");
  const dialog = page.getByRole("dialog", { name: "Batch edit" });
  const apply = dialog.getByRole("button", { name: /^Apply/ });
  await expect(apply).toBeDisabled();
  await dialog.getByRole("combobox", { name: "Kind" }).focus();
  await dialog.getByRole("combobox", { name: "Kind" }).selectOption("one-off");
  // The preview is live
  await expect(dialog).toContainText("2 to change");
  await expect(dialog.locator("tbody tr").filter({ hasText: "on all 2 parts" })).toHaveCount(1);
  await shot(page, "1-annotate-batch-edit-kind-only");
  await apply.click();
  await expect(dialog).toHaveCount(0);
  await expect(page.locator("tbody.dirty")).toHaveCount(2);
  // The review flag wasn't checked, so the rows stay flagged
  await page.keyboard.press("Control+Enter");
  await expect(page.getByText("Saved 2 annotations")).toBeVisible();
  const flagged = await apiGet(page, "/transactions?needs_review=true&kind_set=true");
  expect(flagged.total).toBe(2);
});
