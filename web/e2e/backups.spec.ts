// Backups on the Settings page: made, listed, restored (backed up first, or
// not), downloaded and deleted, and warned about ahead of the limit.
// With BACKUPS_SCREENSHOT_DIR set, screenshots of them are saved there too.
import { mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test, type Page } from "@playwright/test";
import { apiGet, apiPost, DATA_DIR, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

const SHOTS = process.env.BACKUPS_SCREENSHOT_DIR;

async function shot(page: Page, name: string) {
  if (SHOTS) await page.screenshot({ path: join(SHOTS, `${name}.png`) });
}

async function clearAnnotations(page: Page, count: number) {
  const annotated = await apiGet(page, `/transactions?status=annotated&limit=${count}`);
  await apiPost(page, "/annotations/clear", { ids: annotated.items.map((item: { id: string }) => item.id) });
}

test("back up, then restore: the data is the backup's, after backing up the current one", async ({ page }) => {
  const original = (await apiGet(page, "/settings/config")).config;
  try {
    await openPage(page, "settings");
    const section = page.getByRole("region", { name: "Backups" });
    await section.scrollIntoViewIfNeeded();
    const name = section.getByLabel("New backup name");
    const create = section.getByRole("button", { name: "Back up now" });
    await expect(create).toBeDisabled();
    await name.fill("Before cleaning tags");
    await name.press("Enter");
    await expect(page.getByText("Backed up as “Before cleaning tags”")).toBeVisible();
    await expect(section.getByRole("row", { name: /Before cleaning tags/ })).toBeVisible();
    // Names are unique, whatever the case
    await name.fill("before cleaning TAGS");
    await expect(section.getByText("A backup named “before cleaning TAGS” already exists.")).toBeVisible();
    await expect(create).toBeDisabled();
    await name.fill("");

    // Changed after it
    const untagged = (await apiGet(page, "/status")).untagged;
    await clearAnnotations(page, 4);
    const counts = page.locator("header .counts");
    await page.reload();
    await expect(counts).toContainText(`${untagged + 4} untagged`);

    await section.getByRole("row", { name: /Before cleaning tags/ }).getByRole("button", { name: "restore" }).click();
    const dialog = page.getByRole("dialog", { name: "Restore “Before cleaning tags”?" });
    await expect(dialog).toContainText("with the backup “Before cleaning tags”, made");
    // Without a backup, it says what will be gone
    const backupFirst = dialog.getByLabel("Back up the current data first");
    await expect(backupFirst).toBeChecked();
    await backupFirst.uncheck();
    await expect(dialog.getByLabel("Backup name")).toBeDisabled();
    await expect(dialog).toContainText("Without a backup, the current data will be gone for good.");
    await shot(page, "restore-without-backup");
    await backupFirst.check();
    await dialog.getByLabel("Backup name").fill("before restoring");
    await dialog.getByRole("button", { name: "Restore" }).click();
    await expect(page.getByText("Restored “Before cleaning tags”. The previous data is backed up as “before restoring”")).toBeVisible();
    await expect(counts).toContainText(`${untagged} untagged`);
    await expect(section.locator("tbody tr")).toHaveCount(2);
    await expect(section.locator("tbody tr").first()).toContainText("before restoring");
    await shot(page, "backups-list");

    // Deleted after a confirmation
    await section.getByRole("row", { name: /Before cleaning tags/ }).getByRole("button", { name: "delete" }).click();
    const confirmation = page.getByRole("dialog", { name: "Delete a backup?" });
    await expect(confirmation).toContainText("It may be the only copy");
    await page.keyboard.press("Enter");
    await expect(section.locator("tbody tr")).toHaveCount(1);
    expect(readdirSync(join(DATA_DIR, "data", "backups"))).toEqual(["before restoring.zip"]);
  } finally {
    await page.request.put("/api/settings/config", { data: original });
  }
});

test("a warning ahead of the limit, on the list and by the backup before an import, never in the header", async ({ page }) => {
  const original = (await apiGet(page, "/settings/config")).config;
  const temp = mkdtempSync(join(tmpdir(), "finalyze-e2e-backups-"));
  try {
    for (let number = 1; number <= 8; number++) await apiPost(page, "/backups", { name: `backup ${number}` });
    await openPage(page, "settings");
    const section = page.getByRole("region", { name: "Backups" });
    await expect(section.locator("tbody tr")).toHaveCount(8);
    await expect(section.getByRole("status")).toHaveCount(0);

    // One place left: a gentle hint
    await apiPost(page, "/backups", { name: "backup 9" });
    await page.reload();
    await expect(section.getByRole("status")).toHaveText(/^9 of 10 backups: room for one more/);
    const zip = join(temp, "export.zip");
    writeFileSync(zip, await (await page.request.get("/api/export")).body());
    const importSection = page.getByRole("region", { name: "Export and import" });
    await importSection.getByLabel("Zip to import").setInputFiles(zip);
    await importSection.getByRole("button", { name: "Import…" }).click();
    const dialog = page.getByRole("dialog", { name: "Replace all the data?" });
    await expect(dialog.getByRole("status")).toHaveText("This backup takes the last free place of 10.");
    await page.keyboard.press("Escape");

    // At the limit: a warning on the list, and by the checkbox, which blocks the import
    await apiPost(page, "/backups", { name: "backup 10" });
    await page.reload();
    await expect(section.getByRole("status")).toHaveText(/^10 of 10 backups, the most kept: new ones, including the one before an import or a restore, are refused/);
    await expect(section.getByRole("button", { name: "Back up now" })).toBeDisabled();
    await expect(page.locator("header")).not.toContainText(/backup/i);
    await section.scrollIntoViewIfNeeded();
    await shot(page, "backups-at-the-limit");
    await importSection.getByLabel("Zip to import").setInputFiles(zip);
    await importSection.getByRole("button", { name: "Import…" }).click();
    await expect(dialog.getByRole("status")).toContainText("10 of 10 backups");
    const importButton = dialog.getByRole("button", { name: "Import" });
    await expect(importButton).toBeDisabled();
    await shot(page, "import-at-the-limit");
    // Unchecked, it can go ahead
    await dialog.getByLabel("Back up the current data first").uncheck();
    await expect(importButton).toBeEnabled();
    await dialog.getByLabel("Back up the current data first").check();
    // The link leads to the list, to delete one
    await dialog.getByRole("button", { name: "Go to the backups list" }).click();
    await expect(dialog).toBeHidden();
    await expect(section).toBeInViewport();
    await section.getByRole("row", { name: /backup 1\b/ }).getByRole("button", { name: "delete" }).click();
    await page.keyboard.press("Enter");
    await expect(section.locator("tbody tr")).toHaveCount(9);
    await expect(section.getByRole("status")).toHaveText(/^9 of 10 backups/);
    // Raw files the export doesn't have are listed, and those no backup holds marked
    mkdirSync(join(DATA_DIR, "raw", "pot"));
    writeFileSync(join(DATA_DIR, "raw", "pot", "pot.csv"), "source,date,amount,description\n");
    writeFileSync(join(DATA_DIR, "raw", "notes.txt"), "mine");
    await importSection.getByRole("button", { name: "Import…" }).click();
    await expect(importButton).toBeEnabled();
    await expect(dialog.getByRole("list", { name: "Raw files removed" }).getByRole("listitem")).toHaveText([/^notes\.txt\s*in no backup$/, "pot/pot.csv"]);
    await expect(dialog).toContainText("1 file marked “in no backup” is a file finalyze ignores");
    await shot(page, "import-dialog");
    await importButton.click();
    await expect(page.getByText(/^Imported\. The previous data is backed up as “before import on/)).toBeVisible();
    await expect(section.getByRole("status")).toHaveText(/^10 of 10 backups/);
  } finally {
    await page.request.put("/api/settings/config", { data: original });
    rmSync(temp, { recursive: true, force: true });
  }
});
