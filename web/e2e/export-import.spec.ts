import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "@playwright/test";
import { apiGet, apiPost, DATA_DIR, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

/** Today, as the demo's date format shows it in a backup's default name. */
function today(): string {
  const now = new Date();
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${pad(now.getDate())}-${pad(now.getMonth() + 1)}-${now.getFullYear()}`;
}

test("download an export, then upload it back: the data and the raw folder are the export's at once, backed up first", async ({ page }) => {
  // The import rewrites the server's config (kept in memory across tests): restore it after
  const original = (await apiGet(page, "/settings/config")).config;
  const temp = mkdtempSync(join(tmpdir(), "finalyze-e2e-export-"));
  try {
    await openPage(page, "settings");
    const section = page.getByRole("region", { name: "Export and import" });
    const downloading = page.waitForEvent("download");
    await section.getByRole("button", { name: "Download an export" }).click();
    const download = await downloading;
    expect(download.suggestedFilename()).toMatch(/^finalyze-export-.*\.zip$/);
    const zip = join(temp, download.suggestedFilename());
    await download.saveAs(zip);
    const backupsSection = page.getByRole("region", { name: "Backups" });
    await expect(backupsSection.getByText("None yet.")).toBeVisible();

    // Annotations deleted after the export, and a new account's raw file
    const untagged = (await apiGet(page, "/status")).untagged;
    const annotated = await apiGet(page, "/transactions?status=annotated&limit=3");
    await apiPost(page, "/annotations/clear", { ids: annotated.items.map((item: { id: string }) => item.id) });
    mkdirSync(join(DATA_DIR, "raw", "pot"));
    writeFileSync(join(DATA_DIR, "raw", "pot", "pot.csv"), "source,date,amount,description\n");
    await page.reload();
    const counts = page.locator("header .counts");
    await expect(counts).toContainText(`${untagged + 3} untagged`);

    // The dialog says what it replaces, and backs up first by default
    await section.getByLabel("Zip to import").setInputFiles(zip);
    await section.getByRole("button", { name: "Import…" }).click();
    const dialog = page.getByRole("dialog", { name: "Replace all the data?" });
    await expect(dialog).toContainText("Accounts: cash, eitan, loan, maya, savings.");
    await expect(dialog).toContainText(`This machine's raw folder, ${DATA_DIR}/raw, will hold exactly its`);
    await expect(dialog.getByRole("list", { name: "Raw files removed" })).toHaveText("pot/pot.csv");
    await expect(dialog.getByLabel("Back up the current data first")).toBeChecked();
    await expect(dialog.getByLabel("Backup name")).toHaveValue(`before import on ${today()}`);
    await dialog.getByLabel("Backup name").fill("before the e2e import");
    await page.keyboard.press("Enter");
    await expect(page.getByText("Imported. The previous data is backed up as “before the e2e import”")).toBeVisible();
    await expect(dialog).toBeHidden();
    await expect(counts).toContainText(`${untagged} untagged`);
    expect((await apiGet(page, "/status")).untagged).toBe(untagged);
    expect(existsSync(join(DATA_DIR, "raw", "pot"))).toBe(false);

    // The backup made before it is listed, to download
    expect(readdirSync(join(DATA_DIR, "data", "backups"))).toEqual(["before the e2e import.zip"]);
    const row = backupsSection.getByRole("row", { name: /before the e2e import/ });
    await expect(row).toBeVisible();
    const backupDownload = page.waitForEvent("download");
    await row.getByRole("button", { name: "download" }).click();
    expect((await backupDownload).suggestedFilename()).toBe("before the e2e import.zip");
  } finally {
    const response = await page.request.put("/api/settings/config", { data: original });
    expect(response.ok()).toBe(true);
    rmSync(temp, { recursive: true, force: true });
  }
});
