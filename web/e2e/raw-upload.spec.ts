import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "@playwright/test";
import { DATA_DIR, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

const HEADER = "source,date,amount,description\n";

test("upload raw files from the Ingest page: checked, replaced only when confirmed, then previewed", async ({ page }) => {
  const temp = mkdtempSync(join(tmpdir(), "finalyze-e2e-upload-"));
  try {
    const bakery = join(temp, "bakery.csv");
    writeFileSync(bakery, `${HEADER}wallet,2025-06-14,-3.20,Invented bakery\n`);
    const notes = join(temp, "notes.txt");
    writeFileSync(notes, "not a bank export");
    await openPage(page, "ingest");
    // The raw folder's path, to copy
    await expect(page.getByLabel("Raw folder")).toHaveValue(`${DATA_DIR}/raw`);

    await page.keyboard.press("u");
    const dialog = page.getByRole("dialog", { name: "Upload raw files" });
    await dialog.getByLabel("Raw files").setInputFiles([bakery, notes]);
    await dialog.getByLabel("Account").selectOption("cash");
    await page.keyboard.press("Enter");
    const checked = dialog.getByTestId("upload-check");
    await expect(checked.getByRole("row", { name: /bakery\.csv/ })).toContainText("csv");
    await expect(checked.getByRole("row", { name: /bakery\.csv/ })).toContainText("new");
    await expect(checked.getByRole("row", { name: /notes\.txt/ })).toContainText("No parser could read the file");
    await expect(dialog).toContainText("1 file no parser reads is left out");
    await page.keyboard.press("Enter");
    await expect(dialog).toBeHidden();
    await expect(page.getByText("Uploaded 1 file to cash")).toBeVisible();
    expect(existsSync(`${DATA_DIR}/raw/cash/bakery.csv`)).toBe(true);
    expect(existsSync(`${DATA_DIR}/raw/cash/notes.txt`)).toBe(false);
    // The usual preview, before ingesting
    const preview = page.getByTestId("ingest-preview");
    await expect(preview).toContainText("1 new, 0 vanished");

    // The same name again: replaced only once confirmed, comparing both files
    writeFileSync(bakery, `${HEADER}wallet,2025-06-13,-2.10,Invented bakery\nwallet,2025-06-14,-3.20,Invented bakery\n`);
    await page.getByRole("button", { name: /^Upload files/ }).click();
    await dialog.getByLabel("Raw files").setInputFiles(bakery);
    await dialog.getByLabel("Account").selectOption("cash");
    await dialog.getByRole("button", { name: /^Check/ }).click();
    await expect(checked.getByRole("row", { name: /bakery\.csv/ })).toContainText("replaces");
    await expect(checked).toContainText("replaces the file there now: 14/06/2025 to 14/06/2025, 1 row (wallet)");
    await dialog.getByRole("button", { name: /^Upload 1 file/ }).click();
    const replace = page.getByRole("dialog", { name: "Replace files?" });
    await expect(replace).toContainText("Now: 14/06/2025 to 14/06/2025, 1 row (wallet)");
    await expect(replace).toContainText("New: 13/06/2025 to 14/06/2025, 2 rows (wallet)");
    await page.keyboard.press("Escape");
    await expect(replace).toBeHidden();
    expect(readFileSync(`${DATA_DIR}/raw/cash/bakery.csv`, "utf-8")).not.toContain("2025-06-13");
    await dialog.getByRole("button", { name: /^Upload 1 file/ }).click();
    await page.keyboard.press("Enter");
    await expect(dialog).toBeHidden();
    expect(readFileSync(`${DATA_DIR}/raw/cash/bakery.csv`, "utf-8")).toContain("2025-06-13");
    await expect(preview).toContainText("2 new, 0 vanished");

    // Into a new account, whose folder is made
    await page.keyboard.press("u");
    await dialog.getByLabel("Raw files").setInputFiles(bakery);
    await dialog.getByLabel("Account").selectOption({ label: "New account…" });
    await dialog.getByLabel("New account").fill(".pot");
    await expect(dialog.getByRole("button", { name: /^Check/ })).toBeDisabled();
    await dialog.getByLabel("New account").fill("pot");
    await page.keyboard.press("Enter");
    await expect(dialog).toContainText("A new account: its folder is made.");
    await page.keyboard.press("Enter");
    await expect(dialog).toBeHidden();
    await expect(page.locator("table.files")).toContainText(`${DATA_DIR}/raw/pot/`);
    await expect(preview.locator("table.counts")).toContainText("pot");
  } finally {
    rmSync(temp, { recursive: true, force: true });
  }
});
