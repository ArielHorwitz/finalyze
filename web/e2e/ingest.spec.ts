import { readFileSync, writeFileSync, unlinkSync } from "node:fs";
import { expect, test } from "@playwright/test";
import { apiGet, DATA_DIR, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

const MAYA = `${DATA_DIR}/raw/maya`;
const WALLET = `${DATA_DIR}/raw/cash/wallet.csv`;

test("the ingest page checks the raw files, previews, then ingests", async ({ page }) => {
  // A year exported twice, and a file no parser reads
  writeFileSync(`${MAYA}/leumi-checking-2024-again.xls`, readFileSync(`${MAYA}/leumi-checking-2024.xls`));
  writeFileSync(`${MAYA}/notes.txt`, "not a bank export");
  await openPage(page, "ingest");
  const problems = page.getByTestId("ingest-problems");
  await expect(problems).toContainText("unparseable");
  await expect(problems).toContainText("duplicates");
  await expect(problems).toContainText("leumi-checking-2024-again.xls and leumi-checking-2024.xls");
  const preview = page.getByTestId("ingest-preview");
  await expect(preview).toContainText("Ingest would fail");
  await expect(page.getByRole("button", { name: /^Ingest/ })).toBeDisabled();
  const filesTable = page.locator("table.files");
  await expect(filesTable.getByRole("row", { name: /notes\.txt/ })).toContainText("No parser could read the file");
  // (Maya's, the first account)
  await expect(filesTable.getByRole("row", { name: /leumi-checking-2025\.xls/ }).first()).toContainText("leumi checking");
  await expect(filesTable.getByRole("row", { name: /bit-2025\.csv/ })).toContainText("bit");

  // Without the unparseable file, the duplicates would be new transactions
  unlinkSync(`${MAYA}/notes.txt`);
  unlinkSync(`${MAYA}/leumi-checking-2024-again.xls`);
  // And without the wallet's babysitter rows, their annotations would be orphaned
  const lines = readFileSync(WALLET, "utf-8").split("\n");
  const babysitter = lines.filter((line) => line.includes("Babysitter")).length;
  writeFileSync(WALLET, lines.filter((line) => !line.includes("Babysitter")).join("\n"));
  await page.keyboard.press("R");
  await expect(problems).toHaveCount(0);
  await expect(preview).toContainText(`0 new, ${babysitter} vanished`);
  await expect(preview).toContainText(`Annotations that would be orphaned (${babysitter})`);
  await expect(preview.locator("table.orphans")).toContainText("kids / babysitting");
  const before = await apiGet(page, "/status");

  await page.keyboard.press("Control+Enter");
  const confirmDialog = page.getByRole("dialog", { name: "Ingest" });
  await expect(confirmDialog).toContainText(`${babysitter} annotations would be orphaned`);
  await page.keyboard.press("Enter");
  const result = page.getByTestId("ingest-result");
  await expect(result).toContainText(`0 new, ${babysitter} vanished`);
  await expect(result).toContainText(`${babysitter} new orphans`);
  await expect(preview).toContainText("Ingesting would change nothing");
  const after = await apiGet(page, "/status");
  expect(after.orphans).toBe(before.orphans + babysitter);
  expect(after.transactions).toBe(before.transactions - babysitter);
});

test("the header's ingested hint only moves when an ingest changes the data", async ({ page }) => {
  // As if the data last changed, and was last checked, 3 hours ago
  const threeHoursAgo = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString();
  writeFileSync(`${DATA_DIR}/data/ingested/ingest-times.json`, JSON.stringify({ changed_at: threeHoursAgo, checked_at: threeHoursAgo }));
  await openPage(page, "ingest");
  const hint = page.getByTestId("freshness");
  await expect(hint).toHaveText("ingested 3 h ago");
  await expect(hint).toHaveAttribute("title", /Last checked .*, 3 h ago\./);
  const preview = page.getByTestId("ingest-preview");
  await expect(preview).toContainText("Ingesting would change nothing");

  // An ingest that changes nothing is only a check
  await page.keyboard.press("Control+Enter");
  const times = page.getByTestId("ingest-times");
  await expect(times).toContainText("Nothing changed: the ingested data is as it was, last changed");
  await expect(times).toContainText("(3 h ago). Checked");
  await expect(hint).toHaveText("ingested 3 h ago");
  await expect(hint).toHaveAttribute("title", /Last checked .*, just now\./);

  // One that changes something moves it (a hand-written file may keep the
  // old header, description, for the raw description)
  writeFileSync(`${DATA_DIR}/raw/cash/wallet-extra.csv`, "source,date,amount,description\nwallet,2025-06-14,-3.20,Invented bakery\n");
  await page.keyboard.press("R");
  await expect(preview).toContainText("1 new, 0 vanished");
  await page.getByText("New transactions (1)").click();
  const newRows = page.locator("details[open] table");
  await expect(newRows.getByRole("columnheader", { name: "Raw description" })).toBeVisible();
  await expect(newRows.getByRole("cell", { name: "Invented bakery" })).toBeVisible();
  await page.keyboard.press("Control+Enter");
  await expect(times).toContainText("The ingested data changed");
  await expect(hint).toHaveText("ingested just now");
});
