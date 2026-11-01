// The README's screenshots, of the demo data: written into docs/screenshots.
// Only runs with README_SCREENSHOTS set: README_SCREENSHOTS=1 npm run e2e -- readme
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test, type Page } from "@playwright/test";
import { openPage, resetDemoData } from "./helpers";

const DIR = resolve(dirname(fileURLToPath(import.meta.url)), "../../docs/screenshots");
test.skip(!process.env.README_SCREENSHOTS, "set README_SCREENSHOTS to update the README's screenshots");
test.beforeAll(resetDemoData);

const shot = (page: Page, name: string) => page.screenshot({ path: `${DIR}/${name}.png` });

test("readme screenshots", async ({ page }) => {
  const settle = () => page.waitForTimeout(900); // chart animations
  // The demo's 24 complete months: its last one has the untagged rows, as if
  // just ingested
  await openPage(page, "dashboard?from=2023-06&to=2025-05");
  await expect(page.locator(".cards")).toBeVisible();
  await settle();
  await shot(page, "face");
  await page.keyboard.press("c");
  await settle();
  await shot(page, "cash_flow");
  await page.keyboard.press("k");
  await settle();
  await shot(page, "readme_breakdown");
});
