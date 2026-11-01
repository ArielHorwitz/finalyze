import { execFileSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, type Page } from "@playwright/test";

const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
export const DATA_DIR = process.env.FINALYZE_E2E_DIR ?? "/tmp/finalyze-web-e2e";

export const END_DATE = process.env.FINALYZE_E2E_END_DATE ?? "2025-06-15";

/** Recreate the demo under the running server, so each test starts fresh. */
export function resetDemoData() {
  execFileSync("uv", ["run", "python", "-m", "finalyze.demo", DATA_DIR, "--end-date", END_DATE], {
    cwd: REPO,
    stdio: "ignore",
  });
}

/** GET an API path from the test's server. */
export async function apiGet(page: Page, path: string) {
  const response = await page.request.get(`/api${path}`);
  expect(response.ok()).toBe(true);
  return response.json();
}

/** Wait for the app to load the page's data. */
export async function openPage(page: Page, path = "") {
  await page.goto(`/#/${path}`);
  await expect(page.locator("header .counts")).toBeVisible();
}

/** POST JSON to an API path of the test's server. */
export async function apiPost(page: Page, path: string, body: unknown) {
  const response = await page.request.post(`/api${path}`, { data: body });
  expect(response.ok()).toBe(true);
  return response.json();
}

/** Press "g" then a page's letter, e.g. "t" for Transactions. */
export async function goTo(page: Page, letter: string) {
  await page.keyboard.press("g");
  await page.keyboard.press(letter);
}
