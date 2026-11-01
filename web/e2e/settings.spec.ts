import { expect, test } from "@playwright/test";
import { apiGet, DATA_DIR, goTo, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

test("settings by keyboard: dashboard defaults, a label color and the config file", async ({ page }) => {
  // The server keeps its config in memory across tests: restore it after
  const original = (await (await page.request.get("/api/settings/config")).json()).config;
  try {
    await openPage(page);
    await page.keyboard.press("g");
    await page.keyboard.press("s");
    await expect(page.getByRole("heading", { name: "Settings" })).toBeVisible();

    // Dashboard defaults, saved as they change
    // (the demo nets by tag)
    await expect(page.getByLabel(/Net incomes and expenses by tag/)).toBeChecked();
    await page.keyboard.press("n");
    await page.keyboard.press("+");
    await page.keyboard.press("t");
    await page.keyboard.press("w");
    await expect
      .poll(() => apiGet(page, "/settings/dashboard"))
      .toEqual({ net_by_tag: false, breakdown_months: 4, complete_months_only: false, rolling_months: 12 });

    // A label color: the first label is the net worth line
    await page.keyboard.press("Enter");
    const dialog = page.getByRole("dialog", { name: "Color of net worth" });
    await dialog.locator("input").first().fill("#123456");
    await page.keyboard.press("Enter");
    await expect(dialog).toBeHidden();
    expect((await apiGet(page, "/settings/colors")).colors["net worth"]).toBe("#123456");
    await page.keyboard.press("d");
    await expect.poll(async () => (await apiGet(page, "/settings/colors")).colors["net worth"]).toBeUndefined();

    // Where the data is: the host file's, read-only
    const host = page.getByRole("region", { name: "Where the data is" });
    await expect(host).toContainText(`${DATA_DIR}/host.toml`);
    await expect(host).toContainText(`${DATA_DIR}/data`);
    await expect(host).toContainText(`${DATA_DIR}/raw`);
    await expect(host.locator("input")).toHaveCount(0);

    // The config file: an invalid date can't be saved
    const section = page.getByRole("region", { name: "Config file" });
    await section.getByLabel("From").fill("April");
    await expect(section.locator(".errors")).toContainText("Start date isn't a date (as dd/mm/yyyy)");
    await expect(section.getByRole("button", { name: /^Save/ })).toBeDisabled();
    await section.getByLabel("From").fill("2023-01-01");
    await expect(section.locator(".errors")).toHaveCount(0);

    // Leaving with unsaved changes asks first
    await page.keyboard.press("Escape");
    await goTo(page, "a");
    const leave = page.getByRole("dialog", { name: "Confirm" });
    await expect(leave).toContainText("Discard the changes to the config?");
    await page.keyboard.press("Escape");
    await expect(page.getByRole("heading", { name: "Settings" })).toBeVisible();

    await page.keyboard.press("c");
    await page.keyboard.press("Control+Enter");
    await expect(page.getByText("Saved the config file")).toBeVisible();
    const saved = await apiGet(page, "/settings/config");
    expect(saved.config.ingestion.filters.start_date).toBe("2023-01-01");

    // The dashboard uses the new defaults
    await page.keyboard.press("Escape");
    await goTo(page, "d");
    await expect(page.locator(".cards")).toBeVisible();
    await expect(page.locator(".controls select")).toHaveValue("12");
    await expect(page.getByLabel("Monthly view months")).toHaveValue("4");
    await expect(page.getByLabel("complete months only")).not.toBeChecked();
  } finally {
    const response = await page.request.put("/api/settings/config", { data: original });
    expect(response.ok()).toBe(true);
  }
});
