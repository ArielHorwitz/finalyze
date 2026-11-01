import { expect, test } from "@playwright/test";
import { apiGet, goTo, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

test("dates follow the date format: tables, typed filters and stamps, with ISO URLs", async ({ page }) => {
  // Day first by default
  await openPage(page, "transactions");
  const dates = page.locator("td.date");
  await expect(dates.first()).toHaveText(/^\d{2}\/\d{2}\/\d{4}$/);

  // Month first, set in Settings
  await goTo(page, "s");
  const format = page.getByRole("region", { name: "Display" }).getByLabel("Date format");
  await expect(format).toHaveValue("dd/mm/yyyy");
  await format.selectOption("mm/dd/yyyy");
  await expect.poll(() => apiGet(page, "/settings/display")).toEqual({ date_format: "mm/dd/yyyy" });

  // A date typed leniently into a filter: read month first, kept ISO in the
  // URL (Annotate keeps its filters there)
  await openPage(page, "annotate");
  const from = page.getByLabel("From", { exact: true });
  await expect(from).toHaveAttribute("placeholder", "mm/dd/yyyy");
  await from.fill("6.1.25");
  await from.press("Enter");
  await expect(from).toHaveValue("06/01/2025");
  await expect(page).toHaveURL(/start_date=2025-06-01/);
  await expect(dates.first()).toHaveText(/^06\/\d{2}\/2025$/);
  await expect(dates.filter({ hasNotText: /^06\/\d{2}\/2025$/ })).toHaveCount(0);

  // Up and Down step a day
  await from.press("ArrowUp");
  await expect(from).toHaveValue("06/02/2025");
  await expect(page).toHaveURL(/start_date=2025-06-02/);

  // Not a date: an error, and the filter keeps the last date
  await from.fill("13/01/2025");
  await from.press("Tab");
  await expect(page.getByText("No month 13")).toBeVisible();
  await expect(from).toHaveAttribute("aria-invalid", "true");
  await expect(from).toHaveAccessibleDescription("No month 13");
  await expect(page).toHaveURL(/start_date=2025-06-02/);
  await from.fill("");
  await from.press("Enter");
  await expect(page).not.toHaveURL(/start_date/);

  // A URL's ISO date shows in the format
  await openPage(page, "transactions?start_date=2025-05-31");
  await expect(page.getByLabel("From", { exact: true })).toHaveValue("05/31/2025");

  // Stamps: the freshness hint, and the balances' "as of"
  await expect(page.getByTestId("freshness")).toHaveAttribute("title", /\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}/);
  await openPage(page, "dashboard?view=balance");
  await expect(page.getByText(/as of \d{2}\/\d{2}\/2025/).first()).toBeVisible();

  // ISO, set through the API (read when the app loads)
  await page.request.put("/api/settings/display", { data: { date_format: "yyyy-mm-dd" } });
  await openPage(page, "transactions");
  await page.reload();
  await expect(dates.first()).toHaveText(/^\d{4}-\d{2}-\d{2}$/);
});
