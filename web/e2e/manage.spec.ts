import { expect, test } from "@playwright/test";
import { apiGet, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

test("batch apply previews the affected rows, then commits", async ({ page }) => {
  const kidzone = (await apiGet(page, "/transactions?search=KIDZONE")).total;
  await openPage(page, "transactions");
  await page.keyboard.press("/");
  await page.keyboard.type("KIDZONE");
  await expect(page.locator(".page-header .summary")).toContainText(`of ${kidzone}`);
  await page.keyboard.press("Escape");

  await page.keyboard.press("b");
  const dialog = page.getByRole("dialog", { name: "Batch apply" });
  await page.keyboard.type("dining");
  await page.keyboard.press("Enter");
  // Nothing is selected: all matching only by choosing it
  await dialog.getByLabel(`all ${kidzone} matching the filters`).check();
  await page.keyboard.press("Control+Enter");
  await expect(dialog).toContainText(`${kidzone} to change`);
  await expect(dialog.locator("tbody tr")).toHaveCount(kidzone);
  await expect(dialog.locator("tbody tr").first()).toContainText("tags: kids / clothes → food / dining out");

  await page.keyboard.press("Control+Enter");
  await expect(page.getByText(`Changed ${kidzone} transactions`)).toBeVisible();
  const result = await apiGet(page, "/transactions?search=KIDZONE&limit=100");
  expect(result.items.every((item: { annotation: { parts: { subtag: string }[] } }) => item.annotation.parts[0].subtag === "dining out")).toBe(true);
});

test("rename a subtag", async ({ page }) => {
  await openPage(page, "tags");
  await page.getByRole("row", { name: /dining out/ }).click();
  await page.keyboard.press("n");
  const dialog = page.getByRole("dialog");
  await expect(dialog.locator("input")).toHaveValue("dining out");
  await dialog.locator("input").fill("restaurants");
  await page.keyboard.press("Enter");
  await expect(page.getByText("Renamed food / dining out")).toBeVisible();
  await expect(page.getByRole("row", { name: /restaurants/ })).toBeVisible();

  const tags = await apiGet(page, "/tags");
  const food = tags.find((tag: { name: string }) => tag.name === "food");
  expect(food.subtags.map((subtag: { name: string }) => subtag.name)).toEqual(["coffee", "delivery", "groceries", "restaurants"]);
});

test("tag kinds: cycle by key, edit in the dialog, inherited by subtags", async ({ page }) => {
  await openPage(page, "tags");
  const fuel = page.getByRole("row", { name: /^\s*fuel/ });
  await fuel.click();
  await expect(fuel).toContainText("regular (inherited)");
  // x: the next kind
  await page.keyboard.press("x");
  await expect(page.getByText("car / fuel is transfer")).toBeVisible();
  await expect(fuel).toContainText("transfer (own)");

  // The dialog: back to inheriting
  await page.keyboard.press("e");
  const dialog = page.getByRole("dialog", { name: "Edit car / fuel" });
  await expect(dialog.locator("select")).toHaveValue("transfer");
  await dialog.locator("select").selectOption("inherit");
  await expect(dialog).toContainText("Incomes and expenses");
  await page.keyboard.press("Enter");
  await expect(fuel).toContainText("regular (inherited)");

  // A tag's kind is its subtags' unless they override it
  const car = page.getByRole("row", { name: /^\s*car/ }).first();
  await car.click();
  await page.keyboard.press("e");
  await page.getByRole("dialog").locator("select").selectOption("one-off");
  await page.keyboard.press("Enter");
  await expect(fuel).toContainText("one-off (inherited)");
  await expect(page.getByRole("row", { name: /^\s*purchase/ })).toContainText("one-off (own)");

  const tags = await apiGet(page, "/tags");
  const carTag = tags.find((tag: { name: string }) => tag.name === "car");
  expect(carTag.kind).toBe("one-off");
  const fuelSubtag = carTag.subtags.find((subtag: { name: string }) => subtag.name === "fuel");
  expect([fuelSubtag.kind, fuelSubtag.effective_kind]).toEqual([null, "one-off"]);
});

test("account groups: create, move accounts, reorder, role, delete", async ({ page }) => {
  await openPage(page, "accounts");
  const groupRow = (name: string) => page.locator("tr.group-row", { hasText: name });
  await expect(groupRow("spending")).toContainText("3 accounts");

  // Create a holding group by key
  await page.keyboard.press("c");
  const dialog = page.getByRole("dialog", { name: "New group" });
  await dialog.locator("input").first().fill("pension");
  await page.keyboard.press("Enter");
  await expect(page.getByText("Created pension")).toBeVisible();
  await expect(groupRow("pension")).toContainText("holding");

  // Move eitan there with its select, then back with x (the next group)
  const eitan = page.getByRole("row", { name: /^\s*eitan/ });
  await eitan.locator("select").selectOption("pension");
  await expect(page.getByText("eitan is in pension")).toBeVisible();
  await expect(groupRow("pension")).toContainText("1 account");
  await page.getByRole("row", { name: /^\s*eitan/ }).click();
  await page.keyboard.press("x");
  await expect(page.getByText("eitan is in spending")).toBeVisible();

  // Reorder (K moves the group up) and switch the role (x on a group)
  await groupRow("pension").click();
  await page.keyboard.press("K");
  await expect(page.getByText("Moved pension")).toBeVisible();
  await page.keyboard.press("x");
  await expect(page.getByText("pension is for spending")).toBeVisible();
  let groups = await apiGet(page, "/groups");
  expect(groups.map((group: { name: string; role: string }) => [group.name, group.role])).toEqual([
    ["spending", "spending"],
    ["savings", "holding"],
    ["pension", "spending"],
    ["debt", "holding"],
  ]);

  // A group with accounts can't be deleted, an empty one can
  await groupRow("savings").click();
  await page.keyboard.press("d");
  await expect(page.getByText("savings has accounts")).toBeVisible();
  await groupRow("pension").click();
  await page.keyboard.press("d");
  await page.getByRole("dialog").getByRole("button", { name: "Delete" }).click();
  await expect(page.getByText("Deleted pension")).toBeVisible();
  groups = await apiGet(page, "/groups");
  expect(groups.map((group: { name: string }) => group.name)).toEqual(["spending", "savings", "debt"]);
});

test("balance on a date: of a source, a group or the net worth", async ({ page }) => {
  const amount = (value: number) => `${value > 0 ? "+" : value < 0 ? "−" : ""}${Math.abs(value).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  await openPage(page, "accounts");
  const result = page.getByTestId("balance-on-date");
  // The net worth as of the latest data, by default
  const netWorth = await apiGet(page, "/balance");
  await expect(result).toContainText(amount(netWorth.balance));
  await expect(result).toContainText("at the end of 15/06/2025");

  // The wallet, on the day it was counted: the counted amount
  await page.getByLabel("Balance of").selectOption({ label: "cash/wallet" });
  const date = page.getByLabel("Balance date");
  await date.fill("28/04/2025");
  await date.press("Enter");
  const wallet = await apiGet(page, "/balance?account=cash&source=wallet&date=2025-04-28");
  const count = (await apiGet(page, "/transactions?search=Cash count")).items[0];
  expect(count.annotation.description).toContain(`Counted ${wallet.balance.toFixed(2)} in the wallet`);
  await expect(result).toContainText(amount(wallet.balance));
  await expect(result).toContainText(`at the end of 28/04/2025 · ${wallet.rows} rows, the last on 28/04/2025`);

  // A group, on the same date
  await page.getByLabel("Balance of").selectOption({ label: "savings" });
  const savings = await apiGet(page, "/balance?group=savings&date=2025-04-28");
  await expect(result).toContainText(amount(savings.balance));
});
