import { expect, test, type Page } from "@playwright/test";
import { apiGet, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

const period = (page: Page) => page.locator(".period .months");
const card = (page: Page, label: RegExp) => page.locator(".cards .card").filter({ hasText: label });
/** The header of a group of cards, e.g. "Balances" or the month's. */
const cardsHeader = (page: Page, text: string) => page.locator(".cards h3", { hasText: text });

/** A card's number, e.g. 7150 from "7,150". */
async function cardValue(page: Page, label: RegExp): Promise<number> {
  const text = await card(page, label).locator(".value").innerText();
  return Number(text.replace(/[^\d.−-]/g, "").replace("−", "-"));
}

test("controls change the data and stay in the URL", async ({ page }) => {
  await openPage(page, "dashboard");
  await expect(period(page)).toContainText("Jul 2024 – Jun 2025");
  await expect(period(page)).toContainText("12 mo");
  const averageExpenses = await cardValue(page, /^Avg\/mo/);

  // A preset by click, then by key
  await page.getByRole("button", { name: "3M", exact: true }).click();
  await expect(period(page)).toContainText("Apr 2025 – Jun 2025");
  await expect.poll(() => cardValue(page, /^Avg\/mo/)).not.toBe(averageExpenses);
  await page.keyboard.press("p"); // 6M
  await expect(period(page)).toContainText("Jan 2025 – Jun 2025");
  await expect(page).toHaveURL(/period=6m/);

  // Move the period back by its length
  await page.keyboard.press(",");
  await expect(period(page)).toContainText("Jul 2024 – Dec 2024");
  await expect(page).toHaveURL(/from=2024-07&to=2024-12/);

  // With the savings group's flows too, money moved to savings is still saved:
  // savings are measured where they leave the spending groups
  await page.getByRole("button", { name: "6M", exact: true }).click();
  await expect(period(page)).toContainText("Jan 2025 – Jun 2025");
  const savingsRate = async () => (await card(page, /^Set aside/).locator(".sub span").first().innerText()).split(" ")[0];
  await expect(card(page, /^Set aside/).locator(".sub")).toContainText(/^\d+% of incomes/);
  const rate = await savingsRate();
  const groupChip = (name: string) => page.locator(".controls .chip.group", { hasText: name });
  await expect(groupChip("spending")).toHaveClass(/\bon\b/);
  await expect(groupChip("savings")).not.toHaveClass(/\bon\b/);
  await groupChip("savings").click();
  await expect(page).toHaveURL(/group=spending&group=savings/);
  await expect(card(page, /^Avg\/mo/)).toBeVisible();
  await expect.poll(savingsRate).toBe(rate);
  // Back to the default by key: e is every group or the spending groups
  await page.keyboard.press("e");
  await expect(page).not.toHaveURL(/group=/);
  await expect.poll(savingsRate).toBe(rate);
  await page.keyboard.press("e");
  await expect(page).toHaveURL(/group=spending&group=savings/);

  // Only some accounts: balances are theirs only
  const available = await cardValue(page, /^Available/);
  const netWorth = await cardValue(page, /^Net worth/);
  expect(netWorth).toBeGreaterThan(available);
  await page.locator(".controls .chip", { hasText: "eitan" }).click();
  await expect(page).toHaveURL(/account=eitan/);
  await expect.poll(() => cardValue(page, /^Available/)).toBeLessThan(available);

  // The view and options survive a reload
  await page.keyboard.press("t");
  await expect(page.getByRole("heading", { name: "Expenses by tag" })).toBeVisible();
  await page.reload();
  await expect(page.locator(".views .view.current")).toHaveText("Trends");
  await expect(period(page)).toContainText("Jan 2025 – Jun 2025");
  await expect(page.locator(".controls .chips").first().locator(".chip.on")).toHaveText(/eitan/);
  await expect(groupChip("savings")).toHaveClass(/\bon\b/);
});

/** Delay the dashboard's data, as with a lot of real data, until `release` is called. */
async function holdDashboard(page: Page): Promise<() => void> {
  let releaseAll = () => {};
  let held = new Promise<void>((resolve) => (releaseAll = resolve));
  await page.route("**/api/dashboard", async (route) => {
    await held;
    await route.continue();
  });
  return () => {
    releaseAll();
    held = Promise.resolve();
  };
}

test("the period follows the URL at once, not the last data loaded", async ({ page }) => {
  await openPage(page, "dashboard?period=6m");
  await expect(period(page)).toContainText("Jan 2025 – Jun 2025");
  const release = await holdDashboard(page);

  // Moved back, then a preset: its months show while the data loads
  await page.keyboard.press(",");
  await expect(period(page)).toContainText("Jul 2024 – Dec 2024");
  await page.getByRole("button", { name: "6M", exact: true }).click();
  await expect(period(page)).toContainText("Jan 2025 – Jun 2025");
  await expect(page).toHaveURL(/period=6m$/);

  // Moves count from the months asked for: twice back is two periods back
  await page.keyboard.press(",");
  await page.keyboard.press(",");
  await expect(page).toHaveURL(/from=2024-01&to=2024-06/);
  await expect(period(page)).toContainText("Jan 2024 – Jun 2024");
  await page.getByRole("button", { name: "Next period" }).click();
  await expect(page).toHaveURL(/from=2024-07&to=2024-12/);

  release();
  await expect(page.locator(".loading")).toBeHidden();
  await expect(period(page)).toContainText("Jul 2024 – Dec 2024");
  await page.getByRole("button", { name: "3M", exact: true }).click();
  await expect(period(page)).toContainText("Apr 2025 – Jun 2025");
});

test("the period keys stop at the data's ends, like the arrows", async ({ page }) => {
  // The demo data runs from Jun 2023 to Jun 2025
  await openPage(page, "dashboard");
  await expect(period(page)).toContainText("Jul 2024 – Jun 2025");
  await expect(page.getByRole("button", { name: "Next period" })).toBeDisabled();
  await page.keyboard.press(".");
  await page.keyboard.press(",");
  await expect(period(page)).toContainText("Jul 2023 – Jun 2024");
  // Back again: to the period with the first month
  await page.keyboard.press(",");
  await expect(period(page)).toContainText("Jul 2022 – Jun 2023");
  await expect(page.getByRole("button", { name: "Previous period" })).toBeDisabled();
  await page.keyboard.press(",");
  await expect(page).toHaveURL(/from=2022-07&to=2023-06$/);
  await page.keyboard.press(".");
  await page.keyboard.press(".");
  await expect(page).toHaveURL(/from=2024-07&to=2025-06$/);
  await page.keyboard.press(".");
  await expect(page).toHaveURL(/from=2024-07&to=2025-06$/);

  // Every month: nowhere to move
  await page.getByRole("button", { name: "All", exact: true }).click();
  await expect(period(page)).toContainText("Jun 2023 – Jun 2025");
  await page.keyboard.press(",");
  await page.keyboard.press(".");
  await expect(page).toHaveURL(/period=all$/);
  await expect(page.getByRole("button", { name: "Previous period" })).toBeDisabled();
  await expect(page.getByRole("button", { name: "Next period" })).toBeDisabled();

  // Year to date moves by its length, and a preset returns to the latest
  await page.getByRole("button", { name: "YTD", exact: true }).click();
  await expect(period(page)).toContainText("Jan 2025 – Jun 2025");
  await page.keyboard.press(",");
  await expect(period(page)).toContainText("Jul 2024 – Dec 2024");
  await page.getByRole("button", { name: "YTD", exact: true }).click();
  await expect(period(page)).toContainText("Jan 2025 – Jun 2025");
});

test("explicit months in the URL survive a reload and Back, and a preset leaves them", async ({ page }) => {
  await openPage(page, "dashboard?from=2024-01&to=2024-03");
  await expect(period(page)).toContainText("Jan 2024 – Mar 2024");
  await expect(period(page)).toContainText("3 mo");
  await page.keyboard.press(".");
  await expect(page).toHaveURL(/from=2024-04&to=2024-06/);
  await expect(period(page)).toContainText("Apr 2024 – Jun 2024");
  await page.reload();
  await expect(period(page)).toContainText("Apr 2024 – Jun 2024");

  // To another page and Back
  await page.keyboard.press("g");
  await page.keyboard.press("t");
  await expect(page).toHaveURL(/#\/transactions/);
  await page.goBack();
  await expect(period(page)).toContainText("Apr 2024 – Jun 2024");
  await expect(page.locator(".content .card").first()).toBeVisible();

  // Only a start: to the latest data
  await openPage(page, "dashboard?from=2025-03");
  await expect(period(page)).toContainText("Mar 2025 – Jun 2025");
  await page.getByRole("button", { name: "6M", exact: true }).click();
  await expect(page).toHaveURL(/period=6m$/);
  await expect(period(page)).toContainText("Jan 2025 – Jun 2025");
});

test("one-offs count in sums, and in averages only when included", async ({ page }) => {
  // Sep 2024 has two one-offs: the old car's sale (an income) and the new
  // car's purchase (an expense)
  await openPage(page, "dashboard?view=flow");
  await expect(period(page)).toContainText("Jul 2024 – Jun 2025");
  const september = page.locator(".months tbody tr", { hasText: "Sep 2024" });
  await expect(september.locator(".one-off")).toHaveCount(2);
  // What its expenses are compared with: without the one-off
  await expect(september).toContainText("23,511 excl.");
  await page.keyboard.press("o");
  const average = await cardValue(page, /^Avg\/mo/);
  await expect(card(page, /^Avg\/mo/)).toContainText("excl. one-offs");
  const toggle = page.getByLabel("one-offs in averages");
  await expect(toggle).not.toBeChecked();

  await page.keyboard.press("O");
  await expect(page).toHaveURL(/one_offs=1/);
  await expect(toggle).toBeChecked();
  // The average net, with the car's purchase and sale
  await expect.poll(() => cardValue(page, /^Avg\/mo/)).toBeLessThan(average);
  await expect(card(page, /^Avg\/mo/)).toContainText("incl. one-offs");
  // The month's own sum is the same either way
  await page.keyboard.press("c");
  await expect(september.locator("td:nth-child(3) .cell")).toHaveText("156,911");
  await expect(september).not.toContainText("excl.");
  await page.keyboard.press("o");

  await toggle.click();
  await expect(page).not.toHaveURL(/one_offs/);
  await expect.poll(() => cardValue(page, /^Avg\/mo/)).toBe(average);
  // Comparisons say what they compare with
  await page.keyboard.press("c");
  await expect(page.locator(".months thead")).toContainText("vs 6-mo avg");
  // (the cards are a strip on other views than the overviews)
  await page.keyboard.press("o");
  const month = page.locator(".cards h3", { hasText: /^Jun 2025/ });
  await expect(month).toContainText("vs 6-mo avg, excl. one-offs");
  await page.keyboard.press("w"); // 12 months
  await expect(page).toHaveURL(/rolling=12/);
  await expect(month).toContainText("vs 12-mo avg");
  await page.keyboard.press("W");
  await page.keyboard.press("W"); // 3 months
  await expect(month).toContainText("vs 3-mo avg");
  await page.keyboard.press("W"); // the month before
  await expect(month).toContainText("vs previous month");
});

test("trends by subtag are only asked for while shown", async ({ page }) => {
  const asked: boolean[] = [];
  page.on("request", (request) => {
    if (request.method() === "POST" && new URL(request.url()).pathname === "/api/dashboard") {
      asked.push(Boolean(request.postDataJSON().subtag_trends));
    }
  });
  await openPage(page, "dashboard?view=trends");
  await expect(page.locator("table.tags tbody tr").first()).toBeVisible();
  expect(asked).toEqual([false]);
  await page.keyboard.press("u");
  await expect(page.locator("table.tags tbody tr").first()).toContainText(" / ");
  expect(asked).toEqual([false, true]);
  // Back by tag, and to another view: the data loaded still does
  await page.keyboard.press("u");
  await page.keyboard.press("o");
  await expect(page).not.toHaveURL(/view=trends/);
  await page.getByRole("button", { name: "3M", exact: true }).click();
  await expect.poll(() => asked).toEqual([false, true, false]);
});

test("views keep how they show their data in the URL", async ({ page }) => {
  await openPage(page, "dashboard?view=trends");
  await page.getByRole("button", { name: "Monthly sums" }).click();
  await page.getByRole("button", { name: "Stacked" }).click();
  await page.locator(".toolbar select").selectOption("3");
  await expect(page).toHaveURL(/trends=sums/);
  await page.reload();
  await expect(page.getByRole("button", { name: "Monthly sums" })).toHaveClass(/\bon\b/);
  await expect(page.getByRole("button", { name: "Stacked" })).toHaveClass(/\bon\b/);
  await expect(page.locator(".toolbar select")).toHaveValue("3");

  // By subtag, of every tag or of one: by key, by the switch, or a tag's name
  await page.keyboard.press("u");
  await expect(page).toHaveURL(/trends_level=subtag/);
  const table = page.locator("table.tags");
  await expect(table.locator("th").first()).toHaveText("Subtag");
  await expect(table.locator("tbody tr").first()).toContainText(" / ");
  await page.getByRole("combobox", { name: "Subtags of" }).selectOption("food");
  await expect(page).toHaveURL(/trends_of=food/);
  await expect(table.locator("tbody tr").first()).not.toContainText(" / ");
  await page.reload();
  await expect(page.getByRole("combobox", { name: "Subtags of" })).toHaveValue("food");
  await page.getByRole("button", { name: "By tag", exact: true }).click();
  await expect(page).not.toHaveURL(/trends_level/);
  await table.getByRole("button", { name: "car", exact: true }).click();
  await expect(page).toHaveURL(/trends_level=subtag&trends_of=car/);
  await expect(page.getByText("Expenses of car by subtag")).toBeVisible();
  // A subtag's last month opens its rows
  await table.locator("tbody tr").first().locator("button.cell").last().click();
  await expect(page.getByRole("complementary", { name: /Rows of car \/ / })).toBeVisible();
  await page.keyboard.press("Escape");
  await page.keyboard.press("u");

  // The breakdown of any month of the period
  await page.keyboard.press("k");
  await page.getByRole("button", { name: "Month", exact: true }).click();
  await page.getByRole("combobox", { name: "Month" }).selectOption("2024-08");
  await expect(page).toHaveURL(/breakdown=month&month=2024-08/);
  await page.keyboard.press("l");
  await expect(page).toHaveURL(/month=2024-09/);
  await page.reload();
  await expect(page.getByRole("combobox", { name: "Month" })).toHaveValue("2024-09");

  // Balance display and the cash flow's rolling means
  await page.keyboard.press("b");
  await page.getByRole("button", { name: "as lines" }).click();
  await page.getByLabel("accounts and sources").check();
  await page.keyboard.press("c");
  await page.locator(".panel .toggle input").first().uncheck();
  await page.reload();
  await expect(page.locator(".panel .toggle input").first()).not.toBeChecked();
  await page.keyboard.press("b");
  await expect(page.getByRole("button", { name: "as lines" })).toHaveClass(/\bon\b/);
  await expect(page.getByLabel("accounts and sources")).toBeChecked();
});

test("the controls are in groups, whose label lights up when an option isn't the default", async ({ page }) => {
  await openPage(page, "dashboard");
  const groups = {
    When: ["Period", "Previous period", "Next period"],
    "Which money": ["Accounts", "Groups in cash flows and breakdowns"],
    "How to compute": ["one-offs in averages", "net by tag", "complete months only", "Monthly view months"],
  };
  for (const [name, labels] of Object.entries(groups)) {
    const group = page.getByRole("group", { name });
    for (const label of labels) await expect(group.getByLabel(label, { exact: true })).toBeVisible();
    await expect(group.locator("legend")).not.toHaveClass(/active/);
  }
  const legend = (name: string) => page.getByRole("group", { name }).locator("legend");
  await expect(page.getByRole("group", { name: "How to compute" }).getByRole("button", { name: "reset" })).toHaveCount(0);

  // Keys change them as before
  await page.keyboard.press("p");
  await expect(legend("When")).toHaveClass(/active/);
  await page.keyboard.press("O");
  await expect(legend("How to compute")).toHaveClass(/active/);
  await expect(page.getByLabel("one-offs in averages").locator("..")).toHaveClass(/changed/);
  await expect(legend("Which money")).not.toHaveClass(/active/);

  // Only some tags, in a popover
  await page.getByRole("button", { name: "all ▾" }).click();
  await page.getByRole("dialog", { name: "Only these tags" }).getByRole("button", { name: "food", exact: true }).click();
  await expect(page).toHaveURL(/tag=food/);
  await expect(legend("Which money")).toHaveClass(/active/);
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog", { name: "Only these tags" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "food ▾" })).toBeVisible();

  // Reset sets How to compute back, and nothing else
  await page.getByRole("button", { name: "reset" }).click();
  await expect(page).not.toHaveURL(/one_offs/);
  await expect(page).toHaveURL(/period=24m&tag=food/);
  await expect(legend("How to compute")).not.toHaveClass(/active/);
});

test("the flows keep at least one group", async ({ page }) => {
  await openPage(page, "dashboard");
  const spending = page.locator(".controls .chip.group", { hasText: "spending" });
  await expect(spending).toBeDisabled();
  await page.locator(".controls .chip.group", { hasText: "savings" }).click();
  await expect(spending).toBeEnabled();
  await spending.click();
  await expect(page).toHaveURL(/group=savings/);
  await expect(page.locator(".controls .chip.group", { hasText: "savings" })).toBeDisabled();
});

test("balances run to the latest data, as of its date", async ({ page }) => {
  await openPage(page, "dashboard?period=3m");
  await expect(cardsHeader(page, "Balances")).toContainText("as of 15/06/2025");
  await page.keyboard.press("b");
  await expect(page.locator(".panel", { has: page.getByRole("heading", { name: "Balances", exact: true }) })).toContainText("as of 15/06/2025");
  // Rows of the balance table open by keyboard
  await page.locator(".balances .row-link").first().focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("complementary", { name: /Rows of net worth/ })).toBeVisible();
});

test("views switch by key and the help lists them", async ({ page }) => {
  await openPage(page, "dashboard");
  await expect(page.locator(".views .view.current")).toHaveText("Overview");
  for (const [key, title] of [
    ["b", "Balance"],
    ["o", "Overview"],
    ["b", "Balance"],
    ["c", "Cash flow"],
    ["k", "Breakdown"],
    ["m", "Monthly"],
    ["s", "Ledger"],
    ["]", "Overview"],
    ["[", "Ledger"],
  ]) {
    await page.keyboard.press(key);
    await expect(page.locator(".views .view.current")).toHaveText(title);
  }
  await expect(page.locator(".data tbody tr").first()).toBeVisible();
  await page.keyboard.press("?");
  const help = page.getByRole("dialog", { name: "Keyboard shortcuts" });
  await expect(help.getByText("View: Trends")).toBeVisible();
  await expect(help.getByText("View: Overview")).toBeVisible();
  await expect(help.getByText("Move the period back by its length")).toBeVisible();
});

test("drill down from a number to its rows, then annotate them", async ({ page }) => {
  await openPage(page, "dashboard?period=3m");
  await expect(period(page)).toContainText("Apr 2025 – Jun 2025");

  // The month's expenses: the rows add up to the card's number
  const expenses = await cardValue(page, /^Expenses/);
  await card(page, /^Expenses/).click();
  const panel = page.getByRole("complementary", { name: /Rows of Expenses · Jun 2025/ });
  await expect(panel).toBeVisible();
  await expect(panel.locator(".summary")).toContainText("total");
  const summary = await panel.locator(".summary").innerText();
  const total = Number(summary.match(/total ([\d,.]+)/)![1].replaceAll(",", ""));
  expect(Math.round(total)).toBe(expenses);
  await page.keyboard.press("Escape");
  await expect(panel).toBeHidden();

  // A cell of the monthly table: a tag in a month
  await page.keyboard.press("m");
  const untaggedRow = page.locator(".matrix tbody tr", { hasText: "untagged" });
  const cell = untaggedRow.locator("td.cell button").last();
  const cellAmount = Number((await cell.innerText()).replaceAll(",", ""));
  await cell.click();
  const rowsPanel = page.getByRole("complementary", { name: /Rows of untagged · expenses · Jun 2025/ });
  await expect(rowsPanel.locator(".summary")).toContainText("total");
  const rowCount = await rowsPanel.locator(".row").count();
  const rowsSummary = await rowsPanel.locator(".summary").innerText();
  expect(Math.round(Number(rowsSummary.match(/total ([\d,.]+)/)![1].replaceAll(",", "")))).toBe(cellAmount);

  // Open them in Transactions, and tag the first with the keyboard
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/#\/transactions\?ids=/);
  await expect(page.getByText(`Only ${rowCount} chosen transaction`)).toBeVisible();
  const rows = page.locator("tbody[data-id]");
  await expect(rows).toHaveCount(rowCount);
  const id = await rows.nth(0).getAttribute("data-id");
  await page.keyboard.press("Enter");
  await page.keyboard.press("Tab");
  await page.keyboard.type("dining");
  await page.keyboard.press("Enter");
  await page.keyboard.press("Escape");
  await page.keyboard.press("Control+Enter");
  await expect(page.getByText("Saved 1 annotation")).toBeVisible();
  const saved = await apiGet(page, `/transactions/${id}`);
  expect(saved.annotation.parts).toMatchObject([{ tag: "food", subtag: "dining out" }]);

  // Back on the dashboard, the untagged expenses of the month shrank
  await openPage(page, "dashboard?view=monthly&period=3m");
  await expect
    .poll(async () => Number((await untaggedRow.locator("td.cell button").last().innerText()).replaceAll(",", "")))
    .toBeLessThan(cellAmount);
});

test("narrow windows don't scroll sideways", async ({ page }) => {
  await page.setViewportSize({ width: 800, height: 900 });
  for (const view of ["overview", "balance", "flow", "breakdown", "trends", "monthly", "ledger"]) {
    await openPage(page, `dashboard?view=${view}`);
    await expect(page.locator(".cards, .strip")).toBeVisible();
    await page.waitForTimeout(300);
    const width = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(width, view).toBeLessThanOrEqual(800);
  }
});

test("the overview answers, and is reachable by keyboard", async ({ page }) => {
  await openPage(page, "dashboard?view=trends");
  await page.keyboard.press("o");
  await expect(page.locator(".views .view.current")).toHaveText("Overview");
  // The default view: no view in the URL
  await expect(page).not.toHaveURL(/view=/);
  await expect(page.getByRole("button", { name: /What changed/ })).toBeVisible();
  // Its legends are buttons
  const net = page.getByRole("group", { name: "Cash flow series" }).getByRole("button", { name: "Net" });
  await net.focus();
  await page.keyboard.press("Enter");
  await expect(net).toHaveAttribute("aria-pressed", "false");
  // A mover opens its rows
  await page.locator(".changes .row").first().click();
  await expect(page.getByRole("complementary", { name: /Rows of .* · Jun 2025/ })).toBeVisible();
  await page.keyboard.press("Escape");
  await page.keyboard.press("b");
  await expect(page.locator(".views .view.current")).toHaveText("Balance");
  // The view tabs are buttons too
  await page.locator(".views .view", { hasText: "Overview" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.locator(".views .view.current")).toHaveText("Overview");
  // Its old name opens it
  await openPage(page, "dashboard?view=answers");
  await expect(page.locator(".views .view.current")).toHaveText("Overview");
});

test("the bridge adds up and its terms open their rows", async ({ page }) => {
  await openPage(page, "dashboard?view=balance");
  const bridge = page.locator("table.bridge");
  await expect(bridge).toContainText("Opening");
  await bridge.locator("tr", { hasText: "expenses" }).locator(".cell").first().click();
  const panel = page.getByRole("complementary", { name: /Rows of Available · expenses/ });
  await expect(panel).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByText("rows tagged as saving, from the spending groups")).toBeVisible();
  // Reconciliations (the demo's cash count) are a term of their own, and not
  // one-sided transfers
  await bridge.locator("tr", { hasText: "reconciliations" }).locator(".cell").first().click();
  const reconciliations = page.getByRole("complementary", { name: /Rows of Available · reconciliations/ });
  await expect(reconciliations).toContainText("cash count");
  await page.keyboard.press("Escape");
  await expect(page.getByText("adding up to 0: both sides are recorded")).toBeVisible();
});

test("top expenses rank movers or the largest, kept in the URL", async ({ page }) => {
  // Movers by default on the overview, the largest elsewhere (the strip of
  // other views has no top expenses: the choice shows on the overview)
  await openPage(page, "dashboard");
  const top = page.locator(".cards .group.top");
  await expect(top.getByRole("button", { name: "movers" })).toHaveClass(/\bon\b/);
  await top.getByRole("button", { name: "largest" }).click();
  await expect(page).toHaveURL(/top_by=largest/);
  await expect(top.getByRole("button", { name: "largest" })).toHaveClass(/\bon\b/);
  await expect(top.locator("li").first()).toContainText("housing");
  await top.getByRole("button", { name: "movers" }).click();
  await expect(page).not.toHaveURL(/top_by/);
  // The old name of the ledger still opens it
  await openPage(page, "dashboard?view=data");
  await expect(page.locator(".views .view.current")).toHaveText("Ledger");
});

test("movers open the rows they sum", async ({ page }) => {
  // Without one-offs, as movers compare: no one-off row in the panel
  await openPage(page, "dashboard?from=2024-07&to=2024-09&top_by=movers");
  await page.locator(".cards .group.top .link-row", { hasText: "car" }).click();
  const panel = page.getByRole("complementary", { name: /Rows of car · expenses · Sep 2024, excl. one-offs/ });
  await expect(panel).toBeVisible();
  await expect(panel).not.toContainText("purchase");
});
