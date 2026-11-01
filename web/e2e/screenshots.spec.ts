// Screenshots of every page over the demo data, for reviewing the visual design.
// Only runs with SCREENSHOT_DIR set: SCREENSHOT_DIR=/tmp/shots npm run e2e
import { readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { expect, test, type Page } from "@playwright/test";
import { apiGet, DATA_DIR, goTo, openPage, resetDemoData } from "./helpers";

const DIR = process.env.SCREENSHOT_DIR;
test.skip(!DIR, "set SCREENSHOT_DIR to take screenshots");
test.beforeAll(resetDemoData);

const shot = (page: Page, name: string) => page.screenshot({ path: `${DIR}/${name}.png` });

test("screenshots", async ({ page }) => {
  // Annotate: a few rows edited, one split, one flagged, one with an error
  await openPage(page, "annotate");
  const rows = page.locator("tbody[data-id]");
  await expect(rows).toHaveCount(12);
  await shot(page, "01-annotate-queue");
  await page.keyboard.press("a");
  await page.keyboard.press("j");
  await page.keyboard.press("a");
  await page.keyboard.press("r");
  await page.keyboard.press("j");
  await page.keyboard.press("j");
  await page.keyboard.press("s");
  await page.keyboard.type("20");
  await page.keyboard.press("Tab");
  await page.keyboard.type("gifts");
  await page.keyboard.press("Enter"); // on to the part's kind
  for (const _field of ["subtag", "tag", "amount", "description"]) await page.keyboard.press("Shift+Tab");
  await page.keyboard.type("for the neighbours");
  await page.keyboard.press("Escape");
  await page.keyboard.press("j");
  await page.keyboard.press("j");
  await page.keyboard.press("Enter");
  await page.keyboard.type("lunch with the team");
  await page.keyboard.press("Tab");
  await page.keyboard.type("din");
  await shot(page, "02-annotate-editing");
  await page.keyboard.press("Enter");
  await page.keyboard.press("Escape");
  await page.keyboard.press("j");
  await page.keyboard.press("j");
  await page.keyboard.press("s");
  await page.keyboard.type("1000");
  await page.keyboard.press("Escape");
  await page.keyboard.press("x");
  await page.keyboard.press("j");
  await page.keyboard.press("x");
  await shot(page, "03-annotate-drafts");
  await page.keyboard.press("?");
  await shot(page, "04-help");
  await page.keyboard.press("Escape");
  await page.keyboard.press("b");
  await page.keyboard.type("groc");
  await shot(page, "05-annotate-batch-edit");
  await page.keyboard.press("Escape");
  await page.keyboard.press("Escape");

  // Needs review queue
  await page.keyboard.press("m");
  await page.keyboard.press("Enter"); // discard the drafts
  await expect(rows).toHaveCount(5);
  await shot(page, "06-annotate-review");

  // Transactions, with a search and a batch apply preview
  await goTo(page, "t");
  await expect(page.locator(".page-header .summary")).toContainText(`of ${(await apiGet(page, "/status")).transactions}`);
  await page.keyboard.press("/");
  await page.keyboard.type("פיצה");
  await expect(page.locator(".page-header .summary")).toContainText(`of ${(await apiGet(page, "/transactions?search=פיצה")).total}`);
  await page.keyboard.press("Escape");
  await shot(page, "07-transactions-search");
  await page.keyboard.press("b");
  await page.keyboard.type("dining");
  await page.keyboard.press("Enter");
  await page.getByRole("dialog").getByLabel(/matching the filters/).check();
  await page.keyboard.press("Control+Enter");
  await expect(page.getByRole("dialog")).toContainText("to change");
  await shot(page, "08-transactions-batch-apply");
  await page.keyboard.press("Escape");

  // Management pages
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await goTo(page, "g");
  await expect(page.getByRole("row", { name: /groceries/ })).toBeVisible();
  await shot(page, "09-tags");
  await page.getByRole("row", { name: /dining out/ }).click();
  await page.keyboard.press("e");
  await shot(page, "10-tags-edit");
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await goTo(page, "r");
  await expect(page.getByRole("row", { name: /משכורת/ }).first()).toBeVisible();
  await shot(page, "11-rules");
  await page.keyboard.press("c");
  await page.keyboard.type("FRESHWAY|פירות וירקות");
  await page.locator(".editor input").first().focus();
  await page.keyboard.type("food/g");
  await page.keyboard.press("Enter");
  await expect(page.getByRole("dialog")).toContainText("Newly tagged");
  await shot(page, "12-rules-editor");
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await goTo(page, "c");
  await expect(page.getByRole("heading", { name: "Accounts" })).toBeVisible();
  await shot(page, "13-accounts");
  await goTo(page, "o");
  await expect(page.getByText(/^Order cancelled/)).toBeVisible();
  await shot(page, "14-orphans");
  await goTo(page, "d");
  await expect(page.locator(".cards")).toBeVisible();
  await shot(page, "15-dashboard");
  await page.keyboard.press("I");
  await expect(page.getByRole("dialog", { name: "Ingested" })).toBeVisible();
  await shot(page, "16-ingest");
});

/** Hover a chart at a fraction of its width and height, to show its tooltip. */
async function hoverChart(page: Page, label: string, x: number, y: number) {
  const box = (await page.getByRole("img", { name: label }).boundingBox())!;
  await page.mouse.move(box.x + box.width * x, box.y + box.height * y);
  await page.waitForTimeout(400);
}

test("dashboard screenshots", async ({ page }) => {
  const settle = () => page.waitForTimeout(900); // chart animations
  await openPage(page, "dashboard");
  await expect(page.locator(".cards")).toBeVisible();
  await settle();
  await shot(page, "d01-overview");

  await page.keyboard.press("b");
  await settle();
  await hoverChart(page, "Balances over time", 0.62, 0.4);
  await shot(page, "d02-balance-hover");
  await page.getByRole("img", { name: "Balances over time" }).scrollIntoViewIfNeeded();
  await page.locator(".content").evaluate((element) => element.scrollTo(0, element.scrollHeight));
  await settle();
  await shot(page, "d03-balance-table");

  await page.keyboard.press("c");
  await settle();
  await hoverChart(page, "Cash flow by month", 0.55, 0.5);
  await shot(page, "d04-cash-flow");

  await page.keyboard.press("k");
  await settle();
  await page.locator(".list .name", { hasText: "food" }).click();
  await hoverChart(page, "Breakdown treemap", 0.6, 0.3);
  await shot(page, "d05-breakdown");
  await page.keyboard.press("v");
  await page.keyboard.press("v");
  await settle();
  await shot(page, "d06-breakdown-month");

  await page.keyboard.press("t");
  await settle();
  await hoverChart(page, "Trends by tag", 0.7, 0.6);
  await shot(page, "d07-trends");
  await page.keyboard.press("x");
  await page.keyboard.press("v");
  await page.keyboard.press("i");
  await settle();
  await shot(page, "d08-trends-incomes-stacked");
  await page.keyboard.press("i");

  await page.keyboard.press("m");
  await page.keyboard.press("+");
  await page.keyboard.press("+");
  await settle();
  await page.locator(".matrix .name", { hasText: "utilities" }).click();
  await shot(page, "d09-monthly");

  await page.keyboard.press("s");
  await expect(page.locator(".data tbody tr").first()).toBeVisible();
  await shot(page, "d10-source-data");

  await page.keyboard.press("o");
  await page.keyboard.press("p");
  await page.keyboard.press("e");
  await settle();
  await page.locator(".cards .card", { hasText: /^Expenses/ }).click();
  await expect(page.getByRole("complementary")).toContainText("total");
  await shot(page, "d11-drill-down");
  await page.keyboard.press("Escape");

  await page.getByRole("button", { name: /^all ▾/ }).click();
  await shot(page, "d12-only-these-tags");
  await page.getByRole("button", { name: /^all ▾/ }).click();

  await page.keyboard.press("?");
  await shot(page, "d13-help");
  await page.keyboard.press("Escape");
});

test("table screenshots", async ({ page }) => {
  const settle = () => page.waitForTimeout(900); // chart animations
  /** Scroll a table into view, at the top of the dashboard's content. */
  const showTable = async (selector: string) => {
    await page.locator(selector).evaluate((element) => element.scrollIntoView({ block: "start" }));
    await settle();
  };
  await openPage(page, "transactions");
  await expect(page.locator(".page-header .summary")).toContainText(`of ${(await apiGet(page, "/status")).transactions}`);
  await shot(page, "t01-transactions");
  const pages = [
    ["g", "tags", "groceries"],
    ["r", "rules", "משכורת"],
    ["c", "accounts", "savings"],
    ["o", "orphans", "Order cancelled"],
  ];
  for (const [index, [letter, name, content]] of pages.entries()) {
    await goTo(page, letter);
    await expect(page.getByRole("heading", { name, exact: false })).toBeVisible();
    await expect(page.locator("table.grid tbody tr", { hasText: content }).first()).toBeVisible();
    await shot(page, `t0${index + 2}-${name}`);
  }
  await openPage(page, "dashboard");
  await expect(page.locator(".cards")).toBeVisible();
  await page.keyboard.press("c");
  await showTable("table.months");
  await shot(page, "t06-cash-flow-table");
  await page.keyboard.press("b");
  await showTable("table.balances");
  await shot(page, "t07-balance-table");
  await page.keyboard.press("t");
  await showTable("table.tags");
  await shot(page, "t08-trends-table");
  await page.keyboard.press("m");
  await settle();
  await page.locator(".matrix .name", { hasText: "utilities" }).click();
  await shot(page, "t09-monthly-table");
  await page.keyboard.press("s");
  await expect(page.locator(".data tbody tr").first()).toBeVisible();
  await shot(page, "t10-source-data-table");
});

test("ingest screenshots", async ({ page }) => {
  resetDemoData();
  const maya = `${DATA_DIR}/raw/maya`;
  const wallet = `${DATA_DIR}/raw/cash/wallet.csv`;
  await openPage(page, "ingest");
  await expect(page.getByTestId("ingest-preview")).toContainText("Ingesting would change nothing");
  await shot(page, "i01-ingest-clean");

  // Likely problems: a year exported twice, an unparseable and an empty file
  writeFileSync(`${maya}/leumi-checking-2024-again.xls`, readFileSync(`${maya}/leumi-checking-2024.xls`));
  writeFileSync(`${maya}/statement.pdf`, "%PDF-1.4 not a bank export");
  writeFileSync(`${DATA_DIR}/raw/cash/placeholder.csv`, "");
  await page.keyboard.press("R");
  await expect(page.getByTestId("ingest-problems")).toBeVisible();
  await page.locator("details.errors summary").first().click();
  await shot(page, "i02-ingest-problems");

  // The effect: annotations orphaned by vanishing rows, and new rows
  unlinkSync(`${maya}/leumi-checking-2024-again.xls`);
  unlinkSync(`${maya}/statement.pdf`);
  const lines = readFileSync(wallet, "utf-8").split("\n");
  writeFileSync(
    wallet,
    lines.filter((line) => !line.includes("Haircut")).join("\n") + "wallet,2025-06-14,-4.20,Corner bakery\n",
  );
  await page.keyboard.press("R");
  await expect(page.getByTestId("ingest-preview")).toContainText("would be orphaned");
  await page.getByText(/^New transactions/).click();
  await shot(page, "i03-ingest-preview");
  await page.keyboard.press("Control+Enter");
  await shot(page, "i04-ingest-confirm");
  await page.keyboard.press("Enter");
  await expect(page.getByTestId("ingest-result")).toBeVisible();
  await shot(page, "i05-ingest-result");

  // The agent instructions popover, and the help overlay with the page keys
  await page.keyboard.press("@");
  await shot(page, "i06-agent-popover");
  await page.keyboard.press("Escape");
  await page.keyboard.press("?");
  await shot(page, "i07-help");
});

test("settings screenshots", async ({ page }) => {
  await openPage(page, "settings");
  await expect(page.getByLabel(/Net incomes and expenses by tag/)).toBeVisible();
  await shot(page, "s01-settings");
  await page.keyboard.press("j");
  await page.keyboard.press("j");
  await page.keyboard.press("Enter");
  await page.getByRole("dialog").locator("input").first().fill("#5cc596");
  await shot(page, "s02-label-color");
  await page.keyboard.press("Escape");
  const section = page.getByRole("region", { name: "Config file" });
  await section.getByLabel("From").fill("April");
  await section.evaluate((element) => element.scrollIntoView({ block: "end" }));
  await shot(page, "s03-config-error");
  await page.keyboard.press("Escape");
  await page.keyboard.press("?");
  await shot(page, "s04-help");
  await page.keyboard.press("Escape");
  await page.keyboard.press("u");
  await page.keyboard.press("Enter");

  // The banner and badge, as a server with a stale web app and --demo shows them
  await page.route("**/api/server", (route) =>
    route.fulfill({
      json: {
        demo: true,
        stale_web_app:
          "The web app (built 2026-10-20 09:12) is older than its sources (src/pages/Settings.svelte changed 2026-10-27 14:03). Rebuild it with scripts/build-web.sh.",
      },
    }),
  );
  await openPage(page, "dashboard");
  await page.reload();
  await expect(page.locator(".cards")).toBeVisible();
  await page.waitForTimeout(900);
  await shot(page, "s05-banners");
});

test("groups and kinds screenshots", async ({ page }) => {
  const settle = () => page.waitForTimeout(900); // chart animations
  await openPage(page, "accounts");
  await expect(page.locator("tr.group-row").first()).toBeVisible();
  await shot(page, "g01-accounts-groups");
  await page.locator("tr.group-row", { hasText: "savings" }).click();
  await page.keyboard.press("e");
  await expect(page.getByRole("dialog")).toBeVisible();
  await shot(page, "g02-group-dialog");
  await page.keyboard.press("Escape");

  await page.keyboard.press("g");
  await page.keyboard.press("g");
  await page.getByRole("row", { name: /^\s*purchase/ }).click();
  await shot(page, "g03-tags-kinds");
  await page.keyboard.press("e");
  await expect(page.getByRole("dialog")).toBeVisible();
  await shot(page, "g04-kind-dialog");
  await page.keyboard.press("Escape");

  await openPage(page, "dashboard?view=balance&period=24m");
  await settle();
  await shot(page, "g05-balance-groups-stacked");
  await page.getByRole("button", { name: "as lines" }).click();
  await page.getByLabel("accounts and sources").check();
  await settle();
  await shot(page, "g06-balance-groups-lines");
  await page.locator(".content").evaluate((element) => element.scrollTo(0, element.scrollHeight));
  await settle();
  await shot(page, "g07-balance-table");

  await openPage(page, "dashboard?view=flow&period=24m");
  await settle();
  await hoverChart(page, "Cash flow by month", 0.69, 0.5);
  await shot(page, "g08-cash-flow-one-offs-saved");
  await page.locator("table.months").evaluate((element) => element.scrollIntoView({ block: "start" }));
  await page.locator("table.months tbody tr", { hasText: "Sep 2024" }).locator("td").nth(3).hover();
  await page.waitForTimeout(1200);
  await shot(page, "g09-cash-flow-table");

  await page.keyboard.press("O");
  await settle();
  await page.locator(".content").evaluate((element) => element.scrollTo(0, 0));
  await shot(page, "g10-one-offs-included");
  await page.keyboard.press("O");

  await page.locator(".controls .chip.group", { hasText: "savings" }).click();
  await settle();
  await shot(page, "g11-groups-selected");

  await openPage(page, "dashboard?view=monthly&months=6");
  await settle();
  await shot(page, "g12-monthly-vs-average");
  await page.keyboard.press("k");
  await settle();
  await page.locator(".list .name", { hasText: "car" }).click();
  await shot(page, "g13-breakdown-one-offs");
  await page.keyboard.press("t");
  await settle();
  await page.locator(".tags").evaluate((element) => element.scrollIntoView({ block: "start" }));
  await shot(page, "g14-trends-vs-average");
});

test("nav screenshots", async ({ page }) => {
  await openPage(page, "transactions");
  await expect(page.locator(".page-header .summary")).toContainText(/of \d+/);
  const header = { x: 0, y: 0, width: 1500, height: 42 };
  await page.screenshot({ path: `${DIR}/n01-nav.png`, clip: header });
  await page.keyboard.press("g");
  await expect(page.locator(".pending")).toBeVisible();
  await page.screenshot({ path: `${DIR}/n02-nav-after-g.png`, clip: header });
  await shot(page, "n03-after-g");
  await page.keyboard.press("Escape");
});

test("selection screenshots", async ({ page }) => {
  resetDemoData();
  await openPage(page);
  await expect(page.locator(".cards")).toBeVisible();
  await shot(page, "s01-landing-dashboard");
  // Batch edit and batch apply with nothing selected
  await goTo(page, "a");
  await expect(page.locator("tbody[data-id]").first()).toBeVisible();
  await page.keyboard.press("b");
  await expect(page.getByRole("dialog")).toContainText("none selected");
  await shot(page, "s02-annotate-batch-none-selected");
  await page.keyboard.press("Escape");
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await goTo(page, "t");
  await expect(page.locator(".page-header .summary")).toContainText(/of \d+/);
  await page.keyboard.press("b");
  await page.keyboard.type("dining");
  await page.keyboard.press("Enter");
  await shot(page, "s03-transactions-batch-none-selected");
  await page.keyboard.press("Escape");
  await page.keyboard.press("Escape");
  // A selection, with its clear button
  await page.keyboard.press("x");
  await page.keyboard.press("j");
  await page.keyboard.press("x");
  await page.screenshot({ path: `${DIR}/s04-selection-clear.png`, clip: { x: 0, y: 0, width: 1500, height: 200 } });
});

test("dashboard batch A screenshots", async ({ page }) => {
  const settle = () => page.waitForTimeout(900); // chart animations
  await openPage(page, "dashboard");
  await expect(page.locator(".cards")).toBeVisible();
  await settle();
  await shot(page, "a01-overview");
  await openPage(page, "dashboard?view=balance&period=6m");
  await settle();
  await hoverChart(page, "Balances over time", 0.5, 0.4);
  await shot(page, "a02-balance-end-of-day");
  await page.locator(".content").evaluate((element) => element.scrollTo(0, element.scrollHeight));
  await settle();
  await shot(page, "a03-balance-table-as-of");
  await openPage(page, "dashboard?view=flow&period=12m");
  await settle();
  await hoverChart(page, "Cash flow by month", 0.2, 0.5);
  await shot(page, "a04-cash-flow-tooltip");
  await page.locator("table.months").evaluate((element) => element.scrollIntoView({ block: "start" }));
  await page.locator("table.months tbody tr").first().locator("td").nth(3).hover();
  await page.waitForTimeout(1200);
  await shot(page, "a05-cash-flow-table");
  await openPage(page, "dashboard?view=breakdown&net=1&period=all");
  await settle();
  await shot(page, "a06-breakdown-total-net-by-tag");
  await openPage(page, "dashboard?view=breakdown&breakdown=rolling&net=1");
  await settle();
  await shot(page, "a07-breakdown-rolling");
  await openPage(page, "dashboard?view=breakdown&breakdown=month&month=2024-09");
  await settle();
  await shot(page, "a08-breakdown-month");
  await openPage(page, "dashboard?view=trends&rolling=3");
  await settle();
  await shot(page, "a09-trends");
  await openPage(page, "dashboard?view=monthly&period=3m&months=6");
  await settle();
  await shot(page, "a10-monthly-3m");
  await openPage(page, "dashboard?view=ledger&period=3m");
  await expect(page.locator(".data tbody tr").first()).toBeVisible();
  await shot(page, "a11-source-data");
  await openPage(page, "dashboard?rolling=1");
  await settle();
  await page.getByRole("button", { name: /^all ▾/ }).click();
  await shot(page, "a12-previous-month-and-tags");
  await page.keyboard.press("Escape");
  await openPage(page, "settings");
  await expect(page.getByLabel(/Net incomes and expenses by tag/)).toBeVisible();
  await shot(page, "a13-settings-defaults");
  await page.setViewportSize({ width: 800, height: 900 });
  await openPage(page, "dashboard");
  await settle();
  await shot(page, "a14-overview-800px");
  await page.setViewportSize({ width: 1500, height: 900 });
});

test("dashboard batch B screenshots", async ({ page }) => {
  const settle = () => page.waitForTimeout(900); // chart animations
  await page.setViewportSize({ width: 1600, height: 1250 });
  await openPage(page, "dashboard");
  await expect(page.locator(".cards")).toBeVisible();
  await settle();
  await expect(page.getByText("What changed")).toBeVisible();
  await shot(page, "b01-overview");
  await openPage(page, "dashboard?period=all");
  await settle();
  await shot(page, "b03-overview-all");
  await openPage(page, "dashboard?view=flow&period=all");
  await settle();
  await shot(page, "b04-cash-flow-one-offs");
  await page.locator("table.months tfoot").evaluate((element) => element.scrollIntoView({ block: "end" }));
  await shot(page, "b05-cash-flow-footer");
  await openPage(page, "dashboard?view=balance&period=12m");
  await settle();
  await page.locator(".content").evaluate((element) => element.scrollTo(0, element.scrollHeight));
  await settle();
  await shot(page, "b06-balance-bridge-checks");
  await openPage(page, "dashboard?view=trends&trends_by=movers");
  await settle();
  await page.locator("table.tags").evaluate((element) => element.scrollIntoView({ block: "start" }));
  await shot(page, "b07-trends-movers");
  await openPage(page, "dashboard?view=monthly&period=6m");
  await settle();
  await shot(page, "b08-monthly");
  await openPage(page, "dashboard?view=ledger&period=3m");
  await expect(page.locator("tbody tr").first()).toBeVisible();
  await shot(page, "b09-ledger");
  await openPage(page, "dashboard?top_by=largest&one_offs=1");
  await settle();
  await shot(page, "b10-overview-largest-one-offs-included");
  await page.setViewportSize({ width: 1100, height: 1300 });
  await openPage(page, "dashboard");
  await settle();
  await shot(page, "b11-overview-1100px");
  // The car's one-off breaks the cash flow's axis
  await page.setViewportSize({ width: 1600, height: 1250 });
  await openPage(page, "dashboard?view=flow");
  await expect(page.locator(".months tbody tr", { hasText: "Sep 2024" })).toContainText("156,911");
  await settle();
  await shot(page, "b12-cash-flow-axis-break");
  await openPage(page, "dashboard");
  await expect(page.getByText("What changed")).toBeVisible();
  await settle();
  await shot(page, "b13-overview-axis-break");
  await page.setViewportSize({ width: 1500, height: 900 });
});

test("glossary screenshots", async ({ page }) => {
  const settle = () => page.waitForTimeout(900); // chart animations
  // Tooltips: a card's label, a baseline with its months, a table header, a
  // data check, and a label outside the dashboard
  const hover = async (selector: string, name: string) => {
    await page.locator(selector).first().hover();
    await expect(page.locator(".term-tooltip")).toBeVisible();
    await shot(page, name);
    await page.mouse.move(1, 1);
    await expect(page.locator(".term-tooltip")).toHaveCount(0);
  };
  await openPage(page, "dashboard");
  await expect(page.getByText("What changed")).toBeVisible();
  await settle();
  await hover(".cards .label [data-term=runway]", "g01-tooltip-runway");
  await hover(".cards h3 [data-term=baseline]", "g02-tooltip-baseline-months");
  await openPage(page, "dashboard?view=flow");
  await expect(page.locator(".months tbody tr").first()).toBeVisible();
  await settle();
  await hover("th [data-term=set_aside]", "g03-tooltip-cash-flow-header");
  await openPage(page, "dashboard?view=balance&period=12m");
  await settle();
  await page.locator(".content").evaluate((element) => element.scrollTo(0, element.scrollHeight));
  await settle();
  await hover("[data-term=transfer_residual]", "g04-tooltip-data-check");
  await openPage(page, "tags");
  await hover("td [data-term=inherited]", "g05-tooltip-tags-inherited");
  // The glossary page, and one entry opened from a tooltip's link
  await openPage(page, "glossary");
  await expect(page.locator(".entry").first()).toBeVisible();
  await shot(page, "g06-glossary");
  await openPage(page, "glossary?bridge");
  await expect(page.locator("#term-bridge")).toHaveClass(/selected/);
  await settle();
  await shot(page, "g07-glossary-entry");
});

test("overview and kinds set on transactions screenshots", async ({ page }) => {
  const settle = () => page.waitForTimeout(900); // chart animations
  await page.setViewportSize({ width: 1600, height: 1250 });
  await openPage(page, "dashboard");
  await expect(page.getByText("What changed")).toBeVisible();
  await settle();
  await shot(page, "o01-overview");
  await page.setViewportSize({ width: 1500, height: 900 });

  // Annotate: O on a row (its guess accepted as a one-off), the ◆ button on
  // another, and the Kind column's choices on a third
  await openPage(page, "annotate");
  const rows = page.locator("tbody[data-id]");
  await expect(rows).toHaveCount(12);
  await page.keyboard.press("O");
  await rows.nth(2).locator(".one-off-toggle").click();
  await page.keyboard.press("Escape");
  await rows.nth(4).locator('[data-field="kind"]').selectOption("saving");
  await rows.nth(0).hover();
  await shot(page, "o02-annotate-one-off-toggle");
  await page.keyboard.press("?");
  await expect(page.getByRole("dialog", { name: "Keyboard shortcuts" })).toContainText("One-off");
  await shot(page, "o03-help-one-off-key");
  await page.keyboard.press("Escape");
  await page.keyboard.press("Control+Enter");
  await expect(page.getByText("Saved 3 annotations")).toBeVisible();
  await expect(page.locator("tbody.dirty")).toHaveCount(0);

  // Transactions: a split with one part a one-off, and the batch apply's kind
  await openPage(page, "transactions?search=SHOPGLOBE");
  // (the second: the first was just set as a one-off)
  const split = page.locator("tbody[data-id]").nth(1);
  await expect(split.locator('[data-field="description"]')).toHaveAttribute("placeholder", "SHOPGLOBE.COM");
  await page.keyboard.press("j");
  await page.keyboard.press("s");
  await page.keyboard.type("120");
  await page.keyboard.press("Escape");
  await split.locator('[data-field="part-1-tag"]').fill("shopping");
  await split.locator('[data-field="part-1-subtag"]').fill("electronics");
  await split.locator("tr.part").nth(1).locator(".one-off-toggle").click();
  await shot(page, "o04-transactions-split-part-one-off");
  await page.keyboard.press("Escape");
  await page.keyboard.press("u");
  await page.keyboard.press("x");
  await page.keyboard.press("b");
  const dialog = page.getByRole("dialog", { name: "Batch apply" });
  await page.keyboard.type("electronics");
  await page.keyboard.press("Enter");
  await dialog.getByRole("combobox", { name: "Kind" }).selectOption("one-off");
  await page.keyboard.press("Control+Enter");
  await expect(dialog).toContainText("→ one-off");
  await shot(page, "o05-batch-apply-kind");
  await page.keyboard.press("Control+Enter");
  await expect(page.getByText(/Changed 1 transactions/)).toBeVisible();

  // Tags: a subtag's own kind, as opposed to inherited
  await openPage(page, "tags");
  await expect(page.getByText("(own)").first()).toBeVisible();
  await shot(page, "o06-tags-own");

  // The dashboard's rows and ledger mark kinds set on transactions
  await openPage(page, "dashboard?view=ledger&period=all");
  await page.getByRole("button", { name: /Columns/ }).click();
  await page.getByLabel("Kind", { exact: true }).check();
  await page.locator("input.search").fill("SHOPGLOBE");
  await page.locator("input.search").blur();
  await settle();
  await shot(page, "o07-ledger-kind-set");
});

test("header screenshots", async ({ page }) => {
  const header = page.locator("header");
  const shotOfHeader = (name: string) => header.screenshot({ path: `${DIR}/${name}.png` });
  await openPage(page, "dashboard");
  await expect(page.locator(".counts")).toBeVisible();
  await page.mouse.move(1, 400);
  await shotOfHeader("h01-header");
  await page.getByRole("button", { name: "Agent API" }).hover();
  await shotOfHeader("h02-header-agent-hovered");
  await page.getByRole("button", { name: "Keyboard shortcuts" }).hover();
  await shotOfHeader("h03-header-help-hovered");
  await page.getByRole("button", { name: "Agent API" }).click();
  await shot(page, "h04-agent-popover");
  await page.keyboard.press("Escape");
  await openPage(page, "glossary");
  await expect(page.locator(".entry").first()).toBeVisible();
  await page.mouse.move(1, 400);
  await shot(page, "h05-glossary-tab");
});
