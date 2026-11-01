import { expect, test } from "@playwright/test";
import { apiGet, apiPost, goTo, openPage, resetDemoData } from "./helpers";

test.beforeEach(resetDemoData);

test("annotate with the keyboard and submit in one batch", async ({ page }) => {
  await openPage(page, "annotate");
  const rows = page.locator("tbody[data-id]");
  await expect(rows).toHaveCount(12);
  const firstId = await rows.nth(0).getAttribute("data-id");
  const secondId = await rows.nth(1).getAttribute("data-id");

  // Guesses are shown but not applied: nothing to submit yet
  await expect(rows.nth(0)).toHaveClass(/guess/);
  await expect(page.getByRole("button", { name: /^Submit/ })).toBeDisabled();

  // Accept the first row's guess, then type the second row's tags and a description
  await page.keyboard.press("a");
  await expect(rows.nth(0)).toHaveClass(/dirty/);
  await page.keyboard.press("j");
  await page.keyboard.press("Enter");
  await expect(rows.nth(1).locator('[data-field="description"]')).toBeFocused();
  await page.keyboard.type("lunch with Kim");
  await page.keyboard.press("Tab");
  await expect(rows.nth(1).locator('[data-field="tag"]')).toBeFocused();
  await page.keyboard.type("dining");
  await expect(page.getByRole("option", { name: /food \/ dining out/ })).toBeVisible();
  await page.keyboard.press("Enter"); // picks the pair, then moves on to the kind
  await expect(rows.nth(1).locator('[data-field="kind"]')).toBeFocused();
  await page.keyboard.press("Escape");
  await page.keyboard.press("r"); // and flag it for review

  await page.keyboard.press("Control+Enter");
  await expect(page.getByText("Saved 2 annotations")).toBeVisible();
  await expect(rows).toHaveCount(10);

  const first = await apiGet(page, `/transactions/${firstId}`);
  expect(first.status).toBe("annotated");
  expect(first.annotation.parts).toMatchObject([{ tag: first.guess?.tag ?? "food" }]);
  const second = await apiGet(page, `/transactions/${secondId}`);
  expect(second.annotation).toMatchObject({
    description: "lunch with Kim",
    needs_review: true,
    authors: ["owner"],
    parts: [{ tag: "food", subtag: "dining out" }],
  });
  const status = await apiGet(page, "/status");
  expect(status.untagged).toBe(10);
});

test("split a transaction, blocked until the parts sum exactly", async ({ page }) => {
  await openPage(page, "annotate");
  const row = page.locator("tbody[data-id]").nth(0);
  const id = await row.getAttribute("data-id");
  const transaction = await apiGet(page, `/transactions/${id}`);

  // Two parts: the second needs an amount, the first takes the rest
  await page.keyboard.press("s");
  await expect(row.locator('[data-field="part-1-amount"]')).toBeFocused();
  await page.keyboard.press("Escape");
  await page.keyboard.press("Control+Enter");
  await expect(row.locator(".message")).toContainText("part 2: tag is missing");
  await expect(page.getByText("Nothing saved: 1 row has errors")).toBeVisible();

  await row.locator('[data-field="part-1-amount"]').focus();
  await page.keyboard.type("30");
  await page.keyboard.press("Tab");
  await page.keyboard.type("gifts");
  await page.keyboard.press("Enter"); // picks the pair, then moves on to the part's kind
  await expect(row.locator('[data-field="part-1-kind"]')).toBeFocused();
  await row.locator('[data-field="part-1-description"]').focus();
  await page.keyboard.type("birthday");
  await expect(row.locator(".remainder")).toContainText("balanced");

  // An amount that doesn't fit blocks the submit
  await row.locator('[data-field="part-0-amount"]').fill("1");
  await expect(row.locator(".remainder")).toContainText("left");
  await row.locator('[data-field="part-0-amount"]').fill("");

  await page.keyboard.press("Control+Enter");
  await expect(page.getByText("Saved 1 annotation")).toBeVisible();
  const saved = await apiGet(page, `/transactions/${id}`);
  const rest = Math.round((transaction.amount + 30) * 100) / 100;
  expect(saved.annotation.parts).toEqual([
    { tag: transaction.guess.tag, subtag: transaction.guess.subtag, kind: "regular", kind_set: false, amount: rest, description: null },
    { tag: "gifts", subtag: "birthdays", kind: "regular", kind_set: false, amount: -30, description: "birthday" },
  ]);
});

test("help overlay", async ({ page }) => {
  await openPage(page, "annotate");
  await page.keyboard.press("?");
  await expect(page.getByRole("dialog", { name: "Keyboard shortcuts" })).toContainText("Accept the guess");
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  // The header's ? button opens it too
  await page.getByRole("button", { name: "Keyboard shortcuts" }).click();
  await expect(page.getByRole("dialog", { name: "Keyboard shortcuts" })).toBeVisible();
});

test("the header shows the agent instructions' URL to copy", async ({ page, context }) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await openPage(page, "annotate");
  const url = `${new URL(page.url()).origin}/agent.md`;
  const toggle = page.getByRole("button", { name: "Agent API" });
  // A button with a robot icon, that says it is open
  await expect(toggle.locator("svg")).toBeVisible();
  await expect(toggle).toHaveAttribute("aria-expanded", "false");
  await toggle.click();
  await expect(toggle).toHaveAttribute("aria-expanded", "true");
  const popover = page.getByRole("dialog", { name: "Agent API" });
  await expect(popover.getByRole("textbox")).toHaveValue(url);
  await expect(popover.getByRole("link", { name: /Open/ })).toHaveAttribute("href", url);
  await popover.getByRole("button", { name: /Copy URL/ }).click();
  await expect(popover.getByRole("button", { name: /Copied/ })).toBeVisible();
  expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(url);
  // Its keyboard shortcut, and Escape closes it
  await page.keyboard.press("Escape");
  await expect(popover).toHaveCount(0);
  await page.keyboard.press("@");
  await expect(popover).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(popover).toHaveCount(0);
  const response = await page.request.get("/agent.md");
  expect(response.headers()["content-type"]).toContain("text/markdown");
  expect(await response.text()).toContain(`GET ${new URL(page.url()).origin}/api/status`);
});

test("leaving the page with unsaved edits asks first", async ({ page }) => {
  await openPage(page, "annotate");
  await expect(page.locator("tbody[data-id]")).toHaveCount(12);
  await page.keyboard.press("a");
  await goTo(page, "g");
  const dialog = page.getByRole("dialog");
  await expect(dialog).toContainText("Discard 1 unsaved edits?");
  await page.keyboard.press("Escape");
  await expect(page).toHaveURL(/#\/annotate$/);
  await expect(page.locator("tbody.dirty")).toHaveCount(1);
  await goTo(page, "g");
  await expect(dialog).toBeVisible();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/#\/tags$/);
  await expect(page.getByRole("heading", { name: "Tags" })).toBeVisible();
});

test("m and the tabs reach every queue, even when the others are empty", async ({ page }) => {
  // Empty both the untagged and the needs-review queues
  await apiPost(page, "/annotations/bulk", { query: { status: "untagged" }, tag: "food", subtag: "groceries", dry_run: false });
  const flagged = await apiGet(page, "/transactions?needs_review=true&limit=100");
  await apiPost(page, "/annotations/review", { ids: flagged.items.map((item: { id: string }) => item.id) });
  const status = await apiGet(page, "/status");
  expect([status.untagged, status.needs_review]).toEqual([0, 0]);

  await openPage(page, "annotate");
  const rows = page.locator("tbody[data-id]");
  const tab = (name: string) => page.getByRole("tab", { name: new RegExp(`^${name}`) });
  await expect(page.getByText("Nothing left to tag")).toBeVisible();

  await page.keyboard.press("m");
  await expect(tab("Needs review")).toHaveAttribute("aria-selected", "true");
  await expect(page.getByText("Nothing flagged for review")).toBeVisible();
  await expect(page).toHaveURL(/#\/annotate\?review$/);

  await page.keyboard.press("m");
  await expect(tab("Custom")).toHaveAttribute("aria-selected", "true");
  await expect(page).toHaveURL(/#\/annotate\?custom$/);
  await expect(rows).toHaveCount(500);
  await expect(page.getByPlaceholder(/^Search/)).toBeVisible();

  await page.keyboard.press("m");
  await expect(tab("Untagged")).toHaveAttribute("aria-selected", "true");
  await expect(page).toHaveURL(/#\/annotate$/);
  await expect(rows).toHaveCount(0);

  // The tabs too, and the mode stays put
  await tab("Needs review").click();
  await expect(page.getByText("Nothing flagged for review")).toBeVisible();
  await tab("Custom").click();
  await expect(rows).toHaveCount(500);
  await page.waitForTimeout(300);
  await expect(tab("Custom")).toHaveAttribute("aria-selected", "true");

  // A reload keeps the queue, and the header's link opens the review queue
  await page.reload();
  await expect(tab("Custom")).toHaveAttribute("aria-selected", "true");
  await page.locator('header a[href="#/annotate?review"]').click();
  await expect(tab("Needs review")).toHaveAttribute("aria-selected", "true");

  // An old link to the custom queue, by its former name
  await openPage(page, "annotate?filter");
  await expect(tab("Custom")).toHaveAttribute("aria-selected", "true");
  await expect(page).toHaveURL(/#\/annotate\?custom$/);
});

test("the filters are always shown in their groups, and reached from the search with Tab", async ({ page }) => {
  await openPage(page, "transactions");
  const summary = page.locator(".page-header .summary");
  const everything = `of ${(await apiGet(page, "/status")).transactions}`;
  await expect(summary).toContainText(everything);
  const groups = {
    "Dates & amounts": ["From", "until", "Minimum amount", "Maximum amount"],
    "Tags & kinds": ["Tags", "Subtags", "Kinds"],
    "Accounts & sources": ["Accounts", "Sources"],
    "Status & review": ["Status", "Needs review", "Authors"],
  };
  for (const [name, labels] of Object.entries(groups)) {
    const group = page.getByRole("group", { name });
    for (const label of labels) await expect(group.getByLabel(label, { exact: true })).toBeVisible();
  }
  await expect(page.getByRole("button", { name: /filters/ })).toHaveCount(0);

  await page.keyboard.press("/");
  await page.keyboard.type("KIDZONE");
  await expect(summary).toContainText("of 12");
  // Tab on past Clear to the dates, the first group, then to the amounts
  await page.keyboard.press("Tab");
  await expect(page.getByRole("button", { name: /^Clear/ })).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(page.getByLabel("From", { exact: true })).toBeFocused();
  await page.getByLabel("Minimum amount").focus();
  await page.keyboard.type("-50");
  await expect(summary).not.toContainText("of 12");
  await expect(page.getByRole("group", { name: "Dates & amounts" }).locator("legend")).toHaveClass(/active/);
  await page.getByRole("button", { name: /^Clear/ }).click();
  await expect(summary).toContainText(everything);

  // Kinds, toggled by keyboard
  const oneOff = page.getByRole("group", { name: "Kinds" }).getByRole("button", { name: "one-off" });
  await oneOff.focus();
  await page.keyboard.press("Enter");
  await expect(oneOff).toHaveAttribute("aria-pressed", "true");
  await expect(summary).not.toContainText(everything);
  await page.getByRole("button", { name: /^Clear/ }).click();
  await expect(summary).toContainText(everything);

  // Annotate's queues show them too
  await goTo(page, "a");
  await expect(page.getByLabel("Tags", { exact: true })).toBeVisible();
});

test("every Annotate queue has the filters, its own for the session and in the URL", async ({ page }) => {
  await openPage(page, "annotate");
  const rows = page.locator("tbody[data-id]");
  const tab = (name: string) => page.getByRole("tab", { name: new RegExp(`^${name}`) });
  await expect(rows).toHaveCount(12);
  // The untagged queue has no status filter (it is the queue), the review queue no review filter
  await expect(page.getByTitle("Status", { exact: true })).toHaveCount(0);
  await expect(page.getByTitle("Needs review", { exact: true })).toBeVisible();

  // Narrowing the untagged queue
  await page.keyboard.press("/");
  await page.keyboard.type("FRESHWAY");
  await expect(page).toHaveURL(/#\/annotate\?search=FRESHWAY$/);
  // Waits for the narrowed rows, before counting them
  await expect(rows.locator('[data-field="description"]').first()).toHaveAttribute("placeholder", "FRESHWAY EXPRESS 041");
  const narrowed = await rows.count();
  expect(narrowed).toBeGreaterThan(0);
  expect(narrowed).toBeLessThan(12);
  await page.locator("body").click({ position: { x: 1, y: 1 } });
  await page.keyboard.press("Escape");

  // Another queue, with filters of its own
  await tab("Custom").click();
  await expect(page).toHaveURL(/#\/annotate\?custom$/);
  await expect(page.getByPlaceholder(/^Search/)).toHaveValue("");
  await expect(rows).toHaveCount(500);
  await page.getByLabel("Tags", { exact: true }).fill("car");
  await expect(page).toHaveURL(/#\/annotate\?custom&tags=car$/);
  // The kinds are beside the tags, clear of their suggestions
  await page.getByRole("group", { name: "Kinds" }).getByRole("button", { name: "one-off" }).click();
  await expect(page).toHaveURL(/#\/annotate\?custom&tags=car&kinds=one-off$/);
  await expect(rows).not.toHaveCount(500);
  expect(await rows.count()).toBeGreaterThan(0);

  // Back: the untagged queue's filters are as they were
  await tab("Untagged").click();
  await expect(page).toHaveURL(/#\/annotate\?search=FRESHWAY$/);
  await expect(page.getByPlaceholder(/^Search/)).toHaveValue("FRESHWAY");
  await expect(rows).toHaveCount(narrowed);

  // The URL's filters, on a reload or from a link
  await page.reload();
  await expect(page.getByPlaceholder(/^Search/)).toHaveValue("FRESHWAY");
  await expect(rows).toHaveCount(narrowed);
  await openPage(page, "annotate?review&search=nothing-matches-this");
  await expect(tab("Needs review")).toHaveAttribute("aria-selected", "true");
  await expect(page.getByTitle("Needs review", { exact: true })).toHaveCount(0);
  await expect(page.getByText("No transactions in the queue match the filters")).toBeVisible();
});

test("submitting writes only what was edited, keeping changes made meanwhile", async ({ page }) => {
  await openPage(page, "annotate?review");
  const rows = page.locator("tbody[data-id]");
  await expect(rows.first()).toBeVisible();
  const id = await rows.first().getAttribute("data-id");
  const before = await apiGet(page, `/transactions/${id}`);
  expect(before.annotation.needs_review).toBe(true);
  expect(before.annotation.authors).not.toContain("owner");

  // An agent writes a description while the row is shown, then the owner marks it reviewed
  const response = await page.request.patch(`/api/transactions/${id}/annotation`, {
    data: { description: "written meanwhile", author: "claude" },
  });
  expect(response.ok()).toBe(true);
  await page.keyboard.press("r");
  await page.keyboard.press("Control+Enter");
  await expect(page.getByText("Saved 1 annotation")).toBeVisible();

  const after = await apiGet(page, `/transactions/${id}`);
  expect(after.annotation).toMatchObject({
    description: "written meanwhile",
    needs_review: false,
    parts: before.annotation.parts,
    // Marking it reviewed doesn't make the owner an author
    authors: [...new Set([...before.annotation.authors, "claude"])],
  });
});
