import { describe, expect, it } from "vitest";
import { display } from "./display.svelte";
import {
  addMonths,
  averagedSelection,
  baselineLabel,
  baselineTitle,
  cashFlowSelection,
  changedControls,
  COMPUTE_DEFAULTS,
  colorsFor,
  subtagLabel,
  asOfLabel,
  dateLabel,
  defaultState,
  flowSelection,
  formatChange,
  formatWhole,
  movers,
  rankingChoice,
  topRanking,
  moverIsGood,
  oneOffBasis,
  parseState,
  percentChange,
  periodLabel,
  periodMonths,
  monthsBetween,
  rollingLabel,
  rollingTitle,
  shiftPeriod,
  shownGroups,
  stateOptions,
  stateQuery,
  tagSelection,
  toggleGroup,
  transactionsQuery,
  windowMonths,
  windowName,
} from "./dashboard";
import { balanceRowText, breakdownTotal, cashFlowClick, flowBreaks, flowParts, groupByTag, mix } from "./dashboard-charts";
import type { Dashboard } from "./api";

describe("URL state", () => {
  it("leaves defaults out of the URL", () => {
    expect(stateQuery(defaultState())).toBe("");
    expect(parseState("")).toEqual(defaultState());
  });

  it("round-trips every option", () => {
    const settings = {
      ...defaultState(),
      view: "trends" as const,
      period: "6m" as const,
      startMonth: "2024-11",
      endMonth: "2025-02",
      accounts: ["alice", "kim"],
      groups: ["spending", "savings"],
      oneOffs: true,
      tags: ["food, drinks", "car"],
      netByTag: false,
      rolling: "12",
      breakdownMonths: 6,
      completeMonths: false,
      kind: "incomes" as const,
      trendsSums: true,
      trendsStacked: true,
      trendsTop: 3,
      trendsSubtags: true,
      trendsOf: "food",
      breakdownMode: "month" as const,
      breakdownMonth: "2025-01",
      balanceGroups: "lines" as const,
      balanceAccounts: true,
      flowRolling: false,
      topBy: "largest" as const,
      trendsBy: "largest" as const,
    };
    const query = stateQuery(settings);
    expect(query).toContain("account=alice&account=kim");
    expect(query).toContain("group=spending&group=savings");
    expect(query).toContain("one_offs=1");
    expect(parseState(query)).toEqual(settings);
  });

  it("labels dates in the date format, from ISO dates or chart timestamps", () => {
    expect(dateLabel("2026-09-30")).toBe("30/09/2026");
    expect(dateLabel(new Date(2026, 8, 30).getTime())).toBe("30/09/2026");
    display.settings.date_format = "mm/dd/yyyy";
    expect(asOfLabel("2026-09-30")).toBe("as of 09/30/2026");
    display.settings.date_format = "dd/mm/yyyy";
  });

  it("ranks top expenses by movers on the overview, by the largest elsewhere", () => {
    const overview = parseState("");
    expect(topRanking(overview)).toBe("movers");
    expect(topRanking(parseState("view=trends"))).toBe("largest");
    expect(parseState("").trendsBy).toBe("largest");
    // A choice is kept (in the URL) unless it is the view's default
    expect(rankingChoice(overview, "movers")).toBeNull();
    expect(rankingChoice(overview, "largest")).toBe("largest");
    expect(topRanking(parseState("top_by=largest"))).toBe("largest");
    expect(stateQuery({ ...overview, trendsBy: "movers" })).toBe("trends_by=movers");
  });

  it("opens views from their old names", () => {
    expect(parseState("view=data").view).toBe("ledger");
    expect(parseState("view=answers").view).toBe("overview");
    expect(parseState("view=overview").view).toBe("overview");
    expect(stateQuery({ ...defaultState(), view: "ledger" })).toBe("view=ledger");
  });

  it("ignores unknown or invalid values", () => {
    const settings = parseState(
      "view=nope&period=5y&from=2025-13&months=-2&net=maybe&kind=other&top=7&breakdown=pie&month=May&groups_shown=pie",
    );
    expect(settings).toEqual(defaultState());
  });

  it("maps to API options, leaving unset options to their defaults", () => {
    const options = stateOptions(parseState("period=3m&account=kim&net=1"));
    expect(options).toMatchObject({
      period: "3m",
      accounts: ["kim"],
      groups: null,
      include_one_offs: false,
      tags: null,
      net_by_tag: true,
      breakdown_months: null,
      complete_months_only: null,
    });
    // How views show their data is only shown: it doesn't reload
    const shown = stateOptions(parseState("period=3m&trends=sums&breakdown=month&rolling=12"));
    expect(shown).toEqual(stateOptions(parseState("period=3m")));
  });
});

describe("months", () => {
  it("adds months across years", () => {
    expect(addMonths("2024-11", 3)).toBe("2025-02");
    expect(addMonths("2025-01", -1)).toBe("2024-12");
    expect(addMonths("2025-06", -12)).toBe("2024-06");
  });

  it("moves a period by its length, within the available months", () => {
    const months = ["2025-01", "2025-02", "2025-03"];
    const available = monthsBetween("2024-06", "2025-06");
    expect(shiftPeriod(months, -1, available)).toEqual({ startMonth: "2024-10", endMonth: "2024-12" });
    expect(shiftPeriod(months, 1, available)).toEqual({ startMonth: "2025-04", endMonth: "2025-06" });
    expect(shiftPeriod([], 1, available)).toBeNull();
    expect(shiftPeriod(months, 1, [])).toBeNull();
    // Nothing before the first month, or after the last
    expect(shiftPeriod(["2024-06", "2024-07"], -1, available)).toBeNull();
    expect(shiftPeriod(["2025-05", "2025-06"], 1, available)).toBeNull();
    expect(shiftPeriod(["2024-07", "2024-08"], -1, available)).toEqual({ startMonth: "2024-05", endMonth: "2024-06" });
  });

  it("lists months between two", () => {
    expect(monthsBetween("2024-11", "2025-02")).toEqual(["2024-11", "2024-12", "2025-01", "2025-02"]);
    expect(monthsBetween("2025-02", "2025-02")).toEqual(["2025-02"]);
    expect(monthsBetween("2025-03", "2025-02")).toEqual([]);
  });

  it("resolves a period's months from the URL, as the server does", () => {
    const available = monthsBetween("2023-07", "2025-06");
    const months = (query: string) => {
      const resolved = periodMonths(parseState(query), available);
      return resolved.length ? [resolved[0], resolved[resolved.length - 1], resolved.length] : [];
    };
    expect(months("")).toEqual(["2024-07", "2025-06", 12]);
    expect(months("period=3m")).toEqual(["2025-04", "2025-06", 3]);
    expect(months("period=ytd")).toEqual(["2025-01", "2025-06", 6]);
    expect(months("period=all")).toEqual(["2023-07", "2025-06", 24]);
    // Explicit months override the preset's, past the data too
    expect(months("period=6m&from=2024-07&to=2024-12")).toEqual(["2024-07", "2024-12", 6]);
    expect(months("from=2025-03")).toEqual(["2025-03", "2025-06", 4]);
    expect(months("to=2024-12")).toEqual(["2024-07", "2024-12", 6]);
    expect(months("from=2026-01&to=2026-02")).toEqual(["2026-01", "2026-02", 2]);
    expect(months("from=2025-03&to=2025-01")).toEqual([]);
    expect(periodMonths(parseState(""), [])).toEqual([]);
  });

  it("labels periods", () => {
    expect(periodLabel(["2024-12", "2025-01"])).toBe("Dec 2024 – Jan 2025");
    expect(periodLabel(["2025-01"])).toBe("Jan 2025");
    expect(periodLabel([])).toBe("no months");
  });
});

describe("groups", () => {
  const groups = [
    { name: "spending", role: "spending" as const, color: null, position: 0, accounts: ["alice"] },
    { name: "savings", role: "holding" as const, color: null, position: 1, accounts: ["savings"] },
    { name: "debt", role: "holding" as const, color: null, position: 2, accounts: [] },
  ];

  it("shows the spending groups' flows by default", () => {
    expect(shownGroups(defaultState(), groups)).toEqual(["spending"]);
    expect(shownGroups({ ...defaultState(), groups: ["debt"] }, groups)).toEqual(["debt"]);
  });

  it("toggles groups, back to the default", () => {
    const settings = defaultState();
    const withSavings = toggleGroup(settings, groups, "savings");
    expect(withSavings).toEqual(["spending", "savings"]);
    expect(toggleGroup({ ...settings, groups: withSavings! }, groups, "savings")).toEqual([]);
    expect(toggleGroup({ ...settings, groups: withSavings! }, groups, "spending")).toEqual(["savings"]);
  });

  it("keeps the last group", () => {
    expect(toggleGroup(defaultState(), groups, "spending")).toBeNull();
    expect(toggleGroup({ ...defaultState(), groups: ["debt"] }, groups, "debt")).toBeNull();
  });
});

describe("drilling down", () => {
  it("selects cash flows with one-offs, averages without them unless included", () => {
    expect(flowSelection("expenses")).toEqual({ flow: "expenses", kinds: ["regular", "one-off"] });
    expect(averagedSelection("incomes", defaultState())).toEqual({ flow: "incomes", kinds: ["regular"] });
    expect(averagedSelection("incomes", { ...defaultState(), oneOffs: true }).kinds).toEqual(["regular", "one-off"]);
    expect(baselineLabel("6")).toBe("vs 6-mo avg");
    expect(baselineLabel("1")).toBe("vs previous month");
  });

  it("selects untagged rows explicitly", () => {
    expect(tagSelection(null)).toEqual({ untagged: true });
    expect(tagSelection("food")).toEqual({ tag: "food" });
    expect(tagSelection("food", null)).toEqual({ tag: "food", subtag: null });
  });

  it("labels subtags with their tag, unless only one tag's are shown", () => {
    expect(subtagLabel("food", "groceries")).toBe("food / groceries");
    expect(subtagLabel("food", "")).toBe("food / –");
    expect(subtagLabel("food", "groceries", false)).toBe("groceries");
    expect(subtagLabel(null, null)).toBe("untagged");
  });

  it("opens each transaction once", () => {
    expect(transactionsQuery(["a", null, "b", "a"])).toBe("ids=a,b");
  });

  it("tells what a cash flow click selects", () => {
    const months = ["2025-01", "2025-02"];
    expect(cashFlowClick(months, "Expenses", 1)).toMatchObject({ kind: "expenses", month: "2025-02" });
    expect(cashFlowClick(months, `Incomes · ${rollingLabel("6")}`, 0)).toMatchObject({ kind: "incomes", month: "2025-01", rolling: true });
    expect(cashFlowClick(months, "Net", 0).kind).toBe("net");
    expect(cashFlowClick(months, "One-off expenses", 1)).toMatchObject({ kind: "expenses", part: "one-off" });
    expect(cashFlowClick(months, "Expenses", 1)).toMatchObject({ part: "regular", rolling: false });
    expect(cashFlowClick(months, "Set aside", 1)).toMatchObject({ kind: "saved", part: null });
  });

  it("selects a part of a month's flow, or what a rolling mean sums", () => {
    const months = ["2025-01", "2025-02"];
    const settings = defaultState();
    expect(cashFlowSelection(cashFlowClick(months, "One-off expenses", 1), settings)).toEqual({
      flow: "expenses",
      kinds: ["regular", "one-off"],
      month: "2025-02",
      kind: "one-off",
    });
    expect(cashFlowSelection(cashFlowClick(months, "Expenses · 6-mo avg", 0), settings)).toEqual({
      flow: "expenses",
      kinds: ["regular"],
      month: "2025-01",
    });
    expect(cashFlowSelection(cashFlowClick(months, "Net", 0), settings)).toMatchObject({ flow: "net", kind: null });
  });
});

describe("display", () => {
  it("groups breakdown items by tag, largest first", () => {
    const groups = groupByTag([
      { tag: "food", subtag: "groceries", amount: 10 },
      { tag: "car", subtag: "fuel", amount: 25 },
      { tag: "food", subtag: "dining out", amount: 20 },
      { tag: null, subtag: null, amount: 5 },
    ]);
    expect(groups.map((group) => [group.tag, group.amount])).toEqual([
      ["food", 30],
      ["car", 25],
      [null, 5],
    ]);
    expect(groups[0].subtags.map((item) => item.subtag)).toEqual(["dining out", "groceries"]);
  });

  it("keeps labels' own colors and gives the others distinct ones", () => {
    const colors = colorsFor(["food", "car", null, "home"], { car: "#ff0000" });
    expect(colors.get("car")).toBe("#ff0000");
    expect(colors.get("food")).not.toBe(colors.get("home"));
    expect(colors.get(null)).toBeTruthy();
  });

  it("formats whole amounts and changes", () => {
    expect(formatWhole(-1234.5)).toBe("−1,235");
    expect(formatWhole(12.4, { signed: true })).toBe("+12");
    expect(formatWhole(-0.2)).toBe("0");
    expect(percentChange(150, 100)).toBe(50);
    expect(percentChange(50, -100)).toBe(150);
    expect(percentChange(5, 0)).toBeNull();
  });

  it("mixes colors", () => {
    expect(mix("#ffffff", "#000000", 0.5)).toBe("#808080");
    expect(mix("#f00", "#000", 0)).toBe("#ff0000");
  });
});

describe("rolling windows", () => {
  it("labels windows and their months", () => {
    expect(rollingLabel("6")).toBe("6-mo avg");
    expect(windowName(1)).toBe("previous month");
    expect(windowName(12)).toBe("12 months");
    expect(windowMonths("2026-08", 6)).toBe("Mar–Aug 2026");
    expect(windowMonths("2026-02", 6)).toBe("Sep 2025–Feb 2026");
    expect(windowMonths("2026-08", 1)).toBe("Aug 2026");
    // A month's baseline ends the month before
    expect(baselineTitle("6", "2026-09")).toBe("6-mo avg before Sep (Mar–Aug 2026)");
    expect(baselineTitle("1", "2026-09")).toBe("Previous month (Aug 2026)");
    expect(rollingTitle("3", "2026-09")).toBe("3-mo avg through Sep (Jul–Sep 2026)");
  });
});

describe("controls", () => {
  const defaults = { net_by_tag: true, breakdown_months: 3, complete_months_only: true, rolling_months: 6 as const };

  it("tells which options of each group aren't the defaults", () => {
    expect(changedControls(parseState(""), defaults)).toEqual({ when: {}, which: {}, how: {} });
    const settings = parseState("period=6m&account=eitan&group=savings&tag=food&tag=car&one_offs=1&net=0&rolling=3&months=4&complete_months=0");
    expect(changedControls(settings, defaults)).toEqual({
      when: { period: "last 6 months" },
      which: { accounts: "only eitan", groups: "flows of savings", tags: "only the tags food, car" },
      how: {
        rolling: "average of 3 months",
        oneOffs: "one-offs in averages",
        netByTag: "not net by tag",
        completeMonths: "with the current month",
        breakdownMonths: "Monthly view of 4 months",
      },
    });
    expect(changedControls(parseState("from=2024-01&to=2024-06"), defaults).when).toEqual({ startMonth: "explicit months" });
  });

  it("doesn't count options set to their defaults, unless the defaults aren't known", () => {
    const settings = parseState("net=1&rolling=6&months=3&complete_months=1");
    expect(changedControls(settings, defaults).how).toEqual({});
    expect(Object.keys(changedControls(settings, null).how)).toEqual(["rolling", "netByTag", "completeMonths", "breakdownMonths"]);
  });

  it("resets every option of How to compute", () => {
    const settings = { ...parseState("one_offs=1&net=0&rolling=3&months=4&complete_months=0"), ...COMPUTE_DEFAULTS };
    expect(changedControls(settings, null).how).toEqual({});
    expect(stateQuery(settings)).toBe("");
  });
});

describe("changes", () => {
  it("rounds before choosing the arrow", () => {
    expect(formatChange(12.4)).toEqual({ text: "▲ 12%", direction: 1 });
    expect(formatChange(-0.4)).toEqual({ text: "0%", direction: 0 });
    expect(formatChange(-7.6, { space: false })).toEqual({ text: "▼8%", direction: -1 });
  });
});

describe("breakdowns and cash flow bars", () => {
  it("takes percents of the signed total", () => {
    const items = [
      { tag: "rent", subtag: "", amount: 1000 },
      { tag: "car", subtag: "", amount: -200 },
    ];
    expect(breakdownTotal(items)).toBe(800);
  });

  it("draws a refunded one-off within the total", () => {
    expect(flowParts(1000, 300)).toEqual({ regular: 700, oneOff: 300 });
    // A negative one-off lowers the total: the bar is the total, not more
    expect(flowParts(1000, -300)).toEqual({ regular: 1000, oneOff: 0 });
    // Regular refunds larger than the regular part: one bar of the total
    expect(flowParts(100, 300)).toEqual({ regular: 0, oneOff: 100 });
  });
});

describe("movers", () => {
  const trend = (tag: string, amounts: number[], baseline: (number | null)[]) => ({ tag, amounts, rolling: {}, baseline: { "6": baseline } });
  const trends = {
    tag_order: [],
    expenses: [trend("rent", [5000, 5000], [5000, 5000]), trend("food", [2000, 2600], [1900, 2000]), trend("gift", [0, 300], [0, 0])],
    incomes: [trend("salary", [9000, 7500], [9000, 9000])],
  } as unknown as Dashboard["trends"];

  it("ranks tags by their change from the baseline in ₪, either way", () => {
    const ranked = movers(trends, "6");
    expect(ranked.map((mover) => [mover.tag, mover.difference])).toEqual([
      ["salary", -1500],
      ["food", 600],
      ["gift", 300],
    ]);
    // Unchanged tags are left out, and a tag without a baseline is new
    expect(ranked.find((mover) => mover.tag === "gift")?.change).toBeNull();
    expect(movers(trends, "6", ["expenses"]).map((mover) => mover.tag)).toEqual(["food", "gift"]);
  });

  it("tells good news from bad by direction", () => {
    expect(moverIsGood({ direction: "expenses", difference: -10 })).toBe(true);
    expect(moverIsGood({ direction: "incomes", difference: -10 })).toBe(false);
    expect(oneOffBasis(false)).toBe("excl. one-offs");
  });
});

describe("one-offs and the cash flow's scale", () => {
  const cashFlow = (expenses: number[], oneOffs: number[]) =>
    ({
      months: expenses.map((_, index) => `2025-0${index + 1}`),
      incomes: expenses.map(() => 10000),
      expenses,
      saved: expenses.map(() => 0),
      net: expenses.map((value) => 10000 - value),
      one_offs: { incomes: expenses.map(() => 0), expenses: oneOffs },
    }) as unknown as Dashboard["cash_flow"];

  it("breaks the axis below 0 around a month far beyond the routine ones", () => {
    const breaks = flowBreaks(cashFlow([8000, 9000, 108000], [0, 0, 100000]));
    expect(breaks).toHaveLength(1);
    const [{ start, end }] = breaks;
    // From beyond the routine months (the net of the one-off month is -98,000)
    expect(start).toBe(Math.round(-108000 * 0.92));
    expect(end).toBeLessThan(-9000);
    expect(end).toBeGreaterThan(start);
  });

  it("doesn't break routine months", () => {
    expect(flowBreaks(cashFlow([8000, 9000, 9500], [0, 0, 500]))).toEqual([]);
  });
});

describe("balance tooltips", () => {
  const rows = {
    id: ["a", "b", "c"],
    date: ["2025-01-01", "2025-01-01", "2025-01-01"],
    amount: [-1, -2, -3],
    raw_description: ["SuperMart", "Roadstar <Fuel>", "TechnoStore"],
    description: [null, "car wash", null],
    transaction_description: [null, "not fuel", "a gift\nfor Kim"],
    tags: [null, null, null],
    account_source: ["alice/card", "alice/card", "alice/card"],
  };

  it("show the description first, else the raw one in italics", () => {
    expect(balanceRowText(rows, 0)).toBe('<span dir="auto" style="font-style:italic">SuperMart</span>');
    // The part's description alone, the raw one escaped
    expect(balanceRowText(rows, 1)).toMatch(/^<span [^>]*>car wash<\/span>$/);
    expect(balanceRowText({ ...rows, description: [null, null, null], transaction_description: [null, null, null] }, 1)).toContain(">Roadstar &#60;Fuel&#62;</span>");
    // Else the transaction's, on one line
    expect(balanceRowText(rows, 2)).toContain(">a gift (+1 line)</span>");
  });
});
