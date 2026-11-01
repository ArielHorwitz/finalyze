"""The semantics of tag kinds and account groups, in every computation that uses
them, on a small hand-made household:

- "me" (spending): a salary of 1000 and food of 300 a month, an initial balance
  and a wallet counted 25 short of its rows in April (offsets, reconciliations),
  a card payment (a transfer between own sources), 200 set aside into "stash"
  in February (saving), a wedding of 2000 in March and a gift of 500 in April
  (one-offs).
- "stash" (a holding group, "savings"): receives the 200, and 10 of interest
  (regular) in April.

The same kinds set on each transaction, rather than on tags, give the same
results.
"""

import datetime

import polars as pl
import pytest

from finalyze.analysis import dashboard, tables
from finalyze.config import Filters
from finalyze.money import to_minor_units
from finalyze.source.data import FLOW_KINDS, SourceData, post_process
from finalyze.source.identity import derive_ids
from finalyze.source.schema import RAW_SCHEMA
from finalyze.source.tag import snapshot_of
from finalyze.store import Annotation, Part, Store

MONTHS = ["2025-01", "2025-02", "2025-03", "2025-04"]
ALL_MONTHS = dashboard.Period()


def day(month: str, day_of_month: int) -> datetime.date:
    year, month_number = month.split("-")
    return datetime.date(int(year), int(month_number), day_of_month)


def household_rows():
    rows = [("me", "checking", day("2025-01", 1), 5000.0, "Initial balance")]
    for month in MONTHS:
        rows.append(("me", "checking", day(month, 5), 1000.0, "Salary"))
        rows.append(("me", "checking", day(month, 10), -300.0, "Grocer"))
    rows += [
        ("me", "checking", day("2025-02", 15), -200.0, "To stash"),
        ("stash", "savings", day("2025-02", 15), 200.0, "To stash"),
        ("me", "checking", day("2025-03", 20), -2000.0, "Wedding hall"),
        ("me", "checking", day("2025-03", 25), -100.0, "Card payment"),
        ("me", "card", day("2025-03", 25), 100.0, "Card payment"),
        ("me", "checking", day("2025-04", 12), 500.0, "Gift from aunt"),
        ("stash", "savings", day("2025-04", 30), 10.0, "Interest"),
        ("me", "wallet", day("2025-04", 28), -25.0, "Cash count"),
    ]
    schema = {name: dtype for name, dtype in RAW_SCHEMA.items() if name != "id"}
    frame = pl.DataFrame(rows, schema=schema, orient="row")
    return derive_ids(frame).select(*RAW_SCHEMA)


RULES = (
    ("Initial balance", "offset", "initial"),
    ("Cash count", "offset", "count"),
    ("Salary", "salary", ""),
    ("Grocer", "food", "groceries"),
    ("To stash", "saving", "deposit"),
    ("Wedding hall", "wedding", "event"),
    ("Card payment", "transfer", "card"),
    ("Gift from aunt", "gifts", "received"),
    ("Interest", "interest", ""),
)
KINDS = {"offset": "reconciliation", "transfer": "transfer", "saving": "saving"}
SUBTAG_KINDS = {("wedding", "event"): "one-off", ("gifts", "received"): "one-off"}


@pytest.fixture(scope="module")
def frame(tmp_path_factory):
    path = tmp_path_factory.mktemp("kinds").joinpath("finalyze.db")
    with Store(path) as store:
        for description, tag, subtag in RULES:
            store.add_rule(Filters(raw_description=description), tag, subtag)
        for tag, kind in KINDS.items():
            store.update_tag(tag, kind=kind)
        for (tag, subtag), kind in SUBTAG_KINDS.items():
            store.update_subtag(tag, subtag, kind=kind)
        store.add_group("savings", "holding")
        store.ensure_accounts(["me"])
        store.update_account("stash", group="savings")
        return post_process(household_rows(), store)


@pytest.fixture(scope="module")
def frame_with_kinds_set(tmp_path_factory):
    """The household, with every kind set on its transactions: all tags are
    regular, and transactions are annotated rather than tagged by rules."""
    path = tmp_path_factory.mktemp("kinds_set").joinpath("finalyze.db")
    rows = household_rows()
    labels = {description: (tag, subtag) for description, tag, subtag in RULES}
    with Store(path) as store:
        store.add_group("savings", "holding")
        store.ensure_accounts(["me"])
        store.update_account("stash", group="savings")
        annotations = []
        for row in rows.iter_rows(named=True):
            tag, subtag = labels[row["raw_description"]]
            kind = SUBTAG_KINDS.get((tag, subtag), KINDS.get(tag))
            part = Part(to_minor_units(row["amount"]), tag, subtag, kind=kind)
            annotations.append(Annotation(row["id"], snapshot_of(row), (part,)))
        store.set_annotations(annotations)
        return post_process(rows, store)


def by_month(frame: pl.DataFrame) -> dict[str, float]:
    return dict(zip(frame["month"], frame["amount"], strict=True))


def periods(period=ALL_MONTHS) -> dashboard.Periods:
    return dashboard.Periods(period, period, MONTHS)


def build(source: SourceData, period=ALL_MONTHS):
    return dashboard.build(source, periods(period), ["spending"])


def test_rows_carry_their_kind_and_group(frame):
    kinds = dict(frame.filter(~pl.col("is_sentinel_tick")).select("tag", "kind").rows())
    assert kinds == {
        "offset": "reconciliation",
        "salary": "regular",
        "food": "regular",
        "saving": "saving",
        "wedding": "one-off",
        "transfer": "transfer",
        "gifts": "one-off",
        "interest": "regular",
    }
    groups = dict(frame.select("account", "group").unique().rows())
    assert groups == {"me": "spending", "stash": "savings"}


def test_monthly_flows(frame):
    source = SourceData(frame)
    # One-offs are real incomes and expenses, transfers never, savings apart
    assert by_month(tables.monthly_flow(source, "incomes")) == {
        "2025-01": 1000.0,
        "2025-02": 1000.0,
        "2025-03": 1000.0,
        "2025-04": 1500.0,
    }
    assert by_month(tables.monthly_flow(source, "expenses")) == {
        "2025-01": 300.0,
        "2025-02": 300.0,
        "2025-03": 2300.0,
        "2025-04": 300.0,
    }
    assert by_month(tables.monthly_flow(source, "net")) == {
        "2025-01": 700.0,
        "2025-02": 700.0,
        "2025-03": -1300.0,
        "2025-04": 1200.0,
    }
    assert by_month(tables.monthly_flow(source, "saved")) == {"2025-02": 200.0}


def test_averages_leave_out_one_offs_unless_included(frame):
    without = by_month(tables.rolling_flow(SourceData(frame), "expenses", 2))
    assert without["2025-03"] == without["2025-04"] == 300.0
    with_one_offs = by_month(
        tables.rolling_flow(SourceData(frame, include_one_offs=True), "expenses", 2)
    )
    assert with_one_offs["2025-03"] == 1300.0
    incomes = by_month(tables.rolling_flow(SourceData(frame), "incomes", 2))
    assert incomes["2025-04"] == 1000.0


def test_breakdowns(frame):
    source = SourceData(frame)
    expenses = tables.breakdown_total(source, "expenses")
    # Saving and transfers are not expenses; one-offs are, marked
    assert dict(expenses.select("tag", "kind").rows()) == {
        "food": "regular",
        "wedding": "one-off",
    }
    incomes = tables.breakdown_total(source, "incomes")
    assert set(incomes["tag"]) == {"salary", "gifts"}
    march = tables.breakdown_months(source, "expenses").filter(
        pl.col("month") == "2025-03"
    )
    assert dict(march.select("tag", "amount").rows()) == {
        "food": 300.0,
        "wedding": 2000.0,
    }
    # Rolling breakdowns are averages: without one-offs, unless included
    rolling = tables.breakdown_rolling_total(source, "expenses", 2, "2025-04")
    assert set(rolling["tag"]) == {"food"}
    included = tables.breakdown_rolling_total(
        SourceData(frame, include_one_offs=True), "expenses", 1, "2025-03"
    )
    assert dict(included.select("tag", "kind").rows()) == {
        "food": "regular",
        "wedding": "one-off",
    }


def test_trends(frame):
    trends = tables.tag_trends(SourceData(frame), "expenses", 2)
    assert set(trends["tag"]) == {"food"}
    included = tables.tag_trends(
        SourceData(frame, include_one_offs=True), "expenses", 2
    )
    assert set(included["tag"]) == {"food", "wedding"}
    # Every tag of incomes and expenses is ordered, one-offs too
    assert set(tables.tag_order(SourceData(frame))) == {
        "salary",
        "food",
        "wedding",
        "gifts",
    }


def test_balances(frame):
    result = build(SourceData(frame))
    totals = {total["key"]: total["balance"] for total in result["balance"]["totals"]}
    # Reconciliations count, and transfers move money within "me"
    me = 5000 + 4 * 700 - 200 - 2000 + 500 - 25
    assert totals == {
        "net_worth": me + 210,
        "available": me,
        "group:spending": me,
        "group:savings": 210,
    }
    series = {series["key"]: series for series in result["balance"]["series"]}
    assert list(series)[:4] == [
        "net_worth",
        "available",
        "group:spending",
        "group:savings",
    ]
    # One point a day, at the day's end: the card payment's two rows (out of
    # checking, into the card) are one point, and the saving's too
    net_worth = series["net_worth"]
    assert len(net_worth["dates"]) == len(set(net_worth["dates"]))
    payment_day = net_worth["dates"].index(day("2025-03", 25))
    assert len(net_worth["rows"][payment_day]) == 2
    assert net_worth["values"][payment_day] == net_worth["values"][payment_day - 1]
    assert result["balance"]["as_of"] == day("2025-04", 30)


def test_reconciliations_are_only_in_balances(frame):
    source = SourceData(frame)
    reconciliations = source.get(kinds=frozenset({"reconciliation"}))
    assert sorted(reconciliations["amount"]) == [-25.0, 5000.0]
    # Never incomes or expenses (whichever way tags are netted), nor in trends
    # or breakdowns
    for net_by_tag in (False, True):
        netted = SourceData(frame, net_by_tag=net_by_tag, include_one_offs=True)
        for flow in ("incomes", "expenses", "net", "saved"):
            assert "offset" not in set(tables.flow_rows(netted, flow)["tag"])
        for direction in ("incomes", "expenses"):
            assert "offset" not in set(tables.breakdown_total(netted, direction)["tag"])
            assert "offset" not in set(tables.tag_trends(netted, direction, 2)["tag"])
    assert "offset" not in tables.tag_order(source)
    # In the balance of the wallet, its account, its group and the net worth
    balance = build(source)["balance"]
    wallet = next(item for item in balance["accounts"] if item["source"] == "wallet")
    assert wallet["balance"] == -25.0
    totals = {total["key"]: total["balance"] for total in balance["totals"]}
    assert totals["net_worth"] - totals["group:savings"] == totals["available"]


def test_bridge_has_reconciliations_as_their_own_term(frame):
    bridge = build(SourceData(frame), dashboard.Period(start="2025-01"))["bridge"]
    for line in bridge:
        assert line["opening"] == 0.0
        assert line["reconciliations"] == 4975.0
        assert line["transfers"] == 0.0
        assert line["rounding"] == 0.0
        assert line["closing"] == pytest.approx(
            line["opening"]
            + line["incomes"]
            - line["expenses"]
            - line["saved"]
            + line["transfers"]
            + line["reconciliations"]
        )
    # Its rows
    rows = dashboard.selected_rows(
        SourceData(frame),
        periods(),
        dashboard.Selection(line="net_worth", kinds=frozenset({"reconciliation"})),
    )
    assert rows["amount"].sum() == 4975.0


def test_the_transfers_check_leaves_out_reconciliations(frame):
    # The card payment's two sides cancel out, and the initial balance and
    # the cash count are not one-sided transfers
    health = build(SourceData(frame))["health"]
    assert health["transfer_residual"] == 0.0
    assert health["transfer_rows"] == 2


def test_cash_flow_section(frame):
    cash_flow = build(SourceData(frame))["cash_flow"]
    assert cash_flow["months"] == MONTHS
    assert cash_flow["saved"] == [0.0, 200.0, 0.0, 0.0]
    assert cash_flow["saved_rate"] == [0.0, 0.2, 0.0, 0.0]
    assert cash_flow["kept_rate"] == [0.7, 0.7, -1.3, 0.8]
    assert cash_flow["one_offs"] == {
        "incomes": [0.0, 0.0, 0.0, 500.0],
        "expenses": [0.0, 0.0, 2000.0, 0.0],
    }
    # Baselines are the rolling means before each month, without one-offs
    assert cash_flow["baseline"]["1"]["expenses"] == [None, 300.0, 300.0, 300.0]
    assert cash_flow["baseline"]["3"]["expenses"] == [None, None, None, 300.0]
    # Rolling means are over every month, also those without savings
    assert cash_flow["rolling"]["3"]["saved"] == [None, None, 66.67, 66.67]
    # What months are compared with their baselines: without one-offs
    assert cash_flow["averaged"]["expenses"] == [300.0, 300.0, 300.0, 300.0]
    assert cash_flow["averaged"]["incomes"][-1] == 1000.0


def test_headline(frame):
    headline = build(SourceData(frame))["headline"]
    assert headline["last"]["incomes"] == 1500.0
    assert headline["last"]["one_off_incomes"] == 500.0
    assert headline["previous"]["one_off_expenses"] == 2000.0
    # Monthly averages leave out one-offs
    assert headline["averages"]["expenses"] == 300.0
    assert headline["averages"]["incomes"] == 1000.0
    assert headline["averages"]["saved"] == 50.0
    assert headline["averages"]["saved_rate"] == 0.05
    assert headline["averages"]["kept_rate"] == 0.7
    # The period's sums, one-offs included
    assert headline["totals"]["net"] == 1300.0
    assert headline["totals"]["kept_rate"] == round(1300 / 4500, 4)
    assert headline["totals"]["one_off_expenses"] == 2000.0
    assert headline["averages"]["one_off_expenses"] == 0.0
    included = build(SourceData(frame, include_one_offs=True))["headline"]
    assert included["averages"]["expenses"] == 800.0
    assert included["averages"]["one_off_expenses"] == 500.0
    assert headline["available"] < headline["net_worth"]


def test_top_expenses_mark_one_offs(frame):
    march = dashboard.Period(end="2025-03")
    top = build(SourceData(frame), march)["headline"]["top_expenses"]
    assert [(item["tag"], item["amount"], item["one_off"]) for item in top] == [
        ("wedding", 2000.0, 2000.0),
        ("food", 300.0, 0.0),
    ]
    wedding, food = top
    # One-offs have no average to compare with
    assert wedding["baseline"] == {} and wedding["average"] is None
    assert food["baseline"] == {"1": 300.0, "3": None, "6": None, "12": None}


def test_groups_option(frame):
    # The holding group's own flows: its interest is an income, and nothing is
    # saved from it (savings are measured where they leave the spending groups)
    stash = SourceData(frame, groups=frozenset({"savings"}))
    assert by_month(tables.monthly_flow(stash, "saved")) == {}
    assert by_month(tables.monthly_flow(stash, "incomes")) == {"2025-04": 10.0}
    # Both sides of a saving within the groups don't cancel out
    both = SourceData(frame, groups=frozenset({"spending", "savings"}))
    assert by_month(tables.monthly_flow(both, "saved")) == {"2025-02": 200.0}


def test_drilled_rows_add_up(frame):
    source = SourceData(frame)
    result = build(source)
    for flow, index in (("saved", 1), ("expenses", 2), ("incomes", 3)):
        selection = dashboard.Selection(
            flow=flow, kinds=FLOW_KINDS, month=MONTHS[index]
        )
        rows = dashboard.selected_rows(source, periods(), selection)
        assert rows["amount"].sum() == result["cash_flow"][flow][index]
    # The rows of an average leave out one-offs
    selection = dashboard.Selection(
        flow="expenses", kinds=frozenset({"regular"}), month="2025-03"
    )
    rows = dashboard.selected_rows(source, periods(), selection)
    assert rows["amount"].sum() == 300.0
    every_group = dashboard.selected_rows(
        source, periods(), dashboard.Selection(all_groups=True)
    )
    assert set(every_group["account"]) == {"me", "stash"}


def test_comparisons_tell_incomes_and_expenses_apart_like_averages(tmp_path):
    # A car tag: fuel expenses every month, and a large one-off refund in
    # April. Netted by tag over everything, car is an income. Netted among its
    # regular rows (as averages are), it is an expense: what is compared with
    # the average is the regular fuel, not the actual sums less the one-offs.
    months = ["2025-01", "2025-02", "2025-03", "2025-04"]
    rows = [("me", "card", day(month, 3), -100.0, "Fuel") for month in months]
    rows.append(("me", "card", day("2025-04", 9), 1000.0, "Car refund"))
    schema = {name: dtype for name, dtype in RAW_SCHEMA.items() if name != "id"}
    frame = derive_ids(pl.DataFrame(rows, schema=schema, orient="row")).select(
        *RAW_SCHEMA
    )
    with Store(tmp_path.joinpath("finalyze.db")) as store:
        store.add_rule(Filters(raw_description="Fuel"), "car", "fuel")
        store.add_rule(Filters(raw_description="Car refund"), "car", "purchase")
        store.update_subtag("car", "purchase", kind="one-off")
        processed = post_process(frame, store)
    cash_flow = build(SourceData(processed, net_by_tag=True))["cash_flow"]
    assert cash_flow["expenses"][-1] == 0.0
    assert cash_flow["averaged"]["expenses"] == [100.0] * 4
    assert cash_flow["baseline"]["1"]["expenses"][-1] == 100.0


def test_drilling_one_part_of_a_month(frame):
    source = SourceData(frame)
    one_offs = dashboard.Selection(
        flow="expenses", kinds=FLOW_KINDS, month="2025-03", kind="one-off"
    )
    rows = dashboard.selected_rows(source, periods(), one_offs)
    assert rows["amount"].sum() == 2000.0


@pytest.mark.parametrize("net_by_tag", [False, True])
@pytest.mark.parametrize("include_one_offs", [False, True])
def test_kinds_set_on_transactions_count_like_tags_kinds(
    frame, frame_with_kinds_set, net_by_tag, include_one_offs
):
    # Every computation: flows, averages, one-offs, saved, transfers,
    # balances, breakdowns, trends, the bridge and the data checks
    options = dict(net_by_tag=net_by_tag, include_one_offs=include_one_offs)
    by_tags = build(SourceData(frame, **options))
    by_transactions = build(SourceData(frame_with_kinds_set, **options))
    assert by_transactions == by_tags
    kinds_set = frame_with_kinds_set.filter(pl.col("kind_set"))
    assert set(kinds_set["kind"]) == {"transfer", "saving", "one-off", "reconciliation"}
    assert not frame["kind_set"].any()


def test_a_kind_set_on_one_transaction_of_a_subtag(tmp_path):
    # Furniture every month, one of them large and set as a one-off; and a
    # one-off subtag with one transaction set back to regular
    rows = [("me", "card", day(month, 3), -100.0, "Furniture") for month in MONTHS]
    rows.append(("me", "card", day("2025-04", 9), -3000.0, "Sofa"))
    rows.append(("me", "card", day("2025-04", 10), -50.0, "Party snacks"))
    schema = {name: dtype for name, dtype in RAW_SCHEMA.items() if name != "id"}
    data = derive_ids(pl.DataFrame(rows, schema=schema, orient="row")).select(
        *RAW_SCHEMA
    )
    with Store(tmp_path.joinpath("finalyze.db")) as store:
        store.update_subtag("party", "event", kind="one-off")
        annotations = []
        for row in data.iter_rows(named=True):
            label, kind = {
                "Furniture": (("home", "furniture"), None),
                "Sofa": (("home", "furniture"), "one-off"),
                "Party snacks": (("party", "event"), "regular"),
            }[row["raw_description"]]
            part = Part(to_minor_units(row["amount"]), *label, kind=kind)
            annotations.append(Annotation(row["id"], snapshot_of(row), (part,)))
        store.set_annotations(annotations)
        processed = post_process(data, store)
    source = SourceData(processed)
    # In sums and breakdowns, as its own line of the subtag, marked
    expenses = tables.breakdown_total(source, "expenses").sort("kind", "tag")
    assert expenses.select("tag", "subtag", "kind", "amount").rows() == [
        ("home", "furniture", "one-off", 3000.0),
        ("home", "furniture", "regular", 400.0),
        ("party", "event", "regular", 50.0),
    ]
    assert by_month(tables.monthly_flow(source, "expenses"))["2025-04"] == 3150.0
    # Left out of averages and trends, unless included
    assert by_month(tables.rolling_flow(source, "expenses", 1))["2025-04"] == 150.0
    rolling = tables.breakdown_rolling_total(source, "expenses", 1, "2025-04")
    assert sorted(rolling.select("subtag", "kind", "rolling").rows()) == [
        ("event", "regular", 50.0),
        ("furniture", "regular", 100.0),
    ]
    included = SourceData(processed, include_one_offs=True)
    rolling = tables.breakdown_rolling_total(included, "expenses", 1, "2025-04")
    assert sorted(rolling.select("subtag", "kind", "rolling").rows()) == [
        ("event", "regular", 50.0),
        ("furniture", "one-off", 3000.0),
        ("furniture", "regular", 100.0),
    ]
    trends = tables.tag_trends(source, "expenses", 1)
    april = trends.filter(pl.col("month") == "2025-04")
    assert dict(april.select("tag", "amount").rows()) == {"home": 100.0, "party": 50.0}
    # Drilling the one-off part of the month finds it
    one_offs = dashboard.selected_rows(
        source,
        periods(),
        dashboard.Selection(
            flow="expenses", kinds=FLOW_KINDS, month="2025-04", kind="one-off"
        ),
    )
    assert one_offs["raw_description"].to_list() == ["Sofa"]
    assert one_offs["kind_set"].to_list() == [True]
