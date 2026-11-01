"""Tests of the dashboard (service and API), on the demo data.

Every number is also snapshotted by the golden tests, and the semantics of tag
kinds are tested on small frames in `test_kinds`."""

import datetime
import functools
from pathlib import Path
from typing import Optional

import polars as pl
import pytest
from fastapi.testclient import TestClient

from finalyze.analysis import dashboard, tables
from finalyze.api import create_app
from finalyze.config import Filters
from finalyze.service import models
from finalyze.source import data as data_module
from finalyze.source.data import SourceData, post_process
from finalyze.store import Store
from tests.test_api import LOCAL_URL


@pytest.fixture
def client(demo_config):
    return TestClient(create_app(static_dir=None), base_url=LOCAL_URL)


def get_dashboard(client, **options) -> dict:
    response = client.post("/api/dashboard", json=options)
    assert response.status_code == 200, response.text
    return response.json()


def get_rows(client, options: dict, **selection) -> list[dict]:
    response = client.post(
        "/api/dashboard/rows", json={"options": options, "selection": selection}
    )
    assert response.status_code == 200, response.text
    return response.json()["rows"]


FLOWS = ["regular", "one-off"]
"""Kinds of the incomes and expenses of cash flows and breakdowns."""
AVERAGED = ["regular"]
"""Kinds of averages and trends (without one-offs)."""


def total(rows: list[dict]) -> float:
    return round(sum(row["amount"] for row in rows), 2)


@pytest.mark.parametrize(
    ("preset", "start_month", "end_month", "expected"),
    [
        ("all", None, None, (None, "2025-06")),
        ("3m", None, None, ("2025-04", "2025-06")),
        ("12m", None, None, ("2024-07", "2025-06")),
        ("ytd", None, None, ("2025-01", "2025-06")),
        ("3m", "2025-01", None, ("2025-01", "2025-06")),
        ("all", None, "2024-12", (None, "2024-12")),
    ],
)
def test_resolve_period(preset, start_month, end_month, expected):
    data_months = ["2024-06", "2025-01", "2025-06"]
    period = dashboard.resolve_period(
        preset, data_months, start_month=start_month, end_month=end_month
    )
    assert (period.start, period.end) == expected


def test_month_arithmetic():
    assert dashboard.add_months("2024-11", 3) == "2025-02"
    assert dashboard.add_months("2025-01", -1) == "2024-12"
    assert dashboard.months_between("2024-11", "2025-02") == [
        "2024-11",
        "2024-12",
        "2025-01",
        "2025-02",
    ]
    assert dashboard.Period("2024-12", "2024-12").end_date.isoformat() == "2025-01-01"


def test_options_default_to_the_dashboard_defaults(client):
    result = get_dashboard(client)
    options = result["options"]
    # The demo's defaults
    assert options["net_by_tag"] is True
    assert options["breakdown_months"] == 3
    assert options["complete_months_only"] is True
    assert options["rolling_months"] == 6
    assert result["rolling_windows"] == [1, 3, 6, 12]
    assert result["period"]["start"] is None
    assert result["period"]["months"][-1] == result["available_months"][-1]
    # Every month of the period: the monthly view shows the last ones
    assert result["breakdown"]["monthly"]["months"] == result["period"]["months"]

    defaults = {
        "net_by_tag": False,
        "breakdown_months": 2,
        "complete_months_only": False,
        "rolling_months": 3,
    }
    assert client.put("/api/settings/dashboard", json=defaults).json() == defaults
    result = get_dashboard(client)
    assert {name: result["options"][name] for name in defaults} == defaults
    # Given options win over the defaults
    result = get_dashboard(client, net_by_tag=True)
    assert result["options"]["net_by_tag"] is True
    response = client.put("/api/settings/dashboard", json={"rolling_months": 2})
    assert response.status_code == 422


def test_period_trims_but_keeps_balances(client):
    everything = get_dashboard(client)
    recent = get_dashboard(client, period="3m")
    assert recent["period"]["months"] == ["2025-04", "2025-05", "2025-06"]
    assert recent["cash_flow"]["months"] == recent["period"]["months"]
    # Balances at the end are the same, and lines start at the period's start
    assert recent["balance"]["accounts"][0] == {
        **everything["balance"]["accounts"][0],
        "change": recent["balance"]["accounts"][0]["change"],
    }
    total_line = next(s for s in recent["balance"]["series"] if s["key"] == "net_worth")
    assert total_line["dates"][0] == "2025-04-01"
    # Savings has no transaction then: its balance before is carried in
    savings = next(
        s for s in recent["balance"]["series"] if s["key"] == "savings/savings"
    )
    assert savings["dates"][0] == "2025-04-01" and savings["rows"][0] == []
    assert savings["dates"][-1] == total_line["dates"][-1] == "2025-06-15"
    # Monthly sums are the same months' sums, and rolling means look back
    # before the period
    months = everything["cash_flow"]["months"]
    for kind in ("net", "incomes", "expenses"):
        assert recent["cash_flow"][kind] == everything["cash_flow"][kind][-3:]
    first_rolling = recent["cash_flow"]["rolling"]["3"]["net"][0]
    assert (
        first_rolling
        == everything["cash_flow"]["rolling"]["3"]["net"][months.index("2025-04")]
    )
    assert first_rolling is not None
    # Totals only sum the period
    expenses = sum(item["amount"] for item in recent["breakdown"]["totals"]["expenses"])
    assert expenses == pytest.approx(sum(recent["cash_flow"]["expenses"]), abs=0.05)


def test_headline(client):
    result = get_dashboard(client, period="6m")
    headline = result["headline"]
    cash_flow = result["cash_flow"]
    assert headline["month"] == "2025-06"
    assert headline["previous_month"] == "2025-05"
    assert headline["last"]["net"] == cash_flow["net"][-1]
    assert headline["previous"]["expenses"] == cash_flow["expenses"][-2]
    # (without one-offs)
    assert headline["averages"]["expenses"] == pytest.approx(
        sum(cash_flow["averaged"]["expenses"]) / 6, abs=0.01
    )
    top = headline["top_expenses"]
    assert 0 < len(top) <= dashboard.TOP_TAGS
    assert [item["amount"] for item in top] == sorted(
        (item["amount"] for item in top), reverse=True
    )
    totals = {row["key"]: row for row in result["balance"]["totals"]}
    assert headline["available"] == totals["available"]["balance"]
    assert headline["net_worth"] == totals["net_worth"]["balance"]
    assert headline["net_worth"] == pytest.approx(
        totals["group:spending"]["balance"]
        + totals["group:savings"]["balance"]
        + totals["group:debt"]["balance"]
    )
    assert headline["available"] == totals["group:spending"]["balance"]


def test_accounts_option_removes_other_accounts(client):
    result = get_dashboard(client, accounts=["eitan"])
    accounts = {row["account"] for row in result["balance"]["accounts"]}
    assert accounts == {"eitan"}
    keys = {series["key"] for series in result["balance"]["series"]}
    assert keys == {
        "net_worth",
        "available",
        "group:spending",
        "eitan/checking",
        "eitan/card",
    }
    response = client.post("/api/dashboard", json={"accounts": ["nobody"]})
    assert response.status_code == 422


def test_groups_and_tags_options(client):
    spending = get_dashboard(client)
    everything = get_dashboard(client, groups=["spending", "savings"])
    # The savings account adds its interest to the incomes
    assert sum(everything["cash_flow"]["incomes"]) > sum(
        spending["cash_flow"]["incomes"]
    )
    assert everything["balance"] == spending["balance"]
    assert spending["options"]["groups"] is None
    savings = get_dashboard(client, groups=["savings"])
    assert {item["tag"] for item in savings["breakdown"]["totals"]["incomes"]} == {
        "savings"
    }
    assert [group["name"] for group in spending["groups"]] == [
        "spending",
        "savings",
        "debt",
    ]
    assert spending["groups"][1]["accounts"] == ["savings"]
    only_housing = get_dashboard(client, tags=["housing"])
    expense_tags = {
        item["tag"] for item in only_housing["breakdown"]["totals"]["expenses"]
    }
    assert expense_tags == {"housing"}
    assert only_housing["breakdown"]["totals"]["incomes"] == []


def test_rolling_windows(client):
    result = get_dashboard(client, include_one_offs=True)
    cash_flow = result["cash_flow"]
    assert list(cash_flow["rolling"]) == ["1", "3", "6", "12"]
    net = cash_flow["net"]
    # Plain means of the months up to each month, null until there are enough
    assert cash_flow["rolling"]["3"]["net"][:2] == [None, None]
    assert cash_flow["rolling"]["3"]["net"][2] == pytest.approx(
        sum(net[:3]) / 3, abs=0.01
    )
    # The 1-month mean is the month itself, so its baseline is the month before
    assert cash_flow["rolling"]["1"]["net"] == net
    assert cash_flow["baseline"]["1"]["net"] == [None, *net[:-1]]
    assert "rolling" not in result["balance"]


def test_untagged_transactions_are_shown(client):
    result = get_dashboard(client)
    assert None in {item["tag"] for item in result["breakdown"]["totals"]["expenses"]}


def test_rows_add_up_to_the_numbers(client):
    options = {"period": "6m"}
    result = get_dashboard(client, **options)
    months = result["period"]["months"]
    # A month of cash flow
    for kind in ("net", "incomes", "expenses"):
        rows = get_rows(client, options, flow=kind, kinds=FLOWS, month=months[-1])
        assert total(rows) == pytest.approx(result["cash_flow"][kind][-1], abs=0.01)
    # A tag's trend in a month
    for trend in result["trends"]["expenses"]:
        rows = get_rows(
            client,
            options,
            flow="expenses",
            kinds=AVERAGED,
            tag=trend["tag"],
            untagged=trend["tag"] is None,
            month=months[-2],
        )
        assert total(rows) == pytest.approx(trend["amounts"][-2], abs=0.01)
    # A subtag's total over the period
    for item in result["breakdown"]["totals"]["expenses"][:3]:
        rows = get_rows(
            client,
            options,
            flow="expenses",
            kinds=FLOWS,
            tag=item["tag"],
            subtag=item["subtag"],
            untagged=item["tag"] is None,
        )
        assert total(rows) == pytest.approx(item["amount"], abs=0.01)
    # Everything in the period, latest first, of every group
    rows = get_rows(client, options, all_groups=True)
    assert rows[0]["date"] >= rows[-1]["date"]
    assert rows[-1]["date"] >= "2025-01-01"
    assert {row["group_role"] for row in rows} == {"spending", "holding"}
    # A balance line's rows, e.g. the available balance's on a day
    totals = {row["key"]: row for row in result["balance"]["totals"]}
    group_rows = get_rows(client, options, all_groups=True, group="savings")
    assert {row["account"] for row in group_rows} == {"savings"}
    assert total(group_rows) == pytest.approx(
        totals["group:savings"]["change"], abs=0.01
    )
    available = get_rows(client, options, all_groups=True, group_role="spending")
    assert total(available) == pytest.approx(totals["available"]["change"], abs=0.01)


def test_no_ingested_data(isolated_config):
    client = TestClient(create_app(static_dir=None), base_url=LOCAL_URL)
    assert client.post("/api/dashboard", json={}).status_code == 409


def test_subtag_trends_only_when_asked(client):
    assert get_dashboard(client)["trends"]["subtags"] is None
    with_subtags = get_dashboard(client, subtag_trends=True)
    assert with_subtags["options"]["subtag_trends"] is True
    subtags = with_subtags["trends"]["subtags"]
    assert subtags["expenses"]
    assert all("subtag" in trend for trend in subtags["expenses"])
    # The rest is the same either way
    without = get_dashboard(client)
    with_subtags["trends"]["subtags"] = None
    with_subtags["options"]["subtag_trends"] = False
    assert with_subtags == without


def test_invalid_month(client):
    response = client.post("/api/dashboard", json={"start_month": "2025-13"})
    assert response.status_code == 422
    with pytest.raises(ValueError):
        models.DashboardOptions(end_month="June")


def test_rows_add_up_when_netting_by_tag(client):
    # Tags are classified as incomes or expenses over all the data, also when
    # the period (or a drill-down) only covers some months
    options = {"period": "3m", "net_by_tag": True}
    result = get_dashboard(client, **options)
    months = result["period"]["months"]
    for index, month in enumerate(months):
        rows = get_rows(client, options, flow="expenses", kinds=FLOWS, month=month)
        assert total(rows) == pytest.approx(
            result["cash_flow"]["expenses"][index], abs=0.01
        )
    for trend in result["trends"]["incomes"]:
        rows = get_rows(
            client,
            options,
            flow="incomes",
            kinds=AVERAGED,
            month=months[-1],
            tag=trend["tag"],
            untagged=trend["tag"] is None,
        )
        assert total(rows) == pytest.approx(trend["amounts"][-1], abs=0.01)
    # A tag of the monthly breakdown is the sum of all its subtags
    monthly = result["breakdown"]["monthly"]
    month = monthly["months"][-1]
    by_tag: dict = {}
    for item in monthly["expenses"]:
        if item["month"] == month:
            by_tag[item["tag"]] = by_tag.get(item["tag"], 0) + item["amount"]
    for tag, amount in by_tag.items():
        rows = get_rows(
            client,
            options,
            flow="expenses",
            kinds=FLOWS,
            month=month,
            tag=tag,
            untagged=tag is None,
        )
        assert total(rows) == pytest.approx(amount, abs=0.01)


def test_account_labels(client):
    result = get_dashboard(client)
    labels = {series["key"] for series in result["balance"]["series"]}
    for row in result["balance"]["accounts"]:
        assert row["account_source"] in labels
    series = next(s for s in result["balance"]["series"] if s["key"] == "eitan/card")
    assert (series["account"], series["group"]) == ("eitan", "spending")


def test_netting_by_tag_classifies_over_all_the_data():
    # A tag spends 200 in January and is refunded 100 in February: netted over
    # all the data it is an expense, so February's expenses are -100.
    frame = pl.DataFrame(
        {
            "id": ["a", "b"],
            "date": [datetime.date(2025, 1, 10), datetime.date(2025, 2, 10)],
            "month": ["2025-01", "2025-02"],
            "amount": [-200.0, 100.0],
            "tag": ["shop", "shop"],
            "subtag": ["", ""],
            "account": ["me", "me"],
            "source": ["card", "card"],
            "account_source": ["me/card", "me/card"],
            "kind": ["regular", "regular"],
            "group": ["spending", "spending"],
            "group_role": ["spending", "spending"],
            "is_sentinel_tick": [False, False],
        }
    )
    source = SourceData(frame, net_by_tag=True)
    february = dashboard.Period("2025-02", "2025-02")
    flows = tables.monthly_flow(source, "expenses")
    assert flows.filter(pl.col("month") == "2025-02")["amount"].to_list() == [-100.0]
    rows = dashboard.selected_rows(
        source,
        dashboard.Periods(february, february, ["2025-01", "2025-02"]),
        dashboard.Selection(flow="expenses", kinds=frozenset(FLOWS)),
    )
    assert rows["amount"].to_list() == [-100.0]


def sums_by(items: list[dict], key: str, value: str = "amount") -> dict:
    sums: dict = {}
    for item in items:
        sums[item[key]] = sums.get(item[key], 0.0) + item[value]
    return sums


@pytest.mark.parametrize("net_by_tag", [False, True])
@pytest.mark.parametrize("include_one_offs", [False, True])
@pytest.mark.parametrize("period", ["all", "6m"])
def test_views_reconcile(client, net_by_tag, include_one_offs, period):
    """The same period's figures agree across views: cash flow, breakdown
    totals, monthly and rolling breakdowns, trends and the cards. The demo data
    has untagged rows, refunds and one-offs."""
    result = get_dashboard(
        client,
        period=period,
        net_by_tag=net_by_tag,
        include_one_offs=include_one_offs,
        subtag_trends=True,
    )
    months = result["period"]["months"]
    cash_flow = result["cash_flow"]
    breakdown = result["breakdown"]
    headline = result["headline"]
    approx = functools.partial(pytest.approx, abs=0.05)
    for index in range(len(months)):
        # Net is incomes less expenses: no row is on neither side
        assert cash_flow["net"][index] == approx(
            cash_flow["incomes"][index] - cash_flow["expenses"][index]
        )
        assert cash_flow["averaged"]["net"][index] == approx(
            cash_flow["averaged"]["incomes"][index]
            - cash_flow["averaged"]["expenses"][index]
        )
    for direction in ("incomes", "expenses"):
        # Breakdown totals add up to the cash flow over the period
        totals = breakdown["totals"][direction]
        assert sum(item["amount"] for item in totals) == approx(
            sum(cash_flow[direction])
        )
        assert headline["totals"][direction] == approx(sum(cash_flow[direction]))
        if totals:
            assert sum(item["percent"] for item in totals) == approx(100)
        # The monthly breakdown adds up to each month's cash flow
        monthly = sums_by(breakdown["monthly"][direction], "month")
        assert breakdown["monthly"]["months"] == months
        for index, month in enumerate(months):
            assert monthly.get(month, 0.0) == approx(cash_flow[direction][index])
        # Tags' trends add up to the averaged cash flow, and their rolling
        # means to the cash flow's
        trends = result["trends"][direction]
        for index in range(len(months)):
            assert sum(trend["amounts"][index] for trend in trends) == approx(
                cash_flow["averaged"][direction][index]
            )
        for window, means in cash_flow["rolling"].items():
            for index, mean in enumerate(means[direction]):
                tag_means = [trend["rolling"][window][index] for trend in trends]
                if mean is None:
                    assert set(tag_means) <= {None}
                else:
                    assert sum(value or 0.0 for value in tag_means) == approx(mean)
            # The rolling breakdown adds up to the cash flow's rolling mean in
            # the period's last month
            rolling = breakdown["rolling"][window][direction]
            if means[direction][-1] is None:
                assert rolling == []
            else:
                assert sum(item["rolling"] for item in rolling) == approx(
                    means[direction][-1]
                )
        # A tag's subtag trends add up to its trend: sums, means and baselines
        # (each rounded)
        subtags = result["trends"]["subtags"][direction]
        assert {trend["tag"] for trend in trends} <= {part["tag"] for part in subtags}
        for trend in trends:
            parts = [part for part in subtags if part["tag"] == trend["tag"]]
            approx = functools.partial(pytest.approx, abs=0.005 * (len(parts) + 1))
            for index in range(len(months)):
                assert sum(part["amounts"][index] for part in parts) == approx(
                    trend["amounts"][index]
                )
                for values in ("rolling", "baseline"):
                    for window, means in trend[values].items():
                        part_means = [part[values][window][index] for part in parts]
                        if means[index] is None:
                            assert set(part_means) <= {None}
                        else:
                            assert sum(value or 0.0 for value in part_means) == approx(
                                means[index]
                            )
        approx = functools.partial(pytest.approx, abs=0.05)
        # Monthly averages of the cards
        assert headline["averages"][direction] == approx(
            sum(cash_flow["averaged"][direction]) / len(months)
        )
    # The cards' month is the cash flow's last
    for flow in ("net", "incomes", "expenses", "saved"):
        assert headline["last"][flow] == cash_flow[flow][-1]
        assert headline["totals"][flow] == approx(sum(cash_flow[flow]))
    # One saved rate of the period, of the actual sums
    assert headline["totals"]["saved_rate"] == pytest.approx(
        sum(cash_flow["saved"]) / sum(cash_flow["incomes"]), abs=1e-4
    )


def processed(
    directory: Path, rows: list[tuple], kinds: Optional[dict] = None
) -> pl.DataFrame:
    """Post-processed rows of (date, amount, description, tag), with tags of
    regular kind unless given, in one spending account."""
    frame = pl.DataFrame(
        {
            "id": [f"t{index}" for index in range(len(rows))],
            "account": ["me"] * len(rows),
            "source": ["card"] * len(rows),
            "date": [row[0] for row in rows],
            "amount": [row[1] for row in rows],
            "raw_description": [row[2] for row in rows],
        }
    )
    with Store(directory.joinpath("finalyze.db")) as store:
        for _date, _amount, description, tag in rows:
            if tag is not None:
                store.add_rule(Filters(raw_description=description), tag, "")
        for tag, kind in (kinds or {}).items():
            store.update_tag(tag, kind=kind)
        return post_process(frame, store)


def test_rolling_means_count_empty_months_as_zero(tmp_path):
    # B3: a tag with no rows in February counts as 0 there, also when not
    # netting by tag (rows that aren't expenses are left out)
    frame = processed(
        tmp_path,
        [
            (datetime.date(2025, 1, 5), -300.0, "Shop", "shop"),
            (datetime.date(2025, 2, 5), -100.0, "Cafe", "cafe"),
            (datetime.date(2025, 3, 5), -300.0, "Shop", "shop"),
        ],
    )
    for net_by_tag in (False, True):
        source = SourceData(frame, net_by_tag=net_by_tag)
        trends = tables.tag_trends(source, "expenses", 3)
        shop = trends.filter(pl.col("tag") == "shop")
        assert shop["amount"].to_list() == [300.0, 0.0, 300.0]
        assert shop["rolling"].to_list() == [None, None, 200.0]
        rolling = tables.breakdown_rolling_total(source, "expenses", 3, "2025-03")
        assert sums_by(rolling.to_dicts(), "tag", "rolling") == pytest.approx(
            {"shop": 200.0, "cafe": 100 / 3}
        )


def test_every_row_is_an_income_or_an_expense(tmp_path):
    # B4: netting by tag, untagged rows go by their own sign, and a tag whose
    # rows sum to 0 is an expense (with its refund), so net = incomes - expenses
    frame = processed(
        tmp_path,
        [
            (datetime.date(2025, 1, 5), -100.0, "Unknown shop", None),
            (datetime.date(2025, 1, 6), 40.0, "Unknown refund", None),
            (datetime.date(2025, 1, 7), -50.0, "Lent", "loan"),
            (datetime.date(2025, 2, 7), 50.0, "Repaid", "loan"),
        ],
    )
    source = SourceData(frame, net_by_tag=True)
    flows = {
        flow: dict(tables.monthly_flow(source, flow).iter_rows())
        for flow in ("net", "incomes", "expenses")
    }
    assert flows["incomes"] == {"2025-01": 40.0}
    assert flows["expenses"] == {"2025-01": 150.0, "2025-02": -50.0}
    assert flows["net"] == {"2025-01": -110.0, "2025-02": 50.0}


def test_monthly_figures_leave_out_the_current_month(client, monkeypatch):
    # The demo data runs to 2025-06-15: June is the current, incomplete month
    monkeypatch.setattr(data_module, "_today", lambda: datetime.date(2025, 6, 20))
    result = get_dashboard(client, period="3m")
    assert result["period"]["months"] == ["2025-03", "2025-04", "2025-05"]
    assert result["available_months"][-1] == "2025-05"
    assert result["headline"]["month"] == "2025-05"
    # Balances run to the latest data, as of its last day
    assert result["balance"]["as_of"] == result["headline"]["as_of"] == "2025-06-15"
    net_worth = next(s for s in result["balance"]["series"] if s["key"] == "net_worth")
    assert net_worth["dates"][-1] == "2025-06-15"
    every_month = get_dashboard(client, period="3m", complete_months_only=False)
    assert every_month["headline"]["net_worth"] == result["headline"]["net_worth"]
    assert every_month["period"]["months"][-1] == "2025-06"
    # The rows of every group (the source data) too, but not those of flows
    rows = get_rows(client, {"period": "3m"}, all_groups=True)
    assert rows[0]["date"] == "2025-06-15"
    rows = get_rows(client, {"period": "3m"}, flow="expenses", kinds=FLOWS)
    assert rows[0]["date"] < "2025-06-01"
    # An earlier period ends its balances with it
    earlier = get_dashboard(client, start_month="2025-01", end_month="2025-02")
    assert earlier["balance"]["as_of"] <= "2025-02-28"
    # Explicit months may include the current one
    explicit = get_dashboard(client, start_month="2025-05", end_month="2025-06")
    assert explicit["period"]["months"] == ["2025-05", "2025-06"]


def test_balances_are_daily_and_rows_follow_their_order(client):
    result = get_dashboard(client, period="6m")
    balance = result["balance"]
    for series in balance["series"]:
        assert len(series["dates"]) == len(set(series["dates"]))
        assert series["dates"] == sorted(series["dates"])
    # A day's point is the balance at the end of the day, after its rows
    net_worth = next(s for s in balance["series"] if s["key"] == "net_worth")
    rows = balance["rows"]
    # With the descriptions, for the tooltip to show them first
    for column in ("raw_description", "description", "transaction_description"):
        assert len(rows[column]) == len(rows["id"])
    for index in range(1, len(net_worth["dates"])):
        day_rows = net_worth["rows"][index]
        if not day_rows:
            continue
        change = sum(rows["amount"][row] for row in day_rows)
        assert net_worth["values"][index] == pytest.approx(
            net_worth["values"][index - 1] + change, abs=0.01
        )
        assert {rows["date"][row] for row in day_rows} == {net_worth["dates"][index]}
    # The source data's rows are in the reverse of the order balances
    # accumulate: read from the bottom, each row's balance follows the last
    source_rows = get_rows(client, {"period": "6m"}, all_groups=True)
    upward = list(reversed(source_rows))
    for previous, row in zip(upward, upward[1:], strict=False):
        assert row["balance_total"] == pytest.approx(
            previous["balance_total"] + row["amount"], abs=0.01
        )


BRIDGE_TERMS = {
    # A bridge's term: its sign in the bridge, and the selection of its rows
    "incomes": (1, {"flow": "incomes", "kinds": FLOWS}),
    "expenses": (-1, {"flow": "expenses", "kinds": FLOWS}),
    "transfers": (1, {"kinds": ["transfer"]}),
    "reconciliations": (1, {"kinds": ["reconciliation"]}),
}


@pytest.mark.parametrize("net_by_tag", [False, True])
@pytest.mark.parametrize(
    "options",
    [
        {"period": "all"},
        {"period": "6m"},
        {"start_month": "2025-01", "end_month": "2025-02"},
        # Neither the groups nor the tags of flows change a bridge
        {"period": "12m", "groups": ["spending", "savings"], "tags": ["food"]},
    ],
)
def test_bridges_close(client, monkeypatch, net_by_tag, options):
    """Opening + incomes - expenses - saved + transfers + reconciliations =
    closing, to the cent, and each term is the sum of its rows."""
    # June is the current month: left out of the months, not of the balances
    monkeypatch.setattr(data_module, "_today", lambda: datetime.date(2025, 6, 20))
    options = {**options, "net_by_tag": net_by_tag}
    result = get_dashboard(client, **options)
    months = result["period"]["months"]
    bridges = {line["line"]: line for line in result["bridge"]}
    assert list(bridges) == ["available", "net_worth"]
    totals = {total["key"]: total for total in result["balance"]["totals"]}
    for key, line in bridges.items():
        assert line["start"] == f"{months[0]}-01"
        assert line["end"][:7] == months[-1]
        assert line["opening"] + line["incomes"] - line["expenses"] - line[
            "saved"
        ] + line["transfers"] + line["reconciliations"] + line[
            "rounding"
        ] == pytest.approx(line["closing"], abs=0.001)
        assert line["rounding"] == pytest.approx(0, abs=0.02)
        for term, (_sign, selection) in BRIDGE_TERMS.items():
            rows = get_rows(client, options, line=key, **selection)
            assert total(rows) == pytest.approx(line[term], abs=0.01), term
        if key == "available":
            saved_rows = get_rows(client, options, line=key, flow="saved")
        else:
            saved_rows = get_rows(client, options, line=key, kinds=["saving"])
            saved_rows = [{"amount": -row["amount"]} for row in saved_rows]
        assert total(saved_rows) == pytest.approx(line["saved"], abs=0.01)
        # Then the balances' date: the rows after the months
        balance = totals[key]["balance"]
        if months[-1] == "2025-05" and "end_month" not in options:
            assert line["as_of"] == result["balance"]["as_of"] == "2025-06-15"
            later = get_rows(client, options, line=key, after_months=True)
            assert later and all(row["date"] >= "2025-06-01" for row in later)
            assert total(later) == pytest.approx(line["later"], abs=0.01)
            assert line["closing"] + line["later"] == pytest.approx(balance, abs=0.01)
        else:
            assert line["later"] is None and line["as_of"] is None
            assert line["closing"] == pytest.approx(balance, abs=0.01)
    # With the default groups, the available balance's incomes, expenses and
    # saved are the cash flow's
    if "groups" not in options:
        available = bridges["available"]
        for flow in ("incomes", "expenses", "saved"):
            assert available[flow] == pytest.approx(
                sum(result["cash_flow"][flow]), abs=0.05
            )


def test_health(client, monkeypatch):
    monkeypatch.setattr(data_module, "_today", lambda: datetime.date(2025, 6, 20))
    result = get_dashboard(client, period="12m")
    health = result["health"]
    assert health["latest"] == "2025-06-15"
    last_dates = health["last_dates"]
    assert [item["days_behind"] for item in last_dates] == sorted(
        item["days_behind"] for item in last_dates
    )[::-1]
    # The demo data's accounts are all active and recent
    assert not any(item["behind"] for item in last_dates)
    active_months = {
        item["account_source"]: item["active_months"] for item in last_dates
    }
    # The car loan started with the new car
    assert active_months.pop("loan/car") < 12
    assert set(active_months.values()) == {12}
    labels = {item["account_source"] for item in last_dates}
    assert labels == {item["account_source"] for item in result["balance"]["accounts"]}
    # The demo data sets money aside every month
    saved = get_rows(client, {"period": "12m"}, flow="saved")
    assert health["saving_rows"] == len(saved) > 0
    # Both sides of every transfer are recorded: the opening balances and the
    # cash count are reconciliations
    transfers = get_rows(client, {"period": "12m"}, all_groups=True, kinds=["transfer"])
    transfers = [row for row in transfers if row["date"] < "2025-06-01"]
    assert health["transfer_residual"] == total(transfers) == 0
    assert health["transfer_rows"] == len(transfers)
    every_month = get_dashboard(client, period="all")["health"]
    assert every_month["transfer_residual"] == 0
    # The tags shown don't change what the data is
    only_food = get_dashboard(client, period="12m", tags=["food"])["health"]
    assert only_food["transfer_residual"] == health["transfer_residual"]
    assert only_food["saving_rows"] == health["saving_rows"]
    for month in health["income_months"]:
        index = result["period"]["months"].index(month["month"])
        assert month["incomes"] == result["cash_flow"]["averaged"]["incomes"][index]
        assert abs(month["incomes"] - month["usual"]) > 0.4 * month["usual"]


def test_sign_flips_and_no_savings(tmp_path):
    """A tag of expenses refunded more than it spent in a period flips sign,
    and nothing tagged as saving leaves no saving rows."""
    frame = processed(
        tmp_path,
        [
            (datetime.date(2025, 1, 5), 1000.0, "Salary", "salary"),
            (datetime.date(2025, 1, 10), -300.0, "Garage", "car"),
            (datetime.date(2025, 2, 5), 1000.0, "Salary", "salary"),
            (datetime.date(2025, 2, 10), 120.0, "Garage refund", "car"),
        ],
    )
    source = SourceData(frame, net_by_tag=True)
    periods = dashboard.resolve_periods(source, "all", start_month="2025-02")
    health = dashboard.build(source, periods)["health"]
    assert health["sign_flips"] == [
        {"tag": "car", "direction": "expenses", "amount": 120.0}
    ]
    assert health["saving_rows"] == 0


def test_runway(client):
    result = get_dashboard(client, period="12m", include_one_offs=True)
    headline = result["headline"]
    regular = get_dashboard(client, period="12m")
    for window, runway in headline["runway"].items():
        # Without one-offs, even when they are included
        expenses = regular["cash_flow"]["rolling"][window]["expenses"][-1]
        assert runway["expenses"] == expenses
        assert runway["months"] == pytest.approx(
            headline["available"] / expenses, abs=0.01
        )


def test_usually_active_sources_that_stop_are_behind(tmp_path):
    """A source with rows every month that stops is behind, one with rows
    once in a while isn't (it is only quiet)."""
    rows = [
        *(
            ("card", datetime.date(2025, month, 10), -100.0)
            for month in range(1, 5)  # Stops after April
        ),
        *(("checking", datetime.date(2025, month, 10), -50.0) for month in range(1, 8)),
        ("cash", datetime.date(2025, 1, 3), -20.0),  # Once
    ]
    frame = pl.DataFrame(
        {
            "id": [f"t{index}" for index in range(len(rows))],
            "account": ["me"] * len(rows),
            "source": [row[0] for row in rows],
            "date": [row[1] for row in rows],
            "amount": [row[2] for row in rows],
            "raw_description": ["Shop"] * len(rows),
        }
    )
    with Store(tmp_path.joinpath("finalyze.db")) as store:
        frame = post_process(frame, store)
    source = SourceData(frame)
    periods = dashboard.resolve_periods(source, "all")
    last_dates = {
        item["source"]: item
        for item in dashboard.build(source, periods)["health"]["last_dates"]
    }
    assert last_dates["card"]["behind"] and last_dates["card"]["active_months"] == 4
    assert not last_dates["cash"]["behind"]
    assert not last_dates["checking"]["behind"]
    assert last_dates["checking"]["days_behind"] == 0


def get_balance(client, **params) -> dict:
    response = client.get("/api/balance", params=params)
    assert response.status_code == 200, response.text
    return response.json()


def test_balance_on_a_date(client):
    # The wallet was counted on April 28th: re-anchored to what it held
    count = get_rows(
        client, {"period": "all"}, all_groups=True, kinds=["reconciliation"]
    )
    cash_count = next(row for row in count if row["subtag"] == "cash count")
    counted = float(cash_count["transaction_description"].split()[1])
    wallet = {"account": "cash", "source": "wallet"}
    after = get_balance(client, date=cash_count["date"], **wallet)
    assert after == {
        "date": "2025-04-28",
        "label": "cash/wallet",
        "account": "cash",
        "source": "wallet",
        "group": "spending",
        "balance": counted,
        "rows": after["rows"],
        "last_date": "2025-04-28",
    }
    before = get_balance(client, date="2025-04-27", **wallet)
    assert before["balance"] - 137.5 == pytest.approx(counted, abs=0.001)
    assert before["rows"] < after["rows"]
    # By default, as of the latest data: the dashboard's balances
    dashboard_balance = get_dashboard(client, period="all")["balance"]
    totals = {total["key"]: total["balance"] for total in dashboard_balance["totals"]}
    net_worth = get_balance(client)
    assert net_worth["label"] == "net worth"
    assert net_worth["date"] == dashboard_balance["as_of"]
    assert net_worth["balance"] == pytest.approx(totals["net_worth"], abs=0.01)
    savings = get_balance(client, group="savings")
    assert savings["balance"] == pytest.approx(totals["group:savings"], abs=0.01)
    accounts = {
        item["account_source"]: item["balance"]
        for item in dashboard_balance["accounts"]
    }
    maya = get_balance(client, account="maya")
    assert maya["group"] == "spending"
    assert maya["balance"] == pytest.approx(
        sum(value for key, value in accounts.items() if key.startswith("maya/")),
        abs=0.01,
    )
    # Before the data: nothing
    empty = get_balance(client, date="2000-01-01", **wallet)
    assert empty["balance"] == 0 and empty["rows"] == 0 and empty["last_date"] is None


@pytest.mark.parametrize(
    ("params", "status"),
    [
        ({"source": "wallet"}, 422),
        ({"account": "cash", "group": "spending"}, 422),
        ({"date": "June"}, 422),
        ({"account": "nope"}, 404),
        ({"account": "cash", "source": "nope"}, 404),
        ({"group": "nope"}, 404),
    ],
)
def test_invalid_balance_queries(client, params, status):
    assert client.get("/api/balance", params=params).status_code == status


def test_accounts_list_their_sources(client):
    accounts = {item["name"]: item for item in client.get("/api/accounts").json()}
    assert accounts["maya"]["sources"] == ["bit", "card", "checking"]
    assert accounts["cash"]["sources"] == ["wallet"]
