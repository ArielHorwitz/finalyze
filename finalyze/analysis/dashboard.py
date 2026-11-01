"""The interactive dashboard's data: the analysis tables over a period of months.

Everything is computed by the table functions of `finalyze.analysis.tables`,
over the whole data, and then trimmed to the period. So balances and rolling
means are the same as without a period (a rolling mean at the period's start
includes the months before it), while totals only sum the period's rows.

Monthly figures cover the period's months, which may leave out the current
(incomplete) month. Balances are daily, and run to the latest data unless the
period ends earlier (see `Periods`).

The results are JSON-ready dicts, in the shape of the dashboard models of
`finalyze.service.models`.
"""

import dataclasses
import datetime
import re
from collections.abc import Sequence
from typing import Any, Literal, Optional

import polars as pl

from finalyze.analysis import tables
from finalyze.analysis.tables import DIRECTIONS, SAVING_KINDS, Direction, Flow
from finalyze.money import CURRENCY_DECIMALS
from finalyze.source.data import (
    FLOW_KINDS,
    SORT_DESCENDING,
    SORT_ORDER,
    SourceData,
    current_month,
)
from finalyze.store import TagKind

PeriodPreset = Literal["all", "ytd", "3m", "6m", "12m", "24m"]
MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
TOP_TAGS = 5
BALANCE_ROW_COLUMNS = (
    "id",
    "date",
    "amount",
    "raw_description",
    "description",
    "transaction_description",
    "tags",
    "account_source",
)


# Months, as "YYYY-MM" strings


def month_index(month: str) -> int:
    year, month_number = month.split("-")
    return int(year) * 12 + int(month_number) - 1


def month_of_index(index: int) -> str:
    return f"{index // 12:04}-{index % 12 + 1:02}"


def add_months(month: str, count: int) -> str:
    return month_of_index(month_index(month) + count)


def months_between(start: str, end: str) -> list[str]:
    """Every month from start to end, inclusive."""
    return [
        month_of_index(index)
        for index in range(month_index(start), month_index(end) + 1)
    ]


def month_start(month: str) -> datetime.date:
    year, month_number = month.split("-")
    return datetime.date(int(year), int(month_number), 1)


@dataclasses.dataclass(frozen=True)
class Period:
    """Months from start to end (inclusive). None is unbounded."""

    start: Optional[str] = None
    end: Optional[str] = None

    @property
    def start_date(self) -> Optional[datetime.date]:
        """Inclusive."""
        return None if self.start is None else month_start(self.start)

    @property
    def end_date(self) -> Optional[datetime.date]:
        """Exclusive."""
        return None if self.end is None else month_start(add_months(self.end, 1))

    def dates_expr(self, column: str = "date") -> pl.Expr:
        predicate = pl.lit(True)
        if self.start_date is not None:
            predicate = predicate & (pl.col(column) >= self.start_date)
        if self.end_date is not None:
            predicate = predicate & (pl.col(column) < self.end_date)
        return predicate

    def contains_month(self, month: str) -> bool:
        return (self.start is None or month >= self.start) and (
            self.end is None or month <= self.end
        )


def resolve_period(
    preset: PeriodPreset,
    data_months: Sequence[str],
    *,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
) -> Period:
    """A preset relative to the last month of the data, e.g. "3m" for the last
    three months (every preset ends with it). Explicit months override the
    preset's."""
    last = max(data_months) if data_months else None
    start = end = None
    if last is not None:
        end = last
        if preset == "ytd":
            start = f"{last[:4]}-01"
        elif preset != "all":
            start = add_months(last, 1 - int(preset.removesuffix("m")))
    if start_month is not None:
        start = start_month
    if end_month is not None:
        end = end_month
    return Period(start, end)


# Building the dashboard


def data_months(source: SourceData) -> list[str]:
    """Months that have transactions (not only sentinel ticks)."""
    frame = source.frame.filter(~pl.col("is_sentinel_tick"))
    return frame["month"].unique().sort().to_list()


def period_months(period: Period, months: Sequence[str]) -> list[str]:
    """Every month of the period, bounded by the data's months if unbounded."""
    if not months:
        return []
    start = period.start or min(months)
    end = period.end or max(months)
    if start > end:
        return []
    return months_between(start, end)


@dataclasses.dataclass(frozen=True)
class Periods:
    """What the dashboard shows: monthly figures (cash flows, breakdowns,
    trends, averages) over the months of `months`, and balances (and the rows
    of every group) over the days of `days`, which run to the latest data
    unless the months end earlier than the months offered."""

    months: Period
    days: Period
    available_months: list[str]
    """Months offered for monthly figures (without the current month, if
    left out)."""


def resolve_periods(
    source: SourceData,
    preset: PeriodPreset,
    *,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
    complete_months_only: bool = False,
) -> Periods:
    """The periods of a preset (see `resolve_period`). With
    `complete_months_only`, monthly figures leave out the current (incomplete)
    calendar month, unless it is chosen explicitly."""
    available = data_months(source)
    if complete_months_only:
        current = current_month()
        available = [month for month in available if month < current]
    months = resolve_period(
        preset, available, start_month=start_month, end_month=end_month
    )
    if not available and months == Period():
        # Every month left out (e.g. data of the current month only): no
        # month, rather than an unbounded period
        current = current_month()
        months = Period(current, add_months(current, -1))
    last = available[-1] if available else None
    to_latest = months.end is None or last is None or months.end >= last
    days = Period(months.start, None if to_latest else months.end)
    return Periods(months, days, available)


def build(
    source: SourceData,
    periods: Periods,
    group_order: Sequence[str] = (),
    subtag_trends: bool = False,
) -> dict[str, Any]:
    """Every section of the dashboard (see `finalyze.service.models.Dashboard`).
    Groups' balances are in `group_order` (then by name). Trends by subtag are
    only computed with `subtag_trends`."""
    period = periods.months
    months = period_months(period, periods.available_months)
    # Incomes and expenses are told apart over all the data (which matters when
    # netting by tag), then the period's rows are kept
    in_period = source.restricted(period.dates_expr())
    balance = _balance(source, periods.days, group_order)
    cash_flow = _cash_flow(source, months)
    trends = _trends(source, months, subtag_trends)
    return {
        "available_months": periods.available_months,
        "period": {"start": period.start, "end": period.end, "months": months},
        "rolling_windows": list(tables.ROLLING_WINDOWS),
        "headline": _headline(source, balance, cash_flow, trends, months),
        "balance": balance,
        "cash_flow": cash_flow,
        "breakdown": _breakdown(source, in_period, months),
        "trends": trends,
        "bridge": _bridge(source, periods, months),
        "health": _health(source, periods, cash_flow),
    }


def _round(value: Optional[float]) -> Optional[float]:
    return None if value is None else round(value, CURRENCY_DECIMALS)


def _rounded(frame: pl.DataFrame, *columns: str) -> pl.DataFrame:
    return frame.with_columns(
        pl.col(column).round(CURRENCY_DECIMALS) for column in columns
    )


def _ratio(part: Optional[float], whole: Optional[float]) -> Optional[float]:
    """E.g. the saved rate (saved / incomes), null without a positive whole."""
    if part is None or whole is None or whole <= 0:
        return None
    return round(part / whole, 4)


def _ordered_groups(frame: pl.DataFrame, group_order: Sequence[str]) -> list[str]:
    """The groups of the data, in the given order, then by name."""
    groups = set(frame["group"].unique().to_list())
    ordered = [group for group in group_order if group in groups]
    return ordered + sorted(groups - set(ordered))


@dataclasses.dataclass(frozen=True)
class _Line:
    """A balance line: the net worth, the available balance, a group's, or an
    account and source's."""

    key: str
    kind: str
    label: str
    predicate: pl.Expr
    column: str
    account: Optional[str] = None
    group: Optional[str] = None


def _total_lines(groups: Sequence[str]) -> list[_Line]:
    return [
        _Line("net_worth", "net_worth", "net worth", pl.lit(True), "balance_total"),
        _Line(
            "available",
            "available",
            "available",
            pl.col("group_role") == "spending",
            "balance_role",
        ),
        *(
            _Line(
                f"group:{group}",
                "group",
                group,
                pl.col("group") == group,
                "balance_group",
                group=group,
            )
            for group in groups
        ),
    ]


def _balance(
    source: SourceData, period: Period, group_order: Sequence[str]
) -> dict[str, Any]:
    # In the order balances accumulate (see `SORT_ORDER`)
    frame = source.get(all_groups=True)
    rows = frame.filter(period.dates_expr()).with_row_index("row")
    until_end = frame
    if period.end_date is not None:
        until_end = frame.filter(pl.col("date") < period.end_date)
    as_of = until_end["date"].max() if until_end.height else None
    # (account, source) to their label and group, e.g. "alice/card"
    labels = {}
    for account, source_name, label, group in (
        frame.select("account", "source", "account_source", "group")
        .unique()
        .iter_rows()
    ):
        labels[(account, source_name)] = (label, group)
    total_lines = _total_lines(_ordered_groups(frame, group_order))
    lines = [
        *total_lines,
        *(
            _Line(
                label,
                "account_source",
                label,
                pl.col("account_source") == label,
                "balance_source",
                account=account,
                group=group,
            )
            for (account, _source_name), (label, group) in sorted(labels.items())
        ),
    ]
    series = []
    for line in lines:
        # One point a day: the balance at its end, with the day's rows
        days = (
            rows.filter(line.predicate)
            .group_by("date", maintain_order=True)
            .agg(pl.col(line.column).last().alias("value"), pl.col("row"))
        )
        dates = days["date"].to_list()
        values = days["value"].round(CURRENCY_DECIMALS).to_list()
        day_rows: list[list[int]] = days["row"].to_list()
        # Carry the balance before the period into its start, and the last
        # balance to its end, so every line spans the period.
        if period.start_date is not None:
            before = frame.filter(line.predicate & (pl.col("date") < period.start_date))
            if before.height and (not dates or dates[0] > period.start_date):
                dates.insert(0, period.start_date)
                values.insert(0, _round(before[line.column][-1]))
                day_rows.insert(0, [])
        if dates and as_of is not None and dates[-1] < as_of:
            dates.append(as_of)
            values.append(values[-1])
            day_rows.append([])
        if not dates:
            continue
        series.append(
            {
                "key": line.key,
                "label": line.label,
                "kind": line.kind,
                "account": line.account,
                "group": line.group,
                "dates": dates,
                "values": values,
                "rows": day_rows,
            }
        )
    end_balances = _account_balances(source, period.end_date)
    start_balances = (
        {}
        if period.start_date is None
        else _account_balances(source, period.start_date)
    )

    def change(end: float, start: float) -> float:
        # Without a start, since the first data (from nothing)
        return _round(end - start) or 0.0

    accounts = []
    for (account, source_name), balance in end_balances.items():
        start_balance = start_balances.get((account, source_name))
        accounts.append(
            {
                "account": account,
                "source": source_name,
                "account_source": labels[(account, source_name)][0],
                "group": balance.group,
                "balance": _round(balance.balance),
                "change": change(
                    balance.balance, start_balance.balance if start_balance else 0.0
                ),
            }
        )
    end_totals = _totals(end_balances)
    start_totals = _totals(start_balances)
    one_offs = rows.filter(pl.col("kind") == "one-off")
    totals = [
        {
            "key": line.key,
            "label": line.label,
            "kind": line.kind,
            "group": line.group,
            "balance": _round(end_totals.get(line.key, 0.0)),
            "change": change(
                end_totals.get(line.key, 0.0), start_totals.get(line.key, 0.0)
            ),
            "one_offs": _round(one_offs.filter(line.predicate)["amount"].sum()),
        }
        for line in total_lines
    ]
    return {
        "as_of": as_of,
        "rows": {
            column: _rounded(rows, "amount")[column].to_list()
            for column in BALANCE_ROW_COLUMNS
        },
        "series": series,
        "totals": totals,
        "accounts": accounts,
    }


@dataclasses.dataclass(frozen=True)
class _AccountBalance:
    balance: float
    group: str
    group_role: str


def _account_balances(
    source: SourceData, before: Optional[datetime.date]
) -> dict[tuple[str, str], _AccountBalance]:
    """Balances of accounts and sources before a date, largest first."""
    if before is None:
        subset = source
    else:
        subset = source.with_frame(source.frame.filter(pl.col("date") < before))
    if subset.frame.height == 0:
        return {}
    balances = tables.account_balances(subset)
    return {
        (row["account"], row["source"]): _AccountBalance(
            row["balance"], row["group"], row["group_role"]
        )
        for row in balances.iter_rows(named=True)
    }


def _totals(balances: dict[tuple[str, str], _AccountBalance]) -> dict[str, float]:
    """The net worth, the available balance and each group's, by line key."""
    totals = {"net_worth": 0.0, "available": 0.0}
    for balance in balances.values():
        keys = ["net_worth", f"group:{balance.group}"]
        if balance.group_role == "spending":
            keys.append("available")
        for key in keys:
            totals[key] = totals.get(key, 0.0) + balance.balance
    return totals


def _by_month(frame: pl.DataFrame, column: str = "amount") -> dict[str, float]:
    return dict(zip(frame["month"], frame[column], strict=True))


def _baselines(means: dict[str, float], months: list[str]) -> list[Optional[float]]:
    """For each month, the rolling mean of the month before: what the month is
    compared with."""
    return [_round(means.get(add_months(month, -1))) for month in months]


def _rates(flows: dict[str, Any]) -> dict[str, Any]:
    """The kept rate (net / incomes) and saved rate (saved / incomes) of flows,
    each a number or a list of numbers by month."""
    if isinstance(flows["incomes"], list):
        return {
            "kept_rate": list(map(_ratio, flows["net"], flows["incomes"])),
            "saved_rate": list(map(_ratio, flows["saved"], flows["incomes"])),
        }
    return {
        "kept_rate": _ratio(flows["net"], flows["incomes"]),
        "saved_rate": _ratio(flows["saved"], flows["incomes"]),
    }


def _cash_flow(source: SourceData, months: list[str]):
    flows: dict[str, Any] = {"months": months}
    for flow in tables.FLOWS:
        sums = _by_month(tables.monthly_flow(source, flow))
        flows[flow] = [_round(sums.get(month, 0.0)) for month in months]
    flows |= _rates(flows)
    # What is compared with the rolling means: the same kinds, told apart the
    # same way (which matters when netting by tag)
    averaged = {
        flow: tables.rolling_flow_of_windows(source, flow) for flow in tables.FLOWS
    }
    flows["averaged"] = {}
    for flow in tables.FLOWS:
        sums = _by_month(averaged[flow])
        flows["averaged"][flow] = [_round(sums.get(month, 0.0)) for month in months]
    flows["one_offs"] = {}
    for direction in DIRECTIONS:
        one_offs = _by_month(
            tables.flow_rows(source, direction)
            .filter(pl.col("kind") == "one-off")
            .group_by("month")
            .agg(pl.col("amount").sum())
        )
        flows["one_offs"][direction] = [
            _round(one_offs.get(month, 0.0)) for month in months
        ]
    flows["rolling"] = {}
    flows["baseline"] = {}
    for window in tables.ROLLING_WINDOWS:
        rolling = flows["rolling"][str(window)] = {}
        baseline = flows["baseline"][str(window)] = {}
        for flow in tables.FLOWS:
            means = _by_month(averaged[flow], tables.rolling_column(window))
            rolling[flow] = [_round(means.get(month)) for month in months]
            baseline[flow] = _baselines(means, months)
    return flows


def _breakdown(source: SourceData, in_period: SourceData, months: list[str]):
    totals = {}
    for direction in DIRECTIONS:
        data = tables.breakdown_total(in_period, direction)
        totals[direction] = (
            _rounded(
                data.sort("amount", "tags", "kind", descending=[True, False, False]),
                "amount",
                "percent",
            )
            .select("tag", "subtag", "kind", "amount", "percent")
            .to_dicts()
        )
    rolling: dict[str, dict[str, list]] = {
        str(window): {} for window in tables.ROLLING_WINDOWS
    }
    for direction in DIRECTIONS:
        by_window = (
            tables.breakdown_rolling_total_of_windows(
                source, direction, tables.ROLLING_WINDOWS, months[-1]
            )
            if months
            else {}
        )
        for window in tables.ROLLING_WINDOWS:
            data = by_window.get(window, pl.DataFrame())
            rolling[str(window)][direction] = (
                _rounded(
                    data.sort(
                        "rolling",
                        "tag",
                        "subtag",
                        "kind",
                        descending=[True, False, False, False],
                    ),
                    "amount",
                    "rolling",
                    "percent",
                )
                .select(
                    "month", "tag", "subtag", "kind", "amount", "rolling", "percent"
                )
                .to_dicts()
                if data.height
                else []
            )
    monthly: dict[str, Any] = {"months": months}
    for direction in DIRECTIONS:
        monthly[direction] = (
            _rounded(tables.breakdown_months(in_period, direction), "amount")
            .sort("month", "tags", "kind", nulls_last=True)
            .select("month", "tag", "subtag", "kind", "amount")
            .to_dicts()
        )
    return {"totals": totals, "rolling": rolling, "monthly": monthly}


def _trends(source: SourceData, months: list[str], by_subtag: bool):
    trends: dict[str, Any] = {"tag_order": tables.tag_order(source).to_list()}
    for direction in DIRECTIONS:
        trends[direction] = _trend_series(source, direction, months)
    trends["subtags"] = (
        {
            direction: _trend_series(source, direction, months, by_subtag=True)
            for direction in DIRECTIONS
        }
        if by_subtag
        else None
    )
    return trends


def _trend_series(
    source: SourceData,
    direction: Direction,
    months: list[str],
    by_subtag: bool = False,
) -> list[dict[str, Any]]:
    """Trends by tag (or by tag and subtag) in the period's months, largest
    total first, without those of only zeros."""
    keys = ("tag", "subtag") if by_subtag else ("tag",)
    windows = tables.ROLLING_WINDOWS
    all_months = tables.tag_trends_of_windows(source, direction, windows, by_subtag)
    # Of each key (in the order of the table), by month: the sum, then the
    # rolling means of every window
    by_key: dict[tuple, dict[str, tuple]] = {}
    for row in all_months.select(
        *keys, "month", "amount", *map(tables.rolling_column, windows)
    ).iter_rows():
        key, month, values = row[: len(keys)], row[len(keys)], row[len(keys) + 1 :]
        by_key.setdefault(key, {})[month] = values
    previous_months = [add_months(month, -1) for month in months]
    no_values = (0.0, *(None for _window in windows))
    series = []
    for key, by_month in by_key.items():
        in_months = [by_month.get(month, no_values) for month in months]
        before_months = [by_month.get(month, no_values) for month in previous_months]
        series.append(
            {
                **dict(zip(keys, key, strict=True)),
                "amounts": [_round(values[0]) for values in in_months],
                "rolling": {
                    str(window): [_round(values[index]) for values in in_months]
                    for index, window in enumerate(windows, start=1)
                },
                "baseline": {
                    str(window): [_round(values[index]) for values in before_months]
                    for index, window in enumerate(windows, start=1)
                },
            }
        )
    trend_series = [
        entry
        for entry in series
        if any(entry["amounts"])
        or any(any(means) for means in entry["rolling"].values())
        or any(any(means) for means in entry["baseline"].values())
    ]
    trend_series.sort(
        key=lambda entry: (
            -sum(entry["amounts"]),
            entry["tag"] or "",
            entry.get("subtag") or "",
        )
    )
    return trend_series


def _month_flows(
    flows: dict[str, Optional[float]], one_offs: dict[str, Optional[float]]
) -> dict[str, Any]:
    return {
        **flows,
        **_rates(flows),
        "one_off_incomes": one_offs["incomes"],
        "one_off_expenses": one_offs["expenses"],
    }


def _headline(source: SourceData, balance, cash_flow, trends, months: list[str]):
    """Headline numbers: balances (as of the latest data, unless the period
    ends earlier), the last month of the period (with the month before), the
    period's sums, monthly averages over the period (without one-offs, unless
    they are included), and the largest expenses of the last month."""
    totals = {total["key"]: total["balance"] for total in balance["totals"]}
    headline: dict[str, Any] = {
        "as_of": balance["as_of"],
        "month": None,
        "previous_month": None,
        "net_worth": totals.get("net_worth"),
        "available": totals.get("available"),
        "runway": {},
        "last": None,
        "previous": None,
        "totals": None,
        "averages": None,
        "top_expenses": [],
    }
    if not months:
        return headline
    last_index = len(months) - 1
    headline["month"] = months[last_index]
    headline["previous_month"] = months[last_index - 1] if last_index > 0 else None

    def month_flows(index: int) -> Optional[dict[str, Any]]:
        if index < 0:
            return None
        return _month_flows(
            {flow: cash_flow[flow][index] for flow in tables.FLOWS},
            {
                direction: cash_flow["one_offs"][direction][index]
                for direction in DIRECTIONS
            },
        )

    headline["last"] = month_flows(last_index)
    headline["previous"] = month_flows(last_index - 1)
    headline["totals"] = _month_flows(
        {flow: _round(sum(cash_flow[flow])) for flow in tables.FLOWS},
        {
            direction: _round(sum(cash_flow["one_offs"][direction]))
            for direction in DIRECTIONS
        },
    )
    sums = {
        flow: sum(value for value in cash_flow["averaged"][flow])
        for flow in tables.FLOWS
    }
    one_off_sums = {
        direction: (
            sum(cash_flow["one_offs"][direction]) if source.include_one_offs else 0.0
        )
        for direction in DIRECTIONS
    }
    headline["averages"] = _month_flows(
        {flow: _round(total / len(months)) for flow, total in sums.items()},
        {
            direction: _round(total / len(months))
            for direction, total in one_off_sums.items()
        },
    )
    # Rates of the sums, not of the rounded averages
    headline["averages"] |= _rates(sums)
    headline["runway"] = _runway(source, headline["available"], months[last_index])
    headline["top_expenses"] = _top_expenses(source, trends, months)
    return headline


def _runway(
    source: SourceData, available: Optional[float], last_month: str
) -> dict[str, dict[str, Optional[float]]]:
    """By rolling window: the typical monthly expenses (the rolling mean
    through the last month, without one-offs even if they are included), and
    how many months of them the available balance covers."""
    regular = source.with_options(include_one_offs=False)
    expenses_means = tables.rolling_flow_of_windows(regular, "expenses")
    runway = {}
    for window in tables.ROLLING_WINDOWS:
        means = _by_month(expenses_means, tables.rolling_column(window))
        expenses = _round(means.get(last_month))
        months = None
        if available is not None and expenses is not None and expenses > 0:
            months = round(available / expenses, 2)
        runway[str(window)] = {"expenses": expenses, "months": months}
    return runway


def _top_expenses(source: SourceData, trends, months: list[str]) -> list[dict]:
    """The largest expenses by tag in the last month (one-offs too), with the
    month before, the monthly average over the period and the rolling means
    before the month (of the averaged kinds)."""
    last_month = months[-1]
    previous_month = months[-2] if len(months) > 1 else None
    expenses = tables.flow_rows(source, "expenses").filter(
        pl.col("month").is_in([last_month, previous_month])
    )
    by_tag: dict[Optional[str], dict[str, float]] = {}
    for row in (
        expenses.group_by("tag", "month")
        .agg(
            pl.col("amount").sum(),
            pl.col("amount").filter(pl.col("kind") == "one-off").sum().alias("one_off"),
        )
        .iter_rows(named=True)
    ):
        sums = by_tag.setdefault(row["tag"], {})
        sums[row["month"]] = row["amount"]
        if row["month"] == last_month:
            sums["one_off"] = row["one_off"]
    trend_by_tag = {entry["tag"]: entry for entry in trends["expenses"]}
    top = []
    for tag, sums in by_tag.items():
        amount = _round(sums.get(last_month, 0.0))
        if not amount:
            continue
        trend = trend_by_tag.get(tag)
        top.append(
            {
                "tag": tag,
                "amount": amount,
                "one_off": _round(sums.get("one_off", 0.0)),
                "previous": None
                if previous_month is None
                else _round(sums.get(previous_month, 0.0)),
                "average": None
                if trend is None
                else _round(sum(trend["amounts"]) / len(months)),
                "averaged": None if trend is None else trend["amounts"][-1],
                "baseline": {}
                if trend is None
                else {
                    window: values[-1] for window, values in trend["baseline"].items()
                },
            }
        )
    top.sort(key=lambda item: (-item["amount"], item["tag"] or ""))
    return top[:TOP_TAGS]


BridgeLine = Literal["available", "net_worth"]
BRIDGE_LINES: tuple[BridgeLine, ...] = ("available", "net_worth")


def line_source(source: SourceData, line: BridgeLine) -> tuple[SourceData, bool]:
    """The data of a bridge's line, and whether it has every group: incomes
    and expenses are told apart among the line's accounts (the spending groups
    for the available balance, as cash flows by default), and every tag
    counts."""
    return source.with_options(groups=None, tags=None), line == "net_worth"


def _bridge(
    source: SourceData, periods: Periods, months: list[str]
) -> list[dict[str, Any]]:
    """How the available balance and the net worth went from the period's
    start to the end of its last month: opening + incomes - expenses - saved
    + transfers + reconciliations = closing (with every row of the line in one
    term). Then
    what changed after the months, up to the balances' date."""
    if not months:
        return []
    period = Period(months[0], months[-1])
    return [_bridge_line(source, line, period, periods.days) for line in BRIDGE_LINES]


def _bridge_line(
    source: SourceData, line: BridgeLine, period: Period, days: Period
) -> dict[str, Any]:
    data, all_groups = line_source(source, line)
    frame = data.get(all_groups=all_groups)
    in_period = data.restricted(period.dates_expr())
    start_date, end_date = period.start_date, period.end_date
    assert start_date is not None and end_date is not None

    def total(flow: Optional[Flow], kinds: frozenset[TagKind] = FLOW_KINDS) -> float:
        rows = tables.flow_rows(in_period, flow, kinds, all_groups=all_groups)
        return rows["amount"].sum()

    terms = {
        "incomes": _round(total("incomes")),
        "expenses": _round(total("expenses")),
        # Minus the saving rows: on the spending side for the available
        # balance (as cash flows), of every group for the net worth (where
        # both sides of a saving cancel out)
        "saved": _round(
            total("saved") if line == "available" else -total(None, SAVING_KINDS)
        )
        or 0.0,
        "transfers": _round(total(None, TRANSFER_KINDS)),
        "reconciliations": _round(total(None, RECONCILIATION_KINDS)),
    }
    opening = _round(frame.filter(pl.col("date") < start_date)["amount"].sum())
    closing = _round(frame.filter(pl.col("date") < end_date)["amount"].sum())
    assert opening is not None and closing is not None
    sum_of_terms = (
        opening
        + (terms["incomes"] or 0.0)
        - (terms["expenses"] or 0.0)
        - (terms["saved"] or 0.0)
        + (terms["transfers"] or 0.0)
        + (terms["reconciliations"] or 0.0)
    )
    later = frame.filter(days.dates_expr() & (pl.col("date") >= end_date))
    return {
        "line": line,
        "start": start_date,
        "end": end_date - datetime.timedelta(days=1),
        "opening": opening,
        **terms,
        "closing": closing,
        "rounding": _round(closing - sum_of_terms) or 0.0,
        "one_offs": _round(total(None, ONE_OFF_KINDS)),
        "later": _round(later["amount"].sum()) if later.height else None,
        "as_of": later["date"].max() if later.height else None,
    }


TRANSFER_KINDS: frozenset[TagKind] = frozenset({"transfer"})
RECONCILIATION_KINDS: frozenset[TagKind] = frozenset({"reconciliation"})
ONE_OFF_KINDS: frozenset[TagKind] = frozenset({"one-off"})
BEHIND_DAYS = 31
"""Accounts and sources usually active (in at least half of the period's
months) whose latest row is more days than this before the latest data's are
flagged: their raw files may be missing."""
INCOME_OUTLIER = 0.4
"""Months whose incomes are this far (as a share) from the period's median
are flagged."""


def _health(
    source: SourceData, periods: Periods, cash_flow: dict[str, Any]
) -> dict[str, Any]:
    """Checks of the data behind the numbers: how recent each account and
    source is, whether anything is tagged as saving, tags whose sign in the
    period is the opposite of theirs over all the data, transfers that don't
    cancel out, and months whose incomes are far from the usual."""
    every_row = source.get(all_groups=True)
    latest = every_row["date"].max() if every_row.height else None
    months = cash_flow["months"]
    in_months = source.restricted(periods.months.dates_expr())
    active = dict(
        in_months.get(all_groups=True)
        .group_by("account_source")
        .agg(pl.col("month").n_unique())
        .iter_rows()
    )
    last_dates = []
    for account_source, account, source_name, last in (
        every_row.group_by("account_source", "account", "source")
        .agg(pl.col("date").max())
        .sort("date", "account_source")
        .iter_rows()
    ):
        days_behind = (latest - last).days
        active_months = active.get(account_source, 0)
        last_dates.append(
            {
                "account_source": account_source,
                "account": account,
                "source": source_name,
                "last_date": last,
                "days_behind": days_behind,
                "active_months": active_months,
                "behind": days_behind > BEHIND_DAYS
                and active_months * 2 >= len(months) > 0,
            }
        )
    # Whatever the `tags` option: these check the data, not what is shown
    every_tag = in_months.with_options(tags=None)
    saving_rows = tables.flow_rows(every_tag, "saved").height if months else 0
    # Signs of tags over all the data and in the period
    flows = source.get(kinds=FLOW_KINDS)
    overall = flows.group_by("tag").agg(pl.col("amount").sum().alias("overall"))
    in_period = (
        in_months.get(kinds=FLOW_KINDS)
        .group_by("tag")
        .agg(pl.col("amount").sum().alias("amount"))
    )
    sign_flips = [
        {
            "tag": tag,
            "direction": "incomes" if overall_sum > 0 else "expenses",
            "amount": _round(amount),
        }
        for tag, amount, overall_sum in in_period.join(
            overall, on="tag", how="left", nulls_equal=True
        )
        .filter(
            pl.col("tag").is_not_null()
            & (pl.col("amount").round(0) != 0)
            & ((pl.col("amount") > 0) != (pl.col("overall") > 0))
        )
        .sort("tag")
        .select("tag", "amount", "overall")
        .iter_rows()
    ]
    transfers = every_tag.get(kinds=TRANSFER_KINDS, all_groups=True)
    incomes = [value for value in cash_flow["averaged"]["incomes"] if value is not None]
    usual = _median(incomes)
    income_months = []
    if usual is not None and usual > 0:
        for month, value in zip(months, cash_flow["averaged"]["incomes"], strict=True):
            if value is not None and abs(value - usual) > INCOME_OUTLIER * usual:
                income_months.append(
                    {"month": month, "incomes": value, "usual": _round(usual)}
                )
    return {
        "latest": latest,
        "last_dates": last_dates,
        "saving_rows": saving_rows,
        "sign_flips": sign_flips,
        "transfer_residual": _round(transfers["amount"].sum()),
        "transfer_rows": transfers.height,
        "income_months": income_months,
    }


def _median(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


@dataclasses.dataclass(frozen=True)
class Selection:
    """Rows behind a number of the dashboard, e.g. a tag's expenses in a month.

    `flow` (net, incomes, expenses or saved), `kinds` (of tags, all if None)
    and `all_groups` select the subset as the table of the number did, or
    `line` as a term of a bridge's line (over the period's months, or after
    them with `after_months`). The rest filter its rows."""

    flow: Optional[Flow] = None
    kinds: Optional[frozenset[TagKind]] = None
    all_groups: bool = False
    untagged: bool = False
    tag: Optional[str] = None
    subtag: Optional[str] = None
    month: Optional[str] = None
    date: Optional[datetime.date] = None
    account_source: Optional[str] = None
    group: Optional[str] = None
    group_role: Optional[str] = None
    kind: Optional[TagKind] = None
    line: Optional[BridgeLine] = None
    after_months: bool = False


def selected_rows(
    source: SourceData, periods: Periods, selection: Selection
) -> pl.DataFrame:
    """The rows of a selection, latest first, in the reverse of the order
    balances accumulate. Rows of every group (as balances) are over the
    balances' days, others (and bridges' terms) over the period's months."""
    all_groups = selection.all_groups
    if selection.line is not None:
        source, all_groups = line_source(source, selection.line)
        rows = periods.months.dates_expr()
        if selection.after_months:
            end_date = periods.months.end_date
            rows = periods.days.dates_expr() & (
                pl.lit(False) if end_date is None else pl.col("date") >= end_date
            )
    else:
        period = periods.days if all_groups else periods.months
        rows = period.dates_expr()
    frame = tables.flow_rows(
        source.restricted(rows), selection.flow, selection.kinds, all_groups=all_groups
    )
    if selection.untagged:
        frame = frame.filter(pl.col("tag").is_null())
    for column in (
        "tag",
        "subtag",
        "month",
        "date",
        "account_source",
        "group",
        "group_role",
        "kind",
    ):
        value = getattr(selection, column)
        if value is not None:
            frame = frame.filter(pl.col(column) == value)
    return frame.sort(
        SORT_ORDER,
        descending=[not descending for descending in SORT_DESCENDING],
        nulls_last=True,
    )
