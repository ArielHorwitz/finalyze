"""The analysis tables: frames computed from the source data and options alone,
which the dashboard (`finalyze.analysis.dashboard`) is built from.

Incomes and expenses are told apart one way everywhere (by tag when netting by
tag, see `SourceData`), and rolling means are plain means over the calendar
months of a window, with 0 in months without rows."""

from collections.abc import Sequence
from typing import Literal, Optional

import polars as pl

from finalyze.source.data import FLOW_KINDS, SourceData
from finalyze.store import TagKind

Direction = Literal["incomes", "expenses"]
Flow = Literal["net", "incomes", "expenses", "saved"]
DIRECTIONS: tuple[Direction, ...] = ("incomes", "expenses")
FLOWS: tuple[Flow, ...] = ("net", "incomes", "expenses", "saved")
SAVING_KINDS: frozenset[TagKind] = frozenset({"saving"})
ROLLING_WINDOWS: tuple[int, ...] = (1, 3, 6, 12)
"""Rolling means to choose from, by their number of months. A month compared
with the 1-month mean before it is compared with the month before."""
DEFAULT_ROLLING_WINDOW = 6


def _direction_flags(flow: Flow) -> dict[str, bool]:
    return {"incomes": flow == "incomes", "expenses": flow == "expenses"}


def account_balances(source: SourceData) -> pl.DataFrame:
    """The last balance of every account and source, with its group and the
    group's role, largest first."""
    df = source.get(all_groups=True)
    # In the order balances accumulate, so the last is the end balance
    return (
        df.sort("date", maintain_order=True)
        .group_by("account", "source", maintain_order=True)
        .agg(
            pl.col("balance_source").last().alias("balance"),
            pl.col("group").last(),
            pl.col("group_role").last(),
        )
        .sort("balance", "account", "source", descending=[True, False, False])
        .select("account", "source", "group", "group_role", "balance")
    )


def flow_rows(
    source: SourceData,
    flow: Optional[Flow],
    kinds: Optional[frozenset[TagKind]] = FLOW_KINDS,
    **flags,
) -> pl.DataFrame:
    """Rows of a flow: every row of tags of these kinds (net, or None), incomes,
    or expenses (as positive amounts) among them, or money saved (positive
    when set aside).

    Saved is measured on the spending side (rows of saving tags in accounts of
    groups for spending), so that both sides of a saving don't cancel out when
    holding groups are in the flows too."""
    if flow == "saved":
        df = source.get(kinds=SAVING_KINDS, **flags)
        return df.filter(pl.col("group_role") == "spending").with_columns(
            pl.col("amount") * -1
        )
    return source.get(kinds=kinds, **_direction_flags(flow), **flags)


def monthly_flow(
    source: SourceData, flow: Flow, kinds: frozenset[TagKind] = FLOW_KINDS
) -> pl.DataFrame:
    """Monthly sums of a flow (see `flow_rows`)."""
    return (
        flow_rows(source, flow, kinds)
        .group_by("month")
        .agg(pl.col("amount").sum())
        .sort("month")
    )


def calendar(source: SourceData, last_month: Optional[str] = None) -> pl.DataFrame:
    """Every month of the data (up to `last_month`), as a "month" column."""
    months = source.months
    if last_month is not None:
        months = months.filter(pl.col("month") <= last_month)
    return months


def _monthly_grid(
    sums: pl.DataFrame, keys: Sequence[str], months: pl.DataFrame
) -> pl.DataFrame:
    """Monthly sums by keys, in every month (0 without rows), sorted by keys
    and month. Keys may be null (e.g. untagged)."""
    grid = sums.select(keys).unique().join(months, how="cross")
    return (
        grid.join(sums, on=[*keys, "month"], how="left", nulls_equal=True)
        .with_columns(pl.col("amount").fill_null(0.0))
        .sort(*keys, "month", nulls_last=True)
    )


def _rolling_mean(window: int) -> pl.Expr:
    return pl.col("amount").rolling_mean(window_size=window)


def rolling_column(window: int) -> str:
    """The column of a window's rolling means, in tables of several windows."""
    return f"rolling_{window}"


def _rolling_means(windows: Sequence[int], keys: Sequence[str] = ()) -> list[pl.Expr]:
    """The rolling means of every window (by keys), see `rolling_column`."""
    means = []
    for window in windows:
        mean = _rolling_mean(window)
        if keys:
            mean = mean.over(keys)
        means.append(mean.alias(rolling_column(window)))
    return means


def rolling_flow_of_windows(
    source: SourceData, flow: Flow, windows: Sequence[int] = ROLLING_WINDOWS
) -> pl.DataFrame:
    """`monthly_flow` of the averaged kinds over every month of the data (0 in
    months without rows, e.g. without savings), and its rolling means of every
    window (see `rolling_column`)."""
    sums = monthly_flow(source, flow, source.averaged_kinds)
    return (
        calendar(source)
        .join(sums, on="month", how="left")
        .with_columns(pl.col("amount").fill_null(0.0))
        .with_columns(_rolling_means(windows))
    )


def rolling_flow(source: SourceData, flow: Flow, window: int) -> pl.DataFrame:
    """Rolling means of `monthly_flow` of the averaged kinds, over every month
    of the data (0 in months without rows, e.g. without savings)."""
    flows = rolling_flow_of_windows(source, flow, (window,))
    return flows.select("month", amount=rolling_column(window))


def _percent(column: str) -> pl.Expr:
    """Percent of the column's (signed) total, 0 if the total is 0."""
    total = pl.col(column).sum()
    return (
        pl.when(total != 0)
        .then(pl.col(column) / total * 100)
        .otherwise(0.0)
        .alias("percent")
    )


def breakdown_total(source: SourceData, direction: Direction) -> pl.DataFrame:
    """Sums by tag and subtag (with its kind), and their percent of the total.
    Sums are signed (e.g. a tag of expenses that was refunded more than it
    spent in the period is negative)."""
    df = flow_rows(source, direction)
    data = df.group_by("tags", "tag", "subtag", "kind").agg(pl.col("amount").sum())
    return data.with_columns(_percent("amount"))


def breakdown_rolling_total_of_windows(
    source: SourceData, direction: Direction, windows: Sequence[int], month: str
) -> dict[int, pl.DataFrame]:
    """`breakdown_rolling_total` of every window."""
    keys = ("tag", "subtag", "kind")
    df = flow_rows(source, direction, source.averaged_kinds)
    sums = df.group_by("month", *keys).agg(pl.col("amount").sum())
    grid = _monthly_grid(sums, keys, calendar(source, month)).with_columns(
        _rolling_means(windows, keys)
    )
    in_month = grid.filter(pl.col("month") == month)
    totals = {}
    for window in windows:
        data = (
            in_month.select(*keys, "month", "amount", rolling=rolling_column(window))
            .drop_nulls("rolling")
            .filter(pl.col("rolling") != 0)
        )
        totals[window] = data.with_columns(_percent("rolling"))
    return totals


def breakdown_rolling_total(
    source: SourceData, direction: Direction, window: int, month: str
) -> pl.DataFrame:
    """Rolling means of the averaged kinds by tag, subtag and kind in a month,
    and their percent of the total. Means are signed, and those of 0 left out,
    so the means add up to the rolling mean of the flow. A subtag has a mean by
    kind when some of its transactions have a kind set."""
    return breakdown_rolling_total_of_windows(source, direction, (window,), month)[
        window
    ]


def tag_trends_of_windows(
    source: SourceData,
    direction: Direction,
    windows: Sequence[int] = ROLLING_WINDOWS,
    by_subtag: bool = False,
) -> pl.DataFrame:
    """`tag_trends` with the rolling means of every window (see
    `rolling_column`)."""
    keys = ("tag", "subtag") if by_subtag else ("tag",)
    df = flow_rows(source, direction, source.averaged_kinds)
    sums = df.group_by("month", *keys).agg(pl.col("amount").sum())
    return _monthly_grid(sums, keys, calendar(source)).with_columns(
        _rolling_means(windows, keys)
    )


def tag_trends(
    source: SourceData, direction: Direction, window: int, by_subtag: bool = False
) -> pl.DataFrame:
    """Monthly sums of the averaged kinds by tag (or by tag and subtag), in
    every month of the data (0 without rows), and their rolling means. A tag's
    subtags add up to the tag, means too."""
    trends = tag_trends_of_windows(source, direction, (window,), by_subtag)
    return trends.rename({rolling_column(window): "rolling"})


def tag_order(source: SourceData) -> pl.Series:
    """Tags of incomes and expenses by their absolute net amount, largest first."""
    return (
        source.get(kinds=FLOW_KINDS)
        .group_by("tag")
        .agg(pl.col("amount").sum())
        .with_columns(pl.col("amount").abs())
        .sort("amount", "tag", descending=[True, False])["tag"]
    )


def breakdown_months(source: SourceData, direction: Direction) -> pl.DataFrame:
    """Sums by month, tag and subtag (with its kind). Sums may be negative (e.g.
    a refund subtag, when netting by tag)."""
    return (
        flow_rows(source, direction)
        .group_by("month", "tags", "tag", "subtag", "kind")
        .agg(pl.col("amount").sum())
    )
