import dataclasses
import datetime
from typing import Any, Callable, Optional

import plotly.express as px
import polars as pl
from plotly.graph_objects import Figure

from finalyze.config import config
from finalyze.display import round_columns
from finalyze.source.data import SourceData


@dataclasses.dataclass
class Table:
    title: str
    source: pl.DataFrame = dataclasses.field(repr=False)
    figure_constructor: Optional[Callable[[Any], Figure]] = None
    figure_arguments: dict[str, Any] = dataclasses.field(default_factory=dict)
    extra_traces: list["Table"] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        self.source = round_columns(self.source)
        if isinstance(self.source, pl.LazyFrame):
            self.source = self.source.collect()

    @property
    def has_figure(self):
        return self.figure_constructor is not None

    def get_figure(self, **kwargs):
        if self.figure_constructor is None:
            return None
        self_kwargs = self.figure_arguments | kwargs
        figure = self.figure_constructor(self.source.to_pandas(), **self_kwargs)
        for extra_table in self.extra_traces:
            for trace in extra_table.get_figure(**kwargs).data:
                figure.add_trace(trace)
        return figure

    def with_totals(self):
        return add_totals(self.source)


def add_totals(df, collect=True):
    schema = df.collect_schema()
    dtypes = dict(zip(schema.names(), schema.dtypes()))
    totals_row = {}
    for name, dtype in dtypes.items():
        if dtype.is_numeric():
            col = df.lazy().select(pl.col(name).sum()).collect()
        elif dtype.to_python() is str:
            col = "<< TOTAL >>"
        else:
            col = None
        totals_row[name] = col
    final = pl.concat([df.lazy(), pl.LazyFrame(totals_row)])
    if collect or isinstance(df, pl.DataFrame):
        return final.collect()
    return final


def get_tables(source: SourceData) -> dict[str, list[Table]]:
    tables = {
        "Balance": [*_balance(source), *_cash_flow(source)],
        "Total Breakdown": _breakdown_total(source),
        "Rolling breakdowns": _breakdown_rolling(source),
        "Monthly breakdowns": _breakdown_monthly(source),
    }
    return tables


def _balance(source) -> list[Table]:
    account_balances = Table(
        "Account balances",
        source.get(include_external=True, edge_ticks=True),
        figure_constructor=px.line,
        figure_arguments=dict(
            x="date",
            y="balance_source",
            color="account_source",
            hover_data=[
                "balance_source",
                "amount",
                "description",
                "tags",
                "account",
                "source",
            ],
            line_shape="hv",
            labels=dict(balance_source="Balance", account_source="Source"),
        ),
    )
    other_balances = Table(
        "Other balances",
        source.get(edge_ticks=True).with_columns(
            pl.lit("internal balance").alias("color")
        ),
        figure_constructor=px.line,
        figure_arguments=dict(
            x="date",
            y="balance_inexternal",
            color="color",
            hover_data=[
                "amount",
                "description",
                "tags",
                "account",
                "source",
                "is_external",
            ],
            line_shape="hv",
            # labels=dict(balance_other="Balance (other)"),
        ),
    )
    balance = Table(
        "Balance",
        source.get(include_external=True, edge_ticks=True).with_columns(
            pl.lit("total balance").alias("color")
        ),
        figure_constructor=px.line,
        figure_arguments=dict(
            x="date",
            y="balance_total",
            color="color",
            hover_data=[
                "balance_total",
                "amount",
                "description",
                "tags",
                "account",
                "source",
            ],
            line_shape="hv",
            labels=dict(balance_total="Balance"),
        ),
        extra_traces=[other_balances, account_balances],
    )
    return [balance]


def _cash_flow(source) -> list[Table]:
    total_flow = Table(
        "Cash flow - net",
        source.get(breakdown=True)
        .group_by("month")
        .agg(pl.col("amount").sum())
        .sort("month")
        .with_columns(pl.lit("net flow").alias("color")),
        figure_constructor=px.line,
        figure_arguments=dict(
            x="month",
            y="amount",
            color="color",
            hover_data=["month", "amount"],
            markers=True,
        ),
    )
    incomes_flow = Table(
        "Cash flow - incomes",
        source.get(breakdown=True, incomes=True)
        .group_by("month")
        .agg(pl.col("amount").sum())
        .sort("month")
        .with_columns(pl.lit("incomes flow").alias("color")),
        figure_constructor=px.bar,
        figure_arguments=dict(
            x="month",
            y="amount",
            color="color",
            hover_data=["month", "amount"],
            barmode="relative",
        ),
    )
    expenses_flow = Table(
        "Cash flow - expenses",
        source.get(breakdown=True, expenses=True)
        .group_by("month")
        .agg(pl.col("amount").sum() * -1)
        .sort("month")
        .with_columns(pl.lit("expenses flow").alias("color")),
        figure_constructor=px.bar,
        figure_arguments=dict(
            x="month",
            y="amount",
            color="color",
            hover_data=["month", "amount"],
            barmode="relative",
        ),
    )
    rolling_flows = [
        Table(
            "Cash flow (rolling mean)",
            pl.concat(
                df.group_by("month")
                .agg(pl.col("amount").sum())
                .sort("month")
                .with_columns(
                    pl.lit(f"{name} rolling {weight_name}").alias("color"),
                    pl.col("amount")
                    .rolling_mean(window_size=len(weights), weights=weights)
                    .alias("amount"),
                )
                for weight_name, weights in (
                    config().analysis.rolling_average_weights.items()
                )
                if len(weights) > 1
            ),
            figure_constructor=px.line,
            figure_arguments=dict(
                x="month",
                y="amount",
                hover_data=["month", "amount"],
                markers=True,
                line_shape="spline",
                line_dash="color",
                color="color",
            ),
        )
        for df, name in [
            (source.get(breakdown=True), "total"),
            (source.get(breakdown=True, incomes=True), "incomes"),
            (source.get(breakdown=True, expenses=True), "expenses"),
        ]
    ]
    cash_flow = Table(
        "Cash flow",
        pl.DataFrame(),
        figure_constructor=px.bar,
        figure_arguments=dict(
            barmode="relative",
            # color="color",
        ),
        extra_traces=[
            total_flow,
            incomes_flow,
            expenses_flow,
            *rolling_flows,
        ],
    )
    return [cash_flow]


def _breakdown_total(source) -> list[Table]:
    total_breakdowns = []
    for df, name in [
        (source.get(breakdown=True, incomes=True, net_by_subtag=True), "incomes"),
        (source.get(breakdown=True, expenses=True, net_by_subtag=True), "expenses"),
    ]:
        data = df.group_by("tags", "tag", "subtag").agg(pl.col("amount").sum())
        total = data["amount"].sum()
        percent_col = pl.col("amount") / total * 100
        data = data.with_columns(percent_col.alias("percent"))
        table = Table(
            f"Total {name} breakdown",
            data,
            figure_constructor=px.sunburst,
            figure_arguments=dict(
                path=["tag", "subtag"],
                values="percent",
                hover_data={"tags": True, "amount": True, "percent": ":.2f"},
                labels=dict(
                    parent="Tag",
                    id="Tags",
                    labels="Tags",
                    tag="Tag",
                    subtag="Subtag",
                    amount="Amount",
                ),
                color="tag",
            ),
        )
        total_breakdowns.append(table)
    return total_breakdowns


def _breakdown_rolling(source) -> list[Table]:
    monthly_breakdowns = []
    tag_order = (
        source.get(breakdown=True)
        .group_by("tag")
        .agg(pl.col("amount").sum())
        .with_columns(pl.col("amount").abs())
        .sort("amount", descending=True)["tag"]
    )
    for weight_name, weights in config().analysis.rolling_average_weights.items():
        line_shape = "spline" if len(weights) > 1 else "linear"
        for df, name in [
            (source.get(sentinels=True, breakdown=True, incomes=True), "incomes"),
            (source.get(sentinels=True, breakdown=True, expenses=True), "expenses"),
        ]:
            table = Table(
                f"Monthly {name} breakdown - rolling {weight_name}",
                df.group_by("month", "tag")
                .agg(pl.col("amount").sum())
                .sort("month", "tag")
                .with_columns(
                    pl.col("amount")
                    .rolling_mean(window_size=len(weights), weights=weights)
                    .over("tag")
                    .alias("rolling"),
                ),
                figure_constructor=px.line,
                figure_arguments=dict(
                    x="month",
                    y="rolling",
                    color="tag",
                    markers=True,
                    line_shape=line_shape,
                    hover_data=["tag", "amount", "rolling"],
                    labels=dict(tag="Tag", amount="Amount", month="Month"),
                    category_orders=dict(tag=tag_order),
                ),
            )
            monthly_breakdowns.append(table)
    return monthly_breakdowns


def _breakdown_monthly(source) -> list[Table]:
    last_months = (
        source.get()
        .group_by("month")
        .agg(pl.col("amount").count())
        .select(pl.col("month"))
        .sort("month", descending=True)["month"][: config().analysis.breakdown_months]
    )
    last_month_breakdowns = [
        Table(
            f"{name} breakdown - {month}",
            df.filter(pl.col("month") == month)
            .group_by("tags", "tag", "subtag")
            .agg(pl.col("amount").sum())
            .filter(pl.col("amount") >= 0),
            figure_constructor=px.sunburst,
            figure_arguments=dict(
                path=["tag", "subtag"],
                values="amount",
                labels=dict(
                    parent="Tag",
                    id="Tags",
                    labels="Tags",
                    tag="Tag",
                    subtag="Subtag",
                    amount="Amount",
                ),
                color="tag",
            ),
        )
        for month in last_months
        for df, name in [
            (source.get(breakdown=True, incomes=True), "Incomes"),
            (source.get(breakdown=True, expenses=True), "Expenses"),
        ]
    ]
    return last_month_breakdowns


def _months_in_range(min_date: datetime.date, max_date: datetime.date):
    all_months = []
    current = min_date.replace(day=1)
    max_date = max_date.replace(day=2)
    while current < max_date:
        all_months.append(current)
        current = current + datetime.timedelta(days=32)
        current = current.replace(day=1)
    return all_months
