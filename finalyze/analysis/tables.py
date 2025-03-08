import dataclasses
import functools
from typing import Any, Callable, Optional

import plotly.express as px
import polars as pl
from plotly.graph_objects import Figure

from finalyze.config import config
from finalyze.source.data import ENRICHED_SCHEMA, validate_schema


class SourceData:
    def __init__(self, source: pl.DataFrame):
        self._source = source

    @functools.cache
    def get(
        self,
        *,
        breakdown: bool = False,
        include_external: bool = False,
        incomes: bool = False,
        expenses: bool = False,
    ):
        df = self._source
        if breakdown:
            df = config().analysis.breakdown_filters.apply(df)

        if not include_external:
            df = config().analysis.external_filters.apply(df, invert=True)

        if incomes:
            df = df.filter(pl.col("amount") > 0)
        elif expenses:
            df = df.filter(pl.col("amount") < 0).with_columns(pl.col("amount") * -1)

        return df

    def __hash__(self):
        return id(self)


@dataclasses.dataclass
class Table:
    title: str
    source: pl.DataFrame = dataclasses.field(repr=False)
    figure_constructor: Optional[Callable[[Any], Figure]] = None
    figure_arguments: dict[str, Any] = dataclasses.field(default_factory=dict)
    extra_traces: list["Table"] = dataclasses.field(default_factory=list)

    def __post_init__(self):
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
        elif dtype.to_python() == str:
            col = "<< TOTAL >>"
        else:
            col = None
        totals_row[name] = col
    final = pl.concat([df.lazy(), pl.LazyFrame(totals_row)])
    if collect or isinstance(df, pl.DataFrame):
        return final.collect()
    return final


def get_tables(source: pl.DataFrame) -> list[Table]:
    validate_schema(source, ENRICHED_SCHEMA)
    source_data = SourceData(source)
    tables = [
        *_balance(source_data),
        *_cash_flow(source_data),
        *_breakdown_total(source_data),
        *_breakdown_monthly(source_data),
    ]
    return tables


def _balance(source: SourceData) -> list[Table]:
    account_balances = Table(
        "Account balances",
        source.get(include_external=True),
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
        source.get().with_columns(pl.lit("internal balance").alias("Balance")),
        figure_constructor=px.line,
        figure_arguments=dict(
            x="date",
            y="balance_inexternal",
            color="Balance",
            hover_data=[
                "amount",
                "description",
                "tags",
                "account",
                "source",
                "external",
            ],
            line_shape="hv",
            # labels=dict(balance_other="Balance (other)"),
        ),
    )
    balance = Table(
        "Balance",
        source.get(include_external=True).with_columns(
            pl.lit("total balance").alias("Balance")
        ),
        figure_constructor=px.line,
        figure_arguments=dict(
            x="date",
            y="balance_total",
            color="Balance",
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


def _cash_flow(source: SourceData) -> list[Table]:
    incomes_flow, expenses_flow = [
        Table(
            f"Cash flow - {name.capitalize()}",
            df.group_by("month")
            .agg(pl.col("amount").sum())
            .sort("month")
            .with_columns(pl.lit(name).alias("Flow")),
            figure_constructor=px.bar,
            figure_arguments=dict(
                x="month",
                y="amount",
                color="Flow",
                hover_data=["month", "amount"],
                barmode="group",
            ),
        )
        for df, name in [
            (source.get(breakdown=True, incomes=True), "income"),
            (source.get(breakdown=True, expenses=True), "expense"),
        ]
    ]
    rolling_total_flow, rolling_incomes_flow, rolling_expenses_flow = [
        Table(
            "Cash flow (rolling mean)",
            pl.concat(
                df.group_by("month")
                .agg(pl.col("amount").sum())
                .sort("month")
                .with_columns(
                    pl.lit(f"rolling {name} ({len(weights)})").alias("Flow"),
                    pl.col("amount")
                    .rolling_mean(window_size=len(weights), weights=weights)
                    .alias("amount"),
                )
                for weights in config().analysis.rolling_average_weights
            ),
            figure_constructor=px.line,
            figure_arguments=dict(
                x="month",
                y="amount",
                hover_data=["month", "amount"],
                markers=True,
                line_shape="spline",
                line_dash="Flow",
                color="Flow",
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
        source.get(breakdown=True)
        .group_by("month")
        .agg(pl.col("amount").sum())
        .sort("month")
        .with_columns(pl.lit("total flow").alias("Flow")),
        figure_constructor=px.bar,
        figure_arguments=dict(
            x="month",
            y="amount",
            color="Flow",
            hover_data=["month", "amount"],
            barmode="group",
        ),
        extra_traces=[
            incomes_flow,
            expenses_flow,
            rolling_total_flow,
            rolling_incomes_flow,
            rolling_expenses_flow,
        ],
    )
    return [cash_flow]


def _breakdown_total(source: SourceData) -> list[Table]:
    total_breakdowns = [
        Table(
            f"Total {name} breakdown",
            df.group_by("tags", "tag", "subtag").agg(pl.col("amount").sum()),
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
        for df, name in [
            (source.get(breakdown=True, incomes=True), "incomes"),
            (source.get(breakdown=True, expenses=True), "expenses"),
        ]
    ]
    return total_breakdowns


def _breakdown_monthly(source: SourceData) -> list[Table]:
    monthly_breakdowns = [
        Table(
            f"Monthly {name} breakdown",
            df.group_by("month", "tag")
            .agg(pl.col("amount").sum())
            .sort("month", "tag"),
            figure_constructor=px.area,
            figure_arguments=dict(
                x="month",
                y="amount",
                color="tag",
                line_shape="linear",
                hover_data=["tag", "amount"],
                labels=dict(tag="Tag", amount="Amount", month="Month"),
            ),
        )
        for df, name in [
            (source.get(breakdown=True, incomes=True), "incomes"),
            (source.get(breakdown=True, expenses=True), "expenses"),
        ]
    ]
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
            .agg(pl.col("amount").sum()),
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
    return [
        *monthly_breakdowns,
        *last_month_breakdowns,
    ]
