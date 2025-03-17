import dataclasses
import datetime
import functools
import operator
from typing import Any, Callable, Optional

import plotly.express as px
import polars as pl
from plotly.graph_objects import Figure

from finalyze.config import config
from finalyze.source.data import ENRICHED_SCHEMA, derive_month, validate_schema


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
            df = config().analysis.breakdown_filters.apply(df, invert=True)
        if not include_external:
            df = config().analysis.external_filters.apply(df, invert=True)
        if incomes:
            df = self._filter_net(df, incomes_or_expenses=True)
        elif expenses:
            df = self._filter_net(df, incomes_or_expenses=False)
        return df

    def _filter_net(self, df, incomes_or_expenses: bool):
        operation = operator.gt if incomes_or_expenses else operator.lt
        amount_filter = operation(pl.col("amount"), 0)
        if config().analysis.net_by_tag:
            tags_net = self._source.group_by("tag").agg(pl.col("amount").sum())
            tags = tags_net.filter(amount_filter)["tag"]
            df = df.filter(pl.col("tag").is_in(tags))
        else:
            df = df.filter(amount_filter)
        # Reverse (if expenses)
        if not incomes_or_expenses:
            df = df.with_columns(pl.col("amount") * -1)
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
        *_breakdown_rolling(source_data),
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
        source.get().with_columns(pl.lit("internal balance").alias("color")),
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
                "external",
            ],
            line_shape="hv",
            # labels=dict(balance_other="Balance (other)"),
        ),
    )
    balance = Table(
        "Balance",
        source.get(include_external=True).with_columns(
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


def _cash_flow(source: SourceData) -> list[Table]:
    incomes_flow, expenses_flow = [
        Table(
            f"Cash flow - {name.capitalize()}",
            df.group_by("month")
            .agg(pl.col("amount").sum())
            .sort("month")
            .with_columns(pl.lit(name).alias("color")),
            figure_constructor=px.bar,
            figure_arguments=dict(
                x="month",
                y="amount",
                color="color",
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
                    pl.lit(f"{name} rolling {weight_name}").alias("color"),
                    pl.col("amount")
                    .rolling_mean(window_size=len(weights), weights=weights)
                    .alias("amount"),
                )
                for weight_name, weights in (
                    config().analysis.rolling_average_weights.items()
                )
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
        source.get(breakdown=True)
        .group_by("month")
        .agg(pl.col("amount").sum())
        .sort("month")
        .with_columns(pl.lit("total flow").alias("color")),
        figure_constructor=px.bar,
        figure_arguments=dict(
            x="month",
            y="amount",
            color="color",
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


def _breakdown_rolling(source: SourceData) -> list[Table]:
    monthly_breakdowns = []
    all_dates = source.get(breakdown=True)["date"]
    months = pl.Series(_months_in_range(all_dates.min(), all_dates.max()))
    sentinels = (
        source.get(breakdown=True)
        .group_by("tag")
        .count()
        .join(pl.DataFrame(dict(date=months)), how="cross")
        .with_columns(amount=pl.lit(0))
    )
    sentinels = derive_month(sentinels).select("tag", "amount", "month")
    for df, name in [
        (source.get(breakdown=True, incomes=True), "incomes"),
        (source.get(breakdown=True, expenses=True), "expenses"),
    ]:
        data_with_sentinels = (
            df.select("month", "tag", "amount")
            .join(sentinels, how="right", on=("month", "tag"))
            .with_columns(amount=pl.coalesce("amount", "amount_right"))
        )
        for weight_name, weights in config().analysis.rolling_average_weights.items():
            table = Table(
                f"Monthly {name} breakdown - rolling {weight_name}",
                data_with_sentinels.group_by("month", "tag")
                .agg(pl.col("amount").sum())
                .sort("month", "tag")
                .with_columns(
                    # pl.lit(f"rolling {name} ({len(weights)})").alias("color"),
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
                    line_shape="spline",
                    hover_data=["tag", "amount", "rolling"],
                    labels=dict(tag="Tag", amount="Amount", month="Month"),
                ),
            )
            monthly_breakdowns.append(table)
    return monthly_breakdowns


def _breakdown_monthly(source: SourceData) -> list[Table]:
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
