import dataclasses
from typing import Any, Callable, Optional

import plotly.express as px
import polars as pl
from plotly.graph_objects import Figure

from finalyze.source.data import ENRICHED_SCHEMA, validate_schema


@dataclasses.dataclass
class Table:
    title: str
    source: pl.DataFrame = dataclasses.field(repr=False)
    figure_constructor: Optional[Callable[[Any], Figure]] = None
    figure_arguments: dict[str, Any] = dataclasses.field(default_factory=dict)
    extra_traces: Optional["Table"] = None

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
        if self.extra_traces:
            for trace in self.extra_traces.get_figure(**kwargs).data:
                figure.add_trace(trace)
        return figure

    def with_totals(self):
        return add_totals(self.source)


def get_tables(source: pl.DataFrame, config) -> list[Table]:
    validate_schema(source, ENRICHED_SCHEMA)
    breakdowns = config.analysis.breakdown_filters.apply(source)
    incomes = breakdowns.filter(pl.col("amount") > 0)
    expenses = breakdowns.filter(pl.col("amount") < 0).with_columns(
        pl.col("amount") * -1
    )
    last_months = (
        source.group_by("month")
        .agg(pl.col("amount").count())
        .select(pl.col("month"))
        .sort("month", descending=True)["month"][: config.analysis.breakdown_months]
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
        for df, name in ((expenses, "Expenses"), (incomes, "Incomes"))
        for month in last_months
    ]
    account_balances = Table(
        "Account balances",
        source,
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
            labels=dict(balance_source="Balance", account_source="Source"),
        ),
    )
    tables = [
        Table(
            "Total balance",
            source.with_columns(pl.lit("total").alias("Balance")),
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
                labels=dict(balance_total="Balance"),
            ),
            extra_traces=account_balances,
        ),
        Table(
            "Monthly net breakdown",
            breakdowns.group_by("month", "tag", "subtag")
            .agg(pl.col("amount").sum())
            .sort("month", "tag", "subtag"),
            figure_constructor=px.bar,
            figure_arguments=dict(
                x="month",
                y="amount",
                color="tag",
                hover_data=["tag", "subtag", "amount"],
                labels=dict(tag="Tag", subtag="Subtag", amount="Amount", month="Month"),
            ),
        ),
        Table(
            "Monthly expenses breakdown",
            expenses.group_by("month", "tag", "subtag")
            .agg(pl.col("amount").sum())
            .sort("month", "tag", "subtag"),
            figure_constructor=px.bar,
            figure_arguments=dict(
                x="month",
                y="amount",
                color="tag",
                hover_data=["tag", "subtag", "amount"],
                labels=dict(tag="Tag", subtag="Subtag", amount="Amount", month="Month"),
            ),
        ),
        Table(
            "Monthly incomes breakdown",
            incomes.group_by("month", "tag", "subtag")
            .agg(pl.col("amount").sum())
            .sort("month", "tag", "subtag"),
            figure_constructor=px.bar,
            figure_arguments=dict(
                x="month",
                y="amount",
                color="tag",
                hover_data=["tag", "subtag", "amount"],
                labels=dict(tag="Tag", subtag="Subtag", amount="Amount", month="Month"),
            ),
        ),
        Table(
            "Total expenses breakdown",
            expenses.group_by("tags", "tag", "subtag").agg(pl.col("amount").sum()),
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
        ),
        Table(
            "Total incomes breakdown",
            incomes.group_by("tags", "tag", "subtag").agg(pl.col("amount").sum()),
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
        ),
        *last_month_breakdowns,
    ]
    return tables


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
