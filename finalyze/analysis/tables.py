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

    def __post_init__(self):
        if isinstance(self.source, pl.LazyFrame):
            self.source = self.source.collect()

    @property
    def has_figure(self):
        return self.figure_constructor is not None

    def get_figure(self, **kwargs):
        if self.figure_constructor is None:
            return None
        kwargs = self.figure_arguments | kwargs
        return self.figure_constructor(self.source.to_pandas(), **kwargs)

    def with_totals(self):
        return add_totals(self.source)


def get_tables(source: pl.DataFrame) -> list[Table]:
    validate_schema(source, ENRICHED_SCHEMA)
    incomes = source.filter(pl.col("amount") > 0)
    expenses = source.filter(pl.col("amount") < 0).with_columns(pl.col("amount") * -1)
    cumsum_balances = (
        source.group_by("account", "month")
        .agg(pl.col("amount").sum())
        .sort("month", "account")
        .with_columns(pl.col("amount").cum_sum().over("account").alias("amount"))
    )
    cumsum_balances_total = (
        cumsum_balances.group_by("month")
        .agg(pl.col("amount").sum())
        .sort("month")
        .with_columns(pl.lit("total").alias("account"))
        .select(cumsum_balances.columns)
    )
    cumsum_balances = pl.concat((cumsum_balances, cumsum_balances_total)).sort(
        "month", "account"
    )
    tables = [
        Table(
            "Expenses breakdown detailed",
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
            "Incomes breakdown detailed",
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
            "Monthly net breakdown",
            source.group_by("month", "tag", "subtag")
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
            "Total account balances",
            cumsum_balances,
            figure_constructor=px.line,
            figure_arguments=dict(
                x="month",
                y="amount",
                color="account",
                hover_data=["amount"],
                labels=dict(month="Month", amount="Amount"),
            ),
        ),
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
