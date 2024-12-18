import dataclasses
from typing import Any, Callable, Optional

import plotly.express as px
import polars as pl
from plotly.graph_objects import Figure


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


def get_tables(source_data: pl.DataFrame) -> list[Table]:
    source = prepare_source(source_data)
    tables = [
        Table(
            "Expenses breakdown detailed",
            source.filter(pl.col("amount") < 0)
            .with_columns(pl.col("amount") * -1)
            .group_by("tags", "tag", "subtag")
            .agg(pl.col("amount").sum()),
            figure_constructor=px.sunburst,
            figure_arguments=dict(
                path=["tag", "subtag"],
                values="amount",
                labels={
                    "tag": "Tag",
                    "subtag": "Subtag",
                    "amount": "Amount",
                    "parent": "Tag",
                    "id": "Tags",
                },
                color="tag",
            ),
        ),
        Table(
            "Incomes breakdown detailed",
            source.filter(pl.col("amount") > 0)
            .group_by("tags", "tag", "subtag")
            .agg(pl.col("amount").sum()),
            figure_constructor=px.sunburst,
            figure_arguments=dict(
                path=["tag", "subtag"],
                values="amount",
                labels={
                    "tag": "Tag",
                    "subtag": "Subtag",
                    "amount": "Amount",
                    "parent": "Tag",
                    "id": "Tags",
                },
                color="tag",
            ),
        ),
        Table(
            "Monthly breakdown",
            source.group_by("month", "tag", "subtag", "tags")
            .agg(pl.col("amount").sum())
            .sort("month", "tags"),
            figure_constructor=px.bar,
            figure_arguments=dict(
                x="month",
                y="amount",
                color="tag",
                hover_data=["tags", "amount"],
                labels={
                    "tags": "Tags",
                    "month": "Month",
                    "amount": "Amount",
                    "tag": "Tag",
                },
            ),
        ),
    ]
    return tables


def prepare_source(source: pl.DataFrame):
    source = source.lazy()
    # Month column
    year = pl.col("date").dt.year().cast(str)
    month = pl.col("date").dt.month().cast(str).str.pad_start(2, "0")
    source = source.with_columns((year + "-" + month).alias("month"))
    # Combined tags column
    combined_tags = pl.col("tag") + " - " + pl.col("subtag")
    source = source.with_columns(combined_tags.alias("tags"))
    return source


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
