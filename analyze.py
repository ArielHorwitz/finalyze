from typing import Optional

import arrow
import polars as pl

from utils import print_table

HISTORICAL_SCHEMA = {
    "date": pl.Date,
    "balance": pl.Float64,
    "tag1": pl.String,
    "tag2": pl.String,
    "description": pl.String,
    "source": pl.String,
    "year": pl.Int32,
    "month": pl.Int8,
    "hash": pl.UInt64,
}
NULLABLE_COLUMNS = ("tag2",)


def analyze(historical_data, *, month: Optional[arrow.Arrow] = None):
    historical_data = add_metadata(historical_data).select(*HISTORICAL_SCHEMA.keys())
    if month is not None:
        historical_data = filter_month(historical_data, month)

    print_table(historical_data, "historical")
    print_table(historical_data.describe(), "historical describe")
    validate_historical_data(historical_data)

    lf = historical_data.lazy()
    print_table(lf.select(pl.col("balance").sum()).collect(), "Total balance")
    print_table(tag_amount(lf).collect(), "By tags")


def add_metadata(df):
    return df.with_columns(
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
    )


def filter_month(df, month):
    return df.filter(
        pl.col("month") == month.month,
        pl.col("year") == month.year,
    )


def validate_historical_data(df):
    expected_columns = set(HISTORICAL_SCHEMA.keys())
    actual_columns = set(df.columns)
    incompatible_columns = expected_columns ^ actual_columns
    if incompatible_columns:
        raise ValueError(f"Found incompatible columns: {incompatible_columns}")
    for col in actual_columns:
        if col in NULLABLE_COLUMNS:
            continue
        if indices := tuple(df[col].is_null().arg_true()):
            raise ValueError(f"Column {col!r} has nulls at indices: {indices}")


def tag_amount(df):
    return (
        df.group_by("tag1")
        .agg(pl.col("balance").sum())
        .sort("balance", descending=False)
    )
