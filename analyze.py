import arrow
import polars as pl

from utils import print_table

HISTORICAL_SCHEMA = {
    "date": pl.Date,
    "balance": pl.Float64,
    "description": pl.String,
    "category1": pl.String,
    "category2": pl.String,
    "source": pl.String,
    "year": pl.Int32,
    "month": pl.Int8,
    "hash": pl.UInt64,
}
NULLABLE_COLUMNS = ("category2",)


def analyze(historical_data):
    historical_data = add_metadata(historical_data)
    print_table(historical_data, "historical", True)
    validate_historical_data(historical_data)
    print(historical_data.describe())

    lf = historical_data.lazy()
    last_month = filter_last_month(lf, historical_data["date"].max())
    print_table(last_month.select(pl.col("balance").sum()).collect())
    print_table(category_amount(last_month).collect())


def add_metadata(df):
    return df.with_columns(
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
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


def filter_last_month(df, last_month=None):
    if last_month is None:
        last_month = arrow.now().shift(months=-1)
    return df.filter(
        pl.col("month") == last_month.month,
        pl.col("year") == last_month.year,
    )


def category_amount(df):
    return (
        df.group_by("category1")
        .agg(pl.col("balance").sum())
        .sort("balance", descending=False)
    )
