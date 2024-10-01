import arrow

import polars as pl


BALANCE_RAW_SCHEMA = {
    "person": pl.String,
    "date": pl.Date,
    "amount": pl.Float64,
    "category1": pl.String,
    "category2": pl.String,
}

BALANCE_FULL_SCHEMA = {
    "year": pl.Int32,
    "month": pl.Int8,
}
NULLABLE_COLUMNS = ("category2",)


def read_data():
    df = pl.read_csv("balance.csv", schema=BALANCE_RAW_SCHEMA)
    df = df.with_columns(
        [
            pl.col("date").dt.year().alias("year"),
            pl.col("date").dt.month().alias("month"),
        ]
    )
    with pl.Config(set_tbl_rows=20):
        print(df)
    # validate
    expected_columns = set(BALANCE_RAW_SCHEMA.keys()) | set(BALANCE_FULL_SCHEMA.keys())
    actual_columns = set(df.columns)
    incompatible_columns = expected_columns ^ actual_columns
    if incompatible_columns:
        raise ValueError(f"{incompatible_columns=}")
    for col in actual_columns:
        if col in NULLABLE_COLUMNS:
            continue
        if indices := tuple(df[col].is_null().arg_true()):
            raise ValueError(f"column {col!r} has nulls at indices: {indices}")
    return df


def filter_last_month(df):
    last_month = arrow.now().shift(months=-1)
    return df.filter(
        pl.col("month") == last_month.month,
        pl.col("year") == last_month.year,
    )


def category_amount(df):
    return (
        df.group_by("category1")
        .agg(pl.col("amount").sum())
        .sort("amount", descending=False)
    )


def main():
    raw_data = read_data()
    print(raw_data.describe())
    pl.Config.set_tbl_rows(1_000_000)

    lf = raw_data.lazy()
    last_month = filter_last_month(lf)
    print(last_month.select(pl.col("amount").sum()).collect())
    print(category_amount(last_month).collect())


if __name__ == "__main__":
    main()
