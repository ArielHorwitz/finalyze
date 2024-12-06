import polars as pl


def tag_tables(df):
    aggregations = (
        pl.len().alias("txn"),
        pl.col("amount").sum(),
    )
    tag1 = (
        df.group_by("tag1")
        .agg(aggregations)
        .sort(("amount"), descending=False)
        .select("tag1", "amount", "txn")
    )
    tag2 = (
        df.group_by("tag1", "tag2")
        .agg(aggregations)
        .join(tag1, on="tag1", how="left")
        .sort(("amount_right", "amount"), descending=False)
        .select(("tag1", "tag2", "amount", "txn"))
    )
    return tag1, tag2


def monthly(df, tag_order):
    amounts = _monthly(df, tag_order)
    txn = _monthly(df, tag_order, aggregate_counts=True)
    return amounts, txn


def _monthly(df, tag_order, aggregate_counts=False):
    year = pl.col("date").dt.year().cast(str)
    month = pl.col("date").dt.month().cast(str).str.pad_start(2, "0")
    df = df.with_columns((year + "-" + month).alias("month"))
    df = df.with_columns(pl.len().alias("txn"))
    aggregation = pl.len().alias("txn") if aggregate_counts else pl.col("amount").sum()
    aggregation_name = "txn" if aggregate_counts else "amount"
    df = df.group_by(("month", "tag1")).agg(aggregation).sort("month")
    df = (
        df.collect()
        .pivot(index="tag1", columns="month", values=aggregation_name)
        .fill_null(0)
    )
    sort_ref = pl.DataFrame({"tag1": tag_order, "tag_order": range(len(tag_order))})
    df = df.join(sort_ref, on="tag1", how="left").sort("tag_order").drop("tag_order")
    return df


def with_totals(df, label_col, numeric_cols=None):
    unused_cols = set(df.columns) - {label_col}
    numeric_cols = numeric_cols or set(unused_cols)
    unused_cols = unused_cols - set(numeric_cols)
    totals = df.select(pl.col(numeric_cols).sum())
    labeled_totals = totals.with_columns(
        pl.lit("<< TOTAL >>").alias(label_col),
        *(pl.lit(None).alias(col) for col in unused_cols),
    ).select(df.columns)
    return pl.concat([df, labeled_totals])
