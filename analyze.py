from typing import Optional

import arrow
import polars as pl

from source import get_source_data
from tag import apply_tags
from utils import print_table

COLUMN_ORDER = (
    "source",
    "date",
    "amount",
    "tag1",
    "tag2",
    "description",
    "year",
    "month",
    "hash",
)


def add_subparser(subparsers):
    parser = subparsers.add_parser("analyze", help="Analyze historical data")
    parser.set_defaults(func=run)
    parser.add_argument(
        "-T",
        "--lenient",
        action="store_true",
        help="Do not perform strict data validation",
    )
    filters = parser.add_argument_group("FILTERS")
    filters.add_argument(
        "--full",
        action="store_true",
        help="Analyze full historical data (no date filters)",
    )
    filters.add_argument(
        "-M",
        "--month",
        type=int,
        default=arrow.now().shift(months=-1).month,
        help="Month to analyze",
    )
    filters.add_argument(
        "-Y",
        "--year",
        type=int,
        default=arrow.now().shift(months=-1).year,
        help="Year to analyze",
    )


def run(args):
    verbose = args.verbose
    tags_file = args.tags_file
    strict = not args.lenient
    analyze_full = args.full
    year = args.year
    month = args.month
    flip_rtl = args.flip_rtl
    if analyze_full:
        filtered_month = None
    else:
        filtered_month = arrow.Arrow(year, month, 1)
    source_data = get_source_data(args)
    tagged_data = apply_tags(source_data, tags_file)
    analyze(tagged_data, strict=strict, verbose=verbose, month=filtered_month)


def analyze(
    source_data,
    *,
    verbose: bool = False,
    strict: bool = True,
    month: Optional[arrow.Arrow] = None,
):
    source_data = add_metadata(source_data).select(*COLUMN_ORDER)
    print_table(source_data, "prefilter source", verbose > 1)
    if month is not None:
        source_data = filter_month(source_data, month)
    print_table(source_data, "source")
    if strict:
        validate_tags(source_data)

    lf = source_data.lazy()
    total_sum = lf.select(pl.col("amount").sum()).collect()["amount"][0]
    print_table(tag_amount(lf).collect(), "By tags")
    print(f"Total sum: {total_sum}")


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


def validate_tags(source_data):
    missing_tag_indices = tuple(source_data["tag1"].is_null().arg_true())
    if missing_tag_indices:
        raise ValueError(f"Missing tags at indices: {missing_tag_indices}")


def tag_amount(df):
    by_tag1 = (
        df.group_by("tag1").agg(pl.col("amount").sum()).sort("amount", descending=False)
    )
    return (
        df.group_by("tag1", "tag2")
        .agg(pl.col("amount").sum())
        .join(by_tag1, on="tag1", how="left")
        .rename({"amount_right": "amount1", "amount": "amount2"})
        .sort(("amount1", "amount2"), descending=False)
        # Nullify all but first of duplicate tag1
        .with_columns(
            pl.when(pl.col("tag1") == pl.col("tag1").shift(1))
            .then(None)
            .otherwise(pl.col("amount1"))
            .alias("amount1")
        )
        .select(("amount1", "tag1", "tag2", "amount2"))
    )
