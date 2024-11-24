from typing import Optional

import arrow
import polars as pl

from finalyze.source import get_source_data
from finalyze.tag import apply_tags
from finalyze.utils import print_table

COLUMN_ORDER = (
    "source",
    "date",
    "amount",
    "tag1",
    "tag2",
    "description",
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
        "-S",
        "--start-date",
        help="Filter since date (YYYY-MM-DD)",
    )
    filters.add_argument(
        "-E",
        "--end-date",
        help="Filter until date (YYYY-MM-DD)",
    )


def run(args):
    verbose = args.verbose
    tags_file = args.tags_file
    strict = not args.lenient
    start_date = None
    if args.start_date:
        start_date = arrow.get(args.start_date, "YYYY-MM-DD")
    end_date = None
    if args.end_date:
        end_date = arrow.get(args.end_date, "YYYY-MM-DD")
    source_data = get_source_data(args)
    tagged_data = apply_tags(source_data, tags_file)
    analyze(
        tagged_data,
        strict=strict,
        verbose=verbose,
        start_date=start_date,
        end_date=end_date,
    )


def analyze(
    source_data,
    *,
    verbose: bool = False,
    strict: bool = True,
    start_date: Optional[arrow.Arrow] = None,
    end_date: Optional[arrow.Arrow] = None,
):
    source_data = source_data.select(*COLUMN_ORDER)
    print_table(source_data, "prefilter source", verbose > 1)
    source_data = filter_date_range(source_data, start_date, end_date)
    print_table(source_data, "source")
    if strict:
        validate_tags(source_data)

    lf = source_data.lazy()
    total_sum = lf.select(pl.col("amount").sum()).collect()["amount"][0]
    print_table(tag_amount(lf).collect(), "By tags")
    print(f"Total sum: {total_sum}")


def filter_date_range(df, start, end):
    if start:
        df = df.filter(pl.col("date").dt.date() >= start.date())
    if end:
        df = df.filter(pl.col("date").dt.date() < end.date())
    return df


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
