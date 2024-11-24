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
    filters.add_argument(
        "-1",
        "--filter-tag1",
        nargs="*",
        help="Filter by tags",
    )
    filters.add_argument(
        "-2",
        "--filter-tag2",
        nargs="*",
        help="Filter by subtags",
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
    filter_tags1 = args.filter_tag1
    filter_tags2 = args.filter_tag2
    source_data = get_source_data(args)
    tagged_data = apply_tags(source_data, tags_file)
    analyze(
        tagged_data,
        strict=strict,
        verbose=verbose,
        filter_tags1=filter_tags1,
        filter_tags2=filter_tags2,
        start_date=start_date,
        end_date=end_date,
    )


def analyze(
    source_data,
    *,
    verbose: bool = False,
    strict: bool = True,
    filter_tags1,
    filter_tags2,
    start_date: Optional[arrow.Arrow] = None,
    end_date: Optional[arrow.Arrow] = None,
):
    print_table(source_data, "unfiltered source data", verbose > 1)
    source_data = source_data.select(*COLUMN_ORDER).lazy()
    source_data = filter_date_range(source_data, start_date, end_date)
    source_data = filter_tags(source_data, filter_tags1, filter_tags2)
    filtered_data = source_data.collect()
    print_table(filtered_data, "filtered source data")
    if strict:
        validate_tags(filtered_data)

    tag1, tag2 = tag_tables(source_data)
    print_table(tag2.collect(), "By subtags")
    print_table(tag1.collect(), "By tags")
    total_sum = source_data.select(pl.col("amount").sum()).collect()["amount"][0]
    print(f"Total sum: {total_sum}")


def filter_date_range(df, start, end):
    if start:
        df = df.filter(pl.col("date").dt.date() >= start.date())
    if end:
        df = df.filter(pl.col("date").dt.date() < end.date())
    return df


def filter_tags(df, tags1, tags2):
    predicates = [pl.lit(True)]
    if tags1 is not None:
        predicates.append(pl.col("tag1").is_in(pl.Series(tags1)))
    if tags2 is not None:
        predicates.append(pl.col("tag2").is_in(pl.Series(tags2)))
    predicate = predicates[0]
    for p in predicates[1:]:
        predicate = predicate & p
    return df.filter(predicate)


def validate_tags(source_data):
    missing_tag_indices = tuple(source_data["tag1"].is_null().arg_true())
    if missing_tag_indices:
        raise ValueError(f"Missing tags at indices: {missing_tag_indices}")


def tag_tables(df):
    tag1 = (
        df.group_by("tag1")
        .agg(
            [
                pl.len().alias("txn"),
                pl.col("amount").sum(),
            ]
        )
        .sort(("amount"), descending=False)
        .select("tag1", "amount", "txn")
    )
    tag2 = (
        df.group_by("tag1", "tag2")
        .agg(
            [
                pl.len().alias("txn"),
                pl.col("amount").sum(),
            ]
        )
        .join(tag1, on="tag1", how="left")
        .sort(("amount_right", "amount"), descending=False)
        .select(("tag1", "tag2", "amount", "txn"))
    )
    return tag1, tag2
