from typing import Optional

import arrow
import polars as pl

from finalyze.source import get_source_data
from finalyze.tag import apply_tags
from finalyze.utils import print_table

COLUMN_ORDER = (
    "account",
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
    filters.add_argument(
        "-D",
        "--description",
        help="Filter by description (regex pattern)",
    )
    filters.add_argument(
        "-A",
        "--filter-account",
        help="Filter by account name",
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
    filter_description = args.description
    filter_account = args.filter_account
    source_data = get_source_data(args).sort("date", "amount")
    tagged_data = apply_tags(source_data, tags_file)
    print_table(tagged_data, "unfiltered source data", verbose > 1)
    analyze(
        tagged_data,
        strict=strict,
        filter_tags1=filter_tags1,
        filter_tags2=filter_tags2,
        start_date=start_date,
        end_date=end_date,
        filter_description=filter_description,
        filter_account=filter_account,
    )


def analyze(
    source_data,
    *,
    strict: bool = True,
    filter_tags1,
    filter_tags2,
    start_date: Optional[arrow.Arrow] = None,
    end_date: Optional[arrow.Arrow] = None,
    filter_description=None,
    filter_account=None,
):
    source_data = source_data.select(*COLUMN_ORDER).lazy()
    source_data = filter_date_range(source_data, start_date, end_date)
    source_data = filter_tags(source_data, filter_tags1, filter_tags2)
    source_data = filter_patterns(source_data, filter_description, filter_account)
    filtered_data = source_data.collect()
    print_table(filtered_data, "filtered source data")
    if strict:
        validate_tags(filtered_data)

    tag1, tag2 = tag_tables(source_data)
    print_table(tag2.collect(), "By subtags")
    print_table(tag1.collect(), "By tags")
    print_table(monthly(source_data, tag1.collect()["tag1"]), "By month")
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


def filter_patterns(df, description_regex, account_name):
    if description_regex:
        df = df.filter(pl.col("description").str.contains(description_regex))
    if account_name:
        df = df.filter(pl.col("account") == account_name)
    return df


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


def monthly(df, tag_order):
    year = pl.col("date").dt.year().cast(str)
    month = pl.col("date").dt.month().cast(str).str.pad_start(2, "0")
    df = df.with_columns((year + "-" + month).alias("month"))
    df = df.group_by(("month", "tag1")).agg(pl.col("amount").sum()).sort("month")
    df = df.collect().pivot(index="tag1", columns="month", values="amount").fill_null(0)
    sort_ref = pl.DataFrame({"tag1": tag_order, "tag_order": range(len(tag_order))})
    df = df.join(sort_ref, on="tag1", how="left").sort("tag_order").drop("tag_order")
    return df
