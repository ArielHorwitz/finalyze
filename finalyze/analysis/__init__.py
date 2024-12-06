import functools
import operator

import arrow
import polars as pl

from finalyze.source import get_source_data
from finalyze.tag import apply_tags
from finalyze.utils import print_table

from . import tables

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
DATE_PATTERNS = (
    "YYYY-MM-DD",
    "YYYY-MM",
    "YYYY",
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
        help="Filter since date (inclusive)",
    )
    filters.add_argument(
        "-E",
        "--end-date",
        help="Filter until date (non-inclusive)",
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
    source_data = get_source_data(args).sort("date", "amount")
    tagged_data = apply_tags(source_data, tags_file)
    print_table(tagged_data, "unfiltered source data", verbose > 1)
    filtered_data = filter_data(
        tagged_data.lazy(),
        start_date=args.start_date,
        end_date=args.end_date,
        tags1=args.filter_tag1,
        tags2=args.filter_tag2,
        description=args.description,
        account=args.filter_account,
    )
    source_data = filtered_data.select(*COLUMN_ORDER)
    print_table(source_data.collect(), "filtered source data")
    analyze(filtered_data, strict=strict)


def analyze(source_data, *, strict: bool = True):
    if strict:
        validate_tags(source_data.collect())
    tag1, tag2 = tables.tag_tables(source_data)
    tag1, tag2 = tag1.collect(), tag2.collect()
    by_subtags = tables.with_totals(tag2, "tag1", ["amount", "txn"])
    by_tags = tables.with_totals(tag1, "tag1", ["amount", "txn"])
    print_table(by_subtags, "By subtags")
    print_table(by_tags, "By tags")
    monthly_amounts, monthly_txns = tables.monthly(source_data, tag1["tag1"])
    print_table(tables.with_totals(monthly_txns, "tag1"), "Txn by month")
    print_table(tables.with_totals(monthly_amounts, "tag1"), "Amount by month")
    total_sum = source_data.select(pl.col("amount").sum()).collect()["amount"][0]
    print(f"Total sum: {total_sum}")


def validate_tags(source_data):
    missing_tag_indices = tuple(source_data["tag1"].is_null().arg_true())
    if missing_tag_indices:
        raise ValueError(f"Missing tags at indices: {missing_tag_indices}")


def filter_data(
    df,
    *,
    start_date,
    end_date,
    tags1,
    tags2,
    description,
    account,
):
    predicates = []
    # dates
    if start_date is not None:
        predicates.append(pl.col("date").dt.date() >= _parse_date(start_date))
    if end_date is not None:
        predicates.append(pl.col("date").dt.date() < _parse_date(end_date))
    # tags
    if tags1 is not None:
        predicates.append(pl.col("tag1").is_in(pl.Series(tags1)))
    if tags2 is not None:
        predicates.append(pl.col("tag2").is_in(pl.Series(tags2)))
    # patterns
    if description is not None:
        predicates.append(pl.col("description").str.contains(description))
    if account is not None:
        predicates.append(pl.col("account") == account)
    # filter
    predicate = functools.reduce(operator.and_, predicates, pl.lit(True))
    return df.filter(predicate)


def _parse_date(raw_date):
    if not raw_date:
        return None
    for pattern in DATE_PATTERNS:
        try:
            return arrow.get(raw_date, pattern).date()
        except arrow.parser.ParserMatchError:
            pass
    raise arrow.parser.ParserMatchError(
        f"Failed to match date {raw_date!r} against patterns: {DATE_PATTERNS}"
    )
