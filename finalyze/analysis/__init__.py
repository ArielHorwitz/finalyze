import functools
import operator
import subprocess
import tomllib

import arrow
import polars as pl

from finalyze.source import get_source_data
from finalyze.tag import apply_tags
from finalyze.utils import print_table

from . import plot
from .tables import get_tables

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
    parser.add_argument(
        "-g",
        "--open-graphs",
        action="store_true",
        help="Open graphs in browser",
    )
    parser.add_argument(
        "-p",
        "--print-tables",
        action="store_true",
        help="Print tables to stdout",
    )
    parser.add_argument(
        "--plotly-template",
        default="plotly_dark",
        help="Select template/theme for plotly",
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
    print_tables = args.print_tables
    open_graphs = args.open_graphs
    plotly_template = args.plotly_template
    plots_files = args.data_dir / "plots.html"
    color_map_file = args.data_dir / "tag_colors.toml"
    if not color_map_file.is_file():
        color_map_file.write_text('other = "#000000"')
    color_map = tomllib.loads(color_map_file.read_text())
    # Source data
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
    if strict:
        validate_tags(source_data)
    if print_tables:
        print_table(source_data.collect(), "filtered source data")
    # Tables
    tables = get_tables(source_data)
    for table in tables:
        if verbose:
            print(table)
        if print_tables:
            print_table(table.with_totals(), table.title)
    # Plots
    if verbose:
        print(f"{plots_files=}")
    plot.write_html(tables, plots_files, template=plotly_template, color_map=color_map)
    if open_graphs:
        subprocess.run(["xdg-open", plots_files])


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


def validate_tags(df):
    missing_tag_indices = tuple(df.collect()["tag1"].is_null().arg_true())
    if missing_tag_indices:
        raise ValueError(f"Missing tags at indices: {missing_tag_indices}")
