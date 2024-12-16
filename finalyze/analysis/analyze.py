import dataclasses
import functools
import operator
import subprocess
import tomllib
from typing import Optional

import arrow
import polars as pl

from finalyze.source.source import get_source_data
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


@dataclasses.dataclass
class Filters:
    start_date: Optional[str]
    end_date: Optional[str]
    tags: Optional[list[str]]
    subtags: Optional[list[str]]
    description: Optional[str]
    account: Optional[str]

    @staticmethod
    def configure_parser(parser):
        parser.add_argument(
            "-S",
            "--start-date",
            help="Filter since date (inclusive)",
        )
        parser.add_argument(
            "-E",
            "--end-date",
            help="Filter until date (non-inclusive)",
        )
        parser.add_argument(
            "-1",
            "--tags",
            nargs="*",
            help="Filter by tags",
        )
        parser.add_argument(
            "-2",
            "--subtags",
            nargs="*",
            help="Filter by subtags",
        )
        parser.add_argument(
            "-D",
            "--description",
            help="Filter by description (regex pattern)",
        )
        parser.add_argument(
            "-A",
            "--account",
            help="Filter by account name",
        )

    @classmethod
    def from_args(cls, args):
        return cls(
            **{
                field.name: getattr(args, field.name)
                for field in dataclasses.fields(cls)
            }
        )


@dataclasses.dataclass
class Args:
    lenient: bool
    open_graphs: bool
    print_tables: bool
    plotly_template: str
    filters: Filters

    @classmethod
    def configure_parser(cls, parser):
        parser.set_defaults(command_class=cls, run=run)
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
        Filters.configure_parser(parser.add_argument_group("filters"))

    @classmethod
    def from_args(cls, args):
        return cls(
            lenient=args.lenient,
            print_tables=args.print_tables,
            open_graphs=args.open_graphs,
            plotly_template=args.plotly_template,
            filters=Filters.from_args(args),
        )


def run(command_args, global_args):
    plots_files = global_args.data_dir / "plots.html"
    color_map_file = global_args.data_dir / "tag_colors.toml"
    if not color_map_file.is_file():
        color_map_file.write_text('other = "#000000"')
    color_map = tomllib.loads(color_map_file.read_text())
    # Source data
    source_data = get_source_data(global_args).sort("date", "amount")
    tagged_data = apply_tags(source_data, global_args.tags_file)
    if command_args.print_tables:
        print_table(tagged_data, "unfiltered source data")
    filtered_data = filter_data(tagged_data.lazy(), command_args.filters)
    source_data = filtered_data.select(*COLUMN_ORDER)
    if not command_args.lenient:
        validate_tags(source_data)
    if command_args.print_tables:
        print_table(source_data.collect(), "filtered source data")
    # Tables
    tables = get_tables(source_data)
    for table in tables:
        print(table)
        if command_args.print_tables:
            print_table(table.with_totals(), table.title)
    # Plots
    print(f"Exporting plots to: {plots_files=}")
    plot.write_html(
        tables,
        plots_files,
        template=command_args.plotly_template,
        color_map=color_map,
    )
    if command_args.open_graphs:
        subprocess.run(["xdg-open", plots_files])


def filter_data(df, filters):
    predicates = []
    # dates
    if filters.start_date is not None:
        predicates.append(pl.col("date").dt.date() >= _parse_date(filters.start_date))
    if filters.end_date is not None:
        predicates.append(pl.col("date").dt.date() < _parse_date(filters.end_date))
    # tags
    if filters.tags is not None:
        predicates.append(pl.col("tag1").is_in(pl.Series(filters.tags)))
    if filters.subtags is not None:
        predicates.append(pl.col("tag2").is_in(pl.Series(filters.subtags)))
    # patterns
    if filters.description is not None:
        predicates.append(pl.col("description").str.contains(filters.description))
    if filters.account is not None:
        predicates.append(pl.col("account") == filters.account)
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
