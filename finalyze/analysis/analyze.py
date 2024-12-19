import dataclasses
import subprocess
import tomllib

from finalyze.display import print_table
from finalyze.filters import Filters
from finalyze.source.source import load_source_data
from finalyze.tags.tagger import apply_tags

from . import plot
from .tables import get_tables

COLUMN_ORDER = (
    "account",
    "source",
    "date",
    "amount",
    "tag",
    "subtag",
    "description",
    "hash",
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
        Filters.configure_parser(parser)

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
    # Source data
    source_data = load_source_data(global_args.source_dir).sort("date", "amount")
    tagged_data = apply_tags(source_data, global_args.tags_file)
    filtered_data = command_args.filters.filter_data(tagged_data.lazy())
    source_data = filtered_data.select(*COLUMN_ORDER)
    if not command_args.lenient:
        validate_tags(source_data)
    if command_args.print_tables:
        print_table(
            source_data.collect(),
            "filtered source data",
            flip_rtl=global_args.flip_rtl,
        )
    # Tables
    tables = get_tables(source_data)
    for table in tables:
        print(table)
        if command_args.print_tables:
            print_table(table.with_totals(), table.title, flip_rtl=global_args.flip_rtl)
    # Plots
    plots_files = global_args.plots_file
    color_map = load_colors(global_args.colors_file)
    print(f"Exporting plots to: {plots_files}")
    plot.write_html(
        tables,
        plots_files,
        template=command_args.plotly_template,
        color_map=color_map,
    )
    if command_args.open_graphs:
        subprocess.run(["xdg-open", plots_files])


def validate_tags(df):
    missing_tag_indices = tuple(df.collect()["tag"].is_null().arg_true())
    if missing_tag_indices:
        raise ValueError(f"Missing tags at indices: {missing_tag_indices}")


def load_colors(colors_file):
    if not colors_file.is_file():
        colors_file.write_text('other = "#000000"')
    return tomllib.loads(colors_file.read_text())
