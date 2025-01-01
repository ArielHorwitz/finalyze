import subprocess
import sys

import polars as pl

from finalyze.display import print_table
from finalyze.source.data import enrich_source, load_source_data
from finalyze.source.tag import apply_tags

from . import plot
from .tables import get_tables


def run(config):
    source_data = load_source_data(config.general.source_dir)
    source_data = apply_tags(source_data, config.general.tags_file)
    source_data = enrich_source(source_data)
    source_data = config.analysis.filters.apply(source_data)
    if config.analysis.print_source:
        print_table(source_data, "Source data")
    if not config.analysis.allow_untagged:
        _validate_tags(source_data, flip_rtl=config.display.flip_rtl)
    # Tables
    tables = get_tables(source_data)
    if config.analysis.print_tables:
        for table in tables:
            print(table)
            print_table(table.with_totals(), table.title)
    # Plots
    plots_file = config.general.plots_file
    print(f"Exporting plots to: {plots_file}")
    plot.write_html(tables, config)
    if config.analysis.graphs.open:
        subprocess.run(["xdg-open", plots_file])


def _validate_tags(df, flip_rtl):
    null_tags = df.filter(pl.col("tag").is_null())
    if null_tags.height:
        print_table(null_tags, "Missing tags")
        print(f"Missing {null_tags.height} tags", file=sys.stderr)
        exit(1)
