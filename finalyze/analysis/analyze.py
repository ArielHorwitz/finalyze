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
        print_table(source_data, "Source data", flip_rtl=config.general.flip_rtl)
    if not config.analysis.allow_untagged:
        _validate_tags(source_data, flip_rtl=config.general.flip_rtl)
    analyze(source_data, config)


def analyze(source_data, config):
    # Tables
    tables = get_tables(source_data)
    for table in tables:
        if config.analysis.print_tables:
            print(table)
            print_table(
                table.with_totals(), table.title, flip_rtl=config.general.flip_rtl
            )
    # Plots
    plots_files = config.general.plots_file
    color_map = {name: color.as_hex() for name, color in config.analysis.colors.items()}
    print(f"Exporting plots to: {plots_files}")
    plot.write_html(
        tables,
        plots_files,
        template=config.analysis.plotly_template,
        color_map=color_map,
    )
    if config.analysis.open_graphs:
        subprocess.run(["xdg-open", plots_files])


def _validate_tags(df, flip_rtl):
    null_tags = df.filter(pl.col("tag").is_null())
    if null_tags.height:
        print_table(null_tags, "Missing tags", flip_rtl=flip_rtl)
        print(f"Missing {null_tags.height} tags", file=sys.stderr)
        exit(1)
