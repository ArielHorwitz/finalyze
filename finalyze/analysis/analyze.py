import subprocess
import sys
from pathlib import Path

import polars as pl
import polars.exceptions

from finalyze.analysis import plot
from finalyze.analysis.tables import get_tables
from finalyze.config import config
from finalyze.display import print_table
from finalyze.source.data import SourceData


def run():
    source_data = SourceData.load()
    if config().analysis.print_source:
        print_table(source_data.get(), "Source data")
    if not config().analysis.allow_untagged:
        _validate_tags(source_data.get())
    # Tables
    tables = get_tables(source_data)
    if config().analysis.print_tables:
        table_names = config().analysis.print_table_names
        for table in tables:
            if table_names and table.title not in table_names:
                continue
            print(table)
            print_table(table.source, table.title)
    # Plots
    output_file_stem = config().analysis.graphs.title.lower().replace(" ", "_")
    output_file = config().general.output_dir / f"{output_file_stem}.html"
    print(f"Exporting plots to: {output_file}")
    display_table = source_data.get(round=True, include_external=True)
    html_text = plot.get_html(_select_display_columns(display_table), tables)
    Path(output_file).write_text(html_text)
    if config().analysis.graphs.open:
        subprocess.run(["xdg-open", output_file])


def _validate_tags(df):
    null_tags = df.filter(pl.col("tag").is_null())
    if null_tags.height:
        print_table(null_tags, "Missing tags")
        print(f"Missing {null_tags.height} tags", file=sys.stderr)
        exit(1)


def _select_display_columns(source_data):
    display_columns = config().analysis.source_table_columns
    if not display_columns:
        return source_data
    try:
        return source_data.select(display_columns)
    except polars.exceptions.ColumnNotFoundError as e:
        raise ValueError(
            "Available columns for analysis.source_table_columns config: "
            f"{source_data.columns}"
        ) from e
