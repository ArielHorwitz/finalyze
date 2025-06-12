import itertools
import subprocess
import sys

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
    all_tables = get_tables(source_data)
    if config().analysis.print_tables:
        table_names = config().analysis.print_table_names
        for table in itertools.chain.from_iterable(all_tables.values()):
            if table_names and table.title not in table_names:
                continue
            print(table)
            print_table(table.source, table.title)
    # Plots
    top_title = config().analysis.graphs.title
    output_dirname = top_title.lower().replace(" ", "_")
    output_dir = config().general.output_dir / output_dirname
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Exporting plots to: {output_dir}")

    def write_html(title, content):
        file_stem = title.lower().replace(" ", "_")
        file_name = f"{file_stem}.html"
        output_file = output_dir / file_name
        output_file.write_text(content)
        return file_name

    links = {}
    for title, page_tables in all_tables.items():
        content = plot.plots_html(page_tables, title)
        output_file = write_html(title, content)
        links[title] = output_file

    source_table = source_data.get(round=True, include_external=True)
    source_table = _select_display_columns(source_table)
    source_table_file = write_html(
        "Source data",
        plot.table_html(source_table, "Source data"),
    )
    links["Source data"] = source_table_file

    index_content = plot.index_html(links, top_title)
    index_file_name = write_html("index", index_content)

    if config().analysis.graphs.open:
        subprocess.run(["xdg-open", output_dir / index_file_name])


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
