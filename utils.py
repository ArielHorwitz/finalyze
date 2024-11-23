import string

import pandas as pd
import polars as pl

ENGLISH = frozenset(string.printable)
PD_OPTIONS = (
    "display.max_rows",
    1_000_000,
    "display.max_columns",
    1_000,
    "display.width",
    1_000,
)
PL_OPTIONS = {
    "tbl_rows": 1_000_000,
    "tbl_cols": 1_000,
}


def print_table(table, description, enable):
    if not enable:
        return
    print(f"Showing table: {description}")
    with pl.Config(**PL_OPTIONS):
        with pd.option_context(*PD_OPTIONS):
            print(table)


def flip_rtl(text):
    if set(text) - ENGLISH:
        return text[::-1]
    return text


def flip_rtl_column(df, column_name):
    fixed_column = pl.col(column_name).map_elements(
        lambda x: flip_rtl(x),
        return_dtype=pl.String,
    )
    return df.with_columns(fixed_column)
