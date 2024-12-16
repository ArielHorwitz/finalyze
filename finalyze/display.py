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
    "tbl_width_chars": 1_000,
    "tbl_hide_dataframe_shape": True,
}


def print_table(table, description: str = "Unnamed table", enable: bool = True):
    if not enable:
        return
    print()
    print(description)
    with pl.Config(**PL_OPTIONS):
        with pd.option_context(*PD_OPTIONS):
            print(table)
    print()


def flip_rtl_column(df, column_name):
    fixed_column = pl.col(column_name).map_elements(
        lambda text: text[::-1] if set(text) - ENGLISH else text,
        return_dtype=pl.String,
    )
    return df.with_columns(fixed_column)
