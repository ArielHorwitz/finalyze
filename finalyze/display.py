import string

import pandas as pd
import polars as pl

from finalyze.config import config

ENGLISH = frozenset(string.printable)


def print_table(table, description: str = "Unnamed table"):
    print()
    print(description)

    if isinstance(table, pl.DataFrame):
        if config().display.flip_rtl:
            table = flip_rtl_columns(table)
        pl_options = {
            "tbl_rows": config().display.max_rows,
            "tbl_cols": config().display.max_cols,
            "tbl_width_chars": config().display.max_width,
            "tbl_hide_dataframe_shape": not config().display.show_shape,
        }
        with pl.Config(**pl_options):
            print(table)

    elif isinstance(table, pd.DataFrame):
        pd_options = (
            "display.max_rows",
            config().display.max_rows,
            "display.max_columns",
            config().display.max_cols,
            "display.width",
            config().display.max_width,
        )
        with pd.option_context(*pd_options):
            print(table)

    else:
        raise TypeError(f"Unknown table type: {type(table)}")

    print()


def flip_rtl_columns(df):
    schema = df.collect_schema()
    dtypes = dict(zip(schema.names(), schema.dtypes()))
    return df.with_columns(
        pl.col(name).map_elements(flip_rtl_str, return_dtype=pl.String)
        for name, dtype in dtypes.items()
        if dtype == pl.String
    )


def round_columns(df):
    decimals = config().analysis.rounding_decimals
    if decimals < 0:
        return df
    schema = df.collect_schema()
    return df.with_columns(
        pl.col(name).round(decimals)
        for name, dtype in zip(schema.names(), schema.dtypes())
        if dtype.is_float()
    )


def flip_rtl_str(text):
    if set(text) - ENGLISH:
        return text[::-1]
    return text
