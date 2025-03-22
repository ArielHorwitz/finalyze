import polars as pl

from finalyze.config import config
from finalyze.source import HASH_COLUMNS, RAW_SCHEMA


def load_source_data():
    return pl.concat(
        pl.read_csv(file, schema=RAW_SCHEMA)
        for file in config().general.source_dir.glob("*.csv")
    ).sort("date", "amount")


def derive_hash(df):
    return df.with_columns(hash=pl.concat_str(HASH_COLUMNS).hash())
