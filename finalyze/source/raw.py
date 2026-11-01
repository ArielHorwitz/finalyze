import polars as pl

from finalyze.config import host
from finalyze.source.identity import validate_unique_ids
from finalyze.source.schema import RAW_SCHEMA


def load_source_data():
    source_data = pl.concat(
        pl.read_csv(file, schema=RAW_SCHEMA)
        for file in host().ingested_dir.glob("*.csv")
    ).sort("date", "amount")
    validate_unique_ids(source_data)
    return source_data
