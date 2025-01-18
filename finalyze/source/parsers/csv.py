import polars as pl

from finalyze.display import print_table
from finalyze.source.parsing import PARSED_SCHEMA, ParsingError, register_parser


def parse(input_file, config):
    if input_file.suffix != ".csv":
        raise ParsingError("Not a .csv file")
    try:
        df = pl.read_csv(input_file, schema=PARSED_SCHEMA)
    except Exception as exc:
        raise ParsingError(f"Failed to read .csv file: {exc}")
    if config.ingestion.verbose_parsing:
        print_table(df, f"raw csv of {input_file}")
    return df


register_parser("csv", parse)
