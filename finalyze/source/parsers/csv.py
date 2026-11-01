import polars as pl

from finalyze.source.parsing import PARSED_SCHEMA, ParsingError, register_parser

LEGACY_HEADERS = {"description": "raw_description"}
"""Old headers, still accepted as their new names (hand-written files keep them)."""


def parse(input_file):
    if input_file.suffix != ".csv":
        raise ParsingError("Not a .csv file")
    if input_file.stat().st_size == 0:
        # E.g. a placeholder for an account without transactions yet
        return pl.DataFrame(schema=PARSED_SCHEMA)
    try:
        raw = pl.read_csv(input_file, infer_schema=False)
    except Exception as exc:
        raise ParsingError(f"Failed to read .csv file: {exc}") from exc
    raw = raw.rename(
        {
            old: new
            for old, new in LEGACY_HEADERS.items()
            if old in raw.columns and new not in raw.columns
        }
    )
    missing_columns = PARSED_SCHEMA.keys() - set(raw.columns)
    extra_columns = set(raw.columns) - PARSED_SCHEMA.keys()
    if missing_columns or extra_columns:
        raise ParsingError(
            f"Expected columns {list(PARSED_SCHEMA)}, "
            f"missing: {sorted(missing_columns)}, extra: {sorted(extra_columns)}"
        )
    try:
        df = raw.select(
            pl.col(name).cast(dtype) for name, dtype in PARSED_SCHEMA.items()
        )
    except pl.exceptions.PolarsError as exc:
        raise ParsingError(f"Invalid values in .csv file: {exc}") from exc
    return df


register_parser("csv", parse)
