"""Parsing source data for ingestion.

## Implementing a new parser

To implement a new parser, follow these steps:
- Create a module under the `finalyze.source.parsers` directory
- Define a function with the `PARSING_FUNC` signature:
    * The path is the file to import
    * The return value is a polars DataFrame with `PARSED_SCHEMA`, and
      optionally the columns of `OPTIONAL_PARSED_SCHEMA`
    * Handle all errors by raising a `ParsingError`
- Register the function using `register_parser`
"""

import dataclasses
from collections.abc import Callable
from pathlib import Path
from typing import Optional

import polars as pl

from finalyze.source.schema import InvalidSchema, validate_schema

PARSING_FUNC = Callable[[Path], pl.DataFrame]
REGISTERED_PARSERS = {}
PARSED_SCHEMA = {
    "source": pl.String,
    "date": pl.Date,
    "amount": pl.Float64,
    "raw_description": pl.String,
}
OPTIONAL_PARSED_SCHEMA = {
    "id": pl.String,
}
"""A parser may provide native transaction IDs (stable and unique), otherwise
they are derived (see `finalyze.source.identity`)."""


class ParsingError(Exception):
    """Raised when there is a failure to importing source data by a parser."""


def register_parser(name: str, parser: PARSING_FUNC):
    if name in REGISTERED_PARSERS:
        raise KeyError(f"Parser by name '{name}' already registered")
    if not callable(parser):
        raise ValueError(f"Parser must be callable, got: {type(parser)}")
    REGISTERED_PARSERS[name] = parser


@dataclasses.dataclass(frozen=True)
class ParseAttempt:
    """The result of trying the registered parsers on a file, in order."""

    parser: Optional[str]
    """The first parser that accepted the file, if any."""
    data: Optional[pl.DataFrame]
    errors: dict[str, ParsingError]
    """Why each parser before it (or every parser) rejected the file."""


def parse_file(input_file: Path) -> pl.DataFrame:
    attempt = try_parsers(input_file)
    if attempt.data is not None:
        return attempt.data
    message_lines = [f"Failed to parse {input_file}"]
    message_lines.extend(f"{name}: {exc}" for name, exc in attempt.errors.items())
    raise ValueError("\n".join(message_lines))


def try_parsers(input_file: Path) -> ParseAttempt:
    parsing_errors = {}
    for name, parser in REGISTERED_PARSERS.items():
        try:
            df = parser(input_file)
        except ParsingError as exc:
            parsing_errors[name] = exc
            continue
        if not isinstance(df, pl.DataFrame):
            raise TypeError(
                f"Parser '{name}' returned: {type(df)}, expected: polars.DataFrame"
            )
        optional_schema = {
            column: dtype
            for column, dtype in OPTIONAL_PARSED_SCHEMA.items()
            if column in df.columns
        }
        try:
            validate_schema(df, PARSED_SCHEMA | optional_schema)
        except InvalidSchema as exc:
            raise InvalidSchema(f"Invalid schema from '{name}' parser") from exc
        missing_columns = {
            column: pl.lit(None, dtype=dtype)
            for column, dtype in OPTIONAL_PARSED_SCHEMA.items()
            if column not in df.columns
        }
        data = df.with_columns(**missing_columns).select(
            *PARSED_SCHEMA, *OPTIONAL_PARSED_SCHEMA
        )
        return ParseAttempt(parser=name, data=data, errors=parsing_errors)
    return ParseAttempt(parser=None, data=None, errors=parsing_errors)
