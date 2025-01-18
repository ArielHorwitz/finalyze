"""Parsing source data for ingestion.

## Implementing a new parser

To implement a new parser, follow these steps:
- Create a module under the `finalyze.source.parsers` directory
- Define a function with the `PARSING_FUNC` signature:
    * The path is the file to import
    * The return value is a polars DataFrame with `PARSED_SCHEMA`
    * Handle all errors by raising a `ParsingError`
- Register the function using `register_parser`
"""

from pathlib import Path
from typing import Callable

import polars as pl

from finalyze.config import Config
from finalyze.source.data import InvalidSchema, validate_schema

PARSING_FUNC = Callable[[Path, Config], pl.DataFrame]
REGISTERED_PARSERS = {}
PARSED_SCHEMA = {
    "source": pl.String,
    "date": pl.Date,
    "amount": pl.Float64,
    "description": pl.String,
}


class ParsingError(Exception):
    """Raised when there is a failure to importing source data by a parser."""


def register_parser(name: str, parser: PARSING_FUNC):
    if name in REGISTERED_PARSERS:
        raise KeyError(f"Parser by name '{name}' already registered")
    if not callable(parser):
        raise ValueError(f"Parser must be callable, got: {type(parser)}")
    REGISTERED_PARSERS[name] = parser


def parse_file(input_file: Path, config: Config):
    parsing_errors = {}
    for name, parser in REGISTERED_PARSERS.items():
        try:
            df = parser(input_file, config)
        except ParsingError as exc:
            parsing_errors[name] = exc
            continue
        if not isinstance(df, pl.DataFrame):
            raise TypeError(
                f"Parser '{name}' returned: {type(df)}, expected: polars.DataFrame"
            )
        try:
            validate_schema(df, PARSED_SCHEMA)
        except InvalidSchema as exc:
            raise InvalidSchema(f"Invalid schema from '{name}' parser") from exc
        return df
    message_lines = [f"Failed to parse {input_file}"]
    message_lines.extend(f"{name}: {exc}" for name, exc in parsing_errors.items())
    raise ValueError("\n".join(message_lines))
