"""Stable transaction IDs.

A transaction's ID is derived from its canonical fields, plus an occurrence
index that tells identical rows apart (e.g. two identical purchases on the same
day). A parser may provide native IDs instead, which are kept as is.

IDs are of the parsed rows: ingestion rules that change a row keep its ID, and
the row a transfer rule adds has an ID derived from its original's (see
`finalyze.source.ingestion_rules`).
"""

import hashlib

import polars as pl

from finalyze.money import minor_units_expr

ID_FIELDS = ("account", "source", "date", "raw_description", "amount")
ID_LENGTH = 16
FIELD_SEPARATOR = "\x1f"
NULL_FIELD = "\x00"


class DuplicateIds(Exception):
    """Raised when transaction IDs are not unique."""


def derive_ids(df: pl.DataFrame) -> pl.DataFrame:
    """Fill missing values of the `id` column (added if absent) with derived IDs."""
    if "id" not in df.columns:
        df = df.with_columns(id=pl.lit(None, dtype=pl.String))
    canonical_fields = [
        pl.col("account"),
        pl.col("source"),
        pl.col("date").dt.to_string("%Y-%m-%d"),
        pl.col("raw_description"),
        minor_units_expr("amount").cast(pl.String),
        pl.int_range(pl.len()).over(ID_FIELDS).cast(pl.String),
    ]
    canonical = pl.concat_str(
        [field.cast(pl.String).fill_null(NULL_FIELD) for field in canonical_fields],
        separator=FIELD_SEPARATOR,
    )
    derived = canonical.map_elements(hash_canonical, return_dtype=pl.String)
    return df.with_columns(pl.col("id").fill_null(derived))


def transfer_leg_id_expr() -> pl.Expr:
    """The ID of the row a transfer rule adds, from the ID of the row it mirrors."""
    canonical = pl.concat_str(
        [pl.col("id"), pl.lit("transfer")], separator=FIELD_SEPARATOR
    )
    return canonical.map_elements(hash_canonical, return_dtype=pl.String)


def hash_canonical(canonical: str) -> str:
    return hashlib.sha256(canonical.encode()).hexdigest()[:ID_LENGTH]


def validate_unique_ids(df: pl.DataFrame):
    duplicates = df.filter(pl.col("id").is_duplicated())
    if duplicates.height:
        raise DuplicateIds(f"Transaction IDs are not unique:\n{duplicates}")
