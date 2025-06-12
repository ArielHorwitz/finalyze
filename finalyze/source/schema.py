import polars as pl

HASH_COLUMNS = ("account", "source", "date", "description", "amount")
RAW_SCHEMA = {
    "account": pl.String,
    "source": pl.String,
    "date": pl.Date,
    "amount": pl.Float64,
    "description": pl.String,
}
TAGGED_SCHEMA = {
    **RAW_SCHEMA,
    "tag": pl.String,
    "subtag": pl.String,
    "hash": pl.UInt64,
}
ENRICHED_SCHEMA = {
    **TAGGED_SCHEMA,
    "month": pl.String,
    "tags": pl.String,
    "is_edge_tick": pl.Boolean,
    "is_sentinel_tick": pl.Boolean,
    "is_breakdown": pl.Boolean,
    "is_external": pl.Boolean,
    "account_source": pl.String,
    "balance_total": pl.Float64,
    "balance_inexternal": pl.Float64,
    "balance_account": pl.Float64,
    "balance_source": pl.Float64,
}


class InvalidSchema(Exception):
    """Raised when a schema is invalid."""


def validate_schema(df, expected_schema):
    collected_schema = df.collect_schema()
    actual_schema = dict(zip(collected_schema.names(), collected_schema.dtypes()))
    errors = []
    if missing_columns := set(expected_schema.keys()) - set(actual_schema.keys()):
        errors.append(f"Missing columns: {missing_columns}")
    if extra_columns := set(actual_schema.keys()) - set(expected_schema.keys()):
        errors.append(f"Extra columns: {extra_columns}")
    for column, expected_dtype in expected_schema.items():
        if column not in actual_schema:
            errors.append(f"Missing column: {column}")
            continue
        actual_dtype = actual_schema[column]
        if actual_dtype != expected_dtype:
            errors.append(
                f"Column {column!r} [dtype: {actual_dtype} expected: {expected_dtype}]"
            )
    if errors:
        raise InvalidSchema(", ".join(errors))
