import polars as pl

RAW_SCHEMA = {
    "id": pl.String,
    "account": pl.String,
    "source": pl.String,
    "date": pl.Date,
    "amount": pl.Float64,
    "raw_description": pl.String,
}
ANNOTATION_SCHEMA = {
    "tag": pl.String,
    "subtag": pl.String,
    "description": pl.String,
    "transaction_description": pl.String,
}
TAGGED_SCHEMA = {
    **RAW_SCHEMA,
    **ANNOTATION_SCHEMA,
    "part_kind": pl.String,
}
"""`part_kind`: the kind set on the transaction (its annotation's part), if any."""
ENRICHED_SCHEMA = {
    **RAW_SCHEMA,
    **ANNOTATION_SCHEMA,
    "month": pl.String,
    "tags": pl.String,
    "is_sentinel_tick": pl.Boolean,
    "kind": pl.String,
    "kind_set": pl.Boolean,
    "group": pl.String,
    "group_role": pl.String,
    "account_source": pl.String,
    "balance_total": pl.Float64,
    "balance_role": pl.Float64,
    "balance_group": pl.Float64,
    "balance_account": pl.Float64,
    "balance_source": pl.Float64,
}


class InvalidSchema(Exception):
    """Raised when a schema is invalid."""


def validate_schema(df, expected_schema):
    collected_schema = df.collect_schema()
    actual_schema = dict(collected_schema)
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
