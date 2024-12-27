import polars as pl


class InvalidSchema(Exception):
    """Raised when a schema is invalid."""


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
}


def load_source_data(source_dir):
    return pl.concat(
        pl.read_csv(file, schema=RAW_SCHEMA) for file in source_dir.glob("*.csv")
    ).sort("date", "amount")


def enrich_source(source):
    validate_schema(source, TAGGED_SCHEMA)
    # Month column
    year = pl.col("date").dt.year().cast(str)
    month = pl.col("date").dt.month().cast(str).str.pad_start(2, "0")
    source = source.with_columns((year + "-" + month).alias("month"))
    # Combined tags column
    combined_tags = pl.col("tag") + " - " + pl.col("subtag")
    source = source.with_columns(combined_tags.alias("tags"))
    validate_schema(source, ENRICHED_SCHEMA)
    return source


def validate_schema(df, expected_schema):
    collected_schema = df.collect_schema()
    actual_schema = dict(zip(collected_schema.names(), collected_schema.dtypes()))
    errors = []
    if missing_columns := set(expected_schema.keys()) - set(actual_schema.keys()):
        errors.append(f"Missing columns: {missing_columns}")
    if extra_columns := set(actual_schema.keys()) - set(expected_schema.keys()):
        errors.append(f"Extra columns: {extra_columns}")
    for column, expected_dtype in expected_schema.items():
        actual_dtype = actual_schema[column]
        if actual_dtype != expected_dtype:
            errors.append(
                f"Column {column!r}"
                f" [dtype: {actual_dtype} expected: {expected_dtype}]"
            )
    if errors:
        raise InvalidSchema(", ".join(errors))
