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
    "account_source": pl.String,
    "balance_total": pl.Float64,
    "balance_account": pl.Float64,
    "balance_source": pl.Float64,
}


def load_source_data(source_dir):
    return pl.concat(
        pl.read_csv(file, schema=RAW_SCHEMA) for file in source_dir.glob("*.csv")
    ).sort("date", "amount")


def enrich_source(source):
    validate_schema(source, TAGGED_SCHEMA)
    year_str = pl.col("date").dt.year().cast(str)
    month_str = pl.col("date").dt.month().cast(str).str.pad_start(2, "0")
    month = year_str + "-" + month_str
    combined_tags = pl.col("tag") + " - " + pl.col("subtag")
    account_source = pl.col("account") + " - " + pl.col("source")
    source = source.with_columns(
        month.alias("month"),
        combined_tags.alias("tags"),
        account_source.alias("account_source"),
    )
    source = source.sort("date", "tags", "amount", "description", "hash").with_columns(
        pl.col("amount").cum_sum().alias("balance_total"),
        pl.col("amount").cum_sum().over("account").alias("balance_account"),
        pl.col("amount").cum_sum().over("account", "source").alias("balance_source"),
    )
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
