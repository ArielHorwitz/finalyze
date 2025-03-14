import polars as pl

from finalyze.config import config


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
    "external": pl.Boolean,
    "account_source": pl.String,
    "balance_total": pl.Float64,
    "balance_inexternal": pl.Float64,
    "balance_account": pl.Float64,
    "balance_source": pl.Float64,
}


def load_source_data():
    return pl.concat(
        pl.read_csv(file, schema=RAW_SCHEMA)
        for file in config().general.source_dir.glob("*.csv")
    ).sort("date", "amount")


def derive_month(df):
    year_str = pl.col("date").dt.year().cast(str)
    month_str = pl.col("date").dt.month().cast(str).str.pad_start(2, "0")
    month = year_str + "-" + month_str
    return df.with_columns(month=month)


def derive_tags(df):
    delimiter = config().general.multi_column_delimiter
    tags = pl.col("tag") + delimiter + pl.col("subtag")
    return df.with_columns(tags=tags)


def derive_account_source(df):
    delimiter = config().general.multi_column_delimiter
    account_source = pl.col("account") + delimiter + pl.col("source")
    return df.with_columns(account_source=account_source)


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
                f"Column {column!r}"
                f" [dtype: {actual_dtype} expected: {expected_dtype}]"
            )
    if errors:
        raise InvalidSchema(", ".join(errors))
