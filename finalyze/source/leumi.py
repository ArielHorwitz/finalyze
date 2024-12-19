import math

import pandas as pd
import polars as pl


class UnexpectedFormat(Exception):
    """Raised when encountering unexpected format while importing source data."""


def parse_file(input_file):
    try:
        return BalanceFormat.parse(input_file)
    except UnexpectedFormat as exc:
        balance_error = exc
    try:
        return CreditFormat.parse(input_file)
    except UnexpectedFormat as exc:
        credit_error = exc
    raise UnexpectedFormat(
        f"{input_file} not balance: {balance_error!r}, not credit: {credit_error!r}"
    )


class BalanceFormat:
    EXPECTED_HEADER = "תנועות בחשבון"
    EXPECTED_FIRST_COLUMN_NAME = "תאריך"
    CREDIT_CARD_DESCRIPTION = "לאומי ויזה"

    @classmethod
    def check(cls, raw_df):
        if (header := raw_df.iloc[0, 0]) != cls.EXPECTED_HEADER:
            raise UnexpectedFormat(
                f"Expected header {cls.EXPECTED_HEADER!r}, got: {header!r}"
            )
        if (first_column := raw_df.iloc[1, 0]) != cls.EXPECTED_FIRST_COLUMN_NAME:
            raise UnexpectedFormat(
                f"Expected column name {cls.EXPECTED_FIRST_COLUMN_NAME!r}"
                f", got: {first_column!r}"
            )

    @classmethod
    def parse(cls, input_file):
        raw_html = pd.read_html(input_file, encoding="utf-8")
        raw_df = raw_html[2]
        # Remove leading asterisks from otherwise valid data (rows from today)
        raw_df[0] = raw_df[0].str.replace("** ", "", regex=False)
        cls.check(raw_df)
        if raw_df.iloc[-1, 0].startswith("**"):
            raw_df = raw_df.iloc[:-1]
        raw = pl.from_pandas(raw_df.iloc[2:])
        data = {
            "date": raw.select(pl.col("0").str.strptime(pl.Date, format="%d/%m/%y")),
            "amount": raw.select(
                pl.col("5").cast(pl.Float64) - pl.col("4").cast(pl.Float64)
            ),
            "description": raw.select(pl.col("2").cast(pl.String)),
            # "notes": raw.select(pl.col("7").cast(pl.String)),
        }
        parsed = pl.DataFrame(data).with_columns(pl.lit("Account txn").alias("source"))
        # remove direct credit card transactions
        filtered = parsed.filter(pl.col("description") != cls.CREDIT_CARD_DESCRIPTION)
        return filtered


class CreditFormat:
    EXPECTED_HEADERS = (
        'עסקאות מחויבות בש"ח (לידיעה בלבד)',
        'עסקאות בש"ח במועד החיוב',
    )
    EXPECTED_FIRST_COLUMN_NAME = "תאריך העסקה"
    EXPECTED_LAST_ROW_VALUE = 'סה"כ:'

    @classmethod
    def check(cls, raw_df):
        if (header := raw_df.iloc[0, 0]) not in cls.EXPECTED_HEADERS:
            raise UnexpectedFormat(
                f"Expected header {cls.EXPECTED_HEADERS!r}" f", got: {header!r}"
            )
        if (first_column := raw_df.iloc[1, 0]) != cls.EXPECTED_FIRST_COLUMN_NAME:
            raise UnexpectedFormat(
                f"Expected column name {cls.EXPECTED_FIRST_COLUMN_NAME!r}"
                f", got: {first_column!r}"
            )
        if (last_row_value := raw_df.iloc[-1, 4]) != cls.EXPECTED_LAST_ROW_VALUE:
            raise UnexpectedFormat(
                f"Expected {cls.EXPECTED_LAST_ROW_VALUE!r} in last row"
                f", got: {last_row_value!r}"
            )
        if not math.isnan(expected_nan := raw_df.iloc[-1, 0]):
            raise UnexpectedFormat(f"Expected nan, got: {expected_nan!r}")

    @classmethod
    def parse(cls, input_file):
        raw_html = pd.read_html(input_file, encoding="utf-8")
        raw_tables = [
            cls._table_parse(raw_html[2]).with_columns(
                pl.lit("Credit card").alias("source")
            )
        ]
        if len(raw_html) >= 4:
            debit_table = cls._table_parse(raw_html[3]).with_columns(
                pl.lit("Debit").alias("source")
            )
            raw_tables.append(debit_table)
        return pl.concat(raw_tables)

    @classmethod
    def _table_parse(cls, raw_df):
        cls.check(raw_df)
        table = pl.from_pandas(raw_df.iloc[2:-1])
        parsed = pl.DataFrame(
            {
                "date": table.select(
                    pl.col("0").str.strptime(pl.Date, format="%d/%m/%y")
                ),
                "amount": table.select(pl.col("5").cast(pl.Float64).mul(-1)),
                "description": table.select(pl.col("1").cast(pl.String)),
                # "notes": table.select(pl.col("4").cast(pl.String)),
            }
        )
        return parsed
