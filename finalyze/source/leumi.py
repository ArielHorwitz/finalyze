import math

import pandas as pd
import polars as pl

from finalyze.display import print_table


class UnexpectedFormat(Exception):
    """Raised when encountering unexpected format while importing source data."""


def parse_file(input_file, config):
    if config.ingestion.verbose_parsing:
        raw_tables = pd.read_html(input_file, encoding="utf-8")
        for i, table in enumerate(raw_tables):
            print_table(table, f"Raw table {i} for file: {input_file}")
    try:
        return CheckingFormat.parse(input_file, config)
    except UnexpectedFormat as exc:
        checking_error = exc
    try:
        return CardFormat.parse(input_file, config)
    except UnexpectedFormat as exc:
        card_error = exc
    raise UnexpectedFormat(
        f"{input_file} not checking: {checking_error!r}, not card: {card_error!r}"
    )


class CheckingFormat:
    EXPECTED_HEADER = "תנועות בחשבון"
    EXPECTED_FIRST_COLUMN_NAME = "תאריך"
    CARD_DESCRIPTION = "לאומי ויזה"

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
    def parse(cls, input_file, config):
        raw_html = pd.read_html(input_file, encoding="utf-8")
        raw_df = raw_html[2]
        cls.check(raw_df)
        if raw_df.iloc[-1, 0].startswith("**"):
            # Data contains footnotes
            # Remove leading asterisks from otherwise valid data
            raw_df[0] = raw_df[0].str.replace("** ", "", regex=False)
            # Remove last row containing footnote description
            raw_df = raw_df.iloc[:-1]
        # Remove headers
        raw_df = raw_df.iloc[2:]
        # Convert to polars
        raw = pl.from_pandas(raw_df)
        # Compile data
        amount_expression = pl.col("5").cast(pl.Float64) - pl.col("4").cast(pl.Float64)
        data = {
            "date": raw.select(pl.col("0").str.strptime(pl.Date, format="%d/%m/%y")),
            "amount": raw.select(amount_expression),
            "description": raw.select(pl.col("2").cast(pl.String)),
        }
        checking = pl.DataFrame(data).with_columns(pl.lit("checking").alias("source"))
        is_card_transaction = pl.col("description") == cls.CARD_DESCRIPTION
        if config.ingestion.card_transactions == "remove":
            checking = checking.filter(~is_card_transaction)
        elif config.ingestion.card_transactions == "balance":
            # Extract card transfers
            card_transfers = checking.filter(is_card_transaction)
            checking = checking.filter(~is_card_transaction)
            # Add two-way transfers
            card_transfers_to = card_transfers.with_columns(
                source=pl.lit("checking"),
                description=pl.lit("Transfer checking-card"),
            )
            card_transfers_from = card_transfers.with_columns(
                source=pl.lit("card"),
                description=pl.lit("Transfer checking-card"),
                amount=pl.col("amount") * -1,
            )
            checking = pl.concat([checking, card_transfers_to, card_transfers_from])
        return checking


class CardFormat:
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
    def parse(cls, input_file, config):
        raw_html = pd.read_html(input_file, encoding="utf-8")
        # Credit card transactions
        raw_tables = [cls._table_parse(raw_html[2])]
        if len(raw_html) >= 4:
            # Debit card transactions on credit card
            debit_table = cls._table_parse(raw_html[3])
            raw_tables.append(debit_table)
        return pl.concat(raw_tables)

    @classmethod
    def _table_parse(cls, raw_df):
        cls.check(raw_df)
        # Remove headers and total rows
        table = pl.from_pandas(raw_df.iloc[2:-1])
        # Compile data
        data = {
            "date": table.select(pl.col("0").str.strptime(pl.Date, format="%d/%m/%y")),
            "amount": table.select(pl.col("5").cast(pl.Float64).mul(-1)),
            "description": table.select(pl.col("1").cast(pl.String)),
        }
        return pl.DataFrame(data).with_columns(pl.lit("card").alias("source"))
