import math

import pandas as pd
import polars as pl

from finalyze.config import config
from finalyze.display import print_table
from finalyze.source.parsing import ParsingError, register_parser

RAW_PRINT_FLAGS = {}


def _print_raw(input_file):
    if RAW_PRINT_FLAGS.get(input_file):
        return
    if config().ingestion.verbose_parsing:
        raw_tables = pd.read_html(input_file, encoding="utf-8")
        for i, table in enumerate(raw_tables):
            print_table(table, f"Raw table {i} for file: {input_file}")
    RAW_PRINT_FLAGS[input_file] = True


class CheckingFormat:
    EXPECTED_HEADER = "תנועות בחשבון"
    EXPECTED_FIRST_COLUMN_NAME = "תאריך"
    CARD_DESCRIPTION = "לאומי ויזה"

    @classmethod
    def check(cls, raw_df):
        if (header := raw_df.iloc[0, 0]) != cls.EXPECTED_HEADER:
            raise ParsingError(
                f"Expected header {cls.EXPECTED_HEADER!r}, got: {header!r}"
            )
        if (first_column := raw_df.iloc[1, 0]) != cls.EXPECTED_FIRST_COLUMN_NAME:
            raise ParsingError(
                f"Expected column name {cls.EXPECTED_FIRST_COLUMN_NAME!r}"
                f", got: {first_column!r}"
            )

    @classmethod
    def parse(cls, input_file):
        if input_file.suffix != ".xls":
            raise ParsingError("Not a .xls file")
        _print_raw(input_file)
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
        try:
            date_data = raw.select(pl.col("0").str.strptime(pl.Date, format="%d/%m/%y"))
        except Exception:
            date_data = raw.select(pl.col("0").str.strptime(pl.Date, format="%d/%m/%Y"))
        data = {
            "date": date_data,
            "amount": raw.select(amount_expression),
            "description": raw.select(pl.col("2").cast(pl.String)),
        }
        checking = pl.DataFrame(data).with_columns(pl.lit("checking").alias("source"))
        is_card_transaction = pl.col("description") == cls.CARD_DESCRIPTION
        if config().ingestion.card_transactions == "remove":
            checking = checking.filter(~is_card_transaction)
        elif config().ingestion.card_transactions == "balance":
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
    UNCONFIRMED_TITLE = "עסקאות אחרונות שטרם נקלטו"
    TITLES = {
        'עסקאות בש"ח במועד החיוב',
        'עסקאות מחויבות בש"ח (לידיעה בלבד)',
    }
    HEADERS = (
        "תאריך העסקה",
        "שם בית העסק",
        "סכום העסקה",
        "סוג העסקה",
        "פרטים",
        "סכום חיוב",
    )
    TOTALS_NAME = 'סה"כ:'

    @classmethod
    def parse(cls, input_file):
        if input_file.suffix != ".xls":
            raise ParsingError("Not a .xls file")
        _print_raw(input_file)
        raw_tables = pd.read_html(input_file, encoding="utf-8")
        parsed_tables = []
        for raw_table in raw_tables:
            title = raw_table.iloc[0, 0]
            if title not in cls.TITLES:
                continue
            cls.check(raw_table, title)
            parsed_table = cls._table_parse(raw_table)
            parsed_tables.append(parsed_table)
        if not parsed_tables:
            raise ParsingError("No valid tables found")
        return pl.concat(parsed_tables)

    @classmethod
    def check(cls, raw_df, title):
        # Check title row
        for column_index in range(raw_df.shape[1]):
            actual_title = raw_df.iloc[0, column_index]
            if actual_title != title:
                raise ParsingError(f"Expected title {title!r}, got: {actual_title!r}")
        # Check headers row
        for column_index, expected_header in enumerate(cls.HEADERS):
            actual_header = raw_df.iloc[1, column_index]
            if actual_header != expected_header:
                raise ParsingError(
                    f"Expected header {expected_header!r}, got: {actual_header!r}"
                )
        # Check totals row
        actual_totals_name = raw_df.iloc[-1, 4]
        if actual_totals_name != cls.TOTALS_NAME:
            raise ParsingError(
                f"Expected {cls.TOTALS_NAME!r} in last row, got: {actual_totals_name!r}"
            )
        for column_index in range(4):
            expected_nan = raw_df.iloc[-1, column_index]
            if not math.isnan(expected_nan):
                raise ParsingError(f"Expected nan, got: {expected_nan!r}")

    @classmethod
    def _table_parse(cls, raw_df):
        # Remove titles, headers, and totals rows
        table = pl.from_pandas(raw_df.iloc[2:-1])
        # Compile data
        data = {
            "date": table.select(pl.col("0").str.strptime(pl.Date, format="%d/%m/%y")),
            "amount": table.select(pl.col("5").cast(pl.Float64).mul(-1)),
            "description": table.select(pl.col("1").cast(pl.String)),
        }
        return pl.DataFrame(data).with_columns(pl.lit("card").alias("source"))


register_parser("leumi checking", CheckingFormat.parse)
register_parser("leumi card", CardFormat.parse)
