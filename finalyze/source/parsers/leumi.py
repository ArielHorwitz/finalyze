import pandas as pd
import polars as pl

from finalyze.source.parsing import ParsingError, register_parser


def _read_html_tables(input_file):
    if input_file.suffix != ".xls":
        raise ParsingError("Not a .xls file")
    try:
        raw_tables = pd.read_html(input_file, encoding="utf-8")
    except Exception as exc:
        raise ParsingError(f"Failed to read HTML tables: {exc}") from exc
    return raw_tables


def _require_dates(df):
    if (missing := df["date"].null_count()) > 0:
        raise ParsingError(f"Found {missing} rows without a date")


class CheckingFormat:
    EXPECTED_HEADER = "תנועות בחשבון"
    EXPECTED_FIRST_COLUMN_NAME = "תאריך"
    COLUMN_COUNT = 6

    @classmethod
    def check(cls, raw_df):
        if raw_df.empty:
            raise ParsingError("Expected header, got an empty table")
        if (header := raw_df.iloc[0, 0]) != cls.EXPECTED_HEADER:
            raise ParsingError(
                f"Expected header {cls.EXPECTED_HEADER!r}, got: {header!r}"
            )
        if raw_df.shape[0] < 2 or raw_df.shape[1] < cls.COLUMN_COUNT:
            raise ParsingError(f"Unexpected table shape: {raw_df.shape}")
        if (first_column := raw_df.iloc[1, 0]) != cls.EXPECTED_FIRST_COLUMN_NAME:
            raise ParsingError(
                f"Expected column name {cls.EXPECTED_FIRST_COLUMN_NAME!r}"
                f", got: {first_column!r}"
            )

    @classmethod
    def parse(cls, input_file):
        raw_tables = _read_html_tables(input_file)
        if len(raw_tables) < 3:
            raise ParsingError(f"Expected at least 3 tables, got: {len(raw_tables)}")
        raw_df = raw_tables[2]
        cls.check(raw_df)
        last_first_cell = raw_df.iloc[-1, 0]
        if isinstance(last_first_cell, str) and last_first_cell.startswith("**"):
            # Data contains footnotes
            # Remove leading asterisks from otherwise valid data
            raw_df[0] = raw_df[0].str.replace("** ", "", regex=False)
            # Remove last row containing footnote description
            raw_df = raw_df.iloc[:-1]
        # Remove headers
        raw_df = raw_df.iloc[2:]
        try:
            checking = cls._compile(pl.from_pandas(raw_df))
        except pl.exceptions.PolarsError as exc:
            raise ParsingError(f"Failed to compile data: {exc}") from exc
        _require_dates(checking)
        return checking

    @staticmethod
    def _compile(raw):
        # A blank debit or credit cell means zero
        credit = pl.col("5").cast(pl.Float64).fill_null(0)
        debit = pl.col("4").cast(pl.Float64).fill_null(0)
        try:
            date_data = raw.select(pl.col("0").str.strptime(pl.Date, format="%d/%m/%y"))
        except pl.exceptions.PolarsError:
            date_data = raw.select(pl.col("0").str.strptime(pl.Date, format="%d/%m/%Y"))
        data = {
            "date": date_data,
            "amount": raw.select(credit - debit),
            "raw_description": raw.select(pl.col("2").cast(pl.String)),
        }
        return pl.DataFrame(data).with_columns(pl.lit("checking").alias("source"))


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
        raw_tables = _read_html_tables(input_file)
        parsed_tables = []
        for raw_table in raw_tables:
            if raw_table.empty:
                continue
            title = raw_table.iloc[0, 0]
            if title not in cls.TITLES:
                continue
            cls.check(raw_table, title)
            try:
                parsed_table = cls._table_parse(raw_table)
            except pl.exceptions.PolarsError as exc:
                raise ParsingError(f"Failed to compile data: {exc}") from exc
            _require_dates(parsed_table)
            parsed_tables.append(parsed_table)
        if not parsed_tables:
            raise ParsingError("No valid tables found")
        return pl.concat(parsed_tables)

    @classmethod
    def check(cls, raw_df, title):
        if raw_df.shape[0] < 3 or raw_df.shape[1] < len(cls.HEADERS):
            raise ParsingError(f"Unexpected table shape: {raw_df.shape}")
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
            if not pd.isna(expected_nan):
                raise ParsingError(f"Expected nan, got: {expected_nan!r}")

    @classmethod
    def _table_parse(cls, raw_df):
        # Remove titles, headers, and totals rows
        table = pl.from_pandas(raw_df.iloc[2:-1])
        # Compile data
        data = {
            "date": table.select(pl.col("0").str.strptime(pl.Date, format="%d/%m/%y")),
            "amount": table.select(pl.col("5").cast(pl.Float64).mul(-1)),
            "raw_description": table.select(pl.col("1").cast(pl.String)),
        }
        return pl.DataFrame(data).with_columns(pl.lit("card").alias("source"))


register_parser("leumi checking", CheckingFormat.parse)
register_parser("leumi card", CardFormat.parse)
