import math

import pandas as pd
import polars as pl

from utils import print_table


def parse_credit(*, input_file, verbose=False):
    def inner_parse(raw_pd_df, source_name):
        validate(raw_pd_df)
        print_table(raw_pd_df, f"raw {source_name}", verbose > 2)
        table = pl.from_pandas(raw_pd_df.iloc[2:-1])
        parsed = pl.DataFrame(
            {
                "source": source_name,
                "date": table.select(
                    pl.col("0").str.strptime(pl.Date, format="%d/%m/%y")
                ),
                "balance": table.select(pl.col("5").cast(pl.Float64).mul(-1)),
                "description": table.select(pl.col("1").cast(pl.String)),
                # "notes": table.select(pl.col("4").cast(pl.String)),
            }
        )
        print_table(parsed.sort("date"), f"parsed {source_name}", verbose > 1)
        return parsed

    def validate(raw_pd_df):
        try:
            assert raw_pd_df.iloc[0, 0] in (
                'עסקאות מחויבות בש"ח (לידיעה בלבד)',
                'עסקאות בש"ח במועד החיוב',
            )
            assert raw_pd_df.iloc[1, 0] == "תאריך העסקה"
            assert raw_pd_df.iloc[-1, 4] == 'סה"כ:'
            assert math.isnan(raw_pd_df.iloc[-1, 0])
        except AssertionError:
            raise ImportError(f"Unexpected format for {input_file}")

    # parse parts
    raw_html = pd.read_html(input_file, encoding="utf-8")
    credit_raw = inner_parse(raw_html[2], "Credit card")
    debit_raw = inner_parse(raw_html[3], "Debit")

    # combine
    parsed = pl.concat((credit_raw, debit_raw))
    print_table(parsed, "parsed credit", verbose > 1)
    return parsed


def parse_balance(*, input_file, verbose=False):
    balances_raw_pd = pd.read_html(input_file, encoding="utf-8")[2]
    try:
        assert balances_raw_pd.iloc[0, 0] == "תנועות בחשבון"
        assert balances_raw_pd.iloc[1, 0] == "תאריך"
    except AssertionError:
        raise ImportError(f"Unexpected format for {input_file}")
    print_table(balances_raw_pd, "raw balance", verbose > 2)
    raw = pl.from_pandas(balances_raw_pd.iloc[2:])
    data = {
        "source": "Account txn",
        "date": raw.select(pl.col("0").str.strptime(pl.Date, format="%d/%m/%y")),
        "balance": raw.select(
            pl.col("5").cast(pl.Float64) - pl.col("4").cast(pl.Float64)
        ),
        "description": raw.select(pl.col("2").cast(pl.String)),
        # "notes": raw.select(pl.col("7").cast(pl.String)),.
    }
    parsed = pl.DataFrame(data)
    print_table(parsed.sort("date"), "parsed balance", verbose > 1)
    # remove direct credit card transactions
    filtered = parsed.filter(pl.col("description") != "לאומי ויזה")
    print_table(filtered.sort("date"), "filtered balance", verbose > 1)
    return filtered


def parse_sources(*, balance_files, credit_files, verbose):
    balance = pl.concat(
        parse_balance(input_file=file, verbose=verbose) for file in balance_files
    ).unique()
    credit = pl.concat(
        parse_credit(input_file=file, verbose=verbose) for file in credit_files
    ).unique()
    print_table(balance.sort("date", "balance"), "Balance", verbose)
    print_table(credit.sort("date", "balance"), "Credit", verbose)
    final = pl.concat((balance, credit)).sort("date", "balance")
    print_table(final, "Final", verbose)
    return final
