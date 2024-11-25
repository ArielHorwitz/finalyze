import math
from pathlib import Path

import pandas as pd
import polars as pl

from finalyze.utils import flip_rtl_column, print_table

SOURCE_SCHEMA = {
    "source": pl.String,
    "date": pl.Date,
    "amount": pl.Float64,
    "description": pl.String,
}


def add_subparser(subparsers):
    parser = subparsers.add_parser("import", help="Import source data")
    parser.set_defaults(func=run)
    parser.add_argument(
        "-a",
        "--account-files",
        nargs="*",
        default=[],
        help="Account balance .xls files exported from Bank Leumi",
    )
    parser.add_argument(
        "-c",
        "--credit-files",
        nargs="*",
        default=[],
        help="Credit card .xls files exported from Bank Leumi",
    )
    parser.add_argument(
        "-A",
        "--account-files-dir",
        help="Directory of account balance files",
    )
    parser.add_argument(
        "-C",
        "--credit-files-dir",
        help="Directory of credit card files",
    )


def run(args):
    verbose = args.verbose
    account_files = [Path(f).resolve() for f in args.account_files]
    credit_files = [Path(f).resolve() for f in args.credit_files]
    if args.account_files_dir is not None:
        account_files.extend(Path(args.account_files_dir).glob("*.xls"))
    if args.credit_files_dir is not None:
        credit_files.extend(Path(args.credit_files_dir).glob("*.xls"))
    output_file = args.source_file
    if verbose:
        print("Account files:")
        for f in sorted(account_files):
            print(f"  {f}")
        print("Credit files:")
        for f in sorted(credit_files):
            print(f"  {f}")
    if len(account_files) + len(credit_files) == 0:
        raise RuntimeError("No source files provided")
    parsed_data = parse_sources(
        balance_files=account_files,
        credit_files=credit_files,
        verbose=verbose,
    )
    print(f"Writing output to: {output_file}")
    parsed_data.write_csv(output_file)


def get_source_data(args):
    source_data = pl.read_csv(args.source_file, schema=SOURCE_SCHEMA)
    if args.flip_rtl:
        source_data = flip_rtl_column(source_data, "description")
    return source_data


def parse_sources(*, balance_files, credit_files, verbose):
    balance_dfs = [
        parse_balance(input_file=file, verbose=verbose) for file in balance_files
    ]
    credit_dfs = [
        parse_credit(input_file=file, verbose=verbose) for file in credit_files
    ]
    if balance_dfs:
        print_table(pl.concat(balance_dfs), "Balance", verbose > 1)
    if credit_dfs:
        print_table(pl.concat(credit_dfs), "Credit", verbose > 1)
    final = (
        pl.concat(balance_dfs + credit_dfs)
        .unique()
        .sort("date", "amount")
        .select(SOURCE_SCHEMA.keys())
    )
    print_table(final, "Parsed data", verbose)
    return final


def parse_credit(*, input_file, verbose=False):
    def inner_parse(raw_pd_df, source_name):
        validate(raw_pd_df)
        print_table(raw_pd_df, f"raw {source_name}", verbose > 1)
        table = pl.from_pandas(raw_pd_df.iloc[2:-1])
        parsed = pl.DataFrame(
            {
                "source": source_name,
                "date": table.select(
                    pl.col("0").str.strptime(pl.Date, format="%d/%m/%y")
                ),
                "amount": table.select(pl.col("5").cast(pl.Float64).mul(-1)),
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
    raw_tables = [inner_parse(raw_html[2], "Credit card")]
    if len(raw_html) >= 4:
        debit_table = inner_parse(raw_html[3], "Debit")
        raw_tables.append(debit_table)

    # combine
    parsed = pl.concat(raw_tables)
    print_table(parsed, "parsed credit", verbose > 1)
    return parsed


def parse_balance(*, input_file, verbose=False):
    balances_raw_pd = pd.read_html(input_file, encoding="utf-8")[2]
    balances_raw_pd[0] = balances_raw_pd[0].str.replace("** ", "", regex=False)
    try:
        assert balances_raw_pd.iloc[0, 0] == "תנועות בחשבון"
        assert balances_raw_pd.iloc[1, 0] == "תאריך"
    except AssertionError:
        raise ImportError(f"Unexpected format for {input_file}")
    print_table(balances_raw_pd, "raw balance", verbose > 1)
    if balances_raw_pd.iloc[-1, 0].startswith("**"):
        balances_raw_pd = balances_raw_pd.iloc[:-1]
    raw = pl.from_pandas(balances_raw_pd.iloc[2:])
    data = {
        "source": "Account txn",
        "date": raw.select(pl.col("0").str.strptime(pl.Date, format="%d/%m/%y")),
        "amount": raw.select(
            pl.col("5").cast(pl.Float64) - pl.col("4").cast(pl.Float64)
        ),
        "description": raw.select(pl.col("2").cast(pl.String)),
        # "notes": raw.select(pl.col("7").cast(pl.String)),
    }
    parsed = pl.DataFrame(data)
    print_table(parsed.sort("date"), "parsed balance", verbose > 1)
    # remove direct credit card transactions
    filtered = parsed.filter(pl.col("description") != "לאומי ויזה")
    print_table(filtered.sort("date"), "filtered balance", verbose > 1)
    return filtered
