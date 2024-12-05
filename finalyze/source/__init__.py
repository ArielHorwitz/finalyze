from pathlib import Path

import polars as pl

from finalyze.source.leumi import parse_file
from finalyze.utils import flip_rtl_column, print_table

SOURCE_SCHEMA = {
    "account": pl.String,
    "source": pl.String,
    "date": pl.Date,
    "amount": pl.Float64,
    "description": pl.String,
}


def add_subparser(subparsers):
    parser = subparsers.add_parser("source", help="Import source data")
    parser.set_defaults(func=run)
    parser.add_argument(
        "files",
        nargs="+",
        help="Account balance and credit card .xls files exported from Bank Leumi",
    )
    parser.add_argument(
        "-n",
        "--account-name",
        help="Name of account",
    )


def run(args):
    verbose = args.verbose
    files = [Path(f).resolve() for f in args.files]
    account_name = args.account_name or args.dataset_name
    output_file = args.source_dir / f"{account_name}.csv"
    if verbose:
        print("Source files:")
        for f in sorted(files):
            print(f"  {f}")
    parsed_data = parse_sources(files=files, account_name=account_name)
    print_table(parsed_data, "Parsed data", verbose)
    print(f"Writing output to: {output_file}")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    parsed_data.write_csv(output_file)


def get_source_data(args):
    source_data = pl.concat(
        pl.read_csv(file, schema=SOURCE_SCHEMA)
        for file in args.source_dir.glob("*.csv")
    )
    if args.flip_rtl:
        source_data = flip_rtl_column(source_data, "description")
    return source_data


def parse_sources(*, files, account_name):
    raw_dfs = [parse_file(input_file=file) for file in files]
    final = (
        pl.concat(raw_dfs)
        .with_columns(pl.lit(account_name).alias("account"))
        .unique()
        .sort("date", "amount")
        .select(SOURCE_SCHEMA.keys())
    )
    return final
