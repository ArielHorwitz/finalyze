import dataclasses
from pathlib import Path

import polars as pl

from finalyze.display import print_table
from finalyze.filters import Filters
from finalyze.source.data import RAW_SCHEMA, validate_schema
from finalyze.source.leumi import parse_file


@dataclasses.dataclass
class Args:
    account_name: str
    files: list[Path]
    filters: Filters

    @classmethod
    def configure_parser(cls, parser):
        parser.set_defaults(command_class=cls, run=run)
        parser.add_argument(
            "account_name",
            help="Name of account",
        )
        parser.add_argument(
            "files",
            nargs="+",
            help="Account balance and credit card .xls files exported from Bank Leumi",
        )
        Filters.configure_parser(parser, tags=False, account=False)

    @classmethod
    def from_args(cls, args):
        return cls(
            account_name=args.account_name,
            files=tuple(Path(f).resolve() for f in args.files),
            filters=Filters.from_args(args),
        )


def run(command_args, global_args):
    account_name = command_args.account_name
    output_file = global_args.source_dir / f"{account_name}.csv"
    print("Source files:")
    for f in command_args.files:
        if not f.is_file():
            raise FileNotFoundError(f"File not found: {f}")
        print(f"  {f}")
    # Parse sources
    raw_dfs = [parse_file(input_file=file) for file in command_args.files]
    parsed_data = (
        pl.concat(raw_dfs)
        .with_columns(pl.lit(account_name).alias("account"))
        .unique()
        .sort("date", "amount")
    )
    validate_schema(parsed_data, RAW_SCHEMA)
    filtered_data = command_args.filters.filter_data(parsed_data)
    source_data = filtered_data.select(*RAW_SCHEMA.keys())
    print_table(source_data, "Parsed data", flip_rtl=global_args.flip_rtl)
    print(f"Writing output to: {output_file}")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    source_data.write_csv(output_file)


def load_source_data(source_dir):
    return pl.concat(
        pl.read_csv(file, schema=RAW_SCHEMA) for file in source_dir.glob("*.csv")
    )
