import dataclasses
from pathlib import Path

import polars as pl

from finalyze.display import flip_rtl_column, print_table
from finalyze.source.leumi import parse_file

SOURCE_SCHEMA = {
    "account": pl.String,
    "source": pl.String,
    "date": pl.Date,
    "amount": pl.Float64,
    "description": pl.String,
}


@dataclasses.dataclass
class Args:
    account_name: str
    files: list[Path]

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

    @classmethod
    def from_args(cls, args):
        return cls(
            account_name=args.account_name,
            files=tuple(Path(f).resolve() for f in args.files),
        )


def run(command_args, global_args):
    account_name = command_args.account_name
    output_file = global_args.source_dir / f"{account_name}.csv"
    print("Source files:")
    for f in command_args.files:
        if not f.is_file():
            raise FileNotFoundError(f"File not found: {f}")
        print(f"  {f}")
    parsed_data = parse_sources(files=command_args.files, account_name=account_name)
    print_table(parsed_data, "Parsed data")
    print(f"Writing output to: {output_file}")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    parsed_data.write_csv(output_file)


def get_source_data(data):
    source_data = pl.concat(
        pl.read_csv(file, schema=SOURCE_SCHEMA)
        for file in data.source_dir.glob("*.csv")
    )
    if data.flip_rtl:
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
