import argparse
import dataclasses
from pathlib import Path

import polars as pl

from finalyze.display import print_table
from finalyze.filters import Filters
from finalyze.source.data import RAW_SCHEMA, validate_schema
from finalyze.source.leumi import parse_file


@dataclasses.dataclass
class Options:
    verbose: bool

    @classmethod
    def configure_parser(cls, parser):
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            help="Be verbose",
        )

    @classmethod
    def from_args(cls, args):
        return cls(
            verbose=args.verbose,
        )


class AccountMapAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if not getattr(namespace, self.dest, None):
            setattr(namespace, self.dest, {})
        if len(values) < 2:
            parser.error(f"Need an account name and files, got: {values!r}")
        name = values[0]
        files = (Path(f).resolve() for f in values[1:])
        getattr(namespace, self.dest).setdefault(name, []).extend(files)


@dataclasses.dataclass
class Args:
    accounts: dict[str, list[Path]]
    filters: Filters
    options: Options

    @classmethod
    def configure_parser(cls, parser):
        parser.set_defaults(command_class=cls, run=run)
        parser.add_argument(
            "-a",
            "--account",
            nargs="*",
            metavar="NAME FILE",
            action=AccountMapAction,
            help="Account name and files",
        )
        Filters.configure_parser(parser, tags=False, account=False)
        Options.configure_parser(parser.add_argument_group("parsing"))

    @classmethod
    def from_args(cls, args):
        return cls(
            accounts=args.account,
            filters=Filters.from_args(args),
            options=Options.from_args(args),
        )


def run(command_args, global_args):
    if not command_args.accounts:
        raise ValueError("No accounts and files specified")
    for account_name, files in command_args.accounts.items():
        input_files = _get_files(files)
        output_file = global_args.source_dir / f"{account_name}.csv"
        print(f"Source files for account {account_name!r}:")
        for f in input_files:
            print(f"  {f}")
        # Parse sources
        parsed_data = pl.concat(
            parse_file(input_file=file, options=command_args.options)
            for file in input_files
        ).with_columns(pl.lit(account_name).alias("account"))
        validate_schema(parsed_data, RAW_SCHEMA)
        filtered_data = command_args.filters.filter_data(parsed_data)
        source_data = filtered_data.select(*RAW_SCHEMA.keys()).sort("date", "amount")
        print_table(source_data, "Parsed data", flip_rtl=global_args.flip_rtl)
        print(f"Writing output to: {output_file}")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        source_data.write_csv(output_file)


def _get_files(all_paths):
    files = set()
    for path in all_paths:
        if path.is_dir():
            files.update(path.glob("*.xls"))
        elif path.is_file():
            files.add(path)
        else:
            raise FileNotFoundError(f"Path is not file or folder: {path}")
    return tuple(sorted(files))


def load_source_data(source_dir):
    return pl.concat(
        pl.read_csv(file, schema=RAW_SCHEMA) for file in source_dir.glob("*.csv")
    )
