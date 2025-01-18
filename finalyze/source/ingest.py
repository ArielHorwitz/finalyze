import importlib
from pathlib import Path

import polars as pl

from finalyze.display import print_table
from finalyze.source.data import RAW_SCHEMA, validate_schema
from finalyze.source.parsing import PARSED_SCHEMA, parse_file


def run(config):
    if not config.ingestion.directories:
        raise ValueError("No accounts directories specified")
    for account_name, files in config.ingestion.directories.items():
        input_files = _get_files(files)
        output_file = config.general.source_dir / f"{account_name}.csv"
        if config.ingestion.print_directories:
            print(f"Source files for account {account_name!r}:")
            for f in input_files:
                print(f"  {f}")
        # Parse sources
        parsed_data = pl.concat(
            parse_file(input_file=file, config=config).select(*PARSED_SCHEMA.keys())
            for file in input_files
        ).with_columns(pl.lit(account_name).alias("account"))
        validate_schema(parsed_data, RAW_SCHEMA)
        filtered_data = config.ingestion.filters.apply(parsed_data)
        source_data = filtered_data.select(*RAW_SCHEMA.keys()).sort("date", "amount")
        if config.ingestion.print_result:
            print_table(source_data, f"Parsed data for account: {account_name}")
        print(f"Parsed account {account_name!r} to: {output_file}")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        source_data.write_csv(output_file)


def _get_files(all_paths):
    files = set()
    for path in all_paths:
        if path.is_dir():
            files.update((p for p in path.iterdir() if p.is_file()))
        elif path.is_file():
            files.add(path)
        else:
            raise FileNotFoundError(f"Path is not file or folder: {path}")
    return tuple(sorted(files))


def _import_parser_modules():
    parsers_dir = Path(__file__).parent / "parsers"
    for parser_file in parsers_dir.iterdir():
        if not parser_file.is_file() or parser_file.suffix != ".py":
            continue
        module_path = f"finalyze.source.parsers.{parser_file.stem}"
        importlib.import_module(module_path)


_import_parser_modules()
