import polars as pl

from finalyze.display import print_table
from finalyze.source.data import RAW_SCHEMA, validate_schema
from finalyze.source.leumi import parse_file


def run(config):
    if not config.source.directories:
        raise ValueError("No accounts directories specified")
    for account_name, files in config.source.directories.items():
        input_files = _get_files(files)
        output_file = config.general.source_dir / f"{account_name}.csv"
        print(f"Source files for account {account_name!r}:")
        for f in input_files:
            print(f"  {f}")
        # Parse sources
        parsed_data = pl.concat(
            parse_file(input_file=file, config=config) for file in input_files
        ).with_columns(pl.lit(account_name).alias("account"))
        validate_schema(parsed_data, RAW_SCHEMA)
        filtered_data = config.source.filters.apply(parsed_data)
        source_data = filtered_data.select(*RAW_SCHEMA.keys()).sort("date", "amount")
        print_table(
            source_data,
            f"Parsed data for account: {account_name}",
            flip_rtl=config.general.flip_rtl,
        )
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
