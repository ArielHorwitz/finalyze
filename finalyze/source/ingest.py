import importlib
from pathlib import Path

import polars as pl

from finalyze.config import config, host
from finalyze.source.identity import derive_ids
from finalyze.source.ingestion_rules import apply_rules
from finalyze.source.parsing import parse_file
from finalyze.source.schema import RAW_SCHEMA, validate_schema
from finalyze.store import Store


def ingest(store: Store) -> dict[str, pl.DataFrame]:
    """Parse the raw files of every account, then replace the ingested data.

    Every account is parsed before anything is written, so the previously
    ingested data is kept if parsing fails. Returns the ingested data by account.
    """
    ingested = parse_accounts()
    ingested_dir = host().ingested_dir
    for file in ingested_dir.glob("*.csv"):
        file.unlink()
    for account_name, source_data in ingested.items():
        output_file = ingested_dir.joinpath(f"{account_name}.csv")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        source_data.write_csv(output_file)
    # Without its times, the data changed when written, until the service
    # records them (see `finalyze.service.ingest.record_ingest`)
    host().ingest_times_file.unlink(missing_ok=True)
    store.ensure_accounts(ingested)
    return ingested


def parse_accounts() -> dict[str, pl.DataFrame]:
    """The data that ingesting would write, by account, without writing it.

    Every account's files are parsed (with IDs), then the ingestion rules apply
    to all of them (a transfer may go to another account), then the filters."""
    accounts = account_folders()
    if not accounts:
        raise ValueError(
            f"No accounts: make a folder for each account in {host().raw_dir}, "
            "with its raw files"
        )
    check_rule_accounts(accounts)
    parsed = pl.concat(
        _parse_account(account_name, account_files(folder))
        for account_name, folder in accounts.items()
    )
    reshaped = apply_rules(parsed, config().ingestion.rules)
    filtered = config().ingestion.filters.apply(reshaped)
    return {
        account_name: filtered.filter(pl.col("account") == account_name).sort(
            "date", "amount"
        )
        for account_name in accounts
    }


def check_rule_accounts(accounts: dict[str, Path]):
    """Raise ValueError if an ingestion rule transfers to an account without a
    folder (an account only transfers add rows to has an empty one)."""
    for rule in config().ingestion.rules:
        if rule.to_account is not None and rule.to_account not in accounts:
            raise ValueError(
                f"An ingestion rule transfers to account {rule.to_account!r}, "
                f"which has no folder: make {host().raw_dir.joinpath(rule.to_account)} "
                "(it may stay empty)"
            )


def _parse_account(account_name: str, input_files: tuple[Path, ...]) -> pl.DataFrame:
    if not input_files:
        # E.g. an account only transfer rules add rows to
        return pl.DataFrame(schema=RAW_SCHEMA)
    parsed_data = pl.concat(
        parse_file(input_file=file) for file in input_files
    ).with_columns(pl.lit(account_name).alias("account"))
    parsed_data = derive_ids(parsed_data)
    validate_schema(parsed_data, RAW_SCHEMA)
    return parsed_data.select(*RAW_SCHEMA.keys())


def account_folders() -> dict[str, Path]:
    """The accounts: each folder of the raw folder is one, by its name (sorted).
    Hidden ones (starting with ".") are left out."""
    raw_dir = host().raw_dir
    if not raw_dir.is_dir():
        return {}
    return {
        folder.name: folder
        for folder in sorted(raw_dir.iterdir())
        if folder.is_dir() and not is_hidden(folder)
    }


def account_files(folder: Path) -> tuple[Path, ...]:
    """An account's raw files: the files in its folder (not in its subfolders,
    and not hidden ones, e.g. an upload being written), sorted."""
    return tuple(
        sorted(
            file for file in folder.iterdir() if file.is_file() and not is_hidden(file)
        )
    )


def stray_files() -> tuple[Path, ...]:
    """Files in the raw folder itself, outside any account's folder (ignored)."""
    raw_dir = host().raw_dir
    if not raw_dir.is_dir():
        return ()
    return account_files(raw_dir)


def is_hidden(path: Path) -> bool:
    return path.name.startswith(".")


def _import_parser_modules():
    parsers_dir = Path(__file__).parent / "parsers"
    for parser_file in sorted(parsers_dir.iterdir()):
        if not parser_file.is_file() or parser_file.suffix != ".py":
            continue
        module_path = f"finalyze.source.parsers.{parser_file.stem}"
        importlib.import_module(module_path)


_import_parser_modules()
