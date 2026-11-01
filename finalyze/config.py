import copy
import datetime
import functools
import json
import operator
import sys
from pathlib import Path
from typing import Literal, Optional

import polars as pl
import toml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    model_validator,
)
from pydantic_extra_types.pendulum_dt import Date

from finalyze import APP_NAME

CONFIG_DIR = Path.home() / ".config" / APP_NAME.lower()
CONFIG_FILE = CONFIG_DIR / "config.toml"
HOST_FILE_NAME = "host.toml"
"""Next to the config: where things are on this machine (see `Host`)."""

DEFAULT_DATA_DIR = Path("~/.local/share").joinpath(APP_NAME)


class ConfigModel(BaseModel):
    """Base for config sections: unknown keys are errors, not silently ignored."""

    model_config = ConfigDict(extra="forbid")


class Filters(ConfigModel):
    start_date: Optional[Date] = None
    """Minimum date (inclusive)."""
    end_date: Optional[Date] = None
    """Maximum date (non-inclusive)."""
    tags: Optional[list[str]] = None
    """Tags."""
    subtags: Optional[list[str]] = None
    """Subtags."""
    raw_description: Optional[str] = None
    """Regular expression on the raw description (the text in the raw files)."""
    account: Optional[str] = None
    """Account (exact)."""
    source: Optional[str] = None
    """Source (exact)."""
    and_filters: Optional[list["Filters"]] = None
    """Logically AND filters (does not work with other predicates)."""
    or_filters: Optional[list["Filters"]] = None
    """Logically OR filters (does not work with other predicates)."""
    invert: bool = False
    """Invert results."""

    def _get_predicates(self):
        predicates = []
        # dates
        if self.start_date is not None:
            predicates.append(pl.col("date").dt.date() >= self.start_date)
        if self.end_date is not None:
            predicates.append(pl.col("date").dt.date() < self.end_date)
        # tags
        if self.tags is not None:
            predicates.append(pl.col("tag").is_in(pl.Series(self.tags)))
        if self.subtags is not None:
            predicates.append(pl.col("subtag").is_in(pl.Series(self.subtags)))
        # patterns
        if self.raw_description is not None:
            predicates.append(
                pl.col("raw_description").str.contains(self.raw_description)
            )
        if self.account is not None:
            predicates.append(pl.col("account") == self.account)
        if self.source is not None:
            predicates.append(pl.col("source") == self.source)
        return predicates

    @property
    def has_effect(self):
        return (
            len(self._get_predicates()) > 0
            or self.invert
            or self.and_filters
            or self.or_filters
        )

    @property
    def predicate(self):
        exclusive_predicates = (
            len(self._get_predicates()) > 0,
            bool(self.and_filters),
            bool(self.or_filters),
        )
        if sum(exclusive_predicates) > 1:
            raise ValueError(
                "and_filters, or_filters, and other predicates "
                f"are mutually exclusive, found: {self}"
            )
        if self.or_filters:
            predicates = [f.predicate for f in self.or_filters]
            predicate = functools.reduce(operator.or_, predicates, pl.lit(False))
        elif self.and_filters:
            predicates = [f.predicate for f in self.and_filters]
            predicate = functools.reduce(operator.and_, predicates, pl.lit(True))
        else:
            predicates = self._get_predicates()
            predicate = functools.reduce(operator.and_, predicates, pl.lit(True))
        if self.invert:
            predicate = ~predicate
        return predicate

    def apply(self, df, /, *, invert: bool = False):
        predicate = self.predicate
        if invert:
            predicate = ~predicate
        return df.filter(predicate)

    def inverted(self):
        new = copy.deepcopy(self)
        new.invert = not new.invert
        return new


def check_raw_filters(filters: Filters, what: str):
    """Raise ValueError for filters that can't apply to parsed rows (before they
    are tagged): tags, and/or combined with other fields (also nested), or an
    invalid regular expression."""
    pending = [filters]
    while pending:
        nested = pending.pop()
        if nested.tags is not None or nested.subtags is not None:
            raise ValueError(f"{what} can't match tags or subtags")
        pending.extend((nested.and_filters or []) + (nested.or_filters or []))
    sample = pl.DataFrame(
        {
            "account": ["account"],
            "source": ["source"],
            "date": [datetime.date(2000, 1, 1)],
            "raw_description": ["raw description"],
        }
    )
    try:
        sample.filter(filters.predicate)
    except pl.exceptions.PolarsError as exc:
        raise ValueError(f"{what} have an invalid filter: {exc}") from exc


IngestionAction = Literal["drop", "transfer", "rewrite"]


class IngestionRule(ConfigModel):
    """What to do with the parsed rows that match, before they are ingested.

    Rules only reshape rows: what they mean (tags, kinds, descriptions) is for
    annotations and tagging rules. A row keeps its ID (derived from the parsed
    row, see `finalyze.source.identity`) whatever a rule changes.
    """

    match: Filters
    """The rows the rule applies to (tags and subtags don't exist yet)."""
    action: IngestionAction
    """*drop*: leave the rows out.

    *transfer*: the row is money moving to another source (e.g. the monthly card
    charge in checking, to the card): it stays as this source's leg, and a row
    of the opposite amount is added in `to_source` (of `to_account`, by default
    the row's account).

    *rewrite*: only change the raw description or date."""
    to_source: Optional[str] = None
    """The source a transfer goes to."""
    to_account: Optional[str] = None
    """The account a transfer goes to (default: the row's own)."""
    raw_description: Optional[str] = None
    """A new raw description (of both legs of a transfer)."""
    shift_days: int = 0
    """Move the date by days (of both legs of a transfer)."""

    @model_validator(mode="after")
    def consistent_action(self):
        check_raw_filters(self.match, "Ingestion rules")
        transfer_fields = self.to_source is not None or self.to_account is not None
        if self.action == "transfer" and self.to_source is None:
            raise ValueError("A transfer rule needs a 'to_source'")
        if self.action != "transfer" and transfer_fields:
            raise ValueError(f"A {self.action} rule can't have a transfer target")
        changes = self.raw_description is not None or self.shift_days != 0
        if self.action == "drop" and changes:
            raise ValueError("A drop rule can't change the raw description or date")
        if self.action == "rewrite" and not changes:
            raise ValueError("A rewrite rule needs a 'raw_description' or 'shift_days'")
        return self


class Ingestion(ConfigModel):
    rules: list[IngestionRule] = []
    """Reshape the parsed rows before they are ingested: the first matching rule
    of each row applies (see `IngestionRule`)."""
    filters: Filters = Filters()
    """Filter data for ingestion (after the rules)."""


MOVED_TO_HOST_FILE = {"general": "data_dir", "ingestion": "directories"}
"""Settings the config had, which are this machine's: the data directory is in
the host file, and the accounts are the raw folder's folders."""


class Config(ConfigModel):
    """The preferences: portable (exported with the data), without any path."""

    ingestion: Ingestion = Field(default_factory=dict, validate_default=True)
    """Configuration for ingestion."""

    @model_validator(mode="before")
    @classmethod
    def no_moved_settings(cls, data):
        if not isinstance(data, dict):
            return data
        moved = [
            f"{section}.{name}"
            for section, name in MOVED_TO_HOST_FILE.items()
            if isinstance(data.get(section), dict) and name in data[section]
        ]
        if moved:
            raise ValueError(
                f"{' and '.join(moved)} {'are' if len(moved) > 1 else 'is'} no "
                f"longer in the config: remove {'them' if len(moved) > 1 else 'it'}. "
                "The data directory and the raw "
                f"folder are in {HOST_FILE_NAME} (next to the config), and each "
                "folder of the raw folder is an account"
            )
        return data


class Host(ConfigModel):
    """Where things are on this machine, from the host file (`host.toml`, next
    to the config): never exported, and never in the config or the store."""

    data_dir: Path = DEFAULT_DATA_DIR
    """The store, the ingested data and the backups."""
    raw_dir: Optional[Path] = None
    """The raw files, a folder per account (default: `raw` in the data
    directory)."""
    host: Optional[str] = None
    """The address to serve on, unless `--host` is given (default: 127.0.0.1)."""
    port: Optional[int] = None
    """The port to serve on, unless `--port` is given (default: 8765)."""

    @model_validator(mode="after")
    def resolve_paths(self, info: ValidationInfo):
        """Absolute: relative paths are of `relative_to` in the validation
        context (the host file's directory), if given."""
        base = Path((info.context or {}).get("relative_to", ".")).expanduser()
        self.data_dir = base.joinpath(self.data_dir.expanduser()).resolve()
        raw_dir = self.raw_dir or self.data_dir.joinpath("raw")
        self.raw_dir = base.joinpath(raw_dir.expanduser()).resolve()
        # An import replaces the raw folder's contents
        if self.raw_dir == self.data_dir or self.raw_dir in self.data_dir.parents:
            raise ValueError(
                f"raw_dir ({self.raw_dir}) can't be the data directory or hold it: "
                "an import replaces everything in it"
            )
        return self

    @property
    def ingested_dir(self) -> Path:
        """The ingested data."""
        return self.data_dir.joinpath("ingested")

    @property
    def ingest_times_file(self) -> Path:
        """When the ingested data last changed, and when an ingest last ran."""
        return self.ingested_dir.joinpath("ingest-times.json")

    @property
    def db_file(self) -> Path:
        return self.data_dir.joinpath("finalyze.db")

    @property
    def backups_dir(self) -> Path:
        """Backups: exports kept on this machine (see `service.backups`)."""
        return self.data_dir.joinpath("backups")

    def create_directories(self):
        self.ingested_dir.mkdir(parents=True, exist_ok=True)


LOADED_CONFIG = None
LOADED_HOST = None
LOADED_CONFIG_DIR = CONFIG_DIR
"""The directory of `LOADED_CONFIG` (and of the host file), to reload it after
editing the config file."""


def load_config(*, config_dir: Path = CONFIG_DIR, use_preloaded: bool = True):
    """The config of `config_dir`, and its host file (see `host`)."""
    global LOADED_CONFIG, LOADED_HOST, LOADED_CONFIG_DIR
    if LOADED_CONFIG is not None and use_preloaded:
        return LOADED_CONFIG

    config_dir = Path(config_dir)
    config_data = toml.loads(config_file_of(config_dir).read_text())
    host_file = host_file_of(config_dir)
    host_data = toml.loads(host_file.read_text()) if host_file.is_file() else {}
    LOADED_CONFIG = validate_config(config_data)
    # Relative paths are of the config's directory, wherever the server starts
    LOADED_HOST = Host.model_validate(host_data, context={"relative_to": config_dir})
    LOADED_CONFIG_DIR = config_dir
    return LOADED_CONFIG


config = load_config


def host() -> Host:
    """Where things are on this machine (the defaults without a host file)."""
    if LOADED_HOST is None:
        load_config()
    return LOADED_HOST


def config_file_of(config_dir: Path) -> Path:
    return config_dir.joinpath("config.toml")


def host_file_of(config_dir: Path) -> Path:
    return config_dir.joinpath(HOST_FILE_NAME)


def validate_config(config_data: dict) -> "Config":
    return Config.model_validate(config_data)


def reload_config(config_data: dict) -> "Config":
    """Validate new data of the loaded config's file, then write it and use it.
    Comments of the file are lost."""
    return replace_config_text(toml.dumps(config_data))


def replace_config_text(text: str) -> "Config":
    """Validate a new text of the loaded config's file, then write it and use it
    (the previous file is kept as `config.toml.bak`)."""
    global LOADED_CONFIG
    new_config = validate_config(toml.loads(text))
    config_file = config_file_of(LOADED_CONFIG_DIR)
    # Replaced whole, so an interrupted write leaves the file and its backup as
    # they were
    if config_file.is_file():
        _write_atomically(
            config_file.with_name(f"{config_file.name}.bak"), config_file.read_text()
        )
    _write_atomically(config_file, text)
    LOADED_CONFIG = new_config
    return new_config


def _write_atomically(file: Path, text: str):
    temporary = file.with_name(f".{file.name}.tmp")
    temporary.write_text(text)
    temporary.replace(file)


def write_default_config(*, config_dir: Path = CONFIG_DIR):
    config_dir.mkdir(parents=True, exist_ok=True)
    config_file = config_file_of(config_dir)
    if not config_file.is_file():
        print(f"Creating default config at: {config_file}", file=sys.stderr)
        config_dump = json.loads(Config().model_dump_json())
        config_file.write_text(toml.dumps(config_dump))
