import copy
import functools
import json
import operator
from pathlib import Path
from typing import Any, Literal, Optional

import polars as pl
import toml
from pydantic import BaseModel, Field, field_validator
from pydantic.color import Color
from pydantic_extra_types.pendulum_dt import Date

from finalyze import APP_NAME

CONFIG_DIR = Path.home() / ".config" / APP_NAME.lower()
CONFIG_FILE = CONFIG_DIR / "config.toml"

DEFAULT_DATA_DIR = Path.home() / ".local" / "share" / APP_NAME
DEFAULT_COLORS = {
    "total balance": "#fff",
    "expense": "#f00",
    "income": "#0f0",
    "bills": "#c26",
    "other": "#000",
}
DEFAULT_SOURCE_DIRECTORIES = {
    "default": [Path.home() / "Downloads" / "finalyze" / "sources"],
}
CardTransactionStrategies = Literal["remove", "balance", "untouched"]
"""Strategies for handling credit card transactions in the checking account.

*untouched*:
Leave them as is.

*remove*:
Remove them entirely.

*balance*:
Replace them with equal and opposite "Transfer" transactions. This is used to
maintain the balances of all accounts correctly when using data from both
checking account and credit cards.

E.g. if there is a transaction found in the checking account that is identified
as a charge of 100 USD from the credit card: replace it with a transaction for
-100 described as a transfer under the "checking" account, and add a
transaction for +100 described as a transfer under the "card" account. This
assumes the same -100 USD is found in the "card" account.
"""


class Filters(BaseModel):
    start_date: Optional[Date] = None
    """Minimum date (inclusive)."""
    end_date: Optional[Date] = None
    """Maximum date (non-inclusive)."""
    tags: Optional[list[str]] = None
    """Tags."""
    subtags: Optional[list[str]] = None
    """Subtags."""
    description: Optional[str] = None
    """Description."""
    account: Optional[str] = None
    """Account."""
    source: Optional[str] = None
    """Source."""
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
        if self.description is not None:
            predicates.append(pl.col("description").str.contains(self.description))
        if self.account is not None:
            predicates.append(pl.col("account") == self.account)
        if self.source is not None:
            predicates.append(pl.col("source").str.contains(self.source))
        return predicates

    @property
    def has_effect(self):
        return len(self._get_predicates()) > 0 or self.invert

    @property
    def predicate(self):
        predicates = self._get_predicates()
        predicate = functools.reduce(operator.and_, predicates, pl.lit(True))
        if self.invert:
            predicate = ~predicate
        return predicate

    def apply(self, df):
        return df.filter(self.predicate)


class Ingestion(BaseModel):
    directories: dict[str, list[Path]] = Field(
        default=DEFAULT_SOURCE_DIRECTORIES,
        validate_default=True,
    )
    """A list of directories to ingest for each account."""
    card_transactions: CardTransactionStrategies = Field(
        default="untouched",
        validate_default=True,
    )
    """How to handle credit card transaction in the checking account.

    See: `CardTransactionStrategies`."""
    clear_previous: bool = True
    """Clear previously ingested data in the dataset."""
    verbose_parsing: bool = False
    """Print more data while parsing sources."""
    print_directories: bool = False
    """Print the directories used for ingestion."""
    print_result: bool = False
    """Print the resulting data from ingestion."""
    filters: Filters = Filters()
    """Filter data for ingestion."""

    @field_validator("directories", mode="after")
    @classmethod
    def resolve_directories(cls, directories):
        return {
            key: [p.expanduser().resolve() for p in dirlist]
            for key, dirlist in directories.items()
        }


class TagPresetRule(BaseModel):
    tag: str = "Untagged"
    """Tag preset to assign."""
    subtag: str = "Untagged"
    """Subtag preset to assign."""
    filters: Filters = Filters()
    """Filters for entries to assign preset."""


class Tag(BaseModel):
    print_result: bool = False
    """Print a summary of the tags."""
    default_tag: Optional[str] = None
    """Set a default tag for untagged rows."""
    default_subtag: Optional[str] = None
    """Set a default subtag for untagged rows."""
    delete_filters: Filters = Filters()
    """Prompt to delete tags based on filters."""
    delete_unused: bool = False
    """Prompt to delete tags that aren't found in the dataset."""
    preset_rules: list[TagPresetRule] = Field(
        default_factory=list,
        validate_default=True,
    )
    """List of preset rules for tagging."""


class AnalysisGraphs(BaseModel):
    title: str = "plots"
    """Title and name of the output file."""
    open: bool = False
    """Open the output file."""
    plotly_template: str = "plotly_dark"
    """Plotly template for the graphs."""
    colors: dict[str, Color] = Field(default=DEFAULT_COLORS, validate_default=True)
    """Colors for labels in the graphs."""
    lightweight_html: bool = False
    """Do not include scripts in the output HTML file to keep it small in size."""
    plotly_arguments: dict[str, Any] = Field(default_factory=dict)
    """Arbitrary extra arguments for plotly graphs."""


class AnalysisAnonymization(BaseModel):
    enable: bool = False
    """Enable (weak) anonymization."""
    scale: tuple[float, float] = (1_000_000, 1_000_000_000_000)
    """Minimum and maximum amount to scale amounts."""
    anonymize_accounts: bool = True
    """Also anonymize accounts."""
    anonymize_sources: bool = True
    """Also anonymize sources."""
    anonymize_descriptions: bool = True
    """Also anonymize descriptions."""
    anonymize_tags: bool = True
    """Also anonymize tags."""


class AnalysisEdgeTick(BaseModel):
    enable: bool = False
    """Enable edge ticks."""
    pad_days: int = 31
    """Add edge ticks a number of days past the data."""
    cap_same_month: bool = False
    """Don't allow edge tick to overflow past the edge of the month."""


class Analysis(BaseModel):
    filters: Filters = Filters()
    """Filters for analysis data."""
    graphs: AnalysisGraphs = AnalysisGraphs()
    """Configuration for graphs."""
    anonymization: AnalysisAnonymization = AnalysisAnonymization()
    """Configuration for (weak) anonymization of output data."""
    breakdown_filters: Filters = Filters()
    """Filters for breakdown graphs."""
    breakdown_months: int = 3
    """Number of months back to breakdown in detail."""
    rolling_average_weights: list[list[float]] = Field(
        default_factory=lambda: [
            [3, 4],
            [3, 4, 5, 6],
            [3, 4, 5, 6, 7, 8],
        ],
    )
    """List of weights for each of the rolling averages."""
    edge_tick_min: AnalysisEdgeTick = AnalysisEdgeTick()
    """Add empty transactions for every account and source before the minimum date to make some graphs more readable."""  # fmt: skip  # noqa: disable=E501
    edge_tick_max: AnalysisEdgeTick = AnalysisEdgeTick()
    """Add empty transactions for every account and source after the maximum date to make some graphs more readable."""  # fmt: skip  # noqa: disable=E501
    allow_untagged: bool = False
    """Bypass restriction requiring every entry to be tagged."""
    print_source: bool = False
    """Print all the source data for making the analysis."""
    print_tables: bool = False
    """Print the tables used for each graph."""


class Display(BaseModel):
    flip_rtl: bool = False
    """Flip text of non-English letters when printing tables."""
    max_rows: int = 1_000_000
    """Maximum rows to print when printing tables."""
    max_cols: int = 1_000
    """Maximum columns to print when printing tables."""
    max_width: int = 1_000
    """Maximum width in characters when printing tables."""
    show_shape: bool = False
    """Show the shape (dimension sizes) of tables when printing them."""


class General(BaseModel):
    data_dir: Path = Field(default=DEFAULT_DATA_DIR, validate_default=True)
    """Default directory for storing app data."""
    dataset: str = "default"
    """Dataset name to use"""
    tag_set: str = "default"
    """Name of tags file."""
    print_config: bool = False
    """Print the loaded configuration."""
    multi_column_delimiter: str = " - "
    """Delimiter for combined column names."""

    @property
    def source_dir(self):
        return self.data_dir / "sources" / self.dataset

    @property
    def tags_dir(self):
        return self.data_dir / "tags"

    @property
    def output_dir(self):
        return self.data_dir / "output"

    @property
    def tags_file(self):
        return self.tags_dir / f"{self.tag_set}.csv"

    def create_directories(self):
        for directory in (self.source_dir, self.tags_dir, self.output_dir):
            directory.mkdir(parents=True, exist_ok=True)

    @field_validator("data_dir", mode="after")
    @classmethod
    def resolve_directory(cls, directory):
        return directory.expanduser().resolve()


class Config(BaseModel):
    general: General = General()
    """General configuration."""
    display: Display = Display()
    """Configuration for display."""
    ingestion: Ingestion = Ingestion()
    """Configuration for ingestion."""
    tag: Tag = Tag()
    """Configuration for tagging."""
    analysis: Analysis = Analysis()
    """Configuration for analysis."""


LOADED_CONFIG = None


def load_config(
    *,
    config_dir: Path = CONFIG_DIR,
    additional_configs: tuple[str, ...] = tuple(),
    override: str = "",
    use_preloaded: bool = True,
):
    global LOADED_CONFIG
    if LOADED_CONFIG is not None and use_preloaded:
        return LOADED_CONFIG

    default_config_file = config_dir / "config.toml"
    config_data = toml.loads(default_config_file.read_text())
    for config_name in additional_configs:
        config_file_path = Path(config_dir) / f"{config_name}.toml"
        new_config_data = toml.loads(config_file_path.read_text())
        config_data = _depth_first_merge(config_data, new_config_data)
    config_data = _depth_first_merge(config_data, toml.loads(override))
    LOADED_CONFIG = Config(**config_data)
    return LOADED_CONFIG


def get_config_file_names(*, config_dir: Path = CONFIG_DIR) -> tuple[str]:
    return tuple(file.stem for file in config_dir.glob("*.toml"))


def write_default_config(*, config_dir: Path = CONFIG_FILE):
    config_dir.mkdir(parents=True, exist_ok=True)
    config_file = config_dir / "config.toml"
    if not config_file.is_file():
        print(f"Creating default config at: {config_file}")
        config_dump = json.loads(Config().model_dump_json())
        config_file.write_text(toml.dumps(config_dump))


def _depth_first_merge(base: dict, other: dict):
    assert isinstance(base, dict)
    assert isinstance(other, dict)
    base = copy.deepcopy(base)
    for key, other_value in other.items():
        base_value = base.get(key)
        if isinstance(base_value, dict) and isinstance(other_value, dict):
            base[key] = _depth_first_merge(base_value, other_value)
        else:
            base[key] = other_value
    return base


if __name__ == "__main__":
    # Try default config
    print("DEFAULT")
    default_config = Config()
    print(default_config)
