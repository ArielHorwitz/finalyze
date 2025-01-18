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
    "total": "#fff",
    "expense": "#f00",
    "income": "#0f0",
    "bills": "#c26",
    "other": "#000",
}
DEFAULT_SOURCE_DIRECTORIES = {
    "default": [Path.home() / "Downloads" / "finalyze" / "sources"],
}
LiteralCardTransactions = Literal["remove", "balance", "untouched"]


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
    card_transactions: LiteralCardTransactions = Field(
        default="untouched",
        validate_default=True,
    )
    """How to handle credit card data in the checking account."""
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


class AnalysisGraphs(BaseModel):
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
    scale: tuple[float, float] = (1, 1_000_000)
    """Minimum and maximum amount to scale amounts."""
    anonymize_accounts: bool = True
    """Include accounts in anonymization."""
    anonymize_sources: bool = True
    """Include sources in anonymization."""
    anonymize_descriptions: bool = True
    """Include descriptions in anonymization."""
    anonymize_tags: bool = True
    """Include tags in anonymization."""


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
    add_edge_ticks: bool = True
    """Add empty transactions at the min and max dates to make some graphs more readable."""  # fmt: skip  # noqa: disable=E501
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

    @property
    def plots_file(self):
        return self.output_dir / "plots.html"

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


@functools.cache
def load_config(config_file: Path = CONFIG_FILE):
    config_file = Path(config_file)
    config_file.parent.mkdir(parents=True, exist_ok=True)
    if not config_file.is_file():
        print(f"Creating default config at: {config_file}")
        config_dump = json.loads(Config().model_dump_json())
        config_file.write_text(toml.dumps(config_dump))
    config_data = toml.loads(config_file.read_text())
    config = Config(**config_data)
    return config


if __name__ == "__main__":
    # Try default and user configs
    print("DEFAULT")
    default_config = Config()
    print(default_config)
    print()
    print("USER")
    user_config = load_config()
    print(user_config)
