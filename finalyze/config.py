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
    end_date: Optional[Date] = None
    tags: Optional[list[str]] = None
    subtags: Optional[list[str]] = None
    description: Optional[str] = None
    account: Optional[str] = None
    source: Optional[str] = None
    invert: bool = False

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

    def apply(self, df):
        predicates = self._get_predicates()
        # filter
        predicate = functools.reduce(operator.and_, predicates, pl.lit(True))
        if self.invert:
            predicate = ~predicate
        return df.filter(predicate)

    @property
    def has_effect(self):
        return len(self._get_predicates()) > 0


class Ingestion(BaseModel):
    directories: dict[str, list[Path]] = Field(
        default=DEFAULT_SOURCE_DIRECTORIES,
        validate_default=True,
    )
    card_transactions: LiteralCardTransactions = Field(
        default="untouched",
        validate_default=True,
    )
    verbose_parsing: bool = False
    print_directories: bool = False
    print_result: bool = False
    filters: Filters = Filters()

    @field_validator("directories", mode="after")
    @classmethod
    def resolve_directories(cls, directories):
        return {
            key: [p.expanduser().resolve() for p in dirlist]
            for key, dirlist in directories.items()
        }


class Tag(BaseModel):
    print_result: bool = False
    default_tag: Optional[str] = None
    default_subtag: Optional[str] = None
    delete_filters: Filters = Filters()
    delete_unused: bool = False


class AnalysisGraphs(BaseModel):
    open: bool = False
    plotly_template: str = "plotly_dark"
    colors: dict[str, Color] = Field(default=DEFAULT_COLORS, validate_default=True)
    lightweight_html: bool = False
    plotly_arguments: dict[str, Any] = Field(default_factory=dict)


class Analysis(BaseModel):
    filters: Filters = Filters()
    graphs: AnalysisGraphs = AnalysisGraphs()
    breakdown_filters: Filters = Filters()
    breakdown_months: int = 3
    rolling_average_weights: list[list[float]] = Field(
        default_factory=lambda: [
            [3, 4],
            [3, 4, 5, 6],
            [3, 4, 5, 6, 7, 8],
        ],
    )
    allow_untagged: bool = False
    print_source: bool = False
    print_tables: bool = False


class Display(BaseModel):
    flip_rtl: bool = False
    max_rows: int = 1_000_000
    max_cols: int = 1_000
    max_width: int = 1_000
    show_shape: bool = False


class General(BaseModel):
    data_dir: Path = Field(default=DEFAULT_DATA_DIR, validate_default=True)
    dataset: str = "default"
    tags: str = "default"
    print_config: bool = False
    multi_column_delimiter: str = " - "

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
        return self.tags_dir / f"{self.tags}.csv"

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
    display: Display = Display()
    ingestion: Ingestion = Ingestion()
    tag: Tag = Tag()
    analysis: Analysis = Analysis()


@functools.cache
def load_config(config_file: Path = CONFIG_FILE):
    config_file = Path(config_file)
    config_file.parent.mkdir(parents=True, exist_ok=True)
    if not config_file.is_file():
        print("Creating default config...")
        config_dump = json.loads(Config().model_dump_json())
        config_file.write_text(toml.dumps(config_dump))
    config_data = toml.loads(config_file.read_text())
    config = Config(**config_data)
    return config


if __name__ == "__main__":
    # Try default and user configs
    print("DEFAULT")
    config = Config()
    print(config)
    print()
    print("USER")
    config = load_config()
    print(config)
