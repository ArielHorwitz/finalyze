import dataclasses
import functools
import operator
from typing import Optional

import arrow
import polars as pl

DATE_PATTERNS = (
    "YYYY-MM-DD",
    "YYYY-MM",
    "YYYY",
)


@dataclasses.dataclass
class Filters:
    start_date: Optional[str]
    end_date: Optional[str]
    tags: Optional[list[str]]
    subtags: Optional[list[str]]
    description: Optional[str]
    account: Optional[str]

    @staticmethod
    def configure_parser(parser):
        parser.add_argument(
            "-S",
            "--start-date",
            help="Filter since date (inclusive)",
        )
        parser.add_argument(
            "-E",
            "--end-date",
            help="Filter until date (non-inclusive)",
        )
        parser.add_argument(
            "-1",
            "--tags",
            nargs="*",
            help="Filter by tags",
        )
        parser.add_argument(
            "-2",
            "--subtags",
            nargs="*",
            help="Filter by subtags",
        )
        parser.add_argument(
            "-D",
            "--description",
            help="Filter by description (regex pattern)",
        )
        parser.add_argument(
            "-A",
            "--account",
            help="Filter by account name",
        )

    @classmethod
    def from_args(cls, args):
        return cls(
            **{
                field.name: getattr(args, field.name)
                for field in dataclasses.fields(cls)
            }
        )

    def filter_data(self, df):
        predicates = []
        # dates
        if self.start_date is not None:
            predicates.append(pl.col("date").dt.date() >= _parse_date(self.start_date))
        if self.end_date is not None:
            predicates.append(pl.col("date").dt.date() < _parse_date(self.end_date))
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
        # filter
        predicate = functools.reduce(operator.and_, predicates, pl.lit(True))
        return df.filter(predicate)


def _parse_date(raw_date):
    if not raw_date:
        return None
    for pattern in DATE_PATTERNS:
        try:
            return arrow.get(raw_date, pattern).date()
        except arrow.parser.ParserMatchError:
            pass
    raise arrow.parser.ParserMatchError(
        f"Failed to match date {raw_date!r} against patterns: {DATE_PATTERNS}"
    )
