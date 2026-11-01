import datetime
import functools
from typing import Optional

import polars as pl

from finalyze.source.schema import ENRICHED_SCHEMA, validate_schema
from finalyze.source.tag import annotate
from finalyze.store import DEFAULT_GROUP, Store, TagKind

SENTINEL_TICK_DESCRIPTION = "auto-generated sentinel tick"
# Columns of generated ticks, which are not transactions
_NO_TRANSACTION = {
    column: pl.lit(None, dtype=pl.String)
    for column in ("id", "description", "transaction_description", "part_kind")
}
SORT_ORDER = ("date", "account", "source", "amount", "id", "tag", "subtag")
SORT_DESCENDING = (False, False, False, True, False, False, False)
"""The order in which balances accumulate: by day, then by account and source,
incomes before expenses (banks' exports don't keep the order within a day)."""
LABEL_DELIMITER = "/"
"""Joins combined labels, e.g. "food/groceries" and "alice/card"."""
FLOW_KINDS: frozenset[TagKind] = frozenset({"regular", "one-off"})
"""Kinds of tags of incomes and expenses."""
AVERAGED_KINDS: frozenset[TagKind] = frozenset({"regular"})
"""Kinds of tags in averages and trends, unless one-offs are included."""


class SourceData:
    """Post-processed data, and the subsets of it that analysis tables use.

    Options that change every table's subsets:
    - `net_by_tag`: net incomes and expenses by tag rather than by transaction
    - `groups`: only accounts of these groups, unless a table says otherwise
      (balances always have every group). None is the groups for spending.
    - `tags`: only these tags in subsets of tag kinds, e.g. incomes and
      expenses (all if None)
    - `include_one_offs`: one-off tags count in averages and trends
    - `rows`: only rows matching this, after classifying incomes and expenses
      (e.g. a period's rows, with tags netted over all the data)
    """

    def __init__(
        self,
        source: pl.DataFrame,
        *,
        net_by_tag: bool = False,
        groups: Optional[frozenset[str]] = None,
        tags: Optional[frozenset[str]] = None,
        include_one_offs: bool = False,
        rows: Optional[pl.Expr] = None,
    ):
        self._source = source
        self._frames = _Frames(source)
        self._subsets: dict[tuple, pl.DataFrame] = {}
        self.net_by_tag = net_by_tag
        self.groups = groups
        self.tags = tags
        self.include_one_offs = include_one_offs
        self.rows = rows

    def _with(self, **changes) -> "SourceData":
        options = dict(
            net_by_tag=self.net_by_tag,
            groups=self.groups,
            tags=self.tags,
            include_one_offs=self.include_one_offs,
            rows=self.rows,
        )
        source = changes.pop("source", None)
        if source is None:
            data = SourceData(self._source, **(options | changes))
            # The same frame: share what is derived from it
            data._frames = self._frames
            return data
        return SourceData(source, **(options | changes))

    def with_frame(self, source: pl.DataFrame) -> "SourceData":
        """Other data (e.g. a subset of this one) with the same options."""
        return self._with(source=source)

    def with_options(self, **changes) -> "SourceData":
        """The same data, with some options changed (e.g. `groups=None`). Itself
        (with its subsets) if they don't change."""
        if "rows" not in changes and all(
            getattr(self, name) == value for name, value in changes.items()
        ):
            return self
        return self._with(**changes)

    def restricted(self, rows: pl.Expr) -> "SourceData":
        """The same data and options, with only the rows matching `rows` (and
        the current restriction) in its subsets."""
        if self.rows is not None:
            rows = self.rows & rows
        return self._with(rows=rows)

    @property
    def frame(self) -> pl.DataFrame:
        """All rows, including generated ticks."""
        return self._source

    @property
    def months(self) -> pl.DataFrame:
        """Every month of the data (sentinel ticks are in every month), as a
        sorted "month" column."""
        return self._frames.months

    @property
    def averaged_kinds(self) -> frozenset[TagKind]:
        """Kinds of tags in averages and trends."""
        if self.include_one_offs:
            return FLOW_KINDS
        return AVERAGED_KINDS

    def get(
        self,
        *,
        kinds: Optional[frozenset[TagKind]] = None,
        all_groups: bool = False,
        incomes: bool = False,
        expenses: bool = False,
        sentinels: bool = False,
        net_by_subtag: bool = False,
    ) -> pl.DataFrame:
        """Rows of some kinds of tags (all if None), of the groups (or of all
        groups), and only incomes or expenses (as positive amounts)."""
        key = (kinds, all_groups, incomes, expenses, sentinels, net_by_subtag)
        if key not in self._subsets:
            self._subsets[key] = self._subset(*key)
        return self._subsets[key]

    def _subset(
        self,
        kinds: Optional[frozenset[TagKind]],
        all_groups: bool,
        incomes: bool,
        expenses: bool,
        sentinels: bool,
        net_by_subtag: bool,
    ) -> pl.DataFrame:
        # Without sentinel ticks from the start: they are most rows, and their
        # amounts are 0, so they change no tag's sum
        df = self._source if sentinels else self._frames.transactions
        if kinds is not None:
            kind_names = pl.Series(sorted(kinds), dtype=pl.String)
            df = df.filter(pl.col("kind").is_in(kind_names))
            if self.tags is not None:
                tags = pl.Series(sorted(self.tags), dtype=pl.String)
                df = df.filter(pl.col("tag").is_in(tags))
        if not all_groups:
            if self.groups is None:
                df = df.filter(pl.col("group_role") == "spending")
            else:
                groups = pl.Series(sorted(self.groups), dtype=pl.String)
                df = df.filter(pl.col("group").is_in(groups))
        if incomes:
            df = self._filter_net(
                df,
                incomes_or_expenses=True,
                by_subtag=net_by_subtag,
            )
        elif expenses:
            df = self._filter_net(
                df,
                incomes_or_expenses=False,
                by_subtag=net_by_subtag,
            )
        if self.rows is not None:
            df = df.filter(self.rows)
        return df

    def _filter_net(self, df, incomes_or_expenses: bool, by_subtag: bool):
        """Incomes or expenses (as positive amounts). Every row is on one side:
        netting by tag, a tag's rows go by the sign of their sum (a sum of 0 is
        an expense), and untagged rows by their own sign."""
        amount = pl.col("amount")
        if self.net_by_tag:
            tag_cols = ("tag", "subtag") if by_subtag else ("tag",)
            tag_sum = amount.sum().over(tag_cols)
            untagged = pl.col("tag").is_null()
            if incomes_or_expenses:
                side = pl.when(untagged).then(amount > 0).otherwise(tag_sum > 0)
            else:
                side = pl.when(untagged).then(amount < 0).otherwise(tag_sum <= 0)
            df = df.filter(side)
        else:
            df = df.filter(amount > 0 if incomes_or_expenses else amount < 0)
        # Reverse (if expenses)
        if not incomes_or_expenses:
            df = df.with_columns(amount * -1)
        return df


class _Frames:
    """Frames derived from a source frame, computed once and shared by the
    `SourceData` of the same frame (with other options)."""

    def __init__(self, source: pl.DataFrame):
        self._source = source

    @functools.cached_property
    def transactions(self) -> pl.DataFrame:
        """Without sentinel ticks."""
        return self._source.filter(~pl.col("is_sentinel_tick"))

    @functools.cached_property
    def months(self) -> pl.DataFrame:
        return self._source.select(pl.col("month").unique().sort())


def post_process(source: pl.DataFrame, store: Store) -> pl.DataFrame:
    """Enrich ingested data (see `RAW_SCHEMA`) with tags, ticks and balances."""
    # Tags
    source = annotate(source, store)

    # Sentinel ticks
    source = _add_sentinel_ticks(source)

    # Tag kinds and account groups
    source = _add_kind(source, store)
    source = _add_group(source, store)

    # Cumulative balances
    source = source.sort(SORT_ORDER, descending=SORT_DESCENDING).with_columns(
        balance_total=pl.col("amount").cum_sum(),
        balance_role=pl.col("amount").cum_sum().over("group_role"),
        balance_group=pl.col("amount").cum_sum().over("group"),
        balance_account=pl.col("amount").cum_sum().over("account"),
        balance_source=pl.col("amount").cum_sum().over("account", "source"),
    )

    # Derived columns
    source = _derive_month(source)
    source = _derive_tags(source)
    source = _derive_account_source(source)

    validate_schema(source, ENRICHED_SCHEMA)
    return source


def _derive_month(df):
    year_str = pl.col("date").dt.year().cast(str)
    month_str = pl.col("date").dt.month().cast(str).str.pad_start(2, "0")
    month = year_str + "-" + month_str
    return df.with_columns(month=month)


def _derive_tags(df):
    tags = pl.col("tag") + LABEL_DELIMITER + pl.col("subtag")
    return df.with_columns(tags=tags)


def _derive_account_source(df):
    account_source = pl.col("account") + LABEL_DELIMITER + pl.col("source")
    return df.with_columns(account_source=account_source)


def _today() -> datetime.date:
    return datetime.date.today()


def current_month() -> str:
    """The calendar month of today, e.g. "2025-06"."""
    today = _today()
    return f"{today.year:04}-{today.month:02}"


def _add_kind(df, store: Store):
    """The effective kind of every row: the kind set on the transaction, else
    its subtag's own kind, else its tag's. Untagged rows (and unknown tags) are
    regular."""
    tags = pl.DataFrame(
        [(tag.name, tag.kind) for tag in store.list_tags()],
        schema={"tag": pl.String, "tag_kind": pl.String},
        orient="row",
    )
    subtags = pl.DataFrame(
        [(subtag.tag, subtag.name, subtag.kind) for subtag in store.list_subtags()],
        schema={"tag": pl.String, "subtag": pl.String, "subtag_kind": pl.String},
        orient="row",
    )
    kind = pl.coalesce("part_kind", "subtag_kind", "tag_kind", pl.lit("regular"))
    return (
        df.join(tags, on="tag", how="left")
        .join(subtags, on=("tag", "subtag"), how="left")
        .with_columns(kind=kind, kind_set=pl.col("part_kind").is_not_null())
        .drop("tag_kind", "subtag_kind", "part_kind")
    )


def _add_group(df, store: Store):
    """The group of every account and its role. Accounts unknown to the store
    are in the default group, for spending."""
    roles = {group.name: group.role for group in store.list_groups()}
    default_group = next(
        (name for name, role in roles.items() if role == "spending"), DEFAULT_GROUP
    )
    groups = pl.DataFrame(
        [(account.name, account.group) for account in store.list_accounts()],
        schema={"account": pl.String, "group": pl.String},
        orient="row",
    )
    return (
        df.join(groups, on="account", how="left")
        .with_columns(pl.col("group").fill_null(default_group))
        .with_columns(
            group_role=pl.col("group").replace_strict(
                roles, default="spending", return_dtype=pl.String
            )
        )
    )


def _add_sentinel_ticks(df):
    df = df.with_columns(is_sentinel_tick=pl.lit(False))
    all_dates = df["date"]
    months = pl.Series(_months_in_range(all_dates.min(), all_dates.max()))
    combinations = (
        df.group_by("tag", "subtag")
        .len()
        .join(pl.DataFrame(dict(date=months)), how="cross")
        .join(df.group_by("account").len().select("account"), how="cross")
        .join(df.group_by("source").len().select("source"), how="cross")
    )
    sentinels = combinations.with_columns(
        amount=pl.lit(0.0),
        raw_description=pl.lit(SENTINEL_TICK_DESCRIPTION),
        is_sentinel_tick=pl.lit(True),
        **_NO_TRANSACTION,
    )
    sentinels = sentinels.select(df.columns)
    return pl.concat((df, sentinels))


def _months_in_range(min_date: datetime.date, max_date: datetime.date):
    all_months = []
    current = min_date.replace(day=1)
    max_date = max_date.replace(day=2)
    while current < max_date:
        all_months.append(current)
        current = current + datetime.timedelta(days=32)
        current = current.replace(day=1)
    return all_months
