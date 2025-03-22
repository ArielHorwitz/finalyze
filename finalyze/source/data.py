import calendar
import datetime
import functools
import operator
import random
import string

import polars as pl

from finalyze.config import config
from finalyze.display import round_columns
from finalyze.source import ENRICHED_SCHEMA, validate_schema
from finalyze.source.raw import derive_hash, load_source_data
from finalyze.source.tag import apply_tags

ANON_NAMES = ["Einstein", "Newton", "Curie", "Galileo", "Darwin", "Turing", "Planck", "Hawking", "Pasteur", "Lovelace", "Bohr", "Maxwell"]  # fmt: skip  # noqa: disable=E501
ANON_SOURCES = ["Chemistry", "Biology", "Psychology", "Geology", "Sociology", "Philosophy", "Physics", "Mathematics", "Economics", "Astronomy"]  # fmt: skip  # noqa: disable=E501
ANON_TAGS = ["Aardvark", "Albatross", "Alligator", "Ant", "Armadillo", "Avocet", "Bat", "Bear", "Bee", "Beetle", "Bison", "Bumblebee", "Butterfly", "Capybara", "Caracal", "Caribou", "Cat", "Caterpillar", "Centipede", "Chicken", "Chimpanzee", "Chinchilla", "Clam", "Cow", "Crab", "Crocodile", "Crow", "Deer", "Dingo", "Dog", "Dolphin", "Donkey", "Dove", "Duck", "Dugong", "Eagle", "Eel", "Elephant", "Emu", "Falcon", "Ferret", "Finch", "Flamingo", "Fox", "Frog", "Geese", "Giraffe", "Goat", "Goose", "Gorilla", "Guinea Pig", "Gull", "Hare", "Hawk", "Hedgehog", "Hippopotamus", "Hornet", "Horse", "Hyena", "Ibex", "Iguana", "Jaguar", "Javelina", "Jellyfish", "Kangaroo", "Kiwi", "Koala", "Ladybug", "Lemur", "Lion", "Lizard", "Llama", "Lobster", "Loon", "Lynx", "Macaw", "Mallard", "Meerkat", "Millipede", "Monkey", "Moose", "Moth", "Mule", "Mussel", "Narwhal", "Newt", "Ocelot", "Octopus", "Orangutan", "Orca", "Ostrich", "Owl", "Ox", "Oyster", "Pangolin", "Parrot", "Peacock", "Penguin", "Pig", "Pigeon", "Platypus", "Polar Bear", "Puffin", "Quail", "Quokka", "Rabbit", "Raccoon", "Rattlesnake", "Rhinoceros", "Salamander", "Scorpion", "Seal", "Shark", "Sheep", "Shrimp", "Skunk", "Slug", "Snail", "Snake", "Sparrow", "Spider", "Squid", "Squirrel", "Starfish", "Swan", "Tapir", "Tarantula", "Tiger", "Toad", "Turkey", "Turtle", "Umbrellabird", "Vole", "Vulture", "Wallaby", "Walrus", "Wasp", "Weasel", "Whale", "Wolf", "Worm", "Xerus", "Yak", "Zebra", "Zebu"]  # fmt: skip  # noqa: disable=E501
SENTINEL_TICK_DESCRIPTION = "auto-generated sentinel tick"
EDGE_TICK_DESCRIPTION = "auto-generated tick"
EDGE_TICK_TAG = "other"
EDGE_TICK_SUBTAG = "auto-tick"
SORT_ORDER = ("date", "tag", "subtag", "amount", "description", "hash")


class SourceData:
    def __init__(self, source: pl.DataFrame):
        self._source = source

    @classmethod
    def load(cls):
        return cls(get_post_processed_source_data())

    @functools.cache
    def get(
        self,
        *,
        breakdown: bool = False,
        include_external: bool = False,
        incomes: bool = False,
        expenses: bool = False,
        sentinels: bool = False,
        edge_ticks: bool = False,
        round: bool = False,
    ):
        df = self._source
        if breakdown:
            df = df.filter(pl.col("is_breakdown"))
        if not include_external:
            df = df.filter(~pl.col("is_external"))
        if incomes:
            df = self._filter_net(df, incomes_or_expenses=True)
        elif expenses:
            df = self._filter_net(df, incomes_or_expenses=False)
        if not sentinels:
            df = df.filter(~pl.col("is_sentinel_tick"))
        if not edge_ticks:
            df = df.filter(~pl.col("is_edge_tick"))
        if round:
            df = round_columns(df)
        return df

    def _filter_net(self, df, incomes_or_expenses: bool):
        operation = operator.gt if incomes_or_expenses else operator.lt
        amount_filter = operation(pl.col("amount"), 0)
        if config().analysis.net_by_tag:
            tags_net = self._source.group_by("tag").agg(pl.col("amount").sum())
            tags = tags_net.filter(amount_filter)["tag"]
            df = df.filter(pl.col("tag").is_in(tags))
        else:
            df = df.filter(amount_filter)
        # Reverse (if expenses)
        if not incomes_or_expenses:
            df = df.with_columns(pl.col("amount") * -1)
        return df

    def __hash__(self):
        return id(self)


def get_post_processed_source_data() -> SourceData:
    source = load_source_data()

    # Truncate
    source = _truncate_month(
        source,
        by_clock=config().analysis.truncate_month_clock,
        by_data=config().analysis.truncate_month_data,
    )

    # Tags
    source = apply_tags(source, preset_rules=config().tag.preset_rules)

    # Edge ticks
    source = _add_edge_ticks(source)
    source = _add_sentinel_ticks(source)

    # Breakdown
    breakdown_filter = config().analysis.breakdown_filters.inverted()
    source = _add_boolean_column(source, "is_breakdown", breakdown_filter)
    # External
    external_filter = config().analysis.external_filters
    source = _add_boolean_column(source, "is_external", external_filter)

    # Cumulative balances
    source = source.sort(SORT_ORDER).with_columns(
        balance_total=pl.col("amount").cum_sum(),
        balance_inexternal=pl.col("amount").cum_sum().over("is_external"),
        balance_account=pl.col("amount").cum_sum().over("account"),
        balance_source=pl.col("amount").cum_sum().over("account", "source"),
    )

    # Filter - can only happen after calculating cumulative balances
    source = config().analysis.filters.apply(source)

    # Anonymization - after calculations and filters based on source info,
    # but before deriving new columns based on existing columns.
    if config().analysis.anonymization.enable:
        source = _anonymize_data(source)

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
    delimiter = config().general.multi_column_delimiter
    tags = pl.col("tag") + delimiter + pl.col("subtag")
    return df.with_columns(tags=tags)


def _derive_account_source(df):
    delimiter = config().general.multi_column_delimiter
    account_source = pl.col("account") + delimiter + pl.col("source")
    return df.with_columns(account_source=account_source)


def _truncate_month(df, *, by_clock: bool = False, by_data: bool = False):
    if not by_clock and not by_data:
        return df
    truncate_last_clock = datetime.date.today().replace(day=1)
    truncate_last_data = df["date"].max().replace(day=1)
    truncate_last = max(truncate_last_clock, truncate_last_data)
    if by_clock:
        truncate_last = min(truncate_last, truncate_last_clock)
    if by_data:
        truncate_last = min(truncate_last, truncate_last_data)
    return df.filter(pl.col("date") < truncate_last)


def _add_boolean_column(df, name, filters):
    hashes = filters.apply(df).select("hash")
    return df.with_columns(pl.col("hash").is_in(hashes).alias(name))


def _add_edge_ticks(df):
    config_min = config().analysis.edge_ticks.min
    config_max = config().analysis.edge_ticks.max
    dfs = [df.with_columns(is_edge_tick=pl.lit(False))]
    if config_min.enable:
        min_date = df["date"].min()
        min_delta = datetime.timedelta(days=config_min.pad_days)
        edge_date = min_date - min_delta
        if config_min.cap_same_month:
            edge_date = max(min_date.replace(day=1), edge_date)
        new_df = _generate_edge_ticks(df, edge_date)
        new_df = new_df.with_columns(is_edge_tick=pl.lit(True))
        dfs.append(new_df)
    if config_max.enable:
        max_date = df["date"].max()
        max_delta = datetime.timedelta(days=config_max.pad_days)
        edge_date = max_date + max_delta
        if config_max.cap_same_month:
            last_day = calendar.monthrange(max_date.year, max_date.month)[1]
            edge_date = min(max_date.replace(day=last_day), edge_date)
        new_df = _generate_edge_ticks(df, edge_date)
        new_df = new_df.with_columns(is_edge_tick=pl.lit(True))
        dfs.append(new_df)
    return pl.concat(dfs)


def _generate_edge_ticks(df, date):
    account_sources = df.group_by("account", "source").agg(pl.len())
    generated = pl.DataFrame().with_columns(
        account=account_sources["account"],
        source=account_sources["source"],
        date=pl.lit(date),
        amount=pl.lit(0.0),
        description=pl.lit(EDGE_TICK_DESCRIPTION),
        tag=pl.lit(EDGE_TICK_TAG),
        subtag=pl.lit(EDGE_TICK_SUBTAG),
    )
    return derive_hash(generated).select(df.columns)


def _add_sentinel_ticks(df):
    df = df.with_columns(is_sentinel_tick=pl.lit(False))
    all_dates = df["date"]
    months = pl.Series(_months_in_range(all_dates.min(), all_dates.max()))
    combinations = (
        df.group_by("tag", "subtag")
        .count()
        .join(pl.DataFrame(dict(date=months)), how="cross")
        .join(df.group_by("account").count().select("account"), how="cross")
        .join(df.group_by("source").count().select("source"), how="cross")
    )
    sentinels = combinations.with_columns(
        amount=pl.lit(0.0),
        description=pl.lit(SENTINEL_TICK_DESCRIPTION),
        is_edge_tick=pl.lit(False),
        is_sentinel_tick=pl.lit(True),
    )
    sentinels = derive_hash(sentinels).select(df.columns)
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


def _anonymize_data(df):
    conf = config().analysis.anonymization
    min_scale, max_scale = conf.scale
    scale = random.random() * (max_scale - min_scale) + min_scale
    amount_col = pl.col("amount") * scale
    new_columns = [amount_col.alias("amount")]
    if conf.anonymize_accounts:
        new_columns.append(_remap_column(df, "account", ANON_NAMES))
    if conf.anonymize_sources:
        new_columns.append(_remap_column(df, "source", ANON_SOURCES))
    if conf.anonymize_descriptions:
        new_columns.append(
            pl.col("description")
            .map_elements(_generate_hex, return_dtype=pl.String)
            .alias("description")
        )
    if conf.anonymize_tags:
        new_columns.append(_remap_column(df, "tag", ANON_TAGS))
        new_columns.append(_remap_column(df, "subtag", ANON_TAGS))
    return df.with_columns(new_columns)


def _remap_column(df, column_name, new_values):
    original = list(df.group_by(column_name).agg(pl.len())[column_name])
    anon = list(new_values)
    random.shuffle(original)
    random.shuffle(anon)
    return pl.col(column_name).replace(dict(zip(original, anon))).alias(column_name)


def _generate_hex(*args):
    return "".join(random.choice(string.hexdigits) for _i in range(10))
