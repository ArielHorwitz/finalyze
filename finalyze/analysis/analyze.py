import random
import string
import subprocess
import sys
from pathlib import Path

import polars as pl

from finalyze.display import print_table
from finalyze.source.data import enrich_source, load_source_data
from finalyze.source.tag import apply_tags

from . import plot
from .tables import get_tables

ANON_NAMES = ["Einstein", "Newton", "Curie", "Galileo", "Darwin", "Turing", "Planck", "Hawking", "Pasteur", "Lovelace", "Bohr", "Maxwell"]  # fmt: skip  # noqa: disable=E501
ANON_SOURCES = ["Chemistry", "Biology", "Psychology", "Geology", "Sociology", "Philosophy", "Physics", "Mathematics", "Economics", "Astronomy"]  # fmt: skip  # noqa: disable=E501
ANON_TAGS = ["Aardvark", "Albatross", "Alligator", "Ant", "Armadillo", "Avocet", "Bat", "Bear", "Bee", "Beetle", "Bison", "Bumblebee", "Butterfly", "Capybara", "Caracal", "Caribou", "Cat", "Caterpillar", "Centipede", "Chicken", "Chimpanzee", "Chinchilla", "Clam", "Cow", "Crab", "Crocodile", "Crow", "Deer", "Dingo", "Dog", "Dolphin", "Donkey", "Dove", "Duck", "Dugong", "Eagle", "Eel", "Elephant", "Emu", "Falcon", "Ferret", "Finch", "Flamingo", "Fox", "Frog", "Geese", "Giraffe", "Goat", "Goose", "Gorilla", "Guinea Pig", "Gull", "Hare", "Hawk", "Hedgehog", "Hippopotamus", "Hornet", "Horse", "Hyena", "Ibex", "Iguana", "Jaguar", "Javelina", "Jellyfish", "Kangaroo", "Kiwi", "Koala", "Ladybug", "Lemur", "Lion", "Lizard", "Llama", "Lobster", "Loon", "Lynx", "Macaw", "Mallard", "Meerkat", "Millipede", "Monkey", "Moose", "Moth", "Mule", "Mussel", "Narwhal", "Newt", "Ocelot", "Octopus", "Orangutan", "Orca", "Ostrich", "Owl", "Ox", "Oyster", "Pangolin", "Parrot", "Peacock", "Penguin", "Pig", "Pigeon", "Platypus", "Polar Bear", "Puffin", "Quail", "Quokka", "Rabbit", "Raccoon", "Rattlesnake", "Rhinoceros", "Salamander", "Scorpion", "Seal", "Shark", "Sheep", "Shrimp", "Skunk", "Slug", "Snail", "Snake", "Sparrow", "Spider", "Squid", "Squirrel", "Starfish", "Swan", "Tapir", "Tarantula", "Tiger", "Toad", "Turkey", "Turtle", "Umbrellabird", "Vole", "Vulture", "Wallaby", "Walrus", "Wasp", "Weasel", "Whale", "Wolf", "Worm", "Xerus", "Yak", "Zebra", "Zebu"]  # fmt: skip  # noqa: disable=E501


def run(config):
    source_data = load_source_data(config.general.source_dir)
    source_data = apply_tags(
        source_data,
        config.general.tags_file,
        preset_rules=config.tag.preset_rules,
    )
    source_data = config.analysis.filters.apply(source_data)
    if config.analysis.add_edge_ticks:
        source_data = _add_edge_ticks(source_data)
    if config.analysis.anonymization.enable:
        source_data = _anonymize_data(source_data, config)
    source_data = enrich_source(
        source_data,
        delimiter=config.general.multi_column_delimiter,
    )
    if config.analysis.print_source:
        print_table(source_data, "Source data")
    if not config.analysis.allow_untagged:
        _validate_tags(source_data, flip_rtl=config.display.flip_rtl)
    # Tables
    tables = get_tables(source_data, config)
    if config.analysis.print_tables:
        for table in tables:
            print(table)
            print_table(table.with_totals(), table.title)
    # Plots
    source_data_display = source_data.select(
        "account", "source", "date", "amount", "tag", "subtag", "description", "hash"
    )
    output_file_stem = config.analysis.graphs.title.lower()
    output_file = config.general.output_dir / f"{output_file_stem}.html"
    print(f"Exporting plots to: {output_file}")
    html_text = plot.get_html(source_data_display, tables, config)
    Path(output_file).write_text(html_text)
    if config.analysis.graphs.open:
        subprocess.run(["xdg-open", output_file])


def _add_edge_ticks(df):
    min_date = df["date"].min()
    max_date = df["date"].max()
    account_sources = df.group_by("account", "source").agg(pl.len())
    tick_df = pl.concat(
        pl.DataFrame()
        .with_columns(
            account=account_sources["account"],
            source=account_sources["source"],
            date=pl.lit(date),
            amount=pl.lit(0.0),
            description=pl.lit("auto-generated tick"),
            tag=pl.lit("other"),
            subtag=pl.lit("auto-tick"),
            hash=pl.lit(0).cast(pl.UInt64),
        )
        .select(df.columns)
        for date in (min_date, max_date)
    )
    return pl.concat((df, tick_df))


def _anonymize_data(df, config):
    conf = config.analysis.anonymization
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
        new_columns.extend(
            (
                _remap_column(df, "tag", ANON_TAGS),
                _remap_column(df, "subtag", ANON_TAGS),
            )
        )
    return df.with_columns(new_columns)


def _remap_column(df, column_name, new_values):
    original = list(df.group_by(column_name).agg(pl.len())[column_name])
    anon = list(new_values)
    random.shuffle(original)
    random.shuffle(anon)
    return pl.col(column_name).replace(dict(zip(original, anon))).alias(column_name)


def _generate_hex(*args):
    return "".join(random.choice(string.hexdigits) for _i in range(10))


def _validate_tags(df, flip_rtl):
    null_tags = df.filter(pl.col("tag").is_null())
    if null_tags.height:
        print_table(null_tags, "Missing tags")
        print(f"Missing {null_tags.height} tags", file=sys.stderr)
        exit(1)
