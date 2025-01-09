import collections
import shutil
from typing import NamedTuple, Optional

import polars as pl
import readchar

from finalyze.display import flip_rtl_str, print_table
from finalyze.source.data import TAGGED_SCHEMA, load_source_data, validate_schema

LINE_SEPARATOR = "===================="
HASH_COLUMNS = ("account", "source", "date", "description", "amount")
TAG_SCHEMA = {
    "tag": pl.String,
    "subtag": pl.String,
    "hash": pl.UInt64,
}


class Tags(NamedTuple):
    tag: str
    subtag: Optional[str] = None

    def __str__(self):
        if self.subtag:
            return f"{self.tag} [{self.subtag}]"
        return self.tag


def run(config):
    if config.tag.delete_filters.has_effect:
        delete_tags(config)
    tag_interactively(config)


def tag_interactively(config):
    source_data = load_source_data(config.general.source_dir)
    tagger = Tagger(
        source_data=source_data,
        tags_file=config.general.tags_file,
        flip_rtl=config.display.flip_rtl,
    )
    if config.tag.default_tag:
        default_tags = Tags(config.tag.default_tag, config.tag.default_subtag)
    else:
        default_tags = None
    performed_tagging = tagger.tag_interactively(default_tags)
    if not performed_tagging:
        print("No tags missing.")
    if config.tag.print_result:
        print(LINE_SEPARATOR)
        print(tagger.describe_all_tags())


def apply_tags(data, tags_file, *, hash_columns: list[str] = HASH_COLUMNS):
    if not hash_columns:
        raise ValueError("Need columns for hashing")
    tags = read_tags_file(tags_file)
    tagged = (
        data.drop(*TAG_SCHEMA, strict=False)
        .with_columns(pl.concat_str(*hash_columns).hash().alias("hash"))
        .join(tags, on="hash", how="left")
    )
    validate_schema(tagged, TAGGED_SCHEMA)
    return tagged


def read_tags_file(tags_file):
    if not tags_file.is_file():
        tags_file.write_text(",".join(TAG_SCHEMA.keys()))
    return pl.read_csv(tags_file, schema=TAG_SCHEMA)


def write_tags_file(data, tags_file):
    validate_schema(data, TAG_SCHEMA)
    data.sort(*TAG_SCHEMA.keys()).write_csv(tags_file)


def delete_tags(config):
    tags_file = config.general.tags_file

    tags_data = read_tags_file(tags_file)
    source_data = load_source_data(config.general.source_dir)
    tagged_data = apply_tags(source_data, tags_file)

    delete_data = config.tag.delete_filters.apply(tagged_data)
    remaining_tags = tags_data.filter(~pl.col("hash").is_in(delete_data.select("hash")))

    delete_summary = (
        delete_data.group_by("tag", "subtag").len("entries").sort("tag", "subtag")
    )
    print_table(delete_data, "Entries to delete")
    print_table(delete_summary, "Tags to delete")
    if input("Delete tags? [y/N] ").lower() not in ("y", "yes"):
        print("Aborted deleting tags.")
        return
    if tags_file.is_file():
        shutil.copy2(tags_file, f"{tags_file}.bak")
    write_tags_file(remaining_tags, tags_file)
    print("Deleted tags.")


class Tagger:
    def __init__(self, *, source_data, tags_file, flip_rtl):
        self.tags_file = tags_file
        self.flip_rtl = flip_rtl
        self.source = apply_tags(source_data, tags_file).sort("date")
        self.tags = read_tags_file(tags_file)

    def apply_tags(self, index, tags):
        row_hash = self.get_row(index)["hash"]
        new_tag_data = {"tag": tags.tag, "subtag": tags.subtag, "hash": row_hash}
        new_tags_row = pl.DataFrame(new_tag_data, schema=TAG_SCHEMA)
        self.tags = (
            pl.concat([self.tags, new_tags_row])
            .unique(subset="hash", keep="last")
            .sort(*TAG_SCHEMA)
        )
        write_tags_file(self.tags, self.tags_file)
        self.source = apply_tags(self.source, self.tags_file)

    def describe_row(self, index):
        row = self.get_row(index)
        if self.flip_rtl:
            for key, value in row.items():
                if isinstance(value, str):
                    row[key] = flip_rtl_str(value)
        lines = [
            f"    Hash: {row['hash']}",
            f" Account: {row['account']}",
            f"  Source: {row['source']}",
            f"    Date: {row['date']}",
            f"  Amount: {row['amount']}",
            f"    Desc: {row['description']}",
        ]
        return "\n".join(lines)

    def describe_all_tags(self):
        tag_counts = self.source.group_by("tag", "subtag").len().sort("tag", "subtag")
        all_tags = {
            Tags(row["tag"], row["subtag"]): row["len"]
            for row in tag_counts.iter_rows(named=True)
        }
        nulls = all_tags.pop(Tags(None, None), 0)
        lines = ["All existing tags:"] + [
            f"  [ {count:>5} ]  {tags.tag:>20} :: {tags.subtag}"
            for tags, count in all_tags.items()
        ]
        lines.extend(["", f"  [[ {nulls:>3} Untagged entries   ]]"])
        return "\n".join(lines)

    def guess_tags(self, index: int, default: Optional[Tags]) -> Tags:
        tag_descriptions = collections.defaultdict(set)
        row = self.get_row(index)
        target_description = row["description"]
        if any(row[k] is None for k in HASH_COLUMNS):
            raise ValueError(
                "Row missing required fields;"
                " possibly an empty line in source csv file?"
            )
        for row in self.source.sort("date", descending=True).iter_rows(named=True):
            if row["tag"] is None:
                continue
            tags = Tags(row["tag"], row["subtag"])
            row_description = row["description"]
            tag_descriptions[tags].add(row_description)
            if row_description == target_description:
                print(f"Guess from: {row}")
                return tags
        if default is not None:
            return default
        if not tag_descriptions:
            return Tags("unknown", "")
        description_counts = {
            len(descriptions): tags for tags, descriptions in tag_descriptions.items()
        }
        greatest_count = max(description_counts.keys())
        return description_counts[greatest_count]

    def get_row(self, index):
        return self.source.row(index, named=True)

    def get_untagged_index(self, ignore_indices=None):
        ignores = set(ignore_indices or [])
        untagged_indices = set(self.source["tag"].is_null().arg_true())
        indices = sorted(untagged_indices - ignores)
        if indices:
            return indices[0]
        return None

    def tag_interactively(self, default_tags):
        skipped_indices = set()
        if self.get_untagged_index(skipped_indices) is None:
            return False
        while True:
            index = self.get_untagged_index(skipped_indices)
            if index is None:
                return
            guess = self.guess_tags(index, default_tags)
            prompt_text_lines = [
                LINE_SEPARATOR,
                "Currently tagging:",
                self.describe_row(index),
                f"   Guess: {guess}",
                "",
                "(i/enter/space)   Input tags manually",
                "(g/tab)           Use guess",
                "(s)               Skip entry",
                "(t)               Show existing tags",
                "(q)               Quit",
            ]
            print("\n".join(prompt_text_lines))
            key = readchar.readkey()
            if key in ("q"):
                break
            elif key in ("t"):
                print(LINE_SEPARATOR)
                print(self.describe_all_tags())
            elif key in ("s"):
                skipped_indices.add(index)
            elif key in ("i", readchar.key.LF, readchar.key.SPACE):
                if (tag := input("Tag (or 'cancel'): ")) == "cancel":
                    continue
                if (subtag := input("Subtag (or 'cancel'): ")) == "cancel":
                    continue
                self.apply_tags(index, Tags(tag, subtag))
            elif key in ("g", readchar.key.TAB):
                self.apply_tags(index, guess)
        return True
