import collections
import shutil
from typing import NamedTuple, Optional

import arrow
import polars as pl
import readchar

from finalyze.config import TagPresetRule, config
from finalyze.display import flip_rtl_str, print_table
from finalyze.source.raw import derive_hash, load_source_data
from finalyze.source.schema import TAGGED_SCHEMA, validate_schema

LINE_SEPARATOR = "===================="
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


def run():
    if config().tag.delete_filters.has_effect:
        delete_filtered_tags()
    if config().tag.delete_unused:
        delete_unused_tags()
    tag_interactively()


def tag_interactively():
    source_data = load_source_data()
    source_data = derive_hash(source_data)
    preset_hashes = get_tag_preset_hashes(source_data)
    source_data = source_data.filter(~pl.col("hash").is_in(preset_hashes)).drop("hash")
    if config().tag.default_tag:
        default_tags = Tags(config().tag.default_tag, config().tag.default_subtag)
    else:
        default_tags = None
    tagger = Tagger(source_data=source_data, default_tags=default_tags)
    performed_tagging = tagger.tag_interactively()
    if not performed_tagging:
        print("No tags missing.")
    if config().tag.print_result:
        print(LINE_SEPARATOR)
        print(tagger.describe_all_tags())


def apply_tags(
    data,
    *,
    preset_rules: Optional[list[TagPresetRule]] = None,
):
    tags = read_tags_file()
    cleaned_data = data.drop(*TAG_SCHEMA, strict=False)
    hashed_data = derive_hash(cleaned_data)
    tagged = hashed_data.join(tags, on="hash", how="left")
    if preset_rules is not None:
        tagged = apply_tag_presets(tagged, preset_rules)
    validate_schema(tagged, TAGGED_SCHEMA)
    return tagged


def get_tag_preset_hashes(source_data):
    all_hashes = set()
    for preset in config().tag.preset_rules:
        filters = preset.filters
        filters.tags = None
        filters.subtags = None
        if not filters.has_effect:
            continue
        all_hashes.update(filters.apply(source_data)["hash"])
    return pl.Series(list(all_hashes), dtype=pl.UInt64)


def apply_tag_presets(data, preset_rules):
    for preset in reversed(preset_rules):
        filters = preset.filters
        filters.tags = None
        filters.subtags = None
        if not filters.has_effect:
            continue
        filtered = filters.apply(data).with_columns(
            pl.lit(preset.tag).alias("tag"),
            pl.lit(preset.subtag).alias("subtag"),
        )
        data = data.filter(~pl.col("hash").is_in(filtered.select("hash")))
        data = pl.concat((data, filtered))
    return data


def read_tags_file():
    tags_file = config().general.tags_file
    if not tags_file.is_file():
        tags_file.write_text(",".join(TAG_SCHEMA.keys()))
    return pl.read_csv(tags_file, schema=TAG_SCHEMA)


def write_tags_file(data):
    validate_schema(data, TAG_SCHEMA)
    tags_file = config().general.tags_file
    data.sort(*TAG_SCHEMA.keys()).write_csv(tags_file)


def delete_unused_tags():
    tags_data = read_tags_file()
    source_data = load_source_data()
    tagged_data = apply_tags(source_data)

    delete_data = tags_data.filter(~pl.col("hash").is_in(tagged_data.select("hash")))
    delete_summary = (
        delete_data.group_by("tag", "subtag").len("entries").sort("tag", "subtag")
    )
    tag_hashes = delete_data["hash"]
    if len(tag_hashes) == 0:
        return
    print_table(delete_data, "Unused tags to delete")
    print_table(delete_summary, "Unused tags to delete")
    _delete_tags(tag_hashes)


def delete_filtered_tags():
    source_data = load_source_data()
    tagged_data = apply_tags(source_data)

    delete_data = config().tag.delete_filters.apply(tagged_data)
    delete_summary = (
        delete_data.group_by("tag", "subtag").len("entries").sort("tag", "subtag")
    )
    tag_hashes = delete_data["hash"]
    if len(tag_hashes) == 0:
        return
    print_table(delete_data, "Entries of tags to delete")
    print_table(delete_summary, "Tags to delete")
    _delete_tags(tag_hashes)


def _delete_tags(tag_hashes):
    if len(tag_hashes) == 0:
        return

    tags_data = read_tags_file()
    remaining_tags = tags_data.filter(~pl.col("hash").is_in(tag_hashes))

    if input("Delete tags? [y/N] ").lower() not in ("y", "yes"):
        print("Aborted deleting tags.")
        return
    tags_file = config().general.tags_file
    if tags_file.is_file():
        timestamp = arrow.now().format("YYYY-MM-DD_HH-mm-ssSS")
        bak_filename = f"{tags_file.stem}__{timestamp}.csv"
        backup_file_path = tags_file.parent / "bak" / bak_filename
        shutil.copy2(tags_file, backup_file_path)
        backup_file_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Backed up tags at {backup_file_path}")
    write_tags_file(remaining_tags)
    print("Deleted tags.")


class Tagger:
    def __init__(self, *, source_data, default_tags):
        self.default_tags = default_tags
        self.source = apply_tags(source_data).sort("date")
        self.tags = read_tags_file()

    def apply_tags(self, index, tags):
        row_hash = self.get_row(index)["hash"]
        new_tag_data = {"tag": tags.tag, "subtag": tags.subtag, "hash": row_hash}
        new_tags_row = pl.DataFrame(new_tag_data, schema=TAG_SCHEMA)
        self.tags = (
            pl.concat([self.tags, new_tags_row])
            .unique(subset="hash", keep="last")
            .sort(*TAG_SCHEMA)
        )
        write_tags_file(self.tags)
        self.source = apply_tags(self.source)

    def describe_row(self, index):
        row = self.get_row(index)
        if config().display.flip_rtl:
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

    def guess_tags(self, index: int) -> Tags:
        tag_descriptions = collections.defaultdict(set)
        row = self.get_row(index)
        target_description = row["description"]
        for row in self.source.sort("date", descending=True).iter_rows(named=True):
            if row["tag"] is None:
                continue
            tags = Tags(row["tag"], row["subtag"])
            row_description = row["description"]
            tag_descriptions[tags].add(row_description)
            if row_description == target_description:
                print(f"Guess from: {row}")
                return tags
        if self.default_tags is not None:
            return self.default_tags
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

    def tag_interactively(self):
        skipped_indices = set()
        if self.get_untagged_index(skipped_indices) is None:
            return False
        while True:
            index = self.get_untagged_index(skipped_indices)
            if index is None:
                return
            print(f"\n\n{LINE_SEPARATOR}")
            guess = self.guess_tags(index)
            print()
            prompt_text_lines = [
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
            print("> ", end="", flush=True)
            key = readchar.readkey()
            print(f"{key}\n")
            if key in ("q"):
                break
            elif key in ("t"):
                print(LINE_SEPARATOR)
                print(self.describe_all_tags())
            elif key in ("s"):
                skipped_indices.add(index)
                print("Skipped...")
            elif key in ("i", readchar.key.LF, readchar.key.SPACE):
                if (tag := input("Tag (or 'cancel'): ")) == "cancel":
                    continue
                if (subtag := input("Subtag (or 'cancel'): ")) == "cancel":
                    continue
                new_tags = Tags(tag, subtag)
                self.apply_tags(index, new_tags)
                print(f"Applied: {new_tags}")
            elif key in ("g", readchar.key.TAB):
                self.apply_tags(index, guess)
                print(f"Applied: {guess}")
            else:
                print("No action selected.")
        return True
