import collections
import dataclasses
import shutil
from typing import NamedTuple, Optional

import polars as pl
import readchar

from finalyze.display import flip_rtl_str, print_table
from finalyze.filters import Filters
from finalyze.source import source
from finalyze.source.data import TAGGED_SCHEMA, validate_schema

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

    @classmethod
    def from_str(cls, text, separator: str = ","):
        if separator not in text:
            text += separator
        tag, subtag = text.split(separator, 1)
        return cls(tag=tag.strip(), subtag=subtag.strip())

    def __str__(self):
        if self.subtag:
            return f"{self.tag} [{self.subtag}]"
        return self.tag


@dataclasses.dataclass
class Args:
    default_tags: bool
    delete: bool
    filters: Filters

    @classmethod
    def configure_parser(cls, parser):
        parser.set_defaults(command_class=cls, run=run)
        parser.add_argument(
            "--default-tags",
            help="Set default tags (instead of using best guess suggestion)",
        )
        parser.add_argument(
            "--delete",
            action="store_true",
            help="Delete tags and quit",
        )
        Filters.configure_parser(parser)

    @classmethod
    def from_args(cls, args):
        return cls(
            default_tags=args.default_tags,
            delete=args.delete,
            filters=Filters.from_args(args),
        )


def run(command_args, global_args):
    print(f"Tags file: {global_args.tags_file}")
    source_data = source.load_source_data(global_args.source_dir)
    if command_args.delete:
        delete_tags(
            source_data,
            global_args.tags_file,
            command_args.filters,
            global_args.flip_rtl,
        )
        return
    tagger = Tagger(
        source_data=source_data,
        tags_file=global_args.tags_file,
        flip_rtl=global_args.flip_rtl,
    )
    tagger.tag_interactively(command_args.default_tags)
    print(LINE_SEPARATOR)
    print(tagger.describe_all_tags())


def apply_tags(data, tags_file, *, hash_columns: list[str] = HASH_COLUMNS):
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


def delete_tags(source_data, tags_file, filters, flip_rtl):
    tagged_data = apply_tags(source_data, tags_file)
    tags_data = read_tags_file(tags_file)

    delete_data = filters.filter_data(tagged_data)
    remaining_tags = tags_data.filter(~pl.col("hash").is_in(delete_data.select("hash")))

    delete_summary = (
        delete_data.group_by("tag", "subtag").len("entries").sort("tag", "subtag")
    )
    print_table(delete_data, "Entries to delete", flip_rtl=flip_rtl)
    print_table(delete_summary, "Tags to delete", flip_rtl=flip_rtl)
    if input("Delete tags? [y/N] ").lower() not in ("y", "yes"):
        print("Aborted.")
        exit(1)
    if tags_file.is_file():
        shutil.copy2(tags_file, f"{tags_file}.bak")
    write_tags_file(remaining_tags, tags_file)


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
        target_description = self.get_row(index)["description"]
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

    def get_untagged_index(self):
        indices = tuple(self.source["tag"].is_null().arg_true())
        if indices:
            return indices[0]
        return None

    def tag_interactively(self, default_tags):
        if default_tags:
            default_tags = Tags.from_str(default_tags)
        if self.get_untagged_index() is not None:
            print(LINE_SEPARATOR)
            print(self.describe_all_tags())
        while True:
            index = self.get_untagged_index()
            if index is None:
                break
            guess = self.guess_tags(index, default_tags)
            prompt_text_lines = [
                LINE_SEPARATOR,
                "Currently tagging:",
                self.describe_row(index),
                f"   Guess: {guess}",
                "",
                "(i/enter/space)   Input tags manually",
                "(g/tab)           Use guess",
                "(t)               Show existing tags",
                "(q)               Quit",
            ]
            print("\n".join(prompt_text_lines))
            key = readchar.readkey()
            if key in ("q"):
                exit(0)
            elif key in ("t"):
                print(LINE_SEPARATOR)
                print(self.describe_all_tags())
            elif key in ("i", readchar.key.LF, readchar.key.SPACE):
                if (tag := input("Tag (or 'cancel'): ")) == "cancel":
                    break
                if (subtag := input("Subtag (or 'cancel'): ")) == "cancel":
                    break
                self.apply_tags(index, Tags(tag, subtag))
            elif key in ("g", readchar.key.TAB):
                self.apply_tags(index, guess)
