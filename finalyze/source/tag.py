import dataclasses
import shutil

import polars as pl

from finalyze.display import flip_rtl_columns, print_table
from finalyze.filters import Filters
from finalyze.source import source
from finalyze.source.data import TAGGED_SCHEMA, validate_schema

TAG_SCHEMA = {
    "tag": pl.String,
    "subtag": pl.String,
    "hash": pl.UInt64,
}


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
    if global_args.flip_rtl:
        source_data = flip_rtl_columns(source_data)
    if command_args.delete:
        delete_tags(source_data, global_args.tags_file, command_args.filters)
        return
    tagger = Tagger(source_data=source_data, tags_file=global_args.tags_file)
    print(tagger.describe_all_tags())
    tagger.tag_interactively(command_args.default_tags)


def apply_tags(data, tags_file):
    data = data.drop("hash", "tag", "subtag", strict=False)
    hash_column = pl.concat_str(("date", "amount")).hash()
    data = data.with_columns(hash_column.alias("hash"))
    tags = read_tags_file(tags_file)
    tagged = data.join(tags, on="hash", how="left")
    validate_schema(tagged, TAGGED_SCHEMA)
    return tagged


def read_tags_file(tags_file):
    if not tags_file.is_file():
        tags_file.write_text(",".join(TAG_SCHEMA.keys()))
    return pl.read_csv(tags_file, schema=TAG_SCHEMA)


def write_tags_file(data, tags_file):
    data.sort("tag", "subtag", "hash").write_csv(tags_file)


def delete_tags(source_data, tags_file, filters):
    tagged_data = apply_tags(source_data, tags_file)
    tags_data = read_tags_file(tags_file)

    delete_data = filters.filter_data(tagged_data)
    remaining_tags = tags_data.filter(~pl.col("hash").is_in(delete_data.select("hash")))

    delete_summary = (
        delete_data.group_by(["tag", "subtag"])
        .len()
        .rename({"len": "entries"})
        .sort(["tag", "subtag"])
    )
    print_table(delete_data, "Entries to delete")
    print_table(delete_summary, "Tags to delete")
    if input("Delete tags? [y/N] ").lower() not in ("y", "yes"):
        print("Aborted.")
        exit(1)
    if tags_file.is_file():
        shutil.copy2(tags_file, f"{tags_file}.bak")
    write_tags_file(remaining_tags, tags_file)


class Tagger:
    def __init__(self, *, source_data, tags_file):
        self.tags_file = tags_file
        self.source = apply_tags(source_data, tags_file)
        self.tags = read_tags_file(tags_file)

    def apply_tags(self, index, tag, subtag):
        row_hash = self.get_row(index)["hash"]
        new_tag_data = {"hash": row_hash, "tag": tag, "subtag": subtag}
        new_tags_row = pl.DataFrame(new_tag_data, schema=TAG_SCHEMA)
        all_tags = pl.concat([self.tags, new_tags_row])
        all_tags = all_tags.unique(subset="hash", keep="last")
        self.tags = all_tags.sort("tag", "subtag", "hash")
        write_tags_file(self.tags, self.tags_file)
        self.source = apply_tags(self.source, self.tags_file)

    def describe_row(self, index):
        row = self.get_row(index)
        lines = [
            "Applying tags for:",
            f" Account: {row['account']}",
            f"  Source: {row['source']}",
            f"    Date: {row['date']}",
            f"  Amount: {row['amount']}",
            f"    Desc: {row['description']}",
        ]
        return "\n".join(lines)

    def describe_all_tags(self):
        all_tags = {}
        for row in self.source.iter_rows(named=True):
            key = (row["tag"], row["subtag"])
            all_tags.setdefault(key, 0)
            all_tags[key] += 1
        nulls = all_tags.pop((None, None), 0)
        lines = ["All existing tags:"] + [
            f"  [ {count:>3} ]  {tag:>20} :: {subtag}"
            for (tag, subtag), count in sorted(all_tags.items())
        ]
        lines.extend(["", f"  [[ {nulls:>3} Untagged entries   ]]"])
        return "\n".join(lines)

    def guess_tags(self, index, default):
        tag_descriptions = {}
        target_description = self.get_row(index)["description"]
        for row in self.source.sort("date", descending=True).iter_rows(named=True):
            if row["tag"] is None:
                continue
            key = (row["tag"], row["subtag"])
            row_description = row["description"]
            tag_descriptions.setdefault(key, set())
            tag_descriptions[key].add(row_description)
            if row_description == target_description:
                return key
        if default is not None:
            return _split_tag_text(default)
        if not tag_descriptions:
            return ("unknown", "")
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
        line_separator = "===================="
        while True:
            index = self.get_untagged_index()
            if index is None:
                break
            guess1, guess2 = self.guess_tags(index, default_tags)
            guess_repr = guess1
            if guess2:
                guess_repr = f"{guess_repr} [{guess2}]"
            print()
            print(line_separator)
            print(self.describe_all_tags())
            print()
            print(self.describe_row(index))
            print(f"   Guess: {guess_repr}")
            print()
            user_input = input("Tags: ")
            if user_input == "":
                tag, subtag = guess1, guess2
            else:
                tag, subtag = _split_tag_text(user_input, ",")
            self.apply_tags(index, tag, subtag)


def _split_tag_text(text, separator: str = ","):
    if separator not in text:
        text += separator
    tag, subtag = text.split(separator, 1)
    return tag.strip(), subtag.strip()
