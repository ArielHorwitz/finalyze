import shutil

import polars as pl

from finalyze import utils
from finalyze.source import get_source_data

TAG_SCHEMA = {
    "tag1": pl.String,
    "tag2": pl.String,
    "hash": pl.UInt64,
}


def add_subparser(subparsers):
    parser = subparsers.add_parser("tag", help="Tag source data")
    parser.set_defaults(func=run)
    parser.add_argument(
        "--path",
        action="store_true",
        help="Print path to tags file and quit",
    )
    parser.add_argument(
        "-d",
        "--default",
        help="Set default tags (instead of using best guess suggestion)",
    )
    parser.add_argument(
        "-D",
        "--delete",
        action="store_true",
        help="Delete tags and quit",
    )
    parser.add_argument(
        "-1",
        "--filter-tag1",
        nargs="*",
        help="Filter tags for deletion (with --delete)",
    )
    parser.add_argument(
        "-2",
        "--filter-tag2",
        nargs="*",
        help="Filter subtags for deletion (with --delete)",
    )


def run(args):
    tags_file = args.tags_file
    print_path = args.path
    default_tag = args.default
    delete = args.delete
    filter_tag1 = args.filter_tag1
    filter_tag2 = args.filter_tag2
    if print_path:
        print(tags_file)
        return
    if delete:
        delete_tags(tags_file, filter_tag1=filter_tag1, filter_tag2=filter_tag2)
        return
    source_data = get_source_data(args)
    write_tags(source_data, tags_file, default_tag)


def apply_tags(data, tags_file):
    data = data.drop("hash", "tag1", "tag2", strict=False)
    hash_column = pl.concat_str(("date", "amount")).hash()
    data = data.with_columns(hash_column.alias("hash"))
    tags = read_tags_file(tags_file)
    return data.join(tags, on="hash", how="left")


class Tagger:
    def __init__(self, *, source_data, tags_file):
        self.tags_file = tags_file
        self.source = apply_tags(source_data, tags_file)
        self.tags = read_tags_file(tags_file)

    def apply_tags(self, index, tag1, tag2):
        row_hash = self.get_row(index)["hash"]
        new_tag_data = {"hash": row_hash, "tag1": tag1, "tag2": tag2}
        new_tags_row = pl.DataFrame(new_tag_data, schema=TAG_SCHEMA)
        all_tags = pl.concat([self.tags, new_tags_row])
        all_tags = all_tags.unique(subset="hash", keep="last")
        self.tags = all_tags.sort("tag1", "tag2", "hash")
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
            key = (row["tag1"], row["tag2"])
            all_tags.setdefault(key, 0)
            all_tags[key] += 1
        nulls = all_tags.pop((None, None), 0)
        lines = ["All existing tags:"] + [
            f"  [ {count:>3} ]  {tag1:>20} :: {tag2}"
            for (tag1, tag2), count in sorted(all_tags.items())
        ]
        lines.extend(["", f"  [[ {nulls:>3} Untagged entries   ]]"])
        return "\n".join(lines)

    def guess_tags(self, index, default):
        tag_descriptions = {}
        target_description = self.get_row(index)["description"]
        for row in self.source.sort("date", descending=True).iter_rows(named=True):
            if row["tag1"] is None:
                continue
            key = (row["tag1"], row["tag2"])
            row_description = row["description"]
            tag_descriptions.setdefault(key, set())
            tag_descriptions[key].add(row_description)
            if row_description == target_description:
                return key
        if default is not None:
            return split_tag_text(default)
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
        indices = tuple(self.source["tag1"].is_null().arg_true())
        if indices:
            return indices[0]
        return None


def write_tags(source_data, tags_file, default_tags):
    tagger = Tagger(source_data=source_data, tags_file=tags_file)
    print(tagger.describe_all_tags())
    line_separator = "===================="
    while True:
        index = tagger.get_untagged_index()
        if index is None:
            break
        guess1, guess2 = tagger.guess_tags(index, default_tags)
        guess_repr = guess1
        if guess2:
            guess_repr = f"{guess_repr} [{guess2}]"
        print()
        print(line_separator)
        print(tagger.describe_all_tags())
        print()
        print(tagger.describe_row(index))
        print(f"   Guess: {guess_repr}")
        print()
        user_input = input("Tags: ")
        if user_input == "":
            tag1, tag2 = guess1, guess2
        else:
            tag1, tag2 = split_tag_text(user_input, ",")
        tagger.apply_tags(index, tag1, tag2)


def delete_tags(tags_file, *, filter_tag1, filter_tag2):
    tags_data = read_tags_file(tags_file)
    predicates = [pl.lit(True)]
    if filter_tag1 is not None:
        predicates.append(pl.col("tag1").is_in(pl.Series(filter_tag1)))
    if filter_tag2 is not None:
        predicates.append(pl.col("tag2").is_in(pl.Series(filter_tag2)))
    predicate = predicates[0]
    for p in predicates[1:]:
        predicate = predicate & p
    filtered_data = (
        tags_data.filter(predicate)
        .group_by(["tag1", "tag2"])
        .len()
        .rename({"len": "entries"})
        .sort(["tag1", "tag2"])
    )
    utils.print_table(filtered_data, "Tags to delete")
    if input("Delete tags? [y/N] ").lower() not in ("y", "yes"):
        print("Aborted.")
        return
    if tags_file.is_file():
        shutil.copy2(tags_file, f"{tags_file}.bak")
    tags_data = tags_data.filter(~predicate)
    write_tags_file(tags_data, tags_file)


def read_tags_file(tags_file):
    if not tags_file.is_file():
        tags_file.write_text(",".join(TAG_SCHEMA.keys()))
    return pl.read_csv(tags_file, schema=TAG_SCHEMA)


def write_tags_file(data, tags_file):
    data.sort("tag1", "tag2", "hash").write_csv(tags_file)


def split_tag_text(text, separator: str = ","):
    if separator not in text:
        text += separator
    tag1, tag2 = text.split(separator, 1)
    return tag1.strip(), tag2.strip()
