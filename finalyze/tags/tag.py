import dataclasses
import shutil
from typing import Optional

import polars as pl

from finalyze.display import print_table
from finalyze.source.source import get_source_data
from finalyze.tags.tagger import read_tags_file, write_tags_file, Tagger


@dataclasses.dataclass
class Args:
    default_tags: bool
    delete: bool
    tags: Optional[list[str]]
    subtags: Optional[list[str]]

    @classmethod
    def configure_parser(cls, parser):
        parser.set_defaults(command_class=cls, run=run)
        parser.add_argument(
            "-d",
            "--default-tags",
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
            "--tags",
            nargs="*",
            help="Filter tags for deletion (with --delete)",
        )
        parser.add_argument(
            "-2",
            "--subtags",
            nargs="*",
            help="Filter subtags for deletion (with --delete)",
        )

    @classmethod
    def from_args(cls, args):
        return cls(
            **{
                field.name: getattr(args, field.name)
                for field in dataclasses.fields(cls)
            }
        )


def run(command_args, global_args):
    print(f"Tags file: {global_args.tags_file}")
    if command_args.delete:
        delete_tags(
            global_args.tags_file,
            filter_tag1=command_args.tags,
            filter_tag2=command_args.subtags,
        )
        return
    source_data = get_source_data(global_args)
    tagger = Tagger(source_data=source_data, tags_file=global_args.tags_file)
    print(tagger.describe_all_tags())
    tagger.tag_interactively(command_args.default_tags)


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
    print_table(filtered_data, "Tags to delete")
    if input("Delete tags? [y/N] ").lower() not in ("y", "yes"):
        print("Aborted.")
        return
    if tags_file.is_file():
        shutil.copy2(tags_file, f"{tags_file}.bak")
    tags_data = tags_data.filter(~predicate)
    write_tags_file(tags_data, tags_file)
