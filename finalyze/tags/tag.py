import dataclasses
import shutil

import polars as pl

from finalyze.display import print_table
from finalyze.filters import Filters
from finalyze.source import source
from finalyze.tags.tagger import Tagger, apply_tags, read_tags_file, write_tags_file


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
        Filters.configure_parser(parser.add_argument_group("filters"))

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
        delete_tags(source_data, global_args.tags_file, command_args.filters)
        return
    tagger = Tagger(source_data=source_data, tags_file=global_args.tags_file)
    print(tagger.describe_all_tags())
    tagger.tag_interactively(command_args.default_tags)


def delete_tags(source_data, tags_file, filters):
    tagged_data = apply_tags(source_data, tags_file)
    tags_data = read_tags_file(tags_file)

    delete_data = filters.filter_data(tagged_data)
    remaining_tags = tags_data.filter(~pl.col("hash").is_in(delete_data.select("hash")))

    delete_summary = (
        delete_data.group_by(["tag1", "tag2"])
        .len()
        .rename({"len": "entries"})
        .sort(["tag1", "tag2"])
    )
    print_table(delete_data, "Entries to delete")
    print_table(delete_summary, "Tags to delete")
    if input("Delete tags? [y/N] ").lower() not in ("y", "yes"):
        print("Aborted.")
        exit(1)
    if tags_file.is_file():
        shutil.copy2(tags_file, f"{tags_file}.bak")
    write_tags_file(remaining_tags, tags_file)
