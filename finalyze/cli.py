import argparse
import dataclasses
import sys
from pathlib import Path

from finalyze import analysis, source, tag

APP_NAME = "finalyze"
DESCRIPTION = "Personal financial analysis tool"
TAGS_FILENAME = "tags.csv"
DATA_DIR = Path.home() / ".local" / "share" / APP_NAME.lower() / "data"


@dataclasses.dataclass
class GlobalArgs:
    data_dir: Path
    dataset_name: str
    use_dataset_tags: bool
    flip_rtl: bool

    @property
    def dataset_dir(self):
        return self.data_dir / self.dataset_name

    @property
    def tags_file(self):
        if self.use_dataset_tags:
            return self.dataset_dir / TAGS_FILENAME
        return self.data_dir / TAGS_FILENAME

    @property
    def source_dir(self):
        return self.dataset_dir / "sources"

    def __post_init__(self):
        for directory in (self.data_dir, self.dataset_dir, self.source_dir):
            directory.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def configure_parser(parser):
        parser.add_argument(
            "-d",
            "--dataset-name",
            default="default",
            help="Dataset name",
        )
        parser.add_argument(
            "--data-dir",
            help="Data directory",
        )
        parser.add_argument(
            "--dataset-tags",
            action="store_true",
            help="Use isolated tags file for dataset instead of global tags file",
        )
        parser.add_argument(
            "--flip-rtl",
            action="store_true",
            help="Flip non-English (RTL) text",
        )

    @classmethod
    def from_args(cls, args):
        return cls(
            data_dir=DATA_DIR,
            dataset_name=args.dataset_name,
            use_dataset_tags=args.dataset_tags,
            flip_rtl=args.flip_rtl,
        )


def main():
    parser = argparse.ArgumentParser(prog=APP_NAME, description=DESCRIPTION)
    GlobalArgs.configure_parser(parser)
    # Subcommands
    subparsers = parser.add_subparsers(dest="subcommand")
    source.Args.configure_parser(
        subparsers.add_parser("source", help="Import source data")
    )
    tag.Args.configure_parser(subparsers.add_parser("tag", help="Tag source data"))
    analysis.Args.configure_parser(
        subparsers.add_parser("analyze", help="Analyze historical data")
    )
    # Parse and validate
    args = parser.parse_args()
    if args.subcommand is None:
        parser.print_help(file=sys.stderr)
        print("\n\nNo subcommand selected.", file=sys.stderr)
        exit(1)
    global_args = GlobalArgs.from_args(args)
    command_args = args.command_class.from_args(args)
    args.run(command_args, global_args)
