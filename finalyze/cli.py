import argparse
import dataclasses
import sys
from pathlib import Path

from finalyze import APP_DESCRIPTION, APP_NAME
from finalyze.analysis import analyze
from finalyze.config import load_config
from finalyze.source import source, tag

TAGS_FILENAME = "tags.csv"
PLOTS_FILENAME = "plots.html"
ROOT_DIR = Path.home() / ".local" / "share" / APP_NAME.lower()


@dataclasses.dataclass
class GlobalArgs:
    dataset_name: str
    tags_name: str
    flip_rtl: bool

    @property
    def dataset_dir(self):
        return ROOT_DIR / "parsed" / self.dataset_name

    @property
    def tags_dir(self):
        return ROOT_DIR / "tags"

    @property
    def output_dir(self):
        return ROOT_DIR / "output"

    @property
    def tags_file(self):
        return self.tags_dir / f"{self.tags_name}.csv"

    @property
    def plots_file(self):
        return self.output_dir / PLOTS_FILENAME

    def __post_init__(self):
        for directory in (self.dataset_dir, self.tags_dir, self.output_dir):
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
            "--tags-name",
            action="store_true",
            default="default",
            help="Tags file name",
        )
        parser.add_argument(
            "--flip-rtl",
            action="store_true",
            help="Flip non-English (RTL) text in the terminal",
        )

    @classmethod
    def from_args(cls, args):
        return cls(
            dataset_name=args.dataset_name,
            tags_name=args.tags_name,
            flip_rtl=args.flip_rtl,
        )


def add_developer_subcommand(subparsers):
    try:
        from finalyze.dev.dev import Args
    except ModuleNotFoundError:
        return
    Args.configure_parser(subparsers.add_parser("dev", help="Developer experiments"))


def parse_args():
    parser = argparse.ArgumentParser(prog=APP_NAME, description=APP_DESCRIPTION)
    GlobalArgs.configure_parser(parser)
    # Subcommands
    subparsers = parser.add_subparsers(dest="subcommand")
    source.Args.configure_parser(
        subparsers.add_parser("source", help="Import source data")
    )
    tag.Args.configure_parser(subparsers.add_parser("tag", help="Tag source data"))
    analyze.Args.configure_parser(
        subparsers.add_parser("analyze", help="Analyze historical data")
    )
    add_developer_subcommand(subparsers)
    # Get configured default arguments
    args = parser.parse_args()
    if args.subcommand is None:
        parser.print_help(file=sys.stderr)
        print("\n\nNo subcommand selected.", file=sys.stderr)
        exit(1)
    # Get system arguments
    config = load_config()
    config_global_args = config.get("cli", {}).get("global", [])
    config_command_args = config.get("cli", {}).get(args.subcommand, [])
    # Combine configured default and system arguments
    sys_arguments = sys.argv[1:]
    subcommand_index = sys_arguments.index(args.subcommand)
    sys_global_arguments = sys_arguments[:subcommand_index]
    sys_subcommand_arguments = sys_arguments[subcommand_index + 1 :]  # noqa: E203
    combined_arguments = [
        *config_global_args,
        *sys_global_arguments,
        args.subcommand,
        *config_command_args,
        *sys_subcommand_arguments,
    ]
    return parser.parse_args(combined_arguments)


def main():
    args = parse_args()
    global_args = GlobalArgs.from_args(args)
    command_args = args.command_class.from_args(args)
    args.run(command_args, global_args)
