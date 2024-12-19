import argparse
import dataclasses
import sys
import tomllib
from pathlib import Path

from finalyze.analysis import analyze
from finalyze.source import source, tag

APP_NAME = "finalyze"
DESCRIPTION = "Personal financial analysis tool"
TAGS_FILENAME = "tags.csv"
COLORS_FILENAME = "colors.toml"
PLOTS_FILENAME = "plots.html"
DATA_DIR = Path.home() / ".local" / "share" / APP_NAME.lower() / "data"
CONFIG_DIR = Path.home() / ".config" / APP_NAME.lower()
CONFIG_FILE = CONFIG_DIR / "config.toml"


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
    def source_dir(self):
        return self.dataset_dir / "sources"

    @property
    def tags_file(self):
        if self.use_dataset_tags:
            return self.dataset_dir / TAGS_FILENAME
        return self.data_dir / TAGS_FILENAME

    @property
    def colors_file(self):
        return self.data_dir / COLORS_FILENAME

    @property
    def plots_file(self):
        return self.data_dir / PLOTS_FILENAME

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
            "--dataset-tags",
            action="store_true",
            help="Use isolated tags file for dataset instead of global tags file",
        )
        parser.add_argument(
            "--flip-rtl",
            action="store_true",
            help="Flip non-English (RTL) text in the terminal",
        )

    @classmethod
    def from_args(cls, args):
        return cls(
            data_dir=DATA_DIR,
            dataset_name=args.dataset_name,
            use_dataset_tags=args.dataset_tags,
            flip_rtl=args.flip_rtl,
        )


def parse_args():
    parser = argparse.ArgumentParser(prog=APP_NAME, description=DESCRIPTION)
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
    # Get configured default arguments
    args = parser.parse_args()
    if args.subcommand is None:
        parser.print_help(file=sys.stderr)
        print("\n\nNo subcommand selected.", file=sys.stderr)
        exit(1)
    # Get system arguments
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    if not CONFIG_FILE.is_file():
        CONFIG_FILE.write_text("[cli]\nglobal = []")
    config = tomllib.loads(CONFIG_FILE.read_text())
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
