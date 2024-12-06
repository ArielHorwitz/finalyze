import argparse
import sys
from pathlib import Path

from finalyze import analysis, source, tag

APP_NAME = "finalyze"
DESCRIPTION = "Personal financial analysis tool"


def main():
    parser = argparse.ArgumentParser(prog=APP_NAME, description=DESCRIPTION)
    parser.add_argument(
        "-v",
        "--verbose",
        help="Be verbose (can be used multiple times)",
        action="count",
        default=0,
    )
    parser.add_argument(
        "-d",
        "--dataset-name",
        default="default",
        help="Dataset name (default: 'default')",
    )
    parser.add_argument(
        "--data-dir",
        help=f"Data directory (default: ~/.local/{APP_NAME}/data)",
    )
    parser.add_argument(
        "--global-tags",
        action="store_true",
        help="Use global tag data file",
    )
    parser.add_argument(
        "--flip-rtl",
        action="store_true",
        help="Flip non-English (RTL) text",
    )
    subparsers = parser.add_subparsers(dest="subcommand")
    source.add_subparser(subparsers)
    tag.add_subparser(subparsers)
    analysis.add_subparser(subparsers)
    args = parser.parse_args()

    default_data_dir = Path.home() / ".local" / "share" / APP_NAME.lower() / "data"
    args.data_dir = Path(args.data_dir or default_data_dir).resolve()
    args.dataset_dir = args.data_dir / args.dataset_name
    args.dataset_dir.mkdir(parents=True, exist_ok=True)
    args.source_dir = args.dataset_dir / "sources"
    if args.global_tags:
        args.tags_file = args.data_dir / "tags.csv"
    else:
        args.tags_file = args.dataset_dir / "tags.csv"

    if args.verbose:
        print(f"{args=}")
    if args.subcommand is None:
        parser.print_help(file=sys.stderr)
        print("\n\nNo subcommand selected.", file=sys.stderr)
        exit(1)
    args.func(args)
