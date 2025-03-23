import argparse
import pprint

from finalyze import APP_DESCRIPTION, APP_NAME
from finalyze.analysis import analyze
from finalyze.config import (
    CONFIG_DIR,
    get_config_file_names,
    load_config,
    write_default_config,
)
from finalyze.source import ingest, tag


def add_developer_subcommand(subparsers):
    try:
        from finalyze.dev.dev import configure_parser
    except ModuleNotFoundError:
        return
    configure_parser(subparsers.add_parser("dev", help="Developer experiments"))


def parse_args():
    parser = argparse.ArgumentParser(prog=APP_NAME, description=APP_DESCRIPTION)
    parser.set_defaults(run=run_pipeline)
    parser.add_argument(
        "-C",
        "--config-dir",
        help=f"Directory of config files [default: {CONFIG_DIR}]",
        default=CONFIG_DIR,
    )
    parser.add_argument(
        "-c",
        "--additional-configs",
        help="Additional config file names (see --list-configs)",
        action="append",
        default=[],
    )
    parser.add_argument(
        "-o",
        "--config-override",
        help=(
            "Override config using a valid TOML string "
            "(e.g. 'CATEGORY.OPTION=\"new_value\"')"
        ),
        action="append",
        default=[],
    )
    parser.add_argument(
        "-l",
        "--list-configs",
        help="List additional config file names and exit",
        action="store_true",
    )
    # Subcommands
    subparsers = parser.add_subparsers(
        dest="subcommand",
        title="subcommands",
        description="Run a specific subcommand of the pipeline",
        help="Pipeline subcommand",
        metavar="[SUBCOMMAND]",
    )
    subparsers.add_parser("ingest", help="Ingest source data").set_defaults(
        run=ingest.run
    )
    subparsers.add_parser("tag", help="Categorize transactions").set_defaults(
        run=tag.run
    )
    subparsers.add_parser("analyze", help="Analyze historical data").set_defaults(
        run=analyze.run
    )
    add_developer_subcommand(subparsers)
    args = parser.parse_args()
    return args


def run_pipeline():
    ingest.run()
    tag.run()
    analyze.run()


def main():
    args = parse_args()
    write_default_config(config_dir=args.config_dir)
    if args.list_configs:
        print("\n".join(get_config_file_names(config_dir=args.config_dir)))
        exit()
    override = "\n".join(args.config_override)
    config = load_config(
        config_dir=args.config_dir,
        additional_configs=args.additional_configs,
        override=override,
    )
    if config.general.print_config:
        pprint.pprint(config.model_dump())
    config.general.create_directories()
    args.run()
