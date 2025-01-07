import argparse
import pprint
from pathlib import Path

from finalyze import APP_DESCRIPTION, APP_NAME
from finalyze.analysis import analyze
from finalyze.config import CONFIG_FILE, load_config
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
        "--config-file",
        help=f"Location of config file [default: {CONFIG_FILE}]",
        default=str(CONFIG_FILE),
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
    args.config_file = Path(args.config_file).expanduser().resolve()
    return args


def run_pipeline(config):
    ingest.run(config)
    tag.run(config)
    analyze.run(config)


def main():
    args = parse_args()
    config = load_config(args.config_file)
    if config.general.print_config:
        pprint.pprint(config.model_dump())
    config.general.create_directories()
    args.run(config)
