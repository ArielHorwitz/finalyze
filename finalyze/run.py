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
    parser.set_defaults(run=run)
    parser.add_argument(
        "--config-file",
        help="Location of config file",
        default=str(CONFIG_FILE),
    )
    # Subcommands
    subparsers = parser.add_subparsers(dest="subcommand")
    subparsers.add_parser("ingest", help="Ingest source data").set_defaults(
        run=ingest.run
    )
    subparsers.add_parser("tag", help="Tag source data").set_defaults(run=tag.run)
    subparsers.add_parser("analyze", help="Analyze historical data").set_defaults(
        run=analyze.run
    )
    add_developer_subcommand(subparsers)
    args = parser.parse_args()
    args.config_file = Path(args.config_file).expanduser().resolve()
    return args


def run(config):
    pprint.pprint(config.model_dump())


def main():
    args = parse_args()
    config = load_config(args.config_file)
    config.general.create_directories()
    args.run(config)
