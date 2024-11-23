import argparse
from pathlib import Path

import arrow

import analyze
import bank_leumi
import tag
import utils

DESCRIPTION = "Personal financial analysis tool"


def main():
    args = parse_args()
    verbose = args.verbose
    if verbose:
        print(f"{args=}")
    # Extract argument data
    # Analysis
    month = args.month
    year = args.year
    analyze_full = args.full
    # Source data
    account_files = tuple(Path(f).resolve() for f in args.account_files)
    credit_files = tuple(Path(f).resolve() for f in args.credit_files)
    flip_rtl = args.flip_rtl
    # Tagging
    tags_file = Path(args.tags_file).resolve()
    tag_missing = args.tag_missing
    clear_tags = args.clear_tags
    auto_cache_tags = args.auto_cache_tags

    if verbose:
        print("Account files:")
        for f in account_files:
            print(f"  {f}")
        print("Credit files:")
        for f in credit_files:
            print(f"  {f}")
        print(f"{tags_file=}")
    if clear_tags and tags_file.is_file():
        tags_file.replace(f"{tags_file}.bak")

    # Main logic
    if len(account_files) + len(credit_files) == 0:
        raise FileNotFoundError("No source files provided")
    historical_data = bank_leumi.parse_sources(
        balance_files=account_files,
        credit_files=credit_files,
        verbose=verbose,
    )
    if flip_rtl:
        historical_data = utils.flip_rtl_column(historical_data, "description")
    historical_data = tag.tag_transactions(
        historical_data,
        tags_file=tags_file,
        auto_cache=auto_cache_tags,
        tag_missing=tag_missing,
    )
    if analyze_full:
        filtered_month = None
    else:
        filtered_month = arrow.Arrow(year, month, 1)
    analyze.analyze(historical_data, month=filtered_month)


def parse_args():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "-v",
        "--verbose",
        help="Be verbose (can be used multiple times)",
        action="count",
        default=0,
    )
    # Analysis
    analysis_group = parser.add_argument_group("ANALYSIS")
    analysis_group.add_argument(
        "--full",
        action="store_true",
        help="Analyze full historical data (no date filters)",
    )
    analysis_group.add_argument(
        "-M",
        "--month",
        type=int,
        default=arrow.now().shift(months=-1).month,
        help="Month to analyze",
    )
    analysis_group.add_argument(
        "-Y",
        "--year",
        type=int,
        default=arrow.now().shift(months=-1).year,
        help="Year to analyze",
    )
    # Source data importing
    source_group = parser.add_argument_group("SOURCES")
    source_group.add_argument(
        "--account-files",
        nargs="*",
        help="Account balance .xls files exported from Bank Leumi",
    )
    source_group.add_argument(
        "--credit-files",
        nargs="*",
        help="Credit card .xls files exported from Bank Leumi",
    )
    source_group.add_argument(
        "--flip-rtl",
        action="store_true",
        help="Flip non-English (RTL) text",
    )
    # Source data tagging
    tag_group = parser.add_argument_group("TAGGING")
    tag_group.add_argument(
        "--tags-file",
        required=True,
        help="Tag data file",
    )
    tag_group.add_argument(
        "--tag-missing",
        action="store_true",
        help="Prompt for missing tags",
    )
    tag_group.add_argument(
        "--auto-cache-tags",
        action="store_true",
        help="Used cached values for tags",
    )
    tag_group.add_argument(
        "--clear-tags",
        action="store_true",
        help="Clear saved tags",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
