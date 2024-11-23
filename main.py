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
    import_dir = Path(args.import_dir)
    balance_pattern = args.balance_pattern
    credit_pattern = args.credit_pattern
    output_file = Path(args.output_file)
    clear_tags = args.clear_tags
    auto_cache_tags = args.auto_cache_tags
    flip_rtl = args.flip_rtl
    month = args.month
    year = args.year
    analyze_full = args.full

    # Derive from argument data
    tags_file = import_dir / "tags.csv"
    balance_files = tuple(import_dir.glob(balance_pattern))
    credit_files = tuple(import_dir.glob(credit_pattern))
    if len(balance_files) + len(credit_files) == 0:
        raise FileNotFoundError(
            f"Did not find any source files in {import_dir}"
            f" (using pattern for account balance files: '{balance_pattern}'"
            f" and pattern for credit card files: '{credit_pattern}')"
        )
    if verbose:
        print(f"{balance_files=}")
        print(f"{credit_files=}")
        print(f"{tags_file=}")
    if clear_tags:
        tags_file.unlink()

    # Main logic
    historical_data = bank_leumi.parse_sources(
        balance_files=balance_files,
        credit_files=credit_files,
        verbose=verbose,
    )
    if flip_rtl:
        historical_data = utils.flip_rtl_column(historical_data, "description")
    historical_data = tag.tag_transactions(historical_data, tags_file, auto_cache_tags)
    if analyze_full:
        filtered_month = None
    else:
        filtered_month = arrow.Arrow(year, month, 1)
    analyze.analyze(historical_data, month=filtered_month)


def parse_args():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "--output-file",
        default="data/history.csv",
        help="Output file",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Be verbose (can be used multiple times)",
        action="count",
        default=0,
    )
    # Analysis
    parser.add_argument(
        "--full",
        action="store_true",
        help="Analyze full historical data (no date filters)",
    )
    parser.add_argument(
        "-M",
        "--month",
        type=int,
        default=arrow.now().shift(months=-1).month,
        help="Month to analyze",
    )
    parser.add_argument(
        "-Y",
        "--year",
        type=int,
        default=arrow.now().shift(months=-1).year,
        help="Year to analyze",
    )
    # Source data importing
    parser.add_argument(
        "--import-dir",
        default="data",
        help="Directory containing source data files",
    )
    parser.add_argument(
        "--balance-pattern",
        default="*balance*.xls",
        help="File name pattern for account balance files exported from Bank Leumi",
    )
    parser.add_argument(
        "--credit-pattern",
        default="*credit*.xls",
        help="File name pattern for credit card files exported from Bank Leumi",
    )
    # Source data tagging
    parser.add_argument(
        "--auto-cache-tags",
        action="store_true",
        help="Used cached values for tags",
    )
    parser.add_argument(
        "--clear-tags",
        action="store_true",
        help="Clear saved tags",
    )
    parser.add_argument(
        "--flip-rtl",
        action="store_true",
        help="Flip non-English (RTL) text",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
