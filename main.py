import argparse
from pathlib import Path

import analyze
import bank_leumi
import tag


def main():
    parser = argparse.ArgumentParser(description="Finnancial analysis and projection")
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
    args = parser.parse_args()

    verbose = args.verbose
    if verbose:
        print(f"{args=}")
    import_dir = Path(args.import_dir)
    balance_pattern = args.balance_pattern
    credit_pattern = args.credit_pattern
    output_file = Path(args.output_file)

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

    historical_data = bank_leumi.parse_sources(
        balance_files=balance_files,
        credit_files=credit_files,
        verbose=verbose,
    )
    historical_data = tag.tag_transactions(historical_data, tags_file, True)
    print(historical_data)
    print(historical_data.describe())
    return

    analyze.analyze(historical_data)


if __name__ == "__main__":
    main()
