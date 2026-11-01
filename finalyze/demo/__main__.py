"""Write the demo into a directory, e.g. for developing the web app:

uv run python -m finalyze.demo /tmp/finalyze-demo  # replaces it if it exists
uv run finalyze -C /tmp/finalyze-demo
"""

import argparse
import datetime
import shutil
from pathlib import Path

from finalyze.demo import build


def main():
    parser = argparse.ArgumentParser(
        prog="python -m finalyze.demo", description=__doc__.splitlines()[0]
    )
    parser.add_argument("dir", type=Path, help="Replaced if it exists")
    parser.add_argument(
        "--end-date",
        type=datetime.date.fromisoformat,
        help="The data's last day, as YYYY-MM-DD [default: today]",
    )
    args = parser.parse_args()
    if args.dir.exists():
        shutil.rmtree(args.dir)
    build(args.dir, end_date=args.end_date)
    print(f"Demo ready: finalyze -C {args.dir}")


if __name__ == "__main__":
    main()
