"""Diffable snapshot files for characterization tests.

Set the environment variable FINALYZE_UPDATE_SNAPSHOTS=1 to (re)generate
snapshots instead of comparing against them.
"""

import difflib
import json
import os
import shutil
from pathlib import Path

import polars as pl

SNAPSHOTS_DIR = Path(__file__).parent.joinpath("snapshots")
UPDATE_ENV_VAR = "FINALYZE_UPDATE_SNAPSHOTS"
FLOAT_PRECISION = 6
MAX_DIFF_LINES = 80


def is_updating() -> bool:
    return os.environ.get(UPDATE_ENV_VAR) == "1"


def frame_to_csv(frame: pl.DataFrame) -> str:
    """Rows are sorted by all columns, so row order is not part of the snapshot."""
    if frame.width > 0:
        frame = frame.sort(frame.columns, nulls_last=True)
    return frame.write_csv(float_precision=FLOAT_PRECISION)


def frame_schema(frame: pl.DataFrame) -> dict[str, str]:
    return {name: str(dtype) for name, dtype in frame.schema.items()}


def to_json(data) -> str:
    def default(value):
        if isinstance(value, pl.Series):
            return value.to_list()
        raise TypeError(f"Cannot serialize {type(value)} in snapshot: {value!r}")

    return json.dumps(data, indent=2, ensure_ascii=False, default=default) + "\n"


class SnapshotDirectory:
    """Compares (or writes) a directory of snapshot files.

    Call `finish` once all files are checked, to fail on any mismatching,
    missing, or unexpected files.
    """

    def __init__(self, relative_dir: str):
        self.root = SNAPSHOTS_DIR.joinpath(relative_dir)
        self.checked: set[Path] = set()
        self.mismatched: list[tuple[Path, str]] = []
        self.missing: list[Path] = []
        self.frame_paths_by_content: dict[str, str] = {}
        if is_updating() and self.root.exists():
            shutil.rmtree(self.root)

    def check_text(self, relative_path: str, text: str):
        path = self.root.joinpath(relative_path)
        if path in self.checked:
            raise ValueError(f"Duplicate snapshot path: {path}")
        self.checked.add(path)
        if is_updating():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
            return
        if not path.is_file():
            self.missing.append(path)
            return
        expected = path.read_text()
        if expected != text:
            diff = difflib.unified_diff(
                expected.splitlines(keepends=True),
                text.splitlines(keepends=True),
                fromfile=f"{path} (snapshot)",
                tofile=f"{path} (actual)",
            )
            self.mismatched.append((path, "".join(diff)))

    def check_frame(self, relative_stem: str, frame: pl.DataFrame) -> dict:
        """Snapshot the frame's data as CSV, and return its metadata.

        Data identical to a previously checked frame is not written again, the
        metadata refers to the existing file instead.
        """
        data_path = f"{relative_stem}.csv"
        content = frame_to_csv(frame)
        if content in self.frame_paths_by_content:
            data_path = self.frame_paths_by_content[content]
        else:
            self.frame_paths_by_content[content] = data_path
            self.check_text(data_path, content)
        return {
            "data": data_path,
            "height": frame.height,
            "schema": frame_schema(frame),
        }

    def check_json(self, relative_stem: str, data):
        self.check_text(f"{relative_stem}.json", to_json(data))

    def finish(self):
        if is_updating():
            return
        unexpected = sorted(
            path
            for path in self.root.rglob("*")
            if path.is_file() and path not in self.checked
        )
        problems = []
        if self.missing:
            problems.append("Missing snapshots:")
            problems.extend(f"  {path}" for path in self.missing)
        if unexpected:
            problems.append("Unexpected snapshots (no longer produced):")
            problems.extend(f"  {path}" for path in unexpected)
        if self.mismatched:
            problems.append("Mismatched snapshots:")
            problems.extend(f"  {path}" for path, _diff in self.mismatched)
            first_path, first_diff = self.mismatched[0]
            diff_lines = first_diff.splitlines()
            if len(diff_lines) > MAX_DIFF_LINES:
                omitted = len(diff_lines) - MAX_DIFF_LINES
                diff_lines = diff_lines[:MAX_DIFF_LINES] + [f"... ({omitted} more)"]
            problems.append(f"Diff of {first_path}:")
            problems.extend(diff_lines)
        if problems:
            problems.append(
                f"If the changes are intended, rerun with {UPDATE_ENV_VAR}=1 "
                "and review the snapshot diff."
            )
            raise AssertionError("\n".join(problems))
