"""Checking an ingest before running it: a dry run of the raw files, and a
preview of its effect on the transactions. Neither writes anything. Also when
ingests last changed the ingested data, and last ran.
"""

import dataclasses
import datetime
import itertools
import json
from pathlib import Path
from typing import Optional

import polars as pl

from finalyze.config import config, host
from finalyze.money import from_minor_units, minor_units_expr
from finalyze.service import models
from finalyze.source.identity import derive_ids, validate_unique_ids
from finalyze.source.ingest import account_files, account_folders, stray_files
from finalyze.source.ingestion_rules import apply_rules
from finalyze.source.parsing import try_parsers
from finalyze.source.schema import RAW_SCHEMA

DUPLICATE_FIELDS = ("date", "raw_description", "amount_minor")
"""Rows of two files with the same values of these are likely duplicates."""


def dry_run() -> models.IngestDryRun:
    """Parse every raw file on its own, and flag likely problems."""
    times = ingest_times()
    raw_dir = host().raw_dir
    folders = account_folders()
    accounts = []
    problems = []
    overlaps = []
    for account_name, folder in folders.items():
        account = _account_files(account_name, folder, times.checked_at)
        accounts.append(account.files_out)
        problems.extend(account.problems)
        account_overlaps = _overlaps(account_name, account.kept_rows)
        overlaps.extend(account_overlaps)
        problems.extend(_overlap_problems(account_name, account_overlaps))
    problems.extend(
        models.IngestProblem(
            kind="missing_account",
            account=rule.to_account,
            paths=[str(raw_dir.joinpath(rule.to_account))],
            message="An ingestion rule transfers to this account, which has no "
            "folder (make it: it may stay empty)",
        )
        for rule in config().ingestion.rules
        if rule.to_account is not None and rule.to_account not in folders
    )
    problems.extend(
        models.IngestProblem(
            kind="stray_file",
            account=None,
            paths=[str(file)],
            message="In the raw folder itself, not in an account's folder: not "
            "ingested",
        )
        for file in stray_files()
    )
    return models.IngestDryRun(
        raw_dir=str(raw_dir),
        raw_dir_exists=raw_dir.is_dir(),
        ingested_changed_at=times.changed_at,
        ingested_checked_at=times.checked_at,
        accounts=accounts,
        overlaps=overlaps,
        problems=problems,
    )


def _transferred_to(account_name: str) -> bool:
    """Whether transfer rules may add rows to the account (of other accounts)."""
    return any(
        rule.action == "transfer" and rule.to_account == account_name
        for rule in config().ingestion.rules
    )


@dataclasses.dataclass(frozen=True)
class _AccountFiles:
    files_out: models.IngestAccountFiles
    problems: list[models.IngestProblem]
    kept_rows: dict[Path, pl.DataFrame]
    """Of the parsed files."""


def _account_files(
    account_name: str,
    folder: Path,
    last_checked_at: Optional[datetime.datetime],
) -> _AccountFiles:
    problems = []
    files = account_files(folder)
    if not files and not _transferred_to(account_name):
        problems.append(
            models.IngestProblem(
                kind="no_files",
                account=account_name,
                paths=[],
                message="The account has no files",
            )
        )
    files_out = []
    kept_rows = {}
    for file in files:
        file_out, kept = file_dry_run(account_name, file, last_checked_at)
        files_out.append(file_out)
        if kept is not None:
            kept_rows[file] = kept
        problem = _file_problem(file_out)
        if problem is not None:
            kind, message = problem
            problems.append(
                models.IngestProblem(
                    kind=kind, account=account_name, paths=[str(file)], message=message
                )
            )
    return _AccountFiles(
        models.IngestAccountFiles(
            name=account_name, folder=str(folder), files=files_out
        ),
        problems,
        kept_rows,
    )


def file_dry_run(
    account_name: str, file: Path, last_checked_at: Optional[datetime.datetime]
) -> tuple[models.IngestFile, Optional[pl.DataFrame]]:
    """The file's dry run, and its rows that the ingestion rules and filters
    keep, as parsed (overlaps compare rows as exported, before rules rewrite
    them).

    Rules match each row on its own, so applying them to one file is exact."""
    stat = file.stat()
    modified_at = datetime.datetime.fromtimestamp(stat.st_mtime).astimezone()
    error = None
    try:
        attempt = try_parsers(file)
    except Exception as exception:  # A parser failed unexpectedly
        attempt = None
        error = f"{type(exception).__name__}: {exception}"
    data = attempt.data if attempt is not None else None
    if attempt is not None and data is None:
        error = "No parser could read the file"
    kept = None
    transfer_rows = 0
    if data is not None:
        data = derive_ids(data.with_columns(account=pl.lit(account_name)))
        data = data.with_columns(amount_minor=minor_units_expr("amount"))
        reshaped = config().ingestion.filters.apply(
            apply_rules(data, config().ingestion.rules)
        )
        is_own = pl.col("id").is_in(data["id"])
        kept = reshaped.filter(is_own)
        transfer_rows = reshaped.filter(~is_own).height
    file_out = models.IngestFile(
        path=str(file),
        name=file.name,
        size=stat.st_size,
        modified_at=modified_at,
        newer_than_ingest=last_checked_at is None or modified_at > last_checked_at,
        parser=attempt.parser if attempt is not None else None,
        parser_errors={
            name: str(exception)
            for name, exception in (
                attempt.errors if attempt is not None else {}
            ).items()
        },
        error=error,
        rows=data.height if data is not None else 0,
        kept_rows=kept.height if kept is not None else 0,
        transfer_rows=transfer_rows,
        start_date=data["date"].min() if data is not None else None,
        end_date=data["date"].max() if data is not None else None,
        total=from_minor_units(data["amount_minor"].sum()) if data is not None else 0,
        sources=_sources(kept) if kept is not None else [],
    )
    kept_parsed = data.join(kept, on="id", how="semi") if kept is not None else None
    return file_out, kept_parsed


def _sources(kept: pl.DataFrame) -> list[models.IngestFileSource]:
    summary = (
        kept.group_by("source")
        .agg(
            rows=pl.len(),
            start_date=pl.col("date").min(),
            end_date=pl.col("date").max(),
            total=pl.col("amount_minor").sum(),
        )
        .sort("source")
    )
    return [
        models.IngestFileSource(
            source=row["source"],
            rows=row["rows"],
            start_date=row["start_date"],
            end_date=row["end_date"],
            total=from_minor_units(row["total"]),
        )
        for row in summary.iter_rows(named=True)
    ]


def _file_problem(file_out: models.IngestFile) -> Optional[tuple[str, str]]:
    if file_out.error is not None:
        return "unparseable", file_out.error
    if file_out.rows == 0:
        return "empty", "The file has no rows"
    if file_out.kept_rows == 0:
        return (
            "filtered_out",
            "The ingestion rules and filters drop all the file's rows",
        )
    return None


def _overlaps(
    account_name: str, kept_rows: dict[Path, pl.DataFrame]
) -> list[models.IngestOverlap]:
    """Date overlaps between the files' rows of each source, with shared rows."""
    by_source = {}
    for file, rows in kept_rows.items():
        for (source,), source_rows in rows.partition_by(
            "source", as_dict=True, maintain_order=True
        ).items():
            by_source.setdefault(source, []).append((file, source_rows))
    overlaps = []
    for source, files in sorted(by_source.items()):
        for (file, rows), (other_file, other_rows) in itertools.combinations(files, 2):
            start_date = max(rows["date"].min(), other_rows["date"].min())
            end_date = min(rows["date"].max(), other_rows["date"].max())
            if start_date > end_date:
                continue
            overlaps.append(
                models.IngestOverlap(
                    account=account_name,
                    source=source,
                    files=(str(file), str(other_file)),
                    start_date=start_date,
                    end_date=end_date,
                    shared_rows=_shared_rows(rows, other_rows),
                )
            )
    return overlaps


def _overlap_problems(
    account_name: str, overlaps: list[models.IngestOverlap]
) -> list[models.IngestProblem]:
    """One problem per pair of files sharing rows, of any source."""
    by_files = {}
    for overlap in overlaps:
        if overlap.shared_rows:
            by_files.setdefault(overlap.files, []).append(overlap)
    return [
        models.IngestProblem(
            kind="overlap",
            account=account_name,
            paths=list(files),
            message=f"{sum(overlap.shared_rows for overlap in shared)} rows from "
            f"{min(overlap.start_date for overlap in shared)} to "
            f"{max(overlap.end_date for overlap in shared)} are in both files ("
            + ", ".join(f"{overlap.source} {overlap.shared_rows}" for overlap in shared)
            + ")",
        )
        for files, shared in by_files.items()
    ]


def _shared_rows(rows: pl.DataFrame, other_rows: pl.DataFrame) -> int:
    """How many rows the two have in common, counting repeated rows."""
    counts = rows.group_by(DUPLICATE_FIELDS).len()
    other_counts = other_rows.group_by(DUPLICATE_FIELDS).len()
    shared = counts.join(other_counts, on=DUPLICATE_FIELDS, nulls_equal=True)
    return int(shared.select(pl.min_horizontal("len", "len_right")).sum().item() or 0)


def after_ingest(parsed: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """The ingested data after writing the parsed accounts, as an ingest would
    (it replaces all the ingested data)."""
    after = pl.concat(parsed.values()).sort("date", "amount")
    validate_unique_ids(after)
    return after


@dataclasses.dataclass(frozen=True)
class DataChanges:
    """How ingested data differs from other ingested data, by transaction ID."""

    new: pl.DataFrame
    """Rows whose ID is only in the new data."""
    vanished: pl.DataFrame
    """Rows whose ID is only in the old data."""
    changed: pl.DataFrame
    """Rows of the new data whose ID is in both, but with other content (e.g. a
    parser's native ID, kept while a raw description changes)."""

    @property
    def any(self) -> bool:
        return bool(self.new.height or self.vanished.height or self.changed.height)


def compare(before: pl.DataFrame, after: pl.DataFrame) -> DataChanges:
    """What changes from the ingested data `before` to `after`."""
    fields = list(RAW_SCHEMA)
    before = before.select(fields)
    after = after.select(fields)
    kept = after.join(before, on="id", how="semi", maintain_order="left")
    return DataChanges(
        new=after.join(before, on="id", how="anti", maintain_order="left"),
        vanished=before.join(after, on="id", how="anti", maintain_order="left"),
        changed=kept.join(
            before, on=fields, how="anti", nulls_equal=True, maintain_order="left"
        ),
    )


@dataclasses.dataclass(frozen=True)
class IngestTimes:
    changed_at: Optional[datetime.datetime]
    """When an ingest last changed the ingested data."""
    checked_at: Optional[datetime.datetime]
    """When an ingest last ran, whether it changed anything or not."""


def ingest_times() -> IngestTimes:
    """When the ingested data last changed and was last checked, if ever.

    Ingested data from before these were kept has neither: both are when its
    files were written."""
    times_file = host().ingest_times_file
    if times_file.is_file():
        stored = json.loads(times_file.read_text())
        return IngestTimes(
            changed_at=_parse_time(stored["changed_at"]),
            checked_at=_parse_time(stored["checked_at"]),
        )
    files = list(host().ingested_dir.glob("*.csv"))
    if not files:
        return IngestTimes(changed_at=None, checked_at=None)
    written_at = datetime.datetime.fromtimestamp(
        max(file.stat().st_mtime for file in files)
    ).astimezone()
    return IngestTimes(changed_at=written_at, checked_at=written_at)


def record_ingest(before: IngestTimes, changed: bool) -> IngestTimes:
    """Record an ingest that just ran, which changed the ingested data or not.
    `before`: the times before it ran (read before it wrote any files)."""
    now = datetime.datetime.now().astimezone()
    times = IngestTimes(
        changed_at=now if changed else before.changed_at, checked_at=now
    )
    times_file = host().ingest_times_file
    partial_file = times_file.with_suffix(".json.partial")
    partial_file.write_text(
        json.dumps(
            {
                "changed_at": _format_time(times.changed_at),
                "checked_at": _format_time(times.checked_at),
            }
        )
    )
    partial_file.replace(times_file)
    return times


def _parse_time(value: Optional[str]) -> Optional[datetime.datetime]:
    return None if value is None else datetime.datetime.fromisoformat(value)


def _format_time(value: Optional[datetime.datetime]) -> Optional[str]:
    return None if value is None else value.isoformat()
