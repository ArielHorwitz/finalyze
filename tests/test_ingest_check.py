"""Tests of checking an ingest before running it: the dry run and the preview."""

import datetime
import hashlib
from pathlib import Path

import polars as pl
import pytest

from finalyze.config import Filters, IngestionRule, config, host
from finalyze.service import InvalidRequest, load_ingested, models
from finalyze.service import ingest as ingest_check
from finalyze.source.schema import RAW_SCHEMA
from tests.helpers import make_isolated_config
from tests.test_service import first_untagged, set_tags

HEADER = "source,date,amount,description\n"


@pytest.fixture(autouse=True)
def without_annotations(unannotated_demo):
    """These tests start from the demo's rules only, without its annotations."""


def use_ingestion(use_config, **changes):
    """Install the config with some of its ingestion settings changed."""
    ingestion = config().ingestion.model_copy(update=changes)
    use_config(config().model_copy(update={"ingestion": ingestion}))


@pytest.fixture
def cash_dir(demo_config) -> Path:
    """The cash account's folder of raw files."""
    return host().raw_dir.joinpath("cash")


def wallet_file() -> Path:
    """The cash wallet's hand-written raw file."""
    return host().raw_dir.joinpath("cash").joinpath("wallet.csv")


def wallet_raw() -> pl.DataFrame:
    return pl.read_csv(wallet_file(), infer_schema=False)


def files_state(*directories: Path) -> dict[Path, tuple[int, str]]:
    """Every file under the directories, with its modification time and hash."""
    return {
        path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
        for directory in directories
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def file_of(dry_run: models.IngestDryRun, name: str) -> models.IngestFile:
    return next(
        file
        for account in dry_run.accounts
        for file in account.files
        if file.name == name
    )


# Dry run


def test_dry_run_describes_each_file(service):
    dry_run = service.ingest_dry_run()
    assert dry_run.ingested_checked_at is not None
    assert dry_run.ingested_changed_at is not None
    # The raw folder's folders, by name
    assert [account.name for account in dry_run.accounts] == [
        "cash",
        "eitan",
        "loan",
        "maya",
        "savings",
    ]
    assert dry_run.raw_dir == str(host().raw_dir) and dry_run.raw_dir_exists
    assert dry_run.problems == []
    # Overlaps without shared rows, e.g. an opening balance in its own file
    assert all(overlap.shared_rows == 0 for overlap in dry_run.overlaps)
    cash = dry_run.accounts[0]
    assert cash.folder == str(host().raw_dir.joinpath("cash"))
    [file] = cash.files
    raw = wallet_raw().with_columns(
        pl.col("date").str.to_date(), pl.col("amount").cast(pl.Float64)
    )
    assert file.name == "wallet.csv" and file.parser == "csv"
    # The Bit parser comes first (parsers run in name order) and rejects it
    assert set(file.parser_errors) == {"bit"} and file.error is None
    assert file.rows == file.kept_rows == raw.height
    assert (file.start_date, file.end_date) == (raw["date"].min(), raw["date"].max())
    assert file.total == pytest.approx(raw["amount"].sum())
    assert not file.newer_than_ingest
    assert [source.source for source in file.sources] == ["wallet"]
    assert sum(source.rows for source in file.sources) == file.rows


def test_dry_run_flags_problems(service, cash_dir, use_config):
    # A copy of January 2025, as if written twice
    january = wallet_raw().filter(pl.col("date").str.starts_with("2025-01"))
    january.write_csv(cash_dir.joinpath("wallet-2025-01.csv"))
    # Other wallet rows in the same dates: an overlap without shared rows
    cash_dir.joinpath("kiosk.csv").write_text(
        HEADER + "wallet,2025-01-10,-1.50,Invented kiosk\n"
    )
    cash_dir.joinpath("notes.txt").write_text("not a bank export")
    cash_dir.joinpath("placeholder.csv").write_text("")
    cash_dir.joinpath("old.csv").write_text(HEADER + "wallet,2001-01-01,-1,Old\n")
    # Skipped: hidden files (e.g. an upload being written) and subfolders
    cash_dir.joinpath(".wallet.csv.partial").write_text("not a bank export")
    cash_dir.joinpath("older").mkdir()
    cash_dir.joinpath("older", "notes.txt").write_text("not a bank export")
    root = cash_dir.parent
    # Outside the accounts' folders
    root.joinpath("stray.csv").write_text(HEADER)
    root.joinpath(".hidden").mkdir()
    pot_rule = IngestionRule.model_validate(
        {
            "match": {"raw_description": "^Never$"},
            "action": "transfer",
            "to_account": "pot",
            "to_source": "pot",
        }
    )
    use_ingestion(
        use_config,
        rules=[*config().ingestion.rules, pot_rule],
        filters=Filters(start_date=datetime.date(2020, 1, 1)),
    )
    before = files_state(root)

    dry_run = service.ingest_dry_run()

    assert files_state(root) == before
    unparseable = file_of(dry_run, "notes.txt")
    assert unparseable.parser is None and unparseable.rows == 0
    assert unparseable.error == "No parser could read the file"
    assert set(unparseable.parser_errors) == {
        "bit",
        "csv",
        "leumi checking",
        "leumi card",
    }
    assert unparseable.newer_than_ingest
    assert file_of(dry_run, "old.csv").kept_rows == 0
    january_sources = dict(january.group_by("source").len().iter_rows())
    problems = sorted(
        (problem.kind, tuple(Path(path).name for path in problem.paths))
        for problem in dry_run.problems
    )
    assert problems == sorted(
        [
            ("empty", ("placeholder.csv",)),
            ("filtered_out", ("old.csv",)),
            ("missing_account", ("pot",)),
            ("stray_file", ("stray.csv",)),
            ("unparseable", ("notes.txt",)),
            ("overlap", ("wallet-2025-01.csv", "wallet.csv")),
        ]
    )
    shared = {
        (overlap.source, *(Path(path).name for path in overlap.files)): overlap
        for overlap in dry_run.overlaps
    }
    for source, count in january_sources.items():
        assert shared[(source, "wallet-2025-01.csv", "wallet.csv")].shared_rows == count
    kiosk = shared[("wallet", "kiosk.csv", "wallet.csv")]
    assert kiosk.shared_rows == 0
    assert kiosk.start_date == kiosk.end_date == datetime.date(2025, 1, 10)


def test_dry_run_applies_the_ingestion_rules(service, cash_dir, use_config):
    cash_dir.joinpath("bit.csv").write_text(
        HEADER
        + "card,2025-01-10,-30,BIT\n"
        + "card,2025-01-11,-20,Invented kiosk\n"
        + "card,2025-01-12,-10,BIT FEE\n"
    )
    cash_dir.joinpath("dropped.csv").write_text(HEADER + "wallet,2025-01-10,-1,Junk\n")
    rules = [
        IngestionRule.model_validate(rule)
        for rule in [
            {"match": {"raw_description": "^Junk$"}, "action": "drop"},
            {"match": {"raw_description": "^BIT FEE$"}, "action": "drop"},
            {
                "match": {"raw_description": "^BIT$"},
                "action": "transfer",
                "to_account": "eitan",
                "to_source": "bit",
                "raw_description": "Transfer card-bit",
            },
        ]
    ]
    use_ingestion(use_config, rules=rules)

    dry_run = service.ingest_dry_run()

    bit = file_of(dry_run, "bit.csv")
    assert (bit.rows, bit.kept_rows, bit.transfer_rows) == (3, 2, 1)
    # The kept rows only (the other leg is in eitan's bit source)
    [card] = bit.sources
    assert (card.source, card.rows, card.total) == ("card", 2, -50)
    dropped = file_of(dry_run, "dropped.csv")
    assert (dropped.rows, dropped.kept_rows, dropped.transfer_rows) == (1, 0, 0)
    assert [
        (problem.kind, [Path(path).name for path in problem.paths])
        for problem in dry_run.problems
    ] == [("filtered_out", ["dropped.csv"])]


def test_dry_run_of_an_account_without_files(service, cash_dir):
    # Accounts appear by making folders
    cash_dir.parent.joinpath("new").mkdir()
    dry_run = service.ingest_dry_run()
    assert "new" in [account.name for account in dry_run.accounts]
    assert [(problem.kind, problem.account) for problem in dry_run.problems] == [
        ("no_files", "new")
    ]


def test_dry_run_of_an_account_only_transfers_go_to(service, cash_dir, use_config):
    rule = {
        "match": {"account": "cash", "raw_description": "^Never$"},
        "action": "transfer",
        "to_account": "pot",
        "to_source": "pot",
    }
    use_config(make_isolated_config(cash_dir, ingestion={"rules": [rule]}))
    # Without a folder, the ingest fails
    assert [problem.kind for problem in service.ingest_dry_run().problems] == [
        "missing_account"
    ]
    with pytest.raises(InvalidRequest, match="'pot', which has no folder"):
        service.preview_ingest()
    # With an empty one, it is an account
    cash_dir.parent.joinpath("pot").mkdir()
    assert service.ingest_dry_run().problems == []
    service.preview_ingest()


# Preview


def test_preview_without_changes(service):
    preview = service.preview_ingest()
    assert preview.new == [] and preview.vanished == [] and preview.new_orphans == []
    assert preview.changed == []
    assert preview.unchanged == preview.transactions == load_ingested().height
    assert {name: counts.before for name, counts in preview.accounts.items()} == {
        name: counts.after for name, counts in preview.accounts.items()
    }


def test_preview_shows_the_effect_without_writing(service, cash_dir):
    target = first_untagged(service, "Haircut")
    set_tags(service, target.id, "health", "grooming", description="a short one")
    raw = wallet_raw()
    vanishing = raw.filter(pl.col("raw_description") == "Haircut (Eitan)")
    raw.filter(pl.col("raw_description") != "Haircut (Eitan)").write_csv(wallet_file())
    cash_dir.joinpath("new.csv").write_text(
        HEADER + "wallet,2025-06-14,-3.20,Invented bakery\n"
    )
    root = cash_dir.parent
    before = files_state(root)

    preview = service.preview_ingest()

    assert files_state(root) == before
    assert [(row.account, row.raw_description) for row in preview.new] == [
        ("cash", "Invented bakery")
    ]
    assert {row.raw_description for row in preview.vanished} == {"Haircut (Eitan)"}
    assert len(preview.vanished) == vanishing.height
    assert [orphan.transaction_id for orphan in preview.new_orphans] == [target.id]
    assert preview.new_orphans[0].description == "a short one"
    assert preview.unchanged == load_ingested().height - len(preview.vanished)
    assert preview.transactions == preview.unchanged + len(preview.new)
    cash = preview.accounts["cash"]
    assert cash.after == cash.before - len(preview.vanished) + 1

    # The ingest does what the preview said, and restoring the rows restores
    # the orphan
    summary = service.ingest()
    assert summary.new_orphan_ids == [target.id]
    assert set(summary.vanished_ids) == {row.id for row in preview.vanished}
    assert summary.new_ids == [row.id for row in preview.new]
    raw.write_csv(wallet_file())
    assert service.preview_ingest().restored_orphan_ids == [target.id]


def test_preview_of_a_failing_ingest(service, cash_dir):
    cash_dir.joinpath("notes.txt").write_text("not a bank export")
    with pytest.raises(InvalidRequest, match="Ingest would fail.*notes.txt"):
        service.preview_ingest()


# What an ingest changes, and when


def test_compare_finds_changed_content_under_the_same_id():
    before = load_frame(
        [("a", "2025-01-01", -1.0, "Kiosk"), ("b", "2025-01-02", -2.0, "Bakery")]
    )
    after = load_frame(
        [("a", "2025-01-01", -1.0, "Kiosk"), ("b", "2025-01-02", -2.5, "Bakery")]
    )
    changes = ingest_check.compare(before, after)
    assert changes.new.is_empty() and changes.vanished.is_empty()
    assert changes.changed["id"].to_list() == ["b"]
    assert changes.changed["amount"].to_list() == [-2.5]
    assert changes.any
    assert not ingest_check.compare(before, before.reverse()).any


def load_frame(rows: list[tuple[str, str, float, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            (row_id, "alice", "cash", datetime.date.fromisoformat(date), amount, text)
            for row_id, date, amount, text in rows
        ],
        schema=RAW_SCHEMA,
        orient="row",
    )


def test_ingest_without_changes_keeps_the_changed_time(service):
    before = service.status()

    summary = service.ingest()

    assert not summary.data_changed
    assert (summary.new_ids, summary.vanished_ids, summary.changed_ids) == ([], [], [])
    after = service.status()
    assert after.ingested_changed_at == before.ingested_changed_at
    assert after.ingested_checked_at > before.ingested_checked_at
    assert summary.ingested_changed_at == after.ingested_changed_at
    assert summary.ingested_checked_at == after.ingested_checked_at
    dry_run = service.ingest_dry_run()
    assert dry_run.ingested_changed_at == before.ingested_changed_at
    assert dry_run.ingested_checked_at == after.ingested_checked_at


def test_ingest_with_changes_moves_both_times(service, cash_dir):
    before = service.status()
    cash_dir.joinpath("new.csv").write_text(
        HEADER + "wallet,2025-06-14,-3.20,Invented bakery\n"
    )
    assert len(service.preview_ingest().new) == 1

    summary = service.ingest()

    assert summary.data_changed and len(summary.new_ids) == 1
    after = service.status()
    assert after.ingested_changed_at == after.ingested_checked_at
    assert after.ingested_changed_at > before.ingested_changed_at
    assert summary.ingested_changed_at == after.ingested_changed_at


def test_files_are_new_until_an_ingest_checks_them(service, cash_dir):
    cash_dir.joinpath("new.csv").write_text(
        HEADER + "wallet,2025-06-14,-3.20,Invented bakery\n"
    )
    assert file_of(service.ingest_dry_run(), "new.csv").newer_than_ingest
    service.ingest()
    assert not file_of(service.ingest_dry_run(), "new.csv").newer_than_ingest
    # Ingesting again changes nothing, but checks the files
    service.ingest()
    assert not file_of(service.ingest_dry_run(), "new.csv").newer_than_ingest


def test_ingested_data_without_times_changed_when_written(service):
    """Data ingested before the times were kept: both are when it was written."""
    host().ingest_times_file.unlink()
    written = max(file.stat().st_mtime for file in host().ingested_dir.glob("*.csv"))
    status = service.status()
    assert status.ingested_changed_at.timestamp() == pytest.approx(written)
    assert status.ingested_checked_at == status.ingested_changed_at

    service.ingest()

    after = service.status()
    assert after.ingested_changed_at == status.ingested_changed_at
    assert after.ingested_checked_at > status.ingested_checked_at
