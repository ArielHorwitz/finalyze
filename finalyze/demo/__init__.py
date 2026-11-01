"""A demo: an invented household's raw files, config and store, to show the app
off without any real data (`finalyze --demo`).

`build` writes everything into one directory: `config.toml`, the host file
(`host.toml`, pointing at the next two), the raw folder (`raw/`, a folder per
account) and the data directory (`data/`), with the store seeded, the raw files
ingested and the transactions annotated. See
`finalyze.demo.household` for the story.
"""

import contextlib
import datetime
from collections.abc import Iterator
from pathlib import Path
from typing import Optional

import polars as pl
import toml

from finalyze import config as config_module
from finalyze.demo import taxonomy
from finalyze.demo.exports import write_exports
from finalyze.demo.household import Entry, Household, Note, generate
from finalyze.money import to_minor_units
from finalyze.service import Record, models, open_service
from finalyze.source.identity import derive_ids
from finalyze.source.tag import snapshot_of
from finalyze.store import Annotation, Part, Store, TransactionSnapshot


def build(root: Path, *, end_date: Optional[datetime.date] = None) -> Household:
    """Write the demo into `root` (which must be empty or missing), with data up
    to `end_date` (default: today). The config loaded before is kept."""
    if root.exists() and any(root.iterdir()):
        raise FileExistsError(f"Not an empty directory: {root}")
    household = generate(end_date or datetime.date.today())
    write_exports(household, root.joinpath("raw"))
    config_data = {
        "ingestion": {"rules": taxonomy.ingestion_rules(household.bit_export_start)}
    }
    root.joinpath("config.toml").write_text(toml.dumps(config_data))
    host_data = {
        "data_dir": str(root.joinpath("data")),
        "raw_dir": str(root.joinpath("raw")),
    }
    config_module.host_file_of(root).write_text(toml.dumps(host_data))
    with loaded_config(root):
        config_module.host().create_directories()
        with open_service() as service:
            taxonomy.seed(service.store)
            service.set_dashboard_defaults(
                models.DashboardDefaults(**taxonomy.DASHBOARD_DEFAULTS)
            )
            service.ingest()
            annotate(service.store, service.view().records, household)
    return household


@contextlib.contextmanager
def loaded_config(config_dir: Path) -> Iterator[config_module.Config]:
    """The config of `config_dir` (and its host file), loaded until the context
    exits."""
    previous = (
        config_module.LOADED_CONFIG,
        config_module.LOADED_HOST,
        config_module.LOADED_CONFIG_DIR,
    )
    try:
        yield config_module.load_config(config_dir=config_dir, use_preloaded=False)
    finally:
        (
            config_module.LOADED_CONFIG,
            config_module.LOADED_HOST,
            config_module.LOADED_CONFIG_DIR,
        ) = previous


def entry_key(entry: Entry) -> tuple:
    return (
        entry.account,
        entry.source,
        entry.date,
        to_minor_units(entry.amount),
        entry.raw_description,
    )


def record_key(record: Record) -> tuple:
    snapshot = snapshot_of(record.row)
    return (
        snapshot.account,
        snapshot.source,
        snapshot.date,
        snapshot.amount,
        snapshot.raw_description,
    )


def annotate(store: Store, records: list[Record], household: Household):
    """Annotate the transactions of the household's notes, except those left
    for annotating, and add the orphaned and overridden annotations."""
    records_by_key: dict[tuple, list[Record]] = {}
    for record in records:
        records_by_key.setdefault(record_key(record), []).append(record)

    def take(entry: Entry) -> Record:
        matches = records_by_key.get(entry_key(entry))
        if not matches:
            raise ValueError(f"No ingested transaction for {entry}")
        return matches.pop(0)

    described_index, described_text = household.described_only
    annotations = []
    for index, entry in enumerate(household.entries):
        if entry.note is None or not entry.parsed:
            continue
        record = take(entry)
        snapshot = snapshot_of(record.row)
        if index == described_index:
            annotations.append(Annotation(record.id, snapshot, (), described_text))
        elif index not in household.unannotated:
            annotations.append(annotation_of(record.id, snapshot, entry.note))
    overridden_entry, overridden_note = household.overridden
    record = take(overridden_entry)
    annotations.append(
        annotation_of(record.id, snapshot_of(record.row), overridden_note)
    )
    orphan = household.orphan
    snapshot = TransactionSnapshot(
        orphan.account,
        orphan.source,
        orphan.date,
        to_minor_units(orphan.amount),
        orphan.raw_description,
    )
    annotations.append(annotation_of(orphan_id(orphan), snapshot, orphan.note))
    store.set_annotations(annotations)


def annotation_of(
    transaction_id: str, snapshot: TransactionSnapshot, note: Note
) -> Annotation:
    if note.split:
        parts = tuple(
            Part(to_minor_units(amount), tag, subtag, description)
            for amount, tag, subtag, description in note.split
        )
    else:
        parts = (Part(snapshot.amount, note.tag, note.subtag, kind=note.kind),)
    return Annotation(
        transaction_id,
        snapshot,
        parts,
        note.description,
        authors=(note.author,),
        needs_review=note.needs_review,
    )


def orphan_id(entry: Entry) -> str:
    """The ID the orphan's transaction had, when it was in the raw files."""
    row = pl.DataFrame(
        {
            "account": [entry.account],
            "source": [entry.source],
            "date": [entry.date],
            "raw_description": [entry.raw_description],
            "amount": [entry.amount],
        }
    )
    return derive_ids(row)["id"][0]
