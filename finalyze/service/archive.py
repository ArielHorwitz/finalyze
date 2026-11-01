"""Export and import of the whole instance, as one zip: for safekeeping, and to
move to another machine. Exports are downloaded and imports uploaded (through
the API): the server only reads and writes its own folders, those of the host
file (see `finalyze.config.Host`).

An export holds:
- `manifest.json`: when, by which version of the app, the store's schema
  version, the accounts, and a backup's name
- `finalyze.db`: a consistent snapshot of the store (SQLite's backup API, safe
  while serving)
- `ingested/`: the ingested data as it is, so an import serves the same
  transactions without parsing anything
- `raw/<account>/`: the raw folder as ingesting reads it (each account's
  files)
- `config.toml`: the config file (the preferences, which hold no paths)

Never the host file: it is the machine's. An import checks everything first and
writes nothing if the zip is invalid. It then replaces the raw folder's contents
with the export's raw files (so that it holds the export's accounts, and only
them), and the config, the ingested data and the store. Backups (see `backups`)
are exports kept on this machine, and restoring one is importing it.
"""

import contextlib
import dataclasses
import datetime
import importlib.metadata
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
import zipfile
from collections.abc import Callable, Iterator
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Optional

import polars as pl
import pydantic
import toml

from finalyze import APP_NAME
from finalyze import config as config_module
from finalyze.config import host
from finalyze.service import models
from finalyze.source.identity import DuplicateIds, validate_unique_ids
from finalyze.source.ingest import account_files, account_folders, is_hidden
from finalyze.source.schema import RAW_SCHEMA
from finalyze.store import BASELINE_VERSION, SCHEMA_VERSION, Store, note_data_changed

FORMAT = 3
"""The version of the export's layout, in its manifest."""
READABLE_FORMATS = (1, 2, 3)
"""Format 1 had the accounts' directories in its config (`raw/<account>`), and
format 3 added the manifest's `name` (of backups)."""
MANIFEST_NAME = "manifest.json"
STORE_NAME = "finalyze.db"
CONFIG_NAME = "config.toml"
INGESTED_DIR = "ingested"
RAW_DIR = "raw"
EXPORT_PREFIX = f"{APP_NAME}-export-"
TIMESTAMP_FORMAT = "%Y-%m-%d_%H-%M-%S"
MAX_UPLOAD_BYTES = 1024**3
"""The largest export an import takes (1 GiB)."""
MAX_UNPACKED_BYTES = 4 * 1024**3
"""The most an export may hold once unpacked (4 GiB), against zip bombs."""


MAX_NAME_BYTES = 200
"""The longest file or folder name taken (file systems take 255 bytes)."""


class ArchiveError(Exception):
    """An export or import that can't be done: nothing was written."""


TEMPORARY_PREFIXES = (".download-", ".export-", ".import-", ".upload-", ".tmp")
"""Of the temporary files and folders in the data directory (see
`remove_leftovers`)."""


def remove_leftovers():
    """Remove the temporary files and folders a server that stopped abruptly left
    in the data directory (downloads, imports and uploads under way). For when
    the server starts, before any request."""
    data_dir = host().data_dir
    if not data_dir.is_dir():
        return
    for path in data_dir.iterdir():
        if not path.name.startswith(TEMPORARY_PREFIXES):
            continue
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)


def export_name() -> str:
    """A new export's file name: when, to the second."""
    timestamp = datetime.datetime.now().strftime(TIMESTAMP_FORMAT)
    return f"{EXPORT_PREFIX}{timestamp}.zip"


# Export


def export(
    store: Store,
    target: Path,
    *,
    ingest_lock: Optional[contextlib.AbstractContextManager] = None,
    name: Optional[str] = None,
) -> models.ExportManifest:
    """Write the instance into a new zip, `target` (in one of the host's
    folders), named `name` if it is a backup. `ingest_lock` is held while the
    store and the ingested data are copied, so that an ingest can't change them
    in between."""
    if target.exists():
        raise ArchiveError(f"Already exists: {target}")
    accounts = account_folders()
    for account in accounts:
        _check_account_name(account)
    raw_files = {account: account_files(folder) for account, folder in accounts.items()}
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(f".{target.name}.partial")
    with tempfile.TemporaryDirectory(dir=target.parent, prefix=".export-") as temp_dir:
        snapshot = Path(temp_dir).joinpath(STORE_NAME)
        ingested_copy = Path(temp_dir).joinpath(INGESTED_DIR)
        ingested_copy.mkdir()
        with ingest_lock or contextlib.nullcontext():
            schema_version = _snapshot_store(store, snapshot)
            ingested_dir = host().ingested_dir
            ingested_files = ingested_dir.iterdir() if ingested_dir.is_dir() else ()
            for file in ingested_files:
                if file.is_file():
                    shutil.copy2(file, ingested_copy)
        manifest = models.ExportManifest(
            format=FORMAT,
            created_at=datetime.datetime.now().astimezone(),
            app_version=importlib.metadata.version(APP_NAME),
            commit=_commit(),
            schema_version=schema_version,
            accounts=list(accounts),
            name=name,
        )
        try:
            with zipfile.ZipFile(partial, "w", zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(MANIFEST_NAME, manifest.model_dump_json(indent=2))
                archive.writestr(CONFIG_NAME, _config_text())
                archive.write(snapshot, STORE_NAME)
                for file in sorted(ingested_copy.iterdir()):
                    archive.write(file, f"{INGESTED_DIR}/{file.name}")
                for account, files in raw_files.items():
                    # The folder too, which may be empty (e.g. of an account
                    # only transfers add rows to)
                    archive.writestr(f"{RAW_DIR}/{account}/", b"")
                    for file in files:
                        archive.write(file, f"{RAW_DIR}/{account}/{file.name}")
            partial.replace(target)
        finally:
            partial.unlink(missing_ok=True)
    return manifest


def export_to_download(
    store: Store, *, ingest_lock: Optional[contextlib.AbstractContextManager] = None
) -> Path:
    """Export into a hidden temporary file of the data directory, for a
    download: whoever sends it deletes it."""
    data_dir = host().data_dir
    data_dir.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(dir=data_dir, prefix=".download-", suffix=".zip")
    os.close(handle)
    target = Path(name)
    target.unlink()
    try:
        export(store, target, ingest_lock=ingest_lock)
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    return target


def _config_text() -> str:
    """The config file as it is (with its comments), or the loaded config if it
    has no file."""
    config_file = config_module.config_file_of(config_module.LOADED_CONFIG_DIR)
    if config_file.is_file():
        return config_file.read_text()
    return toml.dumps(
        config_module.config().model_dump(
            mode="json", exclude_defaults=True, exclude_none=True
        )
    )


def _snapshot_store(store: Store, target: Path) -> int:
    """Copy the store into `target`, consistently even while it is written, as
    one self-contained file. Returns its schema version."""
    snapshot = sqlite3.connect(target)
    try:
        store.connection.backup(snapshot)
        snapshot.execute("PRAGMA journal_mode = DELETE")
        return snapshot.execute("PRAGMA user_version").fetchone()[0]
    finally:
        snapshot.close()


def _commit() -> Optional[str]:
    """The git commit of the app, when it runs from a checkout."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


# Import


@dataclasses.dataclass(frozen=True)
class _Checked:
    """A valid export, and what importing it would write."""

    manifest: models.ExportManifest
    config_text: str
    """The new config file."""
    raw_files: list[str]
    """The export's raw files, as `<account>/<name>` (also their names in it,
    under `raw/`)."""


def check_import(zip_file: BinaryIO) -> models.ImportResult:
    """What importing the zip would do, after checking it. Writes nothing (but
    a temporary folder in the data directory)."""
    with _open_zip(zip_file) as archive:
        checked = _check(archive)
        with _temporary_folder() as temp:
            _stage(archive, checked, temp)
    return _import_result(checked, dry_run=True, backup=None)


def import_(
    store: Store,
    zip_file: BinaryIO,
    *,
    back_up: Optional[Callable[[], models.Backup]] = None,
) -> models.ImportResult:
    """Replace the instance with an export's (see the module's docstring), after
    calling `back_up` (if given) once the export is checked. Everything is
    checked first, and if anything fails, what was already changed is put
    back."""
    with _open_zip(zip_file) as archive:
        checked = _check(archive)
        result = _import_result(checked, dry_run=False, backup=None)
        with _temporary_folder() as temp:
            staged = _stage(archive, checked, temp)
            live_page_size = store.connection.execute("PRAGMA page_size").fetchone()[0]
            _set_page_size(staged.joinpath(STORE_NAME), live_page_size)
            backup = back_up() if back_up else None
            with undone_on_failure() as undo:
                replaced_raw = _replace_raw_folder(staged.joinpath(RAW_DIR), undo)
                _replace_ingested(staged.joinpath(INGESTED_DIR), temp, undo)
                _write_config(checked.config_text, undo)
                _replace_store(store, staged.joinpath(STORE_NAME))
            shutil.rmtree(replaced_raw, ignore_errors=True)
    return result.model_copy(update={"backup": backup})


def _import_result(
    checked: _Checked, *, dry_run: bool, backup: Optional[models.Backup]
) -> models.ImportResult:
    """What the import does to the raw folder: of its files now, those the
    export has too are replaced, and the others removed."""
    current = raw_folder_files()
    new = set(checked.raw_files)
    removed = sorted(name for name in current if name not in new)
    return models.ImportResult(
        dry_run=dry_run,
        manifest=checked.manifest,
        raw_dir=str(host().raw_dir),
        raw_files=len(checked.raw_files),
        replaced_raw_files=sorted(name for name in current if name in new),
        removed_raw_files=removed,
        unbacked_raw_files=[name for name in removed if not current[name]],
        backup=backup,
    )


def raw_folder_files() -> dict[str, bool]:
    """Every file in the raw folder (but in its hidden folders), as
    `<account>/<name>` (deeper in subfolders, or only `<name>` outside account
    folders): whether an export holds it (an account's raw file), or it is one
    of the files ingesting ignores (hidden ones, those in subfolders, and those
    outside account folders)."""
    raw_dir = host().raw_dir
    if not raw_dir.is_dir():
        return {}
    files = {}
    for entry in sorted(raw_dir.iterdir()):
        if is_hidden(entry):
            continue
        if not entry.is_dir():
            files[entry.name] = False
            continue
        exported = set(account_files(entry))
        for file in sorted(entry.rglob("*")):
            if file.is_file() or file.is_symlink():
                files[file.relative_to(raw_dir).as_posix()] = file in exported
    return files


@contextlib.contextmanager
def _open_zip(zip_file: BinaryIO) -> Iterator[zipfile.ZipFile]:
    if not zipfile.is_zipfile(zip_file):
        raise ArchiveError("Not a zip file")
    zip_file.seek(0)
    with zipfile.ZipFile(zip_file) as archive:
        yield archive


@contextlib.contextmanager
def _temporary_folder() -> Iterator[Path]:
    """A temporary folder in the data directory (on its file system, so its
    files move into place at once)."""
    data_dir = host().data_dir
    data_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=data_dir, prefix=".import-") as temp:
        yield Path(temp)


def _check(archive: zipfile.ZipFile) -> _Checked:
    raw_dir = host().raw_dir
    if raw_dir.exists() and not raw_dir.is_dir():
        raise ArchiveError(f"The raw folder isn't a folder: {raw_dir}")
    unpacked = sum(info.file_size for info in archive.infolist())
    if unpacked > MAX_UNPACKED_BYTES:
        raise ArchiveError(
            f"The export is too big once unpacked ({unpacked} bytes, at most "
            f"{MAX_UNPACKED_BYTES})"
        )
    names = [name for name in archive.namelist() if not name.endswith("/")]
    if corrupt := archive.testzip():
        raise ArchiveError(f"The zip is corrupt (first bad file: {corrupt})")
    for required in (MANIFEST_NAME, CONFIG_NAME, STORE_NAME):
        if required not in names:
            raise ArchiveError(f"Not an export: it has no {required}")
    manifest = _read_manifest(archive)
    accounts = manifest.accounts
    raw_files = []
    # Only the expected files, of plain names: nothing can be written outside
    # the folders they go to (no "..", absolute paths or hidden files)
    for name in archive.namelist():
        parts = PurePosixPath(name).parts
        if name in (MANIFEST_NAME, CONFIG_NAME, STORE_NAME):
            continue
        if name.endswith("/"):
            if parts in ((RAW_DIR,), (INGESTED_DIR,)) or (
                len(parts) == 2 and parts[0] == RAW_DIR and parts[1] in accounts
            ):
                continue
            raise ArchiveError(f"Not an export: unexpected folder {name!r}")
        if len(parts) == 2 and parts[0] == INGESTED_DIR and plain_name(parts[1]):
            continue
        if (
            len(parts) == 3
            and parts[0] == RAW_DIR
            and parts[1] in accounts
            and plain_name(parts[2])
        ):
            raw_files.append(f"{parts[1]}/{parts[2]}")
            continue
        raise ArchiveError(f"Not an export: unexpected file {name!r}")
    return _Checked(
        manifest=manifest,
        config_text=_read_config(archive, manifest.format),
        raw_files=raw_files,
    )


def _read_manifest(archive: zipfile.ZipFile) -> models.ExportManifest:
    try:
        manifest = models.ExportManifest.model_validate_json(
            archive.read(MANIFEST_NAME)
        )
    except pydantic.ValidationError as error:
        raise ArchiveError(f"The export's manifest is invalid: {error}") from error
    if manifest.format not in READABLE_FORMATS:
        raise ArchiveError(
            f"The export's format is {manifest.format}, this version of "
            f"{APP_NAME} reads {', '.join(map(str, READABLE_FORMATS))}"
        )
    if manifest.schema_version > SCHEMA_VERSION:
        raise ArchiveError(
            f"The export's store has schema version {manifest.schema_version}, "
            f"newer than supported ({SCHEMA_VERSION}): update {APP_NAME} first"
        )
    if manifest.schema_version < BASELINE_VERSION:
        raise ArchiveError(
            f"The export's store has schema version {manifest.schema_version}, "
            f"older than the oldest supported ({BASELINE_VERSION})"
        )
    for account in manifest.accounts:
        if not plain_name(account):
            raise ArchiveError(f"The export has an invalid account name: {account!r}")
    return manifest


def _read_config(archive: zipfile.ZipFile, export_format: int) -> str:
    """The export's config file, checked. Format 1's lists the accounts'
    directories, which are left out (the accounts are the raw folder's)."""
    try:
        text = archive.read(CONFIG_NAME).decode()
        data = toml.loads(text)
    except (UnicodeDecodeError, toml.TomlDecodeError) as error:
        raise ArchiveError(f"The export's config is invalid: {error}") from error
    if export_format == 1:
        ingestion = data.get("ingestion")
        if isinstance(ingestion, dict):
            ingestion.pop("directories", None)
        text = toml.dumps(data)
    try:
        config_module.validate_config(data)
    except pydantic.ValidationError as error:
        raise ArchiveError(f"The export's config is invalid: {error}") from error
    return text


def _stage(archive: zipfile.ZipFile, checked: _Checked, temp: Path) -> Path:
    """Extract the export into `temp`, and check its store (upgrading it if it
    is older). Returns the folder extracted into."""
    staged = temp.joinpath("staged")
    for name in archive.namelist():
        if name.endswith("/"):
            continue
        # (only names `_check` allows: plain names, in known folders)
        target = staged.joinpath(*PurePosixPath(name).parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        with archive.open(name) as source, target.open("wb") as destination:
            shutil.copyfileobj(source, destination)
    staged.joinpath(INGESTED_DIR).mkdir(exist_ok=True)
    for account in checked.manifest.accounts:
        staged.joinpath(RAW_DIR, account).mkdir(parents=True, exist_ok=True)
    store_file = staged.joinpath(STORE_NAME)
    connection = sqlite3.connect(store_file)
    try:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    except sqlite3.DatabaseError as error:
        raise ArchiveError(f"The export's store is invalid: {error}") from error
    finally:
        connection.close()
    if version != checked.manifest.schema_version:
        raise ArchiveError(
            f"The export's store has schema version {version}, its manifest "
            f"{checked.manifest.schema_version}"
        )
    if integrity != "ok":
        raise ArchiveError(f"The export's store is damaged: {integrity}")
    # Upgrades it, if an older version wrote it
    Store(store_file).close()
    _check_ingested(staged.joinpath(INGESTED_DIR))
    return staged


def _check_ingested(ingested_dir: Path):
    """Check that the staged ingested data loads as it will be served."""
    try:
        files = sorted(ingested_dir.glob("*.csv"))
        if files:
            data = pl.concat(pl.read_csv(file, schema=RAW_SCHEMA) for file in files)
            validate_unique_ids(data)
        times_file = ingested_dir.joinpath(host().ingest_times_file.name)
        if times_file.is_file():
            times = json.loads(times_file.read_text())
            for name in ("changed_at", "checked_at"):
                if times[name] is not None:
                    datetime.datetime.fromisoformat(times[name])
    except (
        pl.exceptions.PolarsError,
        DuplicateIds,
        ValueError,
        KeyError,
        TypeError,
    ) as error:
        raise ArchiveError(f"The export's ingested data is invalid: {error}") from error


def _set_page_size(store_file: Path, page_size: int):
    """Give the store the page size of the live one, which SQLite needs to back
    it up into the live one (in WAL mode)."""
    connection = sqlite3.connect(store_file)
    try:
        if connection.execute("PRAGMA page_size").fetchone()[0] != page_size:
            connection.execute("PRAGMA journal_mode = DELETE")
            connection.execute(f"PRAGMA page_size = {int(page_size)}")
            connection.execute("VACUUM")
    finally:
        connection.close()


@contextlib.contextmanager
def undone_on_failure() -> Iterator[list]:
    """A list to add undo steps to as changes are made: if the block fails, they
    all run in reverse (a step that fails is noted on the error)."""
    undo = []
    try:
        yield undo
    except BaseException as error:
        for step in reversed(undo):
            try:
                step()
            except Exception as undo_error:
                error.add_note(f"Undoing it failed too: {undo_error!r}")
        raise


def _replace_raw_folder(new_raw: Path, undo: list) -> Path:
    """Swap the raw folder's contents (but hidden entries) for `new_raw`'s, on
    the raw folder's file system: `new_raw` is moved into a hidden folder of the
    raw folder first, so that only renames are left. Returns that folder, which
    then holds the replaced contents, for the caller to delete once the import
    is done (if it fails, they are put back)."""
    raw_dir = host().raw_dir
    make_folders(raw_dir, undo)
    swap = Path(tempfile.mkdtemp(dir=raw_dir, prefix=".import-"))
    staged = swap.joinpath("new")
    previous = swap.joinpath("previous")
    previous.mkdir()

    def remove_swap():
        # Unless an entry couldn't be put back: then it stays there, to recover
        if any(previous.iterdir()):
            raise ArchiveError(
                f"Some of the raw folder's previous contents are still in {previous}"
            )
        shutil.rmtree(swap)

    undo.append(remove_swap)
    # (may copy, from the data directory's file system)
    shutil.move(new_raw, staged)
    for entry in sorted(raw_dir.iterdir()):
        if is_hidden(entry):
            continue
        entry.rename(previous.joinpath(entry.name))
        undo.append(lambda entry=entry: previous.joinpath(entry.name).rename(entry))
    for entry in sorted(staged.iterdir()):
        target = raw_dir.joinpath(entry.name)
        entry.rename(target)
        undo.append(lambda entry=entry, target=target: target.rename(entry))
    return swap


def make_folders(folder: Path, undo: list):
    """Make the folder and its missing parents, each removed by `undo`."""
    for missing in reversed([folder, *folder.parents]):
        if not missing.exists():
            missing.mkdir()
            undo.append(missing.rmdir)


def _replace_ingested(new_dir: Path, temp: Path, undo: list):
    ingested_dir = host().ingested_dir
    previous = temp.joinpath("previous-ingested")
    if ingested_dir.exists():
        ingested_dir.replace(previous)
        undo.append(lambda: previous.replace(ingested_dir))
    new_dir.replace(ingested_dir)
    undo.append(lambda: ingested_dir.replace(new_dir))


def _write_config(config_text: str, undo: list):
    config_file = config_module.config_file_of(config_module.LOADED_CONFIG_DIR)
    backup_file = config_file.with_name(f"{config_file.name}.bak")
    previous_texts = {
        file: file.read_text() if file.is_file() else None
        for file in (config_file, backup_file)
    }
    previous_config = config_module.LOADED_CONFIG
    config_module.replace_config_text(config_text)

    def restore():
        for file, text in previous_texts.items():
            if text is None:
                file.unlink(missing_ok=True)
            else:
                file.write_text(text)
        config_module.LOADED_CONFIG = previous_config

    undo.append(restore)


def _replace_store(store: Store, new_file: Path):
    """Copy the new store over the live one, in one transaction: every other
    connection then reads the new one."""
    source = sqlite3.connect(new_file)
    try:
        source.backup(store.connection)
    finally:
        source.close()
        # The backup writes past the connection's transactions
        note_data_changed()


def _check_account_name(account: str):
    if not plain_name(account):
        raise ArchiveError(
            f"Account {account!r} can't be exported: its name isn't a valid folder name"
        )


def plain_name(name: str) -> bool:
    """Whether `name` is one plain file or folder name: not hidden, and nothing
    that could lead elsewhere (no separators, `..`, or control characters)."""
    return (
        name not in ("", ".", "..")
        and not name.startswith(".")
        and "/" not in name
        and "\\" not in name
        and all(character.isprintable() for character in name)
        # (with room for a temporary name while it is written)
        and len(name.encode()) <= MAX_NAME_BYTES
    )
