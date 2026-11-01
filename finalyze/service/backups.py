"""Backups: exports of the whole instance (see `archive`) kept on this machine, in
the backups folder of the data directory, each with a name. They are made on
request, and (unless declined) before every import or restore. Restoring one is
importing it.

At most `LIMIT` are kept, and none is ever deleted automatically: at the limit,
a new one is refused until one is deleted.
"""

import contextlib
import datetime
import re
import threading
import zipfile
from pathlib import Path
from typing import Literal, Optional

from finalyze.config import host
from finalyze.service import archive, models
from finalyze.source.ingest import is_hidden
from finalyze.store import Store

LIMIT = 10
"""The most backups kept."""
MAX_NAME_LENGTH = 100
"""The longest name taken, in characters."""
MAX_FILE_STEM_BYTES = 150
"""The longest file name made of a name, before `.zip` and a number."""
DATE_FORMATS = {
    "dd/mm/yyyy": "%d-%m-%Y",
    "yyyy-mm-dd": "%Y-%m-%d",
    "mm/dd/yyyy": "%m-%d-%Y",
}
"""The web app's date formats, for names (and so file names): with `-`."""
UNSAFE_CHARACTERS = re.compile(r"[^\w .,()+=@&'-]+")
"""Of names, replaced in their file names."""

_lock = threading.Lock()
"""Held while a backup is made, so that two at once can't both pass the limit,
or have the same name."""


class BackupError(Exception):
    """A backup that can't be made, or found."""


class InvalidName(BackupError):
    pass


class Refused(BackupError):
    """At the limit, or a name that is taken."""


class NoSuchBackup(BackupError):
    pass


def listing(date_format: str) -> models.Backups:
    backups = _backups()
    return models.Backups(
        folder=str(host().backups_dir),
        limit=LIMIT,
        backups=backups,
        default_names={
            action: default_name(action, date_format, backups)
            for action in ("import", "restore")
        },
    )


def default_name(
    action: Literal["import", "restore"],
    date_format: str,
    backups: Optional[list[models.Backup]] = None,
) -> str:
    """`before <action> on <today>`, numbered if that name is taken."""
    today = datetime.date.today().strftime(DATE_FORMATS[date_format])
    name = f"before {action} on {today}"
    taken = {
        _name_key(backup.name)
        for backup in (_backups() if backups is None else backups)
    }
    number = 1
    numbered = name
    while _name_key(numbered) in taken:
        number += 1
        numbered = f"{name} ({number})"
    return numbered


def check_new(name: str) -> str:
    """The name, stripped, if a backup of it can be made now: else why not
    (`InvalidName`, or `Refused` at the limit or if taken)."""
    name = check_name(name)
    backups = _backups()
    if len(backups) >= LIMIT:
        raise Refused(
            f"There are already {len(backups)} backups, the most kept: delete one "
            "first (none is ever deleted automatically)"
        )
    if any(_name_key(backup.name) == _name_key(name) for backup in backups):
        raise Refused(f"A backup named {name!r} already exists: choose another name")
    return name


def check_name(name: str) -> str:
    name = name.strip()
    if not name:
        raise InvalidName("A backup needs a name")
    if len(name) > MAX_NAME_LENGTH:
        raise InvalidName(
            f"A backup's name is at most {MAX_NAME_LENGTH} characters "
            f"(this one has {len(name)})"
        )
    if not name.isprintable():
        raise InvalidName("A backup's name can't have control characters")
    return name


def create(
    store: Store,
    name: str,
    *,
    ingest_lock: Optional[contextlib.AbstractContextManager] = None,
) -> models.Backup:
    """Back up the instance, as `name` (see `check_new`)."""
    with _lock:
        name = check_new(name)
        backups_dir = host().backups_dir
        backups_dir.mkdir(parents=True, exist_ok=True)
        target = _new_file(backups_dir, name)
        archive.export(store, target, ingest_lock=ingest_lock, name=name)
        return _backup(target)


def file_of(file_name: str) -> Path:
    """The backup of that file name, only in the backups folder."""
    if not archive.plain_name(file_name) or not file_name.endswith(".zip"):
        raise NoSuchBackup(f"No such backup: {file_name!r}")
    file = host().backups_dir.joinpath(file_name)
    if not file.is_file():
        raise NoSuchBackup(f"No such backup: {file_name!r}")
    return file


def delete(file_name: str):
    file_of(file_name).unlink()


def file_stem(name: str) -> str:
    """A name, made safe for a file name: other characters than letters, digits
    and some punctuation become `-`, and it is shortened if long."""
    stem = re.sub(r"-{2,}", "-", UNSAFE_CHARACTERS.sub("-", name))
    stem = stem.encode()[:MAX_FILE_STEM_BYTES].decode(errors="ignore")
    return stem.strip(" .-") or "backup"


def _new_file(backups_dir: Path, name: str) -> Path:
    """A new file for a backup named `name`: numbered if (another name made
    safe the same way) its file name is taken."""
    stem = file_stem(name)
    file = backups_dir.joinpath(f"{stem}.zip")
    number = 1
    while file.exists():
        number += 1
        file = backups_dir.joinpath(f"{stem} ({number}).zip")
    return file


def _backups() -> list[models.Backup]:
    """Every backup, newest first."""
    backups_dir = host().backups_dir
    if not backups_dir.is_dir():
        return []
    backups = [
        _backup(file)
        for file in backups_dir.iterdir()
        if file.is_file() and file.suffix == ".zip" and not is_hidden(file)
    ]
    return sorted(backups, key=lambda backup: backup.created_at, reverse=True)


def _backup(file: Path) -> models.Backup:
    """A backup, from its manifest. A zip that has none (or isn't valid) is still
    listed, by its file name, so that it can be deleted."""
    stat = file.stat()
    try:
        with zipfile.ZipFile(file) as backup_zip:
            manifest = models.ExportManifest.model_validate_json(
                backup_zip.read(archive.MANIFEST_NAME)
            )
    # (also encrypted zips, and unknown compressions)
    except (
        OSError,
        KeyError,
        ValueError,
        RuntimeError,
        NotImplementedError,
        zipfile.BadZipFile,
    ):
        manifest = None
    return models.Backup(
        file=file.name,
        name=(manifest and manifest.name) or file.stem,
        # (with a time zone, to sort them)
        created_at=(
            manifest.created_at.astimezone()
            if manifest
            else datetime.datetime.fromtimestamp(stat.st_mtime).astimezone()
        ),
        size=stat.st_size,
        accounts=manifest.accounts if manifest else [],
    )


def _name_key(name: str) -> str:
    """Names are unique whatever their case."""
    return name.strip().casefold()
