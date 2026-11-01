"""Uploading raw files into an account's folder of the raw folder, from anywhere
the browser (or an agent) is: each file is checked by the parsers first, and
replacing a file of the same name is explicit. An upload writes every file, or
none."""

import contextlib
import dataclasses
import functools
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

from finalyze.config import host
from finalyze.service import archive, models
from finalyze.service import ingest as ingest_check
from finalyze.service.archive import plain_name

MAX_FILE_BYTES = 50 * 1024**2
"""The largest raw file an upload takes (50 MiB: bank exports are far smaller)."""
MAX_UPLOAD_BYTES = 200 * 1024**2
"""The most one upload request takes, of every file (200 MiB)."""


class UploadError(Exception):
    """An upload that can't be done as asked: nothing was written."""


class UploadConflict(UploadError):
    """Files of the same names exist, and replacing them wasn't asked."""


@dataclasses.dataclass(frozen=True)
class Upload:
    name: str
    """The file's name, as sent."""
    content: BinaryIO


def upload(
    account: str, uploads: list[Upload], *, dry_run: bool = True, replace: bool = False
) -> models.RawUpload:
    """Check the files (by default), or write them into the account's folder
    (made if new). Writing refuses files no parser reads, and replacing files of
    the same names unless `replace`."""
    if not plain_name(account):
        raise UploadError(f"Not a valid account (folder) name: {account!r}")
    if not uploads:
        raise UploadError("No files")
    names = [file.name for file in uploads]
    for name in names:
        if not plain_name(name):
            raise UploadError(
                f"Not a valid file name: {name!r} (no folders, and not hidden)"
            )
    if duplicates := sorted({name for name in names if names.count(name) > 1}):
        raise UploadError(f"Files of the same name: {', '.join(duplicates)}")
    folder = host().raw_dir.joinpath(account)
    if folder.exists() and not folder.is_dir():
        raise UploadError(f"Not a folder: {folder}")
    checked_at = ingest_check.ingest_times().checked_at
    with _staged(uploads) as staged:
        files = []
        for file in staged:
            target = folder.joinpath(file.name)
            if target.exists() and not target.is_file():
                raise UploadError(f"Not a file: {target}")
            check, _kept = ingest_check.file_dry_run(account, file, checked_at)
            replaces = (
                ingest_check.file_dry_run(account, target, checked_at)[0]
                if target.is_file()
                else None
            )
            files.append(
                models.RawUploadFile(
                    name=file.name,
                    path=str(target),
                    accepted=check.parser is not None,
                    check=check.model_copy(update={"path": str(target)}),
                    replaces=replaces,
                )
            )
        result = models.RawUpload(
            dry_run=dry_run,
            account=account,
            folder=str(folder),
            new_account=not folder.is_dir(),
            files=files,
        )
        if dry_run:
            return result
        if rejected := [file.name for file in files if not file.accepted]:
            raise UploadError(f"No parser reads: {', '.join(rejected)}")
        if (replaced := [file.name for file in files if file.replaces]) and not replace:
            raise UploadConflict(
                f"Files of the same names exist in {folder}: {', '.join(replaced)}. "
                "Replace them explicitly"
            )
        _write_all(folder, staged)
    return result


def _write_all(folder: Path, staged: list[Path]):
    """Write every file into the folder (made if new), or none: they are copied
    into a hidden folder of it first (which ingesting skips), then renamed into
    place. If that fails, the files replaced are put back."""
    with archive.undone_on_failure() as undo_folders:
        archive.make_folders(folder, undo_folders)
        with tempfile.TemporaryDirectory(dir=folder, prefix=".upload-") as temp:
            new_dir = Path(temp).joinpath("new")
            replaced_dir = Path(temp).joinpath("replaced")
            new_dir.mkdir()
            replaced_dir.mkdir()
            for file in staged:
                shutil.copyfile(file, new_dir.joinpath(file.name))
            with archive.undone_on_failure() as undo:
                for file in staged:
                    target = folder.joinpath(file.name)
                    if target.exists():
                        # A copy, so that the file is always there
                        replaced = replaced_dir.joinpath(file.name)
                        shutil.copy2(target, replaced)
                        undo.append(
                            lambda replaced=replaced, target=target: replaced.replace(
                                target
                            )
                        )
                    else:
                        undo.append(functools.partial(target.unlink, missing_ok=True))
                    new_dir.joinpath(file.name).replace(target)


@contextlib.contextmanager
def _staged(uploads: list[Upload]) -> Iterator[list[Path]]:
    """The uploads, written into a temporary folder of the data directory with
    their names (parsers may go by them)."""
    data_dir = host().data_dir
    data_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=data_dir, prefix=".upload-") as temp:
        staged = []
        for upload_file in uploads:
            target = Path(temp).joinpath(upload_file.name)
            with target.open("wb") as destination:
                _copy_limited(upload_file, destination)
            staged.append(target)
        yield staged


def _copy_limited(upload_file: Upload, destination: BinaryIO):
    copied = 0
    while chunk := upload_file.content.read(1024**2):
        copied += len(chunk)
        if copied > MAX_FILE_BYTES:
            raise UploadError(
                f"{upload_file.name} is too big (at most {MAX_FILE_BYTES} bytes)"
            )
        destination.write(chunk)
