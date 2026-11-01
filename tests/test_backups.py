"""Backups, through the API: named exports kept on this machine, made on request
and before imports and restores, at most `backups.LIMIT`, and restored as
imports."""

import datetime
import json
import sqlite3
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from finalyze.api import create_app
from finalyze.config import host
from finalyze.service import archive, backups

LOCAL_URL = "http://127.0.0.1:8765"


@pytest.fixture
def client(demo_config):
    return TestClient(create_app(static_dir=None), base_url=LOCAL_URL)


def create(client, name: str):
    return client.post("/api/backups", json={"name": name})


def listed(client) -> list[dict]:
    response = client.get("/api/backups")
    assert response.status_code == 200, response.text
    return response.json()["backups"]


def restore(client, file: str, **fields):
    return client.post(f"/api/backups/{file}/restore", json=fields)


def import_zip(client, zip_path: Path, **fields):
    with zip_path.open("rb") as zip_file:
        return client.post(
            "/api/import",
            files={"file": (zip_path.name, zip_file, "application/zip")},
            data={name: str(value).lower() for name, value in fields.items()},
        )


def store_content() -> list[str]:
    connection = sqlite3.connect(host().db_file)
    try:
        return list(connection.iterdump())
    finally:
        connection.close()


def raw_files() -> dict[str, bytes]:
    raw_dir = host().raw_dir
    return {
        file.relative_to(raw_dir).as_posix(): file.read_bytes()
        for file in sorted(raw_dir.rglob("*"))
        if file.is_file()
    }


def instance_files() -> dict[str, bytes]:
    """The store, the ingested data and the raw files, to check nothing changed
    (but the backups)."""
    data_dir = host().data_dir
    ingested = {file.name: file.read_bytes() for file in host().ingested_dir.iterdir()}
    return {"store": store_content(), "ingested": ingested, "raw": raw_files()} | {
        "data dir": sorted(path.name for path in data_dir.iterdir())
    }


def today(date_format: str = "%d-%m-%Y") -> str:
    return datetime.date.today().strftime(date_format)


def test_create_list_download_and_restore(client):
    """A backup, then changes (to the store and the raw folder), then restoring
    it brings it all back, after backing up the changed state."""
    backed_up_store = store_content()
    backed_up_raw = raw_files()
    response = create(client, "  Before cleaning tags ")
    assert response.status_code == 200, response.text
    backup = response.json()
    assert backup["name"] == "Before cleaning tags"
    assert backup["file"] == "Before cleaning tags.zip"
    assert backup["accounts"] == ["cash", "eitan", "loan", "maya", "savings"]
    file = host().backups_dir.joinpath(backup["file"])
    assert backup["size"] == file.stat().st_size
    assert listed(client) == [backup]
    # A download is the backup's zip: an export, named in its manifest
    download = client.get(f"/api/backups/{backup['file']}")
    assert download.status_code == 200
    assert download.headers["content-type"] == "application/zip"
    assert download.content == file.read_bytes()
    with zipfile.ZipFile(file) as backup_zip:
        manifest = json.loads(backup_zip.read(archive.MANIFEST_NAME))
    assert manifest["name"] == "Before cleaning tags"
    assert manifest["format"] == archive.FORMAT

    # Changes: of the store, and of the raw folder (a new account, a new file)
    response = client.patch("/api/tags", json={"tag": "food", "kind": "one-off"})
    assert response.status_code == 200, response.text
    host().raw_dir.joinpath("pot").mkdir()
    host().raw_dir.joinpath("pot", "pot.csv").write_text("new")
    host().raw_dir.joinpath("cash", "later.csv").write_text("later")
    changed_store = store_content()
    assert changed_store != backed_up_store

    response = restore(client, backup["file"])
    assert response.status_code == 200, response.text
    preview = response.json()
    assert preview["dry_run"] and preview["backup"] is None
    assert preview["manifest"]["name"] == "Before cleaning tags"
    assert preview["removed_raw_files"] == ["cash/later.csv", "pot/pot.csv"]
    assert store_content() == changed_store

    response = restore(client, backup["file"], dry_run=False)
    assert response.status_code == 200, response.text
    result = response.json()
    assert store_content() == backed_up_store
    assert raw_files() == backed_up_raw
    assert not host().raw_dir.joinpath("pot").exists()
    # The changed state was backed up first, by default as "before restore on"
    before_restore = result["backup"]
    assert before_restore["name"] == f"before restore on {today()}"
    assert "pot" in before_restore["accounts"]
    assert [backup["name"] for backup in listed(client)] == [
        before_restore["name"],
        "Before cleaning tags",
    ]
    # Restoring that one brings the changes back
    response = restore(client, before_restore["file"], dry_run=False, backup=False)
    assert response.status_code == 200, response.text
    assert store_content() == changed_store
    assert host().raw_dir.joinpath("pot", "pot.csv").read_text() == "new"
    assert len(listed(client)) == 2


@pytest.fixture
def demo_export(client, tmp_path) -> Path:
    path = tmp_path.joinpath("demo.zip")
    path.write_bytes(client.get("/api/export").content)
    return path


def test_the_backup_before_an_import_can_be_named_or_declined(client, demo_export):
    response = import_zip(
        client, demo_export, dry_run=False, backup_name="before the move"
    )
    assert response.status_code == 200, response.text
    assert response.json()["backup"]["name"] == "before the move"
    assert host().backups_dir.joinpath("before the move.zip").is_file()
    # By default, dated, and numbered if taken
    for number in ("", " (2)"):
        response = import_zip(client, demo_export, dry_run=False)
        assert response.status_code == 200, response.text
        assert response.json()["backup"]["name"] == (
            f"before import on {today()}{number}"
        )
    # Declined
    response = import_zip(client, demo_export, dry_run=False, backup=False)
    assert response.status_code == 200, response.text
    assert response.json()["backup"] is None
    assert len(listed(client)) == 3


def test_default_names_follow_the_date_format(client):
    names = client.get("/api/backups").json()["default_names"]
    assert names == {
        "import": f"before import on {today()}",
        "restore": f"before restore on {today()}",
    }
    response = client.put("/api/settings/display", json={"date_format": "yyyy-mm-dd"})
    assert response.status_code == 200, response.text
    create(client, f"Before import on {today('%Y-%m-%d')}")
    names = client.get("/api/backups").json()["default_names"]
    # Unique, whatever the case
    assert names["import"] == f"before import on {today('%Y-%m-%d')} (2)"
    assert names["restore"] == f"before restore on {today('%Y-%m-%d')}"


def test_at_the_limit_backups_are_refused_and_so_are_imports_asking_for_one(
    client, demo_export, monkeypatch
):
    assert backups.LIMIT == 10
    monkeypatch.setattr(backups, "LIMIT", 2)
    for name in ("first", "second"):
        assert create(client, name).status_code == 200
    response = create(client, "third")
    assert response.status_code == 409
    assert "delete one first" in response.json()["detail"]
    before = instance_files()
    files_before = sorted(path.name for path in host().backups_dir.iterdir())
    first = listed(client)[-1]["file"]
    for dry_run in (True, False):
        response = import_zip(client, demo_export, dry_run=dry_run)
        assert response.status_code == 409, response.text
        assert "delete one first" in response.json()["detail"]
        response = restore(client, first, dry_run=dry_run)
        assert response.status_code == 409, response.text
    assert instance_files() == before
    assert sorted(path.name for path in host().backups_dir.iterdir()) == files_before
    # Without a backup, or once one is deleted, it goes ahead
    response = import_zip(client, demo_export, dry_run=False, backup=False)
    assert response.status_code == 200, response.text
    assert client.delete(f"/api/backups/{first}").status_code == 204
    response = import_zip(client, demo_export, dry_run=False)
    assert response.status_code == 200, response.text
    assert len(listed(client)) == 2


def test_names_are_unique_and_valid(client, demo_export):
    assert create(client, "Monthly").status_code == 200
    for taken in ("Monthly", " monthly ", "MONTHLY"):
        response = create(client, taken)
        assert response.status_code == 409
        assert "already exists" in response.json()["detail"]
    response = import_zip(client, demo_export, dry_run=False, backup_name="monthly")
    assert response.status_code == 409
    for invalid in ("", "   ", "x" * 101, "tab\there"):
        response = create(client, invalid)
        assert response.status_code == 422, invalid
        response = import_zip(client, demo_export, dry_run=False, backup_name=invalid)
        # (an empty name is the default's)
        assert response.status_code == (200 if not invalid.strip() else 422), invalid
    assert create(client, "x" * 100).status_code == 200


def test_delete(client):
    file = create(client, "old").json()["file"]
    assert client.delete(f"/api/backups/{file}").status_code == 204
    assert listed(client) == []
    for method in ("GET", "DELETE"):
        assert client.request(method, f"/api/backups/{file}").status_code == 404
    assert restore(client, file).status_code == 404


def test_only_the_backups_folders_zips_are_reachable(client):
    create(client, "kept")
    host().data_dir.joinpath("secret.zip").write_text("x")
    host().backups_dir.joinpath("notes.txt").write_text("x")
    host().backups_dir.joinpath(".partial.zip").write_text("x")
    for name in ("notes.txt", ".partial.zip", "..%2Fsecret.zip", "%2E%2E%2Fsecret.zip"):
        for method in ("GET", "DELETE"):
            response = client.request(method, f"/api/backups/{name}")
            assert response.status_code == 404, (method, name)
    assert host().data_dir.joinpath("secret.zip").exists()
    assert [backup["name"] for backup in listed(client)] == ["kept"]


def test_zips_without_a_name_are_listed_by_their_file(client):
    """E.g. an export copied into the folder, or a broken zip: listed (they count
    towards the limit) so that they can be deleted."""
    backups_dir = host().backups_dir
    backups_dir.mkdir()
    export = backups_dir.joinpath("finalyze-export-2025-01-01_00-00-00.zip")
    export.write_bytes(client.get("/api/export").content)
    backups_dir.joinpath("broken.zip").write_text("not a zip")
    # A manifest's time without a time zone
    with (
        zipfile.ZipFile(export) as source,
        zipfile.ZipFile(backups_dir.joinpath("naive.zip"), "w") as naive,
    ):
        manifest = json.loads(source.read(archive.MANIFEST_NAME))
        manifest |= {"created_at": "2025-01-01T10:00:00", "name": "naive"}
        naive.writestr(archive.MANIFEST_NAME, json.dumps(manifest))
    by_name = {backup["name"]: backup for backup in listed(client)}
    assert set(by_name) == {export.stem, "broken", "naive"}
    assert by_name["broken"]["accounts"] == []
    assert by_name[export.stem]["accounts"]


@pytest.mark.parametrize(
    ("name", "file"),
    [
        ("before import on 01/11/2026", "before import on 01-11-2026 (2).zip"),
        ("../../etc/passwd", "etc-passwd.zip"),
        ("..hidden", "hidden.zip"),
        ('a\\b:c*d?e"f<g>h|i', "a-b-c-d-e-f-g-h-i.zip"),
        ("גיבוי לפני ייבוא", "גיבוי לפני ייבוא.zip"),
        ("✓✓✓", "backup.zip"),
        ("é" * 100, f"{'é' * 75}.zip"),
    ],
)
def test_file_names_are_made_safe(client, name, file):
    create(client, "before import on 01-11-2026")
    response = create(client, name)
    assert response.status_code == 200, response.text
    backup = response.json()
    assert backup["file"] == file and backup["name"] == name
    assert host().backups_dir.joinpath(file).is_file()
    assert archive.plain_name(file)
