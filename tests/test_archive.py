"""Export and import of the whole instance, through the API: a download, then an
upload of it into a fresh instance, and imports that are refused or fail,
leaving everything as it was."""

import datetime
import io
import json
import os
import sqlite3
import threading
import zipfile
from pathlib import Path

import pytest
import toml
from fastapi.testclient import TestClient

from finalyze import api
from finalyze import config as config_module
from finalyze.api import RequestGate, create_app
from finalyze.config import host
from finalyze.service import archive
from finalyze.store import SCHEMA_VERSION, Store

LOCAL_URL = "http://127.0.0.1:8765"


@pytest.fixture
def client(demo_config):
    return TestClient(create_app(static_dir=None), base_url=LOCAL_URL)


@pytest.fixture
def demo_export(client, tmp_path) -> Path:
    """The demo, downloaded."""
    response = client.get("/api/export")
    assert response.status_code == 200, response.text
    path = tmp_path.joinpath("demo-export.zip")
    path.write_bytes(response.content)
    return path


def import_zip(client, zip_path: Path, **fields):
    """Upload a zip to import (a dry run unless `dry_run=False`)."""
    with zip_path.open("rb") as zip_file:
        return client.post(
            "/api/import",
            files={"file": (zip_path.name, zip_file, "application/zip")},
            data={name: str(value).lower() for name, value in fields.items()},
        )


class Instance:
    """A fresh instance, without data, in its own directory."""

    def __init__(self, root: Path):
        self.root = root
        self.config_dir = root.joinpath("config")
        self.config_dir.mkdir(parents=True)
        self.config_file = self.config_dir.joinpath("config.toml")
        self.config_file.write_text(
            "# Mine\n[ingestion.filters]\nstart_date = 2001-01-01\n"
        )
        self.host_file = self.config_dir.joinpath("host.toml")
        self.host_file.write_text(
            f'data_dir = "{root.joinpath("data")}"\n'
            f'raw_dir = "{root.joinpath("raw")}"\nport = 9999\n'
        )
        config_module.load_config(config_dir=self.config_dir, use_preloaded=False)
        self.config = config_module.config()
        self.host = config_module.host()
        self.host.create_directories()
        Store(self.host.db_file).close()

    def files(self) -> dict[str, bytes]:
        """Every file of the instance, by path, to check nothing changed (but
        the store's temporary files)."""
        return {
            str(file.relative_to(self.root)): file.read_bytes()
            for file in sorted(self.root.rglob("*"))
            if file.is_file() and not file.name.endswith(("-wal", "-shm"))
        }


@pytest.fixture
def fresh(demo_export, tmp_path) -> Instance:
    """A fresh instance, made after the demo was exported, as the loaded one."""
    return Instance(tmp_path.joinpath("fresh"))


def store_content(db_file: Path) -> list[str]:
    connection = sqlite3.connect(db_file)
    try:
        return list(connection.iterdump())
    finally:
        connection.close()


def raw_files(raw_dir: Path) -> dict[str, dict[str, bytes]]:
    return {
        folder.name: {file.name: file.read_bytes() for file in sorted(folder.iterdir())}
        for folder in sorted(raw_dir.iterdir())
    }


def test_round_trip_into_a_fresh_instance(demo_config, demo_export, client, tmp_path):
    demo_status = client.get("/api/status").json()
    demo_transactions = client.get("/api/transactions", params={"limit": 0}).json()
    demo_host = host()
    demo_config_text = config_module.config_file_of(
        config_module.LOADED_CONFIG_DIR
    ).read_text()
    fresh = Instance(tmp_path.joinpath("fresh"))
    # Served before the import: the fresh instance has nothing (and its
    # ingested data, empty, is now cached)
    status = client.get("/api/status").json()
    assert status["transactions"] == 0 and status["accounts"] == []

    response = import_zip(client, demo_export)
    assert response.status_code == 200, response.text
    preview = response.json()
    assert preview["dry_run"] and preview["backup"] is None
    assert preview["manifest"]["format"] == archive.FORMAT
    assert preview["manifest"]["schema_version"] == SCHEMA_VERSION
    assert preview["manifest"]["accounts"] == [
        "cash",
        "eitan",
        "loan",
        "maya",
        "savings",
    ]
    assert preview["raw_dir"] == str(fresh.host.raw_dir)
    assert preview["raw_files"] > 0
    assert preview["replaced_raw_files"] == preview["removed_raw_files"] == []
    assert not fresh.host.raw_dir.exists()
    assert not fresh.host.backups_dir.exists()

    response = import_zip(client, demo_export, dry_run=False)
    assert response.status_code == 200, response.text
    result = response.json()
    assert not result["dry_run"]

    # The same store and raw files, into this host's folders
    assert store_content(fresh.host.db_file) == store_content(demo_host.db_file)
    connection = sqlite3.connect(fresh.host.db_file)
    assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    connection.close()
    assert raw_files(fresh.host.raw_dir) == raw_files(demo_host.raw_dir)
    # The config is the demo's file as it was, and the host file is untouched
    assert fresh.config_file.read_text() == demo_config_text
    assert config_module.config() == demo_config
    assert fresh.config_dir.joinpath("config.toml.bak").read_text().startswith("# Mine")
    assert config_module.host() == fresh.host
    assert fresh.host_file.read_text().endswith("port = 9999\n")

    # The state before the import was backed up, into this host's backups
    today = datetime.date.today().strftime("%d-%m-%Y")
    backup = result["backup"]
    assert backup["name"] == f"before import on {today}"
    assert backup["file"] == f"before import on {today}.zip"
    with zipfile.ZipFile(fresh.host.backups_dir.joinpath(backup["file"])) as backup_zip:
        manifest = json.loads(backup_zip.read(archive.MANIFEST_NAME))
        assert backup_zip.read(archive.CONFIG_NAME).decode().startswith("# Mine")
    assert manifest["accounts"] == [] and manifest["name"] == backup["name"]
    listed = client.get("/api/backups").json()
    assert listed["folder"] == str(fresh.host.backups_dir)
    assert listed["backups"] == [backup]

    # Served at once, without a restart: the same transactions
    assert client.get("/api/status").json() | {
        "db_path": None,
        "ingested_dir": None,
    } == demo_status | {"db_path": None, "ingested_dir": None}
    assert (
        client.get("/api/transactions", params={"limit": 0}).json() == demo_transactions
    )
    # And the raw files ingest to the same data
    ingest = client.post("/api/ingest")
    assert ingest.status_code == 200, ingest.text
    assert not ingest.json()["data_changed"]


def test_export_is_a_download_without_host_paths(client, demo_config, tmp_path):
    # An empty account folder (e.g. only transfers go to it) is kept
    host().raw_dir.joinpath("pot").mkdir()
    response = client.get("/api/export")
    assert response.status_code == 200, response.text
    assert response.headers["content-type"] == "application/zip"
    disposition = response.headers["content-disposition"]
    assert disposition.startswith('attachment; filename="finalyze-export-')
    # Its temporary file is gone once sent
    assert not list(host().data_dir.glob(".download-*"))
    assert not host().backups_dir.exists()
    with zipfile.ZipFile(io.BytesIO(response.content)) as export_zip:
        names = export_zip.namelist()
        config_text = export_zip.read(archive.CONFIG_NAME).decode()
        manifest_text = export_zip.read(archive.MANIFEST_NAME).decode()
    assert {name.split("/")[0] for name in names} == {
        "manifest.json",
        "config.toml",
        "finalyze.db",
        "ingested",
        "raw",
    }
    assert (
        config_text
        == config_module.config_file_of(config_module.LOADED_CONFIG_DIR).read_text()
    )
    for text in (config_text, manifest_text):
        assert str(host().data_dir) not in text and str(host().raw_dir) not in text
    assert "raw/pot/" in names
    assert json.loads(manifest_text)["name"] is None
    assert json.loads(manifest_text)["accounts"] == [
        "cash",
        "eitan",
        "loan",
        "maya",
        "pot",
        "savings",
    ]
    assert sorted(name for name in names if name.startswith("raw/maya/")) == [
        "raw/maya/",
        *(
            f"raw/maya/{file.name}"
            for file in sorted(host().raw_dir.joinpath("maya").iterdir())
        ),
    ]


def test_a_download_leaves_nothing_behind_however_it_ends(client, demo_config):
    # (a range it can't serve made the file's response end early, unsent)
    for headers in ({"range": "bytes=abc"}, {"range": "bytes=999999999-"}, {}):
        response = client.get("/api/export", headers=headers)
        assert response.status_code == 200, response.text
        assert int(response.headers["content-length"]) == len(response.content)
        assert not list(host().data_dir.glob(".*"))


def test_leftovers_of_an_abrupt_stop_are_removed(demo_config):
    data_dir = host().data_dir
    for name in (".download-x.zip", ".upload-x", ".import-x", ".export-x", ".tmp"):
        if "." in name[1:]:
            data_dir.joinpath(name).write_text("x")
        else:
            data_dir.joinpath(name).mkdir()
            data_dir.joinpath(name, "file").write_text("x")
    before = sorted(
        path.name for path in data_dir.iterdir() if not path.name.startswith(".")
    )
    archive.remove_leftovers()
    assert sorted(path.name for path in data_dir.iterdir()) == before
    assert host().raw_dir.joinpath("cash", "wallet.csv").is_file()


def rewritten(source: Path, target: Path, changes: dict) -> Path:
    """A copy of an export with some members replaced (None removes them)."""
    with zipfile.ZipFile(source) as source_zip, zipfile.ZipFile(target, "w") as copy:
        for item in source_zip.infolist():
            if item.filename not in changes:
                copy.writestr(item, source_zip.read(item))
        for name, content in changes.items():
            if content is not None:
                copy.writestr(name, content)
    return target


def member(export: Path, name: str) -> bytes:
    with zipfile.ZipFile(export) as export_zip:
        return export_zip.read(name)


def not_a_zip(path: Path) -> Path:
    path.write_text("not a zip")
    return path


def manifest_with(export: Path, **fields) -> str:
    with zipfile.ZipFile(export) as export_zip:
        return json.dumps(json.loads(export_zip.read(archive.MANIFEST_NAME)) | fields)


def with_member(name: str, content="a,b"):
    return lambda export, path: rewritten(export, path, {name: content})


@pytest.mark.parametrize(
    ("make_zip", "message"),
    [
        (lambda export, path: not_a_zip(path), "Not a zip"),
        (
            lambda export, path: rewritten(export, path, {"manifest.json": None}),
            "no manifest.json",
        ),
        (
            lambda export, path: rewritten(
                export,
                path,
                {
                    "manifest.json": manifest_with(
                        export, schema_version=SCHEMA_VERSION + 1
                    )
                },
            ),
            "newer than supported",
        ),
        (
            lambda export, path: rewritten(
                export, path, {"manifest.json": manifest_with(export, format=4)}
            ),
            "format is 4",
        ),
        (
            lambda export, path: rewritten(
                export, path, {"manifest.json": manifest_with(export, accounts=[".."])}
            ),
            "invalid account name",
        ),
        # Zip slip: nothing may land outside the folders it goes to
        (with_member("../outside.csv"), "unexpected file"),
        (with_member("/tmp/absolute.csv"), "unexpected file"),
        (with_member("raw/maya/../../../outside.csv"), "unexpected file"),
        (with_member("raw/../outside.csv"), "unexpected file"),
        (with_member("raw/maya/..\\..\\outside.csv"), "unexpected file"),
        (with_member("ingested/../../outside.csv"), "unexpected file"),
        (with_member("raw/maya/.hidden.csv"), "unexpected file"),
        (with_member("raw/maya/sub/file.csv"), "unexpected file"),
        (with_member("raw/stranger/x.csv"), "unexpected file"),
        (with_member("host.toml"), "unexpected file"),
        (with_member("raw/stranger/", ""), "unexpected folder"),
        (with_member("finalyze.db", "garbage"), "store"),
        (with_member("config.toml", '[ingestion]\nfilters = "x"\n'), "config"),
        (with_member("config.toml", '[general]\ndata_dir = "/x"\n'), "config"),
        (
            lambda export, path: rewritten(
                export, path, {"ingested/copy.csv": member(export, "ingested/maya.csv")}
            ),
            "ingested data",
        ),
        (with_member("ingested/ingest-times.json", "{"), "ingested data"),
    ],
)
def test_invalid_exports_change_nothing(
    client, demo_export, fresh, tmp_path, make_zip, message
):
    bad_zip = make_zip(demo_export, tmp_path.joinpath("bad.zip"))
    before = fresh.files()
    for dry_run in (True, False):
        response = import_zip(client, bad_zip, dry_run=dry_run)
        assert response.status_code == 422, response.text
        assert message in response.json()["detail"]
    assert fresh.files() == before
    assert not fresh.host.raw_dir.exists()
    assert not fresh.host.backups_dir.exists()
    assert not tmp_path.joinpath("outside.csv").exists()
    assert not fresh.root.joinpath("outside.csv").exists()


def test_zip_bombs_are_refused(client, demo_export, fresh, monkeypatch):
    unpacked = sum(info.file_size for info in zipfile.ZipFile(demo_export).infolist())
    monkeypatch.setattr(archive, "MAX_UNPACKED_BYTES", unpacked - 1)
    before = fresh.files()
    response = import_zip(client, demo_export, dry_run=False)
    assert response.status_code == 422
    assert "too big once unpacked" in response.json()["detail"]
    assert fresh.files() == before


def test_uploads_over_the_limit_are_refused(client, demo_export, fresh, monkeypatch):
    size = demo_export.stat().st_size
    monkeypatch.setattr(api, "UPLOAD_LIMITS", (("/api/import", size - 1),))
    before = fresh.files()
    # By its declared length
    response = import_zip(client, demo_export, dry_run=False)
    assert response.status_code == 413
    assert "Too large" in response.json()["detail"]
    # And sent without one (chunked), once it has sent too much
    body = io.BytesIO()
    boundary = "finalyze-test-boundary"
    body.write(
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; '
        f'filename="x.zip"\r\nContent-Type: application/zip\r\n\r\n'.encode()
    )
    body.write(demo_export.read_bytes())
    body.write(f"\r\n--{boundary}--\r\n".encode())
    content = body.getvalue()

    def chunks():
        for start in range(0, len(content), 64 * 1024):
            yield content[start : start + 64 * 1024]

    response = client.post(
        "/api/import",
        content=chunks(),
        headers={"content-type": f"multipart/form-data; boundary={boundary}"},
    )
    assert response.status_code == 413
    assert "Too large" in response.json()["detail"]
    assert fresh.files() == before
    # Within the limit, the same upload goes through
    monkeypatch.setattr(api, "UPLOAD_LIMITS", (("/api/import", len(content)),))
    response = client.post(
        "/api/import",
        content=chunks(),
        headers={"content-type": f"multipart/form-data; boundary={boundary}"},
    )
    assert response.status_code == 200, response.text


def export_raw_files(export: Path) -> dict[str, bytes]:
    """An export's raw files, as `<account>/<name>`."""
    with zipfile.ZipFile(export) as export_zip:
        return {
            name.removeprefix("raw/"): export_zip.read(name)
            for name in export_zip.namelist()
            if name.startswith("raw/") and not name.endswith("/")
        }


def files_under(folder: Path) -> dict[str, bytes]:
    return {
        file.relative_to(folder).as_posix(): file.read_bytes()
        for file in sorted(folder.rglob("*"))
        if file.is_file()
    }


@pytest.mark.parametrize("backup", [True, False])
def test_the_raw_folder_becomes_the_exports(client, demo_export, fresh, backup):
    """Its files that the export doesn't have are removed (listed first): any
    folder left would be ingested as an account."""
    exported = export_raw_files(demo_export)
    raw_dir = fresh.host.raw_dir
    same_name = sorted(name for name in exported if name.startswith("maya/"))[0]
    for name, content in {
        same_name: "an older file",
        "maya/mine.csv": "someone's own file",
        "other/theirs.csv": "another account's file",
        # Files ingesting ignores (and so no backup holds)
        "maya/.hidden.csv": "hidden",
        "maya/old/2019.csv": "in a subfolder",
        "stray.csv": "outside account folders",
        # Hidden entries of the raw folder stay
        ".git/config": "kept",
    }.items():
        raw_dir.joinpath(name).parent.mkdir(parents=True, exist_ok=True)
        raw_dir.joinpath(name).write_text(content)
    before = fresh.files()

    preview = import_zip(client, demo_export, backup=backup).json()
    assert preview["replaced_raw_files"] == [same_name]
    assert preview["removed_raw_files"] == [
        "maya/.hidden.csv",
        "maya/mine.csv",
        "maya/old/2019.csv",
        "other/theirs.csv",
        "stray.csv",
    ]
    assert preview["unbacked_raw_files"] == [
        "maya/.hidden.csv",
        "maya/old/2019.csv",
        "stray.csv",
    ]
    assert fresh.files() == before

    response = import_zip(client, demo_export, dry_run=False, backup=backup)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["removed_raw_files"] == preview["removed_raw_files"]
    assert files_under(raw_dir) == exported | {".git/config": b"kept"}
    # The backup holds the account files that were replaced or removed
    backups_made = list(fresh.host.backups_dir.glob("*.zip"))
    if backup:
        assert [file.name for file in backups_made] == [result["backup"]["file"]]
        assert export_raw_files(backups_made[0]) == {
            same_name: b"an older file",
            "maya/mine.csv": b"someone's own file",
            "other/theirs.csv": b"another account's file",
        }
    else:
        assert result["backup"] is None and backups_made == []


def demo_config_raw_dir(export: Path) -> dict[str, bytes]:
    """Maya's raw files in an export, by name."""
    with zipfile.ZipFile(export) as export_zip:
        return {
            name.removeprefix("raw/maya/"): export_zip.read(name)
            for name in export_zip.namelist()
            if name.startswith("raw/maya/") and not name.endswith("/")
        }


def test_an_export_of_format_1_imports(client, demo_export, fresh, tmp_path):
    """Its config lists the accounts' directories (in the zip), which are left
    out."""
    config_data = toml.loads(member(demo_export, archive.CONFIG_NAME).decode())
    accounts = json.loads(member(demo_export, archive.MANIFEST_NAME))["accounts"]
    config_data["ingestion"]["directories"] = {
        account: [f"raw/{account}"] for account in accounts
    }
    old = rewritten(
        demo_export,
        tmp_path.joinpath("old.zip"),
        {
            "manifest.json": manifest_with(demo_export, format=1),
            "config.toml": toml.dumps(config_data),
        },
    )
    response = import_zip(client, old, dry_run=False)
    assert response.status_code == 200, response.text
    assert "directories" not in fresh.config_file.read_text()
    assert config_module.config().ingestion.rules
    assert sorted(path.name for path in fresh.host.raw_dir.iterdir()) == accounts


def analysis(client) -> tuple[dict, dict]:
    """The dashboard and every analysis row."""
    dashboard = client.post("/api/dashboard", json={})
    rows = client.get("/api/analysis/rows")
    for response in (dashboard, rows):
        assert response.status_code == 200, response.text
    return dashboard.json(), rows.json()


@pytest.mark.parametrize("new_transaction", [False, True])
def test_the_dashboard_shows_an_import_at_once(
    client, demo_export, monkeypatch, new_transaction
):
    exported = analysis(client)
    # Another instance, with another tag kind (and a new transaction, or the
    # same ingested files)
    new_file = host().raw_dir.joinpath("cash", "new.csv")
    if new_transaction:
        new_file.write_text(
            "source,date,amount,description\nwallet,2025-06-14,-3.20,Bun\n"
        )
        assert client.post("/api/ingest").json()["new_ids"]
    response = client.patch("/api/tags", json={"tag": "food", "kind": "one-off"})
    assert response.status_code == 200, response.text
    other = analysis(client)
    assert other != exported

    def replace_ingested_keeping_times(new_dir: Path, temp: Path, undo: list):
        """As if copied over with their times kept: without a new transaction,
        the ingested files can't be told apart from the replaced ones."""
        ingested_dir = host().ingested_dir
        times = {file.name: file.stat().st_mtime_ns for file in ingested_dir.iterdir()}
        replace_ingested(new_dir, temp, undo)
        for file in ingested_dir.iterdir():
            os.utime(file, ns=(times[file.name], times[file.name]))

    replace_ingested = archive._replace_ingested
    monkeypatch.setattr(archive, "_replace_ingested", replace_ingested_keeping_times)
    # Into the same raw folder, over its files
    response = import_zip(client, demo_export, dry_run=False)
    assert response.status_code == 200, response.text

    assert analysis(client) == exported


@pytest.mark.parametrize("failing", ["store", "raw folder"])
def test_failed_import_puts_everything_back(
    client, demo_export, fresh, monkeypatch, failing
):
    raw_dir = fresh.host.raw_dir
    for name in ("maya/older.csv", "other/theirs.csv", "stray.csv"):
        raw_dir.joinpath(name).parent.mkdir(parents=True, exist_ok=True)
        raw_dir.joinpath(name).write_text(name)
    status = client.get("/api/status").json()
    before = fresh.files()

    if failing == "store":

        def fail(store, new_file):
            raise OSError("disk full")

        monkeypatch.setattr(archive, "_replace_store", fail)
    else:
        # Halfway through swapping the raw folder's contents
        rename = Path.rename

        def fail_on_the_second_account(self, target):
            if self.parent.name == "new" and self.name == "eitan":
                raise OSError("disk full")
            return rename(self, target)

        monkeypatch.setattr(Path, "rename", fail_on_the_second_account)
    response = import_zip(client, demo_export, dry_run=False)
    assert response.status_code == 422
    assert "disk full" in response.json()["detail"]
    after = fresh.files()
    # Only the backup was added
    added = [path for path in after if path not in before]
    assert len(added) == 1 and "backups/" in added[0]
    assert {path: after[path] for path in before} == before
    assert sorted(path.name for path in raw_dir.iterdir()) == [
        "maya",
        "other",
        "stray.csv",
    ]
    assert config_module.config() == fresh.config
    assert client.get("/api/status").json() == status


def test_raw_files_that_cant_be_put_back_are_kept(
    client, demo_export, fresh, monkeypatch
):
    """When undoing fails too, the raw folder's previous contents stay in the
    import's hidden folder of it, never deleted."""
    raw_dir = fresh.host.raw_dir
    raw_dir.joinpath("cash").mkdir(parents=True)
    raw_dir.joinpath("cash", "only-here.csv").write_text("irreplaceable")
    rename = Path.rename

    def fail_moving_eitan_in_and_cash_back(self, target):
        if self.parent.name == "new" and self.name == "eitan":
            raise OSError("disk full")
        if self.parent == raw_dir and Path(target).parent.name == "new":
            raise OSError("busy")
        return rename(self, target)

    monkeypatch.setattr(Path, "rename", fail_moving_eitan_in_and_cash_back)
    response = import_zip(client, demo_export, dry_run=False)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "disk full" in detail
    kept = list(raw_dir.glob(".import-*/previous/cash/only-here.csv"))
    assert [file.read_text() for file in kept] == ["irreplaceable"]


def test_export_without_data(client, tmp_path):
    Instance(tmp_path.joinpath("empty"))
    response = client.get("/api/export")
    assert response.status_code == 200, response.text
    with zipfile.ZipFile(io.BytesIO(response.content)) as export_zip:
        assert not [name for name in export_zip.namelist() if name.startswith("raw")]


def test_import_a_store_of_another_page_size(
    client, demo_config, demo_export, fresh, tmp_path
):
    store_file = tmp_path.joinpath("finalyze.db")
    store_file.write_bytes(member(demo_export, archive.STORE_NAME))
    connection = sqlite3.connect(store_file)
    connection.execute("PRAGMA page_size = 8192")
    connection.execute("VACUUM")
    connection.close()
    other = rewritten(
        demo_export,
        tmp_path.joinpath("other.zip"),
        {"finalyze.db": store_file.read_bytes()},
    )
    response = import_zip(client, other, dry_run=False)
    assert response.status_code == 200, response.text
    assert store_content(fresh.host.db_file) == store_content(store_file)


def test_no_endpoint_takes_a_filesystem_path():
    """Every file moves through downloads and uploads: the server only reads and
    writes its own folders."""
    schema = create_app(static_dir=None).openapi()
    request_schemas = {
        content["schema"]["$ref"].split("/")[-1]
        for operations in schema["paths"].values()
        for operation in operations.values()
        for content in operation.get("requestBody", {}).get("content", {}).values()
        if "$ref" in content.get("schema", {})
    }
    request_fields = [
        name
        for schema_name in request_schemas
        for name in schema["components"]["schemas"][schema_name].get("properties", {})
    ]
    assert len(request_fields) > 50
    for name in request_fields + [
        parameter["name"]
        for operations in schema["paths"].values()
        for operation in operations.values()
        for parameter in operation.get("parameters", [])
    ]:
        assert not (
            name in ("path", "file_path", "folder", "directory", "directories")
            or name.endswith(("_dir", "_path", "_file"))
        ), name


def test_undoing_goes_on_after_a_step_fails():
    undone = []

    def fail():
        raise OSError("can't")

    with pytest.raises(ValueError) as raised:
        with archive.undone_on_failure() as undo:
            undo.extend([lambda: undone.append(1), fail, lambda: undone.append(3)])
            raise ValueError("failed")
    assert undone == [3, 1]
    assert "can't" in raised.value.__notes__[0]


def test_an_import_waits_for_requests_and_requests_for_it():
    gate = RequestGate()
    events = []
    request_started = threading.Event()
    release_request = threading.Event()

    def request():
        with gate.shared():
            events.append("request")
            request_started.set()
            release_request.wait(5)
            events.append("request done")

    def importing():
        with gate.exclusive():
            events.append("import")

    request_thread = threading.Thread(target=request)
    request_thread.start()
    request_started.wait(5)
    import_thread = threading.Thread(target=importing)
    import_thread.start()
    import_thread.join(0.2)
    assert events == ["request"]
    release_request.set()
    request_thread.join(5)
    import_thread.join(5)
    assert events == ["request", "request done", "import"]
