"""Uploading raw files into an account's folder, through the API: checked by
the parsers first, replacing only explicitly, and only inside the raw folder."""

import pytest
from fastapi.testclient import TestClient

from finalyze import api
from finalyze.api import create_app
from finalyze.config import host
from finalyze.service import raw_upload

LOCAL_URL = "http://127.0.0.1:8765"
HEADER = "source,date,amount,description\n"
BAKERY = HEADER + "wallet,2025-06-14,-3.20,Invented bakery\n"


@pytest.fixture
def client(demo_config):
    return TestClient(create_app(static_dir=None), base_url=LOCAL_URL)


def upload(client, account: str, files: dict[str, str | bytes], **fields):
    return client.post(
        f"/api/raw/{account}",
        files=[("files", (name, content)) for name, content in files.items()],
        data={name: str(value).lower() for name, value in fields.items()},
    )


def raw_state() -> dict[str, bytes]:
    raw_dir = host().raw_dir
    return {
        str(file.relative_to(raw_dir)): file.read_bytes()
        for file in sorted(raw_dir.rglob("*"))
        if file.is_file()
    }


def test_upload_checks_then_writes(client):
    before = raw_state()
    response = upload(client, "cash", {"bakery.csv": BAKERY, "notes.txt": "not one"})
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["dry_run"] and not result["new_account"]
    assert result["folder"] == str(host().raw_dir.joinpath("cash"))
    bakery, notes = result["files"]
    assert bakery["accepted"] and bakery["check"]["parser"] == "csv"
    assert (
        bakery["path"]
        == bakery["check"]["path"]
        == str(host().raw_dir.joinpath("cash", "bakery.csv"))
    )
    assert (bakery["check"]["rows"], bakery["check"]["start_date"]) == (1, "2025-06-14")
    assert bakery["replaces"] is None
    assert not notes["accepted"]
    assert notes["check"]["error"] == "No parser could read the file"
    assert raw_state() == before
    # Not with a file no parser reads
    response = upload(
        client, "cash", {"bakery.csv": BAKERY, "notes.txt": "x"}, dry_run=False
    )
    assert response.status_code == 422
    assert "No parser reads: notes.txt" in response.json()["detail"]
    assert raw_state() == before

    response = upload(client, "cash", {"bakery.csv": BAKERY}, dry_run=False)
    assert response.status_code == 200, response.text
    assert not response.json()["dry_run"]
    assert raw_state() == before | {"cash/bakery.csv": BAKERY.encode()}
    # No temporary files left, and the next ingest reads it
    assert not list(host().data_dir.glob(".upload-*"))
    assert not list(host().raw_dir.rglob(".*"))
    preview = client.get("/api/ingest/preview").json()
    assert [row["raw_description"] for row in preview["new"]] == ["Invented bakery"]


def test_replacing_a_file_shows_both_and_needs_confirming(client):
    wallet = host().raw_dir.joinpath("cash", "wallet.csv")
    old = wallet.read_bytes()
    response = upload(client, "cash", {"wallet.csv": BAKERY})
    replacing = response.json()["files"][0]
    assert replacing["accepted"]
    # The coverage of the file it replaces, to compare
    assert replacing["replaces"]["rows"] > replacing["check"]["rows"] == 1
    assert replacing["replaces"]["start_date"] < replacing["check"]["start_date"]
    response = upload(client, "cash", {"wallet.csv": BAKERY}, dry_run=False)
    assert response.status_code == 409
    assert "Replace them explicitly" in response.json()["detail"]
    assert wallet.read_bytes() == old
    response = upload(
        client, "cash", {"wallet.csv": BAKERY}, dry_run=False, replace=True
    )
    assert response.status_code == 200, response.text
    assert wallet.read_text() == BAKERY


def test_upload_into_a_new_account(client):
    response = upload(client, "pot", {"pot.csv": BAKERY})
    assert response.json()["new_account"]
    assert not host().raw_dir.joinpath("pot").exists()
    response = upload(client, "pot", {"pot.csv": BAKERY}, dry_run=False)
    assert response.status_code == 200, response.text
    assert host().raw_dir.joinpath("pot", "pot.csv").read_text() == BAKERY
    accounts = client.get("/api/ingest/dry-run").json()["accounts"]
    assert "pot" in [account["name"] for account in accounts]


@pytest.mark.parametrize(
    ("account", "name", "message"),
    [
        ("cash", "../outside.csv", "Not a valid file name"),
        ("cash", "..", "Not a valid file name"),
        ("cash", "sub/inside.csv", "Not a valid file name"),
        ("cash", "..\\outside.csv", "Not a valid file name"),
        ("cash", "/tmp/absolute.csv", "Not a valid file name"),
        ("cash", ".hidden.csv", "Not a valid file name"),
        ("%2E%2E", "x.csv", "Not a valid account"),
        (".hidden", "x.csv", "Not a valid account"),
        ("a\\b", "x.csv", "Not a valid account"),
    ],
)
def test_names_that_lead_elsewhere_are_refused(
    client, tmp_path, account, name, message
):
    before = raw_state()
    for dry_run in (True, False):
        response = upload(client, account, {name: BAKERY}, dry_run=dry_run)
        assert response.status_code == 422, response.text
        assert message in response.json()["detail"]
    assert raw_state() == before
    assert not list(tmp_path.rglob("outside.csv"))
    assert not list(tmp_path.rglob("absolute.csv"))


def test_account_names_with_a_slash_never_reach_the_upload(client):
    # (a slash, even encoded, is another route)
    for account in ("a/b", "..%2Foutside", ".."):
        response = upload(client, account, {"x.csv": BAKERY}, dry_run=False)
        assert response.status_code in (404, 405), (account, response.status_code)
    assert not list(host().data_dir.parent.rglob("x.csv"))


@pytest.mark.parametrize(
    "name", ["", ".", "..", "a/b", "a\\b", ".hidden", "new\nline", "nul\0", "x" * 256]
)
def test_plain_names(name):
    assert not raw_upload.plain_name(name)
    assert raw_upload.plain_name("card-2025 03 (copy).xls")
    assert raw_upload.plain_name("תנועות.csv")


def test_the_same_name_twice_is_refused(client):
    response = client.post(
        "/api/raw/cash",
        files=[("files", ("a.csv", BAKERY)), ("files", ("a.csv", BAKERY))],
    )
    assert response.status_code == 422
    assert "Files of the same name: a.csv" in response.json()["detail"]


def test_sizes_are_limited(client, monkeypatch):
    before = raw_state()
    monkeypatch.setattr(raw_upload, "MAX_FILE_BYTES", len(BAKERY) - 1)
    response = upload(client, "cash", {"bakery.csv": BAKERY}, dry_run=False)
    assert response.status_code == 422
    assert "bakery.csv is too big" in response.json()["detail"]
    monkeypatch.setattr(api, "UPLOAD_LIMITS", (("/api/raw/", 100),))
    response = upload(client, "cash", {"bakery.csv": BAKERY * 10}, dry_run=False)
    assert response.status_code == 413
    assert raw_state() == before
    assert not list(host().data_dir.glob(".upload-*"))


@pytest.mark.parametrize("account", ["cash", "pot"])
def test_an_upload_that_fails_partway_writes_nothing(client, monkeypatch, account):
    """Every file or none: a file that fails to go into place (e.g. the disk is
    full) undoes the others, including the one it replaced, and a new account's
    folder."""
    if account == "cash":
        host().raw_dir.joinpath("cash", "bakery.csv").write_text(BAKERY)
    before = raw_state()
    replace_file = type(host().raw_dir).replace
    disk_full = [True]

    def fail_on_the_second(self, target):
        if disk_full[0] and self.parent.name == "new" and self.name == "second.csv":
            raise OSError("No space left on device")
        return replace_file(self, target)

    monkeypatch.setattr(type(host().raw_dir), "replace", fail_on_the_second)
    files = {"bakery.csv": BAKERY.replace("3.20", "4.50"), "second.csv": BAKERY}
    response = upload(client, account, files, dry_run=False, replace=True)
    assert response.status_code == 422
    assert "No space left" in response.json()["detail"]
    assert raw_state() == before
    assert host().raw_dir.joinpath(account).is_dir() == (account == "cash")
    assert not list(host().raw_dir.rglob(".*"))
    # And once it can, both are written
    disk_full[0] = False
    response = upload(client, account, files, dry_run=False, replace=True)
    assert response.status_code == 200, response.text
    state = raw_state()
    assert state[f"{account}/bakery.csv"] == files["bakery.csv"].encode()
    assert state[f"{account}/second.csv"] == BAKERY.encode()
