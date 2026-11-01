"""Tests of the cache of post-processed data: every kind of write shows at once."""

import contextlib
import os
import sqlite3

import pytest
from fastapi.testclient import TestClient

import finalyze.service as service_module
from finalyze.api import create_app
from finalyze.config import host
from finalyze.service import data_changed


@pytest.fixture
def client(demo_config):
    return TestClient(create_app(static_dir=None), base_url="http://127.0.0.1:8765")


@pytest.fixture
def post_process_calls(monkeypatch) -> list[int]:
    """The number of times post-processing ran, as one item per call."""
    calls = []
    original = service_module.post_process

    def counted(*positional, **named):
        calls.append(1)
        return original(*positional, **named)

    monkeypatch.setattr(service_module, "post_process", counted)
    return calls


def analysis(client) -> tuple[dict, dict, dict]:
    """The dashboard, its ledger rows and every analysis row."""
    dashboard = client.post("/api/dashboard", json={})
    rows = client.post(
        "/api/dashboard/rows", json={"options": {}, "selection": {"all_groups": True}}
    )
    analysis_rows = client.get("/api/analysis/rows")
    for response in (dashboard, rows, analysis_rows):
        assert response.status_code == 200, response.text
    return dashboard.json(), rows.json(), analysis_rows.json()


def assert_shows_at_once(client, before):
    """The analysis changed, and is the same as computed afresh."""
    after = analysis(client)
    assert after != before
    data_changed()
    assert analysis(client) == after


def first_id(client, status: str, **params) -> str:
    params = {"status": status, "limit": 1, **params}
    return client.get("/api/transactions", params=params).json()["items"][0]["id"]


def ok(response):
    assert response.status_code < 300, response.text
    return response


GROCERIES = {"parts": [{"tag": "food", "subtag": "groceries"}]}


def set_annotation(client):
    url = f"/api/transactions/{first_id(client, 'untagged')}/annotation"
    ok(client.put(url, json=GROCERIES))


def patch_annotation(client):
    url = f"/api/transactions/{first_id(client, 'untagged')}/annotation"
    ok(client.patch(url, json=GROCERIES))


def clear_annotation(client):
    ok(client.delete(f"/api/transactions/{first_id(client, 'annotated')}/annotation"))


def set_annotations(client):
    items = [{"id": first_id(client, "untagged"), **GROCERIES}]
    ok(client.post("/api/annotations/batch", json={"items": items}))


def patch_annotations(client):
    items = [{"id": first_id(client, "untagged"), **GROCERIES}]
    ok(client.patch("/api/annotations/batch", json={"items": items}))


def clear_annotations(client):
    ids = [first_id(client, "annotated")]
    ok(client.post("/api/annotations/clear", json={"ids": ids}))


def bulk_annotate(client):
    request = {"query": {"status": "untagged"}, **GROCERIES["parts"][0]}
    preview = ok(client.post("/api/annotations/bulk", json=request)).json()
    expected = {"dry_run": False, "expected_ids": preview["affected_ids"]}
    ok(client.post("/api/annotations/bulk", json=request | expected))


def set_kind(client):
    ids = [first_id(client, "annotated")]
    ok(client.post("/api/annotations/kind", json={"ids": ids, "kind": "transfer"}))


def set_tag_kind(client):
    ok(client.patch("/api/tags", json={"tag": "food", "kind": "one-off"}))


def set_subtag_kind(client):
    subtag = {"tag": "food", "subtag": "groceries", "kind": "one-off"}
    ok(client.patch("/api/tags", json=subtag))


def rename_tag(client):
    ok(client.post("/api/tags/rename", json={"tag": "food", "new_name": "meals"}))


def rename_subtag(client):
    subtag = {"tag": "food", "subtag": "groceries", "new_name": "market"}
    ok(client.post("/api/tags/rename", json=subtag))


def merge_tag(client):
    ok(client.post("/api/tags/merge", json={"tag": "food", "into_tag": "kids"}))


def move_account(client):
    ok(client.patch("/api/accounts/eitan", json={"group": "savings"}))


def set_group_role(client):
    ok(client.patch("/api/groups/savings", json={"role": "spending"}))


def rename_group(client):
    ok(client.patch("/api/groups/savings", json={"new_name": "reserve"}))


def create_rule(client):
    filters = {"raw_description": "FRESHWAY MARKET"}
    rule = {"filters": filters, "tag": "kids", "subtag": "babysitting"}
    ok(client.post("/api/rules", json=rule))


def update_rule(client):
    rule = client.get("/api/rules").json()[0]
    change = {"tag": "food", "subtag": "other", "allow_new_tags": True}
    ok(client.patch(f"/api/rules/{rule['id']}", json=change))


def delete_rule(client):
    rule = client.get("/api/rules").json()[0]
    ok(client.delete(f"/api/rules/{rule['id']}"))


def ingest(client):
    raw_file = host().raw_dir.joinpath("cash").joinpath("new.csv")
    raw_file.write_text("source,date,amount,description\nwallet,2025-06-14,-3.20,Bun\n")
    assert ok(client.post("/api/ingest")).json()["new_ids"]


def set_dashboard_defaults(client):
    defaults = ok(client.get("/api/settings/dashboard")).json()
    changed = defaults | {"net_by_tag": True, "rolling_months": 3}
    ok(client.put("/api/settings/dashboard", json=changed))


WRITES = [
    set_annotation,
    patch_annotation,
    clear_annotation,
    set_annotations,
    patch_annotations,
    clear_annotations,
    bulk_annotate,
    set_kind,
    set_tag_kind,
    set_subtag_kind,
    rename_tag,
    rename_subtag,
    merge_tag,
    move_account,
    set_group_role,
    rename_group,
    create_rule,
    update_rule,
    delete_rule,
    ingest,
    set_dashboard_defaults,
]


@pytest.mark.parametrize("write", WRITES, ids=[write.__name__ for write in WRITES])
def test_writes_show_at_once(client, write):
    before = analysis(client)
    write(client)
    assert_shows_at_once(client, before)


def test_reads_use_the_cache(client, post_process_calls):
    analysis(client)
    for url in ("/api/status", "/api/tags", "/api/accounts", "/api/groups"):
        ok(client.get(url))
    ok(client.post("/api/dashboard", json={"period": "12m", "net_by_tag": True}))
    ok(client.get("/api/balance"))
    assert len(post_process_calls) == 1
    # Other accounts are cached apart
    accounts = {"accounts": ["maya", "eitan"]}
    ok(client.post("/api/dashboard", json=accounts))
    ok(client.post("/api/dashboard", json=accounts))
    assert len(post_process_calls) == 2
    # A write of anything (here, a review flag) post-processes again
    ids = [first_id(client, "annotated")]
    ok(client.post("/api/annotations/review", json={"ids": ids}))
    analysis(client)
    assert len(post_process_calls) == 3


def test_reordering_rules_shows_at_once(client):
    # The same transactions as an earlier rule: moved first, it tags them
    rule = {
        "filters": {"raw_description": "^Interest$"},
        "tag": "income",
        "subtag": "interest",
        "allow_new_tags": True,
    }
    ok(client.post("/api/rules", json=rule))
    before = analysis(client)
    ids = [rule["id"] for rule in client.get("/api/rules").json()]
    ok(client.post("/api/rules/reorder", json={"rule_ids": ids[::-1]}))
    assert_shows_at_once(client, before)


def test_data_changed_after_replacing_the_store(client, demo_config, tmp_path):
    """E.g. an import replaces the store's file, not through `Store`."""
    before = analysis(client)
    db_file = host().db_file
    replacement = tmp_path.joinpath("replacement.db")
    with contextlib.closing(sqlite3.connect(db_file)) as connection:
        connection.execute("VACUUM INTO ?", (str(replacement),))
    with contextlib.closing(sqlite3.connect(replacement)) as connection, connection:
        connection.execute("UPDATE tags SET kind = 'one-off' WHERE name = 'food'")
    os.replace(replacement, db_file)
    data_changed()
    assert_shows_at_once(client, before)


def test_data_changed_after_replacing_the_ingested_files(client):
    """Even files of the same size and time, which the ingested cache can't tell
    apart."""
    before = analysis(client)
    ingested = host().ingested_dir.joinpath("cash.csv")
    stat = ingested.stat()
    text = ingested.read_text()
    opening_balance = ",320.0,Opening balance"
    assert opening_balance in text
    ingested.write_text(text.replace(opening_balance, ",330.0,Opening balance"))
    os.utime(ingested, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert ingested.stat().st_size == stat.st_size
    data_changed()
    assert_shows_at_once(client, before)
