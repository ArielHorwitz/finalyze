"""Tests of the HTTP API (FastAPI TestClient), on the demo data."""

import json
import re
import sqlite3

import pytest
from fastapi.testclient import TestClient

from finalyze import agent_md
from finalyze.api import create_app
from finalyze.config import host


@pytest.fixture(autouse=True)
def without_annotations(unannotated_demo):
    """These tests start from the demo's rules only, without its annotations."""


LOCAL_URL = "http://127.0.0.1:8765"


@pytest.fixture
def client(demo_config):
    return TestClient(create_app(static_dir=None), base_url=LOCAL_URL)


def untagged(client, **params) -> list[dict]:
    response = client.get("/api/transactions", params={"status": "untagged", **params})
    assert response.status_code == 200
    return response.json()["items"]


def groceries(**extra) -> dict:
    return {"parts": [{"tag": "food", "subtag": "groceries"}], **extra}


def test_status(client, demo_config):
    response = client.get("/api/status")
    assert response.status_code == 200
    status = response.json()
    assert status["untagged"] > 0 and status["orphans"] == 0
    assert status["db_path"] == str(host().db_file)


def test_store_uses_wal(client, demo_config):
    client.get("/api/status")
    connection = sqlite3.connect(host().db_file)
    assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


def test_list_transactions_query_parameters(client):
    response = client.get(
        "/api/transactions",
        params={
            "accounts": ["maya", "eitan"],
            "search": "freshway market",
            "sort": "amount",
            "limit": 3,
        },
    )
    assert response.status_code == 200
    page = response.json()
    assert page["total"] > 3 and len(page["items"]) == 3
    assert {item["account"] for item in page["items"]} <= {"maya", "eitan"}
    amounts = [item["amount"] for item in page["items"]]
    assert amounts == sorted(amounts)
    # Nothing with the same description is tagged yet
    assert page["items"][0]["guess"] is None
    body = client.post(
        "/api/transactions/query",
        json={
            "accounts": ["maya", "eitan"],
            "search": "freshway market",
            "sort": "amount",
        },
    )
    assert body.json()["total"] == page["total"]
    assert client.get("/api/transactions", params={"bogus": 1}).status_code == 422
    assert client.get("/api/transactions", params={"status": "x"}).status_code == 422


def test_get_transaction(client):
    item = untagged(client, limit=1)[0]
    assert client.get(f"/api/transactions/{item['id']}").json() == item
    response = client.get("/api/transactions/nope")
    assert response.status_code == 404
    assert response.json() == {"detail": "No such transaction: nope"}


def test_descriptions_keep_their_lines(client):
    item = untagged(client, search="FRESHWAY MARKET", limit=1)[0]
    url = f"/api/transactions/{item['id']}/annotation"
    description = "weekly shopping\nfor the party\n\nask Bob"
    part = {"tag": "food", "subtag": "groceries", "description": "milk\neggs"}
    client.put(
        url, json={"description": description, "parts": [part]}
    ).raise_for_status()
    annotation = client.get(f"/api/transactions/{item['id']}").json()["annotation"]
    assert annotation["description"] == description
    assert annotation["parts"][0]["description"] == "milk\neggs"
    # And the search finds a later line
    found = client.get("/api/transactions", params={"search": "ask Bob"}).json()
    assert [row["id"] for row in found["items"]] == [item["id"]]


def test_set_and_clear_annotation(client):
    item = untagged(client, search="FRESHWAY MARKET", limit=1)[0]
    url = f"/api/transactions/{item['id']}/annotation"
    response = client.put(url, json=groceries(description="weekly"))
    assert response.status_code == 200
    annotated = response.json()
    assert annotated["status"] == "annotated"
    assert annotated["annotation"] == {
        "description": "weekly",
        "parts": [
            {
                "amount": item["amount"],
                "tag": "food",
                "subtag": "groceries",
                "kind": "regular",
                "kind_set": False,
                "description": None,
            }
        ],
        "authors": ["owner"],
        "needs_review": False,
    }
    # The next FRESHWAY MARKET is guessed from this one
    assert untagged(client, search="FRESHWAY MARKET", limit=1)[0]["guess"] == {
        "tag": "food",
        "subtag": "groceries",
        "basis": "same_raw_description",
        "from_transaction_id": item["id"],
    }
    response = client.put(url, json={"parts": [{"tag": "food", "subtag": "typo"}]})
    assert response.status_code == 422
    assert "Unknown tags" in response.json()["detail"]
    response = client.put(url, json={"parts": []})
    assert response.status_code == 422
    cleared = client.delete(url)
    assert cleared.status_code == 200
    assert cleared.json()["status"] == "untagged"


def test_batch_errors_and_success(client):
    first, second = untagged(client, limit=2)
    items = [
        {"id": first["id"], **groceries()},
        {"id": "nope", **groceries()},
        {"id": second["id"], "parts": [{"tag": "food", "subtag": "typo"}]},
    ]
    response = client.post("/api/annotations/batch", json={"items": items})
    assert response.status_code == 422
    body = response.json()
    assert body["detail"] == "2 invalid items, nothing was written"
    assert [(error["index"], error["id"]) for error in body["errors"]] == [
        (1, "nope"),
        (2, second["id"]),
    ]
    assert client.get("/api/status").json()["annotated"] == 0
    response = client.post(
        "/api/annotations/batch",
        json={"items": [items[0], items[2]], "allow_new_tags": True},
    )
    assert response.json() == {
        "dry_run": False,
        "updated_ids": [first["id"], second["id"]],
    }
    cleared = client.post("/api/annotations/clear", json={"ids": [first["id"], "nope"]})
    assert cleared.json() == {
        "cleared_ids": [first["id"]],
        "not_annotated_ids": ["nope"],
    }


def test_bulk_annotate(client):
    request = {
        "query": {"search": "FRESHWAY MARKET"},
        "tag": "food",
        "subtag": "groceries",
    }
    preview = client.post("/api/annotations/bulk", json=request).json()
    assert preview["dry_run"] and preview["affected_ids"]
    assert client.get("/api/status").json()["annotated"] == 0
    stale = client.post(
        "/api/annotations/bulk",
        json=request | {"dry_run": False, "expected_ids": ["other"]},
    )
    assert stale.status_code == 409
    committed = client.post(
        "/api/annotations/bulk",
        json=request | {"dry_run": False, "expected_ids": preview["affected_ids"]},
    ).json()
    assert not committed["dry_run"]
    status = client.get("/api/status").json()
    assert status["annotated"] == len(preview["affected_ids"])


def test_bulk_annotate_optional_fields(client):
    query = {"search": "FRESHWAY MARKET"}
    empty = client.post("/api/annotations/bulk", json={"query": query})
    assert empty.status_code == 422 and "Nothing to change" in empty.text
    described = client.post(
        "/api/annotations/bulk",
        json={"query": query, "description": "ask", "dry_run": False},
    ).json()
    kind_only = client.post(
        "/api/annotations/bulk", json={"query": query, "kind": "one-off"}
    ).json()
    # Description-only annotations leave them untagged, so a kind can't apply
    assert kind_only["affected_ids"] == []
    assert kind_only["skipped_untagged_ids"] == described["affected_ids"]


def test_tags(client):
    tags = {tag["name"]: tag for tag in client.get("/api/tags").json()}
    assert tags["transfer"]["kind"] == "transfer"
    created = client.post("/api/tags", json={"tag": "pets", "subtag": ""})
    assert created.status_code == 201
    assert created.json()["subtags"][0]["name"] == ""
    assert client.post("/api/tags", json={"tag": "pets"}).status_code == 409
    updated = client.patch(
        "/api/tags",
        json={"tag": "pets", "subtag": "", "color": "#123456", "kind": "one-off"},
    )
    assert updated.json()["subtags"][0]["color"] == "#123456"
    assert updated.json()["subtags"][0]["effective_kind"] == "one-off"
    invalid = client.patch("/api/tags", json={"tag": "pets", "kind": "excluded"})
    assert invalid.status_code == 422
    renamed = client.post("/api/tags/rename", json={"tag": "pets", "new_name": "pet"})
    assert renamed.json()["name"] == "pet"
    merged = client.post(
        "/api/tags/merge",
        json={"tag": "pet", "subtag": "", "into_tag": "shopping", "into_subtag": "pet"},
    )
    assert "pet" in {subtag["name"] for subtag in merged.json()["subtags"]}
    assert client.post("/api/tags/delete", json={"tag": "pet"}).status_code == 204
    in_use = client.post("/api/tags/delete", json={"tag": "utilities"})
    assert in_use.status_code == 409 and "used by" in in_use.json()["detail"]
    missing = client.patch("/api/tags", json={"tag": "nope", "color": "red"})
    assert missing.status_code == 404


def test_accounts(client):
    accounts = {
        account["name"]: account for account in client.get("/api/accounts").json()
    }
    assert accounts["savings"]["group"] == "savings"
    assert accounts["savings"]["group_role"] == "holding"
    updated = client.patch("/api/accounts/eitan", json={"color": "blue"})
    assert updated.json()["color"] == "#00f"
    moved = client.patch("/api/accounts/eitan", json={"group": "savings"})
    assert moved.json()["group_role"] == "holding"
    assert client.patch("/api/accounts/eitan", json={"group": "x"}).status_code == 404
    assert client.patch("/api/accounts/nope", json={"color": "blue"}).status_code == 404
    bad = client.patch("/api/accounts/eitan", json={"color": "not a color"})
    assert bad.status_code == 422


def test_groups(client):
    groups = client.get("/api/groups").json()
    assert [(group["name"], group["role"]) for group in groups] == [
        ("spending", "spending"),
        ("savings", "holding"),
        ("debt", "holding"),
    ]
    created = client.post("/api/groups", json={"name": "pension", "role": "holding"})
    assert created.status_code == 201 and created.json()["position"] == 3
    assert client.post("/api/groups", json={"name": "pension"}).status_code == 409
    invalid = client.post("/api/groups", json={"name": "x", "role": "external"})
    assert invalid.status_code == 422
    updated = client.patch(
        "/api/groups/pension", json={"new_name": "loans", "position": 0}
    )
    assert (updated.json()["name"], updated.json()["position"]) == ("loans", 0)
    assert client.delete("/api/groups/savings").status_code == 409
    assert client.delete("/api/groups/loans").status_code == 204
    assert client.patch("/api/groups/loans", json={"color": "red"}).status_code == 404


def test_rules(client):
    rules = client.get("/api/rules").json()
    assert rules[0]["position"] == 0 and rules[0]["transactions"] > 0
    filters = {"raw_description": "FRESHWAY MARKET"}
    preview = client.post("/api/rules/preview", json={"filters": filters}).json()
    assert preview["newly_tagged_ids"] == preview["effective_ids"]
    created = client.post(
        "/api/rules",
        json={"filters": filters, "tag": "food", "subtag": "groceries"},
    )
    assert created.status_code == 201
    rule = created.json()
    assert rule["transactions"] == len(preview["effective_ids"])
    assert client.get(f"/api/rules/{rule['id']}").json() == rule
    updated = client.patch(f"/api/rules/{rule['id']}", json={"position": 0})
    assert updated.json()["position"] == 0
    bad_filters = client.post(
        "/api/rules", json={"filters": {"tags": ["x"]}, "tag": "food", "subtag": ""}
    )
    assert bad_filters.status_code == 422
    unknown_key = client.post(
        "/api/rules/preview", json={"filters": {"descriptin": "x"}}
    )
    assert unknown_key.status_code == 422
    ids = [rule["id"] for rule in client.get("/api/rules").json()]
    reordered = client.post("/api/rules/reorder", json={"rule_ids": ids[::-1]})
    assert [rule["id"] for rule in reordered.json()] == ids[::-1]
    assert client.delete(f"/api/rules/{rule['id']}").status_code == 204
    assert client.delete(f"/api/rules/{rule['id']}").status_code == 404


def test_orphans_and_overridden(client):
    assert client.get("/api/orphans").json() == []
    assert client.get("/api/overridden").json() == []
    response = client.post("/api/orphans/delete", json={"ids": ["nope"]})
    assert response.status_code == 422


def test_ingest(client):
    summary = client.post("/api/ingest").json()
    assert summary["new_ids"] == [] and summary["vanished_ids"] == []
    assert set(summary["accounts"]) == {"cash", "eitan", "loan", "maya", "savings"}


def test_ingest_dry_run_and_preview(client, demo_config):
    wallet_dir = host().raw_dir.joinpath("cash")
    copy = wallet_dir.joinpath("wallet-copy.csv")
    copy.write_bytes(wallet_dir.joinpath("wallet.csv").read_bytes())
    dry_run = client.get("/api/ingest/dry-run").json()
    # The raw folder's folders, by name
    assert [account["name"] for account in dry_run["accounts"]] == [
        "cash",
        "eitan",
        "loan",
        "maya",
        "savings",
    ]
    assert dry_run["raw_dir"] == str(host().raw_dir) and dry_run["raw_dir_exists"]
    assert dry_run["accounts"][0]["folder"] == str(wallet_dir)
    wallet_files = dry_run["accounts"][0]["files"]
    assert [file["name"] for file in wallet_files] == ["wallet-copy.csv", "wallet.csv"]
    assert [file["parser"] for file in wallet_files] == ["csv", "csv"]
    overlaps = [
        problem for problem in dry_run["problems"] if problem["account"] == "cash"
    ]
    assert {problem["kind"] for problem in overlaps} == {"overlap"}
    assert all(
        overlap["shared_rows"] > 0
        for overlap in dry_run["overlaps"]
        if overlap["account"] == "cash"
    )
    # The copy's rows are new transactions (told apart by their occurrence)
    preview = client.get("/api/ingest/preview").json()
    assert preview["vanished"] == [] and preview["new_orphans"] == []
    rows = wallet_files[0]["rows"]
    assert len(preview["new"]) == rows
    cash = preview["accounts"]["cash"]
    assert cash["after"] - cash["before"] == rows
    assert client.get("/api/status").json()["transactions"] == preview["unchanged"]
    copy.write_text("not,a,valid\nfile,at,all\n")
    failing = client.get("/api/ingest/preview")
    assert failing.status_code == 422
    assert "Ingest would fail" in failing.json()["detail"]


def test_analysis(client):
    rows = client.get(
        "/api/analysis/rows",
        params={"tags": ["utilities"], "kinds": ["regular", "one-off"]},
    ).json()
    assert rows["total"] == len(rows["rows"]) > 0
    assert {row["tag"] for row in rows["rows"]} == {"utilities"}
    assert {(row["kind"], row["group"]) for row in rows["rows"]} == {
        ("regular", "spending")
    }
    savings = client.get(
        "/api/analysis/rows", params={"groups": ["savings"], "kinds": ["regular"]}
    ).json()
    assert {row["account"] for row in savings["rows"]} == {"savings"}
    aggregate = client.get(
        "/api/analysis/aggregate", params={"group_by": ["month", "tag"]}
    ).json()
    assert aggregate["group_by"] == ["month", "tag"]
    assert set(aggregate["rows"][0]) == {"month", "tag", "amount", "count"}
    bad = client.get("/api/analysis/aggregate", params={"group_by": ["nope"]})
    assert bad.status_code == 422


def test_openapi_documents_the_api(client):
    schema = client.get("/openapi.json").json()
    assert "Concepts" in schema["info"]["description"]
    paths = schema["paths"]
    assert "/api/annotations/bulk" in paths and "/api/rules/preview" in paths
    bulk = paths["/api/annotations/bulk"]["post"]
    assert "Previews by default" in bulk["description"]
    assert client.get("/docs").status_code == 200


def test_agent_md_uses_the_request_base_url(client):
    response = client.get("/agent.md")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/markdown; charset=utf-8"
    assert f"### Status\n\n`GET {LOCAL_URL}/api/status`" in response.text
    assert "{base}" not in response.text
    assert client.get("/agent.txt").status_code == 404
    other_host = client.get("/agent.md", headers={"host": "localhost:9000"}).text
    assert "curl http://localhost:9000/api/status" in other_host
    assert LOCAL_URL not in other_host


def test_agent_md_documents_every_api_route():
    paths = create_app(static_dir=None).openapi()["paths"]
    routes = {
        (method.upper(), path)
        for path, operations in paths.items()
        if path.startswith("/api/")
        for method in operations
    }
    assert len(routes) > 30
    documented = [
        (operation.method, operation.path) for operation in agent_md.OPERATIONS
    ]
    assert len(documented) == len(set(documented))
    assert set(documented) == routes


def test_agent_md_is_structured():
    text = agent_md.render(LOCAL_URL)
    headings = re.findall(r"^(#+) (.*)$", text, flags=re.MULTILINE)
    assert headings[0] == ("#", "finalyze API")
    assert ("##", "Concepts") in headings and headings[-1] == ("##", "Errors")
    operations = [title for level, title in headings if level == "###"]
    assert set(operation.title for operation in agent_md.OPERATIONS) <= set(operations)
    # Every operation has its full URL and a bash example, and fences are balanced
    assert text.count(f"`GET {LOCAL_URL}/api/") > 15
    assert text.count("```bash\n") == len(agent_md.OPERATIONS)
    assert text.count("```") == 2 * len(agent_md.OPERATIONS)


def test_agent_md_examples_are_valid_json():
    text = agent_md.render(LOCAL_URL)
    bodies = re.findall(r" -d '(.*)'$", text, flags=re.MULTILINE)
    assert len(bodies) > 20
    for body in bodies:
        json.loads(body)


def test_static_frontend_hook(demo_config, tmp_path):
    static_dir = tmp_path.joinpath("dist")
    static_dir.mkdir()
    static_dir.joinpath("index.html").write_text("<html>app</html>")
    client = TestClient(create_app(static_dir=static_dir), base_url=LOCAL_URL)
    assert client.get("/").text == "<html>app</html>"
    assert client.get("/agent.md").text.startswith("# finalyze API")
    assert client.get("/api/status").status_code == 200
    missing = TestClient(
        create_app(static_dir=tmp_path.joinpath("missing")), base_url=LOCAL_URL
    )
    response = missing.get("/")
    assert response.status_code == 404 and "scripts/build-web.sh" in response.text


def test_only_local_hosts_are_accepted(client):
    """Blocks DNS rebinding, e.g. a website resolving its name to 127.0.0.1."""
    assert (
        client.get("/api/status", headers={"host": "localhost:8765"}).status_code == 200
    )
    response = client.get("/api/status", headers={"host": "evil.example:8765"})
    assert response.status_code == 400


def test_author_review_and_description_only(client):
    first, second = untagged(client, limit=2)
    response = client.put(
        f"/api/transactions/{first['id']}/annotation",
        json=groceries(author="claude", needs_review=True, description="unsure"),
    )
    assert response.json()["annotation"]["authors"] == ["claude"]
    described = client.put(
        f"/api/transactions/{second['id']}/annotation",
        json={"description": "only a note"},
    ).json()
    assert described["status"] == "untagged" and described["annotation"]["parts"] == []
    flagged = client.get("/api/transactions", params={"needs_review": True}).json()
    assert [item["id"] for item in flagged["items"]] == [first["id"]]
    by_author = client.get("/api/transactions", params={"authors": ["owner"]}).json()
    assert [item["id"] for item in by_author["items"]] == [second["id"]]
    assert client.get("/api/status").json()["needs_review"] == 1
    reviewed = client.post("/api/annotations/review", json={"ids": [first["id"]]})
    assert reviewed.json() == {"needs_review": False, "updated_ids": [first["id"]]}
    assert client.get("/api/status").json()["needs_review"] == 0
    # Reviewing isn't content: the authors are kept, and it takes no author
    annotation = client.get(f"/api/transactions/{first['id']}").json()["annotation"]
    assert annotation["authors"] == ["claude"]
    with_author = client.post(
        "/api/annotations/review", json={"ids": [first["id"]], "author": "owner"}
    )
    assert with_author.status_code == 422
    missing = client.post("/api/annotations/review", json={"ids": ["nope"]})
    assert missing.status_code == 404


def test_patch_annotation(client):
    item = untagged(client, search="FRESHWAY MARKET", limit=1)[0]
    url = f"/api/transactions/{item['id']}/annotation"
    client.put(url, json=groceries(description="weekly", needs_review=True))
    # Only the given fields change
    response = client.patch(
        url, json={"description": "weekly shop", "author": "claude"}
    )
    assert response.status_code == 200
    annotation = response.json()["annotation"]
    assert annotation["description"] == "weekly shop"
    assert (
        annotation["needs_review"] and annotation["parts"][0]["subtag"] == "groceries"
    )
    assert annotation["authors"] == ["owner", "claude"]
    # Changing only the review flag adds no author, here or in a full replace
    annotation = client.patch(
        url, json={"needs_review": False, "author": "ada"}
    ).json()["annotation"]
    assert not annotation["needs_review"]
    assert annotation["authors"] == ["owner", "claude"]
    full = groceries(description="weekly shop", needs_review=True, author="ada")
    annotation = client.put(url, json=full).json()["annotation"]
    assert annotation["needs_review"] and annotation["authors"] == ["owner", "claude"]
    # Explicit null clears
    annotation = client.patch(url, json={"description": None}).json()["annotation"]
    assert annotation["description"] is None and annotation["needs_review"]
    annotation = client.patch(url, json={"kind": "one-off"}).json()["annotation"]
    assert annotation["parts"][0]["kind_set"]
    annotation = client.patch(url, json={"kind": None}).json()["annotation"]
    assert not annotation["parts"][0]["kind_set"]
    # Invalid patches
    for body in (
        {},
        {"needs_review": None},
        {"parts": [{"tag": "food", "subtag": "groceries", "amount": 1}]},
        {"parts": groceries()["parts"], "kind": "one-off"},
    ):
        assert client.patch(url, json=body).status_code == 422, body
    missing = client.patch("/api/transactions/nope/annotation", json={"kind": None})
    assert missing.status_code == 404


def test_patch_annotations_in_a_batch(client):
    first, second = untagged(client, search="FRESHWAY MARKET", limit=2)
    client.put(f"/api/transactions/{first['id']}/annotation", json=groceries())
    items = [
        {"id": first["id"], "needs_review": True},
        {"id": second["id"], "kind": "one-off"},
    ]
    response = client.patch("/api/annotations/batch", json={"items": items})
    assert response.status_code == 422
    body = response.json()
    assert [(error["index"], error["id"]) for error in body["errors"]] == [
        (1, second["id"])
    ]
    items[1] = {"id": second["id"], "description": "ask"}
    response = client.patch(
        "/api/annotations/batch", json={"items": items, "author": "claude"}
    )
    assert response.json() == {
        "dry_run": False,
        "updated_ids": [first["id"], second["id"]],
    }
    annotated = client.get(f"/api/transactions/{first['id']}").json()["annotation"]
    assert annotated["needs_review"] and annotated["parts"][0]["subtag"] == "groceries"
    # Only the review flag changed, so claude isn't an author of the first
    assert annotated["authors"] == ["owner"]
    by_claude = client.get("/api/transactions", params={"authors": ["claude"]}).json()
    assert {item["id"] for item in by_claude["items"]} == {second["id"]}


def test_kind_set_on_transactions(client):
    first, second = untagged(client, search="FRESHWAY MARKET", limit=2)
    one_off = groceries()
    one_off["parts"][0]["kind"] = "one-off"
    part = client.put(
        f"/api/transactions/{first['id']}/annotation", json=one_off
    ).json()["annotation"]["parts"][0]
    assert (part["kind"], part["kind_set"]) == ("one-off", True)
    client.put(f"/api/transactions/{second['id']}/annotation", json=groceries())
    response = client.post(
        "/api/annotations/kind", json={"ids": [second["id"]], "kind": "saving"}
    )
    assert response.json() == {"kind": "saving", "updated_ids": [second["id"]]}
    kinds_set = client.get("/api/transactions", params={"kind_set": True}).json()
    assert {item["id"] for item in kinds_set["items"]} == {first["id"], second["id"]}
    savings = client.get("/api/transactions", params={"kinds": ["saving"]}).json()
    assert second["id"] in {item["id"] for item in savings["items"]}
    rows = client.get(
        "/api/analysis/rows", params={"kind_set": True, "all_groups": True}
    ).json()["rows"]
    assert {(row["id"], row["kind"]) for row in rows} == {
        (first["id"], "one-off"),
        (second["id"], "saving"),
    }
    inherited = client.get(
        "/api/analysis/rows", params={"kind_set": False, "all_groups": True}
    ).json()["rows"]
    assert inherited
    assert not {first["id"], second["id"]} & {row["id"] for row in inherited}
    cleared = client.post(
        "/api/annotations/kind", json={"ids": [first["id"], second["id"]], "kind": None}
    )
    assert cleared.status_code == 200
    assert (
        client.get("/api/transactions", params={"kind_set": True}).json()["total"] == 0
    )
    failed = client.post(
        "/api/annotations/kind", json={"ids": [untagged(client)[0]["id"]], "kind": None}
    )
    assert failed.status_code == 422
    assert "untagged" in failed.json()["errors"][0]["message"]


def test_writes_from_other_sites_are_refused(client):
    """A page anywhere could post forms to the API (they aren't preflighted):
    browsers tell where a request comes from, and only this app's own pages may
    write."""
    upload = {"files": [("files", ("bakery.csv", "source,date,amount,description\n"))]}
    for headers in (
        {"origin": "https://evil.example"},
        {"origin": "null"},
        {"sec-fetch-site": "cross-site"},
        {"origin": "http://127.0.0.1:8765", "sec-fetch-site": "cross-site"},
    ):
        for method, path, body in (
            ("POST", "/api/raw/cash", upload),
            ("POST", "/api/import", {"files": {"file": ("x.zip", b"x")}}),
            ("POST", "/api/ingest", {}),
            ("DELETE", "/api/exports/x.zip", {}),
        ):
            response = client.request(method, path, headers=headers, **body)
            assert response.status_code == 403, (headers, path, response.text)
    # Reads are fine, and so are the app's own pages, and clients that aren't
    # browsers (no headers)
    response = client.get("/api/status", headers={"origin": "https://evil.example"})
    assert response.status_code == 200
    for headers in (
        {"origin": "http://127.0.0.1:8765", "sec-fetch-site": "same-origin"},
        {"origin": "http://localhost:5173", "sec-fetch-site": "same-site"},
        {},
    ):
        assert client.post("/api/ingest", headers=headers).status_code == 200
