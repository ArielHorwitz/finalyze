"""Settings: the config file (edited through the API), the dashboard's defaults and
label colors in the store, and how the server runs."""

import json
import os
import re
import time
from pathlib import Path

import pytest
import toml
from fastapi.testclient import TestClient

from finalyze import config as config_module
from finalyze import keymap as finalyze_keymap
from finalyze.api import create_app, stale_web_app

LOCAL_URL = "http://127.0.0.1:8765"


@pytest.fixture
def client(demo_config):
    return TestClient(create_app(static_dir=None), base_url=LOCAL_URL)


@pytest.fixture
def config_dir(demo_config, tmp_path, monkeypatch):
    """A config file of the demo's config (with a comment), as the loaded one."""
    config_dir = tmp_path.joinpath("config")
    config_dir.mkdir()
    data = demo_config.model_dump(mode="json", exclude_defaults=True)
    config_dir.joinpath("config.toml").write_text("# A comment\n" + toml.dumps(data))
    monkeypatch.setattr(config_module, "LOADED_CONFIG_DIR", config_dir)
    return config_dir


def test_host_settings_are_read_only(client, demo_config):
    response = client.get("/api/settings/host")
    assert response.status_code == 200, response.text
    root = config_module.LOADED_CONFIG_DIR
    assert response.json() == {
        "file": str(root.joinpath("host.toml")),
        "file_exists": True,
        "data_dir": str(root.joinpath("data")),
        "raw_dir": str(root.joinpath("raw")),
        "backups_dir": str(root.joinpath("data", "backups")),
        "host": None,
        "port": None,
    }
    # Neither the config nor any other endpoint takes them
    assert "general" not in client.get("/api/settings/config").json()["config"]
    assert client.put("/api/settings/host", json={}).status_code == 405


def test_get_and_set_config(client, config_dir):
    response = client.get("/api/settings/config")
    assert response.status_code == 200, response.text
    settings = response.json()
    assert settings["file"] == str(config_dir.joinpath("config.toml"))
    new_config = settings["config"]
    assert "analysis" not in new_config
    new_config["ingestion"]["filters"]["start_date"] = "2024-01-01"
    response = client.put("/api/settings/config", json=new_config)
    assert response.status_code == 200, response.text
    assert response.json()["config"] == new_config
    # Written without comments, the previous file kept, and used from now on
    written = config_dir.joinpath("config.toml").read_text()
    assert "# A comment" not in written
    assert toml.loads(written)["ingestion"]["filters"]["start_date"] == "2024-01-01"
    assert "# A comment" in config_dir.joinpath("config.toml.bak").read_text()
    assert sorted(file.name for file in config_dir.iterdir()) == [
        "config.toml",
        "config.toml.bak",
    ]
    assert str(config_module.config().ingestion.filters.start_date) == "2024-01-01"
    dashboard = client.post("/api/dashboard", json={}).json()
    # Ingestion settings apply to the next ingest
    assert dashboard["available_months"][0] == "2023-06"
    assert client.post("/api/ingest").status_code == 200
    dashboard = client.post("/api/dashboard", json={}).json()
    assert dashboard["available_months"][0] == "2024-01"


def test_set_config_with_ingestion_rules(client, config_dir):
    new_config = client.get("/api/settings/config").json()["config"]
    # Unset fields (null) aren't written
    rule = {
        "match": {"source": "card", "raw_description": "^BIT$", "start_date": None},
        "action": "transfer",
        "to_source": "bit",
    }
    new_config["ingestion"]["rules"] = [rule]
    response = client.put("/api/settings/config", json=new_config)
    assert response.status_code == 200, response.text
    returned = response.json()["config"]["ingestion"]["rules"]
    assert (
        returned
        == client.get("/api/settings/config").json()["config"]["ingestion"]["rules"]
    )
    assert returned[0]["to_source"] == "bit"
    written = toml.loads(config_dir.joinpath("config.toml").read_text())
    assert written["ingestion"]["rules"] == [
        {
            "match": {"source": "card", "raw_description": "^BIT$"},
            "action": "transfer",
            "to_source": "bit",
        }
    ]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"ingestion": {"card_transactions": "keep"}}, "card_transactions"),
        (
            {"ingestion": {"rules": [{"match": {}, "action": "transfer"}]}},
            "to_source",
        ),
        ({"ingestion": {"typo": 1}}, "Extra inputs"),
        ({"general": {"data_dir": "/elsewhere"}}, "no longer in the config"),
        ({"ingestion": {"directories": {}}}, "no longer in the config"),
    ],
)
def test_invalid_config_is_not_written(client, config_dir, change, message):
    before = config_dir.joinpath("config.toml").read_text()
    new_config = client.get("/api/settings/config").json()["config"]
    for section, values in change.items():
        new_config[section] = new_config.get(section, {}) | values
    response = client.put("/api/settings/config", json=new_config)
    assert response.status_code == 422
    assert message in response.text
    assert config_dir.joinpath("config.toml").read_text() == before


def test_dashboard_defaults(client):
    # The demo's
    assert client.get("/api/settings/dashboard").json() == {
        "net_by_tag": True,
        "breakdown_months": 3,
        "complete_months_only": True,
        "rolling_months": 6,
    }
    defaults = {
        "net_by_tag": False,
        "breakdown_months": 6,
        "complete_months_only": False,
        "rolling_months": 12,
    }
    assert client.put("/api/settings/dashboard", json=defaults).json() == defaults
    assert client.get("/api/settings/dashboard").json() == defaults
    response = client.put("/api/settings/dashboard", json={"breakdown_months": 0})
    assert response.status_code == 422


def test_display_settings(client):
    assert client.get("/api/settings/display").json() == {"date_format": "dd/mm/yyyy"}
    settings = {"date_format": "yyyy-mm-dd"}
    assert client.put("/api/settings/display", json=settings).json() == settings
    assert client.get("/api/settings/display").json() == settings
    response = client.put("/api/settings/display", json={"date_format": "dd.mm.yy"})
    assert response.status_code == 422
    # Missing settings are reset to their defaults
    assert client.put("/api/settings/display", json={}).json() == {
        "date_format": "dd/mm/yyyy"
    }


def test_label_colors(client):
    colors = client.get("/api/settings/colors").json()
    # The demo's
    assert colors["colors"]["net worth"] == "#fff"
    assert colors["labels"][:2] == ["net worth", "available"]
    assert "maya/card" in colors["labels"]
    response = client.put(
        "/api/settings/colors", json={"label": "maya/card", "color": "red"}
    )
    assert response.json()["colors"]["maya/card"] == "#f00"
    response = client.put(
        "/api/settings/colors", json={"label": "net worth", "color": None}
    )
    assert "net worth" not in response.json()["colors"]
    for invalid in ({"label": "food", "color": "#fff"}, {"label": "x", "color": "?"}):
        assert client.put("/api/settings/colors", json=invalid).status_code == 422


def test_store_colors_are_the_only_colors(client):
    """Accounts' and tags' own colors win over label colors of the same name."""
    client.patch("/api/accounts/eitan", json={"color": "#123"})
    with_seeded = client.post("/api/dashboard", json={}).json()["colors"]
    assert with_seeded["eitan"] == "#123"
    assert with_seeded["maya/card"] == "#7fb2f0"
    assert with_seeded["food"] == "#199e70"


def test_server_info(client):
    assert client.get("/api/server").json() == {
        "demo": False,
        "stale_web_app": None,
    }
    demo = TestClient(create_app(static_dir=None, demo=True), base_url=LOCAL_URL)
    assert demo.get("/api/server").json()["demo"] is True


def test_stale_web_app(tmp_path):
    web_dir = tmp_path.joinpath("web")
    web_dir.joinpath("src", "lib").mkdir(parents=True)
    static_dir = tmp_path.joinpath("static")
    static_dir.mkdir()
    source = web_dir.joinpath("src", "lib", "api.ts")
    source.write_text("")
    test_file = web_dir.joinpath("src", "lib", "api.test.ts")
    built = static_dir.joinpath("index.html")
    built.write_text("")
    now = time.time()
    os.utime(source, (now - 60, now - 60))
    assert stale_web_app(static_dir, web_dir) is None
    # Tests are not part of the bundle
    test_file.write_text("")
    assert stale_web_app(static_dir, web_dir) is None
    os.utime(source, (now + 60, now + 60))
    message = stale_web_app(static_dir, web_dir)
    assert "src/lib/api.ts changed" in message
    assert "scripts/build-web.sh" in message
    # Nothing to compare: not built, or no sources (an installed package)
    assert stale_web_app(tmp_path.joinpath("missing"), web_dir) is None
    assert stale_web_app(static_dir, tmp_path.joinpath("missing")) is None


VIM = {"left": "h", "down": "j", "up": "k", "right": "l"}


def test_keymap(client):
    assert client.get("/api/settings/keymap").json() == {
        "directions": VIM,
        "overrides": {},
    }
    keymap = {
        "directions": {"left": "j", "down": "k", "up": "l", "right": ";"},
        "overrides": {"tags.rename": ["N"], "go.tags": ["q"], "list.down": []},
    }
    assert client.put("/api/settings/keymap", json=keymap).json() == keymap
    assert client.get("/api/settings/keymap").json() == keymap
    # Missing fields are reset
    assert client.put("/api/settings/keymap", json={}).json() == {
        "directions": VIM,
        "overrides": {},
    }
    assert client.put(
        "/api/settings/keymap", json={"directions": {"right": ";"}}
    ).json()["directions"] == {**VIM, "right": ";"}
    for invalid in (
        {"directions": "wasd"},
        {"directions": {"left": "Tab"}},
        {"directions": {"left": "H"}},
        {"directions": {"left": ""}},
        {"overrides": {"tags.rename": ["n", "n"]}},
        {"overrides": {"tags.rename": [""]}},
        {"overrides": {"tags.rename": ["g x y"]}},
        {"overrides": {"Not an action": ["n"]}},
    ):
        assert client.put("/api/settings/keymap", json=invalid).status_code == 422


def test_keymap_of_a_directions_preset(client, service):
    """Before the four keys, the directions were a preset's name."""
    service.store.set_setting("keymap", {"directions": "jkl;", "overrides": {}})
    assert client.get("/api/settings/keymap").json()["directions"] == {
        "left": "j",
        "down": "k",
        "up": "l",
        "right": ";",
    }
    service.store.set_setting("keymap", {"directions": "hjkl"})
    assert client.get("/api/settings/keymap").json()["directions"] == VIM


def test_keymap_clashes_are_refused(client):
    clashing = {"overrides": {"tags.rename": ["e"]}}
    response = client.put("/api/settings/keymap", json=clashing)
    assert response.status_code == 422
    problem = 'e is both "Edit color and kind" and "Rename" (Tags)'
    assert response.json()["problems"] == [problem]
    assert problem in response.json()["detail"]
    # Nothing was saved
    assert client.get("/api/settings/keymap").json()["overrides"] == {}
    # A direction's key too
    two_directions = {"directions": {**VIM, "up": "j"}}
    response = client.put("/api/settings/keymap", json=two_directions)
    assert response.status_code == 422
    assert "j is both down and up" in response.json()["problems"]


KEYMAP_CASES = Path(__file__).parent.joinpath("fixtures", "keymap_problems.json")


@pytest.mark.parametrize(
    "case", json.loads(KEYMAP_CASES.read_text()), ids=lambda case: case["name"]
)
def test_keymap_problems_as_the_web_app(case):
    """The web app's unit tests check its problems against the same cases."""
    # Not through the model, which refuses some of them already (e.g. Tab)
    directions = {**VIM, **case["keymap"].get("directions", {})}
    overrides = case["keymap"].get("overrides", {})
    assert finalyze_keymap.problems(directions, overrides) == case["problems"]


def test_keymap_registry_is_shared_with_the_web_app():
    """The server and the web app read one registry: finalyze/keymap.json."""
    web_lib = Path(__file__).parents[1].joinpath("web", "src", "lib")
    imported = re.search(
        r'^import registry from "([^"]+)";$',
        web_lib.joinpath("keymap.ts").read_text(),
        re.MULTILINE,
    )
    assert imported
    assert web_lib.joinpath(imported[1]).resolve() == finalyze_keymap.KEYMAP_FILE
    for case_import in re.findall(
        r'^import cases from "([^"]+)";$',
        web_lib.joinpath("keymap.test.ts").read_text(),
        re.MULTILINE,
    ):
        assert web_lib.joinpath(case_import).resolve() == KEYMAP_CASES.resolve()
    ids = [action["id"] for action in finalyze_keymap.actions()]
    assert len(set(ids)) == len(ids)
    scopes = {scope["id"] for scope in finalyze_keymap.registry()["scopes"]}
    for context in finalyze_keymap.registry()["contexts"]:
        assert set(context["scopes"]) <= scopes
