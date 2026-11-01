"""The glossary: its file, its endpoint, and its use by the web app (a lint of
the Svelte sources: every key used exists, and labeled figures have one)."""

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from finalyze import glossary
from finalyze.api import create_app

LOCAL_URL = "http://127.0.0.1:8765"
WEB_SOURCES = Path(__file__).parent.parent.joinpath("web", "src")
DASHBOARD_SOURCES = [
    *sorted(WEB_SOURCES.joinpath("components", "dashboard").glob("*.svelte")),
    WEB_SOURCES.joinpath("pages", "Dashboard.svelte"),
]
# Keys in the sources: <Term key="kept">, <Panel term="kept" subtitleTerm="...">,
# and `term: "kept"` in scripts. Keys with a {placeholder} are checked by e2e.
USED_KEY_PATTERNS = (
    re.compile(r"<Term\b[^>]*?\skey=\"([^\"{}]+)\""),
    re.compile(r"\s(?:term|subtitleTerm)=\"([^\"{}]+)\""),
    re.compile(r"\bterm: \"([^\"]+)\""),
)


def entry_text(**fields) -> str:
    entry = {
        "title": "Kept",
        "section": "Cards",
        "labels": ["Kept"],
        "short": "Kept.",
        "computed": "Kept.",
        **fields,
    }
    lines = [f"{name} = {value!r}" for name, value in entry.items()]
    return "\n".join(["[term.kept]", *lines]).replace("'", '"')


def test_the_glossary_parses():
    entries = glossary.entries()
    assert len(entries) > 80
    keys = [entry.key for entry in entries]
    assert len(keys) == len(set(keys))
    for entry in entries:
        assert entry.labels and entry.short and entry.computed.strip(), entry.key
        # A tooltip: one or two sentences
        assert len(entry.short) <= 220, entry.key
        assert len(re.findall(r"[.!?] [A-Z]", entry.short)) <= 1, entry.key
        # Public copy, in the app's voice
        assert "—" not in entry.short + entry.computed, entry.key


def test_related_keys_and_links_exist():
    # parse() refuses them, so this checks the file is still checked
    keys = {entry.key for entry in glossary.entries()}
    for entry in glossary.entries():
        assert set(entry.related) <= keys
        assert set(glossary.LINK_PATTERN.findall(entry.computed)) <= keys


@pytest.mark.parametrize(
    "text, message",
    [
        (entry_text(related=["missing"]), "unknown keys missing"),
        (entry_text(computed="See [it](#missing)."), "unknown keys missing"),
        (entry_text(related=["kept"]), "related to itself"),
        (entry_text().replace("[term.kept]", "[term.Kept]"), "Invalid key"),
    ],
)
def test_invalid_glossaries_are_refused(text, message):
    with pytest.raises(glossary.GlossaryError, match=message):
        glossary.parse(text)


def test_unknown_fields_are_refused():
    with pytest.raises(ValidationError):
        glossary.parse(entry_text(tooltip="Kept."))


def test_glossary_endpoint(demo_config):
    client = TestClient(create_app(static_dir=None), base_url=LOCAL_URL)
    response = client.get("/api/glossary")
    assert response.status_code == 200
    entries = response.json()
    assert [entry["key"] for entry in entries] == [
        entry.key for entry in glossary.entries()
    ]
    kept = next(entry for entry in entries if entry["key"] == "kept")
    assert set(kept) == {
        "key",
        "title",
        "section",
        "labels",
        "short",
        "computed",
        "related",
    }


def used_keys(path: Path) -> set[str]:
    text = path.read_text()
    return {key for pattern in USED_KEY_PATTERNS for key in pattern.findall(text)}


def test_every_key_used_in_the_web_app_exists():
    keys = {entry.key for entry in glossary.entries()}
    sources = [*WEB_SOURCES.rglob("*.svelte"), *WEB_SOURCES.rglob("*.ts")]
    used = {path.name: used_keys(path) for path in sources}
    assert sum(map(len, used.values())) > 100
    unknown = {name: sorted(keys_used - keys) for name, keys_used in used.items()}
    assert {name: missing for name, missing in unknown.items() if missing} == {}


def elements(text: str, tag: str) -> list[tuple[str, str]]:
    """The attributes and content of each element of a tag."""
    pattern = re.compile(rf"<{tag}\b([^>]*)>(.*?)</{tag}>", re.DOTALL)
    return pattern.findall(text)


def has_text(content: str) -> bool:
    """Whether markup shows text (not only an expression or nothing)."""
    without_tags = re.sub(r"<[^>]*>|\{[^{}]*\}", "", content)
    return bool(without_tags.strip()) or "{" in content


@pytest.mark.parametrize("path", DASHBOARD_SOURCES, ids=lambda path: path.name)
def test_labeled_figures_of_the_dashboard_have_a_key(path):
    """Panels' titles, table headers, cards' labels, headings and toggles
    explain themselves with a glossary entry."""
    text = path.read_text()
    missing = []
    for panel in re.findall(r"<Panel\b[^>]*>", text, flags=re.DOTALL):
        if " term=" not in panel:
            missing.append(panel)
    for tag in ("th", "h3", "h4"):
        for attributes, content in elements(text, tag):
            if has_text(content) and "<Term" not in content:
                missing.append(f"<{tag}{attributes}>{content}</{tag}>")
    for attributes, content in elements(text, "span"):
        if 'class="label"' in attributes and "<Term" not in content:
            missing.append(content)
    for attributes, content in elements(text, "label"):
        if "<Term" not in content:
            missing.append(f"<label{attributes}>{content.strip()[:60]}…")
    assert missing == []


def test_tooltips_stay_out_of_the_keyboard_flow():
    term = WEB_SOURCES.joinpath("components", "Term.svelte").read_text()
    tooltip = WEB_SOURCES.joinpath("components", "TermTooltip.svelte").read_text()
    assert "tabindex" not in term
    assert 'tabindex="-1"' in tooltip and "<button" not in tooltip


@pytest.mark.parametrize(
    ("key", "sources"),
    [
        # A kind set on a transaction, wherever kinds show
        (
            "kind_set",
            [
                "components/TransactionRow.svelte",
                "components/TransactionTable.svelte",
                "components/BatchFieldsForm.svelte",
                "components/dashboard/DrillPanel.svelte",
            ],
        ),
        # The description, and the raw description in its place without one
        (
            "description",
            [
                "components/TransactionTable.svelte",
                "components/BatchFieldsForm.svelte",
                "components/dashboard/LedgerView.svelte",
                "pages/Orphans.svelte",
                "pages/Ingest.svelte",
            ],
        ),
        # A subtag's own kind or color, as opposed to inherited
        ("own", ["pages/Tags.svelte"]),
        ("inherited", ["pages/Tags.svelte"]),
    ],
)
def test_kinds_are_explained_where_they_show(key, sources):
    for source in sources:
        assert key in used_keys(WEB_SOURCES.joinpath(source)), source


def test_overridden_only_means_annotations_overridden_by_rules():
    """A subtag's own kind and a kind set on a transaction aren't "overridden"."""
    for path in [*WEB_SOURCES.rglob("*.svelte")]:
        for line in path.read_text().splitlines():
            if "overridden" in line.lower():
                assert "kind" not in line.lower(), f"{path.name}: {line.strip()}"
    keys = {entry.key for entry in glossary.entries()}
    assert "kind_override" not in keys
    own = next(entry for entry in glossary.entries() if entry.key == "own")
    assert "overrid" not in own.short + own.computed
