"""The demo (`finalyze --demo`): what it exercises, that it's deterministic,
and that serving it never touches the user's own config and data."""

import datetime
import fcntl
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

import pytest

from finalyze import config as config_module
from finalyze import demo, run
from finalyze.demo import household as story
from finalyze.demo import taxonomy, temporary
from finalyze.service import models
from tests.helpers import DEMO_END_DATE


def test_the_demo_exercises_the_app(service):
    status = service.status()
    assert status.untagged == story.UNTAGGED_COUNT
    assert (status.needs_review, status.orphans, status.overridden) == (5, 1, 1)
    assert status.accounts == ["cash", "eitan", "loan", "maya", "savings"]
    groups = {group.name: group.role for group in service.list_groups()}
    assert groups == {"spending": "spending", "savings": "holding", "debt": "holding"}
    # Every parser reads its files, and the ingestion rules have work to do
    dry_run = service.ingest_dry_run()
    assert dry_run.problems == []
    files = [file for account in dry_run.accounts for file in account.files]
    assert {file.parser for file in files} == {
        "leumi checking",
        "leumi card",
        "bit",
        "csv",
    }
    assert any(file.kept_rows < file.rows for file in files)  # dropped
    assert sum(file.transfer_rows for file in files) > 0
    transferred = {
        (item.account, item.source)
        for item in service.list_transactions(
            models.TransactionListRequest(tags=["transfer"], limit=None)
        ).items
    }
    assert {("maya", "bit"), ("cash", "wallet"), ("loan", "car")} <= transferred
    # The annotations
    annotations = service.store.list_annotations()
    authors = {author for annotation in annotations for author in annotation.authors}
    assert authors == {"owner", "penny"}
    described = [annotation.description for annotation in annotations]
    assert sum(text is not None for text in described) > 100
    assert sum("\n" in text for text in described if text) >= 4
    assert sum(len(annotation.parts) > 1 for annotation in annotations) >= 5
    kinds_set = [
        part for annotation in annotations for part in annotation.parts if part.kind
    ]
    assert len(kinds_set) >= 3
    flagged = [annotation for annotation in annotations if annotation.needs_review]
    assert {annotation.authors for annotation in flagged} == {("penny",)}
    # Hebrew and English raw descriptions
    raw = {
        item.raw_description
        for item in service.list_transactions(
            models.TransactionListRequest(limit=None)
        ).items
    }
    assert any("א" <= text[0] <= "ת" for text in raw)
    assert any(text.isascii() for text in raw)
    # Every tag is the taxonomy's, with its kind
    tags = {tag.name: tag.kind for tag in service.list_tags()}
    assert tags == {tag: kind for tag, (kind, _color) in taxonomy.TAGS.items()}


def test_the_demo_is_deterministic(tmp_path):
    first = tmp_path.joinpath("first")
    second = tmp_path.joinpath("second")
    demo.build(first, end_date=DEMO_END_DATE)
    demo.build(second, end_date=DEMO_END_DATE)
    first_files = sorted(path.relative_to(first) for path in first.rglob("raw/**/*"))
    assert first_files == sorted(
        path.relative_to(second) for path in second.rglob("raw/**/*")
    )
    for relative in first_files:
        if first.joinpath(relative).is_file():
            assert (
                first.joinpath(relative).read_bytes()
                == second.joinpath(relative).read_bytes()
            )


@pytest.mark.parametrize(
    "end_date",
    [
        datetime.date(2025, 6, 15),
        datetime.date(2026, 11, 1),
        datetime.date(2028, 2, 29),
        datetime.date(2027, 12, 31),
    ],
)
def test_the_demo_builds_for_any_end_date(tmp_path, end_date):
    household = demo.build(tmp_path.joinpath("demo"), end_date=end_date)
    assert max(entry.date for entry in household.entries) <= end_date
    first_month = min(entry.date for entry in household.entries)
    assert (end_date - first_month).days >= 730
    assert_untagged_in_the_current_month(household)
    # The config loaded before is kept
    assert config_module.LOADED_CONFIG_DIR != tmp_path.joinpath("demo")


def test_the_story_holds_for_every_end_date():
    start = datetime.date(2026, 1, 1)
    every_ninth_day = [
        start + datetime.timedelta(days=days) for days in range(0, 366, 9)
    ]
    # (the untagged rows' fallback: the first of each month)
    first_days = [datetime.date(2026, month, 1) for month in range(1, 13)]
    for end_date in every_ninth_day + first_days:
        household = story.generate(end_date)
        assert_untagged_in_the_current_month(household)
        assert household.described_only[0] in household.unannotated
        assert household.orphan.date < end_date


def assert_untagged_in_the_current_month(household: story.Household):
    """Some untagged rows for Annotate, and none in the complete months."""
    untagged = household.unannotated
    assert story.UNTAGGED_MINIMUM <= len(untagged) <= story.UNTAGGED_COUNT
    month_start = household.end_date.replace(day=1)
    assert all(household.entries[index].date >= month_start for index in untagged)


def test_demo_refuses_other_configs(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "argv", ["finalyze", "-C", str(tmp_path), "--demo"])
    with pytest.raises(SystemExit, match="no -C"):
        run.main()
    assert not any(tmp_path.iterdir())


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def api(port: int, path: str, data=None, method: str = "GET"):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/api{path}",
        data=None if data is None else json.dumps(data).encode(),
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


@pytest.mark.parametrize("stop_signal", [signal.SIGINT, signal.SIGTERM])
def test_serve_demo_never_touches_the_users_files(tmp_path, stop_signal):
    """`finalyze --demo` with a home of its own: nothing is written there (not even
    the default config), and its temporary directory is gone once it stops."""
    home = tmp_path.joinpath("home")
    temp = tmp_path.joinpath("tmp")
    home.mkdir()
    temp.mkdir()
    port = free_port()
    environment = os.environ | {
        "HOME": str(home),
        "TMPDIR": str(temp),
        "XDG_CONFIG_HOME": str(home.joinpath(".config")),
        "XDG_DATA_HOME": str(home.joinpath(".local", "share")),
    }
    command = "from finalyze.run import main; main()"
    server = subprocess.Popen(
        [sys.executable, "-c", command, "--demo", "--port", str(port)],
        env=environment,
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                info = api(port, "/server")
                break
            except OSError:
                if server.poll() is not None or time.monotonic() > deadline:
                    raise AssertionError(server.stdout.read()) from None
                time.sleep(0.2)
        assert info["demo"] is True
        status = api(port, "/status")
        (demo_dir,) = temp.iterdir()
        assert Path(status["db_path"]).is_relative_to(demo_dir)
        # Edits work, on the temporary copy
        untagged = api(port, "/transactions?status=untagged&limit=1")["items"][0]
        api(
            port,
            f"/transactions/{untagged['id']}/annotation",
            {"parts": [{"tag": "food", "subtag": "groceries"}]},
            method="PUT",
        )
        assert api(port, "/status")["untagged"] == status["untagged"] - 1
    finally:
        server.send_signal(stop_signal)
        output, _ = server.communicate(timeout=30)
    assert "DEMO: serving an invented household" in output
    assert list(home.iterdir()) == []
    assert list(temp.iterdir()) == []


def test_serve_demo_removes_dead_runs_directories(monkeypatch, tmp_path):
    """Only the directories of runs that are gone: by their lock, or without one,
    by their age. Nothing else in the temporary directory."""
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    old = time.time() - 2 * temporary.UNLOCKED_GRACE_SECONDS

    def run_directory(name: str, lock: bool = True, mtime: float = old) -> Path:
        path = tmp_path.joinpath(name)
        path.mkdir()
        path.joinpath("config.toml").touch()
        if lock:
            path.joinpath(temporary.LOCK_FILE).touch()
        os.utime(path, (mtime, mtime))
        return path

    dead = run_directory("finalyze-demo-dead1234")
    unlocked = run_directory("finalyze-demo-oldrun_1", lock=False)
    starting = run_directory("finalyze-demo-starting", lock=False, mtime=time.time())
    live = run_directory("finalyze-demo-live1234")
    others = [
        run_directory("finalyze-demo-not-a-run"),
        run_directory("finalyze-demo-x"),
        run_directory("finalyze-other1234"),
    ]
    tmp_path.joinpath("finalyze-demo-afile123").touch()
    with live.joinpath(temporary.LOCK_FILE).open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with temporary.directory() as current:
            assert current.parent == tmp_path
            remaining = set(tmp_path.iterdir())
    assert not dead.exists() and not unlocked.exists()
    assert {starting, live, current, *others} <= remaining
    assert tmp_path.joinpath("finalyze-demo-afile123").exists()
    assert not current.exists()
