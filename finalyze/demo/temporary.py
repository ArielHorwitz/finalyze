"""The temporary directory `finalyze --demo` serves from: removed when the server
stops (normally, on SIGINT, SIGTERM or SIGHUP), and if a run was killed
before it could remove its own, by the next run.

A run holds a lock on a file in its directory while it lasts, so the next run
tells a dead run's directory from a live one's (the lock goes with the
process, however it ends)."""

import contextlib
import fcntl
import os
import re
import shutil
import signal
import sys
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

PREFIX = "finalyze-demo-"
NAME_PATTERN = re.compile(f"^{PREFIX}[a-z0-9_]{{8}}$")
"""The names `tempfile` gives the directories made with the prefix."""
LOCK_FILE = ".serving"
UNLOCKED_GRACE_SECONDS = 60
"""How old a directory without a lock file must be to be a dead run's: a new
run creates its lock file right after its directory."""
STOP_SIGNALS = (signal.SIGTERM, signal.SIGHUP)
"""Signals that kill the process by default, without the cleanup (SIGINT raises
KeyboardInterrupt, which cleans up). uvicorn handles SIGTERM itself, then raises
it again after its shutdown, to the handler that was there before it."""


@contextlib.contextmanager
def directory() -> Iterator[Path]:
    """A new temporary directory, locked until the context exits, then removed.
    Earlier runs' leftover directories are removed first."""
    remove_stale()
    previous_handlers = {
        stop_signal: signal.signal(stop_signal, exit_on_signal)
        for stop_signal in STOP_SIGNALS
    }
    try:
        with tempfile.TemporaryDirectory(prefix=PREFIX) as temp_dir:
            path = Path(temp_dir)
            with path.joinpath(LOCK_FILE).open("w") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                yield path
    finally:
        for stop_signal, handler in previous_handlers.items():
            signal.signal(stop_signal, handler)


def exit_on_signal(signal_number: int, _frame):
    """Exit as the signal would have, but through Python, so the cleanup runs."""
    sys.exit(128 + signal_number)


def remove_stale():
    """Remove the directories of earlier runs that are no longer running: only
    the current user's, named like this module names them."""
    for path in Path(tempfile.gettempdir()).glob(f"{PREFIX}*"):
        if is_stale(path):
            shutil.rmtree(path, ignore_errors=True)


def is_stale(path: Path) -> bool:
    if not NAME_PATTERN.match(path.name) or path.is_symlink() or not path.is_dir():
        return False
    if path.stat().st_uid != os.getuid():
        return False
    lock_path = path.joinpath(LOCK_FILE)
    if not lock_path.is_file():
        return time.time() - path.stat().st_mtime > UNLOCKED_GRACE_SECONDS
    with lock_path.open() as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True
