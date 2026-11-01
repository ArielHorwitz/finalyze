"""The glossary: what every figure, label and concept of the app means.

It is `glossary.toml`, next to this module, served at `GET /api/glossary` for
the web app's tooltips and Glossary page (and for assistants).
"""

import functools
import re
import tomllib
from pathlib import Path

from finalyze.service.models import GlossaryEntry

GLOSSARY_FILE = Path(__file__).with_name("glossary.toml")
KEY_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
LINK_PATTERN = re.compile(r"\]\(#([^)]*)\)")
"""Links to other entries in `computed`, e.g. "[kept](#kept)"."""


class GlossaryError(ValueError):
    pass


def parse(text: str) -> list[GlossaryEntry]:
    """The entries of a glossary file, in order, checked: valid keys, and
    related keys and links that exist."""
    terms = tomllib.loads(text).get("term", {})
    entries = [GlossaryEntry(key=key, **fields) for key, fields in terms.items()]
    keys = {entry.key for entry in entries}
    for entry in entries:
        if not KEY_PATTERN.match(entry.key):
            raise GlossaryError(f"Invalid key: {entry.key!r}")
        linked = LINK_PATTERN.findall(entry.computed)
        unknown = [key for key in [*entry.related, *linked] if key not in keys]
        if unknown:
            raise GlossaryError(f"{entry.key}: unknown keys {', '.join(unknown)}")
        if entry.key in entry.related:
            raise GlossaryError(f"{entry.key}: related to itself")
    return entries


@functools.cache
def entries() -> list[GlossaryEntry]:
    return parse(GLOSSARY_FILE.read_text())
