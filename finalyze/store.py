"""The store of user-authored state, in SQLite.

This module is the single way to read and write user state: annotations (tags,
splits and descriptions) of transactions, tag and account properties, account groups,
rules, colors of other chart labels, and settings (e.g. the dashboard's
defaults).
Transactions themselves are not stored: they are derived from the raw files on
every run.

Amounts are integer minor units (see `finalyze.money`), so that the parts of a
split can be checked to sum exactly to the transaction amount.
"""

import dataclasses
import datetime
import json
import sqlite3
import threading
from collections.abc import Iterable
from pathlib import Path
from typing import Literal, Optional, get_args

from pydantic.color import Color

from finalyze.config import Filters

BASELINE_VERSION = 9
"""The schema version of `BASELINE_SCHEMA`. Stores of older versions predate it,
and can't be opened."""
BASELINE_SCHEMA = """
CREATE TABLE account_groups (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL CHECK (role IN ('spending', 'holding')),
    color TEXT,
    position INTEGER NOT NULL
);
-- The default group of new accounts (see `DEFAULT_GROUP`)
INSERT INTO account_groups (name, role, position) VALUES ('spending', 'spending', 0);
CREATE TABLE accounts (
    name TEXT PRIMARY KEY,
    group_id INTEGER NOT NULL REFERENCES account_groups (id),
    color TEXT
);
CREATE TABLE tags (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    color TEXT,
    kind TEXT NOT NULL DEFAULT 'regular'
        CHECK (kind IN ('regular', 'transfer', 'saving', 'one-off', 'reconciliation'))
);
-- Properties that are NULL inherit from the tag.
CREATE TABLE subtags (
    id INTEGER PRIMARY KEY,
    tag_id INTEGER NOT NULL REFERENCES tags (id),
    name TEXT NOT NULL,
    color TEXT,
    kind TEXT
        CHECK (kind IN ('regular', 'transfer', 'saving', 'one-off', 'reconciliation')),
    UNIQUE (tag_id, name)
);
-- The snapshot columns keep the transaction as it was when annotated, so that
-- orphans (annotations whose transaction no longer exists) can be shown in
-- full. Authors are a JSON array, by first contribution.
CREATE TABLE annotations (
    transaction_id TEXT PRIMARY KEY,
    description TEXT,
    account TEXT NOT NULL,
    source TEXT NOT NULL,
    date TEXT NOT NULL,
    amount INTEGER NOT NULL,
    raw_description TEXT NOT NULL,
    needs_review INTEGER NOT NULL DEFAULT 0 CHECK (needs_review IN (0, 1)),
    authors TEXT NOT NULL DEFAULT '[]'
);
-- A kind that isn't NULL is set on the transaction, over its tag's.
CREATE TABLE parts (
    transaction_id TEXT NOT NULL
        REFERENCES annotations (transaction_id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    subtag_id INTEGER NOT NULL REFERENCES subtags (id),
    description TEXT,
    kind TEXT
        CHECK (kind IN ('regular', 'transfer', 'saving', 'one-off', 'reconciliation')),
    PRIMARY KEY (transaction_id, position)
);
CREATE INDEX parts_subtag ON parts (subtag_id);
-- Ordered by position: the first matching rule wins.
CREATE TABLE rules (
    id INTEGER PRIMARY KEY,
    position INTEGER NOT NULL,
    filters TEXT NOT NULL,
    subtag_id INTEGER NOT NULL REFERENCES subtags (id)
);
-- Values are JSON.
CREATE TABLE settings (
    name TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
-- Colors of chart labels other than accounts and tags (which have their own),
-- e.g. "net worth" or "alice/card".
CREATE TABLE label_colors (
    label TEXT PRIMARY KEY,
    color TEXT NOT NULL
);
"""
SCHEMA_UPGRADES = ()
"""The steps after the baseline, each a script or a function of the connection
(run in a transaction): the first upgrades a store to `BASELINE_VERSION` + 1."""
SCHEMA_VERSION = BASELINE_VERSION + len(SCHEMA_UPGRADES)
DEFAULT_AUTHOR = "owner"
TagKind = Literal["regular", "transfer", "saving", "one-off", "reconciliation"]
TAG_KINDS: tuple[TagKind, ...] = get_args(TagKind)
"""What a tag's transactions are:
- regular: incomes and expenses
- transfer: money moving between own accounts. Never incomes or expenses, but
  part of balances.
- saving: money set aside. Not an expense, but saved.
- one-off: incomes and expenses that would distort averages (e.g. a wedding),
  left out of averages and trends by default.
- reconciliation: rows that re-anchor a balance (e.g. an opening balance, or a
  wallet's counted cash). Part of balances only, and never one-sided transfers.
"""
GroupRole = Literal["spending", "holding"]
GROUP_ROLES: tuple[GroupRole, ...] = get_args(GroupRole)
"""What a group's accounts hold:
- spending: day-to-day money, in cash flows, breakdowns and the available balance
- holding: savings, investments or debt, only in the net worth
"""
DEFAULT_GROUP = "spending"
"""The group of new accounts (created if no group is for spending)."""
UNCHANGED = object()
"""Default for optional properties that should keep their current value."""

_data_version = 0
_data_version_lock = threading.Lock()


def data_version() -> int:
    """A counter of changes to the stores of this process: what is derived from a
    store (e.g. post-processed data) can be cached until it changes.

    Every transaction of a `Store` bumps it as it ends. Changes made otherwise
    (e.g. replacing a store's file) must call `note_data_changed`."""
    return _data_version


def note_data_changed():
    """Bump the data version, as a store's data may have changed."""
    global _data_version
    with _data_version_lock:
        _data_version += 1


class _Connection(sqlite3.Connection):
    """A connection that bumps the data version whenever a transaction ends
    (rolled back too, which only invalidates more than needed)."""

    def commit(self):
        try:
            super().commit()
        finally:
            note_data_changed()

    def __exit__(self, *exc_info):
        try:
            return super().__exit__(*exc_info)
        finally:
            note_data_changed()


class StoreError(Exception):
    """Raised for invalid operations on the store."""


class InvalidSplit(StoreError):
    """Raised when the parts of an annotation don't sum to the transaction amount."""


@dataclasses.dataclass(frozen=True)
class Group:
    """A group of accounts, e.g. the day-to-day accounts, or savings."""

    name: str
    role: GroupRole = "spending"
    color: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class Account:
    name: str
    group: str = DEFAULT_GROUP
    color: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class Tag:
    name: str
    color: Optional[str] = None
    kind: TagKind = "regular"


@dataclasses.dataclass(frozen=True)
class Subtag:
    """A subtag's own properties. A property that is None inherits from the tag."""

    tag: str
    name: str
    color: Optional[str] = None
    kind: Optional[TagKind] = None


@dataclasses.dataclass(frozen=True)
class ResolvedSubtag:
    """A subtag's effective properties, after inheriting from the tag."""

    tag: str
    subtag: str
    color: Optional[str]
    kind: TagKind


@dataclasses.dataclass(frozen=True)
class TransactionSnapshot:
    account: str
    source: str
    date: datetime.date
    amount: int
    """Minor units."""
    raw_description: str
    """The transaction's text in the raw files."""


@dataclasses.dataclass(frozen=True)
class Part:
    amount: int
    """Minor units."""
    tag: str
    subtag: str
    description: Optional[str] = None
    kind: Optional[TagKind] = None
    """Set on this transaction, over its tag's. None inherits the tag's."""


@dataclasses.dataclass(frozen=True)
class Annotation:
    """Tags (as parts), descriptions and review state of a transaction.

    Without parts, an annotation is only a description, and its transaction is
    untagged.
    """

    transaction_id: str
    snapshot: TransactionSnapshot
    parts: tuple[Part, ...]
    description: Optional[str] = None
    """What the transaction really was, written by the owner or an agent."""
    authors: tuple[str, ...] = (DEFAULT_AUTHOR,)
    """Who wrote it (e.g. the owner or an agent's name), by first contribution."""
    needs_review: bool = False
    """Flagged for the owner to review."""

    def contributed_by(self, author: str) -> "Annotation":
        """The annotation with the author added to its authors, if not already."""
        if author in self.authors:
            return self
        return dataclasses.replace(self, authors=(*self.authors, author))

    def validate(self):
        if not self.authors:
            raise StoreError(f"Annotation of {self.transaction_id} has no authors")
        for part in self.parts:
            if part.kind is not None:
                _check_kind(part.kind)
        if not self.parts:
            return
        parts_sum = sum(part.amount for part in self.parts)
        if parts_sum != self.snapshot.amount:
            raise InvalidSplit(
                f"Parts of {self.transaction_id} sum to {parts_sum}, "
                f"expected {self.snapshot.amount} (minor units)"
            )


@dataclasses.dataclass(frozen=True)
class Rule:
    id: int
    filters: Filters
    tag: str
    subtag: str


class Store:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        # A connection is used by one thread at a time, but not always the same
        # one (e.g. by the HTTP API's worker threads).
        self.connection = sqlite3.connect(
            path, check_same_thread=False, factory=_Connection
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        # Readers and a writer don't block each other (e.g. the API's requests)
        self.connection.execute("PRAGMA journal_mode = WAL")
        self._upgrade_schema()

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def _upgrade_schema(self):
        version = self.connection.execute("PRAGMA user_version").fetchone()[0]
        opened_version = version
        if version > SCHEMA_VERSION:
            raise StoreError(
                f"Database {self.path} has schema version {version}, "
                f"newer than supported ({SCHEMA_VERSION})"
            )
        if version == 0:
            # executescript commits first, so the version is set in the script
            self.connection.executescript(
                f"BEGIN; {BASELINE_SCHEMA}; "
                f"PRAGMA user_version = {BASELINE_VERSION}; COMMIT;"
            )
            version = BASELINE_VERSION
        elif version < BASELINE_VERSION:
            raise StoreError(
                f"Database {self.path} has schema version {version}, older than "
                f"the oldest supported ({BASELINE_VERSION}). It can't be upgraded."
            )
        for new_version in range(version + 1, SCHEMA_VERSION + 1):
            upgrade = SCHEMA_UPGRADES[new_version - BASELINE_VERSION - 1]
            if callable(upgrade):
                # Its own statements, then the version bump, in one transaction
                self.connection.execute("BEGIN")
                try:
                    upgrade(self.connection)
                    self.connection.execute(f"PRAGMA user_version = {new_version}")
                    self.connection.execute("COMMIT")
                except BaseException:
                    self.connection.execute("ROLLBACK")
                    raise
                continue
            self.connection.executescript(
                f"BEGIN; {upgrade}; PRAGMA user_version = {new_version}; COMMIT;"
            )
        if opened_version < SCHEMA_VERSION:
            # Scripts commit by themselves, not through the connection
            note_data_changed()

    # Groups

    def list_groups(self) -> list[Group]:
        """In their display order."""
        rows = self.connection.execute(
            "SELECT name, role, color FROM account_groups ORDER BY position, id"
        )
        return [Group(row["name"], row["role"], row["color"]) for row in rows]

    def add_group(
        self,
        name: str,
        role: GroupRole = "spending",
        *,
        color: Optional[str] = None,
        position: Optional[int] = None,
    ):
        """Add a group at a position in the order (default: last)."""
        if not name:
            raise StoreError("A group needs a name")
        _check_role(role)
        with self.connection:
            if self._find_group_id(name) is not None:
                raise StoreError(f"Group {name!r} already exists")
            cursor = self.connection.execute(
                "INSERT INTO account_groups (name, role, color, position) "
                "VALUES (?, ?, ?, ?)",
                (name, role, _normalize_color(color), 0),
            )
            group_ids = self._group_ids()
            group_ids.remove(cursor.lastrowid)
            if position is None:
                position = len(group_ids)
            group_ids.insert(position, cursor.lastrowid)
            self._renumber_groups(group_ids)

    def update_group(
        self, name: str, *, new_name=UNCHANGED, role=UNCHANGED, color=UNCHANGED
    ):
        with self.connection:
            group_id = self._get_group_id(name)
            if new_name is not UNCHANGED and new_name != name:
                if not new_name:
                    raise StoreError("A group needs a name")
                if self._find_group_id(new_name) is not None:
                    raise StoreError(f"Group {new_name!r} already exists")
                self.connection.execute(
                    "UPDATE account_groups SET name = ? WHERE id = ?",
                    (new_name, group_id),
                )
            if role is not UNCHANGED:
                _check_role(role)
                self.connection.execute(
                    "UPDATE account_groups SET role = ? WHERE id = ?", (role, group_id)
                )
            if color is not UNCHANGED:
                self.connection.execute(
                    "UPDATE account_groups SET color = ? WHERE id = ?",
                    (_normalize_color(color), group_id),
                )

    def move_group(self, name: str, position: int):
        """Move a group to a position in the order (0 is first)."""
        with self.connection:
            group_id = self._get_group_id(name)
            group_ids = [other for other in self._group_ids() if other != group_id]
            group_ids.insert(position, group_id)
            self._renumber_groups(group_ids)

    def delete_group(self, name: str):
        """Delete a group without accounts."""
        with self.connection:
            group_id = self._get_group_id(name)
            count = self.connection.execute(
                "SELECT COUNT(*) FROM accounts WHERE group_id = ?", (group_id,)
            ).fetchone()[0]
            if count:
                raise StoreError(
                    f"Group {name!r} has {count} accounts, move them first"
                )
            self.connection.execute(
                "DELETE FROM account_groups WHERE id = ?", (group_id,)
            )
            self._renumber_groups(self._group_ids())

    def _group_ids(self) -> list[int]:
        rows = self.connection.execute(
            "SELECT id FROM account_groups ORDER BY position, id"
        )
        return [row[0] for row in rows]

    def _renumber_groups(self, group_ids: list[int]):
        self.connection.executemany(
            "UPDATE account_groups SET position = ? WHERE id = ?",
            [(position, group_id) for position, group_id in enumerate(group_ids)],
        )

    def _find_group_id(self, name: str) -> Optional[int]:
        row = self.connection.execute(
            "SELECT id FROM account_groups WHERE name = ?", (name,)
        ).fetchone()
        return None if row is None else row[0]

    def _get_group_id(self, name: str) -> int:
        group_id = self._find_group_id(name)
        if group_id is None:
            raise StoreError(f"No such group: {name!r}")
        return group_id

    def _default_group_id(self) -> int:
        """The first group for spending, created if there is none."""
        return _default_group_id(self.connection)

    # Accounts

    def list_accounts(self) -> list[Account]:
        rows = self.connection.execute(
            """
            SELECT accounts.name, account_groups.name AS group_name, accounts.color
            FROM accounts JOIN account_groups ON account_groups.id = accounts.group_id
            ORDER BY accounts.name
            """
        )
        return [Account(row["name"], row["group_name"], row["color"]) for row in rows]

    def ensure_accounts(self, names: Iterable[str]):
        """Add accounts that don't exist yet, to the default group."""
        names = list(names)
        with self.connection:
            known = {
                row[0] for row in self.connection.execute("SELECT name FROM accounts")
            }
            new_names = [name for name in dict.fromkeys(names) if name not in known]
            if not new_names:
                return
            group_id = self._default_group_id()
            self.connection.executemany(
                "INSERT INTO accounts (name, group_id) VALUES (?, ?)",
                [(name, group_id) for name in new_names],
            )

    def update_account(self, name: str, *, group=UNCHANGED, color=UNCHANGED):
        self.ensure_accounts([name])
        with self.connection:
            if group is not UNCHANGED:
                self.connection.execute(
                    "UPDATE accounts SET group_id = ? WHERE name = ?",
                    (self._get_group_id(group), name),
                )
            if color is not UNCHANGED:
                self.connection.execute(
                    "UPDATE accounts SET color = ? WHERE name = ?",
                    (_normalize_color(color), name),
                )

    # Label colors

    def label_colors(self) -> dict[str, str]:
        rows = self.connection.execute(
            "SELECT label, color FROM label_colors ORDER BY label"
        )
        return {row["label"]: row["color"] for row in rows}

    def set_label_color(self, label: str, color: Optional[str]):
        """Set a label's color, or remove it (None)."""
        with self.connection:
            if color is None:
                self.connection.execute(
                    "DELETE FROM label_colors WHERE label = ?", (label,)
                )
            else:
                self.connection.execute(
                    "INSERT INTO label_colors (label, color) VALUES (?, ?) "
                    "ON CONFLICT (label) DO UPDATE SET color = excluded.color",
                    (label, _normalize_color(color)),
                )

    # Settings

    def get_setting(self, name: str, default=None):
        row = self.connection.execute(
            "SELECT value FROM settings WHERE name = ?", (name,)
        ).fetchone()
        return default if row is None else json.loads(row["value"])

    def set_setting(self, name: str, value):
        """Set a setting to a JSON-serializable value."""
        with self.connection:
            self.connection.execute(
                "INSERT INTO settings (name, value) VALUES (?, ?) "
                "ON CONFLICT (name) DO UPDATE SET value = excluded.value",
                (name, json.dumps(value)),
            )

    # Tags

    def list_tags(self) -> list[Tag]:
        rows = self.connection.execute(
            "SELECT name, color, kind FROM tags ORDER BY name"
        )
        return [Tag(row["name"], row["color"], row["kind"]) for row in rows]

    def list_subtags(self) -> list[Subtag]:
        rows = self.connection.execute(
            """
            SELECT tags.name AS tag, subtags.name, subtags.color, subtags.kind
            FROM subtags JOIN tags ON tags.id = subtags.tag_id
            ORDER BY tags.name, subtags.name
            """
        )
        return [
            Subtag(row["tag"], row["name"], row["color"], row["kind"]) for row in rows
        ]

    def list_resolved_subtags(self) -> list[ResolvedSubtag]:
        rows = self.connection.execute(
            """
            SELECT tags.name AS tag, subtags.name AS subtag,
                COALESCE(subtags.color, tags.color) AS color,
                COALESCE(subtags.kind, tags.kind) AS kind
            FROM subtags JOIN tags ON tags.id = subtags.tag_id
            ORDER BY tags.name, subtags.name
            """
        )
        return [
            ResolvedSubtag(row["tag"], row["subtag"], row["color"], row["kind"])
            for row in rows
        ]

    def ensure_tag(self, tag: str) -> int:
        with self.connection:
            return self._ensure_tag(tag)

    def ensure_subtag(self, tag: str, subtag: str) -> int:
        with self.connection:
            return self._ensure_subtag(tag, subtag)

    def update_tag(self, tag: str, *, color=UNCHANGED, kind=UNCHANGED):
        with self.connection:
            tag_id = self._ensure_tag(tag)
            if color is not UNCHANGED:
                self.connection.execute(
                    "UPDATE tags SET color = ? WHERE id = ?",
                    (_normalize_color(color), tag_id),
                )
            if kind is not UNCHANGED:
                _check_kind(kind)
                self.connection.execute(
                    "UPDATE tags SET kind = ? WHERE id = ?", (kind, tag_id)
                )

    def update_subtag(self, tag: str, subtag: str, *, color=UNCHANGED, kind=UNCHANGED):
        """Set the subtag's own properties. None inherits from the tag."""
        with self.connection:
            subtag_id = self._ensure_subtag(tag, subtag)
            if color is not UNCHANGED:
                self.connection.execute(
                    "UPDATE subtags SET color = ? WHERE id = ?",
                    (_normalize_color(color), subtag_id),
                )
            if kind is not UNCHANGED:
                if kind is not None:
                    _check_kind(kind)
                self.connection.execute(
                    "UPDATE subtags SET kind = ? WHERE id = ?", (kind, subtag_id)
                )

    def has_tag(self, tag: str) -> bool:
        return self._find_tag_id(tag) is not None

    def has_subtag(self, tag: str, subtag: str) -> bool:
        return self._find_subtag_id(tag, subtag) is not None

    def subtag_usage(self) -> dict[tuple[str, str], tuple[int, int]]:
        """The number of annotated transactions and of rules, by (tag, subtag)."""
        rows = self.connection.execute(
            """
            SELECT tags.name AS tag, subtags.name AS subtag,
                (SELECT COUNT(DISTINCT transaction_id) FROM parts
                    WHERE parts.subtag_id = subtags.id) AS annotations,
                (SELECT COUNT(*) FROM rules WHERE rules.subtag_id = subtags.id)
                    AS rules
            FROM subtags JOIN tags ON tags.id = subtags.tag_id
            """
        )
        return {
            (row["tag"], row["subtag"]): (row["annotations"], row["rules"])
            for row in rows
        }

    def delete_tag(self, tag: str):
        """Delete a tag and its subtags, if no annotation or rule uses them."""
        with self.connection:
            tag_id = self._get_tag_id(tag)
            subtag_ids = [
                row[0]
                for row in self.connection.execute(
                    "SELECT id FROM subtags WHERE tag_id = ?", (tag_id,)
                )
            ]
            for subtag_id in subtag_ids:
                self._check_unused(subtag_id, f"Tag {tag!r}")
            self.connection.execute("DELETE FROM subtags WHERE tag_id = ?", (tag_id,))
            self.connection.execute("DELETE FROM tags WHERE id = ?", (tag_id,))

    def delete_subtag(self, tag: str, subtag: str):
        """Delete a subtag, if no annotation or rule uses it."""
        with self.connection:
            subtag_id = self._get_subtag_id(tag, subtag)
            self._check_unused(subtag_id, f"Subtag {tag!r}/{subtag!r}")
            self.connection.execute("DELETE FROM subtags WHERE id = ?", (subtag_id,))

    def _check_unused(self, subtag_id: int, description: str):
        for table in ("parts", "rules"):
            count = self.connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE subtag_id = ?", (subtag_id,)
            ).fetchone()[0]
            if count:
                raise StoreError(f"{description} is used by {count} {table}")

    def rename_tag(self, tag: str, new_name: str):
        with self.connection:
            tag_id = self._get_tag_id(tag)
            if self._find_tag_id(new_name) is not None:
                raise StoreError(f"Tag {new_name!r} already exists, merge instead")
            self.connection.execute(
                "UPDATE tags SET name = ? WHERE id = ?", (new_name, tag_id)
            )

    def rename_subtag(self, tag: str, subtag: str, new_name: str):
        with self.connection:
            subtag_id = self._get_subtag_id(tag, subtag)
            if self._find_subtag_id(tag, new_name) is not None:
                raise StoreError(
                    f"Subtag {tag!r}/{new_name!r} already exists, merge instead"
                )
            self.connection.execute(
                "UPDATE subtags SET name = ? WHERE id = ?", (new_name, subtag_id)
            )

    def merge_tag(self, tag: str, into_tag: str):
        """Move all subtags of a tag into another tag, then delete the tag.

        Subtags that exist in both are merged (see `merge_subtag`). The merged
        tag's properties are dropped.
        """
        if tag == into_tag:
            raise StoreError(f"Cannot merge tag {tag!r} into itself")
        with self.connection:
            tag_id = self._get_tag_id(tag)
            into_tag_id = self._get_tag_id(into_tag)
            subtags = self.connection.execute(
                "SELECT id, name FROM subtags WHERE tag_id = ?", (tag_id,)
            ).fetchall()
            for subtag_id, subtag_name in subtags:
                into_subtag_id = self._find_subtag_id(into_tag, subtag_name)
                if into_subtag_id is None:
                    self.connection.execute(
                        "UPDATE subtags SET tag_id = ? WHERE id = ?",
                        (into_tag_id, subtag_id),
                    )
                else:
                    self._repoint_subtag(subtag_id, into_subtag_id)
            self.connection.execute("DELETE FROM tags WHERE id = ?", (tag_id,))

    def merge_subtag(self, tag: str, subtag: str, into_tag: str, into_subtag: str):
        """Point all parts and rules of a subtag at another, then delete it.

        The target subtag is created if missing. The merged subtag's properties
        are dropped.
        """
        if (tag, subtag) == (into_tag, into_subtag):
            raise StoreError(f"Cannot merge subtag {tag!r}/{subtag!r} into itself")
        with self.connection:
            subtag_id = self._get_subtag_id(tag, subtag)
            into_subtag_id = self._ensure_subtag(into_tag, into_subtag)
            self._repoint_subtag(subtag_id, into_subtag_id)

    def _repoint_subtag(self, subtag_id: int, into_subtag_id: int):
        for table in ("parts", "rules"):
            self.connection.execute(
                f"UPDATE {table} SET subtag_id = ? WHERE subtag_id = ?",
                (into_subtag_id, subtag_id),
            )
        self.connection.execute("DELETE FROM subtags WHERE id = ?", (subtag_id,))

    def _find_tag_id(self, tag: str) -> Optional[int]:
        row = self.connection.execute(
            "SELECT id FROM tags WHERE name = ?", (tag,)
        ).fetchone()
        return None if row is None else row[0]

    def _get_tag_id(self, tag: str) -> int:
        tag_id = self._find_tag_id(tag)
        if tag_id is None:
            raise StoreError(f"No such tag: {tag!r}")
        return tag_id

    def _ensure_tag(self, tag: str) -> int:
        self.connection.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag,))
        return self._get_tag_id(tag)

    def _find_subtag_id(self, tag: str, subtag: str) -> Optional[int]:
        row = self.connection.execute(
            """
            SELECT subtags.id FROM subtags JOIN tags ON tags.id = subtags.tag_id
            WHERE tags.name = ? AND subtags.name = ?
            """,
            (tag, subtag),
        ).fetchone()
        return None if row is None else row[0]

    def _get_subtag_id(self, tag: str, subtag: str) -> int:
        subtag_id = self._find_subtag_id(tag, subtag)
        if subtag_id is None:
            raise StoreError(f"No such subtag: {tag!r}/{subtag!r}")
        return subtag_id

    def _ensure_subtag(self, tag: str, subtag: str) -> int:
        tag_id = self._ensure_tag(tag)
        self.connection.execute(
            "INSERT OR IGNORE INTO subtags (tag_id, name) VALUES (?, ?)",
            (tag_id, subtag),
        )
        return self._get_subtag_id(tag, subtag)

    # Annotations

    def get_annotation(self, transaction_id: str) -> Optional[Annotation]:
        annotations = self._query_annotations(
            "WHERE annotations.transaction_id = ?", (transaction_id,)
        )
        return annotations[0] if annotations else None

    def list_annotations(self) -> list[Annotation]:
        return self._query_annotations()

    def set_annotation(self, annotation: Annotation):
        """Create or replace the annotation of a transaction."""
        self.set_annotations([annotation])

    def set_annotations(self, annotations: Iterable[Annotation]):
        """Create or replace the annotations of transactions, all or none."""
        annotations = list(annotations)
        for annotation in annotations:
            annotation.validate()
        with self.connection:
            for annotation in annotations:
                self._set_annotation(annotation)

    def _set_annotation(self, annotation: Annotation):
        self._delete_annotation(annotation.transaction_id)
        snapshot = annotation.snapshot
        self.connection.execute(
            """
            INSERT INTO annotations (
                transaction_id, description, authors, needs_review,
                account, source, date, amount, raw_description
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                annotation.transaction_id,
                _optional_text(annotation.description),
                json.dumps(annotation.authors),
                annotation.needs_review,
                snapshot.account,
                snapshot.source,
                snapshot.date.isoformat(),
                snapshot.amount,
                snapshot.raw_description,
            ),
        )
        for position, part in enumerate(annotation.parts):
            self.connection.execute(
                """
                INSERT INTO parts (
                    transaction_id, position, amount, subtag_id, description, kind
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    annotation.transaction_id,
                    position,
                    part.amount,
                    self._ensure_subtag(part.tag, part.subtag),
                    _optional_text(part.description),
                    part.kind,
                ),
            )

    def set_needs_review(self, transaction_ids: Iterable[str], needs_review: bool):
        """Flag or unflag annotations for review, keeping their authors (the flag
        isn't content). Raises if any doesn't exist."""
        with self.connection:
            for transaction_id in transaction_ids:
                cursor = self.connection.execute(
                    "UPDATE annotations SET needs_review = ? WHERE transaction_id = ?",
                    (needs_review, transaction_id),
                )
                if cursor.rowcount == 0:
                    raise StoreError(f"No annotation for transaction: {transaction_id}")

    def delete_annotations(self, transaction_ids: Iterable[str]):
        with self.connection:
            for transaction_id in transaction_ids:
                self._delete_annotation(transaction_id)

    def _delete_annotation(self, transaction_id: str):
        self.connection.execute(
            "DELETE FROM annotations WHERE transaction_id = ?", (transaction_id,)
        )

    def _query_annotations(self, where: str = "", parameters=()) -> list[Annotation]:
        rows = self.connection.execute(
            f"""
            SELECT annotations.*, parts.amount AS part_amount,
                parts.description AS part_description, parts.kind AS part_kind,
                tags.name AS tag, subtags.name AS subtag
            FROM annotations
            LEFT JOIN parts ON parts.transaction_id = annotations.transaction_id
            LEFT JOIN subtags ON subtags.id = parts.subtag_id
            LEFT JOIN tags ON tags.id = subtags.tag_id
            {where}
            ORDER BY annotations.date, annotations.transaction_id, parts.position
            """,
            parameters,
        )
        annotations = {}
        parts = {}
        for row in rows:
            transaction_id = row["transaction_id"]
            if transaction_id not in annotations:
                snapshot = TransactionSnapshot(
                    account=row["account"],
                    source=row["source"],
                    date=datetime.date.fromisoformat(row["date"]),
                    amount=row["amount"],
                    raw_description=row["raw_description"],
                )
                annotations[transaction_id] = Annotation(
                    transaction_id,
                    snapshot,
                    parts=(),
                    description=row["description"],
                    authors=tuple(json.loads(row["authors"])),
                    needs_review=bool(row["needs_review"]),
                )
                parts[transaction_id] = []
            if row["part_amount"] is not None:  # Null for annotations without parts
                parts[transaction_id].append(
                    Part(
                        row["part_amount"],
                        row["tag"],
                        row["subtag"],
                        row["part_description"],
                        row["part_kind"],
                    )
                )
        return [
            dataclasses.replace(annotation, parts=tuple(parts[transaction_id]))
            for transaction_id, annotation in annotations.items()
        ]

    # Rules

    def list_rules(self) -> list[Rule]:
        rows = self.connection.execute(
            """
            SELECT rules.id, rules.filters, tags.name AS tag, subtags.name AS subtag
            FROM rules
            JOIN subtags ON subtags.id = rules.subtag_id
            JOIN tags ON tags.id = subtags.tag_id
            ORDER BY rules.position, rules.id
            """
        )
        return [
            Rule(
                row["id"],
                Filters.model_validate_json(row["filters"]),
                row["tag"],
                row["subtag"],
            )
            for row in rows
        ]

    def add_rule(
        self,
        filters: Filters,
        tag: str,
        subtag: str,
        *,
        position: Optional[int] = None,
    ) -> int:
        """Add a rule at a position in the order (default: last). Returns its ID."""
        validate_rule_filters(filters)
        with self.connection:
            rule_ids = self._rule_ids()
            if position is None:
                position = len(rule_ids)
            cursor = self.connection.execute(
                "INSERT INTO rules (position, filters, subtag_id) VALUES (?, ?, ?)",
                (
                    position,
                    filters.model_dump_json(exclude_defaults=True),
                    self._ensure_subtag(tag, subtag),
                ),
            )
            rule_ids.insert(position, cursor.lastrowid)
            self._renumber_rules(rule_ids)
            return cursor.lastrowid

    def update_rule(
        self, rule_id: int, *, filters=UNCHANGED, tag=UNCHANGED, subtag=UNCHANGED
    ):
        """Change a rule's filters, or its tag and subtag (given together)."""
        if (tag is UNCHANGED) != (subtag is UNCHANGED):
            raise StoreError("A rule's tag and subtag must be changed together")
        if filters is not UNCHANGED:
            validate_rule_filters(filters)
        with self.connection:
            self._get_rule_position(rule_id)
            if filters is not UNCHANGED:
                self.connection.execute(
                    "UPDATE rules SET filters = ? WHERE id = ?",
                    (filters.model_dump_json(exclude_defaults=True), rule_id),
                )
            if tag is not UNCHANGED:
                self.connection.execute(
                    "UPDATE rules SET subtag_id = ? WHERE id = ?",
                    (self._ensure_subtag(tag, subtag), rule_id),
                )

    def move_rule(self, rule_id: int, position: int):
        """Move a rule to a position in the order (0 is first)."""
        with self.connection:
            self._get_rule_position(rule_id)
            rule_ids = [other for other in self._rule_ids() if other != rule_id]
            rule_ids.insert(position, rule_id)
            self._renumber_rules(rule_ids)

    def reorder_rules(self, rule_ids: list[int]):
        """Set the order of all rules, given every rule ID exactly once."""
        with self.connection:
            current = self._rule_ids()
            if sorted(rule_ids) != sorted(current):
                raise StoreError(
                    f"Expected every rule ID exactly once: {sorted(current)}"
                )
            self._renumber_rules(rule_ids)

    def _get_rule_position(self, rule_id: int) -> int:
        row = self.connection.execute(
            "SELECT position FROM rules WHERE id = ?", (rule_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"No such rule: {rule_id}")
        return row[0]

    def delete_rule(self, rule_id: int):
        with self.connection:
            cursor = self.connection.execute(
                "DELETE FROM rules WHERE id = ?", (rule_id,)
            )
            if cursor.rowcount == 0:
                raise StoreError(f"No such rule: {rule_id}")
            self._renumber_rules(self._rule_ids())

    def _rule_ids(self) -> list[int]:
        rows = self.connection.execute("SELECT id FROM rules ORDER BY position, id")
        return [row[0] for row in rows]

    def _renumber_rules(self, rule_ids: list[int]):
        self.connection.executemany(
            "UPDATE rules SET position = ? WHERE id = ?",
            [(position, rule_id) for position, rule_id in enumerate(rule_ids)],
        )


def validate_rule_filters(filters: Filters):
    """Rules match transactions before they are tagged, so they can't use tags."""
    if not filters.has_effect:
        raise StoreError("Rule filters match every transaction")
    for nested in _iter_filters(filters):
        if nested.tags is not None or nested.subtags is not None:
            raise StoreError(f"Rule filters can't use tags or subtags: {filters}")
    _ = filters.predicate  # Raises ValueError for invalid combinations


def _iter_filters(filters: Filters):
    yield filters
    for nested in (filters.and_filters or []) + (filters.or_filters or []):
        yield from _iter_filters(nested)


def _normalize_color(color: Optional[str]) -> Optional[str]:
    if color is None:
        return None
    return Color(color).as_hex()


def _check_kind(kind: str):
    if kind not in TAG_KINDS:
        raise StoreError(f"Invalid tag kind {kind!r}, expected one of {TAG_KINDS}")


def _check_role(role: str):
    if role not in GROUP_ROLES:
        raise StoreError(f"Invalid group role {role!r}, expected one of {GROUP_ROLES}")


def _default_group_id(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT id FROM account_groups WHERE role = 'spending' ORDER BY position, id"
    ).fetchone()
    if row is not None:
        return row[0]
    name = DEFAULT_GROUP
    while connection.execute(
        "SELECT 1 FROM account_groups WHERE name = ?", (name,)
    ).fetchone():
        name = f"{name}*"
    position = connection.execute(
        "SELECT COALESCE(MAX(position) + 1, 0) FROM account_groups"
    ).fetchone()[0]
    cursor = connection.execute(
        "INSERT INTO account_groups (name, role, position) VALUES (?, 'spending', ?)",
        (name, position),
    )
    return cursor.lastrowid


def _optional_text(text: Optional[str]) -> Optional[str]:
    return text or None
