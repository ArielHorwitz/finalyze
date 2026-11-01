import dataclasses
import datetime
import sqlite3
import threading

import pytest

from finalyze import store as store_module
from finalyze.config import Filters
from finalyze.store import (
    BASELINE_VERSION,
    SCHEMA_VERSION,
    Account,
    Annotation,
    Group,
    InvalidSplit,
    Part,
    Store,
    StoreError,
    Subtag,
    Tag,
    TransactionSnapshot,
)

SNAPSHOT = TransactionSnapshot(
    account="alice",
    source="card",
    date=datetime.date(2025, 1, 10),
    amount=-10_000,
    raw_description="SuperMart",
)


@pytest.fixture
def store(tmp_path):
    with Store(tmp_path.joinpath("finalyze.db")) as store:
        yield store


def annotation(
    *parts: Part, transaction_id="tx1", description=None, **fields
) -> Annotation:
    return Annotation(
        transaction_id, SNAPSHOT, parts=parts, description=description, **fields
    )


def full_part(tag="food", subtag="groceries", description=None) -> Part:
    return Part(SNAPSHOT.amount, tag, subtag, description)


def subtag_names(store):
    return [(subtag.tag, subtag.name) for subtag in store.list_subtags()]


# Schema


def test_schema_version_and_reopening(tmp_path):
    path = tmp_path.joinpath("nested", "finalyze.db")
    with Store(path) as store:
        store.set_annotation(annotation(full_part()))
    with Store(path) as store:
        assert store.get_annotation("tx1") == annotation(full_part())
    version = sqlite3.connect(path).execute("PRAGMA user_version").fetchone()[0]
    assert version == SCHEMA_VERSION


def test_newer_schema_is_rejected(tmp_path):
    path = tmp_path.joinpath("finalyze.db")
    sqlite3.connect(path).execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
    with pytest.raises(StoreError, match="newer"):
        Store(path)


def test_foreign_keys_are_enforced(store):
    with pytest.raises(sqlite3.IntegrityError), store.connection:
        store.connection.execute(
            "INSERT INTO subtags (tag_id, name) VALUES (12345, 'orphan')"
        )


# Accounts


def test_accounts(store):
    store.ensure_accounts(["kim", "alice"])
    store.ensure_accounts(["alice"])
    store.add_group("savings", "holding")
    store.update_account("savings", group="savings", color="#00ff00")
    store.update_account("alice", color="red")
    assert store.list_accounts() == [
        Account("alice", group="spending", color="#f00"),
        Account("kim"),
        Account("savings", group="savings", color="#0f0"),
    ]
    store.update_account("savings", color=None)
    assert store.list_accounts()[2] == Account("savings", group="savings")
    with pytest.raises(StoreError, match="No such group"):
        store.update_account("kim", group="missing")


def test_groups(store):
    # The default group exists from the start, for spending
    assert store.list_groups() == [Group("spending", "spending")]
    store.add_group("debt", "holding", color="red")
    store.add_group("savings", "holding", position=1)
    assert [group.name for group in store.list_groups()] == [
        "spending",
        "savings",
        "debt",
    ]
    with pytest.raises(StoreError, match="already exists"):
        store.add_group("debt")
    with pytest.raises(StoreError, match="Invalid group role"):
        store.add_group("cash", "pocket")
    store.update_group("debt", new_name="loans", role="spending", color=None)
    store.move_group("loans", 0)
    assert store.list_groups() == [
        Group("loans", "spending"),
        Group("spending", "spending"),
        Group("savings", "holding"),
    ]
    with pytest.raises(StoreError, match="already exists"):
        store.update_group("loans", new_name="savings")
    # New accounts go to the first group for spending
    store.ensure_accounts(["alice"])
    assert store.list_accounts() == [Account("alice", group="loans")]
    with pytest.raises(StoreError, match="has 1 accounts"):
        store.delete_group("loans")
    store.update_account("alice", group="savings")
    store.delete_group("loans")
    store.delete_group("spending")
    assert store.list_groups() == [Group("savings", "holding")]
    # Without a group for spending, one is created for new accounts
    store.ensure_accounts(["kim"])
    assert store.list_groups() == [
        Group("savings", "holding"),
        Group("spending", "spending"),
    ]
    assert store.list_accounts()[1] == Account("kim", group="spending")


def test_invalid_color_is_rejected(store):
    with pytest.raises(ValueError):
        store.update_account("alice", color="not a color")


# Tags


def test_subtag_properties_inherit_from_tag(store):
    store.update_tag("car", kind="one-off", color="#123456")
    store.update_subtag("car", "fuel", kind="regular")
    store.ensure_subtag("car", "purchase")
    assert store.list_tags() == [Tag("car", "#123456", kind="one-off")]
    assert store.list_subtags() == [
        Subtag("car", "fuel", color=None, kind="regular"),
        Subtag("car", "purchase", color=None, kind=None),
    ]
    assert [subtag.kind for subtag in store.list_resolved_subtags()] == [
        "regular",
        "one-off",
    ]
    # None resets the override
    store.update_subtag("car", "fuel", kind=None)
    assert store.list_subtags()[0].kind is None
    with pytest.raises(StoreError, match="Invalid tag kind"):
        store.update_tag("car", kind="excluded")


def test_rename_tag(store):
    store.set_annotation(annotation(full_part("food", "groceries")))
    store.rename_tag("food", "groceries")
    assert store.get_annotation("tx1").parts[0].tag == "groceries"
    store.ensure_tag("food")
    with pytest.raises(StoreError, match="merge"):
        store.rename_tag("food", "groceries")
    with pytest.raises(StoreError, match="No such tag"):
        store.rename_tag("missing", "other")


def test_rename_subtag(store):
    store.set_annotation(annotation(full_part("food", "groceries")))
    store.add_rule(Filters(raw_description="SuperMart"), "food", "groceries")
    store.rename_subtag("food", "groceries", "supermarket")
    assert store.get_annotation("tx1").parts[0].subtag == "supermarket"
    assert store.list_rules()[0].subtag == "supermarket"
    store.ensure_subtag("food", "dining")
    with pytest.raises(StoreError, match="merge"):
        store.rename_subtag("food", "dining", "supermarket")


def test_merge_tag(store):
    store.update_tag("groceries", color="#f00", kind="transfer")
    store.update_subtag("groceries", "fruit", kind="regular")
    store.set_annotation(
        annotation(
            Part(-6_000, "groceries", "fruit"),
            Part(-4_000, "groceries", "bread"),
        )
    )
    store.add_rule(Filters(raw_description="Bakery"), "groceries", "bread")
    store.ensure_subtag("food", "bread")
    store.merge_tag("groceries", "food")
    assert [tag.name for tag in store.list_tags()] == ["food"]
    # The merged tag's properties are dropped, its subtag's are kept
    assert store.list_tags()[0] == Tag("food")
    assert store.list_subtags() == [
        Subtag("food", "bread"),
        Subtag("food", "fruit", kind="regular"),
    ]
    assert store.get_annotation("tx1").parts == (
        Part(-6_000, "food", "fruit"),
        Part(-4_000, "food", "bread"),
    )
    assert store.list_rules()[0].tag == "food"
    with pytest.raises(StoreError):
        store.merge_tag("food", "food")


def test_merge_subtag(store):
    store.set_annotation(annotation(full_part("food", "supermarket")))
    store.add_rule(Filters(raw_description="SuperMart"), "food", "supermarket")
    store.merge_subtag("food", "supermarket", "groceries", "general")
    assert subtag_names(store) == [("groceries", "general")]
    assert store.get_annotation("tx1").parts[0] == full_part("groceries", "general")
    assert (store.list_rules()[0].tag, store.list_rules()[0].subtag) == (
        "groceries",
        "general",
    )
    # The emptied tag remains, without subtags
    assert [tag.name for tag in store.list_tags()] == ["food", "groceries"]
    with pytest.raises(StoreError, match="No such subtag"):
        store.merge_subtag("food", "supermarket", "food", "other")


# Annotations


def test_annotation_round_trip(store):
    split = annotation(
        Part(-7_550, "food", "groceries", description="weekly"),
        Part(-2_450, "home", "cleaning"),
        description="split receipt",
    )
    store.set_annotation(split)
    assert store.get_annotation("tx1") == split
    assert store.list_annotations() == [split]
    assert store.get_annotation("missing") is None
    assert subtag_names(store) == [("food", "groceries"), ("home", "cleaning")]


def test_annotation_is_replaced(store):
    store.set_annotation(annotation(Part(-5_000, "a", "b"), Part(-5_000, "c", "d")))
    store.set_annotation(annotation(full_part(), description="retagged"))
    assert store.list_annotations() == [annotation(full_part(), description="retagged")]


def test_empty_descriptions_are_none(store):
    store.set_annotation(annotation(full_part(description=""), description=""))
    stored = store.get_annotation("tx1")
    assert stored.description is None
    assert stored.parts[0].description is None


@pytest.mark.parametrize(
    "parts",
    [
        (Part(-9_999, "food", "groceries"),),
        (Part(-5_000, "food", "groceries"), Part(-4_999, "home", "cleaning")),
        (Part(10_000, "food", "groceries"),),
    ],
)
def test_split_must_sum_exactly(store, parts):
    with pytest.raises(InvalidSplit):
        store.set_annotation(annotation(*parts))
    assert store.list_annotations() == []


def test_split_remainder_is_an_explicit_part(store):
    # A remainder may have any sign, but it must be there
    parts = (Part(-12_000, "food", "groceries"), Part(2_000, "refund", "deposit"))
    store.set_annotation(annotation(*parts))
    assert store.get_annotation("tx1").parts == parts


def test_delete_annotations(store):
    for transaction_id in ("tx1", "tx2", "tx3"):
        store.set_annotation(annotation(full_part(), transaction_id=transaction_id))
    store.delete_annotations(["tx1", "tx3", "missing"])
    assert [a.transaction_id for a in store.list_annotations()] == ["tx2"]
    remaining_parts = store.connection.execute("SELECT COUNT(*) FROM parts")
    assert remaining_parts.fetchone()[0] == 1


# Rules


def test_rules_are_ordered(store):
    first = store.add_rule(Filters(raw_description="a"), "t", "1")
    last = store.add_rule(Filters(raw_description="c"), "t", "3")
    middle = store.add_rule(Filters(raw_description="b"), "t", "2", position=1)
    head = store.add_rule(Filters(account="kim"), "t", "0", position=0)
    rules = store.list_rules()
    assert [rule.id for rule in rules] == [head, first, middle, last]
    assert [rule.subtag for rule in rules] == ["0", "1", "2", "3"]
    store.delete_rule(middle)
    assert [rule.id for rule in store.list_rules()] == [head, first, last]
    store.add_rule(Filters(raw_description="b"), "t", "2", position=2)
    assert [rule.subtag for rule in store.list_rules()] == ["0", "1", "2", "3"]
    with pytest.raises(StoreError):
        store.delete_rule(middle)


def test_rule_filters_round_trip(store):
    filters = Filters(
        or_filters=[
            Filters(raw_description="move cash"),
            Filters(account="savings", start_date="2024-04-01", invert=True),
        ]
    )
    store.add_rule(filters, "transfer", "inner")
    assert store.list_rules()[0].filters == filters


@pytest.mark.parametrize(
    "filters",
    [
        Filters(),
        Filters(tags=["food"]),
        Filters(or_filters=[Filters(raw_description="x"), Filters(subtags=["y"])]),
        Filters(raw_description="x", or_filters=[Filters(raw_description="y")]),
    ],
)
def test_invalid_rule_filters(store, filters):
    with pytest.raises((StoreError, ValueError)):
        store.add_rule(filters, "t", "s")
    assert store.list_rules() == []


# Additions for the service layer


def test_wal_mode_and_connections_across_threads(store):
    mode = store.connection.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"
    errors = []

    def use_connection():
        try:
            store.set_annotation(annotation(full_part()))
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=use_connection)
    thread.start()
    thread.join()
    assert errors == []
    assert store.get_annotation("tx1") is not None


def test_set_annotations_is_atomic(store):
    valid = annotation(full_part(), transaction_id="tx1")
    invalid = annotation(Part(1, "food", "groceries"), transaction_id="tx2")
    with pytest.raises(InvalidSplit):
        store.set_annotations([valid, invalid])
    assert store.list_annotations() == []
    store.set_annotations([valid, annotation(full_part(), transaction_id="tx2")])
    assert len(store.list_annotations()) == 2


def test_update_move_and_reorder_rules(store):
    first = store.add_rule(Filters(raw_description="a"), "food", "groceries")
    second = store.add_rule(Filters(raw_description="b"), "food", "dining")
    third = store.add_rule(Filters(raw_description="c"), "car", "fuel")
    store.update_rule(second, filters=Filters(raw_description="B"), tag="x", subtag="y")
    rule = next(rule for rule in store.list_rules() if rule.id == second)
    assert (rule.filters.raw_description, rule.tag, rule.subtag) == ("B", "x", "y")
    with pytest.raises(StoreError, match="together"):
        store.update_rule(second, tag="x")
    with pytest.raises(StoreError, match="tags"):
        store.update_rule(second, filters=Filters(tags=["food"]))
    with pytest.raises(StoreError, match="No such rule"):
        store.update_rule(99, filters=Filters(raw_description="a"))
    store.move_rule(third, 0)
    assert [rule.id for rule in store.list_rules()] == [third, first, second]
    store.reorder_rules([second, third, first])
    assert [rule.id for rule in store.list_rules()] == [second, third, first]
    with pytest.raises(StoreError, match="exactly once"):
        store.reorder_rules([second, third])


def test_delete_tags_only_when_unused(store):
    store.set_annotation(annotation(full_part("food", "groceries")))
    store.add_rule(Filters(raw_description="a"), "car", "fuel")
    store.ensure_subtag("food", "unused")
    store.ensure_subtag("empty", "")
    assert store.subtag_usage()[("food", "groceries")] == (1, 0)
    assert store.subtag_usage()[("car", "fuel")] == (0, 1)
    for delete in (
        lambda: store.delete_tag("food"),
        lambda: store.delete_subtag("food", "groceries"),
        lambda: store.delete_tag("car"),
    ):
        with pytest.raises(StoreError, match="is used by 1"):
            delete()
    store.delete_subtag("food", "unused")
    store.delete_tag("empty")
    assert subtag_names(store) == [("car", "fuel"), ("food", "groceries")]
    assert not store.has_tag("empty")
    assert store.has_subtag("food", "groceries")


def test_annotations_without_parts_are_descriptions(store):
    description_only = annotation(description="ask about this")
    store.set_annotation(description_only)
    assert store.get_annotation("tx1") == description_only
    assert store.list_annotations() == [description_only]
    # Replacing it with parts, and back
    store.set_annotation(annotation(full_part(), description="groceries"))
    assert store.get_annotation("tx1").parts == (full_part(),)
    store.set_annotation(description_only)
    assert store.get_annotation("tx1").parts == ()


def test_authors_and_review(store):
    stored = annotation(full_part())
    store.set_annotation(stored)
    assert (stored.authors, stored.needs_review) == (("owner",), False)
    flagged = dataclasses.replace(stored, authors=("claude",), needs_review=True)
    store.set_annotation(flagged)
    assert store.get_annotation("tx1") == flagged
    # Reviewing keeps the authors: the flag isn't content
    store.set_needs_review(["tx1"], False)
    assert store.get_annotation("tx1") == dataclasses.replace(
        flagged, needs_review=False
    )
    with pytest.raises(StoreError, match="No annotation"):
        store.set_needs_review(["tx1", "nope"], True)
    # All or none
    assert not store.get_annotation("tx1").needs_review


def test_label_colors(store):
    store.set_label_color("net worth", "#fff")
    store.set_label_color("alice/card", "red")
    store.set_label_color("net worth", "#bfb")
    assert store.label_colors() == {"alice/card": "#f00", "net worth": "#bfb"}
    store.set_label_color("alice/card", None)
    assert store.label_colors() == {"net worth": "#bfb"}
    with pytest.raises(ValueError):
        store.set_label_color("net worth", "not a color")


def test_settings(store):
    assert store.get_setting("missing") is None
    assert store.get_setting("missing", {}) == {}
    store.set_setting("defaults", {"months": 3, "net": True})
    store.set_setting("defaults", {"months": 6})
    assert store.get_setting("defaults") == {"months": 6}


def test_kind_of_parts(store):
    parts = (
        Part(-6_000, "food", "fruit", kind="one-off"),
        Part(-4_000, "food", "bread"),
    )
    store.set_annotation(annotation(*parts))
    assert store.get_annotation("tx1").parts == parts
    with pytest.raises(StoreError, match="Invalid tag kind"):
        store.set_annotation(annotation(Part(SNAPSHOT.amount, "a", "b", kind="big")))
    # Unchanged
    assert store.get_annotation("tx1").parts == parts


# Schema


def test_a_new_store_has_the_current_schema(tmp_path):
    path = tmp_path.joinpath("finalyze.db")
    Store(path).close()
    connection = sqlite3.connect(path)
    assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    # Kinds are checked by the database
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("INSERT INTO tags (name, kind) VALUES ('a', 'excluded')")
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("INSERT INTO tags (name, kind) VALUES ('a', NULL)")
    connection.execute("INSERT INTO tags (name, kind) VALUES ('a', 'reconciliation')")


@pytest.mark.parametrize(
    ("version", "message"),
    [(1, "older than the oldest supported"), (SCHEMA_VERSION + 1, "newer than")],
)
def test_stores_of_unsupported_versions_fail_clearly(tmp_path, version, message):
    path = tmp_path.joinpath("finalyze.db")
    connection = sqlite3.connect(path)
    connection.execute(f"PRAGMA user_version = {version}")
    connection.close()
    with pytest.raises(StoreError, match=f"schema version {version}, {message}"):
        Store(path)


def test_upgrades_run_once_after_the_baseline(tmp_path, monkeypatch):
    path = tmp_path.joinpath("finalyze.db")
    with Store(path) as store:
        store.set_setting("kept", 1)
    upgrades = (
        "INSERT INTO settings VALUES ('script', '1')",
        lambda connection: connection.execute(
            "INSERT INTO settings VALUES ('function', '2')"
        ),
    )
    monkeypatch.setattr(store_module, "SCHEMA_UPGRADES", upgrades)
    monkeypatch.setattr(store_module, "SCHEMA_VERSION", BASELINE_VERSION + 2)
    for _ in range(2):
        with Store(path) as store:
            assert [store.get_setting(name) for name in ("kept", "script")] == [1, 1]
            assert store.get_setting("function") == 2
    version = sqlite3.connect(path).execute("PRAGMA user_version").fetchone()[0]
    assert version == BASELINE_VERSION + 2
