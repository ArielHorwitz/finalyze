"""Unit tests for applying annotations and rules, guesses, and orphans."""

import datetime

import polars as pl
import pytest

from finalyze.config import Filters, host
from finalyze.service import View
from finalyze.source import data as data_module
from finalyze.source.identity import derive_ids
from finalyze.source.schema import RAW_SCHEMA
from finalyze.source.tag import annotate, find_orphans, find_overridden, snapshot_of
from finalyze.store import Annotation, Part, Store

TRANSACTIONS = derive_ids(
    pl.DataFrame(
        {
            "account": ["alice", "alice", "alice", "kim", "savings"],
            "source": ["checking", "card", "card", "checking", "savings"],
            "date": [
                datetime.date(2025, 1, 1),
                datetime.date(2025, 1, 5),
                datetime.date(2025, 1, 7),
                datetime.date(2025, 1, 9),
                datetime.date(2025, 1, 12),
            ],
            "amount": [5000.0, -100.5, -30.25, -60.0, 12.34],
            "raw_description": [
                "Payroll",
                "MegaStore",
                "Corner shop",
                "Corner shop",
                "Interest",
            ],
        },
        schema={name: dtype for name, dtype in RAW_SCHEMA.items() if name != "id"},
    )
).select(*RAW_SCHEMA)
PAYROLL_ID, MEGASTORE_ID, CORNER_ALICE_ID, CORNER_KIM_ID, INTEREST_ID = TRANSACTIONS[
    "id"
]


@pytest.fixture
def store(isolated_config):
    with Store(host().db_file) as store:
        yield store


def row_of(transaction_id: str) -> dict:
    return TRANSACTIONS.row(by_predicate=pl.col("id") == transaction_id, named=True)


def annotate_with(store, transaction_id, *parts, description=None):
    snapshot = snapshot_of(row_of(transaction_id))
    store.set_annotation(Annotation(transaction_id, snapshot, parts, description))


def tags_by_id(tagged: pl.DataFrame) -> dict:
    return {
        row["id"]: (row["tag"], row["subtag"]) for row in tagged.iter_rows(named=True)
    }


def test_untagged_without_annotations_or_rules(store):
    tagged = annotate(TRANSACTIONS, store)
    assert tagged.height == TRANSACTIONS.height
    assert tagged["tag"].null_count() == TRANSACTIONS.height


def test_single_part_annotation(store):
    annotate_with(
        store, MEGASTORE_ID, Part(-10_050, "shopping", "home"), description="n"
    )
    tagged = annotate(TRANSACTIONS, store)
    row = tagged.row(by_predicate=pl.col("id") == MEGASTORE_ID, named=True)
    assert (row["tag"], row["subtag"], row["transaction_description"]) == (
        "shopping",
        "home",
        "n",
    )
    # The original amount is kept as is
    assert row["amount"] == -100.5


def test_split_parts_replace_the_transaction(store):
    annotate_with(
        store,
        MEGASTORE_ID,
        Part(-7_025, "food", "groceries", description="fruit"),
        Part(-3_025, "home", "cleaning"),
        description="receipt",
    )
    tagged = annotate(TRANSACTIONS, store)
    parts = tagged.filter(pl.col("id") == MEGASTORE_ID).sort("amount")
    assert parts.select(
        "amount", "tag", "subtag", "description", "transaction_description"
    ).rows() == [
        (-70.25, "food", "groceries", "fruit", "receipt"),
        (-30.25, "home", "cleaning", None, "receipt"),
    ]
    assert tagged.height == TRANSACTIONS.height + 1
    assert tagged["amount"].sum() == pytest.approx(TRANSACTIONS["amount"].sum())


def test_mismatched_split_is_ignored(store, capsys):
    # E.g. a native ID whose transaction amount changed since it was annotated
    snapshot = snapshot_of(row_of(MEGASTORE_ID))
    stale = snapshot.__class__(**{**snapshot.__dict__, "amount": -9_000})
    store.set_annotation(
        Annotation(MEGASTORE_ID, stale, (Part(-9_000, "shopping", "home"),))
    )
    tagged = annotate(TRANSACTIONS, store)
    assert tags_by_id(tagged)[MEGASTORE_ID] == (None, None)
    assert MEGASTORE_ID in capsys.readouterr().err


def test_first_matching_rule_wins_and_overrides_annotations(store):
    annotate_with(
        store, CORNER_KIM_ID, Part(-6_000, "gifts", "kim"), description="kept"
    )
    store.add_rule(Filters(raw_description="Corner", account="alice"), "food", "alice")
    store.add_rule(Filters(raw_description="Corner"), "food", "snacks")
    store.add_rule(Filters(raw_description="shop"), "never", "matched")
    tagged = annotate(TRANSACTIONS, store)
    tags = tags_by_id(tagged)
    assert tags[CORNER_ALICE_ID] == ("food", "alice")
    assert tags[CORNER_KIM_ID] == ("food", "snacks")
    assert tags[PAYROLL_ID] == (None, None)
    row = tagged.row(by_predicate=pl.col("id") == CORNER_KIM_ID, named=True)
    assert row["transaction_description"] == "kept"
    # Rules don't mutate the stored filters (they used to mutate the config)
    assert store.list_rules()[0].filters == Filters(
        raw_description="Corner", account="alice"
    )


def test_kinds_and_groups_from_properties(store):
    store.update_tag("transfer", kind="transfer")
    store.update_subtag("transfer", "loan", kind="regular")
    store.update_subtag("car", "purchase", kind="one-off")
    store.ensure_subtag("car", "fuel")
    store.add_group("savings", "holding")
    store.update_account("savings", group="savings")
    store.ensure_accounts(["alice"])
    df = pl.DataFrame(
        {
            "account": ["alice", "alice", "alice", "alice", "savings", "kim", "alice"],
            "tag": ["transfer", "transfer", "car", "car", "food", None, "car"],
            "subtag": ["inner", "loan", "purchase", "fuel", "unknown", None, "fuel"],
            "part_kind": [None, None, None, None, None, None, "one-off"],
        }
    )
    df = data_module._add_kind(df, store)
    df = data_module._add_group(df, store)
    assert df.columns == [
        "account",
        "tag",
        "subtag",
        "kind",
        "kind_set",
        "group",
        "group_role",
    ]
    # Unknown tags and untagged rows are regular, and a kind set on a
    # transaction wins over its tag's
    assert df["kind"].to_list() == [
        "transfer",
        "regular",
        "one-off",
        "regular",
        "regular",
        "regular",
        "one-off",
    ]
    assert df["kind_set"].to_list() == [False] * 6 + [True]
    # Accounts unknown to the store are in the default group
    assert df["group"].to_list() == ["spending"] * 4 + ["savings"] + ["spending"] * 2
    assert df["group_role"].to_list() == (
        ["spending"] * 4 + ["holding"] + ["spending"] * 2
    )


def test_find_orphans(store):
    annotate_with(store, PAYROLL_ID, Part(500_000, "salary", "alice"))
    annotate_with(store, MEGASTORE_ID, Part(-10_050, "shopping", "home"))
    remaining = TRANSACTIONS.filter(pl.col("id") != MEGASTORE_ID)
    assert [o.transaction_id for o in find_orphans(store, remaining)] == [MEGASTORE_ID]


def test_overridden_annotations(store):
    annotate_with(
        store, CORNER_KIM_ID, Part(-6_000, "gifts", "kim"), description="mine"
    )
    annotate_with(store, MEGASTORE_ID, Part(-10_050, "shopping", "home"))
    store.add_rule(Filters(raw_description="Corner"), "food", "snacks")
    assert [a.transaction_id for a in find_overridden(store, TRANSACTIONS)] == [
        CORNER_KIM_ID
    ]
    store.add_rule(Filters(raw_description="Payroll|MegaStore|Interest"), "x", "y")
    assert len(find_overridden(store, TRANSACTIONS)) == 2


# Guesses


def guessed(store, source_data, transaction_id):
    """The guessed (tag, subtag) of a transaction, if any."""
    view = View(store, source_data)
    guess = view.guess(view.get(transaction_id))
    return None if guess is None else (guess.tag, guess.subtag)


def test_guesses_from_same_description(store):
    annotate_with(store, CORNER_ALICE_ID, Part(-3_025, "food", "snacks"))
    assert guessed(store, TRANSACTIONS, CORNER_KIM_ID) == ("food", "snacks")


def test_no_guess_without_same_description(store):
    assert guessed(store, TRANSACTIONS, PAYROLL_ID) is None
    annotate_with(store, MEGASTORE_ID, Part(-10_050, "shopping", "home"))
    annotate_with(store, CORNER_ALICE_ID, Part(-3_025, "food", "snacks"))
    assert guessed(store, TRANSACTIONS, PAYROLL_ID) is None


SHOP_ROWS = derive_ids(
    pl.DataFrame(
        {
            "account": ["alice", "alice", "alice", "alice", "kim", "alice"],
            "source": ["card"] * 6,
            "date": [datetime.date(2025, 2, day) for day in (1, 2, 4, 5, 6, 7)],
            "amount": [-100.0, -10.0, -12.0, -95.0, -10.0, -50.0],
            "raw_description": ["Shop"] * 6,
        },
        schema={name: dtype for name, dtype in RAW_SCHEMA.items() if name != "id"},
    )
).select(*RAW_SCHEMA)
FIRST_ID, NEAR_FIRST_ID, MIDDLE_ID, NEAR_RULED_ID, RULED_ID, LAST_ID = SHOP_ROWS["id"]


def test_guess_nearest_in_date(store):
    def annotate_shop(transaction_id, subtag):
        row = SHOP_ROWS.row(by_predicate=pl.col("id") == transaction_id, named=True)
        snapshot = snapshot_of(row)
        part = Part(snapshot.amount, "shop", subtag)
        store.set_annotation(Annotation(transaction_id, snapshot, (part,)))

    annotate_shop(FIRST_ID, "first")
    annotate_shop(LAST_ID, "last")
    store.add_rule(Filters(account="kim"), "shop", "ruled")
    # Nearest in date, regardless of amount
    assert guessed(store, SHOP_ROWS, NEAR_FIRST_ID) == ("shop", "first")
    # 3 days from both: the later one wins
    assert guessed(store, SHOP_ROWS, MIDDLE_ID) == ("shop", "last")
    # Only annotated transactions count, not rule-tagged ones
    assert guessed(store, SHOP_ROWS, NEAR_RULED_ID) == ("shop", "last")
    # Later transactions count too
    store.delete_annotations([FIRST_ID])
    assert guessed(store, SHOP_ROWS, NEAR_FIRST_ID) == ("shop", "last")
    # The transaction itself doesn't count
    assert guessed(store, SHOP_ROWS, LAST_ID) is None


def test_description_only_annotation_is_untagged(store):
    annotate_with(store, MEGASTORE_ID, description="what was this?")
    tagged = annotate(TRANSACTIONS, store)
    row = tagged.row(by_predicate=pl.col("id") == MEGASTORE_ID, named=True)
    assert (row["tag"], row["transaction_description"]) == (None, "what was this?")
    # A rule doesn't override a description only
    annotate_with(store, CORNER_KIM_ID, description="kim's")
    store.add_rule(Filters(raw_description="Corner"), "food", "snacks")
    assert find_overridden(store, TRANSACTIONS) == []
    tagged = annotate(TRANSACTIONS, store)
    row = tagged.row(by_predicate=pl.col("id") == CORNER_KIM_ID, named=True)
    assert (row["tag"], row["transaction_description"]) == ("food", "kim's")
