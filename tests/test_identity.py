import datetime
import hashlib

import polars as pl
import pytest

from finalyze.source.identity import (
    ID_LENGTH,
    DuplicateIds,
    derive_ids,
    validate_unique_ids,
)
from finalyze.source.schema import RAW_SCHEMA

VENDING = ("alice", "card", datetime.date(2025, 1, 10), -2.5, "Vending machine")
GROCERIES = ("alice", "card", datetime.date(2025, 1, 10), -42.1, "SuperMart")
SCHEMA_WITHOUT_ID = {name: dtype for name, dtype in RAW_SCHEMA.items() if name != "id"}


def frame(*rows) -> pl.DataFrame:
    columns = ("account", "source", "date", "amount", "raw_description")
    records = [dict(zip(columns, row, strict=True)) for row in rows]
    return pl.DataFrame(records, schema=SCHEMA_WITHOUT_ID)


def ids(*rows) -> list[str]:
    return derive_ids(frame(*rows))["id"].to_list()


def test_ids_are_hex_of_fixed_length():
    (transaction_id,) = ids(GROCERIES)
    assert len(transaction_id) == ID_LENGTH
    int(transaction_id, 16)


def test_ids_are_stable():
    # Pinned: changing the derivation orphans every stored annotation
    canonical = "alice\x1fcard\x1f2025-01-10\x1fSuperMart\x1f-4210\x1f0"
    expected = hashlib.sha256(canonical.encode()).hexdigest()[:ID_LENGTH]
    assert ids(GROCERIES) == [expected] == ["b895996bbfee249a"]


def test_ids_do_not_depend_on_other_rows_or_order():
    assert ids(GROCERIES, VENDING) == list(reversed(ids(VENDING, GROCERIES)))
    assert ids(GROCERIES, VENDING)[0] == ids(GROCERIES)[0]


@pytest.mark.parametrize("field_index", range(5))
def test_every_field_changes_the_id(field_index):
    changed = list(GROCERIES)
    changed[field_index] = {
        datetime.date: datetime.date(2025, 1, 11),
        float: -42.2,
        str: "other",
    }[type(GROCERIES[field_index])]
    assert ids(GROCERIES) != ids(tuple(changed))


def test_amount_is_compared_at_currency_precision():
    noisy = GROCERIES[:3] + (-42.1 + 1e-9,) + GROCERIES[4:]
    assert ids(GROCERIES) == ids(noisy)


def test_identical_rows_get_distinct_ids_by_occurrence():
    first, second = ids(VENDING, VENDING)
    assert first != second
    # The first occurrence keeps the ID of a lone row
    assert first == ids(VENDING)[0]
    # A new identical row keeps the existing IDs
    assert ids(VENDING, VENDING, VENDING)[:2] == [first, second]


def test_native_ids_are_kept():
    df = frame(GROCERIES, VENDING).with_columns(
        id=pl.Series(["native-1", None], dtype=pl.String)
    )
    native_id, derived_id = derive_ids(df)["id"].to_list()
    assert native_id == "native-1"
    assert derived_id == ids(VENDING)[0]


def test_null_fields_are_allowed():
    (transaction_id,) = ids(GROCERIES[:4] + (None,))
    assert transaction_id != ids(GROCERIES)[0]


def test_validate_unique_ids():
    validate_unique_ids(derive_ids(frame(VENDING, VENDING)))
    duplicated = derive_ids(frame(VENDING)).with_columns(id=pl.lit("same"))
    with pytest.raises(DuplicateIds):
        validate_unique_ids(pl.concat([duplicated, duplicated]))
