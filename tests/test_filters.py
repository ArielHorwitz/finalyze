import datetime

import polars as pl
import pytest

from finalyze.config import Filters

FRAME = pl.DataFrame(
    {
        "id": [0, 1, 2, 3, 4],
        "account": ["alice", "alice", "kim", "kim", "savings"],
        "source": ["checking", "card", "checking", "card", "savings"],
        "date": [
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 15),
            datetime.date(2025, 2, 1),
            datetime.date(2025, 2, 28),
            datetime.date(2025, 3, 1),
        ],
        "raw_description": [
            "Payroll - Initech",
            "SuperMart",
            "Payroll - Hooli",
            "Super Saver",
            "Savings interest",
        ],
        "tag": ["salary", "food", "salary", "food", "savings"],
        "subtag": ["alice", "groceries", "kim", "dining out", "interest"],
    }
)
ALL_IDS = [0, 1, 2, 3, 4]


def filtered_ids(filters, **apply_kwargs):
    return filters.apply(FRAME, **apply_kwargs)["id"].to_list()


def complement(ids):
    return [i for i in ALL_IDS if i not in ids]


@pytest.mark.parametrize(
    ("filters", "expected_ids"),
    [
        (Filters(), ALL_IDS),
        # start_date is inclusive
        (Filters(start_date="2025-02-01"), [2, 3, 4]),
        # end_date is exclusive
        (Filters(end_date="2025-02-01"), [0, 1]),
        (Filters(start_date="2025-01-15", end_date="2025-03-01"), [1, 2, 3]),
        (Filters(tags=["salary", "savings"]), [0, 2, 4]),
        (Filters(tags=[]), []),
        (Filters(subtags=["groceries", "kim"]), [1, 2]),
        # raw_description is a regex substring match
        (Filters(raw_description="Payroll"), [0, 2]),
        (Filters(raw_description="^Super"), [1, 3]),
        (Filters(raw_description="Mart|Saver"), [1, 3]),
        (Filters(raw_description="supermart"), []),
        # account is an exact match
        (Filters(account="alice"), [0, 1]),
        (Filters(account="ali"), []),
        # source is exact, not a regular expression
        (Filters(source="card"), [1, 3]),
        (Filters(source="car"), []),
        (Filters(source="^(checking|savings)$"), []),
        # multiple predicates are combined with AND
        (Filters(account="kim", tags=["food"]), [3]),
        (
            Filters(account="alice", source="checking", raw_description="Payroll"),
            [0],
        ),
    ],
)
def test_predicates(filters, expected_ids):
    assert filtered_ids(filters) == expected_ids


@pytest.mark.parametrize(
    "filters",
    [
        Filters(),
        Filters(account="kim"),
        Filters(account="kim", tags=["food"]),
        Filters(start_date="2025-02-01"),
    ],
)
def test_invert_predicates(filters):
    expected_ids = complement(filtered_ids(filters))
    inverted_field = filters.model_copy(update={"invert": True})
    assert filtered_ids(inverted_field) == expected_ids
    assert filtered_ids(filters.inverted()) == expected_ids
    assert filtered_ids(filters, invert=True) == expected_ids


def test_invert_alone_matches_nothing():
    assert filtered_ids(Filters(invert=True)) == []


def test_apply_invert_and_field_invert_cancel_out():
    filters = Filters(account="kim", invert=True)
    assert filtered_ids(filters, invert=True) == [2, 3]


def test_inverted_returns_flipped_copy():
    original = Filters(account="kim")
    inverted = original.inverted()
    assert inverted is not original
    assert inverted.invert is True
    assert original.invert is False
    assert inverted.inverted().invert is False
    assert inverted.model_dump() == original.model_dump() | {"invert": True}


def test_inverted_deep_copies_nested_filters():
    original = Filters(or_filters=[Filters(account="kim")])
    inverted = original.inverted()
    inverted.or_filters[0].account = "alice"
    assert original.or_filters[0].account == "kim"


def test_or_filters():
    filters = Filters(
        or_filters=[Filters(account="savings"), Filters(tags=["food"])],
    )
    assert filtered_ids(filters) == [1, 3, 4]


def test_and_filters():
    filters = Filters(
        and_filters=[Filters(account="kim"), Filters(source="card")],
    )
    assert filtered_ids(filters) == [3]


def test_nested_and_or_filters():
    # (alice AND card) OR (kim AND NOT salary)
    filters = Filters(
        or_filters=[
            Filters(and_filters=[Filters(account="alice"), Filters(source="card")]),
            Filters(
                and_filters=[
                    Filters(account="kim"),
                    Filters(tags=["salary"], invert=True),
                ]
            ),
        ]
    )
    assert filtered_ids(filters) == [1, 3]


def test_nested_filters_respect_their_own_invert():
    filters = Filters(
        and_filters=[Filters(account="kim", invert=True), Filters(tags=["salary"])],
    )
    assert filtered_ids(filters) == [0]


def test_empty_or_and_filters_are_ignored():
    assert filtered_ids(Filters(or_filters=[])) == ALL_IDS
    assert filtered_ids(Filters(and_filters=[])) == ALL_IDS
    assert filtered_ids(Filters(or_filters=[], account="kim")) == [2, 3]


def test_or_filters_with_invert_via_apply():
    filters = Filters(or_filters=[Filters(account="savings"), Filters(tags=["food"])])
    assert filtered_ids(filters, invert=True) == [0, 2]


def test_invert_with_or_filters():
    filters = Filters(
        or_filters=[Filters(account="savings"), Filters(tags=["food"])],
        invert=True,
    )
    assert filtered_ids(filters) == [0, 2]
    assert filtered_ids(filters.inverted()) == [1, 3, 4]
    assert filtered_ids(filters, invert=True) == [1, 3, 4]


def test_invert_with_and_filters():
    filters = Filters(
        and_filters=[Filters(account="kim"), Filters(source="card")],
        invert=True,
    )
    assert filtered_ids(filters) == [0, 1, 2, 4]
    assert filtered_ids(filters.inverted()) == [3]
    assert filtered_ids(filters, invert=True) == [3]


@pytest.mark.parametrize(
    "filters",
    [
        Filters(account="kim", or_filters=[Filters(tags=["food"])]),
        Filters(account="kim", and_filters=[Filters(tags=["food"])]),
        Filters(
            and_filters=[Filters(account="kim")], or_filters=[Filters(tags=["food"])]
        ),
    ],
)
def test_mutually_exclusive_predicates(filters):
    with pytest.raises(ValueError, match="mutually exclusive"):
        _ = filters.predicate


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        (Filters(), False),
        (Filters(or_filters=[]), False),
        (Filters(and_filters=[]), False),
        (Filters(invert=True), True),
        (Filters(account="kim"), True),
        (Filters(tags=[]), True),
        (Filters(or_filters=[Filters()]), True),
        (Filters(and_filters=[Filters()]), True),
    ],
)
def test_has_effect(filters, expected):
    assert bool(filters.has_effect) is expected


def test_dates_parse_from_strings():
    filters = Filters(start_date="2025-02-01", end_date="2025-03-01")
    assert filters.start_date == datetime.date(2025, 2, 1)
    assert filters.end_date == datetime.date(2025, 3, 1)
