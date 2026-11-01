"""Ingestion rules: dropping, rewriting and turning parsed rows into transfers."""

import datetime

import polars as pl
import pydantic
import pytest

from finalyze.config import IngestionRule
from finalyze.source import ingest
from finalyze.source.identity import derive_ids
from finalyze.source.ingestion_rules import apply_rules
from finalyze.source.schema import RAW_SCHEMA
from tests.helpers import make_isolated_config

JANUARY = datetime.date(2025, 1, 10)


def raw_rows(rows):
    """Rows are (account, source, amount, raw description), all on `JANUARY`."""
    df = pl.DataFrame(
        {
            "account": [row[0] for row in rows],
            "source": [row[1] for row in rows],
            "date": [JANUARY] * len(rows),
            "amount": [float(row[2]) for row in rows],
            "raw_description": [row[3] for row in rows],
        }
    )
    return derive_ids(df).select(*RAW_SCHEMA)


def rule(**fields):
    return IngestionRule.model_validate(fields)


def summary(df):
    return sorted(
        df.select("account", "source", "amount", "raw_description").iter_rows()
    )


ROWS = [
    ("alice", "checking", -300, "CARD CHARGE"),
    ("alice", "checking", -40, "Coffee"),
    ("alice", "card", -300, "BIT"),
]


def test_no_rules_change_nothing():
    rows = raw_rows(ROWS)
    assert apply_rules(rows, []).equals(rows)


def test_drop():
    rows = raw_rows(ROWS)
    dropped = apply_rules(
        rows, [rule(match={"raw_description": "^Coffee$"}, action="drop")]
    )
    assert summary(dropped) == summary(
        rows.filter(pl.col("raw_description") != "Coffee")
    )


def test_rewrite_keeps_the_id():
    rows = raw_rows(ROWS)
    rewritten = apply_rules(
        rows,
        [
            rule(
                match={"raw_description": "^Coffee$"},
                action="rewrite",
                raw_description="Cafe",
                shift_days=-2,
            )
        ],
    )
    coffee_id = rows.filter(pl.col("raw_description") == "Coffee")["id"].item()
    cafe = rewritten.filter(pl.col("id") == coffee_id)
    assert cafe.select("raw_description", "date").row(0) == (
        "Cafe",
        datetime.date(2025, 1, 8),
    )


def test_transfer_adds_the_other_leg():
    rows = raw_rows(ROWS)
    transfer = rule(
        match={"source": "checking", "raw_description": "^CARD CHARGE$"},
        action="transfer",
        to_source="card",
        raw_description="Transfer checking-card",
    )
    result = apply_rules(rows, [transfer])
    assert summary(result) == [
        ("alice", "card", -300.0, "BIT"),
        ("alice", "card", 300.0, "Transfer checking-card"),
        ("alice", "checking", -300.0, "Transfer checking-card"),
        ("alice", "checking", -40.0, "Coffee"),
    ]
    # The leg in checking keeps the parsed row's ID, the added leg has its own
    charge_id = rows.filter(pl.col("raw_description") == "CARD CHARGE")["id"].item()
    legs = result.filter(pl.col("raw_description") == "Transfer checking-card")
    assert charge_id in legs["id"].to_list()
    assert legs["id"].n_unique() == 2
    # And the same every time
    assert apply_rules(rows, [transfer])["id"].sort().equals(result["id"].sort())


def test_transfer_to_another_account():
    rows = raw_rows(ROWS)
    result = apply_rules(
        rows,
        [
            rule(
                match={"raw_description": "^BIT$"},
                action="transfer",
                to_account="kim",
                to_source="bit",
            )
        ],
    )
    assert ("kim", "bit", 300.0, "BIT") in summary(result)


def test_first_matching_rule_wins_and_added_rows_are_not_matched():
    rows = raw_rows(ROWS)
    result = apply_rules(
        rows,
        [
            rule(
                match={"raw_description": "^BIT$"},
                action="transfer",
                to_source="bit",
            ),
            # Would match the added leg in bit, and the BIT row already matched
            rule(match={"raw_description": "^BIT$"}, action="drop"),
        ],
    )
    assert ("alice", "bit", 300.0, "BIT") in summary(result)
    assert ("alice", "card", -300.0, "BIT") in summary(result)


def test_date_bounds():
    rows = raw_rows(ROWS)
    later = rule(
        match={"raw_description": "^BIT$", "start_date": "2025-02-01"},
        action="drop",
    )
    assert apply_rules(rows, [later]).height == rows.height


@pytest.mark.parametrize(
    ("fields", "error"),
    [
        ({"match": {}, "action": "transfer"}, "needs a 'to_source'"),
        ({"match": {}, "action": "drop", "to_source": "card"}, "transfer target"),
        ({"match": {}, "action": "rewrite", "to_account": "kim"}, "transfer target"),
        ({"match": {}, "action": "drop", "shift_days": 1}, "can't change"),
        ({"match": {}, "action": "rewrite"}, "needs a 'raw_description'"),
        ({"match": {"tags": ["food"]}, "action": "drop"}, "tags or subtags"),
        ({"match": {}, "action": "keep"}, "action"),
        (
            {"match": {"or_filters": [{"subtags": ["x"]}]}, "action": "drop"},
            "tags or subtags",
        ),
        (
            {"match": {"source": "card", "or_filters": [{}]}, "action": "drop"},
            "mutually exclusive",
        ),
        ({"match": {"raw_description": "(("}, "action": "drop"}, "invalid filter"),
        (
            {"match": {"and_filters": [{"raw_description": "[z"}]}, "action": "drop"},
            "invalid filter",
        ),
    ],
)
def test_invalid_rules(fields, error):
    with pytest.raises(pydantic.ValidationError, match=error):
        IngestionRule.model_validate(fields)


def test_transfer_to_an_account_without_folder(tmp_path, use_config):
    write_csv(account_dir(tmp_path, "alice").joinpath("alice.csv"), [])
    rule = {"match": {}, "action": "transfer", "to_source": "bit", "to_account": "kim"}
    use_config(make_isolated_config(tmp_path, ingestion={"rules": [rule]}))
    with pytest.raises(ValueError, match="'kim', which has no folder"):
        ingest.parse_accounts()


def test_no_accounts(tmp_path):
    with pytest.raises(ValueError, match="No accounts: make a folder"):
        ingest.parse_accounts()


def account_dir(root, account):
    """An account's folder in the raw folder (see `make_isolated_host`)."""
    folder = root.joinpath("raw", account)
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def write_csv(path, rows):
    lines = ["source,date,amount,raw_description"]
    lines.extend(",".join(map(str, row)) for row in rows)
    path.write_text("\n".join(lines) + "\n")
    return path


def test_ingest_applies_rules_across_accounts_then_filters(tmp_path, use_config):
    write_csv(
        account_dir(tmp_path, "alice").joinpath("alice.csv"),
        [
            ("card", "2025-01-10", -300, "BIT"),
            ("card", "2024-12-10", -50, "BIT"),
        ],
    )
    write_csv(
        account_dir(tmp_path, "kim").joinpath("kim.csv"),
        [("bit", "2025-01-10", -300, "Dana: Gift")],
    )
    use_config(
        make_isolated_config(
            tmp_path,
            ingestion={
                "rules": [
                    {
                        "match": {"raw_description": "^BIT$"},
                        "action": "transfer",
                        "to_account": "kim",
                        "to_source": "bit",
                        "raw_description": "Transfer card-bit",
                    }
                ],
                "filters": {"start_date": "2025-01-01"},
            },
        )
    )
    accounts = ingest.parse_accounts()
    assert summary(accounts["alice"]) == [
        ("alice", "card", -300.0, "Transfer card-bit")
    ]
    assert summary(accounts["kim"]) == [
        ("kim", "bit", -300.0, "Dana: Gift"),
        ("kim", "bit", 300.0, "Transfer card-bit"),
    ]
    assert list(accounts["kim"].columns) == list(RAW_SCHEMA)


def test_ingest_an_account_without_files(tmp_path, use_config):
    write_csv(
        account_dir(tmp_path, "alice").joinpath("alice.csv"),
        [("card", "2025-01-10", -300, "BIT")],
    )
    # An empty folder: the account only transfers add rows to
    account_dir(tmp_path, "pot")
    use_config(
        make_isolated_config(
            tmp_path,
            ingestion={
                "rules": [
                    {
                        "match": {"raw_description": "^BIT$"},
                        "action": "transfer",
                        "to_account": "pot",
                        "to_source": "bit",
                    }
                ],
            },
        )
    )
    accounts = ingest.parse_accounts()
    assert summary(accounts["pot"]) == [("pot", "bit", 300.0, "BIT")]
