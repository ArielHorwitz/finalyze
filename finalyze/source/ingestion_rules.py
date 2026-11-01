"""Ingestion rules: reshaping the parsed rows (of every account) before they are
ingested. See `finalyze.config.IngestionRule`."""

import polars as pl

from finalyze.config import IngestionRule
from finalyze.source.identity import transfer_leg_id_expr


def apply_rules(rows: pl.DataFrame, rules: list[IngestionRule]) -> pl.DataFrame:
    """The rows after the first matching rule of each row applied. Rules apply to
    the parsed rows only, never to the rows a rule added."""
    unmatched = rows
    results = []
    for rule in rules:
        is_match = rule.match.predicate.fill_null(False)
        matched = unmatched.filter(is_match)
        unmatched = unmatched.filter(~is_match)
        if rule.action == "drop":
            continue
        changed = matched.with_columns(**_changes(rule))
        results.append(changed)
        if rule.action == "transfer":
            results.append(_transfer_legs(changed, rule))
    return pl.concat([unmatched, *results])


def _changes(rule: IngestionRule) -> dict[str, pl.Expr]:
    changes = {}
    if rule.raw_description is not None:
        changes["raw_description"] = pl.lit(rule.raw_description)
    if rule.shift_days:
        changes["date"] = pl.col("date") + pl.duration(days=rule.shift_days)
    return changes


def _transfer_legs(rows: pl.DataFrame, rule: IngestionRule) -> pl.DataFrame:
    """The other legs of transfers: in the target source, of opposite amounts."""
    account = pl.col("account") if rule.to_account is None else pl.lit(rule.to_account)
    return rows.with_columns(
        id=transfer_leg_id_expr(),
        account=account,
        source=pl.lit(rule.to_source),
        amount=-pl.col("amount"),
    )
