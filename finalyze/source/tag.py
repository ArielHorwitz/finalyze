import sys

import polars as pl

from finalyze.money import (
    CURRENCY_DECIMALS,
    MINOR_UNITS_PER_UNIT,
    minor_units_expr,
    to_minor_units,
)
from finalyze.source.schema import RAW_SCHEMA, TAGGED_SCHEMA, validate_schema
from finalyze.store import (
    Annotation,
    Rule,
    Store,
    TransactionSnapshot,
)

PARTS_SCHEMA = {
    "id": pl.String,
    "part_amount": pl.Int64,
    "tag": pl.String,
    "subtag": pl.String,
    "description": pl.String,
    "transaction_description": pl.String,
    "part_kind": pl.String,
}


def annotate(data: pl.DataFrame, store: Store) -> pl.DataFrame:
    """Tag transactions by rules and stored annotations.

    The first matching rule wins, and rules override annotations. Otherwise,
    each part of a transaction's annotation replaces it as a row with the part's
    amount (and the kind set on the part, if any). Transactions with neither
    remain as they are, with null tags (also with an annotation that has no
    parts, which is only a description).
    """
    validate_schema(data, RAW_SCHEMA)
    data = data.with_columns(rule_index_expr(store.list_rules()))
    rules = pl.DataFrame(
        [
            (index, rule.tag, rule.subtag)
            for index, rule in enumerate(store.list_rules())
        ],
        schema={"rule_index": pl.UInt32, "tag": pl.String, "subtag": pl.String},
        orient="row",
    )
    annotations = store.list_annotations()
    parts = _parts_frame(annotations)
    transaction_descriptions = pl.DataFrame(
        [
            (annotation.transaction_id, annotation.description)
            for annotation in annotations
        ],
        schema={"id": pl.String, "transaction_description": pl.String},
        orient="row",
    )

    ruled = (
        data.filter(pl.col("rule_index").is_not_null())
        .join(rules, on="rule_index", how="left")
        .join(transaction_descriptions, on="id", how="left")
        .with_columns(
            description=pl.lit(None, dtype=pl.String),
            part_kind=pl.lit(None, dtype=pl.String),
        )
    )
    unruled = data.filter(pl.col("rule_index").is_null())
    parts = parts.with_columns(
        parts_total=pl.col("part_amount").sum().over("id"),
        parts_count=pl.len().over("id"),
    )
    mismatched_ids = (
        unruled.join(parts, on="id", how="inner")
        .filter(pl.col("parts_total") != minor_units_expr("amount"))["id"]
        .unique()
    )
    if len(mismatched_ids):
        print(
            "Ignoring annotations whose parts don't sum to the transaction amount: "
            + ", ".join(mismatched_ids),
            file=sys.stderr,
        )
    valid_parts = parts.filter(~pl.col("id").is_in(mismatched_ids))
    # A single part has the full amount: keep the original value as is
    # Rounded, because polars divides by multiplying with the reciprocal
    part_amount = (
        pl.col("part_amount").truediv(MINOR_UNITS_PER_UNIT).round(CURRENCY_DECIMALS)
    )
    annotated = unruled.join(valid_parts, on="id", how="inner").with_columns(
        amount=pl.when(pl.col("parts_count") == 1)
        .then(pl.col("amount"))
        .otherwise(part_amount)
    )
    untagged = (
        unruled.join(valid_parts, on="id", how="anti")
        .join(transaction_descriptions, on="id", how="left")
        .with_columns(
            **{
                column: pl.lit(None, dtype=pl.String)
                for column in ("tag", "subtag", "description", "part_kind")
            }
        )
    )
    columns = list(TAGGED_SCHEMA)
    tagged = pl.concat(
        [frame.select(columns) for frame in (ruled, annotated, untagged)]
    )
    validate_schema(tagged, TAGGED_SCHEMA)
    return tagged


def rule_index_expr(rules: list[Rule]) -> pl.Expr:
    """The index of the first matching rule, null if none match."""
    if not rules:
        return pl.lit(None, dtype=pl.UInt32).alias("rule_index")
    chain = pl
    for index, rule in enumerate(rules):
        chain = chain.when(rule.filters.predicate).then(pl.lit(index, dtype=pl.UInt32))
    return chain.otherwise(None).alias("rule_index")


def _parts_frame(annotations: list[Annotation]) -> pl.DataFrame:
    rows = [
        (
            annotation.transaction_id,
            part.amount,
            part.tag,
            part.subtag,
            part.description,
            annotation.description,
            part.kind,
        )
        for annotation in annotations
        for part in annotation.parts
    ]
    return pl.DataFrame(rows, schema=PARTS_SCHEMA, orient="row")


def find_orphans(store: Store, source_data: pl.DataFrame) -> list[Annotation]:
    """Annotations whose transaction no longer exists."""
    current_ids = set(source_data["id"])
    return [
        annotation
        for annotation in store.list_annotations()
        if annotation.transaction_id not in current_ids
    ]


def find_overridden(store: Store, source_data: pl.DataFrame) -> list[Annotation]:
    """Annotations whose transaction matches a rule, which overrides their
    parts. Annotations without parts (only a description) are not overridden."""
    ruled_ids = set(
        source_data.filter(rule_index_expr(store.list_rules()).is_not_null())["id"]
    )
    return [
        annotation
        for annotation in store.list_annotations()
        if annotation.transaction_id in ruled_ids and annotation.parts
    ]


def snapshot_of(row: dict) -> TransactionSnapshot:
    return TransactionSnapshot(
        account=row["account"],
        source=row["source"],
        date=row["date"],
        amount=to_minor_units(row["amount"]),
        raw_description=row["raw_description"],
    )
