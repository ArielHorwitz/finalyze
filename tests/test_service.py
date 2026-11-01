"""Unit tests of the service layer, on the demo data (see `demo_template`)."""

import datetime

import polars as pl
import pydantic
import pytest

from finalyze.config import Filters, host
from finalyze.service import (
    BatchError,
    Conflict,
    InvalidRequest,
    NotFound,
    load_ingested,
    models,
)
from finalyze.source.raw import load_source_data


@pytest.fixture(autouse=True)
def without_annotations(unannotated_demo):
    """These tests start from the demo's rules only, without its annotations."""


def transactions(service, **query) -> list[models.Transaction]:
    request = models.TransactionListRequest(limit=None, **query)
    return service.list_transactions(request).items


def first_untagged(service, search: str) -> models.Transaction:
    return transactions(service, status="untagged", search=search)[0]


def tag_parts(tag: str, subtag: str, **part) -> list[models.PartIn]:
    return [models.PartIn(tag=tag, subtag=subtag, **part)]


def set_tags(service, transaction_id, tag, subtag, **request):
    return service.set_annotation(
        transaction_id,
        models.AnnotationSetRequest(parts=tag_parts(tag, subtag), **request),
    )


# Status and transactions


def test_status(service):
    status = service.status()
    data = load_source_data()
    assert status.transactions == data.height
    assert status.untagged > 0
    assert status.untagged + status.rule_matched == status.transactions
    assert (status.annotated, status.overridden, status.orphans) == (0, 0, 0)
    assert status.accounts == ["cash", "eitan", "loan", "maya", "savings"]
    assert status.db_path == str(host().db_file)
    assert status.ingested_changed_at is not None
    assert status.ingested_checked_at is not None


def test_untagged_are_those_without_rules(service):
    untagged = transactions(service, status="untagged")
    descriptions = {item.raw_description for item in untagged}
    assert "FRESHWAY MARKET 12" in descriptions and "SHOPGLOBE.COM" in descriptions
    assert all(item.rule is None and item.annotation is None for item in untagged)
    ruled = transactions(service, status="rule")
    assert all(item.rule is not None for item in ruled)
    assert not {item.raw_description for item in ruled} & descriptions
    tagged = transactions(service, status="tagged")
    assert len(tagged) == len(ruled)


def test_query_filters(service):
    everything = transactions(service)
    start, end = datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)
    january = transactions(service, start_date=start, end_date=end)
    assert january and all(start <= item.date < end for item in january)
    assert len(january) == sum(start <= item.date < end for item in everything)
    kim = transactions(service, accounts=["eitan"], sources=["card"])
    assert kim and all((item.account, item.source) == ("eitan", "card") for item in kim)
    searched = transactions(service, search="freshway market")
    assert searched and all(
        item.raw_description == "FRESHWAY MARKET 12" for item in searched
    )
    amounts = transactions(service, min_amount=-100, max_amount=-50)
    assert amounts and all(-100 <= item.amount <= -50 for item in amounts)
    water = transactions(service, tags=["utilities"], subtags=["water"])
    assert water and all(item.raw_description == "מי נווה ארזים" for item in water)
    assert transactions(service, tags=["utilities"], subtags=["groceries"]) == []
    some_ids = [item.id for item in everything[:3]]
    assert [item.id for item in transactions(service, ids=some_ids)] == some_ids


def test_sort_and_pagination(service):
    request = models.TransactionListRequest(sort="amount", descending=True, limit=5)
    page = service.list_transactions(request)
    assert page.total == load_source_data().height
    assert len(page.items) == 5
    amounts = [item.amount for item in page.items]
    assert amounts == sorted(amounts, reverse=True)
    next_page = service.list_transactions(request.model_copy(update={"offset": 5}))
    assert next_page.items[0].amount <= amounts[-1]
    assert not {item.id for item in page.items} & {item.id for item in next_page.items}


def test_sort_by_description(service):
    # What the web app shows: the description, else the parts', else the raw one
    described = first_untagged(service, "FRESHWAY MARKET")
    service.set_annotation(
        described.id, models.AnnotationSetRequest(description="aardvark food")
    )
    by_part = first_untagged(service, "SHOPGLOBE")
    service.set_annotation(
        by_part.id,
        models.AnnotationSetRequest(
            parts=tag_parts("food", "groceries", description="Ab part")
        ),
    )
    shown = {described.id: "aardvark food", by_part.id: "Ab part"}
    items = transactions(service, sort="description")
    texts = [shown.get(item.id, item.raw_description).casefold() for item in items]
    assert texts == sorted(texts)
    assert [item.id for item in items[:2]] == [described.id, by_part.id]


def test_get_transaction(service):
    item = transactions(service)[0]
    assert service.get_transaction(item.id) == item
    with pytest.raises(NotFound):
        service.get_transaction("nope")


def test_guesses(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    # Nothing with the same description is tagged
    assert target.guess is None
    set_tags(service, target.id, "food", "groceries")
    other = first_untagged(service, "FRESHWAY MARKET")
    assert other.guess == models.Guess(
        tag="food",
        subtag="groceries",
        basis="same_raw_description",
        from_transaction_id=target.id,
    )
    # Tagged transactions have no guess
    assert service.get_transaction(target.id).guess is None


# Annotations


def test_set_annotation_is_visible_immediately(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    untagged_before = service.status().untagged
    updated = set_tags(service, target.id, "food", "groceries", description="weekly")
    assert updated.status == "annotated"
    assert updated.annotation == models.AnnotationOut(
        description="weekly",
        parts=[
            models.PartOut(
                amount=target.amount,
                tag="food",
                subtag="groceries",
                kind="regular",
                kind_set=False,
            )
        ],
        authors=["owner"],
        needs_review=False,
    )
    assert service.status().untagged == untagged_before - 1
    tagged = transactions(service, tags=["food"], subtags=["groceries"])
    assert [item.id for item in tagged] == [target.id]
    assert transactions(service, search="WEEKLY")[0].id == target.id


def test_unknown_tags_are_refused_unless_allowed(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    with pytest.raises(InvalidRequest, match="Unknown tags: food/grocery"):
        set_tags(service, target.id, "food", "grocery")
    assert service.get_transaction(target.id).annotation is None
    set_tags(service, target.id, "food", "grocery", allow_new_tags=True)
    assert service.store.has_subtag("food", "grocery")


def test_splits(service):
    target = first_untagged(service, "SHOPGLOBE")
    half = round(target.amount / 2, 2)
    parts = [
        models.PartIn(
            tag="shopping", subtag="electronics", amount=half, description="tv"
        ),
        models.PartIn(
            tag="gifts", subtag="birthdays", amount=round(target.amount - half, 2)
        ),
    ]
    updated = service.set_annotation(
        target.id, models.AnnotationSetRequest(parts=parts)
    )
    assert [part.amount for part in updated.annotation.parts] == [
        part.amount for part in parts
    ]
    assert updated.annotation.parts[0].description == "tv"
    assert updated.status == "annotated"
    for bad_parts, message in (
        ([parts[0]], "Parts sum to"),
        ([parts[0], parts[1].model_copy(update={"amount": None})], "needs an amount"),
        (
            [
                parts[0].model_copy(update={"amount": half + 0.001}),
                parts[1].model_copy(update={"amount": target.amount - half - 0.001}),
            ],
            "decimal places",
        ),
    ):
        with pytest.raises(InvalidRequest, match=message):
            service.set_annotation(
                target.id, models.AnnotationSetRequest(parts=bad_parts)
            )
    with pytest.raises(NotFound):
        set_tags(service, "nope", "gifts", "birthdays")


def test_batch_is_atomic_with_item_errors(service):
    first, second = transactions(service, status="untagged")[:2]
    items = [
        models.BatchItem(id=first.id, parts=tag_parts("food", "groceries")),
        models.BatchItem(id="nope", parts=tag_parts("food", "groceries")),
        models.BatchItem(id=second.id, parts=tag_parts("food", "typo")),
        models.BatchItem(id=first.id, parts=tag_parts("food", "groceries")),
    ]
    with pytest.raises(BatchError) as raised:
        service.set_annotations(models.BatchRequest(items=items))
    assert [(error.index, error.id) for error in raised.value.errors] == [
        (1, "nope"),
        (2, second.id),
        (3, first.id),
    ]
    assert "Duplicate" in raised.value.errors[2].message
    assert service.status().annotated == 0
    valid = [items[0], models.BatchItem(id=second.id, parts=items[0].parts)]
    dry = service.set_annotations(models.BatchRequest(items=valid, dry_run=True))
    assert dry.dry_run and dry.updated_ids == [first.id, second.id]
    assert service.status().annotated == 0
    result = service.set_annotations(models.BatchRequest(items=valid))
    assert result.updated_ids == [first.id, second.id]
    assert service.status().annotated == 2


def test_clear_annotations(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    set_tags(service, target.id, "food", "groceries")
    result = service.clear_annotations(models.ClearRequest(ids=[target.id, "nope"]))
    assert result == models.ClearResult(
        cleared_ids=[target.id], not_annotated_ids=["nope"]
    )
    assert service.get_transaction(target.id).status == "untagged"
    set_tags(service, target.id, "food", "groceries")
    assert service.clear_annotation(target.id).annotation is None


def test_bulk_annotate(service):
    supermart = transactions(service, search="FRESHWAY MARKET")
    described = supermart[0]
    set_tags(service, described.id, "food", "dining out", description="keep me")
    request = models.BulkAnnotateRequest(
        query=models.TransactionQuery(search="FRESHWAY MARKET"),
        tag="food",
        subtag="groceries",
    )
    preview = service.bulk_annotate(request)
    assert preview.dry_run
    assert preview.affected_ids == [item.id for item in supermart]
    assert preview.replaced_ids == [described.id]
    assert preview.skipped_rule_ids == []
    assert service.status().annotated == 1
    with pytest.raises(Conflict, match="expected_ids"):
        service.bulk_annotate(
            request.model_copy(
                update={"dry_run": False, "expected_ids": preview.affected_ids[1:]}
            )
        )
    committed = service.bulk_annotate(
        request.model_copy(
            update={"dry_run": False, "expected_ids": preview.affected_ids}
        )
    )
    assert committed.affected_ids == preview.affected_ids
    groceries = transactions(service, tags=["food"], subtags=["groceries"])
    assert len(groceries) == len(supermart)
    assert service.get_transaction(described.id).annotation.description == "keep me"
    with pytest.raises(InvalidRequest, match="Unknown tags"):
        service.bulk_annotate(request.model_copy(update={"subtag": "typo"}))


def test_bulk_annotate_skips_rule_matched(service):
    request = models.BulkAnnotateRequest(
        query=models.TransactionQuery(search="זרם פלוס"),
        tag="utilities",
        subtag="electricity",
    )
    preview = service.bulk_annotate(request)
    assert preview.affected_ids == []
    assert len(preview.skipped_rule_ids) > 0


# Tags and accounts


def test_list_tags(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    set_tags(service, target.id, "food", "groceries")
    tags = {tag.name: tag for tag in service.list_tags()}
    assert tags["transfer"].kind == "transfer"
    assert tags["food"].kind == "regular"
    assert tags["food"].color == "#199e70"
    groceries = next(sub for sub in tags["food"].subtags if sub.name == "groceries")
    assert (groceries.annotations, groceries.rules, groceries.transactions) == (
        1,
        0,
        1,
    )
    assert groceries.color is None and groceries.effective_color == "#199e70"
    water = next(sub for sub in tags["utilities"].subtags if sub.name == "water")
    assert water.rules == 1 and water.transactions > 0 and water.annotations == 0
    assert tags["utilities"].transactions >= water.transactions


def test_tag_properties(service):
    with pytest.raises(Conflict):
        service.create_tag(models.TagCreate(tag="food"))
    created = service.create_tag(models.TagCreate(tag="pets", subtag="", color="red"))
    assert created.subtags[0].name == "" and created.subtags[0].color == "#f00"
    update = models.TagPropertiesUpdate(tag="pets", subtag="", kind="one-off")
    subtag = service.update_tag_properties(update).subtags[0]
    assert subtag.kind == subtag.effective_kind == "one-off"
    assert subtag.color == "#f00"
    tag = service.update_tag_properties(
        models.TagPropertiesUpdate(tag="pets", kind="saving")
    )
    assert tag.kind == "saving" and tag.subtags[0].effective_kind == "one-off"
    inherit = models.TagPropertiesUpdate(tag="pets", subtag="", kind=None)
    subtag = service.update_tag_properties(inherit).subtags[0]
    assert subtag.kind is None and subtag.effective_kind == "saving"
    with pytest.raises(InvalidRequest, match="can't be null"):
        service.update_tag_properties(models.TagPropertiesUpdate(tag="pets", kind=None))
    with pytest.raises(pydantic.ValidationError):
        models.TagPropertiesUpdate(tag="pets", kind="excluded")
    with pytest.raises(InvalidRequest, match="Invalid color"):
        service.update_tag_properties(models.TagPropertiesUpdate(tag="pets", color="?"))
    with pytest.raises(NotFound):
        service.update_tag_properties(models.TagPropertiesUpdate(tag="nope"))


def test_rename_merge_and_delete_tags(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    set_tags(service, target.id, "food", "groceries")
    renamed = service.rename_tag(models.TagRename(tag="food", new_name="meals"))
    assert renamed.name == "meals"
    assert service.get_transaction(target.id).annotation.parts[0].tag == "meals"
    service.rename_tag(models.TagRename(tag="meals", subtag="groceries", new_name="g"))
    merged = service.merge_tag(
        models.TagMerge(
            tag="meals", subtag="g", into_tag="shopping", into_subtag="home"
        )
    )
    assert merged.name == "shopping"
    assert service.get_transaction(target.id).annotation.parts[0].subtag == "home"
    with pytest.raises(InvalidRequest, match="into_subtag"):
        service.merge_tag(
            models.TagMerge(tag="meals", subtag="dining out", into_tag="x")
        )
    with pytest.raises(Conflict, match="used by"):
        service.delete_tag(models.TagRef(tag="shopping"))
    service.delete_tag(models.TagRef(tag="meals"))
    assert "meals" not in {tag.name for tag in service.list_tags()}
    with pytest.raises(NotFound):
        service.delete_tag(models.TagRef(tag="meals"))


def test_accounts(service):
    accounts = {account.name: account for account in service.list_accounts()}
    savings = accounts["savings"]
    assert (savings.group, savings.group_role, savings.color) == (
        "savings",
        "holding",
        "#f2a05c",
    )
    assert (accounts["maya"].group, accounts["maya"].group_role) == (
        "spending",
        "spending",
    )
    assert (
        accounts["maya"].transactions
        == load_source_data().filter(pl.col("account") == "maya").height
    )
    updated = service.update_account(
        "eitan", models.AccountUpdate(group="savings", color="blue")
    )
    assert updated.group == "savings" and updated.group_role == "holding"
    assert updated.color == "#00f"
    updated = service.update_account("eitan", models.AccountUpdate(color=None))
    assert updated.group == "savings" and updated.color is None
    with pytest.raises(NotFound):
        service.update_account("nope", models.AccountUpdate(group="savings"))
    with pytest.raises(NotFound, match="No such group"):
        service.update_account("eitan", models.AccountUpdate(group="nope"))
    with pytest.raises(InvalidRequest, match="can't be null"):
        service.update_account("eitan", models.AccountUpdate(group=None))
    # Transactions carry their account's group
    eitan = transactions(service, accounts=["eitan"])[0]
    assert (eitan.group, eitan.group_role) == ("savings", "holding")


def test_groups(service):
    groups = service.list_groups()
    assert [(group.name, group.role, group.position) for group in groups] == [
        ("spending", "spending", 0),
        ("savings", "holding", 1),
        ("debt", "holding", 2),
    ]
    assert groups[0].accounts == ["cash", "eitan", "maya"]
    created = service.create_group(
        models.GroupCreate(name="pension", role="holding", color="red", position=1)
    )
    assert (created.position, created.color, created.accounts) == (1, "#f00", [])
    with pytest.raises(Conflict):
        service.create_group(models.GroupCreate(name="pension"))
    updated = service.update_group(
        "pension", models.GroupUpdate(new_name="loans", role="spending", position=3)
    )
    assert (updated.name, updated.role, updated.position) == ("loans", "spending", 3)
    with pytest.raises(Conflict):
        service.update_group("loans", models.GroupUpdate(new_name="savings"))
    with pytest.raises(InvalidRequest, match="can't be null"):
        service.update_group("loans", models.GroupUpdate(role=None))
    with pytest.raises(Conflict, match="move them first"):
        service.delete_group("savings")
    service.delete_group("loans")
    with pytest.raises(NotFound):
        service.delete_group("loans")


# Rules


def test_rules_crud(service):
    rules = service.list_rules()
    assert [rule.position for rule in rules] == list(range(len(rules)))
    created = service.create_rule(
        models.RuleCreate(
            filters=Filters(raw_description="FRESHWAY MARKET"),
            tag="food",
            subtag="groceries",
            position=0,
        )
    )
    assert created.position == 0 and created.transactions > 0
    assert (
        service.get_transaction(first_untagged(service, "KIDZONE").id).status
        == "untagged"
    )
    assert transactions(service, search="FRESHWAY MARKET")[0].status == "rule"
    with pytest.raises(InvalidRequest, match="tags or subtags"):
        service.create_rule(
            models.RuleCreate(filters=Filters(tags=["food"]), tag="a", subtag="b")
        )
    with pytest.raises(InvalidRequest, match="every transaction"):
        service.create_rule(models.RuleCreate(filters=Filters(), tag="a", subtag="b"))
    with pytest.raises(InvalidRequest, match="Unknown tags"):
        service.create_rule(
            models.RuleCreate(filters=Filters(raw_description="x"), tag="a", subtag="b")
        )
    updated = service.update_rule(
        created.id,
        models.RuleUpdate(filters=Filters(raw_description="KIDZONE"), position=3),
    )
    assert updated.position == 3 and updated.filters.raw_description == "KIDZONE"
    with pytest.raises(InvalidRequest, match="together"):
        service.update_rule(created.id, models.RuleUpdate(tag="food"))
    reordered = service.reorder_rules(
        models.RuleReorder(
            rule_ids=[rule.id for rule in reversed(service.list_rules())]
        )
    )
    assert reordered[0].id == rules[-1].id
    with pytest.raises(InvalidRequest, match="exactly once"):
        service.reorder_rules(models.RuleReorder(rule_ids=[created.id]))
    service.delete_rule(created.id)
    assert created.id not in {rule.id for rule in service.list_rules()}
    with pytest.raises(NotFound):
        service.delete_rule(created.id)


def test_rule_preview(service):
    annotated = first_untagged(service, "FRESHWAY MARKET")
    set_tags(service, annotated.id, "food", "groceries")
    payroll = transactions(service, search="משכורת - אורבית")
    preview = service.preview_rule(
        models.RulePreviewRequest(
            filters=Filters(raw_description="FRESHWAY MARKET|אורבית")
        )
    )
    supermart = transactions(service, search="FRESHWAY MARKET")
    assert set(preview.matched_ids) == {item.id for item in supermart + payroll}
    # Payroll is matched by an earlier rule
    assert set(preview.effective_ids) == {item.id for item in supermart}
    assert set(preview.newly_tagged_ids) == {item.id for item in supermart} - {
        annotated.id
    }
    assert preview.overridden_annotation_ids == [annotated.id]
    # First in order, it takes over the payroll too
    first = service.preview_rule(
        models.RulePreviewRequest(
            filters=Filters(raw_description="FRESHWAY MARKET|אורבית"), position=0
        )
    )
    assert set(first.effective_ids) == set(preview.matched_ids)
    # Changing an existing rule leaves it out
    salary_rule = service.list_rules()[0]
    changed = service.preview_rule(
        models.RulePreviewRequest(
            filters=Filters(raw_description="Nothing matches this"),
            rule_id=salary_rule.id,
        )
    )
    assert changed.matched_ids == []


# Orphans, overridden annotations and ingest


def wallet_file():
    """The cash wallet's hand-written raw file."""
    return host().raw_dir.joinpath("cash").joinpath("wallet.csv")


def remove_raw_rows(description: str):
    """Remove the wallet's raw rows with a description, as if never written."""
    raw = pl.read_csv(wallet_file(), infer_schema=False)
    raw.filter(pl.col("raw_description") != description).write_csv(wallet_file())


def test_ingest_reports_vanished_and_orphans(service):
    target = first_untagged(service, "Babysitter")
    set_tags(service, target.id, "kids", "babysitting")
    babysitter = {item.id for item in transactions(service, search="Babysitter")}
    remove_raw_rows("Babysitter (Noa)")
    # Reads use the last ingested data until the next ingest
    assert service.get_transaction(target.id).id == target.id
    summary = service.ingest()
    assert summary.new_ids == []
    assert set(summary.vanished_ids) == babysitter
    assert summary.new_orphan_ids == [target.id]
    assert summary.orphans == 1
    assert summary.transactions == load_ingested().height
    assert sum(summary.accounts.values()) == summary.transactions
    orphans = service.list_orphans()
    assert [orphan.transaction_id for orphan in orphans] == [target.id]
    assert orphans[0].snapshot.raw_description == "Babysitter (Noa)"
    assert orphans[0].parts[0].tag == "kids"
    with pytest.raises(InvalidRequest, match="Not orphaned"):
        service.delete_orphans(models.OrphanDelete(ids=[target.id, "other"]))
    result = service.delete_orphans(models.OrphanDelete(ids=[target.id]))
    assert result.deleted_ids == [target.id]
    assert service.list_orphans() == []


def test_failed_ingest_keeps_previous_data(service):
    before = load_ingested()
    wallet_file().write_text("not,a,valid\nfile,at,all\n")
    with pytest.raises(InvalidRequest, match="Ingest failed"):
        service.ingest()
    assert load_ingested().equals(before)


def test_overridden(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    set_tags(service, target.id, "food", "groceries", description="mine")
    service.create_rule(
        models.RuleCreate(
            filters=Filters(raw_description="FRESHWAY MARKET"),
            tag="food",
            subtag="groceries",
        )
    )
    overridden = service.list_overridden()
    assert [item.transaction_id for item in overridden] == [target.id]
    assert overridden[0].rule.tag == "food" and overridden[0].description == "mine"
    assert service.status().overridden == 1
    assert service.get_transaction(target.id).status == "rule"


# Analysis


def test_analysis_rows_explode_parts(service):
    target = first_untagged(service, "SHOPGLOBE")
    half = round(target.amount / 2, 2)
    parts = [
        models.PartIn(tag="shopping", subtag="electronics", amount=half),
        models.PartIn(
            tag="gifts", subtag="birthdays", amount=round(target.amount - half, 2)
        ),
    ]
    service.set_annotation(
        target.id, models.AnnotationSetRequest(parts=parts, description="n")
    )
    rows = service.analysis_rows(models.AnalysisQuery(search="shopglobe")).rows
    split_rows = [row for row in rows if row.id == target.id]
    assert sorted((row.subtag, row.amount) for row in split_rows) == [
        ("birthdays", round(target.amount - half, 2)),
        ("electronics", half),
    ]
    assert all(row.transaction_description == "n" for row in split_rows)
    assert all(row.raw_description == "SHOPGLOBE.COM" for row in rows)
    everything = service.analysis_rows(models.AnalysisQuery(all_groups=True))
    assert everything.total > len(rows)
    assert not any(row.is_sentinel_tick for row in everything.rows)
    assert {row.account for row in everything.rows} == {
        "cash",
        "eitan",
        "loan",
        "maya",
        "savings",
    }
    spending = service.analysis_rows(models.AnalysisQuery())
    assert {row.account for row in spending.rows} == {"cash", "eitan", "maya"}
    assert {(row.group, row.group_role) for row in spending.rows} == {
        ("spending", "spending")
    }
    savings = service.analysis_rows(models.AnalysisQuery(groups=["savings"]))
    assert {row.account for row in savings.rows} == {"savings"}
    transfers = service.analysis_rows(models.AnalysisQuery(kinds=["transfer"]))
    assert {row.tag for row in transfers.rows} == {"transfer"}
    assert {row.kind for row in transfers.rows} == {"transfer"}


def test_aggregate(service):
    query = models.AggregateQuery(
        kinds=["regular", "one-off"], expenses=True, group_by=["tag"]
    )
    result = service.aggregate(query)
    assert result.group_by == ["tag"]
    by_tag = {row["tag"]: row for row in result.rows}
    rows = service.analysis_rows(
        models.AnalysisQuery(
            kinds=["regular", "one-off"], expenses=True, tags=["utilities"]
        )
    ).rows
    assert by_tag["utilities"]["count"] == len(rows)
    assert by_tag["utilities"]["amount"] == pytest.approx(
        sum(row.amount for row in rows)
    )
    assert "transfer" not in by_tag
    by_kind = service.aggregate(
        models.AggregateQuery(all_groups=True, group_by=["group", "kind"])
    )
    assert {(row["group"], row["kind"]) for row in by_kind.rows} == {
        ("spending", "regular"),
        ("spending", "transfer"),
        ("spending", "saving"),
        ("spending", "reconciliation"),
        ("savings", "regular"),
        ("savings", "transfer"),
        ("savings", "reconciliation"),
        ("savings", "saving"),
        ("debt", "transfer"),
    }
    # Incomes and expenses are of regular and one-off tags, as in the dashboard
    expenses = service.aggregate(
        models.AggregateQuery(expenses=True, group_by=["kind"])
    )
    # (without annotations, none is one-off)
    assert {row["kind"] for row in expenses.rows} == {"regular"}
    total = service.aggregate(models.AggregateQuery(group_by=[]))
    assert len(total.rows) == 1
    monthly = service.aggregate(models.AggregateQuery())
    assert [row["month"] for row in monthly.rows] == sorted(
        row["month"] for row in monthly.rows
    )


def test_analysis_without_ingested_data(use_host, tmp_path):
    from finalyze.service import open_service
    from tests.helpers import make_isolated_host

    use_host(make_isolated_host(tmp_path.joinpath("empty")))
    with open_service() as service:
        assert service.status().transactions == 0
        assert service.status().ingested_changed_at is None
        assert service.status().ingested_checked_at is None
        assert transactions(service) == []
        with pytest.raises(Conflict, match="ingest"):
            service.analysis_rows(models.AnalysisQuery())


# Provenance, review and descriptions only


def test_authors_and_review(service):
    first, second, third = transactions(service, status="untagged")[:3]
    service.set_annotations(
        models.BatchRequest(
            author="claude",
            items=[
                models.BatchItem(id=first.id, parts=tag_parts("food", "groceries")),
                models.BatchItem(
                    id=second.id,
                    parts=tag_parts("food", "groceries"),
                    needs_review=True,
                    description="unsure: could be a gift",
                ),
            ],
        )
    )
    set_tags(service, third.id, "food", "groceries")
    assert service.get_transaction(first.id).annotation.authors == ["claude"]
    assert service.get_transaction(third.id).annotation.authors == ["owner"]
    flagged = transactions(service, needs_review=True)
    assert [item.id for item in flagged] == [second.id]
    assert flagged[0].annotation.needs_review
    assert second.id not in {
        item.id for item in transactions(service, needs_review=False)
    }
    by_claude = transactions(service, authors=["claude"])
    assert {item.id for item in by_claude} == {first.id, second.id}
    assert service.status().needs_review == 1
    # Clearing the flag changes no content, so it keeps the authors
    result = service.review(models.ReviewRequest(ids=[second.id]))
    assert result == models.ReviewResult(needs_review=False, updated_ids=[second.id])
    reviewed = service.get_transaction(second.id).annotation
    assert (reviewed.needs_review, reviewed.authors) == (False, ["claude"])
    assert service.status().needs_review == 0
    service.review(models.ReviewRequest(ids=[first.id], needs_review=True))
    assert service.status().needs_review == 1
    with pytest.raises(NotFound, match="No annotation"):
        service.review(models.ReviewRequest(ids=[first.id, "nope"]))
    assert service.status().needs_review == 1


def test_only_content_changes_add_their_author(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    parts = tag_parts("food", "groceries")
    set_tags(service, target.id, "food", "groceries", description="weekly")

    def authors():
        return service.get_transaction(target.id).annotation.authors

    # Changing only the review flag, or nothing, by every path, adds no author
    patch(service, target.id, needs_review=True, author="claude")
    service.patch_annotations(
        models.PatchBatchRequest(
            items=[models.PatchBatchItem(id=target.id, needs_review=False)],
            author="claude",
        )
    )
    service.set_annotation(
        target.id,
        models.AnnotationSetRequest(
            parts=parts, description="weekly", needs_review=True, author="claude"
        ),
    )
    service.set_annotations(
        models.BatchRequest(
            items=[
                models.BatchItem(
                    id=target.id, parts=parts, description="weekly", needs_review=False
                )
            ],
            author="claude",
        )
    )
    bulk(service, "FRESHWAY MARKET", needs_review=True, author="claude")
    service.review(models.ReviewRequest(ids=[target.id]))
    patch(service, target.id, description="weekly", author="claude")
    service.set_kind(models.KindRequest(ids=[target.id], kind=None, author="claude"))
    assert authors() == ["owner"]
    assert not service.get_transaction(target.id).annotation.needs_review
    # Changing content along with the flag adds the author
    patch(service, target.id, description="monthly", needs_review=True, author="ada")
    assert authors() == ["owner", "ada"]
    service.set_kind(models.KindRequest(ids=[target.id], kind="one-off", author="bob"))
    assert authors() == ["owner", "ada", "bob"]
    bulk(service, "FRESHWAY MARKET", subtag="dining out", tag="food", author="cy")
    assert authors() == ["owner", "ada", "bob", "cy"]
    service.set_annotations(
        models.BatchRequest(
            items=[models.BatchItem(id=target.id, parts=parts)], author="dee"
        )
    )
    assert authors() == ["owner", "ada", "bob", "cy", "dee"]


def test_bulk_annotate_author_and_review(service):
    request = models.BulkAnnotateRequest(
        query=models.TransactionQuery(search="KIDZONE"),
        tag="food",
        subtag="groceries",
        author="claude",
        needs_review=True,
        dry_run=False,
    )
    result = service.bulk_annotate(request)
    flagged = transactions(service, needs_review=True, authors=["claude"])
    assert [item.id for item in flagged] == result.affected_ids


def test_description_only_annotations(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    untagged_before = service.status().untagged
    described = service.set_annotation(
        target.id,
        models.AnnotationSetRequest(description="ask the owner", needs_review=True),
    )
    assert described.status == "untagged"
    assert (
        described.annotation.parts == []
        and described.annotation.description == "ask the owner"
    )
    assert service.status().untagged == untagged_before
    assert service.status().annotated == 0
    assert target.id in {item.id for item in transactions(service, status="untagged")}
    assert transactions(service, search="ask the owner")[0].id == target.id
    with pytest.raises(InvalidRequest, match="needs a description"):
        service.set_annotation(target.id, models.AnnotationSetRequest())
    # Analysis keeps it untagged, with its description
    rows = service.analysis_rows(models.AnalysisQuery(search="ask the owner")).rows
    assert [(row.id, row.tag, row.transaction_description) for row in rows] == [
        (target.id, None, "ask the owner")
    ]
    # Not overridden by a rule
    service.create_rule(
        models.RuleCreate(
            filters=Filters(raw_description="FRESHWAY MARKET"),
            tag="food",
            subtag="groceries",
        )
    )
    assert service.list_overridden() == []


# Kinds set on transactions


def test_kind_set_on_a_transaction(service):
    target = first_untagged(service, "SHOPGLOBE")
    updated = service.set_annotation(
        target.id,
        models.AnnotationSetRequest(
            parts=tag_parts("shopping", "electronics", kind="one-off"),
            allow_new_tags=True,
        ),
    )
    part = updated.annotation.parts[0]
    assert (part.kind, part.kind_set) == ("one-off", True)
    rows = service.analysis_rows(models.AnalysisQuery(search="SHOPGLOBE")).rows
    by_id = {row.id: row for row in rows}
    assert (by_id[target.id].kind, by_id[target.id].kind_set) == ("one-off", True)
    others = [row for row in rows if row.id != target.id]
    assert others and not any(row.kind_set for row in others)
    # Filters
    assert [item.id for item in transactions(service, kind_set=True)] == [target.id]
    assert target.id not in {item.id for item in transactions(service, kind_set=False)}
    one_offs = {item.id for item in transactions(service, kinds=["one-off"])}
    assert target.id in one_offs
    assert target.id not in {
        item.id for item in transactions(service, kinds=["regular"])
    }
    set_rows = service.analysis_rows(models.AnalysisQuery(kind_set=True)).rows
    assert [row.id for row in set_rows] == [target.id]


def test_set_kind(service):
    first, second = transactions(service, status="untagged", search="FRESHWAY MARKET")[
        :2
    ]
    set_tags(service, first.id, "food", "groceries", description="weekly")
    service.set_annotation(
        second.id,
        models.AnnotationSetRequest(
            parts=[
                models.PartIn(tag="food", subtag="groceries", amount=-10),
                models.PartIn(
                    tag="food", subtag="groceries", amount=second.amount + 10
                ),
            ]
        ),
    )
    result = service.set_kind(
        models.KindRequest(ids=[first.id, second.id], kind="one-off")
    )
    assert result.updated_ids == [first.id, second.id]
    annotated = service.get_transaction(first.id).annotation
    # The rest of the annotation is kept
    assert annotated.description == "weekly"
    assert [(part.kind, part.kind_set) for part in annotated.parts] == [
        ("one-off", True)
    ]
    service.set_kind(models.KindRequest(ids=[second.id], kind=None, part=1))
    parts = service.get_transaction(second.id).annotation.parts
    assert [(part.kind, part.kind_set) for part in parts] == [
        ("one-off", True),
        ("regular", False),
    ]
    service.set_kind(models.KindRequest(ids=[first.id], kind=None))
    assert not service.get_transaction(first.id).annotation.parts[0].kind_set


def test_set_kind_is_all_or_none(service):
    tagged = first_untagged(service, "FRESHWAY MARKET")
    set_tags(service, tagged.id, "food", "groceries")
    untagged = first_untagged(service, "SHOPGLOBE")
    ruled = transactions(service, status="rule")[0]
    with pytest.raises(BatchError) as raised:
        service.set_kind(
            models.KindRequest(
                ids=[tagged.id, untagged.id, ruled.id, "nope"], kind="one-off"
            )
        )
    messages = [error.message for error in raised.value.errors]
    assert [error.index for error in raised.value.errors] == [1, 2, 3]
    assert "untagged" in messages[0]
    assert "rule" in messages[1]
    assert "No such transaction" in messages[2]
    assert not service.get_transaction(tagged.id).annotation.parts[0].kind_set
    with pytest.raises(BatchError) as raised:
        service.set_kind(models.KindRequest(ids=[tagged.id], kind="saving", part=1))
    assert "no part 1" in raised.value.errors[0].message


def test_bulk_annotate_sets_a_kind(service):
    request = models.BulkAnnotateRequest(
        query=models.TransactionQuery(search="KIDZONE"),
        tag="food",
        subtag="groceries",
        kind="one-off",
        dry_run=False,
    )
    result = service.bulk_annotate(request)
    assert result.affected_ids
    one_offs = transactions(service, kind_set=True, kinds=["one-off"])
    assert [item.id for item in one_offs] == result.affected_ids
    # Retagging keeps the kind set, and inherit clears it
    service.bulk_annotate(
        request.model_copy(update={"subtag": "dining out", "kind": None})
    )
    retagged = transactions(service, kind_set=True, subtags=["dining out"])
    assert [item.id for item in retagged] == result.affected_ids
    service.bulk_annotate(request.model_copy(update={"kind": "inherit"}))
    assert transactions(service, kind_set=True) == []


def bulk(service, search, **fields):
    return service.bulk_annotate(
        models.BulkAnnotateRequest(
            query=models.TransactionQuery(search=search), dry_run=False, **fields
        )
    )


def test_bulk_annotate_needs_a_field():
    query = models.TransactionQuery(search="FRESHWAY MARKET")
    with pytest.raises(pydantic.ValidationError, match="Nothing to change"):
        models.BulkAnnotateRequest(query=query)
    with pytest.raises(pydantic.ValidationError, match="go together"):
        models.BulkAnnotateRequest(query=query, tag="food")
    with pytest.raises(pydantic.ValidationError, match="go together"):
        models.BulkAnnotateRequest(query=query, subtag="groceries", kind="one-off")


def test_bulk_annotate_kind_only(service):
    supermart = transactions(service, search="FRESHWAY MARKET")
    tagged, split, untagged = supermart[0], supermart[1], supermart[2]
    set_tags(service, tagged.id, "food", "dining out", description="keep me")
    service.set_annotation(
        split.id,
        models.AnnotationSetRequest(
            parts=[
                models.PartIn(tag="food", subtag="groceries", amount=-1),
                models.PartIn(
                    tag="gifts", subtag="birthdays", amount=round(split.amount + 1, 2)
                ),
            ]
        ),
    )
    service.review(models.ReviewRequest(ids=[tagged.id], needs_review=True))
    result = bulk(service, "FRESHWAY MARKET", kind="one-off")
    assert set(result.affected_ids) == {tagged.id, split.id}
    assert untagged.id in result.skipped_untagged_ids
    assert result.replaced_ids == []
    kept = service.get_transaction(tagged.id).annotation
    assert (kept.description, kept.needs_review) == ("keep me", True)
    assert [(part.subtag, part.kind) for part in kept.parts] == [
        ("dining out", "one-off")
    ]
    # Every part of a split gets it
    parts = service.get_transaction(split.id).annotation.parts
    assert [(part.subtag, part.kind_set) for part in parts] == [
        ("groceries", True),
        ("birthdays", True),
    ]
    assert service.get_transaction(untagged.id).annotation is None


def test_bulk_annotate_description_and_review_only(service):
    supermart = transactions(service, search="FRESHWAY MARKET")
    tagged, untagged = supermart[0], supermart[1]
    set_tags(service, tagged.id, "food", "dining out")
    result = bulk(service, "FRESHWAY MARKET", description="ask", needs_review=True)
    assert len(result.affected_ids) == len(supermart)
    described = service.get_transaction(untagged.id)
    assert described.status == "untagged" and described.annotation.description == "ask"
    assert described.annotation.needs_review
    kept = service.get_transaction(tagged.id).annotation
    assert kept.parts[0].subtag == "dining out" and kept.description == "ask"
    # Clearing the description deletes a description-only annotation, keeps tags
    result = bulk(service, "FRESHWAY MARKET", description="")
    assert service.get_transaction(untagged.id).annotation is None
    kept = service.get_transaction(tagged.id).annotation
    assert kept.description is None and kept.needs_review
    # The review flag alone skips transactions without tags or a description
    result = bulk(service, "FRESHWAY MARKET", needs_review=False, author="claude")
    assert result.affected_ids == [tagged.id]
    assert untagged.id in result.skipped_untagged_ids
    kept = service.get_transaction(tagged.id).annotation
    assert not kept.needs_review and kept.authors == ["owner"]


def test_bulk_annotate_leaves_unchanged_alone(service):
    request = models.BulkAnnotateRequest(
        query=models.TransactionQuery(search="KIDZONE"),
        tag="food",
        subtag="groceries",
        author="claude",
        dry_run=False,
    )
    first = service.bulk_annotate(request)
    again = service.bulk_annotate(request.model_copy(update={"author": "owner"}))
    assert again.affected_ids == [] and again.unchanged_ids == first.affected_ids
    authors = {
        tuple(item.annotation.authors)
        for item in transactions(service, search="KIDZONE")
    }
    assert authors == {("claude",)}


def test_bulk_annotate_skips_rules_for_every_field(service):
    result = bulk(service, "זרם פלוס", description="hi")
    assert result.affected_ids == [] and result.skipped_rule_ids


# Patches: only the given fields change


def patch(service, transaction_id, **fields):
    return service.patch_annotation(
        transaction_id, models.AnnotationPatchRequest(**fields)
    )


def test_patch_keeps_the_fields_left_out(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    set_tags(
        service, target.id, "food", "groceries", description="weekly", needs_review=True
    )
    patched = patch(service, target.id, description="weekly shop", author="claude")
    annotation = patched.annotation
    assert annotation.description == "weekly shop"
    assert [part.subtag for part in annotation.parts] == ["groceries"]
    assert annotation.needs_review
    assert annotation.authors == ["owner", "claude"]
    # The review flag alone, keeping the description and tags
    annotation = patch(service, target.id, needs_review=False).annotation
    assert (annotation.description, annotation.needs_review) == ("weekly shop", False)
    assert [part.subtag for part in annotation.parts] == ["groceries"]
    assert annotation.authors == ["owner", "claude"]


def test_patch_null_clears(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    set_tags(service, target.id, "food", "groceries", description="weekly")
    patch(service, target.id, kind="one-off")
    assert service.get_transaction(target.id).annotation.parts[0].kind_set
    annotation = patch(service, target.id, kind=None).annotation
    assert not annotation.parts[0].kind_set
    # Null parts untag it, keeping the description
    untagged = patch(service, target.id, parts=None)
    assert untagged.status == "untagged"
    assert untagged.annotation.description == "weekly"
    # Clearing the rest too would leave nothing: clear the annotation instead
    with pytest.raises(InvalidRequest, match="clear it instead"):
        patch(service, target.id, description=None)
    set_tags(service, target.id, "food", "groceries", description="weekly")
    assert patch(service, target.id, description=None).annotation.description is None


def test_patch_needs_a_valid_field():
    with pytest.raises(pydantic.ValidationError, match="Nothing to change"):
        models.AnnotationPatchRequest(author="claude")
    with pytest.raises(pydantic.ValidationError, match="can't be null"):
        models.AnnotationPatchRequest(needs_review=None)
    with pytest.raises(pydantic.ValidationError, match="not both"):
        models.AnnotationPatchRequest(
            parts=tag_parts("food", "groceries"), kind="one-off"
        )


def test_patch_validates_parts(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    patch(service, target.id, description="weekly", needs_review=True)
    half = round(target.amount / 2, 2)
    split = [
        models.PartIn(tag="food", subtag="groceries", amount=half),
        models.PartIn(tag="food", subtag="dining out", amount=half - 1),
    ]
    with pytest.raises(InvalidRequest, match="Parts sum to"):
        patch(service, target.id, parts=split)
    with pytest.raises(InvalidRequest, match="Unknown tags"):
        patch(service, target.id, parts=tag_parts("food", "typo"))
    split[1] = split[1].model_copy(update={"amount": round(target.amount - half, 2)})
    annotation = patch(service, target.id, parts=split).annotation
    assert [part.amount for part in annotation.parts] == [
        half,
        round(target.amount - half, 2),
    ]
    assert (annotation.description, annotation.needs_review) == ("weekly", True)


def test_patch_kind_needs_tags_of_the_annotation(service):
    untagged = first_untagged(service, "FRESHWAY MARKET")
    with pytest.raises(InvalidRequest, match="untagged"):
        patch(service, untagged.id, kind="one-off")
    ruled = transactions(service, status="rule")[0]
    with pytest.raises(InvalidRequest, match="rule"):
        patch(service, ruled.id, kind="one-off")
    # A description over a rule is fine, as with a full write
    assert patch(service, ruled.id, description="hi").annotation.description == "hi"
    with pytest.raises(NotFound):
        patch(service, "nope", description="hi")


def test_patch_creates_an_annotation(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    created = patch(service, target.id, description="ask", author="claude")
    assert created.annotation == models.AnnotationOut(
        description="ask", parts=[], authors=["claude"], needs_review=False
    )


def test_patch_batch_is_all_or_none(service):
    first, second = transactions(service, status="untagged", search="FRESHWAY")[:2]
    set_tags(service, first.id, "food", "groceries", needs_review=True)
    items = [
        models.PatchBatchItem(id=first.id, description="weekly"),
        models.PatchBatchItem(id=second.id, kind="one-off"),
        models.PatchBatchItem(id="nope", description="hi"),
        models.PatchBatchItem(id=first.id, needs_review=False),
    ]
    with pytest.raises(BatchError) as raised:
        service.patch_annotations(models.PatchBatchRequest(items=items))
    assert [(error.index, error.id) for error in raised.value.errors] == [
        (1, second.id),
        (2, "nope"),
        (3, first.id),
    ]
    assert "Duplicate" in raised.value.errors[2].message
    assert service.get_transaction(first.id).annotation.description is None
    items = [
        models.PatchBatchItem(id=first.id, description="weekly"),
        models.PatchBatchItem(id=second.id, parts=tag_parts("food", "groceries")),
    ]
    request = models.PatchBatchRequest(items=items, author="claude", dry_run=True)
    result = service.patch_annotations(request)
    assert result == models.BatchResult(dry_run=True, updated_ids=[first.id, second.id])
    assert service.get_transaction(first.id).annotation.description is None
    service.patch_annotations(request.model_copy(update={"dry_run": False}))
    annotation = service.get_transaction(first.id).annotation
    assert (annotation.description, annotation.needs_review) == ("weekly", True)
    assert annotation.authors == ["owner", "claude"]
    assert service.get_transaction(second.id).annotation.authors == ["claude"]


def test_every_write_adds_its_author_once(service):
    target = first_untagged(service, "FRESHWAY MARKET")
    set_tags(service, target.id, "food", "groceries", author="claude")
    set_tags(service, target.id, "food", "dining out")
    set_tags(service, target.id, "food", "groceries", author="claude")
    assert service.get_transaction(target.id).annotation.authors == ["claude", "owner"]
    service.set_kind(models.KindRequest(ids=[target.id], kind="one-off", author="ada"))
    authors = service.get_transaction(target.id).annotation.authors
    assert authors == ["claude", "owner", "ada"]
    # Filtered by any of them
    for author in authors:
        assert target.id in {
            item.id for item in transactions(service, authors=[author])
        }
    assert transactions(service, authors=["nobody"]) == []
