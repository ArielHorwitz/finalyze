"""Request and response models of the service layer.

They are the HTTP API's schema. Amounts are in currency units (e.g. 12.34), and
amounts that are written must not be finer than currency precision.
"""

import datetime
from typing import Annotated, Any, Literal, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from finalyze.config import Config, Filters
from finalyze.store import GroupRole, TagKind

TransactionStatus = Literal["untagged", "annotated", "rule"]
StatusFilter = Literal["untagged", "annotated", "rule", "tagged"]
SortField = Literal[
    "date", "amount", "description", "raw_description", "account", "source", "id"
]
GroupField = Literal[
    "month",
    "tag",
    "subtag",
    "tags",
    "account",
    "source",
    "account_source",
    "group",
    "group_role",
    "kind",
    "kind_set",
]
KIND_DESCRIPTION = (
    "What the tag's transactions are. `regular`: incomes and expenses. "
    "`transfer`: money moving between own accounts, never incomes or expenses "
    "but part of balances. `saving`: money set aside, not an expense but saved "
    "(in cash flows and the savings rate). `one-off`: incomes and expenses that "
    "would distort averages (e.g. a wedding), in monthly sums and breakdowns but "
    "left out of averages and trends unless included. `reconciliation`: rows "
    "that re-anchor a balance (e.g. an opening balance, or a wallet's counted "
    "cash), only in balances: never incomes or expenses, nor transfers."
)
ROLE_DESCRIPTION = (
    "`spending`: day-to-day money, in cash flows, breakdowns and the available "
    "balance. `holding`: savings, investments or debt, only in the net worth."
)


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


# Transactions


# Of the fields named description (the `_DESCRIPTION` constants describe fields)
RAW_DESCRIPTION_DOC = (
    "The transaction's text in the raw files (e.g. the bank's export), often vague."
)
DESCRIPTION_DOC = (
    "What the whole transaction really was, written by the owner or an agent. May "
    "have several lines (newlines)."
)
PART_DESCRIPTION_DOC = "What the part really was. May have several lines (newlines)."
KIND_SET_DESCRIPTION = (
    "Whether the kind is set on this transaction, over its tag's (e.g. one big "
    "purchase marked one-off)."
)


class PartOut(Model):
    amount: float
    tag: str
    subtag: str
    kind: TagKind = Field(
        description="The effective kind: set on this transaction, or its subtag's."
    )
    kind_set: bool = Field(description=KIND_SET_DESCRIPTION)
    description: Optional[str] = Field(None, description=PART_DESCRIPTION_DOC)


AUTHOR_DESCRIPTION = (
    'Who writes the annotation, e.g. "owner" or an agent\'s name. Added to its '
    "authors, unless only the review flag changes."
)
AUTHORS_DESCRIPTION = (
    'Everyone who wrote the annotation (e.g. "owner" or an agent\'s name), by first '
    "contribution. Every write that changes its content adds its author."
)
NEEDS_REVIEW_DESCRIPTION = "Flagged for the owner to review, e.g. by an unsure agent."


class AnnotationOut(Model):
    description: Optional[str] = Field(None, description=DESCRIPTION_DOC)
    parts: list[PartOut] = Field(
        description="Tagged parts, summing to the transaction amount. "
        "A single part means the whole transaction has one tag. Without parts, "
        "the annotation is only a description, and the transaction is untagged."
    )
    authors: list[str] = Field(description=AUTHORS_DESCRIPTION)
    needs_review: bool = Field(description=NEEDS_REVIEW_DESCRIPTION)


class RuleRef(Model):
    id: int
    position: int
    tag: str
    subtag: str
    kind: TagKind = Field(description="The subtag's effective kind.")


class Guess(Model):
    tag: str
    subtag: str
    basis: Literal["same_raw_description"] = Field(
        description="`same_raw_description`: the tags of the annotated transaction "
        "with the same raw description that is nearest in date (before or after, the "
        "later one among equals)."
    )
    from_transaction_id: str = Field(description="The transaction guessed from.")


class Transaction(Model):
    id: str = Field(description="Stable transaction ID.")
    account: str
    group: str = Field(description="The account's group.")
    group_role: GroupRole = Field(description=ROLE_DESCRIPTION)
    source: str
    date: datetime.date
    amount: float
    raw_description: str = Field(description=RAW_DESCRIPTION_DOC)
    status: TransactionStatus = Field(
        description="`rule`: tagged by a rule, which overrides any "
        "annotation. `annotated`: tagged by its annotation. `untagged`: neither."
    )
    annotation: Optional[AnnotationOut] = Field(
        None,
        description="The stored annotation. With status `rule`, the annotation is "
        "overridden: only its description is used.",
    )
    rule: Optional[RuleRef] = Field(None, description="The first matching rule.")
    guess: Optional[Guess] = Field(
        None,
        description="A suggestion for untagged transactions, null without an "
        "annotated transaction with the same raw description. Never applied "
        "automatically.",
    )


class TransactionPage(Model):
    total: int = Field(description="Number of matching transactions.")
    offset: int
    limit: Optional[int]
    items: list[Transaction]


class RowFilters(Model):
    """Filters shared by transaction queries and analysis rows."""

    start_date: Optional[datetime.date] = Field(None, description="Inclusive.")
    end_date: Optional[datetime.date] = Field(None, description="Exclusive.")
    accounts: Optional[list[str]] = Field(None, description="Any of these accounts.")
    sources: Optional[list[str]] = Field(None, description="Any of these sources.")
    tags: Optional[list[str]] = Field(
        None,
        description="Any of these tags. With `subtags`, both must match the same "
        "part, e.g. tags=[food] and subtags=[groceries].",
    )
    subtags: Optional[list[str]] = Field(None, description="Any of these subtags.")
    search: Optional[str] = Field(
        None,
        description="Case-insensitive text in the descriptions (the "
        "transaction's and its parts') or the raw description.",
    )
    min_amount: Optional[float] = Field(None, description="Inclusive, signed.")
    max_amount: Optional[float] = Field(None, description="Inclusive, signed.")
    kind_set: Optional[bool] = Field(
        None,
        description="With a kind set on the transaction (on any of its parts), "
        "or (false) without.",
    )


class TransactionQuery(RowFilters):
    """Transactions matching every given filter. Tag filters use the effective
    tags: a rule's tags if a rule matches, otherwise the annotation's parts."""

    ids: Optional[list[str]] = Field(None, description="Any of these IDs.")
    status: Optional[StatusFilter] = Field(
        None, description="`tagged` is either `annotated` or `rule`."
    )
    needs_review: Optional[bool] = Field(
        None,
        description="Annotations flagged for review, or (false) everything else, "
        "including transactions without an annotation.",
    )
    authors: Optional[list[str]] = Field(
        None,
        description="Annotations with any of these authors (among their authors).",
    )
    kinds: Optional[list[TagKind]] = Field(
        None,
        description="Any part of these effective kinds (set on the transaction, "
        "or its tag's). Untagged transactions are regular.",
    )


class TransactionListRequest(TransactionQuery):
    sort: SortField = "date"
    descending: bool = False
    limit: Optional[int] = Field(100, ge=1, description="Null for no limit.")
    offset: int = Field(0, ge=0)


# Annotations


class PartIn(Model):
    tag: str = Field(min_length=1)
    subtag: str = Field(description="May be empty, but must be given.")
    amount: Optional[float] = Field(
        None,
        description="Required in splits (several parts), which must sum exactly to "
        "the transaction amount. Defaults to the full amount for a single part.",
    )
    description: Optional[str] = Field(None, description=PART_DESCRIPTION_DOC)
    kind: Optional[TagKind] = Field(
        None,
        description="Set on this transaction, over its tag's kind (e.g. `one-off` "
        "for one big purchase). Null inherits the tag's.",
    )


class AnnotationIn(Model):
    description: Optional[str] = Field(None, description=DESCRIPTION_DOC)
    parts: list[PartIn] = Field(
        default_factory=list,
        description="Empty for a description only (which requires a description), "
        "leaving the transaction untagged.",
    )
    needs_review: bool = Field(False, description=NEEDS_REVIEW_DESCRIPTION)


Author = Field("owner", min_length=1, description=AUTHOR_DESCRIPTION)


class AnnotationSetRequest(AnnotationIn):
    author: str = Author
    allow_new_tags: bool = Field(
        False,
        description="Create tags and subtags that don't exist yet. Otherwise, "
        "unknown tags are errors, to avoid typos and invented tags.",
    )


class BatchItem(AnnotationIn):
    id: str = Field(description="Transaction ID.")


class BatchRequest(Model):
    items: list[BatchItem] = Field(min_length=1)
    author: str = Author
    allow_new_tags: bool = False
    dry_run: bool = Field(False, description="Only validate, without writing.")


PATCH_FIELDS = ("description", "parts", "kind", "needs_review")


class AnnotationPatch(Model):
    """Only the given fields change: one left out is kept as it is."""

    description: Optional[str] = Field(
        None,
        description=f"{DESCRIPTION_DOC} Null or empty clears it.",
    )
    parts: Optional[list[PartIn]] = Field(
        None,
        description="Replace the parts, as in a full write: a split must sum exactly "
        "to the transaction amount. Null or empty removes the tags, keeping the "
        "description.",
    )
    kind: Optional[TagKind] = Field(
        None,
        description="Set on every part, over the tags' kinds (e.g. `one-off`). Null "
        "clears it: the tags' kinds apply again. Needs tags, and not `parts` (give "
        "the kinds in the parts instead).",
    )
    needs_review: Optional[bool] = Field(
        None, description=f"{NEEDS_REVIEW_DESCRIPTION} Can't be null."
    )

    @model_validator(mode="after")
    def check_fields(self):
        given = self.model_fields_set & set(PATCH_FIELDS)
        if not given:
            raise ValueError(
                "Nothing to change: give description, parts, kind or needs_review"
            )
        if "needs_review" in given and self.needs_review is None:
            raise ValueError("needs_review can't be null: give true or false")
        if {"kind", "parts"} <= given:
            raise ValueError("Give kind or parts, not both (parts have their kinds)")
        return self


class AnnotationPatchRequest(AnnotationPatch):
    author: str = Author
    allow_new_tags: bool = False


class PatchBatchItem(AnnotationPatch):
    id: str = Field(description="Transaction ID.")


class PatchBatchRequest(Model):
    items: list[PatchBatchItem] = Field(min_length=1)
    author: str = Author
    allow_new_tags: bool = False
    dry_run: bool = Field(False, description="Only validate, without writing.")


class BatchResult(Model):
    dry_run: bool
    updated_ids: list[str] = Field(description="Updated, or valid in a dry run.")


class ItemError(Model):
    index: int = Field(description="Index of the item in the batch (line, in JSONL).")
    id: Optional[str]
    message: str


class ReviewRequest(Model):
    ids: list[str] = Field(min_length=1, description="Annotated transaction IDs.")
    needs_review: bool = Field(
        False,
        description="False marks them reviewed, true flags them. Their authors "
        "are kept: the review flag isn't content.",
    )


class ReviewResult(Model):
    needs_review: bool
    updated_ids: list[str]


class KindRequest(Model):
    ids: list[str] = Field(min_length=1, description="Tagged transaction IDs.")
    kind: Optional[TagKind] = Field(
        description="Set on the transactions, over their tags' kinds. Null clears "
        "it: they inherit their tags' kinds again."
    )
    part: Optional[int] = Field(
        None,
        ge=0,
        description="Only this part (by index) of each transaction. Null: every part.",
    )
    author: str = Author


class KindResult(Model):
    kind: Optional[TagKind]
    updated_ids: list[str]


class ClearRequest(Model):
    ids: list[str] = Field(min_length=1)


class ClearResult(Model):
    cleared_ids: list[str]
    not_annotated_ids: list[str]


INHERIT = "inherit"


class BulkAnnotateRequest(Model):
    """Every field is optional: one left out (null) is kept as it is."""

    query: TransactionQuery = Field(description="The transactions to annotate.")
    tag: Optional[str] = Field(
        None,
        min_length=1,
        description="Replace their parts (and splits) with one full-amount part of "
        "this tag and `subtag`. Null keeps their tags.",
    )
    subtag: Optional[str] = Field(None, description="Given with `tag` (and only then).")
    kind: Optional[TagKind | Literal["inherit"]] = Field(
        None,
        description="Set on every part of the transactions, over the tag's kind. "
        "`inherit` clears it (the tags' kinds apply), null keeps it. Without `tag`, "
        "transactions without tags are skipped.",
    )
    description: Optional[str] = Field(
        None,
        description="The transaction's description (may have several lines), empty "
        "clears it. Null keeps it.",
    )
    needs_review: Optional[bool] = Field(
        None,
        description=f"{NEEDS_REVIEW_DESCRIPTION} Null keeps it. Transactions left "
        "without tags or a description are skipped.",
    )
    author: str = Author
    dry_run: bool = Field(
        True, description="Only preview the affected transactions (the default)."
    )
    expected_ids: Optional[list[str]] = Field(
        None,
        description="When committing, fail unless exactly these transactions are "
        "affected, e.g. the `affected_ids` of a preview.",
    )
    allow_new_tags: bool = False

    @model_validator(mode="after")
    def check_fields(self):
        if (self.tag is None) != (self.subtag is None):
            raise ValueError("tag and subtag go together")
        fields = (self.tag, self.kind, self.description, self.needs_review)
        if all(field is None for field in fields):
            raise ValueError(
                "Nothing to change: give tag, kind, description or needs_review"
            )
        return self


class BulkAnnotateResult(Model):
    dry_run: bool
    affected_ids: list[str] = Field(
        description="Transactions that are (or would be) changed, by date."
    )
    unchanged_ids: list[str] = Field(
        description="Matching transactions already as requested, left as they are."
    )
    replaced_ids: list[str] = Field(
        description="Affected transactions whose tags (and splits) are replaced."
    )
    skipped_rule_ids: list[str] = Field(
        description="Matching transactions skipped because a rule tags them."
    )
    skipped_untagged_ids: list[str] = Field(
        description="Matching transactions skipped because they have no tags: a "
        "kind needs one (unless `tag` is given), and the review flag needs tags or "
        "a description."
    )


# Tags


class SubtagOut(Model):
    name: str
    color: Optional[str] = Field(description="Own color, null inherits the tag's.")
    kind: Optional[TagKind] = Field(description="Own kind, null inherits the tag's.")
    effective_color: Optional[str]
    effective_kind: TagKind
    annotations: int = Field(description="Annotated transactions using it.")
    rules: int = Field(description="Rules using it.")
    transactions: int = Field(description="Current transactions tagged with it.")


class TagOut(Model):
    name: str
    color: Optional[str]
    kind: TagKind = Field(description=KIND_DESCRIPTION)
    transactions: int = Field(description="Current transactions tagged with it.")
    subtags: list[SubtagOut]


class TagRef(Model):
    tag: str = Field(min_length=1)
    subtag: Optional[str] = Field(
        None, description="Null refers to the tag itself (an empty string is a subtag)."
    )


class TagCreate(TagRef):
    color: Optional[str] = None
    kind: Optional[TagKind] = Field(None, description="Default: regular, or inherit.")


class TagPropertiesUpdate(TagRef):
    """Only the given properties change. For a subtag, null inherits from the tag."""

    color: Optional[str] = None
    kind: Optional[TagKind] = Field(None, description=KIND_DESCRIPTION)


class TagRename(TagRef):
    new_name: str


class TagMerge(TagRef):
    """Merge a tag into another tag, or a subtag into another subtag."""

    into_tag: str = Field(min_length=1)
    into_subtag: Optional[str] = Field(
        None, description="Required when merging a subtag (created if missing)."
    )


# Accounts and groups


class AccountOut(Model):
    name: str
    group: str = Field(description="Every account is in one group.")
    group_role: GroupRole = Field(description=ROLE_DESCRIPTION)
    color: Optional[str]
    transactions: int
    sources: list[str] = Field(description="Its sources with transactions, by name.")


class BalanceQuery(Model):
    """A balance at the end of a day: of an account, one of its sources, a group,
    or the net worth (without `account` and `group`)."""

    date: Optional[datetime.date] = Field(
        None, description="With every row up to its end. Default: the latest data's."
    )
    account: Optional[str] = None
    source: Optional[str] = Field(
        None, description="One of the account's sources (needs `account`)."
    )
    group: Optional[str] = Field(None, description="Not with `account`.")

    @model_validator(mode="after")
    def _one_balance(self):
        if self.source is not None and self.account is None:
            raise ValueError("A source needs its account")
        if self.account is not None and self.group is not None:
            raise ValueError("Give an account or a group, not both")
        return self


class BalanceOnDate(Model):
    date: datetime.date = Field(description="The balance is at the end of this day.")
    label: str = Field(
        description="`net worth`, the group's name, the account's, or the account "
        "and source (e.g. `cash/wallet`)."
    )
    account: Optional[str]
    source: Optional[str]
    group: Optional[str] = Field(description="Of a group, or of the account.")
    balance: float = Field(
        description="The sum of every row up to the end of the date, of every kind."
    )
    rows: int = Field(description="Rows up to the end of the date.")
    last_date: Optional[datetime.date] = Field(
        description="Of the latest row on or before the date."
    )


class AccountUpdate(Model):
    """Only the given properties change."""

    group: Optional[str] = Field(None, description="An existing group.")
    color: Optional[str] = None


class GroupOut(Model):
    name: str
    role: GroupRole = Field(description=ROLE_DESCRIPTION)
    color: Optional[str]
    position: int = Field(description="0 is first, in the display order.")
    accounts: list[str]


class GroupCreate(Model):
    name: str = Field(min_length=1)
    role: GroupRole = Field("spending", description=ROLE_DESCRIPTION)
    color: Optional[str] = None
    position: Optional[int] = Field(None, ge=0, description="Default: last.")


class GroupUpdate(Model):
    """Only the given properties change."""

    new_name: Optional[str] = Field(None, min_length=1)
    role: Optional[GroupRole] = Field(None, description=ROLE_DESCRIPTION)
    color: Optional[str] = Field(None, description="Null clears it.")
    position: Optional[int] = Field(None, ge=0)


# Rules


RULE_FILTERS_DESCRIPTION = (
    "Filters on transaction fields (not tags), e.g. "
    '`{"raw_description": "SuperMart"}`. `raw_description` and `source` are regular '
    "expressions, `account` is exact, dates are a range (end exclusive). Combine "
    "with `and_filters`/`or_filters`, and negate with `invert`."
)


class RuleOut(Model):
    id: int
    position: int = Field(description="0 is first: the first matching rule wins.")
    filters: Filters
    tag: str
    subtag: str
    transactions: int = Field(description="Current transactions this rule tags.")


class RuleCreate(Model):
    filters: Filters = Field(description=RULE_FILTERS_DESCRIPTION)
    tag: str = Field(min_length=1)
    subtag: str
    position: Optional[int] = Field(None, ge=0, description="Default: last.")
    allow_new_tags: bool = False


class RuleUpdate(Model):
    """Only the given fields change. Tag and subtag are given together."""

    filters: Optional[Filters] = Field(None, description=RULE_FILTERS_DESCRIPTION)
    tag: Optional[str] = Field(None, min_length=1)
    subtag: Optional[str] = None
    position: Optional[int] = Field(None, ge=0)
    allow_new_tags: bool = False


class RuleReorder(Model):
    rule_ids: list[int] = Field(description="Every rule ID, in the new order.")


class RulePreviewRequest(Model):
    filters: Filters = Field(description=RULE_FILTERS_DESCRIPTION)
    position: Optional[int] = Field(
        None, ge=0, description="Default: last, or the current position of `rule_id`."
    )
    rule_id: Optional[int] = Field(
        None, description="Preview a change to this rule (it is left out)."
    )


class RulePreview(Model):
    matched_ids: list[str] = Field(description="Transactions matching the filters.")
    effective_ids: list[str] = Field(
        description="Matching transactions not matched by an earlier rule, which "
        "the rule would tag."
    )
    newly_tagged_ids: list[str] = Field(
        description="Effective transactions that are currently untagged."
    )
    overridden_annotation_ids: list[str] = Field(
        description="Effective transactions with an annotation, which the rule "
        "would override."
    )


# Orphans


class SnapshotOut(Model):
    account: str
    source: str
    date: datetime.date
    amount: float
    raw_description: str


class StoredAnnotation(Model):
    transaction_id: str
    snapshot: SnapshotOut = Field(
        description="The transaction as it was when annotated."
    )
    description: Optional[str] = Field(description=DESCRIPTION_DOC)
    parts: list[PartOut]
    authors: list[str] = Field(description=AUTHORS_DESCRIPTION)
    needs_review: bool
    rule: Optional[RuleRef] = Field(None, description="The rule overriding it.")


class OrphanDelete(Model):
    ids: list[str] = Field(min_length=1, description="Orphaned transaction IDs.")


class OrphanDeleteResult(Model):
    deleted_ids: list[str]


# Ingest and status


class IngestSummary(Model):
    accounts: dict[str, int] = Field(description="Transactions by account.")
    transactions: int
    new_ids: list[str] = Field(description="IDs absent before this ingest.")
    vanished_ids: list[str] = Field(description="IDs present before, absent now.")
    changed_ids: list[str] = Field(
        description="IDs present before and now, whose transaction changed."
    )
    orphans: int
    new_orphan_ids: list[str]
    data_changed: bool = Field(
        description="Whether the ingest changed the ingested data: any new, "
        "vanished or changed transactions."
    )
    ingested_changed_at: Optional[datetime.datetime] = Field(
        description="When an ingest last changed the ingested data (new, vanished "
        "or changed transactions), null if never ingested."
    )
    ingested_checked_at: datetime.datetime = Field(description="When this ingest ran.")


class IngestFileSource(Model):
    """The rows of one source in a file, after the ingestion filters."""

    source: str
    rows: int
    start_date: datetime.date
    end_date: datetime.date
    total: float = Field(description="Sum of the amounts.")


class IngestFile(Model):
    path: str
    name: str
    size: int = Field(description="In bytes.")
    modified_at: datetime.datetime
    newer_than_ingest: bool = Field(
        description="Modified after the last ingest, whether it changed anything "
        "or not (always true if never ingested)."
    )
    parser: Optional[str] = Field(description="The parser that read it, if any.")
    parser_errors: dict[str, str] = Field(
        description="Why each parser tried before it (or every parser) rejected it."
    )
    error: Optional[str] = Field(description="Why the file can't be ingested, if so.")
    rows: int = Field(description="Parsed rows.")
    kept_rows: int = Field(
        description="Rows kept by the ingestion rules and filters (some maybe "
        "rewritten)."
    )
    transfer_rows: int = Field(
        description="Rows that transfer rules add in other sources (the other legs "
        "of its rows), kept by the filters."
    )
    start_date: Optional[datetime.date] = Field(description="Of the parsed rows.")
    end_date: Optional[datetime.date]
    total: float = Field(description="Sum of the parsed amounts.")
    sources: list[IngestFileSource] = Field(description="Of the kept rows.")


class IngestAccountFiles(Model):
    name: str
    folder: str = Field(description="The account's folder, in the raw folder.")
    files: list[IngestFile]


class IngestOverlap(Model):
    """Two files of an account whose rows of a source overlap in dates."""

    account: str
    source: str
    files: tuple[str, str] = Field(description="The two files' paths.")
    start_date: datetime.date = Field(description="The overlapping date range.")
    end_date: datetime.date
    shared_rows: int = Field(
        description="Rows in both files with the same date, description and "
        "amount, which would likely be duplicates."
    )


IngestProblemKind = Literal[
    "no_files",
    "unparseable",
    "empty",
    "filtered_out",
    "overlap",
    "missing_account",
    "stray_file",
]


class IngestProblem(Model):
    kind: IngestProblemKind = Field(
        description="`no_files`: an account's folder has no files. `unparseable`: "
        "no parser reads a file. `empty`: a file has no rows. `filtered_out`: the "
        "ingestion filters drop all of a file's rows. `overlap`: two files share "
        "rows (likely duplicates). `missing_account`: an ingestion rule transfers "
        "to an account without a folder (the ingest fails). `stray_file`: a file "
        "in the raw folder itself, outside the accounts' folders (not ingested)."
    )
    account: Optional[str] = Field(description="None for a stray file.")
    paths: list[str]
    message: str


class IngestDryRun(Model):
    raw_dir: str = Field(
        description="The raw folder (of this machine): each of its folders is an "
        "account, with the account's raw files."
    )
    raw_dir_exists: bool
    ingested_changed_at: Optional[datetime.datetime] = Field(
        description="When an ingest last changed the ingested data (new, vanished "
        "or changed transactions), null if never ingested."
    )
    ingested_checked_at: Optional[datetime.datetime] = Field(
        description="When an ingest last ran, whether it changed anything or not, "
        "null if never."
    )
    accounts: list[IngestAccountFiles]
    overlaps: list[IngestOverlap] = Field(
        description="Every date overlap, including those without shared rows "
        "(e.g. two cards of the same account)."
    )
    problems: list[IngestProblem]


class IngestRow(Model):
    id: str
    account: str
    source: str
    date: datetime.date
    amount: float
    raw_description: str


class IngestAccountCounts(Model):
    before: int
    after: int


class IngestPreview(Model):
    accounts: dict[str, IngestAccountCounts] = Field(
        description="Transactions by account, now and after ingesting."
    )
    transactions: int = Field(description="Transactions after ingesting.")
    unchanged: int = Field(description="Transactions kept as they are, by ID.")
    new: list[IngestRow] = Field(description="Transactions that would appear.")
    vanished: list[IngestRow] = Field(description="Transactions that would vanish.")
    changed: list[IngestRow] = Field(
        description="Transactions that would stay, by ID, but change (as they "
        "would be). Only with IDs from the bank: derived IDs change with the "
        "transaction."
    )
    new_orphans: list[StoredAnnotation] = Field(
        description="Annotations of vanishing transactions, which would be orphaned."
    )
    restored_orphan_ids: list[str] = Field(
        description="Orphaned annotations whose transaction would be back."
    )


class Status(Model):
    db_path: str
    ingested_dir: str
    ingested_changed_at: Optional[datetime.datetime] = Field(
        description="When an ingest last changed the ingested data (new, vanished "
        "or changed transactions), null if never ingested."
    )
    ingested_checked_at: Optional[datetime.datetime] = Field(
        description="When an ingest last ran, whether it changed anything or not, "
        "null if never."
    )
    transactions: int
    untagged: int
    annotated: int
    rule_matched: int
    overridden: int = Field(description="Annotations overridden by rules.")
    needs_review: int = Field(description="Transactions flagged for review.")
    orphans: int = Field(description="Annotations of missing transactions.")
    accounts: list[str]


# Analysis


class AnalysisQuery(RowFilters):
    """Analysis rows: one per transaction part, enriched like the reports' data.

    Filters apply to rows, so amounts are part amounts (positive for expenses
    when `expenses` is set)."""

    kinds: Optional[list[TagKind]] = Field(
        None,
        description="Only tags of these kinds (all if null), e.g. "
        '`["regular", "one-off"]` for incomes and expenses. Untagged rows are '
        "regular.",
    )
    groups: Optional[list[str]] = Field(
        None, description="Only accounts of these groups (null: the spending groups)."
    )
    all_groups: bool = Field(False, description="Accounts of every group.")
    incomes: bool = Field(
        False,
        description="Only incomes (of regular and one-off tags, unless `kinds` "
        "says otherwise).",
    )
    expenses: bool = Field(
        False,
        description="Only expenses, as positive amounts (of regular and one-off "
        "tags, unless `kinds` says otherwise).",
    )
    net_by_subtag: bool = Field(
        False, description="Net incomes and expenses by subtag (with net_by_tag)."
    )
    sentinels: bool = Field(False, description="Include zero-amount sentinel ticks.")


class AnalysisRow(Model):
    id: Optional[str] = Field(description="Null for generated ticks.")
    account: str
    source: str
    date: datetime.date
    amount: float
    raw_description: str = Field(description=RAW_DESCRIPTION_DOC)
    tag: Optional[str]
    subtag: Optional[str]
    description: Optional[str] = Field(description=PART_DESCRIPTION_DOC)
    transaction_description: Optional[str] = Field(
        description="The whole transaction's description."
    )
    month: str
    tags: Optional[str]
    is_sentinel_tick: bool
    kind: TagKind = Field(
        description="Set on the transaction, or its tag's (subtag's). Regular if "
        "untagged."
    )
    kind_set: bool = Field(description=KIND_SET_DESCRIPTION)
    group: str = Field(description="The account's group.")
    group_role: GroupRole
    account_source: str
    balance_total: float = Field(description="The net worth: every group.")
    balance_role: float = Field(
        description="Of the groups with the row's role, e.g. the available "
        "balance for spending."
    )
    balance_group: float = Field(description="Of the row's group.")
    balance_account: float
    balance_source: float


class AnalysisRows(Model):
    total: int
    rows: list[AnalysisRow]


class AggregateQuery(AnalysisQuery):
    group_by: list[GroupField] = Field(default_factory=lambda: ["month"])


class Aggregate(Model):
    group_by: list[GroupField]
    rows: list[dict[str, Any]] = Field(
        description="One per group: the group's values, `amount` (sum) and `count`."
    )


# Dashboard

Month = Annotated[str, Field(pattern=r"^\d{4}-(0[1-9]|1[0-2])$", examples=["2025-06"])]
PeriodPreset = Literal["all", "ytd", "3m", "6m", "12m", "24m"]
Flow = Literal["net", "incomes", "expenses", "saved"]
UNTAGGED_DESCRIPTION = "Null for untagged transactions."


RollingWindow = Literal[1, 3, 6, 12]
"""Months of a rolling mean."""
ROLLING_DESCRIPTION = (
    'By rolling window, in months ("1", "3", "6" and "12"): plain means of the '
    "calendar months up to each month, with 0 in months without rows (null "
    "until the data has as many months)."
)


class DashboardOptions(Model):
    """Options of the dashboard's analysis. Unset options take the dashboard
    defaults (`GET /api/settings/dashboard`)."""

    period: PeriodPreset = Field(
        "all",
        description="Months to show, relative to the last month of the data: "
        "`3m` is the last three months, `ytd` from January of its year.",
    )
    start_month: Optional[Month] = Field(
        None, description="First month (inclusive), overrides the period's."
    )
    end_month: Optional[Month] = Field(
        None, description="Last month (inclusive), overrides the period's."
    )
    accounts: Optional[list[str]] = Field(
        None,
        description="Only these accounts, as if the others didn't exist "
        "(balances are theirs only).",
    )
    groups: Optional[list[str]] = Field(
        None,
        description="Only accounts of these groups in cash flows, breakdowns and "
        "trends (null: the groups for spending). Balances always have every group.",
    )
    tags: Optional[list[str]] = Field(
        None, description="Only these tags in cash flows and breakdowns."
    )
    include_one_offs: bool = Field(
        False,
        description="Count one-off tags in rolling averages, trends and monthly "
        "averages too (they are always in monthly sums and breakdowns).",
    )
    net_by_tag: Optional[bool] = Field(
        None, description="Net incomes and expenses by tag, not by transaction."
    )
    complete_months_only: Optional[bool] = Field(
        None,
        description="Monthly figures (cash flows, breakdowns, trends, averages) "
        "leave out the current calendar month, which is incomplete, unless the "
        "period's months are explicit. Balances always run to the latest data.",
    )
    rolling_months: Optional[RollingWindow] = Field(
        None,
        description="The rolling window shown first. Only resolved for display: "
        "every window is computed.",
    )
    breakdown_months: Optional[int] = Field(
        None,
        ge=1,
        description="Number of last months in the monthly view. Only resolved "
        "for display: the breakdown has every month of the period.",
    )
    subtag_trends: bool = Field(
        False,
        description="Also trends by tag and subtag (`trends.subtags`), which "
        "are only computed when asked.",
    )


class DashboardPeriod(Model):
    start: Optional[str] = Field(description="First month, null if unbounded.")
    end: Optional[str] = Field(description="Last month, null if unbounded.")
    months: list[str] = Field(description="Every month shown.")


RATE_DESCRIPTION = ", e.g. 0.12. Null without positive incomes."


class MonthFlows(Model):
    net: Optional[float] = Field(
        description="Incomes less expenses: what was kept, before saving."
    )
    incomes: Optional[float]
    expenses: Optional[float] = Field(description="Positive.")
    saved: Optional[float] = Field(
        description="Set aside: minus the rows of saving tags in accounts of "
        "groups for spending (positive when set aside)."
    )
    kept_rate: Optional[float] = Field(description="Net / incomes" + RATE_DESCRIPTION)
    saved_rate: Optional[float] = Field(
        description="Saved / incomes" + RATE_DESCRIPTION
    )
    one_off_incomes: Optional[float] = Field(description="Part of the incomes.")
    one_off_expenses: Optional[float] = Field(description="Part of the expenses.")


class TopTag(Model):
    tag: Optional[str] = Field(description=UNTAGGED_DESCRIPTION)
    amount: float = Field(description="Expenses in the last month, one-offs too.")
    one_off: float = Field(description="The one-off part of the amount.")
    previous: Optional[float] = Field(description="In the month before.")
    average: Optional[float] = Field(
        description="Monthly average in the period (without one-offs, unless included)."
    )
    averaged: Optional[float] = Field(
        description="The amount as averages sum it (without one-offs, unless "
        "included), null if none: what is compared with the baseline."
    )
    baseline: dict[str, Optional[float]] = Field(
        description="By rolling window: the rolling mean of the months before "
        "the last (without one-offs, unless included)."
    )


class Runway(Model):
    expenses: Optional[float] = Field(
        description="Typical monthly expenses: the rolling mean through the last "
        "month, without one-offs (even if they are included), null until the "
        "data has as many months."
    )
    months: Optional[float] = Field(
        description="The available balance / expenses, null without positive expenses."
    )


class Headline(Model):
    as_of: Optional[datetime.date] = Field(
        description="The date of the balances: the latest data's, unless the "
        "period ends earlier."
    )
    month: Optional[str] = Field(description="The last month of the period.")
    previous_month: Optional[str]
    net_worth: Optional[float] = Field(description="Every group's balance, as of.")
    available: Optional[float] = Field(
        description="The balance of the groups for spending, as of."
    )
    runway: dict[str, Runway] = Field(
        description="By rolling window: the available balance in months of "
        "typical expenses."
    )
    last: Optional[MonthFlows] = Field(description="Cash flow of the last month.")
    previous: Optional[MonthFlows] = Field(description="Of the month before.")
    totals: Optional[MonthFlows] = Field(
        description="Sums over the period, one-offs included, and their rates."
    )
    averages: Optional[MonthFlows] = Field(
        description="Monthly, over the period, without one-offs unless included. "
        "Its rates are of the period's sums (as averages sum them)."
    )
    top_expenses: list[TopTag] = Field(description="Largest in the last month.")


class BalanceRows(Model):
    """The rows behind balance points, as columns."""

    id: list[Optional[str]]
    date: list[datetime.date]
    amount: list[float]
    raw_description: list[str]
    description: list[Optional[str]] = Field(description=PART_DESCRIPTION_DOC)
    transaction_description: list[Optional[str]] = Field(
        description="The whole transaction's description."
    )
    tags: list[Optional[str]]
    account_source: list[str]


class BalanceSeries(Model):
    key: str = Field(
        description="`net_worth`, `available`, `group:` and the group's name, or "
        "the account and source."
    )
    label: str
    kind: Literal["net_worth", "available", "group", "account_source"]
    account: Optional[str] = Field(description="Of an account and source.")
    group: Optional[str] = Field(description="Of a group, or account and source.")
    dates: list[datetime.date] = Field(description="One per day.")
    values: list[float] = Field(description="Balances at the end of each day.")
    rows: list[list[int]] = Field(
        description="Indices into the rows of each day's point. Empty for "
        "points added at the period's edges."
    )


class AccountBalance(Model):
    account: str
    source: str
    account_source: str = Field(
        description="The label of the account and source, as in balance series "
        "and rows."
    )
    group: str
    balance: float = Field(description="As of.")
    change: float = Field(
        description="Over the period (since the first data without a start)."
    )


class BalanceTotal(Model):
    key: str = Field(description="As in balance series.")
    label: str
    kind: Literal["net_worth", "available", "group"]
    group: Optional[str]
    balance: float = Field(description="As of.")
    change: float = Field(
        description="Over the period (since the first data without a start)."
    )
    one_offs: float = Field(
        description="The rows of one-off tags over the period: part of the change."
    )


class BalanceSection(Model):
    """Daily balances, from the period's start to the latest data (or the
    period's end, if it ends earlier)."""

    as_of: Optional[datetime.date] = Field(description="The last day.")
    rows: BalanceRows
    series: list[BalanceSeries]
    totals: list[BalanceTotal] = Field(
        description="The net worth, the available balance, and each group's."
    )
    accounts: list[AccountBalance] = Field(description="Largest first.")


class FlowSeries(Model):
    net: list[Optional[float]]
    incomes: list[Optional[float]]
    expenses: list[Optional[float]]
    saved: list[Optional[float]]


class OneOffSeries(Model):
    incomes: list[float]
    expenses: list[float]


class CashFlowSection(Model):
    """Monthly sums, aligned with `months`: incomes and expenses (regular and
    one-off tags), and money saved (saving tags)."""

    months: list[str]
    net: list[float] = Field(description="Incomes less expenses.")
    incomes: list[float]
    expenses: list[float] = Field(description="Positive.")
    saved: list[float] = Field(
        description="Positive when set aside (rows of saving tags in accounts "
        "of groups for spending)."
    )
    kept_rate: list[Optional[float]] = Field(
        description="Net / incomes, null without positive incomes."
    )
    saved_rate: list[Optional[float]] = Field(
        description="Saved / incomes, null without positive incomes."
    )
    one_offs: OneOffSeries = Field(
        description="The one-off parts of incomes and expenses."
    )
    averaged: FlowSeries = Field(
        description="Monthly sums as averages sum them (without one-offs, unless "
        "included): what each month is compared with its baseline."
    )
    rolling: dict[str, FlowSeries] = Field(
        description="Rolling means (without one-offs, unless included). "
        + ROLLING_DESCRIPTION
    )
    baseline: dict[str, FlowSeries] = Field(
        description="What each month is compared with: the rolling mean of the "
        "month before, by rolling window."
    )


class BreakdownItem(Model):
    tag: Optional[str] = Field(description=UNTAGGED_DESCRIPTION)
    subtag: Optional[str]
    kind: TagKind
    amount: float = Field(description="Signed: negative if refunded more.")
    percent: float = Field(description="Of the (signed) total.")


class RollingBreakdownItem(Model):
    month: str
    tag: Optional[str] = Field(description=UNTAGGED_DESCRIPTION)
    subtag: Optional[str]
    kind: TagKind
    amount: float = Field(description="In the month.")
    rolling: float = Field(description="Signed.")
    percent: float = Field(description="Of the (signed) total.")


class MonthlyBreakdownItem(Model):
    month: str
    tag: Optional[str] = Field(description=UNTAGGED_DESCRIPTION)
    subtag: Optional[str]
    kind: TagKind
    amount: float


class KindBreakdown(Model):
    incomes: list[BreakdownItem]
    expenses: list[BreakdownItem]


class KindRollingBreakdown(Model):
    incomes: list[RollingBreakdownItem]
    expenses: list[RollingBreakdownItem]


class MonthlyBreakdown(Model):
    months: list[str]
    incomes: list[MonthlyBreakdownItem]
    expenses: list[MonthlyBreakdownItem]


class BreakdownSection(Model):
    """Incomes and expenses by tag and subtag, told apart as in cash flows, so
    they add up to them."""

    totals: KindBreakdown = Field(description="Over the period.")
    rolling: dict[str, KindRollingBreakdown] = Field(
        description="Rolling means in the last month (without one-offs, unless "
        "included), without those of 0. " + ROLLING_DESCRIPTION
    )
    monthly: MonthlyBreakdown = Field(description="Every month of the period.")


class TagTrend(Model):
    tag: Optional[str] = Field(description=UNTAGGED_DESCRIPTION)
    subtag: Optional[str] = Field(
        None,
        description="In trends by subtag, the subtag (empty for none). Null in "
        "trends by tag, and for untagged rows.",
    )
    amounts: list[float] = Field(description="Monthly sums.")
    rolling: dict[str, list[Optional[float]]] = Field(
        description="Rolling means. " + ROLLING_DESCRIPTION
    )
    baseline: dict[str, list[Optional[float]]] = Field(
        description="What each month is compared with: the rolling mean of the "
        "month before, by rolling window."
    )


class SubtagTrends(Model):
    incomes: list[TagTrend]
    expenses: list[TagTrend]


class TrendsSection(Model):
    """Monthly amounts by tag, aligned with the period's months, without
    one-offs unless included."""

    tag_order: list[Optional[str]] = Field(
        description="Tags of incomes and expenses by absolute net amount, "
        "largest first."
    )
    incomes: list[TagTrend]
    expenses: list[TagTrend]
    subtags: Optional[SubtagTrends] = Field(
        None,
        description="The same by tag and subtag: a tag's subtags add up to its "
        "trend. Null unless the `subtag_trends` option is set.",
    )


BridgeLineName = Literal["available", "net_worth"]


class BridgeLine(Model):
    """How a balance went from the period's start to the end of its last
    month: opening + incomes - expenses - saved + transfers + reconciliations
    = closing. Every
    row of the line's accounts is in one term, and every tag counts (whatever
    the `tags` option). Incomes and expenses are told apart among the line's
    accounts: for the available balance, as cash flows with the default
    groups."""

    line: BridgeLineName = Field(
        description="`available` (the spending groups) or `net_worth` (every group)."
    )
    start: datetime.date = Field(description="The first day.")
    end: datetime.date = Field(description="The last day: the last month's.")
    opening: float = Field(description="The balance before the first day.")
    incomes: float
    expenses: float = Field(description="Positive.")
    saved: float = Field(
        description="Minus the rows of saving tags: on the spending side for the "
        "available balance (as cash flows), of every group for the net worth "
        "(where both sides of a saving cancel out, so what is left is a saving "
        "with one side recorded)."
    )
    transfers: float = Field(
        description="The rows of transfer tags: 0 for the net worth when both "
        "sides of every transfer are recorded."
    )
    reconciliations: float = Field(
        description="The rows of reconciliation tags: re-anchored balances (e.g. "
        "opening balances, or a wallet's counted cash)."
    )
    closing: float = Field(description="The balance at the end of the last day.")
    rounding: float = Field(
        description="Closing less the sum of the rounded terms (0 unless cents "
        "round differently)."
    )
    one_offs: float = Field(
        description="The rows of one-off tags: part of incomes less expenses."
    )
    later: Optional[float] = Field(
        description="The change after the last month, up to the balances' date "
        "(null if the balances end with the months)."
    )
    as_of: Optional[datetime.date] = Field(
        description="The balances' date, if after the last month."
    )


class AccountRecency(Model):
    account_source: str
    account: str
    source: str
    last_date: datetime.date = Field(description="Of its latest transaction.")
    days_behind: int = Field(description="Days before the latest data's date.")
    active_months: int = Field(description="Months of the period with its rows.")
    behind: bool = Field(
        description="Usually active (in at least half of the period's months), "
        "but its latest row is over a month before the latest data's: its raw "
        "files may be missing."
    )


class SignFlip(Model):
    tag: str
    direction: Literal["incomes", "expenses"] = Field(
        description="The tag's side over all the data."
    )
    amount: float = Field(
        description="Its net in the period (signed), of the other sign."
    )


class IncomeMonth(Model):
    month: str
    incomes: float = Field(
        description="As averages sum them (without one-offs, unless included)."
    )
    usual: float = Field(description="The median month of the period.")


class HealthSection(Model):
    """Checks of the data behind the numbers."""

    latest: Optional[datetime.date] = Field(description="The latest data's date.")
    last_dates: list[AccountRecency] = Field(
        description="Every account and source, least recent first."
    )
    saving_rows: int = Field(
        description="Rows of saving tags in the period's months, on the spending "
        "side (what Saved sums, whatever the `tags` option). 0 means nothing is "
        "tagged as saving."
    )
    sign_flips: list[SignFlip] = Field(
        description="Tags of incomes and expenses whose net in the period has "
        "the opposite sign of their net over all the data."
    )
    transfer_residual: float = Field(
        description="The sum of transfer rows of every group in the period's "
        "months (whatever the `tags` option): 0 when both sides of every "
        "transfer are recorded. Reconciliation rows (e.g. opening balances) are "
        "not transfers."
    )
    transfer_rows: int
    income_months: list[IncomeMonth] = Field(
        description="Months whose incomes are more than 40% from the median month."
    )


class Dashboard(Model):
    options: DashboardOptions = Field(description="The options used, resolved.")
    available_months: list[str] = Field(
        description="Months with transactions offered for monthly figures "
        "(without the current month, with `complete_months_only`)."
    )
    rolling_windows: list[RollingWindow] = Field(
        description="The rolling windows computed, in months."
    )
    colors: dict[str, str] = Field(
        description="Colors of labels: accounts, tags, and other labels' colors."
    )
    groups: list[GroupOut] = Field(description="In their display order.")
    period: DashboardPeriod
    headline: Headline
    balance: BalanceSection
    cash_flow: CashFlowSection
    breakdown: BreakdownSection
    trends: TrendsSection
    bridge: list[BridgeLine] = Field(
        description="The available balance's and the net worth's, empty without months."
    )
    health: HealthSection


class DashboardSelection(Model):
    """Which rows: as the table of a number selected them, then filtered."""

    flow: Optional[Flow] = Field(
        None,
        description="Only incomes, only expenses (as positive amounts), all (`net` "
        "or null), or `saved` (saving tags, positive when set aside).",
    )
    kinds: Optional[list[TagKind]] = Field(
        None,
        description='Only tags of these kinds (all if null), e.g. `["regular", '
        '"one-off"]` as cash flows and breakdowns, `["regular"]` as averages.',
    )
    all_groups: bool = Field(
        False,
        description="Accounts of every group, as balances (default: the "
        "option's groups). Their rows run to the latest data, as balances do.",
    )
    untagged: bool = Field(False, description="Only untagged rows.")
    tag: Optional[str] = None
    subtag: Optional[str] = None
    month: Optional[Month] = None
    date: Optional[datetime.date] = None
    account_source: Optional[str] = None
    group: Optional[str] = Field(None, description="Of the account.")
    group_role: Optional[GroupRole] = Field(None, description="Of the account.")
    kind: Optional[TagKind] = Field(
        None,
        description="Only rows of this kind, after telling incomes and expenses "
        "apart among `kinds`, e.g. the one-off part of a month's expenses.",
    )
    line: Optional[BridgeLineName] = Field(
        None,
        description="Rows of a bridge's term instead (see `BridgeLine`): of the "
        "line's accounts, told apart as the bridge does, over the period's "
        "months. `groups` and `tags` options don't apply.",
    )
    after_months: bool = Field(
        False,
        description="With `line`: the rows after the period's months, up to the "
        "balances' date.",
    )


class DashboardRowsRequest(Model):
    options: DashboardOptions = Field(default_factory=DashboardOptions)
    selection: DashboardSelection = Field(default_factory=DashboardSelection)


# Settings


class ServerInfo(Model):
    demo: bool = Field(
        description="Serving invented demo data (`finalyze --demo`), in a "
        "temporary directory: changes vanish when the server stops."
    )
    stale_web_app: Optional[str] = Field(
        description="Why the served web app is older than its sources, if it is "
        "(rebuild it with scripts/build-web.sh)."
    )


class DashboardDefaults(Model):
    """Defaults of the dashboard's options, kept in the store."""

    net_by_tag: bool = Field(
        False, description="Net incomes and expenses by tag, not by transaction."
    )
    breakdown_months: int = Field(
        3, ge=1, description="Number of last months in the monthly view."
    )
    complete_months_only: bool = Field(
        True,
        description="Monthly figures leave out the current calendar month, "
        "which is incomplete. Balances always run to the latest data.",
    )
    rolling_months: RollingWindow = Field(
        6, description="The rolling window shown and compared with."
    )


DateFormat = Literal["dd/mm/yyyy", "yyyy-mm-dd", "mm/dd/yyyy"]


class DisplaySettings(Model):
    """How the web app shows things, kept in the store."""

    date_format: DateFormat = Field(
        "dd/mm/yyyy",
        description="How the web app shows and reads full dates (in tables, "
        "tooltips, stamps and date inputs). The API's dates are always ISO "
        "(`yyyy-mm-dd`), and so are the web app's URLs.",
    )


DirectionKey = Annotated[str, StringConstraints(pattern=r"^[^\sA-Z]$")]
"""A direction's key: one character, without Shift (which reorders)."""


class DirectionKeys(Model):
    """The keys of the four directions in the web app: rows, the breakdown's
    months, and shifted, reordering. The arrows always work too."""

    left: DirectionKey = "h"
    down: DirectionKey = "j"
    up: DirectionKey = "k"
    right: DirectionKey = "l"


KEYMAP_ACTION = r"^[a-z][A-Za-z0-9]*(\.[a-z][A-Za-z0-9]*)*$"
KEYMAP_KEY = r"^\S{1,40}$"


class Keymap(Model):
    """The web app's keyboard shortcuts, kept in the store: the keys of the
    directions, and the keys of single actions over their defaults."""

    directions: DirectionKeys = Field(
        default_factory=DirectionKeys,
        description="Keys of left, down, up and right (vim's h j k l by default). "
        "Shifted, they reorder (e.g. `J` and `K`).",
    )
    overrides: dict[
        Annotated[str, StringConstraints(pattern=KEYMAP_ACTION)],
        list[Annotated[str, StringConstraints(pattern=KEYMAP_KEY)]],
    ] = Field(
        default_factory=dict,
        description="Keys of actions, by action ID (e.g. `tags.rename`), "
        "replacing their default keys (an empty list unbinds one). A key is "
        "named like `x`, `X` (shift+x), `?`, `Ctrl+Enter` or `Ctrl+Shift+a`; a "
        "page's key (`go.*`) is its letter after the `go.prefix` key. The web "
        "app's Settings page lists the actions, and refuses keys that clash.",
    )

    @field_validator("directions", mode="before")
    @classmethod
    def _from_preset(cls, directions: Any) -> Any:
        """Before the four keys, the directions were a preset: "hjkl" or "jkl;"."""
        if directions in ("hjkl", "jkl;"):
            return dict(zip(("left", "down", "up", "right"), directions, strict=True))
        return directions

    @field_validator("overrides")
    @classmethod
    def _distinct_keys(cls, overrides: dict[str, list[str]]) -> dict[str, list[str]]:
        for action, keys in overrides.items():
            if len(set(keys)) != len(keys):
                raise ValueError(f"{action} has a key twice")
        return overrides


class LabelColor(Model):
    label: str
    color: Optional[str] = Field(description="A CSS color, null for none.")


class LabelColors(Model):
    colors: dict[str, str] = Field(
        description="Colors of chart labels other than accounts and tags (which "
        "have their own), by label."
    )
    labels: list[str] = Field(
        description='Labels drawn by the dashboard: "net worth", "available", '
        'and every account and source (e.g. "alice/card").'
    )


class HostSettings(Model):
    """Where things are on this machine: the host file's values, read-only."""

    file: str = Field(description="The host file.")
    file_exists: bool = Field(description="Without it, the defaults apply.")
    data_dir: str
    raw_dir: str = Field(description="A folder per account, with its raw files.")
    backups_dir: str = Field(description="In the data directory.")
    host: Optional[str] = Field(description="The default address to serve on.")
    port: Optional[int] = Field(description="The default port to serve on.")


class ConfigSettings(Model):
    """The config file's settings (missing ones are the defaults)."""

    file: str = Field(description="The config file.")
    config: Config


# Export and import, backups


class ExportManifest(Model):
    """What an export holds, from its `manifest.json`."""

    format: int = Field(description="The export format's version.")
    created_at: datetime.datetime
    app_version: str
    commit: Optional[str] = Field(
        description="The git commit of the app that exported it, if known."
    )
    schema_version: int = Field(description="The store's schema version.")
    accounts: list[str]
    name: Optional[str] = Field(None, description="A backup's name.")


BackupName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=100),
    Field(description="Unique (whatever the case), at most 100 characters."),
]


class Backup(Model):
    file: str = Field(
        description="Its file name in the backups folder, which identifies it in "
        "the API (its name, made safe for file names)."
    )
    name: str
    created_at: datetime.datetime
    size: int = Field(description="In bytes.")
    accounts: list[str]


class Backups(Model):
    folder: str = Field(description="The backups folder, in the data directory.")
    limit: int = Field(
        description="The most backups kept: at the limit, new ones are refused "
        "(none are ever deleted automatically)."
    )
    backups: list[Backup] = Field(description="Newest first.")
    default_names: dict[Literal["import", "restore"], str] = Field(
        description="The names a backup made before an import or a restore gets "
        "unless named, e.g. `before import on 01-11-2026` (in the web app's date "
        "format, with `-`), unique."
    )


class BackupRequest(Model):
    name: BackupName


class RestoreRequest(Model):
    dry_run: bool = True
    backup: bool = Field(
        True, description="Back up the current state first (refused at the limit)."
    )
    backup_name: Optional[str] = Field(
        None,
        description="The backup's name (if empty, `before restore on <date>`).",
    )


class ImportResult(Model):
    dry_run: bool
    manifest: ExportManifest
    raw_dir: str = Field(description="This host's raw folder.")
    raw_files: int = Field(
        description="Raw files in the export: the raw folder's contents become "
        "exactly these."
    )
    replaced_raw_files: list[str] = Field(
        description="Files in the raw folder that the export has too, which it "
        "replaces (as `<account>/<file>`, relative to the raw folder)."
    )
    removed_raw_files: list[str] = Field(
        description="Files in the raw folder that the export doesn't have, which "
        "are removed (relative to the raw folder). Only a backup made first keeps "
        "them."
    )
    unbacked_raw_files: list[str] = Field(
        description="Of the removed files, those no backup holds, since ingesting "
        "ignores them: hidden files, files in subfolders of accounts, and files "
        "outside account folders."
    )
    backup: Optional[Backup] = Field(
        description="The backup of the state before (none in a dry run, or if not "
        "asked)."
    )


# Raw files


class RawUploadFile(Model):
    name: str
    path: str = Field(description="Where it goes (or went), in the raw folder.")
    accepted: bool = Field(description="Whether a parser reads it.")
    check: IngestFile = Field(
        description="Its dry run, as the ingest dry run's files (`path` is where "
        "it goes)."
    )
    replaces: Optional[IngestFile] = Field(
        description="The dry run of the file of the same name it replaces, if one "
        "exists (to compare their coverage)."
    )


class RawUpload(Model):
    dry_run: bool
    account: str
    folder: str = Field(description="The account's folder, in the raw folder.")
    new_account: bool = Field(
        description="Whether the account's folder doesn't exist yet (the upload "
        "makes it)."
    )
    files: list[RawUploadFile]


# Glossary


class GlossaryEntry(Model):
    key: str = Field(description="Stable key, e.g. `kept`.")
    title: str = Field(description="Its name, e.g. `Kept (net)`.")
    section: str = Field(description="The heading it is under, e.g. `Cards`.")
    labels: list[str] = Field(
        description="The labels as shown, with placeholders such as `N` and `<month>`."
    )
    short: str = Field(description="The tooltip: one or two sentences.")
    computed: str = Field(
        description="How it is computed, in Markdown: what is included and left "
        "out. Links to other entries are `[text](#key)`."
    )
    related: list[str] = Field(default=[], description="Keys of related entries.")
