"""The service layer: every use case of finalyze, over the store and the pipeline.

The HTTP API (`finalyze.api`) is a thin adapter over `Service`, which takes and
returns the models of `finalyze.service.models`.

Transactions are the last ingested data plus the current store state. Ingesting
(parsing the raw files) only happens on request (`Service.ingest`). The
ingested data is cached until its files change, and the post-processed data
until it or the store changes (see `post_processed`), so writes are visible
immediately. Whatever replaces the store or the ingested data other than through
`Store` and `Service.ingest` (e.g. an import) must call `data_changed`.
"""

import contextlib
import dataclasses
import functools
import sqlite3
import threading
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO, Literal, Optional

import polars as pl
import pydantic
import toml
from pydantic.color import Color

from finalyze import config as config_module
from finalyze import keymap as keymap_registry
from finalyze.analysis import dashboard
from finalyze.config import Config, Filters, host
from finalyze.money import (
    CURRENCY_DECIMALS,
    exact_minor_units,
    from_minor_units,
    to_minor_units,
)
from finalyze.service import archive, backups, models, raw_upload
from finalyze.service import ingest as ingest_check
from finalyze.source import ingest as ingest_module
from finalyze.source.data import (
    FLOW_KINDS,
    LABEL_DELIMITER,
    SourceData,
    post_process,
)
from finalyze.source.identity import DuplicateIds
from finalyze.source.parsing import ParsingError
from finalyze.source.raw import load_source_data
from finalyze.source.schema import RAW_SCHEMA
from finalyze.source.tag import rule_index_expr, snapshot_of
from finalyze.store import (
    DEFAULT_GROUP,
    Annotation,
    Part,
    Rule,
    Store,
    StoreError,
    TagKind,
    TransactionSnapshot,
    data_version,
    note_data_changed,
    validate_rule_filters,
)


class ServiceError(Exception):
    """An invalid request. Subclasses tell its kind."""


class NotFound(ServiceError):
    pass


class Conflict(ServiceError):
    """The request conflicts with the current state."""


class InvalidRequest(ServiceError):
    pass


class KeymapError(InvalidRequest):
    """Keys that clash (or can't be bound), so the keymap wasn't saved."""

    def __init__(self, problems: list[str]):
        super().__init__("The keyboard shortcuts weren't saved: " + "; ".join(problems))
        self.problems = problems


class BatchError(InvalidRequest):
    """Some items of a batch are invalid, so nothing was written."""

    def __init__(self, errors: list[models.ItemError]):
        super().__init__(f"{len(errors)} invalid items, nothing was written")
        self.errors = errors


DASHBOARD_DEFAULTS_SETTING = "dashboard_defaults"
KEYMAP_SETTING = "keymap"
DISPLAY_SETTING = "display"
FIXED_LABELS = ("net worth", "available")
"""Labels of the dashboard's balance lines, besides accounts and sources."""

# Ingested data, by source directory: (files signature, frame)
_ingested_cache: dict[Path, tuple[tuple, pl.DataFrame]] = {}
_ingested_lock = threading.RLock()


def _ingested_files() -> list[Path]:
    return sorted(host().ingested_dir.glob("*.csv"))


def load_ingested() -> pl.DataFrame:
    """The last ingested data (empty if never ingested), cached until it changes."""
    with _ingested_lock:
        files = _ingested_files()
        signature = tuple(
            (file.name, file.stat().st_mtime_ns, file.stat().st_size) for file in files
        )
        ingested_dir = host().ingested_dir
        cached = _ingested_cache.get(ingested_dir)
        if cached is not None and cached[0] == signature:
            return cached[1]
        if files:
            data = load_source_data()
        else:
            data = pl.DataFrame(schema=RAW_SCHEMA)
        _ingested_cache[ingested_dir] = (signature, data)
        return data


# Post-processed data of `_processed_version`, by (id of the ingested data, store
# file, accounts), least recently used first. Each value keeps its ingested data,
# so that the id isn't reused while it is cached.
_processed_cache: dict[tuple, tuple[pl.DataFrame, pl.DataFrame]] = {}
_processed_version: Optional[int] = None
_processed_lock = threading.Lock()
PROCESSED_CACHE_SIZE = 4
"""How many post-processed frames are kept (e.g. of other accounts)."""


def post_processed(
    data: pl.DataFrame, store: Store, accounts: Optional[frozenset[str]] = None
) -> pl.DataFrame:
    """`post_process` of ingested data (of only some accounts, if given), cached
    until the data or any store changes (see `data_version`)."""
    version = data_version()
    key = (id(data), store.path, accounts)
    global _processed_version
    with _processed_lock:
        if _processed_version != version:
            _processed_cache.clear()
            _processed_version = version
        cached = _processed_cache.pop(key, None)
        if cached is not None:
            _processed_cache[key] = cached
            return cached[1]
    # Outside the lock: other requests don't wait (the same frame may be
    # computed twice, the same way)
    rows = data
    if accounts is not None:
        rows = data.filter(
            pl.col("account").is_in(pl.Series(sorted(accounts), dtype=pl.String))
        )
    frame = post_process(rows, store)
    with _processed_lock:
        # Only if nothing changed meanwhile (the frame may predate the change)
        if _processed_version == version == data_version():
            _processed_cache[key] = (data, frame)
            while len(_processed_cache) > PROCESSED_CACHE_SIZE:
                del _processed_cache[next(iter(_processed_cache))]
    return frame


def data_changed():
    """Forget the cached ingested and post-processed data, which are read again
    when next needed. For changes made other than through `Store` and
    `Service.ingest`, e.g. an import replacing the store and the ingested files
    (even with the same sizes and times)."""
    with _ingested_lock:
        _ingested_cache.clear()
    with _processed_lock:
        _processed_cache.clear()
    note_data_changed()


@contextlib.contextmanager
def open_service() -> Iterator["Service"]:
    """A service over the configured store, with its own connection."""
    with Store(host().db_file) as store:
        yield Service(store)


@dataclasses.dataclass(frozen=True)
class Record:
    """A current transaction, with its rule and annotation."""

    row: dict
    rule: Optional[Rule]
    rule_position: Optional[int]
    annotation: Optional[Annotation]

    @property
    def id(self) -> str:
        return self.row["id"]

    @functools.cached_property
    def annotation_applies(self) -> bool:
        """Whether the annotation has parts, which still sum to the amount."""
        if self.annotation is None or not self.annotation.parts:
            return False
        parts_sum = sum(part.amount for part in self.annotation.parts)
        return parts_sum == to_minor_units(self.row["amount"])

    @property
    def status(self) -> models.TransactionStatus:
        if self.rule is not None:
            return "rule"
        if self.annotation_applies:
            return "annotated"
        return "untagged"

    @property
    def kind_set(self) -> bool:
        """Whether a kind is set on any of its effective parts."""
        return (
            self.rule is None
            and self.annotation_applies
            and any(part.kind is not None for part in self.annotation.parts)
        )

    @property
    def labels(self) -> list[tuple[str, str]]:
        """The effective (tag, subtag) of each part."""
        if self.rule is not None:
            return [(self.rule.tag, self.rule.subtag)]
        if self.annotation_applies:
            return [(part.tag, part.subtag) for part in self.annotation.parts]
        return []


class View:
    """The current transactions, as of one read of the store."""

    def __init__(self, store: Store, data: pl.DataFrame):
        self.data = data
        self.kinds: dict[tuple[str, str], TagKind] = {
            (subtag.tag, subtag.subtag): subtag.kind
            for subtag in store.list_resolved_subtags()
        }
        roles = {group.name: group.role for group in store.list_groups()}
        self.account_groups = {
            account.name: (account.group, roles[account.group])
            for account in store.list_accounts()
        }
        self.rules = store.list_rules()
        self.annotations = {
            annotation.transaction_id: annotation
            for annotation in store.list_annotations()
        }
        positions = data.with_columns(rule_index_expr(self.rules))["rule_index"]
        self.records = [
            Record(
                row=row,
                rule=None if position is None else self.rules[position],
                rule_position=position,
                annotation=self.annotations.get(row["id"]),
            )
            for row, position in zip(data.iter_rows(named=True), positions, strict=True)
        ]
        self.by_id = {record.id: record for record in self.records}

    @functools.cached_property
    def _annotated_by_raw_description(self) -> dict[str, list[Record]]:
        annotated = {}
        for record in self.records:
            if record.status == "annotated":
                annotated.setdefault(record.row["raw_description"], []).append(record)
        return annotated

    def guess(self, record: Record) -> Optional[models.Guess]:
        """The tags of the annotated transaction with the same raw description
        nearest in date.

        Candidates are the other annotated transactions with the same raw
        description, before or after. Ties go to the later one. Without
        candidates, no guess.
        """
        candidates = [
            candidate
            for candidate in self._annotated_by_raw_description.get(
                record.row["raw_description"], []
            )
            if candidate.id != record.id
        ]
        if not candidates:
            return None
        target = record.row["date"]
        nearest = min(
            candidates,
            key=lambda candidate: (
                abs((candidate.row["date"] - target).days),
                -candidate.row["date"].toordinal(),
                candidate.id,
            ),
        )
        tag, subtag = nearest.labels[0]
        return models.Guess(
            tag=tag,
            subtag=subtag,
            basis="same_raw_description",
            from_transaction_id=nearest.id,
        )

    def get(self, transaction_id: str) -> Record:
        if transaction_id not in self.by_id:
            raise NotFound(f"No such transaction: {transaction_id}")
        return self.by_id[transaction_id]

    def query(self, query: models.TransactionQuery) -> list[Record]:
        return [record for record in self.records if _matches(self, record, query)]

    def orphans(self) -> list[Annotation]:
        return [
            annotation
            for transaction_id, annotation in self.annotations.items()
            if transaction_id not in self.by_id
        ]

    def overridden(self) -> list[Record]:
        return [
            record
            for record in self.records
            if record.rule is not None
            and record.annotation is not None
            and record.annotation.parts
        ]

    def kind(self, tag: str, subtag: str) -> TagKind:
        """The effective kind of a subtag (regular if unknown)."""
        return self.kinds.get((tag, subtag), "regular")

    def part_kind(self, part: Part) -> TagKind:
        """The effective kind of a part: set on it, or its subtag's."""
        return part.kind or self.kind(part.tag, part.subtag)

    def record_kinds(self, record: Record) -> list[TagKind]:
        """The effective kind of each part (regular if untagged)."""
        if record.rule is not None:
            return [self.kind(record.rule.tag, record.rule.subtag)]
        if record.annotation_applies:
            return [self.part_kind(part) for part in record.annotation.parts]
        return ["regular"]

    def group(self, account: str) -> tuple[str, str]:
        """The group of an account and its role (the default if unknown)."""
        if account in self.account_groups:
            return self.account_groups[account]
        return DEFAULT_GROUP, "spending"

    def label_counts(self) -> dict[tuple[str, str], int]:
        """The number of current transactions by effective (tag, subtag)."""
        counts = {}
        for record in self.records:
            for label in set(record.labels):
                counts[label] = counts.get(label, 0) + 1
        return counts


class Service:
    def __init__(self, store: Store):
        self.store = store

    def view(self) -> View:
        return View(self.store, load_ingested())

    # Status and ingest

    def status(self) -> models.Status:
        view = self.view()
        times = ingest_check.ingest_times()
        statuses = [record.status for record in view.records]
        return models.Status(
            db_path=str(self.store.path),
            ingested_dir=str(host().ingested_dir),
            ingested_changed_at=times.changed_at,
            ingested_checked_at=times.checked_at,
            transactions=len(view.records),
            untagged=statuses.count("untagged"),
            annotated=statuses.count("annotated"),
            rule_matched=statuses.count("rule"),
            overridden=len(view.overridden()),
            needs_review=sum(
                record.annotation is not None and record.annotation.needs_review
                for record in view.records
            ),
            orphans=len(view.orphans()),
            accounts=[account.name for account in self.store.list_accounts()],
        )

    def ingest(self) -> models.IngestSummary:
        """Parse the raw files into the ingested data, and record when it ran (and
        changed the data, if it did)."""
        with _ingested_lock:
            before = self.view()
            times_before = ingest_check.ingest_times()
            try:
                ingested = ingest_module.ingest(self.store)
                after = self.view()
            except (ParsingError, DuplicateIds, ValueError, OSError) as error:
                raise InvalidRequest(f"Ingest failed: {error}") from error
            changes = ingest_check.compare(before.data, after.data)
            times = ingest_check.record_ingest(times_before, changed=changes.any)
        before_orphans = {annotation.transaction_id for annotation in before.orphans()}
        orphans = after.orphans()
        return models.IngestSummary(
            accounts={name: data.height for name, data in ingested.items()},
            transactions=len(after.records),
            new_ids=sorted(changes.new["id"]),
            vanished_ids=sorted(changes.vanished["id"]),
            changed_ids=sorted(changes.changed["id"]),
            orphans=len(orphans),
            new_orphan_ids=sorted(
                annotation.transaction_id
                for annotation in orphans
                if annotation.transaction_id not in before_orphans
            ),
            data_changed=changes.any,
            ingested_changed_at=times.changed_at,
            ingested_checked_at=times.checked_at,
        )

    def ingest_dry_run(self) -> models.IngestDryRun:
        """Parse each raw file on its own, and flag likely problems. Writes nothing."""
        return ingest_check.dry_run()

    def preview_ingest(self) -> models.IngestPreview:
        """What an ingest would change, without writing anything."""
        with _ingested_lock:
            before = self.view()
            try:
                after_data = ingest_check.after_ingest(ingest_module.parse_accounts())
            except (ParsingError, DuplicateIds, ValueError, OSError) as error:
                raise InvalidRequest(f"Ingest would fail: {error}") from error
        changes = ingest_check.compare(before.data, after_data)
        after_ids = set(after_data["id"])
        orphan_ids = {annotation.transaction_id for annotation in before.orphans()}
        vanished = [record for record in before.records if record.id not in after_ids]
        before_counts = _account_counts(before.data)
        after_counts = _account_counts(after_data)
        return models.IngestPreview(
            accounts={
                account: models.IngestAccountCounts(
                    before=before_counts.get(account, 0),
                    after=after_counts.get(account, 0),
                )
                for account in sorted(before_counts.keys() | after_counts.keys())
            },
            transactions=after_data.height,
            unchanged=len(before.records) - len(vanished) - changes.changed.height,
            new=[_ingest_row(row) for row in changes.new.iter_rows(named=True)],
            vanished=[_ingest_row(record.row) for record in vanished],
            changed=[_ingest_row(row) for row in changes.changed.iter_rows(named=True)],
            new_orphans=[
                _stored_annotation_out(
                    before, record.annotation, _rule_ref(before, record)
                )
                for record in vanished
                if record.annotation is not None
            ],
            restored_orphan_ids=sorted(orphan_ids & after_ids),
        )

    # Transactions

    def list_transactions(
        self, request: models.TransactionListRequest
    ) -> models.TransactionPage:
        view = self.view()
        records = view.query(request)
        records.sort(key=lambda record: record.id)
        records.sort(key=lambda record: _sort_value(record, request.sort))
        if request.descending:
            records.reverse()
        end = None if request.limit is None else request.offset + request.limit
        return models.TransactionPage(
            total=len(records),
            offset=request.offset,
            limit=request.limit,
            items=[
                _transaction_out(view, record)
                for record in records[request.offset : end]
            ],
        )

    def get_transaction(self, transaction_id: str) -> models.Transaction:
        view = self.view()
        return _transaction_out(view, view.get(transaction_id))

    # Annotations

    def set_annotation(
        self, transaction_id: str, request: models.AnnotationSetRequest
    ) -> models.Transaction:
        view = self.view()
        annotation = self._build_annotation(
            view,
            transaction_id,
            request,
            author=request.author,
            allow_new_tags=request.allow_new_tags,
        )
        self.store.set_annotation(annotation)
        return self.get_transaction(transaction_id)

    def set_annotations(self, request: models.BatchRequest) -> models.BatchResult:
        """Set many annotations at once: all of them, or none if any is invalid."""
        return self._write_batch(request, self._build_annotation)

    def patch_annotation(
        self, transaction_id: str, request: models.AnnotationPatchRequest
    ) -> models.Transaction:
        """Change only the given fields of a transaction's annotation (creating it
        if needed), keeping the others."""
        view = self.view()
        annotation = self._patched_annotation(
            view,
            transaction_id,
            request,
            author=request.author,
            allow_new_tags=request.allow_new_tags,
        )
        self.store.set_annotation(annotation)
        return self.get_transaction(transaction_id)

    def patch_annotations(
        self, request: models.PatchBatchRequest
    ) -> models.BatchResult:
        """Patch many annotations at once: all of them, or none if any is invalid."""
        return self._write_batch(request, self._patched_annotation)

    def _write_batch(
        self, request: models.BatchRequest | models.PatchBatchRequest, build
    ) -> models.BatchResult:
        """Write the annotations that `build` makes of a batch's items, all or
        none: invalid items are reported together, and nothing is written."""
        view = self.view()
        annotations = []
        errors = []
        seen_ids = set()
        for index, item in enumerate(request.items):
            try:
                if item.id in seen_ids:
                    raise InvalidRequest(f"Duplicate transaction ID: {item.id}")
                seen_ids.add(item.id)
                annotations.append(
                    build(
                        view,
                        item.id,
                        item,
                        author=request.author,
                        allow_new_tags=request.allow_new_tags,
                    )
                )
            except ServiceError as error:
                errors.append(
                    models.ItemError(index=index, id=item.id, message=str(error))
                )
        if errors:
            raise BatchError(errors)
        if not request.dry_run:
            self.store.set_annotations(annotations)
        return models.BatchResult(
            dry_run=request.dry_run, updated_ids=[item.id for item in request.items]
        )

    def clear_annotation(self, transaction_id: str) -> models.Transaction:
        self.view().get(transaction_id)
        self.store.delete_annotations([transaction_id])
        return self.get_transaction(transaction_id)

    def clear_annotations(self, request: models.ClearRequest) -> models.ClearResult:
        """Delete annotations (of current or orphaned transactions)."""
        stored_ids = {
            annotation.transaction_id for annotation in self.store.list_annotations()
        }
        cleared = [id_ for id_ in request.ids if id_ in stored_ids]
        self.store.delete_annotations(cleared)
        return models.ClearResult(
            cleared_ids=cleared,
            not_annotated_ids=[id_ for id_ in request.ids if id_ not in stored_ids],
        )

    def review(self, request: models.ReviewRequest) -> models.ReviewResult:
        """Flag annotations for review, or clear the flag (mark them reviewed)."""
        stored_ids = {
            annotation.transaction_id for annotation in self.store.list_annotations()
        }
        if missing := [id_ for id_ in request.ids if id_ not in stored_ids]:
            raise NotFound(f"No annotation for transactions: {', '.join(missing)}")
        self.store.set_needs_review(request.ids, request.needs_review)
        return models.ReviewResult(
            needs_review=request.needs_review, updated_ids=request.ids
        )

    def set_kind(self, request: models.KindRequest) -> models.KindResult:
        """Set the kind on transactions (or clear it), all or none. Only their
        annotations' parts can have a kind, so rules' transactions can't."""
        view = self.view()
        annotations = []
        errors = []
        for index, transaction_id in enumerate(request.ids):
            try:
                record = view.get(transaction_id)
                if record.rule is not None:
                    raise InvalidRequest(
                        f"{transaction_id} is tagged by rule {record.rule.id}, "
                        "which a kind can't be set over"
                    )
                if not record.annotation_applies:
                    raise InvalidRequest(f"{transaction_id} is untagged, tag it first")
                parts = record.annotation.parts
                if request.part is not None and request.part >= len(parts):
                    raise InvalidRequest(
                        f"{transaction_id} has {len(parts)} parts, no part "
                        f"{request.part}"
                    )
                annotations.append(
                    _written_by(
                        record.annotation,
                        dataclasses.replace(
                            record.annotation,
                            parts=tuple(
                                dataclasses.replace(part, kind=request.kind)
                                if request.part in (None, position)
                                else part
                                for position, part in enumerate(parts)
                            ),
                        ),
                        request.author,
                    )
                )
            except ServiceError as error:
                errors.append(
                    models.ItemError(index=index, id=transaction_id, message=str(error))
                )
        if errors:
            raise BatchError(errors)
        self.store.set_annotations(annotations)
        return models.KindResult(kind=request.kind, updated_ids=request.ids)

    def bulk_annotate(
        self, request: models.BulkAnnotateRequest
    ) -> models.BulkAnnotateResult:
        """Change the given fields of every transaction of a query, keeping the
        others. Rules' transactions are skipped, and so are those a field can't
        apply to without tags (a kind, or the review flag)."""
        if request.tag is not None:
            self._check_tags_exist(
                [(request.tag, request.subtag)], allow_new_tags=request.allow_new_tags
            )
        view = self.view()
        matched = view.query(request.query)
        matched.sort(key=lambda record: (record.row["date"], record.id))
        rule_ids = []
        untagged_ids = []
        unchanged_ids = []
        changes = []
        for record in matched:
            if record.rule is not None:
                rule_ids.append(record.id)
            elif (annotation := _bulk_annotation(record, request)) is None:
                untagged_ids.append(record.id)
            elif _same_content(record.annotation, annotation):
                unchanged_ids.append(record.id)
            else:
                changes.append((record, annotation))
        result = models.BulkAnnotateResult(
            dry_run=request.dry_run,
            affected_ids=[record.id for record, _annotation in changes],
            replaced_ids=[
                record.id
                for record, _annotation in changes
                if request.tag is not None
                and record.annotation is not None
                and record.annotation.parts
            ],
            unchanged_ids=unchanged_ids,
            skipped_rule_ids=rule_ids,
            skipped_untagged_ids=untagged_ids,
        )
        if request.dry_run:
            return result
        if request.expected_ids is not None and set(request.expected_ids) != set(
            result.affected_ids
        ):
            raise Conflict(
                "The affected transactions differ from expected_ids, "
                "preview again before committing"
            )
        # An annotation left without tags and description is deleted, as when cleared
        self.store.set_annotations(
            [
                annotation
                for _record, annotation in changes
                if annotation.parts or annotation.description
            ]
        )
        self.store.delete_annotations(
            [
                record.id
                for record, annotation in changes
                if record.annotation is not None
                and not (annotation.parts or annotation.description)
            ]
        )
        return result

    def _build_annotation(
        self,
        view: View,
        transaction_id: str,
        annotation: models.AnnotationIn,
        *,
        author: str,
        allow_new_tags: bool,
    ) -> Annotation:
        """The annotation that replaces a transaction's, keeping its authors."""
        record = view.get(transaction_id)
        snapshot = snapshot_of(record.row)
        if not annotation.parts and not annotation.description:
            raise InvalidRequest(
                "An annotation without parts needs a description (clear it instead)"
            )
        parts = self._build_parts(
            snapshot, annotation.parts, allow_new_tags=allow_new_tags
        )
        return _written_by(
            record.annotation,
            Annotation(
                transaction_id,
                snapshot,
                parts,
                annotation.description,
                authors=_authors(record.annotation),
                needs_review=annotation.needs_review,
            ),
            author,
        )

    def _patched_annotation(
        self,
        view: View,
        transaction_id: str,
        patch: models.AnnotationPatch,
        *,
        author: str,
        allow_new_tags: bool,
    ) -> Annotation:
        """A transaction's annotation (or a new one) with only the patch's given
        fields changed."""
        record = view.get(transaction_id)
        snapshot = snapshot_of(record.row)
        existing = record.annotation
        given = patch.model_fields_set
        parts = existing.parts if existing is not None else ()
        if "parts" in given:
            parts = self._build_parts(
                snapshot, patch.parts or [], allow_new_tags=allow_new_tags
            )
        if "kind" in given:
            if record.rule is not None:
                raise InvalidRequest(
                    f"{transaction_id} is tagged by rule {record.rule.id}, "
                    "which a kind can't be set over"
                )
            if not parts:
                raise InvalidRequest(f"{transaction_id} is untagged, tag it first")
            parts = tuple(dataclasses.replace(part, kind=patch.kind) for part in parts)
        description = existing.description if existing is not None else None
        if "description" in given:
            description = patch.description or None
        needs_review = existing.needs_review if existing is not None else False
        if "needs_review" in given:
            needs_review = patch.needs_review
        if not parts and not description:
            raise InvalidRequest(
                "An annotation without parts needs a description (clear it instead)"
            )
        return _written_by(
            existing,
            Annotation(
                transaction_id,
                snapshot,
                parts,
                description,
                authors=_authors(existing),
                needs_review=needs_review,
            ),
            author,
        )

    def _build_parts(
        self,
        snapshot: TransactionSnapshot,
        parts: list[models.PartIn],
        *,
        allow_new_tags: bool,
    ) -> tuple[Part, ...]:
        """Parts in minor units, which must sum exactly to the transaction."""
        if not parts:
            return ()
        self._check_tags_exist(
            [(part.tag, part.subtag) for part in parts],
            allow_new_tags=allow_new_tags,
        )
        if len(parts) == 1 and parts[0].amount is None:
            amounts = [snapshot.amount]
        elif any(part.amount is None for part in parts):
            raise InvalidRequest("Every part of a split needs an amount")
        else:
            try:
                amounts = [exact_minor_units(part.amount) for part in parts]
            except ValueError as error:
                raise InvalidRequest(str(error)) from error
        if sum(amounts) != snapshot.amount:
            raise InvalidRequest(
                f"Parts sum to {from_minor_units(sum(amounts))}, expected the "
                f"transaction amount {from_minor_units(snapshot.amount)}"
            )
        return tuple(
            Part(amount, part.tag, part.subtag, part.description, part.kind)
            for amount, part in zip(amounts, parts, strict=True)
        )

    def _check_tags_exist(self, labels: list[tuple[str, str]], *, allow_new_tags: bool):
        if allow_new_tags:
            return
        unknown = [
            f"{tag}/{subtag}"
            for tag, subtag in labels
            if not self.store.has_subtag(tag, subtag)
        ]
        if unknown:
            raise InvalidRequest(
                f"Unknown tags: {', '.join(unknown)} "
                "(allow new tags explicitly to create them)"
            )

    # Tags

    def list_tags(self) -> list[models.TagOut]:
        view = self.view()
        label_counts = view.label_counts()
        tag_counts = {}
        for record in view.records:
            for tag in {tag for tag, _subtag in record.labels}:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
        usage = self.store.subtag_usage()
        subtags_by_tag = {}
        for subtag in self.store.list_subtags():
            subtags_by_tag.setdefault(subtag.tag, []).append(subtag)
        tags = []
        for tag in self.store.list_tags():
            subtags = []
            for subtag in subtags_by_tag.get(tag.name, []):
                annotations, rules = usage[(tag.name, subtag.name)]
                subtags.append(
                    models.SubtagOut(
                        name=subtag.name,
                        color=subtag.color,
                        kind=subtag.kind,
                        effective_color=subtag.color or tag.color,
                        effective_kind=subtag.kind or tag.kind,
                        annotations=annotations,
                        rules=rules,
                        transactions=label_counts.get((tag.name, subtag.name), 0),
                    )
                )
            tags.append(
                models.TagOut(
                    name=tag.name,
                    color=tag.color,
                    kind=tag.kind,
                    transactions=tag_counts.get(tag.name, 0),
                    subtags=subtags,
                )
            )
        return tags

    def get_tag(self, tag: str) -> models.TagOut:
        for tag_out in self.list_tags():
            if tag_out.name == tag:
                return tag_out
        raise NotFound(f"No such tag: {tag!r}")

    def create_tag(self, request: models.TagCreate) -> models.TagOut:
        if request.subtag is None:
            if self.store.has_tag(request.tag):
                raise Conflict(f"Tag {request.tag!r} already exists")
        elif self.store.has_subtag(request.tag, request.subtag):
            raise Conflict(f"Subtag {request.tag!r}/{request.subtag!r} already exists")
        update = models.TagPropertiesUpdate.model_validate(
            request.model_dump(exclude_none=True)
        )
        self._update_tag_properties(update)
        return self.get_tag(request.tag)

    def update_tag_properties(
        self, request: models.TagPropertiesUpdate
    ) -> models.TagOut:
        self._check_tag_ref(request)
        self._update_tag_properties(request)
        return self.get_tag(request.tag)

    def _update_tag_properties(self, request: models.TagPropertiesUpdate):
        properties = {}
        if "color" in request.model_fields_set:
            properties["color"] = _valid_color(request.color)
        if "kind" in request.model_fields_set:
            properties["kind"] = request.kind
        if request.subtag is None:
            if "kind" in properties and properties["kind"] is None:
                raise InvalidRequest("A tag's kind can't be null")
            self.store.update_tag(request.tag, **properties)
        else:
            self.store.update_subtag(request.tag, request.subtag, **properties)

    def rename_tag(self, request: models.TagRename) -> models.TagOut:
        self._check_tag_ref(request)
        if request.subtag is None:
            self.store.rename_tag(request.tag, request.new_name)
            return self.get_tag(request.new_name)
        self.store.rename_subtag(request.tag, request.subtag, request.new_name)
        return self.get_tag(request.tag)

    def merge_tag(self, request: models.TagMerge) -> models.TagOut:
        """Merge into the target, which keeps its own properties."""
        self._check_tag_ref(request)
        if request.subtag is None:
            if request.into_subtag is not None:
                raise InvalidRequest("Merge a tag into a tag (without into_subtag)")
            self._check_tag_ref(models.TagRef(tag=request.into_tag))
            self.store.merge_tag(request.tag, request.into_tag)
        else:
            if request.into_subtag is None:
                raise InvalidRequest("Merging a subtag requires into_subtag")
            self.store.merge_subtag(
                request.tag, request.subtag, request.into_tag, request.into_subtag
            )
        return self.get_tag(request.into_tag)

    def delete_tag(self, request: models.TagRef):
        """Delete a tag (with its subtags) or a subtag, only if unused."""
        self._check_tag_ref(request)
        try:
            if request.subtag is None:
                self.store.delete_tag(request.tag)
            else:
                self.store.delete_subtag(request.tag, request.subtag)
        except StoreError as error:
            raise Conflict(str(error)) from error

    def _check_tag_ref(self, ref: models.TagRef):
        if ref.subtag is None:
            if not self.store.has_tag(ref.tag):
                raise NotFound(f"No such tag: {ref.tag!r}")
        elif not self.store.has_subtag(ref.tag, ref.subtag):
            raise NotFound(f"No such subtag: {ref.tag!r}/{ref.subtag!r}")

    # Accounts and groups

    def list_accounts(self) -> list[models.AccountOut]:
        data = self.view().data
        counts_by_account = dict(data["account"].value_counts().iter_rows())
        sources_by_account = dict(
            data.group_by("account").agg(pl.col("source").unique().sort()).iter_rows()
        )
        roles = {group.name: group.role for group in self.store.list_groups()}
        return [
            models.AccountOut(
                name=account.name,
                group=account.group,
                group_role=roles[account.group],
                color=account.color,
                transactions=counts_by_account.get(account.name, 0),
                sources=sources_by_account.get(account.name, []),
            )
            for account in self.store.list_accounts()
        ]

    def balance(self, query: models.BalanceQuery) -> models.BalanceOnDate:
        """A balance at the end of a day, with every row up to it (of every
        kind), as the dashboard's balances."""
        data = load_ingested()
        if data.height == 0:
            raise Conflict("No ingested data, ingest first")
        frame = post_processed(data, self.store).filter(~pl.col("is_sentinel_tick"))
        date = frame["date"].max() if query.date is None else query.date
        label, group = "net worth", query.group
        if query.account is not None:
            frame = frame.filter(pl.col("account") == query.account)
            if frame.height == 0:
                raise NotFound(f"No transactions in account {query.account!r}")
            label, group = query.account, frame["group"][0]
            if query.source is not None:
                frame = frame.filter(pl.col("source") == query.source)
                label = f"{query.account}{LABEL_DELIMITER}{query.source}"
                if frame.height == 0:
                    raise NotFound(f"No transactions in {label!r}")
        elif query.group is not None:
            self._get_group(query.group)
            frame = frame.filter(pl.col("group") == query.group)
            label = query.group
        rows = frame.filter(pl.col("date") <= date)
        return models.BalanceOnDate(
            date=date,
            label=label,
            account=query.account,
            source=query.source,
            group=group,
            balance=round(rows["amount"].sum(), CURRENCY_DECIMALS),
            rows=rows.height,
            last_date=rows["date"].max(),
        )

    def update_account(
        self, name: str, request: models.AccountUpdate
    ) -> models.AccountOut:
        if name not in {account.name for account in self.store.list_accounts()}:
            raise NotFound(f"No such account: {name!r}")
        properties = {}
        if "group" in request.model_fields_set:
            if request.group is None:
                raise InvalidRequest("An account's group can't be null")
            self._get_group(request.group)
            properties["group"] = request.group
        if "color" in request.model_fields_set:
            properties["color"] = _valid_color(request.color)
        self.store.update_account(name, **properties)
        return next(account for account in self.list_accounts() if account.name == name)

    def list_groups(self) -> list[models.GroupOut]:
        accounts_by_group = {}
        for account in self.store.list_accounts():
            accounts_by_group.setdefault(account.group, []).append(account.name)
        return [
            models.GroupOut(
                name=group.name,
                role=group.role,
                color=group.color,
                position=position,
                accounts=accounts_by_group.get(group.name, []),
            )
            for position, group in enumerate(self.store.list_groups())
        ]

    def _get_group(self, name: str) -> models.GroupOut:
        for group in self.list_groups():
            if group.name == name:
                return group
        raise NotFound(f"No such group: {name!r}")

    def create_group(self, request: models.GroupCreate) -> models.GroupOut:
        if any(group.name == request.name for group in self.list_groups()):
            raise Conflict(f"Group {request.name!r} already exists")
        self.store.add_group(
            request.name,
            request.role,
            color=_valid_color(request.color),
            position=request.position,
        )
        return self._get_group(request.name)

    def update_group(self, name: str, request: models.GroupUpdate) -> models.GroupOut:
        self._get_group(name)
        fields = request.model_fields_set
        properties = {}
        if "new_name" in fields and request.new_name is not None:
            if request.new_name != name and any(
                group.name == request.new_name for group in self.list_groups()
            ):
                raise Conflict(f"Group {request.new_name!r} already exists")
            properties["new_name"] = request.new_name
        if "role" in fields:
            if request.role is None:
                raise InvalidRequest("A group's role can't be null")
            properties["role"] = request.role
        if "color" in fields:
            properties["color"] = _valid_color(request.color)
        self.store.update_group(name, **properties)
        name = properties.get("new_name", name)
        if request.position is not None:
            self.store.move_group(name, request.position)
        return self._get_group(name)

    def delete_group(self, name: str):
        """Delete a group without accounts."""
        group = self._get_group(name)
        if group.accounts:
            raise Conflict(
                f"Group {name!r} has accounts ({', '.join(group.accounts)}), "
                "move them first"
            )
        self.store.delete_group(name)

    # Rules

    def list_rules(self) -> list[models.RuleOut]:
        view = self.view()
        counts = {}
        for record in view.records:
            if record.rule_position is not None:
                counts[record.rule_position] = counts.get(record.rule_position, 0) + 1
        return [
            models.RuleOut(
                id=rule.id,
                position=position,
                filters=rule.filters,
                tag=rule.tag,
                subtag=rule.subtag,
                transactions=counts.get(position, 0),
            )
            for position, rule in enumerate(view.rules)
        ]

    def get_rule(self, rule_id: int) -> models.RuleOut:
        for rule in self.list_rules():
            if rule.id == rule_id:
                return rule
        raise NotFound(f"No such rule: {rule_id}")

    def create_rule(self, request: models.RuleCreate) -> models.RuleOut:
        _check_rule_filters(request.filters)
        self._check_tags_exist(
            [(request.tag, request.subtag)], allow_new_tags=request.allow_new_tags
        )
        rule_id = self.store.add_rule(
            request.filters, request.tag, request.subtag, position=request.position
        )
        return self.get_rule(rule_id)

    def update_rule(self, rule_id: int, request: models.RuleUpdate) -> models.RuleOut:
        self.get_rule(rule_id)
        fields = request.model_fields_set
        changes = {}
        if "filters" in fields:
            if request.filters is None:
                raise InvalidRequest("A rule's filters can't be null")
            _check_rule_filters(request.filters)
            changes["filters"] = request.filters
        if "tag" in fields or "subtag" in fields:
            if request.tag is None or request.subtag is None:
                raise InvalidRequest("A rule's tag and subtag are changed together")
            self._check_tags_exist(
                [(request.tag, request.subtag)],
                allow_new_tags=request.allow_new_tags,
            )
            changes["tag"] = request.tag
            changes["subtag"] = request.subtag
        self.store.update_rule(rule_id, **changes)
        if request.position is not None:
            self.store.move_rule(rule_id, request.position)
        return self.get_rule(rule_id)

    def delete_rule(self, rule_id: int):
        self.get_rule(rule_id)
        self.store.delete_rule(rule_id)

    def reorder_rules(self, request: models.RuleReorder) -> list[models.RuleOut]:
        try:
            self.store.reorder_rules(request.rule_ids)
        except StoreError as error:
            raise InvalidRequest(str(error)) from error
        return self.list_rules()

    def preview_rule(self, request: models.RulePreviewRequest) -> models.RulePreview:
        """What a new or changed rule would match, tag and override."""
        _check_rule_filters(request.filters)
        view = self.view()
        rules = [rule for rule in view.rules if rule.id != request.rule_id]
        position = request.position
        if position is None:
            position = len(rules)
            if request.rule_id is not None:
                position = self.get_rule(request.rule_id).position
        position = min(position, len(rules))
        candidate = Rule(id=-1, filters=request.filters, tag="", subtag="")
        hypothetical = [*rules[:position], candidate, *rules[position:]]
        data = view.data.with_columns(
            matched=request.filters.predicate,
            effective=rule_index_expr(hypothetical) == position,
        ).sort("date", "id")
        matched_ids = data.filter("matched")["id"].to_list()
        effective = [view.by_id[id_] for id_ in data.filter("effective")["id"]]
        return models.RulePreview(
            matched_ids=matched_ids,
            effective_ids=[record.id for record in effective],
            newly_tagged_ids=[
                record.id for record in effective if record.status == "untagged"
            ],
            overridden_annotation_ids=[
                record.id for record in effective if record.annotation is not None
            ],
        )

    # Orphans and overridden annotations

    def list_orphans(self) -> list[models.StoredAnnotation]:
        view = self.view()
        return [_stored_annotation_out(view, orphan) for orphan in view.orphans()]

    def list_overridden(self) -> list[models.StoredAnnotation]:
        view = self.view()
        return [
            _stored_annotation_out(view, record.annotation, _rule_ref(view, record))
            for record in view.overridden()
        ]

    def delete_orphans(self, request: models.OrphanDelete) -> models.OrphanDeleteResult:
        orphan_ids = {orphan.transaction_id for orphan in self.view().orphans()}
        if not_orphans := [id_ for id_ in request.ids if id_ not in orphan_ids]:
            raise InvalidRequest(f"Not orphaned annotations: {', '.join(not_orphans)}")
        self.store.delete_annotations(request.ids)
        return models.OrphanDeleteResult(deleted_ids=request.ids)

    # Analysis

    def analysis_frame(self, query: models.AnalysisQuery) -> pl.DataFrame:
        """Enriched rows (see `ENRICHED_SCHEMA`), one per transaction part."""
        data = load_ingested()
        if data.height == 0:
            raise Conflict("No ingested data, ingest first")
        defaults = self.dashboard_defaults()
        frame = post_processed(data, self.store)
        source = SourceData(
            frame,
            net_by_tag=defaults.net_by_tag,
            groups=None if query.groups is None else frozenset(query.groups),
        )
        kinds = None if query.kinds is None else frozenset(query.kinds)
        if kinds is None and (query.incomes or query.expenses):
            # Incomes and expenses are of these kinds only, as in the dashboard
            kinds = FLOW_KINDS
        frame = source.get(
            kinds=kinds,
            all_groups=query.all_groups,
            incomes=query.incomes,
            expenses=query.expenses,
            net_by_subtag=query.net_by_subtag,
            sentinels=query.sentinels,
        )
        return frame.filter(_row_filters_expr(query)).sort("date", "id")

    def analysis_rows(self, query: models.AnalysisQuery) -> models.AnalysisRows:
        frame = self.analysis_frame(query)
        return models.AnalysisRows(total=frame.height, rows=frame.to_dicts())

    def aggregate(self, query: models.AggregateQuery) -> models.Aggregate:
        """Sum and count of analysis rows by groups."""
        frame = self.analysis_frame(query)
        group_by = list(dict.fromkeys(query.group_by))
        amount_sum = pl.col("amount").sum().round(CURRENCY_DECIMALS)
        if group_by:
            grouped = frame.group_by(group_by).agg(amount=amount_sum, count=pl.len())
            grouped = grouped.sort(group_by, nulls_last=True)
        else:
            grouped = frame.select(amount=amount_sum, count=pl.len())
        return models.Aggregate(group_by=group_by, rows=grouped.to_dicts())

    # Dashboard

    def _dashboard_source(
        self, options: models.DashboardOptions
    ) -> tuple[SourceData, dashboard.Periods]:
        """The data and periods of resolved options."""
        data = load_ingested()
        if data.height == 0:
            raise Conflict("No ingested data, ingest first")
        accounts = None
        if options.accounts is not None:
            accounts = frozenset(options.accounts)
            if (
                not data["account"]
                .is_in(pl.Series(sorted(accounts), dtype=pl.String))
                .any()
            ):
                raise InvalidRequest("No transactions in these accounts")
        frame = post_processed(data, self.store, accounts)
        source = SourceData(
            frame,
            net_by_tag=options.net_by_tag,
            groups=None if options.groups is None else frozenset(options.groups),
            tags=None if options.tags is None else frozenset(options.tags),
            include_one_offs=options.include_one_offs,
        )
        periods = dashboard.resolve_periods(
            source,
            options.period,
            start_month=options.start_month,
            end_month=options.end_month,
            complete_months_only=bool(options.complete_months_only),
        )
        return source, periods

    def dashboard(self, options: models.DashboardOptions) -> models.Dashboard:
        """The dashboard's analysis tables over a period."""
        options = self._resolve_dashboard_options(options)
        source, periods = self._dashboard_source(options)
        groups = self.list_groups()
        data = dashboard.build(
            source,
            periods,
            [group.name for group in groups],
            subtag_trends=options.subtag_trends,
        )
        return models.Dashboard(
            options=options,
            colors=self.chart_colors(),
            groups=groups,
            **data,
        )

    def dashboard_rows(
        self, request: models.DashboardRowsRequest
    ) -> models.AnalysisRows:
        """The rows behind a number of the dashboard."""
        options = self._resolve_dashboard_options(request.options)
        source, periods = self._dashboard_source(options)
        selection = request.selection.model_dump()
        if selection["kinds"] is not None:
            selection["kinds"] = frozenset(selection["kinds"])
        selection = dashboard.Selection(**selection)
        frame = dashboard.selected_rows(source, periods, selection)
        return models.AnalysisRows(total=frame.height, rows=frame.to_dicts())

    def _resolve_dashboard_options(
        self, options: models.DashboardOptions
    ) -> models.DashboardOptions:
        """The options, with unset ones from their defaults."""
        defaults = self.dashboard_defaults().model_dump()
        resolved = {
            name: default if getattr(options, name) is None else getattr(options, name)
            for name, default in defaults.items()
        }
        return options.model_copy(update=resolved)

    # Settings

    def dashboard_defaults(self) -> models.DashboardDefaults:
        stored = self.store.get_setting(DASHBOARD_DEFAULTS_SETTING, {})
        return models.DashboardDefaults.model_validate(stored)

    def set_dashboard_defaults(
        self, defaults: models.DashboardDefaults
    ) -> models.DashboardDefaults:
        self.store.set_setting(DASHBOARD_DEFAULTS_SETTING, defaults.model_dump())
        return self.dashboard_defaults()

    def display_settings(self) -> models.DisplaySettings:
        stored = self.store.get_setting(DISPLAY_SETTING, {})
        return models.DisplaySettings.model_validate(stored)

    def set_display_settings(
        self, settings: models.DisplaySettings
    ) -> models.DisplaySettings:
        self.store.set_setting(DISPLAY_SETTING, settings.model_dump())
        return self.display_settings()

    def keymap(self) -> models.Keymap:
        stored = self.store.get_setting(KEYMAP_SETTING, {})
        return models.Keymap.model_validate(stored)

    def set_keymap(self, keymap: models.Keymap) -> models.Keymap:
        """Save the keyboard shortcuts, unless keys clash: checked like the web
        app's Settings page does, with the same registry."""
        problems = keymap_registry.problems(
            keymap.directions.model_dump(), keymap.overrides
        )
        if problems:
            raise KeymapError(problems)
        self.store.set_setting(KEYMAP_SETTING, keymap.model_dump())
        return self.keymap()

    def chart_colors(self) -> dict[str, str]:
        """Colors of labels: accounts and tags (their own colors win), and other
        labels."""
        own_colors = [
            *((account.name, account.color) for account in self.store.list_accounts()),
            *((tag.name, tag.color) for tag in self.store.list_tags()),
        ]
        return self.store.label_colors() | {
            name: color for name, color in own_colors if color is not None
        }

    def label_colors(self) -> models.LabelColors:
        data = load_ingested()
        account_sources = sorted(
            {
                f"{account}{LABEL_DELIMITER}{source}"
                for account, source in data.select("account", "source").unique().rows()
            }
        )
        return models.LabelColors(
            colors=self.store.label_colors(),
            labels=[*FIXED_LABELS, *account_sources],
        )

    def set_label_color(self, request: models.LabelColor) -> models.LabelColors:
        own = {account.name for account in self.store.list_accounts()} | {
            tag.name for tag in self.store.list_tags()
        }
        if request.label in own:
            raise InvalidRequest(
                f"{request.label!r} is an account or a tag: set its own color"
            )
        self.store.set_label_color(request.label, _valid_color(request.color))
        return self.label_colors()

    def config_settings(self) -> models.ConfigSettings:
        config_file = config_module.config_file_of(config_module.LOADED_CONFIG_DIR)
        try:
            data = toml.loads(config_file.read_text())
            file_config = Config.model_validate(data)
        except (OSError, toml.TomlDecodeError, pydantic.ValidationError) as error:
            raise Conflict(f"Can't read the config file: {error}") from error
        return models.ConfigSettings(file=str(config_file), config=file_config)

    def set_config(self, new_config: Config) -> models.ConfigSettings:
        """Write the config file (without comments), and use it."""
        data = new_config.model_dump(mode="json", exclude_defaults=True)
        try:
            config_module.reload_config(_without_nulls(data))
        except pydantic.ValidationError as error:
            raise InvalidRequest(f"Invalid config: {error}") from error
        return self.config_settings()

    def host_settings(self) -> models.HostSettings:
        host_file = config_module.host_file_of(config_module.LOADED_CONFIG_DIR)
        settings = host()
        return models.HostSettings(
            file=str(host_file),
            file_exists=host_file.is_file(),
            data_dir=str(settings.data_dir),
            raw_dir=str(settings.raw_dir),
            backups_dir=str(settings.backups_dir),
            host=settings.host,
            port=settings.port,
        )

    # Raw files

    def upload_raw(
        self,
        account: str,
        uploads: list[raw_upload.Upload],
        *,
        dry_run: bool = True,
        replace: bool = False,
    ) -> models.RawUpload:
        """Check raw files for an account's folder, or write them there (see
        `raw_upload`)."""
        try:
            # (an ingest sees all of an upload's files, or none)
            with _ingested_lock:
                return raw_upload.upload(
                    account, uploads, dry_run=dry_run, replace=replace
                )
        except raw_upload.UploadConflict as error:
            raise Conflict(f"Upload refused: {error}") from error
        except (raw_upload.UploadError, OSError) as error:
            raise InvalidRequest(f"Upload failed: {error}") from error

    # Export and import

    def export_to_download(self) -> Path:
        """The whole instance in a temporary zip, which the caller deletes (see
        `archive`)."""
        with _archive_errors("Export failed"):
            return archive.export_to_download(self.store, ingest_lock=_ingested_lock)

    def import_instance(
        self,
        zip_file: BinaryIO,
        *,
        dry_run: bool = True,
        backup: bool = True,
        backup_name: Optional[str] = None,
    ) -> models.ImportResult:
        """Check an export, then (unless a dry run) back up the current state (if
        `backup`) and replace it with the export's (see `archive`)."""
        return self._replace_instance(
            zip_file,
            "import",
            dry_run=dry_run,
            backup=backup,
            backup_name=backup_name,
        )

    def restore_backup(
        self,
        file_name: str,
        *,
        dry_run: bool = True,
        backup: bool = True,
        backup_name: Optional[str] = None,
    ) -> models.ImportResult:
        """Import a backup (see `import_instance`)."""
        with self.backup_file(file_name).open("rb") as zip_file:
            return self._replace_instance(
                zip_file,
                "restore",
                dry_run=dry_run,
                backup=backup,
                backup_name=backup_name,
            )

    def _replace_instance(
        self,
        zip_file: BinaryIO,
        action: Literal["import", "restore"],
        *,
        dry_run: bool,
        backup: bool,
        backup_name: Optional[str],
    ) -> models.ImportResult:
        with _ingested_lock, _archive_errors(f"{action.capitalize()} failed"):
            # The backup asked for must be possible, before anything else
            if backup:
                date_format = self.display_settings().date_format
                if not (backup_name or "").strip():
                    backup_name = backups.default_name(action, date_format)
                name = backups.check_new(backup_name)
            if dry_run:
                return archive.check_import(zip_file)
            back_up = None
            if backup:
                back_up = functools.partial(
                    backups.create, self.store, name, ingest_lock=_ingested_lock
                )
            try:
                return archive.import_(self.store, zip_file, back_up=back_up)
            finally:
                data_changed()

    # Backups

    def backups(self) -> models.Backups:
        return backups.listing(self.display_settings().date_format)

    def create_backup(self, name: str) -> models.Backup:
        with _archive_errors("Backup failed"):
            return backups.create(self.store, name, ingest_lock=_ingested_lock)

    def backup_file(self, file_name: str) -> Path:
        try:
            return backups.file_of(file_name)
        except backups.NoSuchBackup as error:
            raise NotFound(str(error)) from error

    def delete_backup(self, file_name: str):
        self.backup_file(file_name).unlink()


@contextlib.contextmanager
def _archive_errors(what: str) -> Iterator[None]:
    try:
        yield
    except backups.Refused as error:
        raise Conflict(f"{what}: {error}") from error
    except (
        archive.ArchiveError,
        backups.InvalidName,
        zipfile.BadZipFile,
        OSError,
        sqlite3.Error,
    ) as error:
        raise InvalidRequest(f"{what}: {error}") from error


def _without_nulls(data):
    """TOML has no null: unset values are left out."""
    if isinstance(data, dict):
        return {
            key: _without_nulls(value)
            for key, value in data.items()
            if value is not None
        }
    if isinstance(data, list):
        return [_without_nulls(value) for value in data]
    return data


def _sort_key(value):
    """Nulls last."""
    return (value is None, value)


def _sort_value(record: Record, field: models.SortField):
    if field == "description":
        return _shown_description(record).casefold()
    return _sort_key(record.row[field])


def _shown_description(record: Record) -> str:
    """What a transaction is, in one text, as the web app shows it: its written
    description, else its parts' (not those of an annotation a rule
    overrides), else the raw description."""
    annotation = record.annotation
    if annotation is not None:
        if annotation.description:
            return annotation.description
        if record.status != "rule":
            parts = [part.description for part in annotation.parts if part.description]
            if parts:
                return " · ".join(parts)
    return record.row["raw_description"]


def _matches(view: View, record: Record, query: models.TransactionQuery) -> bool:
    row = record.row
    if query.ids is not None and record.id not in query.ids:
        return False
    if query.start_date is not None and row["date"] < query.start_date:
        return False
    if query.end_date is not None and row["date"] >= query.end_date:
        return False
    if query.accounts is not None and row["account"] not in query.accounts:
        return False
    if query.sources is not None and row["source"] not in query.sources:
        return False
    if query.min_amount is not None and row["amount"] < query.min_amount:
        return False
    if query.max_amount is not None and row["amount"] > query.max_amount:
        return False
    if query.kind_set is not None and record.kind_set != query.kind_set:
        return False
    if query.kinds is not None and not any(
        kind in query.kinds for kind in view.record_kinds(record)
    ):
        return False
    if query.tags is not None or query.subtags is not None:
        if not any(
            (query.tags is None or tag in query.tags)
            and (query.subtags is None or subtag in query.subtags)
            for tag, subtag in record.labels
        ):
            return False
    if query.status is not None:
        if query.status == "tagged":
            if record.status == "untagged":
                return False
        elif record.status != query.status:
            return False
    annotation = record.annotation
    if query.needs_review is not None:
        if (annotation is not None and annotation.needs_review) != query.needs_review:
            return False
    if query.authors is not None:
        if annotation is None or not any(
            author in query.authors for author in annotation.authors
        ):
            return False
    if query.search is not None:
        needle = query.search.casefold()
        texts = [row["raw_description"]]
        if record.annotation is not None:
            texts.append(record.annotation.description or "")
            texts.extend(part.description or "" for part in record.annotation.parts)
        if not any(needle in text.casefold() for text in texts):
            return False
    return True


def _row_filters_expr(query: models.RowFilters) -> pl.Expr:
    """Polars equivalent of the `RowFilters` part of `_matches`, on rows."""
    predicates = [pl.lit(True)]
    if query.start_date is not None:
        predicates.append(pl.col("date") >= query.start_date)
    if query.end_date is not None:
        predicates.append(pl.col("date") < query.end_date)
    for column, values in (
        ("account", query.accounts),
        ("source", query.sources),
        ("tag", query.tags),
        ("subtag", query.subtags),
    ):
        if values is not None:
            predicates.append(pl.col(column).is_in(pl.Series(values, dtype=pl.String)))
    if query.min_amount is not None:
        predicates.append(pl.col("amount") >= query.min_amount)
    if query.max_amount is not None:
        predicates.append(pl.col("amount") <= query.max_amount)
    if query.kind_set is not None:
        predicates.append(pl.col("kind_set") == query.kind_set)
    if query.search is not None:
        needle = query.search.casefold()
        predicates.append(
            pl.any_horizontal(
                pl.col(column)
                .str.to_lowercase()
                .str.contains(needle, literal=True)
                .fill_null(False)
                for column in (
                    "raw_description",
                    "description",
                    "transaction_description",
                )
            )
        )
    return pl.all_horizontal(predicates).fill_null(False)


def _check_rule_filters(filters: Filters):
    try:
        validate_rule_filters(filters)
    except (StoreError, ValueError) as error:
        raise InvalidRequest(str(error)) from error


def _valid_color(color: Optional[str]) -> Optional[str]:
    if color is None:
        return None
    try:
        return Color(color).as_hex()
    except Exception as error:
        raise InvalidRequest(f"Invalid color: {color!r}") from error


def _rule_ref(view: View, record: Record) -> Optional[models.RuleRef]:
    if record.rule is None:
        return None
    return models.RuleRef(
        id=record.rule.id,
        position=record.rule_position,
        tag=record.rule.tag,
        subtag=record.rule.subtag,
        kind=view.kind(record.rule.tag, record.rule.subtag),
    )


def _parts_out(view: View, annotation: Annotation) -> list[models.PartOut]:
    return [
        models.PartOut(
            amount=from_minor_units(part.amount),
            tag=part.tag,
            subtag=part.subtag,
            kind=view.part_kind(part),
            kind_set=part.kind is not None,
            description=part.description,
        )
        for part in annotation.parts
    ]


def _transaction_out(view: View, record: Record) -> models.Transaction:
    annotation = None
    if record.annotation is not None:
        annotation = models.AnnotationOut(
            description=record.annotation.description,
            parts=_parts_out(view, record.annotation),
            authors=list(record.annotation.authors),
            needs_review=record.annotation.needs_review,
        )
    guess = view.guess(record) if record.status == "untagged" else None
    group, group_role = view.group(record.row["account"])
    return models.Transaction(
        **{column: record.row[column] for column in RAW_SCHEMA},
        group=group,
        group_role=group_role,
        status=record.status,
        annotation=annotation,
        rule=_rule_ref(view, record),
        guess=guess,
    )


def _account_counts(data: pl.DataFrame) -> dict[str, int]:
    counts = data.group_by("account").len()
    return dict(zip(counts["account"], counts["len"], strict=True))


def _ingest_row(row: dict) -> models.IngestRow:
    return models.IngestRow(
        **{field: row[field] for field in models.IngestRow.model_fields}
    )


def _stored_annotation_out(
    view: View, annotation: Annotation, rule: Optional[models.RuleRef] = None
) -> models.StoredAnnotation:
    snapshot = annotation.snapshot
    return models.StoredAnnotation(
        transaction_id=annotation.transaction_id,
        snapshot=models.SnapshotOut(
            account=snapshot.account,
            source=snapshot.source,
            date=snapshot.date,
            amount=from_minor_units(snapshot.amount),
            raw_description=snapshot.raw_description,
        ),
        description=annotation.description,
        parts=_parts_out(view, annotation),
        authors=list(annotation.authors),
        needs_review=annotation.needs_review,
        rule=rule,
    )


def _bulk_annotation(
    record: Record, request: models.BulkAnnotateRequest
) -> Optional[Annotation]:
    """A transaction's annotation with a bulk request's fields changed, or None
    if a field can't apply: a kind or the review flag without tags."""
    existing = record.annotation
    parts = existing.parts if existing is not None else ()
    if request.tag is not None:
        # The new part keeps a kind set on all the old parts (unless one is given)
        kept_kinds = (
            {part.kind for part in parts} if record.annotation_applies else set()
        )
        kept_kind = kept_kinds.pop() if len(kept_kinds) == 1 else None
        amount = snapshot_of(record.row).amount
        parts = (Part(amount, request.tag, request.subtag, kind=kept_kind),)
    elif request.kind is not None and not record.annotation_applies:
        return None
    if request.kind is not None:
        kind = None if request.kind == models.INHERIT else request.kind
        parts = tuple(dataclasses.replace(part, kind=kind) for part in parts)
    description = existing.description if existing is not None else None
    if request.description is not None:
        description = request.description or None
    if request.needs_review is not None and not (parts or description):
        return None
    return _written_by(
        existing,
        Annotation(
            record.id,
            snapshot_of(record.row),
            parts,
            description,
            authors=_authors(existing),
            needs_review=existing.needs_review
            if request.needs_review is None and existing is not None
            else bool(request.needs_review),
        ),
        request.author,
    )


def _authors(existing: Optional[Annotation]) -> tuple[str, ...]:
    """The authors a rewritten annotation keeps: none for a new one."""
    return () if existing is None else existing.authors


def _written_by(
    existing: Optional[Annotation], annotation: Annotation, author: str
) -> Annotation:
    """The annotation replacing an existing one, with the author added to its
    authors if it changes the content: its parts (tags, kinds) or description.
    Changing only the review flag doesn't make one an author."""
    if existing is not None and (existing.parts, existing.description) == (
        annotation.parts,
        annotation.description,
    ):
        return annotation
    return annotation.contributed_by(author)


def _same_content(existing: Optional[Annotation], annotation: Annotation) -> bool:
    """Whether writing the annotation would change nothing but its authors."""
    if existing is None:
        return not (
            annotation.parts or annotation.description or annotation.needs_review
        )
    return (existing.parts, existing.description, existing.needs_review) == (
        annotation.parts,
        annotation.description,
        annotation.needs_review,
    )
