"""The local HTTP API: a thin adapter over `finalyze.service`.

Served by `finalyze`. Each request gets its own store connection.
"""

import contextlib
import datetime
import threading
import urllib.parse
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, FastAPI, File, Form, Query, Request, UploadFile
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware

from finalyze import APP_DESCRIPTION, APP_NAME, agent_md, glossary
from finalyze.config import Config
from finalyze.service import (
    BatchError,
    Conflict,
    InvalidRequest,
    KeymapError,
    NotFound,
    Service,
    ServiceError,
    archive,
    models,
    open_service,
    raw_upload,
)
from finalyze.store import StoreError

DEFAULT_STATIC_DIR = Path(__file__).parent.joinpath("static")
"""Where `scripts/build-web.sh` builds the web app (gitignored)."""
WEB_DIR = Path(__file__).parent.parent.joinpath("web")
"""The web app's sources, in a checkout of the repository."""
WEB_BUILD_INPUTS = (
    "src",
    "index.html",
    "package-lock.json",
    "vite.config.ts",
    "svelte.config.js",
    "tsconfig.json",
)
MISSING_WEB_APP = (
    "The web app is not built. Build it with scripts/build-web.sh, "
    "or run its dev server (see the README)."
)
LOCAL_HOSTS = ("127.0.0.1", "localhost", "::1")
"""Accepted Host headers by default, which blocks DNS rebinding: without them, any
website could read the API through the browser, since it has no authentication."""
API_DESCRIPTION = f"""
{APP_DESCRIPTION}: a local, single-user API over transactions and their
annotations. Instructions for agents, with examples, are at [/agent.md](/agent.md).

**Concepts**
- **Transactions** come from the last ingest of the raw bank exports (`POST
  /api/ingest` re-reads them). Each has a stable `id`, and a `raw_description`:
  its text in the raw files, often vague.
- A transaction is tagged by the first matching **rule** (status `rule`), or
  else by its **annotation** (status `annotated`), or it is `untagged`. Rules
  override annotations: such an annotation is *overridden*.
- An **annotation** is a transaction's `description` (what it really was,
  written by the owner or an agent) plus **parts**, each with a tag, subtag,
  amount and description. Several parts are a split, and must sum exactly to
  the transaction amount. A single part defaults to the full amount. Without
  parts, an annotation is only a description, and the transaction stays
  untagged.
- Annotations record their `authors`: everyone who wrote them, by first
  contribution. Every write that changes their content (description, tags,
  parts or kinds) adds its `author` (default `owner`, agents give their name).
  Annotations also have a `needs_review` flag. Changing only the flag adds no
  author. Clearing it is an explicit write (`POST /api/annotations/review`).
- **Patch** annotations (`PATCH`) to change only some fields, keeping the others
  (e.g. a description, without touching the tags or the review flag). `PUT` and
  `POST /api/annotations/batch` replace annotations whole.
- **Guesses** are suggestions on untagged transactions: the tags of the
  annotated transaction with the same raw description nearest in date. They are never
  applied automatically: only explicit writes change anything.
- Writes refuse unknown tags unless `allow_new_tags` is set, so typos and
  invented tags don't slip in.
- Bulk annotation previews by default (`dry_run`). Commit with `dry_run: false`,
  and optionally `expected_ids` (e.g. a preview's `affected_ids`), which makes it
  fail if the affected transactions differ.
- **Orphans** are annotations whose transaction no longer exists. They are only
  deleted explicitly.
- Every account is in one **group**, whose role is `spending` (day-to-day
  money: cash flows, breakdowns and the available balance) or `holding`
  (savings, investments or debt: only the net worth).
- Every tag has a **kind** (a subtag may have its own, or inherit its tag's):
  `regular` incomes and expenses, `transfer` (between own accounts, or
  bookkeeping: never incomes or expenses), `saving` (money set aside: saved, not
  spent), or `one-off` (real, but left out of averages and trends unless
  included). A kind can also be **set on a transaction** (an annotation's part),
  over its tag's, e.g. one big purchase marked `one-off`.
- Amounts are in currency units, with at most 2 decimal places when written.

**Errors** have a `detail` message: 404 not found, 409 conflicts with the
current state, 422 invalid requests (batches also list per-item `errors`, and
write nothing), 400 other invalid operations.
"""
ERROR_STATUS = ((NotFound, 404), (Conflict, 409), (InvalidRequest, 422))


class RequestGate:
    """Requests pass it together, but an import replaces the data alone: it waits
    for the requests under way, and new ones wait for it. So no write goes to
    the replaced store (lost), and no request sees half an import."""

    def __init__(self):
        self._condition = threading.Condition()
        self._requests = 0
        self._exclusive = False

    @contextlib.contextmanager
    def shared(self) -> Iterator[None]:
        with self._condition:
            self._condition.wait_for(lambda: not self._exclusive)
            self._requests += 1
        try:
            yield
        finally:
            with self._condition:
                self._requests -= 1
                self._condition.notify_all()

    @contextlib.contextmanager
    def exclusive(self) -> Iterator[None]:
        with self._condition:
            self._condition.wait_for(lambda: not self._exclusive)
            self._exclusive = True
            self._condition.wait_for(lambda: self._requests == 0)
        try:
            yield
        finally:
            with self._condition:
                self._exclusive = False
                self._condition.notify_all()


REQUEST_GATE = RequestGate()

UPLOAD_LIMITS = (
    ("/api/import", archive.MAX_UPLOAD_BYTES),
    ("/api/raw/", raw_upload.MAX_UPLOAD_BYTES),
)
"""The largest request bodies of the uploads, by path prefix."""


class _TooLarge(Exception):
    pass


class UploadLimit:
    """Refuse request bodies over their path's limit (`UPLOAD_LIMITS`) with 413:
    at once by their declared length, or once they have sent too much."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        limit = next(
            (
                limit
                for prefix, limit in UPLOAD_LIMITS
                if scope["type"] == "http" and scope["path"].startswith(prefix)
            ),
            None,
        )
        if limit is None:
            await self.app(scope, receive, send)
            return
        declared = dict(scope["headers"]).get(b"content-length")
        if declared is not None and declared.isdigit() and int(declared) > limit:
            await _too_large(limit)(scope, receive, send)
            return
        received = 0
        too_large = False

        async def limited_receive():
            nonlocal received, too_large
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > limit:
                    too_large = True
                    raise _TooLarge()
            return message

        response_started = False

        async def tracked_send(message):
            nonlocal response_started
            # The app's own error about the body it couldn't read, replaced
            if too_large:
                return
            response_started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, tracked_send)
        except _TooLarge:
            pass
        if too_large and not response_started:
            await _too_large(limit)(scope, receive, send)


SAFE_METHODS = ("GET", "HEAD", "OPTIONS")


class SameOriginWrites:
    """Refuse writes (any method but GET, HEAD and OPTIONS) that a browser sends
    from another site (403): without authentication, a page anywhere could
    otherwise post forms to the API (e.g. an upload or an import), which
    browsers send cross-site without asking. By `Sec-Fetch-Site`, and `Origin`,
    whose host must be an allowed one. Clients other than browsers (e.g. curl)
    send neither."""

    def __init__(self, app, allowed_hosts: tuple[str, ...]):
        self.app = app
        self.allowed_hosts = allowed_hosts

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["method"] not in SAFE_METHODS:
            headers = dict(scope["headers"])
            fetch_site = headers.get(b"sec-fetch-site", b"").decode("latin-1")
            origin = headers.get(b"origin")
            if fetch_site == "cross-site" or (
                origin is not None and not self._allowed(origin.decode("latin-1"))
            ):
                response = JSONResponse(
                    status_code=403,
                    content={"detail": "Writes from other sites are refused"},
                )
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)

    def _allowed(self, origin: str) -> bool:
        hostname = urllib.parse.urlsplit(origin).hostname
        return hostname is not None and hostname in self.allowed_hosts


def _too_large(limit: int) -> JSONResponse:
    return JSONResponse(
        status_code=413,
        content={"detail": f"Too large: at most {limit} bytes per request"},
    )


def get_service(request: Request) -> Iterator[Service]:
    with REQUEST_GATE.shared(), open_service() as service:
        yield service


ServiceDependency = Annotated[Service, Depends(get_service)]


def create_app(
    *,
    static_dir: Optional[Path] = DEFAULT_STATIC_DIR,
    allowed_hosts: tuple[str, ...] = LOCAL_HOSTS,
    demo: bool = False,
) -> FastAPI:
    """The API under /api, and a static frontend at / if `static_dir` exists.

    `demo` only tells the web app that the data is a demo's."""
    app = FastAPI(title=APP_NAME, description=API_DESCRIPTION)
    app.state.demo = demo
    app.state.static_dir = static_dir
    app.add_middleware(UploadLimit)
    app.add_middleware(SameOriginWrites, allowed_hosts=allowed_hosts)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=list(allowed_hosts))
    app.add_exception_handler(ServiceError, _service_error_handler)
    app.add_exception_handler(StoreError, _store_error_handler)
    app.include_router(router)
    app.add_api_route("/agent.md", _agent_md, include_in_schema=False)
    if static_dir is not None and static_dir.is_dir():
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="web")
    elif static_dir is not None:
        app.add_api_route("/", _missing_web_app, include_in_schema=False)
    return app


def stale_web_app(static_dir: Optional[Path], web_dir: Path = WEB_DIR) -> Optional[str]:
    """Why the built web app is older than its sources, if it is. Only checked in
    a checkout of the repository (with the web sources)."""
    if static_dir is None or not web_dir.is_dir():
        return None
    built = static_dir.joinpath("index.html")
    if not built.is_file():
        return None
    inputs = []
    for name in WEB_BUILD_INPUTS:
        input_path = web_dir.joinpath(name)
        if input_path.is_dir():
            inputs.extend(
                file
                for file in input_path.rglob("*")
                if file.is_file() and not file.name.endswith(".test.ts")
            )
        elif input_path.is_file():
            inputs.append(input_path)
    if not inputs:
        return None
    newest = max(inputs, key=lambda file: file.stat().st_mtime)
    built_time = built.stat().st_mtime
    if newest.stat().st_mtime <= built_time:
        return None

    def when(timestamp: float) -> str:
        return datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")

    return (
        f"The web app (built {when(built_time)}) is older than its sources "
        f"({newest.relative_to(web_dir)} changed {when(newest.stat().st_mtime)}). "
        "Rebuild it with scripts/build-web.sh."
    )


def _missing_web_app() -> HTMLResponse:
    return HTMLResponse(
        f"<p>{MISSING_WEB_APP} The API docs are at <a href='/docs'>/docs</a>.</p>",
        status_code=404,
    )


def _agent_md(request: Request) -> Response:
    """Instructions for agents, with this instance's base URL."""
    instructions = agent_md.render(str(request.base_url).rstrip("/"))
    return Response(instructions, media_type="text/markdown")


def _service_error_handler(request: Request, error: ServiceError) -> JSONResponse:
    status = next((code for kind, code in ERROR_STATUS if isinstance(error, kind)), 400)
    content = {"detail": str(error)}
    if isinstance(error, BatchError):
        content["errors"] = [item.model_dump() for item in error.errors]
    if isinstance(error, KeymapError):
        content["problems"] = error.problems
    return JSONResponse(status_code=status, content=content)


def _store_error_handler(request: Request, error: StoreError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(error)})


router = APIRouter(prefix="/api")


# Status and ingest


@router.get("/status", tags=["status"])
def get_status(service: ServiceDependency) -> models.Status:
    """Data freshness, counts of transactions by status, orphans, and paths."""
    return service.status()


@router.get("/server", tags=["status"])
def get_server_info(request: Request) -> models.ServerInfo:
    """How this server runs: over demo data or not, and whether its web app is
    stale."""
    return models.ServerInfo(
        demo=request.app.state.demo,
        stale_web_app=stale_web_app(request.app.state.static_dir),
    )


@router.get("/glossary", tags=["status"])
def get_glossary() -> list[models.GlossaryEntry]:
    """What every figure, label and concept of the app means, in order."""
    return glossary.entries()


@router.post("/ingest", tags=["status"])
def ingest(service: ServiceDependency) -> models.IngestSummary:
    """Re-read the raw files of every account into the ingested data.

    Returns counts, the transaction IDs that appeared or vanished since the
    previous ingest, and new orphans. Nothing is replaced if parsing fails.
    """
    return service.ingest()


@router.get("/ingest/dry-run", tags=["status"])
def ingest_dry_run(service: ServiceDependency) -> models.IngestDryRun:
    """Parse each raw file on its own, without writing anything.

    Per account: its configured paths and files, and per file the parser that
    read it (or each parser's error), its rows, dates and sum. Flags likely
    problems: missing paths, unparseable or empty files, and files of the same
    account sharing rows of a source in overlapping dates (likely duplicates).
    """
    return service.ingest_dry_run()


@router.get("/ingest/preview", tags=["status"])
def preview_ingest(service: ServiceDependency) -> models.IngestPreview:
    """What `POST /api/ingest` would change, without writing anything.

    The transactions that would appear or vanish, the annotations that would be
    orphaned, and orphans whose transaction would be back. 422 if the ingest
    would fail.
    """
    return service.preview_ingest()


# Transactions


@router.get("/transactions", tags=["transactions"])
def list_transactions(
    service: ServiceDependency,
    request: Annotated[models.TransactionListRequest, Query()],
) -> models.TransactionPage:
    """Query transactions, sorted and paginated.

    Each item has its annotation, matching rule and, if untagged, a guess.
    List filters are repeated parameters, e.g. `?accounts=a&accounts=b`.
    """
    return service.list_transactions(request)


@router.post("/transactions/query", tags=["transactions"])
def query_transactions(
    service: ServiceDependency, request: models.TransactionListRequest
) -> models.TransactionPage:
    """Like `GET /api/transactions`, with the query as a JSON body."""
    return service.list_transactions(request)


@router.get("/transactions/{transaction_id}", tags=["transactions"])
def get_transaction(
    service: ServiceDependency, transaction_id: str
) -> models.Transaction:
    return service.get_transaction(transaction_id)


# Annotations


@router.put("/transactions/{transaction_id}/annotation", tags=["annotations"])
def set_annotation(
    service: ServiceDependency,
    transaction_id: str,
    request: models.AnnotationSetRequest,
) -> models.Transaction:
    """Set (or replace) the annotation of a transaction, whole: every field left
    out is reset (e.g. the review flag is cleared). To change only some fields,
    patch it instead.

    E.g. `{"parts": [{"tag": "food", "subtag": "groceries"}]}`, or a split with
    an amount on every part.
    """
    return service.set_annotation(transaction_id, request)


@router.patch("/transactions/{transaction_id}/annotation", tags=["annotations"])
def patch_annotation(
    service: ServiceDependency,
    transaction_id: str,
    request: models.AnnotationPatchRequest,
) -> models.Transaction:
    """Change only the given fields of a transaction's annotation (creating it if
    needed): the others are kept as they are. Prefer it to replacing the
    annotation, which resets every field left out.

    E.g. `{"description": "birthday gift"}` keeps the tags and review flag.
    """
    return service.patch_annotation(transaction_id, request)


@router.delete("/transactions/{transaction_id}/annotation", tags=["annotations"])
def clear_annotation(
    service: ServiceDependency, transaction_id: str
) -> models.Transaction:
    """Delete the annotation of a transaction, if any."""
    return service.clear_annotation(transaction_id)


@router.post("/annotations/batch", tags=["annotations"])
def set_annotations(
    service: ServiceDependency, request: models.BatchRequest
) -> models.BatchResult:
    """Set many annotations atomically: all are written, or none.

    If any item is invalid, the response (422) lists every invalid item.
    """
    return service.set_annotations(request)


@router.patch("/annotations/batch", tags=["annotations"])
def patch_annotations(
    service: ServiceDependency, request: models.PatchBatchRequest
) -> models.BatchResult:
    """Patch many annotations atomically: only each item's given fields change,
    and all items are written, or none.

    If any item is invalid, the response (422) lists every invalid item.
    """
    return service.patch_annotations(request)


@router.post("/annotations/review", tags=["annotations"])
def review_annotations(
    service: ServiceDependency, request: models.ReviewRequest
) -> models.ReviewResult:
    """Mark annotations as reviewed (the default), or flag them for review."""
    return service.review(request)


@router.post("/annotations/kind", tags=["annotations"])
def set_kind(
    service: ServiceDependency, request: models.KindRequest
) -> models.KindResult:
    """Set a kind on transactions, over their tags' kinds (e.g. one big purchase
    marked `one-off`), or clear it (`"kind": null`). All or none.

    Every part of each transaction, or only `part` (by index). The transactions
    must be tagged by their annotations: a rule's transactions can't have one.
    """
    return service.set_kind(request)


@router.post("/annotations/clear", tags=["annotations"])
def clear_annotations(
    service: ServiceDependency, request: models.ClearRequest
) -> models.ClearResult:
    """Delete the annotations of transactions (including orphans)."""
    return service.clear_annotations(request)


@router.post("/annotations/bulk", tags=["annotations"])
def bulk_annotate(
    service: ServiceDependency, request: models.BulkAnnotateRequest
) -> models.BulkAnnotateResult:
    """Change the tags, kind, description or review flag of every transaction matching
    a query: each field is optional, and one left out is kept as it is.

    Previews by default (`dry_run`). Transactions tagged by rules are skipped,
    and so are transactions without tags when a field needs them (a kind
    without `tag`, the review flag without tags or a description). A `tag` replaces
    their parts (including splits), a `kind` is set on every part.
    """
    return service.bulk_annotate(request)


# Tags


@router.get("/tags", tags=["tags"])
def list_tags(service: ServiceDependency) -> list[models.TagOut]:
    """Tags with their subtags, properties and usage counts."""
    return service.list_tags()


@router.post("/tags", tags=["tags"], status_code=201)
def create_tag(service: ServiceDependency, request: models.TagCreate) -> models.TagOut:
    """Create a tag, or a subtag (and its tag if missing)."""
    return service.create_tag(request)


@router.patch("/tags", tags=["tags"])
def update_tag_properties(
    service: ServiceDependency, request: models.TagPropertiesUpdate
) -> models.TagOut:
    """Change the color or kind of a tag or subtag.

    Tags are referenced in the body because subtags may be empty strings.
    """
    return service.update_tag_properties(request)


@router.post("/tags/rename", tags=["tags"])
def rename_tag(service: ServiceDependency, request: models.TagRename) -> models.TagOut:
    """Rename a tag or subtag. Refused if the new name exists (merge instead)."""
    return service.rename_tag(request)


@router.post("/tags/merge", tags=["tags"])
def merge_tag(service: ServiceDependency, request: models.TagMerge) -> models.TagOut:
    """Repoint annotations and rules of a tag (or subtag) to another, then delete
    it. The merged tag's properties are dropped."""
    return service.merge_tag(request)


@router.post("/tags/delete", tags=["tags"], status_code=204)
def delete_tag(service: ServiceDependency, request: models.TagRef):
    """Delete a tag (with its subtags) or a subtag, only if no annotation or rule
    uses it."""
    service.delete_tag(request)


# Accounts and groups


@router.get("/accounts", tags=["accounts"])
def list_accounts(service: ServiceDependency) -> list[models.AccountOut]:
    return service.list_accounts()


@router.patch("/accounts/{name}", tags=["accounts"])
def update_account(
    service: ServiceDependency, name: str, request: models.AccountUpdate
) -> models.AccountOut:
    """Change an account's group or color."""
    return service.update_account(name, request)


@router.get("/balance", tags=["accounts"])
def get_balance(
    service: ServiceDependency, query: Annotated[models.BalanceQuery, Query()]
) -> models.BalanceOnDate:
    """A balance at the end of a day: of an account, one of its sources, a group,
    or the net worth."""
    return service.balance(query)


@router.get("/groups", tags=["accounts"])
def list_groups(service: ServiceDependency) -> list[models.GroupOut]:
    """Account groups in their display order, with their accounts."""
    return service.list_groups()


@router.post("/groups", tags=["accounts"], status_code=201)
def create_group(
    service: ServiceDependency, request: models.GroupCreate
) -> models.GroupOut:
    return service.create_group(request)


@router.patch("/groups/{name}", tags=["accounts"])
def update_group(
    service: ServiceDependency, name: str, request: models.GroupUpdate
) -> models.GroupOut:
    """Rename a group, or change its role, color or position."""
    return service.update_group(name, request)


@router.delete("/groups/{name}", tags=["accounts"], status_code=204)
def delete_group(service: ServiceDependency, name: str):
    """Delete a group without accounts (409 otherwise)."""
    service.delete_group(name)


# Rules


@router.get("/rules", tags=["rules"])
def list_rules(service: ServiceDependency) -> list[models.RuleOut]:
    """Rules in order: the first matching rule tags a transaction."""
    return service.list_rules()


@router.post("/rules", tags=["rules"], status_code=201)
def create_rule(
    service: ServiceDependency, request: models.RuleCreate
) -> models.RuleOut:
    """Add a rule. Preview it first with `POST /api/rules/preview`."""
    return service.create_rule(request)


@router.post("/rules/preview", tags=["rules"])
def preview_rule(
    service: ServiceDependency, request: models.RulePreviewRequest
) -> models.RulePreview:
    """What a new or changed rule would match, tag, and override."""
    return service.preview_rule(request)


@router.post("/rules/reorder", tags=["rules"])
def reorder_rules(
    service: ServiceDependency, request: models.RuleReorder
) -> list[models.RuleOut]:
    return service.reorder_rules(request)


@router.get("/rules/{rule_id}", tags=["rules"])
def get_rule(service: ServiceDependency, rule_id: int) -> models.RuleOut:
    return service.get_rule(rule_id)


@router.patch("/rules/{rule_id}", tags=["rules"])
def update_rule(
    service: ServiceDependency, rule_id: int, request: models.RuleUpdate
) -> models.RuleOut:
    return service.update_rule(rule_id, request)


@router.delete("/rules/{rule_id}", tags=["rules"], status_code=204)
def delete_rule(service: ServiceDependency, rule_id: int):
    service.delete_rule(rule_id)


# Orphans and overridden annotations


@router.get("/orphans", tags=["orphans"])
def list_orphans(service: ServiceDependency) -> list[models.StoredAnnotation]:
    """Annotations whose transaction no longer exists in the ingested data."""
    return service.list_orphans()


@router.post("/orphans/delete", tags=["orphans"])
def delete_orphans(
    service: ServiceDependency, request: models.OrphanDelete
) -> models.OrphanDeleteResult:
    """Delete orphaned annotations by transaction ID (only orphans)."""
    return service.delete_orphans(request)


@router.get("/overridden", tags=["orphans"])
def list_overridden(service: ServiceDependency) -> list[models.StoredAnnotation]:
    """Annotations overridden by rules, with the overriding rule."""
    return service.list_overridden()


# Analysis


@router.get("/analysis/rows", tags=["analysis"])
def analysis_rows(
    service: ServiceDependency, query: Annotated[models.AnalysisQuery, Query()]
) -> models.AnalysisRows:
    """Enriched rows, as used by the reports: one row per transaction part."""
    return service.analysis_rows(query)


@router.get("/analysis/aggregate", tags=["analysis"])
def aggregate(
    service: ServiceDependency, query: Annotated[models.AggregateQuery, Query()]
) -> models.Aggregate:
    """Sum and count of analysis rows by groups (default: by month)."""
    return service.aggregate(query)


@router.post("/dashboard", tags=["dashboard"])
def dashboard(
    service: ServiceDependency, options: models.DashboardOptions
) -> models.Dashboard:
    """The dashboard: headline numbers, balances, cash flow, breakdowns and
    trends, over a period of months. Unset options take their defaults (see
    `GET /api/settings/dashboard`)."""
    return service.dashboard(options)


@router.post("/dashboard/rows", tags=["dashboard"])
def dashboard_rows(
    service: ServiceDependency, request: models.DashboardRowsRequest
) -> models.AnalysisRows:
    """The analysis rows behind a number of the dashboard (drill-down), e.g. a
    tag's expenses in a month, with the same options."""
    return service.dashboard_rows(request)


# Settings


@router.get("/settings/dashboard", tags=["settings"])
def get_dashboard_defaults(service: ServiceDependency) -> models.DashboardDefaults:
    """Defaults of the dashboard's options (kept in the store)."""
    return service.dashboard_defaults()


@router.put("/settings/dashboard", tags=["settings"])
def set_dashboard_defaults(
    service: ServiceDependency, defaults: models.DashboardDefaults
) -> models.DashboardDefaults:
    return service.set_dashboard_defaults(defaults)


@router.get("/settings/display", tags=["settings"])
def get_display_settings(service: ServiceDependency) -> models.DisplaySettings:
    """How the web app shows things (kept in the store), e.g. its date format."""
    return service.display_settings()


@router.put("/settings/display", tags=["settings"])
def set_display_settings(
    service: ServiceDependency, settings: models.DisplaySettings
) -> models.DisplaySettings:
    return service.set_display_settings(settings)


@router.get("/settings/keymap", tags=["settings"])
def get_keymap(service: ServiceDependency) -> models.Keymap:
    """The web app's keyboard shortcuts (kept in the store): the directions'
    preset and the actions' keys over their defaults."""
    return service.keymap()


@router.put("/settings/keymap", tags=["settings"])
def set_keymap(service: ServiceDependency, keymap: models.Keymap) -> models.Keymap:
    """Replace the keyboard shortcuts. Keys that clash are refused (422, with
    the `problems`), as on the web app's Settings page."""
    return service.set_keymap(keymap)


@router.get("/settings/colors", tags=["settings"])
def get_label_colors(service: ServiceDependency) -> models.LabelColors:
    """Colors of chart labels other than accounts and tags (which have their own,
    see `PATCH /api/accounts/{name}` and `PATCH /api/tags`)."""
    return service.label_colors()


@router.put("/settings/colors", tags=["settings"])
def set_label_color(
    service: ServiceDependency, request: models.LabelColor
) -> models.LabelColors:
    """Set a label's color, or remove it (null)."""
    return service.set_label_color(request)


@router.get("/settings/config", tags=["settings"])
def get_config(service: ServiceDependency) -> models.ConfigSettings:
    """The config file's settings."""
    return service.config_settings()


@router.put("/settings/config", tags=["settings"])
def set_config(service: ServiceDependency, config: Config) -> models.ConfigSettings:
    """Replace the config file's settings, and use them from now on. The file is
    rewritten without its comments (the previous file is kept as
    `config.toml.bak`)."""
    return service.set_config(config)


@router.get("/settings/host", tags=["settings"])
def get_host_settings(service: ServiceDependency) -> models.HostSettings:
    """Where things are on this machine, from the host file (`host.toml`, next to
    the config): read-only, and never exported."""
    return service.host_settings()


# Raw files


@router.post("/raw/{account}", tags=["status"])
def upload_raw(
    service: ServiceDependency,
    account: str,
    files: Annotated[list[UploadFile], File(description="The raw files.")],
    dry_run: Annotated[bool, Form()] = True,
    replace: Annotated[bool, Form()] = False,
) -> models.RawUpload:
    """Check raw files for an account (the default, `dry_run`), or write them into
    its folder of the raw folder (made if new). Each file's dry run is as the
    ingest dry run's. Writing refuses files no parser reads (422), and files of
    the same names as existing ones unless `replace` (409)."""
    uploads = [raw_upload.Upload(file.filename or "", file.file) for file in files]
    return service.upload_raw(account, uploads, dry_run=dry_run, replace=replace)


# Export and import


@router.get(
    "/export",
    tags=["export"],
    response_class=StreamingResponse,
    responses={200: {"content": {"application/zip": {}}}},
)
def export(service: ServiceDependency) -> StreamingResponse:
    """Download the whole instance as one zip: the store, the ingested data, the
    raw folder and the config (never the host file)."""
    zip_path = service.export_to_download()
    # Deleted at once: the open file is sent, however the response ends
    zip_file = zip_path.open("rb")
    size = zip_path.stat().st_size
    zip_path.unlink()

    def chunks() -> Iterator[bytes]:
        with zip_file:
            while chunk := zip_file.read(1024**2):
                yield chunk

    return StreamingResponse(
        chunks(),
        media_type="application/zip",
        headers={
            "Content-Length": str(size),
            "Content-Disposition": f'attachment; filename="{archive.export_name()}"',
        },
    )


@router.post("/import", tags=["export"])
def import_instance(
    file: Annotated[UploadFile, File(description="An export's zip.")],
    dry_run: Annotated[bool, Form()] = True,
    backup: Annotated[bool, Form()] = True,
    backup_name: Annotated[Optional[str], Form()] = None,
) -> models.ImportResult:
    """Replace the whole instance with an uploaded export: the raw folder's
    contents become its raw files exactly (files it doesn't have are removed,
    see `removed_raw_files`), and its config, ingested data and store replace
    the current ones. The current state is backed up first (`backup`, named
    `backup_name`, by default `before import on <date>`).

    Checks only by default (`dry_run`). Nothing is written if the zip is invalid
    (422), or if the backup asked for can't be made (409 at the limit or for a
    name that is taken, 422 for an invalid name): delete a backup, or don't ask
    for one."""
    # Not through `get_service`: an import waits for every other request
    gate = REQUEST_GATE.shared() if dry_run else REQUEST_GATE.exclusive()
    with gate, open_service() as service:
        return service.import_instance(
            file.file, dry_run=dry_run, backup=backup, backup_name=backup_name
        )


# Backups


@router.get("/backups", tags=["export"])
def list_backups(service: ServiceDependency) -> models.Backups:
    """The backups kept on this machine (in the backups folder), and the default
    names of those made before an import or a restore."""
    return service.backups()


@router.post("/backups", tags=["export"])
def create_backup(
    service: ServiceDependency, request: models.BackupRequest
) -> models.Backup:
    """Back up the whole instance (the same as an export) into the backups
    folder. Refused (409) at the limit, or if a backup has the same name."""
    return service.create_backup(request.name)


@router.get(
    "/backups/{file}",
    tags=["export"],
    response_class=FileResponse,
    responses={200: {"content": {"application/zip": {}}}},
)
def download_backup(service: ServiceDependency, file: str) -> FileResponse:
    """Download a backup (an export's zip, which imports anywhere)."""
    return FileResponse(
        service.backup_file(file), media_type="application/zip", filename=file
    )


@router.delete("/backups/{file}", tags=["export"], status_code=204)
def delete_backup(service: ServiceDependency, file: str):
    """Delete a backup."""
    service.delete_backup(file)


@router.post("/backups/{file}/restore", tags=["export"])
def restore_backup(file: str, request: models.RestoreRequest) -> models.ImportResult:
    """Replace the whole instance with a backup's, as an import does (see `POST
    /api/import`): checks only by default (`dry_run`), and backs up the current
    state first unless `backup` is false."""
    gate = REQUEST_GATE.shared() if request.dry_run else REQUEST_GATE.exclusive()
    with gate, open_service() as service:
        return service.restore_backup(
            file,
            dry_run=request.dry_run,
            backup=request.backup,
            backup_name=request.backup_name,
        )
