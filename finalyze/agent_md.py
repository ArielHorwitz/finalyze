"""Instructions for agents, served as Markdown at `/agent.md`.

They describe how to operate this instance's HTTP API, with its real base URL,
and nothing about how to use it (that is up to whoever instructs the agent).
`OPERATIONS` documents every `/api` route, which the tests check.
"""

import dataclasses
import re
import textwrap

BASE_PLACEHOLDER = "{base}"


def _dedent(text: str) -> str:
    return textwrap.dedent(text).strip()


@dataclasses.dataclass(frozen=True)
class Operation:
    title: str
    method: str
    path: str
    """The route's path, e.g. /api/rules/{rule_id}."""
    text: str
    """Semantics, in Markdown, with {base} for the base URL."""
    examples: str
    """Shell commands, with {base} for the base URL and # comments."""

    @property
    def shown_path(self) -> str:
        """The path with placeholders as in the examples, e.g. RULE_ID."""
        return re.sub(r"\{(\w+)\}", lambda match: match[1].upper(), self.path)

    def render(self, base_url: str) -> str:
        return "\n\n".join(
            [
                f"### {self.title}",
                f"`{self.method} {base_url}{self.shown_path}`",
                _dedent(self.text),
                f"```bash\n{_dedent(self.examples)}\n```",
            ]
        )


@dataclasses.dataclass(frozen=True)
class OperationGroup:
    title: str
    text: str
    """What the group's operations share, in Markdown (may be empty)."""
    operations: tuple[Operation, ...]

    def render(self, base_url: str) -> str:
        sections = [f"## {self.title}", _dedent(self.text)]
        sections += [operation.render(base_url) for operation in self.operations]
        return "\n\n".join(section for section in sections if section)


INTRO = """
# finalyze API

API for finalyze, a personal finance tool: transactions from bank exports, the
tags they are annotated with, and analyses of them. This instance is served at
<{base}>, for a single local user, without authentication. Requests and
responses are JSON. The full schema, with every field, is at <{base}/docs>
(interactive) and <{base}/openapi.json>.
"""

CONCEPTS = """
## Concepts

### Transactions

A transaction has an `id`, an `account` (e.g. a bank account), a `source` (the
kind of file within the account, e.g. a credit card), a `date`, a signed
`amount` (negative for expenses, in currency units) and a `raw_description`:
its text in the raw files (e.g. the bank's export), often vague. They come from
the last ingest of the raw bank exports, and only change when they are ingested
again. What a transaction really was is its annotation's `description` (see
*Annotations*).

### IDs

A transaction's `id` is 16 hex characters, derived from its account, source,
date, raw description and amount (in minor units), plus an occurrence index that
tells identical transactions apart, counted in the order they are read. So an
ID changes when any of these fields changes in the bank's export (e.g. a
pending transaction whose raw description or amount changes later), and the
occurrence index can shift when identical transactions are added or removed.

### Annotations

An annotation belongs to one transaction ID: an optional `description` of the
whole transaction (what it really was, unlike the often vague raw description),
and a list of `parts`, each with a `tag`, a `subtag`, an `amount`, an optional
`description` and an optional `kind` (see *Kinds set on transactions*). Several parts are a split: every part has an amount,
signed like the transaction, with at most 2 decimal places, and they must sum
exactly to the transaction amount. A single part may leave out its amount,
which is then the full amount. An annotation without parts is a description
only (it then needs a description), and leaves the transaction untagged.
Descriptions (the transaction's and the parts') may have several lines: `\n` in
JSON. The web app shows a description's first line in tables, with the rest on
hover.

Each annotation also has its `authors`: everyone who wrote it, in the order of
their first contribution, without repeats. Every write that changes its content
(description, tags, parts or kinds) adds its `author` (any name, `owner` by
default: agents, give yours). An annotation also has a `needs_review` flag
(`false` by default). Changing only the flag, or nothing, adds no author.

To change some fields of an annotation, **patch** it (*Patch an annotation*,
*Patch annotations*): the fields left out are kept as they are, e.g. a new
description keeps the tags and the review flag. *Set an annotation* replaces it
whole instead, resetting every field left out (e.g. clearing the review flag).
Use it only to write the whole annotation.

### Transaction status

A transaction's `status` is `rule` if a rule tags it, otherwise `annotated` if
its annotation has parts, otherwise `untagged`. As a filter, `tagged` means
`rule` or `annotated`. Tag filters and counts use the effective tags: the
rule's if one matches, otherwise the annotation's parts.

### Tags

Tags are two-level: a `tag` (e.g. `food`) and a `subtag` (e.g. `groceries`).
The empty string is a valid subtag, and subtags belong to their tag. A part or
rule can only use a (tag, subtag) pair that exists, unless the write sets
`"allow_new_tags": true`, which creates it. A tag without any subtags can't be
used until it has one.

Properties: a `color` (any CSS color, stored as hex), and a `kind`, which says
what the tag's transactions are:

- `regular`: incomes and expenses (the default).
- `transfer`: money moving between own accounts (e.g. card payments, loans).
  Never incomes or expenses, nor in breakdowns or trends, but part of
  balances. Both sides should be recorded: the dashboard's data checks flag
  transfers that don't add up to 0.
- `saving`: money set aside. Not an expense: cash flows show it as `saved`
  ("set aside" in the web app, positive when set aside), measured in the
  accounts of spending groups (so the other side, in a holding group, doesn't
  cancel it), and the saved rate is saved / incomes. What was kept is the net
  (incomes less expenses), and the kept rate is net / incomes: the usual
  savings rate, whatever is tagged.
- `one-off`: real incomes or expenses that would distort averages (e.g. a
  wedding, a car). In monthly sums and breakdowns, but left out of rolling
  averages, trends and monthly averages unless `include_one_offs` is set.
- `reconciliation`: rows that re-anchor a balance, with no other side: an
  opening balance, or "on this date the wallet held X" (the difference between
  the counted amount and the balance of the rows, see `GET /api/balance`). Part
  of balances only: never incomes or expenses, nor in averages, trends or
  breakdowns, nor transfers. The bridge has them as `reconciliations`.

A subtag's own properties are `null` to inherit its tag's, and
`effective_color` and `effective_kind` are the results.

### Kinds set on transactions

A part of an annotation can have its own `kind`, over its tag's: e.g. one big
purchase tagged `home/furniture` but marked `one-off`, without a dedicated
subtag. `null` (the default) inherits the tag's. Everything uses the effective
kind: the one set on the part, else the subtag's, else the tag's. Read, a
part's `kind` is the effective one, and `kind_set` tells whether it was set on
the transaction. Rules only give tags, so a rule's transactions can't have a
kind set (their annotations are overridden).

### Accounts and groups

Accounts are configured in the config file, one per directory of raw exports,
and listed once ingested. Each has a `color`, and is in exactly one account
group. A group has a `name`, a `color`, a `position` (the display order) and a
`role`: `spending` (day-to-day money: cash flows, breakdowns and the
"available" balance) or `holding` (savings, investments or debt: only in the
"net worth", which is every group). New accounts go to the first spending
group. Transactions and analysis rows carry their account's `group` and
`group_role`.

### Rules

Rules tag transactions automatically, by their fields (not by tags): `filters`
with a `raw_description` (a regular expression, case-sensitive, matching
anywhere, e.g. `(?i)supermart` ignores case), an exact `source` and `account`,
and a `start_date` (inclusive) and `end_date` (exclusive), as `YYYY-MM-DD`.
Several fields combine with AND. Instead of fields (not with them),
`and_filters` or `or_filters` combine a list of nested filters, and
`"invert": true` negates any filters. Rules are ordered by `position` (0 is
first), and the first matching rule tags a transaction with a single
full-amount part. Rules take precedence over annotations.

### Overridden annotations

An annotation with parts, on a transaction that a rule tags, is overridden: the
rule's tags apply instead. Only the annotation's description is still used. It is
kept, and applies again if the rule stops matching.

### Orphans

An orphan is an annotation whose transaction ID is not in the ingested data
anymore (e.g. its ID changed). Orphans keep a `snapshot` of the transaction as
it was when annotated. They are only deleted explicitly.

### Guesses

An untagged transaction has a `guess` when another transaction with exactly
the same raw description is annotated: the tags of the first part of the one
nearest in date (the later one among equals), with
`"basis": "same_raw_description"` and its `from_transaction_id`. Otherwise the
guess is `null`. Guesses are never applied by the server.

### Analysis rows

Analyses work on rows: one per transaction part, with its tag, subtag and
`description`, the transaction's `transaction_description` and fields, `month` (`YYYY-MM`), `tags` (`tag/subtag`),
`kind` (the effective kind: set on the transaction, else its tag's, `regular`
if untagged), `kind_set` (whether it was set on the transaction), `group` and `group_role` (of its
account), and running balances: `balance_total` (the net worth),
`balance_role` (of the groups with its role, e.g. the available balance for
spending), `balance_group`, `balance_account` and `balance_source`. Untagged
transactions have `null` tags. Generated zero-amount ticks (with a `null` `id`)
mark the edges of the data when asked for.

Incomes are positive and expenses negative. Asking for expenses only returns
them as positive amounts. With `net_by_tag` (a dashboard default), incomes and
expenses are classified by the net of each tag over all the data rather than
by each transaction: a tag whose rows sum to 0 is an expense, and untagged rows
go by their own sign, so every row is one or the other. Running balances
accumulate by date, then account and source, incomes before expenses.

### Settings

Three kinds of state:
- **Data**: the store and the raw files, exported together.
- **Preferences**: the config file, how raw exports are read (`ingestion`: the
  `rules` that drop, rewrite or turn parsed rows into transfers between sources,
  and `filters`), and the store's settings below. Exported with the data.
- **Host**: where things are on this machine, read-only through the API (*Get
  the host settings*): the data directory (the store, the ingested data and the
  backups folder) and the **raw folder**. Never exported.

**The raw folder** holds a folder per account (`raw/<account>/`), and an
account's raw files are the files in its folder (not in its subfolders, and not
hidden ones). Accounts appear by making folders, or by uploading files for a new
account (*Upload raw files*). An account that only ingestion rules transfer rows
to has an empty folder. No endpoint takes a path: files move through uploads
and downloads only.

**Ingestion rules** (`ingestion.rules`, a list) reshape the parsed rows of every
account before the `filters`. Each parsed row goes by the first rule whose
`match` it meets (filters as rules have, without tags), and rules never apply to
rows a rule added. A rule's `action` is `drop` (leave the row out), `transfer`
(the row is money moving to `to_source`, of `to_account`, by default the row's
own: it stays as its source's leg, and a row of the opposite amount is added
there) or `rewrite`. Transfers and rewrites may set a new `raw_description` and
move the date by `shift_days` (of both legs). Rows keep the ID of the parsed
row, so changing a rule doesn't orphan annotations. Rules only reshape rows:
tags, kinds and descriptions are for tagging (e.g. a rule tagging `^Transfer`
raw descriptions with a `transfer` tag). A transfer rule over a period without
the other source's data makes the spending vanish: bound it with dates. The
dry run shows each file's rows after the rules, and the transfer rows it adds.
The store holds the dashboard's defaults and the colors of chart labels other
than accounts and tags. A server started with `--demo` serves an invented household from a
temporary directory: every change is lost when it stops.
"""

SERVER = OperationGroup(
    "Server",
    "",
    (
        Operation(
            "Server info",
            "GET",
            "/api/server",
            """
            `demo` (`true` if served with `--demo`, over invented data) and
            `stale_web_app` (why the served web app is older than its sources,
            or `null`).
            """,
            "curl {base}/api/server",
        ),
        Operation(
            "Status",
            "GET",
            "/api/status",
            """
            Counts of transactions (`transactions`, `untagged`, `annotated`,
            `rule_matched`), annotations (`overridden`, `needs_review`,
            `orphans`), the `accounts`, when an ingest last changed the
            ingested data (`ingested_changed_at`) and when one last ran,
            whether it changed anything or not (`ingested_checked_at`; both
            `null` if never), and the paths of the store (`db_path`) and the
            ingested data (`ingested_dir`).
            """,
            "curl {base}/api/status",
        ),
        Operation(
            "Glossary",
            "GET",
            "/api/glossary",
            """
            What every figure, label and concept of the app (and its dashboard)
            means: the source of the web app's tooltips. A list of entries, in
            order, each with a stable `key`, its `title` and `section`, the
            `labels` as shown, a `short` definition, how it is `computed`
            (Markdown: what is included and left out, with `[text](#key)`
            links to other entries) and `related` keys.
            """,
            """
            curl {base}/api/glossary
            curl -s {base}/api/glossary | jq '.[] | select(.key == "kept")'
            """,
        ),
    ),
)

INGEST = OperationGroup(
    "Ingest",
    "",
    (
        Operation(
            "Ingest dry run",
            "GET",
            "/api/ingest/dry-run",
            """
            Parses each raw file on its own, writing nothing. Returns
            `ingested_changed_at` and `ingested_checked_at` (as *Status*
            does), `raw_dir` (the raw folder) and `raw_dir_exists`, and per
            account in `accounts` (the raw folder's folders, by name) its
            `folder` and its `files`: each
            with `path`, `name`, `size`, `modified_at`, `newer_than_ingest`
            (modified after the last ingest), the `parser` that read it (or
            `null`), `parser_errors` (why each parser tried rejected it),
            `error` (why it can't be ingested, or `null`), `rows`, `kept_rows`
            (after the configured ingestion rules and filters),
            `transfer_rows` (the other legs that transfer rules add, in other
            sources), `start_date`, `end_date`, `total` (sum of the parsed
            amounts) and `sources` (per source of the kept rows: `rows`, dates
            and `total`).

            `overlaps` lists every pair of files of an account whose rows of a
            source overlap in dates, with the overlapping dates and
            `shared_rows` (rows with the same date, raw description and amount in
            both, likely duplicates). `problems` lists what is likely wrong,
            each with `kind` (`no_files`, `unparseable`, `empty`,
            `filtered_out`, `overlap`: one per pair of files sharing rows,
            `missing_account`: an ingestion rule transfers to an account without
            a folder, which fails the ingest, or `stray_file`: a file in the raw
            folder itself, not ingested), `account` (`null` for a stray file),
            `paths` and `message`.
            """,
            "curl {base}/api/ingest/dry-run",
        ),
        Operation(
            "Preview ingest",
            "GET",
            "/api/ingest/preview",
            """
            What an ingest would change, writing nothing. Returns `accounts`
            (per account, the transactions `before` and `after`),
            `transactions` (after), `unchanged` (transactions kept as they
            are, by ID), `new` and `vanished` (the transactions that would
            appear or disappear, each with `id`, `account`, `source`, `date`,
            `amount` and `raw_description`), `changed` (those whose ID stays but
            whose content would change, as they would be: only with the
            bank's IDs, since derived IDs change with the transaction),
            `new_orphans` (the annotations of vanishing
            transactions, as *List orphans* returns them) and
            `restored_orphan_ids` (orphans whose transaction would be back).
            422 if the ingest would fail, e.g. on an unparseable file.
            """,
            "curl {base}/api/ingest/preview",
        ),
        Operation(
            "Run an ingest",
            "POST",
            "/api/ingest",
            """
            Parses the raw bank exports of every account again, replacing the
            ingested data. Returns the transactions by account, their total,
            the IDs that appeared (`new_ids`), disappeared (`vanished_ids`) or
            changed (`changed_ids`) since the previous ingest, the orphans
            (`orphans` count, `new_orphan_ids`), whether anything changed
            (`data_changed`, as the preview would tell), and the times of
            *Status* after it: `ingested_changed_at` moves only if the data
            changed, `ingested_checked_at` always. Nothing is replaced if
            parsing fails (422).
            """,
            "curl -X POST {base}/api/ingest",
        ),
        Operation(
            "Upload raw files",
            "POST",
            "/api/raw/{account}",
            """
            Adds raw files (bank exports) to an account, as a multipart form:
            one or more `files`, saved under their own names in the account's
            folder of the raw folder (made if the account is new). By default
            it only checks (`dry_run`): each file is parsed, and the result has
            `account`, `folder`, `new_account` and per file its `name`, `path`
            (where it goes), `accepted` (a parser reads it), `check` (its dry
            run, as *Ingest dry run* describes files) and `replaces` (the dry
            run of the existing file of the same name, or `null`: compare their
            dates and rows).

            Committing (`dry_run=false`) refuses files no parser reads (422) and
            replacing existing files unless `replace=true` (409): ask the owner
            first, showing both files' coverage. Names must be plain file names
            (no folders, not hidden: 422). At most 50 MiB per file and 200 MiB
            per request (413). Every file is written, or none (if writing one
            fails, the others are undone). Nothing is ingested: preview, then
            ingest.
            """,
            """
            # Check, then add
            curl -F files=@card-2025-06.xls -F files=@bit-2025.csv {base}/api/raw/maya
            curl -F files=@card-2025-06.xls -F dry_run=false {base}/api/raw/maya
            # Replace a file of the same name (after the owner confirmed)
            curl -F files=@card-2025-06.xls -F dry_run=false -F replace=true {base}/api/raw/maya
            """,
        ),
    ),
)

TRANSACTIONS = OperationGroup(
    "Transactions",
    """
    Filters of transactions (all optional, combined with AND, lists match any
    of their values): `ids`, `start_date` (inclusive) and `end_date`
    (exclusive) as `YYYY-MM-DD`, `accounts`, `sources`, `tags` and `subtags`
    (both must match the same part when given together), `search`
    (case-insensitive text in the descriptions, the transaction's and its
    parts', or the raw description), `min_amount` and
    `max_amount` (inclusive, signed), `status` (`untagged`, `annotated`, `rule`
    or `tagged`), `needs_review` (`true`: flagged annotations, `false`:
    everything else), `authors` (any of them among the annotation's
    authors), `kinds` (any part of these effective kinds,
    untagged transactions being `regular`) and `kind_set` (`true`: a kind set
    on the transaction, `false`: none).
    """,
    (
        Operation(
            "List transactions",
            "GET",
            "/api/transactions",
            """
            The filters as query parameters, where a list repeats its
            parameter, e.g. `accounts=a&accounts=b`. Also `sort` (`date`,
            `amount`, `description`, `raw_description`, `account`, `source` or
            `id`, default `date`; `description` is case-insensitive, by the
            description, else the parts', else the raw description: what the
            web app shows), `descending` (default `false`), `limit` (default
            100) and `offset` (default 0).

            Returns `total` (all matches), `offset`, `limit` and `items`: each
            transaction with its `status`, `annotation` (or `null`), `rule`
            (the first matching rule's `id`, `position`, `tag` and `subtag`, or
            `null`) and `guess` (or `null`).
            """,
            """
            curl '{base}/api/transactions?status=untagged&limit=20'
            curl '{base}/api/transactions?search=supermart&tags=food&start_date=2025-01-01'
            """,
        ),
        Operation(
            "Query transactions",
            "POST",
            "/api/transactions/query",
            """
            The same as listing transactions, with the query as a JSON body.
            Here `limit` can be `null` for every match.
            """,
            """
            curl {base}/api/transactions/query -H 'Content-Type: application/json' \\
              -d '{"status": "untagged", "accounts": ["checking"], "limit": null}'
            """,
        ),
        Operation(
            "Get a transaction",
            "GET",
            "/api/transactions/{transaction_id}",
            """
            One transaction, as in a list. 404 if no current transaction has
            this ID.
            """,
            "curl {base}/api/transactions/TRANSACTION_ID",
        ),
    ),
)

ANNOTATIONS = OperationGroup(
    "Annotations",
    "",
    (
        Operation(
            "Set an annotation",
            "PUT",
            "/api/transactions/{transaction_id}/annotation",
            """
            Replaces the transaction's annotation whole: fields left out are
            reset, not kept (to keep them, patch it instead). Its authors are
            kept, adding `author` if the content changes. Body: `parts` (each with
            `tag`, `subtag` (required, may be `""`), and optional `amount`,
            `description` and `kind`, set on the transaction over its tag's), optional `description`, `needs_review` (default `false`),
            `author` (default `owner`) and `allow_new_tags` (default `false`).
            Returns the transaction. 422 for an unknown tag, a split part
            without an amount, more than 2 decimal places, parts that don't sum
            to the amount, or no parts and no description.
            """,
            """
            curl -X PUT {base}/api/transactions/TRANSACTION_ID/annotation -H 'Content-Type: application/json' \\
              -d '{"parts": [{"tag": "food", "subtag": "groceries"}], "author": "assistant"}'
            # A split, with descriptions
            curl -X PUT {base}/api/transactions/TRANSACTION_ID/annotation -H 'Content-Type: application/json' \\
              -d '{"parts": [{"tag": "food", "subtag": "groceries", "amount": -180.5}, {"tag": "shopping", "subtag": "gifts", "amount": -40, "description": "birthday"}], "description": "weekly shopping"}'
            # One big purchase, left out of averages
            curl -X PUT {base}/api/transactions/TRANSACTION_ID/annotation -H 'Content-Type: application/json' \\
              -d '{"parts": [{"tag": "home", "subtag": "furniture", "kind": "one-off"}]}'
            # A description only, flagged for review
            curl -X PUT {base}/api/transactions/TRANSACTION_ID/annotation -H 'Content-Type: application/json' \\
              -d '{"description": "what was this transfer for?", "needs_review": true}'
            """,
        ),
        Operation(
            "Patch an annotation",
            "PATCH",
            "/api/transactions/{transaction_id}/annotation",
            """
            Changes only the given fields of the transaction's annotation
            (creating it if there is none), keeping the others as they are.
            Fields (at least one):
            - `description`: `null` or `""` clears it.
            - `parts`: replace the parts, as in *Set an annotation* (a split
              must sum exactly to the amount). `null` or `[]` removes the
              tags, keeping the description.
            - `kind`: set on every part, over the tags' kinds. `null` clears
              it. Needs tags of the annotation (not a rule's), and can't be
              given with `parts` (give the kinds in the parts).
            - `needs_review`: `true` flags, `false` marks reviewed (not
              `null`).

            Also `author` (default `owner`, added to the authors unless only
            the review flag changes) and
            `allow_new_tags` (default `false`). Returns the transaction. 422
            for invalid parts or fields, or if nothing would be left (no tags
            and no description: delete the annotation instead).
            """,
            """
            # A new description, keeping the tags and the review flag
            curl -X PATCH {base}/api/transactions/TRANSACTION_ID/annotation -H 'Content-Type: application/json' \\
              -d '{"description": "birthday gift for Dana", "author": "assistant"}'
            # Mark it reviewed, and a one-off
            curl -X PATCH {base}/api/transactions/TRANSACTION_ID/annotation -H 'Content-Type: application/json' \\
              -d '{"needs_review": false, "kind": "one-off"}'
            """,
        ),
        Operation(
            "Delete an annotation",
            "DELETE",
            "/api/transactions/{transaction_id}/annotation",
            """
            Deletes the transaction's annotation, if any. Returns the
            transaction. 404 if no current transaction has this ID (see
            *Delete annotations* for orphans).
            """,
            "curl -X DELETE {base}/api/transactions/TRANSACTION_ID/annotation",
        ),
        Operation(
            "Set annotations",
            "POST",
            "/api/annotations/batch",
            """
            Sets many annotations at once, atomically: `items` (each an
            annotation with the transaction's `id`), and for all of them
            `author` (default `owner`) and `allow_new_tags` (default `false`).
            Each item replaces that transaction's annotation whole (see *Patch
            annotations* to keep what items leave out). With
            `"dry_run": true`, it only validates. If any item is invalid
            (including a repeated ID), nothing is written, and the 422 response
            lists every invalid item under `errors`, each with its `index`,
            `id` and `message`. Returns `updated_ids`.
            """,
            """
            curl {base}/api/annotations/batch -H 'Content-Type: application/json' \\
              -d '{"author": "assistant", "items": [{"id": "ID_1", "parts": [{"tag": "food", "subtag": "groceries"}]}, {"id": "ID_2", "parts": [{"tag": "car", "subtag": "fuel"}], "description": "trip", "needs_review": true}]}'
            """,
        ),
        Operation(
            "Patch annotations",
            "PATCH",
            "/api/annotations/batch",
            """
            Patches many annotations at once, atomically: `items` (each the
            fields of *Patch an annotation*, with the transaction's `id`), and
            for all of them `author`, `allow_new_tags` and `dry_run`, as in
            *Set annotations*. Each item changes only its given fields. Errors
            and the result are as in *Set annotations*.
            """,
            """
            curl -X PATCH {base}/api/annotations/batch -H 'Content-Type: application/json' \\
              -d '{"author": "assistant", "items": [{"id": "ID_1", "description": "school trip"}, {"id": "ID_2", "needs_review": true}]}'
            """,
        ),
        Operation(
            "Review annotations",
            "POST",
            "/api/annotations/review",
            """
            Sets the `needs_review` flag of existing annotations, leaving the
            rest of them as is: `false` (the default) marks them reviewed,
            `true` flags them. Their authors are kept (the flag isn't content,
            so there's no `author`). 404 if any of the IDs has no annotation,
            and then nothing changes.
            """,
            """
            curl {base}/api/annotations/review -H 'Content-Type: application/json' -d '{"ids": ["ID_1", "ID_2"]}'
            curl {base}/api/annotations/review -H 'Content-Type: application/json' -d '{"ids": ["ID_1"], "needs_review": true}'
            """,
        ),
        Operation(
            "Set kinds on transactions",
            "POST",
            "/api/annotations/kind",
            """
            Sets `kind` on tagged transactions (`ids`), over their tags' kinds,
            or clears it with `"kind": null`, leaving the rest of their
            annotations as is. Every part of each, or only `part` (an index).
            Also `author`.
            422, and nothing changes, if any is untagged, tagged by a rule, or
            has no such part (`errors` lists them). Returns `kind` and
            `updated_ids`.
            """,
            """
            curl {base}/api/annotations/kind -H 'Content-Type: application/json' -d '{"ids": ["ID_1"], "kind": "one-off"}'
            curl {base}/api/annotations/kind -H 'Content-Type: application/json' -d '{"ids": ["ID_1"], "kind": null}'
            """,
        ),
        Operation(
            "Delete annotations",
            "POST",
            "/api/annotations/clear",
            """
            Deletes the annotations of these transaction IDs, including
            orphans. Returns `cleared_ids`, and `not_annotated_ids` for IDs
            without an annotation (not an error).
            """,
            """
            curl {base}/api/annotations/clear -H 'Content-Type: application/json' -d '{"ids": ["ID_1", "ID_2"]}'
            """,
        ),
        Operation(
            "Change matching transactions",
            "POST",
            "/api/annotations/bulk",
            """
            Changes some fields of every transaction matching a `query` (the
            filters of transactions), keeping the others. Every field is
            optional (`null` keeps it), but at least one is needed:
            - `tag` and `subtag` (together): replace the parts, including
              splits, with a single full-amount part. A kind set on all the
              old parts is kept.
            - `kind`: set on every part, over the tags' kinds. `"inherit"`
              clears it.
            - `description`: the transaction's description, `""` clears it.
            - `needs_review`: `true` flags, `false` marks reviewed.

            Transactions tagged by a rule are skipped. So are untagged ones
            when a `kind` is given without `tag`, or `needs_review` would be
            left on nothing (no tags or description). Also `author` and
            `allow_new_tags`.

            By default (`"dry_run": true`) it only previews. Returns
            `affected_ids`, `replaced_ids` (affected, whose tags are replaced),
            `unchanged_ids` (already as requested, left alone),
            `skipped_rule_ids` and `skipped_untagged_ids`. With `"dry_run":
            false` it writes, and with `expected_ids` it fails with 409,
            writing nothing, unless exactly those transactions are affected.
            """,
            """
            curl {base}/api/annotations/bulk -H 'Content-Type: application/json' \\
              -d '{"query": {"status": "untagged", "search": "Roadstar Fuel"}, "tag": "car", "subtag": "fuel"}'
            curl {base}/api/annotations/bulk -H 'Content-Type: application/json' \\
              -d '{"query": {"status": "untagged", "search": "Roadstar Fuel"}, "tag": "car", "subtag": "fuel", "dry_run": false, "expected_ids": ["ID_1", "ID_2"]}'
            curl {base}/api/annotations/bulk -H 'Content-Type: application/json' \\
              -d '{"query": {"ids": ["ID_1", "ID_2"]}, "kind": "one-off", "dry_run": false}'
            """,
        ),
    ),
)

TAGS = OperationGroup(
    "Tags",
    "",
    (
        Operation(
            "List tags",
            "GET",
            "/api/tags",
            """
            Every tag with its `color`, `kind`, number of current
            `transactions`, and `subtags`. Each subtag has its own and
            effective properties, and its usage: `annotations` (annotated
            transactions, including orphans), `rules`, and current
            `transactions`.
            """,
            "curl {base}/api/tags",
        ),
        Operation(
            "Create a tag",
            "POST",
            "/api/tags",
            """
            A `tag`, or with `subtag` a subtag (creating its tag if missing),
            with optional `color` and `kind`. 409 if it exists. Returns the tag
            (201).
            """,
            """
            curl {base}/api/tags -H 'Content-Type: application/json' -d '{"tag": "food", "subtag": "bakery"}'
            """,
        ),
        Operation(
            "Change tag properties",
            "PATCH",
            "/api/tags",
            """
            The `tag`, or with `subtag` the subtag, whose given properties
            change: `color` and `kind` (`regular`, `transfer`, `saving`,
            `one-off` or `reconciliation`). For a subtag, `null` inherits the tag's. Returns the
            tag.
            """,
            """
            curl -X PATCH {base}/api/tags -H 'Content-Type: application/json' -d '{"tag": "transfer", "kind": "transfer"}'
            curl -X PATCH {base}/api/tags -H 'Content-Type: application/json' -d '{"tag": "car", "subtag": "purchase", "kind": "one-off"}'
            curl -X PATCH {base}/api/tags -H 'Content-Type: application/json' -d '{"tag": "food", "subtag": "groceries", "color": null}'
            """,
        ),
        Operation(
            "Rename a tag",
            "POST",
            "/api/tags/rename",
            """
            Renames the `tag`, or with `subtag` the subtag, to `new_name`. The
            annotations and rules using it follow. 400 if the new name exists.
            """,
            """
            curl {base}/api/tags/rename -H 'Content-Type: application/json' -d '{"tag": "food", "subtag": "groceries", "new_name": "supermarket"}'
            """,
        ),
        Operation(
            "Merge tags",
            "POST",
            "/api/tags/merge",
            """
            Moves the annotations and rules of a tag into `into_tag`, or of a
            subtag into `into_tag` and `into_subtag` (created if missing), then
            deletes it, dropping its properties. Merging a tag moves its
            subtags, merging those that exist in both. Returns the target tag.
            """,
            """
            curl {base}/api/tags/merge -H 'Content-Type: application/json' -d '{"tag": "dining", "into_tag": "food"}'
            curl {base}/api/tags/merge -H 'Content-Type: application/json' -d '{"tag": "food", "subtag": "market", "into_tag": "food", "into_subtag": "groceries"}'
            """,
        ),
        Operation(
            "Delete a tag",
            "POST",
            "/api/tags/delete",
            """
            Deletes a `tag` with its subtags, or with `subtag` a subtag. 409 if
            any annotation (including orphans) or rule uses it. Returns 204.
            """,
            """
            curl {base}/api/tags/delete -H 'Content-Type: application/json' -d '{"tag": "food", "subtag": "bakery"}'
            """,
        ),
    ),
)

ACCOUNTS = OperationGroup(
    "Accounts and groups",
    "",
    (
        Operation(
            "List accounts",
            "GET",
            "/api/accounts",
            """
            Every account's `name`, `group`, `group_role`, `color`, number
            of `transactions` and `sources` (with transactions).
            """,
            "curl {base}/api/accounts",
        ),
        Operation(
            "Balance on a date",
            "GET",
            "/api/balance",
            """
            A balance at the end of a `date` (default: the latest data's), with
            every row up to it, of every kind: of an `account`, one of its
            sources (`account` and `source`), a `group`, or the net worth
            (neither). Returns its `label`, `balance`, the number of `rows` up
            to the date and the `last_date` among them. To re-anchor a balance
            (e.g. a counted wallet), tag a row of the counted amount less this
            balance with a `reconciliation` tag. 404 for an account, source or
            group without transactions, 409 before the first ingest.
            """,
            """
            curl '{base}/api/balance?account=cash&source=wallet&date=2025-03-31'
            curl '{base}/api/balance?group=savings'
            """,
        ),
        Operation(
            "Change an account",
            "PATCH",
            "/api/accounts/{name}",
            """
            Any of `group` (an existing group) and `color` (`null` clears it).
            Returns the account.
            """,
            """
            curl -X PATCH {base}/api/accounts/NAME -H 'Content-Type: application/json' -d '{"group": "savings"}'
            """,
        ),
        Operation(
            "List groups",
            "GET",
            "/api/groups",
            """
            Every account group in order: `name`, `role`, `color`, `position`
            and its `accounts`.
            """,
            "curl {base}/api/groups",
        ),
        Operation(
            "Create a group",
            "POST",
            "/api/groups",
            """
            A group's `name`, and optional `role` (`spending` by default, or
            `holding`), `color` and `position` (last by default). 409 if it
            exists. Returns the group (201).
            """,
            """
            curl {base}/api/groups -H 'Content-Type: application/json' -d '{"name": "debt", "role": "holding"}'
            """,
        ),
        Operation(
            "Change a group",
            "PATCH",
            "/api/groups/{name}",
            """
            Any of `new_name`, `role`, `color` (`null` clears it) and
            `position` (0 is first). Returns the group.
            """,
            """
            curl -X PATCH {base}/api/groups/NAME -H 'Content-Type: application/json' -d '{"role": "holding", "position": 1}'
            """,
        ),
        Operation(
            "Delete a group",
            "DELETE",
            "/api/groups/{name}",
            """
            Deletes a group without accounts (409 otherwise: move them first).
            Returns 204.
            """,
            "curl -X DELETE {base}/api/groups/NAME",
        ),
    ),
)

RULES = OperationGroup(
    "Rules",
    "",
    (
        Operation(
            "List rules",
            "GET",
            "/api/rules",
            """
            Every rule in order: `id`, `position`, `filters`, `tag`, `subtag`,
            and the number of current `transactions` it tags (not counting
            those an earlier rule tags).
            """,
            "curl {base}/api/rules",
        ),
        Operation(
            "Create a rule",
            "POST",
            "/api/rules",
            """
            `filters`, `tag` and `subtag`, with optional `position` (default
            last) and `allow_new_tags`. 422 for filters that match everything,
            use tags, or mix fields with nested filters. Returns the rule
            (201).
            """,
            """
            curl {base}/api/rules -H 'Content-Type: application/json' \\
              -d '{"filters": {"raw_description": "^Roadstar"}, "tag": "car", "subtag": "fuel"}'
            curl {base}/api/rules -H 'Content-Type: application/json' \\
              -d '{"filters": {"or_filters": [{"raw_description": "Netflix"}, {"raw_description": "Spotify"}]}, "tag": "leisure", "subtag": "streaming", "position": 0}'
            """,
        ),
        Operation(
            "Preview a rule",
            "POST",
            "/api/rules/preview",
            """
            What a rule with these `filters` would do at a `position` (default
            last), without writing. With `rule_id`, previews a change to that
            rule (by default at its current position). Returns `matched_ids`
            (matching the filters), `effective_ids` (matching and not tagged by
            an earlier rule), `newly_tagged_ids` (effective and currently
            untagged) and `overridden_annotation_ids` (effective, with an
            annotation).
            """,
            """
            curl {base}/api/rules/preview -H 'Content-Type: application/json' -d '{"filters": {"raw_description": "^Roadstar"}}'
            """,
        ),
        Operation(
            "Reorder rules",
            "POST",
            "/api/rules/reorder",
            """
            `rule_ids`: every rule ID exactly once, in the new order (422
            otherwise). Returns the rules.
            """,
            """
            curl {base}/api/rules/reorder -H 'Content-Type: application/json' -d '{"rule_ids": [3, 1, 2]}'
            """,
        ),
        Operation(
            "Get a rule",
            "GET",
            "/api/rules/{rule_id}",
            "One rule, as in the list.",
            "curl {base}/api/rules/RULE_ID",
        ),
        Operation(
            "Change a rule",
            "PATCH",
            "/api/rules/{rule_id}",
            """
            Any of `filters`, `tag` and `subtag` (given together), and
            `position` (moving it), plus `allow_new_tags`. Returns the rule.
            """,
            """
            curl -X PATCH {base}/api/rules/RULE_ID -H 'Content-Type: application/json' -d '{"tag": "car", "subtag": "parking"}'
            curl -X PATCH {base}/api/rules/RULE_ID -H 'Content-Type: application/json' -d '{"position": 0}'
            """,
        ),
        Operation(
            "Delete a rule",
            "DELETE",
            "/api/rules/{rule_id}",
            """
            Returns 204. The transactions it tagged fall back to later rules or
            to their annotations.
            """,
            "curl -X DELETE {base}/api/rules/RULE_ID",
        ),
    ),
)

ORPHANS = OperationGroup(
    "Orphans and overridden annotations",
    "",
    (
        Operation(
            "List orphans",
            "GET",
            "/api/orphans",
            """
            Orphaned annotations: `transaction_id`, the `snapshot` of the
            transaction (`account`, `source`, `date`, `amount`,
            `raw_description`), `description`, `parts`, `authors` and `needs_review`.
            """,
            "curl {base}/api/orphans",
        ),
        Operation(
            "Delete orphans",
            "POST",
            "/api/orphans/delete",
            """
            Deletes orphaned annotations by transaction ID. 422 if any ID is
            not an orphan, and then nothing is deleted. Returns `deleted_ids`.
            """,
            """
            curl {base}/api/orphans/delete -H 'Content-Type: application/json' -d '{"ids": ["ID_1"]}'
            """,
        ),
        Operation(
            "List overridden annotations",
            "GET",
            "/api/overridden",
            """
            Overridden annotations, as orphans are listed, each with the `rule`
            that overrides it.
            """,
            "curl {base}/api/overridden",
        ),
    ),
)

ANALYSIS = OperationGroup(
    "Analysis",
    """
    Query parameters of the analysis, all optional. Filters: `start_date`
    (inclusive), `end_date` (exclusive), `accounts`, `sources`, `tags`,
    `subtags`, `search`, `min_amount` and `max_amount` (inclusive, on the
    row's amount), `kind_set`. `kinds`: only rows of these kinds (all if not
    given), e.g.
    `regular` and `one-off` for incomes and expenses. `groups`: only accounts
    of these groups (by default, the spending groups), or `all_groups=true`.
    Options (`false` by default): `incomes` or `expenses` (only those, of
    regular and one-off tags unless `kinds` is given, expenses as positive
    amounts), `net_by_subtag` (net by subtag rather than by tag, when netting
    by tag) and `sentinels` (include generated zero-amount ticks).
    """,
    (
        Operation(
            "List analysis rows",
            "GET",
            "/api/analysis/rows",
            """
            Returns `total` and the `rows`, sorted by date. 409 before the
            first ingest.
            """,
            """
            curl '{base}/api/analysis/rows?expenses=true&kinds=regular&kinds=one-off&start_date=2025-01-01'
            """,
        ),
        Operation(
            "Aggregate analysis rows",
            "GET",
            "/api/analysis/aggregate",
            """
            Sums analysis rows by `group_by`: any of `month` (the default),
            `tag`, `subtag`, `tags`, `account`, `source`, `account_source`,
            `group`, `group_role` and `kind`, repeated for several. Returns the
            `group_by` and `rows`, each with the group's values, the `amount`
            sum and the `count` of rows.
            """,
            """
            curl '{base}/api/analysis/aggregate?group_by=month&group_by=tag&expenses=true&kinds=regular&kinds=one-off'
            """,
        ),
    ),
)

DASHBOARD = OperationGroup(
    "Dashboard",
    "",
    (
        Operation(
            "Dashboard tables",
            "POST",
            "/api/dashboard",
            """
            Every table of the web app's dashboard over a period of months.
            Options (all optional): `period` (`all` (default), `ytd`, `3m`,
            `6m`, `12m` or `24m`, counted back from the data's last month),
            `start_month` and `end_month` (`YYYY-MM`, inclusive, override the
            period's), `accounts` (only these, as if the others didn't exist),
            `groups` (only accounts of these groups in cash flows, breakdowns
            and trends: the spending groups by default), `tags` (only these in
            cash flows and breakdowns), `include_one_offs` (in averages and
            trends too), `net_by_tag`, and `complete_months_only` (monthly
            figures leave out the current, incomplete month; balances always
            run to the latest data). Unset options take the dashboard
            defaults. `subtag_trends` (false by default) also computes the
            trends by subtag.

            Returns the resolved `options`, the `groups`, the `period` (its
            months), `headline` numbers (net worth and available balance
            `as_of` a date, the `runway` by rolling window (typical monthly
            expenses without one-offs, and the available balance in months of
            them), the last month's cash flow, the period's `totals`, monthly
            `averages`, top expenses), `balance` (a point a day, at the day's
            end, for the net worth, the available balance, each group and each
            account and source, and totals with their `change` and `one_offs`
            over the period), `cash_flow` (incomes, expenses, saved,
            net, `kept_rate` (net / incomes) and `saved_rate` by month, their
            one-off parts, `rolling` means and `baseline`: the rolling mean
            before each month, which it is compared with), `breakdown` (by tag
            and subtag: totals, rolling means and every month, adding up to the
            cash flow), `trends` (by tag, and with `subtag_trends`, `subtags`:
            by tag and subtag, adding up to the tag's), the `bridge` (for the available balance and
            the net worth: `opening` + `incomes` - `expenses` - `saved` +
            `transfers` + `reconciliations` = `closing` over the period's
            months, to the cent, then
            the change `later`, to the balances' date) and `health` (each
            account and source's `last_date`, flagged `behind` when it usually
            has rows but stopped over a month before the latest data;
            `saving_rows`, 0 when nothing in the period is tagged as saving;
            `sign_flips` of tags; the `transfer_residual` of every group, 0 when
            both sides of every transfer are recorded (reconciliations aren't
            transfers); and `income_months` far
            from the usual). Rolling means are plain means of the last 1, 3, 6
            or 12 calendar months (keys `"1"` to `"12"`), with 0 in months
            without rows. 409 before the first ingest.
            """,
            """
            curl {base}/api/dashboard -H 'Content-Type: application/json' -d '{"period": "12m"}'
            """,
        ),
        Operation(
            "Dashboard rows",
            "POST",
            "/api/dashboard/rows",
            """
            The analysis rows behind a number of the dashboard: the dashboard's
            `options`, and a `selection`: `flow` (`net`, `incomes`, `expenses`
            or `saved`), `kinds` (of tags, e.g. `["regular", "one-off"]` as
            cash flows), `all_groups` (default: the option's groups; their rows
            run to the latest data, as balances do), `untagged`, and the
            `tag`, `subtag`, `month`, `date`, `account_source`, `group`,
            `group_role` and `kind` (e.g. the one-off part of a month's
            expenses) to match. With `line` (`available` or `net_worth`), the
            rows of a bridge's term instead (e.g. `{"line": "available",
            "kinds": ["transfer"]}`), or with `after_months` those after the
            period's months. Returns `total` and the `rows`, newest first
            (within a day, in the reverse of the order balances accumulate).
            """,
            """
            curl {base}/api/dashboard/rows -H 'Content-Type: application/json' \\
              -d '{"options": {"period": "12m"}, "selection": {"flow": "expenses", "kinds": ["regular", "one-off"], "tag": "food", "month": "2025-06"}}'
            """,
        ),
    ),
)

SETTINGS = OperationGroup(
    "Settings",
    "",
    (
        Operation(
            "Get dashboard defaults",
            "GET",
            "/api/settings/dashboard",
            """
            Defaults of the dashboard's options: `net_by_tag`,
            `breakdown_months` (of the monthly view), `complete_months_only`
            and `rolling_months` (1, 3, 6 or 12: the rolling average shown and
            compared with).
            """,
            "curl {base}/api/settings/dashboard",
        ),
        Operation(
            "Set dashboard defaults",
            "PUT",
            "/api/settings/dashboard",
            """
            Replaces them all (missing ones are reset to their defaults: not
            netting by tag, 3 months, complete months only, 6-month averages).
            """,
            """
            curl -X PUT {base}/api/settings/dashboard -H 'Content-Type: application/json' \\
              -d '{"net_by_tag": true, "breakdown_months": 6, "complete_months_only": true, "rolling_months": 6}'
            """,
        ),
        Operation(
            "Get the display settings",
            "GET",
            "/api/settings/display",
            """
            How the web app shows things: `date_format`, how it shows and
            reads full dates (`dd/mm/yyyy`, the default, `yyyy-mm-dd` or
            `mm/dd/yyyy`). Only the web app's display and date inputs follow
            it: the API's dates are always ISO (`yyyy-mm-dd`), and so are the
            web app's URLs.
            """,
            "curl {base}/api/settings/display",
        ),
        Operation(
            "Set the display settings",
            "PUT",
            "/api/settings/display",
            """
            Replaces them (missing ones are reset to their defaults:
            `dd/mm/yyyy`). Another format is refused (422).
            """,
            """
            curl -X PUT {base}/api/settings/display -H 'Content-Type: application/json' -d '{"date_format": "yyyy-mm-dd"}'
            """,
        ),
        Operation(
            "Get the keyboard shortcuts",
            "GET",
            "/api/settings/keymap",
            """
            The web app's keyboard shortcuts: `directions`, the keys of
            `left`, `down`, `up` and `right` (vim's `h` `j` `k` `l` by
            default), and `overrides`, the keys of actions (by action ID, e.g.
            `tags.rename`) that replace their defaults. The actions, their
            defaults and where each is active are in `finalyze/keymap.json`
            (the web app's Settings page lists them with their IDs).
            """,
            "curl {base}/api/settings/keymap",
        ),
        Operation(
            "Set the keyboard shortcuts",
            "PUT",
            "/api/settings/keymap",
            """
            Replaces them (missing fields are reset: `h` `j` `k` `l`, no
            overrides). A key is named like `x`, `X` (shift+x), `?`,
            `Ctrl+Enter` or `Ctrl+Shift+a`, and an empty list unbinds an
            action. A direction's key is one character without Shift (shifted,
            it reorders), and the arrows always work too. A page's keys
            (`go.dashboard`, ...) are only its letter, after the `go.prefix`
            key (`g`). Keys that clash (the same key for two actions active
            together, two directions, or a fixed key such as Enter) are
            refused with 422 and `problems`, the clashes in words, as on the
            Settings page. Unknown action IDs are ignored.
            """,
            """
            curl -X PUT {base}/api/settings/keymap -H 'Content-Type: application/json' \\
              -d '{"directions": {"left": "j", "down": "k", "up": "l", "right": ";"}, "overrides": {"tags.rename": ["N"]}}'
            """,
        ),
        Operation(
            "Get label colors",
            "GET",
            "/api/settings/colors",
            """
            `colors`: colors of chart labels other than accounts and tags, by
            label, and `labels`: the labels the dashboard draws (`net worth`,
            `available`, and every account and source, e.g. `alice/card`).
            Accounts, groups and tags have their own colors.
            """,
            "curl {base}/api/settings/colors",
        ),
        Operation(
            "Set a label color",
            "PUT",
            "/api/settings/colors",
            """
            Sets a `label`'s `color` (any CSS color, `null` removes it). An
            account or tag's color is set on the account or tag instead (422).
            """,
            """
            curl -X PUT {base}/api/settings/colors -H 'Content-Type: application/json' -d '{"label": "net worth", "color": "#bfb"}'
            """,
        ),
        Operation(
            "Get the host settings",
            "GET",
            "/api/settings/host",
            """
            Where things are on this machine, from the host file (`host.toml`,
            next to the config): `file`, `file_exists` (without it, the
            defaults apply), `data_dir`, `raw_dir` (the raw folder, by default
            `raw` in the data directory), `backups_dir` and the default `host`
            and `port` to serve on (`null`: 127.0.0.1 and 8765). Read-only:
            they are edited in the file, and apply when the server starts.
            Never exported.
            """,
            "curl {base}/api/settings/host",
        ),
        Operation(
            "Get the config",
            "GET",
            "/api/settings/config",
            """
            The config file's settings (`config`, with defaults for missing
            ones: the preferences, without any path) and its path (`file`).
            """,
            "curl {base}/api/settings/config",
        ),
        Operation(
            "Set the config",
            "PUT",
            "/api/settings/config",
            """
            Replaces the config file's settings with a whole `config` (as
            returned by *Get the config*, without `file`),
            validated first, and uses it immediately. The file is rewritten
            without its comments, and the previous one is kept as
            `config.toml.bak`.
            """,
            """
            curl {base}/api/settings/config | jq .config > config.json
            curl -X PUT {base}/api/settings/config -H 'Content-Type: application/json' -d @config.json
            """,
        ),
    ),
)

EXPORT = OperationGroup(
    "Export, import and backups",
    """
    An export is one zip of the whole instance, for safekeeping or to move it
    to another machine: a consistent snapshot of the store (safe while
    serving), the ingested data, the raw folder (`raw/<account>/`) and the
    config file, with a `manifest.json` (when, the app's version and commit,
    the store's schema version, the accounts, and a backup's `name`). Never the
    host file. It holds all the data: keep it as private as the data itself.
    Exports are downloaded and imports uploaded.

    A **backup** is an export kept on this machine, in the backups folder of
    the data directory, with a name. They are made on request, and before every
    import or restore unless declined. At most 10 are kept, and none is ever
    deleted automatically: at the limit, a new backup is refused (409), and so
    is an import or a restore that asked for one, until a backup is deleted.
    Names are required, at most 100 characters, and unique whatever their case.
    A backup is identified by its `file` name (its name, made safe for file
    names).
    """,
    (
        Operation(
            "Export",
            "GET",
            "/api/export",
            """
            Downloads the export (`application/zip`, as an attachment named
            `finalyze-export-<YYYY-MM-DD_HH-MM-SS>.zip`). Nothing is kept on
            the server.
            """,
            """
            curl -OJ {base}/api/export
            curl -o finalyze.zip {base}/api/export
            """,
        ),
        Operation(
            "Import",
            "POST",
            "/api/import",
            """
            Replaces the whole instance with an uploaded export: a multipart
            form with the zip as `file` (at most 1 GiB, 413). By default it
            only checks (`dry_run`): the zip (its manifest, files, config and
            store, and that its schema version is supported), and the backup
            asked for. It returns the `manifest`, the `raw_dir` (this host's
            raw folder), the number of `raw_files`, and what happens to the raw
            folder's files now (relative to it): `replaced_raw_files` (the
            export has them too), `removed_raw_files` (it doesn't), and of
            those, `unbacked_raw_files` (files ingesting ignores, which no
            backup holds: hidden ones, those in subfolders of accounts, and
            those outside account folders). Show these to the owner before
            committing with `dry_run=false`.

            Committing first backs up the current state (`backup`, true by
            default, named `backup_name`, by default `before import on
            <date>`: see *List backups*), then:
            - replaces the raw folder's contents with the export's raw files,
              a folder per account, exactly (its hidden entries stay). Files the
              export doesn't have are gone, except from the backup
            - replaces the config file with the export's (the previous one is
              kept as `config.toml.bak`). The host file stays as it is
            - replaces the ingested data and the store with the export's (an
              older store is upgraded)

            Other requests wait while it runs, and the new data is served as
            soon as it ends. Nothing is written if the zip is invalid (422:
            including any file outside the expected folders, or one too big
            once unpacked), or if the backup asked for can't be made (409 at
            the limit or for a name that is taken, 422 for an invalid name:
            delete a backup, or pass `backup=false`). If a step fails, the
            earlier ones are undone, and the raw folder is as it was (the backup
            stays).
            """,
            """
            # Check, then import (backed up first, as "before the move")
            curl -F file=@finalyze.zip {base}/api/import
            curl -F file=@finalyze.zip -F dry_run=false -F 'backup_name=before the move' {base}/api/import
            # Without a backup
            curl -F file=@finalyze.zip -F dry_run=false -F backup=false {base}/api/import
            """,
        ),
        Operation(
            "List backups",
            "GET",
            "/api/backups",
            """
            The backups, newest first, in `backups` (each with its `file`,
            `name`, `created_at`, `size` in bytes and `accounts`), with the
            `folder`, the `limit`, and the `default_names` of the backups made
            before an `import` or a `restore` (unique, with the date in the web
            app's format, e.g. `before import on 01-11-2026`).
            """,
            "curl {base}/api/backups",
        ),
        Operation(
            "Create a backup",
            "POST",
            "/api/backups",
            """
            Backs up the whole instance as `name`, and returns it as *List
            backups* does. Refused at the limit, or if a backup has the same
            name (409).
            """,
            """
            curl -X POST {base}/api/backups -H 'Content-Type: application/json' -d '{"name": "before cleaning tags"}'
            """,
        ),
        Operation(
            "Download a backup",
            "GET",
            "/api/backups/{file}",
            """
            Downloads a backup by its `file` (404 if there is none): an export,
            which imports anywhere.
            """,
            "curl -o backup.zip '{base}/api/backups/before%20cleaning%20tags.zip'",
        ),
        Operation(
            "Restore a backup",
            "POST",
            "/api/backups/{file}/restore",
            """
            Replaces the whole instance with a backup's, as *Import* does,
            including the raw folder's contents. JSON body: `dry_run` (default
            true: checks only, and returns what *Import* does), `backup`
            (default true: back up the current state first) and `backup_name`
            (default `before restore on <date>`).
            """,
            """
            curl -X POST '{base}/api/backups/before%20cleaning%20tags.zip/restore' -H 'Content-Type: application/json' -d '{}'
            curl -X POST '{base}/api/backups/before%20cleaning%20tags.zip/restore' -H 'Content-Type: application/json' -d '{"dry_run": false}'
            """,
        ),
        Operation(
            "Delete a backup",
            "DELETE",
            "/api/backups/{file}",
            """
            Deletes a backup (204, 404 if there is none). It may be the only
            copy of the data before an import: download it first if in doubt.
            """,
            "curl -X DELETE '{base}/api/backups/before%20cleaning%20tags.zip'",
        ),
    ),
)

GROUPS = (
    SERVER,
    INGEST,
    TRANSACTIONS,
    ANNOTATIONS,
    TAGS,
    ACCOUNTS,
    RULES,
    ORPHANS,
    ANALYSIS,
    DASHBOARD,
    SETTINGS,
    EXPORT,
)
OPERATIONS = tuple(operation for group in GROUPS for operation in group.operations)

ERRORS = """
## Errors

Errors come back as JSON with a `detail`: a message with 404 (not found), 409
(conflicts with the current state), 422 (invalid request) or 400 (another
invalid operation). For a body or parameters that don't fit the schema
(including unknown fields), the 422 `detail` is instead a list of objects,
each with a `loc`, a `msg` and a `type`. Writes that a browser sends from
another site are refused with 403 (curl and other clients that aren't browsers
are unaffected).
"""


def render(base_url: str) -> str:
    """The instructions, for the API served at `base_url` (no trailing slash)."""
    sections = [
        _dedent(INTRO),
        _dedent(CONCEPTS),
        *(group.render(base_url) for group in GROUPS),
        _dedent(ERRORS),
    ]
    return "\n\n".join(sections).replace(BASE_PLACEHOLDER, base_url) + "\n"
