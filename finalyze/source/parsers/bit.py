"""The Bit payment app's CSV export.

One row per transaction, in the Bit balance's terms: every payment out (whether
paid by credit card or from the balance) is negative, and every payment received
into the balance is positive. How card-paid payments are funded (a transfer from
the card) is left to the ingestion config.

Rows paid by bank account are money that went to the bank, and the export can't
tell which kind: a withdrawal from the balance (its counterparty is the user) or
a payment received straight into the bank (its counterparty is the payer). So
they are all emitted the same, as money received into the balance (positive,
with a "Bank account: " prefix), and the ingestion config decides. The intended
rules: drop the user's own withdrawals by their name (the balance was already
credited by the payments that funded them), and turn checking's side of every
such row into a transfer from bit. A payment received into the bank then nets
to zero in bit while keeping its payer and memo. Only credits were ever seen,
so a bank-account debit is rejected.

The format is asserted strictly (anything never seen in real exports is
rejected), so a changed or edited file fails rather than losing money quietly:
the header, then transaction rows, then a blank row and the numbered disclaimer
lines, which must be there.
"""

import csv
import datetime
import re

import polars as pl

from finalyze.source.parsing import PARSED_SCHEMA, ParsingError, register_parser

SOURCE = "bit"
HEADER = (
    "Status",
    "Description",
    "Fee Amount",
    "Amount",
    "Payment Method",
    "Credit/Debit",
    "From/To",
    "Date",
)
DONE = "Done"
SKIPPED_STATUSES = {"Expired", "Declined", "You declined"}
"""Of rows that moved no money (an expired or declined request or payment)."""
CARD = "כרטיס אשראי"
BALANCE = "יתרה"
BANK_ACCOUNT = "חשבון בנק"
BANK_ACCOUNT_PREFIX = "Bank account: "
SIGNS = {
    (CARD, "Debit"): -1,
    (BALANCE, "Debit"): -1,
    (BALANCE, "Credit"): 1,
    (BANK_ACCOUNT, "Credit"): 1,
}
"""The sign of the amount, in the Bit balance's terms (bank-account rows as
received, see above), by (payment method, credit/debit). Other combinations
were never seen, and are rejected."""
AMOUNT = re.compile(r"\d+(\.\d{1,2})?")
DATE = re.compile(r"\d{2}\.\d{2}\.\d{2}")
DISCLAIMER = re.compile(r"\d+\. \S")
DISCLAIMER_COLUMN = HEADER.index("From/To")
"""The footer's disclaimer lines start in this column (and may span more, split
by their commas)."""


def parse(input_file):
    if input_file.suffix != ".csv":
        raise ParsingError("Not a .csv file")
    try:
        with input_file.open(encoding="utf-8-sig", newline="") as file:
            rows = [[cell.strip() for cell in row] for row in csv.reader(file)]
    except (UnicodeDecodeError, csv.Error) as exc:
        raise ParsingError(f"Failed to read .csv file: {exc}") from exc
    header = tuple(rows[0]) if rows else ()
    if header[: len(HEADER)] != HEADER or any(header[len(HEADER) :]):
        raise ParsingError(f"Expected header {HEADER}, got: {header}")
    try:
        footer_start = rows.index([], 1)
    except ValueError:
        raise ParsingError("Expected a blank row, then the disclaimer") from None
    _check_footer(rows[footer_start + 1 :], first_line_number=footer_start + 2)
    transactions = []
    for line_number, row in enumerate(rows[1:footer_start], start=2):
        try:
            transaction = _parse_row(row)
        except (ValueError, ParsingError) as exc:
            raise ParsingError(f"Line {line_number}: {exc}") from exc
        if transaction is not None:
            transactions.append(transaction)
    df = pl.DataFrame(transactions, schema=PARSED_SCHEMA, orient="row")
    return df


def _check_footer(rows, first_line_number):
    if not rows:
        raise ParsingError("Expected the disclaimer after the blank row")
    for number, row in enumerate(rows, start=1):
        line_number = first_line_number + number - 1
        leading = row[:DISCLAIMER_COLUMN]
        text = row[DISCLAIMER_COLUMN] if len(row) > DISCLAIMER_COLUMN else ""
        if (
            len(leading) < DISCLAIMER_COLUMN
            or any(leading)
            or not DISCLAIMER.match(text)
            or not text.startswith(f"{number}. ")
        ):
            raise ParsingError(
                f"Line {line_number}: expected disclaimer line {number}, got: {row}"
            )


def _parse_row(row):
    if len(row) < len(HEADER) or any(row[len(HEADER) :]):
        raise ParsingError(f"Expected {len(HEADER)} columns, got: {row}")
    fields = dict(zip(HEADER, row[: len(HEADER)], strict=True))
    # Checked on skipped rows too, as signs of the format
    amount = fields["Amount"]
    if not AMOUNT.fullmatch(amount) or float(amount) == 0:
        raise ParsingError(f"Expected a positive amount, got: {amount!r}")
    if fields["Fee Amount"] != "0":
        raise ParsingError(f"Expected no fee (0), got: {fields['Fee Amount']!r}")
    if not DATE.fullmatch(fields["Date"]):
        raise ParsingError(f"Expected a DD.MM.YY date, got: {fields['Date']!r}")
    date = datetime.datetime.strptime(fields["Date"], "%d.%m.%y").date()
    if fields["Credit/Debit"] not in ("Credit", "Debit"):
        raise ParsingError(f"Expected Credit or Debit, got: {fields['Credit/Debit']!r}")
    status = fields["Status"]
    if status in SKIPPED_STATUSES:
        return None
    if status != DONE:
        raise ParsingError(f"Unknown status: {status!r}")
    method = fields["Payment Method"]
    if not fields["From/To"]:
        raise ParsingError("Expected a counterparty (From/To)")
    sign = SIGNS.get((method, fields["Credit/Debit"]))
    if sign is None:
        raise ParsingError(
            f"Unknown payment method and credit/debit: "
            f"{method!r}, {fields['Credit/Debit']!r}"
        )
    return (SOURCE, date, sign * float(amount), _raw_description(fields, method))


def _raw_description(fields, method):
    parts = (fields["From/To"], fields["Description"])
    description = ": ".join(part for part in parts if part)
    if method == BANK_ACCOUNT:
        return BANK_ACCOUNT_PREFIX + description
    return description


register_parser("bit", parse)
