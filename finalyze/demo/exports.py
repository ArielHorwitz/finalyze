"""The demo household's raw files, in the formats of the exports they mimic.

Checking accounts and cards are Leumi-style .xls exports (HTML tables): checking
by year, a card by its monthly charge. Bit is the app's CSV export, by year.
The wallet, savings, the loan and the opening balances are hand-written CSVs,
as the owner would keep them.
"""

import csv
import datetime
import html
import itertools
from collections.abc import Iterable
from pathlib import Path

from finalyze.demo.household import BitDetails, Entry, Household
from finalyze.source.parsers import bit as bit_parser

OPENING = "Opening balance"
CHECKING_TITLE = "תנועות בחשבון"
CHECKING_COLUMNS = (
    "תאריך",
    "תאריך ערך",
    "תיאור",
    "אסמכתא",
    "בחובה",
    "בזכות",
    'היתרה בש"ח',
)
CARD_TITLE = 'עסקאות בש"ח במועד החיוב'
CARD_COLUMNS = (
    "תאריך העסקה",
    "שם בית העסק",
    "סכום העסקה",
    "סוג העסקה",
    "פרטים",
    "סכום חיוב",
)
CARD_TOTAL = 'סה"כ:'
CARD_DIGITS = {"maya": "4821", "eitan": "7306"}
ACCOUNT_NUMBERS = {"maya": "12-845-0391827", "eitan": "12-845-0478210"}
BIT_METHODS = {
    "card": bit_parser.CARD,
    "balance": bit_parser.BALANCE,
    "bank": bit_parser.BANK_ACCOUNT,
}
BIT_DISCLAIMER = (
    "1. הנתונים בקובץ זה מוצגים לצורכי מידע בלבד",
    "2. ייתכנו הבדלים בין הנתונים בקובץ לבין הנתונים בחשבון הבנק",
)
CSV_HEADER = ("source", "date", "amount", "raw_description")


def write_exports(household: Household, raw_dir: Path):
    """Write the raw files under `raw_dir`, a folder per account."""
    entries = household.entries
    for account in ("maya", "eitan"):
        account_dir = raw_dir.joinpath(account)
        account_dir.mkdir(parents=True)
        own = [entry for entry in entries if entry.account == account]
        opening = [entry for entry in own if entry.raw_description == OPENING]
        write_csv(account_dir.joinpath("opening-balances.csv"), opening)
        checking = [
            entry
            for entry in own
            if entry.source == "checking" and entry.raw_description != OPENING
        ]
        write_checking_files(account_dir, account, checking, opening_of(opening))
        card = [entry for entry in own if entry.source == "card"]
        write_card_files(account_dir, account, card)
        bit = [entry for entry in own if entry.source == "bit"]
        for year, year_entries in by_year(bit):
            write_bit_file(account_dir.joinpath(f"bit-{year}.csv"), year_entries)
    for account, file_name in (
        ("cash", "wallet.csv"),
        ("savings", "savings.csv"),
        ("loan", "car-loan.csv"),
    ):
        account_dir = raw_dir.joinpath(account)
        account_dir.mkdir(parents=True)
        own = [entry for entry in entries if entry.account == account]
        write_csv(account_dir.joinpath(file_name), own)


def opening_of(opening: list[Entry]) -> float:
    return sum(entry.amount for entry in opening if entry.source == "checking")


def by_year(entries: list[Entry]) -> Iterable[tuple[int, list[Entry]]]:
    ordered = sorted(entries, key=lambda entry: entry.date)
    for year, group in itertools.groupby(ordered, key=lambda entry: entry.date.year):
        yield year, list(group)


def write_csv(path: Path, entries: list[Entry]):
    with path.open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(CSV_HEADER)
        for entry in sorted(entries, key=lambda entry: entry.date):
            writer.writerow(
                (
                    entry.source,
                    entry.date.isoformat(),
                    f"{entry.amount:.2f}",
                    entry.raw_description,
                )
            )


def short_date(date: datetime.date) -> str:
    return date.strftime("%d/%m/%y")


def money(amount: float) -> str:
    return f"{amount:.2f}"


def html_row(cells, colspan=None) -> str:
    colspan_attribute = f' colspan="{colspan}"' if colspan else ""
    return (
        "<tr>"
        + "".join(
            f"<td{colspan_attribute}>{html.escape(str(cell))}</td>" for cell in cells
        )
        + "</tr>"
    )


def html_table(rows: list[str]) -> str:
    return "<table>" + "".join(rows) + "</table>"


def write_html(path: Path, tables: list[str]):
    head = '<head><meta charset="utf-8"></head>'
    body = "\n".join(tables)
    path.write_text(
        f'<html dir="rtl">{head}<body>{body}</body></html>', encoding="utf-8"
    )


def write_checking_files(
    account_dir: Path, account: str, entries: list[Entry], opening: float
):
    """A year per file, newest last, with the running balance."""
    balance = opening
    reference = 70_000 + len(account) * 1_000
    for year, year_entries in by_year(entries):
        # Within a day, banks list credits first
        year_entries.sort(key=lambda entry: (entry.date, -entry.amount))
        rows = [
            html_row([CHECKING_TITLE] + [""] * (len(CHECKING_COLUMNS) - 1)),
            html_row(CHECKING_COLUMNS),
        ]
        for entry in year_entries:
            balance += entry.amount
            reference += 37
            debit = money(-entry.amount) if entry.amount < 0 else ""
            credit = money(entry.amount) if entry.amount > 0 else ""
            date = short_date(entry.date)
            rows.append(
                html_row(
                    [
                        date,
                        date,
                        entry.raw_description,
                        reference,
                        debit,
                        credit,
                        money(balance),
                    ]
                )
            )
        first, last = year_entries[0].date, year_entries[-1].date
        tables = [
            html_table([html_row([f"חשבון {ACCOUNT_NUMBERS[account]}", "בנק לאומי"])]),
            html_table(
                [html_row([f"תנועות מתאריך {short_date(first)} עד {short_date(last)}"])]
            ),
            html_table(rows),
        ]
        write_html(account_dir.joinpath(f"leumi-checking-{year}.xls"), tables)


def write_card_files(account_dir: Path, account: str, entries: list[Entry]):
    """A file per monthly charge: a month's transactions, charged on the 10th of
    the next."""
    ordered = sorted(entries, key=lambda entry: entry.date)
    for (year, month), group in itertools.groupby(
        ordered, key=lambda entry: (entry.date.year, entry.date.month)
    ):
        group = list(group)
        charge_date = datetime.date(year + month // 12, month % 12 + 1, 10)
        rows = [
            html_row([CARD_TITLE], colspan=len(CARD_COLUMNS)),
            html_row(CARD_COLUMNS),
        ]
        for entry in group:
            charge = money(-entry.amount)
            kind = "זיכוי" if entry.amount > 0 else "רגילה"
            rows.append(
                html_row(
                    [
                        short_date(entry.date),
                        entry.raw_description,
                        charge,
                        kind,
                        "",
                        charge,
                    ]
                )
            )
        total = -sum(entry.amount for entry in group)
        rows.append(html_row(["", "", "", "", CARD_TOTAL, money(total)]))
        summary = html_table(
            [
                html_row(
                    [
                        f"כרטיס לאומי ויזה המסתיים ב-{CARD_DIGITS[account]}",
                        f"מועד חיוב: {charge_date.strftime('%d/%m/%Y')}",
                    ]
                )
            ]
        )
        file_name = f"leumi-card-{charge_date:%Y-%m}.xls"
        write_html(account_dir.joinpath(file_name), [summary, html_table(rows)])


def write_bit_file(path: Path, entries: list[Entry]):
    """Newest first, as the app lists them, then the disclaimer."""
    lines = [", ".join(bit_parser.HEADER)]
    for entry in sorted(entries, key=lambda entry: entry.date, reverse=True):
        lines.append(bit_line(entry.bit, abs(entry.amount), entry.date))
    lines.append("")
    column = bit_parser.DISCLAIMER_COLUMN
    lines.extend("," * column + line for line in BIT_DISCLAIMER)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


def bit_line(details: BitDetails, amount: float, date: datetime.date) -> str:
    cells = (
        details.status,
        details.memo,
        "0",
        f"{amount:.2f}".removesuffix(".00"),
        BIT_METHODS[details.method],
        "Credit" if details.credit else "Debit",
        details.counterparty,
        date.strftime("%d.%m.%y"),
        "",
    )
    return ", ".join(cells)
