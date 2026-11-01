"""Unit tests for source parsers, using small synthetic fixtures.

The Leumi fixtures are HTML tables saved with a .xls suffix, mimicking the
structure of the bank website exports as expected by the parser. All values are
invented.
"""

import datetime
import html

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from finalyze.source import ingest
from finalyze.source.identity import derive_ids
from finalyze.source.parsers import bit as bit_parser
from finalyze.source.parsers import csv as csv_parser
from finalyze.source.parsers.leumi import CardFormat, CheckingFormat
from finalyze.source.parsing import PARSED_SCHEMA, ParsingError, parse_file

CHECKING_HEADER = "תנועות בחשבון"
CHECKING_COLUMNS = ("תאריך", "תאריך ערך", "תיאור", "אסמכתא", "בחובה", "בזכות")
CARD_DESCRIPTION = "לאומי ויזה"
CARD_CHARGED_TITLE = 'עסקאות בש"ח במועד החיוב'
CARD_INFO_TITLE = 'עסקאות מחויבות בש"ח (לידיעה בלבד)'
CARD_UNCONFIRMED_TITLE = "עסקאות אחרונות שטרם נקלטו"
CARD_HEADERS = (
    "תאריך העסקה",
    "שם בית העסק",
    "סכום העסקה",
    "סוג העסקה",
    "פרטים",
    "סכום חיוב",
)
CARD_TOTALS_NAME = 'סה"כ:'
CARD_COLUMN_COUNT = len(CARD_HEADERS)


def html_row(cells, colspan=None):
    colspan_attribute = f' colspan="{colspan}"' if colspan else ""
    cells_html = "".join(
        f"<td{colspan_attribute}>{html.escape(str(cell))}</td>" for cell in cells
    )
    return f"<tr>{cells_html}</tr>"


def html_table(rows):
    return "<table>" + "".join(rows) + "</table>"


def write_html_tables(path, tables):
    body = "\n".join(tables)
    path.write_text(
        f'<html><head><meta charset="utf-8"></head><body>{body}</body></html>',
        encoding="utf-8",
    )
    return path


def filler_table(text):
    return html_table([html_row([text])])


def checking_table(transactions, *, header=CHECKING_HEADER, footnote=None):
    """Transactions are (date, description, debit, credit)."""
    rows = [
        html_row([header] + [""] * (len(CHECKING_COLUMNS) - 1)),
        html_row(CHECKING_COLUMNS),
    ]
    for reference, (date, description, debit, credit) in enumerate(transactions):
        rows.append(
            html_row([date, date, description, 1000 + reference, debit, credit])
        )
    if footnote is not None:
        rows.append(html_row([footnote] + [""] * (len(CHECKING_COLUMNS) - 1)))
    return html_table(rows)


def write_checking_file(path, transactions, **kwargs):
    tables = [
        filler_table("Account summary"),
        filler_table("Search options"),
        checking_table(transactions, **kwargs),
    ]
    return write_html_tables(path, tables)


def card_table(title, transactions, *, totals_name=CARD_TOTALS_NAME):
    """Transactions are (date, merchant, charge)."""
    rows = [
        html_row([title], colspan=CARD_COLUMN_COUNT),
        html_row(CARD_HEADERS),
    ]
    for date, merchant, charge in transactions:
        rows.append(html_row([date, merchant, charge, "רגילה", "", charge]))
    total = sum(charge for _date, _merchant, charge in transactions)
    rows.append(html_row(["", "", "", "", totals_name, total]))
    return html_table(rows)


def write_card_file(path, tables):
    return write_html_tables(path, [filler_table("Card summary"), *tables])


def expected_frame(rows):
    """Rows are (date, amount, raw description, source)."""
    return pl.DataFrame(
        {
            "date": [row[0] for row in rows],
            "amount": [float(row[1]) for row in rows],
            "raw_description": [row[2] for row in rows],
            "source": [row[3] for row in rows],
        },
        schema={
            key: PARSED_SCHEMA[key] for key in ("date", "amount", "raw_description")
        }
        | {"source": pl.String},
    )


CHECKING_TRANSACTIONS = [
    ("01/02/25", "Payroll - Example Corp", 0, 5000.00),
    ("03/02/25", CARD_DESCRIPTION, 1200.50, 0),
    ("05/02/25", "Electric Co", 210.30, 0),
]


# Leumi checking


def test_checking_parses_transactions(tmp_path):
    path = write_checking_file(tmp_path / "checking.xls", CHECKING_TRANSACTIONS)
    expected = expected_frame(
        [
            (datetime.date(2025, 2, 1), 5000.00, "Payroll - Example Corp", "checking"),
            (datetime.date(2025, 2, 3), -1200.50, CARD_DESCRIPTION, "checking"),
            (datetime.date(2025, 2, 5), -210.30, "Electric Co", "checking"),
        ]
    )
    assert_frame_equal(CheckingFormat.parse(path), expected)


def test_checking_four_digit_year(tmp_path):
    transactions = [("01/02/2025", "Payroll", 0, 5000.00)]
    path = write_checking_file(tmp_path / "checking.xls", transactions)
    assert CheckingFormat.parse(path)["date"].to_list() == [datetime.date(2025, 2, 1)]


def test_checking_footnotes(tmp_path):
    transactions = [
        ("01/02/25", "Payroll", 0, 5000.00),
        ("** 03/02/25", "Pending transfer", 100.00, 0),
    ]
    path = write_checking_file(
        tmp_path / "checking.xls",
        transactions,
        footnote="** Transactions marked with asterisks are pending",
    )
    parsed = CheckingFormat.parse(path)
    assert parsed["date"].to_list() == [
        datetime.date(2025, 2, 1),
        datetime.date(2025, 2, 3),
    ]
    assert parsed["raw_description"].to_list() == ["Payroll", "Pending transfer"]


def test_checking_blank_amount_cell_counts_as_zero(tmp_path):
    transactions = [
        ("01/02/25", "Payroll", "", 5000.00),
        ("03/02/25", "Electric Co", 210.30, ""),
    ]
    path = write_checking_file(tmp_path / "checking.xls", transactions)
    assert CheckingFormat.parse(path)["amount"].to_list() == [5000.00, -210.30]


def test_checking_rejects_blank_last_row(tmp_path):
    table = checking_table(CHECKING_TRANSACTIONS).replace(
        "</table>", html_row([""] * len(CHECKING_COLUMNS)) + "</table>"
    )
    path = write_html_tables(
        tmp_path / "checking.xls", [filler_table("a"), filler_table("b"), table]
    )
    with pytest.raises(ParsingError, match="without a date"):
        CheckingFormat.parse(path)


def test_checking_rejects_too_few_tables(tmp_path):
    path = write_html_tables(
        tmp_path / "checking.xls", [checking_table(CHECKING_TRANSACTIONS)]
    )
    with pytest.raises(ParsingError, match="Expected at least 3 tables"):
        CheckingFormat.parse(path)


def test_checking_rejects_too_few_columns(tmp_path):
    table = html_table([html_row([CHECKING_HEADER]), html_row([CHECKING_COLUMNS[0]])])
    path = write_html_tables(
        tmp_path / "checking.xls", [filler_table("a"), filler_table("b"), table]
    )
    with pytest.raises(ParsingError, match="Unexpected table shape"):
        CheckingFormat.parse(path)


def test_checking_rejects_wrong_header(tmp_path):
    path = write_checking_file(
        tmp_path / "checking.xls", CHECKING_TRANSACTIONS, header="Other header"
    )
    with pytest.raises(ParsingError, match="Expected header"):
        CheckingFormat.parse(path)


def test_checking_rejects_wrong_first_column(tmp_path):
    table = checking_table(CHECKING_TRANSACTIONS).replace(
        CHECKING_COLUMNS[0], "Other column", 1
    )
    path = write_html_tables(
        tmp_path / "checking.xls", [filler_table("a"), filler_table("b"), table]
    )
    with pytest.raises(ParsingError, match="Expected column name"):
        CheckingFormat.parse(path)


def test_checking_reads_only_third_table(tmp_path):
    tables = [
        filler_table("a"),
        checking_table(CHECKING_TRANSACTIONS),
        filler_table("c"),
    ]
    path = write_html_tables(tmp_path / "checking.xls", tables)
    with pytest.raises(ParsingError, match="Expected header"):
        CheckingFormat.parse(path)


def test_checking_rejects_non_xls(tmp_path):
    path = write_checking_file(tmp_path / "checking.html", CHECKING_TRANSACTIONS)
    with pytest.raises(ParsingError, match="Not a .xls file"):
        CheckingFormat.parse(path)


def test_checking_keeps_card_charges(tmp_path):
    # Ingestion rules handle them (e.g. a transfer to the card source)
    path = write_checking_file(tmp_path / "checking.xls", CHECKING_TRANSACTIONS)
    parsed = CheckingFormat.parse(path)
    assert parsed["raw_description"].to_list() == [
        "Payroll - Example Corp",
        CARD_DESCRIPTION,
        "Electric Co",
    ]


# Leumi card


CARD_CHARGED = [("03/01/25", "SuperMart", 120.40), ("07/01/25", "Roadstar Fuel", 60)]
CARD_INFO = [("15/12/24", "TechnoStore", 350.00)]


def test_card_parses_titled_tables(tmp_path):
    path = write_card_file(
        tmp_path / "card.xls",
        [
            card_table(CARD_UNCONFIRMED_TITLE, [("20/01/25", "Pending Shop", 9.99)]),
            card_table(CARD_CHARGED_TITLE, CARD_CHARGED),
            filler_table("Unrelated table"),
            card_table(CARD_INFO_TITLE, CARD_INFO),
        ],
    )
    expected = expected_frame(
        [
            (datetime.date(2025, 1, 3), -120.40, "SuperMart", "card"),
            (datetime.date(2025, 1, 7), -60.00, "Roadstar Fuel", "card"),
            (datetime.date(2024, 12, 15), -350.00, "TechnoStore", "card"),
        ]
    )
    assert_frame_equal(CardFormat.parse(path), expected)


def test_card_requires_a_titled_table(tmp_path):
    path = write_card_file(
        tmp_path / "card.xls",
        [card_table(CARD_UNCONFIRMED_TITLE, CARD_CHARGED)],
    )
    with pytest.raises(ParsingError, match="No valid tables found"):
        CardFormat.parse(path)


def test_card_rejects_title_not_spanning_row(tmp_path):
    table = card_table(CARD_CHARGED_TITLE, CARD_CHARGED).replace(
        f' colspan="{CARD_COLUMN_COUNT}"', "", 1
    )
    path = write_card_file(tmp_path / "card.xls", [table])
    with pytest.raises(ParsingError, match="Expected title"):
        CardFormat.parse(path)


def test_card_rejects_wrong_headers(tmp_path):
    table = card_table(CARD_CHARGED_TITLE, CARD_CHARGED).replace(
        CARD_HEADERS[3], "Other header", 1
    )
    path = write_card_file(tmp_path / "card.xls", [table])
    with pytest.raises(ParsingError, match="Expected header"):
        CardFormat.parse(path)


def test_card_rejects_missing_totals_row(tmp_path):
    table = card_table(CARD_CHARGED_TITLE, CARD_CHARGED, totals_name="Other")
    path = write_card_file(tmp_path / "card.xls", [table])
    with pytest.raises(ParsingError, match="in last row"):
        CardFormat.parse(path)


def test_card_rejects_non_empty_totals_row(tmp_path):
    table = card_table(CARD_CHARGED_TITLE, CARD_CHARGED).replace(
        "<tr><td></td>", "<tr><td>1</td>", 1
    )
    path = write_card_file(tmp_path / "card.xls", [table])
    with pytest.raises(ParsingError, match="Expected nan"):
        CardFormat.parse(path)


def test_card_rejects_too_few_columns(tmp_path):
    table = html_table([html_row([CARD_CHARGED_TITLE]), html_row(CARD_HEADERS[:1])])
    path = write_card_file(tmp_path / "card.xls", [table])
    with pytest.raises(ParsingError, match="Unexpected table shape"):
        CardFormat.parse(path)


def test_card_rejects_non_xls(tmp_path):
    path = write_card_file(
        tmp_path / "card.html", [card_table(CARD_CHARGED_TITLE, CARD_CHARGED)]
    )
    with pytest.raises(ParsingError, match="Not a .xls file"):
        CardFormat.parse(path)


# CSV


def write_csv(path, text):
    path.write_text(text)
    return path


# Hand-written files may keep the header from before raw descriptions were named
@pytest.mark.parametrize("header", ["raw_description", "description"])
def test_csv_parses_file(tmp_path, header):
    path = write_csv(
        tmp_path / "account.csv",
        f"source,date,amount,{header}\n"
        "checking,2025-01-01,100.50,Payroll\n"
        "card,2025-01-03,-20,SuperMart\n",
    )
    expected = pl.DataFrame(
        {
            "source": ["checking", "card"],
            "date": [datetime.date(2025, 1, 1), datetime.date(2025, 1, 3)],
            "amount": [100.50, -20.0],
            "raw_description": ["Payroll", "SuperMart"],
        },
        schema=PARSED_SCHEMA,
    )
    assert_frame_equal(csv_parser.parse(path), expected)


def test_csv_reads_columns_by_header_name(tmp_path):
    path = write_csv(
        tmp_path / "account.csv",
        "raw_description,source,date,amount\nPayroll,checking,2025-01-01,100.50\n",
    )
    assert csv_parser.parse(path).row(0) == (
        "checking",
        datetime.date(2025, 1, 1),
        100.50,
        "Payroll",
    )


def test_csv_rejects_missing_columns(tmp_path):
    path = write_csv(
        tmp_path / "account.csv", "source,date,amount\nchecking,2025-01-01,100.50\n"
    )
    with pytest.raises(
        ParsingError, match=r"missing: \['raw_description'\], extra: \[\]"
    ):
        csv_parser.parse(path)


def test_csv_rejects_extra_columns(tmp_path):
    path = write_csv(
        tmp_path / "account.csv",
        "source,date,amount,raw_description,extra\nchecking,2025-01-01,1,Payroll,x\n",
    )
    with pytest.raises(ParsingError, match=r"missing: \[\], extra: \['extra'\]"):
        csv_parser.parse(path)


def test_csv_rejects_both_description_headers(tmp_path):
    path = write_csv(
        tmp_path / "account.csv",
        "source,date,amount,raw_description,description\n"
        "checking,2025-01-01,1,Payroll,Payroll\n",
    )
    with pytest.raises(ParsingError, match=r"extra: \['description'\]"):
        csv_parser.parse(path)


def test_csv_header_does_not_change_ids(tmp_path):
    """IDs hash the values, not the column names (pinned as in test_identity)."""
    ids_by_header = {}
    for header in ("raw_description", "description"):
        path = write_csv(
            tmp_path / f"{header}.csv",
            f"source,date,amount,{header}\ncard,2025-01-10,-42.10,SuperMart\n",
        )
        parsed = csv_parser.parse(path).with_columns(account=pl.lit("alice"))
        ids_by_header[header] = derive_ids(parsed)["id"].to_list()
    assert ids_by_header["raw_description"] == ids_by_header["description"]
    assert ids_by_header["description"] == ["b895996bbfee249a"]


@pytest.mark.parametrize(
    "row",
    ["checking,2025-01-01,lots,Payroll", "checking,01/01/2025,1,Payroll"],
)
def test_csv_rejects_invalid_values(tmp_path, row):
    path = write_csv(
        tmp_path / "account.csv", f"source,date,amount,raw_description\n{row}\n"
    )
    with pytest.raises(ParsingError, match="Invalid values"):
        csv_parser.parse(path)


def test_csv_empty_file_has_no_transactions(tmp_path):
    path = write_csv(tmp_path / "account.csv", "")
    parsed = csv_parser.parse(path)
    assert parsed.height == 0
    assert dict(parsed.schema) == PARSED_SCHEMA


def test_csv_rejects_blank_file(tmp_path):
    path = write_csv(tmp_path / "account.csv", "\n")
    with pytest.raises(ParsingError):
        csv_parser.parse(path)


def test_csv_rejects_non_csv(tmp_path):
    path = write_csv(tmp_path / "account.txt", "source,date,amount,raw_description\n")
    with pytest.raises(ParsingError, match="Not a .csv file"):
        csv_parser.parse(path)


# Bit

BIT_HEADER = (
    "Status, Description, Fee Amount, Amount, Payment Method, Credit/Debit,"
    " From/To, Date"
)
BIT_FOOTER = (
    "\n"
    ",,,,,,1. A disclaimer, with commas, spanning more columns, than the data,\n"
    ",,,,,,2. Another disclaimer line\n"
)


def bit_row(status, memo, amount, method, direction, counterparty, date, fee="0"):
    return (
        f"{status}, {memo}, {fee}, {amount}, {method}, {direction},"
        f" {counterparty}, {date}, \n"
    )


def write_bit_file(path, rows, *, footer=BIT_FOOTER):
    # The export starts with a byte order mark
    path.write_text(BIT_HEADER + "\n" + "".join(rows) + footer, encoding="utf-8-sig")
    return path


def test_bit_parses_transactions(tmp_path):
    path = write_bit_file(
        tmp_path / "bit.csv",
        [
            bit_row(
                "Done",
                "Birthday gift",
                "120",
                bit_parser.CARD,
                "Debit",
                "Dana Levi",
                "03.01.25",
            ),
            bit_row(
                "Done",
                "Pizza",
                "45.50",
                bit_parser.BALANCE,
                "Debit",
                "Pizza Place",
                "05.01.25",
            ),
            bit_row(
                "Done",
                "Congrats",
                "200",
                bit_parser.BALANCE,
                "Credit",
                "Avi Cohen",
                "06.01.25",
            ),
        ],
    )
    expected = pl.DataFrame(
        {
            "source": ["bit"] * 3,
            "date": [datetime.date(2025, 1, day) for day in (3, 5, 6)],
            "amount": [-120.0, -45.50, 200.0],
            "raw_description": [
                "Dana Levi: Birthday gift",
                "Pizza Place: Pizza",
                "Avi Cohen: Congrats",
            ],
        },
        schema=PARSED_SCHEMA,
    )
    assert_frame_equal(bit_parser.parse(path), expected)


def test_bit_skips_rows_that_moved_no_money(tmp_path):
    path = write_bit_file(
        tmp_path / "bit.csv",
        [
            bit_row(
                "Expired", "Groceries", "90", bit_parser.CARD, "Debit", "", "09.01.25"
            ),
            bit_row("Expired", "Lunch", "40", "", "Credit", "Avi Cohen", "10.01.25"),
            bit_row(
                "You declined", "Rent", "900", "", "Credit", "Dana Levi", "11.01.25"
            ),
            bit_row(
                "Declined",
                "Movie",
                "35",
                bit_parser.BALANCE,
                "Debit",
                "Avi Cohen",
                "12.01.25",
            ),
            bit_row(
                "Done", "Lunch", "40", bit_parser.CARD, "Debit", "Avi Cohen", "10.01.25"
            ),
        ],
    )
    assert bit_parser.parse(path)["amount"].to_list() == [-40.0]


def test_bit_emits_bank_account_rows_as_received(tmp_path):
    # A withdrawal (the user's own name) and a payment received into the bank
    # look the same: the ingestion config tells them apart
    path = write_bit_file(
        tmp_path / "bit.csv",
        [
            bit_row(
                "Done",
                "",
                "154.50",
                bit_parser.BANK_ACCOUNT,
                "Credit",
                "Me Myself",
                "07.01.25",
            ),
            bit_row(
                "Done",
                "Rent share",
                "900",
                bit_parser.BANK_ACCOUNT,
                "Credit",
                "Dana Levi",
                "08.01.25",
            ),
        ],
    )
    parsed = bit_parser.parse(path)
    assert parsed["amount"].to_list() == [154.50, 900.0]
    assert parsed["raw_description"].to_list() == [
        "Bank account: Me Myself",
        "Bank account: Dana Levi: Rent share",
    ]


def test_bit_description_without_memo_is_the_counterparty(tmp_path):
    path = write_bit_file(
        tmp_path / "bit.csv",
        [bit_row("Done", "", "40", bit_parser.CARD, "Debit", "Avi Cohen", "10.01.25")],
    )
    assert bit_parser.parse(path)["raw_description"].to_list() == ["Avi Cohen"]


LUNCH = bit_row("Done", "Lunch", "40", bit_parser.CARD, "Debit", "Avi", "10.01.25")


@pytest.mark.parametrize(
    ("footer", "error"),
    [
        ("", "Expected a blank row"),
        ("\n", "Expected the disclaimer"),
        ("\n,,,,,,2. Out of order\n", "expected disclaimer line 1"),
        ("\n,,,,,,1. A disclaimer\n" + LUNCH, "expected disclaimer line 2"),
    ],
)
def test_bit_requires_the_footer(tmp_path, footer, error):
    path = write_bit_file(tmp_path / "bit.csv", [LUNCH], footer=footer)
    with pytest.raises(ParsingError, match=error):
        bit_parser.parse(path)


def test_bit_rejects_a_blank_row_between_transactions(tmp_path):
    # Not the footer: the rows after it would be lost
    path = write_bit_file(tmp_path / "bit.csv", [LUNCH, "\n", LUNCH])
    with pytest.raises(ParsingError, match="expected disclaimer line 1"):
        bit_parser.parse(path)


def test_bit_empty_export(tmp_path):
    path = write_bit_file(tmp_path / "bit.csv", [])
    assert bit_parser.parse(path).is_empty()


@pytest.mark.parametrize(
    ("row", "error"),
    [
        (
            bit_row(
                "Pending", "Lunch", "40", bit_parser.CARD, "Debit", "Avi", "10.01.25"
            ),
            "Unknown status",
        ),
        (
            bit_row("Cancelled", "Lunch", "40", "", "Debit", "Avi", "10.01.25"),
            "Unknown status",
        ),
        (
            bit_row("Done", "Lunch", "40", "", "Debit", "Avi", "10.01.25"),
            "Unknown payment method",
        ),
        (
            bit_row(
                "Done", "", "40", bit_parser.BANK_ACCOUNT, "Debit", "Avi", "10.01.25"
            ),
            "Unknown payment method and credit/debit",
        ),
        (
            bit_row(
                "Done", "", "40", bit_parser.BANK_ACCOUNT, "Credit", "", "10.01.25"
            ),
            "Expected a counterparty",
        ),
        (
            bit_row(
                "Done", "Lunch", "40", bit_parser.CARD, "Credit", "Avi", "10.01.25"
            ),
            "Unknown payment method",
        ),
        (
            bit_row(
                "Done",
                "Lunch",
                "40",
                bit_parser.CARD,
                "Debit",
                "Avi",
                "10.01.25",
                fee="2",
            ),
            "Expected no fee",
        ),
        (
            bit_row(
                "Done", "Lunch", "forty", bit_parser.CARD, "Debit", "Avi", "10.01.25"
            ),
            "Line 2",
        ),
        (
            bit_row(
                "Done", "Lunch", "40", bit_parser.CARD, "Debit", "Avi", "2025-01-10"
            ),
            "Line 2",
        ),
        ("Done, Lunch, 0, 40\n", "Expected 8 columns"),
        (
            bit_row(
                "Done", "Lunch", "-40", bit_parser.CARD, "Debit", "Avi", "10.01.25"
            ),
            "positive amount",
        ),
        (
            bit_row("Done", "Lunch", "0", bit_parser.CARD, "Debit", "Avi", "10.01.25"),
            "positive amount",
        ),
        (
            bit_row(
                "Done", "Lunch", "1,040", bit_parser.CARD, "Debit", "Avi", "10.01.25"
            ),
            "Expected 8 columns",
        ),
        (
            bit_row(
                "Done", "Lunch", "40.123", bit_parser.CARD, "Debit", "Avi", "10.01.25"
            ),
            "positive amount",
        ),
        (
            bit_row("Done", "Lunch", "40", bit_parser.CARD, "Debit", "Avi", "1.1.25"),
            "DD.MM.YY",
        ),
        (
            bit_row("Done", "Lunch", "40", bit_parser.CARD, "Debit", "", "10.01.25"),
            "counterparty",
        ),
        (
            bit_row("Done", "Lunch", "40", bit_parser.CARD, "Out", "Avi", "10.01.25"),
            "Credit or Debit",
        ),
        # Skipped rows are checked too
        (
            bit_row("Expired", "Lunch", "-40", "", "Credit", "", "10.01.25"),
            "positive amount",
        ),
    ],
)
def test_bit_rejects_unexpected_rows(tmp_path, row, error):
    path = write_bit_file(tmp_path / "bit.csv", [row])
    with pytest.raises(ParsingError, match=error):
        bit_parser.parse(path)


def test_bit_rejects_other_csv(tmp_path):
    path = write_csv(
        tmp_path / "account.csv",
        "source,date,amount,raw_description\ncash,2025-01-01,-5,Coffee\n",
    )
    with pytest.raises(ParsingError, match="Expected header"):
        bit_parser.parse(path)


def test_bit_rejects_non_csv(tmp_path):
    path = write_bit_file(tmp_path / "bit.txt", [])
    with pytest.raises(ParsingError, match="Not a .csv file"):
        bit_parser.parse(path)


# Dispatch across registered parsers


def test_parse_file_dispatches_by_format(tmp_path):
    checking_path = write_checking_file(
        tmp_path / "checking.xls", CHECKING_TRANSACTIONS
    )
    card_path = write_card_file(
        tmp_path / "card.xls",
        [
            card_table(CARD_UNCONFIRMED_TITLE, CARD_INFO),
            card_table(CARD_CHARGED_TITLE, CARD_CHARGED),
        ],
    )
    csv_path = write_csv(
        tmp_path / "account.csv",
        "source,date,amount,raw_description\ncash,2025-01-01,-5,Coffee\n",
    )
    assert parse_file(checking_path)["source"].unique().to_list() == ["checking"]
    assert parse_file(card_path)["source"].unique().to_list() == ["card"]
    assert parse_file(csv_path)["source"].to_list() == ["cash"]
    bit_path = write_bit_file(
        tmp_path / "bit.csv",
        [bit_row("Done", "Lunch", "40", bit_parser.CARD, "Debit", "Avi", "10.01.25")],
    )
    assert parse_file(bit_path)["source"].to_list() == ["bit"]


def test_parse_file_reports_all_failures(tmp_path):
    path = write_csv(tmp_path / "unknown.txt", "nothing")
    with pytest.raises(ValueError, match="Failed to parse") as exc_info:
        parse_file(path)
    message = str(exc_info.value)
    for parser_name in ("bit", "csv", "leumi checking", "leumi card"):
        assert f"{parser_name}:" in message


def test_parse_file_falls_through_to_card_parser(tmp_path):
    # A card export with fewer than three tables fails the checking parser
    path = write_card_file(
        tmp_path / "card.xls", [card_table(CARD_CHARGED_TITLE, CARD_CHARGED)]
    )
    assert parse_file(path)["source"].unique().to_list() == ["card"]


@pytest.mark.parametrize("parser", [CheckingFormat.parse, CardFormat.parse])
def test_leumi_rejects_non_html_xls(tmp_path, parser):
    path = tmp_path / "binary.xls"
    path.write_bytes(bytes(range(256)))
    with pytest.raises(ParsingError, match="Failed to read HTML tables"):
        parser(path)


def test_parser_modules_import_in_sorted_order(monkeypatch):
    imported = []
    monkeypatch.setattr(ingest.importlib, "import_module", imported.append)
    ingest._import_parser_modules()
    assert imported == sorted(imported)
    assert "finalyze.source.parsers.leumi" in imported
