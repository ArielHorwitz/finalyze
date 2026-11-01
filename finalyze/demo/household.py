"""The demo household's story: two years of its money, as its banks and apps
export it, and how it was annotated. Everything is invented.

Maya and Eitan Rosen live in a rented flat with their son Yuval (who goes to
daycare) and their dog Pita. They bank separately, at the same bank, and treat
their money as one household: Eitan sends Maya a share of the rent every
month. Each has a checking account and a credit card, Maya pays friends with
the Bit app, and there is a cash wallet, a savings account, and from the new
car's purchase, a car loan.

Over the two years: Maya gets a raise, the rent goes up at the lease's renewal,
the old car's gearbox fails and the family buys a hybrid (selling the old car,
taking money out of savings and a loan from the dealer), the fridge dies in a
heatwave, and they fly abroad in the first summer and for a winter weekend.
Electricity and water peak in the summer, daycare closes in August, and the
holidays (autumn and spring) and the wedding season show in groceries and
gifts.

The generator is deterministic for a given end date: two years of complete
months and the current one, up to it. The story's events are at fixed months
from the start, the seasonal ones (the trips, the heatwave) in their season.
"""

import dataclasses
import datetime
import itertools
import math
import random
from typing import Optional

from finalyze.source.parsers import bit as bit_parser

RANDOM_SEED = 1729
MONTH_COUNT = 25
"""Two years of complete months, and the current one."""
UNTAGGED_COUNT = 12
"""The current month's last transactions are left for annotating, as if just
ingested (at most this many: the complete months are all tagged)."""
UNTAGGED_MINIMUM = 4
"""Early in a month, purchases on the end date make up this many."""

MAYA = "maya"
EITAN = "eitan"
CASH = "cash"
SAVINGS = "savings"
LOAN = "loan"
MAYA_NAME = "מאיה רוזן"
CARD_PAYMENT = "לאומי ויזה"
"""How the monthly card charge shows in checking."""
CARD_BIT = "BIT"
"""How a Bit payment paid by card shows on the card."""
CHECKING_BIT = "BIT העברה לחשבון"
"""How money from Bit shows in checking."""
CASH_WITHDRAWAL = "משיכת מזומן - כספומט"
SAVINGS_DEPOSIT = "הפקדה לחיסכון"
LOAN_REPAYMENT = "החזר הלוואה 0548217"
OWNER = "owner"
AGENT = "penny"

# Months of the story, counted from the first month
BIT_EXPORT_START = 7
"""The Bit export goes back this far: earlier Bit payments are only card charges."""
CLOUD_STORAGE_START = 9
OLD_CAR_GEARBOX = 10
RENT_RISE = 11
RAISE = 14
CAR_SWAP = 15
STREAMING_PRICE_RISE = 16
DENTAL_CROWN = 20
LAPTOP = 21
TAX_REFUND = 23
WEEKENDS_AWAY = (6, 18)
SECOND_HAND_SALES = {
    12: (450.0, "עגלה", "Sold Yuval's old stroller"),
    22: (380.0, "מיטת תינוק", "Sold the crib"),
}
RETURNED_JACKET = 17

FRIENDS = ("רוני אלמוג", "דנה כץ", "עופר לוי", "יעל שגיא", "נועם ברק", "שירה גל")
GRANDMA = "סבתא רינה"
GROCERY_BRANCHES = (
    "סופר גפן סניף 17",
    "סופר גפן - הדר",
    "סופר גפן כפר יונה",
    "סופר גפן אונליין",
)
SMALL_GROCERS = ("FRESHWAY EXPRESS 041", "FRESHWAY MARKET 12", "פירות וירקות האחים")
CAFES = ("קפה נחת", 'קפה תמרה בע"מ', "LA PAUSE COFFEE")
RESTAURANTS = (
    "פיצה נאפולי שלנו",
    "BURGERS & CO TLV",
    "מסעדת הגפן הירוקה",
    "סושי-לי רמת השרון",
    "חומוס אליהו ובניו",
)
DELIVERIES = (
    "FOODRUSH*SUSHI LI",
    "FOODRUSH*PIZZA NAPOLI",
    "FOODRUSH*HUMMUS ELIYA",
    "FOODRUSH*THAI BOX",
)
FUEL_STATIONS = ("דלקן 24 - צומת השרון", "דלקן 24 - כביש 4", "FUELPOINT 117")
VACATION_RESTAURANTS = (
    "RESTAURANTE O FAROL",
    "TASCA DO BAIRRO",
    "PASTELARIA ESTRELA",
    "CERVEJARIA DO CAIS",
)

DINING_NOTES = (
    "Date night",
    "Lunch with Ofir",
    "With Dana and Roni",
    "Yuval's first restaurant without a meltdown",
    "Work lunch, not reimbursed",
)
GROCERY_NOTES = (
    "Shabbat dinner for 10",
    "Stocking up before the holiday",
    "Party supplies for Yuval's birthday",
    "Hosting Eitan's parents",
)
PARKING_NOTES = ("Visiting Grandpa at the hospital", "Meeting in Tel Aviv")
KIDS_CLOTHES_NOTES = (
    "Winter coat for Yuval",
    "Sandals, he outgrew the old ones",
    "Pyjamas",
    "Rain boots",
)
TOYS_NOTES = ("Lego for the rainy days", "Bath toys", "Puzzle for the trip")
VET_NOTES = (
    "Pita's yearly vaccines",
    "Ear infection, again",
    "Check-up, Pita is fine",
    "Ate something in the park",
)
PHARMACY_NOTES = ("Antibiotics for Yuval", "Sunscreen and vitamins", "Allergy pills")
DOCTOR_NOTES = (
    "Pediatrician, Yuval's cough",
    "Blood tests",
    "Dermatologist",
    "Eye test for Maya",
)
CLOTHES_NOTES = ("Shoes for Eitan", "End of season sale", "Dress for the wedding")
HOME_NOTES = (
    "Bedding",
    "Plants for the balcony",
    "Kitchen stuff",
    "Storage boxes for the closet",
)
ONLINE_NOTES = ("Phone case", "Bike lights", "Charger cables", "Yoga mat")
BIRTHDAY_NOTES = (
    "Roni's 40th, the group gift",
    "Shira's birthday",
    "Noam's birthday, with Ofir",
    "Daycare friend's birthday party",
)
GIFT_MEMOS = (
    ("מתנה ליום הולדת", "Birthday gift"),
    ("יום הולדת שמח!", "Birthday gift"),
    ("מתנה מכולנו", "Group gift, our share"),
)
DINNER_SHARE_MEMOS = ("ארוחה אצל דנה", "פיצה ביום שישי", "על המסעדה")


@dataclasses.dataclass(frozen=True)
class Note:
    """How the transaction was annotated (or nothing, if a rule tags it)."""

    tag: str
    subtag: str
    description: Optional[str] = None
    kind: Optional[str] = None
    """Set on the transaction, over its tag's."""
    split: tuple[tuple[float, str, str, Optional[str]], ...] = ()
    """Parts (amount, tag, subtag, description) instead of the tag."""
    author: str = OWNER
    needs_review: bool = False


@dataclasses.dataclass(frozen=True)
class BitDetails:
    """A row of the Bit export, beyond its amount and date."""

    counterparty: str
    memo: str
    method: str
    """The payment method: "card", "balance" or "bank"."""
    credit: bool
    status: str = "Done"


@dataclasses.dataclass(frozen=True)
class Entry:
    """A row of a raw file, as its parser reads it."""

    account: str
    source: str
    date: datetime.date
    amount: float
    """Signed, as parsed: positive is money in."""
    raw_description: str
    note: Optional[Note] = None
    bit: Optional[BitDetails] = None

    @property
    def parsed(self) -> bool:
        """The parser emits it (Bit skips the rows that moved no money)."""
        return self.bit is None or self.bit.status == "Done"


@dataclasses.dataclass(frozen=True)
class Household:
    entries: list[Entry]
    end_date: datetime.date
    opening_date: datetime.date
    bit_export_start: datetime.date
    orphan: Entry
    """Annotated, but gone from the raw files (an order that was cancelled)."""
    overridden: tuple[Entry, Note]
    """An annotation of a transaction a rule tags anyway."""
    unannotated: frozenset[int]
    """Indices of noted entries left for annotating (the last ones)."""
    described_only: tuple[int, str]
    """The index of one of them, only described (by an annotation without
    parts), and its description."""


@dataclasses.dataclass(frozen=True)
class Timeline:
    """The months of the story's seasonal events (indices from the first)."""

    summer_vacation: int
    """A July, late enough to book the flights months before."""

    @property
    def flights_booked(self) -> int:
        return self.summer_vacation - 4

    @property
    def fridge_dies(self) -> int:
        """In the August heatwave."""
        return self.summer_vacation + 1

    @property
    def winter_trip(self) -> int:
        """The January after."""
        return self.summer_vacation + 6


class Month:
    def __init__(
        self,
        rng: random.Random,
        index: int,
        year: int,
        month: int,
        school_years: int,
        timeline: Timeline,
    ):
        self.rng = rng
        self.index = index
        self.year = year
        self.month = month
        self.school_years = school_years
        """Septembers since the first month (a new school year)."""
        self.timeline = timeline

    def day(self, day_of_month: int) -> datetime.date:
        last_day = (
            datetime.date(self.year + self.month // 12, self.month % 12 + 1, 1)
            - datetime.timedelta(days=1)
        ).day
        return datetime.date(self.year, self.month, min(day_of_month, last_day))

    def random_day(self, first: int = 1, last: int = 28) -> datetime.date:
        return self.day(self.rng.randint(first, last))

    def amount(self, low: float, high: float) -> float:
        return round(self.rng.uniform(low, high), 2)

    def chance(self, probability: float) -> bool:
        return self.rng.random() < probability

    def note(
        self,
        tag: str,
        subtag: str,
        descriptions: tuple[str, ...] = (),
        probability: float = 0.3,
        author: str = OWNER,
    ) -> Note:
        """A note, with one of the descriptions some of the time."""
        description = None
        if descriptions and self.chance(probability):
            description = self.rng.choice(descriptions)
        return Note(tag, subtag, description, author=author)

    @property
    def is_holiday_season(self) -> bool:
        """The autumn holidays and Passover."""
        return self.month in (4, 9, 10)

    @property
    def is_wedding_season(self) -> bool:
        return self.month in (5, 6, 7, 8, 9, 10)

    @property
    def summer_factor(self) -> float:
        """1 in mid-summer, 0 in mid-winter."""
        return (1 - math.cos((self.month - 1) / 6 * math.pi)) / 2


class Book:
    """Collects the entries of the household's raw files."""

    def __init__(self):
        self.entries: list[Entry] = []

    def checking(self, owner, date, amount, text, note=None):
        self.entries.append(Entry(owner, "checking", date, amount, text, note))

    def card(self, owner, date, amount, text, note=None):
        self.entries.append(Entry(owner, "card", date, amount, text, note))

    def wallet(self, date, amount, text, note=None):
        self.entries.append(Entry(CASH, "wallet", date, amount, text, note))

    def savings(self, date, amount, text, note=None):
        self.entries.append(Entry(SAVINGS, "savings", date, amount, text, note))

    def loan(self, date, amount, text, note=None):
        self.entries.append(Entry(LOAN, "car", date, amount, text, note))

    def bit(self, date, amount, details: BitDetails, note=None):
        description = ": ".join(
            part for part in (details.counterparty, details.memo) if part
        )
        if details.method == "bank":
            description = bit_parser.BANK_ACCOUNT_PREFIX + description
        self.entries.append(
            Entry(MAYA, "bit", date, amount, description, note, details)
        )


def generate(end_date: datetime.date) -> Household:
    rng = random.Random(RANDOM_SEED)
    book = Book()
    calendar = month_sequence(end_date)
    timeline = Timeline(
        summer_vacation=next(
            index
            for index, (_year, month) in enumerate(calendar)
            if month == 7 and index >= 5
        )
    )
    months = []
    school_years = 0
    for index, (year, month) in enumerate(calendar):
        if month == 9 and index > 0:
            school_years += 1
        months.append(Month(rng, index, year, month, school_years, timeline))
    opening_date = months[0].day(1)
    add_opening_balances(book, opening_date)
    bit_balance = 0.0
    for month in months:
        add_income(book, month)
        add_housing(book, month)
        add_food(book, month)
        add_car(book, month)
        add_family(book, month)
        add_health_and_shopping(book, month)
        add_leisure(book, month)
        add_cash(book, month)
        add_savings(book, month)
        add_events(book, month)
        bit_balance = add_bit(book, month, bit_balance)
    add_cash_count(book, months[-3])
    add_splits(book, months)
    add_review_requests(book, months)
    add_card_payments(book, months)
    entries = sorted_entries(book, end_date)
    if (shortfall := UNTAGGED_MINIMUM - len(annotatable(entries, end_date))) > 0:
        add_todays_purchases(book, end_date, shortfall)
        entries = sorted_entries(book, end_date)
    unannotated = frozenset(annotatable(entries, end_date)[-UNTAGGED_COUNT:])
    # Preferably one of Eitan's: Maya asks what it was
    described_only = min(
        unannotated, key=lambda index: (entries[index].account != EITAN, index)
    )
    return Household(
        entries=entries,
        end_date=end_date,
        opening_date=opening_date,
        bit_export_start=months[BIT_EXPORT_START].day(1),
        orphan=orphan_entry(months),
        overridden=overridden_annotation(entries),
        unannotated=unannotated,
        described_only=(described_only, "What was this? Ask Eitan"),
    )


def sorted_entries(book: Book, end_date: datetime.date) -> list[Entry]:
    return sorted(
        (entry for entry in book.entries if entry.date <= end_date),
        key=lambda entry: (entry.date, entry.account, entry.source),
    )


def annotatable(entries: list[Entry], end_date: datetime.date) -> list[int]:
    """Indices of the current month's entries that could be left for annotating
    (noted, parsed and not flagged for review)."""
    month_start = end_date.replace(day=1)
    return [
        index
        for index, entry in enumerate(entries)
        if entry.date >= month_start
        and entry.note is not None
        and entry.parsed
        and not entry.note.needs_review
    ]


def add_todays_purchases(book: Book, end_date: datetime.date, count: int):
    """Everyday purchases on the end date, for the untagged rows of a month that
    has barely started."""
    purchases = (
        (EITAN, -412.60, GROCERY_BRANCHES[0], Note("food", "groceries")),
        (MAYA, -18.00, CAFES[0], Note("food", "coffee")),
        (EITAN, -96.00, DELIVERIES[2], Note("food", "delivery", author=AGENT)),
        (MAYA, -64.30, SMALL_GROCERS[1], Note("food", "groceries")),
    )
    for owner, amount, text, note in purchases[:count]:
        book.card(owner, end_date, amount, text, note)


def month_sequence(end_date: datetime.date) -> list[tuple[int, int]]:
    year, month = end_date.year, end_date.month
    months = []
    for _ in range(MONTH_COUNT):
        months.append((year, month))
        year, month = (year - 1, 12) if month == 1 else (year, month - 1)
    return list(reversed(months))


def add_opening_balances(book: Book, date: datetime.date):
    opening = "Opening balance"
    book.checking(MAYA, date, 8_420.30, opening)
    book.checking(EITAN, date, 5_870.55, opening)
    book.wallet(date, 320.00, opening)
    book.savings(date, 52_000.00, opening)


def add_income(book: Book, month: Month):
    maya_salary = 15_210.00 if month.index >= RAISE else 13_960.00
    book.checking(MAYA, month.day(9), maya_salary, 'משכורת - אורבית תוכנה בע"מ')
    # With overtime, more in the busy months before the summer and the year's end
    overtime = month.amount(0, 400) + (650 if month.month in (5, 6, 11, 12) else 0)
    eitan_salary = round(11_640.00 + overtime, 2)
    book.checking(EITAN, month.day(10), eitan_salary, "גלבוע הנדסה - משכורת")
    if month.index > LAPTOP and month.index % 2 == 1:
        book.checking(
            MAYA,
            month.random_day(20, 27),
            month.amount(1_800, 3_200),
            'העברה - סטודיו קו בע"מ',
            Note("income", "freelance", "Design work for Studio Kav"),
        )
    if month.month == 3:
        book.checking(EITAN, month.day(10), 9_500.00, "גלבוע הנדסה - מענק")
    book.checking(MAYA, month.day(20), 173.00, "ביטוח לאומי - קצבת ילדים")
    if month.index == TAX_REFUND:
        book.checking(
            MAYA,
            month.day(18),
            2_840.00,
            "החזר מס הכנסה",
            Note(
                "income",
                "tax refund",
                "Refund for the year before last: the daycare credit points",
            ),
        )


def add_housing(book: Book, month: Month):
    rent = 6_650.00 if month.index >= RENT_RISE else 6_400.00
    book.checking(MAYA, month.day(1), -rent, 'שכ"ד - יעקב גולן')
    # Eitan's share of the rent and bills, from his account to Maya's
    book.checking(EITAN, month.day(11), -4_200.00, "העברה - מאיה רוזן")
    book.checking(MAYA, month.day(11), 4_200.00, "זיכוי - איתן רוזן")
    if month.month % 2 == 1:
        book.checking(EITAN, month.day(15), -1_184.00, "עיריית נווה ארזים - ארנונה")
        water = month.amount(190, 240) + 140 * month.summer_factor
        book.checking(EITAN, month.day(25), -round(water, 2), "מי נווה ארזים")
    else:
        # Bi-monthly, of the two months before: air conditioning in the summer,
        # heating in the winter
        demand = 1 + 1.1 * month.summer_factor + 0.6 * (1 - month.summer_factor) ** 3
        electricity = round(480 * demand * month.amount(0.92, 1.08), 2)
        book.checking(EITAN, month.day(22), -electricity, "זרם פלוס חשמל לבית")
    book.card(EITAN, month.day(3), -99.90, "סיבנט אינטרנט")
    book.card(EITAN, month.day(6), -59.00, 'גל סלולר בע"מ')
    book.card(MAYA, month.day(6), -59.00, 'גל סלולר בע"מ')
    streaming = 44.90 if month.index >= STREAMING_PRICE_RISE else 39.90
    book.card(MAYA, month.day(4), -streaming, "STREAMBOX* MONTHLY")
    book.card(MAYA, month.day(12), -21.90, "SONGWAVE PREMIUM")
    if month.index >= CLOUD_STORAGE_START:
        book.card(MAYA, month.day(14), -9.90, "CLOUDSAFE 200GB")
    book.card(EITAN, month.day(2), -219.00, "כושר פלוס - מנוי חודשי")
    book.checking(MAYA, month.day(28), -12.90, "דמי ניהול חשבון")
    book.checking(EITAN, month.day(28), -12.90, "דמי ניהול חשבון")
    book.checking(EITAN, month.day(5), -186.00, "מגן ביטוח בריאות")


def add_food(book: Book, month: Month):
    holiday = 1.35 if month.is_holiday_season else 1.0
    for _ in range(month.rng.randint(4, 5)):
        branch = month.rng.choices(GROCERY_BRANCHES, weights=(5, 3, 2, 2))[0]
        amount = -round(month.amount(360, 690) * holiday, 2)
        note = month.note("food", "groceries", GROCERY_NOTES, 0.15)
        book.card(EITAN, month.random_day(), amount, branch, note)
    for _ in range(month.rng.randint(3, 5)):
        grocer = month.rng.choice(SMALL_GROCERS)
        book.card(
            MAYA,
            month.random_day(),
            -month.amount(48, 210),
            grocer,
            Note("food", "groceries"),
        )
    for _ in range(month.rng.randint(4, 6)):
        book.card(
            MAYA,
            month.random_day(),
            -month.amount(14, 46),
            month.rng.choice(CAFES),
            Note("food", "coffee"),
        )
    for _ in range(month.rng.randint(1, 3)):
        note = month.note("food", "dining out", DINING_NOTES, 0.5)
        owner = month.rng.choice((MAYA, EITAN))
        book.card(
            owner,
            month.random_day(),
            -month.amount(95, 420),
            month.rng.choice(RESTAURANTS),
            note,
        )
    for _ in range(month.rng.randint(2, 3)):
        # Eitan's agent tags the deliveries
        book.card(
            EITAN,
            month.random_day(),
            -month.amount(78, 185),
            month.rng.choice(DELIVERIES),
            Note("food", "delivery", author=AGENT),
        )
    if month.chance(0.6):
        book.wallet(
            month.random_day(),
            -month.amount(40, 95),
            "Bakery on the corner",
            Note("food", "groceries"),
        )


def add_car(book: Book, month: Month):
    hybrid = month.index > CAR_SWAP
    for _ in range(2 if hybrid else month.rng.randint(2, 3)):
        amount = month.amount(150, 230) if hybrid else month.amount(240, 330)
        book.card(
            EITAN,
            month.random_day(),
            -amount,
            month.rng.choice(FUEL_STATIONS),
            Note("car", "fuel", author=AGENT),
        )
    for _ in range(month.rng.randint(1, 3)):
        book.card(
            MAYA,
            month.random_day(),
            -month.amount(8, 42),
            "PARKO* חניה",
            month.note("car", "parking", PARKING_NOTES, 0.15),
        )
    if month.month == 3:
        premium = 5_380.00 if hybrid else 4_120.00
        book.checking(
            EITAN,
            month.day(12),
            -premium,
            "מגן ביטוח - ביטוח רכב",
            Note("car", "insurance", "Comprehensive + third party, for the year"),
        )
    if month.month == 4:
        book.card(
            EITAN,
            month.day(16),
            -1_148.00,
            "משרד התחבורה אגרת רישוי",
            Note("car", "license"),
        )
    if month.month in (2, 8) and month.index != OLD_CAR_GEARBOX:
        book.card(
            EITAN,
            month.random_day(),
            -month.amount(680, 1_350),
            "מוסך האחים גבאי",
            Note("car", "service", "Periodic service"),
        )


def add_family(book: Book, month: Month):
    if month.month != 8:
        # Closed in August, and 4% dearer every September
        fee = round(3_150.00 * 1.04**month.school_years, 2)
        book.checking(MAYA, month.day(5), -fee, "גן הדובונים - שכר לימוד")
    if month.month >= 9 or month.month <= 6:
        book.card(
            MAYA,
            month.day(8),
            -180.00,
            "ג'ימבורי כיף - חוג",
            Note("kids", "activities"),
        )
    if month.chance(0.5):
        book.card(
            MAYA,
            month.random_day(),
            -month.amount(85, 290),
            "KIDZONE KIDS",
            month.note("kids", "clothes", KIDS_CLOTHES_NOTES, 0.5),
        )
    if month.chance(0.3):
        book.card(
            MAYA,
            month.random_day(),
            -month.amount(60, 210),
            "צעצועי הקסם",
            month.note("kids", "toys", TOYS_NOTES, 0.5),
        )
    book.wallet(
        month.random_day(),
        -month.rng.choice((150.0, 200.0, 240.0)),
        "Babysitter (Noa)",
        Note("kids", "babysitting"),
    )
    book.card(
        EITAN,
        month.random_day(),
        -month.amount(170, 265),
        "חיות ועוד - סניף 3",
        Note("pets", "food", "Pita's food"),
    )
    if month.chance(0.25):
        book.card(
            MAYA,
            month.random_day(),
            -month.amount(240, 520),
            'וטרינר ד"ר אורן שחר',
            month.note("pets", "vet", VET_NOTES, 0.8),
        )


def add_health_and_shopping(book: Book, month: Month):
    for _ in range(month.rng.randint(1, 2)):
        book.card(
            MAYA,
            month.random_day(),
            -month.amount(32, 165),
            "פארמה פלוס 33",
            month.note("health", "pharmacy", PHARMACY_NOTES, 0.25),
        )
    if month.chance(0.5):
        book.card(
            MAYA,
            month.random_day(),
            -month.amount(28, 95),
            'קופ"ח שלום - השתתפות',
            month.note("health", "doctor", DOCTOR_NOTES, 0.6),
        )
    if month.index % 6 == 2:
        book.card(
            EITAN,
            month.random_day(),
            -380.00,
            'מרפאת שיניים ד"ר לוין',
            Note("health", "dental", "Checkup and cleaning, both of us"),
        )
    if month.chance(0.55):
        book.card(
            month.rng.choice((MAYA, EITAN)),
            month.random_day(),
            -month.amount(140, 460),
            month.rng.choice(("FASHION OUTLET 22", "בגדי אורן")),
            month.note("shopping", "clothes", CLOTHES_NOTES, 0.4),
        )
    if month.chance(0.4):
        book.card(
            MAYA,
            month.random_day(),
            -month.amount(70, 480),
            "בית ונוי - נתניה",
            month.note("home", "household", HOME_NOTES, 0.5),
        )
    if month.chance(0.5):
        book.card(
            MAYA,
            month.random_day(),
            -month.amount(29, 145),
            "SHOPGLOBE.COM",
            month.note("shopping", "online", ONLINE_NOTES, 0.6),
        )


def add_leisure(book: Book, month: Month):
    if month.chance(0.35):
        book.card(
            MAYA,
            month.random_day(),
            -round(48 * month.rng.choice((2, 2, 3)), 2),
            "קולנוע הכוכב",
            Note("leisure", "culture"),
        )
    if month.index in WEEKENDS_AWAY:
        book.card(
            EITAN,
            month.day(14),
            -1_480.00,
            "צימר נוף הגליל",
            Note("leisure", "weekend away", "Two nights up north, Pita came too"),
        )
    if month.month in (1, 11) and month.index > 2:
        book.card(
            MAYA,
            month.day(19),
            -320.00,
            "TICKETZONE*",
            Note("leisure", "culture", "Concert, with Yael and Noam"),
        )


def add_cash(book: Book, month: Month):
    book.checking(MAYA, month.random_day(1, 6), -400.00, CASH_WITHDRAWAL)
    if month.chance(0.4):
        book.checking(EITAN, month.random_day(12, 20), -200.00, CASH_WITHDRAWAL)
    if month.chance(0.6):
        book.wallet(
            month.random_day(),
            -month.amount(35, 80),
            "Market stall vegetables",
            Note("food", "groceries"),
        )
    if month.index % 2 == 0:
        book.wallet(
            month.random_day(),
            -70.00,
            "Haircut (Eitan)",
            Note("health", "grooming"),
        )


CASH_COUNT_SHORTFALL = 137.50
"""What the wallet's rows had, but not the wallet: small spending nobody wrote
down."""


def add_cash_count(book: Book, month: Month):
    """At the end of the month, the wallet is counted, and re-anchored to what
    it held."""
    date = month.day(28)
    rows = sum(
        entry.amount
        for entry in book.entries
        if entry.date <= date and entry.account == CASH
    )
    withdrawn = -sum(
        entry.amount
        for entry in book.entries
        if entry.date <= date and entry.raw_description == CASH_WITHDRAWAL
    )
    counted = rows + withdrawn - CASH_COUNT_SHORTFALL
    book.wallet(
        date,
        -CASH_COUNT_SHORTFALL,
        "Cash count",
        Note(
            "reconciliation",
            "cash count",
            f"Counted {counted:.2f} in the wallet, {CASH_COUNT_SHORTFALL:.2f} less "
            "than its rows: small spending nobody wrote down",
        ),
    )


def add_savings(book: Book, month: Month):
    deposit = 3_000.00 if month.index >= RAISE else 2_500.00
    book.checking(MAYA, month.day(10), -deposit, SAVINGS_DEPOSIT)
    book.savings(month.day(28), month.amount(95, 140), "Interest")
    if month.index > CAR_SWAP:
        book.checking(EITAN, month.day(15), -1_720.00, LOAN_REPAYMENT)


def add_events(book: Book, month: Month):
    """The story's one-time events."""
    if month.index == month.timeline.flights_booked:
        book.card(
            MAYA,
            month.day(11),
            -7_840.00,
            "SKYBLUE AIR",
            Note("vacation", "flights", "Lisbon in July, booked early"),
        )
    if month.index == month.timeline.summer_vacation:
        hotel = -5_960.00
        book.card(
            MAYA,
            month.day(8),
            hotel,
            "HOTEL MIRADOR LISBOA",
            Note(
                "vacation",
                "lodging",
                split=(
                    (-4_960.00, "vacation", "lodging", "Six nights"),
                    (-1_000.00, "vacation", "food", "Half board"),
                ),
            ),
        )
        book.card(
            MAYA, month.day(8), -1_780.00, "RENTAROAD PORTUGAL", Note("vacation", "car")
        )
        for _ in range(6):
            book.card(
                month.rng.choice((MAYA, EITAN)),
                month.random_day(8, 14),
                -month.amount(110, 390),
                month.rng.choice(VACATION_RESTAURANTS),
                Note("vacation", "food"),
            )
        book.card(
            EITAN,
            month.day(10),
            -260.00,
            "OCEANARIO LISBOA",
            Note("vacation", "activities", "Yuval's favourite day"),
        )
    if month.index == month.timeline.winter_trip:
        book.card(
            MAYA,
            month.day(3),
            -2_640.00,
            "SKYBLUE AIR",
            Note("vacation", "flights", "Athens, the two of us"),
        )
        book.card(
            MAYA,
            month.day(20),
            -1_920.00,
            "ATHENS PLAKA SUITES",
            Note(
                "vacation",
                "lodging",
                "Three nights\nGrandma Rina had Yuval and Pita",
            ),
        )
        for _ in range(3):
            book.card(
                EITAN,
                month.random_day(20, 23),
                -month.amount(140, 330),
                "TAVERNA TO STEKI",
                Note("vacation", "food"),
            )
    if month.index == OLD_CAR_GEARBOX:
        book.card(
            EITAN,
            month.day(17),
            -3_420.00,
            "מוסך האחים גבאי",
            Note(
                "car",
                "service",
                "Gearbox, again\nThe mechanic says it's not worth fixing next time",
            ),
        )
    if month.index == month.timeline.fridge_dies:
        book.card(
            MAYA,
            month.day(6),
            -7_190.00,
            'אלקטרו-בית בע"מ',
            Note(
                "home",
                "appliances",
                "New fridge: the old one died in the heatwave\n"
                "Five years of warranty, the receipt is in the drawer",
                kind="one-off",
            ),
        )
    if month.index == CAR_SWAP:
        add_car_swap(book, month)
    if month.index == DENTAL_CROWN:
        book.card(
            EITAN,
            month.day(13),
            -6_800.00,
            'מרפאת שיניים ד"ר לוין',
            Note("health", "dental", "Root canal and a crown", kind="one-off"),
        )
    if month.index == DENTAL_CROWN + 1:
        # Its own subtag: an income netting by subtag, a refund netting by tag
        book.checking(
            EITAN,
            month.day(7),
            2_720.00,
            "מגן ביטוח בריאות - החזר",
            Note(
                "health",
                "reimbursement",
                "40% of the crown back from the health insurance",
                kind="one-off",
            ),
        )
    if month.index == LAPTOP:
        book.card(
            MAYA,
            month.day(21),
            -5_420.00,
            'מחשבים פלוס בע"מ',
            Note(
                "shopping",
                "electronics",
                "Laptop for the freelance work",
                kind="one-off",
            ),
        )
    if month.index == RETURNED_JACKET:
        book.card(
            MAYA,
            month.day(2),
            129.90,
            "FASHION OUTLET 22",
            Note("shopping", "clothes", "Returned the jacket"),
        )


def add_car_swap(book: Book, month: Month):
    """The old car is sold, and the new one bought with savings and a loan."""
    book.checking(
        EITAN,
        month.day(4),
        38_000.00,
        "זיכוי - דוד שמעוני",
        # Income, not the car's: the purchase stands as the one-off expense, not
        # netted against it (the demo nets by tag)
        Note("income", "car sale", "Sold the old car to a neighbour's son"),
    )
    book.savings(month.day(18), -40_000.00, "Withdrawal to Eitan's checking")
    book.checking(EITAN, month.day(18), 40_000.00, "זיכוי מפיקדון חיסכון")
    book.loan(month.day(20), -60_000.00, "Dealer loan, 35 payments, no interest")
    book.checking(EITAN, month.day(20), 60_000.00, "הלוואה 0548217")
    book.checking(
        EITAN,
        month.day(20),
        -138_000.00,
        'אוטו-סנטר השרון בע"מ',
        Note(
            "car",
            "purchase",
            "The new hybrid, finally\nFirst year's insurance included in the price",
            split=(
                (-133_400.00, "car", "purchase", None),
                (-4_600.00, "car", "insurance", "First year, bundled by the dealer"),
            ),
        ),
    )


def add_bit(book: Book, month: Month, balance: float) -> float:
    """Maya's Bit payments of the month, and the balance after them."""
    if month.index < BIT_EXPORT_START:
        # Before the export: only the card's charges, annotated one by one
        for _ in range(month.rng.randint(1, 2)):
            _memo, description = month.rng.choice(GIFT_MEMOS)
            book.card(
                MAYA,
                month.random_day(),
                -round(month.rng.choice((100, 150, 200)), 2),
                CARD_BIT,
                Note("gifts", "birthdays", f"{description} (Bit)"),
            )
        return balance
    payments = []
    for _ in range(month.rng.randint(1, 2)):
        memo, _description = month.rng.choice(GIFT_MEMOS)
        friend = month.rng.choice(FRIENDS)
        amount = float(month.rng.choice((100, 150, 200, 250)))
        note = month.note("gifts", "birthdays", BIRTHDAY_NOTES, 0.35)
        payments.append((month.random_day(), amount, friend, memo, note))
    if month.is_wedding_season and month.chance(0.55):
        friend = month.rng.choice(FRIENDS)
        amount = float(month.rng.choice((600, 700, 800, 1_000)))
        payments.append(
            (month.random_day(), amount, friend, "מזל טוב!", Note("gifts", "weddings"))
        )
    payments.append((month.day(3), 250.0, "שלומי ועד הבית", "ועד בית", None))
    if month.chance(0.5):
        payments.append(
            (
                month.random_day(),
                float(month.rng.choice((45, 60, 85, 120))),
                month.rng.choice(FRIENDS),
                month.rng.choice(DINNER_SHARE_MEMOS),
                Note("food", "dining out"),
            )
        )
    received = []
    if month.chance(0.6):
        received.append(
            (
                month.random_day(),
                float(month.rng.choice((60, 80, 120, 160))),
                month.rng.choice(FRIENDS),
                month.rng.choice(("על המסעדה", "על הפיצה", "חלק שלי")),
                Note("food", "dining out", "Paid back their share of dinner"),
            )
        )
    if month.index in SECOND_HAND_SALES:
        amount, memo, description = SECOND_HAND_SALES[month.index]
        received.append(
            (
                month.day(16),
                amount,
                "אורית מ.",
                memo,
                Note("income", "second-hand", description),
            )
        )
    events = [(date, -amount, *rest) for date, amount, *rest in payments]
    events += received
    for date, amount, counterparty, memo, note in sorted(
        events, key=lambda event: event[0]
    ):
        if amount > 0:
            details = BitDetails(counterparty, memo, "balance", credit=True)
            balance += amount
        elif balance >= -amount and month.chance(0.5):
            details = BitDetails(counterparty, memo, "balance", credit=False)
            balance += amount
        else:
            details = BitDetails(counterparty, memo, "card", credit=False)
            book.card(MAYA, date, amount, CARD_BIT)
        # (the house committee's, without a note, is tagged by a rule)
        book.bit(date, amount, details, note)
    # Requests that expired or that Maya declined: not money
    if month.chance(0.25):
        book.bit(
            month.random_day(),
            -90.0,
            BitDetails("", "על הפיצה", "card", credit=False, status="Expired"),
        )
    if month.index % 5 == 0:
        book.bit(
            month.random_day(),
            -45.0,
            BitDetails(month.rng.choice(FRIENDS), "", "card", False, "You declined"),
        )
    if month.month == 3:
        # Grandma sends a gift for Yuval's birthday straight to the bank
        date = month.day(15)
        book.bit(
            date,
            500.0,
            BitDetails(GRANDMA, "ליום הולדת של יובל", "bank", credit=True),
            Note("income", "gifts", "From Grandma Rina, for Yuval's birthday"),
        )
        book.checking(MAYA, date, 500.0, CHECKING_BIT)
    if balance >= 300 and month.index % 3 == 2:
        # Maya withdraws the balance to checking
        date = month.day(27)
        withdrawal = round(balance, 2)
        book.bit(date, withdrawal, BitDetails(MAYA_NAME, "", "bank", credit=True))
        book.checking(MAYA, date, withdrawal, CHECKING_BIT)
        balance -= withdrawal
    return balance


def add_splits(book: Book, months: list[Month]):
    """Purchases of several things, annotated as parts."""
    book.card(
        EITAN,
        months[4].day(13),
        -642.30,
        GROCERY_BRANCHES[0],
        Note(
            "food",
            "groceries",
            split=(
                (-487.40, "food", "groceries", None),
                (-154.90, "home", "household", "Cleaning stuff and a new mop"),
            ),
        ),
    )
    book.card(
        MAYA,
        months[7].day(21),
        -154.00,
        "פארמה פלוס 33",
        Note(
            "health",
            "pharmacy",
            split=(
                (-96.00, "health", "pharmacy", None),
                (-58.00, "kids", "clothes", "Sun hat for Yuval"),
            ),
        ),
    )
    book.card(
        MAYA,
        months[10].day(6),
        -389.70,
        "FASHION OUTLET 22",
        Note(
            "shopping",
            "clothes",
            split=(
                (-239.80, "shopping", "clothes", None),
                (-149.90, "gifts", "birthdays", "Shirt for Ofir's birthday"),
            ),
        ),
    )
    book.bit(
        months[15].day(9),
        -360.0,
        BitDetails("דנה כץ", "מתנה לגננת + טיול", "card", credit=False),
        Note(
            "kids",
            "activities",
            "Dana collected for the daycare",
            split=(
                (-150.00, "gifts", "teachers", "The teacher's end of year gift"),
                (-210.00, "kids", "activities", "The daycare's trip"),
            ),
        ),
    )
    book.card(MAYA, months[15].day(9), -360.0, CARD_BIT)


def add_review_requests(book: Book, months: list[Month]):
    """Transactions the agent annotated, flagged with a question for the owner."""
    two_months_ago, last_month = months[-3], months[-2]
    book.card(
        MAYA,
        two_months_ago.day(17),
        -412.60,
        "פארמה פלוס 33",
        Note(
            "health",
            "pharmacy",
            "Much larger than the usual pharmacy charge (about 100). I tagged it "
            "health/pharmacy: was part of it for Yuval (kids)?",
            author=AGENT,
            needs_review=True,
        ),
    )
    book.card(
        MAYA,
        last_month.day(4),
        -1_280.00,
        "בית ונוי - נתניה",
        Note(
            "home",
            "household",
            "Over twice the largest earlier charge here. Tagged home/household: "
            "is it furniture, and should it be a one-off?",
            author=AGENT,
            needs_review=True,
        ),
    )
    book.bit(
        last_month.day(12),
        -300.0,
        BitDetails("נועם ברק", "", "card", credit=False),
        Note(
            "leisure",
            "culture",
            "A Bit payment to Noam Barak without a memo. It's the price of the "
            "concert tickets, so I tagged it leisure/culture. Correct?",
            author=AGENT,
            needs_review=True,
        ),
    )
    book.card(MAYA, last_month.day(12), -300.0, CARD_BIT)
    for _ in range(2):
        book.card(
            EITAN,
            last_month.day(19),
            -146.00,
            "FOODRUSH*THAI BOX",
            Note(
                "food",
                "delivery",
                "Two identical orders on the same evening: a double charge to "
                "dispute, or two orders?",
                author=AGENT,
                needs_review=True,
            ),
        )


def add_card_payments(book: Book, months: list[Month]):
    """Each card's charges of a month are paid from checking on the 10th of the
    next."""
    for owner in (MAYA, EITAN):
        for previous, current in itertools.pairwise(months):
            charges = sum(
                entry.amount
                for entry in book.entries
                if entry.account == owner
                and entry.source == "card"
                and (entry.date.year, entry.date.month)
                == (previous.year, previous.month)
            )
            book.checking(owner, current.day(10), round(charges, 2), CARD_PAYMENT)


def orphan_entry(months: list[Month]) -> Entry:
    return Entry(
        MAYA,
        "card",
        months[-4].day(7),
        -389.00,
        "SHOPGLOBE.COM",
        Note(
            "shopping",
            "online",
            "Order cancelled, the seller never shipped\n"
            "Gone from the next export, the refund cancelled it out",
        ),
    )


def overridden_annotation(entries: list[Entry]) -> tuple[Entry, Note]:
    """Eitan tagged a gym charge himself before the rule for the gym existed."""
    gym = [entry for entry in entries if entry.raw_description.startswith("כושר פלוס")]
    return gym[3], Note(
        "leisure",
        "sports",
        "The gym's summer promo month\ncounts as fun, not health",
    )
