"""The demo household's setup: its tags, groups and colors, the rules that tag
its routine transactions, the ingestion rules that reshape its raw files, and
the dashboard's defaults."""

import datetime

from finalyze.config import Filters
from finalyze.demo import household as story
from finalyze.store import Store

# Tags with their kind and color, colorblind-friendly on the dark surface
TAGS = {
    "income": ("regular", "#5cc596"),
    "housing": ("regular", "#3987e5"),
    "utilities": ("regular", "#c98500"),
    "subscriptions": ("regular", "#8f86e6"),
    "food": ("regular", "#199e70"),
    "car": ("regular", "#d95926"),
    "kids": ("regular", "#e66fb0"),
    "pets": ("regular", "#b08454"),
    "health": ("regular", "#e66767"),
    "shopping": ("regular", "#c34f8c"),
    "home": ("regular", "#4fb3a9"),
    "gifts": ("regular", "#e8c547"),
    "leisure": ("regular", "#8cc152"),
    "vacation": ("one-off", "#b48ce6"),
    "fees": ("regular", "#898781"),
    "savings": ("saving", "#f2a05c"),
    "transfer": ("transfer", "#52514e"),
    "reconciliation": ("reconciliation", "#a8a49b"),
}
SUBTAG_KINDS = {
    ("savings", "interest"): "regular",
    ("car", "purchase"): "one-off",
    ("income", "car sale"): "one-off",
}
GROUPS = (
    # (name, role, color, accounts)
    ("savings", "holding", "#f2a05c", (story.SAVINGS,)),
    ("debt", "holding", "#e66767", (story.LOAN,)),
)
ACCOUNT_COLORS = {
    story.MAYA: "#3987e5",
    story.EITAN: "#d95926",
    story.CASH: "#8cc152",
    story.SAVINGS: "#f2a05c",
    story.LOAN: "#e66767",
}
LABEL_COLORS = {
    "net worth": "#ffffff",
    "available": "#898781",
    "maya/checking": "#3987e5",
    "maya/card": "#7fb2f0",
    "maya/bit": "#2bb3e0",
    "eitan/checking": "#d95926",
    "eitan/card": "#f0a37a",
    "cash/wallet": "#8cc152",
    "savings/savings": "#f2a05c",
    "loan/car": "#e66767",
}
DASHBOARD_DEFAULTS = {
    # Friends paying back their share net out of what it was for
    "net_by_tag": True,
    "breakdown_months": 3,
    "complete_months_only": True,
    "rolling_months": 6,
}
TRANSFER_TAGS = (
    # (subtag, raw description pattern)
    ("card", "^Transfer checking-card$"),
    ("bit", "^Transfer (card-bit|bit-checking)$"),
    ("cash", "^Transfer checking-cash$"),
    ("household", "^(העברה|זיכוי) - (מאיה|איתן) רוזן$"),
    ("loan", "^(הלוואה|החזר הלוואה) 0548217$|^Dealer loan"),
    ("savings", "^זיכוי מפיקדון חיסכון$|^Withdrawal to"),
)
TAGGING_RULES = (
    # (tag, subtag, raw description pattern, account)
    *(("transfer", subtag, pattern, None) for subtag, pattern in TRANSFER_TAGS),
    ("reconciliation", "opening balance", "^Opening balance$", None),
    ("savings", "deposit", f"^{story.SAVINGS_DEPOSIT}$", None),
    ("savings", "interest", "^Interest$", story.SAVINGS),
    ("income", "salary", "^משכורת - אורבית", None),
    ("income", "salary", "^גלבוע הנדסה - משכורת$", None),
    ("income", "bonus", "^גלבוע הנדסה - מענק$", None),
    ("income", "child allowance", "קצבת ילדים$", None),
    ("housing", "rent", '^שכ"ד', None),
    ("housing", "municipal tax", "ארנונה$", None),
    ("housing", "building committee", "^שלומי ועד הבית", None),
    ("utilities", "electricity", "^זרם פלוס", None),
    ("utilities", "water", "^מי נווה ארזים$", None),
    ("utilities", "internet", "^סיבנט", None),
    ("utilities", "mobile", "^גל סלולר", None),
    ("subscriptions", "streaming", "^(STREAMBOX|SONGWAVE)", None),
    ("subscriptions", "cloud", "^CLOUDSAFE", None),
    ("health", "fitness", "^כושר פלוס", None),
    ("health", "insurance", "^מגן ביטוח בריאות$", None),
    ("kids", "daycare", "^גן הדובונים", None),
    ("fees", "bank", "^דמי ניהול חשבון$", None),
)


def ingestion_rules(bit_export_start: datetime.date) -> list[dict]:
    """How the raw files' rows become the household's transactions: card charges
    and Bit, cash, savings and loan transfers get their other legs."""
    return [
        {
            # Maya's withdrawals of her Bit balance: the balance was credited by
            # the payments that funded them, and checking's side is below
            "match": {
                "account": story.MAYA,
                "source": "bit",
                "raw_description": f"^Bank account: {story.MAYA_NAME}$",
            },
            "action": "drop",
        },
        {
            "match": {
                "source": "checking",
                "raw_description": f"^{story.CARD_PAYMENT}$",
            },
            "action": "transfer",
            "to_source": "card",
            "raw_description": "Transfer checking-card",
        },
        {
            # Earlier Bit payments by card have no Bit export to pair with
            "match": {
                "account": story.MAYA,
                "source": "card",
                "raw_description": f"^{story.CARD_BIT}$",
                "start_date": bit_export_start,
            },
            "action": "transfer",
            "to_source": "bit",
            "raw_description": "Transfer card-bit",
        },
        {
            "match": {
                "account": story.MAYA,
                "source": "checking",
                "raw_description": f"^{story.CHECKING_BIT}$",
            },
            "action": "transfer",
            "to_source": "bit",
            "raw_description": "Transfer bit-checking",
        },
        {
            "match": {
                "source": "checking",
                "raw_description": f"^{story.CASH_WITHDRAWAL}$",
            },
            "action": "transfer",
            "to_account": story.CASH,
            "to_source": "wallet",
            "raw_description": "Transfer checking-cash",
        },
        {
            "match": {
                "account": story.MAYA,
                "source": "checking",
                "raw_description": f"^{story.SAVINGS_DEPOSIT}$",
            },
            "action": "transfer",
            "to_account": story.SAVINGS,
            "to_source": "savings",
        },
        {
            "match": {
                "account": story.EITAN,
                "source": "checking",
                "raw_description": f"^{story.LOAN_REPAYMENT}$",
            },
            "action": "transfer",
            "to_account": story.LOAN,
            "to_source": "car",
        },
    ]


def seed(store: Store):
    """Write the tags, groups, colors and tagging rules to a new store."""
    for tag, (kind, color) in TAGS.items():
        store.update_tag(tag, kind=kind, color=color)
    for (tag, subtag), kind in SUBTAG_KINDS.items():
        store.update_subtag(tag, subtag, kind=kind)
    store.ensure_accounts((story.MAYA, story.EITAN, story.CASH))
    for name, role, color, accounts in GROUPS:
        store.add_group(name, role, color=color)
        for account in accounts:
            store.update_account(account, group=name)
    for account, color in ACCOUNT_COLORS.items():
        store.update_account(account, color=color)
    for label, color in LABEL_COLORS.items():
        store.set_label_color(label, color)
    for tag, subtag, pattern, account in TAGGING_RULES:
        store.add_rule(Filters(raw_description=pattern, account=account), tag, subtag)
