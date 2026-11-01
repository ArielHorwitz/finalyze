"""Unit tests for source data post-processing that the golden tests leave out."""

import datetime

import polars as pl
import pytest

from finalyze.analysis import dashboard
from finalyze.source import data as data_module

FRAME = pl.DataFrame(
    {
        "account": ["alice", "alice", "kim", "kim"],
        "source": ["checking", "card", "checking", "card"],
        "date": [
            datetime.date(2025, 3, 31),
            datetime.date(2025, 4, 1),
            datetime.date(2025, 5, 31),
            datetime.date(2025, 6, 15),
        ],
        "amount": [100.0, -20.0, 300.0, -40.0],
        "raw_description": ["Payroll", "SuperMart", "Payroll", "Roadstar Fuel"],
        "tag": ["salary", "food", "salary", "car"],
        "subtag": ["alice", "groceries", "kim", "fuel"],
        "description": [None, "milk", None, None],
        "transaction_description": [None, "weekly", "bonus", None],
    }
)


@pytest.mark.parametrize(
    ("today", "expected_months"),
    [
        (datetime.date(2025, 4, 20), ["2025-03"]),
        (datetime.date(2025, 6, 1), ["2025-03", "2025-04", "2025-05"]),
        (datetime.date(2025, 6, 30), ["2025-03", "2025-04", "2025-05"]),
        # Clock after the data: nothing to leave out
        (datetime.date(2025, 9, 1), ["2025-03", "2025-04", "2025-05", "2025-06"]),
    ],
)
def test_complete_months_only(monkeypatch, today, expected_months):
    monkeypatch.setattr(data_module, "_today", lambda: today)
    frame = FRAME.with_columns(
        month=pl.col("date").dt.strftime("%Y-%m"), is_sentinel_tick=pl.lit(False)
    )
    periods = dashboard.resolve_periods(
        data_module.SourceData(frame), "all", complete_months_only=True
    )
    assert periods.available_months == expected_months
    assert periods.months.end == expected_months[-1]
    # Balances run to the latest data
    assert periods.days.end is None


def test_no_complete_month(monkeypatch):
    # Data of the current month only: no month for monthly figures, while
    # balances still cover the data
    monkeypatch.setattr(data_module, "_today", lambda: datetime.date(2025, 3, 31))
    frame = FRAME.head(1).with_columns(
        month=pl.col("date").dt.strftime("%Y-%m"), is_sentinel_tick=pl.lit(False)
    )
    periods = dashboard.resolve_periods(
        data_module.SourceData(frame), "12m", complete_months_only=True
    )
    assert periods.available_months == []
    assert dashboard.period_months(periods.months, ["2025-03"]) == []
    assert frame.filter(periods.months.dates_expr()).height == 0
    assert frame.filter(periods.days.dates_expr()).height == 1
