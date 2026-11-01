"""Exact amounts as integer minor units (e.g. cents), at currency precision."""

import math

import polars as pl

CURRENCY_DECIMALS = 2
MINOR_UNITS_PER_UNIT = 10**CURRENCY_DECIMALS


def to_minor_units(amount: float) -> int:
    """Round half away from zero, like polars (see `minor_units_expr`)."""
    scaled = abs(amount) * MINOR_UNITS_PER_UNIT
    return int(math.copysign(math.floor(scaled + 0.5), amount))


def exact_minor_units(amount: float) -> int:
    """Like `to_minor_units`, but raises ValueError for finer amounts than currency."""
    minor_units = to_minor_units(amount)
    scaled = amount * MINOR_UNITS_PER_UNIT
    if not math.isclose(minor_units, scaled, rel_tol=1e-12, abs_tol=1e-6):
        raise ValueError(
            f"Amount {amount} has more than {CURRENCY_DECIMALS} decimal places"
        )
    return minor_units


def from_minor_units(minor_units: int) -> float:
    return minor_units / MINOR_UNITS_PER_UNIT


def minor_units_expr(column: str) -> pl.Expr:
    """Polars equivalent of `to_minor_units`."""
    return pl.col(column).mul(MINOR_UNITS_PER_UNIT).round(0).cast(pl.Int64)
