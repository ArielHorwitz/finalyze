"""Characterization ("golden") tests of the full pipeline on the demo data.

The demo is built for a fixed end date (its raw files written, the store
seeded, the data ingested and annotated), then ingested again, and analyzed.
The enriched source data and every number of the dashboard are compared against
snapshots under tests/snapshots/. See tests/snapshot.py for how to regenerate
them.
"""

import datetime
from typing import NamedTuple

import polars as pl
import pytest

from finalyze import config as config_module
from finalyze import demo
from finalyze.demo.household import Household
from finalyze.service import models, open_service
from finalyze.source import data as data_module
from finalyze.source import ingest
from finalyze.source.data import (
    AVERAGED_KINDS,
    FLOW_KINDS,
    SourceData,
    post_process,
)
from finalyze.source.raw import load_source_data
from finalyze.store import Store
from tests.helpers import DEMO_END_DATE
from tests.snapshot import SnapshotDirectory

FLOW_NAMES = ("net", "incomes", "expenses", "saved")
# The demo's config as is, with dashboard defaults stored in the store. The
# clock is pinned to DEMO_END_DATE (see `pinned_clock`).
SCENARIOS = {
    # Every month in monthly figures (the pinned clock is in the data's last
    # month)
    "default": {"complete_months_only": False},
    # Alternative analysis options, to cover more code paths
    "variant": {
        "net_by_tag": True,
        # Leaves out the data's last month, with the pinned clock
        "complete_months_only": True,
        "breakdown_months": 2,
        "rolling_months": 3,
    },
}
FLOWS = {"kinds": FLOW_KINDS}
SOURCE_FLAG_COMBINATIONS = {
    "default": {},
    "all_groups": {"all_groups": True},
    "flows": FLOWS,
    "flows_all_groups": {**FLOWS, "all_groups": True},
    "averaged": {"kinds": AVERAGED_KINDS},
    "saving": {"kinds": frozenset({"saving"})},
    "incomes": {**FLOWS, "incomes": True},
    "expenses": {**FLOWS, "expenses": True},
    "incomes_by_subtag": {**FLOWS, "incomes": True, "net_by_subtag": True},
    "expenses_by_subtag": {**FLOWS, "expenses": True, "net_by_subtag": True},
    "unfiltered_incomes": {"all_groups": True, "incomes": True},
}


class PipelineResult(NamedTuple):
    config: config_module.Config
    host: config_module.Host
    source: SourceData
    household: Household


def pin_clock(monkeypatch):
    monkeypatch.setattr(data_module, "_today", lambda: DEMO_END_DATE)


@pytest.fixture(autouse=True)
def pinned_clock(monkeypatch):
    """Truncating the current month leaves out the demo data's last month."""
    pin_clock(monkeypatch)


@pytest.fixture(scope="module", params=list(SCENARIOS))
def scenario(request):
    return request.param


@pytest.fixture(scope="module")
def pipeline(scenario, tmp_path_factory) -> PipelineResult:
    root = tmp_path_factory.mktemp(f"pipeline-{scenario}")
    household = demo.build(root, end_date=DEMO_END_DATE)
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(config_module, "LOADED_CONFIG", None)
        monkeypatch.setattr(config_module, "LOADED_HOST", None)
        monkeypatch.setattr(
            config_module, "LOADED_CONFIG_DIR", config_module.LOADED_CONFIG_DIR
        )
        pin_clock(monkeypatch)
        config = config_module.load_config(config_dir=root, use_preloaded=False)
        with open_service() as service:
            defaults = models.DashboardDefaults(**SCENARIOS[scenario])
            service.set_dashboard_defaults(defaults)
        host = config_module.host()
        with Store(host.db_file) as store:
            ingest.ingest(store)
            frame = post_process(load_source_data(), store)
        source = SourceData(frame, net_by_tag=defaults.net_by_tag)
    return PipelineResult(config, host, source, household)


@pytest.fixture
def pipeline_config(pipeline, use_config, use_host):
    """Install the pipeline's config (and host) for the duration of a test."""
    use_host(pipeline.host)
    return use_config(pipeline.config)


def test_demo_data_is_tagged_but_the_last_transactions(pipeline, pipeline_config):
    source = pipeline.source.get(all_groups=True)
    untagged = source.filter(pl.col("tag").is_null())
    assert untagged.height == len(pipeline.household.unannotated) > 0
    # Only recent ones, as if just ingested
    assert untagged["date"].min() > DEMO_END_DATE - datetime.timedelta(days=30)


def test_source_data_snapshot(scenario, pipeline, pipeline_config):
    snapshots = SnapshotDirectory(f"{scenario}/source")
    for combination_name, flags in SOURCE_FLAG_COMBINATIONS.items():
        frame = pipeline.source.get(**flags)
        json_flags = {
            name: sorted(value) if isinstance(value, frozenset) else value
            for name, value in flags.items()
        }
        metadata = {
            "flags": json_flags,
            **snapshots.check_frame(combination_name, frame),
        }
        snapshots.check_json(combination_name, metadata)
    # Sentinel ticks are many zero-amount rows (every tag, month, account, and
    # source combination), so only summarize them.
    sentinels = (
        pipeline.source.get(all_groups=True, sentinels=True)
        .filter(pl.col("is_sentinel_tick"))
        .group_by("month", "account", "source", "kind", "group")
        .agg(
            pl.len().alias("rows"),
            pl.col("tags").n_unique().alias("unique_tags"),
            pl.col("amount").abs().sum().alias("total_absolute_amount"),
        )
    )
    snapshots.check_frame("sentinels_summary", sentinels)
    snapshots.finish()


def rows_frame(rows: list[dict], schema=None) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=schema, infer_schema_length=None)


def dashboard_frames(result: dict) -> dict[str, pl.DataFrame]:
    """The dashboard's sections as long tables, one row per number."""
    months = result["period"]["months"]
    balance = result["balance"]
    cash_flow = result["cash_flow"]
    breakdown = result["breakdown"]
    trends = result["trends"]
    return {
        "balance/rows": pl.DataFrame(balance["rows"]),
        "balance/series": rows_frame(
            [
                {
                    "key": series["key"],
                    "label": series["label"],
                    "kind": series["kind"],
                    "account": series["account"],
                    "group": series["group"],
                    "date": date,
                    "value": value,
                    "row": row,
                }
                for series in balance["series"]
                for date, value, rows in zip(
                    series["dates"], series["values"], series["rows"], strict=True
                )
                for row in rows or [None]
            ]
        ),
        "balance/totals": rows_frame(balance["totals"]),
        "balance/bridge": rows_frame(result["bridge"]),
        "health/last_dates": rows_frame(result["health"]["last_dates"]),
        "balance/accounts": rows_frame(balance["accounts"]),
        "cash_flow/months": pl.DataFrame(
            {
                "month": months,
                **{flow: cash_flow[flow] for flow in FLOW_NAMES},
                "kept_rate": cash_flow["kept_rate"],
                "saved_rate": cash_flow["saved_rate"],
                **{
                    f"one_off_{direction}": values
                    for direction, values in cash_flow["one_offs"].items()
                },
            },
            schema_overrides={"kept_rate": pl.Float64, "saved_rate": pl.Float64},
        ),
        "cash_flow/rolling": rows_frame(
            [
                {
                    "window": window,
                    "kind": kind,
                    "month": month,
                    "value": value,
                    "baseline": baseline,
                }
                for window, flows in cash_flow["rolling"].items()
                for kind, values in flows.items()
                for month, value, baseline in zip(
                    months, values, cash_flow["baseline"][window][kind], strict=True
                )
            ],
            schema={
                "window": pl.String,
                "kind": pl.String,
                "month": pl.String,
                "value": pl.Float64,
                "baseline": pl.Float64,
            },
        ),
        "breakdown/totals": rows_frame(
            [
                {"direction": direction, **row}
                for direction, rows in breakdown["totals"].items()
                for row in rows
            ]
        ),
        "breakdown/rolling": rows_frame(
            [
                {"window": window, "direction": direction, **row}
                for window, by_direction in breakdown["rolling"].items()
                for direction, rows in by_direction.items()
                for row in rows
            ]
        ),
        "breakdown/monthly": rows_frame(
            [
                {"direction": direction, **row}
                for direction in ("incomes", "expenses")
                for row in breakdown["monthly"][direction]
            ]
        ),
        "trends/series": rows_frame(
            [
                {
                    "kind": kind,
                    "tag": series["tag"],
                    "window": window,
                    "month": month,
                    "amount": amount,
                    "rolling": mean,
                    "baseline": baseline,
                }
                for kind in ("incomes", "expenses")
                for series in trends[kind]
                for window, means in series["rolling"].items()
                for month, amount, mean, baseline in zip(
                    months,
                    series["amounts"],
                    means,
                    series["baseline"][window],
                    strict=True,
                )
            ]
        ),
        "trends/subtags": rows_frame(
            [
                {
                    "kind": kind,
                    "tag": series["tag"],
                    "subtag": series["subtag"],
                    "window": window,
                    "month": month,
                    "amount": amount,
                    "rolling": mean,
                    "baseline": baseline,
                }
                for kind in ("incomes", "expenses")
                for series in trends["subtags"][kind]
                for window, means in series["rolling"].items()
                for month, amount, mean, baseline in zip(
                    months,
                    series["amounts"],
                    means,
                    series["baseline"][window],
                    strict=True,
                )
            ]
        ),
    }


def test_dashboard_snapshot(scenario, pipeline, pipeline_config):
    """Every number of the dashboard over all months, with the default options
    (and trends by subtag)."""
    snapshots = SnapshotDirectory(f"{scenario}/dashboard")
    with open_service() as service:
        options = models.DashboardOptions(subtag_trends=True)
        result = service.dashboard(options).model_dump(mode="json")
    for name, frame in dashboard_frames(result).items():
        snapshots.check_frame(name, frame)
    snapshots.check_json(
        "summary",
        {
            "options": result["options"],
            "available_months": result["available_months"],
            "period": result["period"],
            "rolling_windows": result["rolling_windows"],
            "breakdown_monthly_months": result["breakdown"]["monthly"]["months"],
            "tag_order": result["trends"]["tag_order"],
            "headline": result["headline"],
            "health": {
                name: value
                for name, value in result["health"].items()
                if name != "last_dates"
            },
        },
    )
    snapshots.finish()
