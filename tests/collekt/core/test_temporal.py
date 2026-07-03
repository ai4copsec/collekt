"""Tests for native-cadence timestamp planning."""

from datetime import UTC, datetime

from collekt.core.config import parse_config
from collekt.core.request import Region, Request
from collekt.core.temporal import day_bounds, sampling_plan


def _source(temporal_sampling):
    cfg = parse_config(
        {
            "sources": {
                "s": {
                    "kind": "cmems",
                    "variables": ["u"],
                    "temporal_sampling": temporal_sampling,
                }
            }
        }
    )
    return cfg.sources["s"]


def _request(start="2026-06-25", end=None):
    return Request(region=Region.from_bbox((-6, 20, 35, 45)), start=start, end=end)


def test_plan_enumerates_the_native_cadence():
    # Each dataset is fetched at its own cadence; there is no request-level sampling.
    daily = sampling_plan(_request(), _source("24h"))
    hourly = sampling_plan(_request(), _source("1h"))
    quarter = sampling_plan(_request(), _source("15min"))

    assert daily.actual_sampling == "24h"
    assert len(daily.source_timestamps) == 1
    assert hourly.actual_sampling == "1h"
    assert len(hourly.source_timestamps) == 24
    assert quarter.actual_sampling == "15min"
    assert len(quarter.source_timestamps) == 96


def test_source_timestamps_filter_by_day():
    plan = sampling_plan(_request(start="2026-06-25", end="2026-06-26"), _source("24h"))
    day = datetime(2026, 6, 25, tzinfo=UTC).date()
    assert plan.timestamps_for_day(day) == (datetime(2026, 6, 25, tzinfo=UTC),)


def test_day_bounds_are_clipped_to_the_request():
    request = Request(region=Region.from_bbox((-6, 20, 35, 45)), start="2026-06-25", end="2026-06-25")
    start, end = day_bounds(request, datetime(2026, 6, 25, tzinfo=UTC).date())
    assert start == datetime(2026, 6, 25, tzinfo=UTC)
    assert end == request.end_datetime
