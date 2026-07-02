"""Tests for temporal sampling planning."""

from datetime import UTC, datetime

from collekt.core.config import parse_config
from collekt.core.request import Region, Request
from collekt.core.temporal import day_bounds, sampling_plan


def _source(supported):
    cfg = parse_config(
        {
            "sources": {
                "s": {
                    "kind": "cmems",
                    "variables": ["u"],
                    "temporal": {"native_sampling": supported[0], "supported_sampling": supported},
                }
            }
        }
    )
    return cfg.sources["s"]


def _request(sampling):
    return Request(region=Region.from_bbox((-6, 20, 35, 45)), start="2026-06-25", sampling=sampling)


def test_requested_sampling_is_used_when_supported():
    plan = sampling_plan(_request("6h"), _source(["1h", "3h", "6h", "24h"]))
    assert plan.requested_sampling == "6h"
    assert plan.actual_sampling == "6h"
    assert plan.warning is None


def test_falls_back_to_coarser_supported_sampling_with_warning():
    plan = sampling_plan(_request("6h"), _source(["24h"]))
    assert plan.requested_sampling == "6h"
    assert plan.actual_sampling == "24h"
    assert plan.warning is not None and "does not support" in plan.warning


def test_source_timestamps_filter_by_day():
    request = Request(region=Region.from_bbox((-6, 20, 35, 45)), start="2026-06-25", end="2026-06-26", sampling="24h")
    plan = sampling_plan(request, _source(["24h"]))
    day = datetime(2026, 6, 25, tzinfo=UTC).date()
    assert plan.timestamps_for_day(day) == (datetime(2026, 6, 25, tzinfo=UTC),)


def test_day_bounds_are_clipped_to_the_request():
    request = Request(region=Region.from_bbox((-6, 20, 35, 45)), start="2026-06-25", end="2026-06-25")
    start, end = day_bounds(request, datetime(2026, 6, 25, tzinfo=UTC).date())
    assert start == datetime(2026, 6, 25, tzinfo=UTC)
    assert end == request.end_datetime
