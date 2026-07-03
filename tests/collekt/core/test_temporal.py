"""Tests for temporal sampling planning."""

from datetime import UTC, datetime

import pytest

from collekt.core.config import parse_config
from collekt.core.request import Region, Request
from collekt.core.temporal import day_bounds, sampling_plan


def _source(actual_sampling):
    cfg = parse_config(
        {
            "sources": {
                "s": {
                    "kind": "cmems",
                    "variables": ["u"],
                    "temporal_sampling": actual_sampling,
                }
            }
        }
    )
    return cfg.sources["s"]


def _request(sampling):
    return Request(region=Region.from_bbox((-6, 20, 35, 45)), start="2026-06-25", sampling=sampling)


def test_requested_sampling_is_used_when_it_is_a_multiple_of_dataset_sampling():
    plan = sampling_plan(_request("6h"), _source("3h"))
    assert plan.requested_sampling == "6h"
    assert plan.actual_sampling == "6h"


def test_minute_sampling_can_be_requested_or_coarsened_to_hours():
    native = sampling_plan(_request("15min"), _source("15min"))
    hourly = sampling_plan(_request("1h"), _source("15min"))

    assert native.actual_sampling == "15min"
    assert len(native.source_timestamps) == 96
    assert hourly.actual_sampling == "1h"
    assert len(hourly.source_timestamps) == 24


def test_rejects_sampling_finer_than_the_dataset():
    with pytest.raises(ValueError, match="integer multiple"):
        sampling_plan(_request("6h"), _source("24h"))


def test_source_timestamps_filter_by_day():
    request = Request(region=Region.from_bbox((-6, 20, 35, 45)), start="2026-06-25", end="2026-06-26", sampling="24h")
    plan = sampling_plan(request, _source("24h"))
    day = datetime(2026, 6, 25, tzinfo=UTC).date()
    assert plan.timestamps_for_day(day) == (datetime(2026, 6, 25, tzinfo=UTC),)


def test_day_bounds_are_clipped_to_the_request():
    request = Request(region=Region.from_bbox((-6, 20, 35, 45)), start="2026-06-25", end="2026-06-25")
    start, end = day_bounds(request, datetime(2026, 6, 25, tzinfo=UTC).date())
    assert start == datetime(2026, 6, 25, tzinfo=UTC)
    assert end == request.end_datetime
