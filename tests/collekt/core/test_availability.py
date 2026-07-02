"""Tests for source availability checks."""

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

from collekt.core import availability as av
from collekt.core.request import Region


def _epoch_ms(day: date) -> float:
    return (
        datetime(day.year, day.month, day.day, tzinfo=UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    ).total_seconds() * 1000


def _catalogue(dataset_id, bbox, time_start, time_end):
    variable = SimpleNamespace(
        short_name="uo",
        bbox=bbox,  # [min_lon, min_lat, max_lon, max_lat]
        coordinates=[
            SimpleNamespace(
                coordinate_id="time",
                coordinate_unit="milliseconds since 1970-01-01 00:00:00",
                minimum_value=_epoch_ms(time_start),
                maximum_value=_epoch_ms(time_end),
            )
        ],
    )
    return SimpleNamespace(
        products=[
            SimpleNamespace(
                datasets=[
                    SimpleNamespace(
                        dataset_id=dataset_id,
                        versions=[
                            SimpleNamespace(parts=[SimpleNamespace(services=[SimpleNamespace(variables=[variable])])])
                        ],
                    )
                ]
            )
        ]
    )


def test_parse_relative_date_handles_iso_and_now_expressions():
    assert av.parse_relative_date("2023-04-01") == date(2023, 4, 1)
    assert av.parse_relative_date("now") == av.today()
    assert av.parse_relative_date("now-5d") == av.today() - timedelta(days=5)
    assert av.parse_relative_date("now+10d") == av.today() + timedelta(days=10)
    assert av.parse_relative_date("now-1w") == av.today() - timedelta(days=7)
    assert av.parse_relative_date(None) is None
    assert av.parse_relative_date(None, default=date(2000, 1, 1)) == date(2000, 1, 1)


def test_parse_coverage_defaults_to_global_and_open_dates():
    coverage = av.parse_coverage({})
    assert (coverage.west, coverage.east, coverage.south, coverage.north) == (-180.0, 180.0, -90.0, 90.0)
    assert coverage.start is None and coverage.end is None


def test_region_overlaps_and_day_in_range():
    coverage = av.Coverage(-6.0, 20.0, 35.0, 45.0, date(2023, 4, 1), date(2023, 8, 31))
    assert av.region_overlaps(Region.from_bbox((0, 8, 38, 42)), coverage) is True
    assert av.region_overlaps(Region.from_bbox((-40, -30, 38, 42)), coverage) is False
    assert av.day_in_range(date(2023, 6, 15), coverage) is True
    assert av.day_in_range(date(2024, 1, 1), coverage) is False
    open_coverage = av.Coverage(-180.0, 180.0, -90.0, 90.0, None, None)
    assert av.day_in_range(date(1900, 1, 1), open_coverage) is True


def test_describe_coverage_reads_dataset_bounds():
    catalogue = _catalogue(
        "glo", bbox=[-180.0, -80.0, 180.0, 90.0], time_start=date(1993, 1, 1), time_end=date(2024, 6, 30)
    )
    coverage = av.describe_coverage("glo", describe=lambda **_: catalogue)
    assert coverage is not None
    assert (coverage.west, coverage.east, coverage.south, coverage.north) == (-180.0, 180.0, -80.0, 90.0)
    assert coverage.start == date(1993, 1, 1)
    assert coverage.end == date(2024, 6, 30)


def test_describe_coverage_unknown_when_lookup_fails_or_missing():
    assert av.describe_coverage("glo", describe=None) is None  # toolbox unavailable

    def failing(**_):
        raise RuntimeError("offline")

    assert av.describe_coverage("glo", describe=failing) is None  # network/provider error
    other = _catalogue("other", bbox=[-1, -1, 1, 1], time_start=date(2020, 1, 1), time_end=date(2020, 12, 31))
    assert av.describe_coverage("glo", describe=lambda **_: other) is None  # dataset not in catalogue


def test_merge_coverages_builds_bounding_envelope():
    nrt = av.Coverage(-180.0, 180.0, -80.0, 90.0, date(2024, 6, 1), date(2026, 1, 1))
    my = av.Coverage(-180.0, 180.0, -80.0, 90.0, date(1993, 1, 1), date(2024, 6, 30))
    merged = av.merge_coverages([nrt, my])
    assert merged.start == date(1993, 1, 1)
    assert merged.end == date(2026, 1, 1)
