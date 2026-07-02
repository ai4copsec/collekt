"""Tests for request normalization, region construction, and naming."""

import json
from datetime import UTC, datetime

import pytest

from collekt.core.naming import PatternError, bbox_hash, format_pattern, pattern_values
from collekt.core.request import Region, Request


def test_region_from_description_tuple():
    region = Region.from_description_tuple((35, 45, -6, 20))
    assert region.as_dict() == {"west": -6.0, "east": 20.0, "south": 35.0, "north": 45.0}


def test_region_from_point_radius_brackets_the_centre():
    region = Region.from_point_radius(latitude=13.311, longitude=42.923, radius_km=50)
    assert region.west < 42.923 < region.east
    assert region.south < 13.311 < region.north
    assert region.geometry is None
    # ~50 km is well under one degree of latitude in either direction.
    assert 0.0 < (region.north - region.south) < 2.0


def test_region_from_geojson_keeps_bbox_and_wkt(tmp_path):
    path = tmp_path / "square.geojson"
    path.write_text(
        json.dumps(
            {
                "type": "Polygon",
                "coordinates": [[[0.0, 0.0], [10.0, 0.0], [10.0, 5.0], [0.0, 5.0], [0.0, 0.0]]],
            }
        ),
        encoding="utf-8",
    )
    region = Region.from_geojson(path)
    assert (region.west, region.east, region.south, region.north) == (0.0, 10.0, 0.0, 5.0)
    assert region.geometry is not None and region.geometry.startswith("POLYGON")
    assert "geometry" in region.as_dict()


def test_request_date_only_expands_to_full_day():
    request = Request(region=Region.from_bbox((-6, 20, 35, 45)), start="2026-06-25")
    assert request.start_datetime == datetime(2026, 6, 25, tzinfo=UTC)
    assert request.end_datetime == datetime(2026, 6, 25, 23, 59, 59, 999999, tzinfo=UTC)
    assert request.sampling_hours == 24
    assert request.sampling_label == "24h"
    assert request.iter_days() == [datetime(2026, 6, 25, tzinfo=UTC).date()]


def test_request_sampling_is_normalized_and_validated():
    request = Request(region=Region.from_bbox((-6, 20, 35, 45)), sampling="6 hours")
    assert request.sampling_hours == 6
    assert request.as_dict()["sampling"] == "6h"
    with pytest.raises(ValueError, match="unsupported sampling"):
        _ = Request(region=Region.from_bbox((-6, 20, 35, 45)), sampling="2h").sampling_hours


def test_bbox_hash_is_stable():
    region = Region.from_bbox((-6, 20, 35, 45))
    assert bbox_hash(region) == bbox_hash(region)
    assert len(bbox_hash(region)) == 8


def test_pattern_formatting_and_unknown_placeholder():
    region = Region.from_bbox((-6, 20, 35, 45))
    values = pattern_values(
        source="era5",
        dataset_id="reanalysis-era5-single-levels",
        region=region,
        start=datetime(2026, 6, 25, tzinfo=UTC),
        end=datetime(2026, 6, 25, 23, 59, 59, tzinfo=UTC),
        sampling="6h",
    )
    assert format_pattern("{source}_{start:%Y%m%d}_{bbox_hash}.nc", values).startswith("era5_20260625_")
    assert format_pattern("{start:%Y%m%d}_{sampling}_{bbox_hash}", values).startswith("20260625_6h_")
    with pytest.raises(PatternError, match="unknown placeholder"):
        format_pattern("{unknown}.nc", values)
