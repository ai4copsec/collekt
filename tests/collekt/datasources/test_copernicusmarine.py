"""Tests for collekt.datasources.copernicusmarine — offline only, no network.

Only get_coordinates_min_max is implemented so far; the remaining tests are
skipped placeholders documenting the intended coverage of the WIP stubs.
"""
import pytest

from collekt.datasources.copernicusmarine import get_coordinates_min_max


def test_get_coordinates_min_max_center_inside_bbox():
    bbox = get_coordinates_min_max(latitude=70.0, longitude=19.0, radius_in_km=50.0)
    assert bbox['lat_min'] < 70.0 < bbox['lat_max']
    assert bbox['lon_min'] < 19.0 < bbox['lon_max']


def test_get_coordinates_min_max_ordering():
    bbox = get_coordinates_min_max(latitude=57.0, longitude=-46.0, radius_in_km=10.0)
    assert bbox['lat_min'] < bbox['lat_max']
    assert bbox['lon_min'] < bbox['lon_max']


def test_get_coordinates_min_max_widens_with_radius():
    small = get_coordinates_min_max(latitude=70.0, longitude=19.0, radius_in_km=10.0)
    large = get_coordinates_min_max(latitude=70.0, longitude=19.0, radius_in_km=100.0)
    assert large['lat_min'] < small['lat_min']
    assert large['lat_max'] > small['lat_max']
    assert large['lon_min'] < small['lon_min']
    assert large['lon_max'] > small['lon_max']


@pytest.mark.skip(reason="not implemented yet")
def test_subset_filename_deterministic_and_unique():
    """_subset_filename: same inputs -> same name; different dataset_id/bbox/window/variables -> different names."""


@pytest.mark.skip(reason="not implemented yet")
def test_interpolate_to_points_recovers_linear_field():
    """interpolate_to_points: exact values on a synthetic in-memory linear field; NaN outside bbox/time window; depth dim handled."""


@pytest.mark.skip(reason="not implemented yet")
def test_execute_reuses_cached_subset():
    """execute: second call with identical arguments returns the cached path without calling copernicusmarine.subset again (monkeypatched)."""


@pytest.mark.skip(reason="not implemented yet")
def test_enrich_adds_variable_columns():
    """enrich: monkeypatched retrieval returning a synthetic netCDF in tmp_path; output has uo/vo columns joined onto the input points."""
