"""Tests for collekt.datasources.geonorge.  Offline so no network.

Network-dependent stages are skipped placeholders for now. 
"""
import geopandas as gpd
import pytest
from shapely.geometry import Point

from collekt.datasources.geonorge import (
    DatasetRecord,
    annotate,
    bbox_intersects,
    classify,
    crop,
    parse_coverage_bbox,
    window_bbox,
)


def _record(**overrides) -> DatasetRecord:
    fields = dict(
        uuid="e106adf4-c9d8-4fce-a9b5-7886a4126d23",
        title="Norges maritime grenser",
        organization="Kartverket",
        protocol="OGC:WFS",
        kind="wfs",
        distribution_url="https://wfs.example/maritime",
        detail_url="https://kartkatalog.geonorge.no/metadata/uuid/e106adf4",
        is_open=True,
        licence_url="http://creativecommons.org/licenses/by/3.0/no/",
    )
    fields.update(overrides)
    return DatasetRecord(**fields)


def test_parse_coverage_bbox_norwegian_locale():
    # real shape of /api/getdata BoundingBox: comma decimals, U+2212 minus
    metadata = {"BoundingBox": {
        "WestBoundLongitude": "−16,00",
        "SouthBoundLatitude": "−56,00",
        "EastBoundLongitude": "45,00",
        "NorthBoundLatitude": "85,00",
    }}
    assert parse_coverage_bbox(metadata) == (-16.0, -56.0, 45.0, 85.0)


def test_parse_coverage_bbox_missing_or_partial_is_none():
    assert parse_coverage_bbox({}) is None
    assert parse_coverage_bbox({"BoundingBox": {"WestBoundLongitude": "1,0"}}) is None
    assert parse_coverage_bbox({"BoundingBox": {
        "WestBoundLongitude": "abc", "SouthBoundLatitude": "1",
        "EastBoundLongitude": "2", "NorthBoundLatitude": "3",
    }}) is None


def test_classify_protocols():
    assert classify("OGC:WFS") == "wfs"
    assert classify("GEONORGE:DOWNLOAD") == "download"
    assert classify("OGC:WMS") == "skip"
    assert classify("GEONORGE:FILEDOWNLOAD") == "skip"
    assert classify("") == "skip"


def test_window_bbox_center_inside_and_buffer_widens():
    inner = window_bbox(70.0, 19.0, radius_km=50.0, buffer_km=0.0)
    outer = window_bbox(70.0, 19.0, radius_km=50.0, buffer_km=25.0)
    assert inner[0] < 19.0 < inner[2]
    assert inner[1] < 70.0 < inner[3]
    assert outer[0] < inner[0] and outer[1] < inner[1]
    assert outer[2] > inner[2] and outer[3] > inner[3]


def test_bbox_intersects():
    assert bbox_intersects((0, 0, 2, 2), (1, 1, 3, 3))
    assert bbox_intersects((0, 0, 2, 2), (2, 2, 3, 3))  # touching counts
    assert not bbox_intersects((0, 0, 1, 1), (2, 2, 3, 3))


def test_crop_clips_to_bbox(tmp_path):
    gdf = gpd.GeoDataFrame(
        {"name": ["inside", "outside"]},
        geometry=[Point(10.5, 63.5), Point(20.0, 70.0)],
        crs="EPSG:4326",
    )
    src = tmp_path / "features.json"
    gdf.to_file(src, driver="GeoJSON")

    cropped = crop([src], bbox=(10.0, 63.0, 11.0, 64.0))
    assert len(cropped) == 1
    assert cropped.iloc[0]["name"] == "inside"


def test_crop_empty_input_gives_empty_frame():
    cropped = crop([], bbox=(10.0, 63.0, 11.0, 64.0))
    assert cropped.empty


def test_annotate_adds_provenance_to_every_feature():
    record = _record()
    gdf = gpd.GeoDataFrame(geometry=[Point(1, 2), Point(3, 4)], crs="EPSG:4326")

    annotated = annotate(gdf, record)

    assert len(annotated) == 2
    for column, expected in [
        ("collekt_dataset_uuid", record.uuid),
        ("collekt_dataset_title", record.title),
        ("collekt_organization", record.organization),
        ("collekt_protocol", record.protocol),
        ("collekt_distribution_url", record.distribution_url),
        ("collekt_licence_url", record.licence_url),
        ("collekt_access", "open"),
        ("collekt_detail_url", record.detail_url),
    ]:
        assert (annotated[column] == expected).all()
    assert annotated["collekt_retrieved_at"].notna().all()
    assert gdf.columns.tolist() == ["geometry"]  # input untouched


@pytest.mark.skip(reason="network: not implemented yet")
def test_search_datasets():
    """search_datasets"""


@pytest.mark.skip(reason="network: not implemented yet")
def test_fetch_wfs_falls_back_on_axis_order_and_output_format():
    """ testing fetch WFS. GML's uses lon/lat-convention unfortunately, so we do need a check for this..."""


@pytest.mark.skip(reason="network: not implemented yet")
def test_execute_writes_manifest_with_every_dataset_status():
    """test execute(). discover/fetch; manifest.json lists all records incl. skipped/empty/deferred reasons."""
