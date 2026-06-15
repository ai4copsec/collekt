"""Tests for collekt.datasources.geonorge.  Offline so no network.

Network-dependent stages are skipped placeholders for now.
"""
import json
import types

import geopandas as gpd
import pytest
from shapely.geometry import Point

from collekt.datasources import geonorge
from collekt.datasources.geonorge import (
    DatasetRecord,
    _await_order,
    _bbox_ring,
    _pick_format,
    annotate,
    bbox_intersects,
    build_order,
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


_BBOX = (4.63, 60.82, 5.37, 61.18)  # Vestland coast
_FORMATS = [{"name": "SOSI"}, {"name": "GML"}, {"name": "FGDB"}]
_PROJECTIONS = [{"code": "25832", "name": "EUREF89 UTM sone 32, 2d"},
                {"code": "25833", "name": "EUREF89 UTM sone 33, 2d"}]


def test_bbox_ring_is_closed_with_five_pairs():
    ring = _bbox_ring(_BBOX)
    values = ring.split()
    assert len(values) == 10
    assert values[0:2] == values[8:10]  # closed ring
    northings = [float(v) for v in values[1::2]]
    assert all(6_700_000 < y < 6_850_000 for y in northings)  # ~61N in UTM33


def test_build_order_with_polygon_clip():
    order = build_order("uuid-1", _BBOX, {"supportsPolygonSelection": True},
                        _FORMATS, _PROJECTIONS, [], "user@example.com")
    line = order["orderLines"][0]
    assert order["email"] == "user@example.com"
    assert line["metadataUuid"] == "uuid-1"
    assert line["formats"] == [{"name": "GML"}]  # OGR-readable preferred over SOSI
    assert line["projections"][0]["code"] == "25833"
    assert line["coordinatesystem"] == "25833"  # lowercase 's', unlike can-download
    assert line["coordinates"].count(" ") == 9


def test_build_order_national_fallback():
    areas = [{"type": "fylke", "name": "Agder", "code": "42"},
             {"type": "landsdekkende", "name": "Hele landet", "code": "0000"}]
    order = build_order("uuid-1", _BBOX, {"supportsPolygonSelection": False},
                        _FORMATS, _PROJECTIONS, areas, "user@example.com")
    line = order["orderLines"][0]
    assert "coordinates" not in line
    assert line["areas"] == [{"code": "0000", "name": "Hele landet",
                              "type": "landsdekkende"}]


def test_pick_format_rejects_sosi_only():
    with pytest.raises(RuntimeError, match="SOSI"):
        _pick_format([{"name": "SOSI"}])


def test_await_order_polls_until_ready(monkeypatch):
    pending = {"referenceNumber": "r1",
               "files": [{"name": "a.zip", "status": "WaitingForProcessing"}]}
    ready = {"referenceNumber": "r1",
             "files": [{"name": "a.zip", "status": "ReadyForDownload"}]}
    polled = []

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return ready

    monkeypatch.setattr(geonorge, "_session",
                        types.SimpleNamespace(get=lambda url, **kw: (polled.append(url), _Resp())[1]))
    monkeypatch.setattr(geonorge.time, "sleep", lambda s: None)

    files = _await_order(pending, auth=None)
    assert files[0]["status"] == "ReadyForDownload"
    assert polled and polled[0].endswith("/order/r1")


def test_await_order_raises_on_failed_file():
    receipt = {"referenceNumber": "r1", "files": [{"name": "a.zip", "status": "Error"}]}
    with pytest.raises(RuntimeError, match="a.zip"):
        _await_order(receipt, auth=None)


def test_await_order_explains_401_on_status_poll(monkeypatch):
    # some datasets gate the order-status endpoint behind a Geonorge login
    pending = {"referenceNumber": "r1",
               "files": [{"name": "a.zip", "status": "WaitingForProcessing"}]}
    resp = types.SimpleNamespace(status_code=401)
    monkeypatch.setattr(geonorge, "_session",
                        types.SimpleNamespace(get=lambda url, **kw: resp))
    monkeypatch.setattr(geonorge.time, "sleep", lambda s: None)
    with pytest.raises(geonorge.RestrictedDatasetError, match="GEONORGE_USERNAME"):
        _await_order(pending, auth=None)


def test_crop_reads_all_layers(tmp_path):
    src = tmp_path / "multi.gpkg"
    for layer, point in [("layer_a", Point(10.5, 63.5)), ("layer_b", Point(10.6, 63.6))]:
        gpd.GeoDataFrame({"name": [layer]}, geometry=[point],
                         crs="EPSG:4326").to_file(src, layer=layer, driver="GPKG")

    cropped = crop([src], bbox=(10.0, 63.0, 11.0, 64.0))
    assert sorted(cropped["name"]) == ["layer_a", "layer_b"]


@pytest.mark.skip(reason="network: not implemented yet")
def test_search_datasets():
    """search_datasets"""


class _FakeResp:
    def __init__(self, body: bytes, ok: bool = True, status_code: int = 200):
        self.content = body
        self.text = body.decode("utf-8", "replace")
        self.ok = ok
        self.status_code = status_code


def _geojson_bytes(n: int) -> bytes:
    fc = {"type": "FeatureCollection",
          "features": [{"type": "Feature", "properties": {"i": i},
                        "geometry": {"type": "Point", "coordinates": [5.0, 61.0]}}
                       for i in range(n)]}
    return json.dumps(fc).encode("utf-8")


class _PagingSession:
    """Serves GeoJSON slices with startIndex/count, records every request."""

    def __init__(self, total: int):
        self.total = total
        self.calls: list[dict] = []

    def get(self, url, params=None, timeout=None, **kw):
        params = dict(params or {})
        self.calls.append(params)
        start = int(params.get("startIndex", 0))
        count = int(params.get("count", 0))
        n = max(0, min(count, self.total - start))
        return _FakeResp(_geojson_bytes(n))


def test_safe_name_strips_path_unsafe_chars():
    assert ":" not in geonorge._safe_name("app:Type")
    assert "/" not in geonorge._safe_name("a/b")
    assert geonorge._safe_name("Sjøkart:Dybdedata") == "sjoekart_dybdedata"


@pytest.mark.parametrize("total, expected_files, expected_starts", [
    (0, 0, [0]),        # empty: one request
    (1, 1, [0]),        # short first page
    (2, 1, [0, 2]),     # exactly one full page
    (3, 2, [0, 2]),     # full page + short page
    (4, 2, [0, 2, 4]),  # exact multiple of page size
])
def test_wfs_pages_termination(tmp_path, monkeypatch, total, expected_files, expected_starts):
    monkeypatch.setattr(geonorge, "WFS_PAGE_SIZE", 2)
    session = _PagingSession(total)
    monkeypatch.setattr(geonorge, "_session", session)

    paths = geonorge._wfs_pages("http://wfs.test", "app:Type", "bbox", tmp_path)
    assert len(paths) == expected_files
    assert [c.get("startIndex") for c in session.calls] == expected_starts
    assert sorted(tmp_path.iterdir()) == sorted(paths)


class _NegotiationSession:
    """similar to the one I found online """

    def __init__(self, geojson_ok: bool):
        self.geojson_ok = geojson_ok
        self.calls: list[dict] = []

    def get(self, url, params=None, timeout=None, **kw):
        params = dict(params or {})
        self.calls.append(params)
        if "outputFormat" in params and self.geojson_ok:
            return _FakeResp(_geojson_bytes(1))
        # GeoJSON refused or GML retry: return XML
        return _FakeResp(b"<?xml version='1.0'?><wfs:FeatureCollection/>")


@pytest.mark.parametrize("geojson_ok, expected_requests, expected_suffix", [
    (True, 1, ".json"),
    (False, 2, ".gml"),
])
def test_wfs_pages_format_negotiation(tmp_path, monkeypatch, geojson_ok,
                                      expected_requests, expected_suffix):
    monkeypatch.setattr(geonorge, "WFS_PAGE_SIZE", 1000)         # one page suffices
    monkeypatch.setattr(geonorge, "_feature_count", lambda path: 1)  # isolate from GDAL
    session = _NegotiationSession(geojson_ok)
    monkeypatch.setattr(geonorge, "_session", session)

    paths = geonorge._wfs_pages("http://wfs.test", "app:Type", "bbox", tmp_path)

    assert len(session.calls) == expected_requests
    assert "outputFormat" in session.calls[0]            # check GeoJSON first
    if not geojson_ok:
        assert "outputFormat" not in session.calls[1]    # retry without it (GML)
    assert [p.suffix for p in paths] == [expected_suffix]


def test_fetch_wfs_falls_back_to_second_axis_order(tmp_path, monkeypatch):
    monkeypatch.setattr(geonorge, "wfs_feature_types", lambda url: ["app:Type"])
    seen_axes = []

    def fake_pages(url, type_name, axis_bbox, out_dir):
        seen_axes.append(axis_bbox)
        if "urn:ogc" in axis_bbox:          # first (urn lat/lon) order -> nothing
            return []
        return [out_dir / "page_0.json"]    # legacy lon/lat order -> features

    monkeypatch.setattr(geonorge, "_wfs_pages", fake_pages)

    record = _record(distribution_url="https://wfs.test/wfs?service=wfs&request=getcapabilities")
    paths = geonorge.fetch_wfs(record, _BBOX, tmp_path)

    assert len(seen_axes) == 2              # trying both axis orders
    assert "urn:ogc" in seen_axes[0]        # lat/lon attempted first
    assert paths == [tmp_path / "raw" / record.uuid / "page_0.json"]


@pytest.mark.skip(reason="network: not implemented yet")
def test_execute_writes_manifest_with_every_dataset_status():
    """test execute(). discover/fetch; manifest.json listing all records"""
