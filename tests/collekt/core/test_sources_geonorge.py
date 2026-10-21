"""Offline tests for the Geonorge adapter.

The WFS and order-API boundaries are stubbed, so no network or credentials are
needed.
"""

import json
import types
from pathlib import Path

import geopandas as gpd
import pyogrio
import pytest
from shapely.geometry import Point

import collekt
from collekt import Fetcher
from collekt.core.config import get_config
from collekt.core.doctor import run_doctor
from collekt.core.request import Region, Request
from collekt.sources import geonorge

BBOX = (4.63, 60.82, 5.37, 61.18)  # Vestland coast: (west, south, east, north)

WFS_SOURCE = {
    "kind": "geonorge",
    "path": "geonorge",
    "filename_pattern": "{source}_{bbox_hash}.parquet",
    "dataset_id": "uuid-wfs",
    "endpoint": "https://wfs.test/wfs",
    "type_names": ["app:A", "app:B"],
}


def _cfg(tmp_path, **sources):
    return get_config(overrides={"source_catalogs": [], "output": {"root": str(tmp_path)}, "sources": sources})


def _request():
    west, south, east, north = BBOX
    return Request(region=Region(west=west, east=east, south=south, north=north), start="2024-01-30")


def _write_points(path: Path, points: dict[str, Point], **kwargs) -> Path:
    gdf = gpd.GeoDataFrame({"name": list(points)}, geometry=list(points.values()), crs="EPSG:4326")
    gdf.to_file(path, **kwargs)
    return path


# --- catalog & selection ----------------------------------------------------


def test_bundled_catalog_entries_are_well_formed():
    catalog = collekt.available_datasets()
    keys = [key for key, meta in catalog.items() if meta["provider"] == "geonorge"]
    assert len(keys) == 10
    config = get_config()
    for key in keys:
        source = config.sources[key]
        assert source.url.endswith(source.dataset_id)
        assert source.raw["endpoint"].startswith("https://")
        assert source.raw["type_names"]


def test_selection_resolves_a_catalog_key():
    config = collekt.DatasetConfig(collekt.Geonorge("geonorge_fairways")).resolve()
    assert config.sources["geonorge_fairways"].kind == "geonorge"


def test_selection_round_trips_through_yaml_form():
    selection = collekt.DatasetConfig(collekt.Geonorge("geonorge_aquaculture"))
    assert collekt.DatasetConfig.from_dict(selection.as_dict()) == selection


def test_doctor_does_not_flag_geonorge_raw_keys():
    checks = run_doctor(config=_cfg(Path("/tmp"), g=WFS_SOURCE))
    assert "source g config keys" not in {check.name for check in checks}


def test_bundled_catalog_entries_cover_norwegian_waters_only():
    config = get_config()
    for key, source in config.sources.items():
        if source.kind == "geonorge":
            coverage = source.raw["coverage"]
            assert coverage["longitude"][0] <= 4.63 and coverage["latitude"][0] <= 57.9, key  # southern Norway
            assert coverage["latitude"][1] >= 80.0, key  # Svalbard


# --- coverage ---------------------------------------------------------------

COVERED_SOURCE = {**WFS_SOURCE, "coverage": {"longitude": [-10.0, 37.0], "latitude": [56.0, 82.0]}}
IRELAND = Region(west=-10.7, east=-10.0, south=53.2, north=53.7)


def test_fetch_outside_coverage_is_skipped_without_network(tmp_path, monkeypatch):
    monkeypatch.setattr(geonorge, "fetch_wfs", lambda *a: pytest.fail("must not query Geonorge"))
    request = Request(region=IRELAND, start="2024-01-30")
    result = Fetcher(request, config=_cfg(tmp_path, g=COVERED_SOURCE)).download()

    assert result.summary.skipped == 1
    assert "outside the available coverage" in result.results[0].message


def test_fetch_inside_coverage_still_queries(tmp_path, monkeypatch):
    def fake_fetch_wfs(source, bbox, out_dir):
        return [_write_points(out_dir / "page.json", {"inside": Point(5.0, 61.0)})]

    monkeypatch.setattr(geonorge, "fetch_wfs", fake_fetch_wfs)
    result = Fetcher(_request(), config=_cfg(tmp_path, g=COVERED_SOURCE)).download()

    assert result.summary.downloaded == 1


def test_plan_reports_coverage_verdict(tmp_path):
    outside = Fetcher(Request(region=IRELAND, start="2024-01-30"), config=_cfg(tmp_path, g=COVERED_SOURCE)).plan()
    inside = Fetcher(_request(), config=_cfg(tmp_path, g=COVERED_SOURCE)).plan()

    assert outside.results[0].status == "skipped"
    assert outside.results[0].details["availability"]["status"] == "unavailable"
    assert inside.results[0].status == "planned"
    assert inside.results[0].details["availability"]["status"] == "available"


# --- plan -------------------------------------------------------------------


def test_plan_reports_wfs_request_without_network(tmp_path):
    result = Fetcher(_request(), config=_cfg(tmp_path, g=WFS_SOURCE)).plan()

    assert result.summary.planned == 1
    details = result.results[0].details
    assert details["method"] == "wfs"
    assert details["request"]["type_names"] == ["app:A", "app:B"]
    assert details["request"]["bbox"].startswith("60.82,4.63,61.18,5.37")  # lat/lon axis order
    assert result.results[0].path.name.startswith("g_")


# --- fetch ------------------------------------------------------------------


def test_fetch_writes_cropped_geoparquet(tmp_path, monkeypatch):
    def fake_fetch_wfs(source, bbox, out_dir):
        return [_write_points(out_dir / "page.json", {"inside": Point(5.0, 61.0), "outside": Point(20, 70)})]

    monkeypatch.setattr(geonorge, "fetch_wfs", fake_fetch_wfs)
    result = Fetcher(_request(), config=_cfg(tmp_path, g=WFS_SOURCE)).download()

    assert result.summary.downloaded == 1
    written = gpd.read_parquet(result.results[0].path)
    assert written["name"].tolist() == ["inside"]
    assert result.results[0].details["features"] == 1


def test_fetch_reuses_existing_file(tmp_path, monkeypatch):
    def fake_fetch_wfs(source, bbox, out_dir):
        return [_write_points(out_dir / "page.json", {"inside": Point(5.0, 61.0)})]

    monkeypatch.setattr(geonorge, "fetch_wfs", fake_fetch_wfs)
    Fetcher(_request(), config=_cfg(tmp_path, g=WFS_SOURCE)).download()
    monkeypatch.setattr(geonorge, "fetch_wfs", lambda *a: pytest.fail("should reuse the cached file"))
    result = Fetcher(_request(), config=_cfg(tmp_path, g=WFS_SOURCE)).download()

    assert result.results[0].status == "reused"


def test_fetch_with_no_features_in_region_is_a_warning(tmp_path, monkeypatch):
    def fake_fetch_wfs(source, bbox, out_dir):
        return [_write_points(out_dir / "page.json", {"outside": Point(20, 70)})]

    monkeypatch.setattr(geonorge, "fetch_wfs", fake_fetch_wfs)
    result = Fetcher(_request(), config=_cfg(tmp_path, g=WFS_SOURCE)).download()

    assert result.summary.skipped == 1
    assert "no features" in result.results[0].message


def test_fetch_provider_error_is_a_failure(tmp_path, monkeypatch):
    def broken(source, bbox, out_dir):
        raise RuntimeError("WFS GetFeature failed")

    monkeypatch.setattr(geonorge, "fetch_wfs", broken)
    result = Fetcher(_request(), config=_cfg(tmp_path, g=WFS_SOURCE)).download()

    assert result.summary.failed == 1
    assert not list(tmp_path.rglob("*.parquet"))


def test_write_geoparquet_stringifies_mixed_object_columns(tmp_path):
    points = [Point(0, 0), Point(1, 1), Point(2, 2)]
    gdf = gpd.GeoDataFrame({"mixed": [1, "a", float("nan")]}, geometry=points, crs="EPSG:4326")
    path = tmp_path / "out.parquet"
    geonorge._write_geoparquet(gdf, path)

    assert gpd.read_parquet(path)["mixed"].tolist() == ["1", "a", None]  # missing stays missing, not "nan"
    assert not path.with_name("out.parquet.part").exists()


# --- crop -------------------------------------------------------------------


def test_crop_clips_to_bbox(tmp_path):
    src = _write_points(tmp_path / "features.json", {"inside": Point(10.5, 63.5), "outside": Point(20, 70)})
    cropped = geonorge.crop([src], bbox=(10.0, 63.0, 11.0, 64.0))
    assert cropped["name"].tolist() == ["inside"]


def test_crop_empty_input_gives_empty_frame():
    assert geonorge.crop([], bbox=(10.0, 63.0, 11.0, 64.0)).empty


# --- WFS paging -------------------------------------------------------------


class _FakeResponse:
    def __init__(self, body: bytes, ok: bool = True):
        self.content = body
        self.text = body.decode("utf-8", "replace")
        self.ok = ok
        self.status_code = 200 if ok else 500


def _geojson_bytes(n: int) -> bytes:
    features = [
        {"type": "Feature", "properties": {"i": i}, "geometry": {"type": "Point", "coordinates": [5.0, 61.0]}}
        for i in range(n)
    ]
    return json.dumps({"type": "FeatureCollection", "features": features}).encode("utf-8")


class _PagingSession:
    """Serves GeoJSON slices by startIndex/count and records every request."""

    def __init__(self, total: int):
        self.total = total
        self.calls: list[dict] = []

    def get(self, url, params=None, timeout=None, **kwargs):
        params = dict(params or {})
        self.calls.append(params)
        start, count = int(params.get("startIndex", 0)), int(params["count"])
        return _FakeResponse(_geojson_bytes(max(0, min(count, self.total - start))))


@pytest.mark.parametrize(
    "total, expected_files, expected_starts",
    [
        (0, 0, [None]),  # empty: one request
        (1, 1, [None]),  # short first page
        (2, 1, [None, 2]),  # exactly one full page
        (3, 2, [None, 2]),  # full page + short page
        (4, 2, [None, 2, 4]),  # exact multiple of the page size
    ],
)
def test_wfs_pages_termination(tmp_path, monkeypatch, total, expected_files, expected_starts):
    monkeypatch.setattr(geonorge, "WFS_PAGE_SIZE", 2)
    session = _PagingSession(total)
    monkeypatch.setattr(geonorge, "_session", session)

    base = {"count": 2}
    paths = geonorge._wfs_pages("http://wfs.test", "app:Type", base, tmp_path)

    assert len(paths) == expected_files
    # the first page omits startIndex: some servers time out on an explicit 0
    assert [call.get("startIndex") for call in session.calls] == expected_starts
    assert sorted(tmp_path.iterdir()) == sorted(paths)


def test_wfs_pages_log_status_time_size_and_features(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(geonorge, "_session", _PagingSession(3))
    monkeypatch.setattr(geonorge, "WFS_PAGE_SIZE", 2)
    with caplog.at_level("INFO", logger=geonorge.logger.name):
        geonorge._wfs_pages("https://wfs.test/wfs", "app:Type", {"count": 2}, tmp_path)
    lines = [record.getMessage() for record in caplog.records]

    assert lines[0] == "GetFeature app:Type page 1 from wfs.test"
    assert lines[1].startswith("app:Type page 1: HTTP 200 in ") and lines[1].endswith("KB, 2 features")
    assert lines[3].endswith("1 features")


def test_wfs_pages_raise_on_unreadable_page(tmp_path, monkeypatch):
    login_page = _FakeResponse(b"<!doctype html><html><body>log in</body></html>")  # HTTP 200, not data
    monkeypatch.setattr(geonorge, "_session", types.SimpleNamespace(get=lambda *a, **k: login_page))
    with pytest.raises(pyogrio.errors.DataSourceError):
        geonorge._wfs_pages("http://wfs.test", "app:Type", {"count": 1000}, tmp_path)


def test_wfs_pages_raise_on_exception_report(tmp_path, monkeypatch):
    response = _FakeResponse(b"<ows:ExceptionReport>bad type</ows:ExceptionReport>")
    monkeypatch.setattr(geonorge, "_session", types.SimpleNamespace(get=lambda *a, **k: response))
    with pytest.raises(RuntimeError, match="app:Type"):
        geonorge._wfs_pages("http://wfs.test", "app:Type", {"count": 1000}, tmp_path)
