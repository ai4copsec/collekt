"""Offline tests for the Geonorge adapter.

The WFS and order-API boundaries are stubbed, so no network or credentials are
needed.
"""

import json
import types
from pathlib import Path

import geopandas as gpd
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
    "protocol": "wfs",
    "endpoint": "https://wfs.test/wfs",
    "type_names": ["app:A", "app:B"],
}

DOWNLOAD_SOURCE = {
    "kind": "geonorge",
    "path": "geonorge",
    "filename_pattern": "{source}_{bbox_hash}.parquet",
    "dataset_id": "uuid-download",
    "protocol": "download",
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
        assert source.raw["protocol"] in {"wfs", "download"}
        assert source.url.endswith(source.dataset_id)
        if source.raw["protocol"] == "wfs":
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


def test_diagnose_warns_without_email(monkeypatch):
    monkeypatch.delenv(geonorge.EMAIL_ENV, raising=False)
    assert geonorge.diagnose(_cfg(Path("/tmp")), False)[0].status == "warn"
    monkeypatch.setenv(geonorge.EMAIL_ENV, "user@example.com")
    assert geonorge.diagnose(_cfg(Path("/tmp")), False)[0].status == "ok"


# --- plan -------------------------------------------------------------------


def test_plan_reports_wfs_request_without_network(tmp_path):
    result = Fetcher(_request(), config=_cfg(tmp_path, g=WFS_SOURCE)).plan()

    assert result.summary.planned == 1
    details = result.results[0].details
    assert details["method"] == "wfs"
    assert details["request"]["type_names"] == ["app:A", "app:B"]
    assert details["request"]["bbox"].startswith("60.82,4.63,61.18,5.37")  # lat/lon axis order
    assert result.results[0].path.name.startswith("g_")


def test_plan_reports_order_request(tmp_path):
    result = Fetcher(_request(), config=_cfg(tmp_path, g=DOWNLOAD_SOURCE)).plan()
    assert result.results[0].details["request"]["metadataUuid"] == "uuid-download"


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


def test_fetch_restricted_dataset_is_skipped(tmp_path, monkeypatch):
    def restricted(source, bbox, out_dir):
        raise geonorge.RestrictedDatasetError(geonorge.RESTRICTED_MESSAGE)

    monkeypatch.setattr(geonorge, "fetch_download", restricted)
    result = Fetcher(_request(), config=_cfg(tmp_path, g=DOWNLOAD_SOURCE)).download()

    assert result.summary.skipped == 1
    assert "GEONORGE_USERNAME" in result.results[0].message


def test_fetch_download_without_email_is_skipped(tmp_path, monkeypatch):
    monkeypatch.delenv(geonorge.EMAIL_ENV, raising=False)
    result = Fetcher(_request(), config=_cfg(tmp_path, g=DOWNLOAD_SOURCE)).download()

    assert result.summary.skipped == 1
    assert geonorge.EMAIL_ENV in result.results[0].message


def test_fetch_provider_error_is_a_failure(tmp_path, monkeypatch):
    def broken(source, bbox, out_dir):
        raise RuntimeError("WFS GetFeature failed")

    monkeypatch.setattr(geonorge, "fetch_wfs", broken)
    result = Fetcher(_request(), config=_cfg(tmp_path, g=WFS_SOURCE)).download()

    assert result.summary.failed == 1
    assert not list(tmp_path.rglob("*.parquet"))


def test_write_geoparquet_stringifies_mixed_object_columns(tmp_path):
    gdf = gpd.GeoDataFrame({"mixed": [1, "a"]}, geometry=[Point(0, 0), Point(1, 1)], crs="EPSG:4326")
    path = tmp_path / "out.parquet"
    geonorge._write_geoparquet(gdf, path)

    assert gpd.read_parquet(path)["mixed"].tolist() == ["1", "a"]
    assert not path.with_name("out.parquet.part").exists()


# --- crop -------------------------------------------------------------------


def test_crop_clips_to_bbox(tmp_path):
    src = _write_points(tmp_path / "features.json", {"inside": Point(10.5, 63.5), "outside": Point(20, 70)})
    cropped = geonorge.crop([src], bbox=(10.0, 63.0, 11.0, 64.0))
    assert cropped["name"].tolist() == ["inside"]


def test_crop_empty_input_gives_empty_frame():
    assert geonorge.crop([], bbox=(10.0, 63.0, 11.0, 64.0)).empty


def test_crop_reads_all_layers(tmp_path):
    src = tmp_path / "multi.gpkg"
    for layer, point in [("layer_a", Point(10.5, 63.5)), ("layer_b", Point(10.6, 63.6))]:
        _write_points(src, {layer: point}, layer=layer, driver="GPKG")
    cropped = geonorge.crop([src], bbox=(10.0, 63.0, 11.0, 64.0))
    assert sorted(cropped["name"]) == ["layer_a", "layer_b"]


def test_crop_reprojects_and_skips_shapefile_sidecars(tmp_path):
    gdf = gpd.GeoDataFrame({"name": ["inside"]}, geometry=[Point(10.5, 63.5)], crs="EPSG:4326")
    shp = tmp_path / "utm.shp"
    gdf.to_crs(25833).to_file(shp)
    paths = sorted(tmp_path.iterdir())  # .shp plus its .dbf/.shx/.prj/.cpg sidecars
    cropped = geonorge.crop(paths, bbox=(10.0, 63.0, 11.0, 64.0))
    assert cropped["name"].tolist() == ["inside"]  # once, not once per sidecar
    assert cropped.crs.to_epsg() == 4326


# --- WFS paging -------------------------------------------------------------


class _FakeResponse:
    def __init__(self, body: bytes, ok: bool = True):
        self.content = body
        self.text = body.decode("utf-8", "replace")
        self.ok = ok


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


def test_wfs_pages_raise_on_exception_report(tmp_path, monkeypatch):
    response = _FakeResponse(b"<ows:ExceptionReport>bad type</ows:ExceptionReport>")
    monkeypatch.setattr(geonorge, "_session", types.SimpleNamespace(get=lambda *a, **k: response))
    with pytest.raises(RuntimeError, match="app:Type"):
        geonorge._wfs_pages("http://wfs.test", "app:Type", {"count": 1000}, tmp_path)


# --- order API --------------------------------------------------------------

FORMATS = [{"name": "SOSI"}, {"name": "GML"}, {"name": "FGDB"}]
PROJECTIONS = [
    {"code": "25832", "name": "EUREF89 UTM sone 32, 2d"},
    {"code": "25833", "name": "EUREF89 UTM sone 33, 2d"},
]


def test_bbox_ring_is_closed_with_five_pairs():
    values = geonorge._bbox_ring(BBOX).split()
    assert len(values) == 10
    assert values[0:2] == values[8:10]
    northings = [float(v) for v in values[1::2]]
    assert all(6_700_000 < y < 6_850_000 for y in northings)  # ~61N in UTM33


def test_build_order_with_polygon_clip():
    order = geonorge.build_order(
        "uuid-1", BBOX, {"supportsPolygonSelection": True}, FORMATS, PROJECTIONS, [], "user@example.com"
    )
    line = order["orderLines"][0]
    assert order["email"] == "user@example.com"
    assert line["metadataUuid"] == "uuid-1"
    assert line["formats"] == [{"name": "GML"}]  # OGR-readable preferred over SOSI
    assert line["projections"][0]["code"] == "25833"
    assert line["coordinatesystem"] == "25833"
    assert line["coordinates"].count(" ") == 9


def test_build_order_national_fallback():
    areas = [
        {"type": "fylke", "name": "Agder", "code": "42"},
        {"type": "landsdekkende", "name": "Hele landet", "code": "0000"},
    ]
    order = geonorge.build_order(
        "uuid-1", BBOX, {"supportsPolygonSelection": False}, FORMATS, PROJECTIONS, areas, "user@example.com"
    )
    line = order["orderLines"][0]
    assert "coordinates" not in line
    assert line["areas"] == [{"code": "0000", "name": "Hele landet", "type": "landsdekkende"}]


def test_pick_format_rejects_sosi_only():
    with pytest.raises(RuntimeError, match="SOSI"):
        geonorge._pick_format([{"name": "SOSI"}])


def test_restricted_order_is_raised_as_restricted(monkeypatch):
    monkeypatch.setenv(geonorge.EMAIL_ENV, "user@example.com")
    monkeypatch.setattr(geonorge, "_nedlasting", lambda path: {} if path.startswith("capabilities") else [])
    monkeypatch.setattr(geonorge, "build_order", lambda *args: {"orderLines": []})
    refused = types.SimpleNamespace(
        ok=False, text='"Order contains restricted datasets, but user does not have required role"'
    )
    monkeypatch.setattr(geonorge, "_session", types.SimpleNamespace(post=lambda *a, **k: refused))
    source = _cfg(Path("/tmp"), g=DOWNLOAD_SOURCE).sources["g"]

    with pytest.raises(geonorge.RestrictedDatasetError, match="required role"):
        geonorge.fetch_download(source, BBOX, Path("/tmp"))


def test_await_order_polls_until_ready(monkeypatch):
    pending = {"referenceNumber": "r1", "files": [{"name": "a.zip", "status": "WaitingForProcessing"}]}
    ready = {"referenceNumber": "r1", "files": [{"name": "a.zip", "status": "ReadyForDownload"}]}
    polled = []
    response = types.SimpleNamespace(status_code=200, raise_for_status=lambda: None, json=lambda: ready)
    monkeypatch.setattr(
        geonorge, "_session", types.SimpleNamespace(get=lambda url, **kw: (polled.append(url), response)[1])
    )
    monkeypatch.setattr(geonorge.time, "sleep", lambda s: None)

    files = geonorge._await_order(pending, auth=None)

    assert files[0]["status"] == "ReadyForDownload"
    assert polled[0].endswith("/order/r1")


def test_await_order_raises_on_failed_file():
    receipt = {"referenceNumber": "r1", "files": [{"name": "a.zip", "status": "Error"}]}
    with pytest.raises(RuntimeError, match="a.zip"):
        geonorge._await_order(receipt, auth=None)


def test_await_order_explains_401_on_status_poll(monkeypatch):
    pending = {"referenceNumber": "r1", "files": [{"name": "a.zip", "status": "WaitingForProcessing"}]}
    response = types.SimpleNamespace(status_code=401)
    monkeypatch.setattr(geonorge, "_session", types.SimpleNamespace(get=lambda url, **kw: response))
    monkeypatch.setattr(geonorge.time, "sleep", lambda s: None)
    with pytest.raises(geonorge.RestrictedDatasetError, match="GEONORGE_USERNAME"):
        geonorge._await_order(pending, auth=None)


def test_unzip_detects_archives_without_an_extension(tmp_path):
    import zipfile

    archive = tmp_path / "clipped"  # clipped order files arrive without an extension
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("data.gml", "<gml/>")
        handle.writestr("db.gdb/a0000001.gdbtable", "x")
        handle.writestr("db.gdb/a0000002.gdbtable", "y")
    out = geonorge._unzip_all([archive], tmp_path / "unzipped")
    assert sorted(p.name for p in out) == ["data.gml", "db.gdb"]
