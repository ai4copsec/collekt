"""Offline tests for the feature/file source adapters.

Skytruth, HOZINT, GFW, and Copernicus Data Space are per-request sources. Their
network/tooling boundaries are stubbed, so no credentials or downloads are
needed. Skytruth's and GFW's Parquet-writing paths need damast and are covered
separately when it's installed.
"""

import asyncio
import datetime
import json
import types
import warnings
from pathlib import Path

import pytest
import requests

from collekt import Fetcher
from collekt.core.config import get_config
from collekt.core.naming import bbox_hash as bbox_hash_of
from collekt.core.request import Region, Request
from collekt.sources import copernicus_dataspace, gfw, hozint, skytruth


def _cfg(tmp_path, **sources):
    return get_config(overrides={"source_catalogs": [], "output": {"root": str(tmp_path)}, "sources": sources})


def _request(**kwargs):
    kwargs.setdefault("region", Region.from_bbox((-6, 20, 35, 45)))
    kwargs.setdefault("start", "2024-01-30")
    return Request(**kwargs)


# --- Skytruth ---------------------------------------------------------------

SKYTRUTH = {
    "kind": "skytruth",
    "enabled": True,
    "path": "skytruth",
    "filename_pattern": "skytruth_{start:%Y%m%d}_{bbox_hash}.parquet",
    "limit": 1000,
}


def _skytruth_source():
    return _cfg(Path("/tmp"), skytruth_slicks=SKYTRUTH).sources["skytruth_slicks"]


def test_skytruth_query_parameters_use_bbox_and_datetime():
    params = skytruth._query_parameters(_request(), _skytruth_source())
    assert params["bbox"] == "-6.0,35.0,20.0,45.0"
    assert params["limit"] == 1000
    assert params["datetime"] == "2024-01-30T00:00:00Z/2024-01-30T23:59:59Z"
    assert "filter" not in params


def test_skytruth_query_parameters_use_geometry_filter():
    region = Region(west=-6, east=20, south=35, north=45, geometry="POLYGON ((-6 35, 20 35, 20 45, -6 45, -6 35))")
    params = skytruth._query_parameters(Request(region=region, start="2024-01-30"), _skytruth_source())
    assert params["filter"].startswith("S_INTERSECTS(geometry, POLYGON")
    assert params["filter-lang"] == "cql2-text"
    assert "bbox" not in params


def test_skytruth_plan_reports_query_without_network(tmp_path):
    result = Fetcher(_request(), config=_cfg(tmp_path, skytruth_slicks=SKYTRUTH)).plan()

    assert result.summary.planned == 1
    assert result.results[0].details["provider"] == "skytruth"
    assert "bbox" in result.results[0].details["request"]


def test_skytruth_zero_features_is_a_warning(tmp_path, monkeypatch):
    monkeypatch.setattr(skytruth, "_fetch_pages", lambda url, parameters: ([], "http://cerulean/items"))
    result = Fetcher(_request(), config=_cfg(tmp_path, skytruth_slicks=SKYTRUTH)).download()

    assert result.summary.skipped == 1
    assert "0 features" in result.results[0].message


def test_skytruth_query_failure_is_a_warning(tmp_path, monkeypatch):
    # A malformed JSON body isn't a requests.exceptions.RequestException, but it
    # must still degrade to a SKIPPED result rather than aborting the whole run.
    def _raise(url, parameters):
        raise json.JSONDecodeError("bad json", "", 0)

    monkeypatch.setattr(skytruth, "_fetch_pages", _raise)
    result = Fetcher(_request(), config=_cfg(tmp_path, skytruth_slicks=SKYTRUTH)).download()

    assert result.summary.skipped == 1
    assert "bad json" in result.results[0].message


def test_skytruth_write_parquet_failure_is_a_warning(tmp_path, monkeypatch):
    # e.g. damast/geopandas missing: _write_parquet raising must not crash the run.
    monkeypatch.setattr(skytruth, "_fetch_pages", lambda url, parameters: ([{"id": 1}], "http://cerulean/items"))

    def _raise(features, output_path, request_url):
        raise ImportError("damast is not installed")

    monkeypatch.setattr(skytruth, "_write_parquet", _raise)
    result = Fetcher(_request(), config=_cfg(tmp_path, skytruth_slicks=SKYTRUTH)).download()

    assert result.summary.skipped == 1
    assert "damast is not installed" in result.results[0].message


def test_skytruth_reuses_existing_parquet(tmp_path, monkeypatch):
    def _must_not_query(url, parameters):
        raise AssertionError("cached file should be reused without querying")

    monkeypatch.setattr(skytruth, "_fetch_pages", _must_not_query)
    cfg = _cfg(tmp_path, skytruth_slicks=SKYTRUTH)
    fetcher = Fetcher(_request(), config=cfg)
    # Pre-create the exact output file the adapter would write.
    target = fetcher.plan().results[0].path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("parquet", encoding="utf-8")

    result = fetcher.download()
    assert result.summary.reused == 1


def _skytruth_feature(feature_id: int, *, hitl_cls: int | None) -> dict:
    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0.0, 35.0], [1.0, 35.0], [1.0, 36.0], [0.0, 36.0], [0.0, 35.0]]],
        },
        "properties": {
            "aoi_type_1_ids": [1],
            "aoi_type_2_ids": [2],
            "aoi_type_3_ids": [3],
            "area": 1000.0,
            "aspect_ratio_factor": 0.4,
            "centerlines": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "id": f"centerline-{feature_id}",
                        "type": "Feature",
                        "properties": {"area": 1000.0, "length": 50.0},
                        "geometry": {
                            "type": "LineString",
                            "coordinates": [[0.0, 35.0], [1.0, 36.0]],
                        },
                    }
                ],
            },
            "cls": 1,
            "fill_factor": 0.6,
            "hitl_cls": hitl_cls,
            "hitl_cls_name": "possible oil",
            "id": feature_id,
            "length": 50.0,
            "linearity": 0.8,
            "machine_confidence": 0.9,
            "max_source_collated_score": 0.2,
            "orchestrator_run": 42,
            "perimeter": 120.0,
            "polsby_popper": 0.5,
            "s1_scene_id": f"S1A_TEST_{feature_id}",
            "slick_confidence": "0.75",
            "slick_timestamp": "2024-01-30T12:00:00Z",
            "slick_url": f"https://cerulean.skytruth.org/slicks/{feature_id}",
            "source_type_1_ids": ["vessel-1"],
            "source_type_2_ids": ["infra-1"],
            "source_type_3_ids": ["dark-1"],
        },
    }


def test_skytruth_write_parquet_normalizes_damast_types(tmp_path):
    pl = pytest.importorskip("polars")

    output_path = tmp_path / "skytruth.parquet"
    features = [
        _skytruth_feature(1, hitl_cls=None),
        _skytruth_feature(2, hitl_cls=2),
    ]

    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter("always")
        skytruth._write_parquet(features, output_path, "https://api.cerulean.skytruth.org/items")

    damast_warnings = [record for record in records if "DataSpecification.apply" in str(record.message)]
    assert damast_warnings == []

    schema = pl.read_parquet(output_path).schema
    assert schema["hitl_cls"] == pl.Int64
    assert schema["slick_confidence"] == pl.Float64
    assert schema["geometry_geojson"] == pl.String


# --- HOZINT -----------------------------------------------------------------

HOZINT = {"kind": "hozint", "enabled": True, "path": "hozint", "command": ["hozint-apiclient", "query"]}


def _hozint_run_writing(returncode=0, filename="hozint-reports.parquet"):
    def _run(cmd):
        out_dir = Path(cmd[cmd.index("--output-dir") + 1])
        if returncode == 0:
            (out_dir / filename).write_text("parquet", encoding="utf-8")
        return types.SimpleNamespace(returncode=returncode)

    return _run


def test_hozint_downloads_reports(tmp_path, monkeypatch):
    monkeypatch.setattr(hozint, "_run", _hozint_run_writing())
    result = Fetcher(_request(), config=_cfg(tmp_path, hozint=HOZINT)).download()

    assert result.summary.downloaded == 1
    assert result.files[0].name == "hozint-reports.parquet"
    assert result.results[0].format == "parquet"


def test_hozint_nonzero_exit_is_a_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(hozint, "_run", _hozint_run_writing(returncode=2))
    result = Fetcher(_request(), config=_cfg(tmp_path, hozint=HOZINT)).download()

    assert result.summary.failed == 1
    assert "exited with 2" in result.results[0].message


def test_hozint_missing_tool_is_a_warning(tmp_path, monkeypatch):
    def _raise(cmd):
        raise FileNotFoundError("hozint-apiclient")

    monkeypatch.setattr(hozint, "_run", _raise)
    result = Fetcher(_request(), config=_cfg(tmp_path, hozint=HOZINT)).download()

    assert result.summary.skipped == 1
    assert "not available" in result.results[0].message


def test_hozint_reuses_cached_reports(tmp_path, monkeypatch):
    monkeypatch.setattr(hozint, "_run", _hozint_run_writing())
    cfg = _cfg(tmp_path, hozint=HOZINT)
    first = Fetcher(_request(), config=cfg).download()
    second = Fetcher(_request(), config=cfg).download()

    assert first.summary.downloaded == 1
    assert second.summary.reused == 1


def test_hozint_plan_reports_command(tmp_path):
    result = Fetcher(_request(), config=_cfg(tmp_path, hozint=HOZINT)).plan()

    assert result.summary.planned == 1
    command = result.results[0].details["request"]["command"]
    assert command[0] == "hozint-apiclient"
    assert "--from-time" in command


# --- GFW ---------------------------------------------------------------------

GFW = {
    "kind": "gfw",
    "enabled": True,
    "path": "gfw",
    "filename_pattern": "gfw_{start:%Y%m%d}_{end:%Y%m%d}_{bbox_hash}{geometry_hash}.parquet",
    "datasets": ["public-global-fishing-events:latest"],
    "limit": 1000,
}


def _gfw_source():
    return _cfg(Path("/tmp"), gfw=GFW).sources["gfw"]


def _fake_event(data):
    return types.SimpleNamespace(model_dump=lambda: data)


def test_gfw_geometry_uses_bbox_when_no_precise_geometry():
    region = Region.from_bbox((-6, 20, 35, 45))
    geometry = gfw._geometry(region)

    assert geometry["type"] == "Polygon"
    ring = geometry["coordinates"][0]
    assert ring[0] == ring[-1]  # closed ring
    lons = [point[0] for point in ring]
    lats = [point[1] for point in ring]
    assert (min(lons), max(lons)) == (-6.0, 20.0)
    assert (min(lats), max(lats)) == (35.0, 45.0)


def test_gfw_geometry_uses_precise_geometry_when_given():
    region = Region(west=-6, east=20, south=35, north=45, geometry="POLYGON ((0 35, 1 35, 1 36, 0 36, 0 35))")
    geometry = gfw._geometry(region)

    assert geometry["type"] == "Polygon"
    # The precise triangle-ish geometry, not the wider bbox rectangle.
    assert geometry["coordinates"][0][0] == (0.0, 35.0)


def test_gfw_output_path_differs_for_same_bbox_different_geometry(tmp_path):
    # Two distinct polygons sharing a bounding box must not collide on the same output path -
    # bbox_hash alone can't tell them apart, and _geometry() queries the precise polygon.
    bbox_only = Region.from_bbox((-6, 20, 35, 45))
    polygon_a = Region(west=-6, east=20, south=35, north=45, geometry="POLYGON ((0 35, 1 35, 1 36, 0 36, 0 35))")
    polygon_b = Region(west=-6, east=20, south=35, north=45, geometry="POLYGON ((5 40, 6 40, 6 41, 5 41, 5 40))")

    source = _gfw_source()
    request_dir = tmp_path

    def _path(region):
        request = Request(region=region, start="2026-08-01", end="2026-08-07")
        return gfw._output_path(request, source, request_dir)

    path_bbox, path_a, path_b = _path(bbox_only), _path(polygon_a), _path(polygon_b)
    assert len({path_bbox, path_a, path_b}) == 3

    # A plain bbox request's filename carries no geometry-hash suffix at all: bbox_hash is
    # immediately followed by the extension, not an extra "_<hash>" segment.
    bbox_hash = bbox_hash_of(bbox_only)
    assert path_bbox.name == f"gfw_20260801_20260807_{bbox_hash}.parquet"
    assert path_a.name.startswith(f"gfw_20260801_20260807_{bbox_hash}_")
    assert path_b.name.startswith(f"gfw_20260801_20260807_{bbox_hash}_")


def test_gfw_query_window_end_date_is_exclusive_adjusted():
    # GFW's end_date is exclusive - the request's own end day must still be included.
    request = Request(region=Region.from_bbox((-6, 20, 35, 45)), start="2026-08-01", end="2026-08-07")
    start_date, end_date = gfw._query_window(request)

    assert start_date == "2026-08-01"
    assert end_date == "2026-08-08"


def test_gfw_flatten_event_extracts_nested_fields():
    row = gfw._flatten_event(
        _fake_event(
            {
                "id": "evt-1",
                "type": "fishing",
                # gfwapiclient's model_dump() keeps timezone-aware datetime objects, not strings.
                "start": datetime.datetime(2026, 8, 1, 0, 0, tzinfo=datetime.UTC),
                "end": datetime.datetime(2026, 8, 1, 1, 0, tzinfo=datetime.UTC),
                "position": {"lat": 68.1, "lon": 13.5},
                "bounding_box": [13.0, 68.0, 14.0, 68.2],
                "distances": {
                    "start_distance_from_shore_km": 1.0,
                    "end_distance_from_shore_km": 2.0,
                    "start_distance_from_port_km": 0.5,
                    "end_distance_from_port_km": 0.0,
                },
                "vessel": {"id": "v-1", "name": "TEST VESSEL", "ssvid": "257000000", "flag": "NOR", "type": "fishing"},
                "regions": {
                    "eez": ["5686"],
                    "mpa": [],
                    "rfmo": [],
                    "fao": [],
                    "major_fao": [],
                    "eez_12_nm": [],
                    "high_seas": [],
                    "mpa_no_take_partial": [],
                    "mpa_no_take": [],
                },
                "encounter": None,
                "fishing": {
                    "total_distance_km": 0.8,
                    "average_speed_knots": 0.002,
                    "average_duration_hours": None,
                    "potential_risk": False,
                    "vessel_public_authorization_status": "publicly_authorized",
                },
                "gap": None,
                "loitering": None,
                "port_visit": None,
            }
        )
    )

    assert row["lat"] == 68.1
    assert row["lon"] == 13.5
    assert row["vessel_ssvid"] == "257000000"
    assert row["start_distance_from_shore_km"] == 1.0
    assert row["regions"]["eez"] == ["5686"]
    assert json.loads(row["fishing_json"])["total_distance_km"] == 0.8
    assert row["encounter_json"] is None
    assert row["port_visit_json"] is None


def test_gfw_fetch_events_works_inside_a_running_event_loop(monkeypatch):
    # A Jupyter kernel (ipykernel >= 7) runs every cell inside a live event loop, where a bare
    # asyncio.run() raises RuntimeError. collekt ships notebooks, so the sync wrapper must cope.
    async def _fake_get_all_events(source, region, start_date, end_date):
        return [{"id": "evt-1"}]

    monkeypatch.setattr(gfw, "_get_all_events", _fake_get_all_events)

    async def _in_a_loop():
        return gfw._fetch_events(_gfw_source(), Region.from_bbox((-6, 20, 35, 45)), "2026-08-01", "2026-08-08")

    assert asyncio.run(_in_a_loop()) == [{"id": "evt-1"}]


def test_gfw_plan_reports_query_without_network(tmp_path):
    result = Fetcher(_request(), config=_cfg(tmp_path, gfw=GFW)).plan()

    assert result.summary.planned == 1
    details = result.results[0].details
    assert details["provider"] == "gfw"
    assert details["request"]["datasets"] == ["public-global-fishing-events:latest"]
    assert details["request"]["geometry"]["type"] == "Polygon"


def test_gfw_limit_has_no_in_code_default(tmp_path):
    # The shipped default lives in gfw.yaml alone - a source config that omits `limit`
    # must not silently fall back to some other hardcoded value in gfw.py.
    bare = _cfg(tmp_path, gfw={"kind": "gfw", "path": "gfw"}).sources["gfw"]
    assert bare.raw.get("limit") is None


def test_gfw_bundled_catalog_sets_the_default_limit():
    import collekt

    resolved = collekt.DatasetConfig(collekt.GFW()).resolve()
    assert resolved.sources["gfw"].raw["limit"] == 1000


def test_gfw_zero_events_is_a_warning(tmp_path, monkeypatch):
    monkeypatch.setattr(gfw, "_fetch_events", lambda source, region, start_date, end_date: [])
    result = Fetcher(_request(), config=_cfg(tmp_path, gfw=GFW)).download()

    assert result.summary.skipped == 1
    assert "0 events" in result.results[0].message


def test_gfw_query_failure_is_a_warning(tmp_path, monkeypatch):
    def _raise(source, region, start_date, end_date):
        raise RuntimeError("401 Unauthorized")

    monkeypatch.setattr(gfw, "_fetch_events", _raise)
    result = Fetcher(_request(), config=_cfg(tmp_path, gfw=GFW)).download()

    assert result.summary.skipped == 1
    assert "401 Unauthorized" in result.results[0].message


def test_gfw_write_parquet_failure_is_a_warning(tmp_path, monkeypatch):
    monkeypatch.setattr(gfw, "_fetch_events", lambda source, region, start_date, end_date: [{"id": "evt-1"}])

    def _raise(events, output_path, source):
        raise ImportError("damast is not installed")

    monkeypatch.setattr(gfw, "_write_parquet", _raise)
    result = Fetcher(_request(), config=_cfg(tmp_path, gfw=GFW)).download()

    assert result.summary.skipped == 1
    assert "damast is not installed" in result.results[0].message


def test_gfw_reuses_existing_parquet(tmp_path, monkeypatch):
    def _must_not_query(source, region, start_date, end_date):
        raise AssertionError("cached file should be reused without querying")

    monkeypatch.setattr(gfw, "_fetch_events", _must_not_query)
    cfg = _cfg(tmp_path, gfw=GFW)
    fetcher = Fetcher(_request(), config=cfg)
    target = fetcher.plan().results[0].path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("parquet", encoding="utf-8")

    result = fetcher.download()
    assert result.summary.reused == 1


def test_gfw_write_parquet_produces_expected_schema(tmp_path):
    pl = pytest.importorskip("polars")
    pytest.importorskip("damast")

    output_path = tmp_path / "gfw.parquet"
    events = [
        gfw._flatten_event(
            _fake_event(
                {
                    "id": "evt-1",
                    "type": "fishing",
                    "start": datetime.datetime(2026, 8, 1, 0, 0, tzinfo=datetime.UTC),
                    "end": datetime.datetime(2026, 8, 1, 1, 0, tzinfo=datetime.UTC),
                    "position": {"lat": 68.1, "lon": 13.5},
                    "bounding_box": [13.0, 68.0, 14.0, 68.2],
                    "distances": {
                        "start_distance_from_shore_km": 1.0,
                        "end_distance_from_shore_km": 2.0,
                        "start_distance_from_port_km": 0.5,
                        "end_distance_from_port_km": 0.0,
                    },
                    "vessel": {
                        "id": "v-1",
                        "name": "TEST VESSEL",
                        "ssvid": "257000000",
                        "flag": "NOR",
                        "type": "fishing",
                    },
                    "regions": {
                        "eez": ["5686"],
                        "mpa": [],
                        "rfmo": [],
                        "fao": [],
                        "major_fao": [],
                        "eez_12_nm": [],
                        "high_seas": [],
                        "mpa_no_take_partial": [],
                        "mpa_no_take": [],
                    },
                    "encounter": None,
                    "fishing": {"total_distance_km": 0.8},
                    "gap": None,
                    "loitering": None,
                    "port_visit": None,
                }
            )
        ),
    ]

    gfw._write_parquet(events, output_path, _gfw_source())

    schema = pl.read_parquet(output_path).schema
    assert schema["lat"] == pl.Float64
    assert schema["start"] == pl.Datetime(time_unit="us", time_zone="UTC")
    assert schema["bounding_box"] == pl.List(pl.Float64)
    assert schema["fishing_json"] == pl.String


# --- Copernicus Data Space --------------------------------------------------

DATASPACE = {
    "kind": "copernicus_dataspace",
    "enabled": True,
    "path": "dataspace",
    "collection": "sentinel-1-grd",
    "max_records": 10,
}


def _stub_dataspace(monkeypatch, features):
    monkeypatch.setattr(copernicus_dataspace, "_credentials", lambda: ("user", "pass"))
    monkeypatch.setattr(copernicus_dataspace, "_login", lambda username, password: "token")
    monkeypatch.setattr(copernicus_dataspace, "_search", lambda token, params: {"features": features})
    monkeypatch.setattr(
        copernicus_dataspace, "_download", lambda url, token, path: Path(path).write_text("product", encoding="utf-8")
    )


def test_dataspace_downloads_products(tmp_path, monkeypatch):
    feature = {"id": "S1A", "assets": {"Product": {"href": "https://x/prod", "file:local_path": "S1A_prod.SAFE.zip"}}}
    _stub_dataspace(monkeypatch, [feature])
    result = Fetcher(_request(), config=_cfg(tmp_path, dataspace=DATASPACE)).download()

    assert result.summary.downloaded == 1
    assert result.files[0].name == "S1A_prod.SAFE.zip"
    assert result.results[0].format == "zip"


def test_dataspace_no_products_is_a_warning(tmp_path, monkeypatch):
    _stub_dataspace(monkeypatch, [])
    result = Fetcher(_request(), config=_cfg(tmp_path, dataspace=DATASPACE)).download()

    assert result.summary.skipped == 1
    assert "no products" in result.results[0].message


def test_dataspace_without_collection_is_skipped(tmp_path):
    cfg = _cfg(tmp_path, dataspace={"kind": "copernicus_dataspace", "enabled": True, "path": "dataspace"})
    result = Fetcher(_request(), config=cfg).download()

    assert result.summary.skipped == 1
    assert "no collection configured" in result.results[0].message


def test_dataspace_login_failure_is_a_warning(tmp_path, monkeypatch):
    monkeypatch.setattr(copernicus_dataspace, "_credentials", lambda: ("user", "pass"))

    def _raise(username, password):
        raise RuntimeError("bad credentials")

    monkeypatch.setattr(copernicus_dataspace, "_login", _raise)
    result = Fetcher(_request(), config=_cfg(tmp_path, dataspace=DATASPACE)).download()

    assert result.summary.skipped == 1
    assert "login failed" in result.results[0].message


class _FakeStreamResponse:
    def __init__(self, chunks):
        self._chunks = chunks

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        yield from self._chunks

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def test_dataspace_download_writes_final_path_only(tmp_path, monkeypatch):
    monkeypatch.setattr(copernicus_dataspace.requests, "get", lambda *a, **k: _FakeStreamResponse([b"product-bytes"]))
    path = tmp_path / "product.zip"

    copernicus_dataspace._download("https://x/prod", "token", path)

    assert path.read_bytes() == b"product-bytes"
    assert not (tmp_path / "product.zip.part").exists()


def test_dataspace_download_failure_leaves_no_partial_file(tmp_path, monkeypatch):
    def _broken_iter_content(chunk_size):
        yield b"partial-bytes"
        raise requests.exceptions.ConnectionError("dropped")

    response = _FakeStreamResponse([])
    response.iter_content = _broken_iter_content
    monkeypatch.setattr(copernicus_dataspace.requests, "get", lambda *a, **k: response)
    path = tmp_path / "product.zip"

    with pytest.raises(requests.exceptions.ConnectionError):
        copernicus_dataspace._download("https://x/prod", "token", path)

    assert not path.exists()
    assert not (tmp_path / "product.zip.part").exists()


def test_dataspace_search_failure_is_a_warning(tmp_path, monkeypatch):
    # A malformed JSON body isn't a requests.exceptions.RequestException, but it
    # must still degrade to a SKIPPED result rather than aborting the whole run.
    monkeypatch.setattr(copernicus_dataspace, "_credentials", lambda: ("user", "pass"))
    monkeypatch.setattr(copernicus_dataspace, "_login", lambda username, password: "token")

    def _raise(token, params):
        raise json.JSONDecodeError("bad json", "", 0)

    monkeypatch.setattr(copernicus_dataspace, "_search", _raise)
    result = Fetcher(_request(), config=_cfg(tmp_path, dataspace=DATASPACE)).download()

    assert result.summary.skipped == 1
    assert "bad json" in result.results[0].message


def test_dataspace_plan_reports_search_without_network(tmp_path):
    result = Fetcher(_request(), config=_cfg(tmp_path, dataspace=DATASPACE)).plan()

    assert result.summary.planned == 1
    assert result.results[0].details["request"]["collections"] == "sentinel-1-grd"
