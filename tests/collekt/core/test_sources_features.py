"""Offline tests for the feature/file source adapters.

Skytruth, HOZINT, and Copernicus Data Space are per-request sources. Their
network/tooling boundaries are stubbed, so no credentials or downloads are
needed. Skytruth's Parquet-writing path needs damast/geopandas and is covered
separately when those are installed.
"""

import types
from pathlib import Path

from collekt import Fetcher
from collekt.core.config import get_config
from collekt.core.request import Region, Request
from collekt.sources import copernicus_dataspace, hozint, skytruth


def _cfg(tmp_path, **sources):
    return get_config(overrides={"output": {"root": str(tmp_path)}, "sources": sources})


def _request(**kwargs):
    kwargs.setdefault("region", Region.from_bbox((-6, 20, 35, 45)))
    kwargs.setdefault("start", "2024-01-30")
    return Request(**kwargs)


# --- Skytruth ---------------------------------------------------------------

SKYTRUTH = {
    "kind": "skytruth",
    "enabled": True,
    "variable_groups": ["oil_slick"],
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
    result = Fetcher(_request(), config=_cfg(tmp_path, skytruth_slicks=SKYTRUTH), preset=None).plan()

    assert result.summary.planned == 1
    assert result.results[0].details["provider"] == "skytruth"
    assert "bbox" in result.results[0].details["request"]


def test_skytruth_zero_features_is_a_warning(tmp_path, monkeypatch):
    monkeypatch.setattr(skytruth, "_fetch_pages", lambda url, parameters: ([], "http://cerulean/items"))
    result = Fetcher(_request(), config=_cfg(tmp_path, skytruth_slicks=SKYTRUTH), preset=None).download()

    assert result.summary.skipped == 1
    assert "0 features" in result.results[0].message


def test_skytruth_reuses_existing_parquet(tmp_path, monkeypatch):
    def _must_not_query(url, parameters):
        raise AssertionError("cached file should be reused without querying")

    monkeypatch.setattr(skytruth, "_fetch_pages", _must_not_query)
    cfg = _cfg(tmp_path, skytruth_slicks=SKYTRUTH)
    fetcher = Fetcher(_request(), config=cfg, preset=None)
    # Pre-create the exact output file the adapter would write.
    target = fetcher.plan().results[0].path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("parquet", encoding="utf-8")

    result = fetcher.download()
    assert result.summary.reused == 1


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
    result = Fetcher(_request(), config=_cfg(tmp_path, hozint=HOZINT), preset=None).download()

    assert result.summary.downloaded == 1
    assert result.files[0].name == "hozint-reports.parquet"
    assert result.results[0].format == "parquet"


def test_hozint_nonzero_exit_is_a_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(hozint, "_run", _hozint_run_writing(returncode=2))
    result = Fetcher(_request(), config=_cfg(tmp_path, hozint=HOZINT), preset=None).download()

    assert result.summary.failed == 1
    assert "exited with 2" in result.results[0].message


def test_hozint_missing_tool_is_a_warning(tmp_path, monkeypatch):
    def _raise(cmd):
        raise FileNotFoundError("hozint-apiclient")

    monkeypatch.setattr(hozint, "_run", _raise)
    result = Fetcher(_request(), config=_cfg(tmp_path, hozint=HOZINT), preset=None).download()

    assert result.summary.skipped == 1
    assert "not available" in result.results[0].message


def test_hozint_reuses_cached_reports(tmp_path, monkeypatch):
    monkeypatch.setattr(hozint, "_run", _hozint_run_writing())
    cfg = _cfg(tmp_path, hozint=HOZINT)
    first = Fetcher(_request(), config=cfg, preset=None).download()
    second = Fetcher(_request(), config=cfg, preset=None).download()

    assert first.summary.downloaded == 1
    assert second.summary.reused == 1


def test_hozint_plan_reports_command(tmp_path):
    result = Fetcher(_request(), config=_cfg(tmp_path, hozint=HOZINT), preset=None).plan()

    assert result.summary.planned == 1
    command = result.results[0].details["request"]["command"]
    assert command[0] == "hozint-apiclient"
    assert "--from-time" in command


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
    result = Fetcher(_request(), config=_cfg(tmp_path, dataspace=DATASPACE), preset=None).download()

    assert result.summary.downloaded == 1
    assert result.files[0].name == "S1A_prod.SAFE.zip"
    assert result.results[0].format == "zip"


def test_dataspace_no_products_is_a_warning(tmp_path, monkeypatch):
    _stub_dataspace(monkeypatch, [])
    result = Fetcher(_request(), config=_cfg(tmp_path, dataspace=DATASPACE), preset=None).download()

    assert result.summary.skipped == 1
    assert "no products" in result.results[0].message


def test_dataspace_without_collection_is_skipped(tmp_path):
    cfg = _cfg(tmp_path, dataspace={"kind": "copernicus_dataspace", "enabled": True, "path": "dataspace"})
    result = Fetcher(_request(), config=cfg, preset=None).download()

    assert result.summary.skipped == 1
    assert "no collection configured" in result.results[0].message


def test_dataspace_login_failure_is_a_warning(tmp_path, monkeypatch):
    monkeypatch.setattr(copernicus_dataspace, "_credentials", lambda: ("user", "pass"))

    def _raise(username, password):
        raise RuntimeError("bad credentials")

    monkeypatch.setattr(copernicus_dataspace, "_login", _raise)
    result = Fetcher(_request(), config=_cfg(tmp_path, dataspace=DATASPACE), preset=None).download()

    assert result.summary.skipped == 1
    assert "login failed" in result.results[0].message


def test_dataspace_plan_reports_search_without_network(tmp_path):
    result = Fetcher(_request(), config=_cfg(tmp_path, dataspace=DATASPACE), preset=None).plan()

    assert result.summary.planned == 1
    assert result.results[0].details["request"]["collections"] == "sentinel-1-grd"
