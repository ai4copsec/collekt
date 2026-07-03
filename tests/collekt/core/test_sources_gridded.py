"""Offline tests for the gridded source adapters (CMEMS, ERA5, ECMWF Open Data).

Provider clients are stubbed; no network access. Source definitions are supplied
inline to keep these adapter tests independent from the bundled catalog.
"""

import json
import sys
import types
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from collekt import Fetcher
from collekt.core.config import get_config
from collekt.core.request import Region, Request
from collekt.sources.base import SourceStatus
from collekt.sources.cmems import _install_raw_tqdm_auto

GLORYS = {
    "kind": "cmems",
    "enabled": True,
    "path": "cmems/glorys",
    "filename_pattern": "glorys_{dataset_id}_{date:%Y%m%d}_{bbox_hash}.nc",
    "dataset_id": "cmems_mod_glo_phy_my_0.083deg_P1D-m",
    "variables": ["uo", "vo"],
    "temporal_sampling": "24h",
    "depth": [1.0, 1.1],
    "coordinates_selection_method": "inside",
}

WAVES = {
    "kind": "cmems",
    "enabled": True,
    "path": "cmems/waves",
    "filename_pattern": "waves_{dataset_id}_{date:%Y%m%d}_{bbox_hash}.nc",
    "dataset_id": "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i",
    "variables": ["VHM0"],
    "temporal_sampling": "3h",
    "time_selection": "full_day",
    "coordinates_selection_method": "outside",
}

DUACS = {
    "kind": "cmems",
    "enabled": True,
    "path": "cmems/duacs",
    "filename_pattern": "duacs_{dataset_id}_{date:%Y%m%d}_{bbox_hash}.nc",
    "dataset_id": "cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.125deg_P1D",
    "variables": ["ugos", "vgos"],
    "temporal_sampling": "24h",
    "coordinates_selection_method": "outside",
}

ERA5 = {
    "kind": "era5",
    "enabled": True,
    "path": "ecmwf/era5",
    "filename_pattern": "era5_10m_wind_{date:%Y%m%d}_{bbox_hash}.nc",
    "dataset_id": "reanalysis-era5-single-levels",
    "variables": ["10m_u_component_of_wind", "10m_v_component_of_wind"],
    "temporal_sampling": "1h",
    "coverage": {"start": "1940-01-01", "end": "now-5d"},
    "pad_deg": 0.5,
}

ECMWF = {
    "kind": "ecmwf_open_data",
    "enabled": True,
    "path": "ecmwf/open_data",
    "filename_pattern": "ecmwf_open_data_10m_wind_{date:%Y%m%d}_{time}z_{bbox_hash}.grib2",
    "dataset_id": "ecmwf-open-data-ifs",
    "variables": ["10u", "10v"],
    "temporal_sampling": "3h",
    "coverage": {"start": "now-4d", "end": "now+10d"},
    "model": "ifs",
    "source": "ecmwf",
    "resol": "0p25",
    "time": "00",
    "steps": [0, 3, 6, 9, 12, 15, 18, 21, 24],
}


def _cfg(tmp_path, **sources):
    return get_config(overrides={"source_catalogs": [], "output": {"root": str(tmp_path)}, "sources": sources})


def _request(start="2023-06-15", sampling="24h", end=None):
    return Request(region=Region.from_bbox((-6, 20, 35, 45)), start=start, end=end, sampling=sampling)


def _stub_copernicusmarine(monkeypatch, calls):
    def subset(**kwargs):
        calls.append(kwargs)
        Path(kwargs["output_directory"], kwargs["output_filename"]).write_text("netcdf", encoding="utf-8")
        return {"ok": True}

    monkeypatch.setitem(sys.modules, "copernicusmarine", types.SimpleNamespace(subset=subset))


# --- CMEMS ------------------------------------------------------------------


def test_cmems_forces_raw_tqdm_auto(monkeypatch):
    monkeypatch.delitem(sys.modules, "tqdm.auto", raising=False)
    monkeypatch.delitem(sys.modules, "tqdm.autonotebook", raising=False)

    _install_raw_tqdm_auto()

    from tqdm import tqdm
    from tqdm.auto import tqdm as auto_tqdm
    from tqdm.autonotebook import tqdm as autonotebook_tqdm

    assert auto_tqdm is tqdm
    assert autonotebook_tqdm is tqdm


def test_cmems_downloads_and_writes_manifest(tmp_path, monkeypatch):
    calls = []
    _stub_copernicusmarine(monkeypatch, calls)
    result = Fetcher(_request(), config=_cfg(tmp_path, cmems_glorys=GLORYS)).download()

    assert result.summary.downloaded == 1
    assert calls[0]["dataset_id"] == "cmems_mod_glo_phy_my_0.083deg_P1D-m"
    assert calls[0]["start_datetime"] == "2023-06-15T00:00:00"
    assert calls[0]["end_datetime"] == "2023-06-15T00:00:00"
    assert calls[0]["maximum_depth"] == 1.1
    assert calls[0]["coordinates_selection_method"] == "inside"
    assert calls[0]["disable_progress_bar"] is True
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["files"][0]["status"] == "downloaded"
    assert not Path(manifest["files"][0]["path"]).is_absolute()
    assert manifest["sources"]["cmems_glorys"]["variables"] == ["uo", "vo"]
    assert manifest["files"][0]["details"]["temporal"]["actual_sampling"] == "24h"


def test_cmems_reuses_cached_file(tmp_path, monkeypatch):
    calls = []
    _stub_copernicusmarine(monkeypatch, calls)
    cfg = _cfg(tmp_path, cmems_glorys=GLORYS)
    first = Fetcher(_request(), config=cfg).download()
    second = Fetcher(_request(), config=cfg).download()

    assert first.summary.downloaded == 1
    assert second.summary.reused == 1
    assert len(calls) == 1


def test_cmems_dry_run_plans_without_downloading(tmp_path, monkeypatch):
    monkeypatch.setattr("collekt.core.availability._copernicusmarine_describe", lambda: None)
    result = Fetcher(_request(), config=_cfg(tmp_path, cmems_glorys=GLORYS)).plan()

    assert result.summary.planned == 1
    assert result.results[0].status == SourceStatus.PLANNED
    assert result.results[0].details["provider"] == "copernicusmarine"
    assert result.results[0].details["request"]["end_datetime"] == "2023-06-15T00:00:00"
    assert not result.output_dir.exists()


def test_cmems_rejects_sampling_finer_than_dataset(tmp_path, monkeypatch):
    monkeypatch.setattr("collekt.core.availability._copernicusmarine_describe", lambda: None)
    with pytest.raises(ValueError, match="integer multiple"):
        Fetcher(_request(sampling="6h"), config=_cfg(tmp_path, cmems_glorys=GLORYS)).plan()


def test_cmems_subdaily_source_uses_single_timestamp_for_daily(tmp_path, monkeypatch):
    calls = []
    _stub_copernicusmarine(monkeypatch, calls)
    result = Fetcher(_request(), config=_cfg(tmp_path, cmems_waves=WAVES)).download()

    assert result.summary.downloaded == 1
    assert calls[0]["start_datetime"] == "2023-06-15T00:00:00"
    assert calls[0]["end_datetime"] == "2023-06-15T00:00:00"


def test_cmems_subdaily_source_uses_time_range_for_subdaily(tmp_path, monkeypatch):
    calls = []
    _stub_copernicusmarine(monkeypatch, calls)
    result = Fetcher(_request(sampling="6h"), config=_cfg(tmp_path, cmems_waves=WAVES)).download()

    assert result.summary.downloaded == 1
    assert calls[0]["start_datetime"] == "2023-06-15T00:00:00"
    assert calls[0]["end_datetime"] == "2023-06-15T23:59:59"


def test_cmems_uses_selected_source_variables(tmp_path, monkeypatch):
    calls = []
    _stub_copernicusmarine(monkeypatch, calls)
    source = dict(DUACS, variables=["ugos", "vgos", "sla", "adt"])
    result = Fetcher(_request(), config=_cfg(tmp_path, cmems_duacs=source)).download()

    assert calls[0]["variables"] == ["ugos", "vgos", "sla", "adt"]
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["sources"]["cmems_duacs"]["variables"] == ["ugos", "vgos", "sla", "adt"]


def test_cmems_unavailable_is_warning_and_strict_raises(tmp_path, monkeypatch):
    module = types.SimpleNamespace(subset=lambda **_: (_ for _ in ()).throw(RuntimeError("not available yet")))
    monkeypatch.setitem(sys.modules, "copernicusmarine", module)
    cfg = _cfg(tmp_path, cmems_glorys=GLORYS)
    request = _request(start="2026-06-25")

    result = Fetcher(request, config=cfg).download()
    assert result.summary.skipped == 1
    assert "not available yet" in result.results[0].message
    with pytest.raises(RuntimeError, match="collection completed with warnings"):
        Fetcher(request, config=cfg, strict=True).download()


# --- ERA5 -------------------------------------------------------------------


def test_era5_downloads_with_stubbed_cdsapi(tmp_path, monkeypatch):
    calls = []

    class FakeRetrieval:
        def download(self, path):
            Path(path).write_text("netcdf", encoding="utf-8")

    class FakeClient:
        def retrieve(self, dataset_id, request):
            calls.append(request)
            assert dataset_id == "reanalysis-era5-single-levels"
            return FakeRetrieval()

    monkeypatch.setitem(sys.modules, "cdsapi", types.SimpleNamespace(Client=FakeClient))
    result = Fetcher(_request(start="2026-06-25"), config=_cfg(tmp_path, era5_reanalysis=ERA5)).download()

    assert result.summary.downloaded == 1
    assert result.files[0].name.startswith("era5_10m_wind_20260625_")
    assert calls[0]["area"] == [45.5, -6.5, 34.5, 20.5]
    assert calls[0]["time"] == ["00:00"]


def test_era5_uses_requested_subdaily_sampling(tmp_path, monkeypatch):
    calls = []

    class FakeRetrieval:
        def download(self, path):
            Path(path).write_text("netcdf", encoding="utf-8")

    class FakeClient:
        def retrieve(self, _dataset_id, request):
            calls.append(request)
            return FakeRetrieval()

    monkeypatch.setitem(sys.modules, "cdsapi", types.SimpleNamespace(Client=FakeClient))
    result = Fetcher(
        _request(start="2026-06-25", sampling="6h"), config=_cfg(tmp_path, era5_reanalysis=ERA5)
    ).download()

    assert result.summary.downloaded == 1
    assert calls[0]["time"] == ["00:00", "06:00", "12:00", "18:00"]


def test_era5_plan_skips_dates_outside_declarative_coverage(tmp_path):
    result = Fetcher(_request(start="1900-01-01"), config=_cfg(tmp_path, era5_reanalysis=ERA5)).plan()

    assert result.summary.planned == 0
    assert result.summary.skipped == 1
    availability = result.results[0].details["availability"]
    assert availability["status"] == "unavailable"
    assert availability["method"] == "coverage"


# --- ECMWF Open Data --------------------------------------------------------


def test_ecmwf_downloads_grib_with_stubbed_client(tmp_path, monkeypatch):
    calls = []

    class FakeClient:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        def retrieve(self, request, target):
            calls.append(("retrieve", request, target))
            Path(target).write_text("grib", encoding="utf-8")

    monkeypatch.setattr("collekt.sources.ecmwf_open_data._client_class", lambda: FakeClient)
    result = Fetcher(_request(start="2026-06-25"), config=_cfg(tmp_path, ecmwf_open_data_forecast=ECMWF)).download()

    assert result.summary.downloaded == 1
    assert result.results[0].format == "grib2"
    assert result.files[0].name.startswith("ecmwf_open_data_10m_wind_20260625_00z_")
    assert calls[0] == ("init", {"source": "ecmwf", "model": "ifs", "resol": "0p25"})
    assert calls[1][1]["param"] == ["10u", "10v"]
    assert calls[1][1]["step"] == [0]


def test_ecmwf_uses_requested_subdaily_sampling(tmp_path, monkeypatch):
    calls = []

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def retrieve(self, request, target):
            calls.append(request)
            Path(target).write_text("grib", encoding="utf-8")

    monkeypatch.setattr("collekt.sources.ecmwf_open_data._client_class", lambda: FakeClient)
    result = Fetcher(
        _request(start="2026-06-25", sampling="6h"),
        config=_cfg(tmp_path, ecmwf_open_data_forecast=ECMWF),
    ).download()

    assert result.summary.downloaded == 1
    assert calls[0]["step"] == [0, 6, 12, 18]


def test_ecmwf_plan_available_within_rolling_window(tmp_path):
    request = Request(region=Region.from_bbox((-6, 20, 35, 45)))  # defaults to today
    result = Fetcher(request, config=_cfg(tmp_path, ecmwf_open_data_forecast=ECMWF)).plan()

    assert result.summary.planned == 1
    availability = result.results[0].details["availability"]
    assert availability["status"] == "available"
    assert availability["method"] == "coverage"


# --- CMEMS availability via describe ---------------------------------------


def _describe(bbox, start: date, end: date):
    def _ms(day: date):
        return (
            datetime(day.year, day.month, day.day, tzinfo=UTC) - datetime(1970, 1, 1, tzinfo=UTC)
        ).total_seconds() * 1000

    def describe(**kwargs):
        variable = SimpleNamespace(
            short_name="uo",
            bbox=list(bbox),
            coordinates=[
                SimpleNamespace(
                    coordinate_id="time",
                    coordinate_unit="milliseconds since 1970-01-01 00:00:00",
                    minimum_value=_ms(start),
                    maximum_value=_ms(end),
                )
            ],
        )
        dataset = SimpleNamespace(
            dataset_id=kwargs["dataset_id"],
            versions=[SimpleNamespace(parts=[SimpleNamespace(services=[SimpleNamespace(variables=[variable])])])],
        )
        return SimpleNamespace(products=[SimpleNamespace(datasets=[dataset])])

    return describe


def test_cmems_plan_marks_available_via_describe(tmp_path, monkeypatch):
    describe = _describe(bbox=[-180.0, -80.0, 180.0, 90.0], start=date(1993, 1, 1), end=date(2024, 6, 30))
    monkeypatch.setattr("collekt.core.availability._copernicusmarine_describe", lambda: describe)
    result = Fetcher(_request(), config=_cfg(tmp_path, cmems_glorys=GLORYS)).plan()

    assert result.summary.planned == 1
    assert result.results[0].details["availability"]["status"] == "available"
    assert result.results[0].details["availability"]["method"] == "describe"


def test_cmems_plan_marks_unavailable_out_of_region(tmp_path, monkeypatch):
    describe = _describe(bbox=[10.0, 10.0, 12.0, 12.0], start=date(1993, 1, 1), end=date(2024, 6, 30))
    monkeypatch.setattr("collekt.core.availability._copernicusmarine_describe", lambda: describe)
    result = Fetcher(_request(), config=_cfg(tmp_path, cmems_glorys=GLORYS)).plan()

    assert result.summary.skipped == 1
    assert result.results[0].details["availability"]["status"] == "unavailable"
    assert "outside the available coverage" in result.results[0].message


def test_cmems_plan_marks_unknown_when_describe_unavailable(tmp_path, monkeypatch):
    monkeypatch.setattr("collekt.core.availability._copernicusmarine_describe", lambda: None)
    result = Fetcher(_request(), config=_cfg(tmp_path, cmems_glorys=GLORYS)).plan()

    assert result.summary.planned == 1
    assert result.results[0].details["availability"]["status"] == "unknown"


def test_cmems_plan_falls_back_to_declarative_coverage(tmp_path, monkeypatch):
    monkeypatch.setattr("collekt.core.availability._copernicusmarine_describe", lambda: None)
    source = GLORYS | {
        "coverage": {
            "longitude": [-180.0, 180.0],
            "latitude": [-80.0, 90.0],
            "temporal": {"start": "1993-01-01", "end": None, "kind": "archive"},
        }
    }

    result = Fetcher(_request(), config=_cfg(tmp_path, cmems_glorys=source)).plan()

    assert result.summary.planned == 1
    availability = result.results[0].details["availability"]
    assert availability["status"] == "available"
    assert availability["method"] == "coverage"
    assert availability["coverage"]["kind"] == "archive"
