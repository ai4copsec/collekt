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
    "filename_pattern": "ecmwf_open_data_10m_wind_{date:%Y%m%d}_{time}z_{bbox_hash}.nc",
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


def _request(start="2023-06-15", end=None):
    return Request(region=Region.from_bbox((-6, 20, 35, 45)), start=start, end=end)


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


def test_cmems_daily_source_uses_a_single_timestamp(tmp_path, monkeypatch):
    # A 24h dataset fetches one timestamp per day (covered by GLORYS above too).
    calls = []
    _stub_copernicusmarine(monkeypatch, calls)
    result = Fetcher(_request(), config=_cfg(tmp_path, cmems_glorys=GLORYS)).download()

    assert result.summary.downloaded == 1
    assert calls[0]["start_datetime"] == "2023-06-15T00:00:00"
    assert calls[0]["end_datetime"] == "2023-06-15T00:00:00"


def test_cmems_subdaily_source_uses_a_full_day_time_range(tmp_path, monkeypatch):
    # A subdaily dataset (time_selection full_day) is fetched over the whole day.
    calls = []
    _stub_copernicusmarine(monkeypatch, calls)
    result = Fetcher(_request(), config=_cfg(tmp_path, cmems_waves=WAVES)).download()

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
    # ERA5 is hourly; with no request sampling it fetches every native hour.
    assert calls[0]["time"] == [f"{hour:02d}:00" for hour in range(24)]


def test_era5_download_failure_leaves_no_partial_file(tmp_path, monkeypatch):
    class FakeRetrieval:
        def download(self, path):
            Path(path).write_text("partial", encoding="utf-8")
            raise RuntimeError("connection dropped")

    class FakeClient:
        def retrieve(self, dataset_id, request):
            return FakeRetrieval()

    monkeypatch.setitem(sys.modules, "cdsapi", types.SimpleNamespace(Client=FakeClient))
    result = Fetcher(_request(start="2026-06-25"), config=_cfg(tmp_path, era5_reanalysis=ERA5)).download()

    assert result.summary.skipped == 1
    out_dir = result.output_dir / "ecmwf/era5"
    assert list(out_dir.glob("*.part")) == []
    assert list(out_dir.glob("era5_10m_wind_*.nc")) == []


def test_era5_plan_skips_dates_outside_declarative_coverage(tmp_path):
    result = Fetcher(_request(start="1900-01-01"), config=_cfg(tmp_path, era5_reanalysis=ERA5)).plan()

    assert result.summary.planned == 0
    assert result.summary.skipped == 1
    availability = result.results[0].details["availability"]
    assert availability["status"] == "unavailable"
    assert availability["method"] == "coverage"


# --- ECMWF Open Data --------------------------------------------------------


def test_ecmwf_downloads_grib_and_crops_to_region_with_stubbed_client(tmp_path, monkeypatch):
    calls = []

    class FakeClient:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        def retrieve(self, request, target):
            calls.append(("retrieve", request, target))
            Path(target).write_text("grib", encoding="utf-8")

    def fake_crop(raw_path, output_path, region, pad_deg, resolution, params):
        calls.append(("crop", raw_path, output_path, region, pad_deg, resolution, params))
        Path(output_path).write_text("netcdf", encoding="utf-8")
        return []

    monkeypatch.setattr("collekt.sources.ecmwf_open_data._client_class", lambda: FakeClient)
    monkeypatch.setattr("collekt.sources.ecmwf_open_data._crop_to_netcdf", fake_crop)
    result = Fetcher(_request(start="2026-06-25"), config=_cfg(tmp_path, ecmwf_open_data_forecast=ECMWF)).download()

    assert result.summary.downloaded == 1
    assert result.results[0].format == "netcdf"
    assert result.files[0].name.startswith("ecmwf_open_data_10m_wind_20260625_00z_")
    assert result.files[0].suffix == ".nc"
    assert calls[0] == ("init", {"source": "ecmwf", "model": "ifs", "resol": "0p25"})
    assert calls[1][1]["param"] == ["10u", "10v"]
    # 3h native cadence over the day -> steps every 3 hours.
    assert calls[1][1]["step"] == [0, 3, 6, 9, 12, 15, 18, 21]

    _, raw_path, output_path, region, pad_deg, resolution, params = calls[2]
    assert raw_path.name.endswith(".raw.grib2")
    assert not raw_path.exists()  # cleaned up after cropping
    assert output_path.name == result.files[0].name
    assert region == Region.from_bbox((-6, 20, 35, 45))
    assert pad_deg == 0.5
    assert resolution == 0.25
    assert params == ("10u", "10v")


def test_ecmwf_grid_resolution_parses_resol_label():
    from collekt.sources.ecmwf_open_data import _grid_resolution

    assert _grid_resolution("0p25") == 0.25
    assert _grid_resolution("0p4-beta") == 0.4


def test_ecmwf_crop_dataset_normalizes_longitude_and_clips_to_region():
    import numpy as np
    import xarray as xr

    from collekt.sources.ecmwf_open_data import _crop_dataset

    latitude = np.arange(90.0, -90.25, -0.25)
    longitude = np.arange(0.0, 360.0, 0.25)
    data = np.zeros((latitude.size, longitude.size))
    dataset = xr.Dataset(
        {"u10": (("latitude", "longitude"), data)},
        coords={"latitude": latitude, "longitude": longitude},
    )

    region = Region.from_bbox((-6, 20, 35, 45))
    cropped = _crop_dataset(dataset, region, pad_deg=0.5, resolution=0.25)

    assert float(cropped.longitude.min()) == -6.5
    assert float(cropped.longitude.max()) == 20.5
    assert float(cropped.latitude.min()) == 34.5
    assert float(cropped.latitude.max()) == 45.5


def _write_mixed_step_type_grib(path):
    """Write a tiny synthetic GRIB2 file mixing instantaneous winds (10u/10v) with a
    3-hourly running-max gust (10fg), reproducing the ECMWF Open Data combination that
    a plain `xr.open_dataset(..., engine="cfgrib")` cannot merge into one hypercube
    and silently drops instead of raising.
    """
    eccodes = pytest.importorskip("eccodes")

    def new_message():
        gid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
        eccodes.codes_set(gid, "Ni", 4)
        eccodes.codes_set(gid, "Nj", 3)
        eccodes.codes_set(gid, "latitudeOfFirstGridPointInDegrees", 46.0)
        eccodes.codes_set(gid, "longitudeOfFirstGridPointInDegrees", -8.0)
        eccodes.codes_set(gid, "latitudeOfLastGridPointInDegrees", 44.0)
        eccodes.codes_set(gid, "longitudeOfLastGridPointInDegrees", -5.0)
        eccodes.codes_set(gid, "iDirectionIncrementInDegrees", 1.0)
        eccodes.codes_set(gid, "jDirectionIncrementInDegrees", 1.0)
        eccodes.codes_set_values(gid, [1.0] * 12)
        eccodes.codes_set(gid, "dataDate", 20260101)
        eccodes.codes_set(gid, "dataTime", 0)
        return gid

    with open(path, "wb") as handle:
        for step in (0, 3):
            for number in (2, 3):  # u-component (10u), v-component (10v)
                gid = new_message()
                eccodes.codes_set(gid, "discipline", 0)
                eccodes.codes_set(gid, "parameterCategory", 2)
                eccodes.codes_set(gid, "parameterNumber", number)
                eccodes.codes_set(gid, "typeOfFirstFixedSurface", 103)
                eccodes.codes_set(gid, "scaledValueOfFirstFixedSurface", 10)
                eccodes.codes_set(gid, "scaleFactorOfFirstFixedSurface", 0)
                eccodes.codes_set(gid, "step", step)
                eccodes.codes_write(gid, handle)
                eccodes.codes_release(gid)

        gid = new_message()  # 10fg: 3-hourly running max, window ending at step=3
        eccodes.codes_set(gid, "productDefinitionTemplateNumber", 8)
        eccodes.codes_set(gid, "discipline", 0)
        eccodes.codes_set(gid, "parameterCategory", 2)
        eccodes.codes_set(gid, "parameterNumber", 22)
        eccodes.codes_set(gid, "typeOfFirstFixedSurface", 103)
        eccodes.codes_set(gid, "scaledValueOfFirstFixedSurface", 10)
        eccodes.codes_set(gid, "scaleFactorOfFirstFixedSurface", 0)
        eccodes.codes_set(gid, "forecastTime", 0)
        eccodes.codes_set(gid, "typeOfStatisticalProcessing", 2)  # maximum
        eccodes.codes_set(gid, "indicatorOfUnitForTimeRange", 1)  # hour
        eccodes.codes_set(gid, "lengthOfTimeRange", 3)
        eccodes.codes_set(gid, "yearOfEndOfOverallTimeInterval", 2026)
        eccodes.codes_set(gid, "monthOfEndOfOverallTimeInterval", 1)
        eccodes.codes_set(gid, "dayOfEndOfOverallTimeInterval", 1)
        eccodes.codes_set(gid, "hourOfEndOfOverallTimeInterval", 3)
        eccodes.codes_set(gid, "minuteOfEndOfOverallTimeInterval", 0)
        eccodes.codes_set(gid, "secondOfEndOfOverallTimeInterval", 0)
        eccodes.codes_set(gid, "numberOfTimeRange", 1)
        eccodes.codes_write(gid, handle)
        eccodes.codes_release(gid)


def test_ecmwf_crop_to_netcdf_keeps_time_processed_variables(tmp_path):
    xr = pytest.importorskip("xarray")
    from collekt.sources.ecmwf_open_data import _crop_to_netcdf

    raw_path = tmp_path / "raw.grib2"
    _write_mixed_step_type_grib(raw_path)
    output_path = tmp_path / "output.nc"
    region = Region.from_bbox((-7, -6, 44.5, 45.5))

    missing = _crop_to_netcdf(raw_path, output_path, region, pad_deg=0.0, resolution=1.0, params=("10u", "10v", "10fg"))

    assert missing == []
    with xr.open_dataset(output_path) as ds:
        assert {"10u", "10v", "10fg"} <= set(ds.data_vars)


def test_ecmwf_crop_to_netcdf_reports_params_missing_from_the_file(tmp_path):
    pytest.importorskip("xarray")
    from collekt.sources.ecmwf_open_data import _crop_to_netcdf

    raw_path = tmp_path / "raw.grib2"
    _write_mixed_step_type_grib(raw_path)
    output_path = tmp_path / "output.nc"
    region = Region.from_bbox((-7, -6, 44.5, 45.5))

    missing = _crop_to_netcdf(
        raw_path, output_path, region, pad_deg=0.0, resolution=1.0, params=("10u", "not_in_the_file")
    )

    assert missing == ["not_in_the_file"]


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


# --- GFS --------------------------------------------------------------------

GFS = {
    "kind": "gfs",
    "enabled": True,
    "path": "gfs/analysis",
    "filename_pattern": "gfs_analysis_{date:%Y%m%d}_{bbox_hash}.nc",
    "dataset_id": "gfs-analysis",
    "variables": ["20u", "20v"],
    "temporal_sampling": "6h",
    "coverage": {"start": "now-9d", "end": "now"},
    "stream": "anl",
    "resolution": "0p25",
    "cycles": [0, 6, 12, 18],
    "pad_deg": 0.5,
}


def _stub_gfs_download(monkeypatch, calls, *, fail_hours=()):
    def download(url, target):
        calls.append(url)
        if any(f"gfs.t{hour:02d}z" in url for hour in fail_hours):
            raise ValueError("not available yet")
        Path(target).write_text("grib", encoding="utf-8")

    monkeypatch.setattr("collekt.sources.gfs._download_cycle", download)

    def concat(raw_paths, output_path, variables):
        Path(output_path).write_text("netcdf", encoding="utf-8")
        return []

    monkeypatch.setattr("collekt.sources.gfs._concat_cycles_to_netcdf", concat)


def test_gfs_downloads_four_analysis_cycles_per_day(tmp_path, monkeypatch):
    calls = []
    _stub_gfs_download(monkeypatch, calls)
    result = Fetcher(_request(start=date.today().isoformat()), config=_cfg(tmp_path, gfs_analysis=GFS)).download()

    assert result.summary.downloaded == 1
    assert result.results[0].format == "netcdf"
    assert result.results[0].details["cycles"] == 4
    assert [url.split("file=")[1].split("&")[0] for url in calls] == [
        "gfs.t00z.pgrb2.0p25.anl",
        "gfs.t06z.pgrb2.0p25.anl",
        "gfs.t12z.pgrb2.0p25.anl",
        "gfs.t18z.pgrb2.0p25.anl",
    ]


def test_gfs_url_requests_wind_levels_and_server_side_subregion(tmp_path, monkeypatch):
    calls = []
    _stub_gfs_download(monkeypatch, calls)
    Fetcher(_request(start=date.today().isoformat()), config=_cfg(tmp_path, gfs_analysis=GFS)).download()

    url = calls[0]
    assert "filter_gfs_0p25.pl" in url
    assert "var_UGRD=on" in url and "var_VGRD=on" in url
    # Both components share one level request, not one per variable.
    assert url.count("lev_20_m_above_ground=on") == 1
    # Padded and snapped to the 0.25 degree native grid.
    assert "leftlon=-6.5" in url and "rightlon=20.5" in url
    assert "bottomlat=34.5" in url and "toplat=45.5" in url


def test_gfs_skips_cycles_that_are_not_published_yet(tmp_path, monkeypatch):
    """A late analysis must not fail the day: the earlier cycles are still written."""
    calls = []
    _stub_gfs_download(monkeypatch, calls, fail_hours=(12, 18))
    result = Fetcher(_request(start=date.today().isoformat()), config=_cfg(tmp_path, gfs_analysis=GFS)).download()

    assert result.summary.downloaded == 1
    assert result.results[0].details["cycles"] == 2
    assert "cycles not available yet" in result.results[0].message
    assert "12z" in result.results[0].message and "18z" in result.results[0].message


def test_gfs_skips_the_day_when_no_cycle_is_available(tmp_path, monkeypatch):
    calls = []
    _stub_gfs_download(monkeypatch, calls, fail_hours=(0, 6, 12, 18))
    result = Fetcher(_request(start=date.today().isoformat()), config=_cfg(tmp_path, gfs_analysis=GFS)).download()

    assert result.summary.downloaded == 0
    assert result.results[0].status is SourceStatus.SKIPPED
    assert "no cycle available" in result.results[0].message


def test_gfs_plan_reports_cycles_without_downloading(tmp_path):
    result = Fetcher(_request(start=date.today().isoformat()), config=_cfg(tmp_path, gfs_analysis=GFS)).plan()

    assert result.summary.planned == 1
    request_details = result.results[0].details["request"]
    assert request_details["cycles"] == ["00z", "06z", "12z", "18z"]
    assert result.results[0].details["crop"] == {"south": 34.5, "north": 45.5, "west": -6.5, "east": 20.5}


def test_gfs_plan_skips_dates_outside_nomads_retention(tmp_path):
    result = Fetcher(_request(start="2023-06-15"), config=_cfg(tmp_path, gfs_analysis=GFS)).plan()

    assert result.results[0].status is SourceStatus.SKIPPED


def test_gfs_rejects_variables_that_are_not_wind_mnemonics():
    from collekt.sources.gfs import UnknownVariableError, _parse_variable

    assert _parse_variable("20u") == ("UGRD", 20)
    assert _parse_variable("100V") == ("VGRD", 100)
    with pytest.raises(UnknownVariableError):
        _parse_variable("10m_u_component_of_wind")


def test_gfs_renames_height_level_to_requested_mnemonics():
    import numpy as np
    import xarray as xr

    from collekt.sources.gfs import _rename_to_requested_variables

    # cfgrib decodes GFS wind above 10 m as bare u/v with the height as a coord.
    dataset = xr.Dataset(
        {
            "u": (("latitude", "longitude"), np.ones((2, 3))),
            "v": (("latitude", "longitude"), np.zeros((2, 3))),
        },
        coords={"latitude": [40.0, 41.0], "longitude": [1.0, 2.0, 3.0], "heightAboveGround": 20.0},
    )

    renamed = _rename_to_requested_variables(dataset, ("20u", "20v"))

    assert set(renamed.data_vars) == {"20u", "20v"}
    assert "heightAboveGround" not in renamed.coords
    assert float(renamed["20u"].mean()) == 1.0


def test_gfs_selects_each_height_from_a_multi_level_dimension():
    """A multi-height request decodes as one u/v pair over a heightAboveGround dim."""
    import numpy as np
    import xarray as xr

    from collekt.sources.gfs import _rename_to_requested_variables

    heights = np.array([20.0, 100.0])
    dataset = xr.Dataset(
        {
            "u": (("heightAboveGround", "latitude"), np.array([[1.0, 1.0], [2.0, 2.0]])),
            "v": (("heightAboveGround", "latitude"), np.array([[3.0, 3.0], [4.0, 4.0]])),
        },
        coords={"heightAboveGround": heights, "latitude": [40.0, 41.0]},
    )

    renamed = _rename_to_requested_variables(dataset, ("20u", "20v", "100u", "100v"))

    assert set(renamed.data_vars) == {"20u", "20v", "100u", "100v"}
    assert "heightAboveGround" not in renamed.dims
    assert float(renamed["20u"].mean()) == 1.0
    assert float(renamed["100u"].mean()) == 2.0
    assert float(renamed["100v"].mean()) == 4.0
