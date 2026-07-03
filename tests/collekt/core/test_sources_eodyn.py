"""Offline tests for the eOdyn adapter (archive interim mode + API seam).

The coverage/skip/plan paths need no heavy dependencies. The archive-read path
needs xarray/numpy and is guarded with importorskip.
"""

import pytest

from collekt import Fetcher
from collekt.core.config import get_config
from collekt.core.request import Region, Request
from collekt.sources import EodynArchiveWarning
from collekt.sources.base import SourceStatus


def _config(tmp_path, archive_root, **override):
    source = {
        "kind": "eodyn",
        "enabled": True,
        "path": "eodyn/osmose",
        "filename_pattern": "osmose_{dataset_id}_{date:%Y%m%d}_{bbox_hash}.nc",
        "dataset_id": "EODYN-OS-VELOCITY-L4",
        "mode": "archive",
        "archive_root": str(archive_root),
        "day_pattern": "{date:%Y%m%d}_MARES/{date:%Y%m%d}_MARES_osmose_L4.nc",
        "coverage": {
            "west": -6.0,
            "east": 20.0,
            "south": 35.0,
            "north": 45.0,
            "start": "2023-04-01",
            "end": "2023-08-31",
        },
        "variables": ["totalewct", "totalnsct"],
        "temporal_sampling": "24h",
    }
    source.update(override)
    return get_config(
        overrides={
            "source_catalogs": [],
            "output": {"root": str(tmp_path / "out")},
            "sources": {"eodyn_osmose_currents": source},
        }
    )


def _request(bbox=(0, 8, 38, 42), start="2023-06-15", end=None):
    return Request(region=Region.from_bbox(bbox), start=start, end=end)


def _write_archive(archive_root, day="20230615"):
    import numpy as np
    import xarray as xr

    day_dir = archive_root / f"{day}_MARES"
    day_dir.mkdir(parents=True, exist_ok=True)
    lat = np.linspace(35.0, 45.0, 11)
    lon = np.linspace(-6.0, 20.0, 27)
    time = np.array([f"{day[:4]}-{day[4:6]}-{day[6:]}T00:00:00"], dtype="datetime64[ns]")
    field = np.ones((1, lat.size, lon.size), dtype="float32")
    ds = xr.Dataset(
        {
            name: (("time", "latitude", "longitude"), field.copy())
            for name in ("totalewct", "totalnsct", "ewct", "nsct")
        },
        coords={"time": time, "latitude": lat, "longitude": lon},
    )
    ds.to_netcdf(day_dir / f"{day}_MARES_osmose_L4.nc")


def test_eodyn_skips_out_of_region(tmp_path):
    cfg = _config(tmp_path, tmp_path / "archive")
    result = Fetcher(_request(bbox=(-40, -30, 40, 45)), config=cfg).download()

    assert result.summary.downloaded == 0
    assert result.summary.skipped == 1
    assert "outside the OSmose preview coverage" in result.results[0].message


def test_eodyn_skips_out_of_date_range(tmp_path):
    cfg = _config(tmp_path, tmp_path / "archive")
    result = Fetcher(_request(start="2024-01-15"), config=cfg).download()

    assert result.summary.skipped == 1
    assert "outside the OSmose preview archive" in result.results[0].message


def test_eodyn_skips_missing_archive_file(tmp_path):
    archive = tmp_path / "archive"
    archive.mkdir()
    result = Fetcher(_request(), config=_config(tmp_path, archive)).download()

    assert result.summary.skipped == 1
    assert "archive file not found" in result.results[0].message


def test_eodyn_api_mode_is_not_available_yet(tmp_path):
    cfg = _config(tmp_path, tmp_path / "archive", mode="api")
    result = Fetcher(_request(), config=cfg).download()

    assert result.summary.skipped == 1
    assert "API is not available yet" in result.results[0].message


def test_eodyn_dry_run_plans_days_and_skips_outside_coverage(tmp_path):
    cfg = _config(tmp_path, tmp_path / "archive")
    request = _request(start="2023-08-30", end="2023-09-02")
    result = Fetcher(request, config=cfg).plan()

    assert result.summary.planned == 2
    assert result.summary.skipped == 2
    planned = [r for r in result.results if r.status == SourceStatus.PLANNED]
    assert planned[0].details["provider"] == "eodyn-osmose"
    assert planned[0].details["request"]["coverage"]["end"] == "2023-08-31"
    assert not result.output_dir.exists()


def test_eodyn_reads_archive_and_subsets(tmp_path):
    pytest.importorskip("xarray")
    pytest.importorskip("numpy")
    xr = pytest.importorskip("xarray")

    archive = tmp_path / "archive"
    _write_archive(archive)
    cfg = _config(tmp_path, archive)

    with pytest.warns(EodynArchiveWarning):
        result = Fetcher(_request(), config=cfg).download()

    assert result.summary.downloaded == 1
    with xr.open_dataset(result.files[0]) as ds:
        assert sorted(ds.data_vars) == ["totalewct", "totalnsct"]
        assert float(ds.latitude.min()) >= 38.0
        assert float(ds.longitude.max()) <= 8.0
