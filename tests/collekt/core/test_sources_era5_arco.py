"""Offline tests for the ARCO-ERA5 adapter: the Zarr store is replaced by a small in-memory global grid."""

import numpy as np
import pytest
import xarray as xr

from collekt import ERA5ARCO, DatasetConfig, Fetcher
from collekt.core.config import get_config
from collekt.core.request import Region, Request
from collekt.sources import era5_arco
from collekt.sources.base import SourceStatus

ARCO = {
    "kind": "era5_arco",
    "enabled": True,
    "path": "ecmwf/era5_arco",
    "filename_pattern": "era5_arco_{date:%Y%m%d}_{bbox_hash}.nc",
    "dataset_id": "arco-era5",
    "variables": ["10m_u_component_of_wind", "sea_surface_temperature"],
    "temporal_sampling": "1h",
    "coverage": {"start": "1940-01-01", "end": "now-7d"},
    "pad_deg": 0.5,
}


def _store(stop="2026-06-30", empty_sst_hour=None):
    """A global 1-degree grid laid out like the ARCO store: latitude 90..-90, longitude 0..359."""
    time = np.arange("2026-06-01T00", "2026-07-11T00", dtype="datetime64[h]").astype("datetime64[ns]")
    latitude = np.arange(90.0, -91.0, -1.0)
    longitude = np.arange(0.0, 360.0, 1.0)
    shape = (len(time), len(latitude), len(longitude))
    u10 = np.broadcast_to(longitude, shape).astype("float32")  # value = native 0-360 longitude
    sst = np.full(shape, 290.0, dtype="float32")
    if empty_sst_hour is not None:
        sst[time == np.datetime64(empty_sst_hour)] = np.nan
    dims = ("time", "latitude", "longitude")
    return xr.Dataset(
        {
            "10m_u_component_of_wind": (dims, u10, {"short_name": "u10"}),
            "sea_surface_temperature": (dims, sst, {"short_name": "sst"}),
        },
        coords={"time": time, "latitude": latitude, "longitude": longitude},
        attrs={"valid_time_start": "1940-01-01", "valid_time_stop": "2026-03-31", "valid_time_stop_era5t": stop},
    )


@pytest.fixture
def store(monkeypatch):
    opened = []

    def install(**kwargs):
        dataset = _store(**kwargs)
        monkeypatch.setattr(era5_arco, "_open_store", lambda url: opened.append(url) or dataset)
        return opened

    return install


def _fetch(tmp_path, start="2026-06-25", end=None, batch_days=None, **source):
    cfg = get_config(
        overrides={"source_catalogs": [], "output": {"root": str(tmp_path)}, "sources": {"arco": ARCO | source}}
    )
    request = Request(region=Region.from_bbox((-6, 20, 35, 45)), start=start, end=end)
    return Fetcher(request, config=cfg, batch_days=batch_days).download()


def test_download_crops_global_store_across_greenwich(tmp_path, store):
    opened = store()
    result = _fetch(tmp_path)

    assert result.summary.downloaded == 1
    assert opened == [era5_arco.DEFAULT_STORE]
    with xr.open_dataset(result.files[0]) as daily:
        assert set(daily.data_vars) == {"u10", "sst"}  # the CDS NetCDF names
        assert daily.sizes["time"] == 24
        assert [float(daily.latitude[0]), float(daily.latitude[-1])] == [45.0, 35.0]
        # The padded bbox is -6.5..20.5; the 1-degree test grid keeps -6..20, wrapped from 354..20.
        assert daily.longitude.values.tolist() == list(range(-6, 21))
        assert daily["u10"].isel(time=0, latitude=0).values[0] == 354.0


def test_days_after_the_store_stop_are_skipped(tmp_path, store):
    store(stop="2026-06-25")
    result = _fetch(tmp_path, start="2026-06-25", end="2026-06-26T23:00")

    statuses = [item.status for item in result.results]
    assert statuses == [SourceStatus.DOWNLOADED, SourceStatus.SKIPPED]
    assert "currently ends on 2026-06-25" in result.results[1].message
    assert len(result.files) == 1


def test_unknown_variable_is_skipped(tmp_path, store):
    store()
    result = _fetch(tmp_path, variables=["10m_u_component_of_wind", "not_a_variable"])

    assert result.summary.skipped == 1
    assert "not_a_variable" in result.results[0].message


def test_empty_timestamps_are_reported(tmp_path, store):
    store(empty_sst_hour="2026-06-25T05")
    result = _fetch(tmp_path)

    assert result.results[0].status == SourceStatus.DOWNLOADED
    assert "sst" in result.results[0].message


def test_batch_reads_once_and_matches_daily_downloads(tmp_path, store):
    opened = store()
    daily = _fetch(tmp_path / "daily", end="2026-06-27T23:00")
    batched = _fetch(tmp_path / "batched", end="2026-06-27T23:00", batch_days=3)

    assert len(opened) == 3 + 1
    assert batched.summary.downloaded == 3
    for one, other in zip(daily.files, batched.files, strict=True):
        assert one.name == other.name
        with xr.open_dataset(one) as a, xr.open_dataset(other) as b:
            xr.testing.assert_identical(a, b)


def test_bundled_catalog_entry_resolves():
    config = DatasetConfig(ERA5ARCO("era5_arco_reanalysis", variables=["10m_u_component_of_wind"])).resolve()

    source = config.sources["era5_arco_reanalysis"]
    assert source.kind == "era5_arco"
    assert source.raw["store"] == era5_arco.DEFAULT_STORE
