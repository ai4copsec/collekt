"""Tests for the freshest-IFS-value adapter (`ecmwf_open_data_latest`). Hermetic: no network."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from collekt import Fetcher
from collekt.core.config import get_config
from collekt.core.request import Region, Request
from collekt.sources import ecmwf_open_data_latest as latest_adapter
from collekt.sources.base import SourceStatus

NOW = datetime(2026, 11, 27, 9, 50, tzinfo=UTC)
REGION = Region.from_bbox((2.0, 4.0, 39.0, 40.5))

LATEST = {
    "kind": "ecmwf_open_data_latest",
    "enabled": True,
    "path": "ecmwf/ifs_latest",
    "filename_pattern": "ifs_{date:%Y%m%d}_{hours}_{run}.nc",
    "dataset_id": "ecmwf-open-data-ifs",
    "variables": ["10u", "10v"],
    "temporal_sampling": "3h",
    "coverage": {"start": "2026-05-12", "end": "now+5d"},
    "source": "google",
    "resol": "0p25",
}


def _cfg(tmp_path):
    return get_config(
        overrides={"source_catalogs": [], "output": {"root": str(tmp_path)}, "sources": {"ifs_latest": LATEST}}
    )


def _last_72h():
    return Request(region=REGION, start=NOW - timedelta(hours=72), end=NOW)


def _utc(*args):
    return datetime(*args, tzinfo=UTC)


@pytest.fixture
def newest_run(monkeypatch):
    """Freeze the clock at NOW and let each test set the newest published run."""
    state = {"run": _utc(2026, 11, 27, 0), "probes": 0, "now": NOW}

    def probe(source):
        state["probes"] += 1
        if isinstance(state["run"], Exception):
            raise state["run"]
        return state["run"]

    monkeypatch.setattr(latest_adapter, "_now", lambda: state["now"])
    monkeypatch.setattr(latest_adapter, "_newest_published_run", probe)
    return state


@pytest.fixture
def fake_provider(monkeypatch):
    """Stub the ECMWF client and the GRIB cropping with small NetCDF pieces."""
    import numpy as np
    import xarray as xr

    retrieved = []

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        def retrieve(self, body, target):
            retrieved.append((body["date"], body["time"], body["step"]))
            Path(target).write_text("grib", encoding="utf-8")

    def fake_crop(raw_path, output_path, region, pad_deg, resolution, params):
        date, hour, step = retrieved[-1]
        run = np.datetime64(f"{date[:4]}-{date[4:6]}-{date[6:]}T{hour:02d}:00", "ns")
        values = np.full((2, 3), float(step))
        xr.Dataset(
            {name: (("latitude", "longitude"), values) for name in params},
            coords={
                "latitude": [39.0, 40.0],
                "longitude": [2.0, 3.0, 4.0],
                "time": run,
                "step": np.timedelta64(step, "h"),
                "valid_time": run + np.timedelta64(step, "h"),
            },
        ).to_netcdf(output_path)
        return []

    monkeypatch.setattr(latest_adapter, "_client_class", lambda: FakeClient)
    monkeypatch.setattr(latest_adapter, "_crop_to_netcdf", fake_crop)
    return retrieved


def test_slot_times_snap_to_the_source_grid():
    times = latest_adapter._slot_times(_last_72h(), timedelta(hours=3))
    assert times[0] == _utc(2026, 11, 24, 12)
    assert times[-1] == _utc(2026, 11, 27, 9)
    assert len(times) == 24


def test_times_are_read_from_their_own_run_then_from_the_newest_forecast(newest_run):
    _, by_day = latest_adapter._resolve(_last_72h(), get_config(overrides={"sources": {"x": LATEST}}).sources["x"])
    slots = [slot for day in by_day.values() for slot in day]
    read_from = {slot.time: (slot.run, slot.step) for slot in slots}
    assert read_from[_utc(2026, 11, 24, 12)] == (_utc(2026, 11, 24, 12), 0)
    assert read_from[_utc(2026, 11, 26, 21)] == (_utc(2026, 11, 26, 18), 3)
    assert read_from[_utc(2026, 11, 27, 3)] == (_utc(2026, 11, 27, 0), 3)
    # The 06 UTC run is not published yet: 06 and 09 UTC come from the 00 UTC forecast.
    assert read_from[_utc(2026, 11, 27, 6)] == (_utc(2026, 11, 27, 0), 6)
    assert read_from[_utc(2026, 11, 27, 9)] == (_utc(2026, 11, 27, 0), 9)
    assert newest_run["probes"] == 1


def test_old_requests_do_not_probe_for_the_newest_run(newest_run, tmp_path):
    newest_run["run"] = AssertionError("must not probe")
    result = Fetcher(Request(region=REGION, start="2026-11-01"), config=_cfg(tmp_path)).plan()
    assert [item.status for item in result.results] == [SourceStatus.PLANNED]
    assert result.results[0].path.name == "ifs_20261101_00-21_2026110118.nc"


def test_lead_times_the_run_does_not_publish_are_unavailable():
    slot = latest_adapter._resolve_slot(_utc(2026, 11, 27, 9), _utc(2026, 11, 21, 6), None)
    assert slot.run is None  # +147 h: the 06 UTC run stops at 144 h
    assert "no lead time" in slot.reason


def test_a_failed_probe_makes_only_recent_times_unavailable(newest_run, tmp_path):
    newest_run["run"] = ConnectionError("mirror down")
    results = Fetcher(_last_72h(), config=_cfg(tmp_path)).plan().results
    statuses = {item.day: item.status for item in results}
    assert statuses == {
        "2026-11-24": SourceStatus.PLANNED,
        "2026-11-25": SourceStatus.PLANNED,
        "2026-11-26": SourceStatus.PLANNED,
        "2026-11-27": SourceStatus.SKIPPED,
    }
    # Runs a day old are certainly published: 26 November is read up to its 06 UTC run.
    assert results[2].path.name == "ifs_20261126_00-09_2026112606.nc"
    assert "no value at 12, 15, 18, 21 UTC" in results[2].message
    assert "mirror down" in results[3].message


def test_plan_lists_the_run_and_lead_time_of_each_time_step(newest_run, tmp_path):
    result = Fetcher(_last_72h(), config=_cfg(tmp_path)).plan()
    today = result.results[-1]
    assert today.path.name == "ifs_20261127_00-09_2026112700.nc"
    assert today.details["slots"][-1] == {"time": "2026-11-27T09:00:00Z", "run": "2026-11-27T00:00:00Z", "step": 9}
    assert not today.path.exists()


def test_fetch_reuses_pieces_and_refreshes_forecast_values_when_a_newer_run_is_published(
    newest_run, fake_provider, tmp_path
):
    import xarray as xr

    first = Fetcher(_last_72h(), config=_cfg(tmp_path)).download()
    assert [item.status for item in first.results] == [SourceStatus.DOWNLOADED] * 4
    assert len(fake_provider) == 24

    again = Fetcher(_last_72h(), config=_cfg(tmp_path)).download()
    assert [item.status for item in again.results] == [SourceStatus.REUSED] * 4
    assert len(fake_provider) == 24

    newest_run["run"] = _utc(2026, 11, 27, 6)
    fresher = Fetcher(_last_72h(), config=_cfg(tmp_path)).download()
    assert [item.status for item in fresher.results] == [SourceStatus.REUSED] * 3 + [SourceStatus.DOWNLOADED]
    assert fake_provider[24:] == [("20261127", 6, 0), ("20261127", 6, 3)]

    # The window moves on 3 hours: only the new 12 UTC value is downloaded (+12 h of the 06 UTC run).
    newest_run["now"] = NOW + timedelta(hours=3)
    later = Request(region=REGION, start=NOW - timedelta(hours=69), end=NOW + timedelta(hours=3))
    rolled = Fetcher(later, config=_cfg(tmp_path)).download()
    # 24 November now starts at 15 UTC and 27 November ends at 12 UTC: both are rebuilt from cached pieces.
    statuses = [item.status for item in rolled.results]
    assert statuses == [SourceStatus.DOWNLOADED, SourceStatus.REUSED, SourceStatus.REUSED, SourceStatus.DOWNLOADED]
    assert fake_provider[26:] == [("20261127", 6, 6)]
    assert len(list((tmp_path / "ecmwf/ifs_latest/pieces").glob("*.nc"))) == 27

    today = fresher.results[-1].path
    assert today.name == "ifs_20261127_00-09_2026112706.nc"
    with xr.open_dataset(today) as day:
        assert day["forecast_step"].values.tolist() == [0, 3, 0, 3]
        assert [str(run)[:13] for run in day["source_cycle"].values] == [
            "2026-11-27T00",
            "2026-11-27T00",
            "2026-11-27T06",
            "2026-11-27T06",
        ]
        assert day["10u"].sizes == {"valid_time": 4, "latitude": 2, "longitude": 3}


def test_a_failed_lead_time_leaves_a_gap_that_the_next_request_fills(newest_run, fake_provider, monkeypatch, tmp_path):
    working = latest_adapter._client_class()

    class Flaky(working):
        def retrieve(self, body, target):
            if (body["time"], body["step"]) == (0, 6):  # 27 November 06 UTC, from the 00 UTC run
                raise ConnectionError("mirror hiccup")
            super().retrieve(body, target)

    monkeypatch.setattr(latest_adapter, "_client_class", lambda: Flaky)
    results = Fetcher(_last_72h(), config=_cfg(tmp_path)).download().results
    assert [item.status for item in results] == [SourceStatus.DOWNLOADED] * 4
    today = results[-1]
    assert today.path.name == "ifs_20261127_00-03_09_2026112700.nc"
    assert today.message == "no value at 06 UTC: download failed: mirror hiccup"
    assert today.details["slots"][2] == {
        "time": "2026-11-27T06:00:00Z",
        "unavailable": "download failed: mirror hiccup",
    }

    monkeypatch.setattr(latest_adapter, "_client_class", lambda: working)
    downloaded = len(fake_provider)
    retried = Fetcher(_last_72h(), config=_cfg(tmp_path)).download().results[-1]
    assert retried.status == SourceStatus.DOWNLOADED
    assert retried.path.name == "ifs_20261127_00-09_2026112700.nc"
    assert fake_provider[downloaded:] == [("20261127", 0, 6)]


def test_an_interrupted_piece_write_is_not_cached(newest_run, fake_provider, monkeypatch, tmp_path):
    def interrupted_crop(raw_path, output_path, *args):
        Path(output_path).write_text("half a file", encoding="utf-8")
        raise OSError("disk full")

    monkeypatch.setattr(latest_adapter, "_crop_to_netcdf", interrupted_crop)
    results = Fetcher(_last_72h(), config=_cfg(tmp_path)).download().results
    assert {item.status for item in results} == {SourceStatus.SKIPPED}
    assert "download failed: disk full" in results[0].message
    assert list((tmp_path / "ecmwf/ifs_latest/pieces").iterdir()) == []


def test_bundled_catalog_offers_both_resolutions():
    sources = get_config().sources
    assert sources["ecmwf_ifs_latest_0p25"].kind == sources["ecmwf_ifs_latest_0p1"].kind == "ecmwf_open_data_latest"
    assert sources["ecmwf_ifs_latest_0p1"].raw["resol"] == "0p1"
    assert sources["ecmwf_ifs_latest_0p1"].raw["coverage"]["start"] == "2026-10-21"
