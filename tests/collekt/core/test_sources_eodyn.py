"""Offline tests for the eOdyn adapter, served by the `fake_oda` client (see conftest)."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
import xarray as xr

from collekt import Fetcher
from collekt.core.config import get_config
from collekt.core.diagnostics import DoctorStatus
from collekt.core.request import Region, Request
from collekt.sources import eodyn
from collekt.sources.base import SourceStatus

ODA_DATASET = "current-os-osmose-l4"


def _config(tmp_path, **override):
    source = {
        "kind": "eodyn",
        "enabled": True,
        "path": "eodyn/osmose",
        "filename_pattern": "osmose_{dataset_id}_{date:%Y%m%d}_{bbox_hash}.nc",
        "dataset_id": "EODYN-OS-VELOCITY-L4",
        "oda_dataset": ODA_DATASET,
        "coverage": {"west": -6.0, "east": 20.0, "south": 35.0, "north": 45.0, "start": "2026-11-20"},
        "variables": ["totalewct", "totalnsct"],
        "temporal_sampling": "3h",
    }
    source.update(override)
    return get_config(
        overrides={
            "source_catalogs": [],
            "output": {"root": str(tmp_path / "out")},
            "sources": {"eodyn_osmose_currents": source},
        }
    )


def _request(bbox=(0, 8, 38, 42), start="2026-11-21", end="2026-11-21"):
    return Request(region=Region.from_bbox(bbox), start=start, end=end)


def _at(day, hour):
    return datetime(2026, 11, day, hour, tzinfo=UTC)


def _area(west, east, south, north):
    ring = [(west, south), (east, south), (east, north), (west, north), (west, south)]
    return SimpleNamespace(coordinates=[[SimpleNamespace(lon=lon, lat=lat) for lon, lat in ring]])


@pytest.fixture(autouse=True)
def _no_ambient_credentials(monkeypatch):
    # A developer's .env may hold real ODA credentials; never let a test reach ODA.
    monkeypatch.delenv("ODA_USERNAME", raising=False)
    monkeypatch.delenv("ODA_PASSWORD", raising=False)


def test_eodyn_writes_one_cropped_file_per_day(tmp_path, fake_oda):
    documents = fake_oda([_at(20, 21), _at(21, 0), _at(21, 3)])

    result = Fetcher(_request(), config=_config(tmp_path)).download()

    assert result.summary.downloaded == 1
    # ODA also matches the previous day's 21:00 file (its window overlaps
    # midnight); only analysis times inside the day are downloaded.
    assert documents.downloads == [
        "OMNISITU_OSMOSE_L4_MED_2026-11-21T00-00-00-PT3H.nc",
        "OMNISITU_OSMOSE_L4_MED_2026-11-21T03-00-00-PT3H.nc",
    ]
    assert set(documents.queries) == {ODA_DATASET}
    with xr.open_dataset(result.files[0]) as ds:
        assert sorted(ds.data_vars) == ["totalewct", "totalnsct"]
        assert ds.time.dt.hour.values.tolist() == [0, 3]
        assert ds.totalewct.isel(time=1).values.max() == 3.0
        assert (float(ds.longitude.min()), float(ds.longitude.max())) == (0.0, 8.0)
        assert (float(ds.latitude.min()), float(ds.latitude.max())) == (38.0, 42.0)
        # Attributes shared by every time step survive; per-file ones do not.
        assert ds.attrs["id"] == "EODYN-OS-VELOCITY-L4"
        assert "analysis_time" not in ds.attrs


def test_eodyn_honours_the_request_window_within_a_day(tmp_path, fake_oda):
    documents = fake_oda([_at(21, 0), _at(21, 3), _at(21, 6)])

    result = Fetcher(_request(start="2026-11-21T02:00", end="2026-11-21T04:00"), config=_config(tmp_path)).download()

    assert result.summary.downloaded == 1
    assert documents.downloads == ["OMNISITU_OSMOSE_L4_MED_2026-11-21T03-00-00-PT3H.nc"]
    # A single time step drops per-analysis attributes like a whole day does.
    with xr.open_dataset(result.files[0]) as ds:
        assert ds.attrs["id"] == "EODYN-OS-VELOCITY-L4"
        assert "analysis_time" not in ds.attrs


def test_eodyn_reads_analysis_times_without_a_time_zone_as_utc(tmp_path, fake_oda):
    documents = fake_oda([_at(20, 21), _at(21, 0), _at(21, 3)])
    for document in documents.documents:
        document.start_datetime = document.start_datetime.replace(tzinfo=None)

    result = Fetcher(_request(), config=_config(tmp_path)).download()

    assert result.summary.downloaded == 1
    assert documents.downloads == [
        "OMNISITU_OSMOSE_L4_MED_2026-11-21T00-00-00-PT3H.nc",
        "OMNISITU_OSMOSE_L4_MED_2026-11-21T03-00-00-PT3H.nc",
    ]
    planned = Fetcher(_request(), config=_config(tmp_path)).plan().results[0]
    assert planned.details["availability"]["method"] == "describe"
    assert planned.details["availability"]["coverage"]["end"] == "2026-11-21"


def test_eodyn_skips_days_without_files(tmp_path, fake_oda):
    fake_oda([_at(21, 0)])

    result = Fetcher(_request(start="2026-11-21", end="2026-11-22"), config=_config(tmp_path)).download()

    assert result.summary.downloaded == 1
    assert result.summary.skipped == 1
    assert f"no file on ODA ({ODA_DATASET}) for 2026-11-22" in result.results[1].message


def test_eodyn_skips_regions_outside_the_footprint_without_downloading(tmp_path, fake_oda):
    documents = fake_oda([_at(21, 0)])

    result = Fetcher(_request(bbox=(-40, -30, 40, 45)), config=_config(tmp_path)).download()

    assert result.summary.skipped == 1
    assert "outside the footprint" in result.results[0].message
    assert documents.downloads == []


def test_eodyn_explains_regions_between_grid_points_after_one_download(tmp_path, fake_oda):
    documents = fake_oda([_at(21, 0), _at(21, 3)])

    result = Fetcher(_request(bbox=(3.2, 3.4, 40.2, 40.4)), config=_config(tmp_path)).download()

    assert result.summary.skipped == 1
    assert "grid spacing is 1°; widen the region" in result.results[0].message
    assert len(documents.downloads) == 1


def test_eodyn_reports_requested_variables_missing_from_the_files(tmp_path, fake_oda):
    fake_oda([_at(21, 0)])
    cfg = _config(tmp_path, variables=["totalewct", "posterior_std"])

    result = Fetcher(_request(), config=cfg).download()

    assert result.summary.downloaded == 1
    assert result.results[0].message == "missing from the ODA files: posterior_std"


def test_eodyn_skips_without_credentials(tmp_path, monkeypatch):
    def unexpected_client():
        raise AssertionError("ODA must not be contacted without credentials")

    monkeypatch.setattr(eodyn, "_oda_client", unexpected_client)

    result = Fetcher(_request(), config=_config(tmp_path)).download()

    assert result.summary.skipped == 1
    assert "set ODA_USERNAME and ODA_PASSWORD" in result.results[0].message


def test_eodyn_reuses_cached_days_without_contacting_oda(tmp_path, fake_oda):
    documents = fake_oda([_at(21, 0)])
    cfg = _config(tmp_path)
    Fetcher(_request(), config=cfg).download()

    documents.queries.clear()
    result = Fetcher(_request(), config=cfg).download()

    assert result.results[0].status == SourceStatus.REUSED
    assert documents.queries == []


def test_eodyn_plan_reads_coverage_from_oda(tmp_path, fake_oda):
    fake_oda([_at(20, 0), _at(21, 21)])

    result = Fetcher(_request(start="2026-11-21", end="2026-11-22"), config=_config(tmp_path)).plan()

    planned, skipped = result.results
    assert planned.status == SourceStatus.PLANNED
    assert planned.details["availability"]["method"] == "describe"
    assert planned.details["availability"]["coverage"]["end"] == "2026-11-21"
    assert planned.details["request"]["oda_dataset"] == ODA_DATASET
    assert skipped.status == SourceStatus.SKIPPED
    assert not result.output_dir.exists()


def test_eodyn_plan_falls_back_to_catalog_coverage_without_credentials(tmp_path):
    result = Fetcher(_request(start="2026-11-19", end="2026-11-20"), config=_config(tmp_path)).plan()

    skipped, planned = result.results
    assert skipped.status == SourceStatus.SKIPPED
    assert planned.status == SourceStatus.PLANNED
    assert planned.details["availability"]["method"] == "coverage"
    assert planned.message == (
        "set ODA_USERNAME and ODA_PASSWORD (shell or .env) to read eOdyn currents from ODA; "
        "planned from the catalog coverage"
    )
    assert planned.details["availability"]["reason"] == planned.message


def test_eodyn_plan_says_why_oda_could_not_be_used(tmp_path, monkeypatch):
    def failing_client():
        raise RuntimeError("401 Unauthorized")

    monkeypatch.setenv("ODA_USERNAME", "user")
    monkeypatch.setenv("ODA_PASSWORD", "wrong")
    monkeypatch.setattr(eodyn, "_oda_client", failing_client)

    planned = Fetcher(_request(), config=_config(tmp_path)).plan().results[0]

    assert planned.status == SourceStatus.PLANNED
    assert planned.details["availability"]["method"] == "coverage"
    assert planned.message == "could not connect to ODA: 401 Unauthorized; planned from the catalog coverage"


def test_eodyn_plan_spans_the_footprints_of_the_first_and_last_files(tmp_path, fake_oda):
    documents = fake_oda([_at(20, 0), _at(21, 21)])
    documents.documents[-1].area_geojson = _area(-6.0, 25.0, 30.0, 45.0)

    planned = Fetcher(_request(), config=_config(tmp_path)).plan().results[0]

    coverage = planned.details["availability"]["coverage"]
    assert (coverage["west"], coverage["east"], coverage["south"], coverage["north"]) == (-6.0, 25.0, 30.0, 45.0)


def test_eodyn_plan_ignores_oda_files_without_an_analysis_time(tmp_path, fake_oda):
    documents = fake_oda([_at(21, 0)])
    documents.documents[0].start_datetime = None

    planned = Fetcher(_request(), config=_config(tmp_path)).plan().results[0]

    assert planned.details["availability"]["method"] == "coverage"
    assert planned.message.startswith(f"ODA lists no dated file with a footprint in {ODA_DATASET}")


def test_eodyn_doctor_warns_without_credentials():
    checks = eodyn.diagnose(_config_for_doctor(), online=True)

    assert [check.name for check in checks] == ["python-oda package", "oda credentials"]
    assert checks[1].status == DoctorStatus.WARN


def test_eodyn_doctor_online_reports_the_dataset_range(fake_oda):
    fake_oda([_at(20, 0), _at(23, 21)])

    checks = eodyn.diagnose(_config_for_doctor(), online=True)

    assert checks[-1].status == DoctorStatus.OK
    assert checks[-1].message == f"{ODA_DATASET}: files from 2026-11-20 to 2026-11-23"


def test_bundled_catalog_reads_osmose_currents_from_oda():
    source = get_config().sources["eodyn_osmose_currents"]

    assert source.raw["oda_dataset"] == ODA_DATASET
    assert source.temporal_sampling == "3h"
    assert {"totalewct", "totalnsct", "ci", "posterior_std"} <= set(source.available_variables)


def _config_for_doctor():
    return get_config(
        overrides={"source_catalogs": [], "sources": {"eodyn_osmose_currents": {"kind": "eodyn", "variables": ["ci"]}}}
    )
