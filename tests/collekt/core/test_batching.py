"""Offline batch integration tests using real daily NetCDF/Parquet outputs."""

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from collekt import Fetcher
from collekt.core.batching import request_windows
from collekt.core.config import get_config
from collekt.core.request import Region, Request
from collekt.sources import cmems, ecmwf_open_data, era5, gfw, hozint, local, skytruth
from collekt.sources.base import SourceStatus, get_adapter


def config(root, kind="era5", **raw):
    return get_config(
        overrides={
            "source_catalogs": [],
            "output": {"root": str(root)},
            "sources": {
                "demo": {
                    "kind": kind,
                    "path": "demo",
                    "filename_pattern": "{start:%Y%m%d}_{end:%Y%m%d}_{date:%Y%m%d}.nc",
                    "variables": ["u10"],
                    "temporal_sampling": "1h",
                    **raw,
                }
            },
        }
    )


def request(start="2023-01-01", end="2023-01-07"):
    return Request(region=Region.from_bbox((4, 5, 40, 41)), start=start, end=end)


def stub_era5(monkeypatch, calls, *, fail_days=(), omit_days=()):
    class Client:
        def retrieve(self, dataset_id, payload):
            calls.append(payload)
            if any(day in fail_days for day in payload["day"]):
                raise RuntimeError("provider unavailable")
            timestamps = np.array(
                [
                    f"{payload['year'][0]}-{payload['month'][0]}-{day}T{hour}"
                    for day in payload["day"]
                    if day not in omit_days
                    for hour in payload["time"]
                ],
                dtype="datetime64[ns]",
            )
            data = xr.Dataset(
                {"u10": ("valid_time", timestamps.astype("int64").astype(float), {"units": "m s**-1"})},
                coords={"valid_time": timestamps, "latitude": 40.5},
                attrs={"institution": "synthetic CDS"},
            )
            return SimpleNamespace(download=lambda path: data.to_netcdf(path))

    monkeypatch.setattr(era5, "_cdsapi", lambda: SimpleNamespace(Client=Client))


@pytest.mark.parametrize("value", [0, -1, True, False, 1.5, "7d", "48h", "7"])
def test_invalid_batch_size_has_no_filesystem_effect(tmp_path, value):
    output = tmp_path / "out"
    with pytest.raises(ValueError, match="positive integer"):
        Fetcher(request(), config=config(output), batch_days=value)
    assert not output.exists()


def test_windows_are_clipped_utc_calendar_days():
    original = request("2023-12-31T23:00:00-02:00", "2024-01-04T02:15:00+02:00")
    windows = request_windows(original, 2)
    assert [window.start_datetime.isoformat() for window in windows] == [
        "2024-01-01T01:00:00+00:00",
        "2024-01-03T00:00:00+00:00",
    ]
    assert windows[-1].end_datetime == original.end_datetime
    assert [day for window in windows for day in window.iter_days()] == original.iter_days()


@pytest.mark.parametrize(
    "kind",
    [
        "era5",
        "cmems",
        "ecmwf_open_data",
        "gfs",
        "eodyn",
        "local",
        "skytruth",
        "gfw",
        "hozint",
        "copernicus_dataspace",
    ],
)
def test_every_builtin_has_a_batch_handler(kind):
    assert get_adapter(kind).batch is not None


def test_era5_one_retrieval_preserves_daily_values_metadata_and_filenames(tmp_path, monkeypatch):
    calls = []
    stub_era5(monkeypatch, calls)
    default = Fetcher(request(), config=config(tmp_path / "default")).download()
    assert len(calls) == 7
    fetcher = Fetcher(request(), config=config(tmp_path / "batch"), batch_days=7)
    plan = fetcher.plan()
    assert not fetcher.output_dir.exists()
    assert plan.summary.planned == 7
    assert len({item.details["batch"]["id"] for item in plan.results}) == 1
    batched = fetcher.download()
    assert len(calls) == 8
    assert batched.summary.downloaded == 7
    assert [item.details["batch"] for item in plan.results] == [item.details["batch"] for item in batched.results]
    for daily, grouped in zip(default.results, batched.results, strict=True):
        assert daily.path.name == grouped.path.name
        assert daily.details["temporal"] == grouped.details["temporal"]
        with xr.open_dataset(daily.path) as left, xr.open_dataset(grouped.path) as right:
            xr.testing.assert_identical(left, right)
    # File counts remain daily, while manifests expose a shared retrieval.
    manifest = json.loads(batched.manifest_path.read_text())
    assert len(manifest["files"]) == 7
    assert manifest["files"][0]["details"]["batch"]["request"]["day"] == [f"{day:02}" for day in range(1, 8)]


def test_era5_partial_hours_month_year_boundaries_and_sampling_anchor(tmp_path, monkeypatch):
    calls = []
    stub_era5(monkeypatch, calls)
    window = request("2023-12-30T10:15:00+00:00", "2024-01-03T04:15:00+00:00")
    fetcher = Fetcher(window, config=config(tmp_path), batch_days=7)
    result = fetcher.download()
    assert result.summary.downloaded == 5
    assert [payload["day"] for payload in calls] == [["30"], ["31"], ["01", "02"], ["03"]]
    assert calls[0]["time"][0] == "10:00"
    assert calls[-1]["time"] == [f"{hour:02}:00" for hour in range(5)]
    assert all(item.details["temporal"]["source_timestamps"][0].endswith("15:00Z") for item in result.results)


def test_cache_gaps_only_fetch_missing_days_and_keep_original_provenance(tmp_path, monkeypatch):
    calls = []
    stub_era5(monkeypatch, calls)
    fetcher = Fetcher(request(), config=config(tmp_path), batch_days=7)
    first = fetcher.download()
    first.results[2].path.unlink()
    first.results[3].path.unlink()
    first.results[5].path.unlink()
    plan = fetcher.plan()
    assert plan.summary.reused == 4
    second = fetcher.download()
    assert [payload["day"] for payload in calls[1:]] == [["03", "04"], ["06"]]
    assert second.results[0].details["batch"] == first.results[0].details["batch"]
    assert second.results[2].details["batch"]["id"] != first.results[2].details["batch"]["id"]
    # Returning to the default still retains actual provenance from the manifest.
    third = Fetcher(request(), config=config(tmp_path)).download()
    assert third.summary.reused == 7
    assert third.results[2].details["batch"] == second.results[2].details["batch"]
    assert len(calls) == 3
    fetcher.download(use_cache=False)
    assert len(calls) == 4


def test_failed_batch_and_missing_day_do_not_hide_successful_outputs(tmp_path, monkeypatch):
    calls = []
    stub_era5(monkeypatch, calls, fail_days=("01",), omit_days=("05",))
    result = Fetcher(request(end="2023-01-06"), config=config(tmp_path), batch_days=2).download()
    assert [item.status for item in result.results] == [
        SourceStatus.SKIPPED,
        SourceStatus.SKIPPED,
        SourceStatus.DOWNLOADED,
        SourceStatus.DOWNLOADED,
        SourceStatus.SKIPPED,
        SourceStatus.DOWNLOADED,
    ]
    assert len(result.files) == 3
    assert "missing requested timestamps" in result.results[4].message
    assert not list(result.output_dir.rglob(".collekt-*"))


@pytest.mark.parametrize("method", ["outside", "nearest", "inside", "strict-inside"])
@pytest.mark.parametrize("instant", [True, False])
def test_cmems_daily_selection_matches_toolbox_with_noon_samples(tmp_path, monkeypatch, method, instant):
    from copernicusmarine.download_functions.subset_xarray import _dataset_custom_sel

    times = np.arange("2022-12-30T12", "2023-01-07T12", dtype="datetime64[h]")[::24].astype("datetime64[ns]")
    data = xr.Dataset({"u10": ("time", np.arange(len(times)), {"units": "m/s"})}, coords={"time": times})
    calls = []

    def subset(**kwargs):
        calls.append(kwargs)
        selected = _dataset_custom_sel(
            data,
            "time",
            slice(np.datetime64(kwargs["start_datetime"]), np.datetime64(kwargs["end_datetime"])),
            kwargs["coordinates_selection_method"],
        )
        selected.to_netcdf(Path(kwargs["output_directory"]) / kwargs["output_filename"])

    monkeypatch.setattr(cmems, "_copernicusmarine", lambda: SimpleNamespace(subset=subset))
    monkeypatch.setattr("collekt.core.availability._copernicusmarine_describe", lambda: None)
    raw = {"time_selection": "instant" if instant else "full_day", "coordinates_selection_method": method}
    window = request(end="2023-01-03")
    default = Fetcher(window, config=config(tmp_path / "default", "cmems", **raw)).download()
    batched = Fetcher(window, config=config(tmp_path / "batch", "cmems", **raw), batch_days=3).download()
    assert batched.summary.downloaded == 3
    assert len(calls) == (4 if method in {"outside", "nearest"} else 6)
    for left, right in zip(default.files, batched.files, strict=True):
        with xr.open_dataset(left) as a, xr.open_dataset(right) as b:
            xr.testing.assert_identical(a, b)


def test_ecmwf_batches_runs_and_retains_lead_and_valid_time(tmp_path, monkeypatch):
    calls = []
    requests = {}

    class Client:
        def __init__(self, **kwargs):
            pass

        def retrieve(self, payload, target):
            calls.append(payload)
            requests[str(target)] = payload

    def open_raw(path):
        payload = requests[str(path)]
        dates = payload["date"] if isinstance(payload["date"], list) else [payload["date"]]
        runs = np.array([datetime.strptime(day, "%Y%m%d") for day in dates], dtype="datetime64[ns]")
        steps = np.array(payload["step"], dtype="timedelta64[h]").astype("timedelta64[ns]")
        valid = runs[:, None] + steps[None, :]
        data = xr.Dataset(
            {
                "u10": (
                    ("time", "step", "latitude", "longitude"),
                    valid.astype("int64")[:, :, None, None],
                    {"units": "m/s"},
                )
            },
            coords={
                "time": runs,
                "step": steps,
                "latitude": [40.5],
                "longitude": [4.5],
                "valid_time": (("time", "step"), valid),
            },
        )
        return [data.isel(time=0) if len(runs) == 1 else data]

    monkeypatch.setattr(ecmwf_open_data, "_client_class", lambda: Client)
    monkeypatch.setattr(ecmwf_open_data, "_open_raw_datasets", open_raw)
    window = request(end="2023-01-03")
    default = Fetcher(window, config=config(tmp_path / "default", "ecmwf_open_data", temporal_sampling="3h")).download()
    batched = Fetcher(
        window, config=config(tmp_path / "batch", "ecmwf_open_data", temporal_sampling="3h"), batch_days=3
    ).download()
    assert len(calls) == 4
    assert batched.summary.downloaded == 3
    for left, right in zip(default.files, batched.files, strict=True):
        with xr.open_dataset(left) as a, xr.open_dataset(right) as b:
            xr.testing.assert_identical(a, b)
            assert "step" in b.dims and b.time.ndim == 0


def test_local_reads_shared_archive_once_and_keeps_daily_metadata(tmp_path, monkeypatch):
    import damast
    import polars as pl

    archive = tmp_path / "archive"
    archive.mkdir()
    frame = pl.DataFrame(
        {
            "timestamp": [datetime(2023, 1, day, 12, tzinfo=UTC) for day in range(1, 4)],
            "lat": [40.5] * 3,
            "lon": [4.5] * 3,
            "value": [1.0, 2.0, 3.0],
        }
    )
    metadata = damast.core.MetaData(columns=[damast.core.DataSpecification(name=name) for name in frame.columns])
    damast.core.AnnotatedDataFrame(frame, metadata).export(archive / "all.parquet")
    raw = {
        "archive_root": str(archive),
        "file_pattern": "*.parquet",
        "time_column": "timestamp",
        "region_columns": ["lat", "lon"],
        "variables": ["value"],
        "filename_pattern": "{date:%Y%m%d}.parquet",
    }
    window = request(end="2023-01-03")
    default = Fetcher(window, config=config(tmp_path / "default", "local", **raw)).download()
    calls = []
    original = local._read_day_subset

    def counted(*args, **kwargs):
        calls.append(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(local, "_read_day_subset", counted)
    batched = Fetcher(window, config=config(tmp_path / "batch", "local", **raw), batch_days=3).download()
    assert len(calls) == 1
    assert batched.summary.downloaded == 3
    for left, right in zip(default.files, batched.files, strict=True):
        a = damast.core.AnnotatedDataFrame.from_files([str(left)], metadata_required=True)
        b = damast.core.AnnotatedDataFrame.from_files([str(right)], metadata_required=True)
        assert a.dataframe.collected().equals(b.dataframe.collected())
        assert a.metadata == b.metadata


def test_skytruth_resumes_failed_window_and_deduplicates_before_atomic_export(tmp_path, monkeypatch):
    calls, written = [], []
    broken = True

    def query(url, params):
        nonlocal broken
        calls.append(params["datetime"])
        if params["datetime"].startswith("2023-01-03") and broken:
            broken = False
            raise RuntimeError("transient query failure")
        return [
            {"id": "overlap", "properties": {"slick_timestamp": "2023-01-01"}},
            {"id": params["datetime"], "properties": {"slick_timestamp": params["datetime"]}},
        ], url

    def write(rows, path, url):
        written.extend(rows)
        path.write_text("complete parquet")

    monkeypatch.setattr(skytruth, "_fetch_pages", query)
    monkeypatch.setattr(skytruth, "_write_parquet", write)
    fetcher = Fetcher(
        request(end="2023-01-06"), config=config(tmp_path, "skytruth", filename_pattern="events.parquet"), batch_days=2
    )
    first = fetcher.download()
    assert first.summary.skipped == 1 and not first.files
    assert len(calls) == 3 and not written
    second = fetcher.download()
    assert second.summary.downloaded == 1
    assert len(calls) == 4 and len(written) == 4
    assert not list(second.output_dir.rglob(".collekt-*"))
    assert not list(second.output_dir.rglob(".batch-queries/*/*.json"))
    assert Fetcher(request(end="2023-01-06"), config=fetcher.config).download().summary.reused == 1
    assert len(calls) == 4


def test_gfw_global_cap_and_overlapping_events(tmp_path, monkeypatch):
    calls, written = [], []

    def query(source, region, start, end, on_page):
        calls.append((start, end))
        return [{"id": "overlap", "start": "2022-01-01T00:00:00Z"}, {"id": start, "start": start + "T00:00:00Z"}]

    def write(rows, path, source):
        written.extend(rows)
        path.write_text("complete parquet")

    monkeypatch.setattr(gfw, "_fetch_events", query)
    monkeypatch.setattr(gfw, "_write_parquet", write)
    fetcher = Fetcher(
        request(end="2023-01-06"),
        config=config(tmp_path, "gfw", max_events=3, filename_pattern="events.parquet"),
        batch_days=2,
    )
    with pytest.warns(gfw.GFWTruncatedResultWarning, match="across all batches"):
        result = fetcher.download()
    assert result.summary.downloaded == 1
    assert calls == [("2023-01-01", "2023-01-03"), ("2023-01-03", "2023-01-05"), ("2023-01-05", "2023-01-07")]
    assert [row["id"] for row in written] == ["overlap", "2023-01-01", "2023-01-03"]


def test_published_event_file_survives_a_checkpoint_cleanup_failure(tmp_path, monkeypatch):
    """Discarding the completed queries is cleanup, not part of publishing the file."""
    import shutil

    obstruction = []

    def write(rows, path, url):
        # Every checkpoint has been read by now. Put a plain file where the
        # checkpoint directory was, so the cleanup that follows this export
        # fails on every platform without relying on directory permissions.
        checkpoints = path.parent.parent / ".batch-queries" / path.name
        shutil.rmtree(checkpoints)
        checkpoints.write_text("not a directory")
        obstruction.append(checkpoints)
        path.write_text("complete parquet")

    monkeypatch.setattr(skytruth, "_fetch_pages", lambda url, params: ([{"id": params["datetime"]}], url))
    monkeypatch.setattr(skytruth, "_write_parquet", write)
    result = Fetcher(
        request(end="2023-01-04"), config=config(tmp_path, "skytruth", filename_pattern="events.parquet"), batch_days=2
    ).download()
    assert result.summary.downloaded == 1
    assert Path(result.files[0]).read_text() == "complete parquet"
    # The obstruction is untouched, so the cleanup did fail and was ignored.
    assert obstruction[0].read_text() == "not a directory"


def test_gfw_batch_caps_refuse_an_unexpected_provider_sort(tmp_path, monkeypatch):
    """The merged cap only matches an unbatched run while the API sorts on start."""
    monkeypatch.setattr(gfw, "EVENT_SORT", "-start")
    monkeypatch.setattr(gfw, "_fetch_events", lambda *args: [{"id": "e", "start": "2023-01-01T00:00:00Z"}])
    result = Fetcher(
        request(end="2023-01-04"),
        config=config(tmp_path, "gfw", max_events=3, filename_pattern="events.parquet"),
        batch_days=2,
    ).download()
    assert result.summary.skipped == 1
    assert "'+start' sort" in result.results[0].message


def test_hozint_failed_partial_output_is_not_cached_as_complete(tmp_path, monkeypatch):
    calls = []
    broken = True

    def run(command):
        nonlocal broken
        start = command[command.index("--from-time") + 1]
        calls.append(start)
        directory = Path(command[command.index("--output-dir") + 1])
        (directory / "reports.parquet").write_text(start)
        (directory / "reports.spec.yaml").write_text("synthetic metadata")
        if start == "2023-01-03" and broken:
            broken = False
            return SimpleNamespace(returncode=1)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(hozint, "_run", run)
    fetcher = Fetcher(request(end="2023-01-06"), config=config(tmp_path, "hozint"), batch_days=2)
    first = fetcher.download()
    assert first.summary.downloaded == 2 and first.summary.skipped == 1
    assert len(first.files) == 2
    second = fetcher.download()
    assert second.summary.downloaded == 1 and second.summary.reused == 2
    assert calls == ["2023-01-01", "2023-01-03", "2023-01-05", "2023-01-03"]
    assert sorted(path.read_text() for path in second.files) == ["2023-01-01", "2023-01-03", "2023-01-05"]
    (second.files[0].parent / "reports.spec.yaml").unlink()
    third = fetcher.download()
    assert third.summary.downloaded == 1 and third.summary.reused == 2
    assert calls[-1] == "2023-01-01"


def test_product_windows_paginate_deduplicate_and_apply_global_limit(tmp_path, monkeypatch):
    from collekt.sources import copernicus_dataspace as cds

    searches, downloads, pages = [], [], []

    def feature(identity):
        return {
            "id": identity,
            "assets": {"Product": {"href": f"https://example.test/{identity}", "file:local_path": identity}},
        }

    def search(token, params):
        searches.append(params)
        if params["datetime"].startswith("2023-01-01"):
            return {"features": [feature("a")], "links": [{"rel": "next", "href": "?page=2"}]}
        return {"features": [feature("b"), feature("c"), feature("d")]}

    def get(url, **kwargs):
        pages.append(url)
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"features": [feature("b")]})

    def download(url, token, path):
        downloads.append(url)
        path.write_text("complete product")

    monkeypatch.setattr(cds, "_credentials", lambda: ("unused", "unused"))
    monkeypatch.setattr(cds, "_login", lambda *args: "unused")
    monkeypatch.setattr(cds, "_search", search)
    monkeypatch.setattr(cds.requests, "get", get)
    monkeypatch.setattr(cds, "_download", download)
    fetcher = Fetcher(
        request(), config=config(tmp_path, "copernicus_dataspace", collection="example", max_records=3), batch_days=2
    )
    result = fetcher.download()
    assert len(searches) == 2 and len(pages) == 1 and len(downloads) == 3
    assert [path.name for path in result.files] == ["a", "b", "c"]
    again = fetcher.download()
    assert again.summary.reused == 3
    assert len(searches) == 2 and len(downloads) == 3


def test_gfs_groups_preserve_cycle_urls_and_original_request_names(tmp_path, monkeypatch):
    from collekt.sources import gfs

    calls = []

    def download(url, path):
        calls.append(url)
        path.write_text("grib")

    def concat(paths, output, variables):
        output.write_text("netcdf")
        return []

    monkeypatch.setattr(gfs, "_download_cycle", download)
    monkeypatch.setattr(gfs, "_concat_cycles_to_netcdf", concat)
    window = request("2023-01-01T10:00:00Z", "2023-01-05T07:00:00Z")
    raw = {"variables": ["20u"], "temporal_sampling": "6h"}
    daily = Fetcher(window, config=config(tmp_path / "default", "gfs", **raw)).download()
    expected_urls = calls[:]
    calls.clear()
    fetcher = Fetcher(window, config=config(tmp_path / "batch", "gfs", **raw), batch_days=2)
    planned = fetcher.plan()
    batch = fetcher.download()
    assert calls == expected_urls
    assert [path.name for path in daily.files] == [path.name for path in batch.files]
    assert len({item.details["batch"]["id"] for item in batch.results}) == 3
    assert [item.details["batch"] for item in planned.results] == [item.details["batch"] for item in batch.results]
    for left, right in zip(daily.results, batch.results, strict=True):
        assert left.details["temporal"] == right.details["temporal"]
        assert right.details["batch"]["transport"] == "individual_files"
    assert fetcher.download().summary.reused == 5
    assert calls == expected_urls


def test_gfs_batch_does_not_download_days_excluded_by_its_plan(tmp_path, monkeypatch):
    from collekt.sources import gfs

    calls = []

    def download(url, path):
        calls.append(url)
        path.write_text("grib")

    def concat(paths, output, variables):
        output.write_text("netcdf")
        return []

    monkeypatch.setattr(gfs, "_download_cycle", download)
    monkeypatch.setattr(gfs, "_concat_cycles_to_netcdf", concat)
    cfg = config(tmp_path, "gfs", variables=["20u"], coverage={"start": "2023-01-02", "end": "2023-01-03"})
    fetcher = Fetcher(request(end="2023-01-03"), config=cfg, batch_days=3)
    assert fetcher.plan().summary.skipped == 1
    result = fetcher.download()
    assert result.summary.downloaded == 2 and result.summary.skipped == 1
    assert len(calls) == 8 and not any("20230101" in url for url in calls)


def test_eodyn_batch_keeps_archive_daily_output(tmp_path):
    from collekt.sources.eodyn import eOdynArchiveWarning

    archive = tmp_path / "archive"
    archive.mkdir()
    for day in range(1, 4):
        xr.Dataset(
            {"u10": (("latitude", "longitude"), [[float(day)]], {"units": "m/s"})},
            coords={"latitude": [40.5], "longitude": [4.5]},
            attrs={"source": "synthetic archive"},
        ).to_netcdf(archive / f"2023010{day}.nc")
    raw = {
        "archive_root": str(archive),
        "day_pattern": "{date:%Y%m%d}.nc",
        "coverage": {"start": "2023-01-01", "end": "2023-01-03"},
        "mode": "archive",
    }
    window = request(end="2023-01-03")
    with pytest.warns(eOdynArchiveWarning):
        default = Fetcher(window, config=config(tmp_path / "default", "eodyn", **raw)).download()
    with pytest.warns(eOdynArchiveWarning):
        batch = Fetcher(window, config=config(tmp_path / "batch", "eodyn", **raw), batch_days=3).download()
    assert batch.summary.downloaded == 3
    for left, right in zip(default.files, batch.files, strict=True):
        with xr.open_dataset(left) as a, xr.open_dataset(right) as b:
            xr.testing.assert_identical(a, b)


def test_daily_coverage_attributes_and_storage_chunks_do_not_describe_whole_batch():
    from collekt.sources.batching.gridded import _prepare_daily

    times = np.arange("2023-01-01", "2023-01-04", dtype="datetime64[h]")
    dataset = xr.Dataset(
        {"wind": ("time", np.arange(72))},
        coords={"time": times},
        attrs={
            "time_coverage_start": "2023-01-01T00:00:00Z",
            "time_coverage_end": "2023-01-03T23:00:00Z",
            "time_coverage_duration": "P3D",
            "history": "synthetic provider job",
            "institution": "example",
        },
    )
    dataset.wind.encoding["chunksizes"] = (72,)
    daily = _prepare_daily(dataset.isel(time=slice(24, 48)))
    # The narrowed values keep the provider's own precision and zone suffix, and
    # the duration stays an ISO 8601 duration rather than a raw second count.
    assert daily.attrs["time_coverage_start"] == "2023-01-02T00:00:00Z"
    assert daily.attrs["time_coverage_end"] == "2023-01-02T23:00:00Z"
    assert daily.attrs["time_coverage_duration"] == "PT23H"
    assert daily.attrs["history"] == dataset.attrs["history"]
    assert daily.wind.encoding["chunksizes"] == (24,)
    assert dataset.wind.encoding["chunksizes"] == (72,)
    assert dataset.attrs["time_coverage_duration"] == "P3D"
    assert "chunksizes" not in _prepare_daily(dataset.isel(time=0)).wind.encoding


@pytest.mark.parametrize(
    ("original", "expected"),
    [
        ("2023-01-01T00:00:00Z", "2023-01-02T00:00:00Z"),
        ("2023-01-01T00:00:00", "2023-01-02T00:00:00"),
        ("2023-01-01T00:00:00.000Z", "2023-01-02T00:00:00.000Z"),
        ("2023-01-01T00:00:00.000000000", "2023-01-02T00:00:00.000000000"),
        ("2023-01-01", "2023-01-02"),
    ],
)
def test_narrowed_coverage_attributes_keep_the_provider_spelling(original, expected):
    from collekt.sources.batching.gridded import _like

    assert _like(original, np.datetime64("2023-01-02T00:00:00.000000000")) == expected


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [(0, "PT0S"), (86400, "P1D"), (172800, "P2D"), (82800, "PT23H"), (3661, "PT1H1M1S"), (45, "PT45S")],
)
def test_coverage_duration_is_an_iso_8601_duration(seconds, expected):
    from collekt.sources.batching.gridded import _iso_duration

    assert _iso_duration(seconds) == expected


def test_failed_atomic_daily_write_can_be_retried_without_a_false_cache_hit(tmp_path, monkeypatch):
    calls = []
    stub_era5(monkeypatch, calls)
    real_write = xr.Dataset.to_netcdf

    def broken_write(dataset, path, *args, **kwargs):
        if Path(path).name.endswith("20230102.nc"):
            Path(path).write_text("partial netcdf")
            raise OSError("simulated export failure")
        return real_write(dataset, path, *args, **kwargs)

    monkeypatch.setattr(xr.Dataset, "to_netcdf", broken_write)
    fetcher = Fetcher(request(end="2023-01-03"), config=config(tmp_path), batch_days=3)
    first = fetcher.download()
    assert first.summary.downloaded == 2 and first.summary.skipped == 1
    assert not list(first.output_dir.rglob("*20230102.nc"))
    monkeypatch.setattr(xr.Dataset, "to_netcdf", real_write)
    second = fetcher.download()
    assert second.summary.downloaded == 1 and second.summary.reused == 2
    assert calls[-1]["day"] == ["02"]


def test_skytruth_midnight_overlap_keeps_fractional_seconds_and_deduplicates(tmp_path, monkeypatch):
    calls, rows_written = [], []
    rows = [
        {"id": "fraction", "properties": {"slick_timestamp": "2023-01-01T23:59:59.500Z"}},
        {"id": "midnight", "properties": {"slick_timestamp": "2023-01-02T00:00:00Z"}},
    ]

    def query(url, params):
        start, end = (datetime.fromisoformat(value.replace("Z", "+00:00")) for value in params["datetime"].split("/"))
        calls.append((start, end))
        return [
            row
            for row in rows
            if start <= datetime.fromisoformat(row["properties"]["slick_timestamp"].replace("Z", "+00:00")) <= end
        ], url

    def write(rows, path, url):
        rows_written.extend(rows)
        path.write_text("parquet")

    monkeypatch.setattr(skytruth, "_fetch_pages", query)
    monkeypatch.setattr(skytruth, "_write_parquet", write)
    result = Fetcher(
        request(end="2023-01-02"), config=config(tmp_path, "skytruth", filename_pattern="events.parquet"), batch_days=1
    ).download()
    assert result.summary.downloaded == 1
    assert {row["id"] for row in rows_written} == {"fraction", "midnight"}
    assert len(rows_written) == 2
    assert calls[0][1] == calls[1][0]


def test_cli_batch_plan_lists_groups_without_creating_files(tmp_path, monkeypatch, capsys):
    from collekt.cli.main import run
    from collekt.core.datasets import DatasetConfig

    monkeypatch.setattr(DatasetConfig, "from_yaml", staticmethod(lambda path: config(tmp_path / "out")))
    arguments = [
        "fetch",
        "--bbox",
        "4",
        "5",
        "40",
        "41",
        "--start",
        "2023-01-01",
        "--end",
        "2023-01-07",
        "--dataset-config",
        "unused.yaml",
        "--output-dir",
        str(tmp_path / "out"),
        "--batch-days",
        "7",
        "--dry-run",
    ]
    assert run(arguments) == 0
    assert "1 planned retrieval groups" in capsys.readouterr().out
    assert not (tmp_path / "out").exists()
    arguments[arguments.index("--batch-days") + 1] = "0"
    assert run(arguments) != 0
    assert not (tmp_path / "out").exists()


def test_unsupported_custom_adapter_fails_before_removing_cached_data(tmp_path, monkeypatch):
    from dataclasses import replace

    from collekt.sources.base import SourceAdapter, register_adapter

    adapter = SourceAdapter(kind="batchless-test", fetch=lambda *args, **kwargs: [], plan=lambda *args: [])
    register_adapter(adapter)
    cfg = config(tmp_path, "batchless-test")
    cfg = replace(cfg, output=replace(cfg.output, request_pattern="existing"))
    directory = tmp_path / "existing"
    directory.mkdir()
    marker = directory / "keep.txt"
    marker.write_text("cached data")
    with pytest.raises(ValueError, match="does not support batch_days"):
        Fetcher(request(), config=cfg, batch_days=7).download(use_cache=False)
    assert marker.read_text() == "cached data"


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ({"data_format": "grib"}, "data_format='netcdf'"),
        ({"download_format": "zip"}, "download_format='unarchived'"),
        ({"filename_pattern": "same.nc"}, "does not give every day its own file"),
        ({"filename_pattern": "{date:%d}.nc"}, "does not give every day its own file"),
        ({"filename_pattern": "{date:%m-%d}.nc"}, "does not give every day its own file"),
        ({"filename_pattern": "{date:%j}.nc"}, "does not give every day its own file"),
    ],
)
def test_unsupported_daily_output_configuration_rejected_in_plan(tmp_path, raw, message):
    with pytest.raises(ValueError, match=message):
        Fetcher(request(), config=config(tmp_path / "out", **raw), batch_days=7).plan()
    assert not (tmp_path / "out").exists()


def test_a_rejected_source_stops_the_run_before_any_other_source_downloads(tmp_path, monkeypatch):
    """A batch configuration error must not leave files behind with no manifest."""
    from dataclasses import replace as replace_dataclass

    calls = []
    stub_era5(monkeypatch, calls)
    cfg = config(tmp_path / "out")
    good = cfg.sources["demo"]
    cfg = replace_dataclass(
        cfg,
        sources={
            "demo": good,
            "zbroken": replace_dataclass(
                good, name="zbroken", path="broken", raw=dict(good.raw) | {"data_format": "grib"}
            ),
        },
    )
    with pytest.raises(ValueError, match="data_format='netcdf'"):
        Fetcher(request(), config=cfg, batch_days=7).download()
    assert calls == []
    assert not list((tmp_path / "out").rglob("*.nc"))


def test_max_records_is_rejected_before_any_product_is_searched(tmp_path):
    cfg = config(tmp_path / "out", "copernicus_dataspace", collection="SENTINEL-1", max_records=0)
    with pytest.raises(ValueError, match="max_records must be positive"):
        Fetcher(request(), config=cfg, batch_days=7).download()
    assert not (tmp_path / "out").exists()


def test_single_day_group_publishes_the_provider_file_unchanged(tmp_path, monkeypatch):
    """A group of one day asks for the daily request, so nothing needs splitting."""
    calls = []
    stub_era5(monkeypatch, calls)
    window = request(end="2023-01-01")
    default = Fetcher(window, config=config(tmp_path / "default")).download()
    batched = Fetcher(window, config=config(tmp_path / "batch"), batch_days=7).download()
    assert len(calls) == 2
    assert Path(default.files[0]).read_bytes() == Path(batched.files[0]).read_bytes()


def test_provider_coverage_attributes_survive_a_split_batch(tmp_path, monkeypatch):
    """Narrowing `time_coverage_*` must not also change how the provider spells it."""

    class Client:
        def retrieve(self, dataset_id, payload):
            timestamps = np.array(
                [f"2023-01-{day}T{hour}" for day in payload["day"] for hour in payload["time"]],
                dtype="datetime64[ns]",
            )
            data = xr.Dataset(
                {"u10": ("valid_time", np.arange(timestamps.size, dtype="float32"))},
                coords={"valid_time": timestamps},
                attrs={
                    "time_coverage_start": "2023-01-01T00:00:00Z",
                    "time_coverage_end": "2023-01-03T23:00:00Z",
                    "time_coverage_duration": "P3D",
                },
            )
            return SimpleNamespace(download=lambda path: data.to_netcdf(path))

    monkeypatch.setattr(era5, "_cdsapi", lambda: SimpleNamespace(Client=Client))
    result = Fetcher(request(end="2023-01-03"), config=config(tmp_path / "out"), batch_days=7).download()
    with xr.open_dataset(sorted(result.files)[1]) as daily:
        assert daily.attrs["time_coverage_start"] == "2023-01-02T00:00:00Z"
        assert daily.attrs["time_coverage_end"] == "2023-01-02T23:00:00Z"
        assert daily.attrs["time_coverage_duration"] == "PT23H"


def test_cmems_split_handles_a_descending_time_coordinate():
    from collekt.sources.cmems import _daily_slice

    times = np.arange("2023-01-01", "2023-01-04", dtype="datetime64[h]").astype("datetime64[ns]")
    ascending = xr.Dataset({"uo": ("time", np.arange(times.size, dtype="float32"))}, coords={"time": times})
    payload = {
        "start_datetime": "2023-01-02T00:00:00",
        "end_datetime": "2023-01-02T23:00:00",
        "coordinates_selection_method": "outside",
    }
    expected = _daily_slice(ascending, payload)
    reversed_slice = _daily_slice(ascending.isel(time=slice(None, None, -1)), payload)
    xr.testing.assert_identical(reversed_slice.sortby("time"), expected)
    with pytest.raises(ValueError, match="monotonic"):
        _daily_slice(ascending.isel(time=[2, 0, 1]), payload)


def test_gfw_checkpoint_resume_preserves_real_parquet_dates_and_metadata(tmp_path, monkeypatch):
    import damast

    def event(day):
        data = {
            "id": f"event-{day}",
            "type": "fishing",
            "start": datetime(2023, 1, day, tzinfo=UTC),
            "end": datetime(2023, 1, day, 1, tzinfo=UTC),
            "position": {"lat": 40.5, "lon": 4.5},
            "bounding_box": [4.0, 40.0, 5.0, 41.0],
            "distances": {
                key: 1.0
                for key in (
                    "start_distance_from_shore_km",
                    "end_distance_from_shore_km",
                    "start_distance_from_port_km",
                    "end_distance_from_port_km",
                )
            },
            "vessel": {"id": "example", "name": "TEST", "ssvid": "123456789", "flag": "FRA", "type": "fishing"},
            "regions": {
                key: []
                for key in (
                    "eez",
                    "mpa",
                    "rfmo",
                    "fao",
                    "major_fao",
                    "eez_12_nm",
                    "high_seas",
                    "mpa_no_take_partial",
                    "mpa_no_take",
                )
            },
        }
        return gfw._flatten_event(SimpleNamespace(model_dump=lambda: data))

    rows = [event(1), event(2)]
    cfg = config(tmp_path / "out", "gfw", variables=[], filename_pattern="events.parquet")
    expected = tmp_path / "expected" / "events.parquet"
    expected.parent.mkdir()
    gfw._write_parquet(rows, expected, cfg.sources["demo"])
    query_calls = []

    def query(source, region, start, end, on_page):
        query_calls.append(start)
        return [rows[0 if start == "2023-01-01" else 1]]

    monkeypatch.setattr(gfw, "_fetch_events", query)
    real_write = gfw._write_parquet

    def failed_export(rows, path, source):
        path.write_text("partial file")
        raise OSError("interrupted export")

    monkeypatch.setattr(gfw, "_write_parquet", failed_export)
    fetcher = Fetcher(request(end="2023-01-02"), config=cfg, batch_days=1)
    assert not fetcher.download().files
    monkeypatch.setattr(gfw, "_write_parquet", real_write)
    result = fetcher.download()
    assert result.summary.downloaded == 1 and len(query_calls) == 2
    a = damast.core.AnnotatedDataFrame.from_files([str(expected)], metadata_required=True)
    b = damast.core.AnnotatedDataFrame.from_files([str(result.files[0])], metadata_required=True)
    assert a.dataframe.collected().equals(b.dataframe.collected())
    assert a.metadata == b.metadata
