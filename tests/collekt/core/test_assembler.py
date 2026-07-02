"""Tests for analysis-ready assembly helpers.

The Assembler needs xarray/numpy (the ``gridded`` extra); the whole module is
skipped when they are not installed.
"""

from pathlib import Path

import pytest

pytest.importorskip("xarray")
pytest.importorskip("numpy")

from collekt import Assembler, Result  # noqa: E402
from collekt.core.reporting import Summary  # noqa: E402
from collekt.sources.base import SourceResult, SourceStatus  # noqa: E402


def _write_dataset(
    path: Path,
    variable: str,
    values,
    *,
    latitudes: list[float],
    longitudes: list[float],
    time: str = "2023-06-15T00:00:00",
) -> Path:
    import xarray as xr

    dataset = xr.Dataset(
        {variable: (("time", "latitude", "longitude"), values)},
        coords={
            "time": [time],
            "latitude": latitudes,
            "longitude": longitudes,
        },
    )
    dataset.to_netcdf(path)
    return path


def _write_dataset_with_times(
    path: Path,
    variable: str,
    values,
    *,
    times: list[str],
    latitudes: list[float],
    longitudes: list[float],
) -> Path:
    import xarray as xr

    dataset = xr.Dataset(
        {variable: (("time", "latitude", "longitude"), values)},
        coords={
            "time": times,
            "latitude": latitudes,
            "longitude": longitudes,
        },
    )
    dataset.to_netcdf(path)
    return path


def _write_dataset_with_valid_time(
    path: Path,
    variable: str,
    values,
    *,
    times: list[str],
    latitudes: list[float],
    longitudes: list[float],
) -> Path:
    import xarray as xr

    dataset = xr.Dataset(
        {variable: (("valid_time", "latitude", "longitude"), values)},
        coords={
            "valid_time": times,
            "latitude": latitudes,
            "longitude": longitudes,
        },
    )
    dataset.to_netcdf(path)
    return path


def _collection_result(tmp_path: Path, *results: SourceResult) -> Result:
    return Result(
        output_dir=tmp_path,
        manifest_path=tmp_path / "manifest.json",
        files=tuple(result.path for result in results if result.path is not None),
        results=results,
        summary=Summary(downloaded=len(results)),
    )


class _DatasetContext:
    def __init__(self, dataset):
        self.dataset = dataset

    def __enter__(self):
        return self.dataset

    def __exit__(self, *_args):
        return False


def test_assembler_keeps_source_identity_in_xarray_dataset(tmp_path):
    glorys = _write_dataset(
        tmp_path / "glorys.nc",
        "uo",
        [[[1.0, 2.0], [3.0, 4.0]]],
        latitudes=[42.0, 43.0],
        longitudes=[5.0, 6.0],
    )
    era5 = _write_dataset(
        tmp_path / "era5.nc",
        "u10",
        [[[5.0, 6.0, 7.0]]],
        latitudes=[42.5],
        longitudes=[4.0, 5.0, 6.0],
    )
    result = _collection_result(
        tmp_path,
        SourceResult("cmems_glorys", SourceStatus.DOWNLOADED, path=glorys),
        SourceResult("era5_reanalysis", SourceStatus.DOWNLOADED, path=era5),
    )

    dataset = Assembler(result).to_xarray()

    assert "cmems_glorys__uo" in dataset.data_vars
    assert "era5_reanalysis__u10" in dataset.data_vars
    assert dataset["cmems_glorys__uo"].dims == (
        "cmems_glorys__time",
        "cmems_glorys__latitude",
        "cmems_glorys__longitude",
    )
    assert dataset["era5_reanalysis__u10"].dims == (
        "era5_reanalysis__time",
        "era5_reanalysis__latitude",
        "era5_reanalysis__longitude",
    )


def test_assembler_applies_explicit_aliases_and_exports(tmp_path):
    glorys = _write_dataset(
        tmp_path / "glorys.nc",
        "uo",
        [[[1.0, 2.0]]],
        latitudes=[42.0],
        longitudes=[5.0, 6.0],
    )
    result = _collection_result(
        tmp_path,
        SourceResult("cmems_glorys", SourceStatus.DOWNLOADED, path=glorys),
    )
    assembler = Assembler(result)

    dataset = assembler.to_xarray(aliases={"cmems_glorys__uo": "glorys_u"})
    arrays = assembler.to_numpy(aliases={"cmems_glorys__uo": "glorys_u"})
    netcdf = assembler.to_netcdf(tmp_path / "assembled.nc", aliases={"cmems_glorys__uo": "glorys_u"})

    assert "glorys_u" in dataset.data_vars
    assert "cmems_glorys__uo" not in dataset.data_vars
    assert arrays["glorys_u"].shape == (1, 1, 2)
    assert netcdf.exists()


def test_assembler_regrids_to_lowest_and_highest_resolution(tmp_path):
    glorys = _write_dataset(
        tmp_path / "glorys.nc",
        "uo",
        [[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]],
        latitudes=[0.0, 1.0, 2.0],
        longitudes=[10.0, 11.0, 12.0],
    )
    era5 = _write_dataset(
        tmp_path / "era5.nc",
        "u10",
        [[[10.0, 20.0], [30.0, 40.0]]],
        latitudes=[0.0, 2.0],
        longitudes=[10.0, 12.0],
    )
    result = _collection_result(
        tmp_path,
        SourceResult("cmems_glorys", SourceStatus.DOWNLOADED, path=glorys),
        SourceResult("era5_reanalysis", SourceStatus.DOWNLOADED, path=era5),
    )
    assembler = Assembler(result)

    lowest = assembler.to_xarray(grid="lowest_resolution")
    highest = assembler.to_xarray(grid="highest_resolution")

    assert lowest["cmems_glorys__uo"].dims == ("time", "latitude", "longitude")
    assert lowest["cmems_glorys__uo"].shape == (1, 2, 2)
    assert lowest["era5_reanalysis__u10"].shape == (1, 2, 2)
    assert list(lowest["latitude"].values) == [0.0, 2.0]
    assert list(lowest["longitude"].values) == [10.0, 12.0]
    assert lowest.attrs["collekt_grid_policy"] == "lowest_resolution"

    assert highest["cmems_glorys__uo"].shape == (1, 3, 3)
    assert highest["era5_reanalysis__u10"].shape == (1, 3, 3)
    assert list(highest["latitude"].values) == [0.0, 1.0, 2.0]
    assert list(highest["longitude"].values) == [10.0, 11.0, 12.0]
    assert highest.attrs["collekt_grid_policy"] == "highest_resolution"


def test_assembler_regrids_to_custom_grid(tmp_path):
    glorys = _write_dataset(
        tmp_path / "glorys.nc",
        "uo",
        [[[1.0, 2.0], [3.0, 4.0]]],
        latitudes=[0.0, 2.0],
        longitudes=[10.0, 12.0],
    )
    result = _collection_result(
        tmp_path,
        SourceResult("cmems_glorys", SourceStatus.DOWNLOADED, path=glorys),
    )

    dataset = Assembler(result).to_xarray(
        aliases={"cmems_glorys__uo": "glorys_u"},
        grid="custom",
        target_grid={"latitude": [0.5, 1.5], "longitude": [10.5, 11.5]},
    )

    assert dataset["glorys_u"].dims == ("time", "latitude", "longitude")
    assert dataset["glorys_u"].shape == (1, 2, 2)
    assert list(dataset["latitude"].values) == [0.5, 1.5]
    assert list(dataset["longitude"].values) == [10.5, 11.5]
    assert dataset.attrs["collekt_grid_policy"] == "custom"
    assert dataset.attrs["collekt_spatial_method"] == "linear"


def test_assembler_regrids_to_custom_grid_with_nearest_method(tmp_path):
    glorys = _write_dataset(
        tmp_path / "glorys.nc",
        "uo",
        [[[0.0, 2.0], [2.0, 4.0]]],
        latitudes=[0.0, 2.0],
        longitudes=[10.0, 12.0],
    )
    result = _collection_result(
        tmp_path,
        SourceResult("cmems_glorys", SourceStatus.DOWNLOADED, path=glorys),
    )

    linear = Assembler(result).to_xarray(
        aliases={"cmems_glorys__uo": "glorys_u"},
        grid="custom",
        target_grid={"latitude": [1.0], "longitude": [11.0]},
    )
    nearest = Assembler(result).to_xarray(
        aliases={"cmems_glorys__uo": "glorys_u"},
        grid="custom",
        spatial_method="nearest",
        target_grid={"latitude": [0.25], "longitude": [10.25]},
    )

    assert float(linear["glorys_u"].isel(time=0, latitude=0, longitude=0)) == pytest.approx(2.0)
    assert float(nearest["glorys_u"].isel(time=0, latitude=0, longitude=0)) == 0.0
    assert nearest.attrs["collekt_spatial_method"] == "nearest"


def test_assembler_aligns_time_to_lowest_and_highest_resolution(tmp_path):
    import numpy as np

    glorys = _write_dataset_with_times(
        tmp_path / "glorys.nc",
        "uo",
        [[[10.0]]],
        times=["2023-06-15T00:00:00"],
        latitudes=[42.0],
        longitudes=[5.0],
    )
    era5 = _write_dataset_with_times(
        tmp_path / "era5.nc",
        "u10",
        [[[-1.0]], [[0.0]], [[1.0]], [[2.0]]],
        times=[
            "2023-06-15T00:00:00",
            "2023-06-15T01:00:00",
            "2023-06-15T02:00:00",
            "2023-06-15T03:00:00",
        ],
        latitudes=[42.0],
        longitudes=[5.0],
    )
    result = _collection_result(
        tmp_path,
        SourceResult(
            "cmems_glorys",
            SourceStatus.DOWNLOADED,
            path=glorys,
            details={"temporal": {"actual_sampling": "24h", "requested_sampling": "1h"}},
        ),
        SourceResult(
            "era5_reanalysis",
            SourceStatus.DOWNLOADED,
            path=era5,
            details={"temporal": {"actual_sampling": "1h", "requested_sampling": "1h"}},
        ),
    )
    assembler = Assembler(result)

    lowest = assembler.to_xarray(time="lowest_resolution", time_tolerance="30min")
    highest = assembler.to_xarray(time="highest_resolution", time_tolerance="30min")

    assert lowest["time"].size == 1
    assert float(lowest["cmems_glorys__uo"].isel(time=0, cmems_glorys__latitude=0, cmems_glorys__longitude=0)) == 10.0
    assert (
        float(lowest["era5_reanalysis__u10"].isel(time=0, era5_reanalysis__latitude=0, era5_reanalysis__longitude=0))
        == -1.0
    )
    assert lowest.attrs["collekt_time_policy"] == "lowest_resolution"

    assert highest["time"].size == 4
    daily_values = highest["cmems_glorys__uo"].isel(cmems_glorys__latitude=0, cmems_glorys__longitude=0).values
    assert daily_values[0] == 10.0
    assert np.isnan(daily_values[1:]).all()
    assert list(
        highest["era5_reanalysis__u10"].isel(era5_reanalysis__latitude=0, era5_reanalysis__longitude=0).values
    ) == [
        -1.0,
        0.0,
        1.0,
        2.0,
    ]
    assert highest.attrs["collekt_time_policy"] == "highest_resolution"


def test_assembler_aligns_time_to_custom_target_with_tolerance(tmp_path):
    import numpy as np

    glorys = _write_dataset_with_times(
        tmp_path / "glorys.nc",
        "uo",
        [[[10.0]]],
        times=["2023-06-15T00:00:00"],
        latitudes=[42.0],
        longitudes=[5.0],
    )
    era5 = _write_dataset_with_times(
        tmp_path / "era5.nc",
        "u10",
        [[[-1.0]], [[0.0]], [[1.0]]],
        times=["2023-06-15T00:00:00", "2023-06-15T01:00:00", "2023-06-15T02:00:00"],
        latitudes=[42.0],
        longitudes=[5.0],
    )
    result = _collection_result(
        tmp_path,
        SourceResult("cmems_glorys", SourceStatus.DOWNLOADED, path=glorys),
        SourceResult("era5_reanalysis", SourceStatus.DOWNLOADED, path=era5),
    )

    dataset = Assembler(result).to_xarray(
        time="custom",
        target_time=["2023-06-15T01:00:00"],
        time_tolerance="1min",
    )

    assert dataset["time"].size == 1
    assert np.isnan(
        dataset["cmems_glorys__uo"].isel(time=0, cmems_glorys__latitude=0, cmems_glorys__longitude=0).values
    )
    assert (
        float(dataset["era5_reanalysis__u10"].isel(time=0, era5_reanalysis__latitude=0, era5_reanalysis__longitude=0))
        == 0.0
    )
    assert dataset.attrs["collekt_time_policy"] == "custom"
    assert dataset.attrs["collekt_temporal_method"] == "nearest"


def test_assembler_interpolates_time_linearly(tmp_path):
    glorys = _write_dataset_with_times(
        tmp_path / "glorys.nc",
        "uo",
        [[[0.0]], [[2.0]]],
        times=["2023-06-15T00:00:00", "2023-06-15T02:00:00"],
        latitudes=[42.0],
        longitudes=[5.0],
    )
    result = _collection_result(
        tmp_path,
        SourceResult("cmems_glorys", SourceStatus.DOWNLOADED, path=glorys),
    )

    dataset = Assembler(result).to_xarray(
        time="custom",
        temporal_method="linear",
        target_time=["2023-06-15T01:00:00"],
    )

    assert float(dataset["cmems_glorys__uo"].isel(time=0, cmems_glorys__latitude=0, cmems_glorys__longitude=0)) == 1.0
    assert dataset.attrs["collekt_temporal_method"] == "linear"


def test_assembler_aggregates_time_to_custom_target(tmp_path):
    glorys = _write_dataset_with_times(
        tmp_path / "glorys.nc",
        "uo",
        [[[value]] for value in [0.0, 6.0, 12.0, 18.0, 24.0, 30.0, 36.0, 42.0]],
        times=[
            "2023-06-15T00:00:00",
            "2023-06-15T06:00:00",
            "2023-06-15T12:00:00",
            "2023-06-15T18:00:00",
            "2023-06-16T00:00:00",
            "2023-06-16T06:00:00",
            "2023-06-16T12:00:00",
            "2023-06-16T18:00:00",
        ],
        latitudes=[42.0],
        longitudes=[5.0],
    )
    result = _collection_result(
        tmp_path,
        SourceResult("cmems_glorys", SourceStatus.DOWNLOADED, path=glorys),
    )

    dataset = Assembler(result).to_xarray(
        time="custom",
        temporal_method="mean",
        target_time=["2023-06-15T00:00:00", "2023-06-16T00:00:00"],
    )

    values = dataset["cmems_glorys__uo"].isel(cmems_glorys__latitude=0, cmems_glorys__longitude=0).values
    assert list(values) == pytest.approx([9.0, 33.0])
    assert dataset.attrs["collekt_temporal_method"] == "mean"


def test_assembler_aligns_era5_valid_time_coordinate(tmp_path):
    era5 = _write_dataset_with_valid_time(
        tmp_path / "era5.nc",
        "u10",
        [[[-1.0]], [[0.0]], [[1.0]]],
        times=["2023-06-15T00:00:00", "2023-06-15T06:00:00", "2023-06-15T12:00:00"],
        latitudes=[42.0],
        longitudes=[5.0],
    )
    result = _collection_result(
        tmp_path,
        SourceResult("era5_reanalysis", SourceStatus.DOWNLOADED, path=era5),
    )

    dataset = Assembler(result).to_xarray(
        time="custom",
        target_time=["2023-06-15T06:00:00"],
        time_tolerance="1min",
    )

    assert "time" in dataset.coords
    assert "valid_time" not in dataset.coords
    assert dataset["era5_reanalysis__u10"].dims == ("time", "era5_reanalysis__latitude", "era5_reanalysis__longitude")
    assert (
        float(dataset["era5_reanalysis__u10"].isel(time=0, era5_reanalysis__latitude=0, era5_reanalysis__longitude=0))
        == 0.0
    )


def test_assembler_requires_custom_target_time(tmp_path):
    glorys = _write_dataset(
        tmp_path / "glorys.nc",
        "uo",
        [[[1.0]]],
        latitudes=[42.0],
        longitudes=[5.0],
    )
    result = _collection_result(
        tmp_path,
        SourceResult("cmems_glorys", SourceStatus.DOWNLOADED, path=glorys),
    )

    with pytest.raises(ValueError, match="target_time is required"):
        Assembler(result).to_xarray(time="custom")


def test_assembler_requires_custom_target_grid(tmp_path):
    glorys = _write_dataset(
        tmp_path / "glorys.nc",
        "uo",
        [[[1.0]]],
        latitudes=[42.0],
        longitudes=[5.0],
    )
    result = _collection_result(
        tmp_path,
        SourceResult("cmems_glorys", SourceStatus.DOWNLOADED, path=glorys),
    )

    with pytest.raises(ValueError, match="target_grid is required"):
        Assembler(result).to_xarray(grid="custom")


def test_assembler_opens_grib2_with_cfgrib_and_valid_time(tmp_path, monkeypatch):
    import numpy as np
    import xarray as xr

    grib = tmp_path / "wind.grib2"
    grib.write_bytes(b"fake grib")
    calls = []

    def fake_open_dataset(path, *args, **kwargs):
        calls.append((Path(path), args, kwargs))
        dataset = xr.Dataset(
            {
                "u10": (("step", "latitude", "longitude"), [[[1.0]], [[2.0]]]),
                "v10": (("step", "latitude", "longitude"), [[[-1.0]], [[-2.0]]]),
            },
            coords={
                "time": np.datetime64("2023-06-15T00:00:00"),
                "step": np.asarray([0, 6], dtype="timedelta64[h]"),
                "valid_time": (
                    "step",
                    np.asarray(["2023-06-15T00:00:00", "2023-06-15T06:00:00"], dtype="datetime64[ns]"),
                ),
                "latitude": [42.0],
                "longitude": [5.0],
            },
        )
        return _DatasetContext(dataset)

    monkeypatch.setattr(xr, "open_dataset", fake_open_dataset)
    monkeypatch.setattr("collekt.assemble.gridded._require_cfgrib_backend", lambda: None)
    result = _collection_result(
        tmp_path,
        SourceResult(
            "ecmwf_open_data_forecast",
            SourceStatus.DOWNLOADED,
            path=grib,
            format="grib2",
            details={"temporal": {"actual_sampling": "6h", "requested_sampling": "6h"}},
        ),
    )

    dataset = Assembler(result).to_xarray(
        time="custom",
        target_time=["2023-06-15T06:00:00"],
        time_tolerance="1min",
    )

    assert calls == [(grib, (), {"engine": "cfgrib", "backend_kwargs": {"indexpath": ""}})]
    assert "time" in dataset.coords
    assert "valid_time" not in dataset.coords
    assert dataset["ecmwf_open_data_forecast__u10"].dims == (
        "time",
        "ecmwf_open_data_forecast__latitude",
        "ecmwf_open_data_forecast__longitude",
    )
    assert (
        float(
            dataset["ecmwf_open_data_forecast__u10"].isel(
                time=0,
                ecmwf_open_data_forecast__latitude=0,
                ecmwf_open_data_forecast__longitude=0,
            )
        )
        == 2.0
    )


def test_assembler_reports_no_dataset_files(tmp_path):
    result = _collection_result(
        tmp_path,
        SourceResult("metadata", SourceStatus.DOWNLOADED, path=tmp_path / "metadata.json", format="json"),
    )

    with pytest.raises(ValueError, match="no downloaded NetCDF or GRIB2"):
        Assembler(result).to_xarray()


def test_assembler_rejects_alias_collisions_across_sources(tmp_path):
    glorys = _write_dataset(
        tmp_path / "glorys.nc",
        "uo",
        [[[1.0]]],
        latitudes=[42.0],
        longitudes=[5.0],
    )
    era5 = _write_dataset(
        tmp_path / "era5.nc",
        "u10",
        [[[2.0]]],
        latitudes=[42.0],
        longitudes=[5.0],
    )
    result = _collection_result(
        tmp_path,
        SourceResult("cmems_glorys", SourceStatus.DOWNLOADED, path=glorys),
        SourceResult("era5_reanalysis", SourceStatus.DOWNLOADED, path=era5),
    )

    with pytest.raises(ValueError, match="collision"):
        Assembler(result).to_xarray(
            aliases={
                "cmems_glorys__uo": "u",
                "era5_reanalysis__u10": "u",
            }
        )


def test_assembler_combines_same_source_files_by_coordinates(tmp_path):
    first = _write_dataset(
        tmp_path / "glorys_20230615.nc",
        "uo",
        [[[1.0]]],
        latitudes=[42.0],
        longitudes=[5.0],
        time="2023-06-15T00:00:00",
    )
    second = _write_dataset(
        tmp_path / "glorys_20230616.nc",
        "uo",
        [[[2.0]]],
        latitudes=[42.0],
        longitudes=[5.0],
        time="2023-06-16T00:00:00",
    )
    result = _collection_result(
        tmp_path,
        SourceResult("cmems_glorys", SourceStatus.DOWNLOADED, path=first),
        SourceResult("cmems_glorys", SourceStatus.DOWNLOADED, path=second),
    )

    dataset = Assembler(result).to_xarray()

    assert dataset["cmems_glorys__uo"].shape == (2, 1, 1)
