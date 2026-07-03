"""Build analysis-ready outputs from a downloaded collection."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import timedelta
from importlib.util import find_spec
from pathlib import Path
from typing import Any, Literal

from collekt.core.request import parse_sampling
from collekt.core.result import Result
from collekt.sources.base import SourceResult, SourceStatus

GridPolicy = Literal["native", "lowest_resolution", "highest_resolution", "custom"]
SpatialMethod = Literal["linear", "nearest"]
TargetGrid = Mapping[str, Iterable[float]]
TimePolicy = Literal["native", "lowest_resolution", "highest_resolution", "custom"]
TemporalMethod = Literal["nearest", "linear", "mean", "max", "min"]
TargetTime = Iterable[Any]

_LATITUDE_NAMES = ("latitude", "lat")
_LONGITUDE_NAMES = ("longitude", "lon")
_TIME_NAMES = ("time", "valid_time")
_DATASET_FORMATS = {"netcdf", "grib", "grib2"}


def _require_xarray():
    try:
        import xarray as xr
    except ImportError as exc:  # pragma: no cover - exercised only without optional analysis deps
        raise ImportError("Assembler requires xarray to build analysis-ready datasets") from exc
    return xr


def _require_cfgrib_backend() -> None:
    if find_spec("cfgrib") is None:
        raise ImportError("Assembler requires cfgrib to open GRIB2 files; reinstall collekt's dependencies (uv sync).")
    try:
        import eccodes

        eccodes.codes_get_api_version()
    except Exception as exc:  # noqa: BLE001 - backend availability varies by platform
        raise ImportError(
            "Assembler requires the ecCodes native library to open GRIB2 files. "
            "Reinstall collekt's dependencies (uv sync), or install ecCodes for your platform."
        ) from exc


def _source_name(source: str, name: str) -> str:
    return f"{source}__{name}"


class Assembler:
    """Convert a downloaded collection into analysis-ready objects.

    `Assembler` starts from a `Result` returned by `Fetcher.download`.
    By default, it keeps source identity explicit by prefixing variables,
    coordinates, and dimensions with the configured source name. This avoids
    collisions when a collection contains several products for the same physical
    quantity, such as multiple wind sources. Non-native grid policies
    interpolate NetCDF and GRIB2 sources onto a common overlapping
    latitude/longitude grid while keeping variable names source-prefixed or
    explicitly aliased.

    Example:
        ```python
        from collekt import Assembler

        result = fetcher.download()
        assembler = Assembler(result)

        ds = assembler.to_xarray(
            aliases={"era5_reanalysis__u10": "era5_u10"},
            grid="lowest_resolution",
            time="lowest_resolution",
        )
        assembler.to_netcdf("merged.nc", grid="lowest_resolution", time="lowest_resolution")
        arrays = assembler.to_numpy(grid="lowest_resolution", time="lowest_resolution")
        ```

    Args:
        result: Download result returned by `Fetcher.download`.
    """

    def __init__(self, result: Result) -> None:
        self.result = result

    def to_xarray(
        self,
        *,
        aliases: Mapping[str, str] | None = None,
        grid: GridPolicy = "native",
        spatial_method: SpatialMethod = "linear",
        target_grid: TargetGrid | None = None,
        time: TimePolicy = "native",
        temporal_method: TemporalMethod = "nearest",
        target_time: TargetTime | None = None,
        time_tolerance: str | timedelta | None = "1h",
    ) -> Any:
        """Open downloaded dataset files as one xarray dataset.

        Args:
            aliases: Optional explicit mapping from default assembled variable
                names, such as ``cmems_glorys__uo``, to user-facing names.
            grid: Spatial grid policy. ``"native"`` keeps each source on its
                own grid. ``"lowest_resolution"`` and ``"highest_resolution"``
                interpolate sources onto the coarsest or finest overlapping
                source grid. ``"custom"`` interpolates to `target_grid`.
            spatial_method: xarray interpolation method used when `grid` is not
                ``"native"``. Supported values are ``"linear"`` and
                ``"nearest"``.
            target_grid: Required when ``grid="custom"``. Mapping with
                ``latitude``/``longitude`` or ``lat``/``lon`` arrays.
            time: Temporal alignment policy. ``"native"`` keeps each source on
                its own time coordinate. ``"lowest_resolution"`` and
                ``"highest_resolution"`` align sources onto the coarsest or
                finest source timestamps. ``"custom"`` aligns to `target_time`.
            temporal_method: Temporal method used when `time` is not
                ``"native"``. ``"nearest"`` uses nearest-neighbour selection
                with `time_tolerance`; ``"linear"`` uses xarray interpolation;
                ``"mean"``, ``"max"``, and ``"min"`` aggregate with xarray
                resampling before aligning to the target timestamps.
            target_time: Required when ``time="custom"``. Iterable of target
                datetimes, ISO datetime strings, or NumPy datetime64 values.
            time_tolerance: Maximum nearest-neighbour temporal distance. Use
                values such as ``"30min"``, ``"1h"``, or a `datetime.timedelta`.
                Set to `None` to allow unbounded nearest-neighbour selection.

        Returns:
            An `xarray.Dataset` containing all dataset variables that could be
            opened from downloaded or reused NetCDF/GRIB2 source files.

        Raises:
            ValueError: If no readable dataset files are available, aliases
                collide, or a requested grid cannot be built.
        """
        self._validate_spatial_method(spatial_method)
        self._validate_temporal_method(temporal_method)
        xr = _require_xarray()
        datasets = self._open_datasets_by_source(
            xr,
            aliases or {},
            prefix_coordinates=grid == "native",
            prefix_time=time == "native",
        )
        if not datasets:
            raise ValueError("no downloaded NetCDF or GRIB2 files are available to assemble")
        if grid != "native":
            datasets = self._regrid_datasets(
                datasets,
                grid=grid,
                spatial_method=spatial_method,
                target_grid=target_grid,
            )
        if time != "native":
            datasets = self._align_time_datasets(
                datasets,
                time=time,
                temporal_method=temporal_method,
                target_time=target_time,
                time_tolerance=time_tolerance,
            )
        self._check_variable_collisions(datasets)
        attrs = {
            "collekt_grid_policy": grid,
            "collekt_spatial_method": spatial_method,
            "collekt_time_policy": time,
            "collekt_temporal_method": temporal_method,
        }
        if len(datasets) == 1:
            return datasets[0].assign_attrs(datasets[0].attrs | attrs)
        return xr.merge(datasets, compat="override", combine_attrs="drop_conflicts").assign_attrs(attrs)

    def to_netcdf(
        self,
        path: str | Path,
        *,
        aliases: Mapping[str, str] | None = None,
        grid: GridPolicy = "native",
        spatial_method: SpatialMethod = "linear",
        target_grid: TargetGrid | None = None,
        time: TimePolicy = "native",
        temporal_method: TemporalMethod = "nearest",
        target_time: TargetTime | None = None,
        time_tolerance: str | timedelta | None = "1h",
        **kwargs: Any,
    ) -> Path:
        """Write assembled data to a NetCDF file.

        Args:
            path: Destination NetCDF path.
            aliases: Optional explicit variable aliases.
            grid: Spatial grid policy.
            spatial_method: Spatial interpolation method.
            target_grid: Required when ``grid="custom"``.
            time: Temporal alignment policy.
            temporal_method: Temporal interpolation or aggregation method.
            target_time: Required when ``time="custom"``.
            time_tolerance: Maximum nearest-neighbour temporal distance.
            **kwargs: Extra keyword arguments passed to `xarray.Dataset.to_netcdf`.

        Returns:
            Destination path.
        """
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        ds = self.to_xarray(
            aliases=aliases,
            grid=grid,
            spatial_method=spatial_method,
            target_grid=target_grid,
            time=time,
            temporal_method=temporal_method,
            target_time=target_time,
            time_tolerance=time_tolerance,
        )
        ds.to_netcdf(destination, **kwargs)
        return destination

    def to_zarr(
        self,
        path: str | Path,
        *,
        aliases: Mapping[str, str] | None = None,
        grid: GridPolicy = "native",
        spatial_method: SpatialMethod = "linear",
        target_grid: TargetGrid | None = None,
        time: TimePolicy = "native",
        temporal_method: TemporalMethod = "nearest",
        target_time: TargetTime | None = None,
        time_tolerance: str | timedelta | None = "1h",
        **kwargs: Any,
    ) -> Path:
        """Write assembled data to a Zarr store.

        Args:
            path: Destination Zarr store path.
            aliases: Optional explicit variable aliases.
            grid: Spatial grid policy.
            spatial_method: Spatial interpolation method.
            target_grid: Required when ``grid="custom"``.
            time: Temporal alignment policy.
            temporal_method: Temporal interpolation or aggregation method.
            target_time: Required when ``time="custom"``.
            time_tolerance: Maximum nearest-neighbour temporal distance.
            **kwargs: Extra keyword arguments passed to `xarray.Dataset.to_zarr`.

        Returns:
            Destination path.
        """
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        ds = self.to_xarray(
            aliases=aliases,
            grid=grid,
            spatial_method=spatial_method,
            target_grid=target_grid,
            time=time,
            temporal_method=temporal_method,
            target_time=target_time,
            time_tolerance=time_tolerance,
        )
        ds.to_zarr(destination, **kwargs)
        return destination

    def to_numpy(
        self,
        variables: Iterable[str] | None = None,
        *,
        aliases: Mapping[str, str] | None = None,
        grid: GridPolicy = "native",
        spatial_method: SpatialMethod = "linear",
        target_grid: TargetGrid | None = None,
        time: TimePolicy = "native",
        temporal_method: TemporalMethod = "nearest",
        target_time: TargetTime | None = None,
        time_tolerance: str | timedelta | None = "1h",
    ) -> dict[str, Any]:
        """Return assembled data variables as NumPy arrays.

        Args:
            variables: Optional assembled variable names to extract after alias
                application. If omitted, all data variables are returned.
            aliases: Optional explicit variable aliases.
            grid: Spatial grid policy.
            spatial_method: Spatial interpolation method.
            target_grid: Required when ``grid="custom"``.
            time: Temporal alignment policy.
            temporal_method: Temporal interpolation or aggregation method.
            target_time: Required when ``time="custom"``.
            time_tolerance: Maximum nearest-neighbour temporal distance.

        Returns:
            Mapping from variable name to NumPy array.
        """
        ds = self.to_xarray(
            aliases=aliases,
            grid=grid,
            spatial_method=spatial_method,
            target_grid=target_grid,
            time=time,
            temporal_method=temporal_method,
            target_time=target_time,
            time_tolerance=time_tolerance,
        )
        selected = tuple(variables) if variables is not None else tuple(str(name) for name in ds.data_vars)
        missing = [name for name in selected if name not in ds.data_vars]
        if missing:
            raise ValueError(f"unknown assembled variable(s): {', '.join(missing)}")
        return {name: ds[name].values for name in selected}

    def _dataset_results(self) -> tuple[SourceResult, ...]:
        return tuple(
            result
            for result in self.result.results
            if result.status in {SourceStatus.DOWNLOADED, SourceStatus.REUSED}
            and result.path is not None
            and result.format.lower() in _DATASET_FORMATS
        )

    def _open_datasets_by_source(
        self,
        xr,
        aliases: Mapping[str, str],
        *,
        prefix_coordinates: bool,
        prefix_time: bool,
    ) -> list[Any]:
        grouped_results: dict[str, list[SourceResult]] = defaultdict(list)
        for result in self._dataset_results():
            grouped_results[result.source].append(result)

        datasets = []
        for results in grouped_results.values():
            source_datasets = [
                self._open_result_as_dataset(
                    result,
                    aliases,
                    prefix_coordinates=prefix_coordinates,
                    prefix_time=prefix_time,
                )
                for result in results
            ]
            if len(source_datasets) == 1:
                datasets.append(source_datasets[0])
            else:
                datasets.append(
                    xr.combine_by_coords(source_datasets, compat="no_conflicts", combine_attrs="drop_conflicts")
                )
        return datasets

    def _open_result_as_dataset(
        self,
        result: SourceResult,
        aliases: Mapping[str, str],
        *,
        prefix_coordinates: bool,
        prefix_time: bool,
    ) -> Any:
        xr = _require_xarray()
        assert result.path is not None
        with self._open_dataset(xr, result) as dataset:
            ds = dataset.load()
        if result.format.lower() in {"grib", "grib2"}:
            ds = self._normalize_grib_dataset(ds)
        renamed = (
            self._renames_for_result(result, ds, aliases, prefix_time=prefix_time)
            if prefix_coordinates
            else self._variable_renames_for_result(result, ds, aliases)
        )
        temporal = (result.details or {}).get("temporal") or {}
        return ds.rename(renamed).assign_attrs(
            collekt_source=result.source,
            collekt_dataset_id=result.dataset_id or "",
            collekt_requested_sampling=str(temporal.get("requested_sampling", "")),
            collekt_actual_sampling=str(temporal.get("actual_sampling", "")),
        )

    @staticmethod
    def _open_dataset(xr, result: SourceResult):
        if result.format.lower() == "netcdf":
            return xr.open_dataset(result.path)
        if result.format.lower() in {"grib", "grib2"}:
            _require_cfgrib_backend()
            return xr.open_dataset(result.path, engine="cfgrib", backend_kwargs={"indexpath": ""})
        raise ValueError(f"unsupported assembled file format {result.format!r}")

    @staticmethod
    def _normalize_grib_dataset(dataset):
        """Normalize cfgrib forecast coordinates for assembly."""
        ds = dataset
        if "valid_time" in ds.coords and "step" in ds.dims and ds.coords["valid_time"].dims == ("step",):
            ds = ds.swap_dims({"step": "valid_time"})
        elif "valid_time" in ds.coords and ds.coords["valid_time"].ndim == 0:
            ds = ds.expand_dims(valid_time=[ds.coords["valid_time"].values])
        elif "time" in ds.coords and ds.coords["time"].ndim == 0:
            ds = ds.expand_dims(time=[ds.coords["time"].values])

        drop_names = [name for name in ("time", "step") if name in ds.coords and name not in ds.dims]
        return ds.drop_vars(drop_names) if drop_names else ds

    def _renames_for_result(
        self,
        result: SourceResult,
        dataset,
        aliases: Mapping[str, str],
        *,
        prefix_time: bool,
    ) -> dict[str, str]:
        renames: dict[str, str] = {}
        used: set[str] = set()
        for name in dict.fromkeys([*dataset.dims, *dataset.coords, *dataset.data_vars]):
            if str(name) in _TIME_NAMES and not prefix_time and name not in dataset.data_vars:
                target_name = str(name)
            else:
                default_name = _source_name(result.source, str(name))
                target_name = aliases.get(default_name, default_name) if name in dataset.data_vars else default_name
            if target_name in used:
                raise ValueError(f"alias collision while assembling {result.source!r}: {target_name!r}")
            used.add(target_name)
            renames[str(name)] = target_name
        return renames

    def _variable_renames_for_result(self, result: SourceResult, dataset, aliases: Mapping[str, str]) -> dict[str, str]:
        renames: dict[str, str] = {}
        used: set[str] = set()
        for name in dataset.data_vars:
            default_name = _source_name(result.source, str(name))
            target_name = aliases.get(default_name, default_name)
            if target_name in used:
                raise ValueError(f"alias collision while assembling {result.source!r}: {target_name!r}")
            used.add(target_name)
            renames[str(name)] = target_name
        return renames

    def _regrid_datasets(
        self,
        datasets: list[Any],
        *,
        grid: GridPolicy,
        spatial_method: SpatialMethod,
        target_grid: TargetGrid | None,
    ) -> list[Any]:
        target_latitude, target_longitude = self._target_grid(datasets, grid=grid, target_grid=target_grid)
        return [
            self._interpolate_to_grid(
                dataset,
                latitude=target_latitude,
                longitude=target_longitude,
                method=spatial_method,
            )
            for dataset in datasets
        ]

    def _align_time_datasets(
        self,
        datasets: list[Any],
        *,
        time: TimePolicy,
        temporal_method: TemporalMethod,
        target_time: TargetTime | None,
        time_tolerance: str | timedelta | None,
    ) -> list[Any]:
        target = self._target_temporal_grid(datasets, time=time, target_time=target_time)
        tolerance = self._time_tolerance(time_tolerance)
        return [
            self._align_dataset_time(
                dataset,
                target_time=target["time"],
                target_resolution_ns=target["resolution_ns"],
                method=temporal_method,
                tolerance=tolerance,
            )
            for dataset in datasets
        ]

    @staticmethod
    def _check_variable_collisions(datasets: Iterable[Any]) -> None:
        seen: set[str] = set()
        collisions: set[str] = set()
        for dataset in datasets:
            for name in dataset.data_vars:
                text = str(name)
                if text in seen:
                    collisions.add(text)
                seen.add(text)
        if collisions:
            raise ValueError(f"assembled variable name collision(s): {', '.join(sorted(collisions))}")

    @staticmethod
    def _validate_spatial_method(method: str) -> None:
        if method not in {"linear", "nearest"}:
            raise ValueError(f"unknown spatial interpolation method {method!r}")

    @staticmethod
    def _validate_temporal_method(method: str) -> None:
        if method not in {"nearest", "linear", "mean", "max", "min"}:
            raise ValueError(f"unknown temporal interpolation method {method!r}")

    def _target_temporal_grid(
        self,
        datasets: list[Any],
        *,
        time: TimePolicy,
        target_time: TargetTime | None,
    ) -> dict[str, Any]:
        if time == "custom":
            values = self._custom_target_time(target_time)
            return {"time": values, "resolution_ns": self._target_time_resolution(values)}
        if time not in {"lowest_resolution", "highest_resolution"}:
            raise ValueError(f"unknown time policy {time!r}")

        temporal = [self._temporal_info(dataset) for dataset in datasets]
        if time == "lowest_resolution":
            selected = max(temporal, key=lambda info: info["resolution_ns"])
        else:
            selected = min(temporal, key=lambda info: info["resolution_ns"])
        return {"time": selected["time"], "resolution_ns": selected["resolution_ns"]}

    @staticmethod
    def _custom_target_time(target_time: TargetTime | None) -> Any:
        if target_time is None:
            raise ValueError("target_time is required when time='custom'")
        import numpy as np

        values = np.asarray(tuple(target_time))
        if values.size == 0:
            raise ValueError("target_time must not be empty")
        return _as_datetime64(values)

    def _temporal_info(self, dataset) -> dict[str, Any]:
        time_name = self._time_coord_name(dataset)
        values = self._one_dimensional_time_coord(dataset, time_name)
        resolution = self._time_resolution(values, dataset)
        return {"time_name": time_name, "time": values, "resolution_ns": resolution}

    @staticmethod
    def _time_coord_name(dataset) -> str:
        for candidate in _TIME_NAMES:
            if candidate in dataset.coords and dataset.coords[candidate].values.ndim == 1:
                return candidate
        if not any(candidate in dataset.coords for candidate in _TIME_NAMES):
            raise ValueError("cannot align dataset without a 1D time coordinate")
        raise ValueError("cannot align dataset without a 1D time coordinate")

    @staticmethod
    def _one_dimensional_time_coord(dataset, name: str) -> Any:
        values = _as_datetime64(dataset.coords[name].values)
        if values.ndim != 1:
            raise ValueError(f"cannot align coordinate {name!r}: only 1D coordinates are supported")
        if values.size == 0:
            raise ValueError(f"cannot align coordinate {name!r}: coordinate is empty")
        return values

    @staticmethod
    def _time_resolution(values, dataset) -> int:
        import numpy as np

        unique = np.unique(values.astype("datetime64[ns]"))
        inferred = Assembler._target_time_resolution(unique)
        if inferred is not None:
            return inferred
        sampling = str(dataset.attrs.get("collekt_actual_sampling") or "")
        if sampling:
            return int(np.timedelta64(parse_sampling(sampling), "h").astype("timedelta64[ns]").astype("int64"))
        return int(np.timedelta64(24, "h").astype("timedelta64[ns]").astype("int64"))

    @staticmethod
    def _target_time_resolution(values) -> int | None:
        import numpy as np

        unique = np.unique(values.astype("datetime64[ns]"))
        if unique.size <= 1:
            return None
        diffs = np.diff(unique).astype("timedelta64[ns]").astype("int64")
        diffs = diffs[diffs > 0]
        if diffs.size == 0:
            return None
        return int(np.median(diffs))

    @staticmethod
    def _time_tolerance(value: str | timedelta | None) -> Any:
        if value is None:
            return None
        import numpy as np

        if isinstance(value, timedelta):
            return np.timedelta64(int(value.total_seconds() * 1_000_000_000), "ns")
        text = str(value).strip().lower()
        units = (
            ("hours", "h"),
            ("hour", "h"),
            ("hrs", "h"),
            ("hr", "h"),
            ("minutes", "m"),
            ("minute", "m"),
            ("mins", "m"),
            ("min", "m"),
            ("seconds", "s"),
            ("second", "s"),
            ("secs", "s"),
            ("sec", "s"),
            ("days", "D"),
            ("day", "D"),
        )
        for suffix, unit in units:
            if text.endswith(suffix):
                return np.timedelta64(int(text.removesuffix(suffix).strip()), unit)
        unit = text[-1:]
        if unit in {"h", "m", "s", "d"}:
            parsed_unit = "D" if unit == "d" else unit
            return np.timedelta64(int(text[:-1].strip()), parsed_unit)
        return np.timedelta64(int(text), "s")

    def _align_dataset_time(
        self,
        dataset,
        *,
        target_time,
        target_resolution_ns: int | None,
        method: TemporalMethod,
        tolerance,
    ):
        if method == "nearest":
            return self._nearest_to_time(dataset, target_time=target_time, tolerance=tolerance)
        if method == "linear":
            return self._interpolate_to_time(dataset, target_time=target_time)
        return self._aggregate_to_time(
            dataset,
            target_time=target_time,
            target_resolution_ns=target_resolution_ns,
            method=method,
            tolerance=tolerance,
        )

    def _nearest_to_time(self, dataset, *, target_time, tolerance):
        time_name = self._time_coord_name(dataset)
        dataset = dataset.assign_coords({time_name: _as_datetime64(dataset.coords[time_name].values)})
        indexer = {time_name: target_time}
        if tolerance is None:
            aligned = dataset.reindex(indexer, method="nearest")
        else:
            aligned = dataset.reindex(indexer, method="nearest", tolerance=tolerance)
        if time_name != "time":
            aligned = aligned.rename({time_name: "time"})
        return aligned

    def _interpolate_to_time(self, dataset, *, target_time):
        time_name = self._time_coord_name(dataset)
        dataset = dataset.assign_coords({time_name: _as_datetime64(dataset.coords[time_name].values)})
        interpolated = dataset.interp({time_name: target_time}, method="linear")
        if time_name != "time":
            interpolated = interpolated.rename({time_name: "time"})
        return interpolated

    def _aggregate_to_time(
        self,
        dataset,
        *,
        target_time,
        target_resolution_ns: int | None,
        method: Literal["mean", "max", "min"],
        tolerance,
    ):
        if target_resolution_ns is None:
            raise ValueError(
                "temporal aggregation with time='custom' requires at least two target timestamps "
                "so the target sampling can be inferred"
            )
        time_name = self._time_coord_name(dataset)
        dataset = dataset.assign_coords({time_name: _as_datetime64(dataset.coords[time_name].values)})
        frequency = self._frequency_from_resolution_ns(target_resolution_ns)
        resampler = dataset.resample({time_name: frequency}, origin=target_time[0])
        aggregated = getattr(resampler, method)()
        return self._nearest_to_time(aggregated, target_time=target_time, tolerance=tolerance)

    @staticmethod
    def _frequency_from_resolution_ns(resolution_ns: int) -> str:
        if resolution_ns <= 0:
            raise ValueError("target temporal resolution must be positive")
        ns_per_second = 1_000_000_000
        if resolution_ns % ns_per_second != 0:
            return f"{resolution_ns}ns"
        seconds = resolution_ns // ns_per_second
        if seconds % 3_600 == 0:
            return f"{seconds // 3_600}h"
        if seconds % 60 == 0:
            return f"{seconds // 60}min"
        return f"{seconds}s"

    def _target_grid(
        self,
        datasets: list[Any],
        *,
        grid: GridPolicy,
        target_grid: TargetGrid | None,
    ) -> tuple[Any, Any]:
        if grid == "custom":
            return self._custom_target_grid(target_grid)
        if grid not in {"lowest_resolution", "highest_resolution"}:
            raise ValueError(f"unknown grid policy {grid!r}")

        import numpy as np

        spatial = [self._spatial_info(dataset) for dataset in datasets]
        lat_min = max(info["lat_min"] for info in spatial)
        lat_max = min(info["lat_max"] for info in spatial)
        lon_min = max(info["lon_min"] for info in spatial)
        lon_max = min(info["lon_max"] for info in spatial)
        if lat_min > lat_max or lon_min > lon_max:
            raise ValueError("cannot build a common grid: source latitude/longitude ranges do not overlap")

        if grid == "lowest_resolution":
            selected = max(spatial, key=lambda info: info["resolution"])
        else:
            selected = min(spatial, key=lambda info: info["resolution"])

        latitude = self._clip_or_generate(selected["latitude"], lat_min, lat_max, selected["lat_resolution"])
        longitude = self._clip_or_generate(selected["longitude"], lon_min, lon_max, selected["lon_resolution"])
        return np.asarray(latitude, dtype=float), np.asarray(longitude, dtype=float)

    @staticmethod
    def _custom_target_grid(target_grid: TargetGrid | None) -> tuple[Any, Any]:
        if target_grid is None:
            raise ValueError("target_grid is required when grid='custom'")
        latitude = target_grid.get("latitude", target_grid.get("lat"))
        longitude = target_grid.get("longitude", target_grid.get("lon"))
        if latitude is None or longitude is None:
            raise ValueError("target_grid must define latitude/longitude or lat/lon")

        import numpy as np

        latitude_array = np.asarray(tuple(latitude), dtype=float)
        longitude_array = np.asarray(tuple(longitude), dtype=float)
        if latitude_array.size == 0 or longitude_array.size == 0:
            raise ValueError("target_grid latitude and longitude must not be empty")
        return latitude_array, longitude_array

    def _spatial_info(self, dataset) -> dict[str, Any]:
        lat_name, lon_name = self._spatial_coord_names(dataset)
        latitude = self._one_dimensional_coord(dataset, lat_name)
        longitude = self._one_dimensional_coord(dataset, lon_name)
        lat_resolution = self._resolution(latitude, lat_name)
        lon_resolution = self._resolution(longitude, lon_name)
        return {
            "latitude_name": lat_name,
            "longitude_name": lon_name,
            "latitude": latitude,
            "longitude": longitude,
            "lat_min": float(min(latitude)),
            "lat_max": float(max(latitude)),
            "lon_min": float(min(longitude)),
            "lon_max": float(max(longitude)),
            "lat_resolution": lat_resolution,
            "lon_resolution": lon_resolution,
            "resolution": max(lat_resolution, lon_resolution),
        }

    @staticmethod
    def _spatial_coord_names(dataset) -> tuple[str, str]:
        latitude = next((name for name in _LATITUDE_NAMES if name in dataset.coords), None)
        longitude = next((name for name in _LONGITUDE_NAMES if name in dataset.coords), None)
        if latitude is None or longitude is None:
            raise ValueError("cannot regrid dataset without 1D latitude/longitude coordinates")
        return latitude, longitude

    @staticmethod
    def _one_dimensional_coord(dataset, name: str) -> Any:
        values = dataset.coords[name].values
        if values.ndim != 1:
            raise ValueError(f"cannot regrid coordinate {name!r}: only 1D coordinates are supported")
        return values

    @staticmethod
    def _resolution(values, name: str) -> float:
        import numpy as np

        diffs = np.abs(np.diff(np.asarray(values, dtype=float)))
        diffs = diffs[diffs > 0]
        if diffs.size == 0:
            raise ValueError(f"cannot infer grid resolution from coordinate {name!r}")
        return float(np.median(diffs))

    @staticmethod
    def _clip_or_generate(values, lower: float, upper: float, resolution: float) -> Any:
        import numpy as np

        source = np.asarray(values, dtype=float)
        ascending = bool(source[-1] >= source[0])
        mask = (source >= lower) & (source <= upper)
        clipped = source[mask]
        if clipped.size > 0:
            return clipped

        count = int(np.floor((upper - lower) / resolution)) + 1
        generated = lower + np.arange(max(count, 1)) * resolution
        generated = generated[generated <= upper]
        if generated.size == 0:
            generated = np.asarray([lower])
        return generated if ascending else generated[::-1]

    def _interpolate_to_grid(self, dataset, *, latitude, longitude, method: SpatialMethod):
        lat_name, lon_name = self._spatial_coord_names(dataset)
        interpolated = dataset.interp({lat_name: latitude, lon_name: longitude}, method=method)
        renames = {}
        if lat_name != "latitude":
            renames[lat_name] = "latitude"
        if lon_name != "longitude":
            renames[lon_name] = "longitude"
        if renames:
            interpolated = interpolated.rename(renames)
        return interpolated


def _as_datetime64(values) -> Any:
    import numpy as np

    array = np.asarray(values)
    if np.issubdtype(array.dtype, np.datetime64):
        return array.astype("datetime64[ns]")
    return np.asarray(array, dtype="datetime64[ns]")
