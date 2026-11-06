"""ERA5 adapter reading Google's analysis-ready, cloud-optimised (ARCO) Zarr store.

The store (https://github.com/google-research/arco-era5) is a public, anonymous
copy of ERA5 on the regular 0.25° grid. It is an alternative to the CDS-backed
`era5` adapter, not a replacement: every Zarr chunk holds one hour of one
variable for the whole globe, so the transfer volume depends on the number of
hours and variables, never on the region size. That makes it fast for short
windows but expensive for long time series over a small area, where the CDS
queue is the better trade. Choosing between them is left to the user.

Days are cropped to the padded request region and written as daily NetCDF files,
like the other gridded adapters. Variables are selected by their long ERA5
names (e.g. ``10m_u_component_of_wind``), as for `era5`, and written under the
short names the CDS NetCDF files use (e.g. ``u10``), taken from each store
variable's ``short_name`` attribute. The time coordinate is ``time``, where CDS
files use ``valid_time``.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
from functools import partial
from pathlib import Path
from typing import Any

from collekt.core.availability import Coverage
from collekt.core.config import Config, SourceConfig
from collekt.core.diagnostics import DoctorCheck, DoctorStatus, package_check
from collekt.core.request import Request
from collekt.core.temporal import SamplingPlan, sampling_plan
from collekt.sources.base import (
    ProgressCallback,
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
    should_reuse_cache,
)
from collekt.sources.batching.common import daily_output_errors, failed, staged_output
from collekt.sources.batching.gridded import GridSteps, run_grid_batch
from collekt.sources.era5 import _padded_area
from collekt.sources.planning import output_path, plan_source, static_source_coverage

DEFAULT_DATASET_ID = "arco-era5"
DEFAULT_STORE = "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3"
DEFAULT_PAD_DEG = 0.5


def _store_url(source: SourceConfig) -> str:
    return str(source.raw.get("store", DEFAULT_STORE))


def _open_store(url: str) -> Any:
    """Open the ARCO Zarr store lazily, anonymously, without loading any chunk."""
    try:
        import xarray as xr
    except ImportError as exc:  # pragma: no cover - exercised only without the dependency
        raise ImportError("xarray is required for ARCO-ERA5 downloads; reinstall collekt (uv sync).") from exc
    return xr.open_zarr(url, chunks=None, consolidated=True, storage_options={"token": "anon"})


def _valid_until(store: Any) -> date:
    """Return the last day the store holds data for (ERA5T included).

    The store's time axis is pre-allocated far beyond the data, so its
    `valid_time_stop*` attributes, not the time coordinate, bound what exists.
    """
    stop = store.attrs.get("valid_time_stop_era5t") or store.attrs["valid_time_stop"]
    return date.fromisoformat(str(stop)[:10])


def _crop(dataset: Any, area: list[float]) -> Any:
    """Crop a global 0-360 dataset to an ``[north, west, south, east]`` area in -180-180.

    Latitude stays descending as in the store; longitude is wrapped to -180-180
    and sorted, so a region crossing the Greenwich meridian stays contiguous.
    """
    import numpy as np

    north, west, south, east = area
    dataset = dataset.sel(latitude=slice(north, south))
    longitude = (dataset["longitude"].values + 180.0) % 360.0 - 180.0
    order = np.argsort(longitude, kind="stable")
    keep = order[(longitude[order] >= west) & (longitude[order] <= east)]
    return dataset.isel(longitude=keep).assign_coords(longitude=longitude[keep])


def _empty_variables(dataset: Any) -> list[str]:
    """Name the variables that hold no valid value for at least one timestamp.

    A missing upstream chunk reads back as fill values without an error, so this
    surfaces it. A land-only region legitimately has no sea surface temperature.
    """
    spatial = [dim for dim in ("latitude", "longitude") if dim in dataset.dims]
    return [name for name, variable in dataset.data_vars.items() if bool(variable.isnull().all(spatial).any())]


def _retrieve(store: Any, payload: dict[str, Any], path: Path) -> str | None:
    """Write the requested hours that an opened store already holds to `path` as NetCDF.

    Hours past the store's last valid day are left out. The store ends on a whole
    day, so a day is either complete or absent; a batch's split step reports the
    absent ones. Returns a message about empty variables, if any.

    Raises:
        ValueError: If a variable is not in the store or is not a single-level
            (time, latitude, longitude) field, or no requested hour is available yet.
    """
    import numpy as np

    missing = [name for name in payload["variables"] if name not in store.data_vars]
    if missing:
        raise ValueError(f"not in the ARCO-ERA5 store: {', '.join(missing)}")
    # The store also holds 37-level fields; one would cost 37x the volume and add a `level` dimension.
    leveled = [name for name in payload["variables"] if set(store[name].dims) != {"time", "latitude", "longitude"}]
    if leveled:
        raise ValueError(f"only single-level variables are supported, not: {', '.join(leveled)}")
    until = _valid_until(store)
    times = np.array([t for t in payload["times"] if date.fromisoformat(t[:10]) <= until], dtype="datetime64[ns]")
    if not len(times):
        raise ValueError(f"the ARCO-ERA5 store currently ends on {until.isoformat()}")
    subset = _crop(store[list(payload["variables"])].sel(time=times), payload["area"]).load()
    subset = subset.rename(
        {name: variable.attrs.get("short_name", name) for name, variable in subset.data_vars.items()}
    )
    for variable in subset.variables.values():
        variable.encoding = {}  # Zarr compressor/chunk encodings are invalid for NetCDF.
    subset.to_netcdf(path)
    empty = _empty_variables(subset)
    return (
        f"no valid values at some timestamps (missing upstream data or land/sea mask): {', '.join(empty)}"
        if empty
        else None
    )


def _request_body(request: Request, source: SourceConfig, day: date, plan: SamplingPlan) -> dict[str, Any]:
    times = plan.timestamps_for_day(day) or (datetime.combine(day, datetime.min.time()),)
    return {
        "store": _store_url(source),
        "variables": list(source.variables),
        "times": [timestamp.strftime("%Y-%m-%dT%H:00:00") for timestamp in times],
        "area": _padded_area(request, float(source.raw.get("pad_deg", DEFAULT_PAD_DEG))),
    }


def _select_day(dataset: Any, item: SourceResult) -> Any:
    """Select one day's requested hours, refusing a dataset that lacks any of them."""
    import numpy as np

    expected = np.array(item.details["request"]["times"], dtype="datetime64[ns]")
    available = np.isin(expected, dataset["time"].values)
    if not available.all():
        raise ValueError(f"the ARCO-ERA5 store has no data yet for {item.day} {str(expected[~available][0])[11:16]}")
    return dataset.sel(time=expected)


def fetch_era5_arco(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
) -> list[SourceResult]:
    """Fetch one cropped NetCDF file per requested day from the ARCO-ERA5 store."""
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    plan = sampling_plan(request, source)
    results: list[SourceResult] = []
    store = None  # opened on the first day that is not cached, then shared by all days
    for day in request.iter_days():
        item = SourceResult(
            source=source.name,
            status=SourceStatus.REUSED,
            path=output_path(request, source, request_dir, dataset_id, day),
            dataset_id=dataset_id,
            variables=source.variables,
            day=day.isoformat(),
            details={"temporal": plan.day_dict(day), "request": _request_body(request, source, day, plan)},
        )
        if should_reuse_cache(item.path.exists(), config):
            results.append(item)
            continue
        progress(source.name, f"downloading {day.isoformat()} from the ARCO-ERA5 store")
        try:
            store = store if store is not None else _open_store(_store_url(source))
            with staged_output(item.path) as staged:
                message = _retrieve(store, item.details["request"], staged)
        except Exception as exc:  # noqa: BLE001 - unavailable days are warnings, like the CDS adapter
            results.append(failed(item, exc))
            continue
        results.append(replace(item, status=SourceStatus.DOWNLOADED, message=message))
    return results


def _day_details(
    request: Request, source: SourceConfig, day: date, plan: SamplingPlan, coverage: Coverage | None
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    details = {
        "provider": "arco-era5",
        "method": "zarr",
        "temporal": plan.day_dict(day),
        "request": _request_body(request, source, day, plan),
    }
    return source.dataset_id or DEFAULT_DATASET_ID, details, {}


def plan_era5_arco(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan ARCO-ERA5 downloads using the source's declarative coverage."""
    coverage, method, checked = static_source_coverage(source)
    return plan_source(
        request,
        source,
        request_dir,
        coverage=coverage,
        method=method,
        checked=checked,
        dataset_for_day=lambda source, day: source.dataset_id or DEFAULT_DATASET_ID,
        day_details=_day_details,
        planned_format="netcdf",
    )


def diagnose(config: Config, online: bool) -> list[DoctorCheck]:
    """Return package diagnostics and, online, the store's current last valid day."""
    checks = [package_check("gcsfs package", "gcsfs"), package_check("zarr package", "zarr")]
    if online:
        try:
            until = _valid_until(_open_store(DEFAULT_STORE))
            checks.append(DoctorCheck("arco-era5 store", DoctorStatus.OK, f"data available until {until.isoformat()}"))
        except Exception as exc:  # noqa: BLE001 - doctor reports, it does not raise
            checks.append(DoctorCheck("arco-era5 store", DoctorStatus.WARN, f"store not reachable: {exc}"))
    return checks


def _batch_key(item: SourceResult) -> dict[str, Any]:
    """Days can share a store read when everything but the hours matches."""
    body = dict(item.details["request"])
    body.pop("times", None)
    return body


def _merge_batch(members: list[SourceResult]) -> dict[str, Any]:
    body = dict(members[0].details["request"])
    body["times"] = [time for item in members for time in item.details["request"]["times"]]
    return body


def _retrieve_batch(request: Request, source: SourceConfig, payload: dict[str, Any], path: Path) -> str | None:
    return _retrieve(_open_store(payload["store"]), payload, path)


register_adapter(
    SourceAdapter(
        kind="era5_arco",
        batch=partial(
            run_grid_batch,
            steps=GridSteps(
                compatible=_batch_key,
                merge=_merge_batch,
                retrieve=_retrieve_batch,
                split=_select_day,
                transport="zarr",
            ),
        ),
        batch_check=daily_output_errors,
        fetch=fetch_era5_arco,
        plan=plan_era5_arco,
        diagnose=diagnose,
        known_raw_keys=frozenset({"store", "pad_deg"}),
    )
)
