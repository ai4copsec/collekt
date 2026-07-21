"""ECMWF Open Data forecast adapter.

ECMWF Open Data always serves the full global grid; there is no server-side
region subsetting. Downloaded GRIB2 files are therefore cropped to the padded
request region and written out as NetCDF, matching the region-scoped output
of the other gridded adapters (`cmems`, `era5`).
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

from collekt.core.availability import Coverage
from collekt.core.config import Config, SourceConfig
from collekt.core.diagnostics import DoctorCheck, package_check
from collekt.core.naming import format_pattern, pattern_values
from collekt.core.request import Region, Request
from collekt.core.temporal import SamplingPlan, sampling_plan
from collekt.sources.base import (
    ProgressCallback,
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
)
from collekt.sources.planning import plan_source, static_source_coverage

DEFAULT_DATASET_ID = "ecmwf-open-data-ifs"
DEFAULT_PAD_DEG = 0.5
DEFAULT_RESOLUTION = 0.25


def _client_class():
    try:
        from ecmwf.opendata import Client
    except ImportError as exc:  # pragma: no cover - exercised only without optional extra
        raise ImportError(
            "The 'ecmwf-opendata' package is required for ECMWF forecast downloads; "
            "reinstall collekt's dependencies (uv sync)."
        ) from exc
    return Client


def _grid_resolution(resol: str, default: float = DEFAULT_RESOLUTION) -> float:
    """Parse an ECMWF Open Data ``resol`` label (e.g. ``0p25``) into degrees."""
    text = str(resol).split("-", 1)[0]
    try:
        return float(text.replace("p", "."))
    except ValueError:
        return default


def _padded_bbox(region: Region, pad_deg: float, resolution: float) -> tuple[float, float, float, float]:
    """Return ``(south, north, west, east)`` padded and snapped to the native grid."""
    south = max(-90.0, math.floor((region.south - pad_deg) / resolution) * resolution)
    north = min(90.0, math.ceil((region.north + pad_deg) / resolution) * resolution)
    west = max(-180.0, math.floor((region.west - pad_deg) / resolution) * resolution)
    east = min(180.0, math.ceil((region.east + pad_deg) / resolution) * resolution)
    return south, north, west, east


def _open_raw_datasets(path: Path) -> list[Any]:
    """Open a downloaded global GRIB2 file, one dataset per cfgrib hypercube.

    ECMWF Open Data mixes instantaneous fields (10u/10v/100u/100v) with
    time-processed ones (10fg, a running maximum); a single `xr.open_dataset` call
    cannot merge those into one hypercube and silently drops the incompatible
    variable. `cfgrib.open_datasets` returns one dataset per compatible group
    instead, so nothing requested is lost.
    """
    try:
        import cfgrib
    except ImportError as exc:  # pragma: no cover - exercised only without optional extra
        raise ImportError(
            "xarray and cfgrib are required to crop ECMWF Open Data files; reinstall collekt's dependencies (uv sync)."
        ) from exc
    return cfgrib.open_datasets(path, backend_kwargs={"indexpath": ""})


def _rename_to_requested_params(dataset: Any, params: tuple[str, ...]) -> Any:
    """Rename cfgrib-decoded variables back to the exact param mnemonics that were requested.

    eccodes appends the accumulation window length to the GRIB short name of
    time-processed parameters (e.g. ``10fg`` decodes as ``10fg3`` for a 3-hourly
    window), and cfgrib derives the xarray variable name from that suffixed short
    name (e.g. ``fg10_3``). Rename each variable back to the mnemonic it was
    requested under so callers can look it up by that name.
    """
    rename = {}
    for name, variable in dataset.data_vars.items():
        short_name = variable.attrs.get("GRIB_shortName", name)
        for param in params:
            if short_name == param or re.fullmatch(re.escape(param) + r"\d+", short_name):
                rename[name] = param
                break
    return dataset.rename(rename) if rename else dataset


def _crop_dataset(dataset: Any, region: Region, pad_deg: float, resolution: float) -> Any:
    """Crop a global, 0-360 longitude dataset to the padded request region."""
    south, north, west, east = _padded_bbox(region, pad_deg, resolution)
    lat_name = "latitude" if "latitude" in dataset.coords else "lat"
    lon_name = "longitude" if "longitude" in dataset.coords else "lon"
    normalized = dataset.assign_coords({lon_name: ((dataset[lon_name] + 180) % 360) - 180})
    sorted_dataset = normalized.sortby([lat_name, lon_name])
    return sorted_dataset.sel({lat_name: slice(south, north), lon_name: slice(west, east)})


def _crop_to_netcdf(
    raw_path: Path, output_path: Path, region: Region, pad_deg: float, resolution: float, params: tuple[str, ...]
) -> list[str]:
    """Crop a downloaded global GRIB2 file to the request region and write NetCDF.

    Returns the requested ``params`` that ended up missing from the written file.
    """
    import xarray as xr

    datasets = _open_raw_datasets(raw_path)
    try:
        combined = datasets[0] if len(datasets) == 1 else xr.merge(datasets, join="outer", compat="override")
        merged = _rename_to_requested_params(combined, params).load()
        cropped = _crop_dataset(merged, region, pad_deg, resolution)
    finally:
        for dataset in datasets:
            dataset.close()
    cropped.to_netcdf(output_path)
    return [param for param in params if param not in cropped.data_vars]


def _request_for_day(source: SourceConfig, day: date, plan: SamplingPlan | None = None) -> dict[str, object]:
    raw = source.raw
    run_time = str(raw.get("time", "00"))
    steps = raw.get("steps", raw.get("step", [0, 3, 6, 9, 12, 15, 18, 21, 24]))
    if plan is not None:
        steps = _steps_for_day(source, day, plan)
    request = {
        "date": day.strftime("%Y%m%d"),
        "time": run_time,
        "step": steps,
        "type": str(raw.get("type", "fc")),
        "levtype": str(raw.get("levtype", "sfc")),
        "param": list(source.variables),
    }
    stream = raw.get("stream")
    if stream is not None:
        request["stream"] = str(stream)
    return request


def fetch_ecmwf_open_data(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
) -> list[SourceResult]:
    """Fetch ECMWF Open Data forecast files, cropped to the request region."""
    results: list[SourceResult] = []
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    model = str(source.raw.get("model", "ifs"))
    resol = str(source.raw.get("resol", "0p25"))
    pad_deg = float(source.raw.get("pad_deg", DEFAULT_PAD_DEG))
    resolution = _grid_resolution(resol)
    plan = sampling_plan(request, source)
    client = None
    for day in request.iter_days():
        run_time = str(source.raw.get("time", "00"))
        values = pattern_values(
            source=source.name,
            dataset_id=dataset_id,
            region=request.region,
            start=request.start_datetime,
            end=request.end_datetime,
            day=day,
        ) | {"time": run_time}
        output_path = out_dir / format_pattern(source.filename_pattern, values)
        if output_path.exists() and config.cache.reuse_existing and not config.cache.overwrite:
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.REUSED,
                    path=output_path,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    day=day.isoformat(),
                    format="netcdf",
                    details={"temporal": plan.day_dict(day)},
                )
            )
            continue

        raw_path = output_path.with_suffix(".raw.grib2")
        progress(source.name, f"downloading {day.isoformat()} {run_time} UTC forecast from ECMWF Open Data")
        try:
            if client is None:
                client = _client_class()(source=str(source.raw.get("source", "ecmwf")), model=model, resol=resol)
            client.retrieve(_request_for_day(source, day, plan), target=str(raw_path))
            missing = _crop_to_netcdf(raw_path, output_path, request.region, pad_deg, resolution, source.variables)
        except Exception as exc:  # noqa: BLE001 - provider availability failures are warnings
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    message=str(exc),
                    day=day.isoformat(),
                    format="netcdf",
                    details={"temporal": plan.day_dict(day)},
                )
            )
            continue
        finally:
            raw_path.unlink(missing_ok=True)
        if output_path.exists():
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.DOWNLOADED,
                    path=output_path,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    message=(f"missing from the cropped file (provider limitation): {', '.join(missing)}")
                    if missing
                    else None,
                    day=day.isoformat(),
                    format="netcdf",
                    details={"temporal": plan.day_dict(day)},
                )
            )
        else:
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    message="download completed but file is missing",
                    day=day.isoformat(),
                    format="netcdf",
                    details={"temporal": plan.day_dict(day)},
                )
            )
    return results


def _steps_for_day(source: SourceConfig, day: date, plan: SamplingPlan) -> list[int]:
    run_hour = int(str(source.raw.get("time", "00")).split(":", 1)[0])
    run = datetime.combine(day, time(hour=run_hour), tzinfo=plan.source_timestamps[0].tzinfo)
    steps: list[int] = []
    for timestamp in plan.timestamps_for_day(day):
        delta = timestamp - run
        step_hours = delta.total_seconds() / 3600
        if step_hours >= 0 and step_hours.is_integer():
            steps.append(int(step_hours))
    return steps or [0]


def _ecmwf_dataset_for_day(source: SourceConfig, day: date) -> str:
    return source.dataset_id or DEFAULT_DATASET_ID


def _ecmwf_day_details(
    request: Request, source: SourceConfig, day: date, plan: SamplingPlan, coverage: Coverage | None
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    run_time = str(source.raw.get("time", "00"))
    pad_deg = float(source.raw.get("pad_deg", DEFAULT_PAD_DEG))
    resolution = _grid_resolution(str(source.raw.get("resol", "0p25")))
    south, north, west, east = _padded_bbox(request.region, pad_deg, resolution)
    details = {
        "provider": "ecmwf-opendata",
        "method": "retrieve",
        "temporal": plan.day_dict(day),
        "request": _request_for_day(source, day, plan),
        "crop": {"south": south, "north": north, "west": west, "east": east},
    }
    return dataset_id, details, {"time": run_time}


def plan_ecmwf_open_data(
    request: Request, source: SourceConfig, config: Config, request_dir: Path
) -> list[SourceResult]:
    """Plan ECMWF Open Data downloads using the source's declarative coverage."""
    coverage, method, checked = static_source_coverage(source)
    return plan_source(
        request,
        source,
        request_dir,
        coverage=coverage,
        method=method,
        checked=checked,
        dataset_for_day=_ecmwf_dataset_for_day,
        day_details=_ecmwf_day_details,
        planned_format="netcdf",
    )


def diagnose(config: Config, online: bool) -> list[DoctorCheck]:
    """Return ECMWF Open Data package diagnostics."""
    return [
        package_check("ecmwf-opendata package", "ecmwf.opendata"),
        package_check("cfgrib package", "cfgrib"),
    ]


register_adapter(
    SourceAdapter(kind="ecmwf_open_data", fetch=fetch_ecmwf_open_data, plan=plan_ecmwf_open_data, diagnose=diagnose)
)
