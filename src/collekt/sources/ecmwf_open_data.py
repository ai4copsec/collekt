"""ECMWF Open Data forecast adapter.

ECMWF Open Data always serves the full global grid; there is no server-side
region subsetting. Downloaded GRIB2 files are therefore cropped to the padded
request region and written out as NetCDF, matching the region-scoped output
of the other gridded adapters (`cmems`, `era5`).

Each day is read from its own run (lead times 0-24 h). Days after the newest
fully published run are read from that run's forecast instead, at the lead
times that reach them.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime, time
from functools import partial
from pathlib import Path
from typing import Any

from collekt.core.availability import Coverage, today
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
    missing_after_fetch,
    null_progress,
    register_adapter,
    should_reuse_cache,
)
from collekt.sources.batching.common import daily_output_errors
from collekt.sources.batching.gridded import GridSteps, run_grid_batch
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


def _run_hour(source: SourceConfig) -> int:
    """Return the configured run hour (``time``, e.g. ``"00"``) as an integer."""
    return int(str(source.raw.get("time", "00")).split(":", 1)[0])


def _published_steps(run_hour: int) -> list[int]:
    """Return the lead times (hours) ECMWF Open Data publishes for an IFS run.

    The 00 and 12 UTC runs are 3-hourly up to 144 h, then 6-hourly up to 360 h.
    The 06 and 18 UTC runs are 3-hourly up to 144 h. This holds since IFS cycle
    49r1 (2024-11-12), before the catalogs' coverage starts.
    """
    if run_hour in (0, 12):
        return [*range(0, 145, 3), *range(150, 361, 6)]
    return list(range(0, 145, 3))


def _latest_run(source: SourceConfig) -> date:
    """Return the date of the newest run published up to its last lead time.

    Probes ECMWF Open Data with ``HEAD`` requests. If the probe fails, assumes
    today's run, so download errors still name the missing run.
    """
    run_hour = _run_hour(source)
    try:
        client = _client_class()(
            source=str(source.raw.get("source", "ecmwf")),
            model=str(source.raw.get("model", "ifs")),
            resol=str(source.raw.get("resol", "0p25")),
        )
        latest = client.latest(
            type=str(source.raw.get("type", "fc")),
            stream=str(source.raw.get("stream", "oper")),
            time=run_hour,
            step=max(_published_steps(run_hour)),
        )
    except Exception:  # noqa: BLE001 - an unreachable provider falls back to today's run
        return today()
    return latest.date()


def _latest_run_for(request: Request, source: SourceConfig) -> date | None:
    """Return the newest run when the request reaches today, else ``None`` (no probe needed)."""
    return _latest_run(source) if request.iter_days()[-1] >= today() else None


def _run_for_day(day: date, latest_run: date | None) -> date:
    """Read a day from its own run, or from the newest run's forecast once past it."""
    return day if latest_run is None or day <= latest_run else latest_run


def _grid_resolution(resol: str, default: float = DEFAULT_RESOLUTION) -> float:
    """Parse an ECMWF Open Data ``resol`` label (e.g. ``0p25``) into degrees."""
    text = str(resol).split("-", 1)[0]
    try:
        return float(text.replace("p", "."))
    except ValueError:
        return default


def _snap(value: float, resolution: float, rounding: Any) -> float:
    """Snap ``value`` to the grid, ignoring float noise (e.g. ``1.5 / 0.1 == 15.000000000000002``)."""
    return round(rounding(round(value / resolution, 9)) * resolution, 6)


def _padded_bbox(region: Region, pad_deg: float, resolution: float) -> tuple[float, float, float, float]:
    """Return ``(south, north, west, east)`` padded and snapped to the native grid."""
    south = max(-90.0, _snap(region.south - pad_deg, resolution, math.floor))
    north = min(90.0, _snap(region.north + pad_deg, resolution, math.ceil))
    west = max(-180.0, _snap(region.west - pad_deg, resolution, math.floor))
    east = min(180.0, _snap(region.east + pad_deg, resolution, math.ceil))
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
    # Rounding drops float noise in the grid (e.g. 181.6 - 180 == 1.5999999999999943),
    # which would otherwise push edge rows just outside the snapped bounds.
    normalized = dataset.assign_coords(
        {lat_name: dataset[lat_name].round(6), lon_name: (((dataset[lon_name] + 180) % 360) - 180).round(6)}
    )
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


def _request_for_day(
    source: SourceConfig, day: date, plan: SamplingPlan | None = None, run_day: date | None = None
) -> dict[str, object]:
    raw = source.raw
    run_day = run_day or day
    run_time = str(raw.get("time", "00"))
    lead = 24 * (day - run_day).days
    steps = [lead + step for step in raw.get("steps", raw.get("step", [0, 3, 6, 9, 12, 15, 18, 21, 24]))]
    if plan is not None:
        steps = _steps_for_day(source, day, plan, run_day)
    request = {
        "date": run_day.strftime("%Y%m%d"),
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
    latest_run = _latest_run_for(request, source)
    client = None
    for day in request.iter_days():
        run_time = str(source.raw.get("time", "00"))
        run_day = _run_for_day(day, latest_run)
        values = pattern_values(
            source=source.name,
            dataset_id=dataset_id,
            region=request.region,
            start=request.start_datetime,
            end=request.end_datetime,
            day=day,
        ) | {"time": run_time, "run": f"{run_day:%Y%m%d}"}
        output_path = out_dir / format_pattern(source.filename_pattern, values)
        if should_reuse_cache(output_path.exists(), config):
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
        progress(
            source.name,
            f"downloading {day.isoformat()} from the {run_day.isoformat()} {run_time} UTC run of ECMWF Open Data",
        )
        try:
            if client is None:
                client = _client_class()(source=str(source.raw.get("source", "ecmwf")), model=model, resol=resol)
            client.retrieve(_request_for_day(source, day, plan, run_day), target=str(raw_path))
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
                missing_after_fetch(source, dataset_id, day=day.isoformat(), details={"temporal": plan.day_dict(day)})
            )
    return results


def _steps_for_day(source: SourceConfig, day: date, plan: SamplingPlan, run_day: date | None = None) -> list[int]:
    """Return the published lead times of ``run_day``'s run that fall on ``day``'s sampled timestamps."""
    run_day = run_day or day
    run_hour = _run_hour(source)
    run = datetime.combine(run_day, time(hour=run_hour), tzinfo=plan.source_timestamps[0].tzinfo)
    published = set(_published_steps(run_hour))
    steps: list[int] = []
    for timestamp in plan.timestamps_for_day(day):
        delta = timestamp - run
        step_hours = delta.total_seconds() / 3600
        if step_hours.is_integer() and int(step_hours) in published:
            steps.append(int(step_hours))
    return steps or [24 * (day - run_day).days]


def _ecmwf_dataset_for_day(source: SourceConfig, day: date) -> str:
    return source.dataset_id or DEFAULT_DATASET_ID


def _ecmwf_day_details(
    request: Request,
    source: SourceConfig,
    day: date,
    plan: SamplingPlan,
    coverage: Coverage | None,
    *,
    latest_run: date | None = None,
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    run_time = str(source.raw.get("time", "00"))
    run_day = _run_for_day(day, latest_run)
    pad_deg = float(source.raw.get("pad_deg", DEFAULT_PAD_DEG))
    resolution = _grid_resolution(str(source.raw.get("resol", "0p25")))
    south, north, west, east = _padded_bbox(request.region, pad_deg, resolution)
    details = {
        "provider": "ecmwf-opendata",
        "method": "retrieve",
        "temporal": plan.day_dict(day),
        "request": _request_for_day(source, day, plan, run_day),
        "crop": {"south": south, "north": north, "west": west, "east": east},
    }
    return dataset_id, details, {"time": run_time, "run": f"{run_day:%Y%m%d}"}


def plan_ecmwf_open_data(
    request: Request, source: SourceConfig, config: Config, request_dir: Path
) -> list[SourceResult]:
    """Plan ECMWF Open Data downloads using the source's declarative coverage.

    When the request reaches today, the newest published run is probed online so
    that later days are planned from its forecast.
    """
    coverage, method, checked = static_source_coverage(source)
    day_details = partial(_ecmwf_day_details, latest_run=_latest_run_for(request, source))
    return plan_source(
        request,
        source,
        request_dir,
        coverage=coverage,
        method=method,
        checked=checked,
        dataset_for_day=_ecmwf_dataset_for_day,
        day_details=day_details,
        planned_format="netcdf",
    )


def diagnose(config: Config, online: bool) -> list[DoctorCheck]:
    """Return ECMWF Open Data package diagnostics."""
    return [
        package_check("ecmwf-opendata package", "ecmwf.opendata"),
        package_check("cfgrib package", "cfgrib"),
    ]


def _batch_key(item: SourceResult) -> dict[str, Any]:
    """Forecast dates can share a client call when everything but the date matches."""
    body = dict(item.details["request"])
    body.pop("date", None)
    return body


def _merge_batch(members: list[SourceResult]) -> dict[str, Any]:
    body = dict(members[0].details["request"])
    body["date"] = [item.details["request"]["date"] for item in members]
    return body


def _retrieve_batch(request: Request, source: SourceConfig, payload: dict[str, Any], path: Path) -> str | None:
    resol = str(source.raw.get("resol", "0p25"))
    client = _client_class()(
        source=str(source.raw.get("source", "ecmwf")),
        model=str(source.raw.get("model", "ifs")),
        resol=resol,
    )
    raw = path.with_suffix(".grib2")
    client.retrieve(payload, target=str(raw))
    missing = _crop_to_netcdf(
        raw,
        path,
        request.region,
        float(source.raw.get("pad_deg", DEFAULT_PAD_DEG)),
        _grid_resolution(resol),
        source.variables,
    )
    if missing:
        return f"missing from the cropped file (provider limitation): {', '.join(missing)}"
    return None


def _split_batch(dataset: Any, item: SourceResult) -> Any:
    """Select one day's forecast run, keeping all of its lead times."""
    import numpy as np

    # `time` is the run, whereas `valid_time` combines run and lead time.
    request = item.details["request"]
    hour = int(str(request["time"]).split(":", 1)[0])
    run = np.datetime64(f"{datetime.strptime(request['date'], '%Y%m%d'):%Y-%m-%d}T{hour:02d}:00:00")
    if "time" in dataset.dims:
        return dataset.sel(time=run)
    if dataset.coords["time"].values != run:
        raise ValueError(f"missing forecast run {run}")
    return dataset


register_adapter(
    SourceAdapter(
        kind="ecmwf_open_data",
        batch=partial(
            run_grid_batch,
            steps=GridSteps(
                compatible=_batch_key,
                merge=_merge_batch,
                retrieve=_retrieve_batch,
                split=_split_batch,
                transport="individual_files",
            ),
        ),
        batch_check=daily_output_errors,
        fetch=fetch_ecmwf_open_data,
        plan=plan_ecmwf_open_data,
        diagnose=diagnose,
        known_raw_keys=frozenset(
            {"model", "resol", "pad_deg", "source", "time", "steps", "step", "type", "levtype", "stream"}
        ),
    )
)
