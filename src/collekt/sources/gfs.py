"""NOAA GFS analysis adapter (NOMADS GRIB filter).

GFS runs four analysis cycles a day — 00, 06, 12 and 18 UTC — so **6-hourly is
the native cadence and a hard ceiling**: there is no finer analysis. Sub-6-hourly
GFS data exists only as forecast steps, which are a different kind of field, so
they belong to a separate source rather than a finer sampling of this one (see
this module's ``stream`` note below).

Levels
------
The analysis file (``gfs.tHHz.pgrb2.0p25.anl``) carries wind at 20, 30, 40, 50,
80 and 100 m above ground — **but not at 10 m**, which is only present in the
forecast files. Variables are therefore requested as ``{height}u`` / ``{height}v``
mnemonics (``20u``, ``100v``, …), following the ECMWF Open Data style, and the
adapter maps each to the provider's ``var_UGRD``/``var_VGRD`` plus
``lev_{height}_m_above_ground``.

Retention
---------
NOMADS keeps roughly the last ten days; older dates return HTTP 403. This adapter
is consequently near-real-time only. The much deeper AWS mirror
(``noaa-gfs-bdp-pds``, from 2021) needs byte-range requests against the ``.idx``
sidecar and is not implemented here.

Region subsetting is done server-side through the filter's ``subregion``
parameters, which shrinks a global 20 m wind field from ~1.9 MB to ~14 kB for a
Mediterranean-sized box.
"""

from __future__ import annotations

import math
import re
import urllib.error
import urllib.request
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
    missing_after_fetch,
    null_progress,
    register_adapter,
    should_reuse_cache,
)
from collekt.sources.batching.common import daily_output_errors
from collekt.sources.batching.files import run_file_batch
from collekt.sources.planning import plan_source, static_source_coverage

DEFAULT_DATASET_ID = "gfs-analysis"
DEFAULT_BASE_URL = "https://nomads.ncep.noaa.gov/cgi-bin"
DEFAULT_CYCLES = (0, 6, 12, 18)
DEFAULT_PAD_DEG = 0.5
DEFAULT_RESOLUTION = "0p25"
DEFAULT_STREAM = "anl"

# Wind variable mnemonic -> (provider variable, height in metres).
_WIND_MNEMONIC = re.compile(r"^(?P<height>\d+)(?P<component>[uv])$")
_COMPONENT_VAR = {"u": "UGRD", "v": "VGRD"}


class UnknownVariableError(ValueError):
    """Raised when a requested variable is not a supported GFS wind mnemonic."""


def _grid_resolution(resolution: str) -> float:
    """Parse a GFS resolution label (e.g. ``0p25``) into degrees.

    Example:
        ```python
        _grid_resolution("0p25")
        ```

    Args:
        resolution: Resolution label as it appears in GFS filenames.

    Returns:
        Grid spacing in degrees.
    """
    try:
        return float(str(resolution).replace("p", "."))
    except ValueError:
        return float(DEFAULT_RESOLUTION.replace("p", "."))


def _parse_variable(name: str) -> tuple[str, int]:
    """Return ``(provider_variable, height_m)`` for one wind mnemonic."""
    match = _WIND_MNEMONIC.match(str(name).strip().lower())
    if match is None:
        raise UnknownVariableError(
            f"{name!r} is not a GFS wind mnemonic; expected <height><component>, e.g. '20u' or '100v'"
        )
    return _COMPONENT_VAR[match.group("component")], int(match.group("height"))


def _padded_bbox(region: Region, pad_deg: float, resolution: float) -> tuple[float, float, float, float]:
    """Return ``(south, north, west, east)`` padded and snapped to the native grid."""
    south = max(-90.0, math.floor((region.south - pad_deg) / resolution) * resolution)
    north = min(90.0, math.ceil((region.north + pad_deg) / resolution) * resolution)
    west = max(-180.0, math.floor((region.west - pad_deg) / resolution) * resolution)
    east = min(180.0, math.ceil((region.east + pad_deg) / resolution) * resolution)
    return south, north, west, east


def _configured_cycles(source: SourceConfig) -> tuple[int, ...]:
    raw = source.raw.get("cycles", DEFAULT_CYCLES)
    return tuple(sorted({int(cycle) for cycle in raw}))


def _cycle_times(request: Request, source: SourceConfig, day: date) -> tuple[datetime, ...]:
    """Return the analysis cycle timestamps to fetch for one UTC day.

    Cycles outside the request's ``[start, end]`` window are dropped, so a
    partial-day request does not download fields it did not ask for.
    """
    tzinfo = request.start_datetime.tzinfo
    stamps = []
    for hour in _configured_cycles(source):
        stamp = datetime.combine(day, time(hour=hour), tzinfo=tzinfo)
        if request.start_datetime <= stamp <= request.end_datetime:
            stamps.append(stamp)
    return tuple(stamps)


def _grib_filename(source: SourceConfig, cycle: datetime) -> str:
    resolution = str(source.raw.get("resolution", DEFAULT_RESOLUTION))
    stream = str(source.raw.get("stream", DEFAULT_STREAM))
    return f"gfs.t{cycle:%H}z.pgrb2.{resolution}.{stream}"


def _cycle_url(request: Request, source: SourceConfig, cycle: datetime) -> str:
    """Build the NOMADS GRIB-filter URL for one analysis cycle."""
    base_url = str(source.raw.get("base_url", DEFAULT_BASE_URL)).rstrip("/")
    resolution = str(source.raw.get("resolution", DEFAULT_RESOLUTION))
    pad_deg = float(source.raw.get("pad_deg", DEFAULT_PAD_DEG))
    south, north, west, east = _padded_bbox(request.region, pad_deg, _grid_resolution(resolution))

    provider_vars, heights = set(), set()
    for name in source.variables:
        provider_var, height = _parse_variable(name)
        provider_vars.add(provider_var)
        heights.add(height)

    params = [
        f"dir=%2Fgfs.{cycle:%Y%m%d}%2F{cycle:%H}%2Fatmos",
        f"file={_grib_filename(source, cycle)}",
    ]
    params += [f"var_{name}=on" for name in sorted(provider_vars)]
    params += [f"lev_{height}_m_above_ground=on" for height in sorted(heights)]
    params += [
        "subregion=",
        f"leftlon={west}",
        f"rightlon={east}",
        f"toplat={north}",
        f"bottomlat={south}",
    ]
    return f"{base_url}/filter_gfs_{resolution}.pl?" + "&".join(params)


def _download_cycle(url: str, target: Path) -> None:
    """Download one filtered GRIB2 cycle, raising if the provider returns nothing.

    The NOMADS filter answers an unsatisfiable level/variable combination with a
    200 and an empty body rather than an error, so an empty response has to be
    treated as a failure explicitly.
    """
    with urllib.request.urlopen(url) as response:  # noqa: S310 - fixed https provider URL
        payload = response.read()
    if not payload:
        raise ValueError(
            "provider returned an empty response; the requested level/variable combination "
            "is probably absent from this file (GFS analyses carry no 10 m wind)"
        )
    target.write_bytes(payload)


def _rename_to_requested_variables(dataset: Any, variables: tuple[str, ...]) -> Any:
    """Rename cfgrib's decoded wind variables to the requested mnemonics.

    cfgrib names GFS wind by GRIB short name, which for heights above 10 m is a
    bare ``u``/``v`` carrying the height as a ``heightAboveGround`` coordinate.
    This expands that back into one variable per requested mnemonic (``20u``,
    ``100v``, …) so callers can look each up by the name they asked for.
    """
    heights = {}
    for name in variables:
        provider_var, height = _parse_variable(name)
        heights[name] = (provider_var[0].lower(), float(height))

    level_name = "heightAboveGround"
    renamed = {}
    for mnemonic, (component, height) in heights.items():
        if component not in dataset.data_vars:
            continue
        variable = dataset[component]
        if level_name in variable.dims:
            renamed[mnemonic] = variable.sel({level_name: height}, drop=True)
        elif level_name not in dataset.coords or float(dataset[level_name]) == height:
            renamed[mnemonic] = variable
    out = dataset.drop_vars([name for name in ("u", "v") if name in dataset.data_vars]).assign(renamed)
    # Drop the level coordinate only after assigning: the renamed variables are
    # selected from `dataset`, so they carry it back in if it is dropped first.
    if level_name in out.coords:
        out = out.drop_vars(level_name)
    return out


def _concat_cycles_to_netcdf(raw_paths: list[Path], output_path: Path, variables: tuple[str, ...]) -> list[str]:
    """Merge one day's downloaded cycles along time and write a single NetCDF.

    Returns the requested variables that ended up missing from the written file.
    """
    try:
        import cfgrib
        import xarray as xr
    except ImportError as exc:  # pragma: no cover - exercised only without optional extra
        raise ImportError(
            "xarray and cfgrib are required to convert GFS GRIB2 files; reinstall collekt's dependencies (uv sync)."
        ) from exc

    per_cycle = []
    opened = []
    try:
        for raw_path in raw_paths:
            dataset = cfgrib.open_dataset(raw_path, backend_kwargs={"indexpath": ""})
            opened.append(dataset)
            renamed = _rename_to_requested_variables(dataset, variables).load()
            per_cycle.append(renamed.expand_dims("time") if "time" not in renamed.dims else renamed)
    finally:
        for dataset in opened:
            dataset.close()

    # coords/compat are passed explicitly: the cycles share an identical grid and
    # differ only along time, and xarray's defaults for both are changing.
    combined = (
        per_cycle[0] if len(per_cycle) == 1 else xr.concat(per_cycle, dim="time", coords="minimal", compat="override")
    )
    combined = combined.sortby("time")
    combined.to_netcdf(output_path)
    return [name for name in variables if name not in combined.data_vars]


def fetch_gfs(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
    _days: tuple[date, ...] | None = None,
) -> list[SourceResult]:
    """Fetch GFS analysis wind, one region-subset NetCDF per day.

    Each day's available analysis cycles are downloaded and concatenated along
    time. Cycles the provider does not serve yet are skipped rather than failing
    the day: an analysis lands a few hours after its cycle time, so a
    near-real-time request legitimately finds the most recent ones missing.

    Args:
        request: Region and time window to fetch.
        source: Resolved source configuration.
        config: Global configuration (cache policy).
        request_dir: Directory for this collection request.
        progress: Optional progress callback.
        _days: Internal batch-planner day selection; keeps the full request's
            timestamp anchor and output naming context.

    Returns:
        One `SourceResult` per requested day.
    """
    results: list[SourceResult] = []
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    plan = sampling_plan(request, source)

    for day in request.iter_days() if _days is None else _days:
        values = pattern_values(
            source=source.name,
            dataset_id=dataset_id,
            region=request.region,
            start=request.start_datetime,
            end=request.end_datetime,
            day=day,
        )
        output_path = out_dir / format_pattern(source.filename_pattern, values)
        details = {"temporal": plan.day_dict(day)}
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
                    details=details,
                )
            )
            continue

        raw_paths: list[Path] = []
        skipped: list[str] = []
        try:
            for cycle in _cycle_times(request, source, day):
                raw_path = output_path.with_suffix(f".{cycle:%H}z.raw.grib2")
                progress(source.name, f"downloading {cycle:%Y-%m-%d %H}z GFS analysis")
                try:
                    _download_cycle(_cycle_url(request, source, cycle), raw_path)
                except (urllib.error.URLError, OSError, ValueError) as exc:
                    skipped.append(f"{cycle:%H}z ({exc})")
                    continue
                raw_paths.append(raw_path)

            if not raw_paths:
                reason = "; ".join(skipped) if skipped else "no analysis cycle fell inside the request window"
                results.append(
                    SourceResult(
                        source=source.name,
                        status=SourceStatus.SKIPPED,
                        dataset_id=dataset_id,
                        variables=source.variables,
                        message=f"no cycle available for {day.isoformat()}: {reason}",
                        day=day.isoformat(),
                        format="netcdf",
                        details=details,
                    )
                )
                continue
            missing = _concat_cycles_to_netcdf(raw_paths, output_path, source.variables)
        finally:
            for raw_path in raw_paths:
                raw_path.unlink(missing_ok=True)

        if not output_path.exists():
            results.append(missing_after_fetch(source, dataset_id, day=day.isoformat(), details=details))
            continue
        notes = []
        if skipped:
            notes.append(f"cycles not available yet: {', '.join(skipped)}")
        if missing:
            notes.append(f"missing from the converted file: {', '.join(missing)}")
        results.append(
            SourceResult(
                source=source.name,
                status=SourceStatus.DOWNLOADED,
                path=output_path,
                dataset_id=dataset_id,
                variables=source.variables,
                message="; ".join(notes) or None,
                day=day.isoformat(),
                format="netcdf",
                details=details | {"cycles": len(raw_paths)},
            )
        )
    return results


def _gfs_dataset_for_day(source: SourceConfig, day: date) -> str:
    return source.dataset_id or DEFAULT_DATASET_ID


def _gfs_day_details(
    request: Request, source: SourceConfig, day: date, plan: SamplingPlan, coverage: Coverage | None
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    pad_deg = float(source.raw.get("pad_deg", DEFAULT_PAD_DEG))
    resolution = str(source.raw.get("resolution", DEFAULT_RESOLUTION))
    south, north, west, east = _padded_bbox(request.region, pad_deg, _grid_resolution(resolution))
    cycles = _cycle_times(request, source, day)
    details = {
        "provider": "nomads-gfs-filter",
        "method": "grib-filter",
        "temporal": plan.day_dict(day),
        "request": {
            "cycles": [f"{cycle:%H}z" for cycle in cycles],
            "files": [_grib_filename(source, cycle) for cycle in cycles],
            "variables": list(source.variables),
        },
        "crop": {"south": south, "north": north, "west": west, "east": east},
    }
    return dataset_id, details, {}


def plan_gfs(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan GFS downloads using the source's declarative coverage."""
    coverage, method, checked = static_source_coverage(source)
    return plan_source(
        request,
        source,
        request_dir,
        coverage=coverage,
        method=method,
        checked=checked,
        dataset_for_day=_gfs_dataset_for_day,
        day_details=_gfs_day_details,
        planned_format="netcdf",
    )


def diagnose(config: Config, online: bool) -> list[DoctorCheck]:
    """Return GFS conversion-dependency diagnostics."""
    return [package_check("cfgrib package", "cfgrib"), package_check("xarray package", "xarray")]


register_adapter(
    SourceAdapter(
        kind="gfs",
        batch=run_file_batch,
        batch_check=daily_output_errors,
        fetch=fetch_gfs,
        plan=plan_gfs,
        diagnose=diagnose,
        known_raw_keys=frozenset({"base_url", "cycles", "pad_deg", "resolution", "stream"}),
    )
)
