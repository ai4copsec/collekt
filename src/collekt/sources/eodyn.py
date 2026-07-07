"""eOdyn ocean-data adapter.

eOdyn exposes surface-current and drifter products. A production API is planned;
until it exists this adapter serves currents from a frozen historical archive of
L4 NetCDF files (``mode: archive``), limited to the western Mediterranean for
April-August 2023. The ``mode: api`` seam is reserved for the live API and
behaves identically from the caller's side once available. The adapter emits an
`eOdynArchiveWarning` and skips any request outside the archive coverage.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from collekt.core.availability import Coverage, clip_region
from collekt.core.config import Config, SourceConfig
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

DEFAULT_DATASET_ID = "EODYN-OS-VELOCITY-L4"
DEFAULT_ARCHIVE_ROOT = "~/Research/Currents/OSmose/results/MARES/causal"
DEFAULT_DAY_PATTERN = "{date:%Y%m%d}_MARES/{date:%Y%m%d}_MARES_osmose_L4.nc"
DEFAULT_COVERAGE: dict[str, object] = {
    "west": -6.0,
    "east": 20.0,
    "south": 35.0,
    "north": 45.0,
    "start": "2023-04-01",
    "end": "2023-08-31",
}


class eOdynArchiveWarning(UserWarning):
    """Warns that eOdyn currents come from a limited historical preview archive."""


@dataclass(frozen=True)
class _Coverage:
    """Spatial and temporal extent served by the eOdyn preview archive."""

    west: float
    east: float
    south: float
    north: float
    start: date
    end: date


def _coverage(source: SourceConfig) -> _Coverage:
    raw = dict(source.raw.get("coverage") or {})
    return _Coverage(
        west=float(raw.get("west", DEFAULT_COVERAGE["west"])),
        east=float(raw.get("east", DEFAULT_COVERAGE["east"])),
        south=float(raw.get("south", DEFAULT_COVERAGE["south"])),
        north=float(raw.get("north", DEFAULT_COVERAGE["north"])),
        start=date.fromisoformat(str(raw.get("start", DEFAULT_COVERAGE["start"]))),
        end=date.fromisoformat(str(raw.get("end", DEFAULT_COVERAGE["end"]))),
    )


def _archive_path(source: SourceConfig, day: date) -> Path:
    root = str(source.raw.get("archive_root", DEFAULT_ARCHIVE_ROOT))
    day_pattern = str(source.raw.get("day_pattern", DEFAULT_DAY_PATTERN))
    return Path(root).expanduser() / format_pattern(day_pattern, {"date": day})


def _region_out_of_coverage(region: Region, coverage: _Coverage) -> bool:
    return (
        region.east < coverage.west
        or region.west > coverage.east
        or region.north < coverage.south
        or region.south > coverage.north
    )


def _day_out_of_coverage(day: date, coverage: _Coverage) -> bool:
    return day < coverage.start or day > coverage.end


def _archive_clip(region: Region, coverage: _Coverage) -> tuple[float, float, float, float]:
    return (
        max(region.west, coverage.west),
        min(region.east, coverage.east),
        max(region.south, coverage.south),
        min(region.north, coverage.north),
    )


def _historical_warning(coverage: _Coverage) -> str:
    return (
        "eOdyn currents are served from a historical preview archive: the production API "
        f"does not exist yet, so coverage is limited to longitudes [{coverage.west}, {coverage.east}], "
        f"latitudes [{coverage.south}, {coverage.north}], and {coverage.start.isoformat()} to "
        f"{coverage.end.isoformat()}. Requests outside this region or date range are skipped."
    )


def _region_skip_message(region: Region, coverage: _Coverage) -> str:
    return (
        f"region (west={region.west}, east={region.east}, south={region.south}, north={region.north}) "
        f"is outside the OSmose preview coverage (west={coverage.west}, east={coverage.east}, "
        f"south={coverage.south}, north={coverage.north})"
    )


def _day_skip_message(day: date, coverage: _Coverage) -> str:
    return (
        f"{day.isoformat()} is outside the OSmose preview archive "
        f"({coverage.start.isoformat()} to {coverage.end.isoformat()})"
    )


def fetch_eodyn(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
) -> list[SourceResult]:
    """Serve eOdyn surface currents.

    With ``mode: archive`` (the default until the API ships), each requested day
    is read from the matching L4 file, subset to the requested variables and to
    the request region clipped to the archive coverage, and written next to the
    other sources. Days or regions outside the archive coverage are skipped with
    an explanatory message.
    """
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    mode = str(source.raw.get("mode", "archive"))
    if mode == "api":
        return [
            SourceResult(
                source=source.name,
                status=SourceStatus.SKIPPED,
                dataset_id=dataset_id,
                variables=source.variables,
                message="the eOdyn API is not available yet; set mode: archive to use the preview archive",
            )
        ]

    coverage = _coverage(source)
    warnings.warn(_historical_warning(coverage), eOdynArchiveWarning, stacklevel=2)
    progress(source.name, "serving the historical eOdyn preview archive (the production API does not exist yet)")

    if _region_out_of_coverage(request.region, coverage):
        return [
            SourceResult(
                source=source.name,
                status=SourceStatus.SKIPPED,
                dataset_id=dataset_id,
                variables=source.variables,
                message=_region_skip_message(request.region, coverage),
                details={"coverage": _coverage_dict(coverage)},
            )
        ]

    results: list[SourceResult] = []
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    plan = sampling_plan(request, source)
    for day in request.iter_days():
        temporal = plan.day_dict(day)
        if _day_out_of_coverage(day, coverage):
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    message=_day_skip_message(day, coverage),
                    day=day.isoformat(),
                    details={"temporal": temporal, "coverage": _coverage_dict(coverage)},
                )
            )
            continue

        values = pattern_values(
            source=source.name,
            dataset_id=dataset_id,
            region=request.region,
            start=request.start_datetime,
            end=request.end_datetime,
            day=day,
        )
        output_path = out_dir / format_pattern(source.filename_pattern, values)
        archive_path = _archive_path(source, day)
        if output_path.exists() and config.cache.reuse_existing and not config.cache.overwrite:
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.REUSED,
                    path=output_path,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    day=day.isoformat(),
                    details={"temporal": temporal, "archive_path": str(archive_path)},
                )
            )
            continue

        if not archive_path.exists():
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    message=f"eOdyn archive file not found: {archive_path}",
                    day=day.isoformat(),
                    details={"temporal": temporal, "archive_path": str(archive_path)},
                )
            )
            continue

        progress(source.name, f"reading {day.isoformat()} from the eOdyn archive")
        try:
            _write_subset(archive_path, output_path, request.region, coverage, source.variables)
        except Exception as exc:  # noqa: BLE001 - corrupted/missing-variable files are warnings, not crashes
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    message=str(exc),
                    day=day.isoformat(),
                    details={"temporal": temporal, "archive_path": str(archive_path)},
                )
            )
            continue

        results.append(
            SourceResult(
                source=source.name,
                status=SourceStatus.DOWNLOADED,
                path=output_path,
                dataset_id=dataset_id,
                variables=source.variables,
                day=day.isoformat(),
                details={
                    "provider": "eodyn-osmose",
                    "method": "archive",
                    "temporal": temporal,
                    "archive_path": str(archive_path),
                },
            )
        )
    return results


def _coverage_dict(coverage: _Coverage) -> dict[str, object]:
    return {
        "west": coverage.west,
        "east": coverage.east,
        "south": coverage.south,
        "north": coverage.north,
        "start": coverage.start.isoformat(),
        "end": coverage.end.isoformat(),
    }


def _write_subset(
    archive_path: Path,
    output_path: Path,
    region: Region,
    coverage: _Coverage,
    variables: tuple[str, ...],
) -> None:
    import xarray as xr

    west, east, south, north = _archive_clip(region, coverage)
    with xr.open_dataset(archive_path) as ds:
        available = [name for name in variables if name in ds.data_vars]
        if not available:
            raise ValueError(f"none of the requested variables {list(variables)} are present in {archive_path.name}")
        subset = ds[available].sel(
            latitude=_ascending_slice(ds["latitude"].values, south, north),
            longitude=_ascending_slice(ds["longitude"].values, west, east),
        )
        subset.to_netcdf(output_path)


def _ascending_slice(values, low: float, high: float) -> slice:
    """Return a coordinate slice ordered to match the coordinate direction."""
    if values.size and values[0] > values[-1]:
        return slice(high, low)
    return slice(low, high)


def _eodyn_dataset_for_day(source: SourceConfig, day: date) -> str:
    return source.dataset_id or DEFAULT_DATASET_ID


def _eodyn_day_details(
    request: Request, source: SourceConfig, day: date, plan: SamplingPlan, coverage: Coverage | None
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    if coverage is not None:
        west, east, south, north = clip_region(request.region, coverage)
    else:
        region = request.region
        west, east, south, north = region.west, region.east, region.south, region.north
    details = {
        "provider": "eodyn-osmose",
        "method": "archive",
        "temporal": plan.day_dict(day),
        "request": {
            "dataset_id": dataset_id,
            "variables": list(source.variables),
            "archive_path": str(_archive_path(source, day)),
            "minimum_longitude": west,
            "maximum_longitude": east,
            "minimum_latitude": south,
            "maximum_latitude": north,
            "coverage": coverage.as_dict() if coverage else None,
        },
    }
    return dataset_id, details, {}


def plan_eodyn(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan eOdyn archive reads using the source's declarative coverage."""
    coverage, method, checked = static_source_coverage(source)
    return plan_source(
        request,
        source,
        request_dir,
        coverage=coverage,
        method=method,
        checked=checked,
        dataset_for_day=_eodyn_dataset_for_day,
        day_details=_eodyn_day_details,
        planned_format="netcdf",
    )


register_adapter(SourceAdapter(kind="eodyn", fetch=fetch_eodyn, plan=plan_eodyn))
