"""CMEMS adapter using the Copernicus Marine Toolbox."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from collekt.core.availability import Coverage, describe_coverage, merge_coverages, parse_relative_date
from collekt.core.config import Config, SourceConfig
from collekt.core.naming import format_pattern, pattern_values
from collekt.core.request import Request
from collekt.core.temporal import SamplingPlan, day_bounds, sampling_plan
from collekt.sources.base import (
    ProgressCallback,
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
)
from collekt.sources.planning import plan_source


def _copernicusmarine():
    try:
        import copernicusmarine
    except ImportError as exc:  # pragma: no cover - exercised only without optional extra
        raise ImportError(
            "The 'copernicusmarine' package is required for CMEMS downloads. "
            "Install the cmems extra with: uv sync --extra cmems"
        ) from exc
    return copernicusmarine


def _select_dataset(source: SourceConfig, day: date) -> str:
    if source.mode == "historical":
        return source.dataset_my or source.dataset_id or source.name
    if source.mode == "nrt":
        return source.dataset_nrt or source.dataset_id or source.name
    if source.dataset_nrt and source.dataset_my and source.nrt_cutoff:
        cutoff = parse_relative_date(source.nrt_cutoff)
        return source.dataset_nrt if cutoff is not None and day >= cutoff else source.dataset_my
    return source.dataset_id or source.dataset_nrt or source.dataset_my or source.name


def fetch_cmems(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
) -> list[SourceResult]:
    """Fetch CMEMS files for all requested days.

    Args:
        request: Normalized collection request.
        source: Source-specific configuration.
        config: Global configuration.
        request_dir: Directory for this collection.
        progress: Optional progress callback.

    Returns:
        One `SourceResult` per requested day.
    """
    results: list[SourceResult] = []
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    plan = sampling_plan(request, source)
    for day in request.iter_days():
        dataset_id = _select_dataset(source, day)
        values = pattern_values(
            source=source.name,
            dataset_id=dataset_id,
            region=request.region,
            start=request.start_datetime,
            end=request.end_datetime,
            day=day,
            sampling=request.sampling_label,
        )
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
                    details={"temporal": plan.day_dict(day)},
                )
            )
            continue

        progress(source.name, f"downloading {day.isoformat()} from {dataset_id}")
        try:
            time_kwargs = _time_kwargs(request, source, day, plan)
            response = _copernicusmarine().subset(
                dataset_id=dataset_id,
                variables=list(source.variables),
                minimum_longitude=request.region.west,
                maximum_longitude=request.region.east,
                minimum_latitude=request.region.south,
                maximum_latitude=request.region.north,
                coordinates_selection_method=str(source.raw.get("coordinates_selection_method", "outside")),
                output_filename=output_path.name,
                output_directory=str(out_dir),
                overwrite=config.cache.overwrite,
                disable_progress_bar=True,
                **time_kwargs,
                **_depth_kwargs(source),
            )
        except Exception as exc:  # noqa: BLE001 - expected provider/date failures are warnings
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    message=str(exc),
                    day=day.isoformat(),
                    details={"temporal": plan.day_dict(day)},
                )
            )
            continue
        if output_path.exists():
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.DOWNLOADED,
                    path=output_path,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    day=day.isoformat(),
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
                    message=f"download completed but file is missing ({response!r})",
                    day=day.isoformat(),
                    details={"temporal": plan.day_dict(day)},
                )
            )
    return results


def _cmems_coverage(request: Request, source: SourceConfig) -> tuple[Coverage | None, str, bool]:
    dataset_ids = dict.fromkeys(_select_dataset(source, day) for day in request.iter_days())
    coverages = [coverage for dataset_id in dataset_ids if (coverage := describe_coverage(dataset_id)) is not None]
    if not coverages:
        return None, "describe", False
    return merge_coverages(coverages), "describe", True


def _cmems_day_details(
    request: Request, source: SourceConfig, day: date, plan: SamplingPlan, coverage: Coverage | None
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    dataset_id = _select_dataset(source, day)
    details = {
        "provider": "copernicusmarine",
        "method": "subset",
        "temporal": plan.day_dict(day),
        "request": {
            "dataset_id": dataset_id,
            "variables": list(source.variables),
            "minimum_longitude": request.region.west,
            "maximum_longitude": request.region.east,
            "minimum_latitude": request.region.south,
            "maximum_latitude": request.region.north,
            "coordinates_selection_method": str(source.raw.get("coordinates_selection_method", "outside")),
            **_time_kwargs(request, source, day, plan),
            **_depth_kwargs(source),
        },
    }
    return dataset_id, details, {}


def plan_cmems(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan CMEMS downloads, resolving coverage online through ``describe``."""
    coverage, method, checked = _cmems_coverage(request, source)
    return plan_source(
        request,
        source,
        request_dir,
        coverage=coverage,
        method=method,
        checked=checked,
        dataset_for_day=_select_dataset,
        day_details=_cmems_day_details,
        planned_format="netcdf",
    )


def _depth_kwargs(source: SourceConfig) -> dict[str, float]:
    depth = source.raw.get("depth")
    if not depth:
        return {}
    minimum_depth, maximum_depth = depth
    return {"minimum_depth": float(minimum_depth), "maximum_depth": float(maximum_depth)}


def _time_kwargs(request: Request, source: SourceConfig, day: date, plan: SamplingPlan) -> dict[str, str]:
    mode = str(source.raw.get("time_selection", "instant"))
    if mode == "full_day" and plan.actual_sampling != "24h":
        start, end = day_bounds(request, day)
        return {"start_datetime": _format_for_provider(start), "end_datetime": _format_for_provider(end)}
    timestamps = plan.timestamps_for_day(day)
    timestamp = timestamps[0] if timestamps else day_bounds(request, day)[0]
    text = _format_for_provider(timestamp)
    return {"start_datetime": text, "end_datetime": text}


def _format_for_provider(value) -> str:
    return value.replace(tzinfo=None).isoformat(timespec="seconds")


register_adapter(SourceAdapter(kind="cmems", fetch=fetch_cmems, plan=plan_cmems))
