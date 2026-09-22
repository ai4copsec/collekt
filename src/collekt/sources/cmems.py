"""CMEMS adapter using the Copernicus Marine Toolbox."""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path
from types import ModuleType
from typing import Any

from collekt.core.availability import (
    AvailabilityMethod,
    Coverage,
    _coverage_from_catalogue,
    describe_coverage,
    merge_coverages,
    static_coverage,
)
from collekt.core.config import Config, SourceConfig
from collekt.core.diagnostics import DoctorCheck, DoctorStatus, package_check, path_exists
from collekt.core.naming import format_pattern, pattern_values
from collekt.core.request import Request
from collekt.core.temporal import SamplingPlan, day_bounds, sampling_plan
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
from collekt.sources.batching.gridded import run_grid_batch
from collekt.sources.planning import plan_source


def _copernicusmarine():
    _install_raw_tqdm_auto()
    try:
        import copernicusmarine
    except ImportError as exc:  # pragma: no cover - exercised only without optional extra
        raise ImportError(
            "The 'copernicusmarine' package is required for CMEMS downloads; "
            "reinstall collekt's dependencies (uv sync)."
        ) from exc
    return copernicusmarine


def _copernicusmarine_describe():
    try:
        import copernicusmarine
    except ImportError:
        return None
    return copernicusmarine.describe


def _install_raw_tqdm_auto() -> None:
    """Force raw tqdm for dependencies that import ``tqdm.auto`` in notebooks."""
    try:
        from tqdm import tqdm, trange
    except ImportError:  # pragma: no cover - tqdm is a project dependency
        return
    module = ModuleType("tqdm.auto")
    module.tqdm = tqdm
    module.trange = trange
    module.__all__ = ["tqdm", "trange"]
    sys.modules["tqdm.auto"] = module
    sys.modules["tqdm.autonotebook"] = module


def _select_dataset(source: SourceConfig, day: date) -> str:
    # Each source maps to a single dataset; near-real-time vs reanalysis is a
    # downstream choice made by selecting the matching ``*_nrt`` / ``*_my`` source.
    # ``day`` is kept for the planner's ``dataset_for_day`` interface.
    return source.dataset_id or source.name


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
        )
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
                missing_after_fetch(
                    source,
                    dataset_id,
                    day=day.isoformat(),
                    details={"temporal": plan.day_dict(day)},
                    extra=repr(response),
                )
            )
    return results


def _cmems_coverage(request: Request, source: SourceConfig) -> tuple[Coverage | None, AvailabilityMethod, bool]:
    dataset_ids = dict.fromkeys(_select_dataset(source, day) for day in request.iter_days())
    coverages = [coverage for dataset_id in dataset_ids if (coverage := describe_coverage(dataset_id)) is not None]
    if not coverages:
        coverage = static_coverage(source)
        if coverage is not None:
            return coverage, AvailabilityMethod.COVERAGE, True
        return None, AvailabilityMethod.DESCRIBE, False
    return merge_coverages(coverages), AvailabilityMethod.DESCRIBE, True


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


def _cmems_dataset_ids(source: SourceConfig) -> tuple[str, ...]:
    return (source.dataset_id,) if source.dataset_id else ()


def _catalogue_variable_names(catalogue: Any, dataset_id: str) -> set[str]:
    names: set[str] = set()
    for product in getattr(catalogue, "products", []) or []:
        for dataset in getattr(product, "datasets", []) or []:
            if getattr(dataset, "dataset_id", None) != dataset_id:
                continue
            for version in getattr(dataset, "versions", []) or []:
                for part in getattr(version, "parts", []) or []:
                    for service in getattr(part, "services", []) or []:
                        for variable in getattr(service, "variables", []) or []:
                            short_name = getattr(variable, "short_name", None)
                            if short_name:
                                names.add(str(short_name))
    return names


def _coverage_mismatches(shipped: Coverage, catalogue: Coverage) -> list[str]:
    mismatches: list[str] = []
    tolerance = 1e-5
    for name in ("west", "east", "south", "north"):
        expected = getattr(shipped, name)
        actual = getattr(catalogue, name)
        if abs(expected - actual) > tolerance:
            mismatches.append(f"{name}={expected} (catalogue {actual})")
    if shipped.start is not None and catalogue.start is not None and shipped.start != catalogue.start:
        mismatches.append(f"start={shipped.start.isoformat()} (catalogue {catalogue.start.isoformat()})")
    if shipped.end is not None and catalogue.end is not None and shipped.end != catalogue.end:
        mismatches.append(f"end={shipped.end.isoformat()} (catalogue {catalogue.end.isoformat()})")
    return mismatches


def _copernicus_credential_check(config: Config) -> DoctorCheck:
    cop_file = config.credentials.copernicusmarine_credentials_file
    cop_home = Path.home() / ".copernicusmarine"
    has_env = bool(
        os.environ.get("COPERNICUSMARINE_SERVICE_USERNAME") and os.environ.get("COPERNICUSMARINE_SERVICE_PASSWORD")
    )
    if path_exists(cop_file):
        return DoctorCheck("copernicus credentials", DoctorStatus.OK, f"found {cop_file}")
    if cop_home.exists():
        return DoctorCheck("copernicus credentials", DoctorStatus.OK, f"found {cop_home}")
    if has_env:
        return DoctorCheck("copernicus credentials", DoctorStatus.OK, "found Copernicus Marine environment variables")
    return DoctorCheck(
        "copernicus credentials",
        DoctorStatus.WARN,
        "no Copernicus Marine credential file or environment variables found",
    )


def _cmems_online_checks(config: Config) -> list[DoctorCheck]:
    describe = _copernicusmarine_describe()
    if describe is None:
        return [
            DoctorCheck(
                "cmems catalogue", DoctorStatus.WARN, "copernicusmarine is not installed; online checks skipped"
            )
        ]

    checks: list[DoctorCheck] = []
    cache: dict[str, Any] = {}
    for source in config.sources.values():
        if not source.enabled or source.kind != "cmems":
            continue
        shipped = set(source.available_variables)
        for dataset_id in _cmems_dataset_ids(source):
            check_name = f"cmems catalogue {source.name}"
            try:
                if dataset_id not in cache:
                    cache[dataset_id] = describe(dataset_id=dataset_id, disable_progress_bar=True, raise_on_error=True)
            except Exception as exc:  # noqa: BLE001 - provider/network errors are surfaced as diagnostics
                checks.append(
                    DoctorCheck(check_name, DoctorStatus.FAIL, f"{dataset_id}: catalogue lookup failed: {exc}")
                )
                continue

            catalogue_variables = _catalogue_variable_names(cache[dataset_id], dataset_id)
            if not catalogue_variables:
                checks.append(
                    DoctorCheck(check_name, DoctorStatus.FAIL, f"{dataset_id}: dataset not found in catalogue response")
                )
                continue
            stale = sorted(shipped - catalogue_variables)
            added = sorted(catalogue_variables - shipped)
            if stale:
                checks.append(
                    DoctorCheck(
                        check_name,
                        DoctorStatus.FAIL,
                        f"{dataset_id}: available_variables absent from catalogue: {', '.join(stale)}",
                    )
                )
            elif added:
                checks.append(
                    DoctorCheck(
                        check_name,
                        DoctorStatus.WARN,
                        f"{dataset_id}: catalogue also offers {', '.join(added)}; consider updating available_variables",
                    )
                )
            else:
                checks.append(
                    DoctorCheck(
                        check_name,
                        DoctorStatus.OK,
                        f"{dataset_id}: {len(shipped)} available_variables match the catalogue",
                    )
                )

            shipped_coverage = static_coverage(source)
            catalogue_coverage = _coverage_from_catalogue(cache[dataset_id], dataset_id)
            if shipped_coverage is None or catalogue_coverage is None:
                continue
            coverage_name = f"cmems coverage {source.name}"
            mismatches = _coverage_mismatches(shipped_coverage, catalogue_coverage)
            if mismatches:
                checks.append(
                    DoctorCheck(
                        coverage_name,
                        DoctorStatus.WARN,
                        f"{dataset_id}: declared coverage differs from catalogue: {', '.join(mismatches)}",
                    )
                )
            else:
                checks.append(
                    DoctorCheck(coverage_name, DoctorStatus.OK, f"{dataset_id}: declared coverage matches catalogue")
                )
    return checks


def diagnose(config: Config, online: bool) -> list[DoctorCheck]:
    """Return CMEMS package, credential, and catalogue diagnostics."""
    checks = [
        package_check("copernicusmarine package", "copernicusmarine"),
        _copernicus_credential_check(config),
    ]
    if online:
        checks.extend(_cmems_online_checks(config))
    return checks


register_adapter(
    SourceAdapter(
        kind="cmems",
        batch=run_grid_batch,
        fetch=fetch_cmems,
        plan=plan_cmems,
        diagnose=diagnose,
        known_raw_keys=frozenset({"coordinates_selection_method", "depth", "time_selection"}),
    )
)
