"""Shared planning helpers for source adapters.

`plan_source` implements the generic dry-run planning skeleton: it checks a
source's coverage against the request region and dates, emitting ``SKIPPED``
results for out-of-coverage region/days and ``PLANNED`` results (with an
``availability`` block and the concrete provider request) otherwise. Adapters
supply the provider-specific pieces as callbacks.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

from collekt.core.availability import (
    Availability,
    AvailabilityMethod,
    AvailabilityStatus,
    Coverage,
    day_in_range,
    day_reason,
    region_overlaps,
    region_reason,
    static_coverage,
)
from collekt.core.config import SourceConfig
from collekt.core.naming import format_pattern, pattern_values
from collekt.core.request import Request
from collekt.core.temporal import sampling_plan
from collekt.sources.base import SourceResult, SourceStatus

DatasetForDay = Callable[[SourceConfig, date], str]
DayDetails = Callable[[Request, SourceConfig, date, Any, Coverage | None], tuple[str, dict[str, Any], dict[str, Any]]]


def output_path(request: Request, source: SourceConfig, request_dir: Path, dataset_id: str, day: date, **extra) -> Path:
    """Return the output path for one source/day file."""
    values = (
        pattern_values(
            source=source.name,
            dataset_id=dataset_id,
            region=request.region,
            start=request.start_datetime,
            end=request.end_datetime,
            day=day,
        )
        | extra
    )
    return request_dir / source.path / format_pattern(source.filename_pattern, values)


def static_source_coverage(source: SourceConfig) -> tuple[Coverage | None, AvailabilityMethod, bool]:
    """Resolve coverage from a source's declarative ``coverage`` block."""
    coverage = static_coverage(source)
    if coverage is not None:
        return coverage, AvailabilityMethod.COVERAGE, True
    return None, AvailabilityMethod.NOT_CHECKED, False


def plan_source(
    request: Request,
    source: SourceConfig,
    request_dir: Path,
    *,
    coverage: Coverage | None,
    method: AvailabilityMethod,
    checked: bool,
    dataset_for_day: DatasetForDay,
    day_details: DayDetails,
    planned_format: str,
) -> list[SourceResult]:
    """Plan one source, checking availability before producing planned results.

    Region/dates outside the resolved coverage become ``SKIPPED`` results; every
    planned or skipped result carries an ``availability`` block in its details.
    """
    if coverage is not None and not region_overlaps(request.region, coverage):
        reason = region_reason(request.region, coverage)
        availability = Availability(AvailabilityStatus.UNAVAILABLE, method, reason=reason, coverage=coverage.as_dict())
        return [
            SourceResult(
                source=source.name,
                status=SourceStatus.SKIPPED,
                dataset_id=dataset_for_day(source, request.iter_days()[0]),
                variables=source.variables,
                message=reason,
                details={"availability": availability.as_dict()},
            )
        ]

    results: list[SourceResult] = []
    plan = sampling_plan(request, source)
    for day in request.iter_days():
        if coverage is not None and not day_in_range(day, coverage):
            reason = day_reason(day, coverage)
            availability = Availability(
                AvailabilityStatus.UNAVAILABLE, method, reason=reason, coverage=coverage.as_dict()
            )
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_for_day(source, day),
                    variables=source.variables,
                    message=reason,
                    day=day.isoformat(),
                    details={"temporal": plan.day_dict(day), "availability": availability.as_dict()},
                )
            )
            continue
        if checked:
            status = AvailabilityStatus.AVAILABLE
        elif method == AvailabilityMethod.NOT_CHECKED:
            status = AvailabilityStatus.NOT_CHECKED
        else:
            status = AvailabilityStatus.UNKNOWN
        availability = Availability(status, method, coverage=coverage.as_dict() if coverage else None)
        dataset_id, details, path_extra = day_details(request, source, day, plan, coverage)
        details["availability"] = availability.as_dict()
        results.append(
            SourceResult(
                source=source.name,
                status=SourceStatus.PLANNED,
                path=output_path(request, source, request_dir, dataset_id, day, **path_extra),
                dataset_id=dataset_id,
                variables=source.variables,
                day=day.isoformat(),
                format=planned_format,
                details=details,
            )
        )
    return results
