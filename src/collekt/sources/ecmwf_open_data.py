"""ECMWF Open Data forecast adapter."""

from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path
from typing import Any

from collekt.core.availability import Coverage
from collekt.core.config import Config, SourceConfig
from collekt.core.naming import format_pattern, pattern_values
from collekt.core.request import Request
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


def _client_class():
    try:
        from ecmwf.opendata import Client
    except ImportError as exc:  # pragma: no cover - exercised only without optional extra
        raise ImportError(
            "The 'ecmwf-opendata' package is required for ECMWF forecast downloads; "
            "reinstall collekt's dependencies (uv sync)."
        ) from exc
    return Client


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
    """Fetch ECMWF Open Data forecast files."""
    results: list[SourceResult] = []
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    model = str(source.raw.get("model", "ifs"))
    resol = str(source.raw.get("resol", "0p25"))
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
            sampling=request.sampling_label,
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
                    format="grib2",
                    details={"temporal": plan.day_dict(day)},
                )
            )
            continue

        progress(source.name, f"downloading {day.isoformat()} {run_time} UTC forecast from ECMWF Open Data")
        try:
            if client is None:
                client = _client_class()(source=str(source.raw.get("source", "ecmwf")), model=model, resol=resol)
            client.retrieve(_request_for_day(source, day, plan), target=str(output_path))
        except Exception as exc:  # noqa: BLE001 - provider availability failures are warnings
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    message=str(exc),
                    day=day.isoformat(),
                    format="grib2",
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
                    format="grib2",
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
                    format="grib2",
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
    details = {
        "provider": "ecmwf-opendata",
        "method": "retrieve",
        "temporal": plan.day_dict(day),
        "request": _request_for_day(source, day, plan),
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
        planned_format="grib2",
    )


register_adapter(SourceAdapter(kind="ecmwf_open_data", fetch=fetch_ecmwf_open_data, plan=plan_ecmwf_open_data))
