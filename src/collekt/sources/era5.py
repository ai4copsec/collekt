"""ERA5 adapter using the CDS API."""

from __future__ import annotations

import math
from datetime import date, datetime
from functools import partial
from pathlib import Path
from typing import Any

from collekt.core.availability import Coverage
from collekt.core.config import Config, SourceConfig
from collekt.core.diagnostics import DoctorCheck, DoctorStatus, package_check, path_exists
from collekt.core.naming import format_pattern, pattern_values
from collekt.core.request import Request
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

DEFAULT_DATASET_ID = "reanalysis-era5-single-levels"


def _cdsapi():
    try:
        import cdsapi
    except ImportError as exc:  # pragma: no cover - exercised only without optional extra
        raise ImportError(
            "The 'cdsapi' package is required for ERA5 downloads; reinstall collekt's dependencies (uv sync)."
        ) from exc
    return cdsapi


def _padded_area(request: Request, pad_deg: float, resolution: float = 0.25) -> list[float]:
    return [
        min(90.0, math.ceil((request.region.north + pad_deg) / resolution) * resolution),
        max(-180.0, math.floor((request.region.west - pad_deg) / resolution) * resolution),
        max(-90.0, math.floor((request.region.south - pad_deg) / resolution) * resolution),
        min(180.0, math.ceil((request.region.east + pad_deg) / resolution) * resolution),
    ]


def fetch_era5(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
) -> list[SourceResult]:
    """Fetch ERA5 files for all requested days."""
    results: list[SourceResult] = []
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    plan = sampling_plan(request, source)
    for day in request.iter_days():
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

        request_body = _request_body(request, source, day, plan)
        progress(source.name, f"downloading {day.isoformat()} from {dataset_id}")
        tmp_path = output_path.parent / (output_path.name + ".part")
        try:
            _cdsapi().Client().retrieve(dataset_id, request_body).download(str(tmp_path))
        except Exception as exc:  # noqa: BLE001 - expected provider/date failures are warnings
            tmp_path.unlink(missing_ok=True)
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
        if tmp_path.exists():
            tmp_path.replace(output_path)
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
                missing_after_fetch(source, dataset_id, day=day.isoformat(), details={"temporal": plan.day_dict(day)})
            )
    return results


def _request_body(request: Request, source: SourceConfig, day: date, plan: SamplingPlan) -> dict[str, object]:
    dt = datetime.combine(day, datetime.min.time())
    times = plan.timestamps_for_day(day)
    if not times:
        times = (datetime.combine(day, datetime.min.time(), tzinfo=request.start_datetime.tzinfo),)
    return {
        "product_type": ["reanalysis"],
        "variable": list(source.variables),
        "year": [dt.strftime("%Y")],
        "month": [dt.strftime("%m")],
        "day": [dt.strftime("%d")],
        "time": [timestamp.strftime("%H:00") for timestamp in times],
        "data_format": str(source.raw.get("data_format", "netcdf")),
        "download_format": str(source.raw.get("download_format", "unarchived")),
        "area": _padded_area(request, float(source.raw.get("pad_deg", 0.5))),
    }


def _era5_dataset_for_day(source: SourceConfig, day: date) -> str:
    return source.dataset_id or DEFAULT_DATASET_ID


def _era5_day_details(
    request: Request, source: SourceConfig, day: date, plan: SamplingPlan, coverage: Coverage | None
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    details = {
        "provider": "cdsapi",
        "method": "retrieve",
        "temporal": plan.day_dict(day),
        "request": {"dataset_id": dataset_id, **_request_body(request, source, day, plan)},
    }
    return dataset_id, details, {}


def plan_era5(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan ERA5 downloads using the source's declarative coverage."""
    coverage, method, checked = static_source_coverage(source)
    return plan_source(
        request,
        source,
        request_dir,
        coverage=coverage,
        method=method,
        checked=checked,
        dataset_for_day=_era5_dataset_for_day,
        day_details=_era5_day_details,
        planned_format="netcdf",
    )


def diagnose(config: Config, online: bool) -> list[DoctorCheck]:
    """Return ERA5 package and credential diagnostics."""
    checks = [package_check("cdsapi package", "cdsapi")]
    if path_exists(config.credentials.cdsapi_rc):
        checks.append(DoctorCheck("cds credentials", DoctorStatus.OK, f"found {config.credentials.cdsapi_rc}"))
    else:
        checks.append(
            DoctorCheck("cds credentials", DoctorStatus.WARN, "CDS rc file not found; ERA5 downloads may fail")
        )
    return checks


def _batch_errors(source: SourceConfig) -> list[str]:
    """Report configurations a grid batch cannot split back into daily files."""
    errors = daily_output_errors(source)
    if (
        str(source.raw.get("data_format", "netcdf")) != "netcdf"
        or str(source.raw.get("download_format", "unarchived")) != "unarchived"
    ):
        errors.append("batch downloads require data_format='netcdf' and download_format='unarchived'")
    return errors


def _batch_key(item: SourceResult) -> dict[str, Any]:
    """Days can share a CDS request when everything but the day matches."""
    body = dict(item.details["request"])
    body.pop("day", None)
    return body


def _merge_batch(members: list[SourceResult]) -> dict[str, Any]:
    body = dict(members[0].details["request"])
    body["day"] = [day for item in members for day in item.details["request"]["day"]]
    return body


def _retrieve_batch(request: Request, source: SourceConfig, payload: dict[str, Any], path: Path) -> str | None:
    body = dict(payload)
    dataset_id = body.pop("dataset_id")
    _cdsapi().Client().retrieve(dataset_id, body).download(str(path))
    return None


def _split_batch(dataset: Any, item: SourceResult) -> Any:
    """Select one day's requested hours, refusing a batch that lacks any of them."""
    import numpy as np

    payload = item.details["request"]
    coordinate = "valid_time" if "valid_time" in dataset.coords else "time"
    expected = np.array([f"{item.day}T{hour}" for hour in payload["time"]], dtype="datetime64[ns]")
    if not np.isin(expected, dataset[coordinate].values).all():
        raise ValueError(f"batch is missing requested timestamps for {item.day}")
    if dataset[coordinate].ndim != 1:
        if len(expected) == 1 and dataset[coordinate].values == expected[0]:
            return dataset
        raise ValueError("ERA5 batch requires a one-dimensional time coordinate")
    return dataset.sel({coordinate: expected})


register_adapter(
    SourceAdapter(
        kind="era5",
        batch=partial(
            run_grid_batch,
            steps=GridSteps(compatible=_batch_key, merge=_merge_batch, retrieve=_retrieve_batch, split=_split_batch),
        ),
        batch_check=_batch_errors,
        fetch=fetch_era5,
        plan=plan_era5,
        diagnose=diagnose,
        known_raw_keys=frozenset({"data_format", "download_format", "pad_deg"}),
    )
)
