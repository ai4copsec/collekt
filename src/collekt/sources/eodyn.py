"""eOdyn surface currents from Ocean Data Access (ODA).

eOdyn publishes its OSmose L4 surface currents on ODA, its REST API, as one NetCDF
file per 3-hour analysis time. For each request day this adapter lists the files
of the catalog's ``oda_dataset`` whose analysis time falls in the request window,
downloads those whose footprint overlaps the request region, crops them to the
region and the selected variables, and writes one NetCDF per UTC day.

Only the dataset named in the catalog is ever queried: ODA also serves other
products (e.g. AIS) that collekt does not expose. Credentials come from the
``ODA_USERNAME`` / ``ODA_PASSWORD`` environment variables (shell or ``.env``).
"""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from collekt.core.availability import AvailabilityMethod, Coverage, clip_region, region_overlaps
from collekt.core.config import Config, SourceConfig
from collekt.core.diagnostics import DoctorCheck, DoctorStatus, package_check
from collekt.core.request import Region, Request
from collekt.core.temporal import SamplingPlan, day_bounds, sampling_plan
from collekt.sources.base import (
    ProgressCallback,
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
    should_reuse_cache,
)
from collekt.sources.batching.common import daily_output_errors, staged_output
from collekt.sources.batching.files import run_file_batch
from collekt.sources.planning import output_path, plan_source, static_source_coverage

DEFAULT_DATASET_ID = "EODYN-OS-VELOCITY-L4"
DEFAULT_ODA_DATASET = "current-os-osmose-l4"
CREDENTIAL_ENVS = ("ODA_USERNAME", "ODA_PASSWORD")
# Global attributes describing one analysis (its time, creation date, fit
# statistics). They differ between a day's files, so daily outputs drop them.
PER_ANALYSIS_ATTRS = (
    "analysis_time",
    "Creation_date",
    "time_coverage_start",
    "time_coverage_end",
    "ekman_residual_gain_factor",
    "ekman_residual_obs_count",
    "ekman_residual_rmse_after_mps",
    "ekman_residual_rmse_before_mps",
)


def _oda_dataset(source: SourceConfig) -> str:
    return str(source.raw.get("oda_dataset", DEFAULT_ODA_DATASET))


def _missing_credentials() -> list[str]:
    return [name for name in CREDENTIAL_ENVS if not os.environ.get(name)]


def _oda_client() -> Any:
    """Return an ODA client authenticated from ``ODA_USERNAME`` / ``ODA_PASSWORD``."""
    from oda import Oda

    return Oda()


def _connect() -> tuple[Any, str | None]:
    """Return ``(client, None)``, or ``(None, reason)`` when ODA cannot be used."""
    missing = _missing_credentials()
    if missing:
        return None, f"set {' and '.join(missing)} (shell or .env) to read eOdyn currents from ODA"
    try:
        return _oda_client(), None
    except ImportError:
        return None, "python-oda is not installed; reinstall collekt's dependencies (uv sync)"
    except Exception as exc:  # noqa: BLE001 - authentication/network errors become skip reasons
        return None, f"could not connect to ODA: {exc}"


def _as_utc(moment: datetime) -> datetime:
    """Return ``moment`` in UTC, reading a timestamp without a time zone as UTC."""
    return moment.astimezone(UTC) if moment.tzinfo else moment.replace(tzinfo=UTC)


def _footprint(document: Any) -> Coverage | None:
    """Return the bounding box of a document's ODA footprint, if it declares one."""
    if document.area_geojson is None:
        return None
    points = [point for ring in document.area_geojson.coordinates for point in ring]
    lons = [point.lon for point in points]
    lats = [point.lat for point in points]
    return Coverage(west=min(lons), east=max(lons), south=min(lats), north=max(lats), start=None, end=None)


def _oda_coverage(client: Any, source: SourceConfig) -> Coverage | None:
    """Return the dataset's live extent from its first and last files.

    The box spans both files' footprints, so a footprint that grew over time is
    covered. Returns ``None`` when the dataset is empty or either file lacks an
    analysis time or a footprint.
    """
    name = _oda_dataset(source)
    first = next(iter(client.documents.find(dataset_name=name, order_by="+start_datetime")), None)
    last = next(iter(client.documents.find(dataset_name=name, order_by="-start_datetime")), None)
    if first is None or last is None or first.start_datetime is None or last.start_datetime is None:
        return None
    boxes = [_footprint(first), _footprint(last)]
    if None in boxes:
        return None
    return Coverage(
        west=min(box.west for box in boxes),
        east=max(box.east for box in boxes),
        south=min(box.south for box in boxes),
        north=max(box.north for box in boxes),
        start=_as_utc(first.start_datetime).date(),
        end=_as_utc(last.start_datetime).date(),
    )


def _day_documents(client: Any, request: Request, source: SourceConfig, day: date) -> list[Any]:
    """List one day's documents whose analysis time falls in the request window."""
    low, high = day_bounds(request, day)
    found = client.documents.find(
        dataset_name=_oda_dataset(source), date__gte=low, date__lte=high, order_by="+start_datetime"
    )
    # ODA returns every file whose 3-hour window overlaps [low, high], so the
    # previous day's last file shows up too; keep only analysis times inside.
    return [
        document for document in found if document.start_datetime and low <= _as_utc(document.start_datetime) <= high
    ]


def _ascending_slice(values, low: float, high: float) -> slice:
    """Return a coordinate slice ordered to match the coordinate direction."""
    if values.size and values[0] > values[-1]:
        return slice(high, low)
    return slice(low, high)


def _crop(dataset: Any, region: Region, variables: tuple[str, ...], name: str) -> Any:
    """Select the requested variables inside the region, loaded into memory."""
    available = [variable for variable in variables if variable in dataset.data_vars]
    if not available:
        raise ValueError(f"none of the requested variables {list(variables)} are present in {name}")
    subset = dataset[available].sel(
        latitude=_ascending_slice(dataset["latitude"].values, region.south, region.north),
        longitude=_ascending_slice(dataset["longitude"].values, region.west, region.east),
    )
    if not subset.sizes["latitude"] or not subset.sizes["longitude"]:
        spacing = abs(float(dataset["latitude"][1] - dataset["latitude"][0]))
        raise ValueError(
            f"the request region contains no grid point of {name}, whose grid spacing is {spacing:g}°; "
            "widen the region to span at least one grid point"
        )
    return subset.load()


def _silent_progress() -> Any:
    """Return a no-op ODA download tracker; without one, python-oda logs every download."""
    from oda.core.progress import DownloadProgress

    class SilentProgress(DownloadProgress):
        def on_bytes_progress(self, *, current: int, total: int | None, name: str) -> None:
            pass

    return SilentProgress()


def _read_day(documents: list[Any], region: Region, variables: tuple[str, ...], scratch: Path) -> Any:
    """Download a day's documents and join their cropped time steps."""
    import xarray as xr

    steps = []
    progress = _silent_progress()
    with TemporaryDirectory(prefix=".collekt-oda-", dir=scratch) as directory:
        for document in documents:
            raw = Path(directory) / document.name
            with raw.open("wb") as handle:
                for chunk in document.download(download_progress=progress).byte_stream:
                    handle.write(chunk)
            with xr.open_dataset(raw) as dataset:
                steps.append(_crop(dataset, region, variables, document.name))
    # Drop per-analysis attributes whatever the number of steps, so the output
    # does not depend on the request window; drop_conflicts catches any other
    # attribute that differs between steps.
    combined = xr.concat(steps, dim="time", coords="minimal", compat="override", combine_attrs="drop_conflicts")
    for name in PER_ANALYSIS_ATTRS:
        combined.attrs.pop(name, None)
    return combined.sortby("time")


def _skipped(source: SourceConfig, dataset_id: str, day: date, message: str, details: dict[str, Any]) -> SourceResult:
    return SourceResult(
        source=source.name,
        status=SourceStatus.SKIPPED,
        dataset_id=dataset_id,
        variables=source.variables,
        message=message,
        day=day.isoformat(),
        format="netcdf",
        details=details,
    )


def fetch_eodyn(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
    _days: tuple[date, ...] | None = None,
) -> list[SourceResult]:
    """Fetch eOdyn OSmose L4 currents from ODA, one region-cropped NetCDF per day.

    Days without any file on ODA (not produced yet, or before the dataset starts)
    and days whose files do not overlap the request region are skipped with an
    explanatory message rather than failing the collection.

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
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    oda_dataset = _oda_dataset(source)
    plan = sampling_plan(request, source)
    client = reason = None
    results: list[SourceResult] = []
    for day in request.iter_days() if _days is None else _days:
        path = output_path(request, source, request_dir, dataset_id, day)
        details: dict[str, Any] = {
            "provider": "eodyn-oda",
            "method": "documents",
            "oda_dataset": oda_dataset,
            "temporal": plan.day_dict(day),
        }
        if should_reuse_cache(path.exists(), config):
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.REUSED,
                    path=path,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    day=day.isoformat(),
                    format="netcdf",
                    details=details,
                )
            )
            continue

        if client is None and reason is None:
            client, reason = _connect()
        if client is None:
            results.append(_skipped(source, dataset_id, day, reason, details))
            continue

        progress(source.name, f"reading {day.isoformat()} from ODA dataset {oda_dataset}")
        try:
            documents = _day_documents(client, request, source, day)
            if not documents:
                results.append(
                    _skipped(source, dataset_id, day, f"no file on ODA ({oda_dataset}) for {day.isoformat()}", details)
                )
                continue
            overlapping = [
                doc for doc in documents if (box := _footprint(doc)) is None or region_overlaps(request.region, box)
            ]
            if not overlapping:
                box = _footprint(documents[0])
                message = (
                    f"the request region is outside the footprint of {oda_dataset} "
                    f"(west={box.west}, east={box.east}, south={box.south}, north={box.north})"
                )
                results.append(_skipped(source, dataset_id, day, message, details))
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            dataset = _read_day(overlapping, request.region, source.variables, path.parent)
            with staged_output(path) as staged:
                dataset.to_netcdf(staged)
        except Exception as exc:  # noqa: BLE001 - provider/file errors skip the day, not the collection
            results.append(_skipped(source, dataset_id, day, str(exc), details))
            continue

        missing = [name for name in source.variables if name not in dataset.data_vars]
        results.append(
            SourceResult(
                source=source.name,
                status=SourceStatus.DOWNLOADED,
                path=path,
                dataset_id=dataset_id,
                variables=source.variables,
                message=f"missing from the ODA files: {', '.join(missing)}" if missing else None,
                day=day.isoformat(),
                format="netcdf",
                details=details | {"documents": [document.name for document in overlapping]},
            )
        )
    return results


def _eodyn_coverage(source: SourceConfig) -> tuple[Coverage | None, AvailabilityMethod, bool, str | None]:
    """Resolve coverage from ODA, falling back to the catalog's ``coverage`` block.

    The last item says why ODA's coverage could not be used, or is ``None``.
    """
    client, reason = _connect()
    if client is not None:
        try:
            coverage = _oda_coverage(client, source)
        except Exception as exc:  # noqa: BLE001 - an unreachable ODA falls back to the declared coverage
            reason = f"could not read the extent of {_oda_dataset(source)} on ODA: {exc}"
        else:
            if coverage is not None:
                return coverage, AvailabilityMethod.DESCRIBE, True, None
            reason = f"ODA lists no dated file with a footprint in {_oda_dataset(source)}"
    return *static_source_coverage(source), reason


def _eodyn_dataset_for_day(source: SourceConfig, day: date) -> str:
    return source.dataset_id or DEFAULT_DATASET_ID


def _eodyn_day_details(
    request: Request, source: SourceConfig, day: date, plan: SamplingPlan, coverage: Coverage | None
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    region = request.region
    if coverage is not None:
        west, east, south, north = clip_region(region, coverage)
    else:
        west, east, south, north = region.west, region.east, region.south, region.north
    low, high = day_bounds(request, day)
    details = {
        "provider": "eodyn-oda",
        "method": "documents",
        "temporal": plan.day_dict(day),
        "request": {
            "oda_dataset": _oda_dataset(source),
            "variables": list(source.variables),
            "date__gte": low.isoformat(),
            "date__lte": high.isoformat(),
            "minimum_longitude": west,
            "maximum_longitude": east,
            "minimum_latitude": south,
            "maximum_latitude": north,
        },
    }
    return source.dataset_id or DEFAULT_DATASET_ID, details, {}


def plan_eodyn(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan eOdyn reads, resolving coverage from ODA when credentials allow it.

    When ODA cannot be used, days are planned from the catalog coverage and each
    planned result says why, since fetching them will likely fail the same way.
    """
    coverage, method, checked, fallback = _eodyn_coverage(source)
    results = plan_source(
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
    if fallback is None:
        return results
    note = f"{fallback}; planned from the catalog coverage"
    return [_with_note(result, note) if result.status == SourceStatus.PLANNED else result for result in results]


def _with_note(result: SourceResult, note: str) -> SourceResult:
    details = dict(result.details or {})
    details["availability"] = {**details.get("availability", {}), "reason": note}
    return replace(result, message=note, details=details)


def diagnose(config: Config, online: bool) -> list[DoctorCheck]:
    """Return python-oda, ODA credential, and (online) ODA dataset diagnostics."""
    checks = [package_check("python-oda package", "oda")]
    missing = _missing_credentials()
    if missing:
        checks.append(
            DoctorCheck("oda credentials", DoctorStatus.WARN, f"no {' / '.join(missing)} found; eOdyn will be skipped")
        )
        return checks
    checks.append(DoctorCheck("oda credentials", DoctorStatus.OK, f"found {' and '.join(CREDENTIAL_ENVS)}"))
    if online:
        checks.extend(_online_checks(config))
    return checks


def _online_checks(config: Config) -> list[DoctorCheck]:
    checks = []
    for source in config.sources.values():
        if not source.enabled or source.kind != "eodyn":
            continue
        name = f"oda dataset {source.name}"
        try:
            coverage = _oda_coverage(_oda_client(), source)
        except Exception as exc:  # noqa: BLE001 - provider/network errors are surfaced as diagnostics
            checks.append(DoctorCheck(name, DoctorStatus.FAIL, f"{_oda_dataset(source)}: lookup failed: {exc}"))
            continue
        if coverage is None:
            checks.append(DoctorCheck(name, DoctorStatus.WARN, f"{_oda_dataset(source)}: no files on ODA"))
            continue
        checks.append(
            DoctorCheck(
                name,
                DoctorStatus.OK,
                f"{_oda_dataset(source)}: files from {coverage.start} to {coverage.end}",
            )
        )
    return checks


register_adapter(
    SourceAdapter(
        kind="eodyn",
        batch=run_file_batch,
        batch_check=daily_output_errors,
        fetch=fetch_eodyn,
        plan=plan_eodyn,
        diagnose=diagnose,
        known_raw_keys=frozenset({"oda_dataset"}),
    )
)
