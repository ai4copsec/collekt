"""Multi-day grid retrievals with the original daily output contract.

The grouping, staging, and daily metadata narrowing here are shared by all grid
sources. How days are grouped, merged into one provider request, retrieved, and
cut back into days is supplied by each adapter as a `GridSteps`.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from collekt.core.config import Config, SourceConfig
from collekt.core.request import Request
from collekt.sources.base import BatchOptions, SourceResult, SourceStatus
from collekt.sources.batching.common import batch_details, daily_groups, failed, staged_output


@dataclass(frozen=True)
class GridSteps:
    """The provider-specific steps of a grid batch.

    Attributes:
        compatible: Key of a planned day's result; consecutive days are merged
            into one retrieval only while their keys are equal.
        merge: Build one provider request from the planned days of a group.
        retrieve: Download a merged request `(request, source, payload, path)`
            to `path`, returning a message for the results, if any.
        split: Select one planned day `(dataset, item)` from the retrieved dataset.
        transport: How the provider serves a group, recorded in the provenance.
    """

    compatible: Callable[[SourceResult], Any]
    merge: Callable[[list[SourceResult]], dict[str, Any]]
    retrieve: Callable[[Request, SourceConfig, dict[str, Any], Path], str | None]
    split: Callable[[Any, SourceResult], Any]
    transport: str = "range"


def run_grid_batch(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    options: BatchOptions,
    *,
    steps: GridSteps,
) -> list[SourceResult]:
    """Plan and execute identical provider groups, splitting each into daily files."""
    results, groups = daily_groups(request, source, config, request_dir, options.days, compatible=steps.compatible)
    for indices in groups:
        members = [results[index] for index in indices]
        try:
            payload = steps.merge(members)
        except Exception as exc:  # noqa: BLE001 - an unbuildable group must not abort the collection
            for index in indices:
                results[index] = failed(results[index], exc)
            continue
        batch = batch_details(source, members[0].day, members[-1].day, payload, transport=steps.transport)
        for index in indices:
            results[index] = replace(results[index], details=results[index].details | {"batch": batch})
        if options.dry_run:
            continue
        options.progress(source.name, f"downloading batch {batch['start']} to {batch['end']}")
        out_dir = request_dir / source.path
        out_dir.mkdir(parents=True, exist_ok=True)
        try:
            with TemporaryDirectory(prefix=".collekt-batch-", dir=out_dir) as directory:
                path = Path(directory) / "batch.nc"
                message = steps.retrieve(request, source, payload, path)
                if len(indices) == 1 and payload == members[0].details["request"]:
                    # The group asked the provider for exactly one day's own
                    # request, so there is nothing to split: publishing the
                    # provider's file untouched keeps it byte-for-byte identical
                    # to the same day downloaded without `batch_days`.
                    item = results[indices[0]]
                    with staged_output(item.path) as staged:
                        path.replace(staged)
                    results[indices[0]] = replace(item, status=SourceStatus.DOWNLOADED, message=message)
                    continue
                import xarray as xr

                with xr.open_dataset(path) as dataset:
                    for index in indices:
                        item = results[index]
                        try:
                            daily = _prepare_daily(steps.split(dataset, item))
                            with staged_output(item.path) as staged:
                                daily.to_netcdf(staged)
                            results[index] = replace(item, status=SourceStatus.DOWNLOADED, message=message)
                        except Exception as exc:  # noqa: BLE001 - isolate incomplete daily slices
                            results[index] = failed(item, exc)
        except Exception as exc:  # noqa: BLE001 - one unavailable batch must not block the next
            for index in indices:
                if results[index].status == SourceStatus.PLANNED:
                    results[index] = failed(results[index], exc)
    return results


def _like(original: str, value: Any) -> str:
    """Render `value` with the precision and zone suffix the provider used.

    A narrowed `time_coverage_start` is still the provider's attribute, so it
    keeps the provider's spelling. Nanosecond precision is only used where the
    original carried it.
    """
    import numpy as np

    text = str(original).strip()
    suffix = "Z" if text.endswith(("Z", "z")) else ""
    body = text.rstrip("Zz")
    if "T" not in body:
        return f"{np.datetime_as_string(value, unit='D')}{suffix}"
    fraction = body.partition("T")[2].partition(".")[2]
    unit = {3: "ms", 6: "us", 9: "ns"}.get(len(fraction), "s" if not fraction else "ns")
    return f"{np.datetime_as_string(value, unit=unit)}{suffix}"


def _iso_duration(seconds: float) -> str:
    """Render a span of seconds as an ISO 8601 duration, not a raw second count.

    `time_coverage_duration` is a duration string, so consumers expect `P1D` or
    `PT23H` rather than `PT82800S`.
    """
    if seconds <= 0:
        return "PT0S"
    days, remainder = divmod(round(seconds, 6), 86400)
    if not remainder:
        return f"P{int(days)}D"
    hours, remainder = divmod(remainder, 3600)
    minutes, remainder = divmod(remainder, 60)
    parts = [f"{int(days)}D"] if days else []
    time_parts = [f"{int(value)}{unit}" for value, unit in ((hours, "H"), (minutes, "M")) if value]
    if remainder:
        time_parts.append(f"{remainder:g}S")
    return "P" + "".join(parts) + "T" + "".join(time_parts)


def _prepare_daily(dataset: Any) -> Any:
    """Narrow standard coverage attributes and storage chunks to the daily slice."""
    import numpy as np

    daily = dataset.copy(deep=False)
    coordinate = "valid_time" if "valid_time" in daily.coords else "time"
    if coordinate in daily.coords:
        times = daily[coordinate].values
        start, end = np.min(times), np.max(times)
        for name, value in (("time_coverage_start", start), ("time_coverage_end", end)):
            if name in daily.attrs:
                daily.attrs[name] = _like(daily.attrs[name], value)
        if "time_coverage_duration" in daily.attrs:
            daily.attrs["time_coverage_duration"] = _iso_duration(float((end - start) / np.timedelta64(1, "s")))
    for variable in daily.variables.values():
        chunks = variable.encoding.get("chunksizes")
        if chunks is not None:
            variable.encoding = dict(variable.encoding)
            if len(chunks) != variable.ndim:
                # Selecting a scalar forecast run can remove an encoded dimension.
                variable.encoding.pop("chunksizes")
            else:
                variable.encoding["chunksizes"] = tuple(
                    min(chunk, size) for chunk, size in zip(chunks, variable.shape, strict=True)
                )
    return daily
