"""Batch archive reads and CLI work without conflating individual file transfers."""

from __future__ import annotations

import json
import shutil
from dataclasses import replace
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from collekt.core.batching import request_windows
from collekt.core.config import Config, SourceConfig
from collekt.core.request import Request
from collekt.sources.base import ProgressCallback, SourceResult, SourceStatus, get_adapter, should_reuse_cache
from collekt.sources.batching.common import batch_details, failed, staged_output
from collekt.sources.batching.gridded import daily_groups


def run_file_batch(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    batch_days: int,
    dry_run: bool,
    progress: ProgressCallback,
) -> list[SourceResult]:
    """Expose logical batches for archives/providers with one URL per native file.

    The grouping is provenance only: each native file still has its own URL and
    transfer. An adapter routed here must accept the private `_days` keyword on
    its `fetch`, which restricts the daily loop to the days this plan still
    needs while keeping the full request as the naming and cadence anchor.
    """
    planned, groups = daily_groups(request, source, config, request_dir, batch_days)
    provenance = {}
    for indices in groups:
        members = [planned[index] for index in indices]
        batch = batch_details(
            source,
            members[0].day,
            members[-1].day,
            [item.details.get("request", {}) for item in members],
            transport="individual_files",
        )
        for index in indices:
            planned[index] = replace(planned[index], details=planned[index].details | {"batch": batch})
            provenance[planned[index].day] = batch
    if dry_run or not groups:
        return planned
    # Preserve the full request: restarting daily loops on clipped requests
    # would change cadence anchors and filename patterns using start/end.
    results = get_adapter(source.kind).fetch(
        request,
        source,
        config,
        request_dir,
        progress=progress,
        _days=tuple(date.fromisoformat(day) for day in provenance),
    )
    if any(item.day is None for item in results):
        return [item for item in planned if item.status != SourceStatus.PLANNED] + results
    by_day = {
        item.day: replace(item, details=(item.details or {}) | {"batch": provenance[item.day]})
        if item.status != SourceStatus.REUSED
        else item
        for item in results
    }
    return [by_day.get(item.day, item) for item in planned]


def run_local_batch(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    batch_days: int,
    dry_run: bool,
    progress: ProgressCallback,
) -> list[SourceResult]:
    """Read a shared archive partition once per batch, preserving daily row files."""
    from collekt.sources import local

    spec = local._spec(source)
    glob_files = local._glob_files(spec) if isinstance(spec, local._LocalGlobSpec) else None
    matched = {
        day.isoformat(): glob_files if glob_files is not None else local._matched_files(spec, day)
        for day in request.iter_days()
    }
    results, groups = daily_groups(
        request,
        source,
        config,
        request_dir,
        batch_days,
        compatible=lambda item: matched[item.day],
    )
    for indices in groups:
        first, last = results[indices[0]], results[indices[-1]]
        files = matched[first.day]
        batch = batch_details(
            source,
            first.day,
            last.day,
            {"files": [str(path) for path in files], "start": first.day, "end": last.day},
            transport="archive_read",
        )
        for index in indices:
            results[index] = replace(results[index], details=results[index].details | {"batch": batch})
        if dry_run:
            continue
        progress(source.name, f"reading archive batch {first.day} to {last.day}")
        try:
            import damast
            import polars as pl

            if not files:
                raise ValueError("no archive files for this query")
            variables = tuple(dict.fromkeys((*source.variables, spec.time_column))) if source.variables else ()
            frame, metadata = local._read_day_subset(
                files,
                time_column=spec.time_column,
                lat_column=spec.lat_column,
                lon_column=spec.lon_column,
                day=date.fromisoformat(first.day),
                end_day=date.fromisoformat(last.day),
                region=request.region,
                variables=variables,
            )
        except local._ConfigError:
            raise
        except Exception as exc:  # noqa: BLE001 - isolate archive failures
            for index in indices:
                results[index] = failed(results[index], exc)
            continue
        for index in indices:
            item = results[index]
            try:
                start, end = local._day_range(date.fromisoformat(item.day))
                if getattr(frame.schema[spec.time_column], "time_zone", None) is None:
                    start, end = start.replace(tzinfo=None), end.replace(tzinfo=None)
                daily = frame.filter(pl.col(spec.time_column).is_between(start, end, closed="left"))
                if source.variables:
                    daily = daily.select(list(source.variables))
                if daily.height == 0:
                    raise ValueError("no archive rows for this day and region")
                subset = damast.core.AnnotatedDataFrame(
                    daily,
                    local._narrowed_metadata(metadata, daily.columns),
                    validation_mode=damast.core.ValidationMode.IGNORE,
                )
                with staged_output(item.path) as staged:
                    subset.export(staged)
                results[index] = replace(
                    item,
                    status=SourceStatus.DOWNLOADED,
                    details=item.details | {"rows": daily.height, "matched_files": [str(path) for path in files]},
                )
            except Exception as exc:  # noqa: BLE001 - one daily export must not block others
                results[index] = failed(item, exc)
    return results


def run_hozint_batch(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    batch_days: int,
    dry_run: bool,
    progress: ProgressCallback,
) -> list[SourceResult]:
    """Isolate each CLI invocation and cache only successfully completed windows."""
    from collekt.sources import hozint

    results = []
    for window in request_windows(request, batch_days):
        item = hozint.plan_hozint(window, source, config, request_dir)[0]
        batch = batch_details(source, window.start_datetime, window.end_datetime, item.details["request"])
        directory = request_dir / source.path / "batches" / batch["id"]
        batch["request"] = {"command": hozint._command(source, directory, window)}
        item = replace(item, details=item.details | {"batch": batch, "request": batch["request"]})
        marker = directory / ".complete.json"
        paths = None
        if should_reuse_cache(marker.exists(), config):
            try:
                completed = json.loads(marker.read_text())
                candidates = [directory / name for name in completed["outputs"]]
                if all((directory / name).is_file() for name in completed["files"]):
                    paths = candidates
            except (OSError, ValueError, KeyError, TypeError):
                pass
        if paths is not None:
            results.extend(replace(item, status=SourceStatus.REUSED, path=path) for path in paths)
            if not paths:
                results.append(failed(item, "hozint-apiclient produced no parquet output (cached completed query)"))
            continue
        if dry_run:
            results.append(item)
            continue
        progress(source.name, f"querying HOZINT batch {batch['start']} to {batch['end']}")
        directory.parent.mkdir(parents=True, exist_ok=True)
        try:
            with TemporaryDirectory(prefix=".collekt-", dir=directory.parent) as temporary:
                output = Path(temporary) / "output"
                output.mkdir()
                completed = hozint._run(hozint._command(source, output, window))
                if completed.returncode != 0:
                    raise RuntimeError(f"hozint-apiclient exited with {completed.returncode}")
                names = [path.name for path in sorted(output.glob("*.parquet"))]
                files = [path.name for path in output.iterdir() if path.is_file()]
                (output / marker.name).write_text(json.dumps({"outputs": names, "files": files}))
                if directory.exists():
                    shutil.rmtree(directory)
                output.replace(directory)
            if names:
                results.extend(replace(item, status=SourceStatus.DOWNLOADED, path=directory / name) for name in names)
            else:
                results.append(failed(item, "hozint-apiclient produced no parquet output"))
        except Exception as exc:  # noqa: BLE001 - isolate CLI window failures
            results.append(failed(item, exc))
    return results
