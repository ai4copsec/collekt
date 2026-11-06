"""Local archive adapter.

Reads a pre-existing tabular collection on disk as a collekt source, row-filtered to each
request day and region, and written into the request directory. This is the tabular analogue
of eOdyn's `mode: archive`, generalized so any local collection can be queried without
per-source code. Two ways to locate a day's rows are supported - see `_spec`:

- `layout`: the archive is already time-partitioned, e.g. via ``damast convert --save-as`` or
  `AnnotatedDataFrame.export_partitioned` - the files that may hold a day are resolved from the
  archive's own layout (`damast.core.SaveAs.expected_paths`) without reading anything.
- `file_pattern` + `time_column`: the archive is not partitioned by time at all - every file
  matching `file_pattern` is globbed once and a day's rows are selected by filtering
  `time_column`.

Either way the rows themselves are filtered by both time and region, so `layout` only prunes
which files are opened - it is not what makes a day's result correct. A layout coarser than the
request (monthly, say) therefore still yields just that day's rows.

Producing or updating the archive itself (e.g. via `damast watch`) is out of scope here - this
adapter only reads an already-existing one.

Only Parquet archives are read lazily (damast loads them via `polars.scan_parquet`, so the
time/region filters push down to row-group statistics). The NetCDF, CSV and HDF readers load
each file in full, and in `file_pattern` mode every matched file is re-read for each request
day - prefer a day-partitioned Parquet archive for anything large.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any

from collekt.core.availability import Coverage
from collekt.core.config import Config, SourceConfig
from collekt.core.naming import format_pattern, pattern_values
from collekt.core.request import Request
from collekt.core.temporal import SamplingPlan, sampling_plan
from collekt.sources.base import (
    BatchOptions,
    ProgressCallback,
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
    should_reuse_cache,
)
from collekt.sources.batching.common import batch_details, daily_groups, daily_output_errors, failed, staged_output
from collekt.sources.planning import plan_source, static_source_coverage
from collekt.sources.tabular import (
    TabularConfigError,
    day_range,
    layout_files,
    narrowed_metadata,
    read_day_subset,
)

DEFAULT_DATASET_ID = "local-archive"


@dataclass(frozen=True)
class _LocalSpec:
    """Parsed ``local``-source configuration: time-partitioned via `layout`."""

    archive_root: Path
    layout: str
    time_column: str
    lat_column: str
    lon_column: str


@dataclass(frozen=True)
class _LocalGlobSpec:
    """Parsed ``local``-source configuration: unpartitioned, day-filtered via `time_column`."""

    archive_root: Path
    file_pattern: str
    time_column: str
    lat_column: str
    lon_column: str


def _layout_time_column(layout: str) -> str:
    """Return the column a ``time:``/``time+column:`` layout partitions on.

    ``"time:timestamp+daily:%Y-%m-%d"`` -> ``"timestamp"``. The spec sits between the strategy
    prefix and the filename template, exactly where `damast.core.SaveAs.parse` reads it, so the
    time column is shared with the archive's own layout instead of being configured twice.
    """
    _, _, after_prefix = layout.partition(":")
    spec, _, _ = after_prefix.partition(":")
    return spec.split("+")[0].strip()


def _spec(source: SourceConfig) -> _LocalSpec | _LocalGlobSpec:
    archive_root = source.raw.get("archive_root")
    layout = source.raw.get("layout")
    file_pattern = source.raw.get("file_pattern")
    region_columns = source.raw.get("region_columns")
    if not archive_root:
        raise ValueError(f"local source {source.name!r}: 'archive_root' is required")
    if not region_columns or len(list(region_columns)) != 2:
        raise ValueError(f"local source {source.name!r}: 'region_columns' must list exactly [lat_column, lon_column]")
    lat_column, lon_column = region_columns
    archive_root_path = Path(str(archive_root)).expanduser()

    if layout and file_pattern:
        raise ValueError(f"local source {source.name!r}: 'layout' and 'file_pattern' are mutually exclusive")

    if file_pattern:
        time_column = source.raw.get("time_column")
        if not time_column:
            raise ValueError(f"local source {source.name!r}: 'file_pattern' requires 'time_column'")
        return _LocalGlobSpec(
            archive_root=archive_root_path,
            file_pattern=str(file_pattern),
            time_column=str(time_column),
            lat_column=str(lat_column),
            lon_column=str(lon_column),
        )

    if not layout:
        raise ValueError(f"local source {source.name!r}: one of 'layout' or 'file_pattern' is required")
    if not str(layout).startswith(("time:", "time+column:")):
        raise ValueError(
            f"local source {source.name!r}: 'layout' must be a 'time:' or 'time+column:' partitioning spec"
            " (see damast.core.SaveAs.parse) - fetch_local serves one result per request day, which needs a"
            f" time dimension to split rows by; got {layout!r}. Use 'file_pattern' + 'time_column' instead if"
            " the archive isn't partitioned by time at all."
        )
    layout_time_column = _layout_time_column(str(layout))
    if not layout_time_column:
        raise ValueError(
            f"local source {source.name!r}: could not read the time column from layout {layout!r}"
            " - expected '<strategy>:<time_column>[+...]:<template>'"
        )
    return _LocalSpec(
        archive_root=archive_root_path,
        layout=str(layout),
        time_column=layout_time_column,
        lat_column=str(lat_column),
        lon_column=str(lon_column),
    )


def _matched_files(spec: _LocalSpec, day: date) -> list[Path]:
    """Return the archive files that may hold `day`'s data, resolving any glob wildcard."""
    return layout_files(spec.archive_root, spec.layout, day)


def _glob_files(spec: _LocalGlobSpec) -> list[Path]:
    """Return every archive file matching `file_pattern`, resolved once per fetch (day-independent)."""
    return sorted(spec.archive_root.glob(spec.file_pattern))


def fetch_local(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
) -> list[SourceResult]:
    """Serve a local, damast-managed tabular archive as a collekt source.

    Each requested day's rows are read, filtered to that day (`time_column`, or the time column
    named by `layout`) and to the request's region (`region_columns`), and written as a single
    Parquet file per day into `request_dir`. A day with no matching file, or no rows in region,
    is skipped rather than treated as an error; a column that the archive does not have at all
    is raised, since it cannot be anything but a misconfiguration. See the module docstring for
    the two ways (`layout` vs `file_pattern` + `time_column`) a day's files can be located.
    """
    import damast

    spec = _spec(source)
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    plan = sampling_plan(request, source)

    # Glob mode resolves the candidate files once - unlike `layout`, they carry no per-day
    # information, so the same file list is filtered by `time_column` for every request day.
    glob_matched = _glob_files(spec) if isinstance(spec, _LocalGlobSpec) else None

    results: list[SourceResult] = []
    for day in request.iter_days():
        temporal = plan.day_dict(day)
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
                    format="parquet",
                    details={"temporal": temporal, "archive_root": str(spec.archive_root)},
                )
            )
            continue

        if isinstance(spec, _LocalGlobSpec):
            matched = glob_matched
            no_match_message = f"no file matches {spec.file_pattern!r} under {spec.archive_root}"
            no_match_details = {
                "temporal": temporal,
                "archive_root": str(spec.archive_root),
                "file_pattern": spec.file_pattern,
            }
        else:
            matched = _matched_files(spec, day)
            no_match_message = f"no archive file matches {day.isoformat()} under {spec.archive_root}"
            no_match_details = {"temporal": temporal, "archive_root": str(spec.archive_root), "layout": spec.layout}

        if not matched:
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    day=day.isoformat(),
                    message=no_match_message,
                    details=no_match_details,
                )
            )
            continue

        progress(source.name, f"reading {day.isoformat()} from {len(matched)} local file(s)")
        try:
            collected, metadata = read_day_subset(
                matched,
                time_column=spec.time_column,
                lat_column=spec.lat_column,
                lon_column=spec.lon_column,
                day=day,
                region=request.region,
                variables=source.variables,
            )
        except TabularConfigError as exc:
            raise ValueError(f"local source {source.name!r}: {exc}") from exc
        except Exception as exc:  # noqa: BLE001 - a malformed archive file is a warning, not a crash
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    day=day.isoformat(),
                    message=str(exc),
                    details={"temporal": temporal, "matched_files": [str(f) for f in matched]},
                )
            )
            continue

        if collected.height == 0:
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    day=day.isoformat(),
                    message=f"no rows within the request region for {day.isoformat()}",
                    details={"temporal": temporal, "matched_files": [str(f) for f in matched]},
                )
            )
            continue

        subset = damast.core.AnnotatedDataFrame(
            collected,
            narrowed_metadata(metadata, collected.columns),
            validation_mode=damast.core.ValidationMode.IGNORE,
        )
        try:
            with staged_output(output_path) as staged:
                subset.export(staged)
        except Exception as exc:  # noqa: BLE001 - one daily export must not block others
            results.append(
                SourceResult(
                    source=source.name,
                    status=SourceStatus.SKIPPED,
                    dataset_id=dataset_id,
                    variables=source.variables,
                    day=day.isoformat(),
                    message=f"could not export {day.isoformat()}: {exc}",
                    details={"temporal": temporal, "matched_files": [str(f) for f in matched]},
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
                format="parquet",
                details={
                    "provider": "local-archive",
                    "method": "archive",
                    "temporal": temporal,
                    "archive_root": str(spec.archive_root),
                    "matched_files": [str(f) for f in matched],
                    "rows": collected.height,
                },
            )
        )
    return results


def _local_dataset_for_day(source: SourceConfig, day: date) -> str:
    return source.dataset_id or DEFAULT_DATASET_ID


def _local_day_details(
    request: Request, source: SourceConfig, day: date, plan: SamplingPlan, coverage: Coverage | None
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    spec = _spec(source)
    request_details: dict[str, Any] = {"archive_root": str(spec.archive_root), "time_column": spec.time_column}
    if isinstance(spec, _LocalGlobSpec):
        request_details["file_pattern"] = spec.file_pattern
    else:
        request_details["layout"] = spec.layout
    details = {
        "provider": "local-archive",
        "method": "archive",
        "temporal": plan.day_dict(day),
        "request": request_details,
    }
    return dataset_id, details, {}


def plan_local(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan local-archive reads using the source's declarative coverage, if any."""
    coverage, method, checked = static_source_coverage(source)
    return plan_source(
        request,
        source,
        request_dir,
        coverage=coverage,
        method=method,
        checked=checked,
        dataset_for_day=_local_dataset_for_day,
        day_details=_local_day_details,
        planned_format="parquet",
    )


def _run_batch(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    options: BatchOptions,
) -> list[SourceResult]:
    """Read a shared archive partition once per batch, preserving daily row files."""
    spec = _spec(source)
    glob_files = _glob_files(spec) if isinstance(spec, _LocalGlobSpec) else None
    matched = {
        day.isoformat(): glob_files if glob_files is not None else _matched_files(spec, day)
        for day in request.iter_days()
    }
    results, groups = daily_groups(
        request,
        source,
        config,
        request_dir,
        options.days,
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
        if options.dry_run:
            continue
        options.progress(source.name, f"reading archive batch {first.day} to {last.day}")
        try:
            import damast
            import polars as pl

            if not files:
                raise ValueError("no archive files for this query")
            variables = tuple(dict.fromkeys((*source.variables, spec.time_column))) if source.variables else ()
            frame, metadata = read_day_subset(
                files,
                time_column=spec.time_column,
                lat_column=spec.lat_column,
                lon_column=spec.lon_column,
                day=date.fromisoformat(first.day),
                end_day=date.fromisoformat(last.day),
                region=request.region,
                variables=variables,
            )
        except TabularConfigError:
            raise
        except Exception as exc:  # noqa: BLE001 - isolate archive failures
            for index in indices:
                results[index] = failed(results[index], exc)
            continue
        for index in indices:
            item = results[index]
            try:
                start, end = day_range(date.fromisoformat(item.day))
                if getattr(frame.schema[spec.time_column], "time_zone", None) is None:
                    start, end = start.replace(tzinfo=None), end.replace(tzinfo=None)
                daily = frame.filter(pl.col(spec.time_column).is_between(start, end, closed="left"))
                if source.variables:
                    daily = daily.select(list(source.variables))
                if daily.height == 0:
                    raise ValueError("no archive rows for this day and region")
                subset = damast.core.AnnotatedDataFrame(
                    daily,
                    narrowed_metadata(metadata, daily.columns),
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


register_adapter(
    SourceAdapter(
        kind="local",
        batch=_run_batch,
        batch_check=daily_output_errors,
        fetch=fetch_local,
        plan=plan_local,
        known_raw_keys=frozenset({"archive_root", "layout", "region_columns", "file_pattern", "time_column"}),
    )
)
